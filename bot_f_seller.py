# bots/bot_f_seller.py

import requests
import redis
import time
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
import re
import json
import random
import base58
import base64
from base64 import b64decode
from typing import List, Union, Dict, Any, Optional
import os
import csv
import uuid
import sys
from collections import defaultdict

from solders.pubkey import Pubkey
from solders.message import MessageV0
from solders.hash import Hash
from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts
from solana.rpc.commitment import Confirmed
from solana.rpc.types import TxOpts
from solders.transaction import VersionedTransaction
from solders.message import VersionedMessage
from solders.message import MessageV0
from solders.instruction import Instruction, AccountMeta
from solders.keypair import Keypair
from solders.signature import Signature
from solders.address_lookup_table_account import AddressLookupTableAccount
from spl.token.instructions import get_associated_token_address
from spl.token.instructions import close_account, CloseAccountParams
from spl.token.instructions import create_associated_token_account as spl_create_ata
from spl.token._layouts import MINT_LAYOUT
from solana.publickey import PublicKey

from dotenv import load_dotenv
load_dotenv()
from shared.utils import fetch_wallet_velocity, fetch_market_overview, confirm_tx_landed







# ---------------------------------------
# CONFIG
# ---------------------------------------

RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"
JUPITER_SWAP_URL = "https://lite-api.jup.ag/swap/v1/swap"
JUPITER_API_KEY = os.getenv("JUPITER_API_KEY")

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

YOUR_HOST = os.getenv("YOUR_HOST")
YOUR_PORT = os.getenv("YOUR_PORT")
YOUR_PASSWORD = os.getenv("YOUR_PASSWORD")

TOKEN_2022_PROGRAM_ID = PublicKey("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
TOKEN_PROGRAM_ID = PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')



# Base token (what you spend) and output token (what you buy)
BASE_MINT = os.getenv("BASE_MINT", "So11111111111111111111111111111111111111112")  # SOL
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "5000"))  # 50%

# Wallet
PRIVATE_KEY = os.getenv("SOLANA_PRIVATE_KEY")  # base58 or seed phrase export
if not PRIVATE_KEY:
    raise RuntimeError("SOLANA_PRIVATE_KEY env var is required for Bot F")
wallet_address = os.getenv("WALLET_ADDRESS")

# You’ll need to adapt this to however you store your key
kp = Keypair.from_base58_string(PRIVATE_KEY)
OWNER_PUBKEY = kp.pubkey()



# Ladder plan for PARTIAL_EXIT (fraction of current position)
PARTIAL_EXIT_FRACTION = float(os.getenv("PARTIAL_EXIT_FRACTION", "0.33"))


QUEUE_SELL = "QUEUE_SELL"
POSITIONS_KEY = "QUEUE_POSITIONS"

sol_client = Client(RPC_URL)




# -------------------------
# Logging Setup
# -------------------------
logger = logging.getLogger("trade_logger")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    "trade_activity.log",
    maxBytes=5_000_000,   # 5 MB
    backupCount=3,
    encoding="utf-8"
)

formatter = logging.Formatter(
    "%(asctime)s — %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

handler.setFormatter(formatter)
logger.addHandler(handler)



# ---------------------------------------
# Redis Cloud connection
# ---------------------------------------
r = redis.Redis(
    host=YOUR_HOST,
    port=int(YOUR_PORT),
    username="default",
    password=YOUR_PASSWORD,
    decode_responses=True
)


# ---------------------------------------
# Jupiter helpers
# ---------------------------------------


def get_jupiter_quote(input_mint: str, output_mint: str, amount_in: int, slippage_bps: int):
    url = "https://api.jup.ag/swap/v1/quote"

    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": int(amount_in),
        "slippageBps": slippage_bps,
        "swapMode": "ExactIn",
        "restrictIntermediateTokens": "true",
    }

    headers = {"x-api-key": JUPITER_API_KEY}

    resp = requests.get(url, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    quote_json = resp.json()

    if "routePlan" not in quote_json or not quote_json["routePlan"]:
        raise RuntimeError("No routePlan returned from Jupiter")

    return quote_json





def get_jupiter_swap_tx(quote, user_pubkey: Pubkey):
    url = "https://lite-api.jup.ag/swap/v1/swap"

    payload = {
        "quoteResponse": quote,
        "userPublicKey": str(user_pubkey),
        "wrapUnwrapSOL": True,          # ✅ auto‑unwrap WSOL → SOL
        "skipPreflight": False,
        "commitment": "processed",
        "dynamicComputeUnitLimit": True,
        "dynamicSlippage": True,
        "prioritizationFeeLamports": {
            "priorityLevelWithMaxLamports": {
                "maxLamports": 1_000_000,
                "priorityLevel": "veryHigh",
            }
        },
    }

    headers = {"x-api-key": JUPITER_API_KEY}

    resp = requests.post(url, json=payload, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    tx_b64 = data.get("swapTransaction")
    if not tx_b64:
        raise RuntimeError("No swapTransaction in Jupiter response")

    return tx_b64





def send_signed_tx(base64_tx: str, keypair: Keypair):
    try:
        raw_tx = b64decode(base64_tx)
        versioned_tx = VersionedTransaction.from_bytes(raw_tx)
    except Exception as e:
        print(f"[Bot F] ❌ Failed to decode Jupiter transaction: {e}")
        return None

    try:
        signed_tx = VersionedTransaction(versioned_tx.message, [keypair])
    except Exception as e:
        print(f"[Bot F] ❌ Failed to sign Jupiter transaction: {e}")
        return None

    try:
        resp = sol_client.send_raw_transaction(bytes(signed_tx))
        return str(resp.value)
    except Exception as e:
        print(f"[Bot F] ❌ Failed to send signed transaction: {e}")
        return None



# ---------------------------------------
# Positions helpers
# ---------------------------------------

def load_open_positions() -> List[Dict[str, Any]]:
    positions = []
    raw_list = r.lrange(POSITIONS_KEY, 0, -1)
    for raw in raw_list:
        try:
            positions.append(json.loads(raw))
        except Exception:
            pass
    return positions


def save_open_positions(positions: List[Dict[str, Any]]):
    # Simple rewrite strategy; you can optimize later
    r.delete(POSITIONS_KEY)
    if positions:
        r.rpush(POSITIONS_KEY, *[json.dumps(p) for p in positions])


def find_position_for_mint(positions: List[Dict[str, Any]], mint: str) -> Dict[str, Any] | None:
    for p in positions:
        if p.get("mint") == mint:
            return p
    return None


def get_token_balance_raw(mint: str, owner: str) -> int:
    # Option A: via Helius (fast, consistent with what we did in Bot C)
    url = f"https://api.helius.xyz/v0/addresses/{owner}/balances?api-key={HELIUS_API_KEY}"
    resp = requests.get(url, timeout=10)
    data = resp.json()

    for t in data.get("tokens", []):
        if t.get("mint") == mint:
            # Helius usually exposes raw amount as `amount` or `tokenAmount`
            return int(t.get("amount", 0))

    return 0


def get_token_balance_raw_rpc(mint: str, owner: str) -> int:
    owner_pk = Pubkey.from_string(owner)
    mint_pk = Pubkey.from_string(mint)

    total = 0

    try:
        resp = sol_client.get_token_accounts_by_owner_json_parsed(
            owner_pk,
            TokenAccountOpts(mint=mint_pk)
        )
    except Exception as e:
        print(f"[Bot F] RPC token balance fetch failed: {e}")
        return 0

    for acc in resp.value:
        try:
            ui = acc.account.data.parsed["info"]["tokenAmount"]
            amt = int(ui["amount"])
            total += amt
        except Exception:
            continue

    return total



# ---------------------------------------
# Logging
# ---------------------------------------

def log_execute_this_sell(symbol, mint, amount_in, quote, action, reason):
    print("\n========== Execute This Sell ==========")
    print(f"Token: {symbol} ({mint})")
    print(f"Exit Action: {action}")
    print(f"Reason: {reason}")
    print(f"Token Amount In: {amount_in}")
    print(f"Route Out Amount: {int(quote.get('outAmount', 0))}")
#    print(f"Price Impact: {quote.get('priceImpactPct', 0):.4f}")
    print("=======================================\n")


# ---------------------------------------
# Main loop
# ---------------------------------------

def run_seller():
    print("[Bot F] Seller worker started")

    while True:

        try:
            queue_name, raw = r.brpop(QUEUE_SELL, timeout=30)
        except redis.exceptions.TimeoutError:
            print("[Bot F] Redis timeout — reconnecting…")
            time.sleep(1)
            continue

        if raw is None:
            continue  # no message, loop again
      
        msg = json.loads(raw)

        mint = msg["mint"]
        symbol = msg.get("symbol", "UNKNOWN")
        action = msg.get("action", "FULL_EXIT")
        reason = msg.get("reason", "")

        print(f"\n[Bot F] → Received SELL signal: {symbol} ({mint}) action={action}")

        positions = load_open_positions()
        pos = find_position_for_mint(positions, mint)

        if not pos:
            print(f"[Bot F] No open position found for {symbol} ({mint}), skipping")
            continue


        current_amount = int(pos.get("amount_out") or pos.get("amount_in") or 0)
        if current_amount <= 0:
            print(f"[Bot F] Position for {symbol} has zero amount_out, skipping")
            continue

        if action == "FULL_EXIT":
            amount_to_sell = current_amount
        elif action == "PARTIAL_EXIT":
            amount_to_sell = max(1, int(current_amount * PARTIAL_EXIT_FRACTION))
        else:
            print(f"[Bot F] Unknown action {action}, skipping")
            continue


        # ----------------------------------------------------
        # On-chain balance detection (RPC first, Helius fallback)
        # ----------------------------------------------------
        onchain_raw = get_token_balance_raw_rpc(mint, wallet_address)

        if onchain_raw == 0:
            # fallback to Helius (rarely helps, but cheap)
            onchain_raw = get_token_balance_raw(mint, wallet_address)

        if onchain_raw <= 0:
            print(f"[Bot F] ❌ On-chain balance is zero for {symbol} — cannot sell. Keeping position open.")
            logger.warning(
                f"[SELL_ABORT_NO_BALANCE] {symbol} | mint={mint} | "
                f"recorded_raw={current_amount} | onchain_raw=0"
            )
            continue

        # ----------------------------------------------------
        # FULL_EXIT → trust on-chain balance completely
        # PARTIAL_EXIT → clamp to on-chain balance if needed
        # ----------------------------------------------------
        if action == "FULL_EXIT":
            amount_to_sell = onchain_raw

        elif action == "PARTIAL_EXIT":
            # already computed earlier, now clamp if needed
            if amount_to_sell > onchain_raw:
                print(
                    f"[Bot F] ⚠️ Recorded amount ({amount_to_sell}) exceeds on-chain balance "
                    f"({onchain_raw}). Clamping sell size."
                )
                logger.warning(
                    f"[SELL_CLAMP] {symbol} | mint={mint} | "
                    f"recorded_raw={current_amount} | onchain_raw={onchain_raw} | "
                    f"clamped_to={onchain_raw}"
                )
                amount_to_sell = onchain_raw


        # Market data snapshot at execution time
        md = fetch_market_overview(mint)
        price = md.get("price")
        liquidity = md.get("liquidity")
        v1m = md.get("v1m")
        v5m = md.get("v5m")
        trade1m = md.get("trade1m")
        trade5m = md.get("trade5m")

        try:
            quote = get_jupiter_quote(
                input_mint=mint,
                output_mint=BASE_MINT,
                amount_in=amount_to_sell,
                slippage_bps=SLIPPAGE_BPS,
            )
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 400:
                print(f"[Bot F] Permanent quote failure (400) for {symbol} — likely dust or illiquid. Skipping this SELL.")
                if amount_to_sell < 10_000:
                    print(f"[Bot F] Marking {symbol} as dust and closing position.")
                    positions = [p for p in positions if p.get("mint") != mint]
                    save_open_positions(positions)
                continue
            print(f"[Bot F] Failed to get Jupiter quote for {symbol}: {e}")
            continue
        except Exception as e:
            print(f"[Bot F] Failed to get Jupiter quote for {symbol}: {e}")
            continue



        # -------------------------
        # Convert outAmount → SOL
        # -------------------------
        out_raw = quote.get("outAmount", "0")

        try:
            sol_out_est = int(out_raw) / 1e9
        except ValueError:
            sol_out_est = float(out_raw) / 1e9

        sol_in = float(pos.get("sol_in", 0.0))
        pnl_sol_est = sol_out_est - sol_in

        log_execute_this_sell(symbol, mint, amount_to_sell, quote, action, reason)

        print(
            f"[Bot F] On-chain balance check → recorded={current_amount}, "
            f"onchain={onchain_raw}, selling={amount_to_sell}"
        )

        logger.info(
            f"[EXEC_SELL] {symbol} | mint={mint} | price={price} | "
            f"recorded_raw={current_amount} | onchain_raw={onchain_raw} | "
            f"amount_to_sell={amount_to_sell} | action={action} | "
            f"sol_in={sol_in:.8f} | sol_out_est={sol_out_est:.8f} | "
            f"pnl_sol_est={pnl_sol_est:.8f} | liquidity={liquidity} | "
            f"vol_1m={v1m} | vol_5m={v5m} | trades_1m={trade1m} | "
            f"trades_5m={trade5m} | reason={reason}"
        )

        try:
            swap_tx_b64 = get_jupiter_swap_tx(quote, OWNER_PUBKEY)
        except Exception as e:
            print(f"[Bot F] Failed to get Jupiter swap tx for {symbol}: {e}")
            continue

        try:
            sig = send_signed_tx(swap_tx_b64, kp)
        except Exception as e:
            print(f"[Bot F] Failed to send swap tx for {symbol}: {e}")
            continue

        if not sig:
            print(f"[Bot F] ❌ Sell FAILED for {symbol} — no signature returned & not updating position")
            continue

        time.sleep(5)

        if not confirm_tx_landed(sig):
            print(f"[Bot F] ❌ Sell FAILED on-chain — not updating position")
            continue


        print(f"[Bot F] ✅ Sell executed for {symbol} — tx sig: {sig} & confirmed on-chain — updating position")



        logger.info(
            f"[EXEC_SELL_CONFIRMED] {symbol} | mint={mint} | tx={sig} | "
            f"price={price} | amount_sold={amount_to_sell} | action={action}"
        )

        # ----------------------------------------------------
        # FULL_EXIT → always close the position
        # ----------------------------------------------------
        if action == "FULL_EXIT":
            positions = [p for p in positions if p.get("mint") != mint]
            print(f"[Bot F] Position closed for {symbol} ({mint}) ✅ Sell confirmed on-chain")
        else:
            # PARTIAL_EXIT → update remaining based on on-chain balance
            remaining = onchain_raw - amount_to_sell
            if remaining <= 0:
                positions = [p for p in positions if p.get("mint") != mint]
                print(f"[Bot F] Position closed for {symbol} ({mint})")
            else:
                pos["amount_out"] = remaining
                for i, p in enumerate(positions):
                    if p.get("mint") == mint:
                        positions[i] = pos
                        break
                print(f"[Bot F] Position reduced for {symbol} ({mint}) → remaining: {remaining}")

        save_open_positions(positions)
        time.sleep(0.2)


if __name__ == "__main__":
    run_seller()
