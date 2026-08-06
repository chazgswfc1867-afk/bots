# bots/bot_c_buyer.py

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
from typing import List, Union, Any, Optional
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
from solders.signature import Signature as SoldersSignature
from solders.address_lookup_table_account import AddressLookupTableAccount
from spl.token.instructions import get_associated_token_address
from spl.token.instructions import close_account, CloseAccountParams
from spl.token.instructions import create_associated_token_account as spl_create_ata
from spl.token._layouts import MINT_LAYOUT
from spl.token.constants import TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID
from dotenv import load_dotenv
load_dotenv()
from shared.utils import fetch_wallet_velocity, fetch_market_overview, confirm_tx_landed

# ---------------------------------------
# ENV / CONFIG
# ---------------------------------------

RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"
JUPITER_SWAP_URL = "https://lite-api.jup.ag/swap/v1/swap"
JUPITER_API_KEY = os.getenv("JUPITER_API_KEY")

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

YOUR_HOST = os.getenv("YOUR_HOST")
YOUR_PORT = os.getenv("YOUR_PORT")
YOUR_PASSWORD = os.getenv("YOUR_PASSWORD")

TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# Base token (what you spend) and output token (what you buy)
BASE_MINT = os.getenv("BASE_MINT", "So11111111111111111111111111111111111111112")  # SOL
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "10000"))  # Was 50%, increased to 100%

# Wallet
PRIVATE_KEY = os.getenv("SOLANA_PRIVATE_KEY")  # base58 or seed phrase export
if not PRIVATE_KEY:
    raise RuntimeError("SOLANA_PRIVATE_KEY env var is required for Bot C")
wallet_address = os.getenv("WALLET_ADDRESS")

kp = Keypair.from_base58_string(PRIVATE_KEY)
OWNER_PUBKEY = kp.pubkey()

QUEUE_IN = "QUEUE_BUY"
QUEUE_POSITIONS = "QUEUE_POSITIONS"

sol_client = Client(RPC_URL)

# -------------------------
# Logging Setup
# -------------------------
logger = logging.getLogger("trade_logger")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    "trade_activity.log",
    maxBytes=5_000_000,
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

def load_wallet():
    key = os.getenv("SOLANA_PRIVATE_KEY")
    if not key:
        raise ValueError("Missing SOLANA_PRIVATE_KEY in environment")

    decoded = base58.b58decode(key)
    return Keypair.from_bytes(decoded)


def get_wallet_balance(pubkey, client):
    try:
        response = client.get_balance(pubkey)
        lamports = response.value
        sol = lamports / 1e9
        return sol
    except Exception as e:
        logger.warning(f"Failed to fetch wallet balance: {e}")
        return None


def setup_wallet():
    wallet = load_wallet()
    client = Client("https://api.mainnet-beta.solana.com")
    sol_balance = get_wallet_balance(wallet.pubkey(), client)
    # fetch_sol_price() omitted in this snippet; keep your existing implementation

    return wallet, client, sol_balance


def get_ata(owner_pubkey: str, mint: str, program_id: Pubkey = TOKEN_PROGRAM_ID) -> Pubkey:
    return get_associated_token_address(
        owner=Pubkey.from_string(owner_pubkey),
        mint=Pubkey.from_string(mint),
        token_program_id=program_id
    )


def get_jupiter_quote(input_mint: str, output_mint: str, amount_in: int, slippage_bps: int):
    url = JUPITER_QUOTE_URL

    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": int(amount_in),
        "slippageBps": slippage_bps,
        "swapMode": "ExactIn",
        "restrictIntermediateTokens": "true"
    }

    headers = {
        "x-api-key": JUPITER_API_KEY
    }

    resp = requests.get(url, params=params, headers=headers, timeout=10)

    if resp.status_code == 401:
        raise RuntimeError("Jupiter API key rejected (401 Unauthorized)")

    if not resp.ok:
        raise RuntimeError(f"Jupiter quote failed: {resp.status_code} — {resp.text}")

    quote_json = resp.json()

    if "routePlan" not in quote_json or not quote_json["routePlan"]:
        raise RuntimeError("No routePlan returned from Jupiter")

    return quote_json


def get_jupiter_swap_tx(quote, user_pubkey: Pubkey):
    route = quote
    output_mint = route.get("outputMint")
    if not output_mint:
        raise RuntimeError("Quote missing outputMint")

    existing_ata_address = str(
        get_ata(str(user_pubkey), output_mint)
    )

    payload = {
        "quoteResponse": route,
        "userPublicKey": str(user_pubkey),
        "wrapUnwrapSOL": True,
        "skipPreflight": False,
        "commitment": "processed",

        # Ultra-fast: disable dynamic features
        "dynamicComputeUnitLimit": False,
        "dynamicSlippage": False,

        # Let Jupiter choose best route without extra filtering
        "restrictIntermediateTokens": False,

        # No prioritization fee logic here (you can add static later)
        "prioritizationFeeLamports": None,

        # Exclude existing ATA to avoid extra account creation
        "excludeTokenAccounts": [existing_ata_address],
    }

    resp = requests.post(JUPITER_SWAP_URL, json=payload, timeout=5)
    resp.raise_for_status()
    data = resp.json()

    if "swapTransaction" not in data:
        raise RuntimeError("No swapTransaction in Jupiter response")

    return data["swapTransaction"]  # base64 tx



def send_signed_tx(base64_tx: str, keypair: Keypair):
    try:
        raw = b64decode(base64_tx)
        tx = VersionedTransaction.from_bytes(raw)
    except Exception as e:
        print(f"[Bot C] Failed to decode Jupiter transaction: {e}")
        return None

    try:
        signed_tx = VersionedTransaction(tx.message, [keypair])
    except Exception as e:
        print(f"[Bot C] Failed to sign Jupiter transaction: {e}")
        return None

    try:
        resp = sol_client.send_raw_transaction(bytes(signed_tx))
        return str(resp.value)
    except Exception as e:
        print(f"[Bot C] Failed to send signed transaction: {e}")
        return None

# ---------------------------------------
# Logging helpers
# ---------------------------------------

def log_execute_this_buy(symbol, mint, amount_base, quote, risk_score, risk_action):
    print("\n========== Execute This Buy ==========")
    print(f"Token: {symbol} ({mint})")
    print(f"Base Spend: {amount_base / 1e9:.4f} SOL")
    print(f"Route Out Amount: {int(quote.get('outAmount', 0))}")
    print(f"Risk Score: {risk_score} | Risk Action: {risk_action}")
    print(f"Date: {datetime.now(timezone.utc).isoformat()}")
    print("======================================\n")

    # ----------------------------------------------------
    # Fetch diff snapshot from Bot X
    # ----------------------------------------------------
    diff_snapshot = fetch_latest_diff_snapshot(mint)

    if diff_snapshot:
        print("\n========== Pre-Buy Signal Snapshot ==========")
        print(f"Mint: {mint}")
        print(f"Diff 5s:  {diff_snapshot.get('diff_5s')}")
        print(f"Diff 7s:  {diff_snapshot.get('diff_7s')}")
        print(f"Diff 10s: {diff_snapshot.get('diff_10s')}")
        print(f"Date: {datetime.now(timezone.utc).isoformat()}")
        print("==========================================\n")
    else:
        print("[Bot C] No diff snapshot found for this mint")



# ---------------------------------------
# Helpers
# ---------------------------------------

def get_token_decimals(mint: str) -> int:
    info = sol_client.get_token_supply(mint)
    return int(info["result"]["value"]["decimals"])




def get_actual_token_amount_helius(sig: str, mint: str, owner: str) -> int:
    url = f"https://api.helius.xyz/v0/transactions/?api-key={HELIUS_API_KEY}"
    resp = requests.post(url, json={"transactions": [sig]}, timeout=10)
    data = resp.json()

    if not data or "tokenTransfers" not in data[0]:
        return 0

    for t in data[0]["tokenTransfers"]:
        if t.get("mint") == mint and t.get("toUserAccount") == owner:
            return int(t.get("tokenAmount", 0))

    return 0






def build_position_record(
    symbol,
    mint,
    amount_in_lamports,
    amount_out_raw,
    decimals,
    sig,
    consensus_info,
    risk_score,
    risk_action,
):
    amount_in_sol = amount_in_lamports / 1e9
    tokens_ui = amount_out_raw / (10 ** decimals)

    return {
        "symbol": symbol,
        "mint": mint,
        "base_mint": BASE_MINT,
        "amount_in": amount_in_lamports,
        "amount_out": amount_out_raw,
        "decimals": decimals,
        "sol_in": amount_in_sol,
        "tokens_in": tokens_ui,
        "tx_sig": sig,
        "consensus_info": consensus_info,
        "risk_score": risk_score,
        "risk_action": risk_action,
        "timestamp": int(time.time()),
        "iso_time": datetime.now(timezone.utc).isoformat(),
    }

# ---------------------------------------------------------
# Shared helpers for reading/writing open positions
# ---------------------------------------------------------

def load_open_positions():
    positions = []
    raw_list = r.lrange(QUEUE_POSITIONS, 0, -1)
    for raw in raw_list:
        try:
            positions.append(json.loads(raw))
        except:
            pass
    return positions


def save_open_positions(positions):
    r.delete(QUEUE_POSITIONS)
    for p in positions:
        r.rpush(QUEUE_POSITIONS, json.dumps(p))

# ---------------------------------------------------------
# Actual token amount from executed transaction
# ---------------------------------------------------------

def get_actual_token_delta(sig: str, mint: str, owner_str: str):
    """
    Fetch the confirmed transaction and compute the actual token amount
    received for (mint, owner).
    Returns (delta_raw, decimals).
    """

    # Convert signature
    try:
        sig_obj = SoldersSignature.from_string(sig)
    except Exception:
        sig_obj = sig

    # Fetch transaction
    try:
        tx = sol_client.get_transaction(sig_obj, encoding="jsonParsed")
    except Exception as e:
        raise RuntimeError(f"RPC get_transaction failed: {e}")

    if tx.value is None:
        return 0, 0

    meta = tx.value.meta
    if meta is None:
        return 0, 0

    pre = meta.pre_token_balances or []
    post = meta.post_token_balances or []

    pre_amt = 0
    post_amt = 0
    decimals = 0

    # Find pre-balance
    for b in pre:
        if b.mint == mint and b.owner == owner_str:
            pre_amt = int(b.ui_token_amount.amount)
            decimals = b.ui_token_amount.decimals

    # Find post-balance
    for b in post:
        if b.mint == mint and b.owner == owner_str:
            post_amt = int(b.ui_token_amount.amount)
            decimals = b.ui_token_amount.decimals

    delta_raw = post_amt - pre_amt
    return delta_raw, decimals



def fetch_latest_diff_snapshot(mint: str):
    """
    Fetch the most recent diff snapshot for a given mint from STREAM_DIFF_TICKS.
    Returns dict or None.
    """
    raw_list = r.lrange("STREAM_DIFF_TICKS", -200, -1)  # last 200 entries

    latest = None
    for raw in reversed(raw_list):
        try:
            snap = json.loads(raw)
        except:
            continue

        if snap.get("mint") == mint:
            latest = snap
            break

    return latest



# ---------------------------------------
# Main loop
# ---------------------------------------

def run_buyer():
    print("[Bot C] Buyer worker started")

    while True:
        queue_name, raw = r.brpop(QUEUE_IN)
        msg = json.loads(raw)

        symbol = msg.get("symbol", "UNKNOWN")
        mint = msg["mint"]
        consensus_info = msg.get("consensus_info", "")
        risk_score_val = msg.get("risk_score", 0)
        risk_action = msg.get("risk_action", "OK")

        print(f"\n[Bot C] → Received BUY candidate: {symbol} ({mint})")

        if risk_action in ("HALT_BUYS", "SELL_100"):
            print(f"[Bot C] Skipping {symbol} due to risk_action={risk_action}")
            continue

        amount_base_sol = float(os.getenv("BUY_SIZE_SOL", "0.01"))
        amount_base_lamports = int(amount_base_sol * 1e9)

        # ----------------------------------------------------
        # Jupiter Quote
        # ----------------------------------------------------
        try:
            quote = get_jupiter_quote(
                input_mint=BASE_MINT,
                output_mint=mint,
                amount_in=amount_base_lamports,
                slippage_bps=SLIPPAGE_BPS,
            )
        except Exception as e:
            print(f"[Bot C] Failed to get Jupiter quote for {symbol}: {e}")
            continue

        # ----------------------------------------------------
        # Market Overview
        # ----------------------------------------------------
        md = fetch_market_overview(mint)

        price = md.get("price")
        liquidity = md.get("liquidity")
        v1m = md.get("v1m")
        v5m = md.get("v5m")
        trade1m = md.get("trade1m")
        trade5m = md.get("trade5m")

        logger.info(
            f"[EXEC_BUY] {symbol} | mint={mint} | "
            f"price={price} | spend_sol={amount_base_sol} | "
            f"time={datetime} | "
            f"liquidity={liquidity} | vol_1m={v1m} | vol_5m={v5m} | "
            f"trades_1m={trade1m} | trades_5m={trade5m} | "
            f"risk_score={risk_score_val} | risk_action={risk_action} | "
            f"consensus={consensus_info}"
        )

        log_execute_this_buy(
            symbol,
            mint,
            amount_base_lamports,
            quote,
            risk_score_val,
            risk_action
        )

        # ----------------------------------------------------
        # Build Swap TX
        # ----------------------------------------------------
        try:
            swap_tx_b64 = get_jupiter_swap_tx(quote, OWNER_PUBKEY)
        except Exception as e:
            print(f"[Bot C] Failed to get Jupiter swap tx for {symbol}: {e}")
            continue

        # ----------------------------------------------------
        # Execute Swap
        # ----------------------------------------------------
        try:
            sig = send_signed_tx(swap_tx_b64, kp)
        except Exception as e:
            print(f"[Bot C] Failed to send swap tx for {symbol}: {e}")
            continue

        if not sig:
            print(f"[Bot C] Buy FAILED for {symbol} — not recording position")
            continue
 
        time.sleep(5)

        if not confirm_tx_landed(sig):
            print(f"[Bot C] ❌ Buy FAILED on-chain — not recording position")
            continue

        print(f"[Bot C] ✅ Buy confirmed on-chain — recording position")


        # ----------------------------------------------------
        # Fetch diff snapshot from Bot X
        # ----------------------------------------------------
        diff_snapshot = fetch_latest_diff_snapshot(mint)

        if diff_snapshot:
            print("\n========== Post-Buy Signal Snapshot ==========")
            print(f"Mint: {mint}")
            print(f"Diff 5s:  {diff_snapshot.get('diff_5s')}")
            print(f"Diff 7s:  {diff_snapshot.get('diff_7s')}")
            print(f"Diff 10s: {diff_snapshot.get('diff_10s')}")
            print(f"Date: {datetime.now(timezone.utc).isoformat()}")
            print("==========================================\n")
        else:
            print("[Bot C] No diff snapshot found for this mint")


        # ----------------------------------------------------
        # Token Amounts (actual from chain, fallback to quote)
        # ----------------------------------------------------
        # Always get decimals first
        try:
            decimals = get_token_decimals(mint)
        except Exception:
            decimals = 6

        try:
            out_amount_raw = get_actual_token_amount_helius(sig, mint, str(OWNER_PUBKEY))
            if out_amount_raw <= 0:
                out_amount_raw = int(quote.get("outAmount", 0))
        except Exception as e:
            print(f"[Bot C] Failed to fetch actual token amount for {symbol}: {e}")
            out_amount_raw = int(quote.get("outAmount", 0))

        tokens_ui = out_amount_raw / (10 ** decimals)

        logger.info(
            f"[EXEC_BUY_CONFIRMED] {symbol} | mint={mint} | "
            f"tx={sig} | price={price} | "
            f"amount_received_raw={out_amount_raw} | amount_received={tokens_ui} | "
            f"decimals={decimals} | spend_sol={amount_base_sol}"
        )


        # ----------------------------------------------------
        # AGGREGATE MULTIPLE BUYS
        # ----------------------------------------------------
        positions = load_open_positions()
        existing = None

        for p in positions:
            if p.get("mint") == mint:
                existing = p
                break

        if existing:
            existing["amount_in"] += amount_base_lamports
            existing["amount_out"] += out_amount_raw
            existing["sol_in"] += amount_base_sol
            existing["tokens_in"] += tokens_ui

            existing["timestamp"] = min(
                existing.get("timestamp", int(time.time())),
                int(time.time())
            )

            new_positions = [p for p in positions if p.get("mint") != mint]
            new_positions.append(existing)
            save_open_positions(new_positions)

            print(f"[Bot C] Aggregated BUY for {symbol}: now holding {existing['tokens_in']} tokens")

        else:
            position = build_position_record(
                symbol=symbol,
                mint=mint,
                amount_in_lamports=amount_base_lamports,
                amount_out_raw=out_amount_raw,
                decimals=decimals,
                sig=sig,
                consensus_info=consensus_info,
                risk_score=risk_score_val,
                risk_action=risk_action,
            )

            # Attach diff snapshot
            position["diff_snapshot"] = diff_snapshot

            positions.append(position)
            save_open_positions(positions)

            print(f"[Bot C] New position recorded for {symbol}")

        time.sleep(0.2)


if __name__ == "__main__":
    run_buyer()
