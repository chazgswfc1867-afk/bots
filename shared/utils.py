# shared/utils.py

import time
import json
import requests
import os
from dotenv import load_dotenv
load_dotenv()

BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")


def fetch_wallet_velocity(mint):
    """
    Fetches wallet velocity fields from BirdEye token_overview.
    Returns dict with None defaults if unavailable.
    """
    url = "https://public-api.birdeye.so/defi/token_overview"
    headers = {"X-API-KEY": BIRDEYE_API_KEY}
    params = {"address": mint}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=5)

        # 🔍 DEBUGGER — print the raw JSON BirdEye returns
        try:
            print("[DEBUG] token_overview raw:", resp.json())
        except Exception as e:
            print("[DEBUG] token_overview raw: <invalid JSON>", e)

        if not resp.ok:
            return {}

        data = resp.json().get("data", {})

        return {
            "unique_wallet_1m": data.get("uniqueWallet1m"),
            "unique_wallet_1m_change_percent": data.get("uniqueWallet1mChangePercent"),
            "unique_wallet_5m_change_percent": data.get("uniqueWallet5mChangePercent"),
        }

    except Exception as e:
        print("[DEBUG] token_overview exception:", e)
        return {}



def fetch_market_overview(mint):
    """
    Fetches full token overview from BirdEye token_overview.
    Includes price, liquidity, volume, trade counts, and wallet velocity.
    Returns dict with None defaults if unavailable.
    """
    url = "https://public-api.birdeye.so/defi/token_overview"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": "solana"}
    params = {"address": mint}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=5)
        data = resp.json().get("data", {})

        return {
            "price": data.get("price"),
            "liquidity": data.get("liquidity"),

            # Volume
            "v1m": data.get("v1m"),
            "v5m": data.get("v5m"),

            # Trades
            "trade1m": data.get("trade1m"),
            "trade5m": data.get("trade5m"),

            # Price change %
            "priceChange1mPercent": data.get("priceChange1mPercent"),
            "priceChange5mPercent": data.get("priceChange5mPercent"),

            # Wallet velocity (already used)
            "uniqueWallet1m": data.get("uniqueWallet1m"),
            "uniqueWallet1mChangePercent": data.get("uniqueWallet1mChangePercent"),
            "uniqueWallet5m": data.get("uniqueWallet5m"),
            "uniqueWallet5mChangePercent": data.get("uniqueWallet5mChangePercent"),
        }

    except Exception as e:
        print("[DEBUG] token_overview exception:", e)
        return {}




def confirm_tx_landed(signature: str) -> bool:
    url = f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}"
    payload = {"transactions": [signature]}

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            return False

        data = resp.json()

        # Empty array → transaction not found on-chain
        if not data:
            return False

        tx = data[0]

        # If transactionError is null → success
        if tx.get("transactionError") is None:
            return True

        # Otherwise → failed
        return False

    except Exception:
        return False
