# test_balance.py
import os
from dotenv import load_dotenv
load_dotenv()

from solders.pubkey import Pubkey
from solana.rpc.api import Client
from solana.rpc.types import TokenAccountOpts
import requests

RPC_URL = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
WALLET = os.getenv("WALLET_ADDRESS")

sol_client = Client(RPC_URL)


def get_token_balance_raw_safe(mint: str, owner: str) -> int:
    """Defensive token balance reader – forces jsonParsed."""
    try:
        owner_pk = Pubkey.from_string(owner)
        mint_pk  = Pubkey.from_string(mint)

        # Force jsonParsed encoding
        resp = sol_client.get_token_accounts_by_owner(
            owner_pk,
            TokenAccountOpts(mint=mint_pk, encoding="jsonParsed")
        )

        # Handle both dict and object responses
        if isinstance(resp, dict):
            accounts = resp.get("result", {}).get("value", []) or []
        else:
            accounts = getattr(resp, "value", []) or []

        total = 0
        for acc in accounts:
            try:
                if isinstance(acc, dict):
                    parsed = acc["account"]["data"]["parsed"]
                else:
                    parsed = acc.account.data.parsed

                amt = int(parsed["info"]["tokenAmount"]["amount"])
                total += amt
            except Exception as e:
                print(f"Parse error: {e}")
                continue

        return total

    except Exception as e:
        print(f"RPC failed: {e}")
        return 0


if __name__ == "__main__":
    # ←←← PUT A MINT YOU CURRENTLY HOLD HERE
    TEST_MINT = "E1f3mmy2u3JrPxdESVeDGNnjKpAPGsiiVVECzdh9pump"   # example – change this

    print(f"Wallet : {WALLET}")
    print(f"Mint   : {TEST_MINT}")
    print("-" * 50)

    bal = get_token_balance_raw_safe(TEST_MINT, WALLET)
    print(f"Raw balance returned: {bal}")

    if bal > 0:
        print("✅ SUCCESS – non-zero balance detected")
    else:
        print("❌ Returned 0 – check RPC / Helius / mint / wallet")
