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
    print("\n--- RPC attempt ---")
    try:
        owner_pk = Pubkey.from_string(owner)
        mint_pk  = Pubkey.from_string(mint)

        resp = sol_client.get_token_accounts_by_owner(
            owner_pk,
            TokenAccountOpts(mint=mint_pk)
        )

        print(f"Type of resp: {type(resp)}")
        print(f"resp = {resp}")          # full raw response

        if isinstance(resp, dict):
            accounts = resp.get("result", {}).get("value", []) or []
            print("Treated as dict")
        else:
            accounts = getattr(resp, "value", None)
            print(f"Treated as object, .value = {accounts}")

        if not accounts:
            print("No accounts found in response")
            return 0

        total = 0
        for i, acc in enumerate(accounts):
            print(f"\nAccount [{i}] type: {type(acc)}")
            print(f"Account [{i}] content: {acc}")

            try:
                if isinstance(acc, dict):
                    parsed = acc.get("account", {}).get("data", {}).get("parsed", {})
                else:
                    parsed = acc.account.data.parsed

                print(f"Parsed: {parsed}")
                amt = int(parsed["info"]["tokenAmount"]["amount"])
                print(f"Extracted amount: {amt}")
                total += amt
            except Exception as e:
                print(f"Failed to parse account [{i}]: {e}")

        return total

    except Exception as e:
        print(f"RPC exception: {e}")
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
