import os
import json
import asyncio
import websockets
import redis
import ssl
import certifi
from datetime import datetime, timezone
from dotenv import load_dotenv

ssl_context = ssl.create_default_context(cafile=certifi.where())


load_dotenv()

HELIUS_WS_URL = os.getenv(
    "HELIUS_WS_URL",
    f"wss://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY')}"
)
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

YOUR_HOST = os.getenv("YOUR_HOST")
YOUR_PORT = int(os.getenv("YOUR_PORT", "6379"))
YOUR_PASSWORD = os.getenv("YOUR_PASSWORD")

PUMP_AMM_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

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

STREAM_TICKS = "STREAM_TICKS"
ACTIVE_TOKENS = "ACTIVE_TOKENS"  # shared with Bot D

# -----------------------------
# Helpers
# -----------------------------

def push_tick(msg: dict):
    """
    Push a normalized swap tick into Redis.
    """
    r.rpush(STREAM_TICKS, json.dumps(msg))


def normalize_pumpfun_swap(event: dict) -> dict | None:
    """
    Normalize pump.fun swap from Enhanced WS.
    Filters out:
      - non-pump AMM txs
      - mints not in ACTIVE_TOKENS
      - micro-transactions below 0.1 SOL
    """

    tx = event.get("transaction", {})
    meta = tx.get("meta", {})
    message = tx.get("transaction", {})

    # 1. Check pump.fun AMM program
    account_keys = message.get("message", {}).get("accountKeys", [])
    if not any(k.get("pubkey") == PUMP_AMM_PROGRAM for k in account_keys):
        return None

    # 2. Extract signature, slot, timestamp
    signature = event.get("signature")
    slot = event.get("slot")
    ts = event.get("blockTime") or int(datetime.now().timestamp())

    # 3. Extract token deltas
    pre = meta.get("preTokenBalances", [])
    post = meta.get("postTokenBalances", [])

    mint = None
    token_amount = None
    wallet = None

    for p, q in zip(pre, post):
        m = p.get("mint")
        if m and m.endswith("pump"):  # pump.fun only
            mint = m
            pre_amt = float(p["uiTokenAmount"]["uiAmount"] or 0)
            post_amt = float(q["uiTokenAmount"]["uiAmount"] or 0)
            token_amount = post_amt - pre_amt
            wallet = p.get("owner")
            break

    # If no pump.fun mint or no token movement, skip
    if not mint or token_amount == 0:
        return None

    # 3b. FILTER: only track mints that Bot D has marked as active
    if not r.sismember(ACTIVE_TOKENS, mint):
        return None

    # 4. Extract SOL delta
    pre_sol = meta.get("preBalances", [])
    post_sol = meta.get("postBalances", [])

    sol_amount = None
    if len(pre_sol) > 0 and len(post_sol) > 0:
        sol_amount = (post_sol[0] - pre_sol[0]) / 1e9  # lamports → SOL

    # If we can't compute SOL delta, skip
    if sol_amount is None:
        return None

    # 5. Detect LP add/remove events (huge SOL movements)
    if abs(sol_amount) > 50:
        return {
            "type": "lp_event",
            "mint": mint,
            "sol_amount": sol_amount,
            "token_amount": token_amount,
            "wallet": wallet,
            "slot": slot,
            "ts": ts,
            "signature": signature,
            "direction": "add" if sol_amount > 0 else "remove"
        }

    # 6. FILTER: ignore micro-transactions below 0.1 SOL
    if abs(sol_amount) < 0.1:
        return None

    # 7. Compute price
    price = None
    if token_amount != 0:
        price = abs(sol_amount) / abs(token_amount)

    # 8. Identify the trader (signer)
    trader = None
    for ak in account_keys:
        if ak.get("signer"):
            trader = ak.get("pubkey")
            break

    # If no signer found, skip (should be rare)
    if not trader:
        return None

    # 9. Correct buy/sell classification:
    # Trader losing SOL = buy
    # Trader gaining SOL = sell
    side = "buy" if sol_amount < 0 else "sell"

    # 10. Use trader as the wallet, not the pool
    wallet = trader

    return {
        "type": "swap_tick",
        "mint": mint,
        "price": price,
        "token_amount": token_amount,
        "sol_amount": sol_amount,
        "side": side,
        "wallet": wallet,
        "slot": slot,
        "ts": ts,
        "signature": signature,
    }


# -----------------------------
# WebSocket subscription payloads
# -----------------------------

def build_pumpfun_swaps_subscription():
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "transactionSubscribe",
        "params": [
            {
                "vote": False,
                "failed": False,
                "accountInclude": [
                    PUMP_AMM_PROGRAM  # pump.fun AMM
                ]
            },
            {
                "commitment": "processed",
                "encoding": "jsonParsed",
                "transactionDetails": "full",
                "showRewards": False,
                "maxSupportedTransactionVersion": 0
            }
        ]
    }


# -----------------------------
# Main WS loop
# -----------------------------

async def run_stream_ingester():
    print("[Bot S] Stream ingester starting...")
    print(f"[Bot S] Connecting to Helius WS: {HELIUS_WS_URL}")

    delay = 5  # start backoff at 5s

    while True:
        try:
            async with websockets.connect(
                HELIUS_WS_URL,
                ping_interval=20,
                ping_timeout=40,
                max_size=2**25,   # allow large bursts
                ssl=ssl_context
            ) as ws:
                # connection succeeded → reset backoff
                delay = 5

                sub_msg = build_pumpfun_swaps_subscription()
                await ws.send(json.dumps(sub_msg))
                print("[Bot S] Sent pump.fun SWAP subscription")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception as e:
                        print("[Bot S] JSON decode error:", e, "raw:", raw)
                        continue

                    if isinstance(msg, str):
                        continue

                    if isinstance(msg, dict) and "error" in msg:
                        print("[Bot S] Subscription error:", msg["error"])
                        continue

                    if not isinstance(msg, dict):
                        continue

                    if msg.get("method") != "transactionNotification":
                        continue

                    params = msg.get("params", {})
                    if not isinstance(params, dict):
                        continue

                    result = params.get("result")
                    if not result:
                        continue

                    txs = result if isinstance(result, list) else [result]

                    for tx in txs:
                        if not isinstance(tx, dict):
                            continue
                        tick = normalize_pumpfun_swap(tx)
                        if tick:
                            push_tick(tick)
                            print(
                                f"[Bot S] tick → mint={tick['mint']} "
                                f"side={tick['side']} "
                                f"sol={tick['sol_amount']} "
                                f"sig={tick['signature']}"
                            )

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"[Bot S] WebSocket closed: {e}. Reconnecting in {delay}s...")
        except Exception as e:
            print(f"[Bot S] WebSocket error: {e}. Reconnecting in {delay}s...")

        # backoff before next reconnect attempt
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)

if __name__ == "__main__":
    asyncio.run(run_stream_ingester())
