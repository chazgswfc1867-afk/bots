import os
import json
import time
import redis
from collections import deque, defaultdict
from dotenv import load_dotenv

load_dotenv()

YOUR_HOST = os.getenv("YOUR_HOST")
YOUR_PORT = int(os.getenv("YOUR_PORT", "6379"))
YOUR_PASSWORD = os.getenv("YOUR_PASSWORD")

STREAM_TICKS = "STREAM_TICKS"
STREAM_WALLET_VELOCITY = "STREAM_WALLET_VELOCITY"

r = redis.Redis(
    host=YOUR_HOST,
    port=YOUR_PORT,
    username="default",
    password=YOUR_PASSWORD,
    decode_responses=True,
)

# -----------------------------
# Per-mint wallet velocity state
# -----------------------------

class WalletVelocityState:
    def __init__(self, mint: str, window_sec: int = 5):
        self.mint = mint

        # Current 1s bucket
        self.bucket_ts = None
        self.wallets_buy = set()
        self.wallets_sell = set()
        self.trades_buy = 0
        self.trades_sell = 0

        # Rolling history of wallet counts for velocity
        self.window_sec = window_sec
        self.history = deque(maxlen=window_sec)  # list of wallets_total per second

    def ingest_tick(self, tick: dict):
        ts = tick.get("ts")
        wallet = tick.get("wallet")
        side = tick.get("side")

        if ts is None or wallet is None or side not in ("buy", "sell"):
            return None

        bucket = int(ts)

        # First bucket
        if self.bucket_ts is None:
            self.bucket_ts = bucket

        # Same bucket → accumulate
        if bucket == self.bucket_ts:
            self._accumulate(wallet, side)
            return None

        # New bucket → close previous, emit snapshot, start new
        snapshot = self._build_snapshot()
        self._roll_history(snapshot)

        # Start new bucket
        self._reset_bucket(bucket)
        self._accumulate(wallet, side)

        return snapshot

    def _accumulate(self, wallet: str, side: str):
        if side == "buy":
            self.wallets_buy.add(wallet)
            self.trades_buy += 1
        else:
            self.wallets_sell.add(wallet)
            self.trades_sell += 1

    def _reset_bucket(self, bucket_ts: int):
        self.bucket_ts = bucket_ts
        self.wallets_buy = set()
        self.wallets_sell = set()
        self.trades_buy = 0
        self.trades_sell = 0

    def _build_snapshot(self) -> dict:
        wallets_buy_count = len(self.wallets_buy)
        wallets_sell_count = len(self.wallets_sell)
        wallets_total = len(self.wallets_buy.union(self.wallets_sell))

        trades_total = self.trades_buy + self.trades_sell

        buy_wallet_ratio = (
            wallets_buy_count / wallets_total if wallets_total > 0 else None
        )
        buy_trade_ratio = (
            self.trades_buy / trades_total if trades_total > 0 else None
        )

        # Velocity: average wallets_total over last N seconds
        if len(self.history) > 0:
            wallet_velocity_5s = sum(self.history) / len(self.history)
        else:
            wallet_velocity_5s = None

        return {
            "type": "wallet_velocity",
            "mint": self.mint,
            "tf": "1s",
            "t_open": self.bucket_ts,
            "t_close": self.bucket_ts + 1 if self.bucket_ts is not None else None,

            "wallets_total": wallets_total,
            "wallets_buy": wallets_buy_count,
            "wallets_sell": wallets_sell_count,

            "trades_total": trades_total,
            "trades_buy": self.trades_buy,
            "trades_sell": self.trades_sell,

            "wallet_velocity_5s": wallet_velocity_5s,
            "buy_wallet_ratio": buy_wallet_ratio,
            "buy_trade_ratio": buy_trade_ratio,
        }

    def _roll_history(self, snapshot: dict):
        wallets_total = snapshot["wallets_total"]
        self.history.append(wallets_total)


# -----------------------------
# Main loop
# -----------------------------

def run_wallet_velocity_engine():
    print("[Bot W] Wallet velocity engine starting...")
    print(f"[Bot W] Connecting to Redis at {YOUR_HOST}:{YOUR_PORT}")

    mint_states: dict[str, WalletVelocityState] = {}
    idx = 0  # index into STREAM_TICKS

    while True:
        try:
            length = r.llen(STREAM_TICKS)
            if idx >= length:
                time.sleep(0.1)
                continue

            raw = r.lindex(STREAM_TICKS, idx)
            idx += 1

            if not raw:
                continue

            try:
                tick = json.loads(raw)
            except json.JSONDecodeError:
                print("[Bot W] JSON decode error:", raw)
                continue

            if tick.get("type") != "swap_tick":
                continue

            mint = tick.get("mint")
            if not mint:
                continue

            state = mint_states.get(mint)
            if state is None:
                state = WalletVelocityState(mint)
                mint_states[mint] = state

            snapshot = state.ingest_tick(tick)
            if snapshot:
                r.rpush(STREAM_WALLET_VELOCITY, json.dumps(snapshot))
                print(
                    f"[Bot W] snapshot → mint={mint} "
                    f"wallets_total={snapshot['wallets_total']} "
                    f"buy_wallet_ratio={snapshot.get('buy_wallet_ratio')} "
                    f"wallet_velocity_5s={snapshot.get('wallet_velocity_5s')}"
                )

        except Exception as e:
            print(f"[Bot W] Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    run_wallet_velocity_engine()
