# bots/bot_x-diff-metric_engine.py

import os
import json
import time
import redis
from collections import deque
from dotenv import load_dotenv

load_dotenv()

YOUR_HOST = os.getenv("YOUR_HOST")
YOUR_PORT = int(os.getenv("YOUR_PORT", "6379"))
YOUR_PASSWORD = os.getenv("YOUR_PASSWORD")

STREAM_TICKS = "STREAM_TICKS"
STREAM_DIFF_TICKS = "STREAM_DIFF_TICKS"
STREAM_DIFF_METRIC = "QUEUE_BUY"

r = redis.Redis(
    host=YOUR_HOST,
    port=YOUR_PORT,
    username="default",
    password=YOUR_PASSWORD,
    decode_responses=True,
)

# ---------------------------------------------------------
# Per-mint time-window diff metric state
# ---------------------------------------------------------

class DiffMetricState:
    def __init__(self, mint: str):
        self.mint = mint

        # Time-based windows: store (timestamp, weighted_value)
        self.win2s  = deque()
        self.win3s  = deque()
        self.win5s  = deque()
        self.win7s  = deque()
        self.win10s = deque()

        # Previous sums for diff calculation
        self.prev_sum2s  = 0.0
        self.prev_sum3s  = 0.0
        self.prev_sum5s  = 0.0
        self.prev_sum7s  = 0.0
        self.prev_sum10s = 0.0

        # Price history for "already ran" guard  (ts, price)
        self.recent_prices = deque(maxlen=40)

        # Cooldown
        self.last_signal_ts = 0

    # -----------------------------
    # Helper: prune old entries
    # -----------------------------
    def prune(self, window: deque, now_ts: int, seconds: int):
        cutoff = now_ts - seconds
        while window and window[0][0] < cutoff:
            window.popleft()

    # -----------------------------
    # Main ingestion
    # -----------------------------
    def ingest_tick(self, tick: dict, wallet_velocity_snapshot: dict | None = None):
        """
        wallet_velocity_snapshot: optional latest snapshot from Bot W
        (pass None if you don't want the unique-buyer filter yet)
        """
        side = tick.get("side")
        ts   = tick.get("ts")

        if side not in ("buy", "sell") or ts is None:
            return None

        # ---------- Weighting ----------
        sol = abs(float(tick.get("sol_amount", 0)))
        if side == "buy":
            weight = min(sol, 30.0)          # let real size matter on the buy side
        else:
            weight = min(sol, 8.0)           # still dampen large dumps a bit

        val = weight if side == "buy" else -weight

        # Append to all windows
        self.win2s.append((ts, val))
        self.win3s.append((ts, val))
        self.win5s.append((ts, val))
        self.win7s.append((ts, val))
        self.win10s.append((ts, val))

        # Prune
        self.prune(self.win2s,  ts, 2)
        self.prune(self.win3s,  ts, 3)
        self.prune(self.win5s,  ts, 5)
        self.prune(self.win7s,  ts, 7)
        self.prune(self.win10s, ts, 10)

        # Current sums
        sum2s  = sum(v for (_, v) in self.win2s)
        sum3s  = sum(v for (_, v) in self.win3s)
        sum5s  = sum(v for (_, v) in self.win5s)
        sum7s  = sum(v for (_, v) in self.win7s)
        sum10s = sum(v for (_, v) in self.win10s)

        # Diffs vs previous tick
        diff2s  = sum2s  - self.prev_sum2s
        diff3s  = sum3s  - self.prev_sum3s
        diff5s  = sum5s  - self.prev_sum5s
        diff7s  = sum7s  - self.prev_sum7s
        diff10s = sum10s - self.prev_sum10s

        # Store for next iteration
        self.prev_sum2s  = sum2s
        self.prev_sum3s  = sum3s
        self.prev_sum5s  = sum5s
        self.prev_sum7s  = sum7s
        self.prev_sum10s = sum10s

        # ---------- Price history ----------
        price = tick.get("price")
        if price is not None and price > 0:
            self.recent_prices.append((ts, float(price)))

        # -------------------------------------------------
        # Signal logic
        # -------------------------------------------------

        # 1. Volume / acceleration (prefers the onset)
        volume_signal = (
            (diff2s >= 9  and sum2s < 35) or
            (diff3s >= 12 and sum3s < 45) or
            (diff5s >= 14 and sum3s > 10 and sum5s < 60) or
            (diff7s >= 16 and sum5s < 70)
        )

        # 2. Wallet-velocity filter
        unique_buyers_ok = True
        if wallet_velocity_snapshot is not None:
            wallets_buy      = wallet_velocity_snapshot.get("wallets_buy", 0) or 0
            buy_wallet_ratio = wallet_velocity_snapshot.get("buy_wallet_ratio") or 0.0

            unique_buyers_ok = (
                wallets_buy >= 3 and
                buy_wallet_ratio >= 0.65
            )

        # 3. Price-from-low guard (reject if already ran hard)
        price_ok = True
        if len(self.recent_prices) >= 3:
            prices = [p for _, p in self.recent_prices]
            low = min(prices)
            current = prices[-1]
            if low > 0 and (current - low) / low > 0.22:   # >22 % already → too late
                price_ok = False

        # 4. Cooldown (one signal per mint every 12 s)
        cooldown_ok = (ts - self.last_signal_ts) > 12

        signal_buy  = volume_signal and unique_buyers_ok and price_ok and cooldown_ok
        signal_sell = (diff7s <= -10) or (diff10s <= -12)

        if signal_buy:
            self.last_signal_ts = ts

        snapshot = {
            "type": "diff_metric",
            "mint": self.mint,
            "ts": ts,

            "sum_2s":  sum2s,
            "sum_3s":  sum3s,
            "sum_5s":  sum5s,
            "sum_7s":  sum7s,
            "sum_10s": sum10s,

            "diff_2s":  diff2s,
            "diff_3s":  diff3s,
            "diff_5s":  diff5s,
            "diff_7s":  diff7s,
            "diff_10s": diff10s,

            "signal_buy":  signal_buy,
            "signal_sell": signal_sell,

            # debugging
            "volume_signal":     volume_signal,
            "unique_buyers_ok":  unique_buyers_ok,
            "price_ok":          price_ok,
            "cooldown_ok":       cooldown_ok,
        }

        return snapshot


def fetch_latest_wallet_velocity(mint: str, lookback: int = 40):
    """
    Return the most recent wallet-velocity snapshot for `mint`
    from STREAM_WALLET_VELOCITY, or None if nothing found.
    """
    raw_list = r.lrange("STREAM_WALLET_VELOCITY", -lookback, -1)

    for raw in reversed(raw_list):
        try:
            snap = json.loads(raw)
        except Exception:
            continue

        if snap.get("mint") == mint and snap.get("type") == "wallet_velocity":
            return snap

    return None


# ---------------------------------------------------------
# Main loop
# ---------------------------------------------------------

def run_diff_metric_engine():
    print("[Bot X] Time-window diff metric engine starting...")
    print(f"[Bot X] Connecting to Redis at {YOUR_HOST}:{YOUR_PORT}")

    mint_states: dict[str, DiffMetricState] = {}
    idx = 0

    while True:
        try:
            length = r.llen(STREAM_TICKS)
            if idx >= length:
                time.sleep(0.05)
                continue

            raw = r.lindex(STREAM_TICKS, idx)
            idx += 1

            if not raw:
                continue

            try:
                tick = json.loads(raw)
            except json.JSONDecodeError:
                print("[Bot X] JSON decode error:", raw)
                continue

            if tick.get("type") != "swap_tick":
                continue

            mint = tick.get("mint")
            if not mint:
                continue

            state = mint_states.get(mint)
            if state is None:
                state = DiffMetricState(mint)
                mint_states[mint] = state

            # ---- single ingest only ----
            wv_snap = fetch_latest_wallet_velocity(mint)
            snapshot = state.ingest_tick(tick, wallet_velocity_snapshot=wv_snap)
            if snapshot is None:
                continue

            # Push diff snapshot for debugging
            r.rpush(STREAM_DIFF_TICKS, json.dumps({
                "mint": mint,
                "ts": snapshot["ts"],
                "diff_2s": snapshot["diff_2s"],
                "diff_3s": snapshot["diff_3s"],
                "diff_5s": snapshot["diff_5s"],
                "diff_7s": snapshot["diff_7s"],
                "diff_10s": snapshot["diff_10s"],
                "volume_signal": snapshot["volume_signal"],
                "unique_buyers_ok": snapshot["unique_buyers_ok"],
                "price_ok": snapshot["price_ok"],
                "cooldown_ok": snapshot["cooldown_ok"],
            }))

            print(
                f"[Bot X] diff → mint={mint} "
                f"2s={snapshot['diff_2s']:.1f} "
                f"3s={snapshot['diff_3s']:.1f} "
                f"5s={snapshot['diff_5s']:.1f} "
                f"7s={snapshot['diff_7s']:.1f} "
                f"10s={snapshot['diff_10s']:.1f} "
                f"BUY={snapshot['signal_buy']} "
                f"(vol={snapshot['volume_signal']} "
                f"uniq={snapshot['unique_buyers_ok']} "
                f"price={snapshot['price_ok']} "
                f"cd={snapshot['cooldown_ok']}) "
                f"time={snapshot['ts']}"
            )

            # BUY SIGNAL → push to QUEUE_BUY
            if snapshot["signal_buy"]:
                r.rpush(STREAM_DIFF_METRIC, json.dumps({
                    "type": "buy_signal",
                    "mint": mint,
                    "ts": snapshot["ts"],
                    "reason": "diff_metric",
                    "diff_2s": snapshot["diff_2s"],
                    "diff_3s": snapshot["diff_3s"],
                    "diff_5s": snapshot["diff_5s"],
                    "diff_7s": snapshot["diff_7s"],
                    "diff_10s": snapshot["diff_10s"],
                }))
                print(f"[Bot X] BUY SIGNAL → mint={mint}")

        except Exception as e:
            print(f"[Bot X] Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    run_diff_metric_engine()
