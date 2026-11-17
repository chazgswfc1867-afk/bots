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

        # Weighted volume windows
        self.win2s  = deque()
        self.win3s  = deque()
        self.win5s  = deque()
        self.win7s  = deque()
        self.win10s = deque()

        # New medium windows
        self.win15s = deque()
        self.win30s = deque()

        self.prev_sum2s  = 0.0
        self.prev_sum3s  = 0.0
        self.prev_sum5s  = 0.0
        self.prev_sum7s  = 0.0
        self.prev_sum10s = 0.0
        self.prev_sum15s = 0.0
        self.prev_sum30s = 0.0


        # Price history for guards  (ts, price)
        self.recent_prices = deque(maxlen=50)

        # Recent buy timestamps for density check
        self.recent_buy_ts = deque(maxlen=30)   # last ~30 buys

        # Cooldown
        self.last_signal_ts = 0

    def prune(self, window: deque, now_ts: int, seconds: int):
        cutoff = now_ts - seconds
        while window and window[0][0] < cutoff:
            window.popleft()

    def ingest_tick(self, tick: dict, wallet_velocity_snapshot: dict | None = None):
        side = tick.get("side")
        ts   = tick.get("ts")

        if side not in ("buy", "sell") or ts is None:
            return None

        # ---------- Weighting ----------
        sol = abs(float(tick.get("sol_amount", 0)))
        if side == "buy":
            weight = min(sol, 30.0)
        else:
            weight = min(sol, 8.0)

        val = weight if side == "buy" else -weight

        # Append + prune weighted windows
        for win, secs in [
            (self.win2s, 2), (self.win3s, 3), (self.win5s, 5),
            (self.win7s, 7), (self.win10s, 10),
            (self.win15s, 15), (self.win30s, 30)
        ]:
            win.append((ts, val))
            self.prune(win, ts, secs)

        # Current sums & diffs
        sum2s  = sum(v for _, v in self.win2s)
        sum3s  = sum(v for _, v in self.win3s)
        sum5s  = sum(v for _, v in self.win5s)
        sum7s  = sum(v for _, v in self.win7s)
        sum10s = sum(v for _, v in self.win10s)
        sum15s = sum(v for _, v in self.win15s)
        sum30s = sum(v for _, v in self.win30s)

        diff2s  = sum2s  - self.prev_sum2s
        diff3s  = sum3s  - self.prev_sum3s
        diff5s  = sum5s  - self.prev_sum5s
        diff7s  = sum7s  - self.prev_sum7s
        diff10s = sum10s - self.prev_sum10s
        diff15s = sum15s - self.prev_sum15s
        diff30s = sum30s - self.prev_sum30s

        self.prev_sum2s  = sum2s
        self.prev_sum3s  = sum3s
        self.prev_sum5s  = sum5s
        self.prev_sum7s  = sum7s
        self.prev_sum10s = sum10s
        self.prev_sum15s = sum15s
        self.prev_sum30s = sum30s

        # ---------- Price history ----------
        price = tick.get("price")
        if price is not None and price > 0:
            self.recent_prices.append((ts, float(price)))

        # ---------- Buy density tracking ----------
        if side == "buy":
            self.recent_buy_ts.append(ts)
            # keep only last 8 seconds
            while self.recent_buy_ts and self.recent_buy_ts[0] < ts - 8:
                self.recent_buy_ts.popleft()

        # =================================================
        # Signal logic  (balanced – more substance required)
        # =================================================



        # Short-window path (original, slightly softened)
        short_signal = (
            (diff2s >= 5 and sum2s >= 7) or
            (diff3s >= 7 and sum3s >= 9) or
            (diff5s >= 9 and sum3s >= 11) or
            (sum2s >= 12 and diff2s > 2)
        )

        # Medium-window path (new – catches the 1-minute scale moves)
        medium_signal = (
            (diff15s >= 12 and sum15s >= 18) or
            (diff30s >= 18 and sum30s >= 25) or
            (sum15s >= 25 and diff15s > 5) or
            (sum30s >= 35 and diff30s > 8)
        )

        volume_signal = short_signal or medium_signal


        # 2. Wallet-velocity – soft
        unique_buyers_ok = True
        if wallet_velocity_snapshot is not None:
            wallets_buy      = wallet_velocity_snapshot.get("wallets_buy", 0) or 0
            buy_wallet_ratio = wallet_velocity_snapshot.get("buy_wallet_ratio") or 0.0
            if wallets_buy < 2 and buy_wallet_ratio < 0.5:
                unique_buyers_ok = False

        # 3. Price-from-low (keep – your best anti-top filter)
        price_ok = True
        if len(self.recent_prices) >= 4:
            prices = [p for _, p in self.recent_prices]
            low = min(prices)
            current = prices[-1]
            if low > 0 and (current - low) / low > 0.25:
                price_ok = False

        # 4. Rising – only reject clear declines
        rising_ok = True
        if len(self.recent_prices) >= 3:
            recent = [p for _, p in list(self.recent_prices)[-3:]]
            if recent[-1] < recent[0] * 0.98:      # more than 2 % down
                rising_ok = False

        # 5. Density – require real activity
        density_ok = True
        buys_last_5s = sum(1 for t in self.recent_buy_ts if t >= ts - 5)
        if buys_last_5s < 2:
            density_ok = False

        # 6. Cooldown
        cooldown_ok = (ts - self.last_signal_ts) > 12

        signal_buy = (
            volume_signal and
            unique_buyers_ok and
            price_ok and
            rising_ok and
            density_ok and
            cooldown_ok
        )

        signal_sell = (diff7s <= -10) or (diff10s <= -12)

        if signal_buy:
            self.last_signal_ts = ts

        snapshot = {
            "type": "diff_metric",
            "mint": self.mint,
            "ts": ts,

            "sum_2s": sum2s, "sum_3s": sum3s, "sum_5s": sum5s,
            "sum_7s": sum7s, "sum_10s": sum10s,
            "sum_15s": sum15s, "sum_30s": sum30s,

            "diff_2s": diff2s, "diff_3s": diff3s, "diff_5s": diff5s,
            "diff_7s": diff7s, "diff_10s": diff10s,
            "diff_15s": diff15s, "diff_30s": diff30s,

            "signal_buy": signal_buy,
            "signal_sell": signal_sell,

            # debugging flags
            "volume_signal":    volume_signal,
            "short_signal": short_signal,
            "medium_signal": medium_signal,
            "unique_buyers_ok": unique_buyers_ok,
            "price_ok":         price_ok,
            "rising_ok":        rising_ok,
            "density_ok":       density_ok,
            "cooldown_ok":      cooldown_ok,
            "buys_last_5s":     buys_last_5s,
        }

        return snapshot


def fetch_latest_wallet_velocity(mint: str, lookback: int = 40):
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

            # Single ingest only
            wv_snap = fetch_latest_wallet_velocity(mint)
            snapshot = state.ingest_tick(tick, wallet_velocity_snapshot=wv_snap)
            if snapshot is None:
                continue

            # Debug push
            r.rpush(STREAM_DIFF_TICKS, json.dumps({
                "mint": mint,
                "ts": snapshot["ts"],
                "diff_2s": snapshot["diff_2s"],
                "diff_3s": snapshot["diff_3s"],
                "diff_5s": snapshot["diff_5s"],
                "diff_7s": snapshot["diff_7s"],
                "diff_10s": snapshot["diff_10s"],
                "diff_15s": snapshot["diff_15s"],
                "diff_30s": snapshot["diff_30s"],
                "volume_signal": snapshot["volume_signal"],
                "short_signal": snapshot["short_signal"],
                "medium_signal": snapshot["medium_signal"],
                "unique_buyers_ok": snapshot["unique_buyers_ok"],
                "price_ok": snapshot["price_ok"],
                "rising_ok": snapshot["rising_ok"],
                "density_ok": snapshot["density_ok"],
                "cooldown_ok": snapshot["cooldown_ok"],
                "buys_last_5s": snapshot["buys_last_5s"],
            }))

            print(
                f"[Bot X] diff → mint={mint} "
                f"2s={snapshot['diff_2s']:.1f} "
                f"3s={snapshot['diff_3s']:.1f} "
                f"5s={snapshot['diff_5s']:.1f} "
                f"7s={snapshot['diff_7s']:.1f} "
                f"10s={snapshot['diff_10s']:.1f} "
                f"15s={snapshot['diff_15s']:.1f} "
                f"30s={snapshot['diff_30s']:.1f} "
                f"BUY={snapshot['signal_buy']} "
                f"(vol={snapshot['volume_signal']} "
                f"uniq={snapshot['unique_buyers_ok']} "
                f"price={snapshot['price_ok']} "
                f"rise={snapshot['rising_ok']} "
                f"dens={snapshot['density_ok']} "
                f"cd={snapshot['cooldown_ok']} "
                f"buys5s={snapshot['buys_last_5s']}) "
                f"t={snapshot['ts']}"
            )


            if snapshot["signal_buy"]:
                r.rpush(STREAM_DIFF_METRIC, json.dumps({
                    "type": "buy_signal",
                    "mint": mint,
                    "ts": snapshot["ts"],
                    "reason": "diff_metric",

                    # diffs
                    "diff_2s": snapshot["diff_2s"],
                    "diff_3s": snapshot["diff_3s"],
                    "diff_5s": snapshot["diff_5s"],
                    "diff_7s": snapshot["diff_7s"],
                    "diff_10s": snapshot["diff_10s"],
                    "diff_15s": snapshot["diff_15s"],
                    "diff_30s": snapshot["diff_30s"],


                    # sums
                    "sum_2s": snapshot["sum_2s"],
                    "sum_3s": snapshot["sum_3s"],
                    "sum_5s": snapshot["sum_5s"],
                    "sum_7s": snapshot["sum_7s"],
                    "sum_10s": snapshot["sum_10s"],
                    "sum_15s": snapshot["sum_15s"],
                    "sum_30s": snapshot["sum_30s"],


                    # flags + density
                    "volume_signal": snapshot["volume_signal"],
                    "short_signal": snapshot["short_signal"],
                    "medium_signal": snapshot["medium_signal"],
                    "unique_buyers_ok": snapshot["unique_buyers_ok"],
                    "price_ok": snapshot["price_ok"],
                    "rising_ok": snapshot["rising_ok"],
                    "density_ok": snapshot["density_ok"],
                    "cooldown_ok": snapshot["cooldown_ok"],
                    "buys_last_5s": snapshot["buys_last_5s"],
                }))
                print(f"[Bot X] BUY SIGNAL → mint={mint}")


        except Exception as e:
            print(f"[Bot X] Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    run_diff_metric_engine()
