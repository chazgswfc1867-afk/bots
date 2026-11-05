# bots/bot_e_exit_watchdog.py

import os
import json
import time
from datetime import datetime, timezone
from collections import deque
import redis
from dotenv import load_dotenv

load_dotenv()

YOUR_HOST = os.getenv("YOUR_HOST")
YOUR_PORT = int(os.getenv("YOUR_PORT", "6379"))
YOUR_PASSWORD = os.getenv("YOUR_PASSWORD")

STREAM_TICKS = "STREAM_TICKS"
POSITIONS_KEY = "QUEUE_POSITIONS"
QUEUE_SELL = "QUEUE_SELL"

r = redis.Redis(
    host=YOUR_HOST,
    port=YOUR_PORT,
    username="default",
    password=YOUR_PASSWORD,
    decode_responses=True,
)

# ---------------------------------------------------------
# Exit metric state (mirrors DiffMetricState but for sells)
# ---------------------------------------------------------

class ExitMetricState:
    def __init__(self, mint: str, entry_ts: int, entry_price: float | None):
        self.mint = mint
        self.entry_ts = entry_ts
        self.entry_price = entry_price

        # Weighted volume windows (same as Bot X)
        self.win2s = deque()
        self.win3s = deque()
        self.win5s = deque()
        self.win7s = deque()
        self.win10s = deque()

        self.prev_sum2s = 0.0
        self.prev_sum3s = 0.0
        self.prev_sum5s = 0.0
        self.prev_sum7s = 0.0
        self.prev_sum10s = 0.0

        # Price tracking
        self.recent_prices = deque(maxlen=40)
        self.high_water = entry_price   # highest price seen since entry

        # Buy / sell density
        self.recent_buy_ts = deque(maxlen=40)
        self.recent_sell_ts = deque(maxlen=40)

        self.last_exit_check_ts = 0

    def prune(self, window: deque, now_ts: int, seconds: int):
        cutoff = now_ts - seconds
        while window and window[0][0] < cutoff:
            window.popleft()

    def ingest_tick(self, tick: dict) -> dict | None:
        side = tick.get("side")
        ts = tick.get("ts")
        if side not in ("buy", "sell") or ts is None:
            return None

        sol = abs(float(tick.get("sol_amount", 0)))
        weight = min(sol, 30.0) if side == "buy" else min(sol, 12.0)
        val = weight if side == "buy" else -weight

        # Update windows
        for win, secs in [
            (self.win2s, 2), (self.win3s, 3), (self.win5s, 5),
            (self.win7s, 7), (self.win10s, 10)
        ]:
            win.append((ts, val))
            self.prune(win, ts, secs)

        sum2s = sum(v for _, v in self.win2s)
        sum3s = sum(v for _, v in self.win3s)
        sum5s = sum(v for _, v in self.win5s)
        sum7s = sum(v for _, v in self.win7s)
        sum10s = sum(v for _, v in self.win10s)

        diff2s = sum2s - self.prev_sum2s
        diff3s = sum3s - self.prev_sum3s
        diff5s = sum5s - self.prev_sum5s
        diff7s = sum7s - self.prev_sum7s
        diff10s = sum10s - self.prev_sum10s

        self.prev_sum2s = sum2s
        self.prev_sum3s = sum3s
        self.prev_sum5s = sum5s
        self.prev_sum7s = sum7s
        self.prev_sum10s = sum10s

        # Price
        price = tick.get("price")
        if price is not None and price > 0:
            price = float(price)
            self.recent_prices.append((ts, price))
            if self.high_water is None or price > self.high_water:
                self.high_water = price

        # Density
        if side == "buy":
            self.recent_buy_ts.append(ts)
        else:
            self.recent_sell_ts.append(ts)

        # keep last 8 s
        while self.recent_buy_ts and self.recent_buy_ts[0] < ts - 8:
            self.recent_buy_ts.popleft()
        while self.recent_sell_ts and self.recent_sell_ts[0] < ts - 8:
            self.recent_sell_ts.popleft()

        buys_5s = sum(1 for t in self.recent_buy_ts if t >= ts - 5)
        sells_5s = sum(1 for t in self.recent_sell_ts if t >= ts - 5)

        # -------------------------------------------------
        # Exit logic
        # -------------------------------------------------
        hold_seconds = ts - self.entry_ts

        # 1. Hard time stop
        time_stop = hold_seconds > 300          # 5 minutes

        # 2. Strong sell-side acceleration
        sell_pressure = (
            (diff2s <= -8) or
            (diff3s <= -10) or
            (diff5s <= -12) or
            (sum2s < -10 and diff2s < 0)
        )

        # 3. Density collapse (buys dry up while sells appear)
        density_collapse = (buys_5s <= 1 and sells_5s >= 2)

        # 4. Drawdown from high-water mark
        drawdown = False
        if self.high_water and price and self.high_water > 0:
            dd = (self.high_water - price) / self.high_water
            if dd >= 0.18:                      # 18 % off the high since entry
                drawdown = True

        # 5. From entry price (optional hard stop)
        from_entry_stop = False
        if self.entry_price and price and self.entry_price > 0:
            if (price - self.entry_price) / self.entry_price <= -0.25:
                from_entry_stop = True

        should_full_exit = time_stop or sell_pressure or density_collapse or drawdown or from_entry_stop

        # Simple partial: take some profit if we are nicely up and momentum fades
        should_partial = False
        if (not should_full_exit) and self.entry_price and price:
            pnl_pct = (price - self.entry_price) / self.entry_price
            if pnl_pct >= 0.35 and (diff3s < 0 or buys_5s <= 1):
                should_partial = True

        return {
            "mint": self.mint,
            "ts": ts,
            "hold_seconds": hold_seconds,
            "diff_2s": diff2s,
            "diff_3s": diff3s,
            "diff_5s": diff5s,
            "sum_2s": sum2s,
            "buys_5s": buys_5s,
            "sells_5s": sells_5s,
            "high_water": self.high_water,
            "current_price": price,
            "time_stop": time_stop,
            "sell_pressure": sell_pressure,
            "density_collapse": density_collapse,
            "drawdown": drawdown,
            "from_entry_stop": from_entry_stop,
            "should_full_exit": should_full_exit,
            "should_partial": should_partial,
        }


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def load_open_positions():
    positions = []
    for raw in r.lrange(POSITIONS_KEY, 0, -1):
        try:
            positions.append(json.loads(raw))
        except Exception:
            pass
    return positions


def push_sell_signal(mint: str, symbol: str, action: str, reason: str):
    msg = {
        "mint": mint,
        "symbol": symbol,
        "action": action,
        "reason": reason,
        "timestamp": int(time.time()),
        "iso_time": datetime.now(timezone.utc).isoformat(),
    }
    r.lpush(QUEUE_SELL, json.dumps(msg))
    print(f"[Bot E] → {action} pushed for {symbol} ({mint}) | {reason}")


# ---------------------------------------------------------
# Main loop
# ---------------------------------------------------------

def run_exit_watchdog():
    print("[Bot E] Flow-based exit watchdog starting...")

    # mint → ExitMetricState
    exit_states: dict[str, ExitMetricState] = {}
    tick_idx = 0

    while True:
        try:
            # Refresh open positions every few seconds
            positions = load_open_positions()
            open_mints = {p["mint"] for p in positions if p.get("mint")}

            # Clean up states for closed positions
            for m in list(exit_states.keys()):
                if m not in open_mints:
                    del exit_states[m]

            # Create state for any new positions
            for pos in positions:
                mint = pos.get("mint")
                if not mint or mint in exit_states:
                    continue

                entry_ts = int(pos.get("timestamp", time.time()))
                # crude entry price from position record
                sol_in = float(pos.get("sol_in", 0))
                tokens = float(pos.get("tokens_in", 0)) or (pos.get("amount_out", 0) / (10 ** pos.get("decimals", 6)))
                entry_price = (sol_in / tokens) if tokens > 0 else None

                exit_states[mint] = ExitMetricState(mint, entry_ts, entry_price)
                print(f"[Bot E] Tracking new position → {mint}")

            # Process new ticks
            length = r.llen(STREAM_TICKS)
            while tick_idx < length:
                raw = r.lindex(STREAM_TICKS, tick_idx)
                tick_idx += 1
                if not raw:
                    continue

                try:
                    tick = json.loads(raw)
                except Exception:
                    continue

                if tick.get("type") != "swap_tick":
                    continue

                mint = tick.get("mint")
                if mint not in exit_states:
                    continue

                state = exit_states[mint]
                result = state.ingest_tick(tick)
                if result is None:
                    continue

                # Decide
                if result["should_full_exit"]:
                    reason_parts = []
                    if result["time_stop"]:
                        reason_parts.append("time_stop")
                    if result["sell_pressure"]:
                        reason_parts.append("sell_pressure")
                    if result["density_collapse"]:
                        reason_parts.append("density_collapse")
                    if result["drawdown"]:
                        reason_parts.append("drawdown")
                    if result["from_entry_stop"]:
                        reason_parts.append("from_entry_stop")

                    symbol = next((p.get("symbol", "UNKNOWN") for p in positions if p.get("mint") == mint), "UNKNOWN")
                    push_sell_signal(mint, symbol, "FULL_EXIT", "+".join(reason_parts))

                    # remove so we don’t keep signalling
                    del exit_states[mint]

                elif result["should_partial"]:
                    symbol = next((p.get("symbol", "UNKNOWN") for p in positions if p.get("mint") == mint), "UNKNOWN")
                    push_sell_signal(mint, symbol, "PARTIAL_EXIT", "profit_take_momentum_fade")

            time.sleep(0.15)

        except Exception as e:
            print(f"[Bot E] Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    run_exit_watchdog()
