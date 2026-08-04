import time
import json
import redis
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

# ---------------------------------------
# Config
# ---------------------------------------
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
YOUR_HOST = os.getenv("YOUR_HOST")
YOUR_PORT = os.getenv("YOUR_PORT")
YOUR_PASSWORD = os.getenv("YOUR_PASSWORD")

ACTIVE_TOKENS = "ACTIVE_TOKENS"
STREAM_DISCOVERY = "STREAM_DISCOVERY"

MAX_AGE_SECONDS = 2 * 60 * 60  # 2 hours

r = redis.Redis(
    host=YOUR_HOST,
    port=int(YOUR_PORT),
    username="default",
    password=YOUR_PASSWORD,
    decode_responses=True
)

# ---------------------------------------
# Birdeye fetch
# ---------------------------------------

def fetch_new_tokens():
    """
    Fetch tokens 0–2 hours old from Birdeye.
    """
    now = int(time.time())
    min_recent = now - MAX_AGE_SECONDS
    max_recent = now

    url = "https://public-api.birdeye.so/defi/v3/token/meme/list"
    headers = {"x-api-key": BIRDEYE_API_KEY}

    params = {
        "sort_by": "recent_listing_time",
        "sort_type": "desc",
        "limit": 100,
        "offset": 0,
        "min_recent_listing_time": min_recent,
        "max_recent_listing_time": max_recent
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if not resp.ok:
            return []

        items = resp.json().get("data", {}).get("items", [])
        results = []

        for t in items:
            mint = t.get("address")
            if not mint or not mint.endswith("pump"):
                continue

            listed = t.get("recent_listing_time")
            if not listed:
                continue

            age = now - int(listed)
            if age > MAX_AGE_SECONDS:
                continue

            meme = t.get("meme_info", {}) or {}
            creator = meme.get("creator") or "unknown"

            # True mint timestamp (more accurate than recent_listing_time)
            created_at = meme.get("created_at", {}) or {}
            mint_ts = created_at.get("block_time") or int(listed)

            results.append({
                "mint": mint,
                "creator": creator,
                "ts": int(mint_ts),
            })

        return results

    except Exception:
        return []


# ---------------------------------------
# Redis helpers
# ---------------------------------------

def add_active_token(mint, creator, ts):
    r.sadd(ACTIVE_TOKENS, mint)
    r.hset(f"TOKEN_METADATA:{mint}", mapping={
        "mint": mint,
        "creator": creator,
        "ts": ts
    })

    r.rpush(STREAM_DISCOVERY, json.dumps({
        "type": "new_token",
        "mint": mint,
        "creator": creator,
        "ts": ts
    }))

def cleanup_old_tokens():
    now = int(time.time())
    active = r.smembers(ACTIVE_TOKENS)

    for mint in active:
        meta = r.hgetall(f"TOKEN_METADATA:{mint}")
        if not meta:
            continue

        ts = int(meta.get("ts", 0))
        age = now - ts

        if age > MAX_AGE_SECONDS:
            r.srem(ACTIVE_TOKENS, mint)
            print(f"[Bot D] Removed old token {mint} (age {age}s)")


# ---------------------------------------
# Main loop
# ---------------------------------------

def run_discovery():
    print("[Bot D] Birdeye discovery bot started")

    while True:
        tokens = fetch_new_tokens()

        for t in tokens:
            mint = t["mint"]

            if not r.sismember(ACTIVE_TOKENS, mint):
                add_active_token(mint, t["creator"], t["ts"])
                print(f"[Bot D] NEW TOKEN → {mint}")

        cleanup_old_tokens()
        time.sleep(5)


if __name__ == "__main__":
    run_discovery()
