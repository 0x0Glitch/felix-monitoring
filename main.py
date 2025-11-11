#!/usr/bin/env python3
import os
import re
import time
import json
import threading
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ---------------------------- ENV ----------------------------
load_dotenv()

SUPABASE_DSN = os.getenv("SUPABASE_DSN")  # e.g. postgresql://user:pass@host:5432/db?sslmode=require

# Optional (with safe defaults):
MARKETS = os.getenv("MARKETS", "flxn:TSLA")            # comma-separated coins
HL_DEX = os.getenv("HL_DEX")                           # e.g. "flxn" (if omitted, auto-infer from coin prefix)
IS_TESTNET = os.getenv("HL_TESTNET", "1").lower() in ("1", "true", "yes")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "3"))
DB_SCHEMA = os.getenv("DB_SCHEMA", "market_data")

# Telegram alerting
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

MAINNET_INFO = "https://api.hyperliquid.xyz/info"
TESTNET_INFO = "https://api.hyperliquid-testnet.xyz/info"
INFO_URL = TESTNET_INFO if IS_TESTNET else MAINNET_INFO

# ---------------------------- Telegram helpers ----------------------------

def _now_hms():
    return time.strftime("%H:%M:%S")

def send_telegram(msg: str) -> None:
    """Best-effort Telegram alert; never raises into caller."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        # Silent if creds missing; but log locally.
        print(f"[{_now_hms()}] TELEGRAM not configured: {msg}")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg[:4000],  # Telegram message limit guard
            "parse_mode": "Markdown"
        }
        requests.post(url, data=data, timeout=5).raise_for_status()
    except Exception as e:
        print(f"[{_now_hms()}] Telegram send failed: {e}")

# ---------------------------- HTTP helpers ----------------------------

def post_info(payload: dict, timeout: float = 7.0) -> dict:
    r = requests.post(INFO_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

def detect_dex(coin: str) -> Optional[str]:
    """Use global HL_DEX if given; else infer from coin prefix like 'flxn:TSLA' -> 'flxn'."""
    if HL_DEX:
        return HL_DEX
    if ":" in coin:
        prefix = coin.split(":", 1)[0].strip()
        return prefix if prefix else None
    return None

def fetch_meta_and_ctxs(dex: Optional[str]) -> Tuple[List[dict], List[dict], dict]:
    """
    Returns (universe, asset_ctxs, meta_obj).
    Shape per docs may be [meta, assetCtxs] or dict; normalize both.
    """
    req = {"type": "metaAndAssetCtxs"}
    if dex:
        req["dex"] = dex
    resp = post_info(req)

    if isinstance(resp, list) and len(resp) >= 2:
        meta_obj = resp[0] or {}
        universe = meta_obj.get("universe", []) or []
        asset_ctxs = resp[1] or []
        return universe, asset_ctxs, meta_obj
    elif isinstance(resp, dict):
        meta_obj = resp
        universe = resp.get("universe", []) or []
        asset_ctxs = resp.get("assetCtxs", []) or []
        return universe, asset_ctxs, meta_obj
    return [], [], {}

# ---------------------------- L2 fetching (concurrent) ----------------------------

def fetch_l2book_custom(coin: str, nSigFigs: Optional[int], dex: Optional[str]) -> dict:
    """
    Fetch l2Book with explicit nSigFigs (None for full precision).
    Adds 'dex' when provided (for HIP-3 style DEX coins like flxn:TSLA).
    """
    body = {"type": "l2Book", "coin": coin}
    body["nSigFigs"] = nSigFigs if nSigFigs is not None else None
    if dex:
        body["dex"] = dex
    return post_info(body)

def fetch_dual_books_concurrently(coin: str, dex: Optional[str]) -> Tuple[dict, dict]:
    """
    Concurrently fetch:
      - fine (nSigFigs=4)   -> for 5bps, 10bps + mid/bbo/spread
      - coarse (nSigFigs=3) -> for 50bps, 100bps
    Raises on any failure (caller should catch and skip DB insert).
    """
    tasks = {
        "fine": (coin, 4, dex),
        "coarse": (coin, 3, dex),
    }
    results: Dict[str, dict] = {}
    errors: Dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_to_key = {
            pool.submit(fetch_l2book_custom, *args): key
            for key, args in tasks.items()
        }
        for fut in as_completed(fut_to_key):
            key = fut_to_key[fut]
            try:
                results[key] = fut.result()
            except Exception as e:
                errors[key] = str(e)

    if errors:
        # Alert once and raise to ensure caller does NOT push to DB
        send_telegram(f"*HL L2 fetch failed* for `{coin}`: " +
                      ", ".join(f"{k}={v}" for k, v in errors.items()))
        raise RuntimeError(f"Dual L2 fetch failed for {coin}: {errors}")

    # quick sanity check on shapes
    for k in ("fine", "coarse"):
        if not isinstance(results.get(k), dict) or "levels" not in results[k].get("l2Book", results[k]):
            send_telegram(f"*HL L2 shape error* for `{coin}` on `{k}` snapshot: {json.dumps(results[k])[:500]}")
            raise RuntimeError(f"L2 shape error on {k} for {coin}")
    return results["fine"], results["coarse"]

# ---------------------------- parsing & math ----------------------------

def _f(x):
    """Coerce '431.66' etc. to float; keep None as None."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def parse_levels_from_l2(book_raw: Dict) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    """Normalize L2 levels into [(price, size), ...] for bids/asks, best-first."""
    book = book_raw.get("l2Book", book_raw)
    levels = book.get("levels") or []
    if not isinstance(levels, list) or len(levels) < 2:
        return [], []
    def norm(side):
        out = []
        for row in side:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                px, sz = row[0], row[1]
            elif isinstance(row, dict):
                px, sz = row.get("px", row.get("limitPx")), row.get("sz")
            else:
                continue
            px = _f(px); sz = _f(sz)
            if px is not None and sz is not None:
                out.append((px, sz))
        return out
    bids = sorted(norm(levels[0]), key=lambda x: x[0], reverse=True)
    asks = sorted(norm(levels[1]), key=lambda x: x[0])
    return bids, asks

def compute_mid(bids: List[Tuple[float,float]], asks: List[Tuple[float,float]]) -> Optional[float]:
    if not bids or not asks:
        return None
    return (bids[0][0] + asks[0][0]) / 2.0

def depth_at_bps(bids, asks, mid, bps: float) -> Dict[str, float]:
    """
    Sum depth by *visible* book up to mid*(1±pct).
    Returns notional (px*sz) per side; caller decides whether to treat as lower bound.
    """
    pct = bps / 10_000.0
    bid_floor = mid * (1.0 - pct)
    ask_ceiling = mid * (1.0 + pct)
    bid_base = sum(sz for px, sz in bids if px >= bid_floor)
    ask_base = sum(sz for px, sz in asks if px <= ask_ceiling)
    bid_notional = sum(px * sz for px, sz in bids if px >= bid_floor)
    ask_notional = sum(px * sz for px, sz in asks if px <= ask_ceiling)
    return {
        "bid_base": bid_base,
        "ask_base": ask_base,
        "total_base": bid_base + ask_base,
        "bid_notional": bid_notional,
        "ask_notional": ask_notional,
        "total_notional": bid_notional + ask_notional,
    }

def depths_bundle_dual(fine_bids, fine_asks, coarse_bids, coarse_asks, mid) -> Dict[str, Dict[str, float]]:
    """
    Compute:
      - 5/10 bps using fine book
      - 50/100 bps using coarse book
    """
    return {
        "bps5":   depth_at_bps(fine_bids,   fine_asks,   mid, 5.0),
        "bps10":  depth_at_bps(fine_bids,   fine_asks,   mid, 10.0),
        "bps50":  depth_at_bps(coarse_bids, coarse_asks, mid, 50.0),
        "bps100": depth_at_bps(coarse_bids, coarse_asks, mid, 100.0),
    }

# ---------------------------- Postgres helpers ----------------------------

def sanitize_table_name(coin: str) -> str:
    """
    Make a safe table name suffix from the coin (e.g., 'flxn:TSLA' -> 'flxn_tsla_data').
    - Lowercase
    - Replace non [a-z0-9_] with underscore
    - Ensure starts with letter/underscore
    """
    base = coin.lower()
    base = re.sub(r'[^a-z0-9_]+', '_', base)
    if not re.match(r'^[a-z_]', base):
        base = '_' + base
    return f"{base}_data"

DDL_COLUMNS = """
    id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT,
    szDecimals INTEGER,
    coin TEXT,
    markPx DOUBLE PRECISION,
    prevDayPx DOUBLE PRECISION,
    marginMode TEXT,
    oraclePx DOUBLE PRECISION,
    maxLeverage INTEGER,
    midPx DOUBLE PRECISION,
    best_bid DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    dayBaseVlm DOUBLE PRECISION,
    funding DOUBLE PRECISION,
    openInterest DOUBLE PRECISION,
    dayNtlVlm DOUBLE PRECISION,
    bid_depth_5bps DOUBLE PRECISION,
    ask_depth_5bps DOUBLE PRECISION,
    bid_depth_10bps DOUBLE PRECISION,
    ask_depth_10bps DOUBLE PRECISION,
    bid_depth_50bps DOUBLE PRECISION,
    ask_depth_50bps DOUBLE PRECISION,
    bid_depth_100bps DOUBLE PRECISION,
    ask_depth_100bps DOUBLE PRECISION,
    premium DOUBLE PRECISION,
    impactPxs_bid DOUBLE PRECISION,
    impactPxs_ask DOUBLE PRECISION
"""

INSERT_COLS = [
    "timestamp","szDecimals","coin","markPx","prevDayPx","marginMode","oraclePx","maxLeverage",
    "midPx","best_bid","best_ask","spread","dayBaseVlm","funding","openInterest","dayNtlVlm",
    "bid_depth_5bps","ask_depth_5bps","bid_depth_10bps","ask_depth_10bps","bid_depth_50bps",
    "ask_depth_50bps","bid_depth_100bps","ask_depth_100bps","premium","impactPxs_bid","impactPxs_ask"
]

def ensure_schema_and_table(conn, schema: str, table: str):
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({DDL_COLUMNS});')
        cur.execute(f'CREATE INDEX IF NOT EXISTS "{table}_ts_idx" ON "{schema}"."{table}" (timestamp);')
    conn.commit()

def insert_rows(conn, schema: str, table: str, rows: List[dict]):
    if not rows:
        return
    values = [[row.get(k) for k in INSERT_COLS] for row in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f'INSERT INTO "{schema}"."{table}" ({",".join(INSERT_COLS)}) VALUES %s',
            values,
            page_size=100
        )
    conn.commit()

# ---------------------------- polling ----------------------------

def poll_cycle(coins: List[str]) -> List[dict]:
    """
    One cycle:
      - group coins by dex
      - fetch meta+ctxs once per dex
      - fetch TWO l2Book snapshots per coin concurrently (nSigFigs=4 and nSigFigs=3)
      - compute bps depths using the intended snapshot
      - produce rows matching INSERT_COLS (no created_at)
    The function guarantees: if ANY API call fails for that coin, no row is produced.
    """
    now_ms = int(time.time() * 1000)
    out_rows: List[dict] = []

    # group coins by detected dex
    dex_groups: Dict[Optional[str], List[str]] = {}
    for coin in coins:
        dex = detect_dex(coin)
        dex_groups.setdefault(dex, []).append(coin)

    for dex, dex_coins in dex_groups.items():
        try:
            universe, asset_ctxs, _ = fetch_meta_and_ctxs(dex)
            name_to_idx = { (u.get("name") or "").upper(): i for i, u in enumerate(universe) }
        except Exception as e:
            send_telegram(f"*HL metaAndAssetCtxs failed* (dex={dex}): {e}")
            # If meta fetch fails, we cannot insert for *any* coin in this dex group this cycle.
            continue

        for coin in dex_coins:
            try:
                idx = name_to_idx.get(coin.upper())
                meta = universe[idx] if idx is not None and idx < len(universe) else {}
                ctx  = asset_ctxs[idx] if idx is not None and idx < len(asset_ctxs) else {}

                # 1) Fetch two L2 snapshots concurrently; any failure => skip this coin for this cycle
                fine_raw, coarse_raw = fetch_dual_books_concurrently(coin, dex)

                # 2) Parse both
                fine_bids, fine_asks     = parse_levels_from_l2(fine_raw)
                coarse_bids, coarse_asks = parse_levels_from_l2(coarse_raw)
                if not fine_bids or not fine_asks:
                    raise RuntimeError("fine L2 had empty side(s)")
                if not coarse_bids or not coarse_asks:
                    raise RuntimeError("coarse L2 had empty side(s)")

                # 3) Mid from fine snapshot (best precision near top); fall back to ctx midPx if needed
                mid = compute_mid(fine_bids, fine_asks) or _f(ctx.get("midPx"))
                if mid is None:
                    raise RuntimeError("mid could not be computed")

                best_bid = fine_bids[0][0]
                best_ask = fine_asks[0][0]
                spread = best_ask - best_bid

                # 4) Depths: 5/10 from fine; 50/100 from coarse
                depth = depths_bundle_dual(fine_bids, fine_asks, coarse_bids, coarse_asks, mid)

                # 5) Context/meta fields
                sz_decimals = meta.get("szDecimals")
                max_lev     = meta.get("maxLeverage")
                margin_mode_meta = meta.get("marginMode")
                only_isolated = meta.get("onlyIsolated", False)
                margin_mode = margin_mode_meta or ("isolated" if only_isolated else "cross")

                mark_px   = _f(ctx.get("markPx"))
                mid_px    = _f(ctx.get("midPx"))
                oracle_px = _f(ctx.get("oraclePx"))
                prev_day  = _f(ctx.get("prevDayPx"))
                funding   = _f(ctx.get("funding"))
                oi        = _f(ctx.get("openInterest"))
                day_ntl   = _f(ctx.get("dayNtlVlm"))
                day_base  = _f(ctx.get("dayBaseVlm"))
                premium   = _f(ctx.get("premium"))
                impacts   = ctx.get("impactPxs") or []
                impact_bid = _f(impacts[0]) if len(impacts) >= 1 else None
                impact_ask = _f(impacts[1]) if len(impacts) >= 2 else None

                # 6) Assemble row
                row = {
                    "timestamp": now_ms,
                    "szDecimals": sz_decimals,
                    "coin": coin,
                    "markPx": mark_px,
                    "prevDayPx": prev_day,
                    "marginMode": margin_mode,
                    "oraclePx": oracle_px,
                    "maxLeverage": max_lev,
                    "midPx": mid_px,           # keep ctx midPx (historical consistency)
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": spread,
                    "dayBaseVlm": day_base,
                    "funding": funding,
                    "openInterest": oi,
                    "dayNtlVlm": day_ntl,
                    "bid_depth_5bps":   depth["bps5"].get("bid_notional"),
                    "ask_depth_5bps":   depth["bps5"].get("ask_notional"),
                    "bid_depth_10bps":  depth["bps10"].get("bid_notional"),
                    "ask_depth_10bps":  depth["bps10"].get("ask_notional"),
                    "bid_depth_50bps":  depth["bps50"].get("bid_notional"),
                    "ask_depth_50bps":  depth["bps50"].get("ask_notional"),
                    "bid_depth_100bps": depth["bps100"].get("bid_notional"),
                    "ask_depth_100bps": depth["bps100"].get("ask_notional"),
                    "premium": premium,
                    "impactPxs_bid": impact_bid,
                    "impactPxs_ask": impact_ask
                }

                out_rows.append(row)

            except Exception as e:
                # Do NOT insert for this coin on this cycle
                msg = f"*Depth compute skipped* for `{coin}` (no DB write): {e}"
                print(msg)
                send_telegram(msg)
                continue

    return out_rows

# ---------------------------- DB + runners ----------------------------

def create_db_connection(conn_dsn: str, keepalive: int = 30):
    """Create a database connection with keepalive settings."""
    conn = psycopg2.connect(
        conn_dsn,
        keepalives=1,
        keepalives_idle=keepalive,
        keepalives_interval=10,
        keepalives_count=5
    )
    conn.autocommit = False
    return conn

def poll_and_insert_market(coin: str, conn_dsn: str, schema: str, stop_event: threading.Event):
    """Poll a single market continuously in its own thread."""
    conn = None
    reconnect_attempts = 0
    max_reconnect_attempts = 5

    while not stop_event.is_set():
        try:
            # Ensure we have a valid connection
            if conn is None:
                if reconnect_attempts > 0:
                    print(json.dumps({"coin": coin, "action": "reconnecting", "attempt": reconnect_attempts}))
                conn = create_db_connection(conn_dsn)
                reconnect_attempts = 0

            t0 = time.time()
            try:
                rows = poll_cycle([coin])
                # rows is empty if we had API failures; only insert when rows present
                if rows:
                    table = sanitize_table_name(coin)
                    ensure_schema_and_table(conn, schema, table)
                    insert_rows(conn, schema, table, rows)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                # Connection lost - close and reconnect
                print(json.dumps({"coin": coin, "error": "connection_lost", "details": str(db_err)}))
                send_telegram(f"*DB connection lost* while writing `{coin}`: {db_err}")
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                    conn = None

                reconnect_attempts += 1
                if reconnect_attempts > max_reconnect_attempts:
                    print(json.dumps({"coin": coin, "error": "max_reconnect_attempts_exceeded"}))
                    send_telegram(f"*DB reconnect attempts exceeded* for `{coin}`; pausing briefly.")
                    reconnect_attempts = 0

                stop_event.wait(min(reconnect_attempts * 2, 30))
                continue
            except Exception as e:
                # Any other failure should NOT write to DB
                print(json.dumps({"coin": coin, "thread_error": str(e)}))
                send_telegram(f"*Monitor error* for `{coin}`: {e}")

            elapsed = time.time() - t0
            # Use Event.wait instead of sleep for faster shutdown
            stop_event.wait(max(0.0, POLL_INTERVAL_SEC - elapsed))

        except Exception as e:
            print(json.dumps({"coin": coin, "fatal_error": str(e)}))
            send_telegram(f"*Fatal loop error* for `{coin}`: {e}")
            stop_event.wait(5)

    # Cleanup
    if conn:
        try:
            conn.close()
        except:
            pass

def run():
    if not SUPABASE_DSN:
        raise RuntimeError("SUPABASE_DSN is required (env).")

    coins = [c.strip() for c in MARKETS.split(",") if c.strip()]

    if len(coins) == 1:
        # Single market - simple synchronous approach with retry logic
        conn = None
        reconnect_attempts = 0
        max_reconnect_attempts = 5

        while True:
            try:
                if conn is None:
                    if reconnect_attempts > 0:
                        print(json.dumps({"action": "reconnecting", "attempt": reconnect_attempts}))
                    conn = create_db_connection(SUPABASE_DSN)
                    reconnect_attempts = 0

                t0 = time.time()
                try:
                    rows = poll_cycle(coins)
                    if rows:
                        table = sanitize_table_name(coins[0])
                        ensure_schema_and_table(conn, DB_SCHEMA, table)
                        insert_rows(conn, DB_SCHEMA, table, rows)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                    print(json.dumps({"error": "connection_lost", "details": str(db_err)}))
                    send_telegram(f"*DB connection lost* (single loop): {db_err}")
                    if conn:
                        try:
                            conn.close()
                        except:
                            pass
                        conn = None

                    reconnect_attempts += 1
                    if reconnect_attempts > max_reconnect_attempts:
                        print(json.dumps({"error": "max_reconnect_attempts_exceeded"}))
                        send_telegram("*DB reconnect attempts exceeded* (single loop); backing off.")
                        reconnect_attempts = 0

                    time.sleep(min(reconnect_attempts * 2, 30))
                    continue

                elapsed = time.time() - t0
                time.sleep(max(0.0, POLL_INTERVAL_SEC - elapsed))

            except KeyboardInterrupt:
                print("\nShutting down...")
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                break
            except Exception as e:
                print(json.dumps({"fatal_error": str(e)}))
                send_telegram(f"*Fatal run error* (single loop): {e}")
                time.sleep(5)
    else:
        # Multiple markets - concurrent threads
        print(f"Starting concurrent monitoring for {len(coins)} markets: {', '.join(coins)}")
        stop_event = threading.Event()
        threads = []

        for coin in coins:
            thread = threading.Thread(
                target=poll_and_insert_market,
                args=(coin, SUPABASE_DSN, DB_SCHEMA, stop_event),
                name=f"Monitor-{coin}",
                daemon=True
            )
            thread.start()
            threads.append(thread)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            stop_event.set()
            for thread in threads:
                thread.join(timeout=5)
            print("All monitors stopped.")

if __name__ == "__main__":
    print(f"# Using INFO_URL={INFO_URL} (IS_TESTNET={IS_TESTNET}) | markets={MARKETS} | dex={HL_DEX or 'auto'}")
    run()