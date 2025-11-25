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

SUPABASE_DSN = os.getenv("SUPABASE_DSN")
MARKETS = os.getenv("MARKETS")
HL_DEX = os.getenv("HL_DEX")
IS_TESTNET = os.getenv("HL_TESTNET", "1").lower() in ("1", "true", "yes")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "3"))
DB_SCHEMA = os.getenv("DB_SCHEMA", "market_data")

# Telegram alerting
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MAINNET_INFO = "https://api.hyperliquid.xyz/info"
TESTNET_INFO = "https://api.hyperliquid-testnet.xyz/info"
INFO_URL = TESTNET_INFO if IS_TESTNET else MAINNET_INFO

# ---------------------------- Telegram helpers ----------------------------


def _now_hms() -> str:
    return time.strftime("%H:%M:%S")


def send_telegram(msg: str) -> None:
    """
    Best-effort Telegram alert; never raises into caller.
    If Telegram is not configured, logs to stdout.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[{_now_hms()}] TELEGRAM not configured: {msg}")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg[:4000],
            "parse_mode": "Markdown",
        }
        requests.post(url, data=data, timeout=5).raise_for_status()
    except Exception as e:
        print(f"[{_now_hms()}] Telegram send failed: {e}")


# ---------------------------- HTTP helpers ----------------------------


def post_info(payload: dict, timeout: float = 7.0) -> dict:
    """Raw POST wrapper to Hyperliquid /info endpoint."""
    r = requests.post(INFO_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def detect_dex(coin: str) -> Optional[str]:
    """
    Use global HL_DEX if given; else infer from coin prefix like 'flx:TSLA' -> 'flx'.
    Returns None if no prefix present.
    """
    if HL_DEX:
        return HL_DEX
    if ":" in coin:
        prefix = coin.split(":", 1)[0].strip()
        return prefix if prefix else None
    return None


def fetch_meta_and_ctxs(dex: Optional[str]) -> Tuple[List[dict], List[dict], dict]:
    """
    Fetch meta and asset contexts for a given DEX.
    Returns (universe, asset_ctxs, meta_obj).
    Shape may be [meta, assetCtxs] or dict; normalize both.
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

    # unexpected shape; treat as failure
    raise RuntimeError(f"Unexpected metaAndAssetCtxs shape for dex={dex}: {type(resp)}")


# ---------------------------- L2 fetching (concurrent) ----------------------------


def fetch_l2book_custom(coin: str, nSigFigs: Optional[int], dex: Optional[str]) -> dict:
    """
    Fetch l2Book with explicit nSigFigs (None for full precision).
    Adds 'dex' when provided.
    """
    body: Dict[str, object] = {"type": "l2Book", "coin": coin}
    if nSigFigs is not None:
        body["nSigFigs"] = nSigFigs
    if dex:
        body["dex"] = dex
    return post_info(body)


def fetch_dual_books_concurrently(coin: str, dex: Optional[str]) -> Tuple[dict, dict]:
    """
    Concurrently fetch:
      - fine (nSigFigs=4)   -> for 5bps, 10bps + BBO
      - coarse (nSigFigs=3) -> for 50bps, 100bps

    Only treats true request failures as errors (network/HTTP); empty books are allowed.
    """
    tasks = {
        "fine": (coin, 4, dex),
        "coarse": (coin, 3, dex),
    }
    results: Dict[str, dict] = {}
    errors: Dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_to_key = {
            pool.submit(fetch_l2book_custom, *args): key for key, args in tasks.items()
        }
        for fut in as_completed(fut_to_key):
            key = fut_to_key[fut]
            try:
                results[key] = fut.result()
            except Exception as e:
                errors[key] = str(e)

    if errors:
        # Alert once and raise to ensure caller does NOT push to DB for this coin
        send_telegram(
            f"*HL L2 fetch failed* for `{coin}`: "
            + ", ".join(f"{k}={v}" for k, v in errors.items())
        )
        raise RuntimeError(f"Dual L2 fetch failed for {coin}: {errors}")

    # Both requests returned successfully; even if shapes are odd or empty,
    # we treat them as "no depth" and still write the row.
    return results.get("fine", {}), results.get("coarse", {})


# ---------------------------- parsing & math ----------------------------


def _f(x) -> Optional[float]:
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


def parse_levels_from_l2(
    book_raw: dict,
) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    """
    Normalize L2 levels into [(price, size), ...] for bids/asks, best-first.

    Handles:
      - {l2Book: {levels: [[...bids...], [...asks...]]}}
      - {levels: [[...], [...]]}
      - [] / {}  -> treated as empty book (no levels)
    """
    levels: List = []

    if isinstance(book_raw, dict):
        book = book_raw.get("l2Book", book_raw)
        if isinstance(book, dict):
            levels = book.get("levels") or []
        else:
            levels = []
    else:
        levels = []

    if not isinstance(levels, list) or len(levels) < 2:
        return [], []

    def norm(side):
        out: List[Tuple[float, float]] = []
        for row in side:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                px, sz = row[0], row[1]
            elif isinstance(row, dict):
                px = row.get("px", row.get("limitPx"))
                sz = row.get("sz")
            else:
                continue
            px = _f(px)
            sz = _f(sz)
            if px is not None and sz is not None:
                out.append((px, sz))
        return out

    bids = sorted(norm(levels[0]), key=lambda x: x[0], reverse=True)
    asks = sorted(norm(levels[1]), key=lambda x: x[0])
    return bids, asks


def depth_at_bps(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
    mid: float,
    bps: float,
) -> Dict[str, float]:
    """
    Sum depth by *visible* book up to mid*(1±pct).
    Returns:
        bid_base / ask_base / total_base
        bid_notional / ask_notional / total_notional
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


def depths_bundle_dual(
    fine_bids: List[Tuple[float, float]],
    fine_asks: List[Tuple[float, float]],
    coarse_bids: List[Tuple[float, float]],
    coarse_asks: List[Tuple[float, float]],
    mid: float,
) -> Dict[str, Dict[str, float]]:
    """
    Compute depth:
      - 5/10 bps using fine book
      - 50/100 bps using coarse book
    """
    return {
        "bps5": depth_at_bps(fine_bids, fine_asks, mid, 5.0),
        "bps10": depth_at_bps(fine_bids, fine_asks, mid, 10.0),
        "bps50": depth_at_bps(coarse_bids, coarse_asks, mid, 50.0),
        "bps100": depth_at_bps(coarse_bids, coarse_asks, mid, 100.0),
    }


# ---------------------------- Postgres helpers ----------------------------


def sanitize_table_name(coin: str) -> str:
    """
    Make a safe table name suffix from the coin (e.g., 'flx:TSLA' -> 'flx_tsla_data').
    - Lowercase
    - Replace non [a-z0-9_] with underscore
    - Ensure starts with letter/underscore
    """
    base = coin.lower()
    base = re.sub(r"[^a-z0-9_]+", "_", base)
    if not re.match(r"^[a-z_]", base):
        base = "_" + base
    return f"{base}_data"


DDL_COLUMNS = """
    id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT,
    szdecimals INTEGER,
    coin TEXT,
    markpx DOUBLE PRECISION,
    prevdaypx DOUBLE PRECISION,
    marginmode TEXT,
    oraclepx DOUBLE PRECISION,
    maxleverage INTEGER,
    midpx DOUBLE PRECISION,
    best_bid DOUBLE PRECISION,
    best_ask DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    daybasevlm DOUBLE PRECISION,
    funding DOUBLE PRECISION,
    openinterest DOUBLE PRECISION,
    dayntlvlm DOUBLE PRECISION,
    bid_depth_5bps DOUBLE PRECISION,
    ask_depth_5bps DOUBLE PRECISION,
    bid_depth_10bps DOUBLE PRECISION,
    ask_depth_10bps DOUBLE PRECISION,
    bid_depth_50bps DOUBLE PRECISION,
    ask_depth_50bps DOUBLE PRECISION,
    bid_depth_100bps DOUBLE PRECISION,
    ask_depth_100bps DOUBLE PRECISION,
    premium DOUBLE PRECISION,
    impactpxs_bid DOUBLE PRECISION,
    impactpxs_ask DOUBLE PRECISION
"""

INSERT_COLS = [
    "timestamp",
    "szdecimals",
    "coin",
    "markpx",
    "prevdaypx",
    "marginmode",
    "oraclepx",
    "maxleverage",
    "midpx",
    "best_bid",
    "best_ask",
    "spread",
    "daybasevlm",
    "funding",
    "openinterest",
    "dayntlvlm",
    "bid_depth_5bps",
    "ask_depth_5bps",
    "bid_depth_10bps",
    "ask_depth_10bps",
    "bid_depth_50bps",
    "ask_depth_50bps",
    "bid_depth_100bps",
    "ask_depth_100bps",
    "premium",
    "impactpxs_bid",
    "impactpxs_ask",
]


def ensure_schema_and_table(conn, schema: str, table: str) -> None:
    """Create schema/table/index if not present."""
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({DDL_COLUMNS});')
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "{table}_ts_idx" ON "{schema}"."{table}" (timestamp);'
        )
    conn.commit()


def insert_rows(conn, schema: str, table: str, rows: List[dict]) -> None:
    """Bulk insert rows into per-coin table."""
    if not rows:
        return
    values = [[row.get(k) for k in INSERT_COLS] for row in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f'INSERT INTO "{schema}"."{table}" ({",".join(INSERT_COLS)}) VALUES %s',
            values,
            page_size=100,
        )
    conn.commit()


# ---------------------------- per-coin processing helper ----------------------------


def process_coin_for_dex(
    coin: str,
    dex: Optional[str],
    now_ms: int,
    name_to_idx: Dict[str, int],
    universe: List[dict],
    asset_ctxs: List[dict],
) -> Optional[dict]:
    """
    Do all work for a single coin on a given dex:
      - look up meta/ctx
      - fetch dual L2
      - parse book
      - compute BBO, depths, ctx fields
      - return row dict or None on error
    """
    try:
        idx = name_to_idx.get(coin.upper())
        meta = universe[idx] if idx is not None and idx < len(universe) else {}
        ctx = asset_ctxs[idx] if idx is not None and idx < len(asset_ctxs) else {}

        # L2 fetch (only true per-coin network failure point)
        fine_raw, coarse_raw = fetch_dual_books_concurrently(coin, dex)

        # Parse books (empty allowed)
        fine_bids, fine_asks = parse_levels_from_l2(fine_raw)
        coarse_bids, coarse_asks = parse_levels_from_l2(coarse_raw)

        # BBO from fine snapshot, otherwise "no trading" (0)
        if fine_bids and fine_asks:
            best_bid = fine_bids[0][0]
            best_ask = fine_asks[0][0]
            spread = best_ask - best_bid
        else:
            best_bid = 0.0
            best_ask = 0.0
            spread = 0.0

        # Depths from ctx.midPx, else 0
        mid_px = _f(ctx.get("midPx"))
        bid_5 = bid_10 = bid_50 = bid_100 = 0.0
        ask_5 = ask_10 = ask_50 = ask_100 = 0.0

        if mid_px is not None and (
            fine_bids or fine_asks or coarse_bids or coarse_asks
        ):
            depth = depths_bundle_dual(
                fine_bids, fine_asks, coarse_bids, coarse_asks, mid_px
            )

            bid_5 = depth["bps5"].get("bid_notional", 0.0)
            bid_10 = depth["bps10"].get("bid_notional", 0.0)
            bid_50 = depth["bps50"].get("bid_notional", 0.0)
            bid_100 = depth["bps100"].get("bid_notional", 0.0)

            ask_5 = depth["bps5"].get("ask_notional", 0.0)
            ask_10 = depth["bps10"].get("ask_notional", 0.0)
            ask_50 = depth["bps50"].get("ask_notional", 0.0)
            ask_100 = depth["bps100"].get("ask_notional", 0.0)

            # Enforce monotonicity on bid side
            if bid_10 < bid_5:
                bid_10 = bid_5
            if bid_50 < bid_10:
                bid_50 = bid_10
            if bid_100 < bid_50:
                bid_100 = bid_50

            # Enforce monotonicity on ask side
            if ask_10 < ask_5:
                ask_10 = ask_5
            if ask_50 < ask_10:
                ask_50 = ask_10
            if ask_100 < ask_50:
                ask_100 = ask_50

        # Context/meta fields
        sz_decimals = meta.get("szDecimals")
        max_lev = meta.get("maxLeverage")
        margin_mode_meta = meta.get("marginMode")
        only_isolated = meta.get("onlyIsolated", False)
        margin_mode = margin_mode_meta or (
            "strictIsolated"
            if margin_mode_meta == "strictIsolated"
            else ("isolated" if only_isolated else "cross")
        )

        mark_px = _f(ctx.get("markPx"))
        oracle_px = _f(ctx.get("oraclePx"))
        prev_day = _f(ctx.get("prevDayPx"))

        def _zero_if_none(v):
            x = _f(v)
            return 0.0 if x is None else x

        funding = _zero_if_none(ctx.get("funding"))
        oi = _zero_if_none(ctx.get("openInterest"))
        day_ntl = _zero_if_none(ctx.get("dayNtlVlm"))
        day_base = _zero_if_none(ctx.get("dayBaseVlm"))

        premium = _f(ctx.get("premium"))  # keep NULL when none
        impacts = ctx.get("impactPxs") or []
        impact_bid = _f(impacts[0]) if len(impacts) >= 1 else None
        impact_ask = _f(impacts[1]) if len(impacts) >= 2 else None

        row = {
            "timestamp": now_ms,
            "szdecimals": sz_decimals,
            "coin": coin,
            "markpx": mark_px,
            "prevdaypx": prev_day,
            "marginmode": margin_mode,
            "oraclepx": oracle_px,
            "maxleverage": max_lev,
            "midpx": mid_px,  # can be NULL when no trading
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "daybasevlm": day_base,
            "funding": funding,
            "openinterest": oi,
            "dayntlvlm": day_ntl,
            "bid_depth_5bps": bid_5,
            "ask_depth_5bps": ask_5,
            "bid_depth_10bps": bid_10,
            "ask_depth_10bps": ask_10,
            "bid_depth_50bps": bid_50,
            "ask_depth_50bps": ask_50,
            "bid_depth_100bps": bid_100,
            "ask_depth_100bps": ask_100,
            "premium": premium,
            "impactpxs_bid": impact_bid,
            "impactpxs_ask": impact_ask,
        }
        return row

    except Exception as e:
        msg = f"*Depth compute skipped* for `{coin}` (no DB write): {e}"
        print(msg)
        send_telegram(msg)
        return None


# ---------------------------- polling ----------------------------


def poll_cycle(coins: List[str]) -> List[dict]:
    """
    One cycle:
      - group coins by dex
      - fetch meta+ctxs once per dex
      - fetch TWO l2Book snapshots per coin concurrently
      - use ctx.midPx as mid when present; otherwise, leave midpx NULL and depths=0
      - treat empty books as "no depth" -> zeros, not errors
      - always write for a coin if meta+L2 succeeded (even if no trading)

    Skips:
      - all coins for a dex if metaAndAssetCtxs fails
      - a specific coin only if its l2Book requests fail (network/HTTP error)
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
            name_to_idx = {
                (u.get("name") or "").upper(): i for i, u in enumerate(universe)
            }
        except Exception as e:
            send_telegram(f"*HL metaAndAssetCtxs failed* (dex={dex}): {e}")
            continue  # skip all coins for this dex this cycle

        # Parallelize per-coin work for this dex
        max_workers = min(len(dex_coins), 8)  # tune concurrency cap as needed
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    process_coin_for_dex,
                    coin,
                    dex,
                    now_ms,
                    name_to_idx,
                    universe,
                    asset_ctxs,
                )
                for coin in dex_coins
            ]
            for fut in as_completed(futures):
                row = fut.result()
                if row is not None:
                    out_rows.append(row)

    return out_rows


# ---------------------------- DB + runners ----------------------------


def create_db_connection(conn_dsn: str, keepalive: int = 30):
    """Create a database connection with keepalive settings."""
    conn = psycopg2.connect(
        conn_dsn,
        keepalives=1,
        keepalives_idle=keepalive,
        keepalives_interval=10,
        keepalives_count=5,
    )
    conn.autocommit = False
    return conn


def poll_and_insert_all_markets(
    coins: List[str], conn_dsn: str, schema: str, stop_event: threading.Event
) -> None:
    """
    Poll ALL markets in one loop so metaAndAssetCtxs is shared per dex.
    Now with per-coin work parallelized inside poll_cycle.
    """
    conn = None
    reconnect_attempts = 0
    max_reconnect_attempts = 5
    created_tables: set = set()

    while not stop_event.is_set():
        try:
            # Ensure we have a valid connection
            if conn is None:
                if reconnect_attempts > 0:
                    print(
                        json.dumps(
                            {
                                "markets": coins,
                                "action": "reconnecting",
                                "attempt": reconnect_attempts,
                            }
                        )
                    )
                conn = create_db_connection(conn_dsn)
                reconnect_attempts = 0

            t0 = time.time()
            try:
                rows = poll_cycle(coins)

                if rows:
                    rows_by_coin: Dict[str, List[dict]] = {}
                    for row in rows:
                        c = row.get("coin")
                        if not c:
                            continue
                        rows_by_coin.setdefault(c, []).append(row)

                    for coin, coin_rows in rows_by_coin.items():
                        table = sanitize_table_name(coin)
                        if table not in created_tables:
                            ensure_schema_and_table(conn, schema, table)
                            created_tables.add(table)
                        insert_rows(conn, schema, table, coin_rows)

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
                print(
                    json.dumps(
                        {
                            "markets": coins,
                            "error": "connection_lost",
                            "details": str(db_err),
                        }
                    )
                )
                send_telegram(
                    f"*DB connection lost* while writing markets {coins}: {db_err}"
                )
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None

                reconnect_attempts += 1
                if reconnect_attempts > max_reconnect_attempts:
                    print(
                        json.dumps(
                            {
                                "markets": coins,
                                "error": "max_reconnect_attempts_exceeded",
                            }
                        )
                    )
                    send_telegram(
                        f"*DB reconnect attempts exceeded* for markets {coins}; pausing briefly."
                    )
                    reconnect_attempts = 0

                stop_event.wait(min(reconnect_attempts * 2, 30))
                continue
            except Exception as e:
                print(
                    json.dumps(
                        {
                            "markets": coins,
                            "thread_error": str(e),
                        }
                    )
                )
                send_telegram(f"*Monitor error* for markets {coins}: {e}")

            elapsed = time.time() - t0
            stop_event.wait(max(0.0, POLL_INTERVAL_SEC - elapsed))

        except Exception as e:
            print(
                json.dumps(
                    {
                        "markets": coins,
                        "fatal_error": str(e),
                    }
                )
            )
            send_telegram(f"*Fatal loop error* for markets {coins}: {e}")
            stop_event.wait(5)

    # Cleanup
    if conn:
        try:
            conn.close()
        except Exception:
            pass


def run() -> None:
    if not SUPABASE_DSN:
        raise RuntimeError("SUPABASE_DSN is required (env).")

    if not MARKETS:
        raise RuntimeError(
            "MARKETS is required (env). Example: MARKETS=flx:TSLA,flx:NVDA"
        )

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
                        print(
                            json.dumps(
                                {
                                    "action": "reconnecting",
                                    "attempt": reconnect_attempts,
                                }
                            )
                        )
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
                    print(
                        json.dumps(
                            {
                                "error": "connection_lost",
                                "details": str(db_err),
                            }
                        )
                    )
                    send_telegram(f"*DB connection lost* (single loop): {db_err}")
                    if conn:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = None

                    reconnect_attempts += 1
                    if reconnect_attempts > max_reconnect_attempts:
                        print(
                            json.dumps(
                                {
                                    "error": "max_reconnect_attempts_exceeded",
                                }
                            )
                        )
                        send_telegram(
                            "*DB reconnect attempts exceeded* (single loop); backing off."
                        )
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
                    except Exception:
                        pass
                break
            except Exception as e:
                print(json.dumps({"fatal_error": str(e)}))
                send_telegram(f"*Fatal run error* (single loop): {e}")
                time.sleep(5)
    else:
        # Multiple markets - single monitoring loop so meta is shared per dex,
        # with per-coin parallelism inside poll_cycle.
        print(
            f"Starting multi-market monitoring for {len(coins)} markets: {', '.join(coins)}"
        )
        stop_event = threading.Event()

        thread = threading.Thread(
            target=poll_and_insert_all_markets,
            args=(coins, SUPABASE_DSN, DB_SCHEMA, stop_event),
            name="Monitor-All",
            daemon=True,
        )
        thread.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            stop_event.set()
            thread.join(timeout=5)
            print("All monitors stopped.")


if __name__ == "__main__":
    print(
        f"# Using INFO_URL={INFO_URL} (IS_TESTNET={IS_TESTNET}) "
        f"| markets={MARKETS} | dex={HL_DEX or 'auto'}"
    )
    run()