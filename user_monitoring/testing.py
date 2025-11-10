#!/usr/bin/env python3
"""
Stream HIP3 trades from Hyperliquid for a specific DEX/coin pair.

Target:
  dex  = "flxn"
  coin = "flxn:TSLA"

Requires: python 3.9+, websockets
Install:  pip install websockets

Run:      python stream_flxn_tsla_trades.py
"""

import asyncio
import json
import signal
import sys
from datetime import datetime, timezone

import websockets

WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"
SUBSCRIBE_PAYLOAD = {
    "method": "subscribe",
    "subscription": {
        "type": "trades",
        "dex": "flxn",
        "coin": "flxn:TSLA",
    },
}

# If the server supports ping/pong, websockets handles pong automatically.
PING_INTERVAL_SEC = 20
RECONNECT_BASE_DELAY = 1.0    # seconds
RECONNECT_MAX_DELAY = 30.0    # seconds


def ts_to_iso(ts):
    """Convert UNIX timestamp in seconds or ms to ISO8601."""
    if ts is None:
        return None
    # Accept both ms and s
    if ts > 10_000_000_000:  # ms
        ts = ts / 1000.0
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def pretty_trade(trade: dict) -> str:
    """
    Print complete trade information with all available fields.
    """
    if not isinstance(trade, dict):
        return str(trade)
    
    print("\n" + "="*80)
    print("🔄 NEW TRADE DETECTED")
    print("="*80)
    
    # Extract and display all trade fields
    coin = trade.get("coin", "N/A")
    side = trade.get("side", "N/A")
    px = trade.get("px", "N/A")  # price
    sz = trade.get("sz", "N/A")  # size
    hash_val = trade.get("hash", "N/A")
    time_val = trade.get("time", "N/A")
    tid = trade.get("tid", "N/A")
    users = trade.get("users", [])
    
    print(f"📊 Market:     {coin}")
    print(f"📈 Side:       {side}")
    print(f"💰 Price:      {px}")
    print(f"📦 Size:       {sz}")
    print(f"⏰ Time:       {ts_to_iso(time_val) if time_val != 'N/A' else 'N/A'}")
    print(f"🔗 Hash:       {hash_val}")
    print(f"🆔 Trade ID:   {tid}")
    
    if users and len(users) == 2:
        buyer, seller = users
        print(f"🟢 Buyer:      {buyer}")
        print(f"🔴 Seller:     {seller}")
    else:
        print(f"👥 Users:      {users}")
    
    # Calculate trade value
    if px != "N/A" and sz != "N/A":
        try:
            trade_value = float(px) * float(sz)
            print(f"💵 Trade Value: ${trade_value:,.2f}")
        except (ValueError, TypeError):
            print("💵 Trade Value: N/A")
    
    # Show any additional fields
    other_fields = {k: v for k, v in trade.items() 
                   if k not in ["coin", "side", "px", "sz", "hash", "time", "tid", "users"]}
    if other_fields:
        print("📋 Additional Fields:")
        for key, value in other_fields.items():
            print(f"   {key}: {value}")
    
    print("="*80 + "\n")
    
    # Return summary for the main loop
    try:
        value_str = f"${float(px)*float(sz):,.2f}" if px != "N/A" and sz != "N/A" else "N/A"
    except (ValueError, TypeError):
        value_str = "N/A"
    
    return f"Trade: {side} {sz} {coin} @ {px} (Value: {value_str} | Users: {len(users)})"


async def send_heartbeats(ws):
    """Periodically send pings to keep the connection alive."""
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL_SEC)
            try:
                await ws.ping()
            except Exception:
                # Up to the caller loop to reconnect
                return
    except asyncio.CancelledError:
        return


async def stream_trades():
    reconnect_delay = RECONNECT_BASE_DELAY

    while True:
        try:
            print(f"Connecting to {WS_URL} …")
            async with websockets.connect(WS_URL, ping_interval=None, ping_timeout=None) as ws:
                # Subscribe
                await ws.send(json.dumps(SUBSCRIBE_PAYLOAD))
                print(f"Subscribed: {json.dumps(SUBSCRIBE_PAYLOAD)}")

                # Start heartbeats
                hb_task = asyncio.create_task(send_heartbeats(ws))

                # Reset backoff on successful connect
                reconnect_delay = RECONNECT_BASE_DELAY

                # Receive loop
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception as e:
                        # Non-JSON message; print raw
                        print(f"⚠️ Non-JSON message: {raw}")
                        continue

                    # Debug: Print message structure
                    print(f"📨 Message keys: {list(msg.keys()) if isinstance(msg, dict) else 'Not dict'}")
                    
                    # Handle different message types
                    if isinstance(msg, dict):
                        channel = msg.get("channel")
                        
                        if channel == "subscriptionResponse":
                            print(f"✅ Subscription confirmed: {json.dumps(msg, indent=2)}")
                            continue
                        elif channel == "error":
                            print(f"❌ Error message: {json.dumps(msg, indent=2)}")
                            continue
                        elif channel == "trades":
                            # Standard format: {"channel": "trades", "data": [...]}
                            data = msg.get("data", [])
                            if data:
                                print(f"🎯 Found {len(data)} trades in message")
                                for tr in data:
                                    pretty_trade(tr)
                            continue
                    
                    # Try other possible formats
                    data = None
                    if isinstance(msg, dict):
                        if "data" in msg and isinstance(msg["data"], list):
                            data = msg["data"]
                        elif "data" in msg and isinstance(msg["data"], dict):
                            data = [msg["data"]]
                        elif "trades" in msg and isinstance(msg["trades"], list):
                            data = msg["trades"]
                        elif msg.get("type") == "trades" and isinstance(msg.get("trades"), list):
                            data = msg["trades"]

                    if data:
                        print(f"🎯 Found {len(data)} trades in alternate format")
                        for tr in data:
                            pretty_trade(tr)
                    else:
                        # Unknown message format - print for debugging
                        print(f"❓ Unknown message format: {json.dumps(msg, indent=2)[:500]}...")
                        
                        # Check if it's an array of trades directly
                        if isinstance(msg, list):
                            print(f"🎯 Found {len(msg)} trades in array format")
                            for tr in msg:
                                if isinstance(tr, dict):
                                    pretty_trade(tr)

                hb_task.cancel()
        except (websockets.ConnectionClosed, ConnectionError) as e:
            print(f"Connection closed: {e}")
        except Exception as e:
            print(f"Error: {e}")

        # Reconnect with backoff
        print(f"Reconnecting in {reconnect_delay:.1f}s …")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Graceful shutdown
    stop = asyncio.Event()

    def _handle_signal(*_):
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # Windows / limited env
            pass

    async def runner():
        worker = asyncio.create_task(stream_trades())
        await stop.wait()
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker

    import contextlib
    try:
        loop.run_until_complete(runner())
    finally:
        loop.close()


if __name__ == "__main__":
    main()