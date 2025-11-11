"""WebSocket handler for monitoring new user addresses via trades."""
import asyncio
import json
import websockets
from typing import Dict, Set, Callable, Optional
from datetime import datetime
from collections import deque
import logging

from utils import is_valid_address

logger = logging.getLogger(__name__)


class WebSocketHandler:
    def __init__(self, config, on_new_address: Callable):
        self.config = config
        self.on_new_address = on_new_address
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.reconnect_delay = config.ws_reconnect_delay
        self.max_reconnect_delay = config.ws_max_reconnect_delay
        self.ping_interval = config.ws_ping_interval
        self.run_task = None  # Store task reference
        
        # Message buffering for partial messages
        self.message_buffer = ""  # Buffer for partial messages
        self.max_buffer_size = 1024 * 1024  # 1MB max buffer
        
        # Track last trade timestamp to detect gaps
        self.last_trade_time = {}  # market -> timestamp
        self.seen_trade_ids = {}   # market -> collections.deque for ordered tracking
        self.max_trade_ids_per_market = config.ws_max_trade_ids_per_market
        
    async def start(self):
        self.running = True
        self.run_task = asyncio.create_task(self._run())  # Store the task
        
    async def stop(self):
        self.running = False
        if self.ws:
            await self.ws.close()
            
    async def _run(self):
        while self.running:
            try:
                await self._connect_and_subscribe()
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
                
    async def _connect_and_subscribe(self):
        # Fix WebSocket URL - use /ws endpoint
        ws_url = self.config.ws_url.replace('/events', '/ws')
        
        logger.info(f"Connecting to WebSocket: {ws_url}")
        
        # Connection options for better stability (from config)
        connect_options = {
            'ping_interval': self.config.ws_ping_interval,
            'ping_timeout': self.config.ws_ping_timeout,
            'close_timeout': 10,  # Wait 10 seconds for close
            'max_size': self.config.ws_max_message_size
        }
        
        async with websockets.connect(ws_url, **connect_options) as ws:
            self.ws = ws
            self.reconnect_delay = self.config.ws_reconnect_delay  # Reset to config value, not hardcoded
            logger.info("WebSocket connected successfully")
            
            # Start heartbeat task for additional keepalive
            heartbeat_task = asyncio.create_task(self._send_heartbeats())
            
            try:
                # Subscribe to each market with delay to avoid overwhelming
                for market in self.config.target_markets:
                    await self._subscribe_to_market(market)
                    await asyncio.sleep(0.1)  # Small delay between subscriptions
                
                logger.info(f"Subscribed to {len(self.config.target_markets)} markets")
                    
                # Handle incoming messages
                await self._handle_messages()
            except Exception as e:
                logger.error(f"Error in WebSocket handler: {e}")
                raise
            finally:
                heartbeat_task.cancel()
                logger.info("WebSocket connection closed")
                
    async def _send_heartbeats(self):
        """Periodically send pings to keep connection alive."""
        try:
            while True:
                await asyncio.sleep(self.ping_interval)
                if self.ws:
                    try:
                        await self.ws.ping()
                    except Exception:
                        return
        except asyncio.CancelledError:
            return
            
    async def _subscribe_to_market(self, market: str):
        """Subscribe to trades for a specific market."""
        subscription = {
            "method": "subscribe",
            "subscription": {
                "type": "trades",
                "coin": market  # Always use the full market name (e.g., "flxn:TSLA")
            }
        }
        
        await self.ws.send(json.dumps(subscription))
        logger.debug(f"Subscribing to {market} trades")
        
    async def _handle_messages(self):
        consecutive_errors = 0
        while self.running:
            try:
                message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                await self._process_message(message)
                consecutive_errors = 0  # Reset on successful message
            except asyncio.TimeoutError:
                # No message in 30s, send ping to keep alive
                try:
                    await self.ws.ping()
                    logger.debug("Sent ping after timeout")
                except Exception:
                    logger.warning("Failed to send ping, connection may be dead")
                    break
            except websockets.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
                break
            except Exception as e:
                logger.error(f"Unexpected message processing error: {e}")
                consecutive_errors += 1
                if consecutive_errors > 5:
                    logger.error("Too many consecutive errors, reconnecting")
                    break
                await asyncio.sleep(0.5)
                
    async def _process_message(self, message: str):
        try:
            # Check if we have a partial message buffered
            if self.message_buffer:
                message = self.message_buffer + message
                self.message_buffer = ""
            
            # Try to parse JSON
            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                # Might be a partial message
                if len(message) < self.max_buffer_size:
                    self.message_buffer = message
                    logger.debug(f"Buffering partial message ({len(message)} bytes)")
                    return
                else:
                    # Too large or corrupted, discard
                    self.message_buffer = ""
                    logger.error(f"Message too large or corrupted ({len(message)} bytes), discarding")
                    return
            
            # Successfully parsed, clear any buffer
            self.message_buffer = ""
            
            if isinstance(data, dict):
                # Process message by type
                if data.get('channel') == 'trades':
                    trades = data.get('data', [])
                    if trades:
                        await self._process_trades(trades)
                elif data.get('channel') == 'subscriptionResponse':
                    logger.info(f"Subscription response: {data}")
                elif data.get('channel') == 'error':
                    logger.error(f"WebSocket error: {data}")
                    
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}", exc_info=True)
            
    async def _process_trades(self, trades: list):
        market_addresses = {}  # Track addresses per market
        new_trades_count = 0
        duplicate_trades_count = 0
        
        for trade in trades:
            if isinstance(trade, dict):
                coin = trade.get('coin')
                if not coin:
                    continue
                
                # Track trade to detect duplicates and gaps
                trade_id = trade.get('tid')
                trade_time = trade.get('time')
                
                if coin not in self.seen_trade_ids:
                    self.seen_trade_ids[coin] = deque(maxlen=self.max_trade_ids_per_market)
                
                # Check if we've seen this trade before (duplicate from reconnect buffer)
                if trade_id and trade_id in self.seen_trade_ids[coin]:
                    duplicate_trades_count += 1
                    logger.debug(f"Duplicate trade {trade_id} for {coin} (expected on reconnect)")
                    continue
                
                # Track new trade with automatic FIFO cleanup
                if trade_id:
                    self.seen_trade_ids[coin].append(trade_id)
                    # deque with maxlen automatically drops oldest when full
                
                # Update last trade time for gap detection
                if trade_time:
                    if coin in self.last_trade_time:
                        time_gap = trade_time - self.last_trade_time[coin]
                        if time_gap > 300000:  # 5 minutes gap
                            logger.warning(f"Large time gap detected for {coin}: {time_gap/1000:.1f}s - possible missed trades")
                    self.last_trade_time[coin] = trade_time
                
                new_trades_count += 1
                    
                # Extract addresses from users field [buyer, seller]
                users = trade.get('users', [])
                if isinstance(users, list) and len(users) == 2:
                    buyer, seller = users
                    
                    # Initialize set for this market if not exists
                    if coin not in market_addresses:
                        market_addresses[coin] = set()
                    
                    # Add valid addresses
                    for addr in [buyer, seller]:
                        if addr and is_valid_address(addr):
                            market_addresses[coin].add(addr.lower())
                            logger.debug(f"Found address {addr[:10]}... in {coin} trade")
                
                # Trade processed
        
        # Log processing summary
        if new_trades_count > 0 or duplicate_trades_count > 0:
            logger.info(f"Processed {new_trades_count} new trades, {duplicate_trades_count} duplicates")
        
        # Process addresses for each market
        for coin, addresses in market_addresses.items():
            if addresses and coin in self.config.target_markets:
                logger.info(f"Found {len(addresses)} new addresses in {coin} trades")
                await self.on_new_address(coin, addresses)
