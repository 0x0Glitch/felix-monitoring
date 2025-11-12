"""Main coordinator for user monitoring system."""
import asyncio
import signal
import sys
from typing import Dict, Set, List, Any
from datetime import datetime, timedelta
import logging

from config import Config
from db_manager import DatabaseManager
from snapshot_processor import SnapshotProcessor
from clearinghouse_client import ClearinghouseClient
from websocket_handler import WebSocketHandler

logger = logging.getLogger(__name__)


class UserMonitor:
    def __init__(self, config: Config):
        self.config = config
        self.running = False
        self.shutdown_event = asyncio.Event()
        
        self.db = DatabaseManager(config)
        self.snapshot = SnapshotProcessor(config)
        self.clearinghouse = ClearinghouseClient(config)
        self.websocket = None
        
        self.address_lock = asyncio.Lock()
        self.market_addresses: Dict[str, Set[str]] = {
            market: set() for market in config.target_markets
        }
        
        self.tasks: List[asyncio.Task] = []
        self.stats = {
            'snapshots_processed': 0,
            'addresses_added': 0,
            'addresses_removed': 0,
            'positions_updated': 0,
            'start_time': datetime.now()
        }
        
    async def start(self):
        logger.info(f"Starting User Monitor for markets: {', '.join(self.config.target_markets)}")
        
        try:
            await self._initialize()
            self._setup_signals()
            await self._initial_sync()
            
            self.running = True
            await self._start_workers()
            await self._run()
            
        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            await self.stop()
            
    async def _initialize(self):
        await self.db.initialize()
        await self.clearinghouse.start()
        
        self.websocket = WebSocketHandler(self.config, self._handle_new_addresses)
        
        logger.info("Components initialized")
        
    def _setup_signals(self):
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}")
            self.running = False
            self.shutdown_event.set()
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
    async def _initial_sync(self):
        logger.info("Performing initial sync with snapshot...")
        
        success, snapshot_addresses = await self.snapshot.process_latest()
        
        if success and snapshot_addresses:
            for market, addresses in snapshot_addresses.items():
                existing = await self.db.get_existing_addresses(market)
                
                to_add = addresses - existing
                to_remove = existing - addresses
                
                if to_remove:
                    logger.info(f"{market}: Removing {len(to_remove)} addresses not in snapshot")
                    await self.db.remove_addresses(market, list(to_remove))
                    self.stats['addresses_removed'] += len(to_remove)
                    
                if to_add:
                    logger.info(f"{market}: Adding {len(to_add)} new addresses from snapshot")
                    positions = await self._fetch_positions_for_addresses(list(to_add), [market])
                    await self._save_positions(market, positions)
                    self.stats['addresses_added'] += len(to_add)
                    
                async with self.address_lock:
                    self.market_addresses[market] = addresses.copy()
                    
            self.stats['snapshots_processed'] += 1
            logger.info("Initial sync completed")
        else:
            logger.warning("No snapshot available for initial sync")
            
            for market in self.config.target_markets:
                existing = await self.db.get_existing_addresses(market)
                async with self.address_lock:
                    self.market_addresses[market] = existing
                    
    async def _start_workers(self):
        self.tasks = [
            asyncio.create_task(self._position_update_worker(), name="position_updater"),
            asyncio.create_task(self._websocket_worker(), name="websocket_worker"),
            asyncio.create_task(self._stats_reporter(), name="stats_reporter")
        ]
        
        logger.info(f"Started {len(self.tasks)} workers")
        
    async def _run(self):
        while self.running:
            try:
                await asyncio.wait_for(
                    self.shutdown_event.wait(),
                    timeout=1.0
                )
            except asyncio.TimeoutError:
                failed_tasks = [t for t in self.tasks if t.done() and not t.cancelled()]
                
                for task in failed_tasks:
                    if exc := task.exception():
                        logger.error(f"Task {task.get_name()} failed: {exc}")
                        
                if len(failed_tasks) > len(self.tasks) // 2:
                    logger.critical("Too many worker failures, shutting down")
                    self.running = False
                    break
                    
    async def _position_update_worker(self):
        """Job 2: Update positions for existing addresses"""
        while self.running:
            try:
                await asyncio.sleep(self.config.position_update_interval)
                
                for market in self.config.target_markets:
                    async with self.address_lock:
                        addresses = list(self.market_addresses[market])
                        
                    if not addresses:
                        continue
                        
                    for i in range(0, len(addresses), self.config.position_batch_size):
                        batch = addresses[i:i+self.config.position_batch_size]
                        positions = await self._fetch_positions_for_addresses(batch, [market])
                        
                        active_positions = []
                        closed_addresses = []
                        api_failed_addresses = []
                        
                        for address in batch:
                            if address not in positions:
                                # API call failed for this address - DO NOT remove
                                api_failed_addresses.append(address)
                                logger.debug(f"API failed for {address}, keeping in database")
                            elif market in positions[address]:
                                # API succeeded and user has position for this market
                                active_positions.append(positions[address][market])
                            else:
                                # API succeeded but user has no position for this market
                                closed_addresses.append(address)
                                
                        if active_positions:
                            await self.db.upsert_positions(market, active_positions)
                            self.stats['positions_updated'] += len(active_positions)
                            
                        if closed_addresses:
                            logger.info(f"{market}: Removing {len(closed_addresses)} addresses with confirmed zero positions")
                            await self.db.remove_addresses(market, closed_addresses)
                            async with self.address_lock:
                                self.market_addresses[market] -= set(closed_addresses)
                            self.stats['addresses_removed'] += len(closed_addresses)
                            
                        if api_failed_addresses:
                            logger.warning(f"{market}: {len(api_failed_addresses)} addresses had API failures - NOT removing from database")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Position update worker error: {e}", exc_info=True)
                await asyncio.sleep(10)
                
    async def _websocket_worker(self):
        """Job 1 - Part B: Monitor WebSocket for new addresses"""
        await self.websocket.start()
        
        while self.running:
            await asyncio.sleep(1)
            
        await self.websocket.stop()
        
    async def _handle_new_addresses(self, market: str, addresses: Set[str]):
        """Handle new addresses from WebSocket"""
        async with self.address_lock:
            current = self.market_addresses.get(market, set())
            new_addresses = addresses - current
            
        if new_addresses:
            positions = await self._fetch_positions_for_addresses(list(new_addresses), [market])
            
            # Only add addresses where API call succeeded
            successfully_fetched = set(positions.keys())
            failed_addresses = new_addresses - successfully_fetched
            
            if failed_addresses:
                logger.warning(f"{market}: API failed for {len(failed_addresses)} new addresses from WebSocket - skipping")
            
            await self._save_positions(market, positions)
            
            # Only add successfully fetched addresses to tracking
            async with self.address_lock:
                self.market_addresses[market] |= successfully_fetched
                
            self.stats['addresses_added'] += len(successfully_fetched)
            
    async def _fetch_positions_for_addresses(
        self, 
        addresses: List[str], 
        markets: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        return await self.clearinghouse.process_addresses_batch(addresses, markets)
        
    async def _save_positions(self, market: str, positions_by_address: Dict[str, Dict[str, Any]]):
        positions = []
        addresses_with_no_positions = []
        
        for address, market_positions in positions_by_address.items():
            if market in market_positions:
                positions.append(market_positions[market])
            else:
                # API succeeded but no position for this market
                addresses_with_no_positions.append(address)
        
        if positions:
            await self.db.upsert_positions(market, positions)
            logger.info(f"{market}: Saved {len(positions)} active positions to database")
        
        if addresses_with_no_positions:
            logger.debug(f"{market}: {len(addresses_with_no_positions)} addresses have no positions (API confirmed)")
        
        if not positions and not addresses_with_no_positions:
            logger.debug(f"{market}: No successful API responses to process")
            
    async def _stats_reporter(self):
        while self.running:
            try:
                await asyncio.sleep(60)
                
                db_stats = await self.db.get_stats()
                uptime = datetime.now() - self.stats['start_time']
                
                logger.info("="*60)
                logger.info(f"Uptime: {uptime}")
                logger.info(f"Snapshots: {self.stats['snapshots_processed']}")
                logger.info(f"Addresses: +{self.stats['addresses_added']} -{self.stats['addresses_removed']}")
                logger.info(f"Positions updated: {self.stats['positions_updated']}")
                
                for market, stats in db_stats.items():
                    logger.info(f"{market}: {stats['count']} positions, ${stats['total_value']:,.2f}")
                    
                logger.info("="*60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stats reporter error: {e}")
                
    async def stop(self):
        logger.info("Shutting down...")
        
        self.running = False
        self.shutdown_event.set()
        
        if self.tasks:
            for task in self.tasks:
                task.cancel()
                
            await asyncio.gather(*self.tasks, return_exceptions=True)
            
        await self.clearinghouse.close()
        await self.db.close()
        
        logger.info("Shutdown complete")
