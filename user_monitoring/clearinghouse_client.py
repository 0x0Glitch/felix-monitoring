"""Client for fetching clearinghouse state from API."""
import asyncio
import aiohttp
from typing import Dict, List, Optional, Set, Any
from datetime import datetime
import logging

from utils import safe_float

logger = logging.getLogger(__name__)


class ClearinghouseClient:
    def __init__(self, config):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(config.max_workers)
        
        # Rate limiting for public API fallback
        self._public_api_semaphore = asyncio.Semaphore(5)  # Max 5 concurrent public API calls
        self._last_public_api_call = 0
        self._min_public_api_interval = 0.2  # 200ms between public API calls
        
        # Node failure tracking
        self._local_node_failures = 0
        self._local_node_failure_threshold = 3
        self._use_public_api_fallback = False
        self._last_node_check = 0
        self._node_check_interval = 60  # Check node health every 60s
        
    async def start(self):
        timeout = aiohttp.ClientTimeout(total=self.config.api_timeout)
        self.session = aiohttp.ClientSession(timeout=timeout)
        
    async def close(self):
        if self.session:
            await self.session.close()
            
    async def fetch_clearinghouse_state(self, address: str, dex_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        async with self._semaphore:
            # Ensure address is properly formatted
            normalized_address = address.lower().strip()
            if not normalized_address.startswith('0x'):
                normalized_address = f"0x{normalized_address}"
            
            payload = {
                "type": "clearinghouseState",
                "user": normalized_address
            }
            
            # Add dex parameter for custom DEX markets
            if dex_name:
                payload["dex"] = dex_name
            
            # Determine which API to use
            should_use_local = (
                self.config.chain_type.lower() != "testnet" and 
                not self._use_public_api_fallback
            )
            
            if should_use_local:
                # Try local node first
                result = await self._try_local_api(payload, address)
                if result is not None:
                    self._local_node_failures = 0  # Reset failure count on success
                    return result
                else:
                    # Local node failed, increment failure count
                    self._local_node_failures += 1
                    logger.warning(f"Local node failure #{self._local_node_failures} for {address}")
                    
                    # Switch to public API fallback if threshold reached
                    if self._local_node_failures >= self._local_node_failure_threshold:
                        logger.error(f"Local node failed {self._local_node_failures} times, switching to public API fallback")
                        self._use_public_api_fallback = True
                        self._last_node_check = asyncio.get_event_loop().time()
            
            # Use public API (either testnet or fallback)
            return await self._try_public_api(payload, address)
    
    async def _try_local_api(self, payload: Dict[str, Any], address: str) -> Optional[Dict[str, Any]]:
        """Try local node API."""
        for attempt in range(self.config.max_retries):
            try:
                async with self.session.post(
                    self.config.api_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=5)  # Shorter timeout for local node
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                    else:
                        logger.debug(f"Local API status {response.status} for {address}")
                        break  # Don't retry on non-timeout errors
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.debug(f"Local API error for {address} (attempt {attempt + 1}): {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay)
            except Exception as e:
                logger.debug(f"Local API unexpected error for {address}: {e}")
                break
        return None
    
    async def _try_public_api(self, payload: Dict[str, Any], address: str) -> Optional[Dict[str, Any]]:
        """Try public API with rate limiting."""
        async with self._public_api_semaphore:
            # Rate limiting for public API
            current_time = asyncio.get_event_loop().time()
            time_since_last_call = current_time - self._last_public_api_call
            if time_since_last_call < self._min_public_api_interval:
                sleep_time = self._min_public_api_interval - time_since_last_call
                await asyncio.sleep(sleep_time)
            
            self._last_public_api_call = asyncio.get_event_loop().time()
            
            for attempt in range(self.config.max_retries):
                try:
                    async with self.session.post(
                        self.config.public_api_url,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=10)  # Longer timeout for public API
                    ) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            # Exponential backoff for rate limiting
                            backoff_time = self.config.retry_delay * (2 ** attempt)
                            logger.warning(f"Public API rate limited, backing off {backoff_time}s")
                            await asyncio.sleep(backoff_time)
                        else:
                            logger.debug(f"Public API status {response.status} for {address}")
                            return None
                except asyncio.TimeoutError:
                    logger.debug(f"Public API timeout for {address} (attempt {attempt + 1})")
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                except Exception as e:
                    logger.debug(f"Public API error for {address}: {e}")
                    return None
            return None
    
    async def _check_local_node_recovery(self):
        """Periodically check if local node has recovered."""
        current_time = asyncio.get_event_loop().time()
        if (self._use_public_api_fallback and 
            current_time - self._last_node_check > self._node_check_interval):
            
            logger.info("Checking if local node has recovered...")
            
            # Test with a simple health check payload
            test_payload = {"type": "exchangeStatus"}
            try:
                async with self.session.post(
                    self.config.api_url,
                    json=test_payload,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        logger.info("✅ Local node has recovered, switching back from public API fallback")
                        self._use_public_api_fallback = False
                        self._local_node_failures = 0
                    else:
                        logger.debug(f"Local node still unhealthy: {response.status}")
            except Exception as e:
                logger.debug(f"Local node still failing: {e}")
            
            self._last_node_check = current_time
            
    async def process_addresses_batch(
        self, 
        addresses: List[str], 
        target_markets: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        # Check if local node has recovered (if we're in fallback mode)
        await self._check_local_node_recovery()
        
        tasks = []
        for address in addresses:
            tasks.append(self._process_single_address(address, target_markets))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        positions = {}
        total_positions = 0
        for address, result in zip(addresses, results):
            if isinstance(result, dict):
                positions[address] = result
                total_positions += len(result)
        
        logger.info(f"Processed {len(addresses)} addresses: {len(positions)} with positions, {total_positions} total positions found")
        return positions
        
    async def _process_single_address(
        self, 
        address: str, 
        target_markets: List[str]
    ) -> Dict[str, Any]:
        # Extract DEX name for custom markets
        dex_name = None
        for market in target_markets:
            if ':' in market:
                # Extract dex name from market (e.g., "flxn:TSLA" -> "flxn")
                dex_name = market.split(':')[0].lower()
                break
        
        state = await self.fetch_clearinghouse_state(address, dex_name)
        if not state:
            return {}
            
        positions = {}
        
        asset_positions = state.get('assetPositions', [])
        logger.debug(f"Address {address}: Found {len(asset_positions)} asset positions (dex={dex_name})")
        
        for asset_pos in asset_positions:
            position = asset_pos.get('position', {})
            coin_raw = position.get('coin', '')  # Keep original case
            
            logger.debug(f"Address {address}: Checking coin '{coin_raw}' against targets {target_markets}")
            
            # Check if this coin matches any target market
            # For custom DEX markets, the API returns the full name "flxn:TSLA"
            # For standard markets, the API returns just the symbol "BTC"
            market_match = None
            for target_market in target_markets:
                if ':' in target_market:
                    # Custom DEX market - direct comparison (case-sensitive for DEX part)
                    if coin_raw == target_market:
                        market_match = target_market
                        break
                else:
                    # Standard market - case-insensitive comparison
                    if coin_raw.upper() == target_market.upper():
                        market_match = target_market
                        break
            
            # Skip if not in target markets
            if not market_match:
                logger.debug(f"Address {address}: Skipping coin '{coin_raw}' (not in targets)")
                continue
            
            coin = market_match  # Use the matched target market name
            logger.debug(f"Address {address}: Matched coin '{coin}'")
                
            szi = safe_float(position.get('szi', '0'))
            logger.debug(f"Address {address}: Coin '{coin}' has position size {szi}")
            if szi == 0:
                logger.debug(f"Address {address}: Skipping coin '{coin}' (zero position size)")
                continue
                
            leverage = position.get('leverage', {})
            # logger.info(f"Address {address}: Leverage data for {coin}: {leverage}")
            entry_px = safe_float(position.get('entryPx', '0'))
            liquidation_px = safe_float(position.get('liquidationPx', '0'))
            margin_used = safe_float(position.get('marginUsed', '0'))
            position_value = safe_float(position.get('positionValue', '0'))
            unrealized_pnl = safe_float(position.get('unrealizedPnl', '0'))
            return_on_equity = safe_float(position.get('returnOnEquity', '0'))
            
            positions[coin] = {
                'address': address.lower(),
                'position_size': szi,
                'entry_price': entry_px,
                'liquidation_price': liquidation_px,
                'margin_used': margin_used,
                'position_value': position_value,
                'unrealized_pnl': unrealized_pnl,
                'return_on_equity': return_on_equity,
                'leverage_type': leverage.get('type', 'cross'),
                'leverage_value': safe_float(leverage.get('value', '0')) if leverage.get('value') is not None else None,
                'leverage_raw_usd': safe_float(leverage.get('rawUsd', '0')),
                'account_value': safe_float(state.get('marginSummary', {}).get('accountValue', '0')),
                'total_margin_used': safe_float(state.get('marginSummary', {}).get('totalMarginUsed', '0')),
                'withdrawable': safe_float(state.get('withdrawable', '0'))
            }
            logger.debug(f"✓ Address {address}: Added position for {coin} (size={szi}, value=${position_value})")
            
        return positions
