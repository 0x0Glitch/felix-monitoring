"""Database management for user positions."""
import asyncio
import asyncpg
import json
import re
from typing import Dict, List, Set, Optional, Any
from datetime import datetime, timedelta
import logging
from utils import send_telegram_alert

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
        
    async def initialize(self):
        self.pool = await asyncpg.create_pool(
            self.config.database_url,
            min_size=self.config.db_min_pool_size,
            max_size=self.config.db_max_pool_size,
            command_timeout=60,
            max_queries=50000,  # Limit queries per connection
            max_inactive_connection_lifetime=300  # Close idle connections after 5 minutes
        )
        await self._ensure_tables()
        # Start pool health monitoring
        asyncio.create_task(self._monitor_pool_health())
    
    async def _monitor_pool_health(self):
        """Monitor database pool health and warn on exhaustion"""
        last_alert_time = 0
        alert_cooldown = 300  # 5 minutes between similar alerts
        
        while True:
            try:
                await asyncio.sleep(30)
                if self.pool:
                    size = self.pool.get_size()
                    idle = self.pool.get_idle_size() 
                    current_time = asyncio.get_event_loop().time()
                    
                    if idle == 0 and size == self.config.db_max_pool_size:
                        logger.warning(f"Database pool exhausted: {size} connections, 0 idle")
                        # Send alert with cooldown
                        if current_time - last_alert_time > alert_cooldown:
                            send_telegram_alert(
                                f"🚨 *DB Pool Exhausted*\n"
                                f"All {size} connections in use\n"
                                f"No idle connections available",
                                self.config.telegram_bot_token,
                                self.config.telegram_chat_id
                            )
                            last_alert_time = current_time
                    elif idle < 2 and size > self.config.db_max_pool_size * 0.8:
                        logger.warning(f"Database pool near exhaustion: {size} connections, {idle} idle")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Pool health monitor error: {e}")
        
    async def close(self):
        if self.pool:
            await self.pool.close()
            
    async def _ensure_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('CREATE SCHEMA IF NOT EXISTS user_positions')
            
            for market in self.config.target_markets:
                table_name = self._get_table_name(market)
                schema, table = table_name.split('.')
                
                # Use PostgreSQL's quote_ident for safe identifier handling
                safe_schema = await conn.fetchval(
                    "SELECT quote_ident($1)", schema
                )
                safe_table = await conn.fetchval(
                    "SELECT quote_ident($1)", table
                )
                
                await conn.execute(f'''
                    CREATE TABLE IF NOT EXISTS {safe_schema}.{safe_table} (
                        address VARCHAR(42) PRIMARY KEY,
                        market VARCHAR(20),
                        position_size DECIMAL,
                        entry_price DECIMAL,
                        liquidation_price DECIMAL,
                        margin_used DECIMAL,
                        position_value DECIMAL,
                        unrealized_pnl DECIMAL,
                        return_on_equity DECIMAL,
                        leverage_type VARCHAR(10),
                        leverage_value INTEGER,
                        leverage_raw_usd DECIMAL,
                        account_value DECIMAL,
                        total_margin_used DECIMAL,
                        withdrawable DECIMAL,
                        last_updated TIMESTAMP DEFAULT NOW(),
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                safe_index_name = await conn.fetchval(
                    "SELECT quote_ident($1)", f"idx_{table.replace('.', '_')}_updated"
                )
                await conn.execute(f'''
                    CREATE INDEX IF NOT EXISTS {safe_index_name} 
                    ON {safe_schema}.{safe_table} (last_updated)
                ''')
                
    def _get_table_name(self, market: str) -> str:
        
        if not re.match(r'^[a-zA-Z0-9_:]+$', market):
            raise ValueError(f"Invalid market name: {market}")
        
        if ':' in market:
            dex, coin = market.split(':', 1)
            # Further sanitize individual components
            dex_safe = re.sub(r'[^a-zA-Z0-9_]', '', dex.lower())
            coin_safe = re.sub(r'[^a-zA-Z0-9_]', '', coin.lower())
            return f'user_positions.{dex_safe}_{coin_safe}_positions'
        
        market_safe = re.sub(r'[^a-zA-Z0-9_]', '', market.lower())
        return f'user_positions.{market_safe}_positions'
        
    async def get_existing_addresses(self, market: str) -> Set[str]:
        table_name = self._get_table_name(market)
        schema, table = table_name.split('.')
        async with self.pool.acquire() as conn:
            safe_schema = await conn.fetchval("SELECT quote_ident($1)", schema)
            safe_table = await conn.fetchval("SELECT quote_ident($1)", table)
            rows = await conn.fetch(f'SELECT address FROM {safe_schema}.{safe_table}')
            return {row['address'] for row in rows}
            
    async def upsert_positions(self, market: str, positions: List[Dict[str, Any]]):
        if not positions:
            return
            
        table_name = self._get_table_name(market)
        schema, table = table_name.split('.')
        
        try:
            async with self.pool.acquire() as conn:
                safe_schema = await conn.fetchval("SELECT quote_ident($1)", schema)
                safe_table = await conn.fetchval("SELECT quote_ident($1)", table)
                await conn.executemany(f'''
                INSERT INTO {safe_schema}.{safe_table} (
                    address, market, position_size, entry_price, liquidation_price,
                    margin_used, position_value, unrealized_pnl, return_on_equity,
                    leverage_type, leverage_value, leverage_raw_usd,
                    account_value, total_margin_used, withdrawable, last_updated
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW())
                ON CONFLICT (address) DO UPDATE SET
                    position_size = EXCLUDED.position_size,
                    entry_price = EXCLUDED.entry_price,
                    liquidation_price = EXCLUDED.liquidation_price,
                    margin_used = EXCLUDED.margin_used,
                    position_value = EXCLUDED.position_value,
                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                    return_on_equity = EXCLUDED.return_on_equity,
                    leverage_type = EXCLUDED.leverage_type,
                    leverage_value = EXCLUDED.leverage_value,
                    leverage_raw_usd = EXCLUDED.leverage_raw_usd,
                    account_value = EXCLUDED.account_value,
                    total_margin_used = EXCLUDED.total_margin_used,
                    withdrawable = EXCLUDED.withdrawable,
                    last_updated = NOW()
            ''', [(
                p['address'], market,
                p['position_size'], p['entry_price'], p['liquidation_price'],
                p['margin_used'], p['position_value'], p['unrealized_pnl'], p['return_on_equity'],
                p['leverage_type'], p['leverage_value'], p['leverage_raw_usd'],
                p['account_value'], p['total_margin_used'], p['withdrawable']
            ) for p in positions])
        except asyncpg.exceptions.TooManyConnectionsError:
            logger.error(f"Database connection pool exhausted for {market}")
            send_telegram_alert(
                f"🛑 *DB Connection Pool Exhausted*\n"
                f"Market: {market}\n"
                f"Cannot write position updates",
                self.config.telegram_bot_token,
                self.config.telegram_chat_id
            )
            # Add exponential backoff and retry
            await asyncio.sleep(1)
            raise
        except asyncpg.exceptions.PostgresError as e:
            logger.error(f"PostgreSQL error for {market}: {e}")
            send_telegram_alert(
                f"🔴 *Database Error*\n"
                f"Market: {market}\n"
                f"Error: {str(e)[:100]}",
                self.config.telegram_bot_token,
                self.config.telegram_chat_id
            )
            raise
        except Exception as e:
            logger.error(f"Failed to upsert positions for {market}: {e}")
            raise
            
    async def remove_addresses(self, market: str, addresses: List[str]):
        if not addresses:
            return
            
        table_name = self._get_table_name(market)
        schema, table = table_name.split('.')
        
        async with self.pool.acquire() as conn:
            safe_schema = await conn.fetchval("SELECT quote_ident($1)", schema)
            safe_table = await conn.fetchval("SELECT quote_ident($1)", table)
            await conn.execute(
                f'DELETE FROM {safe_schema}.{safe_table} WHERE address = ANY($1::text[])',
                addresses
            )
            
    async def get_positions_batch(self, market: str, limit: int, offset: int) -> List[str]:
        table_name = self._get_table_name(market)
        schema, table = table_name.split('.')
        
        async with self.pool.acquire() as conn:
            safe_schema = await conn.fetchval("SELECT quote_ident($1)", schema)
            safe_table = await conn.fetchval("SELECT quote_ident($1)", table)
            rows = await conn.fetch(
                f'SELECT address FROM {safe_schema}.{safe_table} ORDER BY last_updated ASC LIMIT $1 OFFSET $2',
                limit, offset
            )
            
            return [row['address'] for row in rows]
            
    async def get_stats(self) -> Dict[str, Any]:
        stats = {}
        
        async with self.pool.acquire() as conn:
            for market in self.config.target_markets:
                table_name = self._get_table_name(market)
                schema, table = table_name.split('.')
                
                safe_schema = await conn.fetchval("SELECT quote_ident($1)", schema)
                safe_table = await conn.fetchval("SELECT quote_ident($1)", table)
                
                row = await conn.fetchrow(f'''
                    SELECT 
                        COUNT(*) as count,
                        SUM(position_value) as total_value,
                        MAX(last_updated) as last_update
                    FROM {safe_schema}.{safe_table}
                ''')
                
                stats[market] = {
                    'count': row['count'],
                    'total_value': float(row['total_value'] or 0),
                    'last_update': row['last_update']
                }
                
        return stats
