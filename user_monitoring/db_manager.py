"""Database management for user positions."""
import asyncio
import asyncpg
import json
from typing import Dict, List, Set, Optional, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
        
    async def initialize(self):
        self.pool = await asyncpg.create_pool(
            self.config.database_url,
            min_size=5,
            max_size=20,
            command_timeout=60
        )
        await self._ensure_tables()
        
    async def close(self):
        if self.pool:
            await self.pool.close()
            
    async def _ensure_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('CREATE SCHEMA IF NOT EXISTS user_positions')
            
            for market in self.config.target_markets:
                table_name = self._get_table_name(market)
                schema, table = table_name.split('.')
                
                await conn.execute(f'''
                    CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
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
                
                await conn.execute(f'''
                    CREATE INDEX IF NOT EXISTS "idx_{table.replace('.', '_')}_updated" 
                    ON "{schema}"."{table}" (last_updated)
                ''')
                
    def _get_table_name(self, market: str) -> str:
        if ':' in market:
            dex, coin = market.split(':', 1)
            return f'user_positions.{dex.lower()}_{coin.lower()}_positions'
        return f'user_positions.{market.lower()}_positions'
        
    async def get_existing_addresses(self, market: str) -> Set[str]:
        table_name = self._get_table_name(market)
        schema, table = table_name.split('.')
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f'SELECT address FROM "{schema}"."{table}"')
            return {row['address'] for row in rows}
            
    async def upsert_positions(self, market: str, positions: List[Dict[str, Any]]):
        if not positions:
            return
            
        table_name = self._get_table_name(market)
        schema, table = table_name.split('.')
        
        async with self.pool.acquire() as conn:
            await conn.executemany(f'''
                INSERT INTO "{schema}"."{table}" (
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
            
    async def remove_addresses(self, market: str, addresses: List[str]):
        if not addresses:
            return
            
        table_name = self._get_table_name(market)
        schema, table = table_name.split('.')
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                f'DELETE FROM "{schema}"."{table}" WHERE address = ANY($1::text[])',
                addresses
            )
            
    async def get_positions_batch(self, market: str, limit: int, offset: int) -> List[str]:
        table_name = self._get_table_name(market)
        schema, table = table_name.split('.')
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f'SELECT address FROM "{schema}"."{table}" ORDER BY last_updated ASC LIMIT $1 OFFSET $2',
                limit, offset
            )
            
            return [row['address'] for row in rows]
            
    async def get_stats(self) -> Dict[str, Any]:
        stats = {}
        
        async with self.pool.acquire() as conn:
            for market in self.config.target_markets:
                table_name = self._get_table_name(market)
                schema, table = table_name.split('.')
                
                row = await conn.fetchrow(f'''
                    SELECT 
                        COUNT(*) as count,
                        SUM(position_value) as total_value,
                        MAX(last_updated) as last_update
                    FROM "{schema}"."{table}"
                ''')
                
                stats[market] = {
                    'count': row['count'],
                    'total_value': float(row['total_value'] or 0),
                    'last_update': row['last_update']
                }
                
        return stats
