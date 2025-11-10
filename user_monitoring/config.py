"""Configuration management for user monitoring system."""
import os
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    database_url: str
    target_markets: List[str]
    min_position_value_usd: float = 1.0
    
    snapshot_dir: Path = Path.home() / "hl/data/periodic_abci_states"
    node_binary: Path = Path.home() / "hl-node"
    data_dir: Path = Path("./data")
    
    api_url: str = "http://127.0.0.1:3001/info"
    public_api_url: str = "https://api.hyperliquid.xyz/info"
    ws_url: str = "wss://api.hyperliquid.xyz/events"
    chain_type: str = "Mainnet"
    
    position_update_interval: int = 10
    position_batch_size: int = 100
    snapshot_retention_count: int = 5
    
    max_workers: int = 10
    api_timeout: int = 10
    retry_delay: float = 1.0
    max_retries: int = 3
    
    @property
    def rmp_base_path(self) -> Path:
        """Alias for snapshot_dir (compatibility with Anthias snapshot processor)."""
        return self.snapshot_dir
    
    @property
    def node_binary_path(self) -> Path:
        """Alias for node_binary (compatibility with Anthias snapshot processor)."""
        return self.node_binary
    
    @property
    def min_position_size_usd(self) -> float:
        """Alias for min_position_value_usd (compatibility with Anthias snapshot processor)."""
        return self.min_position_value_usd
    
    @classmethod
    def from_env(cls) -> "Config":
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL required")
        
        markets_str = os.getenv("TARGET_MARKETS", "BTC,ETH")
        target_markets = [m.strip() for m in markets_str.split(",") if m.strip()]
        
        data_dir = Path(os.getenv("DATA_DIR", "./data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        
        chain_type = os.getenv("CHAIN_TYPE", "Mainnet")
        if chain_type.lower() == "testnet":
            public_api_url = "https://api.hyperliquid-testnet.xyz/info"
            ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
        else:
            public_api_url = "https://api.hyperliquid.xyz/info"
            ws_url = "wss://api.hyperliquid.xyz/ws"
        
        return cls(
            database_url=database_url,
            target_markets=target_markets,
            data_dir=data_dir,
            public_api_url=public_api_url,
            ws_url=ws_url,
            chain_type=chain_type,
            position_update_interval=int(os.getenv("POSITION_UPDATE_INTERVAL", "10")),
            position_batch_size=int(os.getenv("POSITION_BATCH_SIZE", "100")),
            max_workers=int(os.getenv("MAX_WORKERS", "10")),
            api_timeout=int(os.getenv("API_TIMEOUT", "10"))
        )
