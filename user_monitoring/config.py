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
    
    # Snapshot settings
    snapshot_dir: Path
    node_binary: Path
    data_dir: Path
    min_position_value_usd: float
    
    # API settings
    local_api_url: str
    local_api_enabled: bool
    public_api_url: str
    ws_url: str
    chain_type: str
    
    # Position update settings
    position_update_interval: int
    position_batch_size: int
    
    # API client settings
    max_workers: int
    api_timeout: int
    retry_delay: float
    max_retries: int
    
    # Public API fallback settings
    public_api_max_concurrent: int
    public_api_min_interval: float
    local_node_failure_threshold: int
    node_health_check_interval: int
    
    # WebSocket settings
    ws_reconnect_delay: float
    ws_max_reconnect_delay: float
    ws_ping_interval: int
    ws_ping_timeout: int
    ws_max_message_size: int
    ws_max_trade_ids_per_market: int
    
    # Database pool settings
    db_min_pool_size: int
    db_max_pool_size: int
    
    # Snapshot management
    snapshot_retention_count: int
    
    # Telegram alerting (optional)
    telegram_bot_token: Optional[str]
    telegram_chat_id: Optional[str]
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        import re
        
        # Validate required fields
        if not self.database_url:
            raise ValueError("DATABASE_URL is required")
        
        if not self.target_markets:
            raise ValueError("At least one target market is required")
        
        # Validate paths
        if not self.snapshot_dir.exists():
            raise ValueError(f"Snapshot directory does not exist: {self.snapshot_dir}")
        
        if not self.node_binary.exists():
            raise ValueError(f"Node binary not found: {self.node_binary}")
        
        # Ensure data directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Validate numeric ranges
        if self.position_batch_size <= 0 or self.position_batch_size > 1000:
            raise ValueError(f"Invalid position_batch_size: {self.position_batch_size} (must be 1-1000)")
        
        if self.api_timeout <= 0 or self.api_timeout > 300:
            raise ValueError(f"Invalid api_timeout: {self.api_timeout} (must be 1-300 seconds)")
        
        if self.min_position_value_usd < 0:
            raise ValueError(f"Invalid min_position_value_usd: {self.min_position_value_usd} (must be >= 0)")
        
        if self.max_workers <= 0 or self.max_workers > 100:
            raise ValueError(f"Invalid max_workers: {self.max_workers} (must be 1-100)")
        
        if self.db_min_pool_size <= 0 or self.db_min_pool_size > self.db_max_pool_size:
            raise ValueError(f"Invalid db_min_pool_size: {self.db_min_pool_size} (must be > 0 and <= max_pool_size)")
        
        if self.db_max_pool_size <= 0 or self.db_max_pool_size > 100:
            raise ValueError(f"Invalid db_max_pool_size: {self.db_max_pool_size} (must be 1-100)")
        
        # Validate market names
        for market in self.target_markets:
            if not re.match(r'^[a-zA-Z0-9_:]+$', market):
                raise ValueError(f"Invalid market name: {market} (only alphanumeric, underscore, and colon allowed)")
        
        # Validate URLs
        if not self.local_api_url.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid local_api_url: {self.local_api_url} (must start with http:// or https://)")
        
        if not self.public_api_url.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid public_api_url: {self.public_api_url} (must start with http:// or https://)")
        
        if not self.ws_url.startswith(('ws://', 'wss://')):
            raise ValueError(f"Invalid ws_url: {self.ws_url} (must start with ws:// or wss://)")
        
        # Validate chain type
        if self.chain_type not in ['Mainnet', 'Testnet', 'mainnet', 'testnet']:
            raise ValueError(f"Invalid chain_type: {self.chain_type} (must be Mainnet or Testnet)")
        
        # Validate intervals and delays
        if self.position_update_interval <= 0:
            raise ValueError(f"Invalid position_update_interval: {self.position_update_interval} (must be > 0)")
        
        if self.retry_delay <= 0:
            raise ValueError(f"Invalid retry_delay: {self.retry_delay} (must be > 0)")
        
        if self.public_api_min_interval < 0:
            raise ValueError(f"Invalid public_api_min_interval: {self.public_api_min_interval} (must be >= 0)")
        
        # Validate WebSocket settings
        if self.ws_reconnect_delay <= 0:
            raise ValueError(f"Invalid ws_reconnect_delay: {self.ws_reconnect_delay} (must be > 0)")
        
        if self.ws_max_reconnect_delay < self.ws_reconnect_delay:
            raise ValueError(f"ws_max_reconnect_delay ({self.ws_max_reconnect_delay}) must be >= ws_reconnect_delay ({self.ws_reconnect_delay})")
        
        if self.ws_ping_interval <= 0:
            raise ValueError(f"Invalid ws_ping_interval: {self.ws_ping_interval} (must be > 0)")
        
        if self.ws_ping_timeout <= 0 or self.ws_ping_timeout > self.ws_ping_interval:
            raise ValueError(f"Invalid ws_ping_timeout: {self.ws_ping_timeout} (must be > 0 and <= ping_interval)")
        
        if self.ws_max_message_size <= 0:
            raise ValueError(f"Invalid ws_max_message_size: {self.ws_max_message_size} (must be > 0)")
        
        if self.ws_max_trade_ids_per_market <= 0:
            raise ValueError(f"Invalid ws_max_trade_ids_per_market: {self.ws_max_trade_ids_per_market} (must be > 0)")
    
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
        # Required settings
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL required")
        
        markets_str = os.getenv("TARGET_MARKETS", "BTC,ETH")
        target_markets = [m.strip() for m in markets_str.split(",") if m.strip()]
        
        # Snapshot settings
        snapshot_dir = Path(os.getenv("SNAPSHOT_DIR", str(Path.home() / "hl/data/periodic_abci_states")))
        node_binary = Path(os.getenv("NODE_BINARY", str(Path.home() / "hl-node")))
        data_dir = Path(os.getenv("DATA_DIR", "./data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Chain configuration
        chain_type = os.getenv("CHAIN_TYPE", "Mainnet")
        
        # API URLs - auto-configure based on chain type if not explicitly set
        if chain_type.lower() == "testnet":
            default_public_api = "https://api.hyperliquid-testnet.xyz/info"
            default_ws_url = "wss://api.hyperliquid-testnet.xyz/ws"
        else:
            default_public_api = "https://api.hyperliquid.xyz/info"
            default_ws_url = "wss://api.hyperliquid.xyz/ws"
        
        public_api_url = os.getenv("PUBLIC_API_URL", default_public_api)
        ws_url = os.getenv("WS_URL", default_ws_url)
        
        return cls(
            database_url=database_url,
            target_markets=target_markets,
            snapshot_dir=snapshot_dir,
            node_binary=node_binary,
            data_dir=data_dir,
            min_position_value_usd=float(os.getenv("MIN_POSITION_VALUE_USD", "1.0")),
            local_api_url=os.getenv("LOCAL_API_URL", "http://127.0.0.1:3001/info"),
            local_api_enabled=os.getenv("LOCAL_API_ENABLED", "true").lower() == "true",
            public_api_url=public_api_url,
            ws_url=ws_url,
            chain_type=chain_type,
            position_update_interval=int(os.getenv("POSITION_UPDATE_INTERVAL", "10")),
            position_batch_size=int(os.getenv("POSITION_BATCH_SIZE", "100")),
            max_workers=int(os.getenv("MAX_WORKERS", "10")),
            api_timeout=int(os.getenv("API_TIMEOUT", "10")),
            retry_delay=float(os.getenv("RETRY_DELAY", "1.0")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            public_api_max_concurrent=int(os.getenv("PUBLIC_API_MAX_CONCURRENT", "5")),
            public_api_min_interval=float(os.getenv("PUBLIC_API_MIN_INTERVAL", "0.2")),
            local_node_failure_threshold=int(os.getenv("LOCAL_NODE_FAILURE_THRESHOLD", "3")),
            node_health_check_interval=int(os.getenv("NODE_HEALTH_CHECK_INTERVAL", "60")),
            ws_reconnect_delay=float(os.getenv("WS_RECONNECT_DELAY", "1.0")),
            ws_max_reconnect_delay=float(os.getenv("WS_MAX_RECONNECT_DELAY", "30.0")),
            ws_ping_interval=int(os.getenv("WS_PING_INTERVAL", "20")),
            ws_ping_timeout=int(os.getenv("WS_PING_TIMEOUT", "10")),
            ws_max_message_size=int(os.getenv("WS_MAX_MESSAGE_SIZE", "10485760")),
            ws_max_trade_ids_per_market=int(os.getenv("WS_MAX_TRADE_IDS_PER_MARKET", "1000")),
            db_min_pool_size=int(os.getenv("DB_MIN_POOL_SIZE", "10")),
            db_max_pool_size=int(os.getenv("DB_MAX_POOL_SIZE", "20")),
            snapshot_retention_count=int(os.getenv("SNAPSHOT_RETENTION_COUNT", "5")),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID")
        )
