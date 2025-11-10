"""Constants and enums for user monitoring."""
from enum import Enum


class ProcessingStatus(Enum):
    """Status of snapshot processing."""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"


class FileConfig:
    """File processing configuration."""
    MAX_SNAPSHOT_CACHE_SIZE = 100
    HASH_BLOCK_SIZE = 8192


class MonitoringThresholds:
    """Monitoring thresholds."""
    MAX_TASK_ERRORS = 10


# System addresses to filter out
SYSTEM_ADDRESSES = {
    '0x0000000000000000000000000000000000000000',
    '0x0000000000000000000000000000000000000001',
    '0x000000000000000000000000000000000000dead',
    '0xffffffffffffffffffffffffffffffffffffffff'
}
