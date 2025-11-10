#!/usr/bin/env python3
"""Entry point for user monitoring service."""
import asyncio
import sys
import logging
from pathlib import Path

from config import Config
from coordinator import UserMonitor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def main():
    try:
        config = Config.from_env()
        monitor = UserMonitor(config)
        await monitor.start()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        return 1
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
