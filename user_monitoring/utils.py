"""Utility functions for user monitoring."""
import re
import time
import requests
from typing import Optional, Any
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


def is_valid_address(address: str) -> bool:
    """Validate Ethereum address format."""
    if not isinstance(address, str):
        return False
    if len(address) != 42 or not address.startswith('0x'):
        return False
    try:
        int(address, 16)
        return True
    except ValueError:
        return False


def is_ethereum_address(address: str) -> bool:
    """Check if address is valid Ethereum address (alias for is_valid_address)."""
    return is_valid_address(address)


def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert value to float."""
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value == 'null':
                return default
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_decimal(value: Any, default: Decimal = Decimal('0')) -> Decimal:
    """Safely convert value to Decimal."""
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == '' or value == 'null':
                return default
        return Decimal(str(value))
    except (ValueError, TypeError):
        return default


def sanitize_market_name(market: str) -> str:
    """Sanitize market name for database table naming."""
    if ':' in market:
        dex, coin = market.split(':', 1)
        sanitized_dex = re.sub(r'[^a-zA-Z0-9_]', '', dex.lower())
        sanitized_coin = re.sub(r'[^a-zA-Z0-9_]', '', coin.lower())
        return f"{sanitized_dex}_{sanitized_coin}"
    return re.sub(r'[^a-zA-Z0-9_]', '', market.lower())


def format_position_value(value: float) -> str:
    """Format position value for display."""
    if abs(value) >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif abs(value) >= 1_000:
        return f"${value/1_000:.2f}K"
    else:
        return f"${value:.2f}"


def calculate_health_score(
    margin_used: float,
    account_value: float,
    liquidation_price: float,
    current_price: float
) -> float:
    """Calculate position health score (0-100)."""
    if account_value <= 0 or margin_used <= 0:
        return 0.0
        
    margin_ratio = margin_used / account_value
    
    if liquidation_price > 0 and current_price > 0:
        distance_to_liq = abs(current_price - liquidation_price) / current_price
        health = min(100, distance_to_liq * 100 * (1 - margin_ratio))
    else:
        health = max(0, (1 - margin_ratio) * 100)
        
    return round(health, 2)


def send_telegram_alert(message: str, bot_token: Optional[str] = None, chat_id: Optional[str] = None) -> None:
    """Send Telegram alert for monitoring events. Never raises exceptions."""
    if not bot_token or not chat_id:
        # Silent if credentials missing, just log locally
        logger.debug(f"[{time.strftime('%H:%M:%S')}] Telegram not configured: {message}")
        return
    
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": f"*User Monitoring Alert*\n{message[:3900]}",  # Telegram limit guard
            "parse_mode": "Markdown"
        }
        response = requests.post(url, data=data, timeout=5)
        if not response.ok:
            logger.debug(f"Telegram send failed with status {response.status_code}")
    except Exception as e:
        logger.debug(f"Telegram alert failed: {e}")
