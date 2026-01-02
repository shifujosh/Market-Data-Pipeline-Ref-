"""
Unit tests for the ingestion engine validation logic.

These tests demonstrate the "Data Physics" principles:
- Schema enforcement
- Semantic validation  
- Temporal validation

Run with: pytest tests/test_ingestion.py -v
"""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional


# --- Schema Definition (mirrors ingestion_engine.py) ---

@dataclass
class TickData:
    """Validated market tick data."""
    symbol: str
    price: Decimal
    volume: int
    timestamp: datetime
    exchange: str


class ValidationError(Exception):
    """Raised when data fails validation."""
    pass


# --- Validation Functions ---

def validate_symbol(symbol: str) -> str:
    """Validate ticker symbol format."""
    if not symbol or not symbol.isupper():
        raise ValidationError(f"Invalid symbol: {symbol}")
    if len(symbol) > 10:
        raise ValidationError(f"Symbol too long: {symbol}")
    return symbol


def validate_price(price: Decimal) -> Decimal:
    """Validate price is within realistic bounds."""
    if price <= 0:
        raise ValidationError(f"Price must be positive: {price}")
    if price > Decimal("1000000"):
        raise ValidationError(f"Price exceeds maximum: {price}")
    return price


def validate_volume(volume: int) -> int:
    """Validate volume is non-negative."""
    if volume < 0:
        raise ValidationError(f"Volume cannot be negative: {volume}")
    return volume


def validate_timestamp(ts: datetime) -> datetime:
    """Validate timestamp is not in the future."""
    now = datetime.now(timezone.utc)
    if ts > now:
        raise ValidationError(f"Timestamp is in the future: {ts}")
    return ts


def parse_tick(raw: dict) -> TickData:
    """Parse and validate raw tick data."""
    return TickData(
        symbol=validate_symbol(raw.get("symbol", "")),
        price=validate_price(Decimal(str(raw.get("price", 0)))),
        volume=validate_volume(int(raw.get("volume", 0))),
        timestamp=validate_timestamp(
            datetime.fromisoformat(raw.get("timestamp", ""))
        ),
        exchange=raw.get("exchange", "UNKNOWN"),
    )


# --- Tests ---

class TestSymbolValidation:
    """Tests for ticker symbol validation."""

    def test_valid_symbol(self):
        assert validate_symbol("AAPL") == "AAPL"
        assert validate_symbol("MSFT") == "MSFT"
        assert validate_symbol("BRK.A") == "BRK.A"

    def test_lowercase_symbol_rejected(self):
        with pytest.raises(ValidationError):
            validate_symbol("aapl")

    def test_empty_symbol_rejected(self):
        with pytest.raises(ValidationError):
            validate_symbol("")

    def test_long_symbol_rejected(self):
        with pytest.raises(ValidationError):
            validate_symbol("VERYLONGSYMBOL")


class TestPriceValidation:
    """Tests for price validation."""

    def test_valid_price(self):
        assert validate_price(Decimal("150.25")) == Decimal("150.25")

    def test_zero_price_rejected(self):
        with pytest.raises(ValidationError):
            validate_price(Decimal("0"))

    def test_negative_price_rejected(self):
        with pytest.raises(ValidationError):
            validate_price(Decimal("-10.00"))

    def test_extreme_price_rejected(self):
        with pytest.raises(ValidationError):
            validate_price(Decimal("999999999"))


class TestVolumeValidation:
    """Tests for volume validation."""

    def test_valid_volume(self):
        assert validate_volume(1000) == 1000
        assert validate_volume(0) == 0

    def test_negative_volume_rejected(self):
        with pytest.raises(ValidationError):
            validate_volume(-100)


class TestTimestampValidation:
    """Tests for temporal validation."""

    def test_valid_timestamp(self):
        past = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert validate_timestamp(past) == past

    def test_future_timestamp_rejected(self):
        future = datetime(2030, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValidationError):
            validate_timestamp(future)


class TestTickParsing:
    """Integration tests for full tick parsing."""

    def test_valid_tick(self):
        raw = {
            "symbol": "AAPL",
            "price": "150.25",
            "volume": 1000,
            "timestamp": "2025-12-01T10:30:00+00:00",
            "exchange": "NASDAQ",
        }
        tick = parse_tick(raw)
        assert tick.symbol == "AAPL"
        assert tick.price == Decimal("150.25")
        assert tick.volume == 1000
        assert tick.exchange == "NASDAQ"

    def test_invalid_tick_raises(self):
        raw = {
            "symbol": "invalid",
            "price": "-10",
            "volume": -1,
            "timestamp": "2030-01-01T00:00:00+00:00",
        }
        with pytest.raises(ValidationError):
            parse_tick(raw)
