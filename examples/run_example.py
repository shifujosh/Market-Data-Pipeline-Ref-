#!/usr/bin/env python3
"""
Example: Running the Data Pipeline

This script demonstrates the ingestion and validation pipeline
using sample tick data. It shows how Data Physics principles
catch bad data before it enters the system.

Usage:
    python examples/run_example.py
"""

import csv
import sys
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import dataclass
from typing import List, Tuple

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


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


def validate_and_parse(row: dict) -> TickData:
    """
    Validate and parse a single row of tick data.
    
    Applies Data Physics principles:
    1. Schema enforcement (type coercion)
    2. Semantic validation (realistic bounds)
    3. Temporal validation (no future data)
    """
    # Schema enforcement
    symbol = row["symbol"].strip().upper()
    if not symbol:
        raise ValidationError("Empty symbol")
    
    price = Decimal(row["price"])
    if price <= 0 or price > 1_000_000:
        raise ValidationError(f"Price out of bounds: {price}")
    
    volume = int(row["volume"])
    if volume < 0:
        raise ValidationError(f"Negative volume: {volume}")
    
    timestamp = datetime.fromisoformat(row["timestamp"])
    if timestamp > datetime.now(timezone.utc):
        raise ValidationError(f"Future timestamp: {timestamp}")
    
    return TickData(
        symbol=symbol,
        price=price,
        volume=volume,
        timestamp=timestamp,
        exchange=row.get("exchange", "UNKNOWN"),
    )


def process_file(filepath: Path) -> Tuple[List[TickData], List[dict]]:
    """
    Process a CSV file, returning valid ticks and rejected rows.
    
    This demonstrates the "Fail Loudly" principle:
    - Valid data passes through
    - Invalid data goes to a dead letter queue (rejected list)
    - Nothing is silently dropped
    """
    valid: List[TickData] = []
    rejected: List[dict] = []
    
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                tick = validate_and_parse(row)
                valid.append(tick)
            except (ValidationError, ValueError, KeyError) as e:
                rejected.append({"row": row, "error": str(e)})
    
    return valid, rejected


def main():
    """Run the example pipeline."""
    print("=" * 60)
    print("Data Pipeline Example: Ingesting Sample Tick Data")
    print("=" * 60)
    print()
    
    # Find sample data
    sample_file = Path(__file__).parent / "sample_tick_data.csv"
    if not sample_file.exists():
        print(f"Error: Sample file not found: {sample_file}")
        sys.exit(1)
    
    print(f"Input file: {sample_file}")
    print()
    
    # Process the file
    valid, rejected = process_file(sample_file)
    
    # Report results
    print(f"Validation Results:")
    print(f"  - Valid ticks:    {len(valid)}")
    print(f"  - Rejected rows:  {len(rejected)}")
    print()
    
    if valid:
        print("Sample Valid Ticks:")
        print("-" * 60)
        for tick in valid[:5]:
            print(f"  {tick.symbol:6} | ${tick.price:>10.2f} | Vol: {tick.volume:>8,}")
        if len(valid) > 5:
            print(f"  ... and {len(valid) - 5} more")
    
    if rejected:
        print()
        print("Rejected Rows (Dead Letter Queue):")
        print("-" * 60)
        for item in rejected:
            print(f"  Error: {item['error']}")
            print(f"  Row:   {item['row']}")
    
    print()
    print("=" * 60)
    print("Pipeline complete. All data accounted for.")
    print("=" * 60)


if __name__ == "__main__":
    main()
