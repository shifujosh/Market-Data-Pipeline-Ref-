"""
Market Data Pipeline Demo
-------------------------
Simulates a high-frequency ingestion stream with various anomalies.
Demonstrates "Data Physics" in action:
1. Inertia (Context)
2. Velocity (Price checks)
3. Entropy (Error handling)
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import random
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion_engine import IngestionEngine, DataQuality

def generate_stream(count=20):
    """Generate a synthetic stream of tick data."""
    base_price = 150.00
    timestamp = datetime.now(timezone.utc)
    
    stream = []
    
    # 1. Normal ticks
    for i in range(5):
        price = base_price + random.uniform(-0.10, 0.10)
        stream.append({
            "symbol": "AAPL",
            "price": f"{price:.2f}",
            "volume": random.randint(100, 1000),
            "timestamp": timestamp.isoformat(),
            "sequence_id": i + 1
        })
        timestamp += timedelta(milliseconds=100)
        
    # 2. Flash Crash (Anomaly)
    stream.append({
        "symbol": "AAPL",
        "price": "120.00", # Huge drop
        "volume": 5000,
        "timestamp": timestamp.isoformat(),
        "sequence_id": 6
    })
    
    # 3. Bad Format (Hard Error)
    stream.append({
        "symbol": "AAPL",
        "price": "invalid",
        "timestamp": timestamp.isoformat()
    })
    
    # 4. Old Data (Staleness)
    stream.append({
        "symbol": "AAPL",
        "price": "150.05",
        "timestamp": (timestamp - timedelta(minutes=10)).isoformat(),
        "sequence_id": 8
    })
    
    return stream

def run_demo():
    print("Initializing Ingestion Engine...")
    engine = IngestionEngine()
    
    stream = generate_stream()
    print(f"Processing {len(stream)} synthetic messages...\n")
    
    print(f"{'SYM':<6} {'PRICE':<10} {'QUAL':<10} {'NOTES'}")
    print("-" * 60)
    
    for msg in stream:
        tick, quality, errors = engine.validate(msg)
        
        price_disp = msg.get("price", "N/A")
        if tick:
            price_disp = f"{tick.price:.2f}"
            
        note = ""
        if errors:
            note = f"{errors[0].rule_violated}: {errors[0].suggestion}"
            
        color = ""
        if quality == DataQuality.VERIFIED:
            color = "\033[32m" # Green
        elif quality == DataQuality.SUSPECT:
            color = "\033[33m" # Yellow
        elif quality == DataQuality.REJECTED:
            color = "\033[31m" # Red
            
        reset = "\033[0m"
        
        print(f"{color}{msg.get('symbol', 'ERR'):<6} {price_disp:<10} {quality.value:<10} {note}{reset}")

    print("\n" + "="*30)
    print("Pipeline Statistics")
    print("="*30)
    stats = engine.get_statistics()
    for k, v in stats.items():
        print(f"{k:<20}: {v}")

    print(f"\nDead Letter Queue Size: {len(engine.dead_letter_queue)}")

if __name__ == "__main__":
    run_demo()
