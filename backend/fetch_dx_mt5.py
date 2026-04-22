#!/usr/bin/env python3
"""
Fetch DX.f price data from BlackBull MT5 and update price_cache.json
"""

import sys
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

try:
    import MetaTrader5 as mt5  # type: ignore
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    print("MetaTrader5 not installed")
    sys.exit(1)

def fetch_dx_from_mt5():
    if not MT5_AVAILABLE:
        print("MT5 not available")
        return None

    # Initialize MT5 with BlackBull path
    mt5_path = r"C:\Program Files\BlackBull Markets MT5\terminal64.exe"
    if not mt5.initialize(mt5_path):
        print("MT5 initialize failed with BlackBull path, trying default")
        if not mt5.initialize():
            print("MT5 initialize failed")
            return None

    try:
        # Make symbol visible
        if not mt5.symbol_select("DX.f", True):
            print("Failed to select DX.f")
            return None

        # Get data for last 2 years
        end_date = datetime.now()
        start_date = end_date - timedelta(days=730)

        rates = mt5.copy_rates_range("DX.f", mt5.TIMEFRAME_W1, start_date, end_date)
        if rates is None or len(rates) == 0:
            print("No data for DX.f")
            return None

        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df.set_index('time', inplace=True)
        df = df[['close']]
        df.columns = ['Close']

        # Convert to list of dicts
        series = []
        for date, row in df.iterrows():
            series.append({
                "date": date.strftime("%Y-%m-%d"),
                "close": round(float(row['Close']), 4)
            })

        print(f"Fetched {len(series)} weeks of DX.f data")
        return series

    finally:
        mt5.shutdown()

def update_price_cache(series):
    # NOTE: Cache file lives in COT/data (same as backend service).
    # This script is located in COT/backend, so we go up one level to COT/.
    cache_path = Path(__file__).resolve().parent.parent / "data" / "price_cache.json"

    if not cache_path.exists():
        cache = {"markets": {}}
    else:
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache = json.load(f)

    # Update DXY entry
    cache["markets"]["usd-index-ice-futures-u-s"] = {
        "ticker": "DXY=F",
        "updated_at": datetime.now().isoformat(),
        "series": series,
        "_no_data_flag": len(series) == 0
    }

    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

    print(f"Updated price_cache.json with {len(series)} data points")

if __name__ == "__main__":
    print("Fetching DX.f from BlackBull MT5...")
    series = fetch_dx_from_mt5()
    if series:
        update_price_cache(series)
        print("Success!")
    else:
        print("Failed to fetch data")