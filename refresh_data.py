#!/usr/bin/env python3
"""
Refresh COT data by forcing re-fetch from CFTC and clearing stale caches.
"""
import sys
import time
import json
from pathlib import Path

# Add backend to path
backend_path = Path(__file__).parent / "backend"
sys.path.insert(0, str(backend_path))

from cot_service import refresh_cot_cache, refresh_catalog_cache

def main():
    print("🔄 Refreshing COT data from CFTC...")
    print("-" * 60)
    
    try:
        # Force refresh COT cache from CFTC
        print("📥 Fetching latest COT data from CFTC...")
        cot_payload = refresh_cot_cache(force=True)
        num_records = len(cot_payload.get("records", []))
        print(f"✅ COT data refreshed: {num_records} records")
        
        # Refresh catalog which depends on COT data
        print("📋 Refreshing catalog...")
        catalog = refresh_catalog_cache(force=True)
        num_categories = len(catalog.get("categories", []))
        print(f"✅ Catalog refreshed: {num_categories} categories")
        
        print("-" * 60)
        print("✨ Data synchronization complete!")
        print(f"Updated at: {cot_payload.get('updated_at')}")
        
        return 0
    except Exception as e:
        print(f"❌ Error during refresh: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
