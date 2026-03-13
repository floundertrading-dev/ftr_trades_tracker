"""
Download spot price data for a specific month
"""

import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# Configuration
BASE_URL = 'https://www.emi.ea.govt.nz/Wholesale/Download/DataReport/CSV/CLA3WR'
NODE_CODES = ['OTA2201', 'WKM2201', 'RDF2201', 'HAY2201', 'KIK2201', 'ISL2201', 'BEN2201', 'INV2201']
SPOT_CACHE_DIR = Path(__file__).parent / "ftr_tracking" / "spot_cache"
SPOT_REQUEST_TIMEOUT_SECONDS = int(os.environ.get("SPOT_REQUEST_TIMEOUT_SECONDS", "120"))

SPOT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def download_spot_prices(year_month):
    """
    Download spot prices for a specific month (YYYYMM format)
    """
    # Parse year and month
    year = int(year_month[:4])
    month = int(year_month[4:6])
    
    # Calculate date range
    start_date = datetime(year, month, 1)
    
    # Last day of the month
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    
    print(f"Downloading spot prices for {year_month}")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Nodes: {', '.join([n.replace('2201', '') for n in NODE_CODES])}")
    print(f"Request timeout: {SPOT_REQUEST_TIMEOUT_SECONDS}s")
    print("-" * 60)
    
    # Download spot price data for each node
    data_frames = []
    
    with requests.Session() as session:
        for i, node_code in enumerate(NODE_CODES, 1):
            params = {
                'DateFrom': start_date_str,
                'DateTo': end_date_str,
                'POC': node_code,
                '_si': 'v|3'
            }
            
            try:
                print(f"[{i}/{len(NODE_CODES)}] Downloading {node_code}...", end=' ')
                response = session.get(BASE_URL, params=params, timeout=SPOT_REQUEST_TIMEOUT_SECONDS)
                
                if response.status_code == 200:
                    csv_file_in_memory = StringIO(response.text)
                    node_df = pd.read_csv(csv_file_in_memory, skiprows=9)
                    
                    if not node_df.empty:
                        data_frames.append(node_df)
                        print(f"✓ {len(node_df):,} rows")
                    else:
                        print("✗ No data returned")
                else:
                    print(f"✗ HTTP {response.status_code}")
            
            except requests.RequestException as e:
                print(f"✗ Error: {e}")
            except Exception as e:
                print(f"✗ Unexpected error: {e}")
    
    if not data_frames:
        print("\n❌ No data downloaded")
        return False
    
    # Merge all nodes
    print(f"\n✓ Downloaded {len(data_frames)}/{len(NODE_CODES)} nodes successfully")
    spot_data = pd.concat(data_frames, ignore_index=True)
    
    # Save to cache
    output_file = SPOT_CACHE_DIR / f"spot_{year_month}.csv"
    spot_data.to_csv(output_file, index=False)
    
    print(f"\n✓ Saved to: {output_file}")
    print(f"  Total rows: {len(spot_data):,}")
    
    # Show summary
    spot_data['Trading date'] = pd.to_datetime(spot_data['Trading date'], dayfirst=True)
    print(f"  Date range: {spot_data['Trading date'].min().date()} to {spot_data['Trading date'].max().date()}")
    print(f"  Nodes: {spot_data['Point of connection'].nunique()}")
    print(f"  Trading periods: {spot_data['Trading period'].nunique()}")
    
    return True


if __name__ == "__main__":
    # Get month from command line or use current month
    if len(sys.argv) > 1:
        year_month = sys.argv[1]
    else:
        year_month = datetime.now().strftime('%Y%m')
    
    # Validate format
    if len(year_month) != 6 or not year_month.isdigit():
        print("Usage: python download_spot_prices.py YYYYMM")
        print("Example: python download_spot_prices.py 202602")
        sys.exit(1)
    
    success = download_spot_prices(year_month)
    sys.exit(0 if success else 1)
