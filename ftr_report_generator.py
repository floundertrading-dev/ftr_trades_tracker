"""
FTR Daily Report Generator
==========================
Generates an Excel report with Position Summary and Activity for daily email.
Runs after ftr_daily_pull.py in GitHub Actions.
"""

import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
import warnings
import sys

warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = 'https://www.emi.ea.govt.nz/Wholesale/Download/DataReport/CSV/CLA3WR'
NODE_CODES = ['OTA2201', 'WKM2201', 'RDF2201', 'HAY2201', 'KIK2201', 'ISL2201', 'BEN2201', 'INV2201']

SNAPSHOT_DIR = Path('ftr_tracking/snapshots')
LEDGER_FILE = Path('ftr_tracking/ftr_master_ledger.csv')
SPOT_CACHE_DIR = Path('ftr_tracking/spot_cache')
REPORT_DIR = Path('ftr_tracking/reports')

# Create directories
SPOT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# SPOT PRICE FUNCTIONS
# =============================================================================

def fetch_spot_prices(date_from: str, date_to: str, nodes: list = None) -> pd.DataFrame:
    """Fetch spot prices from EMI for given date range."""
    if nodes is None:
        nodes = NODE_CODES
    
    all_data = []
    
    with requests.Session() as session:
        for i, node in enumerate(nodes, 1):
            params = {
                'DateFrom': date_from,
                'DateTo': date_to,
                'POC': node,
                '_rsdr': 'L4390D',
                '_si': 'v|3'
            }
            
            try:
                print(f"  Fetching {i}/{len(nodes)}: {node}...", end=' ')
                response = session.get(BASE_URL, params=params, timeout=60)
                
                if response.status_code == 200:
                    csv_data = StringIO(response.text)
                    df = pd.read_csv(csv_data, skiprows=9)
                    
                    if not df.empty:
                        all_data.append(df)
                        print(f"✓ {len(df):,} rows")
                    else:
                        print("✗ No data")
                else:
                    print(f"✗ HTTP {response.status_code}")
            except Exception as e:
                print(f"✗ Error: {e}")
    
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined['Trading date'] = pd.to_datetime(combined['Trading date'], dayfirst=True)
        return combined
    
    return pd.DataFrame()


def get_spot_data_with_cache(year: int, month: int) -> pd.DataFrame:
    """Get spot data for a month, using cache when available."""
    year_month = f"{year}{month:02d}"
    cache_file = SPOT_CACHE_DIR / f"spot_{year_month}.csv"
    
    today = datetime.now().date()
    is_current_month = (year == today.year and month == today.month)
    
    # Date range
    first_day = datetime(year, month, 1).date()
    if is_current_month:
        last_day = today - timedelta(days=1)  # Yesterday (data lag)
    else:
        if month == 12:
            last_day = datetime(year + 1, 1, 1).date() - timedelta(days=1)
        else:
            last_day = datetime(year, month + 1, 1).date() - timedelta(days=1)
    
    # Check cache
    if cache_file.exists():
        cached = pd.read_csv(cache_file)
        cached['Trading date'] = pd.to_datetime(cached['Trading date'])
        cached_max = cached['Trading date'].max().date()
        
        if cached_max >= last_day:
            print(f"  ✓ Using cached data for {year_month} (up to {cached_max})")
            return cached
        else:
            # Fetch missing days
            print(f"  Updating cache: {cached_max} → {last_day}")
            fetch_from = (cached_max + timedelta(days=1)).strftime('%Y%m%d')
            fetch_to = last_day.strftime('%Y%m%d')
            
            new_data = fetch_spot_prices(fetch_from, fetch_to)
            
            if not new_data.empty:
                combined = pd.concat([cached, new_data], ignore_index=True)
                combined = combined.drop_duplicates(subset=['Trading date', 'Trading period', 'Point of connection'])
                combined.to_csv(cache_file, index=False)
                return combined
            return cached
    
    # No cache - fetch full range
    print(f"  Fetching spot data for {year_month}...")
    df = fetch_spot_prices(
        first_day.strftime('%Y%m%d'),
        last_day.strftime('%Y%m%d')
    )
    
    if not df.empty:
        df.to_csv(cache_file, index=False)
        print(f"  ✓ Cached to {cache_file.name}")
    
    return df


# =============================================================================
# CALCULATION FUNCTIONS
# =============================================================================

def calculate_price_differences(spot_df: pd.DataFrame) -> dict:
    """Calculate price differences for all route combinations."""
    if spot_df.empty:
        return {'half_hourly': pd.DataFrame(), 'daily': pd.DataFrame()}
    
    spot_df = spot_df.copy()
    spot_df['Node'] = spot_df['Point of connection'].str.replace('2201', '')
    
    # Create half-hourly price differences
    spot_from = spot_df[['Trading date', 'Trading period', 'Node', '$/MWh']].copy()
    spot_from.columns = ['Trading date', 'Trading period', 'Source', 'Price_Source']
    
    spot_to = spot_df[['Trading date', 'Trading period', 'Node', '$/MWh']].copy()
    spot_to.columns = ['Trading date', 'Trading period', 'Sink', 'Price_Sink']
    
    # Cross join for all combinations
    half_hourly = spot_from.merge(spot_to, on=['Trading date', 'Trading period'])
    half_hourly = half_hourly[half_hourly['Source'] != half_hourly['Sink']]
    
    # Calculate differences
    half_hourly['Price_Diff'] = half_hourly['Price_Sink'] - half_hourly['Price_Source']
    half_hourly['Price_Diff_Option'] = half_hourly['Price_Diff'].clip(lower=0)
    
    # Daily averages
    daily = half_hourly.groupby(['Trading date', 'Source', 'Sink']).agg({
        'Price_Source': 'mean',
        'Price_Sink': 'mean',
        'Price_Diff': 'mean',
        'Price_Diff_Option': 'mean'
    }).reset_index()
    
    return {'half_hourly': half_hourly, 'daily': daily}


def calculate_settlement(positions_df: pd.DataFrame, price_diffs: dict) -> pd.DataFrame:
    """Calculate daily settlement and P&L for each position."""
    if positions_df.empty or price_diffs['daily'].empty:
        return pd.DataFrame()
    
    results = []
    daily_diffs = price_diffs['daily'].copy()
    
    for _, pos in positions_df.iterrows():
        ftr_id = pos['FTR_ID']
        source = pos['Source']
        sink = pos['Sink']
        hedge_type = pos['HedgeType']
        mw = float(pos.get('MW', 0) or 0)
        price_paid = float(pos.get('Price', 0) or 0)
        start_date = pos['StartDate']
        end_date = pos['EndDate']
        current_owner = pos.get('CurrentOwner', '')
        
        # Get daily price differences for this route
        route_diffs = daily_diffs[
            (daily_diffs['Source'] == source) & 
            (daily_diffs['Sink'] == sink) &
            (daily_diffs['Trading date'] >= start_date) &
            (daily_diffs['Trading date'] <= end_date)
        ].copy()
        
        if route_diffs.empty:
            continue
        
        # Use appropriate price diff column
        price_col = 'Price_Diff_Option' if hedge_type == 'OPT' else 'Price_Diff'
        
        # Calculate daily values
        route_diffs['Daily_Settlement'] = route_diffs[price_col] * mw * 24
        route_diffs['Daily_Cost'] = price_paid * mw * 24
        route_diffs['Daily_PnL'] = route_diffs['Daily_Settlement'] - route_diffs['Daily_Cost']
        route_diffs['Cumulative_PnL'] = route_diffs['Daily_PnL'].cumsum()
        
        # Add position info
        route_diffs['FTR_ID'] = ftr_id
        route_diffs['Route'] = f"{source} → {sink}"
        route_diffs['HedgeType'] = hedge_type
        route_diffs['MW'] = mw
        route_diffs['Price_Paid'] = price_paid
        route_diffs['CurrentOwner'] = current_owner
        
        results.append(route_diffs)
    
    if results:
        return pd.concat(results, ignore_index=True)
    
    return pd.DataFrame()


# =============================================================================
# MAIN REPORT GENERATION
# =============================================================================

def generate_report():
    """Generate the daily FTR report."""
    today = datetime.now()
    print(f"\n{'='*60}")
    print(f"FTR DAILY REPORT GENERATOR")
    print(f"{'='*60}")
    print(f"Report Date: {today.strftime('%Y-%m-%d %H:%M')}")
    
    # 1. Load latest snapshot
    print(f"\n1. Loading latest snapshot...")
    snapshots = sorted(SNAPSHOT_DIR.glob("ftr_snapshot_*.csv"), reverse=True)
    if not snapshots:
        print("   ❌ No snapshots found!")
        sys.exit(1)
    
    latest_snapshot = snapshots[0]
    positions = pd.read_csv(latest_snapshot)
    
    for col in ['StartDate', 'EndDate']:
        if col in positions.columns:
            positions[col] = pd.to_datetime(positions[col], dayfirst=True, errors='coerce')
    
    print(f"   ✓ Loaded: {latest_snapshot.name} ({len(positions):,} positions)")
    
    # Filter current month positions
    month_start = datetime(today.year, today.month, 1)
    if today.month == 12:
        month_end = datetime(today.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = datetime(today.year, today.month + 1, 1) - timedelta(days=1)
    
    current_month = positions[
        (positions['StartDate'] <= month_end) & 
        (positions['EndDate'] >= month_start)
    ].copy()
    
    print(f"   Current month positions: {len(current_month):,}")
    
    # 2. Fetch spot prices
    print(f"\n2. Fetching spot prices...")
    spot_data = get_spot_data_with_cache(today.year, today.month)
    
    if spot_data.empty:
        print("   ⚠ No spot data available - skipping P&L calculations")
        settlement = pd.DataFrame()
    else:
        print(f"   ✓ Spot data: {len(spot_data):,} rows")
        
        # 3. Calculate P&L
        print(f"\n3. Calculating P&L...")
        price_diffs = calculate_price_differences(spot_data)
        settlement = calculate_settlement(current_month, price_diffs)
        
        if not settlement.empty:
            print(f"   ✓ Calculated for {settlement['FTR_ID'].nunique():,} positions")
    
    # 4. Load activity ledger
    print(f"\n4. Loading activity ledger...")
    if LEDGER_FILE.exists():
        ledger = pd.read_csv(LEDGER_FILE)
        ledger['SnapshotDate'] = pd.to_datetime(ledger['SnapshotDate'])
        print(f"   ✓ Loaded: {len(ledger):,} records")
    else:
        ledger = pd.DataFrame()
        print("   ⚠ No ledger file found")
    
    # 5. Build Position Summary
    print(f"\n5. Building Position Summary...")
    if not settlement.empty:
        # Get latest day's P&L for each position
        latest_date = settlement['Trading date'].max()
        latest_day = settlement[settlement['Trading date'] == latest_date]
        latest_day_pnl = latest_day.groupby('FTR_ID')['Daily_PnL'].sum().reset_index()
        latest_day_pnl.columns = ['FTR_ID', 'Latest_Day_PnL']
        
        # Aggregate by position
        pos_summary = settlement.groupby(['FTR_ID', 'Route', 'HedgeType', 'MW', 'Price_Paid', 'CurrentOwner']).agg({
            'Daily_Settlement': 'sum',
            'Daily_Cost': 'sum',
            'Daily_PnL': 'sum',
            'Trading date': 'count'
        }).reset_index()
        
        pos_summary.columns = ['FTR_ID', 'Route', 'HedgeType', 'MW', 'Price_Paid', 'Owner',
                               'Total_Settlement', 'Total_Cost', 'MTD_PnL', 'Days']
        
        # Merge latest day P&L
        pos_summary = pos_summary.merge(latest_day_pnl, on='FTR_ID', how='left')
        pos_summary['PnL_Per_MW'] = pos_summary['MTD_PnL'] / pos_summary['MW']
        pos_summary = pos_summary.sort_values('MTD_PnL', ascending=False)
        
        # Round numeric columns
        for col in ['Total_Settlement', 'Total_Cost', 'MTD_PnL', 'Latest_Day_PnL', 'PnL_Per_MW']:
            if col in pos_summary.columns:
                pos_summary[col] = pos_summary[col].round(2)
        
        print(f"   ✓ {len(pos_summary):,} positions summarized")
        print(f"   Latest date: {latest_date.strftime('%Y-%m-%d')}")
        print(f"   Total MTD P&L: ${pos_summary['MTD_PnL'].sum():,.2f}")
    else:
        pos_summary = pd.DataFrame()
        print("   ⚠ No position summary (no spot data)")
    
    # 6. Build Activity Summary
    print(f"\n6. Building Activity Summary...")
    if not ledger.empty:
        # Filter to meaningful transactions (exclude INITIAL)
        activity = ledger[ledger['TransactionType'] != 'INITIAL'].copy()
        activity = activity.sort_values('SnapshotDate', ascending=False)
        
        # Format date
        activity['SnapshotDate'] = activity['SnapshotDate'].dt.strftime('%Y-%m-%d')
        
        # Select relevant columns
        activity_cols = ['SnapshotDate', 'TransactionType', 'FTR_ID', 'Source', 'Sink', 
                        'HedgeType', 'MW_Previous', 'MW_Current', 'MW_Sold', 
                        'SaleProceeds', 'Profit', 'PricePerMW', 'CurrentOwner']
        activity_cols = [c for c in activity_cols if c in activity.columns]
        activity = activity[activity_cols]
        
        print(f"   ✓ {len(activity):,} transactions")
    else:
        activity = pd.DataFrame()
        print("   ⚠ No activity data")
    
    # 7. Generate Excel Report
    print(f"\n7. Generating Excel report...")
    report_filename = f"FTR_Daily_Report_{today.strftime('%Y%m%d')}.xlsx"
    report_path = REPORT_DIR / report_filename
    
    with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
        # Position Summary sheet
        if not pos_summary.empty:
            pos_summary.to_excel(writer, sheet_name='Position_Summary', index=False)
            print(f"   ✓ Position_Summary: {len(pos_summary)} rows")
        
        # Activity sheet
        if not activity.empty:
            activity.to_excel(writer, sheet_name='Activity', index=False)
            print(f"   ✓ Activity: {len(activity)} rows")
        
        # Owner Summary sheet
        if not settlement.empty:
            owner_summary = settlement.groupby('CurrentOwner').agg({
                'Daily_Settlement': 'sum',
                'Daily_Cost': 'sum',
                'Daily_PnL': 'sum',
                'FTR_ID': 'nunique'
            }).reset_index()
            owner_summary.columns = ['Owner', 'Total_Settlement', 'Total_Cost', 'MTD_PnL', 'Num_FTRs']
            owner_summary = owner_summary.round(2).sort_values('MTD_PnL', ascending=False)
            owner_summary.to_excel(writer, sheet_name='Owner_Summary', index=False)
            print(f"   ✓ Owner_Summary: {len(owner_summary)} rows")
    
    print(f"\n   📄 Report saved: {report_path}")
    
    # 8. Generate email body summary
    print(f"\n8. Generating email summary...")
    
    summary_lines = [
        f"FTR Daily Report - {today.strftime('%B %d, %Y')}",
        "=" * 40,
        ""
    ]
    
    if not pos_summary.empty:
        total_mtd_pnl = pos_summary['MTD_PnL'].sum()
        total_latest_day = pos_summary['Latest_Day_PnL'].sum() if 'Latest_Day_PnL' in pos_summary.columns else 0
        
        summary_lines.extend([
            "📊 POSITION PERFORMANCE",
            f"   Total Positions: {len(pos_summary):,}",
            f"   Latest Day P&L: ${total_latest_day:,.2f}",
            f"   MTD P&L: ${total_mtd_pnl:,.2f}",
            ""
        ])
        
        # Top 3 / Bottom 3
        if len(pos_summary) >= 3:
            summary_lines.append("   🟢 Top 3 Performers:")
            for _, row in pos_summary.head(3).iterrows():
                summary_lines.append(f"      {row['Route']} ({row['HedgeType']}): ${row['MTD_PnL']:,.0f}")
            
            summary_lines.append("   🔴 Bottom 3 Performers:")
            for _, row in pos_summary.tail(3).iterrows():
                summary_lines.append(f"      {row['Route']} ({row['HedgeType']}): ${row['MTD_PnL']:,.0f}")
            summary_lines.append("")
    
    if not activity.empty:
        today_str = today.strftime('%Y-%m-%d')
        today_activity = activity[activity['SnapshotDate'] == today_str]
        
        summary_lines.extend([
            "🔔 RECENT ACTIVITY",
            f"   Total transactions: {len(activity):,}",
            f"   Today's transactions: {len(today_activity):,}",
        ])
        
        # Count by type
        for tx_type in ['SELL', 'BUY', 'ASSIGNED', 'NEW', 'CLOSED']:
            count = len(activity[activity['TransactionType'] == tx_type])
            if count > 0:
                summary_lines.append(f"   - {tx_type}: {count}")
        summary_lines.append("")
    
    summary_lines.append(f"📎 Full report attached: {report_filename}")
    
    email_body = "\n".join(summary_lines)
    print(email_body)
    
    # Save summary for GitHub Actions
    summary_file = REPORT_DIR / "email_summary.txt"
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(email_body)
    
    print(f"\n{'='*60}")
    print("✓ REPORT GENERATION COMPLETE")
    print(f"{'='*60}")
    
    return str(report_path)


if __name__ == "__main__":
    report_path = generate_report()
    
    # Output the report path for GitHub Actions
    print(f"\n::set-output name=report_path::{report_path}")
