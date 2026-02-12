"""
Generate daily settlement price report for a specific owner
Usage: python generate_owner_settlement_report.py OWNER YYYYMM
Example: python generate_owner_settlement_report.py FLTR 202602
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# Setup paths
BASE_DIR = Path(__file__).parent / "ftr_tracking"
SNAPSHOT_DIR = BASE_DIR / "snapshots"
SPOT_CACHE_DIR = BASE_DIR / "spot_cache"
REPORTS_DIR = BASE_DIR / "reports"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def get_owner_positions(owner_code, snapshot_file):
    """Get all positions for a specific owner from snapshot."""
    df = pd.read_csv(snapshot_file)
    owner_positions = df[df['CurrentOwner'] == owner_code].copy()
    return owner_positions


def load_spot_prices_for_month(year_month):
    """Load all spot prices for the specified month."""
    spot_file = SPOT_CACHE_DIR / f"spot_{year_month}.csv"
    
    if not spot_file.exists():
        logger.error(f"Spot price file not found: {spot_file}")
        return None
    
    spot_df = pd.read_csv(spot_file)
    spot_df['Trading date'] = pd.to_datetime(spot_df['Trading date'], dayfirst=True)
    
    # Filter to the specified month
    year = int(year_month[:4])
    month = int(year_month[4:6])
    
    month_start = datetime(year, month, 1)
    if month == 12:
        month_end = datetime(year + 1, 1, 1)
    else:
        month_end = datetime(year, month + 1, 1)
    
    spot_df = spot_df[(spot_df['Trading date'] >= month_start) & 
                      (spot_df['Trading date'] < month_end)].copy()
    
    logger.info(f"Loaded spot prices for {year_month}: {len(spot_df)} records")
    return spot_df


def calculate_daily_settlements(positions_df, spot_df, year_month):
    """
    Calculate daily settlement prices for each position.
    
    Uses the same logic as AllData notebook:
    - Options: max(0, sink - source) at each half-hourly trading period, then average
    - Obligations: (sink - source) simple average across trading periods
    - Daily profit: (settlement_price - price_paid) * MW * num_trading_periods * 0.5
      (each trading period is half an hour = 0.5 MWh per MW)
    """
    
    # Get unique trading dates in the month
    trading_dates = sorted(spot_df['Trading date'].unique())
    
    results = []
    
    for _, position in positions_df.iterrows():
        ftr_id = position['FTR_ID']
        source = position['Source']
        sink = position['Sink']
        hedge_type = position['HedgeType']
        mw = float(position['MW'] or 0)
        price_paid = float(position['Price'] or 0)
        
        source_node = f"{source}2201"
        sink_node = f"{sink}2201"
        
        # Create FTR product name
        ftr_product = f"24HR-{hedge_type}-{source}->{sink}"
        
        mtd_profit = 0
        
        for trade_date in trading_dates:
            # Get half-hourly spot prices for this date
            day_spot = spot_df[spot_df['Trading date'] == trade_date].copy()
            
            if day_spot.empty:
                continue
            
            # Get source and sink prices per trading period
            source_tp = day_spot[day_spot['Point of connection'] == source_node][['Trading period', '$/MWh']].copy()
            sink_tp = day_spot[day_spot['Point of connection'] == sink_node][['Trading period', '$/MWh']].copy()
            
            if source_tp.empty or sink_tp.empty:
                continue
            
            source_tp.columns = ['Trading period', 'Source_Price']
            sink_tp.columns = ['Trading period', 'Sink_Price']
            
            # Join on trading period to get matched half-hourly pairs
            tp_merged = source_tp.merge(sink_tp, on='Trading period')
            
            if tp_merged.empty:
                continue
            
            num_trading_periods = len(tp_merged)
            
            # Calculate price difference at each trading period
            tp_merged['Price_Diff'] = tp_merged['Sink_Price'] - tp_merged['Source_Price']
            
            if hedge_type == 'OPT':
                # Options: max(0, diff) at each half-hourly period, then average
                tp_merged['Settlement'] = tp_merged['Price_Diff'].clip(lower=0)
            else:  # OBL
                # Obligations: straight price difference
                tp_merged['Settlement'] = tp_merged['Price_Diff']
            
            # Daily settlement price = average across all trading periods
            daily_settlement = tp_merged['Settlement'].mean()
            
            # Daily profit = (settlement - price_paid) * MW * trading_periods * 0.5
            # Each trading period is 0.5 hours, so profit per period = (settlement - price) * MW * 0.5
            daily_profit = (daily_settlement - price_paid) * mw * num_trading_periods * 0.5
            
            mtd_profit += daily_profit
            
            results.append({
                'FTR Period': year_month,
                'FTR Product': ftr_product,
                'FTR_ID': ftr_id,
                'Date': trade_date.strftime('%d/%m/%Y'),
                'Daily Settlement Price': round(daily_settlement, 2),
                'Price Paid': round(price_paid, 2),
                'MW': mw,
                'Num_TPs': num_trading_periods,
                'Daily Profit': round(daily_profit, 2),
                'MTD Profit': round(mtd_profit, 2),
            })
    
    return pd.DataFrame(results)


def generate_settlement_report(owner_code, year_month):
    """Generate settlement report for an owner."""
    logger.info(f"Generating settlement report for {owner_code} - {year_month}")
    
    # Find latest snapshot
    snapshots = sorted(SNAPSHOT_DIR.glob("ftr_snapshot_*.csv"), reverse=True)
    if not snapshots:
        logger.error("No snapshot files found")
        return None
    
    snapshot_file = snapshots[0]
    logger.info(f"Using snapshot: {snapshot_file.name}")
    
    # Get owner positions
    positions = get_owner_positions(owner_code, snapshot_file)
    
    if positions.empty:
        logger.warning(f"No positions found for owner: {owner_code}")
        return None
    
    logger.info(f"Found {len(positions)} positions for {owner_code}")
    
    # Load spot prices
    spot_df = load_spot_prices_for_month(year_month)
    
    if spot_df is None:
        return None
    
    # Calculate daily settlements
    settlements_df = calculate_daily_settlements(positions, spot_df, year_month)
    
    if settlements_df.empty:
        logger.warning("No settlement data calculated")
        return None
    
    # Sort by FTR Product and Date
    settlements_df = settlements_df.sort_values(['FTR Product', 'Date'])
    
    # Save to Excel
    timestamp = datetime.now().strftime('%H%M%S')
    output_file = REPORTS_DIR / f"{owner_code}_Settlement_Report_{year_month}_{timestamp}.xlsx"
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Daily settlements sheet
        settlements_df[['FTR Period', 'FTR Product', 'Date', 'Daily Settlement Price', 
                        'Price Paid', 'MW', 'Num_TPs', 'Daily Profit', 'MTD Profit']].to_excel(
            writer, sheet_name='Daily_Settlements', index=False
        )
        
        # Summary by product
        summary = settlements_df.groupby('FTR Product').agg({
            'MW': 'first',
            'Price Paid': 'first',
            'Daily Profit': 'sum',
            'MTD Profit': 'last'
        }).reset_index()
        summary.columns = ['FTR Product', 'MW', 'Price Paid', 'Total Profit', 'Final MTD Profit']
        summary.to_excel(writer, sheet_name='Summary', index=False)
    
    logger.info(f"Report saved: {output_file}")
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"Settlement Report for {owner_code} - {year_month}")
    print(f"{'='*80}\n")
    print(f"Total Positions: {len(positions)}")
    print(f"Trading Days: {settlements_df['Date'].nunique()}")
    print(f"\nTotal MTD Profit: ${settlements_df.groupby('FTR Product')['MTD Profit'].last().sum():,.2f}\n")
    
    print("Summary by Product:")
    print(summary.to_string(index=False))
    
    print(f"\n{'='*80}")
    print(f"Full report saved to: {output_file}")
    print(f"{'='*80}\n")
    
    return output_file


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python generate_owner_settlement_report.py OWNER YYYYMM")
        print("Example: python generate_owner_settlement_report.py FLTR 202602")
        sys.exit(1)
    
    owner = sys.argv[1]
    year_month = sys.argv[2]
    
    # Validate format
    if len(year_month) != 6 or not year_month.isdigit():
        print("Error: Year-Month must be in YYYYMM format")
        sys.exit(1)
    
    generate_settlement_report(owner, year_month)
