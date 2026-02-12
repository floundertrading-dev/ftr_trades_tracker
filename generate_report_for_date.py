"""
Generate FTR report for a specific date
Usage: python generate_report_for_date.py YYYYMMDD
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from ftr_report_generator_daily import (
    get_latest_snapshot, 
    load_ledger,
    calculate_position_summary,
    calculate_owner_summary,
    get_recent_activity,
    create_excel_report,
    generate_email_summary,
    SNAPSHOT_DIR,
    logger
)

def generate_report_for_date(target_date_str):
    """Generate report for a specific date."""
    logger.info("=" * 50)
    logger.info(f"FTR Report Generation for {target_date_str}")
    
    # Look for snapshot for the target date
    snapshot_file = SNAPSHOT_DIR / f"ftr_snapshot_{target_date_str}.csv"
    
    if snapshot_file.exists():
        logger.info(f"Using snapshot: {snapshot_file.name}")
        snapshot_df = pd.read_csv(snapshot_file)
        report_date = target_date_str
    else:
        # Use the most recent snapshot available
        logger.warning(f"No snapshot found for {target_date_str}, using latest available")
        snapshot_df, report_date = get_latest_snapshot()
        logger.info(f"Using snapshot from: {report_date}")
    
    ledger_df = load_ledger()
    
    logger.info(f"Report date: {target_date_str}")
    logger.info(f"Positions in snapshot: {len(snapshot_df)}")
    
    # Process data - use target date for spot price lookups
    position_df = calculate_position_summary(snapshot_df, target_date_str)
    owner_df = calculate_owner_summary(position_df)
    activity_df = get_recent_activity(ledger_df)
    
    # Generate report with target date
    report_path = create_excel_report(position_df, activity_df, owner_df, target_date_str)
    
    # Generate email summary
    summary = generate_email_summary(
        position_df, owner_df, target_date_str, report_path.name
    )
    
    print(summary)
    
    logger.info("Report generation completed")
    return report_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_report_for_date.py YYYYMMDD")
        print("Example: python generate_report_for_date.py 20260212")
        sys.exit(1)
    
    target_date = sys.argv[1]
    
    # Validate format
    if len(target_date) != 8 or not target_date.isdigit():
        print("Error: Date must be in YYYYMMDD format")
        sys.exit(1)
    
    try:
        datetime.strptime(target_date, '%Y%m%d')
    except ValueError:
        print("Error: Invalid date")
        sys.exit(1)
    
    generate_report_for_date(target_date)
