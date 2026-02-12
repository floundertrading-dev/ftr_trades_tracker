"""
Generate FLTR portfolio email summary
======================================
Creates a plain-text email summary for FLTR positions that are live
in the current month, showing investment, daily return, and MTD return.

Usage: python generate_fltr_email.py [YYYYMMDD]
  - If no date given, uses the latest snapshot date.
  - Filters to positions with a StartDate in the same month as the report date.

Output: ftr_tracking/reports/fltr_email_summary.txt
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# Setup
BASE_DIR = Path(__file__).parent / "ftr_tracking"
SNAPSHOT_DIR = BASE_DIR / "snapshots"
SPOT_CACHE_DIR = BASE_DIR / "spot_cache"
REPORTS_DIR = BASE_DIR / "reports"

REPORTS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_snapshot_for_date(report_date_str=None):
    """Load snapshot. If date given use that, otherwise latest."""
    if report_date_str:
        path = SNAPSHOT_DIR / f"ftr_snapshot_{report_date_str}.csv"
        if path.exists():
            return pd.read_csv(path), report_date_str
        # Fall back to latest
        logger.warning(f"Snapshot for {report_date_str} not found, using latest")

    snapshots = sorted(SNAPSHOT_DIR.glob("ftr_snapshot_*.csv"), reverse=True)
    if not snapshots:
        raise FileNotFoundError("No snapshot files found")
    latest = snapshots[0]
    date_str = latest.stem.replace("ftr_snapshot_", "")
    return pd.read_csv(latest), date_str


def load_spot_prices(year_month):
    """Load half-hourly spot prices for a month."""
    spot_file = SPOT_CACHE_DIR / f"spot_{year_month}.csv"
    if not spot_file.exists():
        logger.error(f"Spot price file not found: {spot_file}")
        return None

    spot_df = pd.read_csv(spot_file)
    spot_df['Trading date'] = pd.to_datetime(spot_df['Trading date'], dayfirst=True)
    return spot_df


def calculate_daily_and_mtd(position, spot_df, report_date):
    """
    Calculate daily return and MTD return for a single position.

    Uses half-hourly trading period logic:
    - Options: max(0, sink - source) at each period, then average
    - Obligations: (sink - source) straight
    - Profit per day = (avg_settlement - price_paid) * MW * num_periods * 0.5
    """
    source_node = f"{position['Source']}2201"
    sink_node = f"{position['Sink']}2201"
    hedge_type = position['HedgeType']
    mw = float(position['MW'] or 0)
    price_paid = float(position['Price'] or 0)

    month_start = datetime(report_date.year, report_date.month, 1)

    # Filter spot data to MTD
    mtd_spot = spot_df[(spot_df['Trading date'] >= month_start) &
                       (spot_df['Trading date'] <= report_date)].copy()

    trading_dates = sorted(mtd_spot['Trading date'].unique())

    daily_profit = 0.0
    mtd_profit = 0.0
    daily_settlement = 0.0
    num_trading_days = 0

    for trade_date in trading_dates:
        day_spot = mtd_spot[mtd_spot['Trading date'] == trade_date]

        source_tp = day_spot[day_spot['Point of connection'] == source_node][['Trading period', '$/MWh']].copy()
        sink_tp = day_spot[day_spot['Point of connection'] == sink_node][['Trading period', '$/MWh']].copy()

        if source_tp.empty or sink_tp.empty:
            continue

        source_tp.columns = ['Trading period', 'Source_Price']
        sink_tp.columns = ['Trading period', 'Sink_Price']

        tp_merged = source_tp.merge(sink_tp, on='Trading period')
        if tp_merged.empty:
            continue

        tp_merged['Price_Diff'] = tp_merged['Sink_Price'] - tp_merged['Source_Price']

        if hedge_type == 'OPT':
            tp_merged['Settlement'] = tp_merged['Price_Diff'].clip(lower=0)
        else:
            tp_merged['Settlement'] = tp_merged['Price_Diff']

        day_settlement = tp_merged['Settlement'].mean()
        day_profit = (day_settlement - price_paid) * mw * len(tp_merged) * 0.5

        mtd_profit += day_profit
        num_trading_days += 1

        # If this is the report date, capture daily figures
        if trade_date == pd.Timestamp(report_date):
            daily_profit = day_profit
            daily_settlement = day_settlement

    return {
        'daily_settlement': daily_settlement,
        'daily_profit': daily_profit,
        'mtd_profit': mtd_profit,
        'trading_days': num_trading_days,
    }


def generate_fltr_email(report_date_str=None):
    """Generate the FLTR portfolio email text."""

    # Load snapshot
    snapshot_df, snap_date = get_snapshot_for_date(report_date_str)
    report_date = datetime.strptime(snap_date, '%Y%m%d')
    year_month = snap_date[:6]

    logger.info(f"Report date: {report_date.date()}")

    # Get FLTR positions with start date in current month
    fltr = snapshot_df[snapshot_df['CurrentOwner'] == 'FLTR'].copy()
    fltr['_StartDate'] = pd.to_datetime(fltr['StartDate'], dayfirst=True)

    # Filter to positions starting in the report month
    month_start = datetime(report_date.year, report_date.month, 1)
    if report_date.month == 12:
        month_end = datetime(report_date.year + 1, 1, 1)
    else:
        month_end = datetime(report_date.year, report_date.month + 1, 1)

    live = fltr[(fltr['_StartDate'] >= month_start) &
                (fltr['_StartDate'] < month_end)].copy()

    if live.empty:
        msg = f"No live FLTR positions for {report_date.strftime('%B %Y')}."
        logger.warning(msg)
        # Still write the file so the email step doesn't fail
        out_path = REPORTS_DIR / "fltr_email_summary.txt"
        out_path.write_text(msg, encoding='utf-8')
        return msg

    logger.info(f"Live FLTR positions this month: {len(live)}")

    # Load spot prices
    spot_df = load_spot_prices(year_month)
    if spot_df is None:
        return "Spot price data not available."

    # ── Build email text ──────────────────────────────────────────────────

    date_nice = report_date.strftime('%B %d, %Y')
    lines = []
    lines.append(f"FLTR Portfolio Update — {date_nice}")
    lines.append("═" * 50)
    lines.append("")

    total_invested = 0.0
    total_daily = 0.0
    total_mtd = 0.0
    position_blocks = []

    for _, pos in live.iterrows():
        result = calculate_daily_and_mtd(pos, spot_df, report_date)

        investment = float(pos['OriginalAcquisitionCost'] or 0)
        total_invested += investment
        total_daily += result['daily_profit']
        total_mtd += result['mtd_profit']

        mw = float(pos['MW'] or 0)
        price = float(pos['Price'] or 0)

        route = f"{pos['Source']}→{pos['Sink']}"
        label = f"{route} ({pos['HedgeType']}) {mw}MW @ ${price:.2f}/MWh"

        block = []
        block.append(f"  📌 {label}")
        block.append(f"     Investment:      ${investment:>10,.2f}")
        block.append(f"     Price Paid:      ${price:>10.2f} /MWh")
        block.append(f"     Today's Settl.:  ${result['daily_settlement']:>10.2f} /MWh")
        block.append(f"     Today's Return:  ${result['daily_profit']:>10,.2f}")
        block.append(f"     MTD Return:      ${result['mtd_profit']:>10,.2f}")
        position_blocks.append("\n".join(block))

    lines.append("\n\n".join(position_blocks))
    lines.append("")
    lines.append("─" * 50)
    lines.append(f"  💼 Portfolio Total")
    lines.append(f"     Total Invested:  ${total_invested:>10,.2f}")
    lines.append(f"     Today's Return:  ${total_daily:>10,.2f}")
    lines.append(f"     MTD Return:      ${total_mtd:>10,.2f}")
    lines.append("─" * 50)
    lines.append("")

    # Add a small legend
    pct_return = (total_mtd / total_invested * 100) if total_invested else 0
    if total_mtd >= 0:
        lines.append(f"  📈 MTD Return on Investment: {pct_return:+.1f}%")
    else:
        lines.append(f"  📉 MTD Return on Investment: {pct_return:+.1f}%")

    lines.append(f"  📅 Trading Days this month: {result['trading_days']}")
    lines.append("")

    email_text = "\n".join(lines)

    # Save
    out_path = REPORTS_DIR / "fltr_email_summary.txt"
    out_path.write_text(email_text, encoding='utf-8')
    logger.info(f"FLTR email summary saved: {out_path}")

    print(email_text)
    return email_text


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    generate_fltr_email(date_arg)
