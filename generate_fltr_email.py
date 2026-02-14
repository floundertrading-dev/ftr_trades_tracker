"""
Generate owner portfolio email summary
=======================================
Creates a plain-text email summary for owner positions that are live
in the current month, showing investment, daily return, and MTD return.

Usage: python generate_fltr_email.py [YYYYMMDD] [OWNER]
  - If no date given, uses the latest snapshot date.
  - If no owner given, defaults to FLTR.
  - Filters to positions with a StartDate in the same month as the report date.

Output: ftr_tracking/reports/{owner}_email_summary.txt
"""

import sys
import numpy as np
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


def build_settlement_table(spot_df, report_date):
    """
    Pre-compute a pivot table of spot prices for vectorized lookups.
    Returns pivot (rows=date/period, cols=node), node list, and trading dates.
    """
    month_start = datetime(report_date.year, report_date.month, 1)
    mtd_spot = spot_df[(spot_df['Trading date'] >= month_start) &
                       (spot_df['Trading date'] <= report_date)].copy()

    pivot = mtd_spot.pivot_table(
        index=['Trading date', 'Trading period'],
        columns='Point of connection',
        values='$/MWh',
        aggfunc='first'
    )

    nodes = pivot.columns.tolist()
    trading_dates = sorted(mtd_spot['Trading date'].unique())
    return pivot, nodes, trading_dates


def calculate_positions_vectorized(live_df, spot_pivot, trading_dates, report_date):
    """
    Calculate daily and MTD settlement for all positions using vectorized
    operations grouped by (source, sink, hedge_type) to avoid redundant work.
    """
    results = []
    grouped = live_df.groupby(['Source', 'Sink', 'HedgeType'])

    for (source, sink, hedge_type), group in grouped:
        source_node = f"{source}2201"
        sink_node = f"{sink}2201"

        if source_node not in spot_pivot.columns or sink_node not in spot_pivot.columns:
            for idx in group.index:
                results.append((idx, 0.0, 0.0, 0.0, 0.0, 0))
            continue

        # Vectorized price diff for all dates/periods at once
        diff = spot_pivot[sink_node] - spot_pivot[source_node]
        if hedge_type == 'OPT':
            diff = diff.clip(lower=0)
        diff = diff.dropna()

        if diff.empty:
            for idx in group.index:
                results.append((idx, 0.0, 0.0, 0.0, 0.0, 0))
            continue

        # Daily aggregation
        daily = diff.groupby('Trading date').agg(['mean', 'count'])
        daily.columns = ['avg_settlement', 'num_periods']

        num_trading_days = len(daily)
        mtd_avg_settlement = daily['avg_settlement'].mean() if num_trading_days > 0 else 0.0

        report_ts = pd.Timestamp(report_date)
        daily_settlement = daily.loc[report_ts, 'avg_settlement'] if report_ts in daily.index else 0.0

        # Per-position profit (each has its own MW and price)
        for idx in group.index:
            pos = live_df.loc[idx]
            mw = float(pos['MW'] or 0)
            price_paid = float(pos['Price'] or 0)

            mtd_profit = ((daily['avg_settlement'] - price_paid) * mw * daily['num_periods'] * 0.5).sum()

            if report_ts in daily.index:
                day_row = daily.loc[report_ts]
                daily_profit = (day_row['avg_settlement'] - price_paid) * mw * day_row['num_periods'] * 0.5
            else:
                daily_profit = 0.0

            results.append((idx, daily_settlement, daily_profit, mtd_profit, mtd_avg_settlement, num_trading_days))

    return results


def generate_owner_email(owner_code, snapshot_df, spot_pivot, trading_dates, report_date):
    """Generate email text for a single owner. Returns the text or None."""
    # Get owner positions with start date in current month
    owner_positions = snapshot_df[snapshot_df['CurrentOwner'] == owner_code].copy()
    owner_positions['_StartDate'] = pd.to_datetime(owner_positions['StartDate'], dayfirst=True)

    month_start = datetime(report_date.year, report_date.month, 1)
    if report_date.month == 12:
        month_end = datetime(report_date.year + 1, 1, 1)
    else:
        month_end = datetime(report_date.year, report_date.month + 1, 1)

    live = owner_positions[(owner_positions['_StartDate'] >= month_start) &
                           (owner_positions['_StartDate'] < month_end)].copy()

    if live.empty:
        logger.warning(f"No live {owner_code} positions for {report_date.strftime('%B %Y')}.")
        return None

    logger.info(f"Live {owner_code} positions this month: {len(live)}")

    # ── Calculate all positions (vectorized) ───────────────────────────────

    results = calculate_positions_vectorized(live, spot_pivot, trading_dates, report_date)

    total_invested = 0.0
    total_daily = 0.0
    total_mtd = 0.0
    wins = 0
    losses = 0
    position_data = []

    for (idx, daily_settlement, daily_profit, mtd_profit, mtd_settlement, trading_days) in results:
        pos = live.loc[idx]
        investment = float(pos['OriginalAcquisitionCost'] or 0)
        mw = float(pos['MW'] or 0)
        price = float(pos['Price'] or 0)

        # Settlement return %: expressed as gain/loss vs price paid
        # e.g. MTD settl = $5, price = $10 → -50.0% (settling 50% below cost)
        # e.g. MTD settl = $12, price = $10 → +20.0% (settling 20% above cost)
        if price > 0 and mtd_settlement > 0:
            settlement_return_pct = ((mtd_settlement - price) / price) * 100
        else:
            settlement_return_pct = -100.0 if price > 0 else 0.0

        # Dollar impact: settlement return % applied to investment
        settlement_dollar_impact = (settlement_return_pct / 100) * investment

        total_invested += investment
        total_daily += daily_profit
        total_mtd += mtd_profit

        if mtd_profit >= 0:
            wins += 1
        else:
            losses += 1

        route = f"{pos['Source']}→{pos['Sink']}"
        label = f"{route} ({pos['HedgeType']}) {mw}MW @ ${price:.2f}/MWh"

        position_data.append({
            'label': label,
            'investment': investment,
            'price': price,
            'daily_settlement': daily_settlement,
            'mtd_settlement': mtd_settlement,
            'daily_profit': daily_profit,
            'mtd_profit': mtd_profit,
            'trading_days': trading_days,
            'settlement_return_pct': settlement_return_pct,
            'settlement_dollar_impact': settlement_dollar_impact,
        })

    # Sort by investment (largest first)
    position_data.sort(key=lambda x: x['investment'], reverse=True)

    trading_days = position_data[0]['trading_days'] if position_data else 0
    pct_return = (total_mtd / total_invested * 100) if total_invested else 0

    # ── Build email text ──────────────────────────────────────────────────

    date_nice = report_date.strftime('%B %d, %Y')
    lines = []
    lines.append(f"{owner_code} Portfolio Update — {date_nice}")
    lines.append("═" * 50)
    lines.append("")

    # Summary header
    roi_icon = "📈" if total_mtd >= 0 else "📉"
    lines.append(f"  {roi_icon} MTD P&L: ${total_mtd:>10,.2f}  ({pct_return:+.1f}%)")
    lines.append(f"  💰 Total Invested:  ${total_invested:>10,.2f}")
    # Note: total_settlement_impact computed after position loop, inserted here via deferred join
    lines.append("__SETTL_IMPACT_PLACEHOLDER__")
    lines.append(f"  📅 Trading Days: {trading_days}")
    lines.append(f"  ✅ Winners: {wins}   ❌ Losers: {losses}   ({len(position_data)} positions)")
    lines.append("")
    lines.append("─" * 50)
    lines.append("")

    # Build position blocks
    total_settlement_impact = sum(p['settlement_dollar_impact'] for p in position_data)

    position_blocks = []
    for pos in position_data:
        block = []
        block.append(f"  📌 {pos['label']}")
        block.append(f"     Investment:      ${pos['investment']:>10,.2f}")
        block.append(f"     Price Paid:      ${pos['price']:>10.2f} /MWh")
        block.append(f"     Today's Settl.:  ${pos['daily_settlement']:>10.2f} /MWh")
        block.append(f"     MTD Avg Settl.:  ${pos['mtd_settlement']:>10.2f} /MWh")
        settl_pct = pos['settlement_return_pct']
        settl_icon = '🟢' if settl_pct >= 0 else '🔴'
        block.append(f"     Settl. Return:   {settl_icon} {settl_pct:>+9.1f}%  (${pos['settlement_dollar_impact']:>+11,.2f})")
        block.append(f"     Today's P&L:     ${pos['daily_profit']:>10,.2f}")
        block.append(f"     MTD P&L:         ${pos['mtd_profit']:>10,.2f}")
        position_blocks.append("\n".join(block))

    lines.append("\n\n".join(position_blocks))
    lines.append("")
    lines.append("─" * 50)
    lines.append(f"  💼 Portfolio Total")
    lines.append(f"     Total Invested:  ${total_invested:>10,.2f}")
    lines.append(f"     Settl. Impact:   ${total_settlement_impact:>+11,.2f}")
    lines.append(f"     Today's P&L:     ${total_daily:>10,.2f}")
    lines.append(f"     MTD P&L:         ${total_mtd:>10,.2f}  ({pct_return:+.1f}%)")
    lines.append("─" * 50)
    lines.append("")

    # Replace settlement impact placeholder now that we have the total
    settl_impact_icon = '🟢' if total_settlement_impact >= 0 else '🔴'
    settl_impact_line = f"  {settl_impact_icon} Settl. Impact:    ${total_settlement_impact:>+11,.2f}"
    text = "\n".join(lines)
    text = text.replace("__SETTL_IMPACT_PLACEHOLDER__", settl_impact_line)
    return text


def generate_all_owner_emails(report_date_str=None, owners=None):
    """
    Generate portfolio email summaries for all specified owners.
    Combines them into a single file for the email body.
    """
    if owners is None:
        owners = ['FLTR', 'BRAD', 'SWET']

    # Load snapshot once
    snapshot_df, snap_date = get_snapshot_for_date(report_date_str)
    report_date = datetime.strptime(snap_date, '%Y%m%d')
    year_month = snap_date[:6]

    logger.info(f"Report date: {report_date.date()}")

    # Load spot prices once and build pivot table
    spot_df = load_spot_prices(year_month)
    if spot_df is None:
        logger.error("Spot price data not available.")
        return

    spot_pivot, nodes, trading_dates = build_settlement_table(spot_df, report_date)
    logger.info(f"Spot pivot: {len(spot_pivot)} rows, {len(nodes)} nodes, {len(trading_dates)} trading days")

    # Generate each owner section
    sections = []
    for owner in owners:
        section = generate_owner_email(owner, snapshot_df, spot_pivot, trading_dates, report_date)
        if section:
            sections.append(section)

    combined = "\n\n".join(sections)

    # Save combined file
    out_path = REPORTS_DIR / "owner_portfolio_email.txt"
    out_path.write_text(combined, encoding='utf-8')
    logger.info(f"Combined owner email saved: {out_path}")

    # Also save individual files
    for owner in owners:
        section = generate_owner_email(owner, snapshot_df, spot_pivot, trading_dates, report_date)
        if section:
            ind_path = REPORTS_DIR / f"{owner.lower()}_email_summary.txt"
            ind_path.write_text(section, encoding='utf-8')

    print(combined.encode('ascii', 'replace').decode('ascii'))
    return combined


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    # Accept optional owner list: python generate_fltr_email.py [date] [OWNER1,OWNER2,...]
    if len(sys.argv) > 2:
        owners = sys.argv[2].split(',')
    else:
        owners = ['FLTR', 'BRAD', 'SWET']
    generate_all_owner_emails(date_arg, owners)
