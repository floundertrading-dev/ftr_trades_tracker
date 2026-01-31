"""
FTR Daily Pull - GitHub Actions Version
"""

import boto3
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging
import time
import os

# =============================================================================
# CONFIGURATION (from GitHub Secrets)
# =============================================================================

CLIENT_ID = os.environ.get('FTR_CLIENT_ID')
USERNAME = os.environ.get('FTR_USERNAME')
PASSWORD = os.environ.get('FTR_PASSWORD')

API_BASE_URL = "https://api.ftr.co.nz/ftr_register/"
AWS_REGION = "ap-southeast-2"

BASE_DIR = Path(__file__).parent / "ftr_tracking"
SNAPSHOT_DIR = BASE_DIR / "snapshots"
LEDGER_FILE = BASE_DIR / "ftr_master_ledger.csv"
LOG_FILE = BASE_DIR / "ftr_daily_pull.log"

SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# =============================================================================
# AUTHENTICATION
# =============================================================================

class TokenManager:
    def __init__(self):
        self.token = None
        self.token_expiry = None
    
    def get_token(self):
        if self.token and self.token_expiry and datetime.now() < self.token_expiry:
            return self.token
        return self._authenticate()
    
    def _authenticate(self):
        client = boto3.client('cognito-idp', region_name=AWS_REGION)
        response = client.initiate_auth(
            ClientId=CLIENT_ID,
            AuthFlow='USER_PASSWORD_AUTH',
            AuthParameters={'USERNAME': USERNAME, 'PASSWORD': PASSWORD}
        )
        self.token = response['AuthenticationResult']['IdToken']
        self.token_expiry = datetime.now() + timedelta(minutes=55)
        logger.info("Authenticated successfully")
        return self.token

token_manager = TokenManager()

# =============================================================================
# DATA FETCHING
# =============================================================================

def fetch_ftr_data(settled="FALSE", page_size=1000):
    all_raw_data = []
    page = 1
    
    while True:
        headers = {"Authorization": token_manager.get_token()}
        params = {"settled": settled, "page": str(page), "pageSize": str(page_size)}
        
        response = requests.get(API_BASE_URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            break
        
        all_raw_data.extend(data)
        logger.info(f"Page {page}: {len(data)} records")
        
        if len(data) < page_size:
            break
        page += 1
        time.sleep(0.1)
    
    # Flatten nested structure
    flattened = []
    for auction in all_raw_data:
        for award in auction.get('awards', []):
            for ftr in award.get('ftrs', []):
                flattened.append({
                    'FTR_ID': ftr.get('ftrId'),
                    'AuctionName': auction.get('marketName', ''),
                    'DateAcquired': auction.get('dateAcquired'),
                    'Status': auction.get('status'),
                    'StartDate': award.get('startDate'),
                    'EndDate': award.get('endDate'),
                    'Source': award.get('source'),
                    'Sink': award.get('sink'),
                    'HedgeType': award.get('hedgeType'),
                    'CurrentOwner': ftr.get('currentOwner'),
                    'MW': ftr.get('mw'),
                    'Price': ftr.get('price'),
                    'AcquisitionCost': ftr.get('aq'),
                    'OriginalAcquisitionCost': ftr.get('origAq'),
                })
    
    logger.info(f"Total FTR records: {len(flattened)}")
    return pd.DataFrame(flattened)

# =============================================================================
# SNAPSHOT MANAGEMENT
# =============================================================================

def save_snapshot(df, date_str=None):
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')
    filepath = SNAPSHOT_DIR / f"ftr_snapshot_{date_str}.csv"
    df.to_csv(filepath, index=False)
    logger.info(f"Saved: {filepath}")

def load_snapshot(date_str):
    filepath = SNAPSHOT_DIR / f"ftr_snapshot_{date_str}.csv"
    return pd.read_csv(filepath) if filepath.exists() else None

def get_previous_snapshot_date(current_date_str):
    current = datetime.strptime(current_date_str, '%Y%m%d')
    for days_back in range(1, 8):
        check = (current - timedelta(days=days_back)).strftime('%Y%m%d')
        if (SNAPSHOT_DIR / f"ftr_snapshot_{check}.csv").exists():
            return check
    return None

# =============================================================================
# CHANGE DETECTION
# =============================================================================

def calculate_trading_periods(start_date, end_date):
    for fmt in ['%d/%m/%Y', '%Y-%m-%d']:
        try:
            start = datetime.strptime(str(start_date), fmt)
            end = datetime.strptime(str(end_date), fmt)
            return (end - start).days * 24
        except:
            continue
    return None

def detect_changes(df_today, df_yesterday):
    if df_yesterday is None:
        return pd.DataFrame()
    
    changes = []
    today_date = datetime.now().strftime('%Y-%m-%d')
    
    merged = df_yesterday.merge(df_today, on='FTR_ID', how='outer', 
                                 suffixes=('_prev', '_curr'), indicator=True)
    
    for _, row in merged.iterrows():
        ftr_id = row['FTR_ID']
        
        # CLOSED
        if row['_merge'] == 'left_only':
            prev_mw = float(row.get('MW_prev', 0) or 0)
            changes.append({
                'SnapshotDate': today_date, 'TransactionType': 'CLOSED',
                'FTR_ID': ftr_id, 'Source': row.get('Source_prev'),
                'Sink': row.get('Sink_prev'), 'MW_Previous': prev_mw,
                'MW_Current': 0, 'MW_Sold': prev_mw, 'Notes': 'Position closed'
            })
            logger.info(f"CLOSED: {ftr_id} - {prev_mw}MW")
            continue
        
        # NEW
        if row['_merge'] == 'right_only':
            curr_mw = float(row.get('MW_curr', 0) or 0)
            changes.append({
                'SnapshotDate': today_date, 'TransactionType': 'NEW',
                'FTR_ID': ftr_id, 'Source': row.get('Source_curr'),
                'Sink': row.get('Sink_curr'), 'MW_Previous': 0,
                'MW_Current': curr_mw, 'Notes': 'New position'
            })
            logger.info(f"NEW: {ftr_id} - {curr_mw}MW")
            continue
        
        # Check for changes
        prev_mw = float(row.get('MW_prev', 0) or 0)
        curr_mw = float(row.get('MW_curr', 0) or 0)
        orig_cost = float(row.get('OriginalAcquisitionCost_curr') or 
                         row.get('OriginalAcquisitionCost_prev') or 0)
        
        prev_acq = row.get('AcquisitionCost_prev')
        curr_acq = row.get('AcquisitionCost_curr')
        prev_acq = orig_cost if pd.isna(prev_acq) or prev_acq == '' else float(prev_acq)
        curr_acq = orig_cost if pd.isna(curr_acq) or curr_acq == '' else float(curr_acq)
        
        # SELL
        if curr_mw < prev_mw:
            mw_sold = prev_mw - curr_mw
            trading_periods = calculate_trading_periods(
                row.get('StartDate_curr'), row.get('EndDate_curr'))
            sale_proceeds = prev_acq - curr_acq
            cost_basis = (orig_cost / prev_mw) * mw_sold if prev_mw > 0 else 0
            profit = sale_proceeds - cost_basis
            price_per_mw = sale_proceeds / trading_periods / mw_sold if trading_periods and mw_sold else None
            
            changes.append({
                'SnapshotDate': today_date, 'TransactionType': 'SELL',
                'FTR_ID': ftr_id, 'Source': row.get('Source_curr'),
                'Sink': row.get('Sink_curr'), 'HedgeType': row.get('HedgeType_curr'),
                'StartDate': row.get('StartDate_curr'), 'EndDate': row.get('EndDate_curr'),
                'MW_Previous': prev_mw, 'MW_Current': curr_mw, 'MW_Sold': mw_sold,
                'OriginalAcquisitionCost': orig_cost,
                'AcquisitionCost_Previous': prev_acq, 'AcquisitionCost_Current': curr_acq,
                'SaleProceeds': sale_proceeds, 'Profit': profit,
                'TradingPeriods': trading_periods, 'PricePerMW': price_per_mw,
                'Notes': f'Sold {mw_sold:.1f}MW'
            })
            logger.info(f"SELL: {ftr_id} - {mw_sold}MW, Profit: ${profit:,.2f}")
        
        # BUY
        elif curr_mw > prev_mw:
            mw_bought = curr_mw - prev_mw
            changes.append({
                'SnapshotDate': today_date, 'TransactionType': 'BUY',
                'FTR_ID': ftr_id, 'Source': row.get('Source_curr'),
                'Sink': row.get('Sink_curr'), 'MW_Previous': prev_mw,
                'MW_Current': curr_mw, 'Notes': f'Bought {mw_bought:.1f}MW'
            })
            logger.info(f"BUY: {ftr_id} - {mw_bought}MW")
    
    return pd.DataFrame(changes)

# =============================================================================
# LEDGER
# =============================================================================

def update_ledger(changes_df):
    if changes_df.empty:
        return
    if LEDGER_FILE.exists():
        existing = pd.read_csv(LEDGER_FILE)
        updated = pd.concat([existing, changes_df], ignore_index=True)
    else:
        updated = changes_df
    updated.to_csv(LEDGER_FILE, index=False)
    logger.info(f"Ledger: +{len(changes_df)} records")

def initialize_ledger(df, date_str):
    records = []
    for _, row in df.iterrows():
        records.append({
            'SnapshotDate': datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d'),
            'TransactionType': 'INITIAL', 'FTR_ID': row.get('FTR_ID'),
            'Source': row.get('Source'), 'Sink': row.get('Sink'),
            'MW_Current': float(row.get('MW', 0) or 0),
            'OriginalAcquisitionCost': float(row.get('OriginalAcquisitionCost', 0) or 0),
        })
    return pd.DataFrame(records)

# =============================================================================
# MAIN
# =============================================================================

def run_daily_pull():
    logger.info("=" * 50)
    logger.info("FTR Daily Pull Started")
    
    today_str = datetime.now().strftime('%Y%m%d')
    df_today = fetch_ftr_data()
    
    if df_today.empty:
        logger.warning("No data fetched")
        return
    
    save_snapshot(df_today, today_str)
    
    prev_date = get_previous_snapshot_date(today_str)
    
    if prev_date is None:
        logger.info("First run - initializing ledger")
        update_ledger(initialize_ledger(df_today, today_str))
    else:
        df_yesterday = load_snapshot(prev_date)
        changes = detect_changes(df_today, df_yesterday)
        
        if not changes.empty:
            update_ledger(changes)
            print("\n📊 CHANGES DETECTED:")
            print(changes['TransactionType'].value_counts())
            
            sells = changes[changes['TransactionType'] == 'SELL']
            if not sells.empty:
                print(f"💰 Total Profit: ${sells['Profit'].sum():,.2f}")
    
    logger.info("Completed")

if __name__ == "__main__":
    run_daily_pull()
