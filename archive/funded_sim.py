"""
funded_sim.py — Simulate multiple combine → XFA accounts across different start dates.

Launches N accounts with staggered start dates across a date range.
Each account runs through the actual backtest engine on real market data:
  1. COMBINE phase: $750 risk, 50 contract cap, -$2K trailing MLL → pass at $3K
  2. XFA phase: Conservative risk steps, contract tiers, -$2K trailing MLL → pass at $12K

Accounts that blow up die. Accounts that pass move to the next phase.
Multiple accounts can trade on the same day.

Usage:
  python3 funded_sim.py --file-1m data.csv --accounts 100 --year 2025
  python3 funded_sim.py --file-1m data.csv --accounts 50 --start 2025-06-01 --end 2025-12-31
"""

import os
import sys
import logging
import argparse
import random
import json
from datetime import datetime, timedelta, date
from collections import defaultdict

import pandas as pd
import pytz

from engine import ICTEngine

# =====================================================================
# CONFIG
# =====================================================================
INSTRUMENT = "MES"
POINT_VALUE = 5.00
LIQUIDITY_DAYS = 5
TZ = pytz.timezone("America/Chicago")

# Combine rules
COMBINE_RISK = 750
COMBINE_MAX_CONTRACTS = 50
COMBINE_TARGET = 3000
COMBINE_MLL_OFFSET = 2000

# XFA rules
XFA_MLL_OFFSET = 2000
XFA_TARGET = 8000

XFA_CONTRACT_TIERS = [
    (2_000, 50),
    (1_500, 30),
    (0,     20),
]

XFA_RISK_TIERS = [
    (5_000, 1_250),
    (3_000, 1_000),
    (1_500,   875),
    (0,       750),
]

# Fees
FEE_MES_RT = 0.74
TICK_SIZE = 0.25
SLIPPAGE_TIERS = [
    (50,   0.5),
    (200,  1.0),
    (500,  1.5),
    (9999, 2.0),
]

def get_xfa_max_contracts(balance):
    for threshold, cap in XFA_CONTRACT_TIERS:
        if balance >= threshold:
            return cap
    return 20

def get_xfa_risk(balance):
    for threshold, risk in XFA_RISK_TIERS:
        if balance >= threshold:
            return risk
    return 750

def get_sl_slippage_ticks(qty):
    for max_qty, ticks in SLIPPAGE_TIERS:
        if qty <= max_qty:
            return ticks
    return 2.0

# =====================================================================
# LOGGING
# =====================================================================
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("ICTEngine").setLevel(logging.WARNING)
log = logging.getLogger("FundedSim")
log.setLevel(logging.INFO)

# =====================================================================
# DATA LOADING (reuse from backtest_replay)
# =====================================================================
def load_csv(filepath):
    ext = filepath.lower()
    if ext.endswith('.parquet'):
        df = pd.read_parquet(filepath)
    elif ext.endswith('.csv.zst') or ext.endswith('.csv.zstd'):
        df = pd.read_csv(filepath, compression='zstd')
    elif ext.endswith('.csv.gz'):
        df = pd.read_csv(filepath, compression='gzip')
    else:
        df = pd.read_csv(filepath)
    
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    
    ts_col = None
    for candidate in ['ts_event', 'timestamp', 'datetime', 'date', 'time', 'ts']:
        if candidate in df.columns:
            ts_col = candidate
            break
    if ts_col is None:
        ts_col = df.columns[0]
    
    if df[ts_col].dtype == 'int64' or df[ts_col].dtype == 'uint64':
        if df[ts_col].iloc[0] > 1e18:
            df['_ts'] = pd.to_datetime(df[ts_col], unit='ns', utc=True)
        elif df[ts_col].iloc[0] > 1e15:
            df['_ts'] = pd.to_datetime(df[ts_col], unit='us', utc=True)
        elif df[ts_col].iloc[0] > 1e12:
            df['_ts'] = pd.to_datetime(df[ts_col], unit='ms', utc=True)
        else:
            df['_ts'] = pd.to_datetime(df[ts_col], unit='s', utc=True)
    else:
        df['_ts'] = pd.to_datetime(df[ts_col], utc=True)
    
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=['close'], inplace=True)
    
    if 'symbol' in df.columns:
        df = df[~df['symbol'].str.contains('-', na=False)].copy()
        df = df[df['close'] > 0].copy()
        df = df.sort_values(['_ts', 'volume'], ascending=[True, False])
        df = df.drop_duplicates(subset='_ts', keep='first')
    
    if 'open' in df.columns and df['open'].median() > 1e6:
        for col in ['open', 'high', 'low', 'close']:
            df[col] = df[col] / 1e9
    
    df.sort_values('_ts', inplace=True)
    
    candles = []
    for _, row in df.iterrows():
        candles.append({
            "timestamp": row['_ts'].timestamp(),
            "open": float(row.get('open', 0)),
            "high": float(row.get('high', 0)),
            "low": float(row.get('low', 0)),
            "close": float(row.get('close', 0)),
            "volume": float(row.get('volume', 0)),
        })
    return candles

def synthesize_5m(candles_1m):
    df = pd.DataFrame(candles_1m)
    df['dt'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    df.set_index('dt', inplace=True)
    ohlcv = df.resample('5min').agg({
        'timestamp': 'first', 'open': 'first', 'high': 'max',
        'low': 'min', 'close': 'last', 'volume': 'sum',
    }).dropna(subset=['close'])
    return [{"timestamp": float(r['timestamp']), "open": float(r['open']),
             "high": float(r['high']), "low": float(r['low']),
             "close": float(r['close']), "volume": float(r['volume'])}
            for _, r in ohlcv.iterrows()]

def get_session_date(ts):
    ct = datetime.fromtimestamp(ts, pytz.utc).astimezone(TZ)
    if ct.hour >= 17:
        return (ct + timedelta(days=1)).date()
    return ct.date()

# =====================================================================
# TRADE SIMULATOR (lightweight, per-account)
# =====================================================================
class AccountSim:
    def __init__(self, account_id, start_date, phase="COMBINE"):
        self.id = account_id
        self.start_date = start_date
        self.phase = phase  # COMBINE or XFA
        self.balance = 0.0
        self.trailing_mll = -2000.0
        self.pending_trade = None
        self.open_trade = None
        self.trades = []
        self.status = "ACTIVE"  # ACTIVE, PASSED_COMBINE, PASSED_XFA, BLOWN
        self.days_traded = 0
        self.combine_days = 0
        self.xfa_days = 0
        self.combine_trades = 0
        self.xfa_trades = 0
    
    def get_risk(self):
        if self.phase == "COMBINE":
            return COMBINE_RISK
        return get_xfa_risk(self.balance)
    
    def get_max_contracts(self):
        if self.phase == "COMBINE":
            return COMBINE_MAX_CONTRACTS
        return get_xfa_max_contracts(self.balance)
    
    def on_signal(self, side, qty, entry, sl, tp):
        self.pending_trade = {
            "side": side, "qty": qty, "entry": entry,
            "sl": sl, "tp": tp,
        }
    
    def activate_pending(self):
        if self.pending_trade:
            self.open_trade = self.pending_trade
            self.pending_trade = None
    
    def check_exit(self, candle):
        if not self.open_trade:
            return False
        t = self.open_trade
        hit_sl = hit_tp = False
        if t["side"] == 0:
            if candle["low"] <= t["sl"]: hit_sl = True
            if candle["high"] >= t["tp"]: hit_tp = True
        else:
            if candle["high"] >= t["sl"]: hit_sl = True
            if candle["low"] <= t["tp"]: hit_tp = True
        
        if hit_sl and hit_tp:
            if t["side"] == 0:
                sl_dist = t["entry"] - t["sl"]
                tp_dist = t["tp"] - t["entry"]
            else:
                sl_dist = t["sl"] - t["entry"]
                tp_dist = t["entry"] - t["tp"]
            if sl_dist <= tp_dist: hit_tp = False
            else: hit_sl = False
        
        if hit_sl:
            pnl = (t["sl"] - t["entry"]) if t["side"] == 0 else (t["entry"] - t["sl"])
            self._close(pnl, "SL", t)
            return True
        if hit_tp:
            pnl = (t["tp"] - t["entry"]) if t["side"] == 0 else (t["entry"] - t["tp"])
            self._close(pnl, "TP", t)
            return True
        return False
    
    def force_close_eod(self, candle):
        if not self.open_trade:
            return
        t = self.open_trade
        pnl = (candle["close"] - t["entry"]) if t["side"] == 0 else (t["entry"] - candle["close"])
        self._close(pnl, "EOD", t)
    
    def _close(self, pnl_pts, reason, t):
        pnl_usd = pnl_pts * t["qty"] * POINT_VALUE
        # Fees
        fees = FEE_MES_RT * t["qty"]
        pnl_usd -= fees
        # Slippage on SL only
        if reason == "SL":
            slip_ticks = get_sl_slippage_ticks(t["qty"])
            slip_cost = slip_ticks * TICK_SIZE * t["qty"] * POINT_VALUE
            pnl_usd -= slip_cost
        
        self.balance += pnl_usd
        self.trades.append({"pnl": round(pnl_usd, 2), "reason": reason})
        
        # Update trailing MLL
        new_mll = self.balance - 2000
        if new_mll > self.trailing_mll:
            self.trailing_mll = min(new_mll, 0.0)
        
        # Check phase completion
        if self.phase == "COMBINE":
            self.combine_trades += 1
            if self.balance >= COMBINE_TARGET:
                self.status = "PASSED_COMBINE"
            elif self.balance <= self.trailing_mll:
                self.status = "BLOWN"
        elif self.phase == "XFA":
            self.xfa_trades += 1
            if self.balance >= XFA_TARGET:
                self.status = "PASSED_XFA"
            elif self.balance <= self.trailing_mll:
                self.status = "BLOWN"
        
        self.open_trade = None
    
    def promote_to_xfa(self):
        """Reset for XFA phase."""
        self.phase = "XFA"
        self.combine_days = self.days_traded
        self.balance = 0.0
        self.trailing_mll = -2000.0
        self.pending_trade = None
        self.open_trade = None
        self.status = "ACTIVE"


# =====================================================================
# MAIN
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Funded account simulation across multiple start dates")
    parser.add_argument("--file-1m", required=True, help="Path to 1-minute OHLCV data")
    parser.add_argument("--file-5m", default=None, help="Path to 5-minute OHLCV data (optional)")
    parser.add_argument("--accounts", type=int, default=100, help="Number of accounts to simulate")
    parser.add_argument("--year", type=int, default=None, help="Year to simulate (e.g., 2025)")
    parser.add_argument("--start", default=None, help="Start date range (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="End date range (YYYY-MM-DD)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()
    
    random.seed(args.seed)
    
    # Date range
    if args.year:
        range_start = date(args.year, 1, 1)
        range_end = date(args.year, 12, 31)
        data_end = date(args.year + 1, 3, 31)  # Allow accounts to run into next year
    elif args.start and args.end:
        range_start = date.fromisoformat(args.start)
        range_end = date.fromisoformat(args.end)
        data_end = range_end + timedelta(days=120)
    else:
        log.error("Specify --year or --start/--end")
        sys.exit(1)
    
    # Load data
    buffer_start = range_start - timedelta(days=LIQUIDITY_DAYS + 10)
    log.info(f"Loading 1m data...")
    all_1m = load_csv(args.file_1m)
    log.info(f"Loaded {len(all_1m):,} 1m bars")
    
    if args.file_5m:
        log.info(f"Loading 5m data...")
        all_5m = load_csv(args.file_5m)
    else:
        log.info("Synthesizing 5m bars...")
        all_5m = synthesize_5m(all_1m)
    log.info(f"{len(all_5m):,} 5m bars")
    
    # Filter to relevant date range
    buf_ts = datetime.combine(buffer_start, datetime.min.time()).replace(tzinfo=pytz.utc).timestamp()
    end_ts = datetime.combine(data_end + timedelta(days=1), datetime.min.time()).replace(tzinfo=pytz.utc).timestamp()
    all_5m = [b for b in all_5m if buf_ts <= b["timestamp"] <= end_ts]
    all_1m = [b for b in all_1m if buf_ts <= b["timestamp"] <= end_ts]
    log.info(f"Filtered: {len(all_5m):,} 5m, {len(all_1m):,} 1m bars")
    
    # Index by session date
    bars_5m_by_date = defaultdict(list)
    bars_1m_by_date = defaultdict(list)
    for b in all_5m:
        bars_5m_by_date[get_session_date(b["timestamp"])].append(b)
    for b in all_1m:
        bars_1m_by_date[get_session_date(b["timestamp"])].append(b)
    
    all_dates = sorted(bars_5m_by_date.keys())
    trading_dates = [d for d in all_dates if d.weekday() < 5]
    
    # Generate staggered start dates within range
    eligible_starts = [d for d in trading_dates if range_start <= d <= range_end]
    if len(eligible_starts) < args.accounts:
        log.warning(f"Only {len(eligible_starts)} trading days available, some accounts will share start dates")
    
    start_dates = sorted(random.sample(eligible_starts, min(args.accounts, len(eligible_starts))))
    # If more accounts than dates, add random duplicates
    while len(start_dates) < args.accounts:
        start_dates.append(random.choice(eligible_starts))
    start_dates.sort()
    
    log.info(f"Launching {args.accounts} accounts from {start_dates[0]} to {start_dates[-1]}")
    
    # Create accounts
    accounts = []
    for i, sd in enumerate(start_dates):
        accounts.append(AccountSim(account_id=i+1, start_date=sd, phase="COMBINE"))
    
    # Track which accounts are active on each day
    results_combine_passed = []
    results_combine_blown = []
    results_xfa_passed = []
    results_xfa_blown = []
    
    # Process day by day
    for day_idx, day in enumerate(trading_dates):
        if day < range_start - timedelta(days=LIQUIDITY_DAYS + 5):
            continue
        
        day_5m = bars_5m_by_date.get(day, [])
        day_1m = bars_1m_by_date.get(day, [])
        if not day_5m or not day_1m:
            continue
        
        # Build liquidity scan bars for this day
        day_pos = all_dates.index(day) if day in all_dates else -1
        if day_pos < 0:
            continue
        scan_start_idx = max(0, day_pos - LIQUIDITY_DAYS)
        scan_dates = all_dates[scan_start_idx:day_pos]
        scan_bars = []
        for sd in scan_dates:
            scan_bars.extend(bars_5m_by_date[sd])
        
        # Find accounts that should trade today
        active_today = [a for a in accounts if a.status == "ACTIVE" and a.start_date <= day]
        if not active_today:
            continue
        
        # Interleave events
        events = [("5m", b) for b in day_5m] + [("1m", b) for b in day_1m]
        events.sort(key=lambda x: x[1]["timestamp"])
        
        # Run each active account through today's data
        for acct in active_today:
            # Fresh engine per account per day
            engine = ICTEngine(account_id=f"SIM-{acct.id}", token="SIM", symbol=INSTRUMENT)
            engine.point_value = POINT_VALUE
            engine.order_callback = acct.on_signal
            engine.trade_log.enabled = False
            engine.max_risk_usd = acct.get_risk()
            engine.max_contracts = acct.get_max_contracts()
            engine.entry_cutoff_minute = 45
            
            if len(scan_bars) > 3:
                engine.scan_historical_liquidity(scan_bars)
            
            acct.days_traded += 1
            
            for tf, bar in events:
                if acct.open_trade and tf == "1m":
                    if acct.check_exit(bar):
                        continue
                if acct.pending_trade and not acct.open_trade:
                    acct.activate_pending()
                if tf == "5m":
                    engine.process_5m_candle(bar)
                elif tf == "1m":
                    engine.process_1m_candle(bar)
            
            # EOD close
            if acct.open_trade and day_1m:
                acct.force_close_eod(day_1m[-1])
            
            # Check status after today
            if acct.status == "PASSED_COMBINE":
                results_combine_passed.append({
                    "id": acct.id, "start": str(acct.start_date),
                    "days": acct.days_traded, "trades": acct.combine_trades,
                    "balance": round(acct.balance, 2),
                })
                acct.promote_to_xfa()
            
            elif acct.status == "PASSED_XFA":
                acct.xfa_days = acct.days_traded - acct.combine_days
                results_xfa_passed.append({
                    "id": acct.id, "start": str(acct.start_date),
                    "combine_days": acct.combine_days,
                    "xfa_days": acct.xfa_days,
                    "total_days": acct.days_traded,
                    "combine_trades": acct.combine_trades,
                    "xfa_trades": acct.xfa_trades,
                    "xfa_balance": round(acct.balance, 2),
                })
                acct.status = "DONE"
            
            elif acct.status == "BLOWN":
                if acct.phase == "COMBINE":
                    results_combine_blown.append({
                        "id": acct.id, "start": str(acct.start_date),
                        "days": acct.days_traded, "trades": acct.combine_trades,
                        "balance": round(acct.balance, 2),
                    })
                else:
                    acct.xfa_days = acct.days_traded - acct.combine_days
                    results_xfa_blown.append({
                        "id": acct.id, "start": str(acct.start_date),
                        "combine_days": acct.combine_days,
                        "xfa_days": acct.xfa_days,
                        "total_days": acct.days_traded,
                        "combine_trades": acct.combine_trades,
                        "xfa_trades": acct.xfa_trades,
                        "xfa_balance": round(acct.balance, 2),
                    })
        
        # Progress
        active_count = sum(1 for a in accounts if a.status == "ACTIVE")
        done_count = sum(1 for a in accounts if a.status in ("DONE", "BLOWN"))
        not_started = sum(1 for a in accounts if a.start_date > day and a.status == "ACTIVE")
        if day_idx % 10 == 0 or active_count == 0:
            pct = done_count / args.accounts * 100
            print(f"\r  {day} | Active: {active_count - not_started} | Not started: {not_started} | "
                  f"Done: {done_count} | {pct:.0f}%", end="", flush=True)
        
        if active_count == 0 or (active_count == not_started and day > range_end + timedelta(days=90)):
            break
    
    print()
    
    # Check for still-active accounts (ran out of data)
    still_active = [a for a in accounts if a.status == "ACTIVE" and a.start_date <= trading_dates[-1]]
    
    # =====================================================================
    # REPORT
    # =====================================================================
    n = args.accounts

    # Count from account objects directly — single source of truth
    n_combine_passed = sum(1 for a in accounts if a.combine_trades > 0 and a.phase in ("XFA",) or a.status in ("DONE", "BLOWN") and a.combine_days > 0)
    # Simpler: anyone who ever made it to XFA phase passed combine
    n_combine_passed = len(results_combine_passed)
    n_combine_blown = len(results_combine_blown)
    n_xfa_entered = n_combine_passed  # Everyone who passed combine entered XFA
    n_xfa_passed = len(results_xfa_passed)
    n_xfa_blown = len(results_xfa_blown)
    n_xfa_still_active = n_xfa_entered - n_xfa_passed - n_xfa_blown
    n_still_active = len(still_active)
    
    print(f"\n{'='*70}")
    print(f"  FUNDED SIMULATION — {n} accounts | {range_start} → {range_end}")
    print(f"{'='*70}")
    
    print(f"\n  COMBINE PHASE")
    print(f"  {'─'*50}")
    n_combine_total = n_combine_passed + n_combine_blown
    print(f"  Passed:     {n_combine_passed:>5} / {n} ({n_combine_passed/n*100:.1f}%)")
    print(f"  Blown:      {n_combine_blown:>5} / {n} ({n_combine_blown/n*100:.1f}%)")
    if n_still_active > 0:
        still_combine = sum(1 for a in still_active if a.phase == "COMBINE")
        if still_combine > 0:
            print(f"  Still going:{still_combine:>5} ({still_combine/n*100:.1f}%)")
    
    if results_combine_passed:
        avg_days = sum(r["days"] for r in results_combine_passed) / len(results_combine_passed)
        avg_trades = sum(r["trades"] for r in results_combine_passed) / len(results_combine_passed)
        print(f"  Avg days to pass: {avg_days:.1f}")
        print(f"  Avg trades:       {avg_trades:.1f}")
    
    if n_combine_blown > 0:
        avg_blown_days = sum(r["days"] for r in results_combine_blown) / len(results_combine_blown)
        print(f"  Avg days to blow: {avg_blown_days:.1f}")
    
    print(f"\n  XFA PHASE (of {n_combine_passed} who passed combine)")
    print(f"  {'─'*50}")
    if n_combine_passed > 0:
        print(f"  Hit ${XFA_TARGET:,}:{n_xfa_passed:>5} ({n_xfa_passed/n_combine_passed*100:.1f}% of combine passers)")
        print(f"  Blown:      {n_xfa_blown:>5} ({n_xfa_blown/n_combine_passed*100:.1f}% of combine passers)")
        if n_xfa_still_active > 0:
            print(f"  Still going:{n_xfa_still_active:>5} ({n_xfa_still_active/n_combine_passed*100:.1f}% — ran out of data)")
        
        if results_xfa_passed:
            avg_xfa_days = sum(r["xfa_days"] for r in results_xfa_passed) / len(results_xfa_passed)
            avg_xfa_trades = sum(r["xfa_trades"] for r in results_xfa_passed) / len(results_xfa_passed)
            avg_total = sum(r["total_days"] for r in results_xfa_passed) / len(results_xfa_passed)
            print(f"  Avg XFA days to ${XFA_TARGET:,}: {avg_xfa_days:.1f}")
            print(f"  Avg XFA trades:       {avg_xfa_trades:.1f}")
            print(f"  Avg total days:       {avg_total:.1f} (combine + XFA)")
        
        if results_xfa_blown:
            avg_xfa_blown = sum(r["xfa_days"] for r in results_xfa_blown) / len(results_xfa_blown)
            print(f"  Avg XFA days to blow: {avg_xfa_blown:.1f}")
    
    print(f"\n  OVERALL")
    print(f"  {'─'*50}")
    print(f"  Combine → ${XFA_TARGET:,} XFA: {n_xfa_passed:>5} / {n} ({n_xfa_passed/n*100:.1f}%)")
    print(f"  Blown (any phase):  {n_combine_blown + n_xfa_blown:>5} / {n} ({(n_combine_blown + n_xfa_blown)/n*100:.1f}%)")
    
    if n_xfa_passed > 0:
        payout = min(6000, XFA_TARGET * 0.50)
        take_home = payout * 0.65
        total_cost = n / n_xfa_passed * 150  # Avg combine cost per success
        net = take_home - total_cost
        print(f"\n  ECONOMICS (per successful account)")
        print(f"  {'─'*50}")
        print(f"  Payout at ${XFA_TARGET:,}:    ${payout:,.0f}")
        print(f"  After fees+tax:     ${take_home:,.0f}")
        print(f"  Avg combines/success: {n/n_xfa_passed:.1f}x (${n/n_xfa_passed*150:,.0f} in fees)")
        print(f"  Net per success:    ${net:,.0f}")
    
    print(f"\n{'='*70}")
    
    # Save detailed results
    output = {
        "config": {
            "accounts": n, "range_start": str(range_start), "range_end": str(range_end),
            "seed": args.seed,
        },
        "summary": {
            "combine_passed": n_combine_passed, "combine_blown": n_combine_blown,
            "xfa_passed": n_xfa_passed, "xfa_blown": n_xfa_blown,
            "still_active": n_still_active,
            "overall_success_pct": round(n_xfa_passed / n * 100, 2),
        },
        "xfa_successes": results_xfa_passed,
        "xfa_failures": results_xfa_blown,
        "combine_failures": results_combine_blown,
    }
    
    with open("funded_sim_results.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Results saved to funded_sim_results.json")


if __name__ == "__main__":
    main()
