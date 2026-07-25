"""
backtest_databento.py — Backtest ICT bot using Databento ES futures data.

Supports multiple Databento export formats:
  - CSV from Databento portal/API (columns: ts_event, open, high, low, close, volume, ...)
  - Parquet files (same schema)
  - Pre-converted CSV with standard OHLCV columns

ES and MES have identical price action — only contract size differs.
This uses ES data but simulates MES position sizing ($5/point).

Usage:
  # If you have separate 1m and 5m files:
  python backtest_databento.py --file-1m data/es_1m.csv --file-5m data/es_5m.csv

  # If you only have 1m data (5m bars will be synthesized):
  python backtest_databento.py --file-1m data/es_1m.csv

  # Specify date range (default: last 252 trading days / ~1 year):
  python backtest_databento.py --file-1m data/es_1m.csv --start 2025-03-17 --end 2026-03-17

  # Databento files often use .csv.zst compression:
  python backtest_databento.py --file-1m data/es_ohlcv_1m.csv.zst
"""

import os
import sys
import logging
import json
import argparse
import math
from datetime import datetime, timedelta, date
from collections import defaultdict

import pandas as pd
import pytz

# Engine import — expects engine.py in same directory or on PYTHONPATH
from engine import ICTEngine
# V2/V3/V4 engine variants were removed (research showed no real-time edge).

# =====================================================================
# CONFIG
# =====================================================================
INSTRUMENT      = "MES"
LIQUIDITY_DAYS  = 5       # Days of data used to build the liquidity map before trading
POINT_VALUE_MES = 5.00    # MES = $5/point
POINT_VALUE_ES  = 50.00   # ES = $50/point
POINT_VALUE     = POINT_VALUE_MES  # Default (overridden dynamically)

TZ = pytz.timezone("America/Chicago")

# =====================================================================
# FEES & SLIPPAGE (TopstepX)
# =====================================================================
FEE_MES_RT = 0.74          # MES round-turn fee per contract
FEE_ES_RT  = 2.80          # ES round-turn fee per contract
TICK_SIZE  = 0.25           # ES/MES tick size in points

# Realistic slippage model:
# - Entry: LIMIT order at trigger price → 0 slippage (fills at price or not at all)
#   The trigger price is the 1m candle close — price just printed there,
#   so the limit fills almost instantly during RTH on ES.
# - TP exit: LIMIT order → 0 slippage
# - SL exit: STOP MARKET order → size-based slippage (unavoidable)
#
# Size-based slippage on SL exits only (in ticks):
#   1-50 contracts:    0.5 tick (50% of the time 0, 50% of the time 1)
#   51-200 contracts:  1.0 tick (almost always 1 tick on stop)
#   201-500 contracts: 1.5 ticks (some market impact)
#   500+ contracts:    2.0 ticks (significant market impact)
SLIPPAGE_TIERS = [
    # (max_qty, sl_ticks)
    (50,   0.5),
    (200,  1.0),
    (500,  1.5),
    (9999, 2.0),
]

# =====================================================================
# INSTRUMENT MIGRATION THRESHOLDS
# =====================================================================
# MES: $5/pt, max ~400 contracts before visibility issues
# ES:  $50/pt, equivalent to 10x MES, better for larger size
ES_SWITCH_BALANCE    = 80_000       # Switch MES → ES at this balance
ES_CEILING_RISK      = 250_000      # Max risk/session on ES before needing multi-instrument
# Multi-instrument: ES 40%, NQ 30%, YM 15%, RTY 15%
MULTI_SWITCH_BALANCE = 5_000_000    # Switch to multi-instrument above this

# =====================================================================
# ACCOUNT RULES — Combine & XFA
# =====================================================================
COMBINE_MAX_CONTRACTS = 50
COMBINE_RISK = 750
COMBINE_MLL = -2_000  # Maximum Loss Limit (trailing, caps at 0)

# XFA contract caps by balance
XFA_CONTRACT_TIERS = [
    # (balance_threshold, max_contracts)
    (2_000, 50),
    (1_500, 30),
    (0,     20),
]

# XFA risk scaling by balance (aggressive — justified by 60%+ WR with 9:15 cutoff)
XFA_RISK_TIERS = [
    # (balance_threshold, risk_per_trade)
    (5_000, 1_250),
    (3_000, 1_000),
    (1_500,   875),
    (0,       750),
]

XFA_MLL = -2_000  # Trailing, caps at 0

def get_xfa_max_contracts(balance):
    """Return the XFA contract cap for the current balance."""
    for threshold, cap in XFA_CONTRACT_TIERS:
        if balance >= threshold:
            return cap
    return 20

def get_xfa_risk(balance):
    """Return the XFA risk per trade for the current balance."""
    for threshold, risk in XFA_RISK_TIERS:
        if balance >= threshold:
            return risk
    return 750

# =====================================================================
# LOGGING
# =====================================================================
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("ICTEngine").setLevel(logging.WARNING)  # Quieter for long runs
log = logging.getLogger("Backtest")
log.setLevel(logging.INFO)

# =====================================================================
# DATA LOADING — Handles multiple Databento export formats
# =====================================================================
def load_databento_csv(filepath, date_start=None, date_end=None):
    """
    Load a Databento OHLCV CSV/Parquet and return a sorted list of candle dicts.
    
    Databento CSV columns (ohlcv-1m / ohlcv-5m schema):
      ts_event, rtype, publisher_id, instrument_id, open, high, low, close, volume, symbol
    
    Also handles:
      - .csv.zst (zstd compressed CSV)
      - .parquet files
      - Generic CSV with columns: datetime/timestamp/date, open, high, low, close, volume
      - Yahoo Finance format (multi-header)
    
    Args:
      date_start: Optional ISO date string to filter from (e.g., "2025-01-01")
      date_end: Optional ISO date string to filter to (e.g., "2026-03-17")
    """
    ext = filepath.lower()
    
    # For very large files, use chunked reading with date filtering
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    use_chunked = file_size_mb > 500 and date_start and ext.endswith('.csv')
    
    if ext.endswith('.parquet'):
        df = pd.read_parquet(filepath)
    elif ext.endswith('.csv.zst') or ext.endswith('.csv.zstd'):
        df = pd.read_csv(filepath, compression='zstd')
    elif ext.endswith('.csv.gz'):
        df = pd.read_csv(filepath, compression='gzip')
    elif use_chunked:
        # Chunked reading for huge CSVs — only keep rows in our date range
        log.info(f"  Large file ({file_size_mb:,.0f} MB) — using chunked reading...")
        chunks = []
        for chunk in pd.read_csv(filepath, chunksize=500_000):
            chunk.columns = [c.strip().lower().replace(' ', '_') for c in chunk.columns]
            ts_col = 'ts_event' if 'ts_event' in chunk.columns else chunk.columns[0]
            # Quick string-based date filter before full parsing
            if date_start:
                chunk = chunk[chunk[ts_col] >= date_start]
            if date_end:
                chunk = chunk[chunk[ts_col] <= date_end + "T23:59:59Z"]
            if len(chunk) > 0:
                chunks.append(chunk)
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.DataFrame()
        log.info(f"  Chunked load complete: {len(df):,} rows in date range")
    else:
        # Try standard CSV first
        try:
            df = pd.read_csv(filepath)
        except Exception:
            # Yahoo Finance format with multi-row header
            df = pd.read_csv(filepath, header=0, skiprows=[1, 2])
    
    # Normalize column names
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    
    # --- Identify and parse the timestamp column ---
    ts_col = None
    for candidate in ['ts_event', 'timestamp', 'datetime', 'date', 'time', 'ts']:
        if candidate in df.columns:
            ts_col = candidate
            break
    
    if ts_col is None:
        # Check if index is a datetime
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            ts_col = df.columns[0]
        else:
            # First column is likely the timestamp
            ts_col = df.columns[0]
    
    # Parse timestamps
    if df[ts_col].dtype == 'int64' or df[ts_col].dtype == 'uint64':
        # Databento uses nanosecond epoch timestamps
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
    
    # Ensure numeric OHLCV
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df.dropna(subset=['close'], inplace=True)
    
    # ---------------------------------------------------------------
    # DATABENTO MULTI-CONTRACT HANDLING
    # The Databento ES dataset includes ALL active contracts per bar
    # (e.g., ESM0, ESU0) plus calendar spreads (ESM0-ESU0).
    # We need to select just the front-month (highest volume) contract
    # for each timestamp to get a continuous series.
    # ---------------------------------------------------------------
    if 'symbol' in df.columns:
        original_len = len(df)
        
        # 1. Drop spread/combo symbols (contain "-")
        df = df[~df['symbol'].str.contains('-', na=False)].copy()
        
        # 2. Drop rows with negative or zero prices (spreads that slipped through)
        df = df[df['close'] > 0].copy()
        
        # 3. For each timestamp, keep only the contract with highest volume
        #    This naturally selects the front-month contract
        df = df.sort_values(['_ts', 'volume'], ascending=[True, False])
        df = df.drop_duplicates(subset='_ts', keep='first')
        
        log.info(f"  Filtered {original_len:,} rows → {len(df):,} (front-month only, spreads removed)")
        
        # Log which contracts were selected
        if 'symbol' in df.columns:
            contract_counts = df['symbol'].value_counts()
            top_contracts = contract_counts.head(8)
            log.info(f"  Contracts used: {', '.join(f'{sym}({cnt:,})' for sym, cnt in top_contracts.items())}")
    
    # Databento prices are sometimes in fixed-point (1e-9 scaling) — detect and fix
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


def synthesize_5m_from_1m(candles_1m):
    """
    Aggregate 1-minute candles into 5-minute candles.
    Groups by 5-minute intervals aligned to the hour (e.g., :00, :05, :10, ...).
    """
    df = pd.DataFrame(candles_1m)
    df['dt'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    df.set_index('dt', inplace=True)
    
    # Resample to 5-minute bars
    ohlcv = df.resample('5min').agg({
        'timestamp': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }).dropna(subset=['close'])
    
    candles_5m = []
    for _, row in ohlcv.iterrows():
        candles_5m.append({
            "timestamp": float(row['timestamp']),
            "open": float(row['open']),
            "high": float(row['high']),
            "low": float(row['low']),
            "close": float(row['close']),
            "volume": float(row['volume']),
        })
    
    return candles_5m


# =====================================================================
# RISK SCALING — Aggressive v3 permanent ratchet ladder
# Once balance hits a threshold, risk steps up and never comes back down.
# Lower entry thresholds + steeper jumps = faster compounding engagement.
# =====================================================================
BASE_RISK = 750.0

RISK_LADDER = [
    # (balance_threshold, new_risk)
    # Aggressive v3: threshold doubles, risk grows sub-linearly
    (10_000,              2_000),
    (20_000,              3_500),
    (40_000,              6_000),
    (80_000,             10_000),
    (160_000,            16_000),
    (320_000,            25_000),
    (640_000,            40_000),
    (1_280_000,          64_000),
    (2_560_000,         100_000),
    (5_120_000,         160_000),
    (10_240_000,        250_000),
    (20_480_000,        400_000),
    (40_960_000,        640_000),
    (81_920_000,      1_000_000),
    (163_840_000,     1_600_000),
    (327_680_000,     2_500_000),
    (655_360_000,     4_000_000),
    (1_310_720_000,   6_400_000),
]

# =====================================================================
# PAYOUT STRUCTURE — 5% of balance/mo; above $1M only 5% of excess
# =====================================================================
PAYOUT_THRESHOLD = 1_000_000.0  # Above this, only pay 5% of excess
PAYOUT_PCT = 0.05               # 5%

# =====================================================================
# KELLY CRITERION — f* = (p*b - q) / b
# With p=55.3%, b=1.57 → Full Kelly = 26.9%
# Half Kelly is standard practice for real trading
# =====================================================================
KELLY_WIN_RATE = 0.553
KELLY_WIN_LOSS_RATIO = 1.57
KELLY_FULL = (KELLY_WIN_RATE * KELLY_WIN_LOSS_RATIO - (1 - KELLY_WIN_RATE)) / KELLY_WIN_LOSS_RATIO
KELLY_FRACTIONS = {
    "full":    KELLY_FULL,          # ~26.9%
    "half":    KELLY_FULL / 2,      # ~13.4%
    "quarter": KELLY_FULL / 4,      # ~6.7%
    "third":   KELLY_FULL / 3,      # ~9.0%
}
KELLY_MIN_RISK = 750.0   # Floor — never risk less than base
KELLY_MAX_RISK = 500_000  # Ceiling — market depth / ruin protection

def compute_payout(balance):
    """5% of balance/mo; above $1M only 5% of the excess."""
    if balance <= 0:
        return 0.0
    if balance > PAYOUT_THRESHOLD:
        return (balance - PAYOUT_THRESHOLD) * PAYOUT_PCT
    return balance * PAYOUT_PCT


# =====================================================================
# TRADE SIMULATOR
# =====================================================================
class TradeSimulator:
    def __init__(self, point_value=5.0, risk_scaling=False, payouts=False, break_even=False, fees_and_slippage=False, kelly=None):
        self.point_value = point_value
        self.risk_scaling = risk_scaling
        self.payouts = payouts
        self.break_even = break_even
        self.fees_and_slippage = fees_and_slippage
        self.kelly = kelly  # None, "full", "half", "quarter", "third"
        self.pending_trade = None
        self.trades = []
        self.open_trade = None
        # Balance tracking for risk scaling
        self.balance = 0.0
        self.peak_balance = 0.0       # High water mark — drives ratchet
        self.current_risk = BASE_RISK  # Locked risk level
        self.balance_history = []      # [(trade_index, balance, risk_used)]
        # Payout tracking
        self.total_paid_out = 0.0
        self.payout_log = []           # [{"month": ..., "salary": ..., "pct_payout": ..., ...}]
        self.last_payout_month = None
        # Break-even tracking
        self.be_levels = []            # Hourly liquidity levels for BE check
        self.be_triggered = False
        self.be_count = 0              # Stats: how many times BE was triggered
        # Instrument migration tracking
        self.current_instrument = "MES"
        self.instrument_switches = []  # [(trade_num, balance, old, new)]
        self.total_fees = 0.0
        self.total_slippage_cost = 0.0

    def process_month_end(self, current_date):
        """Take a payout if we've crossed into a new month."""
        if not self.payouts:
            return
        month_key = current_date.strftime("%Y-%m")
        if self.last_payout_month == month_key:
            return
        # First trading day of a new month — process payout for the previous month
        if self.last_payout_month is not None and self.balance > 0:
            payout = compute_payout(self.balance)
            payout = min(payout, self.balance * 0.90)  # safety cap: never take more than 90%
            if payout > 0:
                bal_before = round(self.balance, 2)
                self.balance -= payout
                self.total_paid_out += payout
                self.payout_log.append({
                    "month": self.last_payout_month,
                    "balance_before": bal_before,
                    "payout": round(payout, 2),
                    "balance_after": round(self.balance, 2),
                    "total_paid_out": round(self.total_paid_out, 2),
                })
        self.last_payout_month = month_key

    def set_be_levels(self, levels):
        """Set hourly liquidity levels for break-even checking."""
        self.be_levels = levels

    def _update_instrument(self):
        """Check if we should switch instruments based on balance."""
        if self.peak_balance >= ES_SWITCH_BALANCE and self.current_instrument == "MES":
            self.instrument_switches.append((len(self.trades), round(self.balance, 2), "MES", "ES"))
            self.current_instrument = "ES"
            self.point_value = POINT_VALUE_ES
        elif self.peak_balance >= MULTI_SWITCH_BALANCE and self.current_instrument == "ES":
            self.instrument_switches.append((len(self.trades), round(self.balance, 2), "ES", "MULTI"))
            self.current_instrument = "MULTI"
            # MULTI uses ES point value but caps risk differently
            self.point_value = POINT_VALUE_ES

    def _get_fee_per_contract(self):
        """Get round-turn fee for current instrument."""
        if self.current_instrument == "MES":
            return FEE_MES_RT
        return FEE_ES_RT  # ES and MULTI use ES fees

    def _get_sl_slippage_ticks(self, qty):
        """
        Get slippage in ticks for a stop-loss exit based on order size.
        Only applies to SL exits (stop market orders).
        Entry (limit) and TP (limit) have 0 slippage.
        """
        for max_qty, sl_ticks in SLIPPAGE_TIERS:
            if qty <= max_qty:
                return sl_ticks
        return SLIPPAGE_TIERS[-1][1]

    def on_trade_signal(self, side, qty, entry, sl, tp):
        # Recalculate qty for current instrument's point value
        # The engine always calculates with its configured point_value,
        # but when we switch to ES the qty needs to be 1/10th
        actual_qty = qty
        if self.current_instrument in ("ES", "MULTI") and self.point_value == POINT_VALUE_ES:
            # Engine calculated qty for MES ($5/pt), convert to ES ($50/pt)
            actual_qty = max(1, qty // 10)

        self.pending_trade = {
            "side": side, "qty": actual_qty, "entry": entry,
            "sl": sl, "tp": tp, "entry_time": None,
            "risk_at_entry": self.get_current_risk(),
            "instrument": self.current_instrument,
            "original_qty": qty,
        }
        self.be_triggered = False

    def activate_pending(self, candle_time):
        if self.pending_trade:
            self.pending_trade["entry_time"] = candle_time
            self.open_trade = self.pending_trade
            self.pending_trade = None

    def check_exit(self, candle) -> bool:
        if not self.open_trade:
            return False
        t = self.open_trade

        # --- Break-even check: move SL to entry if price hits an hourly level ---
        if self.break_even and not self.be_triggered and self.be_levels:
            for lvl in self.be_levels:
                if t["side"] == 0 and candle["high"] > lvl > t["entry"]:
                    t["sl"] = t["entry"]
                    self.be_triggered = True
                    self.be_count += 1
                    break
                if t["side"] == 1 and candle["low"] < lvl < t["entry"]:
                    t["sl"] = t["entry"]
                    self.be_triggered = True
                    self.be_count += 1
                    break

        hit_sl = hit_tp = False
        if t["side"] == 0:  # Long
            if candle["low"] <= t["sl"]: hit_sl = True
            if candle["high"] >= t["tp"]: hit_tp = True
        else:  # Short
            if candle["high"] >= t["sl"]: hit_sl = True
            if candle["low"] <= t["tp"]: hit_tp = True

        if hit_sl and hit_tp:
            if t["side"] == 0:
                sl_dist = t["entry"] - t["sl"]
                tp_dist = t["tp"] - t["entry"]
            else:
                sl_dist = t["sl"] - t["entry"]
                tp_dist = t["entry"] - t["tp"]
            if sl_dist <= tp_dist:
                hit_tp = False
            else:
                hit_sl = False

        if hit_sl:
            pnl = (t["sl"] - t["entry"]) if t["side"] == 0 else (t["entry"] - t["sl"])
            reason = "BE" if self.be_triggered and abs(pnl) < 0.01 else "SL"
            self._close_trade(candle, t["sl"], pnl, reason)
            return True
        if hit_tp:
            pnl = (t["tp"] - t["entry"]) if t["side"] == 0 else (t["entry"] - t["tp"])
            self._close_trade(candle, t["tp"], pnl, "TP")
            return True
        return False

    def _close_trade(self, candle, exit_price, pnl_pts, reason):
        t = self.open_trade
        ct = datetime.fromtimestamp(candle["timestamp"], pytz.utc).astimezone(TZ)

        # Determine point value for this trade's instrument
        trade_pv = POINT_VALUE_ES if t.get("instrument") in ("ES", "MULTI") else POINT_VALUE_MES
        pnl_usd = pnl_pts * t["qty"] * trade_pv

        # Apply fees and slippage
        slippage_cost = 0.0
        fee_cost = 0.0
        if self.fees_and_slippage:
            # Fees apply to all trades (round-turn per contract)
            fee_per_contract = self._get_fee_per_contract()
            fee_cost = fee_per_contract * t["qty"]
            pnl_usd -= fee_cost
            self.total_fees += fee_cost

            # Slippage only on SL exits (stop market orders)
            # Entry = limit (0 slippage), TP = limit (0 slippage)
            if reason == "SL":
                sl_ticks = self._get_sl_slippage_ticks(t["qty"])
                slippage_pts = sl_ticks * TICK_SIZE
                slippage_cost = slippage_pts * t["qty"] * trade_pv
                pnl_usd -= slippage_cost
                self.total_slippage_cost += slippage_cost

        # Track balance and check for ratchet
        self.balance += pnl_usd
        if self.balance > self.peak_balance:
            self.peak_balance = self.balance
        self._check_ratchet()
        self._update_instrument()

        result = {
            "side": "BUY" if t["side"] == 0 else "SELL",
            "qty": t["qty"], "entry": t["entry"], "exit": exit_price,
            "pnl_pts": round(pnl_pts, 2), "pnl_usd": round(pnl_usd, 2),
            "reason": reason, "entry_time": t["entry_time"],
            "exit_time": ct.strftime("%Y-%m-%d %H:%M"),
            "balance": round(self.balance, 2),
            "risk_used": round(t.get("risk_at_entry", BASE_RISK), 2),
            "instrument": t.get("instrument", "MES"),
            "fees": round(fee_cost, 2),
            "slippage": round(slippage_cost, 2),
        }
        self.trades.append(result)
        self.balance_history.append((len(self.trades), round(self.balance, 2), round(self.current_risk, 2)))
        self.open_trade = None

    def _check_ratchet(self):
        """Check if peak balance has crossed a new threshold. Only ratchets UP."""
        if not self.risk_scaling:
            return
        for threshold, risk in RISK_LADDER:
            if self.peak_balance >= threshold and risk > self.current_risk:
                self.current_risk = risk

    def get_current_risk(self):
        """Get the risk level for the next trade."""
        if self.kelly:
            # Kelly: risk a fixed fraction of current balance
            frac = KELLY_FRACTIONS.get(self.kelly, KELLY_FULL / 2)
            # Use current balance, not peak (Kelly is based on current bankroll)
            bankroll = max(self.balance, 10_000)  # Floor at $10K to avoid tiny bets early
            risk = bankroll * frac
            return max(KELLY_MIN_RISK, min(risk, KELLY_MAX_RISK))
        if self.risk_scaling:
            return self.current_risk
        return BASE_RISK

    def force_close_eod(self, candle):
        if not self.open_trade:
            return
        t = self.open_trade
        pnl = (candle["close"] - t["entry"]) if t["side"] == 0 else (t["entry"] - candle["close"])
        self._close_trade(candle, candle["close"], pnl, "EOD")


# =====================================================================
# MONTHLY BREAKDOWN
# =====================================================================
def compute_monthly_stats(trades):
    """Group trades by month and compute per-month stats."""
    monthly = defaultdict(list)
    for t in trades:
        # Parse exit_time to get month
        dt = datetime.strptime(t["exit_time"], "%Y-%m-%d %H:%M")
        key = dt.strftime("%Y-%m")
        monthly[key].append(t)
    
    stats = []
    for month in sorted(monthly.keys()):
        month_trades = monthly[month]
        wins = [t for t in month_trades if t["pnl_usd"] > 0]
        losses = [t for t in month_trades if t["pnl_usd"] <= 0]
        pnl = sum(t["pnl_usd"] for t in month_trades)
        wr = len(wins) / len(month_trades) * 100 if month_trades else 0
        gp = sum(t["pnl_usd"] for t in wins)
        gl = abs(sum(t["pnl_usd"] for t in losses))
        pf = gp / gl if gl > 0 else float("inf")
        stats.append({
            "month": month,
            "trades": len(month_trades),
            "wins": len(wins),
            "losses": len(losses),
            "pnl": round(pnl, 2),
            "win_rate": round(wr, 1),
            "pf": round(pf, 2),
            "avg_win": round(gp / len(wins), 2) if wins else 0,
            "avg_loss": round(-gl / len(losses), 2) if losses else 0,
        })
    return stats


# =====================================================================
# REPORT
# =====================================================================
def generate_report(trades, start_date, end_date, trading_days_total, no_trade_days, risk_scaling=False, balance_history=None, payout_log=None, total_paid_out=0):
    if not trades:
        print("\n  No trades were generated during the backtest period.")
        return

    wins = [t for t in trades if t["pnl_usd"] > 0]
    losses = [t for t in trades if t["pnl_usd"] <= 0]
    total_pnl = sum(t["pnl_usd"] for t in trades)
    win_rate = len(wins) / len(trades) * 100
    avg_win = sum(t["pnl_usd"] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0

    # Equity curve & drawdown
    equity = peak = max_dd = 0
    equity_curve = []
    dd_curve = []
    for t in trades:
        equity += t["pnl_usd"]
        equity_curve.append(round(equity, 2))
        if equity > peak:
            peak = equity
        dd = peak - equity
        dd_curve.append(round(dd, 2))
        if dd > max_dd:
            max_dd = dd

    gross_profit = sum(t["pnl_usd"] for t in wins)
    gross_loss = abs(sum(t["pnl_usd"] for t in losses))
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Streak analysis
    max_win_streak = max_loss_streak = cur_win = cur_loss = 0
    for t in trades:
        if t["pnl_usd"] > 0:
            cur_win += 1
            cur_loss = 0
            max_win_streak = max(max_win_streak, cur_win)
        else:
            cur_loss += 1
            cur_win = 0
            max_loss_streak = max(max_loss_streak, cur_loss)

    # Average hold time (approximate from entry/exit times)
    hold_times = []
    for t in trades:
        try:
            # entry_time and exit_time are strings like "2025-03-17 09:01"
            et = datetime.strptime(t["entry_time"], "%Y-%m-%d %H:%M")
            xt = datetime.strptime(t["exit_time"], "%Y-%m-%d %H:%M")
            hold_times.append((xt - et).total_seconds() / 60)
        except:
            pass
    avg_hold = sum(hold_times) / len(hold_times) if hold_times else 0

    # Monthly breakdown
    monthly = compute_monthly_stats(trades)

    # Day-of-week analysis
    dow_stats = defaultdict(lambda: {"trades": 0, "pnl": 0, "wins": 0})
    for t in trades:
        try:
            dt = datetime.strptime(t["exit_time"], "%Y-%m-%d %H:%M")
            day_name = dt.strftime("%A")
            dow_stats[day_name]["trades"] += 1
            dow_stats[day_name]["pnl"] += t["pnl_usd"]
            if t["pnl_usd"] > 0:
                dow_stats[day_name]["wins"] += 1
        except:
            pass

    # Long vs Short breakdown
    longs = [t for t in trades if t["side"] == "BUY"]
    shorts = [t for t in trades if t["side"] == "SELL"]
    long_pnl = sum(t["pnl_usd"] for t in longs)
    short_pnl = sum(t["pnl_usd"] for t in shorts)
    long_wr = sum(1 for t in longs if t["pnl_usd"] > 0) / len(longs) * 100 if longs else 0
    short_wr = sum(1 for t in shorts if t["pnl_usd"] > 0) / len(shorts) * 100 if shorts else 0

    trade_rate = len(trades) / trading_days_total * 100 if trading_days_total > 0 else 0

    # --- Print Report ---
    print("\n" + "=" * 70)
    print(f"  ICT BOT BACKTEST — {INSTRUMENT} | {start_date} → {end_date}")
    print("=" * 70)
    print(f"  Trading Days:    {trading_days_total} ({len(trades)} trades, {trade_rate:.0f}% hit rate)")
    print(f"  No-Trade Days:   {no_trade_days}")
    print(f"  ───────────────────────────────────────────")
    print(f"  Total Trades:    {len(trades)}")
    print(f"  Wins:            {len(wins)}  ({win_rate:.1f}%)")
    print(f"  Losses:          {len(losses)}")
    print(f"  ───────────────────────────────────────────")
    print(f"  Net P&L:         ${total_pnl:+,.2f}")
    print(f"  Gross Profit:    ${gross_profit:+,.2f}")
    print(f"  Gross Loss:      ${gross_loss:,.2f}")
    print(f"  Profit Factor:   {pf:.2f}")
    print(f"  ───────────────────────────────────────────")
    print(f"  Avg Win:         ${avg_win:+,.2f}")
    print(f"  Avg Loss:        ${avg_loss:+,.2f}")
    print(f"  Win:Loss Ratio:  {abs(avg_win/avg_loss):.2f}:1" if avg_loss != 0 else "  Win:Loss Ratio:  N/A")
    print(f"  Max Drawdown:    ${max_dd:,.2f}")
    print(f"  Return/DD:       {total_pnl/max_dd:.1f}x" if max_dd > 0 else "  Return/DD:       N/A")
    print(f"  ───────────────────────────────────────────")
    print(f"  Max Win Streak:  {max_win_streak}")
    print(f"  Max Loss Streak: {max_loss_streak}")
    print(f"  Avg Hold Time:   {avg_hold:.0f} min")
    print(f"  ───────────────────────────────────────────")
    print(f"  Longs:           {len(longs)} trades, ${long_pnl:+,.2f}, {long_wr:.0f}% WR")
    print(f"  Shorts:          {len(shorts)} trades, ${short_pnl:+,.2f}, {short_wr:.0f}% WR")

    # Break-even exit stats
    be_trades = [t for t in trades if t.get("reason") == "BE"]
    if be_trades:
        be_saved = sum(1 for t in losses if t.get("reason") != "BE")  # Losses that weren't converted to BE
        print(f"  ───────────────────────────────────────────")
        print(f"  Break-Even Exits: {len(be_trades)} (losses converted to scratches)")
        original_losses = len([t for t in trades if t["pnl_usd"] <= 0])
        print(f"  Remaining Losses: {original_losses}")

    # Fee & slippage stats
    total_fees_paid = sum(t.get("fees", 0) for t in trades)
    total_slippage_paid = sum(t.get("slippage", 0) for t in trades)
    if total_fees_paid > 0 or total_slippage_paid > 0:
        print(f"  ───────────────────────────────────────────")
        print(f"  Total Fees:      ${total_fees_paid:,.2f}")
        print(f"  Total Slippage:  ${total_slippage_paid:,.2f}")
        print(f"  Total Costs:     ${total_fees_paid + total_slippage_paid:,.2f}")
        pnl_before_costs = total_pnl + total_fees_paid + total_slippage_paid
        cost_pct = (total_fees_paid + total_slippage_paid) / pnl_before_costs * 100 if pnl_before_costs > 0 else 0
        print(f"  Costs as % P&L:  {cost_pct:.1f}%")

    # Instrument migration
    instruments_used = set(t.get("instrument", "MES") for t in trades)
    if len(instruments_used) > 1:
        print(f"  ───────────────────────────────────────────")
        for inst in sorted(instruments_used):
            inst_trades = [t for t in trades if t.get("instrument") == inst]
            inst_pnl = sum(t["pnl_usd"] for t in inst_trades)
            print(f"  {inst:>5}: {len(inst_trades)} trades, ${inst_pnl:+,.2f}")

    print("=" * 70)

    # Monthly table
    print(f"\n  {'Month':<10} {'Trades':>6} {'Wins':>5} {'WR%':>6} {'PF':>6} {'P&L':>12}")
    print(f"  {'─'*52}")
    for m in monthly:
        pf_str = f"{m['pf']:.2f}" if m['pf'] != float('inf') else "∞"
        print(f"  {m['month']:<10} {m['trades']:>6} {m['wins']:>5} {m['win_rate']:>5.1f}% {pf_str:>6} ${m['pnl']:>+10,.2f}")

    # Day-of-week table
    print(f"\n  {'Day':<12} {'Trades':>6} {'WR%':>6} {'P&L':>12}")
    print(f"  {'─'*40}")
    for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
        if day in dow_stats:
            d = dow_stats[day]
            wr = d["wins"] / d["trades"] * 100 if d["trades"] > 0 else 0
            print(f"  {day:<12} {d['trades']:>6} {wr:>5.0f}% ${d['pnl']:>+10,.2f}")

    # Risk scaling table
    if balance_history and len(balance_history) > 0:
        print(f"\n  Risk Scaling Ladder (balance → risk per trade)")
        print(f"  {'─'*60}")
        # Show at key milestones
        shown = set()
        for trade_idx, bal, risk in balance_history:
            # Show every 25th trade and the first/last
            if trade_idx == 1 or trade_idx == len(balance_history) or trade_idx % 25 == 0:
                if trade_idx not in shown:
                    mult = risk / BASE_RISK
                    print(f"  Trade #{trade_idx:<4} Balance=${bal:>+12,.2f} → Risk=${risk:>8,.2f} ({mult:.2f}x base)")
                    shown.add(trade_idx)
        final_bal, final_risk = balance_history[-1][1], balance_history[-1][2]
        print(f"  {'─'*60}")
        print(f"  Final: Balance=${final_bal:>+12,.2f} | Risk=${final_risk:>8,.2f} ({final_risk/BASE_RISK:.2f}x base)")

    # Payout table
    if payout_log:
        print(f"\n  Monthly Payouts ({PAYOUT_PCT*100:.0f}% of balance; above ${PAYOUT_THRESHOLD:,.0f} only {PAYOUT_PCT*100:.0f}% of excess)")
        print(f"  {'─'*76}")
        print(f"  {'Month':<10} {'Balance':>14} {'Payout':>12} {'Paid Out':>14}")
        print(f"  {'─'*76}")
        for p in payout_log:
            print(f"  {p['month']:<10} ${p['balance_before']:>12,.2f} ${p['payout']:>10,.0f} ${p['total_paid_out']:>12,.0f}")
        print(f"  {'─'*76}")
        final_balance = trades[-1].get("balance", 0) if trades else 0
        print(f"  Total Paid Out:    ${total_paid_out:>14,.2f}")
        print(f"  Remaining Balance: ${final_balance:>14,.2f}")
        print(f"  Total Value:       ${total_paid_out + final_balance:>14,.2f}")

    # Save results
    results = {
        "config": {
            "instrument": INSTRUMENT,
            "point_value": POINT_VALUE,
            "liquidity_days": LIQUIDITY_DAYS,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "risk_scaling": risk_scaling,
        },
        "summary": {
            "total": len(trades), "wins": len(wins), "losses": len(losses),
            "pnl": round(total_pnl, 2), "win_rate": round(win_rate, 2),
            "pf": round(pf, 2) if pf != float('inf') else 999,
            "max_dd": round(max_dd, 2),
            "return_dd": round(total_pnl / max_dd, 2) if max_dd > 0 else 0,
            "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
            "max_win_streak": max_win_streak, "max_loss_streak": max_loss_streak,
            "avg_hold_min": round(avg_hold, 1),
            "trading_days": trading_days_total,
            "no_trade_days": no_trade_days,
            "trade_rate_pct": round(trade_rate, 1),
            "long_count": len(longs), "short_count": len(shorts),
            "long_pnl": round(long_pnl, 2), "short_pnl": round(short_pnl, 2),
            "long_wr": round(long_wr, 1), "short_wr": round(short_wr, 1),
        },
        "trades": trades,
        "equity_curve": equity_curve,
        "drawdown_curve": dd_curve,
        "monthly": monthly,
        "day_of_week": {day: dict(dow_stats[day]) for day in dow_stats},
    }

    if risk_scaling and balance_history:
        results["balance_history"] = [
            {"trade": t, "balance": b, "risk": r} for t, b, r in balance_history
        ]

    if payout_log:
        results["payouts"] = payout_log
        results["summary"]["total_paid_out"] = round(total_paid_out, 2)
        final_balance = trades[-1].get("balance", 0) if trades else 0
        results["summary"]["remaining_balance"] = round(final_balance, 2)
        results["summary"]["total_value"] = round(total_paid_out + final_balance, 2)

    out_file = "backtest_results_replay.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Results saved to {out_file}")

    return results


# =====================================================================
# MAIN
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Backtest ICT bot with Databento ES data")
    parser.add_argument("--file-1m", required=True, help="Path to 1-minute OHLCV file (CSV, Parquet, or .csv.zst)")
    parser.add_argument("--file-5m", default=None, help="Path to 5-minute OHLCV file (optional; synthesized from 1m if omitted)")
    parser.add_argument("--start", default=None, help="Start date (YYYY-MM-DD). Default: 1 year before end")
    parser.add_argument("--end", default=None, help="End date (YYYY-MM-DD). Default: last date in data")
    parser.add_argument("--liquidity-days", type=int, default=LIQUIDITY_DAYS, help="Days for initial liquidity scan")
    parser.add_argument("--verbose", action="store_true", help="Show per-trade engine logs")
    parser.add_argument("--risk-scaling", action="store_true", help="Enable balance-based risk scaling (risk grows with equity)")
    parser.add_argument("--payouts", action="store_true", help="Take 10%% payout of account balance at end of each month")
    parser.add_argument("--break-even", action="store_true", help="Move SL to entry when price hits hourly liquidity level past entry")
    parser.add_argument("--fees", action="store_true", help="Apply TopstepX commission fees and 1-tick slippage per side")
    parser.add_argument("--kelly", choices=["full", "half", "quarter", "third"], default=None,
                        help="Use Kelly criterion sizing instead of fixed ladder (full/half/quarter/third)")
    parser.add_argument("--shuffle", action="store_true",
                        help="Shuffle trading days randomly (OOS test — removes sequential bias)")
    parser.add_argument("--oos-pct", type=float, default=None,
                        help="Only trade a random X%% of days (e.g., --oos-pct 50 for 50%%). Combines with --shuffle.")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for --shuffle/--oos-pct (reproducible results)")
    parser.add_argument("--max-contracts", type=int, default=None,
                        help="Cap position size at N contracts. Trades are clamped, not skipped.")
    parser.add_argument("--combine", action="store_true",
                        help="Simulate TopstepX Combine rules: 50 contract cap, $-2,000 MLL")
    parser.add_argument("--xfa", action="store_true",
                        help="Simulate XFA rules: dynamic contract cap (20/30/50 by balance), $-2,000 MLL")
    parser.add_argument("--cutoff", type=int, default=None,
                        help="Entry cutoff minute past 9:00 CT (e.g. 15 for 9:15, 45 for 9:45). Overrides mode default.")
    # Bar delivery timing. DEFAULT is realistic ("live delivery"): 1m bars
    # arrive at their close (ts+60s), 5m bars at their close (ts+300s), so the
    # engine never sees a 5m bar's full OHLC before the 1m bars that formed it.
    # This is what the live bot actually experiences via the TopstepX websocket.
    #
    # --lookahead-mode restores the OLD biased behavior (bars pre-sorted by
    # open timestamp, 5m-before-1m), which lets the engine "see the future" of
    # the 5m bar at its open. It produces inflated, unrealistic results and is
    # kept ONLY for demonstrating the size of the look-ahead bias. Do not trust
    # numbers produced with this flag.
    parser.add_argument("--lookahead-mode", action="store_true",
                        help="Restore the old look-ahead-biased bar ordering (pre-sorted by open "
                             "timestamp). Produces UNREALISTIC inflated results — for bias demonstration "
                             "only. Default is realistic live-delivery ordering.")
    parser.add_argument("--engine", choices=["v1"], default="v1",
                        help="Engine version. v1: original ICT engine. Backtest defaults to realistic "
                             "live-delivery bar ordering; pass --lookahead-mode to see the old biased "
                             "numbers. v2/v3/v4 variants were removed after research showed no "
                             "real-time edge.")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger("ICTEngine").setLevel(logging.INFO)

    # ----- 1. Compute date range early so we can pass it to the loader -----
    # (For large files, the loader uses this to skip rows outside the range)
    if args.end:
        end_date_hint = args.end
    else:
        end_date_hint = None  # Will use last date in data
    
    if args.start:
        start_date_hint = args.start
    else:
        # Default ~1 year back from end (or today if no end specified)
        if end_date_hint:
            _end = date.fromisoformat(end_date_hint)
        else:
            _end = date.today()
        start_date_hint = str(_end - timedelta(days=365))
    
    # Include extra buffer for liquidity scan
    _buffer_date = date.fromisoformat(start_date_hint) - timedelta(days=args.liquidity_days + 10)
    load_start = str(_buffer_date)
    load_end = end_date_hint  # None means "to end of file"

    # ----- 2. Load data -----
    log.info(f"Loading 1m data from {args.file_1m}...")
    bars_1m = load_databento_csv(args.file_1m, date_start=load_start, date_end=load_end)
    log.info(f"Loaded {len(bars_1m):,} 1m bars")

    if args.file_5m:
        log.info(f"Loading 5m data from {args.file_5m}...")
        bars_5m = load_databento_csv(args.file_5m, date_start=load_start, date_end=load_end)
        log.info(f"Loaded {len(bars_5m):,} 5m bars")
    else:
        log.info("Synthesizing 5m bars from 1m data...")
        bars_5m = synthesize_5m_from_1m(bars_1m)
        log.info(f"Synthesized {len(bars_5m):,} 5m bars")

    # ----- 3. Determine exact date range -----
    def _session_date(ts):
        """Quick session date assignment for date range discovery."""
        ct = datetime.fromtimestamp(ts, pytz.utc).astimezone(TZ)
        if ct.hour >= 17:
            return (ct + timedelta(days=1)).date()
        return ct.date()
    
    all_dates_5m = sorted(set(_session_date(b["timestamp"]) for b in bars_5m))

    if args.end:
        end_date = date.fromisoformat(args.end)
    else:
        end_date = all_dates_5m[-1]

    if args.start:
        start_date = date.fromisoformat(args.start)
    else:
        # Default: ~1 year back
        start_date = end_date - timedelta(days=365)

    # Add liquidity buffer before start
    buffer_start = start_date - timedelta(days=args.liquidity_days + 5)  # extra margin for weekends

    log.info(f"Backtest range: {start_date} → {end_date}")
    log.info(f"Liquidity buffer from: {buffer_start}")

    # Filter data
    buffer_start_ts = datetime.combine(buffer_start, datetime.min.time()).replace(tzinfo=pytz.utc).timestamp()
    end_ts = datetime.combine(end_date + timedelta(days=1), datetime.min.time()).replace(tzinfo=pytz.utc).timestamp()

    bars_5m = [b for b in bars_5m if buffer_start_ts <= b["timestamp"] <= end_ts]
    bars_1m = [b for b in bars_1m if buffer_start_ts <= b["timestamp"] <= end_ts]

    log.info(f"Filtered: {len(bars_5m):,} 5m bars, {len(bars_1m):,} 1m bars")

    # ----- 4. Get trading days -----
    def get_date(ts):
        """
        Assign a CME trading session date to a timestamp.
        
        CME ES futures session runs 17:00 CT (Sun) to 16:00 CT (Fri).
        Each daily session starts at 17:00 CT the prior evening.
        A bar at 22:00 UTC on Monday = 17:00 CT Monday = TUESDAY's session.
        A bar at 14:00 UTC on Tuesday = 09:00 CT Tuesday = TUESDAY's session.
        
        Rule: if CT hour < 17:00, it belongs to that calendar date.
               if CT hour >= 17:00, it belongs to the NEXT calendar date.
        """
        ct = datetime.fromtimestamp(ts, pytz.utc).astimezone(TZ)
        if ct.hour >= 17:
            return (ct + timedelta(days=1)).date()
        return ct.date()

    # All replay days (after liquidity buffer)
    all_replay_dates = sorted(set(get_date(b["timestamp"]) for b in bars_5m if get_date(b["timestamp"]) >= start_date))
    trading_days = [d for d in all_replay_dates if d.weekday() < 5 and d <= end_date]
    log.info(f"Trading days to replay: {len(trading_days)}")
    if trading_days:
        log.info(f"  First: {trading_days[0]} | Last: {trading_days[-1]}")

    if not trading_days:
        log.error("No trading days in the specified range!")
        return

    # ----- OOS: Shuffle and/or subsample trading days -----
    import random as _rng
    if args.seed is not None:
        _rng.seed(args.seed)
    elif args.shuffle or args.oos_pct:
        _rng.seed()  # True random

    original_day_count = len(trading_days)

    if args.oos_pct is not None:
        # Randomly select X% of days
        n_keep = max(1, int(len(trading_days) * args.oos_pct / 100))
        trading_days = sorted(_rng.sample(trading_days, n_keep))
        log.info(f"OOS subsample: {args.oos_pct:.0f}% → {len(trading_days)} of {original_day_count} days")

    if args.shuffle:
        # Shuffle the order days are processed (breaks sequential correlation)
        # Liquidity scan still uses calendar-preceding days for each date
        _rng.shuffle(trading_days)
        log.info(f"Shuffled {len(trading_days)} trading days (liquidity still uses preceding calendar days)")
        if args.seed is not None:
            log.info(f"  Random seed: {args.seed}")

    # ----- 4. Build rolling liquidity scan data -----
    # Pre-index bars by date for fast lookup
    bars_5m_by_date = defaultdict(list)
    bars_1m_by_date = defaultdict(list)
    for b in bars_5m:
        bars_5m_by_date[get_date(b["timestamp"])].append(b)
    for b in bars_1m:
        bars_1m_by_date[get_date(b["timestamp"])].append(b)

    # All available dates (including buffer period)
    all_available_dates = sorted(bars_5m_by_date.keys())

    # ----- 5. Replay -----
    sim = TradeSimulator(point_value=POINT_VALUE, risk_scaling=args.risk_scaling, payouts=args.payouts,
                         break_even=args.break_even, fees_and_slippage=args.fees, kelly=args.kelly)
    if args.kelly:
        frac = KELLY_FRACTIONS.get(args.kelly, KELLY_FULL / 2)
        log.info(f"KELLY SIZING ENABLED: {args.kelly} Kelly = {frac*100:.1f}% of balance per trade")
        log.info(f"  Min risk: ${KELLY_MIN_RISK:,.0f} | Max risk: ${KELLY_MAX_RISK:,.0f}")
        if args.risk_scaling:
            log.warning(f"  Note: --kelly overrides --risk-scaling")
    elif args.risk_scaling:
        ladder_str = " → ".join(f"${t:,}→${r:,}" for t, r in RISK_LADDER)
        log.info(f"Risk scaling ENABLED: base=${BASE_RISK:.0f} | {ladder_str}")
    if args.payouts:
        log.info(f"Monthly payouts ENABLED: {PAYOUT_PCT*100:.0f}% of balance; above ${PAYOUT_THRESHOLD:,.0f} only {PAYOUT_PCT*100:.0f}% of excess")
    if args.break_even:
        log.info(f"Break-even ENABLED: SL moves to entry when price hits hourly liquidity level")
    if args.shuffle and (args.payouts or args.risk_scaling):
        log.warning(f"⚠️  Shuffle + payouts/risk-scaling: compounded results are not meaningful")
        log.warning(f"    Focus on: win rate, profit factor, avg win/loss, flat-risk P&L")
    if args.fees:
        log.info(f"Fees & slippage ENABLED: MES=${FEE_MES_RT}/RT, ES=${FEE_ES_RT}/RT")
        log.info(f"  Entry: limit order at trigger price, 0 slippage")
        log.info(f"  TP: limit order, 0 slippage")
        log.info(f"  SL: stop market, size-based slippage (0.5-2 ticks)")
        log.info(f"Instrument migration: MES→ES at ${ES_SWITCH_BALANCE:,} | ES ceiling ${ES_CEILING_RISK:,}/session | Multi at ${MULTI_SWITCH_BALANCE:,}")
    if args.max_contracts is not None and not args.combine and not args.xfa:
        log.info(f"Contract cap ENABLED: max {args.max_contracts} contracts per trade (clamped, not skipped)")
    if args.combine:
        log.info(f"COMBINE mode: ${COMBINE_RISK}/trade | {COMBINE_MAX_CONTRACTS} contract cap | MLL ${COMBINE_MLL:,}")
    if args.xfa:
        log.info(f"XFA mode: Risk $750→$875→$1,000→$1,250 | Contracts 20→30→50 | MLL ${XFA_MLL:,}")
        log.info(f"  Risk tiers:     $750 (<$1.5K) → $875 ($1.5K-$3K) → $1,000 ($3K-$5K) → $1,250 (>$5K)")
        log.info(f"  Contract tiers: 20 (<$1.5K) → 30 ($1.5K-$2K) → 50 (>$2K)")
    if args.combine and args.xfa:
        log.warning(f"⚠️  Both --combine and --xfa specified — using --xfa")
    if (args.combine or args.xfa) and args.max_contracts is not None:
        log.warning(f"⚠️  --combine/--xfa overrides --max-contracts")
    no_trade_count = 0

    for day_idx, day in enumerate(trading_days):
        # Check for month boundary — process payout before trading
        sim.process_month_end(day)
        # Build liquidity scan from preceding N days
        day_pos = all_available_dates.index(day) if day in all_available_dates else -1
        if day_pos < 0:
            continue

        # Gather liquidity_days worth of 5m bars BEFORE this day
        scan_start_idx = max(0, day_pos - args.liquidity_days)
        scan_dates = all_available_dates[scan_start_idx:day_pos]
        scan_bars = []
        for sd in scan_dates:
            scan_bars.extend(bars_5m_by_date[sd])

        # Create fresh engine for each day
        engine_cls = {"v1": ICTEngine}[args.engine]
        engine = engine_cls(account_id="BACKTEST", token="BACKTEST", symbol=INSTRUMENT)
        engine.point_value = POINT_VALUE
        engine.order_callback = sim.on_trade_signal
        engine.trade_log.enabled = False  # Don't write to ict_trades.jsonl

        # Apply risk scaling based on running balance
        engine.max_risk_usd = sim.get_current_risk()

        # Account rules: XFA > Combine > manual cap > uncapped
        if args.xfa or (args.combine and args.xfa):
            if sim.balance <= XFA_MLL:
                log.info(f"💀 XFA BLOWN: Balance ${sim.balance:,.2f} hit MLL ${XFA_MLL:,} — stopping")
                break
            engine.max_contracts = get_xfa_max_contracts(sim.balance)
            engine.max_risk_usd = get_xfa_risk(sim.balance)
            engine.entry_cutoff_minute = args.cutoff if args.cutoff is not None else 45
        elif args.combine:
            if sim.balance <= COMBINE_MLL:
                log.info(f"💀 COMBINE BLOWN: Balance ${sim.balance:,.2f} hit MLL ${COMBINE_MLL:,} — stopping")
                break
            engine.max_contracts = COMBINE_MAX_CONTRACTS
            engine.max_risk_usd = COMBINE_RISK
            engine.entry_cutoff_minute = args.cutoff if args.cutoff is not None else 45
        elif args.max_contracts is not None:
            engine.max_contracts = args.max_contracts
        else:
            engine.max_contracts = 999_999

        # Apply --cutoff override for non-combine/xfa modes
        if args.cutoff is not None and not args.combine and not args.xfa:
            engine.entry_cutoff_minute = args.cutoff

        if len(scan_bars) > 3:
            engine.scan_historical_liquidity(scan_bars)
            engine.preload_atr_history(scan_bars)

        # Pass hourly liquidity levels to simulator for break-even
        if args.break_even:
            sim.set_be_levels(engine.liquidity_pools.get("HOURLY", []))

        # Get today's bars
        day_5m = bars_5m_by_date.get(day, [])
        day_1m = bars_1m_by_date.get(day, [])

        if not day_5m or not day_1m:
            no_trade_count += 1
            continue

        trades_before = len(sim.trades)

        # Interleave 5m and 1m events.
        #
        # DEFAULT (realistic live-delivery): sort by *delivery time* = bar's
        # close time, mirroring what the live websocket delivers.
        #   - 1m@T delivers at T+60s (when the minute closes)
        #   - 5m@T delivers at T+300s (when the 5-min period closes)
        # Within the same delivery time (e.g. 5m@09:00 and 1m@09:04 both arrive
        # at 09:05:00), canonical sort: 5m-before-1m, then by ts. This is exactly
        # what the live drainer sees with our 1.5s debounce — no look-ahead.
        #
        # --lookahead-mode (old/biased): sort by open timestamp, so 5m@T is
        # processed before the 1m bars that formed it. The engine "sees the
        # future" of the 5m bar at its open. Inflated, unrealistic — kept only
        # to demonstrate the bias.
        events = [("5m", b) for b in day_5m] + [("1m", b) for b in day_1m]
        if args.lookahead_mode:
            events.sort(key=lambda x: x[1]["timestamp"])
        else:
            def _delivery_key(item):
                tf, b = item
                duration = 300 if tf == "5m" else 60
                # (delivery_time, 0-if-5m-else-1, ts) — canonical within batch
                return (b["timestamp"] + duration, 0 if tf == "5m" else 1, b["timestamp"])
            events.sort(key=_delivery_key)

        for tf, bar in events:
            if sim.open_trade and tf == "1m":
                if sim.check_exit(bar):
                    continue
            if sim.pending_trade and not sim.open_trade:
                c_time = datetime.fromtimestamp(bar["timestamp"], pytz.utc).astimezone(TZ)
                sim.activate_pending(c_time.strftime("%Y-%m-%d %H:%M"))
            if tf == "5m":
                engine.process_5m_candle(bar)
            elif tf == "1m":
                engine.process_1m_candle(bar)

        # Force close any open trade at end of day
        if sim.open_trade and day_1m:
            sim.force_close_eod(day_1m[-1])

        traded = len(sim.trades) > trades_before
        if not traded:
            no_trade_count += 1

        pct = (day_idx + 1) / len(trading_days) * 100
        status = "T" if traded else "."
        n_trades = len(sim.trades)

        # Progress bar
        if (day_idx + 1) % 5 == 0 or day_idx == len(trading_days) - 1:
            bar_len = 30
            filled = int(bar_len * pct / 100)
            bar_str = "█" * filled + "░" * (bar_len - filled)
            print(f"\r  [{bar_str}] {pct:5.1f}% | {day} | {n_trades} trades", end="", flush=True)

    print()  # Newline after progress bar

    # Process final month payout
    if args.payouts and trading_days:
        # Trigger payout for the last month by simulating a new month
        fake_next = trading_days[-1].replace(month=trading_days[-1].month % 12 + 1, day=1) if trading_days[-1].month < 12 else trading_days[-1].replace(year=trading_days[-1].year + 1, month=1, day=1)
        sim.process_month_end(fake_next)

    # ----- 6. Report -----
    results = generate_report(
        sim.trades, start_date, end_date,
        len(trading_days), no_trade_count,
        risk_scaling=args.risk_scaling or args.kelly,
        balance_history=sim.balance_history if (args.risk_scaling or args.kelly) else None,
        payout_log=sim.payout_log if args.payouts else None,
        total_paid_out=sim.total_paid_out if args.payouts else 0,
    )

    return results


if __name__ == "__main__":
    main()
