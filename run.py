"""
run.py — Main entry point for the ICT Trading Bot.

Broker-agnostic orchestration layer. Connects a BrokerAdapter (TopstepX)
to the ICT Engine (strategy logic). Includes risk scaling ladder that
permanently ratchets up risk as account grows.

v3 Changes:
  - Broker adapter abstraction
  - Position tracking via order fill events (not unreliable position updates)
  - Auto journal logging on trade close
  - Auto-shutdown at 9:45 CT if no position, or when position closes, hard cutoff at 13:00 CT
  - Fixed: cancel-rejection race (verify fill after any cancel failure)
  - Fixed: TP placement failure sends alert
  - Fixed: daily_trade_count persisted in risk_state.json

Usage:
  python run.py
"""

import os
import sys
import asyncio
import signal
import logging
import json
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, time as dtime
from pathlib import Path

import pytz

from broker import get_broker, BarEvent, OrderEvent
from engine import ICTEngine
from journal import TradeJournal


# =====================================================================
# CONFIG — Loads from config.json (written by desktop app settings page)
# Falls back to defaults if config.json doesn't exist
# =====================================================================
CONFIG_FILE = Path(__file__).parent / "config.json"

def _load_config():
    defaults = {
        # --- Broker selection ---
        "broker": "topstepx",

        # --- TopstepX credentials (set these in config.json, never commit them) ---
        "username": "",
        "api_key": "",
        "account_name": "",

        # --- Shared settings ---
        "account_type": "COMBINE",
        "instrument": "MES",
        "combine_risk": 750,
        "combine_max_contracts": 50,
        "xfa_base_risk": 750,
        "live_base_risk": 750,
        "historical_days": 5,
        "historical_interval": 5,
        "alert_email": "",
        "alert_smtp_pass": "",
        "alert_on_trade": False,
        "alert_on_emergency": True,
        "alert_weekly_recap": True,
    }
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE) as f:
                saved = json.load(f)
            defaults.update({k: v for k, v in saved.items() if v is not None and v != ""})
        except Exception:
            pass

    return defaults

CFG = _load_config()

# =====================================================================
# ALERT SYSTEM — sends email on critical events
# =====================================================================
ALERT_EMAIL = CFG["alert_email"]
ALERT_SMTP_USER = CFG["alert_email"]
ALERT_SMTP_PASS = CFG["alert_smtp_pass"]
ALERT_ENABLED = bool(ALERT_SMTP_PASS)

def send_alert(subject, body):
    """Send email alert. Non-blocking, never crashes the bot."""
    if not ALERT_ENABLED:
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = f"🤖 TradeAI — {subject}"
        msg["From"] = ALERT_SMTP_USER
        msg["To"] = ALERT_EMAIL
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as s:
            s.login(ALERT_SMTP_USER, ALERT_SMTP_PASS)
            s.send_message(msg)
    except Exception:
        pass  # Never let alert failure crash the bot

INSTRUMENT = CFG["instrument"]
TIMEFRAMES = ["1min", "5min"]
HISTORICAL_DAYS = int(CFG["historical_days"])
HISTORICAL_INTERVAL = int(CFG["historical_interval"])
TZ = pytz.timezone("America/Chicago")

# =====================================================================
# ACCOUNT TYPE — Set this to control risk mode
# "COMBINE"  = Survival mode: $750 flat, 50 cap, 9:15 cutoff
# "XFA"      = Survival mode: scaling risk/contracts, 9:15 cutoff
# "LIVE"     = Compounding mode: risk ladder, uncapped, 9:45 cutoff
# =====================================================================
ACCOUNT_TYPE = CFG["account_type"]

# =====================================================================
# COMBINE RULES
# =====================================================================
COMBINE_RISK = int(CFG["combine_risk"])
COMBINE_MAX_CONTRACTS = int(CFG["combine_max_contracts"])
COMBINE_FEE_PER_CONTRACT = 0.74

# =====================================================================
# XFA RULES — Conservative risk steps + contract tiers
# =====================================================================
XFA_RISK_TIERS = [
    (5_000, 1_250),
    (3_000, 1_000),
    (1_500,   875),
    (0,       750),
]
XFA_CONTRACT_TIERS = [
    (2_000, 50),
    (1_500, 30),
    (0,     20),
]
XFA_FEE_PER_CONTRACT = 0.74

def get_xfa_risk(balance):
    for threshold, risk in XFA_RISK_TIERS:
        if balance >= threshold:
            return risk
    return 750

def get_xfa_max_contracts(balance):
    for threshold, cap in XFA_CONTRACT_TIERS:
        if balance >= threshold:
            return cap
    return 20

# =====================================================================
# LIVE RULES — Compounding mode, risk ladder
# =====================================================================
LIVE_BASE_RISK = 750.0
LIVE_BASE_MAX_CONTRACTS = 999_999  # Uncapped
LIVE_FEE_PER_CONTRACT = 0.00  # Update when broker is chosen

RISK_LADDER = [
    # (cumulative_pnl_threshold, risk_per_trade)
    (10_000,      2_000),
    (20_000,      3_500),
    (40_000,      6_000),
    (80_000,     10_000),
    (160_000,    16_000),
    (320_000,    25_000),
    (640_000,    40_000),
    (1_280_000,  64_000),
    (2_560_000, 100_000),
    (5_120_000, 160_000),
]

# =====================================================================
# ACCOUNT TYPE → CONFIG MAPPING
# =====================================================================
ACCOUNT_CONFIGS = {
    "COMBINE": {
        "base_risk": COMBINE_RISK,
        "base_max_contracts": COMBINE_MAX_CONTRACTS,
        "fee_per_contract": COMBINE_FEE_PER_CONTRACT,
        "entry_cutoff_minute": 45,  # 9:45 CT
        "soft_cutoff": dtime(9, 45),
        "hard_cutoff": dtime(13, 0),
        "use_ladder": False,
        "use_xfa_scaling": False,
    },
    "XFA": {
        "base_risk": 750,
        "base_max_contracts": 20,  # Starting tier
        "fee_per_contract": XFA_FEE_PER_CONTRACT,
        "entry_cutoff_minute": 45,  # 9:45 CT
        "soft_cutoff": dtime(9, 45),
        "hard_cutoff": dtime(13, 0),
        "use_ladder": False,
        "use_xfa_scaling": True,
    },
    "LIVE": {
        "base_risk": LIVE_BASE_RISK,
        "base_max_contracts": LIVE_BASE_MAX_CONTRACTS,
        "fee_per_contract": LIVE_FEE_PER_CONTRACT,
        "entry_cutoff_minute": 45,  # 9:45 CT — compounding mode
        "soft_cutoff": dtime(9, 45),
        "hard_cutoff": dtime(13, 0),
        "use_ladder": True,
        "use_xfa_scaling": False,
    },
}

# Active config
ACFG = ACCOUNT_CONFIGS[ACCOUNT_TYPE]
FEE_PER_CONTRACT = ACFG["fee_per_contract"]
SOFT_CUTOFF = ACFG["soft_cutoff"]
HARD_CUTOFF = ACFG["hard_cutoff"]

STATE_FILE = "risk_state.json"

class RiskManager:
    """
    Tracks cumulative P&L across sessions and manages risk per account type.
    - COMBINE: Fixed $750 risk, no ratchet
    - XFA: Balance-based risk tiers, no ratchet
    - LIVE: Permanent risk ratchet ladder
    State persists to disk so restarts don't lose progress.
    """
    def __init__(self, state_file=STATE_FILE):
        self.state_file = state_file
        self.cumulative_pnl = 0.0
        self.peak_pnl = 0.0
        self.current_risk = ACFG["base_risk"]
        self.current_max_contracts = ACFG["base_max_contracts"]
        self.trades_today = []
        self.total_trades = 0
        self.total_wins = 0
        self.daily_trade_count = 0
        self.last_trade_date = None
        self._load_state()

    def _load_state(self):
        """Load persisted risk state from disk."""
        if Path(self.state_file).exists():
            try:
                with open(self.state_file) as f:
                    state = json.load(f)
                self.cumulative_pnl = state.get("cumulative_pnl", 0.0)
                self.peak_pnl = state.get("peak_pnl", 0.0)
                self.current_risk = state.get("current_risk", ACFG["base_risk"])
                self.current_max_contracts = state.get("current_max_contracts", ACFG["base_max_contracts"])
                self.total_trades = state.get("total_trades", 0)
                self.total_wins = state.get("total_wins", 0)
                # Restore daily trade count (prevents re-trading after restart)
                saved_date = state.get("last_trade_date")
                if saved_date:
                    try:
                        self.last_trade_date = datetime.fromisoformat(saved_date).date()
                        if self.last_trade_date == datetime.now(TZ).date():
                            self.daily_trade_count = state.get("daily_trade_count", 0)
                        else:
                            self.daily_trade_count = 0
                            self.last_trade_date = None
                    except (ValueError, TypeError):
                        pass
                log.info(
                    f"📂 Loaded risk state: P&L=${self.cumulative_pnl:+,.2f} | "
                    f"Peak=${self.peak_pnl:+,.2f} | Risk=${self.current_risk:,.0f} | "
                    f"Trades={self.total_trades} | Today={self.daily_trade_count}"
                )
            except Exception as e:
                log.warning(f"Could not load risk state: {e} — starting fresh")

    def _save_state(self):
        """Persist risk state to disk."""
        state = {
            "cumulative_pnl": round(self.cumulative_pnl, 2),
            "peak_pnl": round(self.peak_pnl, 2),
            "current_risk": self.current_risk,
            "current_max_contracts": self.current_max_contracts,
            "total_trades": self.total_trades,
            "total_wins": self.total_wins,
            "daily_trade_count": self.daily_trade_count,
            "last_trade_date": self.last_trade_date.isoformat() if self.last_trade_date else None,
            "last_updated": datetime.now().isoformat(),
            "account_type": ACCOUNT_TYPE,
        }
        try:
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            log.error(f"Could not save risk state: {e}")

    def _check_ratchet(self):
        """Check if peak P&L has crossed a new threshold. Only for LIVE accounts."""
        if not ACFG["use_ladder"]:
            return
        for threshold, risk in RISK_LADDER:
            if self.peak_pnl >= threshold and risk > self.current_risk:
                old_risk = self.current_risk
                self.current_risk = risk
                # Uncapped contracts on live — scale proportionally just for tracking
                self.current_max_contracts = LIVE_BASE_MAX_CONTRACTS
                log.info(
                    f"🔺 RISK RATCHET: ${old_risk:,.0f} → ${risk:,.0f} "
                    f"(peak P&L ${self.peak_pnl:+,.2f} crossed ${threshold:,})"
                )

    def record_trade(self, pnl_usd):
        """Record a completed trade and update risk state."""
        self.cumulative_pnl += pnl_usd
        self.total_trades += 1
        if pnl_usd > 0:
            self.total_wins += 1

        if self.cumulative_pnl > self.peak_pnl:
            self.peak_pnl = self.cumulative_pnl

        self._check_ratchet()
        self._save_state()

        wr = (self.total_wins / self.total_trades * 100) if self.total_trades > 0 else 0
        log.info(
            f"📊 Trade #{self.total_trades}: ${pnl_usd:+,.2f} | "
            f"Cumulative: ${self.cumulative_pnl:+,.2f} | "
            f"Peak: ${self.peak_pnl:+,.2f} | "
            f"Risk: ${self.current_risk:,.0f} | "
            f"WR: {wr:.0f}%"
        )

    def record_daily_trade(self):
        """Increment daily trade count and persist."""
        self.daily_trade_count += 1
        self.last_trade_date = datetime.now(TZ).date()
        self._save_state()

    def apply_to_engine(self, engine):
        """Set the engine's risk parameters based on account type."""
        if ACFG["use_xfa_scaling"]:
            # XFA: dynamic risk and contracts based on running balance
            engine.max_risk_usd = get_xfa_risk(self.cumulative_pnl)
            engine.max_contracts = get_xfa_max_contracts(self.cumulative_pnl)
        elif ACFG["use_ladder"]:
            # LIVE: ratchet ladder
            engine.max_risk_usd = self.current_risk
            engine.max_contracts = self.current_max_contracts
        else:
            # COMBINE: fixed
            engine.max_risk_usd = ACFG["base_risk"]
            engine.max_contracts = ACFG["base_max_contracts"]


# =====================================================================
# LOGGING
# =====================================================================
# Use an absolute path anchored to this file so the log lands beside run.py
# no matter what cwd the launcher (desktop app, cron, terminal) uses.
# The previous relative "ict_bot.log" caused the log to go wherever the bot
# was launched from — which is why ict_bot.log appears stale after April 24
# (the launcher's cwd changed and writes went somewhere else).
_LOG_FILE = Path(__file__).parent / "ict_bot.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(_LOG_FILE), mode="a"),
    ],
)
log = logging.getLogger("Run")
log.info(f"📝 Engine log: {_LOG_FILE}")


# =====================================================================
# BAR LOG — forensic capture of every bar the engine sees
# =====================================================================
# Every completed bar (historical scan + live websocket) is appended to
# bars/YYYY-MM-DD.jsonl in order of arrival. This file IS the ground
# truth for what the live engine processed. replay.py --from-bar-log
# reads it back byte-for-byte so live vs replay can no longer diverge
# due to data-source differences.
#
# Record schema (one JSON object per line):
#   {"phase": "scan"|"live", "tf": "1min"|"5min",
#    "ts": <epoch_seconds>, "o","h","l","c","v": <float>}
# Session marker lines (no "phase" key) are also written.

BAR_LOG_DIR = Path(__file__).parent / "bars"
BAR_LOG_DIR.mkdir(exist_ok=True)

def _bar_log_path_for_today():
    return BAR_LOG_DIR / f"{datetime.now(TZ).date().isoformat()}.jsonl"

def write_bar_record(path, phase, tf, candle):
    """Append one bar to the log. Never raises — logging must not crash the bot."""
    try:
        with open(path, "a") as f:
            f.write(json.dumps({
                "phase": phase,
                "tf": tf,
                "ts": candle["timestamp"],
                "o": candle["open"],
                "h": candle["high"],
                "l": candle["low"],
                "c": candle["close"],
                "v": candle.get("volume", 0),
            }) + "\n")
    except Exception as e:
        log.warning(f"bar log write failed ({phase}/{tf}): {e}")

def write_bar_marker(path, **fields):
    try:
        with open(path, "a") as f:
            f.write(json.dumps(fields) + "\n")
    except Exception:
        pass


# =====================================================================
# MAIN
# =====================================================================
async def main():
    # ----- 1. Initialize broker adapter -----
    broker_name = CFG.get("broker", "topstepx")
    log.info(f"Broker: {broker_name}")

    broker = get_broker(CFG)

    try:
        await broker.connect()
    except Exception as e:
        log.error(f"Broker connection failed: {e}")
        sys.exit(1)

    acct = await broker.get_account_info()
    log.info(f"✅ Account: {acct.name} (ID: {acct.id})")

    instrument = await broker.get_instrument_info(INSTRUMENT)
    log.info(f"Instrument: {instrument.name} | Tick: {instrument.tick_size} | Value: {instrument.tick_value}")

    # ----- 2. Initialize risk manager & journal -----
    risk_mgr = RiskManager()
    journal = TradeJournal()

    # ----- Orphaned position warning -----
    log.warning(
        "⚠️ REMINDER: Verify no orphaned positions exist before trading. "
        "Check your broker dashboard manually."
    )

    # ----- 3. Build engine -----
    token = await broker.get_session_token()

    engine = ICTEngine(
        account_id=str(acct.id),
        token=token,
        symbol=INSTRUMENT,
    )

    # Override point_value from instrument metadata
    if instrument.tick_value and instrument.tick_size:
        engine.point_value = instrument.tick_value / instrument.tick_size
        log.info(f"Point value set to ${engine.point_value:.2f}/pt from instrument metadata")

    # Set entry cutoff from account type
    engine.entry_cutoff_minute = ACFG["entry_cutoff_minute"]

    # Live mode: daily_trade_count incremented after confirmed fill, not on signal
    engine.live_mode = True

    # Sync daily trade count from persisted risk state
    engine.daily_trade_count = risk_mgr.daily_trade_count
    engine.last_trade_date = risk_mgr.last_trade_date

    # Apply current risk position to engine
    risk_mgr.apply_to_engine(engine)

    mode = "SURVIVAL" if ACCOUNT_TYPE in ("COMBINE", "XFA") else "COMPOUNDING"
    log.info(
        f"🏦 Account type: {ACCOUNT_TYPE} ({mode} mode) | "
        f"Entry cutoff: 9:{ACFG['entry_cutoff_minute']:02d} CT | "
        f"Shutdown: {SOFT_CUTOFF.strftime('%H:%M')}/{HARD_CUTOFF.strftime('%H:%M')} CT"
    )
    log.info(
        f"🎯 Risk: ${engine.max_risk_usd:,.0f}/trade | "
        f"Max contracts: {engine.max_contracts} | "
        f"Cumulative P&L: ${risk_mgr.cumulative_pnl:+,.2f}"
    )

    # ----- 4. Wire up order execution callback -----
    # Shared state for tracking bracket order IDs
    bracket_orders = {
        "entry_order_id": None,
        "stop_order_id": None,
        "target_order_id": None,
        "entry_time": None,
        "position_confirmed": False,  # True only after entry fill confirmed on exchange
    }

    # Execution lock — prevents double execution if engine fires twice
    executing_lock = asyncio.Lock()

    # Fill processing lock — prevents OCO race where both SL and TP fill events
    # arrive simultaneously and both try to process the close
    fill_lock = asyncio.Lock()

    # Event that fires when a position closes (for auto-shutdown)
    position_closed = asyncio.Event()

    # Track the bracket execution task so it can be cancelled on shutdown
    bracket_task_ref = {"task": None}

    async def execute_bracket(side, qty, entry, sl, tp):
        """
        Manual bracket order: 3 independent orders instead of SDK's bracket.

        1. Place LIMIT entry order
        2. Wait for fill confirmation via order tracker
        3. Place SL + TP orders simultaneously

        This avoids race conditions in SDK bracket managers where the REST API
        returns stale data while the websocket already confirmed the fill.
        """
        # ============================================================
        # EXECUTION LOCK — prevent double execution
        # ============================================================
        if executing_lock.locked():
            log.warning("⚠️ execute_bracket called while already executing — ignoring duplicate")
            return
        await executing_lock.acquire()

        try:
            side_str = 'BUY' if side == 0 else 'SELL'
            close_side = 1 if side == 0 else 0  # Opposite side for SL/TP
            risk_pts = abs(entry - sl)
            risk_usd = risk_pts * qty * engine.point_value
            log.info(
                f"🚀 Executing: {side_str} {qty}x {INSTRUMENT} | "
                f"Entry~{entry:.2f} SL={sl:.2f} TP={tp:.2f} | "
                f"Risk=${risk_usd:,.2f} (${engine.max_risk_usd:,.0f} max)"
            )

            # ============================================================
            # STEP 1: Place LIMIT entry order
            # ============================================================
            log.info(f"📋 Step 1/3: Placing LIMIT {side_str} {qty}x @ {entry:.2f}")
            entry_result = await broker.place_limit_order(
                side=side,
                size=qty,
                price=entry,
            )
            entry_order_id = entry_result.order_id

            if not entry_order_id:
                log.error(f"❌ Could not get entry order ID")
                if engine.active_position:
                    engine.active_position = None
                return

            log.info(f"✅ Entry order placed: ID={entry_order_id}")
            bracket_orders["entry_order_id"] = entry_order_id
            bracket_orders["entry_time"] = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")

            # ============================================================
            # STEP 2: Wait for fill via order tracker
            # ============================================================
            log.info(f"⏳ Step 2/3: Waiting for fill on order {entry_order_id}...")

            fill_confirmed = False
            wait_count = 0
            max_wait = 1200  # 300 seconds / 0.25s = 1200 iterations (5 min max)
            while not fill_confirmed:
                await asyncio.sleep(0.25)
                wait_count += 1

                # Timeout: cancel order and abort if fill takes too long
                if wait_count >= max_wait:
                    log.warning(f"⏰ Fill timeout after {max_wait * 0.25:.0f}s — cancelling entry order")
                    try:
                        await broker.cancel_order(entry_order_id)
                        log.info(f"🗑️ Entry order {entry_order_id} cancelled (timeout)")
                    except Exception as cancel_err:
                        # FIX C2: Always verify fill status after cancel failure
                        log.warning(f"Cancel failed: {cancel_err} — verifying fill status")
                        try:
                            if await broker.is_order_filled(entry_order_id):
                                fill_confirmed = True
                                log.info(f"✅ Fill confirmed — order was already filled")
                                break
                        except Exception:
                            pass
                    if not fill_confirmed:
                        if engine.active_position:
                            engine.active_position = None
                        return

                # Log every 30 seconds so we know it's alive
                if wait_count % 120 == 0:
                    elapsed = wait_count * 0.25
                    log.info(f"⏳ Still waiting for fill... ({elapsed:.0f}s elapsed)")

                # Primary: check order tracker
                try:
                    if await broker.is_order_filled(entry_order_id):
                        fill_confirmed = True
                        log.info(f"✅ Fill confirmed via order tracker ({wait_count * 0.25:.1f}s)")
                        break
                except Exception:
                    pass

                # Fallback: check tracked order status directly
                try:
                    tracked = await broker.get_order_status(entry_order_id)
                    if tracked is not None:
                        ts = tracked.get("status")
                        if ts == 2:  # 2 = Filled
                            fill_confirmed = True
                            log.info(f"✅ Fill confirmed via tracked status=2 ({wait_count * 0.25:.1f}s)")
                            break
                        elif ts == 3:  # 3 = Cancelled (engine reset or session end)
                            log.info(f"🗑️ Entry order was cancelled externally — aborting")
                            if engine.active_position:
                                engine.active_position = None
                            return
                except Exception:
                    pass

                # Check if engine reset (setup invalidated) — cancel the pending entry
                if engine.active_position is None:
                    log.info("🔄 Engine reset while waiting for fill — cancelling entry order")
                    try:
                        await broker.cancel_order(entry_order_id)
                        log.info(f"🗑️ Entry order {entry_order_id} cancelled")
                    except Exception as cancel_err:
                        # FIX C2: Always verify fill status after cancel failure
                        log.warning(f"Cancel failed: {cancel_err} — verifying fill status")
                        try:
                            if await broker.is_order_filled(entry_order_id):
                                fill_confirmed = True
                                log.info(f"✅ Fill confirmed — order was already filled")
                        except Exception:
                            pass
                    if not fill_confirmed:
                        return

            # Entry confirmed filled — count against daily limit now (not before)
            engine.daily_trade_count += 1
            engine.last_trade_date = datetime.now(TZ).date()
            risk_mgr.record_daily_trade()
            bracket_orders["position_confirmed"] = True

            # ============================================================
            # STEP 3: Place SL and TP as independent orders
            # Entry is confirmed filled — now protect it immediately
            # ============================================================
            log.info(f"📋 Step 3/3: Placing SL @ {sl:.2f} and TP @ {tp:.2f}")

            sl_order_id = None
            tp_order_id = None

            # Place SL (Stop Market order) — CRITICAL, do this first
            # 5-second timeout — if SL can't be placed, flatten immediately
            try:
                sl_result = await asyncio.wait_for(
                    broker.place_stop_order(
                        side=close_side,
                        size=qty,
                        price=sl,
                    ),
                    timeout=5.0,
                )
                sl_order_id = sl_result.order_id
                log.info(f"✅ SL placed: ID={sl_order_id} @ {sl:.2f}")
            except (Exception, asyncio.TimeoutError) as sl_err:
                log.error(f"❌ SL placement failed: {sl_err}")
                # CRITICAL: No stop loss — emergency flatten
                log.error("🚨 EMERGENCY: Could not place SL — flattening position")
                send_alert("EMERGENCY FLATTEN", f"SL placement failed. Flattening {qty}x {INSTRUMENT} at market.")
                try:
                    await broker.place_market_order(
                        side=close_side,
                        size=qty,
                    )
                    log.info("🚨 Emergency flatten sent (SL failed)")
                except Exception as flat_err:
                    log.error(f"🚨 EMERGENCY FLATTEN FAILED: {flat_err}")
                    log.error("🚨 MANUAL INTERVENTION REQUIRED")
                if engine.active_position:
                    engine.active_position = None
                bracket_orders["position_confirmed"] = False
                return

            # Place TP (Limit order)
            try:
                tp_result = await asyncio.wait_for(
                    broker.place_limit_order(
                        side=close_side,
                        size=qty,
                        price=tp,
                    ),
                    timeout=5.0,
                )
                tp_order_id = tp_result.order_id
                log.info(f"✅ TP placed: ID={tp_order_id} @ {tp:.2f}")
            except Exception as tp_err:
                # FIX C3: Alert on TP failure (SL is active, position is protected)
                log.error(f"❌ TP placement failed: {tp_err} — SL is active, TP needs manual management")
                send_alert(
                    "TP PLACEMENT FAILED",
                    f"TP order failed for {qty}x {INSTRUMENT}.\n"
                    f"SL is active at {sl:.2f}.\n"
                    f"Intended TP was {tp:.2f}.\n"
                    f"Place TP manually or manage exit.",
                )

            # Store order IDs
            bracket_orders["stop_order_id"] = sl_order_id
            bracket_orders["target_order_id"] = tp_order_id

            if engine.active_position:
                engine.active_position["stop_order_id"] = sl_order_id
                engine.active_position["target_order_id"] = tp_order_id

            log.info(
                f"✅ Manual bracket complete | ENTRY={entry_order_id} "
                f"SL={sl_order_id} TP={tp_order_id}"
            )

        except Exception as e:
            log.error(f"❌ Manual bracket failed: {e}", exc_info=True)

            # ============================================================
            # EMERGENCY SAFETY NET — flatten using local state
            # ============================================================
            log.warning("🚨 Checking for naked position after bracket failure...")
            if engine.active_position:
                em_pos = engine.active_position
                em_qty = em_pos["qty"]
                em_close_side = 1 if em_pos["side"] == 0 else 0
                log.error(f"🚨 NAKED POSITION: {em_qty} contracts — flattening via local state")
                send_alert("NAKED POSITION", f"Naked position detected: {em_qty} contracts. Emergency flatten.")
                try:
                    await broker.place_market_order(
                        side=em_close_side,
                        size=em_qty,
                    )
                    log.info(f"🚨 Emergency flatten sent: {em_qty}x MARKET")
                except Exception as flat_err:
                    log.error(f"🚨 FLATTEN FAILED: {flat_err}")
                    log.error("🚨 MANUAL INTERVENTION REQUIRED")

            if engine.active_position:
                log.warning("🧹 Clearing engine position state")
                engine.active_position = None
            bracket_orders["position_confirmed"] = False

        finally:
            executing_lock.release()

    def order_callback(side, qty, entry, sl, tp):
        task = asyncio.get_running_loop().create_task(execute_bracket(side, qty, entry, sl, tp))
        bracket_task_ref["task"] = task

    engine.order_callback = order_callback

    # ----- 5. Wire up order-fill tracking for position close detection -----
    async def on_order_update(event: OrderEvent):
        """
        Watch for SL or TP order fills to detect position close.
        Receives normalized OrderEvent from the broker adapter.
        Serialized via fill_lock to prevent OCO race.
        """
        async with fill_lock:
            try:
                order_id = event.order_id
                if order_id is None:
                    return

                # Check if this is our SL or TP order
                pos = engine.active_position
                if not pos:
                    return

                sl_id = bracket_orders.get("stop_order_id")
                tp_id = bracket_orders.get("target_order_id")

                # Compare as strings to avoid int/str mismatch
                order_id_str = str(order_id)
                sl_match = sl_id is not None and str(sl_id) == order_id_str
                tp_match = tp_id is not None and str(tp_id) == order_id_str

                if not (sl_match or tp_match):
                    return

                # Determine which one filled
                if tp_match:
                    reason = "TP"
                    exit_price = pos["tp"]
                    cancel_id = sl_id  # Cancel the SL
                else:
                    reason = "SL"
                    exit_price = pos["sl"]
                    cancel_id = tp_id  # Cancel the TP

                # Use actual fill price from event if available
                if event.fill_price is not None and event.fill_price > 0:
                    log.info(f"📊 Actual fill price: {event.fill_price:.2f} (theoretical: {exit_price:.2f})")
                    exit_price = event.fill_price

                log.info(f"🔔 Detected {reason} fill: order {order_id}")

                # OCO: Cancel the other side
                if cancel_id:
                    try:
                        await broker.cancel_order(cancel_id)
                        log.info(f"🗑️ OCO: Cancelled {'SL' if reason == 'TP' else 'TP'} order {cancel_id}")
                    except Exception as cancel_err:
                        log.warning(f"OCO cancel failed: {cancel_err} — verifying fill status")
                        # Verify the other side didn't also fill (both legs hit)
                        try:
                            if await broker.is_order_filled(cancel_id):
                                log.error(
                                    f"🚨 BOTH {reason} AND {'SL' if reason == 'TP' else 'TP'} FILLED — "
                                    f"flattening extra fill"
                                )
                                send_alert(
                                    "DOUBLE FILL",
                                    f"Both SL and TP filled. Sending market order to flatten extra.",
                                )
                                # Extra fill = position in opposite direction
                                # Flatten by re-entering original side
                                flatten_side = pos["side"]
                                await broker.place_market_order(
                                    side=flatten_side,
                                    size=pos["qty"],
                                )
                                log.info(f"🚨 Double-fill flatten sent: {pos['qty']}x MARKET")
                        except Exception as verify_err:
                            log.error(f"🚨 Could not verify OCO fill: {verify_err}")
                            log.error("🚨 CHECK FOR NAKED POSITION MANUALLY")

                side = pos["side"]
                entry = pos["entry"]
                qty = pos["qty"]
                sl = pos["sl"]
                tp = pos["tp"]
                side_str = "BUY" if side == 0 else "SELL"

                # Compute PnL using actual exit price (may include slippage)
                if side == 0:  # Long
                    pnl_pts = exit_price - entry
                else:  # Short
                    pnl_pts = entry - exit_price

                pnl_gross = pnl_pts * qty * engine.point_value

                # Compute fees using config-based rate
                fees = FEE_PER_CONTRACT * qty
                pnl_net = pnl_gross - fees

                icon = "✅" if pnl_net > 0 else "❌"
                log.info(
                    f"{icon} POSITION CLOSED: {side_str} {qty}x | "
                    f"Entry={entry:.2f} Exit={exit_price:.2f} | "
                    f"{reason} | Gross=${pnl_gross:+,.2f} Fees=${fees:.2f} Net=${pnl_net:+,.2f}"
                )

                # Record in risk manager
                risk_mgr.record_trade(pnl_net)
                risk_mgr.apply_to_engine(engine)

                # Log to journal
                journal.log_trade(
                    date=datetime.now(TZ).strftime("%Y-%m-%d"),
                    time=bracket_orders.get("entry_time", "").split(" ")[-1] if bracket_orders.get("entry_time") else "",
                    account=ACCOUNT_TYPE,
                    side=side_str,
                    instrument=INSTRUMENT,
                    qty=qty,
                    entry=entry,
                    exit=exit_price,
                    sl=sl,
                    tp=tp,
                    reason=reason,
                    pnl_gross=round(pnl_gross, 2),
                    fees=round(fees, 2),
                    pnl_net=round(pnl_net, 2),
                    risk_usd=round(abs(entry - sl) * qty * engine.point_value, 2),
                )

                # Clear position
                engine.active_position = None
                bracket_orders["entry_order_id"] = None
                bracket_orders["stop_order_id"] = None
                bracket_orders["target_order_id"] = None
                bracket_orders["position_confirmed"] = False

                # Signal auto-shutdown
                position_closed.set()

            except Exception as e:
                log.error(f"Order tracking error: {e}", exc_info=True)

    # Subscribe to order events via broker adapter
    order_tracking_active = await broker.subscribe_order_events(on_order_update)
    if order_tracking_active:
        log.info("✅ Order fill tracking active")
    else:
        log.warning("⚠️ Could not subscribe to order events — using SL/TP polling fallback")

    # Shutdown event — must be defined before any task that references it.
    shutdown = asyncio.Event()

    # Backup: Poll SL/TP order status every 5s to detect fills.
    # Only checks our specific SL/TP order IDs (not position manager),
    # so it won't false-trigger on entry fills like the old position tracker did.
    async def poll_sl_tp_fills():
        while not shutdown.is_set():
            await asyncio.sleep(5)
            pos = engine.active_position
            if not pos:
                continue
            sl_id = bracket_orders.get("stop_order_id")
            tp_id = bracket_orders.get("target_order_id")
            if not sl_id and not tp_id:
                continue
            try:
                for check_id, label in [(sl_id, "SL"), (tp_id, "TP")]:
                    if not check_id:
                        continue
                    try:
                        if await broker.is_order_filled(check_id):
                            log.info(f"🔔 Poll detected {label} fill: order {check_id}")
                            # Build a synthetic event to reuse the same handler
                            await on_order_update(OrderEvent(
                                order_id=str(check_id),
                            ))
                            break
                    except Exception:
                        pass
            except Exception:
                pass

    poll_task = asyncio.create_task(poll_sl_tp_fills())

    # ----- 6. Pull historical data & build liquidity map -----
    log.info(f"Fetching {HISTORICAL_DAYS} days of {HISTORICAL_INTERVAL}m bars...")
    try:
        candles = await broker.get_historical_bars(
            INSTRUMENT, days=HISTORICAL_DAYS, interval_minutes=HISTORICAL_INTERVAL,
        )
    except Exception as e:
        log.error(f"Historical data fetch failed: {e}")
        await broker.disconnect()
        sys.exit(1)

    # Initialize bar log for this session and capture historical scan bars
    BAR_LOG = _bar_log_path_for_today()
    log.info(f"📝 Bar log: {BAR_LOG}")
    write_bar_marker(
        BAR_LOG,
        event="session_start",
        ts=datetime.now(TZ).isoformat(),
        instrument=INSTRUMENT,
        account_type=ACCOUNT_TYPE,
        historical_days=HISTORICAL_DAYS,
        historical_interval=HISTORICAL_INTERVAL,
    )

    if candles:
        log.info(f"Retrieved {len(candles)} historical bars")
        # Persist every scan bar to the log BEFORE feeding the engine, so replay
        # can rebuild the identical liquidity map.
        scan_tf = f"{HISTORICAL_INTERVAL}min"
        for b in candles:
            write_bar_record(BAR_LOG, "scan", scan_tf, b)
        engine.scan_historical_liquidity(candles)
        engine.preload_atr_history(candles)
    else:
        log.warning("No historical data — liquidity map will be empty")

    # ----- 7. Register real-time bar handler -----
    # The websocket delivers 5m and 1m bars in arrival order, which is non-
    # deterministic when both close at the same minute boundary (every 5m).
    # The backtest (backtest_replay.py / replay.py) processes bars sorted by
    # (timestamp asc, 5m-before-1m at ties) — see backtest_replay.py:1186.
    # We mirror that exactly: enqueue arrivals, settle briefly to gather any
    # sibling bar(s), then dispatch in canonical order on a single drainer.
    bar_queue: asyncio.Queue = asyncio.Queue()
    # Debounce config: collect bars until the queue has been quiet for
    # BAR_QUIET_SEC, then dispatch the batch sorted (timestamp asc, 5m-before-1m).
    # Each new arrival RESETS the quiet timer — so even if TopstepX delivers
    # the matching 5m bar a full second after the 1m at a boundary, both end
    # up in the same batch and the canonical sort gets the order right.
    # MAX_BATCH_SEC caps total wait so a pathological storm can't stall dispatch.
    BAR_QUIET_SEC = 1.5
    MAX_BATCH_SEC = 5.0

    async def on_new_bar(event: BarEvent):
        """
        Persist every bar to the log first (forensic record of what live saw),
        then enqueue for the drainer. The write is on the synchronous side of
        the queue so a crash between write and enqueue still preserves the bar.
        """
        try:
            write_bar_record(BAR_LOG, "live", event.timeframe, event.candle)
            await bar_queue.put((event.candle["timestamp"], event.timeframe, event.candle))
        except Exception as e:
            log.error(f"Bar enqueue failed ({event.timeframe}): {e}", exc_info=True)

    async def drain_bars():
        """
        Drain queued bars in (timestamp asc, 5m-before-1m) order so live
        produces identical engine state transitions to backtest_replay.py.

        Debounce semantics: after the first bar arrives, keep collecting
        until BAR_QUIET_SEC passes with no new arrivals (deadline resets on
        each bar). Capped at MAX_BATCH_SEC end-to-end as a safety net.
        """
        pending = []
        loop = asyncio.get_running_loop()
        while not shutdown.is_set():
            # Wait for the first bar of a new batch. Periodic wakeups so we
            # can notice shutdown promptly even during quiet stretches.
            try:
                ev = await asyncio.wait_for(bar_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            pending.append(ev)

            # Debounce: each get() with timeout=BAR_QUIET_SEC resets the
            # quiet window. The loop ends only when no bar arrives within
            # BAR_QUIET_SEC, OR when we've spent MAX_BATCH_SEC collecting.
            batch_start = loop.time()
            while True:
                if loop.time() - batch_start >= MAX_BATCH_SEC:
                    log.warning(
                        f"⏱️ Bar batch cap hit ({MAX_BATCH_SEC}s) — dispatching "
                        f"{len(pending)} bars; remaining stay queued for next cycle"
                    )
                    break
                try:
                    ev = await asyncio.wait_for(bar_queue.get(), timeout=BAR_QUIET_SEC)
                except asyncio.TimeoutError:
                    break  # Quiet period reached — batch complete
                pending.append(ev)

            # Canonical order: timestamp asc, 5min before 1min at ties.
            pending.sort(key=lambda x: (x[0], 0 if x[1] == "5min" else 1))
            for ts, tf, candle in pending:
                try:
                    if tf == "5min":
                        engine.process_5m_candle(candle)
                        log.debug(
                            f"5m | C={candle['close']:.2f} H={candle['high']:.2f} "
                            f"L={candle['low']:.2f} | Stage={engine.strategy_stage}"
                        )
                    elif tf == "1min":
                        engine.process_1m_candle(candle)
                        log.debug(f"1m | C={candle['close']:.2f} | Stage={engine.strategy_stage}")
                except Exception as e:
                    log.error(f"Bar processing error ({tf}): {e}", exc_info=True)
            pending.clear()

    bar_drain_task = asyncio.create_task(drain_bars())

    bar_subscribed = await broker.subscribe_bars(INSTRUMENT, TIMEFRAMES, on_new_bar)
    if not bar_subscribed:
        log.error("❌ Could not subscribe to bar events — bot cannot function")
        send_alert("BOT FAILED TO START", "Could not subscribe to bar events. Bot cannot function.")
        await broker.disconnect()
        sys.exit(1)

    log.info("📡 Listening for real-time bars...")
    log.info(f"Strategy stage: {engine.strategy_stage}")

    # ----- 8. Keep alive with heartbeat + auto-shutdown logic -----
    def handle_signal():
        log.info("🛑 Shutdown signal received")
        shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    async def heartbeat_and_shutdown():
        """Heartbeat logger + auto-shutdown at session end."""
        while not shutdown.is_set():
            now_ct = datetime.now(TZ)
            now_time = now_ct.time()

            # --- Auto-shutdown logic ---

            # Hard cutoff: 13:00 CT — flatten everything and shut down
            if now_time >= HARD_CUTOFF:
                log.info(f"🛑 HARD CUTOFF {HARD_CUTOFF.strftime('%H:%M')} CT — shutting down")
                if engine.active_position and bracket_orders.get("position_confirmed"):
                    hc_pos = engine.active_position
                    hc_qty = hc_pos["qty"]
                    hc_close_side = 1 if hc_pos["side"] == 0 else 0
                    log.warning(f"🚨 HARD CUTOFF FLATTEN: closing {hc_qty}x at market")
                    send_alert("HARD CUTOFF FLATTEN", f"Position still open at {HARD_CUTOFF.strftime('%H:%M')} CT. Flattening {hc_qty}x at market.")
                    try:
                        # Cancel existing SL/TP first
                        for oid in [bracket_orders.get("stop_order_id"), bracket_orders.get("target_order_id")]:
                            if oid:
                                try:
                                    await broker.cancel_order(oid)
                                except Exception:
                                    pass
                        await broker.place_market_order(
                            side=hc_close_side,
                            size=hc_qty,
                        )
                        log.info(f"✅ Hard cutoff flatten sent: {hc_qty}x MARKET")
                        engine.active_position = None
                        bracket_orders["position_confirmed"] = False
                    except Exception as hc_err:
                        log.error(f"🚨 HARD CUTOFF FLATTEN FAILED: {hc_err}")
                        log.error("🚨 MANUAL INTERVENTION REQUIRED — position still open!")
                shutdown.set()
                break

            # Soft cutoff: 9:45 CT — shut down if no position open
            if now_time >= SOFT_CUTOFF:
                if not bracket_orders.get("position_confirmed"):
                    # No position — safe to shut down
                    log.info(f"🛑 Session over ({SOFT_CUTOFF.strftime('%H:%M')} CT) — no position open, shutting down")
                    shutdown.set()
                    break
                else:
                    # Position open — wait for it to close
                    log.info(
                        f"⏳ Past {SOFT_CUTOFF.strftime('%H:%M')} CT but position open — "
                        f"waiting for close (hard cutoff at {HARD_CUTOFF.strftime('%H:%M')} CT)"
                    )
                    # Wait up to 60s for position to close, then check again
                    try:
                        await asyncio.wait_for(position_closed.wait(), timeout=60)
                        # Position closed — shut down on next loop iteration
                        log.info("✅ Position closed — initiating shutdown")
                        shutdown.set()
                        break
                    except asyncio.TimeoutError:
                        pass  # Will re-check on next heartbeat
                    continue

            # --- Normal heartbeat ---
            pos_str = "None"
            if engine.active_position:
                p = engine.active_position
                pos_str = (
                    f"{'LONG' if p['side'] == 0 else 'SHORT'} "
                    f"@ {p['entry']:.2f} SL={p['sl']:.2f}"
                )
            sweeps_str = ""
            if engine.pending_sweeps:
                sides = [s["type"] for s in engine.pending_sweeps]
                sweeps_str = f" | Sweeps={sides}"
            wr = (risk_mgr.total_wins / risk_mgr.total_trades * 100) if risk_mgr.total_trades > 0 else 0
            log.info(
                f"💓 Stage={engine.strategy_stage}{sweeps_str} | "
                f"5m={len(engine.candles_5m)} 1m={len(engine.candles_1m)} | "
                f"Risk=${engine.max_risk_usd:,.0f} | "
                f"P&L=${risk_mgr.cumulative_pnl:+,.2f} ({risk_mgr.total_trades}t, {wr:.0f}%WR) | "
                f"Position={pos_str}"
            )
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    hb = asyncio.create_task(heartbeat_and_shutdown())
    await shutdown.wait()

    # ----- 9. Cleanup -----
    # Cancel bracket execution task first (may be waiting for fill)
    bt = bracket_task_ref.get("task")
    if bt and not bt.done():
        bt.cancel()
        try:
            await bt
        except asyncio.CancelledError:
            pass
        log.info("🗑️ Bracket execution task cancelled on shutdown")

    hb.cancel()
    poll_task.cancel()
    bar_drain_task.cancel()
    for t in [hb, poll_task, bar_drain_task]:
        try:
            await t
        except asyncio.CancelledError:
            pass

    # Cancel any pending unfilled entry orders
    entry_id = bracket_orders.get("entry_order_id")
    if entry_id and not bracket_orders.get("position_confirmed"):
        log.info(f"🗑️ Cancelling pending entry order {entry_id} on shutdown...")
        try:
            await broker.cancel_order(entry_id)
            log.info(f"✅ Pending entry order cancelled")
        except Exception as ce:
            if "filled" in str(ce).lower():
                log.warning(f"⚠️ Entry filled during shutdown — check for open position!")
                send_alert(
                    "ENTRY FILLED AT SHUTDOWN",
                    f"Entry order {entry_id} filled during shutdown.\n"
                    f"No SL/TP was placed. Check for open position immediately.",
                )
            else:
                log.warning(f"Could not cancel entry order: {ce}")

    # Cancel orphaned orders — but NEVER cancel SL/TP if a confirmed position is still open
    if not bracket_orders.get("position_confirmed"):
        try:
            await broker.cancel_all_orders()
            log.info("✅ All open orders cancelled on shutdown (no position)")
        except Exception:
            pass
    else:
        log.warning(
            "⚠️ Position still open at shutdown — keeping SL/TP orders active! "
            "SL={} TP={}".format(
                bracket_orders.get("stop_order_id"),
                bracket_orders.get("target_order_id"),
            )
        )
        send_alert(
            "POSITION OPEN AT SHUTDOWN",
            f"Bot shutting down with position still open.\n"
            f"SL order {bracket_orders.get('stop_order_id')} and "
            f"TP order {bracket_orders.get('target_order_id')} are still active.\n"
            f"Monitor or close manually.",
        )

    log.info("Disconnecting...")
    await broker.disconnect()
    log.info(
        f"✅ Clean shutdown | P&L=${risk_mgr.cumulative_pnl:+,.2f} | "
        f"Risk=${risk_mgr.current_risk:,.0f} | Trades={risk_mgr.total_trades}"
    )

    # Weekly recap email — send on Fridays after shutdown
    now = datetime.now(TZ)
    if now.weekday() == 4:  # 4 = Friday
        try:
            # Read this week's trades from trades.csv
            trades_file = Path(STATE_FILE).parent / "trades.csv"
            week_trades = []
            week_pnl = 0.0
            week_wins = 0
            week_losses = 0
            if trades_file.exists():
                import csv
                monday = now.date() - __import__('datetime').timedelta(days=now.weekday())
                with open(trades_file) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            trade_date = datetime.strptime(row.get("date", ""), "%Y-%m-%d").date()
                            if trade_date >= monday:
                                pnl = float(row.get("pnl_net", 0))
                                week_trades.append(row)
                                week_pnl += pnl
                                if pnl > 0:
                                    week_wins += 1
                                else:
                                    week_losses += 1
                        except (ValueError, KeyError):
                            continue

            n = len(week_trades)
            wr = (week_wins / n * 100) if n > 0 else 0

            body = f"Weekly Recap — {monday.strftime('%b %d')} to {now.strftime('%b %d, %Y')}\n"
            body += f"{'='*45}\n\n"
            body += f"Trades:    {n}\n"
            body += f"Wins:      {week_wins}\n"
            body += f"Losses:    {week_losses}\n"
            body += f"Win Rate:  {wr:.0f}%\n"
            body += f"Week P&L:  ${week_pnl:+,.2f}\n\n"
            body += f"Cumulative P&L:  ${risk_mgr.cumulative_pnl:+,.2f}\n"
            body += f"Current Risk:    ${risk_mgr.current_risk:,.0f}/trade\n"
            body += f"Account:         {ACCOUNT_TYPE}\n\n"

            if week_trades:
                body += f"{'Date':<12} {'Side':<5} {'Qty':>4} {'Net P&L':>10} {'Result'}\n"
                body += f"{'-'*45}\n"
                for t in week_trades:
                    pnl = float(t.get("pnl_net", 0))
                    body += f"{t.get('date',''):<12} {t.get('side',''):<5} {t.get('qty',''):>4} ${pnl:>+9,.2f} {t.get('reason','')}\n"

            if n == 0:
                body += "No trades taken this week.\n"

            send_alert(
                f"Weekly Recap — {week_wins}W/{week_losses}L ${week_pnl:+,.2f}",
                body,
            )
            log.info("📧 Weekly recap email sent")
        except Exception as recap_err:
            log.warning(f"Could not send weekly recap: {recap_err}")


if __name__ == "__main__":
    asyncio.run(main())
