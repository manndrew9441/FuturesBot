"""
replay.py — Replay historical TopstepX data through the engine.

Pulls 1m and 5m bars from the SDK and feeds them through the engine
exactly as run.py would in live mode. Reads config.json so that
account type, historical_days, entry cutoff, and risk settings all
match live. This lets you verify that the engine's decisions match
what you see on the chart.

Usage:
  python replay.py                    # Replay today's session
  python replay.py --days 5           # Replay last 5 days
  python replay.py --verbose          # Show all candle data
"""

import os
import sys
import asyncio
import logging
import argparse
import json
from datetime import datetime, time as dtime
from pathlib import Path

import pytz

from project_x_py import TradingSuite
from engine import ICTEngine

# =====================================================================
# CONFIG — Loads from config.json to match run.py exactly
# =====================================================================
CONFIG_FILE = Path(__file__).parent / "config.json"

def _load_config():
    defaults = {
        "username": "",
        "api_key": "",
        "account_name": "",
        "account_type": "COMBINE",
        "instrument": "MES",
        "combine_risk": 750,
        "combine_max_contracts": 50,
        "xfa_base_risk": 750,
        "live_base_risk": 750,
        "historical_days": 5,
        "historical_interval": 5,
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

USERNAME     = CFG["username"]
API_KEY      = CFG["api_key"]
ACCOUNT_NAME = CFG["account_name"]

INSTRUMENT = CFG["instrument"]
HISTORICAL_DAYS = int(CFG["historical_days"])  # Match run.py (default: 5)
POINT_VALUE = 5.00
TZ = pytz.timezone("America/Chicago")

# =====================================================================
# ACCOUNT TYPE CONFIG — mirrors run.py exactly
# =====================================================================
ACCOUNT_TYPE = CFG["account_type"]

ACCOUNT_CONFIGS = {
    "COMBINE": {
        "base_risk": int(CFG["combine_risk"]),
        "base_max_contracts": int(CFG["combine_max_contracts"]),
        "entry_cutoff_minute": 45,
    },
    "XFA": {
        "base_risk": 750,
        "base_max_contracts": 20,
        "entry_cutoff_minute": 45,
    },
    "LIVE": {
        "base_risk": 750,
        "base_max_contracts": 999_999,
        "entry_cutoff_minute": 45,
    },
}

ACFG = ACCOUNT_CONFIGS.get(ACCOUNT_TYPE, ACCOUNT_CONFIGS["COMBINE"])

# =====================================================================
# LOGGING
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("ICTEngine").setLevel(logging.INFO)
log = logging.getLogger("Replay")

# =====================================================================
# CANDLE ADAPTER
# =====================================================================
def to_candle(row: dict) -> dict:
    ts = row.get("timestamp")
    if isinstance(ts, (int, float)):
        epoch = float(ts)
    elif isinstance(ts, datetime):
        epoch = ts.timestamp()
    elif ts is not None:
        epoch = datetime.fromisoformat(str(ts)).timestamp()
    else:
        epoch = datetime.now().timestamp()

    return {
        "timestamp": epoch,
        "open":   float(row.get("open", 0)),
        "high":   float(row.get("high", 0)),
        "low":    float(row.get("low", 0)),
        "close":  float(row.get("close", 0)),
        "volume": float(row.get("volume", 0)),
    }

# =====================================================================
# TRADE SIMULATOR (same as backtest)
# =====================================================================
class ReplaySimulator:
    def __init__(self, point_value=5.0):
        self.point_value = point_value
        self.pending_trade = None
        self.trades = []
        self.open_trade = None

    def on_trade_signal(self, side, qty, entry, sl, tp):
        self.pending_trade = {
            "side": side, "qty": qty, "entry": entry,
            "sl": sl, "tp": tp, "entry_time": None,
        }
        side_str = "BUY" if side == 0 else "SELL"
        log.info(f"🚀 SIGNAL: {side_str} {qty}x @ {entry:.2f} | SL={sl:.2f} TP={tp:.2f}")

    def activate_pending(self, candle_time):
        if self.pending_trade:
            self.pending_trade["entry_time"] = candle_time
            self.open_trade = self.pending_trade
            self.pending_trade = None

    def check_exit(self, candle) -> bool:
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
            if sl_dist <= tp_dist:
                hit_tp = False
            else:
                hit_sl = False

        if hit_sl:
            pnl = (t["sl"] - t["entry"]) if t["side"] == 0 else (t["entry"] - t["sl"])
            self._close_trade(candle, t["sl"], pnl, "SL")
            return True
        if hit_tp:
            pnl = (t["tp"] - t["entry"]) if t["side"] == 0 else (t["entry"] - t["tp"])
            self._close_trade(candle, t["tp"], pnl, "TP")
            return True
        return False

    def _close_trade(self, candle, exit_price, pnl_pts, reason):
        t = self.open_trade
        pnl_usd = pnl_pts * t["qty"] * self.point_value
        ct = datetime.fromtimestamp(candle["timestamp"], pytz.utc).astimezone(TZ)
        icon = "✅" if pnl_usd > 0 else "❌"
        side_str = "BUY" if t["side"] == 0 else "SELL"
        log.info(
            f"{icon} CLOSED: {side_str} {t['qty']}x | "
            f"Entry={t['entry']:.2f} Exit={exit_price:.2f} | "
            f"{reason} | P&L=${pnl_usd:+,.2f}"
        )
        self.trades.append({
            "side": side_str, "qty": t["qty"],
            "entry": t["entry"], "exit": exit_price,
            "pnl_pts": round(pnl_pts, 2), "pnl_usd": round(pnl_usd, 2),
            "reason": reason, "entry_time": t["entry_time"],
            "exit_time": ct.strftime("%Y-%m-%d %H:%M"),
        })
        self.open_trade = None

    def force_close_eod(self, candle):
        if not self.open_trade:
            return
        t = self.open_trade
        pnl = (candle["close"] - t["entry"]) if t["side"] == 0 else (t["entry"] - candle["close"])
        self._close_trade(candle, candle["close"], pnl, "EOD")


# =====================================================================
# MAIN
# =====================================================================
async def main():
    parser = argparse.ArgumentParser(description="Replay TopstepX data through ICT engine")
    parser.add_argument("--days", type=int, default=1, help="Days of data to replay (default: 1 = today)")
    parser.add_argument("--verbose", action="store_true", help="Log every candle")
    parser.add_argument(
        "--max-contracts",
        type=int,
        default=None,
        help=f"Override max contracts per trade (default: account config = {ACFG['base_max_contracts']})",
    )
    parser.add_argument(
        "--from-bar-log",
        default=None,
        help="Replay from a captured bar log JSONL (e.g. bars/2026-05-07.jsonl). "
             "Bypasses TopstepX entirely so live and replay are byte-identical.",
    )
    args = parser.parse_args()

    # =================================================================
    # DATA LOAD — either from captured bar log (preferred for parity)
    # or from TopstepX historical API (legacy / backfill mode).
    # =================================================================
    # When --from-bar-log is set, we partition bars by their explicit "phase"
    # marker (scan vs live) instead of the legacy day-count auto-split.
    # phase_split is None in legacy mode; a (scan_5m, live_5m, live_1m) tuple
    # in from-bar-log mode.
    phase_split = None

    if args.from_bar_log:
        log.info(f"📂 Replaying from bar log: {args.from_bar_log}")
        log.info(
            f"Account type: {ACCOUNT_TYPE} | Entry cutoff: 9:{ACFG['entry_cutoff_minute']:02d} CT | "
            f"Risk: ${ACFG['base_risk']}/trade | Max contracts: {ACFG['base_max_contracts']}"
        )
        point_value = POINT_VALUE

        # Load all records; dedupe by (phase, tf, ts) in case run.py was
        # restarted within a session and re-logged the historical scan.
        seen = {}
        with open(args.from_bar_log) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # Skip session markers (no "phase" key)
                if "phase" not in rec or "tf" not in rec or "ts" not in rec:
                    continue
                key = (rec["phase"], rec["tf"], rec["ts"])
                seen[key] = rec

        scan_5m_loaded = []
        live_5m_loaded = []
        live_1m_loaded = []
        for (phase, tf, ts), rec in seen.items():
            b = {
                "timestamp": rec["ts"],
                "open":   rec["o"],
                "high":   rec["h"],
                "low":    rec["l"],
                "close":  rec["c"],
                "volume": rec.get("v", 0),
            }
            if phase == "scan" and tf == "5min":
                scan_5m_loaded.append(b)
            elif phase == "live":
                if tf == "5min":
                    live_5m_loaded.append(b)
                elif tf == "1min":
                    live_1m_loaded.append(b)

        scan_5m_loaded.sort(key=lambda x: x["timestamp"])
        live_5m_loaded.sort(key=lambda x: x["timestamp"])
        live_1m_loaded.sort(key=lambda x: x["timestamp"])

        # bars_5m / bars_1m unified for downstream lookup; phase_split tells
        # the day-split logic which dates are scan vs replay.
        bars_5m = scan_5m_loaded + live_5m_loaded
        bars_1m = live_1m_loaded
        bars_5m.sort(key=lambda x: x["timestamp"])

        if not live_5m_loaded or not live_1m_loaded:
            log.error("No live bars found in log — cannot replay")
            return
        log.info(
            f"Loaded scan: {len(scan_5m_loaded)} 5m | "
            f"live: {len(live_5m_loaded)} 5m, {len(live_1m_loaded)} 1m"
        )
        phase_split = (scan_5m_loaded, live_5m_loaded, live_1m_loaded)

    else:
        os.environ["PROJECT_X_USERNAME"] = USERNAME
        os.environ["PROJECT_X_API_KEY"] = API_KEY
        if ACCOUNT_NAME:
            os.environ["PROJECT_X_ACCOUNT_NAME"] = ACCOUNT_NAME

        # ----- 1. Connect -----
        log.info(f"Connecting to TopstepX...")
        try:
            suite = await TradingSuite.create(
                instrument=INSTRUMENT,
                timeframes=["1min", "5min"],
            )
        except Exception as e:
            log.error(f"Connection failed: {e}")
            sys.exit(1)

        acct = suite.client.account_info
        log.info(f"✅ Account: {acct.name}")

        instrument = await suite.client.get_instrument(suite.instrument_id or INSTRUMENT)
        point_value = instrument.tickValue / instrument.tickSize if instrument.tickValue and instrument.tickSize else POINT_VALUE
        log.info(f"Instrument: {instrument.name} | Point value: ${point_value:.2f}")
        log.info(
            f"Account type: {ACCOUNT_TYPE} | Entry cutoff: 9:{ACFG['entry_cutoff_minute']:02d} CT | "
            f"Risk: ${ACFG['base_risk']}/trade | Max contracts: {ACFG['base_max_contracts']} | "
            f"Historical days: {HISTORICAL_DAYS}"
        )

        # ----- 2. Pull historical bars -----
        num_replay_days = args.days
        # Request extra calendar days to account for weekends/holidays
        total_days = HISTORICAL_DAYS + num_replay_days + 4

        log.info(f"Fetching {total_days} days of 5m bars...")
        hist_5m = await suite.client.get_bars(INSTRUMENT, days=total_days, interval=5)
        log.info(f"Fetching {total_days} days of 1m bars...")
        hist_1m = await suite.client.get_bars(INSTRUMENT, days=total_days, interval=1)

        await suite.disconnect()

        if hist_5m is None or hist_5m.is_empty() or hist_1m is None or hist_1m.is_empty():
            log.error("No data returned")
            return

        bars_5m = [to_candle(row) for row in hist_5m.iter_rows(named=True)]
        bars_1m = [to_candle(row) for row in hist_1m.iter_rows(named=True)]

        bars_5m.sort(key=lambda x: x["timestamp"])
        bars_1m.sort(key=lambda x: x["timestamp"])

        log.info(f"Loaded {len(bars_5m)} 5m bars, {len(bars_1m)} 1m bars")

    # ----- 3. Split into scan and replay by trading day -----
    def _get_session_date(ts):
        ct = datetime.fromtimestamp(ts, pytz.utc).astimezone(TZ)
        from datetime import timedelta
        if ct.hour >= 17:
            return (ct + timedelta(days=1)).date()
        return ct.date()

    all_days = sorted(set(_get_session_date(b["timestamp"]) for b in bars_5m))
    all_days = [d for d in all_days if d.weekday() < 5]

    if phase_split is not None:
        # --from-bar-log: trust the explicit phase markers instead of
        # day-count auto-split. Trading days = unique live-bar dates.
        scan_5m_loaded, live_5m_loaded, live_1m_loaded = phase_split
        replay_days_set = set(_get_session_date(b["timestamp"]) for b in live_5m_loaded)
        replay_days_set = {d for d in replay_days_set if d.weekday() < 5}
        scan_days = set(_get_session_date(b["timestamp"]) for b in scan_5m_loaded) - replay_days_set
        scan_bars = scan_5m_loaded
        replay_5m = live_5m_loaded
        replay_1m = live_1m_loaded
        if not replay_days_set:
            log.error("No live trading days found in bar log")
            return
        if not scan_bars:
            log.warning("No scan bars in log — liquidity map will be empty")
    else:
        if len(all_days) <= HISTORICAL_DAYS:
            log.error(f"Only {len(all_days)} trading days available, need >{HISTORICAL_DAYS} to replay")
            return

        scan_days = set(all_days[:HISTORICAL_DAYS])
        replay_days_set = set(all_days[HISTORICAL_DAYS:])

        scan_bars = [b for b in bars_5m if _get_session_date(b["timestamp"]) in scan_days]
        replay_5m = [b for b in bars_5m if _get_session_date(b["timestamp"]) in replay_days_set]
        replay_1m = [b for b in bars_1m if _get_session_date(b["timestamp"]) in replay_days_set]

    log.info(f"Liquidity scan: {len(scan_bars)} bars | Replay: {len(replay_5m)} 5m + {len(replay_1m)} 1m bars")

    # ----- 4. Group by trading day -----
    trading_days = sorted(replay_days_set)
    log.info(f"Trading days to replay: {len(trading_days)}")

    # Index ALL bars by date (needed for liquidity scan lookups on pre-replay days)
    from collections import defaultdict
    bars_5m_by_date = defaultdict(list)
    bars_1m_by_date = defaultdict(list)
    for b in bars_5m:
        bars_5m_by_date[_get_session_date(b["timestamp"])].append(b)
    for b in bars_1m:
        bars_1m_by_date[_get_session_date(b["timestamp"])].append(b)

    # ----- 5. Replay each day -----
    sim = ReplaySimulator(point_value=point_value)
    total_pnl = 0

    # Resolve effective max contracts (CLI override > account config)
    effective_max_contracts = args.max_contracts if args.max_contracts is not None else ACFG["base_max_contracts"]
    if args.max_contracts is not None:
        log.info(f"⚙️  Max contracts overridden via --max-contracts: {effective_max_contracts} (account default: {ACFG['base_max_contracts']})")

    for day in trading_days:
        day_5m = bars_5m_by_date.get(day, [])
        day_1m = bars_1m_by_date.get(day, [])

        if not day_5m or not day_1m:
            continue

        # Fresh engine per day — configured to match run.py
        engine = ICTEngine(account_id="REPLAY", token="REPLAY", symbol=INSTRUMENT)
        engine.point_value = point_value
        engine.entry_cutoff_minute = ACFG["entry_cutoff_minute"]
        engine.max_risk_usd = ACFG["base_risk"]
        engine.max_contracts = effective_max_contracts
        engine.order_callback = sim.on_trade_signal
        engine.trade_log.enabled = False  # Don't write to ict_trades.jsonl

        # Build liquidity bars to scan.
        #
        # The invariant: replay's scan should contain exactly what live's
        # startup get_historical_bars() returned, which is the prior
        # HISTORICAL_DAYS days of 5m bars PLUS today's overnight + pre-session
        # 5m bars (everything up to when the bot started, ~session open).
        # Earlier versions of this file excluded today entirely, which made
        # replay diverge from live whenever a pre-session fractal mattered.
        #
        # --from-bar-log: use ALL phase=scan bars (live wrote them, byte-
        # identical to what live's scan saw).
        # Legacy: include prior HISTORICAL_DAYS + today's bars BEFORE the
        # session window opens (08:25 CT). This mirrors live's behavior
        # without requiring a bar log.
        if phase_split is not None:
            liq_bars = scan_bars[:]
        else:
            day_idx = all_days.index(day)
            scan_start = max(0, day_idx - HISTORICAL_DAYS)
            liq_days = all_days[scan_start:day_idx]
            liq_bars = []
            for d in liq_days:
                liq_bars.extend(bars_5m_by_date.get(d, []))
            # Add today's pre-session bars (everything strictly before 08:25 CT
            # on the replay day). This matches what live's startup fetch
            # contains when the bot starts at session open.
            session_start_local = TZ.localize(datetime.combine(day, dtime(8, 25)))
            session_start_ts = session_start_local.timestamp()
            for b in bars_5m_by_date.get(day, []):
                if b["timestamp"] < session_start_ts:
                    liq_bars.append(b)
            if not liq_bars:
                liq_bars = scan_bars[:]

        if len(liq_bars) > 3:
            engine.scan_historical_liquidity(liq_bars)
            engine.preload_atr_history(liq_bars)

        n_high = sum(1 for s in engine.sweep_levels if s["side"] == "HIGH")
        n_low = sum(1 for s in engine.sweep_levels if s["side"] == "LOW")

        log.info(f"\n{'='*70}")
        log.info(f"📅 {day} — {len(day_5m)} 5m bars, {len(day_1m)} 1m bars | Levels: {n_high}H/{n_low}L")
        log.info(f"{'='*70}")

        trades_before = len(sim.trades)

        # Interleave 5m and 1m events chronologically — matches backtest exactly.
        # Engine's own session check handles filtering (in_session, in_sweep_window).
        events = [("5m", b) for b in day_5m] + [("1m", b) for b in day_1m]
        events.sort(key=lambda x: x[1]["timestamp"])

        for tf, bar in events:
            ct = datetime.fromtimestamp(bar["timestamp"], pytz.utc).astimezone(TZ)
            in_session = True

            if args.verbose and in_session:
                log.info(
                    f"  {tf:>2} {ct.strftime('%H:%M')} | "
                    f"O={bar['open']:.2f} H={bar['high']:.2f} "
                    f"L={bar['low']:.2f} C={bar['close']:.2f} | "
                    f"Stage={engine.strategy_stage}"
                )

            if sim.open_trade and tf == "1m":
                if sim.check_exit(bar):
                    continue
            if sim.pending_trade and not sim.open_trade:
                sim.activate_pending(ct.strftime("%Y-%m-%d %H:%M"))

            if tf == "5m":
                engine.process_5m_candle(bar)
            elif tf == "1m":
                engine.process_1m_candle(bar)

        # EOD close
        if sim.open_trade and day_1m:
            sim.force_close_eod(day_1m[-1])

        traded = len(sim.trades) > trades_before
        if traded:
            last_trade = sim.trades[-1]
            log.info(f"  📊 Day result: ${last_trade['pnl_usd']:+,.2f} ({last_trade['reason']})")
        else:
            log.info(f"  ⬜ No trade — stuck at {engine.strategy_stage}")

    # ----- 6. Summary -----
    print(f"\n{'='*60}")
    print(f"  REPLAY RESULTS — {INSTRUMENT}")
    print(f"{'='*60}")
    if sim.trades:
        wins = [t for t in sim.trades if t["pnl_usd"] > 0]
        losses = [t for t in sim.trades if t["pnl_usd"] <= 0]
        total = sum(t["pnl_usd"] for t in sim.trades)
        wr = len(wins) / len(sim.trades) * 100

        print(f"  Trades: {len(sim.trades)} | Wins: {len(wins)} ({wr:.0f}%) | Losses: {len(losses)}")
        print(f"  Net P&L: ${total:+,.2f}")
        print()
        print(f"  {'#':<4} {'Side':<5} {'Qty':<4} {'Entry':>9} {'Exit':>9} {'P&L':>10} {'Reason':<4} {'Time'}")
        print(f"  {'─'*60}")
        for i, t in enumerate(sim.trades, 1):
            print(f"  {i:<4} {t['side']:<5} {t['qty']:<4} {t['entry']:>9.2f} {t['exit']:>9.2f} ${t['pnl_usd']:>+9.2f} {t['reason']:<4} {t['exit_time']}")
    else:
        print("  No trades generated.")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
