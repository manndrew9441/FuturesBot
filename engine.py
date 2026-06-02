"""
engine.py — ICT Strategy Engine (v2 Rewrite)

Strategy narrative:
  1. Market opens, price sweeps a liquidity level (fake move, traps retail)
  2. Watch reversal legs. Each leg is a swing (high-to-low or low-to-high).
     When a leg BREAKS STRUCTURE (closes past the prior leg's extreme), 
     that confirms the real direction.
  3. The BOS leg is where we mark EQ and FVGs (internal liquidity / fuel)
  4. Wait for price to retrace into that internal liquidity
  5. On 1m, wait for BOS or IFVG confirmation that the fuel is being used
  6. Enter there. SL = 1m swing high/low at entry. TP = external liquidity draw.

Funnel:
  WAITING → SWEPT → CONFIRMED_SHIFT → RETRACED → (1m entry)

Key design: the bot tracks 5m swing structure naturally. After a sweep,
it watches legs form. A "leg" is a move from one 5m swing point to another.
BOS = a leg closes past the prior swing on the opposite side.
"""

import math
import json
import logging
from datetime import datetime
import pytz

log = logging.getLogger("ICTEngine")


class TradeLogger:
    def __init__(self, filename="ict_trades.jsonl", enabled=True):
        self.filename = filename
        self.enabled = enabled

    def log(self, event_type, data):
        if self.enabled:
            entry = {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "event": event_type.upper(),
                **data,
            }
            with open(self.filename, "a") as f:
                f.write(json.dumps(entry) + "\n")
        log.info(f"[{event_type.upper()}] {data.get('message', json.dumps(data))}")


class ICTEngine:
    def __init__(self, account_id, token, symbol="MES"):
        self.account_id = account_id
        self.token = token
        self.symbol = symbol
        self.tz = pytz.timezone("America/Chicago")
        self.trade_log = TradeLogger()
        self.order_callback = None
        self.live_mode = False  # When True, run.py increments daily_trade_count after confirmed fill

        # --- Session Window ---
        # entry_cutoff: minute past 9:00 CT to stop accepting entries
        # Compounding mode (live):     45 (9:45 CT) — more trades, higher volume
        self.entry_cutoff_minute = 45  # Default: full window

        # --- Data Buffers ---
        self.candles_5m = []
        self.candles_1m = []

        # --- Liquidity ---
        self.sweep_levels = []
        self.liquidity_pools = {"HIGHS": [], "LOWS": [], "HOURLY": [], "STRUCTURAL": []}

        # --- 5m Swing Tracking ---
        self.swing_highs = []  # [{price, index, timestamp}]
        self.swing_lows = []

        # --- Strategy State ---
        self.strategy_stage = "WAITING"
        self.active_sweep = None
        self.pending_sweeps = []
        self.pending_side = None
        self.bos_leg = None  # {high, low} — the leg that broke structure
        self.bos_leg_fvgs = []  # FVGs within the BOS leg [{top, bottom, type}]
        self.stage_entry_time = None
        self.retracement_extreme = None  # Deepest point price pushed into EQ/FVG
        self.sweep_candle_extreme = None  # The extreme price reached on the sweep candle(s)
        self._bos_just_confirmed = False  # Skip EQ check on the BOS candle (EQ not known yet)
        # 1m swing tracking for proper BOS detection
        # A swing low = lowest point where a down candle is followed by an up candle
        # A wick below doesn't count — it becomes the new level to break
        self.swing_low_1m = None   # Current 1m swing low price to break for bearish BOS
        self.swing_high_1m = None  # Current 1m swing high price to break for bullish BOS

        # --- Risk & Position ---
        self.max_risk_usd = 750.0
        self.max_stop_pts = 15.0
        self.max_contracts = 50
        self.tp_rr_min = 1.7
        self.tp_rr_max = 2.2
        self.point_value = 5.00
        self.daily_trade_count = 0
        self.max_daily_trades = 1
        self.last_trade_date = None
        self.active_position = None

        # --- F4 day-quality filter ---
        # Skip entries when the morning's realized range (08:25–08:50 CT) is
        # too small in absolute pts AND/OR too small relative to the recent
        # 5-day ATR of the bot's window (08:25–09:45 CT).
        # Source: 5y MES backtest — F4 saved $1.74M with 35.7% WR on the
        # filtered trades vs 54% baseline.
        self.f4_enabled = True
        self.f4_min_realized_pts = 8.0       # absolute floor on 8:50 realized range
        self.f4_min_atr_ratio = 0.25         # 8:50 realized range must be ≥ 25% of 5d ATR
        self.f4_atr_lookback_days = 5
        self.f4_atr_history = []             # list of past N days' bot-window ranges (in pts)
        self.f4_morning_high = None          # tracked from today's 1m candles 08:25–08:50
        self.f4_morning_low = None
        self._f4_skipped_today = False       # log gate so we only print the rejection once

    # ==================================================================
    # TIME
    # ==================================================================
    def parse_candle_time(self, ts):
        return datetime.fromtimestamp(ts, pytz.utc).astimezone(self.tz)

    # ==================================================================
    # STATE
    # ==================================================================
    def _log_stage(self, old, new, reason=""):
        """Single-line stage transition log — the forensic backbone."""
        if old != new:
            extra = f" | {reason}" if reason else ""
            log.info(f"🔀 STAGE {old} → {new}{extra}")

    def _bar_ct(self, candle):
        """Format a candle's CT timestamp for logging."""
        return self.parse_candle_time(candle["timestamp"]).strftime("%H:%M")

    def reset_state(self, reason="Manual"):
        old = self.strategy_stage
        log.info(f"🔄 Reset → WAITING | {reason}")
        self.strategy_stage = "WAITING"
        self.active_sweep = None
        self.pending_sweeps = []
        self.pending_side = None
        self.bos_leg = None
        self.bos_leg_fvgs = []
        self.stage_entry_time = None
        self.retracement_extreme = None
        self.sweep_candle_extreme = None
        self._bos_just_confirmed = False
        self.swing_low_1m = None
        self.swing_high_1m = None
        self._f4_debug_logged_today = False

    # ==================================================================
    # POSITION SIZING
    # ==================================================================
    def calculate_qty(self, entry, stop):
        risk_pts = abs(entry - stop)
        if risk_pts == 0:
            return 0
        qty = math.floor(self.max_risk_usd / (risk_pts * self.point_value))
        qty = max(1, qty)
        if qty > self.max_contracts:
            log.info(f"📉 Qty {qty} clamped to {self.max_contracts} (cap) — risk reduced to ${self.max_contracts * risk_pts * self.point_value:.2f}")
            qty = self.max_contracts
        return qty

    # ==================================================================
    # LIQUIDITY SCANNING
    # ==================================================================
    def scan_historical_liquidity(self, bars):
        log.info(f"🔍 Scanning {len(bars)} bars for liquidity...")

        fractal_highs = []
        fractal_lows = []

        for i in range(1, len(bars) - 1):
            c = bars[i]
            if c["high"] > bars[i - 1]["high"] and c["high"] > bars[i + 1]["high"]:
                fractal_highs.append((c["high"], i))
                self.liquidity_pools["HIGHS"].append(c["high"])
            if c["low"] < bars[i - 1]["low"] and c["low"] < bars[i + 1]["low"]:
                fractal_lows.append((c["low"], i))
                self.liquidity_pools["LOWS"].append(c["low"])

        for i in range(12, len(bars) - 12):
            window = bars[i - 12: i + 13]
            c = bars[i]
            if c["high"] == max(b["high"] for b in window):
                self.liquidity_pools["HOURLY"].append(c["high"])
            if c["low"] == min(b["low"] for b in window):
                self.liquidity_pools["HOURLY"].append(c["low"])

        self._extract_structural_levels(bars)

        for k in self.liquidity_pools:
            self.liquidity_pools[k] = sorted(list(set(self.liquidity_pools[k])))

        total_bars = len(bars)
        proximity = 1.5
        structural_set = set(self.liquidity_pools["STRUCTURAL"])
        hourly_set = set(self.liquidity_pools["HOURLY"])

        def cleanness(price, idx, side):
            clean = 0
            for j in range(idx + 2, total_bars):
                if side == "HIGH" and bars[j]["high"] >= price - proximity:
                    break
                if side == "LOW" and bars[j]["low"] <= price + proximity:
                    break
                clean += 1
            return clean

        for price, idx in fractal_highs:
            w = cleanness(price, idx, "HIGH")
            if price in structural_set: w += 50
            if price in hourly_set: w += 20
            nearby = sum(1 for p, _ in fractal_highs if abs(p - price) <= 2.5 and p != price)
            w += nearby * 5
            if w >= 3:
                src = "structural" if price in structural_set else ("hourly" if price in hourly_set else "fractal")
                self.sweep_levels.append({"price": price, "side": "HIGH", "weight": w, "source": src})

        for price, idx in fractal_lows:
            w = cleanness(price, idx, "LOW")
            if price in structural_set: w += 50
            if price in hourly_set: w += 20
            nearby = sum(1 for p, _ in fractal_lows if abs(p - price) <= 2.5 and p != price)
            w += nearby * 5
            if w >= 3:
                src = "structural" if price in structural_set else ("hourly" if price in hourly_set else "fractal")
                self.sweep_levels.append({"price": price, "side": "LOW", "weight": w, "source": src})

        for lvl in self.liquidity_pools["STRUCTURAL"]:
            if not any(abs(s["price"] - lvl) < 0.5 for s in self.sweep_levels):
                self.sweep_levels.append({"price": lvl, "side": "HIGH", "weight": 50, "source": "structural"})
                self.sweep_levels.append({"price": lvl, "side": "LOW", "weight": 50, "source": "structural"})

        self.sweep_levels.sort(key=lambda x: x["weight"], reverse=True)
        n_h = sum(1 for s in self.sweep_levels if s["side"] == "HIGH")
        n_l = sum(1 for s in self.sweep_levels if s["side"] == "LOW")
        log.info(f"📊 Sweep levels: {n_h} highs, {n_l} lows")

    def _extract_structural_levels(self, bars):
        tz = self.tz
        daily = {}
        for b in bars:
            dt = datetime.fromtimestamp(b["timestamp"], pytz.utc).astimezone(tz)
            day = dt.date()
            if day not in daily:
                daily[day] = {"high": b["high"], "low": b["low"], "bars": []}
            daily[day]["high"] = max(daily[day]["high"], b["high"])
            daily[day]["low"] = min(daily[day]["low"], b["low"])
            daily[day]["bars"].append((dt, b))

        sorted_days = sorted(daily.keys())
        for i in range(1, len(sorted_days)):
            prev = daily[sorted_days[i - 1]]
            self.liquidity_pools["STRUCTURAL"].append(prev["high"])
            self.liquidity_pools["STRUCTURAL"].append(prev["low"])

        weekly = {}
        for day, data in daily.items():
            wk = day.isocalendar()[:2]
            if wk not in weekly:
                weekly[wk] = {"high": data["high"], "low": data["low"]}
            weekly[wk]["high"] = max(weekly[wk]["high"], data["high"])
            weekly[wk]["low"] = min(weekly[wk]["low"], data["low"])
        sorted_wks = sorted(weekly.keys())
        for i in range(1, len(sorted_wks)):
            prev = weekly[sorted_wks[i - 1]]
            self.liquidity_pools["STRUCTURAL"].append(prev["high"])
            self.liquidity_pools["STRUCTURAL"].append(prev["low"])

        for day in sorted_days:
            on_h, on_l = None, None
            for dt, b in daily[day]["bars"]:
                if dt.hour < 8 or (dt.hour == 8 and dt.minute < 30) or dt.hour >= 17:
                    on_h = max(on_h, b["high"]) if on_h else b["high"]
                    on_l = min(on_l, b["low"]) if on_l else b["low"]
            if on_h: self.liquidity_pools["STRUCTURAL"].append(on_h)
            if on_l: self.liquidity_pools["STRUCTURAL"].append(on_l)

    # ==================================================================
    # BREAK-EVEN
    # ==================================================================
    # Break-even is not implemented — backtest results achieved without it.
    # If added later, it would need an SDK callback to modify the real stop order.

    # ==================================================================
    # 5m SWING DETECTION
    # ==================================================================
    def _update_swings(self):
        """Detect 5m fractal swing points with 1-bar confirmation."""
        if len(self.candles_5m) < 3:
            return
        i = len(self.candles_5m) - 2  # Check the bar before current (needs both neighbors)
        bar = self.candles_5m[i]
        prev_bar = self.candles_5m[i - 1]
        next_bar = self.candles_5m[i + 1]  # Current candle confirms

        if bar["high"] > prev_bar["high"] and bar["high"] > next_bar["high"]:
            self.swing_highs.append({"price": bar["high"], "index": i, "timestamp": bar["timestamp"]})
        if bar["low"] < prev_bar["low"] and bar["low"] < next_bar["low"]:
            self.swing_lows.append({"price": bar["low"], "index": i, "timestamp": bar["timestamp"]})

    # ==================================================================
    # FVG DETECTION WITHIN BOS LEG
    # ==================================================================
    def _find_bos_leg_fvgs(self):
        """
        Scan recent completed 5m candles for FVGs (fair value gaps) that fall
        within the BOS leg's range. These are internal liquidity / fuel zones.
        
        A bullish FVG: candle3.low > candle1.high (gap up)
        A bearish FVG: candle3.high < candle1.low (gap down)
        
        Rules:
          - Minimum gap size of 1.0 points to filter microstructure noise
          - Must overlap with the BOS leg range
        """
        self.bos_leg_fvgs = []
        if not self.bos_leg or len(self.candles_5m) < 3:
            return

        leg_high = self.bos_leg["high"]
        leg_low = self.bos_leg["low"]
        min_gap = 1.0  # Minimum FVG size in points

        for i in range(max(2, len(self.candles_5m) - 20), len(self.candles_5m)):
            if i < 2:
                continue
            c1 = self.candles_5m[i - 2]
            c2 = self.candles_5m[i - 1]
            c3 = self.candles_5m[i]

            # Bullish FVG (gap up)
            if c3["low"] > c1["high"]:
                fvg_top = c3["low"]
                fvg_bottom = c1["high"]
                if (fvg_top - fvg_bottom) >= min_gap and fvg_bottom < leg_high and fvg_top > leg_low:
                    self.bos_leg_fvgs.append({
                        "top": fvg_top, "bottom": fvg_bottom, "type": "BULL"
                    })

            # Bearish FVG (gap down)
            if c3["high"] < c1["low"]:
                fvg_top = c1["low"]
                fvg_bottom = c3["high"]
                if (fvg_top - fvg_bottom) >= min_gap and fvg_bottom < leg_high and fvg_top > leg_low:
                    self.bos_leg_fvgs.append({
                        "top": fvg_top, "bottom": fvg_bottom, "type": "BEAR"
                    })

        if self.bos_leg_fvgs:
            for fvg in self.bos_leg_fvgs:
                log.info(f"   📦 {fvg['type']} FVG: {fvg['bottom']:.2f}-{fvg['top']:.2f}")

    # ==================================================================
    # 5-MINUTE PROCESSOR — THE NARRATIVE
    # ==================================================================
    def process_5m_candle(self, new_candle):
        self.candles_5m.append(new_candle)
        if len(self.candles_5m) > 200:
            self.candles_5m = self.candles_5m[-200:]
        if len(self.candles_5m) < 3:
            return

        c_time = self.parse_candle_time(new_candle["timestamp"])
        self._bos_just_confirmed = False

        # Daily limit gates execution, not analysis
        already_traded = (self.last_trade_date == c_time.date() and self.daily_trade_count >= self.max_daily_trades)
        if already_traded:
            return

        if self.active_position:
            return  # Position/execution pending — don't advance strategy

        # Session: sweeps 8:25-9:00, everything else until cutoff
        in_session = (c_time.hour == 8 and c_time.minute >= 25) or (c_time.hour == 9 and c_time.minute <= self.entry_cutoff_minute)
        if not in_session:
            if self.strategy_stage != "WAITING":
                self.reset_state("Outside session")
            return

        in_sweep_window = (c_time.hour == 8 and c_time.minute >= 25) or (c_time.hour == 9 and c_time.minute == 0)

        # Update swing structure
        self._update_swings()

        # =============================================================
        # STEP 1: SWEEP DETECTION
        # =============================================================
        if self.strategy_stage in ("WAITING", "SWEPT") and in_sweep_window:
            for lvl in self.sweep_levels[:]:
                swept = False
                if lvl["side"] == "HIGH" and new_candle["high"] > lvl["price"]:
                    swept, stype = True, "BUY_SIDE"
                elif lvl["side"] == "LOW" and new_candle["low"] < lvl["price"]:
                    swept, stype = True, "SELL_SIDE"
                if swept:
                    log.info(f"🌊 Sweep {'HIGH' if stype == 'BUY_SIDE' else 'LOW'}: {lvl['price']:.2f} (w={lvl['weight']})")
                    self.pending_sweeps.append({"level": lvl["price"], "type": stype, "weight": lvl["weight"]})
                    self.sweep_levels.remove(lvl)

            if self.pending_sweeps and self.strategy_stage == "WAITING":
                self.strategy_stage = "SWEPT"
                self.stage_entry_time = new_candle["timestamp"]
                # Track the extreme of the sweep candle(s) — used for re-sweep detection
                self.sweep_candle_extreme = {
                    "high": new_candle["high"],
                    "low": new_candle["low"],
                }
                n_buy = sum(1 for s in self.pending_sweeps if s["type"] == "BUY_SIDE")
                n_sell = sum(1 for s in self.pending_sweeps if s["type"] == "SELL_SIDE")
                self._log_stage("WAITING", "SWEPT",
                                f"@5m{self._bar_ct(new_candle)} buy={n_buy} sell={n_sell}")
            elif self.strategy_stage == "SWEPT" and self.sweep_candle_extreme:
                # Extend sweep extreme if more sweeps happen
                self.sweep_candle_extreme["high"] = max(self.sweep_candle_extreme["high"], new_candle["high"])
                self.sweep_candle_extreme["low"] = min(self.sweep_candle_extreme["low"], new_candle["low"])

        # =============================================================
        # STEP 2: WATCH FOR BOS
        # After sweep, watch 5m swings form. BOS = close past a prior
        # swing point in the reversal direction.
        # FIX 2: When both sides are swept, only confirm BOS for the
        # side with the higher-weight sweep.
        # =============================================================
        if self.strategy_stage == "SWEPT" and self.stage_entry_time:
            post_highs = [sh for sh in self.swing_highs if sh["timestamp"] >= self.stage_entry_time]
            post_lows = [sl for sl in self.swing_lows if sl["timestamp"] >= self.stage_entry_time]

            buy_side = [s for s in self.pending_sweeps if s["type"] == "BUY_SIDE"]
            sell_side = [s for s in self.pending_sweeps if s["type"] == "SELL_SIDE"]

            confirmed = False

            # BEARISH BOS: swept a high → close below post-sweep swing low
            if post_lows and buy_side:
                latest_sl = post_lows[-1]
                if new_candle["close"] < latest_sl["price"]:
                    best = max(buy_side, key=lambda s: s.get("weight", 1))
                    leg_high = max(sh["price"] for sh in post_highs) if post_highs else new_candle["high"]
                    self.active_sweep = best
                    self.pending_side = 1
                    self.bos_leg = {"high": leg_high, "low": new_candle["low"]}
                    self._find_bos_leg_fvgs()
                    self.pending_sweeps = []
                    self.strategy_stage = "CONFIRMED_SHIFT"
                    self._bos_just_confirmed = True
                    fvg_str = f" | {len(self.bos_leg_fvgs)} FVGs in leg" if self.bos_leg_fvgs else ""
                    leg_size = leg_high - new_candle["low"]
                    log.info(f"📉 BEARISH BOS — close < swing low {latest_sl['price']:.2f} | Leg: {leg_high:.2f}→{new_candle['low']:.2f} ({leg_size:.2f}pt){fvg_str}")
                    self._log_stage("SWEPT", "CONFIRMED_SHIFT",
                                    f"@5m{self._bar_ct(new_candle)} BEARISH leg={leg_high:.2f}→{new_candle['low']:.2f}")
                    confirmed = True

            # BULLISH BOS: swept a low → close above post-sweep swing high
            if not confirmed and post_highs and sell_side:
                latest_sh = post_highs[-1]
                if new_candle["close"] > latest_sh["price"]:
                    best = max(sell_side, key=lambda s: s.get("weight", 1))
                    leg_low = min(sl["price"] for sl in post_lows) if post_lows else new_candle["low"]
                    self.active_sweep = best
                    self.pending_side = 0
                    self.bos_leg = {"high": new_candle["high"], "low": leg_low}
                    self._find_bos_leg_fvgs()
                    self.pending_sweeps = []
                    self.strategy_stage = "CONFIRMED_SHIFT"
                    self._bos_just_confirmed = True
                    fvg_str = f" | {len(self.bos_leg_fvgs)} FVGs in leg" if self.bos_leg_fvgs else ""
                    leg_size = new_candle["high"] - leg_low
                    log.info(f"📈 BULLISH BOS — close > swing high {latest_sh['price']:.2f} | Leg: {leg_low:.2f}→{new_candle['high']:.2f} ({leg_size:.2f}pt){fvg_str}")
                    self._log_stage("SWEPT", "CONFIRMED_SHIFT",
                                    f"@5m{self._bar_ct(new_candle)} BULLISH leg={leg_low:.2f}→{new_candle['high']:.2f}")
                    confirmed = True

        # =============================================================
        # STEP 3: RETRACEMENT INTO INTERNAL LIQUIDITY
        # Price must come back to EITHER:
        #   a) EQ (50% of BOS leg) — discount/premium
        #   b) An FVG within the BOS leg — imbalance / fuel
        # Whichever is hit first qualifies as getting fuel.
        # Track the deepest retracement point for 1m SL.
        #
        # NOTE: EQ is derived from the BOS candle's extremes, so checking
        # EQ on the same candle that confirmed BOS is logically invalid.
        # FVG retracement on the BOS candle is fine — FVGs predate the BOS.
        # =============================================================
        if self.strategy_stage == "CONFIRMED_SHIFT" and self.bos_leg:
            eq = (self.bos_leg["high"] + self.bos_leg["low"]) / 2
            hit_internal = False
            hit_reason = ""

            if self.pending_side == 0:  # Long — retracement = price dropping back
                if not self._bos_just_confirmed and new_candle["low"] <= eq:
                    hit_internal = True
                    hit_reason = f"EQ({eq:.2f})"
                else:
                    for fvg in self.bos_leg_fvgs:
                        if new_candle["low"] <= fvg["top"]:
                            hit_internal = True
                            hit_reason = f"FVG({fvg['bottom']:.2f}-{fvg['top']:.2f})"
                            break
                if hit_internal:
                    self.retracement_extreme = new_candle["low"]

            elif self.pending_side == 1:  # Short — retracement = price rising back
                if not self._bos_just_confirmed and new_candle["high"] >= eq:
                    hit_internal = True
                    hit_reason = f"EQ({eq:.2f})"
                else:
                    for fvg in self.bos_leg_fvgs:
                        if new_candle["high"] >= fvg["bottom"]:
                            hit_internal = True
                            hit_reason = f"FVG({fvg['bottom']:.2f}-{fvg['top']:.2f})"
                            break
                if hit_internal:
                    self.retracement_extreme = new_candle["high"]

            if hit_internal:
                log.info(f"🎯 Retracement into {hit_reason} → 1m trigger")
                self._log_stage("CONFIRMED_SHIFT", "RETRACED",
                                f"@5m{self._bar_ct(new_candle)} {hit_reason} ext={self.retracement_extreme:.2f}")
                self.strategy_stage = "RETRACED"

        # Track retracement extreme while in RETRACED (price may push deeper)
        if self.strategy_stage == "RETRACED" and self.retracement_extreme is not None:
            if self.pending_side == 0:
                self.retracement_extreme = min(self.retracement_extreme, new_candle["low"])
            else:
                self.retracement_extreme = max(self.retracement_extreme, new_candle["high"])

        # =============================================================
        # STEP 4: INVALIDATION
        # Invalid if retracement pushes past 100% of the BOS leg.
        # For longs: price drops below the BOS leg low (the sweep area)
        # For shorts: price rises above the BOS leg high
        # This means the internal liquidity couldn't hold — thesis is dead.
        # =============================================================
        if self.strategy_stage in ("CONFIRMED_SHIFT", "RETRACED") and self.bos_leg:
            invalid = False
            if self.pending_side == 0 and new_candle["close"] < self.bos_leg["low"]:
                invalid = True
            elif self.pending_side == 1 and new_candle["close"] > self.bos_leg["high"]:
                invalid = True
            if invalid:
                log.info(f"🚫 Invalidated — retracement pushed past 100% of BOS leg")
                self.reset_state("Invalidated — past BOS leg")

    # ==================================================================
    # TP TARGETING — External draw on liquidity
    # ==================================================================
    def _find_tp_target(self, entry, sl, risk_pts):
        min_dist = risk_pts * self.tp_rr_min
        max_dist = risk_pts * self.tp_rr_max
        if self.pending_side == 0:
            tp_min, tp_max = entry + min_dist, entry + max_dist
        else:
            tp_min, tp_max = entry - max_dist, entry - min_dist

        best, best_pri = None, 999
        for pool, pri in [("STRUCTURAL", 0), ("HOURLY", 1), ("HIGHS" if self.pending_side == 1 else "LOWS", 2)]:
            for lvl in self.liquidity_pools.get(pool, []):
                if tp_min <= lvl <= tp_max:
                    if pri < best_pri or (pri == best_pri and (
                        (self.pending_side == 0 and (best is None or lvl > best)) or
                        (self.pending_side == 1 and (best is None or lvl < best)))):
                        best, best_pri = lvl, pri

        if best:
            log.info(f"🎯 TP @ external liquidity {best:.2f} ({abs(best - entry) / risk_pts:.1f}R)")
            return best
        fb = (self.tp_rr_min + self.tp_rr_max) / 2
        tp = entry + (risk_pts * fb) if self.pending_side == 0 else entry - (risk_pts * fb)
        log.info(f"🎯 TP fallback {fb:.1f}R @ {tp:.2f}")
        return tp

    # ==================================================================
    # F4 DAY-QUALITY FILTER
    # ==================================================================
    def preload_atr_history(self, bars):
        """
        Seed F4 ATR history from historical bars (typically the same scan_bars
        passed to scan_historical_liquidity). Computes each prior day's range
        during the bot's trading window (08:25–09:45 CT) and stores the most
        recent N days for the relative-range check.

        Call this once at session start, AFTER scan_historical_liquidity.
        """
        by_date = {}
        for b in bars:
            ts = self.parse_candle_time(b["timestamp"])
            mins = ts.hour * 60 + ts.minute
            if 8*60 + 25 <= mins <= 9*60 + 45:
                d = ts.date()
                if d not in by_date:
                    by_date[d] = {"high": b["high"], "low": b["low"]}
                else:
                    by_date[d]["high"] = max(by_date[d]["high"], b["high"])
                    by_date[d]["low"] = min(by_date[d]["low"], b["low"])

        sorted_dates = sorted(by_date.keys())
        ranges = [by_date[d]["high"] - by_date[d]["low"] for d in sorted_dates]
        self.f4_atr_history = ranges[-self.f4_atr_lookback_days:] if ranges else []
        if self.f4_atr_history:
            avg = sum(self.f4_atr_history) / len(self.f4_atr_history)
            # Log per-date ranges for cross-source divergence diagnostics
            recent = sorted_dates[-self.f4_atr_lookback_days:]
            details = ", ".join(
                f"{d}={by_date[d]['high'] - by_date[d]['low']:.2f}"
                f"(H={by_date[d]['high']:.2f}/L={by_date[d]['low']:.2f})"
                for d in recent
            )
            log.info(
                f"📊 F4 ATR seeded from {len(self.f4_atr_history)} prior days → avg={avg:.2f}pt | {details}"
            )
        else:
            log.info("📊 F4 ATR history empty — relative check disabled until tomorrow")

    def _update_f4_morning_range(self, candle):
        """Track today's 08:25–08:50 CT high/low from incoming 1m candles."""
        c_time = self.parse_candle_time(candle["timestamp"])
        mins = c_time.hour * 60 + c_time.minute
        if 8*60 + 25 <= mins < 8*60 + 50:
            if self.f4_morning_high is None:
                self.f4_morning_high = candle["high"]
                self.f4_morning_low = candle["low"]
            else:
                self.f4_morning_high = max(self.f4_morning_high, candle["high"])
                self.f4_morning_low = min(self.f4_morning_low, candle["low"])

    def _f4_check(self, c_time):
        """
        Returns True if the entry should be allowed under F4.
        - Pre-08:50: no morning data yet → allow.
        - Post-08:50 AND no morning data captured (e.g. backtest started mid-session) → allow.
        - Otherwise, require both:
            (a) realized 08:25–08:50 range ≥ f4_min_realized_pts
            (b) realized range ≥ f4_min_atr_ratio × 5d-ATR (only when ATR history present)

        Note: realized range and ATR are quantized to 1-tick (0.25pt) precision
        before comparison so the decision is deterministic across data sources
        whose underlying bars may differ by sub-tick amounts (TopstepX vs Databento).
        """
        if not self.f4_enabled:
            return True

        mins = c_time.hour * 60 + c_time.minute
        if mins < 8*60 + 50:
            return True
        if self.f4_morning_high is None or self.f4_morning_low is None:
            return True

        # Quantize realized range to 1-tick precision (0.25pt)
        realized = round((self.f4_morning_high - self.f4_morning_low) * 4) / 4

        # One-time debug log on first check past 08:50 — captures all F4 inputs
        # so we can diff replay.py vs backtest_replay.py for the same date.
        if not getattr(self, "_f4_debug_logged_today", False):
            atr5_dbg = (
                round((sum(self.f4_atr_history) / len(self.f4_atr_history)) * 4) / 4
                if self.f4_atr_history else 0.0
            )
            ratio_dbg = realized / atr5_dbg if atr5_dbg > 0 else 0.0
            log.info(
                f"🔬 F4 inputs @ {c_time.strftime('%Y-%m-%d %H:%M')} | "
                f"morning_H={self.f4_morning_high:.2f} morning_L={self.f4_morning_low:.2f} "
                f"realized={realized:.2f}pt | atr5={atr5_dbg:.2f}pt ratio={ratio_dbg:.4f}"
            )
            self._f4_debug_logged_today = True

        if realized < self.f4_min_realized_pts:
            if not self._f4_skipped_today:
                log.info(
                    f"⛔ F4: realized 08:50 range {realized:.2f}pt < "
                    f"{self.f4_min_realized_pts}pt absolute floor — entries blocked today"
                )
                self._f4_skipped_today = True
            return False

        if self.f4_atr_history:
            # Quantize 5d ATR to 1-tick precision before ratio comparison
            atr5 = round((sum(self.f4_atr_history) / len(self.f4_atr_history)) * 4) / 4
            if atr5 > 0:
                ratio = realized / atr5
                if ratio < self.f4_min_atr_ratio:
                    if not self._f4_skipped_today:
                        log.info(
                            f"⛔ F4: realized 08:50 range {realized:.2f}pt / 5d ATR {atr5:.2f}pt = "
                            f"{ratio:.0%} < {self.f4_min_atr_ratio:.0%} threshold — entries blocked today"
                        )
                        self._f4_skipped_today = True
                    return False

        return True

    # ==================================================================
    # 1m IFVG
    # ==================================================================
    def _detect_1m_ifvg(self):
        """Detect inversed FVG on the last 3 completed 1m candles."""
        if len(self.candles_1m) < 3:
            return None
        c1, c2, c3 = self.candles_1m[-3], self.candles_1m[-2], self.candles_1m[-1]
        min_gap = 0.5  # Minimum IFVG size in points
        if c3["low"] > c1["high"] and (c3["low"] - c1["high"]) >= min_gap and c3["close"] < c1["high"]:
            return "BEAR_IFVG"
        if c3["high"] < c1["low"] and (c1["low"] - c3["high"]) >= min_gap and c3["close"] > c1["low"]:
            return "BULL_IFVG"
        return None

    # ==================================================================
    # 1-MINUTE PROCESSOR — THE ENTRY
    # ==================================================================
    def process_1m_candle(self, ltf_candle):
        self.candles_1m.append(ltf_candle)
        if len(self.candles_1m) > 500:
            self.candles_1m = self.candles_1m[-500:]

        # Track today's morning range (08:25–08:50 CT) for the F4 filter.
        # Runs every 1m candle, regardless of stage or position state.
        self._update_f4_morning_range(ltf_candle)

        if len(self.candles_1m) < 2 or self.strategy_stage != "RETRACED":
            return
        if self.active_position:
            return  # Position/execution pending — don't fire another signal

        ltf_prev = self.candles_1m[-2]
        c_time = self.parse_candle_time(ltf_candle["timestamp"])

        if c_time.hour == 9 and c_time.minute >= self.entry_cutoff_minute:
            self.reset_state(f"9:{self.entry_cutoff_minute:02d} deadline")
            return
        if c_time.hour >= 10:
            self.reset_state("Past 10:00")
            return

        # F4 day-quality filter — block entries on weak-range mornings.
        if not self._f4_check(c_time):
            return

        # --- Invalidation is handled on 5m. On 1m we just look for the trigger. ---

        entry = ltf_candle["close"]
        sl = None
        trigger_type = ""

        # 1m BOS — simple: close beyond prev bar's high/low
        if self.pending_side == 0 and entry > ltf_prev["high"]:
            sl = self.retracement_extreme if self.retracement_extreme else min(c["low"] for c in self.candles_1m[-10:])
            trigger_type = "1m BOS"
            log.info(f"   1m BOS LONG: close={entry:.2f} > prev_high={ltf_prev['high']:.2f} | prev_OHLC=({ltf_prev['open']:.2f},{ltf_prev['high']:.2f},{ltf_prev['low']:.2f},{ltf_prev['close']:.2f}) | SL={sl:.2f}")
        elif self.pending_side == 1 and entry < ltf_prev["low"]:
            sl = self.retracement_extreme if self.retracement_extreme else max(c["high"] for c in self.candles_1m[-10:])
            trigger_type = "1m BOS"
            log.info(f"   1m BOS SHORT: close={entry:.2f} < prev_low={ltf_prev['low']:.2f} | prev_OHLC=({ltf_prev['open']:.2f},{ltf_prev['high']:.2f},{ltf_prev['low']:.2f},{ltf_prev['close']:.2f}) | SL={sl:.2f}")

        # 1m IFVG
        if sl is None and len(self.candles_1m) >= 3:
            ifvg = self._detect_1m_ifvg()
            if ifvg == "BULL_IFVG" and self.pending_side == 0:
                sl = self.retracement_extreme if self.retracement_extreme else min(c["low"] for c in self.candles_1m[-10:])
                trigger_type = "1m IFVG"
            elif ifvg == "BEAR_IFVG" and self.pending_side == 1:
                sl = self.retracement_extreme if self.retracement_extreme else max(c["high"] for c in self.candles_1m[-10:])
                trigger_type = "1m IFVG"

        if sl is None:
            # In RETRACED state but neither 1m BOS nor IFVG triggered.
            # Log why so post-session forensics can answer "why didn't we fire on this bar?"
            side_lbl = "LONG" if self.pending_side == 0 else "SHORT"
            if self.pending_side == 0:
                log.info(
                    f"   ⏸  1m{self._bar_ct(ltf_candle)} {side_lbl} no-trigger | "
                    f"close={entry:.2f} prev_high={ltf_prev['high']:.2f} "
                    f"(needs close > prev_high)"
                )
            else:
                log.info(
                    f"   ⏸  1m{self._bar_ct(ltf_candle)} {side_lbl} no-trigger | "
                    f"close={entry:.2f} prev_low={ltf_prev['low']:.2f} "
                    f"(needs close < prev_low)"
                )
            return

        # Sanity check: is the entry price still within the BOS leg?
        # If we're entering long but price is below the BOS leg low,
        # or entering short but price is above the BOS leg high,
        # the retracement blew through the entire leg — don't trade.
        if self.bos_leg:
            if self.pending_side == 0 and entry < self.bos_leg["low"]:
                log.info(f"⛔ Skip — entry {entry:.2f} below BOS leg low {self.bos_leg['low']:.2f}")
                return
            elif self.pending_side == 1 and entry > self.bos_leg["high"]:
                log.info(f"⛔ Skip — entry {entry:.2f} above BOS leg high {self.bos_leg['high']:.2f}")
                return

        risk_pts = abs(entry - sl)
        if risk_pts > self.max_stop_pts or risk_pts < 0.5:
            log.info(
                f"⛔ Skip — risk_pts {risk_pts:.2f} outside bounds "
                f"[0.5, {self.max_stop_pts}] (entry={entry:.2f} sl={sl:.2f})"
            )
            return

        # Validate SL is on the correct side of entry
        if self.pending_side == 0 and sl >= entry:
            log.info(f"⛔ Skip — long SL {sl:.2f} is above entry {entry:.2f}")
            return
        elif self.pending_side == 1 and sl <= entry:
            log.info(f"⛔ Skip — short SL {sl:.2f} is below entry {entry:.2f}")
            return

        tp = self._find_tp_target(entry, sl, risk_pts)
        qty = self.calculate_qty(entry, sl)
        if qty == 0:
            log.info(f"⛔ Skip — calculate_qty returned 0 (entry={entry:.2f} sl={sl:.2f})")
            return

        side_str = "BUY" if self.pending_side == 0 else "SELL"
        self.trade_log.log("ENTRY", {
            "message": f"{self.symbol} {side_str} {qty} @ {entry:.2f} ({trigger_type})",
            "side": side_str, "trigger": trigger_type,
            "entry": entry, "stop": sl, "target": tp,
            "qty": qty, "risk_usd": round(risk_pts * qty * self.point_value, 2),
        })

        self.active_position = {
            "side": self.pending_side, "entry": entry,
            "sl": sl, "tp": tp, "qty": qty,
        }
        # In live mode, run.py increments daily_trade_count after confirmed fill.
        # In backtest mode, increment here since fills are instant.
        if not self.live_mode:
            self.daily_trade_count += 1
            self.last_trade_date = c_time.date()

        if self.order_callback:
            self.order_callback(self.pending_side, qty, entry, sl, tp)
        else:
            log.warning("⚠️ No order_callback — logged but NOT sent")

        self.reset_state("Trade executed")
