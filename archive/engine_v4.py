"""
engine_v4.py — Option 2: Pure 1m strategy.

Everything on 1m bars:
  - 5m bars are stored but never used for state machine logic
  - 1m fractal swings (3-bar) replace 5m swings
  - 1m FVGs replace 5m FVGs (with min_gap reduced to 0.5pt for 1m granularity)
  - Sweep detection on 1m bars touching 5m-derived sweep_levels
    (sweep_levels still come from the historical 5m scan — 1m fractals would
    produce thousands of low-quality levels)
  - BOS on 1m close past 1m swing
  - Retracement on 1m bar into 1m FVG or EQ
  - 1m BOS trigger (unchanged)

Why test this:
  V2 / V3 showed the strategy collapses to PF ~1.0 in live-delivery because
  the 5m bar's "future" data isn't available. Maybe the multi-timeframe
  structure was never the right abstraction — maybe the sweep → BOS →
  retracement pattern needs to be detected on the same timeframe it
  executes on (1m).

  Honest expectation: 1m structure is noisier. More setups detected, more
  false positives. WR likely lower than V1 live-delivery. But the trades it
  catches might be timed better.
"""

import logging
from engine import ICTEngine

log = logging.getLogger("ICTEngine")


class ICTEngineV4(ICTEngine):
    """Pure 1m strategy — see module docstring."""

    # ------------------------------------------------------------------
    # 5m PROCESSOR — V4: pure storage, no logic
    # ------------------------------------------------------------------
    def process_5m_candle(self, new_candle):
        """V4 doesn't use 5m bars for anything except holding them around."""
        self.candles_5m.append(new_candle)
        if len(self.candles_5m) > 200:
            self.candles_5m = self.candles_5m[-200:]

    # ------------------------------------------------------------------
    # SWING DETECTION — overridden to use 1m bars
    # ------------------------------------------------------------------
    def _update_swings(self):
        """V4: detect 1m fractals (3-bar confirmation)."""
        if len(self.candles_1m) < 3:
            return
        i = len(self.candles_1m) - 2  # bar between prev and current
        bar = self.candles_1m[i]
        prev_bar = self.candles_1m[i - 1]
        next_bar = self.candles_1m[i + 1]

        if bar["high"] > prev_bar["high"] and bar["high"] > next_bar["high"]:
            self.swing_highs.append({
                "price": bar["high"], "index": i, "timestamp": bar["timestamp"],
            })
        if bar["low"] < prev_bar["low"] and bar["low"] < next_bar["low"]:
            self.swing_lows.append({
                "price": bar["low"], "index": i, "timestamp": bar["timestamp"],
            })

    # ------------------------------------------------------------------
    # FVG DETECTION — overridden to use 1m bars
    # ------------------------------------------------------------------
    def _find_bos_leg_fvgs(self):
        """
        Scan recent 1m candles for FVGs within the BOS leg.
        Min gap reduced to 0.5pt (vs 1.0pt for 5m) since 1m gaps are smaller.
        Lookback extended to 60 bars (~1 hour) since 1m is denser.
        """
        self.bos_leg_fvgs = []
        if not self.bos_leg or len(self.candles_1m) < 3:
            return

        leg_high = self.bos_leg["high"]
        leg_low = self.bos_leg["low"]
        min_gap = 0.5

        for i in range(max(2, len(self.candles_1m) - 60), len(self.candles_1m)):
            if i < 2:
                continue
            c1 = self.candles_1m[i - 2]
            c3 = self.candles_1m[i]

            # Bullish FVG: candle3.low > candle1.high (gap up)
            if c3["low"] > c1["high"]:
                fvg_top = c3["low"]
                fvg_bottom = c1["high"]
                if (fvg_top - fvg_bottom) >= min_gap and fvg_bottom < leg_high and fvg_top > leg_low:
                    self.bos_leg_fvgs.append({
                        "top": fvg_top, "bottom": fvg_bottom, "type": "BULL",
                    })

            # Bearish FVG: candle3.high < candle1.low (gap down)
            if c3["high"] < c1["low"]:
                fvg_top = c1["low"]
                fvg_bottom = c3["high"]
                if (fvg_top - fvg_bottom) >= min_gap and fvg_bottom < leg_high and fvg_top > leg_low:
                    self.bos_leg_fvgs.append({
                        "top": fvg_top, "bottom": fvg_bottom, "type": "BEAR",
                    })

        if self.bos_leg_fvgs:
            for fvg in self.bos_leg_fvgs:
                log.info(f"   📦 1m {fvg['type']} FVG: {fvg['bottom']:.2f}-{fvg['top']:.2f}")

    # ------------------------------------------------------------------
    # 1m PROCESSOR — V4: full state machine on 1m
    # ------------------------------------------------------------------
    def process_1m_candle(self, ltf_candle):
        """Full sweep → BOS → retracement → 1m BOS trigger on 1m bars."""
        self.candles_1m.append(ltf_candle)
        if len(self.candles_1m) > 500:
            self.candles_1m = self.candles_1m[-500:]

        self._update_f4_morning_range(ltf_candle)

        if len(self.candles_1m) < 3:
            return

        c_time = self.parse_candle_time(ltf_candle["timestamp"])
        self._bos_just_confirmed = False

        # Update 1m fractal swings as bars complete
        self._update_swings()

        # Daily limit
        already_traded = (
            self.last_trade_date == c_time.date()
            and self.daily_trade_count >= self.max_daily_trades
        )
        if already_traded:
            return

        if self.active_position:
            return

        in_session = (
            (c_time.hour == 8 and c_time.minute >= 25)
            or (c_time.hour == 9 and c_time.minute <= self.entry_cutoff_minute)
        )
        if not in_session:
            if self.strategy_stage != "WAITING":
                self.reset_state("Outside session")
            return

        # Sweep window: 8:25 CT through 9:04 CT
        in_sweep_window = (
            (c_time.hour == 8 and c_time.minute >= 25)
            or (c_time.hour == 9 and c_time.minute <= 4)
        )

        # =============================================================
        # STEP 1: SWEEP DETECTION (1m bar vs 5m-derived sweep level)
        # =============================================================
        if self.strategy_stage in ("WAITING", "SWEPT") and in_sweep_window:
            for lvl in self.sweep_levels[:]:
                swept = False
                stype = None
                if lvl["side"] == "HIGH" and ltf_candle["high"] > lvl["price"]:
                    swept, stype = True, "BUY_SIDE"
                elif lvl["side"] == "LOW" and ltf_candle["low"] < lvl["price"]:
                    swept, stype = True, "SELL_SIDE"
                if swept:
                    log.info(
                        f"🌊 Sweep {'HIGH' if stype == 'BUY_SIDE' else 'LOW'}: "
                        f"{lvl['price']:.2f} (w={lvl['weight']})"
                    )
                    self.pending_sweeps.append({
                        "level": lvl["price"], "type": stype, "weight": lvl["weight"],
                    })
                    self.sweep_levels.remove(lvl)

            if self.pending_sweeps and self.strategy_stage == "WAITING":
                self.strategy_stage = "SWEPT"
                self.stage_entry_time = ltf_candle["timestamp"]
                self.sweep_candle_extreme = {
                    "high": ltf_candle["high"], "low": ltf_candle["low"],
                }
                n_buy = sum(1 for s in self.pending_sweeps if s["type"] == "BUY_SIDE")
                n_sell = sum(1 for s in self.pending_sweeps if s["type"] == "SELL_SIDE")
                self._log_stage(
                    "WAITING", "SWEPT",
                    f"@1m{self._bar_ct(ltf_candle)} buy={n_buy} sell={n_sell}",
                )
            elif self.strategy_stage == "SWEPT" and self.sweep_candle_extreme:
                self.sweep_candle_extreme["high"] = max(
                    self.sweep_candle_extreme["high"], ltf_candle["high"]
                )
                self.sweep_candle_extreme["low"] = min(
                    self.sweep_candle_extreme["low"], ltf_candle["low"]
                )

        # =============================================================
        # STEP 2: BOS DETECTION (1m close past 1m swing)
        # =============================================================
        if self.strategy_stage == "SWEPT" and self.stage_entry_time:
            post_highs = [
                sh for sh in self.swing_highs
                if sh["timestamp"] >= self.stage_entry_time
            ]
            post_lows = [
                sl for sl in self.swing_lows
                if sl["timestamp"] >= self.stage_entry_time
            ]

            buy_side = [s for s in self.pending_sweeps if s["type"] == "BUY_SIDE"]
            sell_side = [s for s in self.pending_sweeps if s["type"] == "SELL_SIDE"]

            confirmed = False

            since_sweep_lows = [
                c["low"] for c in self.candles_1m
                if c["timestamp"] >= self.stage_entry_time
            ]
            since_sweep_highs = [
                c["high"] for c in self.candles_1m
                if c["timestamp"] >= self.stage_entry_time
            ]

            # BEARISH BOS: 1m close < latest post-sweep 1m swing low
            if post_lows and buy_side:
                latest_sl = post_lows[-1]
                if ltf_candle["close"] < latest_sl["price"]:
                    best = max(buy_side, key=lambda s: s.get("weight", 1))
                    leg_high = (
                        max(sh["price"] for sh in post_highs)
                        if post_highs
                        else (max(since_sweep_highs) if since_sweep_highs else ltf_candle["high"])
                    )
                    leg_low = (
                        min(since_sweep_lows) if since_sweep_lows else ltf_candle["low"]
                    )
                    self.active_sweep = best
                    self.pending_side = 1
                    self.bos_leg = {"high": leg_high, "low": leg_low}
                    self._find_bos_leg_fvgs()
                    self.pending_sweeps = []
                    self.strategy_stage = "CONFIRMED_SHIFT"
                    self._bos_just_confirmed = True
                    fvg_str = (
                        f" | {len(self.bos_leg_fvgs)} FVGs in leg"
                        if self.bos_leg_fvgs else ""
                    )
                    leg_size = leg_high - leg_low
                    log.info(
                        f"📉 BEARISH BOS — close < 1m swing low {latest_sl['price']:.2f} | "
                        f"Leg: {leg_high:.2f}→{leg_low:.2f} ({leg_size:.2f}pt){fvg_str}"
                    )
                    self._log_stage(
                        "SWEPT", "CONFIRMED_SHIFT",
                        f"@1m{self._bar_ct(ltf_candle)} BEARISH "
                        f"leg={leg_high:.2f}→{leg_low:.2f}",
                    )
                    confirmed = True

            # BULLISH BOS: 1m close > latest post-sweep 1m swing high
            if not confirmed and post_highs and sell_side:
                latest_sh = post_highs[-1]
                if ltf_candle["close"] > latest_sh["price"]:
                    best = max(sell_side, key=lambda s: s.get("weight", 1))
                    leg_low = (
                        min(sl["price"] for sl in post_lows)
                        if post_lows
                        else (min(since_sweep_lows) if since_sweep_lows else ltf_candle["low"])
                    )
                    leg_high = (
                        max(since_sweep_highs) if since_sweep_highs else ltf_candle["high"]
                    )
                    self.active_sweep = best
                    self.pending_side = 0
                    self.bos_leg = {"high": leg_high, "low": leg_low}
                    self._find_bos_leg_fvgs()
                    self.pending_sweeps = []
                    self.strategy_stage = "CONFIRMED_SHIFT"
                    self._bos_just_confirmed = True
                    fvg_str = (
                        f" | {len(self.bos_leg_fvgs)} FVGs in leg"
                        if self.bos_leg_fvgs else ""
                    )
                    leg_size = leg_high - leg_low
                    log.info(
                        f"📈 BULLISH BOS — close > 1m swing high {latest_sh['price']:.2f} | "
                        f"Leg: {leg_low:.2f}→{leg_high:.2f} ({leg_size:.2f}pt){fvg_str}"
                    )
                    self._log_stage(
                        "SWEPT", "CONFIRMED_SHIFT",
                        f"@1m{self._bar_ct(ltf_candle)} BULLISH "
                        f"leg={leg_low:.2f}→{leg_high:.2f}",
                    )
                    confirmed = True

        # =============================================================
        # STEP 3: RETRACEMENT into EQ or 1m FVG
        # =============================================================
        if self.strategy_stage == "CONFIRMED_SHIFT" and self.bos_leg:
            eq = (self.bos_leg["high"] + self.bos_leg["low"]) / 2
            hit_internal = False
            hit_reason = ""

            if self.pending_side == 0:  # Long
                if not self._bos_just_confirmed and ltf_candle["low"] <= eq:
                    hit_internal = True
                    hit_reason = f"EQ({eq:.2f})"
                else:
                    for fvg in self.bos_leg_fvgs:
                        if ltf_candle["low"] <= fvg["top"]:
                            hit_internal = True
                            hit_reason = f"FVG({fvg['bottom']:.2f}-{fvg['top']:.2f})"
                            break
                if hit_internal:
                    self.retracement_extreme = ltf_candle["low"]

            elif self.pending_side == 1:  # Short
                if not self._bos_just_confirmed and ltf_candle["high"] >= eq:
                    hit_internal = True
                    hit_reason = f"EQ({eq:.2f})"
                else:
                    for fvg in self.bos_leg_fvgs:
                        if ltf_candle["high"] >= fvg["bottom"]:
                            hit_internal = True
                            hit_reason = f"FVG({fvg['bottom']:.2f}-{fvg['top']:.2f})"
                            break
                if hit_internal:
                    self.retracement_extreme = ltf_candle["high"]

            if hit_internal:
                log.info(f"🎯 Retracement into {hit_reason} → 1m trigger")
                self._log_stage(
                    "CONFIRMED_SHIFT", "RETRACED",
                    f"@1m{self._bar_ct(ltf_candle)} {hit_reason} "
                    f"ext={self.retracement_extreme:.2f}",
                )
                self.strategy_stage = "RETRACED"

        # Deepen retracement_extreme while in RETRACED
        if self.strategy_stage == "RETRACED" and self.retracement_extreme is not None:
            if self.pending_side == 0:
                self.retracement_extreme = min(
                    self.retracement_extreme, ltf_candle["low"]
                )
            else:
                self.retracement_extreme = max(
                    self.retracement_extreme, ltf_candle["high"]
                )

        # =============================================================
        # STEP 4: INVALIDATION (1m close past BOS leg)
        # =============================================================
        if self.strategy_stage in ("CONFIRMED_SHIFT", "RETRACED") and self.bos_leg:
            invalid = False
            if self.pending_side == 0 and ltf_candle["close"] < self.bos_leg["low"]:
                invalid = True
            elif self.pending_side == 1 and ltf_candle["close"] > self.bos_leg["high"]:
                invalid = True
            if invalid:
                log.info("🚫 Invalidated — retracement pushed past 100% of BOS leg")
                self.reset_state("Invalidated — past BOS leg")
                return

        # =============================================================
        # STEP 5: 1m ENTRY TRIGGER (only if RETRACED)
        # =============================================================
        if self.strategy_stage != "RETRACED":
            return

        ltf_prev = self.candles_1m[-2]

        if c_time.hour == 9 and c_time.minute >= self.entry_cutoff_minute:
            self.reset_state(f"9:{self.entry_cutoff_minute:02d} deadline")
            return
        if c_time.hour >= 10:
            self.reset_state("Past 10:00")
            return

        if not self._f4_check(c_time):
            return

        entry = ltf_candle["close"]
        sl = None
        trigger_type = ""

        if self.pending_side == 0 and entry > ltf_prev["high"]:
            sl = (
                self.retracement_extreme
                if self.retracement_extreme
                else min(c["low"] for c in self.candles_1m[-10:])
            )
            trigger_type = "1m BOS"
            log.info(
                f"   1m BOS LONG: close={entry:.2f} > prev_high={ltf_prev['high']:.2f} | "
                f"SL={sl:.2f}"
            )
        elif self.pending_side == 1 and entry < ltf_prev["low"]:
            sl = (
                self.retracement_extreme
                if self.retracement_extreme
                else max(c["high"] for c in self.candles_1m[-10:])
            )
            trigger_type = "1m BOS"
            log.info(
                f"   1m BOS SHORT: close={entry:.2f} < prev_low={ltf_prev['low']:.2f} | "
                f"SL={sl:.2f}"
            )

        if sl is None and len(self.candles_1m) >= 3:
            ifvg = self._detect_1m_ifvg()
            if ifvg == "BULL_IFVG" and self.pending_side == 0:
                sl = (
                    self.retracement_extreme
                    if self.retracement_extreme
                    else min(c["low"] for c in self.candles_1m[-10:])
                )
                trigger_type = "1m IFVG"
            elif ifvg == "BEAR_IFVG" and self.pending_side == 1:
                sl = (
                    self.retracement_extreme
                    if self.retracement_extreme
                    else max(c["high"] for c in self.candles_1m[-10:])
                )
                trigger_type = "1m IFVG"

        if sl is None:
            return

        if self.bos_leg:
            if self.pending_side == 0 and entry < self.bos_leg["low"]:
                return
            elif self.pending_side == 1 and entry > self.bos_leg["high"]:
                return

        risk_pts = abs(entry - sl)
        if risk_pts > self.max_stop_pts or risk_pts < 0.5:
            return

        if self.pending_side == 0 and sl >= entry:
            return
        elif self.pending_side == 1 and sl <= entry:
            return

        tp = self._find_tp_target(entry, sl, risk_pts)
        qty = self.calculate_qty(entry, sl)
        if qty == 0:
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
        if not self.live_mode:
            self.daily_trade_count += 1
            self.last_trade_date = c_time.date()

        if self.order_callback:
            self.order_callback(self.pending_side, qty, entry, sl, tp)

        self.reset_state("Trade executed (V4)")
