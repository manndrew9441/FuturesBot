"""
engine_v2.py — Option 1: 1m confirmation of 5m setups.

Same sweep → BOS → retracement thesis as engine.py, but state transitions
happen on 1m bars instead of 5m bars. This eliminates the look-ahead bias
that gave v1 its backtest edge but couldn't be reproduced live.

Differences from engine.py (everything else is inherited unchanged):

  process_5m_candle (v2):
    Only maintains candles_5m and runs _update_swings to build the 5m
    fractal swing structure (swing_highs / swing_lows). Does NOT advance
    the state machine.

  process_1m_candle (v2):
    Drives the entire state machine.
      - Sweep detection on 1m bar's high/low touching a 5m sweep_level
      - BOS confirmation on 1m close past a post-sweep 5m swing
      - Retracement on 1m bar's high/low touching an FVG (5m-derived) or EQ
      - Invalidation on 1m close past the BOS leg
      - 1m BOS / IFVG trigger (unchanged)

What stays multi-timeframe:
  - sweep_levels: still from the historical 5m scan
  - swing_highs / swing_lows: still 5m fractals (built from completed 5m
    bars, which is fine because each 5m bar's data is finalized by the
    time _update_swings sees it)
  - bos_leg_fvgs: still detected from completed 5m candles

What becomes 1m:
  - When each state transition happens (real-time, not delayed by 5m close)
  - The leg low/high used for the BOS leg is the actual deepest 1m extreme
    since the sweep — not just the 5m BOS bar's extreme

For 5/22 09:00, this fires entry at the 09:00 1m close (~7499.50) with
SL anchored at the 09:00 1m bar's low (7497.75) — risk_pts ~2, well
within max_stop_pts. Same direction as the canonical backtest's "look-
ahead" entry, but reachable in real-time.
"""

import logging
from engine import ICTEngine

log = logging.getLogger("ICTEngine")


class ICTEngineV2(ICTEngine):
    """1m-confirmation engine. See module docstring."""

    # ------------------------------------------------------------------
    # 5m PROCESSOR — V2: structure-only, no state transitions
    # ------------------------------------------------------------------
    def process_5m_candle(self, new_candle):
        """Maintain candles_5m and 5m swing structure. No state machine."""
        self.candles_5m.append(new_candle)
        if len(self.candles_5m) > 200:
            self.candles_5m = self.candles_5m[-200:]
        if len(self.candles_5m) < 3:
            return
        # Only update swings — the rest of the state machine lives in
        # process_1m_candle now.
        self._update_swings()

    # ------------------------------------------------------------------
    # 1m PROCESSOR — V2: drives the full state machine
    # ------------------------------------------------------------------
    def process_1m_candle(self, ltf_candle):
        """
        Full state machine on 1m bars:
          WAITING → SWEPT → CONFIRMED_SHIFT → RETRACED → (entry)
        Uses 5m structure (sweep_levels, post-sweep swings, FVGs) but
        evaluates every transition on the 1m bar's OHLC in real time.
        """
        self.candles_1m.append(ltf_candle)
        if len(self.candles_1m) > 500:
            self.candles_1m = self.candles_1m[-500:]

        # F4 morning range runs on every 1m bar regardless of stage.
        self._update_f4_morning_range(ltf_candle)

        if len(self.candles_1m) < 2:
            return

        c_time = self.parse_candle_time(ltf_candle["timestamp"])
        # Reset the BOS-just-confirmed gate each bar (prevents EQ check
        # on the same bar that confirmed BOS — same semantics as v1).
        self._bos_just_confirmed = False

        # Daily limit
        already_traded = (
            self.last_trade_date == c_time.date()
            and self.daily_trade_count >= self.max_daily_trades
        )
        if already_traded:
            return

        if self.active_position:
            return

        # Session window (1m bars allowed 8:25 CT through entry_cutoff_minute past 9:00)
        in_session = (
            (c_time.hour == 8 and c_time.minute >= 25)
            or (c_time.hour == 9 and c_time.minute <= self.entry_cutoff_minute)
        )
        if not in_session:
            if self.strategy_stage != "WAITING":
                self.reset_state("Outside session")
            return

        # Sweep window: 8:25 CT through 9:04 CT (mirrors v1's "5m bar at 9:00"
        # which covered 9:00–9:04).
        in_sweep_window = (
            (c_time.hour == 8 and c_time.minute >= 25)
            or (c_time.hour == 9 and c_time.minute <= 4)
        )

        # =============================================================
        # STEP 1: SWEEP DETECTION (on 1m bar)
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
                    "high": ltf_candle["high"],
                    "low": ltf_candle["low"],
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
        # STEP 2: BOS DETECTION (on 1m close past 5m post-sweep swing)
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

            # 1m closes since the sweep — used to compute actual leg extremes
            since_sweep_lows = [
                c["low"] for c in self.candles_1m
                if c["timestamp"] >= self.stage_entry_time
            ]
            since_sweep_highs = [
                c["high"] for c in self.candles_1m
                if c["timestamp"] >= self.stage_entry_time
            ]

            # BEARISH BOS: 1m close < latest post-sweep 5m swing low
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
                        f"📉 BEARISH BOS — close < swing low {latest_sl['price']:.2f} | "
                        f"Leg: {leg_high:.2f}→{leg_low:.2f} ({leg_size:.2f}pt){fvg_str}"
                    )
                    self._log_stage(
                        "SWEPT", "CONFIRMED_SHIFT",
                        f"@1m{self._bar_ct(ltf_candle)} BEARISH "
                        f"leg={leg_high:.2f}→{leg_low:.2f}",
                    )
                    confirmed = True

            # BULLISH BOS: 1m close > latest post-sweep 5m swing high
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
                        f"📈 BULLISH BOS — close > swing high {latest_sh['price']:.2f} | "
                        f"Leg: {leg_low:.2f}→{leg_high:.2f} ({leg_size:.2f}pt){fvg_str}"
                    )
                    self._log_stage(
                        "SWEPT", "CONFIRMED_SHIFT",
                        f"@1m{self._bar_ct(ltf_candle)} BULLISH "
                        f"leg={leg_low:.2f}→{leg_high:.2f}",
                    )
                    confirmed = True

        # =============================================================
        # STEP 3: RETRACEMENT into EQ or FVG (on 1m bar)
        # =============================================================
        if self.strategy_stage == "CONFIRMED_SHIFT" and self.bos_leg:
            eq = (self.bos_leg["high"] + self.bos_leg["low"]) / 2
            hit_internal = False
            hit_reason = ""

            if self.pending_side == 0:  # Long — wait for price to drop
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

            elif self.pending_side == 1:  # Short — wait for price to rise
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
        # STEP 5: 1m ENTRY TRIGGER (only when RETRACED)
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

        # 1m BOS trigger — same as v1
        if self.pending_side == 0 and entry > ltf_prev["high"]:
            sl = (
                self.retracement_extreme
                if self.retracement_extreme
                else min(c["low"] for c in self.candles_1m[-10:])
            )
            trigger_type = "1m BOS"
            log.info(
                f"   1m BOS LONG: close={entry:.2f} > prev_high={ltf_prev['high']:.2f} | "
                f"prev_OHLC=({ltf_prev['open']:.2f},{ltf_prev['high']:.2f},"
                f"{ltf_prev['low']:.2f},{ltf_prev['close']:.2f}) | SL={sl:.2f}"
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
                f"prev_OHLC=({ltf_prev['open']:.2f},{ltf_prev['high']:.2f},"
                f"{ltf_prev['low']:.2f},{ltf_prev['close']:.2f}) | SL={sl:.2f}"
            )

        # 1m IFVG fallback
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
            side_lbl = "LONG" if self.pending_side == 0 else "SHORT"
            if self.pending_side == 0:
                log.info(
                    f"   ⏸  1m{self._bar_ct(ltf_candle)} {side_lbl} no-trigger | "
                    f"close={entry:.2f} prev_high={ltf_prev['high']:.2f}"
                )
            else:
                log.info(
                    f"   ⏸  1m{self._bar_ct(ltf_candle)} {side_lbl} no-trigger | "
                    f"close={entry:.2f} prev_low={ltf_prev['low']:.2f}"
                )
            return

        # Sanity: entry still within BOS leg
        if self.bos_leg:
            if self.pending_side == 0 and entry < self.bos_leg["low"]:
                log.info(
                    f"⛔ Skip — entry {entry:.2f} below BOS leg low "
                    f"{self.bos_leg['low']:.2f}"
                )
                return
            elif self.pending_side == 1 and entry > self.bos_leg["high"]:
                log.info(
                    f"⛔ Skip — entry {entry:.2f} above BOS leg high "
                    f"{self.bos_leg['high']:.2f}"
                )
                return

        risk_pts = abs(entry - sl)
        if risk_pts > self.max_stop_pts or risk_pts < 0.5:
            log.info(
                f"⛔ Skip — risk_pts {risk_pts:.2f} outside bounds "
                f"[0.5, {self.max_stop_pts}] (entry={entry:.2f} sl={sl:.2f})"
            )
            return

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
        if not self.live_mode:
            self.daily_trade_count += 1
            self.last_trade_date = c_time.date()

        if self.order_callback:
            self.order_callback(self.pending_side, qty, entry, sl, tp)
        else:
            log.warning("⚠️ No order_callback — logged but NOT sent")

        self.reset_state("Trade executed")
