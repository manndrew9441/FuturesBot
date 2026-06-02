"""
engine_v3.py — Option 3: Confirmed 5m setup + immediate entry on retracement.

Same state machine as engine.py (V1):
  - 5m bars drive sweep / BOS / retracement / invalidation transitions
  - 5m structure for swing detection, FVG detection, etc.

What changes:
  - The 1m BOS / IFVG entry trigger is REMOVED.
  - Instead, V3 fires entry on the FIRST 1m bar processed while state is RETRACED.
  - Entry = that 1m bar's close
  - SL = retracement_extreme (same as V1)
  - Size = floor(max_risk_usd / (risk_pts × point_value)) — naturally scales down
    when entry is far from SL

Why this might work:
  V1 canonical's edge came from look-ahead — by the time the 5m bar advanced
  state at its open ts, the bot knew the 5m bar's CLOSE direction (which is
  itself a form of momentum confirmation). The 1m BOS trigger on the next 1m
  bar was somewhat redundant.

  V3 makes the look-ahead explicit: use the completed 5m bar to confirm the
  setup direction, then enter immediately on the next available 1m close.
  No second-tier filter. Size adapts to the actual risk_pts so wider entry-to-SL
  distances just mean smaller positions, not over-risk.

Difference from V1 live-delivery:
  V1 live-delivery only fires when 1m BOS condition is independently met
  AFTER state reaches RETRACED. If price is choppy or moving away from
  retracement_extreme, V1 keeps waiting until 1m close crosses prev_low/high
  in the right direction — by which point risk_pts is often > max_stop_pts.

  V3 fires immediately when state reaches RETRACED, capturing the trade
  before risk_pts blows out. Trade-off: no momentum confirmation from the
  1m bar — V3 takes the trade even if the 1m bar's direction is contrary.
"""

import logging
from engine import ICTEngine

log = logging.getLogger("ICTEngine")


class ICTEngineV3(ICTEngine):
    """Confirmed 5m + market entry on retracement. See module docstring."""

    def process_1m_candle(self, ltf_candle):
        """
        Identical setup pipeline to V1's process_5m_candle (which already ran
        and advanced state). On 1m bars, just fire entry if state is RETRACED.
        No 1m BOS confirmation required.
        """
        self.candles_1m.append(ltf_candle)
        if len(self.candles_1m) > 500:
            self.candles_1m = self.candles_1m[-500:]

        # F4 morning range — same as V1, runs on every 1m bar
        self._update_f4_morning_range(ltf_candle)

        if len(self.candles_1m) < 2 or self.strategy_stage != "RETRACED":
            return
        if self.active_position:
            return

        c_time = self.parse_candle_time(ltf_candle["timestamp"])

        # Session cutoffs (same as V1)
        if c_time.hour == 9 and c_time.minute >= self.entry_cutoff_minute:
            self.reset_state(f"9:{self.entry_cutoff_minute:02d} deadline")
            return
        if c_time.hour >= 10:
            self.reset_state("Past 10:00")
            return

        # F4 day-quality filter (same as V1)
        if not self._f4_check(c_time):
            return

        # V3 entry: fire on this 1m bar's close, SL from retracement_extreme.
        # No 1m BOS / IFVG check — the 5m setup IS the confirmation.
        entry = ltf_candle["close"]
        sl = self.retracement_extreme
        if sl is None:
            log.warning("⚠️ V3: RETRACED state with no retracement_extreme set — skipping")
            return

        trigger_type = "V3-market"

        # Sanity: entry still within BOS leg (retracement hasn't blown through)
        if self.bos_leg:
            if self.pending_side == 0 and entry < self.bos_leg["low"]:
                log.info(
                    f"⛔ V3 Skip — entry {entry:.2f} below BOS leg low "
                    f"{self.bos_leg['low']:.2f}"
                )
                return
            elif self.pending_side == 1 and entry > self.bos_leg["high"]:
                log.info(
                    f"⛔ V3 Skip — entry {entry:.2f} above BOS leg high "
                    f"{self.bos_leg['high']:.2f}"
                )
                return

        risk_pts = abs(entry - sl)
        if risk_pts > self.max_stop_pts or risk_pts < 0.5:
            log.info(
                f"⛔ V3 Skip — risk_pts {risk_pts:.2f} outside bounds "
                f"[0.5, {self.max_stop_pts}] (entry={entry:.2f} sl={sl:.2f})"
            )
            return

        # SL must be on the correct side of entry
        if self.pending_side == 0 and sl >= entry:
            log.info(f"⛔ V3 Skip — long SL {sl:.2f} ≥ entry {entry:.2f}")
            return
        elif self.pending_side == 1 and sl <= entry:
            log.info(f"⛔ V3 Skip — short SL {sl:.2f} ≤ entry {entry:.2f}")
            return

        tp = self._find_tp_target(entry, sl, risk_pts)
        qty = self.calculate_qty(entry, sl)
        if qty == 0:
            log.info(
                f"⛔ V3 Skip — calculate_qty=0 (entry={entry:.2f} sl={sl:.2f})"
            )
            return

        side_str = "BUY" if self.pending_side == 0 else "SELL"
        log.info(
            f"   V3 ENTRY {side_str}: close={entry:.2f} SL={sl:.2f} "
            f"risk_pts={risk_pts:.2f} qty={qty}"
        )
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

        self.reset_state("Trade executed (V3)")
