"""
journal.py — Lightweight trade journal for the ICT bot.

Appends one row per completed trade to a CSV file.
Import in run.py and call journal.log_trade() after each fill.

Usage:
  from journal import TradeJournal
  journal = TradeJournal()  # Creates trades.csv if it doesn't exist
  journal.log_trade(side="BUY", qty=50, entry=6745.25, exit=6730.75,
                    sl=6752.25, tp=6730.75, reason="TP", pnl_usd=1522.50,
                    risk_usd=750, instrument="MES", stage_flow="SWEPT→CONFIRMED→RETRACED→ENTRY")

Review:
  python journal.py                  # Print summary stats
  python journal.py --weekly         # Weekly breakdown
  python journal.py --health         # Edge health check
"""

import csv
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict

DEFAULT_FILE = "trades.csv"

FIELDS = [
    "date",           # Trade date (YYYY-MM-DD)
    "time",           # Entry time (HH:MM CT)
    "account",        # COMBINE, XFA, LIVE
    "side",           # BUY or SELL
    "instrument",     # MES, ES, etc.
    "qty",            # Contracts
    "entry",          # Entry price
    "exit",           # Exit price
    "sl",             # Stop loss price
    "tp",             # Take profit price
    "reason",         # TP, SL, EOD, or MANUAL
    "pnl_pts",        # P&L in points
    "pnl_gross",      # P&L before fees
    "fees",           # Total round-turn fees
    "pnl_net",        # P&L after fees
    "risk_usd",       # Risk per trade
    "r_multiple",     # Net P&L as multiple of risk
    "hold_minutes",   # Duration in minutes
    "sweep_weight",   # Weight of the sweep that started the narrative
    "fvg_count",      # Number of FVGs in the BOS leg
    "stage_flow",     # Engine stage progression
    "notes",          # Manual notes (optional)
]

# Fee schedule per account type (round-turn per contract)
ACCOUNT_FEES = {
    "COMBINE":  {"MES": 0.74, "ES": 2.80, "NQ": 2.80, "MNQ": 0.74},
    "XFA":      {"MES": 0.74, "ES": 2.80, "NQ": 2.80, "MNQ": 0.74},
    "LIVE":     {"MES": 0.00, "ES": 0.00},  # Update when broker is chosen
}


class TradeJournal:
    def __init__(self, filepath=DEFAULT_FILE):
        self.filepath = filepath
        self._ensure_file()

    def _ensure_file(self):
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                writer.writeheader()

    def log_trade(self, **kwargs):
        """Append a trade row. Missing fields default to empty."""
        row = {field: kwargs.get(field, "") for field in FIELDS}

        # Backwards compatibility: accept pnl_usd as pnl_gross
        if "pnl_usd" in kwargs and not row["pnl_gross"]:
            row["pnl_gross"] = kwargs["pnl_usd"]

        # Auto-calculate fees based on account type and instrument
        if row["qty"] and row.get("account") and row.get("instrument") and not row["fees"]:
            try:
                acct = str(row["account"]).upper()
                inst = str(row["instrument"]).upper()
                fee_per_contract = ACCOUNT_FEES.get(acct, {}).get(inst, 0)
                row["fees"] = round(fee_per_contract * float(row["qty"]), 2)
            except (ValueError, TypeError):
                pass

        # Auto-calculate net P&L
        if row["pnl_gross"] and not row["pnl_net"]:
            try:
                fees = float(row["fees"]) if row["fees"] else 0
                row["pnl_net"] = round(float(row["pnl_gross"]) - fees, 2)
            except (ValueError, TypeError):
                row["pnl_net"] = row["pnl_gross"]

        # Auto-fill R-multiple from net P&L
        if row["pnl_net"] and row["risk_usd"]:
            try:
                row["r_multiple"] = round(float(row["pnl_net"]) / float(row["risk_usd"]), 2)
            except (ValueError, ZeroDivisionError):
                pass

        if row["entry"] and row["exit"]:
            try:
                row["pnl_pts"] = round(float(row["exit"]) - float(row["entry"]), 2)
                if row["side"] == "SELL":
                    row["pnl_pts"] = round(float(row["entry"]) - float(row["exit"]), 2)
            except ValueError:
                pass

        if not row["date"]:
            row["date"] = datetime.now().strftime("%Y-%m-%d")
        if not row["time"]:
            row["time"] = datetime.now().strftime("%H:%M")

        with open(self.filepath, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writerow(row)

    def read_trades(self):
        """Read all trades from the CSV."""
        trades = []
        if not os.path.exists(self.filepath):
            return trades
        with open(self.filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for key in ["qty", "entry", "exit", "sl", "tp", "pnl_pts",
                            "pnl_gross", "fees", "pnl_net", "risk_usd",
                            "r_multiple", "hold_minutes", "sweep_weight", "fvg_count"]:
                    if row.get(key):
                        try:
                            row[key] = float(row[key])
                        except ValueError:
                            pass
                # Backwards compat: if old CSV has pnl_usd, map it
                if "pnl_usd" in row and "pnl_net" not in row:
                    row["pnl_net"] = row.get("pnl_usd", 0)
                    row["pnl_gross"] = row.get("pnl_usd", 0)
                trades.append(row)
        return trades

    def summary(self, trades=None):
        """Print overall performance summary."""
        trades = trades or self.read_trades()
        if not trades:
            print("  No trades recorded yet.")
            return

        def _pnl(t):
            return t.get("pnl_net", t.get("pnl_gross", 0))

        wins = [t for t in trades if isinstance(_pnl(t), (int, float)) and _pnl(t) > 0]
        losses = [t for t in trades if isinstance(_pnl(t), (int, float)) and _pnl(t) <= 0]
        total_pnl = sum(_pnl(t) for t in trades if isinstance(_pnl(t), (int, float)))
        total_fees = sum(t.get("fees", 0) for t in trades if isinstance(t.get("fees"), (int, float)))
        n = len(trades)
        wr = len(wins) / n * 100 if n > 0 else 0

        gross_profit = sum(_pnl(t) for t in wins)
        gross_loss = abs(sum(_pnl(t) for t in losses))
        pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        avg_win = gross_profit / len(wins) if wins else 0
        avg_loss = -gross_loss / len(losses) if losses else 0
        wl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

        # Streaks
        max_win_streak = max_loss_streak = cur_win = cur_loss = 0
        for t in trades:
            if isinstance(_pnl(t), (int, float)):
                if _pnl(t) > 0:
                    cur_win += 1; cur_loss = 0
                    max_win_streak = max(max_win_streak, cur_win)
                else:
                    cur_loss += 1; cur_win = 0
                    max_loss_streak = max(max_loss_streak, cur_loss)

        # R-multiples
        r_values = [t["r_multiple"] for t in trades if isinstance(t.get("r_multiple"), (int, float))]
        avg_r = sum(r_values) / len(r_values) if r_values else 0

        # Account breakdown
        accounts = defaultdict(lambda: {"trades": 0, "pnl": 0, "fees": 0})
        for t in trades:
            acct = t.get("account", "UNKNOWN")
            accounts[acct]["trades"] += 1
            accounts[acct]["pnl"] += _pnl(t) if isinstance(_pnl(t), (int, float)) else 0
            accounts[acct]["fees"] += t.get("fees", 0) if isinstance(t.get("fees"), (int, float)) else 0

        print(f"\n  {'='*50}")
        print(f"  TRADE JOURNAL SUMMARY — {n} trades")
        print(f"  {'='*50}")
        print(f"  Net P&L:        ${total_pnl:+,.2f}")
        print(f"  Total Fees:     ${total_fees:,.2f}")
        print(f"  Win Rate:       {wr:.1f}% ({len(wins)}W / {len(losses)}L)")
        print(f"  Profit Factor:  {pf:.2f}")
        print(f"  Avg Win:        ${avg_win:+,.2f}")
        print(f"  Avg Loss:       ${avg_loss:+,.2f}")
        print(f"  W/L Ratio:      {wl_ratio:.2f}:1")
        print(f"  Avg R:          {avg_r:+.2f}R")
        print(f"  Max Win Streak: {max_win_streak}")
        print(f"  Max Loss Streak:{max_loss_streak}")
        if len(accounts) > 0:
            print(f"  ───────────────────────────────────────")
            for acct, data in sorted(accounts.items()):
                print(f"  {acct:<10} {data['trades']} trades  ${data['pnl']:+,.2f}  (fees: ${data['fees']:,.2f})")
        print(f"  {'='*50}")

    def weekly(self, trades=None):
        """Print weekly breakdown."""
        trades = trades or self.read_trades()
        if not trades:
            print("  No trades recorded yet.")
            return

        weeks = defaultdict(list)
        for t in trades:
            try:
                dt = datetime.strptime(t["date"], "%Y-%m-%d")
                week_start = dt - timedelta(days=dt.weekday())
                weeks[week_start.strftime("%Y-%m-%d")].append(t)
            except (ValueError, KeyError):
                pass

        print(f"\n  {'Week Starting':<15} {'Trades':>6} {'WR%':>6} {'P&L':>12} {'Avg R':>7}")
        print(f"  {'─'*48}")

        for week in sorted(weeks.keys()):
            wt = weeks[week]
            n = len(wt)
            wins = sum(1 for t in wt if isinstance(t.get("pnl_net"), (int, float)) and t["pnl_net"] > 0)
            wr = wins / n * 100 if n > 0 else 0
            pnl = sum(t["pnl_net"] for t in wt if isinstance(t.get("pnl_net"), (int, float)))
            r_vals = [t["r_multiple"] for t in wt if isinstance(t.get("r_multiple"), (int, float))]
            avg_r = sum(r_vals) / len(r_vals) if r_vals else 0
            print(f"  {week:<15} {n:>6} {wr:>5.0f}% ${pnl:>+10,.2f} {avg_r:>+6.2f}R")

    def health_check(self, trades=None):
        """Check for edge decay — compare recent vs historical performance."""
        trades = trades or self.read_trades()
        if len(trades) < 20:
            print("  Need at least 20 trades for health check.")
            return

        def _pnl(t):
            return t.get("pnl_net", t.get("pnl_gross", 0))

        recent = trades[-20:]
        historical = trades[:-20] if len(trades) > 40 else trades[:20]

        def stats(subset):
            n = len(subset)
            wins = sum(1 for t in subset if isinstance(_pnl(t), (int, float)) and _pnl(t) > 0)
            wr = wins / n * 100 if n > 0 else 0
            r_vals = [t["r_multiple"] for t in subset if isinstance(t.get("r_multiple"), (int, float))]
            avg_r = sum(r_vals) / len(r_vals) if r_vals else 0
            pnl = sum(_pnl(t) for t in subset if isinstance(_pnl(t), (int, float)))
            return wr, avg_r, pnl

        h_wr, h_r, h_pnl = stats(historical)
        r_wr, r_r, r_pnl = stats(recent)

        print(f"\n  {'='*55}")
        print(f"  EDGE HEALTH CHECK")
        print(f"  {'='*55}")
        print(f"  {'':20} {'Historical':>15} {'Recent 20':>15}")
        print(f"  {'─'*55}")
        print(f"  {'Win Rate':<20} {h_wr:>14.1f}% {r_wr:>14.1f}%")
        print(f"  {'Avg R-Multiple':<20} {h_r:>+14.2f}R {r_r:>+14.2f}R")
        print(f"  {'─'*55}")

        # Warnings
        warnings = []
        if r_wr < 50:
            warnings.append(f"  ⚠️  Win rate below 50% ({r_wr:.1f}%)")
        if r_wr < h_wr - 10:
            warnings.append(f"  ⚠️  Win rate dropped {h_wr - r_wr:.1f}% vs historical")
        if r_r < 0:
            warnings.append(f"  ⚠️  Negative average R ({r_r:.2f}R)")
        if r_r < h_r - 0.5:
            warnings.append(f"  ⚠️  Avg R dropped {h_r - r_r:.2f}R vs historical")

        if warnings:
            print()
            for w in warnings:
                print(w)
            print()
            print("  Consider: is market regime shifting? Review recent setups.")
        else:
            print()
            print("  ✅ Edge looks healthy — performance consistent with historical")
        print(f"  {'='*55}")


if __name__ == "__main__":
    journal = TradeJournal()

    if "--weekly" in sys.argv:
        journal.weekly()
    elif "--health" in sys.argv:
        journal.health_check()
    else:
        journal.summary()
