# LiquidityBot — Automated Futures Trading System

A real-time algorithmic trading bot for micro E-mini S&P 500 futures (MES),
built around an ICT-style liquidity strategy. Connects to the TopstepX broker,
processes live market data, and executes bracket orders automatically — with a
matching offline backtester and replay system that run the *same* strategy code.

This is an engineering project. It demonstrates real-time market-data handling,
broker integration, deterministic backtesting, and — importantly — the discovery
and correction of a look-ahead bias that was inflating the strategy's results.

## What it does

- Streams 1-minute and 5-minute bars from TopstepX over WebSocket
- Runs a state-machine strategy engine (sweep → break-of-structure → retracement → entry)
- Places **bracket orders** (limit entry + stop-loss + take-profit as OCO) automatically
- Manages risk, position sizing, and a daily trade limit
- Logs every bar and state transition for full forensic traceability
- Backtests the identical engine over years of historical data
- Replays captured live bars to verify live/backtest parity bar-for-bar

## Architecture

```
Live market data (TopstepX WebSocket)
        │  1m + 5m bars
        ▼
  Bar drainer  ── debounce + canonical ordering (5m before 1m at a tie)
        ▼
  Strategy engine (engine.py)  ── WAITING → SWEPT → CONFIRMED_SHIFT → RETRACED → entry
        │  signal
        ▼
  Bracket executor  ── LIMIT entry, then STOP + LIMIT (OCO) once filled
        ▼
  Broker (TopstepX)  ── order goes to market
        │  fill events
        ▼
  Risk manager + journal  ── sizing, P&L state, trade log
```

Everything is captured to `bars/*.jsonl` (every bar seen) and `ict_bot.log`
(every state transition), so any decision can be reconstructed after the fact.

## Why it processes bars carefully

The live WebSocket delivers a 5-minute bar only when it *closes* — five minutes
after it opened. A naive backtest, by contrast, can hand the engine a completed
5-minute bar at its open timestamp, letting the strategy "see the future." That
gap was the source of a major **look-ahead bias**.

- The live bot uses a debounced drainer that orders simultaneous bars
  deterministically (5m before 1m at the same timestamp), matching the offline
  tools exactly.
- The backtester (`backtest_replay.py`) defaults to **realistic live-delivery
  ordering** so its results reflect what would actually happen live. A
  `--lookahead-mode` flag reproduces the old inflated numbers purely to
  demonstrate the size of the bias.

Discovering this was the most valuable outcome of the project: the original
backtest looked far more profitable than the strategy actually is, and only
realistic bar-delivery simulation revealed the true (much more modest) edge.

## Layout

| File | Purpose |
|---|---|
| `run.py` | Live trading entry point — orchestrates broker, engine, execution, risk |
| `engine.py` | Strategy engine (`ICTEngine`) — the sweep/BOS/retracement state machine |
| `replay.py` | Re-run the engine over captured or historical bars (no live trading) |
| `backtest_replay.py` | Multi-year backtester with realistic-delivery default |
| `broker/base.py` | Broker abstraction (`BrokerAdapter` interface) |
| `broker/topstepx.py` | TopstepX implementation of the adapter |
| `journal.py` | Trade logging to CSV |
| `config.example.json` | Configuration template (copy to `config.json`) |
| `archive/` | Superseded engine variants kept for reference |

## Setup

```bash
pip install -r requirements.txt        # or: pip install project_x_py pytz
cp config.example.json config.json     # then fill in your TopstepX credentials
```

`config.json` holds your API key and is **gitignored** — never commit it. The
bot reads credentials from there (or from `PROJECT_X_*` environment variables).

## Usage

```bash
# Backtest the strategy over historical 1-minute data (realistic delivery by default)
python3 backtest_replay.py --file-1m data.csv --start 2021-01-01 --end 2026-01-01 --combine --fees

# Demonstrate the look-ahead bias for comparison (inflated, unrealistic numbers)
python3 backtest_replay.py --file-1m data.csv --combine --fees --lookahead-mode

# Replay a captured live session bar-for-bar
python3 replay.py --from-bar-log bars/2026-05-22.jsonl

# Run live (requires configured TopstepX account)
python3 run.py
```

## Honest results note

Under realistic bar-delivery simulation, this strategy is roughly break-even over
a 5-year backtest — the original strong-looking numbers were an artifact of
look-ahead bias. The value of this repo is the **engineering**: real-time data
pipelines, broker integration, deterministic backtesting, live/replay parity, and
the rigor to find a bias most backtests never catch.

## Security

`config.json`, logs, captured bars, and account state are gitignored. Credentials
are never hardcoded. If you fork this, set your own credentials in `config.json`
or via environment variables and rotate any key that has ever been exposed.
