"""
test_connection.py — Validate SDK setup before running the bot.

Checks: credentials → auth → instrument → historical data → real-time stream

Usage:
  python test_connection.py
"""

import os
import sys
import asyncio

# ---- Credentials (read from environment — never hardcode) ----
import os
USERNAME = os.environ.get("PROJECT_X_USERNAME", "")
API_KEY  = os.environ.get("PROJECT_X_API_KEY", "")


async def main():
    os.environ["PROJECT_X_USERNAME"] = USERNAME
    os.environ["PROJECT_X_API_KEY"] = API_KEY

    print(f"🔑 User: {USERNAME}")
    print(f"🔑 Key:  {API_KEY[:10]}...\n")

    try:
        from project_x_py import TradingSuite, EventType
    except ImportError:
        print("❌ SDK not installed. Run: pip install project-x-py")
        sys.exit(1)

    # --- Auth ---
    print("1️⃣  Authenticating...")
    try:
        suite = await TradingSuite.create(instrument="MES", timeframes=["1min", "5min"])
        info = suite.client.account_info
        print(f"   ✅ Account: {info.name} | ID: {info.id}\n")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        sys.exit(1)

    # v3.5+: per-instrument context
    ctx = suite["MES"]

    # --- Instrument ---
    print("2️⃣  Resolving MES instrument...")
    try:
        inst = await suite.client.get_instrument(suite.instrument_id or "MES")
        print(f"   ✅ {inst.name} | Tick: {inst.tickSize} | Value: {inst.tickValue}\n")
    except Exception as e:
        print(f"   ❌ Failed: {e}\n")

    # --- Historical data ---
    print("3️⃣  Pulling 2 days of 5m bars...")
    try:
        data = await suite.client.get_bars("MES", days=2, interval=5)
        if data is not None and not data.is_empty():
            print(f"   ✅ {len(data)} bars | Columns: {data.columns}")
            for row in data.tail(1).iter_rows(named=True):
                print(f"   Last bar: {row['timestamp']} O={row['open']} H={row['high']} L={row['low']} C={row['close']}\n")
        else:
            print("   ⚠️  No data (market may be closed)\n")
    except Exception as e:
        print(f"   ❌ Failed: {e}\n")

    # --- Real-time ---
    print("4️⃣  Listening for real-time bars (30s)...")
    count = {"1min": 0, "5min": 0}

    async def on_bar(event):
        tf = event.data.get("timeframe", "?")
        bar = event.data.get("data", event.data)
        count[tf] = count.get(tf, 0) + 1
        print(f"   📊 {tf} #{count[tf]} | Close={bar.get('close', '?')}")

    await ctx.event_bus.on(EventType.NEW_BAR, on_bar)
    # NOTE: If this errors, try: ctx.event_bus.on(EventType.NEW_BAR, on_bar) without await

    await asyncio.sleep(30)

    total = sum(count.values())
    if total > 0:
        print(f"   ✅ {total} bars received\n")
    else:
        print("   ⚠️  No bars (market may be closed)\n")

    await suite.disconnect()
    print("✅ All checks complete.")


if __name__ == "__main__":
    asyncio.run(main())
