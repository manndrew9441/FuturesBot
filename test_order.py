"""
test_order.py — Test manual 3-step bracket on TopstepX.

Methods: place_limit_order, place_stop_order, place_market_order, cancel_order
Fill detection: is_order_filled / get_tracked_order_status (NOT get_all_positions)
"""

import os, sys, asyncio, argparse, logging
from project_x_py import TradingSuite

USERNAME     = os.environ.get("PROJECT_X_USERNAME", "")
API_KEY      = os.environ.get("PROJECT_X_API_KEY", "")
ACCOUNT_NAME = os.environ.get("PROJECT_X_ACCOUNT_NAME", "")
INSTRUMENT   = "MES"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("TestOrder")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", default=ACCOUNT_NAME)
    parser.add_argument("--side", choices=["buy","sell"], default="buy")
    parser.add_argument("--qty", type=int, default=1)
    args = parser.parse_args()

    os.environ["PROJECT_X_USERNAME"] = USERNAME
    os.environ["PROJECT_X_API_KEY"] = API_KEY
    os.environ["PROJECT_X_ACCOUNT_NAME"] = args.account

    log.info("Connecting...")
    suite = await TradingSuite.create(instrument=INSTRUMENT, timeframes=["1min"])
    acct = suite.client.account_info
    log.info(f"✅ Account: {acct.name}")

    instrument = await suite.client.get_instrument(suite.instrument_id or INSTRUMENT)
    tick_size = instrument.tickSize
    point_value = (instrument.tickValue / tick_size) if instrument.tickValue and tick_size else 5.0

    ctx = suite[INSTRUMENT]
    order_mgr = getattr(ctx, 'orders', None) or suite.orders
    contract_id = getattr(ctx, 'instrument', None)
    contract_id = contract_id.id if contract_id else suite.instrument_id

    # Get price
    current_price = None
    try: current_price = await ctx.data.get_current_price()
    except Exception:
        bars = await suite.client.get_bars(INSTRUMENT, days=1, interval=1)
        if bars is not None and not bars.is_empty():
            current_price = float(bars.select("close").tail(1).item())
    if not current_price:
        log.error("No price"); await suite.disconnect(); sys.exit(1)

    side = 0 if args.side == "buy" else 1
    side_str = "BUY" if side == 0 else "SELL"
    close_side = 1 - side
    close_str = "SELL" if side == 0 else "BUY"
    qty = args.qty
    entry = round(current_price / tick_size) * tick_size
    if side == 0:
        sl = round((entry - 10.0) / tick_size) * tick_size
        tp = round((entry + 15.0) / tick_size) * tick_size
    else:
        sl = round((entry + 10.0) / tick_size) * tick_size
        tp = round((entry - 15.0) / tick_size) * tick_size

    log.info(f"{'='*60}")
    log.info(f"  {side_str} {qty}x @ {entry:.2f} | SL={sl:.2f} | TP={tp:.2f}")
    log.info(f"{'='*60}")

    # ================================================================
    # STEP 1: Entry — place_limit_order
    # ================================================================
    log.info(f"📋 STEP 1/3: place_limit_order {side_str} {qty}x @ {entry:.2f}")
    r = await order_mgr.place_limit_order(contract_id=contract_id, side=side, size=qty, limit_price=entry)
    entry_order_id = getattr(r, 'orderId', None)
    log.info(f"  ✅ Entry ID: {entry_order_id} | Response: {r}")

    if not entry_order_id:
        log.error("No entry order ID"); await suite.disconnect(); sys.exit(1)

    # ================================================================
    # STEP 2: Wait for fill via order tracker
    # ================================================================
    log.info(f"⏳ STEP 2/3: Waiting for fill (order tracker)...")
    fill_confirmed = False

    for attempt in range(24):  # 6 seconds
        await asyncio.sleep(0.25)

        # Primary: is_order_filled
        try:
            if await order_mgr.is_order_filled(entry_order_id):
                fill_confirmed = True
                log.info(f"  ✅ Fill confirmed via is_order_filled (attempt {attempt+1})")
                break
        except Exception as e:
            if attempt == 0: log.warning(f"  is_order_filled error: {e}")

        # Fallback: get_tracked_order_status
        try:
            status = await order_mgr.get_tracked_order_status(entry_order_id)
            if status == 2:
                fill_confirmed = True
                log.info(f"  ✅ Fill confirmed via tracked status=2 (attempt {attempt+1})")
                break
        except Exception:
            pass

    if not fill_confirmed:
        log.warning("  ⚠️ Not confirmed after 6s — trying cancel...")
        try:
            cancelled = await order_mgr.cancel_order(entry_order_id)
            if cancelled:
                log.info(f"  🗑️ Cancelled — no fill")
                await suite.disconnect(); sys.exit(0)
        except Exception as ce:
            if "filled" in str(ce).lower():
                fill_confirmed = True
                log.info(f"  ✅ Fill confirmed via cancel rejection")
            else:
                log.error(f"  Cancel error: {ce}")
                fill_confirmed = True  # Assume filled, SL will protect
                log.warning("  ⚠️ Assuming filled")

    if not fill_confirmed:
        await suite.disconnect(); sys.exit(1)

    # ================================================================
    # STEP 3: SL + TP
    # ================================================================
    log.info(f"📋 STEP 3/3: SL @ {sl:.2f} + TP @ {tp:.2f}")
    sl_order_id = None
    tp_order_id = None

    # SL
    try:
        r = await asyncio.wait_for(
            order_mgr.place_stop_order(contract_id=contract_id, side=close_side, size=qty, stop_price=sl),
            timeout=5.0)
        sl_order_id = getattr(r, 'orderId', None)
        log.info(f"  ✅ SL ID: {sl_order_id}")
    except Exception as e:
        log.error(f"  ❌ SL FAILED: {e}")

    # TP
    try:
        r = await asyncio.wait_for(
            order_mgr.place_limit_order(contract_id=contract_id, side=close_side, size=qty, limit_price=tp),
            timeout=5.0)
        tp_order_id = getattr(r, 'orderId', None)
        log.info(f"  ✅ TP ID: {tp_order_id}")
    except Exception as e:
        log.error(f"  ❌ TP FAILED: {e}")

    log.info(f"  Entry={entry_order_id} SL={sl_order_id} TP={tp_order_id}")

    if not sl_order_id:
        log.error("🚨 NO SL — flattening")
        try: await order_mgr.place_market_order(contract_id=contract_id, side=close_side, size=qty)
        except Exception as ef: log.error(f"🚨 {ef}")
        await suite.disconnect(); sys.exit(1)

    # ================================================================
    # Wait 10s
    # ================================================================
    log.info(f"⏳ Monitoring 10s...")
    for i in range(10, 0, -1):
        await asyncio.sleep(1)
        try:
            p = await ctx.data.get_current_price()
            pnl = ((p - entry) if side == 0 else (entry - p)) * qty * point_value
            log.info(f"  {i}s | {p:.2f} | ${pnl:+,.2f}")
        except Exception: log.info(f"  {i}s...")

    # ================================================================
    # Close
    # ================================================================
    log.info(f"Closing...")
    if sl_order_id:
        try: await order_mgr.cancel_order(sl_order_id); log.info(f"  🗑️ SL cancelled")
        except Exception as e: log.warning(f"  SL cancel: {e}")
    if tp_order_id:
        try: await order_mgr.cancel_order(tp_order_id); log.info(f"  🗑️ TP cancelled")
        except Exception as e: log.warning(f"  TP cancel: {e}")
    try:
        await order_mgr.place_market_order(contract_id=contract_id, side=close_side, size=qty)
        log.info(f"  ✅ Closed {close_str} {qty}x")
    except Exception as e:
        log.error(f"  ❌ Close: {e}")
        log.error(f"  🚨 CHECK TOPSTEPX UI")

    await asyncio.sleep(2)
    await suite.disconnect()
    log.info(f"✅ Done")

if __name__ == "__main__":
    asyncio.run(main())
