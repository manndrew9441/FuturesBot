"""
broker/topstepx.py — TopstepX Broker Adapter (project_x_py SDK)

Wraps the TopstepX ProjectX SDK behind the BrokerAdapter interface.
All SDK-specific quirks (event name fallbacks, data format conversion,
field name inconsistencies) are isolated here.
"""

import os
import asyncio
import logging
from datetime import datetime
from typing import Callable, Awaitable, Optional

from broker.base import (
    BrokerAdapter, AccountInfo, InstrumentInfo,
    OrderResult, BarEvent, OrderEvent,
)

log = logging.getLogger("Broker.TopstepX")


def _to_candle(row: dict) -> dict:
    """
    Convert a Polars row or SDK event data dict into a standard candle format.
    Moved from run.py — this is SDK-specific conversion logic.
    """
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


class TopstepXAdapter(BrokerAdapter):
    """
    Wraps project_x_py TradingSuite for the BrokerAdapter interface.

    SDK quirks handled internally:
      - Event subscription fallbacks (suite.on vs ctx.event_bus.on)
      - Multiple event name attempts (ORDER_UPDATED/FILLED/CHANGED)
      - NEW_BAR delivers first tick only — we fetch completed bar from ctx.data
      - Inconsistent order event payloads (orderId vs order_id vs id)
      - contractDisplayName bug in get_all_positions (never called)
    """

    def __init__(self, config: dict):
        self.config = config
        self.instrument = config.get("instrument", "MES")
        self.timeframes = ["1min", "5min"]

        # SDK objects — populated on connect()
        self._suite = None
        self._ctx = None
        self._order_mgr = None
        self._contract_id = None

        # Bar deduplication
        self._last_bar_ts = {"5min": None, "1min": None}

    async def connect(self) -> None:
        from project_x_py import TradingSuite

        # Set credentials for SDK
        os.environ["PROJECT_X_USERNAME"] = self.config.get("username", "")
        os.environ["PROJECT_X_API_KEY"] = self.config.get("api_key", "")
        acct_name = self.config.get("account_name", "")
        if acct_name:
            os.environ["PROJECT_X_ACCOUNT_NAME"] = acct_name

        log.info(f"Connecting to TopstepX for {self.instrument}...")
        self._suite = await TradingSuite.create(
            instrument=self.instrument,
            timeframes=self.timeframes,
        )

        self._ctx = self._suite[self.instrument]

        # Resolve order manager (SDK has multiple access patterns)
        self._order_mgr = getattr(self._ctx, 'orders', None) or self._suite.orders

        # Resolve contract ID
        ci = getattr(self._ctx, 'instrument', None)
        self._contract_id = ci.id if ci else self._suite.instrument_id

        log.info(f"Connected | Contract ID: {self._contract_id}")

    async def disconnect(self) -> None:
        if self._suite:
            await self._suite.disconnect()
            log.info("Disconnected from TopstepX")

    async def get_account_info(self) -> AccountInfo:
        acct = self._suite.client.account_info
        return AccountInfo(name=acct.name, id=str(acct.id))

    async def get_instrument_info(self, symbol: str) -> InstrumentInfo:
        inst = await self._suite.client.get_instrument(
            self._suite.instrument_id or symbol
        )
        return InstrumentInfo(
            name=inst.name,
            tick_size=inst.tickSize,
            tick_value=inst.tickValue,
            contract_id=str(self._contract_id),
        )

    async def get_session_token(self) -> str:
        try:
            return self._suite.client.get_session_token()
        except Exception:
            return ""

    async def get_historical_bars(
        self, symbol: str, days: int, interval_minutes: int
    ) -> list[dict]:
        hist = await self._suite.client.get_bars(
            symbol, days=days, interval=interval_minutes,
        )
        if hist is None or hist.is_empty():
            return []
        return [_to_candle(row) for row in hist.iter_rows(named=True)]

    async def subscribe_bars(
        self,
        symbol: str,
        timeframes: list[str],
        callback: Callable[[BarEvent], Awaitable[None]],
    ) -> bool:
        from project_x_py import EventType

        async def _on_new_bar(event):
            """
            SDK fires NEW_BAR on the FIRST TICK of a new bar period.
            The event data is only that opening tick (O=H=L=C).
            We fetch the COMPLETED previous bar from ctx.data instead.
            """
            try:
                tf = event.data.get("timeframe", "")
                if tf not in timeframes:
                    return

                data = await self._ctx.data.get_data(tf, bars=2)
                if data is None or len(data) < 2:
                    return  # First bar of session

                completed = data.row(-2, named=True)
                candle = _to_candle(completed)

                # Deduplicate — SDK may fire multiple events for same bar
                prev_ts = self._last_bar_ts.get(tf)
                if prev_ts is not None and candle["timestamp"] <= prev_ts:
                    return
                self._last_bar_ts[tf] = candle["timestamp"]

                await callback(BarEvent(timeframe=tf, candle=candle))

            except Exception as e:
                log.error(f"Bar processing error: {e}", exc_info=True)

        # Try multiple subscription patterns (SDK inconsistency)
        for method in [
            lambda: self._ctx.event_bus.on(EventType.NEW_BAR, _on_new_bar),
            lambda: self._suite.on(EventType.NEW_BAR, _on_new_bar),
        ]:
            try:
                result = method()
                if asyncio.iscoroutine(result):
                    await result
                log.info("Bar subscription active")
                return True
            except Exception:
                continue

        log.error("Could not subscribe to bar events")
        return False

    # --- Orders ---

    async def place_limit_order(
        self, side: int, size: int, price: float
    ) -> OrderResult:
        result = await self._order_mgr.place_limit_order(
            contract_id=self._contract_id,
            side=side,
            size=size,
            limit_price=price,
        )
        oid = getattr(result, 'orderId', None) or getattr(result, 'id', None)
        if not oid:
            raise RuntimeError(f"Could not extract order ID from: {result}")
        return OrderResult(order_id=str(oid))

    async def place_stop_order(
        self, side: int, size: int, price: float
    ) -> OrderResult:
        result = await self._order_mgr.place_stop_order(
            contract_id=self._contract_id,
            side=side,
            size=size,
            stop_price=price,
        )
        oid = getattr(result, 'orderId', None) or getattr(result, 'id', None)
        if not oid:
            raise RuntimeError(f"Could not extract order ID from: {result}")
        return OrderResult(order_id=str(oid))

    async def place_market_order(self, side: int, size: int) -> OrderResult:
        result = await self._order_mgr.place_market_order(
            contract_id=self._contract_id,
            side=side,
            size=size,
        )
        oid = getattr(result, 'orderId', None) or getattr(result, 'id', None)
        if not oid:
            raise RuntimeError(f"Could not extract order ID from: {result}")
        return OrderResult(order_id=str(oid))

    async def cancel_order(self, order_id: str) -> None:
        await self._order_mgr.cancel_order(order_id)

    async def cancel_all_orders(self) -> None:
        await self._order_mgr.cancel_all_orders()

    async def is_order_filled(self, order_id: str) -> bool:
        return await self._order_mgr.is_order_filled(order_id)

    async def get_order_status(self, order_id: str) -> Optional[dict]:
        tracked = await self._order_mgr.get_tracked_order_status(order_id)
        if tracked is None:
            return None

        status = (
            tracked.get("status") if isinstance(tracked, dict)
            else getattr(tracked, "status", None)
        )
        fill_price = None
        if isinstance(tracked, dict):
            fill_price = tracked.get("fillPrice") or tracked.get("avgFillPrice")
        else:
            fill_price = getattr(tracked, "fillPrice", None) or getattr(tracked, "avgFillPrice", None)

        return {"status": status, "fill_price": fill_price}

    async def subscribe_order_events(
        self, callback: Callable[[OrderEvent], Awaitable[None]]
    ) -> bool:
        from project_x_py import EventType

        async def _on_order_event(event):
            """Normalize the SDK's inconsistent event payloads into OrderEvent."""
            data = event.data if hasattr(event, 'data') else event

            # Extract order_id — SDK uses multiple field names
            order_id = None
            if isinstance(data, dict):
                order_id = (
                    data.get("order_id")
                    or data.get("orderId")
                    or data.get("id")
                )
                if order_id is None:
                    order_obj = data.get("order")
                    if order_obj is not None:
                        order_id = (
                            getattr(order_obj, "orderId", None)
                            or getattr(order_obj, "id", None)
                        )
                        if order_id is None and isinstance(order_obj, dict):
                            order_id = order_obj.get("orderId") or order_obj.get("id")
            else:
                order_id = (
                    getattr(data, "order_id", None)
                    or getattr(data, "orderId", None)
                    or getattr(data, "id", None)
                )

            if order_id is None:
                return

            # Extract fill price
            fill_price = None
            if isinstance(data, dict):
                fill_price = (
                    data.get("fillPrice")
                    or data.get("fill_price")
                    or data.get("avgFillPrice")
                )
                if fill_price is None:
                    order_obj = data.get("order")
                    if order_obj is not None:
                        fill_price = (
                            getattr(order_obj, "fillPrice", None)
                            or getattr(order_obj, "avgFillPrice", None)
                        )
                        if fill_price is None and isinstance(order_obj, dict):
                            fill_price = (
                                order_obj.get("fillPrice")
                                or order_obj.get("avgFillPrice")
                            )

            if fill_price is not None:
                try:
                    fill_price = float(fill_price)
                    if fill_price <= 0:
                        fill_price = None
                except (ValueError, TypeError):
                    fill_price = None

            await callback(OrderEvent(
                order_id=str(order_id),
                fill_price=fill_price,
            ))

        # Try multiple event names and subscription patterns
        for event_name in ['ORDER_UPDATED', 'ORDER_FILLED', 'ORDER_CHANGED']:
            evt = getattr(EventType, event_name, None)
            if evt is None:
                continue
            # Try suite-level first, then context-level
            for subscriber in [
                lambda e=evt: self._suite.on(e, _on_order_event),
                lambda e=evt: self._ctx.event_bus.on(e, _on_order_event),
            ]:
                try:
                    result = subscriber()
                    if asyncio.iscoroutine(result):
                        await result
                    log.info(f"Order event tracking via {event_name}")
                    return True
                except Exception:
                    continue

        log.warning("Could not subscribe to order events — polling fallback needed")
        return False
