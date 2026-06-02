"""
broker/base.py — Abstract Broker Adapter Interface

All broker-specific operations behind one interface.
Engine.py never touches this — only run.py uses it.

Side encoding: 0 = BUY, 1 = SELL (matches engine.py convention)
"""

from abc import ABC, abstractmethod
from typing import Callable, Awaitable, Optional
from dataclasses import dataclass, field


@dataclass
class AccountInfo:
    name: str
    id: str


@dataclass
class InstrumentInfo:
    name: str
    tick_size: float
    tick_value: float
    contract_id: str  # TopstepX instrument_id
    exchange: str = ""  # Unused for TopstepX


@dataclass
class OrderResult:
    order_id: str


@dataclass
class BarEvent:
    """Delivered to the bar callback by the adapter."""
    timeframe: str  # "1min" or "5min"
    candle: dict    # {timestamp, open, high, low, close, volume}


@dataclass
class OrderEvent:
    """Delivered to the order callback by the adapter."""
    order_id: str
    fill_price: Optional[float] = None
    status: Optional[int] = None  # 2 = Filled, 3 = Cancelled


class BrokerAdapter(ABC):
    """
    Abstract interface for broker operations.

    Each adapter wraps a specific SDK (e.g. TopstepX)
    and translates its API into this common interface.

    The adapter owns contract/instrument resolution internally —
    run.py never deals with contract IDs.
    """

    # --- Connection lifecycle ---

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to the broker.
        Must resolve instrument/contract IDs internally.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Clean disconnect from broker."""
        ...

    # --- Account & instrument ---

    @abstractmethod
    async def get_account_info(self) -> AccountInfo:
        """Return account name and ID."""
        ...

    @abstractmethod
    async def get_instrument_info(self, symbol: str) -> InstrumentInfo:
        """
        Return instrument metadata (tick size, tick value, contract ID).
        The adapter may need to resolve front-month contracts.
        """
        ...

    @abstractmethod
    async def get_session_token(self) -> str:
        """
        Return a session token or identifier.
        TopstepX uses this for session auth.
        """
        ...

    # --- Historical data ---

    @abstractmethod
    async def get_historical_bars(
        self, symbol: str, days: int, interval_minutes: int
    ) -> list[dict]:
        """
        Fetch historical bars as a list of candle dicts.
        Each dict: {timestamp: float, open, high, low, close, volume}
        Adapter handles SDK-specific format conversion (Polars, etc).
        """
        ...

    # --- Real-time data ---

    @abstractmethod
    async def subscribe_bars(
        self,
        symbol: str,
        timeframes: list[str],
        callback: Callable[[BarEvent], Awaitable[None]],
    ) -> bool:
        """
        Subscribe to real-time completed bar events.

        The adapter is responsible for:
          - Delivering COMPLETED bars only (not partial/opening ticks)
          - Deduplicating (no duplicate bar deliveries)
          - Converting to standard candle dict format

        callback receives a BarEvent with timeframe and candle dict.
        Returns True if subscription succeeded.
        """
        ...

    # --- Orders ---

    @abstractmethod
    async def place_limit_order(
        self, side: int, size: int, price: float
    ) -> OrderResult:
        """
        Place a limit order. side: 0=BUY, 1=SELL.
        Returns OrderResult with the broker-assigned order ID.
        """
        ...

    @abstractmethod
    async def place_stop_order(
        self, side: int, size: int, price: float
    ) -> OrderResult:
        """
        Place a stop-market order. side: 0=BUY, 1=SELL.
        Returns OrderResult with the broker-assigned order ID.
        """
        ...

    @abstractmethod
    async def place_market_order(self, side: int, size: int) -> OrderResult:
        """
        Place a market order. side: 0=BUY, 1=SELL.
        Returns OrderResult with the broker-assigned order ID.
        """
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        """
        Cancel an order by ID.
        May raise if the order is already filled — caller handles this.
        """
        ...

    @abstractmethod
    async def cancel_all_orders(self) -> None:
        """Cancel all open orders for this instrument."""
        ...

    @abstractmethod
    async def is_order_filled(self, order_id: str) -> bool:
        """Check if an order has been filled."""
        ...

    @abstractmethod
    async def get_order_status(self, order_id: str) -> Optional[dict]:
        """
        Get order status details.
        Returns {"status": int, "fill_price": float|None} or None.
        Status: 2 = Filled, 3 = Cancelled
        """
        ...

    # --- Fill event subscription ---

    @abstractmethod
    async def subscribe_order_events(
        self, callback: Callable[[OrderEvent], Awaitable[None]]
    ) -> bool:
        """
        Subscribe to order fill/cancel events.

        The adapter normalizes events from the SDK into OrderEvent objects
        before calling the callback.

        Returns True if subscription succeeded.
        """
        ...
