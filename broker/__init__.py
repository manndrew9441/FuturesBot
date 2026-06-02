"""
broker/__init__.py — Broker adapter factory.

Usage:
    from broker import get_broker
    broker = get_broker(config_dict)
    await broker.connect()
"""

from broker.base import BrokerAdapter, AccountInfo, InstrumentInfo, OrderResult, BarEvent, OrderEvent


def get_broker(config: dict) -> BrokerAdapter:
    """
    Factory: returns the correct broker adapter based on config["broker"].
    """
    broker_name = config.get("broker", "topstepx").lower()

    if broker_name == "topstepx":
        from broker.topstepx import TopstepXAdapter
        return TopstepXAdapter(config)
    else:
        raise ValueError(
            f"Unknown broker: '{broker_name}'. Valid options: 'topstepx'"
        )


__all__ = [
    "get_broker",
    "BrokerAdapter",
    "AccountInfo",
    "InstrumentInfo",
    "OrderResult",
    "BarEvent",
    "OrderEvent",
]
