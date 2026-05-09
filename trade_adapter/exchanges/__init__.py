"""Exchange abstraction layer.

The ``ExchangeAdapter`` ABC defines a uniform interface for venues. Each
concrete venue lives in its own subpackage and implements the contract:
``binance/`` and ``bybit/`` in v1.0.

Adding a third venue (OKX, Hyperliquid, dYdX) means creating a new
subpackage that implements the ABC and registering it in the factory.
No changes elsewhere.

Public contract (``base.ExchangeAdapter``):
    place_order(req) -> OrderAck
    cancel_order(client_order_id, symbol)
    fetch_positions() -> list[Position]
    fetch_open_orders() -> list[Order]
    fetch_balance() -> Balance
    subscribe_user_data(on_event)
    subscribe_market_data(symbols, on_event)
    round_qty(symbol, qty) -> float
    round_price(symbol, price) -> float

Per-venue subpackage layout (``binance/``, ``bybit/``):
    rest.py          - Signed REST client; retry policy; rate limiting.
    ws_user.py       - Private user-data WebSocket. listenKey lifecycle
                       (Binance) or auth-on-connect (Bybit). Reconnect
                       with exponential backoff.
    ws_market.py     - Public market-data WebSocket subscription
                       manager.
    mappers.py       - Translators between venue-specific JSON shapes
                       and internal dataclasses (``Fill``,
                       ``OrderUpdate``, ``PositionUpdate``).
    errors.py        - Map venue error codes to shared ``ExchangeError``
                       enum (``RATE_LIMITED``, ``INSUFFICIENT_FUNDS``,
                       ``INVALID_QTY``, ``MARKET_CLOSED``, ``UNKNOWN``).
    adapter.py       - Concrete ``ExchangeAdapter`` subclass that wires
                       REST + WS clients together.
"""
