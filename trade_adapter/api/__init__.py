"""Public HTTP + WebSocket API.

Surface (planned):
    POST   /v1/signal             - submit UniversalSignal
    POST   /v1/order              - manual order bypass
    POST   /v1/position/close     - close a position
    GET    /v1/positions          - list current positions
    GET    /v1/orders             - list active orders
    GET    /v1/balances           - per-venue balances
    GET    /v1/health             - liveness + readiness + protocol version
    GET    /metrics               - Prometheus scrape (loopback only)
    WS     /v1/events             - outbound event stream

All endpoints require ``Authorization: Bearer <consumer_token>``. Tokens
are issued and revoked via the ``uta`` CLI; see ``docs/SECURITY.md``.

Modules:
    auth.py             - Bearer token verification dependency.
    signal_routes.py    - Signal submission endpoint and response model.
    order_routes.py     - Manual-bypass order placement.
    position_routes.py  - Close + query endpoints.
    info_routes.py      - Health, balances, orders read endpoints.
    metrics_routes.py   - Prometheus exporter.
    events_ws.py        - Outbound WebSocket fan-out.
    app.py              - Assembles the FastAPI app and lifespan hooks.
"""
