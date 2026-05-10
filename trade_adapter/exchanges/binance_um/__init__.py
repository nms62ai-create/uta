"""Binance USD-M Futures venue adapter (decision A0 / A3b).

Trading and user-data are spoken WebSocket-first; REST is bootstrap,
reconciliation, and explicit fallback only. The package is split so
each transport concern stays self-contained:

    auth.py       — HMAC-SHA256 signing of REST query strings and
                    WS-API param objects. Pure, no I/O.
    symbols.py    — exchangeInfo cache + tick/step/min-notional
                    rounding using ``Decimal`` arithmetic. Pure,
                    no I/O.
    rest.py       — Signed REST client (``httpx.AsyncClient``). Used
                    for ``exchangeInfo``, ``positionRisk``,
                    ``openOrders``, ``account``, ``time``, plus
                    fallback order placement / cancel.
    ws/           — WebSocket layer (Phase 2b/2c):
                    transport.py — WS-API RPC transport.
                    trade.py     — signed ``order.place`` / ``order.cancel``.
                    stream.py    — push-only stream transport.
                    market.py    — combined market-data client.
                    user_stream.py — USER_DATA_STREAM with listenKey
                                    lifecycle.
    adapter.py    — Phase 3a venue glue: lifecycle owner for the four
                    clients above, ``OrderRequest`` → Binance params
                    translation, ``order.place`` result → ``OrderAck``.
"""

from .adapter import (
    BinanceUmAdapter,
    BinanceUmAdapterClosed,
    BinanceUmAdapterConfig,
    BinanceUmAdapterError,
    BinanceUmAdapterNotStarted,
    BinanceUmAdapterWrongVenue,
)
from .auth import (
    canonical_query_string,
    sign_hmac,
    sign_params,
    sign_query,
    url_encoded_query_string,
)
from .rest import (
    DEFAULT_BASE_URL,
    TESTNET_BASE_URL,
    BinanceApiError,
    BinanceHttpError,
    BinanceRestClient,
    BinanceRestError,
)
from .symbols import (
    SymbolInfo,
    SymbolRegistry,
    precision_from_step,
    precision_to_step,
)

__all__ = [
    "DEFAULT_BASE_URL",
    "TESTNET_BASE_URL",
    "BinanceApiError",
    "BinanceHttpError",
    "BinanceRestClient",
    "BinanceRestError",
    "BinanceUmAdapter",
    "BinanceUmAdapterClosed",
    "BinanceUmAdapterConfig",
    "BinanceUmAdapterError",
    "BinanceUmAdapterNotStarted",
    "BinanceUmAdapterWrongVenue",
    "SymbolInfo",
    "SymbolRegistry",
    "canonical_query_string",
    "precision_from_step",
    "precision_to_step",
    "sign_hmac",
    "sign_params",
    "sign_query",
    "url_encoded_query_string",
]
