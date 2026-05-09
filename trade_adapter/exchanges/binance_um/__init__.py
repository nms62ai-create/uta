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

Phase 2b/2c will add:

    transport.py  — WS connect, ping/pong, reconnect with backoff,
                    request-id router for WS-API responses.
    trade.py      — WS-API ``order.place`` / ``order.cancel`` / batch.
    user.py       — USER_DATA_STREAM (listenKey lifecycle,
                    ORDER_TRADE_UPDATE, ACCOUNT_UPDATE).
    market.py     — depth, bookTicker, aggTrade.
    adapter.py    — wires the above into the ``ExchangeAdapter``
                    contract consumed by the signal router /
                    position manager (Phase 3).
"""

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
