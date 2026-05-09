"""WebSocket layer for Binance USD-M Futures.

Phase 2b shipped the bottom-layer transport — see :mod:`.transport`.
Phase 2c-1 (this commit) adds WS-trade (signed RPC for order placement /
cancel / status) on top — see :mod:`.trade`. Phase 2c-2 will add
WS-user (USER_DATA_STREAM listenKey lifecycle, ORDER_TRADE_UPDATE /
ACCOUNT_UPDATE) and WS-market (depth / bookTicker / aggTrade) on a
separate stream transport.
"""

from .trade import (
    BinanceWsApiError,
    BinanceWsProtocolError,
    BinanceWsTradeClient,
    BinanceWsTradeError,
)
from .transport import (
    WsRpcClient,
    WsRpcClosed,
    WsRpcConfig,
    WsRpcDisconnected,
    WsRpcError,
    WsRpcTimeout,
)

__all__ = [
    "BinanceWsApiError",
    "BinanceWsProtocolError",
    "BinanceWsTradeClient",
    "BinanceWsTradeError",
    "WsRpcClient",
    "WsRpcClosed",
    "WsRpcConfig",
    "WsRpcDisconnected",
    "WsRpcError",
    "WsRpcTimeout",
]
