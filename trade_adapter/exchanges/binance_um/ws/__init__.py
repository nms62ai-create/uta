"""WebSocket layer for Binance USD-M Futures.

Phase 2b ships only the bottom-layer transport — see :mod:`.transport`.
Phase 2c will add WS-trade (signed RPC for order placement / cancel /
batch), WS-user (USER_DATA_STREAM listenKey lifecycle, ORDER_TRADE_UPDATE
/ ACCOUNT_UPDATE), and WS-market (depth / bookTicker / aggTrade) on top.
"""

from .transport import (
    WsRpcClient,
    WsRpcClosed,
    WsRpcConfig,
    WsRpcDisconnected,
    WsRpcError,
    WsRpcTimeout,
)

__all__ = [
    "WsRpcClient",
    "WsRpcClosed",
    "WsRpcConfig",
    "WsRpcDisconnected",
    "WsRpcError",
    "WsRpcTimeout",
]
