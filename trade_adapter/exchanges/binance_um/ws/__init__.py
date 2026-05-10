"""WebSocket layer for Binance USD-M Futures.

Phase 2b shipped the bottom-layer WS-API transport — see :mod:`.transport`.
Phase 2c-1 added WS-trade (signed RPC) — see :mod:`.trade`.
Phase 2c-2 (this commit) adds the push-only stream transport
(:mod:`.stream`) and the combined market-data client (:mod:`.market`).
Phase 2c-3 will add the user-data stream (listenKey lifecycle,
ORDER_TRADE_UPDATE / ACCOUNT_UPDATE) on the same stream transport.
"""

from .market import MarketStreamClient, StreamHandler, build_combined_stream_url
from .stream import (
    MAINNET_STREAM_BASE_URL,
    TESTNET_STREAM_BASE_URL,
    MessageHandler,
    WsStreamClosed,
    WsStreamConfig,
    WsStreamError,
    WsStreamTransport,
)
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
    "MAINNET_STREAM_BASE_URL",
    "TESTNET_STREAM_BASE_URL",
    "BinanceWsApiError",
    "BinanceWsProtocolError",
    "BinanceWsTradeClient",
    "BinanceWsTradeError",
    "MarketStreamClient",
    "MessageHandler",
    "StreamHandler",
    "WsRpcClient",
    "WsRpcClosed",
    "WsRpcConfig",
    "WsRpcDisconnected",
    "WsRpcError",
    "WsRpcTimeout",
    "WsStreamClosed",
    "WsStreamConfig",
    "WsStreamError",
    "WsStreamTransport",
    "build_combined_stream_url",
]
