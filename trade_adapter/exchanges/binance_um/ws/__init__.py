"""WebSocket layer for Binance USD-M Futures.

Phase 2b shipped the bottom-layer WS-API transport — see :mod:`.transport`.
Phase 2c-1 added WS-trade (signed RPC) — see :mod:`.trade`.
Phase 2c-2 added the push-only stream transport (:mod:`.stream`) and
the combined market-data client (:mod:`.market`).
Phase 2c-3 (this commit) adds the user-data stream client
(:mod:`.user_stream`): listenKey REST lifecycle, periodic keepalive,
listenKeyExpired-driven rotation, ORDER_TRADE_UPDATE / ACCOUNT_UPDATE /
MARGIN_CALL / ACCOUNT_CONFIG_UPDATE event routing.
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
from .user_stream import (
    DEFAULT_KEEPALIVE_INTERVAL_S,
    LISTEN_KEY_EXPIRED_EVENT,
    EventHandler,
    UserDataStreamClient,
    UserDataStreamClosed,
    UserDataStreamConfig,
    UserDataStreamError,
)

__all__ = [
    "DEFAULT_KEEPALIVE_INTERVAL_S",
    "LISTEN_KEY_EXPIRED_EVENT",
    "MAINNET_STREAM_BASE_URL",
    "TESTNET_STREAM_BASE_URL",
    "BinanceWsApiError",
    "BinanceWsProtocolError",
    "BinanceWsTradeClient",
    "BinanceWsTradeError",
    "EventHandler",
    "MarketStreamClient",
    "MessageHandler",
    "StreamHandler",
    "UserDataStreamClient",
    "UserDataStreamClosed",
    "UserDataStreamConfig",
    "UserDataStreamError",
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
