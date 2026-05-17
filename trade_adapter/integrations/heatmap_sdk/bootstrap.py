"""One-call bootstrap that wires UTA's Binance USD-M stack for heatmap-sdk.

What this module gives you
--------------------------

A single async :func:`build_stack` function (and the :class:`HeatmapUtaStack`
container it returns) that assembles every UTA layer you need to drive
order execution from heatmap-sdk's UI:

* REST client + WS-trade transport + USER_DATA_STREAM client
* :class:`BinanceUmAdapter`
* :class:`PositionStore` + :class:`PositionManager` (with auto-reconcile)
* :class:`RiskGate` + :class:`RiskState`
* :class:`SignalRouter`
* embedded :class:`TradeAdapter` (the public producer-facing API)
* :class:`HeatmapAutoTrader` (UI-gated signal forwarder)
* :class:`HeatmapEventBroadcaster` (event-bus -> browser-WS fan-out)
* a fresh :class:`SqliteDAO` for idempotency persistence

Why it exists
-------------

``INTEGRATION.md`` documents the wiring step-by-step. That recipe is the
right abstraction for *understanding* the integration, but copying it
into a real heatmap-sdk service is mechanical, easy to get wrong, and
adds 80 lines to ``ws_session.py``. This module collapses the entire
recipe into:

    stack = await build_stack(HeatmapUtaConfig(api_key=..., api_secret=...))
    stack.update_settings(AutotradeSettings(symbol="BTCUSDT", ...))
    stack.attach_browser_sender(live_heatmap_service.broadcast)
    # on every adaptive signal:
    await stack.on_adaptive_signal(signal)
    # on every market trade:
    stack.on_market_trade(symbol, last_price)
    # on shutdown:
    await stack.stop()

What it deliberately does *not* do
----------------------------------

* It does NOT speak to the Binance market-data WS — heatmap-sdk already
  has its own market client, and UTA only needs ``get_reference_price``
  for sizing. :class:`LastTradeMarketData` exposes a single ``update``
  method that the caller pumps from its existing aggTrade callback.
* It does NOT subscribe to user-data events from heatmap-sdk — UTA owns
  the listenKey lifecycle and routes ``ORDER_TRADE_UPDATE`` /
  ``ACCOUNT_UPDATE`` directly into its own :class:`PositionManager`.
* It does NOT touch heatmap-sdk's database, FastAPI app, or any other
  surface. It is purely a self-contained UTA bring-up.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.position_manager import PositionManager
from trade_adapter.core.position_store import PositionStore
from trade_adapter.core.risk import RiskConfig, RiskGate, RiskState
from trade_adapter.core.signal_router import SignalRouter
from trade_adapter.embedded import TradeAdapter
from trade_adapter.exchanges.binance_um import (
    DEFAULT_BASE_URL,
    TESTNET_BASE_URL,
    BinanceRestClient,
    BinanceUmAdapter,
    BinanceUmAdapterConfig,
    BinanceUmSnapshotProvider,
    make_user_data_handlers,
)
from trade_adapter.exchanges.binance_um.ws.trade import BinanceWsTradeClient
from trade_adapter.exchanges.binance_um.ws.transport import WsRpcClient, WsRpcConfig
from trade_adapter.exchanges.binance_um.ws.user_stream import (
    UserDataStreamClient,
    UserDataStreamConfig,
)
from trade_adapter.storage.idempotency import IdempotencyCache
from trade_adapter.storage.sqlite import SqliteDAO
from trade_adapter.types import SignalAck, Venue

from .autotrader import AutotradeSettings, HeatmapAutoTrader
from .event_broadcaster import HeatmapEventBroadcaster, WsSender
from .translator import AdaptiveSignal

_log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Default URLs
# ----------------------------------------------------------------------

# WS-API (signed RPC) endpoints. These are *not* the public stream URLs;
# they're the FAPI WebSocket request/response endpoints used by
# :class:`BinanceWsTradeClient` for ``order.place`` / ``order.cancel`` etc.
MAINNET_WS_TRADE_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
TESTNET_WS_TRADE_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"

# USER_DATA_STREAM base URLs — the listenKey is appended as ``/ws/<key>``.
MAINNET_USER_STREAM_BASE_URL = "wss://fstream.binance.com"
TESTNET_USER_STREAM_BASE_URL = "wss://stream.binancefuture.com"


# ----------------------------------------------------------------------
# Tiny MarketDataProvider for heatmap-sdk's existing market client
# ----------------------------------------------------------------------


class LastTradeMarketData:
    """A :class:`MarketDataProvider` backed by externally-supplied prices.

    heatmap-sdk's :class:`BinanceMarketClient` already streams aggTrade
    frames into ``LiveHeatmapService``. We just need the last trade
    price per (venue, symbol) so :class:`SignalRouter` can resolve
    :class:`NotionalUsd` → ``qty`` at sizing time.

    Thread-safety: simple ``dict`` writes are atomic under the GIL and
    the only ever read-after-write happens on the asyncio event loop,
    so no lock is required.
    """

    __slots__ = ("_prices",)

    def __init__(self) -> None:
        self._prices: dict[tuple[Venue, str], float] = {}

    def update(self, venue: Venue, symbol: str, price: float) -> None:
        """Record ``price`` as the last reference for ``(venue, symbol)``.

        Raises:
            ValueError: if ``price`` is not a positive finite number.
        """

        p = float(price)
        if not (p > 0.0 and p < float("inf")):
            raise ValueError(f"price must be positive finite, got {price!r}")
        self._prices[(venue, symbol.upper())] = p

    def get_reference_price(self, venue: Venue, symbol: str) -> float | None:
        return self._prices.get((venue, symbol.upper()))


# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class HeatmapUtaConfig:
    """Caller-supplied parameters for :func:`build_stack`.

    The defaults are chosen for **testnet-first** development so a
    careless invocation can't fire real orders. Flip ``testnet=False``
    only when you're ready to trade mainnet.

    Attributes:
        api_key, api_secret: Credentials for Binance USD-M Futures.
        testnet: When ``True`` (default), point REST + WS-trade +
            user-stream URLs at Binance's testnet hosts. When ``False``,
            use mainnet URLs.
        db_path: Path to the SQLite file backing the
            :class:`IdempotencyCache`. ``":memory:"`` is fine for tests
            but disappears between restarts — use a real path in prod.
        risk_config: Optional pre-built :class:`RiskConfig`. Defaults to
            "no caps configured" — explicit producer caps go here.
        reconcile_interval_s: How often the :class:`PositionManager`
            re-fetches positions + equity from REST to correct any
            user-data-stream drift. ``0`` disables the periodic sweep
            (only the on-start bootstrap runs).
        load_exchange_info_on_start: Whether the venue adapter pulls
            ``GET /fapi/v1/exchangeInfo`` on :meth:`start` to populate
            tick / step / min-notional filters. Almost always wanted in
            prod; tests with no live REST set it to ``False``.
        autotrade_settings: Optional initial :class:`AutotradeSettings`
            handed to :class:`HeatmapAutoTrader`. None ⇒ the autotrader
            starts disabled and waits for ``update_settings``.
        rest_base_url, ws_trade_url, user_stream_base_url: Override the
            URL pickers above. Useful for local mock servers; otherwise
            leave at ``None`` to use the testnet/mainnet defaults.
    """

    api_key: str
    api_secret: str
    testnet: bool = True
    db_path: str = "./uta_state.db"
    risk_config: RiskConfig | None = None
    reconcile_interval_s: float = 30.0
    load_exchange_info_on_start: bool = True
    autotrade_settings: AutotradeSettings | None = None
    rest_base_url: str | None = None
    ws_trade_url: str | None = None
    user_stream_base_url: str | None = None

    def resolved_rest_base_url(self) -> str:
        if self.rest_base_url is not None:
            return self.rest_base_url
        return TESTNET_BASE_URL if self.testnet else DEFAULT_BASE_URL

    def resolved_ws_trade_url(self) -> str:
        if self.ws_trade_url is not None:
            return self.ws_trade_url
        return TESTNET_WS_TRADE_URL if self.testnet else MAINNET_WS_TRADE_URL

    def resolved_user_stream_base_url(self) -> str:
        if self.user_stream_base_url is not None:
            return self.user_stream_base_url
        return (
            TESTNET_USER_STREAM_BASE_URL
            if self.testnet
            else MAINNET_USER_STREAM_BASE_URL
        )


# ----------------------------------------------------------------------
# Builder hooks (override for tests; default to real constructors)
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Builders:
    """Construction hooks. Tests replace these with mock factories.

    Production callers never touch this dataclass — :func:`build_stack`
    falls back to real Binance clients when ``builders=None``. The
    typing on each field matches what the real constructor returns
    (not a Protocol) so a swap-out has to opt into the actual
    interface.
    """

    rest: Callable[[HeatmapUtaConfig], BinanceRestClient]
    ws_rpc: Callable[[HeatmapUtaConfig], WsRpcClient]
    ws_trade: Callable[
        [HeatmapUtaConfig, WsRpcClient], BinanceWsTradeClient
    ]
    user_stream: Callable[
        [HeatmapUtaConfig, BinanceRestClient, dict[str, Any]],
        UserDataStreamClient,
    ]
    exchange: Callable[
        [
            HeatmapUtaConfig,
            BinanceRestClient,
            BinanceWsTradeClient,
            UserDataStreamClient,
        ],
        BinanceUmAdapter,
    ]
    dao: Callable[[HeatmapUtaConfig], SqliteDAO]


def _default_rest(config: HeatmapUtaConfig) -> BinanceRestClient:
    return BinanceRestClient(
        api_key=config.api_key,
        api_secret=config.api_secret,
        base_url=config.resolved_rest_base_url(),
    )


def _default_ws_rpc(config: HeatmapUtaConfig) -> WsRpcClient:
    return WsRpcClient(WsRpcConfig(url=config.resolved_ws_trade_url()))


def _default_ws_trade(
    config: HeatmapUtaConfig, rpc: WsRpcClient
) -> BinanceWsTradeClient:
    return BinanceWsTradeClient(
        rpc=rpc,
        api_key=config.api_key,
        api_secret=config.api_secret,
        own_rpc=True,
    )


def _default_user_stream(
    config: HeatmapUtaConfig,
    rest: BinanceRestClient,
    handlers: dict[str, Any],
) -> UserDataStreamClient:
    return UserDataStreamClient(
        rest=rest,
        handlers=handlers,
        config=UserDataStreamConfig(
            stream_base_url=config.resolved_user_stream_base_url(),
        ),
    )


def _default_exchange(
    config: HeatmapUtaConfig,
    rest: BinanceRestClient,
    ws_trade: BinanceWsTradeClient,
    user_stream: UserDataStreamClient,
) -> BinanceUmAdapter:
    return BinanceUmAdapter(
        rest=rest,
        ws_trade=ws_trade,
        user_stream=user_stream,
        config=BinanceUmAdapterConfig(
            load_exchange_info_on_start=config.load_exchange_info_on_start,
        ),
    )


def _default_dao(config: HeatmapUtaConfig) -> SqliteDAO:
    return SqliteDAO(path=config.db_path)


_DEFAULT_BUILDERS = _Builders(
    rest=_default_rest,
    ws_rpc=_default_ws_rpc,
    ws_trade=_default_ws_trade,
    user_stream=_default_user_stream,
    exchange=_default_exchange,
    dao=_default_dao,
)


# ----------------------------------------------------------------------
# Stack container
# ----------------------------------------------------------------------


@dataclass(slots=True)
class HeatmapUtaStack:
    """Wired UTA stack handed to a heatmap-sdk consumer.

    Attributes are exposed as public so consumers can subscribe to
    additional event topics, inspect risk state, etc. Lifecycle is
    owned here: call :meth:`start` once on boot, :meth:`stop` once on
    shutdown.

    Common operations have one-line helpers:

    * :meth:`on_adaptive_signal` — forward an adaptive signal to UTA.
    * :meth:`on_market_trade` — feed last price for $-notional sizing.
    * :meth:`attach_browser_sender` — wire the bus → browser fan-out.
    * :meth:`update_settings` — replace the autotrade settings.

    Lower-level access (event bus, position store, risk state) is
    available via the matching attributes when the helpers are not
    enough.
    """

    config: HeatmapUtaConfig

    # Core components (populated by :func:`build_stack`).
    dao: SqliteDAO
    bus: EventBus
    store: PositionStore
    market_data: LastTradeMarketData
    risk_state: RiskState
    risk_gate: RiskGate
    idempotency: IdempotencyCache
    rest: BinanceRestClient
    ws_rpc: WsRpcClient
    ws_trade: BinanceWsTradeClient
    user_stream: UserDataStreamClient
    exchange: BinanceUmAdapter
    position_manager: PositionManager
    signal_router: SignalRouter
    trade_adapter: TradeAdapter
    autotrader: HeatmapAutoTrader

    # Optional, lazily-bound bridges.
    broadcaster: HeatmapEventBroadcaster | None = field(default=None)

    _started: bool = field(default=False, init=False)
    _stopped: bool = field(default=False, init=False)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start every owned component in dependency order.

        Idempotent — calling twice is a no-op. The :class:`TradeAdapter`
        manages its own children (``exchange.start()`` then
        ``position_manager.start()``); we only own the DAO + the
        user-stream client here. If any single ``.start`` raises, we
        surface it unchanged so the caller can call :meth:`stop` to
        tear down whatever did come up.
        """

        if self._started:
            return
        await self.dao.open()
        await self.trade_adapter.start()
        # TradeAdapter starts the venue (which owns ws_rpc + ws_trade)
        # but not user_stream. Start it explicitly so ORDER_TRADE_UPDATE
        # / ACCOUNT_UPDATE frames reach the handlers we wired.
        await self.user_stream.start()
        self._started = True
        _log.info(
            "heatmap-uta stack started (testnet=%s, db=%s)",
            self.config.testnet,
            self.config.db_path,
        )

    async def stop(self) -> None:
        """Tear down everything :meth:`start` brought up. Idempotent.

        Components are stopped in reverse construction order. Each
        ``stop`` / ``close`` is best-effort: exceptions are logged but
        never raised so a single faulty teardown can't strand the
        others.
        """

        if self._stopped:
            return
        self._stopped = True

        async def _safe(coro_factory: Callable[[], Awaitable[None]], name: str) -> None:
            try:
                await coro_factory()
            except Exception as e:  # pragma: no cover - defensive
                _log.warning("heatmap-uta stack failed to stop %s: %s", name, e)

        if self.broadcaster is not None:
            await _safe(self.broadcaster.stop, "broadcaster")
        await _safe(self.user_stream.close, "user_stream")
        # TradeAdapter.close() walks position_manager.stop() then
        # exchange.close() (which closes ws_rpc + ws_trade).
        await _safe(self.trade_adapter.close, "trade_adapter")
        await _safe(self.rest.aclose, "rest")
        await _safe(self.dao.close, "dao")
        _log.info("heatmap-uta stack stopped")

    # ------------------------------------------------------------------
    # Pass-through helpers (consumer-facing API)
    # ------------------------------------------------------------------

    def update_settings(self, settings: AutotradeSettings) -> None:
        """Forward to :meth:`HeatmapAutoTrader.update_settings`."""

        self.autotrader.update_settings(settings)

    def enable(self) -> None:
        """Flip autotrade on (settings must already be configured)."""

        self.autotrader.enable()

    def disable(self) -> None:
        """Flip autotrade off without touching settings."""

        self.autotrader.disable()

    async def on_adaptive_signal(
        self, signal: AdaptiveSignal
    ) -> SignalAck | None:
        """Forward an adaptive_sdk-shaped signal to the autotrader."""

        return await self.autotrader.on_adaptive_signal(signal)

    def on_market_trade(
        self, symbol: str, price: float, *, venue: Venue = Venue.BINANCE_UM
    ) -> None:
        """Record ``price`` as the most recent reference for ``symbol``.

        Pump this from heatmap-sdk's existing ``on_agg_trade`` / order-
        book callback so :class:`SignalRouter` can resolve $-notional
        sizing the moment a signal arrives.
        """

        self.market_data.update(venue, symbol, price)

    def attach_browser_sender(self, send: WsSender) -> HeatmapEventBroadcaster:
        """Build and start a :class:`HeatmapEventBroadcaster` over ``send``.

        ``send`` is the async function that heatmap-sdk uses to push
        JSON to every connected browser — typically
        ``LiveHeatmapService.broadcast``. Returns the broadcaster so
        the caller can inspect ``is_running`` / call ``stop`` directly
        if they want fine-grained control; :meth:`stop` on this stack
        will also stop the broadcaster, so most callers can ignore the
        return value.

        Calling this twice raises :class:`RuntimeError` — UTA only
        supports a single browser sink per stack today; if you need
        more, build a fan-out yourself on top of ``trade_adapter.subscribe``.
        """

        if self.broadcaster is not None:
            raise RuntimeError(
                "attach_browser_sender already called on this stack"
            )
        broadcaster = HeatmapEventBroadcaster(
            adapter=self.trade_adapter, send=send
        )
        broadcaster.start()
        self.broadcaster = broadcaster
        return broadcaster

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def trip_kill_switch(self, reason: str) -> None:
        """Forward to :meth:`TradeAdapter.trip_kill_switch`."""

        self.trade_adapter.trip_kill_switch(reason)

    def reset_kill_switch(self) -> None:
        """Forward to :meth:`TradeAdapter.reset_kill_switch`."""

        self.trade_adapter.reset_kill_switch()

    @property
    def is_started(self) -> bool:
        return self._started and not self._stopped

    @property
    def is_stopped(self) -> bool:
        return self._stopped


# ----------------------------------------------------------------------
# Builder
# ----------------------------------------------------------------------


async def build_stack(
    config: HeatmapUtaConfig,
    *,
    builders: _Builders | None = None,
    auto_start: bool = True,
) -> HeatmapUtaStack:
    """Assemble and start a wired :class:`HeatmapUtaStack`.

    Construction order matches dependency order: shared infra first
    (DAO, bus, store, risk, market-data, idempotency), then venue
    transports (REST, WS-trade), then position manager + user-stream
    handlers, then the adapter + signal router + embedded TradeAdapter,
    then the autotrader.

    Parameters
    ----------
    config:
        Caller-supplied :class:`HeatmapUtaConfig`. ``api_key`` and
        ``api_secret`` are required even with ``builders`` overridden,
        because :class:`BinanceWsTradeClient.call_signed` rejects
        empty creds upfront.
    builders:
        Internal hook used by tests to swap in mock REST / WS / etc.
        clients. Production callers leave this as ``None``.
    auto_start:
        When ``True`` (default), :func:`build_stack` calls
        :meth:`HeatmapUtaStack.start` before returning. When ``False``,
        the caller is responsible for calling :meth:`start` later. Tests
        that don't want the position-manager bootstrap to hit a real
        REST mock during construction can pass ``False``.
    """

    b = builders or _DEFAULT_BUILDERS

    # ---- shared infra ------------------------------------------------
    dao = b.dao(config)
    bus = EventBus()
    store = PositionStore()
    market_data = LastTradeMarketData()
    risk_state = RiskState()
    risk_gate = RiskGate(
        config=config.risk_config or RiskConfig(),
        state=risk_state,
        position_provider=store,
    )
    idempotency = IdempotencyCache(dao, ttl_s=3600.0)

    # ---- venue transports --------------------------------------------
    rest = b.rest(config)
    ws_rpc = b.ws_rpc(config)
    ws_trade = b.ws_trade(config, ws_rpc)

    # ---- position manager + user-data handlers -----------------------
    snapshot_provider = BinanceUmSnapshotProvider(rest=rest)
    position_manager = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snapshot_provider,
        event_bus=bus,
        reconcile_interval_s=config.reconcile_interval_s,
    )
    handlers = make_user_data_handlers(
        position_manager=position_manager,
        event_bus=bus,
        risk_state=risk_state,
    )
    user_stream = b.user_stream(config, rest, handlers)

    # ---- venue adapter + signal router -------------------------------
    exchange = b.exchange(config, rest, ws_trade, user_stream)
    signal_router = SignalRouter(
        adapter=exchange,
        idempotency=idempotency,
        market_data=market_data,
        position_provider=store,
        equity_provider=store,
        event_bus=bus,
        risk_gate=risk_gate,
    )

    # ---- embedded TradeAdapter + autotrader --------------------------
    trade_adapter = TradeAdapter(
        venue=Venue.BINANCE_UM,
        exchange_adapter=exchange,
        signal_router=signal_router,
        event_bus=bus,
        position_manager=position_manager,
        risk_state=risk_state,
        risk_gate=risk_gate,
    )
    autotrader = HeatmapAutoTrader(
        trade_adapter, settings=config.autotrade_settings
    )

    stack = HeatmapUtaStack(
        config=config,
        dao=dao,
        bus=bus,
        store=store,
        market_data=market_data,
        risk_state=risk_state,
        risk_gate=risk_gate,
        idempotency=idempotency,
        rest=rest,
        ws_rpc=ws_rpc,
        ws_trade=ws_trade,
        user_stream=user_stream,
        exchange=exchange,
        position_manager=position_manager,
        signal_router=signal_router,
        trade_adapter=trade_adapter,
        autotrader=autotrader,
    )

    if auto_start:
        await stack.start()

    return stack


__all__ = [
    "MAINNET_USER_STREAM_BASE_URL",
    "MAINNET_WS_TRADE_URL",
    "TESTNET_USER_STREAM_BASE_URL",
    "TESTNET_WS_TRADE_URL",
    "HeatmapUtaConfig",
    "HeatmapUtaStack",
    "LastTradeMarketData",
    "build_stack",
]
