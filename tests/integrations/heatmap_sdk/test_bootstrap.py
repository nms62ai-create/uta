"""Tests for :mod:`trade_adapter.integrations.heatmap_sdk.bootstrap`.

We don't speak to a real Binance from a unit test — the bootstrap is
verified through its ``_Builders`` injection hook, which lets us swap in
fake REST / WS-trade / user-stream / venue-adapter components while still
exercising the *wiring*:

* every UTA layer is constructed and held on the stack,
* :meth:`HeatmapUtaStack.start` / :meth:`stop` are idempotent,
* the consumer helpers delegate to the right downstream object,
* an end-to-end signal really flows from
  ``stack.on_adaptive_signal`` → :class:`SignalRouter` → the fake venue.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from trade_adapter.exchanges.binance_um.ws.trade import BinanceWsTradeClient
from trade_adapter.exchanges.binance_um.ws.transport import (
    WsRpcClient,
    WsRpcConfig,
)
from trade_adapter.exchanges.binance_um.ws.user_stream import UserDataStreamClient
from trade_adapter.integrations.heatmap_sdk.bootstrap import (
    MAINNET_USER_STREAM_BASE_URL,
    MAINNET_WS_TRADE_URL,
    TESTNET_USER_STREAM_BASE_URL,
    TESTNET_WS_TRADE_URL,
    HeatmapUtaConfig,
    HeatmapUtaStack,
    LastTradeMarketData,
    _Builders,
    build_stack,
)
from trade_adapter.integrations.heatmap_sdk.event_broadcaster import (
    HeatmapEventBroadcaster,
)
from trade_adapter.types import (
    OrderAck,
    OrderRequest,
    Venue,
)

from .test_autotrader import (
    BUY_EXHAUSTION,
    SELL_EXHAUSTION,
    _FakeAdaptiveSignal,
    _settings,
)

# ---------------------------------------------------------------------------
# Fakes for the venue stack
# ---------------------------------------------------------------------------


@dataclass
class _FakeRest:
    """Stand-in for :class:`BinanceRestClient` covering only the calls
    actually issued during :meth:`HeatmapUtaStack.start` /
    :meth:`HeatmapUtaStack.stop`.

    :class:`BinanceUmSnapshotProvider` calls ``fetch_positions`` +
    ``fetch_account`` on the first reconcile that fires from
    :meth:`PositionManager.start`; we return empty / minimal payloads.
    """

    base_url: str = "https://fake-rest.local"
    aclose_called: int = 0
    positions: list[dict[str, Any]] = field(default_factory=list)
    account: dict[str, Any] = field(
        default_factory=lambda: {"totalWalletBalance": "10000.00"}
    )
    listen_key_started: int = 0
    listen_key_closed: int = 0

    async def fetch_positions(self) -> list[dict[str, Any]]:
        return list(self.positions)

    async def fetch_account(self) -> dict[str, Any]:
        return dict(self.account)

    async def start_user_data_stream(self) -> str:
        self.listen_key_started += 1
        return "fake-listen-key"

    async def keepalive_user_data_stream(self) -> None:
        return None

    async def close_user_data_stream(self) -> None:
        self.listen_key_closed += 1

    async def aclose(self) -> None:
        self.aclose_called += 1


@dataclass
class _FakeWsRpc:
    config: WsRpcConfig
    started: int = 0
    closed: int = 0

    async def start(self) -> None:
        self.started += 1

    async def close(self) -> None:
        self.closed += 1


@dataclass
class _FakeWsTrade:
    rpc: _FakeWsRpc
    api_key: str
    api_secret: str
    started: int = 0
    closed: int = 0

    async def start(self) -> None:
        self.started += 1

    async def close(self) -> None:
        self.closed += 1


@dataclass
class _FakeUserStream:
    rest: _FakeRest
    handlers: dict[str, Any]
    started: int = 0
    closed: int = 0

    async def start(self) -> None:
        self.started += 1

    async def close(self) -> None:
        self.closed += 1


@dataclass
class _FakeExchange:
    """Replaces :class:`BinanceUmAdapter` end-to-end so we can assert
    on submitted :class:`OrderRequest` payloads without touching a
    real WS-trade client."""

    rest: _FakeRest
    ws_trade: _FakeWsTrade
    user_stream: _FakeUserStream
    venue: Venue = Venue.BINANCE_UM
    submitted: list[OrderRequest] = field(default_factory=list)
    canceled: list[tuple[str, str]] = field(default_factory=list)
    started: int = 0
    closed: int = 0

    async def start(self) -> None:
        # The real :class:`BinanceUmAdapter.start` would spin up the
        # underlying ws_rpc + ws_trade transports here. Our fake
        # transports are no-op, so just bump a counter.
        self.started += 1

    async def close(self) -> None:
        self.closed += 1

    async def submit_order(
        self, req: OrderRequest, *, timeout_s: float | None = None
    ) -> OrderAck:
        self.submitted.append(req)
        return OrderAck(
            client_order_id=req.client_order_id,
            exchange_order_id=f"ex-{len(self.submitted)}",
            venue=req.venue,
            symbol=req.symbol,
            accepted_at=1_700_000_000.0,
        )

    async def cancel_order(
        self,
        symbol: str,
        client_order_id: str,
        *,
        timeout_s: float | None = None,
    ) -> None:
        self.canceled.append((symbol, client_order_id))


def _fake_builders() -> _Builders:
    """Production builders swapped for in-memory fakes.

    Kept as a factory (not a module-level constant) so each test gets a
    fresh set of counters / submitted lists.
    """

    def b_rest(_config: HeatmapUtaConfig) -> Any:
        return _FakeRest()

    def b_ws_rpc(config: HeatmapUtaConfig) -> Any:
        return _FakeWsRpc(WsRpcConfig(url=config.resolved_ws_trade_url()))

    def b_ws_trade(config: HeatmapUtaConfig, rpc: Any) -> Any:
        return _FakeWsTrade(
            rpc=rpc, api_key=config.api_key, api_secret=config.api_secret
        )

    def b_user_stream(
        _config: HeatmapUtaConfig, rest: Any, handlers: dict[str, Any]
    ) -> Any:
        return _FakeUserStream(rest=rest, handlers=handlers)

    def b_exchange(
        _config: HeatmapUtaConfig, rest: Any, ws_trade: Any, user_stream: Any
    ) -> Any:
        return _FakeExchange(rest=rest, ws_trade=ws_trade, user_stream=user_stream)

    def b_dao(_config: HeatmapUtaConfig) -> Any:
        # The real DAO is fine — opens an in-memory SQLite, no I/O outside
        # the test process. Tests get their own ``dao`` fixture, but
        # build_stack creates the DAO itself; we let the real type through.
        from trade_adapter.storage.sqlite import SqliteDAO

        return SqliteDAO(":memory:")

    return _Builders(
        rest=b_rest,
        ws_rpc=b_ws_rpc,
        ws_trade=b_ws_trade,
        user_stream=b_user_stream,
        exchange=b_exchange,
        dao=b_dao,
    )


def _config(
    *,
    api_key: str = "test-key",
    api_secret: str = "test-secret",
    testnet: bool = True,
) -> HeatmapUtaConfig:
    return HeatmapUtaConfig(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        reconcile_interval_s=0.0,
        load_exchange_info_on_start=False,
    )


# ---------------------------------------------------------------------------
# LastTradeMarketData
# ---------------------------------------------------------------------------


def test_market_data_returns_none_when_unset() -> None:
    md = LastTradeMarketData()
    assert md.get_reference_price(Venue.BINANCE_UM, "BTCUSDT") is None


def test_market_data_returns_last_price() -> None:
    md = LastTradeMarketData()
    md.update(Venue.BINANCE_UM, "BTCUSDT", 50_000.0)
    assert md.get_reference_price(Venue.BINANCE_UM, "BTCUSDT") == 50_000.0


def test_market_data_symbol_is_case_insensitive() -> None:
    md = LastTradeMarketData()
    md.update(Venue.BINANCE_UM, "btcusdt", 1.0)
    assert md.get_reference_price(Venue.BINANCE_UM, "BTCUSDT") == 1.0
    assert md.get_reference_price(Venue.BINANCE_UM, "btcusdt") == 1.0


def test_market_data_rejects_non_positive_price() -> None:
    md = LastTradeMarketData()
    with pytest.raises(ValueError, match="positive finite"):
        md.update(Venue.BINANCE_UM, "BTCUSDT", 0.0)
    with pytest.raises(ValueError, match="positive finite"):
        md.update(Venue.BINANCE_UM, "BTCUSDT", -1.0)


def test_market_data_rejects_non_finite_price() -> None:
    md = LastTradeMarketData()
    with pytest.raises(ValueError, match="positive finite"):
        md.update(Venue.BINANCE_UM, "BTCUSDT", float("inf"))


# ---------------------------------------------------------------------------
# HeatmapUtaConfig URL resolution
# ---------------------------------------------------------------------------


def test_config_uses_testnet_urls_by_default() -> None:
    c = _config(testnet=True)
    assert c.resolved_ws_trade_url() == TESTNET_WS_TRADE_URL
    assert c.resolved_user_stream_base_url() == TESTNET_USER_STREAM_BASE_URL
    assert "testnet" in c.resolved_rest_base_url()


def test_config_uses_mainnet_urls_when_testnet_false() -> None:
    c = _config(testnet=False)
    assert c.resolved_ws_trade_url() == MAINNET_WS_TRADE_URL
    assert c.resolved_user_stream_base_url() == MAINNET_USER_STREAM_BASE_URL


def test_config_overrides_take_precedence() -> None:
    c = HeatmapUtaConfig(
        api_key="k",
        api_secret="s",
        testnet=True,
        rest_base_url="http://localhost:9999",
        ws_trade_url="ws://localhost:9999",
        user_stream_base_url="ws://localhost:9998",
    )
    assert c.resolved_rest_base_url() == "http://localhost:9999"
    assert c.resolved_ws_trade_url() == "ws://localhost:9999"
    assert c.resolved_user_stream_base_url() == "ws://localhost:9998"


# ---------------------------------------------------------------------------
# build_stack: assembly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_stack_assembles_every_component() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=False
    )
    try:
        assert isinstance(stack, HeatmapUtaStack)
        # Core infra
        assert stack.bus is not None
        assert stack.store is not None
        assert isinstance(stack.market_data, LastTradeMarketData)
        assert stack.risk_state is not None
        assert stack.risk_gate is not None
        assert stack.idempotency is not None
        # Venue transports
        assert isinstance(stack.rest, _FakeRest)
        assert isinstance(stack.ws_rpc, _FakeWsRpc)
        assert isinstance(stack.ws_trade, _FakeWsTrade)
        assert isinstance(stack.user_stream, _FakeUserStream)
        # Higher layers
        assert isinstance(stack.exchange, _FakeExchange)
        assert stack.position_manager is not None
        assert stack.signal_router is not None
        assert stack.trade_adapter is not None
        assert stack.autotrader is not None
        # Broadcaster is opt-in
        assert stack.broadcaster is None
        # Lifecycle flag
        assert stack.is_started is False
        assert stack.is_stopped is False
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_build_stack_wires_user_stream_handlers_for_order_and_account_events() -> (
    None
):
    """USER_DATA_STREAM must be set up with at least the ``ORDER_TRADE_UPDATE``
    and ``ACCOUNT_UPDATE`` handlers so positions reconcile on fills."""

    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=False
    )
    try:
        handlers = stack.user_stream.handlers  # type: ignore[attr-defined]
        assert "ORDER_TRADE_UPDATE" in handlers
        assert "ACCOUNT_UPDATE" in handlers
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_build_stack_threads_api_credentials_to_ws_trade() -> None:
    stack = await build_stack(
        _config(api_key="abc", api_secret="xyz"),
        builders=_fake_builders(),
        auto_start=False,
    )
    try:
        assert stack.ws_trade.api_key == "abc"  # type: ignore[attr-defined]
        assert stack.ws_trade.api_secret == "xyz"  # type: ignore[attr-defined]
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_build_stack_default_autotrader_starts_without_settings() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=False
    )
    try:
        assert stack.autotrader.settings is None
        assert stack.autotrader.enabled is False
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_build_stack_accepts_initial_autotrade_settings() -> None:
    settings = _settings(enabled=True)
    config = HeatmapUtaConfig(
        api_key="k",
        api_secret="s",
        testnet=True,
        autotrade_settings=settings,
        reconcile_interval_s=0.0,
        load_exchange_info_on_start=False,
    )
    stack = await build_stack(
        config, builders=_fake_builders(), auto_start=False
    )
    try:
        assert stack.autotrader.settings == settings
        assert stack.autotrader.enabled is True
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_build_stack_auto_start_true_starts_lifecycle() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=True
    )
    try:
        assert stack.is_started is True
    finally:
        await stack.stop()


# ---------------------------------------------------------------------------
# Lifecycle (start / stop / idempotency)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_is_idempotent() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=False
    )
    try:
        await stack.start()
        await stack.start()  # no-op
        assert stack.is_started is True
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_stop_is_idempotent() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=True
    )
    await stack.stop()
    await stack.stop()  # no-op
    assert stack.is_stopped is True


@pytest.mark.asyncio
async def test_stop_calls_rest_aclose() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=True
    )
    rest: _FakeRest = stack.rest  # type: ignore[assignment]
    await stack.stop()
    assert rest.aclose_called == 1


@pytest.mark.asyncio
async def test_stop_does_not_raise_when_component_close_throws() -> None:
    """A broken close() in one component must not strand the others."""

    class _ExplodingRest(_FakeRest):
        async def aclose(self) -> None:
            raise RuntimeError("boom")

    builders = _fake_builders()
    orig_rest = builders.rest

    def b_rest(config: HeatmapUtaConfig) -> Any:
        return _ExplodingRest()

    safe_builders = _Builders(
        rest=b_rest,
        ws_rpc=builders.ws_rpc,
        ws_trade=builders.ws_trade,
        user_stream=builders.user_stream,
        exchange=builders.exchange,
        dao=builders.dao,
    )
    _ = orig_rest  # silence unused; we deliberately swapped this one
    stack = await build_stack(
        _config(), builders=safe_builders, auto_start=True
    )
    # Should swallow the rest.aclose RuntimeError, not propagate.
    await stack.stop()
    assert stack.is_stopped is True


# ---------------------------------------------------------------------------
# Consumer helpers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_settings_delegates_to_autotrader() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=False
    )
    try:
        s = _settings(enabled=False)
        stack.update_settings(s)
        assert stack.autotrader.settings == s
        stack.enable()
        assert stack.autotrader.enabled is True
        stack.disable()
        assert stack.autotrader.enabled is False
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_on_market_trade_records_price_for_sizing() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=False
    )
    try:
        stack.on_market_trade("BTCUSDT", 49_999.5)
        assert (
            stack.market_data.get_reference_price(Venue.BINANCE_UM, "BTCUSDT")
            == 49_999.5
        )
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_attach_browser_sender_starts_broadcaster() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=True
    )
    try:
        sent: list[dict[str, Any]] = []

        async def sink(payload: dict[str, Any]) -> None:
            sent.append(payload)

        broadcaster = stack.attach_browser_sender(sink)
        assert isinstance(broadcaster, HeatmapEventBroadcaster)
        assert broadcaster.is_running
        assert stack.broadcaster is broadcaster
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_attach_browser_sender_raises_on_double_call() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=True
    )
    try:

        async def sink(_payload: dict[str, Any]) -> None:
            return None

        stack.attach_browser_sender(sink)
        with pytest.raises(RuntimeError, match="already called"):
            stack.attach_browser_sender(sink)
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_stop_also_stops_broadcaster() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=True
    )

    async def sink(_payload: dict[str, Any]) -> None:
        return None

    broadcaster = stack.attach_browser_sender(sink)
    assert broadcaster.is_running

    await stack.stop()
    assert broadcaster.is_running is False


@pytest.mark.asyncio
async def test_trip_and_reset_kill_switch_delegate_to_trade_adapter() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=True
    )
    try:
        stack.trip_kill_switch("manual abort")
        assert stack.trade_adapter.kill_switch_tripped is True
        stack.reset_kill_switch()
        assert stack.trade_adapter.kill_switch_tripped is False
    finally:
        await stack.stop()


# ---------------------------------------------------------------------------
# End-to-end signal flow through the stack
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_adaptive_signal_routes_through_to_fake_exchange() -> None:
    """A SELL_EXHAUSTION signal must surface as a LONG entry order on
    the fake venue adapter once autotrade is enabled and we've fed at
    least one market trade for sizing."""

    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=True
    )
    try:
        stack.update_settings(_settings(enabled=True))
        stack.on_market_trade("BTCUSDT", 50_000.0)

        signal = _FakeAdaptiveSignal(
            signal_id="sig-bootstrap-1",
            symbol="BTCUSDT",
            timestamp=1_700_000_000.0,
            exhaustion_type=SELL_EXHAUSTION,
            confidence=0.9,
        )
        ack = await stack.on_adaptive_signal(signal)
        assert ack is not None
        assert ack.signal_id == "sig-bootstrap-1"

        # Allow the event-bus subscription tasks (if any) to settle.
        await asyncio.sleep(0)

        exchange: _FakeExchange = stack.exchange  # type: ignore[assignment]
        # Entry + optional SL/TP — at minimum 1 submitted order.
        assert len(exchange.submitted) >= 1
        entry = exchange.submitted[0]
        assert entry.symbol == "BTCUSDT"
        assert entry.venue == Venue.BINANCE_UM
    finally:
        await stack.stop()


@pytest.mark.asyncio
async def test_disabled_autotrader_does_not_emit_orders() -> None:
    stack = await build_stack(
        _config(), builders=_fake_builders(), auto_start=True
    )
    try:
        # Settings configured but autotrade not enabled.
        stack.update_settings(_settings(enabled=False))
        stack.on_market_trade("BTCUSDT", 50_000.0)

        signal = _FakeAdaptiveSignal(
            signal_id="sig-disabled",
            symbol="BTCUSDT",
            timestamp=1_700_000_000.0,
            exhaustion_type=BUY_EXHAUSTION,
            confidence=0.99,
        )
        ack = await stack.on_adaptive_signal(signal)
        assert ack is None

        exchange: _FakeExchange = stack.exchange  # type: ignore[assignment]
        assert exchange.submitted == []
    finally:
        await stack.stop()


# ---------------------------------------------------------------------------
# Real-builder smoke: ensure the *default* builders construct objects of
# the right type without throwing (they do not call any I/O during
# construction, only during start()).
# ---------------------------------------------------------------------------


def test_default_builders_produce_real_types() -> None:
    """Sanity check that default builders compile against current
    UTA constructors. We never call .start() here, so no network."""

    from trade_adapter.exchanges.binance_um import (
        BinanceRestClient,
        BinanceUmAdapter,
    )
    from trade_adapter.integrations.heatmap_sdk.bootstrap import (
        _DEFAULT_BUILDERS,
    )

    config = _config()
    rest = _DEFAULT_BUILDERS.rest(config)
    assert isinstance(rest, BinanceRestClient)
    ws_rpc = _DEFAULT_BUILDERS.ws_rpc(config)
    assert isinstance(ws_rpc, WsRpcClient)
    ws_trade = _DEFAULT_BUILDERS.ws_trade(config, ws_rpc)
    assert isinstance(ws_trade, BinanceWsTradeClient)
    user_stream = _DEFAULT_BUILDERS.user_stream(config, rest, {})
    assert isinstance(user_stream, UserDataStreamClient)
    exchange = _DEFAULT_BUILDERS.exchange(config, rest, ws_trade, user_stream)
    assert isinstance(exchange, BinanceUmAdapter)
