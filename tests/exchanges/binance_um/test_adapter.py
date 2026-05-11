"""Tests for :class:`BinanceUmAdapter` (Phase 3a venue glue).

All tests use in-memory fakes for the inner REST / WS-trade / stream
clients; nothing reaches the network. The goal is to lock the
translation contract (``OrderRequest`` → Binance ``order.place``
params; Binance result → :class:`OrderAck`) and the lifecycle
semantics (start / close / idempotency / error gating).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from trade_adapter.exchanges.binance_um.adapter import (
    BinanceUmAdapter,
    BinanceUmAdapterClosed,
    BinanceUmAdapterConfig,
    BinanceUmAdapterNotStarted,
    BinanceUmAdapterWrongVenue,
)
from trade_adapter.exchanges.binance_um.symbols import SymbolRegistry
from trade_adapter.types import (
    OrderRequest,
    OrderSide,
    OrderType,
    TimeInForce,
    Venue,
)

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class FakeRest:
    """In-memory stub for :class:`BinanceRestClient`.

    Only ``fetch_exchange_info`` and ``aclose`` are exercised by the
    adapter at the Phase 3a level.
    """

    exchange_info_payload: dict[str, Any] = field(
        default_factory=lambda: _trivial_exchange_info()
    )
    exchange_info_calls: int = 0
    aclose_calls: int = 0

    async def fetch_exchange_info(self) -> dict[str, Any]:
        self.exchange_info_calls += 1
        return self.exchange_info_payload

    async def aclose(self) -> None:
        self.aclose_calls += 1


@dataclass
class _PlaceOrderCall:
    params: dict[str, Any]
    timeout: float | None


@dataclass
class _CancelOrderCall:
    params: dict[str, Any]
    timeout: float | None


@dataclass
class FakeWsTrade:
    """In-memory stub for :class:`BinanceWsTradeClient`.

    Records each ``place_order`` / ``cancel_order`` call and returns a
    canned response. Lifecycle methods (start / close) just bump
    counters so tests can assert ordering.
    """

    place_response: dict[str, Any] = field(default_factory=lambda: _stock_place_ack())
    cancel_response: dict[str, Any] = field(default_factory=dict)
    place_calls: list[_PlaceOrderCall] = field(default_factory=list)
    cancel_calls: list[_CancelOrderCall] = field(default_factory=list)
    start_calls: int = 0
    close_calls: int = 0
    place_exc: BaseException | None = None
    cancel_exc: BaseException | None = None

    async def start(self) -> None:
        self.start_calls += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def place_order(
        self,
        params: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        self.place_calls.append(_PlaceOrderCall(params=dict(params), timeout=timeout))
        if self.place_exc is not None:
            raise self.place_exc
        return dict(self.place_response)

    async def cancel_order(
        self,
        params: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        self.cancel_calls.append(
            _CancelOrderCall(params=dict(params), timeout=timeout)
        )
        if self.cancel_exc is not None:
            raise self.cancel_exc
        return dict(self.cancel_response)


@dataclass
class FakeStreamClient:
    """Stub for the optional user-stream / market-stream clients."""

    start_calls: int = 0
    close_calls: int = 0

    async def start(self) -> None:
        self.start_calls += 1

    async def close(self) -> None:
        self.close_calls += 1


def _trivial_exchange_info() -> dict[str, Any]:
    """A two-symbol ``exchangeInfo`` payload used by most adapter tests."""

    return {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "pricePrecision": 1,
                "quantityPrecision": 3,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {
                        "filterType": "LOT_SIZE",
                        "stepSize": "0.001",
                        "minQty": "0.001",
                        "maxQty": "10000",
                    },
                    {"filterType": "MIN_NOTIONAL", "notional": "100"},
                ],
            },
            {
                "symbol": "ETHUSDT",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {
                        "filterType": "LOT_SIZE",
                        "stepSize": "0.001",
                        "minQty": "0.001",
                        "maxQty": "10000",
                    },
                    {"filterType": "MIN_NOTIONAL", "notional": "20"},
                ],
            },
        ]
    }


def _stock_place_ack(
    *,
    order_id: int = 28,
    symbol: str = "BTCUSDT",
    client_order_id: str = "test-coid-1",
    update_time: int = 1700000001234,
) -> dict[str, Any]:
    return {
        "orderId": order_id,
        "symbol": symbol,
        "status": "NEW",
        "clientOrderId": client_order_id,
        "price": "0",
        "avgPrice": "0",
        "origQty": "0.001",
        "executedQty": "0",
        "cumQuote": "0",
        "timeInForce": "GTC",
        "type": "MARKET",
        "reduceOnly": False,
        "closePosition": False,
        "side": "BUY",
        "positionSide": "BOTH",
        "stopPrice": "0",
        "workingType": "CONTRACT_PRICE",
        "priceProtect": False,
        "origType": "MARKET",
        "updateTime": update_time,
    }


def _make_adapter(
    *,
    rest: FakeRest | None = None,
    ws_trade: FakeWsTrade | None = None,
    user_stream: FakeStreamClient | None = None,
    market_stream: FakeStreamClient | None = None,
    load_exchange_info: bool = True,
    symbols: SymbolRegistry | None = None,
) -> BinanceUmAdapter:
    return BinanceUmAdapter(
        rest=rest or FakeRest(),  # type: ignore[arg-type]
        ws_trade=ws_trade or FakeWsTrade(),  # type: ignore[arg-type]
        symbols=symbols if symbols is not None else SymbolRegistry(),
        user_stream=user_stream,  # type: ignore[arg-type]
        market_stream=market_stream,  # type: ignore[arg-type]
        config=BinanceUmAdapterConfig(
            load_exchange_info_on_start=load_exchange_info
        ),
    )


def _order_request(
    *,
    client_order_id: str = "test-coid-1",
    symbol: str = "BTCUSDT",
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.MARKET,
    qty: float = 0.001,
    price: float | None = None,
    stop_price: float | None = None,
    time_in_force: TimeInForce = TimeInForce.GTC,
    reduce_only: bool = False,
    close_position: bool = False,
    venue: Venue = Venue.BINANCE_UM,
    signal_id: str = "sig-1",
) -> OrderRequest:
    return OrderRequest(
        client_order_id=client_order_id,
        venue=venue,
        symbol=symbol,
        side=side,
        order_type=order_type,
        qty=qty,
        price=price,
        stop_price=stop_price,
        time_in_force=time_in_force,
        reduce_only=reduce_only,
        close_position=close_position,
        signal_id=signal_id,
    )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_loads_exchange_info_and_starts_components() -> None:
    rest = FakeRest()
    ws_trade = FakeWsTrade()
    user = FakeStreamClient()
    market = FakeStreamClient()
    adapter = _make_adapter(
        rest=rest, ws_trade=ws_trade, user_stream=user, market_stream=market
    )

    assert adapter.is_started is False
    await adapter.start()

    assert adapter.is_started is True
    assert rest.exchange_info_calls == 1
    assert ws_trade.start_calls == 1
    assert user.start_calls == 1
    assert market.start_calls == 1
    assert "BTCUSDT" in adapter.symbols
    assert "ETHUSDT" in adapter.symbols

    await adapter.close()


@pytest.mark.asyncio
async def test_start_is_idempotent() -> None:
    rest = FakeRest()
    ws_trade = FakeWsTrade()
    adapter = _make_adapter(rest=rest, ws_trade=ws_trade)

    await adapter.start()
    await adapter.start()
    await adapter.start()

    assert rest.exchange_info_calls == 1
    assert ws_trade.start_calls == 1

    await adapter.close()


@pytest.mark.asyncio
async def test_start_skips_exchange_info_when_disabled() -> None:
    rest = FakeRest()
    ws_trade = FakeWsTrade()
    adapter = _make_adapter(rest=rest, ws_trade=ws_trade, load_exchange_info=False)

    await adapter.start()

    assert rest.exchange_info_calls == 0
    assert ws_trade.start_calls == 1
    assert len(adapter.symbols) == 0

    await adapter.close()


@pytest.mark.asyncio
async def test_close_is_idempotent_and_closes_in_reverse_order() -> None:
    rest = FakeRest()
    ws_trade = FakeWsTrade()
    user = FakeStreamClient()
    market = FakeStreamClient()
    adapter = _make_adapter(
        rest=rest, ws_trade=ws_trade, user_stream=user, market_stream=market
    )
    await adapter.start()

    await adapter.close()
    await adapter.close()
    await adapter.close()

    assert adapter.is_closed is True
    assert adapter.is_started is False
    assert user.close_calls == 1
    assert market.close_calls == 1
    assert ws_trade.close_calls == 1
    assert rest.aclose_calls == 1


@pytest.mark.asyncio
async def test_close_swallows_component_exceptions() -> None:
    """One faulty close must not strand the rest."""

    rest = FakeRest()
    ws_trade = FakeWsTrade()
    user = FakeStreamClient()

    async def boom() -> None:
        raise RuntimeError("boom")

    user.close = boom  # type: ignore[assignment]
    adapter = _make_adapter(rest=rest, ws_trade=ws_trade, user_stream=user)
    await adapter.start()
    await adapter.close()

    assert ws_trade.close_calls == 1
    assert rest.aclose_calls == 1


@pytest.mark.asyncio
async def test_start_after_close_raises() -> None:
    adapter = _make_adapter()
    await adapter.start()
    await adapter.close()
    with pytest.raises(BinanceUmAdapterClosed):
        await adapter.start()


@pytest.mark.asyncio
async def test_submit_order_before_start_raises_not_started() -> None:
    adapter = _make_adapter()
    with pytest.raises(BinanceUmAdapterNotStarted):
        await adapter.submit_order(_order_request())


@pytest.mark.asyncio
async def test_cancel_order_before_start_raises_not_started() -> None:
    adapter = _make_adapter()
    with pytest.raises(BinanceUmAdapterNotStarted):
        await adapter.cancel_order("BTCUSDT", "coid")


@pytest.mark.asyncio
async def test_submit_order_after_close_raises_closed() -> None:
    adapter = _make_adapter()
    await adapter.start()
    await adapter.close()
    with pytest.raises(BinanceUmAdapterClosed):
        await adapter.submit_order(_order_request())


# ---------------------------------------------------------------------------
# submit_order translation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_market_order_builds_canonical_params() -> None:
    ws_trade = FakeWsTrade(
        place_response=_stock_place_ack(order_id=42, update_time=1700000005000)
    )
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()
    req = _order_request(qty=0.0015, side=OrderSide.SELL)

    ack = await adapter.submit_order(req)

    assert len(ws_trade.place_calls) == 1
    call = ws_trade.place_calls[0]
    assert call.params["symbol"] == "BTCUSDT"
    assert call.params["side"] == "SELL"
    assert call.params["type"] == "MARKET"
    assert call.params["newClientOrderId"] == "test-coid-1"
    # 0.0015 rounds down to 0.001 (stepSize=0.001).
    assert call.params["quantity"] == "0.001"
    assert "closePosition" not in call.params
    assert "reduceOnly" not in call.params
    assert "price" not in call.params
    assert "timeInForce" not in call.params
    assert "stopPrice" not in call.params

    assert ack.client_order_id == "test-coid-1"
    assert ack.exchange_order_id == "42"
    assert ack.symbol == "BTCUSDT"
    assert ack.venue is Venue.BINANCE_UM
    assert ack.accepted_at == pytest.approx(1700000005.0)

    await adapter.close()


@pytest.mark.asyncio
async def test_submit_limit_order_includes_price_and_tif() -> None:
    ws_trade = FakeWsTrade()
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()
    req = _order_request(
        order_type=OrderType.LIMIT,
        qty=0.002,
        price=65000.13,  # not on tick -> snaps to nearest 0.10
        time_in_force=TimeInForce.GTX,
    )

    await adapter.submit_order(req)

    call = ws_trade.place_calls[0]
    assert call.params["type"] == "LIMIT"
    assert call.params["quantity"] == "0.002"
    # 65000.13 rounds to nearest 0.10 -> 65000.1.
    assert call.params["price"] == "65000.1"
    assert call.params["timeInForce"] == "GTX"

    await adapter.close()


@pytest.mark.asyncio
async def test_submit_limit_order_without_price_raises() -> None:
    adapter = _make_adapter()
    await adapter.start()
    req = _order_request(order_type=OrderType.LIMIT, price=None)
    with pytest.raises(ValueError, match="LIMIT order requires price"):
        await adapter.submit_order(req)
    await adapter.close()


@pytest.mark.asyncio
async def test_submit_stop_market_includes_stop_price() -> None:
    ws_trade = FakeWsTrade()
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()
    req = _order_request(
        order_type=OrderType.STOP_MARKET,
        qty=0.005,
        stop_price=63000.07,
        reduce_only=True,
    )

    await adapter.submit_order(req)

    call = ws_trade.place_calls[0]
    assert call.params["type"] == "STOP_MARKET"
    assert call.params["stopPrice"] == "63000.1"
    assert call.params["reduceOnly"] == "true"
    assert "price" not in call.params

    await adapter.close()


@pytest.mark.asyncio
async def test_submit_take_profit_market_includes_stop_price() -> None:
    ws_trade = FakeWsTrade()
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()
    req = _order_request(
        order_type=OrderType.TAKE_PROFIT_MARKET,
        qty=0.005,
        stop_price=70000.0,
        reduce_only=True,
    )

    await adapter.submit_order(req)

    call = ws_trade.place_calls[0]
    assert call.params["type"] == "TAKE_PROFIT_MARKET"
    assert call.params["stopPrice"] == "70000"
    assert call.params["reduceOnly"] == "true"

    await adapter.close()


@pytest.mark.asyncio
async def test_submit_stop_without_stop_price_raises() -> None:
    adapter = _make_adapter()
    await adapter.start()
    req = _order_request(order_type=OrderType.STOP_MARKET, stop_price=None)
    with pytest.raises(ValueError, match="stop_price"):
        await adapter.submit_order(req)
    await adapter.close()


@pytest.mark.asyncio
async def test_submit_close_position_omits_quantity_and_reduce_only() -> None:
    """``closePosition=true`` is mutually exclusive with ``quantity``."""

    ws_trade = FakeWsTrade()
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()
    req = _order_request(
        order_type=OrderType.MARKET, close_position=True, reduce_only=True
    )

    await adapter.submit_order(req)

    call = ws_trade.place_calls[0]
    assert call.params["closePosition"] == "true"
    assert "quantity" not in call.params
    # reduceOnly suppressed when closePosition is set.
    assert "reduceOnly" not in call.params

    await adapter.close()


@pytest.mark.asyncio
async def test_submit_uses_submit_timeout_from_config() -> None:
    ws_trade = FakeWsTrade()
    adapter = BinanceUmAdapter(
        rest=FakeRest(),  # type: ignore[arg-type]
        ws_trade=ws_trade,  # type: ignore[arg-type]
        config=BinanceUmAdapterConfig(submit_timeout_s=7.5),
    )
    await adapter.start()
    await adapter.submit_order(_order_request())
    assert ws_trade.place_calls[0].timeout == 7.5

    # Per-call timeout overrides config default.
    await adapter.submit_order(_order_request(client_order_id="c-2"), timeout_s=2.5)
    assert ws_trade.place_calls[1].timeout == 2.5

    await adapter.close()


@pytest.mark.asyncio
async def test_submit_wrong_venue_raises() -> None:
    adapter = _make_adapter()
    await adapter.start()
    req = _order_request(venue=Venue.BYBIT_LINEAR)
    with pytest.raises(BinanceUmAdapterWrongVenue):
        await adapter.submit_order(req)
    await adapter.close()


@pytest.mark.asyncio
async def test_submit_propagates_ws_trade_errors() -> None:
    """Venue errors (rate limit, insufficient balance, etc.) propagate raw."""

    class _Boom(Exception):
        pass

    ws_trade = FakeWsTrade(place_exc=_Boom("rate limited"))
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()
    with pytest.raises(_Boom):
        await adapter.submit_order(_order_request())
    await adapter.close()


@pytest.mark.asyncio
async def test_submit_market_uses_market_lot_size_when_present() -> None:
    """Regression: MARKET orders snap to ``MARKET_LOT_SIZE``, not ``LOT_SIZE``.

    Binance validates MARKET-order qty against the ``MARKET_LOT_SIZE``
    filter, which is allowed to differ from ``LOT_SIZE``. If we round to
    ``LOT_SIZE`` instead, an otherwise-valid qty (e.g. 2.5 vs. step=1)
    is sent to the venue and rejected with ``-4014`` ``PRICE_FILTER``.
    """

    payload: dict[str, Any] = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "pricePrecision": 1,
                "quantityPrecision": 3,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {
                        "filterType": "LOT_SIZE",
                        "stepSize": "0.1",
                        "minQty": "0.1",
                        "maxQty": "10000",
                    },
                    {
                        "filterType": "MARKET_LOT_SIZE",
                        "stepSize": "1",
                        "minQty": "1",
                        "maxQty": "100",
                    },
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            },
        ]
    }
    rest = FakeRest(exchange_info_payload=payload)
    ws_trade = FakeWsTrade()
    adapter = _make_adapter(rest=rest, ws_trade=ws_trade)
    await adapter.start()

    # MARKET: 2.5 snaps to MARKET_LOT_SIZE step=1 -> "2".
    await adapter.submit_order(
        _order_request(qty=2.5, order_type=OrderType.MARKET)
    )
    # LIMIT: same 2.5 snaps to LOT_SIZE step=0.1 -> "2.5".
    await adapter.submit_order(
        _order_request(
            client_order_id="coid-limit",
            qty=2.5,
            order_type=OrderType.LIMIT,
            price=65000.0,
        )
    )

    market_call, limit_call = ws_trade.place_calls
    assert market_call.params["type"] == "MARKET"
    assert market_call.params["quantity"] == "2"
    assert limit_call.params["type"] == "LIMIT"
    assert limit_call.params["quantity"] == "2.5"

    await adapter.close()


@pytest.mark.asyncio
async def test_submit_market_falls_back_to_lot_size_when_market_filter_absent() -> None:
    """When ``MARKET_LOT_SIZE`` isn't present, MARKET orders fall back to ``LOT_SIZE``.

    The default fixture has no ``MARKET_LOT_SIZE`` filter, so ``round_market_qty``
    falls back to ``LOT_SIZE`` (stepSize=0.001) and 0.0015 → "0.001" as before.
    """

    ws_trade = FakeWsTrade()
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()

    await adapter.submit_order(
        _order_request(qty=0.0015, order_type=OrderType.MARKET)
    )

    assert ws_trade.place_calls[0].params["quantity"] == "0.001"

    await adapter.close()


@pytest.mark.asyncio
async def test_submit_without_symbol_in_registry_skips_rounding() -> None:
    """Unknown symbols still produce a usable params dict (no float-drift)."""

    ws_trade = FakeWsTrade()
    adapter = _make_adapter(ws_trade=ws_trade, load_exchange_info=False)
    await adapter.start()
    req = _order_request(symbol="UNKNOWN", qty=0.0000001)
    await adapter.submit_order(req)

    call = ws_trade.place_calls[0]
    # 0.0000001 must not be truncated to "0" and must not appear in
    # scientific notation either.
    assert call.params["quantity"] == "0.0000001"

    await adapter.close()


# ---------------------------------------------------------------------------
# OrderAck translation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_order_ack_falls_back_to_transact_time() -> None:
    """``transactTime`` is used when ``updateTime`` is missing."""

    response = _stock_place_ack()
    del response["updateTime"]
    response["transactTime"] = 1700000009999
    ws_trade = FakeWsTrade(place_response=response)
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()

    ack = await adapter.submit_order(_order_request())

    assert ack.accepted_at == pytest.approx(1700000009.999)

    await adapter.close()


@pytest.mark.asyncio
async def test_order_ack_handles_missing_timestamps() -> None:
    response = _stock_place_ack()
    del response["updateTime"]
    ws_trade = FakeWsTrade(place_response=response)
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()

    ack = await adapter.submit_order(_order_request())

    assert ack.accepted_at == 0.0

    await adapter.close()


@pytest.mark.asyncio
async def test_order_ack_uses_request_coid_when_missing() -> None:
    """A response without clientOrderId still produces a usable ack."""

    response = _stock_place_ack()
    del response["clientOrderId"]
    ws_trade = FakeWsTrade(place_response=response)
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()

    ack = await adapter.submit_order(_order_request(client_order_id="my-coid"))

    assert ack.client_order_id == "my-coid"

    await adapter.close()


@pytest.mark.asyncio
async def test_order_ack_missing_order_id_raises() -> None:
    """No ``orderId`` is a hard error — without it we can't track the order."""

    response = _stock_place_ack()
    del response["orderId"]
    ws_trade = FakeWsTrade(place_response=response)
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()

    with pytest.raises(ValueError, match="orderId"):
        await adapter.submit_order(_order_request())

    await adapter.close()


# ---------------------------------------------------------------------------
# cancel_order
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_order_sends_orig_client_order_id() -> None:
    ws_trade = FakeWsTrade()
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()

    await adapter.cancel_order("BTCUSDT", "my-coid")

    assert len(ws_trade.cancel_calls) == 1
    call = ws_trade.cancel_calls[0]
    assert call.params == {"symbol": "BTCUSDT", "origClientOrderId": "my-coid"}

    await adapter.close()


@pytest.mark.asyncio
async def test_cancel_uses_cancel_timeout_from_config() -> None:
    ws_trade = FakeWsTrade()
    adapter = BinanceUmAdapter(
        rest=FakeRest(),  # type: ignore[arg-type]
        ws_trade=ws_trade,  # type: ignore[arg-type]
        config=BinanceUmAdapterConfig(cancel_timeout_s=3.0),
    )
    await adapter.start()

    await adapter.cancel_order("BTCUSDT", "c-1")
    assert ws_trade.cancel_calls[0].timeout == 3.0

    await adapter.cancel_order("BTCUSDT", "c-2", timeout_s=1.0)
    assert ws_trade.cancel_calls[1].timeout == 1.0

    await adapter.close()


@pytest.mark.asyncio
async def test_cancel_propagates_ws_trade_errors() -> None:
    class _Boom(Exception):
        pass

    ws_trade = FakeWsTrade(cancel_exc=_Boom("unknown order"))
    adapter = _make_adapter(ws_trade=ws_trade)
    await adapter.start()
    with pytest.raises(_Boom):
        await adapter.cancel_order("BTCUSDT", "c-1")
    await adapter.close()
