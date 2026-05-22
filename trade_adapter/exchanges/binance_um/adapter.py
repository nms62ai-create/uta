"""Phase 3a Binance USD-M Futures venue adapter.

Wires the REST + WS-trade + market-stream + user-data-stream clients
into a single lifecycle-owning object that the signal router (Phase 3d)
talks to. This is the topmost Binance-aware layer; signal_router uses
the UTA-internal types (``OrderRequest`` / ``OrderAck``) and delegates
all Binance specifics here.

What this layer translates
==========================

* ``OrderRequest`` → Binance ``order.place`` params (``symbol``, ``side``,
  ``type``, ``quantity`` rendered to ``stepSize``, ``price`` rendered to
  ``tickSize``, ``newClientOrderId``, ``reduceOnly`` / ``closePosition``
  flags, ``timeInForce`` for LIMIT, ``stopPrice`` for STOP_MARKET /
  TAKE_PROFIT_MARKET).
* Binance ``order.place`` result → :class:`OrderAck` (``clientOrderId``,
  ``orderId`` stringified as ``exchange_order_id``, ``symbol``,
  ``accepted_at`` derived from ``updateTime`` / ``transactTime`` in ms).
* ``cancel``: ``(symbol, client_order_id)`` → cancel params; the
  result is discarded (success-or-raise).

What this layer does NOT do (deferred)
=======================================

* Translate Binance market-stream frames / user-stream events into UTA
  event types (Phase 3b — translators).
* Build :class:`OrderRequest` from :class:`UniversalSignal` (Phase 3c —
  sizing & stop calculators; Phase 3d — signal_router).
* Maintain a position cache or reconcile against the venue
  (Phase 3e).
* Apply risk gates (Phase 3f).
* Expose the public ``TradeAdapter`` API used by external consumers
  (Phase 3g).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ...types import (
    OrderAck,
    OrderRequest,
    OrderType,
    Venue,
)
from .rest import BinanceRestClient
from .symbols import SymbolRegistry
from .ws.market import MarketStreamClient
from .ws.trade import BinanceWsTradeClient
from .ws.user_stream import UserDataStreamClient

logger = logging.getLogger(__name__)


class BinanceUmAdapterError(Exception):
    """Base class for adapter-level errors."""


class BinanceUmAdapterClosed(BinanceUmAdapterError):
    """Operation attempted on a closed adapter."""


class BinanceUmAdapterNotStarted(BinanceUmAdapterError):
    """Operation attempted on an adapter that hasn't been started."""


class BinanceUmAdapterWrongVenue(BinanceUmAdapterError):
    """An :class:`OrderRequest` for a different venue was submitted here."""


@dataclass(frozen=True, slots=True)
class BinanceUmAdapterConfig:
    """Tunables for :class:`BinanceUmAdapter`.

    Defaults are conservative — production callers can override via
    constructor. ``load_exchange_info_on_start`` defaults to ``True`` so
    the adapter ships with rounding capabilities out of the box; tests
    that don't care about rounding can set it to ``False`` to skip the
    REST call entirely.
    """

    submit_timeout_s: float = 10.0
    cancel_timeout_s: float = 5.0
    load_exchange_info_on_start: bool = True


# Maps the UTA ``OrderType`` enum to the Binance ``type`` string.
# Only the four canonical types are supported in v1 — Binance has more
# (TRAILING_STOP_MARKET, LIQUIDATION, etc.) but they're out of scope.
_ORDER_TYPE_MAP: dict[OrderType, str] = {
    OrderType.MARKET: "MARKET",
    OrderType.LIMIT: "LIMIT",
    OrderType.STOP_MARKET: "STOP_MARKET",
    OrderType.TAKE_PROFIT_MARKET: "TAKE_PROFIT_MARKET",
}


def _decimal_to_plain(d: Decimal) -> str:
    """Render a ``Decimal`` in fixed-point form without scientific notation.

    Same algorithm as :func:`...auth._decimal_to_plain_string`. Kept
    local to the adapter so wire-formatting helpers live next to the
    layer that uses them. ``Decimal('1E-8')`` → ``"0.00000001"``,
    ``Decimal('1.000')`` → ``"1"``, ``Decimal('0.5')`` → ``"0.5"``.
    """

    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _format_qty_for_wire(
    symbols: SymbolRegistry,
    symbol: str,
    qty: float,
    *,
    order_type: OrderType,
) -> str:
    """Round ``qty`` to the correct step filter and render as a Binance string.

    Binance validates MARKET orders against the ``MARKET_LOT_SIZE`` filter,
    which can have a different ``stepSize`` than ``LOT_SIZE``. The other
    order types (LIMIT / STOP_MARKET / TAKE_PROFIT_MARKET) are validated
    against ``LOT_SIZE``. ``SymbolRegistry.round_market_qty`` already falls
    back to ``LOT_SIZE`` when ``MARKET_LOT_SIZE`` is absent.

    If the symbol isn't in the registry (e.g. ``load_exchange_info_on_start``
    was disabled in a test), we skip rounding and just stringify via the
    shortest-float repr → ``Decimal`` round-trip so we never emit
    scientific notation or float-drift garbage.
    """

    if symbol in symbols:
        if order_type is OrderType.MARKET:
            return _decimal_to_plain(symbols.round_market_qty(symbol, qty))
        return _decimal_to_plain(symbols.round_qty(symbol, qty))
    return _decimal_to_plain(Decimal(repr(qty)))


def _format_price_for_wire(
    symbols: SymbolRegistry, symbol: str, price: float
) -> str:
    """Round ``price`` to ``tickSize`` and render as a Binance-canonical string."""

    if symbol in symbols:
        return _decimal_to_plain(symbols.round_price(symbol, price))
    return _decimal_to_plain(Decimal(repr(price)))


@dataclass(slots=True)
class BinanceUmAdapter:
    """Lifecycle owner for the Binance USD-M Futures venue.

    Construction is **explicit dependency injection**: the caller hands
    in pre-built ``rest`` / ``ws_trade`` / optional ``user_stream`` /
    optional ``market_stream`` instances. This keeps the adapter
    testable (pass mocks) and forces production callers to be explicit
    about which transport layers they actually need.

    Lifecycle
    ---------
    * :meth:`start` — loads ``exchangeInfo`` into :attr:`symbols`, then
      starts the WS-trade transport, then any optional user-stream and
      market-stream clients. Subsequent calls are no-ops (idempotent).
    * :meth:`close` — closes all components in reverse order, swallowing
      and logging any exceptions so a single bad close doesn't strand
      the others.

    Concurrency
    -----------
    :meth:`submit_order` and :meth:`cancel_order` are coroutine-safe as
    long as the underlying ``ws_trade`` is — :class:`WsRpcClient` already
    correlates by request id so concurrent calls are fine.

    Errors
    ------
    * :class:`BinanceUmAdapterNotStarted` — operation before :meth:`start`.
    * :class:`BinanceUmAdapterClosed` — operation after :meth:`close`.
    * :class:`BinanceUmAdapterWrongVenue` — order for non-BINANCE_UM venue.
    * Binance-level errors (:class:`BinanceWsApiError`,
      :class:`BinanceHttpError`, etc.) propagate unchanged so the upper
      layer can translate them into ``OutcomeReport`` / ``AlertEvent``
      with full fidelity.
    """

    rest: BinanceRestClient
    ws_trade: BinanceWsTradeClient
    symbols: SymbolRegistry = field(default_factory=SymbolRegistry)
    user_stream: UserDataStreamClient | None = None
    market_stream: MarketStreamClient | None = None
    config: BinanceUmAdapterConfig = field(default_factory=BinanceUmAdapterConfig)

    _started: bool = field(default=False, init=False)
    _closed: bool = field(default=False, init=False)
    _start_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    @property
    def is_started(self) -> bool:
        return self._started and not self._closed

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def start(self) -> None:
        """Bring the adapter online. Idempotent.

        Order of operations matters: ``exchangeInfo`` first (REST, so
        we have rounding before any signed call), then WS-trade (the
        signed-RPC socket), then optional push-only streams. If any
        single step raises, we don't try to roll back partial state —
        the caller should :meth:`close` and rebuild.
        """

        if self._closed:
            raise BinanceUmAdapterClosed("adapter is closed")
        async with self._start_lock:
            if self._started:
                return
            if self.config.load_exchange_info_on_start:
                payload = await self.rest.fetch_exchange_info()
                count = self.symbols.load_from_exchange_info(payload)
                logger.info("binance-um adapter loaded %d symbols", count)
            await self.ws_trade.start()
            if self.user_stream is not None:
                await self.user_stream.start()
            if self.market_stream is not None:
                await self.market_stream.start()
            self._started = True
            logger.info("binance-um adapter started")

    async def close(self) -> None:
        """Tear down all owned components. Idempotent.

        We close in *reverse* order of :meth:`start`: streams first
        (so we stop receiving frames), then WS-trade, then REST. Each
        close is best-effort — exceptions are logged but never
        re-raised so a single faulty close doesn't strand the others.
        """

        if self._closed:
            return
        self._closed = True
        components: list[tuple[str, Any]] = [
            ("user_stream", self.user_stream),
            ("market_stream", self.market_stream),
            ("ws_trade", self.ws_trade),
            ("rest", self.rest),
        ]
        for name, component in components:
            if component is None:
                continue
            close_fn = getattr(component, "close", None) or getattr(
                component, "aclose", None
            )
            if close_fn is None:
                continue
            try:
                await close_fn()
            except Exception as e:
                logger.warning("binance-um adapter error closing %s: %s", name, e)
        logger.info("binance-um adapter closed")

    async def submit_order(
        self, req: OrderRequest, *, timeout_s: float | None = None
    ) -> OrderAck:
        """Translate ``req`` into a Binance ``order.place`` and return the ack.

        Quantity is rounded to ``stepSize`` and price to ``tickSize``
        from the :class:`SymbolRegistry` loaded at :meth:`start` time.
        Binance error envelopes propagate as
        :class:`...ws.trade.BinanceWsApiError` so the caller can
        distinguish ``-2010 INSUFFICIENT_BALANCE`` from
        ``-1100 ILLEGAL_CHARS`` etc.
        """

        self._ensure_running()
        if req.venue is not Venue.BINANCE_UM:
            raise BinanceUmAdapterWrongVenue(
                f"this adapter only handles {Venue.BINANCE_UM}, got {req.venue}"
            )
        params = self._build_place_order_params(req)
        timeout = (
            timeout_s if timeout_s is not None else self.config.submit_timeout_s
        )
        result = await self.ws_trade.place_order(params, timeout=timeout)
        return self._build_order_ack(req, result)

    async def cancel_order(
        self,
        symbol: str,
        client_order_id: str,
        *,
        timeout_s: float | None = None,
    ) -> None:
        """Cancel ``client_order_id`` on ``symbol``.

        Identifies the order by the client-side ID — that's the only
        thing we know synchronously after :meth:`submit_order` returns.
        The Binance ``orderId`` would also work via
        :class:`BinanceWsTradeClient.cancel_order` but we don't always
        have it before the cancel happens (e.g. local TTL expiry).

        Returns ``None`` on success. Raises ``BinanceWsApiError`` if
        the venue rejects the cancel (e.g. ``-2011 UNKNOWN_ORDER``).
        """

        self._ensure_running()
        params: dict[str, Any] = {
            "symbol": symbol,
            "origClientOrderId": client_order_id,
        }
        timeout = (
            timeout_s if timeout_s is not None else self.config.cancel_timeout_s
        )
        await self.ws_trade.cancel_order(params, timeout=timeout)

    def _ensure_running(self) -> None:
        if self._closed:
            raise BinanceUmAdapterClosed("adapter is closed")
        if not self._started:
            raise BinanceUmAdapterNotStarted(
                "adapter not started; call start() first"
            )

    def _build_place_order_params(self, req: OrderRequest) -> dict[str, Any]:
        """Map an :class:`OrderRequest` to Binance ``order.place`` params.

        Numeric fields (``quantity`` / ``price`` / ``stopPrice``) are
        pre-rendered as canonical strings so the wire form matches the
        signed canonical form exactly — we don't rely on Binance's
        float-rendering convention matching ours.
        """

        try:
            binance_type = _ORDER_TYPE_MAP[req.order_type]
        except KeyError as e:
            raise ValueError(
                f"unsupported order type for binance_um: {req.order_type}"
            ) from e

        params: dict[str, Any] = {
            "symbol": req.symbol,
            "side": str(req.side),
            "type": binance_type,
            "newClientOrderId": req.client_order_id,
        }

        # closePosition is mutually exclusive with quantity — Binance
        # interprets it as "close the entire position on this symbol",
        # so we must omit quantity entirely when it's set.
        if req.close_position:
            params["closePosition"] = "true"
        else:
            params["quantity"] = _format_qty_for_wire(
                self.symbols,
                req.symbol,
                req.qty,
                order_type=req.order_type,
            )
            if req.reduce_only:
                params["reduceOnly"] = "true"

        if req.order_type is OrderType.LIMIT:
            if req.price is None:
                raise ValueError("LIMIT order requires price")
            params["price"] = _format_price_for_wire(
                self.symbols, req.symbol, req.price
            )
            params["timeInForce"] = str(req.time_in_force)

        if req.order_type in (
            OrderType.STOP_MARKET,
            OrderType.TAKE_PROFIT_MARKET,
        ):
            if req.stop_price is None:
                raise ValueError(
                    f"{req.order_type} order requires stop_price"
                )
            params["stopPrice"] = _format_price_for_wire(
                self.symbols, req.symbol, req.stop_price
            )

        return params

    @staticmethod
    def _build_order_ack(req: OrderRequest, result: dict[str, Any]) -> OrderAck:
        """Build an :class:`OrderAck` from Binance's ``order.place`` result.

        Binance returns ``updateTime`` (ms) on most successful responses
        and ``transactTime`` on a few; we accept either. Missing ts
        falls back to ``0.0`` rather than raising — a partial ack with
        an unknown timestamp is still a usable ack.
        """

        client_order_id = str(result.get("clientOrderId") or req.client_order_id)
        raw_exchange_id = result.get("orderId")
        if raw_exchange_id is None:
            raise ValueError(
                f"binance order.place result missing 'orderId': {result!r}"
            )
        exchange_order_id = str(raw_exchange_id)
        symbol = str(result.get("symbol") or req.symbol)
        raw_ts = result.get("updateTime") or result.get("transactTime") or 0
        try:
            accepted_at = float(raw_ts) / 1000.0
        except (TypeError, ValueError):
            accepted_at = 0.0
        return OrderAck(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            venue=Venue.BINANCE_UM,
            symbol=symbol,
            accepted_at=accepted_at,
        )


__all__ = [
    "BinanceUmAdapter",
    "BinanceUmAdapterClosed",
    "BinanceUmAdapterConfig",
    "BinanceUmAdapterError",
    "BinanceUmAdapterNotStarted",
    "BinanceUmAdapterWrongVenue",
]
