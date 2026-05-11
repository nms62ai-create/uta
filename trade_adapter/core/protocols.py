"""Protocol interfaces consumed by :mod:`trade_adapter.core.signal_router`.

Defined here so the router can be unit-tested with in-memory fakes
that don't import any venue-specific code, and so :mod:`position_manager`
(Phase 3e), :mod:`risk` (Phase 3f), and the embedded :class:`TradeAdapter`
API (Phase 3g) can be plugged in without circular imports.

These Protocols are :class:`typing.Protocol` (structural) — the existing
:class:`BinanceUmAdapter` and :class:`IdempotencyCache` already satisfy
them by shape without any explicit subclassing.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from ..types import OrderAck, OrderRequest, Position, Venue


@runtime_checkable
class ExchangeAdapter(Protocol):
    """Venue-side surface that :class:`SignalRouter` needs.

    Satisfied structurally by
    :class:`trade_adapter.exchanges.binance_um.adapter.BinanceUmAdapter`
    (Phase 3a) and by the upcoming Bybit Linear adapter (Phase 4).
    """

    async def submit_order(
        self, req: OrderRequest, *, timeout_s: float | None = None
    ) -> OrderAck: ...

    async def cancel_order(
        self,
        symbol: str,
        client_order_id: str,
        *,
        timeout_s: float | None = None,
    ) -> None: ...


@runtime_checkable
class PositionProvider(Protocol):
    """Read-only view of current positions per ``(venue, symbol)``.

    The :class:`position_manager` (Phase 3e) implements this against
    live state; tests use :class:`NullPositionProvider` to model an
    account that has never traded.
    """

    def get_position(self, venue: Venue, symbol: str) -> Position | None: ...


@runtime_checkable
class MarketDataProvider(Protocol):
    """Cached BBO mid / mark price for sizing and stop math.

    Populated by the venue stream client (Phase 2c-2). For signals
    arriving before the first BBO frame, returns ``None`` and the
    router rejects with :class:`RejectionReason.UNKNOWN_SYMBOL`.
    """

    def get_reference_price(self, venue: Venue, symbol: str) -> float | None: ...


@runtime_checkable
class EquityProvider(Protocol):
    """Total equity per venue, in USD.

    Required for :class:`PctEquity` sizing. The position_manager (Phase 3e)
    populates this from REST account snapshots and ``ACCOUNT_UPDATE``
    events. Returns ``None`` until the first snapshot has arrived.
    """

    def get_equity_usd(self, venue: Venue) -> float | None: ...


class NullPositionProvider:
    """No-op :class:`PositionProvider` that always returns ``None``.

    Useful for tests and for the embedded API before the position
    manager (Phase 3e) is wired in.
    """

    def get_position(self, venue: Venue, symbol: str) -> Position | None:
        return None


__all__ = [
    "EquityProvider",
    "ExchangeAdapter",
    "MarketDataProvider",
    "NullPositionProvider",
    "PositionProvider",
]
