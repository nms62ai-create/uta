"""Auto-subscribe BBO for every open position.

The :class:`BboTracker` listens to :class:`PositionUpdate` events on the
embedded :class:`EventBus`, opens a :class:`MarketDataHub` BBO
subscription for every ``(venue, symbol)`` that crosses into ``qty > 0``,
and closes it when the position transitions back to flat. The collected
tick-by-tick envelope (``min_bid``, ``max_ask``, first / last mid) is
stored per position so the upcoming ``OutcomeReport`` emitter (A.2) can
compute MFE / MAE without rescanning the tape.

Design notes:

* Pure consumer-side helper. Never publishes. Tracker is optional —
  :class:`TradeAdapter` constructs one only if a hub is wired.
* One background task per tracker reads ``POSITION_UPDATE`` events and
  decides subscribe / unsubscribe. Subscribed streams produce
  :class:`BBOUpdate` events which a second task per ``(venue, symbol)``
  consumes and folds into :class:`PositionBboSnapshot`.
* Reconnect-safe: if the upstream BBO stream churns we keep the
  subscription handle alive; min/max trackers tolerate small gaps.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from ..bus.event_bus import EventBus, Subscription
from ..types import (
    BBOUpdate,
    EventType,
    PositionState,
    PositionUpdate,
    Venue,
)
from .hub import MarketDataHub, MarketDataSubscription

_log = logging.getLogger(__name__)


@dataclass(slots=True)
class PositionBboSnapshot:
    """Aggregate BBO envelope observed while a position was open.

    Populated tick-by-tick by :class:`BboTracker`. ``first_mid`` and
    ``last_mid`` are anchored on the first and most recent BBO tick;
    ``min_bid`` / ``max_ask`` track the worst- and best-case excursions
    *of the market itself*, which is what spec A22 wants for MFE / MAE
    (rather than the trader's own fills).

    All values are seeded from the first tick — until that first tick
    arrives the snapshot has zeros (a consumer that wants to detect
    "no ticks yet" can check :attr:`tick_count`).
    """

    venue: Venue
    symbol: str
    opened_at: float
    tick_count: int = 0
    first_mid: float = 0.0
    last_mid: float = 0.0
    min_bid: float = 0.0
    max_ask: float = 0.0
    last_ts: float = 0.0

    def ingest(self, tick: BBOUpdate) -> None:
        """Fold one :class:`BBOUpdate` into the running envelope."""

        mid = (tick.bid_price + tick.ask_price) / 2.0
        if self.tick_count == 0:
            self.first_mid = mid
            self.min_bid = tick.bid_price
            self.max_ask = tick.ask_price
        else:
            if tick.bid_price < self.min_bid:
                self.min_bid = tick.bid_price
            if tick.ask_price > self.max_ask:
                self.max_ask = tick.ask_price
        self.last_mid = mid
        self.last_ts = tick.ts
        self.tick_count += 1


@dataclass(slots=True)
class _ActiveBboStream:
    """One auto-subscribed BBO stream + the per-position snapshot it feeds."""

    subscription: MarketDataSubscription
    snapshot: PositionBboSnapshot
    consumer_task: asyncio.Task[None]


class BboTracker:
    """Auto-subscribe BBO for open positions; fold ticks into snapshots.

    The tracker is started after the event bus + position manager are
    running. ``start()`` spawns a background task that reads the bus's
    ``POSITION_UPDATE`` topic. Closing the tracker cancels the task,
    closes every active BBO subscription, and drains the consumer
    tasks. Idempotent on both ends.
    """

    def __init__(
        self,
        *,
        event_bus: EventBus,
        market_data_hub: MarketDataHub,
        queue_size: int = 256,
    ) -> None:
        self._event_bus = event_bus
        self._hub = market_data_hub
        self._queue_size = queue_size
        self._streams: dict[tuple[Venue, str], _ActiveBboStream] = {}
        self._supervisor: asyncio.Task[None] | None = None
        self._subscription: Subscription | None = None
        self._started = False
        self._closed = False

    @property
    def is_started(self) -> bool:
        return self._started and not self._closed

    def get_snapshot(
        self, venue: Venue, symbol: str
    ) -> PositionBboSnapshot | None:
        """Return the current :class:`PositionBboSnapshot` for ``(venue, symbol)``."""

        stream = self._streams.get((venue, symbol))
        return stream.snapshot if stream is not None else None

    async def start(self) -> None:
        """Begin consuming ``POSITION_UPDATE`` events. Idempotent."""

        if self._closed:
            raise RuntimeError("BboTracker is closed")
        if self._started:
            return
        self._subscription = self._event_bus.subscribe(
            EventType.POSITION_UPDATE.value,
            queue_size=self._queue_size,
        )
        self._supervisor = asyncio.create_task(
            self._supervise(), name="bbo-tracker-supervisor"
        )
        self._started = True

    async def close(self) -> None:
        """Cancel the supervisor and close every auto-subscription."""

        if self._closed:
            return
        self._closed = True

        if self._supervisor is not None:
            self._supervisor.cancel()
            try:
                await self._supervisor
            except (asyncio.CancelledError, Exception):
                pass
            self._supervisor = None

        if self._subscription is not None:
            try:
                await self._subscription.close()
            except Exception:  # pragma: no cover - defensive
                pass
            self._subscription = None

        for key, stream in list(self._streams.items()):
            await self._tear_down_stream(key, stream)
        self._streams.clear()

    async def _supervise(self) -> None:
        assert self._subscription is not None
        try:
            async for event in self._subscription:
                if isinstance(event, PositionUpdate):
                    await self._handle_position_update(event)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            _log.exception("BboTracker supervisor crashed")

    async def _handle_position_update(self, update: PositionUpdate) -> None:
        key = (update.venue, update.symbol)
        is_open = update.qty > 0 and update.state is not PositionState.IDLE
        active = self._streams.get(key)

        if is_open and active is None:
            await self._open_stream(key, update)
        elif not is_open and active is not None:
            await self._tear_down_stream(key, active)
            self._streams.pop(key, None)

    async def _open_stream(
        self,
        key: tuple[Venue, str],
        update: PositionUpdate,
    ) -> None:
        venue, symbol = key
        try:
            subscription = await self._hub.subscribe_bbo(
                venue, symbol, queue_size=self._queue_size
            )
        except Exception as e:
            _log.warning(
                "BboTracker subscribe failed venue=%s symbol=%s: %s",
                venue.value,
                symbol,
                e,
            )
            return
        snapshot = PositionBboSnapshot(
            venue=venue,
            symbol=symbol,
            opened_at=update.ts,
        )
        consumer_task = asyncio.create_task(
            self._consume(snapshot, subscription),
            name=f"bbo-tracker-consume-{venue.value}-{symbol}",
        )
        self._streams[key] = _ActiveBboStream(
            subscription=subscription,
            snapshot=snapshot,
            consumer_task=consumer_task,
        )

    async def _consume(
        self,
        snapshot: PositionBboSnapshot,
        subscription: MarketDataSubscription,
    ) -> None:
        try:
            async for tick in subscription:
                if isinstance(tick, BBOUpdate):
                    snapshot.ingest(tick)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            _log.exception(
                "BboTracker consumer crashed venue=%s symbol=%s",
                snapshot.venue.value,
                snapshot.symbol,
            )

    async def _tear_down_stream(
        self,
        key: tuple[Venue, str],
        stream: _ActiveBboStream,
    ) -> None:
        stream.consumer_task.cancel()
        try:
            await stream.consumer_task
        except (asyncio.CancelledError, Exception):
            pass
        try:
            await stream.subscription.close()
        except Exception as e:  # pragma: no cover - defensive
            _log.warning(
                "BboTracker error closing subscription %s/%s: %s",
                key[0].value,
                key[1],
                e,
            )


__all__ = ["BboTracker", "PositionBboSnapshot"]
