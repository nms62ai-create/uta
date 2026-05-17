"""UI-controlled autotrader for the heatmap-sdk producer integration.

What this class is
------------------

A small piece of *state* sitting between heatmap-sdk's existing
``AdaptiveMarketService.on_agg_trade(...)`` callback (which already
yields an ``adaptive_sdk.Signal`` every time a new exhaustion fires)
and UTA's :class:`TradeAdapter.submit_signal`. It encodes the user
flow described in the spec:

    UI → set symbol / $-volume / SL% / TP% / confidence threshold →
    UI → "start autotrade" (enable) →
    on each adaptive Signal: gate → translate → submit to UTA.

What this class is *not*
------------------------

* Not an order placer — UTA's :class:`SignalRouter` does that.
* Not a position tracker — UTA's :class:`PositionManager` does that.
* Not an exit engine — heatmap-sdk's existing :class:`ExitEngine`
  (which already calls :meth:`OrderExecutor.close_position`) can stay
  as-is; or the consumer can let UTA's stop / take-profit native orders
  handle exits and remove the ``ExitEngine`` entirely. Both work; see
  ``INTEGRATION.md`` for the trade-off.

Why a guard against existing positions
--------------------------------------

The user's described flow is single-position scalping: one signal,
one entry, exit via SL/TP. We refuse new entries while a position is
open on the same (venue, symbol). This is independent of UTA's own
idempotency layer (which dedupes by ``signal_id``); UTA can't refuse a
*different* ``signal_id`` that happens to fire while we're already in a
trade — that's the autotrader's job.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace

from trade_adapter.embedded import TradeAdapter
from trade_adapter.types import SignalAck, Venue

from .translator import AdaptiveSignal, build_universal_signal

_log = logging.getLogger(__name__)


class InvalidAutotradeSettings(ValueError):
    """Raised when :meth:`HeatmapAutoTrader.update_settings` is called
    with an inconsistent or out-of-range payload."""


@dataclass(slots=True, frozen=True)
class AutotradeSettings:
    """User-controlled autotrade parameters set from the UI.

    ``enabled`` is the master switch: while ``False`` the autotrader
    short-circuits every signal at the gate, regardless of confidence
    or position state. The expectation is that the UI ties this to a
    "Start / Stop autotrade" toggle.

    ``min_confidence`` is the floor on ``adaptive_signal.confidence``
    that must be met before the autotrader submits. ``0.0`` means
    "submit everything"; ``1.0`` means "submit nothing".

    ``ttl_seconds`` is forwarded into the resulting
    :class:`UniversalSignal` so UTA can age out a stale signal if its
    submission gets stuck behind a slow consumer of the bus. The
    heatmap-sdk default polling interval is sub-second so 5s is a
    conservative ceiling.
    """

    symbol: str
    venue: Venue
    notional_usd: float
    sl_pct: float | None
    tp_pct: float | None
    min_confidence: float = 0.0
    ttl_seconds: float = 5.0
    enabled: bool = False

    def __post_init__(self) -> None:
        if not self.symbol:
            raise InvalidAutotradeSettings("symbol must be non-empty")
        if self.notional_usd <= 0.0:
            raise InvalidAutotradeSettings(
                f"notional_usd must be positive, got {self.notional_usd}"
            )
        if self.sl_pct is not None and self.sl_pct <= 0.0:
            raise InvalidAutotradeSettings(
                f"sl_pct must be positive when set, got {self.sl_pct}"
            )
        if self.tp_pct is not None and self.tp_pct <= 0.0:
            raise InvalidAutotradeSettings(
                f"tp_pct must be positive when set, got {self.tp_pct}"
            )
        if not 0.0 <= self.min_confidence <= 1.0:
            raise InvalidAutotradeSettings(
                f"min_confidence must be in [0, 1], got {self.min_confidence}"
            )
        if self.ttl_seconds <= 0.0:
            raise InvalidAutotradeSettings(
                f"ttl_seconds must be positive, got {self.ttl_seconds}"
            )


class HeatmapAutoTrader:
    """Glue between heatmap-sdk's adaptive Signal stream and UTA.

    Lifecycle is simple: construct with a started
    :class:`TradeAdapter`, call :meth:`update_settings` once when the
    UI is loaded (and again on every UI change), call :meth:`enable` /
    :meth:`disable` for the master switch, and call
    :meth:`on_adaptive_signal` from wherever you currently consume
    ``AdaptiveMarketService.on_agg_trade``'s return value.
    """

    def __init__(
        self,
        adapter: TradeAdapter,
        *,
        settings: AutotradeSettings | None = None,
    ) -> None:
        self._adapter = adapter
        self._settings: AutotradeSettings | None = settings
        self._submit_lock = asyncio.Lock()
        self._seen_signal_ids: set[str] = set()
        # Bound the dedupe set so a long-running session doesn't leak
        # memory. heatmap-sdk emits at most a few signals/sec; 4096 is
        # plenty for hours of trading.
        self._max_seen_signal_ids = 4096

    # ------------------------------------------------------------------
    # Settings surface
    # ------------------------------------------------------------------

    @property
    def settings(self) -> AutotradeSettings | None:
        return self._settings

    @property
    def enabled(self) -> bool:
        return self._settings is not None and self._settings.enabled

    def update_settings(self, settings: AutotradeSettings) -> None:
        """Replace the active settings wholesale.

        Atomic: the autotrader either sees the old settings or the new
        ones, never a partial blend.
        """

        self._settings = settings

    def enable(self) -> None:
        """Flip the master switch on. No-op if no settings set yet."""

        if self._settings is None:
            raise InvalidAutotradeSettings(
                "cannot enable before update_settings was called"
            )
        self._settings = replace(self._settings, enabled=True)

    def disable(self) -> None:
        """Flip the master switch off. No-op if no settings set yet."""

        if self._settings is None:
            return
        self._settings = replace(self._settings, enabled=False)

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    async def on_adaptive_signal(
        self, signal: AdaptiveSignal
    ) -> SignalAck | None:
        """Gate, translate and submit ``signal``.

        Returns the :class:`SignalAck` from UTA when the signal was
        forwarded, ``None`` when the autotrader skipped it. ``None``
        is the common case while the user is just watching the
        heatmap — keep it cheap.
        """

        settings = self._settings
        if settings is None or not settings.enabled:
            return None

        if signal.confidence < settings.min_confidence:
            _log.debug(
                "skip signal_id=%s confidence=%.3f < %.3f",
                signal.signal_id,
                signal.confidence,
                settings.min_confidence,
            )
            return None

        # Cheap local dedupe so we don't re-bounce the same adaptive
        # signal through UTA's idempotency cache on every callback.
        if signal.signal_id in self._seen_signal_ids:
            return None

        if self._adapter.kill_switch_tripped:
            _log.info(
                "skip signal_id=%s — UTA kill switch tripped",
                signal.signal_id,
            )
            return None

        # Refuse new entries while we already hold a position on this
        # (venue, symbol). The user's flow is one position at a time;
        # the next adaptive signal after a close will be the entry.
        existing = self._adapter.get_position(settings.venue, settings.symbol)
        if existing is not None and existing.qty > 0.0:
            _log.debug(
                "skip signal_id=%s — position already open on %s/%s",
                signal.signal_id,
                settings.venue.value,
                settings.symbol,
            )
            return None

        universal = build_universal_signal(
            signal,
            symbol=settings.symbol,
            venue=settings.venue,
            notional_usd=settings.notional_usd,
            sl_pct=settings.sl_pct,
            tp_pct=settings.tp_pct,
            ttl_seconds=settings.ttl_seconds,
        )

        # Serialise submissions so two near-simultaneous signals can't
        # both pass the position guard before either has registered an
        # OPENING state on the store.
        async with self._submit_lock:
            # Re-check the kill switch + position inside the lock —
            # state may have changed while we were awaiting it.
            if self._adapter.kill_switch_tripped:
                return None
            existing = self._adapter.get_position(
                settings.venue, settings.symbol
            )
            if existing is not None and existing.qty > 0.0:
                return None
            ack = await self._adapter.submit_signal(universal)

        self._remember_signal(signal.signal_id)
        return ack

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _remember_signal(self, signal_id: str) -> None:
        if len(self._seen_signal_ids) >= self._max_seen_signal_ids:
            # Cheap, deterministic eviction: drop ~half. Frequency of
            # this branch is once per ~4k signals so a single rebuild
            # is fine.
            keep = list(self._seen_signal_ids)[self._max_seen_signal_ids // 2:]
            self._seen_signal_ids = set(keep)
        self._seen_signal_ids.add(signal_id)


__all__ = [
    "AutotradeSettings",
    "HeatmapAutoTrader",
    "InvalidAutotradeSettings",
]
