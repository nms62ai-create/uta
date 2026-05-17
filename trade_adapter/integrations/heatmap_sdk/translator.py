"""Pure translator from heatmap-sdk's ``adaptive_sdk.Signal`` shape to
UTA's :class:`UniversalSignal`.

Why structural typing
---------------------

We do not want to import ``adaptive_sdk`` here. The heatmap-sdk package
ships its own ``adaptive_sdk`` (vendored, in-process) and ours might be
a different version on the consumer side. Instead, we describe the
narrow contract the translator needs (``signal_id`` + ``exhaustion_type``
+ ``confidence`` + ``timestamp``) as a :class:`typing.Protocol`; any
dataclass / object with those attributes satisfies it. heatmap-sdk's
``adaptive_sdk.Signal`` matches structurally with zero changes.

The translator is total: given valid inputs it always returns a valid
:class:`UniversalSignal`. The autotrader (one level up) is the one that
decides whether to *call* the translator at all (confidence gate,
duplicate suppression, position-already-open guard, etc.).
"""

from __future__ import annotations

from typing import Protocol

from trade_adapter.types import (
    Direction,
    Intent,
    NotionalUsd,
    PctFromEntry,
    UniversalSignal,
    Venue,
)


class _ExhaustionTypeLike(Protocol):
    """Subset of ``adaptive_sdk.ExhaustionType`` we depend on.

    We only need the enum *name* so we don't accidentally bind to the
    consumer's specific enum class. heatmap-sdk's
    ``ExhaustionType.BUY_EXHAUSTION`` and ``.SELL_EXHAUSTION`` both have
    a ``.name`` attribute returning the enum-member name.
    """

    name: str


class AdaptiveSignal(Protocol):
    """Structural shape of an adaptive_sdk ``Signal`` we accept.

    heatmap-sdk's ``adaptive_sdk.types.Signal`` already matches this
    protocol — see ``adaptive_sdk/types.py`` in heatmap-sdk:

    .. code-block:: python

        @dataclass(slots=True, frozen=True)
        class Signal:
            signal_id: str
            symbol: str
            timestamp: float
            exhaustion_type: ExhaustionType
            confidence: float
            arm_id: int
            metrics: MetricsSnapshot
    """

    signal_id: str
    symbol: str
    timestamp: float
    exhaustion_type: _ExhaustionTypeLike
    confidence: float


def direction_from_exhaustion(exhaustion_name: str) -> Direction:
    """Map an exhaustion-type *name* to a :class:`Direction`.

    Exhaustion is a *reversal* signal: aggressive buying that runs out
    of fuel (``BUY_EXHAUSTION``) is a short setup; ``SELL_EXHAUSTION``
    is a long setup. This matches heatmap-sdk's own ``EntryFilterEngine``
    behavior (BUY_EXHAUSTION → ``short_filter='OK'``, SELL_EXHAUSTION →
    ``long_filter='OK'``).
    """

    name = exhaustion_name.upper()
    if name == "BUY_EXHAUSTION":
        return Direction.SHORT
    if name == "SELL_EXHAUSTION":
        return Direction.LONG
    raise ValueError(f"unknown exhaustion type: {exhaustion_name!r}")


def build_universal_signal(
    adaptive_signal: AdaptiveSignal,
    *,
    symbol: str,
    venue: Venue,
    notional_usd: float,
    sl_pct: float | None,
    tp_pct: float | None,
    ttl_seconds: float,
    source: str = "heatmap_sdk",
) -> UniversalSignal:
    """Translate an ``adaptive_sdk.Signal`` into a UTA :class:`UniversalSignal`.

    Caller is responsible for the ``symbol`` (we don't trust the
    signal's own symbol field — the autotrader is bound to the symbol
    the *user* picked in the UI, so it stays the source of truth).

    Sizing is always :class:`NotionalUsd` because that's what the
    heatmap-sdk UI gives us (dollar volume). SL/TP — when provided —
    are translated as :class:`PctFromEntry` in native (venue-managed)
    mode. ``signal_id`` and ``correlation_id`` are both the
    adaptive-signal id so the producer can thread one signal end-to-
    end through UTA's idempotency / outcome layers.
    """

    if notional_usd <= 0.0:
        raise ValueError(f"notional_usd must be positive, got {notional_usd}")
    if sl_pct is not None and sl_pct <= 0.0:
        raise ValueError(f"sl_pct must be positive when set, got {sl_pct}")
    if tp_pct is not None and tp_pct <= 0.0:
        raise ValueError(f"tp_pct must be positive when set, got {tp_pct}")
    if ttl_seconds <= 0.0:
        raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")

    direction = direction_from_exhaustion(adaptive_signal.exhaustion_type.name)
    sl = PctFromEntry(pct=sl_pct) if sl_pct is not None else None
    tp = PctFromEntry(pct=tp_pct) if tp_pct is not None else None

    return UniversalSignal(
        signal_id=adaptive_signal.signal_id,
        source=source,
        symbol=symbol.upper(),
        venue=venue,
        direction=direction,
        intent=Intent.OPEN,
        sizing=NotionalUsd(notional_usd=float(notional_usd)),
        sl=sl,
        tp=tp,
        ttl_seconds=float(ttl_seconds),
        correlation_id=adaptive_signal.signal_id,
        metadata={
            "producer": source,
            "confidence": f"{float(adaptive_signal.confidence):.4f}",
            "adaptive_signal_ts": f"{float(adaptive_signal.timestamp):.6f}",
        },
    )


__all__ = [
    "AdaptiveSignal",
    "build_universal_signal",
    "direction_from_exhaustion",
]
