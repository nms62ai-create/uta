"""Phase 3c: pure stop / take-profit price calculator.

Translates a :data:`StopSpec` (``AbsolutePrice`` / ``BpsFromEntry`` /
``PctFromEntry`` / ``AtrMultiple``) into an absolute trigger price,
given the entry price, the position :class:`Direction`, and whether
the spec describes a stop-loss or take-profit.

Direction + ``kind`` together fix the sign of the offset:

* ``LONG``  + ``"sl"`` → price below entry
* ``LONG``  + ``"tp"`` → price above entry
* ``SHORT`` + ``"sl"`` → price above entry
* ``SHORT`` + ``"tp"`` → price below entry

For :class:`AbsolutePrice` we additionally validate that the
producer-supplied price falls on the correct side of entry; an SL above
entry on a LONG is a malformed signal and we surface it as an error
rather than place an order that triggers immediately.

This module is pure: no I/O, no state, no clock.

Decision references (see ``docs/spec_v1.0.md``):

* A8 — stops are first-class and parallel-submitted alongside the entry
  order in a single multiplexed WS-trade frame.
"""

from __future__ import annotations

from typing import Literal

from ..types import (
    AbsolutePrice,
    AtrMultiple,
    BpsFromEntry,
    Direction,
    PctFromEntry,
    StopSpec,
)

#: Marker for whether the stop is a stop-loss or take-profit.
#: Combined with :class:`Direction` this fixes the offset sign.
StopKind = Literal["sl", "tp"]


class StopError(ValueError):
    """A :data:`StopSpec` cannot be resolved into a valid trigger price.

    Raised on invalid inputs (negative bps / pct / atr_multiple, missing
    ``atr`` for :class:`AtrMultiple`, etc.) and when an
    :class:`AbsolutePrice` falls on the wrong side of entry for the
    given ``direction`` + ``kind``. Subclasses :class:`ValueError`.
    """


def _sign_for(direction: Direction, kind: StopKind) -> float:
    """Return ``+1.0`` or ``-1.0`` so ``entry * (1 + sign * magnitude)`` lands correctly.

    * LONG  + SL → ``-1.0`` (below entry)
    * LONG  + TP → ``+1.0`` (above entry)
    * SHORT + SL → ``+1.0`` (above entry)
    * SHORT + TP → ``-1.0`` (below entry)
    """

    if kind not in ("sl", "tp"):
        raise StopError(f"unknown stop kind: {kind!r}")
    if direction is Direction.LONG:
        return -1.0 if kind == "sl" else 1.0
    if direction is Direction.SHORT:
        return 1.0 if kind == "sl" else -1.0
    raise StopError(f"unknown direction: {direction!r}")


def compute_protective_price(
    stop: StopSpec,
    *,
    entry_price: float,
    direction: Direction,
    kind: StopKind,
    atr: float | None = None,
) -> float:
    """Resolve ``stop`` into an absolute trigger price (always strictly positive).

    Parameters
    ----------
    stop:
        The producer-supplied stop spec. Exactly one of the four
        :data:`StopSpec` variants.
    entry_price:
        Fill price (for an open) or current avg-entry (for a move).
        Must be strictly positive.
    direction:
        Position direction (LONG or SHORT). Together with ``kind`` it
        fixes whether the trigger is above or below entry.
    kind:
        ``"sl"`` for stop-loss, ``"tp"`` for take-profit.
    atr:
        Most-recent ATR sample for the configured period. Required for
        :class:`AtrMultiple` and ignored for the other variants. Must
        be strictly positive when provided.

    Returns
    -------
    float
        Absolute trigger price, strictly positive.

    Raises
    ------
    StopError
        Required context is missing or invalid, or the computed price
        would be non-positive, or an :class:`AbsolutePrice` falls on
        the wrong side of entry.
    """

    if entry_price <= 0:
        raise StopError(f"entry_price must be > 0, got {entry_price!r}")

    sign = _sign_for(direction, kind)

    if isinstance(stop, AbsolutePrice):
        if stop.price <= 0:
            raise StopError(
                f"AbsolutePrice.price must be > 0, got {stop.price!r}"
            )
        # Direction invariant: SL/TP must sit on the correct side of entry.
        if sign < 0 and stop.price >= entry_price:
            raise StopError(
                f"{kind} for {direction} requires price < entry "
                f"({stop.price!r} >= {entry_price!r})"
            )
        if sign > 0 and stop.price <= entry_price:
            raise StopError(
                f"{kind} for {direction} requires price > entry "
                f"({stop.price!r} <= {entry_price!r})"
            )
        return float(stop.price)

    if isinstance(stop, BpsFromEntry):
        if stop.bps <= 0:
            raise StopError(
                f"BpsFromEntry.bps must be > 0, got {stop.bps!r}"
            )
        magnitude = float(stop.bps) / 10000.0
        return _apply_offset(entry_price, sign * magnitude, kind=kind, direction=direction)

    if isinstance(stop, PctFromEntry):
        if stop.pct <= 0:
            raise StopError(
                f"PctFromEntry.pct must be > 0, got {stop.pct!r}"
            )
        magnitude = float(stop.pct) / 100.0
        return _apply_offset(entry_price, sign * magnitude, kind=kind, direction=direction)

    if isinstance(stop, AtrMultiple):
        if stop.multiple <= 0:
            raise StopError(
                f"AtrMultiple.multiple must be > 0, got {stop.multiple!r}"
            )
        if atr is None:
            raise StopError("AtrMultiple stop requires atr")
        if atr <= 0:
            raise StopError(f"atr must be > 0, got {atr!r}")
        price = float(entry_price) + sign * float(stop.multiple) * float(atr)
        if price <= 0:
            raise StopError(
                f"AtrMultiple stop produced non-positive price {price!r} "
                f"(entry={entry_price!r}, multiple={stop.multiple!r}, atr={atr!r})"
            )
        return price

    raise StopError(f"unknown stop variant: {type(stop).__name__}")


def _apply_offset(
    entry_price: float,
    signed_magnitude: float,
    *,
    kind: StopKind,
    direction: Direction,
) -> float:
    """``entry * (1 + signed_magnitude)``, guarded against non-positive results.

    Pct/bps stops larger than 100% on a SHORT TP or LONG SL would push
    the price through zero. We refuse rather than emit a negative price
    that the venue will reject anyway.
    """

    price = float(entry_price) * (1.0 + signed_magnitude)
    if price <= 0:
        raise StopError(
            f"{kind} for {direction} with magnitude {signed_magnitude!r} "
            f"produced non-positive price {price!r}"
        )
    return price


__all__ = ["StopError", "StopKind", "compute_protective_price"]
