"""Phase 3c: pure sizing engine.

Translates a :class:`SizingSpec` (``FixedQty`` / ``NotionalUsd`` /
``PctEquity`` / ``RiskBased``) into a target base-asset quantity given
the reference price at signal-receipt time and any optional context
(equity, stop-loss price).

The result is the **unrounded** desired ``qty``; the venue adapter is
responsible for snapping to ``stepSize`` / ``MARKET_LOT_SIZE`` and for
min-notional checks. Keeping rounding venue-side means this module is
pure and venue-agnostic — the same number falls out for Binance UM and
Bybit Linear given the same inputs.

This module is pure: no I/O, no state, no clock.

Decision references (see ``docs/spec_v1.0.md``):

* A11 — four sizing modes.
* A21 — ``RiskBased`` is the only strategy-shaped sizer on the adapter
  side; producers can pre-resolve and submit ``FixedQty`` to bypass it.
"""

from __future__ import annotations

from ..types import FixedQty, NotionalUsd, PctEquity, RiskBased, SizingSpec


class SizingError(ValueError):
    """A :class:`SizingSpec` cannot be resolved into a positive quantity.

    Raised when required context is missing (e.g. ``PctEquity`` without
    ``equity_usd``), when an input is non-positive, or when the
    computed qty would be zero / negative. ``SizingError`` subclasses
    :class:`ValueError` so callers that already handle ``ValueError``
    keep working.
    """


def compute_target_qty(
    sizing: SizingSpec,
    *,
    reference_price: float,
    equity_usd: float | None = None,
    sl_price: float | None = None,
) -> float:
    """Resolve ``sizing`` into a strictly-positive base-asset ``qty``.

    Parameters
    ----------
    sizing:
        The producer-supplied sizing intent. Exactly one of the four
        :data:`SizingSpec` variants.
    reference_price:
        The price the adapter expects to fill at (BBO mid for MARKET,
        the explicit limit price for LIMIT). Must be strictly positive.
    equity_usd:
        Total equity for the venue, in USD. Required for
        :class:`PctEquity` and ignored for the other variants. Must be
        strictly positive when provided.
    sl_price:
        Stop-loss trigger price. Required for :class:`RiskBased` and
        ignored for the other variants. Must differ from
        ``reference_price`` so the stop distance is non-zero.

    Returns
    -------
    float
        Target qty in base-asset units, strictly positive.

    Raises
    ------
    SizingError
        Required context is missing or invalid, or the computed qty
        is non-positive.
    """

    if reference_price <= 0:
        raise SizingError(
            f"reference_price must be > 0, got {reference_price!r}"
        )

    if isinstance(sizing, FixedQty):
        if sizing.qty <= 0:
            raise SizingError(f"FixedQty.qty must be > 0, got {sizing.qty!r}")
        return float(sizing.qty)

    if isinstance(sizing, NotionalUsd):
        if sizing.notional_usd <= 0:
            raise SizingError(
                f"NotionalUsd.notional_usd must be > 0, got {sizing.notional_usd!r}"
            )
        return float(sizing.notional_usd) / float(reference_price)

    if isinstance(sizing, PctEquity):
        if sizing.pct <= 0:
            raise SizingError(f"PctEquity.pct must be > 0, got {sizing.pct!r}")
        if equity_usd is None:
            raise SizingError("PctEquity sizing requires equity_usd")
        if equity_usd <= 0:
            raise SizingError(f"equity_usd must be > 0, got {equity_usd!r}")
        notional_usd = float(equity_usd) * float(sizing.pct) / 100.0
        return notional_usd / float(reference_price)

    if isinstance(sizing, RiskBased):
        if sizing.risk_usd <= 0:
            raise SizingError(
                f"RiskBased.risk_usd must be > 0, got {sizing.risk_usd!r}"
            )
        if sl_price is None:
            raise SizingError("RiskBased sizing requires sl_price")
        if sl_price <= 0:
            raise SizingError(f"sl_price must be > 0, got {sl_price!r}")
        distance = abs(float(reference_price) - float(sl_price))
        if distance == 0.0:
            raise SizingError(
                "RiskBased sizing requires sl_price != reference_price "
                f"(both {reference_price!r})"
            )
        return float(sizing.risk_usd) / distance

    raise SizingError(f"unknown sizing variant: {type(sizing).__name__}")


__all__ = ["SizingError", "compute_target_qty"]
