"""Phase 3b wire-event translators for Binance USD-M Futures.

Pure functions that convert raw Binance WS frames into UTA event types.
No I/O, no state, no clock — each call is a deterministic transform.
The adapter wires them into the stream/user-data clients' handler
slots in a later sub-phase; here we just lock the field-mapping
contract and pin it with tests so a future Binance API drift surfaces
as a translator-level test failure rather than a phantom production
bug.

Frame shapes (Binance USD-M Futures docs):

Market data (combined stream, ``data`` sub-object):
    aggTrade     — ``{"e":"aggTrade","T":<ms>,"s":<symbol>,"p":<price>,
                    "q":<qty>,"m":<is buyer maker>,"a":<aggTradeId>}``
    bookTicker   — ``{"e":"bookTicker","T":<ms>,"s":<symbol>,
                    "b":<bidPx>,"B":<bidQty>,
                    "a":<askPx>,"A":<askQty>}``
    depth5/10/20 — partial-book *snapshot* every 100/250 ms (no diff
                    bookkeeping required): same shape as the depth
                    diff stream but always representing the current
                    top-N state.
                    ``{"e":"depthUpdate","T":<ms>,"s":<symbol>,
                    "u":<lastUpdateId>,
                    "b":[[px,qty], ...], "a":[[px,qty], ...]}``

User data (single-key listenKey stream):
    ORDER_TRADE_UPDATE — order lifecycle events plus per-execution
                    fills. The ``o`` sub-object carries everything we
                    need. ``x`` (execution type) tells us whether this
                    is a TRADE (fill happened) vs. a state transition
                    (NEW / CANCELED / EXPIRED / REJECTED).
    ACCOUNT_UPDATE  — per-position-and-balance reconciliation event.
                    The ``a.P`` array carries one entry per affected
                    symbol; the translator emits one
                    :class:`PositionUpdate` per entry.

Translator contract:
    * Single-event translators return ``None`` if the frame doesn't
      represent a UTA event (e.g. an ``ORDER_TRADE_UPDATE`` with
      ``x="CALCULATED"`` carries no new state).
    * Bulk translators (``account_update_to_position_updates``) return
      a (possibly empty) list.
    * Translators raise :class:`ValueError` on a *malformed* frame
      (missing required field, wrong shape) — silent skip would mask
      a real protocol drift.
    * Producer-side fields not present on the wire
      (``signal_id`` / ``correlation_id``) are left ``None``; the
      signal_router joins them by ``client_order_id`` in Phase 3d.

These functions don't import the adapter — they're at the same layer
as ``auth.py`` / ``symbols.py`` so future venues (Bybit etc.) can
follow the exact same pattern.
"""

from __future__ import annotations

from typing import Any

from ...types import (
    BBOUpdate,
    BookLevel,
    BookUpdate,
    Direction,
    Fill,
    OrderSide,
    OrderStatus,
    OrderUpdate,
    PositionState,
    PositionUpdate,
    TradePrint,
    Venue,
)

_VENUE = Venue.BINANCE_UM


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ms_to_seconds(ms: Any) -> float:
    """Convert a millisecond timestamp (int / float / str) to a seconds float."""

    try:
        return float(ms) / 1000.0
    except (TypeError, ValueError) as e:
        raise ValueError(f"invalid millisecond timestamp: {ms!r}") from e


def _required(frame: dict[str, Any], key: str, *, context: str) -> Any:
    """Look up ``key`` in ``frame``; raise ``ValueError`` if absent.

    ``context`` is included in the error so a translator failure
    points at the original event type.
    """

    if key not in frame:
        raise ValueError(f"{context}: missing required field {key!r}: {frame!r}")
    return frame[key]


def _float(value: Any, *, field: str, context: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"{context}: field {field!r} not convertible to float: {value!r}"
        ) from e


def _parse_book_side(levels: Any, *, side: str, context: str) -> tuple[BookLevel, ...]:
    if not isinstance(levels, list):
        raise ValueError(
            f"{context}: book {side} expected list, got {type(levels).__name__}"
        )
    out: list[BookLevel] = []
    for entry in levels:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            raise ValueError(
                f"{context}: malformed book {side} entry: {entry!r}"
            )
        price = _float(entry[0], field=f"{side}[0]", context=context)
        qty = _float(entry[1], field=f"{side}[1]", context=context)
        out.append(BookLevel(price=price, qty=qty))
    return tuple(out)


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------


def agg_trade_to_trade_print(frame: dict[str, Any]) -> TradePrint:
    """Translate one Binance ``aggTrade`` frame into a :class:`TradePrint`.

    Binance's ``m`` flag means "is the buyer the maker?" — i.e. the
    *taker* is the seller. UTA's ``TradePrint.side`` is the side of
    the *taker*, so ``m=True`` → ``OrderSide.SELL``.
    """

    context = "agg_trade_to_trade_print"
    symbol = str(_required(frame, "s", context=context))
    price = _float(_required(frame, "p", context=context), field="p", context=context)
    qty = _float(_required(frame, "q", context=context), field="q", context=context)
    is_buyer_maker = bool(_required(frame, "m", context=context))
    ts = _ms_to_seconds(_required(frame, "T", context=context))
    trade_id = str(_required(frame, "a", context=context))
    return TradePrint(
        venue=_VENUE,
        symbol=symbol,
        price=price,
        qty=qty,
        side=OrderSide.SELL if is_buyer_maker else OrderSide.BUY,
        ts=ts,
        trade_id=trade_id,
    )


def book_ticker_to_bbo_update(frame: dict[str, Any]) -> BBOUpdate:
    """Translate one Binance ``bookTicker`` frame into a :class:`BBOUpdate`.

    ``T`` (transaction time) is preferred over ``E`` (event time) — it
    reflects when the matching engine produced the BBO, not when the
    server queued the frame. Falls back to ``E`` if ``T`` is absent.
    """

    context = "book_ticker_to_bbo_update"
    symbol = str(_required(frame, "s", context=context))
    bid_price = _float(_required(frame, "b", context=context), field="b", context=context)
    bid_qty = _float(_required(frame, "B", context=context), field="B", context=context)
    ask_price = _float(_required(frame, "a", context=context), field="a", context=context)
    ask_qty = _float(_required(frame, "A", context=context), field="A", context=context)
    ts_raw = frame.get("T", frame.get("E"))
    if ts_raw is None:
        raise ValueError(f"{context}: missing both 'T' and 'E' timestamps: {frame!r}")
    ts = _ms_to_seconds(ts_raw)
    return BBOUpdate(
        venue=_VENUE,
        symbol=symbol,
        bid_price=bid_price,
        bid_qty=bid_qty,
        ask_price=ask_price,
        ask_qty=ask_qty,
        ts=ts,
    )


def depth_snapshot_to_book_update(frame: dict[str, Any]) -> BookUpdate:
    """Translate one Binance ``depth<N>`` partial-book snapshot into a
    :class:`BookUpdate`.

    Partial book streams (``depth5`` / ``depth10`` / ``depth20``) push
    full top-N snapshots — no diff bookkeeping required. ``u`` is the
    last update id; we use it as :attr:`BookUpdate.sequence` so
    downstream consumers can detect gaps.
    """

    context = "depth_snapshot_to_book_update"
    symbol = str(_required(frame, "s", context=context))
    bids = _parse_book_side(
        _required(frame, "b", context=context), side="bids", context=context
    )
    asks = _parse_book_side(
        _required(frame, "a", context=context), side="asks", context=context
    )
    ts_raw = frame.get("T", frame.get("E"))
    if ts_raw is None:
        raise ValueError(f"{context}: missing both 'T' and 'E' timestamps: {frame!r}")
    ts = _ms_to_seconds(ts_raw)
    sequence_raw = frame.get("u")
    try:
        sequence = int(sequence_raw) if sequence_raw is not None else 0
    except (TypeError, ValueError):
        sequence = 0
    return BookUpdate(
        venue=_VENUE,
        symbol=symbol,
        bids=bids,
        asks=asks,
        ts=ts,
        sequence=sequence,
    )


# ---------------------------------------------------------------------------
# User data
# ---------------------------------------------------------------------------


# Binance ``X`` (order status) -> UTA :class:`OrderStatus`.
# Unknown values are mapped to ``OrderStatus.AMBIGUOUS`` so a future
# Binance status doesn't silently drop the event — the AMBIGUOUS state
# is what the upper layer should reconcile against REST.
_ORDER_STATUS_MAP: dict[str, OrderStatus] = {
    "NEW": OrderStatus.ACK,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELLED,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.EXPIRED,
    "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
}


def _map_order_status(raw: Any) -> OrderStatus:
    return _ORDER_STATUS_MAP.get(str(raw), OrderStatus.AMBIGUOUS)


def order_trade_update_to_order_update(event: dict[str, Any]) -> OrderUpdate:
    """Translate one ``ORDER_TRADE_UPDATE`` event into an :class:`OrderUpdate`.

    The translator always returns an :class:`OrderUpdate` — even for
    execution types that don't change semantically observable state
    (``CALCULATED``, ``AMENDMENT``) — because the cumulative filled
    quantity and avg-fill price may still have advanced. Consumers
    that want to de-dupe should key on ``(client_order_id, status,
    filled_qty)``.

    ``rejection_reason`` is left ``None`` — Binance doesn't carry a
    human-readable reason on the ORDER_TRADE_UPDATE event; the upper
    layer can populate it from the REST ``GET /openOrders`` reconcile
    if needed.
    """

    context = "order_trade_update_to_order_update"
    o = _required(event, "o", context=context)
    if not isinstance(o, dict):
        raise ValueError(f"{context}: 'o' field is not an object: {o!r}")
    symbol = str(_required(o, "s", context=context))
    client_order_id = str(_required(o, "c", context=context))
    exchange_order_id_raw = o.get("i")
    exchange_order_id = (
        str(exchange_order_id_raw) if exchange_order_id_raw is not None else None
    )
    status = _map_order_status(_required(o, "X", context=context))
    filled_qty = _float(o.get("z", "0"), field="z", context=context)
    avg_price_raw = o.get("ap")
    if avg_price_raw is None or _float(avg_price_raw, field="ap", context=context) == 0.0:
        avg_fill_price: float | None = None
    else:
        avg_fill_price = _float(avg_price_raw, field="ap", context=context)
    ts = _ms_to_seconds(_required(o, "T", context=context))
    return OrderUpdate(
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        venue=_VENUE,
        symbol=symbol,
        status=status,
        filled_qty=filled_qty,
        avg_fill_price=avg_fill_price,
        ts=ts,
    )


def order_trade_update_to_fill(event: dict[str, Any]) -> Fill | None:
    """Translate one ``ORDER_TRADE_UPDATE`` event into a :class:`Fill`.

    Returns ``None`` if the event isn't a trade execution
    (``x != "TRADE"``) or the reported last-filled quantity is zero —
    Binance occasionally sends ``x="TRADE"`` with ``l="0"`` on
    amendments; we filter those out so consumers can treat every Fill
    they see as a real fill.
    """

    context = "order_trade_update_to_fill"
    o = _required(event, "o", context=context)
    if not isinstance(o, dict):
        raise ValueError(f"{context}: 'o' field is not an object: {o!r}")
    if str(o.get("x")) != "TRADE":
        return None
    last_qty = _float(o.get("l", "0"), field="l", context=context)
    if last_qty == 0.0:
        return None
    trade_id_raw = o.get("t")
    if trade_id_raw is None or str(trade_id_raw) == "0":
        # No trade id means no fill actually happened; fail-safe to skip.
        return None
    last_price = _float(_required(o, "L", context=context), field="L", context=context)
    fee_asset = str(o.get("N", ""))
    fee_raw = o.get("n", "0")
    # We don't FX-convert here — the producer asked for "USD-ish" so
    # USDT-denominated fees pass through as-is, everything else
    # surfaces as 0 with the asset noted in the test. Phase 3e
    # (reconciliation) is where FX-aware fee accounting would live.
    if fee_asset.upper() in ("USDT", "BUSD", "USD"):
        fee_usd = _float(fee_raw, field="n", context=context)
    else:
        fee_usd = 0.0
    side_raw = str(_required(o, "S", context=context)).upper()
    side = OrderSide.BUY if side_raw == "BUY" else OrderSide.SELL
    is_maker = bool(o.get("m", False))
    ts = _ms_to_seconds(_required(o, "T", context=context))
    exchange_order_id = str(_required(o, "i", context=context))
    client_order_id = str(_required(o, "c", context=context))
    symbol = str(_required(o, "s", context=context))
    return Fill(
        fill_id=str(trade_id_raw),
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        venue=_VENUE,
        symbol=symbol,
        side=side,
        qty=last_qty,
        price=last_price,
        fee_usd=fee_usd,
        is_maker=is_maker,
        ts=ts,
    )


def account_update_to_position_updates(event: dict[str, Any]) -> list[PositionUpdate]:
    """Translate one ``ACCOUNT_UPDATE`` event into per-symbol :class:`PositionUpdate` s.

    A single Binance ``ACCOUNT_UPDATE`` carries every position that
    moved during the triggering action (cross margin, multi-symbol
    fills, liquidation cascades), so the translator returns a list.
    Binance encodes direction in the sign of ``pa`` (positionAmount):
    positive = LONG, negative = SHORT, zero = flat. Flat positions are
    surfaced with :class:`PositionState.IDLE` so the upper-layer
    position manager can transition out of OPEN/REDUCING on close.

    Only Net-mode (``ps="BOTH"``) positions are translated — Hedge-mode
    (``ps="LONG"`` / ``ps="SHORT"``) is out of scope for v1 and is
    silently skipped. If a v1 user has Hedge mode on by accident, the
    risk gates (Phase 3f) will catch the mismatch on submit.
    """

    context = "account_update_to_position_updates"
    a = _required(event, "a", context=context)
    if not isinstance(a, dict):
        raise ValueError(f"{context}: 'a' field is not an object: {a!r}")
    positions = a.get("P") or []
    if not isinstance(positions, list):
        raise ValueError(f"{context}: 'a.P' is not a list: {positions!r}")
    ts = _ms_to_seconds(_required(event, "T", context=context))

    out: list[PositionUpdate] = []
    for raw in positions:
        if not isinstance(raw, dict):
            raise ValueError(f"{context}: position entry not an object: {raw!r}")
        position_side = str(raw.get("ps", "BOTH")).upper()
        if position_side not in ("BOTH", ""):
            # Hedge-mode entry; skip. Net-only is decision A0.
            continue
        symbol = str(_required(raw, "s", context=context))
        pa = _float(_required(raw, "pa", context=context), field="pa", context=context)
        entry_price = _float(raw.get("ep", "0"), field="ep", context=context)
        unrealized = raw.get("up")
        unrealized_pnl = (
            _float(unrealized, field="up", context=context)
            if unrealized is not None
            else None
        )
        if pa > 0.0:
            direction = Direction.LONG
            qty = pa
            state = PositionState.OPEN
        elif pa < 0.0:
            direction = Direction.SHORT
            qty = -pa
            state = PositionState.OPEN
        else:
            # Flat: side is unobservable from this event alone; we
            # default to LONG and rely on state=IDLE+qty=0 to convey
            # "no position". The upper layer treats IDLE as terminal
            # regardless of direction.
            direction = Direction.LONG
            qty = 0.0
            state = PositionState.IDLE
        out.append(
            PositionUpdate(
                venue=_VENUE,
                symbol=symbol,
                direction=direction,
                qty=qty,
                entry_price=entry_price,
                state=state,
                liquidation_price=None,
                unrealized_pnl_usd=unrealized_pnl,
                margin_used_usd=None,
                ts=ts,
            )
        )
    return out


__all__ = [
    "account_update_to_position_updates",
    "agg_trade_to_trade_print",
    "book_ticker_to_bbo_update",
    "depth_snapshot_to_book_update",
    "order_trade_update_to_fill",
    "order_trade_update_to_order_update",
]
