"""JSON wire-format (de)serializers for ``trade_adapter.types``.

Pure, no I/O. The adapter does not depend on Pydantic (decision A1 +
prohibition C.13), so we hand-roll the (de)serializers for the small
finite set of wire types. The wire shape is stable and locked by
``tests/protocol/test_schema_lock.py``.

Conventions
-----------
* Tagged unions (``SizingSpec``, ``StopSpec``) carry a ``"kind"`` field
  on the wire. Adapter-side variants are distinct dataclass subclasses,
  so ``"kind"`` is added on serialize and dispatched on deserialize.
* ``Enum`` instances are written as their ``str`` value.
* ``None`` round-trips as JSON ``null``.
* ``bytes`` is not part of any wire shape.
"""

from __future__ import annotations

from typing import Any

from . import types as T

# ---------------------------------------------------------------------------
# Tagged-union dispatch tables
# ---------------------------------------------------------------------------

_SIZING_KIND_BY_TYPE: dict[type, str] = {
    T.FixedQty: "fixed_qty",
    T.NotionalUsd: "notional_usd",
    T.PctEquity: "pct_equity",
    T.RiskBased: "risk_based",
}

_STOP_KIND_BY_TYPE: dict[type, str] = {
    T.AbsolutePrice: "absolute",
    T.BpsFromEntry: "bps",
    T.PctFromEntry: "pct",
    T.AtrMultiple: "atr_multiple",
}


# ---------------------------------------------------------------------------
# Sizing
# ---------------------------------------------------------------------------


def sizing_to_wire(s: T.SizingSpec) -> dict[str, Any]:
    kind = _SIZING_KIND_BY_TYPE.get(type(s))
    if kind is None:
        raise TypeError(f"Unsupported sizing variant: {type(s).__name__}")
    if isinstance(s, T.FixedQty):
        return {"kind": kind, "qty": s.qty}
    if isinstance(s, T.NotionalUsd):
        return {"kind": kind, "notional_usd": s.notional_usd}
    if isinstance(s, T.PctEquity):
        return {"kind": kind, "pct": s.pct}
    if isinstance(s, T.RiskBased):
        return {"kind": kind, "risk_usd": s.risk_usd}
    raise TypeError(f"Unsupported sizing variant: {type(s).__name__}")


def sizing_from_wire(d: dict[str, Any]) -> T.SizingSpec:
    kind = d["kind"]
    if kind == "fixed_qty":
        return T.FixedQty(qty=float(d["qty"]))
    if kind == "notional_usd":
        return T.NotionalUsd(notional_usd=float(d["notional_usd"]))
    if kind == "pct_equity":
        return T.PctEquity(pct=float(d["pct"]))
    if kind == "risk_based":
        return T.RiskBased(risk_usd=float(d["risk_usd"]))
    raise ValueError(f"Unknown sizing kind: {kind!r}")


# ---------------------------------------------------------------------------
# Stops
# ---------------------------------------------------------------------------


def stop_to_wire(s: T.StopSpec) -> dict[str, Any]:
    kind = _STOP_KIND_BY_TYPE.get(type(s))
    if kind is None:
        raise TypeError(f"Unsupported stop variant: {type(s).__name__}")
    if isinstance(s, T.AbsolutePrice):
        return {"kind": kind, "price": s.price, "mode": s.mode.value}
    if isinstance(s, T.BpsFromEntry):
        return {"kind": kind, "bps": s.bps, "mode": s.mode.value}
    if isinstance(s, T.PctFromEntry):
        return {"kind": kind, "pct": s.pct, "mode": s.mode.value}
    if isinstance(s, T.AtrMultiple):
        return {
            "kind": kind,
            "multiple": s.multiple,
            "atr_period_seconds": s.atr_period_seconds,
            "mode": s.mode.value,
        }
    raise TypeError(f"Unsupported stop variant: {type(s).__name__}")


def stop_from_wire(d: dict[str, Any]) -> T.StopSpec:
    kind = d["kind"]
    mode = T.StopMode(d.get("mode", T.StopMode.NATIVE.value))
    if kind == "absolute":
        return T.AbsolutePrice(price=float(d["price"]), mode=mode)
    if kind == "bps":
        return T.BpsFromEntry(bps=int(d["bps"]), mode=mode)
    if kind == "pct":
        return T.PctFromEntry(pct=float(d["pct"]), mode=mode)
    if kind == "atr_multiple":
        return T.AtrMultiple(
            multiple=float(d["multiple"]),
            atr_period_seconds=int(d["atr_period_seconds"]),
            mode=mode,
        )
    raise ValueError(f"Unknown stop kind: {kind!r}")


def _opt_stop_to_wire(s: T.StopSpec | None) -> dict[str, Any] | None:
    return None if s is None else stop_to_wire(s)


def _opt_stop_from_wire(d: dict[str, Any] | None) -> T.StopSpec | None:
    return None if d is None else stop_from_wire(d)


# ---------------------------------------------------------------------------
# UniversalSignal
# ---------------------------------------------------------------------------


def signal_to_wire(s: T.UniversalSignal) -> dict[str, Any]:
    return {
        "signal_id": s.signal_id,
        "source": s.source,
        "symbol": s.symbol,
        "venue": s.venue.value,
        "direction": s.direction.value,
        "intent": s.intent.value,
        "sizing": sizing_to_wire(s.sizing),
        "sl": _opt_stop_to_wire(s.sl),
        "tp": _opt_stop_to_wire(s.tp),
        "ttl_seconds": s.ttl_seconds,
        "correlation_id": s.correlation_id,
        "metadata": dict(s.metadata),
    }


def signal_from_wire(d: dict[str, Any]) -> T.UniversalSignal:
    return T.UniversalSignal(
        signal_id=d["signal_id"],
        source=d["source"],
        symbol=d["symbol"],
        venue=T.Venue(d["venue"]),
        direction=T.Direction(d["direction"]),
        intent=T.Intent(d["intent"]),
        sizing=sizing_from_wire(d["sizing"]),
        sl=_opt_stop_from_wire(d.get("sl")),
        tp=_opt_stop_from_wire(d.get("tp")),
        ttl_seconds=float(d["ttl_seconds"]),
        correlation_id=d.get("correlation_id"),
        metadata=dict(d.get("metadata") or {}),
    )


# ---------------------------------------------------------------------------
# Outbound events (one-way: adapter -> producer)
#
# Producers parse these from the gateway WS feed; the embedded API gives
# them as Python dataclasses directly so deserializers there are unused.
# We still ship matched ``*_from_wire`` helpers so that the gateway and
# tests can round-trip.
# ---------------------------------------------------------------------------


def order_update_to_wire(u: T.OrderUpdate) -> dict[str, Any]:
    return {
        "client_order_id": u.client_order_id,
        "exchange_order_id": u.exchange_order_id,
        "venue": u.venue.value,
        "symbol": u.symbol,
        "status": u.status.value,
        "filled_qty": u.filled_qty,
        "avg_fill_price": u.avg_fill_price,
        "ts": u.ts,
        "signal_id": u.signal_id,
        "correlation_id": u.correlation_id,
        "rejection_reason": u.rejection_reason,
    }


def fill_to_wire(f: T.Fill) -> dict[str, Any]:
    return {
        "fill_id": f.fill_id,
        "client_order_id": f.client_order_id,
        "exchange_order_id": f.exchange_order_id,
        "venue": f.venue.value,
        "symbol": f.symbol,
        "side": f.side.value,
        "qty": f.qty,
        "price": f.price,
        "fee_usd": f.fee_usd,
        "is_maker": f.is_maker,
        "ts": f.ts,
        "signal_id": f.signal_id,
        "correlation_id": f.correlation_id,
    }


def position_update_to_wire(p: T.PositionUpdate) -> dict[str, Any]:
    return {
        "venue": p.venue.value,
        "symbol": p.symbol,
        "direction": p.direction.value,
        "qty": p.qty,
        "entry_price": p.entry_price,
        "state": p.state.value,
        "liquidation_price": p.liquidation_price,
        "unrealized_pnl_usd": p.unrealized_pnl_usd,
        "margin_used_usd": p.margin_used_usd,
        "ts": p.ts,
        "signal_id": p.signal_id,
        "correlation_id": p.correlation_id,
    }


def book_update_to_wire(b: T.BookUpdate) -> dict[str, Any]:
    return {
        "venue": b.venue.value,
        "symbol": b.symbol,
        "bids": [[lvl.price, lvl.qty] for lvl in b.bids],
        "asks": [[lvl.price, lvl.qty] for lvl in b.asks],
        "ts": b.ts,
        "sequence": b.sequence,
    }


def trade_print_to_wire(t: T.TradePrint) -> dict[str, Any]:
    return {
        "venue": t.venue.value,
        "symbol": t.symbol,
        "price": t.price,
        "qty": t.qty,
        "side": t.side.value,
        "ts": t.ts,
        "trade_id": t.trade_id,
    }


def bbo_update_to_wire(b: T.BBOUpdate) -> dict[str, Any]:
    return {
        "venue": b.venue.value,
        "symbol": b.symbol,
        "bid_price": b.bid_price,
        "bid_qty": b.bid_qty,
        "ask_price": b.ask_price,
        "ask_qty": b.ask_qty,
        "ts": b.ts,
    }


def outcome_report_to_wire(o: T.OutcomeReport) -> dict[str, Any]:
    return {
        "signal_id": o.signal_id,
        "venue": o.venue.value,
        "symbol": o.symbol,
        "direction": o.direction.value,
        "entry_price": o.entry_price,
        "exit_price": o.exit_price,
        "qty": o.qty,
        "realized_pnl_usd": o.realized_pnl_usd,
        "fees_usd": o.fees_usd,
        "slippage_bps": o.slippage_bps,
        "holding_time_s": o.holding_time_s,
        "mfe_bps": o.mfe_bps,
        "mae_bps": o.mae_bps,
        "close_reason": o.close_reason.value,
        "opened_at": o.opened_at,
        "closed_at": o.closed_at,
        "correlation_id": o.correlation_id,
    }


def alert_to_wire(a: T.AlertEvent) -> dict[str, Any]:
    return {
        "severity": a.severity,
        "code": a.code,
        "message": a.message,
        "ts": a.ts,
        "venue": a.venue.value if a.venue is not None else None,
        "symbol": a.symbol,
    }


def reconcile_diff_to_wire(r: T.ReconcileDiff) -> dict[str, Any]:
    return {
        "venue": r.venue.value,
        "symbol": r.symbol,
        "kind": r.kind,
        "detail": dict(r.detail),
        "ts": r.ts,
    }


def signal_ack_to_wire(a: T.SignalAck) -> dict[str, Any]:
    return {
        "signal_id": a.signal_id,
        "accepted": a.accepted,
        "duplicate": a.duplicate,
        "rejection_reason": a.rejection_reason.value if a.rejection_reason else None,
        "ts": a.ts,
        "correlation_id": a.correlation_id,
    }


# ---------------------------------------------------------------------------
# Single-entry envelope used by the gateway WS feed and the audit log.
# ---------------------------------------------------------------------------


def envelope(event_type: T.EventType, payload: dict[str, Any]) -> dict[str, Any]:
    """Wrap a payload as ``{"type": "...", "payload": {...}}``.

    The wire feed multiplexes events of different shapes onto one
    socket; the envelope makes the type explicit.
    """

    return {"type": event_type.value, "payload": payload}
