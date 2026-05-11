"""Golden-snapshot test for the v1.0 wire shape.

Builds every wire-visible type with deterministic field values, runs
it through ``trade_adapter.serialization.*_to_wire``, and compares the
result against a JSON file in ``tests/protocol/golden/``. Any change
to a field name, ``kind`` tag, enum value, or shape will fail this
test.

Updating goldens
----------------

If a change to the wire shape is *intentional* and approved by spec
review, regenerate the golden files by running with the env var
``UPDATE_GOLDEN=1``::

    UPDATE_GOLDEN=1 pytest tests/protocol/test_schema_lock.py

Then commit the changed JSON files alongside the spec change.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from trade_adapter import serialization as ser
from trade_adapter import types as T

GOLDEN_DIR = Path(__file__).parent / "golden"
UPDATE_GOLDEN = os.environ.get("UPDATE_GOLDEN") == "1"


def _check(name: str, payload: Any) -> None:
    """Compare ``payload`` against ``golden/{name}.json``.

    On mismatch: pytest assertion error showing a unified diff.
    Under ``UPDATE_GOLDEN=1``: write the payload as the new golden.
    """

    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    path = GOLDEN_DIR / f"{name}.json"
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if UPDATE_GOLDEN or not path.exists():
        path.write_text(serialized)
        if not UPDATE_GOLDEN:
            pytest.skip(f"created new golden: {path}")
        return
    expected = path.read_text()
    assert serialized == expected, (
        f"\nwire shape drifted vs golden {path.name}.\n"
        f"set UPDATE_GOLDEN=1 to refresh after a spec change.\n"
        f"--- golden\n{expected}\n--- actual\n{serialized}"
    )


# ---------------------------------------------------------------------------
# Sizing variants
# ---------------------------------------------------------------------------


def test_sizing_fixed_qty_wire() -> None:
    _check("sizing_fixed_qty", ser.sizing_to_wire(T.FixedQty(qty=0.5)))


def test_sizing_notional_usd_wire() -> None:
    _check("sizing_notional_usd", ser.sizing_to_wire(T.NotionalUsd(notional_usd=500.0)))


def test_sizing_pct_equity_wire() -> None:
    _check("sizing_pct_equity", ser.sizing_to_wire(T.PctEquity(pct=2.0)))


def test_sizing_risk_based_wire() -> None:
    _check("sizing_risk_based", ser.sizing_to_wire(T.RiskBased(risk_usd=50.0)))


# ---------------------------------------------------------------------------
# Stop variants
# ---------------------------------------------------------------------------


def test_stop_absolute_wire() -> None:
    _check(
        "stop_absolute",
        ser.stop_to_wire(T.AbsolutePrice(price=64950.0, mode=T.StopMode.NATIVE)),
    )


def test_stop_bps_wire() -> None:
    _check(
        "stop_bps",
        ser.stop_to_wire(T.BpsFromEntry(bps=50, mode=T.StopMode.NATIVE)),
    )


def test_stop_pct_wire() -> None:
    _check(
        "stop_pct",
        ser.stop_to_wire(T.PctFromEntry(pct=0.5, mode=T.StopMode.NATIVE)),
    )


def test_stop_atr_wire() -> None:
    _check(
        "stop_atr",
        ser.stop_to_wire(
            T.AtrMultiple(multiple=2.0, atr_period_seconds=900, mode=T.StopMode.LOCAL)
        ),
    )


# ---------------------------------------------------------------------------
# UniversalSignal: heatmap-sdk UI scenario from SIGNAL_PROTOCOL.md
# ---------------------------------------------------------------------------


def test_universal_signal_heatmap_ui_scenario_wire() -> None:
    sig = T.UniversalSignal(
        signal_id="00000000-0000-0000-0000-000000000001",
        source="heatmap_sdk",
        symbol="BTCUSDT",
        venue=T.Venue.BINANCE_UM,
        direction=T.Direction.LONG,
        intent=T.Intent.OPEN,
        sizing=T.NotionalUsd(notional_usd=500.0),
        sl=T.PctFromEntry(pct=0.5, mode=T.StopMode.NATIVE),
        tp=T.PctFromEntry(pct=1.0, mode=T.StopMode.NATIVE),
        ttl_seconds=3.0,
        correlation_id="00000000-0000-0000-0000-000000000001",
        metadata={"ui_session": "op-1-2026-05"},
    )
    _check("signal_heatmap_ui", ser.signal_to_wire(sig))


def test_universal_signal_round_trip() -> None:
    sig = T.UniversalSignal(
        signal_id="abc",
        source="bot",
        symbol="ETHUSDT",
        venue=T.Venue.BYBIT_LINEAR,
        direction=T.Direction.SHORT,
        intent=T.Intent.OPEN,
        sizing=T.RiskBased(risk_usd=25.0),
        sl=T.BpsFromEntry(bps=80, mode=T.StopMode.NATIVE),
        tp=None,
        ttl_seconds=5.0,
        correlation_id=None,
        metadata={},
    )
    wire = ser.signal_to_wire(sig)
    restored = ser.signal_from_wire(wire)
    assert restored == sig


# ---------------------------------------------------------------------------
# Outbound events
# ---------------------------------------------------------------------------


def test_order_update_wire() -> None:
    u = T.OrderUpdate(
        client_order_id="uta-1",
        exchange_order_id="123456789",
        venue=T.Venue.BINANCE_UM,
        symbol="BTCUSDT",
        status=T.OrderStatus.FILLED,
        filled_qty=0.05,
        avg_fill_price=65000.0,
        ts=1700000000.0,
        signal_id="sig-1",
        correlation_id="corr-1",
        rejection_reason=None,
    )
    _check("order_update", ser.order_update_to_wire(u))


def test_fill_wire() -> None:
    f = T.Fill(
        fill_id="f-1",
        client_order_id="uta-1",
        exchange_order_id="123456789",
        venue=T.Venue.BINANCE_UM,
        symbol="BTCUSDT",
        side=T.OrderSide.BUY,
        qty=0.05,
        price=65000.0,
        fee_usd=0.65,
        is_maker=False,
        ts=1700000000.0,
        signal_id="sig-1",
        correlation_id="corr-1",
    )
    _check("fill", ser.fill_to_wire(f))


def test_position_update_wire() -> None:
    p = T.PositionUpdate(
        venue=T.Venue.BINANCE_UM,
        symbol="BTCUSDT",
        direction=T.Direction.LONG,
        qty=0.05,
        entry_price=65000.0,
        state=T.PositionState.OPEN,
        liquidation_price=58000.0,
        unrealized_pnl_usd=12.5,
        margin_used_usd=325.0,
        ts=1700000000.5,
        signal_id="sig-1",
        correlation_id="corr-1",
    )
    _check("position_update", ser.position_update_to_wire(p))


def test_book_update_wire() -> None:
    b = T.BookUpdate(
        venue=T.Venue.BINANCE_UM,
        symbol="BTCUSDT",
        bids=(T.BookLevel(price=64999.0, qty=1.5), T.BookLevel(price=64998.5, qty=2.0)),
        asks=(T.BookLevel(price=65001.0, qty=1.0), T.BookLevel(price=65001.5, qty=3.0)),
        ts=1700000000.123,
        sequence=42,
    )
    _check("book_update", ser.book_update_to_wire(b))


def test_trade_print_wire() -> None:
    t = T.TradePrint(
        venue=T.Venue.BINANCE_UM,
        symbol="BTCUSDT",
        price=65000.0,
        qty=0.123,
        side=T.OrderSide.SELL,
        ts=1700000000.234,
        trade_id="987654321",
    )
    _check("trade_print", ser.trade_print_to_wire(t))


def test_bbo_update_wire() -> None:
    b = T.BBOUpdate(
        venue=T.Venue.BINANCE_UM,
        symbol="BTCUSDT",
        bid_price=64999.0,
        bid_qty=1.5,
        ask_price=65001.0,
        ask_qty=1.0,
        ts=1700000000.345,
    )
    _check("bbo_update", ser.bbo_update_to_wire(b))


def test_outcome_report_wire() -> None:
    o = T.OutcomeReport(
        signal_id="sig-1",
        venue=T.Venue.BINANCE_UM,
        symbol="BTCUSDT",
        direction=T.Direction.LONG,
        entry_price=65000.0,
        exit_price=65100.0,
        qty=0.05,
        realized_pnl_usd=4.95,
        fees_usd=1.30,
        slippage_bps=2.5,
        holding_time_s=180.0,
        mfe_bps=18.0,
        mae_bps=4.0,
        close_reason=T.CloseReason.TP,
        opened_at=1700000000.0,
        closed_at=1700000180.0,
        correlation_id="corr-1",
    )
    _check("outcome_report", ser.outcome_report_to_wire(o))


def test_alert_wire() -> None:
    a = T.AlertEvent(
        severity="warning",
        code="WS_RECONNECT",
        message="reconnected after 3 attempts",
        ts=1700000000.0,
        venue=T.Venue.BINANCE_UM,
        symbol=None,
    )
    _check("alert", ser.alert_to_wire(a))


def test_reconcile_diff_wire() -> None:
    r = T.ReconcileDiff(
        venue=T.Venue.BINANCE_UM,
        symbol="BTCUSDT",
        kind="qty_mismatch",
        detail={"local_qty": "0.05", "remote_qty": "0.04"},
        ts=1700000000.0,
    )
    _check("reconcile_diff", ser.reconcile_diff_to_wire(r))


def test_signal_ack_accepted_wire() -> None:
    a = T.SignalAck(
        signal_id="sig-1",
        accepted=True,
        duplicate=False,
        rejection_reason=None,
        ts=1700000000.0,
        correlation_id="corr-1",
    )
    _check("signal_ack_accepted", ser.signal_ack_to_wire(a))


def test_signal_ack_rejected_wire() -> None:
    a = T.SignalAck(
        signal_id="sig-2",
        accepted=False,
        duplicate=False,
        rejection_reason=T.RejectionReason.TTL_EXPIRED,
        ts=1700000000.0,
        correlation_id=None,
    )
    _check("signal_ack_rejected", ser.signal_ack_to_wire(a))


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------


def test_envelope_wire() -> None:
    payload = {"foo": "bar"}
    _check("envelope", ser.envelope(T.EventType.FILL, payload))


# ---------------------------------------------------------------------------
# Enum values are part of the wire shape too. Locking them as a set
# means a typo or accidental rename is caught even if nothing else
# uses the renamed enum.
# ---------------------------------------------------------------------------


def test_enum_values() -> None:
    snapshot = {
        "Venue": sorted(v.value for v in T.Venue),
        "Direction": sorted(v.value for v in T.Direction),
        "Intent": sorted(v.value for v in T.Intent),
        "OrderSide": sorted(v.value for v in T.OrderSide),
        "OrderType": sorted(v.value for v in T.OrderType),
        "TimeInForce": sorted(v.value for v in T.TimeInForce),
        "OrderStatus": sorted(v.value for v in T.OrderStatus),
        "PositionState": sorted(v.value for v in T.PositionState),
        "StopMode": sorted(v.value for v in T.StopMode),
        "CloseReason": sorted(v.value for v in T.CloseReason),
        "RejectionReason": sorted(v.value for v in T.RejectionReason),
        "EventType": sorted(v.value for v in T.EventType),
    }
    _check("enums", snapshot)
