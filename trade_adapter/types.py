"""Wire-protocol types.

Pure dataclasses + enums. No I/O, no logic. Every type that travels
between a producer (a UI, a strategy bot, any external script) and
the adapter — and every event the adapter publishes back — is
defined here. The shape is locked by
`tests/protocol/test_schema_lock.py`; any change to a field name or
kind-tag is a v1.0 protocol break.

Decisions referenced (see ``docs/spec_v1.0.md``):

- A10 — ``UniversalSignal`` shape, including ``correlation_id``.
- A11 — four sizing modes: ``FixedQty`` / ``NotionalUsd`` /
        ``PctEquity`` / ``RiskBased``.
- A16 — market-data passthrough events: ``BookUpdate`` /
        ``TradePrint`` / ``BBOUpdate``.
- A22 — ``OutcomeReport`` carries ``mfe`` / ``mae`` sampled from BBO
        ticks while the position was open.

Wire format is JSON (see ``docs/SIGNAL_PROTOCOL.md`` for examples).
Adapter-side dataclasses are ``slots=True, frozen=True`` so that they
are cheap to construct, hashable where applicable, and immutable once
crossed into the trading hot path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

PROTOCOL_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Venue(StrEnum):
    """Supported execution venues."""

    BINANCE_UM = "binance_um"
    BYBIT_LINEAR = "bybit_linear"


class Direction(StrEnum):
    """Position direction expressed on the signal."""

    LONG = "LONG"
    SHORT = "SHORT"


class Intent(StrEnum):
    """Producer-supplied intent. Resolved against current position state."""

    OPEN = "OPEN"
    ADD = "ADD"
    REDUCE = "REDUCE"
    CLOSE = "CLOSE"
    REVERSE = "REVERSE"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


class TimeInForce(StrEnum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTX = "GTX"  # post-only


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    ACK = "ACK"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    AMBIGUOUS = "AMBIGUOUS"


class PositionState(StrEnum):
    """Per-(venue, symbol) state machine. Owned by the position manager."""

    IDLE = "IDLE"
    OPENING = "OPENING"
    OPEN = "OPEN"
    ADDING = "ADDING"
    REDUCING = "REDUCING"
    CLOSING = "CLOSING"
    RECONCILING = "RECONCILING"


class StopMode(StrEnum):
    """How a stop-loss / take-profit is enforced."""

    NATIVE = "native"
    LOCAL = "local"


class CloseReason(StrEnum):
    SL = "sl"
    TP = "tp"
    MANUAL = "manual"
    SIGNAL = "signal"
    RECONCILE = "reconcile"
    LIQUIDATION = "liquidation"


class RejectionReason(StrEnum):
    """Reasons the adapter declines a signal at the accept boundary."""

    TTL_EXPIRED = "TTL_EXPIRED"
    SIZING_REQUIRES_SL = "SIZING_REQUIRES_SL"
    INVALID_INTENT = "INVALID_INTENT"
    DUPLICATE_SIGNAL_ID = "DUPLICATE_SIGNAL_ID"
    UNKNOWN_SYMBOL = "UNKNOWN_SYMBOL"
    UNKNOWN_VENUE = "UNKNOWN_VENUE"
    EMERGENCY_LIMIT_EXCEEDED = "EMERGENCY_LIMIT_EXCEEDED"
    RATE_LIMITED = "RATE_LIMITED"
    SCHEMA = "SCHEMA"


class EventType(StrEnum):
    """Event-bus topics. Used by the embedded API and gateway alike."""

    SIGNAL_RECEIVED = "signal_received"
    ORDER_UPDATE = "order_update"
    FILL = "fill"
    POSITION_UPDATE = "position_update"
    BOOK_UPDATE = "book_update"
    TRADE_PRINT = "trade_print"
    BBO_UPDATE = "bbo_update"
    OUTCOME_REPORT = "outcome_report"
    ALERT = "alert"
    RECONCILE_DIFF = "reconcile_diff"


# ---------------------------------------------------------------------------
# Sizing variants (A11)
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class FixedQty:
    """Literal quantity in base asset (rounded to ``stepSize``)."""

    qty: float


@dataclass(slots=True, frozen=True)
class NotionalUsd:
    """Literal USD-equivalent notional.

    Adapter divides by the cached BBO mid (or mark) at signal-receipt
    time and rounds the resulting ``qty`` to the venue's ``stepSize``.
    Natural mode for UI flows where the operator types a dollar amount.
    """

    notional_usd: float


@dataclass(slots=True, frozen=True)
class PctEquity:
    """Percentage of total equity (account-wide for the venue)."""

    pct: float


@dataclass(slots=True, frozen=True)
class RiskBased:
    """Sized so a stop-out costs exactly ``risk_usd``. Requires ``sl``."""

    risk_usd: float


SizingSpec = FixedQty | NotionalUsd | PctEquity | RiskBased


# ---------------------------------------------------------------------------
# Stop variants (used for both SL and TP)
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class AbsolutePrice:
    price: float
    mode: StopMode = StopMode.NATIVE


@dataclass(slots=True, frozen=True)
class BpsFromEntry:
    """Distance from entry in basis points (1 bp = 0.01%)."""

    bps: int
    mode: StopMode = StopMode.NATIVE


@dataclass(slots=True, frozen=True)
class PctFromEntry:
    """Distance from entry in percent. Ergonomic alias for ``BpsFromEntry``."""

    pct: float
    mode: StopMode = StopMode.NATIVE


@dataclass(slots=True, frozen=True)
class AtrMultiple:
    multiple: float
    atr_period_seconds: int
    mode: StopMode = StopMode.NATIVE


StopSpec = AbsolutePrice | BpsFromEntry | PctFromEntry | AtrMultiple


# ---------------------------------------------------------------------------
# Inbound: UniversalSignal
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class UniversalSignal:
    """Single command from a producer to the adapter."""

    signal_id: str
    source: str
    symbol: str
    venue: Venue
    direction: Direction
    intent: Intent
    sizing: SizingSpec
    sl: StopSpec | None
    tp: StopSpec | None
    ttl_seconds: float
    correlation_id: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Outbound: order / fill / position
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class OrderRequest:
    """Adapter-internal: the concrete order the adapter intends to send.

    Derived from a ``UniversalSignal`` after intent resolution and
    sizing computation. Carries the producer's ``correlation_id``
    (D, point 11) so a producer can thread one signal end-to-end.
    """

    client_order_id: str
    venue: Venue
    symbol: str
    side: OrderSide
    order_type: OrderType
    qty: float
    price: float | None
    stop_price: float | None
    time_in_force: TimeInForce
    reduce_only: bool
    close_position: bool
    signal_id: str
    correlation_id: str | None = None


@dataclass(slots=True, frozen=True)
class OrderAck:
    """Synchronous result of submitting an ``OrderRequest`` to a venue."""

    client_order_id: str
    exchange_order_id: str
    venue: Venue
    symbol: str
    accepted_at: float


@dataclass(slots=True, frozen=True)
class OrderUpdate:
    client_order_id: str
    exchange_order_id: str | None
    venue: Venue
    symbol: str
    status: OrderStatus
    filled_qty: float
    avg_fill_price: float | None
    ts: float
    signal_id: str | None = None
    correlation_id: str | None = None
    rejection_reason: str | None = None


@dataclass(slots=True, frozen=True)
class Fill:
    fill_id: str
    client_order_id: str
    exchange_order_id: str
    venue: Venue
    symbol: str
    side: OrderSide
    qty: float
    price: float
    fee_usd: float
    is_maker: bool
    ts: float
    signal_id: str | None = None
    correlation_id: str | None = None


@dataclass(slots=True, frozen=True)
class Position:
    """Snapshot of a position at a point in time."""

    venue: Venue
    symbol: str
    direction: Direction
    qty: float
    entry_price: float
    state: PositionState
    liquidation_price: float | None
    unrealized_pnl_usd: float | None
    margin_used_usd: float | None
    opened_at: float | None


@dataclass(slots=True, frozen=True)
class PositionUpdate:
    """Event emitted whenever a position transitions or changes size."""

    venue: Venue
    symbol: str
    direction: Direction
    qty: float
    entry_price: float
    state: PositionState
    liquidation_price: float | None
    unrealized_pnl_usd: float | None
    margin_used_usd: float | None
    ts: float
    signal_id: str | None = None
    correlation_id: str | None = None


# ---------------------------------------------------------------------------
# Market data (A16)
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class BookLevel:
    price: float
    qty: float


@dataclass(slots=True, frozen=True)
class BookUpdate:
    """Top-N order book snapshot (after coalescing from venue diffs)."""

    venue: Venue
    symbol: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    ts: float
    sequence: int


@dataclass(slots=True, frozen=True)
class TradePrint:
    """One taker print on the public tape."""

    venue: Venue
    symbol: str
    price: float
    qty: float
    side: OrderSide
    ts: float
    trade_id: str


@dataclass(slots=True, frozen=True)
class BBOUpdate:
    """Best bid + best ask snapshot."""

    venue: Venue
    symbol: str
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    ts: float


# ---------------------------------------------------------------------------
# Outcome / alerts / reconcile
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class OutcomeReport:
    """Per-position summary emitted on close (A22).

    ``mfe`` / ``mae`` are computed from BBO ticks the adapter
    auto-subscribed to while the position was open, *not* from the
    position's own fills (so they reflect the real envelope of the
    market, not just the trader's executions).
    """

    signal_id: str
    venue: Venue
    symbol: str
    direction: Direction
    entry_price: float
    exit_price: float
    qty: float
    realized_pnl_usd: float
    fees_usd: float
    slippage_bps: float
    holding_time_s: float
    mfe_bps: float
    mae_bps: float
    close_reason: CloseReason
    opened_at: float
    closed_at: float
    correlation_id: str | None = None


@dataclass(slots=True, frozen=True)
class AlertEvent:
    """Operator-facing alert (e.g. clock drift, repeated rate-limit)."""

    severity: str  # "info" | "warning" | "critical"
    code: str
    message: str
    ts: float
    venue: Venue | None = None
    symbol: str | None = None


@dataclass(slots=True, frozen=True)
class ReconcileDiff:
    """One discrepancy found by the post-reconnect REST sweep."""

    venue: Venue
    symbol: str
    kind: str  # e.g. "missing_local_position", "qty_mismatch", "orphaned_stop"
    detail: dict[str, str]
    ts: float


# ---------------------------------------------------------------------------
# Submit response
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class SignalAck:
    """Synchronous response to ``submit_signal``.

    The adapter classifies the signal at the accept boundary into one
    of three terminal states: ``accepted`` (will be acted on),
    ``rejected`` (with reason), or ``duplicate`` (a previous identical
    ``signal_id`` already had a response — replayed from the
    idempotency cache, see D.6).
    """

    signal_id: str
    accepted: bool
    duplicate: bool
    rejection_reason: RejectionReason | None
    ts: float
    correlation_id: str | None = None
