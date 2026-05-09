# Universal Trade Adapter — Specification v1.0 (LOCKED)

> **Status:** Locked. All 15 decisions have been signed off.
> Departing from any of them requires a major version bump and explicit re-review.

---

## Purpose

A self-hosted, single-process Python adapter that:

1. **Ingests** market data (trades + book snapshots) and private streams
   (fills, orders, positions) from Binance USD-M Futures and Bybit Linear
   via WebSocket.
2. **Accepts** universal `UniversalSignal` commands from external sources
   (analytics SDK, custom bots, UI) via REST API.
3. **Manages** positions per-symbol with a state machine (open / scale /
   reduce / close / reverse).
4. **Executes** orders idempotently with `client_order_id` tracking.
5. **Stores** exchange API keys encrypted on disk.
6. **Reconciles** state aggressively after WebSocket reconnects via REST.
7. **Exposes** stable HTTP + WebSocket API outward, decoupled from
   exchange-specific quirks.

> **It is not a strategy engine.** It executes intents. Strategy logic
> (when to enter, when to exit, how to size) lives in callers.

---

## Section A — Locked decisions

### A0. Separate repository

Lives in a dedicated repo (`universal-trade-adapter`), not as a branch in
the analytics SDK repo. Rationale: API key isolation, independent
versioning, independent deployment lifecycle.

### A1. Stack: Python 3.11+ async

- Python 3.11+ (uses `dataclass(slots=True)`, PEP 673)
- `asyncio` for concurrency
- `FastAPI` for the public HTTP API
- `websockets` for outbound exchange connections
- `uvicorn` for the ASGI server
- `numpy`/`pydantic` only where needed; avoid pandas/scipy
- Latency target: sub-second decisions, NOT sub-millisecond. HFT is out of
  scope for this stack choice.

### A2. API key storage: encrypted file

- Keys live in `~/.universal-trade-adapter/secrets.enc` (path configurable).
- Encryption: `cryptography.fernet` with a key derived from a master
  password using `Argon2id`.
- Master password sourced in priority order:
  1. `UTA_MASTER_PASSWORD` environment variable
  2. Interactive prompt at startup (TTY only)
- Plaintext keys exist only in process memory, never on disk, never in
  logs, never in metrics.
- Memory lifetime: keys are pulled into `bytes` only at the moment of
  signing each REST request; no long-lived `ApiKeyHolder` object holds
  them as plaintext attributes for more than the duration of a single
  HTTP call.

### A3. Outward transport: REST + WebSocket

- **REST (FastAPI)** for commands: `POST /signal`, `POST /order`,
  `POST /position/close`, `GET /positions`, etc.
- **WebSocket (`/events`)** for outbound push: fills, position changes,
  errors, reconnect events.
- Single port, both protocols share authentication and connection
  lifecycle.

### A4. Outward authentication: Bearer token per consumer

- Each consumer (UI, bot, SDK) gets its own `consumer_token` (UUID).
- Tokens stored in the same encrypted file alongside exchange keys.
- Bearer auth on every REST request and on the WebSocket upgrade
  handshake.
- Roles intentionally **out of v1.0** — tokens are equally privileged.
  Roles (read / trade / admin) move to v1.1 if needed.

### A5. State store: SQLite (truth) + Redis (cache + pub/sub)

- **SQLite** is the single source of truth for:
  - Positions (current and historical)
  - Orders (lifecycle: PENDING → ACK → FILLED / CANCELLED / REJECTED)
  - Fills (immutable, append-only)
  - Signals received (audit log, immutable)
  - Reconciliation events (immutable)
- **Redis** is used for:
  - Pub/sub of internal events between async tasks (e.g. ws-fill →
    position-manager → api-broadcaster).
  - Hot cache of latest market data (best bid/ask per symbol) for sizing
    calculations.
  - Distributed lock for "only one reconciliation in flight per
    exchange".
- Redis is treated as ephemeral. Loss of Redis must not corrupt state.
  All state-of-record writes are SQLite first, Redis second.

### A6. Order idempotency: client_order_id

- Every outgoing order has a `client_order_id` (UUID v4).
- On retry (network error, timeout, ambiguous response), the same
  `client_order_id` is sent again. Both Binance (`newClientOrderId`) and
  Bybit (`orderLinkId`) reject duplicates with a specific error code we
  treat as success.
- After three retries with no resolution, the order is marked
  `AMBIGUOUS` and a manual reconciliation is enqueued.

### A7. Position model: Net only (one-way mode)

- The adapter operates in one-way mode on both venues:
  - Binance: `dualSidePosition=false`
  - Bybit: `positionMode=MergedSingle`
- A symbol has at most one position at a time; intent `OPEN` in opposite
  direction of an existing position becomes `REVERSE`.
- **Hedge mode (long+short on same symbol simultaneously) is not
  supported in v1.0.** Strategies needing hedge mode (e.g. funding
  capture with simultaneous spot/perp legs across symbols) require v2.0.

### A8. SL/TP placement: native exchange stop-orders, configurable

- Default: SL is placed as a native exchange stop-order immediately upon
  entry fill confirmation.
- Default: TP is also a native stop-order (TAKE_PROFIT_MARKET) by default.
- Per-signal override: `sl: { mode: "native" | "local", ... }`
- Native mode: stop sits on the exchange. If the adapter dies, exchange
  still closes the position. Required for production.
- Local mode: adapter holds trigger price in memory and emits market
  close when triggered. Useful for trailing logic that updates frequently.
- Cancel-on-position-close: closing a position must cancel its child SL/TP
  orders. Verified by post-close REST sweep.

### A9. Reconnect: aggressive REST reconciliation

After every WebSocket reconnect (private user data stream):

1. Fetch all open orders from exchange via REST.
2. Fetch all positions from exchange via REST.
3. Diff against local SQLite state.
4. For every diff:
   - Local has order, exchange does not → mark order CANCELLED locally,
     emit `OrderCancelled` event with `cause=reconciliation`.
   - Exchange has order, local does not → log warning, ingest as
     `external_order` (no position-manager intent), emit alert.
   - Position size mismatch → trust exchange, update local, emit alert.
5. Reconciliation is wrapped in a Redis-backed mutex per exchange to
   prevent concurrent runs.

### A10. Signal format: `UniversalSignal` dataclass

Full schema in [`SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md). Skeleton:

```python
@dataclass(slots=True, frozen=True)
class UniversalSignal:
    signal_id: str            # UUID v4
    source: str               # "adaptive_sdk" | "manual_ui" | "external_bot" | <freeform>
    received_at: float        # adapter-side timestamp
    symbol: str               # "BTCUSDT"
    venue: str                # "binance_um" | "bybit_linear"
    direction: Direction      # LONG | SHORT
    intent: Intent            # OPEN | ADD | REDUCE | CLOSE | REVERSE
    sizing: SizingSpec        # {fixed_qty | pct_equity | risk_based}
    sl: Optional[StopSpec]    # absolute price, bps, ATR multiple, or None
    tp: Optional[StopSpec]
    ttl_seconds: float        # signal expires if not actioned within TTL
    metadata: dict[str, str]  # opaque, never used by logic
```

### A11. Sizing: all three modes

`SizingSpec` is a tagged union accepting:

- `FixedQty`: literal quantity in base asset (e.g. `qty=0.5`)
- `PctEquity`: percentage of total equity (e.g. `pct=2.0` → 2%)
- `RiskBased`: `risk_usd / sl_distance_bps` — sized so a stop-out costs
  exactly `risk_usd`. Requires `sl` to be set on the signal.

Only one of the three is set per signal.

### A12. Risk: emergency kill switch only

The adapter is NOT a risk manager. Strategy-level risk lives in callers.
But two **hardcoded emergency limits** prevent orders-of-magnitude bugs:

- `max_single_order_notional_usd` — default 50% of total equity.
  Orders exceeding this are rejected before being sent to the exchange.
- `max_leverage` — default 50×. Symbols with leverage above this in
  exchange settings will fail to register.

Both can be overridden in config: `risk_circuit_breakers_enabled: false`.
Default is `true`.

These exist to catch "added an extra zero" bugs in the strategy code.
They are not a substitute for proper risk management.

### A13. Exchange differences: hidden behind abstraction

The public API speaks one universal vocabulary. Exchange-specific quirks
are confined to `trade_adapter/exchanges/<venue>/`:

- Tick size & qty step rounding handled inside the exchange adapter.
- Hedge/one-way mode setup done at adapter init per venue.
- Error code mapping: every venue's error vocabulary is mapped to a
  shared `ExchangeError` enum (`RATE_LIMITED`, `INSUFFICIENT_FUNDS`,
  `INVALID_QTY`, `MARKET_CLOSED`, `UNKNOWN`).
- Private WebSocket message shapes normalized to internal `Fill`,
  `OrderUpdate`, `PositionUpdate` dataclasses.

Adding a third venue must not require any changes outside
`trade_adapter/exchanges/<venue>/`.

### A14. Testing: real exchanges only

- No testnet integration.
- No mock exchange.
- No paper trading mode.
- Unit tests cover pure logic (signal validation, sizing math,
  state-machine transitions).
- Integration with real exchanges is verified by hand on a small ($50)
  account.

### A15. Observability: structlog JSON + Prometheus

- All logs go through `structlog` to stdout in JSON format.
- Sensitive fields (API keys, master password, full Bearer tokens) are
  scrubbed by a global `structlog` processor before any log emission.
- Prometheus metrics on `GET /metrics`:
  - `uta_orders_total{venue, side, type, status}`
  - `uta_signals_total{source, intent}`
  - `uta_position_count{venue}`
  - `uta_ws_reconnects_total{venue, stream}`
  - `uta_reconcile_diffs_total{venue, kind}`
  - `uta_emergency_rejects_total{reason}`
  - `uta_request_latency_seconds{venue, method}` (histogram)
- No OpenTelemetry traces in v1.0.

---

## Section B — Public HTTP API

All endpoints require `Authorization: Bearer <consumer_token>`. Errors
return JSON `{error: {code, message, details}}` with appropriate HTTP
status.

### `POST /v1/signal`

Submit a `UniversalSignal`. Body matches the `UniversalSignal` schema.
Response:

```json
{
  "accepted": true,
  "signal_id": "...",
  "intent_resolved_to": "OPEN",
  "expected_qty": 0.5,
  "expected_notional_usd": 32500.0
}
```

Or, if rejected:

```json
{
  "accepted": false,
  "signal_id": "...",
  "rejection_reason": "EMERGENCY_NOTIONAL_LIMIT",
  "details": "..."
}
```

### `POST /v1/order`

Bypass the signal layer and submit a raw order. Requires explicit
`override_acknowledged=true` in body. Discouraged in production; provided
for manual ops.

### `POST /v1/position/close`

```json
{ "symbol": "BTCUSDT", "venue": "binance_um", "reduce_pct": 100 }
```

### `GET /v1/positions`

Returns array of current positions across all venues.

### `GET /v1/orders?status=open`

Returns active orders.

### `GET /v1/balances`

Returns balances per venue.

### `GET /v1/health`

Liveness + readiness:

```json
{
  "status": "ready",
  "venues": {
    "binance_um": { "ws": "connected", "rest": "ok", "last_event_ts": 1700000000.0 },
    "bybit_linear": { "ws": "connected", "rest": "ok", "last_event_ts": 1700000000.0 }
  }
}
```

### `GET /metrics`

Prometheus scrape endpoint. Not authenticated by default; bind to
loopback only.

### `WS /v1/events`

Server pushes JSON messages of the form:

```json
{ "type": "fill", "ts": 1700000000.0, "data": { ... } }
{ "type": "order_update", ... }
{ "type": "position_update", ... }
{ "type": "alert", "severity": "warning", ... }
{ "type": "reconcile_diff", ... }
```

---

## Section C — Hard prohibitions

The implementation MUST NOT:

1. Log API keys or master password under any conditions, including
   exception traces.
2. Persist plaintext API keys to disk.
3. Send orders without a `client_order_id`.
4. Operate in hedge mode on either venue (v1.0).
5. Auto-flatten positions on connection issues (let exchange handle via
   native stops; never auto-close from local logic on disconnect).
6. Trust local state over exchange state during reconciliation.
7. Expose `/metrics` to public network without explicit override.
8. Skip emergency kill-switch checks unless config-disabled.
9. Use `pickle` for any state persistence (use SQLite + JSON only).
10. Block the asyncio event loop with synchronous file/network I/O.

---

## Section D — Hard requirements

The implementation MUST:

1. Emit a `signal_received` audit-log entry to SQLite for every signal
   before any processing.
2. Maintain `client_order_id → exchange_order_id` mapping in SQLite.
3. Use `asyncio.Lock` per (venue, symbol) to serialize state mutations.
4. Implement exponential backoff on WS reconnects (1s → 2s → 4s → ... →
   max 30s).
5. Refresh exchange listenKey / wsKey before expiry.
6. Validate every incoming `UniversalSignal` against schema before
   acceptance.
7. Round order qty and price to exchange-specific tick/step sizes
   automatically.
8. Cancel child SL/TP orders when their parent position closes.
9. Survive Redis outage without corrupting SQLite state.
10. Run on a single process, single host. No distributed coordination
    required for v1.0.

---

## Section E — Out of scope (v1.0)

Explicitly excluded; revisit in v1.x or v2.0:

- Hedge mode (long + short same symbol)
- Spot trading (futures only in v1.0)
- More than two venues
- OCO orders (One-Cancels-Other) — implement as two child orders
- Trailing stops with native exchange support (use local trailing in
  v1.0)
- Multi-account (one account per venue in v1.0)
- Cross-venue arbitrage (signals are scoped to one venue)
- Strategy auto-tuning, hyperparameter optimization
- HFT-grade latency optimization (Rust hot-path, kernel bypass, etc.)

---

## Section F — Begin instructions

Implementation begins when this spec is signed off. The implementation
work is broken into phases per [`ROADMAP.md`](ROADMAP.md). Spec review
checklist:

- [ ] Section A — all 15 decisions match what was agreed.
- [ ] Section B — API surface covers required operations.
- [ ] Section C — prohibitions are exhaustive.
- [ ] Section D — requirements are achievable.
- [ ] Section E — out-of-scope items are correctly deferred.
