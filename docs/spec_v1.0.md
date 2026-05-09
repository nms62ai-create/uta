# Universal Trade Adapter — Specification v1.0

> **Status:** Pre-implementation revision (2026-05). The original v1.0
> spec was locked at 15 decisions; this revision is being applied **before
> any implementation has shipped** in response to integration analysis
> with `heatmap-sdk` (the first real consumer). The decision count moves
> from 15 to 17 (new **A3b** Exchange transport, new **A16** Market data
> publish). **A1**, **A3**, **A8** were rewritten; the rest of the
> locked decisions are unchanged. Once implementation begins, any further
> change to a numbered decision requires a major version bump.

### What changed vs v1.0-original

| # | Decision | Change |
|---|---|---|
| A1 | Stack | Latency target rewritten with concrete per-hop budgets (was "sub-second"). |
| A3 | Outward transport | Embedded Python API is now the primary mode; REST/WS gateway is the optional remote-access mode. |
| A3b | Exchange transport (NEW) | Trading and user-data are WebSocket-first; REST is allowed only for bootstrap, reconciliation, and explicit fallback. |
| A8 | SL/TP placement | SL/TP are submitted in a single `place_order` call (bundled where the venue allows; child-on-ACK on Binance UM). |
| A16 | Market data publish (NEW) | Adapter publishes `book_update` / `trade_print` / `bbo_update` to subscribers so consumers do not open duplicate venue streams. |

A10 (`UniversalSignal`) gains an optional `correlation_id` field — see
[`SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md). New event type
`outcome_report` is added in the same document so the analytics SDK can
close the MAB feedback loop.

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
- `FastAPI` for the optional gateway HTTP/WS API (see A3)
- `websockets` for outbound exchange connections
- `uvicorn` for the ASGI server (gateway mode)
- `numpy`/`pydantic` only where needed; avoid pandas/scipy
- **Latency targets** (operator VPS at 10–30 ms RTT to exchange, warm WS
  connections, p95):
  - `≤ 25 ms` from `place_order()` call to exchange `order ACK`.
  - `≤ 10 ms` from a private/public WS event arriving on the wire to
    delivery to an in-process subscriber.
  - `≤ 5 ms` adapter-added overhead on top of network RTT for any
    single hop (sign + send, parse + dispatch).
  - In gateway mode, add one local loopback hop and JSON
    (de)serialization on top of the embedded numbers.
- HFT-class sub-millisecond latency (kernel bypass, colo, C++/Rust
  hot-path) is explicitly out of scope; the wide-area network is the
  bottleneck on this stack and the adapter's own overhead is engineered
  to stay below it.

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

### A3. Outward transport: embedded-first, REST/WS gateway optional

The adapter ships as a Python package and supports two interchangeable
modes against the same core:

- **Embedded mode (default).** Consumers in the same Python process
  (`heatmap-sdk`, custom strategies, the Adaptive Analytics SDK) import
  `uta.TradeAdapter` directly. Commands are method calls
  (`place_order`, `cancel_order`, `close_position`); events are
  `async` iterators (`subscribe_orders`, `subscribe_positions`,
  `subscribe_book`, `subscribe_trades`, `subscribe_bbo`,
  `subscribe_outcomes`). No serialization, no extra hop.
- **Gateway mode (optional).** A FastAPI server wraps the same core and
  exposes:
  - REST: `POST /v1/signal`, `POST /v1/order`,
    `POST /v1/position/close`, `GET /v1/positions`, `GET /v1/orders`,
    `GET /v1/balances`, `GET /v1/health`.
  - WebSocket: `WS /v1/events` for fan-out of every event channel with
    per-subscription filters by `(type, venue, symbol)`.
  - Single port, shared authentication and connection lifecycle.

  Gateway mode is for remote consumers (other-language bots, multi-host
  deployments). It is not on the latency path of in-process consumers.

Both modes use the same data contracts. A consumer authored against the
embedded API can be moved behind the gateway without changing the
event schema it observes.

### A3b. Exchange transport: WebSocket-first

For both venues, the adapter uses WebSocket for trading and user-data,
not REST:

- **Trading.** Order placement and cancellation go over the venue's
  trading WebSocket:
  - Binance USD-M Futures: `wss://ws-fapi.binance.com/ws-fapi/v1`,
    methods `order.place` / `order.cancel`.
  - Bybit Linear: `wss://stream.bybit.com/v5/trade`,
    ops `order.create` / `order.cancel`.
- **User-data.** Fills, order updates, and position updates come from
  the venue's private WebSocket:
  - Binance: listenKey-based user-data stream
    (`wss://fstream.binance.com/ws/{listenKey}`), `ORDER_TRADE_UPDATE`
    and `ACCOUNT_UPDATE`.
  - Bybit: private V5 stream
    (`wss://stream.bybit.com/v5/private`), topics `order`,
    `execution`, `position`.
- **Market data.** Public depth + trade streams over WebSocket
  (Binance `@depth@100ms` + `@aggTrade`, Bybit
  `orderbook.{depth}.{symbol}` + `publicTrade.{symbol}`).

REST is allowed only for:

1. One-time bootstrap: order-book snapshot
   (Binance `/fapi/v1/depth?limit=1000`), exchange info (`tickSize`,
   `stepSize`), listenKey lifecycle, position-mode setup.
2. Reconciliation after WebSocket reconnect (decision A9).
3. Explicit fallback when a WS channel has been unavailable beyond a
   configurable degradation timeout. Every fallback REST send is logged
   with `transport=rest_fallback` and counted by an explicit metric
   counter (see A15).

Steady-state trading and event ingest do not depend on REST. There is
no periodic REST polling of positions or orders in normal operation.

### A4. Outward authentication: Bearer token per consumer

- Each consumer (UI, bot, SDK) gets its own `consumer_token` (UUID).
- Tokens stored in the same encrypted file alongside exchange keys.
- Bearer auth on every REST request and on the WebSocket upgrade
  handshake.
- Roles intentionally **out of v1.0** — tokens are equally privileged.
  Roles (read / trade / admin) move to v1.1 if needed.
- Embedded mode does not authenticate — it runs in the same trust
  domain as the consumer. Consumer tokens apply only to gateway mode.

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

### A8. SL/TP placement: native, attached to the entry order

- Default mode is **native**: SL and TP live on the exchange so they
  survive an adapter crash.
- Default delivery is **bundled with the entry order in a single
  `place_order` call**:
  - Bybit Linear accepts `stopLoss` / `takeProfit` parameters directly
    on the entry order (V5). The adapter passes them through.
  - Binance USD-M Futures does not bundle natively; the adapter
    submits child `STOP_MARKET` / `TAKE_PROFIT_MARKET` orders
    immediately on entry-order ACK (not on fill), so a network pause
    between ACK and fill cannot leave the position unprotected. Child
    orders are `reduceOnly=true` and use `closePosition=true` where
    applicable.
- Per-signal override: `sl: { mode: "native" | "local", ... }`.
- Local mode: adapter holds trigger price in memory and emits a market
  close when triggered. Useful for trailing logic that updates frequently.
- Cancel-on-position-close: closing a position must cancel its child
  SL/TP orders. Verified by post-close REST sweep (A9 reconciliation
  path).

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
  - `uta_order_send_latency_seconds{venue, transport}` (histogram of
    `place_order` → exchange ACK; `transport ∈ {ws, rest_fallback}`)
  - `uta_event_dispatch_latency_seconds{kind}` (histogram of WS
    on-wire → in-process subscriber for `fill`, `book_update`,
    `trade_print`)
  - `uta_rest_fallback_total{venue, op}` (counter; should stay near
    zero in steady state)
- No OpenTelemetry traces in v1.0.

### A16. Market data publish

The adapter publishes raw market data to internal subscribers (embedded
mode) and to opted-in gateway consumers. This exists so heatmap-sdk-class
consumers do not have to open their own WebSocket to the same
`(venue, symbol)`.

Channels:

- `book_update` — top-N levels of the book (default `N=50`) or per-level
  deltas, depending on subscription type. Carries `ts_event` (exchange
  timestamp), `ts_recv` (adapter receive timestamp), `seq`.
- `trade_print` — single-trade events with `price`, `qty`, aggressor
  side (`is_buyer_maker` semantics), `ts_event`, `ts_recv`.
- `bbo_update` — best bid/ask snapshot with sizes; throttled (default
  10 ms) for cache-driven sizing math.

Subscription is per `(venue, symbol)`. The adapter coalesces multiple
in-process subscribers onto a single venue WebSocket connection per
`(venue, symbol)` — no duplicate exchange streams.

In gateway mode the same channels are exposed on `WS /v1/events` with
filters; consumers opt in explicitly to avoid bandwidth blowup.

Market-data publish is decoupled from trading: an embedded consumer
that only needs the book or trade tape can use the adapter without
ever calling `place_order`.

---

## Section B — Public HTTP API

All endpoints require `Authorization: Bearer <consumer_token>`. Errors
return JSON `{error: {code, message, details}}` with appropriate HTTP
status.

Endpoints below describe the **gateway mode** surface (A3). Embedded
mode exposes the same operations as Python method calls and async
iterators on `uta.TradeAdapter`; see [`ARCHITECTURE.md`](ARCHITECTURE.md).

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
{ "type": "book_update", "data": { ... } }
{ "type": "trade_print", "data": { ... } }
{ "type": "bbo_update", "data": { ... } }
{ "type": "outcome_report", "data": { ... } }
```

Market data channels (`book_update`, `trade_print`, `bbo_update`) are
opt-in per connection: consumers send a `subscribe` message specifying
`{venue, symbol, channels}` to receive them. See A16 and
[`SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md).

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
11. Open more than one venue WebSocket per `(venue, symbol)` for the
    same channel type (multiple subscribers must share one connection).
12. Send orders or cancellations over REST in steady state — REST is
    bootstrap, reconciliation, and explicit fallback only (A3b).

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
11. Propagate `correlation_id` (when present on the signal) onto every
    derived `OrderRequest`, `OrderUpdate`, `Fill`, `PositionUpdate`,
    and `OutcomeReport` so a producer can thread a signal end-to-end.
12. Emit an `outcome_report` event for every closed position,
    including realized PnL, fees, slippage, holding time, MFE, and MAE
    (see [`SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md)).

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

- [ ] Section A — all 17 decisions (A0–A16, including A3b) match what
      was agreed.
- [ ] Section B — API surface covers required operations.
- [ ] Section C — prohibitions are exhaustive.
- [ ] Section D — requirements are achievable.
- [ ] Section E — out-of-scope items are correctly deferred.
