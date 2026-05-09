# Universal Trade Adapter — Specification v1.0

> **Status:** Pre-implementation revision (2026-05). The original v1.0
> spec was locked at 15 decisions; an interim revision against
> `heatmap-sdk` integration analysis took it to 17 (new **A3b**, new
> **A16**, rewritten **A1/A3/A8**). A subsequent post-critique cleanup
> raised it to 23: contradictions between latency targets and the
> mandatory-Redis state store / sync audit-log requirement were resolved,
> and previously-implicit safety items (rate-limit pre-throttling, time
> sync, cancel-on-disconnect, backpressure, light-strategy disclosure,
> BBO auto-subscribe for MFE/MAE sampling) were promoted to numbered
> decisions A17–A22. All revisions land **before any implementation has
> shipped**. Once implementation begins, any further change to a
> numbered decision requires a major version bump.

### What changed vs v1.0-original

| # | Decision | Change |
|---|---|---|
| A1 | Stack | Latency target rewritten with concrete per-hop budgets (was "sub-second"); targets gated on the Phase 0 latency bench in [`ROADMAP.md`](ROADMAP.md). |
| A3 | Outward transport | Embedded Python API is now the primary mode; REST/WS gateway is the optional remote-access mode (extras `[gateway]`). |
| A3b | Exchange transport (NEW) | Trading and user-data are WebSocket-first; REST is allowed only for bootstrap, reconciliation, and explicit fallback. |
| A5 | State store | Redis dropped from v1.0 (single-process default uses `asyncio.Queue` + in-process dict cache + `asyncio.Lock`). Redis pub/sub returns later as opt-in extras `[multiproc]`, not as a hard dependency. |
| A8 | SL/TP placement | SL/TP are submitted in a single `place_order` call (bundled where the venue allows; on Binance UM the entry order ships in parallel with a `STOP_MARKET closePosition=true` so the position is never unprotected, even before fill). |
| A14 | Testing | Pure-logic unit tests may use an in-process mock exchange (the prohibition is on a paper-trade *runtime*, not on test doubles). |
| A16 | Market data publish (NEW) | Adapter publishes `book_update` / `trade_print` / `bbo_update` to subscribers so consumers do not open duplicate venue streams. |
| A17 | Rate-limit pre-throttling (NEW) | Token-bucket per `(venue, endpoint-class)`, capped below the venue's documented limit; over-quota sends are rejected locally with `RATE_LIMITED` rather than handed to the exchange. |
| A18 | Time sync (NEW) | Server-time offset measured at startup and refreshed hourly; signed REST requests use `now() + offset` so a drifting host clock never trips the venue's `recvWindow`. |
| A19 | Cancel-on-disconnect (NEW) | Off by default; opt-in per venue (uses native exchange COD where supported). Default keeps positions and child SL/TP alive across adapter restarts (consistent with prohibition C.5). |
| A20 | Backpressure (NEW) | Internal event bus uses bounded `asyncio.Queue` per subscriber with drop-oldest + counter; one slow consumer cannot stall the trading hot path. |
| A21 | Light strategy disclosure (NEW) | The intent resolver and `RiskBased` sizer make small strategy-shaped decisions on the adapter side. They are explicitly listed and behave deterministically; producers can pre-resolve and submit `FixedQty` / explicit intent to bypass them. |
| A22 | BBO auto-subscribe for open positions (NEW) | While a position is open, the adapter auto-subscribes to that symbol's BBO so MFE / MAE in `outcome_report` are computed from real ticks, not the position's own fills. |

A10 (`UniversalSignal`) gains an optional `correlation_id` field — see
[`SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md). New event type
`outcome_report` is added in the same document so the analytics SDK can
close the MAB feedback loop. The audit-log requirement (see Section D.1)
is now async-after-accept rather than sync-before-routing, so SQLite
`fsync` cannot eat into the A1 latency budget.

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
- `websockets` for outbound exchange connections (trading + user-data +
  market data — A3b)
- `aiosqlite` for state-of-record persistence (A5)
- `httpx` for the bootstrap-and-fallback REST client (A3b)
- `cryptography` + `argon2-cffi` for the encrypted keystore (A2)
- `structlog` + `PyYAML` for logging and config
- **No** `pydantic`, `numpy`, `pandas`, `scipy`, or `redis` in the core
  runtime. The base install is 7 wheels and ~12 MB of site-packages —
  this is what `import uta` on a heatmap-sdk-class consumer pulls in.
- `FastAPI` + `uvicorn` + `prometheus-client` + `click` are extras
  `[gateway]` and only land on disk if the operator opts into the
  optional HTTP/WS facade (A3 gateway mode).
- **Latency targets** (operator VPS at 10–30 ms RTT to exchange, warm WS
  connections, p95). These are *targets*, not measured numbers; they
  must be validated by the Phase 0 latency bench (see
  [`ROADMAP.md`](ROADMAP.md)) before Phase 1 begins. If the bench shows
  the target is unreachable on the chosen stack, this section is
  rewritten with the observed numbers — not the implementation
  retro-fit to fictional ones:
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

### A5. State store: SQLite truth + in-process bus and cache

- **SQLite** (via `aiosqlite`) is the single source of truth for:
  - Positions (current and historical)
  - Orders (lifecycle: PENDING → ACK → FILLED / CANCELLED / REJECTED)
  - Fills (immutable, append-only)
  - Signals received (audit log, immutable, written async-after-accept;
    see D.1)
  - Reconciliation events (immutable)
  - Signal idempotency cache (D.6: `signal_id → cached_response` with
    TTL; backed by a short-lived in-memory dict for hot lookups, SQLite
    for durability across restarts)
- **Internal event bus** is an `asyncio.Queue` per subscriber, fed from
  a single in-process publisher. Bounded with drop-oldest backpressure
  (A20) so a slow consumer cannot stall the trading hot path. No
  network hop, no serialization, no second daemon to run.
- **Hot market-data cache** (latest BBO per symbol, last book sequence,
  funding-rate snapshot, etc.) is a plain `dict[str, T]` guarded by
  the per-`(venue, symbol)` `asyncio.Lock` it shares with the position
  manager.
- **Reconciliation mutex** is one `asyncio.Lock` per venue, in-process.
  v1.0 runs as a single process per host (D.10) so distributed
  coordination is unnecessary.
- **Redis is not a v1.0 dependency.** It is reserved for a future
  multi-process deployment mode shipped as extras `[multiproc]` (one
  process per venue, IPC via Redis pub/sub). That path is
  out-of-scope until single-process throughput is shown to be the
  bottleneck — see ROADMAP v2.0.

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
  `place_order` call** wherever the venue allows it:
  - Bybit Linear accepts `stopLoss` / `takeProfit` parameters directly
    on the entry order (V5). The adapter passes them through.
  - Binance USD-M Futures does not bundle natively. The adapter ships
    the entry order **and** a `STOP_MARKET closePosition=true` child
    in parallel from the same `place_order` call (single WS-trade
    multiplexed send). `closePosition=true` makes the stop trigger on
    full position size, so an entry that is still mid-fill is already
    protected. The TP child (`TAKE_PROFIT_MARKET closePosition=true`)
    ships in the same parallel send. If both children's pre-image
    triggers would put them on the wrong side of the entry's eventual
    fill (e.g. SL above ask at the moment of placement) the adapter
    waits for the entry ACK and submits children using the rounded
    fill-side reference, but the *closePosition=true STOP_MARKET* is
    always shipped first so there is no "naked entry" window.
  - The previous wording "submitted on entry-order ACK" implied a tiny
    unprotected window between ACK and fill. With `closePosition=true`
    the window collapses: the stop is live at the moment the position
    becomes non-zero.
- Per-signal override: `sl: { mode: "native" | "local", ... }`.
- Local mode: adapter holds trigger price in memory and emits a market
  close when triggered. Useful for trailing logic that updates
  frequently. Local mode does **not** survive adapter crashes; producers
  who pick local mode accept that risk explicitly.
- Cancel-on-position-close: closing a position must cancel its child
  SL/TP orders. Verified by post-close REST sweep (A9 reconciliation
  path).
- The adapter does **not** advertise that a position is "always
  protected" — it advertises that *if* a venue accepted the entry, the
  protective child was sent in the same multiplexed batch. Network
  partitioning between adapter and venue is observable and surfaces as
  an `alert` event with severity `critical`.

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
5. Reconciliation is wrapped in an in-process `asyncio.Lock` per
   `(venue)` to prevent concurrent runs. v1.0 is single-process
   (D.10), so an in-process lock is sufficient; the `multiproc`
   deployment mode (extras) reintroduces a Redis-backed lock here.

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

### A17. Rate-limit pre-throttling

The adapter holds a token-bucket per `(venue, endpoint-class)` set
below the venue's documented limit (e.g. Binance UM order-rate
`50/10s` → adapter cap `40/10s`; weight-based `2400/min` → adapter cap
`2000/min`). When a send would exceed the bucket the adapter rejects
locally with rejection reason `RATE_LIMITED` and emits a metric
(`uta_rate_limit_local_rejects_total{venue, endpoint_class}`); the
request never reaches the exchange. Bucket sizes live in config and
can be tuned by the operator if their account has elevated limits.

This exists because exchange-side `429` / weight-ban responses are
orders of magnitude more expensive than a local pre-throttle, and a
ban on the user-data WebSocket would defeat the entire latency
budget.

### A18. Time sync

At startup and every hour, the adapter measures the wall-clock
offset between the host and each enabled venue's reported server time
(`GET /fapi/v1/time`, `GET /v5/market/time`). Signed REST and signed
WS-trade requests use `host_now() + venue_offset` for `timestamp`
and stay within `recvWindow=5000ms`.

If any sync sample exceeds `±2000ms` drift relative to NTP, the
adapter logs an `alert` with severity `warning` and refuses to start
until the host clock is corrected. Trading on a host with a broken
clock has been the proximate cause of "my orders are randomly rejected"
incidents and is not a failure mode the adapter will silently mask.

### A19. Cancel-on-disconnect (COD)

Off by default. Opt-in per venue via config:

```yaml
venues:
  binance_um:
    cancel_on_disconnect: false   # default; positions and child SL/TP
                                  # survive adapter restarts.
  bybit_linear:
    cancel_on_disconnect: true    # native COD (Bybit V5 supports it);
                                  # all open orders are cancelled by
                                  # the venue if the WS-trade session
                                  # drops for >cod_window_seconds.
```

With COD off (default), a planned restart leaves working orders and
native SL/TP on the exchange, consistent with prohibition C.5
("never auto-flatten on connection issues"). With COD on, a session
drop becomes a venue-side cancel — useful for short-TTL strategies
where a stale resting order is a liability. The choice is per
operator; the adapter does not pick a default for you.

### A20. Backpressure on the internal event bus

Every in-process subscriber receives events through a bounded
`asyncio.Queue` (default `maxsize=1024`, configurable per subscriber).
When the queue is full the publisher drops the **oldest** queued
event for that subscriber and increments
`uta_event_bus_drops_total{channel, subscriber}`. The trading hot
path (signal router → position manager → exchange WS-trade) cannot
be stalled by a slow market-data consumer.

Dropped events are visible in metrics; the subscriber can decide
to re-fetch state via the public read endpoints if it cares about
gapless history. Market-data subscribers (`book_update`,
`trade_print`, `bbo_update`) get drop-oldest by design — a stale
L2 frame is worthless. Order-state subscribers (`order_update`,
`fill`, `position_update`) use a larger default queue (`8192`) and
should generally not drop in a healthy deployment.

### A21. Light strategy disclosure

The adapter is described as "not a strategy engine," but it does run
two small strategy-shaped pieces of logic on the producer's behalf:

1. **Intent resolution.** `OPEN` against an existing same-direction
   position becomes `ADD`; against opposite-direction becomes
   `REVERSE`. This is deterministic and visible in
   `signal_accepted.intent_resolved` so the producer always knows what
   was actually done. Producers that want to bypass it submit
   `intent=ADD` or `intent=REVERSE` explicitly.
2. **Sizing.** `RiskBased` and `PctEquity` size to the producer's
   constraint at signal-receipt time using the cached BBO and the
   current equity snapshot. Producers that want byte-exact control
   submit `FixedQty`. The exact computation is in
   [`SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md) so it can be reproduced
   off-line for backtesting parity.

No other behavioral logic is on the adapter side. There is no entry
filter, no exit filter, no take-profit ratcheting, no drawdown gate,
no position-correlation rebalancing. Those are the producer's problem.

### A22. BBO auto-subscribe for symbols with open positions

While a `(venue, symbol)` has an open position, the adapter ensures
an internal BBO subscription on that symbol exists (re-using any
shared upstream connection per A16 + prohibition C.11). This drives
the MFE/MAE sampler in `outcome_report` (see
[`SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md)) using *real* mid-prices
at tick cadence, not the position's own fills.

The sampler runs in-process (no extra exchange call). Producers may
still subscribe to the same channel themselves; both subscriptions
share one upstream connection.

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
13. Pull `redis`, `fastapi`, `uvicorn`, `pydantic`, `numpy`, `pandas`,
    or `scipy` into the core embedded import path. Anything
    gateway-only or multiproc-only stays behind extras
    (`[gateway]` / `[multiproc]`).
14. Advertise a safety property the adapter cannot deliver. If a
    network partition can leave a position momentarily unprotected
    (e.g. before a SL ACK on Binance UM in fallback paths), say so —
    do not paper over it in docs or in logs.

---

## Section D — Hard requirements

The implementation MUST:

1. Persist a `signal_received` audit-log entry to SQLite for every
   signal **after** the signal has been validated and dispatched, on a
   background flusher (batched, fsync ≤ 1×/sec). The acceptance path
   never waits on disk. The in-memory idempotency cache (D.6) is the
   authoritative dedup guard during the synchronous accept; the SQLite
   row is the durable record once the event loop returns to idle.
2. Maintain `client_order_id → exchange_order_id` mapping in SQLite.
3. Use `asyncio.Lock` per (venue, symbol) to serialize state mutations.
4. Implement exponential backoff on WS reconnects (1s → 2s → 4s → ... →
   max 30s).
5. Refresh exchange listenKey / wsKey before expiry.
6. Maintain a `signal_id → cached_response` idempotency cache (in-memory
   `dict` with bounded size + SQLite mirror). Same `signal_id` returns
   the cached response within the configured TTL
   (`signal_idempotency_ttl_seconds`, default 3600). After cache
   expiry, the same `signal_id` is treated as a new signal. The
   in-memory layer answers within microseconds; the SQLite mirror
   makes the cache survive an adapter restart.
7. Validate every incoming `UniversalSignal` against schema before
   acceptance.
8. Round order qty and price to exchange-specific tick/step sizes
   automatically. Symbol metadata (`tickSize`, `stepSize`,
   `minNotional`) is fetched lazily on first registration of a symbol
   and refreshed on a configurable interval (default 24 h) and on
   exchange `EXCHANGE_INFO`-changed events; metadata changes do not
   require an adapter restart.
9. Cancel child SL/TP orders when their parent position closes.
10. Run on a single process, single host. No distributed coordination
    required for v1.0. (Multi-process operation requires extras
    `[multiproc]` and is out of scope until single-process throughput
    is shown to be the bottleneck.)
11. Propagate `correlation_id` (when present on the signal) onto every
    derived `OrderRequest`, `OrderUpdate`, `Fill`, `PositionUpdate`,
    and `OutcomeReport` so a producer can thread a signal end-to-end.
12. Emit an `outcome_report` event for every closed position,
    including realized PnL, fees, slippage, holding time, MFE, and MAE
    (see [`SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md)).
13. Validate the A1 latency targets against the operator's host before
    Phase 1 implementation begins (see Phase 0 in
    [`ROADMAP.md`](ROADMAP.md)). If observed numbers diverge, A1 is
    rewritten with the observed numbers and the implementation is
    sized against reality.

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

- [ ] Section A — all 23 decisions (A0–A22, including A3b) match what
      was agreed.
- [ ] Section B — API surface covers required operations.
- [ ] Section C — prohibitions are exhaustive (including the new
      C.13 dependency-floor and C.14 honest-safety-claims rules).
- [ ] Section D — requirements are achievable, including the new
      D.13 latency-bench gate.
- [ ] Section E — out-of-scope items are correctly deferred.
