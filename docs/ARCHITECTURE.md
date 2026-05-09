# Architecture

Block-by-block view of the adapter. For the locked contract see
[`spec_v1.0.md`](spec_v1.0.md).

---

## Top-level diagram

The adapter exposes the same core through two transports (decision A3):
an in-process Python API for embedded consumers and an optional FastAPI
gateway for remote consumers.

```
        In-process embedded consumers              Remote consumers
   ┌──────────────┐  ┌──────────────┐         ┌──────────────────┐
   │ heatmap-sdk  │  │ Adaptive SDK │         │ External bot /   │
   │ + UI         │  │ (numpy core) │         │ other-language   │
   └──────┬───────┘  └──────┬───────┘         └────────┬─────────┘
          │ method calls    │ method calls              │ HTTPS / WSS
          │ + async iters   │ + async iters             │ Bearer token
          └────────┬────────┴───────────────┐           │
                   ▼                        │           ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │                Universal Trade Adapter (one process)             │
   │                                                                  │
   │  ┌─────────────────────────────────────────────────────────────┐ │
   │  │  trade_adapter/embedded/   (default API, A3)                │ │
   │  │   place_order / cancel_order / close_position               │ │
   │  │   subscribe_orders / subscribe_positions / subscribe_fills  │ │
   │  │   subscribe_book / subscribe_trades / subscribe_bbo  (A16)  │ │
   │  │   subscribe_outcomes / subscribe_alerts                     │ │
   │  └─────────────────────────────────────────────────────────────┘ │
   │  ┌─────────────────────────────────────────────────────────────┐ │
   │  │  trade_adapter/gateway/    (optional FastAPI + WS, A3)      │ │
   │  │   POST /v1/signal      POST /v1/order                       │ │
   │  │   POST /v1/position/close                                   │ │
   │  │   GET  /v1/positions, /v1/orders, /v1/balances              │ │
   │  │   GET  /v1/health      GET  /metrics                        │ │
   │  │   WS   /v1/events    ← fan-out + opt-in market data         │ │
   │  └────────────────────────┬────────────────────────────────────┘ │
   │                           │  UniversalSignal                     │
   │                           ▼                                      │
   │  ┌─────────────────────────────────────────────────────────────┐ │
   │  │  trade_adapter/core/signal_router.py                        │ │
   │  │   - validates schema                                        │ │
   │  │   - resolves intent (OPEN vs ADD vs REVERSE) using current  │ │
   │  │     position state                                          │ │
   │  │   - computes target qty from SizingSpec (queries balance)   │ │
   │  │   - checks emergency kill switch (max notional, max lev)    │ │
   │  │   - emits IntentResolved → position manager                 │ │
   │  └────────────────────────┬────────────────────────────────────┘ │
   │                           │  IntentResolved                      │
   │                           ▼                                      │
   │  ┌─────────────────────────────────────────────────────────────┐ │
   │  │  trade_adapter/core/position_manager.py                     │ │
   │  │   per-symbol state machine (asyncio.Lock per symbol)        │ │
   │  │     IDLE → OPENING → OPEN → REDUCING → CLOSING → IDLE       │ │
   │  │   - emits OrderRequest                                      │ │
   │  │   - on ACK: places child SL/TP (Binance UM) or passes them  │ │
   │  │     through (Bybit V5 single-call)                          │ │
   │  │   - on close: cancels child SL/TP, emits OutcomeReport      │ │
   │  │   - mid-price sampler tracks MFE/MAE while position open    │ │
   │  └────────────────────────┬────────────────────────────────────┘ │
   │                           │  OrderRequest                        │
   │                           ▼                                      │
   │  ┌─────────────────────────────────────────────────────────────┐ │
   │  │  trade_adapter/marketdata/   (A16, fed by exchange WS)      │ │
   │  │   - book + trade tape pub/sub, one upstream connection per  │ │
   │  │     (venue, symbol), coalesced subscribers, BBO throttler   │ │
   │  │   - feeds the in-process bus and the gateway WS broadcaster │ │
   │  └─────────────────────────────────────────────────────────────┘ │
   │  ┌─────────────────────────────────────────────────────────────┐ │
   │  │  trade_adapter/exchanges/  (Exchange Abstraction)           │ │
   │  │   abstract: ExchangeAdapter                                 │ │
   │  │     place_order(req) → OrderAck         (over WS-trade A3b) │ │
   │  │     cancel_order(client_order_id)       (over WS-trade A3b) │ │
   │  │     fetch_positions/orders/balance      (REST, bootstrap +  │ │
   │  │                                          reconcile only)    │ │
   │  │     subscribe_user_data()               (private WS)        │ │
   │  │     subscribe_book/trades/bbo()         (public WS, A16)    │ │
   │  │   impls: binance/, bybit/                                   │ │
   │  │   - per-venue WS-trade clients with sign + reply correlation│ │
   │  │   - per-venue REST clients (bootstrap + reconcile only)     │ │
   │  │   - per-venue public/private WS clients with auto-reconnect │ │
   │  │   - tick/step rounding                                      │ │
   │  │   - error normalization                                     │ │
   │  └────────────────┬─────────────────────────┬──────────────────┘ │
   │                   │                         │                    │
   │                   ▼                         ▼                    │
   │  ┌──────────────────────┐    ┌──────────────────────┐            │
   │  │  Binance USD-M       │    │  Bybit Linear        │            │
   │  │  - WS-trade (orders) │    │  - WS-trade (orders) │            │
   │  │  - private WS (user) │    │  - private WS (user) │            │
   │  │  - public WS (depth +│    │  - public WS (book + │            │
   │  │    aggTrade)         │    │    publicTrade)      │            │
   │  │  - REST (bootstrap + │    │  - REST (bootstrap + │            │
   │  │    reconcile only)   │    │    reconcile only)   │            │
   │  └──────────────────────┘    └──────────────────────┘            │
   │                                                                  │
   │  ┌─────────────────────────────────────────────────────────────┐ │
   │  │  trade_adapter/storage/                                     │ │
   │  │   sqlite.py    schema + migrations + DAO layer              │ │
   │  │   redis_pubsub.py    internal event fan-out + hot cache     │ │
   │  └─────────────────────────────────────────────────────────────┘ │
   │                                                                  │
   │  ┌─────────────────────────────────────────────────────────────┐ │
   │  │  trade_adapter/secrets/                                     │ │
   │  │   - Argon2id-derived Fernet key                             │ │
   │  │   - read API keys at sign-time only                         │ │
   │  │   - never log plaintext                                     │ │
   │  └─────────────────────────────────────────────────────────────┘ │
   └──────────────────────────────────────────────────────────────────┘
```

---

## Component contracts

### `trade_adapter/embedded/`

The primary public API. `TradeAdapter` is a thin object whose methods
are 1:1 with the operations exposed in gateway mode:

```python
from uta.embedded import TradeAdapter
from uta.types import OrderRequest, Stop, Side, Venue, OrderType

adapter = TradeAdapter(
    venue=Venue.BINANCE_UM,
    keystore_path="~/.uta/keys",
    transport="ws_first",       # default; opens WS-trade + WS-user-data
)
await adapter.start()

# trading
ack = await adapter.place_order(OrderRequest(
    symbol="BTCUSDT", side=Side.BUY, type=OrderType.MARKET, qty=0.01,
    correlation_id=signal.signal_id,
    sl=Stop(price=64000.0, mode="native"),
    tp=Stop(price=66000.0, mode="native"),
))
await adapter.cancel_order(ack.client_order_id)
await adapter.close_position("BTCUSDT")

# state and execution events
async for upd in adapter.subscribe_orders("BTCUSDT"): ...
async for upd in adapter.subscribe_positions("BTCUSDT"): ...
async for fill in adapter.subscribe_fills("BTCUSDT"): ...

# market data passthrough (A16)
async for book in adapter.subscribe_book("BTCUSDT", depth=50): ...
async for tp in adapter.subscribe_trades("BTCUSDT"): ...
async for bbo in adapter.subscribe_bbo("BTCUSDT"): ...

# outcome feedback (closes the MAB loop in adaptive_sdk)
async for oc in adapter.subscribe_outcomes("BTCUSDT"): ...
```

The iterators back onto an internal pub/sub. Multiple subscribers on
the same `(venue, symbol, channel)` share one upstream WS connection
(decision A16, prohibition C.11).

### `trade_adapter/gateway/`

FastAPI app exposing the same surface to remote consumers. Router
modules:

| File | Endpoints | Purpose |
|---|---|---|
| `signal_routes.py` | `POST /v1/signal` | Validate + accept `UniversalSignal`, dispatch to signal router. |
| `order_routes.py` | `POST /v1/order` | Bypass mode for manual ops; requires `override_acknowledged`. |
| `position_routes.py` | `POST /v1/position/close`, `GET /v1/positions` | Position queries and manual close. |
| `info_routes.py` | `GET /v1/orders`, `GET /v1/balances`, `GET /v1/health` | Read-only diagnostics. |
| `metrics_routes.py` | `GET /metrics` | Prometheus scrape (loopback only by default). |
| `events_ws.py` | `WS /v1/events` | Outbound fan-out, including opt-in market-data channels. |

Authentication is a single FastAPI dependency `verify_consumer_token` that
runs on every endpoint. WS upgrade reads the token from the
`Authorization` header before accepting the connection. Embedded mode
bypasses this dependency entirely (it runs in the same trust domain as
the consumer).

### `trade_adapter/core/`

Pure business logic. No I/O directly — calls into `exchanges/` and
`storage/`.

#### `signal_router.py`

```python
async def route(signal: UniversalSignal, ctx: RouterContext) -> RoutingResult:
    1. validate(signal)                          # schema, TTL, venue known
    2. position = await ctx.positions.get(signal.symbol, signal.venue)
    3. resolved_intent = resolve_intent(signal.intent, position)
    4. qty = await compute_qty(signal.sizing, signal.symbol, signal.venue)
    5. notional = qty * await ctx.market_data.last_price(signal.symbol)
    6. check_emergency_limits(qty, notional, signal.symbol, signal.venue)
    7. await ctx.audit_log.record(signal, resolved_intent, qty)
    8. await ctx.position_manager.dispatch(IntentResolved(...))
    9. return RoutingResult(accepted=True, ...)
```

Emergency kill switch is invoked here, BEFORE any state change in the
position manager.

#### `position_manager.py`

State machine per `(venue, symbol)`. Holds an `asyncio.Lock` per pair so
two concurrent signals on the same symbol serialize cleanly. State enum:

```
IDLE         (no position, no orders)
OPENING      (entry order submitted, waiting for fill)
OPEN         (filled, child SL/TP orders attached)
ADDING       (scaling in; entry order in flight)
REDUCING     (partial close in flight)
CLOSING      (full close in flight)
RECONCILING  (post-reconnect; no new actions until reconcile completes)
```

Transitions are driven by:
- Inbound `IntentResolved` from signal router.
- Inbound `Fill`, `OrderUpdate` events from exchange WS.
- Inbound `ReconcileEvent` from reconciliation task.

Output: `OrderRequest` to exchange adapter, `PositionUpdate` event to
internal pub/sub.

#### `risk.py`

Just two functions:

```python
def check_max_single_order_notional(notional_usd, equity_usd, cfg) -> None
def check_max_leverage(symbol, leverage, cfg) -> None
```

Both raise `EmergencyRejection` on violation. That's the entire risk
surface in v1.0.

#### `reconciliation.py`

Run after every private-WS reconnect for a venue:

```python
async def reconcile(venue, ctx):
    async with ctx.redis.lock(f"reconcile:{venue}"):
        exchange_state = await ctx.exchange.fetch_positions_and_orders(venue)
        local_state = await ctx.storage.fetch_state(venue)
        diffs = compute_diffs(exchange_state, local_state)
        for diff in diffs:
            await apply_diff(diff)
            await ctx.events.publish("reconcile_diff", diff)
```

### `trade_adapter/exchanges/`

#### `base.py`

```python
class ExchangeAdapter(ABC):
    venue: str

    # Trading: WebSocket by default (A3b). REST is fallback only.
    @abstractmethod
    async def place_order(self, req: OrderRequest) -> OrderAck: ...
    @abstractmethod
    async def cancel_order(self, client_order_id: str, symbol: str) -> None: ...

    # State queries: REST, used for bootstrap and reconciliation only.
    @abstractmethod
    async def fetch_positions(self) -> list[Position]: ...
    @abstractmethod
    async def fetch_open_orders(self) -> list[Order]: ...
    @abstractmethod
    async def fetch_balance(self) -> Balance: ...

    # Streams: WebSocket only.
    @abstractmethod
    async def subscribe_user_data(self, on_event: Callable[[Event], Awaitable]) -> None: ...
    @abstractmethod
    async def subscribe_book(
        self, symbol: str, depth: int, on_event: Callable[[BookUpdate], Awaitable]
    ) -> None: ...
    @abstractmethod
    async def subscribe_trades(
        self, symbol: str, on_event: Callable[[TradePrint], Awaitable]
    ) -> None: ...
    @abstractmethod
    async def subscribe_bbo(
        self, symbol: str, on_event: Callable[[BBOUpdate], Awaitable]
    ) -> None: ...

    # Rounding helpers (driven by exchangeInfo bootstrap).
    @abstractmethod
    async def round_qty(self, symbol: str, qty: float) -> float: ...
    @abstractmethod
    async def round_price(self, symbol: str, price: float) -> float: ...
```

The `marketdata/` layer wraps the per-venue `subscribe_book` /
`subscribe_trades` / `subscribe_bbo` calls behind a coalescing fan-out:
any number of in-process subscribers on the same `(venue, symbol)`
share exactly one upstream WebSocket (prohibition C.11).

#### `binance/` and `bybit/`

Each owns:
- `ws_trade.py` — trading WebSocket (Binance `ws-fapi` /
  Bybit `v5/trade`); request signing, reply correlation, reconnect.
- `ws_user.py` — private WebSocket with reconnect + listenKey refresh.
- `ws_market.py` — public WebSocket subscription manager (depth +
  trades; book snapshot bootstrap on Binance).
- `rest.py` — signed REST client used only for bootstrap and
  reconciliation; every send increments
  `uta_rest_fallback_total` if `transport=rest_fallback`.
- `mappers.py` — schema translators (their format ↔ internal dataclasses)
- `errors.py` — error code → `ExchangeError` enum

Adding a third venue (e.g. OKX, Hyperliquid) means dropping a new
directory and registering it in the factory. No changes elsewhere.

### `trade_adapter/storage/`

#### `sqlite.py`

Schema (initial):

```sql
CREATE TABLE signals (
    signal_id TEXT PRIMARY KEY,
    received_at REAL NOT NULL,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    venue TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    resolved_intent TEXT,
    accepted INTEGER NOT NULL,
    rejection_reason TEXT
);

CREATE TABLE orders (
    client_order_id TEXT PRIMARY KEY,
    exchange_order_id TEXT,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    type TEXT NOT NULL,
    qty REAL NOT NULL,
    price REAL,
    status TEXT NOT NULL,
    parent_signal_id TEXT,
    parent_position_key TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    client_order_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    price REAL NOT NULL,
    fee REAL NOT NULL,
    fee_asset TEXT NOT NULL,
    ts REAL NOT NULL
);

CREATE TABLE positions (
    position_key TEXT PRIMARY KEY,        -- "{venue}:{symbol}"
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,                   -- LONG/SHORT/FLAT
    qty REAL NOT NULL,
    entry_price REAL,
    sl_client_order_id TEXT,
    tp_client_order_id TEXT,
    state TEXT NOT NULL,                  -- IDLE/OPENING/OPEN/...
    updated_at REAL NOT NULL
);

CREATE TABLE reconcile_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    venue TEXT NOT NULL,
    kind TEXT NOT NULL,
    details_json TEXT NOT NULL
);
```

All writes go through a thin DAO layer with one `aiosqlite` connection
serialized via `asyncio.Lock` (SQLite single-writer model).

#### `redis_pubsub.py`

Two responsibilities:
- Pub/sub channels: `events.fill`, `events.order`, `events.position`,
  `events.alert`. Internal tasks subscribe; WS broadcaster fans out to
  consumers.
- Hot cache keys: `mktdata:{venue}:{symbol}:bbo` (best bid/ask, JSON,
  TTL 5s).

If Redis is down, internal events fall back to an in-process
`asyncio.Queue`. WS broadcaster degrades but doesn't crash. Hot cache
falls back to direct REST calls (slower).

### `trade_adapter/secrets/`

```python
class SecretStore:
    def __init__(self, path: Path, master_password: str | None = None): ...
    def get_exchange_keys(self, venue: str) -> ExchangeKeys: ...
    def get_consumer_tokens(self) -> dict[str, str]: ...   # token -> consumer_name
    def add_exchange_keys(self, venue: str, keys: ExchangeKeys) -> None: ...
    def add_consumer_token(self, name: str) -> str: ...    # generates + persists
```

Backed by:
- File: `~/.universal-trade-adapter/secrets.enc` (Fernet-encrypted JSON)
- KDF: Argon2id (`time_cost=3, memory_cost=64MB, parallelism=4`)
- Master password from `UTA_MASTER_PASSWORD` env or interactive prompt.

`get_exchange_keys` is the only function that decrypts; it returns a
short-lived `ExchangeKeys` object that the REST signer uses immediately
and discards. Plaintext key material is never assigned to a long-lived
attribute or held in module-level state.

---

## Process & lifecycle

The adapter runs as a single asyncio process. Startup sequence:

```
1. Load config (YAML + ENV overrides)
2. Open SecretStore (prompt or env for master password)
3. Init SQLite (run migrations)
4. Init Redis client (with fallback to in-process queue if unreachable)
5. For each enabled venue:
   a. Initialize ExchangeAdapter
   b. Verify connectivity (REST ping)
   c. Set position mode = ONE_WAY (idempotent)
   d. Subscribe to user data WS
   e. Reconcile state via REST
6. Start signal router + position manager tasks
7. Start FastAPI server (uvicorn)
8. Mark adapter "ready"
```

Shutdown sequence (SIGTERM):

```
1. Stop accepting new signals (return 503 from POST /v1/signal)
2. Wait up to 5s for in-flight signals to finish routing
3. Disconnect WS streams (clean close)
4. Flush pending SQLite writes
5. Close Redis client
6. Exit
```

The adapter does NOT auto-flatten positions on shutdown. Existing
positions remain on the exchange with native SL/TP attached. This is
deliberate — premature auto-flatten on planned restarts is more
dangerous than leaving the position open with a stop.

---

## Concurrency model

- One asyncio event loop, one OS process.
- One `asyncio.Lock` per `(venue, symbol)` for position mutations.
- One global `asyncio.Lock` for the SQLite connection (single-writer).
- One `asyncio.Lock` per venue for "exchange settings setup" (mode
  changes, leverage changes).
- Reconciliation guarded by Redis-backed lock (in case multiple
  reconciles fire from rapid reconnects).

No threads. No multiprocessing. If single-process throughput becomes a
bottleneck, that's a v2.0 conversation (likely sharding by venue).

---

## Data flow: signal → fill

Concrete walkthrough of a single signal:

1. Consumer `POST /v1/signal` with body matching `UniversalSignal`.
2. FastAPI auth dependency verifies Bearer token.
3. `signal_routes` deserializes into `UniversalSignal` dataclass.
4. `signal_router.route()` runs:
   - Audit log written to SQLite.
   - Current position fetched from SQLite (cached in memory, refreshed
     by WS events).
   - Intent resolved: `OPEN` against existing same-direction position
     becomes `ADD`; against opposite-direction becomes `REVERSE`.
   - Quantity computed from `SizingSpec`. For `RiskBased`, requires
     SL → computes `qty = risk_usd / sl_distance_bps × notional_factor`.
   - Emergency limits checked.
   - Result returned to consumer immediately (HTTP 200).
5. In background, `position_manager.dispatch(IntentResolved)`:
   - Acquires `asyncio.Lock` for `(venue, symbol)`.
   - Transitions state machine: `IDLE → OPENING`.
   - Generates `client_order_id`.
   - Builds `OrderRequest`, sends to `ExchangeAdapter.place_order`.
6. `ExchangeAdapter.place_order`:
   - Rounds qty to step size, price to tick size.
   - Signs REST request with API key (decrypted from `SecretStore` for
     this call only).
   - Sends to exchange.
   - On 200 OK, persists `client_order_id → exchange_order_id` mapping.
   - Returns `OrderAck`.
7. Position manager updates SQLite: `orders.status = ACK`.
8. Exchange WS pushes `Fill` event.
9. `subscribe_user_data` callback normalizes into internal `Fill`,
   publishes to internal pub/sub.
10. Position manager subscriber receives `Fill`:
    - Acquires `(venue, symbol)` lock.
    - Updates position: `OPENING → OPEN`, sets `entry_price`.
    - If signal had `sl`/`tp`, places child stop orders.
11. WS broadcaster fans out `position_update` event to all connected
    consumer WS clients.

End-to-end latency targets (decision A1, p95, warm WS, VPS pings
10–30 ms):

- `place_order()` call → exchange ACK: `≤ 25 ms`.
- WS event arrival on the wire → in-process subscriber: `≤ 10 ms`.
- Adapter-added overhead per hop: `≤ 5 ms` on top of network RTT.

Gateway mode adds one local loopback hop and JSON serialization on top
of the embedded numbers.

---

## Error handling

| Class | Behavior |
|---|---|
| Schema validation error | HTTP 400, no state change. |
| Auth error | HTTP 401. |
| Emergency limit | HTTP 422 with `rejection_reason`. No state change. |
| Exchange transient error (5xx, rate limit) | Retry with exponential backoff up to 3 attempts. |
| Exchange permanent error (insufficient funds, bad qty) | Mark order REJECTED, emit alert event, no auto-retry. |
| Network timeout on order submit | Retry with same `client_order_id`. After 3 attempts: mark AMBIGUOUS, queue manual reconciliation. |
| WS disconnect | Auto-reconnect with backoff; trigger reconciliation on success. |
| SQLite I/O error | Crash the process. State must be intact; restart and recover. |
| Redis outage | Degrade gracefully; in-memory pub/sub fallback. |
| Master password wrong | Fail to start; do not run with broken keys. |

---

## Observability

Every position state transition logs:
```json
{
  "event": "position_state_change",
  "venue": "binance_um",
  "symbol": "BTCUSDT",
  "from": "OPENING",
  "to": "OPEN",
  "trigger": "fill",
  "client_order_id": "...",
  "ts": 1700000000.0
}
```

Every accepted signal logs (with payload, scrubbed of metadata if
sensitive):
```json
{
  "event": "signal_accepted",
  "signal_id": "...",
  "source": "adaptive_sdk",
  "symbol": "BTCUSDT",
  "venue": "binance_um",
  "intent_resolved": "OPEN",
  "qty": 0.5,
  "notional_usd": 32500.0,
  "ts": 1700000000.0
}
```

Prometheus counters increment on every domain event. Histograms record
REST latency per `(venue, method)`.

For local debugging, stdout is JSON; pipe through `jq` to filter.
