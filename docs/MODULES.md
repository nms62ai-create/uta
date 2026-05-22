# Modules — per-file reference

What lives in each file under `trade_adapter/`. Read top to bottom for
the data flow: types → storage → bus → core → venue → public API.

---

## Top level

### `trade_adapter/types.py`
The wire-protocol types — pure dataclasses + enums, no I/O, no logic.
Every object that crosses a process boundary (incoming `UniversalSignal`,
outgoing `OrderRequest`, every `*Update` event) is defined here. The
shape is locked by `tests/protocol/test_schema_lock.py`; changing a
field name or kind tag is a v1.0 protocol break.

Notable members:

* `UniversalSignal` — what producers send in.
* `SizingSpec` family: `FixedQty`, `NotionalUsd`, `PctEquity`,
  `RiskBased`.
* `StopSpec` family: `AbsolutePrice`, `BpsFromEntry`, `PctFromEntry`,
  `AtrMultiple`. Each carries a `mode` of `native` (venue stop) or
  `local_trigger` (deferred to v1.1).
* `OrderRequest` / `OrderAck` / `OrderUpdate` / `Fill` —
  exchange-side primitives.
* `Position` / `PositionUpdate` — position state (carries
  `liquidation_price`, `unrealized_pnl_usd`, `margin_used_usd`).
* `BookUpdate` / `TradePrint` / `BBOUpdate` — market data passthrough.
* `OutcomeReport` — position-close summary with MAE/MFE samples.
* `SignalAck`, `RejectionReason`, `EventType`, `CloseReason`, `Venue`,
  `Direction`, `Intent`, etc.

### `trade_adapter/serialization.py`
Symmetric JSON wire codecs for everything in `types.py`:
`signal_to_wire` / `signal_from_wire`, `order_update_to_wire`, etc.
Used by the golden-snapshot test, by the audit-log writer, and by the
optional gateway (Phase 5). Deterministic key order — wire output is
byte-stable for any given dataclass.

### `trade_adapter/config.py`
YAML config loader with env override. Defines the schema for
operator-facing configuration (venue endpoints, file paths,
rate-limit knobs). Not used by the embedded `TradeAdapter` directly —
producers using the embedded API construct their stack programmatically.

### `trade_adapter/embedded.py`
The public `TradeAdapter` class — the single, stable surface external
producers use. Composes the venue adapter, signal router, position
manager, risk gate, and event bus behind:

* `await start()` / `await close()` (and `async with ta:`)
* `await submit_signal(UniversalSignal) -> SignalAck`
* `await cancel_order(symbol, client_order_id)`
* `subscribe(EventType) -> Subscription`
* `get_position(venue, symbol)`, `open_positions()`,
  `get_equity_usd(venue)`
* `trip_kill_switch(reason)` / `reset_kill_switch()`

Plus advanced read-only escape hatches (`event_bus`, `position_manager`,
`risk_state`, `signal_router`, `exchange_adapter`) for consumers that
need more than the curated surface.

### `trade_adapter/api/__init__.py`
Reserved namespace for the optional FastAPI gateway (Phase 5). Empty
in v1.0.

---

## `bus/`

### `bus/event_bus.py`
In-process pub/sub. Bounded `asyncio.Queue` per subscriber with
**drop-oldest** backpressure (decision A20). A slow consumer can never
stall the trading hot path.

* `EventBus.subscribe(topic, queue_size=256) -> Subscription`
* `EventBus.publish(topic, payload)` — fire-and-forget.
* `Subscription.queue` — `asyncio.Queue[dict]`; the caller reads.
* `Subscription.close()` — unsubscribe + drain.

No wildcards by design; subscribe to each `EventType` explicitly.

---

## `storage/`

### `storage/sqlite.py`
SQLite schema + async DAO (`SqliteDAO`). Single file on disk holds:

* `idempotency` — `signal_id` → cached `SignalAck` JSON.
* `audit_log` — every accepted/rejected signal + outbound order + fill,
  flushed off the hot path.
* `outcomes` — closed-position summaries.

WAL mode, configurable path. `SqliteDAO.open(path)` is the entry
point; `await dao.close()` flushes pending writes.

### `storage/idempotency.py`
In-memory `signal_id` → response cache mirrored to SQLite (decision
D.6). The signal router checks it first thing on every
`submit_signal`; a retry replays the original ack with
`duplicate=True`. TTL-bounded; expired entries fall back to the
SQLite mirror.

### `storage/audit_flusher.py`
Background batched writer for `audit_log`. The signal router enqueues
events synchronously (in-memory ring buffer); the flusher drains them
to SQLite in 100 ms windows. Accept-path latency is therefore
independent of disk IO.

---

## `secrets/`

### `secrets/keystore.py`
Encrypted key file at rest. Argon2id KDF + AES-GCM. `Keystore.load(path,
password)` returns a dataclass with one entry per venue (`api_key`,
`api_secret`, `label`) plus `consumer_tokens` for the optional gateway.
Producers using the embedded API typically read keys directly from env
vars; the keystore exists for operators who don't want plaintext keys
on disk.

---

## `core/`

### `core/protocols.py`
`typing.Protocol` interfaces consumed by the signal router and
embedded API. Lets the core stay venue-neutral and unit-testable.

* `ExchangeAdapter` — `submit_order`, `cancel_order`. Satisfied by
  `BinanceUmAdapter` (Phase 3a) and Bybit Linear (Phase 4).
* `PositionProvider` — `get_position(venue, symbol)`. Implemented by
  `PositionStore`.
* `MarketDataProvider` — `get_reference_price(venue, symbol)`. Backed
  by the venue's BBO cache.
* `EquityProvider` — `get_equity_usd(venue)`. Backed by
  `PositionStore`.
* `VenueSnapshotProvider` — `fetch_position_snapshot()`,
  `fetch_equity_snapshot()`. Implemented by
  `BinanceUmSnapshotProvider`.
* `NullPositionProvider` — convenience stub for tests / pre-bootstrap.

### `core/signal_router.py`
The accept boundary. `SignalRouter.submit_signal(signal)` orchestrates:

1. Idempotency check.
2. Schema sanity (`ttl_seconds > 0`, non-empty symbol).
3. Intent resolution against the position provider.
4. Reference-price lookup.
5. Stop-price math via `stops.compute_protective_price`.
6. Sizing math via `sizing.compute_target_qty`.
7. Risk-gate evaluation.
8. Entry MARKET order plus optional SL/TP children dispatched in
   parallel via `asyncio.gather`. SL/TP use `closePosition=true` so
   they auto-cancel when the position is flat.
9. `SIGNAL_RECEIVED` event on the bus + cache the ack.

Returns a typed `SignalAck`. Venue exceptions during the actual submit
propagate; everything before the submit becomes a non-accepted
`SignalAck` with a `RejectionReason`.

### `core/position_manager.py`
REST bootstrap + reconcile + user-data event ingestion. Three
responsibilities:

1. **Bootstrap** at `start()` — fetches positions + equity via
   `VenueSnapshotProvider` and writes them into `PositionStore`.
2. **Event ingestion** — venue adapter calls
   `apply_position_update` / `apply_fill` / `apply_equity_update`
   for each user-data-stream event. Hot path; no network.
3. **Reconciliation** — background coroutine re-runs the bootstrap
   every `reconcile_interval_s`, diffs against the store, applies
   venue truth on drift, and publishes one `ReconcileDiff` per
   drift. Covers user-data-stream gap-or-drop.

Venue-agnostic; Bybit reuses it unchanged in Phase 4.

### `core/position_store.py`
In-memory truth for positions, fills, and equity. `dict` keyed by
`(venue, symbol)`. Satisfies both `PositionProvider` and
`EquityProvider`. Synchronous writes (called from `PositionManager`),
synchronous reads (called from `SignalRouter` and the public API).

### `core/sizing.py`
Pure sizing-math engine. `compute_target_qty(spec, *, reference_price,
equity_usd, sl_price) -> float`. One function per `SizingSpec`
variant. `RiskBased` requires `sl_price`; the signal router catches
that case earlier so the producer sees `SIZING_REQUIRES_SL`.

### `core/stops.py`
Pure stop-price calculator. `compute_protective_price(spec, *,
entry_price, direction, kind) -> float`. Handles every `StopSpec`
variant (`absolute`, `bps`, `pct`, `atr_multiple`) for both `sl` and
`tp` legs.

### `core/risk.py`
Pre-submit risk gate.

* `RiskConfig` — producer-supplied caps (per-symbol qty, per-symbol
  notional, daily loss).
* `RiskState` — mutable runtime accounting (daily realized PnL by
  UTC date, kill-switch flag). The position manager calls
  `record_realized_pnl` on close; producers can call
  `trip_kill_switch` manually.
* `RiskGate.evaluate(signal, *, target_qty, reference_price) ->
  RiskDecision` — kill switch → daily-loss cap → per-symbol caps.
  Returns `allow()` or `deny(reason, detail)`.

Venue-agnostic. Bybit reuses it unchanged in Phase 4.

---

## `exchanges/binance_um/`

The Binance USD-M Futures venue adapter. Everything here is
Binance-specific; abstractions live in `core/protocols.py`.

### `exchanges/binance_um/adapter.py`
`BinanceUmAdapter` — implements the `ExchangeAdapter` Protocol. Owns:

* The REST client (bootstrap / fallback only).
* The WS-trade client (`order.place` / `order.cancel` over
  `wss://ws-fapi.binance.com/ws-fapi/v1`).
* The WS user-data stream (listenKey-based).
* The WS market stream (book / aggTrade / BBO).
* The symbol registry (for `tickSize` / `stepSize` / `minNotional`).
* The market-data cache used as `MarketDataProvider`.

Lifecycle: `start()` → time-sync + WS connects + symbol registry
warmup. `close()` → reverse order, errors swallowed.

`submit_order` rounds qty + price to venue precision, signs the
request, sends via WS-trade, awaits the matching reply by `id`, and
returns an `OrderAck`.

### `exchanges/binance_um/auth.py`
HMAC-SHA256 signing for Binance REST query strings and WS-API params.
Pure, no I/O. Used by both the REST client and WS-trade.

### `exchanges/binance_um/rest.py`
`BinanceRestClient` — `aiohttp` session over Binance REST. Used for:

* Bootstrap (`/fapi/v2/positionRisk`, `/fapi/v2/account`,
  `/fapi/v1/exchangeInfo`).
* listenKey lifecycle.
* Fallback `place/cancel` when WS-trade is down (every fallback is
  tagged so it shows up in audit + metrics).

### `exchanges/binance_um/rest_translators.py`
Pure functions translating Binance REST JSON → `types.py` dataclasses.
`position_risk_to_position_updates`, `account_info_to_equity_usd`,
`exchange_info_to_symbol_specs`. Used by the snapshot provider and the
symbol registry.

### `exchanges/binance_um/snapshot.py`
`BinanceUmSnapshotProvider` — implements `VenueSnapshotProvider` over a
`BinanceRestClient`. Two methods: `fetch_position_snapshot()`,
`fetch_equity_snapshot()`. Translator errors propagate so a Binance API
drift surfaces loudly rather than silently producing a stale snapshot.

### `exchanges/binance_um/symbols.py`
`SymbolRegistry` — lazy `register_symbol()` + 24 h refresh of
`tickSize` / `stepSize` / `minNotional`. The adapter uses these for
qty / price rounding before sending to the venue.

### `exchanges/binance_um/translators.py`
Pure functions translating Binance WS frames → `types.py` events.
Handles `ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`, `executionReport`,
depth, aggTrade, bookTicker. Used by the WS clients.

### `exchanges/binance_um/user_handlers.py`
Routes user-data-stream events into the `PositionManager`. Wires:

* `ORDER_TRADE_UPDATE` → `OrderUpdate` event + `Fill` event.
* `ACCOUNT_UPDATE` (positions section) → `PositionUpdate` event +
  `mgr.apply_position_update`.
* `ACCOUNT_UPDATE` (balances) → `mgr.apply_equity_update`.

### `exchanges/binance_um/ws/transport.py`
`WsRpcClient` — base WebSocket transport with supervisor task,
exponential-backoff reconnect, request/response correlation by `id`,
and per-request timeout. Used by `ws/trade.py` and indirectly by
`ws/stream.py`.

### `exchanges/binance_um/ws/trade.py`
WS-trade RPC client. Signed `order.place` / `order.cancel` /
`order.placeBatch` over `wss://ws-fapi.binance.com/ws-fapi/v1`.
Unwraps Binance's error envelope into typed exceptions.

### `exchanges/binance_um/ws/market.py`
Combined market-data stream (book diffs + aggTrade + bookTicker).
Feeds the in-process cache that backs `MarketDataProvider`.

### `exchanges/binance_um/ws/stream.py`
Generic WS-stream transport (no RPC; for market and user streams).
Reconnect + subscription replay.

### `exchanges/binance_um/ws/user_stream.py`
listenKey lifecycle + private-stream connection. Refreshes the
listenKey every 30 minutes; reconnects + re-subscribes on drop.
Routes raw frames through `user_handlers`.

---

## `exchanges/bybit_linear/`
Reserved for Phase 4 (Bybit Linear venue adapter). Empty in v1.0.

---

## Tests

* `tests/protocol/test_schema_lock.py` — locks the wire shape of
  every dataclass in `types.py` against golden JSON in
  `tests/protocol/golden/`. Any unintended field rename / kind-tag
  change fails this test.
* `tests/core/` — pure-logic unit tests for `signal_router`,
  `position_manager`, `position_store`, `sizing`, `stops`, `risk`.
* `tests/exchanges/binance_um/` — Binance-side unit tests
  (auth, REST translators, WS translators, adapter, snapshot,
  user_handlers).
* `tests/test_embedded.py` — public `TradeAdapter` contract.
* `tests/test_keystore.py`, `tests/test_event_bus.py`,
  `tests/test_sqlite.py`, etc. — infra-level tests.

All tests are offline. No network, no testnet, no real keys.
