# Roadmap

## v1.0 — Initial release (in development)

Core adapter as specified in [`spec_v1.0.md`](spec_v1.0.md). Phases of
implementation, in dependency order. Phases 1–4 are reordered (vs the
original v1.0 roadmap) so the first vertical slice is exactly what an
embedded consumer needs: WS-trade + market-data passthrough on a
single venue. The previous "API-first / Bybit-day-one" ordering moved
gateway-mode and second-venue work into later phases.
Latency is treated as best-effort given the chosen Python+`websockets`
stack — there is no synthetic latency-bench gate before Phase 1; the
actual numbers are measured on the integrated adapter once Phase 2 can
place real orders against a real account, and recorded in the
operations runbook.

### Phase 1 — Foundation (contract + storage)
- [ ] `pyproject.toml`, dependencies pinned (core minimal per A1; gateway
      and multiproc as extras)
- [ ] `trade_adapter/types.py` — all dataclasses and enums:
      `UniversalSignal` (with `correlation_id`), `OrderRequest`, `Stop`,
      `OrderUpdate`, `Fill`, `PositionUpdate` (with `liquidation_price`
      / `unrealized_pnl_usd` / `margin_used_usd`), `BookUpdate`,
      `TradePrint`, `BBOUpdate`, `OutcomeReport`
- [ ] `trade_adapter/config.py` — YAML config loader with env override
- [ ] `trade_adapter/secrets/keystore.py` — encrypted file, Argon2id KDF
- [ ] `trade_adapter/storage/sqlite.py` — schema + DAO
- [ ] `trade_adapter/storage/audit_flusher.py` — background batched
      audit-log writer (D.1; never blocks the accept path)
- [ ] `trade_adapter/storage/idempotency.py` — in-memory `signal_id`→
      response cache + SQLite mirror (D.6)
- [ ] `trade_adapter/bus/event_bus.py` — in-process pub/sub with bounded
      `asyncio.Queue` per subscriber + drop-oldest backpressure (A20)
- [ ] `tests/protocol/test_schema_lock.py` — golden-snapshot test
      that fails on any change to the v1.0 wire shape

### Phase 2 — Binance USD-M Futures, WebSocket-first (vertical slice)

The minimum viable adapter for the embedded consumer. One venue, one
process, no gateway.

- [ ] `trade_adapter/exchanges/base.py` — abstract `ExchangeAdapter`
      with WS-trade + market-data subscribe methods (A3b, A16)
- [ ] `trade_adapter/infra/time_sync.py` — server-time offset per
      venue + drift-guard at startup (A18)
- [ ] `trade_adapter/infra/rate_limit.py` — token-bucket per
      `(venue, endpoint_class)`; over-quota → local `RATE_LIMITED` (A17)
- [ ] `trade_adapter/exchanges/binance/ws_trade.py` —
      `wss://ws-fapi.binance.com/ws-fapi/v1`, `order.place` /
      `order.cancel` with sign + reply correlation; SL/TP children
      shipped in parallel with the entry as `STOP_MARKET
      closePosition=true` / `TAKE_PROFIT_MARKET closePosition=true`
      (A8)
- [ ] `trade_adapter/exchanges/binance/ws_user.py` — listenKey-based
      private stream, reconnect + listenKey refresh
- [ ] `trade_adapter/exchanges/binance/ws_market.py` — depth +
      `aggTrade`, snapshot bootstrap via REST `/fapi/v1/depth`
- [ ] `trade_adapter/exchanges/binance/rest.py` — bootstrap +
      reconciliation only, every send tagged
      `transport=rest_fallback` if used outside bootstrap (A3b)
- [ ] `trade_adapter/exchanges/binance/symbols.py` — lazy
      `register_symbol()` + 24 h refresh of `tickSize` / `stepSize` /
      `minNotional` (D.8)
- [ ] `trade_adapter/marketdata/` — coalescing pub/sub layer fed by
      `subscribe_book` / `subscribe_trades` / `subscribe_bbo`; one
      upstream WS per `(venue, symbol)` regardless of subscriber
      count (prohibition C.11)

### Phase 3 — Core logic + embedded API
- [ ] `trade_adapter/core/signal_router.py` — validation + intent
      resolution + sizing, `correlation_id` propagation, idempotency
      cache lookup (D.6), audit-log enqueue
- [ ] `trade_adapter/core/position_manager.py` — per-symbol state
      machine, child SL/TP shipped in parallel with entry (A8),
      MFE/MAE sampler driven by `marketdata/` BBO subscription
      (auto-subscribed for any open position per A22)
- [ ] `trade_adapter/core/outcome.py` — emits `OutcomeReport` on
      position close (cancel-on-close + sampler readout)
- [ ] `trade_adapter/core/risk.py` — emergency kill switch
- [ ] `trade_adapter/core/reconciliation.py` — post-reconnect REST
      sweep, in-process `asyncio.Lock` per venue
- [ ] `trade_adapter/embedded/adapter.py` — public `TradeAdapter` API:
      `place_order` / `cancel_order` / `close_position` plus the
      `subscribe_*` async iterators (A3, A16); `register_symbol()`
      lazy warmup; sync wrapper at `uta.embedded.sync.SyncAdapter`
      for non-async callers (A3 hybrid surface)

### Phase 4 — Latency observation + Bybit Linear

Once the Binance vertical works against a real $50 account, take a
first measurement of `place_order → exchange ACK` and `WS event →
in-process subscriber` through `uta.embedded.TradeAdapter` so the
operator has real numbers for capacity-planning and ops-runbook
purposes. No spec gate — the numbers describe the system, they do
not block it. Then add the second venue.

- [ ] Integrated latency observation: in-process p50/p95 for
      `place_order → exchange ACK`, WS event → in-process subscriber,
      adapter overhead per hop — measured through
      `uta.embedded.TradeAdapter`, not through a private WS client.
      Numbers go into the operations runbook, not into the spec.
- [ ] `trade_adapter/exchanges/bybit/ws_trade.py` —
      `wss://stream.bybit.com/v5/trade`, `order.create` /
      `order.cancel`, with `stopLoss` / `takeProfit` bundled on entry
      (A8)
- [ ] `trade_adapter/exchanges/bybit/ws_user.py` — `v5/private`,
      topics `order` / `execution` / `position`
- [ ] `trade_adapter/exchanges/bybit/ws_market.py` —
      `orderbook.{depth}.{symbol}` + `publicTrade.{symbol}`
- [ ] `trade_adapter/exchanges/bybit/rest.py` — bootstrap +
      reconciliation only

### Phase 5 — Optional gateway (REST + WS facade)

For remote consumers and other-language bots. Embedded consumers do
not need this.

- [ ] `trade_adapter/gateway/auth.py` — Bearer token verification
- [ ] `trade_adapter/gateway/signal_routes.py` — `POST /v1/signal`
- [ ] `trade_adapter/gateway/order_routes.py` — `POST /v1/order`
- [ ] `trade_adapter/gateway/position_routes.py` — close + query
- [ ] `trade_adapter/gateway/info_routes.py` — health, balances,
      orders
- [ ] `trade_adapter/gateway/events_ws.py` — outbound WS event stream
      with opt-in market-data filters (A16)
- [ ] `trade_adapter/gateway/metrics_routes.py` — Prometheus

### Phase 6 — Operational
- [ ] `trade_adapter/main.py` — startup/shutdown lifecycle
- [ ] `scripts/uta-cli` — key management, consumer-token management
- [ ] `tests/` — unit tests for pure logic (sizing math, intent
  resolution, state machine transitions, MFE/MAE sampler)
- [ ] Manual integration testing against $50 real account on both
  venues
- [ ] Systemd unit example
- [ ] README operations runbook

### Phase 7 — Hardening
- [ ] Stress test: 100 signals/sec sustained
- [ ] Reconnect chaos test: disconnect WS-trade and WS-user at
      random intervals during an active position; verify no orphaned
      stops, no duplicated orders (idempotency via `client_order_id`)
- [ ] Audit log inspection tool: `uta-cli audit --signal-id ...`

---

## v1.1 — Quality-of-life

Additive features that don't break the v1.0 contract.

- [ ] **Roles for consumer tokens** — `read`, `trade`, `admin`. Useful
  when UI is read-only and bots get trade scope.
- [ ] **Trailing stops** — local-trigger trailing logic with native
  stop replacement.
- [ ] **OCO orders** — emit two child orders, cancel sibling on either
  fill.
- [ ] **Multi-account per venue** — multiple labeled accounts in
  secret store, signal targets specific account by name.
- [ ] **Max-positions-per-venue** safety knob.
- [ ] **Symbol allowlist** — refuse signals for unlisted symbols even
  if exchange knows them.
- [ ] **`uta-cli replay`** — replay signals from audit log against a
  paper-trade backend (NOT real exchange).
- [ ] **Better metrics**: per-symbol histograms, per-source
  acceptance/rejection rates.

---

## v1.2 — Strategy adapter helpers

Glue code making it easier to consume from common producers.

- [ ] **`AdaptiveSdkBridge`** — Python helper that takes
  `adaptive_sdk.Signal` and POSTs the equivalent
  `UniversalSignal`. Lives in this repo, depends on the SDK as a
  dev dep.
- [ ] **TradingView webhook receiver** — translates TradingView alert
  JSON → `UniversalSignal`. Endpoint with shared-secret HMAC.
- [ ] **Telegram command bot** — chat commands like
  `/close BTCUSDT` or `/positions` proxied to the adapter.

---

## v2.0 — Major

Breaking changes, longer-running concerns.

- [ ] **Hedge mode** — long+short same symbol simultaneously. Required
  for some funding-capture and basis-trading strategies.
- [ ] **Spot trading** — currently futures-only. Adds another venue
  type per exchange (Binance Spot, Bybit Spot).
- [ ] **More venues** — OKX, Hyperliquid, dYdX. Each is one new
  directory under `exchanges/`.
- [ ] **Cross-venue signals** — `venue: "preferred:[binance_um, bybit_linear]"`
  with cost-aware routing.
- [ ] **Async signal queue with priority** — high-priority manual
  closes jump ahead of low-priority bot signals during congestion.
- [ ] **Sharded multi-process** — one process per venue, IPC via
  Redis or shared SQLite. Needed if single-process throughput becomes
  the bottleneck.
- [ ] **Native trailing stops** (server-side, where exchanges
  support it).
- [ ] **TLS termination in-process** (currently relies on reverse
  proxy).

---

## v2.x — Research / experimental

Items requiring real-world validation before being product features.

- [ ] **Smart order routing** — split a large order across venues to
  minimize slippage.
- [ ] **Iceberg orders** — break a large order into hidden child
  orders.
- [ ] **TWAP/VWAP execution algorithms** for sizing larger entries.
- [ ] **Latency profiler** — per-hop latency telemetry from signal
  receipt → exchange ack → fill received.

---

## Out of scope (won't do)

Explicitly considered and rejected:

- **Strategy logic in the adapter.** Strategy is the caller's job.
  This is an executor.
- **Backtesting framework.** Backtesting belongs to the SDK or a
  separate harness, not here. The adapter only operates on real
  exchanges.
- **GUI dashboard.** All necessary state is exposed via REST and WS;
  build your own dashboard.
- **Built-in alerting (email, Slack, PagerDuty).** Integration via
  Prometheus AlertManager or similar; out of scope to bake in.
- **Auto-deleveraging on PnL drawdown.** Risk management is the
  caller's responsibility (decision A12). The kill switch in v1.0 is
  bug-protection, not risk-management.
- **C++/Rust port.** Python is fast enough for sub-second decisions.
  If HFT is needed, that's a different product.
- **Cloud-managed deployment.** Self-hosted only.

---

## Versioning

Same scheme as the analytics SDK:
- Patch (1.0.x): bug fixes that don't change semantics.
- Minor (1.x): new optional fields, new commands, new strategies.
  Existing producers MUST continue working.
- Major (2.x): breaking changes. Locked decisions in
  [`spec_v1.0.md`](spec_v1.0.md) require a major bump.
