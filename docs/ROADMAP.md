# Roadmap

## v1.0 — Initial release (in development)

Core adapter as specified in [`spec_v1.0.md`](spec_v1.0.md). Phases of
implementation, in dependency order:

### Phase 1 — Foundation
- [ ] `pyproject.toml`, dependencies pinned
- [ ] `trade_adapter/types.py` — all dataclasses, enums (the contract surface)
- [ ] `trade_adapter/config.py` — YAML config loader with env override
- [ ] `trade_adapter/secrets/keystore.py` — encrypted file, Argon2id KDF
- [ ] `trade_adapter/storage/sqlite.py` — schema + DAO
- [ ] `trade_adapter/storage/redis_pubsub.py` — pub/sub + cache (with fallback)

### Phase 2 — Exchange adapters
- [ ] `trade_adapter/exchanges/base.py` — abstract `ExchangeAdapter`
- [ ] `trade_adapter/exchanges/binance/` — REST + private/public WS
- [ ] `trade_adapter/exchanges/bybit/` — REST + private/public WS
- [ ] Tick/step rounding, error normalization, listenKey lifecycle

### Phase 3 — Core logic
- [ ] `trade_adapter/core/signal_router.py` — validation + intent resolution + sizing
- [ ] `trade_adapter/core/position_manager.py` — per-symbol state machine
- [ ] `trade_adapter/core/risk.py` — emergency kill switch
- [ ] `trade_adapter/core/reconciliation.py` — post-reconnect REST sweep

### Phase 4 — Public API
- [ ] `trade_adapter/api/auth.py` — Bearer token verification
- [ ] `trade_adapter/api/signal_routes.py` — `POST /v1/signal`
- [ ] `trade_adapter/api/position_routes.py` — close + query
- [ ] `trade_adapter/api/info_routes.py` — health, balances, orders
- [ ] `trade_adapter/api/events_ws.py` — outbound WS event stream
- [ ] `trade_adapter/api/metrics_routes.py` — Prometheus

### Phase 5 — Operational
- [ ] `trade_adapter/main.py` — startup/shutdown lifecycle
- [ ] `scripts/uta-cli` — key management, consumer-token management
- [ ] `tests/` — unit tests for pure logic (sizing math, intent
  resolution, state machine transitions)
- [ ] Manual integration testing against $50 real account
- [ ] Systemd unit example
- [ ] README operations runbook

### Phase 6 — Hardening
- [ ] Stress test: 100 signals/sec sustained
- [ ] Reconnect chaos test: disconnect WS at random intervals during
  active position
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
