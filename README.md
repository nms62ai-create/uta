# Universal Trade Adapter (UTA)

[![python](https://img.shields.io/badge/python-3.11%2B-blue)]()
[![license](https://img.shields.io/badge/license-private-red)]()

Self-hosted, single-process, WebSocket-first trade execution module for
Binance USD-M Futures (Bybit Linear in v1.1). Ships as a Python library
you import into your own producer — strategy bot, manual UI, anything
else — and drive via a single `TradeAdapter` class.

UTA owns the boring, error-prone half of trading: connect, sign, place,
maintain, cancel, reconcile. You own the interesting half: deciding
**when** to trade and **how big**.

```
your strategy                       UTA
─────────────                       ────
 produces       UniversalSignal   ─► signal_router (dedupe / size / stop)
                                   ─► risk_gate    (kill switch / caps)
                                   ─► binance_um   (WS-trade signed RPC)
                                   ─► position_manager (REST bootstrap + reconcile)
                <─ events on bus  ◄─ user-data stream + market data
```

---

## TL;DR (на русском)

UTA — это **исполнительный модуль**, который ты импортируешь в свой код
и кормишь сигналами. Что он умеет:

1. WS-only торговля с Binance UM Futures (REST только для bootstrap и
   reconcile). Минимум задержки, минимум REST.
2. Принимает `UniversalSignal` (символ + направление + размер + SL/TP)
   и ставит ордера сразу с native стопами (SL/TP идут вместе с входом,
   без «голого» окна).
3. Ведёт state-machine на каждый символ — позиции, средняя цена,
   нереализованный PnL, ликвидация.
4. Раздаёт market data (book / aggTrade / BBO) подписчикам — один WS
   на `(venue, symbol)`, не нужно открывать второй коннект из UI.
5. Идемпотентный — повтор сигнала с тем же `signal_id` не создаёт
   дубль.
6. Emergency kill switch (max notional, daily loss, per-symbol caps).
7. Хранит API-ключи зашифрованно на диске (опционально).

Что UTA **не делает**: не придумывает торговые идеи, не делает бэктест,
не рисует графики, не торгует за тебя. Это исполнитель — стратегия
твоя.

---

## Quick start

```bash
python -m pip install -e .
```

Минимальный embed (полный код — [`docs/QUICKSTART.md`](docs/QUICKSTART.md)):

```python
import asyncio
from trade_adapter.embedded import TradeAdapter
from trade_adapter.types import (
    Direction, FixedQty, Intent, UniversalSignal, Venue,
)

async def main() -> None:
    ta: TradeAdapter = build_my_stack()   # see docs/QUICKSTART.md
    async with ta:
        ack = await ta.submit_signal(UniversalSignal(
            signal_id="...",
            source="my_strategy",
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            direction=Direction.LONG,
            intent=Intent.OPEN,
            sizing=FixedQty(qty=0.001),
            sl=None, tp=None, ttl_seconds=5.0,
            correlation_id=None,
        ))
        print(ack.accepted)

asyncio.run(main())
```

Runnable, fully offline example (no network, no keys):

```bash
python -m examples.mocked_venue
```

---

## Documentation

| Document | What's inside |
|---|---|
| [`docs/QUICKSTART.md`](docs/QUICKSTART.md) | Install + minimum-viable wiring + the 7 things you actually do with `TradeAdapter`. **Start here.** |
| [`docs/MODULES.md`](docs/MODULES.md) | Per-file reference for every module under `trade_adapter/`. What it does, what it doesn't do, what it depends on. |
| [`docs/EXAMPLE.md`](docs/EXAMPLE.md) | Worked single-signal walkthrough — every step from `submit_signal` to `OUTCOME_REPORT`, including what happens when things go wrong. |
| [`docs/SIGNAL_PROTOCOL.md`](docs/SIGNAL_PROTOCOL.md) | `UniversalSignal` wire format + every event the adapter publishes. |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Block-by-block diagrams: embedded API, gateway, signal router, position manager, market-data passthrough, event bus, time-sync, rate-limit. |
| [`docs/spec_v1.0.md`](docs/spec_v1.0.md) | Locked design decisions A0–A22 with the reasoning. |
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | v1.0 phases, v1.1 / v2.0 plans. |
| [`docs/SECURITY.md`](docs/SECURITY.md) | Threat model, encrypted keystore, audit log, kill switch. |

---

## Repository layout

```
trade_adapter/
    types.py                         # wire-protocol dataclasses + enums
    serialization.py                 # JSON wire codecs (locked by golden tests)
    config.py                        # YAML config loader (operator-facing)
    embedded.py                      # public TradeAdapter class — start here
    bus/event_bus.py                 # in-process pub/sub, drop-oldest
    storage/sqlite.py                # SQLite DAO (idempotency + audit + outcomes)
    storage/idempotency.py           # signal_id dedup cache
    storage/audit_flusher.py         # batched audit-log writer
    secrets/keystore.py              # Argon2id+AES-GCM key file
    core/
        protocols.py                 # ExchangeAdapter / PositionProvider / ...
        signal_router.py             # accept boundary: dedup / size / stop / risk / submit
        position_manager.py          # REST bootstrap + reconcile + user-event ingest
        position_store.py            # in-memory position truth
        sizing.py                    # pure sizing math
        stops.py                     # pure stop-price math
        risk.py                      # RiskConfig / RiskState / RiskGate
    exchanges/binance_um/
        adapter.py                   # BinanceUmAdapter (composes WS + REST + symbols)
        auth.py                      # HMAC-SHA256 signing
        rest.py                      # REST bootstrap + fallback
        rest_translators.py          # Binance JSON → types.py
        snapshot.py                  # VenueSnapshotProvider over REST
        symbols.py                   # tickSize / stepSize / minNotional
        translators.py               # WS frames → types.py
        user_handlers.py             # user-stream events → PositionManager
        ws/transport.py              # WsRpcClient (signed RPC, reconnect, id routing)
        ws/trade.py                  # order.place / order.cancel
        ws/user_stream.py            # listenKey + private events
        ws/market.py                 # book / aggTrade / BBO
        ws/stream.py                 # generic stream transport
    exchanges/bybit_linear/          # reserved for Phase 4
tests/                               # unit + protocol golden-snapshot
docs/                                # see Documentation table above
examples/                            # mocked_venue.py, testnet_smoke.py
```

---

## What UTA does **not** do

* No strategy logic. The adapter executes; strategy is the caller's job.
* No backtesting. Real exchanges only.
* No GUI. State is exposed via Python API and event bus; build your own.
* No built-in alerting (Prometheus alertmanager or similar belongs in
  ops, not here).
* No auto-deleveraging on PnL drawdown. The kill switch is
  bug-protection, not risk management.
* No hedge mode in v1.0 (net-only). Hedge mode is on the v2.0 roadmap.
* No spot trading (futures-only).

See [`docs/ROADMAP.md`](docs/ROADMAP.md) ("Out of scope") for the full
list.

---

## License

Private / proprietary. No public license grant.
