# Universal Trade Adapter

[![status](https://img.shields.io/badge/status-spec%20phase-yellow)]()
[![python](https://img.shields.io/badge/python-3.11%2B-blue)]()
[![license](https://img.shields.io/badge/license-private-red)]()

Self-hosted, single-process trading adapter for Binance and Bybit. Accepts
universal signals from external sources (analytics SDK, custom bots, UI),
manages positions, and executes orders via the appropriate exchange-specific
WebSocket trading API (REST is bootstrap-and-fallback only). Ships as a
Python package for in-process consumers; an optional FastAPI gateway
exposes the same surface to remote consumers.

> **Status: planning phase.** This repository currently contains the v1.0
> specification and architecture documentation. The spec was given a
> pre-implementation revision in 2026-05 (see the "What changed" table at
> the top of [`docs/spec_v1.0.md`](docs/spec_v1.0.md)) before any
> production code shipped. Implementation begins once the revised spec is
> signed off.

---

## Что это и зачем (TL;DR на русском)

Универсальный исполнительный модуль:

1. Подключается по WebSocket к Binance USD-M Futures и Bybit Linear для
   потоков market data + private user data (filly, ордера, позиции) и
   для отправки самих ордеров (WS-trade). REST — только bootstrap и
   reconcile.
2. Принимает универсальные сигналы либо как Python-метод (embedded-режим,
   дефолт), либо по REST/WS gateway (опционально, для удалённых
   потребителей и других языков). Все источники говорят на одном
   протоколе.
3. Отдаёт наружу не только события торговли, но и market data
   (`book_update`, `trade_print`, `bbo_update`) — потребителям не нужно
   открывать второй WS на тот же символ.
4. Открывает позиции, ставит SL/TP на бирже одной командой, ведёт
   state-machine на каждый символ.
5. Хранит API-ключи бирж зашифрованно на диске.
6. Идемпотентный — двойная отправка одного сигнала не порождает дубль.
7. Защищён emergency kill switch (max notional, max leverage) от багов в
   бот-стороне.

---

## Документация

| Документ | Что внутри |
|---|---|
| [`docs/spec_v1.0.md`](docs/spec_v1.0.md) | v1.0 спецификация — 17 решений (A0–A16, включая A3b), контракт. |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Блок-за-блоком: embedded API, gateway, Signal Router, Position Manager, Exchange Abstraction, market-data passthrough. |
| [`docs/SIGNAL_PROTOCOL.md`](docs/SIGNAL_PROTOCOL.md) | Формат `UniversalSignal` (с `correlation_id`), все события, в т.ч. `book_update`/`trade_print`/`bbo_update`/`outcome_report`. |
| [`docs/SECURITY.md`](docs/SECURITY.md) | Модель угроз, шифрование ключей, audit log, kill switch. |
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | v1.0 / v1.1 / v2.0 — что и когда. |

---

## Locked decisions (быстрая справка)

| # | Решение | Значение |
|---|---|---|
| 0 | Repo | Отдельный (не моно с SDK) |
| 1 | Stack | Python 3.11+ asyncio + websockets; FastAPI только в gateway-режиме. p95 латентности: ≤25 ms `place_order` → ACK, ≤10 ms WS-event → подписчик |
| 2 | API keys | Encrypted file (master password из env) |
| 3 | Outward transport | Embedded Python API (дефолт) + опциональный REST/WS gateway (тот же контракт) |
| 3b | Exchange transport | WebSocket-first: торговля и user-data по WS. REST — только bootstrap, reconcile, явный fallback |
| 4 | Auth | Bearer token per consumer (gateway-режим; embedded — тот же trust domain) |
| 5 | State store | SQLite (truth) + Redis (cache + pub/sub) |
| 6 | Order idempotency | client_order_id (UUID) per order |
| 7 | Position model | Net-only (one-way mode) |
| 8 | SL/TP | Нативные стопы на бирже, прикреплены к ордеру входа в одном вызове `place_order` |
| 9 | Reconnect | Aggressive REST reconciliation |
| 10 | Signal format | `UniversalSignal` dataclass + `correlation_id` (см. протокол) |
| 11 | Sizing | Fixed qty / % equity / risk-based — все три |
| 12 | Risk limits | Emergency kill switch only (max notional, max leverage) |
| 13 | Exchange diff | Полностью прячется за абстракцией |
| 14 | Testing | Только real exchanges (нет testnet/mock/paper) |
| 15 | Observability | structlog JSON + Prometheus metrics (включая `uta_order_send_latency_seconds`, `uta_event_dispatch_latency_seconds`, `uta_rest_fallback_total`) |
| 16 | Market data publish | `book_update` / `trade_print` / `bbo_update` отдаются подписчикам; одно WS-соединение на (venue, symbol) на всех |

Полное обоснование каждого решения — в [`docs/spec_v1.0.md`](docs/spec_v1.0.md).

---

## Layout (пока scaffold)

```
trade_adapter/
    __init__.py
    embedded/             # public TradeAdapter API (default surface)
    gateway/              # optional FastAPI + WS facade for remote consumers
    core/                 # Signal Router, Position Manager, Risk, Outcome
    marketdata/           # book / trade / BBO pub-sub fed by exchange WS
    exchanges/            # binance/, bybit/ — WS-trade + private/public WS + REST (bootstrap+reconcile)
    storage/              # SQLite schema, Redis pubsub
    secrets/              # encrypted keystore
    config.py
    main.py               # entrypoint
tests/                    # to be filled in implementation phase
docs/                     # see Documentation above
scripts/                  # operational helpers (key-management, db-migrate)
```

---

## Next steps

1. Spec review by maintainer.
2. Implementation phase per [`docs/ROADMAP.md`](docs/ROADMAP.md).
3. Integration with [`Analitics-sdk-`](https://github.com/nms6277-ops/Analitics-sdk-) — adapter ingests its market data WS streams, consumes its `Signal` objects via the universal protocol.
