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
> production code shipped, and a follow-up post-critique cleanup
> resolved contradictions and promoted previously-implicit safety items
> (rate-limit pre-throttling, time sync, cancel-on-disconnect,
> backpressure, light-strategy disclosure, BBO auto-subscribe) to
> numbered decisions. Total locked decisions: 23 (A0–A22, including
> A3b). Implementation starts at Phase 1 per
> [`docs/ROADMAP.md`](docs/ROADMAP.md); latency is treated as
> best-effort given the chosen stack — observed numbers go in the
> operations runbook after Phase 2, not into the spec as targets.

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
| [`docs/spec_v1.0.md`](docs/spec_v1.0.md) | v1.0 спецификация — 23 решения (A0–A22, включая A3b), контракт. |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Блок-за-блоком: embedded API, gateway, Signal Router, Position Manager, Exchange Abstraction, market-data passthrough, in-process event bus, time-sync, rate-limit. |
| [`docs/SIGNAL_PROTOCOL.md`](docs/SIGNAL_PROTOCOL.md) | Формат `UniversalSignal` (с `correlation_id`), все события, в т.ч. `book_update`/`trade_print`/`bbo_update`/`outcome_report`; `position_update` теперь несёт `liquidation_price`/`unrealized_pnl_usd`/`margin_used_usd`. |
| [`docs/SECURITY.md`](docs/SECURITY.md) | Модель угроз, шифрование ключей, audit log, kill switch. |
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | v1.0 / v1.1 / v2.0 — что и когда. Старт с Phase 1 (типы + storage + event_bus). |

---

## Locked decisions (быстрая справка)

| # | Решение | Значение |
|---|---|---|
| 0 | Repo | Отдельный (не моно с SDK) |
| 1 | Stack | Python 3.11+ asyncio + websockets. Core install — 7 wheels, без FastAPI/Redis/Pydantic/Numpy. Латенси — «что даёт стек» (все выравнивающие решения: WS-first, audit офф hot path, bounded queue, rate-limit pre-throttle); измеряем на интегрированном адаптере в Phase 4, в спеку цифры не пишутся |
| 2 | API keys | Encrypted file (master password из env) |
| 3 | Outward transport | Embedded Python API (дефолт) + опциональный REST/WS gateway за `[gateway]` extras |
| 3b | Exchange transport | WebSocket-first: торговля и user-data по WS. REST — только bootstrap, reconcile, явный fallback |
| 4 | Auth | Bearer token per consumer (gateway-режим; embedded — тот же trust domain) |
| 5 | State store | SQLite (truth) + in-process `asyncio.Queue` event bus + `dict` hot-cache. Redis НЕ входит в v1.0 — только в `[multiproc]` extras для будущего multi-process развёртывания |
| 6 | Order idempotency | client_order_id (UUID) per order |
| 7 | Position model | Net-only (one-way mode) |
| 8 | SL/TP | Нативные стопы на бирже, отправляются параллельно с ордером входа в одном `place_order`. На Binance UM — `STOP_MARKET closePosition=true` ребёнок шлётся одновременно с входом, без "naked" окна |
| 9 | Reconnect | Aggressive REST reconciliation, in-process `asyncio.Lock` per venue |
| 10 | Signal format | `UniversalSignal` dataclass + `correlation_id` (см. протокол) |
| 11 | Sizing | Fixed qty / **notional in $** / % equity / risk-based — все четыре (`NotionalUsd` — для UI-сценариев, где оператор задаёт объём в долларах) |
| 12 | Risk limits | Emergency kill switch only (max notional, max leverage) |
| 13 | Exchange diff | Полностью прячется за абстракцией |
| 14 | Testing | Real exchanges + in-process mock-биржа для unit-тестов чистой логики (testnet/paper-runtime по-прежнему out of scope) |
| 15 | Observability | structlog JSON + Prometheus metrics (включая `uta_order_send_latency_seconds`, `uta_event_dispatch_latency_seconds`, `uta_rest_fallback_total`, `uta_event_bus_drops_total`, `uta_rate_limit_local_rejects_total`) |
| 16 | Market data publish | `book_update` / `trade_print` / `bbo_update` отдаются подписчикам; одно WS-соединение на (venue, symbol) на всех |
| 17 | Rate-limit pre-throttling | Token-bucket per (venue, endpoint-class), порог ниже лимита биржи; over-quota → локальный `RATE_LIMITED`, не уходит на биржу |
| 18 | Time sync | Server-time offset на venue, refresh каждый час; drift > ±2s → adapter не стартует |
| 19 | Cancel-on-disconnect | Off by default; opt-in per venue |
| 20 | Backpressure | Bounded `asyncio.Queue` per subscriber + drop-oldest; trading hot path не блокируется медленными подписчиками |
| 21 | Light strategy | Адаптер делает intent resolution и risk-based sizing — это явно задокументировано; producer может обходить через `intent=ADD/REVERSE` + `FixedQty` |
| 22 | BBO auto-subscribe | На каждый открытый position — авто-подписка на BBO для честного MFE/MAE из тиков |

Полное обоснование каждого решения — в [`docs/spec_v1.0.md`](docs/spec_v1.0.md).

---

## Layout (пока scaffold)

```
trade_adapter/
    __init__.py
    embedded/             # public TradeAdapter API (default surface) + sync wrapper
    gateway/              # optional FastAPI + WS facade for remote consumers ([gateway] extras)
    core/                 # Signal Router, Position Manager, Risk, Outcome, Reconciliation
    marketdata/           # book / trade / BBO pub-sub fed by exchange WS
    exchanges/            # binance/, bybit/ — WS-trade + private/public WS + REST (bootstrap+reconcile)
    bus/                  # in-process event bus (bounded asyncio.Queue per subscriber)
    storage/              # SQLite schema + DAO, audit flusher, idempotency cache
    infra/                # time_sync, rate_limit
    secrets/              # encrypted keystore
    config.py
    main.py               # entrypoint
tests/                    # unit + protocol golden-snapshot
docs/                     # see Documentation above
scripts/                  # operational helpers (key-management, db-migrate)
```

---

## Next steps

1. Spec review by maintainer.
2. Implementation phases 1–4 per [`docs/ROADMAP.md`](docs/ROADMAP.md). Phase 1 = `types.py` + storage + event bus + golden-snapshot test; no network, no keys.
3. Integration with the embedded `heatmap-sdk` consumer (first real producer; market-data passthrough + `OutcomeReport` feedback).
