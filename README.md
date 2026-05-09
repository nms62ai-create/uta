# Universal Trade Adapter

[![status](https://img.shields.io/badge/status-spec%20phase-yellow)]()
[![python](https://img.shields.io/badge/python-3.11%2B-blue)]()
[![license](https://img.shields.io/badge/license-private-red)]()

Self-hosted, single-process trading adapter for Binance and Bybit. Accepts
universal signals from external sources (analytics SDK, custom bots, UI),
manages positions, and executes orders via the appropriate exchange-specific
REST/WebSocket API. Exposes a stable HTTP+WebSocket API outward.

> **Status: planning phase.** This repository currently contains the locked
> v1.0 specification and architecture documentation. No production code yet.
> Implementation begins once the spec is signed off — see
> [`docs/spec_v1.0.md`](docs/spec_v1.0.md).

---

## Что это и зачем (TL;DR на русском)

Универсальный исполнительный модуль:

1. Подключается по WebSocket к Binance USD-M Futures и Bybit Linear для
   потоков market data + private user data (filly, ордера, позиции).
2. Принимает универсальные сигналы по REST API от любых источников —
   аналитический SDK, ручной UI, внешний бот. Все источники говорят на
   одном протоколе.
3. Открывает позиции, ставит SL/TP на бирже, ведёт state-machine на каждый
   символ.
4. Хранит API-ключи бирж зашифрованно на диске.
5. Идемпотентный — двойная отправка одного сигнала не порождает дубль.
6. Защищён emergency kill switch (max notional, max leverage) от багов в
   бот-стороне.

---

## Документация

| Документ | Что внутри |
|---|---|
| [`docs/spec_v1.0.md`](docs/spec_v1.0.md) | Locked v1.0 спецификация — 15 зафиксированных решений, контракт. |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Блок-за-блоком: API layer, Signal Router, Position Manager, Exchange Abstraction. |
| [`docs/SIGNAL_PROTOCOL.md`](docs/SIGNAL_PROTOCOL.md) | Формат `UniversalSignal`, intents, sizing, SL/TP. Контракт между потребителями и адаптером. |
| [`docs/SECURITY.md`](docs/SECURITY.md) | Модель угроз, шифрование ключей, audit log, kill switch. |
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | v1.0 / v1.1 / v2.0 — что и когда. |

---

## Locked decisions (быстрая справка)

| # | Решение | Значение |
|---|---|---|
| 0 | Repo | Отдельный (не моно с SDK) |
| 1 | Stack | Python 3.11+ asyncio + FastAPI + websockets |
| 2 | API keys | Encrypted file (master password из env) |
| 3 | Outward transport | REST (commands) + WebSocket (events) |
| 4 | Auth | Bearer token per consumer |
| 5 | State store | SQLite (truth) + Redis (cache + pub/sub) |
| 6 | Order idempotency | client_order_id (UUID) per order |
| 7 | Position model | Net-only (one-way mode) |
| 8 | SL/TP | Native exchange stop-orders, configurable |
| 9 | Reconnect | Aggressive REST reconciliation |
| 10 | Signal format | `UniversalSignal` dataclass (см. протокол) |
| 11 | Sizing | Fixed qty / % equity / risk-based — все три |
| 12 | Risk limits | Emergency kill switch only (max notional, max leverage) |
| 13 | Exchange diff | Полностью прячется за абстракцией |
| 14 | Testing | Только real exchanges (нет testnet/mock/paper) |
| 15 | Observability | structlog JSON + Prometheus metrics |

Полное обоснование каждого решения — в [`docs/spec_v1.0.md`](docs/spec_v1.0.md).

---

## Layout (пока scaffold)

```
trade_adapter/
    __init__.py
    api/                  # FastAPI routers, WS event broadcast
    core/                 # Signal Router, Position Manager, Risk Layer
    exchanges/            # binance/, bybit/ — REST + WS adapters
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
