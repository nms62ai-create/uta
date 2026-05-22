# Quickstart — embed UTA into your own program

This is the shortest path from "I have a strategy that produces buy/sell
decisions" to "the adapter places real orders on Binance UM Futures and
keeps positions consistent with the venue".

UTA is a Python library, not a service. There is no daemon to start, no
container to launch. You construct a
[`TradeAdapter`](../trade_adapter/embedded.py) in your own process, feed
it [`UniversalSignal`](../trade_adapter/types.py) objects, and read
events back from its bus.

---

## 1. Install

```bash
python -m pip install -e .          # from this repo (editable)
# or, if you have a built wheel:
python -m pip install ./dist/trade_adapter-*.whl
```

Python 3.11+ is required. The core install pulls 7 wheels — no
FastAPI, no Redis, no Pydantic, no numpy. See `pyproject.toml`.

---

## 2. The 30-line embed

The minimal real-venue setup wires four pieces by hand:

```python
import asyncio

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.position_manager import PositionManager
from trade_adapter.core.position_store import PositionStore
from trade_adapter.core.risk import RiskConfig, RiskGate, RiskState
from trade_adapter.core.signal_router import SignalRouter
from trade_adapter.embedded import TradeAdapter
from trade_adapter.exchanges.binance_um.adapter import (
    BinanceUmAdapter, BinanceUmAdapterConfig,
)
from trade_adapter.exchanges.binance_um.rest import BinanceRestClient
from trade_adapter.exchanges.binance_um.snapshot import BinanceUmSnapshotProvider
from trade_adapter.exchanges.binance_um.symbols import SymbolRegistry
from trade_adapter.storage.idempotency import IdempotencyCache
from trade_adapter.storage.sqlite import SqliteDAO
from trade_adapter.types import (
    Direction, FixedQty, Intent, UniversalSignal, Venue,
)


async def main() -> None:
    # --- shared infra ------------------------------------------------
    dao = await SqliteDAO.open("uta.db")     # idempotency + audit
    bus = EventBus()                          # in-process pub/sub
    store = PositionStore()                   # truth for live positions
    cache = IdempotencyCache(dao, ttl_s=3600) # dedup by signal_id

    # --- venue stack (REST + WS-trade + user-stream + market-stream) -
    rest = BinanceRestClient(api_key=..., api_secret=..., base_url=...)
    symbols = SymbolRegistry(rest)
    snapshot = BinanceUmSnapshotProvider(rest)
    # ...wire WS clients (ws_trade, ws_user, ws_market), translators...
    binance = BinanceUmAdapter(
        config=BinanceUmAdapterConfig(...),
        # ...
    )

    # --- core --------------------------------------------------------
    mgr = PositionManager(
        venue=Venue.BINANCE_UM, store=store,
        snapshot_provider=snapshot, event_bus=bus,
    )
    risk_state = RiskState()
    risk_gate = RiskGate(
        config=RiskConfig(max_notional_usd_per_symbol=500.0),
        state=risk_state, position_provider=store,
    )
    router = SignalRouter(
        adapter=binance, idempotency=cache,
        market_data=binance.market_data, position_provider=store,
        equity_provider=store, event_bus=bus, risk_gate=risk_gate,
    )

    # --- public surface ---------------------------------------------
    ta = TradeAdapter(
        venue=Venue.BINANCE_UM,
        exchange_adapter=binance, signal_router=router, event_bus=bus,
        position_manager=mgr, risk_state=risk_state, risk_gate=risk_gate,
    )

    async with ta:                            # start() + close()
        ack = await ta.submit_signal(UniversalSignal(
            signal_id="...",                  # UUIDv4 from your code
            source="my_strategy",
            symbol="BTCUSDT", venue=Venue.BINANCE_UM,
            direction=Direction.LONG, intent=Intent.OPEN,
            sizing=FixedQty(qty=0.001),
            sl=None, tp=None, ttl_seconds=5.0,
            correlation_id=None,
        ))
        assert ack.accepted


asyncio.run(main())
```

A runnable, fully-mocked variant of the same wiring (no network, no
keys) lives in [`examples/mocked_venue.py`](../examples/mocked_venue.py).

A real-testnet variant (Binance UM testnet, your keys, your symbols)
lives in [`examples/testnet_smoke.py`](../examples/testnet_smoke.py).

---

## 3. What you actually do with `TradeAdapter`

| You want to… | API call |
|---|---|
| Open / close / reduce a position | `await ta.submit_signal(signal)` |
| Cancel a specific working order | `await ta.cancel_order(symbol, client_order_id)` |
| Read current position state | `ta.get_position(venue, symbol)`, `ta.open_positions()` |
| Read venue equity (USD) | `ta.get_equity_usd(venue)` |
| Subscribe to events (orders, fills, positions, market data) | `ta.subscribe(EventType.ORDER_UPDATE)` |
| Emergency stop | `ta.trip_kill_switch("manual")`, `ta.reset_kill_switch()` |
| Lifecycle | `await ta.start()`, `await ta.close()` (or `async with ta:`) |

The single submit method does:

1. **Idempotency check** — same `signal_id` replays the cached
   `SignalAck`.
2. **Schema sanity** + **intent resolution** against the live position.
3. **Reference price** from cached BBO.
4. **Stop math** — `pct` / `bps` / `absolute` → absolute price.
5. **Sizing math** — `fixed_qty` / `notional_usd` / `pct_equity` /
   `risk_based` → contract qty.
6. **Risk gate** — kill switch, daily loss, per-symbol caps.
7. **Submit** — entry MARKET order + optional SL/TP children
   (`STOP_MARKET` / `TAKE_PROFIT_MARKET` with
   `closePosition=true`), all dispatched in parallel via
   `asyncio.gather`.
8. **Publish** `SIGNAL_RECEIVED` on the event bus.
9. **Cache** the ack for retry safety.

The whole accept path returns a `SignalAck`. Venue errors during
the actual submit propagate as exceptions — the caller decides
whether to retry.

---

## 4. Reading events back

The adapter writes everything it does to a single in-process bus
(`trade_adapter.bus.event_bus.EventBus`). Subscribe with
`ta.subscribe(EventType.X)` and read JSON-serialisable payloads off the
returned `Subscription.queue`:

```python
from trade_adapter.types import EventType

orders = ta.subscribe(EventType.ORDER_UPDATE)
fills  = ta.subscribe(EventType.FILL)
poses  = ta.subscribe(EventType.POSITION_UPDATE)

while True:
    payload = await orders.queue.get()
    print("order", payload)
```

Available topics (full list in
[`docs/SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md)):

* `signal_received` — every `submit_signal` ack
* `order_update` — venue-side state changes (NEW / PARTIALLY_FILLED / FILLED / CANCELED …)
* `fill` — individual trade execution prints
* `position_update` — net position changes per `(venue, symbol)`
* `book_update`, `trade_print`, `bbo_update` — market-data passthrough
* `outcome_report` — emitted when a position closes (MAE / MFE / realized PnL)
* `reconcile_diff` — drift between local store and venue snapshot
* `alert` — internal warnings (rate-limit, time drift, …)

The bus is **drop-oldest with bounded queues** (default 256). A slow
consumer cannot stall the trading hot path. Consume promptly or accept
that you'll miss frames.

---

## 5. Where to go next

* [`docs/EXAMPLE.md`](EXAMPLE.md) — end-to-end walkthrough of one
  signal from `submit_signal` to position close.
* [`docs/MODULES.md`](MODULES.md) — per-file reference: every module
  under `trade_adapter/`, what it does, what it doesn't do.
* [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) — block-by-block diagram of
  how the embedded API, signal router, position manager, venue adapter,
  and event bus fit together.
* [`docs/SIGNAL_PROTOCOL.md`](SIGNAL_PROTOCOL.md) — the
  `UniversalSignal` wire format plus every event the adapter publishes.
* [`docs/spec_v1.0.md`](spec_v1.0.md) — locked design decisions
  (A0–A22) with the reasoning behind each one.
