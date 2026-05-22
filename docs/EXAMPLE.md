# Worked example — one signal, end to end

What actually happens between
`await ta.submit_signal(sig)` returning a `SignalAck` and the position
appearing on Binance. Same flow whether you use the runnable
[`examples/mocked_venue.py`](../examples/mocked_venue.py) or wire your
own producer; this doc just narrates one trip through the stack.

---

## Setup

Producer state:

* Operator wants to open `LONG BTCUSDT` on Binance UM.
* Budget: $500 notional.
* Protective stops: `SL = 0.5%`, `TP = 1.0%`.
* Cached BBO mid for `BTCUSDT` is `50_000.00 USD`.
* Account: net-mode, no existing position on `BTCUSDT`.
* Risk config: `max_notional_usd_per_symbol = 1_000.0`.

The producer constructs the signal:

```python
sig = UniversalSignal(
    signal_id="a1b2c3d4-...",          # UUIDv4 from the producer
    source="my_strategy",
    symbol="BTCUSDT",
    venue=Venue.BINANCE_UM,
    direction=Direction.LONG,
    intent=Intent.OPEN,
    sizing=NotionalUsd(notional_usd=500.0),
    sl=PctFromEntry(pct=0.5, mode=StopMode.NATIVE),
    tp=PctFromEntry(pct=1.0, mode=StopMode.NATIVE),
    ttl_seconds=3.0,
    correlation_id="op-1-2026-05",
)

ack = await ta.submit_signal(sig)
```

---

## What happens inside `submit_signal`

### Step 1 — Idempotency check (`storage/idempotency.py`)

Router looks up `signal_id` in the in-memory cache (and falls through
to SQLite on miss). First-time signal → miss → continue.

If the producer retried the same `signal_id` later (e.g. after a
crash), the router would replay the original `SignalAck` with
`duplicate=True` and skip every step below — no second order.

### Step 2 — Schema sanity (`core/signal_router.py`)

* `ttl_seconds = 3.0` > 0 → ok.
* `symbol = "BTCUSDT"` non-empty → ok.

If either fails, the router returns
`SignalAck(accepted=False, rejection_reason=SCHEMA)` and stops.

### Step 3 — Intent resolution (`core/protocols.py::PositionProvider`)

* `position_provider.get_position(BINANCE_UM, "BTCUSDT")` → `None`
  (no existing position).
* `signal.intent is Intent.OPEN` and no position exists → ok.

If a position already existed, the router would reject with
`INVALID_INTENT` — `OPEN` on top of an open position is a protocol
error (use `ADD` instead, reserved for Phase 3e).

### Step 4 — Reference price (`core/protocols.py::MarketDataProvider`)

* `market_data.get_reference_price(BINANCE_UM, "BTCUSDT")` → `50_000.0`.

This is the cached BBO mid populated by the venue's market stream
(`exchanges/binance_um/ws/market.py`). If no BBO frame has arrived yet
the router rejects with `UNKNOWN_SYMBOL` — the producer should
subscribe + wait.

### Step 5 — Stop math (`core/stops.py`)

`compute_protective_price` runs for both legs:

* `sl_price = compute_protective_price(PctFromEntry(0.5), entry=50_000, LONG, "sl")`
  → `50_000 * (1 - 0.005)` = `49_750.0`.
* `tp_price = compute_protective_price(PctFromEntry(1.0), entry=50_000, LONG, "tp")`
  → `50_000 * (1 + 0.010)` = `50_500.0`.

(For SHORT the math flips; for `absolute` / `bps` / `atr_multiple`
each variant has its own formula. See module docstrings.)

### Step 6 — Sizing math (`core/sizing.py`)

`compute_target_qty(NotionalUsd(500.0), reference_price=50_000.0, ...)`
→ `500.0 / 50_000.0` = `0.01 BTC`.

Other sizing variants:

| Variant | Formula |
|---|---|
| `FixedQty(0.01)` | `0.01` |
| `NotionalUsd(500.0)` | `500.0 / reference_price` |
| `PctEquity(2.0)` | `(equity_usd * 0.02) / reference_price` (needs `EquityProvider`) |
| `RiskBased(risk_pct=1.0)` | `(equity_usd * 0.01) / abs(entry - sl_price)` (requires `sl_price`) |

### Step 7 — Risk gate (`core/risk.py`)

`RiskGate.evaluate(signal, target_qty=0.01, reference_price=50_000.0)`
runs three checks in order:

1. `state.kill_switch_tripped` → `False`.
2. `daily_realized_pnl` vs `max_daily_loss_usd` → no cap configured.
3. `target_qty * reference_price = 500.0 ≤ max_notional_usd_per_symbol = 1_000.0`
   → ok.
4. `max_position_qty_per_symbol` → not configured.

Decision: `allow()`.

If any cap had been breached, the router would return
`SignalAck(accepted=False, rejection_reason=EMERGENCY_LIMIT_EXCEEDED)`
without sending anything to the venue.

### Step 8 — Submit (`core/signal_router.py::_submit_orders`)

Three coroutines fire in parallel via `asyncio.gather`:

```python
entry = OrderRequest(
    client_order_id="a1b2c3d4-entry",     # signal_id[:24] + "-entry"
    venue=BINANCE_UM, symbol="BTCUSDT",
    side=BUY, order_type=MARKET,
    qty=0.01, time_in_force=GTC,
    reduce_only=False, close_position=False,
    ...
)
sl_order = OrderRequest(
    client_order_id="a1b2c3d4-sl",
    venue=BINANCE_UM, symbol="BTCUSDT",
    side=SELL, order_type=STOP_MARKET,
    qty=0.01, stop_price=49_750.0,
    close_position=True,
    ...
)
tp_order = OrderRequest(
    client_order_id="a1b2c3d4-tp",
    venue=BINANCE_UM, symbol="BTCUSDT",
    side=SELL, order_type=TAKE_PROFIT_MARKET,
    qty=0.01, stop_price=50_500.0,
    close_position=True,
    ...
)
await asyncio.gather(
    adapter.submit_order(entry),
    adapter.submit_order(sl_order),
    adapter.submit_order(tp_order),
)
```

Each call goes through
`BinanceUmAdapter._build_place_order_params`, which:

* Rounds `qty` to the symbol's `stepSize`.
* Rounds `stop_price` to the symbol's `tickSize`.
* Adds `recvWindow`, `timestamp`, HMAC signature.

Then `WsRpcClient` ships the signed frame on
`wss://ws-fapi.binance.com/ws-fapi/v1` and awaits the matching reply
by `id`. Binance's error envelope is unwrapped to typed exceptions
(`BinanceUmAdapterError` family).

SL/TP use `closePosition=true` — they're not standalone working
orders, they're attached to the net position and auto-cancel when
the position is flat. **No naked window** between entry fill and
stop placement (decision A8).

If any of the three fails, `asyncio.gather` re-raises the first
exception. The accept path does not mask venue errors as rejections —
the caller gets the full Binance error and decides whether to retry.
Idempotency is **not** cached on raise, so a retry actually re-runs.

### Step 9 — Publish `SIGNAL_RECEIVED` (`bus/event_bus.py`)

Router publishes one event on the bus:

```python
event_bus.publish("signal_received", {
    "signal": signal_to_wire(sig),
    "ack": signal_ack_to_wire(ack),
})
```

Anyone subscribed via `ta.subscribe(EventType.SIGNAL_RECEIVED)` sees
it on their `Subscription.queue`.

### Step 10 — Cache the ack

Router writes the ack to the idempotency cache. Same `signal_id`
later replays this exact ack with `duplicate=True`.

---

## What the producer gets back

```python
ack = SignalAck(
    signal_id="a1b2c3d4-...",
    accepted=True,
    duplicate=False,
    rejection_reason=None,
    ts=1716_393_025.123,
    correlation_id="op-1-2026-05",
)
```

The actual fills arrive **asynchronously** on the bus. The producer
typically subscribes to `ORDER_UPDATE`, `FILL`, and `POSITION_UPDATE`
before calling `submit_signal` and reads them off the queues.

---

## What happens after the venue fills the entry

Binance's user-data stream emits an `ORDER_TRADE_UPDATE` (FILLED for
the entry, then PARTIALLY_FILLED → FILLED for the children as price
moves) plus `ACCOUNT_UPDATE` for the new position.

`exchanges/binance_um/user_handlers.py` ingests each frame and:

1. Translates the frame to typed `OrderUpdate` / `Fill` /
   `PositionUpdate` via `translators.py`.
2. Publishes each on the event bus
   (`ORDER_UPDATE`, `FILL`, `POSITION_UPDATE`).
3. Calls `position_manager.apply_position_update(...)` /
   `apply_fill(...)`, which writes into `PositionStore`.

The producer's subscribers see:

```python
order_update = {
    "client_order_id": "a1b2c3d4-entry",
    "exchange_order_id": "12345",
    "status": "FILLED",
    "filled_qty": 0.01,
    "avg_fill_price": 50_001.20,
    ...
}
fill = {
    "exchange_order_id": "12345",
    "qty": 0.01,
    "price": 50_001.20,
    "fee_usd": 0.02,
    ...
}
position_update = {
    "symbol": "BTCUSDT",
    "qty": 0.01,
    "entry_price": 50_001.20,
    "state": "OPEN",
    "unrealized_pnl_usd": 0.0,
    "liquidation_price": 47_500.0,
    ...
}
```

`ta.get_position(BINANCE_UM, "BTCUSDT")` now returns a non-None
`Position`. `ta.open_positions()` includes it.

---

## What happens when an SL or TP fires

Same flow, but with `position_update.state = "CLOSED"` and
`position_update.qty = 0`. The position manager emits an
`OUTCOME_REPORT` event:

```python
outcome = {
    "symbol": "BTCUSDT",
    "side": "LONG",
    "entry_price": 50_001.20,
    "exit_price": 49_750.00,
    "qty": 0.01,
    "realized_pnl_usd": -2.51,
    "fees_usd": 0.04,
    "mae_pct": 0.5,                       # max adverse excursion
    "mfe_pct": 0.18,                      # max favourable excursion
    "close_reason": "STOP_LOSS",
    ...
}
```

`OutcomeReport` is the feedback loop signal — strategies that use a
bandit / MAB to learn from results read this topic.

If the stop sweeps but the position re-opens via another signal, the
producer chooses whether to honour idempotency by reusing the same
`signal_id` (no-op) or generating a fresh UUID (new trade).

---

## When things go wrong

| Failure | What you see |
|---|---|
| Duplicate `signal_id` | `SignalAck(accepted=True/False, duplicate=True, ...)` — original response replayed |
| `ttl_seconds <= 0` or empty symbol | `rejection_reason=SCHEMA` |
| BBO not yet cached | `rejection_reason=UNKNOWN_SYMBOL` |
| Existing position blocks `OPEN` | `rejection_reason=INVALID_INTENT` |
| `RiskBased` sizing with no `sl` | `rejection_reason=SIZING_REQUIRES_SL` |
| Kill switch tripped | `rejection_reason=EMERGENCY_LIMIT_EXCEEDED`, detail `"kill_switch: ..."` |
| Daily-loss cap breached | `rejection_reason=EMERGENCY_LIMIT_EXCEEDED`, detail `"daily_loss_exceeded: ..."`, kill switch auto-trips |
| Per-symbol notional cap breached | `rejection_reason=EMERGENCY_LIMIT_EXCEEDED`, detail `"notional_cap: ..."` |
| Venue rejects the entry order | exception propagates from `submit_signal`; idempotency **not** cached; caller can retry |
| User-data-stream drop | reconcile loop catches up at the next interval; `RECONCILE_DIFF` published |
