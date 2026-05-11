# Integrating heatmap-sdk with UTA's embedded `TradeAdapter`

This document is the **exact wiring recipe** for plugging UTA in as the
order-execution backend behind heatmap-sdk's existing UI + adaptive
analytics. It assumes:

* You already have heatmap-sdk running locally (FastAPI + browser UI).
* You already have UTA installed in the same virtualenv (`pip install -e .`
  from the UTA repo root, or `pip install universal-trade-adapter` once
  it's published).
* You want to keep heatmap-sdk's UI, analytics (`adaptive_sdk`), and
  WebSocket browser channel untouched; only the *order execution*
  side moves from heatmap-sdk's own `OrderExecutor` + Binance REST
  client to UTA.

## Architecture at a glance

```
                  +-------------------------+
   heatmap-sdk    | LiveHeatmapService      |
   (your repo)    |   AdaptiveMarketService |---- Signal --+
                  |   BinanceMarketClient   |              |
                  +-------------------------+              v
                                                +-----------------------+
                                                | HeatmapAutoTrader     |
                                                | (UTA integration)     |
                                                |  -> build_universal_  |
                                                |     signal()          |
                                                +----------+------------+
                                                           |
                                                           v
                                                +-----------------------+
                                                | TradeAdapter          |
                                                | (UTA embedded API)    |
                                                |  signal_router ->     |
                                                |  exchange_adapter ->  |
                                                |  Binance UM venue     |
                                                +----------+------------+
                                                           |
                                                           v event bus
                                                +-----------------------+
                                                | HeatmapEventBroadcast |
                                                | (UTA integration)     |
                                                |  -> to_ws_payload()   |
                                                +----------+------------+
                                                           |
                                                           v
                                                LiveHeatmapService.broadcast
                                                (heatmap-sdk's existing WS)
```

heatmap-sdk's existing browser code does not change. The change is in
two places inside `app/ws_session.py`:

1. **Boot** — construct the `TradeAdapter`, `HeatmapAutoTrader`, and
   `HeatmapEventBroadcaster` once when `LiveHeatmapService` starts.
2. **Per-signal callback** — in `_on_market_trade`, when
   `AdaptiveMarketService.on_agg_trade(...)` returns a non-`None`
   `Signal`, forward it to `autotrader.on_adaptive_signal(signal)`.

That's it. Everything else (settings UI, market-data fan-out, browser
WS) is reused.

## Step 1: settings from the UI

heatmap-sdk's existing UI already collects the four values the
autotrader needs: symbol, $ notional, SL %, TP %. Wire them through
your existing settings endpoint:

```python
# app/ws_session.py — inside LiveHeatmapService

from trade_adapter.integrations.heatmap_sdk import (
    AutotradeSettings,
    HeatmapAutoTrader,
)
from trade_adapter.types import Venue

def on_settings_update(self, payload: dict) -> None:
    settings = AutotradeSettings(
        symbol=payload["symbol"],
        venue=Venue.BINANCE_UM,
        notional_usd=float(payload["notional_usd"]),
        sl_pct=float(payload["sl_pct"]) if payload.get("sl_pct") else None,
        tp_pct=float(payload["tp_pct"]) if payload.get("tp_pct") else None,
        min_confidence=float(payload.get("min_confidence", 0.0)),
        ttl_seconds=5.0,
        enabled=bool(payload.get("autotrade_enabled", False)),
    )
    self.autotrader.update_settings(settings)
```

`AutotradeSettings` is a `slots=True, frozen=True` dataclass — any
field validation error raises `InvalidAutotradeSettings`. Catch it
and surface the message in the UI.

## Step 2: build the UTA stack on service start

UTA uses **explicit dependency injection** — there is no
`BinanceUmAdapter.from_config(...)`. You build the four transports
(REST + WS-trade + user-stream + optional market-stream), then hand
them to `BinanceUmAdapter`. This keeps the venue adapter testable and
forces production callers to be explicit about which transports they
actually need.

```python
# app/ws_session.py — at the top of LiveHeatmapService.start()

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.position_manager import PositionManager
from trade_adapter.core.position_store import PositionStore
from trade_adapter.core.risk import RiskConfig, RiskGate, RiskState
from trade_adapter.core.signal_router import SignalRouter
from trade_adapter.embedded import TradeAdapter
from trade_adapter.exchanges.binance_um import (
    BinanceRestClient,
    BinanceUmAdapter,
    BinanceUmSnapshotProvider,
    DEFAULT_BASE_URL,
    TESTNET_BASE_URL,
    make_user_data_handlers,
)
from trade_adapter.exchanges.binance_um.ws.trade import BinanceWsTradeClient
from trade_adapter.exchanges.binance_um.ws.transport import WsRpcClient
from trade_adapter.exchanges.binance_um.ws.user_stream import UserDataStreamClient
from trade_adapter.integrations.heatmap_sdk import (
    HeatmapAutoTrader,
    HeatmapEventBroadcaster,
)
from trade_adapter.storage.idempotency import IdempotencyCache
from trade_adapter.storage.sqlite import SqliteDAO
from trade_adapter.types import Venue


class _LastTradeMarketData:
    """Thin :class:`MarketDataProvider` over heatmap-sdk's own market client.

    heatmap-sdk's :class:`BinanceMarketClient` already streams aggTrade
    frames. We just need the last trade price per (venue, symbol) so
    that ``SignalRouter`` can resolve ``NotionalUsd`` → ``qty``.
    """

    def __init__(self) -> None:
        self._prices: dict[tuple[Venue, str], float] = {}

    def update(self, venue: Venue, symbol: str, price: float) -> None:
        self._prices[(venue, symbol.upper())] = float(price)

    def get_reference_price(self, venue: Venue, symbol: str) -> float | None:
        return self._prices.get((venue, symbol.upper()))


async def _build_uta(self) -> None:
    """Construct UTA stack. Called once on service boot."""

    creds = load_binance_account_settings()  # heatmap-sdk's existing loader

    self._dao = SqliteDAO("./uta_state.db")
    await self._dao.open()

    # ---- shared infra ----------------------------------------------------
    self._bus = EventBus()
    self._store = PositionStore()
    self._risk_state = RiskState()
    self._risk_gate = RiskGate(
        config=RiskConfig(),
        state=self._risk_state,
        position_provider=self._store,
    )
    self._market_data = _LastTradeMarketData()
    self._idempotency = IdempotencyCache(self._dao, ttl_s=3600.0)

    # ---- venue transports ------------------------------------------------
    base_url = TESTNET_BASE_URL if creds.testnet else DEFAULT_BASE_URL
    self._rest = BinanceRestClient(
        api_key=creds.api_key,
        api_secret=creds.api_secret,
        base_url=base_url,
    )
    ws_url = (
        "wss://testnet.binancefuture.com/ws-fapi/v1"
        if creds.testnet
        else "wss://ws-fapi.binance.com/ws-fapi/v1"
    )
    self._ws_trade = BinanceWsTradeClient(
        rpc=WsRpcClient(url=ws_url),
        api_key=creds.api_key,
        api_secret=creds.api_secret,
        own_rpc=True,
    )

    # ---- position manager + user-data handlers ---------------------------
    snapshot_provider = BinanceUmSnapshotProvider(rest=self._rest)
    self._position_manager = PositionManager(
        venue=Venue.BINANCE_UM,
        store=self._store,
        snapshot_provider=snapshot_provider,
        event_bus=self._bus,
    )
    self._user_stream = UserDataStreamClient(
        rest=self._rest,
        handlers=make_user_data_handlers(
            venue=Venue.BINANCE_UM,
            position_manager=self._position_manager,
            event_bus=self._bus,
            risk_state=self._risk_state,
        ),
    )

    # ---- venue adapter + signal router (adapter first) -------------------
    self._exchange = BinanceUmAdapter(
        rest=self._rest,
        ws_trade=self._ws_trade,
        user_stream=self._user_stream,
    )
    self._router = SignalRouter(
        adapter=self._exchange,
        idempotency=self._idempotency,
        market_data=self._market_data,
        position_provider=self._store,
        equity_provider=self._store,
        event_bus=self._bus,
        risk_gate=self._risk_gate,
    )

    # ---- embedded TradeAdapter + bridges --------------------------------
    self.trade_adapter = TradeAdapter(
        venue=Venue.BINANCE_UM,
        exchange_adapter=self._exchange,
        signal_router=self._router,
        event_bus=self._bus,
        position_manager=self._position_manager,
        risk_state=self._risk_state,
        risk_gate=self._risk_gate,
    )
    await self.trade_adapter.start()

    self.autotrader = HeatmapAutoTrader(self.trade_adapter)
    self.broadcaster = HeatmapEventBroadcaster(
        adapter=self.trade_adapter,
        send=self.broadcast,  # heatmap-sdk's existing browser fan-out
    )
    self.broadcaster.start()


async def _teardown_uta(self) -> None:
    """Reverse of _build_uta. Called on service stop."""

    await self.broadcaster.stop()
    await self.trade_adapter.close()
    await self._dao.close()
```

Whenever heatmap-sdk's existing `BinanceMarketClient.on_trade(...)`
fires, also call `self._market_data.update(Venue.BINANCE_UM, symbol,
last_price)` so the router has a fresh price to size $-notional
signals against.

## Step 3: forward adaptive signals into the autotrader

heatmap-sdk's `_on_market_trade` already produces `adaptive_sdk.Signal`
objects from `AdaptiveMarketService.on_agg_trade`. Forward them:

```python
# app/ws_session.py — inside LiveHeatmapService

async def _on_market_trade(self, trade: TradeTick) -> None:
    signal = self.adaptive_market.on_agg_trade(trade)
    if signal is None:
        return
    # Existing UI bookkeeping…
    self._latest_signal = signal
    # NEW: hand the signal to UTA via the autotrader.
    ack = await self.autotrader.on_adaptive_signal(signal)
    if ack is not None and ack.accepted:
        _log.info("UTA accepted signal %s", signal.signal_id)
```

Everything else — confidence filtering, position-already-open guard,
duplicate suppression — happens inside `HeatmapAutoTrader` and inside
UTA. The integration callsite is one line.

## Step 4: retire the old `OrderExecutor` exit path

heatmap-sdk's `ExitEngine` currently calls `OrderExecutor.close_position`
on hard exits. Two equivalent options:

* **Option A (recommended)** — let UTA's native SL / TP orders handle
  exits. `AutotradeSettings` already carries `sl_pct` and `tp_pct`,
  which UTA submits as `STOP_MARKET` / `TAKE_PROFIT_MARKET` orders on
  the venue side. Delete `ExitEngine` and `OrderExecutor`.
* **Option B (gentle migration)** — keep `ExitEngine` for the
  "max_holding_time" and "toxic_vpin" soft exits, but route those
  through UTA too:

  ```python
  # Replace: await self.order_executor.close_position(position)
  # With:
  reverse_signal = UniversalSignal(
      signal_id=f"exit-{position.symbol}-{int(now_ms)}",
      source="heatmap_sdk_exit_engine",
      symbol=position.symbol,
      venue=Venue.BINANCE_UM,
      direction=position.direction,
      intent=Intent.CLOSE,
      sizing=FixedQty(qty=position.qty),
      sl=None, tp=None, ttl_seconds=2.0,
  )
  await self.trade_adapter.submit_signal(reverse_signal)
  ```

  This keeps the spec contract for exits identical to entries.

## Step 5: optional — kill switch from the UI

If your UI has an emergency-stop button, wire it to UTA's kill switch:

```python
async def on_emergency_stop(self) -> None:
    self.trade_adapter.trip_kill_switch("ui-emergency-stop")
    # UTA will now reject every new submit_signal and publish an
    # ALERT event that HeatmapEventBroadcaster will surface to the
    # browser.
```

## What you do not need to change

* heatmap-sdk's market-data fan-out (`BinanceMarketClient`) — UTA does
  not interfere with the public stream subscription.
* heatmap-sdk's frame builder, OBI metrics, browser WebSocket payloads.
* `adaptive_sdk` — UTA reads `Signal` structurally, no version coupling.

## Migration smoke test

After wiring is done, the smallest end-to-end sanity check is:

1. Start heatmap-sdk in **testnet** mode (the Binance Futures testnet —
   point `BINANCE_API_URL` and the WS streams to the testnet hosts).
2. Open the UI. Set BTCUSDT, $50 notional, 0.3% SL, 0.6% TP, enable
   autotrade.
3. Wait for an exhaustion signal. Verify:
   * Browser shows `{"type": "order_status", "status": "FILLED", …}`.
   * `trade_adapter.get_position(Venue.BINANCE_UM, "BTCUSDT")` returns
     a non-`None` `Position`.
   * Browser shows `{"type": "position", "side": "LONG"|"SHORT", …}`.
   * On SL/TP fill, browser shows another `order_status` + an
     `{"type": "outcome", …}` with `realized_pnl_usd`.

If any of these are missing, check `_log.warning` lines from
`HeatmapEventBroadcaster` — a broken `send(...)` swallows its own
errors but logs them.
