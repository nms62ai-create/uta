"""Runnable end-to-end example with a fully mocked Binance venue.

No network, no API keys, no real exchange. The Binance adapter is
replaced by a fake that records every order it would have sent and
synthesises an ``OrderAck``. The position snapshot provider returns a
deterministic starting state.

Run it::

    python -m examples.mocked_venue

Expected output (last line is the synchronous ack from the router):

    [event] signal_received signal_id=ex-001 accepted=True
    [info] submitted 3 orders: ['ex-001-entry', 'ex-001-sl', 'ex-001-tp']
    [info] ack accepted=True duplicate=False reason=None

The exact same wiring works against a real ``BinanceUmAdapter`` —
swap ``FakeExchangeAdapter`` for the real one and pass it actual
config + API keys. See ``examples/testnet_smoke.py``.
"""

from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.position_manager import PositionManager
from trade_adapter.core.position_store import PositionStore
from trade_adapter.core.risk import RiskConfig, RiskGate, RiskState
from trade_adapter.core.signal_router import SignalRouter
from trade_adapter.embedded import TradeAdapter
from trade_adapter.storage.idempotency import IdempotencyCache
from trade_adapter.storage.sqlite import SqliteDAO
from trade_adapter.types import (
    Direction,
    EventType,
    Intent,
    NotionalUsd,
    OrderAck,
    OrderRequest,
    PctFromEntry,
    PositionUpdate,
    StopMode,
    UniversalSignal,
    Venue,
)

# ---------------------------------------------------------------------------
# Fakes — same shape used by tests/test_embedded.py, kept simple here.
# ---------------------------------------------------------------------------


@dataclass
class FakeExchangeAdapter:
    submitted: list[OrderRequest] = field(default_factory=list)
    canceled: list[tuple[str, str]] = field(default_factory=list)

    async def start(self) -> None:
        print("[info] FakeExchangeAdapter.start()")

    async def close(self) -> None:
        print("[info] FakeExchangeAdapter.close()")

    async def submit_order(
        self, req: OrderRequest, *, timeout_s: float | None = None
    ) -> OrderAck:
        self.submitted.append(req)
        return OrderAck(
            client_order_id=req.client_order_id,
            exchange_order_id=f"ex-{len(self.submitted):03d}",
            venue=req.venue,
            symbol=req.symbol,
            accepted_at=1_700_000_000.0,
        )

    async def cancel_order(
        self,
        symbol: str,
        client_order_id: str,
        *,
        timeout_s: float | None = None,
    ) -> None:
        self.canceled.append((symbol, client_order_id))


@dataclass
class FakeSnapshotProvider:
    venue: Venue = Venue.BINANCE_UM
    positions: list[PositionUpdate] = field(default_factory=list)
    equity_usd: float = 10_000.0

    async def fetch_position_snapshot(self) -> list[PositionUpdate]:
        return list(self.positions)

    async def fetch_equity_snapshot(self) -> float:
        return self.equity_usd


@dataclass
class FakeMarketData:
    prices: dict[tuple[Venue, str], float] = field(default_factory=dict)

    def get_reference_price(self, venue: Venue, symbol: str) -> float | None:
        return self.prices.get((venue, symbol))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="uta-example-"))
    dao = await SqliteDAO.open(tmp / "uta.db")

    bus = EventBus()
    store = PositionStore()
    fake_adapter = FakeExchangeAdapter()
    snapshot = FakeSnapshotProvider(equity_usd=10_000.0)
    market_data = FakeMarketData(prices={(Venue.BINANCE_UM, "BTCUSDT"): 50_000.0})

    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snapshot,
        event_bus=bus,
        reconcile_interval_s=0,
    )

    risk_state = RiskState()
    risk_gate = RiskGate(
        config=RiskConfig(max_notional_usd_per_symbol=1_000.0),
        state=risk_state,
        position_provider=store,
    )

    cache = IdempotencyCache(dao, ttl_s=3600.0)
    router = SignalRouter(
        adapter=fake_adapter,
        idempotency=cache,
        market_data=market_data,
        position_provider=store,
        equity_provider=store,
        event_bus=bus,
        risk_gate=risk_gate,
        clock=lambda: 1_700_000_000.0,
    )

    ta = TradeAdapter(
        venue=Venue.BINANCE_UM,
        exchange_adapter=fake_adapter,
        signal_router=router,
        event_bus=bus,
        position_manager=mgr,
        risk_state=risk_state,
        risk_gate=risk_gate,
    )

    sub = ta.subscribe(EventType.SIGNAL_RECEIVED)

    async with ta:
        sig = UniversalSignal(
            signal_id="ex-001",
            source="example",
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            direction=Direction.LONG,
            intent=Intent.OPEN,
            sizing=NotionalUsd(notional_usd=500.0),
            sl=PctFromEntry(pct=0.5, mode=StopMode.NATIVE),
            tp=PctFromEntry(pct=1.0, mode=StopMode.NATIVE),
            ttl_seconds=3.0,
            correlation_id="ex-001",
        )

        ack = await ta.submit_signal(sig)

        # Drain whatever the bus has accumulated synchronously.
        while not sub.queue.empty():
            payload = sub.queue.get_nowait()
            print(
                "[event] signal_received "
                f"signal_id={payload['signal']['signal_id']} "
                f"accepted={payload['ack']['accepted']}"
            )

        print(
            f"[info] submitted {len(fake_adapter.submitted)} orders: "
            f"{[o.client_order_id for o in fake_adapter.submitted]}"
        )
        print(
            f"[info] ack accepted={ack.accepted} duplicate={ack.duplicate} "
            f"reason={ack.rejection_reason}"
        )

    await sub.close()
    await dao.close()


if __name__ == "__main__":
    asyncio.run(main())
