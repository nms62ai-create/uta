"""Real Binance USD-M Futures **testnet** smoke test.

Wires the full Phase 3g stack against the public testnet at
``https://testnet.binancefuture.com`` and
``wss://stream.binancefuture.com``, places a single MARKET order with a
native stop-loss and take-profit, and prints whatever the bus emits for
30 seconds.

You'll need:

* A testnet account (https://testnet.binancefuture.com).
* Testnet API key + secret with **futures trading** permission.
* Both exported as environment variables:

  .. code-block:: shell

      export UTA_BINANCE_TESTNET_API_KEY=...
      export UTA_BINANCE_TESTNET_API_SECRET=...

Run it::

    python -m examples.testnet_smoke

The script is intentionally conservative: it sizes for $20 notional on
``BTCUSDT`` and keeps the SL/TP within 1% of mid so the worst case
realises ~$0.20.

This file is a **smoke test**, not production code. It does not handle
restart, partial fills, retries, or any of the failure modes that a real
producer needs to handle — those are exactly what ``TradeAdapter`` is
for. The point is to verify the wiring against a live exchange before
you commit to integrating UTA into your own stack.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.embedded import TradeAdapter
from trade_adapter.types import (
    Direction,
    EventType,
    Intent,
    NotionalUsd,
    PctFromEntry,
    StopMode,
    UniversalSignal,
    Venue,
)

_log = logging.getLogger("examples.testnet_smoke")


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        sys.stderr.write(
            f"missing env var {name}; see the module docstring for setup\n"
        )
        sys.exit(2)
    return val


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    api_key = _require_env("UTA_BINANCE_TESTNET_API_KEY")
    api_secret = _require_env("UTA_BINANCE_TESTNET_API_SECRET")

    # ---------------------------------------------------------------
    # NOTE: Wiring the live BinanceUmAdapter (REST + WS-trade +
    # user-stream + market-stream) is more than a 30-line snippet.
    # It needs:
    #
    #   * a BinanceRestClient with the testnet base URL
    #     (https://testnet.binancefuture.com)
    #   * a SymbolRegistry (call register_symbol("BTCUSDT") at startup)
    #   * a WsRpcClient pointed at wss://testnet.binancefuture.com/ws-fapi/v1
    #   * a user-stream client with listenKey rotation
    #   * a market-stream client subscribed to bookTicker so the
    #     reference-price cache has data before any signal lands
    #
    # See trade_adapter/exchanges/binance_um/adapter.py for the
    # constructor signature. The exact wiring will be folded into a
    # convenience factory (`TradeAdapter.from_binance_um(config)`)
    # once Phase 4 lands a second venue to factor against.
    # ---------------------------------------------------------------

    bus = EventBus()
    ta = build_testnet_stack(
        api_key=api_key,
        api_secret=api_secret,
        bus=bus,
    )

    # Pre-subscribe before any signal so we don't miss the
    # SIGNAL_RECEIVED frame.
    sub_signal = ta.subscribe(EventType.SIGNAL_RECEIVED)
    sub_order = ta.subscribe(EventType.ORDER_UPDATE)
    sub_fill = ta.subscribe(EventType.FILL)
    sub_pos = ta.subscribe(EventType.POSITION_UPDATE)

    async with ta:
        # Give the market-stream a moment to populate the BBO cache.
        await asyncio.sleep(2.0)

        sig = UniversalSignal(
            signal_id=str(uuid.uuid4()),
            source="testnet_smoke",
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            direction=Direction.LONG,
            intent=Intent.OPEN,
            sizing=NotionalUsd(notional_usd=20.0),
            sl=PctFromEntry(pct=1.0, mode=StopMode.NATIVE),
            tp=PctFromEntry(pct=1.0, mode=StopMode.NATIVE),
            ttl_seconds=10.0,
            correlation_id=None,
        )

        ack = await ta.submit_signal(sig)
        _log.info(
            "ack accepted=%s duplicate=%s rejection=%s",
            ack.accepted, ack.duplicate, ack.rejection_reason,
        )

        # Drain the bus for 30s so the user sees order/fill/position
        # events arrive from the testnet user-stream.
        deadline = asyncio.get_event_loop().time() + 30.0
        while asyncio.get_event_loop().time() < deadline:
            for name, sub in (
                ("signal", sub_signal),
                ("order", sub_order),
                ("fill", sub_fill),
                ("position", sub_pos),
            ):
                while not sub.queue.empty():
                    payload = sub.queue.get_nowait()
                    _log.info("[%s] %s", name, payload)
            await asyncio.sleep(0.1)

    for sub in (sub_signal, sub_order, sub_fill, sub_pos):
        await sub.close()


def build_testnet_stack(*, api_key: str, api_secret: str, bus: EventBus) -> TradeAdapter:
    """Construct a TradeAdapter wired to Binance UM **testnet**.

    Intentionally left as a stub here — the live wiring requires
    private SymbolRegistry / WsRpcClient / user-stream client setup
    that depends on local symbol allowlists and rate-limit knobs the
    operator has to choose. Replace this function body with your own
    wiring (or wait for ``TradeAdapter.from_binance_um(config)`` in a
    later phase) before running the script.
    """

    raise NotImplementedError(
        "fill in build_testnet_stack() with your live BinanceUmAdapter "
        "wiring — see the module docstring and "
        "trade_adapter/exchanges/binance_um/adapter.py"
    )


if __name__ == "__main__":
    asyncio.run(main())
