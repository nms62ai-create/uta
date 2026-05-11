"""Phase 3g REST snapshot provider for Binance USD-M Futures.

Adapts :class:`BinanceRestClient` + :mod:`rest_translators` to the
venue-agnostic :class:`VenueSnapshotProvider` Protocol so the
:class:`PositionManager` can bootstrap + reconcile without knowing any
Binance specifics.

The position manager calls this provider once at bootstrap and then
periodically (at ``reconcile_interval_s``). Each call hits two REST
endpoints (``/fapi/v2/positionRisk`` and ``/fapi/v2/account``). Translator
errors propagate so a Binance API drift surfaces loudly rather than
silently producing a stale snapshot.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from ...types import PositionUpdate, Venue
from .rest import BinanceRestClient
from .rest_translators import (
    account_info_to_equity_usd,
    position_risk_to_position_updates,
)


class BinanceUmSnapshotProvider:
    """:class:`VenueSnapshotProvider` over a :class:`BinanceRestClient`.

    Structural Protocol implementation: nothing on this class is
    Binance-specific from the position manager's point of view — it
    just exposes ``fetch_position_snapshot`` and ``fetch_equity_snapshot``
    plus the ``venue`` tag.
    """

    venue: Venue = Venue.BINANCE_UM

    def __init__(
        self,
        rest: BinanceRestClient,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._rest = rest
        self._clock = clock

    async def fetch_position_snapshot(self) -> list[PositionUpdate]:
        """REST ``/fapi/v2/positionRisk`` → list of :class:`PositionUpdate`."""

        ts = self._clock()
        rows = await self._rest.fetch_positions()
        return position_risk_to_position_updates(rows, ts=ts)

    async def fetch_equity_snapshot(self) -> float:
        """REST ``/fapi/v2/account`` → total wallet balance in USD."""

        info = await self._rest.fetch_account()
        return account_info_to_equity_usd(info)


__all__ = ["BinanceUmSnapshotProvider"]
