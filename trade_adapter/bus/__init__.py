"""In-process pub/sub.

The adapter is single-process by default (decision A5, post-cleanup).
The event bus lives in this subpackage. Multi-process / Redis-backed
pub/sub is reserved for the optional ``[multiproc]`` extra and would
land here as a separate module that implements the same ``EventBus``
contract.

Modules:
    event_bus.py  - bounded ``asyncio.Queue`` per subscriber with
                    drop-oldest backpressure (decision A20). One slow
                    subscriber cannot stall the trading hot path.
"""

from .event_bus import EventBus, Subscription

__all__ = ["EventBus", "Subscription"]
