"""Core business logic.

This subpackage contains the pure logic of the adapter: signal validation
and routing, the per-symbol position state machine, the emergency risk
kill switch, and post-reconnect state reconciliation.

Code in this subpackage MUST NOT do direct I/O against exchanges or the
filesystem. It calls into ``trade_adapter.exchanges`` and
``trade_adapter.storage`` through abstract interfaces that can be replaced
in tests.

Modules:
    signal_router.py    - Validates ``UniversalSignal``, resolves intent
                          against current position state, computes target
                          quantity from ``SizingSpec``, runs emergency
                          kill-switch checks, dispatches ``IntentResolved``
                          to position manager.
    position_manager.py - Per-(venue,symbol) state machine. Holds an
                          ``asyncio.Lock`` per pair. States:
                          IDLE, OPENING, OPEN, ADDING, REDUCING, CLOSING,
                          RECONCILING. Emits ``OrderRequest`` to exchange
                          adapter; consumes ``Fill`` and ``OrderUpdate``
                          events; manages child SL/TP orders.
    risk.py             - Emergency kill-switch checks.
                          ``check_max_single_order_notional()``
                          ``check_max_leverage()``
                          Both raise ``EmergencyRejection`` on violation.
                          NOT a strategy-level risk module.
    reconciliation.py   - Post-reconnect REST sweep. For each venue,
                          fetches all positions/orders, diffs against
                          local SQLite, applies corrections, emits
                          ``reconcile_diff`` events. Wrapped in an
                          in-process ``asyncio.Lock`` per venue
                          (decision A5/A9, post-cleanup; the
                          multi-process variant lives in the
                          ``[multiproc]`` extra and is out of scope
                          for v1.0).
    intents.py          - ``Intent``, ``IntentResolution``, helper logic
                          to resolve OPEN against existing position.
    sizing.py           - Pure functions converting ``SizingSpec`` to
                          target quantity given current price + balance.
"""
