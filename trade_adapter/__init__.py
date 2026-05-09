"""Universal Trade Adapter.

Self-hosted single-process Python trading adapter for Binance USD-M
Futures and Bybit Linear. Embedded Python API by default (decision A3);
the optional FastAPI gateway under ``[gateway]`` extras gives remote
or cross-language consumers an HTTP + WS facade over the same core.

Trading and user-data are spoken WebSocket-first (decision A3b); REST
is bootstrap, reconciliation, and explicit fallback only.

Implementation lands per ``docs/ROADMAP.md``. Phase 1 (this commit)
ships the wire-protocol types, the in-process event bus, the SQLite
state store, the background audit flusher, the idempotency cache, the
encrypted keystore, the YAML config loader, and the golden-snapshot
test that locks the wire shape. Phase 2 onward fills in the venue
adapters and the embedded ``TradeAdapter`` surface.

Public surface (planned):
    trade_adapter.types         - dataclasses, enums (UniversalSignal etc.)
    trade_adapter.serialization - JSON wire (de)serializers
    trade_adapter.config        - YAML config loader
    trade_adapter.bus           - in-process event bus (A20)
    trade_adapter.storage       - SQLite DAO + audit flusher + idempotency
    trade_adapter.secrets       - encrypted keystore (A2)
    trade_adapter.exchanges     - per-venue WS-first ExchangeAdapter (A3b)
    trade_adapter.core          - signal router, position manager, risk
    trade_adapter.embedded      - public ``TradeAdapter`` API (A3 default)
    trade_adapter.gateway       - optional FastAPI facade (A3 extras)
    trade_adapter.main          - process entrypoint
"""

__version__ = "1.0.0a0"
