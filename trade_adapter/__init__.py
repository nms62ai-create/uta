"""Universal Trade Adapter.

Self-hosted single-process Python trading adapter for Binance USD-M Futures
and Bybit Linear. Exposes a stable HTTP+WebSocket public API; speaks
exchange-specific REST/WS to upstream venues.

This package is currently in **specification phase**. Module stubs below
define the shape; implementation lands per ``docs/ROADMAP.md``.

Public surface (planned):
    trade_adapter.types     - dataclasses, enums (UniversalSignal etc.)
    trade_adapter.config    - YAML config loader
    trade_adapter.api       - FastAPI app + routers + WS broadcaster
    trade_adapter.core      - SignalRouter, PositionManager, risk, reconcile
    trade_adapter.exchanges - per-venue ExchangeAdapter implementations
    trade_adapter.storage   - SQLite DAO + Redis pub/sub
    trade_adapter.secrets   - encrypted keystore
    trade_adapter.main      - process entrypoint
"""

__version__ = "1.0.0a0"
