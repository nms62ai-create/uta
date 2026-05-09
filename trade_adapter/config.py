"""YAML config loader with env-var override.

Reads a YAML file whose path is given by:
  1. The ``UTA_CONFIG`` environment variable, or
  2. The ``--config`` CLI flag (gateway mode), or
  3. ``$HOME/.uta/config.yaml`` as a fallback.

Every scalar in the YAML tree can be overridden by an environment
variable of the form ``UTA_<SECTION>_<KEY>`` (upper-cased, dots →
underscores).

The config is a plain ``dict`` on the Python side — this module does
not depend on Pydantic or any schema library (C.13). Validation of
individual fields happens lazily at their point of use. The returned
``Config`` frozen dataclass only carries the top-level sections so
that the rest of the adapter can import sub-configs by name without
type-juggling dictionaries.

Example ``config.yaml``::

    venues:
      binance_um:
        enabled: true
        # api keys are in the encrypted keystore, NOT in config.
      bybit_linear:
        enabled: false

    storage:
      db_path: "~/.uta/state.db"

    bus:
      default_queue_size: 256

    audit:
      flush_interval_s: 0.1
      batch_size: 256
      queue_max: 4096

    idempotency:
      ttl_s: 3600
      max_entries: 100000

    risk:
      max_single_notional_usd: 10000
      max_leverage: 20
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path.home() / ".uta" / "config.yaml"


@dataclass(slots=True, frozen=True)
class VenueConfig:
    enabled: bool = True


@dataclass(slots=True, frozen=True)
class StorageConfig:
    db_path: str = "~/.uta/state.db"


@dataclass(slots=True, frozen=True)
class BusConfig:
    default_queue_size: int = 256


@dataclass(slots=True, frozen=True)
class AuditConfig:
    flush_interval_s: float = 0.1
    batch_size: int = 256
    queue_max: int = 4096


@dataclass(slots=True, frozen=True)
class IdempotencyConfig:
    ttl_s: float = 3600.0
    max_entries: int = 100_000


@dataclass(slots=True, frozen=True)
class RiskConfig:
    max_single_notional_usd: float = 10_000.0
    max_leverage: float = 20.0


@dataclass(slots=True, frozen=True)
class Config:
    venues: dict[str, VenueConfig] = field(default_factory=dict)
    storage: StorageConfig = field(default_factory=StorageConfig)
    bus: BusConfig = field(default_factory=BusConfig)
    audit: AuditConfig = field(default_factory=AuditConfig)
    idempotency: IdempotencyConfig = field(default_factory=IdempotencyConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)


def load_config(path: str | Path | None = None) -> Config:
    """Load config from YAML, apply env overrides, return ``Config``."""

    if path is None:
        env_path = os.environ.get("UTA_CONFIG")
        if env_path:
            path = Path(env_path)
        else:
            path = DEFAULT_CONFIG_PATH
    else:
        path = Path(path)

    raw: dict[str, Any] = {}
    if path.exists():
        with path.open("r") as f:
            raw = yaml.safe_load(f) or {}
        _log.debug("config loaded from %s", path)
    else:
        _log.debug("config file not found at %s; using defaults", path)

    raw = _apply_env_overrides(raw)
    return _parse(raw)


def _apply_env_overrides(raw: dict[str, Any]) -> dict[str, Any]:
    """Replace scalars where ``UTA_<SECTION>_<KEY>`` is set."""

    prefix = "UTA_"
    for key, value in os.environ.items():
        if not key.startswith(prefix) or key == "UTA_CONFIG":
            continue
        parts = key[len(prefix):].lower().split("_", 1)
        if len(parts) != 2:
            continue
        section, field_name = parts
        if section not in raw:
            raw[section] = {}
        existing = raw[section]
        if isinstance(existing, dict):
            existing[field_name] = _coerce(value)
    return raw


def _coerce(value: str) -> Any:
    """Best-effort cast string env value to int/float/bool."""

    if value.lower() in ("true", "yes", "1"):
        return True
    if value.lower() in ("false", "no", "0"):
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _parse(raw: dict[str, Any]) -> Config:
    venues: dict[str, VenueConfig] = {}
    for name, v in (raw.get("venues") or {}).items():
        if isinstance(v, dict):
            venues[name] = VenueConfig(enabled=bool(v.get("enabled", True)))
        else:
            venues[name] = VenueConfig(enabled=bool(v))

    # ``slots=True`` dataclass class-attribute access returns a slot
    # descriptor, not the default value. Instantiate once and read
    # defaults off the instance.
    storage_d = StorageConfig()
    bus_d = BusConfig()
    audit_d = AuditConfig()
    idem_d = IdempotencyConfig()
    risk_d = RiskConfig()

    storage_raw = raw.get("storage") or {}
    storage = StorageConfig(
        db_path=str(storage_raw.get("db_path", storage_d.db_path)),
    )

    bus_raw = raw.get("bus") or {}
    bus = BusConfig(
        default_queue_size=int(bus_raw.get("default_queue_size", bus_d.default_queue_size)),
    )

    audit_raw = raw.get("audit") or {}
    audit = AuditConfig(
        flush_interval_s=float(audit_raw.get("flush_interval_s", audit_d.flush_interval_s)),
        batch_size=int(audit_raw.get("batch_size", audit_d.batch_size)),
        queue_max=int(audit_raw.get("queue_max", audit_d.queue_max)),
    )

    idem_raw = raw.get("idempotency") or {}
    idempotency = IdempotencyConfig(
        ttl_s=float(idem_raw.get("ttl_s", idem_d.ttl_s)),
        max_entries=int(idem_raw.get("max_entries", idem_d.max_entries)),
    )

    risk_raw = raw.get("risk") or {}
    risk = RiskConfig(
        max_single_notional_usd=float(
            risk_raw.get("max_single_notional_usd", risk_d.max_single_notional_usd)
        ),
        max_leverage=float(risk_raw.get("max_leverage", risk_d.max_leverage)),
    )

    return Config(
        venues=venues,
        storage=storage,
        bus=bus,
        audit=audit,
        idempotency=idempotency,
        risk=risk,
    )
