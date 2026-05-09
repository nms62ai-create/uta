"""Tests for the YAML config loader."""

from __future__ import annotations

import pytest

from trade_adapter.config import Config, load_config


def test_missing_file_returns_defaults(tmp_path) -> None:
    cfg = load_config(tmp_path / "missing.yaml")
    assert isinstance(cfg, Config)
    assert cfg.audit.flush_interval_s == 0.1
    assert cfg.idempotency.ttl_s == 3600.0
    assert cfg.bus.default_queue_size == 256


def test_loads_yaml_overrides(tmp_path) -> None:
    p = tmp_path / "config.yaml"
    p.write_text(
        """
venues:
  binance_um:
    enabled: true
  bybit_linear:
    enabled: false

storage:
  db_path: /var/lib/uta/state.db

audit:
  flush_interval_s: 0.05
  batch_size: 64
  queue_max: 512

idempotency:
  ttl_s: 600
  max_entries: 1000

bus:
  default_queue_size: 128

risk:
  max_single_notional_usd: 5000
  max_leverage: 10
"""
    )
    cfg = load_config(p)
    assert cfg.venues["binance_um"].enabled is True
    assert cfg.venues["bybit_linear"].enabled is False
    assert cfg.storage.db_path == "/var/lib/uta/state.db"
    assert cfg.audit.flush_interval_s == 0.05
    assert cfg.audit.batch_size == 64
    assert cfg.audit.queue_max == 512
    assert cfg.idempotency.ttl_s == 600.0
    assert cfg.idempotency.max_entries == 1000
    assert cfg.bus.default_queue_size == 128
    assert cfg.risk.max_single_notional_usd == 5000.0
    assert cfg.risk.max_leverage == 10.0


def test_env_override_replaces_scalar(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "config.yaml"
    p.write_text(
        """
audit:
  flush_interval_s: 0.5
"""
    )
    monkeypatch.setenv("UTA_AUDIT_FLUSH_INTERVAL_S", "0.01")
    cfg = load_config(p)
    assert cfg.audit.flush_interval_s == 0.01


def test_env_override_bool(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UTA_VENUES_BINANCE_UM", "false")
    p = tmp_path / "config.yaml"
    p.write_text(
        """
venues:
  binance_um:
    enabled: true
"""
    )
    # The current override implementation replaces the *whole* venue
    # entry from the env scalar; this is intentionally narrow for v1
    # and documented in the module docstring. The richer override
    # surface lands when we have a real schema.
    cfg = load_config(p)
    # binance_um was overwritten with a bool scalar via env; loader
    # coerces that into a VenueConfig with enabled=False.
    assert cfg.venues["binance_um"].enabled is False
