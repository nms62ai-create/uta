"""Tests for Binance USD-M HMAC signing helpers."""

from __future__ import annotations

import hashlib
import hmac
from decimal import Decimal

import pytest

from trade_adapter.exchanges.binance_um.auth import (
    canonical_query_string,
    sign_hmac,
    sign_params,
    sign_query,
    url_encoded_query_string,
)


def test_canonical_query_string_sorts_keys() -> None:
    out = canonical_query_string({"b": 1, "a": 2})
    assert out == "a=2&b=1"


def test_canonical_query_string_formats_bools() -> None:
    out = canonical_query_string({"reduceOnly": True, "closePosition": False})
    assert out == "closePosition=false&reduceOnly=true"


def test_canonical_query_string_formats_floats() -> None:
    # Integer-valued floats render without a fractional part.
    assert canonical_query_string({"qty": 1.0}) == "qty=1"
    # True fractional floats keep their value, no trailing zeros.
    assert canonical_query_string({"px": 65000.5}) == "px=65000.5"


def test_canonical_query_string_preserves_small_floats() -> None:
    """Small fractional floats must not be silently truncated to ``"0"``.

    Regression: ``format(v, "f")`` defaults to 6 decimals and would
    truncate ``0.0000001`` to ``"0.000000"`` → ``"0"`` after stripping
    trailing zeros. Going through ``Decimal(repr(v))`` preserves the
    intended value at any magnitude.
    """
    assert canonical_query_string({"qty": 0.0000001}) == "qty=0.0000001"
    assert canonical_query_string({"qty": 1e-8}) == "qty=0.00000001"
    assert canonical_query_string({"qty": 5e-9}) == "qty=0.000000005"


def test_canonical_query_string_avoids_scientific_notation() -> None:
    """Tiny floats must never render as ``1e-07`` — Binance rejects that form."""
    s = canonical_query_string({"qty": 1e-7})
    assert "e" not in s.lower()
    assert s == "qty=0.0000001"


def test_canonical_query_string_preserves_shortest_float_repr() -> None:
    """``0.1`` must stay ``"0.1"``, not get expanded to its IEEE-754 form.

    Using ``format(d, '.20f')`` on the raw float would yield
    ``"0.10000000000000000555"``. Going through ``Decimal(repr(v))``
    (the *shortest* round-trip repr) keeps it as ``"0.1"``.
    """
    assert canonical_query_string({"qty": 0.1}) == "qty=0.1"
    assert canonical_query_string({"qty": 0.2}) == "qty=0.2"
    assert canonical_query_string({"qty": 0.3}) == "qty=0.3"


def test_canonical_query_string_formats_decimals() -> None:
    """``Decimal`` values render in fixed-point form, no trailing zeros, no sci."""
    assert canonical_query_string({"qty": Decimal("0.001")}) == "qty=0.001"
    # ``Decimal('1E-8')`` would render as ``'1E-8'`` via ``str()``; we
    # expand to plain form so the exchange accepts it.
    assert canonical_query_string({"qty": Decimal("1E-8")}) == "qty=0.00000001"
    # Integer-valued Decimals drop the fractional part.
    assert canonical_query_string({"qty": Decimal("1.0000")}) == "qty=1"
    assert canonical_query_string({"qty": Decimal("5")}) == "qty=5"
    # Negative fractional Decimals.
    assert canonical_query_string({"px": Decimal("-0.0001")}) == "px=-0.0001"


def test_canonical_query_string_rejects_non_finite_floats() -> None:
    """``nan`` / ``inf`` can't be serialised — fail loudly, not silently."""
    with pytest.raises(ValueError, match="non-finite"):
        canonical_query_string({"qty": float("nan")})
    with pytest.raises(ValueError, match="non-finite"):
        canonical_query_string({"qty": float("inf")})
    with pytest.raises(ValueError, match="non-finite"):
        canonical_query_string({"qty": Decimal("NaN")})
    with pytest.raises(ValueError, match="non-finite"):
        canonical_query_string({"qty": Decimal("Infinity")})


def test_url_encoded_quotes_special_chars() -> None:
    out = url_encoded_query_string({"symbol": "BTC USDT", "side": "BUY"})
    assert out == "side=BUY&symbol=BTC+USDT"


def test_sign_hmac_matches_stdlib() -> None:
    payload = "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=1&timestamp=1700000000000"
    secret = "TEST_SECRET"
    expected = hmac.new(
        secret.encode("ascii"),
        payload.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    assert sign_hmac(secret, payload) == expected


def test_sign_params_appends_signature() -> None:
    secret = "abc"
    params = {"timestamp": 1700000000000, "symbol": "BTCUSDT"}
    out = sign_params(secret, params)
    assert "signature" in out
    canonical = "symbol=BTCUSDT&timestamp=1700000000000"
    assert out["signature"] == sign_hmac(secret, canonical)
    # Original dict not mutated.
    assert "signature" not in params


def test_sign_params_rejects_existing_signature() -> None:
    with pytest.raises(ValueError):
        sign_params("s", {"timestamp": 1, "signature": "x"})


def test_sign_query_signs_canonical_then_encodes() -> None:
    secret = "abc"
    params = {"timestamp": 1700000000000, "symbol": "BTC USDT"}
    out = sign_query(secret, params)
    # Signature is over the canonical (un-encoded) form.
    canonical = "symbol=BTC USDT&timestamp=1700000000000"
    expected_sig = sign_hmac(secret, canonical)
    # Returned string transmits the URL-encoded form.
    assert out == f"symbol=BTC+USDT&timestamp=1700000000000&signature={expected_sig}"


def test_sign_query_rejects_existing_signature() -> None:
    with pytest.raises(ValueError):
        sign_query("s", {"signature": "x"})
