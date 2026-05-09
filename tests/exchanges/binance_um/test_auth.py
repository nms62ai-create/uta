"""Tests for Binance USD-M HMAC signing helpers."""

from __future__ import annotations

import hashlib
import hmac

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
