"""Tests for the encrypted keystore (decision A2)."""

from __future__ import annotations

import pytest

from trade_adapter.secrets.keystore import (
    ExchangeKey,
    KeystoreError,
    Secrets,
    decrypt,
    encrypt,
    load,
    save,
)


def _sample() -> Secrets:
    return Secrets(
        exchanges={
            "binance_um": ExchangeKey(api_key="ak1", api_secret="as1", label="main"),
            "bybit_linear": ExchangeKey(api_key="ak2", api_secret="as2", label="alt"),
        },
        consumer_tokens={"example_consumer": "tok-1"},
    )


def test_encrypt_decrypt_round_trip() -> None:
    s = _sample()
    blob = encrypt(s, b"hunter2")
    restored = decrypt(blob, b"hunter2")
    assert restored == s


def test_wrong_password_raises() -> None:
    s = _sample()
    blob = encrypt(s, b"hunter2")
    with pytest.raises(KeystoreError):
        decrypt(blob, b"wrong")


def test_corrupt_blob_raises() -> None:
    s = _sample()
    blob = bytearray(encrypt(s, b"pw"))
    blob[-1] ^= 0xFF  # corrupt the last byte of ciphertext
    with pytest.raises(KeystoreError):
        decrypt(bytes(blob), b"pw")


def test_save_and_load_round_trip(tmp_path) -> None:
    path = tmp_path / "secrets.enc"
    s = _sample()
    save(s, b"pw", path=path)
    restored = load(b"pw", path=path)
    assert restored == s


def test_save_uses_atomic_write(tmp_path) -> None:
    path = tmp_path / "secrets.enc"
    save(_sample(), b"pw", path=path)
    assert path.exists()
    # No leftover .tmp file from atomic rename.
    assert not (tmp_path / "secrets.enc.tmp").exists()


def test_save_creates_parent_dir(tmp_path) -> None:
    path = tmp_path / "nested" / "dir" / "secrets.enc"
    save(_sample(), b"pw", path=path)
    assert path.exists()


def test_load_missing_file_raises(tmp_path) -> None:
    with pytest.raises(KeystoreError):
        load(b"pw", path=tmp_path / "does-not-exist.enc")


def test_blob_too_short_raises() -> None:
    with pytest.raises(KeystoreError):
        decrypt(b"\x00" * 4, b"pw")
