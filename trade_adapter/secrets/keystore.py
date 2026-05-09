"""Encrypted on-disk keystore for exchange API keys.

Implements decision **A2**: API keys live in an encrypted file under
``$HOME/.uta/secrets.enc`` (path overridable via ``UTA_SECRETS_PATH``).
The encryption key is derived from a master password via Argon2id
(time_cost=3, memory_cost=64 MiB, parallelism=4) plus a per-file
random 16-byte salt; ciphertext uses Fernet (AES-128-CBC + HMAC-SHA256).

Design notes
------------
* The store is a flat JSON document inside the encrypted blob:
  ``{"version": 1, "exchanges": {"binance_um": {...}, ...},
     "consumer_tokens": {"<name>": "<bearer>"}}``.
  Adding/changing a key rewrites the entire blob.
* Plaintext keys live only on the stack of ``decrypt()`` and on
  ``Secrets``; we do not stash them on long-lived attributes elsewhere.
  The structlog scrubber in :mod:`trade_adapter.secrets.scrubber`
  (Phase 3) will drop them from log lines just in case.
* Master-password sourcing is the caller's job — this module just
  takes the bytes. Production code reads ``UTA_MASTER_PASSWORD`` or
  prompts via ``getpass`` (Phase 2).
* On bad password / corrupt file we raise :class:`KeystoreError`.
  We never operate with broken keys (decision: "fail to start; do
  not run with broken keys").
"""

from __future__ import annotations

import json
import logging
import os
import secrets as _stdlib_secrets
from base64 import urlsafe_b64encode
from dataclasses import dataclass, field
from pathlib import Path

from argon2.low_level import Type, hash_secret_raw
from cryptography.fernet import Fernet, InvalidToken

_log = logging.getLogger(__name__)

KEYSTORE_VERSION = 1
SALT_SIZE = 16
KEY_SIZE = 32  # bytes; Fernet wants a 32-byte url-safe-b64 encoded key.

# Argon2id KDF parameters. Conservative for a single-user CLI laptop;
# the operator only types the master password at process start.
_ARGON2_TIME_COST = 3
_ARGON2_MEMORY_COST_KIB = 64 * 1024  # 64 MiB
_ARGON2_PARALLELISM = 4


class KeystoreError(RuntimeError):
    """Raised when the keystore cannot be opened (wrong password, corrupt)."""


@dataclass(slots=True, frozen=True)
class ExchangeKey:
    api_key: str
    api_secret: str
    label: str = "main"


@dataclass(slots=True, frozen=True)
class Secrets:
    exchanges: dict[str, ExchangeKey] = field(default_factory=dict)
    consumer_tokens: dict[str, str] = field(default_factory=dict)


def default_secrets_path() -> Path:
    env = os.environ.get("UTA_SECRETS_PATH")
    if env:
        return Path(env)
    return Path.home() / ".uta" / "secrets.enc"


def _derive_key(password: bytes, salt: bytes) -> bytes:
    """Argon2id → 32 raw bytes → url-safe-b64 → Fernet key."""

    raw = hash_secret_raw(
        secret=password,
        salt=salt,
        time_cost=_ARGON2_TIME_COST,
        memory_cost=_ARGON2_MEMORY_COST_KIB,
        parallelism=_ARGON2_PARALLELISM,
        hash_len=KEY_SIZE,
        type=Type.ID,
    )
    return urlsafe_b64encode(raw)


def _encode(secrets_obj: Secrets) -> bytes:
    payload = {
        "version": KEYSTORE_VERSION,
        "exchanges": {
            name: {
                "api_key": k.api_key,
                "api_secret": k.api_secret,
                "label": k.label,
            }
            for name, k in secrets_obj.exchanges.items()
        },
        "consumer_tokens": dict(secrets_obj.consumer_tokens),
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _decode(blob: bytes) -> Secrets:
    payload = json.loads(blob.decode("utf-8"))
    if payload.get("version") != KEYSTORE_VERSION:
        raise KeystoreError(
            f"Unsupported keystore version: {payload.get('version')!r}"
        )
    exchanges = {
        name: ExchangeKey(
            api_key=str(k["api_key"]),
            api_secret=str(k["api_secret"]),
            label=str(k.get("label", "main")),
        )
        for name, k in (payload.get("exchanges") or {}).items()
    }
    tokens = {str(n): str(t) for n, t in (payload.get("consumer_tokens") or {}).items()}
    return Secrets(exchanges=exchanges, consumer_tokens=tokens)


def encrypt(secrets_obj: Secrets, password: bytes) -> bytes:
    """Serialize + encrypt. Returns ``salt || ciphertext``."""

    salt = _stdlib_secrets.token_bytes(SALT_SIZE)
    key = _derive_key(password, salt)
    f = Fernet(key)
    ciphertext = f.encrypt(_encode(secrets_obj))
    return salt + ciphertext


def decrypt(blob: bytes, password: bytes) -> Secrets:
    """Decrypt a blob written by :func:`encrypt`."""

    if len(blob) < SALT_SIZE + 1:
        raise KeystoreError("blob too short")
    salt, ciphertext = blob[:SALT_SIZE], blob[SALT_SIZE:]
    key = _derive_key(password, salt)
    try:
        plaintext = Fernet(key).decrypt(ciphertext)
    except InvalidToken as e:
        raise KeystoreError("invalid master password or corrupt keystore") from e
    return _decode(plaintext)


def save(secrets_obj: Secrets, password: bytes, path: Path | None = None) -> Path:
    """Encrypt + atomically write to ``path`` (or default location)."""

    path = path or default_secrets_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = encrypt(secrets_obj, password)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(blob)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except OSError:  # pragma: no cover - non-POSIX
        pass
    _log.debug("keystore saved path=%s size=%d", path, len(blob))
    return path


def load(password: bytes, path: Path | None = None) -> Secrets:
    """Read + decrypt the keystore at ``path`` (or default location)."""

    path = path or default_secrets_path()
    if not path.exists():
        raise KeystoreError(f"keystore not found at {path}")
    blob = path.read_bytes()
    return decrypt(blob, password)
