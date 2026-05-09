"""Binance USD-M Futures request signing.

Binance signs requests with HMAC-SHA256 (default) or Ed25519 (newer
key type, opt-in). v1.0 implements the HMAC variant only — it covers
both the public REST endpoints used for bootstrap / reconciliation and
the WS-API trade methods. Ed25519 is added when the user explicitly
opts in by storing an Ed25519 key in the keystore (out of scope for
Phase 2a).

Signing rules (`https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info`):

REST signed endpoints
    The signature is HMAC-SHA256 over the **query string** (or
    URL-encoded body for ``POST/PUT/DELETE``) **without** the
    ``signature`` parameter. The hex digest is appended as the last
    parameter.

WS-API signed methods
    The ``params`` object is converted into a canonical query string
    by sorting keys lexicographically and joining as
    ``k1=v1&k2=v2&...``. The HMAC-SHA256 hex digest of that string
    is then placed back into the ``params`` object as ``signature``.

Both variants require a ``timestamp`` field (server-time milliseconds)
and accept an optional ``recvWindow`` (default 5000 ms).

This module is pure: it does **no** I/O. Time is injected; the random
``uid`` for ``client_order_id`` generation is also injected so tests
are deterministic.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Any
from urllib.parse import quote_plus


def _format_value(v: Any) -> str:
    """Stringify a single param value as Binance expects.

    - ``bool`` → ``"true"``/``"false"``
    - ``float`` with no fractional part → integer-looking (e.g. ``1.0`` → ``"1"``)
    - everything else → ``str(v)``
    """

    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        # Use repr to avoid scientific notation for small floats.
        return format(v, "f").rstrip("0").rstrip(".")
    return str(v)


def canonical_query_string(params: dict[str, Any]) -> str:
    """Render ``params`` as ``key=value&key=value`` with keys sorted.

    No URL-encoding — Binance's REST endpoint accepts the raw form for
    signing purposes (the actual HTTP request encodes separately). For
    WS-API, the canonical form is signed verbatim.
    """

    return "&".join(f"{k}={_format_value(v)}" for k, v in sorted(params.items()))


def url_encoded_query_string(params: dict[str, Any]) -> str:
    """Render ``params`` as a URL-encoded query string with keys sorted.

    Used as the actual HTTP query / body. Binance signs the **un**-encoded
    form per :func:`canonical_query_string`, but transmits the encoded
    form. Keeping both variants explicit makes signing bugs easier to
    diagnose.
    """

    return "&".join(f"{k}={quote_plus(_format_value(v))}" for k, v in sorted(params.items()))


def sign_hmac(api_secret: str, payload: str) -> str:
    """Return the lowercase hex HMAC-SHA256 digest of ``payload``."""

    return hmac.new(
        api_secret.encode("ascii"),
        payload.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()


def sign_params(api_secret: str, params: dict[str, Any]) -> dict[str, Any]:
    """Return ``params`` with a ``signature`` field appended.

    Used by the WS-API trade methods. The caller is responsible for
    putting ``timestamp`` (and optionally ``recvWindow``) into ``params``
    before calling this; we don't assume a clock here.
    """

    if "signature" in params:
        raise ValueError("params already contain a 'signature' field")
    payload = canonical_query_string(params)
    signed = dict(params)
    signed["signature"] = sign_hmac(api_secret, payload)
    return signed


def sign_query(api_secret: str, params: dict[str, Any]) -> str:
    """Return a URL-encoded query string with ``signature`` appended.

    Used by signed REST endpoints. The signature is over the **un**-encoded
    canonical form; the returned string is encoded for transmission.
    """

    if "signature" in params:
        raise ValueError("params already contain a 'signature' field")
    canonical = canonical_query_string(params)
    sig = sign_hmac(api_secret, canonical)
    return url_encoded_query_string(params) + "&signature=" + sig
