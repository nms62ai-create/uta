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
import math
from decimal import Decimal
from typing import Any
from urllib.parse import quote_plus


def _decimal_to_plain_string(d: Decimal) -> str:
    """Render a ``Decimal`` in fixed-point form without scientific notation.

    Strips trailing zeros and a trailing decimal point so ``1.0000`` becomes
    ``"1"`` (matches Binance's canonical numeric form). ``format(d, 'f')``
    is the only stdlib way to force fixed-point output for *any* magnitude
    of ``Decimal`` — ``str(Decimal('1E-8'))`` returns ``'1E-8'`` which the
    exchange would reject.
    """

    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _format_value(v: Any) -> str:
    """Stringify a single param value as Binance expects.

    - ``bool``     → ``"true"``/``"false"``
    - integer-valued numeric (``int``, ``1.0``, ``Decimal('1')``) → ``"1"``
    - ``float``    → round-tripped via ``Decimal(repr(v))`` so the
      *shortest* decimal representation is preserved (``0.1`` stays
      ``"0.1"``, not ``"0.10000000000000000555"``), then expanded via
      :func:`_decimal_to_plain_string` so small values aren't truncated
      and no scientific notation is emitted (``0.0000001`` →
      ``"0.0000001"``, never ``"0"`` or ``"1e-07"``).
    - ``Decimal``  → same fixed-point rendering as above.
    - anything else → ``str(v)``.

    ``float('nan')`` / ``float('inf')`` are rejected with ``ValueError``
    — they can't be serialised into a valid Binance request.
    """

    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        if not math.isfinite(v):
            raise ValueError(f"cannot format non-finite float for Binance: {v!r}")
        if v.is_integer():
            return str(int(v))
        # ``repr(float)`` returns the shortest decimal string that
        # round-trips to the same float — e.g. ``repr(0.1) == '0.1'``,
        # ``repr(1e-8) == '1e-08'``. Decimal accepts both; the subsequent
        # ``format(d, 'f')`` expands scientific notation to fixed-point.
        return _decimal_to_plain_string(Decimal(repr(v)))
    if isinstance(v, Decimal):
        if not v.is_finite():
            raise ValueError(f"cannot format non-finite Decimal for Binance: {v!r}")
        if v == v.to_integral_value():
            return str(int(v))
        return _decimal_to_plain_string(v)
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
