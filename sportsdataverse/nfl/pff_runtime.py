"""Runtime getter for the generated PFF Premium Stats wrappers (:mod:`sportsdataverse.nfl.pff_core`).

PFF Premium Stats (``premium.pff.com/api/v1``) is **paywalled**: every request must carry the
authenticated-session cookies from a logged-in browser. Unlike stats.nba.com, PFF does **not**
JA3/TLS-fingerprint-block plain ``requests`` -- the only gate is the cookies -- so the live
transport goes through the shared :func:`sportsdataverse.dl_utils.download` gateway (retry loop +
pooling) rather than ``curl_cffi``.

**Supported auth = user-supplied cookies.** Precedence:

1. an explicit ``cookies=`` dict on the call,
2. environment: a raw ``SDV_PY_PFF_COOKIES`` cookie string (``k=v; k=v``), or the pair
   ``SDV_PY_PFF_PREMIUM_KEY`` (the entitlement cookie ``_premium_key``) + optional
   ``SDV_PY_PFF_SESSION`` (the Clerk ``__session`` JWT),
3. otherwise a clear :class:`RuntimeError` telling the caller to log in and supply cookies.

The HTTP call is injectable (``transport=``) with signature
``(url, params, headers, cookies) -> (status_code, text)`` so wrappers and tests run fully
offline. :func:`pff_login` is an experimental stub -- see its docstring.
"""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Dict, Optional

from sportsdataverse.dl_utils import download

__all__ = ["_get", "pff_login"]

Transport = Callable[[str, dict, dict, dict], tuple]

_COOKIE_ENV = ("SDV_PY_PFF_PREMIUM_KEY", "SDV_PY_PFF_SESSION", "SDV_PY_PFF_COOKIES")


def _pff_headers() -> Dict[str, str]:
    """Build minimal browser-mimicking request headers for premium.pff.com.

    Returns:
        A dict of HTTP request headers suitable for :func:`sportsdataverse.dl_utils.download`.

    Example:
        Quick start::

            from sportsdataverse.nfl.pff_runtime import _pff_headers
            print(_pff_headers()["Accept"])  # "application/json, text/plain, */*"
    """
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://premium.pff.com/",
        "Origin": "https://premium.pff.com",
    }


def _cookies_from_env() -> Dict[str, str]:
    """Assemble a PFF cookie dict from environment variables.

    Reads a raw ``SDV_PY_PFF_COOKIES`` cookie string first (``k=v; k=v``); otherwise
    builds ``{_premium_key, __session}`` from ``SDV_PY_PFF_PREMIUM_KEY`` /
    ``SDV_PY_PFF_SESSION``. Returns ``{}`` when none are set.

    Returns:
        A ``dict`` of cookie name -> value (possibly empty).
    """
    raw = os.environ.get("SDV_PY_PFF_COOKIES")
    if raw:
        out: Dict[str, str] = {}
        for part in raw.split(";"):
            key, _, value = part.partition("=")
            if key.strip():
                out[key.strip()] = value.strip()
        return out
    cookies: Dict[str, str] = {}
    premium_key = os.environ.get("SDV_PY_PFF_PREMIUM_KEY")
    if premium_key:
        cookies["_premium_key"] = premium_key
    session = os.environ.get("SDV_PY_PFF_SESSION")
    if session:
        cookies["__session"] = session
    return cookies


def _resolve_cookies(explicit: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Return the cookie dict to send, or raise when none can be found.

    Args:
        explicit: Cookies passed directly on the call; wins over the environment.

    Returns:
        A non-empty cookie ``dict``.

    Raises:
        RuntimeError: When neither an explicit ``cookies`` dict nor the PFF cookie
            environment variables are set.
    """
    cookies = explicit or _cookies_from_env()
    if not cookies:
        raise RuntimeError(
            "PFF Premium is paywalled -- log in and supply cookies via the cookies= arg or the "
            "SDV_PY_PFF_PREMIUM_KEY / SDV_PY_PFF_SESSION (or SDV_PY_PFF_COOKIES) env vars."
        )
    return cookies


def _default_transport(url: str, params: dict, headers: dict, cookies: dict) -> tuple:
    """Live transport: fold cookies into a ``Cookie`` header and go through ``download``.

    :func:`sportsdataverse.dl_utils.download` has no ``cookies=`` parameter (it shares a
    pooled cookie jar), so the resolved cookies are serialized into a per-request ``Cookie``
    header instead.

    Args:
        url: Fully-qualified premium.pff.com URL.
        params: Query-string params (``None`` values already stripped by :func:`_get`).
        headers: Base request headers.
        cookies: Resolved auth cookie dict.

    Returns:
        ``(status_code, response_text)``.
    """
    hdrs = dict(headers or {})
    if cookies:
        hdrs["Cookie"] = "; ".join(f"{name}={value}" for name, value in cookies.items())
    resp = download(url=url, params=params, headers=hdrs, timeout=30)
    if hasattr(resp, "raise_for_status"):
        resp.raise_for_status()
    return resp.status_code, resp.text


def _get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    *,
    cookies: Optional[Dict[str, str]] = None,
    transport: Optional[Transport] = None,
    **kwargs: Any,
) -> Dict:
    """GET a premium.pff.com endpoint and return its parsed JSON body.

    Resolves auth cookies (explicit arg > environment), attaches them, and issues the
    request through the injectable *transport*. Non-200 status, a blank body, or a JSON
    decode error all return ``{}`` so the parsers can chain without null-checks.

    Args:
        url: Fully-qualified premium.pff.com URL built by the generated wrapper.
        params: Query-string parameters; ``None`` values are stripped.
        headers: HTTP headers dict. Defaults to :func:`_pff_headers`.
        cookies: PFF auth cookies (e.g. ``{"_premium_key": "..."}``). Falls back to the
            ``SDV_PY_PFF_*`` environment variables; a :class:`RuntimeError` is raised when
            neither is available.
        transport: Callable ``(url, params, headers, cookies) -> (status_code, text)``.
            Defaults to :func:`_default_transport`. Inject a fake to run offline.
        **kwargs: Accepted for forward-compatibility with generated callers; unused.

    Returns:
        Parsed JSON ``dict``, or ``{}`` on non-200 status, blank body, or JSON error.

    Raises:
        RuntimeError: When no auth cookies can be resolved.

    Example:
        Quick start (offline -- inject a transport, no cookies needed on the wire)::

            import json
            from sportsdataverse.nfl.pff_runtime import _get

            def fake(url, params, headers, cookies):
                return 200, json.dumps({"passing_summary": []})

            data = _get(
                "https://premium.pff.com/api/v1/facet/passing/summary",
                {"league": "nfl"},
                cookies={"_premium_key": "PK"},
                transport=fake,
            )

        Live use (residential IP, logged-in session)::

            import os
            os.environ["SDV_PY_PFF_PREMIUM_KEY"] = "<your _premium_key cookie>"
            raw = _get("https://premium.pff.com/api/v1/leagues", {})
    """
    clean: Dict[str, Any] = {k: v for k, v in (params or {}).items() if v is not None}
    resolved = _resolve_cookies(cookies)
    _transport = transport or _default_transport
    status, text = _transport(url, clean, headers or _pff_headers(), resolved)
    if status != 200 or not (text or "").strip():
        return {}
    try:
        body = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return body if isinstance(body, dict) else {}


def pff_login(
    email: Optional[str] = None,
    password: Optional[str] = None,
    *,
    transport: Optional[Callable] = None,
) -> Dict[str, str]:
    """Experimental: mint PFF session cookies from credentials (NOT IMPLEMENTED).

    The Wave-0 auth spike (a live Clerk sign-in flow against ``clerk.pff.com`` that exchanges
    email/password for the ``_premium_key`` + ``__session`` cookies) was **deferred** -- there
    was no live PFF session to reverse-engineer the flow against. Two caveats make the
    credential path fragile even once built: the Clerk ``__session`` JWT is short-lived
    (~60 s, refreshed by the browser), and the ``_premium_key`` entitlement cookie is what
    actually authorizes the API for its TTL.

    **Supported path instead:** supply the cookies you already hold from a logged-in browser
    session -- pass ``cookies=`` to a wrapper / :func:`_get`, or set ``SDV_PY_PFF_PREMIUM_KEY``
    (+ optional ``SDV_PY_PFF_SESSION``) / ``SDV_PY_PFF_COOKIES`` in the environment.

    Args:
        email: PFF account email (or ``SDV_PY_PFF_EMAIL``). Never logged.
        password: PFF account password (or ``SDV_PY_PFF_PASSWORD``). Never logged.
        transport: Injectable Clerk transport for a future implementation / tests.

    Returns:
        A cookie dict ``{"_premium_key": ..., "__session": ...}`` once implemented.

    Raises:
        NotImplementedError: Always -- use the supported cookie-supply path (see above).
    """
    raise NotImplementedError(
        "pff_login (Clerk credential sign-in) is not implemented -- the auth spike was deferred. "
        "Supply your logged-in browser cookies instead: pass cookies= to a wrapper / _get, or set "
        "SDV_PY_PFF_PREMIUM_KEY / SDV_PY_PFF_SESSION / SDV_PY_PFF_COOKIES in the environment."
    )
