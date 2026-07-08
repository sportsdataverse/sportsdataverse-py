"""Runtime getter for the generated ``sports247`` wrappers (247Sports RDB).

``ipa.247sports.com`` fronts the 247Sports **Recruit Database** (RDB — the
service the ``247sports-recruit-database.openapi.yaml`` spec describes; note
the host is ``ipa.``, not ``api.``). Three mechanics live here:

* **Browser-TLS impersonation** — the Fastly edge fingerprint-blocks plain
  ``requests`` (0-byte 403 on every route), the same class of block as
  stats.nba.com, so the live transport uses ``curl_cffi`` with Chrome
  impersonation. ``curl_cffi`` is a lazy optional import (``tests``/``all``
  extras); the HTTP call is injectable (``transport=``) so wrappers and tests
  run fully offline.
* **Trailing-slash enforcement** — the RDB 301-redirects every slash-less
  path (``/rdb/v1/teams`` -> ``/rdb/v1/teams/``); ``_get`` appends the slash
  up front to save the extra round trip.
* **Guest-JWT bearer** — most ``/rdb/v1/*`` routes (``recruits``,
  ``transfers``, ``coaches``, the ranking feeds, ``currentTargetPredictions``)
  return 401/403 unless an ``Authorization: Bearer <jwt>`` header is sent. The
  token is a **guest** JWT that ``GET https://247sports.com/`` mints for free
  (no login) as a ``JWT`` cookie — ~12 h TTL, ``sub``/``iss`` ``247sports.com``,
  ``fastly: true``. ``_get`` mints it lazily, caches it for the process, always
  attaches it (the auth-free ``teams`` / ``institutionrankings`` routes ignore
  it), and re-mints once on a 401/403 (expiry). The mint step is patchable
  (``_mint_guest_jwt``) so tests never hit the network.

The guest token unlocks 12 of the 25 RDB GET routes (``teams``,
``institutionrankings``, ``recruits``, ``transfers``, ``coaches``, the three
ranking feeds, ``currentTargetPredictions``, ``sports/{k}/year``,
``tags/autocomplete``, ``positions``). The remaining 13 GET routes are
**bearer-only** (a logged-in/premium session; the guest token still 403s) and
are deliberately **not wrapped**: ``playerSportRankings``,
``transferPlayerSportRankings``, ``unrankedRecruits``,
``rankings/{rankingKey}/biggestMovers``,
``rankings/{rankingKey}/archivedPlayerRankings``,
``rankings/{rankingKey}/playerSportsUnderSpecialEvaluation``,
``transferrankings/{rankingKey}/unrankedtransfers``, ``rankings`` (the
non-feed criteria query), ``sports``, ``year`` (global class-year list),
``institutionGroups``, and the two ``tags/.../photos`` routes. (Probe-confirmed
2026-07-08 under the guest JWT.)
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Union

__all__ = ["_get", "rdb_headers"]

Transport = Callable[[str, dict, dict, Optional[str]], tuple]

_SITE_ROOT = "https://247sports.com/"
# Process-lifetime cache of the guest bearer JWT; re-minted on a 401/403.
_jwt: Optional[str] = None


def rdb_headers() -> Dict[str, str]:
    """Build browser-mimicking request headers for ipa.247sports.com.

    Returns:
        A dict of HTTP request headers suitable for curl_cffi or requests.

    Example:
        Quick start::

            from sportsdataverse.cfb.sports247_runtime import rdb_headers
            h = rdb_headers()
            print(h["Accept"])  # "application/json, text/plain, */*"
    """
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://247sports.com/",
        "Origin": "https://247sports.com",
    }


def _mint_guest_jwt() -> Optional[str]:
    """Mint a guest bearer JWT from the 247sports.com site root.

    A plain ``GET https://247sports.com/`` sets a ``JWT`` cookie with no login —
    a short-lived (~12 h) guest token that the gated RDB routes accept as
    ``Authorization: Bearer``. Returns ``None`` when curl_cffi is unavailable or
    the cookie is absent (the caller then falls back to an unauthenticated
    request, which still serves the public ``teams`` / ``institutionrankings``
    routes).

    Returns:
        The guest JWT string, or ``None`` when it could not be obtained.
    """
    try:
        from curl_cffi import requests as creq
    except ImportError:  # pragma: no cover - exercised only on the live path
        return None
    try:
        sess = creq.Session()
        sess.get(_SITE_ROOT, headers=rdb_headers(), impersonate="chrome", timeout=30)
        return sess.cookies.get("JWT")
    except Exception:  # noqa: BLE001  # pragma: no cover - network failure path
        return None


def _curl_transport(
    url: str,
    params: dict,
    headers: dict,
    proxy_url: Optional[str],
) -> tuple:
    try:
        from curl_cffi import requests as creq
    except ImportError as exc:  # pragma: no cover - exercised only on the live path
        raise ImportError(
            "Live ipa.247sports.com calls require curl_cffi (the RDB edge "
            "fingerprint-blocks plain requests). Install with: pip install curl_cffi "
            "or pip install sportsdataverse[all]"
        ) from exc
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    r = creq.get(
        url,
        params=params,
        headers=headers,
        proxies=proxies,
        impersonate="chrome",
        timeout=30,
    )
    return r.status_code, r.text


def _get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    transport: Optional[Transport] = None,
    proxy_url: Optional[str] = None,
    auth: bool = True,
    **kwargs: Any,
) -> Union[Dict, List]:
    """GET an ipa.247sports.com RDB route and return its JSON body.

    Attaches the cached guest bearer JWT (minting it on first use) so the gated
    routes resolve; the auth-free public routes ignore it. On a 401/403 the JWT
    is re-minted once and the request retried (guest tokens expire ~12 h).

    Args:
        url: fully-qualified RDB URL built by the generated wrapper. A missing
            trailing slash is added (the RDB 301s slash-less paths).
        params: query-string parameters; ``None`` values are stripped.
        headers: HTTP headers dict. Defaults to :func:`rdb_headers`.
        transport: callable with signature
            ``(url, params, headers, proxy_url) -> (status_code, text)``.
            Defaults to curl_cffi Chrome impersonation.
        proxy_url: optional proxy URL forwarded to the transport.
        auth: attach (and refresh) the guest bearer JWT. ``True`` for every
            generated wrapper; pass ``False`` to force an unauthenticated call.
        **kwargs: accepted for forward-compatibility with generated callers;
            unused.

    Returns:
        Parsed JSON (``dict`` for enveloped payloads, ``list`` for the
        array routes), or ``{}`` on non-200 status, blank body, or JSON error.

    Example:
        Quick start (offline — inject a transport, no minting)::

            import sportsdataverse.cfb.sports247_runtime as rt
            rt._jwt = "test-token"
            def fake(url, params, headers, proxy_url):
                return 200, '{"players": []}'
            data = rt._get("https://ipa.247sports.com/rdb/v1/recruits/", transport=fake)
    """
    global _jwt

    clean: Dict[str, Any] = {k: v for k, v in (params or {}).items() if v is not None}
    base, sep, query = url.partition("?")
    if not base.endswith("/"):
        base += "/"
    full = base + sep + query

    _transport = transport or _curl_transport
    for attempt in range(2):
        hdrs = dict(headers or rdb_headers())
        if auth:
            if _jwt is None:
                _jwt = _mint_guest_jwt()
            if _jwt:
                hdrs["Authorization"] = f"Bearer {_jwt}"
        status, text = _transport(full, clean, hdrs, proxy_url)
        # 401/403 == the guest token expired (or was never minted). Re-mint once.
        if auth and status in (401, 403) and attempt == 0:
            _jwt = _mint_guest_jwt()
            if _jwt is None:
                break
            continue
        if status != 200 or not (text or "").strip():
            return {}
        try:
            body = json.loads(text)
        except json.JSONDecodeError:
            return {}
        return body if isinstance(body, (dict, list)) else {}
    return {}
