"""Runtime getter for the generated ``sports247`` wrappers (247Sports RDB).

``ipa.247sports.com`` fronts the 247Sports **Recruit Database** (RDB — the
service the ``247sports-recruit-database.openapi.yaml`` spec describes; note
the host is ``ipa.``, not ``api.``). Two mechanics live here:

* **Browser-TLS impersonation** — the Fastly edge fingerprint-blocks plain
  ``requests`` (0-byte 403 on every route), the same class of block as
  stats.nba.com, so the live transport uses ``curl_cffi`` with Chrome
  impersonation. ``curl_cffi`` is a lazy optional import (``tests``/``all``
  extras); the HTTP call is injectable (``transport=``) so wrappers and tests
  run fully offline.
* **Trailing-slash enforcement** — the RDB 301-redirects every slash-less
  path (``/rdb/v1/teams`` -> ``/rdb/v1/teams/``); ``_get`` appends the slash
  up front to save the extra round trip.

Only the RDB's **public** endpoints are wrapped (``teams``, per-year
``institutionrankings``). The other ~23 ``/rdb/v1/*`` routes return 401
without an internal CBSi bearer token and are intentionally not exposed.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Union

__all__ = ["_get", "rdb_headers"]

Transport = Callable[[str, dict, dict, Optional[str]], tuple]


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
    }


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
    **kwargs: Any,
) -> Union[Dict, List]:
    """GET an ipa.247sports.com RDB route and return its JSON body.

    Args:
        url: fully-qualified RDB URL built by the generated wrapper. A missing
            trailing slash is added (the RDB 301s slash-less paths).
        params: query-string parameters; ``None`` values are stripped.
        headers: HTTP headers dict. Defaults to :func:`rdb_headers`.
        transport: callable with signature
            ``(url, params, headers, proxy_url) -> (status_code, text)``.
            Defaults to curl_cffi Chrome impersonation.
        proxy_url: optional proxy URL forwarded to the transport.
        **kwargs: accepted for forward-compatibility with generated callers;
            unused.

    Returns:
        Parsed JSON (``dict`` for enveloped payloads, ``list`` for the teams
        directory), or ``{}`` on non-200 status, blank body, or JSON error.

    Example:
        Quick start (offline — inject a transport)::

            from sportsdataverse.cfb.sports247_runtime import _get
            def fake(url, params, headers, proxy_url):
                return 200, '[{"teamId": 1}]'
            data = _get("https://ipa.247sports.com/rdb/v1/teams/", {"sportKey": 1}, transport=fake)
    """
    clean: Dict[str, Any] = {k: v for k, v in (params or {}).items() if v is not None}
    base, sep, query = url.partition("?")
    if not base.endswith("/"):
        base += "/"
    full = base + sep + query

    _transport = transport or _curl_transport
    status, text = _transport(full, clean, headers or rdb_headers(), proxy_url)
    if status != 200 or not (text or "").strip():
        return {}
    try:
        body = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return body if isinstance(body, (dict, list)) else {}
