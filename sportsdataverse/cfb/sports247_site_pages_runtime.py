"""Runtime getter for the generated ``sports247_site_pages`` wrappers.

``247sports.com`` (the ``www`` front-end host, **not** ``ipa.247sports.com``)
serves a family of ``*.json`` page-model routes that back the site's rendered
pages — institution / player / coach detail, recruit class rankings, expert
predictions, draft-pick embeds, etc. Two mechanics live here, and both are
deliberate *deltas* from the sibling :mod:`sportsdataverse.cfb.sports247_runtime`
(which fronts the guest-JWT ``ipa.247sports.com/rdb/v1`` RDB):

* **Browser-TLS impersonation** — the Fastly edge fingerprint-blocks plain
  ``requests`` (0-byte 403 on every route), the same class of block as
  stats.nba.com, so the live transport uses ``curl_cffi`` with Chrome
  impersonation. ``curl_cffi`` is a lazy optional import (``tests``/``all``
  extras); the HTTP call is injectable (``transport=``) so wrappers and tests
  run fully offline.
* **Auth-free, no slash-rewrite** — unlike the RDB runtime there is **no
  guest-JWT bearer** (the ``*.json`` surface is public) and **no trailing-slash
  enforcement** (these URLs terminate in ``.json``; appending a slash would
  404). ``_get`` therefore issues a single unauthenticated request against the
  URL exactly as the generated wrapper built it.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Union

__all__ = ["_get", "site_headers"]

Transport = Callable[[str, dict, dict, Optional[str]], tuple]


def site_headers() -> Dict[str, str]:
    """Build browser-mimicking request headers for 247sports.com.

    Returns:
        A dict of HTTP request headers suitable for curl_cffi or requests.

    Example:
        Quick start::

            from sportsdataverse.cfb.sports247_site_pages_runtime import site_headers
            h = site_headers()
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
            "Live 247sports.com calls require curl_cffi (the site edge "
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
    """GET a 247sports.com site-page ``*.json`` route and return its JSON body.

    Auth-free and slash-preserving (see the module docstring for why): a single
    unauthenticated request against ``url`` exactly as built by the generated
    wrapper, with ``None``-valued query params stripped.

    Args:
        url: fully-qualified ``247sports.com/....json`` URL built by the
            generated wrapper. The path is sent verbatim (no trailing slash).
        params: query-string parameters; ``None`` values are stripped.
        headers: HTTP headers dict. Defaults to :func:`site_headers`.
        transport: callable with signature
            ``(url, params, headers, proxy_url) -> (status_code, text)``.
            Defaults to curl_cffi Chrome impersonation.
        proxy_url: optional proxy URL forwarded to the transport.
        **kwargs: accepted for forward-compatibility with generated callers;
            unused.

    Returns:
        Parsed JSON (``dict`` for the detail routes, ``list`` for the array
        routes), or ``{}`` on non-200 status, blank body, or JSON error.

    Example:
        Quick start (offline — inject a transport)::

            import sportsdataverse.cfb.sports247_site_pages_runtime as rt
            def fake(url, params, headers, proxy_url):
                return 200, '{"Key": 24099}'
            data = rt._get("https://247sports.com/Institution/24099.json", transport=fake)
    """
    clean: Dict[str, Any] = {k: v for k, v in (params or {}).items() if v is not None}
    hdrs = dict(headers or site_headers())
    _transport = transport or _curl_transport
    status, text = _transport(url, clean, hdrs, proxy_url)
    if status != 200 or not (text or "").strip():
        return {}
    try:
        body = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return body if isinstance(body, (dict, list)) else {}
