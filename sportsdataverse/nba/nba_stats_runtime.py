"""Runtime getter for the nba_stats wrappers.

stats.nba.com silently drops non-browser TLS/JA3 handshakes (plain requests times
out), so the live transport uses curl_cffi with Chrome impersonation. The HTTP call
is injectable (``transport=``) so wrappers/tests stay offline-friendly.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Optional

__all__ = ["_get", "stats_headers"]

Transport = Callable[[str, dict, dict, Optional[str]], tuple]


def stats_headers(host: str = "stats.nba.com") -> dict:
    """Build browser-mimicking request headers for stats.nba.com / stats.wnba.com.

    The headers satisfy stats.nba.com's JA3/browser-origin checks:
    ``x-nba-stats-token`` and ``x-nba-stats-origin`` are required by the API;
    ``Referer`` and ``Origin`` switch to wnba.com when *host* contains ``"wnba"``.

    Args:
        host: The stats host (e.g. ``"stats.nba.com"`` or ``"stats.wnba.com"``).
            Determines whether NBA or WNBA referrer/origin values are used.

    Returns:
        A dict of HTTP request headers suitable for use with curl_cffi or requests.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_stats_runtime import stats_headers
            h = stats_headers("stats.nba.com")
            print(h["x-nba-stats-token"])  # "true"

        WNBA host::

            h = stats_headers("stats.wnba.com")
            print(h["Referer"])  # "https://www.wnba.com/"
    """
    is_wnba = "wnba" in host
    return {
        "Host": host,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.wnba.com/" if is_wnba else "https://www.nba.com/",
        "Origin": "https://www.wnba.com" if is_wnba else "https://www.nba.com",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
        "Connection": "keep-alive",
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
            "Live stats.nba.com calls require curl_cffi (stats.nba.com fingerprint-blocks "
            "plain requests). Install with: pip install curl_cffi"
        ) from exc
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    # Tunable per-request timeout. stats.nba.com is slow for some historical
    # endpoints (a real gamerotation payload for a 2011-12 game can take ~27s,
    # right at the old hardcoded 30s cliff), so bump SDV_PY_NBA_STATS_TIMEOUT
    # when back-filling old seasons.
    timeout = float(os.environ.get("SDV_PY_NBA_STATS_TIMEOUT", "30"))
    r = creq.get(
        url,
        params=params,
        headers=headers,
        proxies=proxies,
        impersonate="chrome",
        timeout=timeout,
    )
    return r.status_code, r.text


def _get(
    path: str,
    params: Optional[dict] = None,
    *,
    host: str = "stats.nba.com",
    headers: Optional[dict] = None,
    transport: Optional[Transport] = None,
    proxy_url: Optional[str] = None,
    **kwargs: Any,
) -> dict:
    """Fetch a stats.nba.com (or stats.wnba.com) endpoint and return parsed JSON.

    Handles the JA3/TLS browser-fingerprint requirement by routing live calls
    through ``curl_cffi`` with Chrome impersonation. The transport is injectable
    so wrappers and tests can run fully offline.

    URL handling (dual bare-path / full-URL):
        - If *path* already starts with ``"http://"`` or ``"https://"``, it is
          used verbatim as the request URL.
        - Otherwise the URL is built as ``f"https://{host}/stats/{path}"``.

    Args:
        path: Either a bare endpoint name (e.g. ``"leaguedashplayerstats"``) or a
            fully-qualified URL (e.g.
            ``"https://stats.nba.com/stats/leaguedashplayerstats"``).  The codegen
            wrappers pass full URLs; the bare-path form is convenient for ad-hoc use.
        params: Query-string parameters. ``None`` values are stripped before the
            request.  ``GameID`` is zero-padded to 10 characters.
        host: Target host, used only when *path* is a bare endpoint name.
            Defaults to ``"stats.nba.com"``.
        headers: HTTP headers dict.  Defaults to ``stats_headers(host)``.
        transport: Callable with signature
            ``(url, params, headers, proxy_url) -> (status_code, text)``.
            Defaults to ``_curl_transport`` (curl_cffi Chrome impersonation).
        proxy_url: Optional proxy URL forwarded to the transport.
        **kwargs: Accepted for forward-compatibility with generated callers; unused.

    Returns:
        Parsed JSON dict, or ``{}`` on non-200 status, blank body, or JSON error.

    Example:
        Quick start (offline — inject a transport)::

            from sportsdataverse.nba.nba_stats_runtime import _get
            def fake(url, params, headers, proxy_url):
                return 200, '{"resultSets": []}'
            data = _get("leaguedashplayerstats", {"LeagueID": "00"}, transport=fake)

        Full-URL passthrough (codegen wrapper style)::

            data = _get(
                "https://stats.nba.com/stats/leaguedashplayerstats",
                {"LeagueID": "00"},
                transport=fake,
            )
    """
    clean: dict = {k: v for k, v in (params or {}).items() if v is not None}
    if "GameID" in clean:
        clean["GameID"] = str(clean["GameID"]).zfill(10)
    # nba_api sorts query parameters alphabetically before sending -- their
    # source carries the comment "for some reason this matters for some
    # requests". Dict insertion order survives all the way through curl_cffi's
    # query string, so match that canonical order. Free insurance against the
    # order-sensitive endpoints; a no-op for everything else.
    clean = dict(sorted(clean.items()))

    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        url = f"https://{host}/stats/{path}"

    _transport = transport or _curl_transport
    _headers = headers or stats_headers(host)

    # Optional retry-with-backoff for the throttle/slowness failure modes.
    # stats.nba.com intermittently hangs (curl timeout) or returns a blank /
    # bare ``{}`` body under load for historical endpoints even though the data
    # exists — a retry recovers it. Defaults to 0 retries so behavior is
    # byte-identical unless SDV_PY_NBA_STATS_RETRIES is set (back-fill sweeps
    # set it; the tight-timeout single-shot path is unchanged for everyone else).
    retries = int(os.environ.get("SDV_PY_NBA_STATS_RETRIES", "0"))
    backoff = float(os.environ.get("SDV_PY_NBA_STATS_BACKOFF", "1.5"))
    for attempt in range(retries + 1):
        try:
            status, text = _transport(url, clean, _headers, proxy_url)
        except Exception:
            if attempt < retries:
                time.sleep(backoff * (attempt + 1))
                continue
            raise  # exhausted: preserve the "timeout propagates" contract
        if status == 200 and text.strip():
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                payload = {}
            if payload:  # a valid, non-empty envelope
                return payload
        # non-200 / blank / undecodable / bare {} — a transient throttle; retry
        if attempt < retries:
            time.sleep(backoff * (attempt + 1))
            continue
        return {}
    return {}  # unreachable; keeps type-checkers happy
