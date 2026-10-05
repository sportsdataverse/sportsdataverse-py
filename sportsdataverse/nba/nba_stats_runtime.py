"""Runtime getter for the nba_stats wrappers.

stats.nba.com silently drops non-browser TLS/JA3 handshakes (plain requests times
out), so the live transport uses curl_cffi with Chrome impersonation. The HTTP call
is injectable (``transport=``) so wrappers/tests stay offline-friendly.
"""

from __future__ import annotations

import json
import os
import time
import warnings
from datetime import date
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from sportsdataverse.errors import EmptyResponseWarning

__all__ = ["_get", "season_latest_with_data", "stats_headers"]

Transport = Callable[[str, dict, dict, Optional[str]], tuple]


class _DefaultSeason(str):
    """A season a wrapper filled in (not the caller's). ``_get`` re-dates it for the league and endpoint."""


# The month from which the newest season has rows, and how many years after the season's first
# year that month falls. Measured on stats.nba.com 2026-10-05: the 2025-26 NBA season opened
# October 21; the G League "Regular Season" (the default SeasonType) ran 2025-12-19 to 2026-03-28,
# the Tip-Off Tournament before it is not in it; the Summer League labelled "2026-27" was played
# 2026-07-09 to 07-19; draft-combine SeasonYear "2026-27" holds the May 2026 combine.
_FIRST_ROWS = {
    "00": (11, 0),  # NBA: tips off late October
    "20": (1, 1),  # G League: regular season from late December
    "15": (8, 0),  # Summer League: July, labelled by its own year
    "10": (6, 0),  # WNBA: tips off mid-May
    "00 draftcombine": (6, 0),  # the mid-May combine
    "00 commonplayoffseries": (5, 1),  # NBA playoffs: from mid-April of the season's second year
    "10 commonplayoffseries": (10, 0),  # WNBA playoffs: from mid-September
}


def _latest_season(league_id: str = "00", endpoint: str = "", today: Optional[date] = None) -> str:
    """The latest season that has rows on ``today``, labelled for ``league_id``.

    ``"2025-26"`` style for the NBA, G League (``"20"``) and Summer League (``"15"``); a year for the
    WNBA (``"10"``). ``endpoint`` picks the draft-combine and playoff-series rules.
    """
    today = today or date.today()
    endpoint = "draftcombine" if endpoint.startswith("draftcombine") else endpoint
    month, lag = _FIRST_ROWS.get(f"{league_id} {endpoint}") or _FIRST_ROWS.get(league_id, _FIRST_ROWS["00"])
    start = today.year - lag - (today.month < month)
    return str(start) if league_id == "10" else f"{start}-{str(start + 1)[2:]}"


def season_latest_with_data(season: Optional[str]) -> str:
    """Return ``season`` unchanged, or the latest season that has data when it is ``None``.

    The codegen transform behind every generated ``nba_stats_*`` season argument that
    stats.nba.com needs (it answers a request without one with an empty HTTP 500). It resolves
    per call, not as a signature default, so a long-running process rolls over too. Each league
    rolls over once its newest season has rows:

    * NBA: from November (``"2025-26"`` until October 2026, ``"2026-27"`` from November 2026).
    * G League (``league_id="20"``): from the January after it tips off, when its regular season
      (from late December) has rows.
    * Summer League (``league_id="15"``): from August. stats.nba.com labels a Summer League by its
      own July, so July 2026's is ``"2026-27"``.
    * Draft combine (``SeasonYear``, read by its leading year): from June.
    * ``commonplayoffseries``: from May of the season's second year, once the playoffs have started.

    Until a rollover the previous season is returned. It has data, but for a few weeks after the
    newest season's first games it is not the newest: late October for the NBA, late December for
    the G League, July for the Summer League, late May for the combine, late April for the playoffs.

    Args:
        season: The caller's season (e.g. ``"2024-25"``), or ``None`` for the latest one with data.
            An explicit ``""`` is returned as-is.

    Returns:
        str: The season label to send as ``Season`` / ``SeasonYear``. The NBA label for ``None``;
        the stats getter re-labels it per league and endpoint.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_stats_runtime import season_latest_with_data
            season_latest_with_data(None)        # "2025-26" in October 2026
            season_latest_with_data("2023-24")   # "2023-24"

        See Also:
            * `hoopR`_ -- companion R package for the same endpoints

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return season if season is not None else _DefaultSeason(_latest_season())


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
    # No User-Agent: curl_cffi's Chrome impersonation sends one that matches its
    # sec-ch-ua client hints. An explicit Chrome/120 UA against impersonated
    # Chrome 146 (measured with curl_cffi 0.15.0) made the two disagree.
    return {
        "Host": host,
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
        Parsed JSON dict, or ``{}`` on non-200 status, blank body, or JSON error. Returning
        ``{}`` also warns :class:`~sportsdataverse.errors.EmptyResponseWarning`, naming the URL
        and status; silence it with ``warnings.filterwarnings("ignore", category=EmptyResponseWarning)``.

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
    if path.startswith("http://") or path.startswith("https://"):
        url = path
    else:
        url = f"https://{host}/stats/{path}"
    host = urlparse(url).netloc or host
    endpoint = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    league = str(clean.get("LeagueID") or ("10" if "wnba" in host else "00"))
    for key in ("Season", "SeasonYear"):
        if isinstance(clean.get(key), _DefaultSeason):  # a wrapper's default: date it for this league
            clean[key] = _latest_season(league, endpoint)
    clean = dict(sorted(clean.items()))

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
                payload = None
            if payload:  # a valid, non-empty envelope
                return payload
        # non-200 / blank / undecodable / bare {} — a transient throttle; retry
        if attempt < retries:
            time.sleep(backoff * (attempt + 1))
            continue
        if not text.strip():
            reason = " with an empty body"
        elif status != 200:
            reason = ""
        else:
            reason = " with a body that is not JSON" if payload is None else " with an empty object"
        warnings.warn(
            f"{url} answered HTTP {status}{reason}; returning {{}}. {host} answers this way when a "
            "parameter it needs is missing or invalid (most often Season), and when it throttles.",
            EmptyResponseWarning,
            stacklevel=3 + kwargs.get("_shim_frames", 0),  # the caller, past the WNBA shim's frame
        )
        return {}
    return {}  # unreachable; keeps type-checkers happy
