"""Runtime getter for nbagl_stats wrappers — thin shim over the nba_stats runtime.

The G League lives on the NBA host (stats.nba.com) under ``LeagueID=20``; the generated wrappers
send that league id by default. Transport, retries and ``SDV_PY_NBA_STATS_PROXY`` are the NBA
runtime's."""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse.nba.nba_stats_runtime import _DefaultSeason, _latest_season
from sportsdataverse.nba.nba_stats_runtime import _get as _nba_get
from sportsdataverse.nba.nba_stats_runtime import stats_headers

__all__ = ["_get", "season_latest_with_data", "stats_headers"]


def season_latest_with_data(season: Optional[str]) -> str:
    """Return ``season`` unchanged, or the latest G League season that has data when it is ``None``.

    The codegen transform behind every generated ``nbagl_stats_*`` season argument that hoopR gives
    a season default (no R package wraps the G League stats API, so the generator reuses hoopR's
    NBA defaults). It resolves per call. The G League regular season starts in late December, so a
    season becomes the default in January (``"2024-25"`` until December 2025, ``"2025-26"`` from
    January 2026); its playoffs (``commonplayoffseries``, or ``SeasonType`` ``"Playoffs"``) in May.
    The getter re-dates the value for the request's ``LeagueID``, endpoint and season type. A fixed
    month table cannot follow a lockout or pandemic calendar; pass ``season`` explicitly then.

    Args:
        season: The caller's season (e.g. ``"2024-25"``), or ``None`` for the latest one with data.
            An explicit ``""`` is returned as-is.

    Returns:
        str: The season label to send as ``Season`` / ``SeasonYear``.

    Example:
        Quick start::

            from sportsdataverse.nbagl.nbagl_stats_runtime import season_latest_with_data
            season_latest_with_data(None)       # "2025-26" in October 2026
            season_latest_with_data("2023-24")  # "2023-24"

        See Also:
            * `hoopR`_ -- the R package these defaults are mined from (NBA stats API)

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return season if season is not None else _DefaultSeason(_latest_season("20"))


def _get(
    path: str,
    params: Optional[dict] = None,
    *,
    host: str = "stats.nba.com",
    **kwargs: Any,
) -> dict:
    """Fetch a stats.nba.com endpoint for the G League and return parsed JSON.

    Thin shim over :func:`sportsdataverse.nba.nba_stats_runtime._get`; the generated
    ``nbagl_stats_*`` wrappers pass ``LeagueID="20"`` by default. All arguments are forwarded
    verbatim; see the NBA runtime docs for full parameter details.

    Args:
        path: Bare endpoint name or fully-qualified URL.
        params: Query-string parameters. ``None`` values are stripped; ``GameID`` is zero-padded to
            10 characters. Season defaults are applied by the generated wrappers
            (:func:`season_latest_with_data`), not here.
        host: Target host. Defaults to ``"stats.nba.com"``.
        **kwargs: Forwarded to :func:`sportsdataverse.nba.nba_stats_runtime._get`
            (``headers``, ``transport``, ``proxy_url``, etc.).

    Returns:
        Parsed JSON dict. A 2xx with an empty JSON object (or a 204 / 205) returns ``{}`` and
        warns :class:`~sportsdataverse.errors.EmptyResponseWarning`.

    Raises:
        NoDataError: The host answered HTTP 404.
        ValueError: The host answered HTTP 400 / 422.
        AssetFetchError: Any other non-2xx (an empty HTTP 500 included), a blank or non-JSON
            2xx body, or a connection failure, once ``SDV_PY_NBA_STATS_RETRIES`` is spent.

    Example:
        Quick start (offline — inject a transport)::

            from sportsdataverse.nbagl.nbagl_stats_runtime import _get
            def fake(url, params, headers, proxy_url):
                return 200, '{"resultSets": []}'
            data = _get("leaguedashplayerstats", {"LeagueID": "20"}, transport=fake)
    """
    return _nba_get(path, params, host=host, _shim_frames=1, **kwargs)
