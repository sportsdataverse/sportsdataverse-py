"""Runtime getter for wnba_stats wrappers — thin shim over the nba_stats runtime
with the WNBA host fixed (stats.wnba.com)."""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse.nba.nba_stats_runtime import _get as _nba_get
from sportsdataverse.nba.nba_stats_runtime import stats_headers

__all__ = ["_get", "season_or_current", "season_or_previous", "stats_headers"]


def season_or_current(season: Optional[str]) -> str:
    """Return ``season`` unchanged, or the current WNBA season year when it is ``None``.

    The codegen transform behind every generated ``wnba_stats_*`` season argument whose wehoop
    default is a ``most_recent_wnba_season()`` call. stats.wnba.com answers those endpoints with
    an empty HTTP 500 when ``Season`` is missing. wehoop often writes
    ``most_recent_wnba_season() - 1``, which is always one season behind the most recent one
    (last season while one is being played, the season before last in the offseason). hoopR's
    matching default is the current season, so that is what this returns.

    Args:
        season: The caller's season (e.g. ``"2025"``), or ``None`` for the current one.
            An explicit ``""`` is returned as-is.

    Returns:
        str: The season year to send as ``Season`` / ``SeasonYear``.

    Example:
        Quick start::

            from sportsdataverse.wnba.wnba_stats_runtime import season_or_current
            season_or_current(None)     # e.g. "2026"
            season_or_current("2024")   # "2024"

        See Also:
            * `wehoop`_ -- the R sister package these defaults are mined from

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    if season is not None:
        return season
    from sportsdataverse.wnba.wnba_schedule import most_recent_wnba_season

    return str(most_recent_wnba_season())


def season_or_previous(season: Optional[str]) -> str:
    """Return ``season`` unchanged, or the previous WNBA season year when it is ``None``.

    The codegen transform for the season arguments whose hoopR counterpart defaults to the last
    finished season (``commonplayoffseries``); wehoop's own default there is
    ``most_recent_wnba_season() - 1``, which this returns.

    Args:
        season: The caller's season (e.g. ``"2024"``), or ``None`` for the previous one.
            An explicit ``""`` is returned as-is.

    Returns:
        str: The season year to send as ``Season``.

    Example:
        Quick start::

            from sportsdataverse.wnba.wnba_stats_runtime import season_or_previous
            season_or_previous(None)     # e.g. "2025"
            season_or_previous("2023")   # "2023"

        See Also:
            * `wehoop`_ -- the R sister package these defaults are mined from

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    if season is not None:
        return season
    from sportsdataverse.wnba.wnba_schedule import most_recent_wnba_season

    return str(most_recent_wnba_season() - 1)


def _get(
    path: str,
    params: Optional[dict] = None,
    *,
    host: str = "stats.wnba.com",
    **kwargs: Any,
) -> dict:
    """Fetch a stats.wnba.com endpoint and return parsed JSON.

    Thin shim over :func:`sportsdataverse.nba.nba_stats_runtime._get` with
    the host defaulted to ``"stats.wnba.com"``.  All arguments are forwarded
    verbatim; see the NBA runtime docs for full parameter details.

    URL handling mirrors the NBA runtime (dual bare-path / full-URL):
        - Full URLs (``"https://..."`` or ``"http://..."``) are passed through verbatim.
        - Bare endpoint names are expanded to ``f"https://{host}/stats/{path}"``.

    Args:
        path: Bare endpoint name or fully-qualified URL.
        params: Query-string parameters. ``None`` values are stripped;
            ``GameID`` is zero-padded to 10 characters.
        host: Target host. Defaults to ``"stats.wnba.com"``.
        **kwargs: Forwarded to :func:`sportsdataverse.nba.nba_stats_runtime._get`
            (``headers``, ``transport``, ``proxy_url``, etc.).

    Returns:
        Parsed JSON dict, or ``{}`` on non-200 status, blank body, or JSON error.

    Example:
        Quick start (offline — inject a transport)::

            from sportsdataverse.wnba.wnba_stats_runtime import _get
            def fake(url, params, headers, proxy_url):
                return 200, '{"resultSets": []}'
            data = _get("leaguedashplayerstats", {"LeagueID": "40"}, transport=fake)
    """
    return _nba_get(path, params, host=host, **kwargs)
