"""Runtime getter for wnba_stats wrappers — thin shim over the nba_stats runtime
with the WNBA host fixed (stats.wnba.com)."""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse.nba.nba_stats_runtime import _DefaultSeason, _latest_season
from sportsdataverse.nba.nba_stats_runtime import _get as _nba_get
from sportsdataverse.nba.nba_stats_runtime import stats_headers

__all__ = ["_get", "season_latest_with_data", "stats_headers"]


def season_latest_with_data(season: Optional[str]) -> str:
    """Return ``season`` unchanged, or the latest WNBA season that has data when it is ``None``.

    The codegen transform behind every generated ``wnba_stats_*`` season argument that wehoop gives
    a season default (stats.wnba.com answers most endpoints without one with an empty HTTP 500,
    and the rest with every season summed). It resolves per call, so a long-running process rolls
    over too. The WNBA tips off in mid-May, so the current year is the default from June and the
    previous year before it (``"2025"`` until May 2026, ``"2026"`` from June 2026). ``drafthistory``
    rolls over in May, after the mid-April draft; the playoffs (``commonplayoffseries``, or
    ``SeasonType`` ``"Playoffs"`` on any endpoint) in October, once they have started in
    mid-September. A fixed month table cannot follow a lockout, a CBA delay or a pandemic
    calendar; pass ``season`` explicitly then.

    Of the 79 season defaults in wehoop's ``wnba_stats_*.R`` that call ``most_recent_wnba_season()``,
    54 subtract one, a season behind this default in every month but May; 24 do not, the same as
    this default except in May, when theirs has not tipped off; one subtracts two.

    Args:
        season: The caller's season (e.g. ``"2024"``), or ``None`` for the latest one with data.
            An explicit ``""`` is returned as-is.

    Returns:
        str: The season year to send as ``Season`` / ``SeasonYear``.

    Example:
        Quick start::

            from sportsdataverse.wnba.wnba_stats_runtime import season_latest_with_data
            season_latest_with_data(None)     # "2026" in October 2026
            season_latest_with_data("2023")   # "2023"

        See Also:
            * `wehoop`_ -- the R sister package these defaults are mined from

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return season if season is not None else _DefaultSeason(_latest_season("10"))


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
            ``GameID`` is zero-padded to 10 characters. Season defaults are applied by the
            generated wrappers (:func:`season_latest_with_data`), not here.
        host: Target host. Defaults to ``"stats.wnba.com"``.
        **kwargs: Forwarded to :func:`sportsdataverse.nba.nba_stats_runtime._get`
            (``headers``, ``transport``, ``proxy_url``, etc.).

    Returns:
        Parsed JSON dict, or ``{}`` on non-200 status, blank body, or JSON error, which also
        warns :class:`~sportsdataverse.errors.EmptyResponseWarning`.

    Example:
        Quick start (offline — inject a transport)::

            from sportsdataverse.wnba.wnba_stats_runtime import _get
            def fake(url, params, headers, proxy_url):
                return 200, '{"resultSets": []}'
            data = _get("leaguedashplayerstats", {"LeagueID": "40"}, transport=fake)
    """
    return _nba_get(path, params, host=host, _shim_frames=1, **kwargs)
