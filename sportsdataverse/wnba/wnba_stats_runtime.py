"""Runtime getter for wnba_stats wrappers — thin shim over the nba_stats runtime
with the WNBA host fixed (stats.wnba.com)."""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse.nba.nba_stats_runtime import _get as _nba_get
from sportsdataverse.nba.nba_stats_runtime import stats_headers

__all__ = ["_get", "season_or_previous", "stats_headers"]


def season_or_previous(season: Optional[str]) -> str:
    """Return ``season`` unchanged, or the previous WNBA season year when it is ``None``.

    The codegen transform behind every generated ``wnba_stats_*`` season argument that
    stats.wnba.com needs (it answers a request without one with an empty HTTP 500). The previous
    season always has data, which a current-season default lacks before a season tips off. It is
    wehoop's own default, ``most_recent_wnba_season() - 1`` (``"2025"`` during 2026).

    Args:
        season: The caller's season (e.g. ``"2024"``), or ``None`` for the previous one.
            An explicit ``""`` is returned as-is.

    Returns:
        str: The season year to send as ``Season`` / ``SeasonYear``.

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
            ``GameID`` is zero-padded to 10 characters. Season defaults are applied by the
            generated wrappers (:func:`season_or_previous`), not here.
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
    return _nba_get(path, params, host=host, **kwargs)
