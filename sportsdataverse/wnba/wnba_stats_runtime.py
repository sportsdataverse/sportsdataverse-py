"""Runtime getter for wnba_stats wrappers — thin shim over the nba_stats runtime
with the WNBA host fixed (stats.wnba.com)."""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse.nba.nba_stats_runtime import _get as _nba_get
from sportsdataverse.nba.nba_stats_runtime import stats_headers

__all__ = ["_get", "stats_headers"]


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
