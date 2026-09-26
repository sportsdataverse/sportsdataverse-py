"""WNBA cdn.wnba.com liveData play-by-play and boxscore wrappers.

Thin shim over :mod:`sportsdataverse.nba.nba_live` -- ``cdn.wnba.com`` serves the
same liveData JSON shape as ``cdn.nba.com``, just under a different host and
``Origin``/``Referer``. The fetch helper (``_fetch_live``, curl_cffi Chrome
impersonation -- see the ``nba_live`` module docstring for the transport-spike
rationale) and both parsers (``parse_nba_live_pbp`` / ``parse_nba_live_boxscore``)
are reused verbatim; this module supplies only the ``league="wnba"`` host/header
switch, mirroring wehoop's ``wnba_live_pbp`` / ``wnba_live_boxscore``.
"""

from __future__ import annotations

from typing import Any

from sportsdataverse.nba.nba_live import _fetch_live, parse_nba_live_boxscore, parse_nba_live_pbp

__all__ = ["wnba_live_pbp", "wnba_live_boxscore"]


def _wnba_live(kind: str, game_id: str | int, *, proxy: dict[str, str] | None = None) -> dict[str, Any]:
    """Fetch a cdn.wnba.com liveData payload -- the one helper both public functions share."""
    return _fetch_live(kind, game_id, league="wnba", proxy=proxy)


def wnba_live_pbp(
    game_id: str | int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    proxy: dict[str, str] | None = None,
) -> Any:
    """Fetch and parse WNBA cdn.wnba.com liveData play-by-play for a game.

    Retrieves ``https://cdn.wnba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json``
    and parses it via :func:`sportsdataverse.nba.nba_live.parse_nba_live_pbp`. Carries
    the same per-whistle referee ids (``official_id``) and wall-clock timestamps
    (``time_actual``) as the NBA feed.

    Args:
        game_id: WNBA game ID (int or str). Zero-padded to 10 digits.
        raw: If True, return the raw JSON payload (dict) instead of a DataFrame.
        return_as_pandas: If True, return a pandas DataFrame instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        If ``raw=True``, the raw JSON dict. Otherwise, a DataFrame as documented
        in :func:`sportsdataverse.nba.nba_live.parse_nba_live_pbp`.

    Raises:
        NoDataError: The game has no liveData play-by-play object.
        AssetFetchError: The fetch failed (network error, rate limit, or a
            bot-check block).
        ImportError: curl_cffi is not installed -- required for the live
            transport (``pip install curl_cffi`` / ``sportsdataverse[all]``).

    Example:
        Fetch a game's live play-by-play::

            from sportsdataverse.wnba.wnba_live import wnba_live_pbp
            pbp = wnba_live_pbp("1022600097")
            print(pbp.filter(pbp["action_type"] == "foul").height)

        See Also:
            * `wehoop`_ -- R package for WNBA data access and visualization
            * `hoopR`_ -- R package for NBA data access and visualization

            .. _wehoop: https://wehoop.sportsdataverse.org
            .. _hoopR: https://hoopR.sportsdataverse.org
    """
    payload = _wnba_live("playbyplay", game_id, proxy=proxy)
    return payload if raw else parse_nba_live_pbp(payload, return_as_pandas=return_as_pandas)


def wnba_live_boxscore(
    game_id: str | int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    proxy: dict[str, str] | None = None,
) -> Any:
    """Fetch and parse WNBA cdn.wnba.com liveData boxscore for a game.

    Retrieves ``https://cdn.wnba.com/static/json/liveData/boxscore/boxscore_{game_id}.json``
    and parses it via :func:`sportsdataverse.nba.nba_live.parse_nba_live_boxscore`
    into six tables (game, officials, home/away players, home/away team).

    Args:
        game_id: WNBA game ID (int or str). Zero-padded to 10 digits.
        raw: If True, return the raw JSON payload (dict) instead of parsed DataFrames.
        return_as_pandas: If True, return pandas DataFrames instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        If ``raw=True``, the raw JSON dict. Otherwise, a dict of DataFrames as
        documented in :func:`sportsdataverse.nba.nba_live.parse_nba_live_boxscore`.

    Raises:
        NoDataError: The game has no liveData boxscore object.
        AssetFetchError: The fetch failed (network error, rate limit, or a
            bot-check block).
        ImportError: curl_cffi is not installed -- required for the live
            transport (``pip install curl_cffi`` / ``sportsdataverse[all]``).

    Example:
        Fetch a game's live boxscore::

            from sportsdataverse.wnba.wnba_live import wnba_live_boxscore
            result = wnba_live_boxscore("1022600097")
            officials = result["officials"]
            print(officials.select("person_id", "name", "assignment"))

        See Also:
            * `wehoop`_ -- R package for WNBA data access and visualization
            * `hoopR`_ -- R package for NBA data access and visualization

            .. _wehoop: https://wehoop.sportsdataverse.org
            .. _hoopR: https://hoopR.sportsdataverse.org
    """
    payload = _wnba_live("boxscore", game_id, proxy=proxy)
    return payload if raw else parse_nba_live_boxscore(payload, return_as_pandas=return_as_pandas)
