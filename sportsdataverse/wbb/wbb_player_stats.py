"""ESPN women's-college-basketball athlete *season* stats scraper (core-v2).

Thin league wrapper over
:func:`sportsdataverse._common_espn_player_stats._espn_player_stats`. Returns
**one wide, self-describing row** (athlete identity + season line as
``{category}_{stat}`` columns + ``team_*`` identity) from ESPN's core-v2
``/athletes/{id}/statistics`` graph -- matching the wehoop / hoopR /
cfbfastR ``espn_*_player_stats`` convention.

``player_stats`` = season line (core-v2); ``player_stats_v3`` =
comprehensive (web-v3, see :func:`espn_wbb_player_stats_v3`).
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse._common_espn_player_stats import _espn_player_stats

_SPORT_SLUG: str = "basketball"
_LEAGUE_SLUG: str = "womens-college-basketball"


@overload
def espn_wbb_player_stats(
    athlete_id: int,
    season: int,
    *,
    season_type: str = ...,
    total: bool = ...,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wbb_player_stats(
    athlete_id: int,
    season: int,
    *,
    season_type: str = ...,
    total: bool = ...,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> pd.DataFrame: ...
@overload
def espn_wbb_player_stats(
    athlete_id: int,
    season: int,
    *,
    season_type: str = ...,
    total: bool = ...,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_wbb_player_stats(
    athlete_id: int,
    season: int,
    *,
    season_type: str = "regular",
    total: bool = False,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull a women's-college-basketball athlete's ESPN **season** stat line.

    Returns **one wide row** combining athlete identity, the season stat
    line pivoted as ``{category}_{stat}`` columns, and team identity. For
    the richer multi-category web-v3 payload use
    :func:`espn_wbb_player_stats_v3` instead.

    Args:
        athlete_id: ESPN athlete identifier (e.g. ``4433985``).
        season: Season year, used in the core-v2 path.
        season_type: ``"regular"`` (type 2) or ``"postseason"`` (type 3).
        total: Forward-compat totals passthrough.
        raw: If True, returns the raw core-v2 statistics JSON dict.
        return_as_pandas: If True, returns a pandas DataFrame; else polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A single-row wide DataFrame (polars by default). Columns: identity /
        echo (``season``, ``season_type``, ``total``), athlete metadata
        (``athlete_id``, ``full_name``, ``position_*``, ...), the season
        stat line as ``{category}_{stat}`` numeric columns (e.g.
        ``offensive_points``, ``defensive_blocks``), and team metadata
        (``team_id``, ``team_display_name``, ...). When ``raw=True`` returns
        the raw statistics JSON ``dict``.

    Raises:
        ValueError: ``season_type`` is not ``"regular"``/``"postseason"``.
        sportsdataverse.errors.NoDataError: ESPN returned 404 (no season
            line for that athlete/season).

    Example:
        Pull a player's 2025 season line as a single wide row::

            from sportsdataverse.wbb import espn_wbb_player_stats
            df = espn_wbb_player_stats(athlete_id=4433985, season=2025)
            df.select(["full_name", "team_display_name", "offensive_points"])

        See Also:
            * :func:`espn_wbb_player_stats_v3` -- comprehensive web-v3 stats
            * `wehoop`_ -- R sister package (``espn_wbb_player_stats``)

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return _espn_player_stats(
        sport=_SPORT_SLUG,
        league=_LEAGUE_SLUG,
        athlete_id=athlete_id,
        season=season,
        season_type=season_type,
        total=total,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )
