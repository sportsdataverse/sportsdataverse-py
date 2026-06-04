"""ESPN men's-college-basketball athlete *season* stats scraper (core-v2).

Thin league wrapper over
:func:`sportsdataverse._common_espn_player_stats._espn_player_stats`. Returns
**one wide row** (athlete identity + season stat line as ``{category}_{stat}``
columns + ``team_*`` identity) from ESPN's core-v2 ``/athletes/{id}/statistics``
graph -- matching the hoopR ``espn_mbb_player_stats`` convention. For the
richer web-v3 payload use :func:`sportsdataverse.mbb.espn_mbb_player_stats_v3`.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse._common_espn_player_stats import _espn_player_stats

_SPORT_SLUG: str = "basketball"
_LEAGUE_SLUG: str = "mens-college-basketball"


@overload
def espn_mbb_player_stats(
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
def espn_mbb_player_stats(
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
def espn_mbb_player_stats(
    athlete_id: int,
    season: int,
    *,
    season_type: str = ...,
    total: bool = ...,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_mbb_player_stats(
    athlete_id: int,
    season: int,
    *,
    season_type: str = "regular",
    total: bool = False,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull a men's-college-basketball athlete's ESPN **season** stat line.

    See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full
    documentation of the wide return shape, the ``{category}_{stat}`` stat
    columns, the athlete / team metadata blocks, and the ``season_type`` /
    ``total`` parameters. For the richer web-v3 payload use
    :func:`sportsdataverse.mbb.espn_mbb_player_stats_v3`.

    Args:
        athlete_id: ESPN men's-college-basketball athlete identifier.
        season: Season year, used in the core-v2 path.
        season_type: ``"regular"`` (type 2) or ``"postseason"`` (type 3).
        total: Forward-compat totals passthrough.
        raw: If True, returns the raw core-v2 statistics JSON dict.
        return_as_pandas: If True, returns a pandas DataFrame; else polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A single-row wide DataFrame (polars by default). When ``raw=True``
        returns the raw statistics JSON ``dict``.

    Raises:
        ValueError: ``season_type`` is not ``"regular"``/``"postseason"``.
        sportsdataverse.errors.NoESPNDataError: ESPN returned 404.

    Example:
        Pull a player's 2023 season line as a single wide row::

            from sportsdataverse.mbb import espn_mbb_player_stats
            df = espn_mbb_player_stats(athlete_id=4395624, season=2023)
            df.select(["full_name", "team_display_name", "offensive_points"])

    See Also:
        * :func:`espn_mbb_player_stats_v3` -- comprehensive web-v3 stat payload
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package (``espn_mbb_player_stats``)
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
