"""ESPN WNBA athlete *season* stats scraper (core-v2).

Thin league wrapper over
:func:`sportsdataverse._common_espn_player_stats._espn_player_stats`. Returns
**one wide row** (athlete identity + season stat line as
``{category}_{stat}`` columns + ``team_*`` identity) from ESPN's core-v2
``/athletes/{id}/statistics`` graph -- matching the wehoop / hoopR
``espn_*_player_stats`` convention. For the richer web-v3 payload use
:func:`sportsdataverse.wnba.espn_wnba_player_stats_v3`.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse._common_espn_player_stats import _espn_player_stats

_SPORT_SLUG: str = "basketball"
_LEAGUE_SLUG: str = "wnba"


@overload
def espn_wnba_player_stats(
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
def espn_wnba_player_stats(
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
def espn_wnba_player_stats(
    athlete_id: int,
    season: int,
    *,
    season_type: str = ...,
    total: bool = ...,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_wnba_player_stats(
    athlete_id: int,
    season: int,
    *,
    season_type: str = "regular",
    total: bool = False,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull a WNBA athlete's ESPN **season** stat line.

    See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full
    documentation of the wide return shape, the ``{category}_{stat}``
    stat columns, the athlete / team metadata blocks, and the
    ``season_type`` / ``total`` parameters.

    Args:
        athlete_id: ESPN WNBA athlete identifier (e.g. ``3149391`` for A'ja
            Wilson).
        season: Season year, used in the core-v2 path.
        season_type: ``"regular"`` (type 2) or ``"postseason"`` (type 3).
        total: Forward-compat totals passthrough.
        raw: If True, returns the raw core-v2 statistics JSON dict.
        return_as_pandas: If True, returns a pandas DataFrame; else polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A single-row wide DataFrame (polars by default). When ``raw=True``
        returns the raw statistics JSON ``dict``. See
        :func:`sportsdataverse.wbb.espn_wbb_player_stats` for the column
        layout.

    Raises:
        ValueError: ``season_type`` is not ``"regular"``/``"postseason"``.
        sportsdataverse.errors.NoDataError: ESPN returned 404.

    Example:
        Pull A'ja Wilson's 2024 season line as a single wide row::

            from sportsdataverse.wnba import espn_wnba_player_stats
            df = espn_wnba_player_stats(athlete_id=3149391, season=2024)
            df.select(["full_name", "team_display_name", "offensive_points"])

        See Also:
            * :func:`sportsdataverse.wnba.espn_wnba_player_stats_v3` -- web-v3 stats
            * `wehoop`_ -- R sister package; mirrors this surface

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
