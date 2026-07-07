"""Women's college basketball opponent-adjusted team ratings.

Thin shim over :mod:`sportsdataverse.mbb.mbb_team_ratings` -- the efficiency /
tempo fixed-point engine is league-agnostic (every women's-specific number
comes from ``LEAGUE_CONSTANTS["womens"]``), and the loader dispatch inside the
mbb core switches to the WBB schedule/boxscore releases when
``league="womens"``. The engine functions are re-exported **by reference**.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_team_ratings import wbb_team_ratings
        ratings = wbb_team_ratings(2024)
        ratings.sort("rank").head()

See Also:
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
"""

from __future__ import annotations

from typing import Union

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_team_ratings import (
    adjust_efficiency,
    adjust_tempo,
    mbb_team_ratings,
    raw_game_efficiency,
)

__all__ = [
    "adjust_efficiency",
    "adjust_tempo",
    "raw_game_efficiency",
    "wbb_team_ratings",
]


def wbb_team_ratings(
    seasons: Union[int, list[int]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Women's opponent-adjusted team ratings (AdjO/AdjD/AdjEM/AdjTempo).

    Delegates to :func:`sportsdataverse.mbb.mbb_team_ratings.mbb_team_ratings`
    with ``league="womens"`` (WBB loaders + women's fitted constants).

    Args:
        seasons: A season (e.g. ``2024``) or list of seasons.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per (season, team_id) -- see the mbb core for the schema.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_team_ratings
            wbb_team_ratings(2024).sort("rank").head()
    """
    return mbb_team_ratings(seasons, league="womens", return_as_pandas=return_as_pandas)
