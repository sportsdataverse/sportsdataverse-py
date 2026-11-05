"""Women's college coach seasons and careers (BB-D8): ``wbb.coach_seasons`` / ``wbb.coach_careers``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_coach_seasons`: ``league="womens"`` switches to the
``ncaa_wbb_possessions`` release, the WBB schedule and the ``ncaa-wbb-hoops-raw`` roster pages.

Example:
    Quick start::

        from sportsdataverse.mbb.mbb_coach_seasons import ncaa_team_coaches
        from sportsdataverse.wbb.wbb_coach_seasons import wbb_coach_seasons
        coaches = ncaa_team_coaches("ncaa-wbb-hoops-raw", 2025, league="womens")
        wbb_coach_seasons(2025, coaches).head()
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_coach_seasons import mbb_coach_careers, mbb_coach_seasons

__all__ = ["wbb_coach_careers", "wbb_coach_seasons"]


def wbb_coach_seasons(
    seasons: int | list[int],
    coaches: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college head-coach seasons: W-L, ratings and four factors of each coach's team-seasons.

    Delegates to :func:`sportsdataverse.mbb.mbb_coach_seasons.mbb_coach_seasons` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        coaches: :func:`~sportsdataverse.mbb.mbb_coach_seasons.ncaa_team_coaches` rows
            (``league="womens"``) for those seasons.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, coach_id)``; see the mbb core for the columns.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_coach_seasons
            wbb_coach_seasons(2025, coaches).sort("net_rtg_rank").head()
    """
    return mbb_coach_seasons(seasons, coaches, league="womens", return_as_pandas=return_as_pandas)


def wbb_coach_careers(
    seasons: int | list[int],
    coaches: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college head-coach careers over ``seasons``.

    Delegates to :func:`sportsdataverse.mbb.mbb_coach_seasons.mbb_coach_careers` with
    ``league="womens"``.

    Args:
        seasons: The seasons the careers cover.
        coaches: :func:`~sportsdataverse.mbb.mbb_coach_seasons.ncaa_team_coaches` rows for them.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``coach_id``, most games first; see the mbb core.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_coach_careers
            wbb_coach_careers([2024, 2025], coaches).head()
    """
    return mbb_coach_careers(seasons, coaches, league="womens", return_as_pandas=return_as_pandas)
