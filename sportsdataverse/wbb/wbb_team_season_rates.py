"""Women's college season team rates from possessions (BB-D3): ``wbb.team_season_rates``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_team_season_rates`: ``league="womens"`` switches
to the ``ncaa_wbb_*`` releases and the WBB schedule, and to quarters from 2016.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_team_season_rates import wbb_team_season_rates
        rates = wbb_team_season_rates(2025)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_team_season_rates import (
    mbb_team_game_four_factors,
    mbb_team_game_possessions,
    mbb_team_season_rates,
)

__all__ = ["wbb_team_game_four_factors", "wbb_team_game_possessions", "wbb_team_season_rates"]


def wbb_team_season_rates(
    seasons: int | list[int],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college season team rates from possessions, with home/away/close/clutch splits.

    Delegates to :func:`sportsdataverse.mbb.mbb_team_season_rates.mbb_team_season_rates` with
    ``league="womens"``. Clutch is a possession in play in the last 5:00 of the 4th quarter (the
    2nd half before 2016) or an overtime, the score within 5 as it starts. That is one starting
    with 5:00 or less left, or the one live at the mark: a possession from 5:10 to 4:50 counts in
    full, one that ends exactly at 5:00 does not.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, split)``: ``games``, ``poss``, ``opp_poss``, ``pts``,
        ``opp_pts``, the twelve rates (``off_rtg``, ``def_rtg``, ``net_rtg``, ``pace``, the four
        factors and the four against) and their ``_rank`` / ``_pct`` / ``_n``. See the mbb core
        for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_team_season_rates
            wbb_team_season_rates(2025).filter(split="all").sort("net_rtg_rank").head()
    """
    return mbb_team_season_rates(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_team_game_possessions(
    seasons: int | list[int],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college per team and game: the possession counts behind :func:`wbb_team_season_rates`.

    Delegates to :func:`sportsdataverse.mbb.mbb_team_season_rates.mbb_team_game_possessions` with
    ``league="womens"``. A team-season's rows of a split add up to its season row.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, game_id, split)``: ``poss``, ``opp_poss``, ``pts``,
        ``opp_pts`` and ``seconds``. See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_team_game_possessions
            wbb_team_game_possessions(2025).filter(split="all").head()
    """
    return mbb_team_game_possessions(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_team_game_four_factors(
    seasons: int | list[int],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college per team and game: the four factors in points against the season's league average.

    Delegates to :func:`sportsdataverse.mbb.mbb_team_season_rates.mbb_team_game_four_factors` with
    ``league="womens"``. The five ``*_pts`` columns add up to ``pts_vs_exp`` exactly.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, game_id)``: ``poss``, ``pts``, ``exp_pts``, ``pts_vs_exp``,
        the five ``*_pts`` columns and the season's league values. See the mbb core for the
        definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_team_game_four_factors
            wbb_team_game_four_factors(2025).sort("pts_vs_exp").head()
    """
    return mbb_team_game_four_factors(seasons, league="womens", return_as_pandas=return_as_pandas)
