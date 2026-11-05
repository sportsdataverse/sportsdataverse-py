"""Women's college situational splits (BB-D21): ``wbb.situational_{team,player,lineup}``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_situational`: ``league="womens"`` switches to the
``ncaa_wbb_possessions`` release, the women's schedule and ``wbb_ratings``.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_situational import wbb_situational_team
        splits = wbb_situational_team(2026)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_situational import mbb_situational_lineup, mbb_situational_player, mbb_situational_team

__all__ = ["wbb_situational_lineup", "wbb_situational_player", "wbb_situational_team"]


def wbb_situational_team(
    seasons: int | list[int], *, top_n: int = 25, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Women's college team splits by situation: clutch, period, half, garbage time, score, venue, opponent, play type.

    Delegates to :func:`sportsdataverse.mbb.mbb_situational.mbb_situational_team` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        top_n: How many of the season's best teams by the published ``adj_em`` are ``vs_top``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, family, split)`` with the columns of
        :func:`sportsdataverse.nba.nba_situational.nba_situational_team`.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_situational_team
            wbb_situational_team(2026).filter(split="clutch").sort("net_rtg", descending=True).head()
    """
    return mbb_situational_team(seasons, league="womens", top_n=top_n, return_as_pandas=return_as_pandas)


def wbb_situational_player(
    seasons: int | list[int], *, top_n: int = 25, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Women's college player on-floor splits by situation.

    Delegates to :func:`sportsdataverse.mbb.mbb_situational.mbb_situational_player` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        top_n: As :func:`wbb_situational_team`.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id, family, split)`` with the columns of
        :func:`sportsdataverse.nba.nba_situational.nba_situational_player` (String player ids).

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_situational_player
            wbb_situational_player(2026).filter(split="clutch").sort("net_rtg_n", descending=True).head()
    """
    return mbb_situational_player(seasons, league="womens", top_n=top_n, return_as_pandas=return_as_pandas)


def wbb_situational_lineup(
    seasons: int | list[int], *, top_n: int = 25, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Women's college five-man unit splits by situation.

    Delegates to :func:`sportsdataverse.mbb.mbb_situational.mbb_situational_lineup` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        top_n: As :func:`wbb_situational_team`.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, lineup_id, family, split)`` with the columns of
        :func:`sportsdataverse.nba.nba_situational.nba_situational_lineup` (String player ids).

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_situational_lineup
            wbb_situational_lineup(2026).filter(split="transition").sort("poss", descending=True).head()
    """
    return mbb_situational_lineup(seasons, league="womens", top_n=top_n, return_as_pandas=return_as_pandas)
