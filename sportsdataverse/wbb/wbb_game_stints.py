"""Women's college game stints, in-game units and on/off (BB-D32): ``wbb.game_stints`` / ``wbb.game_units``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_game_stints`: ``league="womens"`` switches to the
``ncaa_wbb_pbp`` / ``ncaa_wbb_possessions`` releases.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_game_stints import wbb_game_stints
        stints = wbb_game_stints(2025)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_game_stints import (
    mbb_game_on_off,
    mbb_game_player_box_splits,
    mbb_game_stints,
    mbb_game_units,
)

__all__ = ["wbb_game_on_off", "wbb_game_player_box_splits", "wbb_game_stints", "wbb_game_units"]


def wbb_game_stints(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's college game stints: per player and game, every stretch on the floor.

    Delegates to :func:`sportsdataverse.mbb.mbb_game_stints.mbb_game_stints` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id, stint_number)``: ``opp_team_id``,
        ``period``, ``clock_in``, ``clock_out``, ``seconds``, ``minutes``, ``poss``, ``opp_poss``,
        ``pts``, ``opp_pts`` and ``plus_minus``. See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_game_stints
            wbb_game_stints(2025).head()
    """
    return mbb_game_stints(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_game_units(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's college in-game units: per 1- to 5-man set of teammates and game, ratings, four factors and shot zones.

    Delegates to :func:`sportsdataverse.mbb.mbb_game_stints.mbb_game_units` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, n_players, lineup_id)``: ``player_1..5``, the
        totals, ratings, four factors, shot zones and ``<m>_n``. See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_game_units
            wbb_game_units(2025).filter(n_players=2).head()
    """
    return mbb_game_units(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_game_on_off(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's college in-game on/off: per player and game, the team with her on the floor and without her.

    Delegates to :func:`sportsdataverse.mbb.mbb_game_stints.mbb_game_on_off` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id)``: the ``_on`` and ``_off`` totals and
        ratings, ``<rating>_diff`` and their ``_n``. See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_game_on_off
            wbb_game_on_off(2025).head()
    """
    return mbb_game_on_off(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_game_player_box_splits(
    seasons: int | list[int], *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Women's college player box by game, period and garbage time.

    Delegates to :func:`sportsdataverse.mbb.mbb_game_stints.mbb_game_player_box_splits` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        Per ``(season, game_id, team_id, player_id)``: the ``all``, ``period`` (with
        ``on_floor_at_end``) and ``garbage`` rows, each with minutes and the box counts. See the
        mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_game_player_box_splits
            wbb_game_player_box_splits(2025).filter(family="period").head()
    """
    return mbb_game_player_box_splits(seasons, league="womens", return_as_pandas=return_as_pandas)
