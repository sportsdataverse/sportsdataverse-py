"""Women's college tournament runs (BB-D43 for WBB): ``wbb.tournament_run_games`` / ``_teams`` / ``_players``.

Thin shims over :mod:`sportsdataverse.mbb.mbb_tournament_runs`: ``league="womens"`` switches to
the WBB ESPN schedule and box score releases.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_tournament_runs import (
    mbb_tournament_run_players,
    mbb_tournament_run_teams,
    mbb_tournament_runs,
)

__all__ = ["wbb_tournament_run_players", "wbb_tournament_run_teams", "wbb_tournament_runs"]


def wbb_tournament_runs(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's tournament games, once per side, keyed to each team's run (NCAA, WNIT, WBIT, conference ...).

    Delegates to :func:`sportsdataverse.mbb.mbb_tournament_runs.mbb_tournament_runs` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        Two rows per tournament game, as the mbb function: the run keys, the tournament's name and
        kind, the round, the score, the run's record after the game and ``run_result``.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_tournament_runs
            runs = wbb_tournament_runs(2025)
            runs.filter(runs["run_result"] == "champion")
    """
    return mbb_tournament_runs(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_tournament_run_teams(
    seasons: int | list[int], *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Each women's run's record and box totals. Delegates to ``mbb_tournament_run_teams(league="womens")``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per run with a final game: the run's tournament name and kind, the W-L, points for
        and against and the summed counts.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_tournament_run_teams
            wbb_tournament_run_teams(2025).head()
    """
    return mbb_tournament_run_teams(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_tournament_run_players(
    seasons: int | list[int], *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Each women's player's run line against the regular season. Delegates to ``mbb_tournament_run_players``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per player and run: totals, per-game lines, ``ts_pct``, the regular season's and the shift.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_tournament_run_players
            wbb_tournament_run_players(2025).sort("pts", descending=True).head()
    """
    return mbb_tournament_run_players(seasons, league="womens", return_as_pandas=return_as_pandas)
