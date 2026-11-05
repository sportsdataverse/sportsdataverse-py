"""Women's college game flow and sequence stats (BB-D27): ``wbb.game_flow`` / ``wbb.team_season_flow``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_game_flow`: ``league="womens"`` switches to the
``ncaa_wbb_pbp`` / ``ncaa_wbb_possessions`` releases, the women's schedule and win-probability model.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_game_flow import wbb_game_flow
        flow = wbb_game_flow(2026)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_game_flow import mbb_game_flow, mbb_team_season_flow

__all__ = ["wbb_game_flow", "wbb_team_season_flow"]


def wbb_game_flow(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's college game flow per team and game: runs, droughts, kills, lead changes, time leading, game control.

    Delegates to :func:`sportsdataverse.mbb.mbb_game_flow.mbb_game_flow` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id)`` with the columns of
        :func:`sportsdataverse.nba.nba_game_flow.nba_game_flow`; ``game_id`` is the NCAA contest id,
        ``team_id`` the ESPN team id. See :mod:`sportsdataverse._common.game_flow` for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_game_flow
            wbb_game_flow(2026).sort("max_run").tail()
    """
    return mbb_game_flow(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_team_season_flow(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's college game flow per team and season, with ranks, percentiles and ``_n``.

    Delegates to :func:`sportsdataverse.mbb.mbb_game_flow.mbb_team_season_flow` with ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)`` with the columns of
        :func:`sportsdataverse.nba.nba_game_flow.nba_team_season_flow`.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_team_season_flow
            wbb_team_season_flow(2026).sort("kills_pg_rank").head()
    """
    return mbb_team_season_flow(seasons, league="womens", return_as_pandas=return_as_pandas)
