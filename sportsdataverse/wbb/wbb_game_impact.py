"""Women's college game impact box (BB-D6): ``wbb.game_player_impact`` / ``wbb.game_lineups_used``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_game_impact`: ``league="womens"`` switches to the
``ncaa_wbb_pbp`` / ``ncaa_wbb_possessions`` releases.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_game_impact import wbb_game_player_impact
        box = wbb_game_player_impact(2025)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_game_impact import mbb_game_lineups_used, mbb_game_player_impact

__all__ = ["wbb_game_lineups_used", "wbb_game_player_impact"]


def wbb_game_player_impact(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's college game impact box: per player and game, minutes, possessions, +/- and ratings.

    Delegates to :func:`sportsdataverse.mbb.mbb_game_impact.mbb_game_player_impact` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id)``: ``opp_team_id``, ``minutes``,
        ``seconds``, ``poss``, ``opp_poss``, ``pts``, ``opp_pts``, ``plus_minus``, ``off_rtg``,
        ``def_rtg``, ``net_rtg`` and their ``_n``. See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_game_player_impact
            wbb_game_player_impact(2025).sort("plus_minus").tail()
    """
    return mbb_game_player_impact(seasons, league="womens", return_as_pandas=return_as_pandas)


def wbb_game_lineups_used(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Women's college lineups used: per five-man unit and game, minutes, possessions, +/- and ratings.

    Delegates to :func:`sportsdataverse.mbb.mbb_game_impact.mbb_game_lineups_used` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, lineup_id)``: ``player_1..5`` and the same totals
        and ratings as :func:`wbb_game_player_impact`. See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_game_lineups_used
            wbb_game_lineups_used(2025).head()
    """
    return mbb_game_lineups_used(seasons, league="womens", return_as_pandas=return_as_pandas)
