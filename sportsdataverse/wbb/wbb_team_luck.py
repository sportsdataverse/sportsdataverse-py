"""Women's college luck and close-game record (BB-D22): ``wbb.team_luck``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_team_luck`: ``league="womens"`` switches to the
``ncaa_wbb_possessions`` release, the WBB schedule and the WBB Pythagorean exponent.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_team_luck import wbb_team_luck
        luck = wbb_team_luck(2025)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_team_luck import mbb_team_luck

__all__ = ["wbb_team_luck"]


def wbb_team_luck(
    seasons: int | list[int],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college luck and close-game record: Pythagorean expected wins vs actual, close-game W-L.

    Delegates to :func:`sportsdataverse.mbb.mbb_team_luck.mbb_team_luck` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)``: ``games``, ``wins``, ``losses``, ``pts``, ``opp_pts``,
        ``win_pct``, ``pythag_win_pct``, ``exp_wins``, ``luck_wins``, ``pbp_games``, the
        close-game record and the ``_rank`` / ``_pct`` / ``_n`` columns. See the mbb core for the
        definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_team_luck
            wbb_team_luck(2025).sort("luck_wins", descending=True).head()
    """
    return mbb_team_luck(seasons, league="womens", return_as_pandas=return_as_pandas)
