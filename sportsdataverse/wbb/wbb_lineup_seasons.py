"""Women's college lineup seasons (BB-D4): ``wbb.lineup_seasons``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_lineup_seasons`: ``league="womens"`` switches to the
``ncaa_wbb_pbp`` / ``ncaa_wbb_possessions`` releases.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_lineup_seasons import wbb_lineup_seasons
        units = wbb_lineup_seasons(2025)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_lineup_seasons import MIN_POSS, mbb_lineup_seasons

__all__ = ["wbb_lineup_seasons"]


def wbb_lineup_seasons(
    seasons: int | list[int], *, min_poss: int = MIN_POSS, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Women's college lineup seasons: 2-, 3- and 5-man units with ratings, four factors and shot zones.

    Delegates to :func:`sportsdataverse.mbb.mbb_lineup_seasons.mbb_lineup_seasons` with
    ``league="womens"``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        min_poss: The rank gate, offensive plus defensive possessions (default 200).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, n_players, lineup_id)``: ``player_1..5``, ``minutes``,
        ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``, ``plus_minus``, the ratings, the four
        factors and shot zones for and against, each with ``_rank`` / ``_pct`` / ``_n``. See the
        mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_lineup_seasons
            wbb_lineup_seasons(2025).filter(n_players=5).sort("poss").tail()
    """
    return mbb_lineup_seasons(seasons, league="womens", min_poss=min_poss, return_as_pandas=return_as_pandas)
