"""Women's college team strength (BB-D19): ``wbb.team_strength``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_team_strength`: ``league="womens"`` switches to the
``wbb_ratings`` release, the ``ncaa_wbb_*`` releases and the WBB schedule.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_team_strength import wbb_team_strength
        strength = wbb_team_strength(2025)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_team_strength import mbb_team_strength

__all__ = ["wbb_team_strength"]


def wbb_team_strength(
    seasons: int | list[int],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college team strength: adjusted ratings, SOS, SOV, tempo and the four factors.

    Delegates to :func:`sportsdataverse.mbb.mbb_team_strength.mbb_team_strength` with
    ``league="womens"``. The adjusted ratings are the published ``wbb_ratings``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)``: ``games``, ``wins``, ``losses``, ``poss``,
        ``opp_poss``, ``pts``, ``opp_pts``, the adjusted ratings, ``sos``, ``sov``, the BB-D3
        rates and their ``_rank`` / ``_pct`` / ``_n``. See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_team_strength
            wbb_team_strength(2025).sort("adj_net_rtg_rank").head()
    """
    return mbb_team_strength(seasons, league="womens", return_as_pandas=return_as_pandas)
