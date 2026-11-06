"""Women's college WAR and VORP (BB-D26): ``wbb.player_war``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_player_war`: ``league="womens"`` switches to the
women's box BPM artifact, the women's team ratings and the WBB replacement level and points per
win.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_player_war import wbb_player_war
        war = wbb_player_war(2025)
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_player_war import mbb_player_war

__all__ = ["wbb_player_war"]


def wbb_player_war(
    seasons: int | list[int],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Women's college WAR and VORP per player-season, from our box BPM.

    Delegates to :func:`sportsdataverse.mbb.mbb_player_war.mbb_player_war` with
    ``league="womens"``. Box BPM covers every D1 team from 2017 (about 80 teams in 2014-2016).

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, player_id)`` of a D1 team: ``player_name``, ``team_id``,
        ``teams``, ``min``, ``poss``, ``rating``, ``vorp``, ``war`` and the ``_rank`` / ``_pct`` /
        ``_n`` columns. See the mbb core for the definitions.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_player_war
            wbb_player_war(2025).select("player_name", "rating", "war").head()
    """
    return mbb_player_war(seasons, league="womens", return_as_pandas=return_as_pandas)
