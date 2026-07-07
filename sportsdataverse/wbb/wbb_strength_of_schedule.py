"""Women's college basketball SoS / Quad résumé / Wins Above Bubble.

Thin shim over :mod:`sportsdataverse.mbb.mbb_strength_of_schedule` -- the
résumé math is league-agnostic (quad thresholds, HFA, and the bubble AdjEM
come from ``LEAGUE_CONSTANTS["womens"]``); the frame-level core is
re-exported **by reference**.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_strength_of_schedule import wbb_strength_of_schedule
        resume = wbb_strength_of_schedule([2024])

See Also:
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
"""

from __future__ import annotations

from typing import Union

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_strength_of_schedule import (
    mbb_strength_of_schedule,
    strength_of_schedule,
)

__all__ = [
    "strength_of_schedule",
    "wbb_strength_of_schedule",
]


def wbb_strength_of_schedule(
    seasons: list[int],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Women's season-level SoS / Quad / WAB résumé.

    Delegates to :func:`sportsdataverse.mbb.mbb_strength_of_schedule.mbb_strength_of_schedule` with ``league="womens"`` (WBB loaders + women's constants).

    Args:
        seasons: Seasons to compute (e.g. ``[2024]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per (season, team_id): ``season, team_id, sos, sos_rank, wab,
        quad1_w .. quad4_l, quality_wins`` -- see the mbb core for the full
        contract.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_strength_of_schedule
            wbb_strength_of_schedule([2024]).sort("wab", descending=True).head(20)
    """
    if return_as_pandas:
        return mbb_strength_of_schedule(seasons, league="womens", return_as_pandas=True)
    return mbb_strength_of_schedule(seasons, league="womens")
