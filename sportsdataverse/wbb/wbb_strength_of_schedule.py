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

import datetime
from collections.abc import Iterable
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
    as_of_date: Union[datetime.date, None] = None,
    ineligible: Iterable[str] = (),
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Women's daily ``strength_of_schedule`` table: records, SoS, quad résumé, WAB, RPI and a NET estimate.

    Delegates to :func:`sportsdataverse.mbb.mbb_strength_of_schedule.mbb_strength_of_schedule` with ``league="womens"`` (WBB loaders, women's constants and quad ranges; RPI location weights from 2011-12).

    Args:
        seasons: Seasons to compute (e.g. ``[2024]``).
        as_of_date: Only use games strictly before this date; ``None`` uses every
            completed game.
        ineligible: Team ids barred from the NCAA tournament (sets ``eligible``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per Division I (season, team_id) -- see the mbb core for the full
        contract.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_strength_of_schedule
            wbb_strength_of_schedule([2024]).sort("rpi_rank").head(20)
    """
    if return_as_pandas:
        return mbb_strength_of_schedule(
            seasons, as_of_date=as_of_date, league="womens", ineligible=ineligible, return_as_pandas=True
        )
    return mbb_strength_of_schedule(seasons, as_of_date=as_of_date, league="womens", ineligible=ineligible)
