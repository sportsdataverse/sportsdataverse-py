"""Women's college basketball bracketology.

Thin shim over :mod:`sportsdataverse.mbb.mbb_bracketology` -- the résumé
blend, field selection, and seeding are league-agnostic; the WBB loaders and
women's constants are selected via ``league="womens"``. The frame-level core
is re-exported **by reference**.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_bracketology import wbb_bracketology
        field = wbb_bracketology(2024)

See Also:
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
"""

from __future__ import annotations

import datetime
from typing import Union

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_bracketology import mbb_bracketology, project_bracket

__all__ = [
    "project_bracket",
    "wbb_bracketology",
]


def wbb_bracketology(
    season: int,
    *,
    as_of_date: Union[datetime.date, None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Women's projected tournament field for a season.

    Delegates to
    :func:`sportsdataverse.mbb.mbb_bracketology.mbb_bracketology` with
    ``league="womens"`` (WBB loaders + women's constants).

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_bracketology
            field = wbb_bracketology(2024)
    """
    if return_as_pandas:
        return mbb_bracketology(season, as_of_date=as_of_date, league="womens", return_as_pandas=True)
    return mbb_bracketology(season, as_of_date=as_of_date, league="womens")
