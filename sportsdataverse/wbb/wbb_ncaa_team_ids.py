"""Women's college basketball NCAA team-id crosswalk (wbigballR port).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_team_ids` with
``league="wbb"`` bound. The bundled WBB table
(``sportsdataverse/wbb/data/ncaa_teamids_wbb.csv``) is the port of
wbigballR's ``teamids`` data asset.

Name→id resolution: use the shared
:func:`~sportsdataverse.mbb.mbb_ncaa_team_ids.resolve_ncaa_team_id`
(re-exported here) with ``league="wbb"`` — e.g.
``resolve_ncaa_team_id("South Carolina", "2024-25", league="wbb")``.
Passing the league explicitly is the deliberate fix of wbigballR, which
always searched the men's table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_team_ids import _ncaa_bb_team_ids, resolve_ncaa_team_id

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = [
    "ncaa_wbb_team_ids",
    "resolve_ncaa_team_id",
]


def ncaa_wbb_team_ids(*, return_as_pandas: bool = False) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Women's-basketball ``(team, season) -> stats.ncaa.org id`` crosswalk.

    Port of wbigballR's bundled ``teamids`` data asset (one row per team per
    season). Algorithm detail:
    :func:`sportsdataverse.mbb.mbb_ncaa_team_ids.ncaa_mbb_team_ids`.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        DataFrame with columns ``team`` (str), ``conference`` (str),
        ``id`` (Int64 — the season-specific stats.ncaa.org team id) and
        ``season`` (str, ``"YYYY-YY"``).

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_team_ids import ncaa_wbb_team_ids
            df = ncaa_wbb_team_ids()
            print(df.shape)
    """
    df = _ncaa_bb_team_ids("wbb")
    return df.to_pandas() if return_as_pandas else df
