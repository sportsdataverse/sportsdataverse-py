"""Women's college basketball season win-probability compile helper.

Thin shim over :mod:`sportsdataverse.mbb.mbb_win_prob` -- the pregame anchor,
in-game scorer, and metadata assembly are league-agnostic; the WBB loaders and
women's constants are selected via ``league="womens"``.

Example:
    Quick start::

        from sportsdataverse.wbb import build_wbb_season_wp
        wp = build_wbb_season_wp(2024)

See Also:
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Union, overload

import polars as pl

from sportsdataverse.mbb.mbb_win_prob import build_mbb_season_wp

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["build_wbb_season_wp"]


@overload
def build_wbb_season_wp(season: int, *, return_as_pandas: Literal[False] = False) -> pl.DataFrame: ...


@overload
def build_wbb_season_wp(season: int, *, return_as_pandas: Literal[True]) -> "pd.DataFrame": ...


def build_wbb_season_wp(season: int, *, return_as_pandas: bool = False) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """A WBB season's play-by-play with win-probability columns joined in.

    Delegates to :func:`sportsdataverse.mbb.mbb_win_prob.build_mbb_season_wp`
    with ``league="womens"`` (WBB loaders + women's constants).

    Args:
        season: Season year (e.g. ``2024``); bounded by ``load_wbb_pbp`` release
            availability.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The season's ``load_wbb_pbp`` frame with ``pregame_home_prob`` +
        ``home_win_prob`` appended -- see the mbb core for the contract.

    Example:
        Quick start::

            from sportsdataverse.wbb import build_wbb_season_wp
            wp = build_wbb_season_wp(2024)
    """
    if return_as_pandas:
        return build_mbb_season_wp(season, league="womens", return_as_pandas=True)
    return build_mbb_season_wp(season, league="womens")
