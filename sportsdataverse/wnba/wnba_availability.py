"""WNBA availability -- by-reference shim over the NBA core.

The logistic-availability algorithm is league-agnostic and lives once in
:mod:`sportsdataverse.nba.nba_availability`; this module binds it to
``league="wnba"`` (WNBA GP% uses the shorter 40-game
``games_full_season`` from ``LEAGUE_CONSTANTS["wnba"]``) and the bundled
women's artifact.
"""

from __future__ import annotations

from functools import partial

import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_availability import availability_features, nba_availability, score_availability

__all__ = ["availability_features", "score_availability", "wnba_availability"]


def wnba_availability(seasons: "int | list[int]", *, return_as_pandas: bool = False) -> "pl.DataFrame | pd.DataFrame":
    """WNBA availability -- the NBA core bound to ``league="wnba"``.

    See :func:`sportsdataverse.nba.nba_availability.nba_availability` for the
    full contract; ``avail_pct`` is availability, not skill.

    Args:
        seasons: A season (start year) or list of seasons.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Frame ``player_id:Utf8, season:Int64, avail_pct:Float64``.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_availability
            proj = wnba_availability(2023)
    """
    return nba_availability(seasons, league="wnba", return_as_pandas=return_as_pandas)  # type: ignore[call-overload,no-any-return]


wnba_availability.__wrapped_core__ = partial(nba_availability, league="wnba")  # type: ignore[attr-defined]
