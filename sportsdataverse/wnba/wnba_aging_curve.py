"""WNBA aging curve / career trajectory -- by-reference shim over the NBA core.

The delta-method algorithm is league-agnostic and lives once in
:mod:`sportsdataverse.nba.nba_aging_curve`; this module binds it to
``league="wnba"`` and the bundled women's artifact
(``sportsdataverse/nba/models/wnba_aging_curve.json``).
"""

from __future__ import annotations

from functools import partial

import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_aging_curve import build_aging_deltas, nba_aging_curve, nba_career_trajectory

__all__ = ["build_aging_deltas", "wnba_aging_curve", "wnba_career_trajectory"]


def wnba_aging_curve(*, return_as_pandas: bool = False) -> "pl.DataFrame | pd.DataFrame":
    """WNBA aging curve -- the NBA core bound to ``league="wnba"``.

    See :func:`sportsdataverse.nba.nba_aging_curve.nba_aging_curve` for the
    full contract; this is a by-reference re-export, same algorithm, women's
    bundled artifact.

    Returns:
        Frame ``age:Int64, rel_value:Float64, peak_age:Float64``.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_aging_curve
            curve = wnba_aging_curve()
    """
    return nba_aging_curve(league="wnba", return_as_pandas=return_as_pandas)  # type: ignore[call-overload,no-any-return]


def wnba_career_trajectory(
    player_values: pl.DataFrame, *, return_as_pandas: bool = False
) -> "pl.DataFrame | pd.DataFrame":
    """WNBA career trajectory -- the NBA core bound to ``league="wnba"``.

    See :func:`sportsdataverse.nba.nba_aging_curve.nba_career_trajectory` for
    the full contract.

    Args:
        player_values: Frame ``player_id, age:Int64, value:Float64``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``player_values`` plus ``age_adjusted_value`` and ``proj_next_value``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.wnba import wnba_career_trajectory
            player_values = pl.DataFrame({"player_id": ["1"], "age": [26], "value": [10.0]})
            wnba_career_trajectory(player_values)
    """
    return nba_career_trajectory(player_values, league="wnba", return_as_pandas=return_as_pandas)  # type: ignore[call-overload,no-any-return]


wnba_aging_curve.__wrapped_core__ = partial(nba_aging_curve, league="wnba")  # type: ignore[attr-defined]
wnba_career_trajectory.__wrapped_core__ = partial(nba_career_trajectory, league="wnba")  # type: ignore[attr-defined]
