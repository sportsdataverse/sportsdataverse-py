"""College baseball RE24/WPA -- by-reference shim over :mod:`sportsdataverse.baseball.college_run_expectancy`.

Fixes ``league="college_baseball"`` on the shared, league-agnostic core so
callers don't have to pass it. No math lives here -- see the core module
(and, one level further up the reuse chain,
:mod:`sportsdataverse.mlb.mlb_run_expectancy` / ``mlb_win_expectancy``, T6.4)
for the actual RE24/WPA implementation.

See Also:
    * `baseballr`_ -- R sibling package for MLB/college sabermetrics.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.baseball import college_run_expectancy as _core

__all__ = ["college_baseball_state", "college_baseball_re24", "college_baseball_wpa"]


def college_baseball_state(plays: Dict[str, Any]) -> pl.DataFrame:
    """:func:`sportsdataverse.baseball.college_run_expectancy.college_baseball_state` fixed to ``league="college_baseball"``.

    Args:
        plays: Raw payload from ``espn_college_baseball_game_plays(event_id, return_parsed=False)``.

    Returns:
        pl.DataFrame: see the core function's Returns table.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_baseball.college_baseball_re import college_baseball_state
            state = college_baseball_state(raw)
    """
    return _core.college_baseball_state(plays, league="college_baseball")


def college_baseball_re24(
    seasons: Union[int, List[int], None] = None,
    *,
    state: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """:func:`sportsdataverse.baseball.college_run_expectancy.college_baseball_re24` fixed to ``league="college_baseball"``.

    Args:
        seasons: See the core function.
        state: See the core function.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: see the core function's Returns table.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_baseball.college_baseball_re import college_baseball_state, college_baseball_re24
            state = college_baseball_state(raw)
            matrix = college_baseball_re24(state=state)
    """
    return _core.college_baseball_re24(
        seasons, league="college_baseball", state=state, return_as_pandas=return_as_pandas
    )


def college_baseball_wpa(
    seasons: Union[int, List[int], None] = None,
    *,
    state: Optional[pl.DataFrame] = None,
    results: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """:func:`sportsdataverse.baseball.college_run_expectancy.college_baseball_wpa` fixed to ``league="college_baseball"``.

    Args:
        seasons: See the core function.
        state: See the core function.
        results: See the core function.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: see the core function's Returns table.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_baseball.college_baseball_re import college_baseball_wpa
            wpa = college_baseball_wpa(state=state, results=results)
    """
    return _core.college_baseball_wpa(
        seasons, league="college_baseball", state=state, results=results, return_as_pandas=return_as_pandas
    )
