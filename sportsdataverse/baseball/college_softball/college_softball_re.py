"""College softball RE24/WPA -- by-reference shim over :mod:`sportsdataverse.baseball.college_run_expectancy`.

Fixes ``league="college_softball"`` (7-inning regulation, via
:mod:`sportsdataverse.baseball.college_baseball_constants`) on the shared,
league-agnostic core so callers don't have to pass it. No math lives here --
see the core module (and, one level further up the reuse chain,
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

__all__ = ["college_softball_state", "college_softball_re24", "college_softball_wpa"]


def college_softball_state(plays: Dict[str, Any]) -> pl.DataFrame:
    """:func:`sportsdataverse.baseball.college_run_expectancy.college_baseball_state` fixed to ``league="college_softball"``.

    Args:
        plays: Raw payload from ``espn_college_softball_game_plays(event_id, return_parsed=False)``.

    Returns:
        pl.DataFrame: see the core function's Returns table.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_softball.college_softball_re import college_softball_state
            state = college_softball_state(raw)
    """
    return _core.college_baseball_state(plays, league="college_softball")


def college_softball_re24(
    seasons: Union[int, List[int], None] = None,
    *,
    state: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """:func:`sportsdataverse.baseball.college_run_expectancy.college_baseball_re24` fixed to ``league="college_softball"``.

    Args:
        seasons: See the core function.
        state: See the core function.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: see the core function's Returns table.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_softball.college_softball_re import college_softball_state, college_softball_re24
            state = college_softball_state(raw)
            matrix = college_softball_re24(state=state)
    """
    return _core.college_baseball_re24(
        seasons, league="college_softball", state=state, return_as_pandas=return_as_pandas
    )


def college_softball_wpa(
    seasons: Union[int, List[int], None] = None,
    *,
    state: Optional[pl.DataFrame] = None,
    results: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """:func:`sportsdataverse.baseball.college_run_expectancy.college_baseball_wpa` fixed to ``league="college_softball"``.

    Args:
        seasons: See the core function.
        state: See the core function.
        results: See the core function.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: see the core function's Returns table.

    Example:
        Quick start::

            from sportsdataverse.baseball.college_softball.college_softball_re import college_softball_wpa
            wpa = college_softball_wpa(state=state, results=results)
    """
    return _core.college_baseball_wpa(
        seasons, league="college_softball", state=state, results=results, return_as_pandas=return_as_pandas
    )
