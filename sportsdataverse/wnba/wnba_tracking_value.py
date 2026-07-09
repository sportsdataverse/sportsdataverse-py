"""wnba_tracking_value -- WNBA tracking-value models (by-reference over the nba core, league_id="10").

Thin shim over :mod:`sportsdataverse.nba.nba_tracking_value` -- the six
over-expected models are one league-agnostic core switched by ``league_id``,
so each ``wnba_tracking_*`` function binds ``league_id="10"`` and delegates.
G-League needs no shim -- call the nba core function with ``league_id="20"``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional, Union

import polars as pl

from sportsdataverse.nba import nba_tracking_value as _core

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "wnba_tracking_reb_oe",
    "wnba_tracking_pass_value",
    "wnba_tracking_drive_value",
    "wnba_tracking_shot_diet_value",
    "wnba_tracking_touch_value",
    "wnba_tracking_rim_protect_value",
]


def wnba_tracking_reb_oe(
    seasons: "int | str | list",
    *,
    league_id: str = "10",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA rebounding-over-expected (``league_id="10"``); see
    :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_reb_oe`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_tracking_reb_oe
            df = wnba_tracking_reb_oe(2024)
            print(df.sort("reb_oe", descending=True).head())
    """
    return _core.nba_tracking_reb_oe(
        seasons,
        league_id=league_id,
        per_mode=per_mode,
        by_position=by_position,
        positions=positions,
        return_as_pandas=return_as_pandas,
        _get_fn=_get_fn,
    )


def wnba_tracking_pass_value(
    seasons: "int | str | list",
    *,
    league_id: str = "10",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    fetch_potential_assists: bool = False,
    max_players: int = 0,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
    _pass_get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA expected-assists / passer value (``league_id="10"``); see
    :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_pass_value`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_tracking_pass_value
            df = wnba_tracking_pass_value(2024)
            print(df.sort("ast_oe", descending=True).head())
    """
    return _core.nba_tracking_pass_value(
        seasons,
        league_id=league_id,
        per_mode=per_mode,
        by_position=by_position,
        positions=positions,
        fetch_potential_assists=fetch_potential_assists,
        max_players=max_players,
        return_as_pandas=return_as_pandas,
        _get_fn=_get_fn,
        _pass_get_fn=_pass_get_fn,
    )


def wnba_tracking_drive_value(
    seasons: "int | str | list",
    *,
    league_id: str = "10",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA drive value + rim-pressure (``league_id="10"``); see
    :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_drive_value`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_tracking_drive_value
            df = wnba_tracking_drive_value(2024)
            print(df.sort("drive_pts_oe", descending=True).head())
    """
    return _core.nba_tracking_drive_value(
        seasons,
        league_id=league_id,
        per_mode=per_mode,
        by_position=by_position,
        positions=positions,
        return_as_pandas=return_as_pandas,
        _get_fn=_get_fn,
    )


def wnba_tracking_shot_diet_value(
    seasons: "int | str | list",
    *,
    league_id: str = "10",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA catch-&-shoot vs pull-up points-over-expected (``league_id="10"``);
    see :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_shot_diet_value`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_tracking_shot_diet_value
            df = wnba_tracking_shot_diet_value(2024)
            print(df.sort("cs_pts_oe", descending=True).head())
    """
    return _core.nba_tracking_shot_diet_value(
        seasons,
        league_id=league_id,
        per_mode=per_mode,
        by_position=by_position,
        positions=positions,
        return_as_pandas=return_as_pandas,
        _get_fn=_get_fn,
    )


def wnba_tracking_touch_value(
    seasons: "int | str | list",
    *,
    league_id: str = "10",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA touch / possession-time value (``league_id="10"``); see
    :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_touch_value`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_tracking_touch_value
            df = wnba_tracking_touch_value(2024)
            print(df.sort("pts_per_touch_oe", descending=True).head())
    """
    return _core.nba_tracking_touch_value(
        seasons,
        league_id=league_id,
        per_mode=per_mode,
        by_position=by_position,
        positions=positions,
        return_as_pandas=return_as_pandas,
        _get_fn=_get_fn,
    )


def wnba_tracking_rim_protect_value(
    seasons: "int | str | list",
    *,
    league_id: str = "10",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    source: str = "leaguedash",
    max_players: int = 0,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
    _defend_get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA rim-protection / shot-defend points-saved (``league_id="10"``); see
    :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_rim_protect_value`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_tracking_rim_protect_value
            df = wnba_tracking_rim_protect_value(2024)
            print(df.sort("rim_protect_pts_saved", descending=True).head())
    """
    return _core.nba_tracking_rim_protect_value(
        seasons,
        league_id=league_id,
        per_mode=per_mode,
        by_position=by_position,
        positions=positions,
        source=source,
        max_players=max_players,
        return_as_pandas=return_as_pandas,
        _get_fn=_get_fn,
        _defend_get_fn=_defend_get_fn,
    )
