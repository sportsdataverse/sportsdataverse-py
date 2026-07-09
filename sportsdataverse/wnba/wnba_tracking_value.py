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
    """WNBA rebounding-over-expected (``league_id="10"`` by-reference shim).

    See :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_reb_oe`
    for the full recipe (contest-difficulty-adjusted expected rebounds,
    role-bucket baseline).

    Args:
        seasons: A single season or list of seasons.
        league_id: Defaults to ``"10"`` (WNBA); pass ``"20"`` here for G-League.
        per_mode: ``per_mode_simple`` passed to the fetch (default ``"Totals"``).
        by_position: Compute the baseline within role buckets (default);
            ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``.

    Returns:
        One row per player-season:
        ``season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8,
        position_bucket:Utf8, gp:Int64, min:Float64, reb:Float64,
        reb_chances:Float64, reb_baseline_rate:Float64, reb_expected:Float64,
        reb_oe:Float64, reb_oe_per_36:Float64, oreb_oe:Float64, dreb_oe:Float64,
        league_id:Utf8``. Empty/malformed input returns a zero-row frame with
        this schema.

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
    """WNBA expected-assists / passer value (``league_id="10"`` by-reference shim).

    See :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_pass_value`
    for the full recipe (Passing-measure proxy + optional ``playerdashptpass``
    enrichment).

    Args:
        seasons: A single season or list of seasons.
        league_id: Defaults to ``"10"`` (WNBA); pass ``"20"`` here for G-League.
        per_mode: ``per_mode_simple`` passed to the fetch (default ``"Totals"``).
        by_position: Compute the baseline within role buckets (default);
            ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame.
        fetch_potential_assists: Enrich the top passers with
            ``playerdashptpass`` potential-assist counts.
        max_players: Cap on per-player enrichment fetches; ``0`` disables
            enrichment regardless of ``fetch_potential_assists``.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``.
        _pass_get_fn: Injectable replacement for ``nba_stats_playerdashptpass``.

    Returns:
        One row per player-season:
        ``season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8,
        position_bucket:Utf8, gp:Int64, min:Float64, ast:Float64,
        passes:Float64, ast_baseline_rate:Float64, ast_expected:Float64,
        ast_oe:Float64, ast_oe_per_36:Float64, ast_pts_created:Float64,
        league_id:Utf8``. Empty/malformed input returns a zero-row frame with
        this schema.

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
    """WNBA drive value + rim-pressure (``league_id="10"`` by-reference shim).

    See :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_drive_value`
    for the full recipe.

    Args:
        seasons: A single season or list of seasons.
        league_id: Defaults to ``"10"`` (WNBA); pass ``"20"`` here for G-League.
        per_mode: ``per_mode_simple`` passed to the fetch (default ``"Totals"``).
        by_position: Compute the baseline within role buckets (default);
            ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``.

    Returns:
        One row per player-season:
        ``season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8,
        position_bucket:Utf8, gp:Int64, min:Float64, drives:Float64,
        drive_pts:Float64, drive_baseline_rate:Float64, drive_expected:Float64,
        drive_pts_oe:Float64, drive_pts_oe_per_36:Float64, drive_fta:Float64,
        rim_pressure:Float64, drive_ast:Float64, drive_tov:Float64,
        league_id:Utf8``. Empty/malformed input returns a zero-row frame with
        this schema.

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
    """WNBA catch-&-shoot vs pull-up points-over-expected (``league_id="10"``
    by-reference shim).

    See :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_shot_diet_value`
    for the full recipe.

    Args:
        seasons: A single season or list of seasons.
        league_id: Defaults to ``"10"`` (WNBA); pass ``"20"`` here for G-League.
        per_mode: ``per_mode_simple`` passed to each fetch (default ``"Totals"``).
        by_position: Compute each measure's baseline within role buckets
            (default); ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``.

    Returns:
        One row per player-season:
        ``season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8,
        position_bucket:Utf8, cs_fga:Float64, cs_pts:Float64,
        cs_pts_oe:Float64, pu_fga:Float64, pu_pts:Float64, pu_pts_oe:Float64,
        shot_diet_delta:Float64, league_id:Utf8``. Empty/malformed input
        returns a zero-row frame with this schema.

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
    """WNBA touch / possession-time value (``league_id="10"`` by-reference shim).

    See :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_touch_value`
    for the full recipe.

    Args:
        seasons: A single season or list of seasons.
        league_id: Defaults to ``"10"`` (WNBA); pass ``"20"`` here for G-League.
        per_mode: ``per_mode_simple`` passed to the fetch (default ``"Totals"``).
        by_position: Compute the baseline within role buckets (default);
            ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``.

    Returns:
        One row per player-season:
        ``season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8,
        position_bucket:Utf8, gp:Int64, min:Float64, touches:Float64,
        pts:Float64, touch_baseline_rate:Float64, touch_expected:Float64,
        pts_per_touch_oe:Float64, time_of_poss:Float64,
        time_of_poss_eff:Float64, league_id:Utf8``. Empty/malformed input
        returns a zero-row frame with this schema.

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
    """WNBA rim-protection / shot-defend points-saved (``league_id="10"``
    by-reference shim).

    See :func:`sportsdataverse.nba.nba_tracking_value.nba_tracking_rim_protect_value`
    for the full recipe (bucket-mean defended-rate baseline; optional
    ``playerdashptshotdefend`` rim-band enrichment).

    Args:
        seasons: A single season or list of seasons.
        league_id: Defaults to ``"10"`` (WNBA); pass ``"20"`` here for G-League.
        per_mode: ``per_mode_simple`` passed to the fetch (default ``"Totals"``).
        by_position: Compute the baseline within role buckets (default);
            ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame.
        source: ``"leaguedash"`` (default) or ``"shotdefend"``.
        max_players: Cap on per-player ``shotdefend`` enrichment fetches;
            ignored unless ``source="shotdefend"``.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``.
        _defend_get_fn: Injectable replacement for
            ``nba_stats_playerdashptshotdefend``.

    Returns:
        One row per player-season:
        ``season:Int64, player_id:Utf8, player_name:Utf8, team_id:Utf8,
        position_bucket:Utf8, gp:Int64, min:Float64, d_fga:Float64,
        d_fgm:Float64, d_fg_pct:Float64, normal_fg_pct:Float64,
        rim_protect_pts_saved:Float64, rim_protect_pts_saved_per_36:Float64,
        source:Utf8, league_id:Utf8``. Empty/malformed input returns a
        zero-row frame with this schema.

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
