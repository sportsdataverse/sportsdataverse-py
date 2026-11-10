"""College game stints, in-game units and on/off (BB-D32): ``mbb.game_stints`` / ``mbb.game_units`` (and wbb).

Builds the :mod:`sportsdataverse._common.game_stints` tables from the two stats.ncaa.org releases
the college game impact box reads (:mod:`sportsdataverse.mbb.mbb_game_impact`):

* the **stints** walk the ``ncaa_{mbb,wbb}_pbp`` events in release (game) order on each event's
  lineup; the period clock is the period's end less the event's ``game_seconds`` (0 on its last
  event, which takes the period's tail), and each ``ncaa_{mbb,wbb}_possessions`` row is counted on
  the first event of its ``poss_num``, whose lineup is the possession's on every fixture possession;
* the **units** and **on/off** roll up the game impact rows, with the four-factor and shot-zone
  counts of each event (:func:`sportsdataverse.mbb.mbb_lineup_seasons.ncaa_lineup_count_rows`: a
  rim attempt is a dunk, layup, hook or tip-in).

``game_id`` is the stats.ncaa.org ``contest_id``, ``team_id`` the ESPN team id (Int64) and
``player_id`` the stats.ncaa.org player id (a String). A non-D1 opponent has no ESPN team id, so
its players get no row; its points still count against the D1 team.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_stints import (
    credit_free_throws_to_foul_row,
    game_on_off,
    game_stints,
    game_units,
    stint_row_schema,
)
from sportsdataverse.mbb.mbb_game_impact import _check_league, _events, _load_releases, ncaa_impact_rows
from sportsdataverse.mbb.mbb_lineup_seasons import ncaa_lineup_count_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_game_on_off", "mbb_game_stints", "mbb_game_units", "ncaa_stint_rows"]

_PLAYER = pl.Utf8()
_ROWS = stint_row_schema(_PLAYER)


def ncaa_stint_rows(pbp: pl.DataFrame, possessions: pl.DataFrame, *, league: str = "mens") -> pl.DataFrame:
    """Map the stats.ncaa.org pbp and possessions onto the stint-row frame.

    One row per event in game order, on the event's lineup: its clock, its points (a free throw's
    on the row of the game's last foul) and the possessions that start at it. A possession starts
    at the first event of its ``poss_num``; one whose ``poss_team`` is neither side is dropped.

    Args:
        pbp: ``load_ncaa_mbb_pbp`` / ``load_ncaa_wbb_pbp`` rows, in release (game) order, with
            ``period``, ``game_seconds`` and ``poss_num``.
        possessions: ``load_ncaa_mbb_possessions`` / ``load_ncaa_wbb_possessions`` rows
            (``contest_id``, ``poss_num``, ``poss_team``, ``home``, ``away``).
        league: ``"mens"`` (20-minute halves) or ``"womens"`` (10-minute quarters from 2016,
            halves before); overtimes are 5 minutes.

    Returns:
        Rows in :func:`sportsdataverse._common.game_stints.stint_row_schema` with String player
        ids: ``period``, ``clock`` (seconds left in the period after the event) and ``order`` (the
        event's row in the release).

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp, load_ncaa_mbb_possessions
            from sportsdataverse.mbb.mbb_game_stints import ncaa_stint_rows
            rows = ncaa_stint_rows(load_ncaa_mbb_pbp(2025), load_ncaa_mbb_possessions(2025))
    """
    _check_league(league)
    if pbp.height == 0:
        return pl.DataFrame(schema=_ROWS)
    events = credit_free_throws_to_foul_row(
        _events(pbp, league),
        is_free_throw=pl.col("event_type") == "Free Throw",
        is_foul=pl.col("foul_class").is_not_null(),
    )
    starts = pl.DataFrame(
        schema={"game_id": pl.Utf8(), "order": pl.Int64(), "home_poss": pl.Int64(), "away_poss": pl.Int64()}
    )
    if possessions.height:
        # _events numbers the release rows the same way, so "order" is the same key.
        first = (
            pbp.select(pl.col("contest_id").cast(pl.Utf8).alias("game_id"), "poss_num")
            .with_row_index("order")
            .group_by("game_id", "poss_num")
            .agg(pl.col("order").min().cast(pl.Int64))
        )
        home_has = pl.col("poss_team") == pl.col("home")
        kept = possessions.filter(home_has | (pl.col("poss_team") == pl.col("away"))).select(
            pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
            "poss_num",
            home_has.cast(pl.Int64).alias("home_poss"),
            (~home_has).cast(pl.Int64).alias("away_poss"),
        )
        starts = (
            kept.join(first, on=["game_id", "poss_num"], how="inner")
            .group_by("game_id", "order")
            .agg(pl.col("home_poss", "away_poss").sum())
        )
    out = events.drop("home_poss", "away_poss").join(
        starts, on=["game_id", "order"], how="left", validate="1:1", maintain_order="left"
    )
    return out.with_columns(pl.col("home_poss", "away_poss").fill_null(0)).select(list(_ROWS)).cast(_ROWS)


def mbb_game_stints(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College game stints: per player and game, every stretch on the floor with its clock, points and possessions.

    The definitions are in :mod:`sportsdataverse._common.game_stints`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_game_stints.wbb_game_stints`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id, stint_number)``: ``opp_team_id``,
        ``stint_number`` (1 = the first in the game), ``period``, ``clock_in`` / ``clock_out`` (the
        period clock, seconds left, when the stint began and ended), ``seconds`` / ``minutes``,
        ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts`` and ``plus_minus``. ``game_id`` is the NCAA
        contest id, ``team_id`` the ESPN team id (Int64), ``player_id`` the NCAA player id
        (String). Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_stints import mbb_game_stints
            stints = mbb_game_stints(2025)
    """
    out = game_stints(ncaa_stint_rows(*_load_releases(seasons, league), league=league), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def mbb_game_units(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College in-game units: per 1- to 5-man set of teammates and game, ratings, four factors and shot zones.

    The definitions are in :mod:`sportsdataverse._common.game_stints`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_game_stints.wbb_game_units`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, n_players, lineup_id)``: ``opp_team_id``,
        ``n_players`` (1 to 5), ``lineup_id`` (the NCAA player ids sorted and joined with ``-``),
        ``player_1..5``, ``minutes`` / ``seconds``, ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``,
        ``plus_minus``, ``off_rtg`` / ``def_rtg`` / ``net_rtg``, the four factors and the shot
        zones for and against (as in :func:`sportsdataverse.nba.nba_game_stints.nba_game_units`)
        and every rate's ``<m>_n``. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_stints import mbb_game_units
            units = mbb_game_units(2025)
    """
    pbp, possessions = _load_releases(seasons, league)
    counts = ncaa_lineup_count_rows(pbp)
    rows = ncaa_impact_rows(pbp, possessions, league=league)
    del pbp, possessions  # the pbp read is most of the peak memory; free it before the roll-up
    out = game_units(rows, counts, player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def mbb_game_on_off(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College in-game on/off: per player and game, the team with him on the floor and the rest of its game.

    The definitions are in :mod:`sportsdataverse._common.game_stints`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_game_stints.wbb_game_on_off`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id)``: ``opp_team_id``, for each side
        (``_on``, ``_off``) ``minutes``, ``poss``, ``opp_poss``, ``pts``, ``opp_pts``,
        ``plus_minus``, ``off_rtg``, ``def_rtg`` and ``net_rtg``; ``<rating>_diff`` (on minus off)
        and every rating's ``_n``. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_stints import mbb_game_on_off
            mbb_game_on_off(2025)
    """
    out = game_on_off(ncaa_impact_rows(*_load_releases(seasons, league), league=league), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out
