"""NBA / WNBA game stints, in-game units and on/off (BB-D32): ``nba.game_stints`` and ``nba.game_units``.

Builds the :mod:`sportsdataverse._common.game_stints` tables from the four stats.nba.com releases
the game impact box reads (:mod:`sportsdataverse.nba.nba_game_impact`):

* the **stints** walk the ``nba_stats_pbp`` actions in ``order_index`` order on the
  ``nba_stats_game_lineups`` lineup at each action; the clock in and out is the action's
  ``seconds_remaining``, and each ``nba_stats_possessions`` row is counted on the action at its
  ``start_order_index`` (on the 2025-26 fixture games that action's lineup is the possession's on
  99.5% of possessions, on every 2026 WNBA one);
* the **units** and **on/off** roll up the game impact rows, with the four-factor counts of each
  possession (:func:`sportsdataverse.nba.nba_lineup_seasons.nba_lineup_count_rows`) and its rim and
  mid-range twos from the pbp shots (a rim attempt is a two under 4 feet).

``league_id="10"`` reads the WNBA twins; :mod:`sportsdataverse.wnba.wnba_game_stints` binds it.
Every game of every season type is included.
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
from sportsdataverse.nba.nba_game_impact import (
    _QUARTER,
    _actions,
    _check_league,
    _load_releases,
    _possession_rows,
    nba_impact_rows,
)
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_game_on_off", "nba_game_stints", "nba_game_units", "nba_stint_rows"]

_PLAYER = pl.Int64()
_ROWS = stint_row_schema(_PLAYER)


def nba_stint_rows(
    pbp: pl.DataFrame,
    game_lineups: pl.DataFrame,
    possessions: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    league_id: str = "00",
) -> pl.DataFrame:
    """Map the stats.nba.com releases onto the stint-row frame.

    One row per play-by-play action in game order, on the lineup at that action: its clock, its
    points (a free throw's on the row of the game's last foul) and the possessions that start at
    it. Games not in ``schedule`` are dropped, and so are possessions whose offense is neither team.

    Args:
        pbp: ``load_nba_stats_pbp`` / ``load_wnba_stats_pbp`` rows (``game_id``, ``order_index``,
            ``action_number``, ``period``, ``seconds_remaining``, ``score_home``, ``score_away``,
            ``is_free_throw``, ``is_foul``, ``season``).
        game_lineups: ``load_nba_stats_game_lineups`` / ``load_wnba_stats_game_lineups`` rows.
        possessions: ``load_nba_stats_possessions`` / ``load_wnba_stats_possessions`` rows
            (``game_id``, ``offense_team_id``, ``start_order_index``).
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows
            (``game_id``, ``home_team_id``, ``away_team_id``).
        league_id: ``"00"`` NBA (12-minute quarters) or ``"10"`` WNBA (10-minute quarters).

    Returns:
        Rows in :func:`sportsdataverse._common.game_stints.stint_row_schema` with Int64 player
        ids: ``period``, ``clock`` (the action's ``seconds_remaining``) and ``order`` (its
        ``order_index``).

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_game_stints import nba_stint_rows
            rows = nba_stint_rows(L.load_nba_stats_pbp(2025), L.load_nba_stats_game_lineups(2025),
                                  L.load_nba_stats_possessions(2025), L.load_nba_stats_schedules(2025))
    """
    _check_league(league_id)
    if pbp.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema=_ROWS)
    games = schedule.select(
        pl.col("game_id").cast(pl.Utf8), pl.col("home_team_id").cast(pl.Int64), pl.col("away_team_id").cast(pl.Int64)
    ).unique("game_id")
    actions = credit_free_throws_to_foul_row(
        _actions(pbp, game_lineups, games, _QUARTER[league_id]),
        is_free_throw=pl.col("is_free_throw").eq(True),
        is_foul=pl.col("is_foul").eq(True),
    )
    starts = pl.DataFrame(
        schema={"game_id": pl.Utf8(), "order_index": pl.Int64(), "home_poss": pl.Int64(), "away_poss": pl.Int64()}
    )
    if possessions.height:
        starts = (
            _possession_rows(possessions, games, keep=("start_order_index",))
            .group_by("game_id", pl.col("start_order_index").cast(pl.Int64).alias("order_index"))
            .agg(pl.col("home_poss", "away_poss").sum())
        )
    out = actions.join(starts, on=["game_id", "order_index"], how="left", validate="1:1", maintain_order="left")
    return (
        out.with_columns(
            pl.col("home_poss", "away_poss").fill_null(0),
            pl.col("seconds_remaining").alias("clock"),
            pl.col("order_index").alias("order"),
        )
        .select(list(_ROWS))
        .cast(_ROWS)
    )


def nba_game_stints(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Game stints: per player and game, every stretch on the floor with its clock, points and possessions.

    The definitions are in :mod:`sportsdataverse._common.game_stints`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id, stint_number)``: ``opp_team_id``,
        ``stint_number`` (1 = his first in the game), ``period``, ``clock_in`` / ``clock_out`` (the
        period clock, seconds left, when the stint began and ended), ``seconds`` / ``minutes`` on
        the floor, ``poss`` / ``opp_poss`` (offensive / defensive possessions started), ``pts`` /
        ``opp_pts`` (points scored / allowed) and ``plus_minus``. Ids are the stats.nba.com ids
        (Int64; ``game_id`` a String). Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_stints import nba_game_stints
            stints = nba_game_stints(2026)
            stints.filter(game_id="0022500101")
    """
    out = game_stints(nba_stint_rows(*_load_releases(seasons, league_id), league_id=league_id), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def nba_game_units(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """In-game units: per 1- to 5-man set of teammates and game, minutes, ratings, four factors and shot zones.

    The definitions are in :mod:`sportsdataverse._common.game_stints`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, n_players, lineup_id)``: ``opp_team_id``,
        ``n_players`` (1 to 5), ``lineup_id`` (the player ids sorted and joined with ``-``),
        ``player_1..5``, ``minutes`` / ``seconds``, ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``,
        ``plus_minus``, ``off_rtg`` / ``def_rtg`` / ``net_rtg`` (per 100 possessions), the four
        factors for (``efg_pct``, ``tov_pct``, ``orb_pct``, ``ft_rate``) and against (``opp_*``),
        the shot zones for and against (``rim_rate``, ``mid_rate``, ``fg3_rate`` shares of
        field-goal attempts, ``rim_pct``, ``mid_pct``, ``fg3_pct`` make rates, ``opp_*``) as
        fractions, and every rate's ``<m>_n``. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or the possessions carry a null
            shot, free-throw, rebound or turnover count.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_stints import nba_game_units
            units = nba_game_units(2026)
            units.filter(game_id="0022500101", n_players=2).head()
    """
    pbp, game_lineups, possessions, schedule = _load_releases(seasons, league_id)
    rows = nba_impact_rows(pbp, game_lineups, possessions, schedule, league_id=league_id)
    out = game_units(rows, nba_lineup_count_rows(possessions, schedule, pbp=pbp), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def nba_game_on_off(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """In-game on/off: per player and game, the team with him on the floor and the rest of its game.

    The definitions are in :mod:`sportsdataverse._common.game_stints`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id)``: ``opp_team_id``, for each side
        (``_on``, ``_off``) ``minutes``, ``poss``, ``opp_poss``, ``pts``, ``opp_pts``,
        ``plus_minus``, ``off_rtg``, ``def_rtg`` and ``net_rtg``; ``<rating>_diff`` (on minus off)
        and every rating's ``_n``. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_stints import nba_game_on_off
            nba_game_on_off(2026).filter(game_id="0022500101")
    """
    out = game_on_off(nba_impact_rows(*_load_releases(seasons, league_id), league_id=league_id), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out
