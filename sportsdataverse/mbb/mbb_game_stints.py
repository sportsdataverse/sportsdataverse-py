"""College game stints, in-game units and on/off (BB-D32): ``mbb.game_stints`` / ``mbb.game_units`` (and wbb).

Builds the :mod:`sportsdataverse._common.game_stints` tables from the two stats.ncaa.org releases
the college game impact box reads (:mod:`sportsdataverse.mbb.mbb_game_impact`):

* the **stints** walk the ``ncaa_{mbb,wbb}_pbp`` events in release (game) order on each event's
  lineup; the period clock is the period's end less the event's ``game_seconds`` (0 on its last
  event, which takes the period's tail), and each ``ncaa_{mbb,wbb}_possessions`` row is counted on
  the first event of its ``poss_num``, whose lineup is the possession's on every fixture possession;
* the **units** and **on/off** roll up the game impact rows, with the four-factor and shot-zone
  counts of each event (:func:`sportsdataverse.mbb.mbb_lineup_seasons.ncaa_lineup_count_rows`: a
  rim attempt is a dunk, layup, hook or tip-in);
* the **player box** (:func:`ncaa_box_events`, on the stints and in :func:`mbb_game_player_box_splits`)
  reads each event's ``player_1_id`` (shots, free throws, ``Offensive`` / ``Defensive Rebound``,
  ``Steal``, ``Blocked Shot``, ``Turnover``, ``Commits Foul``) and a made field goal's
  ``player_2_id`` (the assist). An event with no player id (a team rebound, a player the release
  could not identify) is no player's. Garbage time is each event's own ``is_garbage_time``, the
  flag the situational splits read.

``game_id`` is the stats.ncaa.org ``contest_id``, ``team_id`` the ESPN team id (Int64) and
``player_id`` the stats.ncaa.org player id (a String). A non-D1 opponent has no ESPN team id, so
its players get no row; its points still count against the D1 team.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_stints import (
    BOX_COUNTS,
    box_event_schema,
    credit_free_throws_to_foul_row,
    game_on_off,
    game_player_box_splits,
    game_stints,
    game_units,
    stint_row_schema,
)
from sportsdataverse.mbb.mbb_game_impact import _check_league, _events, _load_releases, ncaa_impact_rows
from sportsdataverse.mbb.mbb_lineup_seasons import ncaa_lineup_count_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "mbb_game_on_off",
    "mbb_game_player_box_splits",
    "mbb_game_stints",
    "mbb_game_units",
    "ncaa_box_events",
    "ncaa_stint_rows",
]

_PLAYER = pl.Utf8()
_ROWS = stint_row_schema(_PLAYER)
_EVENTS = box_event_schema(_PLAYER)
_COUNTS = [c for c in BOX_COUNTS if c != "reb"]


def ncaa_box_events(pbp: pl.DataFrame) -> pl.DataFrame:
    """Each player's box counts per stats.ncaa.org event, the input of the stint and split box.

    Args:
        pbp: ``load_ncaa_mbb_pbp`` / ``load_ncaa_wbb_pbp`` rows, in release order (``season``,
            ``contest_id``, ``event_team_espn_team_id``, ``player_1_id``, ``player_2_id``,
            ``event_type``, ``event_result``, ``shot_value``).

    Returns:
        Rows in :func:`sportsdataverse._common.game_stints.box_event_schema` (String player ids),
        ``order`` the event's row in the release (the stint rows' ``order``): one per event with a
        count for its ``player_1_id``, and one per assisted field goal for its ``player_2_id``. An
        event with no player id or no ESPN team id (a non-D1 side) has no row.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp
            from sportsdataverse.mbb.mbb_game_stints import ncaa_box_events
            ncaa_box_events(load_ncaa_mbb_pbp(2025)).head()
    """
    if pbp.height == 0:
        return pl.DataFrame(schema=_EVENTS)
    kind, value = pl.col("event_type"), pl.col("shot_value")
    made = pl.col("event_result") == "made"
    fg, three, ft = value.is_in([2, 3]), value == 3, value == 1
    flags = {
        "fga": fg,
        "fgm": fg & made,
        "fg3a": three,
        "fg3m": three & made,
        "fta": ft,
        "ftm": ft & made,
        "oreb": kind == "Offensive Rebound",
        "dreb": kind == "Defensive Rebound",
        "stl": kind == "Steal",
        "blk": kind == "Blocked Shot",
        "tov": kind == "Turnover",
        "pf": kind == "Commits Foul",
    }
    e = pbp.with_row_index("order").select(
        pl.col("season").cast(pl.Int64),
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.col("event_team_espn_team_id").cast(pl.Int64).alias("team_id"),
        pl.col("order").cast(pl.Int64),
        "player_1_id",
        "player_2_id",
        *[x.fill_null(False).cast(pl.Int64).alias(c) for c, x in flags.items()],
    )
    own = e.with_columns(
        (2 * pl.col("fgm") + pl.col("fg3m") + pl.col("ftm")).alias("points"), pl.lit(0).alias("ast")
    ).rename({"player_1_id": "player_id"})
    assists = e.filter(pl.col("fgm") == 1).select(
        "season", "game_id", "team_id", "order", pl.col("player_2_id").alias("player_id"), pl.lit(1).alias("ast")
    )
    out = pl.concat([own.filter(pl.sum_horizontal(_COUNTS) > 0), assists], how="diagonal_relaxed")
    return (
        out.drop_nulls(["team_id", "player_id"])
        .with_columns(pl.col(_COUNTS).fill_null(0))
        .select(list(_EVENTS))
        .cast(_EVENTS)
        .sort("game_id", "order", "player_id")
    )


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
        ids: ``period``, ``clock`` (seconds left in the period after the event), ``order`` (the
        event's row in the release) and ``is_garbage_time`` (the event's own flag).

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
    garbage = pbp.with_row_index("order").select(
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.col("order").cast(pl.Int64),
        pl.col("is_garbage_time").cast(pl.Boolean),
    )
    out = out.join(garbage, on=["game_id", "order"], how="left", validate="1:1", maintain_order="left")
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
        ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``, ``plus_minus`` and the player's own box in
        the stint (:func:`ncaa_box_events`; the columns of
        :func:`sportsdataverse.nba.nba_game_stints.nba_game_stints`). ``game_id`` is the NCAA
        contest id, ``team_id`` the ESPN team id (Int64), ``player_id`` the NCAA player id
        (String). Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_stints import mbb_game_stints
            stints = mbb_game_stints(2025)
    """
    pbp, possessions = _load_releases(seasons, league)
    events = ncaa_box_events(pbp)
    out = game_stints(ncaa_stint_rows(pbp, possessions, league=league), player_dtype=_PLAYER, events=events)
    return out.to_pandas() if return_as_pandas else out


def mbb_game_player_box_splits(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College player box by game, period and garbage time: minutes and counts from the play-by-play.

    The definitions are in :mod:`sportsdataverse._common.game_stints`
    (:func:`~sportsdataverse._common.game_stints.game_player_box_splits`).

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_game_stints.wbb_game_player_box_splits`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        Per ``(season, game_id, team_id, player_id)``: the ``family="all"`` row, one
        ``family="period"`` row per period played (``split`` ``p1``, ``p2`` for the men's halves,
        ``p1`` .. ``p4`` for the women's quarters from 2016, later periods the overtimes;
        ``period``; ``on_floor_at_end``) and the two ``family="garbage"`` rows (``garbage_time``,
        ``not_garbage_time``), each with the columns of
        :func:`sportsdataverse.nba.nba_game_stints.nba_game_player_box_splits`. Ids as in
        :func:`mbb_game_stints`. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_stints import mbb_game_player_box_splits
            mbb_game_player_box_splits(2025).filter(family="garbage", split="not_garbage_time").head()
    """
    pbp, possessions = _load_releases(seasons, league)
    events = ncaa_box_events(pbp)
    out = game_player_box_splits(ncaa_stint_rows(pbp, possessions, league=league), events, player_dtype=_PLAYER)
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
