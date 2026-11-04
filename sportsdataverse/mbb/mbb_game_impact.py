"""College game impact box (BB-D6): ``mbb.game_player_impact`` / ``mbb.game_lineups_used`` (and wbb).

Builds the :mod:`sportsdataverse._common.game_impact` tables from two stats.ncaa.org releases:

* ``ncaa_{mbb,wbb}_pbp`` -- each event with the ten players on the floor, the running score,
  the game clock (``game_seconds``), free throws (``event_type == "Free Throw"``) and fouls (a
  ``foul_class``: personal, offensive, technical, administrative technical);
* ``ncaa_{mbb,wbb}_possessions`` -- the possessions and their lineups, as
  :mod:`sportsdataverse.mbb.mbb_team_season_rates` reads them.

``game_id`` is the stats.ncaa.org ``contest_id``, ``team_id`` the ESPN team id (Int64) and
``player_id`` the stats.ncaa.org player id (a String): there is no NCAA-to-ESPN player crosswalk
yet. A non-D1 opponent has no ESPN team id, so its players get no row; its points still count
against the D1 team. Events keep the release's row order, which is game order.

There is no official college +/- to check against (neither the ESPN nor the NCAA box score
carries one), so the tables are checked against the final score and the possession counts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_impact import (
    credit_free_throws_to_foul_lineup,
    game_lineups_used,
    game_player_impact,
    impact_row_schema,
)
from sportsdataverse.mbb.mbb_team_season_rates import _WBB_FIRST_QUARTERS_SEASON

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_game_lineups_used", "mbb_game_player_impact", "ncaa_impact_rows"]

_LEAGUES = ("mens", "womens")
_PLAYER = pl.Utf8()
_ROWS = impact_row_schema(_PLAYER)
_SLOT_NAMES = [f"{side}_{i}" for side in ("home", "away") for i in range(1, 6)]


def _check_league(league: str) -> None:
    if league not in _LEAGUES:
        raise ValueError(f"league must be one of {list(_LEAGUES)}, got {league!r}")


def _slots(prefix: str = "") -> list[pl.Expr]:
    """``home_1..5`` / ``away_1..5`` from the release's ``{side}_{i}_player_id`` columns."""
    return [
        pl.col(f"{prefix}{side}_{i}_player_id").cast(_PLAYER).alias(f"{side}_{i}")
        for side in ("home", "away")
        for i in range(1, 6)
    ]


def _period_bounds(league: str) -> tuple[pl.Expr, pl.Expr]:
    """(start, end) of the event's period in cumulative game seconds: halves, or quarters for women from 2016."""
    quarters = (pl.col("season") >= _WBB_FIRST_QUARTERS_SEASON) if league == "womens" else pl.lit(False)
    n_reg = pl.when(quarters).then(4).otherwise(2)
    reg_len = 2400.0 / n_reg
    period = pl.col("period")
    regulation = period <= n_reg
    start = pl.when(regulation).then((period - 1) * reg_len).otherwise(2400.0 + 300.0 * (period - n_reg - 1))
    return start, start + pl.when(regulation).then(reg_len).otherwise(300.0)


def _teams() -> list[pl.Expr]:
    return [
        pl.col("season").cast(pl.Int64),
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.col("home_espn_team_id").cast(pl.Int64).alias("home_team_id"),
        pl.col("away_espn_team_id").cast(pl.Int64).alias("away_team_id"),
    ]


def ncaa_impact_rows(pbp: pl.DataFrame, possessions: pl.DataFrame, *, league: str = "mens") -> pl.DataFrame:
    """Map the stats.ncaa.org pbp and possessions onto the game-impact row frame.

    One row per event (the clock since the previous event in the period and the points scored on
    it, on that event's lineup; a free throw on the lineup at the last foul) and one per
    possession (its lineup and which side had it, ``poss_team == home``). The clock left in a
    period after its last event goes to that event's lineup, so each period's clock is whole. The
    possessions of a contest with no pbp events, and those whose ``poss_team`` is neither side,
    are dropped.

    Args:
        pbp: ``load_ncaa_mbb_pbp`` / ``load_ncaa_wbb_pbp`` rows, in release (game) order, with the
            cumulative ``game_seconds`` and the ``period``.
        possessions: ``load_ncaa_mbb_possessions`` / ``load_ncaa_wbb_possessions`` rows.
        league: ``"mens"`` (20-minute halves) or ``"womens"`` (10-minute quarters from 2016,
            halves before); overtimes are 5 minutes.

    Returns:
        Rows in :func:`sportsdataverse._common.game_impact.impact_row_schema` with String player ids.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp, load_ncaa_mbb_possessions
            from sportsdataverse.mbb.mbb_game_impact import ncaa_impact_rows
            rows = ncaa_impact_rows(load_ncaa_mbb_pbp(2025), load_ncaa_mbb_possessions(2025))
    """
    _check_league(league)
    if pbp.height == 0:
        return pl.DataFrame(schema=_ROWS)
    game = pl.col("contest_id")
    in_period = ["contest_id", "period"]
    elapsed = pl.col("game_seconds").cast(pl.Float64)
    start, end = _period_bounds(league)
    is_last = pl.int_range(pl.len()).over(in_period) == pl.len().over(in_period) - 1
    events = (
        pbp.select(
            "season",
            "contest_id",
            "period",
            "game_seconds",
            "home_score",
            "away_score",
            "event_type",
            "foul_class",
            "home_espn_team_id",
            "away_espn_team_id",
            *[f"{c}_player_id" for c in _SLOT_NAMES],
        )  # fmt: skip
        .with_row_index("_order")
        .sort([game, "_order"])  # ponytail: the release is in game order; sort only groups games
        .with_columns(
            (
                elapsed
                - elapsed.shift(1).over(in_period).fill_null(start)
                + pl.when(is_last).then(end - elapsed).otherwise(0.0)
            ).alias("seconds"),
            *[
                (pl.col(f"{s}_score") - pl.col(f"{s}_score").shift(1).over(game).fill_null(0))
                .cast(pl.Int64)
                .alias(f"{s}_pts")
                for s in ("home", "away")
            ],
            pl.lit(0).alias("home_poss"),
            pl.lit(0).alias("away_poss"),
        )
        .select(
            *_teams(),
            *_slots(),
            "seconds",
            "home_pts",
            "away_pts",
            "home_poss",
            "away_poss",
            "event_type",
            "foul_class",
        )
    )
    events = credit_free_throws_to_foul_lineup(
        events, is_free_throw=pl.col("event_type") == "Free Throw", is_foul=pl.col("foul_class").is_not_null()
    )
    parts = [events.select(list(_ROWS))]
    if possessions.height:
        parts.append(_possession_rows(possessions, events["game_id"].unique()))
    return pl.concat([p.cast(_ROWS) for p in parts])


def _possession_rows(possessions: pl.DataFrame, played: pl.Series) -> pl.DataFrame:
    """One impact row per possession of a contest with events, its side from ``poss_team``.

    A contest with no events would publish poss with no points, and a ``poss_team`` that is
    neither side has no team to credit: both are dropped.
    """
    home_has = pl.col("poss_team") == pl.col("home")
    has_events = pl.col("contest_id").cast(pl.Utf8).is_in(played.implode())
    kept = possessions.filter(has_events & (home_has | (pl.col("poss_team") == pl.col("away"))))
    return kept.select(
        *_teams(),
        *_slots(),
        pl.lit(0.0).alias("seconds"),
        pl.lit(0).alias("home_pts"),
        pl.lit(0).alias("away_pts"),
        home_has.cast(pl.Int64).alias("home_poss"),
        (~home_has).cast(pl.Int64).alias("away_poss"),
    )


def _load_rows(seasons: int | list[int], league: str) -> pl.DataFrame:
    _check_league(league)
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_pbp as load_pbp
        from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_possessions as load_poss
    else:
        from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp as load_pbp
        from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_possessions as load_poss
    return ncaa_impact_rows(load_pbp(seasons), load_poss(seasons), league=league)


def mbb_game_player_impact(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """The college game impact box: per player and game, minutes, on-court possessions, +/- and ratings.

    The definitions are in :mod:`sportsdataverse._common.game_impact`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_game_impact.wbb_game_player_impact`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id)``: ``opp_team_id``, ``minutes`` and
        ``seconds`` on the floor, ``poss`` / ``opp_poss`` (offensive / defensive possessions on the
        floor), ``pts`` / ``opp_pts`` (points scored / allowed while on), ``plus_minus``,
        ``off_rtg`` / ``def_rtg`` / ``net_rtg`` (per 100 possessions) and their ``_n`` sample sizes.
        ``game_id`` is the NCAA contest id, ``team_id`` the ESPN team id (Int64), ``player_id``
        the NCAA player id (String). Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_impact import mbb_game_player_impact
            box = mbb_game_player_impact(2025)
    """
    out = game_player_impact(_load_rows(seasons, league), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def mbb_game_lineups_used(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """The college lineups used: per five-man unit and game, minutes, possessions, +/- and ratings.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_game_impact.wbb_game_lineups_used`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, lineup_id)``: ``lineup_id`` (the five NCAA player
        ids sorted and joined with ``-``), ``player_1..5``, and the same totals and ratings as
        :func:`mbb_game_player_impact`, sorted by minutes within each team and game. Empty input
        returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_impact import mbb_game_lineups_used
            units = mbb_game_lineups_used(2025)
    """
    out = game_lineups_used(_load_rows(seasons, league), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out
