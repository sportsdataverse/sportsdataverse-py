"""NBA / WNBA game flow and sequence stats (BB-D27): ``nba.game_flow`` and ``nba.team_season_flow``.

Builds the :mod:`sportsdataverse._common.game_flow` tables from three stats.nba.com releases:

* ``nba_stats_pbp`` -- the running score after each action (``score_home`` / ``score_away``,
  carried forward over the blank or ``"0"`` non-scoring rows, as BB-D6 reads it), the period and
  its clock. Only the made shots and free throws are read for the score: the "period" and
  "replay" rows can carry a stale one (the end of the 3rd in 0022500232 reads 60-55 at 86-87, a
  replay in 0022500109 13-15 at 16-15).
* ``nba_stats_possessions`` -- the stops, through BB-D3's
  :func:`~sportsdataverse.nba.nba_team_season_rates.nba_possession_rate_rows` (points from the
  made-shot counts; they agree with the play-by-play's score on whether a possession scored on
  all but 20 of the 2025-26 regular season's 245,646 possessions).
* ``nba_stats_schedules`` -- the home and away team and the season type of each completed game.

``league_id="10"`` reads the WNBA twins; :mod:`sportsdataverse.wnba.wnba_game_flow` binds it.
A game whose first period runs 20 minutes (the WNBA through 2005) is played in halves.

The stats schedule has no neutral-site column; :mod:`sportsdataverse.nba.nba_neutral_site` marks
the bubble, abroad and Las Vegas Cup games, which get the neutral prior (as BB-D3 treats them).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_flow import (
    GAME_FLOW_SCHEMA,
    SCORE_ROWS_SCHEMA,
    TEAM_SEASON_FLOW_SCHEMA,
    attach_home_wp,
    game_flow,
    team_season_flow,
)
from sportsdataverse.nba import nba_neutral_site
from sportsdataverse.nba.nba_game_impact import _running_score
from sportsdataverse.nba.nba_team_season_rates import _check_args, nba_possession_rate_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_game_flow", "nba_score_rows", "nba_team_season_flow"]

#: league_id -> (quarter seconds, model league).
_LEAGUES = {"00": (720.0, "nba"), "10": (600.0, "wnba")}
_OT_SECONDS = 300.0
#: A first period longer than this is a half (two 20-minute halves).
_HALF_GAME_FIRST_PERIOD_SECONDS = 900.0
#: The events whose score the running score reads.
_SCORING_EVENTS = ("made_shot", "free_throw")


def nba_score_rows(
    pbp: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
) -> pl.DataFrame:
    """Map the stats.nba.com play-by-play onto the game-flow score rows.

    Keeps the completed games of ``schedule`` whose ``season_type`` matches.

    Args:
        pbp: ``load_nba_stats_pbp`` / ``load_wnba_stats_pbp`` rows (``game_id``, ``order_index``,
            ``period``, ``seconds_remaining``, ``event_type``, ``team_id``, ``score_home``,
            ``score_away``, ``season``).
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows (``game_id``,
            ``season_type``, ``home_team_id``, ``away_team_id``, ``home_pts``).
        league_id: ``"00"`` NBA (12-minute quarters) or ``"10"`` WNBA (10-minute quarters).
        season_type: The schedule ``season_type`` to keep (see
            :func:`~sportsdataverse.nba.nba_team_season_rates.nba_possession_rate_rows`).

    Returns:
        Rows in :data:`~sportsdataverse._common.game_flow.SCORE_ROWS_SCHEMA`, without ``home_wp``.

    Raises:
        ValueError: If ``league_id`` or ``season_type`` is unknown.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_loaders import load_nba_stats_pbp, load_nba_stats_schedules
            from sportsdataverse.nba.nba_game_flow import nba_score_rows
            rows = nba_score_rows(load_nba_stats_pbp(2025), load_nba_stats_schedules(2025))
    """
    _check_args(league_id, season_type)
    if pbp.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema=SCORE_ROWS_SCHEMA)
    games = schedule.filter((pl.col("season_type") == season_type) & pl.col("home_pts").is_not_null()).select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("away_team_id").cast(pl.Int64),
        nba_neutral_site._neutral_site(schedule, league_id).alias("neutral_site"),
    )
    quarter = _LEAGUES[league_id][0]
    period, clock = pl.col("period"), pl.col("seconds_remaining").cast(pl.Float64)
    halves = clock.filter(period == 1).max().over("game_id") > _HALF_GAME_FIRST_PERIOD_SECONDS
    reg_len = pl.when(pl.col("_halves")).then(2 * quarter).otherwise(quarter)
    n_reg = pl.when(pl.col("_halves")).then(2).otherwise(4)
    in_reg = period <= n_reg
    start = pl.when(in_reg).then((period - 1) * reg_len).otherwise(n_reg * reg_len + (period - n_reg - 1) * _OT_SECONDS)
    length = pl.when(in_reg).then(reg_len).otherwise(_OT_SECONDS)
    scores = pl.col("event_type").is_in(_SCORING_EVENTS)
    return (
        pbp.select(
            "season",
            "game_id",
            "order_index",
            "period",
            "seconds_remaining",
            "event_type",
            "team_id",
            "score_home",
            "score_away",
        )
        .with_columns(pl.col("game_id").cast(pl.Utf8))
        .join(games, on="game_id", how="inner", validate="m:1")
        .sort("game_id", "order_index")
        .with_columns(pl.when(scores).then(pl.col(f"score_{s}")).alias(f"score_{s}") for s in ("home", "away"))
        .with_columns(_running_score("home").alias("home_score"), _running_score("away").alias("away_score"))
        .with_columns(halves.alias("_halves"))
        .with_columns(
            (start + length - clock).alias("elapsed"),
            pl.when(in_reg).then((n_reg - period) * reg_len + clock).otherwise(clock).alias("sec_left"),
            (n_reg * reg_len + (period.max().over("game_id") - n_reg).clip(lower_bound=0) * _OT_SECONDS).alias(
                "game_seconds"
            ),
        )
        .select(
            "season",
            "game_id",
            pl.col("order_index").alias("order"),
            "home_team_id",
            "away_team_id",
            "neutral_site",
            "elapsed",
            "sec_left",
            "game_seconds",
            pl.col("team_id").alias("event_team_id"),
            "home_score",
            "away_score",
        )
        .cast(SCORE_ROWS_SCHEMA)
    )


def _load_game_flow(seasons: int | list[int], league_id: str, season_type: str) -> pl.DataFrame:
    """Load the three releases for ``seasons`` (named by the year they end) and build the game rows."""
    _check_args(league_id, season_type)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if league_id == "10":
        from sportsdataverse.wnba import wnba_loaders as W

        args = seasons_list
        pbp, poss, sched = (
            W.load_wnba_stats_pbp(args),
            W.load_wnba_stats_possessions(args),
            W.load_wnba_stats_schedules(args),
        )
    else:
        from sportsdataverse.nba import nba_loaders as N

        args = [s - 1 for s in seasons_list]  # the NBA loaders take the start year
        pbp, poss, sched = (
            N.load_nba_stats_pbp(args),
            N.load_nba_stats_possessions(args),
            N.load_nba_stats_schedules(args),
        )
    rows = nba_score_rows(pbp, sched, league_id=league_id, season_type=season_type)
    if rows.height == 0:
        return pl.DataFrame(schema=GAME_FLOW_SCHEMA)
    stops = nba_possession_rate_rows(poss, sched, league_id=league_id, season_type=season_type)
    return game_flow(attach_home_wp(rows, league=_LEAGUES[league_id][1]), stops)


def nba_game_flow(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Game flow per team and game: runs, droughts, kills, lead changes, time leading and game control.

    The definitions are in :mod:`sportsdataverse._common.game_flow`. Over the 1,315 games of the
    2025-26 NBA season (every season type), ``lead_changes`` equals the official cdn.nba.com box
    score's in every game and ``times_tied`` and ``largest_lead`` in all but one; over the 310 games
    of the 2025 WNBA season all three equal cdn.wnba.com's in every game.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or
            ``"play-in"``, matched to the schedule's ``season_type``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id)``: ``opp_team_id``, ``is_home``, ``pts`` /
        ``opp_pts`` and ``won``; ``game_seconds``; ``lead_sec`` / ``trail_sec`` / ``tied_sec`` and
        ``lead_pct``; ``lead_changes`` and ``times_tied`` (game totals); ``largest_lead``,
        ``largest_deficit`` and ``comeback``; ``max_run``, ``runs_10`` and ``opp_runs_10`` (unanswered-point runs);
        ``longest_drought_sec``; ``avg_margin`` and ``avg_wp`` (game control); ``def_poss``,
        ``stops``, ``kills`` and ``double_kills``. Ids are the stats.nba.com ids (Int64; ``game_id``
        a String). Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or ``season_type`` is not one
            of the four above.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_flow import nba_game_flow
            flow = nba_game_flow(2026)
            flow.sort("comeback", descending=True).head()
    """
    out = _load_game_flow(seasons, league_id, season_type)
    return out.to_pandas() if return_as_pandas else out


def nba_team_season_flow(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Game flow per team and season, with ranks, percentiles and ``_n``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or
            ``"play-in"``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)``: ``games``, ``wins``; ``lead_pct``, ``trail_pct``,
        ``tied_pct``, ``avg_margin`` and ``avg_wp`` (weighted by game clock); ``lead_changes_pg``
        and ``times_tied_pg``; ``largest_lead``, ``largest_comeback`` and ``max_run`` (season
        bests); ``runs_10`` / ``opp_runs_10`` and their per-game rates; ``avg_longest_drought_sec``;
        ``def_poss``, ``stops``, ``stop_pct``; ``kills``, ``double_kills``, ``kills_pg``,
        ``double_kills_pg`` and ``kills_per_100``. The ten rated metrics carry ``<m>_rank`` (1 =
        best; ``opp_runs_10_pg`` and ``avg_longest_drought_sec`` rank low first), ``<m>_pct`` and
        ``<m>_n`` (games; the games with possessions for ``kills_pg`` / ``double_kills_pg`` and with a win
        probability for ``avg_wp``; defensive possessions for ``stop_pct`` and ``kills_per_100``), among
        teams with at least half the season's most games, null under five teams. Empty input
        returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or ``season_type`` is not one
            of the four above.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_flow import nba_team_season_flow
            nba_team_season_flow(2026).sort("kills_pg_rank").select("team_id", "kills_pg", "avg_wp").head()
    """
    games = _load_game_flow(seasons, league_id, season_type)
    out = team_season_flow(games) if games.height else pl.DataFrame(schema=TEAM_SEASON_FLOW_SCHEMA)
    return out.to_pandas() if return_as_pandas else out
