"""College game flow and sequence stats (BB-D27): ``mbb.game_flow`` / ``mbb.team_season_flow`` (and wbb).

Builds the :mod:`sportsdataverse._common.game_flow` tables from three releases:

* ``ncaa_{mbb,wbb}_pbp`` -- each event's running score (``home_score`` / ``away_score``, filled
  on every row), the cumulative game clock (``game_seconds``) and the event team, in release
  (game) order;
* ``ncaa_{mbb,wbb}_possessions`` -- the stops, through BB-D3's
  :func:`~sportsdataverse.mbb.mbb_team_season_rates.ncaa_possession_rate_rows`;
* the ESPN schedule -- ``neutral_site`` (by ``espn_game_id``) for the win-probability prior.

``game_id`` is the stats.ncaa.org ``contest_id`` and ``team_id`` the ESPN team id (Int64), as in
BB-D3 and BB-D6. A non-D1 opponent has no ESPN team id, so it gets no row; its points still count.
Men play two 20-minute halves, women 10-minute quarters from 2015-16 (halves before); overtimes
are 5 minutes. The release's row order is taken as game order (its ``game_seconds`` never runs
backwards within a 2025-26 contest).

Known source gap: in some neutral-site contests the release's NCAA home and away sides are the
reverse of ESPN's, and its ``home_espn_team_id`` / ``away_espn_team_id`` follow ESPN while the
scores follow the NCAA, so the two teams' rows are swapped (45 of 6,297 men's and 65 of 6,018
women's 2025-26 contests). BB-D3 and BB-D6 read the same ids; the fix belongs in the release.
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
from sportsdataverse.mbb.mbb_game_impact import _period_bounds
from sportsdataverse.mbb.mbb_team_season_rates import _check_league, _league_loaders, ncaa_possession_rate_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_game_flow", "mbb_team_season_flow", "ncaa_score_rows"]

_MODEL = {"mens": "mbb", "womens": "wbb"}


def ncaa_score_rows(pbp: pl.DataFrame, schedule: pl.DataFrame, *, league: str = "mens") -> pl.DataFrame:
    """Map the stats.ncaa.org play-by-play onto the game-flow score rows.

    Args:
        pbp: ``load_ncaa_mbb_pbp`` / ``load_ncaa_wbb_pbp`` rows, in release (game) order
            (``contest_id``, ``season``, ``period``, ``game_seconds``, ``home_score``,
            ``away_score``, ``event_team_espn_team_id``, ``home_espn_team_id``,
            ``away_espn_team_id``, ``espn_game_id``).
        schedule: ``load_mbb_schedule`` / ``load_wbb_schedule`` rows (``game_id``,
            ``neutral_site``), joined on ``espn_game_id``; a contest it lacks is not neutral.
        league: ``"mens"`` or ``"womens"``.

    Returns:
        Rows in :data:`~sportsdataverse._common.game_flow.SCORE_ROWS_SCHEMA`, without ``home_wp``.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_pbp
            from sportsdataverse.mbb.mbb_game_flow import ncaa_score_rows
            rows = ncaa_score_rows(load_ncaa_mbb_pbp(2026), load_mbb_schedule(2026))
    """
    _check_league(league)
    if pbp.height == 0:
        return pl.DataFrame(schema=SCORE_ROWS_SCHEMA)
    neutral = schedule.select(
        pl.col("game_id").cast(pl.Int64).alias("espn_game_id"), pl.col("neutral_site").cast(pl.Boolean)
    ).unique("espn_game_id")
    start, end = _period_bounds(league)
    elapsed = pl.col("game_seconds").cast(pl.Float64)
    in_reg = start < 2400.0
    return (
        pbp.select(
            "season",
            "contest_id",
            "period",
            "game_seconds",
            "home_score",
            "away_score",
            "event_team_espn_team_id",
            "home_espn_team_id",
            "away_espn_team_id",
            pl.col("espn_game_id").cast(pl.Int64),
        )
        .with_row_index("order")
        .join(neutral, on="espn_game_id", how="left", validate="m:1")
        .select(
            "season",
            pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
            "order",
            pl.col("home_espn_team_id").alias("home_team_id"),
            pl.col("away_espn_team_id").alias("away_team_id"),
            pl.col("neutral_site").fill_null(False),
            elapsed.alias("elapsed"),
            pl.when(in_reg).then(2400.0 - elapsed).otherwise(end - elapsed).alias("sec_left"),
            end.max().over("contest_id").alias("game_seconds"),
            pl.col("event_team_espn_team_id").alias("event_team_id"),
            "home_score",
            "away_score",
        )
        .cast(SCORE_ROWS_SCHEMA)
    )


def _load_game_flow(seasons: int | list[int], league: str) -> pl.DataFrame:
    _check_league(league)
    load_possessions, _, load_schedule = _league_loaders(league)
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_pbp as load_pbp
    else:
        from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp as load_pbp
    schedule = load_schedule(seasons)
    rows = ncaa_score_rows(load_pbp(seasons), schedule, league=league)
    if rows.height == 0:
        return pl.DataFrame(schema=GAME_FLOW_SCHEMA)
    stops = ncaa_possession_rate_rows(load_possessions(seasons), schedule, league=league)
    return game_flow(attach_home_wp(rows, league=_MODEL[league]), stops)


def mbb_game_flow(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College game flow per team and game: runs, droughts, kills, lead changes, time leading, game control.

    The definitions are in :mod:`sportsdataverse._common.game_flow`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_game_flow.wbb_game_flow`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id)`` with the columns of
        :func:`sportsdataverse.nba.nba_game_flow.nba_game_flow`. ``game_id`` is the NCAA contest
        id, ``team_id`` the ESPN team id (Int64). Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_flow import mbb_game_flow
            flow = mbb_game_flow(2026)
    """
    out = _load_game_flow(seasons, league)
    return out.to_pandas() if return_as_pandas else out


def mbb_team_season_flow(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College game flow per team and season, with ranks, percentiles and ``_n``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_game_flow.wbb_team_season_flow`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)`` with the columns of
        :func:`sportsdataverse.nba.nba_game_flow.nba_team_season_flow`; ranks are among teams with
        at least half the season's most games (which leaves out the non-D1 opponents). Empty input
        returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_flow import mbb_team_season_flow
            mbb_team_season_flow(2026).sort("avg_wp_rank").head()
    """
    games = _load_game_flow(seasons, league)
    out = team_season_flow(games) if games.height else pl.DataFrame(schema=TEAM_SEASON_FLOW_SCHEMA)
    return out.to_pandas() if return_as_pandas else out
