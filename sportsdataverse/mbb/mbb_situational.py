"""College situational splits (BB-D21): ``mbb.situational_{team,player,lineup}`` (and wbb).

Builds the :mod:`sportsdataverse._common.situational_splits` tables from three releases:

* ``ncaa_{mbb,wbb}_possessions`` -- the possessions, through BB-D3's
  :func:`~sportsdataverse.mbb.mbb_team_season_rates.ncaa_possession_rate_rows` (points, clock and
  the clutch window), with the release's own ``is_transition`` and ``is_garbage_time`` (bigballR's
  rules; the garbage-time rule is the one BB-D7 ported to the pros) and the ten players on the floor
  (``{home,away}_{1..5}_player_id``);
* the ESPN schedule -- ``neutral_site``, by ``espn_game_id``;
* ``mbb_ratings`` / ``wbb_ratings`` -- the published adjusted ratings (BB-D19's college source), for
  the ``opponent`` family's top 25, among teams with at least half the most games (non-D1 out).
  The ranking is the release at call time (mid-season, the ratings to date) and is hindsight: the
  split games are in it, so it describes the schedule and is not a predictive feature.

``game_id`` is the stats.ncaa.org ``contest_id``, ``team_id`` the ESPN team id (Int64) and
``player_id`` the stats.ncaa.org player id (a String), as in BB-D6. The releases carry no
offensive-rebound count per possession, so college has no ``chance`` family. Men play halves
(``p1``, ``p2``); women 10-minute quarters from 2015-16, halves before.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.situational_splits import (
    DEF_SLOTS,
    OFF_SLOTS,
    situation_row_schema,
    situational_lineup,
    situational_player,
    situational_team,
    situational_team_games,
)
from sportsdataverse.mbb.mbb_team_season_rates import (
    _check_league,
    _final_period,
    _league_loaders,
    ncaa_possession_rate_rows,
)
from sportsdataverse.mbb.mbb_team_strength import _load_ratings, _ratings

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "mbb_situational_lineup",
    "mbb_situational_player",
    "mbb_situational_team",
    "mbb_situational_team_games",
    "ncaa_situation_rows",
]

_PLAYER = pl.Utf8()
_ROWS = situation_row_schema(_PLAYER)
_TOP_N = 25
_KEY = ["game_id", "period", "possession_number"]


def ncaa_situation_rows(possessions: pl.DataFrame, schedule: pl.DataFrame, *, league: str = "mens") -> pl.DataFrame:
    """Map ``ncaa_{mbb,wbb}_possessions`` rows onto the situational-splits possession frame.

    Args:
        possessions: ``load_ncaa_mbb_possessions`` / ``load_ncaa_wbb_possessions`` rows.
        schedule: ``load_mbb_schedule`` / ``load_wbb_schedule`` rows (``game_id``, ``neutral_site``).
        league: ``"mens"`` or ``"womens"``.

    Returns:
        One row per possession in
        :func:`~sportsdataverse._common.situational_splits.situation_row_schema` (String player ids,
        null ``is_second_chance``).

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_possessions
            from sportsdataverse.mbb.mbb_situational import ncaa_situation_rows
            rows = ncaa_situation_rows(load_ncaa_mbb_possessions(2025), load_mbb_schedule(2025))
    """
    _check_league(league)
    if possessions.height == 0:
        return pl.DataFrame(schema=_ROWS)
    home_has = pl.col("poss_team") == pl.col("home")

    def slots(names: tuple[str, ...], ours: bool) -> list[pl.Expr]:
        own, other = ("home", "away") if ours else ("away", "home")
        return [
            pl.when(home_has).then(pl.col(f"{own}_{i}_player_id")).otherwise(pl.col(f"{other}_{i}_player_id")).alias(s)
            for i, s in enumerate(names, 1)
        ]

    extras = possessions.select(
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.col("period").cast(pl.Int64),
        pl.col("poss_num").cast(pl.Int64).alias("possession_number"),
        _final_period(league).cast(pl.Int64).alias("reg_periods"),
        (pl.col("is_transition") == 1).alias("is_transition"),
        (pl.col("is_garbage_time") == 1).alias("is_garbage_time"),
        pl.lit(None, dtype=pl.Boolean).alias("is_second_chance"),
        *slots(OFF_SLOTS, ours=True),
        *slots(DEF_SLOTS, ours=False),
    )
    rows = ncaa_possession_rate_rows(possessions, schedule, league=league)
    return rows.join(extras, on=_KEY, how="left", validate="1:1").select(list(_ROWS)).cast(_ROWS)


def _qualified_ratings(released: pl.DataFrame) -> pl.DataFrame:
    """The published ratings of teams with at least half the season's most games (no non-D1 opponent)."""
    keep = pl.col("games") >= 0.5 * pl.col("games").max().over("season")
    return _ratings(released.filter(keep))


def _load(seasons: int | list[int], league: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The situation rows and the qualified published ratings for ``seasons``."""
    _check_league(league)
    load_possessions, _, load_schedule = _league_loaders(league)
    rows = ncaa_situation_rows(load_possessions(seasons), load_schedule(seasons), league=league)
    ratings = _qualified_ratings(_load_ratings(league)(seasons)) if rows.height else pl.DataFrame()
    return rows, ratings


def _check(league: str, top_n: int) -> None:
    _check_league(league)
    if top_n < 1:
        raise ValueError(f"top_n must be at least 1, got {top_n}")


def mbb_situational_team(
    seasons: int | list[int], *, league: str = "mens", top_n: int = _TOP_N, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College team splits by situation: clutch, period, half, garbage time, score, venue, opponent, play type.

    The families and their splits are in :mod:`sportsdataverse._common.situational_splits`; within
    a family the splits add up to the ``all`` row.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_situational.wbb_situational_team`).
        top_n: How many of the season's best teams by the published ``adj_em`` are ``vs_top``
            (default 25).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, family, split)`` with the columns of
        :func:`sportsdataverse.nba.nba_situational.nba_situational_team`; ``team_id`` is the ESPN
        team id (Int64). A non-D1 opponent gets no row. Empty input returns that schema with zero
        rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``, or ``top_n`` is below 1.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_situational import mbb_situational_team
            mbb_situational_team(2026).filter(split="vs_top").sort("net_rtg", descending=True).head()
    """
    _check(league, top_n)
    rows, ratings = _load(seasons, league)
    out = situational_team(rows, ratings, top_n=top_n)
    return out.to_pandas() if return_as_pandas else out


def mbb_situational_team_games(
    seasons: int | list[int], *, league: str = "mens", top_n: int = _TOP_N, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College team splits by situation, one game at a time: :func:`mbb_situational_team` per game.

    The same possessions and labels, grouped by game too, so a team-season's rows of a split add
    up to its :func:`mbb_situational_team` row (``games``, ``seconds``, ``poss``, ``opp_poss``,
    ``pts``, ``opp_pts``). The ``opponent`` family keeps the season's top ``top_n``.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        league: ``"mens"`` or ``"womens"``
            (:func:`sportsdataverse.wbb.wbb_situational.wbb_situational_team_games`).
        top_n: As :func:`mbb_situational_team`.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, game_id, family, split)``, the columns of
        :func:`mbb_situational_team` plus ``game_id`` (the stats.ncaa.org contest id); ``games`` is
        1. A split the team did not play in the game has no row. Empty input returns that schema
        with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``, or ``top_n`` is below 1.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_situational import mbb_situational_team_games
            mbb_situational_team_games(2026).filter(family="score").head()
    """
    _check(league, top_n)
    rows, ratings = _load(seasons, league)
    out = situational_team_games(rows, ratings, top_n=top_n)
    return out.to_pandas() if return_as_pandas else out


def mbb_situational_player(
    seasons: int | list[int], *, league: str = "mens", top_n: int = _TOP_N, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College player on-floor splits by situation: the team splits over the possessions each player started.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        league: ``"mens"`` or ``"womens"``.
        top_n: As :func:`mbb_situational_team`.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id, family, split)`` with the columns of
        :func:`sportsdataverse.nba.nba_situational.nba_situational_player`; ``player_id`` is the
        stats.ncaa.org player id (String). Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``, or ``top_n`` is below 1.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_situational import mbb_situational_player
            mbb_situational_player(2026).filter(split="clutch").sort("net_rtg_n", descending=True).head()
    """
    _check(league, top_n)
    rows, ratings = _load(seasons, league)
    out = situational_player(rows, ratings, top_n=top_n, player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def mbb_situational_lineup(
    seasons: int | list[int], *, league: str = "mens", top_n: int = _TOP_N, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College five-man unit splits by situation: the team splits over the possessions each unit started.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        league: ``"mens"`` or ``"womens"``.
        top_n: As :func:`mbb_situational_team`.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, lineup_id, family, split)`` with the columns of
        :func:`sportsdataverse.nba.nba_situational.nba_situational_lineup` (String player ids).
        Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``, or ``top_n`` is below 1.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_situational import mbb_situational_lineup
            mbb_situational_lineup(2026).filter(split="not_garbage_time").sort("poss", descending=True).head()
    """
    _check(league, top_n)
    rows, ratings = _load(seasons, league)
    out = situational_lineup(rows, ratings, top_n=top_n, player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out
