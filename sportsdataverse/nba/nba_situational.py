"""NBA / WNBA situational splits (BB-D21): ``nba.situational_{team,player,lineup}``.

Builds the :mod:`sportsdataverse._common.situational_splits` tables from three stats.nba.com
releases, read once per call:

* ``nba_stats_possessions`` -- the possessions, through BB-D3's
  :func:`~sportsdataverse.nba.nba_team_season_rates.nba_possession_rate_rows` (points from the
  made-shot counts, the clutch clock), with their ``is_second_chance`` flag and the ten players on
  the floor as each starts (``off_player_1..5`` / ``def_player_1..5``);
* ``nba_stats_pbp`` -- BB-D7's :func:`~sportsdataverse.nba.nba_possession_flags.add_possession_flags`
  reads it for ``is_transition`` and ``is_garbage_time``;
* ``nba_stats_schedules`` -- the home and away team and the season type.

The ``opponent`` family's top teams are BB-D19's adjusted net rating
(:func:`~sportsdataverse._common.team_strength.possession_ridge_ratings`) over the regular season,
whatever ``season_type`` is asked for: a hindsight ranking, the split games included, so a
description of the schedule and not a predictive feature. ``league_id="10"`` reads the WNBA twins
(:mod:`sportsdataverse.wnba.wnba_situational` binds it); a WNBA game through 2005 is two 20-minute
halves, so its ``period`` splits are ``p1``, ``p2`` and ``ot``.
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
)
from sportsdataverse._common.team_strength import RIDGE_LAMBDA, possession_ridge_ratings
from sportsdataverse.nba.nba_possession_flags import POSSESSION_FLAGS_SCHEMA, add_possession_flags
from sportsdataverse.nba.nba_team_season_rates import _check_args, _regulation_periods, nba_possession_rate_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "nba_situation_rows",
    "nba_situational_lineup",
    "nba_situational_player",
    "nba_situational_team",
]

_PLAYER = pl.Int64()
_ROWS = situation_row_schema(_PLAYER)
#: league_id -> the default ``top_n``: about the top third of the league.
_TOP_N = {"00": 10, "10": 4}
_KEY = ["game_id", "period", "possession_number"]
_PBP_COLUMNS = ("game_id", "order_index", "event_type", "seconds_remaining", "score_home", "score_away")


def nba_situation_rows(
    possessions: pl.DataFrame,
    pbp: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
) -> pl.DataFrame:
    """Map the stats.nba.com releases onto the situational-splits possession frame.

    Args:
        possessions: ``load_nba_stats_possessions`` / ``load_wnba_stats_possessions`` rows, every
            possession of each game (BB-D7 reads the previous possession and the starters).
        pbp: ``load_nba_stats_pbp`` / ``load_wnba_stats_pbp`` rows of the same games (``game_id``,
            ``order_index``, ``event_type``, ``seconds_remaining``, ``score_home``, ``score_away``).
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: The schedule ``season_type`` to keep (as
            :func:`~sportsdataverse.nba.nba_team_season_rates.nba_possession_rate_rows`).

    Returns:
        One row per possession in
        :func:`~sportsdataverse._common.situational_splits.situation_row_schema` (Int64 player ids).

    Raises:
        ValueError: If ``league_id`` or ``season_type`` is unknown, or ``pbp`` misses the events of a
            possession of a ``season_type`` game (the other season types' games are not read).

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_situational import nba_situation_rows
            rows = nba_situation_rows(L.load_nba_stats_possessions(2025), L.load_nba_stats_pbp(2025),
                                      L.load_nba_stats_schedules(2025))
    """
    _check_args(league_id, season_type)
    if possessions.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema=_ROWS)
    # flag the requested games only: another season type's game with a gap in its pbp must not fail this one
    games = schedule.filter(pl.col("season_type") == season_type).select(pl.col("game_id").cast(pl.Utf8)).unique()
    poss = possessions.with_columns(pl.col("game_id").cast(pl.Utf8)).join(games, on="game_id", how="semi")
    if poss.height == 0:
        return pl.DataFrame(schema=_ROWS)
    events = (
        pbp.select(_PBP_COLUMNS).with_columns(pl.col("game_id").cast(pl.Utf8)).join(games, on="game_id", how="semi")
    )
    # a release that already carries BB-D7's flags is re-flagged: this version's window, never a clash
    flagged = add_possession_flags(poss.drop(*POSSESSION_FLAGS_SCHEMA, strict=False), events)
    extras = flagged.select(
        *_KEY,
        _regulation_periods().alias("reg_periods"),
        "is_transition",
        "is_garbage_time",
        "is_second_chance",
        *[pl.col(f"off_player_{i}").alias(s) for i, s in enumerate(OFF_SLOTS, 1)],
        *[pl.col(f"def_player_{i}").alias(s) for i, s in enumerate(DEF_SLOTS, 1)],
    ).cast({"period": pl.Int64, "possession_number": pl.Int64})
    rows = nba_possession_rate_rows(poss, schedule, league_id=league_id, season_type=season_type)
    return rows.join(extras, on=_KEY, how="left", validate="1:1").select(list(_ROWS)).cast(_ROWS)


def _ratings(possessions: pl.DataFrame, schedule: pl.DataFrame, league_id: str) -> pl.DataFrame:
    """BB-D19's adjusted ratings over the regular season of ``possessions``."""
    regular = nba_possession_rate_rows(possessions, schedule, league_id=league_id, season_type="regular-season")
    return possession_ridge_ratings(regular, lam=RIDGE_LAMBDA[league_id])


def _load(seasons: int | list[int], league_id: str, season_type: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The situation rows and regular-season ratings for ``seasons`` (named by the year they end)."""
    _check_args(league_id, season_type)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if league_id == "10":
        from sportsdataverse.wnba import wnba_loaders as W

        args = seasons_list
        poss, pbp, sched = (
            W.load_wnba_stats_possessions(args),
            W.load_wnba_stats_pbp(args),
            W.load_wnba_stats_schedules(args),
        )
    else:
        from sportsdataverse.nba import nba_loaders as N

        args = [s - 1 for s in seasons_list]  # the NBA loaders take the start year
        poss, pbp, sched = (
            N.load_nba_stats_possessions(args),
            N.load_nba_stats_pbp(args),
            N.load_nba_stats_schedules(args),
        )
    rows = nba_situation_rows(poss, pbp, sched, league_id=league_id, season_type=season_type)
    ratings = _ratings(poss, sched, league_id) if rows.height else pl.DataFrame()
    return rows, ratings


def _top_n(top_n: int | None, league_id: str) -> int:
    """``top_n`` or the league default, checked before anything is loaded."""
    if top_n is not None and top_n < 1:
        raise ValueError(f"top_n must be at least 1, got {top_n}")
    return _TOP_N[league_id] if top_n is None else top_n


def nba_situational_team(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    top_n: int | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Team splits by situation: clutch, period, half, garbage time, score, venue, opponent, play type, chance.

    The families and their splits are in :mod:`sportsdataverse._common.situational_splits`; within
    a family the splits add up to the ``all`` row.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or
            ``"play-in"``, matched to the schedule's ``season_type``.
        top_n: How many of the season's best teams (BB-D19 regular-season ``adj_net_rtg``) are
            ``vs_top``. Default 10 (NBA) or 4 (WNBA).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, family, split)``: ``games``, ``minutes`` / ``seconds``,
        ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``, ``plus_minus``, ``off_rtg`` / ``def_rtg`` /
        ``net_rtg`` (per 100 possessions) and their ``_n``. Ids are the stats.nba.com ids (Int64).
        Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id``, ``season_type`` or ``top_n`` is invalid.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_situational import nba_situational_team
            splits = nba_situational_team(2026)
            splits.filter(split="clutch").sort("net_rtg", descending=True).head()
    """
    _check_args(league_id, season_type)
    n = _top_n(top_n, league_id)
    rows, ratings = _load(seasons, league_id, season_type)
    out = situational_team(rows, ratings, top_n=n)
    return out.to_pandas() if return_as_pandas else out


def nba_situational_player(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    top_n: int | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Player on-floor splits by situation: the team splits over the possessions each player started on the floor.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or ``"play-in"``.
        top_n: As :func:`nba_situational_team`.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id, family, split)``, the columns of
        :func:`nba_situational_team`: the team's possessions, points and ratings with the player on
        the floor as the possession starts. ``player_id`` is the stats.nba.com person id (Int64).
        Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id``, ``season_type`` or ``top_n`` is invalid.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_situational import nba_situational_player
            clutch = nba_situational_player(2026).filter(split="clutch")
            clutch.filter(clutch["net_rtg_n"] >= 200).sort("net_rtg", descending=True).head()
    """
    _check_args(league_id, season_type)
    n = _top_n(top_n, league_id)
    rows, ratings = _load(seasons, league_id, season_type)
    out = situational_player(rows, ratings, top_n=n, player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def nba_situational_lineup(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    top_n: int | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Five-man unit splits by situation: the team splits over the possessions each unit started.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or ``"play-in"``.
        top_n: As :func:`nba_situational_team`.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, lineup_id, family, split)``: ``lineup_id`` is the five
        player ids sorted and joined with ``-`` (BB-D4's id), ``player_1..5`` the same ids, then the
        columns of :func:`nba_situational_team`. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id``, ``season_type`` or ``top_n`` is invalid.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_situational import nba_situational_lineup
            units = nba_situational_lineup(2026).filter(split="transition")
            units.sort("poss", descending=True).head()
    """
    _check_args(league_id, season_type)
    n = _top_n(top_n, league_id)
    rows, ratings = _load(seasons, league_id, season_type)
    out = situational_lineup(rows, ratings, top_n=n, player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out
