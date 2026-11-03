"""NBA / WNBA season team rates from possessions (BB-D3): ``nba.team_season_rates``.

Builds :func:`sportsdataverse._common.team_season_rates.team_season_rates` from the
``nba_stats_possessions`` release (the possession engine of
:mod:`sportsdataverse.nba.nba_possessions`) and the ``nba_stats_schedules`` release (home team and
season type). ``league_id="10"`` reads the WNBA twins; :mod:`sportsdataverse.wnba.wnba_team_season_rates`
binds it.

Two source rules, both checked on the 2025-26 NBA season against stats.nba.com
``team_stats_advanced``:

* ``points`` is ``2 * fg2m + 3 * fg3m + ftm``, not the possession's score-delta ``points``
  column. The two agree on 99.96% of possessions, but the score delta carries forward-fill
  glitches (game 0022500232 has a +52 possession). With the counts every team's net rating is
  within 0.5 of stats.nba.com; with the score delta the worst team is off by 1.1.
* Every possession row counts, including the end-of-period heaves that
  ``count_as_possession`` flags False (0.3% of rows). Dropping them moved the worst team further
  from stats.nba.com (0.55).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse._common.team_season_rates import RATE_ROWS_SCHEMA, TEAM_SEASON_RATES_SCHEMA, team_season_rates
from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions, load_nba_stats_schedules

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_possession_rate_rows", "nba_team_season_rates"]

#: league_id -> (quarter seconds, regulation minutes).
_LEAGUES = {"00": (720.0, 48), "10": (600.0, 40)}
#: The schedule ``season_type`` values a season table is built for.
_SEASON_TYPES = ("regular-season", "preseason", "playoffs", "play-in")


def _check_args(league_id: str, season_type: str) -> None:
    """Raise ValueError for an unknown ``league_id`` or ``season_type`` (a typo would match no games)."""
    if league_id not in _LEAGUES:
        raise ValueError(f"league_id must be one of {sorted(_LEAGUES)}, got {league_id!r}")
    if season_type not in _SEASON_TYPES:
        raise ValueError(f"season_type must be one of {list(_SEASON_TYPES)}, got {season_type!r}")


_OT_SECONDS = 300.0


def nba_possession_rate_rows(
    possessions: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
) -> pl.DataFrame:
    """Map ``nba_stats_possessions`` rows onto the team-season-rates possession frame.

    Keeps the games of ``schedule`` whose ``season_type`` matches, takes the home team from the
    schedule, and sets each possession's start clock to the previous possession's end clock in the
    period (the period length for the first one). Points come from the made-shot counts.

    Args:
        possessions: ``load_nba_stats_possessions`` / ``load_wnba_stats_possessions`` rows.
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows
            (``game_id``, ``season_type``, ``home_team_id``).
        league_id: ``"00"`` NBA (12-minute quarters) or ``"10"`` WNBA (10-minute quarters).
        season_type: The schedule ``season_type`` to keep: ``"regular-season"`` (the default,
            stats.nba.com's "Regular Season"; NBA Cup group and knockout games count, the Cup final
            does not), ``"preseason"``, ``"playoffs"`` or ``"play-in"``.

    Returns:
        One row per possession in
        :data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or ``season_type`` is not one
            of the four above.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions, load_nba_stats_schedules
            from sportsdataverse.nba.nba_team_season_rates import nba_possession_rate_rows
            rows = nba_possession_rate_rows(load_nba_stats_possessions(2025), load_nba_stats_schedules(2025))
    """
    _check_args(league_id, season_type)
    quarter, _ = _LEAGUES[league_id]
    if possessions.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema=RATE_ROWS_SCHEMA)
    games = schedule.filter(pl.col("season_type") == season_type).select(
        pl.col("game_id").cast(pl.Utf8), pl.col("home_team_id").cast(pl.Int64)
    )
    poss = possessions.with_columns(pl.col("game_id").cast(pl.Utf8))
    assert poss.schema["game_id"] == games.schema["game_id"], "game_id dtype differs between possessions and schedule"
    period_len = pl.when(pl.col("period") <= 4).then(pl.lit(quarter)).otherwise(pl.lit(_OT_SECONDS))
    return (
        poss.join(games, on="game_id", how="inner", validate="m:1")
        .sort(["game_id", "period", "possession_number"])
        .with_columns(
            pl.col("end_seconds_remaining")
            .shift(1)
            .over(["game_id", "period"])
            .fill_null(period_len)
            .alias("start_clock"),
        )
        .select(
            pl.col("season").cast(pl.Int64),
            "game_id",
            pl.col("period").cast(pl.Int64),
            pl.col("possession_number").cast(pl.Int64),
            (pl.col("period") >= 4).alias("late"),
            "start_clock",
            pl.col("end_seconds_remaining").cast(pl.Float64).alias("end_clock"),
            pl.col("offense_team_id").cast(pl.Int64),
            pl.col("defense_team_id").cast(pl.Int64),
            (pl.col("offense_team_id").cast(pl.Int64) == pl.col("home_team_id")).alias("offense_is_home"),
            pl.lit(False).alias("neutral_site"),
            (2 * pl.col("fg2m") + 3 * pl.col("fg3m") + pl.col("ftm")).cast(pl.Int64).alias("points"),
            (pl.col("fg2a") + pl.col("fg3a")).alias("fga"),
            (pl.col("fg2m") + pl.col("fg3m")).alias("fgm"),
            "fg3m",
            "fta",
            "oreb",
            "dreb",
            "tov",
            pl.lit(1.0).alias("ff_poss"),
        )
        .cast(RATE_ROWS_SCHEMA)
    )


def _league_loaders(league_id: str):  # type: ignore[no-untyped-def]
    """(possessions, schedule) loaders and the loader season offset for a ``league_id``.

    The NBA loaders take the season's START year; the WNBA ones take the year itself.
    """
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_possessions, load_wnba_stats_schedules

        return load_wnba_stats_possessions, load_wnba_stats_schedules, 0
    return load_nba_stats_possessions, load_nba_stats_schedules, 1


@overload
def nba_team_season_rates(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_team_season_rates(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_team_season_rates(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Season team rates from possessions: ORtg/DRtg/net, pace and the four factors, with splits.

    One row per team, season and split (``all``, ``home``, ``away``, ``close``, ``clutch``).
    Clutch is a possession that starts with 5:00 or less left in the 4th quarter or an overtime,
    the score within 5 (stats.nba.com's clutch window); a close game is one with a clutch
    possession. The full definitions are in :mod:`sportsdataverse._common.team_season_rates`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or ``"play-in"``, matched to
            the schedule's ``season_type``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, split)``. ``team_id`` is the stats.nba.com team id
        (Int64). ``games``, ``poss`` / ``opp_poss`` (offensive / defensive possessions) and
        ``pts`` / ``opp_pts`` are the split's totals. The twelve rates are ``off_rtg``,
        ``def_rtg``, ``net_rtg`` (per 100 possessions), ``pace`` (possessions per 48 minutes,
        40 in the WNBA), the four factors ``efg_pct``, ``tov_pct``, ``orb_pct``, ``ft_rate`` and
        the same four against (``opp_*``), all fractions. Each rate has ``<m>_rank`` (1 = best;
        ``def_rtg``, ``tov_pct``, ``opp_efg_pct``, ``opp_orb_pct`` and ``opp_ft_rate`` rank low
        first; pace ranks high first), ``<m>_pct`` (the Weibull percentile) and ``<m>_n`` (the
        sample: possessions for the ratings, games for pace, field-goal attempts for eFG% and
        FT rate, possessions for turnover rate, rebound chances for ORB%). Ranks are within the
        season and split, among teams with at least half the most games, and null under five
        teams. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or ``season_type`` is not one
            of the four above.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_season_rates import nba_team_season_rates
            rates = nba_team_season_rates(2026)
            rates.filter(split="all").sort("net_rtg_rank").head()

        WNBA via ``league_id``::

            wnba = nba_team_season_rates(2025, league_id="10")
    """
    _check_args(league_id, season_type)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    loader_seasons = [s - offset for s in seasons_list]
    rows = nba_possession_rate_rows(
        load_possessions(loader_seasons), load_schedule(loader_seasons), league_id=league_id, season_type=season_type
    )
    out = (
        team_season_rates(rows, minutes_per_game=_LEAGUES[league_id][1])
        if rows.height
        else pl.DataFrame(schema=TEAM_SEASON_RATES_SCHEMA)
    )
    return out.to_pandas() if return_as_pandas else out
