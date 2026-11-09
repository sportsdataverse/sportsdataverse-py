"""College season team rates from possessions (BB-D3): ``mbb.team_season_rates`` / ``wbb.team_season_rates``.

Builds :func:`sportsdataverse._common.team_season_rates.team_season_rates` from three releases:

* ``ncaa_{mbb,wbb}_possessions`` -- the stats.ncaa.org possessions (the cbb-explorer port of
  :mod:`sportsdataverse.mbb.mbb_ncaa_possessions`): who had the ball, its points, and its length
  in seconds (``last_event_time``). The time left when a possession starts is the sum of its
  length and the lengths of the possessions after it in the period.
* ``ncaa_{mbb,wbb}_team_box`` -- the same pipeline's per-game team totals, for the four factors.
  The possession rows carry no shot or rebound counts, so the four factors are game-level: they
  fill the ``all`` / ``home`` / ``away`` / ``close`` splits and are null for ``clutch``. The
  turnover rate uses the box's own possession count (``o_poss``) as its denominator.
* the ESPN schedule -- ``neutral_site``, by ``espn_game_id``. A neutral-site game is in neither
  the home nor the away split. A game ESPN does not carry (no ``espn_game_id``) keeps the
  possessions' nominal home and away.

``team_id`` is the ESPN team id (Int64), the id the rest of the ESPN college surface uses. A
non-D1 opponent has none, so it gets no row; its points still count in the game score.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.rate_gates import NCAA_LEVEL
from sportsdataverse._common.team_season_rates import (
    COUNT_ROWS_SCHEMA,
    RATE_ROWS_SCHEMA,
    TEAM_SEASON_RATES_SCHEMA,
    team_season_rates,
)
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_possessions, load_ncaa_mbb_team_box

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_team_season_rates", "ncaa_possession_rate_rows", "ncaa_team_box_count_rows"]

_LEAGUES = ("mens", "womens")
#: Women's college basketball has played 10-minute quarters since 2015-16 (ESPN season 2016).
_WBB_FIRST_QUARTERS_SEASON = 2016


def _final_period(league: str) -> pl.Expr:
    """The last regulation period for each row's season: the 2nd half, or the 4th quarter."""
    if league == "mens":
        return pl.lit(2)
    return pl.when(pl.col("season") >= _WBB_FIRST_QUARTERS_SEASON).then(4).otherwise(2)


def _check_league(league: str) -> None:
    if league not in _LEAGUES:
        raise ValueError(f"league must be one of {list(_LEAGUES)}, got {league!r}")


def ncaa_possession_rate_rows(
    possessions: pl.DataFrame, schedule: pl.DataFrame, *, league: str = "mens"
) -> pl.DataFrame:
    """Map ``ncaa_{mbb,wbb}_possessions`` rows onto the team-season-rates possession frame.

    Args:
        possessions: ``load_ncaa_mbb_possessions`` / ``load_ncaa_wbb_possessions`` rows.
        schedule: ``load_mbb_schedule`` / ``load_wbb_schedule`` rows (``game_id``,
            ``neutral_site``), joined on ``espn_game_id``.
        league: ``"mens"`` (two 20-minute halves) or ``"womens"`` (10-minute quarters from 2016,
            halves before).

    Returns:
        One row per possession in
        :data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`, with null four-factor
        counts.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_ncaa_mbb_possessions
            from sportsdataverse.mbb.mbb_team_season_rates import ncaa_possession_rate_rows
            rows = ncaa_possession_rate_rows(load_ncaa_mbb_possessions(2025), load_mbb_schedule(2025))
    """
    _check_league(league)
    if possessions.height == 0:
        return pl.DataFrame(schema=RATE_ROWS_SCHEMA)
    neutral = schedule.select(
        pl.col("game_id").cast(pl.Int64).alias("espn_game_id"), pl.col("neutral_site").cast(pl.Boolean)
    ).unique("espn_game_id")
    poss = possessions.with_columns(pl.col("espn_game_id").cast(pl.Int64))
    assert poss.schema["espn_game_id"] == neutral.schema["espn_game_id"], "espn_game_id dtype differs"
    duration = pl.col("last_event_time").cast(pl.Float64).fill_null(0.0)
    is_home = pl.col("poss_team") == pl.col("home")
    return (
        poss.join(neutral, on="espn_game_id", how="left", validate="m:1")
        .sort(["contest_id", "period", "poss_num"])
        # The clock runs back from the end of the period: the time left when a possession starts
        # is the length of it and every later possession in the period. A period whose lengths do
        # not add up to its length is then off at its start, not at its end, where clutch is.
        .with_columns(duration.cum_sum(reverse=True).over(["contest_id", "period"]).alias("start_clock"))
        .select(
            pl.col("season").cast(pl.Int64),
            pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
            pl.col("period").cast(pl.Int64),
            pl.col("poss_num").cast(pl.Int64).alias("possession_number"),
            (pl.col("period") >= _final_period(league)).alias("late"),
            "start_clock",
            (pl.col("start_clock") - duration).alias("end_clock"),
            pl.col("poss_team_espn_team_id").cast(pl.Int64).alias("offense_team_id"),
            pl.when(is_home)
            .then(pl.col("away_espn_team_id"))
            .otherwise(pl.col("home_espn_team_id"))
            .cast(pl.Int64)
            .alias("defense_team_id"),
            is_home.alias("offense_is_home"),
            pl.col("neutral_site").fill_null(False),
            pl.col("pts").cast(pl.Int64).alias("points"),
            *[pl.lit(None, dtype=pl.Float64).alias(c) for c in ("fga", "fgm", "fg3m", "fta", "oreb", "dreb", "tov")],
            pl.lit(None, dtype=pl.Float64).alias("ff_poss"),
        )
        .cast(RATE_ROWS_SCHEMA)
    )


def ncaa_team_box_count_rows(team_box: pl.DataFrame) -> pl.DataFrame:
    """Map ``ncaa_{mbb,wbb}_team_box`` rows onto the four-factor count frame.

    Each row is one team's offense in one game: its FGA, FGM, 3PM, FTA, offensive rebounds and
    turnovers, the opponent's defensive rebounds (``d_drb``) and its own possessions (``o_poss``).
    ``offense_is_home`` is the box's own home designation (``team == home``).

    Args:
        team_box: ``load_ncaa_mbb_team_box`` / ``load_ncaa_wbb_team_box`` rows.

    Returns:
        One row per (game, team) in
        :data:`~sportsdataverse._common.team_season_rates.COUNT_ROWS_SCHEMA`.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_team_box
            from sportsdataverse.mbb.mbb_team_season_rates import ncaa_team_box_count_rows
            counts = ncaa_team_box_count_rows(load_ncaa_mbb_team_box(2025))
    """
    if team_box.height == 0:
        return pl.DataFrame(schema=COUNT_ROWS_SCHEMA)
    is_home = pl.col("team") == pl.col("home")
    return team_box.select(
        pl.col("season").cast(pl.Int64),
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.col("team_espn_team_id").cast(pl.Int64).alias("offense_team_id"),
        pl.when(is_home)
        .then(pl.col("away_espn_team_id"))
        .otherwise(pl.col("home_espn_team_id"))
        .cast(pl.Int64)
        .alias("defense_team_id"),
        is_home.alias("offense_is_home"),
        "fga",
        "fgm",
        pl.col("tpm").alias("fg3m"),
        "fta",
        pl.col("orb").alias("oreb"),
        pl.col("d_drb").alias("dreb"),
        pl.col("to").alias("tov"),
        pl.col("o_poss").alias("ff_poss"),
    ).cast(COUNT_ROWS_SCHEMA)


def _league_loaders(league: str):  # type: ignore[no-untyped-def]
    """(possessions, team box, schedule) loaders -- the mens core serves wbb too."""
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_possessions, load_ncaa_wbb_team_box, load_wbb_schedule

        return load_ncaa_wbb_possessions, load_ncaa_wbb_team_box, load_wbb_schedule
    return load_ncaa_mbb_possessions, load_ncaa_mbb_team_box, load_mbb_schedule


def mbb_team_season_rates(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College season team rates from possessions: ORtg/DRtg/net, tempo and the four factors, with splits.

    One row per team, season and split (``all``, ``home``, ``away``, ``close``, ``clutch``).
    Clutch is a possession in play with 5:00 or less left (starting there, or the one live at the
    5:00 mark) in the 2nd half (the 4th quarter in women's basketball from 2016) or an overtime, the
    score within 5 as it starts; a close game is one with a clutch possession. The full definitions
    are in :mod:`sportsdataverse._common.team_season_rates`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_team_season_rates.wbb_team_season_rates`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, split)``. ``team_id`` is the ESPN team id (Int64).
        ``games``, ``poss`` / ``opp_poss`` (offensive / defensive possessions) and ``pts`` /
        ``opp_pts`` are the split's totals. The twelve rates are ``off_rtg``, ``def_rtg``,
        ``net_rtg`` (per 100 possessions), ``pace`` (possessions per 40 minutes), the four
        factors ``efg_pct``, ``tov_pct``, ``orb_pct``, ``ft_rate`` and the same four against
        (``opp_*``), all fractions. The four factors are game-level (the NCAA team box) and null
        in the ``clutch`` split. Each rate has ``<m>_rank`` (1 = best; ``def_rtg``,
        ``tov_pct``, ``opp_efg_pct``, ``opp_orb_pct`` and ``opp_ft_rate`` rank low first; pace
        ranks high first), ``<m>_pct`` (the Weibull percentile) and ``<m>_n`` (the sample:
        possessions for the ratings, the clock in regulation games for pace, field-goal attempts
        for eFG% and FT rate, the box possessions for turnover rate, rebound chances for ORB%).
        Ranks are within the season and split, among teams with at least half the most games
        (which leaves out the non-D1 opponents) whose ``<m>_n`` reaches the league's
        :data:`~sportsdataverse._common.rate_gates.TEAM_MIN_N` gate, and null under five teams.
        Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_team_season_rates import mbb_team_season_rates
            rates = mbb_team_season_rates(2025)
            rates.filter(split="all").sort("net_rtg_rank").head()
    """
    _check_league(league)
    load_possessions, load_team_box, load_schedule = _league_loaders(league)
    rows = ncaa_possession_rate_rows(load_possessions(seasons), load_schedule(seasons), league=league)
    out = (
        team_season_rates(
            rows, minutes_per_game=40, counts=ncaa_team_box_count_rows(load_team_box(seasons)), level=NCAA_LEVEL[league]
        )
        if rows.height
        else pl.DataFrame(schema=TEAM_SEASON_RATES_SCHEMA)
    )
    return out.to_pandas() if return_as_pandas else out
