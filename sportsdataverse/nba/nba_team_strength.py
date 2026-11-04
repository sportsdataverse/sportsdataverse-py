"""NBA / WNBA team strength (BB-D19): ``nba.team_strength``.

Adjusted offensive / defensive / net ratings from a possession ridge, strength of schedule and
strength of victory, with pace and the four factors for and against from BB-D3's
:func:`sportsdataverse.nba.nba_team_season_rates.nba_team_season_rates`. The definitions are in
:mod:`sportsdataverse._common.team_strength`. ``league_id="10"`` reads the WNBA releases;
:mod:`sportsdataverse.wnba.wnba_team_strength` binds it.

The ``nba_stats_possessions`` and ``nba_stats_schedules`` releases are read once: the same
possession rows feed the BB-D3 rates and the ridge, and the schedule's final scores give the
wins and the games behind SOS and SOV.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse._common.team_season_rates import team_season_rates
from sportsdataverse._common.team_strength import (
    GAMES_SCHEMA,
    RIDGE_LAMBDA,
    possession_ridge_ratings,
    team_strength,
)
from sportsdataverse.nba.nba_team_season_rates import _LEAGUES, _check_args, _league_loaders, nba_possession_rate_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_team_strength"]


def _schedule_games(schedule: pl.DataFrame, season_type: str) -> pl.DataFrame:
    """Two rows per completed game of ``season_type``: each side, its opponent and the result."""
    done = schedule.filter(
        (pl.col("season_type") == season_type) & pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null()
    )
    sides = [("home_team_id", "away_team_id", pl.col("home_pts") > pl.col("away_pts"))]
    sides.append(("away_team_id", "home_team_id", pl.col("away_pts") > pl.col("home_pts")))
    return pl.concat(
        [
            done.select(
                "season", "game_id", pl.col(team).alias("team_id"), pl.col(opp).alias("opp_team_id"), won.alias("won")
            )
            for team, opp, won in sides
        ]
    ).cast(GAMES_SCHEMA)


@overload
def nba_team_strength(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_team_strength(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_team_strength(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Team strength from possessions: adjusted ratings, SOS, SOV, pace and the four factors.

    One row per team and season. The adjusted ratings are a ridge regression of points per 100
    on each possession on offense, defense and home terms (penalty
    :data:`~sportsdataverse._common.team_strength.RIDGE_LAMBDA`, fit by season-disjoint CV):
    points scored / allowed per 100 against an average opponent on a neutral court. SOS is the
    mean opponent ``adj_net_rtg`` over the team's games, SOV the same over its wins.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or
            ``"play-in"``, matched to the schedule's ``season_type``. The ridge, the rates and the
            games all use it.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)``; ``team_id`` is the stats.nba.com team id (Int64).
        ``games``, ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts`` are the season totals from the
        possessions; ``wins`` / ``losses`` come from the schedule's final scores. The rated
        metrics are ``adj_off_rtg``, ``adj_def_rtg``, ``adj_net_rtg``, ``sos``, ``sov`` and the
        BB-D3 rates (``off_rtg``, ``def_rtg``, ``net_rtg``, ``pace``, the four factors and the
        four against). Each has ``<m>_rank`` (1 = best; ``adj_def_rtg``, ``def_rtg``,
        ``tov_pct`` and the opponent shooting, rebounding and free-throw rates rank low first;
        ``sos`` ranks the hardest schedule first), ``<m>_pct`` (the Weibull percentile) and
        ``<m>_n`` (possessions for the ratings, games for SOS and pace, wins for SOV). Ranks are
        within the season, among teams with at least half the most games, and null under five
        teams. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or ``season_type`` is not one
            of the four above (checked before any data is loaded).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_strength import nba_team_strength
            strength = nba_team_strength(2026)
            strength.sort("adj_net_rtg_rank").select("team_id", "adj_net_rtg", "sos", "sov").head()

        WNBA via ``league_id``::

            wnba = nba_team_strength(2025, league_id="10")
    """
    _check_args(league_id, season_type)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    loader_seasons = [s - offset for s in seasons_list]
    schedule = load_schedule(loader_seasons)
    rows = nba_possession_rate_rows(
        load_possessions(loader_seasons), schedule, league_id=league_id, season_type=season_type
    )
    out = team_strength(
        team_season_rates(rows, minutes_per_game=_LEAGUES[league_id][1]),
        possession_ridge_ratings(rows, lam=RIDGE_LAMBDA[league_id]),
        _schedule_games(schedule, season_type) if schedule.height else pl.DataFrame(schema=GAMES_SCHEMA),
    )
    return out.to_pandas() if return_as_pandas else out
