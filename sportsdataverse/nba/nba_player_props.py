"""Player-prop distributions + team pace projection (model ⑥) for the NBA stack.

Per-player rate stats from box logs, projected onto a matchup's expected
minutes and pace, returning a mean and a distribution so ``P(stat > line)`` is
directly computable. Counts (reb/ast/fg3m) use a Negative-Binomial (Poisson
fallback when dispersion ~ mean); points use a Normal.

Rate basis (ponytail): rates are per-MINUTE, not per-possession. The box-log
fixtures carry per-player minutes but not per-game team possessions, so a
per-minute rate times projected minutes times a pace factor
(``exp_poss / avg_pace``) is the pace-aware projection without needing a
possession column the loader doesn't provide. `# ponytail: per-minute x pace
factor captures the pace linkage without a per-game team-possession join.`

Fitted-constant note: the Normal points SD and the count-dispersion are seeded
(``_PTS_SD_A/_B``, ``_COUNT_DISPERSION``) from the box-log residuals in
``dev/nba_prediction/fit_player_props.py``; refit there if the Phase-5 gate
floors move.
"""

from __future__ import annotations

import math
from typing import Any, Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl
import scipy.stats as st

from sportsdataverse.nba.nba_game_predict import expected_possessions
from sportsdataverse.nba.nba_prediction_constants import get_constants

__all__ = [
    "nba_player_props",
    "player_rates",
    "project_player_line",
    "prob_over",
    "prop_distribution",
    "team_pace_projection",
]

_COUNT_STATS = ("reb", "ast", "fg3m")
# points SD law sd = a + b*sqrt(mu), FITTED (dev/nba_prediction/fit_player_props.py,
# 2026-07-08: least squares of per-player realized pts-SD on sqrt(mean) over 320
# rotation players).
_PTS_SD_A = 1.406
_PTS_SD_B = 1.396
# count over-dispersion var/mean for the Negative-Binomial (> 1 -> NB, ~1 -> Poisson),
# FITTED: median var/mean over rotation players is reb 1.33 / ast 1.25 / fg3m 1.13; the
# shared value below is their mean.
_COUNT_DISPERSION = 1.24

_RATES_SCHEMA = {
    "player_id": pl.Utf8,
    "team_id": pl.Utf8,
    "games": pl.Int64,
    "minutes_pg": pl.Float64,
    "pts_per_min": pl.Float64,
    "reb_per_min": pl.Float64,
    "ast_per_min": pl.Float64,
    "fg3m_per_min": pl.Float64,
}


def player_rates(box_logs: pl.DataFrame) -> pl.DataFrame:
    """Per-player per-minute rate stats from box logs.

    Rows with null minutes (DNPs) are dropped. Rate = total stat / total
    minutes across the player's games; ``minutes_pg`` is the mean minutes.

    Args:
        box_logs: Per-player-per-game frame with ``player_id, team_id,
            minutes, pts, reb, ast, fg3m``.

    Returns:
        One row per player: ``player_id, team_id, games, minutes_pg,
        pts_per_min, reb_per_min, ast_per_min, fg3m_per_min``. Empty input
        returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import player_rates
            rates = player_rates(box_logs)
    """
    if box_logs.height == 0:
        return pl.DataFrame(schema=_RATES_SCHEMA)
    df = box_logs.drop_nulls("minutes").filter(pl.col("minutes") > 0)
    return (
        df.group_by("player_id")
        .agg(
            pl.col("team_id").last(),
            pl.len().alias("games"),
            pl.col("minutes").mean().alias("minutes_pg"),
            (pl.col("pts").sum() / pl.col("minutes").sum()).alias("pts_per_min"),
            (pl.col("reb").sum() / pl.col("minutes").sum()).alias("reb_per_min"),
            (pl.col("ast").sum() / pl.col("minutes").sum()).alias("ast_per_min"),
            (pl.col("fg3m").sum() / pl.col("minutes").sum()).alias("fg3m_per_min"),
        )
        .select(list(_RATES_SCHEMA))
        .sort("player_id")
    )


def project_player_line(rate_row: dict[str, Any], exp_minutes: float, pace_factor: float = 1.0) -> dict[str, float]:
    """Project a player's expected counting line from per-minute rates.

    ``exp_stat = rate_per_min * exp_minutes * pace_factor`` -- counting stats
    scale with both projected minutes and pace.

    Args:
        rate_row: One row of :func:`player_rates` (as a dict).
        exp_minutes: Projected minutes for the game.
        pace_factor: Pace multiplier (``exp_poss / avg_pace``); ``1.0`` for a
            league-average-pace matchup.

    Returns:
        ``{"exp_pts", "exp_reb", "exp_ast", "exp_fg3m"}``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import player_rates, project_player_line
            r = player_rates(box_logs).row(0, named=True)
            line = project_player_line(r, exp_minutes=32.0, pace_factor=1.02)
    """
    m = exp_minutes * pace_factor
    return {
        "exp_pts": float(rate_row["pts_per_min"] * m),
        "exp_reb": float(rate_row["reb_per_min"] * m),
        "exp_ast": float(rate_row["ast_per_min"] * m),
        "exp_fg3m": float(rate_row["fg3m_per_min"] * m),
    }


def prop_distribution(exp_value: float, stat: str, *, league_id: str = "00") -> tuple[str, dict[str, float]]:
    """Distribution family + parameters for a projected stat mean.

    Points -> Normal ``(mu, sd)`` with ``sd = a + b*sqrt(mu)``; counts
    (reb/ast/fg3m) -> Negative-Binomial ``(r, p)`` matching mean ``mu`` and
    variance ``dispersion*mu`` (Poisson if dispersion <= 1).

    Args:
        exp_value: Projected mean of the stat.
        stat: One of ``"pts"``, ``"reb"``, ``"ast"``, ``"fg3m"``.
        league_id: Accepted for parity (dispersion is currently league-shared).

    Returns:
        ``(family, params)`` where family is ``"normal"``, ``"nbinom"`` or
        ``"poisson"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import prop_distribution
            fam, par = prop_distribution(24.0, "pts")
    """
    mu = max(float(exp_value), 1e-6)
    if stat == "pts":
        return "normal", {"mu": mu, "sd": _PTS_SD_A + _PTS_SD_B * math.sqrt(mu)}
    var = _COUNT_DISPERSION * mu
    if var <= mu * 1.0001:  # not over-dispersed -> Poisson
        return "poisson", {"mu": mu}
    # NB parameterization: var = mu + mu^2 / r  => r = mu^2 / (var - mu); p = r / (r + mu)
    r = mu * mu / (var - mu)
    p = r / (r + mu)
    return "nbinom", {"r": r, "p": p}


def prob_over(exp_value: float, line: float, stat: str, *, league_id: str = "00") -> float:
    """Probability a stat finishes strictly above ``line``.

    Args:
        exp_value: Projected mean of the stat.
        line: The prop line.
        stat: One of ``"pts"``, ``"reb"``, ``"ast"``, ``"fg3m"``.
        league_id: Accepted for parity.

    Returns:
        ``P(stat > line)`` in ``[0, 1]``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import prob_over
            prob_over(24.0, 22.5, "pts")
    """
    fam, par = prop_distribution(exp_value, stat, league_id=league_id)
    if fam == "normal":
        return float(1.0 - st.norm.cdf(line, loc=par["mu"], scale=par["sd"]))
    floor_line = int(np.floor(line))
    if fam == "poisson":
        return float(st.poisson.sf(floor_line, par["mu"]))
    return float(st.nbinom.sf(floor_line, par["r"], par["p"]))


def team_pace_projection(
    home_team_id: str, away_team_id: str, ratings: pl.DataFrame, *, league_id: str = "00"
) -> float:
    """Expected possessions for a matchup (Phase-3 :func:`expected_possessions`).

    Args:
        home_team_id: Home team id (matched against ``ratings['team_id']``).
        away_team_id: Away team id.
        ratings: One row per team with ``team_id, adj_pace``.
        league_id: ``"00"``/``"10"``/``"20"``.

    Returns:
        Expected possessions for the game.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import team_pace_projection
            poss = team_pace_projection("1", "2", ratings)
    """
    pace = dict(zip(ratings["team_id"].to_list(), ratings["adj_pace"].to_list()))
    return expected_possessions(float(pace[home_team_id]), float(pace[away_team_id]), league_id=league_id)


def _load_player_logs(season: int, league_id: str) -> pl.DataFrame:  # pragma: no cover - live network
    """Live player box logs (monkeypatched to fixtures in tests); WNBA via league_id=10."""
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_player_boxscore  # noqa: PLC0415

        box = load_wnba_player_boxscore([season])
    else:
        from sportsdataverse.nba.nba_loaders import load_nba_player_boxscore  # noqa: PLC0415

        box = load_nba_player_boxscore([season])
    return box.select(
        pl.col("athlete_id").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
        pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("minutes").cast(pl.Float64),
        pl.col("points").cast(pl.Float64).alias("pts"),
        pl.col("rebounds").cast(pl.Float64).alias("reb"),
        pl.col("assists").cast(pl.Float64).alias("ast"),
        pl.col("three_point_field_goals_made").cast(pl.Float64).alias("fg3m"),
    )


def _load_ratings(season: int, league_id: str) -> pl.DataFrame:  # pragma: no cover - live network
    """Live team ratings for pace (monkeypatched to fixtures in tests)."""
    from sportsdataverse.nba.nba_team_ratings import nba_team_ratings  # noqa: PLC0415

    return nba_team_ratings(season, league_id=league_id)


_PROPS_SCHEMA = {
    "player_id": pl.Utf8,
    "team_id": pl.Utf8,
    "stat_pts_exp": pl.Float64,
    "stat_reb_exp": pl.Float64,
    "stat_ast_exp": pl.Float64,
    "stat_fg3m_exp": pl.Float64,
    "pace_proj": pl.Float64,
}


@overload
def nba_player_props(
    season: int,
    game_id: str,
    home_team_id: str,
    away_team_id: str,
    *,
    league_id: str = "00",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_player_props(
    season: int,
    game_id: str,
    home_team_id: str,
    away_team_id: str,
    *,
    league_id: str = "00",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_player_props(
    season: int,
    game_id: str,
    home_team_id: str,
    away_team_id: str,
    *,
    league_id: str = "00",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Per-player expected prop lines + team pace projection for a matchup.

    Loads the season's player box logs + team ratings, computes per-minute
    :func:`player_rates`, and projects each player's line onto their mean
    minutes and the matchup's pace factor (``exp_poss / avg_pace``). Only the
    two teams in the matchup are returned.

    Args:
        season: End year of the season (e.g. ``2024``).
        game_id: The game id (passed through for the caller's join; not used to
            filter historical rates).
        home_team_id: Home team id.
        away_team_id: Away team id.
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G-League.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per player on either team: ``player_id, team_id, stat_pts_exp,
        stat_reb_exp, stat_ast_exp, stat_fg3m_exp, pace_proj``. Empty input
        returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_props import nba_player_props
            props = nba_player_props(2024, "401585828", "2", "6")
    """
    logs = _load_player_logs(season, league_id)
    ratings = _load_ratings(season, league_id)
    rates = player_rates(logs)
    if rates.height == 0 or ratings.height == 0:
        out = pl.DataFrame(schema=_PROPS_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    pace_proj = team_pace_projection(home_team_id, away_team_id, ratings, league_id=league_id)
    pace_factor = pace_proj / get_constants(league_id).avg_pace
    rates = rates.filter(pl.col("team_id").is_in([home_team_id, away_team_id]))
    out = (
        rates.with_columns(
            (pl.col("pts_per_min") * pl.col("minutes_pg") * pace_factor).alias("stat_pts_exp"),
            (pl.col("reb_per_min") * pl.col("minutes_pg") * pace_factor).alias("stat_reb_exp"),
            (pl.col("ast_per_min") * pl.col("minutes_pg") * pace_factor).alias("stat_ast_exp"),
            (pl.col("fg3m_per_min") * pl.col("minutes_pg") * pace_factor).alias("stat_fg3m_exp"),
            pl.lit(pace_proj, dtype=pl.Float64).alias("pace_proj"),
        )
        .select(list(_PROPS_SCHEMA))
        .sort("player_id")
    )
    return out.to_pandas() if return_as_pandas else out
