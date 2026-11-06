"""College team strength (BB-D19): ``mbb.team_strength`` / ``wbb.team_strength``.

Two adjusted-rating methods, side by side:

* ``adj_off_rtg`` / ``adj_def_rtg`` / ``adj_net_rtg``: the published ``mbb_ratings`` /
  ``wbb_ratings`` (the :mod:`sportsdataverse.mbb.mbb_team_ratings` fixed point), ``adj_o`` /
  ``adj_d`` / ``adj_em`` unchanged. Strength of schedule and strength of victory use these.
* ``adj_off_rtg_ridge`` / ``adj_def_rtg_ridge`` / ``adj_net_rtg_ridge``: the NBA / WNBA
  possession ridge (:func:`sportsdataverse._common.team_strength.possession_ridge_ratings`) on the
  same stats.ncaa.org possessions that feed the rates, penalty
  :data:`~sportsdataverse._common.team_strength.COLLEGE_RIDGE_LAMBDA`.

Neither method replaces the other. On the 2024-2026 as-of holdout (``tools/compare_college_team_strength.py``)
the ridge does better in the first four weeks (clearly for women's, by 1.7 standard errors for men's);
after that the men's fixed point is better and the women's two tie. Season-end rankings agree to a
Spearman of 0.99 (men's) and 0.998 (women's).

Strength of schedule and strength of victory are averaged over ESPN's completed games, pace and the
four factors come from BB-D3's :func:`sportsdataverse.mbb.mbb_team_season_rates.mbb_team_season_rates`.
The definitions are in :mod:`sportsdataverse._common.team_strength`.

``team_id`` is the ESPN team id (Int64), as in BB-D3; the ratings release's text id is cast to
it. The ESPN schedule is read once, for BB-D3's neutral sites and for the results.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import polars as pl

from sportsdataverse._common.team_season_rates import team_season_rates
from sportsdataverse._common.team_strength import (
    COLLEGE_RIDGE_LAMBDA,
    GAMES_SCHEMA,
    RATINGS_SCHEMA,
    possession_ridge_ratings,
    team_strength,
)
from sportsdataverse.mbb.mbb_loaders import load_mbb_ratings
from sportsdataverse.mbb.mbb_team_season_rates import (
    _check_league,
    _league_loaders,
    ncaa_possession_rate_rows,
    ncaa_team_box_count_rows,
)

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_team_strength"]


def _load_ratings(league: str) -> Callable[..., Any]:
    """The ratings loader for ``league``: ``load_wbb_ratings`` for ``"womens"``, else ``load_mbb_ratings``."""
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_ratings

        return load_wbb_ratings
    return load_mbb_ratings


def _ratings(released: pl.DataFrame) -> pl.DataFrame:
    """The ratings release in :data:`~sportsdataverse._common.team_strength.RATINGS_SCHEMA`."""
    return released.select(
        "season",
        "team_id",
        pl.col("adj_o").alias("adj_off_rtg"),
        pl.col("adj_d").alias("adj_def_rtg"),
        pl.col("adj_em").alias("adj_net_rtg"),
    ).cast(RATINGS_SCHEMA)


def _schedule_games(schedule: pl.DataFrame) -> pl.DataFrame:
    """Two rows per completed ESPN game: each side, its opponent and the result."""
    done = schedule.unique("game_id", keep="first", maintain_order=True).filter(
        (pl.col("status_type_completed") == True)  # noqa: E712
        & pl.col("home_score").is_not_null()
        & pl.col("away_score").is_not_null()
    )
    sides = [
        ("home_id", "away_id", pl.col("home_score") > pl.col("away_score")),
        ("away_id", "home_id", pl.col("away_score") > pl.col("home_score")),
    ]
    return pl.concat(
        [
            done.select(
                "season", "game_id", pl.col(team).alias("team_id"), pl.col(opp).alias("opp_team_id"), won.alias("won")
            ).cast(GAMES_SCHEMA)
            for team, opp, won in sides
        ]
    )


def mbb_team_strength(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College team strength: adjusted ratings, SOS, SOV, tempo and the four factors.

    One row per team and season, with two adjusted-rating methods. ``adj_*_rtg`` are the
    published college ratings (``load_mbb_ratings`` / ``load_wbb_ratings``, a fixed point on ESPN
    box scores). ``adj_*_rtg_ridge`` are the NBA / WNBA possession ridge on the stats.ncaa.org
    possessions (points per 100 against an average Division I opponent on a neutral court; home
    court fitted, neutral sites from ESPN, non-Division I opponents left out). SOS is the mean
    opponent ``adj_net_rtg`` over the team's completed ESPN games (unrated opponents left out),
    SOV the same over its wins. Bracketology and the season sims keep the fixed point.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_team_strength.wbb_team_strength`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)``; ``team_id`` is the ESPN team id (Int64). ``games``,
        ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts`` are the season totals from the NCAA
        possessions; ``wins`` / ``losses`` come from ESPN's completed games. The rated metrics
        are ``adj_off_rtg``, ``adj_def_rtg``, ``adj_net_rtg``, ``sos``, ``sov``,
        ``adj_off_rtg_ridge``, ``adj_def_rtg_ridge``, ``adj_net_rtg_ridge`` and the BB-D3
        rates (``off_rtg``, ``def_rtg``, ``net_rtg``, ``pace``, the four factors and the four
        against). Each has ``<m>_rank`` (1 = best; ``adj_def_rtg``, ``adj_def_rtg_ridge``,
        ``def_rtg``, ``tov_pct`` and the opponent shooting, rebounding and free-throw rates rank low first; ``sos`` ranks the
        hardest schedule first), ``<m>_pct`` (the Weibull percentile) and ``<m>_n``
        (possessions for the ratings, rated games for SOS, games for pace, wins over rated
        opponents for SOV). Ranks are within the season, among teams with at least half the
        most games (which leaves out the non-D1 opponents), and null under five teams. Empty
        input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_team_strength import mbb_team_strength
            strength = mbb_team_strength(2025)
            strength.sort("adj_net_rtg_rank").select("team_id", "adj_net_rtg", "sos", "sov").head()

        The two methods side by side::

            strength.select("team_id", "adj_net_rtg_rank", "adj_net_rtg_ridge_rank").head()
    """
    _check_league(league)
    load_possessions, load_team_box, load_schedule = _league_loaders(league)
    schedule = load_schedule(seasons)
    rows = ncaa_possession_rate_rows(load_possessions(seasons), schedule, league=league)
    rates = team_season_rates(rows, minutes_per_game=40, counts=ncaa_team_box_count_rows(load_team_box(seasons)))
    games = _schedule_games(schedule) if schedule.height else pl.DataFrame(schema=GAMES_SCHEMA)
    ridge = possession_ridge_ratings(rows, lam=COLLEGE_RIDGE_LAMBDA[league])
    out = team_strength(rates, _ratings(_load_ratings(league)(seasons)), games, ridge=ridge)
    return out.to_pandas() if return_as_pandas else out
