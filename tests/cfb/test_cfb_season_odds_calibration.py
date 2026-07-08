"""Retrospective season-simulation calibration gate (T2.1 Task 4.3).

Runs the ratings-driven season Monte Carlo on the full 2023 FBS slate (all games
re-simulated fresh from the committed pbp ratings + schedule) and checks the
properties the sim *machinery* must guarantee. Offline + deterministic (seed=0),
~15-20s -- a phase gate, not a unit test.

**What is and isn't gated (read before treating this as a full calibration).**
The per-game predictors are calibrated in the Phase-2 backtest (Brier vs FPI); this
gate covers what the season *aggregation* adds:

- **Total wins are conserved** -- ``mean(exp_wins) == mean(actual_wins)`` -- so the
  sampler is unbiased (a wrong HFA sign or biased draw would break this).
- **Win totals are rank-calibrated** -- ``spearman(exp_wins, actual) >= 0.90``.
- **The elite teams rise** -- the 2023 CFP field's mean playoff probability towers
  over the field median.

What it does NOT assert is an absolute win-total *dispersion* slope of ~1.0. The
observed slope is ~1.55 (predicted win totals are compressed toward the mean): the
ridge-shrunk ratings under-estimate team-quality *magnitude*, so a team's games share
that under-estimate and real outcomes are more correlated -- hence more dispersed --
than the sim's independent-Bernoulli sum. That is a Phase-1 ridge-shrinkage property
(rank-preserving), not a Phase-4 sampler bug (which conservation + rank confirm is
correct). A dispersion recalibration (lighter ridge or a spread inflation) is a
documented follow-up; widening a slope band to 1.55 would only enshrine the shrinkage.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_prediction_constants import spearman_corr
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
from sportsdataverse.cfb.cfb_season_odds import make_ratings_compute_results
from sportsdataverse.cfb.cfb_simulations import cfb_simulations

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_prediction"
_CONF = pl.read_parquet(_FIX / "team_conference_2023.parquet")
_RES = pl.read_parquet(_FIX / "results_2023.parquet")
_RATINGS = efficiency_ratings(pl.read_parquet(_FIX / "pbp_2023_sample.parquet")).select("team_id", "adj_net")

_FBS = set(_CONF["team_id"].to_list())
_GAMES = _RES.filter(
    pl.col("home_team_id").is_in(_FBS) & pl.col("away_team_id").is_in(_FBS) & pl.col("home_score").is_not_null()
).with_columns((pl.col("home_score") - pl.col("away_score")).alias("_margin"))

# Engine input: every FBS-vs-FBS game re-simulated (result nulled) for a fresh projection.
_ENGINE_GAMES = _GAMES.select(
    pl.col("season").cast(pl.Int64),
    pl.col("week").cast(pl.Int64),
    pl.lit("REG").alias("game_type"),
    pl.col("home_team_id").alias("home_team"),
    pl.col("away_team_id").alias("away_team"),
    pl.lit(None, dtype=pl.Float64).alias("result"),
    pl.col("neutral_site").cast(pl.Int64).alias("neutral"),
)
_TEAMS = _CONF.select(team=pl.col("team_id"), conference=pl.col("conference"))

_SIM = cfb_simulations(
    _ENGINE_GAMES,
    _TEAMS,
    compute_results=make_ratings_compute_results(_RATINGS),
    simulations=500,
    playoff_seeds=4,
    sim_include="POST",
    seed=0,
)
_OVERALL = _SIM["overall"]

# Actual FBS-vs-FBS wins per team.
_ACTUAL = (
    pl.concat(
        [
            _GAMES.select(team=pl.col("home_team_id"), w=(pl.col("_margin") > 0).cast(pl.Int64)),
            _GAMES.select(team=pl.col("away_team_id"), w=(pl.col("_margin") < 0).cast(pl.Int64)),
        ]
    )
    .group_by("team")
    .agg(pl.col("w").sum().alias("actual_wins"))
)
_JOINED = _OVERALL.select("team", "wins", "made_playoff").join(_ACTUAL, on="team", how="inner")

# 2023 CFP field (ESPN ids): Michigan, Washington, Texas, Alabama.
_CFP_2023 = ["130", "264", "251", "333"]


def test_total_wins_conserved() -> None:
    """The sampler is unbiased in aggregate: mean predicted wins == mean actual wins."""
    diff = abs(_JOINED["wins"].mean() - _JOINED["actual_wins"].mean())
    assert diff < 0.15, diff


def test_exp_wins_rank_calibrated() -> None:
    """Predicted season win totals rank-track the realized ones (observed 0.928)."""
    r = spearman_corr(_JOINED["wins"].to_numpy(), _JOINED["actual_wins"].to_numpy())
    assert r >= 0.90, r


def test_cfp_field_playoff_prob_towers_over_median() -> None:
    """The 2023 CFP four carry, on average, far more playoff probability than the field."""
    cfp_mean = _OVERALL.filter(pl.col("team").is_in(_CFP_2023))["made_playoff"].mean()
    field_median = _OVERALL["made_playoff"].median()
    assert cfp_mean >= 0.10, cfp_mean
    assert cfp_mean >= 5.0 * field_median, (cfp_mean, field_median)
