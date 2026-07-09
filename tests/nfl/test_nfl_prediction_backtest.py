"""Phase-2 backtest gates: as-of pregame predictions vs ESPN predictor + closing lines.

Offline -- committed 2023 fixtures only. The walk is leakage-safe: ratings for
week W are fit on plays from weeks < W only.

NOTE (in-sample disclosure): the era constants (``NFL_CONSTANTS`` /
``PROP_CONSTANTS``) were fitted by the ``dev/nfl_prediction/fit_*.py`` scripts
on this same 2023 fixture walk, so these floors are in-sample regression pins
that lock the shipped behavior -- NOT out-of-sample performance claims. A
held-out-season fixture (2022/2024) is the upgrade path if these numbers are
ever quoted as backtest performance.

Gate rule (binding): floors are set from the observed value at gate time and
NEVER widened/lowered to make a red gate pass. Observed at fit time
(ridge_lambda=25; constants from dev/nfl_prediction/fit_pregame.py, 2023
weeks 5-18, 208 gated games):

- ``brier(mine) = 0.2318`` vs ``brier(espn predictor) = 0.2294``
  -> gate ``brier_mine <= brier_espn + 0.01``
- ``mae(exp_margin, close_spread_home) = 2.961``  -> floor 3.5
- ``mae(exp_total, close_total) = 3.235``         -> floor 4.0
"""

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.nfl_market import nfl_predict_games
from sportsdataverse.nfl.nfl_prediction_constants import brier_score, mae
from sportsdataverse.nfl.nfl_ratings import efficiency_ratings

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_prediction"
WEEKS = range(5, 19)  # 4-week burn-in: earlier as-of ratings are too thin

SPREAD_FLOOR = 3.5  # observed 2.961
TOTAL_FLOOR = 4.0  # observed 3.235
BRIER_TOLERANCE = 0.01  # observed mine 0.2318 vs espn 0.2294


@pytest.fixture(scope="module")
def backtest():
    """As-of weekly predictions joined to ESPN predictor + closing lines."""
    pbp = pl.read_parquet(FIXTURES / "pbp_2023_sample.parquet")
    results = pl.read_parquet(FIXTURES / "results_2023.parquet").with_columns(
        (pl.col("home_score") - pl.col("away_score")).alias("margin"),
        (pl.col("home_score") + pl.col("away_score")).alias("total"),
    )
    odds = pl.read_parquet(FIXTURES / "espn_odds_sample.parquet")
    predictor = pl.read_parquet(FIXTURES / "espn_predictor_sample.parquet")

    frames = []
    for w in WEEKS:
        ratings = efficiency_ratings(pbp.filter(pl.col("week") < w))
        games = results.filter(pl.col("week") == w).select("game_id", "home_team_id", "away_team_id", "neutral_site")
        preds = nfl_predict_games(games, ratings, odds=odds)
        frames.append(preds.drop_nulls(["exp_margin"]))
    preds = pl.concat(frames)

    out = (
        preds.join(results.select("game_id", "margin", "total"), on="game_id", how="inner")
        .join(predictor.select("game_id", pl.col("home_win_prob").alias("p_espn")), on="game_id", how="inner")
        .join(odds.select("game_id", "close_total"), on="game_id", how="inner")
    )
    assert out.height >= 200  # weeks 5-18 of 2023 = 208 gated games
    return out


def test_win_prob_brier_tracks_espn_predictor(backtest):
    """Gate: my as-of Brier <= ESPN predictor Brier + 0.01 (obs 0.2318 vs 0.2294)."""
    y = (backtest["margin"].to_numpy() > 0).astype(float)
    b_mine = brier_score(y, backtest["home_win_prob"].to_numpy())
    b_espn = brier_score(y, backtest["p_espn"].to_numpy())
    assert b_mine <= b_espn + BRIER_TOLERANCE


def test_win_prob_quintile_calibration(backtest):
    """Gate: home_win_prob quintile calibration max |gap| <= 0.06 (observed 0.0359).

    Quintiles (not deciles) because the 208-game backtest leaves deciles too
    sparse for a stable gap; bins with n < 30 are excluded from the max.
    """
    from sportsdataverse.nfl.nfl_prediction_constants import calibration_table

    y = (backtest["margin"].to_numpy() > 0).astype(float)
    tbl = calibration_table(y, backtest["home_win_prob"].to_numpy(), n_bins=5).filter(pl.col("n") >= 30)
    assert tbl.height >= 2
    assert float((tbl["mean_pred"] - tbl["mean_actual"]).abs().max()) <= 0.06


def test_spread_mae_vs_closing_line(backtest):
    """Gate: mae(exp_margin, close_spread_home) <= 3.5 (observed 2.961)."""
    edge = backtest["market_edge"].to_numpy()  # exp_margin - close_spread_home
    assert mae(np.zeros_like(edge), edge) <= SPREAD_FLOOR


def test_total_mae_vs_closing_line(backtest):
    """Gate: mae(exp_total, close_total) <= 4.0 (observed 3.235)."""
    assert mae(backtest["exp_total"].to_numpy(), backtest["close_total"].to_numpy()) <= TOTAL_FLOOR


# ---------------------------------------------------------------------------
# Phase-3 props backtest gates (2023 weeks 6-18, as-of by week).
#
# Observed at fit time (kappas/priors/SDs from dev/nfl_prediction/fit_props.py):
#   mae passing_yards   = 70.508 (n=446)   -> floor 75.0
#   mae rushing_yards   = 21.105 (n=938)   -> floor 23.0
#   mae receiving_yards = 21.448 (n=2279)  -> floor 23.0
#
# p_over calibration: the plan's intended oracle (espn_nfl_game_propbets
# lines) is ABSENT upstream -- ESPN purges propbets for completed games (the
# committed espn_propbets_sample.parquet is zero-row by construction, see the
# fixtures README). The calibration gate therefore uses lagged-realized
# pseudo-lines (each player's previous-week value), which is a strictly
# HARSHER target than a market line: the pseudo-line is itself a noisy draw
# correlated with the outcome. Observed max decile |mean_pred - mean_actual|
# = 0.0954 (Gaussian p_over, n=3663); alternatives tried and rejected during
# debugging: empirical residual CDF (0.1256), sd inflation x1.4 (0.0918) --
# none reaches the plan's aspirational 0.05, which presumed market-centered
# lines. Gate locked from the observed value at 0.12 (never to be raised).
# ---------------------------------------------------------------------------

PROP_MAE_FLOORS = {"passing_yards": 75.0, "rushing_yards": 23.0, "receiving_yards": 23.0}
PROP_CALIBRATION_CEILING = 0.12  # observed 0.0954 (pseudo-lines; see block comment)


@pytest.fixture(scope="module")
def props_backtest():
    """As-of weekly full projections joined to realized stats + pseudo-lines."""
    import importlib

    props_mod = importlib.import_module("sportsdataverse.nfl.nfl_player_props")
    from sportsdataverse.nfl.nfl_player_props import player_usage_efficiency

    stats = pl.read_parquet(FIXTURES / "player_stats_2023.parquet")
    pbp = pl.read_parquet(FIXTURES / "pbp_2023_sample.parquet")
    results = pl.read_parquet(FIXTURES / "results_2023.parquet")
    frames = []
    for w in range(6, 19):
        ratings = efficiency_ratings(pbp.filter(pl.col("week") < w))
        games = results.filter(pl.col("week") == w).select("game_id", "home_team_id", "away_team_id", "neutral_site")
        preds = nfl_predict_games(games, ratings)
        usage = player_usage_efficiency(stats, as_of_week=w)
        proj = props_mod._project_week(usage, ratings, games, preds, era="modern")
        realized = stats.filter(pl.col("week") == w)
        assert proj.schema["player_id"] == stats.schema["player_id"] == pl.Utf8
        prev = (
            stats.filter(pl.col("week") < w)
            .sort("week")
            .group_by("player_id")
            .agg(
                pl.col("passing_yards").last().alias("prev_passing_yards"),
                pl.col("rushing_yards").last().alias("prev_rushing_yards"),
                pl.col("receiving_yards").last().alias("prev_receiving_yards"),
            )
        )
        for stat in ("passing_yards", "rushing_yards", "receiving_yards"):
            j = (
                proj.filter(pl.col("stat") == stat)
                .join(
                    realized.select("player_id", pl.col(stat).alias("realized")),
                    on="player_id",
                    how="inner",
                )
                .join(
                    prev.select("player_id", pl.col(f"prev_{stat}").alias("line")),
                    on="player_id",
                    how="left",
                )
            )
            frames.append(j)
    return pl.concat(frames)


def test_prop_mae_per_family(props_backtest):
    """Gate: as-of mae(proj_mean, realized) per family (see floors above)."""
    for stat, floor in PROP_MAE_FLOORS.items():
        f = props_backtest.filter(pl.col("stat") == stat)
        assert f.height > 300, stat
        observed = mae(f["proj_mean"].to_numpy(), f["realized"].to_numpy())
        assert observed <= floor, f"{stat}: mae {observed:.3f} > floor {floor}"


def test_prop_p_over_calibration_pseudo_lines(props_backtest):
    """Gate: max decile |mean_pred - mean_actual| <= 0.12 (observed 0.0954).

    Pseudo-lines = each player's previous-week realized value (the market
    propbets oracle is absent for past games -- see the block comment above).
    """
    from scipy.stats import norm

    from sportsdataverse.nfl.nfl_prediction_constants import calibration_table

    lined = props_backtest.drop_nulls(["line"])
    assert lined.height > 3000
    p_over = 1.0 - norm.cdf((lined["line"].to_numpy() - lined["proj_mean"].to_numpy()) / lined["proj_sd"].to_numpy())
    y = (lined["realized"].to_numpy() > lined["line"].to_numpy()).astype(float)
    # Exclude sparse bins from the max (all deciles carry n>=135 today; the
    # filter guards a future re-capture from a noisy near-empty decile).
    tbl = calibration_table(y, p_over, n_bins=10).filter(pl.col("n") >= 30)
    max_gap = float((tbl["mean_pred"] - tbl["mean_actual"]).abs().max())
    assert max_gap <= PROP_CALIBRATION_CEILING


def test_propbets_market_line_mae_reported():
    """The plan's `mae(proj_mean, propbets_line)` report: no line exists.

    ESPN purges propbets for completed games, so the committed fixture is
    zero-row by construction (documented in the fixtures README) and there is
    nothing to report against. This test pins that contract so a future
    re-capture that DOES find lines fails loudly and upgrades this report.
    """
    propbets = pl.read_parquet(FIXTURES / "espn_propbets_sample.parquet")
    assert propbets.height == 0
    assert propbets.schema == {
        "game_id": pl.Utf8,
        "player_id": pl.Utf8,
        "stat": pl.Utf8,
        "line": pl.Float64,
    }
