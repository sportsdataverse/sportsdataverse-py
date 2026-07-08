"""Phase-2 backtest gates: as-of pregame predictions vs ESPN predictor + closing lines.

Offline -- committed 2023 fixtures only. The walk is leakage-safe: ratings for
week W are fit on plays from weeks < W only.

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


def test_spread_mae_vs_closing_line(backtest):
    """Gate: mae(exp_margin, close_spread_home) <= 3.5 (observed 2.961)."""
    edge = backtest["market_edge"].to_numpy()  # exp_margin - close_spread_home
    assert mae(np.zeros_like(edge), edge) <= SPREAD_FLOOR


def test_total_mae_vs_closing_line(backtest):
    """Gate: mae(exp_total, close_total) <= 4.0 (observed 3.235)."""
    assert mae(backtest["exp_total"].to_numpy(), backtest["close_total"].to_numpy()) <= TOTAL_FLOOR
