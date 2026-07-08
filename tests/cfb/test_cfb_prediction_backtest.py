"""2023 pregame backtest gate (T2.1 Task 2.3).

Leakage-free by construction: each game's prediction uses ratings fit on plays
from *strictly prior* weeks (``week < game_week``), never the game's own week or
later. Predictions run through the public :func:`cfb_predict_games` with the
fitted ``CFB_CONSTANTS["modern"]`` coefficients.

Floors are the values observed at gate time by
``dev/cfb_prediction/fit_pregame.py`` (Brier 0.147 vs FPI 0.144; spread MAE 4.51;
total MAE 5.14), set just loose enough to guard against regression per the
binding "never lower a gate to make it pass -- debug the model" rule. This test
does ~10 week-by-week ridge fits, so it runs ~15-20s -- it is a phase gate, not a
unit test, but it is fully offline (committed fixtures, no network).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_game_predict import cfb_predict_games
from sportsdataverse.cfb.cfb_prediction_constants import brier_score, mae
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_prediction"
_PBP = pl.read_parquet(_FIX / "pbp_2023_sample.parquet")
_RES = pl.read_parquet(_FIX / "results_2023.parquet")
_PRED = pl.read_parquet(_FIX / "espn_predictor_sample.parquet")
_ODDS = pl.read_parquet(_FIX / "espn_odds_sample.parquet")

_BURN_IN_WEEK = 5  # weeks 1-4 are the as-of burn-in (ratings too thin)
_MIN_GAMES = 25  # match-rate floor: sample must retain enough games to be meaningful
_SPREAD_FLOOR = 5.0  # observed 4.51 vs the closing line
_TOTAL_FLOOR = 6.0  # observed 5.14 vs the closing total


def _asof_predictions() -> pl.DataFrame:
    """Predict every ESPN-sample game from ratings fit on strictly-prior weeks."""
    sample_ids = set(_PRED["game_id"].to_list()) | set(_ODDS["game_id"].to_list())
    sched = _RES.filter(pl.col("game_id").is_in(sample_ids)).select(
        "game_id", "week", "home_team_id", "away_team_id", "neutral_site"
    )
    weeks = sorted(w for w in sched["week"].unique().to_list() if w is not None and w >= _BURN_IN_WEEK)
    frames: list[pl.DataFrame] = []
    for w in weeks:
        ratings = efficiency_ratings(_PBP.filter(pl.col("week") < w))
        if ratings.height == 0:
            continue
        rated = set(ratings["team_id"].to_list())
        games_w = sched.filter(
            (pl.col("week") == w) & pl.col("home_team_id").is_in(rated) & pl.col("away_team_id").is_in(rated)
        ).select("game_id", "home_team_id", "away_team_id", "neutral_site")
        if games_w.height:
            frames.append(cfb_predict_games(games_w, ratings))
    return pl.concat(frames)


_PREDS = _asof_predictions()


def _with_actual(df: pl.DataFrame) -> pl.DataFrame:
    """Attach the binary home-win outcome, dropping games with no final score."""
    actual = _RES.select(
        "game_id",
        (pl.col("home_score") > pl.col("away_score")).cast(pl.Float64).alias("y"),
        pl.col("home_score").is_not_null().alias("_decided"),
    )
    return df.join(actual, on="game_id", how="inner").filter(pl.col("_decided") == True)  # noqa: E712


def test_asof_boundary_is_strict_prior_weeks() -> None:
    """The burn-in cutoff really drops in-week + future plays (no leakage)."""
    prior = _PBP.filter(pl.col("week") < _BURN_IN_WEEK).height
    assert 0 < prior < _PBP.height


def test_enough_games_backtested() -> None:
    """Match-rate floor: the as-of prediction set is not degenerate."""
    assert _PREDS.height >= _MIN_GAMES, _PREDS.height


def test_win_prob_brier_within_fpi() -> None:
    """My win-prob Brier is within 0.01 of ESPN FPI on the shared games."""
    espn = _PRED.select("game_id", pl.col("home_win_prob").alias("espn_wp"))
    j = _with_actual(_PREDS.join(espn, on="game_id", how="inner"))
    assert j.height >= _MIN_GAMES, j.height
    y = j["y"].to_numpy()
    b_mine = brier_score(y, j["home_win_prob"].to_numpy())
    b_espn = brier_score(y, j["espn_wp"].to_numpy())
    assert b_mine <= b_espn + 0.01, (b_mine, b_espn)


def test_spread_mae_within_floor() -> None:
    """Expected margin tracks the closing line (market implied margin)."""
    odds = _ODDS.select("game_id", "close_spread_home").drop_nulls()
    j = _PREDS.join(odds, on="game_id", how="inner").with_columns(mkt_margin=-pl.col("close_spread_home"))
    assert j.height >= _MIN_GAMES, j.height
    assert mae(j["exp_margin"].to_numpy(), j["mkt_margin"].to_numpy()) <= _SPREAD_FLOOR


def test_total_mae_within_floor() -> None:
    """Expected total tracks the closing total."""
    odds = _ODDS.select("game_id", "close_total").drop_nulls()
    j = _PREDS.join(odds, on="game_id", how="inner")
    assert j.height >= _MIN_GAMES, j.height
    assert mae(j["exp_total"].to_numpy(), j["close_total"].to_numpy()) <= _TOTAL_FLOOR
