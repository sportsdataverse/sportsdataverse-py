"""Tests for the native in-game win-probability model (Phase 3, model ④).

Gate scope note: the plan's gate (b) "MAE vs native winprobabilitypbp" is
UNOBTAINABLE -- that stats.nba.com endpoint is dead (HTTP 500; hoopR's own
nba_winprobabilitypbp() is deprecate_stop()-ed). This model is gated ONLY on
gate (a): per-time-bucket realized-outcome calibration. See the fixtures
README + SDD ledger for the retirement record.

Gate rule (binding): never lower a gate to make it pass -- debug the model.
Floors are the observed value at gate time rounded to the safe side.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_game_predict import in_game_features, nba_in_game_win_prob
from sportsdataverse.nba.nba_prediction_constants import calibration_table

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"


def _synthetic_pbp() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "start_game_seconds_remaining": [2880.0, 1440.0, 30.0],
            "home_score": [0, 40, 100],
            "away_score": [0, 42, 84],
            "team_id": ["A", "B", "A"],
            "home_team_id": ["A", "A", "A"],
        }
    )


def test_in_game_features_shape_and_math() -> None:
    feats = in_game_features(_synthetic_pbp(), 0.6)
    assert feats.columns == ["score_diff", "sec_left", "sqrt_sec_left", "pregame_logit", "home_has_ball"]
    assert feats["score_diff"].to_list() == [0.0, -2.0, 16.0]
    # sec_left strictly decreasing across the three plays
    assert feats["sec_left"].to_list() == [2880.0, 1440.0, 30.0]
    assert abs(feats["sqrt_sec_left"][2] - math.sqrt(30.0)) < 1e-9
    assert abs(feats["pregame_logit"][0] - math.log(0.6 / 0.4)) < 1e-9
    assert feats["home_has_ball"].to_list() == [1, 0, 1]


def test_in_game_features_overtime_clips_negative_seconds() -> None:
    pbp = pl.DataFrame(
        {
            "start_game_seconds_remaining": [-45.0],
            "home_score": [110],
            "away_score": [108],
            "team_id": ["A"],
            "home_team_id": ["A"],
        }
    )
    feats = in_game_features(pbp, 0.5)
    assert feats["sec_left"][0] == 0.0
    assert feats["sqrt_sec_left"][0] == 0.0


def test_scorer_large_late_home_lead_near_one() -> None:
    pbp = pl.DataFrame(
        {
            "start_game_seconds_remaining": [15.0],
            "home_score": [120],
            "away_score": [100],
            "team_id": ["A"],
            "home_team_id": ["A"],
        }
    )
    wp = nba_in_game_win_prob(pbp, 0.5)
    assert wp["home_win_prob"][0] > 0.9


def test_scorer_monotonic_in_score_diff_at_fixed_time() -> None:
    pbp = pl.DataFrame(
        {
            "start_game_seconds_remaining": [600.0, 600.0, 600.0],
            "home_score": [90, 100, 110],
            "away_score": [100, 100, 100],  # score_diff -10, 0, +10
            "team_id": ["A", "A", "A"],
            "home_team_id": ["A", "A", "A"],
        }
    )
    wp = nba_in_game_win_prob(pbp, 0.5)["home_win_prob"].to_list()
    assert wp[0] < wp[1] < wp[2]


def test_scorer_return_as_pandas() -> None:
    out = nba_in_game_win_prob(_synthetic_pbp(), 0.5, return_as_pandas=True)
    assert type(out).__name__ == "DataFrame"
    assert "home_win_prob" in out.columns


def test_in_game_wp_calibration_gate_2024() -> None:
    """Gate (a): per-decile realized-outcome calibration on the 2024 pbp sample."""
    sample = pl.read_parquet(FIXTURE_DIR / "pbp_sample_2024.parquet")
    assert sample.height > 0

    preds = []
    labels = []
    for gid, g in sample.group_by("game_id"):
        pregame = float(g["pregame_home_prob"][0])
        wp = nba_in_game_win_prob(g, pregame)
        preds.append(wp["home_win_prob"].to_numpy())
        labels.append(g["home_win"].to_numpy().astype(float))
    p = np.concatenate(preds)
    y = np.concatenate(labels)

    tbl = calibration_table(y, p, n_bins=10).filter(pl.col("n") >= 20)
    max_gap = float((tbl["mean_pred"] - tbl["mean_actual"]).abs().max())
    slope = float(np.polyfit(tbl["mean_pred"].to_numpy(), tbl["mean_actual"].to_numpy(), 1)[0])
    # Observed at gate time (2026-07-08, 399-game / ~6400-play sample; home_win_rate
    # 0.541 matches the population, so the earlier small-sample base-rate skew is gone):
    # the shipped shallow-xgboost model (escalated from a plain logistic that maxed at
    # ~0.11) gives max bucket gap 0.039 and calibration slope 0.965. Floors from observed,
    # rounded to the safe side. NOTE: gate (b) "MAE vs native winprobabilitypbp" is
    # unobtainable (dead endpoint); this realized-outcome calibration is the only gate.
    assert max_gap <= 0.05, f"in-game WP calibration max bucket gap {max_gap:.3f} above 0.05 floor"
    assert 0.85 <= slope <= 1.15, f"in-game WP calibration slope {slope:.3f} outside [0.85, 1.15]"
