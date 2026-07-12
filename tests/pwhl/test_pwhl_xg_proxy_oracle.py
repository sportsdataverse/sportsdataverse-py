"""Internal-oracle gates for the T5.3 PWHL categorical-shot_quality xG proxy.

No external PWHL oracle exists (first-of-its-kind, best-effort): the honest
internal oracle is a realized-outcome Brier/calibration backtest against a
naive baseline, on the committed `tests/fixtures/pwhl_prediction/` corpus
(3 seasons, 2024-2026, as-of-date walk-forward). Floors below are set from
the observed value at gate-authoring time (rounded to the conservative
side) -- never lower a floor to pass; debug the model. See
`tests/fixtures/pwhl_prediction/README.md` for full provenance + the
documented thin-sample limitation.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nhl.nhl_prediction_constants import brier_score, calibration_table, get_constants
from sportsdataverse.pwhl.pwhl_xg_proxy import fit_shot_quality_xg

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "pwhl_prediction"


@pytest.fixture(scope="module")
def pooled_shots() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES_DIR / "shots_2024_2026.parquet")


@pytest.fixture(scope="module")
def game_rates() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES_DIR / "game_rates_2024_2026.parquet")


@pytest.fixture(scope="module")
def backtest() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES_DIR / "backtest_predictions_2024_2026.parquet")


# ---------------------------------------------------------------------------
# Shot-quality tier weight: directional sanity (the internal oracle for the
# proxy fit itself -- "Quality" shots must score at a higher empirical goal
# rate than "Non quality" shots, or the tier labels are meaningless/inverted).
# ---------------------------------------------------------------------------
QUALITY_WEIGHT_MARGIN = 0.03  # observed diff ~0.0746 (0.1213 vs 0.0467); floor conservative below


def test_shot_quality_weights_directionally_sane(pooled_shots: pl.DataFrame) -> None:
    model = fit_shot_quality_xg(pooled_shots)
    assert "quality" in model.weights and "non_quality" in model.weights
    diff = model.weights["quality"] - model.weights["non_quality"]
    assert diff >= QUALITY_WEIGHT_MARGIN, (
        f"'quality' tier goal-rate {model.weights['quality']:.4f} exceeds 'non_quality' "
        f"{model.weights['non_quality']:.4f} by only {diff:.4f} (floor {QUALITY_WEIGHT_MARGIN}) "
        "-- the shot_quality tier collapse or fit may be inverted"
    )
    # Both weights must be legitimate probabilities.
    for w in model.weights.values():
        assert 0.0 < w < 1.0


# ---------------------------------------------------------------------------
# Game-rates shape sanity.
# ---------------------------------------------------------------------------
def test_game_rates_shape_and_dtypes(game_rates: pl.DataFrame) -> None:
    assert game_rates.height > 400, "PWHL game-rates corpus unexpectedly small"
    assert game_rates.schema["game_id"] == pl.Utf8
    assert game_rates.schema["team"] == pl.Utf8
    assert game_rates.schema["date"] == pl.Date
    assert game_rates.filter(pl.col("date").is_null()).height == 0, "every regular-season game should resolve a date"
    # Home/away symmetry: every game_id appears exactly twice (once per side).
    counts = game_rates.group_by("game_id").agg(pl.len().alias("n"))
    assert (counts["n"] == 2).all(), "every game should contribute exactly one home + one away row"


# ---------------------------------------------------------------------------
# Realized-outcome backtest: Brier vs naive baseline + calibration table.
# This is the honest internal oracle (see module docstring) -- no external
# PWHL market/power-index exists to compare against.
# ---------------------------------------------------------------------------
NAIVE_BRIER = 0.25
MODEL_BRIER_FLOOR = 0.248  # observed 0.2438; must beat naive with margin (floor conservative above observed)
CALIBRATION_FLOOR = 0.15  # observed max deviation ~0.1177 across 2 buckets (n=199, n=45)
MIN_CALIBRATION_BUCKET_N = 30
MIN_EVALUATED_GAMES = 200


def test_pwhl_backtest_beats_naive_baseline(backtest: pl.DataFrame) -> None:
    assert backtest.height >= MIN_EVALUATED_GAMES, (
        f"PWHL backtest corpus only {backtest.height} evaluated games (floor {MIN_EVALUATED_GAMES})"
    )
    y = backtest["home_win"].to_numpy()
    p = backtest["home_win_prob"].to_numpy()
    model_brier = brier_score(y, p)
    naive_brier = brier_score(y, np.full(len(y), 0.5))
    assert abs(naive_brier - NAIVE_BRIER) < 0.01, "sanity-check the documented naive baseline"
    assert model_brier <= MODEL_BRIER_FLOOR, (
        f"PWHL model Brier {model_brier:.4f} above floor {MODEL_BRIER_FLOOR} -- debug the xG proxy "
        "or the margin_sd fit before lowering this floor"
    )
    assert model_brier < naive_brier, "PWHL model must beat the p=0.5 naive baseline (even modestly)"


def test_pwhl_backtest_calibration(backtest: pl.DataFrame) -> None:
    cal = calibration_table(backtest["home_win"].to_numpy(), backtest["home_win_prob"].to_numpy(), n_bins=5)
    cal = cal.filter(pl.col("n") >= MIN_CALIBRATION_BUCKET_N)
    assert cal.height >= 2, "too few adequately-sampled calibration buckets -- corpus may have shrunk"
    dev = (cal["mean_pred"] - cal["mean_actual"]).abs()
    assert dev.max() <= CALIBRATION_FLOOR, (
        f"PWHL pregame calibration max per-bucket deviation {dev.max():.4f} above floor {CALIBRATION_FLOOR}"
    )


def test_pwhl_margin_sd_was_fit_not_left_seeded() -> None:
    # The old seeded value (2.35, a real-world-goal-margin-scale guess) is a
    # documented ~3x scale mismatch against this proxy's heavily
    # shrink-compressed exp_margin (std ~0.065) -- confirm the fitted,
    # much-smaller sigma is in place (see nhl_prediction_constants.py's PWHL
    # margin_sd comment + tests/fixtures/pwhl_prediction/README.md).
    const = get_constants("pwhl")
    assert 0.5 <= const.margin_sd <= 1.2, (
        f"PWHL margin_sd {const.margin_sd} outside the fitted-scale range -- was it reverted to the old seed?"
    )
