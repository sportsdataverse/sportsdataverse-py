"""Internal-oracle gates for the T5.3 PWHL categorical-shot_quality xG proxy.

No external PWHL oracle exists (first-of-its-kind, best-effort): the honest
internal oracle is a HELD-OUT realized-outcome backtest. The fixtures are
built with a strict train/holdout split (see
``tests/fixtures/pwhl_prediction/README.md`` +
``dev/pwhl_prediction/build_pwhl_xg_fixture.py``):

- tier weights fit on strictly-prior complete seasons (2024+2025 for the
  held-out 2026 walk) -- no game sees weights fit on its own or later data;
- ``margin_sd`` fit on 2025 only, with 2026 kept entirely out of the fit.

**Honest result** (held-out 2026, n=107): Brier 0.2449 vs naive 0.2500 -- a
delta of only -0.0051, WITHIN sampling noise (SE ~0.0053). The model is
directionally correct but does NOT robustly beat naive out-of-sample, so per
the review's honesty rule the gate is held-out CALIBRATION +
no-worse-than-naive-within-noise, NOT a beats-naive magnitude assertion (that
needs more PWHL seasons). Floors are set from observed values, conservative;
never lower a floor to pass -- debug the model.
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
def train_shots() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES_DIR / "shots_train_2024_2025.parquet")


@pytest.fixture(scope="module")
def game_rates() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES_DIR / "game_rates_heldout_2026.parquet")


@pytest.fixture(scope="module")
def backtest() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES_DIR / "backtest_heldout_2026.parquet")


# ---------------------------------------------------------------------------
# Shot-quality tier weight: directional sanity (the internal oracle for the
# proxy fit itself -- "Quality" shots must score at a higher empirical goal
# rate than "Non quality" shots, or the tier labels are meaningless/inverted).
# Fit on the training pool (2024+2025); this checks the fit MECHANISM, not an
# out-of-sample prediction, so it is not a leakage surface.
# ---------------------------------------------------------------------------
QUALITY_WEIGHT_MARGIN = 0.03  # observed diff ~0.070 (0.120 vs 0.050); floor conservative below


def test_shot_quality_weights_directionally_sane(train_shots: pl.DataFrame) -> None:
    model = fit_shot_quality_xg(train_shots)
    assert "quality" in model.weights and "non_quality" in model.weights
    diff = model.weights["quality"] - model.weights["non_quality"]
    assert diff >= QUALITY_WEIGHT_MARGIN, (
        f"'quality' tier goal-rate {model.weights['quality']:.4f} exceeds 'non_quality' "
        f"{model.weights['non_quality']:.4f} by only {diff:.4f} (floor {QUALITY_WEIGHT_MARGIN}) "
        "-- the shot_quality tier collapse or fit may be inverted"
    )
    for w in model.weights.values():
        assert 0.0 < w < 1.0


# ---------------------------------------------------------------------------
# Held-out game-rates shape sanity.
# ---------------------------------------------------------------------------
def test_game_rates_shape_and_dtypes(game_rates: pl.DataFrame) -> None:
    assert game_rates.height > 150, "held-out PWHL game-rates corpus unexpectedly small"
    assert game_rates.schema["game_id"] == pl.Utf8
    assert game_rates.schema["team"] == pl.Utf8
    assert game_rates.schema["date"] == pl.Date
    assert game_rates.filter(pl.col("date").is_null()).height == 0, "every regular-season game should resolve a date"
    counts = game_rates.group_by("game_id").agg(pl.len().alias("n"))
    assert (counts["n"] == 2).all(), "every game should contribute exactly one home + one away row"


# ---------------------------------------------------------------------------
# Held-out realized-outcome backtest -- the honest internal oracle.
# NOT a beats-naive magnitude gate (the -0.0051 edge is within noise): instead
# (a) no-worse-than-naive within sampling noise, and (b) calibration.
# ---------------------------------------------------------------------------
NAIVE_BRIER = 0.25
# No-harm tolerance ~2 SE (SE ~0.0053 on 107 games) so a genuine degradation
# (anti-predictive model -> Brier well above naive) fails, while noise on a
# fixture refresh does not spuriously fail. Observed held-out Brier 0.2449.
BRIER_NO_HARM_TOL = 0.011
MIN_EVALUATED_GAMES = 90
# Held-out predictions cluster near 0.5 (exp_margin is heavily shrink-
# compressed), so calibration_table collapses to a single adequately-sampled
# base-rate bucket: observed mean_pred 0.5505 vs mean_actual 0.5607, dev
# ~0.0102. This is effectively a base-rate calibration check; a multi-bucket
# calibration gate needs more predicted spread (more seasons). Floor
# conservative above observed.
CALIBRATION_FLOOR = 0.06
MIN_CALIBRATION_BUCKET_N = 30


def test_pwhl_heldout_no_worse_than_naive(backtest: pl.DataFrame) -> None:
    assert backtest.height >= MIN_EVALUATED_GAMES, (
        f"held-out PWHL backtest only {backtest.height} games (floor {MIN_EVALUATED_GAMES})"
    )
    y = backtest["home_win"].to_numpy()
    p = backtest["home_win_prob"].to_numpy()
    model_brier = brier_score(y, p)
    naive_brier = brier_score(y, np.full(len(y), 0.5))
    assert abs(naive_brier - NAIVE_BRIER) < 0.01, "sanity-check the documented naive baseline"
    # Honest claim: the de-leaked model is NOT meaningfully WORSE than naive
    # out-of-sample (it is very slightly better, within noise). A real
    # degradation (bug making it anti-predictive) pushes Brier above this bar.
    assert model_brier <= naive_brier + BRIER_NO_HARM_TOL, (
        f"held-out PWHL model Brier {model_brier:.4f} exceeds naive {naive_brier:.4f} by more than the "
        f"no-harm tolerance {BRIER_NO_HARM_TOL} -- the xG proxy or margin_sd fit regressed; debug before "
        "touching this tolerance"
    )


def test_pwhl_heldout_calibration(backtest: pl.DataFrame) -> None:
    cal = calibration_table(backtest["home_win"].to_numpy(), backtest["home_win_prob"].to_numpy(), n_bins=5)
    cal = cal.filter(pl.col("n") >= MIN_CALIBRATION_BUCKET_N)
    assert cal.height >= 1, "no adequately-sampled calibration bucket -- corpus may have shrunk"
    dev = (cal["mean_pred"] - cal["mean_actual"]).abs()
    assert dev.max() <= CALIBRATION_FLOOR, (
        f"held-out PWHL calibration max per-bucket deviation {dev.max():.4f} above floor {CALIBRATION_FLOOR}"
    )


def test_pwhl_margin_sd_was_fit_out_of_sample() -> None:
    # The old seeded value (2.35) was a real-world-goal-margin-scale guess; the
    # fitted, much-smaller sigma (1.21, fit on 2025 with 2026 held out) must be
    # in place. See nhl_prediction_constants.py's PWHL margin_sd comment.
    const = get_constants("pwhl")
    assert 0.9 <= const.margin_sd <= 1.6, (
        f"PWHL margin_sd {const.margin_sd} outside the fitted-scale range -- reverted to the old seed?"
    )
