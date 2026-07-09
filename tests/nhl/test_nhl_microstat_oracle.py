"""Internal-oracle gates for the NHL microstat value spine (T5.2).

Every model ends with a gate asserting agreement with an internal oracle on
the committed `tests/fixtures/nhl_microstat/` corpus -- never lower a floor
to pass; debug the model. Floors below are set from the observed value at
gate time (rounded down/conservative), per the plan's Global Constraints.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_faceoff_value import extract_faceoffs, _taker_perspective_rows, fit_faceoff_context
from sportsdataverse.nhl.nhl_microstat_constants import split_half_stability

# ---------------------------------------------------------------------------
# Phase 1 -- faceoff-win value (model 4)
# ---------------------------------------------------------------------------

# Observed on the committed 2023-24 slice (40 games, 2373 faceoffs) after
# fixing the loser-row strength-state flip bug (Task 1.2): max |mean_pred -
# mean_actual| across calibration buckets with n>=20 is ~0.058. Floor set
# conservatively above that observed value -- if this regresses, debug the
# context logistic / zone-strength flip logic, do not raise the tolerance
# further without re-deriving from a fresh observed run.
CALIBRATION_ABS_DIFF_FLOOR = 0.10
CALIBRATION_MIN_BUCKET_N = 20

# Observed split-half Spearman on players with >=10 total faceoffs (144 of
# 283 players; ~10/half): ~0.20. This is a *within-a-40-game-slice* number --
# thin per-player samples (median 10 total draws) genuinely damp the
# correlation vs. a full-season sample; the floor is conservative relative
# to that observed value, not an aspirational full-season number.
SPLIT_HALF_MIN_ATTEMPTS = 10
SPLIT_HALF_FLOOR = 0.15


def test_faceoff_calibration_and_stability(oracle_pbp: pl.DataFrame) -> None:
    fo = extract_faceoffs(oracle_pbp)
    assert fo.height > 0

    model = fit_faceoff_context(fo)
    taker = _taker_perspective_rows(fo)
    expected = model.predict(taker)
    taker = taker.with_columns(expected.alias("expected_win"), expected.round(4).alias("_bucket"))

    calibration = taker.group_by("_bucket").agg(
        pl.col("expected_win").mean().alias("mean_pred"),
        pl.col("won").mean().alias("mean_actual"),
        pl.len().alias("n"),
    )
    big_buckets = calibration.filter(pl.col("n") >= CALIBRATION_MIN_BUCKET_N)
    assert big_buckets.height > 0
    max_diff = (big_buckets["mean_pred"] - big_buckets["mean_actual"]).abs().max()
    assert max_diff is not None and max_diff <= CALIBRATION_ABS_DIFF_FLOOR, (
        f"faceoff context-logistic calibration off by {max_diff:.4f} "
        f"(floor {CALIBRATION_ABS_DIFF_FLOOR}) -- debug the zone/strength "
        "flip logic before touching this floor"
    )

    # Split-half player win% stability, restricted to players with enough
    # attempts to be non-degenerate (see SPLIT_HALF_MIN_ATTEMPTS docstring above).
    half_taker = taker.with_columns((pl.arange(0, pl.len()) % 2).alias("half"), pl.lit(1).alias("one"))
    attempt_counts = half_taker.group_by("player_id").agg(pl.len().alias("n_attempts"))
    eligible = attempt_counts.filter(pl.col("n_attempts") >= SPLIT_HALF_MIN_ATTEMPTS)["player_id"]
    filtered = half_taker.filter(pl.col("player_id").is_in(eligible.implode()))

    stability = split_half_stability(filtered, id_col="player_id", half_col="half", num_col="won", den_col="one")
    assert stability >= SPLIT_HALF_FLOOR, (
        f"faceoff split-half stability {stability:.4f} below floor {SPLIT_HALF_FLOOR} "
        "-- debug before lowering this floor"
    )
