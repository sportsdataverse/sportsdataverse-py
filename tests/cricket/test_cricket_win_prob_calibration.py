"""Phase-2 gate: resource-surface monotonicity + holdout calibration (T7.3 Task 2.4).

Validated on the committed random-match-level Cricsheet holdout
(``tests/fixtures/league_ports/cricket_holdout.parquet``): 861 male T20I+ODI
matches / ~51k over-boundary states unseen by the fit (no match appears in both
train and holdout). Observed at fit time (never lower these — debug the model):
Brier ~= 0.190 (no-skill 0.5 = 0.25); max per-decile calibration ~= 0.031.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.cricket.cricket_model_constants import brier_score, calibration_table
from sportsdataverse.cricket.cricket_win_prob import _load_surface, cricket_win_probability

HOLDOUT = Path(__file__).resolve().parents[1] / "fixtures/league_ports/cricket_holdout.parquet"


# --- Step 1: resource-surface monotonicity (hard assert) ----------------------
def test_surface_monotone_in_overs_left() -> None:
    surf = _load_surface()
    for (fmt,), grp in surf.group_by(["fmt"]):
        for (_w,), col in grp.group_by(["wickets_left"]):
            res = col.sort("overs_left")["resource"].to_numpy()
            diffs = res[1:] - res[:-1]
            assert (diffs >= -1e-9).all(), f"{fmt}: resource not non-decreasing in overs_left"


def test_surface_monotone_in_wickets_left() -> None:
    surf = _load_surface()
    for (fmt,), grp in surf.group_by(["fmt"]):
        for (_o,), row in grp.group_by(["overs_left"]):
            res = row.sort("wickets_left")["resource"].to_numpy()
            diffs = res[1:] - res[:-1]
            assert (diffs >= -1e-9).all(), f"{fmt}: resource not non-decreasing in wickets_left"


def test_surface_bounds() -> None:
    surf = _load_surface()
    r = surf["resource"].to_numpy()
    assert (r >= 0.0).all() and (r <= 1.0).all()


# --- Step 2: holdout calibration gate -----------------------------------------
def _scored() -> tuple[pl.DataFrame, pl.DataFrame]:
    holdout = pl.read_parquet(HOLDOUT)
    scored = cricket_win_probability(holdout)
    return holdout, scored


def test_holdout_brier_beats_no_skill() -> None:
    holdout, scored = _scored()
    y = holdout["chasing_won"].to_numpy().astype(float)
    p = scored["win_prob"].to_numpy()
    brier = brier_score(y, p)
    assert brier <= 0.25, f"Brier {brier:.4f} does not beat the 0.5-constant no-skill baseline (0.25)"


def test_holdout_per_decile_calibration() -> None:
    holdout, scored = _scored()
    y = holdout["chasing_won"].to_numpy().astype(float)
    p = scored["win_prob"].to_numpy()
    tbl = calibration_table(y, p, n_bins=10)
    max_gap = float((tbl["mean_pred"] - tbl["mean_actual"]).abs().max())
    assert max_gap <= 0.05, f"per-decile calibration gap {max_gap:.4f} exceeds 0.05"


def test_holdout_predictions_bounded() -> None:
    _holdout, scored = _scored()
    p = scored["win_prob"].to_numpy()
    assert ((p >= 0.0) & (p <= 1.0)).all()
