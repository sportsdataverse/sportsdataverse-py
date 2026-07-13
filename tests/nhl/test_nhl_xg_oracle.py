"""Productionized shot-level oracle for the NHL booster xG (T5 R5, closes W6).

The NHL xG is a **fixed published booster** (scored, not fit), so every real shot it
scores is trivially held-out -- there is no train/holdout leakage to guard. This gate
turns the dev-only AUC-0.784 evaluation (``dev/t5_xg_reevaluation/xg_nhl_eval.py``,
120,345 shots) into a committed, offline guardrail on the 3 captured games in
``pbp_sample.parquet`` (2024020001/2/3; 273 unblocked shots, 14 goals), scored live
through :func:`sportsdataverse.nhl.nhl_xg.nhl_xg` so the scorer itself is exercised.

Two gate families, both with floors DERIVED FROM OBSERVED VALUES (2026-07-13) and never
lowered to pass -- debug the model, not the floor:

1. **Calibration + discrimination** on the 273 shots. On a 14-goal sample AUC/Brier
   carry wide SE, so (matching the PWHL oracle's honesty rule) these are conservative
   floors -- no over-powered "beats naive by X" magnitude claim: AUC > 0.65 (obs 0.749),
   Brier no-worse-than-naive (obs 0.0472 vs base 0.0487), ECE < 0.05 (obs 0.026),
   mean xG within 0.035 of the goal rate (obs |0.0726-0.0513| = 0.021), xG in [0, 1].

2. **MoneyPuck per-shot agreement** -- the well-powered gate. MoneyPuck's *independent*
   ``xGoal`` (a different model, different features) is joined per shot against our
   booster ``xg`` on (game_id, shooter_id, game_seconds); 265/273 match (97%). Pearson
   corr on 265 paired continuous values has a tight SE, unlike AUC on 14 goals. Floors:
   match rate > 0.90 (obs 0.971), corr > 0.55 (obs 0.664), mean-abs-diff < 0.06
   (obs 0.038). Fixture: ``mp_shots_sample.parquet`` (Data: MoneyPuck.com, free for
   non-commercial use w/ credit), built by ``dev/nhl_player_impact/capture_mp_shots.py``.

Fully offline (both fixtures committed) -- runs in CI unconditionally, no live gate.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
from sklearn.metrics import roc_auc_score

from sportsdataverse.nhl.nhl_xg import nhl_xg

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"
_SHOT_EVENTS = ["SHOT", "MISSED_SHOT", "GOAL"]
_JOIN_KEYS = ["game_id", "shooter_id", "game_seconds"]
_MIN_SHOTS = 250  # the 3-game corpus is 273 scored shots; a shrunk fixture must fail loud, not pass vacuously

# --- oracle floors, from observed values 2026-07-13 (never lower to pass) --------------
_AUC_FLOOR = 0.65  # observed 0.749
_ECE_CEIL = 0.05  # observed 0.026
_CALIB_TOL = 0.035  # |mean_xg - goal_rate|, observed 0.021
_MATCH_RATE_FLOOR = 0.90  # observed 0.971
_MP_CORR_FLOOR = 0.55  # observed 0.664
_MP_MAD_CEIL = 0.06  # observed 0.038


def _scored_shots() -> pl.DataFrame:
    pbp = pl.read_parquet(FIX / "pbp_sample.parquet")
    scored = nhl_xg(pbp, model_dir=MODELS)
    return scored.filter(pl.col("event_type").is_in(_SHOT_EVENTS) & pl.col("xg").is_not_null())


def _yp(sh: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    y = sh["event_type"].str.to_uppercase().str.contains("GOAL").cast(pl.Int64).to_numpy().astype(float)
    p = np.clip(sh["xg"].to_numpy().astype(float), 1e-6, 1 - 1e-6)
    return y, p


def _ece(y: np.ndarray, p: np.ndarray, nbins: int = 10) -> float:
    edges = np.linspace(0, 1, nbins + 1)
    e = 0.0
    for i in range(nbins):
        m = (p >= edges[i]) & (p < edges[i + 1])
        if m.sum():
            e += abs(p[m].mean() - y[m].mean()) * m.sum()
    return e / len(y)


def test_nhl_xg_scores_in_unit_interval() -> None:
    sh = _scored_shots()
    p = sh["xg"].to_numpy().astype(float)
    assert sh.height >= _MIN_SHOTS, f"expected the 3-game shot corpus, got {sh.height}"
    assert p.min() >= 0.0 and p.max() <= 1.0


def test_nhl_xg_discriminates_and_calibrates() -> None:
    """Calibration + discrimination floors on the held-out 3-game corpus."""
    sh = _scored_shots()
    y, p = _yp(sh)
    auc = roc_auc_score(y, p)
    brier = float(np.mean((p - y) ** 2))
    brier_base = float(np.mean((y.mean() - y) ** 2))
    ece = _ece(y, p)
    assert auc > _AUC_FLOOR, f"AUC {auc:.4f} below floor {_AUC_FLOOR}"
    assert brier <= brier_base + 1e-9, f"Brier {brier:.4f} worse than naive {brier_base:.4f}"
    assert ece < _ECE_CEIL, f"ECE {ece:.4f} above ceiling {_ECE_CEIL}"
    assert abs(p.mean() - y.mean()) < _CALIB_TOL, (
        f"mean xG {p.mean():.4f} not within {_CALIB_TOL} of goal rate {y.mean():.4f}"
    )


def test_nhl_xg_agrees_with_moneypuck_per_shot() -> None:
    """The well-powered gate: per-shot agreement with MoneyPuck's independent xGoal."""
    sh = _scored_shots()
    ours = sh.select(
        game_id=pl.col("game_id").cast(pl.Int64),
        shooter_id=pl.col("event_player_1_id").cast(pl.Int64, strict=False),
        game_seconds=pl.col("game_seconds").cast(pl.Int64),
        our_xg=pl.col("xg").cast(pl.Float64),
    )
    mp = pl.read_parquet(FIX / "mp_shots_sample.parquet")
    # ID-discipline: assert join-key dtype agreement before the oracle join (a mismatch
    # would mis-join silently rather than raise on some polars paths).
    for k in _JOIN_KEYS:
        assert ours.schema[k] == mp.schema[k], f"join-key {k} dtype mismatch: {ours.schema[k]} vs {mp.schema[k]}"
    joined = ours.join(mp.select([*_JOIN_KEYS, "mp_xgoal"]), on=_JOIN_KEYS, how="inner")

    # Minimum-size floor so a shrunk fixture fails loud instead of computing corr on a
    # handful of points and passing vacuously (the corr/mad gate is only "powered" at scale).
    assert ours.height >= _MIN_SHOTS, f"scored corpus shrank to {ours.height} (< {_MIN_SHOTS})"
    assert joined.height >= _MIN_SHOTS, f"MP-joined corpus shrank to {joined.height} (< {_MIN_SHOTS})"
    match_rate = joined.height / ours.height
    assert match_rate > _MATCH_RATE_FLOOR, f"MP join match rate {match_rate:.3f} below {_MATCH_RATE_FLOOR}"

    a = joined["our_xg"].to_numpy()
    b = joined["mp_xgoal"].to_numpy()
    corr = float(np.corrcoef(a, b)[0, 1])
    mad = float(np.mean(np.abs(a - b)))
    assert corr > _MP_CORR_FLOOR, f"per-shot MoneyPuck corr {corr:.4f} below floor {_MP_CORR_FLOOR}"
    assert mad < _MP_MAD_CEIL, f"per-shot mean-abs-diff {mad:.4f} above ceiling {_MP_MAD_CEIL}"
