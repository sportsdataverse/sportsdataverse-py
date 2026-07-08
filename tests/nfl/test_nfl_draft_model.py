"""Unit + oracle tests for the draft outcome model (Phase 5)."""

from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_draft_model import (
    _ridge_fit,
    assemble_draft_features,
    project_draft_class,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_projection"


def test_ridge_recovers_negative_forty_coefficient():
    rng = np.random.default_rng(0)
    forty = rng.normal(4.5, 0.15, 500)
    y = 100.0 - 15.0 * forty + rng.normal(0, 0.5, 500)
    X = np.column_stack([np.ones(500), (forty - forty.mean()) / forty.std()])
    beta = _ridge_fit(X, y - y.mean(), 1.0)
    assert beta[1] < 0  # faster (lower forty) -> better outcome


def _synth_loader_frames():
    """Loader-shaped synthetic combine + draft frames: car_av is a known linear
    function of forty (faster = better)."""
    rng = np.random.default_rng(1)
    rows_c, rows_d = [], []
    pid = 0
    for season in range(2000, 2021):
        for i in range(30):
            pid += 1
            forty = float(rng.uniform(4.3, 5.0))
            w_av = max(0.0, 120.0 - 22.0 * forty + float(rng.normal(0, 2)))
            rows_c.append(
                {
                    "pfr_id": f"PFR{pid}",
                    "season": season,
                    "ht": "6-2",
                    "wt": 210.0,
                    "forty": forty,
                    "bench": 20.0,
                    "vertical": 35.0,
                    "broad_jump": 120.0,
                    "cone": 7.0,
                    "shuttle": 4.3,
                }
            )
            rows_d.append(
                {
                    "gsis_id": f"00-{pid:07d}",
                    "pfr_player_id": f"PFR{pid}",
                    "season": season,
                    "position": "WR",
                    "round": (i // 10) + 1,
                    "pick": i + 1,
                    "w_av": w_av,
                    "seasons_started": 3.0 if w_av > 25 else 0.0,
                }
            )
    return pl.DataFrame(rows_c), pl.DataFrame(rows_d)


def test_assemble_and_project_fast_beats_slow():
    combine, draft = _synth_loader_frames()
    feats = assemble_draft_features(combine, draft)
    assert feats.schema["gsis_id"] == pl.Utf8
    assert feats.schema["ht"] == pl.Float64  # "6-2" parsed to inches
    assert feats["forty_imputed"].sum() == 0
    out = project_draft_class(feats, 2020, lam=1.0)
    j = out.join(feats.filter(pl.col("season") == 2020).select("gsis_id", "forty"), on="gsis_id", how="inner")
    fast = j.filter(pl.col("forty") <= 4.45)["pred_car_av"].mean()
    slow = j.filter(pl.col("forty") >= 4.85)["pred_car_av"].mean()
    assert fast > slow
    assert out["hit_prob"].min() >= 0.0 and out["hit_prob"].max() <= 1.0


def test_projection_excludes_target_class_from_training():
    """Leakage: the target class must not appear in its own training slice."""
    combine, draft = _synth_loader_frames()
    feats = assemble_draft_features(combine, draft)
    # poison the target class labels; predictions must be unchanged
    poisoned = feats.with_columns(
        pl.when(pl.col("season") == 2020).then(pl.lit(999.0)).otherwise(pl.col("car_av")).alias("car_av")
    )
    a = project_draft_class(feats, 2020, lam=1.0)
    b = project_draft_class(poisoned, 2020, lam=1.0)
    assert np.allclose(a["pred_car_av"].to_numpy(), b["pred_car_av"].to_numpy())


def test_assemble_imputes_missing_measurables():
    combine, draft = _synth_loader_frames()
    combine = combine.with_columns(
        pl.when(pl.col("pfr_id") == "PFR1").then(None).otherwise(pl.col("forty")).alias("forty")
    )
    feats = assemble_draft_features(combine, draft)
    r = feats.filter(pl.col("gsis_id") == "00-0000001").row(0, named=True)
    assert r["forty_imputed"] == 1
    assert r["forty"] is not None  # imputed to position-season median


def test_assemble_empty_returns_schema():
    out = assemble_draft_features(pl.DataFrame(), pl.DataFrame())
    assert out.height == 0
    assert out.schema["gsis_id"] == pl.Utf8


def _fixture_features():
    from sportsdataverse.nfl.nfl_draft_model import MEASURABLES

    feats = pl.read_parquet(FIX / "draft_matured.parquet")
    for c in MEASURABLES:
        feats = feats.with_columns(pl.col(c).is_null().cast(pl.Int8).alias(f"{c}_imputed"))
        feats = feats.with_columns(
            pl.col(c)
            .fill_null(pl.col(c).median().over("position", "season"))
            .fill_null(pl.col(c).median().over("position"))
            .fill_null(pl.col(c).median())
            .fill_null(0.0)
        )
    return feats


def test_oracle_draft_model_vs_realized_and_pick():
    """Draft-model oracle on matured holdout classes 2015-2019 (offline fixtures).

    Train per class on classes <= class - 5 (maturity boundary); pool the five
    holdout classes (n=1269). Observed 2026-07-08 with DEFAULT_LAM=100
    (selected on inner classes 2008-2014, see
    dev/nfl_projection/fit_draft_lambda.py): ours 0.5888 vs pick-only 0.5865;
    hit calibration max decile gap 0.0757 (gate 0.10). Floors set from observed
    values rounded down. Realized `car_av` is nflverse `w_av` (see fixture
    README).
    """
    from sportsdataverse.nfl.nfl_draft_model import HIT_SEASONS_STARTED
    from sportsdataverse.nfl.nfl_projection_constants import calibration_table, spearman_corr

    feats = _fixture_features()
    preds = []
    for cls in [2015, 2016, 2017, 2018, 2019]:
        p = project_draft_class(feats, cls)
        realized = feats.filter(pl.col("season") == cls).select("gsis_id", "car_av", "pick", "seasons_started")
        assert p.schema["gsis_id"] == realized.schema["gsis_id"]
        preds.append(p.join(realized, on="gsis_id", how="inner"))
    allp = pl.concat(preds)
    assert allp.height >= 1200
    ours = spearman_corr(allp["pred_car_av"].to_numpy(), allp["car_av"].to_numpy())
    # lower pick = better, so negate for correlation direction
    pick_only = abs(spearman_corr(-allp["pick"].to_numpy(), allp["car_av"].to_numpy()))
    assert ours >= 0.58, f"spearman {ours:.4f} < floor 0.58"
    assert ours >= pick_only - 1e-6, f"ours {ours:.4f} < pick-only {pick_only:.4f}"
    hit = (allp["seasons_started"].fill_null(0.0) >= HIT_SEASONS_STARTED).cast(pl.Float64).to_numpy()
    tbl = calibration_table(hit, allp["hit_prob"].to_numpy())
    gap = float((tbl["mean_pred"] - tbl["mean_actual"]).abs().max())
    assert gap <= 0.10, f"hit calibration max decile gap {gap:.4f} > 0.10\n{tbl}"
