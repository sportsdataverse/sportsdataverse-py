"""Fit the draft-value artifact (ridge `proj_career_value` + logistic `draft_prob`).

Reads the committed Task-0.1/0.3 fixtures (offline, no live network needed),
builds combine features via `build_combine_features`, trains on classes
<= CUTOFF_YEAR (`as_of_class_split`), and writes the bundled
`sportsdataverse/nba/models/nba_draft_value.json` artifact consumed by
`nba_draft_model`.

Leakage guard: only pre-draft combine measurements are features; the
realized career value (`career_values.parquet`, derived from
`playercareerstats`, i.e. strictly post-draft data) and drafted-outcome
label (`draft_outcomes.parquet`) are measured after the combine.

Run: ``uv run python dev/nba_draft/fit_draft_model.py``
"""

from __future__ import annotations

import json

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_draft_constants import (
    COMBINE_FEATURES,
    as_of_class_split,
    auc,
    logistic_fit_irls,
    ridge_fit,
    spearman_corr,
)

FIXTURE_DIR = "tests/fixtures/nba_draft"
# Corpus expanded 2026-07-08 back to 2000-2019 (20 classes, ~1328 prospects)
# after the initial 2016-2019-only fit (~250 prospects) showed ~0/negative
# holdout Spearman under every debugging attempt (feature subsets, lambda
# sweeps, xgboost escalation) -- a real small-sample-size problem, not a
# modeling bug. cutoff_year=2015 now gives train=2000-2015 (16 classes),
# holdout=2016-2019 (4 classes, matching the original oracle-fixture years).
CUTOFF_YEAR = 2015
ARTIFACT_PATH = "sportsdataverse/nba/models/nba_draft_value.json"


def main() -> None:
    combine = pl.read_parquet(f"{FIXTURE_DIR}/combine_2016_2019.parquet")
    career = pl.read_parquet(f"{FIXTURE_DIR}/career_values.parquet")
    outcomes = pl.read_parquet(f"{FIXTURE_DIR}/draft_outcomes.parquet")

    assert combine.schema["player_id"] == career.schema["player_id"] == outcomes.schema["player_id"] == pl.Utf8

    df = (
        combine.join(career.select("player_id", "career_value"), on="player_id", how="left")
        .join(outcomes.select("player_id", "drafted"), on="player_id", how="left")
        .with_columns(pl.col("career_value").fill_null(0.0), pl.col("drafted").fill_null(False))
    )
    # combine_2016_2019.parquet is the raw joined capture (Task 0.1); a couple
    # of raw columns (e.g. `weight`) come back as strings for some seasons'
    # payloads, so `pl.concat(..., how="diagonal_relaxed")` unions them to
    # String -- cast every raw combine measurement to Float64 defensively.
    raw_numeric_cols = [
        c
        for c in [
            "height_wo_shoes",
            "weight",
            "wingspan",
            "standing_reach",
            "body_fat_pct",
            "hand_length",
            "hand_width",
            "lane_agility",
            "three_quarter_sprint",
            "standing_vertical",
            "max_vertical",
            "spot_fifteen_corner_left_pct",
            "offdrib_fifteen_top_pct",
        ]
        if c in df.columns
    ]
    df = df.with_columns([pl.col(c).cast(pl.Float64, strict=False) for c in raw_numeric_cols])
    # derive the two engineered combine features to match COMBINE_FEATURES.
    df = df.with_columns(
        (pl.col("weight") / (pl.col("height_wo_shoes") ** 2) * 703.0).alias("bmi"),
        (pl.col("wingspan") - pl.col("height_wo_shoes")).alias("wingspan_diff"),
    )

    # Split BEFORE computing the imputation median. Combine columns are 56-92%
    # null, so a median taken over the full frame leaks the holdout-class
    # distribution into every train row's imputed value (an as-of-class
    # leakage boundary violation). The train-only median is stored in the
    # artifact and re-applied to holdout by nba_draft_model._score at score
    # time -- the gate test mirrors that.
    train, holdout = as_of_class_split(df, cutoff_year=CUTOFF_YEAR)
    feature_median = {c: float(train[c].drop_nulls().median() or 0.0) for c in COMBINE_FEATURES}
    train = train.with_columns([pl.col(c).fill_null(feature_median[c]) for c in COMBINE_FEATURES])
    print(f"train classes {sorted(train['draft_year'].unique().to_list())}: {train.height} prospects")
    print(f"holdout classes {sorted(holdout['draft_year'].unique().to_list())}: {holdout.height} prospects")

    X_train_raw = train.select(COMBINE_FEATURES).to_numpy()
    feature_mean = X_train_raw.mean(axis=0)
    feature_sd = X_train_raw.std(axis=0)
    feature_sd[feature_sd == 0.0] = 1.0
    X_train = (X_train_raw - feature_mean) / feature_sd
    y_value = train["career_value"].to_numpy()
    y_prob = train["drafted"].cast(pl.Int64).to_numpy()

    # lambda chosen by a small 5-fold CV grid (career_value scale is O(10^2)
    # per season summed across a career, so a larger ridge penalty than the
    # box-value fit is appropriate on standardized features).
    lambdas = [1.0, 5.0, 10.0, 25.0, 50.0, 100.0]
    rng = np.random.default_rng(0)
    fold_ids = rng.permutation(len(y_value)) % 5
    best_lam, best_err = lambdas[0], np.inf
    for lam in lambdas:
        errs = []
        for f in range(5):
            test_mask = fold_ids == f
            train_mask = ~test_mask
            beta_cv = ridge_fit(X_train[train_mask], y_value[train_mask], lam)
            pred_cv = beta_cv[0] + X_train[test_mask] @ beta_cv[1:]
            errs.append(float(np.mean((pred_cv - y_value[test_mask]) ** 2)))
        mean_err = float(np.mean(errs))
        if mean_err < best_err:
            best_err, best_lam = mean_err, lam
    print(f"chosen ridge lambda (5-fold CV): {best_lam}")

    value_beta = ridge_fit(X_train, y_value, lam=best_lam)
    prob_beta = logistic_fit_irls(X_train, y_prob)

    fitted_value = value_beta[0] + X_train @ value_beta[1:]
    fitted_logit = prob_beta[0] + X_train @ prob_beta[1:]
    fitted_prob = 1.0 / (1.0 + np.exp(-fitted_logit))

    print(f"train in-sample Spearman(fit, career_value): {spearman_corr(fitted_value, y_value):.3f}")
    print(f"train in-sample AUC(draft_prob, drafted): {auc(y_prob, fitted_prob):.3f}")

    artifact = {
        "league": "nba",
        "features": COMBINE_FEATURES,
        "value_coef": value_beta[1:].tolist(),
        "value_intercept": float(value_beta[0]),
        "prob_coef": prob_beta[1:].tolist(),
        "prob_intercept": float(prob_beta[0]),
        "feature_median": feature_median,
        "feature_mean": feature_mean.tolist(),
        "feature_sd": feature_sd.tolist(),
        "cutoff_year": CUTOFF_YEAR,
    }
    with open(ARTIFACT_PATH, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2)
    print(f"wrote {ARTIFACT_PATH}")


if __name__ == "__main__":
    main()
