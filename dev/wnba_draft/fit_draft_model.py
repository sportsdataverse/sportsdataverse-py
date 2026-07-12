"""Fit the WNBA draft-value artifact (ridge `proj_career_value`; `draft_prob` head, see below).

Genuine re-fit -- the shipped T3.4 artifact was an all-zero placeholder
(``value_coef=[0,0]``, ``prob_coef=[0,0]``), not an NBA relabel (``wnba_draft_model`` was
already correctly scoped to the draft-slot feature set ``["overall_pick", "round_number"]``,
never the NBA combine features -- see ``wnba_draft_constants.py``'s coverage caveat).

Reads the committed Task-5.1 fixtures (offline, no live network needed):
``draft_history.parquet`` (``overall_pick``/``round_number`` -- pre-career-value, non-leaking
features) and ``career_values.parquet`` (all-era realized career value, computed in
``fit_box_value.py`` from real WNBA box totals). Ridge-fits
``career_value ~ [overall_pick, round_number]`` on classes ``<= CUTOFF_YEAR``
(``as_of_class_split``), the same leakage boundary the NBA fit uses.

**`draft_prob` is a documented constant, not a fitted classifier.** Unlike the NBA corpus
(combine invitees who then may or may not get drafted -- a real positive/negative population),
``wnba_stats_drafthistory`` contains *only drafted* players: there is no WNBA combine-invitee
list to supply the negative class (confirmed live 2026-07-08:
``wnba_stats_draftcombinestats`` returns 0 rows for every WNBA season). A logistic regression
with a single-class label (`y` all 1) is degenerate -- fitting IRLS against it would either
diverge or silently produce a meaningless separator, which is worse than being explicit. Per
the "never fake a fit" rule, ``prob_coef`` stays ``[0.0, 0.0]`` (no per-feature signal) and
``prob_intercept`` is set so ``sigmoid(prob_intercept) ~= 0.99`` (reflecting the true base rate
of this corpus: every scoreable player *was* drafted) instead of the old placeholder's
``intercept=0.0`` -> ``0.5`` (which wrongly implied a coin-flip). `pro_tier` (the field actually
consumed downstream) is derived from `proj_career_value`'s rank, not from `draft_prob` --
`wnba_draft_model`'s tier logic is unaffected by this constant.

Run: ``uv run python dev/wnba_draft/fit_draft_model.py``
"""

from __future__ import annotations

import json

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_draft_constants import as_of_class_split, ridge_fit, spearman_corr

FIXTURE_DIR = "tests/fixtures/wnba_draft"
ARTIFACT_PATH = "sportsdataverse/nba/models/wnba_draft_value.json"
FEATURES = ["overall_pick", "round_number"]
# See progress.md for the class-count/holdout-size trade-off that picked this cutoff --
# 29 draft classes (1997-2025) span WNBA history; the cutoff leaves enough post-cutoff classes
# with realized multi-season careers by fit time (2026) to backtest against, while keeping the
# most recent 1-2 classes (minimal accumulated career value by construction) out of train.
CUTOFF_YEAR = 2018
# sigmoid(4.6) ~= 0.99 -- see module docstring: this is a documented constant, not a fit.
_DRAFT_PROB_CONSTANT_LOGIT = 4.6


def main() -> None:
    draft_history = pl.read_parquet(f"{FIXTURE_DIR}/draft_history.parquet")
    career = pl.read_parquet(f"{FIXTURE_DIR}/career_values.parquet")
    assert draft_history.schema["player_id"] == career.schema["player_id"] == pl.Utf8

    df = (
        draft_history.select("player_id", "draft_year", "overall_pick", "round_number")
        .with_columns(
            pl.col("overall_pick").cast(pl.Float64, strict=False), pl.col("round_number").cast(pl.Float64, strict=False)
        )
        .join(career.select("player_id", "career_value"), on="player_id", how="left")
        .with_columns(pl.col("career_value").fill_null(0.0))
    )

    train, holdout = as_of_class_split(df, cutoff_year=CUTOFF_YEAR)
    feature_median = {c: float(train[c].drop_nulls().median() or 0.0) for c in FEATURES}
    train = train.with_columns([pl.col(c).fill_null(feature_median[c]) for c in FEATURES])
    print(f"train classes {sorted(train['draft_year'].unique().to_list())}: {train.height} prospects")
    print(f"holdout classes {sorted(holdout['draft_year'].unique().to_list())}: {holdout.height} prospects")

    X_train_raw = train.select(FEATURES).to_numpy()
    feature_mean = X_train_raw.mean(axis=0)
    feature_sd = X_train_raw.std(axis=0)
    feature_sd[feature_sd == 0.0] = 1.0
    X_train = (X_train_raw - feature_mean) / feature_sd
    y_value = train["career_value"].to_numpy()

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
    fitted_value = value_beta[0] + X_train @ value_beta[1:]
    print(f"train in-sample Spearman(fit, career_value): {spearman_corr(fitted_value, y_value):.3f}")
    # sign sanity check: better (lower) overall_pick should predict HIGHER career value.
    pick_idx = FEATURES.index("overall_pick")
    print(f"overall_pick standardized coef: {value_beta[1 + pick_idx]:.4f} (expect negative)")

    artifact = {
        "league": "wnba",
        "features": FEATURES,
        "value_coef": value_beta[1:].tolist(),
        "value_intercept": float(value_beta[0]),
        "prob_coef": [0.0, 0.0],
        "prob_intercept": _DRAFT_PROB_CONSTANT_LOGIT,
        "feature_median": feature_median,
        "feature_mean": feature_mean.tolist(),
        "feature_sd": feature_sd.tolist(),
        "cutoff_year": CUTOFF_YEAR,
        "_draft_prob_note": (
            "prob_coef/prob_intercept are a documented CONSTANT (~0.99), not a fitted "
            "classifier -- wnba_stats_drafthistory has no undrafted/invitee negative class "
            "to fit against (draftcombinestats returns 0 rows). See fit_draft_model.py."
        ),
    }
    with open(ARTIFACT_PATH, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2)
    print(f"wrote {ARTIFACT_PATH}")


if __name__ == "__main__":
    main()
