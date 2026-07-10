"""Fit the all-era career-value box formula (`box_value_per100`) vs `nba_bpm`.

Reads the Task-0.1 corpus (``tests/fixtures/nba_draft/season_stats_raw.parquet``
+ ``nba_bpm_overlap.parquet``), computes per-(player_id, season) per-100 box
rates, ridge-fits ``bpm ~ box_value_per100(feats)`` (5-fold CV over a lambda
grid), sets ``REPLACEMENT`` to the 10th-percentile fitted value (the standard
VORP replacement-level convention), and prints the coefficients to paste into
``LEAGUE_CONSTANTS["nba"]`` in ``nba_draft_constants.py``.

It also materializes the two derived corpus fixtures that depend on the
fitted coefficients: ``career_values.parquet`` (all combine-class players,
all-era) and ``rookie_values.parquet`` (rookie/soph season values for the
same players). See ``capture_corpus.py`` module docstring for why these two
are produced here instead of the raw-capture script.

Run: ``uv run python dev/nba_draft/fit_box_value.py`` (offline -- reads only
the committed fixtures, no live network needed).
"""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_draft_constants import BOX_VALUE_FEATURES, ridge_fit, spearman_corr

FIXTURE_DIR = "tests/fixtures/nba_draft"
MIN_MINUTES = 300.0


def add_per100_rates(season_stats: pl.DataFrame) -> pl.DataFrame:
    """Per-player-estimated-possessions per-100 rates (documented approximation).

    ``playercareerstats`` season totals carry no team-pace context, so
    possessions are estimated per-player as ``fga + 0.44*fta + tov`` (the
    standard individual scoring-possession estimate). This is *not*
    team-pace-adjusted true usage%, but is sufficient signal for the
    rank-based (Spearman) oracle gate this label ultimately serves.
    """
    pos_est = pl.col("fga") + 0.44 * pl.col("fta") + pl.col("tov")
    return season_stats.with_columns(
        pos_est.alias("_pos_est"),
        pl.col("season_id").str.slice(0, 4).cast(pl.Int64).alias("season"),
    ).with_columns(
        (pl.col("pts") / pl.col("_pos_est").clip(1.0, None) * 100).alias("pts100"),
        (pl.col("reb") / pl.col("_pos_est").clip(1.0, None) * 100).alias("reb100"),
        (pl.col("ast") / pl.col("_pos_est").clip(1.0, None) * 100).alias("ast100"),
        (pl.col("stl") / pl.col("_pos_est").clip(1.0, None) * 100).alias("stl100"),
        (pl.col("blk") / pl.col("_pos_est").clip(1.0, None) * 100).alias("blk100"),
        (pl.col("tov") / pl.col("_pos_est").clip(1.0, None) * 100).alias("tov100"),
        (pl.col("pts") / (2 * (pl.col("fga") + 0.44 * pl.col("fta")).clip(1.0, None))).alias("ts_pct"),
        (pl.col("_pos_est") / pl.col("min").clip(1.0, None) * 100).alias("usg"),
        pl.col("min").alias("minutes"),
    )


def cv_ridge_lambda(X: np.ndarray, y: np.ndarray, w: np.ndarray, lambdas: list[float], *, folds: int = 5) -> float:
    rng = np.random.default_rng(0)
    idx = rng.permutation(len(y))
    fold_ids = idx % folds
    best_lam, best_err = lambdas[0], np.inf
    for lam in lambdas:
        errs = []
        for f in range(folds):
            test_mask = fold_ids == f
            train_mask = ~test_mask
            if train_mask.sum() < 5 or test_mask.sum() < 1:
                continue
            beta = ridge_fit(X[train_mask], y[train_mask], lam)
            pred = beta[0] + X[test_mask] @ beta[1:]
            errs.append(np.average((pred - y[test_mask]) ** 2, weights=w[test_mask]))
        mean_err = float(np.mean(errs)) if errs else np.inf
        if mean_err < best_err:
            best_err, best_lam = mean_err, lam
    return best_lam


def main() -> None:
    season_stats = pl.read_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    bpm_overlap = pl.read_parquet(f"{FIXTURE_DIR}/nba_bpm_overlap.parquet")

    rates = add_per100_rates(season_stats)
    assert rates.schema["player_id"] == bpm_overlap.schema["player_id"] == pl.Utf8

    anchor = rates.join(bpm_overlap, on=["player_id", "season"], how="inner").filter(pl.col("minutes") >= MIN_MINUTES)
    print(f"anchor rows (combine-player seasons w/ bpm overlap, min {MIN_MINUTES} minutes): {anchor.height}")

    X = anchor.select(BOX_VALUE_FEATURES).fill_null(0.0).to_numpy()
    y = anchor["bpm"].to_numpy()
    w = anchor["minutes"].to_numpy()

    lam = cv_ridge_lambda(X, y, w, lambdas=[0.01, 0.1, 1.0, 10.0, 50.0, 100.0])
    beta = ridge_fit(X, y, lam)
    fitted = beta[0] + X @ beta[1:]
    replacement = float(np.percentile(fitted, 10))
    rho = spearman_corr(fitted, y)

    print(f"chosen lambda: {lam}")
    print(f"box_value_coef (intercept + {BOX_VALUE_FEATURES}):")
    print("  " + ", ".join(f"{b:.4f}" for b in beta))
    print(f"replacement (10th pct of fitted): {replacement:.4f}")
    print(f"in-sample Spearman(fit, nba_bpm): {rho:.4f}")

    # Materialize career_values.parquet + rookie_values.parquet using the
    # freshly fit coefficients (avoids the Task 0.1/0.3 chicken-and-egg: the
    # label needs coefficients that can only be fit from this same corpus).
    all_rates = add_per100_rates(season_stats).with_columns(
        (beta[0] + sum(pl.col(f).fill_null(0.0) * c for f, c in zip(BOX_VALUE_FEATURES, beta[1:]))).alias("_box_value")
    )
    all_rates = all_rates.with_columns(
        ((pl.col("_box_value") - replacement) * pl.col("minutes") / 1000.0).alias("_season_vorp")
    )
    career_values = (
        all_rates.group_by("player_id")
        .agg(
            pl.col("_season_vorp").sum().alias("career_value"),
            pl.len().alias("seasons_played"),
            pl.col("minutes").sum().alias("total_minutes"),
        )
        .with_columns(pl.col("seasons_played").cast(pl.Int64))
    )
    career_values.write_parquet(f"{FIXTURE_DIR}/career_values.parquet")
    print(f"wrote career_values.parquet ({career_values.height} rows)")

    # rookie/soph: first two season rows per player, ordered by season.
    ordered = all_rates.sort("player_id", "season").with_columns(
        pl.int_range(0, pl.len()).over("player_id").alias("_season_idx")
    )
    combine = pl.read_parquet(f"{FIXTURE_DIR}/combine_2016_2019.parquet").select("player_id", "draft_year")
    rookie = ordered.filter(pl.col("_season_idx") == 0).select(
        "player_id", pl.col("_season_vorp").alias("rookie_value"), pl.col("minutes").alias("rookie_min")
    )
    soph = ordered.filter(pl.col("_season_idx") == 1).select("player_id", pl.col("_season_vorp").alias("soph_value"))
    rookie_values = (
        combine.join(rookie, on="player_id", how="left")
        .join(soph, on="player_id", how="left")
        .with_columns(
            pl.col("rookie_value").fill_null(0.0),
            pl.col("soph_value").fill_null(0.0),
            pl.col("rookie_min").fill_null(0.0),
        )
    )
    rookie_values.write_parquet(f"{FIXTURE_DIR}/rookie_values.parquet")
    print(f"wrote rookie_values.parquet ({rookie_values.height} rows)")


if __name__ == "__main__":
    main()
