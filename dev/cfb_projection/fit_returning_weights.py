"""Fit returning-production unit weights (T2.2 Task 2.3).

Regresses realized YoY scoring-margin change on standardized off/def returning
production (FBS, 2018-2023, >=6 games both seasons) and sets
``returning_prod_weights`` from the non-negative standardized coefficients.
Run from the repo root:  uv run python dev/cfb_projection/fit_returning_weights.py
"""

from __future__ import annotations

import numpy as np
import polars as pl

rp = pl.read_parquet("tests/fixtures/cfb_projection/returning_2017_2023.parquet")
res = pl.read_parquet("tests/fixtures/cfb_projection/results_2016_2023.parquet")

home = res.select(
    pl.col("season"),
    pl.col("home_team_id").alias("team_id"),
    (pl.col("home_score") - pl.col("away_score")).alias("m"),
)
away = res.select(
    pl.col("season"),
    pl.col("away_team_id").alias("team_id"),
    (pl.col("away_score") - pl.col("home_score")).alias("m"),
)
margins = (
    pl.concat([home, away])
    .group_by("season", "team_id")
    .agg(pl.col("m").mean().alias("avg_margin"), pl.len().alias("g"))
)
delta = (
    margins.join(
        margins.with_columns((pl.col("season") + 1).alias("season")).rename(
            {"avg_margin": "prior_margin", "g": "prior_g"}
        ),
        on=["season", "team_id"],
        how="inner",
    )
    .filter((pl.col("g") >= 6) & (pl.col("prior_g") >= 6))
    .with_columns((pl.col("avg_margin") - pl.col("prior_margin")).alias("margin_delta"))
)

j = (
    rp.filter(pl.col("classification") == "fbs")
    .drop_nulls(["team_id", "off_returning", "def_returning"])
    .join(delta, on=["season", "team_id"], how="inner")
)
X = np.column_stack([j["off_returning"].to_numpy(), j["def_returning"].to_numpy()])
X = (X - X.mean(axis=0)) / X.std(axis=0)
y = j["margin_delta"].to_numpy()
coef, *_ = np.linalg.lstsq(np.column_stack([np.ones(len(y)), X]), y, rcond=None)
b_off, b_def = coef[1], coef[2]
w = np.maximum([b_off, b_def], 0.0)
w = w / w.sum()
print(f"n={len(y)}  std-coefs: off={b_off:.4f} def={b_def:.4f}")
print(f"fitted returning_prod_weights: offense={w[0]:.3f} defense={w[1]:.3f}")
