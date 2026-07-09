"""Fit the clutch-shrinkage sampling-variance scale (Phase 4, Task 4.3).

Gitignored working script. Run:

    uv run python dev/nba_prediction/fit_clutch_shrinkage.py

Estimates the per-team clutch-net sampling-variance scale ``s`` in
``σ²_i = s / clutch_poss`` from season-to-season test-retest reliability:
with reliability ``r = corr(clutch_net_N, clutch_net_N+1)``, the noise
variance is ``(1 - r) * var(clutch_net)`` and ``s ≈ noise_var * mean_poss``.
Paste the printed scale into ``_CLUTCH_SIGMA2_SCALE`` in
``sportsdataverse/nba/nba_clutch.py``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nba_prediction"


def fit() -> float:
    c23 = pl.read_parquet(FIXTURE_DIR / "clutch_team_2023.parquet")
    c24 = pl.read_parquet(FIXTURE_DIR / "clutch_team_2024.parquet")
    j = c23.join(c24, on="team_id", how="inner", suffix="_24")
    a = j["clutch_net_rating"].to_numpy()
    b = j["clutch_net_rating_24"].to_numpy()
    r = float(np.corrcoef(a, b)[0, 1])
    total_var = float(np.var(np.concatenate([a, b])))
    mean_poss = float(np.mean(np.concatenate([j["clutch_poss"].to_numpy(), j["clutch_poss_24"].to_numpy()])))
    noise_var = max((1.0 - r) * total_var, 0.0)
    scale = noise_var * mean_poss
    print(
        f"reliability r={r:.3f} total_var={total_var:.1f} mean_poss={mean_poss:.0f} noise_var={noise_var:.1f} scale={scale:.0f}"
    )
    return scale


if __name__ == "__main__":
    fit()
