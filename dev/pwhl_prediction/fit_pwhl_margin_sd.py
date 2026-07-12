"""One-off: grid-search margin_sd (holding shrink_k/hfa fixed) to minimize
Brier on the committed PWHL backtest predictions, mirroring how the NHL
constants' margin_sd was fit ("minimising Brier of Phi(exp_margin/margin_sd)
directly"). Informs whether PWHL LEAGUE_CONSTANTS should be refit as a
byproduct of this session (not committed logic itself).
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.stats import norm

from sportsdataverse.nhl.nhl_prediction_constants import brier_score, calibration_table

bt = pl.read_parquet("tests/fixtures/pwhl_prediction/backtest_predictions_2024_2026.parquet")
y = bt["home_win"].to_numpy()
margin = bt["exp_margin"].to_numpy()
print("exp_margin stats: min", margin.min(), "max", margin.max(), "std", margin.std())

best = None
for sd in np.arange(0.05, 3.0, 0.02):
    p = norm.cdf(margin / sd)
    b = brier_score(y, p)
    if best is None or b < best[1]:
        best = (sd, b)
print("best margin_sd:", best)

sd = best[0]
p = norm.cdf(margin / sd)
print("Brier at best sd:", brier_score(y, p))
print("naive Brier:", brier_score(y, np.full(len(y), 0.5)))
for n_bins in (3, 5, 8):
    cal = calibration_table(y, p, n_bins=n_bins)
    print(f"n_bins={n_bins}:")
    print(cal)
