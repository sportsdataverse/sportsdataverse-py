"""xERA/SIERA-like (③) oracle gate — the Phase 4 gate.

(a) MAE(``x_era``, Savant's own ``xera`` leaderboard column) on the held-out
2024 population (15 pitchers, full season) joined to
``savant_expected_stats_2024.parquet``: observed 0.200 ERA points. Floor set
above the observed value (MAE is a "lower is better" metric — the floor here
is a ceiling on acceptable error), rounded up to a documented margin.

**SIERA next-season skill check deferred**: the plan calls for
Spearman(``siera_like``, next-season realized ERA) >= raw-ERA's own
autocorrelation. This needs two real, distinct seasons of the SAME pitchers
with a realized-ERA oracle for both. The only overlap available in this
session's captures is 4 pitchers (2023 sample season ∩ 2024 expected-stats
leaderboard) — too small for a reliable Spearman estimate (observed 0.0 with
n=4, which is not evidence of anything at that sample size, not a genuine
"no skill" finding). Rather than assert a floor on a statistically
meaningless n=4 correlation, this sub-check is deferred pending a larger
cross-season pitcher-overlap capture. The MAE leg (a) — the primary xERA
oracle check, and the one with real statistical power (n=15) — ships.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.mlb.mlb_pitch_era import mlb_pitch_era
from sportsdataverse.mlb.mlb_pitching_constants import mae

FIX = "tests/fixtures/mlb_pitching"

#: Observed MAE(x_era, Savant xera) on the 15-pitcher held-out 2024 population: 0.200.
#: Floor rounded up (ceiling on acceptable error), documented.
FLOOR_XERA_MAE = 0.30


def test_x_era_mae_vs_savant_xera():
    holdout = pl.read_parquet(f"{FIX}/pitcher_holdout_season_2024.parquet")
    mine = mlb_pitch_era(holdout, 2024)

    savant = pl.read_parquet(f"{FIX}/savant_expected_stats_2024.parquet")
    assert mine.schema["pitcher"] == savant.schema["pitcher"]

    joined = mine.join(savant.select("pitcher", "x_era"), on="pitcher", suffix="_sav")
    assert joined.height >= 10  # observed: all 15 holdout pitchers matched

    observed_mae = mae(joined["x_era"].to_numpy(), joined["x_era_sav"].to_numpy())
    assert observed_mae <= FLOOR_XERA_MAE
