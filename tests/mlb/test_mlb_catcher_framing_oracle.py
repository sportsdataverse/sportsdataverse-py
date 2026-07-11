"""Catcher-framing oracle gate vs Savant's real 2024 leaderboard.

Corpus: tests/fixtures/mlb_fielding/pitches_2024-06.parquet (one month, 116k
pitches) vs lb_catcher_framing_2024.parquet (a FULL-SEASON leaderboard --
see tests/fixtures/mlb_fielding/README.md). This is a month-vs-season scope
mismatch; the like-for-like FULL-SEASON gate lives in
``test_mlb_fielding_oracle_live.py`` (``@skip_if_no_live``).

Gate (never lower to pass -- debug the model instead): Pearson >= 0.50
(observed 0.547 at min_takes>=500). ``mlb_catcher_framing`` now follows
Savant's method -- a smooth logistic P(called strike | zone location) with
framing runs summed over SHADOW-ZONE takes only -- replacing the old coarse
empirical grid. Measured like-for-like on the FULL 2024 season that raised the
correlation from **0.435 to 0.468** (n=44 -- see the live gate); on this one
month the two models are within noise (grid 0.556, logistic 0.547, n~33), so
the floor is held at 0.50 rather than raised. The full-season 0.47 is the
honest ceiling, NOT the design's 0.90: the public per-pitch feed lacks the
pitch movement / release-point / receiving-path signals Savant's framing model
uses, and Spearman ~= Pearson (~0.45) confirms the cap is feature poverty,
not outliers.

Real Savant column names (differ from the plan's assumed names): the
leaderboard's id column is `id` (not `player_id`), and the framing-runs
column is `rv_tot` (not `runs_extra_strikes` -- no such column exists in the
real payload).
"""

import polars as pl

from sportsdataverse.mlb.mlb_catcher_framing import mlb_catcher_framing
from sportsdataverse.mlb.mlb_run_values import pearson_corr

FIXTURE_DIR = "tests/fixtures/mlb_fielding"


def test_framing_matches_savant_month_vs_season():
    pitches = pl.read_parquet(f"{FIXTURE_DIR}/pitches_2024-06.parquet")
    sav = pl.read_parquet(f"{FIXTURE_DIR}/lb_catcher_framing_2024.parquet")

    mine = mlb_catcher_framing(pitches).filter(pl.col("takes") >= 500)
    assert mine.schema["catcher_id"] == pl.Utf8

    sav = sav.with_columns(pl.col("id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id"))
    assert sav.schema["catcher_id"] == mine.schema["catcher_id"]

    j = mine.join(sav.select("catcher_id", "rv_tot"), on="catcher_id", how="inner")
    assert j.height >= 20, f"join produced only {j.height} catchers -- capture too sparse to gate"

    r = pearson_corr(j["framing_runs"].to_numpy(), j["rv_tot"].to_numpy())
    assert r >= 0.50, f"framing corr {r:.3f} < 0.50 -- debug grid/normalization/run-value, do NOT lower the gate"
