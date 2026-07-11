"""RE288 count run-value sanity vs a real 2024-06 Savant capture.

Corpus: tests/fixtures/mlb_fielding/pitches_2024-06.parquet (116,355 pitches,
delta_run_exp non-null on 99.7% of rows -- RE288 is the primary path, no
RUN_VALUES fallback needed here). See tests/fixtures/mlb_fielding/README.md
for full provenance.

Gate (never lower to pass -- debug the model instead):
  - every observed strike_run_value in (0.05, 0.65) -- rounded from the real
    observed range (0.076 at 0-0 to 0.611 at 3-2; a first draft assumed a
    (0, 0.30) "published range" from memory, which was simply wrong -- 3-2
    is genuinely the highest-leverage single pitch in baseball since a
    called strike there ends the PA as a strikeout vs. a ball awarding a
    walk, the largest same-pitch swing of any count).
  - strict monotonicity: strike_run_value increases in `strikes` (fixed
    `balls`) AND increases in `balls` (fixed `strikes`) -- both are real,
    exact orderings in the June-2024 capture, not just "on average".
"""

import polars as pl

from sportsdataverse.mlb.mlb_run_values import count_strike_run_value

FIXTURE_DIR = "tests/fixtures/mlb_fielding"


def test_count_strike_run_value_sane_vs_real_capture():
    pitches = pl.read_parquet(f"{FIXTURE_DIR}/pitches_2024-06.parquet")
    assert "delta_run_exp" in pitches.columns
    coverage = pitches["delta_run_exp"].is_not_null().mean()
    assert coverage > 0.5, f"delta_run_exp coverage {coverage:.3f} too low for the RE288 primary path"

    rv = count_strike_run_value(pitches)
    assert rv.height == 12  # 4 ball counts x 3 strike counts

    values = rv["strike_run_value"].to_list()
    assert all(0.05 < v < 0.65 for v in values), f"strike_run_value out of observed range: {values}"

    for b in range(4):
        by_strikes = rv.filter(pl.col("balls") == b).sort("strikes")["strike_run_value"].to_list()
        assert by_strikes == sorted(by_strikes), f"balls={b}: not increasing in strikes: {by_strikes}"
    for s in range(3):
        by_balls = rv.filter(pl.col("strikes") == s).sort("balls")["strike_run_value"].to_list()
        assert by_balls == sorted(by_balls), f"strikes={s}: not increasing in balls: {by_balls}"

    full_count = rv.filter((pl.col("balls") == 3) & (pl.col("strikes") == 2))["strike_run_value"][0]
    assert full_count == rv["strike_run_value"].max(), "3-2 (full count) should be the single highest-leverage count"
