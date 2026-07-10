"""Expand the draft/combine corpus back to 2000-2015 (20 classes total).

The initial Task-0.1 capture (2016-2019, ~250 players) turned out to be too
small a sample for the draft-model (Task 1.3/1.4) ridge/logistic heads to
find any real holdout signal -- exhaustively debugged (feature-subset sweeps,
lambda sweeps, xgboost escalation, multiple train/holdout cutoffs all
converged to ~0 or negative holdout Spearman). The all-era career-value
label was specifically designed to span decades (design doc §3.4) precisely
so the corpus wouldn't be capped at the ~4 seasons of v3-pbp overlap -- this
script realizes that by pulling combine classes back to 2000.

Appends to (does not replace) the existing committed fixtures:
    combine_2016_2019.parquet, draft_outcomes.parquet, season_stats_raw.parquet

nba_bpm_overlap.parquet / aging_published.parquet are NOT touched (the
box-value fit already achieved in-sample Spearman 0.93 on the 2016-2019
overlap; more overlap-era seasons wouldn't materially change those
coefficients, and BPM/box-logs only exist from 2016-17 anyway -- v3 pbp).

Run: ``SDV_PY_NBA_STATS_LIVE=1 uv run python dev/nba_draft/capture_corpus_expand.py``
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from capture_corpus import capture_combine, capture_draft_outcomes, capture_season_stats  # noqa: E402

FIXTURE_DIR = "tests/fixtures/nba_draft"
EXPAND_YEARS = [str(y) for y in range(2000, 2016)]


def main() -> None:
    print(f"Capturing combine classes {EXPAND_YEARS[0]}-{EXPAND_YEARS[-1]} ...")
    new_combine = capture_combine(EXPAND_YEARS)
    old_combine = pl.read_parquet(f"{FIXTURE_DIR}/combine_2016_2019.parquet")
    combine = pl.concat([old_combine, new_combine], how="diagonal_relaxed").unique(subset=["player_id"], keep="first")
    combine.write_parquet(f"{FIXTURE_DIR}/combine_2016_2019.parquet")
    print(f"  combine corpus now {combine.height} rows ({combine['draft_year'].n_unique()} classes)")

    new_ids = new_combine["player_id"].unique().to_list()

    print("Capturing draft outcomes for new players ...")
    new_outcomes = capture_draft_outcomes(new_ids)
    old_outcomes = pl.read_parquet(f"{FIXTURE_DIR}/draft_outcomes.parquet")
    outcomes = pl.concat([old_outcomes, new_outcomes], how="diagonal_relaxed").unique(
        subset=["player_id"], keep="first"
    )
    outcomes.write_parquet(f"{FIXTURE_DIR}/draft_outcomes.parquet")
    print(f"  draft_outcomes corpus now {outcomes.height} rows")

    print("Capturing season totals for new players ...")
    new_season_stats = capture_season_stats(new_ids)
    old_season_stats = pl.read_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    season_stats = pl.concat([old_season_stats, new_season_stats], how="diagonal_relaxed")
    season_stats.write_parquet(f"{FIXTURE_DIR}/season_stats_raw.parquet")
    print(f"  season_stats_raw corpus now {season_stats.height} rows")

    print("Done. Re-run dev/nba_draft/fit_box_value.py to re-materialize")
    print("career_values.parquet / rookie_values.parquet from the expanded corpus.")


if __name__ == "__main__":
    time.sleep(0)  # keep the shared SLEEP_S pacing from capture_corpus's helpers
    main()
