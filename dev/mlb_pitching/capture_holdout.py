"""Scratch (gitignored) -- captures a genuine held-out 2024 full-season pitch
fixture for the Stuff+/Command+/xERA oracle gates.

Why this exists: the Task 0.1 single-day fixture (2024-06-15) turned out to be
too small-sample per pitcher-arsenal for a stable calibration/Spearman
check (per-pitcher within-day standardization is noisy with only a handful of
pitches), and the 2023 pitcher-season sample OVERLAPS the Stuff+/Command+
training corpus (same discovery week, same top-N-by-volume selection) so it
is not a fair out-of-sample check. This script discovers a fresh set of
pitchers from a DIFFERENT week/season than either the day fixture or the
training corpus, explicitly excludes the training corpus's 30 pitcher ids,
and pulls their full 2024 season -- giving a real, out-of-sample,
full-season fixture to join against the real 2024 Savant leaderboards.

Run with: SDV_PY_LIVE_TESTS=1 uv run python dev/mlb_pitching/capture_holdout.py
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.mlb import mlb_statcast_search

FIX = "tests/fixtures/mlb_pitching"

# The 30 pitcher ids used by dev/mlb_pitching/fit_stuff_plus.py's training corpus
# (2023-06-01..06-07 probe, top-30 by pitch count) -- excluded here for a clean holdout.
_TRAINING_IDS = {
    434378,
    450203,
    453286,
    506433,
    543475,
    592332,
    592351,
    592791,
    592836,
    605135,
    605200,
    605397,
    608379,
    641154,
    642547,
    656427,
    657277,
    657756,
    663687,
    665152,
    665795,
    666200,
    668678,
    669203,
    669854,
    672710,
    676440,
    676664,
    682227,
    682847,
}


def main() -> None:
    probe = mlb_statcast_search("2024-08-01", "2024-08-07", player_type="pitcher").with_columns(
        pl.col("pitcher").cast(pl.Int64)
    )
    counts = probe.group_by("pitcher").agg(pl.len().alias("n")).sort("n", descending=True)
    holdout_ids = [p for p in counts["pitcher"].to_list() if p not in _TRAINING_IDS][:15]
    assert not (set(holdout_ids) & _TRAINING_IDS), "holdout must not overlap the Stuff+/Command+ training corpus"
    print("holdout pitcher ids", holdout_ids)

    season = mlb_statcast_search("2024-03-28", "2024-10-01", player_type="pitcher", pitchers_lookup=holdout_ids)
    season = season.with_columns(
        [
            pl.col(c).cast(pl.Int64)
            for c in ("pitcher", "batter", "game_pk", "at_bat_number", "pitch_number", "balls", "strikes", "inning")
            if c in season.columns
        ]
    )
    season.write_parquet(f"{FIX}/pitcher_holdout_season_2024.parquet")
    print("holdout season sample", season.height)


if __name__ == "__main__":
    main()
