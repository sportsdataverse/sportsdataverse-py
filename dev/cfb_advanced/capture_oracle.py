"""Capture the 2021 pbp slice fixture (T2.3 Phase 0) + fp_reference anchors.

Reads the LOCAL cfbfastR-data checkout (offline; the hosted load_cfb_pbp
parquet covers 2002-2021 only -- 2022+ 404s, escalated to cfb-data backfill).
Down-selects the full 2021 season to the columns the advanced-stats models
consume, mirroring the T2.1 pbp_2023_sample precedent.

PBP_COLS maps real released-pbp names -> canonical build_play_long names.
"""

from __future__ import annotations

import polars as pl

SRC = "c:/Users/saiem/Documents/GitHub-Data/sdv-dev/cfbfastR-dev/cfbfastR-data/pbp/parquet/play_by_play_2021.parquet"
OUT_DIR = "tests/fixtures/cfb_advanced"

#: real released-pbp column name -> canonical name (Task 0.1 Step 2 discovery)
PBP_COLS: dict[str, str] = {
    "game_id": "game_id",
    "season": "season",
    "week": "week",
    "wallclock": "wallclock",
    "period": "period",
    "down": "down",
    "distance": "distance",
    "statYardage": "yards_gained",
    "EPA": "epa",
    "pass": "pass",
    "rush": "rush",
    "havoc": "havoc",
    "scrimmage_play": "scrimmage_play",
    "pos_team_score": "pos_team_score",
    "def_pos_team_score": "def_pos_team_score",
    "start.pos_team.id": "pos_team_id",
    "start.def_pos_team.id": "def_pos_team_id",
    "pos_team": "pos_team",
    "def_pos_team": "def_pos_team",
    "type.text": "play_type",
    "drive.id": "drive_id",
    "drive.result": "drive_result",
    "start.yardsToEndzone": "yards_to_goal",
    "start.TimeSecsRem": "start_time_secs_rem",
    "end.TimeSecsRem": "end_time_secs_rem",
    "drive_play_index": "drive_play_index",
}


def main() -> None:
    lf = pl.scan_parquet(SRC).select(list(PBP_COLS)).rename(PBP_COLS)
    df = lf.collect()
    df.write_parquet(f"{OUT_DIR}/pbp_slice_2021.parquet", compression="zstd")
    print("pbp_slice_2021:", df.shape)

    # Published EP-by-field-position anchors (yardline from OWN goal, net
    # next-score expected points; Connelly/GameOnPaper-style references).
    fp = pl.DataFrame(
        {
            "yardline_own": [1, 25, 50, 75, 95],
            "ep": [-0.5, 1.4, 2.8, 4.1, 5.6],
        },
        schema={"yardline_own": pl.Int64, "ep": pl.Float64},
    )
    fp.write_parquet(f"{OUT_DIR}/fp_reference.parquet")
    print("fp_reference:", fp.shape)


if __name__ == "__main__":
    main()
