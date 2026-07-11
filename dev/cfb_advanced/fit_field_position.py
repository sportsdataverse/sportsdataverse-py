"""Fit + bundle the CFB EP-by-starting-yardline curve (T2.3 Phase 4).

Reads 2018-2021 pbp from the LOCAL cfbfastR-data checkout (the hosted
load_cfb_pbp asset; 2022+ 404s -- producer gap), derives one row per drive
with the net next-score points inside the half (offense-signed), and fits a
count-weighted isotonic curve via
sportsdataverse.cfb.cfb_field_position.fit_field_position_ep.

Writes sportsdataverse/cfb/models/cfb_field_position_ep.parquet (committed).
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb.cfb_field_position import (
    FP_PBP_COLS,
    _drives_from_pbp,
    fit_field_position_ep,
)

SRC = "c:/Users/saiem/Documents/GitHub-Data/sdv-dev/cfbfastR-dev/cfbfastR-data/pbp/parquet/play_by_play_{y}.parquet"
SEASONS = [2018, 2019, 2020, 2021]
OUT = "sportsdataverse/cfb/models/cfb_field_position_ep.parquet"


def next_score_pts(d: pl.DataFrame) -> pl.DataFrame:
    """Add drive_next_score_pts: net next-score points in the half, offense-signed."""
    d = d.sort(["game_id", "half", "order"]).with_columns(
        nz_pts=pl.when(pl.col("drive_pts") != 0).then(pl.col("drive_pts")),
        nz_team=pl.when(pl.col("drive_pts") != 0).then(pl.col("team_id")),
    )
    d = d.with_columns(
        next_pts=pl.col("nz_pts").backward_fill().over(["game_id", "half"]),
        next_team=pl.col("nz_team").backward_fill().over(["game_id", "half"]),
    )
    return d.with_columns(
        drive_next_score_pts=pl.when(pl.col("next_pts").is_null())
        .then(0.0)
        .when(pl.col("next_team") == pl.col("team_id"))
        .then(pl.col("next_pts"))
        .otherwise(-pl.col("next_pts"))
    )


def main() -> None:
    frames = []
    for y in SEASONS:
        pbp = pl.scan_parquet(SRC.format(y=y)).select(list(FP_PBP_COLS)).rename(FP_PBP_COLS).collect()
        d = _drives_from_pbp(pbp, exclude_garbage=True)
        frames.append(next_score_pts(d))
        print(y, "drives:", d.height)
    drives = pl.concat(frames)
    # Target choice (2026-07-08, anchors experiment): offense-signed realized
    # drive points ("drive_pts": TD +7 / FG +3 / SF -2 / return-TD -7).
    # With drive-level starts (drive.start.yardLine + homeTeamId orientation)
    # it matches the published anchors at own-25 (1.803 vs 1.4), midfield
    # (2.820 vs 2.8), opp-25 (3.674 vs 4.1) and opp-5 (5.387 vs 5.6) within
    # +-0.6; the within-half net-next-score target ("drive_next_score_pts",
    # kept above for comparison) sits ~1.3 pts lower across own territory.
    # Own-1 (0.857 vs ref -0.5) is a documented divergence: realized drive
    # points bottom out near 0.8 at the goal line (no opponent-next-score
    # negative term) -- see tests/cfb/test_cfb_field_position.py.
    curve = fit_field_position_ep(drives, start_col="start_yardline_own", pts_col="drive_pts")
    curve.write_parquet(OUT)
    anchors = {yl: round(curve.filter(pl.col("yardline_own") == yl)["ep"][0], 3) for yl in (1, 25, 50, 75, 95)}
    print("curve rows:", curve.height, "anchors:", anchors)


if __name__ == "__main__":
    main()
