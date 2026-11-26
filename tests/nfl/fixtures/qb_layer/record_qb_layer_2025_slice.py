"""Rebuild the QB-layer test slice (provenance in README.md): real nflverse 2025 rows, weeks 1-6.

From the repo root::

    PYTHONPATH=. uv run --frozen python tests/nfl/fixtures/qb_layer/record_qb_layer_2025_slice.py
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nfl import load_nfl_depth_charts, load_nfl_injuries, load_nfl_pbp, load_nfl_schedule

HERE = Path(__file__).parent
WEEKS = (1, 6)
PBP_COLS = [
    "game_id",
    "season",
    "week",
    "game_date",
    "posteam",
    "defteam",
    "home_team",
    "play_type",
    "qb_dropback",
    "qb_epa",
    "epa",
    "wp",
    "special",
    "qb_kneel",
    "qb_spike",
    "passer_player_id",
    "rusher_player_id",
]


def main() -> None:
    reg = pl.col("week").is_between(*WEEKS)
    sched = load_nfl_schedule([2025]).filter((pl.col("game_type") == "REG") & reg)
    sched = sched.select(
        "game_id",
        "season",
        "game_type",
        "week",
        "gameday",
        "gametime",
        "location",
        "home_team",
        "away_team",
        "home_qb_id",
        "away_qb_id",
        "result",
    )
    pbp = load_nfl_pbp([2025]).filter((pl.col("season_type") == "REG") & reg).select(PBP_COLS)
    inj = (
        load_nfl_injuries([2025])
        .filter(reg & (pl.col("position") == "QB"))
        .select("season", "week", "team", "gsis_id", "position", "report_status")
    )
    last = sched["gameday"].max()
    depth = (
        load_nfl_depth_charts([2025])
        .filter((pl.col("pos_abb") == "QB") & (pl.col("dt") >= "2025-08-20") & (pl.col("dt") <= f"{last}T23:59:59Z"))
        .select("dt", "team", "gsis_id", "pos_abb", "pos_rank")
    )
    for name, df in (("schedule", sched), ("pbp", pbp), ("injuries", inj), ("depth_charts", depth)):
        df.write_parquet(HERE / f"{name}_2025_w1_6.parquet", compression="zstd", compression_level=19)
        print(name, df.shape)


if __name__ == "__main__":
    main()
