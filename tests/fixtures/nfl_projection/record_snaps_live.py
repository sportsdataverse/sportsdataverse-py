"""Rebuild `snap_counts_live_2020_2023.parquet` + `ff_playerids_bridge.parquet`: loader rows as returned (README.md).

From the repo root::

    PYTHONPATH=. uv run --frozen python tests/fixtures/nfl_projection/record_snaps_live.py
"""

from pathlib import Path

import polars as pl

from sportsdataverse.nfl import load_nfl_ff_playerids, load_nfl_snap_counts

HERE = Path(__file__).parent
COLS = ["season", "game_type", "week", "pfr_player_id", "position", "team", "offense_snaps", "offense_pct"]

if __name__ == "__main__":
    snaps = load_nfl_snap_counts([2020, 2021, 2022, 2023]).select(COLS)
    ids = load_nfl_ff_playerids().select("pfr_id", "gsis_id", "position")
    ids = ids.filter(pl.col("pfr_id").is_in(snaps["pfr_player_id"].unique().implode()))
    for name, df in (("snap_counts_live_2020_2023", snaps), ("ff_playerids_bridge", ids)):
        df.write_parquet(HERE / f"{name}.parquet", compression="zstd", compression_level=19)
        print(name, df.shape, df.schema)
