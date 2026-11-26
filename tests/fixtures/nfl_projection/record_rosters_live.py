"""Rebuild `rosters_live_2020_2023.parquet`: `load_nfl_rosters` rows as the loader returns them (README.md).

From the repo root::

    PYTHONPATH=. uv run --frozen python tests/fixtures/nfl_projection/record_rosters_live.py
"""

from pathlib import Path

import polars as pl

from sportsdataverse.nfl import load_nfl_rosters

HERE = Path(__file__).parent

if __name__ == "__main__":
    ids = pl.read_parquet(HERE / "rosters_2020_2023.parquet")["player_id"].unique().to_list()
    live = load_nfl_rosters([2020, 2021, 2022, 2023]).select("gsis_id", "season", "position", "birth_date")
    live = live.filter(pl.col("gsis_id").is_in(ids))
    live.write_parquet(HERE / "rosters_live_2020_2023.parquet", compression="zstd", compression_level=19)
    print(live.shape, live.schema)
