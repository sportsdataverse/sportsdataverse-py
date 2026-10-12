"""Rebuild `espn_qbr_2023_slice.parquet` (task 2.7's QBR oracle; provenance in README.md).

Runs NFLPlayProcess offline over nfl-raw's committed 2023 ESPN summaries, keeps each passer's six QBR model
inputs, and joins ESPN Total QBR (`load_nfl_espn_qbr([2023], summary_type="week")`) on ESPN game id plus
first initial + last name. Never downloads a model. About 3 minutes on 6 processes; from the repo root:

    PYTHONPATH=. uv run --frozen --all-extras python tests/nfl/fixtures/record_espn_qbr_2023_slice.py \
        <nflverse-dev/nfl-raw>/nfl/espn/raw/2023
"""

from __future__ import annotations

import gzip
import json
import sys
from multiprocessing import Pool
from pathlib import Path

import polars as pl

import sportsdataverse.nfl.nfl_fourth_down as nfl_fourth_down
from sportsdataverse.nfl import NFLPlayProcess, load_nfl_espn_qbr

INPUTS = ["qbr_epa", "pass_epa", "rush_epa", "sack_epa", "pen_epa", "spread"]
OUT = Path(__file__).parent / "espn_qbr_2023_slice.parquet"


def _key(first: pl.Expr, last: pl.Expr) -> pl.Expr:
    return (first.str.slice(0, 1) + last).str.to_lowercase().str.replace_all(r"[^a-z]", "")


def _game(f: Path) -> list[dict]:
    nfl_fourth_down._try_load = lambda name: None  # the on-demand nfl4th boosters are never fetched
    gid = f.name.split(".")[0]
    proc = NFLPlayProcess(gameId=int(gid))
    with gzip.open(f, "rt", encoding="utf-8") as fh:
        proc.espn_nfl_pbp(summary=json.load(fh))
    try:
        box = proc.run_processing_pipeline()["advBoxScore"]["pass"]
    except KeyError:  # 401616889 (the 2023 Pro Bowl Games) has no passing box
        return []
    return [{"game_id": gid, "passer_player_name": r["passer_player_name"], **{k: r[k] for k in INPUTS}} for r in box]


def main(raw_dir: str) -> None:
    with Pool(6) as pool:
        rows = [r for game in pool.map(_game, sorted(Path(raw_dir).glob("*.json.gz"))) for r in game]
    inputs = pl.DataFrame(rows).with_columns(
        k=pl.col("passer_player_name").str.to_lowercase().str.replace_all(r"[^a-z]", "")
    )
    espn = load_nfl_espn_qbr([2023], summary_type="week").with_columns(
        k=_key(pl.col("name_first"), pl.col("name_last"))
    )
    out = (
        espn.join(inputs, on=["game_id", "k"], how="inner")
        .select("game_id", "season_type", "player_id", pl.col("name_display").alias("passer"), *INPUTS, "qbr_total")
        .sort("game_id", "player_id")
    )
    print(f"ESPN rows {espn.height}, joined {out.height}")
    out.write_parquet(OUT, compression="zstd")


if __name__ == "__main__":
    main(sys.argv[1])
