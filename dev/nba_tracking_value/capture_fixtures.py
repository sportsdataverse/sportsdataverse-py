"""Scratch (not committed) capture script for the T3.2 tracking-value fixtures.

Run from a residential IP:
    SDV_PY_NBA_STATS_LIVE=1 uv run python dev/nba_tracking_value/capture_fixtures.py

Writes raw leaguedashptstats payloads for season 2023-24 / league_id="00" for
each of the six pt_measure_type values, plus a player_positions parquet built
from nba_player_positions (bucketed guard/wing/big).
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_player_positions import nba_player_positions
from sportsdataverse.nba.nba_stats import nba_stats_leaguedashptstats
from sportsdataverse.nba.nba_tracking_value import _position_num_to_bucket

OUT = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nba_stats" / "tracking"
SEASON = "2023-24"
LEAGUE_ID = "00"
MEASURES = ["Rebounding", "Possessions", "Drives", "CatchShoot", "PullUpShot", "Defense"]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for measure in MEASURES:
        raw = nba_stats_leaguedashptstats(
            pt_measure_type=measure,
            season=SEASON,
            league_id=LEAGUE_ID,
            per_mode_simple="Totals",
            player_or_team="Player",
            return_parsed=False,
        )
        rowset = raw.get("resultSets", [{}])[0].get("rowSet", []) if isinstance(raw, dict) else []
        print(f"{measure}: {len(rowset)} rows")
        path = OUT / f"leaguedashptstats_{measure.lower()}_2324.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        time.sleep(1.0)

    positions = nba_player_positions(SEASON, league_id=LEAGUE_ID)
    positions = positions.with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8),
        pl.col("position_num").map_elements(_position_num_to_bucket, return_dtype=pl.Utf8).alias("position_bucket"),
    ).select("player_id", "position_bucket")
    print(f"positions: {positions.height} rows")
    positions.write_parquet(OUT / "player_positions_2324.parquet")


if __name__ == "__main__":
    main()
