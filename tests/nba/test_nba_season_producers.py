"""First test reaching `sportsdataverse.nba.nba_season_producers` (supp-013).

`helper_nba_standings` on the raw 2024 standings payload must reproduce the R producer's released
`espn_nba_standings` 2024 asset row for row (690 rows, same columns and dtypes). Fixtures and
provenance: `tests/fixtures/season_producers/README.md`. Offline.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_season_producers import build_nba_player_identity_lookup, helper_nba_standings

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "season_producers"


def test_helper_nba_standings_reproduces_the_r_release_2024():
    with gzip.open(FIX / "nba_standings_2024.json.gz", "rt", encoding="utf-8") as fh:
        mine = helper_nba_standings(json.load(fh), season=2024)
    release = pl.read_parquet(FIX / "nba_standings_2024_release.parquet")
    assert mine.columns == release.columns
    assert mine.schema == release.schema
    key = mine.columns
    assert mine.sort(key, nulls_last=True).equals(release.sort(key, nulls_last=True))
    print("NBA_STANDINGS_PARITY_OK", mine.height)


def test_identity_lookup_keeps_the_team_of_the_latest_game():
    """player_box is written newest game first; a traded athlete must keep the team of his latest game,
    not the first row's. A null game_date sorts first and loses to any dated row; equal dates keep frame order."""
    box = pl.DataFrame(
        {
            "athlete_id": [7, 7, 7, 8, 8],
            "athlete_display_name": ["A"] * 3 + ["B"] * 2,
            "team_id": [2, 1, 3, 5, 6],
            "team_display_name": ["New", "Old", "Undated", "X", "Y"],
            "game_date": ["2024-03-01", "2023-11-01", None, "2024-01-05", "2024-01-05"],
        }
    ).with_columns(pl.col("game_date").str.to_date())
    lookup = build_nba_player_identity_lookup(box)
    assert lookup["7"]["team_id"] == 2 and lookup["7"]["team_display_name"] == "New"
    assert lookup["8"]["team_id"] == 6  # same date: the later row in frame order
    assert build_nba_player_identity_lookup(box.drop("game_date"))["7"]["team_id"] == 3  # frame order
