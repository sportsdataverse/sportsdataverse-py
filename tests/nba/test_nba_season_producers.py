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

from sportsdataverse.nba.nba_season_producers import helper_nba_standings

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
