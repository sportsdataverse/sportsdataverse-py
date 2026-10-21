"""First test reaching `sportsdataverse.wbb.wbb_season_producers` (supp-013).

`helper_wbb_standings` on the raw 2026 standings payload (trimmed to two conferences) must reproduce the
R producer's released `espn_womens_college_basketball_standings` 2026 rows for those conferences.
Fixtures and provenance: `tests/fixtures/season_producers/README.md`. Offline.

Values match row for row; the dtypes do not: the release carries `group_id` as Int32 (the module
docstring promises "Int32 ids" mirroring the R parquets) and the port emits String (EX-1.5-c -> task 5.1).
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wbb.wbb_season_producers import helper_wbb_standings

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "season_producers"


@pytest.fixture(scope="module")
def pair() -> tuple[pl.DataFrame, pl.DataFrame]:
    with gzip.open(FIX / "wbb_standings_2026_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        mine = helper_wbb_standings(json.load(fh), season=2026)
    return mine, pl.read_parquet(FIX / "wbb_standings_2026_release.parquet")


def test_helper_wbb_standings_reproduces_the_r_release_values_2026(pair):
    mine, release = pair
    assert mine.columns == release.columns and mine.height == release.height == 1848
    as_text = [pl.col(c).cast(pl.Utf8) for c in mine.columns]
    a = mine.select(as_text).sort(mine.columns, nulls_last=True)
    b = release.select(as_text).sort(mine.columns, nulls_last=True)
    assert a.equals(b)
    print("WBB_STANDINGS_VALUES_OK", mine.height)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="EX-1.5-c -> task 5.1: helper_wbb_standings emits group_id String; the R release carries Int32",
)
def test_helper_wbb_standings_matches_the_r_release_dtypes(pair):
    mine, release = pair
    assert mine.schema == release.schema, {
        c: (mine.schema[c], release.schema[c]) for c in mine.columns if mine.schema[c] != release.schema[c]
    }
