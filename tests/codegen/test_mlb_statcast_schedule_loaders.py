"""Offline contract tests for ``load_mlb_statcast`` / ``load_mlb_schedule``.

Tags ``mlb_statcast`` (2015+) and ``mlb_schedules`` (1988+) publish one parquet per
season. These pin the asset each season resolves to, the season floors, and the
float-to-Int64 cast of the statcast runner ids, with the download mocked.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.mlb import mlb_loaders

SDV = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"


@pytest.fixture
def seen(monkeypatch):
    urls: list[str] = []

    def fake(url):
        urls.append(url)
        season = int(url.rsplit("_", 1)[1].split(".")[0])
        # Runner ids ship as Float64 in the release, like the real parquet.
        return pl.DataFrame({"game_pk": [1], "season": [season], "on_1b": [665750.0], "on_2b": [None], "on_3b": [None]})

    monkeypatch.setattr(mlb_loaders, "_read_release_parquet", fake)
    return urls


def test_statcast_requests_one_asset_per_season_and_casts_runner_ids(seen):
    df = mlb_loaders.load_mlb_statcast(seasons=[2015, 2024])
    assert seen == [SDV + "mlb_statcast/mlb_statcast_2015.parquet", SDV + "mlb_statcast/mlb_statcast_2024.parquet"]
    assert df["season"].to_list() == [2015, 2024]
    assert all(df.schema[c] == pl.Int64 for c in ("on_1b", "on_2b", "on_3b"))
    assert df["on_1b"].to_list() == [665750, 665750]


def test_schedule_requests_the_schedules_tag(seen):
    df = mlb_loaders.load_mlb_schedule(seasons=1988)
    assert seen == [SDV + "mlb_schedules/mlb_schedule_1988.parquet"]
    assert df.height == 1


@pytest.mark.parametrize("fn,floor", [("load_mlb_statcast", 2015), ("load_mlb_schedule", 1988)])
def test_season_floor(seen, fn, floor):
    with pytest.raises(SeasonNotFoundError, match=str(floor)):
        getattr(mlb_loaders, fn)(seasons=floor - 1)
    assert seen == []
