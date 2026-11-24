"""Tests for ``load_nfl_model_pbp`` -- the named loader for the SDV-native
``nfl_model_pbp`` release (28 assets, 1999-2026, verified live 2026-09-17).
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nfl import nfl_loaders
from sportsdataverse.nfl.cache import clear_cache
from tests.conftest import skip_if_no_live


@pytest.fixture(autouse=True)
def _no_cache():
    # load_nfl_pbp is @cached_loader; a stale entry would mask the URL assertion.
    clear_cache()
    yield
    clear_cache()


@pytest.fixture
def urls(monkeypatch):
    seen: list[str] = []

    def fake(url):
        seen.append(url)
        return pl.DataFrame({"game_id": ["2024_01_x"], "epa": [0.1]})

    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", fake)
    return seen


def test_reads_the_sdv_model_pbp_asset_not_the_nflverse_one(urls):
    nfl_loaders.load_nfl_model_pbp([2024])
    assert urls == [
        "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_model_pbp/model_pbp_2024.parquet"
    ]


def test_matches_load_nfl_pbp_source_sdv(urls):
    nfl_loaders.load_nfl_model_pbp([2023, 2024])
    from_alias = list(urls)
    urls.clear()
    nfl_loaders.load_nfl_pbp([2023, 2024], source="sdv")
    assert from_alias == urls


def test_season_below_1999_raises(urls):
    with pytest.raises(Exception, match="1999"):
        nfl_loaders.load_nfl_model_pbp([1998])


def test_return_as_pandas_round_trips(urls):
    out = nfl_loaders.load_nfl_model_pbp([2024], return_as_pandas=True)
    assert not isinstance(out, pl.DataFrame)
    assert list(out.columns) == ["game_id", "epa"]


def test_exported_from_the_nfl_package():
    import sportsdataverse.nfl as nfl

    assert nfl.load_nfl_model_pbp is nfl_loaders.load_nfl_model_pbp


@skip_if_no_live
def test_live_model_pbp_reads_the_release():
    df = nfl_loaders.load_nfl_model_pbp([2024])
    # 335 columns as of 2026-11-24. The SCHEMA_REV 11 rebuild (nfl-data #95, 2026-11-13) added
    # nflfastR clean_pbp's passer/rusher/receiver names and ids after series_success, plus nflfastR's
    # success and first_down (enrich_nfl_pbp, sdv-py #1037). Before it: 327 (SCHEMA_REV 10, sdv-py
    # #1028: the provider `drive` number, nflverse "ARI 10" drive_start/end_yard_line text). Assert
    # the floor and the named columns, not an exact width, so an appended column is not a failure.
    assert df.width >= 327, df.width
    assert {"passer", "passer_id", "rusher", "rusher_id", "receiver", "receiver_id"} <= set(df.columns)
    assert {"success", "first_down"} <= set(df.columns)
    assert df.height > 40_000, df.height
    assert df.schema["game_id"] == pl.String
    assert df.schema["drive"] == pl.Int64
    assert df.schema["drive_start_yard_line"] == pl.String
    assert df.schema["drive_end_yard_line"] == pl.String
    assert {"ep", "epa", "wp", "vegas_wp", "cp", "cpoe"} <= set(df.columns)


@skip_if_no_live
def test_live_first_season_is_1999():
    df = nfl_loaders.load_nfl_model_pbp([1999])
    assert df.height > 0
    assert df["season"].unique().to_list() == [1999]
