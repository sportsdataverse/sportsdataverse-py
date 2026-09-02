"""Tests for ``load_nfl_model_pbp`` -- the named loader for the SDV-native
``nfl_model_pbp`` release (27 assets, 1999-2025, verified live 2026-09-02).
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
    # 257 columns in every published season, checked against all 27 assets on
    # 2026-09-02 via a parquet-footer scan.
    assert df.width == 257, df.width
    assert df.height > 40_000, df.height
    assert df.schema["game_id"] == pl.String
    assert {"ep", "epa", "wp", "vegas_wp", "cp", "cpoe"} <= set(df.columns)


@skip_if_no_live
def test_live_first_season_is_1999():
    df = nfl_loaders.load_nfl_model_pbp([1999])
    assert df.height > 0
    assert df["season"].unique().to_list() == [1999]
