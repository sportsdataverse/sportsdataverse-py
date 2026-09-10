"""``load_nfl_ngs`` -- the SDV-native Next Gen Stats loader (nfl_ngs_* releases).

Offline tests pin the contract without the network: URL construction per
dataset (tag + stem + season), the per-dataset season floor, the unified
diagonal concat, and the pandas round-trip. One live smoke per dataset runs
under ``SDV_PY_LIVE_TESTS=1``. Column-name assertions are deliberately
minimal -- the producer's schemas may add columns.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.nfl import load_nfl_ngs, update_config
from sportsdataverse.nfl import nfl_loaders as _mod
from sportsdataverse.nfl.nfl_loaders import _NFL_NGS_DATASETS
from tests.conftest import skip_if_no_live

SDV = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"


@pytest.fixture(autouse=True)
def _no_cache():
    update_config(cache_mode="off")
    yield
    update_config(cache_mode="memory")


def test_registry_covers_the_twelve_published_tags():
    assert len(_NFL_NGS_DATASETS) == 12
    assert {t for t, _, _ in _NFL_NGS_DATASETS.values()} == {f"nfl_ngs_{k}" for k in _NFL_NGS_DATASETS}
    # the one stem that is NOT derivable from the tag
    assert _NFL_NGS_DATASETS["schedules"][1] == "ngs_schedule"


def test_urls_per_dataset_and_diagonal_concat(monkeypatch):
    seen: list[str] = []

    def fake_fetch(url: str) -> pl.DataFrame:
        seen.append(url)
        season = int(url.rsplit("_", 1)[1].split(".")[0])
        # second season adds a column: the concat must be diagonal, not strict
        extra = {"new_col": [1.0]} if season == 2024 else {}
        return pl.DataFrame({"season": [season], "team_id": ["0810"], **extra})

    monkeypatch.setattr(_mod, "_fetch_release_parquet", fake_fetch)
    df = load_nfl_ngs(seasons=[2023, 2024], dataset="leaders")
    assert seen == [
        SDV + "nfl_ngs_leaders/ngs_leaders_2023.parquet",
        SDV + "nfl_ngs_leaders/ngs_leaders_2024.parquet",
    ]
    assert df.height == 2 and "new_col" in df.columns and df.schema["team_id"] == pl.Utf8

    seen.clear()
    load_nfl_ngs(seasons=2024, dataset="schedules")  # int season accepted
    assert seen == [SDV + "nfl_ngs_schedules/ngs_schedule_2024.parquet"]


def test_pandas_round_trip(monkeypatch):
    monkeypatch.setattr(_mod, "_fetch_release_parquet", lambda url: pl.DataFrame({"season": [2024]}))
    out = load_nfl_ngs(seasons=[2024], dataset="teams", return_as_pandas=True)
    assert out.__class__.__module__.startswith("pandas") and len(out) == 1


def test_bad_dataset_raises():
    with pytest.raises(ValueError, match="dataset must be one of"):
        load_nfl_ngs(seasons=[2024], dataset="nope")


@pytest.mark.parametrize(
    "dataset,too_early",
    [("passing", 2015), ("leaders", 2015), ("teams", 2012), ("gamecenter_rushers", 2014), ("schedules", 2008)],
)
def test_per_dataset_season_floor(monkeypatch, dataset, too_early):
    calls = []
    monkeypatch.setattr(_mod, "_fetch_release_parquet", lambda url: calls.append(url))
    with pytest.raises(SeasonNotFoundError):
        load_nfl_ngs(seasons=[too_early], dataset=dataset)
    assert calls == []  # rejected before any fetch


@skip_if_no_live
@pytest.mark.parametrize("dataset", sorted(_NFL_NGS_DATASETS))
def test_live_2024(dataset):
    df = load_nfl_ngs(seasons=[2024], dataset=dataset)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert "season" in df.columns
