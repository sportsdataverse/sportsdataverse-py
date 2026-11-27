"""``load_mlb_leverage_index`` offline, on a real slice of the published asset.

``tests/fixtures/mlb_game_state/leverage_index_2024_inning9.parquet`` is every ``inning_capped == 9``
row (466 buckets: the 9th and all extra innings) of the ``mlb_game_state`` release asset
``mlb_leverage_index_2024.parquet`` as rebuilt 2026-11-26. It stands in for the release download.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
import yaml

import sportsdataverse.mlb.mlb_loaders as loaders

_ROOT = Path(__file__).resolve().parents[2]
FIX = _ROOT / "tests" / "fixtures" / "mlb_game_state" / "leverage_index_2024_inning9.parquet"


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    def read(url: str) -> pl.DataFrame | None:
        if url.endswith("mlb_game_state/mlb_leverage_index_2024.parquet"):
            return pl.read_parquet(FIX)
        return None  # any other season: not published

    monkeypatch.setattr(loaders, "_read_release_parquet", read)


def test_the_loader_returns_the_declared_schema() -> None:
    df = loaders.load_mlb_leverage_index(2024)
    declared = yaml.safe_load((_ROOT / "tools/codegen/schemas/loader_schemas.yaml").read_text(encoding="utf-8"))
    assert [(c["name"], c["type"]) for c in declared["load_mlb_leverage_index"]] == [
        (name, str(dtype)) for name, dtype in df.schema.items()
    ]
    assert df.height == 466 and df["season"].unique().to_list() == [2024]


def test_extra_innings_open_with_the_runner_on_second() -> None:
    df = loaders.load_mlb_leverage_index(2024)
    tied_open = df.filter((pl.col("outs_start") == 0) & (pl.col("score_diff_bucket") == 0))
    # a tied 9th-or-later half with a runner on 2nd and nobody out is a common, high-leverage state
    on_second = tied_open.filter(pl.col("base_state") == "_2_")
    assert on_second.height == 2  # top and bottom
    assert (on_second["n"] >= 100).all() and (on_second["thin"] == False).all()  # noqa: E712
    assert (on_second["leverage_index"] > 1.0).all()


def test_floor_and_unpublished_seasons() -> None:
    with pytest.raises(loaders.SeasonNotFoundError):
        loaders.load_mlb_leverage_index(2019)
    with pytest.warns(UserWarning, match=r"no data for season\(s\) \[2030\]"):
        empty = loaders.load_mlb_leverage_index(2030)
    assert empty.height == 0
    assert empty.schema == loaders.load_mlb_leverage_index(2024).schema
