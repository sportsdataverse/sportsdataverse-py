"""Tests for the declarative rolling-feature layer (WS3)."""

from __future__ import annotations

import datetime

import polars as pl
import pytest

from sportsdataverse._common.feature_set import (
    FeatureSetSpec,
    as_of_features,
    feature_column_names,
    rolling_features,
    splits_grid,
)


def _games() -> pl.DataFrame:
    d = datetime.date
    return pl.DataFrame(
        {
            "player_id": [1, 1, 1, 1, 2, 2],
            "date": [
                d(2026, 1, 1),
                d(2026, 1, 3),
                d(2026, 1, 5),
                d(2026, 1, 7),
                d(2026, 1, 2),
                d(2026, 1, 4),
            ],
            "home_away": ["H", "A", "H", "A", "H", "H"],
            "pts": [10.0, 20.0, 30.0, 40.0, 5.0, 15.0],
        }
    )


def _spec(**overrides) -> FeatureSetSpec:
    base = dict(name="form", unit="player_id", aggfuncs={"pts": ("mean", "sum")}, spans=(2, 0))
    base.update(overrides)
    return FeatureSetSpec(**base)


def test_spec_validation() -> None:
    with pytest.raises(ValueError, match="unknown aggs"):
        FeatureSetSpec("s", "player_id", {"pts": ("median",)})
    with pytest.raises(ValueError, match="at least one column"):
        FeatureSetSpec("s", "player_id", {})
    with pytest.raises(ValueError, match="non-negative"):
        FeatureSetSpec("s", "player_id", {"pts": ("mean",)}, spans=(-1,))


def test_feature_column_names_deterministic() -> None:
    assert feature_column_names(_spec()) == [
        "pts_mean___2",
        "pts_sum___2",
        "pts_mean___0",
        "pts_sum___0",
    ]


def test_splits_grid_cross_product() -> None:
    spec = _spec(splits={"home_away": ("H", "A")})
    grid = splits_grid(spec, pl.Series([2, 1]))
    assert grid.height == 4
    assert grid.columns == ["player_id", "home_away"]
    assert grid["player_id"].to_list() == [1, 1, 2, 2]


def test_missing_columns_fail_fast() -> None:
    with pytest.raises(ValueError, match="missing columns"):
        rolling_features(_games().drop("pts"), _spec())


def test_trailing_window_and_full_history() -> None:
    out = rolling_features(_games(), _spec())
    p1 = out.filter(pl.col("player_id") == 1)
    # span=2: two most recent games (Jan 5, Jan 7) -> mean 35, sum 70
    assert p1["pts_mean___2"][0] == pytest.approx(35.0)
    assert p1["pts_sum___2"][0] == pytest.approx(70.0)
    # span=0: all history
    assert p1["pts_mean___0"][0] == pytest.approx(25.0)
    assert p1["pts_sum___0"][0] == pytest.approx(100.0)


def test_as_of_is_strictly_before() -> None:
    out = rolling_features(_games(), _spec(), as_of=datetime.date(2026, 1, 7))
    p1 = out.filter(pl.col("player_id") == 1)
    # Jan 7 game MUST be excluded (leak guard): window = Jan 3, Jan 5
    assert p1["pts_mean___2"][0] == pytest.approx(25.0)
    assert p1["pts_sum___0"][0] == pytest.approx(60.0)


def test_min_rows_nulls_thin_cells() -> None:
    out = rolling_features(_games(), _spec(min_rows=3, spans=(2,)))
    # every cell has >= 2 obs only; min_rows=3 nulls player 2 (2 games)
    p2 = out.filter(pl.col("player_id") == 2)
    assert p2["pts_mean___2"][0] is None
    p1 = out.filter(pl.col("player_id") == 1)
    assert p1["pts_mean___2"][0] is None  # window keeps 2 rows < 3


def test_splits_compute_separately_and_grid_is_complete() -> None:
    spec = _spec(splits={"home_away": ("H", "A")}, spans=(0,))
    out = rolling_features(_games(), spec)
    assert out.height == 4  # 2 players x H/A
    p1_home = out.filter((pl.col("player_id") == 1) & (pl.col("home_away") == "H"))
    assert p1_home["pts_mean___0"][0] == pytest.approx(20.0)  # 10, 30
    p1_away = out.filter((pl.col("player_id") == 1) & (pl.col("home_away") == "A"))
    assert p1_away["pts_mean___0"][0] == pytest.approx(30.0)  # 20, 40
    # player 2 never played away: grid cell exists, features null
    p2_away = out.filter((pl.col("player_id") == 2) & (pl.col("home_away") == "A"))
    assert p2_away.height == 1
    assert p2_away["pts_mean___0"][0] is None


def test_empty_input_keeps_schema() -> None:
    out = rolling_features(_games().head(0), _spec())
    assert out.height == 0
    assert set(feature_column_names(_spec())) <= set(out.columns)


def test_as_of_features_stacks_snapshots() -> None:
    cuts = [datetime.date(2026, 1, 4), datetime.date(2026, 1, 8)]
    out = as_of_features(_games(), _spec(spans=(0,)), cuts)
    assert out.columns[0] == "as_of"
    early = out.filter((pl.col("as_of") == cuts[0]) & (pl.col("player_id") == 1))
    late = out.filter((pl.col("as_of") == cuts[1]) & (pl.col("player_id") == 1))
    assert early["pts_sum___0"][0] == pytest.approx(30.0)  # Jan 1 + Jan 3
    assert late["pts_sum___0"][0] == pytest.approx(100.0)  # all four
