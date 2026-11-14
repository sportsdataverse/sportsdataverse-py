"""The derived basketball release loaders, offline: metric curves, rolling windows and the ESPN NBA depth charts.

Real release rows (``tests/fixtures/basketball_release_ids/README.md``) stand in for the assets. The metric-curve
and rolling-window ids are published as String; the loaders cast them to Int64, the stats ``person_id`` /
``team_id`` dtype (``id_int64`` in ``releases.yaml``).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.nba.nba_loaders as nba
import sportsdataverse.wnba.wnba_loaders as wnba

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "basketball_release_ids"
ASSETS = {
    "nba_stats_metric_curves/metric_curves_2026.parquet": "nba_stats_metric_curves_2026",
    "wnba_stats_rolling_windows/rolling_windows_2025.parquet": "wnba_stats_rolling_windows_2025",
    "espn_nba_depthcharts/depthcharts_2027.parquet": "nba_depthcharts_2027",
}


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    def read(url: str) -> pl.DataFrame:
        return pl.read_parquet(FIX / f"{ASSETS['/'.join(url.split('/')[-2:])]}.parquet")

    monkeypatch.setattr(nba, "_read_release_parquet", read)
    monkeypatch.setattr(wnba, "_read_release_parquet", read)


def test_the_fixtures_carry_the_published_string_ids() -> None:
    for name in ("nba_stats_metric_curves_2026", "wnba_stats_rolling_windows_2025"):
        schema = pl.read_parquet_schema(FIX / f"{name}.parquet")
        assert schema["entity_id"] == schema["team_id"] == pl.String, name


def test_nba_metric_curves_take_the_start_year_and_cast_the_ids() -> None:
    df = nba.load_nba_stats_metric_curves(2025)  # 2025-26, the asset keyed 2026
    assert df["season"].unique().to_list() == [2026]
    assert df.schema["entity_id"] == df.schema["team_id"] == pl.Int64
    assert set(df["entity_type"]) == {"league", "team", "player"}
    assert df.filter(pl.col("entity_type") == "league")["entity_id"].null_count() == 3
    assert df.filter(pl.col("entity_type") == "team")["entity_id"].min() > 1610612700  # NBA Stats team ids


def test_wnba_rolling_windows_cast_the_ids() -> None:
    df = wnba.load_wnba_stats_rolling_windows(2025)
    assert df.height == 4 and df.schema["entity_id"] == df.schema["team_id"] == pl.Int64
    assert df["entity_id"].null_count() == 0


def test_nba_depth_charts_floor_and_shape() -> None:
    df = nba.load_nba_depth_charts(2027)
    assert df.height == 37 and df["as_of_date"].n_unique() == 1
    assert set(df["position_abbreviation"]) == {"PG", "SG", "SF", "PF", "C"}
    with pytest.raises(nba.SeasonNotFoundError):
        nba.load_nba_depth_charts(2026)
