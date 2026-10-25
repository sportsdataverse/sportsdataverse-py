from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value


def _frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "player_id": ["8478402", "8477934", "8479318", "8476453"],  # NHL ids as the EDGE detail capture types them
            "season": [2024] * 4,
            "top_speed": [24.0, 23.0, 22.0, 21.0],
            "distance_km": [6.0, 5.5, 5.0, 4.5],
            "speed_bursts_20": [30.0, 20.0, 10.0, 5.0],
            "oz_time_pct": [0.6, 0.5, 0.4, 0.3],
            "dz_time_pct": [0.2] * 4,
            "nz_time_pct": [0.2] * 4,
        }
    )


def test_edge_value_ranks_and_pwhl_zero_row() -> None:
    out = nhl_edge_skating_value(season=2024, detail_frames=_frame()).sort("skating_value_rank")
    assert out.row(0, named=True)["player_id"] == 8478402
    assert out.row(0, named=True)["skating_value_rank"] == 1
    pwhl = nhl_edge_skating_value(season=2024, league="pwhl")
    assert pwhl.height == 0 and "skating_value" in pwhl.columns


def test_edge_value_monotone_with_components() -> None:
    out = nhl_edge_skating_value(season=2024, detail_frames=_frame())
    ordered = out.sort("skating_value", descending=True)["player_id"].to_list()
    assert ordered == [8478402, 8477934, 8479318, 8476453]  # the first dominates every component


def test_edge_pwhl_zero_row_full_schema() -> None:
    pwhl = nhl_edge_skating_value(season=2024, league="pwhl")
    for col in ("player_id", "season", "top_speed", "skating_value", "skating_value_rank"):
        assert col in pwhl.columns


def test_edge_value_player_id_is_int64() -> None:
    """B-GB-10: the value frame joins the EDGE frames it was built from (both Int64), empty or not."""
    sample = pl.read_parquet("tests/fixtures/nhl_microstat/edge_skater_detail_sample.parquet")
    assert sample.schema["player_id"] == pl.String  # the capture's own text ids
    out = nhl_edge_skating_value(season=2024, detail_frames=sample)
    assert out.schema["player_id"] == pl.Int64 and out.height == sample.height
    assert sorted(out["player_id"].to_list()) == sorted(int(p) for p in sample["player_id"])
    assert nhl_edge_skating_value(season=2024, league="pwhl").schema["player_id"] == pl.Int64
