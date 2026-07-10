from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value


def _frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "player_id": ["A", "B", "C", "D"],
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
    assert out.row(0, named=True)["player_id"] == "A"
    assert out.row(0, named=True)["skating_value_rank"] == 1
    pwhl = nhl_edge_skating_value(season=2024, league="pwhl")
    assert pwhl.height == 0 and "skating_value" in pwhl.columns


def test_edge_value_monotone_with_components() -> None:
    out = nhl_edge_skating_value(season=2024, detail_frames=_frame())
    ordered = out.sort("skating_value", descending=True)["player_id"].to_list()
    assert ordered == ["A", "B", "C", "D"]  # A dominates every component


def test_edge_pwhl_zero_row_full_schema() -> None:
    pwhl = nhl_edge_skating_value(season=2024, league="pwhl")
    for col in ("player_id", "season", "top_speed", "skating_value", "skating_value_rank"):
        assert col in pwhl.columns
