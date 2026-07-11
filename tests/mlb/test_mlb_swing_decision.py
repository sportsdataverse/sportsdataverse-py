"""Tests for the ② swing/take run-value + SEAGER analog (T6.2, Phase 2)."""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mlb.mlb_swing_decision import (
    _add_decision,
    mlb_swing_decision,
    swing_take_surfaces,
)


def _pitches() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "zone": [5, 5, 5, 5],
            "balls": [1, 1, 1, 1],
            "strikes": [1, 1, 1, 1],
            "description": ["foul", "hit_into_play", "called_strike", "ball"],
            "delta_run_exp": [0.06, 0.04, -0.015, -0.025],
        }
    )


def test_add_decision_labels_swing_and_take() -> None:
    out = _add_decision(_pitches())
    assert out["decision"].to_list() == ["swing", "swing", "take", "take"]
    assert out["count"].to_list() == ["1-1", "1-1", "1-1", "1-1"]


def test_swing_take_surfaces_cell_means() -> None:
    surf = swing_take_surfaces(_add_decision(_pitches()))
    assert surf.height == 2
    rows = {(r["zone"], r["count"], r["decision"]): r for r in surf.iter_rows(named=True)}
    swing_row = rows[(5, "1-1", "swing")]
    take_row = rows[(5, "1-1", "take")]
    assert swing_row["n"] == 2
    assert abs(swing_row["rv"] - 0.05) < 1e-9
    assert take_row["n"] == 2
    assert abs(take_row["rv"] - (-0.02)) < 1e-9


def test_mlb_swing_decision_schema_and_selectivity_direction() -> None:
    def _fake_puller(start_dt: str, end_dt: str, *, player_type: str = "batter") -> pl.DataFrame:
        # batter 1 always swings at the high-RV pitch (zone 5), always takes the low-RV pitch (zone 13)
        # batter 2 does the opposite (swings the bad pitch, takes the good one)
        rows = []
        for _ in range(10):
            rows.append(
                {
                    "batter": 1,
                    "game_date": "2024-06-01",
                    "zone": 5,
                    "balls": 1,
                    "strikes": 1,
                    "description": "hit_into_play",
                    "delta_run_exp": 0.05,
                }
            )
            rows.append(
                {
                    "batter": 1,
                    "game_date": "2024-06-01",
                    "zone": 13,
                    "balls": 1,
                    "strikes": 1,
                    "description": "ball",
                    "delta_run_exp": -0.05,
                }
            )
            rows.append(
                {
                    "batter": 2,
                    "game_date": "2024-06-01",
                    "zone": 5,
                    "balls": 1,
                    "strikes": 1,
                    "description": "called_strike",
                    "delta_run_exp": -0.06,
                }
            )
            rows.append(
                {
                    "batter": 2,
                    "game_date": "2024-06-01",
                    "zone": 13,
                    "balls": 1,
                    "strikes": 1,
                    "description": "foul",
                    "delta_run_exp": -0.08,
                }
            )
            # league fill for both zone/count cells so the neutral rate isn't degenerate
            rows.append(
                {
                    "batter": 3,
                    "game_date": "2024-06-01",
                    "zone": 5,
                    "balls": 1,
                    "strikes": 1,
                    "description": "called_strike",
                    "delta_run_exp": -0.06,
                }
            )
            rows.append(
                {
                    "batter": 3,
                    "game_date": "2024-06-01",
                    "zone": 13,
                    "balls": 1,
                    "strikes": 1,
                    "description": "ball",
                    "delta_run_exp": -0.05,
                }
            )
        return pl.DataFrame(rows)

    out = mlb_swing_decision("2024-06-01", "2024-06-21", puller=_fake_puller)
    assert out.columns == ["batter", "season", "pitches", "swing_take_runs", "selective_agg", "chase_rate", "n_swings"]
    assert out.schema["batter"] == pl.Int64

    by_batter = {r["batter"]: r for r in out.iter_rows(named=True)}
    assert by_batter[1]["selective_agg"] > 0  # swings at good pitches, takes bad ones
    assert by_batter[2]["selective_agg"] < 0  # opposite

    pdf = mlb_swing_decision("2024-06-01", "2024-06-21", puller=_fake_puller, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)


def test_mlb_swing_decision_empty_pull_returns_documented_schema() -> None:
    def _empty_puller(start_dt: str, end_dt: str, *, player_type: str = "batter") -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "batter": pl.Int64,
                "game_date": pl.Utf8,
                "zone": pl.Int64,
                "balls": pl.Int64,
                "strikes": pl.Int64,
                "description": pl.Utf8,
                "delta_run_exp": pl.Float64,
            }
        )

    out = mlb_swing_decision("2024-06-01", "2024-06-21", puller=_empty_puller)
    assert out.height == 0
    assert out.columns == ["batter", "season", "pitches", "swing_take_runs", "selective_agg", "chase_rate", "n_swings"]
