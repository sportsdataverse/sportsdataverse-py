"""Tests for the ③ expected home runs + park-factor adjustment (T6.2, Phase 3)."""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.mlb.mlb_expected_home_runs import (
    _add_hr_bins,
    build_hr_grid,
    mlb_expected_home_runs,
    park_adjust,
    predict_hr_prob,
)


def _bb() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "type": ["X", "X", "X"],
            "events": ["home_run", "home_run", "field_out"],
            "launch_speed": [102.0, 102.5, 101.0],
            "launch_angle": [27.0, 27.5, 26.5],
            "hc_x": [180.0, 178.0, 130.0],
            "hc_y": [80.0, 82.0, 100.0],
            "stand": ["R", "R", "L"],
        }
    )


def test_add_hr_bins_reads_events_and_distinguishes_pull_vs_oppo() -> None:
    out = _add_hr_bins(_bb())
    assert out["_is_hr"].to_list() == [1, 1, 0]
    # pull-side (R, hc_x large -> positive spray) differs from the oppo-ish third row
    assert out["spray_bin"][0] != out["spray_bin"][2]


def test_build_hr_grid_cell_probability() -> None:
    grid = build_hr_grid(_add_hr_bins(_bb()))
    assert grid.height >= 1
    assert (grid["p_hr"] <= 1.0).all()
    assert (grid["p_hr"] >= 0.0).all()


def test_predict_hr_prob_dense_and_sparse_fallback() -> None:
    bb = _add_hr_bins(_bb())
    grid = build_hr_grid(bb)
    pred = predict_hr_prob(bb, grid)
    assert pred.null_count() == 0
    assert pred.len() == bb.height


def test_park_adjust_hitter_park_boosts_probability() -> None:
    bb = _add_hr_bins(_bb()).with_columns(pl.Series("p_hr", [0.3, 0.3, 0.1]))
    bb = bb.with_columns(home_team=pl.Series(["COL", "SD"] + ["COL"] * (bb.height - 2)))
    park_factors = pl.DataFrame({"team_id": [115, 135], "hr_factor": [120.0, 80.0]})
    out = park_adjust(bb, park_factors)
    row0 = out.row(0, named=True)  # COL, hitter-friendly (120)
    row1 = out.row(1, named=True)  # SD, pitcher-friendly (80)
    assert row0["p_hr_park_adj"] > row0["p_hr"]
    assert row1["p_hr_park_adj"] < row1["p_hr"]


def test_mlb_expected_home_runs_schema_and_pandas() -> None:
    def _fake_puller(start_dt: str, end_dt: str, *, player_type: str = "batter") -> pl.DataFrame:
        return pl.DataFrame(
            {
                "batter": [1, 1, 1],
                "game_date": ["2024-06-01", "2024-06-02", "2024-06-03"],
                "type": ["X", "X", "X"],
                "events": ["home_run", "field_out", "home_run"],
                "launch_speed": [103.0, 90.0, 104.0],
                "launch_angle": [28.0, 15.0, 29.0],
                "hc_x": [180.0, 130.0, 182.0],
                "hc_y": [80.0, 100.0, 78.0],
                "stand": ["R", "R", "R"],
                "home_team": ["COL", "COL", "COL"],
            }
        )

    park_factors = pl.DataFrame({"team_id": [115], "hr_factor": [120.0]})
    out = mlb_expected_home_runs("2024-06-01", "2024-06-21", puller=_fake_puller, park_factors=park_factors)
    assert out.columns == ["batter", "season", "hr", "xhr_neutral", "xhr_park_adj", "hr_above_expected"]
    assert out.schema["batter"] == pl.Int64
    row = out.row(0, named=True)
    assert row["hr"] == 2
    assert row["hr_above_expected"] == row["hr"] - row["xhr_neutral"]

    pdf = mlb_expected_home_runs(
        "2024-06-01", "2024-06-21", puller=_fake_puller, park_factors=park_factors, return_as_pandas=True
    )
    assert isinstance(pdf, pd.DataFrame)


def test_mlb_expected_home_runs_empty_pull_returns_documented_schema() -> None:
    def _empty_puller(start_dt: str, end_dt: str, *, player_type: str = "batter") -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "batter": pl.Int64,
                "game_date": pl.Utf8,
                "type": pl.Utf8,
                "events": pl.Utf8,
                "launch_speed": pl.Float64,
                "launch_angle": pl.Float64,
                "hc_x": pl.Float64,
                "hc_y": pl.Float64,
                "stand": pl.Utf8,
                "home_team": pl.Utf8,
            }
        )

    out = mlb_expected_home_runs("2024-06-01", "2024-06-21", puller=_empty_puller)
    assert out.height == 0
    assert out.columns == ["batter", "season", "hr", "xhr_neutral", "xhr_park_adj", "hr_above_expected"]
