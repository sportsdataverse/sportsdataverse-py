"""Tests for the Connelly-style returning-production core (T2.2 Phase 2)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb.cfb_returning_production import _returning_from_frames


def test_half_offense_returns() -> None:
    prod_prev = pl.DataFrame(
        {  # season S-1 = 2022 production
            "season": [2022, 2022],
            "team_id": ["A", "A"],
            "player_id": ["p1", "p2"],
            "unit": ["offense", "offense"],
            "prod_weight": [10.0, 10.0],
            "position": ["WR", "WR"],
        }
    )
    roster_curr = pl.DataFrame(
        {  # season S = 2023 roster: only p1 returns
            "season": [2023],
            "team_id": ["A"],
            "player_id": ["p1"],
        }
    )
    out = _returning_from_frames(prod_prev, roster_curr, division="fbs")
    row = out.filter((pl.col("season") == 2023) & (pl.col("team_id") == "A")).row(0, named=True)
    assert abs(row["off_returning"] - 0.5) < 1e-9


def test_units_aggregate_independently_and_overall_averages() -> None:
    prod_prev = pl.DataFrame(
        {
            "season": [2022] * 4,
            "team_id": ["A"] * 4,
            "player_id": ["p1", "p2", "d1", "d2"],
            "unit": ["offense", "offense", "defense", "defense"],
            "prod_weight": [30.0, 10.0, 5.0, 15.0],
            "position": ["QB", "WR", "LB", "CB"],
        }
    )
    roster_curr = pl.DataFrame(
        {
            "season": [2023, 2023],
            "team_id": ["A", "A"],
            "player_id": ["p1", "d2"],  # QB (30/40 off) + CB (15/20 def) return
        }
    )
    out = _returning_from_frames(prod_prev, roster_curr, division="fbs")
    row = out.row(0, named=True)
    assert abs(row["off_returning"] - 0.75) < 1e-9
    assert abs(row["def_returning"] - 0.75) < 1e-9
    assert abs(row["overall_returning"] - 0.75) < 1e-9
    assert row["n_returning"] == 2


def test_empty_input_returns_documented_schema() -> None:
    empty_prod = pl.DataFrame(
        schema={
            "season": pl.Int64,
            "team_id": pl.Utf8,
            "player_id": pl.Utf8,
            "unit": pl.Utf8,
            "prod_weight": pl.Float64,
            "position": pl.Utf8,
        }
    )
    empty_roster = pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8, "player_id": pl.Utf8})
    out = _returning_from_frames(empty_prod, empty_roster, division="fbs")
    assert out.height == 0
    assert out.schema["off_returning"] == pl.Float64
    assert out.schema["n_returning"] == pl.Int64
