"""Tests for the Connelly-style returning-production core (T2.2 Phase 2)."""

from __future__ import annotations

import importlib

import polars as pl

from sportsdataverse.cfb.cfb_returning_production import _returning_from_frames

# The package re-exports the FUNCTION under the module's name, so attribute
# import would hand back the callable; resolve the module explicitly.
rp = importlib.import_module("sportsdataverse.cfb.cfb_returning_production")


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


def test_roster_keys_use_espn_team_id_without_name_crosswalk(monkeypatch) -> None:
    """espn_cfb_rosters (#399) has team_id but no `team`; the name join must be skipped."""
    espn_roster = pl.DataFrame(
        {
            "season": [2025, 2025, 2025],
            "team_id": [333, 333, None],
            "athlete_id": [1, 2, 3],
            "team_name": ["Crimson Tide", "Crimson Tide", "Crimson Tide"],
        }
    )
    monkeypatch.setattr(rp, "load_cfb_rosters", lambda season: espn_roster)

    def _no_info(season):  # pragma: no cover - the assertion is that it is never reached
        raise AssertionError("team_info crosswalk must not run for an ESPN roster")

    monkeypatch.setattr(rp, "load_cfb_team_info", _no_info)
    out = rp._roster_keys(2025)
    assert out.columns == ["season", "team_id", "player_id"]
    assert out["team_id"].to_list() == ["333", "333"]  # null team_id row dropped
    assert out["player_id"].to_list() == ["1", "2"]
    assert out.schema["team_id"] == pl.Utf8 and out.schema["player_id"] == pl.Utf8
