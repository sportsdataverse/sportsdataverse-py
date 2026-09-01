"""Tests for the Connelly-style returning-production core (T2.2 Phase 2)."""

from __future__ import annotations

import importlib

import pytest

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
    monkeypatch.setattr(rp, "load_cfb_rosters_cfbd", lambda season: pl.DataFrame())

    def _no_info(season):  # pragma: no cover - the assertion is that it is never reached
        raise AssertionError("team_info crosswalk must not run when only the ESPN roster is present")

    monkeypatch.setattr(rp, "load_cfb_team_info", _no_info)
    out = rp._roster_keys(2025)
    assert out.columns == ["season", "team_id", "player_id"]
    assert out["team_id"].to_list() == ["333", "333"]  # null team_id row dropped
    assert out["player_id"].to_list() == ["1", "2"]
    assert out.schema["team_id"] == pl.Utf8 and out.schema["player_id"] == pl.Utf8


def test_roster_keys_union_espn_and_cfbd_for_the_season_in_progress(monkeypatch) -> None:
    """Week 1: ESPN game rosters cover a handful of teams; the CFBD preseason roster fills the rest.

    A player listed by both sources appears once; the CFBD school name is resolved
    through team_info to the same ESPN team id the ESPN roster carries.
    """
    espn_roster = pl.DataFrame({"season": [2026, 2026], "team_id": [333, 333], "athlete_id": [1, 2]})
    cfbd_roster = pl.DataFrame(
        {
            "athlete_id": ["2", "3", "4", "5"],
            "team": ["Alabama", "Alabama", "Auburn", "Nowhere State"],
        }
    )
    info = pl.DataFrame({"team_id": [333, 2], "school": ["Alabama", "Auburn"], "alt_name1": [None, None]})
    monkeypatch.setattr(rp, "load_cfb_rosters", lambda season: espn_roster)
    monkeypatch.setattr(rp, "load_cfb_rosters_cfbd", lambda season: cfbd_roster)
    monkeypatch.setattr(rp, "load_cfb_team_info", lambda season: info)
    monkeypatch.setattr(rp, "_MIN_ROSTER_MATCH", 0.5)  # 3 of 4 CFBD rows resolve; "Nowhere State" does not
    out = rp._roster_keys(2026).sort("team_id", "player_id")
    assert out.rows() == [(2026, "2", "4"), (2026, "333", "1"), (2026, "333", "2"), (2026, "333", "3")]


def test_roster_keys_cfbd_match_floor_still_asserts(monkeypatch) -> None:
    monkeypatch.setattr(rp, "load_cfb_rosters", lambda season: pl.DataFrame())
    monkeypatch.setattr(
        rp,
        "load_cfb_rosters_cfbd",
        lambda season: pl.DataFrame({"athlete_id": ["1", "2"], "team": ["Nowhere", "Elsewhere"]}),
    )
    monkeypatch.setattr(
        rp, "load_cfb_team_info", lambda season: pl.DataFrame({"team_id": [1], "school": ["Somewhere"]})
    )
    with pytest.raises(ValueError, match="resolved to a team id"):
        rp._roster_keys(2026)
