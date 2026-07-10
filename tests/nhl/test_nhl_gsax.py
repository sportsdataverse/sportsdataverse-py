"""Offline tests for goalie GSAx (goals saved above expected)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nhl.nhl_gsax import _aggregate_gsax, _attribute_goalie, nhl_goalie_gsax

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"


def _synthetic_scored() -> pl.DataFrame:
    # 4 already-xG-scored unblocked shots by the away team against home goalie 111 --
    # 3 saves (SHOT/MISSED_SHOT) + 1 GOAL, known xg values (sum = 1.0).
    return pl.DataFrame(
        {
            "game_id": [1, 1, 1, 1],
            "event_id": [1, 2, 3, 4],
            "event_type": ["SHOT", "SHOT", "MISSED_SHOT", "GOAL"],
            "event_team_abbr": ["AWY", "AWY", "AWY", "AWY"],
            "home_abbr": ["HOM", "HOM", "HOM", "HOM"],
            "away_abbr": ["AWY", "AWY", "AWY", "AWY"],
            "home_goalie_id": [111, 111, 111, 111],
            "home_goalie": ["Home Goalie", "Home Goalie", "Home Goalie", "Home Goalie"],
            "away_goalie_id": [222, 222, 222, 222],
            "away_goalie": ["Away Goalie", "Away Goalie", "Away Goalie", "Away Goalie"],
            "game_seconds": [10, 20, 30, 40],
            "xg": [0.1, 0.2, 0.3, 0.4],
        }
    ).with_columns(pl.col("game_id").cast(pl.Int64), pl.col("event_id").cast(pl.Int64))


def test_attribute_goalie_assigns_defending_team_goalie():
    scored = _synthetic_scored()
    out = _attribute_goalie(scored)
    assert out.schema["defending_goalie_id"] == pl.Int64
    # The away team shoots -> the home goalie (111) is defending on every row.
    assert out["defending_goalie_id"].to_list() == [111, 111, 111, 111]


def test_aggregate_gsax_exact_on_synthetic_shots():
    scored = _synthetic_scored()
    out = _aggregate_gsax(scored)
    row = out.filter(pl.col("player_id") == 111)
    assert row.height == 1
    assert row["shots"][0] == 4
    assert abs(row["xga"][0] - 1.0) < 1e-9
    assert row["ga"][0] == 1
    assert abs(row["gsax"][0] - 0.0) < 1e-9  # sum(xg)=1.0, ga=1 -> gsax=0


def test_nhl_goalie_gsax_empty_input_returns_documented_schema():
    empty = pl.DataFrame()
    out = nhl_goalie_gsax(empty, pl.DataFrame())
    assert out.height == 0
    assert set(out.columns) == {"player_id", "goalie", "shots", "xga", "ga", "gsax", "gsax_per_60"}


def test_nhl_goalie_gsax_on_real_fixture_runs_and_telescopes_correctly():
    # sum(gsax) == sum(xga) - sum(ga) is an algebraic identity of the per-goalie
    # aggregation itself (not a calibration claim -- Sigma(xg) approx Sigma(goals) only
    # holds at large sample; see the dedicated oracle gate in
    # test_nhl_player_impact_oracle.py for that empirical, tolerance-bounded check).
    pbp = pl.read_parquet(FIX / "pbp_sample.parquet")
    out = nhl_goalie_gsax(pbp, pl.DataFrame(), model_dir=MODELS)
    assert out.height > 0
    assert out.schema["player_id"] == pl.Int64
    assert abs(out["gsax"].sum() - (out["xga"].sum() - out["ga"].sum())) < 1e-6
