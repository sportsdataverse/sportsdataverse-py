from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_microstat_constants import get_constants
from sportsdataverse.nhl.nhl_penalty_value import extract_penalties, nhl_penalty_value


def _pen(committed: str | None, drawn: str | None, code: str = "MIN") -> dict:
    return {
        "game_id": "G1",
        "season": 2024,
        "event_idx": 0,
        "period": 1,
        "time_in_period": "5:00",
        "type_desc_key": "penalty",
        "event_owner_team_id": "10",
        "zone_code": "N",
        "x_coord": 0.0,
        "y_coord": 0.0,
        "situation_code": "1551",
        "home_team_id": "10",
        "home_team_defending_side": "left",
        "winning_player_id": None,
        "losing_player_id": None,
        "scoring_player_id": None,
        "assist1_player_id": None,
        "assist2_player_id": None,
        "shooting_player_id": None,
        "committed_player_id": committed,
        "drawn_player_id": drawn,
        "penalty_type_code": code,
        "shot_type": None,
    }


def test_extract_penalties_basic() -> None:
    pbp = pl.DataFrame([_pen("A", "B"), {**_pen(None, None), "type_desc_key": "shot-on-goal"}])
    out = extract_penalties(pbp)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["committed_player_id"] == "A"
    assert row["drawn_player_id"] == "B"
    assert row["is_minor"] is True
    assert row["is_major"] is False


def test_extract_penalties_empty() -> None:
    empty = extract_penalties(pl.DataFrame(schema={"type_desc_key": pl.Utf8}))
    assert empty.height == 0
    assert "is_minor" in empty.columns


def test_net_penalty_value_and_conservation() -> None:
    pbp = pl.DataFrame([_pen("A", "B"), _pen("A", "B"), _pen("B", "A")])
    out = nhl_penalty_value(pbp)
    a = out.filter(pl.col("player_id") == "A").row(0, named=True)
    assert a["penalties_taken"] == 2 and a["penalties_drawn"] == 1
    ppv = get_constants("nhl").pp_goal_value
    assert abs(a["net_penalty_value"] - (1 - 2) * ppv) < 1e-9
    # conservation: league-wide net drawn == net taken -> sum(net_penalty_value) == 0
    assert abs(out["net_penalty_value"].sum()) < 1e-9


def test_major_penalty_uses_major_value() -> None:
    pbp = pl.DataFrame([_pen("A", "B", code="MAJ")])
    out = nhl_penalty_value(pbp)
    b = out.filter(pl.col("player_id") == "B").row(0, named=True)
    assert b["majors_drawn"] == 1
    major_val = get_constants("nhl").major_penalty_value
    assert abs(b["net_penalty_value"] - major_val) < 1e-9


def test_empty_pbp_returns_schema() -> None:
    empty = nhl_penalty_value(pl.DataFrame(schema={"type_desc_key": pl.Utf8}))
    assert empty.height == 0
    assert "net_penalty_value" in empty.columns
