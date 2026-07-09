from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_faceoff_value import extract_faceoffs, nhl_faceoff_value


def _row(type_desc_key: str, **overrides: object) -> dict:
    base = {
        "game_id": "G1",
        "season": 2024,
        "event_idx": 0,
        "period": 1,
        "time_in_period": "10:00",
        "type_desc_key": type_desc_key,
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
        "committed_player_id": None,
        "drawn_player_id": None,
        "penalty_type_code": None,
        "shot_type": None,
    }
    base.update(overrides)
    return base


def _fo(winner: str, loser: str, zone: str, sit: str = "1551") -> dict:
    return _row(
        "faceoff",
        zone_code=zone,
        situation_code=sit,
        winning_player_id=winner,
        losing_player_id=loser,
    )


def test_extract_faceoffs_basic() -> None:
    pbp = pl.DataFrame([_fo("A", "B", "O"), _row("shot-on-goal")])
    out = extract_faceoffs(pbp)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["winner_player_id"] == "A"
    assert row["loser_player_id"] == "B"
    assert row["strength_state"] == "even"


def test_extract_faceoffs_empty() -> None:
    empty = extract_faceoffs(pl.DataFrame(schema={"type_desc_key": pl.Utf8}))
    assert empty.height == 0
    assert "winner_player_id" in empty.columns


def test_faceoff_value_dominant_center() -> None:
    rows = [_fo("A", "B", "O") for _ in range(8)] + [_fo("B", "A", "O") for _ in range(2)]
    pbp = pl.DataFrame(rows)
    out = nhl_faceoff_value(pbp).sort("player_id")
    a = out.filter(pl.col("player_id") == "A").row(0, named=True)
    assert a["faceoffs_taken"] == 10 and a["faceoffs_won"] == 8
    assert abs(a["fo_win_pct"] - 0.8) < 1e-9
    assert a["fo_win_pct_above_exp"] > 0  # beats the 50% context baseline
    assert a["faceoff_value"] > 0


def test_empty_pbp_returns_schema() -> None:
    empty = nhl_faceoff_value(pl.DataFrame(schema={"type_desc_key": pl.Utf8}))
    assert empty.height == 0
    assert "faceoff_value" in empty.columns
