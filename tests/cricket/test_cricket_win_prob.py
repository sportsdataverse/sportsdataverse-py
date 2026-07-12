"""Unit tests for cricket match-state extraction + in-play win probability (T7.3)."""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.cricket.cricket_win_prob import (
    STATE_SCHEMA,
    _parse_score_string,
    cricket_match_state,
    cricket_win_probability,
)


# --- Task 2.1: score-string parsing -------------------------------------------
def test_parse_score_string() -> None:
    assert _parse_score_string("161/5 (18/20 ov, target 156)") == (161, 5, 108, 156)


def test_parse_score_string_partial_over() -> None:
    assert _parse_score_string("88/3 (12.4/20 ov)") == (88, 3, 76, None)


def test_parse_score_string_no_limit() -> None:
    assert _parse_score_string("168/7 (20 ov)") == (168, 7, 120, None)


def test_parse_score_string_bad() -> None:
    assert _parse_score_string("no score yet") is None
    assert _parse_score_string(None) is None  # type: ignore[arg-type]


# --- Task 2.1: match-state extraction -----------------------------------------
def _fake_summary() -> dict:
    return {
        "header": {
            "id": "12345",
            "competitions": [
                {
                    "id": "12345",
                    "competitors": [
                        {"homeAway": "home", "team": {"id": "1"}, "score": "168/7 (20 ov)"},
                        {"homeAway": "away", "team": {"id": "2"}, "score": "169/5 (18.2/20 ov, target 169)"},
                    ],
                }
            ],
        }
    }


def test_cricket_match_state_shape() -> None:
    st = cricket_match_state(_fake_summary(), fmt="t20")
    assert st.schema == dict(STATE_SCHEMA)
    assert st.height == 2
    first = st.filter(pl.col("innings_number") == 1).to_dicts()[0]
    assert (first["runs"], first["wickets"], first["balls_bowled"], first["target"]) == (168, 7, 120, None)
    assert first["balls_total"] == 120 and first["batting_team_id"] == "1" and first["event_id"] == "12345"
    second = st.filter(pl.col("innings_number") == 2).to_dicts()[0]
    assert (second["runs"], second["wickets"], second["balls_bowled"], second["target"]) == (169, 5, 110, 169)


def test_cricket_match_state_empty() -> None:
    st = cricket_match_state({}, fmt="odi")
    assert st.height == 0
    assert st.schema == dict(STATE_SCHEMA)


def test_cricket_match_state_test_fmt_raises() -> None:
    with pytest.raises(ValueError, match="Test cricket deferred"):
        cricket_match_state(_fake_summary(), fmt="test")


# --- Task 2.3: win probability ------------------------------------------------
def _chase_state(runs: int) -> dict:
    return {
        "event_id": "M1",
        "innings_number": 2,
        "batting_team_id": "A",
        "wickets": 3,
        "balls_bowled": 60,
        "balls_total": 120,
        "target": 160,
        "fmt": "t20",
        "runs": runs,
    }


def test_win_prob_empty_carries_schema() -> None:
    empty = pl.DataFrame(schema=STATE_SCHEMA)
    out = cricket_win_probability(empty)
    assert out.height == 0
    for col in ("overs_left", "wickets_left", "resources_left", "proj_final", "win_prob_raw", "win_prob"):
        assert col in out.columns


def test_win_prob_columns_and_bounds() -> None:
    st = pl.DataFrame([_chase_state(r) for r in (40, 80, 120)])
    out = cricket_win_probability(st)
    assert out.schema["win_prob"] == pl.Float64
    assert out.schema["proj_final"] == pl.Float64
    wp = out["win_prob"].to_numpy()
    assert ((wp >= 0.0) & (wp <= 1.0)).all()


def test_win_prob_monotone_in_runs() -> None:
    st = pl.DataFrame([_chase_state(r) for r in (40, 80, 120)])
    wp = cricket_win_probability(st)["win_prob"].to_list()
    assert wp[0] < wp[1] < wp[2]


def test_win_prob_big_late_lead_near_one() -> None:
    # runs already past the target with few balls left -> near-certain win.
    st = pl.DataFrame(
        [{**_chase_state(165), "balls_bowled": 114, "wickets": 2}]  # 161 target already passed, 1 over left
    )
    assert cricket_win_probability(st)["win_prob"].item() > 0.9


def test_win_prob_first_innings_uses_setting_benchmark() -> None:
    # first innings (no target) resolves via the setting phase without error.
    st = pl.DataFrame(
        [
            {
                "event_id": "M2",
                "innings_number": 1,
                "batting_team_id": "B",
                "runs": 90,
                "wickets": 2,
                "balls_bowled": 60,
                "balls_total": 120,
                "target": None,
                "fmt": "t20",
            }
        ]
    )
    out = cricket_win_probability(st)
    assert out.height == 1
    assert 0.0 <= out["win_prob"].item() <= 1.0


def test_win_prob_odi_format() -> None:
    st = pl.DataFrame(
        [
            {
                "event_id": "O1",
                "innings_number": 2,
                "batting_team_id": "C",
                "runs": 200,
                "wickets": 4,
                "balls_bowled": 210,
                "balls_total": 300,
                "target": 260,
                "fmt": "odi",
            }
        ]
    )
    assert 0.0 <= cricket_win_probability(st)["win_prob"].item() <= 1.0
