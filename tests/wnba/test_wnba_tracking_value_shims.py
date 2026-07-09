"""Phase 7 -- WNBA tracking-value shims (league_id="10" by-reference)."""

from __future__ import annotations

from sportsdataverse.wnba.wnba_tracking_value import (
    wnba_tracking_drive_value,
    wnba_tracking_pass_value,
    wnba_tracking_reb_oe,
    wnba_tracking_rim_protect_value,
    wnba_tracking_shot_diet_value,
    wnba_tracking_touch_value,
)


def _fake_reb_payload():
    headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GP", "MIN", "REB", "REB_CHANCES"]
    rows = [[1628886, "A", 1611661319, 30, 900.0, 200.0, 300.0]]
    return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}


def test_wnba_reb_oe_delegates_and_defaults_league_id():
    calls = []

    def fake(**kw):
        calls.append(kw)
        return _fake_reb_payload()

    out = wnba_tracking_reb_oe(2024, by_position=False, _get_fn=fake)
    assert out.height == 1
    assert out["league_id"].to_list() == ["10"]
    assert calls[0]["league_id"] == "10"


def test_wnba_pass_value_delegates():
    def fake(**kw):
        headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GP", "MIN", "PASSES_MADE", "AST", "AST_PTS_CREATED"]
        rows = [[1628886, "A", 1611661319, 30, 900.0, 200.0, 40.0, 90.0]]
        return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}

    out = wnba_tracking_pass_value(2024, by_position=False, _get_fn=fake)
    assert out["league_id"].to_list() == ["10"]


def test_wnba_drive_value_delegates():
    def fake(**kw):
        headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GP", "MIN", "DRIVES", "DRIVE_PTS", "DRIVE_FTA"]
        rows = [[1628886, "A", 1611661319, 30, 900.0, 100.0, 80.0, 10.0]]
        return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}

    out = wnba_tracking_drive_value(2024, by_position=False, _get_fn=fake)
    assert out["league_id"].to_list() == ["10"]


def test_wnba_shot_diet_value_delegates():
    def fake(**kw):
        if kw.get("pt_measure_type") == "CatchShoot":
            headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "CATCH_SHOOT_FGA", "CATCH_SHOOT_PTS"]
        else:
            headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "PULL_UP_FGA", "PULL_UP_PTS"]
        rows = [[1628886, "A", 1611661319, 50.0, 60.0]]
        return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}

    out = wnba_tracking_shot_diet_value(2024, by_position=False, _get_fn=fake)
    assert out["league_id"].to_list() == ["10"]


def test_wnba_touch_value_delegates():
    def fake(**kw):
        headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GP", "MIN", "TOUCHES", "POINTS", "TIME_OF_POSS"]
        rows = [[1628886, "A", 1611661319, 30, 900.0, 500.0, 200.0, 100.0]]
        return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}

    out = wnba_tracking_touch_value(2024, by_position=False, _get_fn=fake)
    assert out["league_id"].to_list() == ["10"]


def test_wnba_rim_protect_value_delegates():
    def fake(**kw):
        headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GP", "MIN", "DEF_RIM_FGA", "DEF_RIM_FGM", "DEF_RIM_FG_PCT"]
        rows = [[1628886, "A", 1611661319, 30, 900.0, 100.0, 50.0, 0.5]]
        return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}

    out = wnba_tracking_rim_protect_value(2024, by_position=False, _get_fn=fake)
    assert out["league_id"].to_list() == ["10"]
