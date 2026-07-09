"""Phase 5 -- nba_tracking_touch_value (touch / possession-time value)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_tracking_value import nba_tracking_touch_value

_TOUCH_SCHEMA = [
    "season",
    "player_id",
    "player_name",
    "team_id",
    "position_bucket",
    "gp",
    "min",
    "touches",
    "pts",
    "touch_baseline_rate",
    "touch_expected",
    "pts_per_touch_oe",
    "time_of_poss",
    "time_of_poss_eff",
    "league_id",
]


def _fake_possessions_payload():
    headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GP", "MIN", "TOUCHES", "POINTS", "TIME_OF_POSS"]
    rows = [
        [203999, "A", 1610612743, 50, 1800.0, 3000.0, 1200.0, 400.0],  # high pts/touch, high economy
        [2544, "B", 1610612747, 50, 1800.0, 3000.0, 600.0, 400.0],
    ]
    return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}


def test_touch_value_schema_and_math():
    out = nba_tracking_touch_value(2024, by_position=False, _get_fn=lambda **kw: _fake_possessions_payload())
    assert out.columns == _TOUCH_SCHEMA
    assert out.schema["player_id"] == pl.Utf8

    rows = {r["player_id"]: r for r in out.iter_rows(named=True)}
    # baseline rate = (1200+600)/(3000+3000) = 0.3 ; A expected = 3000*0.3=900 ; oe=300
    assert abs(rows["203999"]["pts_per_touch_oe"] - 300.0) < 1e-9
    assert abs(rows["2544"]["pts_per_touch_oe"] - (-300.0)) < 1e-9
    # A scores way more per second of possession -> positive time_of_poss_eff
    assert rows["203999"]["time_of_poss_eff"] > 0
    assert rows["2544"]["time_of_poss_eff"] < 0


def test_touch_value_empty_is_zero_row_schema():
    out = nba_tracking_touch_value(2024, _get_fn=lambda **kw: {"resultSets": []})
    assert out.height == 0 and out.columns == _TOUCH_SCHEMA
