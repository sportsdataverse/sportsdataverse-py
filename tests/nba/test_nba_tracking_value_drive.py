"""Phase 3 -- nba_tracking_drive_value (drive value & rim-pressure)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_tracking_value import nba_tracking_drive_value

_DRIVE_SCHEMA = [
    "season",
    "player_id",
    "player_name",
    "team_id",
    "position_bucket",
    "gp",
    "min",
    "drives",
    "drive_pts",
    "drive_baseline_rate",
    "drive_expected",
    "drive_pts_oe",
    "drive_pts_oe_per_36",
    "drive_fta",
    "rim_pressure",
    "drive_ast",
    "drive_tov",
    "league_id",
]


def _fake_drives_payload():
    headers = [
        "PLAYER_ID",
        "PLAYER_NAME",
        "TEAM_ID",
        "GP",
        "MIN",
        "DRIVES",
        "DRIVE_PTS",
        "DRIVE_FTA",
        "DRIVE_AST",
        "DRIVE_TOV",
    ]
    rows = [
        # A: high FTA/drive rate (draws fouls), B: low FTA/drive rate
        [1628983, "A", 1610612760, 50, 1800.0, 200.0, 160.0, 80.0, 20.0, 10.0],
        [1628973, "B", 1610612752, 50, 1800.0, 200.0, 120.0, 5.0, 30.0, 15.0],
    ]
    return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}


def test_drive_value_schema_and_math():
    out = nba_tracking_drive_value(2024, by_position=False, _get_fn=lambda **kw: _fake_drives_payload())
    assert out.columns == _DRIVE_SCHEMA
    assert out.schema["player_id"] == pl.Utf8

    rows = {r["player_id"]: r for r in out.iter_rows(named=True)}
    # baseline rate = (160+120)/(200+200) = 0.7 ; expected = 200*0.7 = 140
    assert abs(rows["1628983"]["drive_baseline_rate"] - 0.7) < 1e-9
    assert abs(rows["1628983"]["drive_pts_oe"] - 20.0) < 1e-9
    assert abs(rows["1628973"]["drive_pts_oe"] - (-20.0)) < 1e-9
    # high FTA/drive player gets a positive rim_pressure z-score
    assert rows["1628983"]["rim_pressure"] > 0
    assert rows["1628973"]["rim_pressure"] < 0


def test_drive_value_empty_is_zero_row_schema():
    out = nba_tracking_drive_value(2024, _get_fn=lambda **kw: {"resultSets": []})
    assert out.height == 0 and out.columns == _DRIVE_SCHEMA
