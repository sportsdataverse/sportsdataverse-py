"""Phase 1 -- nba_tracking_reb_oe (rebounding-over-expected)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_tracking_value import nba_tracking_reb_oe

_REB_SCHEMA = [
    "season",
    "player_id",
    "player_name",
    "team_id",
    "position_bucket",
    "gp",
    "min",
    "reb",
    "reb_chances",
    "reb_baseline_rate",
    "reb_expected",
    "reb_oe",
    "reb_oe_per_36",
    "oreb_oe",
    "dreb_oe",
    "league_id",
]


def _fake_reb_payload():
    # contested rate = (6+2)/(10+10)=0.4 ; uncontested = (4+8)/(5+5)=1.2 (capped conceptually)
    headers = [
        "PLAYER_ID",
        "PLAYER_NAME",
        "TEAM_ID",
        "GP",
        "MIN",
        "REB",
        "REB_CHANCES",
        "REB_CONTEST",
        "REB_CONTEST_PCT",
        "REB_UNCONTEST",
        "OREB",
        "OREB_CHANCES",
        "DREB",
        "DREB_CHANCES",
    ]
    rows = [
        [201939, "A", 1610612744, 50, 1800.0, 10.0, 15.0, 6.0, 0.6, 4.0, 3.0, 5.0, 7.0, 10.0],
        [2544, "B", 1610612747, 50, 1800.0, 10.0, 15.0, 2.0, 0.2, 8.0, 4.0, 5.0, 6.0, 10.0],
    ]
    return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}


def test_reb_oe_schema_and_math():
    out = nba_tracking_reb_oe(2024, by_position=False, _get_fn=lambda **kw: _fake_reb_payload())
    assert out.columns == _REB_SCHEMA
    assert out.schema["player_id"] == pl.Utf8
    r = {row["player_id"]: row for row in out.iter_rows(named=True)}
    # equal total reb (10 each) on equal reb_chances (15 each) -> reb_baseline_rate == league mean == 20/30
    assert abs(r["201939"]["reb_baseline_rate"] - (20.0 / 30.0)) < 1e-9
    assert abs(r["201939"]["reb_oe"]) < 1e-9
    assert abs(r["2544"]["reb_oe"]) < 1e-9
    # OREB/DREB splits computed too
    assert r["201939"]["oreb_oe"] is not None
    assert r["201939"]["dreb_oe"] is not None


def test_reb_oe_pandas_output():
    out = nba_tracking_reb_oe(2024, by_position=False, return_as_pandas=True, _get_fn=lambda **kw: _fake_reb_payload())
    import pandas as pd

    assert isinstance(out, pd.DataFrame)


def test_reb_oe_empty_is_zero_row_schema():
    out = nba_tracking_reb_oe(2024, _get_fn=lambda **kw: {"resultSets": []})
    assert out.height == 0 and out.columns == _REB_SCHEMA
