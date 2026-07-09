"""Phase 4 -- nba_tracking_shot_diet_value (catch-&-shoot vs pull-up)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_tracking_value import nba_tracking_shot_diet_value

_SHOT_DIET_SCHEMA = [
    "season",
    "player_id",
    "player_name",
    "team_id",
    "position_bucket",
    "cs_fga",
    "cs_pts",
    "cs_pts_oe",
    "pu_fga",
    "pu_pts",
    "pu_pts_oe",
    "shot_diet_delta",
    "league_id",
]


def _fake_cs_payload():
    headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "CATCH_SHOOT_FGA", "CATCH_SHOOT_PTS"]
    rows = [
        [201939, "A", 1610612744, 100.0, 130.0],  # C&S specialist: high pts/fga
        [1629029, "B", 1610612742, 100.0, 90.0],
    ]
    return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}


def _fake_pu_payload():
    headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "PULL_UP_FGA", "PULL_UP_PTS"]
    rows = [
        [201939, "A", 1610612744, 100.0, 90.0],  # weaker off-the-dribble
        [1629029, "B", 1610612742, 100.0, 110.0],  # pull-up specialist
    ]
    return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}


def _fake_get(**kw):
    if kw.get("pt_measure_type") == "CatchShoot":
        return _fake_cs_payload()
    return _fake_pu_payload()


def test_shot_diet_schema_and_math():
    out = nba_tracking_shot_diet_value(2024, by_position=False, _get_fn=_fake_get)
    assert out.columns == _SHOT_DIET_SCHEMA
    assert out.schema["player_id"] == pl.Utf8

    rows = {r["player_id"]: r for r in out.iter_rows(named=True)}
    # cs baseline rate = (130+90)/200 = 1.1 ; A's cs_pts_oe = 130 - 100*1.1 = 20
    assert abs(rows["201939"]["cs_pts_oe"] - 20.0) < 1e-9
    # pu baseline rate = (90+110)/200 = 1.0 ; A's pu_pts_oe = 90 - 100*1.0 = -10
    assert abs(rows["201939"]["pu_pts_oe"] - (-10.0)) < 1e-9
    # A is a C&S specialist -> positive shot_diet_delta; B (pull-up specialist) -> negative
    assert rows["201939"]["shot_diet_delta"] > 0
    assert rows["1629029"]["shot_diet_delta"] < 0


def test_shot_diet_pandas_output():
    import pandas as pd

    out = nba_tracking_shot_diet_value(2024, by_position=False, return_as_pandas=True, _get_fn=_fake_get)
    assert isinstance(out, pd.DataFrame)


def test_shot_diet_empty_is_zero_row_schema():
    out = nba_tracking_shot_diet_value(2024, _get_fn=lambda **kw: {"resultSets": []})
    assert out.height == 0 and out.columns == _SHOT_DIET_SCHEMA
