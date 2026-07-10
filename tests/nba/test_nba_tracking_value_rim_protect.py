"""Phase 6 -- nba_tracking_rim_protect_value (rim-protection / shot-defend)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_tracking_value import nba_tracking_rim_protect_value

_RIM_SCHEMA = [
    "season",
    "player_id",
    "player_name",
    "team_id",
    "position_bucket",
    "gp",
    "min",
    "d_fga",
    "d_fgm",
    "d_fg_pct",
    "normal_fg_pct",
    "rim_protect_pts_saved",
    "rim_protect_pts_saved_per_36",
    "source",
    "league_id",
]


def _fake_defense_payload():
    headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GP", "MIN", "DEF_RIM_FGA", "DEF_RIM_FGM", "DEF_RIM_FG_PCT"]
    rows = [
        # A: holds shooters well below the bucket-mean rate -> saves points
        [203497, "A", 1610612762, 50, 1800.0, 400.0, 180.0, 0.45],
        # B: allows shooters to convert well above the bucket-mean rate
        [1626167, "B", 1610612754, 50, 1800.0, 400.0, 260.0, 0.65],
    ]
    return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}


def test_rim_protect_schema_and_math():
    out = nba_tracking_rim_protect_value(2024, by_position=False, _get_fn=lambda **kw: _fake_defense_payload())
    assert out.columns == _RIM_SCHEMA
    assert out.schema["player_id"] == pl.Utf8

    rows = {r["player_id"]: r for r in out.iter_rows(named=True)}
    # bucket-mean defended rate = (180+260)/(400+400) = 0.55
    assert abs(rows["203497"]["normal_fg_pct"] - 0.55) < 1e-9
    # A: (0.55 - 0.45) * 400 * 2 = 80 points saved
    assert abs(rows["203497"]["rim_protect_pts_saved"] - 80.0) < 1e-9
    # B allowed shooters ABOVE the bucket mean -> negative points saved
    assert rows["1626167"]["rim_protect_pts_saved"] < 0
    assert (out["source"] == "leaguedash").all()


def test_rim_protect_shotdefend_source_swaps_band():
    def fake_get(**kw):
        return _fake_defense_payload()

    def fake_defend(**kw):
        pid = kw["player_id"]
        headers = ["PLAYER_ID", "LESS_THAN6FT_FGA", "LESS_THAN6FT_FGM", "LESS_THAN6FT_FG_PCT"]
        # both players allow exactly bucket-average at the rim in this synthetic capture
        return {
            "resultSets": [{"name": "DefendingShots", "headers": headers, "rowSet": [[int(pid), 100.0, 50.0, 0.5]]}]
        }

    out = nba_tracking_rim_protect_value(
        2024, by_position=False, source="shotdefend", max_players=2, _get_fn=fake_get, _defend_get_fn=fake_defend
    )
    assert (out["source"] == "shotdefend").all()
    assert out["d_fga"].to_list() == [100.0, 100.0]


def test_rim_protect_empty_is_zero_row_schema():
    out = nba_tracking_rim_protect_value(2024, _get_fn=lambda **kw: {"resultSets": []})
    assert out.height == 0 and out.columns == _RIM_SCHEMA
