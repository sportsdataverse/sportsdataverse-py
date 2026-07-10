"""Phase 7 -- G-League (league_id="20") + sparse-tracking graceful degradation.

No new source -- exercises the empty-payload path and the by_position=True /
empty-positions-frame fallback to a single "all" bucket, never a raise.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_tracking_value import (
    nba_tracking_drive_value,
    nba_tracking_pass_value,
    nba_tracking_reb_oe,
    nba_tracking_rim_protect_value,
    nba_tracking_shot_diet_value,
    nba_tracking_touch_value,
)

_MODELS = [
    nba_tracking_reb_oe,
    nba_tracking_pass_value,
    nba_tracking_drive_value,
    nba_tracking_shot_diet_value,
    nba_tracking_touch_value,
    nba_tracking_rim_protect_value,
]


def test_gleague_empty_tracking_returns_zero_row_never_raises():
    for model in _MODELS:
        out = model(2024, league_id="20", _get_fn=lambda **kw: {"resultSets": []})
        assert out.height == 0
        assert out["league_id"].dtype == pl.Utf8


def test_by_position_with_empty_positions_frame_degrades_to_all():
    headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GP", "MIN", "REB", "REB_CHANCES"]
    rows = [[1628886, "A", 1611661319, 30, 900.0, 200.0, 300.0]]
    raw = {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}
    empty_positions = pl.DataFrame(schema={"player_id": pl.Utf8, "position_bucket": pl.Utf8})

    out = nba_tracking_reb_oe(2024, league_id="20", positions=empty_positions, _get_fn=lambda **kw: raw)
    assert out["position_bucket"].to_list() == ["all"]
