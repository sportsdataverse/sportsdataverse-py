"""Tests for shot-selection quality + per-player zone-value maps."""

import polars as pl

from sportsdataverse.nba.nba_shot_value import (
    score_shot_xpoints,
    shot_selection_quality,
    zone_value_map,
)


def _league_avgs():
    return pl.DataFrame(
        {
            "shot_zone_basic": ["Restricted Area", "Left Corner 3", "Right Corner 3", "Mid-Range"],
            "shot_zone_area": ["Center(C)", "Left Side(L)", "Right Side(R)", "Center(C)"],
            "shot_zone_range": ["Less Than 8 ft.", "24+ ft.", "24+ ft.", "16-24 ft."],
            "fga": [1000, 500, 500, 800],
            "fgm": [640, 195, 195, 320],
            "fg_pct": [0.640, 0.390, 0.390, 0.400],
        }
    )


def _make_shots(player_id, zone_basic, zone_area, zone_range, shot_type, n, made):
    return pl.DataFrame(
        {
            "player_id": [player_id] * n,
            "shot_type": [shot_type] * n,
            "shot_zone_basic": [zone_basic] * n,
            "shot_zone_area": [zone_area] * n,
            "shot_zone_range": [zone_range] * n,
            "shot_made_flag": [1] * made + [0] * (n - made),
        }
    )


def test_selection_quality_rim_beats_midrange():
    # player 1: 60 rim (xev 0.64*2=1.28); player 2: 60 mid (xev 0.40*2=0.80)
    rim = _make_shots(1, "Restricted Area", "Center(C)", "Less Than 8 ft.", "2PT Field Goal", 60, 40)
    mid = _make_shots(2, "Mid-Range", "Center(C)", "16-24 ft.", "2PT Field Goal", 60, 20)
    scored = score_shot_xpoints(pl.concat([rim, mid]), _league_avgs())
    sel = shot_selection_quality(scored, min_attempts=50)
    p1 = sel.filter(pl.col("player_id") == 1).row(0, named=True)
    p2 = sel.filter(pl.col("player_id") == 2).row(0, named=True)
    assert p1["selection_quality"] > 0 > p2["selection_quality"]
    # both reference the SAME league baseline
    assert abs(p1["league_xev_per_shot"] - p2["league_xev_per_shot"]) < 1e-12
    assert abs(p1["xev_per_shot"] - 1.28) < 1e-9


def test_zone_map_collapses_corners():
    # 3 rim + 2 corner-3 (one left, one right) for one player
    rim = _make_shots(1, "Restricted Area", "Center(C)", "Less Than 8 ft.", "2PT Field Goal", 3, 2)
    lc = _make_shots(1, "Left Corner 3", "Left Side(L)", "24+ ft.", "3PT Field Goal", 1, 1)
    rc = _make_shots(1, "Right Corner 3", "Right Side(R)", "24+ ft.", "3PT Field Goal", 1, 0)
    scored = score_shot_xpoints(pl.concat([rim, lc, rc]), _league_avgs())
    zmap = zone_value_map(scored)
    zones = set(zmap["zone"].to_list())
    assert "corner_3" in zones and "Left Corner 3" not in zones
    corner = zmap.filter(pl.col("zone") == "corner_3").row(0, named=True)
    assert corner["att"] == 2 and corner["makes"] == 1  # merged L+R
    # pps = (3 + 0)/2 = 1.5 (one made 3, one missed 3)
    assert abs(corner["pps"] - 1.5) < 1e-9
    rim_row = zmap.filter(pl.col("zone") == "rim").row(0, named=True)
    assert abs(rim_row["pps"] - (2 * 2) / 3) < 1e-9  # 2 makes * 2pts / 3 att


def test_selection_empty_and_zone_empty():
    assert shot_selection_quality(pl.DataFrame()).height == 0
    assert zone_value_map(pl.DataFrame()).height == 0
    assert "pps_above_expected" in zone_value_map(pl.DataFrame()).columns
