"""Tests for regressed shooter true-talent."""

import polars as pl

from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shooter_talent


def _rim_la():
    return pl.DataFrame(
        {
            "shot_zone_basic": ["Restricted Area"],
            "shot_zone_area": ["Center(C)"],
            "shot_zone_range": ["Less Than 8 ft."],
            "fga": [1000],
            "fgm": [640],
            "fg_pct": [0.64],
        }
    )


def test_talent_shrinks_toward_zero():
    # one player, 100 rim shots (base 0.64), makes 80 → raw_above = (80-64)/100 = 0.16
    shots = pl.DataFrame(
        {
            "player_id": [201939] * 100,
            "shot_type": ["2PT Field Goal"] * 100,
            "shot_zone_basic": ["Restricted Area"] * 100,
            "shot_zone_area": ["Center(C)"] * 100,
            "shot_zone_range": ["Less Than 8 ft."] * 100,
            "shot_made_flag": [1] * 80 + [0] * 20,
        }
    )
    scored = score_shot_xpoints(shots, _rim_la())
    tal = shooter_talent(scored, league_id="00", min_attempts=50).row(0, named=True)
    assert abs(tal["raw_above_pct"] - 0.16) < 1e-9
    # k=100 → talent = 0.16 * 100/200 = 0.08; shrunk toward 0, same sign, |talent| < |raw|
    assert abs(tal["talent_pct"] - 0.08) < 1e-9
    assert 0 < tal["talent_pct"] < tal["raw_above_pct"]
    # points above expected: 80*2 - 100*(0.64*2) = 160 - 128 = 32
    assert abs(tal["points_above_expected"] - 32.0) < 1e-9
    assert tal["n_att"] == 100 and tal["actual_makes"] == 80


def test_talent_min_attempts_filters():
    shots = pl.DataFrame(
        {
            "player_id": [1] * 10,
            "shot_type": ["2PT Field Goal"] * 10,
            "shot_zone_basic": ["Restricted Area"] * 10,
            "shot_zone_area": ["Center(C)"] * 10,
            "shot_zone_range": ["Less Than 8 ft."] * 10,
            "shot_made_flag": [1] * 10,
        }
    )
    scored = score_shot_xpoints(shots, _rim_la())
    assert shooter_talent(scored, min_attempts=50).height == 0


def test_talent_empty_schema():
    out = shooter_talent(pl.DataFrame())
    assert out.height == 0
    assert out.columns == [
        "player_id",
        "n_att",
        "actual_makes",
        "exp_makes",
        "points_above_expected",
        "raw_above_pct",
        "talent_pct",
    ]
