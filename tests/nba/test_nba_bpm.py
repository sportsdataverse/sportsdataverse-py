"""Tests for BPM2_COEFFICIENTS constant and position/role estimators."""

from __future__ import annotations

import polars as pl
from sportsdataverse.nba.nba_bpm import (
    BPM2_COEFFICIENTS,
    _estimate_position,
    _estimate_role,
    _interp,
    _raw_bpm,
    _recursive_team_center,
)


def test_position_regression_team_sums_to_three_and_clamps() -> None:
    # 5 players, one team, arbitrary team-stat shares summing to 100% each
    shares = pl.DataFrame(
        {
            "player_id": [1, 2, 3, 4, 5],
            "team_id": [10] * 5,
            "min": [200.0] * 5,
            "pct_trb": [0.10, 0.15, 0.20, 0.25, 0.30],
            "pct_stl": [0.2] * 5,
            "pct_pf": [0.2] * 5,
            "pct_ast": [0.4, 0.3, 0.15, 0.1, 0.05],
            "pct_blk": [0.2] * 5,
        }
    )
    listed = pl.DataFrame({"player_id": [1, 2, 3, 4, 5], "position_num": [1.0, 2.0, 3.0, 4.0, 5.0]})
    pos = _estimate_position(shares, listed)
    # minute-weighted team mean position == 3.0 (the recursive constraint)
    m = (
        pos.join(shares.select(["player_id", "min"]), on="player_id")
        .select((pl.col("position_num") * pl.col("min")).sum() / pl.col("min").sum())
        .item()
    )
    assert abs(m - 3.0) < 1e-6
    assert pos["position_num"].min() >= 1.0 and pos["position_num"].max() <= 5.0


def test_role_regression_clamps_1_5() -> None:
    shares = pl.DataFrame(
        {
            "player_id": [1, 2],
            "team_id": [10, 10],
            "min": [200.0, 200.0],
            "pct_ast": [0.05, 0.40],
            "pct_threshold_pts": [0.30, 0.02],
        }
    )
    role = _estimate_role(shares)
    assert role["role_num"].min() >= 1.0 and role["role_num"].max() <= 5.0


def test_coefficients_constant_has_lebron_anchor_values() -> None:
    base = BPM2_COEFFICIENTS["base"]
    assert base["pts"] == (0.860, 0.860)  # (pos1, pos5)
    assert base["ast"] == (0.580, 1.034)
    assert base["fga_role"] == (-0.560, -0.780)  # (role1 creator, role5 receiver)
    off = BPM2_COEFFICIENTS["offense"]
    assert off["pts"] == (0.605, 0.605)
    assert off["blk"] == (0.725, 0.097)


def test_recursive_team_center_converges_with_clamping() -> None:
    # raw positions span outside [1,5] so clamping engages, but a mean-3 solution exists
    df = pl.DataFrame(
        {
            "player_id": [1, 2, 3, 4, 5],
            "team_id": [7] * 5,
            "min": [240.0] * 5,
            "raw": [0.2, 1.0, 3.0, 5.0, 6.5],  # 0.2 and 6.5 will clamp
        }
    )
    out = _recursive_team_center(df, "raw", "position_num", target=3.0)
    m = (
        out.join(df.select(["player_id", "min"]), on="player_id")
        .select((pl.col("position_num") * pl.col("min")).sum() / pl.col("min").sum())
        .item()
    )
    assert abs(m - 3.0) < 1e-6
    assert out["position_num"].min() >= 1.0 and out["position_num"].max() <= 5.0


def test_interp_endpoints_and_midpoint() -> None:
    # at scale=1 should return lo; at scale=5 should return hi; at scale=3 midpoint
    assert _interp((0.860, 0.860), 1.0) == 0.860
    assert _interp((0.860, 0.860), 5.0) == 0.860
    assert abs(_interp((0.580, 1.034), 1.0) - 0.580) < 1e-9
    assert abs(_interp((0.580, 1.034), 5.0) - 1.034) < 1e-9
    assert abs(_interp((0.580, 1.034), 3.0) - (0.580 + 1.034) / 2) < 1e-9


def test_raw_bpm_reproduces_bref_lebron_2017() -> None:
    # B-Ref worked example, per-100 (pts already shooting-context-adjusted 34.9 -> 30.4)
    feats = pl.DataFrame(
        {
            "player_id": [23],
            "pts": [30.4],
            "fg3m": [2.2],
            "ast": [11.5],
            "tov": [5.4],
            "orb": [1.7],
            "drb": [9.7],
            "stl": [1.6],
            "blk": [0.8],
            "pf": [2.4],
            "fga": [24.0],
            "fta": [9.5],
        }
    )
    positions = pl.DataFrame({"player_id": [23], "position_num": [2.30]})
    roles = pl.DataFrame({"player_id": [23], "role_num": [1.0]})
    out = _raw_bpm(feats, positions, roles)
    assert out.schema["raw_bpm"] == pl.Float64
    assert out.schema["raw_obpm"] == pl.Float64
    assert abs(out["raw_bpm"][0] - 18.7) < 0.3  # published raw total 18.7


def test_raw_bpm_missing_position_defaults_neutral_not_dropped():
    feats = pl.DataFrame(
        {
            "player_id": [1, 2],
            "pts": [20.0, 15.0],
            "fg3m": [1.0, 1.0],
            "ast": [4.0, 3.0],
            "tov": [2.0, 2.0],
            "orb": [1.0, 1.0],
            "drb": [4.0, 4.0],
            "stl": [1.0, 1.0],
            "blk": [0.5, 0.5],
            "pf": [2.0, 2.0],
            "fga": [12.0, 10.0],
            "fta": [3.0, 2.0],
        }
    )
    # player 2 is missing from positions AND roles -> must still appear (neutral 3.0), not dropped
    positions = pl.DataFrame({"player_id": [1], "position_num": [2.0]})
    roles = pl.DataFrame({"player_id": [1], "role_num": [1.0]})
    out = _raw_bpm(feats, positions, roles)
    assert set(out["player_id"].to_list()) == {1, 2}  # player 2 NOT dropped
    assert out.filter(pl.col("player_id") == 2)["raw_bpm"].item() is not None
