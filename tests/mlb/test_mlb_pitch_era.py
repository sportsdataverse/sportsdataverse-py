"""Tests for xERA/SIERA-like ERA estimators (model ③)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.mlb.mlb_pitch_era import mlb_pitch_era, siera_like, x_era
from sportsdataverse.mlb.mlb_pitching_constants import get_baselines


def _pitcher_with_mean_xwoba(x_woba: float, n: int = 20) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pitcher": [1] * n,
            "estimated_woba_using_speedangle": [x_woba] * n,
        }
    )


def test_x_era_identity_at_league_woba():
    b = get_baselines(2024)
    df = _pitcher_with_mean_xwoba(b.league_woba)
    out = x_era(df, 2024).row(0, named=True)
    assert abs(out["x_era"] - b.league_era) < 1e-6


def test_x_era_higher_xwoba_gives_higher_era():
    b = get_baselines(2024)
    lo = x_era(_pitcher_with_mean_xwoba(b.league_woba - 0.05), 2024).row(0, named=True)
    hi = x_era(_pitcher_with_mean_xwoba(b.league_woba + 0.05), 2024).row(0, named=True)
    assert hi["x_era"] > lo["x_era"]


def test_x_era_empty_input():
    out = x_era(pl.DataFrame(), 2024)
    assert out.height == 0 and "x_era" in out.columns


def _pitcher_events(events: list, bb_types: "list | None" = None) -> pl.DataFrame:
    n = len(events)
    row = {"pitcher": [1] * n, "events": events}
    if bb_types is not None:
        row["bb_type"] = bb_types
    return pl.DataFrame(row)


def test_siera_like_higher_k_pct_lowers_siera():
    # 10 PAs: pitcher A strikes out 8/10, pitcher B strikes out 2/10 (rest field_out, no walks).
    events_a = ["strikeout"] * 8 + ["field_out"] * 2
    events_b = ["strikeout"] * 2 + ["field_out"] * 8
    a = siera_like(pl.DataFrame({"pitcher": [1] * 10, "events": events_a}), 2024).row(0, named=True)
    b = siera_like(pl.DataFrame({"pitcher": [2] * 10, "events": events_b}), 2024).row(0, named=True)
    assert a["siera_like"] < b["siera_like"]


def test_siera_like_empty_input():
    out = siera_like(pl.DataFrame(), 2024)
    assert out.height == 0 and "siera_like" in out.columns


def test_mlb_pitch_era_joins_both():
    events = ["strikeout"] * 5 + ["field_out"] * 5
    df = pl.DataFrame(
        {
            "pitcher": [1] * 10,
            "events": events,
            "estimated_woba_using_speedangle": [0.31] * 10,
        }
    )
    out = mlb_pitch_era(df, 2024)
    row = out.row(0, named=True)
    assert "x_era" in out.columns and "siera_like" in out.columns
    assert row["pitcher"] == 1


def test_mlb_pitch_era_empty_input():
    out = mlb_pitch_era(pl.DataFrame(), 2024)
    assert out.height == 0
    assert "x_era" in out.columns and "siera_like" in out.columns
