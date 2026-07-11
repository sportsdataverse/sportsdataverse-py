"""As-of-date leakage tests for the pitcher injury-risk index (model ⑦).

Every trailing feature for appearance G must be computed using ONLY
appearances with ``game_date < G``. A future appearance injected after G must
never change G's own row.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

from sportsdataverse.mlb.mlb_pitch_injury import pitcher_appearance_trends


def _appearances(velos_by_date):
    rows = []
    for i, (d, v) in enumerate(velos_by_date):
        rows.append(
            {
                "pitcher": 1,
                "game_pk": 1000 + i,
                "game_date": d,
                "pitch_type": "FF",
                "release_speed": v,
                "release_spin_rate": 2300.0,
                "pfx_x": 0.5,
                "pfx_z": 1.4,
                "sz_top": 3.5,
                "sz_bot": 1.5,
                "plate_x": 0.0,
                "plate_z": 2.5,
                "delta_run_exp": 0.0,
                "balls": 0,
                "strikes": 0,
                "at_bat_number": 1,
                "pitch_number": 1,
                "inning": 1,
            }
        )
    return pl.DataFrame(rows)


def test_trailing_features_ignore_future_games():
    base = [(dt.date(2024, 5, 1), 96.0), (dt.date(2024, 5, 8), 95.0)]
    two = pitcher_appearance_trends(_appearances(base), window=5)
    row2 = two.filter(pl.col("game_date") == dt.date(2024, 5, 8)).row(0, named=True)

    with_future = pitcher_appearance_trends(_appearances(base + [(dt.date(2024, 5, 15), 90.0)]), window=5)
    row2b = with_future.filter(pl.col("game_date") == dt.date(2024, 5, 8)).row(0, named=True)

    assert row2["velo_drop"] == row2b["velo_drop"]  # future game must not leak backward
    assert row2["velo_trend"] == row2b["velo_trend"]
    assert row2["trailing_workload"] == row2b["trailing_workload"]
    assert row2["days_rest"] == row2b["days_rest"]


def test_first_appearance_has_no_trailing_baseline():
    base = [(dt.date(2024, 5, 1), 96.0)]
    out = pitcher_appearance_trends(_appearances(base), window=5)
    row = out.row(0, named=True)
    assert row["velo_drop"] is None
    assert row["velo_trend"] is None
    assert row["days_rest"] is None


def test_injury_risk_as_of_date_excludes_current_and_later():
    from sportsdataverse.mlb.mlb_pitch_injury import mlb_injury_risk

    base = [
        (dt.date(2024, 5, 1), 96.0),
        (dt.date(2024, 5, 8), 95.0),
        (dt.date(2024, 5, 15), 94.0),
    ]
    df = _appearances(base)
    out = mlb_injury_risk(df, as_of_date=dt.date(2024, 5, 15))
    assert out["game_date"].max() < dt.date(2024, 5, 15)
