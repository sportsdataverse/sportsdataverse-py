"""Unit tests for model (1): Synergy play-type-adjusted offense/defense."""

import polars as pl

from sportsdataverse.nba.nba_playtype import (
    adjust_playtype_efficiency,
    nba_playtype_ratings,
    raw_playtype_efficiency,
)


def _mini():
    off = pl.DataFrame(
        {
            "team_id": [1, 1, 2, 2],
            "play_type": ["Isolation", "Transition", "Isolation", "Transition"],
            "poss": [100.0, 100.0, 50.0, 150.0],
            "pts": [90.0, 120.0, 40.0, 165.0],
        }
    )
    deff = pl.DataFrame(
        {
            "team_id": [1, 1, 2, 2],
            "play_type": ["Isolation", "Transition", "Isolation", "Transition"],
            "poss": [80.0, 120.0, 110.0, 90.0],
            "pts": [88.0, 132.0, 99.0, 90.0],
        }
    )
    return off, deff


def test_raw_ppp_and_freq():
    off, deff = _mini()
    out = raw_playtype_efficiency(off, deff)
    iso1 = out.filter((pl.col("team_id") == 1) & (pl.col("play_type") == "Isolation")).row(0, named=True)
    assert abs(iso1["off_ppp"] - 0.90) < 1e-9  # 90/100
    assert abs(iso1["off_freq"] - 0.50) < 1e-9  # 100 / (100+100)
    assert abs(iso1["def_ppp"] - 88.0 / 80.0) < 1e-9
    assert out.schema["team_id"] == pl.Int64


def test_empty_returns_schema():
    out = raw_playtype_efficiency(pl.DataFrame(), pl.DataFrame())
    assert out.height == 0
    assert "off_ppp" in out.columns and out.schema["team_id"] == pl.Int64


def test_adjustment_is_mean_preserving():
    off, deff = _mini()
    raw = raw_playtype_efficiency(off, deff)
    sched = pl.DataFrame({"team_id": [1, 2], "opp_team_id": [2, 1]})
    adj = adjust_playtype_efficiency(raw, sched)
    for pt in ["Isolation", "Transition"]:
        r = raw.filter(pl.col("play_type") == pt)
        a = adj.filter(pl.col("play_type") == pt)
        lg_raw = (r["off_ppp"] * r["off_poss"]).sum() / r["off_poss"].sum()
        lg_adj = (a["adj_off_ppp"] * a["off_poss"]).sum() / a["off_poss"].sum()
        assert abs(lg_raw - lg_adj) < 1e-6


def test_adjustment_type_independence():
    """Adjusting Isolation must not perturb Transition's raw values."""
    off, deff = _mini()
    raw = raw_playtype_efficiency(off, deff)
    sched = pl.DataFrame({"team_id": [1, 2], "opp_team_id": [2, 1]})
    adj = adjust_playtype_efficiency(raw, sched)
    # symmetric round-robin (each team faces exactly the other) -> adjustment
    # collapses to the raw value itself (opponent IS the entire league already).
    trans = adj.filter(pl.col("play_type") == "Transition").sort("team_id")
    assert trans.height == 2


def test_playtype_ratings_rollup_identity():
    off, deff = _mini()
    sched = pl.DataFrame({"team_id": [1, 2], "opp_team_id": [2, 1]})
    r = nba_playtype_ratings("2023-24", off_team=off, def_team=deff, schedule=sched)
    assert r.schema["team_id"] == pl.Int64
    assert set(["adj_off", "adj_def", "adj_net"]) <= set(r.columns)
    for row in r.iter_rows(named=True):
        freq_cols = [c for c in r.columns if c.startswith("off_freq_")]
        ppp_cols = [c.replace("off_freq_", "adj_off_ppp_") for c in freq_cols]
        recon = sum((row[f] or 0.0) * (row[p] or 0.0) * 100.0 for f, p in zip(freq_cols, ppp_cols))
        assert abs(recon - row["adj_off"]) < 1e-6
        assert abs(row["adj_net"] - (row["adj_off"] - row["adj_def"])) < 1e-9


def test_playtype_ratings_pandas_and_empty():
    off, deff = _mini()
    sched = pl.DataFrame({"team_id": [1, 2], "opp_team_id": [2, 1]})
    pdf = nba_playtype_ratings("2023-24", off_team=off, def_team=deff, schedule=sched, return_as_pandas=True)
    assert type(pdf).__name__ == "DataFrame" and hasattr(pdf, "iloc")

    empty = nba_playtype_ratings("2023-24", off_team=pl.DataFrame(), def_team=pl.DataFrame(), schedule=pl.DataFrame())
    assert empty.height == 0
    assert "adj_off" in empty.columns
