"""Tests for cfb_advanced_stats raw math + adjust wiring (synthetic, offline)."""

import datetime

import polars as pl

import importlib

m = importlib.import_module("sportsdataverse.cfb.cfb_advanced_stats")


def _long() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2021] * 4,
            "game_id": ["G"] * 4,
            "date": [datetime.date(2021, 9, 1)] * 4,
            "team_id": ["A", "A", "B", "B"],
            "opp_team_id": ["B", "B", "A", "A"],
            "epa": [3.0, -1.0, 0.5, 0.5],
            "success": [True, False, True, True],
            "explosive": [True, False, False, False],
            "havoc": [False, True, False, False],
            "pass": [True, False, True, True],
            "rush": [False, True, False, False],
        }
    )


def test_advanced_stats_raw_math(monkeypatch):
    monkeypatch.setattr(m, "load_cfb_pbp", lambda s, **k: pl.DataFrame())
    monkeypatch.setattr(m, "build_play_long", lambda *a, **k: _long())
    out = m.cfb_advanced_stats([2021], adjust=False)
    a = out.filter(pl.col("team_id") == "A").row(0, named=True)
    assert abs(a["off_success_rate"] - 0.5) < 1e-9  # 1 of 2 A plays successful
    assert abs(a["off_iso_ppp"] - 3.0) < 1e-9  # mean EPA on A's successful play
    assert abs(a["off_epa_play"] - 1.0) < 1e-9
    assert abs(a["off_explosive_rate"] - 0.5) < 1e-9
    assert abs(a["off_havoc_allowed"] - 0.5) < 1e-9
    # defense side: A faced B's 2 plays (both successful, epa 0.5 each)
    assert abs(a["def_success_rate"] - 1.0) < 1e-9
    assert abs(a["def_epa_play"] - 0.5) < 1e-9
    assert abs(a["def_havoc"] - 0.0) < 1e-9
    assert a["plays"] == 2
    assert out.schema["team_id"] == pl.Utf8
    assert "adj_off_epa_play" not in out.columns


def test_advanced_stats_adjusted_and_pandas(monkeypatch):
    monkeypatch.setattr(m, "load_cfb_pbp", lambda s, **k: pl.DataFrame())
    monkeypatch.setattr(m, "build_play_long", lambda *a, **k: _long())
    out = m.cfb_advanced_stats([2021])
    assert {"adj_off_epa_play", "adj_def_havoc", "off_epa_rank"} <= set(out.columns)
    assert out.schema["off_epa_rank"] == pl.Int64
    # A's offense is stronger -> rank 1
    assert out.filter(pl.col("team_id") == "A")["off_epa_rank"][0] == 1
    pdf = m.cfb_advanced_stats([2021], return_as_pandas=True)
    assert pdf.__class__.__module__.startswith("pandas")


def test_advanced_stats_empty(monkeypatch):
    monkeypatch.setattr(m, "load_cfb_pbp", lambda s, **k: pl.DataFrame())
    monkeypatch.setattr(m, "build_play_long", lambda *a, **k: pl.DataFrame(schema={"season": pl.Int64}))
    out = m.cfb_advanced_stats([1999])
    assert out.height == 0
    assert "off_iso_ppp" in out.columns and "adj_def_epa_play" in out.columns
