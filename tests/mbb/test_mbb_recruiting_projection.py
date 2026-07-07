"""Tests for the recruiting -> production runtime (``mbb_recruiting_projection``)."""

import importlib

import polars as pl

rec = importlib.import_module("sportsdataverse.mbb.mbb_recruiting_projection")

_FAKE_ART = {
    "league": "mens",
    "feature_cols": ["composite", "log_rank"],
    "coef": [0.0, 0.1, 0.0],  # exp = 0.1 * composite
    "lambda": 10.0,
    "bubble_rank": 150,
    "min_minutes": 150.0,
    "train_seasons": [2025, 2026],
}


def _fake_recruits(seasons, league="mens"):
    return pl.DataFrame(
        {
            "recruit_id": ["1", "2"],
            "player": ["High Comp", "Low Comp"],
            "team_id": ["3", "4"],
            "season": [2025, 2025],
            "composite": [95.0, 75.0],
            "rank_nat": [1, 90],
        }
    )


def _fake_bpm(seasons, league="mens"):
    # only High Comp has a realized freshman season
    return pl.DataFrame(
        {
            "player_id": ["77"],
            "player": ["High Comp"],
            "season": [2025],
            "team_id": ["3"],
            "min": [500.0],
            "box_obpm": [4.0],
            "box_dbpm": [2.0],
            "box_bpm": [6.0],
        }
    )


def test_recruiting_projection_monotone_and_residual(monkeypatch):
    monkeypatch.setattr(rec, "_load_recruits", _fake_recruits)
    monkeypatch.setattr(rec, "mbb_box_bpm", _fake_bpm)
    monkeypatch.setattr(rec, "load_artifact", lambda name: _FAKE_ART)
    out = rec.mbb_recruiting_projection(2025)
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["exp_box_bpm"] == pl.Float64
    hi = out.filter(pl.col("recruit_id") == "1").row(0, named=True)
    lo = out.filter(pl.col("recruit_id") == "2").row(0, named=True)
    # higher composite -> higher expected production
    assert hi["exp_box_bpm"] > lo["exp_box_bpm"]
    assert abs(hi["exp_box_bpm"] - 9.5) < 1e-9
    # realized 6.0 - expected 9.5 = -3.5; unmatched recruit -> null
    assert abs(hi["resume_residual"] - (-3.5)) < 1e-9
    assert hi["player_id"] == "77"
    assert lo["resume_residual"] is None and lo["player_id"] is None


def test_recruiting_projection_empty(monkeypatch):
    monkeypatch.setattr(rec, "_load_recruits", lambda s, league="mens": pl.DataFrame())
    out = rec.mbb_recruiting_projection([])
    assert out.height == 0
    assert out.columns == [
        "recruit_id",
        "player_id",
        "player",
        "season",
        "team_id",
        "composite",
        "rank_nat",
        "exp_box_bpm",
        "resume_residual",
    ]
