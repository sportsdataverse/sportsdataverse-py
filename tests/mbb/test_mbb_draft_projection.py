"""Tests for the draft projection runtime (``mbb_draft_projection``)."""

import importlib

import polars as pl

dp = importlib.import_module("sportsdataverse.mbb.mbb_draft_projection")

_FAKE_ART = {
    "league": "mens",
    "feature_cols": ["box_bpm"],
    "feature_mean": [0.0],
    "feature_sd": [1.0],
    "archetype_labels": [],
    "prob_coef": [0.0, 1.0],  # logit = box_bpm
    "pick_coef": [30.0, -3.0],  # pick = 30 - 3*box_bpm (better player -> earlier pick)
    "tier_edges": [14.5, 30.5, 45.5],
    "tier_labels": ["lottery", "first round", "early second", "late second"],
    "bubble_rank": 150,
    "median_composite": 75.0,
    "min_minutes": 150.0,
    "train_seasons": [2025, 2026],
}


def _fake_bpm(seasons, league="mens"):
    return pl.DataFrame(
        {
            "player_id": ["7", "8"],
            "player": ["Star Guy", "Role Guy"],
            "season": [2025, 2025],
            "team_id": ["3", "3"],
            "min": [900.0, 700.0],
            "box_obpm": [6.0, 0.0],
            "box_dbpm": [3.0, 0.0],
            "box_bpm": [9.0, 0.0],
        }
    )


def test_draft_projection_monotone_and_tiers(monkeypatch):
    monkeypatch.setattr(dp, "mbb_box_bpm", _fake_bpm)
    monkeypatch.setattr(dp, "load_artifact", lambda name: _FAKE_ART)
    out = dp.mbb_draft_projection(2025)
    assert out.schema["player_id"] == pl.Utf8
    star = out.filter(pl.col("player_id") == "7").row(0, named=True)
    role = out.filter(pl.col("player_id") == "8").row(0, named=True)
    # better box_bpm -> higher prob, earlier (lower) pick
    assert star["draft_prob"] > role["draft_prob"]
    assert star["projected_pick"] < role["projected_pick"]
    # pick = 30 - 3*9 = 3 -> lottery; pick = 30 -> first round
    assert star["pro_tier"] == "lottery"
    assert role["pro_tier"] == "first round"
    assert 0.0 <= role["draft_prob"] <= star["draft_prob"] <= 1.0


def test_draft_projection_empty(monkeypatch):
    monkeypatch.setattr(dp, "mbb_box_bpm", lambda s, league="mens": pl.DataFrame())
    out = dp.mbb_draft_projection([])
    assert out.height == 0
    assert out.columns == [
        "player_id",
        "player",
        "season",
        "team_id",
        "draft_prob",
        "projected_pick",
        "pro_tier",
    ]
