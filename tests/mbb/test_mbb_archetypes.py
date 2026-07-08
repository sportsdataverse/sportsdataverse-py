"""Tests for the archetype runtime assignment (``mbb_archetypes``)."""

import importlib

import polars as pl

arch = importlib.import_module("sportsdataverse.mbb.mbb_archetypes")


def _fake_agg(seasons, league="mens"):
    # player 7: all-three guard; player 8: all-rim big
    return pl.DataFrame(
        {
            "player_id": ["7", "8"],
            "player": ["A Shooter", "B Big"],
            "season": [2025, 2025],
            "team_id": ["3", "3"],
            "position": ["G", "C"],
            "minutes": [300.0, 300.0],
            "field_goals_made": [50.0, 50.0],
            "field_goals_attempted": [100.0, 100.0],
            "three_point_field_goals_made": [50.0, 0.0],
            "three_point_field_goals_attempted": [100.0, 0.0],
            "free_throws_made": [10.0, 10.0],
            "free_throws_attempted": [12.0, 20.0],
            "offensive_rebounds": [2.0, 40.0],
            "defensive_rebounds": [30.0, 60.0],
            "assists": [20.0, 5.0],
            "steals": [10.0, 5.0],
            "blocks": [1.0, 20.0],
            "turnovers": [15.0, 15.0],
            "points": [160.0, 110.0],
            "fga_rim": [0.0, 100.0],
            "fga_mid": [0.0, 0.0],
            "fga_three": [100.0, 0.0],
        }
    )


_FAKE_ART = {
    "league": "mens",
    "feature_cols": ["three_share", "rim_share"],
    "feature_mean": {"three_share": 0.5, "rim_share": 0.5},
    "feature_sd": {"three_share": 0.5, "rim_share": 0.5},
    "centers": [[1.0, -1.0], [-1.0, 1.0]],  # z-space: shooter vs rim big
    "labels": ["shooter", "rim big"],
    "k": 2,
    "seed": 0,
}


def test_archetypes_nearest_center(monkeypatch):
    monkeypatch.setattr(arch, "aggregate_player_seasons", _fake_agg)
    monkeypatch.setattr(arch, "load_artifact", lambda name: _FAKE_ART)
    out = arch.mbb_archetypes(2025)
    assert out.schema["player_id"] == pl.Utf8
    assert out.schema["cluster"] == pl.Int64
    assert out.schema["dist_to_center"] == pl.Float64
    a = out.filter(pl.col("player_id") == "7").row(0, named=True)
    b = out.filter(pl.col("player_id") == "8").row(0, named=True)
    assert a["archetype"] == "shooter" and a["cluster"] == 0
    assert b["archetype"] == "rim big" and b["cluster"] == 1
    assert a["dist_to_center"] >= 0.0


def test_archetypes_empty_seasons(monkeypatch):
    monkeypatch.setattr(arch, "aggregate_player_seasons", lambda s, league="mens": pl.DataFrame())
    out = arch.mbb_archetypes([])
    assert out.height == 0
    assert out.columns == ["player_id", "player", "season", "team_id", "min", "archetype", "cluster", "dist_to_center"]


def test_archetypes_bundled_artifact_loads():
    """The committed artifact drives a real assignment (labels are frozen)."""
    from sportsdataverse.mbb.mbb_player_value_constants import load_artifact

    art = load_artifact("mbb_archetypes")
    assert art["k"] == len(art["labels"]) == len(art["centers"])
    assert set(art["feature_cols"]) >= {"usage", "rim_share", "three_share", "pos_score"}
