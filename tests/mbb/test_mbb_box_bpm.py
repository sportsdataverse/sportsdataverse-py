"""Tests for the box-BPM runtime scorer (``mbb_box_bpm``)."""

import polars as pl

import importlib

# the package star-exports the function `mbb_box_bpm`, shadowing the
# submodule attribute -- resolve the MODULE via importlib for monkeypatching
bpm = importlib.import_module("sportsdataverse.mbb.mbb_box_bpm")


def _fake_agg(seasons, league="mens"):
    # two players, one team; usage is the only live feature
    return pl.DataFrame(
        {
            "player_id": ["7", "8"],
            "player": ["A Guard", "B Big"],
            "season": [2025, 2025],
            "team_id": ["3", "3"],
            "minutes": [300.0, 100.0],
            "field_goals_made": [50.0, 20.0],
            "field_goals_attempted": [100.0, 40.0],
            "three_point_field_goals_made": [10.0, 0.0],
            "three_point_field_goals_attempted": [30.0, 0.0],
            "free_throws_made": [20.0, 10.0],
            "free_throws_attempted": [25.0, 20.0],
            "offensive_rebounds": [10.0, 30.0],
            "defensive_rebounds": [40.0, 60.0],
            "assists": [60.0, 10.0],
            "steals": [15.0, 5.0],
            "blocks": [2.0, 12.0],
            "turnovers": [30.0, 15.0],
            "points": [130.0, 50.0],
            "fga_rim": [30.0, 30.0],
            "fga_mid": [40.0, 10.0],
            "fga_three": [30.0, 0.0],
        }
    )


def _fake_ratings(seasons, league="mens", **kwargs):
    # two teams so the league means are hand-computable: mean_o=100, mean_d=95
    return pl.DataFrame(
        {
            "season": [2025, 2025],
            "team_id": ["3", "9"],
            "adj_o": [110.0, 90.0],
            "adj_d": [90.0, 100.0],
            "games": [30, 30],
        }
    )


_FAKE_ART = {
    "league": "mens",
    "method": "team_constrained",
    "feature_cols": ["usage"],
    "feature_mean": {"usage": 0.0},
    "feature_sd": {"usage": 1.0},
    "z_clip": 1e9,  # identity standardization: raw == usage * slope
    "obpm_coef": [0.0, 0.0],  # raw scores 0 -> adjustment carries everything
    "dbpm_coef": [0.0, 0.0],
    "min_minutes": 50.0,  # both fake players qualify
    "train_seasons": [2025, 2026],
}


def test_box_bpm_team_adjustment_arithmetic(monkeypatch):
    monkeypatch.setattr(bpm, "aggregate_player_seasons", _fake_agg)
    monkeypatch.setattr(bpm, "mbb_team_ratings", _fake_ratings)
    monkeypatch.setattr(bpm, "load_artifact", lambda name: _FAKE_ART)
    out = bpm.mbb_box_bpm(2025)
    assert out.schema["player_id"] == pl.Utf8
    # team 3: y_o = 110-100 = +10, y_d = -(90-95) = +5; raw scores are all 0,
    # so the uniform adjustment is c_o = 10/5 = 2, c_d = 5/5 = 1 per player
    for r in out.iter_rows(named=True):
        assert abs(r["box_obpm"] - 2.0) < 1e-9
        assert abs(r["box_dbpm"] - 1.0) < 1e-9
        assert abs(r["box_bpm"] - 3.0) < 1e-9


def test_box_bpm_nonzero_coef_weighted_sum_matches_team(monkeypatch):
    art = dict(_FAKE_ART, obpm_coef=[0.0, 0.1], dbpm_coef=[0.0, -0.05])
    monkeypatch.setattr(bpm, "aggregate_player_seasons", _fake_agg)
    monkeypatch.setattr(bpm, "mbb_team_ratings", _fake_ratings)
    monkeypatch.setattr(bpm, "load_artifact", lambda name: art)
    out = bpm.mbb_box_bpm(2025)
    j = out.join(_fake_agg(None).select("player_id", "minutes"), on="player_id")
    w = j.get_column("minutes") / j.get_column("minutes").sum() * 5.0
    # BPM constraint: minutes-weighted player scores sum to the team rating
    assert abs(float((w * j.get_column("box_obpm")).sum()) - 10.0) < 1e-6
    assert abs(float((w * j.get_column("box_dbpm")).sum()) - 5.0) < 1e-6


def test_box_bpm_empty_seasons_schema(monkeypatch):
    monkeypatch.setattr(bpm, "aggregate_player_seasons", lambda s, league="mens": pl.DataFrame())
    out = bpm.mbb_box_bpm([])
    assert out.columns == ["player_id", "player", "season", "team_id", "min", "box_obpm", "box_dbpm", "box_bpm"]
    assert out.height == 0
