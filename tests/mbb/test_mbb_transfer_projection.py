"""Tests for transfer-cohort detection + the transfer projection runtime."""

import importlib

import polars as pl

tp = importlib.import_module("sportsdataverse.mbb.mbb_transfer_projection")


def test_transfer_cohort_detection():
    # X moves A->B between 2022 and 2023, stays at B into 2024 (one row);
    # Y never moves (no row); Z re-transfers twice (two rows)
    rosters = pl.DataFrame(
        {
            "player_id": ["X", "X", "X", "Y", "Y", "Z", "Z", "Z"],
            "team_id": ["A", "B", "B", "A", "A", "A", "B", "C"],
            "season": [2022, 2023, 2024, 2022, 2023, 2022, 2023, 2024],
        }
    )
    out = tp.transfer_cohort(rosters)
    assert out.schema["player_id"] == pl.Utf8
    x = out.filter(pl.col("player_id") == "X")
    assert x.height == 1
    assert x.row(0, named=True) == {
        "player_id": "X",
        "from_team_id": "A",
        "to_team_id": "B",
        "from_season": 2022,
        "to_season": 2023,
    }
    assert out.filter(pl.col("player_id") == "Y").height == 0
    assert out.filter(pl.col("player_id") == "Z").height == 2


_FAKE_ART = {
    "league": "mens",
    "feature_cols": ["pre_box_bpm"],
    "coef": [1.0, 0.5],  # proj = 1 + 0.5 * pre
    "lambda": 10.0,
    "min_minutes": 150.0,
    "train_to_seasons": [2026],
}


def _fake_bpm(seasons, league="mens"):
    # both players moved 3 -> 9 between 2025 and 2026 (cohort is derived
    # from this boxscore discontinuity, not from a roster release)
    return pl.DataFrame(
        {
            "player_id": ["7", "8", "7", "8"],
            "player": ["Good Pre", "Bad Pre", "Good Pre", "Bad Pre"],
            "season": [2025, 2025, 2026, 2026],
            "team_id": ["3", "3", "9", "9"],
            "min": [600.0, 600.0, 500.0, 500.0],
            "box_obpm": [3.0, -1.0, 0.0, 0.0],
            "box_dbpm": [1.0, -1.0, 0.0, 0.0],
            "box_bpm": [4.0, -2.0, 0.0, 0.0],
        }
    )


def test_transfer_projection_monotone(monkeypatch):
    monkeypatch.setattr(tp, "mbb_box_bpm", _fake_bpm)
    monkeypatch.setattr(tp, "load_artifact", lambda name: _FAKE_ART)
    out = tp.mbb_transfer_projection(2026)
    assert out.schema["player_id"] == pl.Utf8
    a = out.filter(pl.col("player_id") == "7").row(0, named=True)
    b = out.filter(pl.col("player_id") == "8").row(0, named=True)
    assert a["proj_box_bpm"] > b["proj_box_bpm"]  # higher pre -> higher proj
    assert abs(a["proj_box_bpm"] - 3.0) < 1e-9  # 1 + 0.5*4
    assert abs(a["proj_delta"] - (-1.0)) < 1e-9  # 3 - 4
    assert a["to_season"] == 2026


def test_transfer_projection_empty(monkeypatch):
    monkeypatch.setattr(tp, "mbb_box_bpm", lambda s, league="mens": pl.DataFrame())
    out = tp.mbb_transfer_projection([])
    assert out.height == 0
    assert out.columns == [
        "player_id",
        "player",
        "from_team_id",
        "to_team_id",
        "to_season",
        "pre_box_bpm",
        "proj_box_bpm",
        "proj_delta",
    ]
