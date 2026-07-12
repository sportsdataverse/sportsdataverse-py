"""Tests for the WBB season win-probability shim (``wbb_win_prob``)."""

from __future__ import annotations

import datetime

import polars as pl

import sportsdataverse.mbb.mbb_win_prob as core
from sportsdataverse.wbb.wbb_win_prob import build_wbb_season_wp

_D = datetime.date(2024, 1, 1)


def _frames():
    """One completed game (no prior -> fallback anchor) in the loader schemas."""
    pbp = pl.DataFrame(
        {
            "game_id": [5, 5],
            "game_play_number": [1, 2],
            "home_score": [0, 60],
            "away_score": [0, 55],
            "start_game_seconds_remaining": [2400, 30],
            "team_id": [1, 2],
            "home_team_id": [1, 1],
        },
        schema_overrides={
            c: pl.Int32
            for c in (
                "game_id",
                "game_play_number",
                "home_score",
                "away_score",
                "start_game_seconds_remaining",
                "team_id",
                "home_team_id",
            )
        },
    )
    schedule = pl.DataFrame(
        {
            "game_id": [5],
            "season": [2024],
            "date": [_D],
            "home_team_id": [1],
            "away_team_id": [2],
            "home_score": [60],
            "away_score": [55],
            "neutral_site": [False],
        },
        schema_overrides={
            c: pl.Int32 for c in ("game_id", "season", "home_team_id", "away_team_id", "home_score", "away_score")
        },
    )
    team_box = pl.DataFrame(
        {
            "game_id": [5, 5],
            "season": [2024, 2024],
            "game_date": [_D, _D],
            "team_id": [1, 2],
            "field_goals_attempted": [60.0, 58.0],
            "offensive_rebounds": [10.0, 9.0],
            "turnovers": [12.0, 13.0],
            "free_throws_attempted": [20.0, 18.0],
            "team_score": [60.0, 55.0],
        },
        schema_overrides={c: pl.Int32 for c in ("game_id", "season", "team_id")},
    )
    return pbp, schedule, team_box


def test_delegates_with_womens_league(monkeypatch):
    seen = {}

    def _fake(season, league):
        seen["league"] = league
        return _frames()

    monkeypatch.setattr(core, "_load_league_frames", _fake)
    out = build_wbb_season_wp(2024)
    assert seen["league"] == "womens"
    assert set(core._WP_COLS).issubset(out.columns)
    assert out.schema["home_win_prob"] == pl.Float64
