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


def test_real_season_slice_compiles_and_scores_against_real_outcomes(monkeypatch):
    """supp-013: build_wbb_season_wp compiles the committed real 2023-24 corpus
    (tests/fixtures/wbb_prediction/: the first 400 games of pbp_sample_2024, every 25th play, with the
    full results_2024 + team_box_2024), scored against each game's real result. Observed 2026-10-11:
    Brier in-game 0.11951, pregame 0.17037, base rate 0.24472; floors rounded to the safe side."""
    from pathlib import Path

    fix = Path(__file__).resolve().parents[1] / "fixtures" / "wbb_prediction"
    pbp = pl.read_parquet(fix / "pbp_sample_2024.parquet")
    pbp = pbp.filter(pl.col("game_id").is_in(pbp["game_id"].unique().sort()[:400].implode())).with_columns(
        pl.int_range(pl.len())
        .over("game_id", order_by=-pl.col("start_game_seconds_remaining"))
        .alias("game_play_number")
    )
    sched, box = pl.read_parquet(fix / "results_2024.parquet"), pl.read_parquet(fix / "team_box_2024.parquet")
    monkeypatch.setattr(core, "_load_league_frames", lambda season, league: (pbp, sched, box))
    out = build_wbb_season_wp(2024)
    assert out.height == pbp.height and set(pbp.columns) <= set(out.columns)
    y = out["home_win"].cast(pl.Float64)
    brier_wp = float(((out["home_win_prob"] - y) ** 2).mean())
    brier_pre = float(((out["pregame_home_prob"] - y) ** 2).mean())
    assert brier_wp <= 0.1196 and brier_pre <= 0.1704, (brier_wp, brier_pre)
    assert brier_wp < brier_pre < float(((y.mean() - y) ** 2).mean())
    print("WBB_SEASON_WP_REAL_OK", round(brier_wp, 5), round(brier_pre, 5))
