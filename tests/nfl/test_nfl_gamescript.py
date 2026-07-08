"""Unit tests for game-script pace / PROE (Tasks 2.1/2.3)."""

import polars as pl

from sportsdataverse.nfl.nfl_gamescript import team_game_pace


def _pbp() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": ["G"] * 4,
            "season": [2023] * 4,
            "week": [1] * 4,
            "posteam": ["A"] * 4,
            "drive": [1.0, 1.0, 2.0, 2.0],
            "play_type": ["pass", "run", "pass", "pass"],
            "qb_dropback": [1, 0, 1, 1],
            "pass": [1, 0, 1, 1],
            "pass_oe": [10.0, None, -5.0, 5.0],
            "game_seconds_remaining": [3600.0, 3560.0, 3000.0, 2960.0],
            "wp": [0.5] * 4,
            "half_seconds_remaining": [1800.0] * 4,
        }
    )


def test_pace_and_proe():
    out = team_game_pace(_pbp()).row(0, named=True)
    assert out["off_plays"] == 4
    # drive1: (3600-3560)/2=20 ; drive2: (3000-2960)/2=20 -> mean 20 sec/play
    assert abs(out["sec_per_play"] - 20.0) < 1e-6
    # proe = mean(10, -5, 5) over dropbacks = 3.333...
    assert abs(out["proe"] - (10.0 - 5.0 + 5.0) / 3) < 1e-6


def test_neutral_filter():
    df = _pbp().with_columns(
        pl.Series("wp", [0.5, 0.5, 0.95, 0.95]),
    )
    out = team_game_pace(df).row(0, named=True)
    assert out["neutral_plays"] == 2
    assert out["off_plays"] == 4


def test_kneels_excluded():
    df = _pbp().with_columns(pl.Series("play_type", ["pass", "qb_kneel", "pass", "pass"]))
    out = team_game_pace(df).row(0, named=True)
    assert out["off_plays"] == 3


def test_empty_zero_row():
    out = team_game_pace(_pbp().head(0))
    assert out.height == 0
    assert "sec_per_play" in out.columns
