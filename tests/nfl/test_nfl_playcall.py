"""Unit tests for the play-call feature build + scorer (Tasks 1.1/1.3)."""

import polars as pl

from sportsdataverse.nfl.nfl_playcall import parse_personnel, playcall_features


def _mini_pbp() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": ["G"] * 5,
            "play_id": [1, 2, 3, 4, 5],
            "season": [2023] * 5,
            "week": [1] * 5,
            "posteam": ["A"] * 5,
            "defteam": ["B"] * 5,
            "pass": [1, 1, 0, 0, 1],
            "rush": [0, 0, 1, 1, 0],
            "qb_scramble": [0, 0, 0, 0, 1],
            "pass_length": ["short", "deep", None, None, None],
            "run_location": [None, None, "middle", "left", None],
            "run_gap": [None, None, "guard", "end", None],
            "down": [1, 3, 2, 1, 2],
            "ydstogo": [10.0, 8.0, 4.0, 10.0, 6.0],
            "yardline_100": [75.0, 50.0, 40.0, 80.0, 60.0],
            "score_differential": [0.0] * 5,
            "half_seconds_remaining": [1800.0] * 5,
            "game_seconds_remaining": [3600.0] * 5,
            "wp": [0.5] * 5,
            "shotgun": [1, 1, 0, 0, 1],
            "no_huddle": [0] * 5,
            "xpass": [0.6, 0.8, 0.4, 0.3, 0.7],
            "pass_oe": [0.0] * 5,
        }
    )


def test_family_labels():
    out = playcall_features(_mini_pbp()).sort("play_id")
    assert out["family"].to_list() == [
        "short_pass",
        "deep_pass",
        "inside_run",
        "outside_run",
        "scramble",
    ]
    assert out["is_pass"].to_list() == [1, 1, 0, 0, 1]


def test_personnel_parse():
    df = pl.DataFrame({"offense_personnel": ["1 RB, 2 TE, 2 WR", "2 RB, 1 TE, 2 WR"]})
    rb, te, wr = parse_personnel(pl.col("offense_personnel"))
    got = df.select(rb.alias("rb"), te.alias("te"), wr.alias("wr"))
    assert got["rb"].to_list() == [1, 2]
    assert got["te"].to_list() == [2, 1]
    assert got["wr"].to_list() == [2, 2]


def test_null_safe_without_participation():
    out = playcall_features(_mini_pbp(), participation=None)
    assert out["has_participation"].to_list() == [0] * 5
    assert out["n_rb"].null_count() == out.height


def test_participation_join():
    part = pl.DataFrame(
        {
            "game_id": ["G", "G"],
            "play_id": [1, 3],
            "offense_personnel": ["1 RB, 2 TE, 2 WR", "2 RB, 1 TE, 2 WR"],
            "defense_personnel": ["4 DL, 3 LB, 4 DB"] * 2,
            "offense_formation": ["SHOTGUN", "I_FORM"],
            "defenders_in_box": [6.0, 8.0],
            "number_of_pass_rushers": [4.0, None],
        }
    )
    out = playcall_features(_mini_pbp(), participation=part).sort("play_id")
    assert out["has_participation"].to_list() == [1, 0, 1, 0, 0]
    assert out["n_rb"].to_list()[0] == 1.0
    assert out["n_rb"].to_list()[2] == 2.0
