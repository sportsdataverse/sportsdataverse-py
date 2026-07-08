"""Tests for the garbage-filtered play substrate (Connelly flags + long frame)."""

import datetime

import polars as pl

from sportsdataverse.cfb.cfb_opponent_adjust import (
    add_explosive,
    add_success,
    build_play_long,
    flag_garbage_time,
)


def test_garbage_time_thresholds():
    pbp = pl.DataFrame(
        {
            "period": [1, 1, 2, 3, 4, 4, 5],
            "abs_score_diff": [44, 43, 38, 28, 22, 21, 22],
        }
    )
    out = flag_garbage_time(pbp)["garbage_time"].to_list()
    # strictly greater than the threshold -> garbage; equal -> not garbage.
    # OT (period 5) uses the 4th-quarter threshold.
    assert out == [True, False, True, True, True, False, True]


def test_garbage_time_derives_margin():
    pbp = pl.DataFrame(
        {
            "period": [4, 4],
            "pos_team_score": [50, 24],
            "def_pos_team_score": [3, 21],
        }
    )
    assert flag_garbage_time(pbp)["garbage_time"].to_list() == [True, False]


def test_success_rule():
    pbp = pl.DataFrame(
        {
            "down": [1, 1, 2, 2, 3, 3, 0, None],
            "distance": [10.0, 10.0, 10.0, 10.0, 5.0, 5.0, 10.0, 10.0],
            "yards_gained": [5.0, 4.0, 7.0, 6.0, 5.0, 4.0, 30.0, 30.0],
        }
    )
    # 1st: >=5 ok; 2nd: >=7 ok; 3rd: >=5 ok; non-downs -> False
    assert add_success(pbp)["success"].to_list() == [
        True,
        False,
        True,
        False,
        True,
        False,
        False,
        False,
    ]


def test_explosive_rule():
    pbp = pl.DataFrame(
        {
            "epa": [2.5, 2.3, 1.9, 1.7, None],
            "pass": [True, True, False, False, True],
            "rush": [False, False, True, True, False],
        }
    )
    assert add_explosive(pbp)["explosive"].to_list() == [
        True,
        False,
        True,
        False,
        False,
    ]


def _synthetic_pbp() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2021] * 5,
            "game_id": [401_000_001] * 5,
            "date": [datetime.date(2021, 9, 4)] * 4 + [datetime.date(2021, 9, 5)],
            "period": [1, 1, 4, 2, 1],
            "down": [1, 2, 1, 1, 1],
            "distance": [10.0] * 5,
            "yards_gained": [5.0, 8.0, 3.0, 12.0, 6.0],
            "epa": [0.5, 2.5, 0.1, 1.9, 0.2],
            "pass": [True, True, False, False, True],
            "rush": [False, False, True, True, False],
            "havoc": [False, False, False, True, False],
            "scrimmage_play": [True, True, True, False, True],
            "pos_team_score": [0, 0, 45, 0, 0],
            "def_pos_team_score": [0, 7, 0, 0, 0],
            "pos_team_id": [1, 1, 2, 2, 1],
            "def_pos_team_id": [2, 2, 1, 1, 2],
        }
    )


def test_build_play_long_filters_and_schema():
    long = build_play_long(_synthetic_pbp())
    # row 2 = garbage (4Q margin 45 > 21), row 3 = non-scrimmage -> dropped
    assert long.height == 3
    assert long.schema["team_id"] == pl.Utf8
    assert long.schema["opp_team_id"] == pl.Utf8
    assert long.schema["game_id"] == pl.Utf8
    assert long["team_id"].to_list() == ["1", "1", "1"]
    assert long["success"].to_list() == [True, True, True]
    assert long["explosive"].to_list() == [False, True, False]


def test_build_play_long_keeps_garbage_when_asked():
    long = build_play_long(_synthetic_pbp(), exclude_garbage=False)
    assert long.height == 4


def test_build_play_long_as_of_date_is_strict():
    # 2021-09-04 rows only; the same-day 09-05 cutoff row is excluded
    long = build_play_long(_synthetic_pbp(), as_of_date=datetime.date(2021, 9, 5))
    assert long["date"].max() == datetime.date(2021, 9, 4)
    assert long.height == 2


def test_build_play_long_empty_input():
    long = build_play_long(pl.DataFrame())
    assert long.height == 0
    assert long.schema["epa"] == pl.Float64
    assert long.schema["date"] == pl.Date
