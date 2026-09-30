"""ESPN "& 0 at" downs: goal-to-go or a missing distance, told apart by the previous snap.

ESPN reports ``start.distance = 0`` on goal-to-go downs. When the text says
"Goal" the processor already sets the distance to the yards to the goal. ESPN also
writes "& 0 at" (no "Goal") for two different things:

* goal-to-go after the ball moved back -- "2nd & 0 at LSU 14" follows a sack that
  ended "2nd & Goal at LSU 14";
* a distance it simply lost -- "2nd & 0 at UWA 21" follows a play that ended
  "2nd & 18 at UWA 21".

The previous real snap's end state (same down, same spot) separates them. Across
the 2,472 such rows in 2014-2026 finals, 2,127 end "Goal" at that spot, 24 carry
the real distance, and 321 leave nothing to go on and stay 0.

Fixtures, trimmed from ``cfbfastR-cfb-raw/cfb/json/raw/{game_id}.json`` to the keys
the processor reads (see ``test_cfb_pbp_offline._trimmed``):

* ``summary_401752671_trimmed.json.gz`` -- LSU @ Clemson, 2025 week 1.
* ``summary_400559176_trimmed.json.gz`` -- 2014, "2nd & 0 at UWA 21" after "2nd & 18".
* ``summary_400547673_trimmed.json.gz`` -- 2014, "1st & 0 at TULN 15" after a penalty
  row with no end state.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.cfb.cfb_pbp as cfb_pbp_mod
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _run(monkeypatch, game_id: int) -> pl.DataFrame:
    with gzip.open(FIX / f"summary_{game_id}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        summary = json.load(fh)

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr(cfb_pbp_mod, "download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=game_id)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    return pl.from_dicts(proc.plays_json, infer_schema_length=None)


def _play(df: pl.DataFrame, play_id: int) -> dict:
    row = df.filter(pl.col("id").cast(pl.Int64) == play_id)
    assert row.height == 1, f"play {play_id} not found exactly once"
    return row.row(0, named=True)


@pytest.mark.parametrize(
    ("game_id", "play_id", "text", "expected"),
    [
        # goal-to-go after a sack: the previous snap ended "2nd & Goal at LSU 14"
        (401752671, 401752671102948001, "2nd & 0 at LSU 14", 14),
        (401752671, 401752671102948901, "3rd & 0 at LSU 14", 14),
        # a lost distance: the previous snap ended "2nd & 18 at UWA 21"
        (400559176, 400559176102897711, "2nd & 0 at UWA 21", 18),
        # the previous snap (a penalty with no end state) started "1st & Goal at
        # TULN 10" for the same offense: the series is still goal-to-go
        (400547673, 400547673104976905, "1st & 0 at TULN 15", 15),
    ],
)
def test_amp0_distance_follows_the_previous_snap(monkeypatch, game_id, play_id, text, expected):
    r = _play(_run(monkeypatch, game_id), play_id)
    assert r["start.downDistanceText"] == text
    assert r["start.distance"] == expected


def test_goal_text_rows_are_unchanged(monkeypatch):
    """The existing "Goal" rule still runs first: "1st & Goal at LSU 4" -> 4."""
    r = _play(_run(monkeypatch, 401752671), 401752671102945901)
    assert r["start.downDistanceText"] == "1st & Goal at LSU 4"
    assert r["start.distance"] == 4


def _frame(rows: list[tuple]) -> pl.DataFrame:
    cols = [
        "type.text",
        "start.down",
        "start.distance",
        "start.yardsToEndzone",
        "start.downDistanceText",
        "end.down",
        "end.distance",
        "end.downDistanceText",
    ]
    return pl.DataFrame(rows, schema=cols, orient="row")


@pytest.mark.parametrize(
    ("prev_rows", "row", "expected"),
    [
        # goal-to-go: previous end "Goal" at the same down and spot
        (
            [("Sack", 1, 4, 4, "1st & Goal at LSU 4", 2, 14, "2nd & Goal at LSU 14")],
            ("Pass Incompletion", 2, 0, 14, "2nd & 0 at LSU 14", 3, 14, "3rd & Goal at LSU 14"),
            14,
        ),
        # the same, with a timeout between the two snaps
        (
            [
                ("Sack", 1, 4, 4, "1st & Goal at LSU 4", 2, 14, "2nd & Goal at LSU 14"),
                ("Timeout", 2, 14, 14, None, 2, 14, None),
            ],
            ("Pass Incompletion", 2, 0, 14, "2nd & 0 at LSU 14", 3, 14, None),
            14,
        ),
        # lost distance: previous end "2nd & 18 at UWA 21"
        (
            [("Rush", 1, 10, 23, "1st & 10 at UWA 23", 2, 18, "2nd & 18 at UWA 21")],
            ("Rush", 2, 0, 21, "2nd & 0 at UWA 21", 3, 0, None),
            18,
        ),
        # a previous end distance past the goal line is capped at the yards to go
        (
            [("Rush", 1, 10, 30, "1st & 10 at TROY 30", 2, 48, "2nd & 48 at TROY 38")],
            ("Rush", 2, 0, 38, "2nd & 0 at TROY 38", 3, 0, None),
            38,
        ),
        # spot differs -> nothing to read
        (
            [("Rush", 1, 10, 23, "1st & 10 at UWA 23", 2, 18, "2nd & 18 at UWA 25")],
            ("Rush", 2, 0, 21, "2nd & 0 at UWA 21", 3, 0, None),
            0,
        ),
        # down differs -> nothing to read
        (
            [("Rush", 1, 10, 23, "1st & 10 at UWA 23", 3, 18, "3rd & 18 at UWA 21")],
            ("Rush", 2, 0, 21, "2nd & 0 at UWA 21", 3, 0, None),
            0,
        ),
        # previous end distance 0 without "Goal" -> nothing to read
        (
            [("Penalty", 1, 10, 10, "1st & Goal at TULN 10", 1, 0, None)],
            ("Rush", 1, 0, 15, "1st & 0 at TULN 15", 2, 0, None),
            0,
        ),
        # "Goal" end state that ESPN also encodes as distance 0: only the Goal
        # branch can resolve it
        (
            [("Sack", 1, 4, 4, "1st & Goal at LSU 4", 2, 0, "2nd & Goal at LSU 14")],
            ("Pass Incompletion", 2, 0, 14, "2nd & 0 at LSU 14", 3, 14, None),
            14,
        ),
        # same spot and down, no "Goal", end distance missing -> nothing to read
        (
            [("Rush", 1, 10, 23, "1st & 10 at UWA 23", 2, None, "2nd & 0 at UWA 21")],
            ("Rush", 2, 0, 21, "2nd & 0 at UWA 21", 3, 0, None),
            0,
        ),
        # kickoffs are never touched
        (
            [("Rush", 1, 10, 23, "1st & 10 at UWA 23", 2, 18, "2nd & 18 at UWA 21")],
            ("Kickoff", 2, 0, 21, "2nd & 0 at UWA 21", 3, 0, None),
            0,
        ),
    ],
)
def test_repair_amp0_distance_branches(prev_rows, row, expected):
    """Values are taken from the real plays above; each branch in isolation."""
    out = cfb_pbp_mod._repair_amp0_distance(_frame([*prev_rows, row]))
    assert out["start.distance"][-1] == expected
    assert out["start.distance"].dtype == pl.Int64
    # rows other than the "& 0 at" one are never touched
    assert out["start.distance"][:-1].to_list() == [r[2] for r in prev_rows]


def test_repair_amp0_distance_skips_payloads_without_end_text():
    df = _frame([("Rush", 2, 0, 21, "2nd & 0 at UWA 21", 3, 0, None)]).drop("end.downDistanceText")
    assert cfb_pbp_mod._repair_amp0_distance(df).equals(df)


def test_repair_amp0_distance_survives_an_all_null_end_text():
    """A payload that carries end.downDistanceText but never fills it types the
    column Null; the repair must leave the frame alone rather than raise."""
    df = _frame(
        [
            ("Penalty", 1, 10, 10, "1st & Goal at TULN 10", 1, 0, None),
            ("Rush", 1, 0, 15, "1st & 0 at TULN 15", 2, 0, None),
        ]
    )
    assert df.schema["end.downDistanceText"] == pl.Null
    assert cfb_pbp_mod._repair_amp0_distance(df)["start.distance"].to_list() == [10, 0]


def _series_frame(rows: list[tuple]) -> pl.DataFrame:
    cols = [
        "type.text",
        "start.team.id",
        "start.down",
        "start.distance",
        "start.yardsToEndzone",
        "start.downDistanceText",
        "end.down",
        "end.distance",
        "end.downDistanceText",
    ]
    return pl.DataFrame(rows, schema=cols, orient="row")


@pytest.mark.parametrize(
    ("prev_team", "prev_start", "expected"),
    [
        # 400547673: a penalty with no end state backs "1st & Goal at TULN 10" up to the 15
        ("2653", "1st & Goal at TULN 10", 15),
        # a different offense (possession changed): nothing to read
        ("202", "1st & Goal at TULN 10", 0),
        # the previous snap did not start goal-to-go: nothing to read
        ("2653", "1st & 10 at TULN 30", 0),
    ],
)
def test_repair_amp0_distance_same_series(prev_team, prev_start, expected):
    df = _series_frame(
        [
            ("Penalty", prev_team, 1, 10, 10, prev_start, 1, 0, None),
            ("Rush", "2653", 1, 0, 15, "1st & 0 at TULN 15", 2, 0, None),
        ]
    )
    assert cfb_pbp_mod._repair_amp0_distance(df)["start.distance"].to_list() == [10, expected]
