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
        # nothing to go on (the previous row is a penalty with no end state)
        (400547673, 400547673104976905, "1st & 0 at TULN 15", 0),
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
