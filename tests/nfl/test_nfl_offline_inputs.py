"""Offline processor inputs: a stored summary + stored play items, no network.

Uses ``tests/nfl/fixtures/summary_401872922.json``. These are the entry points a
committed raw library (nfl-raw's ``nfl/espn/``) feeds the processor through.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.play_participants import (
    athlete_lookup_from_summary,
    coalesce_participants,
    play_participants_from_items,
)
from sportsdataverse.nfl import NFLPlayProcess

FIX = Path(__file__).parent / "fixtures" / "summary_401872922.json"
GAME_ID = 401872922


@pytest.fixture(scope="module")
def summary() -> dict:
    return json.loads(FIX.read_text())


def test_espn_nfl_pbp_accepts_a_prefetched_summary(summary, monkeypatch):
    import sportsdataverse.nfl.nfl_pbp as mod

    def _boom(*a, **k):  # any request is a failure
        raise AssertionError("network call on the offline path")

    monkeypatch.setattr(mod, "download", _boom)
    proc = NFLPlayProcess(gameId=GAME_ID, join_participants=False)
    payload = proc.espn_nfl_pbp(summary=summary)
    assert "drives" in payload and "boxscore" in payload
    out = proc.run_processing_pipeline()
    assert set(out) >= {"plays", "advBoxScore"} and len(out["plays"]) > 100


def test_athlete_lookup_from_summary(summary):
    lookup = athlete_lookup_from_summary(summary)
    assert len(lookup) > 50
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in lookup.items())


def test_play_participants_from_items_is_offline_and_joins():
    items = [
        {
            "id": "4018729221",
            "participants": [
                {
                    "athlete": {"$ref": "http://x/athletes/7"},
                    "position": {"$ref": "http://x/positions/8"},
                    "type": "passer",
                    "order": 1,
                },
                {
                    "athlete": {"$ref": "http://x/athletes/9"},
                    "position": {"$ref": "http://x/positions/1"},
                    "type": "receiver",
                    "order": 2,
                },
            ],
        },
        {"id": "4018729222", "participants": []},
    ]
    wide = play_participants_from_items(items, GAME_ID, athlete_lookup={"7": "Q. Back"})
    assert wide.height == 1
    row = wide.row(0, named=True)
    assert row["play_id"] == 4018729221
    assert (row["passer_player_id"], row["passer_player_name"]) == ("7", "Q. Back")
    assert (row["receiver_player_id"], row["receiver_player_name"]) == ("9", None)
    # unresolved names keep the text-extracted name; ids still take precedence
    plays = pl.DataFrame(
        {
            "id": [4018729221],
            "passer_player_name": ["Q.Back"],
            "passer_player_id": [None],
            "receiver_player_name": ["W.Out"],
            "receiver_player_id": ["old"],
        }
    )
    joined = coalesce_participants(plays, wide, prefer_ids=True)
    got = joined.row(0, named=True)
    assert got["passer_player_name"] == "Q. Back" and got["passer_player_id"] == "7"
    assert got["receiver_player_name"] == "W.Out" and got["receiver_player_id"] == "9"
    assert play_participants_from_items([], GAME_ID).height == 0
