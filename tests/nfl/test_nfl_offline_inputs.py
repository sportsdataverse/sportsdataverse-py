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
    # no join_participants=False needed: a supplied summary implies the offline path
    proc = NFLPlayProcess(gameId=GAME_ID)
    payload = proc.espn_nfl_pbp(summary=summary)
    assert proc.join_participants is False
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


def test_supplied_summary_never_fetches_odds(summary, monkeypatch):
    import copy

    import sportsdataverse.nfl.nfl_pbp as mod

    def _boom(*a, **k):
        raise AssertionError("network call on the offline path")

    monkeypatch.setattr(mod, "download", _boom)
    # one provider is enough offline (the live path wants two before trusting it)
    one = copy.deepcopy(summary)
    one["pickcenter"] = summary["pickcenter"][:1]
    proc = NFLPlayProcess(gameId=GAME_ID)
    proc.espn_nfl_pbp(summary=one)
    odds = proc._NFLPlayProcess__helper_nfl_pickcenter(proc.json)
    assert odds["gameSpreadAvailable"] is True and odds["gameSpread"] is not None
    # an empty pickcenter means the documented defaults, not a request
    none = copy.deepcopy(summary)
    none["pickcenter"] = []
    proc = NFLPlayProcess(gameId=GAME_ID)
    proc.espn_nfl_pbp(summary=none)
    assert proc._NFLPlayProcess__helper_nfl_pickcenter(proc.json)["gameSpreadAvailable"] is False


def test_odds_override_replaces_the_pickcenter_cascade(summary, monkeypatch):
    import sportsdataverse.nfl.nfl_pbp as mod

    def _boom(*a, **k):
        raise AssertionError("network call on the offline path")

    monkeypatch.setattr(mod, "download", _boom)
    # nflverse convention: spread_line positive = home favored
    spread_line, total_line = 8.5, 47.5
    proc = NFLPlayProcess(
        gameId=GAME_ID,
        odds_override={
            "gameSpread": abs(spread_line),
            "overUnder": total_line,
            "homeFavorite": spread_line > 0,
            "gameSpreadAvailable": True,
        },
    )
    proc.espn_nfl_pbp(summary=summary)
    odds = proc._NFLPlayProcess__helper_nfl_pickcenter(proc.json)
    assert odds == {"gameSpread": 8.5, "overUnder": 47.5, "homeFavorite": True, "gameSpreadAvailable": True}
    assert proc.odds_source == "injected"
    with pytest.raises(ValueError, match="missing required keys"):
        NFLPlayProcess(gameId=GAME_ID, odds_override={"gameSpread": 1})


@pytest.mark.parametrize("keys", [("start",), ("end",), ("clock",), ("homeScore", "awayScore")], ids=lambda k: k[0])
def test_a_play_field_absent_from_every_play_does_not_crash(summary, keys):
    """C-NF-10(a): with a block absent from EVERY play json_normalize emits no column for it, and the
    plays build raised ColumnNotFoundError; the processor now runs with those columns null."""
    stripped = json.loads(json.dumps(summary))
    for drive in stripped["drives"]["previous"]:
        for play in drive["plays"]:
            for key in keys:
                play.pop(key, None)
    proc = NFLPlayProcess(gameId=GAME_ID)
    proc.espn_nfl_pbp(summary=stripped)
    plays = proc.run_processing_pipeline()["plays"]
    assert len(plays) > 100


_ODDS_KEYS = ("gameSpread", "overUnder", "homeFavorite", "gameSpreadAvailable")


def _payload_odds(summary, **kwargs):
    import copy

    proc = NFLPlayProcess(gameId=GAME_ID, **kwargs)
    pbp_txt, _ = proc._NFLPlayProcess__helper_nfl_pbp(copy.deepcopy(proc.espn_nfl_pbp(summary=summary)))
    return proc, pbp_txt


def test_pickcenter_round_trip_odds_are_espn_scalars(summary):
    """The CFB C-M19 twin: the summary_pickcenter odds were shape-(1,) ndarrays, so the payload's odds
    could not be serialized into ``odds_override``. They are Python scalars equal to ESPN's pickcenter
    numbers (the oracle) and survive JSON into ``odds_override`` unchanged."""
    proc, pbp_txt = _payload_odds(summary)
    assert proc.odds_source == "summary_pickcenter"
    odds = {k: pbp_txt[k] for k in _ODDS_KEYS}
    assert [type(odds[k]) for k in _ODDS_KEYS] == [float, float, bool, bool]
    assert odds == {"gameSpread": -8.5, "overUnder": 40.5, "homeFavorite": True, "gameSpreadAvailable": True}
    assert {(p["spread"], p["overUnder"], p["homeTeamOdds"]["favorite"]) for p in summary["pickcenter"]} == {
        (odds["gameSpread"], odds["overUnder"], odds["homeFavorite"]),
    }
    again, re_txt = _payload_odds(summary, odds_override=json.loads(json.dumps(odds)))
    assert again.odds_source == "injected"
    assert {k: re_txt[k] for k in _ODDS_KEYS} == odds
    assert re_txt["homeTeamSpread"] == pbp_txt["homeTeamSpread"] == 8.5


def test_pickcenter_all_null_spread_takes_the_default_path(summary):
    """An all-null pickcenter spread raised IndexError (the CFB C-NF-10(b) twin): it is no spread, so the
    offline path takes the documented defaults."""
    import copy

    stripped = copy.deepcopy(summary)
    for provider in stripped["pickcenter"]:
        provider["spread"] = None
    proc, pbp_txt = _payload_odds(stripped)
    assert proc.odds_source == "default"
    assert {k: pbp_txt[k] for k in _ODDS_KEYS} == {
        "gameSpread": 2.5,
        "overUnder": 55.5,
        "homeFavorite": True,
        "gameSpreadAvailable": False,
    }
