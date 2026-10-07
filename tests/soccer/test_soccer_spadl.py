"""SPADL conversion: vocabularies, mapping, post-processing and parity with the socceraction oracle."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from sportsdataverse.soccer import spadl

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
EVENTS = FIXTURES / "kloppy" / "statsbomb_8658_events.json"
LINEUPS = FIXTURES / "kloppy" / "statsbomb_8658_lineups.json"
ORACLE = FIXTURES / "socceraction" / "8658_spadl.csv"


def test_vocabularies_are_socceraction_verbatim() -> None:
    assert spadl.FIELD_LENGTH == 105.0 and spadl.FIELD_WIDTH == 68.0
    assert spadl.BODYPARTS == ("foot", "head", "other", "head/other", "foot_left", "foot_right")
    assert spadl.RESULTS == ("fail", "success", "offside", "owngoal", "yellow_card", "red_card")
    assert spadl.ACTIONTYPES == (
        "pass",
        "cross",
        "throw_in",
        "freekick_crossed",
        "freekick_short",
        "corner_crossed",
        "corner_short",
        "take_on",
        "foul",
        "tackle",
        "interception",
        "shot",
        "shot_penalty",
        "shot_freekick",
        "keeper_save",
        "keeper_claim",
        "keeper_punch",
        "keeper_pick_up",
        "clearance",
        "bad_touch",
        "non_action",
        "dribble",
        "goalkick",
    )
    assert spadl.SPADL_COLUMNS == (
        "game_id",
        "original_event_id",
        "action_id",
        "period_id",
        "time_seconds",
        "team_id",
        "player_id",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "bodypart_id",
        "bodypart_name",
        "type_id",
        "type_name",
        "result_id",
        "result_name",
    )
    assert spadl.SPADL_SCHEMA["game_id"] == pl.Utf8 and spadl.SPADL_SCHEMA["action_id"] == pl.Int64
    assert spadl.SPADL_SCHEMA["start_x"] == pl.Float64


statsbomb = pytest.importorskip("kloppy.statsbomb")


def _dataset() -> Any:
    return statsbomb.load(event_data=str(EVENTS), lineup_data=str(LINEUPS))


def _kd() -> Any:
    import kloppy.domain as kd

    return kd


def test_parse_event_maps_the_first_pass_of_the_fixture() -> None:
    kd = _kd()
    ds = _dataset()
    first_pass = next(e for e in ds.events if e.event_type == kd.EventType.PASS)
    t, r, b = spadl._parse_event(first_pass, kd)
    assert spadl.ACTIONTYPES[t] in {
        "pass",
        "cross",
        "freekick_short",
        "freekick_crossed",
        "corner_short",
        "corner_crossed",
        "throw_in",
        "goalkick",
    }
    assert spadl.RESULTS[r] in {"fail", "success", "offside"}
    assert spadl.BODYPARTS[b] in spadl.BODYPARTS


def test_parse_event_maps_every_fixture_event_into_the_vocabularies() -> None:
    kd = _kd()
    for e in _dataset().events:
        t, r, b = spadl._parse_event(e, kd)
        assert 0 <= t < len(spadl.ACTIONTYPES) and 0 <= r < len(spadl.RESULTS) and 0 <= b < len(spadl.BODYPARTS)


def test_parse_event_non_action_for_substitutions() -> None:
    kd = _kd()
    ds = _dataset()
    subs = [e for e in ds.events if e.event_type == kd.EventType.SUBSTITUTION]
    if not subs:
        pytest.skip("fixture has no substitution")
    t, r, b = spadl._parse_event(subs[0], kd)
    assert spadl.ACTIONTYPES[t] == "non_action"


def test_end_location_of_a_pass_is_the_receiver_location() -> None:
    kd = _kd()
    ds = _dataset()
    p = next(e for e in ds.events if e.event_type == kd.EventType.PASS and e.receiver_coordinates)
    assert spadl._end_location(p, kd) == (p.receiver_coordinates.x, p.receiver_coordinates.y)


def test_end_location_falls_back_to_the_start() -> None:
    kd = _kd()
    ds = _dataset()
    e = next(
        e
        for e in ds.events
        if e.event_type not in (kd.EventType.PASS, kd.EventType.CARRY, kd.EventType.SHOT) and e.coordinates
    )
    assert spadl._end_location(e, kd) == (e.coordinates.x, e.coordinates.y)
