"""SPADL conversion: vocabularies, mapping, post-processing and parity with the socceraction oracle."""

from __future__ import annotations

from pathlib import Path

import polars as pl

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
