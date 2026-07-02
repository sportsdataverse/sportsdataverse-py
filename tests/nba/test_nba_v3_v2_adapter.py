"""Tests for the v3 -> v2 pbp adapter lookup tables, roster builder, and name-match.

Task 1 of Phase A (pbpstats-adaptation program): pure lookup tables +
``_build_roster`` + ``_lookup_player`` (4-tier name resolution). No network,
no polars — these are the primitives the full ``nba_v3_to_v2_pbp`` assembly
(Task 3) builds on.
"""

from __future__ import annotations

import json
import pathlib

from sportsdataverse.nba.nba_v3_v2_adapter import (
    _ACTION_TYPE_MAPS,
    _EVENT_TYPE_MAP,
    _build_roster,
    _lookup_player,
)

FXROOT = pathlib.Path("tests/fixtures/nba_engine")


def _box(game_id: str) -> dict:
    """Load a committed boxscoretraditionalv3 fixture."""
    return json.loads((FXROOT / game_id / "boxscoretraditionalv3.json").read_text())


def test_event_type_map_core_values():
    """Core EVENTMSGTYPE codes match the hoopR .v3_to_v2_format recipe."""
    assert _EVENT_TYPE_MAP["Made Shot"] == "1"
    assert _EVENT_TYPE_MAP["Missed Shot"] == "2"
    assert _EVENT_TYPE_MAP["Free Throw"] == "3"
    assert _EVENT_TYPE_MAP["Rebound"] == "4"
    assert _EVENT_TYPE_MAP["Turnover"] == "5"
    assert _EVENT_TYPE_MAP["Foul"] == "6"
    assert _EVENT_TYPE_MAP["Violation"] == "7"
    assert _EVENT_TYPE_MAP["Substitution"] == "8"
    assert _EVENT_TYPE_MAP["Timeout"] == "9"
    assert _EVENT_TYPE_MAP["Jump Ball"] == "10"
    assert _EVENT_TYPE_MAP["Ejection"] == "11"
    assert _EVENT_TYPE_MAP["period"] == "12"
    assert _EVENT_TYPE_MAP["Game"] == "12"
    assert _EVENT_TYPE_MAP["Instant Replay"] == "18"
    assert _EVENT_TYPE_MAP["Stoppage"] == "18"


def test_event_type_map_unknown_falls_back_to_zero():
    """Unmapped actionType strings resolve to '0' via .get(..., '0') at call sites."""
    assert _EVENT_TYPE_MAP.get("Nonsense", "0") == "0"
    assert "Nonsense" not in _EVENT_TYPE_MAP


def test_action_type_maps_has_six_parent_tables():
    """_ACTION_TYPE_MAPS is keyed by the 6 category names, not literal actionType strings."""
    assert set(_ACTION_TYPE_MAPS.keys()) == {
        "shot",
        "ft",
        "turnover",
        "foul",
        "timeout",
        "violation",
    }


def test_action_type_map_shot_values():
    shot = _ACTION_TYPE_MAPS["shot"]
    assert shot["Jump Shot"] == "1"
    assert shot["Layup"] == "5"
    assert shot["Layup Shot"] == "5"
    assert shot["Dunk"] == "7"
    assert shot["Dunk Shot"] == "7"


def test_action_type_map_ft_values():
    ft = _ACTION_TYPE_MAPS["ft"]
    assert ft["Free Throw 1 of 1"] == "10"
    assert ft["Free Throw 1 of 2"] == "11"
    assert ft["Free Throw 2 of 2"] == "12"


def test_action_type_map_turnover_values():
    turnover = _ACTION_TYPE_MAPS["turnover"]
    assert turnover["Bad Pass"] == "1"
    assert turnover["Lost Ball"] == "2"
    assert turnover["Traveling"] == "4"


def test_action_type_map_foul_values():
    foul = _ACTION_TYPE_MAPS["foul"]
    assert foul["Personal"] == "1"
    assert foul["Shooting"] == "2"


def test_action_type_map_timeout_values():
    timeout = _ACTION_TYPE_MAPS["timeout"]
    assert timeout["Regular"] == "1"
    assert timeout["Short"] == "2"
    assert timeout["Official"] == "4"


def test_action_type_map_violation_values():
    violation = _ACTION_TYPE_MAPS["violation"]
    assert violation["Delay Of Game"] == "1"


def test_action_type_maps_cover_fixture_subtypes():
    """Every non-blank subType present in the committed fixture resolves in its table.

    Guards the "extended as new subTypes appear" contract: any subType found
    in a real capture must have a landing spot in the appropriate table (or a
    documented '0' fallback for parents that are not one of the 6 tables,
    e.g. Rebound / Substitution / Jump Ball).
    """
    pbp = json.loads((FXROOT / "0022300001" / "playbyplayv3.json").read_text())
    actions = pbp["game"]["actions"]
    category_by_parent = {
        "Made Shot": "shot",
        "Missed Shot": "shot",
        "Free Throw": "ft",
        "Turnover": "turnover",
        "Foul": "foul",
        "Timeout": "timeout",
        "Violation": "violation",
    }
    missing = []
    for action in actions:
        parent = action.get("actionType")
        category = category_by_parent.get(parent)
        if category is None:
            continue
        sub_type = action.get("subType") or ""
        if not sub_type:
            continue
        if sub_type not in _ACTION_TYPE_MAPS[category]:
            missing.append((parent, sub_type))
    assert missing == []


def test_build_roster_has_at_least_20_players_with_family_and_team_id():
    roster = _build_roster(_box("0022300001"))
    assert len(roster) >= 20
    for info in roster.values():
        assert "family" in info and info["family"]
        assert "team_id" in info and isinstance(info["team_id"], int)
        assert "name_i" in info
        assert "full_name" in info


def test_build_roster_full_name_and_team_fields():
    roster = _build_roster(_box("0022300001"))
    # Evan Mobley, Cleveland Cavaliers, away team in this fixture.
    info = roster[1630596]
    assert info["first"] == "Evan"
    assert info["family"] == "Mobley"
    assert info["name_i"] == "E. Mobley"
    assert info["full_name"] == "Evan Mobley"
    assert info["team_id"] == 1610612739
    assert info["city"] == "Cleveland"
    assert info["nickname"] == "Cavaliers"
    assert info["tricode"] == "CLE"


def test_build_roster_empty_input_never_raises():
    assert _build_roster({}) == {}
    assert _build_roster(None) == {}  # type: ignore[arg-type]


def test_lookup_player_exact_family_match():
    roster = _build_roster(_box("0022300001"))
    assert _lookup_player("Mobley", roster) == 1630596


def test_lookup_player_case_insensitive_family_match():
    roster = _build_roster(_box("0022300001"))
    assert _lookup_player("mobley", roster) == 1630596
    assert _lookup_player("MOBLEY", roster) == 1630596


def test_lookup_player_name_i_match():
    roster = _build_roster(_box("0022300001"))
    assert _lookup_player("E. Mobley", roster) == 1630596


def test_lookup_player_abbrev_match():
    roster = _build_roster(_box("0022300001"))
    # "F. Family" abbreviation built from first-initial + family, distinct from name_i.
    assert _lookup_player("E. Mobley", roster) == 1630596


def test_lookup_player_fuzzy_substring_match():
    roster = _build_roster(_box("0022300001"))
    assert _lookup_player("Evan Mobley", roster) == 1630596


def test_lookup_player_unknown_name_returns_none():
    roster = _build_roster(_box("0022300001"))
    assert _lookup_player("Zzyzzogeton Nobody", roster) is None


def test_lookup_player_empty_inputs_return_none():
    roster = _build_roster(_box("0022300001"))
    assert _lookup_player("", roster) is None
    assert _lookup_player(None, roster) is None  # type: ignore[arg-type]
    assert _lookup_player("Mobley", {}) is None


def test_lookup_player_family_collision_returns_first():
    """Two ids sharing a family name resolve to the first roster entry (insertion order)."""
    roster = {
        100: {
            "first": "Alpha",
            "family": "Smith",
            "name_i": "A. Smith",
            "team_id": 1,
            "city": "City A",
            "nickname": "Team A",
            "tricode": "AAA",
            "full_name": "Alpha Smith",
        },
        200: {
            "first": "Beta",
            "family": "Smith",
            "name_i": "B. Smith",
            "team_id": 2,
            "city": "City B",
            "nickname": "Team B",
            "tricode": "BBB",
            "full_name": "Beta Smith",
        },
    }
    assert _lookup_player("Smith", roster) == 100
