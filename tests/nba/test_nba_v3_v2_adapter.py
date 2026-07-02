"""Tests for the v3 -> v2 pbp adapter lookup tables, roster builder, and name-match.

Task 1 of Phase A (pbpstats-adaptation program): pure lookup tables +
``_build_roster`` + ``_lookup_player`` (4-tier name resolution). No network,
no polars — these are the primitives the full ``nba_v3_to_v2_pbp`` assembly
(Task 3) builds on.

Task 2 adds the 1-to-1 secondary-player extraction (assist/block/steal/sub/
jump) tests, validated offline against the committed cdn feed truth.
"""

from __future__ import annotations

import json
import pathlib
from typing import Dict, List, Optional, Tuple

import pytest

from sportsdataverse.nba.nba_v3_v2_adapter import (
    _ACTION_TYPE_MAPS,
    _EVENT_TYPE_MAP,
    _build_roster,
    _extract_secondary_players,
    _lookup_player,
)

FXROOT = pathlib.Path("tests/fixtures/nba_engine")


def _box(game_id: str) -> dict:
    """Load a committed boxscoretraditionalv3 fixture."""
    return json.loads((FXROOT / game_id / "boxscoretraditionalv3.json").read_text())


def _v3_actions(game_id: str) -> List[dict]:
    """Load the committed ``playbyplayv3.json`` fixture's action list."""
    payload = json.loads((FXROOT / game_id / "playbyplayv3.json").read_text())
    actions: List[dict] = payload["game"]["actions"]
    return actions


def _cdn_actions(game_id: str) -> List[dict]:
    """Load the committed ``cdn_playbyplay.json`` fixture's action list.

    This is the offline structured-truth oracle (the richer live/cdn feed) --
    never fetched over the network in tests, only read from the committed
    fixture.
    """
    payload = json.loads((FXROOT / game_id / "cdn_playbyplay.json").read_text())
    actions: List[dict] = payload["game"]["actions"]
    return actions


def _cdn_truth(game_id: str) -> Dict[int, Dict[str, Optional[int]]]:
    """Build ``{actionNumber: {assist, block, steal}}`` from the cdn fixture."""
    truth: Dict[int, Dict[str, Optional[int]]] = {}
    for action in _cdn_actions(game_id):
        truth[action["actionNumber"]] = {
            "assist": action.get("assistPersonId") or None,
            "block": action.get("blockPersonId") or None,
            "steal": action.get("stealPersonId") or None,
        }
    return truth


def _match_rate(
    game_id: str,
) -> Dict[str, Tuple[int, int, int, int]]:
    """Compute (agree, total, missed, mismatched) per field for one fixture.

    Mirrors the proven ``desc_extract2.py`` scratchpad accounting: for every
    cdn action where the truth field is set, compare against the extracted
    value at the same ``actionNumber`` (assist -> player2_id on the field's
    own action, block -> player3_id, steal -> player2_id).
    """
    roster = _build_roster(_box(game_id))
    extracted = _extract_secondary_players(_v3_actions(game_id), roster)
    truth = _cdn_truth(game_id)

    field_to_key = {"assist": "player2_id", "block": "player3_id", "steal": "player2_id"}
    results: Dict[str, Tuple[int, int, int, int]] = {}
    for field, key in field_to_key.items():
        total = agree = missed = mismatched = 0
        for action_number, t in truth.items():
            expected = t.get(field)
            if not expected:
                continue
            total += 1
            got = extracted.get(action_number, {}).get(key)
            if got == expected:
                agree += 1
            elif got is None:
                missed += 1
            else:
                mismatched += 1
        results[field] = (agree, total, missed, mismatched)
    return results


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
    """Unmapped actionType strings are absent, so callers' .get(..., '0') falls back."""
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


def test_lookup_player_tier3_abbrev_isolated_from_name_i_and_fuzzy():
    """Isolate Tier 3 ("F. Family" abbreviation) from Tiers 2 and 4.

    ``test_lookup_player_abbrev_match`` above uses Evan Mobley, but his
    fixture ``name_i`` IS literally "E. Mobley" -- that lookup resolves at
    Tier 2 and never exercises Tier 3. This test uses a synthetic roster
    where ``name_i`` deliberately diverges from the plain "F. Family"
    abbreviation (a "Jr." suffix), so:

    - Tier 1 (exact family) misses: "O. Porter" != "Porter".
    - Tier 2 (exact name_i) misses: "O. Porter" != "O. Porter Jr.".
    - Tier 4 (fuzzy substring within full_name/family) also misses: the
      literal ". " in "O. Porter" never appears in "Otto Porter" or
      "Porter", so "o. porter" is not a substring of either.
    - Tier 3 (first-initial + ". " + family, built from "Otto" + "Porter")
      is therefore the *only* tier able to produce "o. porter" == "o. porter".
    """
    roster = {
        900: {
            "first": "Otto",
            "family": "Porter",
            "name_i": "O. Porter Jr.",
            "team_id": 3,
            "city": "City C",
            "nickname": "Team C",
            "tricode": "CCC",
            "full_name": "Otto Porter",
        },
    }
    # Sanity: confirm the fixture-divergence precondition the test relies on.
    assert roster[900]["name_i"] != "O. Porter"
    assert "o. porter" not in roster[900]["full_name"].lower()
    assert "o. porter" not in roster[900]["family"].lower()

    assert _lookup_player("O. Porter", roster) == 900


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


# ---------------------------------------------------------------------------
# Task 2: _extract_secondary_players -- 1-to-1 vs the cdn structured truth
# ---------------------------------------------------------------------------
# Per-fixture expected (agree, total) for each field, MEASURED against the
# committed cdn_playbyplay.json oracle (never fetched live in tests). Game
# 0022300001 is the previously-proven game (assist 55/55, block 14/14,
# steal 17/17, from the desc_extract2.py scratchpad). Games 0022100001 and
# 0022200001 were never validated before this task; both also measure a
# clean 100% match on all three fields -- no roster name-collision misses
# were observed on any of the 3 committed fixtures.
_EXPECTED_MATCH_RATES: Dict[str, Dict[str, Tuple[int, int]]] = {
    "0022100001": {"assist": (44, 44), "block": (18, 18), "steal": (11, 11)},
    "0022200001": {"assist": (40, 40), "block": (6, 6), "steal": (16, 16)},
    "0022300001": {"assist": (55, 55), "block": (14, 14), "steal": (17, 17)},
}


@pytest.mark.parametrize("game_id", sorted(_EXPECTED_MATCH_RATES))
def test_secondary_player_extraction_matches_cdn_truth(game_id: str) -> None:
    """1-to-1 vs the cdn feed: assist->player2_id, block->player3_id, steal->player2_id.

    For each fixture, every cdn action carrying a truth ``assistPersonId`` /
    ``blockPersonId`` / ``stealPersonId`` must have a matching extracted
    value at the same ``actionNumber``. The measured rate is asserted
    exactly (a documented floor below 100% would be acceptable per the
    design spec if a roster name-collision caused it, but none was found on
    these 3 fixtures).
    """
    rates = _match_rate(game_id)
    expected = _EXPECTED_MATCH_RATES[game_id]
    for field, (expected_agree, expected_total) in expected.items():
        agree, total, missed, mismatched = rates[field]
        assert (agree, total) == (expected_agree, expected_total), (
            f"{game_id} {field}: got {agree}/{total} "
            f"({missed} missed, {mismatched} mismatched), "
            f"expected {expected_agree}/{expected_total}"
        )


def test_secondary_player_extraction_game_0022300001_exact_counts() -> None:
    """The previously-proven game: assist 55/55, block 14/14, steal 17/17 EXACT."""
    rates = _match_rate("0022300001")
    assert rates["assist"] == (55, 55, 0, 0)
    assert rates["block"] == (14, 14, 0, 0)
    assert rates["steal"] == (17, 17, 0, 0)


def test_extract_secondary_players_jump_ball_sets_player2_and_player3() -> None:
    """Jump Ball at actionNumber 4 in 0022300001: 'Turner vs. Allen: Tip to Toppin'.

    player2_id = Allen (the "vs." jumper), player3_id = Toppin (tip-to
    recipient). Exact ids confirmed via the roster built from the same
    fixture's boxscore.
    """
    roster = _build_roster(_box("0022300001"))
    allen_id = _lookup_player("Allen", roster)
    toppin_id = _lookup_player("Toppin", roster)
    assert allen_id is not None
    assert toppin_id is not None

    extracted = _extract_secondary_players(_v3_actions("0022300001"), roster)
    assert extracted[4]["player2_id"] == allen_id
    assert extracted[4]["player3_id"] == toppin_id


def test_extract_secondary_players_sub_incoming_player() -> None:
    """A known 'SUB:' row sets player2_id to the INCOMING player's id.

    actionNumber 69 in 0022300001: 'SUB: Niang FOR Allen' -- personId on the
    row is Allen (outgoing = player1, handled in a later task), so player2_id
    must resolve to Niang, the incoming player.
    """
    roster = _build_roster(_box("0022300001"))
    niang_id = _lookup_player("Niang", roster)
    assert niang_id is not None

    extracted = _extract_secondary_players(_v3_actions("0022300001"), roster)
    assert extracted[69]["player2_id"] == niang_id


def test_extract_secondary_players_returns_none_for_actions_with_no_secondary() -> None:
    """actionNumbers with no recoverable secondary player are absent from the output."""
    roster = _build_roster(_box("0022300001"))
    extracted = _extract_secondary_players(_v3_actions("0022300001"), roster)
    # actionNumber 2 is the period-start action -- no assist/block/steal/sub/jump.
    assert 2 not in extracted
