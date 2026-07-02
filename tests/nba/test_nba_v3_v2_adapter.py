"""Tests for the v3 -> v2 pbp adapter lookup tables, roster builder, and name-match.

Task 1 of Phase A (pbpstats-adaptation program): pure lookup tables +
``_build_roster`` + ``_lookup_player`` (4-tier name resolution). No network,
no polars — these are the primitives the full ``nba_v3_to_v2_pbp`` assembly
(Task 3) builds on.

Task 2 adds the 1-to-1 secondary-player extraction (assist/block/steal/sub/
jump) tests, validated offline against the committed cdn feed truth.

Task 3 adds the full ``nba_v3_to_v2_pbp`` assembly tests: the v2 output
schema, event/action-type codes, home/visitor/neutral description split,
score forward-fill, and the block/steal row-count drop.

Task 4 adds the pbpstats ``stats_nba`` feed shim tests: an always-on
structural test of ``_to_pbpstats_stats_nba_rows`` / ``_to_pbpstats_stats_nba_envelope``
(no pbpstats import required), plus a gated round-trip test that feeds the
shimmed envelope through a vendored local checkout of pbpstats and compares
possessions/starters against pbpstats' own ``live`` provider on the same
committed cdn fixture.
"""

from __future__ import annotations

import json
import pathlib
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.nba.nba_v3_v2_adapter import (
    _ACTION_TYPE_MAPS,
    _EVENT_TYPE_MAP,
    _build_roster,
    _extract_secondary_players,
    _is_dropped_block_steal,
    _lookup_player,
    _to_pbpstats_stats_nba_envelope,
    _to_pbpstats_stats_nba_rows,
    nba_v3_to_v2_pbp,
)

FXROOT = pathlib.Path("tests/fixtures/nba_engine")

# Vendored local pbpstats checkout used only by the Deliverable-2 gated
# round-trip test below. NOT a project dependency -- CI (and any contributor
# without this path) hits ImportError and the test skips cleanly.
_PBPSTATS_ROOT = "c:/Users/saiem/Documents/GitHub-Data/sdv-dev/pbpstats"


def _box(game_id: str) -> dict:
    """Load a committed boxscoretraditionalv3 fixture."""
    return json.loads((FXROOT / game_id / "boxscoretraditionalv3.json").read_text())


def _pbp(game_id: str) -> dict:
    """Load the full committed ``playbyplayv3.json`` fixture payload."""
    payload: dict = json.loads((FXROOT / game_id / "playbyplayv3.json").read_text())
    return payload


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


# ---------------------------------------------------------------------------
# Task 3: nba_v3_to_v2_pbp -- full v2-schema assembly
# ---------------------------------------------------------------------------

_V2_REPRESENTATIVE_COLUMNS = [
    "game_id",
    "event_num",
    "event_type",
    "event_action_type",
    "home_description",
    "visitor_description",
    "neutral_description",
    "score",
    "score_margin",
    "team_leading",
    "person1type",
    "player1_id",
    "player2_id",
    "player3_id",
    "time_quarter",
    "minute_game",
]


def test_v3_to_v2_pbp_has_representative_v2_schema_columns() -> None:
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    for column in _V2_REPRESENTATIVE_COLUMNS:
        assert column in df.columns, f"missing v2 column: {column}"


def test_v3_to_v2_pbp_event_type_values_are_within_mapped_code_set() -> None:
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    allowed = set(_EVENT_TYPE_MAP.values()) | {"0", "13"}
    observed = set(df["event_type"].unique().to_list())
    assert observed <= allowed


def test_v3_to_v2_pbp_known_assisted_made_shot_has_player2_id_set() -> None:
    """actionNumber 7 in 0022300001: 'Turner ... (Haliburton 1 AST)' -- a Made Shot with an assist."""
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    row = df.filter(pl.col("event_num") == "7")
    assert row.height == 1
    assert row["player2_id"][0] is not None


def test_v3_to_v2_pbp_home_and_visitor_descriptions_are_mutually_exclusive() -> None:
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    both_set = df.filter(pl.col("home_description").is_not_null() & pl.col("visitor_description").is_not_null())
    assert both_set.height == 0


def test_v3_to_v2_pbp_score_forward_fill_never_null_and_constant_between_scores() -> None:
    """Verified against the real 0022300001 output (not synthetic):

    actionNumber 7 (Turner cutting dunk, made shot) sets score "0 - 2" /
    margin "2". actionNumbers 9-12 (MISS Mobley, Turner REBOUND, MISS Brown,
    Brown REBOUND -- all non-scoring) must forward-fill that exact value with
    zero drift. actionNumber 13 (Mathurin 3PT) changes it to "0 - 5" / "5",
    and actionNumber 15 (Mitchell dunk) changes it again to "2 - 5" / "3".
    """
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    assert df["score"].null_count() == 0
    assert df["score_margin"].null_count() == 0

    # Run of consecutive non-scoring rows between two made shots: score and
    # score_margin must hold EXACTLY constant across the whole run -- this is
    # the forward-fill regression teeth (would catch e.g. an accidental
    # `.fill_null()` without `.forward_fill()`, or a period-boundary reset).
    non_scoring_run = df.filter((pl.col("event_num").cast(pl.Int64) >= 9) & (pl.col("event_num").cast(pl.Int64) <= 12))
    assert non_scoring_run.height == 4
    assert non_scoring_run["score"].to_list() == ["0 - 2", "0 - 2", "0 - 2", "0 - 2"]
    assert non_scoring_run["score_margin"].to_list() == ["2", "2", "2", "2"]

    # The value DOES change at the next real scoring events -- proves the
    # constancy above isn't just a frozen/stuck column.
    row_13 = df.filter(pl.col("event_num") == "13")
    assert row_13["score"].to_list() == ["0 - 5"]
    assert row_13["score_margin"].to_list() == ["5"]

    row_15 = df.filter(pl.col("event_num") == "15")
    assert row_15["score"].to_list() == ["2 - 5"]
    assert row_15["score_margin"].to_list() == ["3"]


def test_v3_to_v2_pbp_row_count_equals_v3_actions_minus_dropped_block_steal_rows() -> None:
    actions = _v3_actions("0022300001")
    dropped = sum(1 for action in actions if _is_dropped_block_steal(action))
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    assert df.height == len(actions) - dropped


def test_v3_to_v2_pbp_ids_are_utf8_strings() -> None:
    """Binding id-dtype rule (CLAUDE.md): every join-key id column is Utf8,
    and casting never strips a leading zero off the game id.
    """
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    assert df.schema["game_id"] == pl.Utf8
    assert df.schema["event_num"] == pl.Utf8
    assert df.schema["player1_id"] == pl.Utf8
    assert df.schema["player2_id"] == pl.Utf8
    assert df.schema["player3_id"] == pl.Utf8
    assert df.schema["player1_team_id"] == pl.Utf8
    assert df.schema["player2_team_id"] == pl.Utf8
    assert df.schema["player3_team_id"] == pl.Utf8

    # Leading zero preserved -- proves no int/float round-trip stripped it.
    assert df["game_id"][0] == "0022300001"


def test_v3_to_v2_pbp_return_as_pandas() -> None:
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    df_pd = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"), return_as_pandas=True)
    assert isinstance(df_pd, pd.DataFrame)
    assert len(df_pd) == df.height


def test_v3_to_v2_pbp_empty_actions_returns_zero_row_frame_with_schema() -> None:
    empty_pbp = {"game": {"gameId": "0000000000", "actions": []}}
    df = nba_v3_to_v2_pbp(empty_pbp, {})
    assert df.height == 0
    for column in _V2_REPRESENTATIVE_COLUMNS:
        assert column in df.columns


def test_v3_to_v2_pbp_malformed_input_never_raises() -> None:
    df = nba_v3_to_v2_pbp({}, {})
    assert df.height == 0
    assert isinstance(df, pl.DataFrame)


# ---------------------------------------------------------------------------
# Task 4: pbpstats stats_nba feed shim -- always-on structural tests
# ---------------------------------------------------------------------------
# These never import pbpstats -- they assert the shim's own contract (the
# UPPERCASE keys pbpstats' StatsEnhancedPbpItem reads are present, the id/type
# columns it needs are Python int, PCTIMESTRING is "MM:SS" and matches the v2
# frame's time_quarter, and the resultSets envelope shape is well-formed).
# They MUST run in CI.

_PBPSTATS_REQUIRED_KEYS = [
    "GAME_ID",
    "EVENTNUM",
    "EVENTMSGTYPE",
    "EVENTMSGACTIONTYPE",
    "PERIOD",
    "PCTIMESTRING",
    "PLAYER1_ID",
    "PLAYER2_ID",
    "PLAYER3_ID",
]

_PBPSTATS_INT_KEYS = [
    "EVENTMSGTYPE",
    "EVENTMSGACTIONTYPE",
    "EVENTNUM",
    "PERIOD",
    "PLAYER1_ID",
    "PLAYER2_ID",
    "PLAYER3_ID",
]


def test_to_pbpstats_stats_nba_rows_has_uppercase_keys() -> None:
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    rows = _to_pbpstats_stats_nba_rows(df)
    assert len(rows) == df.height
    for row in rows:
        for key in _PBPSTATS_REQUIRED_KEYS:
            assert key in row, f"missing pbpstats key: {key}"


def test_to_pbpstats_stats_nba_rows_key_ids_and_types_are_python_int() -> None:
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    rows = _to_pbpstats_stats_nba_rows(df)
    for row in rows:
        for key in _PBPSTATS_INT_KEYS:
            value = row[key]
            assert isinstance(value, int) and not isinstance(value, bool), f"{key} not int: {value!r}"


def test_to_pbpstats_stats_nba_rows_known_assisted_made_shot_player2_id_is_correct_int() -> None:
    """actionNumber 7 in 0022300001: known assisted made shot (see Task 3 test above)."""
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    row7 = df.filter(pl.col("event_num") == "7")
    expected_player2_id = int(row7["player2_id"][0])

    rows = _to_pbpstats_stats_nba_rows(df)
    target = next(row for row in rows if row["EVENTNUM"] == 7)
    assert target["PLAYER2_ID"] == expected_player2_id


def test_to_pbpstats_stats_nba_rows_pctimestring_format_and_matches_time_quarter() -> None:
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    time_quarter_by_event_num: Dict[str, str] = dict(zip(df["event_num"].to_list(), df["time_quarter"].to_list()))

    rows = _to_pbpstats_stats_nba_rows(df)
    for row in rows:
        pctimestring = row["PCTIMESTRING"]
        assert re.match(r"^\d{2}:\d{2}$", pctimestring), f"bad PCTIMESTRING format: {pctimestring!r}"
        assert pctimestring == time_quarter_by_event_num[str(row["EVENTNUM"])]


def test_to_pbpstats_stats_nba_rows_null_player_ids_default_to_zero() -> None:
    """A row with no player2/player3 (no assist/block/steal/sub/jump) casts null id -> 0.

    actionNumber 2 in 0022300001 is the period-start action -- no
    assist/block/steal/sub/jump (see the Task 2 test of the same fixture row
    above), so its v2 player2_id/player3_id are null and must cast to 0
    (pbpstats' own "no player" convention).
    """
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    rows = _to_pbpstats_stats_nba_rows(df)
    row2 = next(row for row in rows if row["EVENTNUM"] == 2)
    assert row2["PLAYER2_ID"] == 0
    assert row2["PLAYER3_ID"] == 0


def test_to_pbpstats_stats_nba_envelope_shape() -> None:
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    envelope = _to_pbpstats_stats_nba_envelope(df, "0022300001")

    result_set = envelope["resultSets"][0]
    assert result_set["name"] == "PlayByPlay"
    headers: List[str] = result_set["headers"]
    row_set: List[List[Any]] = result_set["rowSet"]

    assert len(row_set) == df.height
    for row in row_set:
        assert len(row) == len(headers)

    game_id_index = headers.index("GAME_ID")
    assert all(row[game_id_index] == "0022300001" for row in row_set)


def test_to_pbpstats_stats_nba_envelope_game_id_override() -> None:
    """The envelope's game_id param overrides whatever the frame itself carries."""
    df = nba_v3_to_v2_pbp(_pbp("0022300001"), _box("0022300001"))
    envelope = _to_pbpstats_stats_nba_envelope(df, "9999999999")
    result_set = envelope["resultSets"][0]
    headers = result_set["headers"]
    game_id_index = headers.index("GAME_ID")
    assert all(row[game_id_index] == "9999999999" for row in result_set["rowSet"])


# ---------------------------------------------------------------------------
# Task 4: pbpstats round-trip -- gated, best-effort "ultimate 1-to-1"
# ---------------------------------------------------------------------------
# Feeds the shimmed envelope through a vendored local pbpstats checkout's
# stats_nba file-mode loader and compares possessions/period-starters against
# pbpstats' own live provider fed from the committed cdn fixture. Entirely
# offline (committed fixtures only, no network) -- NOT gated by
# SDV_PY_LIVE_TESTS. Skips (rather than fails) when pbpstats can't be
# imported in this environment (it is vendored, not a project dependency) or
# when a required cdn fixture is missing.
#
# Measured possession counts (stats_nba-fed, live-fed) per game, pinned as a
# regression guard on the committed fixtures. The residual documented in the
# design spec ("foul-drawn player" gap -- fouls carry null player2_id/
# player3_id in the v3-derived frame, an accepted limitation) plus the
# v2-vs-live provider difference keeps the delta small but not always zero;
# Phase 0's own harness measured a +2..+5 possession/game ceiling for the
# related v3-engine-vs-pbpstats-live comparison, used here as the documented
# tolerance bound.
_ROUNDTRIP_EXPECTED_POSSESSION_COUNTS: Dict[str, Tuple[int, int]] = {
    "0022100001": (204, 206),
    "0022200001": (193, 195),
    "0022300001": (205, 205),
}
_ROUNDTRIP_MAX_ABS_POSSESSION_DELTA = 5


def _pbpstats_client_or_skip() -> Any:
    """Import pbpstats' ``Client`` from the vendored checkout, or skip the test."""
    if _PBPSTATS_ROOT not in sys.path:
        sys.path.insert(0, _PBPSTATS_ROOT)
    try:
        from pbpstats.client import Client
    except ImportError as exc:
        pytest.skip(f"pbpstats not importable in this env (vendored, not a dep): {exc}")
    return Client


def _write_stats_nba_inputs(game_dir: pathlib.Path, game_id: str) -> None:
    """Write the pbpstats stats_nba file-mode inputs for one game.

    ``StatsNbaPbpFileLoader`` reads ``{dir}/pbp/stats_{game_id}.json`` (our
    shimmed envelope). ``StatsNbaEnhancedPbpLoader._add_shot_x_y_coords``
    additionally, unconditionally loads a shot-chart file pair
    (``{dir}/game_details/stats_{home,away}_shots_{game_id}.json``) to attach
    x/y coords to made/missed-shot events -- unused by the possession/starter
    comparison below, but required to construct the pbpstats ``Game`` object
    at all. Synthesized here from the v2 frame's own x_legacy/y_legacy
    passthrough columns; this plumbing is test-harness-only, not part of the
    ``_to_pbpstats_stats_nba_*`` shim contract.
    """
    (game_dir / "pbp").mkdir(parents=True, exist_ok=True)
    (game_dir / "game_details").mkdir(parents=True, exist_ok=True)

    v2_df = nba_v3_to_v2_pbp(_pbp(game_id), _box(game_id))
    envelope = _to_pbpstats_stats_nba_envelope(v2_df, game_id)
    (game_dir / "pbp" / f"stats_{game_id}.json").write_text(json.dumps(envelope))

    shots = v2_df.filter(pl.col("event_type").is_in(["1", "2"]))
    shot_headers = ["GAME_EVENT_ID", "LOC_X", "LOC_Y"]
    shot_rows = [
        [int(event_num), int(loc_x or 0), int(loc_y or 0)]
        for event_num, loc_x, loc_y in zip(
            shots["event_num"].to_list(),
            shots["x_legacy"].to_list(),
            shots["y_legacy"].to_list(),
        )
    ]
    home_shots = {"resultSets": [{"headers": shot_headers, "rowSet": shot_rows}]}
    away_shots = {"resultSets": [{"headers": shot_headers, "rowSet": []}]}
    (game_dir / "game_details" / f"stats_home_shots_{game_id}.json").write_text(json.dumps(home_shots))
    (game_dir / "game_details" / f"stats_away_shots_{game_id}.json").write_text(json.dumps(away_shots))


def _write_live_inputs(game_dir: pathlib.Path, game_id: str) -> None:
    """Write the committed cdn fixtures into pbpstats' live file-mode paths."""
    (game_dir / "pbp").mkdir(parents=True, exist_ok=True)
    (game_dir / "game_details").mkdir(parents=True, exist_ok=True)
    pbp_text = (FXROOT / game_id / "cdn_playbyplay.json").read_text()
    box_text = (FXROOT / game_id / "cdn_boxscore.json").read_text()
    (game_dir / "pbp" / f"live_{game_id}.json").write_text(pbp_text)
    (game_dir / "game_details" / f"live_{game_id}.json").write_text(box_text)


def _period_starters(possessions: List[Any]) -> Dict[int, Dict[int, set]]:
    """``{period: {team_id: {player_id, ...}}}`` from a list of pbpstats Possessions."""
    from pbpstats.resources.enhanced_pbp import StartOfPeriod

    starters: Dict[int, Dict[int, set]] = {}
    for possession in possessions:
        for event in possession.events:
            if isinstance(event, StartOfPeriod):
                starters[event.period] = {team_id: set(players) for team_id, players in event.period_starters.items()}
    return starters


@pytest.mark.parametrize("game_id", sorted(_ROUNDTRIP_EXPECTED_POSSESSION_COUNTS))
def test_pbpstats_stats_nba_roundtrip_possessions_and_starters(tmp_path: pathlib.Path, game_id: str) -> None:
    """The 1-to-1: pbpstats-stats_nba (fed by our adapter) vs pbpstats-live, same cdn fixture."""
    Client = _pbpstats_client_or_skip()

    cdn_pbp = FXROOT / game_id / "cdn_playbyplay.json"
    cdn_box = FXROOT / game_id / "cdn_boxscore.json"
    if not cdn_pbp.exists() or not cdn_box.exists():
        pytest.skip(f"cdn fixture missing for {game_id}")

    game_dir = tmp_path / game_id
    _write_stats_nba_inputs(game_dir, game_id)
    _write_live_inputs(game_dir, game_id)

    stats_client = Client(
        {
            "dir": str(game_dir),
            "Possessions": {"source": "file", "data_provider": "stats_nba"},
        }
    )
    stats_possessions = stats_client.Game(game_id).possessions.items

    live_client = Client(
        {
            "dir": str(game_dir),
            "Boxscore": {"source": "file", "data_provider": "live"},
            "Possessions": {"source": "file", "data_provider": "live"},
        }
    )
    live_possessions = live_client.Game(game_id).possessions.items

    expected_stats_count, expected_live_count = _ROUNDTRIP_EXPECTED_POSSESSION_COUNTS[game_id]
    assert len(stats_possessions) == expected_stats_count, (
        f"{game_id}: pbpstats-stats_nba possession count drifted from the "
        f"pinned measurement ({len(stats_possessions)} != {expected_stats_count})"
    )
    assert len(live_possessions) == expected_live_count, (
        f"{game_id}: pbpstats-live possession count drifted from the pinned "
        f"measurement ({len(live_possessions)} != {expected_live_count})"
    )

    delta = len(stats_possessions) - len(live_possessions)
    assert abs(delta) <= _ROUNDTRIP_MAX_ABS_POSSESSION_DELTA, (
        f"{game_id}: possession count delta {delta} exceeds the documented "
        f"tolerance of {_ROUNDTRIP_MAX_ABS_POSSESSION_DELTA} (foul-drawn-player "
        "gap + v2-vs-live provider difference)"
    )

    stats_starters = _period_starters(stats_possessions)
    live_starters = _period_starters(live_possessions)
    assert set(stats_starters) == set(live_starters), f"{game_id}: period set mismatch"
    for period, teams in stats_starters.items():
        assert teams == live_starters[period], f"{game_id} period {period}: starters mismatch"
