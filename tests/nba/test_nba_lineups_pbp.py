"""Tests for the pbp-derived on-court lineup producer + its boxscore helpers."""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    _boxscore_name_map,
    _parse_sub_in_name,
    _played,
    _starters_from_boxscore_v3,
    boxscore_home_away,
    parse_rotation_resultsets,
    players_on_court_from_pbp,
    players_on_court_from_rotation,
)
from sportsdataverse.nba.nba_pbp_constants import LINEUPS_SCHEMA

FXROOT = pathlib.Path("tests/fixtures/nba_engine")


def _box(g: str) -> dict:
    return json.loads((FXROOT / g / "boxscoretraditionalv3.json").read_text())


def test_starters_from_boxscore_v3_five_per_team() -> None:
    starters = _starters_from_boxscore_v3(_box("0022200001"))
    # homeTeam 1610612738 starters, awayTeam 1610612755 starters (verified from fixture)
    assert set(starters[1610612738]) == {1627759, 1628369, 201143, 1628401, 203935}
    assert set(starters[1610612755]) == {202699, 200782, 203954, 1630178, 201935}
    assert all(len(v) == 5 for v in starters.values())


def test_starters_from_boxscore_v3_empty_never_raises() -> None:
    assert _starters_from_boxscore_v3({}) == {}


def test_starters_pad_prefers_played_over_dnp() -> None:
    """Pad pool must include only players with recorded playing time.

    Constructs a synthetic one-team boxscore with exactly 4 positional
    starters plus two bench players — one who played 12 minutes and one DNP.
    Asserts the function fills the 5th slot with the played bench player and
    never includes the DNP player.
    """

    def _player(pid: int, position: str, minutes: str) -> dict:
        return {
            "personId": pid,
            "position": position,
            "statistics": {"minutes": minutes, "points": 0},
        }

    # 4 starters (non-empty position) + 1 played bench + 1 DNP
    home_players = [
        _player(1, "G", "34:00"),
        _player(2, "G", "30:00"),
        _player(3, "F", "28:00"),
        _player(4, "F", "25:00"),
        # no 5th starter — position is empty for both bench players
        _player(5, "", "12:00"),  # played bench — should be padded in
        _player(6, "", ""),  # DNP — must NOT be padded in
    ]
    raw_box = {
        "boxScoreTraditional": {
            "homeTeam": {"teamId": 9999, "players": home_players},
            "awayTeam": {},
        }
    }
    result = _starters_from_boxscore_v3(raw_box)
    assert 9999 in result
    ids = result[9999]
    assert len(ids) == 5
    assert 5 in ids, "played bench player (pid=5) should be padded into starter-5"
    assert 6 not in ids, "DNP player (pid=6) must not appear in starter-5"


# ---------------------------------------------------------------------------
# _played helper unit tests
# ---------------------------------------------------------------------------


def test_played_real_minutes() -> None:
    assert _played({"minutes": "34:12"}) is True


def test_played_short_minutes() -> None:
    assert _played({"minutes": "3:19"}) is True


def test_played_dnp_empty_string() -> None:
    assert _played({"minutes": ""}) is False


def test_played_dnp_none() -> None:
    assert _played({"minutes": None}) is False


def test_played_zero_duration() -> None:
    assert _played({"minutes": "0:00"}) is False


def test_played_empty_stats() -> None:
    assert _played({}) is False


# ---------------------------------------------------------------------------
# Task 2: _boxscore_name_map + _parse_sub_in_name
# ---------------------------------------------------------------------------


def test_boxscore_name_map_lists_collisions() -> None:
    """Collision handling is tested with a synthetic boxscore — never edits the real fixture."""

    def _player(pid: int, family: str, position: str = "F", minutes: str = "30:00", first: str = "") -> dict:
        return {
            "personId": pid,
            "familyName": family,
            "firstName": first,
            "position": position,
            "statistics": {"minutes": minutes, "points": 0},
        }

    synthetic = {
        "boxScoreTraditional": {
            "homeTeam": {
                "teamId": 8888,
                "players": [
                    _player(111, "Williams"),
                    _player(222, "Williams"),
                    _player(333, "Tatum"),
                ],
            },
            "awayTeam": {"teamId": 7777, "players": []},
        }
    }
    nm = _boxscore_name_map(synthetic)
    team = nm[8888]
    # both Williams ids appear under the "williams" key
    assert 111 in team["williams"]
    assert 222 in team["williams"]
    assert len(team["williams"]) >= 2
    # a uniquely-named player maps to exactly one id
    assert team["tatum"] == [333]


def test_boxscore_name_map_first_initial_disambiguates() -> None:
    """First-initial keys resolve same-family teammates to distinct ids."""

    def _player(pid: int, family: str, position: str = "F", minutes: str = "30:00", first: str = "") -> dict:
        return {
            "personId": pid,
            "familyName": family,
            "firstName": first,
            "position": position,
            "statistics": {"minutes": minutes, "points": 0},
        }

    team_id = 5555
    synthetic = {
        "boxScoreTraditional": {
            "homeTeam": {
                "teamId": team_id,
                "players": [
                    _player(111, "Antetokounmpo", first="Giannis"),
                    _player(222, "Antetokounmpo", first="Thanasis"),
                    _player(333, "Holiday", first="Jrue"),
                ],
            },
            "awayTeam": {"teamId": 4444, "players": []},
        }
    }
    nm = _boxscore_name_map(synthetic)
    team = nm[team_id]
    # first-initial keys resolve to exactly one id each
    assert team["g. antetokounmpo"] == [111]
    assert team["t. antetokounmpo"] == [222]
    # bare family key still contains BOTH ids (for backward-compat collision lookup)
    assert 111 in team["antetokounmpo"]
    assert 222 in team["antetokounmpo"]


def test_boxscore_name_map_real_fixture_unique_name() -> None:
    """Sanity-check the map works on the real 0022200001 fixture via a uniquely-named player."""
    nm = _boxscore_name_map(_box("0022200001"))
    # Tatum is unique on the Celtics roster — should resolve to exactly one id
    assert nm[1610612738]["tatum"] == [1628369]


def test_parse_sub_in_name() -> None:
    assert _parse_sub_in_name("SUB: Vonleh FOR Horford") == "vonleh"
    assert _parse_sub_in_name("SUB: Williams FOR White") == "williams"
    assert _parse_sub_in_name("not a sub") is None


# ---------------------------------------------------------------------------
# Task 3: players_on_court_from_pbp
# ---------------------------------------------------------------------------


def _pbp(g: str) -> dict:
    return json.loads((FXROOT / g / "playbyplayv3.json").read_text())


@pytest.mark.parametrize("game_id", ["0022100001", "0022200001", "0022300001"])
def test_pbp_producer_schema_and_coverage(game_id: str) -> None:
    home, away = boxscore_home_away(_box(game_id))
    enh = enhanced_pbp_from_payload(_pbp(game_id))
    oc = players_on_court_from_pbp(enh, _box(game_id), home_team_id=home, away_team_id=away)
    assert oc.schema == LINEUPS_SCHEMA
    assert oc.height == enh.height  # one row per action
    # every row has exactly 5 home + 5 away non-null (fully covered)
    home_cols = [f"home_player_{i}" for i in range(1, 6)]
    away_cols = [f"away_player_{i}" for i in range(1, 6)]
    nulls = oc.select(pl.sum_horizontal([pl.col(c).is_null() for c in home_cols + away_cols]).alias("n"))
    assert int(nulls["n"].sum()) == 0


def test_pbp_producer_seeds_starters_first_action() -> None:
    enh = enhanced_pbp_from_payload(_pbp("0022200001"))
    oc = players_on_court_from_pbp(enh, _box("0022200001"), home_team_id=1610612738, away_team_id=1610612755).sort(
        "action_number"
    )
    first = oc.row(0, named=True)
    home5 = {first[f"home_player_{i}"] for i in range(1, 6)}
    assert home5 == {1627759, 1628369, 201143, 1628401, 203935}  # Celtics starters


def test_pbp_producer_applies_first_sub() -> None:
    # action 67: SUB Vonleh(203943) FOR Horford(201143) on Celtics
    enh = enhanced_pbp_from_payload(_pbp("0022200001"))
    oc = players_on_court_from_pbp(enh, _box("0022200001"), home_team_id=1610612738, away_team_id=1610612755).sort(
        "action_number"
    )
    # find the row for the action immediately AFTER 67
    after = oc.filter(pl.col("action_number") > 67).sort("action_number").row(0, named=True)
    home5 = {after[f"home_player_{i}"] for i in range(1, 6)}
    assert 201143 not in home5  # Horford subbed out
    assert 203943 in home5  # Vonleh subbed in


# ---------------------------------------------------------------------------
# Task 4: cross-source meta-oracle (pbp vs gamerotation agreement)
# ---------------------------------------------------------------------------


def _rot(g: str) -> dict:
    return json.loads((FXROOT / g / "gamerotation.json").read_text())


def _oncourt10(frame: pl.DataFrame) -> dict[int, frozenset[int]]:
    """action_number -> frozenset of the 10 on-court ids."""
    cols = [f"home_player_{i}" for i in range(1, 6)] + [f"away_player_{i}" for i in range(1, 6)]
    out: dict[int, frozenset[int]] = {}
    for r in frame.select(["action_number", *cols]).to_dicts():
        out[r["action_number"]] = frozenset(v for c in cols if (v := r[c]) is not None)
    return out


@pytest.mark.parametrize("game_id", ["0022100001", "0022200001", "0022300001"])
def test_pbp_agrees_with_rotation(game_id: str) -> None:
    enh = enhanced_pbp_from_payload(_pbp(game_id))
    home, away = boxscore_home_away(_box(game_id))
    oc_rot = players_on_court_from_rotation(
        enh, parse_rotation_resultsets(_rot(game_id)), home_team_id=home, away_team_id=away
    )
    oc_pbp = players_on_court_from_pbp(enh, _box(game_id), home_team_id=home, away_team_id=away)
    a, b = _oncourt10(oc_rot), _oncourt10(oc_pbp)
    shared = [k for k in a if k in b and len(a[k]) == 10]
    agree = sum(1 for k in shared if a[k] == b[k])
    rate = agree / len(shared) if shared else 0.0
    print(f"\ngame {game_id}: pbp/rotation agreement {rate:.4f} ({agree}/{len(shared)} actions)")
    # Report + assert the parity floor. If a fixture legitimately drops below,
    # print `rate` and set the floor to the measured value with a comment.
    assert rate >= 0.95, f"game {game_id}: pbp/rotation agreement {rate:.3f} < 0.95"
