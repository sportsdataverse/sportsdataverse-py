"""Tests for the pbp-derived on-court lineup producer + its boxscore helpers."""

from __future__ import annotations

import json
import pathlib


from sportsdataverse.nba.nba_lineups import _played, _starters_from_boxscore_v3

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

from sportsdataverse.nba.nba_lineups import _boxscore_name_map, _parse_sub_in_name


def test_boxscore_name_map_lists_collisions() -> None:
    """Collision handling is tested with a synthetic boxscore — never edits the real fixture."""

    def _player(pid: int, family: str, position: str = "F", minutes: str = "30:00") -> dict:
        return {
            "personId": pid,
            "familyName": family,
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


def test_boxscore_name_map_real_fixture_unique_name() -> None:
    """Sanity-check the map works on the real 0022200001 fixture via a uniquely-named player."""
    nm = _boxscore_name_map(_box("0022200001"))
    # Tatum is unique on the Celtics roster — should resolve to exactly one id
    assert nm[1610612738]["tatum"] == [1628369]


def test_parse_sub_in_name() -> None:
    assert _parse_sub_in_name("SUB: Vonleh FOR Horford") == "vonleh"
    assert _parse_sub_in_name("SUB: Williams FOR White") == "williams"
    assert _parse_sub_in_name("not a sub") is None
