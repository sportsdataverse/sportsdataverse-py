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
