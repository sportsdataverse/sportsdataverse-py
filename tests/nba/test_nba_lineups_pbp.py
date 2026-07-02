"""Tests for the pbp-derived on-court lineup producer + its boxscore helpers."""

from __future__ import annotations

import json
import pathlib


from sportsdataverse.nba.nba_lineups import _starters_from_boxscore_v3

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
