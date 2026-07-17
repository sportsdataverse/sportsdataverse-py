"""Regression: archival payloads with `boxscoreAvailable=false` but real stats.

ESPN's header flag is unreliable for pre-2014 WBB games -- ~30% of 2012 games
carry full team statistics while the flag says false. The original R helpers
(and the faithful port) gated on the flag and silently dropped them, which is
why the compiled team_box for 2009-2013 held 10-280 rows against ~5,400-game
seasons. The gate is now payload-derived; these tests pin that on a real
archival payload (the 2012 title game, trimmed).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sportsdataverse.wbb.wbb_player_box import helper_wbb_player_box
from sportsdataverse.wbb.wbb_team_box import helper_wbb_team_box

_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "wbb" / "final_320940239_archival_flag_false.json"


@pytest.fixture()
def archival_final() -> dict:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


def test_fixture_is_the_flag_false_case(archival_final):
    """Guard: the fixture must stay the case the bug ate -- flag false, stats present."""
    comp = archival_final["header"]["competitions"][0]
    assert comp.get("boxscoreAvailable") is False
    assert archival_final["boxscore"]["teams"][0]["statistics"]


def test_team_box_extracts_despite_false_flag(archival_final):
    df = helper_wbb_team_box(archival_final)
    assert df.height == 2
    assert df["team_id"].n_unique() == 2
    assert df["field_goals_attempted"].null_count() == 0


def test_player_box_extracts_despite_false_flag(archival_final):
    df = helper_wbb_player_box(archival_final)
    assert df.height > 0


def test_genuinely_boxless_payload_still_returns_empty(archival_final):
    """The payload checks remain the gate: no teams/players -> typed empty."""
    boxless = {
        "header": archival_final["header"],
        "boxscore": {"teams": [], "players": []},
    }
    assert helper_wbb_team_box(boxless).height == 0
    assert helper_wbb_player_box(boxless).height == 0
