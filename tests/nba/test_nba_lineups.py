"""Tests for nba_lineups: boxscore name map + period starters (Task 4)."""

import json
import pathlib


FX = pathlib.Path("tests/fixtures/nba_engine/0022200001")


def _box() -> dict:
    return json.loads((FX / "boxscoretraditionalv3.json").read_text())


def _payload() -> dict:
    return json.loads((FX / "playbyplayv3.json").read_text())


def test_name_map_and_home_away() -> None:
    from sportsdataverse.nba.nba_lineups import boxscore_home_away, boxscore_name_map

    nm = boxscore_name_map(_box())
    home, away = boxscore_home_away(_box())
    assert home == 1610612738 and away == 1610612755
    assert all(isinstance(v, int) for team in nm.values() for v in team.values())


def test_period_starters_five_each() -> None:
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
    from sportsdataverse.nba.nba_lineups import period_starters

    s = period_starters(enhanced_pbp_from_payload(_payload()), _box())
    assert set(s.keys()) >= {1, 2, 3, 4}
    for teams in s.values():
        assert len(teams) == 2 and all(len(p) == 5 for p in teams.values())
