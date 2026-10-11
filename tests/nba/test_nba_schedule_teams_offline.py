"""Offline `espn_nba_schedule` / `espn_nba_teams` checks on the real ESPN captures (B-M34).

The cases and their known-defect xfails live in :mod:`tests.espn_family_offline`, shared by the
8 hand-written families.
"""

import pytest

from tests.espn_family_offline import check_schedule, check_teams, schedule_cases, teams_cases


@pytest.mark.parametrize("case", schedule_cases("nba"))
def test_nba_schedule_offline(monkeypatch, case):
    check_schedule("nba", case, monkeypatch)


@pytest.mark.parametrize("case", teams_cases("nba"))
def test_nba_teams_offline(monkeypatch, case):
    check_teams("nba", case, monkeypatch)
