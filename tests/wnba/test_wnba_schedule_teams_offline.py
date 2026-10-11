"""Offline `espn_wnba_schedule` / `espn_wnba_teams` checks on the real ESPN captures (B-M34).

The cases and their known-defect xfails live in :mod:`tests.espn_family_offline`, shared by the
8 hand-written families.
"""

import pytest

from tests.espn_family_offline import check_schedule, check_teams, schedule_cases, teams_cases


@pytest.mark.parametrize("case", schedule_cases("wnba"))
def test_wnba_schedule_offline(monkeypatch, case):
    check_schedule("wnba", case, monkeypatch)


@pytest.mark.parametrize("case", teams_cases("wnba"))
def test_wnba_teams_offline(monkeypatch, case):
    check_teams("wnba", case, monkeypatch)
