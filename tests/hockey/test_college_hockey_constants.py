import pytest

from sportsdataverse.hockey.college_hockey_constants import get_college_hockey_constants


def test_mch_resolves():
    c = get_college_hockey_constants("mch")
    assert c.league == "mch"
    assert c.espn_slug == "mens-college-hockey"


def test_wch_resolves():
    c = get_college_hockey_constants("wch")
    assert c.league == "wch"


def test_unknown_league_raises():
    with pytest.raises(ValueError, match="Unknown NCAA hockey league"):
        get_college_hockey_constants("nhl")


def test_capture_contract_locked_false():
    # Phase-0 finding: ESPN college-hockey pbp carries no shots/coordinates/shifts.
    # If a future recapture ships these, this test (and test_feasibility_locked_
    # to_goals_and_penalties_only below in test_college_hockey_ratings.py) breaks
    # and the xG/RAPM port becomes reachable.
    for league in ("mch", "wch"):
        c = get_college_hockey_constants(league)
        assert c.has_shot_coordinates is False
        assert c.has_shift_data is False
        assert c.has_full_pbp is False
