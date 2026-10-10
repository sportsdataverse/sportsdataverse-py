"""Season-aware three-point geometry (sdv-internal-refs rules/nba.yaml, rules/wnba.yaml)."""

import pytest

from sportsdataverse.nba.nba_shot_value_constants import COURT_ERAS, LEAGUE_COURT, get_court


def test_current_geometry_unchanged_without_season() -> None:
    for lg in LEAGUE_COURT:
        assert get_court(lg) == LEAGUE_COURT[lg]


def test_nba_shortened_arc_1995_to_1997() -> None:
    # nba-1995-three-point-line-shortened / nba-1998-three-point-line-restored (END-year seasons)
    assert get_court("00", season=1994).three_point_radius_ft == 23.75
    for s in (1995, 1996, 1997):
        assert get_court("00", season=s).three_point_radius_ft == 22.0
    assert get_court("00", season=1998) == LEAGUE_COURT["00"]
    assert get_court("00", season=1980) == LEAGUE_COURT["00"]
    assert get_court("00", season=1970) == LEAGUE_COURT["00"]  # before the first era -> first era


def test_wnba_three_arc_eras() -> None:
    # wnba-2004-three-point-line-20-6 / wnba-2013-three-point-line-fiba (calendar-year seasons)
    assert get_court("10", season=2003).three_point_radius_ft == 19.75
    assert get_court("10", season=2004).three_point_radius_ft == 20.52
    assert get_court("10", season=2012).three_point_radius_ft == 20.52
    assert get_court("10", season=2013) == LEAGUE_COURT["10"]


def test_g_league_always_nba_court() -> None:
    assert get_court("20", season=2005) == LEAGUE_COURT["20"]
    assert set(COURT_ERAS) == set(LEAGUE_COURT)


def test_unknown_league_raises() -> None:
    with pytest.raises(ValueError, match="unknown league_id"):
        get_court("99", season=2020)
