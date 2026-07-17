from __future__ import annotations


def test_year_to_season_takes_start_year_unchanged() -> None:
    from sportsdataverse.nba import year_to_season

    # year_to_season is a low-level START-year helper; callers pass end_year - 1.
    assert year_to_season(2023) == "2023-24"
    assert year_to_season(1996) == "1996-97"
    assert year_to_season(1999) == "1999-00"  # century rollover
