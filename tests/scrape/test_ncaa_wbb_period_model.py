"""WBB period-model era split (halves through 2015, quarters from 2016).

NCAA women's basketball moved from two 20-minute halves to four 10-minute
quarters for the 2015-16 season; men's basketball never switched. The parser
applied the quarters model to EVERY women's season until 2026-08-18, which does
not fail loudly -- it yields a frame with no rows. Every pre-2016 women's game
therefore parsed to an empty record while the parse stage reported success (it
counted files written, not rows extracted), and six seasons were published
~0.6% populated.

Measured on real captured bundles at the time of the fix:

    season  quarters  halves
    2016         550     262
    2015           0     520
    2012           0     566
    2010           0     527

The reverse degrades too, so the model is a real discriminator rather than a
fallback -- which is why this is an era SPLIT and not a "try one, then the
other".
"""

from __future__ import annotations

import pytest

from sportsdataverse.scrape.ncaa.parse import wbb_period_model

_HALVES = (2, 1200, 300)
_QUARTERS = (4, 600, 300)


@pytest.mark.parametrize("season", ["2010", "2011", "2012", "2013", "2014", "2015"])
def test_halves_era_seasons(season):
    assert wbb_period_model(season) == _HALVES


@pytest.mark.parametrize("season", ["2016", "2017", "2020", "2026"])
def test_quarters_era_seasons(season):
    assert wbb_period_model(season) == _QUARTERS


def test_boundary_is_2015_16():
    """2015-16 is the FIRST quarters season; 2014-15 the last halves season."""
    assert wbb_period_model("2015") == _HALVES
    assert wbb_period_model("2016") == _QUARTERS


@pytest.mark.parametrize(
    ("season", "expected"),
    [("2014-15", _HALVES), ("2015-16", _QUARTERS), ("2009-10", _HALVES)],
)
def test_accepts_hyphenated_season_form(season, expected):
    """Raw bundles carry `season` as an ending year OR a `YYYY-YY` string."""
    assert wbb_period_model(season) == expected


def test_unknown_season_falls_back_to_modern():
    """Absent/garbage season -> quarters: guessing modern is the smaller error."""
    assert wbb_period_model(None) == _QUARTERS
    assert wbb_period_model("not-a-season") == _QUARTERS
