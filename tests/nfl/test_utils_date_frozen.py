"""Frozen-clock oracle for the NFL date helpers (C-M14, C-M22; ledger gate frozen-clock-table).

The oracle is nflreadr's `R/utils_date.R` (nflverse-dev/nflreadr @ 23f915a5, 2026-09-17), which the
owner ruled is the rule to port (nflreadpy's copy is not the oracle):

* `most_recent_season()` (:20-39): the season flips on the Wednesday after Labor Day (`labor_day + 2`,
  changed in March 2026 for the Wednesday kickoff); roster years flip on March 15.
* `get_current_week(use_date = TRUE)` (:94-118): week 1 starts on the Thursday after Labor Day of the
  current season (`monday1_sep + 3`); week = days since then %/% 7 + 1, clamped to [1, 22].

Labor Day per season is a literal table (checked against a calendar), not computed by the code under
test. `NFLVERSE_WEEK1_OPENER` is the earliest week-1 REG `gameday` per season in nflverse's schedule file
(nflverse/nfldata `data/games.csv` @ 0fa7bc18, 2026-10-11): Labor Day + 3 for 2015-2025, Labor Day + 2 for
the Wednesday 2026 opener. `date.today()` is frozen inside `sportsdataverse.nfl.utils_date`; no network.

Task 3.3 ported the rule (C-M22, EX-1.5-a): before it, the code flipped the season on the Thursday and
anchored week 1 on the first Thursday of September (one week high in 2015-16, 2020-22, 2026-27), and
`nfl_schedule.most_recent_nfl_season` was a second definition flipping on September 1 (B-L12).
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from sportsdataverse.nfl import nfl_schedule, utils_date

LABOR_DAY = {
    2014: date(2014, 9, 1),
    2015: date(2015, 9, 7),
    2016: date(2016, 9, 5),
    2017: date(2017, 9, 4),
    2018: date(2018, 9, 3),
    2019: date(2019, 9, 2),
    2020: date(2020, 9, 7),
    2021: date(2021, 9, 6),
    2022: date(2022, 9, 5),
    2023: date(2023, 9, 4),
    2024: date(2024, 9, 2),
    2025: date(2025, 9, 1),
    2026: date(2026, 9, 7),
    2027: date(2027, 9, 6),
    2028: date(2028, 9, 4),
}

SEASONS = list(range(2015, 2028))

NFLVERSE_WEEK1_OPENER = {
    2015: date(2015, 9, 10),
    2016: date(2016, 9, 8),
    2017: date(2017, 9, 7),
    2018: date(2018, 9, 6),
    2019: date(2019, 9, 5),
    2020: date(2020, 9, 10),
    2021: date(2021, 9, 9),
    2022: date(2022, 9, 8),
    2023: date(2023, 9, 7),
    2024: date(2024, 9, 5),
    2025: date(2025, 9, 4),
    2026: date(2026, 9, 9),
}


def _oracle_season(today: date, roster: bool = False) -> int:
    """nflreadr `most_recent_season()` (R/utils_date.R:20-39)."""
    if roster:
        return today.year if (today.month, today.day) >= (3, 15) else today.year - 1
    return today.year if today >= LABOR_DAY[today.year] + timedelta(days=2) else today.year - 1


def _oracle_week(today: date) -> int:
    """nflreadr `get_current_week(use_date = TRUE)` (R/utils_date.R:94-118)."""
    first_game = LABOR_DAY[_oracle_season(today)] + timedelta(days=3)
    return min(max((today - first_game).days // 7 + 1, 1), 22)


@pytest.fixture
def freeze(monkeypatch):
    """Freeze `date.today()` as seen by `sportsdataverse.nfl.utils_date`."""

    def _freeze(day: date) -> None:
        class _Frozen(date):
            @classmethod
            def today(cls):
                return day

        monkeypatch.setattr(utils_date, "date", _Frozen)

    return _freeze


def _season_dates(season: int) -> list[date]:
    """Every day from Labor Day - 7 through the next Labor Day - 1."""
    start = LABOR_DAY[season] - timedelta(days=7)
    return [start + timedelta(days=i) for i in range((LABOR_DAY[season + 1] - start).days)]


def _mismatches(freeze, days, fn, oracle):
    bad = []
    for d in days:
        freeze(d)
        got, want = fn(), oracle(d)
        if got != want:
            bad.append((d.isoformat(), got, want))
    return bad


@pytest.mark.parametrize("season", SEASONS)
def test_current_season_matches_nflreadr(freeze, season):
    bad = _mismatches(freeze, _season_dates(season), utils_date.get_current_nfl_season, _oracle_season)
    assert not bad, f"season {season}: {len(bad)} days differ, first {bad[:3]}"
    print("CURRENT_SEASON_OK", season)


@pytest.mark.parametrize("season", SEASONS)
def test_roster_year_matches_nflreadr(freeze, season):
    days = [date(season, 3, 14), date(season, 3, 15), date(season, 1, 1), date(season, 12, 31)]
    bad = _mismatches(
        freeze,
        days,
        lambda: utils_date.get_current_nfl_season(roster=True),
        lambda d: _oracle_season(d, roster=True),
    )
    assert not bad, f"roster {season}: {bad}"


def test_current_season_on_the_wednesday_after_labor_day(freeze):
    """EX-1.5-a: nflreadr flips the season on the Wednesday (labor_day + 2) since March 2026."""
    days = [LABOR_DAY[s] + timedelta(days=2) for s in SEASONS]
    bad = _mismatches(freeze, days, utils_date.get_current_nfl_season, _oracle_season)
    assert not bad, f"{len(bad)} of {len(days)} Wednesdays differ: {bad[:3]}"


@pytest.mark.parametrize("season", SEASONS)
def test_current_week_matches_nflreadr(freeze, season):
    bad = _mismatches(
        freeze,
        _season_dates(season),
        lambda: utils_date.get_current_nfl_week(use_date=True),
        _oracle_week,
    )
    assert not bad, f"season {season}: {len(bad)} days differ, first {bad[:3]}"


@pytest.mark.parametrize("season", sorted(NFLVERSE_WEEK1_OPENER))
def test_real_opener_is_week_one_of_its_season(freeze, season):
    """On the real opener (nflverse schedules) and the six days after, it is week 1 of that season."""
    for d in [NFLVERSE_WEEK1_OPENER[season] + timedelta(days=i) for i in range(7)]:
        freeze(d)
        assert (utils_date.get_current_nfl_season(), utils_date.get_current_nfl_week()) == (season, 1), d


def test_one_definition_of_the_rule(freeze):
    """B-L12: the nfl_schedule names are the utils_date rule, not a second one."""
    assert nfl_schedule.most_recent_nfl_season is utils_date.most_recent_nfl_season
    for d in [date(2026, 9, 1), date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 17), date(2027, 1, 5)]:
        freeze(d)
        assert nfl_schedule.most_recent_nfl_season() == _oracle_season(d), d
        assert nfl_schedule.get_current_week() == _oracle_week(d), d
