"""``period`` / ``sec_left`` derivation for NCAA shot events.

Both columns were hardcoded ``None`` on the NCAA path, so every published
``ncaa_{mbb,wbb}_shots`` season shipped them entirely null (~2.8M rows per
league). They are derivable from ``ShotEvent.min``, the ascending elapsed
game-clock minute.
"""

from __future__ import annotations

import pytest

from sportsdataverse.mbb.mbb_shots_adapter import period_and_sec_left


class TestMensHalves:
    """MBB has always played two 20-minute halves."""

    @pytest.mark.parametrize(
        "minute,period,sec_left",
        [
            (0.0, 1, 1200.0),  # tip
            (19.99, 1, 0.6),  # end of the 1st half
            (20.0, 2, 1200.0),  # start of the 2nd
            (25.0, 2, 900.0),
            (39.5, 2, 30.0),
        ],
    )
    def test_regulation(self, minute: float, period: int, sec_left: float) -> None:
        assert period_and_sec_left(minute, league="mbb", season=2024) == (period, sec_left)

    @pytest.mark.parametrize("minute,period", [(40.0, 3), (44.9, 3), (45.0, 4), (50.0, 5)])
    def test_overtimes_are_five_minutes(self, minute: float, period: int) -> None:
        assert period_and_sec_left(minute, league="mbb", season=2024)[0] == period


class TestWomensEraSplit:
    """WBB is HALVES before season 2016 and quarters from 2016.

    This is the trap that silently emptied six WBB seasons during the first
    publish. ``start_time_from_period`` takes a BOOLEAN ``is_women_game`` and
    unconditionally assumes quarters, so deriving through it would label a
    2014 first half as "quarter 1" and put ``sec_left`` on a 10-minute clock
    that actually ran for 20.
    """

    def test_quarters_from_2016(self) -> None:
        assert period_and_sec_left(25.0, league="wbb", season=2024) == (3, 300.0)
        assert period_and_sec_left(0.0, league="wbb", season=2016) == (1, 600.0)

    def test_halves_before_2016(self) -> None:
        assert period_and_sec_left(25.0, league="wbb", season=2014) == (2, 900.0)
        assert period_and_sec_left(0.0, league="wbb", season=2010) == (1, 1200.0)

    def test_the_same_minute_differs_across_the_era_boundary(self) -> None:
        """The regression this guards: one minute, two correct answers."""
        halves = period_and_sec_left(25.0, league="wbb", season=2015)
        quarters = period_and_sec_left(25.0, league="wbb", season=2016)
        assert halves == (2, 900.0)
        assert quarters == (3, 300.0)
        assert halves != quarters

    def test_womens_regulation_still_ends_at_40_minutes(self) -> None:
        """Both eras play 40 minutes; only the subdivision changed."""
        for season in (2014, 2024):
            assert period_and_sec_left(40.0, league="wbb", season=season)[0] > (2 if season < 2016 else 4)


class TestBadClockStaysUnresolved:
    """A missing or nonsense clock must NOT become a confident wrong period."""

    @pytest.mark.parametrize("bad", [None, -1.0, "abc"])
    def test_returns_none(self, bad: object) -> None:
        assert period_and_sec_left(bad, league="mbb", season=2024) == (None, None)  # type: ignore[arg-type]


class TestLeagueVocabulary:
    """The adapter says "mens"/"womens"; the release layer says "mbb"/"wbb".

    Matching only one vocabulary is a SILENT failure: a women's game passed as
    "womens" would fall through to the men's halves schedule and every quarter
    would be mislabelled, with no error anywhere. Caught exactly that way.
    """

    @pytest.mark.parametrize("womens", ["wbb", "womens", "Womens", "W"])
    def test_all_womens_spellings_get_the_womens_schedule(self, womens: str) -> None:
        assert period_and_sec_left(25.0, league=womens, season=2024) == (3, 300.0)

    @pytest.mark.parametrize("mens", ["mbb", "mens", "Mens"])
    def test_all_mens_spellings_get_the_mens_schedule(self, mens: str) -> None:
        assert period_and_sec_left(25.0, league=mens, season=2024) == (2, 900.0)

    def test_the_two_leagues_disagree_at_the_same_minute(self) -> None:
        assert period_and_sec_left(25.0, league="womens", season=2024) != period_and_sec_left(
            25.0, league="mens", season=2024
        )
