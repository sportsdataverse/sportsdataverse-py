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
            (19.99, 1, 0.6),  # just before the horn
            (20.0, 1, 0.0),  # ON the horn -- the period that ENDED
            (20.01, 2, 1199.4),  # start of the 2nd
            (25.0, 2, 900.0),
            (39.5, 2, 30.0),
            (40.0, 2, 0.0),  # end of regulation, NOT overtime
        ],
    )
    def test_regulation(self, minute: float, period: int, sec_left: float) -> None:
        assert period_and_sec_left(minute, league="mbb", season=2024) == (period, sec_left)

    @pytest.mark.parametrize("minute,period", [(40.01, 3), (44.9, 3), (45.0, 3), (45.01, 4), (50.0, 4)])
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
        """Both eras play 40 minutes; only the subdivision changed.

        40.0 is the END of regulation, so it belongs to the LAST regulation
        period at 0 seconds -- half 2 pre-2016, quarter 4 after.
        """
        assert period_and_sec_left(40.0, league="wbb", season=2014) == (2, 0.0)
        assert period_and_sec_left(40.0, league="wbb", season=2024) == (4, 0.0)
        assert period_and_sec_left(40.01, league="wbb", season=2024)[0] == 5


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


class TestPeriodBoundaries:
    """A shot ON the horn belongs to the period that ENDED, at 0 seconds.

    The naive floor-division put 20.0 in period 2 with a full 1200s clock, so
    every buzzer-beater was attributed to the following period as though it had
    just started. Caught in review; these pin it.
    """

    @pytest.mark.parametrize(
        "minute,expected",
        [
            (20.0, (1, 0.0)),  # end of the men's 1st half
            (40.0, (2, 0.0)),  # end of men's regulation, NOT overtime
            (45.0, (3, 0.0)),  # end of OT1, NOT OT2
        ],
    )
    def test_mens_boundaries_close_the_period(self, minute, expected) -> None:
        assert period_and_sec_left(minute, league="mbb", season=2024) == expected

    @pytest.mark.parametrize("minute,expected", [(10.0, (1, 0.0)), (30.0, (3, 0.0))])
    def test_womens_quarter_boundaries(self, minute, expected) -> None:
        assert period_and_sec_left(minute, league="wbb", season=2024) == expected

    def test_a_hair_past_the_boundary_opens_the_next_period(self) -> None:
        assert period_and_sec_left(20.01, league="mbb", season=2024) == (2, 1199.4)


class TestNonFiniteClock:
    """NaN passed the ``< 0`` guard and then crashed the floor division."""

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_non_finite_returns_none_rather_than_raising(self, bad: float) -> None:
        assert period_and_sec_left(bad, league="mbb", season=2024) == (None, None)


class TestDisplayNameToRosterKey:
    """Box/shot pages render "Surname, First"; rosters render FIRST.MIDDLE.LAST.

    One canonical direction, previously duplicated in both -data repos'
    ops/publish_rapm.py. Each normalization below earned its place against real
    2024 MBB data: 93.04% -> 98.07% (suffix/nickname) -> 99.08% (whitespace as
    dots).
    """

    @pytest.mark.parametrize(
        "display,key",
        [
            ("Clark, Garry", "GARRY.CLARK"),
            ("Wrightsell Jr., Latrell", "LATRELL.WRIGHTSELL"),  # suffix glued to surname
            ('"TJ" Madlock, Antonio', "ANTONIO.MADLOCK"),  # quoted nickname
            ("Ballisager Webb, Jermaine", "JERMAINE.BALLISAGER.WEBB"),  # dots, not concat
            ("De Luna, Kendrick", "KENDRICK.DE.LUNA"),
            ("Wright-Forde, Dian", "DIAN.WRIGHTFORDE"),  # hyphen collapses
            ("Washington, Jr., Teddy", "TEDDY.WASHINGTON"),  # suffix as its own field
        ],
    )
    def test_known_renderings(self, display: str, key: str) -> None:
        from sportsdataverse.mbb.mbb_ncaa_names import display_name_to_roster_key

        assert display_name_to_roster_key(display) == key

    @pytest.mark.parametrize("bad", [None, "", "Cher", ","])
    def test_unsplittable_names_yield_an_empty_key(self, bad: object) -> None:
        """An empty key never matches -- unresolved beats a wrong join."""
        from sportsdataverse.mbb.mbb_ncaa_names import display_name_to_roster_key

        assert display_name_to_roster_key(bad) == ""  # type: ignore[arg-type]

    def test_multi_token_surname_is_not_concatenated(self) -> None:
        """The subtle one: rosters keep INTERIOR dots as token separators."""
        from sportsdataverse.mbb.mbb_ncaa_names import display_name_to_roster_key

        got = display_name_to_roster_key("Tchamwa Tchatchoua, Jonathan")
        assert got == "JONATHAN.TCHAMWA.TCHATCHOUA"
        assert got != "JONATHAN.TCHAMWATCHATCHOUA"
