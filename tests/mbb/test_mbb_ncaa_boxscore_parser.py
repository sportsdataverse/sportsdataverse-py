"""Oracle tests for :mod:`sportsdataverse.mbb.mbb_ncaa_boxscore_parser` (Task 5e.2).

1:1 transliteration of ``BoxscoreParserTests.scala`` (``utest``,
``"BoxscoreParser"`` block): ``get_lineup`` (fixture-based, loads
``tests/fixtures/ncaa/test_lineup.html``, vendored in Task 5e.1),
``parse_final_score`` (inline), and ``validate_box_score`` (inline).

``get_lineup``'s oracle asserts the resulting ``lineup`` against
``{literal_names}.map(build_player_code(_, None)).sortBy(_.code)`` -- as
documented in ``mbb_ncaa_boxscore_parser.py``'s module docstring, the actual
``get_box_lineup`` pipeline never sorts, and ``test_lineup.html``'s player
names happen to already be in ascending-code order, so the tests below
compare directly against the NATURAL (unsorted) ``build_player_code`` list,
matching what the port actually computes.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup, parse_final_score, validate_box_score
from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError
from sportsdataverse.mbb.mbb_ncaa_models import LineupEvent, LocationType, Score, TeamId, TeamSeasonId, Year
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code
from types import SimpleNamespace
from sportsdataverse.mbb.mbb_ncaa_names import code_from_box

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa"

_LINEUP_HTML = (FIXTURES / "test_lineup.html").read_text(encoding="utf-8")

_TEAM_A_NAMES = [f"S{n}rname, F{n}rstname TeamA" for n in range(1, 10)]
_TEAM_B_NAMES = [f"S{n}rname, F{n}rstname TeamB" for n in range(1, 10)] + ["SArname, FArstname TeamB"]

_PERIODS = [(1, 0.0), (2, 20.0), (3, 40.0), (4, 45.0)]


class TestGetBoxLineup:
    """``BoxscoreParserTests.scala:32-174`` (``"get_lineup"`` block)."""

    @pytest.mark.parametrize(("period", "mins"), _PERIODS)
    def test_home_team_is_away_location(self, period: int, mins: float) -> None:
        result = get_box_lineup(f"test_p{period}.html", _LINEUP_HTML, TeamId("TeamA"), format_version=0)
        assert isinstance(result, LineupEvent), result
        assert result.location_type is LocationType.AWAY
        assert result.start_min == mins
        assert result.end_min == mins
        assert result.duration_mins == 0.0
        assert result.team == TeamSeasonId(TeamId("TeamA"), Year(2018))
        assert result.opponent == TeamSeasonId(TeamId("TeamB"), Year(2018))
        assert result.players_in == []
        assert result.players_out == []
        assert result.raw_game_events == []
        assert result.date == datetime(2018, 12, 10, 17, 0)
        assert result.players == [build_player_code(name, None) for name in _TEAM_A_NAMES]
        assert result.score_info.start == Score(0, 0)
        assert result.score_info.end == Score(92, 91)
        assert result.score_info.start_diff == 0
        assert result.score_info.end_diff == 0

    @pytest.mark.parametrize(("period", "mins"), _PERIODS)
    def test_away_team_is_home_location(self, period: int, mins: float) -> None:
        result = get_box_lineup(
            f"test_p{period}.html",
            _LINEUP_HTML,
            TeamId("TeamB"),
            format_version=0,
            external_roster=([], []),
            neutral_game_dates=frozenset({"date_mismatch"}),
        )
        assert isinstance(result, LineupEvent), result
        assert result.location_type is LocationType.HOME
        assert result.start_min == mins
        assert result.end_min == mins
        assert result.team == TeamSeasonId(TeamId("TeamB"), Year(2018))
        assert result.opponent == TeamSeasonId(TeamId("TeamA"), Year(2018))
        assert result.date == datetime(2018, 12, 10, 17, 0)
        assert result.players == [build_player_code(name, None) for name in _TEAM_B_NAMES]
        assert result.score_info.end == Score(91, 92)

    @pytest.mark.parametrize("period", [1, 2, 3, 4])
    @pytest.mark.parametrize("adjusted", [False, True])
    def test_neutral_site_game(self, period: int, adjusted: bool) -> None:
        # Oracle's "adjusted_lineup_html" variant appends " 17:00" onto the
        # date cell -- neutral-site detection matches on the date string's
        # FIRST whitespace token, so both variants resolve identically.
        html = _LINEUP_HTML.replace("12/10/2018", "12/10/2018 17:00") if adjusted else _LINEUP_HTML
        result = get_box_lineup(
            f"test_p{period}.html",
            html,
            TeamId("TeamA"),
            format_version=0,
            external_roster=([], []),
            neutral_game_dates=frozenset({"12/10/2018"}),
        )
        assert isinstance(result, LineupEvent), result
        assert result.location_type is LocationType.NEUTRAL


class TestParseFinalScore:
    """``BoxscoreParserTests.scala:176-211`` (``"parse_final_score"`` block)."""

    def test_empty_list_is_an_error(self) -> None:
        assert isinstance(parse_final_score([], target_team_first=True), ParseError)

    def test_single_value_is_an_error(self) -> None:
        assert isinstance(parse_final_score(["1"], target_team_first=True), ParseError)

    def test_two_values_target_first(self) -> None:
        assert parse_final_score(["1", "2"], target_team_first=True) == Score(1, 2)

    def test_three_values_odd_count_is_an_error(self) -> None:
        assert isinstance(parse_final_score(["1", "2", "3"], target_team_first=True), ParseError)

    def test_four_values_target_second(self) -> None:
        assert parse_final_score(["1", "2", "3", "4"], target_team_first=False) == Score(4, 2)

    def test_non_integer_score_is_an_error(self) -> None:
        assert isinstance(parse_final_score(["1", "rabbit", "3", "4"], target_team_first=True), ParseError)

    def test_six_values_target_first(self) -> None:
        assert parse_final_score(["1", "2", "3", "4", "5", "6"], target_team_first=True) == Score(3, 6)


class TestValidateBoxScore:
    """``BoxscoreParserTests.scala:212-221`` (``"validate_box_score"`` block)."""

    def test_no_duplicates_is_ok(self) -> None:
        result = validate_box_score(TeamId("Team"), ["Player One", "Player Two"])
        assert isinstance(result, list)
        assert len(result) == 2

    def test_colliding_codes_are_disambiguated_not_rejected(self) -> None:
        # "Pete One" and "Peter One" both truncate their first-name fragment
        # to "Pe" (build_player_code caps the first-name transform at 2 chars)
        # and share the last-name fragment "One".
        #
        # DELIBERATE DIVERGENCE FROM THE SCALA ORACLE. Upstream
        # ExtractorUtils.scala treats this as bad_lineup and rejects the game;
        # this test asserted that behaviour. Rejecting is catastrophic rather
        # than cautious, because every game a team plays carries the same
        # roster -- so one colliding pair deleted the team's ENTIRE season from
        # `lineups`, `matchup_stints` and `possessions`. Measured across
        # 2010-2026, that cost 79 D-I team-seasons; Kansas published 0 of 36
        # games in 2010 because it rostered the Morris twins.
        #
        # Only the colliding players are re-coded, so every other player keeps
        # the ported code and `lineup_key`s churn only for affected teams. A
        # genuinely identical full name still raises -- see
        # tests/mbb/test_mbb_ncaa_sibling_codes.py.
        result = validate_box_score(TeamId("Team"), ["Pete One", "Peter One"])
        assert not isinstance(result, ParseError), result
        assert len({c.code for c in result}) == 2, result


_TEAM = TeamId("Kansas")


def _codes(names):
    out = validate_box_score(_TEAM, names)
    assert not isinstance(out, ParseError), out
    return [c.code for c in out]


class TestSiblingDisambiguation:
    def test_morris_twins_no_longer_fail_the_game(self):
        """Kansas 2010: Markieff and Marcus Morris both coded MaMorris."""
        codes = _codes(["Morris, Markieff", "Morris, Marcus", "Collins, Sherron"])
        assert len(set(codes)) == 3, codes
        assert "MarkieffMorris" in codes and "MarcusMorris" in codes

    def test_non_colliding_players_keep_the_ported_code(self):
        """Only the collision is re-coded -- lineup_keys must not churn."""
        codes = _codes(["Morris, Markieff", "Morris, Marcus", "Collins, Sherron"])
        assert "ShCollins" in codes, codes

    def test_a_roster_with_no_collision_is_untouched(self):
        before = _codes(["Collins, Sherron", "Aldrich, Cole"])
        assert before == ["ShCollins", "CoAldrich"]

    def test_womens_twins_case(self):
        """San Diego 2015 rostered Sophie AND Sophia Ederaine."""
        codes = _codes(["Ederaine, Sophie", "Ederaine, Sophia"])
        assert len(set(codes)) == 2, codes

    def test_three_way_collision(self):
        codes = _codes(["Wolf, Madison", "Wolf, Mackenzie", "Wolf, Marcus"])
        assert len(set(codes)) == 3, codes

    def test_identical_full_names_still_raise(self):
        """A true ambiguity is NOT a coding artefact -- it must still fail."""
        out = validate_box_score(_TEAM, ["Smith, John", "Smith, John"])
        # build_sub_error returns a bare ParseError, not a list of them.
        assert isinstance(out, ParseError), type(out)

    def test_space_separated_names_disambiguate_too(self):
        codes = _codes(["Markieff Morris", "Marcus Morris"])
        assert len(set(codes)) == 2, codes


class TestCodeFromBox:
    """The PBP-side half of the sibling fix.

    Widening the code in ``validate_box_score`` is only half a fix: every
    PBP-side path used to RE-DERIVE a code from the tidied name with
    ``build_player_code``, which collapses both twins back to ``MaMorris``.
    One of them then matched the roster and the other vanished -- Kansas 2010
    parsed 110 healthy-looking events with ``MarcusMorris`` present 18 times
    and ``MarkieffMorris`` present ZERO times. Counting events did not catch
    it; only asserting the specific code did.
    """

    @staticmethod
    def _box(names):
        return SimpleNamespace(players=validate_box_score(_TEAM, names))

    def test_returns_the_widened_roster_code_not_a_rederived_one(self):
        box = self._box(["Morris, Markieff", "Morris, Marcus", "Aldrich, Cole"])
        for name, want in (("Morris, Markieff", "MarkieffMorris"), ("Morris, Marcus", "MarcusMorris")):
            assert code_from_box(name, box, _TEAM).code == want
            # The bug this test exists for: the derived code is the SAME for both.
            assert build_player_code(name, _TEAM).code == "MaMorris"

    def test_non_colliding_player_is_unchanged(self):
        box = self._box(["Morris, Markieff", "Morris, Marcus", "Aldrich, Cole"])
        assert code_from_box("Aldrich, Cole", box, _TEAM).code == build_player_code("Aldrich, Cole", _TEAM).code

    def test_name_absent_from_the_roster_falls_back_to_a_derived_code(self):
        box = self._box(["Aldrich, Cole"])
        assert code_from_box("Nobody, Ann", box, _TEAM).code == build_player_code("Nobody, Ann", _TEAM).code
