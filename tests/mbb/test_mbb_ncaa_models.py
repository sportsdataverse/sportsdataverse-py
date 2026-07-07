"""Oracle-replay tests for ``sportsdataverse.mbb.mbb_ncaa_models`` (Task 5a.1).

Every ``PossCalcFragment`` / ``score_to_tuple`` assertion transliterates an
inline literal from ``cbb-explorer``'s ``PossessionUtilsTests.scala``
(``utest``, ``"PossCalcFragment"`` block at ``:181-189``); the
``RawGameEvent`` factory + accessor cases replay the ``Events`` fixture
object's construction calls (``:20-79``) plus the field-level ``def``s
documented in ``LineupEvent.scala:65-105``. No vendored fixture file is
needed here -- every oracle value is a short inline literal, same as the
upstream ``utest`` block.
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_models import (
    Direction,
    LineupEventStats,
    LineupId,
    PlayerId,
    PossCalcFragment,
    PossessionEvent,
    RawGameEvent,
    Score,
    ScoreInfo,
    TeamId,
    TeamSeasonId,
    Year,
    poss_calc_fragment_sum,
    score_to_tuple,
)


class TestPossCalcFragment:
    """``PossessionUtilsTests.scala:181-189`` (utest ``"PossCalcFragment"``)."""

    def test_total_poss_oracle(self) -> None:
        frag1 = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
        assert frag1.total_poss == 2

    def test_sum_oracle(self) -> None:
        frag1 = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
        frag2 = PossCalcFragment(1, 3, 5, 7, 9, 11, 13, 15)
        frag_sum = PossCalcFragment(2, 5, 8, 11, 14, 17, 20, 23)
        assert poss_calc_fragment_sum(frag1, frag2) == frag_sum

    def test_summary_oracle(self) -> None:
        frag1 = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
        expected = (
            "total=[2] = shots=[1] - (orbs=[2] + db_orbs=[3]) + "
            "(ft_sets=[4] - techs=[6]) + to=[8] { +1s=[5] offset_techs=[7] }"
        )
        assert frag1.summary == expected

    def test_empty_fragment_total_poss_is_zero(self) -> None:
        assert PossCalcFragment().total_poss == 0

    def test_sum_is_commutative_and_pure(self) -> None:
        frag1 = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
        frag2 = PossCalcFragment(1, 3, 5, 7, 9, 11, 13, 15)
        assert poss_calc_fragment_sum(frag1, frag2) == poss_calc_fragment_sum(frag2, frag1)
        # inputs untouched
        assert frag1 == PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)


class TestScoreToTuple:
    """``ExtractorUtils.scala:107-113`` (full-match, not find/search)."""

    def test_parses_score(self) -> None:
        assert score_to_tuple("55-68") == (55, 68)

    def test_defaults_to_zero_zero_on_no_match(self) -> None:
        assert score_to_tuple("garbage") == (0, 0)

    def test_defaults_to_zero_zero_on_empty_string(self) -> None:
        assert score_to_tuple("") == (0, 0)

    def test_requires_full_match_not_partial(self) -> None:
        # Scala's `.r` unapply requires the ENTIRE string to match -- a
        # trailing/leading non-digit character must fail, not just extract
        # the numeric substring (re.search would wrongly succeed here).
        assert score_to_tuple("prefix 10-20") == (0, 0)
        assert score_to_tuple("10-20 suffix") == (0, 0)


class TestRawGameEvent:
    """Transliterates the ``Events`` fixture's construction pattern
    (``PossessionUtilsTests.scala:20-79``) + the ``def`` accessors
    (``LineupEvent.scala:73-105``)."""

    def test_for_team_factory_and_accessors(self) -> None:
        ev = RawGameEvent.for_team(s="19:58:00,0-0,Bruno Fernando, jumpball won", min=0.0)
        assert ev.team == "19:58:00,0-0,Bruno Fernando, jumpball won"
        assert ev.opponent is None
        assert ev.info == "19:58:00,0-0,Bruno Fernando, jumpball won"
        assert ev.date_str == "19:58:00"
        assert ev.score_str == "0-0"
        assert ev.show_dir == ">"

    def test_for_opponent_factory_and_accessors(self) -> None:
        # PossessionUtilsTests.scala:21 builds the opponent twin off the
        # team event's own raw string: `.opponent(min=0.0, s=jump_won_team.team.get)`
        team_ev = RawGameEvent.for_team(s="19:58:00,0-0,Bruno Fernando, jumpball won", min=0.0)
        assert team_ev.team is not None
        opp_ev = RawGameEvent.for_opponent(s=team_ev.team, min=0.0)
        assert opp_ev.team is None
        assert opp_ev.opponent == "19:58:00,0-0,Bruno Fernando, jumpball won"
        assert opp_ev.info == team_ev.team
        assert opp_ev.date_str == "19:58:00"
        assert opp_ev.score_str == "0-0"
        assert opp_ev.show_dir == "<"

    def test_default_event_is_empty(self) -> None:
        ev = RawGameEvent(min=0.0)
        assert ev.get_info is None
        assert ev.info == ""
        assert ev.get_date_str is None
        assert ev.date_str == ""
        assert ev.get_score_str is None
        assert ev.score_str == "0-0"
        assert ev.show_dir == "<"

    def test_score_str_none_when_no_comma_segment(self) -> None:
        # info.split(',') of length 1 -> get_score_str is None -> score_str
        # defaults to "0-0" (LineupEvent.scala:94-100).
        ev = RawGameEvent.for_team(s="no-comma-here", min=0.0)
        assert ev.date_str == "no-comma-here"
        assert ev.get_score_str is None
        assert ev.score_str == "0-0"


class TestPossessionEvent:
    """``RawGameEvent.PossessionEvent`` accessors (``LineupEvent.scala:126-149``)."""

    def test_attacking_team_dir_gated(self) -> None:
        team_ev = RawGameEvent.for_team(s="team-str", min=0.0)
        opp_ev = RawGameEvent.for_opponent(s="opp-str", min=0.0)

        team_dir = PossessionEvent(Direction.TEAM)
        opp_dir = PossessionEvent(Direction.OPPONENT)
        init_dir = PossessionEvent(Direction.INIT)

        assert team_dir.attacking_team(team_ev) == "team-str"
        assert team_dir.attacking_team(opp_ev) is None
        assert opp_dir.attacking_team(opp_ev) == "opp-str"
        assert opp_dir.attacking_team(team_ev) is None
        assert init_dir.attacking_team(team_ev) is None
        assert init_dir.attacking_team(opp_ev) is None

    def test_defending_team_is_the_flip(self) -> None:
        team_ev = RawGameEvent.for_team(s="team-str", min=0.0)
        opp_ev = RawGameEvent.for_opponent(s="opp-str", min=0.0)

        team_dir = PossessionEvent(Direction.TEAM)
        opp_dir = PossessionEvent(Direction.OPPONENT)

        assert team_dir.defending_team(opp_ev) == "opp-str"
        assert team_dir.defending_team(team_ev) is None
        assert opp_dir.defending_team(team_ev) == "team-str"
        assert opp_dir.defending_team(opp_ev) is None


class TestScoreInfoAndLineupEventStatsEmpty:
    """``.empty`` factories must build FRESH instances (mutable dataclass
    port -- a shared singleton would alias mutations across callers)."""

    def test_score_info_empty(self) -> None:
        info = ScoreInfo.empty()
        assert info == ScoreInfo(Score(0, 0), Score(0, 0), 0, 0)

    def test_score_info_empty_calls_are_independent(self) -> None:
        a = ScoreInfo.empty()
        b = ScoreInfo.empty()
        a.start.scored = 99
        assert b.start.scored == 0

    def test_lineup_event_stats_empty(self) -> None:
        stats = LineupEventStats.empty()
        assert stats == LineupEventStats()
        assert stats.num_events == 0
        assert stats.num_possessions == 0
        assert stats.pts == 0
        assert stats.plus_minus == 0

    def test_lineup_event_stats_empty_calls_are_independent(self) -> None:
        a = LineupEventStats.empty()
        b = LineupEventStats.empty()
        a.num_possessions = 7
        a.fg.made.total = 3
        assert b.num_possessions == 0
        assert b.fg.made.total == 0


class TestValueTypesAndLineupId:
    def test_lineup_id_unknown(self) -> None:
        assert LineupId.unknown == LineupId("")

    def test_team_season_id_composition(self) -> None:
        tsid = TeamSeasonId(TeamId("Maryland"), Year(2019))
        assert tsid.team.name == "Maryland"
        assert tsid.year.value == 2019

    def test_player_id_and_team_id_are_hashable(self) -> None:
        assert len({PlayerId("A"), PlayerId("A"), PlayerId("B")}) == 2
        assert len({TeamId("X"), TeamId("X")}) == 1
