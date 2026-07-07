"""Oracle-replay tests for ``sportsdataverse.mbb.mbb_ncaa_possessions``
(Task 5a.3).

Transliterated 1:1 from ``PossessionUtilsTests.scala`` (utest,
``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``): the ``Events``
fixture object (``:19-86``), the ``"concurrent_event_handler"`` clumping
block (``:111-179``), the ``"calculate_stats"`` block's ``tests`` list
(``:191-336``, 47 cases) + ``test_prevs`` list (``:337-374``, 6 cases) with
the direction-symmetry / prev-alone-empty invariants, and the
``"assign_to_right_lineup"`` / ``"calculate_possessions"`` blocks
(``:377-475``). ``utest``'s ``==>``/``TestUtils.inside`` pattern-match is
deep-equality (no tolerance) -- ported as plain ``==`` assertions.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

import pytest

from sportsdataverse.mbb.mbb_ncaa_models import (
    Direction,
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PossCalcFragment,
    RawGameEvent,
    Score,
    ScoreInfo,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_possessions import (
    ConcurrentClump,
    PossState,
    assign_to_right_lineup,
    calculate_possessions,
    calculate_stats,
    concurrent_event_handler,
    count_matching,
    lineup_as_raw_clumps,
)

# ---------------------------------------------------------------------------
# The `Events` fixture object (PossessionUtilsTests.scala:19-86)
# ---------------------------------------------------------------------------


class Events:
    jump_won_team = RawGameEvent.for_team(min=0.0, s="19:58:00,0-0,Bruno Fernando, jumpball won")
    jump_won_opponent = RawGameEvent.for_opponent(min=0.0, s=jump_won_team.team)
    jump_lost_opponent = RawGameEvent.for_opponent(min=0.0, s="19:58:00,0-0,Kavell Bigby-Williams, jumpball lost")

    turnover_team = RawGameEvent.for_team(min=0.0, s="08:44:00,20-23,Bruno Fernando, turnover badpass")
    turnover_opponent = RawGameEvent.for_opponent(min=0.0, s=turnover_team.team)
    steal_team = RawGameEvent.for_team(min=0.0, s="05:10,55-68,MASON III,FRANK Steal")
    steal_opponent = RawGameEvent.for_opponent(min=0.0, s=steal_team.team)

    made_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,SMITH,JALEN made Three Point Jumper")
    made_opponent = RawGameEvent.for_opponent(min=0.0, s=made_team.team)
    missed_team = RawGameEvent.for_team(min=0.0, s="02:28:00,27-38,Eric Ayala, 3pt jumpshot missed")
    missed_team_2ndchance = RawGameEvent.for_team(min=0.0, s="02:28:00,27-38,Eric Ayala, 3pt jumpshot 2ndchance missed")
    missed_opponent = RawGameEvent.for_opponent(min=0.0, s=missed_team.team)
    made_ft_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,DREAD,MYLES made Free Throw")
    made_ft_opponent = RawGameEvent.for_opponent(min=0.0, s=made_ft_team.team)
    made_ft1_team = RawGameEvent.for_team(min=0.0, s="05:10,55-68,Kevin Anderson, freethrow 1of2 made")
    made_ft1_opponent = RawGameEvent.for_opponent(min=0.0, s=made_ft1_team.team)
    made_ft2_team = RawGameEvent.for_team(min=0.0, s="05:10,55-68,Kevin Anderson, freethrow 2of2 made")
    made_ft2_opponent = RawGameEvent.for_opponent(min=0.0, s=made_ft2_team.team)
    missed_ft_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,DREAD,MYLES missed Free Throw")
    missed_ft_opponent = RawGameEvent.for_opponent(min=0.0, s=missed_ft_team.team)
    missed_ft1_team = RawGameEvent.for_team(min=0.0, s="05:10,55-68,Kevin Anderson, freethrow 1of2 missed")
    missed_ft1_opponent = RawGameEvent.for_opponent(min=0.0, s=missed_ft1_team.team)
    missed_ft2_team = RawGameEvent.for_team(min=0.0, s="05:10,55-68,Kevin Anderson, freethrow 2of2 missed")
    missed_ft2_opponent = RawGameEvent.for_opponent(min=0.0, s=missed_ft2_team.team)
    # NB: the Scala source itself references `made_ft1_team.team.get` (not the
    # ftp1 variants' own strings) for both of the next two -- transliterated
    # faithfully; neither `made_ftp1_opponent` nor `missed_ftp1_opponent` is
    # exercised by any oracle case below (dead fixture in the upstream too).
    made_ftp1_team = RawGameEvent.for_team(min=0.0, s="05:10,56-68,Kevin Anderson, freethrow 1of1 made")
    made_ftp1_opponent = RawGameEvent.for_opponent(min=0.0, s=made_ft1_team.team)
    missed_ftp1_team = RawGameEvent.for_team(min=0.0, s="05:10,56-68,Kevin Anderson, freethrow 1of1 missed")
    missed_ftp1_opponent = RawGameEvent.for_opponent(min=0.0, s=made_ft1_team.team)
    # other shots:
    made_3p_team = made_team
    missed_3p_team = missed_team
    made_rim_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,Eric Carter, 2pt layup made")
    made_mid_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,Eric Ayala, 2pt jumpshot made")
    missed_rim_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,Eric Carter, 2pt layup missed")
    missed_mid_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,Eric Ayala, 2pt jumpshot missed")

    orb_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,Darryl Morsell, rebound offensive")
    orb_opponent = RawGameEvent.for_opponent(min=0.0, s=orb_team.team)
    drb_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,Darryl Morsell, rebound defensive")
    drb_opponent = RawGameEvent.for_opponent(min=0.0, s=drb_team.team)
    deadball_rb_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,TEAM Deadball Rebound")
    deadball_rb_opponent = RawGameEvent.for_opponent(min=0.0, s=deadball_rb_team.team)
    deadball_orb_team = RawGameEvent.for_team(min=0.0, s="04:28:0,52-59,Team, rebound offensivedeadball")
    deadball_orb_opponent = RawGameEvent.for_opponent(min=0.0, s=deadball_orb_team.team)
    # other events
    assist_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,Kyle Guy, assist")
    assist_opponent = RawGameEvent.for_opponent(min=0.0, s=assist_team.team)
    block_team = RawGameEvent.for_team(min=0.0, s="04:53,55-69,LAYMAN,JAKE Blocked Shot")
    block_opponent = RawGameEvent.for_opponent(min=0.0, s=block_team.team)

    foul_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,MYKHAILIUK,SVI Commits Foul")
    foul_opponent = RawGameEvent.for_opponent(min=0.0, s=foul_team.team)
    tech_team = RawGameEvent.for_team(min=0.0, s="06:43:00,55-79,Bruno Fernando, foul technical classa;2freethrow")
    tech_opponent = RawGameEvent.for_opponent(min=0.0, s=tech_team.team)
    flagrant_team = RawGameEvent.for_team(min=0.0, s="03:42:00,55-79,Eric Carter, foul personal flagrant1;2freethrow")
    flagrant_opponent = RawGameEvent.for_opponent(min=0.0, s=flagrant_team.team)
    # other:
    foul_off_team = RawGameEvent.for_team(min=0.0, s="10:00,51-60,Eric Ayala, foul offensive")

    @staticmethod
    def reverse_dir(ev: RawGameEvent) -> RawGameEvent:
        """``Events.reverse_dir`` (``:81-85``) -- swaps team<->opponent."""
        if ev.team is not None and ev.opponent is None:
            return RawGameEvent(ev.min, None, ev.team)
        if ev.opponent is not None and ev.team is None:
            return RawGameEvent(ev.min, ev.opponent, None)
        return ev


def _with_score(ev: RawGameEvent, new_score: str) -> RawGameEvent:
    """Rewrite an event's embedded score substring -- mirrors the Scala
    fixture idiom ``ev.team.map(_.replace(ev.score_str, new_score))``."""
    assert ev.team is not None
    return replace(ev, team=ev.team.replace(ev.score_str, new_score))


def _with_date_str(ev: RawGameEvent, new_date: str) -> RawGameEvent:
    """Rewrite an event's embedded date substring -- mirrors the Scala
    fixture idiom ``ev.team.map(_.replace(ev.date_str, new_date))``."""
    assert ev.team is not None
    return replace(ev, team=ev.team.replace(ev.date_str, new_date))


# ---------------------------------------------------------------------------
# "calculate_stats" -- the `tests` list (PossessionUtilsTests.scala:195-320, 47 cases)
# ---------------------------------------------------------------------------

_EMPTY = PossCalcFragment()

CALCULATE_STATS_CASES: list[tuple[list[RawGameEvent], PossCalcFragment]] = [
    # Shots made/missed
    ([Events.made_team, Events.missed_team, Events.made_team], PossCalcFragment(shots_made_or_missed=3)),
    ([Events.missed_team], PossCalcFragment(shots_made_or_missed=1)),
    ([Events.made_opponent, Events.missed_opponent], _EMPTY),
    # Free throws
    ([Events.made_ft1_team, Events.missed_ft1_team], PossCalcFragment(ft_events=1)),
    ([Events.made_ft_team, Events.missed_ft_team], PossCalcFragment(ft_events=1)),  # legacy format
    ([Events.made_ft2_team], _EMPTY),  # only FT1s count
    ([Events.made_ft1_team] * 4, PossCalcFragment(ft_events=1)),  # at most one FT set per clump
    ([Events.made_ft_team] * 4, PossCalcFragment(ft_events=1)),  # at most one FT set per clump
    ([Events.made_ft1_opponent, Events.missed_ft_opponent], _EMPTY),
    # Free throws that definitely _aren't_ and-1s:
    ([Events.missed_ft1_team], PossCalcFragment(ft_events=1)),  # not all single FTs are and-1s
    ([Events.missed_ft_team], PossCalcFragment(ft_events=1)),  # not all single FTs are and-1s; legacy
    ([Events.made_ft1_team], PossCalcFragment(ft_events=1)),  # not all single FTs are and-1s; legacy
    ([Events.made_ft_team], PossCalcFragment(ft_events=1)),  # not all single FTs are and-1s; legacy
    (
        [Events.made_ft1_team, Events.missed_ft1_team, Events.made_team],  # 2 FTs so ignore make
        PossCalcFragment(shots_made_or_missed=1, ft_events=1),
    ),
    (
        [Events.made_ft_team, Events.missed_ft_team, Events.made_team],  # 2 FTs so ignore make; legacy
        PossCalcFragment(shots_made_or_missed=1, ft_events=1),
    ),
    (
        [Events.made_ft1_team, Events.missed_team],  # missed so ignore 1 FT; legacy
        PossCalcFragment(shots_made_or_missed=1, ft_events=1),
    ),
    (
        [Events.made_ft_team, Events.missed_team],  # missed so ignore 1 FT; legacy
        PossCalcFragment(shots_made_or_missed=1, ft_events=1),
    ),
    # and-1s (see also below, under prev-testing)
    ([Events.made_ft1_team, Events.made_team], PossCalcFragment(shots_made_or_missed=1, ignored_and_ones=1)),
    ([Events.made_ft_team, Events.made_team], PossCalcFragment(shots_made_or_missed=1, ignored_and_ones=1)),  # legacy
    ([Events.missed_ft1_team, Events.made_team], PossCalcFragment(shots_made_or_missed=1, ignored_and_ones=1)),
    (
        [Events.missed_ft_team, Events.made_team],  # legacy
        PossCalcFragment(shots_made_or_missed=1, ignored_and_ones=1),
    ),
    ([Events.missed_ft1_opponent, Events.made_opponent], _EMPTY),
    ([Events.missed_ft_opponent, Events.made_opponent], _EMPTY),  # legacy
    # offsetting techs/flagrants
    ([Events.tech_team, Events.tech_opponent], PossCalcFragment(offsetting_bad_fouls=1)),
    ([Events.flagrant_team, Events.flagrant_opponent], PossCalcFragment(offsetting_bad_fouls=1)),
    (
        [Events.tech_team, Events.tech_opponent, Events.tech_team, Events.tech_opponent],
        PossCalcFragment(offsetting_bad_fouls=1),  # counts at most for one
    ),
    (
        [Events.tech_team, Events.tech_opponent, Events.flagrant_team, Events.flagrant_opponent],
        PossCalcFragment(offsetting_bad_fouls=1),  # counts at most for one
    ),
    (
        [Events.flagrant_team, Events.flagrant_opponent, Events.flagrant_team, Events.flagrant_opponent],
        PossCalcFragment(offsetting_bad_fouls=1),  # counts at most for one
    ),
    # non-offsetting techs/flagrants
    ([Events.flagrant_team, Events.tech_opponent], PossCalcFragment(bad_fouls=1)),
    ([Events.tech_team, Events.flagrant_opponent], PossCalcFragment(bad_fouls=1)),
    ([Events.flagrant_opponent], PossCalcFragment(bad_fouls=1)),
    ([Events.tech_opponent, Events.tech_opponent], PossCalcFragment(bad_fouls=1)),  # counts at most for one
    (
        [Events.flagrant_opponent, Events.flagrant_opponent],
        PossCalcFragment(bad_fouls=1),  # counts at most for one
    ),
    ([Events.tech_opponent], PossCalcFragment(bad_fouls=1)),
    (
        [Events.tech_opponent, Events.tech_opponent, Events.tech_opponent],
        PossCalcFragment(bad_fouls=1),  # counts at most for one
    ),
    # ORBs
    ([Events.orb_team, Events.orb_team, Events.orb_team], PossCalcFragment(liveball_orbs=3)),
    ([Events.orb_opponent], _EMPTY),
    # deadball ORBs
    ([Events.deadball_rb_team, Events.deadball_rb_team], _EMPTY),  # currently only handle new format rebounds
    ([Events.deadball_orb_team, Events.deadball_orb_team], PossCalcFragment(actual_deadball_orbs=2)),
    (
        [
            Events.deadball_orb_team,
            Events.deadball_orb_team,
            Events.missed_ft1_team,
            Events.missed_ft2_team,
            Events.made_ft2_team,
        ],
        PossCalcFragment(ft_events=1),
    ),
    (
        [
            Events.deadball_orb_team,
            Events.deadball_orb_team,  # mismatch vs number of misses IGNORED
            Events.missed_ft1_team,
            Events.made_ft2_team,
            Events.missed_ft2_team,
        ],
        PossCalcFragment(ft_events=1),
    ),
    (
        [
            Events.deadball_orb_team,
            Events.deadball_orb_team,  # only miss was the last free throw
            Events.made_ft1_team,
            Events.made_ft2_team,
            _with_score(Events.missed_ft2_team, "100-100"),
        ],
        PossCalcFragment(ft_events=1, actual_deadball_orbs=2),
    ),
    (
        [_with_date_str(Events.deadball_orb_team, "00:00:10")],  # ignore end of period deadball rebounds
        _EMPTY,
    ),
    ([Events.deadball_orb_opponent], _EMPTY),
    # turnovers
    ([Events.turnover_team, Events.turnover_team], PossCalcFragment(turnovers=2)),
    ([Events.turnover_opponent], _EMPTY),
    # misc other events
    (
        [
            Events.jump_won_team,
            Events.jump_won_opponent,
            Events.jump_lost_opponent,
            Events.steal_team,
            Events.steal_opponent,
            Events.drb_team,
            Events.drb_opponent,
            Events.foul_team,
            Events.foul_opponent,
        ],
        _EMPTY,
    ),
]


@pytest.mark.parametrize(
    "events, expected",
    CALCULATE_STATS_CASES,
    ids=[f"case{i}" for i in range(len(CALCULATE_STATS_CASES))],
)
def test_calculate_stats_oracle_and_direction_symmetry(events: list[RawGameEvent], expected: PossCalcFragment) -> None:
    """Both directions, and confirms a lone ``prev`` (empty current clump)
    is always ignored (``PossessionUtilsTests.scala:322-336``)."""
    events_oppo = [Events.reverse_dir(ev) for ev in events]

    assert calculate_stats(ConcurrentClump(events), ConcurrentClump([]), Direction.TEAM) == expected
    assert calculate_stats(ConcurrentClump(events_oppo), ConcurrentClump([]), Direction.OPPONENT) == expected
    assert calculate_stats(ConcurrentClump([]), ConcurrentClump(events), Direction.TEAM) == _EMPTY
    assert calculate_stats(ConcurrentClump([]), ConcurrentClump(events_oppo), Direction.OPPONENT) == _EMPTY


# ---------------------------------------------------------------------------
# "calculate_stats" -- `test_prevs` (PossessionUtilsTests.scala:337-374, 6 cases)
# ---------------------------------------------------------------------------

TEST_PREVS_CASES: list[tuple[list[RawGameEvent], list[RawGameEvent], PossCalcFragment]] = [
    # Check use of prev in determining and-1s
    ([Events.made_team], [Events.missed_ft1_team], PossCalcFragment(ignored_and_ones=1)),
    ([Events.made_team], [Events.made_ft1_team], PossCalcFragment(ignored_and_ones=1)),
    ([Events.missed_team], [Events.made_ft1_team], PossCalcFragment(ft_events=1)),
    (
        [Events.made_team, Events.made_opponent],
        [Events.made_ft1_team],
        PossCalcFragment(ft_events=1),
    ),
    # Check use of prev in determining deadball rebounds
    (
        [Events.missed_ft1_team],
        [
            Events.deadball_orb_team,
            Events.deadball_orb_team,  # mismatch vs number of misses IGNORED
            Events.made_ft2_team,
            Events.missed_ft2_team,
        ],
        _EMPTY,
    ),
    (
        [Events.made_ft1_team],
        [
            Events.deadball_orb_team,
            Events.deadball_orb_team,  # only miss was the last free throw
            Events.made_ft2_team,
            _with_score(Events.missed_ft2_team, "100-100"),
        ],
        PossCalcFragment(actual_deadball_orbs=2),
    ),
]


@pytest.mark.parametrize(
    "prev_events, curr_events, expected",
    TEST_PREVS_CASES,
    ids=[f"prev_case{i}" for i in range(len(TEST_PREVS_CASES))],
)
def test_calculate_stats_prev_dependent_oracle(
    prev_events: list[RawGameEvent], curr_events: list[RawGameEvent], expected: PossCalcFragment
) -> None:
    prev_oppo = [Events.reverse_dir(ev) for ev in prev_events]
    curr_oppo = [Events.reverse_dir(ev) for ev in curr_events]

    assert calculate_stats(ConcurrentClump(curr_events), ConcurrentClump(prev_events), Direction.TEAM) == expected
    assert calculate_stats(ConcurrentClump(curr_oppo), ConcurrentClump(prev_oppo), Direction.OPPONENT) == expected


# ---------------------------------------------------------------------------
# "concurrent_event_handler" (PossessionUtilsTests.scala:111-179)
# ---------------------------------------------------------------------------


def _build_base_lineup() -> LineupEvent:
    return LineupEvent(
        date=datetime(2019, 1, 1),
        location_type=LocationType.HOME,
        start_min=0.0,
        end_min=1.0,
        duration_mins=0.0,
        score_info=ScoreInfo(Score(1, 1), Score(3, 2), 2, 1),
        team=TeamSeasonId(TeamId("TeamA"), Year(2017)),
        opponent=TeamSeasonId(TeamId("TeamB"), Year(2017)),
        lineup_id=LineupId.unknown,
        players=[],
        players_in=[],
        players_out=[],
        raw_game_events=[],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )


def test_concurrent_event_handler_minute_and_game_break_clumping() -> None:
    """``PossessionUtilsTests.scala:114-150`` -- minute-based clumping plus
    the game-break / post-game-break-singleton port trap (``ev7``)."""
    ev1 = RawGameEvent.for_team(min=0.4, s="20:00,ev-1")
    ev2 = RawGameEvent.for_opponent(min=0.5, s="19:00,ev-2")
    ev3 = RawGameEvent.for_team(min=0.9, s="18:00,ev-3")
    ev4 = RawGameEvent.for_opponent(min=0.9, s="17:00,ev-4")
    ev5 = RawGameEvent.for_team(min=0.8, s="16:00,ev-5")
    ev6 = RawGameEvent.for_opponent(min=0.8, s="15:00,ev-6")
    ev7 = RawGameEvent.for_opponent(min=0.8, s="20:00,ev-7")  # game break!
    ev8 = RawGameEvent.for_team(min=1.0, s="19:00,ev-8")

    clumps_in = [ConcurrentClump([ev]) for ev in (ev1, ev2, ev3, ev4, ev5, ev6, ev7, ev8)]
    result = concurrent_event_handler(clumps_in)

    assert [c.evs for c in result] == [
        [ev1],
        [ev2],
        [ev3, ev4],
        [ev5, ev6],
        [ev7],
        [ev8],
    ]


def test_concurrent_event_handler_absorbs_lineup_boundaries() -> None:
    """``PossessionUtilsTests.scala:152-178`` -- lineup-boundary markers are
    absorbed into the current batch and never trigger a flush themselves."""
    ev2 = RawGameEvent.for_opponent(min=0.5, s="19:00,ev-2")
    ev3 = RawGameEvent.for_team(min=0.9, s="18:00,ev-3")
    ev4 = RawGameEvent.for_opponent(min=0.9, s="17:00,ev-4")
    ev5 = RawGameEvent.for_team(min=0.8, s="16:00,ev-5")

    base_lineup = _build_base_lineup()
    lineup1 = replace(base_lineup, start_min=1.0, end_min=2.0)
    lineup2 = replace(base_lineup, start_min=2.0, end_min=3.0)

    clumps_in = [
        ConcurrentClump([ev2], []),
        ConcurrentClump([ev3], [base_lineup]),
        ConcurrentClump([ev4], [lineup1]),
        ConcurrentClump([ev5], [lineup2]),
    ]
    result = concurrent_event_handler(clumps_in)

    assert [(c.evs, c.lineups) for c in result] == [
        ([ev2], []),
        ([ev3, ev4], [base_lineup, lineup1]),
        ([ev5], [lineup2]),
    ]


# ---------------------------------------------------------------------------
# "assign_to_right_lineup" + "calculate_possessions"
# (PossessionUtilsTests.scala:377-475)
# ---------------------------------------------------------------------------


def _build_test_lineups() -> list[LineupEvent]:
    """The ``lineup_2a``/``lineup_2b`` pair (``:377-411``) shared by the
    ``"assign_to_right_lineup"`` and ``"calculate_possessions"`` blocks."""
    base_lineup = _build_base_lineup()

    lineup_2a_events = sorted(
        [replace(Events.made_opponent, min=2.0) for _ in range(3)] + [replace(Events.made_opponent, min=1.0)],
        key=lambda ev: ev.min,
    )
    lineup_2a = replace(
        base_lineup,
        team_stats=replace(base_lineup.team_stats, pts=0, num_possessions=0),
        opponent_stats=replace(base_lineup.opponent_stats, pts=0, num_possessions=0),
        raw_game_events=lineup_2a_events,
    )

    lineup_2b_events = sorted(
        [replace(Events.made_opponent, min=3.0) for _ in range(2)] + [replace(Events.made_opponent, min=2.0)],
        key=lambda ev: ev.min,
    )
    lineup_2b = replace(
        base_lineup,
        team_stats=replace(base_lineup.team_stats, pts=0, num_possessions=0),
        opponent_stats=replace(base_lineup.opponent_stats, pts=0, num_possessions=0),
        raw_game_events=lineup_2b_events,
    )

    return [lineup_2a, lineup_2b]


def test_assign_to_right_lineup_single_lineup_and_negative_fixer() -> None:
    """``PossessionUtilsTests.scala:413-439`` -- single-lineup add path,
    plus the ``pts > 0 && num_possessions <= 0 -> 1`` clamp."""
    base_lineup = _build_base_lineup()
    lineup1 = replace(
        base_lineup,
        team_stats=replace(base_lineup.team_stats, pts=0, num_possessions=0),
        opponent_stats=replace(base_lineup.opponent_stats, pts=2, num_possessions=0),
    )
    state1 = PossState(
        team_stats=PossCalcFragment(shots_made_or_missed=2),
        opponent_stats=PossCalcFragment(),
        prev_clump=ConcurrentClump(),
    )
    team_stats1 = PossCalcFragment(shots_made_or_missed=1)
    oppo_stats1 = PossCalcFragment()
    clump1 = ConcurrentClump([], [lineup1])

    result = assign_to_right_lineup(state1, team_stats1, oppo_stats1, clump1, ConcurrentClump())

    assert len(result) == 1
    assert result[0].team_stats.num_possessions == 3
    assert result[0].opponent_stats.num_possessions == 1


def test_assign_to_right_lineup_multi_lineup_balancer() -> None:
    """``PossessionUtilsTests.scala:441-463`` -- the greedy round-robin
    split across two candidate lineups."""
    test_lineups = _build_test_lineups()

    state2 = PossState(
        team_stats=PossCalcFragment(shots_made_or_missed=2),
        opponent_stats=PossCalcFragment(shots_made_or_missed=3),
        prev_clump=ConcurrentClump(),
    )
    team_stats2 = PossCalcFragment(shots_made_or_missed=0)
    oppo_stats2 = PossCalcFragment(shots_made_or_missed=4)
    clump2 = ConcurrentClump([replace(Events.made_team, min=2.0)], test_lineups)

    result = assign_to_right_lineup(state2, team_stats2, oppo_stats2, clump2, ConcurrentClump())

    assert len(result) == 2
    lineup_a, lineup_b = result
    assert lineup_a.team_stats.num_possessions == 2  # from state2
    assert lineup_a.opponent_stats.num_possessions == 6  # 3 from state2 + 75% of the fragment
    assert lineup_b.team_stats.num_possessions == 0  # allowed because score==0
    assert lineup_b.opponent_stats.num_possessions == 1  # 25% of the fragment


def test_calculate_possessions_end_to_end() -> None:
    """``PossessionUtilsTests.scala:466-475`` -- the full driver over the
    same ``lineup_2a``/``lineup_2b`` pair."""
    test_lineups = _build_test_lineups()

    result = calculate_possessions(test_lineups)

    assert len(result) == 2
    lineup_a, lineup_b = result
    assert lineup_a.team_stats.num_possessions == 0
    assert lineup_a.opponent_stats.num_possessions == 5  # all the min1 and all-1 min2 events
    assert lineup_b.team_stats.num_possessions == 0
    assert lineup_b.opponent_stats.num_possessions == 2  # 1 min2 event, and all the min 3 events


# ---------------------------------------------------------------------------
# Supplementary coverage (not in the Scala oracle, but cheap + load-bearing)
# ---------------------------------------------------------------------------


class TestCountMatching:
    def test_no_match_when_side_is_none(self) -> None:
        # Events.made_opponent has no `.team` -> attacking_team under
        # Direction.TEAM is None for it.
        from sportsdataverse.mbb.mbb_ncaa_models import PossessionEvent

        attacking = PossessionEvent(Direction.TEAM).attacking_team
        from sportsdataverse.mbb.mbb_ncaa_events import parse_shot_made

        assert count_matching([Events.made_opponent], attacking, parse_shot_made) == 0

    def test_union_of_multiple_parsers(self) -> None:
        from sportsdataverse.mbb.mbb_ncaa_events import parse_free_throw_made, parse_free_throw_missed
        from sportsdataverse.mbb.mbb_ncaa_models import PossessionEvent

        attacking = PossessionEvent(Direction.TEAM).attacking_team
        assert (
            count_matching(
                [Events.made_ft1_team, Events.missed_ft1_team],
                attacking,
                parse_free_throw_made,
                parse_free_throw_missed,
            )
            == 2
        )


def test_lineup_as_raw_clumps_yields_singletons_then_boundary() -> None:
    lineup = replace(_build_base_lineup(), raw_game_events=[Events.made_team, Events.missed_team])
    clumps = list(lineup_as_raw_clumps(lineup))

    assert len(clumps) == 3
    assert clumps[0] == ConcurrentClump([Events.made_team])
    assert clumps[1] == ConcurrentClump([Events.missed_team])
    assert clumps[2] == ConcurrentClump([], [lineup])
