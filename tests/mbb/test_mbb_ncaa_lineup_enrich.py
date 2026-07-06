"""Oracle-replay tests for ``sportsdataverse.mbb.mbb_ncaa_lineup_enrich``
(Tasks 5c.1/5c.2).

Transliterated 1:1 from ``LineupUtilsTests.scala`` (utest,
``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``): the ``"enrich_lineup"``
block (``:61-80``), the ``"fix_possible_score_swap_bug"`` block (``:81-208``,
incl. the real-game-4690813 fixture), the ``"enrich_stats"`` block's
table-driven per-event-type cases (``:209-449``, TOTAL-only dispatch --
scramble/transition tagging lands in Task 5c.3) plus its two assist-pairing
cases (``assist_rim_test``/``assist_mid_test``, ``:411-449``), the
``"add_stats_to_lineups"`` block (``:451-460``), the ``"is_scramble"`` block's
13 tag sub-cases (``:478-699``), and the ``"is_end_of_game_fouling_vs_fastbreak"``
block (``:714-755``).

The ``Events`` fixture is reused (imported) from
``test_mbb_ncaa_possessions.py`` rather than redefined -- it already carries
every raw-event literal this block needs, transliterated from
``PossessionUtilsTests.Events``.

The oracle's block-3 comparison strips scramble/transition (``.orb``)
markers from the actual result before comparing against a total-only
expected value, because the *real* Scala ``enrich_stats`` already has
``is_scramble``/``is_transition`` wired in. This port's ``enrich_stats``
doesn't wire those in until Task 5c.3 -- it never touches ``.orb``/``.early``
at all yet -- so no such stripping is needed here: the actual result is
asserted directly against a total-only expected value.

The ``"is_scramble"`` block's FINAL 2 cases (``LineupUtilsTests.scala
:652-698``) run a scenario through ``enrich_stats`` to prove ``is_scramble``
is actually wired into the dispatch (team version and player version) --
that wiring is the ``_shot_clock_selector_builder`` seam, which doesn't land
until Task 5c.3, so those 2 integration cases are deferred there.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any, Optional

import pytest

from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import (
    add_stats_to_lineups,
    enrich_lineup,
    enrich_stats,
    ensure_ev_uniqueness,
    fix_possible_score_swap_bug,
    is_end_of_game_fouling_vs_fastbreak,
    is_scramble,
)
from sportsdataverse.mbb.mbb_ncaa_models import (
    AssistEvent,
    AssistInfo,
    Direction,
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PossessionEvent,
    RawGameEvent,
    Score,
    ScoreInfo,
    ShotClockStats,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_possessions import ConcurrentClump
from tests.mbb.test_mbb_ncaa_possessions import Events

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_base_lineup() -> LineupEvent:
    """Mirrors the oracle's ``base_lineup`` (``LineupUtilsTests.scala:19-37``)."""
    return LineupEvent(
        date=datetime(2019, 1, 1),
        location_type=LocationType.HOME,
        start_min=0.0,
        end_min=-100.0,
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


def _get_shot_clock(stats: LineupEventStats, *path: str) -> ShotClockStats:
    """Navigate a dotted attribute path into a :class:`ShotClockStats` leaf,
    creating any ``None`` optional container/leaf along the way (test-only
    helper mirroring the oracle's quicklens ``.atOrElse(...)`` selectors used
    to build the block-3 expected-stats table)."""
    obj: Any = stats
    for part in path[:-1]:
        nxt = getattr(obj, part)
        if nxt is None:
            nxt = AssistInfo()
            setattr(obj, part, nxt)
        obj = nxt
    leaf_attr = path[-1]
    leaf = getattr(obj, leaf_attr)
    if leaf is None:
        leaf = ShotClockStats()
        setattr(obj, leaf_attr, leaf)
    return leaf


# ---------------------------------------------------------------------------
# "enrich_lineup" (LineupUtilsTests.scala:61-80)
# ---------------------------------------------------------------------------


def test_enrich_lineup_team_side() -> None:
    lineup = replace(
        _build_base_lineup(),
        raw_game_events=[RawGameEvent(0.0, "19:58:00,0-0,team1.1", None)],
    )
    enriched = enrich_lineup(lineup)
    assert enriched.team_stats.num_events == 0
    assert enriched.team_stats.num_possessions == 0
    assert enriched.team_stats.pts == 2
    assert enriched.team_stats.plus_minus == 1
    assert enriched.opponent_stats.num_events == 0
    assert enriched.opponent_stats.num_possessions == 0
    assert enriched.opponent_stats.pts == 1
    assert enriched.opponent_stats.plus_minus == -1


def test_enrich_lineup_opponent_side() -> None:
    lineup = replace(
        _build_base_lineup(),
        raw_game_events=[RawGameEvent(0.0, None, "19:58:00,0-0,opp1.1")],
    )
    enriched = enrich_lineup(lineup)
    assert enriched.team_stats.pts == 2
    assert enriched.team_stats.plus_minus == 1
    assert enriched.opponent_stats.pts == 1
    assert enriched.opponent_stats.plus_minus == -1


# ---------------------------------------------------------------------------
# "fix_possible_score_swap_bug" (LineupUtilsTests.scala:81-208, incl. the
# real game-4690813 fixture)
# ---------------------------------------------------------------------------


def _build_correct_box_score() -> LineupEvent:
    return LineupEvent(
        date=datetime(2019, 1, 1),
        location_type=LocationType.HOME,
        start_min=0.0,
        end_min=-100.0,
        duration_mins=0.0,
        score_info=ScoreInfo(Score(0, 0), Score(67, 78), 0, 0),
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


def test_fix_possible_score_swap_bug_game_4690813() -> None:
    # Data taken from https://stats.ncaa.org/gaame/box_score/4690813?period_no=1
    correct_box_score = _build_correct_box_score()

    score1_swapped = ScoreInfo(start=Score(8, 14), end=Score(9, 18), start_diff=-6, end_diff=-9)
    score1_fixed = ScoreInfo(start=Score(14, 8), end=Score(18, 9), start_diff=6, end_diff=9)

    team_stats1_swapped = replace(
        LineupEventStats.empty(),
        num_events=9,
        orb=ShotClockStats(0, 1, 2, 3, 4),
        num_possessions=4,
        pts=1,
        plus_minus=-3,
    )
    team_stats1_fixed = replace(team_stats1_swapped, pts=4, plus_minus=3)

    opp_stats1_swapped = replace(
        LineupEventStats.empty(),
        num_events=22,
        orb=ShotClockStats(10, 11, 12, 13, 14),
        num_possessions=3,  # (was actually 4 but want to demo this not changing)
        pts=4,
        plus_minus=3,
    )
    opp_stats1_fixed = replace(opp_stats1_swapped, pts=1, plus_minus=-3)

    score2_swapped = ScoreInfo(start=Score(77, 67), end=Score(78, 67), start_diff=10, end_diff=11)
    score2_fixed = ScoreInfo(start=Score(67, 77), end=Score(67, 78), start_diff=-10, end_diff=-11)

    team_stats2_swapped = replace(
        LineupEventStats.empty(),
        num_events=1,
        orb=ShotClockStats(5, 6, 7, 8, 9),
        num_possessions=1,
        pts=1,
        plus_minus=1,
    )
    team_stats2_fixed = replace(team_stats2_swapped, pts=0, plus_minus=-1)

    opp_stats2_swapped = replace(
        LineupEventStats.empty(),
        num_events=1,
        orb=ShotClockStats(15, 16, 17, 18, 19),
        num_possessions=1,
        pts=0,
        plus_minus=-1,
    )
    opp_stats2_fixed = replace(opp_stats2_swapped, pts=1, plus_minus=1)

    swapped_lineup = [
        replace(
            correct_box_score,
            score_info=score1_swapped,
            team_stats=team_stats1_swapped,
            opponent_stats=opp_stats1_swapped,
        ),
        replace(
            correct_box_score,
            score_info=score2_swapped,
            team_stats=team_stats2_swapped,
            opponent_stats=opp_stats2_swapped,
        ),
    ]

    fixed = fix_possible_score_swap_bug(swapped_lineup, correct_box_score)

    assert fixed == [
        replace(
            correct_box_score, score_info=score1_fixed, team_stats=team_stats1_fixed, opponent_stats=opp_stats1_fixed
        ),
        replace(
            correct_box_score, score_info=score2_fixed, team_stats=team_stats2_fixed, opponent_stats=opp_stats2_fixed
        ),
    ]

    # Check random score inaccuracies don't cause this -- ie we do nothing:
    different_incorrect_score = ScoreInfo(Score(0, 0), Score(92, 100), 0, 0)
    incorrect_box_score = replace(correct_box_score, score_info=different_incorrect_score)

    assert fix_possible_score_swap_bug(swapped_lineup, incorrect_box_score) == swapped_lineup


def test_fix_possible_score_swap_bug_empty_list() -> None:
    box_lineup = _build_correct_box_score()
    assert fix_possible_score_swap_bug([], box_lineup) == []


# ---------------------------------------------------------------------------
# "ensure_ev_uniqueness"
# ---------------------------------------------------------------------------


def test_ensure_ev_uniqueness_nudges_by_index() -> None:
    clump = ConcurrentClump([replace(Events.made_team, min=1.0), replace(Events.missed_team, min=1.0)])
    result = ensure_ev_uniqueness(clump)
    assert result.evs[0].min == pytest.approx(1.0)
    assert result.evs[1].min == pytest.approx(1.0 + 1e-6)


# ---------------------------------------------------------------------------
# "enrich_stats" -- table-driven per-event-type cases (LineupUtilsTests.scala
# :209-449). TOTAL-only: the scramble/transition selectors land in Tasks
# 5c.2/5c.3, so every expected transform below only ever bumps `.total`.
# ---------------------------------------------------------------------------

TEST_CASES: list[tuple[list[RawGameEvent], list[tuple[str, ...]]]] = [
    # Edge cases
    ([], []),
    ([Events.made_opponent], []),  # ignored, wrong direction
    # Shots
    (
        [Events.made_3p_team],
        [("fg_3p", "attempts"), ("fg", "attempts"), ("fg_3p", "made"), ("fg", "made")],
    ),
    ([Events.missed_3p_team], [("fg_3p", "attempts"), ("fg", "attempts")]),
    (
        [Events.made_rim_team],
        [
            ("fg_2p", "attempts"),
            ("fg_rim", "attempts"),
            ("fg", "attempts"),
            ("fg_2p", "made"),
            ("fg_rim", "made"),
            ("fg", "made"),
        ],
    ),
    ([Events.missed_rim_team], [("fg_2p", "attempts"), ("fg_rim", "attempts"), ("fg", "attempts")]),
    (
        [Events.made_mid_team],
        [
            ("fg_2p", "attempts"),
            ("fg_mid", "attempts"),
            ("fg", "attempts"),
            ("fg_2p", "made"),
            ("fg_mid", "made"),
            ("fg", "made"),
        ],
    ),
    ([Events.missed_mid_team], [("fg_2p", "attempts"), ("fg_mid", "attempts"), ("fg", "attempts")]),
    # Free throws
    ([Events.made_ft_team], [("ft", "attempts"), ("ft", "made")]),
    ([Events.missed_ft_team], [("ft", "attempts")]),
    # Misc (rebounds)
    ([Events.orb_team], [("orb",)]),
    ([Events.drb_team], [("drb",)]),
    ([Events.deadball_orb_team], []),
    ([Events.deadball_rb_team], []),  # (not ideal because could be a defensive rebound)
    # (steals)
    ([Events.steal_team], [("stl",)]),
    # (turnovers)
    ([Events.turnover_team], [("to",)]),
    # (assists)
    ([Events.assist_team], [("assist",)]),
    # (blocks)
    ([Events.block_team], [("blk",)]),
    # Fouls
    ([Events.foul_team], [("foul",)]),
    ([Events.flagrant_team], [("foul",)]),
    ([Events.tech_team], [("foul",)]),
    ([Events.foul_off_team], [("foul",)]),
]

_all_events: list[RawGameEvent] = []
_all_transforms: list[tuple[str, ...]] = []
for _events, _transforms in TEST_CASES:
    _all_events = _all_events + _events
    _all_transforms = _all_transforms + _transforms

ALL_AT_ONCE: tuple[list[RawGameEvent], list[tuple[str, ...]]] = (
    _all_events,
    _all_transforms
    + [
        ("fg_3p", "ast"),
        ("fg_rim", "ast"),
        ("fg_mid", "ast"),
        ("ast_3p", "counts"),  # (only the 3P'er since they are co-located in time)
    ],
)


@pytest.mark.parametrize("use_oppo", [False, True], ids=["team_filter", "oppo_filter"])
def test_enrich_stats_table(use_oppo: bool) -> None:
    event_parser = PossessionEvent(Direction.OPPONENT if use_oppo else Direction.TEAM)
    for events, transforms in [*TEST_CASES, ALL_AT_ONCE]:
        test_events = [Events.reverse_dir(ev) for ev in events] if use_oppo else events
        lineup = replace(_build_base_lineup(), raw_game_events=test_events)

        actual = enrich_stats(lineup, event_parser, LineupEventStats())

        expected = LineupEventStats()
        for path in transforms:
            _get_shot_clock(expected, *path).total += 1

        assert actual == expected, f"events={[ev.info for ev in events]} transforms={transforms}"


def test_enrich_stats_player_filter_restricts_to_named_player() -> None:
    team_filter = PossessionEvent(Direction.TEAM)
    lineup = replace(_build_base_lineup(), raw_game_events=[Events.foul_team, Events.tech_team])
    stats = enrich_stats(
        lineup,
        team_filter,
        LineupEventStats(),
        player_filter_coder=lambda p: (p == "Bruno Fernando", "not_used"),
    )
    expected = LineupEventStats()
    _get_shot_clock(expected, "foul").total = 1
    assert stats == expected


def test_enrich_stats_player_filter_empty_case() -> None:
    """A diverse batch of events, all rejected by an always-false player
    filter, must leave the stats untouched. The oracle uses
    ``EventUtilsTests.all_test_cases`` for this batch; that fixture isn't
    ported, so this reuses ``ALL_AT_ONCE``'s already-diverse event list as
    equivalent insurance."""
    team_filter = PossessionEvent(Direction.TEAM)
    lineup = replace(_build_base_lineup(), raw_game_events=ALL_AT_ONCE[0])
    stats = enrich_stats(
        lineup,
        team_filter,
        LineupEventStats(),
        player_filter_coder=lambda _p: (False, "not_used"),
    )
    assert stats == LineupEventStats()


def test_enrich_stats_assist_target_rim() -> None:
    """Test assist.target generation for a rim shot (assist_rim_test,
    LineupUtilsTests.scala:412-430)."""
    team_filter = PossessionEvent(Direction.TEAM)
    lineup = replace(_build_base_lineup(), raw_game_events=[Events.assist_team, Events.made_rim_team])
    stats = enrich_stats(
        lineup,
        team_filter,
        LineupEventStats(),
        player_filter_coder=lambda p: (p == "Kyle Guy", f"code({p})"),
    )
    expected = LineupEventStats()
    expected.assist = ShotClockStats(total=1)
    expected.ast_rim = AssistInfo(
        counts=ShotClockStats(total=1),
        target=[AssistEvent("code(Eric Carter)", ShotClockStats(total=1))],
    )
    assert stats == expected


def test_enrich_stats_assist_target_mid() -> None:
    """Test assist.target generation for a mid-range shot (assist_mid_test,
    LineupUtilsTests.scala:431-449)."""
    team_filter = PossessionEvent(Direction.TEAM)
    lineup = replace(_build_base_lineup(), raw_game_events=[Events.assist_team, Events.made_mid_team])
    stats = enrich_stats(
        lineup,
        team_filter,
        LineupEventStats(),
        player_filter_coder=lambda p: (p == "Kyle Guy", f"code({p})"),
    )
    expected = LineupEventStats()
    expected.assist = ShotClockStats(total=1)
    expected.ast_mid = AssistInfo(
        counts=ShotClockStats(total=1),
        target=[AssistEvent("code(Eric Ayala)", ShotClockStats(total=1))],
    )
    assert stats == expected


def test_enrich_stats_does_not_mutate_input_stats() -> None:
    """Regression guard for the deep-copy boundary: a shared "zero stats"
    literal passed as the ``stats`` argument must never be mutated by a
    call, even though the returned value is built by mutating a private
    working copy."""
    zero_stats = LineupEventStats()
    lineup = replace(_build_base_lineup(), raw_game_events=[Events.made_rim_team])
    team_filter = PossessionEvent(Direction.TEAM)
    result = enrich_stats(lineup, team_filter, zero_stats)
    assert zero_stats == LineupEventStats()
    assert result != zero_stats


# ---------------------------------------------------------------------------
# "add_stats_to_lineups" (LineupUtilsTests.scala:451-460)
# ---------------------------------------------------------------------------


def test_add_stats_to_lineups() -> None:
    test_lineup = replace(_build_base_lineup(), raw_game_events=[Events.foul_team, Events.turnover_opponent])
    lineup = add_stats_to_lineups(test_lineup)
    assert lineup.team_stats == replace(LineupEventStats(), foul=ShotClockStats(total=1))
    assert lineup.opponent_stats == replace(LineupEventStats(), to=ShotClockStats(total=1))
    assert lineup.raw_game_events == test_lineup.raw_game_events


# ---------------------------------------------------------------------------
# "is_scramble" (LineupUtilsTests.scala:478-699) -- 13 tag sub-cases.
#
# The block's FINAL 2 cases (:652-698) run a scenario through `enrich_stats`
# to prove `is_scramble` is wired into the dispatch (team + player version);
# that wiring is the `_shot_clock_selector_builder` seam from Task 5c.3, so
# those 2 integration cases are deferred there (see the module docstring).
# ---------------------------------------------------------------------------

_SEC_TO_MIN = 1.0 / 60
_TEAM_FILTER = PossessionEvent(Direction.TEAM)
_OPPO_FILTER = PossessionEvent(Direction.OPPONENT)


def _clump_scenario_builder(
    current: list[RawGameEvent], before: list[RawGameEvent], current_sec: float, before_sec: float
) -> tuple[ConcurrentClump, list[ConcurrentClump]]:
    """Builds 2 clumps of events with a specified time difference -- also
    exercises :func:`ensure_ev_uniqueness` (``clump_scenario_builder``,
    ``LineupUtilsTests.scala:464-476``)."""
    current_clump = ConcurrentClump([replace(ev, min=current_sec * _SEC_TO_MIN) for ev in current])
    before_clumps = (
        [] if not before else [ConcurrentClump([replace(ev, min=before_sec * _SEC_TO_MIN) for ev in before])]
    )
    return (
        ensure_ev_uniqueness(current_clump),
        [ensure_ev_uniqueness(c) for c in before_clumps],
    )


# Each row: (case id, current events, before events, current_sec, before_sec,
# expected debug tag, expected per-event booleans for `current_clump.evs`).
IS_SCRAMBLE_CASES: list[tuple[str, list[RawGameEvent], list[RawGameEvent], float, float, str, list[bool]]] = [
    (
        # N/A no offensive events in current
        "N/A",
        [Events.foul_team, Events.made_opponent],
        [Events.made_team, Events.missed_ft1_team, Events.orb_team, Events.missed_rim_team],
        10.0,
        5.0,
        "N/A",
        [],  # oracle only asserts the tag for this case
    ),
    (
        # 0a.2: (nothing), small gap, turnover, ORB, made shot
        "0a.2",
        [Events.turnover_team, Events.orb_team, Events.missed_rim_team],
        [],
        10.0,
        5.0,
        "0a",
        [True, True, False],
    ),
    (
        # 1aa.1: missed, short gap, rebound, missed shot + made shot
        "1aa.1",
        [Events.missed_rim_team, Events.made_team, Events.orb_team],
        [Events.missed_rim_team],
        10.0,
        5.0,
        "1aa",
        [True, True, True],
    ),
    (
        # 1ab.1: TOs then missed shot (TO allowed)
        "1ab.1",
        [Events.turnover_team, Events.made_team, Events.missed_ftp1_team],
        [Events.missed_team, Events.orb_team],
        10.0,
        5.0,
        "1ab",
        [True, False, False],
    ),
    (
        # 1ab.2: dangling FT bug workaround
        "1ab.2",
        [Events.missed_ft_team, Events.made_team],
        [Events.missed_ft_team],
        10.0,
        5.0,
        "1ab",
        [False, False],
    ),
    (
        # 1b.1: like 1aa.1 but with large gap
        "1b.1",
        [Events.missed_rim_team, Events.made_team, Events.orb_team],
        [Events.missed_rim_team],
        12.0,
        5.0,
        "1b",
        [False, True, True],
    ),
    (
        # 1b.2: like 1aa.1 but with large gap and including an assist
        "1b.2",
        [Events.made_team, Events.assist_team, Events.missed_ftp1_team, Events.orb_team, Events.missed_rim_team],
        [Events.missed_rim_team],
        12.0,
        5.0,
        "1b",
        [False, False, False, True, True],
    ),
    (
        # 2aa.1: def, small gap, old format FTs split by ORBs
        "2aa.1",
        [Events.made_ft_team, Events.orb_team, Events.made_ft1_team, Events.made_ft2_team],
        [Events.foul_team, Events.made_opponent],
        10.0,
        5.0,
        "2aa",
        [False, True, True, True],
    ),
    (
        # 2aa.2: def, long/irrelevant gap, 2ndchance miss, ORB, miss
        "2aa.2",
        [Events.missed_team_2ndchance, Events.orb_team, Events.made_team],
        [Events.foul_team, Events.made_opponent],
        12.0,
        5.0,
        "2aa",
        [True, True, False],
    ),
    (
        # 2ab.1: defense, small gap, missed shot, deadball rebound, FT, FT (no ORBs)
        "2ab.1",
        [Events.missed_rim_team, Events.deadball_rb_team, Events.made_ft_team, Events.missed_ft2_team],
        [Events.missed_opponent],
        10.0,
        5.0,
        "2ab",
        [False, False, False, False],
    ),
    (
        # 2ab.2: (nothing), small gap, FT1, FT2, ORB, FT3 (ie all one offensive event)
        "2ab.2",
        [Events.made_ft1_team, Events.orb_team, Events.made_ft2_team],
        [],
        10.0,
        5.0,
        "2ab",
        [False, False, False],
    ),
    (
        # 0a.3: like 2ab.2 but the 2nd "made_ft2_team" won't be a dup, thanks to ensure_ev_uniqueness
        "0a.3",
        [Events.made_ft1_team, Events.orb_team, Events.made_ft2_team, Events.made_ft2_team],
        [],
        10.0,
        5.0,
        "0a",
        [False, True, False, True],
    ),
    (
        # 2ab.3: like 1ab.1 but no misses in prev clump, so first event can't be a rebound either
        "2ab.3",
        [Events.turnover_team, Events.made_team, Events.missed_ftp1_team],
        [Events.made_team],
        10.0,
        5.0,
        "2ab",
        [False, False, False],
    ),
]


@pytest.mark.parametrize(
    "current,before,current_sec,before_sec,expected_tag,expected_bools",
    [case[1:] for case in IS_SCRAMBLE_CASES],
    ids=[case[0] for case in IS_SCRAMBLE_CASES],
)
def test_is_scramble(
    current: list[RawGameEvent],
    before: list[RawGameEvent],
    current_sec: float,
    before_sec: float,
    expected_tag: str,
    expected_bools: list[bool],
) -> None:
    current_clump, before_clumps = _clump_scenario_builder(current, before, current_sec, before_sec)
    predicate, tag = is_scramble(current_clump, before_clumps, _TEAM_FILTER, player_version=False)
    assert tag == expected_tag
    if expected_bools:
        assert [predicate(ev) for ev in current_clump.evs] == expected_bools


# ---------------------------------------------------------------------------
# "is_end_of_game_fouling_vs_fastbreak" (LineupUtilsTests.scala:714-755)
# ---------------------------------------------------------------------------


def _sub_score(new_score: str, ev: RawGameEvent) -> RawGameEvent:
    """Rewrites the 2nd comma-field (the score) of ``ev``'s populated side to
    ``new_score``, leaving everything else unchanged (``sub_score``,
    ``LineupUtilsTests.scala:701-713``)."""

    def _sub(s: Optional[str]) -> Optional[str]:
        if s is None:
            return None
        parts = s.split(",")
        if len(parts) < 2:
            return s
        return ",".join([parts[0], new_score] + parts[2:])

    return replace(ev, team=_sub(ev.team), opponent=_sub(ev.opponent))


# Each row: (event_parser, minute, score, pre-event, expected result).
IS_END_OF_GAME_FOULING_CASES: list[tuple[PossessionEvent, float, str, RawGameEvent, bool]] = [
    # Basic logic:
    (_TEAM_FILTER, 37.5, "60-58", Events.made_team, False),
    (_TEAM_FILTER, 38.1, "60-58", Events.made_ft_team, True),
    (_TEAM_FILTER, 38.1, "60-58", Events.made_team, False),
    (_TEAM_FILTER, 38.1, "60-58", Events.made_ft_opponent, False),
    (_TEAM_FILTER, 38.1, "58-60", Events.made_ft_team, False),
    (_TEAM_FILTER, 38.1, "70-58", Events.made_team, False),
    # Check account for the score increase on made FTs:
    (_TEAM_FILTER, 38.1, "60-59", Events.made_ft_team, False),
    (_TEAM_FILTER, 38.1, "60-59", Events.missed_ft_team, True),
    # Opponents:
    (_OPPO_FILTER, 38.1, "60-58", Events.made_ft_opponent, False),
    (_OPPO_FILTER, 38.1, "58-60", Events.made_ft_opponent, True),
    # OTs:
    (_TEAM_FILTER, 42.0, "60-58", Events.made_ft_team, False),
    (_TEAM_FILTER, 43.2, "60-58", Events.made_ft_team, True),
    (_TEAM_FILTER, 45.1, "60-58", Events.made_ft_team, False),
    (_TEAM_FILTER, 49.5, "60-58", Events.made_ft_team, True),
    (_TEAM_FILTER, 51.1, "60-58", Events.made_ft_team, False),
    (_TEAM_FILTER, 55.0, "60-58", Events.made_ft_team, True),
    (_TEAM_FILTER, 57.1, "60-58", Events.made_ft_team, False),
    (_TEAM_FILTER, 59.0, "60-58", Events.made_ft_team, True),
    (_TEAM_FILTER, 63.0, "60-58", Events.made_ft_team, False),
    (_TEAM_FILTER, 64.9, "60-58", Events.made_ft_team, True),
]


@pytest.mark.parametrize(
    "event_parser,minute,score,pre_ev,expected",
    IS_END_OF_GAME_FOULING_CASES,
    ids=[str(i) for i in range(len(IS_END_OF_GAME_FOULING_CASES))],
)
def test_is_end_of_game_fouling_vs_fastbreak(
    event_parser: PossessionEvent, minute: float, score: str, pre_ev: RawGameEvent, expected: bool
) -> None:
    ev = replace(_sub_score(score, pre_ev), min=minute)
    assert is_end_of_game_fouling_vs_fastbreak(ConcurrentClump([ev]), event_parser) == expected
