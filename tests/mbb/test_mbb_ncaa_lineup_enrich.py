"""Oracle-replay tests for ``sportsdataverse.mbb.mbb_ncaa_lineup_enrich``
(Task 5c.1).

Transliterated 1:1 from ``LineupUtilsTests.scala`` (utest,
``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``): the ``"enrich_lineup"``
block (``:61-80``), the ``"fix_possible_score_swap_bug"`` block (``:81-208``,
incl. the real-game-4690813 fixture), the ``"enrich_stats"`` block's
table-driven per-event-type cases (``:209-449``, TOTAL-only dispatch --
scramble/transition tagging lands in Tasks 5c.2/5c.3) plus its two
assist-pairing cases (``assist_rim_test``/``assist_mid_test``, ``:411-449``),
and the ``"add_stats_to_lineups"`` block (``:451-460``).

The ``Events`` fixture is reused (imported) from
``test_mbb_ncaa_possessions.py`` rather than redefined -- it already carries
every raw-event literal this block needs, transliterated from
``PossessionUtilsTests.Events``.

The oracle's block-3 comparison strips scramble/transition (``.orb``)
markers from the actual result before comparing against a total-only
expected value, because the *real* Scala ``enrich_stats`` already has
``is_scramble``/``is_transition`` wired in. This port's ``enrich_stats``
doesn't wire those in until Tasks 5c.2/5c.3 -- it never touches ``.orb``/
``.early`` at all yet -- so no such stripping is needed here: the actual
result is asserted directly against a total-only expected value.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any

import pytest

from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import (
    add_stats_to_lineups,
    enrich_lineup,
    enrich_stats,
    ensure_ev_uniqueness,
    fix_possible_score_swap_bug,
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
