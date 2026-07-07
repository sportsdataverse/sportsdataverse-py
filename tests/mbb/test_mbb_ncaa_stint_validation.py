"""Tests for :mod:`sportsdataverse.mbb.mbb_ncaa_stint_validation` (Tasks 5d.1/5d.2).

``test_validate_lineup_oracle`` is a 1:1 transliteration of the
``"validate_lineup"`` block in ``LineupErrorAnalysisUtilsTests.scala``
(``:55-120``, read-only cbb-explorer clone) -- the ONLY upstream oracle for
``LineupErrorAnalysisUtils.scala``'s validation half.

Every other test in this file (``clump_bad_lineups`` / ``categorize_bad_lineups``,
Task 5d.2) has **NO upstream oracle** -- the Scala's own doc comment on
``clump_bad_lineups`` reads "TODO test". Each fixture's expected output is
HAND-DERIVED from the Scala fold at ``LineupErrorAnalysisUtils.scala:229-263``
(walked step-by-step in each test's docstring) BEFORE the port was run against
it -- never produced by running the port itself (that would be a tautology).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PlayerCodeId,
    PlayerId,
    RawGameEvent,
    ScoreInfo,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    BadLineupClump,
    ValidationError,
    categorize_bad_lineups,
    clump_bad_lineups,
    validate_lineup,
)
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code

# --- Fixture builders for the clump_bad_lineups / categorize_bad_lineups
# tests below (Task 5d.2, no upstream oracle -- see module docstring). ---

_TEAM_A = TeamSeasonId(TeamId("TeamA"), Year(2024))
_TEAM_B = TeamSeasonId(TeamId("TeamB"), Year(2024))
_OPP = TeamSeasonId(TeamId("Opp"), Year(2024))
_OPP2 = TeamSeasonId(TeamId("Opp2"), Year(2024))


def _make_event(
    *,
    team: TeamSeasonId = _TEAM_A,
    opponent: TeamSeasonId = _OPP,
    start_min: float,
    end_min: float,
    n_players: int = 6,
    n_in: int = 1,
    n_out: int = 1,
    num_possessions: int = 0,
) -> LineupEvent:
    """A minimal :class:`LineupEvent`, varying only the fields
    ``clump_bad_lineups``'s 5-condition predicate reads (``team``,
    ``opponent``, ``start_min``/``end_min``, ``len(players)``,
    ``len(players_in)``/``len(players_out)``) plus ``num_possessions`` for
    the ``categorize_bad_lineups`` smoke test. ``n_in``/``n_out`` default to
    a balanced 1/1 sub so only the fixture under test needs to override them.
    """
    players = [PlayerCodeId(f"P{i}", PlayerId(f"Player{i}")) for i in range(n_players)]
    team_stats = LineupEventStats.empty()
    team_stats.num_possessions = num_possessions
    return LineupEvent(
        date=datetime(2024, 1, 1),
        location_type=LocationType.HOME,
        start_min=start_min,
        end_min=end_min,
        duration_mins=end_min - start_min,
        score_info=ScoreInfo.empty(),
        team=team,
        opponent=opponent,
        lineup_id=LineupId.unknown,
        players=players,
        players_in=players[:n_in],
        players_out=players[:n_out],
        raw_game_events=[],
        team_stats=team_stats,
        opponent_stats=LineupEventStats.empty(),
    )


def test_validate_lineup_oracle() -> None:
    """Transliterated from ``LineupErrorAnalysisUtilsTests.scala:55-120``.

    Builds 8 player codes (``build_player_code(name, None)``, matching the
    oracle's ``ExtractorUtils.build_player_code(_, None)``), a
    ``base_lineup`` template (``:72-88``), 6 lineup variants derived from it
    (``:90-100``), and asserts ``validate_lineup(...).toList`` for each --
    ported here as a plain list-equality assert (the oracle's ``.toList`` on
    an ``Enumeration`` ``Set`` always yields declaration order, which is
    exactly what this port's :func:`~sportsdataverse.mbb
    .mbb_ncaa_stint_validation.validate_lineup` returns natively -- see that
    module's "Return shape" docstring note).
    """
    player_names = [
        "Player One",
        "Player Two",
        "Player Three",
        "Player Four",
        "Player Five",
        "Player Six",
        "Player Seven",
    ]
    all_players = [build_player_code(name, None) for name in player_names]
    player1, player2, player3, player4, player5, _player6, _player7 = all_players
    all_player_set = {p.code for p in all_players}
    player8 = build_player_code("Player Eight", None)

    valid_players = [player1, player2, player3, player4, player5]
    too_few_players = [player1, player2, player3, player4]
    unknown_player = [player1, player2, player3, player4, player8]
    multi_bad = [player8] + valid_players

    my_team = TeamSeasonId(TeamId("TestTeam1"), Year(2017))
    other_team = TeamSeasonId(TeamId("TestTeam2"), Year(2017))
    base_lineup = LineupEvent(
        date=datetime.now(),
        location_type=LocationType.HOME,
        start_min=0.0,
        end_min=-100.0,
        duration_mins=0.0,
        score_info=ScoreInfo.empty(),
        team=my_team,
        opponent=other_team,
        lineup_id=LineupId.unknown,
        players=[],
        players_in=[],
        players_out=[],
        raw_game_events=[],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )

    good_lineup = replace(base_lineup, players=valid_players)
    lineup_too_many = replace(base_lineup, players=all_players)
    lineup_too_few = replace(base_lineup, players=too_few_players)
    lineup_unknown_player = replace(base_lineup, players=unknown_player)
    lineup_multi_bad = replace(base_lineup, players=multi_bad)
    lineup_inactive = replace(
        base_lineup,
        players=valid_players,
        raw_game_events=[RawGameEvent.for_team("0:00,0-0,PLAYER,BAD Does Stuff", 0.0)],
    )

    assert validate_lineup(good_lineup, base_lineup, all_player_set) == []
    assert validate_lineup(lineup_too_many, base_lineup, all_player_set) == [
        ValidationError.WRONG_NUMBER_OF_PLAYERS,
    ]
    assert validate_lineup(lineup_too_few, base_lineup, all_player_set) == [
        ValidationError.WRONG_NUMBER_OF_PLAYERS,
    ]
    assert validate_lineup(lineup_unknown_player, base_lineup, all_player_set) == [
        ValidationError.UNKNOWN_PLAYERS,
    ]
    assert validate_lineup(lineup_multi_bad, base_lineup, all_player_set) == [
        ValidationError.WRONG_NUMBER_OF_PLAYERS,
        ValidationError.UNKNOWN_PLAYERS,
    ]
    assert validate_lineup(lineup_inactive, base_lineup, all_player_set) == [
        ValidationError.INACTIVE_PLAYERS,
    ]


# --- clump_bad_lineups: each of the 5 adjacency-predicate conditions,
# individually breaking a clump (Scala ``LineupErrorAnalysisUtils.scala:242-249``). ---


def test_clump_bad_lineups_breaks_on_team_mismatch() -> None:
    """Derivation: fold starts empty -> clumps=[Clump([ev1], None)] (base case,
    ``:234``). Step 2 (ev2): last=ev1; condition 1 (``lineup.team==last.team``)
    is ``TeamB==TeamA`` -> False, so the whole conjunction is False regardless
    of the other 4 conditions -> new clump (``:252-254``, the "else" branch).
    Result: 2 single-event clumps, in order.
    """
    ev1 = _make_event(team=_TEAM_A, start_min=0.0, end_min=5.0)
    ev2 = _make_event(team=_TEAM_B, start_min=5.0, end_min=10.0)

    clumps = clump_bad_lineups([(ev1, None), (ev2, None)])

    assert clumps == [BadLineupClump([ev1], None), BadLineupClump([ev2], None)]


def test_clump_bad_lineups_breaks_on_opponent_mismatch() -> None:
    """Derivation: same as the team-mismatch case but condition 2
    (``lineup.opponent==last.opponent``) is the one that fails
    (``Opp2==Opp`` -> False); conditions 1/3/4/5 all hold. Since the
    conjunction requires ALL 5, one failure is enough -> new clump.
    """
    ev1 = _make_event(opponent=_OPP, start_min=0.0, end_min=5.0)
    ev2 = _make_event(opponent=_OPP2, start_min=5.0, end_min=10.0)

    clumps = clump_bad_lineups([(ev1, None), (ev2, None)])

    assert clumps == [BadLineupClump([ev1], None), BadLineupClump([ev2], None)]


def test_clump_bad_lineups_breaks_on_time_gap() -> None:
    """Derivation: condition 3 (``lineup.start_min==last.end_min``) fails --
    ev1 ends at 5.0 but ev2 starts at 7.0 (a 2-minute gap, e.g. a dropped
    event) -> ``7.0 != 5.0`` -> new clump.
    """
    ev1 = _make_event(start_min=0.0, end_min=5.0)
    ev2 = _make_event(start_min=7.0, end_min=12.0)

    clumps = clump_bad_lineups([(ev1, None), (ev2, None)])

    assert clumps == [BadLineupClump([ev1], None), BadLineupClump([ev2], None)]


def test_clump_bad_lineups_breaks_on_player_count_mismatch() -> None:
    """Derivation: condition 4 (``len(lineup.players)==len(last.players)``)
    fails -- ev1 has 6 players on the floor, ev2 has 5 -> ``5 != 6`` -> new
    clump. Everything else (team/opponent/adjacent-time/own-balance) holds.
    """
    ev1 = _make_event(start_min=0.0, end_min=5.0, n_players=6)
    ev2 = _make_event(start_min=5.0, end_min=10.0, n_players=5)

    clumps = clump_bad_lineups([(ev1, None), (ev2, None)])

    assert clumps == [BadLineupClump([ev1], None), BadLineupClump([ev2], None)]


def test_clump_bad_lineups_breaks_on_unbalanced_incoming_sub() -> None:
    """Derivation: condition 5 (``len(lineup.players_in)==len(lineup.players_out)``)
    checks the INCOMING event's OWN in/out balance, not a comparison against
    ``last`` (Scala comment at ``:247``: "once you get a sub mismatch it
    becomes a bit hard to reason"). ev2 has 2 players_in but only 1
    players_out -> ``2 != 1`` -> new clump, even though ev1/ev2 otherwise
    satisfy conditions 1-4 (same team/opponent, adjacent time, same size).
    """
    ev1 = _make_event(start_min=0.0, end_min=5.0, n_in=1, n_out=1)
    ev2 = _make_event(start_min=5.0, end_min=10.0, n_in=2, n_out=1)

    clumps = clump_bad_lineups([(ev1, None), (ev2, None)])

    assert clumps == [BadLineupClump([ev1], None), BadLineupClump([ev2], None)]


def test_clump_bad_lineups_extends_when_all_5_conditions_hold() -> None:
    """Derivation: ev2 vs last=ev1 -- same team, same opponent,
    ``start_min(5.0)==last.end_min(5.0)``, same player count (6==6), and
    ev2's own ``players_in``/``players_out`` are balanced (1==1) -> ALL 5
    hold -> extend the existing clump (``:250-251``): ``evs`` becomes
    ``[ev1, ev2]`` (chronological order after the port's append-based
    reconstruction of the Scala's prepend-then-reverse) and ``next_good``
    is set to ev2's own ``next`` (``None`` here).
    """
    ev1 = _make_event(start_min=0.0, end_min=5.0)
    ev2 = _make_event(start_min=5.0, end_min=10.0)

    clumps = clump_bad_lineups([(ev1, None), (ev2, None)])

    assert clumps == [BadLineupClump([ev1, ev2], None)]


def test_clump_bad_lineups_multi_clump_sequence() -> None:
    """Derivation, 3-event walk: ev1 seeds clump1=[ev1] (base case). ev2 vs
    last=ev1: all 5 hold (same team A, adjacent time 5.0==5.0, same size,
    balanced sub) -> extends -> clump1=[ev1, ev2]. ev3 vs last=ev2: team B
    != team A -> condition 1 fails -> new clump2=[ev3]. Final: 2 clumps,
    ``[Clump([ev1, ev2]), Clump([ev3])]`` -- the first clump has 2 events
    (the "one multi-event" clump), the second has 1.
    """
    ev1 = _make_event(team=_TEAM_A, start_min=0.0, end_min=5.0)
    ev2 = _make_event(team=_TEAM_A, start_min=5.0, end_min=10.0)
    ev3 = _make_event(team=_TEAM_B, start_min=10.0, end_min=15.0)

    clumps = clump_bad_lineups([(ev1, None), (ev2, None), (ev3, None)])

    assert clumps == [BadLineupClump([ev1, ev2], None), BadLineupClump([ev3], None)]


def test_clump_bad_lineups_empty_input() -> None:
    """Derivation: the Scala ``foldLeft`` over an empty list never invokes
    any case and returns the zero value (``List[BadLineupClump]()``)
    unchanged; ``.map(...)`` and ``.reverse`` on an empty list are both
    no-ops. Result: ``[]``.
    """
    assert clump_bad_lineups([]) == []


def test_clump_bad_lineups_single_element() -> None:
    """Derivation: only the base case fires (``:234``,
    ``case (Nil, (lineup, next)) => List(BadLineupClump(lineup :: Nil, next))``)
    -- one clump containing just ``ev1``, with ``next_good`` set to the
    pair's own ``next`` (here a distinct sentinel event, ``next1``, to prove
    it's threaded through and not dropped).
    """
    ev1 = _make_event(start_min=0.0, end_min=5.0)
    next1 = _make_event(start_min=100.0, end_min=101.0)

    clumps = clump_bad_lineups([(ev1, next1)])

    assert clumps == [BadLineupClump([ev1], next1)]
    assert clumps[0].next_good is next1


def test_clump_bad_lineups_next_good_threading_uses_last_extension() -> None:
    """Derivation (the ``next_good`` threading rule, Scala ``:251``): step 1
    seeds ``clump=Clump([ev1], next1)``. Step 2 (ev2) extends the clump (all
    5 conditions hold, per the "extends" derivation above) via
    ``BadLineupClump(lineup :: clump_evs, next)`` -- ``next`` here is ev2's
    OWN pair-partner, ``next2``, which REPLACES ``next1`` rather than being
    combined with it. The final clump's ``next_good`` is therefore ``next2``,
    the LAST-extended event's ``next`` -- never ``next1``, even though
    ``next1`` was seen first.
    """
    ev1 = _make_event(start_min=0.0, end_min=5.0)
    ev2 = _make_event(start_min=5.0, end_min=10.0)
    next1 = _make_event(start_min=100.0, end_min=101.0)
    next2 = _make_event(start_min=200.0, end_min=201.0)

    clumps = clump_bad_lineups([(ev1, next1), (ev2, next2)])

    assert len(clumps) == 1
    assert clumps[0].next_good is next2
    assert clumps[0].next_good is not next1


# --- categorize_bad_lineups: display-only aggregation (Scala ``:617-633``,
# "can live without tests" per the Scala doc comment) -- smoke coverage only. ---


def test_categorize_bad_lineups_smoke() -> None:
    """Derivation: ``[ev_a, ev_b, ev_c]`` re-clumps (each paired with
    ``next_good=None``, matching the Scala's own ``.map(e => (e, None))``)
    to ``[Clump([ev_a, ev_b]), Clump([ev_c])]`` -- ev_a/ev_b share team A,
    are time-adjacent (5.0==5.0), both have 5 players, and are individually
    balanced (1 in/1 out) -> extend; ev_c has 6 players, a size mismatch vs
    ev_b's 5 -> new clump.

    Grouping key = the FIRST event's player count per clump: clump 1's
    ``evs[0]`` (ev_a) has 5 players -> key 5; clump 2's ``evs[0]`` (ev_c) has
    6 -> key 6. Per key: ``(num_clumps, total_possessions)`` --
    key 5 -> ``(1, ev_a.num_possessions[2] + ev_b.num_possessions[3] == 5)``;
    key 6 -> ``(1, ev_c.num_possessions[10])``.
    """
    ev_a = _make_event(n_players=5, start_min=0.0, end_min=5.0, num_possessions=2)
    ev_b = _make_event(n_players=5, start_min=5.0, end_min=10.0, num_possessions=3)
    ev_c = _make_event(n_players=6, start_min=100.0, end_min=105.0, num_possessions=10)

    result = categorize_bad_lineups([ev_a, ev_b, ev_c])

    assert result == {5: (1, 5), 6: (1, 10)}
