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
    add_missing_players,
    analyze_and_fix_clumps,
    categorize_bad_lineups,
    clump_bad_lineups,
    find_missing_subs,
    handle_common_sub_bug,
    validate_lineup,
)
from sportsdataverse.mbb.mbb_ncaa_stints import build_lineup_id, build_player_code

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


# ===========================================================================
# Task 5d.3: self-healing fixers (handle_common_sub_bug / find_missing_subs /
# add_missing_players / analyze_and_fix_clumps).
#
# NO upstream oracle -- every expected output below was HAND-DERIVED from the
# Scala algorithm (``LineupErrorAnalysisUtils.scala:269-610``) on paper BEFORE
# the port was run against it (candidate-set evolution per event,
# ``matching_index``, per-phase routing, ``validate_lineup`` outcomes). The
# derivation is walked step-by-step in each test's docstring. The only use of
# the shipped code during fixture design was pinning the *leaf* name->code
# mapping (``build_player_code``/``tidy_player``/``parse_any_play`` -- all
# oracle-backed 5a/5b helpers, NOT the 5d.3 code under test); the fixer
# algorithm's behavior was never produced by running the fixer.
#
# Player codes (build_player_code(name, TeamId("TeamA")), verified via the
# shipped 5a/5b helpers): A=AaAaronson, B=BeBellson, C=CaCarlson, D=DaDanson,
# E=EtEthanson, F=FrFrankson, G=GaGarrison.
# ===========================================================================

_FIXER_TEAM_ID = _TEAM_A.team  # TeamId("TeamA")
_FIXER_NAMES = {
    "A": "Aaron Aaronson",
    "B": "Bella Bellson",
    "C": "Carl Carlson",
    "D": "Dana Danson",
    "E": "Ethan Ethanson",
    "F": "Frank Frankson",
    "G": "Gary Garrison",
}
#: Player-code map, keyed by single-letter handle -> PlayerCodeId.
_PC = {k: build_player_code(v, _FIXER_TEAM_ID) for k, v in _FIXER_NAMES.items()}
#: Code strings, keyed by handle (A -> "AaAaronson", ...).
_CODE = {k: pc.code for k, pc in _PC.items()}


def _codes(players: list[PlayerCodeId]) -> set[str]:
    """The set of ``.code`` strings for a lineup's players."""
    return {p.code for p in players}


def _mention(handle: str) -> RawGameEvent:
    """A team-side raw play line naming player ``handle`` in the new NCAA
    format (``"time,score,Full Name, verb ..."``) -- ``parse_any_play``
    extracts ``"Full Name"``, which resolves (via ``tidy_player`` ->
    ``build_player_code``) back to ``_PC[handle]``'s code (round-trip verified
    against the shipped helpers).
    """
    return RawGameEvent.for_team(f"10:00,0-0,{_FIXER_NAMES[handle]}, made Layup", 10.0)


def _fev(
    handles: list[str],
    *,
    players_in: list[str] | None = None,
    players_out: list[str] | None = None,
    mentions: list[str] | None = None,
    start_min: float = 0.0,
    end_min: float = 1.0,
) -> LineupEvent:
    """A ``LineupEvent`` on ``TeamA`` whose ``players``/``players_in``/
    ``players_out`` are the ``_PC`` players for the given handles and whose
    team-side raw events are ``_mention``\\ s for the given handles.
    """
    return LineupEvent(
        date=datetime(2024, 1, 1),
        location_type=LocationType.HOME,
        start_min=start_min,
        end_min=end_min,
        duration_mins=end_min - start_min,
        score_info=ScoreInfo.empty(),
        team=_TEAM_A,
        opponent=_OPP,
        lineup_id=LineupId.unknown,
        players=[_PC[h] for h in handles],
        players_in=[_PC[h] for h in (players_in or [])],
        players_out=[_PC[h] for h in (players_out or [])],
        raw_game_events=[_mention(h) for h in (mentions or [])],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )


def _fbox(handles: list[str]) -> LineupEvent:
    """A box-score ``LineupEvent`` (roster = the ``_PC`` players for the given
    handles), used as the name-resolution context + roster for the fixers.
    """
    return _fev(handles)


def _valid(handles: list[str]) -> set[str]:
    """The set of valid player codes for the given roster handles."""
    return {_CODE[h] for h in handles}


# --- handle_common_sub_bug (Scala ``:269-298``) ---


def test_handle_common_sub_bug_guard_hit_fix_accepted() -> None:
    """Derivation (guard hit, fix accepted).

    clump = single event ``bad`` (players [A,B,C,D,E,F], players_in [A,B]
    size 2, players_out [C] size 1), next_good = ``good`` (players_in []
    size 0, players_out [F] size 1). Guard (``:275-278``):
    ``len(in)=2 > len(out)=1`` True; ``len(good.in)=0`` True;
    ``len(good.out)=1 > 0`` True -> FIRES.

    Fix (``:279-283``): all_players = bad.players filterNot good.players_out
    {F} = [A,B,C,D,E] (5). players_out = distinct([C] ++ [F]) = [C,F].
    validate([A,B,C,D,E], box=[A..F], valid={A..F}): len 5, all known,
    no raws -> [] empty -> ACCEPTED (``:284-291``).

    Result: fixed = [event with players [A,B,C,D,E], players_out [C,F]];
    still_to_fix = empty clump (BadLineupClump([], None)).
    """
    bad = _fev(["A", "B", "C", "D", "E", "F"], players_in=["A", "B"], players_out=["C"])
    good = _fev(["A", "B", "C", "D", "E"], players_in=[], players_out=["F"])
    clump = BadLineupClump([bad], good)

    fixed, still = handle_common_sub_bug(
        clump, _fbox(["A", "B", "C", "D", "E", "F"]), _valid(["A", "B", "C", "D", "E", "F"])
    )

    assert len(fixed) == 1
    assert _codes(fixed[0].players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["E"]}
    assert fixed[0].players_out == [_PC["C"], _PC["F"]]
    assert still.evs == []
    assert still.next_good is None


def test_handle_common_sub_bug_guard_hit_validate_rejects() -> None:
    """Derivation (guard hit, but the fix still fails validate).

    clump = single event ``bad`` (players [A,B,C,D,E,F,G] -- SEVEN,
    players_in [A,B] size 2, players_out [C] size 1); next_good ``good``
    (players_in [] size 0, players_out [G] size 1). Guard fires (2>1, 0, 1>0).

    Fix: all_players = [A,B,C,D,E,F,G] filterNot {G} = [A,B,C,D,E,F] (SIX);
    players_out = distinct([C] ++ [G]) = [C,G]. validate([A,B,C,D,E,F]):
    len 6 -> WrongNumberOfPlayers -> non-empty -> REJECTED (``:292-293``).

    Result: fixed = []; still_to_fix = BadLineupClump([the *fixed* event],
    good) -- the still-to-fix event carries the MUTATED players/players_out,
    not the original, and keeps ``good`` as next_good.
    """
    bad = _fev(["A", "B", "C", "D", "E", "F", "G"], players_in=["A", "B"], players_out=["C"])
    good = _fev(["A", "B", "C", "D", "E"], players_in=[], players_out=["G"])
    clump = BadLineupClump([bad], good)

    fixed, still = handle_common_sub_bug(
        clump, _fbox(["A", "B", "C", "D", "E", "F", "G"]), _valid(["A", "B", "C", "D", "E", "F", "G"])
    )

    assert fixed == []
    assert len(still.evs) == 1
    assert _codes(still.evs[0].players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["E"], _CODE["F"]}
    assert still.evs[0].players_out == [_PC["C"], _PC["G"]]
    assert still.next_good is good


def test_handle_common_sub_bug_guard_miss_multi_event() -> None:
    """Derivation (guard miss -- clump is not single-event).

    The Scala pattern ``BadLineupClump(bad :: Nil, Some(good))`` requires
    EXACTLY one event; a 2-event clump falls through to ``case _ => (Nil,
    clump)`` (``:296``). Result: fixed = []; still_to_fix = the input clump
    unchanged.
    """
    ev1 = _fev(["A", "B", "C", "D", "E", "F"], players_in=["A", "B"], players_out=["C"])
    ev2 = _fev(["A", "B", "C", "D", "E", "F"], players_in=["A", "B"], players_out=["C"])
    good = _fev(["A", "B", "C", "D", "E"], players_in=[], players_out=["F"])
    clump = BadLineupClump([ev1, ev2], good)

    fixed, still = handle_common_sub_bug(
        clump, _fbox(["A", "B", "C", "D", "E", "F"]), _valid(["A", "B", "C", "D", "E", "F"])
    )

    assert fixed == []
    assert still is clump
    assert len(still.evs) == 2


def test_handle_common_sub_bug_guard_miss_no_next_good() -> None:
    """Derivation (guard miss -- no next_good).

    The pattern requires ``Some(good)``; a clump with ``next_good = None``
    falls through to ``case _ => (Nil, clump)``. Result: fixed = [];
    still_to_fix = the input clump unchanged.
    """
    bad = _fev(["A", "B", "C", "D", "E", "F"], players_in=["A", "B"], players_out=["C"])
    clump = BadLineupClump([bad], None)

    fixed, still = handle_common_sub_bug(
        clump, _fbox(["A", "B", "C", "D", "E", "F"]), _valid(["A", "B", "C", "D", "E", "F"])
    )

    assert fixed == []
    assert still is clump
    assert still.next_good is None


def test_handle_common_sub_bug_guard_miss_balanced_subs() -> None:
    """Derivation (guard miss -- the in>out size condition fails).

    Single event, next_good present, but ``len(bad.players_in)=1`` is NOT
    ``> len(bad.players_out)=1`` -> the ``if`` guard (``:276``) is False ->
    ``case _ => (Nil, clump)``. Result: fixed = []; still_to_fix unchanged.
    """
    bad = _fev(["A", "B", "C", "D", "E", "F"], players_in=["A"], players_out=["B"])
    good = _fev(["A", "B", "C", "D", "E"], players_in=[], players_out=["F"])
    clump = BadLineupClump([bad], good)

    fixed, still = handle_common_sub_bug(
        clump, _fbox(["A", "B", "C", "D", "E", "F"]), _valid(["A", "B", "C", "D", "E", "F"])
    )

    assert fixed == []
    assert still is clump


# --- find_missing_subs (Scala ``:406-514``) ---


def test_find_missing_subs_mid_clump_match_both_phases() -> None:
    """Derivation (6-player clump, mid-clump matching_index, BOTH phases).

    box = roster [A,B,C,D,E,F,G], valid = {A..G}. next_good = None.
    clump.evs (all raws empty, so shrinking is driven purely by sub-outs):

        ev0: players [A,B,C,D,E,F], players_out []      (head)
        ev1: players [A,B,C,D,E,F], players_out [A,B]
        ev2: players [A,B,C,D,E,F], players_out [C,D]
        ev3: players [A,B,C,D,E,F], players_out [E]
        ev4: players [A,B,C,D,E,F], players_in [G], players_out [E]

    Phase 1 (candidates = first-event players):
      candidates = {A,B,C,D,E,F} (6); expected_size_diff = 6 - 5 = 1.
      idx0 ev0 (head -> sub-outs skipped, no raws): {A,B,C,D,E,F} (6). 6==1? no.
      idx1: sub-out [A,B] -> {C,D,E,F} (4). ==1? no.
      idx2: sub-out [C,D] -> {E,F} (2). ==1? no.
      idx3: sub-out [E]   -> {F} (1). ==1? YES -> matching_index = 3.
      idx4: phase 2 (frozen), candidates stay {F}.
      filtered = {F}; matching_index = 3.
    Accept gate: non-empty AND size 1 <= expected 1 -> ACCEPT.

    Phase 3 (index > matching_index=3 is the "after" branch):
      idx0..3 (before/at match): players filterNot {F} -> [A,B,C,D,E] (5).
      idx4 (after match): rebuild via build_new_player_list(ev4, prev tidied
        ev3 = [A,B,C,D,E]); ev4 subs OUT [E] IN [G] -> poss1 =
        [A,B,C,D,E] - {E} + {G} = [A,B,C,D,G] (5) -> wins. So ev4 -> [A,B,C,D,G].
      -> the "after" branch produces {A,B,C,D,G}, distinct from the "before"
      branch's {A,B,C,D,E}, which PROVES both phases ran.

    Partition: all 5 rebuilt events have 5 known players, no raws -> all valid.
    Result: fixed = 5 events; still_to_fix = BadLineupClump([], None).
    """
    ev0 = _fev(["A", "B", "C", "D", "E", "F"])
    ev1 = _fev(["A", "B", "C", "D", "E", "F"], players_out=["A", "B"])
    ev2 = _fev(["A", "B", "C", "D", "E", "F"], players_out=["C", "D"])
    ev3 = _fev(["A", "B", "C", "D", "E", "F"], players_out=["E"])
    ev4 = _fev(["A", "B", "C", "D", "E", "F"], players_in=["G"], players_out=["E"])
    clump = BadLineupClump([ev0, ev1, ev2, ev3, ev4], None)

    fixed, still = find_missing_subs(
        clump, _fbox(["A", "B", "C", "D", "E", "F", "G"]), _valid(["A", "B", "C", "D", "E", "F", "G"])
    )

    assert len(fixed) == 5
    # ev0..ev3 (before/at match) trimmed the ghost F -> {A,B,C,D,E}.
    for ev in fixed[:4]:
        assert _codes(ev.players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["E"]}
    # ev4 (after match) rebuilt via build_new_player_list -> {A,B,C,D,G}.
    assert _codes(fixed[4].players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["G"]}
    assert still.evs == []


def test_find_missing_subs_noop_below_six() -> None:
    """Derivation (no-op: first event has < 6 players).

    candidates = {A,B,C,D,E} (5) < 6 -> the ``candidates.size < 6`` guard
    (``:416``) returns ``(Nil, clump)`` immediately. Result: fixed = [];
    still_to_fix = the input clump unchanged.
    """
    ev = _fev(["A", "B", "C", "D", "E"])
    clump = BadLineupClump([ev], None)

    fixed, still = find_missing_subs(clump, _fbox(["A", "B", "C", "D", "E"]), _valid(["A", "B", "C", "D", "E"]))

    assert fixed == []
    assert still is clump


def test_find_missing_subs_reject_candidates_never_shrink() -> None:
    """Derivation (reject: matching_index stays None, pool never shrinks).

    Single 6-player event, no sub-outs, no raws:
      candidates = {A,B,C,D,E,F} (6); expected_size_diff = 1.
      idx0 (head): sub-outs skipped, no raws -> pool stays {A,B,C,D,E,F} (6).
      6 == 1? no -> matching_index stays None.
      filtered = {A,B,C,D,E,F} (6).
    Accept gate: non-empty (True) AND size 6 <= expected 1 (FALSE) -> REJECT
    (``:510-511``, ``(Nil, clump)``). Result: fixed = []; still_to_fix =
    input clump unchanged. (This is the ``matching_index is None`` path: with
    nothing confirmed, the residual pool exceeds ``expected_size_diff``.)
    """
    ev = _fev(["A", "B", "C", "D", "E", "F"])
    clump = BadLineupClump([ev], None)

    fixed, still = find_missing_subs(
        clump, _fbox(["A", "B", "C", "D", "E", "F"]), _valid(["A", "B", "C", "D", "E", "F"])
    )

    assert fixed == []
    assert still is clump


# --- add_missing_players (Scala ``:315-401``) ---


def test_add_missing_players_next_good_seed_contributes() -> None:
    """Derivation (4-player clump, next_good sub-out seeds the add).

    box = [A,B,C,D,E], valid = {A,B,C,D,E}. clump = single event ev
    (players [A,B,C,D], no subs, no raws); next_good = event with
    players_out [E] (E is NOT on the clump's floor).

    players_in (first-ev players) = {A,B,C,D} (4) <= 4 -> FIRES.
    candidates = box.players {A,B,C,D,E} filterNot {A,B,C,D} = {E}.
    all_clump_players = {A,B,C,D}. initial_candidates (seed) =
    next_good.players_out [E] filterNot {A,B,C,D} = {E}. players_to_add = [E].
    Walk ev: new_candidates = {E} - players_in([]) = {E}; no raws -> nothing
    added. players_to_add = [E] (non-empty).
    Augment: ev.players [A,B,C,D] ++ [E] = [A,B,C,D,E] (5) -> validate ok.
    Result: fixed = [event with [A,B,C,D,E]]; still = BadLineupClump([],
    next_good).
    """
    ev = _fev(["A", "B", "C", "D"])
    good = _fev(["A", "B", "C", "D", "E"], players_out=["E"])
    clump = BadLineupClump([ev], good)

    fixed, still = add_missing_players(clump, _fbox(["A", "B", "C", "D", "E"]), _valid(["A", "B", "C", "D", "E"]))

    assert len(fixed) == 1
    assert _codes(fixed[0].players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["E"]}
    assert still.evs == []
    assert still.next_good is good


def test_add_missing_players_mention_driven_add() -> None:
    """Derivation (4-player clump, next_good = None -- add driven by a raw
    play mention).

    box = [A,B,C,D,E], valid = {A,B,C,D,E}. clump = single event ev
    (players [A,B,C,D], a raw play mentioning E); next_good = None (so the
    seed is empty).

    players_in = {A,B,C,D} (4) <= 4 -> FIRES.
    candidates = {A,B,C,D,E} filterNot {A,B,C,D} = {E}.
    initial_candidates = [] (no next_good). players_to_add = [].
    Walk ev: new_candidates = {E}; raw mentions E -> parse_any_play ->
    tidy_player -> build_player_code = E's code, which is in new_candidates
    and not yet in players_to_add -> players_to_add = [E].
    Augment: [A,B,C,D] ++ [E] = [A,B,C,D,E] -> valid.
    Result: fixed = [event with [A,B,C,D,E]]; still = BadLineupClump([], None).
    """
    ev = _fev(["A", "B", "C", "D"], mentions=["E"])
    clump = BadLineupClump([ev], None)

    fixed, still = add_missing_players(clump, _fbox(["A", "B", "C", "D", "E"]), _valid(["A", "B", "C", "D", "E"]))

    assert len(fixed) == 1
    assert _codes(fixed[0].players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["E"]}
    assert still.evs == []


def test_add_missing_players_noop_above_four() -> None:
    """Derivation (no-op: first event already has 5 players).

    players_in = {A,B,C,D,E} (5) > 4 -> the ``players_in.size > 4`` guard
    (``:325``) returns ``(Nil, clump)``. Result: fixed = []; still_to_fix =
    input clump unchanged.
    """
    ev = _fev(["A", "B", "C", "D", "E"])
    clump = BadLineupClump([ev], None)

    fixed, still = add_missing_players(clump, _fbox(["A", "B", "C", "D", "E"]), _valid(["A", "B", "C", "D", "E"]))

    assert fixed == []
    assert still is clump


def test_add_missing_players_reject_nothing_to_add() -> None:
    """Derivation (reject: fires but finds no players to add).

    box = [A,B,C,D,E], clump = single event ev (players [A,B,C,D], a raw
    mentioning A -- who is ALREADY on the floor, hence NOT a candidate);
    next_good = None.

    players_in = {A,B,C,D} (4) <= 4 -> FIRES.
    candidates = {E}. initial_candidates = [] (no next_good).
    Walk ev: new_candidates = {E}; raw mentions A, whose code is NOT in
    new_candidates {E} -> not added. players_to_add = [] (empty) -> the
    ``players_to_add.nonEmpty`` guard (``:386``) is False -> ``(Nil, clump)``.
    Result: fixed = []; still_to_fix = input clump unchanged.
    """
    ev = _fev(["A", "B", "C", "D"], mentions=["A"])
    clump = BadLineupClump([ev], None)

    fixed, still = add_missing_players(clump, _fbox(["A", "B", "C", "D", "E"]), _valid(["A", "B", "C", "D", "E"]))

    assert fixed == []
    assert still is clump


# --- analyze_and_fix_clumps (Scala ``:556-610``) ---


def test_analyze_and_fix_clumps_second_find_missing_subs_pass_matters() -> None:
    """Derivation (the SECOND find_missing_subs pass fixes an
    add_missing_players over-add) + lineup_id recompute.

    box = [A,B,C,D,E,F], valid = {A,B,C,D,E,F}. clump = single event ev
    (players [A,B,C,D] -- FOUR, no subs, raws mentioning A,B,C,D,E);
    next_good = event with players_out [E,F], players_in [] .

    Stage 1 handle_common_sub_bug: single event + next_good, but
      ``len(bad.players_in)=0 > len(bad.players_out)=0`` is False -> guard
      miss -> (fixed=[], to_fix=clump unchanged).
    Stage 2 find_missing_subs (1st): first-ev players {A,B,C,D} (4) < 6 ->
      no-op -> (fixed=[], to_fix=clump).
    Stage 3 add_missing_players: players_in {A,B,C,D} (4) <= 4 -> FIRES.
      candidates = box {A,B,C,D,E,F} - {A,B,C,D} = {E,F}.
      initial_candidates (seed) = next_good.players_out [E,F] - clump {A,B,C,D}
        = {E,F}. players_to_add = [E,F].
      Walk ev: new_candidates = {E,F}; raws mention A,B,C,D,E -- of those only
        E is in {E,F}, and E is already in players_to_add -> no change.
      players_to_add = [E,F] (non-empty).
      Augment: [A,B,C,D] ++ [E,F] = [A,B,C,D,E,F] (SIX) -> WrongNumberOfPlayers
        -> lands in the BAD bucket. -> (newly_fixed=[], to_fix =
        BadLineupClump([ev' with [A,B,C,D,E,F]], next_good)).  <-- over-add.
    Stage 4 find_missing_subs (2nd): first-ev players {A,B,C,D,E,F} (6) >= 6
      -> FIRES. expected_size_diff = 1.
      idx0 (head -> sub-outs skipped): raws mention A,B,C,D,E, all in
        candidates -> removed; F is never mentioned -> pool = {F} (1) == 1 ->
        matching_index = 0. filtered = {F}. Accept (non-empty, 1<=1).
      Phase 3 idx0: (0 > 0) is False -> "before" branch -> players filterNot
        {F} -> [A,B,C,D,E] (5). validate ok. -> (newly_fixed=[ev'' with
        [A,B,C,D,E]], to_fix = BadLineupClump([], next_good)).
    Final: recompute lineup_id = build_lineup_id([A,B,C,D,E]) for the fixed
      event.

    Result: fixed = 1 event, players {A,B,C,D,E}, lineup_id =
    build_lineup_id([A,B,C,D,E]); still_to_fix = BadLineupClump([], next_good).
    """
    ev = _fev(["A", "B", "C", "D"], mentions=["A", "B", "C", "D", "E"])
    good = _fev(["A", "B", "C", "D", "E"], players_in=[], players_out=["E", "F"])
    clump = BadLineupClump([ev], good)

    fixed, still = analyze_and_fix_clumps(
        clump, _fbox(["A", "B", "C", "D", "E", "F"]), _valid(["A", "B", "C", "D", "E", "F"])
    )

    assert len(fixed) == 1
    assert _codes(fixed[0].players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["E"]}
    # lineup_id was recomputed from the trimmed 5-player list (not the stale
    # LineupId.unknown the fixture events carried).
    assert fixed[0].lineup_id == build_lineup_id([_PC["A"], _PC["B"], _PC["C"], _PC["D"], _PC["E"]])
    assert fixed[0].lineup_id != LineupId.unknown
    assert still.evs == []
    assert still.next_good is good


def test_analyze_and_fix_clumps_single_pass_fix_and_lineup_id() -> None:
    """Derivation (a straightforward single-stage fix, verifying the
    lineup_id recompute on the fixed lineup).

    box = [A,B,C,D,E], valid = {A,B,C,D,E}. clump = single event ev
    (players [A,B,C,D], no subs, no raws); next_good = event with
    players_out [E].

    Stage 1 handle_common_sub_bug: 0 > 0 False -> miss.
    Stage 2 find_missing_subs (1st): 4 players < 6 -> no-op.
    Stage 3 add_missing_players: FIRES (4 <= 4); candidates {E}; seed from
      next_good.players_out [E] = {E}; players_to_add = [E]; augment ->
      [A,B,C,D,E] (5) valid -> newly_fixed = [ev' with [A,B,C,D,E]],
      to_fix = BadLineupClump([], next_good).
    Stage 4 find_missing_subs (2nd): to_fix.evs == [] -> candidates = [] < 6
      -> no-op.
    Final: lineup_id recompute -> build_lineup_id([A,B,C,D,E]).

    Result: fixed = 1 event [A,B,C,D,E] with recomputed lineup_id;
    still_to_fix = BadLineupClump([], next_good).
    """
    ev = _fev(["A", "B", "C", "D"])
    good = _fev(["A", "B", "C", "D", "E"], players_out=["E"])
    clump = BadLineupClump([ev], good)

    fixed, still = analyze_and_fix_clumps(clump, _fbox(["A", "B", "C", "D", "E"]), _valid(["A", "B", "C", "D", "E"]))

    assert len(fixed) == 1
    assert _codes(fixed[0].players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["E"]}
    assert fixed[0].lineup_id == build_lineup_id([_PC["A"], _PC["B"], _PC["C"], _PC["D"], _PC["E"]])
    assert still.evs == []


def test_analyze_and_fix_clumps_mixed_some_fixed_some_bad() -> None:
    """Derivation (a mixed clump: one event gets fixed, one stays bad).

    box = [A,B,C,D,E,F,G], valid = {A,B,C,D,E,F,G}. next_good = None.
    clump.evs (6-player first event -> find_missing_subs path; raws empty so
    shrinking is sub-out-driven):

        ev0: players [A,B,C,D,E,F]                       (head)
        ev1: players [A,B,C,D,E,F], players_out [A,B,C,D,E]

    Stage 1 handle_common_sub_bug: 2-event clump -> guard miss (single-event
      only) -> to_fix = clump.
    Stage 2 find_missing_subs (1st): candidates {A,B,C,D,E,F} (6);
      expected_size_diff = 1.
      idx0 (head): no sub-outs, no raws -> pool {A,B,C,D,E,F} (6). ==1? no.
      idx1: sub-out [A,B,C,D,E] -> pool {F} (1) == 1 -> matching_index = 1.
      filtered = {F}; accept (1<=1).
      Phase 3:
        idx0 (0 > 1 False -> before): filterNot {F} -> [A,B,C,D,E] (5) -> VALID.
        idx1 (1 > 1 False -> before): filterNot {F} -> ev1.players
          [A,B,C,D,E,F] - {F} = [A,B,C,D,E] (5) -> VALID.
      Both valid -> newly_fixed = 2 events, to_fix = BadLineupClump([], None).

    Hmm -- both are valid, so to make the "mixed" case, ev1 instead carries a
    7th player G in its own ``players`` so that after removing the single
    ghost F it still has 6 players and stays BAD. Re-derive with:

        ev0: players [A,B,C,D,E,F]                       (head)
        ev1: players [A,B,C,D,E,F,G], players_out [A,B,C,D,E]

    Phase 1 uses ONLY the FIRST event for the candidate pool, so ev1's extra
    G does not change candidates {A,B,C,D,E,F} / expected 1 / matching_index
    1 / filtered {F} (G is not in the pool, so the [A..E] sub-out still
    shrinks the pool to {F}).
    Phase 3:
      idx0 (before): [A,B,C,D,E,F] - {F} = [A,B,C,D,E] (5) -> VALID -> fixed.
      idx1 (before, since 1 > 1 is False): [A,B,C,D,E,F,G] - {F} =
        [A,B,C,D,E,G] (6) -> WrongNumberOfPlayers -> BAD -> still_to_fix.
    Stage 3 add_missing_players on to_fix = BadLineupClump([ev1''], None):
      first-ev players {A,B,C,D,E,G} (6) > 4 -> no-op.
    Stage 4 find_missing_subs (2nd) on the same: 6 >= 6 -> FIRES, expected 1;
      idx0 (head, no sub-outs/raws) -> pool stays 6 -> matching None,
      filtered size 6 > 1 -> REJECT -> unchanged.
    Final lineup_id recompute applies to the ONE fixed event.

    Result: fixed = 1 event ([A,B,C,D,E], recomputed lineup_id);
    still_to_fix.evs = 1 event ([A,B,C,D,E,G]).
    """
    ev0 = _fev(["A", "B", "C", "D", "E", "F"])
    ev1 = _fev(["A", "B", "C", "D", "E", "F", "G"], players_out=["A", "B", "C", "D", "E"])
    clump = BadLineupClump([ev0, ev1], None)

    fixed, still = analyze_and_fix_clumps(
        clump, _fbox(["A", "B", "C", "D", "E", "F", "G"]), _valid(["A", "B", "C", "D", "E", "F", "G"])
    )

    assert len(fixed) == 1
    assert _codes(fixed[0].players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["E"]}
    assert fixed[0].lineup_id == build_lineup_id([_PC["A"], _PC["B"], _PC["C"], _PC["D"], _PC["E"]])
    assert len(still.evs) == 1
    assert _codes(still.evs[0].players) == {_CODE["A"], _CODE["B"], _CODE["C"], _CODE["D"], _CODE["E"], _CODE["G"]}
