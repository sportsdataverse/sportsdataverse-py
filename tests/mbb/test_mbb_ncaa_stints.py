"""Oracle tests for :mod:`sportsdataverse.mbb.mbb_ncaa_stints` (Tasks 5b.2/5b.4/5b.5).

Every case below is a 1:1 transliteration of an inline ``utest`` literal from
``ExtractorUtilsTests.scala`` in the read-only cbb-explorer clone:

* ``build_player_code`` block (``ExtractorUtilsTests.scala:15-117``) -- 10
  name -> code assertions (utest ``TestUtils.inside`` destructuring = exact
  equality on ``(code, PlayerId(name), ncaa_id=None)``).
* ``parse_team_name`` block (``ExtractorUtilsTests.scala:118-156``) -- 6
  ``Right((team, opp, is_first))`` assertions.
* ``reorder_and_reverse`` block (``ExtractorUtilsTests.scala:158-272``) --
  the trivial 2-element swap + the full 19-event reversed-input scenario,
  asserted against the oracle's exact flat string ordering (Task 5b.4).
* ``build_partial_lineup_list`` block (``ExtractorUtilsTests.scala:274-715``,
  Task 5b.5) -- all 3 scenarios: the main old-format-latch scenario (both
  2017/2018 years produce IDENTICAL output because the format latches on
  the first sub name's case, overriding the year-based default), the
  new-format carry-over regression, and the alt-name-format scenario that
  exercises the ``tidy_player`` fuzzy/alt-format chain (5b.3) end-to-end.
  ``start_time_from_period``/``duration_from_period`` have no Scala oracle
  in ``ExtractorUtilsTests`` -- their tests below are hand-written.

Upstream-test-bug note (``:86-115``): the Scala loops ``team`` over
``[None, Some(TeamId("TCU"))]`` but every call passes ``None``, and no TCU
entry exists in the misspellings table -- the ``if team.nonEmpty`` guard
cases are dead. The faithful transliteration therefore expects the
UNcorrected code/name on both iterations.

The ``PlayByPlayEvent`` ADT / ``LineupBuildingState`` have no standalone
Scala oracle beyond ``reorder_and_reverse`` (which only exercises
``with_min``/``score``/``event_string``/``player_name``/``is_team_dir``
indirectly) -- ``build_partial_lineup_list``'s oracle (below) is the first
end-to-end exercise of ``LineupBuildingState``. The ``test_*_with_min`` /
``test_lineup_building_state_*`` cases below are hand-written smoke tests,
each traced by hand against the ported algorithm.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError
from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PlayerCodeId,
    PlayerId,
    RawGameEvent,
    Score,
    ScoreInfo,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context
from sportsdataverse.mbb.mbb_ncaa_stints import (
    SUB_SAFETY_DELTA_MINS,
    GameBreakEvent,
    GameEndEvent,
    LineupBuildingState,
    OtherOpponentEvent,
    OtherTeamEvent,
    PlayByPlayEvent,
    SubInEvent,
    SubOutEvent,
    build_partial_lineup_list,
    build_player_code,
    duration_from_period,
    parse_team_name,
    remove_diacritics,
    reorder_and_reverse,
    start_time_from_period,
)

# --- build_player_code (jest.. utest :15-117) -------------------------------

BUILD_PLAYER_CODE_CASES: list[tuple[str, str, str]] = [
    # (input name, expected code, expected PlayerId name)
    (
        "Surname, F.irstname A B Iiii Iiiiaiii Jr Jr. Sr Sr. 4test the First second rAbbit Third",
        "FiRaSurname",
        "Surname, F.irstname A B Iiii Iiiiaiii Jr Jr. Sr Sr. 4test the First second rAbbit Third",
    ),
    ("Mitchell, Makhi", "MiMitchell", "Mitchell, Makhi"),  # duplicate-name -> first_last
    ("MAYER,M", "MMayer", "MAYER,M"),  # first names are never filtered
    ("Brown, Jr., Barry", "BaBrown", "Brown, Jr., Barry"),  # 3-part comma+suffix
    ("Dorka Juhász", "DoJuhasz", "Dorka Juhasz"),  # diacritics stripped in PlayerId
    ("MAMUKELASHVIL,SANDRO", "SaMamukelash", "MAMUKELASHVIL,SANDRO"),  # fragment cap
    ("BIGBY-WILLIAM,KAVELL", "KaBigby-will", "BIGBY-WILLIAM,KAVELL"),
    ("Kavell Bigby-Williams", "KaBigby-will", "Kavell Bigby-Williams"),
]


@pytest.mark.parametrize(("in_name", "code", "player_id"), BUILD_PLAYER_CODE_CASES)
def test_build_player_code_cases(in_name: str, code: str, player_id: str) -> None:
    """utest ``:15-84`` -- exact ``PlayerCodeId(code, PlayerId(name), None)``."""
    assert build_player_code(in_name, None) == PlayerCodeId(code=code, id=PlayerId(player_id))


@pytest.mark.parametrize("team", [None, TeamId("TCU")])
def test_build_player_code_misspelling_loop(team: TeamId | None) -> None:
    """utest ``:86-115`` -- the misspelling-fixer loop. The Scala always calls
    with ``team=None`` (its ``team`` loop var is unused in the call -- a latent
    upstream test bug) and no TCU misspelling entry exists, so the uncorrected
    branch is the one that fires on both iterations.
    """
    assert build_player_code("Dylan Ostekowski", None) == PlayerCodeId(
        code="DyOstekowski", id=PlayerId("Dylan Ostekowski")
    )
    assert build_player_code("Ostekowski, Dylan", None) == PlayerCodeId(
        code="DyOstekowski", id=PlayerId("Ostekowski, Dylan")
    )


def test_build_player_code_ncaa_id_is_none() -> None:
    """The Scala oracle destructures ``ncaa_id`` as ``None`` on every case."""
    assert build_player_code("Mitchell, Makhi", None).ncaa_id is None


def test_remove_diacritics() -> None:
    """Supplement (no standalone Scala oracle -- exercised via the Juhász
    case above): NFD + combining-mark strip."""
    assert remove_diacritics("Dorka Juhász") == "Dorka Juhasz"
    assert remove_diacritics("plain") == "plain"


# --- parse_team_name (utest :118-156) ---------------------------------------


def test_parse_team_name_plain_first() -> None:
    """utest ``:119-122``."""
    assert parse_team_name(["TeamA", "TeamB"], TeamId("TeamA"), Year(2018)) == (
        "TeamA",
        "TeamB",
        True,
    )


def test_parse_team_name_plain_second() -> None:
    """utest ``:123-126``."""
    assert parse_team_name(["TeamB", "TeamA"], TeamId("TeamA"), Year(2018)) == (
        "TeamA",
        "TeamB",
        False,
    )


def test_parse_team_name_seed_stripped() -> None:
    """utest ``:127-134``."""
    assert parse_team_name(["#1 TeamA", "#3 TeamB"], TeamId("TeamA"), Year(2018)) == (
        "TeamA",
        "TeamB",
        True,
    )


def test_parse_team_name_seed_and_record_stripped() -> None:
    """utest ``:135-142``."""
    assert parse_team_name(["#1 TeamA (1-1)", "TeamB (4-1)"], TeamId("TeamA"), Year(2018)) == ("TeamA", "TeamB", True)


def test_parse_team_name_alias_not_applied_other_year() -> None:
    """utest ``:143-147`` -- the NIU alias is scoped to Year(2021) only."""
    assert parse_team_name(["NIU", "TeamB"], TeamId("NIU"), Year(2018)) == (
        "NIU",
        "TeamB",
        True,
    )


def test_parse_team_name_alias_applied_2021() -> None:
    """utest ``:148-155`` -- NIU -> Northern Ill. for Year(2021), both slots."""
    assert parse_team_name(["NIU", "NIU"], TeamId("Northern Ill."), Year(2021)) == (
        "Northern Ill.",
        "Northern Ill.",
        True,
    )


def test_parse_team_name_failure_returns_parse_error() -> None:
    """Supplement (the Scala Left branch has no inline oracle case): a
    non-matching target yields a ``ParseError`` with the ``[team]`` id."""
    result = parse_team_name(["TeamB", "TeamC"], TeamId("TeamA"), Year(2018))
    assert isinstance(result, ParseError)
    assert result.id == "[team]"


# --- PlayByPlayEvent ADT `with_min` (supplement -- no standalone oracle) ----


def test_sub_in_event_with_min() -> None:
    ev = SubInEvent(1.0, Score(0, 0), "player1")
    assert ev.with_min(2.0) == SubInEvent(2.0, Score(0, 0), "player1")


def test_sub_out_event_with_min() -> None:
    ev = SubOutEvent(1.0, Score(0, 0), "player1")
    assert ev.with_min(2.0) == SubOutEvent(2.0, Score(0, 0), "player1")


def test_other_team_event_is_team_dir_and_with_min() -> None:
    """``ExtractorUtils.scala:864`` -- ``is_team_dir`` is a fixed ``True``."""
    ev = OtherTeamEvent(1.0, Score(0, 0), "foo")
    assert ev.is_team_dir is True
    assert ev.with_min(2.0) == OtherTeamEvent(2.0, Score(0, 0), "foo")


def test_other_opponent_event_is_team_dir_and_with_min() -> None:
    """``ExtractorUtils.scala:872`` -- ``is_team_dir`` is a fixed ``False``."""
    ev = OtherOpponentEvent(1.0, Score(0, 0), "foo")
    assert ev.is_team_dir is False
    assert ev.with_min(2.0) == OtherOpponentEvent(2.0, Score(0, 0), "foo")


def test_game_break_event_with_min() -> None:
    ev = GameBreakEvent(1.0, Score(0, 0))
    assert ev.with_min(2.0) == GameBreakEvent(2.0, Score(0, 0))


def test_game_end_event_with_min() -> None:
    ev = GameEndEvent(1.0, Score(0, 0))
    assert ev.with_min(2.0) == GameEndEvent(2.0, Score(0, 0))


# --- LineupBuildingState (hand-written smoke tests) -------------------------

_TEAM = TeamId("Test Team")


def _lineup_event(
    team: TeamId,
    *,
    start_min: float = 0.0,
    end_min: float = 0.0,
    players_in: list[PlayerCodeId] | None = None,
    players_out: list[PlayerCodeId] | None = None,
    raw_game_events: list[RawGameEvent] | None = None,
    score_info: ScoreInfo | None = None,
) -> LineupEvent:
    """Minimal :class:`LineupEvent` fixture, mirroring
    ``test_mbb_ncaa_names.py``'s ``_lineup_event`` helper -- only the fields
    :class:`LineupBuildingState`'s methods actually read/write are
    meaningful; the rest are filler."""
    return LineupEvent(
        date=datetime(2020, 1, 1),
        location_type=LocationType.HOME,
        start_min=start_min,
        end_min=end_min,
        duration_mins=0.0,
        score_info=score_info or ScoreInfo.empty(),
        team=TeamSeasonId(team, Year(2020)),
        opponent=TeamSeasonId(TeamId("Other Team"), Year(2020)),
        lineup_id=LineupId.unknown,
        players=[],
        players_in=players_in or [],
        players_out=players_out or [],
        raw_game_events=raw_game_events or [],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )


def _state(curr: LineupEvent, prev: list[LineupEvent] | None = None) -> LineupBuildingState:
    return LineupBuildingState(curr=curr, tidy_ctx=build_tidy_player_context(curr), prev=prev or [])


def test_lineup_building_state_build_restores_chronological_order() -> None:
    """``:742-744`` -- ``(curr :: prev).reverse``: ``prev`` is stored
    most-recently-completed-first; ``build()`` restores forward-time order."""
    e1 = _lineup_event(_TEAM, start_min=0.0, end_min=1.0)
    e2 = _lineup_event(_TEAM, start_min=1.0, end_min=2.0)
    curr = _lineup_event(_TEAM, start_min=2.0, end_min=2.0)
    state = _state(curr, prev=[e2, e1])
    assert state.build() == [e1, e2, curr]


def test_is_sub_checks_opponent_side_only() -> None:
    """``:749-758`` -- only ``raw.opponent`` is inspected; a team-side raw
    event with identical text is NOT recognized as a sub (quirk ported
    verbatim -- see the method's docstring)."""
    state = _state(_lineup_event(_TEAM))
    assert state.is_sub(RawGameEvent(min=1.0, opponent="X, Substitution In")) is True
    assert state.is_sub(RawGameEvent(min=1.0, opponent="X, substitution out")) is True
    assert state.is_sub(RawGameEvent(min=1.0, opponent="X enters game")) is True
    assert state.is_sub(RawGameEvent(min=1.0, opponent="X leaves game")) is True
    assert state.is_sub(RawGameEvent(min=1.0, opponent="X, rebound")) is False
    assert state.is_sub(RawGameEvent(min=1.0, team="X, substitution in")) is False


def test_is_active_true_when_non_sub_event_present() -> None:
    """``:760-766`` first disjunct -- any non-sub raw event makes the
    lineup "active" regardless of elapsed time."""
    curr = _lineup_event(_TEAM, end_min=1.0, raw_game_events=[RawGameEvent(min=1.0, team="rebound")])
    assert _state(curr).is_active(1.0) is True


def test_is_active_true_once_past_safety_delta() -> None:
    """``:760-766`` second disjunct -- only opponent-sub raw events, but
    enough time has passed to trust the lineup anyway."""
    curr = _lineup_event(_TEAM, end_min=1.0, raw_game_events=[RawGameEvent(min=1.0, opponent="X, substitution in")])
    assert _state(curr).is_active(1.0 + SUB_SAFETY_DELTA_MINS + 0.001) is True


def test_is_active_false_within_safety_delta_with_only_opponent_subs() -> None:
    curr = _lineup_event(_TEAM, end_min=1.0, raw_game_events=[RawGameEvent(min=1.0, opponent="X, substitution in")])
    assert _state(curr).is_active(1.0 + SUB_SAFETY_DELTA_MINS / 2) is False


def test_with_player_in_prepends_code_and_does_not_mutate_original() -> None:
    """``:770-778``. Also exercises the module docstring's "returns a new
    instance" contract -- the original ``state`` must be untouched."""
    state = _state(_lineup_event(_TEAM))
    new_state = state.with_player_in("Mitchell, Makhi")
    assert new_state is not state
    assert new_state.curr.players_in == [build_player_code("Mitchell, Makhi", _TEAM)]
    assert state.curr.players_in == []


def test_with_player_in_prepend_order() -> None:
    """``players_in = build_player_code(...) :: curr.players_in`` -- the
    most recently added player ends up at the head."""
    state = _state(_lineup_event(_TEAM)).with_player_in("First, F").with_player_in("Second, S")
    assert [p.id.name for p in state.curr.players_in] == ["Second, S", "First, F"]


def test_with_player_out_prepends_code() -> None:
    """``:779-787``."""
    state = _state(_lineup_event(_TEAM)).with_player_out("Mitchell, Makhi")
    assert state.curr.players_out == [build_player_code("Mitchell, Makhi", _TEAM)]


def test_with_latest_score_updates_end_only() -> None:
    """``:788-796`` -- only ``score_info.end`` changes, ``start`` is untouched."""
    state = _state(_lineup_event(_TEAM)).with_latest_score(Score(5, 3))
    assert state.curr.score_info.end == Score(5, 3)
    assert state.curr.score_info.start == Score(0, 0)


def test_with_team_event_bumps_end_min_and_prepends() -> None:
    """``:797-807``."""
    state = _state(_lineup_event(_TEAM, end_min=1.0)).with_team_event(2.0, "rebound")
    assert state.curr.end_min == 2.0
    assert state.curr.raw_game_events == [RawGameEvent.for_team("rebound", 2.0)]


def test_with_opponent_event_bumps_end_min_and_prepends() -> None:
    """``:808-818``."""
    state = _state(_lineup_event(_TEAM, end_min=1.0)).with_opponent_event(2.0, "rebound")
    assert state.curr.end_min == 2.0
    assert state.curr.raw_game_events == [RawGameEvent.for_opponent("rebound", 2.0)]


def test_with_team_event_prepend_order() -> None:
    state = _state(_lineup_event(_TEAM)).with_team_event(1.0, "a").with_team_event(2.0, "b")
    assert state.curr.raw_game_events == [
        RawGameEvent.for_team("b", 2.0),
        RawGameEvent.for_team("a", 1.0),
    ]


# --- reorder_and_reverse (utest :158-272) -----------------------------------

# 1:1 transliteration of the oracle's `event_list` literal (:162-233) -- kept
# in the Scala's original (forward-chronological) order; the two test cases
# below feed slices/reversals of it, matching how the Scala test consumes it.
_REORDER_EVENT_LIST: list[PlayByPlayEvent] = [
    OtherTeamEvent(0.4, Score(0, 0), "pre-sub-1-no-ref"),
    OtherOpponentEvent(0.4, Score(0, 0), "pre-sub-2-no-ref"),
    OtherTeamEvent(0.4, Score(0, 0), "[player1] pre-sub-3-ref-p1"),
    OtherTeamEvent(0.4, Score(0, 0), "[player2] pre-sub-4-ref-p2"),
    OtherOpponentEvent(0.4, Score(0, 0), "[player1] pre-sub-5-ignore-p1"),
    OtherOpponentEvent(0.4, Score(0, 0), "[player2] pre-sub-6-ignore-p2"),
    OtherTeamEvent(0.4, Score(1, 0), "post-sub-1-no-ref"),  # (confirm sorting works)
    OtherTeamEvent(0.4, Score(0, 0), "11:11,0-0,misc_player, foulon"),  # (check FT logic)
    SubInEvent(0.4, Score(0, 0), "player1"),
    OtherTeamEvent(0.4, Score(0, 0), "middle-event-1"),
    OtherTeamEvent(0.4, Score(1, 0), "middle-event-2"),
    SubOutEvent(0.4, Score(0, 0), "player2"),
    OtherOpponentEvent(0.4, Score(1, 0), "post-sub-2-no-ref"),
    OtherTeamEvent(0.4, Score(1, 0), "[player1] post-sub-3-ref-p1"),
    OtherTeamEvent(0.4, Score(1, 0), "[player2] post-sub-4-ref-p2"),
    OtherOpponentEvent(0.4, Score(1, 0), "[player1] post-sub-5-ignore-p1"),
    OtherOpponentEvent(0.4, Score(1, 0), "[player2] post-sub-6-ignore-p2"),
    OtherOpponentEvent(0.4, Score(2, 0), "11:11,0-0,misc_player missed Free Throw"),  # (ignored wrong direction)
    OtherTeamEvent(0.4, Score(2, 0), "11:11,0-0,random_player missed Free Throw"),
]


def _event_str(ev: PlayByPlayEvent) -> str:
    if isinstance(ev, (SubInEvent, SubOutEvent)):
        return ev.player_name
    if isinstance(ev, (OtherTeamEvent, OtherOpponentEvent)):
        return ev.event_string
    return str(ev)


def test_reorder_and_reverse_trivial_two_element_swap() -> None:
    """utest ``:236-238`` -- ``event_list.take(2)`` (NOT reversed -- a plain
    2-event input) still groups into one same-min block and comes out
    prepend-swapped."""
    result = reorder_and_reverse(_REORDER_EVENT_LIST[:2])
    assert result == [_REORDER_EVENT_LIST[1], _REORDER_EVENT_LIST[0]]


def test_reorder_and_reverse_full_scenario() -> None:
    """utest ``:241-271`` -- ``event_list.reverse`` fed to
    ``reorder_and_reverse``, asserted against the oracle's exact flat
    string ordering. Exercises: score-based inner sort (post-sub-1 sorted
    ahead of the subs by score), FT-attempt direction-pull (the ``foulon``
    line establishes ``direction_team``, pulling the trailing
    ``random_player missed Free Throw`` FT back into the pre-sub group),
    sub-in/-out player-reference routing (``pre-sub-3/4`` and
    ``post-sub-3/4`` split on which player they reference), and the
    "ignore" opponent events that never reference a sub and fall through
    ``add_to_state``'s score-comparison branch."""
    result = reorder_and_reverse(list(reversed(_REORDER_EVENT_LIST)))
    assert [_event_str(ev) for ev in result] == [
        "pre-sub-1-no-ref",
        "pre-sub-2-no-ref",
        "[player2] pre-sub-4-ref-p2",
        "[player1] pre-sub-5-ignore-p1",
        "[player2] pre-sub-6-ignore-p2",
        "11:11,0-0,misc_player, foulon",
        "middle-event-1",
        "[player2] post-sub-4-ref-p2",
        "11:11,0-0,random_player missed Free Throw",
        "player1",
        "player2",
        "[player1] pre-sub-3-ref-p1",
        "post-sub-1-no-ref",
        "middle-event-2",  # (sorted to a different position)
        "post-sub-2-no-ref",
        "[player1] post-sub-3-ref-p1",
        "[player1] post-sub-5-ignore-p1",
        "[player2] post-sub-6-ignore-p2",
        "11:11,0-0,misc_player missed Free Throw",
    ]


def test_reorder_and_reverse_no_subs_returns_score_sorted_block() -> None:
    """Supplement (no standalone Scala oracle) -- a same-minute block with
    no ``SubEvent`` returns ``ordered_block`` unchanged (the ``subs.isEmpty``
    early return, ``:462-463``)."""
    events: list[PlayByPlayEvent] = [
        OtherTeamEvent(0.4, Score(1, 0), "later"),
        OtherOpponentEvent(0.4, Score(0, 0), "earlier"),
    ]
    result = reorder_and_reverse(events)
    assert [_event_str(ev) for ev in result] == ["earlier", "later"]


# --- build_partial_lineup_list (utest :274-715, Task 5b.5) ------------------

_NOW = datetime(2020, 1, 1, 12, 0, 0)

_ALL_PLAYER_NAMES = [
    "Player One",
    "Player Two",
    "Player Three",
    "Player Four",
    "Player Five",
    "Player Six",
    "Player Seven",
]


def _add_remove(
    players: list[PlayerCodeId], *, add: tuple[PlayerCodeId, ...] = (), remove: tuple[PlayerCodeId, ...] = ()
) -> list[PlayerCodeId]:
    """Mirrors the Scala oracle's ``{players.toSet + a - b}.toList.sortBy(_.code)``
    set arithmetic -- keyed by ``.code`` since :class:`PlayerCodeId` isn't
    hashable/frozen in this port (unlike Scala's structurally-equal,
    immutable case class); this test fixture's players are unique by code,
    so keying by code is equivalent."""
    by_code = {p.code: p for p in players}
    for p in remove:
        by_code.pop(p.code, None)
    for p in add:
        by_code[p.code] = p
    return sorted(by_code.values(), key=lambda p: p.code)


def _raw_event_pairs(events: list[RawGameEvent]) -> list[tuple[str | None, str | None]]:
    """``(team, opponent)`` per event -- mirrors the Scala oracle's
    ``RawGameEvent.Team``/``Opponent`` extractor-object pattern matches,
    which check only that one field (``LineupEvent.scala:111-115``),
    ignoring ``min``."""
    return [(ev.team, ev.opponent) for ev in events]


def _make_main_scenario_fixtures() -> tuple[list[PlayerCodeId], TeamSeasonId, TeamSeasonId, LineupEvent, LineupEvent]:
    """utest ``:275-308`` -- the shared ``all_players``/``my_team``/
    ``other_team``/``box_lineup``/``starting_lineup`` fixtures for the main
    and new-format-regression scenarios."""
    all_players = [build_player_code(name, None) for name in _ALL_PLAYER_NAMES]
    my_team = TeamSeasonId(TeamId("TestTeam1"), Year(2017))
    other_team = TeamSeasonId(TeamId("TestTeam2"), Year(2017))
    box_lineup = LineupEvent(
        date=_NOW,
        location_type=LocationType.HOME,
        start_min=0.0,
        end_min=-100.0,
        duration_mins=0.0,
        score_info=ScoreInfo.empty(),
        team=my_team,
        opponent=other_team,
        lineup_id=LineupId.unknown,
        players=all_players,
        players_in=[],
        players_out=[],
        raw_game_events=[],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )
    starting_lineup = replace(box_lineup, players=box_lineup.players[:5])
    return all_players, my_team, other_team, box_lineup, starting_lineup


def _make_main_test_events(all_players: list[PlayerCodeId]) -> list[PlayByPlayEvent]:
    """utest ``:309-375`` -- the shared 29-event stream for the main and
    new-format-regression scenarios."""
    player1, player2, player3, player4, player5, player6, player7 = all_players
    return [
        # First event - sub immediately after game start
        SubInEvent(0.1, Score(0, 0), player6.id.name.upper()),
        # (note player6 being all upper case for 2017- and lower case for 2018 is
        # important to this test because we latch format on the first name found)
        SubOutEvent(0.1, Score(0, 0), player1.id.name),
        OtherTeamEvent(0.2, Score(1, 0), "event1a"),
        OtherTeamEvent(0.2, Score(2, 0), "event2a"),
        # Second event
        SubInEvent(0.4, Score(2, 0), player1.id.name),
        # confirm that all upper case names are returned to normal form:
        SubInEvent(0.4, Score(2, 0), player7.id.name.upper()),
        # CHECK: we only care about "code" not "id":
        SubOutEvent(0.4, Score(2, 0), player2.id.name.upper() + " ii"),
        SubOutEvent(0.4, Score(2, 0), player4.id.name),
        OtherOpponentEvent(0.4, Score(2, 1), "event1b"),
        OtherOpponentEvent(0.4, Score(2, 2), "event2b"),
        OtherTeamEvent(0.4, Score(3, 2), "event3a"),
        OtherTeamEvent(0.4, Score(4, 2), "event4a"),
        # Half time! (third event)
        GameBreakEvent(20.0, Score(4, 2)),
        # (subs happen immediately after break)
        SubOutEvent(20.0, Score(4, 2), player1.id.name),
        OtherOpponentEvent(20.0, Score(4, 2), "PlayerA Leaves Game"),  # opponents can sub too....
        OtherOpponentEvent(20.0, Score(4, 2), "PlayerB, substitution in"),  # (new format))
        SubInEvent(20.0, Score(4, 2), player6.id.name),
        # Fourth event - sub-on-sub action
        SubOutEvent(20.4, Score(4, 2), player2.id.name),
        SubOutEvent(20.4, Score(4, 2), player4.id.name),
        SubInEvent(20.4, Score(4, 2), player1.id.name),  # check subs in any order
        SubInEvent(20.4, Score(4, 2), player7.id.name),
        # Overtime! (first event)
        GameBreakEvent(40.0, Score(4, 2)),
        # Sixth event
        OtherOpponentEvent(40.4, Score(4, 3), "event3b"),
        OtherTeamEvent(40.4, Score(5, 3), "event5a"),
        SubInEvent(40.5, Score(5, 3), player6.id.name),
        SubOutEvent(40.5, Score(5, 3), player1.id.name),
        OtherTeamEvent(40.6, Score(6, 3), "event6a"),
        OtherOpponentEvent(40.7, Score(6, 4), "event4b"),
        # Fin (Seventh event)
        GameEndEvent(45.0, Score(6, 4)),
    ]


@pytest.mark.parametrize("year", [2017, 2018])
def test_build_partial_lineup_list_main_scenario_old_format_latch(year: int) -> None:
    """utest ``:275-633`` -- both years produce IDENTICAL output because
    player6's first sub name is upper-cased, latching ``old_format=True``
    regardless of the team's actual year (the latch-overrides-year-default
    proof)."""
    all_players, my_team, other_team, box_lineup, starting_lineup = _make_main_scenario_fixtures()
    old_format_team = replace(my_team, year=Year(year))
    old_format_lineup = replace(box_lineup, team=old_format_team)
    test_events = _make_main_test_events(all_players)
    player1, player2, player3, player4, player5, player6, player7 = all_players

    result = build_partial_lineup_list(list(reversed(test_events)), old_format_lineup)
    assert len(result) == 7
    event_1, event_2, event_3, event_4, event_5, event_6, event_7 = result

    # event_1 (:396-424)
    assert event_1.date == _NOW
    assert event_1.location_type == LocationType.HOME
    assert (event_1.start_min, event_1.end_min) == (0.0, 0.1)
    assert round(event_1.duration_mins, 1) == 0.1
    assert event_1.score_info == ScoreInfo(Score(0, 0), Score(0, 0), 0, 0)
    assert event_1.team == old_format_team
    assert event_1.opponent == other_team
    assert event_1.lineup_id == LineupId("_".join(sorted(p.code for p in event_1.players)))
    assert event_1.players == sorted(starting_lineup.players, key=lambda p: p.code)
    assert event_1.players_in == []
    assert event_1.players_out == []
    assert event_1.raw_game_events == []

    # event_2 (:425-459)
    assert event_2.date == _NOW + timedelta(milliseconds=6000)
    assert (event_2.start_min, event_2.end_min) == (0.1, 0.4)
    assert round(event_2.duration_mins, 1) == 0.3
    assert event_2.score_info == ScoreInfo(Score(0, 0), Score(2, 0), 0, 2)
    assert event_2.team == old_format_team
    assert event_2.opponent == other_team
    assert event_2.lineup_id == LineupId("_".join(sorted(p.code for p in event_2.players)))
    assert event_2.players == _add_remove(event_1.players, add=(player6,), remove=(player1,))
    assert event_2.players_in == [player6]
    assert event_2.players_out == [player1]
    assert _raw_event_pairs(event_2.raw_game_events) == [("event1a", None), ("event2a", None)]

    # event_3 (:460-496) -- players_out[0]'s id is corrupted by the
    # " ii" + upper() mangling, so only its code is checked (per the
    # oracle's own `player2_with_mods.code ==> player2.code` comment).
    assert (event_3.start_min, event_3.end_min) == (0.4, 20.0)
    assert round(event_3.duration_mins, 1) == 19.6
    assert event_3.score_info == ScoreInfo(Score(2, 0), Score(4, 2), 2, 2)
    assert event_3.team == old_format_team
    assert event_3.opponent == other_team
    assert event_3.lineup_id == LineupId("_".join(sorted(p.code for p in event_3.players)))
    assert event_3.players == _add_remove(event_2.players, add=(player1, player7), remove=(player2, player4))
    assert event_3.players_in == [player1, player7]
    assert len(event_3.players_out) == 2
    assert event_3.players_out[0].code == player2.code
    assert event_3.players_out[1] == player4
    assert _raw_event_pairs(event_3.raw_game_events) == [
        (None, "event1b"),
        (None, "event2b"),
        ("event3a", None),
        ("event4a", None),
    ]

    # event_4 (:497-534)
    assert (event_4.start_min, event_4.end_min) == (20.0, 20.4)
    assert round(event_4.duration_mins, 1) == 0.4
    assert event_4.score_info == ScoreInfo(Score(4, 2), Score(4, 2), 2, 2)
    assert event_4.team == old_format_team
    assert event_4.opponent == other_team
    assert event_4.lineup_id == LineupId("_".join(sorted(p.code for p in event_4.players)))
    assert event_4.players == _add_remove(starting_lineup.players, add=(player6,), remove=(player1,))
    assert event_4.players_in == [player6]
    assert event_4.players_out == [player1]
    assert _raw_event_pairs(event_4.raw_game_events) == [
        (None, "PlayerA Leaves Game"),
        (None, "PlayerB, substitution in"),
    ]

    # event_5 (:535-565)
    assert (event_5.start_min, event_5.end_min) == (20.4, 40.0)
    assert round(event_5.duration_mins, 1) == 19.6
    assert event_5.score_info == ScoreInfo(Score(4, 2), Score(4, 2), 2, 2)
    assert event_5.team == old_format_team
    assert event_5.opponent == other_team
    assert event_5.lineup_id == LineupId("_".join(sorted(p.code for p in event_5.players)))
    assert event_5.players == _add_remove(event_4.players, add=(player1, player7), remove=(player2, player4))
    assert event_5.players_in == [player1, player7]
    assert event_5.players_out == [player2, player4]
    assert event_5.raw_game_events == []

    # event_6 (:566-597)
    assert (event_6.start_min, event_6.end_min) == (40.0, 40.5)
    assert round(event_6.duration_mins, 1) == 0.5
    assert event_6.score_info == ScoreInfo(Score(4, 2), Score(5, 3), 2, 2)
    assert event_6.team == old_format_team
    assert event_6.opponent == other_team
    assert event_6.lineup_id == LineupId("_".join(sorted(p.code for p in event_6.players)))
    assert event_6.players == sorted(starting_lineup.players, key=lambda p: p.code)
    assert event_6.players_in == []
    assert event_6.players_out == []
    assert _raw_event_pairs(event_6.raw_game_events) == [(None, "event3b"), ("event5a", None)]

    # event_7 (:598-631) -- the oracle's `players` check is literally
    # self-referential (`event_7.players.toSet + player6 - player1`, not
    # `event_6.players` like every other event above); transliterated
    # verbatim, this only proves player6 is present and player1 is absent.
    assert (event_7.start_min, event_7.end_min) == (40.5, 45.0)
    assert round(event_7.duration_mins, 1) == 4.5
    assert event_7.score_info == ScoreInfo(Score(5, 3), Score(6, 4), 2, 2)
    assert event_7.team == old_format_team
    assert event_7.opponent == other_team
    assert event_7.lineup_id == LineupId("_".join(sorted(p.code for p in event_7.players)))
    assert event_7.players == _add_remove(event_7.players, add=(player6,), remove=(player1,))
    assert event_7.players_in == [player6]
    assert event_7.players_out == [player1]
    assert _raw_event_pairs(event_7.raw_game_events) == [("event6a", None), (None, "event4b")]


def test_build_partial_lineup_list_new_format_carries_over_lineup() -> None:
    """utest ``:635-679`` -- 2018+ without an old-format latch (player6's
    first sub name NOT upper-cased) carries the post-half lineup over from
    the pre-half lineup, instead of resetting to the starting 5."""
    all_players, my_team, other_team, box_lineup, _starting_lineup = _make_main_scenario_fixtures()
    my_team_2018 = replace(my_team, year=Year(2018))
    player1, player2, player3, player4, player5, player6, player7 = all_players
    test_events = _make_main_test_events(all_players)
    new_format_test_events = [SubInEvent(0.1, Score(0, 0), player6.id.name)] + test_events[1:]

    result = build_partial_lineup_list(list(reversed(new_format_test_events)), replace(box_lineup, team=my_team_2018))
    assert len(result) == 7
    _, _, event_3, event_4, _, _, _ = result

    assert event_4.team == my_team_2018
    assert event_4.opponent == other_team
    assert event_4.score_info == ScoreInfo(Score(4, 2), Score(4, 2), 2, 2)
    assert event_4.lineup_id == LineupId("_".join(sorted(p.code for p in event_4.players)))
    assert event_4.players_in == [player6]
    assert event_4.players_out == [player1]
    assert event_4.players == _add_remove(event_3.players, remove=(player1,))


def test_build_partial_lineup_list_alt_name_format_resolves_via_tidy_player() -> None:
    """utest ``:681-714`` -- exercises the ``tidy_player`` fuzzy/alt-format
    resolution chain (5b.3) end-to-end: comma+initial (``"Mitchell,M"``), a
    hyphenated-surname double-barrel strip (``"NEAL-WILLIAMS,SHAUN"``), a
    truncated first name (``"DOUMBIA,IBRAHIM"``), initials
    (``"MCCLURE,K"``), a swapped first/last plain name
    (``"Preston Horne"``), and a truncated first name
    (``"GUDMUNDSSON,J"``). Note the event list is fed WITHOUT a ``.reverse()``
    here (unlike the two scenarios above) -- it is already in the
    reverse-chronological order the function expects (the literal
    SubIn/SubIn/SubIn/SubOut/SubOut/SubOut ordering processes, chronologically,
    back-to-front: GUDMUNDSSON out first, ..., Mitchell in last -- see the
    task report for the full derivation)."""
    _all_players, my_team, other_team, box_lineup, _starting_lineup = _make_main_scenario_fixtures()
    alt_format_players = [
        build_player_code(name, None)
        for name in [
            "Mitchell, Makhel",
            "Mitchell, Makhi",
            "Kevin McClure",
            "Williams, Shaun",
            "Horne, P.J.",
            "Doumbia, Ibrahim Famouke",
            "Gudmundsson, Jon Axel",
        ]
    ]
    alt_format_box = replace(box_lineup, players=alt_format_players)

    alt_format_test_events: list[PlayByPlayEvent] = [
        SubInEvent(0.1, Score(0, 0), "Mitchell,M"),
        SubInEvent(0.1, Score(0, 0), "NEAL-WILLIAMS,SHAUN"),
        SubInEvent(0.1, Score(0, 0), "DOUMBIA,IBRAHIM"),
        SubOutEvent(0.1, Score(0, 0), "MCCLURE,K"),
        SubOutEvent(0.1, Score(0, 0), "Preston Horne"),
        SubOutEvent(0.1, Score(0, 0), "GUDMUNDSSON,J"),
    ]

    result = build_partial_lineup_list(alt_format_test_events, alt_format_box)
    assert len(result) == 2
    _start, event = result
    assert [p.code for p in event.players_in] == ["MMitchell", "ShWilliams", "IbFaDoumbia"]
    assert [p.code for p in event.players_out] == ["KeMcclure", "PjHorne", "JoAxGudmundsso"]


# --- start_time_from_period / duration_from_period (hand-written -- no Scala oracle) ---


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        (1, 0.0),  # 1st half
        (2, 20.0),  # 2nd half
        (3, 40.0),  # 1st OT
        (4, 45.0),  # 2nd OT
    ],
)
def test_start_time_from_period_men(period: int, expected: float) -> None:
    """``ExtractorUtils.scala:272-281`` -- men play two 20-minute halves,
    then 5-minute overtimes."""
    assert start_time_from_period(period, is_women_game=False) == expected


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        (1, 0.0),  # 1st quarter
        (2, 10.0),  # 2nd quarter
        (3, 20.0),  # 3rd quarter
        (4, 30.0),  # 4th quarter
        (5, 40.0),  # 1st OT
        (6, 45.0),  # 2nd OT
    ],
)
def test_start_time_from_period_women(period: int, expected: float) -> None:
    """``ExtractorUtils.scala:272-281`` -- women play four 10-minute
    quarters, then 5-minute overtimes."""
    assert start_time_from_period(period, is_women_game=True) == expected


def test_duration_from_period_is_next_periods_start() -> None:
    """``ExtractorUtils.scala:286-287`` -- ``duration_from_period(p, w) ==
    start_time_from_period(p + 1, w)``, i.e. the elapsed time once ``p`` has
    completed equals the start time of ``p + 1``."""
    assert duration_from_period(1, is_women_game=False) == 20.0  # end of men's 1st half
    assert duration_from_period(2, is_women_game=False) == start_time_from_period(3, is_women_game=False) == 40.0
    assert duration_from_period(4, is_women_game=True) == start_time_from_period(5, is_women_game=True) == 40.0


class TestTeamNameEquivalence:
    """A directional alias fixes one spelling by breaking the other.

    `team_aliases` rewrites a page name to a canonical one. That works only
    while every game in the season targets the canonical spelling -- and both
    spellings occur in the SAME season, so the rewrite is a perfect trade.
    Measured on the inherited `Year(2021): {NIU -> Northern Ill.}` entry
    before the equivalence class existed:

        season 2021-22 (alias active)  target `NIU` FAIL x3  `Northern Ill.` OK x3
        season 2015    (no alias)      target `NIU` OK       `Northern Ill.` FAIL

    Six of these reached the skip ledger during the corpus re-parse, every one
    with BOTH titles present and one exactly equal to the target.
    """

    @staticmethod
    def _ok(titles, target, year):
        from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError

        return not isinstance(parse_team_name(titles, TeamId(target), Year(year)), ParseError)

    def test_both_spellings_resolve_in_the_aliased_season(self):
        """Year 2021 has the rewrite active; both targets must still match."""
        for target in ("NIU", "Northern Ill."):
            for titles in (["Bradley", "NIU"], ["NIU", "Drake"], ["Bradley", "Northern Ill."]):
                assert self._ok(titles, target, 2021), (target, titles)

    def test_both_spellings_resolve_in_an_unaliased_season(self):
        """Year 2015 has no rewrite; the equivalence must carry it alone."""
        for target in ("NIU", "Northern Ill."):
            for titles in (["Bradley", "NIU"], ["Bradley", "Northern Ill."]):
                assert self._ok(titles, target, 2015), (target, titles)

    def test_distinct_schools_are_not_merged(self):
        """The guard on the whole approach: near-identical names stay distinct."""
        from sportsdataverse.mbb.mbb_ncaa_data_quality import same_school

        assert not same_school("Miami (FL)", "Miami (OH)")
        assert not same_school("New Orleans", "Southern-N.O.")
        assert not same_school("Loyola (IL)", "Loyola (MD)")
