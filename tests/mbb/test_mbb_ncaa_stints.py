"""Oracle tests for :mod:`sportsdataverse.mbb.mbb_ncaa_stints` (Tasks 5b.2/5b.4).

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

Upstream-test-bug note (``:86-115``): the Scala loops ``team`` over
``[None, Some(TeamId("TCU"))]`` but every call passes ``None``, and no TCU
entry exists in the misspellings table -- the ``if team.nonEmpty`` guard
cases are dead. The faithful transliteration therefore expects the
UNcorrected code/name on both iterations.

The ``PlayByPlayEvent`` ADT / ``LineupBuildingState`` have no standalone
Scala oracle beyond ``reorder_and_reverse`` (which only exercises
``with_min``/``score``/``event_string``/``player_name``/``is_team_dir``
indirectly) -- Task 5b.5's ``build_partial_lineup_list`` oracle is the
first end-to-end exercise of ``LineupBuildingState``. The
``test_*_with_min`` / ``test_lineup_building_state_*`` cases below are
hand-written smoke tests, each traced by hand against the ported algorithm.
"""

from __future__ import annotations

from datetime import datetime

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
    build_player_code,
    parse_team_name,
    remove_diacritics,
    reorder_and_reverse,
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
