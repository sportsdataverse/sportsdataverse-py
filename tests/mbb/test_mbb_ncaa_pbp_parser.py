"""Oracle tests for :mod:`sportsdataverse.mbb.mbb_ncaa_pbp_parser` (Task 5e.3).

1:1 transliteration of ``PlayByPlayParserTests.scala`` (``utest``,
``"PlayByPlayParser"`` block):

* ``"create_lineup_data"`` -- the fixture end-to-end oracle (loads
  ``tests/fixtures/ncaa/test_play_by_play.html``, vendored in Task 5e.1),
  the flagship test of this whole phase since it exercises the ENTIRE
  Phase 5a-5d pipeline through :func:`~sportsdataverse.mbb.mbb_ncaa_pbp_parser
  .create_lineup_data`.
* ``"parse_game_events"`` -- same fixture, asserts the raw event/break/end
  counts.
* ``"enrich_and_reverse_game_events"`` -- the Scala oracle's own ``case _ =>
  {}`` block is a smoke-only assertion (its literal pattern-match is commented
  out with a ``TODO ... need to revisit`` note) -- ported as a smoke test
  plus a couple of structural assertions the commented-out block still
  implies (a trailing ``GameEndEvent``, one more event than the input).
* ``"parse_game_score"`` / ``"parse_desc_game_time"`` / ``"parse_game_event"``
  -- inline HTML snippets, transliterated via :func:`_body` (this project's
  equivalent of the Scala oracle's ``TestUtils.with_doc``).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from bs4.element import Tag

from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError
from sportsdataverse.mbb.mbb_ncaa_html import parse_html
from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    Score,
    ScoreInfo,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_pbp_parser import (
    create_lineup_data,
    enrich_and_reverse_game_events,
    parse_desc_game_time,
    parse_game_event,
    parse_game_events,
    parse_game_score,
    v0_builders,
)
from sportsdataverse.mbb.mbb_ncaa_stints import (
    GameBreakEvent,
    GameEndEvent,
    OtherOpponentEvent,
    OtherTeamEvent,
    SubInEvent,
    SubOutEvent,
    build_player_code,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa"

_PLAY_BY_PLAY_HTML = (FIXTURES / "test_play_by_play.html").read_text(encoding="utf-8")


def _body(html: str) -> Tag:
    """Parse a bare HTML snippet and return its ``<body>`` (this project's
    equivalent of the Scala oracle's ``TestUtils.with_doc(html) { doc =>
    ... doc.body ... }``)."""
    soup = parse_html(html)
    assert soup.body is not None
    return soup.body


def _box_lineup() -> LineupEvent:
    """``PlayByPlayParserTests.scala:43-72`` -- the ``box_lineup`` used by
    the ``"create_lineup_data"`` fixture end-to-end test."""
    box_player_names = [f"S{n}rname, F{n}rstname TeamA" for n in range(1, 10)]
    box_players = [build_player_code(name, None) for name in box_player_names]
    return LineupEvent(
        date=datetime.now(),
        location_type=LocationType.HOME,
        start_min=0.0,
        end_min=-100.0,
        duration_mins=0.0,
        score_info=ScoreInfo.empty(),
        team=TeamSeasonId(TeamId("TeamA"), Year(2017)),
        opponent=TeamSeasonId(TeamId("TeamB"), Year(2017)),
        lineup_id=LineupId.unknown,
        players=box_players,
        players_in=[],
        players_out=[],
        raw_game_events=[],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )


class TestCreateLineupData:
    """``PlayByPlayParserTests.scala:42-130`` (``"create_lineup_data"`` block)."""

    def test_fixture_end_to_end(self) -> None:
        box_lineup = _box_lineup()
        result = create_lineup_data("filename_test", _PLAY_BY_PLAY_HTML, box_lineup, format_version=0)
        assert not isinstance(result, list), result  # not a list[ParseError]
        lineup_events, bad_lineup_events = result

        assert len(lineup_events) == 27  # (by inspection)
        assert len(bad_lineup_events) == 1
        # (1 with wrong lineup size, 3 where a benched player was in an event)

        # Spot checks on a "random" entry (index 1, ``.drop(1).headOption``):
        spot = lineup_events[1]
        assert any(p.id.name == "S6rname, F6rstname TeamA" for p in spot.players)
        # Basic enriched scores (unlucky coincidence: team/oppo stats match).
        assert spot.team_stats.num_events == 6
        assert spot.team_stats.pts == 3
        assert spot.team_stats.plus_minus == 0
        assert spot.opponent_stats.num_events == 6
        assert spot.opponent_stats.pts == 3
        assert spot.opponent_stats.plus_minus == -spot.team_stats.plus_minus

        # Spot checks across all entries.
        for event in lineup_events:
            assert event.duration_mins > 0.0
            assert len(event.players) == 5
            for p in event.players:
                name = p.id.name
                name_no_spaces = name.replace(" ", "")
                # Sub happens only if the tidier player name is in the box lineup.
                if any(bp.id.name == name for bp in box_lineup.players):
                    assert name_no_spaces.upper() != name_no_spaces

        # Sum of the durations is the entire game.
        total = sum(ev.duration_mins for ev in lineup_events) + sum(ev.duration_mins for ev in bad_lineup_events)
        assert f"{total:.1f}" == "45.0"


class TestParseGameEvents:
    """``PlayByPlayParserTests.scala:132-152`` (``"parse_game_events"`` block)."""

    def test_fixture_counts(self) -> None:
        result = parse_game_events("filename_test", _PLAY_BY_PLAY_HTML, TeamId("TeamA"), Year(2018), v0_builders)
        assert not (result and isinstance(result[0], ParseError)), result
        assert len(result) == 567
        # grep -c 'smtext' tests/fixtures/ncaa/test_play_by_play.html
        # 2256 # 564*4, i.e. 564 rows of 4 columns, + 3 game breaks
        assert sum(1 for ev in result if isinstance(ev, GameBreakEvent)) == 2
        assert sum(1 for ev in result if isinstance(ev, GameEndEvent)) == 1


class TestEnrichAndReverseGameEvents:
    """``PlayByPlayParserTests.scala:154-183`` (``"enrich_and_reverse_game_events"``
    block) -- the Scala oracle's own expected-pattern match is commented out
    (a ``TODO ... need to revisit`` note), leaving only a smoke assertion
    (``case _ => {}``). Ported as a smoke test plus the structural invariants
    that block still implies: exactly one more event than the input (the
    prepended ``GameEndEvent``), and that event leading the (reversed) list.
    """

    def test_smoke(self) -> None:
        score = Score(1, 1)
        test_list = [
            OtherTeamEvent(2.0, score, "test1"),
            OtherTeamEvent(3.0, score, "test2a"),
            OtherTeamEvent(2.0, score, "test2b"),
            OtherTeamEvent(2.5, score, "test3"),
        ]
        result = enrich_and_reverse_game_events(test_list)
        assert len(result) == len(test_list) + 1
        assert isinstance(result[0], GameEndEvent)


# ---------------------------------------------------------------------------
# Lower-level tests (``PlayByPlayParserTests.scala:185-425``)
# ---------------------------------------------------------------------------

_SAMPLE_GAME_EVENT = """
<table><tr>
  <td class="smtext">20:00:00</td>
  <td colspan="3" align="center" class="boldtext"><b></b> random event like timeout </td>
</table></tr>
"""

_SAMPLE_TEAM_SUB_IN = """
<table><tr>
  <td class="smtext">15:00</td>
  <td class="smtext">S8RNAME,F8RSTNAME TEAMA Enters Game</td>
  <td class="smtext" align="center">45-26</td>
  <td class="smtext"></td>
</table></tr>
"""
_SAMPLE_TEAM_SUB_IN_NEW_FORMAT = """
<table><tr>
  <td class="smtext">15:00</td>
  <td class="smtext">F5rstname TeamA S5rname, substitution in</td>
  <td class="smtext" align="center">45-26</td>
  <td class="smtext"></td>
</table></tr>
"""

_SAMPLE_OPPO_SUB_IN = """
<table><tr>
  <td class="smtext">15:00</td>
  <td class="smtext"></td>
  <td class="smtext" align="center">45-26</td>
  <td class="smtext">S8RNAME,F8RSTNAME TEAMB Enters Game</td>
</table></tr>
"""

_SAMPLE_TEAM_SUB_OUT = """
<table><tr>
  <td class="smtext">15:00</td>
  <td class="smtext">S8RNAME,F8RSTNAME TEAMA Leaves Game</td>
  <td class="smtext" align="center">45-26</td>
  <td class="smtext"></td>
</table></tr>
"""
_SAMPLE_TEAM_SUB_OUT_NEW_FORMAT = """
<table><tr>
  <td class="smtext">15:00</td>
  <td class="smtext">F5rstname TeamA S5rname, substitution out</td>
  <td class="smtext" align="center">45-26</td>
  <td class="smtext"></td>
</table></tr>
"""

_SAMPLE_OPPO_SUB_OUT = """
<table><tr>
  <td class="smtext">15:00</td>
  <td class="smtext"></td>
  <td class="smtext" align="center">45-26</td>
  <td class="smtext">S8RNAME,F8RSTNAME TEAMB Leaves Game</td>
</table></tr>
"""

_SAMPLE_TEAM_EVENT = """
<table><tr>
  <td class="smtext">15:00</td>
  <td class="smtext">event text</td>
  <td class="smtext" align="center">45-26</td>
  <td class="smtext"></td>
</table></tr>
"""
_SAMPLE_TEAM_EVENT_NEW_FORMAT = """
<table><tr>
  <td class="smtext">15:00:50</td>
  <td class="smtext">event text</td>
  <td class="smtext" align="center">45-26</td>
  <td class="smtext"></td>
</table></tr>
"""
_SAMPLE_OPPO_EVENT = """
<table><tr>
  <td class="smtext">15:00</td>
  <td class="smtext"></td>
  <td class="smtext" align="center">45-26</td>
  <td class="smtext">event text</td>
</table></tr>
"""


class TestParseGameScore:
    """``:271-277``."""

    def test_parses_score(self) -> None:
        result = parse_game_score(_body(_SAMPLE_TEAM_EVENT), v0_builders)
        assert result == ("45-26", Score(45, 26))


class TestParseDescGameTime:
    """``:278-291``."""

    def test_parses_mm_ss(self) -> None:
        result = parse_desc_game_time(_body(_SAMPLE_TEAM_EVENT), v0_builders)
        assert result is not None and not isinstance(result, ParseError)
        raw, mins = result
        assert raw == "15:00"
        assert f"{mins:.1f}" == "15.0"

    def test_parses_mm_ss_ss(self) -> None:
        result = parse_desc_game_time(_body(_SAMPLE_TEAM_EVENT_NEW_FORMAT), v0_builders)
        assert result is not None and not isinstance(result, ParseError)
        raw, mins = result
        assert raw == "15:00:50"
        assert f"{mins:.1f}" == "15.0"


class TestParseGameEvent:
    """``:292-425``."""

    def test_game_event_ignored(self) -> None:
        result = parse_game_event(_body(_SAMPLE_GAME_EVENT), target_team_first=True, builders=v0_builders)
        assert result == []

    def test_team_event(self) -> None:
        result = parse_game_event(_body(_SAMPLE_TEAM_EVENT), target_team_first=True, builders=v0_builders)
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, OtherTeamEvent)
        assert f"{ev.min:.1f}" == "15.0"
        assert ev.score == Score(45, 26)
        assert ev.event_string == "15:00,45-26,event text"

    def test_oppo_event(self) -> None:
        result = parse_game_event(_body(_SAMPLE_OPPO_EVENT), target_team_first=True, builders=v0_builders)
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, OtherOpponentEvent)
        assert f"{ev.min:.1f}" == "15.0"
        assert ev.score == Score(45, 26)
        assert ev.event_string == "15:00,45-26,event text"

    def test_team_sub_in(self) -> None:
        result = parse_game_event(_body(_SAMPLE_TEAM_SUB_IN), target_team_first=True, builders=v0_builders)
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, SubInEvent)
        assert f"{ev.min:.1f}" == "15.0"
        assert ev.player_name == "S8RNAME,F8RSTNAME TEAMA"

    def test_team_sub_in_new_format(self) -> None:
        result = parse_game_event(_body(_SAMPLE_TEAM_SUB_IN_NEW_FORMAT), target_team_first=True, builders=v0_builders)
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, SubInEvent)
        assert f"{ev.min:.1f}" == "15.0"
        assert ev.player_name == "F5rstname TeamA S5rname"

    def test_oppo_sub_in(self) -> None:
        result = parse_game_event(_body(_SAMPLE_OPPO_SUB_IN), target_team_first=True, builders=v0_builders)
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, OtherOpponentEvent)
        assert f"{ev.min:.1f}" == "15.0"
        assert ev.score == Score(45, 26)
        assert ev.event_string == "15:00,45-26,S8RNAME,F8RSTNAME TEAMB Enters Game"

    def test_team_sub_out(self) -> None:
        result = parse_game_event(_body(_SAMPLE_TEAM_SUB_OUT), target_team_first=True, builders=v0_builders)
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, SubOutEvent)
        assert f"{ev.min:.1f}" == "15.0"
        assert ev.player_name == "S8RNAME,F8RSTNAME TEAMA"

    def test_team_sub_out_new_format(self) -> None:
        doc = _body(_SAMPLE_TEAM_SUB_OUT_NEW_FORMAT)

        result = parse_game_event(doc, target_team_first=True, builders=v0_builders)
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, SubOutEvent)
        assert f"{ev.min:.1f}" == "15.0"
        assert ev.player_name == "F5rstname TeamA S5rname"

        result_flipped = parse_game_event(doc, target_team_first=False, builders=v0_builders)
        assert len(result_flipped) == 1
        ev_flipped = result_flipped[0]
        assert isinstance(ev_flipped, OtherOpponentEvent)
        assert f"{ev_flipped.min:.1f}" == "15.0"
        assert ev_flipped.score == Score(26, 45)
        assert ev_flipped.event_string == "15:00,45-26,F5rstname TeamA S5rname, substitution out"

    def test_oppo_sub_out(self) -> None:
        doc = _body(_SAMPLE_OPPO_SUB_OUT)

        result = parse_game_event(doc, target_team_first=True, builders=v0_builders)
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, OtherOpponentEvent)
        assert f"{ev.min:.1f}" == "15.0"
        assert ev.score == Score(45, 26)
        assert ev.event_string == "15:00,45-26,S8RNAME,F8RSTNAME TEAMB Leaves Game"

        result_flipped = parse_game_event(doc, target_team_first=False, builders=v0_builders)
        assert len(result_flipped) == 1
        ev_flipped = result_flipped[0]
        assert isinstance(ev_flipped, SubOutEvent)
        assert f"{ev_flipped.min:.1f}" == "15.0"
        assert ev_flipped.player_name == "S8RNAME,F8RSTNAME TEAMB"
