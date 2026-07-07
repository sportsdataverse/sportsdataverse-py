"""Oracle tests for :mod:`sportsdataverse.mbb.mbb_ncaa_roster_parser` (Task 5e.1).

The ``TestParseRoster`` cases are a 1:1 transliteration of
``RosterParserTests.scala`` (``utest``, ``"RosterParser"`` block), which
loads ``tests/fixtures/ncaa/sample_roster.html`` (vendored byte-exact from
the upstream clone -- see that directory's ``README.md``) and asserts:

* ``version_format=0``, ``TeamId("TeamA")`` -> the first 6 entries (by
  games-played descending) match an exact literal list, including the
  gp=20 tie group's Scala-``HashMap``-iteration-order-dependent sub-order
  (Eytle-Rock, Spasojevic, Rogers, Kennedy -- see
  ``mbb_ncaa_roster_parser.py``'s module docstring for the full derivation
  of why this exact order is correct).
* ``version_format=0``, ``TeamId("TeamB")``, with ``"Davis, Brendan"``
  string-mutated to ``"Akin, David"`` (the oracle's ``lineup_html_dup_check``
  case) -> a ``Left`` (here: ``list[ParseError]``), because the mutated row
  now collides with the real ``"Akin, Daniel"`` row on the SAME player code
  (``"DaAkin"``) while being a genuinely different player.

The upstream oracle also defines a ``lineup_html_diacritic`` variant
(``.replace("Akin, Daniel", "Akin, Daniél")``) that is never actually
asserted against in any ``test()`` block -- transliterated here as a
smoke-only parse (no dedicated assertion), matching the oracle's own
(apparently unused) scope.

``get_unified_ncaa_id`` has **no dedicated Scala oracle** anywhere in the
upstream test suite (grepped the full ``cbb-explorer`` tree) -- its only
caller is ``LineupController.scala``, and it expects a *player* page
(``tr[id^=player_season_] td:first-child a``), a different shape than
``sample_roster.html``'s *team* roster page. The tests below are
hand-written against an inline HTML snippet matching the documented
selector, exercising the "pick the lowest numeric id" behavior directly.
"""

from __future__ import annotations

from pathlib import Path

from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError
from sportsdataverse.mbb.mbb_ncaa_models import PlayerCodeId, PlayerId, RosterEntry, TeamId
from sportsdataverse.mbb.mbb_ncaa_roster_parser import get_unified_ncaa_id, parse_roster

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa"

_ROSTER_HTML = (FIXTURES / "sample_roster.html").read_text(encoding="utf-8")


def _entry(code, name, ncaa_id, number, pos, height, height_in, year_class, gp):
    return RosterEntry(
        PlayerCodeId(code, PlayerId(name), ncaa_id),
        number,
        pos,
        height,
        height_in,
        year_class,
        gp,
        None,
        None,
    )


class TestParseRoster:
    """``RosterParserTests.scala:36-155`` (``"RosterParser"`` block)."""

    def test_top_6_by_games_played_v0_format(self) -> None:
        result = parse_roster("test.html", _ROSTER_HTML, TeamId("TeamA"), version_format=0)
        assert isinstance(result, list)
        assert result and isinstance(result[0], RosterEntry), result
        top_6 = result[:6]
        assert top_6 == [
            _entry("RjEytle-rock", "Eytle-Rock, R.J.", "2084828", "11", "G", "6-3", 75, "Jr", 20),
            _entry("DiSpasojevic", "Spasojevic, Dimitrije", "1958411", "32", "F", "6-8", 80, "Sr", 20),
            _entry("DaRogers", "Rogers, Darnell", "2265903", "2", "G", "5-2", 62, "Sr", 20),
            _entry("KeKennedy", "Kennedy, Keondre", "2265883", "0", "G", "6-6", 78, "Jr", 20),
            _entry("BrHorvath", "Horvath, Brandon", "1954159", "12", "F", "6-10", 82, "Sr", 19),
            _entry("LjOwens", "Owens, L.J.", "2081843", "1", "G", "6-3", 75, "Jr", 19),
        ]

    def test_initials_only_row_is_rejected(self) -> None:
        # sample_roster.html's first data row is name "A, B" (a 4-char
        # comma-initials shape, "A, B") -- name_is_initials rejects it, so
        # the roster's 16 rows yield only 15 entries.
        result = parse_roster("test.html", _ROSTER_HTML, TeamId("TeamA"), version_format=0)
        assert isinstance(result, list) and result and isinstance(result[0], RosterEntry)
        assert len(result) == 15  # 16 rows - 1 rejected initials row
        codes = {e.player_code_id.code for e in result}
        assert "DaAkin" in codes  # the real "Akin, Daniel" row does survive

    def test_duplicate_player_code_different_names_is_an_error(self) -> None:
        # Oracle's lineup_html_dup_check: "Davis, Brendan" (a real player,
        # code "BrDavis") is string-mutated to "Akin, David", which builds
        # to code "DaAkin" -- the SAME code as the real "Akin, Daniel" row,
        # but a genuinely different id (different full name) -- a hard
        # duplicate-code error, not a benign same-name repeat.
        mutated_html = _ROSTER_HTML.replace("Davis, Brendan", "Akin, David")
        result = parse_roster("test.html", mutated_html, TeamId("TeamB"), version_format=0)
        assert isinstance(result, list)
        assert result and isinstance(result[0], ParseError)

    def test_diacritic_mutation_smoke_parses_without_error(self) -> None:
        # Oracle's lineup_html_diacritic const is defined but never asserted
        # against in any RosterParserTests test() block -- ported as a
        # smoke-only parse matching that (apparently unused) upstream scope.
        mutated_html = _ROSTER_HTML.replace("Akin, Daniel", "Akin, Daniél")
        result = parse_roster("test.html", mutated_html, TeamId("TeamA"), version_format=0)
        assert isinstance(result, list)
        assert result and isinstance(result[0], RosterEntry)

    def test_include_coach_appends_a_coach_entry_when_present(self) -> None:
        # sample_roster.html's div#head_coaches_div has no <a href> child
        # (an empty fieldset), so the coach_finder yields None even with
        # include_coach=True -- no coach entry should be appended.
        result = parse_roster("test.html", _ROSTER_HTML, TeamId("TeamA"), version_format=0, include_coach=True)
        assert isinstance(result, list)
        assert result and isinstance(result[0], RosterEntry)
        assert all(e.player_code_id.code != "__coach__" for e in result)


class TestGetUnifiedNcaaId:
    """No Scala oracle exists for ``get_unified_ncaa_id`` (see module
    docstring) -- hand-written against the documented v1 selector shape."""

    def test_picks_the_numerically_lowest_id(self) -> None:
        html = """
        <table>
          <tr id="player_season_1"><td><a href="/players/7033318">2020/21</a></td></tr>
          <tr id="player_season_2"><td><a href="/players/123">2019/20</a></td></tr>
          <tr id="player_season_3"><td><a href="/players/999999">2021/22</a></td></tr>
        </table>
        """
        assert get_unified_ncaa_id("player.html", html) == "123"

    def test_no_matching_rows_returns_none(self) -> None:
        assert get_unified_ncaa_id("player.html", "<table></table>") is None

    def test_row_missing_href_is_skipped(self) -> None:
        html = """
        <table>
          <tr id="player_season_1"><td><a>no href</a></td></tr>
          <tr id="player_season_2"><td><a href="/players/42">has href</a></td></tr>
        </table>
        """
        assert get_unified_ncaa_id("player.html", html) == "42"
