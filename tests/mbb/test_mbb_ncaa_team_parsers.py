"""Oracle tests for :mod:`sportsdataverse.mbb.mbb_ncaa_team_parsers` (Task 5e.4).

``TestGetNeutralGames`` is a 1:1 transliteration of
``TeamScheduleParserTests.scala`` (``utest``, ``"TeamScheduleParser"``
block), which is ACTIVE upstream and loads
``tests/fixtures/ncaa/test_schedule.html`` (vendored byte-exact from the
upstream clone -- see that directory's ``README.md``).

``TestTeamIdParser`` mirrors ``TeamIdParserTests.scala``, which is
upstream-DISABLED in its ENTIRETY (``val DISABLED = true // TODO (failing
as of 04/2021, ...)`` wraps the WHOLE ``"TeamIdParser" - { ... }`` block,
including ``build_lineup_cli_array``/``build_available_team_list``, not just
the fixture-based ``get_team_triples`` case). Every case below is ported
faithfully but marked ``@pytest.mark.skip`` for the same reason, rather than
promoting the two pure-helper cases to "active" on the theory that they
don't touch HTML -- see :mod:`sportsdataverse.mbb.mbb_ncaa_team_parsers`'s
module docstring for the full derivation (including independent evidence
that ``get_team_triples``'s default ``old_format=False`` genuinely cannot
match ``test_attendance_list.html``'s old-format rows, corroborating that
the upstream disablement is real).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from sportsdataverse.mbb.mbb_ncaa_models import ConferenceId, TeamId
from sportsdataverse.mbb.mbb_ncaa_team_parsers import (
    build_available_team_list,
    build_lineup_cli_array,
    get_neutral_games,
    get_team_triples,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa"

_SCHEDULE_HTML = (FIXTURES / "test_schedule.html").read_text(encoding="utf-8")
_TEAM_ID_HTML = (FIXTURES / "test_attendance_list.html").read_text(encoding="utf-8")

_UPSTREAM_DISABLED_REASON = (
    "mirrors upstream DISABLED=true (04/2021, suspected COVID-era HTML changes) -- "
    "the entire 'TeamIdParser' utest block is gated off, not just this case"
)


class TestGetNeutralGames:
    """``TeamScheduleParserTests.scala:30-53`` (``"get_neutral_games"``, ACTIVE)."""

    def test_get_neutral_games(self) -> None:
        result = get_neutral_games("test_schedule.html", _SCHEDULE_HTML, format_version=0)
        assert isinstance(result, tuple)
        team, date_set = result
        assert team == TeamId("TEAM_NAME")
        assert sorted(date_set) == sorted(["12/08/2018", "01/26/2019", "03/23/2019", "03/21/2019", "03/14/2019"])


class TestTeamIdParser:
    """``TeamIdParserTests.scala:23-81`` -- entirely upstream-DISABLED."""

    @pytest.mark.skip(reason=_UPSTREAM_DISABLED_REASON)
    def test_get_team_triples(self) -> None:
        result = get_team_triples("filename_test", _TEAM_ID_HTML)
        assert result == [
            (TeamId("Syracuse"), "450738", ConferenceId("ACC")),
            (TeamId("Kentucky"), "450591", ConferenceId("SEC")),
        ]

    @pytest.mark.skip(reason=_UPSTREAM_DISABLED_REASON)
    def test_build_lineup_cli_array(self) -> None:
        test_in = [
            (TeamId("Penn St."), "1", ConferenceId("B1G")),
            (TeamId("Michigan St."), "1000", ConferenceId("B1G")),
            (TeamId("Kentucky"), "450591", ConferenceId("SEC")),
        ]
        result = build_lineup_cli_array(test_in)
        assert result[ConferenceId("SEC")] == "   '450591::Kentucky'"
        assert result[ConferenceId("B1G")] == "   '1::Penn+St.'\n   '1000::Michigan+St.'"

    @pytest.mark.skip(reason=_UPSTREAM_DISABLED_REASON)
    def test_build_available_team_list(self) -> None:
        test_in = [
            (TeamId("Penn St."), "1", ConferenceId("B1G")),
            (TeamId("Michigan St."), "1000", ConferenceId("B1G")),
            (TeamId("Kentucky"), "450591", ConferenceId("SEC")),
        ]
        test_in_2 = [
            (TeamId("Penn St."), "11", ConferenceId("B1G")),
            (TeamId("Maryland"), "10", ConferenceId("B1G")),
        ]
        res = build_available_team_list({"2018/9": test_in, "2019/20": test_in_2})
        assert res[ConferenceId("B1G")]("test") == (
            ' "Penn St.": [\n'
            '   { team: "Penn St.", year: "2018/9", gender: "Men", index_template: "test" },\n'
            '   { team: "Penn St.", year: "2019/20", gender: "Men", index_template: "test" },\n'
            " ],\n"
            ' "Michigan St.": [\n'
            '   { team: "Michigan St.", year: "2018/9", gender: "Men", index_template: "test" },\n'
            " ],\n"
            ' "Maryland": [\n'
            '   { team: "Maryland", year: "2019/20", gender: "Men", index_template: "test" },\n'
            " ],"
        )
        assert res[ConferenceId("SEC")]("test2") == (
            ' "Kentucky": [\n   { team: "Kentucky", year: "2018/9", gender: "Men", index_template: "test2" },\n ],'
        )
