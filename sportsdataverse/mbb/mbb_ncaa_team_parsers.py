"""NCAA team-id + team-schedule HTML parsers (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) ``TeamIdParser.scala`` +
``TeamScheduleParser.scala`` -- the fourth of six Phase-5e modules.
``TeamIdParser`` extracts ``(team, ncaa_id, conference)`` triples from an NCAA
team-list/attendance page and formats them for two downstream consumers (a
shell-script team array, and a JSON team-index fragment); ``TeamSchedule
Parser`` extracts the set of "neutral-site" game dates (in practice: every
``"@Opponent"``-marked away/neutral game) from a saved team-schedule page.

**Both parsers are far smaller than the roster/boxscore/pbp parsers** --
``get_team_triples`` is a single flat row-scan, and ``get_neutral_games`` is a
single builder-table lookup with no downstream stint/possession wiring.

**``TeamIdParserTests.scala`` is upstream-DISABLED, in its ENTIRETY --
verified by reading the test file, not assumed from the plan.** The whole
``"TeamIdParser" - { ... }`` block (covering ALL THREE of ``get_team_triples``,
``build_lineup_cli_array``, AND ``build_available_team_list``) sits inside
``val tests = if (DISABLED) Tests {} else Tests { ... }`` with ``val DISABLED
= true // TODO (failing as of 04/2021, haven't looked into why but likely
related to covid changes?)``. This means NONE of the three subtests run
upstream today -- not just the fixture-based ``get_team_triples`` case.
Independent confirmation from reading the fixture itself:
``test_attendance_list.html``'s rows are OLD-format (team name and
conference in separate ``<td>``s, e.g. ``"Syracuse"`` / ``"ACC"``), but the
disabled test calls ``get_team_triples(..., team_id_html)`` with the
DEFAULT ``old_format = false`` -- which only ever matches the NEW-format
``team_conf_regex`` (``"TeamName (Conf)"`` embedded in one cell), so even
undoing the ``DISABLED`` flag would not make this specific call pass against
this specific fixture. That mismatch is consistent with the "failing as of
04/2021" comment and independently corroborates that the disablement is
real, not a stale flag guarding otherwise-passing tests. All three ported
tests below are therefore marked ``@pytest.mark.skip``, mirroring upstream
honestly rather than cherry-picking the two pure-helper tests as "secretly
still active" (:func:`build_lineup_cli_array`/:func:`build_available_team_list`
would additionally not be reproducibly ordered even if un-skipped -- see
those functions' docstrings).

**``TeamScheduleParserTests.scala`` (``get_neutral_games``) is ACTIVE** --
ported 1:1 as a real (passing) oracle test.

**Selector decompositions (JSoup -> bs4, this task's findings):**

* ``tr:has(td:has(a.skipMask))`` (row finder) -- nested ``:has()``,
  soupsieve supports arbitrarily nested pseudo-classes: direct
  ``doc.select(...)``.
* ``a.skipMask`` (id/name finder, scoped to one row) -- plain CSS class
  selector, direct ``row.select_one(...)``.
* ``td:has(a.skipMask) + td`` (old-format conference finder) -- a *fully
  plain* CSS adjacent-sibling combinator (``:has()`` is plain, ``+`` is
  standard CSS): direct ``row.select_one(...)``, unlike the boxscore/pbp
  parsers' ``:contains(...) + X`` cases (which need
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.select_contains` first because
  ``:contains`` itself isn't plain-CSS-equivalent under soupsieve's
  case-sensitive spelling).
* ``fieldset > legend > img[alt]`` / ``div.card-header > img[alt]`` (v0/v1
  team-name finders) -- fully plain CSS (child combinators + attribute
  existence): direct ``doc.select_one(...)``, reading ``.get("alt")``.
* ``legend:contains(Schedule/Results) + table tr:has(td:matches(.*[@]
  [a-zA-Z]+.*)) > td:matches([0-9]+/[0-9]+/[0-9]+)`` (v0 neutral-game
  finder) / the analogous ``div.card-header:contains(...) + div.card-body
  ...`` (v1) -- a five-part compound selector, decomposed into four
  sequential steps rather than one query (mirrors the boxscore parser's
  ``:contains(...) + td`` precedent, extended one level deeper):

  1. :func:`~sportsdataverse.mbb.mbb_ncaa_html.select_contains` for the
     legend/card-header containing ``"Schedule/Results"`` (JSoup's
     ``:contains()`` case-insensitive semantics, not soupsieve's
     case-sensitive ``:-soup-contains()``).
  2. :meth:`~bs4.element.Tag.find_next_sibling` (name-filtered:
     ``"table"`` / ``"div"`` + ``class_="card-body"``) for the ``+``
     adjacent-sibling step -- same precedent as the boxscore parser's
     ``td.boldtext:contains(...) + td`` (JSoup's single-``Option``
     extractor takes only the first legend/header match anyway, so a
     name-filtered ``find_next_sibling`` is the faithful "closest matching
     sibling" translation used throughout this port).
  3. ``tr:has(td:matches(AT_REGEX))`` -- :func:`~sportsdataverse.mbb
     .mbb_ncaa_html.select_matching` scoped to each row's descendant
     ``<td>``s, truthiness-tested (any match qualifies the row) --
     reproduces JSoup's descendant ``:has()`` + ``:matches()`` nesting.
  4. ``> td:matches(DATE_REGEX)`` -- **direct children only** (the ``>``
     combinator), which none of ``mbb_ncaa_html.py``'s existing helpers
     express (they all use ``root.select(selector)``, a *descendant*
     query) -- a local one-off filter over ``row.find_all("td",
     recursive=False)`` instead, since promoting a generalized
     "direct-child :matches()" helper for this single call site would be
     speculative (the pbp parser's module docstring flags the same
     YAGNI-vs-promote tradeoff for its own module-private helpers).

**``URLEncoder.encode`` (deprecated single-arg Java overload) ->
``urllib.parse.quote_plus``.** Both encode space as ``+`` and leave
alphanumerics plus ``.``/``-``/``_``/``~`` unescaped -- verified against the
oracle's literal ``'1::Penn+St.'`` (space encoded, ``.`` left bare).

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of ``TeamIdParser.scala`` and ``TeamScheduleParser.scala``
from `Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_
(package ``org.piggottfamily.cbb_explorer``), which is licensed under the
Apache License, Version 2.0 (the upstream repo's ``LICENSE`` file; full text
at `<http://www.apache.org/licenses/LICENSE-2.0>`_). Per Apache-2.0 Section
4's redistribution-of-derivative-works obligations, sportsdataverse-py
(itself MIT-licensed) retains the upstream copyright notice for this
derivative::

    Copyright (c) Alex-At-Home (https://github.com/Alex-At-Home) and
    contributors. Licensed under the Apache License, Version 2.0.

See ``THIRD_PARTY_NOTICES.md`` at the repository root for the full
third-party attribution entry.

**Landmine index (reachable scalar division).** None. Every computation in
this module is string formatting/URL-encoding, guarded list/dict lookups, or
``re.search``/``re.fullmatch`` -- no division site exists to enumerate.

Example::

    from sportsdataverse.mbb.mbb_ncaa_team_parsers import get_neutral_games

    with open("tests/fixtures/ncaa/test_schedule.html", encoding="utf-8") as f:
        html = f.read()
    result = get_neutral_games("test_schedule.html", html, format_version=0)
    if isinstance(result, list):
        raise RuntimeError(result)  # list[ParseError]
    team, neutral_dates = result

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- R women's basketball companion package
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Optional, Union
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from bs4.element import Tag

from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError, build_sub_error, enrich_sub_error
from sportsdataverse.mbb.mbb_ncaa_html import jsoup_text, parse_html, select_contains, select_matching
from sportsdataverse.mbb.mbb_ncaa_models import ConferenceId, TeamId

__all__ = [
    "get_team_triples",
    "build_lineup_cli_array",
    "build_available_team_list",
    "ScheduleBuilders",
    "v0_builders",
    "v1_builders",
    "get_neutral_games",
]

#: Error-reporter location tags (``` `ncaa.get_team_ids` ```/
#: ``` `ncaa.get_neutral_games` ```, ``TeamIdParser.scala:35`` /
#: ``TeamScheduleParser.scala:33``).
_LOCATION_GET_TEAM_IDS = "ncaa.get_team_ids"
_LOCATION_GET_NEUTRAL_GAMES = "ncaa.get_neutral_games"

#: ``ParseUtils.enrich_sub_error``/``build_sub_error``'s ``base_id`` at the
#: no-team-name call site (``` `parent_fills_in` = "" ```,
#: ``ExtractorUtils.scala:20``).
_PARENT_FILLS_IN = ""


def _build_request_error(location: str, filename: str, exc: Exception) -> list[ParseError]:
    """``ParseUtils.build_request``'s exception branch -- this module's two
    entry points share the same shape (mirrors ``parse_roster``'s /
    ``get_box_lineup``'s precedent)."""
    return [ParseError(location=location, id=f"[{filename}]" if filename else "", messages=[f"Exception=[{exc}]"])]


# ---------------------------------------------------------------------------
# TeamIdParser (``TeamIdParser.scala:27-125``)
# ---------------------------------------------------------------------------

_ID_REGEX_OLD = re.compile(r"/teams/([0-9]+)")
_ID_REGEX_NEW = re.compile(r"/team/([0-9.]+/[0-9]+)")
_TEAM_CONF_REGEX = re.compile(r"^(.*) \(([A-Za-z0-9 .'-]+)\)$")
_CONFERENCE_REGEX = re.compile(r"([A-Za-z].*)")


def _team_id_finder(row: Tag) -> Optional[str]:
    """``e >?> element("a.skipMask")`` -> ``.attr("href")`` matched against
    ``id_regex_new``/``id_regex_old`` (``:43-50`` -- Scala's match tries the
    new-format URL shape first; both regexes are full-string matches, since
    Scala's ``case regex(id) =>`` extractor calls ``Matcher.matches()``)."""
    a = row.select_one("a.skipMask")
    if a is None:
        return None
    href = str(a.get("href", ""))
    m = _ID_REGEX_NEW.fullmatch(href)
    if m is not None:
        return m.group(1)
    m = _ID_REGEX_OLD.fullmatch(href)
    if m is not None:
        return m.group(1)
    return None


def _team_name_finder(row: Tag, old_format: bool) -> Optional[Union[str, tuple[str, str]]]:
    """``e >?> element("a.skipMask")`` -> ``.text`` -> ``Either[String,
    (String, String)]`` (``:54-58``). ``old_format=True`` always yields the
    plain name (Scala's ``Left(s)``, ported as a bare ``str``); otherwise
    only a ``"Team (Conf)"``-shaped text yields the ``(team, conf)`` pair
    (``Right``, ported as a ``tuple[str, str]``) -- anything else is
    ``None`` (the row is skipped by the caller)."""
    a = row.select_one("a.skipMask")
    if a is None:
        return None
    text = jsoup_text(a)
    if old_format:
        return text
    m = _TEAM_CONF_REGEX.fullmatch(text)
    if m is None:
        return None
    return (m.group(1), m.group(2))


def _team_conference_finder(row: Tag) -> Optional[str]:
    """``e >?> element("td:has(a.skipMask) + td")`` -> ``.text`` -> must
    start with a letter (``:60-65``) -- fully plain CSS (``:has()`` + the
    ``+`` adjacent-sibling combinator), direct ``select_one``."""
    td = row.select_one("td:has(a.skipMask) + td")
    if td is None:
        return None
    text = jsoup_text(td)
    m = _CONFERENCE_REGEX.fullmatch(text)
    if m is None:
        return None
    return m.group(1)


def get_team_triples(
    filename: str, in_html: str, old_format: bool = False
) -> Union[list[tuple[TeamId, str, ConferenceId]], list[ParseError]]:
    """Extracts ``(team, NCAA id, conference)`` triples from a saved NCAA
    team-list/attendance page (``TeamIdParser.get_team_triples``,
    ``TeamIdParser.scala:69-91``).

    Args:
        filename: The source file name, used only for error reporting.
        in_html: The raw team-list-page HTML.
        old_format: ``True`` for pages where the team name and conference
            are in separate ``<td>``s; ``False`` (default) for pages where
            the conference is embedded in the team-name cell as
            ``"Team (Conf)"``.

    Returns:
        One ``(TeamId, ncaa_id, ConferenceId)`` triple per row that has both
        a resolvable id and name/conference (rows missing either are
        silently skipped, matching the Scala's ``case _ => Nil``), or a
        single-element ``list[ParseError]`` if the HTML itself fails to
        parse.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_team_parsers import get_team_triples

            with open("tests/fixtures/ncaa/test_attendance_list.html", encoding="utf-8") as f:
                html = f.read()
            result = get_team_triples("test_attendance_list.html", html, old_format=True)
    """
    try:
        soup = parse_html(in_html)
    except Exception as exc:  # pragma: no cover - bs4/lxml is lenient; mirrors Scala's Try(request)
        return _build_request_error(_LOCATION_GET_TEAM_IDS, filename, exc)

    triples: list[tuple[TeamId, str, ConferenceId]] = []
    for row in soup.select("tr:has(td:has(a.skipMask))"):
        # All three finders are pure (no side effects), so -- unlike the
        # Scala's eager 3-tuple evaluation -- `_team_conference_finder` is
        # only called in the old-format branch that actually needs it;
        # the new-format (`Right`) branch ignores it entirely (Scala's `_`
        # wildcard), same as the Scala.
        name_result = _team_name_finder(row, old_format)
        team_id_str = _team_id_finder(row)
        if isinstance(name_result, tuple) and team_id_str is not None:
            name, conf = name_result
            triples.append((TeamId(name), team_id_str, ConferenceId(conf)))
        elif isinstance(name_result, str) and team_id_str is not None:
            conf_str = _team_conference_finder(row)
            if conf_str is not None:
                triples.append((TeamId(name_result), team_id_str, ConferenceId(conf_str)))
        # else: one of the required fields is missing -- skip the row (`case _ => Nil`).
    return triples


def build_lineup_cli_array(in_triples: list[tuple[TeamId, str, ConferenceId]]) -> dict[ConferenceId, str]:
    """Builds the per-conference team array for ``lineups-cli.sh`` files
    (``TeamIdParser.build_lineup_cli_array``, ``TeamIdParser.scala:94-100``).

    **Iteration-order note (upstream-DISABLED context).** Scala's
    ``List.groupBy`` returns an immutable ``Map`` whose iteration order is
    hash-bucket-dependent, not insertion order -- the disabled oracle's
    expected ``Map.toList`` ordering (``SEC`` before ``B1G``) reflects that
    JVM-specific hashing, not a documented contract. This port uses a plain
    ``dict`` (Python 3.7+ preserves insertion order), the natural pythonic
    choice; since the upstream test asserting a specific cross-conference
    order is itself permanently disabled (see the module docstring), there
    is no live oracle to match here regardless of dict vs hash-map ordering.

    Args:
        in_triples: ``(team, ncaa_id, conference)`` triples, e.g. from
            :func:`get_team_triples`.

    Returns:
        Conference -> newline-joined ``"   'ncaa_id::URL-encoded team
        name'"`` lines, one per team in that conference (in encounter
        order).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_team_parsers import build_lineup_cli_array
            from sportsdataverse.mbb.mbb_ncaa_models import ConferenceId, TeamId

            triples = [(TeamId("Penn St."), "1", ConferenceId("B1G"))]
            build_lineup_cli_array(triples)[ConferenceId("B1G")]
            # "   '1::Penn+St.'"
    """
    groups: dict[ConferenceId, list[str]] = {}
    for team_id, ncaa_id, conf in in_triples:
        groups.setdefault(conf, []).append(f"   '{ncaa_id}::{quote_plus(team_id.name)}'")
    return {conf: "\n".join(lines) for conf, lines in groups.items()}


def build_available_team_list(
    in_by_year: dict[str, list[tuple[TeamId, str, ConferenceId]]],
) -> dict[ConferenceId, Callable[[str], str]]:
    """Builds a per-conference team-index JSON fragment for
    ``cbb-on-off-analyzer`` (``TeamIdParser.build_available_team_list``,
    ``TeamIdParser.scala:105-124``) -- the caller inserts the app-specific
    index key to get the final JSON string.

    See :func:`build_lineup_cli_array`'s docstring for why this port doesn't
    attempt to reproduce Scala's hash-map iteration order (both the
    conference-level and, here, the team-level grouping) -- the upstream
    oracle covering this ordering is itself permanently disabled.

    Args:
        in_by_year: Season-key (e.g. ``"2018/9"``) -> that season's
            ``(team, ncaa_id, conference)`` triples, e.g. from repeated
            :func:`get_team_triples` calls.

    Returns:
        Conference -> a function ``index_key -> JSON-fragment string``, one
        ``' "team": [ ... ],'`` block per team in that conference (each
        block listing every season that team appeared in, in encounter
        order).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_team_parsers import build_available_team_list
            from sportsdataverse.mbb.mbb_ncaa_models import ConferenceId, TeamId

            by_year = {"2018/9": [(TeamId("Kentucky"), "450591", ConferenceId("SEC"))]}
            build_available_team_list(by_year)[ConferenceId("SEC")]("test")
    """
    by_conf: dict[ConferenceId, list[tuple[TeamId, str]]] = {}
    for year_key, triples in in_by_year.items():
        for team_id, _ncaa_id, conf in triples:
            by_conf.setdefault(conf, []).append((team_id, year_key))

    def _make_by_conf_str(entries: list[tuple[TeamId, str]]) -> Callable[[str], str]:
        by_team: dict[TeamId, list[str]] = {}
        for team_id, year_key in entries:
            by_team.setdefault(team_id, []).append(year_key)

        def by_conf_str(key: str) -> str:
            blocks = []
            for team_id, year_keys in by_team.items():
                lines = "\n".join(
                    f'   {{ team: "{team_id.name}", year: "{year_key}", gender: "Men", index_template: "{key}" }},'
                    for year_key in year_keys
                )
                blocks.append(f' "{team_id.name}": [\n{lines}\n ],')
            return "\n".join(blocks)

        return by_conf_str

    return {conf: _make_by_conf_str(entries) for conf, entries in by_conf.items()}


# ---------------------------------------------------------------------------
# TeamScheduleParser (``TeamScheduleParser.scala:25-96``)
# ---------------------------------------------------------------------------

_NEUTRAL_ROW_REGEX = r".*[@][a-zA-Z]+.*"
_NEUTRAL_DATE_REGEX = r"[0-9]+/[0-9]+/[0-9]+"


def _direct_child_tds_matching(row: Tag, regex: str) -> list[str]:
    """``> td:matches(regex)`` -- direct children only, the one selector
    shape none of ``mbb_ncaa_html.py``'s helpers express (see the module
    docstring's step 4)."""
    return [jsoup_text(td) for td in row.find_all("td", recursive=False) if re.search(regex, jsoup_text(td))]


def _v0_team_name_finder(doc: BeautifulSoup) -> Optional[str]:
    """``fieldset > legend > img[alt]`` (``:41-42``) -- fully plain CSS."""
    img = doc.select_one("fieldset > legend > img[alt]")
    return str(img.get("alt", "")) if img is not None else None


def _v0_neutral_game_finder(doc: BeautifulSoup) -> list[str]:
    """``legend:contains(Schedule/Results) + table tr:has(td:matches(AT))
    > td:matches(DATE)`` (``:44-48``) -- decomposed per the module
    docstring's four-step recipe."""
    dates: list[str] = []
    for legend in select_contains(doc, "legend", "Schedule/Results"):
        table = legend.find_next_sibling("table")
        if table is None:
            continue
        for row in table.select("tr"):
            if not select_matching(row, "td", _NEUTRAL_ROW_REGEX):
                continue
            dates.extend(_direct_child_tds_matching(row, _NEUTRAL_DATE_REGEX))
    return dates


def _v1_team_name_finder(doc: BeautifulSoup) -> Optional[str]:
    """``div.card-header > img[alt]`` (``:51-52``) -- fully plain CSS."""
    img = doc.select_one("div.card-header > img[alt]")
    return str(img.get("alt", "")) if img is not None else None


def _v1_neutral_game_finder(doc: BeautifulSoup) -> list[str]:
    """``div.card-header:contains(Schedule/Results) + div.card-body
    tr:has(td:matches(AT)) > td:matches(DATE)`` (``:54-58``) -- same
    four-step decomposition as :func:`_v0_neutral_game_finder`; the
    ``+``-sibling target is ``div.card-body`` instead of ``table``."""
    dates: list[str] = []
    for header in select_contains(doc, "div.card-header", "Schedule/Results"):
        body = header.find_next_sibling("div", class_="card-body")
        if body is None:
            continue
        for row in body.select("tr"):
            if not select_matching(row, "td", _NEUTRAL_ROW_REGEX):
                continue
            dates.extend(_direct_child_tds_matching(row, _NEUTRAL_DATE_REGEX))
    return dates


@dataclass(frozen=True)
class ScheduleBuilders:
    """One version-era's HTML finder functions (``TeamScheduleParser
    .base_builders``, ``TeamScheduleParser.scala:36-39``)."""

    team_name_finder: Callable[[BeautifulSoup], Optional[str]]
    neutral_game_finder: Callable[[BeautifulSoup], list[str]]


#: ``v0_builders``/``v1_builders`` (``:40-59``), exported at module level to
#: match the pbp parser's precedent (Task 5e.3's note: later tasks needing
#: inline low-level oracle transliteration should export builder tables the
#: same way, when the Scala oracle references them by name).
v0_builders = ScheduleBuilders(team_name_finder=_v0_team_name_finder, neutral_game_finder=_v0_neutral_game_finder)
v1_builders = ScheduleBuilders(team_name_finder=_v1_team_name_finder, neutral_game_finder=_v1_neutral_game_finder)

#: ``builders_from_version = Array(v0_builders, v1_builders)`` (``:60``).
_SCHEDULE_BUILDERS = (v0_builders, v1_builders)


def get_neutral_games(
    filename: str, in_html: str, format_version: int
) -> Union[tuple[TeamId, set[str]], list[ParseError]]:
    """Extracts the set of neutral/away-marked game dates from a saved NCAA
    team-schedule page (``TeamScheduleParser.get_neutral_games``,
    ``TeamScheduleParser.scala:63-94``).

    Args:
        filename: The source file name, used only for error reporting.
        in_html: The raw team-schedule-page HTML.
        format_version: ``0`` for the legacy ``fieldset``/``legend`` layout,
            ``1`` for the 2018+ ``div.card-header``/``div.card-body`` layout.

    Returns:
        ``(team, neutral_game_dates)`` -- the team parsed from the page's
        image ``alt`` attribute, and every ``"MM/DD/YYYY"`` date string
        found on an ``"@Opponent"``-marked row -- or a single-element
        ``list[ParseError]`` if the HTML fails to parse, or the team name
        can't be located.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_team_parsers import get_neutral_games

            with open("tests/fixtures/ncaa/test_schedule.html", encoding="utf-8") as f:
                html = f.read()
            result = get_neutral_games("test_schedule.html", html, format_version=0)
            if isinstance(result, list):
                raise RuntimeError(result)  # list[ParseError]
            team, neutral_dates = result
    """
    try:
        soup = parse_html(in_html)
    except Exception as exc:  # pragma: no cover - bs4/lxml is lenient; mirrors Scala's Try(request)
        return _build_request_error(_LOCATION_GET_NEUTRAL_GAMES, filename, exc)

    builders = _SCHEDULE_BUILDERS[format_version]
    team_name = builders.team_name_finder(soup)
    if team_name is None:
        return enrich_sub_error(
            _LOCATION_GET_NEUTRAL_GAMES,
            filename,
            build_sub_error(_PARENT_FILLS_IN, error="Failed to find team name in image alt"),
        )

    candidate_neutral_games = builders.neutral_game_finder(soup)
    return (TeamId(team_name.strip()), set(candidate_neutral_games))
