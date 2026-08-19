"""NCAA box-score HTML parser: ``get_box_lineup`` (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) ``BoxscoreParser.scala`` -- the
second of six Phase-5e modules. Turns a saved NCAA box-score page into a
:class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` whose ``players``
field is the team's full box-score lineup (not yet split into stints -- that
is the play-by-play parser's job, Task 5e.3).

**v0/v1 selector tables.** As with the roster parser (Task 5e.1), two eras of
the NCAA stats site ship different HTML shapes for the same box-score data.
Both eras are ported as a pair of finder-function bundles (``_BUILDERS_V0`` /
``_BUILDERS_V1``, indexed by ``format_version`` exactly like the Scala's
``builders_from_version = Array(v0_builders, v1_builders)``).

**Selector translations (JSoup -> bs4, this task's findings):**

* ``table.mytable[width~=[45]0%]`` (a JSoup attribute-*regex* match, not
  plain CSS) -> :func:`~sportsdataverse.mbb.mbb_ncaa_html.attr_regex_filter`
  over ``doc.select("div#contentarea table.mytable")``'s ``width`` attribute
  with pattern ``r"[45]0%"``. The v0 team/score finders share this table
  lookup (:func:`_v0_score_tables`).
* ``td.boldtext:contains(Game Date:) + td`` -- **the review-flagged
  ``:contains()`` case-insensitivity gotcha.** JSoup's ``:contains()`` is
  documented case-insensitive substring containment; soupsieve's
  ``:-soup-contains()`` is case-SENSITIVE with no case-insensitive variant.
  Ported via the new
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.select_contains` helper (added
  to ``mbb_ncaa_html.py`` by this task) rather than ``:-soup-contains()``:
  ``select_contains(doc, "td.boldtext", "Game Date:")``, then the ``+ td``
  adjacent-sibling step is a plain
  :meth:`~bs4.element.Tag.find_next_sibling` call on the first match (JSoup's
  ``element(...)`` extractor takes only the first match anyway).
* ``div#contentarea div.header_menu + table.mytable[width=1000px] td a[href]``
  / ``div#contentarea br + table.mytable[width=1000px] td a[href]`` (v0
  ``boxscore_finder``) -- **fully plain CSS**, no regex/contains needed.
  Soupsieve supports the ``+`` adjacent-sibling combinator natively (it is
  standard CSS, not a JSoup extension), so these translate to a direct
  ``doc.select(...)`` call (with the attribute value quoted,
  ``[width="1000px"]``, since an unquoted ``1000px`` is not a valid CSS
  identifier token).
* ``div.card-header img[alt]`` (v1 ``team_finder``) -- plain CSS attribute
  existence, direct ``doc.select(...)``; returns each element's ``alt``
  attribute (:meth:`Tag.get`), not :func:`jsoup_text`.
* ``td[style~=font-size:36px]:matchesOwn([0-9]+)`` (v1 ``score_finder``) --
  a *compound* selector mixing an attribute-regex term (unsupported by
  soupsieve) with a ``:matchesOwn()`` term (also unsupported). Ported as a
  two-step manual filter: :func:`~sportsdataverse.mbb.mbb_ncaa_html.attr_regex_filter`
  over ``doc.select("div.table-responsive td[style]")`` for the ``style``
  regex, then the new
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.filter_matching_own` helper
  (added to ``mbb_ncaa_html.py`` by this task, since
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.select_matching_own` only
  accepts a fresh ``root.select(selector)`` call, not a pre-filtered
  candidate list) for the ``:matchesOwn`` regex.
* ``div.table-responsive > table table td:matchesOwn([0-9]+/[0-9]+/[0-9]+)``
  (v1 ``date_finder``) -- a single plain-CSS structural selector (child +
  descendant combinators only, no attribute regex), so
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.select_matching_own` handles it
  directly in one call.
* ``table.dataTable`` (v1 ``boxscore_finder``) -- ``doc.select(...)`` for
  every data table, then ``tables[0 if target_team_first else 1]`` (guarded
  against a short list), then ``td a[href]`` within that one table.

**Coverage note.** Only the v0 path is exercised by the vendored fixture
(``tests/fixtures/ncaa/test_lineup.html`` is v0-only; ``BoxscoreParserTests
.scala`` only calls ``get_box_lineup`` with ``format_version=0``) -- same
situation as the roster parser's v1 table (Task 5e.1). The v1 selector table
here is implemented per spec but untested by this task.

**``players_missing_from_boxscore`` (Task 5b.1 deferral, closed here).**
``inject_validated_players`` needs
:data:`~sportsdataverse.mbb.mbb_ncaa_data_quality.players_missing_from_boxscore`
-- ``DataQualityIssues.scala:16-34``'s table of players entirely absent from
a box-score page (4 team/season entries), deferred by Task 5b.1 since this
was its only consumer. Appended (verbatim) to ``mbb_ncaa_data_quality.py`` by
this task.

**``inject_validated_players``'s un-threaded ``tidy_ctx`` (ported verbatim).**
Unlike :func:`~sportsdataverse.mbb.mbb_ncaa_stints.build_partial_lineup_list`
(which threads :class:`~sportsdataverse.mbb.mbb_ncaa_names.TidyPlayerContext`'s
updated resolution cache through each successive
:func:`~sportsdataverse.mbb.mbb_ncaa_names.tidy_player` call),
``BoxscoreParser.inject_validated_players``'s Scala body computes ``tidy_ctx``
ONCE before its ``.map`` over ``ordered_lineup_from_box``, then calls
``val (fixed_player, _) = tidy_player(player, tidy_ctx)`` inside the loop --
the updated context returned by each call is discarded (``_``), so every
iteration resolves against the SAME original (never-updated) ``tidy_ctx``.
This is almost certainly a Scala oversight (the discarded ``_`` binding is a
classic "forgot to thread the accumulator" shape), but it is upstream
behavior baked into the released hoop-explorer.com pipeline -- ported
byte-for-byte per this project's faithful-port discipline, not "fixed" into
a threaded version that would silently change resolution behavior for any
game with >1 mismatched box-score name.

**Extra-players ``Set`` ordering (a second instance of the Scala
``HashMap``/``HashSet`` iteration-order problem, ported by re-application of
the algorithm derived in Task 5e.1).** ``inject_validated_players``'s trailer
-- ``(just_players ++ other_players ++ manual_extra_players)
.filterNot(validated_ordered_lineup_set).toSet`` -- converts a
deduplicated ``List[String]`` to a Scala ``Set[String]`` before appending it
back onto the validated lineup. For >4 elements this is the SAME
``scala.collection.immutable.HashSet`` hash-trie iteration order documented
at length in ``mbb_ncaa_roster_parser.py`` (Task 5e.1) for
``Map[PlayerCodeId, _]`` -- except a plain ``Set[String]`` hashes its raw
``String`` elements directly (Java's own polynomial ``String.hashCode()``),
not a case-class ``productHash``. :func:`_scala_string_set_order` reuses the
already-JVM-validated ``improve``/radix-key trie-order math from that task
(:func:`_hashmap_improve` / :func:`_hashmap_radix_key`, duplicated here --
NOT imported from ``mbb_ncaa_roster_parser.py``, whose private helpers this
task's brief marks read-only/do-not-refactor), fed a plain
:func:`_java_string_hash` this time instead of the roster parser's
case-class ``productHash``. **This path is UNTESTED by any current oracle**:
every ``BoxscoreParserTests.scala`` call uses ``external_roster=(Nil, Nil)``
and a team/year absent from ``players_missing_from_boxscore``, so
``just_players``/``other_players``/``manual_extra_players`` are always empty
and the trailing ``Set`` is always empty too -- the extra-players branch (and
therefore its ordering) is exercised by none of this task's tests, ported
for correctness against a future non-empty-roster caller (e.g. Task 5e.3's
pipeline orchestration, which will call ``get_box_lineup`` with a real
``RosterEntry`` list).

**``get_box_lineup``'s ``players`` field is NOT sorted.** Re-reading
``BoxscoreParser.scala`` end to end: no step of the pipeline sorts the
lineup -- ``validate_box_score`` returns ``lineup.map(build_player_code(...))``
in the SAME order as its input, and (with an empty external roster, as in
every current oracle test) ``inject_validated_players`` passes
``ordered_lineup_from_box`` straight through unchanged. The oracle's
``lineup ==> {...}.map(build_player_code(_, None)).sortBy(_.code)`` assertion
therefore only agrees with the actual (unsorted, natural-HTML-order) output
because ``test_lineup.html``'s players happen to already be in
ascending-code order (single-digit player-name suffixes ``S1``..``S9``, and
TeamB's odd ``SArname`` row naturally sorts -- and appears in the HTML --
last). This port does NOT add a sort to match the test's cosmetic
``.sortBy`` call; it preserves natural HTML order end to end, faithfully
matching what ``get_box_lineup`` actually computes.

**``parse_period_from_filename``'s vestigial ``Either``.** The Scala
signature is ``Either[ParseError, Int]``, but both match arms
(``case filename_parser(period_str) => Right(...)`` and
``case _ => Right(1)``) return ``Right`` -- there is no reachable ``Left``.
Ported here as a plain ``int``-returning function rather than a
``Union[int, ParseError]``, documenting the dead error branch instead of
inventing an unreachable one.

Apache-2.0 third-party port — see the ``NOTICE`` file at the repository root for the upstream copyright and full attribution.

**Either convention.** Scala's ``Either[ParseError, X]`` / ``Either[List[
ParseError], X]`` both become plain ``X | ParseError`` / ``X | list[
ParseError]`` return-type unions (the 5b/5e.1 precedent) -- callers
``isinstance``-check for ``ParseError`` (single) or ``list`` (already-listed)
to detect the error branch. ``get_box_lineup`` itself is the top-level
``Either[List[ParseError], LineupEvent]`` function; every sub-helper it
calls (``parse_date``, ``parse_final_score``, ``parse_players_from_boxscore``,
``validate_box_score``) returns a single ``ParseError`` on failure (matching
each one's own oracle-tested ``Either[ParseError, X]`` signature), which
``get_box_lineup`` wraps into a location-enriched single-element list via
:func:`_completer` (the Python translation of the Scala's
``ParseUtils.enrich_sub_error(location, filename)`` partial application).

**Landmine index (reachable scalar division).** None. Every computation in
this module is string splitting/regex, guarded list/dict lookups, or integer
bit arithmetic (the Set-ordering helpers, duplicated from the same
already-verified derivation as the roster parser's Map-ordering helpers). No
division site exists.

Example::

    from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
    from sportsdataverse.mbb.mbb_ncaa_models import TeamId

    with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
        html = f.read()
    result = get_box_lineup("test_p1.html", html, TeamId("TeamA"), format_version=0)
    if isinstance(result, list):
        raise RuntimeError(result)  # list[ParseError]
    box_lineup = result  # LineupEvent
    len(box_lineup.players)

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- R women's basketball companion package
"""

from __future__ import annotations

import re
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Callable, Optional, Union

from bs4 import BeautifulSoup
from bs4.element import Tag

from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError, build_sub_error, players_missing_from_boxscore
from sportsdataverse.mbb.mbb_ncaa_html import (
    attr_regex_filter,
    current_ncaa_team_alts,
    filter_matching_own,
    jsoup_text,
    parse_html,
    select_contains,
    select_matching_own,
)
from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PlayerCodeId,
    RosterEntry,
    Score,
    ScoreInfo,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context, tidy_player
from sportsdataverse.mbb.mbb_ncaa_stints import (
    build_player_code,
    name_in_v0_box_format,
    parse_team_name,
    start_time_from_period,
)

__all__ = [
    "get_box_lineup",
    "inject_validated_players",
    "parse_period_from_filename",
    "parse_date",
    "parse_final_score",
    "parse_players_from_boxscore",
    "validate_box_score",
]

#: Error-reporter location tag (``` `ncaa.parse_boxscore` ```, ``BoxscoreParser.scala:33``).
_LOCATION_PARSE_BOXSCORE = "ncaa.parse_boxscore"

#: ``[^_]+_p([0-9]+)[.][^.]*`` -- e.g. ``test_p2.html`` -> period ``2``
#: (``BoxscoreParser.parse_period_from_filename``, ``:288``). Scala's
#: ``s match { case filename_parser(g) => ... }`` requires a FULL match ->
#: ``re.fullmatch``.
_FILENAME_PERIOD_RE = re.compile(r"[^_]+_p([0-9]+)[.][^.]*")

#: ``"MM/dd/yyyy"`` (``BoxscoreParser.parse_date``, ``:302``).
_DATE_FORMAT = "%m/%d/%Y"


# ---------------------------------------------------------------------------
# v0/v1 builder tables (``base_builders``/``v0_builders``/``v1_builders``,
# ``BoxscoreParser.scala:36-117``)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _BoxscoreBuilders:
    """One version-era's HTML finder functions (``BoxscoreParser.base_builders``,
    ``BoxscoreParser.scala:36-44``)."""

    team_finder: Callable[[BeautifulSoup], list[str]]
    score_finder: Callable[[BeautifulSoup], list[str]]
    date_finder: Callable[[BeautifulSoup], Optional[str]]
    boxscore_finder: Callable[[BeautifulSoup, bool], Optional[list[Tag]]]


def _v0_score_tables(doc: BeautifulSoup) -> list[Tag]:
    """``table.mytable[width~=[45]0%]`` -- a JSoup attribute-*regex* match
    (unsupported by soupsieve), shared by :func:`_v0_team_finder` and
    :func:`_v0_score_finder` (both scoped to the same period-scores table;
    ``BoxscoreParser.scala:47-61`` comment: "2020+ is 40%, 2019- is 50%")."""
    return attr_regex_filter(doc.select("div#contentarea table.mytable"), "width", r"[45]0%")


def _v0_team_finder(doc: BeautifulSoup) -> list[str]:
    """``:47-52``."""
    els: list[Tag] = []
    for table in _v0_score_tables(doc):
        els.extend(table.select("td a[href]"))
    return [jsoup_text(el) for el in els]


def _v0_score_finder(doc: BeautifulSoup) -> list[str]:
    """``:54-61``."""
    els: list[Tag] = []
    for table in _v0_score_tables(doc):
        els.extend(table.select('td[align="right"]'))
    return [jsoup_text(el) for el in els]


def _v0_date_finder(doc: BeautifulSoup) -> Optional[str]:
    """``td.boldtext:contains(Game Date:) + td`` (``:63-64``) -- see the
    module docstring's ``:contains()`` case-insensitivity note."""
    candidates = select_contains(doc, "td.boldtext", "Game Date:")
    if not candidates:
        return None
    sibling = candidates[0].find_next_sibling("td")
    return jsoup_text(sibling) if sibling is not None else None


def _v0_boxscore_finder(doc: BeautifulSoup, target_team_first: bool) -> Optional[list[Tag]]:
    """``:66-78`` -- fully plain CSS (the ``+`` adjacent-sibling combinator
    is standard, not a JSoup extension)."""
    if target_team_first:
        els = doc.select('div#contentarea div.header_menu + table.mytable[width="1000px"] td a[href]')
    else:
        els = doc.select('div#contentarea br + table.mytable[width="1000px"] td a[href]')
    return list(els) if els else None


_BUILDERS_V0 = _BoxscoreBuilders(
    team_finder=_v0_team_finder,
    score_finder=_v0_score_finder,
    date_finder=_v0_date_finder,
    boxscore_finder=_v0_boxscore_finder,
)


def _v1_team_finder(doc: BeautifulSoup) -> list[str]:
    """``div.card-header img[alt]`` -> the ``alt`` attribute, not
    :func:`~sportsdataverse.mbb.mbb_ncaa_html.jsoup_text` (``:82-85``).

    Falls back to :func:`~sportsdataverse.mbb.mbb_ncaa_html.current_ncaa_team_alts`
    for current (2026) markup, which no longer wraps the team logos in a
    ``div.card-header`` (see that helper's drift note)."""
    ported = [str(el.get("alt", "")) for el in doc.select("div.card-header img[alt]")]
    return ported or current_ncaa_team_alts(doc)


def _v1_score_finder(doc: BeautifulSoup) -> list[str]:
    """``td[style~=font-size:36px]:matchesOwn([0-9]+)`` (``:87-92``) -- a
    compound selector mixing an attribute-regex term with a ``:matchesOwn``
    term, ported as a 2-step manual filter (see the module docstring)."""
    candidates = doc.select("div.table-responsive td[style]")
    style_matched = attr_regex_filter(candidates, "style", r"font-size:36px")
    own_matched = filter_matching_own(style_matched, r"[0-9]+")
    return [jsoup_text(el) for el in own_matched]


def _v1_date_finder(doc: BeautifulSoup) -> Optional[str]:
    """``div.table-responsive > table table td:matchesOwn([0-9]+/[0-9]+/[0-9]+)``
    (``:94-97``) -- a single plain-CSS structural selector, handled directly
    by :func:`~sportsdataverse.mbb.mbb_ncaa_html.select_matching_own`."""
    matches = select_matching_own(doc, "div.table-responsive > table table td", r"[0-9]+/[0-9]+/[0-9]+")
    return jsoup_text(matches[0]) if matches else None


def _v1_boxscore_finder(doc: BeautifulSoup, target_team_first: bool) -> Optional[list[Tag]]:
    """``:99-115`` -- picks the 1st (``target_team_first``) or 2nd data
    table, then its ``td a[href]`` descendants."""
    tables = doc.select("table.dataTable")
    idx = 0 if target_team_first else 1
    if idx >= len(tables):
        return None
    els = tables[idx].select("td a[href]")
    return list(els) if els else None


_BUILDERS_V1 = _BoxscoreBuilders(
    team_finder=_v1_team_finder,
    score_finder=_v1_score_finder,
    date_finder=_v1_date_finder,
    boxscore_finder=_v1_boxscore_finder,
)

#: Indexed by ``format_version`` (``BoxscoreParser.builders_from_version``, ``:117``).
_BUILDERS = (_BUILDERS_V0, _BUILDERS_V1)


# ---------------------------------------------------------------------------
# Utils (``BoxscoreParser.scala:284-404``)
# ---------------------------------------------------------------------------


def parse_period_from_filename(filename: str) -> int:
    """Gets the box score's period from the filename (``BoxscoreParser
    .parse_period_from_filename``, ``:285-296``). See the module docstring's
    "vestigial ``Either``" note -- there is no reachable error case, so this
    returns a plain ``int`` rather than a ``Union[int, ParseError]``.

    Args:
        filename: The source file name, e.g. ``"test_p2.html"``.

    Returns:
        The 1-indexed period parsed out of the filename, or ``1`` if it
        doesn't match the expected ``..._p<period>.<ext>`` shape.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import parse_period_from_filename
            parse_period_from_filename("test_p2.html")  # 2
            parse_period_from_filename("no_match.html")  # 1
    """
    m = _FILENAME_PERIOD_RE.fullmatch(filename)
    return int(m.group(1)) if m is not None else 1


def parse_date(date: Optional[str]) -> Union[datetime, ParseError]:
    """Parses dates of the format ``'12/03/2017'`` (``BoxscoreParser.parse_date``,
    ``:299-329``).

    Args:
        date: The raw date-cell text (already whitespace-collapsed by the
            caller's ``date_finder``), optionally with a trailing time
            segment (e.g. ``"12/10/2018 17:00"``) which is discarded.

    Returns:
        A naive :class:`datetime.datetime` with ``hour=17`` (Scala's
        ``withHourOfDay(17)``, "an early evening game, no reason") on
        success, or a :class:`~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError`
        if ``date`` is ``None`` or doesn't match ``"MM/dd/yyyy"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import parse_date
            parse_date("12/10/2018")  # datetime(2018, 12, 10, 17, 0)
    """
    if date is None:
        return build_sub_error(error="Could not find date")
    trimmed = date.strip()
    token = trimmed.split(" ")[0]
    try:
        parsed = datetime.strptime(token, _DATE_FORMAT)
    except ValueError:
        return build_sub_error(error=f"Unexpected date format: [{trimmed}]")
    return parsed.replace(hour=17)


def parse_final_score(scores_per_period: list[str], target_team_first: bool) -> Union[Score, ParseError]:
    """Computes the final score from a list of per-period score strings
    (``BoxscoreParser.parse_final_score``, ``:331-360``).

    Args:
        scores_per_period: Every period's score-cell text for BOTH teams,
            in document order (e.g. ``[team_p1, team_p2, ..., team_total,
            opp_p1, opp_p2, ..., opp_total]``) -- an even-length list whose
            ``[n/2 - 1]`` and ``[n - 1]`` entries are the two teams' totals.
        target_team_first: Whether the team under analysis's scores come
            first in ``scores_per_period``.

    Returns:
        The ``(scored, allowed)`` :class:`~sportsdataverse.mbb.mbb_ncaa_models.Score`
        for the team under analysis, or a
        :class:`~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError` if
        ``scores_per_period`` has an odd length, fewer than 2 entries, or a
        non-integer total.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import parse_final_score
            parse_final_score(["1", "2"], target_team_first=True)  # Score(1, 2)
    """
    num_scores = len(scores_per_period)
    if num_scores >= 2 and num_scores % 2 == 0:
        first_str = scores_per_period[num_scores // 2 - 1]
        second_str = scores_per_period[num_scores - 1]
        try:
            score1 = int(first_str)
            score2 = int(second_str)
        except ValueError:
            return build_sub_error(
                error=f"Unexpected score format [one of the scores not integer]: [({first_str},{second_str})]"
            )
        return Score(score1, score2) if target_team_first else Score(score2, score1)
    return build_sub_error(error=f"Unexpected score format [odd number of values]: [{scores_per_period}]")


def parse_players_from_boxscore(boxscore_table: Optional[list[Tag]]) -> Union[list[str], ParseError]:
    """Gets the list of starters from the boxscore (``BoxscoreParser
    .parse_players_from_boxscore``, ``:363-385``).

    Args:
        boxscore_table: The player-name anchor elements found by a
            ``boxscore_finder`` (one ``<a>`` per player row), or ``None`` if
            the finder matched nothing.

    Returns:
        Each anchor's :func:`~sportsdataverse.mbb.mbb_ncaa_html.jsoup_text`,
        in document order, or a
        :class:`~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError` if
        ``boxscore_table`` is ``None`` or has fewer than 5 rows.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import parse_players_from_boxscore
            parse_players_from_boxscore(None)  # ParseError
    """
    if boxscore_table is None:
        return build_sub_error(error="Could not find boxscore table")
    if len(boxscore_table) >= 5:
        return [jsoup_text(el) for el in boxscore_table]
    return build_sub_error(error=f"Not enough rows in boxscore table: [{boxscore_table}]")


def validate_box_score(team: TeamId, lineup: list[str]) -> Union[list[PlayerCodeId], ParseError]:
    """Checks there are no duplicates in the lineup (``BoxscoreParser
    .validate_box_score``, ``:388-404``).

    Args:
        team: The team the lineup belongs to (feeds
            :func:`~sportsdataverse.mbb.mbb_ncaa_stints.build_player_code`'s
            team-scoped misspelling corrections).
        lineup: The raw player-name strings, in whatever order they were
            assembled by :func:`inject_validated_players`.

    Returns:
        ``lineup``, mapped to
        :class:`~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId` (same
        order, no sort -- see the module docstring's "not sorted" note).

        When two teammates collide on the ``{first-two-letters}{Surname}``
        scheme -- siblings, in practice -- **only the colliding players** are
        re-coded to ``{First}{Last}`` by :func:`_disambiguate_sibling_codes`;
        every other player keeps the Scala-faithful code. This is a
        DELIBERATE divergence from ``ExtractorUtils.scala``, which rejects
        the game: since a team's roster is the same all season, one sibling
        pair cost the team its ENTIRE season of lineups.

        A :class:`~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError` is
        returned only when widening cannot separate them, i.e. two players
        with the SAME full name -- genuinely ambiguous, so still an error.

        Callers must not re-derive a code from a name after this point:
        ``build_player_code`` would undo the widening and silently drop one
        twin. Use :func:`~sportsdataverse.mbb.mbb_ncaa_names.code_from_box`,
        which resolves against this roster.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import validate_box_score
            from sportsdataverse.mbb.mbb_ncaa_models import TeamId
            validate_box_score(TeamId("Team"), ["Player One", "Player Two"])
    """
    codes = [build_player_code(name, team) for name in lineup]
    if len(codes) != len({c.code for c in codes}):
        codes = _disambiguate_sibling_codes(codes)
    if len(codes) != len({c.code for c in codes}):
        return build_sub_error(error=f"Duplicate players: [{codes}]")
    return codes


_FIRST_PREFIX_LEN = 2


def _first_last(full_name: str) -> "tuple[str, str]":
    """``"Morris, Markieff"`` -> ``("Markieff", "Morris")``; also ``"First Last"``."""
    if "," in full_name:
        last, _, rest = full_name.partition(",")
        first = rest.strip().split(" ")[0] if rest.strip() else ""
        return first.strip(), last.strip()
    parts = [p for p in full_name.split(" ") if p]
    if len(parts) < 2:
        return (parts[0] if parts else ""), ""
    return parts[0], parts[-1]


def _disambiguate_sibling_codes(codes: "list[PlayerCodeId]") -> "list[PlayerCodeId]":
    """Re-code teammates whose player codes collide, instead of failing the game.

    ``build_player_code`` is a faithful port of ``ExtractorUtils.scala`` and
    keys a player as ``{first-two-letters}{Surname}``. Teammates sharing a
    surname AND their first two initials therefore collide -- which in practice
    means SIBLINGS, and college basketball is full of them:

        Kansas 2010     MaMorris     Markieff / Marcus Morris
        San Diego 2015  SoEderaine   Sophie / Sophia Ederaine
        Green Bay 2019  MaWolf       Madison / Mackenzie Wolf

    Rejecting the game was catastrophic rather than cautious: every game a team
    plays has the same roster, so ONE sibling pair deleted the team's whole
    season from `lineups` (and so from `matchup_stints` and `possessions`).
    Measured across 2010-2026, 79 D-I team-seasons were affected -- Kansas 2010
    published 0 of 36 games.

    Only the colliding players are re-coded, to the full first name plus
    surname (``MarkieffMorris`` / ``MarcusMorris``); every other player keeps
    the ported code untouched, so `lineup_key`s change ONLY for affected teams.
    The rule is a pure function of the name, so the play-by-play side derives
    the same code from the same roster and the join still lands.

    Two players with an identical full name still collide and still raise --
    that is a genuine ambiguity, not a coding artefact.

    **The widening is per-GAME, not per-season, and that is fine.** It fires
    only when both siblings appear on the SAME box, so in a game where only
    one of them dressed there is no collision and that player keeps the
    narrow code. Stanford 2015 shows it exactly: 18 games with
    ``KaileeJohnson`` + ``KayleeJohnson``, and 2 games where one sat, coded
    ``KaJohnson``. Nothing downstream keys on ``code``, so this does not
    split a player: the published ``lineups`` / ``possessions`` slots carry
    full names (``Johnson, Kaylee``), and the RAPM path keys on
    ``p["id"]`` -- also the full name -- with the code only carried
    alongside. ``code``'s job is within-game box-to-PBP matching, and within
    a game it is unique, which is the whole contract.

    Do NOT "fix" this by widening every code unconditionally: that would
    rewrite every player code in the corpus and break parity with the Scala
    oracle everywhere, to stabilise a field nothing reads across games.
    """
    from collections import Counter

    counts = Counter(c.code for c in codes)
    out: "list[PlayerCodeId]" = []
    for c in codes:
        if counts[c.code] < 2:
            out.append(c)
            continue
        first, last = _first_last(c.id.name)
        if not first or not last:
            out.append(c)
            continue
        widened = f"{first[:1].upper()}{first[1:].lower()}{last[:1].upper()}{last[1:].lower()}"
        out.append(replace(c, code=widened))
    return out


# ---------------------------------------------------------------------------
# Scala immutable.HashSet iteration-order emulation for a plain Set[String]
# -- re-application (NOT a fresh derivation) of the Task 5e.1
# improve/radix-key math from mbb_ncaa_roster_parser.py, duplicated here per
# this task's "do not refactor prior work" scope (that module's helpers are
# private). See the module docstring's "Extra-players Set ordering" note.
# ---------------------------------------------------------------------------


def _java_string_hash(s: str) -> int:
    """Java/Scala ``String.hashCode()``, 32-bit wraparound -- what a plain
    ``Set[String]`` hashes directly (no case-class ``productHash`` mixing,
    unlike ``mbb_ncaa_roster_parser.py``'s ``Map[PlayerCodeId, _]``)."""
    h = 0
    for ch in s:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return h


def _hashmap_improve(hcode: int) -> int:
    """``scala.collection.immutable.HashMap``/``HashSet``'s internal
    hash-smear (``improve``) -- identical math to
    ``mbb_ncaa_roster_parser._hashmap_improve`` (both collection types share
    the same underlying hash-trie implementation)."""
    h = hcode & 0xFFFFFFFF
    h = (h + (~(h << 9) & 0xFFFFFFFF)) & 0xFFFFFFFF
    h ^= h >> 14
    h = (h + ((h << 4) & 0xFFFFFFFF)) & 0xFFFFFFFF
    h ^= h >> 10
    return h & 0xFFFFFFFF


def _hashmap_radix_key(hcode: int) -> int:
    """Turns an improved hash into an ascending sort key reproducing the
    hash-trie's depth-first iteration order -- identical math to
    ``mbb_ncaa_roster_parser._hashmap_radix_key``."""
    h = hcode & 0xFFFFFFFF
    key = 0
    for i in range(7):  # 7 groups of 5 bits covers all 32 bits (ceil(32/5))
        key = (key << 5) | ((h >> (5 * i)) & 0x1F)
    return key


def _scala_string_set_order(items: list[str]) -> list[str]:
    """Reorders a deduplicated string list to match ``List[String]
    .toSet``'s iteration order: insertion order for <= 4 entries (Scala's
    ``Set1``..``Set4``), else the ``HashSet`` hash-trie order.

    Args:
        items: Already-deduplicated strings, in first-seen order.

    Returns:
        ``items`` reordered per Scala ``Set[String]`` semantics.
    """
    if len(items) <= 4:
        return items
    return sorted(items, key=lambda s: _hashmap_radix_key(_hashmap_improve(_java_string_hash(s))))


# ---------------------------------------------------------------------------
# Public surface (``BoxscoreParser.scala:119-279``)
# ---------------------------------------------------------------------------


def inject_validated_players(
    ordered_lineup_from_box: list[str],
    box_minus_players: LineupEvent,
    external_roster: tuple[list[str], list[RosterEntry]],
) -> list[str]:
    """Validates box players against the roster (if available) and any
    other available box scores (``BoxscoreParser.inject_validated_players``,
    ``:233-279``).

    See the module docstring's "un-threaded ``tidy_ctx``" note -- every
    fuzzy-resolution call inside the loop uses the SAME original context,
    never the updated one a call returns (ported verbatim, including this
    apparent Scala oversight).

    Args:
        ordered_lineup_from_box: The raw player-name strings scraped
            straight off the box-score page (already v0-normalized if the
            source was v1, by :func:`get_box_lineup`'s caller).
        box_minus_players: The in-progress
            :class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` (used
            only for its ``team`` field, both to scope the fuzzy-match
            context and to key
            :data:`~sportsdataverse.mbb.mbb_ncaa_data_quality.players_missing_from_boxscore`).
        external_roster: ``(other_players, roster_players)`` -- extra known
            player names, and a full team roster (if available) to validate
            against / fuzzy-correct box names onto.

    Returns:
        ``ordered_lineup_from_box`` with any name not found in
        ``roster_players`` fuzzy-corrected onto the closest roster name (if
        a roster was supplied at all), followed by any roster/other/
        known-missing players not already present in that corrected list
        (see the module docstring's "Extra-players Set ordering" note for
        this trailing group's order).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import inject_validated_players
            inject_validated_players(["Player One"], box_lineup, ([], []))
    """
    other_players, roster_players = external_roster

    tidy_ctx = build_tidy_player_context(replace(box_minus_players, players=[r.player_code_id for r in roster_players]))

    manual_extra_players = players_missing_from_boxscore.get(box_minus_players.team.team, {}).get(
        box_minus_players.team.year, []
    )

    just_players = [r.player_code_id.id.name for r in roster_players]
    just_players_set = set(just_players)

    validated_ordered_lineup: list[str] = []
    for player in ordered_lineup_from_box:
        if not just_players_set or player in just_players_set:
            validated_ordered_lineup.append(player)
        else:
            # NOTE: reuses the ORIGINAL tidy_ctx every iteration -- see the
            # module docstring's "un-threaded tidy_ctx" note.
            fixed_player, _ = tidy_player(player, tidy_ctx)
            validated_ordered_lineup.append(fixed_player)

    validated_ordered_lineup_set = set(validated_ordered_lineup)
    combined = just_players + other_players + manual_extra_players
    remaining = [p for p in combined if p not in validated_ordered_lineup_set]
    extra_players = _scala_string_set_order(list(dict.fromkeys(remaining)))

    return validated_ordered_lineup + extra_players


def get_box_lineup(
    filename: str,
    in_html: str,
    team_id: TeamId,
    format_version: int,
    external_roster: tuple[list[str], list[RosterEntry]] = ([], []),
    neutral_game_dates: AbstractSet[str] = frozenset(),
    home_team: Optional[str] = None,
    away_team: Optional[str] = None,
) -> Union[LineupEvent, list[ParseError]]:
    """Gets the boxscore lineup from the HTML page (``BoxscoreParser
    .get_box_lineup``, ``:122-222``).

    Args:
        filename: The source file name -- used both for error reporting and
            to extract the period via :func:`parse_period_from_filename`
            (e.g. ``"test_p2.html"``).
        in_html: The raw box-score-page HTML.
        team_id: The team this box score is being parsed for.
        format_version: ``0`` for the legacy layout, ``1`` for the 2018+
            layout (see the module docstring's selector-translation notes).
        external_roster: ``(other_players, roster_players)`` -- either just
            names, or a full roster, to validate/fuzzy-correct box names
            against (see :func:`inject_validated_players`). Also seeds
            :attr:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent.players_out`
            on the interim lineup (``roster_players``, each's ``code``
            replaced by its jersey ``number``).
        neutral_game_dates: Date strings (the first whitespace-separated
            token of the raw date-cell text) known to be neutral-site games
            -- overrides the default home/away inference.

    Returns:
        A :class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` whose
        ``players`` is the validated box-score lineup (natural HTML order --
        see the module docstring's "not sorted" note), or a
        ``list[ParseError]`` if any parsing step failed.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
            from sportsdataverse.mbb.mbb_ncaa_models import TeamId

            with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
                html = f.read()
            result = get_box_lineup("test_p1.html", html, TeamId("TeamA"), format_version=0)
    """
    builders = _BUILDERS[format_version]

    def _completer(err: ParseError) -> list[ParseError]:
        base_id = f"[{filename}]" if filename else ""
        return [ParseError(location=_LOCATION_PARSE_BOXSCORE, id=base_id + err.id, messages=err.messages)]

    try:
        doc = parse_html(in_html)
    except Exception as exc:  # pragma: no cover - bs4/lxml is lenient; mirrors Scala's Try(request)
        return [
            ParseError(
                location=_LOCATION_PARSE_BOXSCORE,
                id=f"[{filename}]" if filename else "",
                messages=[f"Exception=[{exc}]"],
            )
        ]

    period = parse_period_from_filename(filename)

    maybe_date_str = builders.date_finder(doc)
    date_result = parse_date(maybe_date_str)
    if isinstance(date_result, ParseError):
        return _completer(date_result)
    date = date_result

    year = Year(date.year if date.month >= 6 else date.year - 1)

    team_info = parse_team_name(builders.team_finder(doc), team_id, year, home_team, away_team)
    if isinstance(team_info, ParseError):
        return _completer(team_info)
    team, opponent, target_team_first = team_info

    date_token = maybe_date_str.split(" ")[0] if maybe_date_str is not None else None
    if date_token is not None and date_token in neutral_game_dates:
        location_type = LocationType.NEUTRAL
    elif target_team_first:
        location_type = LocationType.AWAY
    else:
        location_type = LocationType.HOME

    final_score = parse_final_score(builders.score_finder(doc), target_team_first)
    if isinstance(final_score, ParseError):
        return _completer(final_score)

    players_result = parse_players_from_boxscore(builders.boxscore_finder(doc, target_team_first))
    if isinstance(players_result, ParseError):
        return _completer(players_result)
    # Note: in v1 this is NOT ordered by appearance, so can't be used to
    # infer the starting lineup (BoxscoreParser.scala:177).
    ordered_lineup_from_box = (
        [name_in_v0_box_format(p) for p in players_result] if format_version == 1 else players_result
    )

    other_players, roster_players = external_roster
    temp_box_score = LineupEvent(  # (need this to build a player context)
        date=date,
        location_type=location_type,
        start_min=start_time_from_period(period, is_women_game=False),  # (doesn't matter which)
        end_min=start_time_from_period(period, is_women_game=False),
        duration_mins=0.0,
        score_info=ScoreInfo(Score(0, 0), final_score, 0, 0),
        team=TeamSeasonId(TeamId(team), year),
        opponent=TeamSeasonId(TeamId(opponent), year),
        lineup_id=LineupId.unknown,
        players=[],
        players_in=[],
        players_out=[replace(r.player_code_id, code=r.number) for r in roster_players],
        raw_game_events=[],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )

    ordered_lineup = inject_validated_players(ordered_lineup_from_box, temp_box_score, external_roster)

    final_validated_lineup = validate_box_score(TeamId(team), ordered_lineup)
    if isinstance(final_validated_lineup, ParseError):
        return _completer(final_validated_lineup)

    return replace(temp_box_score, players=final_validated_lineup)
