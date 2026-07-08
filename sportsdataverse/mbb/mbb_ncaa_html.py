"""Shared JSoup->bs4 selector/text helpers for the NCAA HTML-parser layer.

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) HTML-parsing surface -- the first
of six Phase-5e modules. **Task 5e.1 builds the shared selector/text helpers**
every later 5e parser (RosterParser, BoxscoreParser, PlayByPlayParser,
TeamIdParser/TeamScheduleParser, ShotEventParser) reuses. The Scala parsers
are written against `scala-scraper
<https://github.com/ruippeixotog/scala-scraper>`_'s JSoup-backed DSL
(``net.ruippeixotog.scalascraper``), whose CSS-selector dialect is JSoup's
own (`<https://jsoup.org/cookbook/extracting-data/selector-syntax>`_) -- a
strict superset of the plain CSS soupsieve (bs4's selector engine) supports.
This module is the one place that translates the JSoup-only selector/text
idioms into bs4 + plain Python; every other 5e parser module composes these
helpers rather than re-deriving the translation.

**Translation table (JSoup -> this module):**

* ``doc >?> element("sel")`` / ``elementList("sel")`` (scala-scraper's
  ``Option``/``List``-returning DSL extractors over a plain CSS selector) ->
  ``soup.select_one("sel")`` / ``soup.select("sel")`` directly. Soupsieve
  (bs4's selector engine, bs4 >= 4.9) already supports every *plain* CSS
  selector JSoup does, including ``:has()`` and ``:contains()`` -- these need
  no helper.
* ``el.text`` -> :func:`jsoup_text`. JSoup's ``Element.text()`` collapses all
  whitespace (including newlines) between text nodes into single spaces and
  strips the ends; bs4's ``.get_text()`` does not collapse internal
  whitespace, so every text extraction needs this helper or captured HTML's
  literal ``\\n``/tab formatting leaks into parsed values.
* ``el.attr("x")`` -> ``el.get("x")``, never bare ``el["x"]`` (JSoup's
  ``attr()`` returns ``""`` for a missing attribute and never raises; bs4's
  ``Tag.__getitem__`` raises ``KeyError``). No dedicated helper -- just the
  convention of always using ``.get()``.
* ``el >?> element("td:eq(N)")`` -> :func:`td_at`. Soupsieve does not
  implement JSoup's ``:eq()`` positional pseudo-class (`
  <https://facelessuser.github.io/soupsieve/selectors/>`_ lists soupsieve's
  supported subset; ``:eq()`` / ``:lt()`` / ``:gt()`` are absent) -- every
  ``td:eq(N)`` selector translates to a plain 0-indexed list lookup into
  ``row.find_all("td")``.
* ``el :matches(regex)`` (any-descendant-text regex filter, JSoup's
  ``Elements.select(":matches(...)")``) -> :func:`select_matching`. No
  soupsieve equivalent; implemented as a plain CSS ``select()`` (for the
  structural part of the selector) followed by an ``re.search`` filter over
  :func:`jsoup_text` (which walks the element's own text plus every
  descendant's, matching JSoup's ``:matches()`` semantics).
* ``el :matchesOwn(regex)`` (own-text-only regex filter, distinct from
  ``:matches()`` -- JSoup's ``ownText()`` excludes descendant elements'
  text) -> :func:`select_matching_own`, filtering on the element's own
  ``NavigableString`` children only (not :func:`jsoup_text`, which would
  wrongly include descendant text).
* ``[attr~=regex]`` (JSoup attribute-regex matcher) -> :func:`attr_regex_filter`,
  a plain ``re.search`` filter over each candidate tag's named attribute.
* ``el :contains(text)`` (JSoup substring-containment filter) -> :func:`select_contains`.
  **Critical divergence, found during Task 5e.2's review of the BoxscoreParser
  port: JSoup's ``:contains()`` is documented CASE-INSENSITIVE substring
  containment, but soupsieve's non-deprecated spelling
  ``:-soup-contains()`` is CASE-SENSITIVE** (soupsieve has no
  case-insensitive ``:contains()`` variant at all -- verified against the
  soupsieve selector-reference docs). A ``:contains(...)`` selector ported
  as ``:-soup-contains(...)`` therefore silently stops matching the moment
  the captured HTML's casing differs from the selector's literal text.
  :func:`select_contains` reproduces JSoup's actual semantics: a plain
  (non-regex) case-folded substring test over :func:`jsoup_text`. Every
  ``:contains(...)`` selector ported anywhere in this HTML-parser layer
  should use this helper, never ``:-soup-contains()`` directly.
* ``el :matchesOwn(regex)`` applied to a candidate list already narrowed by
  a non-CSS filter (e.g. an :func:`attr_regex_filter` result) ->
  :func:`filter_matching_own`. :func:`select_matching_own` takes a
  ``(root, selector)`` pair and calls ``root.select(selector)`` itself,
  which cannot express a JSoup compound selector that mixes an
  ``[attr~=regex]`` term with a ``:matchesOwn(regex)`` term in the same
  chain (soupsieve can filter on structure but not on attribute *regexes*)
  -- :func:`filter_matching_own` is the same own-text filter, applied to an
  already-computed candidate list instead of a fresh ``root.select()`` call,
  so the two filters can be composed in sequence.

Apache-2.0 third-party port — see the ``NOTICE`` file at the repository root for the upstream copyright and full attribution.

**Landmine index (reachable scalar division).** None. Every helper below is
string splitting/joining, list indexing (guarded), or ``re.search`` --
no division site exists to enumerate.

Example::

    from sportsdataverse.mbb.mbb_ncaa_html import jsoup_text, parse_html, td_at

    soup = parse_html("<table><tr><td>1</td><td>Name</td></tr></table>")
    row = soup.select_one("tr")
    jsoup_text(td_at(row, 1))  # "Name"
    td_at(row, 5)  # None (out of range, guarded)

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- R women's basketball companion package
"""

from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup
from bs4.element import Tag

__all__ = [
    "parse_html",
    "jsoup_text",
    "td_at",
    "select_matching",
    "select_matching_own",
    "attr_regex_filter",
    "select_contains",
    "filter_matching_own",
]


def parse_html(html: str) -> BeautifulSoup:
    """Parse an HTML document/fragment (``JsoupBrowser().parseString(in)``).

    Args:
        html: The raw HTML string.

    Returns:
        A :class:`bs4.BeautifulSoup` document, parsed with the ``lxml``
        parser (already a project dependency; faster and more lenient than
        the stdlib ``html.parser``, matching JSoup's own lenient-HTML5
        parsing).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_html import parse_html
            soup = parse_html("<table><tr><td>1</td></tr></table>")
    """
    return BeautifulSoup(html, "lxml")


def jsoup_text(el: Optional[Tag]) -> str:
    """JSoup ``Element.text()``: all descendant text, whitespace-collapsed.

    JSoup's ``.text()`` joins every text node under ``el`` (including
    descendants) and collapses runs of whitespace (spaces, tabs, newlines)
    into single spaces, trimming the ends. bs4's ``.get_text()`` does the
    joining but not the collapsing, so captured HTML's indentation/newlines
    would otherwise leak into every extracted value.

    Args:
        el: The element to extract text from, or ``None``.

    Returns:
        The whitespace-collapsed text, or ``""`` if ``el`` is ``None``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_html import jsoup_text, parse_html
            soup = parse_html("<td>\\n  Akin,\\tDaniel  </td>")
            jsoup_text(soup.find("td"))  # "Akin, Daniel"
    """
    if el is None:
        return ""
    return " ".join(el.get_text().split())


def td_at(row: Tag, n: int) -> Optional[Tag]:
    """JSoup ``row >?> element("td:eq(n)")``: the ``n``-th ``<td>`` child.

    Soupsieve has no ``:eq()`` positional pseudo-class (unlike JSoup), so
    this is a plain 0-indexed lookup into ``row.find_all("td")``, guarded
    against an out-of-range index (JSoup's ``>?>`` returns ``None`` rather
    than raising when the selector matches nothing).

    Args:
        row: The row (or other container) element to search.
        n: The 0-indexed ``<td>`` position.

    Returns:
        The ``n``-th ``<td>`` descendant, or ``None`` if ``row`` has fewer
        than ``n + 1`` of them.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_html import parse_html, td_at
            soup = parse_html("<tr><td>A</td><td>B</td></tr>")
            row = soup.find("tr")
            td_at(row, 1).get_text()  # "B"
            td_at(row, 5)  # None
    """
    cells = row.find_all("td")
    try:
        return cells[n]
    except IndexError:
        return None


def select_matching(root: Tag, selector: str, regex: str) -> list[Tag]:
    """JSoup ``root.select(sel + ":matches(regex)")``: candidates whose full
    text (own + every descendant's) matches ``regex``.

    Soupsieve has no ``:matches()`` pseudo-class equivalent, so this runs the
    plain structural ``selector`` first, then filters by :func:`re.search`
    over each candidate's :func:`jsoup_text` (own text plus descendants',
    matching JSoup's ``:matches()`` semantics -- as opposed to
    :func:`select_matching_own`'s own-text-only ``:matchesOwn()``).

    Args:
        root: The element to search within.
        selector: A plain (soupsieve-legal) CSS selector.
        regex: The pattern each candidate's collapsed text must
            :func:`re.search`-match.

    Returns:
        Every ``selector`` match whose :func:`jsoup_text` contains a
        ``regex`` match, in document order.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_matching
            soup = parse_html("<div><p>Home Team</p><p>Away Team</p></div>")
            select_matching(soup, "p", r"^Home")  # [<p>Home Team</p>]
    """
    return [el for el in root.select(selector) if re.search(regex, jsoup_text(el))]


def select_matching_own(root: Tag, selector: str, regex: str) -> list[Tag]:
    """JSoup ``root.select(sel + ":matchesOwn(regex)")``: candidates whose
    OWN text only (excluding descendant elements' text) matches ``regex``.

    JSoup's ``Element.ownText()`` walks only the element's direct
    ``TextNode`` children, not text nested inside child elements -- the
    same distinction bs4 draws between a tag's direct
    :class:`bs4.NavigableString` children and its full ``.get_text()``.

    Args:
        root: The element to search within.
        selector: A plain (soupsieve-legal) CSS selector.
        regex: The pattern each candidate's own (whitespace-collapsed) text
            must :func:`re.search`-match.

    Returns:
        Every ``selector`` match whose own text contains a ``regex`` match,
        in document order.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_matching_own
            soup = parse_html('<div class="card-header">Coach <b>Info</b></div>')
            select_matching_own(soup, "div.card-header", r"^Coach")
            # [<div class="card-header">Coach <b>Info</b></div>]
    """
    own_text_re = re.compile(regex)
    matches = []
    for el in root.select(selector):
        own_text = " ".join(child.strip() for child in el.find_all(string=True, recursive=False) if child.strip())
        if own_text_re.search(own_text):
            matches.append(el)
    return matches


def attr_regex_filter(tags: list[Tag], attr: str, regex: str) -> list[Tag]:
    """JSoup ``[attr~=regex]``: candidates whose ``attr`` value matches
    ``regex``.

    Args:
        tags: Candidate tags to filter (typically the result of an earlier
            ``.select()``/``.find_all()`` call).
        attr: The attribute name to test.
        regex: The pattern the attribute value must :func:`re.search`-match.

    Returns:
        The subset of ``tags`` that have ``attr`` set and whose value
        matches ``regex``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_html import attr_regex_filter, parse_html
            soup = parse_html('<div width="45%"></div><div width="10%"></div>')
            attr_regex_filter(soup.find_all("div"), "width", r"^\\[?4?5")
    """
    pattern = re.compile(regex)
    matches = []
    for tag in tags:
        value = tag.get(attr)
        if value is not None and pattern.search(str(value)):
            matches.append(tag)
    return matches


def select_contains(root: Tag, selector: str, text: str) -> list[Tag]:
    """JSoup ``root.select(sel + ":contains(text)")``: candidates whose full
    text (own + every descendant's) case-insensitively CONTAINS ``text`` as
    a plain substring -- **not** a regex (Task 5e.2 addition; see the module
    docstring's "Critical divergence" note).

    JSoup's ``:contains()`` is documented case-insensitive substring
    containment; soupsieve's ``:-soup-contains()`` (the non-deprecated
    spelling of its ``:contains()``) is case-SENSITIVE, with no
    case-insensitive variant of its own. Reproducing JSoup's actual
    semantics therefore needs this helper rather than ``:-soup-contains()``.

    Args:
        root: The element to search within.
        selector: A plain (soupsieve-legal) CSS selector for the
            structural part of the match (everything before ``:contains``).
        text: The plain substring each candidate's collapsed text must
            case-insensitively contain.

    Returns:
        Every ``selector`` match whose :func:`jsoup_text` case-insensitively
        contains ``text``, in document order.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_html import parse_html, select_contains
            soup = parse_html("<td>game date:</td><td>Location:</td>")
            select_contains(soup, "td", "Game Date:")  # [<td>game date:</td>]
    """
    needle = text.lower()
    return [el for el in root.select(selector) if needle in jsoup_text(el).lower()]


def filter_matching_own(tags: list[Tag], regex: str) -> list[Tag]:
    """JSoup ``:matchesOwn(regex)`` applied to an already-computed candidate
    list, rather than a fresh ``root.select(selector)`` call (Task 5e.2
    addition; see the module docstring's note on composing this with
    :func:`attr_regex_filter`).

    Same own-text-only semantics as :func:`select_matching_own` -- JSoup's
    ``Element.ownText()`` walks only the element's direct ``TextNode``
    children, not text nested inside child elements.

    Args:
        tags: Candidate tags to filter (typically the result of an earlier
            ``.select()``/:func:`attr_regex_filter` call).
        regex: The pattern each candidate's own (whitespace-collapsed) text
            must :func:`re.search`-match.

    Returns:
        The subset of ``tags`` whose own text contains a ``regex`` match,
        in the input list's order.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_html import attr_regex_filter, filter_matching_own, parse_html
            soup = parse_html('<td style="font-size:36px">92</td><td style="color:red">x</td>')
            candidates = attr_regex_filter(soup.find_all("td"), "style", r"font-size:36px")
            filter_matching_own(candidates, r"[0-9]+")  # [<td style="font-size:36px">92</td>]
    """
    own_text_re = re.compile(regex)
    matches = []
    for el in tags:
        own_text = " ".join(child.strip() for child in el.find_all(string=True, recursive=False) if child.strip())
        if own_text_re.search(own_text):
            matches.append(el)
    return matches


def current_ncaa_team_alts(doc: Tag) -> list[str]:
    """The two team names from a *current* (2026) stats.ncaa.org game page.

    Both team logos render as ``a.skipMask > img[alt]`` on the modern
    play-by-play / box-score / individual-stats pages -- the ``alt`` is the
    short team name (e.g. ``"Illinois"``), in ``[home_or_first, second]``
    document order, deduped (the individual-stats page repeats each team).

    This is the current-markup counterpart to the cbb-explorer ``:2018+``
    ``team_finder`` selectors (``div.card-header img[alt]`` for the box,
    ``table[align=center] > tbody a > img[alt]`` for the pbp), which no
    longer match: NCAA's markup drifted since cbb-explorer's ~2020 capture.
    The v1 ``team_finder``\\ s try their ported selector first, then fall
    back here, so both the vendored-era and current pages resolve.
    """
    out: list[str] = []
    for el in doc.select("a.skipMask img[alt]"):
        alt = str(el.get("alt", "")).strip()
        if alt and alt not in out:
            out.append(alt)
    return out
