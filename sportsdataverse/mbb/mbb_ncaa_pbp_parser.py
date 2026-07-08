"""NCAA play-by-play HTML parser + full-pipeline orchestration (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) ``PlayByPlayParser.scala`` --
the third of six Phase-5e modules, and the FLAGSHIP task of the phase: this
is the orchestrator that chains the ENTIRE ported Phase 5a-5d surface
(:mod:`~sportsdataverse.mbb.mbb_ncaa_stints`,
:mod:`~sportsdataverse.mbb.mbb_ncaa_lineup_enrich`,
:mod:`~sportsdataverse.mbb.mbb_ncaa_possessions`,
:mod:`~sportsdataverse.mbb.mbb_ncaa_stint_validation`) into one call,
:func:`create_lineup_data`, that turns a saved NCAA play-by-play HTML page
into validated lineup stints.

**v0/v1 builder tables.** As with the roster and boxscore parsers (Tasks
5e.1/5e.2), two eras of the NCAA stats site ship different HTML shapes for
the play-by-play table. Both eras are a pair of finder-function bundles
(:data:`v0_builders` / :data:`v1_builders`, indexed by ``format_version``
exactly like the Scala's ``builders_from_version = Array(v0_builders,
v1_builders)`` -- exported at module level, unlike the boxscore parser's
private ``_BUILDERS`` tuple, because ``PlayByPlayParserTests.scala``'s
inline low-level tests reference ``v0_builders`` directly by name).

**Selector translations (JSoup -> bs4, this task's findings):**

* ``div#contentarea table.mytable[width~=[45]0%] td a[href]`` (v0
  ``team_finder``) -- same attribute-*regex* + nested-select pattern as the
  boxscore parser's ``_v0_team_finder``:
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.attr_regex_filter` over
  ``div#contentarea table.mytable`` for the ``width`` regex, then ``td
  a[href]`` within each matched table.
* ``table.mytable tr:has(td.smtext)`` (v0 ``event_finder``) -- ``:has()`` is
  plain CSS, soupsieve supports it directly: ``doc.select(...)``.
* ``td.smtext:eq(N)`` (v0 ``event_time_finder``/``event_score_finder``/
  ``event_team_finder``/``event_opponent_finder``) -- soupsieve has no
  ``:eq()``, so this is ``event.select("td.smtext")[n]``, guarded against an
  out-of-range index (:func:`_smtext_at`, a local variant of
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.td_at` scoped to the
  ``smtext``-classed columns rather than every ``<td>``).
* ``td.boldtext:not(.smtext)`` (v0 ``game_event_finder``) -- ``:not()`` is
  plain CSS, direct ``event.select_one(...)``.
* ``table[align=center] > tbody a > img[alt]`` (v1 ``team_finder``) --
  fully plain CSS (attribute-equality + child combinators), direct
  ``doc.select(...)``; reads each element's ``alt`` attribute
  (:meth:`~bs4.element.Tag.get`), not
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.jsoup_text` (mirrors the
  boxscore parser's v1 ``team_finder``).
* ``div.card-body > table.table > tbody tr:matches([0-9]+:[0-9]+(:[0-9]+)?)``
  (v1 ``event_finder``) -- ``:matches()`` has no soupsieve equivalent;
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.select_matching` handles it
  directly (a plain structural selector plus a full-text regex filter).
* ``td:eq(N)`` (v1 ``event_time_finder`` etc.) --
  :func:`~sportsdataverse.mbb.mbb_ncaa_html.td_at` (the *unscoped* helper,
  since v1 has no ``smtext`` class distinction) handles this directly.
* ``td.boldtext`` (v1 ``game_event_finder``) -- plain CSS,
  ``event.select_one(...)``.

**The ``index(is, want)`` team/opponent-column trick.** Both eras share the
same 1-or-3 column-selection logic (``PlayByPlayParser.scala:78-79,127-128``):
column 1 if ``is_team == target_team_first`` else column 3. Ported as
:func:`_column_index`, shared by both builder tables (the Scala duplicates
this private method verbatim inside each ``object``; deduplicating it here
is a pure DRY win with zero behavior change -- the Scala's two copies are
byte-identical).

**``parse_game_event``'s two-tier error handling -- the ``sequence`` vs
``parMapN`` distinction (this task's main design question, resolved by
reading ``cats.implicits._``'s actual semantics rather than guessing):**

* Inside ONE ``parse_game_event`` call, the score and time sub-parses are
  combined via Scala's ``(score_or_error, time_or_error).parMapN((_, _))``
  -- ``parMapN`` invokes cats' ``Parallel`` typeclass, which for
  ``Either[List[ParseError], *]`` (``List`` has a lawful ``Semigroup`` via
  ``++``) is the ACCUMULATING instance: if BOTH the score and time parses
  fail, BOTH errors are concatenated into the returned list, not just the
  first. Ported as :func:`parse_game_event` collecting into a local
  ``errors: list[ParseError]`` from both sub-results before returning.
* Across MANY events, ``parse_game_events``'s
  ``html_events.map(parse_game_event(...)).sequence`` calls the PLAIN
  ``.sequence`` (not ``.parSequence``), which resolves to cats'
  ``Traverse[List]`` sequencing over the ordinary (monadic) ``Either``
  ``Applicative`` -- this is FAIL-FAST: the first ``Left`` short-circuits
  and every subsequent event is never even parsed. Ported as
  :func:`parse_game_events`'s loop returning immediately on the first
  event that yields errors, never accumulating across events.

  These are genuinely different semantics at two different scopes (within
  one event: accumulate; across events: fail-fast), both correctly ported.

**Python has no ``Either``, so the row-level parse result -- ``list[
PlayByPlayEvent] | list[ParseError]`` -- is disambiguated by
``bool(result) and isinstance(result[0], ParseError)``.** This works because
the SUCCESS branch's list is either empty (a game event, deliberately
ignored -- ``Right(Nil)``) or holds exactly one ``PlayByPlayEvent`` (never a
``ParseError``), while every ERROR branch always returns at least one
``ParseError`` (``build_sub_error``/``enrich_sub_error`` never produce an
empty list) -- so an empty list is unambiguously success, and a non-empty
list's first element unambiguously tells the two shapes apart.
:func:`typing.cast` narrows the checked branches for mypy (the project's
existing precedent in ``mbb_rapm.py``), since ``isinstance(result[0], ...)``
narrows ``result[0]``'s type, not ``result``'s.

**``build_sub_error`` vs ``enrich_sub_error`` -- two different error shapes
in the same function, ported literally.** Re-reading
``PlayByPlayParser.scala:391-457`` closely: the score/time sub-errors are
wrapped through ``ParseUtils.enrich_sub_error(`ncaa.parse_playbyplay`,
`parent_fills_in`)`` (``parent_fills_in = ""`` per
``ExtractorUtils.scala:20``) -- giving them ``location="ncaa.parse_playbyplay"``,
``id`` unchanged (an empty ``base_id`` contributes nothing). But the
"both team and opponent" / "neither team nor opponent" terminal errors use
a DIFFERENT, un-enriched shape: ``ParseUtils.build_sub_error(
`ncaa.parse_playbyplay`)(msg)`` -- here ``` `ncaa.parse_playbyplay` ``` is
passed as a *subid* (bracket-wrapped into ``id``), with ``location=""``
left untouched. The "no play by play events found" error in
``parse_game_events`` uses this SAME un-enriched ``build_sub_error`` shape,
not ``enrich_sub_error``. All three shapes are ported exactly as written,
not unified into a single helper the Scala itself doesn't use.

Apache-2.0 third-party port — see the ``NOTICE`` file at the repository root for the upstream copyright and full attribution.

**Landmine index (reachable scalar division).** None. Every computation in
this module is string splitting/regex, guarded list/dict lookups, or plain
arithmetic on already-validated floats (the ``enrich_and_reverse_game_events``
minute-ascension math, which only adds/subtracts -- no division by a
runtime-derived value exists).

Example::

    from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
    from sportsdataverse.mbb.mbb_ncaa_models import TeamId
    from sportsdataverse.mbb.mbb_ncaa_pbp_parser import create_lineup_data

    with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
        box_html = f.read()
    box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=0)

    with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
        pbp_html = f.read()
    result = create_lineup_data("test.html", pbp_html, box_lineup, format_version=0)
    if isinstance(result, list):
        raise RuntimeError(result)  # list[ParseError]
    good_lineups, bad_lineups = result

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- R women's basketball companion package
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from typing import Callable, Optional, Union, cast

from bs4 import BeautifulSoup
from bs4.element import Tag

from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError, build_sub_error, enrich_sub_error
from sportsdataverse.mbb.mbb_ncaa_events import parse_game_time, parse_team_sub_in_pair, parse_team_sub_out_pair
from sportsdataverse.mbb.mbb_ncaa_html import (
    attr_regex_filter,
    current_ncaa_team_alts,
    jsoup_text,
    parse_html,
    select_matching,
    td_at,
)
from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import enrich_lineup, fix_possible_score_swap_bug
from sportsdataverse.mbb.mbb_ncaa_models import LineupEvent, Score, TeamId, Year
from sportsdataverse.mbb.mbb_ncaa_possessions import calculate_possessions
from sportsdataverse.mbb.mbb_ncaa_stint_validation import analyze_and_fix_clumps, clump_bad_lineups, validate_lineup
from sportsdataverse.mbb.mbb_ncaa_stints import (
    GameBreakEvent,
    GameEndEvent,
    OtherOpponentEvent,
    OtherTeamEvent,
    PlayByPlayEvent,
    SubInEvent,
    SubOutEvent,
    build_partial_lineup_list,
    duration_from_period,
    parse_team_name,
)

__all__ = [
    "PbpBuilders",
    "v0_builders",
    "v1_builders",
    "parse_game_score",
    "parse_desc_game_time",
    "parse_game_event",
    "enrich_and_reverse_game_events",
    "parse_game_events",
    "get_sorted_pbp_events",
    "create_lineup_data",
]

#: Error-reporter location tag (``` `ncaa.parse_playbyplay` ```,
#: ``PlayByPlayParser.scala:34``).
_LOCATION_PARSE_PBP = "ncaa.parse_playbyplay"

#: ``ParseUtils.enrich_sub_error``'s ``base_id`` at the ``parse_game_event``
#: call site (``` `parent_fills_in` = "" ```, ``ExtractorUtils.scala:20``).
_PARENT_FILLS_IN = ""

_SCORE_REGEX = re.compile(r"([0-9]+)[-]([0-9]+)")


# ---------------------------------------------------------------------------
# v0/v1 builder tables (``base_builders``/``v0_builders``/``v1_builders``,
# ``PlayByPlayParser.scala:37-149``)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PbpBuilders:
    """One version-era's HTML finder functions (``PlayByPlayParser
    .base_builders``, ``PlayByPlayParser.scala:37-51``)."""

    team_finder: Callable[[BeautifulSoup], list[str]]
    event_finder: Callable[[BeautifulSoup], list[Tag]]
    event_time_finder: Callable[[Tag], Optional[str]]
    event_score_finder: Callable[[Tag], Optional[str]]
    game_event_finder: Callable[[Tag], Optional[str]]
    event_team_finder: Callable[[Tag, bool], Optional[str]]
    event_opponent_finder: Callable[[Tag, bool], Optional[str]]


def _column_index(is_team: bool, want_team_first: bool) -> int:
    """Shared team/opponent column-selection trick (``index(is, want)``,
    ``PlayByPlayParser.scala:78-79,127-128`` -- byte-identical in both
    ``v0_builders`` and ``v1_builders``, deduplicated here).

    Args:
        is_team: Whether the caller wants the "team" (``True``) or
            "opponent" (``False``) column.
        want_team_first: Whether the target team's columns come first in
            the row.

    Returns:
        ``1`` if ``is_team == want_team_first``, else ``3``.
    """
    return 1 if is_team == want_team_first else 3


def _v0_team_finder(doc: BeautifulSoup) -> list[str]:
    """``div#contentarea table.mytable[width~=[45]0%] td a[href]``
    (``:55-59``)."""
    tables = attr_regex_filter(doc.select("div#contentarea table.mytable"), "width", r"[45]0%")
    els: list[Tag] = []
    for table in tables:
        els.extend(table.select("td a[href]"))
    return [jsoup_text(el) for el in els]


def _v0_event_finder(doc: BeautifulSoup) -> list[Tag]:
    """``table.mytable tr:has(td.smtext)`` (``:62-65``)."""
    return doc.select("table.mytable tr:has(td.smtext)")


def _smtext_at(event: Tag, n: int) -> Optional[Tag]:
    """``td.smtext:eq(n)`` -- soupsieve has no ``:eq()``, so this is a
    guarded 0-indexed lookup into ``event.select("td.smtext")`` (a
    ``v0_builders``-specific variant of
    :func:`~sportsdataverse.mbb.mbb_ncaa_html.td_at`, which indexes
    unscoped ``<td>``\\ s)."""
    cells = event.select("td.smtext")
    try:
        return cells[n]
    except IndexError:
        return None


def _v0_event_time_finder(event: Tag) -> Optional[str]:
    """``td.smtext:eq(0)`` (``:67-68``)."""
    text = jsoup_text(_smtext_at(event, 0))
    return text if text else None


def _v0_event_score_finder(event: Tag) -> Optional[str]:
    """``td.smtext:eq(2)`` (``:70-71``)."""
    text = jsoup_text(_smtext_at(event, 2))
    return text if text else None


def _v0_game_event_finder(event: Tag) -> Optional[str]:
    """``td.boldtext:not(.smtext)`` (``:73-76``) -- ``:not()`` is plain CSS."""
    text = jsoup_text(event.select_one("td.boldtext:not(.smtext)"))
    return text if text else None


def _v0_event_team_finder(event: Tag, target_team_first: bool) -> Optional[str]:
    """``td.smtext:eq(index(true, target_team_first))`` (``:81-89``)."""
    text = jsoup_text(_smtext_at(event, _column_index(True, target_team_first)))
    return text if text else None


def _v0_event_opponent_finder(event: Tag, target_team_first: bool) -> Optional[str]:
    """``td.smtext:eq(index(false, target_team_first))`` (``:91-99``)."""
    text = jsoup_text(_smtext_at(event, _column_index(False, target_team_first)))
    return text if text else None


#: The legacy (pre-2018-ish) NCAA play-by-play page layout
#: (``PlayByPlayParser.v0_builders``, ``:52-100``).
v0_builders = PbpBuilders(
    team_finder=_v0_team_finder,
    event_finder=_v0_event_finder,
    event_time_finder=_v0_event_time_finder,
    event_score_finder=_v0_event_score_finder,
    game_event_finder=_v0_game_event_finder,
    event_team_finder=_v0_event_team_finder,
    event_opponent_finder=_v0_event_opponent_finder,
)


def _v1_team_finder(doc: BeautifulSoup) -> list[str]:
    """``table[align=center] > tbody a > img[alt]`` (``:103-106``) -- reads
    the ``alt`` attribute, not
    :func:`~sportsdataverse.mbb.mbb_ncaa_html.jsoup_text`.

    Falls back to :func:`~sportsdataverse.mbb.mbb_ncaa_html.current_ncaa_team_alts`
    for current (2026) markup (see that helper's drift note); the modern
    play-by-play page's ``event_finder`` selector still matches, only this
    team header drifted."""
    ported = [str(el.get("alt", "")) for el in doc.select('table[align="center"] > tbody a > img[alt]')]
    return ported or current_ncaa_team_alts(doc)


def _v1_event_finder(doc: BeautifulSoup) -> list[Tag]:
    """``div.card-body > table.table > tbody tr:matches([0-9]+:[0-9]+(:[0-9]+)?)``
    (``:108-114``)."""
    return select_matching(doc, "div.card-body > table.table > tbody tr", r"[0-9]+:[0-9]+(:[0-9]+)?")


def _v1_event_time_finder(event: Tag) -> Optional[str]:
    """``td:eq(0)`` (``:116-117``)."""
    text = jsoup_text(td_at(event, 0))
    return text if text else None


def _v1_event_score_finder(event: Tag) -> Optional[str]:
    """``td:eq(2)`` (``:119-120``)."""
    text = jsoup_text(td_at(event, 2))
    return text if text else None


def _v1_game_event_finder(event: Tag) -> Optional[str]:
    """``td.boldtext`` (``:122-125``)."""
    text = jsoup_text(event.select_one("td.boldtext"))
    return text if text else None


def _v1_event_team_finder(event: Tag, target_team_first: bool) -> Optional[str]:
    """``td:eq(index(true, target_team_first))`` (``:130-138``)."""
    text = jsoup_text(td_at(event, _column_index(True, target_team_first)))
    return text if text else None


def _v1_event_opponent_finder(event: Tag, target_team_first: bool) -> Optional[str]:
    """``td:eq(index(false, target_team_first))`` (``:140-148``)."""
    text = jsoup_text(td_at(event, _column_index(False, target_team_first)))
    return text if text else None


#: The 2018+ NCAA play-by-play page layout (``PlayByPlayParser.v1_builders``,
#: ``:101-149``).
v1_builders = PbpBuilders(
    team_finder=_v1_team_finder,
    event_finder=_v1_event_finder,
    event_time_finder=_v1_event_time_finder,
    event_score_finder=_v1_event_score_finder,
    game_event_finder=_v1_game_event_finder,
    event_team_finder=_v1_event_team_finder,
    event_opponent_finder=_v1_event_opponent_finder,
)

#: Indexed by ``format_version`` (``PlayByPlayParser.builders_from_version``, ``:150``).
_BUILDERS_FROM_VERSION = (v0_builders, v1_builders)


# ---------------------------------------------------------------------------
# Row-level parsing (``PlayByPlayParser.scala:372-510``)
# ---------------------------------------------------------------------------


def parse_game_score(el: Tag, builders: PbpBuilders) -> Union[tuple[str, Score], ParseError]:
    """Parses the ``NN-MM`` score string off one play-by-play row
    (``PlayByPlayParser.parse_game_score``, ``:462-484``).

    Args:
        el: The play-by-play row element.
        builders: The version-era's finder functions.

    Returns:
        ``(raw_score_string, Score(team, opponent))`` (the team named FIRST
        in the string, not yet flipped for ``target_team_first`` -- that
        flip happens in :func:`parse_game_event`), or a
        :class:`~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError` if no
        score cell was found or it didn't match ``"NN-MM"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_parser import parse_game_score, v0_builders
            from sportsdataverse.mbb.mbb_ncaa_html import parse_html

            doc = parse_html('<tr><td class="smtext">15:00</td>'
                              '<td class="smtext">e</td>'
                              '<td class="smtext" align="center">45-26</td>'
                              '<td class="smtext"></td></tr>')
            parse_game_score(doc.body, v0_builders)  # ("45-26", Score(45, 26))
    """
    raw = builders.event_score_finder(el)
    if raw is None:
        return build_sub_error("game_score", error=f"Could not find score in [{el}]")
    m = _SCORE_REGEX.fullmatch(raw)
    if m is not None:
        return (raw, Score(int(m.group(1)), int(m.group(2))))
    return build_sub_error("game_score", error=f"Could not find parse score [A-B] from [{raw}] in [{el}]")


def parse_desc_game_time(el: Tag, builders: PbpBuilders) -> Union[tuple[str, float], ParseError]:
    """Parses a DESCENDING ``NN:MM`` (or ``NN:MM:SS``) time string off one
    play-by-play row (``PlayByPlayParser.parse_desc_game_time``, ``:489-510``).
    The value still descends here; :func:`enrich_and_reverse_game_events`
    is the separate stateful pass that turns it ascending.

    Args:
        el: The play-by-play row element.
        builders: The version-era's finder functions.

    Returns:
        ``(raw_time_string, descending_minutes)``, or a
        :class:`~sportsdataverse.mbb.mbb_ncaa_data_quality.ParseError` if no
        time cell was found or it didn't match
        :func:`~sportsdataverse.mbb.mbb_ncaa_events.parse_game_time`'s
        pattern.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_parser import parse_desc_game_time, v0_builders
            from sportsdataverse.mbb.mbb_ncaa_html import parse_html

            doc = parse_html('<tr><td class="smtext">15:00</td>'
                              '<td class="smtext">e</td>'
                              '<td class="smtext" align="center">45-26</td>'
                              '<td class="smtext"></td></tr>')
            parse_desc_game_time(doc.body, v0_builders)  # ("15:00", 15.0)
    """
    raw = builders.event_time_finder(el)
    if raw is None:
        return build_sub_error("game_time", error=f"Could not find time in [{el}]")
    mins = parse_game_time(raw)
    if mins is not None:
        return (raw, mins)
    return build_sub_error("game_time", error=f"Could not find parse time [MM:SS] from [{raw}] in [{el}]")


def parse_game_event(
    el: Tag, target_team_first: bool, builders: PbpBuilders
) -> Union[list[PlayByPlayEvent], list[ParseError]]:
    """Turns one play-by-play row into 0 or 1 :data:`~sportsdataverse.mbb
    .mbb_ncaa_stints.PlayByPlayEvent`\\ s (``PlayByPlayParser.parse_game_event``,
    ``:375-459``).

    A row identified by ``game_event_finder`` (a non-score/time narrative
    row, e.g. a timeout banner) is deliberately ignored -- see the module
    docstring's disambiguation note for why an empty success list is always
    unambiguous. Otherwise the row's score and time are parsed (both errors
    ACCUMULATE if both sub-parses fail -- see the module docstring's
    ``parMapN`` note), the score is flipped so the target team is always
    first, and the (team, opponent) column-text pair is matched against, in
    order: :func:`~sportsdataverse.mbb.mbb_ncaa_events.parse_team_sub_in_pair`,
    :func:`~sportsdataverse.mbb.mbb_ncaa_events.parse_team_sub_out_pair`,
    team-only, opponent-only, both (error), neither (error).

    Args:
        el: The play-by-play row element.
        target_team_first: Whether the target team's columns come first in
            this row.
        builders: The version-era's finder functions.

    Returns:
        ``[]`` for an ignored narrative row, a single-element list holding
        the parsed event, or a non-empty ``list[ParseError]`` on failure.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_parser import parse_game_event, v0_builders
            from sportsdataverse.mbb.mbb_ncaa_html import parse_html

            doc = parse_html('<tr><td class="smtext">15:00</td>'
                              '<td class="smtext">S8RNAME,F8RSTNAME TEAMA Enters Game</td>'
                              '<td class="smtext" align="center">45-26</td>'
                              '<td class="smtext"></td></tr>')
            parse_game_event(doc.body, True, v0_builders)
            # [SubInEvent(15.0, Score(45, 26), "S8RNAME,F8RSTNAME TEAMA")]
    """
    if builders.game_event_finder(el) is not None:
        # TODO: for now just ignore these, later on can use timeouts to
        # split lineups maybe? (PlayByPlayParser.scala:389)
        return []

    score_result = parse_game_score(el, builders)
    time_result = parse_desc_game_time(el, builders)

    errors: list[ParseError] = []
    if isinstance(score_result, ParseError):
        errors.extend(enrich_sub_error(_LOCATION_PARSE_PBP, _PARENT_FILLS_IN, score_result))
    if isinstance(time_result, ParseError):
        errors.extend(enrich_sub_error(_LOCATION_PARSE_PBP, _PARENT_FILLS_IN, time_result))
    if errors:
        return errors

    score_str, raw_score = cast("tuple[str, Score]", score_result)
    time_str, time_mins = cast("tuple[str, float]", time_result)

    score = raw_score if target_team_first else Score(scored=raw_score.allowed, allowed=raw_score.scored)

    team = builders.event_team_finder(el, target_team_first)
    opponent = builders.event_opponent_finder(el, target_team_first)

    sub_in_player = parse_team_sub_in_pair(team, opponent)
    if sub_in_player is not None:
        sub_in_event: PlayByPlayEvent = SubInEvent(time_mins, score, sub_in_player)
        return [sub_in_event]

    sub_out_player = parse_team_sub_out_pair(team, opponent)
    if sub_out_player is not None:
        sub_out_event: PlayByPlayEvent = SubOutEvent(time_mins, score, sub_out_player)
        return [sub_out_event]

    if team is not None and opponent is None:
        team_event: PlayByPlayEvent = OtherTeamEvent(time_mins, score, f"{time_str},{score_str},{team}")
        return [team_event]
    if team is None and opponent is not None:
        opponent_event: PlayByPlayEvent = OtherOpponentEvent(time_mins, score, f"{time_str},{score_str},{opponent}")
        return [opponent_event]
    if team is not None and opponent is not None:
        return [
            build_sub_error(
                _LOCATION_PARSE_PBP,
                error=f"Not allowed both team and opponent events in the same entry [{el}]: [{team}] vs [{opponent}]",
            )
        ]
    return [
        build_sub_error(
            _LOCATION_PARSE_PBP,
            error=f"Must have either team or opponent event in one entry [{el}]: [({team}, {opponent})]",
        )
    ]


# ---------------------------------------------------------------------------
# enrich_and_reverse_game_events (``PlayByPlayParser.scala:297-370``)
# ---------------------------------------------------------------------------


def enrich_and_reverse_game_events(in_events: list[PlayByPlayEvent]) -> list[PlayByPlayEvent]:
    """Inserts game-break events and turns descending per-row times into
    ascending game-clock minutes, returning the whole list latest-to-earliest
    (``PlayByPlayParser.enrich_and_reverse_game_events``, ``:297-370``).

    Args:
        in_events: The raw parsed events, earliest to latest, with each
            ``.min`` still a per-period DESCENDING clock reading.

    Returns:
        ``in_events`` with :class:`~sportsdataverse.mbb.mbb_ncaa_stints
        .GameBreakEvent`\\ s inserted at every period boundary, every
        ``.min`` converted to an ASCENDING whole-game reading, and a
        trailing (once reversed, LEADING) :class:`~sportsdataverse.mbb
        .mbb_ncaa_stints.GameEndEvent` -- the whole list in LATEST-TO-EARLIEST
        order (the caller is expected to ``reversed(...)`` it back when
        chronological order is wanted, exactly like
        :func:`get_sorted_pbp_events` does).

    Note:
        Two special cases, both ported verbatim from the Scala fold:

        * **Period-boundary detection.** ``event.min > prev.min + 1.1`` (the
          ``+1.1`` safety margin absorbs mildly-out-of-order rows near a
          half's end) triggers a new
          :class:`~sportsdataverse.mbb.mbb_ncaa_stints.GameBreakEvent` and
          bumps the period counter.
        * **Spurious mid-half ``0:00`` corruption.** A rare NCAA data bug
          re-emits a ``0:00`` reading in the MIDDLE of the following
          period's block (not at a real boundary) -- detected via
          ``event.min == 0 and prev.min >= half_or_quarter_thresh`` (the
          threshold is one full period's duration short by half a minute)
          and repaired by clamping the corrupted event's minute to the
          previous event's, rather than treating it as a period boundary.

        ``is_women_game`` (quarters vs. halves) is inferred ONCE, from
        whether the FIRST event's ``.min`` is ``<= 10`` -- a women's first
        quarter is only 10 minutes long, so any real first-row reading
        above 10 can only be a men's half.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_models import Score
            from sportsdataverse.mbb.mbb_ncaa_pbp_parser import enrich_and_reverse_game_events
            from sportsdataverse.mbb.mbb_ncaa_stints import OtherTeamEvent

            events = [OtherTeamEvent(18.0, Score(1, 1), "tipoff")]
            reversed_enriched = enrich_and_reverse_game_events(events)
            reversed_enriched[0].__class__.__name__  # 'GameEndEvent'
    """
    is_women_game = in_events[0].min <= 10 if in_events else False

    def ascend_minutes(ev: PlayByPlayEvent, period: int) -> PlayByPlayEvent:
        total_duration = duration_from_period(period, is_women_game)
        return ev.with_min(total_duration - ev.min)

    period = 1
    last: Optional[PlayByPlayEvent] = None
    game_events: list[PlayByPlayEvent] = []

    for event in in_events:
        if last is None:
            last = event
            game_events = [ascend_minutes(event, period)]
        elif event.min > last.min + 1.1:
            # Game break! (+1.1 for safety -- sometimes events are mildly
            # out of order, e.g. end of 2019/20 Florida-Missouri 1st half.)
            game_break = GameBreakEvent(duration_from_period(period, is_women_game), event.score)
            new_period = period + 1
            game_events = [ascend_minutes(event, new_period), game_break] + game_events
            period = new_period
            last = event
        else:
            half_or_quarter_thresh = (
                duration_from_period(period, is_women_game) - duration_from_period(period - 1, is_women_game) - 0.5
            )
            if event.min == 0 and last.min >= half_or_quarter_thresh:
                # Rare corruption: the initial block of 2nd-half results has
                # a spurious 0:00 in the middle of it (e.g. 2020/21 N.C A&T
                # vs. Alabama St. / Jackson St. vs. South Carolina St.).
                adjusted_event = event.with_min(last.min)
                game_events = [ascend_minutes(adjusted_event, period)] + game_events
                last = adjusted_event
            else:
                game_events = [ascend_minutes(event, period)] + game_events
                last = event

    end_score = last.score if last is not None else Score(0, 0)
    return [GameEndEvent(duration_from_period(period, is_women_game), end_score)] + game_events


# ---------------------------------------------------------------------------
# parse_game_events / get_sorted_pbp_events (``PlayByPlayParser.scala:219-289``)
# ---------------------------------------------------------------------------


def parse_game_events(
    filename: str,
    in_html: str,
    target_team: TeamId,
    year: Year,
    builders: PbpBuilders,
    enrich: bool = True,
) -> Union[list[PlayByPlayEvent], list[ParseError]]:
    """Creates a list of raw play-by-play events from the HTML, fixes the
    dates, and injects game breaks (``PlayByPlayParser.parse_game_events``,
    ``:244-289``). The returned list is reversed (latest to earliest) when
    ``enrich`` is ``True``.

    Args:
        filename: The source file name, used only for error reporting.
        in_html: The raw play-by-play-page HTML.
        target_team: The team under analysis.
        year: The season, for the team-name matcher's alias table.
        builders: The version-era's finder functions (:data:`v0_builders` or
            :data:`v1_builders`).
        enrich: Whether to run :func:`enrich_and_reverse_game_events` (game
            breaks + ascending minutes + reversal) over the parsed events.

    Returns:
        The play-by-play events (reversed, latest-to-earliest, if
        ``enrich``), or a ``list[ParseError]`` if the HTML couldn't be
        parsed, the team names couldn't be matched, no play-by-play rows
        were found, or any row failed to parse (the first such row's
        error(s) only -- see the module docstring's ``sequence``
        fail-fast note).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_models import TeamId, Year
            from sportsdataverse.mbb.mbb_ncaa_pbp_parser import parse_game_events, v0_builders

            with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
                html = f.read()
            events = parse_game_events("test.html", html, TeamId("TeamA"), Year(2018), v0_builders)
    """
    try:
        doc = parse_html(in_html)
    except Exception as exc:  # pragma: no cover - bs4/lxml is lenient; mirrors Scala's Try(request)
        return [
            ParseError(
                location=_LOCATION_PARSE_PBP,
                id=f"[{filename}]" if filename else "",
                messages=[f"Exception=[{exc}]"],
            )
        ]

    team_info = parse_team_name(builders.team_finder(doc), target_team, year)
    if isinstance(team_info, ParseError):
        return enrich_sub_error(_LOCATION_PARSE_PBP, filename, team_info)
    _, _, target_team_first = team_info

    html_events = builders.event_finder(doc)
    if not html_events:
        return [build_sub_error(_LOCATION_PARSE_PBP, error=f"No play by play events found [{doc}]")]

    model_events: list[PlayByPlayEvent] = []
    for html_event in html_events:
        result = parse_game_event(html_event, target_team_first, builders)
        if result and isinstance(result[0], ParseError):
            return result
        model_events.extend(cast("list[PlayByPlayEvent]", result))

    if enrich:
        return enrich_and_reverse_game_events(model_events)
    return model_events


def get_sorted_pbp_events(
    filename: str,
    in_html: str,
    box_lineup: LineupEvent,
    format_version: int,
) -> Union[list[PlayByPlayEvent], list[ParseError]]:
    """Handy util to return the play-by-play events in chronological order,
    used in a few other places (``PlayByPlayParser.get_sorted_pbp_events``,
    ``:221-239``).

    Args:
        filename: The source file name, used only for error reporting.
        in_html: The raw play-by-play-page HTML.
        box_lineup: The team's box-score lineup (supplies ``team``/``year``).
        format_version: ``0`` for the legacy layout, ``1`` for the 2018+
            layout.

    Returns:
        The play-by-play events in chronological (earliest-to-latest)
        order, or a ``list[ParseError]`` on failure. ``enrich=True`` is
        used internally to get the correct ascending timestamps, and its
        reversal is undone here (``.reverse``) to restore chronological
        order.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
            from sportsdataverse.mbb.mbb_ncaa_models import TeamId
            from sportsdataverse.mbb.mbb_ncaa_pbp_parser import get_sorted_pbp_events

            with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
                box_html = f.read()
            box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=0)

            with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
                pbp_html = f.read()
            events = get_sorted_pbp_events("test.html", pbp_html, box_lineup, format_version=0)
    """
    builders = _BUILDERS_FROM_VERSION[format_version]
    result = parse_game_events(filename, in_html, box_lineup.team.team, box_lineup.team.year, builders, enrich=True)
    if result and isinstance(result[0], ParseError):
        return result
    return list(reversed(cast("list[PlayByPlayEvent]", result)))


# ---------------------------------------------------------------------------
# create_lineup_data -- the full pipeline orchestration
# (``PlayByPlayParser.scala:153-217``)
# ---------------------------------------------------------------------------


def create_lineup_data(
    filename: str,
    in_html: str,
    box_lineup: LineupEvent,
    format_version: int,
) -> Union[tuple[list[LineupEvent], list[LineupEvent]], list[ParseError]]:
    """Combines the different methods to build a set of lineup events
    (``PlayByPlayParser.create_lineup_data``, ``:153-217``) -- the
    orchestrator that chains the ENTIRE Phase 5a-5d surface:

    1. :func:`parse_game_events` -- HTML -> reversed
       :data:`~sportsdataverse.mbb.mbb_ncaa_stints.PlayByPlayEvent`\\ s.
    2. :func:`~sportsdataverse.mbb.mbb_ncaa_stints.build_partial_lineup_list`
       -- events -> chronological lineup stints.
    3. :func:`~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.fix_possible_score_swap_bug`
       -- undoes a rare NCAA score-transposition bug.
    4. :func:`~sportsdataverse.mbb.mbb_ncaa_lineup_enrich.enrich_lineup`
       (mapped over every stint) -- populates ``pts``/``plus_minus``/stat
       trees.
    5. :func:`~sportsdataverse.mbb.mbb_ncaa_possessions.calculate_possessions`
       -- per-stint possession counts.
    6. Zip each stint with its successor (``None`` for the last), then
       :func:`~sportsdataverse.mbb.mbb_ncaa_stint_validation.validate_lineup`
       partitions the ``(stint, next)`` pairs into good (empty error list)
       and bad.
    7. :func:`~sportsdataverse.mbb.mbb_ncaa_stint_validation.clump_bad_lineups`
       groups consecutive bad stints, then
       :func:`~sportsdataverse.mbb.mbb_ncaa_stint_validation.analyze_and_fix_clumps`
       tries to self-heal each clump.
    8. Concatenate: good stints + every clump's fixed stints -> ``good``;
       every clump's still-unfixed stints -> ``bad``, each stamped with
       ``player_count_error=len(players)`` as the VERY LAST step.

    Args:
        filename: The source file name, used only for error reporting.
        in_html: The raw play-by-play-page HTML.
        box_lineup: The team's validated box-score lineup
            (:func:`~sportsdataverse.mbb.mbb_ncaa_boxscore_parser.get_box_lineup`'s
            result) -- supplies the full roster (for validation), the
            team/year (for parsing), and the trusted final score (for the
            swap-bug fix).
        format_version: ``0`` for the legacy layout, ``1`` for the 2018+
            layout.

    Returns:
        ``(good_lineups, bad_lineups)`` on success, or a ``list[ParseError]``
        if :func:`parse_game_events` failed.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import get_box_lineup
            from sportsdataverse.mbb.mbb_ncaa_models import TeamId
            from sportsdataverse.mbb.mbb_ncaa_pbp_parser import create_lineup_data

            with open("tests/fixtures/ncaa/test_lineup.html", encoding="utf-8") as f:
                box_html = f.read()
            box_lineup = get_box_lineup("test_p1.html", box_html, TeamId("TeamA"), format_version=0)

            with open("tests/fixtures/ncaa/test_play_by_play.html", encoding="utf-8") as f:
                pbp_html = f.read()
            result = create_lineup_data("test.html", pbp_html, box_lineup, format_version=0)

            Pipeline next step (one line)::

                good, bad = result
                sum(ev.duration_mins for ev in good + bad)
    """
    player_codes = {p.code for p in box_lineup.players}
    builders = _BUILDERS_FROM_VERSION[format_version]

    reversed_events = parse_game_events(filename, in_html, box_lineup.team.team, box_lineup.team.year, builders)
    if reversed_events and isinstance(reversed_events[0], ParseError):
        return reversed_events

    # There is a weird bug that has happened one time where the scores got
    # swapped, so we'll identify and fix this case.
    events = fix_possible_score_swap_bug(
        build_partial_lineup_list(cast("list[PlayByPlayEvent]", reversed_events), box_lineup),
        box_lineup,
    )

    processed_events = [enrich_lineup(ev) for ev in events]

    # Calculate possessions per lineup.
    lineups_with_poss = calculate_possessions(processed_events)

    # Get good and bad lineups (together with context). Use the context to
    # fix the bad lineups if possible.
    next_events: list[Optional[LineupEvent]] = list(lineups_with_poss[1:]) + [None]
    zip_lineups: list[tuple[LineupEvent, Optional[LineupEvent]]] = list(zip(lineups_with_poss, next_events))

    good_pairs = [pair for pair in zip_lineups if not validate_lineup(pair[0], box_lineup, player_codes)]
    bad_pairs = [pair for pair in zip_lineups if validate_lineup(pair[0], box_lineup, player_codes)]

    bad_lineup_clumps = clump_bad_lineups(bad_pairs)
    fixed_or_not = [analyze_and_fix_clumps(clump, box_lineup, player_codes) for clump in bad_lineup_clumps]

    final_good_lineups = [ev for ev, _ in good_pairs] + [fixed for fixed_evs, _ in fixed_or_not for fixed in fixed_evs]
    final_bad_lineups = [ev for _, still_to_fix in fixed_or_not for ev in still_to_fix.evs]

    return (
        final_good_lineups,
        # At the last moment, add the player_count_error.
        [replace(ev, player_count_error=len(ev.players)) for ev in final_bad_lineups],
    )
