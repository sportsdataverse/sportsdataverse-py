"""NCAA play-by-play / shot enrichment glue (men's basketball).

Faithful Python port of ``PlayByPlayUtils.scala`` in Alex-At-Home/cbb-explorer
(the Scala NCAA play-by-play ingestion pipeline behind hoop-explorer.com): the
two pure-logic "glue" functions that stitch the shot-event, play-by-play, and
lineup (stint) surfaces together, plus the whole ``ShotEnrichmentUtils`` helper
family. This is the sixth of Phase 5e's seven modules and the last piece of
pure logic before the wbb shims (5e.7).

Ported members (Scala anchors in each docstring):

* :func:`inject_starting_lineup_into_box` -- infer the starting five from
  play-by-play sub sequencing (v1 box scores dropped the ordered starter list)
  (``PlayByPlayUtils.scala:684-845``).
* :func:`enrich_shot_events_with_pbp` -- the main fold that matches each
  :class:`~sportsdataverse.mbb.mbb_ncaa_models.ShotEvent` to its play-by-play
  event and on-floor lineup, filling in the fields Task 5e.5 left as
  placeholders (``:28-278``).
* :func:`find_lineup` -- the recursive lineup-matching state machine, branch
  cases 2.1-2.4 (``:352-517``).
* :func:`find_pbp_clump` -- gather the play-by-play events sharing a shot's
  time (``:556-608``).
* :func:`matching_player` / :func:`extract_player_from_ev` -- resolve and
  compare the shooter/assister against the shot's player (``:610-652``).
* :func:`right_kind_of_shot` -- make/miss + 2/3 gating (``:659-679``).
* :func:`shot_value` -- ``event_string`` -> point value classifier
  (``:534-542``).
* :class:`PeekableIterator` -- the Python stand-in for Scala's
  ``scala.collection.Iterator`` (see the "Scala idiom decisions" note).

**Scala idiom decisions (documented per project convention):**

* **The ``protected object ShotEnrichmentUtils`` is flattened to module
  level.** This project already flattens every Scala companion/nested object
  into plain module-level members (see ``mbb_ncaa_models.py`` /
  ``mbb_ncaa_stints.py`` / ``mbb_ncaa_names.py``); the same is done here. The
  oracle (``PlayByPlayUtilsTests.scala``) imports ``ShotEnrichmentUtils._``
  and calls :func:`find_lineup` / :func:`shot_value` / :func:`find_pbp_clump`
  / :func:`extract_player_from_ev` / :func:`matching_player` /
  :func:`right_kind_of_shot` directly, so all six are public here (no
  ``ShotEnrichmentUtils`` wrapper class).

* **``scala.collection.Iterator`` becomes :class:`PeekableIterator`.**
  :func:`find_lineup` and :func:`find_pbp_clump` consume a *stateful*
  iterator: Scala's ``Iterator.find(pred)`` scans (and consumes) until the
  predicate holds (or the iterator is exhausted), ``Iterator.hasNext`` peeks
  without consuming, and ``Iterator.toList`` drains the rest. A bare Python
  iterator has ``next`` but no ``hasNext``/``find``/``toList`` and cannot peek,
  so :class:`PeekableIterator` wraps one with those exact operations (one
  element of look-ahead buffer for ``has_next``). ``find_pbp_clump`` uses
  ``has_next`` directly, matching the Scala; ``find_lineup`` only uses
  ``find`` / ``to_list``, but the oracle inspects the passed-in iterator's
  ``has_next`` afterward, so the type is shared.

* **Scala's ``EnrichmentState`` case class becomes loop-local variables.**
  ``enrich_shot_events_with_pbp`` is a Scala ``foldLeft`` whose accumulator is
  a mutable-shape ``EnrichmentState``; ported as a plain Python ``for`` loop
  threading ``curr_pbp_clump`` / ``maybe_next_pbp_event`` / ``curr_lineups``
  (and the two :class:`PeekableIterator`\\ s) as locals -- no dataclass wrapper
  for a value that is only ever the fold accumulator.

* **``eq`` (reference identity) becomes ``is``, NOT ``==``.** The clump
  de-duplication ``pbp_clump.filterNot(ev => (ev eq selected_pbp) ||
  maybe_assist_pbp.exists(_ eq ev))`` (``:215-220``) removes *the specific
  matched objects*, by reference. Using ``==`` would be a real bug: two
  distinct play-by-play events at the same time can carry identical strings
  (the oracle's "two valid shots at the same time" case -- shot5/shot6 at
  14.5 min -- feeds two structurally-equal ``OtherTeamEvent``\\ s), and ``==``
  would drop *both* on the first shot, starving the second. Ported as ``is``.

* **Scala ``Set[String]`` becomes Python ``set[str]``.**
  :func:`inject_starting_lineup_into_box`'s ``starters`` / ``excluded`` /
  ``valid_player_codes_set`` all key on player *code* strings (hashable) --
  a direct ``set[str]`` translation, no unhashable-element workaround needed
  (unlike some earlier phases that keyed on whole dataclasses).

* **Debug ``println``\\ s dropped.** The Scala scatters
  ``println(s"[enrich_shot_events_with_pbp] WARN: ...")`` on every discard
  path, plus a ``maybe_debug_event`` summary printer and four
  ``debug_*``/``no_*_debug`` ``val ... = false`` gated blocks
  (``:54-56, 78-80, 103-105, 121-123, 164-169, 222, 229-264, 285-336,
  713-720, 738-807, 819-826``). None affects a return value -- they are
  maintainer diagnostics with no logging surface in this port to route to
  (matching ``mbb_ncaa_names.py``'s dropped ``fixes_for_debug`` and
  ``mbb_ncaa_stints.py``'s dropped debug prints). All dropped; the control
  flow they annotated is preserved exactly.

* **``inject_starting_lineup_into_box``'s ``external_roster`` /
  ``format_version`` params are unused, ported for signature parity.** The
  Scala signature carries both (its ``create_lineup_data`` caller -- Task
  5e.3's pipeline -- passes them), but the function body references neither
  (verified across ``:684-845``). Kept so the eventual pipeline call site
  matches upstream; documented here rather than silently dropped.

* **``CutdownShotEvent`` is NOT produced.** Despite the name, the Scala
  ``shot_value`` here is an unrelated ``event_str -> Int`` point classifier;
  ``CutdownShotEvent`` is dead code in the entire upstream tree (see
  ``mbb_ncaa_models.py``'s note). This module never constructs it.

**Landmine index (reachable error sites, numbered across the module):**
    1. :class:`PeekableIterator.__next__` raises ``StopIteration`` when
       exhausted (standard iterator protocol) -- every internal consumer
       (:meth:`~PeekableIterator.find`, :meth:`~PeekableIterator.to_list`)
       iterates via ``for`` / ``list()`` which absorb it; :func:`find_pbp_clump`
       guards its own ``_pbp_clump_matcher`` calls behind ``has_next``. No
       unguarded ``next`` is reachable through the public surface.
    2. :func:`find_lineup` indexes ``curr_lineups[0]`` / ``curr_lineups[1:]``
       only under an explicit ``if curr_lineups`` truthiness guard -- no
       ``IndexError`` reachable. No division anywhere in the module (the
       distance comparisons in :func:`right_kind_of_shot` are constant
       thresholds, not ratios).
    3. :func:`shot_value` returns ``-1`` for any unrecognized string; every
       caller treats ``> 0`` as "a real shot" and ``<= 0`` as "not a shot"
       (assist == ``0``), so the sentinel never indexes or divides.

Attribution: derived from `cbb-explorer
<https://github.com/Alex-At-Home/cbb-explorer>`_ (Apache License 2.0,
Copyright Alex-At-Home / org.piggottfamily). This is a source-language
translation (Scala -> Python), not a copy; upstream file:
``src/main/scala/org/piggottfamily/cbb_explorer/utils/parsers/ncaa/PlayByPlayUtils.scala``.
See ``NOTICE`` for the full notice.

Example::

    from sportsdataverse.mbb.mbb_ncaa_pbp_glue import enrich_shot_events_with_pbp

    enriched = enrich_shot_events_with_pbp(
        sorted_shot_events,   # from mbb_ncaa_shot_parser.create_shot_event_data
        sorted_pbp_events,    # from mbb_ncaa_pbp_parser.get_sorted_pbp_events
        lineup_events,        # good stints
        bad_lineup_events,    # stints flagged by validation
        box_lineup,           # the roster lineup event
    )
    print(len(enriched))

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college
      basketball data in R.
"""

from __future__ import annotations

from typing import Callable, Generic, Iterable, Iterator, Optional, TypeVar

from sportsdataverse.mbb.mbb_ncaa_events import (
    parse_any_play,
    parse_assist,
    parse_shot_made,
    parse_shot_missed,
    parse_three_pointer_made,
    parse_three_pointer_missed,
    parse_two_pointer_made,
    parse_two_pointer_missed,
)
from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupId,
    PlayerCodeId,
    RawGameEvent,
    RosterEntry,
    ShotEvent,
)
from sportsdataverse.mbb.mbb_ncaa_names import (
    TidyPlayerContext,
    build_tidy_player_context,
    tidy_player,
)
from sportsdataverse.mbb.mbb_ncaa_stints import (
    GameBreakEvent,
    MiscGameEvent,
    OtherOpponentEvent,
    OtherTeamEvent,
    PlayByPlayEvent,
    SubInEvent,
    SubOutEvent,
    build_player_code,
    name_in_v0_box_format,
)

__all__ = [
    "PeekableIterator",
    "inject_starting_lineup_into_box",
    "enrich_shot_events_with_pbp",
    "find_lineup",
    "find_pbp_clump",
    "matching_player",
    "extract_player_from_ev",
    "right_kind_of_shot",
    "shot_value",
]

_T = TypeVar("_T")


class PeekableIterator(Generic[_T]):
    """A stateful iterator with one element of look-ahead, the Python
    stand-in for Scala's ``scala.collection.Iterator``.

    Reproduces the three ``Iterator`` operations :func:`find_pbp_clump` /
    :func:`find_lineup` rely on: :meth:`find` (scan-and-consume until a
    predicate holds), :meth:`has_next` (peek without consuming), and
    :meth:`to_list` (drain the remainder). Standard iterator protocol
    (:meth:`__iter__` / :meth:`__next__`) is also supported.

    Args:
        iterable: Any iterable to wrap.
    """

    def __init__(self, iterable: Iterable[_T]) -> None:
        self._it: Iterator[_T] = iter(iterable)
        self._has_buffer = False
        self._buffer: Optional[_T] = None

    def has_next(self) -> bool:
        """Whether another element is available, without consuming it
        (Scala ``Iterator.hasNext``)."""
        if self._has_buffer:
            return True
        try:
            self._buffer = next(self._it)
        except StopIteration:
            return False
        self._has_buffer = True
        return True

    def __iter__(self) -> "PeekableIterator[_T]":
        return self

    def __next__(self) -> _T:
        if self._has_buffer:
            self._has_buffer = False
            val = self._buffer
            self._buffer = None
            # (mypy: the buffer is only set alongside _has_buffer=True, so it
            # is never None here -- but the field type stays Optional.)
            assert val is not None
            return val
        return next(self._it)

    def find(self, pred: Callable[[_T], bool]) -> Optional[_T]:
        """First element satisfying ``pred``, consuming up to and including
        it (or exhausting the iterator and returning ``None``) -- Scala
        ``Iterator.find``."""
        for x in self:
            if pred(x):
                return x
        return None

    def to_list(self) -> list[_T]:
        """Drain the remaining elements into a list (Scala ``Iterator.toList``)."""
        return list(self)


# ---------------------------------------------------------------------------
# ShotEnrichmentUtils (flattened -- ``PlayByPlayUtils.scala:283-680``)
# ---------------------------------------------------------------------------


def shot_value(event_str: str) -> int:
    """Classify a play-by-play event string as a 3, a 2, or an assist
    (``ShotEnrichmentUtils.shot_value``, ``PlayByPlayUtils.scala:534-542``).

    Ported as an ordered first-match cascade, exactly mirroring the Scala
    ``match`` arm order (assist is tested first, so an assist string never
    falls through to a shot classifier).

    Args:
        event_str: The raw play-by-play event string.

    Returns:
        ``0`` for an assist, ``3`` for any 3-pointer (made or missed), ``2``
        for any 2-pointer (made or missed), or ``-1`` for anything else
        (rebounds, turnovers, unparseable, ...).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_glue import shot_value
            shot_value("18:28:00,0-0,Eric Ayala, 3pt jumpshot made")   # 3
            shot_value("18:28:00,0-0,Kyle Guy, assist")                # 0
            shot_value("04:28:0,52-59,Team, rebound deadballdeadball")  # -1
    """
    if parse_assist(event_str) is not None:
        return 0
    if parse_three_pointer_made(event_str) is not None:
        return 3
    if parse_three_pointer_missed(event_str) is not None:
        return 3
    if parse_two_pointer_made(event_str) is not None:
        return 2
    if parse_two_pointer_missed(event_str) is not None:
        return 2
    return -1


def _shot_or_assist_finder(ev: PlayByPlayEvent) -> Optional[MiscGameEvent]:
    """Return ``ev`` iff it is a shot/assist :data:`~sportsdataverse.mbb
    .mbb_ncaa_stints.MiscGameEvent`, else ``None``
    (``ShotEnrichmentUtils.ShotOrAssistFinder.unapply``,
    ``PlayByPlayUtils.scala:520-532``)."""
    if isinstance(ev, (OtherTeamEvent, OtherOpponentEvent)):
        s = ev.event_string
        if parse_assist(s) is not None or parse_shot_made(s) is not None or parse_shot_missed(s) is not None:
            return ev
    return None


def _pbp_clump_matcher(pbp_it: "PeekableIterator[PlayByPlayEvent]", shot_time: float) -> Optional[MiscGameEvent]:
    """Advance ``pbp_it`` to the next shot/assist event at or after
    ``shot_time`` (``ShotEnrichmentUtils.pbp_clump_matcher``,
    ``PlayByPlayUtils.scala:545-553``). Returns ``None`` when the iterator is
    exhausted without a match."""

    def pred(ev: PlayByPlayEvent) -> bool:
        found = _shot_or_assist_finder(ev)
        return found is not None and found.min >= shot_time

    match = pbp_it.find(pred)
    if match is None:
        return None
    # (pred only accepts shot/assist MiscGameEvents; narrow for the type checker.)
    assert isinstance(match, (OtherTeamEvent, OtherOpponentEvent))
    return match


def find_pbp_clump(
    shot_time: float,
    pbp_it: "PeekableIterator[PlayByPlayEvent]",
    curr_pbp_clump: list[MiscGameEvent],
    maybe_next_pbp_event: Optional[MiscGameEvent],
) -> tuple[list[MiscGameEvent], Optional[MiscGameEvent]]:
    """Gather every play-by-play shot/assist event sharing ``shot_time``
    (``ShotEnrichmentUtils.find_pbp_clump``, ``PlayByPlayUtils.scala:556-608``).

    If ``curr_pbp_clump`` (carried over from the previous shot) already holds
    events at ``shot_time`` they are returned as-is; otherwise the iterator is
    walked forward, discarding earlier events, accumulating the equal-time
    ones, and stopping (returning it as ``maybe_next_pbp_event``) at the first
    later event.

    Args:
        shot_time: The shot's game-clock minute to gather events for.
        pbp_it: The shared play-by-play iterator (consumed in place).
        curr_pbp_clump: Events left over from the previous shot's clump.
        maybe_next_pbp_event: The look-ahead event stashed by the previous
            call, if any.

    Returns:
        ``(clump, maybe_next)`` -- the equal-time events, plus the first
        strictly-later event (or ``None`` at end of stream).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_glue import (
                PeekableIterator,
                find_pbp_clump,
            )
            clump, nxt = find_pbp_clump(5.0, PeekableIterator([]), [], None)
            # ([], None)
    """

    def recurse(
        tmp_curr_pbp_clump: list[MiscGameEvent],
        tmp_maybe_next_pbp_event: Optional[MiscGameEvent],
    ) -> tuple[list[MiscGameEvent], Optional[MiscGameEvent]]:
        while True:
            if tmp_maybe_next_pbp_event is None and pbp_it.has_next():
                # get next pbp event
                tmp_maybe_next_pbp_event = _pbp_clump_matcher(pbp_it, shot_time)
                continue
            if tmp_maybe_next_pbp_event is None:
                # end of the PbP events
                return (tmp_curr_pbp_clump, None)
            if tmp_maybe_next_pbp_event.min < shot_time:
                # next pbp is before the clump, discard it and check the next one
                tmp_maybe_next_pbp_event = _pbp_clump_matcher(pbp_it, shot_time)
                continue
            if tmp_maybe_next_pbp_event.min == shot_time:
                # next pbp is part of clump
                tmp_curr_pbp_clump = tmp_curr_pbp_clump + [tmp_maybe_next_pbp_event]
                tmp_maybe_next_pbp_event = _pbp_clump_matcher(pbp_it, shot_time)
                continue
            # next pbp is not part of clump (min > shot_time), so we're done for now
            return (tmp_curr_pbp_clump, tmp_maybe_next_pbp_event)

    clump_time_matches = [ev for ev in curr_pbp_clump if ev.min == shot_time]
    if clump_time_matches:  # Still some events from prev call left over
        return (clump_time_matches, maybe_next_pbp_event)
    # get the next clump, having flushed
    return recurse([], maybe_next_pbp_event)


def extract_player_from_ev(
    shot: ShotEvent,
    pbp_event: MiscGameEvent,
    tidy_ctx: TidyPlayerContext,
) -> Optional[PlayerCodeId]:
    """Resolve the player named in ``pbp_event`` to a
    :class:`~sportsdataverse.mbb.mbb_ncaa_models.PlayerCodeId`
    (``ShotEnrichmentUtils.extract_player_from_ev``,
    ``PlayByPlayUtils.scala:613-635``).

    For a shot by the team under analysis (``shot.is_off``) the name is
    tidied against the box score before coding (so a mis-spelled play-by-play
    name resolves to the roster identity); for an opponent shot it is coded
    verbatim with no team context.

    Args:
        shot: The shot being enriched (only ``is_off`` is read).
        pbp_event: The play-by-play event naming the player.
        tidy_ctx: The name-resolution context for this game.

    Returns:
        The resolved ``PlayerCodeId``, or ``None`` if the event string names
        no player (:func:`~sportsdataverse.mbb.mbb_ncaa_events.parse_any_play`
        found nothing).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_glue import extract_player_from_ev
            pc = extract_player_from_ev(shot, pbp_event, tidy_ctx)
    """
    v1_player_name = parse_any_play(pbp_event.event_string)
    if v1_player_name is None:
        return None
    player_name = name_in_v0_box_format(v1_player_name)
    if shot.is_off:
        tidier_player_name, _ = tidy_player(player_name, tidy_ctx)
        return build_player_code(tidier_player_name, tidy_ctx.box_lineup.team.team)
    return build_player_code(player_name, None)


def matching_player(
    shot: ShotEvent,
    pbp_event: MiscGameEvent,
    tidy_ctx: TidyPlayerContext,
    code_match: bool,
) -> bool:
    """Whether the player in ``pbp_event`` matches ``shot``'s shooter
    (``ShotEnrichmentUtils.matching_player``, ``PlayByPlayUtils.scala:638-652``).

    Args:
        shot: The shot being enriched.
        pbp_event: The candidate play-by-play event.
        tidy_ctx: The name-resolution context.
        code_match: If ``True``, compare on player *code* only (looser -- lets
            a name that resolves to the wrong identity but the right code
            match); if ``False``, require full :class:`~sportsdataverse.mbb
            .mbb_ncaa_models.PlayerCodeId` equality.

    Returns:
        ``True`` if the resolved player matches ``shot.player`` under the
        selected comparison, else ``False``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_glue import matching_player
            matching_player(shot, pbp_event, tidy_ctx, code_match=False)
    """
    player = extract_player_from_ev(shot, pbp_event, tidy_ctx)
    if player is None:
        return False
    if code_match:
        return shot.player is not None and shot.player.code == player.code
    return shot.player is not None and shot.player == player


def right_kind_of_shot(shot: ShotEvent, pbp_event: MiscGameEvent, strict: bool) -> bool:
    """Whether ``pbp_event``'s shot type is compatible with ``shot``'s
    distance and make/miss (``ShotEnrichmentUtils.right_kind_of_shot``,
    ``PlayByPlayUtils.scala:659-679``).

    The distance-in-the-data is approximate, so exact 2-vs-3 discrimination is
    impossible; this only rules out the *obvious* mismatches (a clearly-short
    shot matched to a 3, or vice versa) and always requires make/miss
    agreement.

    Args:
        shot: The shot being enriched (``pts``/``dist`` read).
        pbp_event: The candidate play-by-play event.
        strict: If ``True``, also apply the distance gate; if ``False``, only
            the make/miss agreement is required.

    Returns:
        ``True`` if the event could plausibly be this shot.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_glue import right_kind_of_shot
            right_kind_of_shot(shot, pbp_event, strict=True)
    """
    ev_shot_value = shot_value(pbp_event.event_string)
    ev_shot_made = parse_shot_made(pbp_event.event_string) is not None
    shot_made = shot.pts > 0
    definitely_2 = shot.dist < 20  # 21.6 to be exact, but the data is noisy so be conservative
    definitely_3 = shot.dist >= 23.1  # 22.1 to be exact, but the data is noisy so basically ignore

    return (shot_made == ev_shot_made) and (
        not strict
        or (definitely_2 and ev_shot_value == 2)
        or (definitely_3 and ev_shot_value == 3)
        or (not definitely_2 and not definitely_3)
    )


def find_lineup(
    shot: ShotEvent,
    curr_pbp: Optional[MiscGameEvent],
    curr_lineups: list[LineupEvent],
    lineup_it: "PeekableIterator[LineupEvent]",
) -> tuple[Optional[LineupEvent], list[LineupEvent]]:
    """Find the lineup (stint) event on the floor for ``shot``
    (``ShotEnrichmentUtils.find_lineup``, ``PlayByPlayUtils.scala:352-517``).

    A recursive state machine over three lists: ``curr_lineup`` (the current
    candidate), ``fallback_lineups`` (time-matching lineups whose raw events
    did not contain ``curr_pbp`` -- kept as fallbacks), and ``stashed_lineups``
    (lineups pulled from the iterator but not yet stepped into, available for
    future shots). The branch cases (labelled 2.1-2.4 in the Scala):

    * **2.1** -- no time-matching lineup left: return the fallbacks.
    * **2.2** -- the next lineup starts *after* the shot: no match, stash it.
    * **2.3** -- strictly inside a lineup with no prior fallbacks: take it.
    * **2.4** -- shot is exactly at a lineup boundary (or we are already
      choosing among multiple fallbacks): take this lineup iff its raw game
      events contain ``curr_pbp``'s event string (``curr_pbp is None`` takes
      it unconditionally); otherwise keep it as a fallback and recurse.

    Args:
        shot: The shot to place (only ``min`` / ``is_off`` are read).
        curr_pbp: The already-matched play-by-play event for this shot, used
            to disambiguate boundary lineups; ``None`` disables that check.
        curr_lineups: Lineups pulled from the iterator on a previous call and
            still available (the current one first).
        lineup_it: The shared lineup iterator (consumed in place).

    Returns:
        ``(matched_lineup_or_None, lineups_to_retry_next_time)`` -- the second
        element always includes the matched lineup (so out-of-order shots
        sharing it still resolve) plus any leftover stash.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_glue import (
                PeekableIterator,
                find_lineup,
            )
            matched, retry = find_lineup(shot, None, [lineup], PeekableIterator([]))
    """

    def lineup_matcher(ev: LineupEvent, shot_min: float) -> bool:
        return ev.end_min >= shot_min and shot_min >= ev.start_min

    def pbp_event_str(raw_events: list[RawGameEvent], is_off: bool) -> list[str]:
        """Event strings from the shot's side (team if is_off else opponent)."""
        out: list[str] = []
        for raw in raw_events:
            side = raw.team if is_off else raw.opponent
            if side is not None:
                out.append(side)
        return out

    def find_lineup_recurse(
        curr_lineup: Optional[LineupEvent],
        fallback_lineups: list[LineupEvent],
        stashed_lineups: list[LineupEvent],
    ) -> tuple[Optional[LineupEvent], list[LineupEvent], list[LineupEvent]]:
        while True:
            # Step 1: get a lineup that matches the time
            stashed_it: PeekableIterator[LineupEvent] = PeekableIterator(stashed_lineups)
            if curr_lineup is not None and lineup_matcher(curr_lineup, shot.min):
                maybe_matching_lineup: Optional[LineupEvent] = curr_lineup
            else:
                # Check stash then back to main list looking for candidate lineups
                maybe_matching_lineup = stashed_it.find(lambda lu: shot.min <= lu.end_min)
                if maybe_matching_lineup is None:
                    maybe_matching_lineup = lineup_it.find(lambda lu: shot.min <= lu.end_min)
            updated_stash = stashed_it.to_list()  # (keep any lineups we haven't stepped into yet)

            # Step 2: handle the shot-exactly-at-lineup-end special case (2.4)
            # plus the misc cases (2.1 - 2.3)
            if maybe_matching_lineup is None:
                # 2.1] no more data in main it, just return fallback
                return (None, fallback_lineups, updated_stash)
            if not lineup_matcher(maybe_matching_lineup, shot.min):
                # 2.2] This lineup starts after the shot, so no match but keep it in the stash
                return (None, fallback_lineups, [maybe_matching_lineup] + updated_stash)
            if not fallback_lineups and shot.min < maybe_matching_lineup.end_min:
                # 2.3] Strictly inside the lineup and the previous lineup(s) didn't match
                return (maybe_matching_lineup, fallback_lineups, updated_stash)
            # 2.4] Either "pick from multiple lineups" (fallbacks nonempty) or shot_min == end_min
            if curr_pbp is None or (
                curr_pbp.event_string in pbp_event_str(maybe_matching_lineup.raw_game_events, shot.is_off)
            ):
                return (
                    maybe_matching_lineup,
                    fallback_lineups + [maybe_matching_lineup],
                    updated_stash,
                )
            # this lineup matches but didn't match the PbP event so keep looking
            # (we save the *first* matching lineup in case we can't find a matching PbP event)
            curr_lineup = None  # (force it to take a new lineup)
            fallback_lineups = fallback_lineups + [maybe_matching_lineup]
            stashed_lineups = updated_stash  # (move from the stash to the fallbacks)

    # Top-level logic
    if curr_lineups and curr_lineups[0].start_min > shot.min:
        # Special case: we've gone past the lineup, wait for the shot to catch up
        post_curr_lineup: Optional[LineupEvent] = None
        post_fallback_lineups: list[LineupEvent] = []
        post_stashed_lineups: list[LineupEvent] = curr_lineups
    else:
        head = curr_lineups[0] if curr_lineups else None
        post_curr_lineup, post_fallback_lineups, post_stashed_lineups = find_lineup_recurse(head, [], curr_lineups[1:])

    if not post_fallback_lineups:
        # (no fallbacks, just return the lineup)
        retry = ([post_curr_lineup] if post_curr_lineup is not None else []) + post_stashed_lineups
        return (post_curr_lineup, retry)
    # there are fallbacks, which means the "matching_lineup" must be one of them
    matched = post_curr_lineup if post_curr_lineup is not None else post_fallback_lineups[0]
    return (matched, post_fallback_lineups + post_stashed_lineups)


# ---------------------------------------------------------------------------
# enrich_shot_events_with_pbp (``PlayByPlayUtils.scala:28-278``)
# ---------------------------------------------------------------------------


def enrich_shot_events_with_pbp(
    sorted_shot_events: list[ShotEvent],
    sorted_pbp_events: list[PlayByPlayEvent],
    lineup_events: list[LineupEvent],
    bad_lineup_events: list[LineupEvent],
    box_lineup: LineupEvent,
) -> list[ShotEvent]:
    """Enrich each shot with its play-by-play event + on-floor lineup
    (``PlayByPlayUtils.enrich_shot_events_with_pbp``,
    ``PlayByPlayUtils.scala:28-278``).

    Folds over the (time-sorted) shots, threading two iterators (play-by-play
    and lineup) and a small amount of carry-over state. For each shot it:

    1. gathers the play-by-play events at the shot's time (:func:`find_pbp_clump`),
       keeping only the ones on the shot's side (team if ``is_off``);
    2. picks the matching shot event via a strict -> loose -> first-of-N
       cascade (:func:`right_kind_of_shot` then :func:`matching_player`);
    3. locates the on-floor lineup (:func:`find_lineup`), falling back to
       ``bad_lineup_events`` if the good lineups yield nothing (a bad-lineup
       match is used for ``players`` but its id is suppressed);
    4. attributes an assist (a same-time non-self assist event) and transition
       flag (``"fastbreak"`` in the event string), and fills in ``lineup_id`` /
       ``players`` / ``pts`` / ``value`` / ``ast_by`` / ``is_ast`` / ``is_trans``
       -- exactly the fields Task 5e.5's parser left as placeholders.

    Shots with no matching play-by-play clump, no matching shot event, or no
    matching lineup are dropped (the Scala logs a ``WARN`` and discards; the
    logging is dropped per the module note, the discard preserved).

    Args:
        sorted_shot_events: Shots in ascending game-clock order.
        sorted_pbp_events: The full play-by-play event stream, ascending.
        lineup_events: The good (validation-passing) stint events.
        bad_lineup_events: The validation-flagged stint events, used only as a
            last resort (their ids are never attributed).
        box_lineup: The roster lineup event (drives name resolution).

    Returns:
        The enriched, still-time-sorted list of shots (a subset of the input --
        unmatchable shots are dropped).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_glue import enrich_shot_events_with_pbp
            enriched = enrich_shot_events_with_pbp(
                shots, pbp, good_lineups, bad_lineups, box_lineup
            )
    """
    tidy_ctx = build_tidy_player_context(box_lineup)

    pbp_it: PeekableIterator[PlayByPlayEvent] = PeekableIterator(sorted_pbp_events)
    lineup_it: PeekableIterator[LineupEvent] = PeekableIterator(lineup_events)

    enriched_shot_events: list[ShotEvent] = []
    curr_pbp_clump: list[MiscGameEvent] = []
    maybe_next_pbp_event: Optional[MiscGameEvent] = None
    curr_lineups: list[LineupEvent] = []

    for shot in sorted_shot_events:
        pbp_clump, maybe_next_pbp_event = find_pbp_clump(shot.min, pbp_it, curr_pbp_clump, maybe_next_pbp_event)

        # (ignore game events from the other team than took the shot)
        pbp_clump_for_shot = [
            ev
            for ev in pbp_clump
            if (isinstance(ev, OtherTeamEvent) and shot.is_off)
            or (isinstance(ev, OtherOpponentEvent) and not shot.is_off)
        ]

        maybe_enriched_shot: Optional[ShotEvent] = None
        remaining_pbp_events: list[MiscGameEvent] = pbp_clump
        saved_lineups: list[LineupEvent] = curr_lineups

        if pbp_clump_for_shot:
            pbp_shots = [ev for ev in pbp_clump_for_shot if shot_value(ev.event_string) > 0]
            pbp_assists = [ev for ev in pbp_clump_for_shot if not (shot_value(ev.event_string) > 0)]

            maybe_selected_pbp = _select_pbp_shot(shot, pbp_shots, tidy_ctx)

            if maybe_selected_pbp is not None:
                selected_pbp = maybe_selected_pbp
                # We have a matching PbP event, now match it to a lineup:
                lineup_result = find_lineup(shot, maybe_selected_pbp, curr_lineups, lineup_it)
                if lineup_result[0] is None:
                    # Try with bad lineups (rare; don't care that this re-scans)
                    curr_bad_lineup, _ = find_lineup(shot, maybe_selected_pbp, [], PeekableIterator(bad_lineup_events))
                    curr_lineup: Optional[LineupEvent] = curr_bad_lineup
                    stashed_lineups = lineup_result[1]
                    used_bad_lineup = True
                else:
                    curr_lineup = lineup_result[0]
                    stashed_lineups = lineup_result[1]
                    used_bad_lineup = False

                if curr_lineup is not None:
                    lineup = curr_lineup
                    # Look for assists (can't assist a missed shot, can't self-assist):
                    maybe_assist_pbp: Optional[MiscGameEvent] = None
                    if shot.pts > 0:
                        for ev in pbp_assists:
                            if not matching_player(shot, ev, tidy_ctx, code_match=True):
                                maybe_assist_pbp = ev
                                break

                    shot_val = shot_value(selected_pbp.event_string)
                    enriched_shot = _copy_shot(
                        shot,
                        player=shot.player if shot.is_off else None,  # (discard oppo shooters)
                        lineup_id=None if used_bad_lineup else lineup.lineup_id,
                        raw_event=None,  # (filter out before writing to disk)
                        players=lineup.players,
                        pts=shot.pts * shot_val,
                        value=shot_val,
                        is_ast=True if maybe_assist_pbp is not None else None,
                        ast_by=(
                            extract_player_from_ev(shot, maybe_assist_pbp, tidy_ctx)
                            if (maybe_assist_pbp is not None and shot.is_off)
                            else None
                        ),
                        is_trans=True if "fastbreak" in selected_pbp.event_string else None,
                    )

                    remaining_pbp_events = [
                        ev
                        for ev in pbp_clump
                        if not (ev is selected_pbp or (maybe_assist_pbp is not None and ev is maybe_assist_pbp))
                    ]
                    maybe_enriched_shot = enriched_shot
                    saved_lineups = stashed_lineups
                else:
                    # No matching lineup -- discard (WARN dropped)
                    remaining_pbp_events = pbp_clump
                    saved_lineups = stashed_lineups
            else:
                # No matching shot event -- discard (WARN dropped)
                remaining_pbp_events = pbp_clump
                saved_lineups = curr_lineups
        else:
            # No matching PbP events -- discard (WARN dropped)
            remaining_pbp_events = pbp_clump
            saved_lineups = curr_lineups

        if maybe_enriched_shot is not None:
            enriched_shot_events = enriched_shot_events + [maybe_enriched_shot]
        curr_pbp_clump = remaining_pbp_events
        curr_lineups = saved_lineups

    return enriched_shot_events


def _select_pbp_shot(
    shot: ShotEvent,
    pbp_shots: list[MiscGameEvent],
    tidy_ctx: TidyPlayerContext,
) -> Optional[MiscGameEvent]:
    """The strict -> loose -> first-of-N shot-selection cascade
    (``PlayByPlayUtils.scala:73-142``).

    Extracted from :func:`enrich_shot_events_with_pbp`'s fold body for
    readability (the Scala inlines it); pure, so the extraction is behavior-
    preserving.
    """
    # First: keep only the plausibly-right shots (lax distance gate)
    candidate_matches = [ev for ev in pbp_shots if right_kind_of_shot(shot, ev, strict=False)]
    if not candidate_matches:
        return None  # (WARN dropped) NO_PBP

    # Player filter: strict (full identity) then loose (code only)
    player_filtered = [ev for ev in candidate_matches if matching_player(shot, ev, tidy_ctx, code_match=False)]
    if not player_filtered:
        player_filtered = [ev for ev in candidate_matches if matching_player(shot, ev, tidy_ctx, code_match=True)]

    if not player_filtered:
        if len(candidate_matches) == 1:
            # only one candidate, use it despite the player-code mismatch (WARN dropped)
            return candidate_matches[0]
        # no player match and multiple time/make-or-miss matches: try the strict distance gate
        strict_matches = [ev for ev in candidate_matches if right_kind_of_shot(shot, ev, strict=True)]
        if len(strict_matches) == 1:
            return strict_matches[0]
        # too many "wrong" candidates (or none), bail (WARN dropped)
        return None

    if len(player_filtered) == 1:
        return player_filtered[0]  # A happy case!

    # Multiple player matches: try to narrow with the strict distance gate,
    # otherwise just ... pick the first.
    strict_player_matches = [ev for ev in player_filtered if right_kind_of_shot(shot, ev, strict=True)]
    if strict_player_matches:
        return strict_player_matches[0]
    return player_filtered[0]


def _copy_shot(
    shot: ShotEvent,
    *,
    player: Optional[PlayerCodeId],
    lineup_id: Optional[LineupId],
    raw_event: Optional[str],
    players: list[PlayerCodeId],
    pts: int,
    value: int,
    is_ast: Optional[bool],
    ast_by: Optional[PlayerCodeId],
    is_trans: Optional[bool],
) -> ShotEvent:
    """Build the enriched ``ShotEvent`` copy (Scala ``shot.copy(...)``,
    ``PlayByPlayUtils.scala:192-213``). All non-overridden fields carry over
    from ``shot``."""
    return ShotEvent(
        player=player,
        date=shot.date,
        location_type=shot.location_type,
        team=shot.team,
        opponent=shot.opponent,
        is_off=shot.is_off,
        lineup_id=lineup_id,
        players=players,
        score=shot.score,
        min=shot.min,
        loc=shot.loc,
        geo=shot.geo,
        dist=shot.dist,
        pts=pts,
        value=value,
        ast_by=ast_by,
        is_ast=is_ast,
        is_trans=is_trans,
        raw_event=raw_event,
    )


# ---------------------------------------------------------------------------
# inject_starting_lineup_into_box (``PlayByPlayUtils.scala:684-845``)
# ---------------------------------------------------------------------------


def inject_starting_lineup_into_box(
    sorted_pbp_events: list[PlayByPlayEvent],
    box_lineup: LineupEvent,
    external_roster: tuple[list[str], list[RosterEntry]],
    format_version: int,
) -> LineupEvent:
    """Infer the starting five and reorder the box-score roster so they lead
    (``PlayByPlayUtils.inject_starting_lineup_into_box``,
    ``PlayByPlayUtils.scala:684-845``).

    The v1 (2018+) NCAA box score dropped the ordered list of starters, so we
    reconstruct it from the play-by-play sub sequencing. A player is a starter
    if, walking the events forward, they are seen *before* their first sub-in
    -- either subbed *out* (before ever being subbed in), or *named in a
    team-side play* that isn't concurrent with a sub. Anyone subbed *in* before
    ever being seen is excluded. The reconstruction stops once five starters
    are found.

    Args:
        sorted_pbp_events: The full play-by-play event stream, ascending time.
        box_lineup: The box-score lineup event (its ``players`` is the full
            roster to reorder).
        external_roster: Unused here -- carried for signature parity with the
            Scala (its pipeline caller passes it). See the module note.
        format_version: Unused here -- carried for signature parity. See the
            module note.

    Returns:
        A copy of ``box_lineup`` with ``players`` reordered so the inferred
        starters lead. If fewer than five starters could be inferred (a
        "40-trillion" player who was never subbed nor mentioned), the roster
        is ordered starters -> possible-starters -> definitely-not-starters as
        the best available guess.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_pbp_glue import inject_starting_lineup_into_box
            fixed = inject_starting_lineup_into_box(pbp_events, box_lineup, ([], []), 1)
    """
    tidy_ctx = build_tidy_player_context(box_lineup)

    def pbp_name_to_code(player_name: str) -> str:
        tidier_player_name, _ = tidy_player(player_name, tidy_ctx)
        return build_player_code(tidier_player_name, box_lineup.team.team).code

    valid_player_codes_set = {p.code for p in box_lineup.players}

    starters: set[str] = set()
    excluded: set[str] = set()
    last_sub_time = 0.0  # (times ascend from 0)

    for ev in sorted_pbp_events:
        if len(starters) >= 5:
            break  # (we have all the starters; the fold leaves the rest untouched)

        # Ignore mis-spellings in sub-events (a sub whose code isn't on the roster)
        if isinstance(ev, (SubInEvent, SubOutEvent)) and pbp_name_to_code(ev.player_name) not in valid_player_codes_set:
            continue

        # Game break resets the last-sub time
        if isinstance(ev, GameBreakEvent):
            last_sub_time = ev.min
            continue

        # Subbed out, so if not excluded (we haven't seen them subbed-in) then a starter
        if isinstance(ev, SubOutEvent):
            player_code = pbp_name_to_code(ev.player_name)
            if player_code not in excluded:
                starters.add(player_code)
                last_sub_time = ev.min
            continue

        # Subbed in, so (if not already a known starter) definitely not a starter
        if isinstance(ev, SubInEvent):
            player_code = pbp_name_to_code(ev.player_name)
            if player_code not in starters:
                excluded.add(player_code)
                last_sub_time = ev.min
            continue

        # A team-direction play, past the last sub time: a non-excluded, not-yet-seen
        # mentioned player must be a starter
        if isinstance(ev, (OtherTeamEvent, OtherOpponentEvent)) and ev.is_team_dir and ev.min > last_sub_time:
            name = parse_any_play(ev.event_string)
            if name is not None:
                player_code = pbp_name_to_code(name)
                if (
                    player_code in valid_player_codes_set
                    and player_code not in excluded
                    and player_code not in starters
                ):
                    starters.add(player_code)
        # else: no-op

    inferred_starters = [p for p in box_lineup.players if p.code in starters]
    probably_not_starters = [p for p in box_lineup.players if p.code not in starters]

    # (WARN when != 5 dropped -- diagnostic only)

    if len(inferred_starters) >= 5:
        return _copy_lineup_players(box_lineup, inferred_starters + probably_not_starters)

    # Pathological: a starter played the whole game without ever being mentioned.
    # Pick the players who never appeared in the excluded set as the best guess.
    definitely_not_starters = [p for p in probably_not_starters if p.code in excluded]
    just_possibly_starters = [p for p in probably_not_starters if p.code not in excluded]
    return _copy_lineup_players(box_lineup, inferred_starters + just_possibly_starters + definitely_not_starters)


def _copy_lineup_players(box_lineup: LineupEvent, players: list[PlayerCodeId]) -> LineupEvent:
    """Build a copy of ``box_lineup`` with a new ``players`` order (Scala
    ``box_lineup.copy(players = ...)``)."""
    return LineupEvent(
        date=box_lineup.date,
        location_type=box_lineup.location_type,
        start_min=box_lineup.start_min,
        end_min=box_lineup.end_min,
        duration_mins=box_lineup.duration_mins,
        score_info=box_lineup.score_info,
        team=box_lineup.team,
        opponent=box_lineup.opponent,
        lineup_id=box_lineup.lineup_id,
        players=players,
        players_in=box_lineup.players_in,
        players_out=box_lineup.players_out,
        raw_game_events=box_lineup.raw_game_events,
        team_stats=box_lineup.team_stats,
        opponent_stats=box_lineup.opponent_stats,
        player_count_error=box_lineup.player_count_error,
    )
