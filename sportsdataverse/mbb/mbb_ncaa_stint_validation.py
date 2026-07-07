"""NCAA lineup validation (men's basketball) -- ``ValidationError`` + ``validate_lineup``.

Faithful Python port of the **stint-VALIDATION half** of
``LineupErrorAnalysisUtils.scala`` in Alex-At-Home/cbb-explorer (the Scala
NCAA play-by-play ingestion pipeline behind hoop-explorer.com): the checks
that flag a built lineup stint as internally inconsistent. The
**name-resolution half** of the same Scala object (``tidy_player`` /
``build_tidy_player_context`` / ``NameFixer``) was ported in Phase 5b as
:mod:`sportsdataverse.mbb.mbb_ncaa_names`; this module consumes that surface
directly (:func:`~sportsdataverse.mbb.mbb_ncaa_names.build_tidy_player_context`,
:func:`~sportsdataverse.mbb.mbb_ncaa_names.tidy_player`) rather than
re-deriving it. Task 5d.1 ports only ``ValidationError`` + ``allowedErrors``
+ ``validate_lineup`` (``LineupErrorAnalysisUtils.scala:18-26,181-218``) --
the ONE piece of this Scala object with an upstream oracle
(``LineupErrorAnalysisUtilsTests.scala:55-120``). Task 5d.2 extends this
module with ``BadLineupClump`` + ``clump_bad_lineups`` +
``categorize_bad_lineups`` (``:223-263,617-633``) -- the clump-grouping half,
which has NO upstream oracle (the Scala's own doc comment on
``clump_bad_lineups`` says "TODO test"); its tests are hand-built fixtures
with expected outputs hand-derived from the Scala fold, not transliterations
of an existing suite. Task 5d.3 extends this module with the self-healing
fixers (``handle_common_sub_bug`` / ``find_missing_subs`` /
``add_missing_players`` / ``analyze_and_fix_clumps``), which likewise have NO
upstream oracle (the Scala's own doc comment on the object reads "TODO test")
-- their tests are hand-built fixtures whose expected outputs were
hand-derived from the Scala algorithm on paper (candidate-set evolution per
event, ``matching_index``, per-phase routing, ``validate_lineup`` outcomes),
never produced by running the port.

Ported members (Scala anchors in each docstring):

* :class:`ValidationError` -- the 3 ways a lineup can be declared invalid
  (``:18-20``).
* :data:`ALLOWED_ERRORS` -- the terminating filter set (``:21-26``).
* :func:`validate_lineup` -- the 3 independent checks (``:181-218``).
* :class:`BadLineupClump` -- a clumped run of bad lineup events, plus the
  next known-good event (``:223-226``).
* :func:`clump_bad_lineups` -- groups consecutive bad lineup events into
  :class:`BadLineupClump`\\ s via a 5-condition adjacency predicate
  (``:229-263``).
* :func:`categorize_bad_lineups` -- display-only aggregation of clumps by
  player count (``:617-633``).
* :func:`handle_common_sub_bug` -- fixes a single-event ``2-in-1-out``
  followed by a compensating ``1-out`` in the next good lineup (``:269-298``).
* :func:`find_missing_subs` -- trims a too-many-players clump by identifying
  the "ghost" player(s) never confirmed by a sub-out or play mention
  (``:406-514``).
* :func:`add_missing_players` -- back-fills a too-few-players clump from the
  next-good sub-outs and play mentions (``:315-401``).
* :func:`analyze_and_fix_clumps` -- the strict fixer pipeline
  ``handle_common_sub_bug`` -> ``find_missing_subs`` -> ``add_missing_players``
  -> ``find_missing_subs`` (again), then a ``lineup_id`` recompute on every
  fixed lineup (``:556-610``).

**Debug-only ``display_lineup`` (``:301-312``) and ``analyze_unfixed_clumps``
(``:517-553``) are DROPPED, not ported.** Both are pure ``println``
diagnostics gated behind the Scala object's ``debug`` flag (their only
call site, ``:594-595``, is ``if (debug && to_fix.evs.nonEmpty)``); this port
has no ``debug`` flag and no equivalent output path, so
:func:`analyze_and_fix_clumps` simply omits the gated branch. Dropping them is
behavior-preserving -- neither function has any effect on the returned
``(fixed, still_to_fix)`` tuple; they only print.

**Set-of-``PlayerCodeId`` sites are ported as list membership (``in`` /
``not in``), NOT ``.code``-keyed sets.** ``PlayerCodeId`` is a mutable (and
therefore unhashable) dataclass in this port, so the Scala's
``Set[PlayerCodeId]`` membership/difference/``.distinct`` cannot use a Python
``set``. Every such site here uses a plain ``list`` with ``in`` / ``not in``,
which dispatches ``PlayerCodeId.__eq__`` -- full-value (``code`` + ``id`` +
``ncaa_id``) equality, byte-for-byte the same predicate Scala's ``Set``
membership uses (``equals``). Keying by ``.code`` alone would be *narrower*
(two distinct identities sharing a code would collide), so the value-equality
list-membership port is the faithful choice, not the convenient one.
:func:`_distinct` is the order-preserving ``List#distinct`` / ``.toSet``
size-and-dedup port built on the same ``==`` predicate.

**Return shape: a Python ``list``, not a ``set``.** The Scala signature
returns ``Set[ValidationError.Value]``, but ``ValidationError`` is a Scala
``Enumeration``, and the oracle asserts against ``.toList`` --
``scala.collection.immutable.Set`` built from an ``Enumeration``'s
``ValueSet`` always iterates/``.toList``s in declaration (ordinal) order,
regardless of insertion order (see the multi-error oracle case:
``List(WrongNumberOfPlayers, UnknownPlayers)``, never the reverse). A plain
Python ``set`` of :class:`ValidationError` members has no such ordering
guarantee, so :func:`validate_lineup` returns a ``list`` built by appending
each failing check **in declaration order** (:data:`ValidationError`'s
member order matches the Scala's exactly) -- this reproduces the oracle's
``.toList`` order directly, with no extra sort step needed at either the
library or the test call site. Downstream 5d.3 fixers that only need the
Scala's ``.isEmpty`` idiom can use ``not validate_lineup(...)`` against this
list identically to a set.

**``tidy_ctx`` is rebuilt per call, and NOT threaded across the loop
iterations within one call.** ``validate_lineup`` calls
:func:`~sportsdataverse.mbb.mbb_ncaa_names.build_tidy_player_context` fresh
every time (cheap; matches the Scala exactly, ``:195``) -- no caching
across calls. Within one call's inactive-players loop, every
:func:`~sportsdataverse.mbb.mbb_ncaa_names.tidy_player` call reads the SAME
``tidy_ctx`` object; the Scala's ``.collect { case ParseAnyPlay(player) if
... => ...tidy_player(player, tidy_ctx)._1... }`` never threads
``tidy_player``'s returned (cache-updated) context back into the next
iteration -- it always resolves against the one context built at ``:195``.
Ported verbatim: this module's loop never reassigns ``tidy_ctx``.

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of the ``ValidationError``/``allowedErrors``/
``validate_lineup`` portion of ``LineupErrorAnalysisUtils.scala`` from
`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_
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

Landmine index (reachable error sites, numbered across the module):
    1. None in :func:`validate_lineup` -- a pure sequence of comprehensions,
       an ``all()``, and list appends; no arithmetic, no indexing without a
       length guard, no division.
    2. :func:`clump_bad_lineups` indexes ``clumps[-1].evs[-1]`` and
       :func:`categorize_bad_lineups` indexes ``clump.evs[0]`` -- both
       provably safe, not a landmine in practice: every
       :class:`BadLineupClump` this module constructs is seeded with exactly
       one event (``BadLineupClump([lineup], next_good)``) and is only ever
       appended to afterward, so ``evs`` can never be empty for any clump
       either function encounters.
    3. :func:`find_missing_subs` and :func:`add_missing_players` read
       ``clump.evs[0]`` guarded by an explicit ``if clump.evs`` (empty ->
       ``[]`` candidates, mirroring the Scala ``headOption ... getOrElse(Nil)``);
       :func:`find_missing_subs`'s per-event head test ``ev == clump.evs[0]``
       is only reached once ``len(candidates) >= 6`` has already proven
       ``evs`` non-empty. No division, no unguarded indexing, and every
       ``PlayerCodeId`` membership test is a value-equality ``in`` / ``not
       in`` (see the module docstring's Set-of-``PlayerCodeId`` note). The
       Scala wraps nothing extra here; any unexpected exception propagates.

Example::

    from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
        ValidationError,
        validate_lineup,
        clump_bad_lineups,
    )

    errors = validate_lineup(lineup_event, box_lineup, valid_player_codes)
    if not errors:
        ...  # lineup is clean
    elif ValidationError.WRONG_NUMBER_OF_PLAYERS in errors:
        ...

    clumps = clump_bad_lineups([(bad_ev, None)])

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college
      basketball data in R.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Optional

from sportsdataverse.mbb.mbb_ncaa_events import parse_any_play
from sportsdataverse.mbb.mbb_ncaa_models import LineupEvent, PlayerCodeId
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context, tidy_player
from sportsdataverse.mbb.mbb_ncaa_stints import (
    build_lineup_id,
    build_new_player_list,
    build_player_code,
)

__all__ = [
    "ValidationError",
    "ALLOWED_ERRORS",
    "validate_lineup",
    "BadLineupClump",
    "clump_bad_lineups",
    "categorize_bad_lineups",
    "handle_common_sub_bug",
    "find_missing_subs",
    "add_missing_players",
    "analyze_and_fix_clumps",
]


class ValidationError(Enum):
    """The 3 ways a lineup can be declared invalid, in Scala declaration
    (ordinal) order (``LineupErrorAnalysisUtils.ValidationError``, ``:18-20``).
    Member order is load-bearing -- see the module docstring's "Return
    shape" note.
    """

    #: ``len(lineup.players) != 5`` (``:22``).
    WRONG_NUMBER_OF_PLAYERS = "WrongNumberOfPlayers"

    #: A player on the floor isn't in the box score (``:23``).
    UNKNOWN_PLAYERS = "UnknownPlayers"

    #: A player mentioned in a game event isn't on the floor (``:24``).
    INACTIVE_PLAYERS = "InactivePlayers"


#: The terminating filter set (``LineupErrorAnalysisUtils.allowedErrors``,
#: ``:21-26``) -- every :class:`ValidationError` is currently allowed; kept
#: as an explicit set (rather than an implicit "all of them") to mirror the
#: Scala's explicit listing and to give a single place to narrow the filter
#: later without touching :func:`validate_lineup`.
ALLOWED_ERRORS: frozenset[ValidationError] = frozenset(
    (
        ValidationError.WRONG_NUMBER_OF_PLAYERS,
        ValidationError.UNKNOWN_PLAYERS,
        ValidationError.INACTIVE_PLAYERS,
    )
)


def validate_lineup(
    lineup_event: LineupEvent,
    box_lineup: LineupEvent,
    valid_player_codes: set[str],
) -> list[ValidationError]:
    """Flags a lineup stint as internally inconsistent, via 3 independent
    checks (``LineupErrorAnalysisUtils.validate_lineup``, ``:181-218``).

    Args:
        lineup_event: The lineup stint to validate.
        box_lineup: The team's box-score lineup event (``players`` is the
            full roster) -- used both to build the name-resolution context
            (see :func:`~sportsdataverse.mbb.mbb_ncaa_names.build_tidy_player_context`)
            and, indirectly, as the source of ``players_out`` for
            jersey-number resolution inside :func:`~sportsdataverse.mbb
            .mbb_ncaa_names.tidy_player`.
        valid_player_codes: Every player code that's actually on the box
            score / roster for this team-season.

    Returns:
        The failing :class:`ValidationError`\\ s, in declaration order (see
        the module docstring's "Return shape" note) -- empty if
        ``lineup_event`` is clean.

        * :attr:`ValidationError.WRONG_NUMBER_OF_PLAYERS` -- ``lineup_event``
          doesn't have exactly 5 players on the floor.
        * :attr:`ValidationError.UNKNOWN_PLAYERS` -- some player on the
          floor isn't in ``valid_player_codes``.
        * :attr:`ValidationError.INACTIVE_PLAYERS` -- some player mentioned
          in ``lineup_event``'s own (team-side) raw game events resolves to
          a code not in ``valid_player_codes`` (i.e. isn't on the floor,
          per the lineup being validated).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stint_validation import validate_lineup
            errors = validate_lineup(lineup_event, box_lineup, {"MiMitchell", "BbBob"})
            assert not errors  # a clean lineup returns []
    """
    right_number_of_players = len(lineup_event.players) == 5
    all_players_known = all(p.code in valid_player_codes for p in lineup_event.players)

    # Players mentioned in game events who aren't in the lineup (``:195-208``).
    # `tidy_ctx` is built once and never reassigned below -- see the module
    # docstring's "tidy_ctx is rebuilt per call" note.
    tidy_ctx = build_tidy_player_context(box_lineup)
    inactive_players_mentioned: list[str] = []
    for raw in lineup_event.raw_game_events:
        if raw.team is None:
            continue
        player = parse_any_play(raw.team)
        if player is None or player.lower() == "team":
            continue
        resolved_name = tidy_player(player, tidy_ctx)[0]
        code = build_player_code(resolved_name, lineup_event.team.team).code
        if code not in valid_player_codes:
            inactive_players_mentioned.append(code)

    errors: list[ValidationError] = []
    if not right_number_of_players:
        errors.append(ValidationError.WRONG_NUMBER_OF_PLAYERS)
    if not all_players_known:
        errors.append(ValidationError.UNKNOWN_PLAYERS)
    if inactive_players_mentioned:
        errors.append(ValidationError.INACTIVE_PLAYERS)

    return [e for e in errors if e in ALLOWED_ERRORS]


@dataclass
class BadLineupClump:
    """A run of consecutive bad :class:`~sportsdataverse.mbb.mbb_ncaa_models
    .LineupEvent`\\ s that were merged together, plus the first following
    good event (``LineupErrorAnalysisUtils.BadLineupClump``, ``:223-226``).

    The Scala case class is ``protected`` (module-private), but this port
    exports it: the Task 5d.3 fixers and the (not-yet-ported) Task 5e
    orchestrator both consume ``BadLineupClump`` instances directly, so
    keeping it private here would just force every caller to reach past a
    leading underscore.

    Args:
        evs: The clumped lineup events, in chronological order.
        next_good: The first known-good lineup event following the clump,
            if any -- used by the Task 5d.3 fixers to reason about a
            player who should have subbed back in.
    """

    evs: list[LineupEvent]
    next_good: Optional[LineupEvent] = None


def clump_bad_lineups(
    lineup_events: list[tuple[LineupEvent, Optional[LineupEvent]]],
) -> list[BadLineupClump]:
    """Groups consecutive bad lineup events into :class:`BadLineupClump`\\ s
    (``LineupErrorAnalysisUtils.clump_bad_lineups``, ``:229-263``).

    The Scala original is a bespoke ``foldLeft`` (NOT the generic
    ``Clumper`` utility used elsewhere in the codebase) that prepends onto
    two nested lists -- the per-clump ``evs`` and the top-level clump list
    -- and reverses both at the end. This port walks the input once and
    appends directly (to the current clump's ``evs``, or a new clump to the
    result list), which produces the identical chronological order as the
    Scala's prepend-then-double-reverse without needing an explicit reverse
    step: mirroring a "prepend to the front, reverse at the end" fold as a
    plain "append to the back" loop is behavior-preserving precisely because
    reversing a prepend-built list restores insertion order.

    The current clump extends to cover the next ``(lineup, next_good)`` pair
    iff ALL 5 conditions hold, compared against the clump's LAST-ADDED event
    (``last``, not its first event) (``:242-249``):

    1. ``lineup.team == last.team``
    2. ``lineup.opponent == last.opponent``
    3. ``lineup.start_min == last.end_min`` (no time gap)
    4. ``len(lineup.players) == len(last.players)``
    5. ``len(lineup.players_in) == len(lineup.players_out)`` -- this checks
       the INCOMING lineup's own in/out balance, not a comparison against
       ``last`` (an unbalanced sub is a bad sign in isolation, per the
       Scala's own comment at ``:247``).

    ``TeamSeasonId`` (``lineup.team`` / ``.opponent``) is a plain (non-frozen)
    dataclass, so ``==`` is a field-wise value comparison out of the box --
    no ``PlayerCodeId``-unhashability workaround is needed here, since this
    predicate only compares team identities and player-list lengths, never a
    set of ``PlayerCodeId``.

    Each time a clump is extended, ``next_good`` is REPLACED with the
    incoming pair's own second element (``:251``) -- the final clump's
    ``next_good`` is always the LAST-extended event's ``next``, discarding
    whatever ``next_good`` an earlier extension set.

    Starting a new clump uses the incoming pair's own ``next`` too (``:234``,
    ``:253``) -- a fresh clump's ``next_good`` is never inherited from the
    clump before it.

    The Scala's third ``foldLeft`` case (``:255-259``, matching a head clump
    whose ``evs`` is empty) is dead code in practice -- every
    ``BadLineupClump`` this function ever constructs starts with exactly one
    event and is only ever appended to, so ``evs`` can never be empty. Omitted
    here with this comment in place of an unreachable branch.

    Args:
        lineup_events: ``(lineup_event, next_good_or_None)`` pairs, in
            chronological order.

    Returns:
        The clumps, in chronological order, each with ``evs`` in
        chronological order.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stint_validation import clump_bad_lineups
            clumps = clump_bad_lineups([(bad_ev, good_ev)])
            clumps[0].evs  # [bad_ev]
    """
    clumps: list[BadLineupClump] = []
    for lineup, next_good in lineup_events:
        if clumps:
            last = clumps[-1].evs[-1]
            if (
                lineup.team == last.team
                and lineup.opponent == last.opponent
                and lineup.start_min == last.end_min
                and len(lineup.players) == len(last.players)
                and len(lineup.players_in) == len(lineup.players_out)
            ):
                clumps[-1].evs.append(lineup)
                clumps[-1].next_good = next_good
                continue
        clumps.append(BadLineupClump([lineup], next_good))
    return clumps


def categorize_bad_lineups(lineup_events: list[LineupEvent]) -> dict[int, tuple[int, int]]:
    """Aggregates bad lineup events for display, by clump-leader player count
    (``LineupErrorAnalysisUtils.categorize_bad_lineups``, ``:617-633``,
    display-only -- the Scala doc comment says "can live without tests").

    Re-clumps ``lineup_events`` (each paired with ``next_good=None`` --
    :func:`clump_bad_lineups`'s grouping predicate never inspects
    ``next_good``, so this re-clumping is faithful to the Scala's own
    ``lineup_events.map(e => (e, None))``), then groups the resulting clumps
    by ``len(clump.evs[0].players)`` (the FIRST event's player count -- ``5``
    means a lineup with a bad *player*, not a bad *count*).

    Args:
        lineup_events: The bad lineup events to categorize, in chronological
            order.

    Returns:
        Player count -> ``(num_clumps, total_possessions)``, where
        ``total_possessions`` sums ``team_stats.num_possessions`` across
        every event in every clump in that group.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stint_validation import categorize_bad_lineups
            categorize_bad_lineups([bad_ev])  # {5: (1, bad_ev.team_stats.num_possessions)}
    """
    clumps = clump_bad_lineups([(e, None) for e in lineup_events])
    groups: dict[int, list[BadLineupClump]] = {}
    for clump in clumps:
        key = len(clump.evs[0].players) if clump.evs else 0
        groups.setdefault(key, []).append(clump)

    return {
        key: (
            len(group),
            sum(ev.team_stats.num_possessions for clump in group for ev in clump.evs),
        )
        for key, group in groups.items()
    }


def _distinct(players: list[PlayerCodeId]) -> list[PlayerCodeId]:
    """Order-preserving dedup by value equality -- the port of Scala's
    ``List#distinct`` and the size/membership half of ``.toSet`` on
    ``PlayerCodeId``.

    ``PlayerCodeId`` is unhashable in this port (see the module docstring's
    Set-of-``PlayerCodeId`` note), so this cannot delegate to
    ``dict.fromkeys`` / a ``set``. The ``p not in out`` test dispatches
    ``PlayerCodeId.__eq__`` (full-value equality), exactly the predicate
    Scala's ``distinct`` / ``Set`` dedup uses. ``O(n^2)`` in the worst case,
    but ``n`` is a single lineup's player count (<= ~15), so it is never hot.

    Args:
        players: The players to dedup, in the order to preserve.

    Returns:
        ``players`` with later value-duplicates dropped, first occurrence
        kept.
    """
    out: list[PlayerCodeId] = []
    for p in players:
        if p not in out:
            out.append(p)
    return out


def handle_common_sub_bug(
    clump: BadLineupClump,
    box_lineup: LineupEvent,
    valid_player_codes: set[str],
) -> tuple[list[LineupEvent], BadLineupClump]:
    """Fixes the "2-in-1-out then a compensating 1-out" substitution bug
    (``LineupErrorAnalysisUtils.handle_common_sub_bug``, ``:269-298``).

    Handles a **single-event** bad clump whose next known-good lineup carries
    a lone sub-out that the clump's event should have applied but didn't (e.g.
    ``IN: X, Y, Z; OUT: A, B`` in the bad event, then ``OUT: C`` in the good
    one). Fires only when all three guard conditions hold
    (``:275-278``):

    * the bad event has more players subbing IN than OUT
      (``len(players_in) > len(players_out)``),
    * the good event has **no** sub-ins (``len(good.players_in) == 0`` --
      otherwise there's no way to tell which of its sub-outs to borrow), and
    * the good event has at least one sub-out (``len(good.players_out) > 0``).

    The fix (``:279-283``) removes the good event's sub-outs from the bad
    event's on-floor ``players`` (value-equality ``not in`` -- the Scala's
    ``filterNot(good.players_out.toSet)``) and appends them to the bad
    event's ``players_out`` (order-preserving :func:`_distinct` -- the Scala's
    ``(bad.players_out ++ good.players_out).distinct``). The fix is **accepted
    only if** the result then passes :func:`validate_lineup` (``:284-294``):
    on success the fixed event is returned as the sole ``fixed`` lineup and
    the still-to-fix clump is emptied; on failure the *fixed* event (not the
    original) is returned as the still-to-fix clump, keeping the same
    ``next_good`` so a later pass can try again.

    Args:
        clump: The bad-lineup clump to attempt to repair.
        box_lineup: The team's box-score lineup event (roster + name context).
        valid_player_codes: Every player code on the box score / roster.

    Returns:
        ``(fixed_lineups, still_to_fix)`` -- ``fixed_lineups`` is ``[fixed]``
        on an accepted fix else ``[]``; ``still_to_fix`` is an empty clump on
        accept, the (unchanged) input clump on a guard miss, or the
        single-event *fixed*-but-still-invalid clump on a rejected fix.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
                handle_common_sub_bug,
            )
            fixed, still = handle_common_sub_bug(clump, box_lineup, valid_codes)
    """
    if len(clump.evs) != 1 or clump.next_good is None:
        return ([], clump)
    bad = clump.evs[0]
    good = clump.next_good
    if not (len(bad.players_in) > len(bad.players_out) and len(good.players_in) == 0 and len(good.players_out) > 0):
        return ([], clump)

    all_players = [p for p in bad.players if p not in good.players_out]
    fixed_lineup_ev = replace(
        bad,
        players_out=_distinct(bad.players_out + good.players_out),
        players=all_players,
    )
    if not validate_lineup(fixed_lineup_ev, box_lineup, valid_player_codes):
        return ([fixed_lineup_ev], BadLineupClump([], None))
    return ([], BadLineupClump([fixed_lineup_ev], good))


def find_missing_subs(
    clump: BadLineupClump,
    box_lineup: LineupEvent,
    valid_player_codes: set[str],
) -> tuple[list[LineupEvent], BadLineupClump]:
    """Trims a clump whose lineups carry TOO MANY players by identifying the
    "ghost" player(s) a missing sub-out left behind
    (``LineupErrorAnalysisUtils.find_missing_subs``, ``:406-514``).

    Fires only when the clump's first event has ``>= 6`` on-floor players
    (``:415-416``: ``candidates.size < 6`` is a no-op). ``expected_size_diff``
    (``:419``) is ``first_event_player_count - 5`` -- the number of ghosts the
    trim should end up removing.

    **Phase 1 -- shrink the candidate pool** (``:437-478``). Starting from the
    first event's players, walk the clump chronologically. At each event a
    candidate is *confirmed present* (and dropped from the pool) if it subs
    out (``ev.players_out``, **skipped for the first event** -- ``:445``,
    literal port of ``clump.evs.headOption.contains(ev)`` as value equality
    ``ev == clump.evs[0]``; for a well-formed clump of distinct events this is
    exactly ``index == 0``) or is named in one of the event's team-side raw
    plays (``parse_any_play`` -> :func:`~sportsdataverse.mbb.mbb_ncaa_names
    .tidy_player` -> :func:`~sportsdataverse.mbb.mbb_ncaa_stints
    .build_player_code`, ``:448-456``; unlike :func:`validate_lineup` this
    does NOT skip the literal ``"team"`` token -- ported verbatim).
    ``matching_index`` is the **FIRST** event index at which the pool size
    first equals ``expected_size_diff`` (``:475``); once set it freezes -- all
    later events are skipped in phase 1 (``:439-441``).

    **Accept gate** (``:479-480``): the final pool must be non-empty and no
    larger than ``expected_size_diff``. If ``matching_index`` never fired
    (the pool jumped past ``expected_size_diff`` in a single step, or never
    shrank to it), the gate still accepts iff the residual pool is a non-empty
    subset of size ``<= expected_size_diff`` -- in which case phase 3 routes
    **every** event through the "before match" branch (``index > None`` is
    always false). On failure the **original** clump is returned unchanged.

    **Phase 3 -- rebuild the events** (``:482-503``, a ``scanLeft`` ported as
    a manual accumulate loop that drops the seed). For events at/before
    ``matching_index`` the ghost pool is simply removed from ``players``
    (``filterNot``). For events strictly **after** ``matching_index``
    (``index > matching_index`` -- the matched event itself is "before")
    ``players`` is rebuilt from the previous *tidied* event via
    :func:`~sportsdataverse.mbb.mbb_ncaa_stints.build_new_player_list` (the
    ``scanLeft`` threads the previously-emitted event; its seed is ``None``,
    but the first event can never be an "after match" event, so the
    ``getOrElse(ev)`` fallback is only ever a formality -- ported faithfully
    all the same). The rebuilt events are partitioned by
    :func:`validate_lineup`.

    Args:
        clump: The bad-lineup clump to attempt to repair.
        box_lineup: The team's box-score lineup event (roster + name context).
        valid_player_codes: Every player code on the box score / roster.

    Returns:
        ``(fixed_lineups, still_to_fix)`` -- the now-valid rebuilt events and
        a clump of the still-invalid ones (carrying the input's ``next_good``);
        or ``([], clump)`` on a no-op / rejected fix.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
                find_missing_subs,
            )
            fixed, still = find_missing_subs(clump, box_lineup, valid_codes)
    """
    candidates = _distinct(clump.evs[0].players) if clump.evs else []
    if len(candidates) < 6:
        return ([], clump)

    expected_size_diff = len(candidates) - 5
    tidy_ctx = build_tidy_player_context(box_lineup)
    head = clump.evs[0]

    curr_candidates = candidates
    matching_index: Optional[int] = None
    for index, ev in enumerate(clump.evs):
        if matching_index is not None:
            # Phase 2: candidate already matched -- freeze and skip (``:439-441``).
            continue
        if ev == head:
            candidates_who_sub_out: list[PlayerCodeId] = []
        else:
            candidates_who_sub_out = [p for p in ev.players_out if p in curr_candidates]
        candidates_who_are_in_plays: list[PlayerCodeId] = []
        for raw in ev.raw_game_events:
            if raw.team is None:
                continue
            player = parse_any_play(raw.team)
            if player is None:
                continue
            code = build_player_code(tidy_player(player, tidy_ctx)[0], ev.team.team)
            if code in curr_candidates:
                candidates_who_are_in_plays.append(code)
        curr_candidates = [
            p for p in curr_candidates if p not in candidates_who_sub_out and p not in candidates_who_are_in_plays
        ]
        if len(curr_candidates) == expected_size_diff:
            matching_index = index

    filtered_candidates = curr_candidates
    if not (filtered_candidates and len(filtered_candidates) <= expected_size_diff):
        return ([], clump)

    tidied_evs: list[LineupEvent] = []
    last_ev: Optional[LineupEvent] = None
    for index, ev in enumerate(clump.evs):
        if matching_index is not None and index > matching_index:
            prev_ev = last_ev if last_ev is not None else ev
            new_ev = replace(ev, players=build_new_player_list(ev, prev_ev))
        else:
            new_ev = replace(ev, players=[p for p in ev.players if p not in filtered_candidates])
        tidied_evs.append(new_ev)
        last_ev = new_ev

    good_lineups: list[LineupEvent] = []
    bad_lineups: list[LineupEvent] = []
    for ev in tidied_evs:
        if validate_lineup(ev, box_lineup, valid_player_codes):
            bad_lineups.append(ev)
        else:
            good_lineups.append(ev)
    return (good_lineups, BadLineupClump(bad_lineups, clump.next_good))


def add_missing_players(
    clump: BadLineupClump,
    box_lineup: LineupEvent,
    valid_player_codes: set[str],
) -> tuple[list[LineupEvent], BadLineupClump]:
    """Back-fills a clump whose lineups carry TOO FEW players
    (``LineupErrorAnalysisUtils.add_missing_players``, ``:315-401``).

    Fires only when the clump's first event has ``<= 4`` on-floor players
    (``:324-325``: ``players_in.size > 4`` is a no-op). The candidate pool is
    every box-score player NOT already on the first event's floor
    (``:328``), **seeded** with a heuristic (``:352-357``): the ``next_good``
    lineup's sub-outs minus anyone appearing anywhere in the clump -- a good
    lineup that opens by subbing out a player who was never actually on the
    floor is a strong signal that player belongs to this under-filled clump.

    Walking the clump chronologically (``:359-385``): a candidate who subs IN
    is dropped from the pool (they're accounted for), and any remaining
    candidate named in a team-side raw play (same ``parse_any_play`` ->
    ``tidy_player`` -> ``build_player_code`` chain as
    :func:`find_missing_subs`) is collected into ``players_to_add``. If
    anything was collected, it is appended to **every** event's ``players``
    (raw list concat, no dedup -- ``:388-391``, a verbatim port; an over-add
    that pushes an event past 5 players lands it in the still-to-fix bucket
    for the second :func:`find_missing_subs` pass in
    :func:`analyze_and_fix_clumps` to trim back). The augmented events are
    partitioned by :func:`validate_lineup`. If nothing was collected, the
    original clump is returned unchanged.

    Args:
        clump: The bad-lineup clump to attempt to repair.
        box_lineup: The team's box-score lineup event (roster + name context).
        valid_player_codes: Every player code on the box score / roster.

    Returns:
        ``(fixed_lineups, still_to_fix)`` -- the now-valid augmented events and
        a clump of the still-invalid ones (carrying the input's ``next_good``);
        or ``([], clump)`` on a no-op / nothing-to-add outcome.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
                add_missing_players,
            )
            fixed, still = add_missing_players(clump, box_lineup, valid_codes)
    """
    players_in = _distinct(clump.evs[0].players) if clump.evs else []
    if len(players_in) > 4:
        return ([], clump)

    candidates = [p for p in _distinct(box_lineup.players) if p not in players_in]
    tidy_ctx = build_tidy_player_context(box_lineup)

    all_clump_players = _distinct([p for ev in clump.evs for p in ev.players])
    next_good_outs = clump.next_good.players_out if clump.next_good is not None else []
    initial_candidates = _distinct([p for p in next_good_outs if p not in all_clump_players])

    curr_candidates = candidates
    players_to_add = list(initial_candidates)
    for ev in clump.evs:
        new_candidates = [p for p in curr_candidates if p not in ev.players_in]
        for raw in ev.raw_game_events:
            if raw.team is None:
                continue
            player = parse_any_play(raw.team)
            if player is None:
                continue
            code = build_player_code(tidy_player(player, tidy_ctx)[0], ev.team.team)
            if code in new_candidates and code not in players_to_add:
                players_to_add.append(code)
        curr_candidates = new_candidates

    if not players_to_add:
        return ([], clump)

    good_lineups: list[LineupEvent] = []
    bad_lineups: list[LineupEvent] = []
    for ev in clump.evs:
        new_ev = replace(ev, players=ev.players + players_to_add)
        if validate_lineup(new_ev, box_lineup, valid_player_codes):
            bad_lineups.append(new_ev)
        else:
            good_lineups.append(new_ev)
    return (good_lineups, BadLineupClump(bad_lineups, clump.next_good))


def analyze_and_fix_clumps(
    clump: BadLineupClump,
    box_lineup: LineupEvent,
    valid_player_codes: set[str],
) -> tuple[list[LineupEvent], BadLineupClump]:
    """Runs the full self-healing fixer pipeline over one bad-lineup clump
    (``LineupErrorAnalysisUtils.analyze_and_fix_clumps``, ``:556-610``).

    The strict, order-dependent sequence (each stage threads
    ``(fixed_so_far + newly_fixed, still_to_fix)``):

    1. :func:`handle_common_sub_bug`,
    2. :func:`find_missing_subs`,
    3. :func:`add_missing_players`,
    4. :func:`find_missing_subs` **again** -- the Scala's own comment
       (``:587-588``) explains: "Try this again since add_missing_players can
       go too far". Step 3 back-fills onto every event and can push some past
       5 players; the second trim pass removes the over-add.

    Finally every accumulated ``fixed`` lineup gets a fresh ``lineup_id`` via
    :func:`~sportsdataverse.mbb.mbb_ncaa_stints.build_lineup_id` (``:597-605``)
    -- the fixers changed the on-floor ``players``, so the id computed during
    stint construction is stale. The Scala's ``debug``-gated
    ``analyze_unfixed_clumps`` call (``:593-596``) is dropped (see the module
    docstring's "Debug-only" note); it only prints.

    The Scala wraps the whole pipeline in ``Some(clump).map { ... }
    .getOrElse((Nil, clump))``, but ``Some(_)`` is never empty so the
    ``getOrElse`` is dead -- omitted here.

    Args:
        clump: The bad-lineup clump to repair.
        box_lineup: The team's box-score lineup event (roster + name context).
        valid_player_codes: Every player code on the box score / roster.

    Returns:
        ``(fixed_lineups, still_to_fix)`` -- every repaired lineup (with a
        recomputed ``lineup_id``) and whatever clump the pipeline could not
        fix.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
                analyze_and_fix_clumps,
            )
            fixed, still = analyze_and_fix_clumps(clump, box_lineup, valid_codes)
            for lineup in fixed:
                print(lineup.lineup_id.value)
    """
    fixed, to_fix = handle_common_sub_bug(clump, box_lineup, valid_player_codes)

    newly_fixed, to_fix = find_missing_subs(to_fix, box_lineup, valid_player_codes)
    fixed = fixed + newly_fixed

    newly_fixed, to_fix = add_missing_players(to_fix, box_lineup, valid_player_codes)
    fixed = fixed + newly_fixed

    # (Try find_missing_subs again since add_missing_players can go too far, ``:587-588``.)
    newly_fixed, to_fix = find_missing_subs(to_fix, box_lineup, valid_player_codes)
    fixed = fixed + newly_fixed

    fixed = [replace(fl, lineup_id=build_lineup_id(fl.players)) for fl in fixed]
    return (fixed, to_fix)
