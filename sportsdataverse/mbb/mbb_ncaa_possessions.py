"""NCAA possession calculator (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``)
``PossessionUtils.scala`` -- the third of four Phase-5a modules
(``mbb_ncaa_models.py`` / ``mbb_ncaa_events.py`` / ``mbb_ncaa_possessions.py``
/ wbb shims). **Task 5a.3 ports the possession calculator**: the
concurrent-event batching, the per-clump possession-fragment algorithm
(:func:`calculate_stats`), and the lineup-assignment / balancing / clamping
pass that turns raw NCAA play-by-play lines into per-:class:`~sportsdataverse
.mbb.mbb_ncaa_models.LineupEvent` possession counts.

**Concurrent-event batching is a plain manual loop, not the generic
``Clumper``/``StateUtils.foldLeft`` machinery.** The upstream
``StateUtils.foldLeft`` is a reusable state-transition fold that threads an
opaque clump-accumulator state through an arbitrary element stream, calling
back into a ``Clumper`` for *any* clumping predicate/merge pair. This port's
only clumper is ``Concurrency.concurrent_event_handler`` (the
:func:`concurrent_event_handler` here), so the generic machinery has exactly
one instantiation in the whole codebase -- ``# ponytail`` inlines its
behavior as a direct ``for``-loop over a running ``(last_min, last_date_str,
batch)`` triple instead of porting ``Clumper``/``TempState``/
``StateTransition`` as a reusable abstraction. If a later phase needs a
*second* clumping family sharing this batching, that's the signal to lift
this back into a small reusable clumper type; until then, one manual loop is
the lazy-and-correct choice (verified byte-for-byte against the oracle's
``concurrent_event_handler`` test, including the post-game-break singleton
quirk described below).

**The batching predicates** (``Concurrency.check_for_concurrent_event``,
``PossessionUtils.scala:71-93``), replicated as :func:`concurrent_event_handler`'s
loop body:

* The very first item of a fresh batch is always accepted (unconditionally
  merges), and seeds ``last_min``/``last_date_str`` from a raw-event item (a
  lineup-boundary marker seeds nothing -- see below).
* A **lineup-boundary marker** (``evs`` empty, ``lineups`` non-empty) is
  *always* absorbed into whatever the current batch is, regardless of
  ``last_min``/``last_date_str`` -- it never triggers a flush and never
  updates the clump state.
* A raw-event item whose ``date_str`` is greater (lexicographically) than
  the running ``last_date_str`` is a **game break**: flush the current
  batch, then start a new one seeded with ``last_min = -1.0`` (**not** the
  triggering event's own ``.min``) and ``last_date_str = ev.date_str``.
* Otherwise, a raw-event item whose ``.min`` equals the running
  ``last_min`` joins the current batch; any other raw-event item flushes
  the current batch and starts a new one seeded from itself.

**Port trap: the post-game-break singleton.** Because a game-break flush
resets ``last_min`` to the sentinel ``-1.0`` rather than the triggering
event's own minute, the *very next* raw event after a game break can never
satisfy ``ev.min == last_min`` (unless its minute happens to be exactly
``-1.0``, which never occurs in real data) -- so it always triggers its own
flush, becoming a forced singleton clump, before normal minute-based
batching resumes from the following event. This is a genuine quirk of the
upstream Scala (verified against the oracle's ``concurrent_event_handler``
test, event ``ev-7``), not a bug this port should "fix": the manual loop
reproduces it exactly because it mirrors the identical per-item state
update, not a reinterpretation of the intent.

Each merged clump is handed to :func:`calculate_stats` (once per direction,
Team and Opponent) to build a :class:`~sportsdataverse.mbb.mbb_ncaa_models.
PossCalcFragment`; :func:`calculate_possessions_by_event` accumulates those
fragments across clumps and, at each lineup boundary, calls
:func:`assign_to_right_lineup` (which composes :func:`lineup_balancer` then
:func:`lineup_fixer`) to attribute possessions to the just-completed
:class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent`\\ (s).

**Scala idiom decisions:**

* **Quicklens ``.modify(...).using(...)`` deep-updates become
  ``dataclasses.replace``-based pure-copy helpers**, not in-place mutation
  -- even though this port's dataclasses are otherwise deliberately mutable
  (see ``mbb_ncaa_models.py``'s idiom notes). Scala case classes are
  immutable, so every ``.modify`` call in the original produces a *new*
  ``LineupEvent``; the oracle test suite relies on this (the same ``val
  test_lineups`` fixture is fed into two separate assertions --
  ``"assign_to_right_lineup"`` and ``"calculate_possessions"`` -- and must
  come out unaffected by the first). Building fresh copies via
  :func:`dataclasses.replace` reproduces that non-destructive behavior
  faithfully and avoids test/caller cross-contamination that in-place
  mutation would introduce.
* **``shapeless``/``Poly1`` (:func:`~sportsdataverse.mbb.mbb_ncaa_models
  .poss_calc_fragment_sum`, Task 5a.1) and quicklens (here) are both
  generic-programming libraries with no Python equivalent** -- ported as
  plain functions operating on the concrete field shape, per the module's
  "port behavior, not machinery" mandate.
* ``PossState``/``ConcurrentClump`` are ported as plain mutable
  ``@dataclass``\\ es (matching the Scala ``case class`` field shape); their
  ``.init()``/constructor defaults build a *fresh* instance per call (same
  aliased-mutable-default rationale as ``ScoreInfo.empty``/
  ``LineupEventStats.empty`` in Task 5a.1).
* :func:`assign_to_right_lineup` **guards ``clump.lineups`` being empty**
  by returning ``[]`` immediately, rather than porting the Scala's
  ``case Nil => Nil`` pattern-match arm literally into :func:`lineup_balancer`.
  In the Scala, an empty ``clump.lineups`` reaching ``lineup_balancer``
  would fall into the *multi-lineup* branch (the single-element pattern
  ``head :: Nil`` doesn't match ``Nil``) and call ``.tracker.head`` on an
  empty list -- an unguarded ``NoSuchElementException`` if
  ``possessions_available > 0``. This is unreachable in practice: the only
  caller, :func:`calculate_possessions_by_event`, invokes
  :func:`assign_to_right_lineup` exclusively from the ``if clump.lineups:``
  branch. The explicit guard here makes that invariant visible in Python
  rather than relying on an upstream crash that never fires.

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of ``PossessionUtils.scala`` and ``StateUtils.scala``
(behavior only -- the generic ``Clumper``/``StateContext`` types are not
ported, see above) from
`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_
(package ``org.piggottfamily.cbb_explorer`` / ``org.piggottfamily.utils``),
which is licensed under the Apache License, Version 2.0 (the upstream repo's
``LICENSE`` file; full text at
`<http://www.apache.org/licenses/LICENSE-2.0>`_). Per Apache-2.0 Section 4's
redistribution-of-derivative-works obligations, sportsdataverse-py (itself
MIT-licensed) retains the upstream copyright notice for this derivative::

    Copyright (c) Alex-At-Home (https://github.com/Alex-At-Home) and
    contributors. Licensed under the Apache License, Version 2.0.

See ``THIRD_PARTY_NOTICES.md`` at the repository root for the full
third-party attribution entry -- Task 5a.4 adds cbb-explorer's entry there.

**Landmine index (reachable scalar division / indexing).** No division
exists in this module's scope -- every computation is integer counting,
string comparison, or list indexing. The one reachable "empty sequence"
risk (``lineup_balancer``'s ``tracker[0]`` when ``lineups`` is empty) is
guarded upstream in :func:`assign_to_right_lineup` (see the idiom-decision
note above); :func:`lineup_balancer` is never called directly with an empty
``lineups`` list by any function in this module.

Example::

    from sportsdataverse.mbb.mbb_ncaa_models import RawGameEvent
    from sportsdataverse.mbb.mbb_ncaa_possessions import (
        ConcurrentClump,
        calculate_possessions,
    )

    lineup.raw_game_events = [
        RawGameEvent.for_team("10:00,51-60,Eric Carter, 2pt layup made", min=5.0),
        RawGameEvent.for_opponent("09:58,51-62,Someone, 3pt jumpshot made", min=5.0),
    ]
    enriched = calculate_possessions([lineup])
    enriched[0].team_stats.num_possessions

See Also:
    * `cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_ -- the TypeScript sibling this Scala core feeds
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Iterator, Optional

from sportsdataverse.mbb.mbb_ncaa_events import (
    parse_deadball_rebound,
    parse_flagrant_foul,
    parse_free_throw_event,
    parse_free_throw_made,
    parse_free_throw_missed,
    parse_offensive_deadball_rebound,
    parse_offensive_event,
    parse_offensive_rebound,
    parse_shot_made,
    parse_shot_missed,
    parse_technical_foul,
    parse_turnover,
)
from sportsdataverse.mbb.mbb_ncaa_models import (
    Direction,
    LineupEvent,
    LineupEventStats,
    PossCalcFragment,
    PossessionEvent,
    RawGameEvent,
    poss_calc_fragment_sum,
    score_to_tuple,
)

__all__ = [
    "ConcurrentClump",
    "PossState",
    "lineup_as_raw_clumps",
    "concurrent_event_handler",
    "count_matching",
    "calculate_stats",
    "calculate_possessions_by_event",
    "calculate_possessions",
    "lineup_balancer",
    "lineup_fixer",
    "assign_to_right_lineup",
]

DirFn = Callable[[RawGameEvent], Optional[str]]
"""Type alias for :meth:`~sportsdataverse.mbb.mbb_ncaa_models.PossessionEvent
.attacking_team` / ``.defending_team`` -- the ``side`` accessor passed to
:func:`count_matching`."""

Parser = Callable[[str], Optional[Any]]
"""Type alias for a ``parse_x`` extractor from ``mbb_ncaa_events.py``."""


@dataclass
class ConcurrentClump:
    """A clump of concurrent raw events, together with the lineups that end
    in that clump (``Concurrency.ConcurrentClump``, ``PossessionUtils.scala
    :64-69``).

    Args:
        evs: The raw game events in this clump, in chronological order.
        lineups: The lineups (if any) whose ``end_min`` falls in this clump.
    """

    evs: list[RawGameEvent] = field(default_factory=list)
    lineups: list[LineupEvent] = field(default_factory=list)

    @property
    def min(self) -> Optional[float]:
        """The clump's minute, from its first event, or ``None`` if empty."""
        return self.evs[0].min if self.evs else None

    @property
    def date_str(self) -> Optional[str]:
        """The clump's date string, from its first event, or ``None`` if
        empty."""
        return self.evs[0].date_str if self.evs else None


@dataclass
class PossState:
    """Running state threaded through :func:`calculate_possessions_by_event`
    (``PossessionUtils.PossState``, ``PossessionUtils.scala:39-49``).

    Args:
        team_stats: Accumulated fragment for the team since the last lineup
            boundary.
        opponent_stats: Accumulated fragment for the opponent since the last
            lineup boundary.
        prev_clump: The previously-processed merged clump (used by
            :func:`calculate_stats`'s and-one / deadball-rebound heuristics).
    """

    team_stats: PossCalcFragment
    opponent_stats: PossCalcFragment
    prev_clump: ConcurrentClump

    @classmethod
    def init(cls) -> "PossState":
        """A fresh starting state (``PossState.init``, ``:45-48``) -- builds
        new instances per call, matching the mutable-dataclass rationale in
        ``mbb_ncaa_models.py``'s ``.empty()`` factories."""
        return cls(PossCalcFragment(), PossCalcFragment(), ConcurrentClump())


def lineup_as_raw_clumps(lineup: LineupEvent) -> Iterator[ConcurrentClump]:
    """Turn one lineup's raw events into unprocessed singleton clumps, plus a
    trailing lineup-boundary marker (``Concurrency.lineup_as_raw_clumps``,
    ``PossessionUtils.scala:114-120``).

    Args:
        lineup: The lineup event to expand.

    Yields:
        One ``ConcurrentClump([ev])`` per raw event (in order), then a
        final ``ConcurrentClump([], [lineup])`` boundary marker.
    """
    for ev in lineup.raw_game_events:
        yield ConcurrentClump([ev])
    yield ConcurrentClump([], [lineup])


def concurrent_event_handler(clumps: Iterable[ConcurrentClump]) -> list[ConcurrentClump]:
    """Batch a stream of singleton/boundary clumps into merged
    concurrent-event clumps (``Concurrency.concurrent_event_handler`` +
    ``StateUtils.foldLeft``'s clumping machinery, ``PossessionUtils.scala
    :71-111`` -- see the module docstring for the full batching-predicate
    breakdown and the post-game-break singleton port trap).

    # ponytail: manual accumulate-and-flush loop replacing the generic
    # Clumper/StateUtils.foldLeft abstraction -- this is the ONE clumper
    # instantiation in the port, so a reusable abstraction buys nothing.
    # Lift this back into a small clumper type if a second concurrent-event
    # family needs the same batching later.

    Args:
        clumps: An ordered stream of ``ConcurrentClump``\\ s, each either a
            singleton raw event (``evs=[ev]``) or a lineup-boundary marker
            (``evs=[]``, ``lineups=[lineup]``), e.g. from
            :func:`lineup_as_raw_clumps`.

    Returns:
        The merged clumps, each an in-order concatenation of one batch's
        ``evs``/``lineups``.
    """

    def merge(batch: list[ConcurrentClump]) -> ConcurrentClump:
        evs: list[RawGameEvent] = []
        lineups: list[LineupEvent] = []
        for item in batch:
            evs.extend(item.evs)
            lineups.extend(item.lineups)
        return ConcurrentClump(evs, lineups)

    last_min = -1.0
    last_date_str = ""
    batch: list[ConcurrentClump] = []
    result: list[ConcurrentClump] = []

    for item in clumps:
        if item.evs:
            ev = item.evs[0]
            if not batch:
                last_min, last_date_str = ev.min, ev.date_str
                batch.append(item)
            elif ev.date_str > last_date_str:
                # Game break -- flush, then seed the sentinel -1.0 (not
                # ev.min) per the Scala; see the module docstring's
                # "post-game-break singleton" port trap.
                result.append(merge(batch))
                last_min, last_date_str = -1.0, ev.date_str
                batch = [item]
            elif ev.min == last_min:
                batch.append(item)
            else:
                result.append(merge(batch))
                last_min, last_date_str = ev.min, ev.date_str
                batch = [item]
        else:
            # Lineup-boundary marker -- always absorbed, state unchanged.
            batch.append(item)

    if batch:
        result.append(merge(batch))
    return result


def _matches(ev: RawGameEvent, side: DirFn, *parsers: Parser) -> bool:
    """``True`` if ``side(ev)`` is set and any ``parsers`` extracts from it.

    Ports the ``case side(ParseX(_)) => ...`` collect-guard idiom shared by
    every branch of :func:`calculate_stats`.
    """
    s = side(ev)
    return s is not None and any(parser(s) is not None for parser in parsers)


def count_matching(evs: Iterable[RawGameEvent], side: DirFn, *parsers: Parser) -> int:
    """Count events on one side matching any of the given parsers.

    Ports the pervasive ``clump.evs.collect { case side(ParseX(_)) => () }
    .size`` idiom (and its multi-arm ``case side(ParseX(_)) => ();
    case side(ParseY(_)) => ()`` union form, when more than one parser is
    passed -- e.g. the and-one free-throw count, which matches *either* a
    made or a missed free throw on the same event).

    Args:
        evs: The events to scan.
        side: :meth:`~sportsdataverse.mbb.mbb_ncaa_models.PossessionEvent
            .attacking_team` or ``.defending_team``, selecting which raw
            string (if any) to test per event.
        *parsers: One or more ``parse_x`` extractors from
            ``mbb_ncaa_events.py``; an event counts if ``side(ev)`` is set
            and *any* parser matches it.

    Returns:
        The count of matching events.
    """
    return sum(1 for ev in evs if _matches(ev, side, *parsers))


def calculate_stats(clump: ConcurrentClump, prev: ConcurrentClump, dir: Direction) -> PossCalcFragment:
    """Calculate one direction's possession-fragment for one merged clump
    (``PossessionUtils.calculate_stats``, ``PossessionUtils.scala:170-369``).

    See the upstream source's inline worked examples (and-one detection,
    technical/flagrant offsetting, the deadball-rebound heuristic) for the
    hand-annotated NCAA play-by-play snippets that motivate each step; this
    port reproduces every step in the same order.

    Args:
        dir: Which side (``Direction.TEAM``/``Direction.OPPONENT``) is
            "attacking" for this calculation. Named to match the Scala
            (shadows the ``dir`` builtin -- consistent with this port's
            existing precedent of naming params after their Scala originals,
            e.g. ``RawGameEvent.for_team``'s ``min``).
        clump: The merged clump to score.
        prev: The previously-processed merged clump (feeds the and-one and
            deadball-rebound heuristics -- see below).

    Returns:
        A :class:`~sportsdataverse.mbb.mbb_ncaa_models.PossCalcFragment` for
        this clump/direction.
    """
    poss_event = PossessionEvent(dir)
    attacking = poss_event.attacking_team
    defending = poss_event.defending_team

    ft_event_this_clump = count_matching(clump.evs, attacking, parse_free_throw_event) > 0

    and_one_ft_count = count_matching(clump.evs, attacking, parse_free_throw_made, parse_free_throw_missed)
    clump_has_made_shot = count_matching(clump.evs, attacking, parse_shot_made) > 0
    prev_has_made_shot = count_matching(prev.evs, attacking, parse_shot_made) > 0
    prev_has_defending_offensive_event = count_matching(prev.evs, defending, parse_offensive_event) > 0
    and_one = (
        1
        if (
            and_one_ft_count == 1
            and (clump_has_made_shot or (prev_has_made_shot and not prev_has_defending_offensive_event))
        )
        else 0
    )

    filtered_clump = [ev for ev in clump.evs if not _matches(ev, attacking, parse_deadball_rebound)]

    shots_made_or_missed = count_matching(filtered_clump, attacking, parse_shot_made, parse_shot_missed)

    ft_event = 1 if (ft_event_this_clump and and_one == 0) else 0

    offsetting_tech = (
        1
        if (
            count_matching(filtered_clump, attacking, parse_technical_foul) > 0
            and count_matching(filtered_clump, defending, parse_technical_foul) > 0
        )
        else 0
    )
    offsetting_flagrant = (
        1
        if (
            count_matching(filtered_clump, attacking, parse_flagrant_foul) > 0
            and count_matching(filtered_clump, defending, parse_flagrant_foul) > 0
        )
        else 0
    )
    offsetting_tech_or_flagrant = 1 if (offsetting_tech + offsetting_flagrant > 0) else 0

    tech_or_flagrant = (
        1 if count_matching(filtered_clump, defending, parse_technical_foul, parse_flagrant_foul) > 0 else 0
    ) - offsetting_tech_or_flagrant

    orbs = count_matching(filtered_clump, attacking, parse_offensive_rebound)

    # recent_dead_ft_misses: combine prev + current clump's attacking FT
    # make/miss events, sort by score ascending then reverse (highest
    # score first), drop the highest, count misses among the rest.
    combined_evs = list(prev.evs) + list(clump.evs)
    ft_evs = [ev for ev in combined_evs if _matches(ev, attacking, parse_free_throw_made, parse_free_throw_missed)]
    ft_evs_desc = list(reversed(sorted(ft_evs, key=lambda ev: score_to_tuple(ev.score_str))))
    recent_dead_ft_misses = sum(1 for ev in ft_evs_desc[1:] if _matches(ev, attacking, parse_free_throw_missed))

    if recent_dead_ft_misses == 0 and tech_or_flagrant == 0:
        real_deadball_orbs = sum(
            1
            for ev in clump.evs
            if _matches(ev, attacking, parse_offensive_deadball_rebound) and not ev.info.startswith("00:00")
        )
    else:
        real_deadball_orbs = 0

    turnovers = count_matching(filtered_clump, attacking, parse_turnover)

    return PossCalcFragment(
        shots_made_or_missed,
        orbs,
        real_deadball_orbs,
        ft_event,
        and_one,
        tech_or_flagrant,
        offsetting_tech_or_flagrant,
        turnovers,
    )


def calculate_possessions_by_event(raw_events_as_clumps: Iterable[ConcurrentClump]) -> list[LineupEvent]:
    """Drive the batch loop + per-clump scoring over an already-flattened
    clump stream (``PossessionUtils.calculate_possessions_by_event``,
    ``PossessionUtils.scala:521-573``).

    Args:
        raw_events_as_clumps: The unbatched clump stream, e.g. from
            flat-mapping :func:`lineup_as_raw_clumps` over several lineups.

    Returns:
        The lineups, each enriched with possession counts, in original
        order.
    """
    state = PossState.init()
    output: list[LineupEvent] = []

    for clump in concurrent_event_handler(raw_events_as_clumps):
        team_stats = calculate_stats(clump, state.prev_clump, Direction.TEAM)
        opponent_stats = calculate_stats(clump, state.prev_clump, Direction.OPPONENT)

        if not clump.lineups:
            state = PossState(
                team_stats=poss_calc_fragment_sum(state.team_stats, team_stats),
                opponent_stats=poss_calc_fragment_sum(state.opponent_stats, opponent_stats),
                prev_clump=clump,
            )
        else:
            output.extend(assign_to_right_lineup(state, team_stats, opponent_stats, clump, state.prev_clump))
            state = PossState.init()
            state.prev_clump = clump

    return output


def calculate_possessions(lineup_events: Iterable[LineupEvent]) -> list[LineupEvent]:
    """Top-level entry point: calculate team/opponent possessions for a
    sequence of lineup events (``PossessionUtils.calculate_possessions``,
    ``PossessionUtils.scala:371-379``).

    Args:
        lineup_events: The lineups to enrich, in chronological order.

    Returns:
        The lineups, each enriched with possession counts.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_possessions import calculate_possessions

            enriched = calculate_possessions(lineups)
            enriched[0].team_stats.num_possessions
    """

    def flattened() -> Iterator[ConcurrentClump]:
        for lineup in lineup_events:
            yield from lineup_as_raw_clumps(lineup)

    return calculate_possessions_by_event(list(flattened()))


def _add_num_possessions(stats: LineupEventStats, delta: int) -> LineupEventStats:
    """Pure-copy ``+= delta`` on ``num_possessions`` (quicklens
    ``.modify(_.num_possessions).using(_ + delta)`` -> ``dataclasses.replace``)."""
    return replace(stats, num_possessions=stats.num_possessions + delta)


def _add_team_poss(lineup: LineupEvent, delta: int) -> LineupEvent:
    """Pure-copy ``+= delta`` on ``lineup.team_stats.num_possessions``."""
    return replace(lineup, team_stats=_add_num_possessions(lineup.team_stats, delta))


def _add_opponent_poss(lineup: LineupEvent, delta: int) -> LineupEvent:
    """Pure-copy ``+= delta`` on ``lineup.opponent_stats.num_possessions``."""
    return replace(lineup, opponent_stats=_add_num_possessions(lineup.opponent_stats, delta))


def lineup_balancer(
    lineups: list[LineupEvent],
    team_stats: PossCalcFragment,
    opponent_stats: PossCalcFragment,
    clump: ConcurrentClump,
    prev_clump: ConcurrentClump,
) -> list[LineupEvent]:
    """Attribute this clump's possessions to the candidate lineup(s)
    (``PossessionUtils.assign_to_right_lineup.lineup_balancer``,
    ``PossessionUtils.scala:429-471``).

    A single candidate just receives the whole clump's possessions. Multiple
    candidates (a lineup change landing mid-clump) are split via a greedy
    round-robin: for each direction, rank lineups by an "approximate" possession
    count computed from just that lineup's own raw events at the clump's
    minute, then hand out possessions one at a time to whichever lineup
    currently has the highest remaining approximate share.

    Args:
        lineups: The candidate lineups (already updated with any running
            state total from :func:`assign_to_right_lineup`).
        team_stats: This clump's team-direction fragment.
        opponent_stats: This clump's opponent-direction fragment.
        clump: The merged clump being assigned.
        prev_clump: The previous merged clump (only used for the first
            candidate's approximate stats -- see below).

    Returns:
        New lineup copies with ``num_possessions`` incremented.
    """
    if len(lineups) == 1:
        lineup = _add_team_poss(lineups[0], team_stats.total_poss)
        lineup = _add_opponent_poss(lineup, opponent_stats.total_poss)
        return [lineup]

    min_of_interest = clump.min if clump.min is not None else -1.0
    balancer_by_dir: dict[Direction, dict[int, int]] = {}

    for dir_ in (Direction.TEAM, Direction.OPPONENT):
        # (-approx_total_poss, index) so ascending sort ranks the highest
        # approximate possession count first.
        tracker: list[list[int]] = []
        for index, lineup in enumerate(lineups):
            # Only the first candidate inherits the real prev_clump -- the
            # and-one heuristic doesn't carry across a lineup split.
            prev_for_index = prev_clump if index == 0 else ConcurrentClump()
            events_of_interest = ConcurrentClump([ev for ev in lineup.raw_game_events if ev.min == min_of_interest])
            approx_stats = calculate_stats(events_of_interest, prev_for_index, dir_)
            tracker.append([-approx_stats.total_poss, index])
        tracker.sort(key=lambda t: t[0])

        possessions_available = team_stats.total_poss if dir_ == Direction.TEAM else opponent_stats.total_poss
        balancer: dict[int, int] = {}
        for _ in range(possessions_available):
            lineup_to_add = tracker[0][1]
            balancer[lineup_to_add] = balancer.get(lineup_to_add, 0) + 1
            tracker[0][0] += 1
            tracker.sort(key=lambda t: t[0])
        balancer_by_dir[dir_] = balancer

    result = []
    for index, lineup in enumerate(lineups):
        updated = _add_team_poss(lineup, balancer_by_dir[Direction.TEAM].get(index, 0))
        updated = _add_opponent_poss(updated, balancer_by_dir[Direction.OPPONENT].get(index, 0))
        result.append(updated)
    return result


def lineup_fixer(lineups: list[LineupEvent]) -> list[LineupEvent]:
    """Clamp obviously-broken possession counts (``PossessionUtils
    .assign_to_right_lineup.lineup_fixer``, ``PossessionUtils.scala:490-507``).

    For both ``team_stats`` and ``opponent_stats`` independently: a lineup
    that scored (``pts > 0``) but was attributed zero-or-fewer possessions
    is clamped to exactly 1 (you can't score on zero possessions); any
    still-negative possession count is clamped to 0.

    Args:
        lineups: The lineups to fix (already balanced).

    Returns:
        New lineup copies with clamped ``num_possessions``.
    """

    def fix(stats: LineupEventStats) -> LineupEventStats:
        if stats.pts > 0 and stats.num_possessions <= 0:
            return replace(stats, num_possessions=1)
        if stats.num_possessions < 0:
            return replace(stats, num_possessions=0)
        return stats

    return [
        replace(lineup, team_stats=fix(lineup.team_stats), opponent_stats=fix(lineup.opponent_stats))
        for lineup in lineups
    ]


def assign_to_right_lineup(
    state: PossState,
    team_stats: PossCalcFragment,
    opponent_stats: PossCalcFragment,
    clump: ConcurrentClump,
    prev_clump: ConcurrentClump,
) -> list[LineupEvent]:
    """Assign a clump's possessions to the lineup(s) ending in it
    (``PossessionUtils.assign_to_right_lineup``, ``PossessionUtils.scala
    :418-518``).

    Applies the running ``state`` total (accumulated since the last lineup
    boundary) to the *first* ending lineup only, then hands off to
    :func:`lineup_balancer` (this clump's own fragment, split across
    candidates if there's more than one) and finally :func:`lineup_fixer`
    (the negative-possession clamp).

    Args:
        state: The running possession state since the last lineup boundary.
        team_stats: This clump's team-direction fragment.
        opponent_stats: This clump's opponent-direction fragment.
        clump: The merged clump ending one or more lineups.
        prev_clump: The previous merged clump.

    Returns:
        The lineup(s) ending in this clump, enriched with possession
        counts. Empty if ``clump.lineups`` is empty (see the module
        docstring's landmine-index note -- unreachable via
        :func:`calculate_possessions_by_event`).
    """
    if not clump.lineups:
        return []

    head = _add_team_poss(clump.lineups[0], state.team_stats.total_poss)
    head = _add_opponent_poss(head, state.opponent_stats.total_poss)
    working = [head, *clump.lineups[1:]]

    balanced = lineup_balancer(working, team_stats, opponent_stats, clump, prev_clump)
    return lineup_fixer(balanced)
