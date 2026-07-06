"""NCAA lineup enrichment / stat-tree population (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``)
``LineupUtils.scala`` -- the first of five Phase-5c modules. **Task 5c.1
ports the lineup-enrichment core**: :func:`enrich_lineup` (score-delta pts/
plus_minus), :func:`fix_possible_score_swap_bug`, :func:`ensure_ev_uniqueness`,
:func:`add_stats_to_lineups`, and the full :func:`enrich_stats` event
dispatch -- but with a **total-only** shot-clock selector (see "The dispatch
seam" below). The scramble (:func:`~sportsdataverse.mbb.mbb_ncaa_lineup_enrich
.is_scramble`, Task 5c.2) and transition (``is_transition``, Task 5c.3)
heuristics that tag events ``early``/``orb`` are NOT ported yet.

**THE critical port fact.** ``ShotClockStats.mid``/``.late`` are **dead
fields -- never populated** anywhere in ``LineupUtils.scala``. Only three
segments are ever written: ``total`` (always), ``early`` (the
``is_transition`` heuristic, Task 5c.3), and ``orb`` (the ``is_scramble``
heuristic, Task 5c.2). These are PLAY-TYPE heuristics, not shot-clock timer
derivations -- there is no game-clock arithmetic anywhere in this file. Only
shots/FTs/TOs/assists are ever eligible for ``early``/``orb`` tagging;
rebounds/steals/blocks/fouls are permanently total-only (``LineupUtils.scala
:1332-1433`` never wraps those branches' ``implicit`` selector in
``shot_clock_selector_builder``, using the static ``basic_shotclock_selector``
instead).

**The dispatch seam (5c.2/5c.3).** :func:`_shot_clock_selector_builder`
mirrors the Scala's ``shot_clock_selector_builder`` closure
(``:996-1004``) -- it returns the list of segment names to increment for one
event. For 5c.1 it unconditionally returns ``["total"]``; Task 5c.3 will
extend it to additionally consult the (not-yet-ported) ``is_scramble``/
``is_transition`` builders and append ``"orb"``/``"early"`` the same way the
Scala appends ``selector_shotclock_scramble``/``selector_shotclock_transition``
on top of the base ``[selector_shotclock_total]`` list. The always-total-only
branches (ORB/DRB/STL/BLK/foul) use the module-level :data:`_BASE_SELECTORS`
constant directly and are UNAFFECTED by that future extension -- exactly per
the plan's "only shots/FTs/TOs/assists get transition/scramble tagging" fact.

**Deferred to Task 5c.4 (not stubbed).** ``increment_player_3p_shot_info``
(``LineupUtils.scala:1147-1178``) buckets a 3pt shot into the shooter's
per-lineup-slot :class:`~sportsdataverse.mbb.mbb_ncaa_models.PlayerShotInfo`
tuple, but it is a no-op unless ``player_index`` is in ``[0, 4]`` -- and no
caller passes a non-default ``player_index`` before Task 5c.4 ports
``create_player_events`` (the only call site that ever sets it). Per YAGNI,
this port omits that helper entirely rather than shipping an
unreachable-until-5c.4 stub; the 3pt made/missed dispatch branches below
carry an inline comment marking exactly where 5c.4 wires it back in.

**Scala idiom decisions.**

* :func:`enrich_lineup` / :func:`add_stats_to_lineups` /
  :func:`fix_possible_score_swap_bug` build fresh copies via
  :func:`dataclasses.replace` rather than mutating their ``lineup`` argument
  in place -- for the same reason Task 5a.3 chose pure-copy helpers over
  in-place mutation: test fixtures (and, in production, the same
  :class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` list) get reused
  across multiple assertions/calls, and an in-place mutation would leak state
  between them. :func:`enrich_stats` reinforces this at its own boundary: it
  ``copy.deepcopy``'s its ``stats`` argument exactly once before doing any
  work, so a caller's shared ``LineupEventStats`` literal (e.g. a test's
  ``zero_stats`` fixture, reused as the starting point for many separate
  calls) is never mutated by a call that reads from it.
* **Quicklens ``.atOrElse(default)`` becomes plain ``getattr``/``setattr``
  get-or-create helpers** (:func:`_get_or_create_shot_clock` /
  :func:`_get_or_create_assist_info` / :func:`_get_or_create_assist_list`),
  since Python has no lens library and this port's stat-tree dataclasses are
  mutable by convention -- once :func:`enrich_stats` has taken its one
  defensive deep copy, mutating the private working tree directly is simpler
  than porting quicklens' ``PathLazyModify`` machinery.
* **``AssistEvent`` list increment-or-prepend** (:func:`_increment_player_assist`,
  ``increment_player_assist``, ``:1040-1060``) mutates the list and its
  matching entry's counts in place rather than rebuilding via ``::``/``.map``
  -- behaviorally identical (at most one ``AssistEvent`` per distinct
  ``player_code`` ever exists), simpler in a mutable-by-convention port.

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of ``LineupUtils.scala`` from
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
third-party attribution entry -- Task 5c.5 extends the existing cbb-explorer
entry to cover ``LineupUtils.scala``.

**Landmine index (reachable scalar division).** None. Every computation in
this module's scope is integer counting, dict/list-shaped mutation, or plain
string/regex matching (via the already-ported ``mbb_ncaa_events`` parsers) --
no division by a runtime-derived value exists.

Example::

    from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import enrich_lineup

    enriched = enrich_lineup(lineup)
    enriched.team_stats.pts
    enriched.team_stats.fg.made.total

See Also:
    * `cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_ -- the TypeScript sibling this Scala core feeds
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
"""

from __future__ import annotations

import copy
from dataclasses import replace
from typing import Any, Callable, Optional

from sportsdataverse.mbb.mbb_ncaa_events import (
    parse_assist,
    parse_defensive_rebound,
    parse_flagrant_foul,
    parse_free_throw_made,
    parse_free_throw_missed,
    parse_offensive_deadball_rebound,
    parse_offensive_foul,
    parse_offensive_rebound,
    parse_personal_foul,
    parse_rim_made,
    parse_rim_missed,
    parse_shot_blocked,
    parse_stolen,
    parse_technical_foul,
    parse_three_pointer_made,
    parse_three_pointer_missed,
    parse_turnover,
    parse_two_pointer_made,
    parse_two_pointer_missed,
)
from sportsdataverse.mbb.mbb_ncaa_models import (
    AssistEvent,
    AssistInfo,
    Direction,
    LineupEvent,
    LineupEventStats,
    PossessionEvent,
    RawGameEvent,
    Score,
    ScoreInfo,
    ShotClockStats,
)
from sportsdataverse.mbb.mbb_ncaa_possessions import (
    ConcurrentClump,
    concurrent_event_handler,
    lineup_as_raw_clumps,
)

__all__ = [
    "enrich_lineup",
    "add_stats_to_lineups",
    "fix_possible_score_swap_bug",
    "enrich_stats",
    "ensure_ev_uniqueness",
]

PlayerFilterCoder = Callable[[str], "tuple[bool, str]"]
"""Type alias for the ``player_filter_coder`` argument -- given a raw
play-by-play name, returns ``(is_this_player, this_players_code)``."""


def enrich_lineup(lineup: LineupEvent) -> LineupEvent:
    """Populate ``pts``/``plus_minus`` from the score delta, then run the
    full stat-tree enrichment (``enrich_lineup``, ``LineupUtils.scala:29-46``).

    Args:
        lineup: The lineup event to enrich (not mutated -- see the module
            docstring's "Scala idiom decisions").

    Returns:
        A new :class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` with
        ``team_stats``/``opponent_stats`` fully populated.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import enrich_lineup

            enriched = enrich_lineup(lineup)
            enriched.team_stats.pts
    """
    scored = lineup.score_info.end.scored - lineup.score_info.start.scored
    allowed = lineup.score_info.end.allowed - lineup.score_info.start.allowed
    updated = replace(
        lineup,
        team_stats=replace(lineup.team_stats, pts=scored, plus_minus=scored - allowed),
        opponent_stats=replace(lineup.opponent_stats, pts=allowed, plus_minus=allowed - scored),
    )
    return add_stats_to_lineups(updated)


def fix_possible_score_swap_bug(lineup: list[LineupEvent], box_lineup: LineupEvent) -> list[LineupEvent]:
    """Undo a rare NCAA data bug where the scores get transposed
    (``fix_possible_score_swap_bug``, ``LineupUtils.scala:51-90``).

    If the last lineup's ending score is the exact transpose of the box
    score's ending score, every lineup's ``score_info`` is un-transposed and
    ``pts``/``plus_minus`` are swapped/negated between ``team_stats`` and
    ``opponent_stats`` -- nothing else in the stat trees changes.

    Args:
        lineup: The lineups to (maybe) fix, in chronological order.
        box_lineup: The trusted box-score lineup to compare the final score
            against.

    Returns:
        ``lineup`` unchanged if the scores aren't transposed (or ``lineup``
        is empty); otherwise a new list with every entry's score/pts/
        plus_minus corrected.
    """

    def scores_reversed(g1: Score, g2: Score) -> bool:
        return g1.scored == g2.allowed and g2.scored == g1.allowed

    if not lineup:
        return lineup
    if not scores_reversed(lineup[-1].score_info.end, box_lineup.score_info.end):
        return lineup

    def reverse_score(score: Score) -> Score:
        return Score(scored=score.allowed, allowed=score.scored)

    def reverse_score_info(info: ScoreInfo) -> ScoreInfo:
        return ScoreInfo(
            start=reverse_score(info.start),
            end=reverse_score(info.end),
            start_diff=-info.start_diff,
            end_diff=-info.end_diff,
        )

    fixed: list[LineupEvent] = []
    for x in lineup:
        team_pts = x.team_stats.pts
        opponent_pts = x.opponent_stats.pts
        fixed.append(
            replace(
                x,
                score_info=reverse_score_info(x.score_info),
                team_stats=replace(x.team_stats, pts=opponent_pts, plus_minus=-x.team_stats.plus_minus),
                opponent_stats=replace(x.opponent_stats, pts=team_pts, plus_minus=-x.opponent_stats.plus_minus),
            )
        )
    return fixed


def ensure_ev_uniqueness(clump: ConcurrentClump) -> ConcurrentClump:
    """Nudge each event's ``min`` by a tiny per-index delta so truly
    concurrent (identical-``min``) events within a clump don't collapse
    under ``==`` (``ensure_ev_uniqueness``, ``LineupUtils.scala:105-111``).

    Args:
        clump: The clump whose events to nudge.

    Returns:
        A new :class:`~sportsdataverse.mbb.mbb_ncaa_possessions.ConcurrentClump`
        with each event's ``min`` incremented by ``1e-6 * index``.
    """
    return replace(
        clump,
        evs=[replace(ev, min=ev.min + 1.0e-6 * i) for i, ev in enumerate(clump.evs)],
    )


# ---------------------------------------------------------------------------
# enrich_stats dispatch internals
# ---------------------------------------------------------------------------

_BASE_SELECTORS: list[str] = ["total"]
"""The ``total``-only selector list (``shotclock_selectors``,
``LineupUtils.scala:995``) -- used directly (never extended) by the ORB/DRB/
STL/BLK/foul branches, which are permanently total-only per the module
docstring's "critical port fact"."""


def _shot_clock_selector_builder(ev: RawGameEvent) -> list[str]:
    """Segment names to increment for one shot/FT/TO/assist event
    (``shot_clock_selector_builder``, ``LineupUtils.scala:996-1004``).

    Args:
        ev: The event being dispatched (unused until Task 5c.3 wires
            ``is_scramble``/``is_transition`` in -- see the module
            docstring's "The dispatch seam").

    Returns:
        ``["total"]`` -- Task 5c.3 will append ``"orb"``/``"early"`` here
        based on the (not-yet-ported) scramble/transition heuristics.
    """
    del ev  # ponytail: seam for Task 5c.2/5c.3 (is_scramble/is_transition)
    return list(_BASE_SELECTORS)


def _increment_shot_clock(stat: ShotClockStats, selectors: list[str]) -> None:
    """Apply ``+1`` to each named segment (``increment_misc_count``,
    ``LineupUtils.scala:1006-1015``) -- ``selectors`` is a list of segment
    names rather than quicklens ``PathLazyModify`` objects, since Python has
    no lens library.

    Args:
        stat: The shot-clock stats object to mutate in place.
        selectors: Segment names to bump (``"total"``/``"early"``/``"orb"``).

    Raises:
        ValueError: If a selector name isn't one of the three known
            segments (an internal-logic-error guard).
    """
    for name in selectors:
        if name == "total":
            stat.total += 1
        elif name == "early":
            stat.early = (stat.early or 0) + 1
        elif name == "orb":
            stat.orb = (stat.orb or 0) + 1
        else:
            raise ValueError(f"Internal Logic Error, unknown shot-clock selector {name!r}")


def _get_or_create_shot_clock(obj: Any, attr: str) -> ShotClockStats:
    """Get-or-create an ``Optional[ShotClockStats]`` field by name (quicklens
    ``.atOrElse(emptyShotClock)``, e.g. ``LineupUtils.scala:167-168``).

    Args:
        obj: The dataclass instance owning the field.
        attr: The field name.

    Returns:
        The existing :class:`~sportsdataverse.mbb.mbb_ncaa_models.ShotClockStats`,
        or a freshly-created (and stored) one if the field was ``None``.
    """
    val: Optional[ShotClockStats] = getattr(obj, attr)
    if val is None:
        val = ShotClockStats()
        setattr(obj, attr, val)
    return val


def _get_or_create_assist_info(obj: Any, attr: str) -> AssistInfo:
    """Get-or-create an ``Optional[AssistInfo]`` field by name (quicklens
    ``.atOrElse(emptyAssist)``, ``LineupUtils.scala:165``).

    Args:
        obj: The dataclass instance owning the field.
        attr: The field name.

    Returns:
        The existing :class:`~sportsdataverse.mbb.mbb_ncaa_models.AssistInfo`,
        or a freshly-created (and stored) one if the field was ``None``.
    """
    val: Optional[AssistInfo] = getattr(obj, attr)
    if val is None:
        val = AssistInfo()
        setattr(obj, attr, val)
    return val


def _get_or_create_assist_list(assist_info: AssistInfo, attr: str) -> list[AssistEvent]:
    """Get-or-create the ``target``/``source`` ``Optional[list[AssistEvent]]``
    field on an :class:`~sportsdataverse.mbb.mbb_ncaa_models.AssistInfo`
    (quicklens ``.atOrElse(Nil)``, ``LineupUtils.scala:1076``).

    Args:
        assist_info: The assist-info instance owning the field.
        attr: ``"target"`` or ``"source"``.

    Returns:
        The existing list, or a freshly-created (and stored) empty one.
    """
    val: Optional[list[AssistEvent]] = getattr(assist_info, attr)
    if val is None:
        val = []
        setattr(assist_info, attr, val)
    return val


def _find_matching_assist(evs: list[RawGameEvent], event_parser: PossessionEvent) -> Optional[str]:
    """First co-located assist event's player, if any (``find_matching_assist``,
    ``LineupUtils.scala:176-186``).

    Args:
        evs: The clump's events (co-location = same clump).
        event_parser: Selects which side of each event is "attacking".

    Returns:
        The assisting player's name, or ``None`` if no assist is co-located.
    """
    for ev in evs:
        s = event_parser.attacking_team(ev)
        if s is None:
            continue
        player = parse_assist(s)
        if player is not None:
            return player
    return None


def _find_matching_fg(evs: list[RawGameEvent], event_parser: PossessionEvent) -> Optional[tuple[str, str]]:
    """First co-located made shot's player + bucket, if any
    (``find_matching_fg``, ``LineupUtils.scala:191-209``).

    Args:
        evs: The clump's events (co-location = same clump).
        event_parser: Selects which side of each event is "attacking".

    Returns:
        ``(player, attr)`` where ``attr`` is ``"ast_rim"``/``"ast_mid"``/
        ``"ast_3p"`` (the :class:`~sportsdataverse.mbb.mbb_ncaa_models
        .LineupEventStats` field name to increment), or ``None`` if no made
        shot is co-located.
    """
    for ev in evs:
        s = event_parser.attacking_team(ev)
        if s is None:
            continue
        player = parse_rim_made(s)
        if player is not None:
            return (player, "ast_rim")
        player = parse_two_pointer_made(s)
        if player is not None:
            return (player, "ast_mid")
        player = parse_three_pointer_made(s)
        if player is not None:
            return (player, "ast_3p")
    return None


def _increment_player_assist(assist_events: list[AssistEvent], player_code: str, selectors: list[str]) -> None:
    """Increment-if-present else prepend a fresh :class:`AssistEvent`
    (``increment_player_assist``, ``LineupUtils.scala:1040-1060``).

    Args:
        assist_events: The list to mutate in place.
        player_code: The other player in the assist relationship.
        selectors: Segment names to bump on the matching entry's ``count``.
    """
    for assist_event in assist_events:
        if assist_event.player_code == player_code:
            _increment_shot_clock(assist_event.count, selectors)
            return
    new_event = AssistEvent(player_code)
    _increment_shot_clock(new_event.count, selectors)
    assist_events.insert(0, new_event)


def _maybe_increment_assisted_stats(
    stats: LineupEventStats,
    fg_bucket_attr: str,
    ast_info_attr: str,
    clump: ConcurrentClump,
    event_parser: PossessionEvent,
    player_coder: Optional[Callable[[str], str]],
    selectors: list[str],
) -> None:
    """Called from a made-shot branch: if a co-located assist exists,
    increments the shooter's own ``fg_XX.ast`` and (team-direction,
    player-coded calls only) appends the assister into ``ast_XX.source``
    (``maybe_increment_assisted_stats``, ``LineupUtils.scala:1095-1118``).

    Args:
        stats: The stat tree being mutated.
        fg_bucket_attr: ``"fg_rim"``/``"fg_mid"``/``"fg_3p"``.
        ast_info_attr: ``"ast_rim"``/``"ast_mid"``/``"ast_3p"``.
        clump: The merged clump (for assist co-location).
        event_parser: Selects which side is "attacking".
        player_coder: Maps a raw name to its team-scoped code, if this call
            is player-scoped.
        selectors: Segment names to bump.
    """
    player_name = _find_matching_assist(clump.evs, event_parser)
    if player_name is None:
        return
    fg_bucket = getattr(stats, fg_bucket_attr)
    ast_shot_clock = _get_or_create_shot_clock(fg_bucket, "ast")
    _increment_shot_clock(ast_shot_clock, selectors)
    if player_coder is not None and event_parser.dir == Direction.TEAM:
        ast_info = _get_or_create_assist_info(stats, ast_info_attr)
        source_list = _get_or_create_assist_list(ast_info, "source")
        _increment_player_assist(source_list, player_coder(player_name), selectors)


def _increment_assisted_fg_stats(
    stats: LineupEventStats,
    clump: ConcurrentClump,
    event_parser: PossessionEvent,
    player_coder: Optional[Callable[[str], str]],
    selectors: list[str],
) -> None:
    """Called from the assist branch: finds the co-located made shot (first
    match in clump order -- rim, then mid, then 3p) and increments that
    bucket's ``AssistInfo.counts`` plus (team-direction, player-coded calls
    only) ``.target`` (``increment_assisted_fg_stats``, ``LineupUtils.scala
    :1119-1144``). Preserves the upstream TODO: multiple shots co-located in
    the same clump aren't handled -- only the first is credited.

    Args:
        stats: The stat tree being mutated.
        clump: The merged clump (for FG co-location).
        event_parser: Selects which side is "attacking".
        player_coder: Maps a raw name to its team-scoped code, if this call
            is player-scoped.
        selectors: Segment names to bump.
    """
    found = _find_matching_fg(clump.evs, event_parser)
    if found is None:
        return
    player_name, ast_info_attr = found
    ast_info = _get_or_create_assist_info(stats, ast_info_attr)
    _increment_shot_clock(ast_info.counts, selectors)
    if player_coder is not None and event_parser.dir == Direction.TEAM:
        target_list = _get_or_create_assist_list(ast_info, "target")
        _increment_player_assist(target_list, player_coder(player_name), selectors)


def _enrich_stats_with_clump(
    event_parser: PossessionEvent,
    player_filter_coder: Optional[PlayerFilterCoder],
    clump: ConcurrentClump,
    prev_clumps: list[ConcurrentClump],
    player_index: int,
    stats: LineupEventStats,
) -> LineupEventStats:
    """Dispatch one merged clump's events onto the stat tree (``private def
    enrich_stats_with_clump``, ``LineupUtils.scala:936-1437``). ``stats`` is
    mutated in place and returned.

    Args:
        event_parser: Selects which side of each event is "attacking".
        player_filter_coder: Optional ``name -> (is_this_player, code)``
            predicate/coder for per-player scoping.
        clump: The merged clump to dispatch.
        prev_clumps: Prior merged clumps, most-recent-first (unused until
            Task 5c.2/5c.3 wire ``is_scramble``/``is_transition`` in).
        player_index: Lineup-slot index for :func:`PlayerShotInfo` tuples
            (unused until Task 5c.4 -- see the module docstring).
        stats: The stat tree to mutate in place.

    Returns:
        ``stats``, mutated.
    """
    del prev_clumps  # ponytail: seam for Task 5c.2/5c.3
    del player_index  # ponytail: seam for Task 5c.4 (create_player_events)

    player_filter: Optional[Callable[[str], bool]]
    player_coder: Optional[Callable[[str], str]]
    if player_filter_coder is not None:
        pfc = player_filter_coder
        player_filter = lambda p: pfc(p)[0]  # noqa: E731
        player_coder = lambda p: pfc(p)[1]  # noqa: E731
    else:
        player_filter = None
        player_coder = None

    for ev in clump.evs:
        s = event_parser.attacking_team(ev)
        if s is None:
            continue

        player = parse_free_throw_made(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                _increment_shot_clock(stats.ft.attempts, selectors)
                _increment_shot_clock(stats.ft.made, selectors)
            continue

        player = parse_free_throw_missed(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                _increment_shot_clock(stats.ft.attempts, selectors)
            continue

        player = parse_rim_made(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg.made, selectors)
                _increment_shot_clock(stats.fg_2p.attempts, selectors)
                _increment_shot_clock(stats.fg_2p.made, selectors)
                _increment_shot_clock(stats.fg_rim.attempts, selectors)
                _increment_shot_clock(stats.fg_rim.made, selectors)
                _maybe_increment_assisted_stats(
                    stats, "fg_rim", "ast_rim", clump, event_parser, player_coder, selectors
                )
            continue

        player = parse_rim_missed(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg_2p.attempts, selectors)
                _increment_shot_clock(stats.fg_rim.attempts, selectors)
            continue

        player = parse_two_pointer_made(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg.made, selectors)
                _increment_shot_clock(stats.fg_2p.attempts, selectors)
                _increment_shot_clock(stats.fg_2p.made, selectors)
                _increment_shot_clock(stats.fg_mid.attempts, selectors)
                _increment_shot_clock(stats.fg_mid.made, selectors)
                _maybe_increment_assisted_stats(
                    stats, "fg_mid", "ast_mid", clump, event_parser, player_coder, selectors
                )
            continue

        player = parse_two_pointer_missed(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg_2p.attempts, selectors)
                _increment_shot_clock(stats.fg_mid.attempts, selectors)
            continue

        player = parse_three_pointer_made(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg.made, selectors)
                _increment_shot_clock(stats.fg_3p.attempts, selectors)
                _increment_shot_clock(stats.fg_3p.made, selectors)
                _maybe_increment_assisted_stats(stats, "fg_3p", "ast_3p", clump, event_parser, player_coder, selectors)
                # ponytail: increment_player_3p_shot_info (PlayerShotInfo
                # bucketing) is player_index>=0-only with no caller before
                # Task 5c.4's create_player_events -- deferred, see the
                # module docstring (LineupUtils.scala:1147-1178, 1316).
            continue

        player = parse_three_pointer_missed(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg_3p.attempts, selectors)
                # ponytail: see the 3pt-made branch above -- is_make=False
                # path deferred to Task 5c.4 the same way.
            continue

        player = parse_offensive_rebound(s)
        if player is not None and parse_offensive_deadball_rebound(s) is None:
            if player_filter is None or player_filter(player):
                orb = _get_or_create_shot_clock(stats, "orb")
                _increment_shot_clock(orb, _BASE_SELECTORS)
            continue

        player = parse_defensive_rebound(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                drb = _get_or_create_shot_clock(stats, "drb")
                _increment_shot_clock(drb, _BASE_SELECTORS)
            continue

        player = parse_turnover(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                _increment_shot_clock(stats.to, selectors)
            continue

        player = parse_stolen(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                stl = _get_or_create_shot_clock(stats, "stl")
                _increment_shot_clock(stl, _BASE_SELECTORS)
            continue

        player = parse_shot_blocked(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                blk = _get_or_create_shot_clock(stats, "blk")
                _increment_shot_clock(blk, _BASE_SELECTORS)
            continue

        player = parse_assist(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev)
                assist = _get_or_create_shot_clock(stats, "assist")
                _increment_shot_clock(assist, selectors)
                _increment_assisted_fg_stats(stats, clump, event_parser, player_coder, selectors)
            continue

        player = parse_personal_foul(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                foul = _get_or_create_shot_clock(stats, "foul")
                _increment_shot_clock(foul, _BASE_SELECTORS)
            continue

        player = parse_flagrant_foul(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                foul = _get_or_create_shot_clock(stats, "foul")
                _increment_shot_clock(foul, _BASE_SELECTORS)
            continue

        player = parse_technical_foul(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                foul = _get_or_create_shot_clock(stats, "foul")
                _increment_shot_clock(foul, _BASE_SELECTORS)
            continue

        player = parse_offensive_foul(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                foul = _get_or_create_shot_clock(stats, "foul")
                _increment_shot_clock(foul, _BASE_SELECTORS)
            continue

        # else: no-op (matches the Scala catch-all `case (state, _) => state`)

    return stats


def enrich_stats(
    lineup: LineupEvent,
    event_parser: PossessionEvent,
    stats: LineupEventStats,
    player_filter_coder: Optional[PlayerFilterCoder] = None,
    player_index: int = -1,
) -> LineupEventStats:
    """Fold a lineup's raw events into a counting-stat tree (``protected def
    enrich_stats``, ``LineupUtils.scala:115-162``). Reuses the Task 5a.3
    concurrent-clump batching (:func:`~sportsdataverse.mbb.mbb_ncaa_possessions
    .lineup_as_raw_clumps` + :func:`~sportsdataverse.mbb.mbb_ncaa_possessions
    .concurrent_event_handler`) rather than duplicating it -- both were
    already public/exported from Task 5a.3.

    ``stats`` is deep-copied once up front (see the module docstring's
    "Scala idiom decisions"), so this function never mutates the caller's
    ``stats`` argument -- safe to call repeatedly against the same starting
    literal (e.g. a shared "empty stats" fixture).

    Args:
        lineup: The lineup whose ``raw_game_events`` to fold over.
        event_parser: Selects which side (team/opponent) is "attacking".
        stats: The starting stat tree (not mutated -- see above).
        player_filter_coder: Optional ``name -> (is_this_player, code)``
            predicate/coder, for per-player scoping (Task 5c.4).
        player_index: Lineup-slot index for :class:`~sportsdataverse.mbb
            .mbb_ncaa_models.PlayerShotInfo` tuples (Task 5c.4; ``-1`` for
            team-level calls, the only value exercised before then).

    Returns:
        A new :class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEventStats`
        with every matching event folded in.
    """
    curr_stats = copy.deepcopy(stats)
    raw_clumps = [ensure_ev_uniqueness(c) for c in lineup_as_raw_clumps(lineup)]
    prev_clumps: list[ConcurrentClump] = []
    for clump in concurrent_event_handler(raw_clumps):
        curr_stats = _enrich_stats_with_clump(
            event_parser, player_filter_coder, clump, prev_clumps, player_index, curr_stats
        )
        prev_clumps = [clump] + prev_clumps
    return curr_stats


def add_stats_to_lineups(lineup: LineupEvent) -> LineupEvent:
    """Enrich a lineup with play-by-play stats for both team and opponent
    (``add_stats_to_lineups``, ``LineupUtils.scala:1441-1451``).

    Args:
        lineup: The lineup event to enrich (not mutated).

    Returns:
        A new :class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent` with
        ``team_stats``/``opponent_stats`` populated.
    """
    team_filter = PossessionEvent(Direction.TEAM)
    oppo_filter = PossessionEvent(Direction.OPPONENT)
    return replace(
        lineup,
        team_stats=enrich_stats(lineup, team_filter, lineup.team_stats),
        opponent_stats=enrich_stats(lineup, oppo_filter, lineup.opponent_stats),
    )
