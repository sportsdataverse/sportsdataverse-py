"""NCAA lineup enrichment / stat-tree population (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``)
``LineupUtils.scala`` -- the first four of five Phase-5c modules-in-one-file.
**Task 5c.1 ported the lineup-enrichment core**: :func:`enrich_lineup`
(score-delta pts/plus_minus), :func:`fix_possible_score_swap_bug`,
:func:`ensure_ev_uniqueness`, :func:`add_stats_to_lineups`, and the full
:func:`enrich_stats` event dispatch. **Task 5c.2 additionally ported**
:func:`is_scramble` (+ its private recursive ``_get_first_off_ev_set``
helper) and :func:`is_end_of_game_fouling_vs_fastbreak`. **Task 5c.3 ported**
:func:`is_transition` (which itself calls
``is_end_of_game_fouling_vs_fastbreak``) and wired **both** heuristics into
:func:`_shot_clock_selector_builder` -- the dispatch became feature-complete
for scramble/transition tagging. **Task 5c.4 ports** :func:`create_player_events`
(per-player stat splitting via :func:`~sportsdataverse.mbb.mbb_ncaa_names
.tidy_player`-backed name resolution + a per-player ``enrich_stats`` call),
the ``increment_player_3p_shot_info`` seam deferred by 5c.1-5c.3 (now
:func:`_increment_player_3p_shot_info`, wired into the two 3pt dispatch
branches), and the debug-only field-wise adders :func:`sum_event_stats` /
:func:`sum_shot_infos`. The new :class:`~sportsdataverse.mbb.mbb_ncaa_models
.PlayerEvent` dataclass (``models/ncaa/PlayerEvent.scala``, deferred by 5a)
is appended to ``mbb_ncaa_models.py`` as this task's scope addition.

**THE critical port fact.** ``ShotClockStats.mid``/``.late`` are **dead
fields -- never populated** anywhere in ``LineupUtils.scala``. Only three
segments are ever written: ``total`` (always), ``early`` (the
``is_transition`` heuristic), and ``orb`` (the ``is_scramble`` heuristic).
These are PLAY-TYPE heuristics, not shot-clock timer derivations -- there is
no game-clock arithmetic anywhere in this file. Only shots/FTs/TOs/assists
are ever eligible for ``early``/``orb`` tagging; rebounds/steals/blocks/fouls
are permanently total-only (``LineupUtils.scala:1332-1433`` never wraps those
branches' ``implicit`` selector in ``shot_clock_selector_builder``, using the
static ``basic_shotclock_selector`` instead).

**The dispatch seam (wired, Task 5c.3).** :func:`_shot_clock_selector_builder`
mirrors the Scala's ``shot_clock_selector_builder`` closure (``:996-1004``):
it takes the per-clump ``is_scramble_builder``/``is_transition_builder``
predicates (computed ONCE per clump in :func:`_enrich_stats_with_clump`,
mirroring ``LineupUtils.scala:949-960``) and returns ``["total"]`` plus
``"orb"`` if the event is a scramble and/or ``"early"`` if it's a
(non-scramble) transition play -- **scramble always wins**:
``is_transition_builder(ev, is_scramble)`` takes the just-computed scramble
result as its second argument, and :func:`is_transition`'s predicate hard-
codes ``not is_scramble and is_transition_event``. The always-total-only
branches (ORB/DRB/STL/BLK/foul) use the module-level :data:`_BASE_SELECTORS`
constant directly and are UNAFFECTED by this wiring -- exactly per the
plan's "only shots/FTs/TOs/assists get transition/scramble tagging" fact.

**3pt shot-info bucketing (Task 5c.4).** :func:`_increment_player_3p_shot_info`
(``increment_player_3p_shot_info``, ``LineupUtils.scala:1147-1178``) buckets
a 3pt shot into the shooter's per-lineup-slot :class:`~sportsdataverse.mbb
.mbb_ncaa_models.PlayerShotInfo` tuple; it is a no-op unless ``player_index``
is in ``[0, 5)`` -- the only caller that ever passes a non-default
``player_index`` is :func:`create_player_events`. Bucket priority (first
match wins): assisted (only possible on a make) -> ``ast_3pm``; else
transition -> ``early_3pa``; else unassisted make -> ``unast_3pm``; else
(unassisted miss) -> ``unknown_3pm``.

**``is_scramble`` port notes (Task 5c.2).**

* **Returns ``(predicate, tag)``, not just the predicate.** The oracle
  (``LineupUtilsTests.scala``'s ``"is_scramble"`` block) asserts the debug
  tag string directly (``"N/A"``/``"0a"``/``"1aa"``/``"1ab"``/``"1b"``/
  ``"2aa"``/``"2ab"``), so the tuple shape is load-bearing, not incidental.
* **``player_version`` is a dead parameter.** In the Scala,
  ``play_type_debug_scramble = false && !player_version`` is always
  ``false`` (Scala ``&&`` short-circuits on a literal ``false`` left operand
  regardless of the right side), so every ``debug_check_select_events``/
  ``debug_scramble_context`` call this flag guards is permanently
  unreachable -- pure ``println`` debug infra, zero effect on the returned
  ``(predicate, tag)``. This port keeps the parameter (for call-site/oracle
  signature parity) but does not port the dead debug-print bodies at all;
  ``del player_version`` documents the no-op.
* **Scala ``Set[RawGameEvent]`` becomes ``list[RawGameEvent]``.**
  :class:`~sportsdataverse.mbb.mbb_ncaa_models.RawGameEvent` is a plain
  (non-frozen) dataclass, so it has no ``__hash__`` -- a Python ``set``
  can't hold it. Every place the Scala builds a ``Set`` for ``ev => set(ev)``
  membership testing uses a plain ``list`` here instead; ``in``/``not in``
  on a list only needs ``__eq__`` (which the dataclass does define), so
  membership semantics -- including the ``ensure_ev_uniqueness``-nudged
  ``min`` making two textually-identical events compare unequal -- are
  preserved exactly (see ``test_is_scramble_0a_3_ensure_ev_uniqueness_dedup``).
* **The redundant ``.collect(get_off_ev)`` in ``has_multiple_distinct_off_evs``
  is a no-op**, ported away. The Scala's ``off_evs`` is already
  ``curr_clump.evs.collect(get_off_ev)`` (every element already satisfies
  ``get_off_ev``), so re-``collect``-ing an already-filtered list after
  ``filterNot`` changes nothing -- this port is
  ``any(ev not in first_off_ev_set for ev in off_evs)``, with no redundant
  second filter.

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

Apache-2.0 third-party port — see the ``NOTICE`` file at the repository root for the upstream copyright and full attribution.

**Landmine index (reachable scalar division).** None. Every computation in
this module's scope is integer counting, dict/list-shaped mutation, or plain
string/regex matching (via the already-ported ``mbb_ncaa_events`` parsers) --
no division by a runtime-derived value exists. ``is_scramble``'s
``threshold = 6.5 / 60``, ``is_transition``'s ``threshold = (7.5 or 10.5) /
60.0``, and ``is_end_of_game_fouling_vs_fastbreak``'s score arithmetic are all
fixed-literal/int-only, not runtime-denominator division.

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
    is_gen2,
    parse_any_play,
    parse_assist,
    parse_defensive_rebound,
    parse_flagrant_foul,
    parse_free_throw_attempt,
    parse_free_throw_event_attempt_gen2,
    parse_free_throw_made,
    parse_free_throw_missed,
    parse_live_offensive_rebound,
    parse_offensive_deadball_rebound,
    parse_offensive_event,
    parse_offensive_foul,
    parse_offensive_rebound,
    parse_personal_foul,
    parse_rim_made,
    parse_rim_missed,
    parse_shot_blocked,
    parse_shot_made,
    parse_shot_missed,
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
    FieldGoalStats,
    LineupEvent,
    LineupEventStats,
    PlayerCodeId,
    PlayerEvent,
    PlayerShotInfo,
    PossessionEvent,
    RawGameEvent,
    Score,
    ScoreInfo,
    ShotClockStats,
    score_to_tuple,
)
from sportsdataverse.mbb.mbb_ncaa_names import TidyPlayerContext, build_tidy_player_context, tidy_player
from sportsdataverse.mbb.mbb_ncaa_possessions import (
    ConcurrentClump,
    concurrent_event_handler,
    lineup_as_raw_clumps,
)
from sportsdataverse.mbb.mbb_ncaa_names import code_from_box

__all__ = [
    "enrich_lineup",
    "add_stats_to_lineups",
    "fix_possible_score_swap_bug",
    "enrich_stats",
    "ensure_ev_uniqueness",
    "is_scramble",
    "is_end_of_game_fouling_vs_fastbreak",
    "is_transition",
    "create_player_events",
    "sum_event_stats",
    "sum_shot_infos",
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


def _shot_clock_selector_builder(
    ev: RawGameEvent,
    is_scramble_builder: Callable[[RawGameEvent], bool],
    is_transition_builder: Callable[[RawGameEvent, bool], bool],
) -> list[str]:
    """Segment names to increment for one shot/FT/TO/assist event
    (``shot_clock_selector_builder``, ``LineupUtils.scala:996-1004``).

    Args:
        ev: The event being dispatched.
        is_scramble_builder: The per-clump scramble predicate from
            :func:`is_scramble` (computed once per clump by the caller).
        is_transition_builder: The per-clump transition predicate from
            :func:`is_transition` -- takes ``(ev, is_scramble)`` since
            scramble always wins over transition.

    Returns:
        ``["total"]`` plus ``"orb"`` if ``ev`` is a scramble event and/or
        ``"early"`` if ``ev`` is a (non-scramble) transition event.
    """
    selectors = list(_BASE_SELECTORS)
    scramble = is_scramble_builder(ev)
    transition = is_transition_builder(ev, scramble)
    if scramble:
        selectors.append("orb")
    if transition:
        selectors.append("early")
    return selectors


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


def _get_or_create_player_shot_info(stats: LineupEventStats) -> PlayerShotInfo:
    """Get-or-create ``stats.player_shot_info`` (quicklens
    ``.atOrElse(emptyPlayerShotInfo)``, ``LineupUtils.scala:1169-1171``).

    Args:
        stats: The stat tree owning the field.

    Returns:
        The existing :class:`~sportsdataverse.mbb.mbb_ncaa_models
        .PlayerShotInfo`, or a freshly-created (and stored) one.
    """
    if stats.player_shot_info is None:
        stats.player_shot_info = PlayerShotInfo()
    return stats.player_shot_info


def _bump_player_tuple(shot_info: PlayerShotInfo, attr: str, player_index: int) -> None:
    """``+1`` at ``player_index`` of a 5-slot ``PlayerTuple[Int]`` field,
    get-or-create'ing an all-zero tuple first (quicklens
    ``.atOrElse(emptyPlayerTupleInt) andThenModify player_tuple_selector
    (player_index)).using(_ + 1)``, ``LineupUtils.scala:970-992,1169-1174``).
    Tuples are immutable in Python, so this reads-mutates-as-list-writes-back
    rather than mutating in place.

    Args:
        shot_info: The :class:`~sportsdataverse.mbb.mbb_ncaa_models
            .PlayerShotInfo` owning the field.
        attr: ``"unknown_3pm"``/``"early_3pa"``/``"unast_3pm"``/``"ast_3pm"``.
        player_index: The lineup-slot index to bump, already guard-checked
            by the caller (:func:`_increment_player_3p_shot_info`).
    """
    tup = getattr(shot_info, attr)
    slots = list(tup) if tup is not None else [0, 0, 0, 0, 0]
    slots[player_index] += 1
    setattr(shot_info, attr, (slots[0], slots[1], slots[2], slots[3], slots[4]))


def _increment_player_3p_shot_info(
    stats: LineupEventStats,
    ev: RawGameEvent,
    is_make: bool,
    clump: ConcurrentClump,
    event_parser: PossessionEvent,
    player_index: int,
    is_scramble_builder: Callable[[RawGameEvent], bool],
    is_transition_builder: Callable[[RawGameEvent, bool], bool],
) -> None:
    """Bucket a 3pt shot into the shooter's per-lineup-slot
    :class:`~sportsdataverse.mbb.mbb_ncaa_models.PlayerShotInfo` tuple
    (``increment_player_3p_shot_info``, ``LineupUtils.scala:1147-1178``).
    No-op unless ``player_index`` is in ``[0, 5)`` (team-level calls pass
    ``-1``).

    Bucket priority, first match wins: **assisted** (only checked when
    ``is_make`` -- the Scala's ``clump.evs.filter(_ => is_make)`` reduces to
    the empty list on a miss, so a missed 3pt can never land in ``ast_3pm``)
    -> ``ast_3pm``; else **transition** -> ``early_3pa``; else
    **unassisted make** -> ``unast_3pm``; else (unassisted miss, or an
    unassisted/non-transition make that fell through -- can't happen given
    the ``elif is_make`` branch above it, kept for oracle-signature parity)
    -> ``unknown_3pm``.

    Args:
        stats: The stat tree being mutated.
        ev: The 3pt event being dispatched.
        is_make: Whether this is the made (``True``) or missed (``False``)
            3pt branch.
        clump: The merged clump (for assist co-location).
        event_parser: Selects which side of each event is "attacking".
        player_index: Lineup-slot index; no-op unless ``0 <= player_index < 5``.
        is_scramble_builder: The per-clump scramble predicate (same instance
            :func:`_shot_clock_selector_builder` uses for this event).
        is_transition_builder: The per-clump transition predicate.
    """
    if not (0 <= player_index < 5):
        return
    bucket_attr: str
    if is_make and _find_matching_assist(clump.evs, event_parser) is not None:
        bucket_attr = "ast_3pm"
    else:
        scramble = is_scramble_builder(ev)
        transition = is_transition_builder(ev, scramble)
        if transition:
            bucket_attr = "early_3pa"
        elif is_make:
            bucket_attr = "unast_3pm"
        else:
            bucket_attr = "unknown_3pm"
    shot_info = _get_or_create_player_shot_info(stats)
    _bump_player_tuple(shot_info, bucket_attr, player_index)


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
        prev_clumps: Prior merged clumps, most-recent-first -- feeds the
            per-clump :func:`is_scramble`/:func:`is_transition` builders
            (``LineupUtils.scala:949-960``).
        player_index: Lineup-slot index for :class:`~sportsdataverse.mbb
            .mbb_ncaa_models.PlayerShotInfo` tuples (Task 5c.4) -- ``-1`` for
            team-level calls, in which case :func:`_increment_player_3p_shot_info`
            is a no-op.
        stats: The stat tree to mutate in place.

    Returns:
        ``stats``, mutated.
    """
    player_filter: Optional[Callable[[str], bool]]
    player_coder: Optional[Callable[[str], str]]
    if player_filter_coder is not None:
        pfc = player_filter_coder
        player_filter = lambda p: pfc(p)[0]  # noqa: E731
        player_coder = lambda p: pfc(p)[1]  # noqa: E731
    else:
        player_filter = None
        player_coder = None

    # Computed ONCE per clump (LineupUtils.scala:949-960) -- not per event.
    player_version = player_filter_coder is not None
    is_transition_builder, _ = is_transition(clump, prev_clumps, event_parser, player_version)
    is_scramble_builder, _ = is_scramble(clump, prev_clumps, event_parser, player_version)

    for ev in clump.evs:
        s = event_parser.attacking_team(ev)
        if s is None:
            continue

        player = parse_free_throw_made(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
                _increment_shot_clock(stats.ft.attempts, selectors)
                _increment_shot_clock(stats.ft.made, selectors)
            continue

        player = parse_free_throw_missed(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
                _increment_shot_clock(stats.ft.attempts, selectors)
            continue

        player = parse_rim_made(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
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
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg_2p.attempts, selectors)
                _increment_shot_clock(stats.fg_rim.attempts, selectors)
            continue

        player = parse_two_pointer_made(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
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
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg_2p.attempts, selectors)
                _increment_shot_clock(stats.fg_mid.attempts, selectors)
            continue

        player = parse_three_pointer_made(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg.made, selectors)
                _increment_shot_clock(stats.fg_3p.attempts, selectors)
                _increment_shot_clock(stats.fg_3p.made, selectors)
                _maybe_increment_assisted_stats(stats, "fg_3p", "ast_3p", clump, event_parser, player_coder, selectors)
                _increment_player_3p_shot_info(
                    stats, ev, True, clump, event_parser, player_index, is_scramble_builder, is_transition_builder
                )
            continue

        player = parse_three_pointer_missed(s)
        if player is not None:
            if player_filter is None or player_filter(player):
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
                _increment_shot_clock(stats.fg.attempts, selectors)
                _increment_shot_clock(stats.fg_3p.attempts, selectors)
                _increment_player_3p_shot_info(
                    stats, ev, False, clump, event_parser, player_index, is_scramble_builder, is_transition_builder
                )
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
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
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
                selectors = _shot_clock_selector_builder(ev, is_scramble_builder, is_transition_builder)
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


# ---------------------------------------------------------------------------
# is_scramble (Task 5c.2)
# ---------------------------------------------------------------------------


def _clump_has_event(
    evs: list[RawGameEvent], event_parser: PossessionEvent, check: Callable[[str], Optional[str]]
) -> bool:
    """``True`` iff some attacking-side event in ``evs`` matches ``check``
    (shared by ``curr_clump_has_offense``/``curr_clump_has_orb``,
    ``LineupUtils.scala:415-431``).

    Args:
        evs: The events to scan.
        event_parser: Selects which side of each event is "attacking".
        check: A ``parse_x`` extractor (e.g. :func:`parse_offensive_event`).

    Returns:
        ``True`` on the first attacking-side event where ``check`` matches.
    """
    for ev in evs:
        s = event_parser.attacking_team(ev)
        if s is not None and check(s) is not None:
            return True
    return False


def _is_off_ev(ev: RawGameEvent, event_parser: PossessionEvent) -> bool:
    """Shot (made or missed) / FT-attempt / turnover membership test
    (``get_off_ev``, ``LineupUtils.scala:298-309``).

    Args:
        ev: The event to test.
        event_parser: Selects which side of ``ev`` is "attacking".

    Returns:
        ``True`` iff ``ev`` is attacking-side AND parses as one of shot
        missed/made, FT attempt, or turnover.
    """
    s = event_parser.attacking_team(ev)
    if s is None:
        return False
    return (
        parse_shot_missed(s) is not None
        or parse_shot_made(s) is not None
        or parse_free_throw_attempt(s) is not None
        or parse_turnover(s) is not None
    )


def _get_first_off_ev_set(
    curr_clump: ConcurrentClump,
    event_parser: PossessionEvent,
    off_evs: list[RawGameEvent],
    allow_tos: bool,
    skip_2nd_chance: bool,
) -> tuple[list[RawGameEvent], list[str], str]:
    """Find the event(s) making up the *first* offensive play in ``off_evs``
    (private, recursive; ``get_first_off_ev_set``, ``LineupUtils.scala
    :318-411``). See the module docstring's ``is_scramble`` port notes for
    why the Scala's ``Set[RawGameEvent]`` becomes a ``list[RawGameEvent]``
    here.

    Args:
        curr_clump: The full current clump (co-location context for the
            made-shot/FT branches, which scan ``curr_clump.evs`` rather than
            ``off_evs``).
        event_parser: Selects which side of each event is "attacking".
        off_evs: The candidate offensive events (already filtered to
            :func:`_is_off_ev`) to pick the first play from.
        allow_tos: If ``False``, a turnover can't be the head of a
            multi-pseudo-possession clump.
        skip_2nd_chance: If ``True``, an event whose ``.info`` contains
            ``"2ndchance"`` is skipped when picking the head (out-of-order
            2nd-chance-event workaround).

    Returns:
        ``(ev_list, info_list, debug_context)`` -- the events making up the
        first offensive play, their ``.info`` strings, and a debug label
        (``"(made shot)"``/``"(free throws)"``/``"(missed shots,
        turnovers)"``).
    """

    def is_to_or_maybe_2ndchance(ev: RawGameEvent) -> bool:
        if skip_2nd_chance and "2ndchance" in ev.info:
            return True
        if not allow_tos:
            s = event_parser.attacking_team(ev)
            if s is not None and parse_turnover(s) is not None:
                return True
        return False

    head: Optional[RawGameEvent] = None
    for ev in off_evs:
        if not is_to_or_maybe_2ndchance(ev):
            head = ev
            break

    ev_list: list[RawGameEvent]
    debug_context: str

    if head is None:
        ev_list = []
        debug_context = "(missed shots, turnovers)"
    else:
        s = event_parser.attacking_team(head)
        made_player = parse_shot_made(s) if s is not None else None
        ft_player = parse_free_throw_attempt(s) if s is not None and made_player is None else None

        if made_player is not None:
            # made-shot branch: co-located FT attempts + assists (check for and-1/assists)
            collected = [head]
            for ev2 in curr_clump.evs:
                t = event_parser.attacking_team(ev2)
                if t is None:
                    continue
                if parse_free_throw_attempt(t) is not None or parse_assist(t) is not None:
                    collected.append(ev2)
            ev_list = collected
            debug_context = "(made shot)"
        elif ft_player is not None:
            gen2 = parse_free_throw_event_attempt_gen2(s) if s is not None else None
            if gen2 is not None:
                _, attempt_no, total_fts = gen2
                if attempt_no == 1 and total_fts == 1 and skip_2nd_chance:
                    # this is an and-1 so it can't start the event, will just bypass
                    ev_list = []
                else:
                    # new format, can infer the right number of FT events to take
                    matching: list[RawGameEvent] = []
                    for ev2 in curr_clump.evs:
                        t = event_parser.attacking_team(ev2)
                        if t is not None and parse_free_throw_attempt(t) == ft_player:
                            matching.append(ev2)
                    ev_list = matching[:total_fts]
            else:
                # old gen... keep going until you see a (live) rebound
                prefix: list[RawGameEvent] = []
                for ev2 in curr_clump.evs:
                    t = event_parser.attacking_team(ev2)
                    if t is not None and parse_live_offensive_rebound(t) is not None:
                        break
                    prefix.append(ev2)
                gen1_matching: list[RawGameEvent] = []
                for ev2 in prefix:
                    t = event_parser.attacking_team(ev2)
                    if t is not None and parse_free_throw_attempt(t) == ft_player:
                        gen1_matching.append(ev2)
                ev_list = gen1_matching
            debug_context = "(free throws)"
        else:
            # just this event (missed shots, turnovers)
            ev_list = [head]
            debug_context = "(missed shots, turnovers)"

    if not ev_list and skip_2nd_chance:
        # try again but allowing 2nd chance this time
        return _get_first_off_ev_set(curr_clump, event_parser, off_evs, allow_tos, skip_2nd_chance=False)
    if not ev_list and not allow_tos:
        # try again but allowing TOs this time
        return _get_first_off_ev_set(curr_clump, event_parser, off_evs, allow_tos=True, skip_2nd_chance=skip_2nd_chance)
    return (ev_list, [ev2.info for ev2 in ev_list], debug_context)


def is_scramble(
    curr_clump: ConcurrentClump,
    prev_clumps: list[ConcurrentClump],
    event_parser: PossessionEvent,
    player_version: bool,
) -> tuple[Callable[[RawGameEvent], bool], str]:
    """Figure out if (each event of) the current clump is part of a
    "scramble scenario" following an ORB (``is_scramble``, ``LineupUtils
    .scala:222-597``).

    Returns a ``(predicate, debug_tag)`` tuple -- **the tuple shape is
    load-bearing**: the oracle asserts the debug tag string directly
    (``"N/A"``/``"0a"``/``"1aa"``/``"1ab"``/``"1b"``/``"2aa"``/``"2ab"``).

    Args:
        curr_clump: The clump to classify.
        prev_clumps: Prior merged clumps, most-recent-first.
        event_parser: Selects which side of each event is "attacking".
        player_version: Unused -- see the module docstring's ``is_scramble``
            port notes (the Scala's debug-print gate this flag controls is
            permanently ``false`` regardless of its value).

    Returns:
        ``(predicate, debug_tag)`` where ``predicate(ev)`` reports whether
        ``ev`` is part of a scramble.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_scramble

            predicate, tag = is_scramble(curr_clump, prev_clumps, event_parser, player_version=False)
            [predicate(ev) for ev in curr_clump.evs]
    """
    del player_version  # ponytail: only gates a permanently-dead Scala debug println.

    maybe_prev_clump = prev_clumps[0] if prev_clumps else None

    curr_clump_has_offense = _clump_has_event(curr_clump.evs, event_parser, parse_offensive_event)
    if not curr_clump_has_offense:
        # (no offensive plays in current clump so can just ignore all this logic)
        return (lambda ev: False, "N/A")

    curr_clump_has_orb = _clump_has_event(curr_clump.evs, event_parser, parse_live_offensive_rebound)

    prev_evs = maybe_prev_clump.evs if maybe_prev_clump is not None else []
    last_clump_offense_time: Optional[float] = None
    for ev in prev_evs:
        s = event_parser.attacking_team(ev)
        if s is None:
            continue
        if (
            parse_free_throw_missed(s) is not None
            or parse_shot_missed(s) is not None
            or parse_live_offensive_rebound(s) is not None
        ):
            last_clump_offense_time = ev.min
            break

    threshold = 6.5 / 60
    # (6.5s, leads to about 60% of ORBs being categorized as scrambles - Synergy has 50% being called "putback"s)

    if last_clump_offense_time is not None:
        # 1] Last play was my offense
        curr_min = curr_clump.min if curr_clump.min is not None else 0.0
        events_diff_mins = curr_min - last_clump_offense_time
        if events_diff_mins < threshold:
            if curr_clump_has_orb:
                # 1aa] all happened within threshold, so everything "now" is a scramble
                return (lambda ev: True, "1aa")

            # 1ab] the _1st_ event set is a scramble, but others aren't
            off_evs = [ev for ev in curr_clump.evs if _is_off_ev(ev, event_parser)]
            first_off_ev_set, first_off_ev_list, _ctx = _get_first_off_ev_set(
                curr_clump, event_parser, off_evs, allow_tos=True, skip_2nd_chance=False
            )
            # Look for dangling FT - ignore if so, timing error in PbP
            if len(first_off_ev_list) == 1 and parse_free_throw_attempt(first_off_ev_list[0]) is not None:
                first_off_ev_set = []

            def _predicate_1ab(ev: RawGameEvent) -> bool:
                return ev in first_off_ev_set

            return (_predicate_1ab, "1ab")
        # else: 1b] longer than threshold ago -- fall through to case 2 below
        # (the first event _won't_ be a scramble, though subsequent events will be)

    # 2] Last thing that happened was either opponent offense, or my recycled offense
    # (or a lineup change) -- first shot is _not_ a scramble (possibly including +1s)
    off_evs = [ev for ev in curr_clump.evs if _is_off_ev(ev, event_parser)]
    skip_2nd_chance = last_clump_offense_time is None
    # (ie lineup start or possession switch, start with 2nd chance => probably misordered events)
    first_off_ev_set, first_off_ev_list, _ctx = _get_first_off_ev_set(
        curr_clump, event_parser, off_evs, allow_tos=False, skip_2nd_chance=skip_2nd_chance
    )
    has_multiple_distinct_off_evs = any(ev not in first_off_ev_set for ev in off_evs)

    if curr_clump_has_orb and has_multiple_distinct_off_evs:
        # Multiple offensive events so need to do some more scramble analysis
        if maybe_prev_clump is None:
            debug_case = "0a"
        elif last_clump_offense_time is not None:
            debug_case = "1b"
        else:
            debug_case = "2aa"

        def _predicate_multi(ev: RawGameEvent) -> bool:
            return ev not in first_off_ev_set

        return (_predicate_multi, debug_case)

    # Just have one offensive option so can return simpler method
    # (or no ORB in current clump -- a "2ab] Weird case (fouls not ORBs?)" per the Scala comment)
    return (lambda ev: False, "2ab")


# ---------------------------------------------------------------------------
# is_end_of_game_fouling_vs_fastbreak (Task 5c.2)
# ---------------------------------------------------------------------------


def _near_end_of_game(minute: float) -> bool:
    """``True`` iff ``minute`` falls in the last ~2 minutes of regulation or
    any of 5 possible overtimes (``near_end_of_game``, ``LineupUtils.scala
    :613-615``).

    Args:
        minute: The game-clock minute (fractional).

    Returns:
        ``True`` iff ``minute`` is in ``(38, 40] | (43, 45] | (48, 50] |
        (53, 55] | (58, 60] | (63, 65]``.
    """
    return (
        (38 < minute <= 40)
        or (43 < minute <= 45)
        or (48 < minute <= 50)
        or (53 < minute <= 55)
        or (58 < minute <= 60)
        or (63 < minute <= 65)
    )


def _scores_close_but_behind(ev: RawGameEvent, event_parser: PossessionEvent, last_shot_made: bool) -> bool:
    """``True`` iff the attacking team is ahead by ``(0, 10]`` points, per
    ``ev``'s embedded score string (``scores_close_but_behind``,
    ``LineupUtils.scala:617-635``).

    Args:
        ev: The event whose ``score_str`` to parse.
        event_parser: Selects which side is "attacking" (the perspective the
            margin is computed from).
        last_shot_made: If ``True``, subtracts 1 from the margin (the score
            string already reflects the just-made FT/shot).

    Returns:
        ``True`` iff ``0 < diff <= 10``.
    """
    s1, s2 = score_to_tuple(ev.score_str)
    extra = 1 if last_shot_made else 0
    diff = (s1 - s2 if event_parser.dir == Direction.TEAM else s2 - s1) - extra
    return 0 < diff <= 10


def is_end_of_game_fouling_vs_fastbreak(curr_clump: ConcurrentClump, event_parser: PossessionEvent) -> bool:
    """Check for intentional fouling to prolong the game, specifically so it
    can be excluded from being counted as a fast break
    (``is_end_of_game_fouling_vs_fastbreak``, ``LineupUtils.scala:603-656``).

    Args:
        curr_clump: The clump to classify.
        event_parser: Selects which side of each event is "attacking".

    Returns:
        ``True`` iff the FIRST attacking-side FT-made/FT-missed event in
        ``curr_clump.evs`` is both near the end of a period AND has the
        attacking team ahead by ``(0, 10]`` points; ``False`` if no such
        event exists.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_end_of_game_fouling_vs_fastbreak

            is_end_of_game_fouling_vs_fastbreak(curr_clump, event_parser)
    """
    for ev in curr_clump.evs:
        s = event_parser.attacking_team(ev)
        if s is None:
            continue
        if parse_free_throw_missed(s) is not None:
            return _near_end_of_game(ev.min) and _scores_close_but_behind(ev, event_parser, last_shot_made=False)
        if parse_free_throw_made(s) is not None:
            return _near_end_of_game(ev.min) and _scores_close_but_behind(ev, event_parser, last_shot_made=True)
    return False


# ---------------------------------------------------------------------------
# is_transition (Task 5c.3)
# ---------------------------------------------------------------------------


def is_transition(
    curr_clump: ConcurrentClump,
    prev_clumps: list[ConcurrentClump],
    event_parser: PossessionEvent,
    player_version: bool,
) -> tuple[Callable[[RawGameEvent, bool], bool], str]:
    """Figure out if the current clump is part of a transition offense
    following opponent offense (or a marked-fastbreak play) (``is_transition``,
    ``LineupUtils.scala:668-927``).

    Returns a ``(predicate, debug_tag)`` tuple mirroring :func:`is_scramble`
    -- the oracle asserts the debug tag directly (``"N/A"``/``"0a.X"``/
    ``"1a.a"``/``"1a.b"``/``"1b.a"``/``"1b.b"``/``"1b.X"``/``"NOT"``).
    Unlike :func:`is_scramble`'s predicate, this one takes a *second*
    argument -- ``is_scramble`` -- so **scramble always wins**: an event
    already classified as a scramble is never additionally tagged
    transition (``!is_scramble && is_transition_event``).

    Args:
        curr_clump: The clump to classify.
        prev_clumps: Prior merged clumps, most-recent-first.
        event_parser: Selects which side of each event is "attacking" (and,
            for this heuristic, "defending").
        player_version: Unused -- see :func:`is_scramble`'s port notes in
            the module docstring (the Scala's debug-print gate this flag
            controls is permanently ``false`` regardless of its value).

    Returns:
        ``(predicate, debug_tag)`` where ``predicate(ev, is_scramble)``
        reports whether ``ev`` is part of a transition play, given whether
        it was already classified as a scramble.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import is_transition

            predicate, tag = is_transition(curr_clump, prev_clumps, event_parser, player_version=False)
            [predicate(ev, is_scramble=False) for ev in curr_clump.evs]
    """
    del player_version  # ponytail: only gates a permanently-dead Scala debug println (same as is_scramble).

    if not prev_clumps:
        # (if a lineup change has occurred we don't include it as transition by policy)
        return (lambda ev, is_scramble: False, "N/A")

    prev_clump = prev_clumps[0]

    if is_end_of_game_fouling_vs_fastbreak(curr_clump, event_parser):
        return (lambda ev, is_scramble: False, "0a.X")

    threshold = (7.5 if curr_clump.evs and is_gen2(curr_clump.evs[0]) else 10.5) / 60.0
    # (Based on analysis of events marked as fastbreak, this should be in the
    # 7-9 range; more conservative if we have fastbreak indications.)

    # Category 1a: quick shot/rebound following opponent offense.
    def _is_oppo_shot_or_team_rebound(ev: RawGameEvent) -> bool:
        d = event_parser.defending_team(ev)
        if d is not None and (
            parse_turnover(d) is not None or parse_shot_made(d) is not None or parse_free_throw_attempt(d) is not None
        ):
            return True
        a = event_parser.attacking_team(ev)
        return a is not None and parse_defensive_rebound(a) is not None

    candidate: Optional[RawGameEvent] = None
    for ev in prev_clump.evs:
        if _is_oppo_shot_or_team_rebound(ev):
            candidate = ev
            break

    def _attacking_is_drb(ev: RawGameEvent) -> bool:
        a = event_parser.attacking_team(ev)
        return a is not None and parse_defensive_rebound(a) is not None

    # 1a.b special case: the miss and its rebound land in different clumps.
    rebound_special_case: Optional[RawGameEvent] = None
    if candidate is None and any(_attacking_is_drb(ev) for ev in curr_clump.evs):
        for ev in prev_clump.evs:
            d = event_parser.defending_team(ev)
            if d is not None and parse_shot_missed(d) is not None:
                rebound_special_case = ev
                break

    quick_candidate = candidate if candidate is not None else rebound_special_case
    curr_min = curr_clump.min if curr_clump.min is not None else 0.0
    quick_shot_taken = quick_candidate is not None and (curr_min - quick_candidate.min) < threshold

    # Category 1b: "fastbreak"-marked plays (new-format PbP).
    def _first_team_offense_info(clump: ConcurrentClump) -> Optional[str]:
        for ev in clump.evs:
            s = event_parser.attacking_team(ev)
            if s is not None and parse_offensive_event(s) is not None:
                return ev.info
        return None

    first_off_ev = _first_team_offense_info(curr_clump)
    play_is_fastbreak = first_off_ev is not None and "fastbreak" in first_off_ev
    # (don't require candidate.nonEmpty because the existence of the fastbreak
    # in the 1st event trumps that -- but blocks certain prev-clump categories.)
    is_fastbreak_override_allowed = candidate is not None or _first_team_offense_info(prev_clump) is None

    is_transition_event_standard = (quick_shot_taken and first_off_ev is not None) or (  # (1b.a)
        play_is_fastbreak and is_fastbreak_override_allowed  # (1b.b, see above)
    )

    # Special case (also seen in is_scramble): the 2nd half of a FT pair.
    is_transition_event_dangling_ft = False
    if candidate is None:
        for ev in curr_clump.evs:
            s = event_parser.attacking_team(ev)
            if s is None:
                continue
            gen2 = parse_free_throw_event_attempt_gen2(s)
            if gen2 is not None and "fastbreak" in ev.info:
                _, attempt, total = gen2
                # (if attempts==1 should be 1b if anything, unless it's an +1)
                is_transition_event_dangling_ft = attempt > 1 or total == 1
                break

    is_transition_event = is_transition_event_standard or is_transition_event_dangling_ft

    debug_context: str
    if is_transition_event_standard and quick_shot_taken and rebound_special_case is not None:
        debug_context = "1a.b"  # short gap (RB in wrong clump)
    elif is_transition_event_standard and quick_shot_taken:
        debug_context = "1a.a"  # short gap
    elif is_transition_event_standard and not is_transition_event_dangling_ft:
        debug_context = "1b.a"  # play is fast break
    elif is_transition_event_dangling_ft:
        debug_context = "1b.b"  # "dangling FT" special case
    elif not is_transition_event_standard and play_is_fastbreak:
        debug_context = "1b.X"  # (fastbreak override REJECTED)
    else:
        debug_context = "NOT"  # not a transition event

    def _predicate(ev: RawGameEvent, is_scramble: bool) -> bool:
        del ev  # ponytail: only feeds a dead debug println (check_for_fastbreak).
        return not is_scramble and is_transition_event

    return (_predicate, debug_context)


# ---------------------------------------------------------------------------
# create_player_events (Task 5c.4)
# ---------------------------------------------------------------------------


def _player_tidier(
    player: PlayerCodeId, tidy_ctx: TidyPlayerContext, box_lineup: LineupEvent, valid_player_codes: set[str]
) -> list[PlayerCodeId]:
    """Re-resolve one lineup-slot player against the (possibly corrupted)
    box score, dropping it if it isn't actually in the box lineup
    (``player_tidier``, ``LineupUtils.scala:1463-1473``).

    Args:
        player: The player to re-resolve.
        tidy_ctx: The box-score lookup context (:func:`~sportsdataverse.mbb
            .mbb_ncaa_names.build_tidy_player_context`).
        box_lineup: The trusted box-score lineup (its ``team.team`` is the
            misspelling-correction scope).
        valid_player_codes: ``{p.code for p in box_lineup.players}``.

    Returns:
        ``[tidied_player]`` if the tidied code is a valid box-score player,
        else ``[]`` (Scala's ``flatMap``-friendly single-or-none list).
    """
    tidy_name, _ = tidy_player(player.id.name, tidy_ctx)
    tidy_player_code = code_from_box(tidy_name, box_lineup, box_lineup.team.team)
    if tidy_player_code.code in valid_player_codes:
        return [tidy_player_code]
    return []


def create_player_events(lineup_event_maybe_bad: LineupEvent, box_lineup: LineupEvent) -> list[PlayerEvent]:
    """Split a lineup event into one :class:`~sportsdataverse.mbb
    .mbb_ncaa_models.PlayerEvent` per player on the floor
    (``create_player_events``, ``LineupUtils.scala:1454-1529``).

    First re-tidies ``lineup_event_maybe_bad``'s ``players``/``players_in``/
    ``players_out`` against ``box_lineup`` (via :func:`_player_tidier`),
    dropping any player who doesn't actually resolve to a box-score player --
    this recovers from "impossible" lineups. Then, for each surviving player
    (in lineup-slot order, 0-4), builds their own :func:`enrich_stats` call
    with a per-player ``player_filter_coder`` + that player's slot index (the
    only caller in this module that ever passes a non-default
    ``player_index``, wiring :func:`_increment_player_3p_shot_info`).

    Args:
        lineup_event_maybe_bad: The lineup event to split (its player lists
            may reference names not actually in ``box_lineup``).
        box_lineup: The trusted box-score lineup for this game (name
            resolution + team-scoping context).

    Returns:
        One :class:`~sportsdataverse.mbb.mbb_ncaa_models.PlayerEvent` per
        (tidied) player in ``lineup_event_maybe_bad.players``, same order.
        Kept even if a player has zero matching raw events -- needed
        downstream for usage/possession math.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import create_player_events

            player_events = create_player_events(lineup, box_lineup)
            player_events[0].player_stats.fg_3p.made.total
    """
    tidy_ctx = build_tidy_player_context(box_lineup)
    valid_player_codes = {p.code for p in box_lineup.players}

    def player_tidier(player: PlayerCodeId) -> list[PlayerCodeId]:
        return _player_tidier(player, tidy_ctx, box_lineup, valid_player_codes)

    lineup_event = replace(
        lineup_event_maybe_bad,
        players=[tidied for p in lineup_event_maybe_bad.players for tidied in player_tidier(p)],
        players_in=[tidied for p in lineup_event_maybe_bad.players_in for tidied in player_tidier(p)],
        players_out=[tidied for p in lineup_event_maybe_bad.players_out for tidied in player_tidier(p)],
    )

    team_event_filter = PossessionEvent(Direction.TEAM)

    def base_player_event(player_id: PlayerCodeId) -> PlayerEvent:
        return PlayerEvent(
            player=player_id,
            player_stats=LineupEventStats.empty(),
            date=lineup_event.date,
            location_type=lineup_event.location_type,
            start_min=lineup_event.start_min,
            end_min=lineup_event.end_min,
            duration_mins=lineup_event.duration_mins,
            score_info=lineup_event.score_info,
            team=lineup_event.team,
            opponent=lineup_event.opponent,
            lineup_id=lineup_event.lineup_id,
            players=lineup_event.players,
            players_in=lineup_event.players_in,
            players_out=lineup_event.players_out,
            raw_game_events=lineup_event.raw_game_events,
            team_stats=lineup_event.team_stats,
            opponent_stats=lineup_event.opponent_stats,
            player_count_error=lineup_event.player_count_error,
        )

    def player_filter(player_id: PlayerCodeId) -> PlayerFilterCoder:
        def f(player_str: str) -> "tuple[bool, str]":
            code = code_from_box(tidy_player(player_str, tidy_ctx)[0], lineup_event, lineup_event.team.team).code
            return (code == player_id.code, code)

        return f

    results: list[PlayerEvent] = []
    for player_index, player in enumerate(lineup_event.players):
        this_player_filter = player_filter(player)
        player_event = base_player_event(player)

        player_raw_game_events: list[RawGameEvent] = []
        for ev in lineup_event.raw_game_events:
            s = team_event_filter.attacking_team(ev)
            if s is None:
                continue
            player_str = parse_any_play(s)
            if player_str is None:
                continue
            if this_player_filter(player_str)[0]:
                player_raw_game_events.append(ev)

        player_stats = enrich_stats(
            lineup_event, team_event_filter, player_event.player_stats, this_player_filter, player_index
        )
        results.append(
            replace(
                player_event,
                player_stats=replace(player_stats, num_events=len(player_raw_game_events)),
                raw_game_events=player_raw_game_events,
            )
        )
    return results


# ---------------------------------------------------------------------------
# sum_event_stats / sum_shot_infos (Task 5c.4, debug-only field-wise adders)
# ---------------------------------------------------------------------------


def _sum_opt_int(a: Optional[int], b: Optional[int]) -> Optional[int]:
    """``Some(l+r).filter(_ > 0)`` (``sum_int.case_maybe_int2``,
    ``LineupUtils.scala:1540-1543``) -- ``None`` if the sum isn't positive."""
    total = (a or 0) + (b or 0)
    return total if total > 0 else None


def _sum_shot_clock(a: ShotClockStats, b: ShotClockStats) -> ShotClockStats:
    """Field-wise sum of two :class:`ShotClockStats`
    (``sum_shot.case_shot2``, ``LineupUtils.scala:1548-1554``)."""
    return ShotClockStats(
        total=a.total + b.total,
        early=_sum_opt_int(a.early, b.early),
        mid=_sum_opt_int(a.mid, b.mid),
        late=_sum_opt_int(a.late, b.late),
        orb=_sum_opt_int(a.orb, b.orb),
    )


def _sum_opt_shot_clock(a: Optional[ShotClockStats], b: Optional[ShotClockStats]) -> Optional[ShotClockStats]:
    """Field-wise sum if both present, else whichever one is present
    (``sum_shot.case_maybe_shot2``, ``LineupUtils.scala:1556-1570``)."""
    if a is not None and b is not None:
        return _sum_shot_clock(a, b)
    return a if a is not None else b


def _sum_field_goal(a: FieldGoalStats, b: FieldGoalStats) -> FieldGoalStats:
    """Field-wise sum of two :class:`FieldGoalStats`
    (``sum.case_field2``, ``LineupUtils.scala:1602-1608``)."""
    return FieldGoalStats(
        attempts=_sum_shot_clock(a.attempts, b.attempts),
        made=_sum_shot_clock(a.made, b.made),
        ast=_sum_opt_shot_clock(a.ast, b.ast),
    )


def _merge_assist_events(a: Optional[list[AssistEvent]], b: Optional[list[AssistEvent]]) -> Optional[list[AssistEvent]]:
    """``Some(l.getOrElse(Nil) ++ r.getOrElse(Nil)).filter(_.nonEmpty)``
    (``sum_assist.case_maybe_assist2``, ``LineupUtils.scala:1589-1592``) --
    debug-only concatenation, no de-duplication (matches the Scala TODO)."""
    merged = (a or []) + (b or [])
    return merged if merged else None


def _sum_opt_assist_info(a: Optional[AssistInfo], b: Optional[AssistInfo]) -> Optional[AssistInfo]:
    """Field-wise sum if both present, else whichever one is present
    (``sum_assist.case_maybe_assist2``, ``LineupUtils.scala:1575-1597``)."""
    if a is not None and b is not None:
        return AssistInfo(
            counts=_sum_shot_clock(a.counts, b.counts),
            target=_merge_assist_events(a.target, b.target),
            source=_merge_assist_events(a.source, b.source),
        )
    return a if a is not None else b


def _sum_player_tuple(
    a: Optional[tuple[int, int, int, int, int]], b: Optional[tuple[int, int, int, int, int]]
) -> Optional[tuple[int, int, int, int, int]]:
    """Elementwise sum of two 5-slot ``PlayerTuple[Int]``\\ s if both
    present, else whichever one is present (``combine_info.case_tuple5x2``,
    ``LineupUtils.scala:1631-1643``)."""
    if a is not None and b is not None:
        x0, x1, x2, x3, x4 = a
        y0, y1, y2, y3, y4 = b
        return (x0 + y0, x1 + y1, x2 + y2, x3 + y3, x4 + y4)
    return a if a is not None else b


def sum_shot_infos(shot_infos: list[PlayerShotInfo]) -> Optional[PlayerShotInfo]:
    """Field-wise sum a list of :class:`~sportsdataverse.mbb.mbb_ncaa_models
    .PlayerShotInfo`\\ s (``sum_shot_infos``, ``LineupUtils.scala:1625-1655``,
    debug-only).

    Args:
        shot_infos: The list to combine, in order.

    Returns:
        ``None`` if ``shot_infos`` is empty; the single element if there's
        exactly one; otherwise a left-fold of pairwise field-wise sums
        (``reduceOption``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import sum_shot_infos
            from sportsdataverse.mbb.mbb_ncaa_models import PlayerShotInfo

            sum_shot_infos([PlayerShotInfo(ast_3pm=(1, 0, 0, 0, 0)), PlayerShotInfo(ast_3pm=(0, 1, 0, 0, 0))])
    """
    if not shot_infos:
        return None
    result = shot_infos[0]
    for other in shot_infos[1:]:
        result = PlayerShotInfo(
            unknown_3pm=_sum_player_tuple(result.unknown_3pm, other.unknown_3pm),
            early_3pa=_sum_player_tuple(result.early_3pa, other.early_3pa),
            unast_3pm=_sum_player_tuple(result.unast_3pm, other.unast_3pm),
            ast_3pm=_sum_player_tuple(result.ast_3pm, other.ast_3pm),
        )
    return result


def sum_event_stats(lhs: LineupEventStats, rhs: LineupEventStats) -> LineupEventStats:
    """Field-wise add two :class:`~sportsdataverse.mbb.mbb_ncaa_models
    .LineupEventStats` (``protected def sum_event_stats``, ``LineupUtils.scala
    :1534-1622``, debug-only -- the Scala's own docstring says "just used for
    debug"). The Scala builds this via ``shapeless.Generic`` field-zipping;
    this port is an explicit field-by-field call since Python has no
    equivalent generic-programming machinery.

    Args:
        lhs: The left-hand stat tree.
        rhs: The right-hand stat tree.

    Returns:
        A new :class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEventStats`
        with every field summed (see the module's private ``_sum_*`` helpers
        for the ``Optional``/nested-field summing rules).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import sum_event_stats
            from sportsdataverse.mbb.mbb_ncaa_models import LineupEventStats

            sum_event_stats(LineupEventStats.empty(), LineupEventStats.empty()).num_events
    """
    return LineupEventStats(
        num_events=lhs.num_events + rhs.num_events,
        num_possessions=lhs.num_possessions + rhs.num_possessions,
        fg=_sum_field_goal(lhs.fg, rhs.fg),
        fg_rim=_sum_field_goal(lhs.fg_rim, rhs.fg_rim),
        fg_mid=_sum_field_goal(lhs.fg_mid, rhs.fg_mid),
        fg_2p=_sum_field_goal(lhs.fg_2p, rhs.fg_2p),
        fg_3p=_sum_field_goal(lhs.fg_3p, rhs.fg_3p),
        ft=_sum_field_goal(lhs.ft, rhs.ft),
        orb=_sum_opt_shot_clock(lhs.orb, rhs.orb),
        drb=_sum_opt_shot_clock(lhs.drb, rhs.drb),
        to=_sum_shot_clock(lhs.to, rhs.to),
        stl=_sum_opt_shot_clock(lhs.stl, rhs.stl),
        blk=_sum_opt_shot_clock(lhs.blk, rhs.blk),
        assist=_sum_opt_shot_clock(lhs.assist, rhs.assist),
        ast_rim=_sum_opt_assist_info(lhs.ast_rim, rhs.ast_rim),
        ast_mid=_sum_opt_assist_info(lhs.ast_mid, rhs.ast_mid),
        ast_3p=_sum_opt_assist_info(lhs.ast_3p, rhs.ast_3p),
        foul=_sum_opt_shot_clock(lhs.foul, rhs.foul),
        player_shot_info=sum_shot_infos([x for x in (lhs.player_shot_info, rhs.player_shot_info) if x is not None]),
        pts=lhs.pts + rhs.pts,
        plus_minus=lhs.plus_minus + rhs.plus_minus,
    )
