"""Possession-boundary rule engine (pbpstats ``stats_nba`` semantics port).

Each rule function ports exactly one pbpstats property and carries a
``# pbpstats: file:lines`` citation. See the Phase B design spec and the
grounding dossier for the semantics inventory.

Task 3 catalog reconciliation (2026-07-03)
-------------------------------------------
The shot/FT-trip/jump-ball foul-family string tables below were reconciled
against the full 2023-24 NBA season subType catalog
(``c:/Users/saiem/Documents/ClaudeCowork/nba_data/v3_sweep/v3_subtype_catalog_2023.json``,
1,230 games, 187 actionType/subType combos, 0 errors), superseding the task
brief's 7-fixture-sample draft tables. Deliberately-unmapped / not further
discriminated subTypes, by action_type:

* **Foul** -- all 22 real subTypes observed (``Shooting``, ``Personal``,
  ``Offensive``, ``Loose Ball``, ``Offensive Charge``, ``Personal Take``,
  ``Technical``, ``Defense 3 Second``, ``Transition Take``, ``Flagrant Type
  1``/``2``, ``Away From Play``, ``Double Technical``, ``Flopping``,
  ``Hanging Technical``, ``Delay Technical``, ``Clear Path``, ``Double
  Personal``, ``Excess Timeout Technical``, ``Bench``,
  ``Non-Unsportsmanlike Technical``, ``Too Many Players Technical``) are
  members of ``_FOUL_ALL_KNOWN`` (so none of them ever trips
  ``UNKNOWN_SUBTYPE_COUNTER``), but only the subset referenced by a
  per-purpose table below (``_FOUL_TECHNICAL`` / ``_FOUL_AWAY_FROM_PLAY`` /
  ``_FOUL_TRANSITION_TAKE`` / ``_FOUL_NEXT_EVENT_SUPPRESS``) actually gates a
  boundary-rule branch -- ``Offensive``, ``Offensive Charge``, ``Clear
  Path``, ``Personal Take``, and ``Double Personal`` are recognized but
  inert here (pbpstats itself does not reference them in
  ``is_make_that_does_not_end_possession`` / ``ft_ends_possession`` /
  ``jump_ball_ends_possession`` either).
* **"Inbound" is NOT a v3 Foul subType at all** -- 0 occurrences across the
  full season. pbpstats' own ``is_inbound_foul`` reads a ``descriptor``
  field (``live/foul.py:46-47``) that ``playbyplayv3`` never populates (the
  raw action dict has no ``descriptor`` key at all -- confirmed against
  every committed fixture, unlike the cdn ``live`` feed pbpstats' own
  ``live`` provider consumes). ``_FOUL_INBOUND`` is therefore an empty
  frozenset; ``_is_inbound_foul_ft`` always evaluates ``False`` for this
  data source, which is the conservative direction (never suppresses a
  ``ft_ends_possession`` boundary for an exception this feed cannot
  detect).
* **Turnover** -- beyond the Task 2 ``is_no_turnover`` placeholder check and
  the shot-clock/kicked-ball rebound-coincidence set, individual subTypes
  are not further discriminated in Task 3: every real turnover
  (``is_no_turnover`` False) ends the possession uniformly per pbpstats
  (``stats_nba/enhanced_pbp_item.py:217-218``). ``Lane Violation`` and
  ``Offensive Goaltending`` are referenced (the no-FT branch of
  ``is_make_that_does_not_end_possession``); ``Jump Ball Violation``
  participates only through ``jump_ball_ends_possession``'s co-clock/next
  turnover guard, not a subType table.
* **Violation** -- ``Jump Ball`` (jumpball-violation guard in
  ``jump_ball_ends_possession``) and ``Double Lane`` (double-lane-violation,
  the no-FT branch of ``is_make_that_does_not_end_possession``) are
  referenced; ``Kicked Ball`` (941), ``Defensive Goaltending`` (669),
  ``Delay Of Game`` (479), and ``Lane`` (40) are not possession-boundary
  signals under pbpstats' scheme (the adjacent FieldGoal/Turnover event
  carries the boundary) and are deliberately left unmapped here.
* **Jump Ball** -- subTypes ``""`` (regular, 2,001 occurrences) and
  ``"Coach Challenge"`` (69) are NOT distinguished; ``jump_ball_ends_possession``
  applies identically to both (pbpstats' own ``JumpBall`` class has no
  subType branch either).
* **Free Throw** -- every subType is handled generically via the numeric
  ``"N of M"`` / G-League ``"NPT"`` pattern (:func:`_ft_trip_shape`) plus the
  ``"technical"``/``"flagrant"`` substring checks already established by
  :func:`is_last_ft_of_trip` (Task 2's ``_is_last_ft``) and
  :func:`is_technical_ft_row` -- no additional per-subType table is needed.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

#: Unknown subType strings seen at rule-decision time (conservative fallback taken).
UNKNOWN_SUBTYPE_COUNTER: "Counter[str]" = Counter()


def _norm(s: object) -> str:
    """Casefolded, stripped string of a possibly-None value."""
    return str(s or "").strip().casefold()


def resolve_event_team(row: dict, home_id: int, away_id: int) -> int:
    """Resolve the acting team id for one enhanced-pbp row.

    Prefers the row's own ``team_id`` when present and truthy; falls back to
    the ``location`` flag (``"h"`` / ``"v"``) mapped onto the game's home/away
    team ids for rows that carry a location but no team id (e.g. some
    period/jump-ball rows); returns ``0`` when neither signal is available.

    Args:
        row: A single enhanced-pbp row dict (as produced by
            :func:`sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`).
        home_id: The home team's ``team_id``.
        away_id: The away team's ``team_id``.

    Returns:
        The resolved team id, or ``0`` if the row carries neither a truthy
        ``team_id`` nor a recognized ``location``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_rules import resolve_event_team

            team_id = resolve_event_team({"team_id": 0, "location": "h"}, 1610612747, 1610612738)
            print(team_id)  # 1610612747

        Pipeline next step (resolve the acting team for every row)::

            teams = [resolve_event_team(r, home_id, away_id) for r in rows]
    """
    team = row.get("team_id") or 0
    if team:
        return int(team)
    loc = row.get("location") or ""
    if loc == "h":
        return home_id
    if loc == "v":
        return away_id
    return 0


@dataclass
class EventContext:
    """Pre-pass index over enhanced-pbp rows for co-clock rule lookups.

    Mirrors pbpstats ``get_all_events_at_current_time``
    (pbpstats: resources/enhanced_pbp/enhanced_pbp_item.py:52-69).

    Args:
        rows: The full ordered list of enhanced-pbp row dicts for one game.
        at_clock: Index mapping ``(period, seconds_remaining)`` to the list
            of row indices sharing that exact clock instant. Built once by
            :func:`build_event_context`; not intended to be constructed or
            mutated by hand.
    """

    rows: list[dict] = field(default_factory=list)
    at_clock: dict[tuple[int, float], list[int]] = field(default_factory=dict)

    def co_clock(self, i: int) -> list[int]:
        """Indices of all rows sharing (period, seconds_remaining) with row i.

        Args:
            i: Index of the row (into ``self.rows``) to look up.

        Returns:
            List of row indices sharing the same ``(period,
            seconds_remaining)`` instant as row ``i``, always including ``i``
            itself. Falls back to ``[i]`` when the exact clock key was not
            recorded during index construction (should not occur for
            in-range indices built via :func:`build_event_context`).
        """
        row = self.rows[i]
        return self.at_clock.get(
            (int(row.get("period") or 0), float(row.get("seconds_remaining") or 0.0)),
            [i],
        )


def build_event_context(rows: list[dict]) -> EventContext:
    """Build the co-clock index in one pass over the row dicts.

    Groups row indices by their ``(period, seconds_remaining)`` clock instant
    so that :meth:`EventContext.co_clock` can later answer "which other rows
    happened at the exact same moment" in O(1) -- the building block several
    possession-rule exclusions (e.g. rebound/turnover coincidence) rely on.

    Args:
        rows: Ordered enhanced-pbp row dicts for one game, as produced by
            :func:`sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`.

    Returns:
        An :class:`EventContext` wrapping ``rows`` plus the derived
        ``at_clock`` index. An empty ``rows`` list returns an
        :class:`EventContext` with empty ``rows``/``at_clock``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_possession_rules import build_event_context

            rows = enhanced_pbp_from_payload(pbp_v3_payload).to_dicts()
            ctx = build_event_context(rows)
            print(len(ctx.rows), len(ctx.at_clock))

        Pipeline next step (look up co-clock rows for a rebound)::

            co_clock_indices = ctx.co_clock(i)
    """
    at_clock: dict[tuple[int, float], list[int]] = {}
    for i, row in enumerate(rows):
        key = (int(row.get("period") or 0), float(row.get("seconds_remaining") or 0.0))
        at_clock.setdefault(key, []).append(i)
    return EventContext(rows=rows, at_clock=at_clock)


def is_no_turnover(row: dict) -> bool:
    """A turnover placeholder row that is not a real turnover.

    # pbpstats: resources/enhanced_pbp/turnover.py:16-18 (abstract);
    # live/turnover.py:15-17 (``not hasattr(self, "sub_type")``). playbyplayv3
    # carries subType strings for every real turnover (season-catalog-verified);
    # an empty/missing subType is the conservative placeholder signal.
    """
    return (row.get("event_type") or "") == "turnover" and not _norm(row.get("sub_type"))


def _rebound_missed_shot_index(ctx: EventContext, i: int) -> int:
    """Index of the missed shot / missed FT this rebound follows, else -1.

    # pbpstats: resources/enhanced_pbp/rebound.py:16-28 (``missed_shot`` walks
    # previous events for a Missed FieldGoal or missed FreeThrow).
    """
    rows = ctx.rows
    for j in range(i - 1, -1, -1):
        et = rows[j].get("event_type") or ""
        if et == "missed_shot":
            return j
        if et == "free_throw":
            sh = (rows[j].get("score_home") or "").strip()
            sa = (rows[j].get("score_away") or "").strip()
            if not (sh or sa):  # missed FT (no score string -- same signal as _ft_made)
                return j
            return -1
        if et in ("made_shot", "turnover", "rebound", "jump_ball", "period"):
            return -1
    return -1


_TURNOVER_COINCIDENT_SUBTYPES = frozenset(("shot clock turnover", "kicked ball violation"))


def is_real_rebound(ctx: EventContext, i: int) -> bool:
    """The 4 placeholder exclusions of pbpstats ``is_real_rebound``, v3-reconstructed.

    # pbpstats: resources/enhanced_pbp/rebound.py:30-133 (abstract exclusion
    # chain) + resources/enhanced_pbp/live/rebound.py:39-52 (concrete
    # ``LiveRebound.is_placeholder``, the class actually instantiated by the
    # ``live`` data provider -- the oracle this v3-native engine is validated
    # against via the file-mode ``pbpstats`` round-trip harness).
    #
    # Empirical grounding (2026-07-03, oracle-derived; see dev/ probe script,
    # not committed): a standalone ``player1_id == 0`` / v3 ``team_id == 0``
    # exclusion -- the shape of ``StatsRebound.is_placeholder`` (stats_nba/
    # rebound.py:75-82, ``event_action_type != 0 and player1_id == 0``) -- was
    # cross-tabulated against pbpstats' own ``live``-provider classification
    # (``Client({"Possessions": {"data_provider": "live"}})``) for every
    # Rebound event across all 3 committed cdn fixtures (298 rebound rows
    # total). Result: it is WRONG to port as a 5th exclusion here. The
    # concrete ``LiveRebound.is_placeholder`` (deadball qualifier / flagrant
    # missed FT) does not fire for routine team rebounds, so pbpstats-live
    # scores essentially every team rebound (v3 ``team_id == 0``) as a REAL
    # rebound -- confirmed for 96-99 of ~98-107 team-attributed rows across
    # the 3 games. Blanket-excluding on ``team_id == 0`` alone flips 11-12
    # per game from correct to wrong (measured: game 0022100001 9->11 false
    # negatives, 0022200001 1->12, 0022300001 0->11), which is a regression,
    # not a fix -- so ``StatsRebound``'s reading does not transfer to the
    # oracle this engine targets, and is deliberately NOT ported as a
    # standalone rule.
    #
    # What the ``player1_id == 0`` signal (v3: ``team_id == 0`` -- a
    # team-attributed rebound with no individual crediting player) DOES
    # gate, per the abstract ``Rebound`` class itself, is a REQUIRED
    # sub-condition of 3 of the 4 remaining exclusions:
    #   ``is_turnover_placeholder``              (rebound.py:65-79)
    #   ``is_buzzer_beater_placeholder``          (rebound.py:92-113)
    #   ``is_buzzer_beater_rebound_at_shot_time`` (rebound.py:115-133)
    # all three literally read ``... and self.player1_id == 0`` in their
    # return conditions. Exclusions 2/3/4 below were previously missing that
    # guard, which the oracle cross-tab caught as 3 real false negatives:
    # a *personal* (non-team) rebound in the final 3 seconds of a period
    # followed by period-end was being wrongly excluded as a buzzer-beater
    # placeholder (e.g. game 0022100001 action_number 163/326 "Harden
    # REBOUND", game 0022200001 action_number 193 "House Jr. REBOUND" --
    # oracle scores all 3 as real; the pre-fix predicate scored them False).
    # Exclusion 2 (turnover-coincident) never manifested this bug in the 3
    # fixtures (its one qualifying co-clock-turnover row happened to already
    # be a team rebound) but gets the same guard for pbpstats fidelity.
    #  1. non-final-(live-ball)-FT-miss placeholder (rebound.py:81-90,
    #     is_non_live_ft_placeholder) -- reconstructed via
    #     _rebound_missed_shot_index + _is_last_ft instead of
    #     ``missed_shot.is_end_ft``. ``is_end_ft`` additionally excludes
    #     flagrant free throws even when numerically last-of-trip
    #     (free_throw.py:60-70: ``... and not self.is_flagrant_ft``), which
    #     the oracle cross-tab caught as 1 false positive (game 0022100001
    #     action_number 267 "BUCKS Rebound" after a missed "Free Throw
    #     Flagrant 3 of 3" -- oracle scores it a placeholder; the pre-fix
    #     predicate, using bare ``_is_last_ft``, scored it real). Handled
    #     locally here (a "flagrant" substring check on the FT's sub_type)
    #     rather than editing the shared ``_is_last_ft`` in nba_possessions,
    #     which serves a different (possession-trip) purpose and is out of
    #     this module's scope.
    #  2. turnover-coincident placeholder (rebound.py:65-79,
    #     is_turnover_placeholder) -- reconstructed via co_clock +
    #     is_no_turnover instead of ``is_shot_clock_violation``/
    #     ``is_kicked_ball``; gated on the row being a team rebound
    #     (v3 team_id == 0), matching the ``player1_id == 0`` conjunct.
    #  3+4. buzzer-beater-at-0.0s (rebound.py:92-113) + buzzer-beater-at-
    #     shot-time (rebound.py:115-133) placeholders -- reconstructed
    #     together via seconds_remaining <= 3 and the next non-rebound row
    #     being a period boundary (v3 has no Replay event type to skip over,
    #     so ``next_event`` is simply the next row), gated on the row being
    #     a team rebound (v3 team_id == 0), matching the ``player1_id == 0``
    #     conjunct both pbpstats properties require.
    #
    # Oracle agreement after this fix (pbpstats-live vs this predicate, all
    # Rebound rows, 3 committed fixtures): 118/118, 86/86, 94/94 (0 disagree).
    """
    rows = ctx.rows
    row = rows[i]
    if (row.get("event_type") or "") != "rebound":
        return False
    is_team_rebound = int(row.get("team_id") or 0) == 0
    # 1. rebound after a missed NON-final (or flagrant) FT -> placeholder
    #    (play continues to the next FT / possession does not go live).
    j = _rebound_missed_shot_index(ctx, i)
    if j >= 0 and (rows[j].get("event_type") or "") == "free_throw":
        # local import: avoids cycle with nba_possessions
        from sportsdataverse.nba.nba_possessions import _is_last_ft

        ft_sub_type = rows[j].get("sub_type") or ""
        is_flagrant_ft = "flagrant" in _norm(ft_sub_type)
        if not _is_last_ft(ft_sub_type) or is_flagrant_ft:
            return False
    # 2. team rebound coincident with a shot-clock / kicked-ball turnover at
    #    the same clock (pbpstats requires both the turnover coincidence AND
    #    player1_id == 0 -- a personal rebound at the same instant is real).
    if is_team_rebound:
        for k in ctx.co_clock(i):
            if k == i:
                continue
            if (rows[k].get("event_type") or "") == "turnover" and not is_no_turnover(rows[k]):
                if _norm(rows[k].get("sub_type")) in _TURNOVER_COINCIDENT_SUBTYPES:
                    return False
    # 3+4. buzzer-beater placeholders (team rebounds only -- see docstring):
    #    rebound at 0.0s, or at the same clock as a <=3s missed shot, when
    #    the next non-rebound row is a period boundary.
    if is_team_rebound:
        secs = float(row.get("seconds_remaining") or 0.0)
        if secs <= 3.0:
            nxt = i + 1
            while nxt < len(rows) and (rows[nxt].get("event_type") or "") == "rebound":
                nxt += 1
            next_is_period_end = nxt >= len(rows) or (rows[nxt].get("event_type") or "") == "period"
            if next_is_period_end and (
                secs == 0.0 or (j >= 0 and float(rows[j].get("seconds_remaining") or 0.0) == secs)
            ):
                return False
    return True


# ---------------------------------------------------------------------------
# Task 3: shot / FT-trip / jump-ball rules + is_possession_ending_event
# ---------------------------------------------------------------------------

# -- Foul-family string tables (subType, case-insensitive via _norm) --------
#
# See the module docstring "Task 3 catalog reconciliation" section for the
# full-season provenance and the deliberately-unmapped subTypes.

_FOUL_TECHNICAL = frozenset(
    {
        "technical",
        "double technical",
        "delay technical",
        "hanging technical",
        "flopping",
        "excess timeout technical",
        "non-unsportsmanlike technical",
        "too many players technical",
        "bench",
    }
)
_FOUL_AWAY_FROM_PLAY = frozenset({"away from play"})
#: Empty -- v3 carries no "inbound" discriminant (see module docstring).
_FOUL_INBOUND: "frozenset[str]" = frozenset()
_FOUL_TRANSITION_TAKE = frozenset({"transition take"})
_FOUL_LOOSE_BALL = frozenset({"loose ball"})
_FOUL_PERSONAL = frozenset({"personal"})
_FOUL_FLAGRANT = frozenset({"flagrant type 1", "flagrant type 2"})
#: pbpstats stats_nba/enhanced_pbp_item.py:239-245 -- the exact 4-way OR used
#: for ``next_event_is_foul_drawn_at_ft_time`` (loose_ball/personal/away_from
#: _play/flagrant). Deliberately narrower than "every non-technical foul" --
#: e.g. "personal take"/"offensive"/"clear path" are NOT in pbpstats' list.
_FOUL_NEXT_EVENT_SUPPRESS = _FOUL_LOOSE_BALL | _FOUL_PERSONAL | _FOUL_AWAY_FROM_PLAY | _FOUL_FLAGRANT

#: All 22 real Foul subTypes observed in the full 2023-24 season catalog.
#: Used only to detect a genuinely novel subType (future-season drift) for
#: ``UNKNOWN_SUBTYPE_COUNTER`` -- membership here does not imply the subType
#: gates any specific boundary-rule branch (see module docstring).
_FOUL_ALL_KNOWN = frozenset(
    {
        "shooting",
        "personal",
        "offensive",
        "loose ball",
        "offensive charge",
        "personal take",
        "technical",
        "defense 3 second",
        "transition take",
        "flagrant type 1",
        "away from play",
        "double technical",
        "flopping",
        "hanging technical",
        "delay technical",
        "clear path",
        "flagrant type 2",
        "double personal",
        "excess timeout technical",
        "bench",
        "non-unsportsmanlike technical",
        "too many players technical",
    }
)

_FT_OF_RE = re.compile(r"(\d+)\s+of\s+(\d+)")
_FT_GL_PT_RE = re.compile(r"(\d+)\s*pt\b")


def _same_instant(rows: "list[dict]", a: int, b: int) -> bool:
    """True if rows[a] and rows[b] share (period, seconds_remaining)."""
    ra, rb = rows[a], rows[b]
    return int(ra.get("period") or 0) == int(rb.get("period") or 0) and float(
        ra.get("seconds_remaining") or 0.0
    ) == float(rb.get("seconds_remaining") or 0.0)


def _note_unknown_foul_sub_type(row: dict) -> None:
    """Increment UNKNOWN_SUBTYPE_COUNTER for a Foul subType outside the
    full-season catalog vocabulary (``_FOUL_ALL_KNOWN``) -- a future-season
    drift signal, not an expected occurrence today."""
    sub = _norm(row.get("sub_type"))
    if sub and sub not in _FOUL_ALL_KNOWN:
        UNKNOWN_SUBTYPE_COUNTER[f"foul_sub_type:{sub}"] += 1


def _ft_trip_shape(row: dict) -> "tuple[int, int] | None":
    """Parse a ``(shot_number, trip_size)`` pair from a free-throw sub_type.

    Handles the standard ``"N of M"`` pattern (``"Free Throw 1 of 2"``,
    ``"Free Throw Clear Path 2 of 2"``, ``"Free Throw Flagrant 3 of 3"``) and
    the G-League ``"NPT"`` single-free-throw variant, mapped onto the trip
    shape it functionally replaces (``"Free Throw 2PT"`` -> ``(2, 2)``, not
    ``(1, 1)``) -- matching pbpstats' own grouping of ``is_ft_Npt`` alongside
    ``is_ft_N_of_N`` (e.g. ``field_goal.py:246-260`` groups
    ``is_ft_2_of_2 or is_ft_2pt`` together) rather than treating a
    point-valued FT as a literal one-shot trip.

    Args:
        row: A free-throw enhanced-pbp row dict.

    Returns:
        ``(shot_number, trip_size)``, or ``None`` for sub_types with no
        numeric trip shape (e.g. a plain ``"Free Throw Technical"``).
    """
    sub = _norm(row.get("sub_type"))
    m = _FT_OF_RE.search(sub)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    m = _FT_GL_PT_RE.search(sub)
    if m:
        n = int(m.group(1))
        return (n, n)
    return None


def is_technical_ft_row(row: dict) -> bool:
    """# pbpstats: free_throw.py is_technical_ft (subType contains 'Technical')."""
    return "technical" in _norm(row.get("sub_type"))


def is_last_ft_of_trip(row: dict) -> bool:
    """WP1's ``_is_last_ft`` + pbpstats' flagrant carve-out.

    # pbpstats: free_throw.py:59-71 (is_end_ft excludes is_flagrant_ft).
    """
    sub = _norm(row.get("sub_type"))
    if "flagrant" in sub:
        return False
    from sportsdataverse.nba.nba_possessions import _is_last_ft  # local: avoids cycle

    return _is_last_ft(row.get("sub_type") or "")


def foul_that_led_to_ft(ctx: EventContext, i: int) -> int:
    """Resolve the foul row that produced FT row i (co-clock, opposite team).

    # pbpstats: free_throw.py:173-221 -- backward walk over same-clock rows
    # for a non-technical Foul; if none found, a forward walk (pbpstats' own
    # "bug in pbp where foul is after FT" fallback). Returns -1 when neither
    # walk finds one (row i is not a real drawn-foul FT, or the foul fell
    # outside the co-clock instant entirely).
    """
    rows = ctx.rows
    row = rows[i]
    period = int(row.get("period") or 0)
    secs = float(row.get("seconds_remaining") or 0.0)

    def _is_real_foul(j: int) -> bool:
        r = rows[j]
        return (r.get("event_type") or "") == "foul" and _norm(r.get("sub_type")) not in _FOUL_TECHNICAL

    j = i - 1
    while (
        j >= 0 and int(rows[j].get("period") or 0) == period and float(rows[j].get("seconds_remaining") or 0.0) == secs
    ):
        if _is_real_foul(j):
            _note_unknown_foul_sub_type(rows[j])
            return j
        j -= 1

    j = i + 1
    while (
        j < len(rows)
        and int(rows[j].get("period") or 0) == period
        and float(rows[j].get("seconds_remaining") or 0.0) == secs
    ):
        if _is_real_foul(j):
            _note_unknown_foul_sub_type(rows[j])
            return j
        j += 1

    return -1


def _fouls_at_shot_time(ctx: EventContext, i: int) -> "list[int]":
    """Co-clock Foul rows at a made shot's instant, excluding the technical
    family.

    # pbpstats: field_goal.py:326-335 (``not is_delay_of_game and not
    # is_technical``). v3 has no distinct "Delay Of Game" *Foul* subType
    # (only a Turnover/Violation one) -- "Delay Technical" is the only
    # delay-flavored Foul subType observed in the full-season catalog, and
    # it is already a member of ``_FOUL_TECHNICAL``, so one technical-family
    # exclusion covers both upstream conditions for this data source.
    """
    rows = ctx.rows
    return [
        j
        for j in ctx.co_clock(i)
        if (rows[j].get("event_type") or "") == "foul" and _norm(rows[j].get("sub_type")) not in _FOUL_TECHNICAL
    ]


def _make_ends_possession_1_foul(ctx: EventContext, i: int, foul_idx: int) -> bool:
    """# pbpstats: field_goal.py:223-281 (_check_if_make_ends_possession_when_1_foul)."""
    rows = ctx.rows
    row = rows[i]
    foul_row = rows[foul_idx]
    foul_team = foul_row.get("team_id")
    shooter_team = row.get("team_id")

    if _norm(foul_row.get("sub_type")) in _FOUL_FLAGRANT and foul_team != shooter_team:
        return True

    if shooter_team == foul_team:
        return False

    co = ctx.co_clock(i)
    other_makes = [
        j
        for j in co
        if j != i and (rows[j].get("event_type") or "") == "made_shot" and rows[j].get("team_id") == shooter_team
    ]
    ft11 = [
        j
        for j in co
        if (rows[j].get("event_type") or "") == "free_throw"
        and _ft_trip_shape(rows[j]) == (1, 1)
        and not is_technical_ft_row(rows[j])
    ]

    if len(other_makes) == 1 and len(ft11) == 1:
        if rows[ft11[0]].get("person_id") == rows[other_makes[0]].get("person_id"):
            return False
        if rows[ft11[0]].get("person_id") == row.get("person_id"):
            return True
    elif ft11:
        return any(rows[j].get("team_id") == shooter_team for j in ft11)
    else:
        for j in co:
            r = rows[j]
            sub = _norm(r.get("sub_type"))
            if (r.get("event_type") or "") == "turnover" and sub in ("lane violation", "offensive goaltending"):
                return True
            if (r.get("event_type") or "") == "violation" and sub == "double lane":
                return True
    return False


def _foul_awards_exactly_one_ft(ctx: EventContext, foul_idx: int) -> bool:
    """Approximates pbpstats' ``number_of_fta_for_foul == 1`` (foul.py:18-77)
    via a co-clock free-throw's trip shape, since this v3-native engine has
    no direct FTA-count field on the Foul row itself.
    """
    rows = ctx.rows
    foul_row = rows[foul_idx]
    for j in ctx.co_clock(foul_idx):
        r = rows[j]
        if (r.get("event_type") or "") != "free_throw" or is_technical_ft_row(r):
            continue
        shape = _ft_trip_shape(r)
        if shape is not None and shape[1] == 1 and r.get("team_id") != foul_row.get("team_id"):
            return True
    return False


def _make_ends_possession_not_1_foul(ctx: EventContext, i: int, foul_idxs: "list[int]") -> bool:
    """# pbpstats: field_goal.py:283-318 (_check_if_make_ends_possession_when_not_1_foul)."""
    rows = ctx.rows
    row = rows[i]
    shooter_team = row.get("team_id")
    foul_teams = {rows[j].get("team_id") for j in foul_idxs}

    if shooter_team not in foul_teams:
        co = ctx.co_clock(i)
        ft11 = [
            j
            for j in co
            if (rows[j].get("event_type") or "") == "free_throw"
            and _ft_trip_shape(rows[j]) == (1, 1)
            and not is_technical_ft_row(rows[j])
        ]
        if len(ft11) == 1:
            if rows[ft11[0]].get("team_id") == shooter_team:
                return True
        elif len(ft11) > 1:
            return any(rows[j].get("person_id") == row.get("person_id") for j in ft11)
    else:
        opponent_fouls = [j for j in foul_idxs if rows[j].get("team_id") != shooter_team]
        if any(_foul_awards_exactly_one_ft(ctx, j) for j in opponent_fouls):
            return True
    return False


def is_make_that_does_not_end_possession(ctx: EventContext, i: int) -> bool:
    """True if a made shot does NOT end the current possession.

    # pbpstats: field_goal.py:320-343 (is_make_that_does_not_end_possession)
    # + 223-318 (the exactly-one-foul / not-one-foul helpers) + consumer
    # suppression at stats_nba/enhanced_pbp_item.py:220-232 (flagrant-
    # pending, folded into this callable per the Task 3 brief rather than
    # left at pbpstats' original call-site, so this one function fully
    # answers "does this make end the possession").

    Covers the classic and-1 (exactly one co-clock shooting foul + a 1-of-1
    FT by the shooter's team), the rarer flagrant-and-1 / lane-violation /
    offensive-goaltending / double-lane-violation edges, and the
    flagrant-drawn-next suppression (a flagrant foul by the defense at the
    exact instant of the make, before any FT resolves).
    """
    rows = ctx.rows
    row = rows[i]
    fouls = _fouls_at_shot_time(ctx, i)
    if len(fouls) == 1:
        suppressed = _make_ends_possession_1_foul(ctx, i, fouls[0])
    else:
        suppressed = _make_ends_possession_not_1_foul(ctx, i, fouls)
    if suppressed:
        return True

    nxt = i + 1
    if nxt < len(rows):
        nrow = rows[nxt]
        if (
            (nrow.get("event_type") or "") == "foul"
            and _norm(nrow.get("sub_type")) in _FOUL_FLAGRANT
            and nrow.get("team_id") != row.get("team_id")
            and _same_instant(rows, i, nxt)
        ):
            return True
    return False


def _is_away_from_play_ft(ctx: EventContext, i: int) -> bool:
    """# pbpstats: free_throw.py:101-145 (is_away_from_play_ft), team-level port.

    v3 exposes ``person_id`` for the FT shooter (but no assist/block/steal
    secondary ids), so the "same player" tie-breaks below use ``person_id``
    directly; team-level fallbacks are used only where pbpstats itself
    compares team ids.
    """
    rows = ctx.rows
    row = rows[i]
    if _ft_trip_shape(row) not in ((1, 1), (2, 2)):
        return False
    foul_idx = foul_that_led_to_ft(ctx, i)
    if foul_idx < 0 or _norm(rows[foul_idx].get("sub_type")) not in _FOUL_AWAY_FROM_PLAY:
        return False

    co = ctx.co_clock(i)
    made_shots = [j for j in co if j != i and (rows[j].get("event_type") or "") == "made_shot"]
    other_player_fts = [
        j
        for j in co
        if j != i
        and (rows[j].get("event_type") or "") == "free_throw"
        and rows[j].get("person_id") != row.get("person_id")
    ]
    if not made_shots:
        if not other_player_fts:
            return True
        return any(rows[j].get("team_id") != row.get("team_id") for j in other_player_fts)

    first_make = min(made_shots)
    return bool(
        rows[first_make].get("team_id") == rows[foul_idx].get("team_id")
        and row.get("person_id") != rows[first_make].get("person_id")
    )


def _ft_1_of_1_co_clock_foul_in(ctx: EventContext, i: int, subtypes: "frozenset[str]") -> bool:
    """Shared shape for ``_is_inbound_foul_ft`` / ``_is_transition_take_foul_ft``
    (both pbpstats: free_throw.py -- ft-1-of-1-gated co-clock foul-type
    checks)."""
    rows = ctx.rows
    row = rows[i]
    if _ft_trip_shape(row) != (1, 1):
        return False
    for j in ctx.co_clock(i):
        if j == i:
            continue
        r = rows[j]
        if (r.get("event_type") or "") == "foul" and _norm(r.get("sub_type")) in subtypes:
            return True
    return False


def _is_inbound_foul_ft(ctx: EventContext, i: int) -> bool:
    """# pbpstats: free_throw.py:147-158 (is_inbound_foul_ft).

    ``_FOUL_INBOUND`` is an empty frozenset (v3 carries no discriminant --
    see module docstring), so this always returns False for v3 data, which
    is the conservative direction.
    """
    return _ft_1_of_1_co_clock_foul_in(ctx, i, _FOUL_INBOUND)


def _is_transition_take_foul_ft(ctx: EventContext, i: int) -> bool:
    """# pbpstats: free_throw.py:160-171 (is_transition_take_foul_ft)."""
    return _ft_1_of_1_co_clock_foul_in(ctx, i, _FOUL_TRANSITION_TAKE)


def ft_ends_possession(ctx: EventContext, i: int) -> bool:
    """Made last-FT boundary with the four exceptions.

    # pbpstats: stats_nba/enhanced_pbp_item.py:234-252; free_throw.py:101-171.
    Made (score-string signal) AND is_last_ft_of_trip AND NOT technical AND
    NOT (away-from-play / inbound-1of1 / transition-take-1of1 /
    foul-drawn-at-FT-time). Resolves the originating foul via
    foul_that_led_to_ft for the type checks; an unknown foul subType at
    FT-drawn-time is conservatively treated as "not one of the 4 suppressing
    types" (the boundary stands) and counted in UNKNOWN_SUBTYPE_COUNTER.
    """
    rows = ctx.rows
    row = rows[i]
    if (row.get("event_type") or "") != "free_throw":
        return False
    made = bool((row.get("score_home") or "").strip() or (row.get("score_away") or "").strip())
    if not made:
        return False
    if not is_last_ft_of_trip(row):
        return False
    if is_technical_ft_row(row):
        return False
    if _is_away_from_play_ft(ctx, i):
        return False
    if _is_inbound_foul_ft(ctx, i):
        return False
    if _is_transition_take_foul_ft(ctx, i):
        return False

    nxt = i + 1
    if nxt < len(rows):
        nrow = rows[nxt]
        if (
            (nrow.get("event_type") or "") == "foul"
            and nrow.get("team_id") != row.get("team_id")
            and _same_instant(rows, i, nxt)
        ):
            _note_unknown_foul_sub_type(nrow)
            if _norm(nrow.get("sub_type")) in _FOUL_NEXT_EVENT_SUPPRESS:
                return False
    return True


def _is_start_of_period(row: dict) -> bool:
    """v3's period-start marker row (event_type "period", sub_type "start")
    -- the closest v3-native analogue of pbpstats' ``StartOfPeriod`` class,
    used only to replicate the ``not isinstance(previous_event,
    StartOfPeriod)`` guard on ``jump_ball_ends_possession``
    (stats_nba/enhanced_pbp_item.py:254-256).
    """
    return (row.get("event_type") or "") == "period" and _norm(row.get("sub_type")) == "start"


def _is_local_possession_boundary(ctx: EventContext, j: int) -> bool:
    """Team-scoped approximation of the full is_possession_ending_event
    dispatcher, used ONLY by ``jump_ball_ends_possession``'s backward walk
    for "the previous possession boundary".

    ``jump_ball_ends_possession`` -- per its own interface -- has no
    ``offense_team_id`` / ``home_id`` / ``away_id`` to thread through a full
    dispatcher call, so this local helper resolves each candidate boundary
    from row-local fields only (``team_id`` equality, never
    ``resolve_event_team``). Falls back to False whenever team resolution
    is ambiguous, matching the brief's own guidance ("Default False
    (non-boundary) when the tail cannot resolve teams").
    """
    rows = ctx.rows
    row = rows[j]
    et = row.get("event_type") or ""
    if et == "made_shot":
        return not is_make_that_does_not_end_possession(ctx, j)
    if et == "turnover":
        return not is_no_turnover(row)
    if et == "free_throw":
        return ft_ends_possession(ctx, j)
    if et == "rebound":
        if not is_real_rebound(ctx, j):
            return False
        k = _rebound_missed_shot_index(ctx, j)
        if k < 0:
            return False
        reb_team = row.get("team_id")
        shot_team = rows[k].get("team_id")
        if not reb_team or not shot_team:
            return False
        return bool(reb_team != shot_team)
    if et == "jump_ball":
        return jump_ball_ends_possession(ctx, j)
    return False


def jump_ball_ends_possession(ctx: EventContext, i: int) -> bool:
    """True for the rare jump ball that itself changes possession.

    # pbpstats: stats_nba/enhanced_pbp_item.py:254-299
    # (_is_jump_ball_possession_ending_event) + the
    # ``not isinstance(previous_event, StartOfPeriod)`` guard at the call
    # site (lines 254-256), folded in here since this callable owns the
    # full jump-ball boundary decision per the Task 3 brief.

    Guard clauses (return False, no possession change needed via this path):
    a co-clock real turnover immediately before or after the jump ball (the
    turnover itself is the boundary), a jump-ball-violation next event (the
    ensuing turnover is the boundary), or a foul immediately after that
    itself leads to a co-clock turnover. Otherwise walks back to the
    previous local possession boundary and compares the jump ball's winning
    team against who started that possession with the ball; the jump ball
    is possession-ending only when the winning team did NOT start with the
    ball AND the next event isn't a real rebound or another jump ball
    (either of which would themselves carry the boundary).
    """
    rows = ctx.rows
    row = rows[i]
    if (row.get("event_type") or "") != "jump_ball":
        return False
    if i > 0 and _is_start_of_period(rows[i - 1]):
        return False

    if i + 1 < len(rows):
        nrow = rows[i + 1]
        net = nrow.get("event_type") or ""
        if net == "turnover" and not is_no_turnover(nrow) and _same_instant(rows, i, i + 1):
            return False
        if net == "violation" and _norm(nrow.get("sub_type")) == "jump ball":
            return False
        if net == "foul" and _same_instant(rows, i, i + 1) and i + 2 < len(rows):
            n2 = rows[i + 2]
            if (n2.get("event_type") or "") == "turnover" and not is_no_turnover(n2) and _same_instant(rows, i, i + 2):
                return False
    if i > 0:
        prow = rows[i - 1]
        if (prow.get("event_type") or "") == "turnover" and not is_no_turnover(prow) and _same_instant(rows, i, i - 1):
            return False

    jump_ball_winning_team_id = row.get("team_id")

    prev_j = i - 1
    while prev_j >= 0 and not _is_local_possession_boundary(ctx, prev_j):
        prev_j -= 1
    if prev_j < 0:
        return False

    prev_row = rows[prev_j]
    prev_team = prev_row.get("team_id")
    if (prev_row.get("event_type") or "") == "rebound":
        started_with_ball = jump_ball_winning_team_id == prev_team
    else:
        if not jump_ball_winning_team_id or not prev_team:
            return False
        started_with_ball = jump_ball_winning_team_id != prev_team

    next_is_real_rebound = (
        i + 1 < len(rows) and (rows[i + 1].get("event_type") or "") == "rebound" and is_real_rebound(ctx, i + 1)
    )
    next_is_jump_ball = i + 1 < len(rows) and (rows[i + 1].get("event_type") or "") == "jump_ball"

    if not started_with_ball and not (next_is_real_rebound or next_is_jump_ball):
        return True
    return False


def is_possession_ending_event(ctx: EventContext, i: int, offense_team_id: int, home_id: int, away_id: int) -> bool:
    """Dispatcher: made shot (unless non-ending make) | real defensive rebound |
    real turnover | made-last-FT (with exceptions) | jump-ball rare case.

    # pbpstats: stats_nba/enhanced_pbp_item.py:200-259.
    """
    row = ctx.rows[i]
    et = row.get("event_type") or ""
    if et == "made_shot":
        return not is_make_that_does_not_end_possession(ctx, i)
    if et == "turnover":
        return not is_no_turnover(row)
    if et == "rebound":
        if not is_real_rebound(ctx, i):
            return False
        reb_team = resolve_event_team(row, home_id, away_id)
        return offense_team_id != 0 and reb_team != 0 and reb_team != offense_team_id
    if et == "free_throw":
        return ft_ends_possession(ctx, i)
    if et == "jump_ball":
        return jump_ball_ends_possession(ctx, i)
    return False
