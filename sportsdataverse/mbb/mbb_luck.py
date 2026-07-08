"""3P luck-adjustment engine (regress-to-baseline shooting luck) + application glue.

Faithful port of hoop-explorer's ``LuckUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/LuckUtils.ts``, 950 LOC). Task 2.4 (Phase 2) ported the
**offensive** half -- ``calcOffTeamLuckAdj`` / ``calcOffPlayerLuckAdj`` and
the 3P-shot-decomposition helpers they depend on
(``build3PShotInfo`` / ``buildAdjusted3P`` / ``buildExp3P``, plus the
generalized ``buildShotInfo`` / ``buildAdjustedFG`` they wrap). Task 2.5
("LuckUtils port, 2/2") completes the module with the **defensive**
counterpart (``calcDefTeamLuckAdj`` / ``calcDefPlayerLuckAdj``) and the
mutate-in-place ``injectLuck`` -- plus a scoped port of the
``OverrideUtils.overrideMutableVal`` primitive ``injectLuck`` calls 8 times
(``src/utils/stats/OverrideUtils.ts``), since no Python port of it existed
anywhere in this codebase (grep-confirmed at the start of this task). See
"Deferred beyond this task" below for what's still out of scope.

Apache-2.0 third-party port — see the ``NOTICE`` file at the repository root for the upstream copyright and full attribution.

**Single-sourcing correction (brief vs. actual code -- TS/actual-code
governs).** The task brief instructed moving a canonical
``LUCK_AFFECTED_FIELDS`` set out of ``mbb_lineup_stats._LUCK_AFFECTED_FIELDS``
-- but no such private constant exists in ``mbb_lineup_stats.py`` (confirmed
by reading the whole module). The actual duplicate lives in
``tests/mbb/_hoop_explorer_replay.py`` (a hardcoded ``LUCK_AFFECTED_FIELDS``
frozenset backing that test-helper module's :func:`~tests.mbb._hoop_explorer_replay.insert_old_values`,
itself a Python replay of the jest test file's local ``insertOldValues``
helper). Cross-checked against ``LuckUtils.ts:159-169``
(``LuckUtils.affectedFieldSet``) and its only two upstream consumers
(both jest-test-local ``insertOldValues`` helpers in
``LineupUtils.test.ts:16`` and ``RapmUtils.test.ts:445`` -- ``affectedFieldSet``
has **no production TS consumer**, only test-helper ones). This module now
owns the canonical copy as :data:`LUCK_AFFECTED_FIELDS` (public, matching the
upstream field's own visibility -- it's a ``static readonly`` class member,
not module-private), and ``tests/mbb/_hoop_explorer_replay.py`` imports it
rather than keeping its own literal. No circular import: the test helper
module already only ever imports *from* production modules, never the
reverse, so this is a plain one-directional import, not the
alias-with-fallback design the brief anticipated for a hypothetical
circularity.

Ported behavior (``LuckUtils.ts`` anchors):

- :data:`LUCK_AFFECTED_FIELDS` -- verbatim copy of ``affectedFieldSet``
  (``LuckUtils.ts:159-169``): the 9 stat-set keys a luck adjustment can
  touch (``off_adj_ppp``, ``off_ppp``, ``off_efg``, ``off_3p``,
  ``def_adj_ppp``, ``def_ppp``, ``def_efg``, ``def_3p``, ``oppo_def_3p``).
- :func:`_luck_get` -- private port of the module-level ``LuckUtils.get``
  utility (``LuckUtils.ts:655-660``): *prefers* a field's ``old_value`` over
  its ``value`` when present (falls back to ``value``, then to the caller's
  ``fallback``). This is a **different** null-coalescing rule from
  :func:`~sportsdataverse.mbb.mbb_lineup_stats._num` (which always reads
  ``value``) -- ``LuckUtils.get`` exists specifically so that re-running a
  luck calculation over an *already luck-adjusted* stat set uses the
  original (pre-adjustment) raw numbers rather than double-adjusting.
  Nearly every field read inside :func:`calc_off_team_luck_adj` goes through
  this helper; the few that don't (``adjustedTO``/``adjusted2P``/
  ``adjustedFT`` and the ``off_ppp`` term inside ``deltaPtsLostFromTos``)
  are called out explicitly in the docstring below because the TS source
  itself uses a plain ``field?.value || 0`` there instead.
- :func:`_deserialize_lineup_sum` -- private port of the ``calcOffTeamLuckAdj``-
  local ``deserializeLineupSum`` closure (``LuckUtils.ts:220-226``): unpacks
  a single JS number into 5 unsigned 10-bit fields (one per lineup-slot
  index 0-4), used when a *lineup* aggregate carries pre-summed shot-info
  counts across up to 5 players packed into one ``Statistic`` value (bit
  layout: slot ``i`` occupies bits ``[10*i, 10*i+10)``, masked with
  ``0x3ff``). Ported as ``(int(value) >> (10*index)) & 0x3ff`` -- an exact
  integer right-shift, which is equivalent to the TS's
  float-division-then-``ToInt32``-truncation for the non-negative
  integer-valued packed sums this function is fed (all values fit safely
  within a 50-bit envelope, well inside JS's 53-bit safe-integer range), and
  is clearer than replicating the float-divide-then-truncate dance. The TS
  wraps the 5-element list in a spurious ``{value: [...]}`` object purely so
  the call site can immediately unwrap it via ``.value``; this port skips
  that wrapper and returns the list directly. **Not exercised by this
  task's vendored oracle fixtures** -- the vendored ``sampleTeamStatsResponse``
  team buckets carry no ``shot_info_*`` aggregate fields (confirmed:
  ``hasLineupInfo`` stays ``False`` for every jest call this task replays),
  so :func:`calc_off_team_luck_adj` always falls through to the
  per-player (non-lineup-aggregate) branch in every oracle test. Verified
  independently via a synthetic hand-built round-trip test in
  ``tests/mbb/test_mbb_luck.py`` instead.
- :func:`_build_shot_info` -- private port of the generic ``buildShotInfo``
  (``LuckUtils.ts:669-738``), parameterized on ``shot_type`` (``"3p"`` /
  ``"2pmid"`` / ``"2prim"``) and ``separate_half_court``. Decomposes a
  player's makes/misses at one shot location into assisted / unassisted /
  early(-transition) / scramble / unknown-missed buckets. Kept private for
  this task -- its only other upstream consumer, ``PlayTypeUtils.ts:1296``
  (calling it with ``separateHalfCourt=True`` for play-type decomposition,
  a different numeric surface with no Python port yet), is out of this
  task's scope. Every call site in this module invokes it with
  ``shot_type="3p"`` (via :func:`build_3p_shot_info`); the ``"2pmid"``/
  ``"2prim"`` branches are ported faithfully (needed for TS-signature
  parity / the future ``PlayTypeUtils`` port) but untested by this task's
  oracle.
- :func:`build_3p_shot_info` -- **public** port of ``build3PShotInfo``
  (``LuckUtils.ts:741-759``): the 3P-only wrapper around
  :func:`_build_shot_info`, remapping its generic ``shot_info_*_made``/
  ``*_attempts`` keys to the ``OffLuckShotInfo3P``-shaped ``shot_info_*_3pm``/
  ``shot_info_total_3p`` names ``calc_off_team_luck_adj`` and
  :func:`build_adjusted_3p` consume.
- :func:`_build_adjusted_fg` -- private port of the generic
  ``buildAdjustedFG`` (``LuckUtils.ts:761-808``): estimates a player's
  unassisted/assisted FG% at one shot location from their season-long
  shooting split, with small-sample regression toward a location-specific
  regression target (``regressionPct``) once ``total_shots_taken <
  regress_number`` (every jest call in this task's oracle passes the
  default ``regress_number=0``, so that branch is untested here too, ported
  for parity). Kept private for the same reason as :func:`_build_shot_info`
  (only other consumer is the un-ported ``buildAdjustedFG`` fallback path
  inside ``buildAdjusted3P``'s own general form -- there is no other
  upstream caller of the generic function outside ``buildAdjusted3P``
  itself, confirmed by grep).
- :func:`build_adjusted_3p` -- **public** port of ``buildAdjusted3P``
  (``LuckUtils.ts:812-835``): the 3P-only wrapper around
  :func:`_build_adjusted_fg`, "retained for bwc [backwards compat]" per the
  upstream comment.
- :func:`build_exp_3p` -- **public** port of ``buildExp3P``
  (``LuckUtils.ts:838-847``): ``(assisted 3PM * assisted3P%) +
  (unassisted 3PM * unassisted3P%) + (early/scramble/unknown 3PA *
  base3P%)`` -- the player's *expected* made-3P count given their
  shot-type mix and (baseline-derived) shooting percentages. No division,
  so it introduces no landmine.
- :func:`calc_off_team_luck_adj` -- **public** port of ``calcOffTeamLuckAdj``
  (``LuckUtils.ts:190-399``), the headline function. For a "sample" period
  (e.g. an on/off split or a single lineup) vs. a "base" period (e.g. the
  full season), regresses the sample's 3P% toward the base 3P%
  (possession-weighted Bayesian shrink, per player and in aggregate:
  ``regress3P = (sampleBase3P*base3PA + sample3P*sample3PA) /
  (sample3PA+base3PA)``), then propagates that 3P%-luck delta through eFG%,
  miss%, an ORB-recovery multiplier, and finally raw + SoS-adjusted PPP
  (``deltaOffPpp`` / ``deltaOffAdjEff``). Supports an optional
  ``sample_3pa_override`` (used when the caller wants to force the 3PA
  denominator, e.g. player-luck delegation with a team-level 3PA) and
  optional ``manual_overrides`` (per-player 3P%-expectation overrides from
  the UI, keyed by ``rowId``/``off_3p``/``newVal`` -- see
  ``ManualOverride``). When ``manual_overrides`` is a non-``None`` list
  (**including an empty one**, matching JS's own-array-is-truthy semantics
  -- see the landmine-adjacent note below), the team-level manual
  ``off_to``/``off_2p``/``off_ft`` override deltas are additionally folded
  into ``deltaOffEfg``/``deltaOffPpp``.
- :func:`calc_off_player_luck_adj` -- **public** port of
  ``calcOffPlayerLuckAdj`` (``LuckUtils.ts:174-187``). Per Task 2.1's
  surprise #4, this is a **literal 1-player-team delegation** to
  :func:`calc_off_team_luck_adj` (``calc_off_team_luck_adj(sample_player,
  [sample_player], base_player, {base_player["key"]: base_player},
  avg_eff)``) -- confirmed by the jest test's own assertion
  (``offPlayerLuckAdj == offTeamLuckAdj`` for matching inputs). There is
  only one real implementation; this wrapper exists purely for call-site
  convenience upstream.
- :data:`LUCK_PCT` -- verbatim ``0.66`` from ``calcDefTeamLuckAdj``'s local
  ``const luckPct`` (``LuckUtils.ts:444``): the SoS-vs-luck split weight the
  defensive engine uses to decide how much of a team's *observed* opponent-
  3P% delta to attribute to "the schedule was tougher/easier than average"
  (SoS) versus "the opponents just ran hot/cold" (luck) -- ``(1 -
  LUCK_PCT)`` of the raw 3P%-minus-SoS-baseline gap gets banked as
  ``base3PSosAdj``/``sample3PSosAdj`` per period, and the two periods'
  contributions are then blended by attempts-share (``avg3PSosAdj``) before
  being added back onto the SoS baseline to get ``adjDef3P``. Promoted to a
  module-level constant (rather than a function-local literal) so it's also
  surfaced verbatim in :func:`calc_def_team_luck_adj`'s returned
  diagnostics dict (TS returns it as the ``luckPct`` key).
- :func:`calc_def_team_luck_adj` -- **public** port of
  ``calcDefTeamLuckAdj`` (``LuckUtils.ts:429-531``), the defensive
  counterpart of :func:`calc_off_team_luck_adj`. Regresses a team's
  *opponent* 3P% toward its SoS-implied baseline (not toward a "base
  period" the way the offensive engine regresses toward a full-season
  split -- see the SoS-vs-luck-split note above), then propagates that
  delta through eFG%, miss%, an ORB-recovery multiplier, and raw + SoS-
  adjusted defensive PPP, mirroring the offensive engine's back half almost
  field-for-field. Reuses :func:`_luck_get` (no new fallback helper
  needed). Supports ``sample_def_3pa_override`` for the same lineup-
  regression reason :func:`calc_off_team_luck_adj`'s ``sample_3pa_override``
  exists (see that function's docstring).
- :func:`calc_def_player_luck_adj` -- **public** port of
  ``calcDefPlayerLuckAdj`` (``LuckUtils.ts:402-426``). **Not** a pure
  delegation (unlike the offensive player function) -- it has its own
  ``translate()`` closure that remaps a player's
  ``oppo_total_def_3p_made``/``oppo_total_def_3p_attempts`` counts into a
  computed ``def_3p`` ratio (there is no player-level ``def_3p`` field the
  way there's a team-level one) plus 3 renamed passthroughs
  (``def_3p_opp``, ``def_poss``, ``total_def_3p_attempts``), before
  delegating to :func:`calc_def_team_luck_adj`. The TS closure's ``key:
  sample.key`` is ported verbatim (both the sample- and base-side
  ``translate()`` calls stamp the *sample*'s key, a closure-capture quirk
  in the original) even though :func:`calc_def_team_luck_adj` never reads
  ``key`` -- harmless, kept for fidelity.
- :func:`inject_luck` -- **public** port of ``injectLuck``
  (``LuckUtils.ts:534-650``): the reversible mutate-in-place application of
  both offensive and defensive luck deltas onto a stat set (team, lineup,
  or player), via 8 calls to the newly-ported :func:`_override_mutable_val`.
  Idempotent (re-running with the same diagnostics is a no-op) and
  reversible (``inject_luck(stats, None, None)`` restores the pre-luck
  values) -- see the landmine notes below for the mechanics.
- :func:`_override_get_original_val` / :func:`_override_get_ignore_nil` /
  :func:`_override_mutable_val` -- private ports of
  ``OverrideUtils.getOriginalVal`` / ``OverrideUtils.getIgnoreNil`` /
  ``OverrideUtils.overrideMutableVal`` (``OverrideUtils.ts:80-90,409-457``),
  scoped to exactly what :func:`inject_luck` needs -- see the "OverrideUtils
  scope" note below for what's deliberately *not* ported.

**JS truthiness landmine specific to this module: ``manualOverrides ?
... : 0``.** ``LuckUtils.ts:336,342,348`` gate ``deltaTO``/``delta2P``/
``deltaFT`` on the truthiness of the ``manualOverrides`` *parameter itself*
(not the derived ``manual3pPct`` map) -- and in JS, **an empty array is
truthy**. So a caller passing ``manual_overrides=[]`` still takes the
"apply team override deltas" branch (with ``deltaTO`` etc. computed from
whatever ``old_value``s already sit on ``sample_team``), exactly as if a
non-empty override list had been passed; only ``manual_overrides=None``
(JS ``undefined``, the omitted-argument default) takes the zero branch.
This is ported as an explicit ``is not None`` check, per this project's
"``is None`` for ``!x`` null-guards" convention -- a naive
``if manual_overrides:`` would silently diverge from upstream for the
empty-list case (Python ``[]`` is falsy, unlike JS).

**Object-selection ``||`` vs. numeric-fallback ``||`` (the other recurring
landmine in this port).** Two separate spots in ``calcOffTeamLuckAdj`` use
``||`` to pick between two *objects* rather than to supply a numeric
default: ``basePlayersMap[player.key] || player`` (``LuckUtils.ts:268``) and
``basePlayerStats || {}`` (``LuckUtils.ts:278``). In JS, any object
(including ``{}``) is truthy, so these are "use the left value if it
exists, else the right" -- but Python's ``or`` treats an empty dict as
falsy, which would silently substitute the fallback even when
``base_players_map[player["key"]]`` legitimately resolves to ``{}``. Both
spots are ported as explicit ``is not None`` ternaries, not ``or``.

**Known unguarded-division landmines (preserved for fidelity).** Matching
the project's no-NaN-emulation policy (see ``mbb_ratings.py``'s module
docstring for the general policy statement):

1. ``deltaOffOrbFactor = (deltaMissesPct * sampleOffOrb) / (1 -
   deltaMissesPct * sampleOffOrb)`` (``LuckUtils.ts:362-363``) has no
   guard on its denominator. JS would produce ``Infinity``/``NaN`` if
   ``deltaMissesPct * sampleOffOrb == 1``; Python raises
   ``ZeroDivisionError`` at that expression instead. Reachable only for a
   pathological combination of a very large 3P-luck delta and a
   near-maximal (capped at ``0.66``) offensive-rebound rate -- none of
   this task's oracle fixtures exercise it.
2. ``deltaDefOrbFactor = (deltaMissesPct * sampleDefOrb) / (1 -
   deltaMissesPct * sampleDefOrb)`` (``LuckUtils.ts:490-491``) -- the exact
   same shape, defensive side. Same non-guard, same
   ``ZeroDivisionError``-instead-of-``Infinity``/``NaN`` divergence, same
   "not reachable by this task's oracle fixtures" status.

**SoS-vs-luck-split nil-check landmine (defensive-only).**
``calcDefTeamLuckAdj``'s ``baseDef3PSos``/``sampleDef3PSos`` (``LuckUtils.ts
:450,458``) check ``_.isNil(base?.def_3p_opp?.value)`` -- this is a
*different* nil-check from :func:`_luck_get`'s (which prefers ``old_value``
over ``value``): it looks at **only** ``value``, ignoring any
``old_value``, to decide whether opponent-SoS data exists *at all* for this
team/period (falling back to the raw, non-SoS-adjusted ``baseDef3P``/
``sampleDef3P`` when it doesn't). Ported as the private
:func:`_field_value_is_nil` helper -- a plain ``field.get("value") is
None`` check -- rather than reusing :func:`_luck_get` or
:func:`~sportsdataverse.mbb.mbb_lineup_stats._num`, neither of which has
the right null-coalescing shape here.

**``injectLuck``/``overrideMutableVal`` landmines.**

1. **Absolute value vs. delta, per call site.** 6 of the 8
   :func:`_override_mutable_val` calls in :func:`inject_luck` wrap their
   diagnostic in ``maybe_delta(...)`` (-> ``{"delta": val}``, added to the
   field's original value) -- but the two ``def_3p``/``oppo_def_3p`` calls
   (``LuckUtils.ts:577-582,618-623``) pass ``defLuck?.adjDef3P`` **directly,
   unwrapped**, i.e. an *absolute* override value, not a delta. This is a
   real distinction in the upstream source (not an oversight) --
   ``adjDef3P`` is already the fully-adjusted opponent-3P%, whereas the
   other diagnostics are deltas-from-baseline -- and is preserved verbatim
   in this port (see :func:`inject_luck`'s body).
2. **Player- vs. team-shaped stats gate is JS object-truthiness.**
   ``if (mutableStats.oppo_total_def_3p_made)`` (``LuckUtils.ts:609``) gates
   the player-only ``oppo_def_3p`` recompute + override on whether that key
   is present at all on the mutable stat set (a team/lineup stat set never
   carries it) -- ported as ``mutable_stats.get("oppo_total_def_3p_made")
   is not None``, not a Python dict-truthy check (an empty ``{}`` sentinel
   value would be truthy in JS but falsy in Python).
3. **``getIgnoreNil``'s "leave it alone" collapse.**
   :func:`_override_mutable_val` discards its own freshly-computed
   ``value``/``old_value``/``override`` and replaces them with
   ``{"value": None}`` whenever the resolved *original* value was nil (no
   real baseline existed for that field on entry) -- i.e. it refuses to
   fabricate a luck override on top of a field with no prior data, per the
   upstream doc comment ("If the old value was nil we leave it alone").
   Unexercised by this task's oracle (every field :func:`inject_luck`
   touches in the vendored fixtures already carries a real ``value``), but
   ported faithfully since :func:`_override_mutable_val` is a shared
   primitive future callers may exercise differently.
4. **``noOverwrite`` guard (reason-mismatch protection).** A *reset* call
   (``new_val=None``) is silently skipped -- the field is left exactly as
   -is -- if it currently carries a *different*, non-``None`` override
   ``reason`` than the one the caller is resetting with (e.g. a manual
   override sitting on top of what would otherwise be a luck-adjustment
   reset). Both ``reason`` values must be non-``None`` and unequal for the
   skip to trigger; ``inject_luck``'s own calls always pass a fixed,
   non-``None`` ``reason`` string, so this only matters once a *different*
   override mechanism (manual overrides, Phase 3) writes to the same
   fields.

**OverrideUtils scope note.** Only ``OverrideUtils.overrideMutableVal``
(plus its two small dependencies, ``getOriginalVal``/``getIgnoreNil``) is
ported -- the only ``OverrideUtils`` member :func:`inject_luck` calls. The
shot-quality (``sq_2prim``/``sq_2pmid``/``sq_3p``) key-remap branch inside
``overrideMutableVal`` itself (``OverrideUtils.ts:415-431``, needing
``OverrideUtils.getRimPctFromTs``) is **not** ported -- it's dead code from
:func:`inject_luck`'s perspective (none of its 8 call-site keys start with
``sq_``) and belongs to the manual shot-quality-override UI feature
(``RatingUtils``/``OverrideUtils.applyOverrides``), out of this task's
scope. :func:`_override_mutable_val` raises ``NotImplementedError`` if ever
called with a ``sq_``-prefixed key instead of silently mis-mutating. The
rest of ``OverrideUtils.ts`` (``applyOverrides``, ``applyPlayerOverridesToTeam``,
``updateDerivedStats``, ``handleShotSelection``, the shot-quality helpers) is
Phase-3 (manual-override UI) territory, not this task's.

**Deferred beyond this task (Phase 3 / a future ``PlayTypeUtils``/
``OverrideUtils`` port):**

- ``decomposeUnknownMisses`` / ``decomposeUnknown3PMisses``
  (``LuckUtils.ts:850-949``) -- splits a player's "unknown" (unclassified)
  misses into estimated assisted/unassisted buckets. Not called by
  ``calcOffTeamLuckAdj``/``calcOffPlayerLuckAdj`` (confirmed by reading the
  full offensive call graph); its only upstream consumers are
  ``PlayTypeUtils.ts:1311`` (a different, unported numeric module) and
  ``LuckAdjDiagView.tsx:163`` (a React display component, no numeric-engine
  analog -- out of scope per this port's UI-exclusion convention). Left
  for whichever future task ports ``PlayTypeUtils.ts``.
- ``affectedPartialFieldnames`` (``LuckUtils.ts:171``) -- a sibling list to
  :data:`LUCK_AFFECTED_FIELDS` (partial/substring field-name matches
  instead of exact keys). Its only upstream consumer is ``RapmUtils.ts:816``,
  a Phase-3 (RAPM) concern, not this task's.
- ``OverrideUtils.applyOverrides`` / ``applyPlayerOverridesToTeam`` /
  ``updateDerivedStats`` / ``handleShotSelection`` / ``buildOverrideAsMap`` /
  ``filterManualOverrides`` / the shot-quality helper family
  (``getRimPctFromTs``, ``getOldRimTs``, ``shotQualityKeyToKey``, ...) --
  the manual-override / shot-quality-adjustment UI feature surface, not
  needed by :func:`inject_luck`. A future task porting the manual-override
  UI path (or ``RatingUtils.buildOffOverrides``'s siblings) owns these.
"""

from __future__ import annotations

from typing import Any

from sportsdataverse.mbb.mbb_lineup_stats import LineupStatSet, _field_val, _num

__all__ = [
    "ManualOverride",
    "OffShotInfoBreakdown",
    "OffAdjShotBreakdown",
    "OffLuckShotInfo3P",
    "OffLuckAdj3P",
    "OffLuckShotTypeAndAdj3P",
    "OffLuckAdjustmentDiags",
    "DefLuckAdjustmentDiags",
    "LUCK_PCT",
    "LUCK_AFFECTED_FIELDS",
    "build_3p_shot_info",
    "build_adjusted_3p",
    "build_exp_3p",
    "calc_off_team_luck_adj",
    "calc_off_player_luck_adj",
    "calc_def_team_luck_adj",
    "calc_def_player_luck_adj",
    "inject_luck",
]

#: ``ManualOverride`` (``FilterModels.ts``) -- a UI-originated per-row
#: manual-value override: ``{"rowId": str, "statName": str, "newVal":
#: float, "use": bool}``. Kept as a plain dict alias (TS-verbatim keys),
#: matching this port's other ``*StatSet``-shaped type aliases.
ManualOverride = dict[str, Any]

#: ``OffShotInfoBreakdown`` (``LuckUtils.ts:20-27``) / ``OffAdjShotBreakdown``
#: (``LuckUtils.ts:30-36``) / ``OffLuckShotInfo3P`` (``LuckUtils.ts:39-46``) /
#: ``OffLuckAdj3P`` (``LuckUtils.ts:49-55``) -- all plain dict aliases, per
#: this port's convention of keeping TS ``type`` shapes as ``dict[str, Any]``
#: rather than dataclasses (see ``mbb_ratings.py``'s ``ORtgDiagnostics`` for
#: the precedent).
OffShotInfoBreakdown = dict[str, float]
OffAdjShotBreakdown = dict[str, float]
OffLuckShotInfo3P = dict[str, float]
OffLuckAdj3P = dict[str, float]

#: ``OffLuckShotTypeAndAdj3P`` (``LuckUtils.ts:57``, ``OffLuckShotInfo3P &
#: OffLuckAdj3P`` plus an ``expected3P?`` field stamped in later) --
#: ``float | dict`` because ``player3PInfo``'s per-player entries also nest.
OffLuckShotTypeAndAdj3P = dict[str, Any]

#: ``OffLuckAdjustmentDiags`` (``LuckUtils.ts:60-88``) --
#: :func:`calc_off_team_luck_adj`'s return shape; TS-verbatim keys (see
#: ``mbb_ratings.py``'s module docstring for why diagnostics-dict keys stay
#: un-snake_cased across this port).
OffLuckAdjustmentDiags = dict[str, Any]

#: ``DefLuckAdjustmentDiags`` (``LuckUtils.ts:91-118``, inferred from the
#: object literal :func:`calc_def_team_luck_adj` returns) -- TS-verbatim
#: keys, same rationale as :data:`OffLuckAdjustmentDiags`.
DefLuckAdjustmentDiags = dict[str, Any]

#: Verbatim from ``calcDefTeamLuckAdj``'s local ``const luckPct = 0.66``
#: (``LuckUtils.ts:444``) -- see the module docstring's "SoS-vs-luck-split"
#: note for what this weight controls. Promoted to a module constant (TS
#: keeps it function-local) so both :func:`calc_def_team_luck_adj`'s
#: computation and its returned ``luckPct`` diagnostic key reference the
#: same single value.
LUCK_PCT: float = 0.66

#: Verbatim from ``LuckUtils.ts:159-169`` (``LuckUtils.affectedFieldSet``).
#: The canonical, single-sourced copy of "every stat-set key a luck
#: adjustment can touch" -- see the module docstring's single-sourcing note
#: for where the pre-existing duplicate lived and why it now imports this.
LUCK_AFFECTED_FIELDS: frozenset[str] = frozenset(
    {
        "off_adj_ppp",
        "off_ppp",
        "off_efg",
        "off_3p",
        "def_adj_ppp",
        "def_ppp",
        "def_efg",
        "def_3p",
        "oppo_def_3p",
    }
)

#: Verbatim from ``LuckUtils.ts:147-152`` (``LuckUtils.lineupShotInfoFields``).
_LINEUP_SHOT_INFO_FIELDS: tuple[str, str, str, str] = (
    "ast_3pm",
    "unast_3pm",
    "early_3pa",
    "unknown_3pM",
)

#: Verbatim from ``LuckUtils.ts:153-156``
#: (``LuckUtils.lineupAggregatedShotInfoFields``) -- ``lineupShotInfoFields``
#: plus ``"scramble_3pa"``, each prefixed with ``"shot_info_"``.
_LINEUP_AGGREGATED_SHOT_INFO_FIELDS: tuple[str, ...] = tuple(
    f"shot_info_{name}" for name in (*_LINEUP_SHOT_INFO_FIELDS, "scramble_3pa")
)


def _luck_get(field: Any, fallback: float) -> float:
    """Port of the module-level ``LuckUtils.get`` utility (``LuckUtils.ts:655-660``).

    Prefers ``field["old_value"]`` over ``field["value"]`` when the field
    carries a non-nil ``old_value`` (i.e. it has already been luck-adjusted
    /overridden elsewhere) -- see the module docstring for why this differs
    from :func:`~sportsdataverse.mbb.mbb_lineup_stats._num`.

    Args:
        field: A ``Statistic``-shaped dict (or ``None``/non-dict).
        fallback: Returned when the resolved value is falsy (``0``
            included, matching JS ``||``) or ``field`` isn't a dict.

    Returns:
        ``field["old_value"]`` if present and non-nil, else
        ``field["value"]``, else ``fallback``.
    """
    if not isinstance(field, dict):
        return fallback
    old_value = field.get("old_value")
    resolved = field.get("value") if old_value is None else old_value
    return resolved if resolved else fallback


def _deserialize_lineup_sum(n: Any) -> list[int]:
    """Port of the ``calcOffTeamLuckAdj``-local ``deserializeLineupSum``
    closure (``LuckUtils.ts:220-226``). See the module docstring's
    bit-packing note for the algorithm and the simplification (no
    ``{"value": [...]}`` wrapper) taken here.

    Args:
        n: A ``Statistic``-shaped dict whose ``value`` is a single packed
            non-negative number (or ``None``/non-dict, folded to ``0``).

    Returns:
        A 5-element list of unsigned 10-bit ints, one per lineup-slot index
        ``0..4`` (least-significant 10 bits first).
    """
    packed = int(_field_val(n, "value", 0.0))
    return [(packed >> (10 * index)) & 0x3FF for index in range(5)]


def _build_shot_info(
    p: LineupStatSet,
    shot_type: str,
    separate_half_court: bool = False,
) -> OffShotInfoBreakdown:
    """Private port of the generic ``buildShotInfo`` (``LuckUtils.ts:669-738``).

    Decomposes a player's made/missed shots at one shot location
    (``shot_type``) into assisted / unassisted / early-transition /
    scramble / unknown-missed buckets. See the module docstring for why
    this stays private (only other consumer is the un-ported
    ``PlayTypeUtils.ts``).

    Args:
        p: The player's ``LineupStatSet``/``IndivStatSet``-shaped dict.
        shot_type: ``"3p"``, ``"2pmid"``, or ``"2prim"``.
        separate_half_court: When ``True``, keeps transition/scramble
            assists in their own bin instead of folding them into the
            half-court ``shot_info_ast_made`` bucket (used by the
            play-type decomposition caller, not by this task's luck-engine
            call sites, which always pass the default ``False``).

    Returns:
        ``{"shot_info_ast_made", "shot_info_early_attempts",
        "shot_info_scramble_attempts", "shot_info_unast_made",
        "shot_info_unknown_missed", "shot_info_total_attempts"}``.
    """
    ast_trans_made = _num(p, f"total_off_trans_{shot_type}_ast", 0.0)
    unast_trans_made = max(_num(p, f"total_off_trans_{shot_type}_made", 0.0) - ast_trans_made, 0.0)
    early_attempts = max(
        _num(p, f"total_off_trans_{shot_type}_attempts", 0.0) - (0.0 if separate_half_court else ast_trans_made),
        0.0,
    )

    ast_scramble_made = _num(p, f"total_off_scramble_{shot_type}_ast", 0.0)
    unast_scramble_made = max(_num(p, f"total_off_scramble_{shot_type}_made", 0.0) - ast_scramble_made, 0.0)
    scramble_attempts = max(
        _num(p, f"total_off_scramble_{shot_type}_attempts", 0.0) - (0.0 if separate_half_court else ast_scramble_made),
        0.0,
    )

    # (this includes assisted transition and scramble shots, unless separate_half_court):
    ast_made = _num(p, f"total_off_{shot_type}_ast", 0.0) - (
        (ast_trans_made + ast_scramble_made) if separate_half_court else 0.0
    )

    if separate_half_court:
        made_subtracted = ast_made + ast_trans_made + ast_scramble_made + unast_trans_made + unast_scramble_made
    else:
        # (in this case, ast_made already includes HC/scramble/trans):
        made_subtracted = ast_made + unast_trans_made + unast_scramble_made
    unast_made = max(_num(p, f"total_off_{shot_type}_made", 0.0) - made_subtracted, 0.0)

    total_attempts = _num(p, f"total_off_{shot_type}_attempts", 0.0)
    unknown_missed = max(
        0.0,
        total_attempts - ast_made - early_attempts - scramble_attempts - unast_made,
    )
    return {
        "shot_info_ast_made": ast_made,
        "shot_info_early_attempts": early_attempts,
        "shot_info_scramble_attempts": scramble_attempts,
        "shot_info_unast_made": unast_made,
        "shot_info_unknown_missed": unknown_missed,
        "shot_info_total_attempts": total_attempts,
    }


def build_3p_shot_info(p: LineupStatSet) -> OffLuckShotInfo3P:
    """3P-only shot-decomposition wrapper.

    Public port of ``build3PShotInfo`` (``LuckUtils.ts:741-759``) --
    remaps :func:`_build_shot_info`'s generic keys to the ``_3pm``/
    ``_3pa``/``_3p`` suffixes used throughout the luck-adjustment engine.

    Args:
        p: The player's ``LineupStatSet``/``IndivStatSet``-shaped dict.

    Returns:
        ``{"shot_info_ast_3pm", "shot_info_early_3pa",
        "shot_info_scramble_3pa", "shot_info_unast_3pm",
        "shot_info_unknown_3pM", "shot_info_total_3p"}``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_luck import build_3p_shot_info

            info = build_3p_shot_info(player)
            print(info["shot_info_total_3p"])

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    info = _build_shot_info(p, "3p")
    return {
        "shot_info_ast_3pm": info["shot_info_ast_made"],
        "shot_info_early_3pa": info["shot_info_early_attempts"],
        "shot_info_scramble_3pa": info["shot_info_scramble_attempts"],
        "shot_info_unast_3pm": info["shot_info_unast_made"],
        "shot_info_unknown_3pM": info["shot_info_unknown_missed"],
        "shot_info_total_3p": info["shot_info_total_attempts"],
    }


def _build_adjusted_fg(
    p: LineupStatSet,
    base_shot_info: OffShotInfoBreakdown,
    shot_type: str,
    regress_number: float = 0.0,
) -> OffAdjShotBreakdown:
    """Private port of the generic ``buildAdjustedFG`` (``LuckUtils.ts:761-808``).

    Estimates a player's unassisted/assisted FG% at one shot location from
    their season-long shooting split, regressing toward a location-specific
    target when the sample (``base_shot_info``'s total attempts) is smaller
    than ``regress_number``. See the module docstring for why this stays
    private.

    Args:
        p: The player's dict -- read via :func:`_luck_get` (prefers
            ``old_value``), matching the TS ``LuckUtils.get(p?.[...], 0)``.
        base_shot_info: The ``OffShotInfoBreakdown`` (from
            :func:`_build_shot_info`) whose ``shot_info_total_attempts`` /
            ``shot_info_ast_made`` / ``shot_info_unast_made`` drive the
            assist-rate + regression-weight calc.
        shot_type: ``"3p"``, ``"2pmid"``, or ``"2prim"``.
        regress_number: Small-sample regression strength (``0`` disables
            regression -- every jest call in this task's oracle uses the
            default).

    Returns:
        ``{"base", "unassisted", "assisted", "baseAssistPct"}``.
    """
    base_pre = _luck_get(p.get(f"off_{shot_type}"), 0.0)

    # (ideally can't use off_3p_ast because some transition 3PAs are
    # unassisted makes, so we just use non-early ast%):
    fallback_base_assist_pct_pre = _luck_get(p.get(f"off_{shot_type}_ast"), 0.0)
    if fallback_base_assist_pct_pre == 0.0 and base_pre <= 0.2:
        # (fallback is that they only take assisted shots, because they're bad!)
        fallback_base_assist_pct = 1.0 if shot_type == "3p" else 0.5
    else:
        fallback_base_assist_pct = fallback_base_assist_pct_pre

    made_this_sample = base_shot_info["shot_info_ast_made"] + base_shot_info["shot_info_unast_made"]
    base_assist_pct = (
        base_shot_info["shot_info_ast_made"] / (made_this_sample or 1.0)
        if made_this_sample > 0
        else fallback_base_assist_pct
    )

    total_shots_taken = base_shot_info["shot_info_total_attempts"]
    regression_pct = 0.3 if shot_type == "3p" else (0.35 if shot_type == "2pmid" else 0.5)
    regress_weight = regress_number / ((total_shots_taken + regress_number) or 1.0)

    if total_shots_taken < regress_number:
        base = regress_weight * regression_pct + (1.0 - regress_weight) * base_pre
    else:
        base = base_pre

    weight = 0.06 if shot_type == "3p" else 0.1  # (totally arbitrary, per upstream comment)
    return {
        "base": base,
        "unassisted": base - base_assist_pct * weight,
        "assisted": base + (1 - base_assist_pct) * weight,
        "baseAssistPct": base_assist_pct,
    }


def build_adjusted_3p(p: LineupStatSet, info: OffLuckShotInfo3P) -> OffLuckAdj3P:
    """3P-only approx-unassisted/assisted-FG% wrapper.

    Public port of ``buildAdjusted3P`` (``LuckUtils.ts:812-835``, "retained
    for bwc [backwards compat]" per the upstream comment) -- a thin remap of
    :func:`_build_adjusted_fg` called with ``shot_type="3p"``.

    Args:
        p: The (typically base-period) player dict driving ``off_3p``/
            ``off_3p_ast``.
        info: An :func:`build_3p_shot_info`-shaped dict (the "biggest
            sample available" per the upstream comment -- normally the
            base period, not the sample being luck-adjusted).

    Returns:
        ``{"base3P", "unassisted3P", "assisted3P", "baseAssistPct"}``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_luck import build_3p_shot_info, build_adjusted_3p

            base_info = build_3p_shot_info(base_player)
            adj = build_adjusted_3p(base_player, base_info)
            print(adj["assisted3P"], adj["unassisted3P"])

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    adjusted = _build_adjusted_fg(
        p,
        {
            "shot_info_ast_made": info["shot_info_ast_3pm"],
            "shot_info_early_attempts": info["shot_info_early_3pa"],
            "shot_info_scramble_attempts": info["shot_info_scramble_3pa"],
            "shot_info_unast_made": info["shot_info_unast_3pm"],
            "shot_info_unknown_missed": info["shot_info_unknown_3pM"],
            "shot_info_total_attempts": info["shot_info_total_3p"],
        },
        "3p",
    )
    return {
        "base3P": adjusted["base"],
        "unassisted3P": adjusted["unassisted"],
        "assisted3P": adjusted["assisted"],
        "baseAssistPct": adjusted["baseAssistPct"],
    }


def build_exp_3p(info: OffLuckShotTypeAndAdj3P) -> float:
    """Expected made-3P count given a player's shot-type mix + shooting %s.

    Public port of ``buildExp3P`` (``LuckUtils.ts:838-847``): ``(assisted
    3PM * assisted3P%) + (unassisted 3PM * unassisted3P%) +
    (early/scramble/unknown 3PA * base3P%)``. Pure weighted sum -- no
    division, so this introduces no landmine.

    Args:
        info: A dict carrying both :func:`build_3p_shot_info`'s
            ``shot_info_*`` keys and :func:`build_adjusted_3p`'s
            ``*3P`` keys (i.e. an ``OffLuckShotTypeAndAdj3P``).

    Returns:
        The expected number of made 3-pointers (``3P% * total 3P``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_luck import (
                build_3p_shot_info, build_adjusted_3p, build_exp_3p,
            )

            base_info = build_3p_shot_info(base_player)
            info = {**build_3p_shot_info(player), **build_adjusted_3p(base_player, base_info)}
            expected_makes = build_exp_3p(info)

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return (
        info["shot_info_ast_3pm"] * info["assisted3P"]
        + info["shot_info_unast_3pm"] * info["unassisted3P"]
        + (info["shot_info_early_3pa"] + info["shot_info_scramble_3pa"] + info["shot_info_unknown_3pM"])
        * info["base3P"]
    )


def calc_off_team_luck_adj(
    sample_team: LineupStatSet,
    sample_players: list[LineupStatSet],
    base_team: LineupStatSet,
    base_players_map: dict[str, LineupStatSet],
    avg_eff: float,
    sample_3pa_override: float | None = None,
    manual_overrides: list[ManualOverride] | None = None,
) -> OffLuckAdjustmentDiags:
    """Offensive 3P-luck adjustment for a team (or lineup).

    Faithful port of ``LuckUtils.calcOffTeamLuckAdj`` (``LuckUtils.ts:190-399``).
    See the module docstring for the Bayesian-shrink formula, the JS-array-
    truthiness / object-selection landmines, and the one unguarded-division
    landmine this function carries.

    Args:
        sample_team: The team/lineup stat dict for the period being
            luck-adjusted (e.g. an on/off split or a single lineup).
        sample_players: The roster of per-player stat dicts backing
            ``sample_team`` (``samplePlayers == players.map(on/off/baseline)``
            per the upstream comment).
        base_team: The team stat dict for the baseline/reference period
            (typically full-season).
        base_players_map: ``{player_key: base_period_player_stat_dict}``.
        avg_eff: League/context average efficiency (``100`` in every
            vendored jest call).
        sample_3pa_override: When given, used as ``sample3PA`` instead of
            ``sample_team["total_off_3p_attempts"]``. Per the upstream
            comment (``LuckUtils.ts:196-198``, shared verbatim with
            :func:`calc_def_team_luck_adj`'s ``sample_def_3pa_override``):
            "when calc'ing luck on lineups, each lineup gets the total
            sample as its regression so its average is right over the
            set" -- i.e. this lets every lineup in a sweep share one common
            3PA denominator (the team's) for its regression target, rather
            than each lineup regressing against its own much smaller,
            noisier 3PA count. Note that :func:`calc_off_player_luck_adj`
            itself does *not* pass this (its delegation call omits it
            entirely) -- the jest oracle's own "3P override" cross-check
            (``LuckUtils.test.ts:100-115``) instead calls
            :func:`calc_off_team_luck_adj` directly with the player's own
            3PA as this override, purely to demonstrate the parameter's
            effect in isolation.
        manual_overrides: Per-player 3P%-expectation overrides from the UI.
            **A non-``None`` empty list still activates the team-level
            override-delta branch** (JS array truthiness) -- see the
            module docstring's landmine note. ``None`` (the default) is
            the "no overrides at all" case.

    Returns:
        An :data:`OffLuckAdjustmentDiags` dict -- TS-verbatim keys
        (``avgEff``, ``samplePoss``, ``sample3P``, ``sample3PA``,
        ``base3PA``, ``player3PInfo`` (per-player detail, sorted by
        descending ``shot_info_total_3p``), ``sampleBase3P``, ``regress3P``,
        ``sampleOff3PRate``, ``sampleOffFGA``, ``sampleOffOrb``,
        ``sampleOffEfg``, ``sampleOffPpp``, ``sampleDefSos``, ``delta3P``,
        ``deltaOffEfg``, ``deltaMissesPct``, ``deltaOffPppNoOrb``,
        ``deltaOffOrbFactor``, ``deltaPtsOffMisses``, ``deltaOffPpp``,
        ``deltaOffAdjEff``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_luck import calc_off_team_luck_adj

            diags = calc_off_team_luck_adj(
                sample_team_on, sample_players_on, base_team, base_players_map, 100.0,
            )
            print(diags["deltaOffAdjEff"])

        With per-player manual 3P% overrides::

            diags = calc_off_team_luck_adj(
                sample_team_on, sample_players_on, base_team, base_players_map, 100.0,
                manual_overrides=[
                    {"rowId": "Cowan, Anthony", "statName": "off_3p", "newVal": 0.5, "use": True},
                ],
            )

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    manual_3p_pct: dict[str, float] = {
        row["rowId"]: row["newVal"] for row in (manual_overrides or []) if row.get("statName") == "off_3p"
    }

    sample_poss = _luck_get(sample_team.get("off_poss"), 0.0)
    sample_3p = _luck_get(sample_team.get("off_3p"), 0.0)
    sample_3pa = (
        sample_3pa_override
        if sample_3pa_override is not None
        else _luck_get(sample_team.get("total_off_3p_attempts"), 0.0)
    )
    base_3pa = _luck_get(base_team.get("total_off_3p_attempts"), 0.0)

    # If we don't have roster but we do have lineup shot info, use that
    # instead (bit-packed 10-bit-per-slot aggregate fields -- see the
    # module docstring's bit-packing note; unexercised by this task's
    # vendored oracle):
    player_shot_info: dict[str, Any] = {"hasLineupInfo": False, "total": [0, 0, 0, 0, 0]}
    for field_name in _LINEUP_AGGREGATED_SHOT_INFO_FIELDS:
        raw = sample_team.get(field_name)
        if raw is not None:
            decoded = _deserialize_lineup_sum(raw)
            player_shot_info[field_name] = decoded
            player_shot_info["total"] = [a + b for a, b in zip(decoded, player_shot_info["total"])]
            player_shot_info["hasLineupInfo"] = player_shot_info["hasLineupInfo"] or (_field_val(raw, "value", 0.0) > 0)

    def _build_shot_lineup_info(
        base_player_stats: LineupStatSet,
        index: int,
        base_shot_info: OffLuckShotInfo3P,
    ) -> OffLuckShotTypeAndAdj3P:
        """Port of the ``calcOffTeamLuckAdj``-local ``buildShotLineupInfo``
        closure (``LuckUtils.ts:248-263``)."""
        acc: dict[str, Any] = {
            "shot_info_total_3p": (player_shot_info["total"][index] if index < len(player_shot_info["total"]) else 0)
            or 0,
            **build_adjusted_3p(base_player_stats, base_shot_info),
        }
        for field_name in _LINEUP_AGGREGATED_SHOT_INFO_FIELDS:
            values = player_shot_info.get(field_name)
            acc[field_name] = (values[index] if isinstance(values, list) and index < len(values) else None) or 0
        return acc

    var_total_3pa = 0.0
    var_total_3p = 0.0
    player_entries: list[tuple[str, OffLuckShotTypeAndAdj3P]] = []

    for index, player in enumerate(sample_players):
        # (JS `basePlayersMap[player.key] || player` / `basePlayerStats ||
        # {}` are object-selection, not numeric fallbacks -- see module
        # docstring's landmine note; ported as explicit `is not None`.)
        base_player_stats = base_players_map.get(player["key"])
        base_shot_info = build_3p_shot_info(base_player_stats if base_player_stats is not None else player)

        if index < 5 and player_shot_info["hasLineupInfo"] and base_player_stats is not None:
            player_info = _build_shot_lineup_info(base_player_stats, index, base_shot_info)
        else:
            player_info = {
                **build_3p_shot_info(player),
                **build_adjusted_3p(base_player_stats if base_player_stats is not None else {}, base_shot_info),
            }

        # NOTE: the TS `if (playerInfo) {...} else {return []}` else-branch
        # is unreachable dead code -- `playerInfo` is unconditionally an
        # object from the ternary above, so the "player not in this lineup"
        # branch never fires. Not ported (same style as Task 2.1's
        # `samplePlayerNeedingOverride` dead-code finding).
        var_total_3pa += player_info["shot_info_total_3p"]

        manual_3p_over = manual_3p_pct.get(player["key"])
        if manual_3p_over is None:
            total_times_3p = build_exp_3p(player_info)
            player_info["expected3P"] = total_times_3p / (player_info["shot_info_total_3p"] or 1.0)
            var_total_3p += total_times_3p
        else:
            # (use the manual override, e.g. player's career stats as their
            # expected shooting %):
            player_info["expected3P"] = manual_3p_over
            var_total_3p += player_info["shot_info_total_3p"] * manual_3p_over

        if player_info["shot_info_total_3p"] > 0:
            # (don't bother with players who didn't take a 3P shot)
            player_entries.append((player["key"], player_info))

    # lodash `.sortBy` is a stable ascending sort; `-1 * (x || 0)` ascending
    # == descending by shot_info_total_3p:
    player_entries.sort(key=lambda kv: -(kv[1]["shot_info_total_3p"] or 0))
    player_3p_info: dict[str, OffLuckShotTypeAndAdj3P] = dict(player_entries)

    sample_base_3p = var_total_3p / (var_total_3pa or 1.0)

    total_3pa = (sample_3pa + base_3pa) or 1.0
    regress_3p = (sample_base_3p * base_3pa + sample_3p * sample_3pa) / total_3pa

    sample_off_3p_rate = _luck_get(sample_team.get("off_3pr"), 0.0)
    sample_off_fga = _luck_get(sample_team.get("total_off_2p_attempts"), 0.0) + _luck_get(
        sample_team.get("total_off_3p_attempts"), 0.0
    )
    sample_off_fta = _luck_get(sample_team.get("total_off_fta"), 0.0)
    raw_sample_off_orb = _luck_get(sample_team.get("off_orb"), 0.0)
    sample_off_orb = 0.66 if raw_sample_off_orb > 0.66 else raw_sample_off_orb

    sample_off_efg = _luck_get(sample_team.get("off_efg"), 0.0)
    sample_off_ppp = _luck_get(sample_team.get("off_ppp"), 0.0)
    sample_def_sos = _luck_get(sample_team.get("def_adj_opp"), 0.0)

    # Team only -- checks manual overrides, which is unset for a player
    # call by construction. NOTE: plain `field?.value || 0` here, NOT
    # `_luck_get` (see module docstring's `_luck_get` note):
    adjusted_to = _num(sample_team, "off_to", 0.0)
    delta_to = (
        (adjusted_to - (_field_val(sample_team.get("off_to"), "old_value", 0.0) or adjusted_to))
        if manual_overrides is not None
        else 0.0
    )

    adjusted_2p = _num(sample_team, "off_2p", 0.0)
    delta_2p = (
        (adjusted_2p - (_field_val(sample_team.get("off_2p"), "old_value", 0.0) or adjusted_2p))
        if manual_overrides is not None
        else 0.0
    )

    adjusted_ft = _num(sample_team, "off_ft", 0.0)
    delta_ft = (
        (adjusted_ft - (_field_val(sample_team.get("off_ft"), "old_value", 0.0) or adjusted_ft))
        if manual_overrides is not None
        else 0.0
    )

    delta_3p = regress_3p - sample_3p
    delta_off_efg = 1.5 * delta_3p * sample_off_3p_rate + delta_2p * (1 - sample_off_3p_rate)
    delta_misses_pct = -1 * (delta_3p * sample_off_3p_rate + delta_2p * (1 - sample_off_3p_rate))
    delta_off_ppp_no_orb = (200 * delta_off_efg * sample_off_fga + 100 * delta_ft * sample_off_fta) / (
        sample_poss or 1.0
    )
    # pts_off_misses = delta_misses*ORB*(ppp_no_orb + pts_off_misses)
    # ie pts_off_misses = delta_misses*ORB*ppp_no_orb/(1 - delta_misses*ORB)
    # NOTE: unguarded division (matches upstream) -- see module docstring landmine list.
    delta_off_orb_factor = (delta_misses_pct * sample_off_orb) / (1 - delta_misses_pct * sample_off_orb)
    delta_pts_off_misses = delta_off_orb_factor * (sample_off_ppp + delta_off_ppp_no_orb)
    delta_off_ppp_pre_to = delta_off_ppp_no_orb + delta_pts_off_misses
    # (again plain `.value || 0`, not `_luck_get`):
    delta_pts_lost_from_tos = delta_to * (_num(sample_team, "off_ppp", 0.0) + delta_off_ppp_pre_to)
    delta_off_ppp = delta_off_ppp_pre_to - delta_pts_lost_from_tos
    delta_off_adj_eff = (delta_off_ppp * avg_eff) / (sample_def_sos or 1.0)

    return {
        "avgEff": avg_eff,
        "samplePoss": sample_poss,
        "sample3P": sample_3p,
        "sample3PA": sample_3pa,
        "base3PA": base_3pa,
        "player3PInfo": player_3p_info,
        "sampleBase3P": sample_base_3p,
        "regress3P": regress_3p,
        "sampleOff3PRate": sample_off_3p_rate,
        "sampleOffFGA": sample_off_fga,
        "sampleOffOrb": sample_off_orb,
        "sampleOffEfg": sample_off_efg,
        "sampleOffPpp": sample_off_ppp,
        "sampleDefSos": sample_def_sos,
        "delta3P": delta_3p,
        "deltaOffEfg": delta_off_efg,
        "deltaMissesPct": delta_misses_pct,
        "deltaOffPppNoOrb": delta_off_ppp_no_orb,
        "deltaOffOrbFactor": delta_off_orb_factor,
        "deltaPtsOffMisses": delta_pts_off_misses,
        "deltaOffPpp": delta_off_ppp,
        "deltaOffAdjEff": delta_off_adj_eff,
    }


def calc_off_player_luck_adj(
    sample_player: LineupStatSet,
    base_player: LineupStatSet,
    avg_eff: float,
) -> OffLuckAdjustmentDiags:
    """Offensive 3P-luck adjustment for a single player.

    Faithful port of ``LuckUtils.calcOffPlayerLuckAdj`` (``LuckUtils.ts:174-187``).
    Per Task 2.1's surprise #4, this is a literal 1-player-team delegation
    to :func:`calc_off_team_luck_adj` -- ORB effects are ignored for an
    individual player (the upstream comment: "the team calc basically
    works fine here, apart from ORBs, which we'll ignore").

    Args:
        sample_player: The player's stat dict for the period being
            luck-adjusted.
        base_player: The player's stat dict for the baseline/reference
            period.
        avg_eff: League/context average efficiency (``100`` in every
            vendored jest call).

    Returns:
        Same shape as :func:`calc_off_team_luck_adj` -- identical to
        calling that function with ``sample_players=[sample_player]``,
        ``base_players_map={base_player["key"]: base_player}``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_luck import calc_off_player_luck_adj

            diags = calc_off_player_luck_adj(sample_player, base_player, 100.0)
            print(diags["deltaOffAdjEff"])

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return calc_off_team_luck_adj(
        sample_player,
        [sample_player],
        base_player,
        {base_player["key"]: base_player},
        avg_eff,
    )


def _field_value_is_nil(field: Any) -> bool:
    """``_.isNil(field?.value)`` -- checks *only* ``value``, never
    ``old_value`` (unlike :func:`_luck_get`). See the module docstring's
    "SoS-vs-luck-split nil-check landmine" note for why
    :func:`calc_def_team_luck_adj` needs this distinct null-check shape.

    Args:
        field: A ``Statistic``-shaped dict (or ``None``/non-dict).

    Returns:
        ``True`` if ``field`` isn't a dict, or its ``value`` key is absent
        or ``None``.
    """
    return not isinstance(field, dict) or field.get("value") is None


def calc_def_team_luck_adj(
    sample: LineupStatSet,
    base: LineupStatSet,
    avg_eff: float,
    sample_def_3pa_override: float | None = None,
) -> DefLuckAdjustmentDiags:
    """Defensive 3P-luck adjustment for a team (or lineup).

    Faithful port of ``LuckUtils.calcDefTeamLuckAdj`` (``LuckUtils.ts:429-531``).
    See the module docstring for the SoS-vs-luck-split formula (:data:`LUCK_PCT`)
    and the shared unguarded-division landmine.

    Args:
        sample: The team/lineup/player stat dict for the period being
            luck-adjusted (e.g. an on/off split or a single lineup).
        base: The team/lineup/player stat dict for the baseline/reference
            period.
        avg_eff: League/context average efficiency (``100`` in every
            vendored jest call).
        sample_def_3pa_override: When given, used as ``sampleDef3PA``
            instead of ``sample["total_def_3p_attempts"]`` -- see
            :func:`calc_off_team_luck_adj`'s ``sample_3pa_override``
            docstring for the shared "lineup regression" rationale
            (``LuckUtils.ts:433-434``, verbatim comment).

    Returns:
        A :data:`DefLuckAdjustmentDiags` dict -- TS-verbatim keys
        (``avgEff``, ``luckPct``, ``baseDef3P``, ``baseDef3PSos``,
        ``baseDef3PA``, ``basePoss``, ``base3PSosAdj``, ``sampleDef3P``,
        ``sampleDef3PSos``, ``sampleDef3PA``, ``samplePoss``,
        ``sample3PSosAdj``, ``sampleDefEfg``, ``sampleDefPpp``,
        ``sampleOffSos``, ``sampleDef3PRate``, ``sampleDefFGA``,
        ``sampleDefOrb``, ``avg3PSosAdj``, ``adjDef3P``, ``delta3P``,
        ``deltaDefEfg``, ``deltaDefPppNoOrb``, ``deltaMissesPct``,
        ``deltaDefOrbFactor``, ``deltaPtsOffMisses``, ``deltaDefPpp``,
        ``deltaDefAdjEff``).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_luck import calc_def_team_luck_adj

            diags = calc_def_team_luck_adj(sample_team_off, base_team, 100.0)
            print(diags["deltaDefAdjEff"])

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    base_def_3p = _luck_get(base.get("def_3p"), 0.0)
    base_def_3p_sos = (
        base_def_3p if _field_value_is_nil(base.get("def_3p_opp")) else 0.01 * _luck_get(base.get("def_3p_opp"), 0.0)
    )
    base_poss = _luck_get(base.get("def_poss"), 0.0)
    base_def_3pa = _luck_get(base.get("total_def_3p_attempts"), 0.0)
    base_3p_sos_adj = (1.0 - LUCK_PCT) * (base_def_3p - base_def_3p_sos)

    sample_def_3p = _luck_get(sample.get("def_3p"), 0.0)
    sample_def_3p_sos = (
        sample_def_3p
        if _field_value_is_nil(sample.get("def_3p_opp"))
        else 0.01 * _luck_get(sample.get("def_3p_opp"), 0.0)
    )
    sample_poss = _luck_get(sample.get("def_poss"), 0.0)
    sample_3p_sos_adj = (1.0 - LUCK_PCT) * (sample_def_3p - sample_def_3p_sos)

    sample_def_efg = _luck_get(sample.get("def_efg"), 0.0)
    sample_def_ppp = _luck_get(sample.get("def_ppp"), 0.0)
    sample_off_sos = _luck_get(sample.get("off_adj_opp"), 0.0)
    sample_def_3pa = (
        sample_def_3pa_override
        if sample_def_3pa_override is not None
        else _luck_get(sample.get("total_def_3p_attempts"), 0.0)
    )

    avg_3p_sos_adj = (sample_def_3pa * sample_3p_sos_adj + base_def_3pa * base_3p_sos_adj) / (
        (sample_def_3pa + base_def_3pa) or 1.0
    )
    adj_def_3p = sample_def_3p_sos + avg_3p_sos_adj

    sample_def_3p_rate = _luck_get(sample.get("def_3pr"), 0.0)
    sample_def_fga = _luck_get(sample.get("total_def_2p_attempts"), 0.0) + _luck_get(
        sample.get("total_def_3p_attempts"), 0.0
    )
    raw_sample_def_orb = _luck_get(sample.get("def_orb"), 0.0)
    sample_def_orb = 0.66 if raw_sample_def_orb > 0.66 else raw_sample_def_orb

    delta_3p = adj_def_3p - sample_def_3p
    delta_def_efg = 1.5 * delta_3p * sample_def_3p_rate
    delta_misses_pct = -1.0 * delta_3p * sample_def_3p_rate
    delta_def_ppp_no_orb = (200.0 * delta_def_efg * sample_def_fga) / (sample_poss or 1.0)
    # pts_off_misses = delta_misses*ORB*(ppp_no_orb + pts_off_misses)
    # ie pts_off_misses = delta_misses*ORB*ppp_no_orb/(1 - delta_misses*ORB)
    # NOTE: unguarded division (matches upstream) -- see module docstring landmine list.
    delta_def_orb_factor = (delta_misses_pct * sample_def_orb) / (1.0 - delta_misses_pct * sample_def_orb)
    delta_pts_off_misses = delta_def_orb_factor * (sample_def_ppp + delta_def_ppp_no_orb)
    delta_def_ppp = delta_def_ppp_no_orb + delta_pts_off_misses
    delta_def_adj_eff = (delta_def_ppp * avg_eff) / (sample_off_sos or 1.0)

    return {
        "avgEff": avg_eff,
        "luckPct": LUCK_PCT,
        "baseDef3P": base_def_3p,
        "baseDef3PSos": base_def_3p_sos,
        "baseDef3PA": base_def_3pa,
        "basePoss": base_poss,
        "base3PSosAdj": base_3p_sos_adj,
        "sampleDef3P": sample_def_3p,
        "sampleDef3PSos": sample_def_3p_sos,
        "sampleDef3PA": sample_def_3pa,
        "samplePoss": sample_poss,
        "sample3PSosAdj": sample_3p_sos_adj,
        "sampleDefEfg": sample_def_efg,
        "sampleDefPpp": sample_def_ppp,
        "sampleOffSos": sample_off_sos,
        "sampleDef3PRate": sample_def_3p_rate,
        "sampleDefFGA": sample_def_fga,
        "sampleDefOrb": sample_def_orb,
        "avg3PSosAdj": avg_3p_sos_adj,
        "adjDef3P": adj_def_3p,
        "delta3P": delta_3p,
        "deltaDefEfg": delta_def_efg,
        "deltaDefPppNoOrb": delta_def_ppp_no_orb,
        "deltaMissesPct": delta_misses_pct,
        "deltaDefOrbFactor": delta_def_orb_factor,
        "deltaPtsOffMisses": delta_pts_off_misses,
        "deltaDefPpp": delta_def_ppp,
        "deltaDefAdjEff": delta_def_adj_eff,
    }


def calc_def_player_luck_adj(
    sample: LineupStatSet,
    base: LineupStatSet,
    avg_eff: float,
) -> DefLuckAdjustmentDiags:
    """Defensive 3P-luck adjustment for a single player.

    Faithful port of ``LuckUtils.calcDefPlayerLuckAdj`` (``LuckUtils.ts:402-426``).
    Unlike :func:`calc_off_player_luck_adj`, this is **not** a pure
    delegation -- see the module docstring's ``calc_def_player_luck_adj``
    note for the ``translate()`` remap this wraps around
    :func:`calc_def_team_luck_adj`.

    Args:
        sample: The player's stat dict for the period being luck-adjusted
            (must carry ``oppo_total_def_3p_made``/``oppo_total_def_3p_attempts``
            -- there is no player-level ``def_3p`` field upstream, hence
            the remap).
        base: The player's stat dict for the baseline/reference period.
        avg_eff: League/context average efficiency (``100`` in every
            vendored jest call).

    Returns:
        Same shape as :func:`calc_def_team_luck_adj`, computed against the
        translated (``oppo_*`` -> ``def_*``) player stat dicts.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_luck import calc_def_player_luck_adj

            diags = calc_def_player_luck_adj(sample_player, base_player, 100.0)
            print(diags["deltaDefAdjEff"])

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """

    def translate(stat_set: LineupStatSet) -> LineupStatSet:
        """Port of the ``calcDefPlayerLuckAdj``-local ``translate`` closure
        (``LuckUtils.ts:407-419``). Note ``key`` is unconditionally
        ``sample["key"]`` (the outer function's own ``sample`` parameter,
        not ``stat_set["key"]``) even when translating ``base`` -- a
        closure-capture quirk in the TS original, ported verbatim (see the
        module docstring)."""
        made = _num(stat_set, "oppo_total_def_3p_made", 0.0)
        attempts = _num(stat_set, "oppo_total_def_3p_attempts", 0.0)
        return {
            "key": sample.get("key"),
            "def_3p": {"value": made / (attempts or 1.0)},
            "def_3p_opp": stat_set.get("oppo_def_3p_opp"),
            "def_poss": stat_set.get("oppo_total_def_poss"),
            "total_def_3p_attempts": stat_set.get("oppo_total_def_3p_attempts"),
        }

    return calc_def_team_luck_adj(translate(sample), translate(base), avg_eff)


def _override_get_original_val(field: Any) -> float | None:
    """Port of ``OverrideUtils.getOriginalVal`` (``OverrideUtils.ts:80-86``).

    Nullish-selects ``old_value`` over ``value`` -- unlike :func:`_luck_get`,
    there is **no fallback**: an absent/``None`` result is itself a
    meaningful "no original value" signal to :func:`_override_mutable_val`
    (see that function and the module docstring's ``getIgnoreNil`` note),
    not just a coalesce default to paper over.

    Args:
        field: A ``Statistic``-shaped dict (or ``None``/non-dict).

    Returns:
        ``field["old_value"]`` if present and non-``None``, else
        ``field["value"]`` (which may itself be ``None`` or absent), else
        ``None`` when ``field`` isn't a dict at all.
    """
    if not isinstance(field, dict):
        return None
    old_value = field.get("old_value")
    return field.get("value") if old_value is None else old_value


def _override_get_ignore_nil(val: dict[str, Any]) -> dict[str, Any]:
    """Port of ``OverrideUtils.getIgnoreNil`` (``OverrideUtils.ts:89-90``).

    "If the old value was nil we leave it alone" (upstream doc comment):
    collapses a freshly-built override object down to ``{"value": None}``
    when its own ``old_value`` is ``None`` (i.e. there was no real original
    baseline to adjust from) -- discarding the computed ``value``/
    ``override`` instead of fabricating an override on top of nothing. See
    the module docstring's landmine list for why this is unexercised by
    this task's oracle but kept for shared-primitive fidelity.

    Args:
        val: A freshly-constructed ``{"value", "old_value", "override"}``
            dict (never called on anything else in this port).

    Returns:
        ``val`` unchanged if ``val["old_value"]`` is non-``None``, else
        ``{"value": None}``.
    """
    return {"value": None} if val.get("old_value") is None else val


def _override_mutable_val(
    mutable_stats: LineupStatSet,
    key: str,
    new_val: float | dict[str, float] | None,
    reason: str | None,
) -> bool:
    """Port of ``OverrideUtils.overrideMutableVal`` (``OverrideUtils.ts:409-457``).

    The field-mutation primitive :func:`inject_luck` calls 8 times to
    reversibly apply (or reset) a luck delta onto one stat-set field. See
    the module docstring's "OverrideUtils scope note" for what's
    deliberately not ported (the shot-quality key remap), and the
    "``injectLuck``/``overrideMutableVal`` landmines" list for the
    absolute-vs-delta, object-truthiness, ``getIgnoreNil``, and
    ``noOverwrite`` behaviors this function implements.

    Args:
        mutable_stats: The stat-set dict to mutate **in place** (mirrors
            the JS reference-mutation semantics -- this function does not
            return a new dict).
        key: The stat-set field name to override. Must not start with
            ``"sq_"`` (raises ``NotImplementedError`` -- see the module
            docstring's scope note).
        new_val: ``None`` resets the field to its original (pre-override)
            value; a plain ``int``/``float`` sets it as an *absolute*
            value; a ``{"delta": float}`` dict adds ``delta`` to the
            field's original value.
        reason: The override-reason string stamped into the field's
            ``override`` key on a real mutation. Also participates in the
            ``noOverwrite`` reason-mismatch guard when ``new_val`` is
            ``None`` (see the module docstring's landmine #4).

    Returns:
        ``True`` if ``new_val`` was not ``None`` (matching the TS return
        contract -- "returns true if mutated"). Unused by
        :func:`inject_luck` itself (which discards it), kept for parity /
        future callers.
    """
    if key.startswith("sq_"):
        raise NotImplementedError(
            "_override_mutable_val: shot-quality-prefixed keys (sq_2prim/sq_2pmid/sq_3p) are not "
            "ported -- that key-remap branch belongs to the manual shot-quality-override UI feature "
            "(OverrideUtils.getRimPctFromTs), out of this task's scope. See the mbb_luck module "
            "docstring's 'OverrideUtils scope note'."
        )

    existing = mutable_stats.get(key)
    if existing is None:
        return False

    original_val = _override_get_original_val(existing)
    maybe_reason = existing.get("override")
    no_overwrite = new_val is None and maybe_reason is not None and reason is not None and maybe_reason != reason

    if not no_overwrite:
        if new_val is None:
            mutable_stats[key] = {"value": original_val}
        else:
            computed_val = (
                new_val if isinstance(new_val, (int, float)) else (original_val or 0.0) + (new_val.get("delta") or 0.0)
            )
            mutable_stats[key] = _override_get_ignore_nil(
                {"value": computed_val, "old_value": original_val, "override": reason}
            )

    return new_val is not None


def _diag(d: OffLuckAdjustmentDiags | DefLuckAdjustmentDiags | None, key: str) -> Any:
    """``d?.[key]`` -- optional-chaining accessor for a possibly-``None``
    luck-diagnostics dict (:func:`inject_luck`'s ``off_luck``/``def_luck``
    params, both optional)."""
    return None if d is None else d.get(key)


def _maybe_delta(val: float | None) -> dict[str, float] | None:
    """Port of the ``injectLuck``-local ``maybeDelta`` closure
    (``LuckUtils.ts:540-541``): wraps a nullable diagnostic number into the
    ``{"delta": float}`` shape :func:`_override_mutable_val` expects for its
    delta-application branch, or ``None`` through (a reset request) when
    the diagnostic itself is ``None``."""
    return None if val is None else {"delta": val}


def inject_luck(
    mutable_stats: LineupStatSet,
    off_luck: OffLuckAdjustmentDiags | None,
    def_luck: DefLuckAdjustmentDiags | None,
) -> None:
    """Reversibly mutate a stat set in place with luck-adjustment deltas.

    Faithful port of ``LuckUtils.injectLuck`` (``LuckUtils.ts:534-650``).
    Works on a team, lineup, or player stat dict -- only the fields already
    present on ``mutable_stats`` are touched (see
    :func:`_override_mutable_val`'s object-presence gate), so calling this
    on a stat set that doesn't carry a given field (e.g. a bare
    ``{"key": ..., "doc_count": 0}`` placeholder) is a safe no-op for that
    field. Passing ``off_luck=None, def_luck=None`` resets every field this
    function has ever touched back to its pre-luck value (see the module
    docstring's landmine list for the exact mechanics, including the
    absolute-vs-delta distinction on ``def_3p``/``oppo_def_3p``).

    Args:
        mutable_stats: The stat-set dict to mutate in place. May be a
            team/lineup stat set (carries ``off_net``/``off_raw_net``/no
            ``oppo_total_def_3p_made``) or a player stat set (carries
            ``oppo_total_def_3p_made``, gating the extra ``oppo_def_3p``
            recompute -- see the module docstring's landmine #2).
        off_luck: The output of :func:`calc_off_team_luck_adj` /
            :func:`calc_off_player_luck_adj`, or ``None`` to omit/reset the
            offensive-side fields.
        def_luck: The output of :func:`calc_def_team_luck_adj` /
            :func:`calc_def_player_luck_adj`, or ``None`` to omit/reset the
            defensive-side fields.

    Returns:
        ``None`` -- this function mutates ``mutable_stats`` in place (TS
        ``injectLuck`` likewise returns nothing).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_luck import (
                calc_off_team_luck_adj, calc_def_team_luck_adj, inject_luck,
            )

            off_luck = calc_off_team_luck_adj(sample_team_on, sample_players_on, base_team, base_players_map, 100.0)
            def_luck = calc_def_team_luck_adj(sample_team_off, base_team, 100.0)
            inject_luck(sample_team_on, off_luck, def_luck)
            print(sample_team_on["off_3p"])

        Reset back to the pre-luck values::

            inject_luck(sample_team_on, None, None)

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    # Offense - 3P
    _override_mutable_val(mutable_stats, "off_3p", _maybe_delta(_diag(off_luck, "delta3P")), "Luck adjusted")

    # Offense - derived 4 factors and efficiency
    _override_mutable_val(
        mutable_stats, "off_efg", _maybe_delta(_diag(off_luck, "deltaOffEfg")), "Adjusted from Off 3P%"
    )
    _override_mutable_val(
        mutable_stats, "off_ppp", _maybe_delta(_diag(off_luck, "deltaOffPpp")), "Adjusted from Off 3P%"
    )
    _override_mutable_val(
        mutable_stats, "off_adj_ppp", _maybe_delta(_diag(off_luck, "deltaOffAdjEff")), "Adjusted from Off 3P%"
    )

    # Defense - 3P (NOTE: absolute value, not a delta -- see module docstring landmine #1)
    _override_mutable_val(mutable_stats, "def_3p", _diag(def_luck, "adjDef3P"), "Luck adjusted")

    # Defense - derived 4 factors and efficiency
    _override_mutable_val(
        mutable_stats, "def_efg", _maybe_delta(_diag(def_luck, "deltaDefEfg")), "Adjusted from Def 3P%"
    )
    _override_mutable_val(
        mutable_stats, "def_ppp", _maybe_delta(_diag(def_luck, "deltaDefPpp")), "Adjusted from Def 3P%"
    )
    _override_mutable_val(
        mutable_stats, "def_adj_ppp", _maybe_delta(_diag(def_luck, "deltaDefAdjEff")), "Adjusted from Def 3P%"
    )

    # Player defense (JS object-truthiness gate -- see module docstring landmine #2)
    if mutable_stats.get("oppo_total_def_3p_made") is not None:
        # (use this to tell us if it's a player stat not a team one)
        # Don't have oppo_3p_def so we'll calculate it every time
        # (and pretend it was a raw value from the query):
        mutable_stats["oppo_def_3p"] = {
            "value": _num(mutable_stats, "oppo_total_def_3p_made", 0.0)
            / _num(mutable_stats, "oppo_total_def_3p_attempts", 1.0)
        }
        _override_mutable_val(mutable_stats, "oppo_def_3p", _diag(def_luck, "adjDef3P"), "Luck adjusted")

    # Net
    off_adj_eff, def_adj_eff = _diag(off_luck, "deltaOffAdjEff"), _diag(def_luck, "deltaDefAdjEff")
    ppp_adj_margin = (
        None if off_adj_eff is None and def_adj_eff is None else (off_adj_eff or 0.0) - (def_adj_eff or 0.0)
    )

    off_ppp_delta, def_ppp_delta = _diag(off_luck, "deltaOffPpp"), _diag(def_luck, "deltaDefPpp")
    ppp_margin = (
        None if off_ppp_delta is None and def_ppp_delta is None else (off_ppp_delta or 0.0) - (def_ppp_delta or 0.0)
    )

    _override_mutable_val(mutable_stats, "off_net", _maybe_delta(ppp_adj_margin), "Adjusted from Off 3P% and Def 3P%")
    _override_mutable_val(mutable_stats, "off_raw_net", _maybe_delta(ppp_margin), "Adjusted from Off 3P% and Def 3P%")
