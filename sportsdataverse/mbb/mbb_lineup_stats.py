"""College lineup aggregation + on/off splits.

Faithful port of hoop-explorer's ``LineupUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/LineupUtils.ts``). This module ports :func:`weighted_avg`
(the ``LineupUtils.weightedAvg`` private static method, ``LineupUtils.ts``
around line 645) — the possession-weighted merge engine that combines
per-lineup Elasticsearch aggregation buckets (``LineupStatSet``) into a
running accumulator. Later tasks in the hoop-explorer port build
``calculate_aggregated_lineup_stats`` / ``lineup_to_team_report`` on top of
this primitive.

Apache-2.0 third-party port — see the ``NOTICE`` file at the repository root for the upstream copyright and full attribution.

Ported behavior (mirrors ``LineupUtils.ts`` line-for-line):

- ``IGNORE_FIELDS`` (``LineupUtils.ts:464`` ``ignoreFieldSet``) and
  ``SUM_FIELDS`` (``LineupUtils.ts:478`` ``sumFieldSet``) are copied
  verbatim as module constants.
- Field classification inside ``weighted_avg`` follows the exact upstream
  if/elif cascade: shot-type stats (``getShotTypeField`` regex,
  ``LineupUtils.ts:635-641``) weight by that lineup's own
  ``total_{off|def}_{type}_{made|attempts}``; ``ppp_totals`` /
  ``orb_totals`` / ``fta_totals`` / ``ast_totals`` fields (all derived by
  ``getSimpleWeights``, ``LineupUtils.ts:562-632``) weight by the matching
  possession/rebound/FT/assist total; ``total_*`` fields and ``SUM_FIELDS``
  plain-sum; ``off_trans_*`` / ``def_trans_*`` / ``off_scramble_*`` /
  ``def_scramble_*`` fields are skipped (upstream: "ppp handled by
  recalculatePlayTypePoss", which is the ``completeWeightedAvg`` companion
  — out of scope for this task); everything else starting with ``off_`` /
  ``def_`` falls back to the plain FGA-weighted total.
- The ``old_value`` / ``override`` (luck-adjustment) bookkeeping is ported
  verbatim — every additive branch that updates ``value`` also updates
  ``old_value`` when the field carries an ``override`` marker.

Task 1.2 additionally ports the finishing + aggregation layer:

- :func:`complete_weighted_avg` — port of ``LineupUtils.completeWeightedAvg``
  (``LineupUtils.ts:752``). *Finishes* a ``weighted_avg`` accumulator by
  dividing each field's weighted sum by the matching weight total
  (``ppp_totals`` / ``orb_totals`` / ``fta_totals`` / ``ast_totals`` /
  shot-type / generic FGA fallback), and recomputes the transition/scramble
  possession + PPP fields (skipped entirely by ``weighted_avg``) via
  :func:`_recalculate_play_type_poss`
  (``LineupUtils.recalculatePlayTypePoss``, ``LineupUtils.ts:501``). After
  this call, previously-summed fields on the accumulator are true weighted
  **averages**.
- :func:`calculate_aggregated_lineup_stats` — port of
  ``LineupUtils.calculateAggregatedLineupStats`` (``LineupUtils.ts:106``).
  Folds a list of per-lineup ``LineupStatSet`` docs into one team-total
  ``LineupStatSet``, honoring the ``rapmRemove`` flag (lineups so marked are
  diverted into a separate ``all_lineups`` sub-accumulator instead of the
  main team total), then calls :func:`complete_weighted_avg`, re-merges
  ``all_lineups`` against the now-averaged team total, and finally rebuilds
  ``off_net`` / ``off_raw_net`` via :func:`build_efficiency_margins`.
- :func:`build_efficiency_margins` — port of
  ``LineupUtils.buildEfficiencyMargins`` (``LineupUtils.ts:145``). Derives
  ``off_net`` (``off_adj_ppp - def_adj_ppp``) and ``off_raw_net``
  (``off_ppp - def_ppp``).

Task 1.3 additionally ports the on/off engine:

- :func:`get_stats_diff` -- port of ``LineupUtils.getStatsDiff``
  (``LineupUtils.ts:185``). A "no clever weighting" straight subtraction of
  two ``LineupStatSet`` team-stat dicts field-by-field (``value`` and, when
  both sides carry one, ``old_value``); fields missing a ``value`` on either
  side become ``None`` (the JS ``undefined`` analog) rather than being
  silently dropped, matching ``_.fromPairs`` explicit-undefined semantics.
  Not called internally by :func:`lineup_to_team_report` -- it is a
  standalone utility used elsewhere in the upstream app (UI diff tables).
- :func:`lineup_to_team_report` -- port of ``LineupUtils.lineupToTeamReport``
  (``LineupUtils.ts:277``). Partitions every distinct player across a
  team's lineups into ON (lineups the player appears in) and OFF (lineups
  they don't) ``LineupStatSet`` accumulators via :func:`weighted_avg` /
  :func:`complete_weighted_avg`, plus an optional "replacement" (on-minus-
  off, harmonic-mean-weighted) composite when ``inc_replacement=True``. Also
  builds a per-player ``teammates`` possession-overlap map and (when a
  player never appears OFF, e.g. played every lineup) zero-fills their OFF
  side via :func:`_copy_and_zero` rather than leaving it a bare ``{"key":
  ...}`` stub.
- Private helpers backing the above: ``_get_player_set`` (roster extraction
  from the ES ``players_array`` hits payload), ``_update_lineup_composition``
  (teammate on/off possession tally), ``_copy_and_zero``,
  ``_is_complement_lineup`` (4-of-5-shared-players check for the
  replacement on/off pairing), ``_calc_harmonic_mean``, and
  ``_combine_replacement_on_off`` (``LineupUtils.ts:924`` --
  ``completeWeightedAvg``'s ``harmonic_weighting=True`` mode applied to the
  on-minus-off diff of every complementary lineup pair).

**Faithful bug-for-bug port in ``_combine_replacement_on_off``**: the
upstream ``combineReplacementOnOff`` (``LineupUtils.ts:1005-1038``) captures
an unused ``oldValue`` local, then immediately reassigns ``myLineup[key]``
to a fresh ``{value: ...}`` object (dropping any prior ``old_value``),
*then* -- in the very next block -- reads ``myLineup[key]?.old_value`` off
that just-reassigned object (which never has an ``old_value`` property) to
decide the harmonic-mean-of-old-values branch. The read therefore always
sees ``undefined`` / ``0``, so the "both old values > 0" branch is
unreachable and ``old_value`` is unconditionally pinned to ``0`` whenever
an ``override`` was present pre-reassignment. This looks like an upstream
bug (the captured ``oldValue`` local is dead code), but it is preserved
verbatim here for oracle fidelity -- see the inline comment at the call
site.

**``game_info`` is not exercised by ``lineup_to_team_report``'s jest
oracle.** ``LineupUtils.test.ts``'s ``getGameInfo`` test calls
``LineupUtils.getGameInfo`` directly on a hand-built ES date-histogram
payload (``testIn``) -- a separate code path from
``lineupToTeamReport``/``weightedAvg``/``completeWeightedAvg``. None of the
3 vendored lineup buckets used by the ``lineupToTeamReport`` jest test carry
a ``game_info`` key, so the ``weighted_avg``/``complete_weighted_avg``
``game_info`` stubs (raising :class:`NotImplementedError`) remain
unreached through this call path too, and ``getGameInfo`` itself stays
unported (deferred to a later task if/when ``lineup_to_team_report`` gains
a game-info-aggregation caller).

Deliberately NOT ported:

- ``LineupUtils.getGameInfo`` (``LineupUtils.ts:222``) -- the ES
  date-histogram-bucket parser behind the ``game_info`` merge branches (see
  above). Exercised by a dedicated ``getGameInfo`` jest test/snapshot that
  is out of this task's scope (``lineupToTeamReport`` never reaches it with
  the vendored fixtures).
- The ``game_info`` merge/finishing branches
  (``weightedAvg`` ``LineupUtils.ts:722-746`` and ``completeWeightedAvg``
  ``LineupUtils.ts:860-865``), which are reachable despite ``game_info``
  being a member of ``ignoreFieldSet`` (the upstream code special-cases it
  via a separate ``else if (key == "game_info")`` branch in both
  functions). Both branches depend on ``LineupUtils.getGameInfo``
  (``LineupUtils.ts:222-270``), an unrelated ES date-histogram-bucket
  parser used by ``lineupToTeamReport`` / ``getGameInfo`` tests. **Confirmed
  unreachable in this task's oracle scope**: none of the 3 vendored
  ``sampleLineupStatsResponse`` lineup buckets carry a ``game_info`` key,
  and ``calculate_aggregated_lineup_stats`` seeds its accumulator from
  ``StatModels.emptyLineup()`` (``{key, doc_count}`` only — no
  ``game_info``), so the key can never appear on the accumulator through
  this call path either. Both :func:`weighted_avg` and
  :func:`complete_weighted_avg` raise :class:`NotImplementedError` for that
  key instead of silently dropping data, in case a future caller feeds a
  lineup doc that does carry ``game_info``.
- Anything React/UI-only (chart formatting, table rendering, luck-model
  UI toggles) has no numeric-engine analog and was never in scope.
"""

from __future__ import annotations

import re
from typing import Any

__all__ = [
    "LineupStatSet",
    "IGNORE_FIELDS",
    "SUM_FIELDS",
    "weighted_avg",
    "complete_weighted_avg",
    "build_efficiency_margins",
    "calculate_aggregated_lineup_stats",
    "get_stats_diff",
    "lineup_to_team_report",
]

LineupStatSet = dict[str, Any]

#: Verbatim from ``LineupUtils.ts:464`` (``ignoreFieldSet``). Fields that
#: never participate in the weighted-sum merge (identifiers, ES hit
#: payloads, bookkeeping). ``game_info`` is included per upstream (it is a
#: member of ``ignoreFieldSet`` there too) but is still specially handled
#: -- see the module docstring's "Deliberately NOT ported" section.
IGNORE_FIELDS: frozenset[str] = frozenset(
    {
        "key",
        "players_array",
        "doc_count",
        # (replacement on/off vals:)
        "offLineups",
        "offLineupKeys",
        "onLineup",
        # Game info, handled separately:
        "game_info",
        # Removed lineups
        "removed",
    }
)

#: Verbatim from ``LineupUtils.ts:478`` (``sumFieldSet``). Fields that
#: plain-sum across merged lineups (no possession weighting).
SUM_FIELDS: frozenset[str] = frozenset({"off_poss", "def_poss", "duration_mins"})

# ``^(off|def)_([23][a-z]*[^_r]+)(_ast)?$`` -- verbatim from
# ``LineupUtils.ts:636`` (``getShotTypeField``). No lookaround needed; a
# direct 1:1 transcription of the JS regex.
_SHOT_TYPE_RE = re.compile(r"^(off|def)_([23][a-z]*[^_r]+)(_ast)?$")


def _field_val(field: Any, attr: str = "value", default: float = 0.0) -> float:
    """Mimic JS ``field?.[attr] || default`` (falsy -> default, incl. 0)."""
    if not isinstance(field, dict):
        return default
    val = field.get(attr)
    return val if val else default


def _num(obj: LineupStatSet, key: str, default: float = 0.0) -> float:
    """Mimic JS ``obj[key]?.value || default``."""
    return _field_val(obj.get(key), "value", default)


def _get_shot_type_field(key: str) -> str | None:
    """Port of ``LineupUtils.getShotTypeField`` (``LineupUtils.ts:635``).

    For a shot-type-percentage field (e.g. ``off_3p``, ``def_2pmid_ast``,
    *not* the rate variants ending in ``r`` like ``off_3pr``), return the
    matching ``total_{off|def}_{type}_{made|attempts}`` field name used as
    that stat's possession-weight source. Returns ``None`` for keys that
    don't match the shot-type-percentage shape.
    """
    match = _SHOT_TYPE_RE.match(key)
    if not match:
        return None
    side, stat_type, ast_suffix = match.group(1), match.group(2), match.group(3)
    metric = "made" if ast_suffix else "attempts"
    return f"total_{side}_{stat_type}_{metric}"


def _regression_weights(regress: float, poss: float) -> dict[str, float]:
    """Port of ``LineupUtils.regressionWeights`` (``LineupUtils.ts:550``)."""
    use_poss = max(0.0, -regress - poss) if regress < 0 else regress
    return {
        "poss": use_poss,
        "orb": 0.4 * use_poss,
        "ast": 0.2 * use_poss,
        "fga": 0.8 * use_poss,
        "fta": 0.1 * use_poss,
    }


def _get_simple_weights(obj: LineupStatSet, default_val: float, regress_diffs: float = 0.0) -> dict[str, Any]:
    """Port of ``LineupUtils.getSimpleWeights`` (``LineupUtils.ts:562``).

    Returns the per-field-family possession/rebound/FT/assist weight
    tables used by both :func:`weighted_avg` (this task) and
    ``completeWeightedAvg`` (deferred; not yet ported).
    """
    off_regress = _regression_weights(regress_diffs, _num(obj, "off_poss", default_val))
    def_regress = _regression_weights(regress_diffs, _num(obj, "def_poss", default_val))

    ppp_totals = {
        "off_ppp": off_regress["poss"] + _num(obj, "off_poss", default_val),
        "def_ppp": def_regress["poss"] + _num(obj, "def_poss", default_val),
        "off_to": off_regress["poss"] + _num(obj, "off_poss", default_val),
        "def_to": def_regress["poss"] + _num(obj, "def_poss", default_val),
        "off_adj_opp": off_regress["poss"] + _num(obj, "def_poss", default_val),
        "def_adj_opp": def_regress["poss"] + _num(obj, "off_poss", default_val),
        "off_adj_ppp": off_regress["poss"] + _num(obj, "off_poss", default_val),
        "def_adj_ppp": def_regress["poss"] + _num(obj, "def_poss", default_val),
    }
    orb_totals = {
        "off_orb": (
            off_regress["orb"] + _num(obj, "total_off_orb", default_val) + _num(obj, "total_def_drb", default_val)
        ),
        "def_orb": (
            def_regress["orb"] + _num(obj, "total_def_orb", default_val) + _num(obj, "total_off_drb", default_val)
        ),
    }
    off_ast = off_regress["ast"] + _num(obj, "total_off_assist", default_val)
    def_ast = def_regress["ast"] + _num(obj, "total_def_assist", default_val)
    ast_totals = {
        "off_ast_3p": off_ast,
        "off_ast_mid": off_ast,
        "off_ast_rim": off_ast,
        "def_ast_3p": def_ast,
        "def_ast_mid": def_ast,
        "def_ast_rim": def_ast,
    }
    fga_totals = {
        "off": off_regress["fga"] + _num(obj, "total_off_fga", default_val),
        "def": def_regress["fga"] + _num(obj, "total_def_fga", default_val),
    }
    fta_totals = {
        "off_ft": off_regress["fta"] + _num(obj, "total_off_fta", default_val),
        "def_ft": def_regress["fta"] + _num(obj, "total_def_fta", default_val),
    }
    return {
        "ppp_totals": ppp_totals,
        "orb_totals": orb_totals,
        "ast_totals": ast_totals,
        "fga_totals": fga_totals,
        "fta_totals": fta_totals,
        "regress": {"off": off_regress, "def": def_regress},
    }


def weighted_avg(mutable_acc: LineupStatSet, obj: LineupStatSet) -> None:
    """Merge ``obj`` into ``mutable_acc`` with possession weighting.

    Faithful port of ``LineupUtils.weightedAvg`` (``LineupUtils.ts:645``).
    Mutates ``mutable_acc`` in place (matching the upstream mutable-state
    contract) and returns ``None``. Each call accumulates a **weighted
    sum**, not a weighted average -- the companion ``completeWeightedAvg``
    (upstream ``LineupUtils.ts:752``, not yet ported) divides by the
    accumulated weight totals to finish the average. The per-field weight
    used at each merge step is derived from ``obj``'s *own* totals (e.g.
    that single lineup's ``total_off_fga``), not from any running total on
    ``mutable_acc`` -- callers accumulating many lineups must call
    ``weighted_avg`` once per lineup so every lineup contributes its own
    weight.

    Args:
        mutable_acc: The running accumulator (``LineupStatSet``). Mutated
            in place; fields absent from the accumulator are initialized
            to ``{"value": 0.0}`` (plus ``old_value`` / ``override`` when
            ``obj``'s field carries a luck-adjustment ``override`` marker)
            before ``obj``'s contribution is added.
        obj: The per-lineup ``LineupStatSet`` document to merge in.

    Returns:
        None. ``mutable_acc`` is mutated in place.

    Raises:
        NotImplementedError: If ``obj`` carries a ``game_info`` key -- that
            merge branch is deliberately deferred (see module docstring).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_lineup_stats import weighted_avg

            acc: dict = {}
            weighted_avg(acc, lineup_a)
            weighted_avg(acc, lineup_b)
            print(acc["off_poss"]["value"])  # plain sum (SUM_FIELDS)

        Two-lineup possession-weighted merge::

            acc = {}
            for lineup in three_lineups:
                weighted_avg(acc, lineup)
            # acc now holds weighted SUMS; complete_weighted_avg (not yet
            # ported) is required to turn these into rate-stat averages.

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    weights = _get_simple_weights(obj, 0.0)
    ppp_totals = weights["ppp_totals"]
    orb_totals = weights["orb_totals"]
    ast_totals = weights["ast_totals"]
    fta_totals = weights["fta_totals"]
    fga_totals = weights["fga_totals"]

    for key, field in obj.items():
        if key in IGNORE_FIELDS:
            if key == "game_info":
                raise NotImplementedError(
                    "weighted_avg: the 'game_info' merge branch "
                    "(LineupUtils.ts:722-746, dependent on getGameInfo) is "
                    "not yet ported -- see the mbb_lineup_stats module "
                    "docstring. This task's oracle fixtures never exercise "
                    "it; a later task must implement it before any caller "
                    "feeds a lineup doc carrying a 'game_info' key."
                )
            continue

        val = _field_val(field, "value", 0.0)
        old_val = _field_val(field, "old_value", 0.0)
        old_val_override = field.get("override") if isinstance(field, dict) else None

        total_shot_type_key = _get_shot_type_field(key)

        if key not in mutable_acc:
            # (init if necessary)
            mutable_acc[key] = {"value": 0.0}
            if old_val_override:
                # (luck adjustment)
                mutable_acc[key]["old_value"] = 0.0
                mutable_acc[key]["override"] = old_val_override
        elif old_val_override and not mutable_acc[key].get("override"):
            # (was init'd without override)
            mutable_acc[key]["old_value"] = 0.0
            mutable_acc[key]["override"] = old_val_override

        if total_shot_type_key:
            weight = _num(obj, total_shot_type_key, 0.0)
            mutable_acc[key]["value"] += val * weight
            if old_val_override:
                mutable_acc[key]["old_value"] += old_val * weight
        elif key in ppp_totals:
            weight = ppp_totals[key]
            mutable_acc[key]["value"] += val * weight
            if old_val_override:
                mutable_acc[key]["old_value"] += old_val * weight
        elif key in orb_totals:
            mutable_acc[key]["value"] += val * orb_totals[key]
            # (no luck adjustment currently)
        elif key in fta_totals:
            mutable_acc[key]["value"] += val * fta_totals[key]
            # (no luck adjustment currently)
        elif key in ast_totals:
            mutable_acc[key]["value"] += val * ast_totals[key]
            # (no luck adjustment currently)
        elif key.startswith("total_") or key in SUM_FIELDS:
            mutable_acc[key]["value"] += val
            # (no luck adjustment currently)
            # (note includes total_X_(trans|scramble)_poss, which is
            # recalc'd by recalculatePlayTypePoss -- not yet ported)
        elif key.startswith("off_trans_") or key.startswith("def_trans_"):
            pass  # Ignore for now (ppp handled by recalculatePlayTypePoss)
        elif key.startswith("off_scramble_") or key.startswith("def_scramble_"):
            pass  # Ignore for now (ppp handled by recalculatePlayTypePoss)
        elif key.startswith("off_"):
            # everything else if off/def FGA
            mutable_acc[key]["value"] += val * fga_totals["off"]
            if old_val_override:
                mutable_acc[key]["old_value"] += old_val * fga_totals["off"]
        elif key.startswith("def_"):
            mutable_acc[key]["value"] += val * fga_totals["def"]
            if old_val_override:
                mutable_acc[key]["old_value"] += old_val * fga_totals["def"]


def _recalculate_play_type_poss(mutable_stats: LineupStatSet) -> None:
    """Port of ``LineupUtils.recalculatePlayTypePoss`` (``LineupUtils.ts:501``).

    ``weighted_avg`` deliberately skips ``off_trans_*`` / ``def_trans_*`` /
    ``off_scramble_*`` / ``def_scramble_*`` fields (their "complex ORB" term
    makes naive possession weighting diverge from the true value). This
    helper recomputes ``total_{off|def}_{trans|scramble}_poss`` and
    ``{off|def}_{trans|scramble}_ppp`` directly from the accumulated raw
    totals (FGA/FGM/FTA/TO/ORB/DRB, all plain-summed by ``weighted_avg`` via
    the ``total_`` prefix rule) instead. Mutates ``mutable_stats`` in place,
    adding/overwriting 8 keys (4 ``total_*_poss`` + 4 ``*_ppp``, one pair per
    ``(off, def) x (trans, scramble)`` combination).

    Args:
        mutable_stats: The ``LineupStatSet`` to mutate in place. Expected to
            already carry the plain-summed ``total_{off|def}_{trans|
            scramble}_{fga,fgm,fta,to}`` and ``total_{off|def}_{orb,drb}``
            fields (i.e. called after ``weighted_avg`` accumulation, not
            before).

    Returns:
        None. ``mutable_stats`` is mutated in place.
    """
    for dst_prefix, oppo_dst_prefix in (("off", "def"), ("def", "off")):
        for type_prefix in ("trans_", "scramble_"):
            fga = _num(mutable_stats, f"total_{dst_prefix}_{type_prefix}fga", 0.0)
            fgm = _num(mutable_stats, f"total_{dst_prefix}_{type_prefix}fgm", 0.0)
            fta = _num(mutable_stats, f"total_{dst_prefix}_{type_prefix}fta", 0.0)
            to = _num(mutable_stats, f"total_{dst_prefix}_{type_prefix}to", 0.0)
            var_orb = _num(mutable_stats, f"total_{dst_prefix}_orb", 0.0)
            var_drb = _num(mutable_stats, f"total_{oppo_dst_prefix}_drb", 0.0)

            fg_missed = fga - fgm
            rebound_pct = (1.0 * var_orb) / (var_orb + var_drb) if var_orb > 0 else 0.0
            poss = fgm + (1.0 - rebound_pct) * fg_missed + 0.475 * fta + to

            mutable_stats[f"total_{dst_prefix}_{type_prefix}poss"] = {"value": poss}
            total_pts = _num(mutable_stats, f"total_{dst_prefix}_{type_prefix}pts", 0.0)
            mutable_stats[f"{dst_prefix}_{type_prefix}ppp"] = {"value": (100 * total_pts) / (poss or 1)}


def complete_weighted_avg(
    mutable_acc: LineupStatSet,
    harmonic_weighting: bool = False,
    regress_diffs: float = 0.0,
) -> None:
    """Finish a ``weighted_avg`` accumulator into true weighted averages.

    Faithful port of ``LineupUtils.completeWeightedAvg`` (``LineupUtils.ts:752``).
    Mutates ``mutable_acc`` in place and returns ``None``, mirroring the
    upstream ``void`` + mutable-arg contract. Recomputes the per-field weight
    tables from ``mutable_acc`` itself (``getSimpleWeights(mutableAcc, 1,
    regressDiffs)`` -- note the ``default_val=1``, unlike ``weighted_avg``'s
    ``default_val=0``), then, unless ``harmonic_weighting`` is set, calls
    :func:`_recalculate_play_type_poss` to fix up the transition/scramble
    possession fields that ``weighted_avg`` skipped. Finally divides every
    non-ignored field's accumulated weighted sum by its matching weight
    total (shot-type / ``ppp_totals`` / ``orb_totals`` / ``fta_totals`` /
    ``ast_totals`` / generic FGA fallback); ``total_*`` and ``SUM_FIELDS``
    fields are left untouched (they are already true totals, not sums to be
    averaged). ``off_ftr`` / ``def_ftr`` get a special non-``harmonic_weighting``
    recompute straight from the accumulated ``total_{off|def}_fta`` rather
    than dividing their own weighted sum.

    Args:
        mutable_acc: The ``weighted_avg``-accumulated ``LineupStatSet`` to
            finish in place. Every field with a non-``total_``/``SUM_FIELDS``
            key is converted from a weighted sum to a weighted average.
        harmonic_weighting: When ``True``, skips the
            ``_recalculate_play_type_poss`` fixup and uses a harmonic-style
            division for ``off_ftr``/``def_ftr`` instead of the
            totals-based recompute. Matches the upstream default (``False``)
            used by ``calculate_aggregated_lineup_stats``.
        regress_diffs: Forwarded to ``_get_simple_weights`` -- regression
            toward ~1000 possessions for on/off diff calculations. Defaults
            to ``0.0`` (no regression), matching
            ``calculate_aggregated_lineup_stats``'s call site.

    Returns:
        None. ``mutable_acc`` is mutated in place.

    Raises:
        NotImplementedError: If ``mutable_acc`` carries a ``game_info`` key
            -- that finishing branch is deliberately deferred (see module
            docstring). Not reachable via ``calculate_aggregated_lineup_stats``
            today since ``weighted_avg`` already raises before a
            ``game_info`` key can land on the accumulator.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_lineup_stats import weighted_avg, complete_weighted_avg

            acc: dict = {}
            for lineup in lineups:
                weighted_avg(acc, lineup)
            complete_weighted_avg(acc)
            print(acc["off_ppp"]["value"])  # now a true weighted average

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    weights = _get_simple_weights(mutable_acc, 1.0, regress_diffs)
    ppp_totals = weights["ppp_totals"]
    orb_totals = weights["orb_totals"]
    ast_totals = weights["ast_totals"]
    fta_totals = weights["fta_totals"]
    fga_totals = weights["fga_totals"]
    regress = weights["regress"]

    if not harmonic_weighting:
        _recalculate_play_type_poss(mutable_acc)

    for key, field in list(mutable_acc.items()):
        total_shot_type_key = _get_shot_type_field(key)

        if key not in IGNORE_FIELDS:
            val = _field_val(field, "value", 0.0)
            old_val = _field_val(field, "old_value", 0.0)
            old_val_override = field.get("override") if isinstance(field, dict) else None

            if total_shot_type_key:
                off_or_def_weight = regress["off"]["fga"] if key.startswith("off_") else regress["def"]["fga"]
                # (3P, 2P mid, 2P rim == 1/3rd each, 2p == 2p rim + 2p mid so 2/3s)
                adj_reg_weight = off_or_def_weight * 2.0 / 3 if key.endswith("2p") else off_or_def_weight / 3
                denom = adj_reg_weight + _num(mutable_acc, total_shot_type_key, 0.0)
                denom = denom or 1
                mutable_acc[key]["value"] = (1.0 * val) / denom
                if old_val_override:
                    mutable_acc[key]["old_value"] = (1.0 * old_val) / denom
            elif key in ppp_totals:
                mutable_acc[key]["value"] = (1.0 * val) / ppp_totals[key]
                if old_val_override:
                    mutable_acc[key]["old_value"] = (1.0 * old_val) / ppp_totals[key]
            elif key in orb_totals:
                mutable_acc[key]["value"] = (1.0 * val) / orb_totals[key]
                # (no luck adjustment for these stats)
            elif key in fta_totals:
                mutable_acc[key]["value"] = (1.0 * val) / fta_totals[key]
                # (no luck adjustment for these stats)
            elif key in ast_totals:
                mutable_acc[key]["value"] = (1.0 * val) / ast_totals[key]
                # (no luck adjustment for these stats)
            elif key.startswith("total_") or key in SUM_FIELDS:
                pass  # (nothing to do; includes total_X_(trans|scramble)_poss, recalc'd above)
            elif key == "off_ftr":
                # FTR is a special case because you can have a FT but 0 FGA
                if harmonic_weighting:
                    mutable_acc[key]["value"] = (1.0 * val) / fga_totals["off"]
                else:
                    mutable_acc[key]["value"] = (1.0 * _num(mutable_acc, "total_off_fta", 0.0)) / fga_totals["off"]
                # (no luck adjustment for these stats)
            elif key == "def_ftr":
                if harmonic_weighting:
                    mutable_acc[key]["value"] = (1.0 * val) / fga_totals["def"]
                else:
                    mutable_acc[key]["value"] = (1.0 * _num(mutable_acc, "total_def_fta", 0.0)) / fga_totals["def"]
                # (no luck adjustment for these stats)
            elif key.startswith("off_trans_") or key.startswith("def_trans_"):
                pass  # Ignore for now (ppp handled by _recalculate_play_type_poss, above)
            elif key.startswith("off_scramble_") or key.startswith("def_scramble_"):
                pass  # Ignore for now (ppp handled by _recalculate_play_type_poss, above)
            elif key.startswith("off_"):
                # everything else if off/def FGA
                mutable_acc[key]["value"] = (1.0 * val) / fga_totals["off"]
                if old_val_override:
                    mutable_acc[key]["old_value"] = (1.0 * old_val) / fga_totals["off"]
            elif key.startswith("def_"):
                mutable_acc[key]["value"] = (1.0 * val) / fga_totals["def"]
                if old_val_override:
                    mutable_acc[key]["old_value"] = (1.0 * old_val) / fga_totals["def"]
        elif key == "game_info":
            raise NotImplementedError(
                "complete_weighted_avg: the 'game_info' finishing branch "
                "(LineupUtils.ts:860-865, associative-array -> list "
                "conversion via getGameInfo) is not yet ported -- see the "
                "mbb_lineup_stats module docstring. Unreachable via "
                "calculate_aggregated_lineup_stats today because "
                "weighted_avg already raises before a 'game_info' key can "
                "land in the accumulator; a later task must implement it if "
                "that upstream guard is ever relaxed."
            )


def build_efficiency_margins(
    mutable_stat_set: LineupStatSet,
    key_override: str | None = None,
) -> None:
    """Derive ``off_net`` / ``off_raw_net`` on a stat set, in place.

    Faithful port of ``LineupUtils.buildEfficiencyMargins`` (``LineupUtils.ts:145``).
    ``off_net`` is ``off_adj_ppp - def_adj_ppp`` (adjusted efficiency margin);
    ``off_raw_net`` is ``off_ppp - def_ppp`` (raw/unadjusted margin). Both are
    only written when their two source fields are both present on
    ``mutable_stat_set``.

    Args:
        mutable_stat_set: The ``LineupStatSet`` (or team-report equivalent)
            to mutate in place.
        key_override: ``"value"`` or ``"old_value"`` -- which sub-key to
            read from the source fields and write into ``off_net`` /
            ``off_raw_net``. When ``None`` (the default), the upstream
            ``nonLuckKey`` fallback applies: use ``"old_value"`` if
            ``mutable_stat_set["off_ppp"]["old_value"]`` is present,
            otherwise ``"value"``. When given explicitly, the written field
            is merged onto any existing ``off_net`` / ``off_raw_net`` dict
            (so a second call with the other key preserves the first call's
            key) rather than replacing it outright.

    Returns:
        None. ``mutable_stat_set`` is mutated in place.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_lineup_stats import build_efficiency_margins

            build_efficiency_margins(team_info, "value")
            off_ppp = team_info.get("off_ppp")
            if isinstance(off_ppp, dict) and off_ppp.get("old_value") is not None:
                build_efficiency_margins(team_info, "old_value")
            print(team_info["off_net"]["value"])

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    off_ppp = mutable_stat_set.get("off_ppp")
    non_luck_key = key_override or (
        "old_value" if isinstance(off_ppp, dict) and off_ppp.get("old_value") is not None else "value"
    )

    off_adj = mutable_stat_set.get("off_adj_ppp")
    def_adj = mutable_stat_set.get("def_adj_ppp")
    if off_adj is not None and def_adj is not None:
        value = _field_val(off_adj, non_luck_key, 0.0) - _field_val(def_adj, non_luck_key, 0.0)
        if key_override:
            existing = mutable_stat_set.get("off_net")
            merged = dict(existing) if isinstance(existing, dict) else {}
            merged[key_override] = value
            mutable_stat_set["off_net"] = merged
        else:
            mutable_stat_set["off_net"] = {"value": value}

    off_ppp2 = mutable_stat_set.get("off_ppp")
    def_ppp = mutable_stat_set.get("def_ppp")
    if off_ppp2 is not None and def_ppp is not None:
        value = _field_val(off_ppp2, non_luck_key, 0.0) - _field_val(def_ppp, non_luck_key, 0.0)
        if key_override:
            existing = mutable_stat_set.get("off_raw_net")
            merged = dict(existing) if isinstance(existing, dict) else {}
            merged[key_override] = value
            mutable_stat_set["off_raw_net"] = merged
        else:
            mutable_stat_set["off_raw_net"] = {"value": value}


def calculate_aggregated_lineup_stats(lineups: list[LineupStatSet] | None) -> LineupStatSet:
    """Combine all lineups into a single team stat set.

    Faithful port of ``LineupUtils.calculateAggregatedLineupStats``
    (``LineupUtils.ts:106``). Seeds an accumulator from
    ``StatModels.emptyLineup()`` (``{"key": "empty", "doc_count": 0}``) plus
    an ``all_lineups`` sub-accumulator of the same shape, then merges every
    lineup via :func:`weighted_avg`: lineups without a truthy ``rapmRemove``
    key merge into the main accumulator, while ``rapmRemove`` lineups merge
    into ``all_lineups`` instead (their contribution is folded back in
    afterward). Calls :func:`complete_weighted_avg` to turn the main
    accumulator's weighted sums into weighted averages, then -- because
    ``StatModels.emptyLineup()`` always carries ``key``/``doc_count`` and so
    is never considered "empty" by the upstream ``lodash.isEmpty`` check --
    unconditionally re-merges the (now-averaged) team totals into
    ``all_lineups`` and finishes that sub-accumulator too. Finally rebuilds
    ``off_net`` / ``off_raw_net`` via :func:`build_efficiency_margins`
    (value-key always; old-value-key too when the team is in luck-adjusted
    mode, i.e. ``off_ppp.old_value`` is present) -- but only on the top-level
    result, matching upstream's "don't bother for all_lineups" comment.

    Args:
        lineups: The per-lineup ``LineupStatSet`` docs to fold together
            (e.g. the ES aggregation buckets under
            ``responses[0].aggregations.lineups.buckets``). ``None`` or an
            empty list yields an all-zero/empty team stat set (mirrors the
            upstream ``lineups || []`` guard).

    Returns:
        The aggregated team-total ``LineupStatSet``, including a nested
        ``all_lineups`` key holding the ``rapmRemove``-lineups-plus-team-total
        composite sub-aggregate.

    Raises:
        NotImplementedError: Propagated from :func:`weighted_avg` /
            :func:`complete_weighted_avg` if any lineup (or the accumulator)
            carries a ``game_info`` key -- see their docstrings.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_lineup_stats import calculate_aggregated_lineup_stats

            buckets = raw_response["responses"][0]["aggregations"]["lineups"]["buckets"]
            team_info = calculate_aggregated_lineup_stats(buckets)
            print(team_info["off_ppp"]["value"], team_info["off_poss"]["value"])

        RAPM-exclusion flag::

            buckets[1]["rapmRemove"] = True  # divert into all_lineups instead
            team_info = calculate_aggregated_lineup_stats(buckets)

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    team_info: LineupStatSet = {
        "key": "empty",
        "doc_count": 0,
        "all_lineups": {"key": "empty", "doc_count": 0},
    }
    for lineup in lineups or []:
        if not lineup.get("rapmRemove"):
            weighted_avg(team_info, lineup)
        else:
            # (the !rapmRemove lineups get incorporated below)
            weighted_avg(team_info["all_lineups"], lineup)

    complete_weighted_avg(team_info)

    if team_info["all_lineups"]:
        # (TODO upstream: only actually need to do this for _poss and
        # _adj_ppp, can save some CPU cycles. Also: StatModels.emptyLineup()
        # always carries {key, doc_count}, so this condition -- ported from
        # `!_.isEmpty(teamInfo.all_lineups)` -- is always truthy upstream
        # too; this branch always runs.)
        weighted_avg(team_info["all_lineups"], team_info)
        complete_weighted_avg(team_info["all_lineups"])

    # Rebuild net margin since the aggregated version won't be quite right:
    build_efficiency_margins(team_info, "value")
    off_ppp = team_info.get("off_ppp")
    if isinstance(off_ppp, dict) and off_ppp.get("old_value") is not None:
        # (luck adjusted mode)
        build_efficiency_margins(team_info, "old_value")
    # (don't bother for "all_lineups" since off_net is not used in any stats)

    return team_info


def get_stats_diff(
    stat_set1: LineupStatSet,
    stat_set2: LineupStatSet,
    off_title: str,
    def_title: str | None = None,
) -> LineupStatSet:
    """Straight (unweighted) field-by-field diff of two team stat sets.

    Faithful port of ``LineupUtils.getStatsDiff`` (``LineupUtils.ts:185``).
    For every field on ``stat_set1``, subtracts the matching field's
    ``value`` (and, when both sides carry one, ``old_value``) from
    ``stat_set2``. No possession weighting or regression -- this is a raw
    subtraction, unlike :func:`weighted_avg` / :func:`complete_weighted_avg`.

    Args:
        stat_set1: The "from" team stat set (e.g. this team).
        stat_set2: The "to subtract" team stat set (e.g. the opponent, or a
            prior period).
        off_title: Written into the result's ``off_title`` field verbatim.
        def_title: Written into the result's ``def_title`` field verbatim
            (``None`` when omitted, mirroring the upstream optional arg).

    Returns:
        A new ``LineupStatSet``: one ``{"value": ..., "old_value": ...,
        "override": ...}`` dict per field present on ``stat_set1``, plus
        ``off_title`` / ``def_title``. A field becomes ``None`` (the JS
        ``undefined`` analog) instead of a diff dict when either side is
        missing a ``value`` -- e.g. because that field was never populated
        for one of the two stat sets.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_lineup_stats import get_stats_diff

            diff = get_stats_diff(team_a, team_b, "Team A", "Team B")
            print(diff["off_ppp"]["value"])  # team_a.off_ppp - team_b.off_ppp

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    stats_diff: LineupStatSet = {}
    for key, start_val in stat_set1.items():
        to_sub = stat_set2.get(key)
        start_value = start_val.get("value") if isinstance(start_val, dict) else None
        to_sub_value = to_sub.get("value") if isinstance(to_sub, dict) else None
        if to_sub_value is None or start_value is None:
            stats_diff[key] = None
            continue
        start_old = start_val.get("old_value") if isinstance(start_val, dict) else None
        to_sub_old = to_sub.get("old_value") if isinstance(to_sub, dict) else None
        stats_diff[key] = {
            "value": start_value - to_sub_value,
            "old_value": None if (to_sub_old is None or start_old is None) else start_old - to_sub_old,
            "override": start_val.get("override") if isinstance(start_val, dict) else None,
        }
    stats_diff["off_title"] = off_title
    stats_diff["def_title"] = def_title
    return stats_diff


def _empty_lineup() -> LineupStatSet:
    """Port of ``StatModels.emptyLineup()`` (referenced throughout
    ``LineupUtils.ts``, e.g. ``LineupUtils.ts:120-122``). Returns a fresh
    dict each call -- callers must not share a single instance across
    multiple accumulators.
    """
    return {"key": "empty", "doc_count": 0}


def _get_player_set(lineup: LineupStatSet) -> dict[str, str]:
    """Port of the ``getPlayerSet`` closure inside ``lineupToTeamReport``
    (``LineupUtils.ts:283-291``).

    Returns ``{player_id: player_code}`` for every player on the lineup's
    roster, read from the ES hits payload at
    ``lineup.players_array.hits.hits[0]._source.players``. Every hop is
    optional-chained (mirroring the upstream ``?.``); a missing/malformed
    payload yields an empty dict rather than raising.
    """
    players_array = lineup.get("players_array")
    hits_outer = players_array.get("hits") if isinstance(players_array, dict) else None
    hits = hits_outer.get("hits") if isinstance(hits_outer, dict) else None
    first = hits[0] if isinstance(hits, list) and hits else None
    source = first.get("_source") if isinstance(first, dict) else None
    players = source.get("players") if isinstance(source, dict) else None
    return {p["id"]: p["code"] for p in (players or [])}


def _update_lineup_composition(
    mutable_teammate_info: dict[str, float] | None,
    player: str,
    lineup_info: LineupStatSet,
) -> None:
    """Port of ``LineupUtils.updateLineupComposition`` (``LineupUtils.ts:485``).

    Accumulates ``off_poss`` / ``def_poss`` from ``lineup_info`` onto
    ``mutable_teammate_info`` in place (a no-op when ``mutable_teammate_info``
    is ``None``, mirroring the upstream ``if (mutableTeammateInfo)`` guard).
    ``player`` is accepted for signature parity with the upstream function
    but, like upstream, is unused in the body.
    """
    del player  # (unused, kept for signature parity with LineupUtils.ts:485)
    if mutable_teammate_info is not None:
        mutable_teammate_info["off_poss"] += _num(lineup_info, "off_poss", 0.0)
        mutable_teammate_info["def_poss"] += _num(lineup_info, "def_poss", 0.0)


def _copy_and_zero(mutable_to_zero: LineupStatSet, from_: LineupStatSet) -> None:
    """Port of ``LineupUtils.copyAndZero`` (``LineupUtils.ts:871``).

    Zero-fills every non-``IGNORE_FIELDS`` key present on ``from_`` onto
    ``mutable_to_zero`` in place, as ``{"value": 0.0}``. Used by
    :func:`lineup_to_team_report` to give a player who never appears OFF
    (e.g. played every lineup) a same-shaped, all-zero OFF stat set instead
    of a bare ``{"key": ...}`` stub.
    """
    for key in from_:
        if key not in IGNORE_FIELDS:
            mutable_to_zero[key] = {"value": 0.0}


def _is_complement_lineup(player: str, on_lineup: LineupStatSet, off_lineup: LineupStatSet) -> bool:
    """Port of ``LineupUtils.isComplementLineup`` (``LineupUtils.ts:888``).

    ``True`` iff ``off_lineup`` shares exactly 4 of its 5 players with
    ``on_lineup`` (excluding ``player`` itself) -- i.e. ``off_lineup`` is
    "the same lineup, minus ``player``, plus one substitute". Used to find
    the OFF-lineup complements that feed a player's "replacement" on/off
    diff.
    """
    on_lineup_player_map = _get_player_set(on_lineup)
    off_lineup_player_ids = _get_player_set(off_lineup).keys()
    overlap = sum(1 for pid in off_lineup_player_ids if pid != player and pid in on_lineup_player_map)
    return overlap == 4


def _calc_harmonic_mean(w1: float, w2: float) -> float:
    """Port of ``LineupUtils.calcHarmonicMean`` (``LineupUtils.ts:919``)."""
    return 2.0 / (1.0 / w1 + 1.0 / w2)


def _combine_replacement_on_off(
    mutable_replacement_obj: LineupStatSet,
    key_source: list[str],
    regress_diffs: float = 0.0,
    rep_on_off_diag_mode: int = 0,
) -> None:
    """Port of ``LineupUtils.combineReplacementOnOff`` (``LineupUtils.ts:924``).

    For every "my lineup" (a lineup ``mutable_replacement_obj``'s player
    appeared ON in) that accumulated at least one complementary OFF lineup
    into its ``offLineups`` sub-accumulator, finishes ``offLineups`` via
    :func:`complete_weighted_avg`, then overwrites each field on the
    ``myLineups`` entry with either a harmonic mean (for
    ``total_*``/``*_poss`` fields -- ``key_source``-derived
    ``harmonic_weights``) or a straight on-minus-off diff (everything else).
    Finally re-merges the diffed ``myLineups`` entries into
    ``mutable_replacement_obj`` via :func:`weighted_avg` and finishes with
    ``complete_weighted_avg(..., harmonic_weighting=True, regress_diffs)``.

    Mutates ``mutable_replacement_obj`` (and every retained ``myLineups``
    entry) in place; returns ``None``.

    **Faithful bug-for-bug port** (see module docstring): the ``old_value``
    branch of the per-key harmonic-mean loop reads
    ``my_lineup[key]["old_value"]`` *after* ``my_lineup[key]`` has already
    been reassigned to a fresh ``{"value": ...}`` dict (no ``old_value``
    key), so it always resolves to ``0.0`` and the "harmonic mean of old
    values" branch is unreachable in the ported code, exactly as it is
    upstream.

    Args:
        mutable_replacement_obj: The player's ``replacement`` entry (from
            :func:`lineup_to_team_report`'s ``players`` list), carrying
            ``myLineups`` (the ON lineups this player appeared in, each
            augmented with an ``offLineups`` sub-accumulator of
            complementary OFF-lineup stats).
        key_source: The field-name list used to derive which fields get
            harmonic-mean treatment vs. straight diffing (upstream passes
            ``_.keys(playerObj.on)`` -- the finished ON stat set's field
            names).
        regress_diffs: Forwarded to the final ``complete_weighted_avg`` call.
        rep_on_off_diag_mode: When ``> 0``, retains ``myLineups`` (each
            entry additionally gets an ``onLineup`` shallow-clone snapshot
            taken before this function mutates it) instead of deleting the
            key entirely.

    Returns:
        None. ``mutable_replacement_obj`` is mutated in place.
    """
    harmonic_weights = {k for k in key_source if k.startswith("total_") or k.endswith("_poss")}

    weighted_lineups: list[LineupStatSet] = []
    for my_lineup in mutable_replacement_obj.get("myLineups") or []:
        off_lineups = my_lineup.get("offLineups") or _empty_lineup()
        if "off_poss" not in off_lineups:
            # (remove lineups with no possessions at all -- no complement
            # OFF lineup was ever found for this ON lineup)
            continue
        weighted_lineups.append(my_lineup)

    for my_lineup in weighted_lineups:
        if rep_on_off_diag_mode > 0:
            my_lineup["onLineup"] = dict(my_lineup)  # (shallow clone, pre-mutation)

        off_lineups = my_lineup.get("offLineups") or _empty_lineup()
        complete_weighted_avg(off_lineups)  # mutates off_lineups in place

        for key in harmonic_weights:
            existing = my_lineup.get(key)
            old_val_override = existing.get("override") if isinstance(existing, dict) else None
            my_val = _num(my_lineup, key, 0.0)
            off_val = _num(off_lineups, key, 0.0)
            if my_val > 0 and off_val > 0:
                my_lineup[key] = {"value": _calc_harmonic_mean(my_val, off_val)}
            else:
                my_lineup[key] = {"value": 0.0}
            if old_val_override:
                # (bug-for-bug: reads my_lineup[key]["old_value"] off the
                # object just reassigned above -- see docstring)
                cur_old_val = _field_val(my_lineup.get(key), "old_value", 0.0)
                off_old_val = _field_val(off_lineups.get(key), "old_value", 0.0)
                if cur_old_val > 0 and off_old_val > 0:
                    my_lineup[key]["old_value"] = _calc_harmonic_mean(cur_old_val, off_old_val)
                    my_lineup[key]["override"] = old_val_override
                else:
                    my_lineup[key]["old_value"] = 0.0

        for key, field in list(my_lineup.items()):
            if key in IGNORE_FIELDS or key in harmonic_weights:
                continue
            val = _field_val(field, "value", 0.0)
            off_field = off_lineups.get(key)
            off_val = _field_val(off_field, "value", 0.0)
            new_field: LineupStatSet = {"value": val - off_val}
            override = field.get("override") if isinstance(field, dict) else None
            if override:
                old_val = _field_val(field, "old_value", 0.0)
                off_old_val = _field_val(off_field, "old_value", 0.0)
                new_field["old_value"] = old_val - off_old_val
                new_field["override"] = override
            my_lineup[key] = new_field

    if rep_on_off_diag_mode == 0:
        mutable_replacement_obj.pop("myLineups", None)
    else:
        # (remove any lineups that don't contribute)
        mutable_replacement_obj["myLineups"] = weighted_lineups

    for lineup in weighted_lineups:
        weighted_avg(mutable_replacement_obj, lineup)
    complete_weighted_avg(mutable_replacement_obj, True, regress_diffs)


def lineup_to_team_report(
    lineup_report: LineupStatSet,
    inc_replacement: bool = False,
    regress_diffs: float = 0.0,
    rep_on_off_diag_mode: int = 0,
) -> LineupStatSet:
    """Build per-player on/off splits out of a team's lineups.

    Faithful port of ``LineupUtils.lineupToTeamReport`` (``LineupUtils.ts:277``).
    For every distinct player across ``lineup_report["lineups"]``, partitions
    the team's lineups into ON (the player was on the floor) and OFF (they
    weren't) buckets, merging each bucket via :func:`weighted_avg` /
    :func:`complete_weighted_avg`. Also builds a ``teammates`` map of
    possession overlap with every other player, and -- when
    ``inc_replacement=True`` -- a "replacement" on-minus-off composite via
    :func:`_combine_replacement_on_off`.

    Lineups whose ``key`` is the empty string are skipped in the
    on/off-partition loop (workaround for an upstream data issue, tracked
    as upstream issue #53) but still contribute to the player roster.
    Every lineup's ``rapmRemove`` key (if present, e.g. left over from a
    prior :func:`calculate_aggregated_lineup_stats` call sharing the same
    input list) is deleted as a side effect while building the roster --
    ``lineup_to_team_report`` itself never consults ``rapmRemove``.

    Args:
        lineup_report: ``{"lineups": [...], "avgOff": ..., "error_code":
            ...}`` -- the per-team lineup list plus metadata (mirrors
            upstream's ``LineupStatsModel``). Only ``lineups`` and
            ``error_code`` are consumed here.
        inc_replacement: When ``True``, additionally builds each player's
            ``replacement`` on-minus-off composite (more expensive -- scans
            every OFF lineup against every ON lineup for a 4-of-5-shared-
            players complement match).
        regress_diffs: Forwarded to :func:`_combine_replacement_on_off`'s
            final ``complete_weighted_avg`` call -- regression toward
            ~1000 possessions for the replacement diff (only meaningful
            when ``inc_replacement=True``).
        rep_on_off_diag_mode: When ``> 0``, retains diagnostic detail
            (``myLineups`` on each player's replacement entry, plus
            ``lineupUsage`` bookkeeping) instead of discarding it after use.

    Returns:
        ``{"playerMap": {code: id}, "players": [...], "error_code": ...}``.
        Each entry in ``players`` is ``{"playerId", "playerCode",
        "teammates", "on", "off", "replacement"}`` -- ``on``/``off`` are
        finished ``LineupStatSet`` averages (or, for a player who's always
        ON, an all-zero ``off``); ``replacement`` is ``None`` unless
        ``inc_replacement=True``.

    Raises:
        NotImplementedError: Propagated from :func:`weighted_avg` /
            :func:`complete_weighted_avg` if any lineup carries a
            ``game_info`` key -- not exercised by this module's oracle
            fixtures (see module docstring).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_lineup_stats import lineup_to_team_report

            report = lineup_to_team_report({"lineups": buckets, "error_code": None})
            for player in report["players"]:
                print(player["playerId"], player["on"]["off_poss"]["value"])

        With replacement (on-minus-off) splits::

            report = lineup_to_team_report(
                {"lineups": buckets, "error_code": None},
                inc_replacement=True,
                regress_diffs=-500,
            )

    See Also:
        * `hoopR`_ -- R-side college basketball data + on/off analysis.
        * `wehoop`_ -- women's college basketball counterpart.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    lineups = lineup_report.get("lineups") or []

    all_players_set: dict[str, str] = {}
    for lineup in lineups:
        lineup.pop("rapmRemove", None)  # (ugly hack/coupling with RAPM utils, ported verbatim)
        all_players_set.update(_get_player_set(lineup))

    player_map = {code: pid for pid, code in all_players_set.items()}

    players: list[LineupStatSet] = []
    for player_id, player_code in all_players_set.items():
        teammates: dict[str, LineupStatSet] = {
            player: {
                "on": {"off_poss": 0.0, "def_poss": 0.0},
                "off": {"off_poss": 0.0, "def_poss": 0.0},
            }
            for player in all_players_set
        }

        replacement: LineupStatSet | None = None
        if inc_replacement:
            my_lineups: list[LineupStatSet] = []
            for lineup in lineups:
                players_set = _get_player_set(lineup)
                if player_id in players_set and lineup.get("key") != "":
                    my_lineups.append(
                        {
                            "offLineups": _empty_lineup(),
                            "offLineupKeys": [],
                            "onLineup": _empty_lineup(),
                            **lineup,
                        }
                    )
            replacement = {
                "key": f"'r:On-Off' {player_id}",
                "lineupUsage": {},
                "myLineups": my_lineups,
            }

        players.append(
            {
                "playerId": player_id,
                "playerCode": player_code,
                "teammates": teammates,
                "on": {"key": f"'On' {player_id}"},
                "off": {"key": f"'Off' {player_id}"},
                "replacement": replacement,
            }
        )

    mutable_state: LineupStatSet = {
        "playerMap": player_map,
        "players": players,
        "error_code": lineup_report.get("error_code"),
    }

    for lineup in lineups:
        if lineup.get("key") == "":
            # (workaround for upstream #53 pending fix)
            continue
        players_set = _get_player_set(lineup)

        for player_obj in mutable_state["players"]:
            if player_obj["playerId"] in players_set:
                # ON!
                weighted_avg(player_obj["on"], lineup)
                for player in players_set:
                    _update_lineup_composition(player_obj["teammates"].get(player, {}).get("on"), player, lineup)
            else:
                # OFF!
                weighted_avg(player_obj["off"], lineup)
                for player in players_set:
                    _update_lineup_composition(player_obj["teammates"].get(player, {}).get("off"), player, lineup)

                if inc_replacement:
                    replacement_obj = player_obj.get("replacement") or {}
                    for on_lineup in replacement_obj.get("myLineups") or []:
                        if not _is_complement_lineup(player_obj["playerId"], on_lineup, lineup):
                            continue
                        if rep_on_off_diag_mode > 0:
                            off_lineup_keys = on_lineup.get("offLineupKeys")
                            if off_lineup_keys is not None:
                                off_lineup_keys.append(lineup["key"])
                            lineup_usage = replacement_obj.setdefault("lineupUsage", {})
                            if lineup["key"] not in lineup_usage:
                                lineup_usage[lineup["key"]] = {
                                    "poss": _num(lineup, "off_poss", 0.0),
                                    "keyArray": lineup["key"].split("_"),
                                    "overlap": 1,
                                }
                            else:
                                tmp = lineup_usage.get(lineup["key"])
                                if tmp and tmp.get("overlap"):
                                    tmp["overlap"] += 1
                        if on_lineup.get("offLineups"):
                            weighted_avg(on_lineup["offLineups"], lineup)

    # Finish off the weighted averages:
    for player_obj in mutable_state["players"]:
        if "off_poss" in player_obj["on"]:
            complete_weighted_avg(player_obj["on"])
            if "off_poss" not in player_obj["off"]:
                _copy_and_zero(player_obj["off"], player_obj["on"])
        if "off_poss" in player_obj["off"]:
            complete_weighted_avg(player_obj["off"])
        if inc_replacement and player_obj.get("replacement"):
            _combine_replacement_on_off(
                player_obj["replacement"],
                list(player_obj["on"].keys()),
                regress_diffs,
                rep_on_off_diag_mode,
            )

    return mutable_state
