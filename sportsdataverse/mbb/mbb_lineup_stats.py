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

Deliberately NOT ported:

- The ``game_info`` merge/finishing branches
  (``weightedAvg`` ``LineupUtils.ts:722-746`` and ``completeWeightedAvg``
  ``LineupUtils.ts:849-853``), which are reachable despite ``game_info``
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
                "(LineupUtils.ts:849-853, associative-array -> list "
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
