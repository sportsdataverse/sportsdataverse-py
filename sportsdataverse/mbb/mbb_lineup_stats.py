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

Deliberately NOT ported in this task:

- ``completeWeightedAvg`` (``LineupUtils.ts:752`` onward) — the companion
  function that *finishes* a weighted-average accumulator by dividing by
  the accumulated weight totals (and recalculates transition/scramble PPP
  via ``recalculatePlayTypePoss``). ``weighted_avg`` only performs the
  accumulation (weighted-sum) half; the brief for this task scopes the
  module to ``weighted_avg`` alone, and ``completeWeightedAvg`` is deferred
  to whichever later task ports ``calculate_aggregated_lineup_stats`` /
  ``lineup_to_team_report``. Consequently the accumulator this function
  produces holds weighted **sums**, not weighted **averages** — callers
  must not read ``mutable_acc[key]["value"]`` as an average until
  ``complete_weighted_avg`` (not yet implemented) has been applied.
- The ``game_info`` merge branch (``LineupUtils.ts:722-746``), which is
  reachable inside ``weightedAvg`` despite ``game_info`` being a member of
  ``ignoreFieldSet`` (the upstream code special-cases it via a separate
  ``else if (key == "game_info")`` branch). That branch depends on
  ``LineupUtils.getGameInfo`` (``LineupUtils.ts:222-270``), an unrelated ES
  date-histogram-bucket parser used by ``lineupToTeamReport`` /
  ``getGameInfo`` tests, not exercised by this task's oracle fixtures (the
  vendored ``sampleLineupStatsResponse`` lineup buckets carry no
  ``game_info`` key). To avoid silently dropping data if a future caller
  passes a lineup doc that *does* carry ``game_info``, :func:`weighted_avg`
  raises :class:`NotImplementedError` for that key instead of ignoring it.
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
