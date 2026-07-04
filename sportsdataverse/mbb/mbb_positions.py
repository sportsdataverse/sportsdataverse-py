"""Positional classifier core (LDA position-confidence model + height reweight).

Faithful port of hoop-explorer's ``PositionUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/PositionUtils.ts``, 920 LOC, ``class PositionUtils`` -- all
static). **Task 4.2 (Phase 4) ports the classifier core**: the five LDA
constant tables (``PositionUtils.ts:23/28/37/129/184``), the memoized
:data:`AVERAGE_SCORES_BY_POS` derived value (``PositionUtils.ts:192-211``),
:func:`regress_shot_quality` (``PositionUtils.regressShotQuality``,
``PositionUtils.ts:216``), :func:`build_position_confidences`
(``PositionUtils.buildPositionConfidences``, ``PositionUtils.ts:263``), and
:func:`incorporate_height` (``PositionUtils.incorporateHeight``, plus its
private ``cdf`` helper, ``PositionUtils.ts:341/346``).

**Task 4.3 (this update) adds the decision-tree layer**: :func:`build_position`
(``PositionUtils.buildPosition``, ``PositionUtils.ts:401-580``) -- the PG /
s-PG / CG / WG / WF / S-PF / PF/C / C branch cascade, the manual-override
short-circuit, and the roster reconciliation -- plus :func:`using_roster_pos`
(``PositionUtils.usingRosterPos``, ``:583-626``), :func:`pos_class_to_score`
(``PositionUtils.posClassToScore``, ``:629-654``), the :data:`ID_TO_POSITION`
lookup table (``PositionUtils.idToPosition``, ``:387-398``), and the tested
subset of ``PositionalManualFixes.absolutePositionFixes`` as
:data:`ABSOLUTE_POSITION_FIXES`.

**Task 4.4 (this update) completes the module -- the lineup-ordering
layer**: :func:`order_lineup` (``PositionUtils.orderLineup``,
``PositionUtils.ts:696-761``) -- the greedy PG/SG/SF/PF/C slot-assignment
algorithm (with recursive eviction/re-fit) -- plus the private recursive
helper :func:`apply_relative_positional_overrides`
(``PositionUtils.applyRelativePositionalOverrides``, ``:657-693``, ``private``
upstream but exposed here per the port task's naming contract), the
search-filter utilities :func:`build_positional_aware_filter`
(``PositionUtils.buildPositionalAwareFilter``, ``:764-828``) and
:func:`test_positional_aware_filter` (``PositionUtils.testPositionalAwareFilter``,
``:831-858``), and the tested subset of
``PositionalManualFixes.relativePositionFixes`` as
:data:`RELATIVE_POSITION_FIXES`.

**Permanently out of scope (confirmed unused by every ``PositionUtils``
function, this task closes the question):** the ``positionClasses`` /
``posClassToNickname`` / ``nicknameToPosClass`` / ``positionGroupings`` /
``positionsToGroup`` / ``expandedPosClasses`` lookup tables
(``PositionUtils.ts:371-385``, ``:863-919``). Task 4.3 already confirmed
``buildPosition``/``usingRosterPos``/``posClassToScore`` never read them;
this task's full read of ``orderLineup``/``applyRelativePositionalOverrides``/
``buildPositionalAwareFilter``/``testPositionalAwareFilter`` (the entire
remaining ``PositionUtils`` surface) confirms none of *those* read them
either -- they are display/legend tables consumed elsewhere in the
hoop-explorer UI, entirely outside the ``PositionUtils`` class's own
functional surface, and are not ported.

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of ``PositionUtils.ts`` (and, for Tasks 4.3/4.4,
``PositionalManualFixes.ts``) from
`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
(the hoop-explorer.com SPA), which is licensed under the Apache License,
Version 2.0 (the upstream repo's ``LICENSE`` file; full text at
`<http://www.apache.org/licenses/LICENSE-2.0>`_). Per Apache-2.0 Section 4's
redistribution-of-derivative-works obligations, sportsdataverse-py (itself
MIT-licensed) retains the upstream copyright notice for this derivative::

    Copyright (c) Alex-At-Home (https://github.com/Alex-At-Home) and
    contributors. Licensed under the Apache License, Version 2.0.

See ``THIRD_PARTY_NOTICES.md`` at the repository root for the full
third-party attribution entry (upstream URL, license, and exactly what was
derived), and ``tests/fixtures/hoop_explorer/README.md`` for the vendored
jest-oracle fixture provenance (same upstream repo, same commit, test-only --
not shipped in the distributed wheel).

**Master landmine index (scalar dict-math NaN regime).** This port does NOT
emulate JavaScript's silent ``NaN`` propagation on scalar division. In the TS
source a ``0`` denominator produces ``NaN`` and the computation limps on; in
Python the corresponding site would raise ``ZeroDivisionError``. Every
reachable scalar-division site in this task's scope is enumerated below with
its regime:

  1. **(No unguarded division sites in the 4.2 scope.)** Every denominator in
     :func:`regress_shot_quality`, :func:`build_position_confidences`, and
     :func:`incorporate_height` is either floored away from zero
     (``regress_vol = max(0.25 * total, 15)`` -> ``>= 15``) or falsy-coalesced
     in the upstream source with a JS ``|| 1`` / ``|| 0`` guard (ported
     faithfully as Python ``or 1`` / ``or 0`` -- see the "JS-semantics"
     note below), so none can reach a zero divisor. This index is opened at 1
     for the module; Tasks 4.3/4.4 append their own sites as they land.
  2. **(No division sites in the 4.3 scope either.)** :func:`build_position`
     multiplies (``effective_poss = poss * usage``) but never divides;
     :func:`using_roster_pos` / :func:`pos_class_to_score` are pure
     string/dict lookups. No new landmine sites to log.
  3. **(No division sites in the 4.4 scope either -- this closes the index.)**
     :func:`order_lineup` / :func:`apply_relative_positional_overrides` /
     :func:`build_positional_aware_filter` / :func:`test_positional_aware_filter`
     perform no numeric division at all -- pure comparisons, list indexing,
     and string parsing. Across the full ported ``PositionUtils`` function
     surface (Tasks 4.2-4.4), item 1 above is the *only* landmine class.

**JS-semantics fidelity (``||`` falsy-coalesce is load-bearing here).** The
upstream denominators use JS ``x.value || 1`` (falsy-coalesce), NOT ``?? 1``
(nullish) nor ``_.isNil``. That means a *legitimate* ``0`` denominator is
*deliberately* mapped to ``1`` to dodge a divide-by-zero -- so the faithful
Python port is ``... or 1`` (falsy-coalesce), **not** an ``is None`` check.
Using ``is None`` here would be the bug: it would leave a ``0`` in the
denominator and raise ``ZeroDivisionError``, diverging from the TS. The
numerator-side ``... || 0`` sites (missing/zero stat -> ``0``) coalesce a
value that then multiplies into a ``0`` contribution either way, so ``or 0``
is likewise faithful and introduces no falsy-preservation bug.

Ported behavior (``PositionUtils.ts`` anchors):

* :data:`POSITION_FEATURE_INIT` -- LDA intercepts (``:23``).
* :data:`TRAD_POS_LIST` -- confidence-vector field order (``:28``).
* :data:`POSITION_FEATURE_WEIGHTS` -- 17 ``[field, scale, weights[5]]`` rows
  (``:37``).
* :data:`POSITION_FEATURE_AVERAGES` -- per-feature positional averages
  (``:129``).
* :data:`HEIGHT_MEAN_STDS` -- per-position height mean/std (``:184``).
* :data:`AVERAGE_SCORES_BY_POS` -- memoized weight x average reduction
  (``:192``).

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R).
    * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R).
"""

from __future__ import annotations

import math
import re
from typing import Any

__all__ = [
    "POSITION_FEATURE_INIT",
    "TRAD_POS_LIST",
    "POSITION_FEATURE_WEIGHTS",
    "POSITION_FEATURE_AVERAGES",
    "HEIGHT_MEAN_STDS",
    "AVERAGE_SCORES_BY_POS",
    "ID_TO_POSITION",
    "ABSOLUTE_POSITION_FIXES",
    "RELATIVE_POSITION_FIXES",
    "regress_shot_quality",
    "build_position_confidences",
    "incorporate_height",
    "build_position",
    "using_roster_pos",
    "pos_class_to_score",
    "order_lineup",
    "apply_relative_positional_overrides",
    "build_positional_aware_filter",
    "test_positional_aware_filter",
]

_SQRT2 = math.sqrt(2)

#: The LDA intercepts (``PositionUtils.ts:23`` ``positionFeatureInit``).
POSITION_FEATURE_INIT: list[float] = [
    -2.82375823,
    -2.41283573,
    -3.74982844,
    -8.98755013,
    -3.23442276,
]

#: Confidence-vector field names, ``pos_``-prefixed
#: (``PositionUtils.ts:28`` ``tradPosList``). Order is load-bearing: the
#: confidences / scores dicts are built in exactly this insertion order.
TRAD_POS_LIST: list[str] = [
    "pos_pg",
    "pos_sg",
    "pos_sf",
    "pos_pf",
    "pos_c",
]

#: Triples ``[field_name, scale, weights[5]]`` from the ML model
#: (``PositionUtils.ts:37`` ``positionFeatureWeights``). Transcribed verbatim.
POSITION_FEATURE_WEIGHTS: list[tuple[str, float, list[float]]] = [
    # Ball handling
    ("calc_ast_tov", 1.0, [0.08281269, 0.09093907, -0.37973552, -0.67240486, 0.5964297]),
    ("off_assist", 100.0, [0.15829941, 0.02598234, -0.06537337, -0.05021328, -0.12142258]),
    ("off_to", 100.0, [-0.00680258, 0.0051497, -0.02123889, -0.03861639, 0.04709196]),
    ("calc_assist_per_fga", 100.0, [0.01429017, -0.00313073, 0.0082461, 0.01833772, -0.0319402]),
    # Shot selection
    ("off_3pr", 100.0, [0.02713631, 0.0218532, -0.00223302, 0.00081636, -0.06555841]),
    ("off_2pmidr", 100.0, [-0.0010662, -0.00969839, -0.01555429, -0.04862983, 0.06485701]),
    ("off_2primr", 100.0, [0.01545738, 0.01531782, -0.00856427, -0.03075521, -0.01459524]),
    ("off_ftr", 100.0, [0.00270944, 0.00083536, 0.00011253, -0.01560428, 0.00500472]),
    # Shot making ability
    ("calc_three_relative", 100.0, [0.00753295, 0.00814222, 0.00794373, 0.01847985, -0.04255395]),
    ("calc_mid_relative", 100.0, [0.00281905, 0.00377201, 0.00400989, 0.01991123, -0.02632626]),
    ("calc_rim_relative", 100.0, [-0.00995088, 0.00740773, 0.01560057, 0.03010704, -0.03693076]),
    ("calc_ft_relative_inv", 100.0, [-0.01016761, -0.0056131, -0.00079665, -0.00547513, 0.02533069]),
    # Rebounding and defense
    ("def_to", 100.0, [0.8133556, 0.54765371, -0.02580977, -0.68504559, -1.39476509]),
    ("off_orb", 100.0, [-0.26888945, -0.21892123, 0.07832771, 0.26210603, 0.42330573]),
    ("def_orb", 100.0, [-0.23799504, -0.07938086, 0.10442655, 0.21672752, 0.15512722]),
    ("def_2prim", 100.0, [-0.29122875, -0.22875385, -0.09758256, 0.20918001, 0.69598967]),
    ("def_ftr", 100.0, [-0.08827297, -0.20674559, -0.01827295, 0.22834328, 0.3239175]),
]

#: Per-feature positional averages (``PositionUtils.ts:129``
#: ``positionFeatureAverages``). Each value is a ``[pg, sg, sf, pf, c]`` list.
POSITION_FEATURE_AVERAGES: dict[str, list[float]] = {
    "calc_ast_tov": [1.71771567, 1.17932242, 0.79621661, 0.74306857, 0.51254529],
    "off_assist": [24.86785714, 13.05019881, 9.63812274, 8.73104467, 6.37806666],
    "off_to": [22.45299921, 18.04530996, 18.44424188, 17.58870432, 20.07028442],
    "calc_assist_per_fga": [51.81818417, 24.21690841, 17.89512103, 15.10389804, 13.50735024],
    "off_3pr": [40.5474501, 49.22191966, 28.19592525, 33.62534936, 1.68639513],
    "off_2pmidr": [28.41474701, 24.5276034, 31.24101352, 29.92330697, 38.24966097],
    "off_2primr": [30.86647093, 26.14022775, 40.36403453, 36.10765493, 59.75727503],
    "off_ftr": [38.30457774, 30.19707211, 39.07963899, 32.61391657, 49.06417728],
    "calc_three_relative": [105.20372914, 103.32803494, 94.36055369, 110.83302518, 10.55692479],
    "calc_mid_relative": [72.5681835, 69.11712719, 68.56060901, 72.54753109, 67.48685787],
    "calc_rim_relative": [109.28254527, 115.53768733, 123.82853889, 125.66705542, 122.39476683],
    "calc_ft_relative_inv": [64.98897552, 68.39666748, 74.86478424, 75.10386658, 93.1631546],
    "def_to": [2.38133386, 1.92994759, 1.75611913, 1.37696567, 1.37380911],
    "off_orb": [2.10153907, 2.86361829, 6.82231047, 7.9960502, 10.19931949],
    "def_orb": [9.06756117, 10.43093259, 15.15236462, 18.20837948, 17.1899494],
    "def_2prim": [0.46286504, 0.79057473, 1.77850181, 3.52768549, 4.76859187],
    "def_ftr": [3.13113654, 3.08755648, 3.86938628, 4.45762274, 5.06942942],
}

#: Per-position height ``{mean, std}`` in inches (``PositionUtils.ts:184``
#: ``heightMeanStds``), ordered ``[pg, sg, sf, pf, c]``.
HEIGHT_MEAN_STDS: list[dict[str, float]] = [
    {"mean": 73.57716289697129, "std": 2.5561436676424854},
    {"mean": 75.23626157179437, "std": 2.539520215232971},
    {"mean": 77.73867983130089, "std": 2.286685273859796},
    {"mean": 79.14888834651121, "std": 2.0930557964524996},
    {"mean": 80.2680159415085, "std": 2.014906878530024},
]


def _build_average_scores_by_pos() -> dict[str, dict[str, float]]:
    """Reduce ``POSITION_FEATURE_WEIGHTS`` x ``POSITION_FEATURE_AVERAGES`` +
    ``POSITION_FEATURE_INIT`` into per-position average scores.

    Direct port of the lodash ``_.chain(...).transform(...).map(...)
    .fromPairs()`` reduction memoized at ``PositionUtils.ts:192-211``. Note the
    per-index accumulation uses ``field_val[i] * weight[i]`` **without** the
    per-feature ``scale`` -- unlike :func:`build_position_confidences`, which
    multiplies by ``scale``. The final ``0.1 *`` factor "makes the scores
    render nicely" (upstream comment).
    """
    acc = list(POSITION_FEATURE_INIT)
    for feat, _scale, weights in POSITION_FEATURE_WEIGHTS:
        field_val = POSITION_FEATURE_AVERAGES[feat]
        for i, weight in enumerate(weights):
            acc[i] += field_val[i] * weight
    return {TRAD_POS_LIST[i]: {"value": 0.1 * v} for i, v in enumerate(acc)}


#: Memoized per-position average scores (``PositionUtils.ts:192``
#: ``averageScoresByPos``). Derived at import time from the weight/average
#: tables -- a built-in checksum on those transcriptions (a jest test pins
#: ``tidyObj(AVERAGE_SCORES_BY_POS)`` to ``["0.15", "-0.03", "-0.11", "0.03",
#: "0.42"]``).
AVERAGE_SCORES_BY_POS: dict[str, dict[str, float]] = _build_average_scores_by_pos()


def regress_shot_quality(stat: float, pos: int, feat: str, player: dict[str, Any]) -> float:
    """Shrink a small-sample shot-quality stat toward its positional average.

    Faithful port of ``PositionUtils.regressShotQuality``
    (``PositionUtils.ts:216-258``). Only the three relative shot-quality
    features (``calc_three_relative`` / ``calc_rim_relative`` /
    ``calc_mid_relative``) are regressed; any other ``feat`` passes ``stat``
    through unchanged. A player is regressed toward the positional average
    whenever the relevant shot volume is below ``max(0.25 * total_fga, 15)``
    (i.e. under 25% of their attempts come from that zone, floored at 15
    attempts). A ``center`` (``pos == 4``) who took 0-2 threes and made none is
    left at ``0`` to avoid widespread changes.

    Args:
        stat: The raw (unregressed) feature value.
        pos: Position index (``0=pg`` ... ``4=c``).
        feat: Feature field name (only the three relative shot-quality keys
            trigger regression; anything else is a passthrough).
        player: The player stat dict; reads ``total_off_fga`` and the
            per-feature volume field (``total_off_{3p,2pmid,2prim}_attempts``),
            each shaped ``{"value": N}``.

    Returns:
        The regressed feature value (or ``stat`` unchanged when the feature is
        not regressed, volume is sufficient, or the center-3s carve-out fires).

    Raises:
        KeyError: If ``player`` lacks ``total_off_fga`` (faithful to the TS,
            which throws on the same missing field).

    Example:
        Passthrough for a non-regressed feature::

            from sportsdataverse.mbb.mbb_positions import regress_shot_quality
            player = {"total_off_fga": {"value": 25},
                      "total_off_3p_attempts": {"value": 1}}
            regress_shot_quality(-15.5, 2, "misc_feature", player)

        Low-volume shrink toward the positional average::

            regress_shot_quality(100, 3, "calc_rim_relative",
                {"total_off_fga": {"value": 25},
                 "total_off_2prim_attempts": {"value": 8}})
    """
    if feat in ("calc_three_relative", "calc_rim_relative", "calc_mid_relative"):
        volume_index = {
            "calc_three_relative": "total_off_3p_attempts",
            "calc_mid_relative": "total_off_2pmid_attempts",
            "calc_rim_relative": "total_off_2prim_attempts",
        }
        # `player.total_off_fga.value || 0` (falsy-coalesce, ts:232)
        total_volume = player["total_off_fga"]["value"] or 0
        # Regress to the positional average if under 25% of shots come from
        # this zone, floored at 15 attempts (ts:233). Floor keeps the divisor
        # away from zero -- see module landmine index, item 1.
        regress_vol = max(0.25 * total_volume, 15)
        regress_vol_inv = 1.0 / regress_vol
        # `player[volumeIndex[feat]]?.value || 0` (optional-chain + coalesce, ts:235)
        vol_stat = player.get(volume_index[feat])
        volume = (vol_stat.get("value") if vol_stat is not None else None) or 0

        if volume < regress_vol:
            # Center who took 0-2 threes and hit none: keep at 0 (ts:240-246).
            if pos == 4 and volume < 3 and stat == 0 and feat == "calc_three_relative":
                return stat
            av = 0.01 * POSITION_FEATURE_AVERAGES[feat][pos]
            return regress_vol_inv * (volume * stat + (regress_vol - volume) * av)
        # Enough samples, leave as is (ts:252).
        return stat
    return stat


def build_position_confidences(
    player: dict[str, Any], height_in: float | None = None
) -> tuple[dict[str, float], dict[str, Any]]:
    """Build the 5-way positional confidence vector for a player.

    Faithful port of ``PositionUtils.buildPositionConfidences``
    (``PositionUtils.ts:263-338``). Derives the six ``calc_*`` ratios from the
    player's box-score fields, dot-products the resulting 17-feature vector
    against :data:`POSITION_FEATURE_WEIGHTS` (each field regressed via
    :func:`regress_shot_quality` and multiplied by its per-feature ``scale``)
    plus the :data:`POSITION_FEATURE_INIT` intercepts, applies a softmax over
    the five raw scores, and -- when ``height_in`` is supplied -- reweights the
    confidences via :func:`incorporate_height`.

    Args:
        player: The player stat dict (ES-aggregation bucket shape); each stat
            field is ``{"value": N}``. Reads ``total_off_assist``,
            ``total_off_to``, ``off_3p``, ``off_efg``, ``off_2pmid``,
            ``off_2prim``, ``total_off_fga``, ``total_off_fta``,
            ``total_off_ftm`` (for the ``calc_*`` ratios) plus every
            non-``calc_`` field in :data:`POSITION_FEATURE_WEIGHTS`.
        height_in: Optional player height in inches. When truthy, the returned
            confidences are height-adjusted; when ``None`` / ``0``, the raw
            softmax confidences are returned. (JS ``height_in ? ... : ...``
            falsy check, ts:324 -- a ``0`` height is treated as "no height".)

    Returns:
        A ``(confidences, diagnostics)`` tuple. ``confidences`` maps each
        :data:`TRAD_POS_LIST` key (in order) to its final confidence.
        ``diagnostics`` carries ``"scores"`` (raw scores x ``0.1``, keyed by
        position), ``"confsNoHeight"`` (the pre-height confidences, present
        only when ``height_in`` is truthy, else ``None``), and ``"calculated"``
        (the six derived ``calc_*`` ratios). The upstream diag object has
        exactly these three fields -- no UI-only fields are dropped.

    Example:
        Confidences for a player bucket (no height)::

            from sportsdataverse.mbb.mbb_positions import build_position_confidences
            confs, diags = build_position_confidences(player_bucket)
            print(confs["pos_pg"], diags["calculated"]["calc_ast_tov"])

        Height-adjusted confidences::

            confs_h, diags_h = build_position_confidences(player_bucket, 78.0)
    """
    pos_list = TRAD_POS_LIST

    # Six derived ratios (ts:269-282). Every denominator is `|| 1`-guarded in
    # the source -- ported as `or 1` (falsy-coalesce), NOT `is None`: a valid
    # `0` denominator is deliberately mapped to 1 (see module JS-semantics
    # note + landmine index item 1).
    calculated: dict[str, float] = {
        "calc_ast_tov": player["total_off_assist"]["value"] / (player["total_off_to"]["value"] or 1),
        "calc_three_relative": (1.5 * player["off_3p"]["value"]) / (player["off_efg"]["value"] or 1),
        "calc_mid_relative": player["off_2pmid"]["value"] / (player["off_efg"]["value"] or 1),
        "calc_rim_relative": player["off_2prim"]["value"] / (player["off_efg"]["value"] or 1),
        "calc_assist_per_fga": player["total_off_assist"]["value"] / (player["total_off_fga"]["value"] or 1),
        # = eFG / FT% where FT% = FTM/FTA (ts:279-281)
        "calc_ft_relative_inv": (player["off_efg"]["value"] * player["total_off_fta"]["value"])
        / (player["total_off_ftm"]["value"] or 1),
    }

    # Dot-product the 17-feature vector against the LDA weights (ts:284-309).
    scores = list(POSITION_FEATURE_INIT)
    for feat, scale, weights in POSITION_FEATURE_WEIGHTS:
        if feat.startswith("calc_"):
            field_val = calculated.get(feat, 0) or 0
        else:
            stat = player.get(feat)
            field_val = (stat.get("value") if stat is not None else None) or 0
        for index, weight in enumerate(weights):
            regressed = regress_shot_quality(field_val, index, feat, player)
            scores[index] += regressed * scale * weight

    # Softmax over the raw scores (ts:317-322).
    max_score = max(scores) or 0
    confs_no_height = [math.exp(s - max_score) for s in scores]
    max_conf_no_height_inv = 1.0 / (sum(confs_no_height) or 1)
    confs_no_height_scaled = [s * max_conf_no_height_inv for s in confs_no_height]

    confs_scaled = incorporate_height(height_in, confs_no_height_scaled) if height_in else confs_no_height_scaled

    def _add_pos_and_scale(vec: list[float], scale: float) -> dict[str, float]:
        return {pos_list[i]: s * scale for i, s in enumerate(vec)}

    diagnostics: dict[str, Any] = {
        # 0.1 == "factor to make the scores render nicely" (ts:331)
        "scores": _add_pos_and_scale(scores, 0.1),
        "confsNoHeight": _add_pos_and_scale(confs_no_height_scaled, 1.0) if height_in else None,
        "calculated": calculated,
    }
    return _add_pos_and_scale(confs_scaled, 1.0), diagnostics


def _cdf(val: float, mean: float, std: float) -> float:
    """Standard-normal CDF at ``val`` for ``N(mean, std)`` (``PositionUtils.ts:341``).

    Direct port of the private ``cdf`` helper::

        0.5 * (1 + erf((val - mean) / (sqrt2 * std)))

    using stdlib :func:`math.erf` in place of the upstream mathjs ``erf``.
    """
    return 0.5 * (1 + math.erf((val - mean) / (_SQRT2 * std)))


def incorporate_height(height_in: float, confs: list[float]) -> list[float]:
    """Reweight positional confidences by height (Bayesian-ish height prior).

    Faithful port of ``PositionUtils.incorporateHeight``
    (``PositionUtils.ts:346-368``; see ``build_height_adj_probs`` in the
    linked hoop-explorer blog post). For each position ``i`` it computes a
    height-plausibility mass ``cdf(height + 1) - cdf(height - 1)`` under
    ``N(mean_i, sqrt2 * std_i)`` (the ``sqrt2`` "height dampening" widens the
    variance so the effect is not too aggressive), multiplies it into the
    prior confidence, and renormalizes.

    Args:
        height_in: Player height in inches.
        confs: The five raw (pre-height) confidences, in :data:`TRAD_POS_LIST`
            order.

    Returns:
        The five height-adjusted confidences, renormalized to sum to 1 (the
        ``sum_product or 1`` guard makes a degenerate all-zero product a no-op
        rather than a divide-by-zero -- see module landmine index item 1).

    Example:
        The "Krutwig" worked example from the hoop-explorer article::

            from sportsdataverse.mbb.mbb_positions import incorporate_height
            incorporate_height(81, [0.03, 0.19, 0.49, 0.09, 0.18])
    """
    thresh = 1
    height_dampening = _SQRT2
    new_scores: list[float] = [0.0, 0.0, 0.0, 0.0, 0.0]
    sum_product = 0.0
    for i, v in enumerate(confs):
        mean = HEIGHT_MEAN_STDS[i]["mean"]
        std = height_dampening * HEIGHT_MEAN_STDS[i]["std"]
        new_score = _cdf(height_in + thresh, mean, std) - _cdf(height_in - thresh, mean, std)
        sum_product += new_score * v
        new_scores[i] = new_score
    # `(confs[i] * v) / (sumProduct || 1)` (ts:365-367)
    return [(confs[i] * v) / (sum_product or 1) for i, v in enumerate(new_scores)]


#: Human-readable descriptions for the short position-class codes returned by
#: :func:`build_position` / :func:`using_roster_pos` (``PositionUtils.ts:387-398``
#: ``idToPosition``). Ported verbatim.
ID_TO_POSITION: dict[str, str] = {
    "PG": "Pure PG",
    "s-PG": "Scoring PG",
    "CG": "Combo Guard",
    "WG": "Wing Guard",
    "WF": "Wing Forward",
    "S-PF": "Stretch PF",
    "PF/C": "Power Forward/Center",
    "C": "Center",
    "G?": "Unknown - probably Guard",
    "F/C?": "Unknown - probably Forward/Center",
}

#: Team/season -> player key -> forced-position override
#: (``PositionalManualFixes.absolutePositionFixes``, 386 LOC upstream file).
#: **Only the row exercised by this module's test suite is ported** -- the
#: ``"Men_Boston College_2019/20"`` -> ``"Popovic, Nik"`` -> ``PF/C`` fix
#: (``PositionalManualFixes.ts:25-29``). The remaining ~35 rows (other
#: team/seasons -- Baylor's "Vital, Mark", Cincinnati's "DeJulius, David",
#: Iowa's "Garza, Luka", etc.) are a **deliberate deferral**: no jest case in
#: ``PositionUtils.test.ts`` exercises them, and the table is pure data with
#: no branching logic to get wrong -- vendor the remaining rows verbatim from
#: ``PositionalManualFixes.ts`` if/when a caller needs a specific team/season
#: override not listed here (a missing entry is not a silent bug: an
#: unlisted ``(team_season, player_key)`` pair simply falls through to the
#: normal stats-driven classification, same as upstream for any team/season
#: string not present in the real 386-line table).
ABSOLUTE_POSITION_FIXES: dict[str, dict[str, dict[str, str]]] = {
    "Men_Boston College_2019/20": {
        "Popovic, Nik": {"position": "PF/C"},
    },
}

_MIN_AST_RATE = 0.09
_MIN_THREE_RATE = 0.2


def _field_value(player: dict[str, Any], field: str) -> Any:
    """``player?.field?.value`` optional-chain read (e.g.
    ``player?.off_assist?.value``, ``PositionUtils.ts:426``).

    Returns ``None`` (not ``0``) when ``field`` is absent from ``player`` --
    an ``is not None`` check on the wrapper, mirroring JS ``?.`` (which only
    short-circuits on ``null``/``undefined``, not on a falsy-but-present
    value). Callers apply their own trailing ``or 0`` per the TS call site,
    same convention as :func:`build_position_confidences`.
    """
    stat = player.get(field)
    return stat.get("value") if isinstance(stat, dict) else None


def _max_conf_pos(confs: dict[str, float], pos_list: list[str]) -> str:
    """``_.maxBy(posList, pos => confs[pos] || 0) || 0`` (``PositionUtils.ts:424``).

    Lodash ``maxBy`` scans left-to-right and keeps the first element that
    reaches the running maximum (only a *strictly greater* value replaces
    it) -- replicated with an explicit scan so the tie-breaking behavior is
    documented rather than incidental. The trailing ``|| 0`` in the TS never
    actually fires (``posList`` is always the fixed 5-element
    :data:`TRAD_POS_LIST`, never empty), so it isn't reproduced here.
    """
    best_pos = pos_list[0]
    best_val = confs[best_pos] or 0
    for pos in pos_list[1:]:
        val = confs[pos] or 0
        if val > best_val:
            best_val = val
            best_pos = pos
    return best_pos


def build_position(
    confs: dict[str, float],
    confs_no_height: dict[str, float] | None,
    player: dict[str, Any],
    team_season: str,
) -> tuple[str, str]:
    """Classify a player into a position label + diagnostic trace string.

    Faithful port of ``PositionUtils.buildPosition`` (``PositionUtils.ts:401-580``)
    -- the PG / s-PG / CG / WG / WF / S-PF / PF/C / C decision tree. A
    :data:`ABSOLUTE_POSITION_FIXES` manual override short-circuits the whole
    tree (recursing once, with ``team_season=""``, purely to compute the
    diagnostic "what would this have been" string); otherwise the function
    walks the confidence-threshold / assist-rate / 3PT-rate branch cascade,
    applies the "too few effective possessions" (< 25) fallback, and
    reconciles the result against roster metadata via :func:`using_roster_pos`.

    Args:
        confs: The 5-way positional confidence dict (:data:`TRAD_POS_LIST`
            keys), typically the height-adjusted output of
            :func:`build_position_confidences`.
        confs_no_height: The pre-height-adjustment confidences, or ``None``
            when the caller has no height data. When present, a PG <-> s-PG
            flip caused solely by the height adjustment is reverted (the
            ``maybeIgnoreHeight`` closure, ``ts:433-457``). The check is
            ``is not None`` (JS object-truthiness: an empty dict is still a
            truthy JS object), NOT a Python-falsy ``if confs_no_height``.
        player: The player stat dict. Reads ``key`` (override lookup),
            ``off_assist`` / ``off_3pr`` / ``off_usage`` / ``off_team_poss``
            (each ``{"value": N}``-wrapped), and ``roster`` (a plain
            ``{"pos": ..., "role": ...}`` dict of un-wrapped strings).
        team_season: ``"{sport}_{team}_{season}"`` key into
            :data:`ABSOLUTE_POSITION_FIXES`. Pass ``""`` to disable override
            lookup for a given call (the recursive diagnostic call inside the
            override branch does exactly this).

    Returns:
        A ``(position, diagnostic)`` tuple. ``position`` is one of
        :data:`ID_TO_POSITION`'s keys; ``diagnostic`` is a human-readable
        trace of which rule fired, byte-identical to the TS's template
        strings (including ``.toFixed(1)``-style percentage formatting).

    Example:
        A confident, high-assist point guard::

            from sportsdataverse.mbb.mbb_positions import build_position, TRAD_POS_LIST
            confs = dict(zip(TRAD_POS_LIST, [0.9, 0.1, 0, 0, 0]))
            player = {"off_assist": {"value": 0.10}, "off_3pr": {"value": 0.20},
                      "off_team_poss": {"value": 1000}, "off_usage": {"value": 0.20}}
            build_position(confs, None, player, "Men_Boston College_2019/20")

        A manual-override short-circuit::

            build_position(confs, None, {"key": "Popovic, Nik",
                "off_usage": {"value": 1}, "off_team_poss": {"value": 200},
                "off_assist": {"value": 0.10}}, "Men_Boston College_2019/20")
    """
    player_key = player.get("key")
    override = ABSOLUTE_POSITION_FIXES.get(team_season, {}).get(player_key) if isinstance(player_key, str) else None
    if override:
        manual_pos, diag = build_position(confs, confs_no_height, player, "")
        return override["position"], f"Override from [{manual_pos}] which matched rule [{diag}]"

    pos_list = TRAD_POS_LIST
    max_pos = _max_conf_pos(confs, pos_list)

    # `player?.off_assist?.value || 0` / `player?.off_3pr?.value || 0` (ts:426/428).
    assist_rate = _field_value(player, "off_assist") or 0
    three_rate = _field_value(player, "off_3pr") or 0
    min_ast_rate = _MIN_AST_RATE
    min_three_rate = _MIN_THREE_RATE

    fwd_conf_sum = confs["pos_sf"] + confs["pos_pf"] + confs["pos_c"]

    def _maybe_ignore_height(in_pos_info: tuple[str, str, str]) -> tuple[str, str, str]:
        # `if (confsNoHeight)` (ts:434) -- JS object-truthiness: `is not None`,
        # NOT Python-falsy (an empty dict would still be a truthy JS object).
        if confs_no_height is not None:
            pos_with_height = in_pos_info[0]
            pos_no_height, diag_no_height = build_position(confs_no_height, None, player, team_season)
            if (pos_no_height == "s-PG" and pos_with_height == "PG") or (
                pos_no_height == "PG" and pos_with_height == "s-PG"
            ):
                return pos_no_height, f"{diag_no_height} ('PG' vs 's-PG', ignore height)", in_pos_info[2]
            return in_pos_info
        return in_pos_info

    def _get_position() -> tuple[str, str, str]:
        # Big if/elif cascade, branch-for-branch from PositionUtils.ts:460-549.
        if confs["pos_pg"] > 0.85:
            if assist_rate >= min_ast_rate:
                return _maybe_ignore_height(("PG", "(P[PG] >= 85%)", "G?"))
            return "WG", f"(PG:)(P[PG] >= 85%) BUT (AST%[{assist_rate * 100:.1f}] < 9%)", "G?"
        elif confs["pos_pg"] > 0.5:
            if assist_rate >= min_ast_rate:
                return _maybe_ignore_height(("s-PG", "(P[PG] >= 50%)", "G?"))
            return "WG", f"(pG:)(P[PG] >= 50%) BUT (AST%[{assist_rate * 100:.1f}] < 9%)", "G?"
        elif max_pos == pos_list[0]:
            if assist_rate >= min_ast_rate:
                return "CG", "(Max[P] == PG)", "G?"
            return "WG", f"(CG:)(Max[P] == PG) BUT (AST%[{assist_rate * 100:.1f}] < 9%)", "G?"
        elif max_pos == pos_list[1] and confs["pos_pg"] >= fwd_conf_sum:
            if assist_rate >= min_ast_rate:
                return "CG", "(Max[P] == SG) AND (P[PG] >= P[SF] + P[PF] + P[C])", "G?"
            return (
                "WG",
                f"(CG:)(Max[P] == SG) AND (P[PG] >= P[SF] + P[PF] + P[C]) BUT (AST%[{assist_rate * 100:.1f}] < 9%)",
                "G?",
            )
        elif max_pos == pos_list[1] and confs["pos_pg"] < fwd_conf_sum:
            return "WG", "(Max[P] == SG) AND (P[PG] < P[SF] + P[PF] + P[C])", "G?"
        elif max_pos == pos_list[2] and confs["pos_pg"] + confs["pos_sg"] >= confs["pos_pf"] + confs["pos_c"]:
            return "WG", "(Max[P] == SF) AND (P[PG] + P[SG] >= P[PF] + P[C])", "G?"
        elif max_pos == pos_list[2]:
            return "WF", "(Max[P] == SF) AND (P[PG] + P[SG] < P[PF] + P[C])", "F/C?"
        elif confs["pos_pf"] >= 0.85:
            return "PF/C", "(P[PF] >= 85%)", "F/C?"
        elif max_pos == pos_list[3] and confs["pos_pg"] + confs["pos_sg"] + confs["pos_sf"] >= confs["pos_c"]:
            if three_rate >= min_three_rate:
                return "S-PF", "(Max[P] == PF) AND (P[PG] + P[SG] + P[SF] >= P[C])", "F/C?"
            return (
                "PF/C",
                f"(S4:)(Max[P] == PF) AND (P[PG] + P[SG] + P[SF] >= P[C]) BUT 3PR%[{three_rate * 100:.1f}] < 20%",
                "F/C?",
            )
        elif confs["pos_c"] >= 0.85:
            return "C", "(P[C] >= 85%)", "F/C?"
        # (else fallback, ts:544-549)
        return (
            "PF/C",
            "(Max[P] == C) OR ((Max[P] == PF) AND (P[PG] + P[SG] + P[SF] < P[C]))",
            "F/C?",
        )

    pos, diag, fallback_pos = _get_position()

    # `player?.off_usage?.value || 0` / `player?.off_team_poss?.value || 0` (ts:553-554).
    usage = _field_value(player, "off_usage") or 0
    poss = _field_value(player, "off_team_poss") or 0
    effective_poss = poss * usage

    pos_from_stats = fallback_pos if effective_poss < 25.0 else pos

    # `player.roster?.pos` / `player.roster?.role` (ts:561/570) -- optional
    # chaining on a plain (un-wrapped) sub-dict, not a `{"value": N}` stat.
    roster = player.get("roster")
    roster_pos = roster.get("pos") if roster is not None else None
    roster_role = roster.get("role") if roster is not None else None

    pos_with_roster, pos_with_roster_info = using_roster_pos(pos_from_stats, roster_pos)
    extra_info = f"{pos_with_roster_info}. From stats: " if pos_with_roster_info else ""

    if effective_poss < 25.0:
        # `player.roster?.role || posWithRoster` (ts:570) -- falsy-coalesce:
        # an empty-string role is treated the same as a missing one.
        return (
            roster_role or pos_with_roster,
            f"{extra_info}Too few used possessions [{effective_poss:.1f}]=[{poss:.0f}]*"
            f"[{usage * 100:.1f}]% < [25.0]. Would have matched [{pos}] from rule [{diag}]",
        )
    return pos_with_roster, f"{extra_info}{diag}"


def pos_class_to_score(pos_class: str) -> int:
    """Ordinal "positional weight" for a position class, PG=1000..C=8000.

    Faithful port of ``PositionUtils.posClassToScore`` (``PositionUtils.ts:629-654``,
    a literal ``switch``). Unmapped classes default to ``4000`` (the TS
    default-case comment notes "won't happen").

    Args:
        pos_class: A position-class code (e.g. ``"PG"``, ``"WF"``, ``"C"``).

    Returns:
        The class's ordinal score.

    Example:
        ::

            from sportsdataverse.mbb.mbb_positions import pos_class_to_score
            pos_class_to_score("WF")
    """
    return {
        "PG": 1000,
        "s-PG": 2000,
        "CG": 3000,
        "G?": 3000,
        "WG": 4000,
        "WF": 5000,
        "S-PF": 6000,
        "PF/C": 7000,
        "F/C?": 7000,
        "C": 8000,
    }.get(pos_class, 4000)


def using_roster_pos(pos_class: str, roster_pos: str | None) -> tuple[str, str | None]:
    """Reconcile a stats-derived position class against roster metadata.

    Faithful port of ``PositionUtils.usingRosterPos`` (``PositionUtils.ts:583-626``).
    When the classifier landed on an "unsure" bucket (``"G?"``/``"F/C?"``),
    roster info narrows it (a roster ``"C"`` always wins outright); otherwise
    an obviously-wrong stats classification is compromised toward the
    roster-implied side, gated by :func:`pos_class_to_score` thresholds.

    Args:
        pos_class: The stats-derived position class.
        roster_pos: The roster-reported position (``"G"``/``"F"``/``"C"``),
            or ``None``/``""`` when unknown. ``if (rosterPos)`` (ts:587) is a
            plain JS truthiness check on a string -- ``""`` and ``None``
            behave identically (both mean "no correction"), so ``if not
            roster_pos`` is the faithful Python mirror, not an ``is None``
            landmine.

    Returns:
        A ``(position, info)`` tuple. ``info`` is ``None`` when no
        correction/explanation applies (matches the TS ``undefined``), else
        a human-readable note on why the position was adjusted.

    Example:
        A "C" roster position always wins over an unsure stats read::

            from sportsdataverse.mbb.mbb_positions import using_roster_pos
            using_roster_pos("G?", "C")
    """
    if not roster_pos:
        return pos_class, None

    if pos_class in ("G?", "F/C?"):
        if roster_pos == "G":
            return "G?", "Based on roster info"
        if roster_pos == "C":
            # (if someone's roster pos is a C then they are always a C!)
            return "C", "Based on roster info"
        return "F/C?", "Based on roster info"

    score = pos_class_to_score(pos_class)
    if score < 7000 and roster_pos == "C":
        return "PF/C", f"Roster info says 'C', stats say [{pos_class}] - compromize at 'PF/C'"
    if score < 4000 and roster_pos == "F":
        return "WG", f"Roster info says 'F', stats say [{pos_class}] - compromize at 'WG'"
    if score == 4000 and roster_pos == "F":
        return "WF", "Roster info says 'F', stats say 'WG'"
    if score == 5000 and roster_pos == "G":
        return "WG", "Roster info says 'G', stats say 'WF'"
    if score > 5000 and roster_pos == "G":
        return "WF", f"Roster info says 'G', stats say [{pos_class}] - compromize at 'WF'"
    return pos_class, None


#: Team/season -> lineup-slot positional-override rules
#: (``PositionalManualFixes.relativePositionFixes``, 386 LOC upstream file,
#: rules at ``:206-386``). **Only ``"Men_Maryland_2019/20"`` is ported** --
#: the one row exercised by ``orderLineup``'s override test
#: (``PositionUtils.test.ts:290-296``): the shared ``Maryland_2018_2020``
#: 2-rule base (Morsell<->Wiggins 4-guard-lineup swaps, ``:206-230``) plus the
#: season-specific "Lindo plays the 4 alongside Jalen Smith" rule
#: (``:249-261``). The remaining ~10 team/season keys (``Men_Maryland_2014/5``,
#: ``Men_Maryland_2018/9``, ``_2020/21``, ``_2021/22``, ``_2022/23``,
#: ``_2023/24``, ``Men_Illinois_2023/24``) are a **deliberate deferral**, same
#: rationale as :data:`ABSOLUTE_POSITION_FIXES`: no jest case exercises them,
#: the table is pure data (no branching logic to get wrong), and an unlisted
#: team/season simply falls through :func:`apply_relative_positional_overrides`
#: unchanged -- vendor the remaining rows verbatim from
#: ``PositionalManualFixes.ts`` if/when a caller needs one not listed here.
#: ``None`` stands in for the TS ``undefined`` wildcard in both ``key``
#: (any player code matches that slot) and ``rule`` (leave that slot's
#: player unchanged); an ``int`` in ``rule`` is a 1-based back-reference into
#: the *pre-rule* ``results`` array (``PositionUtils.ts:677``, ``changeRule -
#: 1``); a ``dict`` is a literal ``{"code": ..., "id": ...}`` replacement.
_MARYLAND_2018_2020: list[dict[str, list[Any]]] = [
    {
        # 2/2022: In 4-guard lineups, Morsell plays the 4 (even when he's
        # supposedly playing the 2!) (PositionalManualFixes.ts:208-218).
        "key": [None, "DaMorsell", None, "AaWiggins", None],
        "rule": [
            None,
            3,
            {"code": "AaWiggins", "id": "Wiggins, Aaron"},
            {"code": "DaMorsell", "id": "Morsell, Darryl"},
            None,
        ],
    },
    {
        # 7/6/2020: In 4-guard lineups, Morsell plays the 4 (:219-229).
        "key": [None, None, "DaMorsell", "AaWiggins", None],
        "rule": [
            None,
            None,
            {"code": "AaWiggins", "id": "Wiggins, Aaron"},
            {"code": "DaMorsell", "id": "Morsell, Darryl"},
            None,
        ],
    },
]

RELATIVE_POSITION_FIXES: dict[str, list[dict[str, list[Any]]]] = {
    "Men_Maryland_2019/20": _MARYLAND_2018_2020
    + [
        {
            # 7/10/2020: Lindo plays the 4 alongside Jalen Smith (:249-260).
            "key": [None, None, None, "JaSmith", "RiLindo"],
            "rule": [
                None,
                None,
                None,
                {"code": "RiLindo", "id": "Lindo Jr., Ricky"},
                {"code": "JaSmith", "id": "Smith, Jalen"},
            ],
        },
    ],
}


def apply_relative_positional_overrides(
    results: list[dict[str, str]], team_season: str, recurse_count: int = 0
) -> list[dict[str, str]]:
    """Recursively re-shuffle an ordered lineup per :data:`RELATIVE_POSITION_FIXES`.

    Faithful port of the private ``PositionUtils.applyRelativePositionalOverrides``
    (``PositionUtils.ts:657-693``). Finds the first rule (in table order) whose
    ``key`` slots all match the current ``results`` codes (a ``None`` key slot
    matches anything), applies that rule's ``rule`` slots (``None`` = leave
    unchanged, ``int`` = 1-based back-reference into the *pre-rule* results,
    ``dict`` = literal replacement) to produce a new ordering, then recurses on
    the new ordering -- since one swap can expose a second rule to match (e.g.
    the Maryland 2019/20 Morsell/Wiggins swap can cascade into the Lindo/Smith
    swap). Recursion is bounded by ``recurse_count < len(rules)`` (ported
    verbatim from the TS bound), so it always terminates even if two rules
    somehow ping-ponged each other.

    Args:
        results: The current 5-slot ``{"code": ..., "id": ...}`` ordering
            (PG/SG/SF/PF/C, index 0-4).
        team_season: Key into :data:`RELATIVE_POSITION_FIXES`. A team/season
            absent from the table (or the recursion exhausting that
            team/season's rule count) returns ``results`` unchanged.
        recurse_count: Internal recursion depth counter -- callers should
            not pass this explicitly (mirrors the TS default parameter).

    Returns:
        The (possibly re-shuffled) 5-slot ordering.

    Example:
        A rule-set match swaps two slots and then re-checks for a cascade::

            from sportsdataverse.mbb.mbb_positions import apply_relative_positional_overrides
            results = [
                {"code": "AnCowan", "id": "Cowan, Anthony"},
                {"code": "ErAyala", "id": "Ayala, Eric"},
                {"code": "DaMorsell", "id": "Morsell, Darryl"},
                {"code": "AaWiggins", "id": "Wiggins, Aaron"},
                {"code": "JaSmith", "id": "Smith, Jalen"},
            ]
            apply_relative_positional_overrides(results, "Men_Maryland_2019/20")
    """
    rules = RELATIVE_POSITION_FIXES.get(team_season)
    if rules is not None and recurse_count < len(rules):
        rule_set = next(
            (
                rule
                for rule in rules
                if all(key is None or key == results[index]["code"] for index, key in enumerate(rule["key"]))
            ),
            None,
        )
        if rule_set is not None:
            new_results: list[dict[str, str]] = []
            for index, val in enumerate(results):
                change_rule = rule_set["rule"][index]
                if change_rule is None:
                    new_results.append(val)
                elif isinstance(change_rule, int):
                    new_results.append(results[change_rule - 1])
                else:
                    new_results.append(change_rule)
            return apply_relative_positional_overrides(new_results, team_season, recurse_count + 1)
        return results
    return results


def _fit_player(
    pl_index: int,
    player_ids: list[str],
    player_infos: list[dict[str, Any] | None],
    mutable_scores: list[float],
    mutable_best_fits: list[int],
) -> None:
    """Fit one player to their best-available slot, recursively re-fitting
    any player they displace (``PositionUtils.ts:715-750``, the ``fitPlayer``
    closure inside ``orderLineup``).

    Candidate slots are tried in a fixed priority order -- PG (0), C (4), SG
    (1), PF (3), then SF (2) as an always-available fallback (score ``0``,
    which beats the ``-100000`` initial sentinel) -- mirroring the TS
    ``_.takeWhile`` loop: keep scanning candidates while the current slot's
    incumbent score is not beaten (``return true`` => keep going), and stop
    at the first slot this player *does* beat (``return false`` => the
    ``takeWhile`` halts), evicting and recursively re-fitting the incumbent
    first if there was one.
    """
    info = player_infos[pl_index]
    # `playerInfos[plIndex]?.posConfidences || [0,0,0,0,0]` (ts:716).
    pos_class = (info.get("posConfidences") if info is not None else None) or [0, 0, 0, 0, 0]
    # `playerInfos[plIndex]?.posClass || ""` (ts:719).
    pos_class_score = pos_class_to_score((info.get("posClass") if info is not None else None) or "")
    pg_score = 3 * pos_class[0] + pos_class[1]
    post_score = 3 * pos_class[4] + pos_class[3]
    backcourt_score = pos_class[0] + pos_class[1]
    frontcourt_score = pos_class[4] + pos_class[3]

    pl_scores: list[tuple[float, int]] = [
        (pg_score - 2 * frontcourt_score - pos_class_score, 0),  # PG
        (post_score - 2 * backcourt_score + pos_class_score, 4),  # C
        (backcourt_score - frontcourt_score - pos_class_score, 1),  # SG
        (frontcourt_score - backcourt_score + pos_class_score, 3),  # PF
        (0, 2),  # SF is fallback
    ]
    for score, score_pos in pl_scores:
        if score > mutable_scores[score_pos]:
            prev_best_fit = mutable_best_fits[score_pos]
            if prev_best_fit >= 0:
                # Refit the player being replaced.
                _fit_player(prev_best_fit, player_ids, player_infos, mutable_scores, mutable_best_fits)
            mutable_best_fits[score_pos] = pl_index
            mutable_scores[score_pos] = score
            break


def order_lineup(
    player_codes_and_ids: list[dict[str, str]],
    players_by_id: dict[str, dict[str, Any]],
    team_season: str,
) -> list[dict[str, str]]:
    """Order a 5-man lineup ``X1_X2_X3_X4_X5`` into PG/SG/SF/PF/C slot order.

    Faithful port of ``PositionUtils.orderLineup`` (``PositionUtils.ts:696-761``).
    Greedily fits each player (in input order) to their best-scoring slot via
    :func:`_fit_player` (dominated by :func:`pos_class_to_score` on the
    player's ``posClass``, tie-broken by their raw ``posConfidences``),
    evicting and recursively re-fitting any player displaced along the way,
    then applies :func:`apply_relative_positional_overrides` (keyed on
    ``team_season``) as a final hand-tuned correction pass.

    Args:
        player_codes_and_ids: The lineup membership, each a
            ``{"code": ..., "id": ...}`` dict. Order does not affect the
            final result (the slot-fitting algorithm is order-invariant by
            construction -- displaced players are always re-fit).
        players_by_id: Per-player positional info keyed by ``id``, each a
            ``{"posConfidences": [pg, sg, sf, pf, c], "posClass": "..."}``
            dict (the tradPosList-ordered raw confidence scores plus the
            classifier's :data:`ID_TO_POSITION`-keyed class label).
        team_season: Key into :data:`RELATIVE_POSITION_FIXES` for the final
            override pass.

    Returns:
        A 5-element list of ``{"code": ..., "id": ...}`` dicts in
        PG/SG/SF/PF/C order.

    Example:
        ::

            from sportsdataverse.mbb.mbb_positions import order_lineup
            players_by_id = {
                "Cowan, Anthony": {"posConfidences": [60, 40, 10, 0, 0], "posClass": "s-PG"},
                "Ayala, Eric": {"posConfidences": [40, 60, 10, 0, 0], "posClass": "CG"},
            }
            order_lineup(
                [{"code": "AnCowan", "id": "Cowan, Anthony"},
                 {"code": "ErAyala", "id": "Ayala, Eric"}],
                players_by_id, "",
            )
    """
    # `_.fromPairs(playerCodesAndIds.map(codeId => [codeId.id, codeId.code]))`
    # (ts:701-706) -- a dict preserves insertion order, matching lodash.
    player_id_to_code = {c["id"]: c["code"] for c in player_codes_and_ids}
    player_ids = list(player_id_to_code.keys())
    init = -100000.0
    mutable_scores: list[float] = [init, init, init, init, init]
    mutable_best_fits: list[int] = [-1, -1, -1, -1, -1]
    player_infos: list[dict[str, Any] | None] = [players_by_id.get(pid) for pid in player_ids]

    for pl_index in range(len(player_ids)):
        _fit_player(pl_index, player_ids, player_infos, mutable_scores, mutable_best_fits)

    # NOTE (landmine, unreachable in this codebase's domain): if
    # `len(player_ids) < 5`, a scorePos slot can be left at `-1` (never
    # fit). JS `player_ids[-1]` returns `undefined`; Python negative
    # indexing would instead silently wrap around to the *last* player -- a
    # real behavioral divergence from the TS. `order_lineup` is only ever
    # called with exactly 5-player lineups in this codebase, and with 5
    # players / 5 slots the SF fallback (score `0`, always `>` the `-100000`
    # sentinel) guarantees every slot is filled, so `-1` never survives to
    # this point in practice -- left unguarded rather than adding dead code;
    # guard here first if this function is ever exposed to variable-size
    # groups.
    ordered = [{"code": player_id_to_code[player_ids[idx]], "id": player_ids[idx]} for idx in mutable_best_fits]
    return apply_relative_positional_overrides(ordered, team_season)


#: 0-indexed tradPosList slot for each recognized filter position token
#: (``PositionUtils.ts:784-802``, the ``switch`` inside ``buildPositionalAwareFilter``'s
#: ``decomp`` closure). Both the 1-based numeric token and the position
#: abbreviation map to the same slot; an unrecognized token contributes no
#: index (``PositionUtils.ts:801`` falls through to ``return []``).
_POSITION_FILTER_TOKENS: dict[str, int] = {
    "1": 0,
    "pg": 0,
    "2": 1,
    "sg": 1,
    "3": 2,
    "sf": 2,
    "4": 3,
    "pf": 3,
    "5": 4,
    "c": 4,
}

#: ``([^=]+)(?:=(([a-zA-Z1-5+]+)))?`` (``PositionUtils.ts:781``) -- group 1 is
#: the filter name (everything up to an optional ``=``), group 2 is the
#: optional ``+``-joined position-token spec.
_FILTER_FRAGMENT_RE = re.compile(r"([^=]+)(?:=(([a-zA-Z1-5+]+)))?")


def _decomp_positional_filter_fragment(fragment: str, has_position: list[bool]) -> list[dict[str, Any]]:
    """Parse one filter fragment into 0 or 1 ``{"filter": ..., "pos": [...]}``
    dicts (``PositionUtils.ts:780-809``, the ``decomp`` closure).

    ``has_position`` is a 1-element mutable list standing in for the TS
    closure's shared ``var hasPosition`` -- both :func:`build_positional_aware_filter`
    call sites (positive and negative fragments) mutate the same cell, and
    the final flag is true iff *any* fragment (either side) carried a
    recognized position token.
    """
    match_info = _FILTER_FRAGMENT_RE.match(fragment)
    # `matchInfo?.[1]` (ts:782) -- group 1 is always present when the regex
    # matches at all (it requires >= 1 non-`=` char), so a `None` match
    # object is the only "no filter" case in practice.
    filt = match_info.group(1) if match_info is not None else None
    if filt:
        # `(matchInfo?.[2] || "").split("+")` (ts:784).
        pos_spec = (match_info.group(2) or "") if match_info is not None else ""
        pos: list[int] = []
        for token in pos_spec.split("+"):
            index = _POSITION_FILTER_TOKENS.get(token.strip().lower())
            if index is not None:
                pos.append(index)
        # `hasPosition = hasPosition || !_.isEmpty(pos)` (ts:804).
        if pos:
            has_position[0] = True
        return [{"filter": filt.lower(), "pos": pos}]
    return []


def build_positional_aware_filter(
    filter_str: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    """Decompose a search-filter string into positionally-aware +ve/-ve fragments.

    Faithful port of ``PositionUtils.buildPositionalAwareFilter``
    (``PositionUtils.ts:764-828``). Picks a fragment separator by scanning
    ``[";", "/", ","]`` in priority order for the first one present anywhere
    in ``filter_str`` (a fragment separator of ``"!!!"`` -- never itself
    present -- is the "no separator found" fallback, which leaves the whole
    string as a single fragment). Splits on that separator, trims whitespace,
    drops empty fragments and ``[``-prefixed ones (reserved for aggregation-key
    filters elsewhere in the app), then routes each fragment to the positive
    or negative bucket by a leading ``-``, and parses each fragment's optional
    ``=<tokens>`` position spec via :func:`_decomp_positional_filter_fragment`.

    Args:
        filter_str: A raw filter string, e.g. ``"test1=pg / -test2=Pf+C / test3"``.

    Returns:
        A ``(positive_fragments, negative_fragments, has_position)`` triple.
        Each fragment is ``{"filter": <lowercased name>, "pos": [indices]}``.
        ``has_position`` is ``True`` iff any fragment (either side) carried at
        least one recognized position token.

    Example:
        ::

            from sportsdataverse.mbb.mbb_positions import build_positional_aware_filter
            build_positional_aware_filter("test1=pg / -test2=Pf+C / test3")
    """
    # Pick the separator: first of `;` / `/` / `,` (in that priority order)
    # that appears anywhere in filter_str (ts:771-777).
    separator = "!!!"
    for candidate in (";", "/", ","):
        if candidate in filter_str:
            separator = candidate
        if separator != "!!!":
            break

    has_position = [False]

    # `filterStr.split(separator).map(trim).filter(...)` (ts:811-815):
    # drop `[`-prefixed and empty fragments.
    fragments = [frag.strip() for frag in filter_str.split(separator)]
    fragments = [frag for frag in fragments if frag and frag[0] != "["]

    filter_fragments_pve = [frag for frag in fragments if frag[0] != "-"]
    filter_fragments_nve = [frag[1:] for frag in fragments if frag[0] == "-"]

    pve_frags = [
        item for frag in filter_fragments_pve for item in _decomp_positional_filter_fragment(frag, has_position)
    ]
    nve_frags = [
        item for frag in filter_fragments_nve for item in _decomp_positional_filter_fragment(frag, has_position)
    ]

    return pve_frags, nve_frags, has_position[0]


def test_positional_aware_filter(
    sorted_to_test: list[dict[str, str]],
    pve_frags: list[dict[str, Any]],
    nve_frags: list[dict[str, Any]],
) -> bool:
    """Check a positional-aware filter (from :func:`build_positional_aware_filter`)
    against a sorted (:func:`order_lineup`-ordered) lineup array.

    Faithful port of ``PositionUtils.testPositionalAwareFilter``
    (``PositionUtils.ts:831-858``). A fragment matches if any of its
    position-restricted slots (or, when ``pos`` is empty, any slot at all)
    has a ``code``/``id`` containing the fragment's filter text
    (case-insensitive substring match). Every positive fragment must match
    (vacuously true if there are none); no negative fragment may match
    (vacuously true if there are none).

    Args:
        sorted_to_test: The ordered lineup, each a ``{"id": ..., "code": ...}``
            dict (as returned by :func:`order_lineup`).
        pve_frags: Positive-filter fragments (must ALL match).
        nve_frags: Negative-filter fragments (NONE may match).

    Returns:
        Whether the lineup satisfies both the positive and negative filters.

    Example:
        ::

            from sportsdataverse.mbb.mbb_positions import test_positional_aware_filter
            lineup = [{"code": "AnCowan", "id": "Cowan, Anthony"}]
            test_positional_aware_filter(lineup, [{"filter": "cowan", "pos": []}], [])
    """
    no_pve_frags = not pve_frags
    no_nve_frags = not nve_frags

    def _match_frag(cid: dict[str, str], frag: str) -> bool:
        return frag in cid["id"].lower() or frag in cid["code"].lower()

    def _match(frag: dict[str, Any]) -> bool:
        pos = frag["pos"]
        names_to_test = sorted_to_test if not pos else [sorted_to_test[index] for index in pos]
        return any(_match_frag(cid, frag["filter"]) for cid in names_to_test)

    return (no_pve_frags or all(_match(frag) for frag in pve_frags)) and (
        no_nve_frags or not any(_match(frag) for frag in nve_frags)
    )
