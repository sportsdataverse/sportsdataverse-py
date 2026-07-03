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

**Deferred to later Phase-4 tasks (intentionally absent from this file):**
the decision-tree layer -- ``buildPosition`` / ``usingRosterPos`` /
``posClassToScore`` and the ``idToPosition`` / ``positionClasses`` lookup
tables (Task 4.3, ``PositionUtils.ts:387/401/583/629``) -- and the
lineup-ordering layer -- ``orderLineup`` /
``applyRelativePositionalOverrides`` / ``buildPositionalAwareFilter`` /
``testPositionalAwareFilter`` (Task 4.4, ``PositionUtils.ts:657/696/764/831``).
Those two tasks also pull in the ``PositionalManualFixes.ts`` data tables
(``absolutePositionFixes`` / ``relativePositionFixes``), which this task does
not read.

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
from typing import Any

__all__ = [
    "POSITION_FEATURE_INIT",
    "TRAD_POS_LIST",
    "POSITION_FEATURE_WEIGHTS",
    "POSITION_FEATURE_AVERAGES",
    "HEIGHT_MEAN_STDS",
    "AVERAGE_SCORES_BY_POS",
    "regress_shot_quality",
    "build_position_confidences",
    "incorporate_height",
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
