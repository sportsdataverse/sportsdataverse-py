"""Oracle-replay tests for ``sportsdataverse.mbb.mbb_positions`` (Task 4.2).

Every assertion here replays an inline literal from the upstream jest suite
``cbb-on-off-analyzer/src/utils/stats/__tests__/PositionUtils.test.ts`` (382
LOC, 9 ``test()`` blocks, **no snapshot file** -- all oracles are inline
``.toEqual`` / ``.toFixed(n)`` / ``.toBe`` literals). Fixture provenance and
the full assertion classification map live in
``tests/fixtures/hoop_explorer/README.md`` (PositionUtils section).

**In scope for Task 4.2 (the classifier core) -- consumed below:**

* ``PositionUtils - incorporateHeight`` (jest ``:14-24``)
* ``PositionUtils - averageScoresByPos`` (jest ``:25-27``)
* ``PositionUtils - buildPositionConfidences`` (jest ``:28-52``)
* ``PositionUtils - regressShotQuality`` (jest ``:173-205``)

**Out of scope for Task 4.2 (deferred to later Phase-4 tasks, NOT tested
here) -- accounted for so none are silently skipped:**

* ``PositionUtils - buildPosition`` (jest ``:53-172``) -> Task 4.3 (decision
  tree + ``PositionalManualFixes.absolutePositionFixes``).
* ``PositionUtils - usingRosterPos`` (jest ``:207-225``) -> Task 4.3.
* ``PositionUtils - orderLineup`` (jest ``:227-312``) -> Task 4.4 (lineup
  ordering + ``relativePositionFixes``).
* ``PositionUtils - buildPositionalAwareFilter`` (jest ``:313-334``) -> Task 4.4.
* ``PositionUtils - testPositionalAwareFilter`` (jest ``:335-380``) -> Task 4.4.

The jest file-scope helper ``tidyObj`` (``:12``,
``_.mapValues(vo, v => (v.value || v).toFixed(2))``) is replayed as
:func:`_tidy_obj` below.
"""

from __future__ import annotations

from typing import Any

from sportsdataverse.mbb.mbb_positions import (
    AVERAGE_SCORES_BY_POS,
    TRAD_POS_LIST,
    build_position_confidences,
    incorporate_height,
    regress_shot_quality,
)

from ._hoop_explorer_replay import load_fixture, load_rating_inputs


def _tidy_obj(vo: dict[str, Any]) -> dict[str, str]:
    """Replay of the jest ``tidyObj`` helper (``PositionUtils.test.ts:12``):
    2-decimal string projection, unwrapping a ``{"value": N}`` wrapper via
    ``v.value`` when present. Our confidence / score / calculated values are
    plain floats (never wrapped), so ``v.value`` is absent and ``v`` is
    formatted directly -- but the wrapper branch is kept for fidelity (e.g.
    :data:`AVERAGE_SCORES_BY_POS` values ARE ``{"value": N}`` wrapped).
    """
    out: dict[str, str] = {}
    for k, v in vo.items():
        if isinstance(v, dict) and (v.get("value") or 0) != 0:
            num = v["value"]
        elif isinstance(v, dict) and "value" in v:
            num = v["value"]
        else:
            num = v
        out[k] = f"{num:.2f}"
    return out


def _baseline_buckets() -> list[dict]:
    """The 2 player buckets (``Cowan, Anthony`` idx 0, ``Wiggins, Aaron`` idx 1)
    reused from ``rating_utils_inputs.json`` (README: shared cross-reference,
    not re-vendored under position_utils).
    """
    resp = load_rating_inputs()["samplePlayerStatsResponse"]
    return resp["responses"][0]["aggregations"]["tri_filter"]["buckets"]["baseline"]["player"]["buckets"]


# --- Test 1: PositionUtils - incorporateHeight (jest :14-24) ---------------


def test_incorporate_height_krutwig() -> None:
    """Krutwig worked example: height 81 reweights raw confidences (``.toFixed(4)``)."""
    result = incorporate_height(81, [0.03, 0.19, 0.49, 0.09, 0.18])
    assert [f"{n:.4f}" for n in result] == ["0.0055", "0.0776", "0.4753", "0.1289", "0.3127"]


# --- Test 2: PositionUtils - averageScoresByPos (jest :25-27) --------------


def test_average_scores_by_pos_checksum() -> None:
    """Derived ``AVERAGE_SCORES_BY_POS`` -- the transcription checksum on the
    weight/average constant tables."""
    assert list(_tidy_obj(AVERAGE_SCORES_BY_POS).values()) == ["0.15", "-0.03", "-0.11", "0.03", "0.42"]


# --- Test 3: PositionUtils - buildPositionConfidences (jest :28-52) --------


def test_build_position_confidences_player0() -> None:
    """buckets[0] (Cowan) -- full assertion trio + tradPosList key order."""
    real_confidences, real_diags = build_position_confidences(_baseline_buckets()[0], None)
    assert list(_tidy_obj(real_confidences).values()) == ["0.76", "0.24", "0.00", "0.00", "0.00"]
    assert list(_tidy_obj(real_diags["scores"]).values()) == ["0.19", "0.07", "-0.33", "-0.61", "-1.62"]
    assert _tidy_obj(real_diags["calculated"]) == {
        "calc_assist_per_fga": "0.41",
        "calc_ast_tov": "2.13",
        "calc_ft_relative_inv": "0.58",  # 47% eFG / (166/206)
        "calc_mid_relative": "0.59",
        "calc_rim_relative": "1.18",
        "calc_three_relative": "1.03",
    }
    # Key ORDER must match TRAD_POS_LIST exactly (jest :45-46).
    assert list(real_confidences.keys()) == TRAD_POS_LIST
    assert list(real_diags["scores"].keys()) == TRAD_POS_LIST


def test_build_position_confidences_player1() -> None:
    """buckets[1] (Wiggins) -- confidences only."""
    real_confidences2, _ = build_position_confidences(_baseline_buckets()[1], None)
    assert list(_tidy_obj(real_confidences2).values()) == ["0.02", "0.39", "0.42", "0.18", "0.00"]


# --- Test 5: PositionUtils - regressShotQuality (jest :173-205) ------------


def test_regress_shot_quality() -> None:
    """8 direct calls across passthrough / special-case / low- & high-volume
    regression, using the vendored ``player`` / ``player2`` fixtures."""
    inputs = load_fixture("position_utils_inputs.json")
    player = inputs["player"]
    player2 = inputs["player2"]

    # Case 1: not a regressed feature -> passthrough.
    assert regress_shot_quality(-15.5, 2, "misc_feature", player) == -15.5
    # Case 2: regressed feature, but volume high enough to skip.
    assert regress_shot_quality(-15.5, 2, "calc_mid_relative", player) == -15.5
    # Case 3: center-3s special carve-out (stat == 0).
    assert regress_shot_quality(0, 4, "calc_three_relative", player) == 0
    assert f"{regress_shot_quality(10, 4, 'calc_three_relative', player):.2f}" == "0.77"
    assert f"{regress_shot_quality(0, 3, 'calc_three_relative', player):.2f}" == "1.03"
    # Case 4a: low-volume regression.
    assert f"{regress_shot_quality(100, 3, 'calc_rim_relative', player):.2f}" == "53.92"
    # Case 4b: higher-volume player2.
    assert f"{regress_shot_quality(10, 4, 'calc_three_relative', player2):.2f}" == "0.50"
    assert f"{regress_shot_quality(-15.5, 2, 'calc_mid_relative', player2):.2f}" == "-12.26"
    # player2 rim share > 25% of total_off_fga -> no regression, passthrough.
    assert f"{regress_shot_quality(100, 3, 'calc_rim_relative', player2):.2f}" == "100.00"
