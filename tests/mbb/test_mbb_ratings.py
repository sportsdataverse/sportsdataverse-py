"""Oracle tests for ``mbb_ratings`` against vendored hoop-explorer fixtures.

**Task 2.2** replayed ``RatingUtils.test.ts``'s ``"RatingUtils - buildORtg"``
jest test (``src/utils/stats/__tests__/RatingUtils.test.ts:27-117``)
call-for-call: 3 progressive calls sharing a single mutated ``playerInfo``
(baseline, then a manual ``off_3p`` override, then a team-TO scenario),
asserting the ``oRtg``/``adjORtg`` oracle values (``expORtg``/``expORtgAdj``/
... vendored in ``rating_utils_inputs.json``) plus the full ``oRtgDiags``
snapshot (``rating_utils_snap.json``'s ``"RatingUtils - buildORtg 1"`` entry,
92 keys -- baseline call only, per upstream) and the ``sampleOrtgDiagnostics``
``toMatchObject`` oracle (93 keys, the extra key being an explicit
``Raw_Usage: undefined`` literal in the upstream sample file).

**Task 2.3** adds: ``build_d_rtg`` (replays ``"RatingUtils - buildDRtg"``,
``RatingUtils.test.ts:247-275`` -- baseline + ``oppo_def_3p``-override
calls, both the ``expDRtg``/``expDRtgAdj`` oracle values and the full
47-key ``dRtgDiags`` snapshot/``sampleDrtgDiagnostics`` ``toEqual`` oracle);
``build_net_points`` (replays ``"RatingUtils - buildNetPoints uses
adjPtsFactor and adjPossFactor"``, ``RatingUtils.test.ts:119-155``, 20-key
snapshot); ``adjust_off_rating_stats`` (replays ``"RatingUtils -
adjustOffRatingStats updates the right fields"``,
``RatingUtils.test.ts:157-182``, 7-key projection snapshot); and
``build_productivity`` (promoted from private -- ``_build_productivity`` in
Task 2.2 -- see ``mbb_ratings.py``'s module docstring for the promotion
rationale).

``build_o_rtg``/``build_d_rtg`` mirror ``buildORtg``/``buildDRtg``'s actual
TS signatures (snake_cased) -- see ``sportsdataverse/mbb/mbb_ratings.py``'s
module docstring for why this deliberately diverges from the Phase-2 plan
brief's proposed stub signature.

**Unreachable-via-this-task's-scope** (``rating_utils_snap.json`` entries NOT
consumed by any test in this module -- see ``mbb_ratings.py``'s module
docstring "Deferred beyond this task" section for the full rationale, this
is the test-side accounting the oracle-discipline rule requires):

- ``"RatingUtils - injectUncatOnBallDefenseStats 1"``
- ``"RatingUtils - buildOnBallDefenseAdjustmentsPhase1 1"``
- ``"RatingUtils - injectOnBallDefenseAdjustmentsPhase2 1"``

These 3 belong to the on-ball-defense adjustment family, which is out of
scope for Phase 2 (no owning task in ``PLAN-phase2.md``; depends on a
proprietary Synergy-style ``OnBallDefenseModel`` input this port has no
producer for). All other ``rating_utils_snap.json`` entries (``buildORtg``,
``buildDRtg``, ``buildNetPoints``, ``adjustOffRatingStats``) are consumed
below or in the Task 2.2 tests above.
"""

from __future__ import annotations

import copy

import pytest

from sportsdataverse.mbb.mbb_ratings import (
    REPLACEMENT_LEVEL,
    RETAIN_POSS_WITH_REBOUND_RATE,
    _build_off_overrides,
    adjust_off_rating_stats,
    build_d_rtg,
    build_net_points,
    build_o_rtg,
    build_productivity,
)
from tests.mbb._hoop_explorer_replay import approx_tree, load_rating_inputs, load_rating_snap


@pytest.fixture(scope="module")
def snap() -> dict:
    return load_rating_snap()


@pytest.fixture(scope="module")
def inputs() -> dict:
    return load_rating_inputs()


@pytest.fixture()
def player_info(inputs: dict) -> dict:
    """Fresh deep copy of ``samplePlayerStatsResponse``'s baseline player[0]
    doc -- the exact ``_.cloneDeep(...buckets[0])`` the jest test performs
    (``RatingUtils.test.ts:28-31``).
    """
    buckets = inputs["samplePlayerStatsResponse"]["responses"][0]["aggregations"]["tri_filter"]["buckets"]["baseline"][
        "player"
    ]["buckets"]
    return copy.deepcopy(buckets[0])


def test_constants_verbatim():
    """``Replacement_Level`` / ``retainPossWithReboundRate`` copied verbatim
    from ``RatingUtils.ts:321,324``.
    """
    assert REPLACEMENT_LEVEL == 0.92
    assert RETAIN_POSS_WITH_REBOUND_RATE == 1.07


def test_build_o_rtg_none_stat_set_returns_all_none():
    """``buildORtg`` returns an all-``undefined`` 5-tuple for a
    null/undefined ``statSet`` (``RatingUtils.ts:412-413``'s
    ``if (!statSet)``) -- ``None`` only, matching JS object truthiness.
    """
    assert build_o_rtg(None, {}, {}, 100.0, True, False) == (None, None, None, None, None)


def test_build_o_rtg_empty_stat_set_falls_through_to_landmine():
    """``{}`` is truthy in JS, so the TS does NOT short-circuit -- it falls
    through and computes a degenerate result whose ``Team_FTM / Team_FTA``
    is ``0/0 = NaN`` (verified against node; the NaN is contained to the
    ``teamProbFtHitOnePlus`` diagnostic since every downstream use is behind
    a ``Team_FTA > 0`` guard). This port matches the TS control flow
    (``is None`` short-circuit only) but, per the module's documented
    no-NaN-emulation policy (unguarded-division landmine 1,
    ``RatingUtils.ts:627``), Python raises ``ZeroDivisionError`` at that
    expression instead of degrading to NaN.
    """
    with pytest.raises(ZeroDivisionError):
        build_o_rtg({}, {}, {}, 100.0, True, False)


def test_build_o_rtg_baseline_call(player_info, inputs, snap):
    """Call 1 (``calc_diags=True, override_adjusted=False``): baseline ORtg,
    no override applied yet.
    """
    o_rtg, adj_o_rtg, raw_o_rtg, raw_adj_o_rtg, o_rtg_diags = build_o_rtg(
        player_info,
        {},
        {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
        100,
        True,
        False,
    )

    assert raw_o_rtg is None
    assert raw_adj_o_rtg is None

    exp_o_rtg = inputs["expORtg"]
    exp_o_rtg_adj = inputs["expORtgAdj"]
    assert o_rtg["value"] == pytest.approx(exp_o_rtg["value"], rel=1e-9)
    assert adj_o_rtg["value"] == pytest.approx(exp_o_rtg_adj["value"], rel=1e-9)

    # Full oRtgDiags snapshot (92 keys, jest toMatchSnapshot):
    expected_diags = snap["RatingUtils RatingUtils - buildORtg 1"]
    approx_tree(o_rtg_diags, expected_diags)

    # toMatchObject(sampleOrtgDiagnostics) -- 93 keys (adds Raw_Usage: None):
    approx_tree(o_rtg_diags, inputs["sampleOrtgDiagnostics"])

    assert o_rtg_diags["adjPtsFactor"] == 1
    assert o_rtg_diags["adjPossFactor"] == 1


def test_build_o_rtg_override_call(player_info, inputs):
    """Call 2 (``calc_diags=False, override_adjusted=True``), on top of a
    mutated ``off_3p`` override (``RatingUtils.test.ts:50-63``): manual
    3P%-override projection is applied, and the raw (un-overridden) values
    surface in the 3rd/4th tuple slots.
    """
    # Call 1 first (mutates nothing, but establishes the shared playerInfo
    # baseline exactly like the jest test's single shared const):
    build_o_rtg(player_info, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}}, 100, True, False)

    player_info["off_3p"] = {
        "value": player_info["off_3p"]["value"] - 0.1,
        "old_value": player_info["off_3p"]["value"],
    }
    o_rtg2, adj_o_rtg2, raw_o_rtg2, raw_adj_o_rtg2, _ = build_o_rtg(
        player_info,
        {},
        {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
        100,
        False,
        True,
    )

    exp_o_rtg2 = inputs["expORtg2"]
    exp_o_rtg_ad2 = inputs["expORtgAd2"]
    exp_o_rtg = inputs["expORtg"]
    exp_o_rtg_adj = inputs["expORtgAdj"]

    assert o_rtg2["value"] == pytest.approx(exp_o_rtg2["value"], rel=1e-9)
    assert adj_o_rtg2["value"] == pytest.approx(exp_o_rtg_ad2["value"], rel=1e-9)
    # rawORtg2/rawAdjORtg2 == the un-overridden baseline values:
    assert raw_o_rtg2["value"] == pytest.approx(exp_o_rtg["value"], rel=1e-9)
    assert raw_adj_o_rtg2["value"] == pytest.approx(exp_o_rtg_adj["value"], rel=1e-9)

    # Call 2b: override exists on the stat set but override_adjusted=False
    # -- disabled in output, values should equal the untouched baseline
    # (RatingUtils.test.ts:66-76):
    o_rtg2b, adj_o_rtg2b, _, _, _ = build_o_rtg(
        player_info,
        {},
        {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
        100,
        False,
        False,
    )
    assert o_rtg2b["value"] == pytest.approx(exp_o_rtg["value"], rel=1e-9)
    assert adj_o_rtg2b["value"] == pytest.approx(exp_o_rtg_adj["value"], rel=1e-9)


def test_build_o_rtg_team_to_override_call(player_info, inputs):
    """Call 3 (``calc_diags=False, override_adjusted=True``) with a nonzero
    ``extra_team_stat_info`` "unblamed TOV" pool and ``off_team_poss_pct``
    mutated to ``0.5`` (``RatingUtils.test.ts:78-93``).
    """
    build_o_rtg(player_info, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}}, 100, True, False)
    player_info["off_3p"] = {
        "value": player_info["off_3p"]["value"] - 0.1,
        "old_value": player_info["off_3p"]["value"],
    }
    build_o_rtg(player_info, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}}, 100, False, True)

    player_info["off_team_poss_pct"] = {"value": 0.5}
    o_rtg3, adj_o_rtg3, _, _, _ = build_o_rtg(
        player_info,
        {},
        {"total_off_to": {"value": 100}, "sum_total_off_to": {"value": 80}},
        100,
        False,
        True,
    )

    exp_o_rtg3 = inputs["expORtg3"]
    exp_o_rtg_ad3 = inputs["expORtgAd3"]
    assert o_rtg3["value"] == pytest.approx(exp_o_rtg3["value"], rel=1e-9)
    assert adj_o_rtg3["value"] == pytest.approx(exp_o_rtg_ad3["value"], rel=1e-9)


def test_build_off_overrides(inputs):
    """Replay of the ``"RatingUtils - buildOffOverrides"`` jest test
    (``RatingUtils.test.ts:184-245``) -- no ``rating_utils_snap.json`` entry
    (its 2 oracle blocks are inline ``toEqual`` literals, not
    ``toMatchSnapshot``), and ``testStatSet``/``testStatSet2`` themselves
    failed json5 vendoring (object-spread over ``outputs``) -- reconstructed
    verbatim per ``tests/fixtures/hoop_explorer/README.md``'s replay recipe.
    """
    outputs = inputs["outputs"]
    test_stat_set = {
        "total_off_3p_attempts": {"value": 10},
        "total_off_2p_attempts": {"value": 20},
        "total_off_fta": {"value": 20},
        "total_off_to": {"value": 20},
        "off_poss": {"value": 100},
        "off_3p": {"value": 0.5, "old_value": 0.4},
        "off_2p": {"value": 0.6, "old_value": 0.4},
        "off_ft": {"value": 0.9, "old_value": 0.7},
        "off_to": {"value": 0.25, "old_value": 0.2},
        **outputs,
    }
    result = _build_off_overrides(test_stat_set)
    expected = {
        "total_off_to": {"value": 26.666666666666668},
        "off_poss": {"value": 106.66666666666667},
        "total_off_fgm": {"value": 5.000099999999999},
        "total_off_2p_made": {"value": 4.0001999999999995},
        "total_off_3p_made": {"value": 1.0002999999999997},
        "total_off_ftm": {"value": 4.000400000000002},
        "team_total_off_pts": {"value": 15.000499999999999},
        "team_total_off_fgm": {"value": 4.999999999999999},
        "team_total_off_3p_made": {"value": 1.0006999999999997},
        "team_total_off_ftm": {"value": 4.000800000000002},
        "team_total_off_to": {"value": 6.667566666666667},
    }
    approx_tree(result, expected)

    # testStatSet2: overrides equal their own old_value (no net change) --
    # buildOffOverrides should be a no-op modulo the always-recomputed
    # total_off_to/off_poss/team_total_off_fgm (undefined -> 0) fields.
    test_stat_set2 = {
        **test_stat_set,
        "off_3p": {"value": 0.4},
        "off_2p": {"value": 0.4},
        "off_ft": {"value": 0.7},
        "off_to": {"value": 0.2},
    }
    result2 = _build_off_overrides(test_stat_set2)
    expected2 = {
        **outputs,
        "team_total_off_fgm": {"value": 0},
        "total_off_to": {"value": 20},
        "off_poss": {"value": 100},
    }
    approx_tree(result2, expected2)


def test_build_productivity_is_pure_arithmetic():
    """``buildProductivity`` has no direct jest test (Task 2.1 surprise #3)
    -- it's only exercised indirectly through ``build_o_rtg`` (covered
    above). This is a targeted unit check of the Dean-Oliver PUE formula
    itself, independent of the ORtg possession-chain plumbing. Promoted to
    public in Task 2.3 (was ``_build_productivity`` in Task 2.2).
    """
    result = build_productivity(o_rtg=110.0, o_adj=1.05, usage=20.0, avg_efficiency=100.0)
    assert set(result.keys()) == {"Adj_ORtg", "Adj_ORtgPlus", "Usage_Bonus", "SoS_Bonus"}
    assert result["Adj_ORtg"] == pytest.approx(110.0 * 1.05)
    # SoS_Bonus should vanish when o_adj == 1 (no schedule adjustment):
    identity = build_productivity(o_rtg=110.0, o_adj=1.0, usage=20.0, avg_efficiency=100.0)
    assert identity["SoS_Bonus"] == pytest.approx(0.0, abs=1e-9)


def test_build_d_rtg_none_stat_set_returns_all_none():
    """``buildDRtg`` returns an all-``undefined`` 5-tuple for a
    null/undefined ``statSet`` (``RatingUtils.ts:1264-1265``), same
    ``is None``-only short-circuit convention as ``build_o_rtg``.
    """
    assert build_d_rtg(None, 100.0, True, False) == (None, None, None, None, None)


def test_build_d_rtg_empty_stat_set_computes_without_error():
    """Contrast with ``build_o_rtg``: every division inside ``buildDRtg`` is
    guard-ternary'd (``x > 0 ? a/b : 0``), so an empty ``{}`` stat set
    computes a degenerate all-zero(ish) result instead of raising
    ``ZeroDivisionError`` (see the module docstring's "Contrast" note).
    """
    d_rtg, adj_d_rtg, raw_d_rtg, raw_adj_d_rtg, diags = build_d_rtg({}, 100.0, True, False)
    # Opponent_Possessions_Box == 0, not > 0 -- both value tuples are None:
    assert d_rtg is None
    assert adj_d_rtg is None
    assert raw_d_rtg is None
    assert raw_adj_d_rtg is None
    assert diags is not None
    assert diags["dRtg"] == 0.0
    assert diags["oppoPoss"] == 0.0


def test_build_d_rtg_baseline_call(player_info, inputs, snap):
    """Replay of ``"RatingUtils - buildDRtg"`` (``RatingUtils.test.ts:247-275``):
    baseline call (``calc_diags=True, override_adjusted=False``), then an
    ``oppo_def_3p`` override call (``calc_diags=True, override_adjusted=True``).
    """
    d_rtg, adj_d_rtg, raw_d_rtg, raw_adj_d_rtg, d_rtg_diags = build_d_rtg(player_info, 100, True, False)

    exp_d_rtg = inputs["expDRtg"]
    exp_d_rtg_adj = inputs["expDRtgAdj"]
    assert d_rtg["value"] == pytest.approx(exp_d_rtg["value"], rel=1e-9)
    assert adj_d_rtg["value"] == pytest.approx(exp_d_rtg_adj["value"], rel=1e-9)
    assert raw_d_rtg is None
    assert raw_adj_d_rtg is None

    # Full dRtgDiags snapshot (47 keys, jest toMatchSnapshot) + toEqual(sampleDrtgDiagnostics):
    expected_diags = snap["RatingUtils RatingUtils - buildDRtg 1"]
    approx_tree(d_rtg_diags, expected_diags)
    approx_tree(d_rtg_diags, inputs["sampleDrtgDiagnostics"])

    # Check with override (RatingUtils.test.ts:264-274):
    player_info["oppo_def_3p"] = {"value": 0.3, "old_value": 0.4}
    d_rtg2, adj_d_rtg2, raw_d_rtg2, raw_adj_d_rtg2, _ = build_d_rtg(player_info, 100, True, True)

    assert d_rtg2["value"] == pytest.approx(90.04849177213895, rel=1e-9)
    assert adj_d_rtg2["value"] == pytest.approx(-3.3304841564964764, rel=1e-9)
    # raw values == the un-overridden baseline values:
    assert raw_d_rtg2["value"] == pytest.approx(exp_d_rtg["value"], rel=1e-9)
    assert raw_adj_d_rtg2["value"] == pytest.approx(exp_d_rtg_adj["value"], rel=1e-9)


def test_build_net_points(inputs, snap):
    """Replay of ``"RatingUtils - buildNetPoints uses adjPtsFactor and
    adjPossFactor"`` (``RatingUtils.test.ts:119-155``): a fresh player doc
    with ``off_team_poss_pct``/``def_team_poss_pct`` set to ``0.25``, ORtg
    and DRtg diagnostics built from it, then ``adjPtsFactor``/
    ``adjPossFactor`` overridden to ``1.1``/``0.9`` on a shallow copy of the
    ORtg diags before calling ``build_net_points``.
    """
    buckets = inputs["samplePlayerStatsResponse"]["responses"][0]["aggregations"]["tri_filter"]["buckets"]["baseline"][
        "player"
    ]["buckets"]
    player_info = copy.deepcopy(buckets[0])
    player_info["off_team_poss_pct"] = {"value": 0.25}
    player_info["def_team_poss_pct"] = {"value": 0.25}

    _, _, _, _, o_rtg_diags = build_o_rtg(
        player_info, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}}, 100, True, False
    )
    _, _, _, _, d_rtg_diags = build_d_rtg(player_info, 100, True, False)

    ortg_with_factors = {**o_rtg_diags, "adjPtsFactor": 1.1, "adjPossFactor": 0.9}
    net_points = build_net_points(player_info, ortg_with_factors, d_rtg_diags, 100, "T%", 1, 1)

    expected = snap["RatingUtils RatingUtils - buildNetPoints uses adjPtsFactor and adjPossFactor 1"]
    approx_tree(net_points, expected)
    assert "defNetPtsIndiv" not in net_points  # onBallDiags never set by this port


def test_adjust_off_rating_stats(inputs, snap):
    """Replay of ``"RatingUtils - adjustOffRatingStats updates the right
    fields"`` (``RatingUtils.test.ts:157-182``): builds a fresh baseline
    ``oRtgDiags`` (``override_adjusted=False``, so ``rawORtg`` and hence
    ``Raw_Usage``/``maybe_raw_o_rtg`` are both ``None``), deep-copies it,
    applies a ``1.1``/``0.9`` pts/poss correction, and asserts the 7-field
    projection against the jest snapshot.
    """
    buckets = inputs["samplePlayerStatsResponse"]["responses"][0]["aggregations"]["tri_filter"]["buckets"]["baseline"][
        "player"
    ]["buckets"]
    player_info = copy.deepcopy(buckets[0])

    _, _, raw_o_rtg, _, o_rtg_diags = build_o_rtg(
        player_info, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}}, 100, True, False
    )
    mutable_diag = copy.deepcopy(o_rtg_diags)
    maybe_raw_o_rtg = raw_o_rtg["value"] if raw_o_rtg else None
    assert maybe_raw_o_rtg is None  # no override was applied -- rawORtg is undefined upstream

    result = adjust_off_rating_stats(1.1, 0.9, mutable_diag, maybe_raw_o_rtg)
    assert result is None  # Raw_Usage is also None -- both nil, matches jest's implicit expectation

    projection = {
        "oRtg": mutable_diag["oRtg"],
        "adjORtg": mutable_diag["adjORtg"],
        "adjORtgPlus": mutable_diag["adjORtgPlus"],
        "Usage_Bonus": mutable_diag["Usage_Bonus"],
        "SoS_Bonus": mutable_diag["SoS_Bonus"],
        "adjPtsFactor": mutable_diag["adjPtsFactor"],
        "adjPossFactor": mutable_diag["adjPossFactor"],
    }
    expected = snap["RatingUtils RatingUtils - adjustOffRatingStats updates the right fields 1"]
    approx_tree(projection, expected)
