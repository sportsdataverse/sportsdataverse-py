"""Oracle tests for mbb_lineup_stats against vendored hoop-explorer snapshots."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from sportsdataverse.mbb.mbb_lineup_stats import (
    IGNORE_FIELDS,
    SUM_FIELDS,
    calculate_aggregated_lineup_stats,
    weighted_avg,
)

#: Verbatim from ``LuckUtils.affectedFieldSet`` (``LuckUtils.ts:159``) --
#: the jest test file's ``insertOldValues`` local helper stamps
#: ``old_value``/``override`` onto every stat whose key is in this set
#: (see ``tests/fixtures/hoop_explorer/README.md``).
_LUCK_AFFECTED_FIELDS = frozenset(
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

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/hoop_explorer"


@pytest.fixture(scope="module")
def snap() -> dict:
    return json.loads((FIXTURES / "lineup_utils_snap.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def inputs() -> dict:
    return json.loads((FIXTURES / "lineup_utils_inputs.json").read_text(encoding="utf-8"))


def _approx_tree(a, b, path=""):
    """Recursively assert numeric equality to 1e-9 rel, exact otherwise."""
    if isinstance(b, dict):
        assert isinstance(a, dict), f"{path}: expected dict"
        for k, v in b.items():
            assert k in a, f"{path}.{k}: missing"
            _approx_tree(a[k], v, f"{path}.{k}")
    elif isinstance(b, (int, float)) and not isinstance(b, bool):
        assert a == pytest.approx(b, rel=1e-9, abs=1e-12), f"{path}: {a} != {b}"
    elif isinstance(b, list):
        assert len(a) == len(b), f"{path}: length {len(a)} != {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            _approx_tree(x, y, f"{path}[{i}]")
    else:
        assert a == b, f"{path}: {a!r} != {b!r}"


def _first_lineup_list(inputs: dict) -> list[dict]:
    # Known vendored shape (see tests/fixtures/hoop_explorer/README.md):
    # sampleLineupStatsResponse.responses[0].aggregations.lineups.buckets
    try:
        buckets = inputs["sampleLineupStatsResponse"]["responses"][0]["aggregations"]["lineups"]["buckets"]
        if isinstance(buckets, list) and buckets and isinstance(buckets[0], dict) and "off_poss" in buckets[0]:
            return buckets
    except (KeyError, IndexError, TypeError):
        pass

    def _search(node, depth: int = 0) -> list[dict] | None:
        if depth > 6:
            return None
        if isinstance(node, list) and node and isinstance(node[0], dict) and "off_poss" in node[0]:
            return node
        if isinstance(node, dict):
            for v in node.values():
                found = _search(v, depth + 1)
                if found is not None:
                    return found
        elif isinstance(node, list):
            for v in node:
                found = _search(v, depth + 1)
                if found is not None:
                    return found
        return None

    found = _search(inputs)
    if found is not None:
        return found
    pytest.skip("no lineup list found in vendored inputs")


def test_weighted_avg_two_lineups(inputs, snap):
    lineups = _first_lineup_list(inputs)
    acc = copy.deepcopy(lineups[0])
    weighted_avg(acc, lineups[1])
    # acc possession counts must sum
    assert acc["off_poss"]["value"] == pytest.approx(lineups[0]["off_poss"]["value"] + lineups[1]["off_poss"]["value"])


def test_weighted_avg_sum_fields_plain_sum(inputs):
    """SUM_FIELDS (off_poss/def_poss/duration_mins) always plain-sum,
    regardless of the accumulator's starting point -- LineupUtils.ts:478.
    """
    lineups = _first_lineup_list(inputs)
    l0, l1 = lineups[0], lineups[1]
    acc = copy.deepcopy(l0)
    weighted_avg(acc, l1)
    for field in SUM_FIELDS:
        if field in l0 and field in l1:
            expected = l0[field]["value"] + l1[field]["value"]
            assert acc[field]["value"] == pytest.approx(expected, rel=1e-9), field


def test_weighted_avg_ignore_fields_never_populated(inputs):
    """Starting from an empty accumulator, IGNORE_FIELDS keys present on
    the merged object must never be copied into the accumulator --
    LineupUtils.ts:464 (ignoreFieldSet). ``game_info`` is excluded here
    since it raises NotImplementedError (see other test).
    """
    lineups = _first_lineup_list(inputs)
    acc: dict = {}
    weighted_avg(acc, lineups[0])
    for field in IGNORE_FIELDS - {"game_info"}:
        if field in lineups[0]:
            assert field not in acc, f"{field} should never be merged into acc"


def test_weighted_avg_shot_pct_field_weighted_by_own_lineup_total(inputs):
    """Hand-derived parity check for a shot-type-percentage field
    (off_efg, matched via the generic 'off_' FGA-weight fallback since it
    doesn't match the shot-type regex). With regress_diffs=0 (the only
    mode weighted_avg supports), the weight for each merged lineup is
    exactly that lineup's own total_off_fga -- LineupUtils.ts:612-615 /
    645-720. This pins the semantic that matters most for Task 1.2/1.3:
    each merge step weights the *incoming* object's own totals, not any
    running total already on the accumulator.
    """
    lineups = _first_lineup_list(inputs)
    l0, l1 = lineups[0], lineups[1]
    acc: dict = {}
    weighted_avg(acc, l0)
    weighted_avg(acc, l1)

    w0 = l0["total_off_fga"]["value"]
    w1 = l1["total_off_fga"]["value"]
    expected = l0["off_efg"]["value"] * w0 + l1["off_efg"]["value"] * w1
    assert acc["off_efg"]["value"] == pytest.approx(expected, rel=1e-9)


def test_weighted_avg_shot_type_pct_field_weighted_by_total_made_attempts(inputs):
    """Hand-derived parity check for a genuine shot-type field (off_3p),
    which routes through getShotTypeField -> total_off_3p_attempts as its
    weight source (LineupUtils.ts:635-641, 672-677), not the generic FGA
    fallback used by off_efg.
    """
    lineups = _first_lineup_list(inputs)
    l0, l1 = lineups[0], lineups[1]
    acc: dict = {}
    weighted_avg(acc, l0)
    weighted_avg(acc, l1)

    w0 = l0["total_off_3p_attempts"]["value"]
    w1 = l1["total_off_3p_attempts"]["value"]
    expected = l0["off_3p"]["value"] * w0 + l1["off_3p"]["value"] * w1
    assert acc["off_3p"]["value"] == pytest.approx(expected, rel=1e-9)


def test_weighted_avg_total_fields_plain_sum(inputs):
    """total_* fields always plain-sum (LineupUtils.ts:693-696)."""
    lineups = _first_lineup_list(inputs)
    l0, l1 = lineups[0], lineups[1]
    acc: dict = {}
    weighted_avg(acc, l0)
    weighted_avg(acc, l1)

    expected = l0["total_off_fga"]["value"] + l1["total_off_fga"]["value"]
    assert acc["total_off_fga"]["value"] == pytest.approx(expected, rel=1e-9)


def test_weighted_avg_trans_scramble_fields_stay_zero(inputs):
    """off_trans_* / def_trans_* / off_scramble_* / def_scramble_* fields
    are intentionally not accumulated by weightedAvg -- their ppp is
    handled by recalculatePlayTypePoss inside completeWeightedAvg
    (LineupUtils.ts:700-709), which is out of scope for this task.

    NOTE (upstream quirk, LineupUtils.ts:659-666): the ``{value: 0}``
    init-if-absent block runs unconditionally for *every* non-ignored
    field, before the if/elif dispatch that decides whether to actually
    accumulate into it. So these fields DO end up present in the
    accumulator with a key -- they just never advance past ``0.0``, even
    across many merged lineups (the init only fires once, on first sight
    of the key). Callers must not treat "absent from acc" as the signal
    for "not accumulated"; check the *value*, not membership.
    """
    lineups = _first_lineup_list(inputs)
    acc: dict = {}
    weighted_avg(acc, lineups[0])
    weighted_avg(acc, lineups[1])
    for prefix in ("off_trans_", "def_trans_", "off_scramble_", "def_scramble_"):
        skipped = [k for k in lineups[0] if k.startswith(prefix) and not k.startswith("total_")]
        assert skipped, f"fixture should contain at least one {prefix}* field"
        for key in skipped:
            assert key in acc, f"{key} should still be present (zero-initialized) in acc"
            assert acc[key]["value"] == 0.0, f"{key} should stay 0.0 (never accumulated)"


def test_weighted_avg_game_info_not_implemented():
    """The game_info merge branch (LineupUtils.ts:722-746) is deliberately
    deferred -- see the module docstring. weighted_avg must fail loudly
    rather than silently drop the data.
    """
    acc: dict = {}
    with pytest.raises(NotImplementedError):
        weighted_avg(acc, {"game_info": {"buckets": []}})


def test_weighted_avg_does_not_mutate_obj(inputs):
    """weighted_avg must only mutate mutable_acc, never obj."""
    lineups = _first_lineup_list(inputs)
    obj = copy.deepcopy(lineups[1])
    obj_snapshot = copy.deepcopy(obj)
    acc: dict = {}
    weighted_avg(acc, obj)
    _approx_tree(obj, obj_snapshot)


def test_weighted_avg_override_old_value_accumulation():
    """Reviewer-mandated synthetic test (from Task 1.1's report) exercising
    the override/old_value luck-adjustment bookkeeping branches of
    weighted_avg. No vendored fixture lineup carries an override on its own
    -- that's a jest test-local mutation applied by ``insertOldValues`` (see
    test_aggregated_lineup_stats_matches_snapshot below) -- so this is a
    hand-crafted minimal 2-lineup case with hand-computed expected values.
    """
    lineup_a = {
        "total_off_fga": {"value": 8.0},
        "off_efg": {"value": 0.5, "old_value": 0.4, "override": "luck-a"},
    }
    lineup_b = {
        "total_off_fga": {"value": 10.0},
        "off_efg": {"value": 0.6, "old_value": 0.55, "override": "luck-b"},
    }
    acc: dict = {}
    weighted_avg(acc, lineup_a)
    weighted_avg(acc, lineup_b)

    # off_efg doesn't match the shot-type regex (getShotTypeField requires
    # the type to start with 2/3), isn't in any of the ppp/orb/fta/ast
    # weight tables, and isn't total_/SUM_FIELDS -- so it falls through to
    # the generic off_-prefix FGA-weight fallback (LineupUtils.ts:709-713).
    # With regress_diffs=0 (weighted_avg's only mode), that weight is
    # exactly each merged lineup's own total_off_fga.
    w_a, w_b = 8.0, 10.0
    expected_value = 0.5 * w_a + 0.6 * w_b
    expected_old_value = 0.4 * w_a + 0.55 * w_b
    assert acc["off_efg"]["value"] == pytest.approx(expected_value, rel=1e-9)
    assert acc["off_efg"]["old_value"] == pytest.approx(expected_old_value, rel=1e-9)
    # override is stamped once on first-sight init (from lineup_a, merged
    # first) and never overwritten by a later lineup's own override value:
    # the "was init'd without override" elif (LineupUtils.ts:665-668) only
    # fires when the accumulator doesn't already carry a truthy override.
    assert acc["off_efg"]["override"] == "luck-a"


def _insert_old_values(lineup: dict) -> dict:
    """Python replay of the jest test file's local ``insertOldValues``
    helper (``LineupUtils.test.ts``, inside the ``describe("LineupUtils")``
    block): for every stat whose key is in ``LuckUtils.affectedFieldSet``
    and whose ``value`` is not nil, stamp ``old_value = value`` and
    ``override = "Test override"``. Mutates ``lineup`` in place and returns
    it (matching the jest helper's own mutate-and-return shape).
    """
    for key, stat in lineup.items():
        if key in _LUCK_AFFECTED_FIELDS and isinstance(stat, dict) and stat.get("value") is not None:
            stat["old_value"] = stat["value"]
            stat["override"] = "Test override"
    return lineup


def _to_fixed(obj):
    """Python replay of the jest test file's local ``toFixed`` helper:
    3-decimal string formatting for snapshot comparison, preserving the
    ``override``/``old_value`` shape when present.
    """
    if not isinstance(obj, dict):
        return obj
    if obj.get("override"):
        return {
            "value": f"{obj['value']:.3f}",
            "old_value": f"{obj['old_value']:.3f}",
            "override": obj["override"],
        }
    if "value" in obj:
        return {"value": f"{obj['value']:.3f}"}
    return obj


def _find_snapshot_for(snap: dict, needle: str):
    for name, val in snap.items():
        if needle.lower() in name.lower() and isinstance(val, dict):
            return val
    pytest.skip(f"no parsed snapshot entry matching {needle!r}")


def test_aggregated_lineup_stats_matches_snapshot(inputs, snap):
    """Oracle test for calculate_aggregated_lineup_stats, replaying the two
    jest test-local transforms documented in
    tests/fixtures/hoop_explorer/README.md before calling the ported
    function: (1) ``insertOldValues`` stamps old_value/override onto every
    LuckUtils.affectedFieldSet field of every lineup, and (2)
    ``lineups[1].rapmRemove = true`` diverts the 2nd lineup into the
    all_lineups sub-accumulator. The jest snapshot only picks 4 fields
    (off_poss/def_poss/off_adj_ppp/def_adj_ppp) and formats them with
    toFixed(3) before matching -- replicate both the pick and the
    formatting so the comparison is apples-to-apples.
    """
    lineups = [_insert_old_values(copy.deepcopy(lineup)) for lineup in _first_lineup_list(inputs)]
    lineups[1]["rapmRemove"] = True

    agg = calculate_aggregated_lineup_stats(lineups)
    picked = {key: _to_fixed(agg[key]) for key in ("off_poss", "def_poss", "off_adj_ppp", "def_adj_ppp")}

    expected = _find_snapshot_for(snap, "calculateAggregatedLineupStats")
    _approx_tree(picked, expected)
