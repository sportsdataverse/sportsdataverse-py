"""Oracle tests for mbb_lineup_stats against vendored hoop-explorer snapshots."""

from __future__ import annotations

import copy

import pytest

from sportsdataverse.mbb.mbb_lineup_stats import (
    IGNORE_FIELDS,
    SUM_FIELDS,
    calculate_aggregated_lineup_stats,
    get_stats_diff,
    lineup_to_team_report,
    weighted_avg,
)
from tests.mbb._hoop_explorer_replay import (
    approx_tree,
    find_snapshot_exact,
    find_snapshot_for,
    first_lineup_list,
    insert_old_values,
    load_inputs,
    load_snap,
    to_fixed,
)


@pytest.fixture(scope="module")
def snap() -> dict:
    return load_snap()


@pytest.fixture(scope="module")
def inputs() -> dict:
    return load_inputs()


def test_weighted_avg_two_lineups(inputs, snap):
    lineups = first_lineup_list(inputs)
    acc = copy.deepcopy(lineups[0])
    weighted_avg(acc, lineups[1])
    # acc possession counts must sum
    assert acc["off_poss"]["value"] == pytest.approx(lineups[0]["off_poss"]["value"] + lineups[1]["off_poss"]["value"])


def test_weighted_avg_sum_fields_plain_sum(inputs):
    """SUM_FIELDS (off_poss/def_poss/duration_mins) always plain-sum,
    regardless of the accumulator's starting point -- LineupUtils.ts:478.
    """
    lineups = first_lineup_list(inputs)
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
    lineups = first_lineup_list(inputs)
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
    lineups = first_lineup_list(inputs)
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
    lineups = first_lineup_list(inputs)
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
    lineups = first_lineup_list(inputs)
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
    lineups = first_lineup_list(inputs)
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
    lineups = first_lineup_list(inputs)
    obj = copy.deepcopy(lineups[1])
    obj_snapshot = copy.deepcopy(obj)
    acc: dict = {}
    weighted_avg(acc, obj)
    approx_tree(obj, obj_snapshot)


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
    lineups = [insert_old_values(copy.deepcopy(lineup)) for lineup in first_lineup_list(inputs)]
    lineups[1]["rapmRemove"] = True

    agg = calculate_aggregated_lineup_stats(lineups)
    picked = {key: to_fixed(agg[key]) for key in ("off_poss", "def_poss", "off_adj_ppp", "def_adj_ppp")}

    expected = find_snapshot_for(snap, "calculateAggregatedLineupStats")
    approx_tree(picked, expected)


# --------------------------------------------------------------------------
# Task 1.3: lineup_to_team_report / get_stats_diff
# --------------------------------------------------------------------------


def test_on_off_partition_micro():
    """Hand-computable micro-case (brief, adapted to the real ES
    players_array hits shape used by _get_player_set): player E is on in
    lineup 1 (A_B_C_D_E), off in lineup 2 (A_B_C_D_F). E's ON off_poss must
    equal lineup 1's off_poss (10.0); E's OFF off_poss must equal lineup 2's
    off_poss (10.0, the only lineup E doesn't appear in).
    """

    def _players_array(codes: str) -> dict:
        return {"hits": {"hits": [{"_source": {"players": [{"id": p, "code": p} for p in codes]}}]}}

    a = {
        "key": "A_B_C_D_E",
        "players_array": _players_array("ABCDE"),
        "off_poss": {"value": 10.0},
        "def_poss": {"value": 10.0},
        "off_pts": {"value": 12.0},
        "def_pts": {"value": 8.0},
    }
    b = {
        "key": "A_B_C_D_F",
        "players_array": _players_array("ABCDF"),
        "off_poss": {"value": 10.0},
        "def_poss": {"value": 10.0},
        "off_pts": {"value": 9.0},
        "def_pts": {"value": 11.0},
    }
    rep = lineup_to_team_report({"lineups": [a, b], "error_code": None})
    e = next(p for p in rep["players"] if p["playerId"] == "E")
    assert e["on"]["off_poss"]["value"] == pytest.approx(10.0)
    assert e["off"]["off_poss"]["value"] == pytest.approx(10.0)
    # D is on in both lineups -> never OFF -> zero-filled via _copy_and_zero
    d = next(p for p in rep["players"] if p["playerId"] == "D")
    assert d["off"]["off_poss"]["value"] == pytest.approx(0.0)


def test_get_stats_diff_synthetic():
    """Hand-crafted straight-diff check for get_stats_diff (LineupUtils.ts:185)
    -- getStatsDiff has no dedicated jest test/snapshot upstream, so this is
    a synthetic oracle exercising the value/old_value/override/missing-field
    branches directly.
    """
    stat_set1 = {
        "off_ppp": {"value": 110.0, "old_value": 108.0, "override": "luck"},
        "off_poss": {"value": 500.0},
        "only_in_1": {"value": 5.0},
    }
    stat_set2 = {
        "off_ppp": {"value": 100.0, "old_value": 102.0},
        "off_poss": {"value": 480.0},
        "only_in_2": {"value": 3.0},
    }
    diff = get_stats_diff(stat_set1, stat_set2, "Team A", "Team B")

    assert diff["off_ppp"]["value"] == pytest.approx(10.0)
    # old_value diffed too, but the *result*'s override comes only from
    # stat_set1's own override (LineupUtils.ts:207) -- old_value is diffed
    # unconditionally when both sides carry one, independent of override:
    assert diff["off_ppp"]["old_value"] == pytest.approx(6.0)
    assert diff["off_ppp"]["override"] == "luck"

    assert diff["off_poss"]["value"] == pytest.approx(20.0)
    assert diff["off_poss"]["old_value"] is None  # neither side carries old_value
    assert diff["off_poss"]["override"] is None

    # only_in_1 has no counterpart on stat_set2 -> nil -> None (JS undefined)
    assert diff["only_in_1"] is None
    # only_in_2 isn't iterated at all (loop is over stat_set1's own keys)
    assert "only_in_2" not in diff

    assert diff["off_title"] == "Team A"
    assert diff["def_title"] == "Team B"


def _build_lineup_report(inputs: dict) -> dict:
    """Replays the jest ``lineupReport`` const (``LineupUtils.test.ts``):
    the 3 vendored lineup buckets, each run through ``insertOldValues``,
    plus the hardcoded scalar companions documented in
    ``tests/fixtures/hoop_explorer/README.md``.
    """
    lineups = [insert_old_values(copy.deepcopy(lineup)) for lineup in first_lineup_list(inputs)]
    return {"lineups": lineups, "avgOff": 100.0, "error_code": "test"}


_SOME_ONOFF_PICK_FIELDS = [
    "key",
    "off_poss",
    "def_poss",
    "off_ppp",
    "def_ppp",
    "off_adj_opp",
    "def_adj_opp",
    "def_2prim",
    "def_2primr",
    "off_ft",
    "off_orb",
    "def_orb",
    "off_ftr",
    "total_off_fga",
    "total_off_pts",
    "doc_count",
    "player_array",  # (upstream typo for players_array; never present -- always ignored)
    "duration_mins",
    "total_off_trans_poss",
    "off_scramble_ppp",
]


def _pick_to_fixed(obj: dict, fields: list[str]) -> dict:
    return {f: to_fixed(obj[f]) for f in fields if f in obj}


def _some_on_off_vals(players: list[dict]) -> list[dict]:
    ayala = next(p for p in players if p["on"]["key"] == "'On' Ayala, Eric")
    items = [ayala["on"], ayala["off"], ayala.get("replacement") or {}]
    return [_pick_to_fixed(item, _SOME_ONOFF_PICK_FIELDS) for item in items]


def _snap_key_for(diag_mode: int, regress_diffs: float, inc_on_off: bool) -> str:
    regress_str = "-500" if regress_diffs < 0 else "0"
    inc_str = "true" if inc_on_off else "false"
    return (
        f"LineupUtils LineupUtils - lineupToTeamReport: "
        f"diagMode=[{diag_mode}] regressDiffs=[{regress_str}] incOnOff=[{inc_str}] 1"
    )


_SWEEP = [
    (diag_mode, regress_diffs, inc_on_off)
    for diag_mode in (0, 10)
    for regress_diffs in (0.0, -500.0)
    for inc_on_off in (False, True)
]


@pytest.mark.parametrize("diag_mode,regress_diffs,inc_on_off", _SWEEP)
def test_lineup_to_team_report_matches_snapshot(inputs, snap, diag_mode, regress_diffs, inc_on_off):
    """Oracle test for lineup_to_team_report, sweeping the same
    (diagMode x regressDiffs x incOnOff) grid as
    ``LineupUtils.test.ts``'s ``lineupToTeamReport`` test (its
    ``[0, 100 - 100, -500]`` regressDiffs sweep collapses to two distinct
    values -- ``100 - 100`` evaluates to plain ``0`` in JS, so it's dropped
    here as a duplicate of the literal ``0`` case; both produce
    byte-identical snapshot entries upstream).

    Replicates every structural jest assertion (player roster, replacement
    roster, Wiggins' all-zero OFF split, Ayala/Cowan lineup composition,
    Wiggins' empty replacement, and -- when applicable -- Ayala's
    same-4-lineups replacement diagnostic) plus the numeric ``someOnOffVals``
    projection (pick + toFixed(3)) against the vendored snapshot.
    """
    lineup_report = _build_lineup_report(inputs)
    report = lineup_to_team_report(
        lineup_report,
        inc_replacement=inc_on_off,
        regress_diffs=regress_diffs,
        rep_on_off_diag_mode=diag_mode,
    )
    players = report["players"]

    # Player roster: flatMap(on.key, off.key), sorted.
    player_list = sorted(kv for p in players for kv in (p["on"]["key"], p["off"]["key"]))
    assert player_list == [
        "'Off' Ayala, Eric",
        "'Off' Cowan, Anthony",
        "'Off' Morsell, Darryl",
        "'Off' Scott, Donta",
        "'Off' Smith, Jalen",
        "'Off' Wiggins, Aaron",
        "'On' Ayala, Eric",
        "'On' Cowan, Anthony",
        "'On' Morsell, Darryl",
        "'On' Scott, Donta",
        "'On' Smith, Jalen",
        "'On' Wiggins, Aaron",
    ]

    # Replacement roster: only present (non-None) when inc_on_off.
    replacement_player_list = sorted(p["replacement"]["key"] for p in players if p.get("replacement") is not None)
    expected_replacements = (
        [
            "'r:On-Off' Ayala, Eric",
            "'r:On-Off' Cowan, Anthony",
            "'r:On-Off' Morsell, Darryl",
            "'r:On-Off' Scott, Donta",
            "'r:On-Off' Smith, Jalen",
            "'r:On-Off' Wiggins, Aaron",
        ]
        if inc_on_off
        else []
    )
    assert replacement_player_list == expected_replacements

    # Wiggins is on in all 3 lineups -> never OFF -> "off" is zero-filled
    # (_copy_and_zero), so every off field except "key" must be exactly 0.
    wiggins = next(p for p in players if p["off"]["key"] == "'Off' Wiggins, Aaron")
    for key, field in wiggins["off"].items():
        if key == "key":
            continue
        assert isinstance(field, dict) and field.get("value", 0) == 0, f"{key}: {field}"

    # Lineup composition: Ayala's teammate-possession overlap with Cowan.
    ayala = next(p for p in players if p["on"]["key"] == "'On' Ayala, Eric")
    cowan = ayala["teammates"]["Cowan, Anthony"]
    assert cowan["on"]["off_poss"] == pytest.approx(598.0)
    assert cowan["on"]["def_poss"] == pytest.approx(581.0)
    assert cowan["off"]["off_poss"] == pytest.approx(211.0)
    assert cowan["off"]["def_poss"] == pytest.approx(213.0)

    # Empty replacement check: Wiggins is never OFF, so no complement
    # off-lineup is ever found for any of his (3) on-lineups.
    if inc_on_off:
        wiggins_repl = next(p["replacement"] for p in players if p["replacement"]["key"] == "'r:On-Off' Wiggins, Aaron")
        assert wiggins_repl["lineupUsage"] == {}
        if diag_mode > 0:
            assert wiggins_repl.get("myLineups") == []
        else:
            assert "myLineups" not in wiggins_repl

    # Diagnostic myLineups retention: Ayala's 2 lineups both find a
    # complement (lineup 2, the only Ayala-less lineup, is 4/5-complementary
    # to both), in original bucket order.
    if diag_mode > 0 and inc_on_off:
        ayala_repl = next(p["replacement"] for p in players if p["on"]["key"] == "'On' Ayala, Eric")
        same4 = [lineup["key"] for lineup in ayala_repl["myLineups"]]
        assert same4 == [
            "AaWiggins_AnCowan_DaMorsell_ErAyala_JaSmith",
            "AaWiggins_AnCowan_DoScott_ErAyala_JaSmith",
        ]

    # Numeric oracle: someOnOffVals (pick + toFixed(3)) vs. vendored snapshot.
    expected = find_snapshot_exact(snap, _snap_key_for(diag_mode, regress_diffs, inc_on_off))
    actual = _some_on_off_vals(players)
    approx_tree(actual, expected)
