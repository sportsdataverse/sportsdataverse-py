"""Oracle tests for ``mbb_luck`` (offensive half) against vendored hoop-explorer fixtures.

**Task 2.4** replays ``LuckUtils.test.ts``'s 3 offensive ``test()`` blocks
(``src/utils/stats/__tests__/LuckUtils.test.ts:38-116``) call-for-call:

- ``"LuckUtils - calcOffTeamLuckAdj"`` (line 38) -- baseline call, asserted
  against both the ``luck_utils_snap.json`` snapshot entry and the
  ``sampleOffOnOffLuckDiagnostics`` ``toEqual`` oracle (both vendored in
  Task 2.1).
- ``"LuckUtils - calcOffTeamLuckAdj (+manual overrides)"`` (line 51) -- same
  call with a mutated ``off_to``/``off_2p``/``off_ft`` (via manually
  stamped ``old_value``s) plus a ``manualOverrides`` array forcing Cowan,
  Anthony's ``expected3P`` to ``0.5``.
- ``"LuckUtils - calcOffPlayerLuckAdj"`` (line 84) -- asserts
  ``calc_off_player_luck_adj`` is a literal 1-player-team delegation, plus
  the ``sample_3pa_override`` cross-check.

Also adds a synthetic hand-computed 2-player Bayesian-shrink micro-test (per
the task brief) and a synthetic round-trip test for
:func:`~sportsdataverse.mbb.mbb_luck._deserialize_lineup_sum` (the bit-packed
lineup-aggregate decoder, unexercised by the vendored oracle -- see
``mbb_luck.py``'s module docstring).
"""

from __future__ import annotations

import copy

import pytest

from sportsdataverse.mbb.mbb_luck import (
    LUCK_AFFECTED_FIELDS,
    _deserialize_lineup_sum,
    build_3p_shot_info,
    build_adjusted_3p,
    build_exp_3p,
    calc_off_player_luck_adj,
    calc_off_team_luck_adj,
)
from tests.mbb._hoop_explorer_replay import approx_tree, load_fixture


@pytest.fixture(scope="module")
def snap() -> dict:
    return load_fixture("luck_utils_snap.json")


@pytest.fixture(scope="module")
def inputs() -> dict:
    return load_fixture("luck_utils_inputs.json")


@pytest.fixture(scope="module")
def rating_inputs() -> dict:
    """``samplePlayerStatsResponse`` -- vendored once in ``rating_utils_inputs.json``
    (Task 2.1's report: shared across both jest suites, not duplicated).
    """
    return load_fixture("rating_utils_inputs.json")


@pytest.fixture()
def base_team(inputs: dict) -> dict:
    return copy.deepcopy(
        inputs["sampleTeamStatsResponse"]["responses"][0]["aggregations"]["global"]["only"]["buckets"]["team"]
    )


@pytest.fixture()
def sample_team_on(inputs: dict) -> dict:
    return copy.deepcopy(
        inputs["sampleTeamStatsResponse"]["responses"][0]["aggregations"]["tri_filter"]["buckets"]["on"]
    )


@pytest.fixture()
def base_players(rating_inputs: dict) -> list[dict]:
    return copy.deepcopy(
        rating_inputs["samplePlayerStatsResponse"]["responses"][0]["aggregations"]["tri_filter"]["buckets"]["baseline"][
            "player"
        ]["buckets"]
    )


@pytest.fixture()
def sample_players_on(rating_inputs: dict) -> list[dict]:
    return copy.deepcopy(
        rating_inputs["samplePlayerStatsResponse"]["responses"][0]["aggregations"]["tri_filter"]["buckets"]["on"][
            "player"
        ]["buckets"]
    )


@pytest.fixture()
def base_players_map(base_players: list[dict]) -> dict:
    return {p["key"]: p for p in base_players}


def test_luck_affected_fields_verbatim():
    """Verbatim from ``LuckUtils.affectedFieldSet`` (``LuckUtils.ts:159-169``)."""
    assert LUCK_AFFECTED_FIELDS == frozenset(
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


def test_calc_off_team_luck_adj_baseline(base_team, sample_team_on, base_players_map, sample_players_on, inputs, snap):
    """Replay of ``"LuckUtils - calcOffTeamLuckAdj"`` (``LuckUtils.test.ts:38-50``)."""
    off_team_luck_adj = calc_off_team_luck_adj(
        sample_team_on,
        sample_players_on,
        base_team,
        base_players_map,
        100.0,
    )

    expected_snap = snap["LuckUtils LuckUtils - calcOffTeamLuckAdj: Offensive Luck Adjustments 1"]
    approx_tree(off_team_luck_adj, expected_snap)

    expected_oracle = inputs["sampleOffOnOffLuckDiagnostics"]
    approx_tree(off_team_luck_adj, expected_oracle)

    # player3PInfo is keyed by player, sorted descending by shot_info_total_3p:
    assert list(off_team_luck_adj["player3PInfo"].keys()) == ["Cowan, Anthony", "Wiggins, Aaron"]


def test_calc_off_team_luck_adj_manual_overrides(base_team, sample_team_on, base_players_map, sample_players_on, snap):
    """Replay of ``"LuckUtils - calcOffTeamLuckAdj (+manual overrides)"``
    (``LuckUtils.test.ts:51-83``): mutates ``off_to``/``off_2p``/``off_ft``
    with a stamped ``old_value`` (as if previously luck-adjusted) and forces
    Cowan, Anthony's expected 3P% to ``0.5`` via ``manualOverrides``.
    """
    overrides = [
        {
            "rowId": "Cowan, Anthony",
            "statName": "off_3p",
            "newVal": 0.5,
            "use": True,
        }
    ]
    adj_sample_team_on = copy.deepcopy(sample_team_on)
    adj_sample_team_on["off_to"] = {"value": 0.1, "old_value": sample_team_on["off_to"]["value"]}
    adj_sample_team_on["off_2p"] = {"value": 0.8, "old_value": sample_team_on["off_2p"]["value"]}
    adj_sample_team_on["off_ft"] = {"value": 0.0, "old_value": sample_team_on["off_ft"]["value"]}

    off_team_luck_adj = calc_off_team_luck_adj(
        adj_sample_team_on,
        sample_players_on,
        base_team,
        base_players_map,
        100.0,
        None,
        overrides,
    )

    expected_snap = snap["LuckUtils LuckUtils - calcOffTeamLuckAdj (+manual overrides): Offensive Luck Adjustments 1"]
    approx_tree(off_team_luck_adj, expected_snap)

    # The manual override pins Cowan, Anthony's expected3P to exactly 0.5:
    assert off_team_luck_adj["player3PInfo"]["Cowan, Anthony"]["expected3P"] == pytest.approx(0.5)


def test_calc_off_player_luck_adj_is_team_delegation(base_players, sample_players_on):
    """Replay of ``"LuckUtils - calcOffPlayerLuckAdj"`` (``LuckUtils.test.ts:84-116``):
    a 1-player-team call must equal the dedicated player function, and the
    ``sample_3pa_override`` cross-check must agree on ``sample3PA``.
    """
    sample_player = sample_players_on[0]
    base_player = base_players[0]

    off_team_luck_adj = calc_off_team_luck_adj(
        sample_player,
        [sample_player],
        base_player,
        {base_player["key"]: base_player},
        100.0,
    )
    off_player_luck_adj = calc_off_player_luck_adj(sample_player, base_player, 100.0)
    # Literal delegation -- same call graph, so exact (not just approx) equality:
    approx_tree(off_player_luck_adj, off_team_luck_adj)

    # Finally test the 3P override (LuckUtils.test.ts:100-115):
    overridden_sample = {**sample_player, "total_off_3p_attempts": {"value": 0}}
    off_team_luck_adj_with_override = calc_off_team_luck_adj(
        overridden_sample,
        [sample_player],
        base_player,
        {base_player["key"]: base_player},
        100.0,
        sample_player["total_off_3p_attempts"]["value"],
    )
    assert off_player_luck_adj["sample3PA"] == pytest.approx(off_team_luck_adj_with_override["sample3PA"])
    # (rest of the stats are different because of ORBs, per the upstream comment)


def test_build_3p_shot_info_and_adjusted_3p_and_exp_3p(base_players):
    """Sanity-check the shot-decomposition chain in isolation (these feed
    ``calc_off_team_luck_adj``'s ``player3PInfo`` but are independently
    public per the task brief).
    """
    player = base_players[0]
    info = build_3p_shot_info(player)
    assert info["shot_info_total_3p"] == player["total_off_3p_attempts"]["value"]
    assert set(info.keys()) == {
        "shot_info_ast_3pm",
        "shot_info_early_3pa",
        "shot_info_scramble_3pa",
        "shot_info_unast_3pm",
        "shot_info_unknown_3pM",
        "shot_info_total_3p",
    }

    adj = build_adjusted_3p(player, info)
    assert set(adj.keys()) == {"base3P", "unassisted3P", "assisted3P", "baseAssistPct"}
    assert adj["base3P"] == pytest.approx(player["off_3p"]["value"])

    combined = {**info, **adj}
    expected_makes = build_exp_3p(combined)
    # Expected makes should be non-negative and bounded by total 3PA (the
    # weighted sum of 3 percentages each in [0, 1] over disjoint buckets
    # that partition shot_info_total_3p):
    assert 0.0 <= expected_makes <= info["shot_info_total_3p"] * 1.0 + 1e-9


def test_bayesian_shrink_two_player_micro_case():
    """Synthetic hand-computed test (per the task brief) for the
    possession-weighted Bayesian-shrink formula that
    :func:`calc_off_team_luck_adj` applies:

    ``regress3P = (sampleBase3P*base3PA + sample3P*sample3PA) /
    (sample3PA+base3PA)``

    where ``sampleBase3P`` is itself the 3PA-weighted average of each
    player's *expected* 3P% (from :func:`build_exp_3p`, using each
    player's own baseline 3P% since no per-player ``off_3p_ast`` data is
    supplied here, so ``build_adjusted_3p``'s unassisted/assisted split
    degenerates to a single flat rate for every shot bucket).

    2-player micro-case, by hand:

    - Player A: sample 10/20 3PA (0.50), base 30/100 3PA (0.30).
    - Player B: sample 5/20 3PA (0.25), base 20/100 3PA (0.20).

    Team sample: 15/40 3PA -> sample3P = 0.375, sample3PA = 40.
    Team base: 50/200 3PA -> base3PA = 200 (only the *count* matters for
    ``base3PA`` -- ``LuckUtils.get(baseTeam.total_off_3p_attempts, 0)`` is a
    single team-level number independent of the per-player breakdown).

    ``sampleBase3P`` (the 3PA-weighted expected 3P% across the 2 players,
    each with ``off_3p==base 3P%`` and no assist data so ``base3P ==
    unassisted3P == assisted3P``): ``(20*0.30 + 20*0.20) / 40 == 0.25``.

    ``regress3P = (0.25*200 + 0.375*40) / (40+200) = (50 + 15) / 240 =
    65/240 = 0.2708333...``
    """
    player_a_sample = {
        "key": "A",
        "total_off_3p_attempts": {"value": 20},
        "total_off_3p_made": {"value": 10},
    }
    player_b_sample = {
        "key": "B",
        "total_off_3p_attempts": {"value": 20},
        "total_off_3p_made": {"value": 5},
    }
    player_a_base = {
        "key": "A",
        "total_off_3p_attempts": {"value": 100},
        "total_off_3p_made": {"value": 30},
        "off_3p": {"value": 0.30},
    }
    player_b_base = {
        "key": "B",
        "total_off_3p_attempts": {"value": 100},
        "total_off_3p_made": {"value": 20},
        "off_3p": {"value": 0.20},
    }
    sample_team = {
        "off_poss": {"value": 100.0},
        "off_3p": {"value": 15.0 / 40.0},
        "total_off_3p_attempts": {"value": 40},
    }
    base_team = {"total_off_3p_attempts": {"value": 200}}
    base_players_map = {"A": player_a_base, "B": player_b_base}

    diags = calc_off_team_luck_adj(
        sample_team,
        [player_a_sample, player_b_sample],
        base_team,
        base_players_map,
        100.0,
    )

    assert diags["sample3PA"] == pytest.approx(40.0)
    assert diags["base3PA"] == pytest.approx(200.0)
    assert diags["sample3P"] == pytest.approx(15.0 / 40.0)
    assert diags["sampleBase3P"] == pytest.approx(0.25)
    assert diags["regress3P"] == pytest.approx(65.0 / 240.0)
    assert diags["delta3P"] == pytest.approx(65.0 / 240.0 - 15.0 / 40.0)


def test_deserialize_lineup_sum_round_trip():
    """Synthetic round-trip test for :func:`_deserialize_lineup_sum` (the
    10-bit-per-slot bit-packed lineup-aggregate decoder) -- not exercised by
    the vendored jest oracle (see ``mbb_luck.py``'s module docstring), so
    this hand-built packed value is the only correctness check for the
    bit-shift/mask arithmetic.
    """
    slots = [7, 512, 1023, 0, 300]  # 0x3ff == 1023 is the max representable per-slot value
    packed_value = sum(v << (10 * i) for i, v in enumerate(slots))
    assert _deserialize_lineup_sum({"value": packed_value}) == slots

    # Non-dict / None input folds to all-zero (mirrors `n?.value || 0`):
    assert _deserialize_lineup_sum(None) == [0, 0, 0, 0, 0]
    assert _deserialize_lineup_sum({}) == [0, 0, 0, 0, 0]
