"""Oracle tests for ``mbb_rapm`` against vendored hoop-explorer fixtures.

**Task 3.2** replays ``RapmUtils.test.ts``'s ``"RapmUtils - buildPlayerContext"``
jest test (``src/utils/stats/__tests__/RapmUtils.test.ts:476-525``) call-for-call:

1. Builds ``lineupReport`` -- the 3 vendored lineup buckets
   (``sampleLineupStatsResponse``) each run through ``insertOldValues``, same
   recipe as ``tests/mbb/test_mbb_lineup_stats.py``'s ``_build_lineup_report``.
2. Builds two synthetic "dummy" lineups off a deep copy of
   ``lineupReport["lineups"][0]``, renaming one or two players' ``id`` (the
   RAPM-relevant identity field) and overriding ``off_poss``/``def_poss``.
   See :func:`_rename_player_id`'s docstring for why this is a *direct*
   ``id``-field mutation rather than a literal port of the upstream
   ``JSON.parse(JSON.stringify(...).replace(...))`` string-substitution hack.
3. Calls ``lineup_to_team_report`` once on the combined 5-lineup set, then
   ``build_player_context`` twice (``removal_pct`` in ``[0.0, 0.2]``, sharing
   the same ``players``/``lineups`` references across both calls, matching
   the upstream ``.forEach`` loop), asserting:
   - the full ``RapmPlayerContext`` (minus ``filtered_lineups``/``team_info``,
     per the upstream ``_.omit``) against ``rapm_utils_snap.json``'s 2 entries,
     via exact (non-approximate) equality -- both sides are deterministic
     recomputations of the same arithmetic, so any difference is real.
   - the 2 inline assertions: ``filtered_lineups("off")`` has length 5, and
     ``team_info["off_poss"]["value"] == 959`` (both hold for *both*
     thresholds -- the upstream comment notes "filtering now v rare").

Builds ``playersInfoByKey`` per the README's documented recipe: the vendored
``samplePlayerStatsResponse`` baseline-bucket player list (2 entries), keyed
by ``key``, with a hand-rolled ``off_adj_rtg``/``def_adj_rtg`` index-based
overlay -- **not** a call to ``mbb_ratings.build_productivity`` (see Task
3.1's report + ``mbb_rapm.py``'s own module docstring "RAPM prior source"
note for why this test doesn't exercise that production wiring).

**Task 3.3** extends this with ``RapmUtils.test.ts``'s ``"RapmUtils -
calcPlayerWeights"`` (``:527-573``) and ``"RapmUtils - calcLineupOutputs"``
(``:575-629``) blocks -- both build their own fresh ``lineup_report``/
``build_player_context`` off the *base* 3-lineup ``lineupReport`` (not
``lineupReportWithExtra``), per the classification map, and both use
exact (non-approximate) equality against hand-transcribed literal arrays
(no ``toMatchSnapshot`` in either block).

**Task 3.4** adds the core ridge-regression solve
(:func:`~sportsdataverse.mbb.mbb_rapm.slow_regression`,
:func:`~sportsdataverse.mbb.mbb_rapm.calculate_rapm`,
:func:`~sportsdataverse.mbb.mbb_rapm.calc_slow_pseudo_inverse`,
:func:`~sportsdataverse.mbb.mbb_rapm.calculate_predicted_out`,
:func:`~sportsdataverse.mbb.mbb_rapm.calculate_residual_error`,
:func:`~sportsdataverse.mbb.mbb_rapm.calculate_sd_rapm`). Unlike blocks 1-3,
these functions have **no dedicated jest ``test()`` block of their own** --
``RapmUtils.test.ts``'s only oracle for this layer is the
``"pickRidgeRegression"`` block (classification map item 4), which exercises
them indirectly through the full adaptive-lambda loop (Task 3.5). This
module instead unit-tests them against an independent, hand-computed
closed-form ridge-regression micro-case (2 players, 3 lineups) -- see the
``_MICRO_*`` module constants below.

**Task 3.5** adds ``RapmUtils.test.ts``'s ``"RapmUtils - pickRidgeRegression"``
block (``:631-772``, classification map item 4 -- "the single strongest
oracle gate in the file"). Uses ``semiRealRapmResults.testOffWeights``/
``testDefWeights``/``testContext`` (hand-transcribed below, per the
classification map's replay recipe -- these are NOT vendorable, they embed
``StatModels.emptyIndiv()`` calls, an object spread, and an arrow-function
closure) directly as pre-computed weight matrices; ``ctx.filteredLineups``
(-> the vendored ``reducedFilteredLineups``, ``rapm_utils_inputs.json``) is
still exercised transitively via :func:`~sportsdataverse.mbb.mbb_rapm.pick_ridge_regression`'s
internal :func:`~sportsdataverse.mbb.mbb_rapm.calc_lineup_outputs` call.
Parametrized over ``luckAdjusted`` in ``[True, False]`` (both iterations
assert identical numeric literals -- passing ``"old_value"`` vs ``"value"``
doesn't crash/diverge on this fixture, which has no ``old_value`` fields at
all), replaying the deep-equality adaptive-weight assertions
(``off_results1 == off_results``, ``off_results2 != off_results``,
``def_results1 == def_results``, ``def_results2 == def_results`` -- the def
side is invariant because ``testContext.priorInfo.playersStrong`` entries
carry only ``off_adj_ppp``, no ``def_adj_ppp``) plus every hand-transcribed
``.toFixed`` literal (``playerPossPcts``, the 3-iteration ``prevAttempts``,
``ridgeLambda == "1.536"``, and the ``rapmAdjPpp``/``rapmRawAdjPpp`` arrays
for both sides).

See ``tests/fixtures/hoop_explorer/README.md``'s classification map for the
full accounting of ``RapmUtils.test.ts``'s 7 ``test()`` blocks -- this module
covers blocks 1-4 (``buildPlayerContext``/``calcPlayerWeights``/
``calcLineupOutputs``/``pickRidgeRegression``) plus a from-scratch micro-case
for Task 3.4's solve primitives; blocks 5-7 (``injectRapmIntoPlayers``
onward) belong to Task 3.6.
"""

from __future__ import annotations

import copy

import numpy as np
import pytest

from sportsdataverse.mbb.mbb_lineup_stats import lineup_to_team_report
from sportsdataverse.mbb.mbb_rapm import (
    DEFAULT_RAPM_CONFIG,
    build_player_context,
    build_weak_prior_from_rapm,
    calc_lineup_outputs,
    calc_player_weights,
    calc_slow_pseudo_inverse,
    calculate_predicted_out,
    calculate_rapm,
    calculate_residual_error,
    calculate_sd_rapm,
    pick_ridge_regression,
    slow_regression,
)
from tests.mbb._hoop_explorer_replay import (
    first_lineup_list,
    insert_old_values,
    load_inputs,
    load_rapm_inputs,
    load_rapm_snap,
    load_rating_inputs,
)

# Task 3.4 hand-computed micro-case (2 players, 3 lineups) shared by the
# solve-layer tests below -- an independent closed-form ridge regression,
# NOT derived from any vendored jest fixture (RapmUtils.test.ts's own
# pickRidgeRegression oracle uses pre-computed weight matrices belonging to
# Task 3.5). X = [[1,0],[0,1],[1,1]] (lineup 3 has both players), ridge
# lambda = 1.0. Closed form: XtX = [[2,1],[1,2]], bottom = XtX + I =
# [[3,1],[1,3]], det = 8, bottom^-1 = 1/8 * [[3,-1],[-1,3]] =
# [[0.375,-0.125],[-0.125,0.375]]; solver = bottom^-1 @ X.T. Cross-checked
# against an independent numpy computation (not this module's own functions)
# -- see Task 3.4's report for the full derivation.
_MICRO_X = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
_MICRO_LAMBDA = 1.0
_MICRO_CTX = {"num_players": 2}
_MICRO_Y = [1.0, 2.0, 3.0]
_MICRO_SOLVER = [[0.375, -0.125, 0.25], [-0.125, 0.375, 0.25]]
_MICRO_PARAMS = [0.875, 1.375]
_MICRO_PARAM_ERRS = [0.6123724356957945, 0.6123724356957945]
_MICRO_PREDICTED = [0.875, 1.375, 2.25]
_MICRO_ERR_SQ = 0.96875
_MICRO_SD_RAPM = [0.8706824010355317, 0.8706824010355317]


@pytest.fixture(scope="module")
def snap() -> dict:
    return load_rapm_snap()


@pytest.fixture(scope="module")
def inputs() -> dict:
    return load_inputs()


@pytest.fixture(scope="module")
def rating_inputs() -> dict:
    return load_rating_inputs()


def _build_lineup_report(inputs: dict) -> dict:
    """Replays the jest ``lineupReport`` const (``RapmUtils.test.ts:454-460``):
    the 3 vendored lineup buckets, each run through ``insertOldValues``, plus
    the hardcoded scalar companions. Identical recipe to
    ``test_mbb_lineup_stats.py``'s ``_build_lineup_report`` (kept as a local
    copy -- test modules don't share private helpers across files).
    """
    lineups = [insert_old_values(copy.deepcopy(lineup)) for lineup in first_lineup_list(inputs)]
    return {"lineups": lineups, "avgOff": 100.0, "error_code": "test"}


def _players_info_by_key(rating_inputs: dict) -> dict:
    """Replays the jest ``playersInfoByKey`` const (``RapmUtils.test.ts:462-474``):
    the vendored ``samplePlayerStatsResponse`` baseline-bucket player list (2
    entries: "Cowan, Anthony" / "Wiggins, Aaron"), overlaid with a hand-rolled
    ``off_adj_rtg``/``def_adj_rtg`` per the index-based formula
    (``5.0 - 0.5*ii`` / ``-5.0 + ii*0.5``), keyed by each player's ``key`` field.
    """
    buckets = rating_inputs["samplePlayerStatsResponse"]["responses"][0]["aggregations"]["tri_filter"]["buckets"][
        "baseline"
    ]["player"]["buckets"]
    out: dict = {}
    for ii, p in enumerate(buckets):
        entry = copy.deepcopy(p)
        entry["off_adj_rtg"] = {"value": 5.0 - 0.5 * ii}
        entry["def_adj_rtg"] = {"value": -5.0 + ii * 0.5}
        out[entry["key"]] = entry
    return out


def _rename_player_id(lineup: dict, old_id: str, new_id: str) -> None:
    """Rename a player's ``id`` (the RAPM-relevant identity field) in place,
    within ``lineup["players_array"]["hits"]["hits"][0]["_source"]["players"]``.

    The upstream jest test instead does
    ``JSON.parse(JSON.stringify(lineup).replace(oldCode, newCode).replace(oldName, newName))``
    (``RapmUtils.test.ts:479-494``) -- but JS ``String.prototype.replace(str,
    str)`` (no ``/g`` flag) replaces only the *first* occurrence in the whole
    serialized document, not every occurrence. Per the real property order in
    the upstream ``sampleLineupStatsResponse.ts`` (``"key"`` precedes
    ``"players_array"``), the *code*-shaped replace targets (e.g. ``"JaSmith"``,
    which also appears as a substring of the lineup's ``"key"``, e.g.
    ``"..._ErAyala_JaSmith"``) actually get consumed by the ``key`` field
    first and leave ``players_array...code`` untouched -- only the
    *full-name* targets (e.g. ``"Smith, Jalen"``), which are singleton
    substrings appearing solely in ``players_array...id``, are reliably
    renamed by the upstream hack regardless of ordering.

    Since ``_get_player_set``/``build_player_context`` key exclusively on
    ``id`` (never ``code`` or the lineup's ``key`` string), a direct
    ``id``-only rename produces an identical practical outcome to the
    upstream hack for every value this test asserts on -- and, unlike a
    literal string-replace port, doesn't depend on reproducing upstream's
    exact JSON property order (our vendored fixture JSON is alphabetically
    re-sorted during vendoring and can't reproduce it).
    """
    players = lineup["players_array"]["hits"]["hits"][0]["_source"]["players"]
    for p in players:
        if p["id"] == old_id:
            p["id"] = new_id


def _dummy_lineup(base_lineup: dict, renames: list[tuple[str, str]], poss: float) -> dict:
    """Deep-copies ``base_lineup``, applies ``renames`` via
    :func:`_rename_player_id`, and overrides ``off_poss``/``def_poss`` to
    ``poss`` (matching ``dummyLineup1``/``dummyLineup2``'s own
    ``off_poss = def_poss`` overrides, ``RapmUtils.test.ts:486-487,493-494``).
    """
    lineup = copy.deepcopy(base_lineup)
    for old_id, new_id in renames:
        _rename_player_id(lineup, old_id, new_id)
    lineup["off_poss"] = {"value": poss}
    lineup["def_poss"] = {"value": poss}
    return lineup


def _to_camel_context(result: dict) -> dict:
    """Translate a :class:`~sportsdataverse.mbb.mbb_rapm.RapmPlayerContext`'s
    snake_case field names back to the exact camelCase names the vendored
    jest snapshot uses, so the two can be diff-compared directly.

    Only the ``RapmPlayerContext``/``RapmPriorInfo``/``RapmConfig`` *struct*
    field names need translating -- per ``mbb_rapm.py``'s module docstring,
    the *content* dicts they wrap (``players_strong``/``players_weak``
    entries, the ``removed_players`` tuple's embedded stat doc) already use
    the same (upstream-native snake_case) field names in both languages, so
    nothing inside those needs touching.
    """
    prior = result["prior_info"]
    config = result["config"]
    return {
        "unbiasWeight": result["unbias_weight"],
        "removedPlayers": result["removed_players"],
        "playerToCol": result["player_to_col"],
        "colToPlayer": result["col_to_player"],
        "avgEfficiency": result["avg_efficiency"],
        "numPlayers": result["num_players"],
        "numOffLineups": result["num_off_lineups"],
        "numDefLineups": result["num_def_lineups"],
        "offLineupPoss": result["off_lineup_poss"],
        "defLineupPoss": result["def_lineup_poss"],
        "priorInfo": {
            "strongWeight": prior["strong_weight"],
            "noWeakPrior": prior["no_weak_prior"],
            "useRecursiveWeakPrior": prior["use_recursive_weak_prior"],
            "includeStrong": prior["include_strong"],
            "playersStrong": prior["players_strong"],
            "playersWeak": prior["players_weak"],
            "basis": prior["basis"],
            "keyUsed": prior["key_used"],
        },
        "config": {
            "priorMode": config["prior_mode"],
            "removalPct": config["removal_pct"],
            "fixedRegression": config["fixed_regression"],
        },
    }


@pytest.mark.parametrize("i,threshold", [(1, 0.0), (2, 0.2)])
def test_build_player_context_matches_snapshot(
    inputs: dict, rating_inputs: dict, snap: dict, i: int, threshold: float
) -> None:
    """Full replay of ``RapmUtils.test.ts:476-525``'s ``[0.0,
    0.2].forEach(threshold => ...)`` loop, one parametrized case per
    iteration (both share the underlying ``lineupReportWithExtra``/
    ``onOffReport.players`` construction, computed fresh per test id since
    ``inputs``/``rating_inputs`` are the only module-scoped fixtures).
    """
    lineup_report = _build_lineup_report(inputs)
    base = lineup_report["lineups"][0]
    dummy_lineup1 = _dummy_lineup(
        base,
        [("Smith, Jalen", "Data, Dummy"), ("Ayala, Eric", "Player, Other")],
        50,
    )
    dummy_lineup2 = _dummy_lineup(base, [("Ayala, Eric", "Player, Other")], 100)
    lineup_report_with_extra = {
        "lineups": lineup_report["lineups"] + [dummy_lineup1, dummy_lineup2],
        "avgOff": lineup_report["avgOff"],
        "error_code": lineup_report["error_code"],
    }

    on_off_report = lineup_to_team_report(lineup_report_with_extra)
    players_info_by_key = _players_info_by_key(rating_inputs)

    results = build_player_context(
        on_off_report.get("players") or [],
        lineup_report_with_extra.get("lineups") or [],
        players_info_by_key,
        {},
        100.0,
        "value",
        {**DEFAULT_RAPM_CONFIG, "removal_pct": threshold},
    )

    expected = snap[f"RapmUtils RapmUtils - buildPlayerContext {i}"]
    assert _to_camel_context(results) == expected

    # Inline assertions (RapmUtils.test.ts:518-523) -- both thresholds assert
    # the same values ("filtering now v rare").
    assert len(results["filtered_lineups"]("off")) == 5
    assert results["team_info"]["off_poss"]["value"] == 959


def test_default_rapm_config_verbatim() -> None:
    """``defaultRapmConfig`` copied verbatim (``RapmUtils.ts:181-185``)."""
    assert DEFAULT_RAPM_CONFIG == {"prior_mode": -1, "removal_pct": 0.06, "fixed_regression": -1}


def _tidy_matrix(matrix: object, decimals: int) -> list[list[str]]:
    """Replays the test-local ``tidyResults`` helper for both
    ``calcPlayerWeights`` (``RapmUtils.test.ts:546-548``, 3 decimals) and
    ``calcLineupOutputs`` (``:607-611``, 2 decimals) -- both format every
    scalar to a fixed-decimal string; only the decimal count differs.
    """
    return [[f"{v:.{decimals}f}" for v in row] for row in matrix]  # type: ignore[union-attr]


@pytest.mark.parametrize("unbias_weight", [0.0, 2.0])
def test_calc_player_weights(inputs: dict, rating_inputs: dict, unbias_weight: float) -> None:
    """Replay of ``RapmUtils.test.ts:527-573`` (``"RapmUtils - calcPlayerWeights"``).

    Builds ``context`` fresh (via ``build_player_context(removal_pct=0.0)``
    off the *base* ``lineupReport``, not ``lineupReportWithExtra`` -- see
    Task 3.2's report, "Concerns for Tasks 3.3-3.6" item 3) per parametrized
    case, since ``build_player_context`` mutates its ``lineups`` argument in
    place and this module's fixture-building helpers are not shared with
    ``test_build_player_context_matches_snapshot`` above.
    """
    lineup_report = _build_lineup_report(inputs)
    on_off_report = lineup_to_team_report(lineup_report)
    context = build_player_context(
        on_off_report.get("players") or [],
        lineup_report.get("lineups") or [],
        _players_info_by_key(rating_inputs),
        {},
        100.0,
        "value",
        {**DEFAULT_RAPM_CONFIG, "removal_pct": 0.0},
    )
    context["unbias_weight"] = unbias_weight

    results = calc_player_weights(context)

    expected_off = [
        ["0.704", "0.704", "0.704", "0.704", "0.704", "0.000"],
        ["0.511", "0.511", "0.511", "0.511", "0.000", "0.511"],
        ["0.493", "0.493", "0.493", "0.000", "0.493", "0.493"],
        ["2.000", "2.000", "2.000", "1.513", "1.478", "1.009"],  # (extra row if adding unbiasing obs)
    ]
    expected_def = [
        ["0.699", "0.699", "0.699", "0.699", "0.699", "0.000"],
        ["0.518", "0.518", "0.518", "0.518", "0.000", "0.518"],
        ["0.493", "0.493", "0.493", "0.000", "0.493", "0.493"],
        ["2.000", "2.000", "2.000", "1.514", "1.463", "1.023"],  # (extra row if adding unbiasing obs)
    ]
    n_rows = len(expected_off) if unbias_weight != 0 else 3
    assert _tidy_matrix(results[0], 3) == expected_off[:n_rows]
    assert _tidy_matrix(results[1], 3) == expected_def[:n_rows]


@pytest.mark.parametrize("prior_mode", [-1, 0.5])
def test_calc_lineup_outputs(inputs: dict, rating_inputs: dict, prior_mode: float) -> None:
    """Replay of ``RapmUtils.test.ts:575-629`` (``"RapmUtils - calcLineupOutputs"``).

    Both loop values (``-1``/``0.5``) are JS-truthy, so both parametrized
    cases assert the *same* literal array (the classification map's item 3)
    -- this is not a copy/paste bug, it faithfully replays the jest
    ``strongWeight ? X : []`` ternary always taking the ``X`` branch. Also
    replays the ``oldValResults`` variant (``useOldValIfPossible=[False,
    True]``) asserting the identical literal -- a documented, upstream-
    inherited coverage gap (see the module docstring's "Task 3.3 coverage
    gap" note): ``lineupReport``'s lineups were built via ``insertOldValues``,
    which stamps ``old_value = value`` everywhere, so this variant doesn't
    exercise an actual value/old_value divergence, only that the plumbing
    doesn't crash.
    """
    lineup_report = _build_lineup_report(inputs)
    on_off_report = lineup_to_team_report(lineup_report)
    context = build_player_context(
        on_off_report.get("players") or [],
        lineup_report.get("lineups") or [],
        _players_info_by_key(rating_inputs),
        {},
        100.0,
        "value",
        {**DEFAULT_RAPM_CONFIG, "prior_mode": prior_mode, "removal_pct": 0.0},
    )
    assert context["prior_info"]["basis"] == {"off": 0.0, "def": 0.0}

    adjusted_basis_off_eff = 100.0 - 5 * context["prior_info"]["basis"]["off"]
    adjusted_basis_def_eff = 100.0 - 5 * context["prior_info"]["basis"]["def"]
    adaptive_weights = [0.5 for _ in (on_off_report.get("players") or [])]

    expected = [["13.07", "5.84", "7.70"], ["-8.48", "-10.69", "-8.83"]]

    results = calc_lineup_outputs(
        "adj_ppp",
        adjusted_basis_off_eff,
        adjusted_basis_def_eff,
        context,
        adaptive_weights if prior_mode < 0 else None,
    )
    assert _tidy_matrix(results, 2) == expected

    old_val_results = calc_lineup_outputs(
        "adj_ppp",
        adjusted_basis_off_eff,
        adjusted_basis_def_eff,
        context,
        adaptive_weights if prior_mode < 0 else None,
        (False, True),
    )
    assert _tidy_matrix(old_val_results, 2) == expected


# ---------------------------------------------------------------------------
# Task 3.4 -- core ridge-regression solve, hand-computed 2-player/3-lineup
# micro-case (no vendored jest oracle exists for this layer in isolation;
# see the module docstring's "Task 3.4" paragraph).
# ---------------------------------------------------------------------------


def test_slow_regression_matches_closed_form() -> None:
    """``slow_regression`` reproduces the hand-derived ``(XᵀX + λI)⁻¹Xᵀ``
    solver matrix for the shared micro-case."""
    solver = slow_regression(_MICRO_X, _MICRO_LAMBDA, _MICRO_CTX)
    np.testing.assert_allclose(solver, _MICRO_SOLVER, rtol=1e-12)


def test_calculate_rapm_applies_solver_to_outputs() -> None:
    """``calculate_rapm(solver, y)`` recovers the hand-derived per-player
    ridge estimates (``RapmUtils.ts:772`` -- note: no ``ctx`` parameter)."""
    solver = np.array(_MICRO_SOLVER)
    rapm = calculate_rapm(solver, _MICRO_Y)
    np.testing.assert_allclose(rapm, _MICRO_PARAMS, rtol=1e-12)


def test_calc_slow_pseudo_inverse_matches_closed_form() -> None:
    """``calc_slow_pseudo_inverse`` returns ``sqrt(diag((XᵀX + λI)⁻¹))`` for
    the shared micro-case."""
    param_errs = calc_slow_pseudo_inverse(_MICRO_X, _MICRO_LAMBDA, _MICRO_CTX)
    np.testing.assert_allclose(param_errs, _MICRO_PARAM_ERRS, rtol=1e-12)


def test_calculate_predicted_out_and_residual_error() -> None:
    """``calculate_predicted_out`` predicts per-lineup values from the fitted
    RAPM params, and ``calculate_residual_error`` sums their squared
    deviation from the actual outputs (``RapmUtils.ts:1559/1569``)."""
    predicted = calculate_predicted_out(_MICRO_X, _MICRO_PARAMS, _MICRO_CTX)
    np.testing.assert_allclose(predicted, _MICRO_PREDICTED, rtol=1e-12)

    err_sq = calculate_residual_error(_MICRO_Y, predicted, _MICRO_CTX)
    assert err_sq == pytest.approx(_MICRO_ERR_SQ, rel=1e-12)


def test_calculate_residual_error_length_mismatch_raises() -> None:
    """Landmine 7: a length mismatch between ``player_outs``/``regressed_outs``
    raises ``ValueError`` in this numpy port (TS silently produces ``NaN``
    via lodash ``_.zip`` padding) -- not reachable via any real call site,
    both arguments are always index-aligned to the same lineup count."""
    with pytest.raises(ValueError):
        calculate_residual_error([1.0, 2.0], [1.0, 2.0, 3.0], _MICRO_CTX)


def test_calculate_sd_rapm_matches_closed_form() -> None:
    """``calculate_sd_rapm`` composes ``dof_inv`` +
    ``sqrt(sqrt(param_errs) * err_sq * dof_inv)`` for the shared micro-case
    (``RapmUtils.ts:1373-1390``, cites arXiv:1509.09169)."""
    sd_rapm = calculate_sd_rapm(np.array(_MICRO_PARAM_ERRS), _MICRO_ERR_SQ, num_lineups=3, num_players=2)
    np.testing.assert_allclose(sd_rapm, _MICRO_SD_RAPM, rtol=1e-12)


def test_calculate_sd_rapm_zero_dof_raises() -> None:
    """Landmine 8: ``num_lineups == num_players`` raises ``ZeroDivisionError``
    in this Python port (JS float division by zero silently yields
    ``Infinity``) -- matches this module's already-established landmine-2
    convention (unguarded division, Python-raises vs JS-Infinity/NaN)."""
    with pytest.raises(ZeroDivisionError):
        calculate_sd_rapm(np.array(_MICRO_PARAM_ERRS), _MICRO_ERR_SQ, num_lineups=2, num_players=2)


# ---------------------------------------------------------------------------
# Task 3.5 -- pickRidgeRegression (RapmUtils.test.ts:631-772), the "single
# strongest oracle gate in the file" (classification map item 4).
#
# ``semiRealRapmResults`` (testOffWeights/testDefWeights/testContext) is NOT
# vendorable (see the module docstring's "Task 3.5" paragraph) -- hand
# -transcribed verbatim from ``RapmUtils.test.ts:281-436`` below.
# ``reducedFilteredLineups`` (the vendored 31-row lineup array
# ``testContext.filteredLineups`` closes over) IS vendored, in
# ``rapm_utils_inputs.json`` -- see :func:`load_rapm_inputs`.
# ---------------------------------------------------------------------------


def _empty_indiv() -> dict:
    """Replay of jest's ``StatModels.emptyIndiv()`` call sites inside
    ``semiRealRapmResults.testContext.removedPlayers`` (``RapmUtils.test.ts:358-363``).
    """
    return {"key": "empty", "doc_count": 0}


_SEMI_REAL_COL_TO_PLAYER = [
    "Smith, Jalen",
    "Cowan, Anthony",
    "Wiggins, Aaron",
    "Morsell, Darryl",
    "Ayala, Eric",
    "Scott, Donta",
    "Lindo Jr., Ricky",
    "Smith Jr., Serrel",
]

# ``semiRealRapmResults.testOffWeights`` (``RapmUtils.test.ts:283-316``) --
# 31 filtered-lineup rows + 1 trailing "unbiasing observation" row (since
# ``testContext.unbiasWeight == 2``, this row is what
# ``pick_ridge_regression``'s ``build_usage_vector`` reads back out as
# ``player_poss_pcts``, see that function's own docstring).
_SEMI_REAL_TEST_OFF_WEIGHTS = [
    [0.542, 0.542, 0.542, 0.542, 0.542, 0, 0, 0],
    [0.3933, 0.3933, 0.3933, 0.3933, 0, 0.3933, 0, 0],
    [0.3818, 0.3818, 0.3818, 0, 0.3818, 0.3818, 0, 0],
    [0.3599, 0.3599, 0, 0.3599, 0.3599, 0.3599, 0, 0],
    [0.2124, 0.2124, 0.2124, 0, 0.2124, 0, 0.2124, 0],
    [0.1804, 0.1804, 0.1804, 0.1804, 0, 0, 0.1804, 0],
    [0.1804, 0, 0.1804, 0.1804, 0.1804, 0.1804, 0, 0],
    [0.1677, 0.1677, 0.1677, 0, 0, 0.1677, 0, 0.1677],
    [0.1387, 0.1387, 0.1387, 0, 0, 0, 0.1387, 0.1387],
    [0.1216, 0.1216, 0.1216, 0.1216, 0, 0, 0, 0.1216],
    [0.1216, 0.1216, 0, 0.1216, 0.1216, 0, 0.1216, 0],
    [0.1017, 0, 0.1017, 0.1017, 0.1017, 0, 0.1017, 0],
    [0.1017, 0, 0.1017, 0.1017, 0.1017, 0, 0, 0.1017],
    [0.098, 0.098, 0, 0.098, 0, 0.098, 0, 0.098],
    [0.0942, 0.0942, 0, 0.0942, 0.0942, 0, 0, 0.0942],
    [0, 0.086, 0.086, 0.086, 0, 0.086, 0.086, 0],
    [0, 0.0816, 0.0816, 0.0816, 0.0816, 0.0816, 0, 0],
    [0, 0.0769, 0.0769, 0, 0.0769, 0.0769, 0.0769, 0],
    [0.0719, 0, 0.0719, 0, 0.0719, 0, 0.0719, 0.0719],
    [0.0544, 0, 0.0544, 0, 0.0544, 0.0544, 0, 0.0544],
    [0, 0, 0.0544, 0.0544, 0.0544, 0.0544, 0, 0.0544],
    [0.0544, 0, 0, 0.0544, 0.0544, 0.0544, 0, 0.0544],
    [0.0544, 0, 0, 0, 0.0544, 0.0544, 0.0544, 0.0544],
    [0, 0.0471, 0.0471, 0.0471, 0.0471, 0, 0.0471, 0],
    [0.0471, 0.0471, 0, 0.0471, 0, 0, 0.0471, 0.0471],
    [0.0471, 0.0471, 0, 0, 0.0471, 0.0471, 0, 0.0471],
    [0.0471, 0.0471, 0.0471, 0, 0.0471, 0, 0, 0.0471],
    [0.0384, 0, 0, 0.0384, 0.0384, 0, 0.0384, 0.0384],
    [0, 0.0272, 0.0272, 0.0272, 0, 0, 0.0272, 0.0272],
    [0, 0.0272, 0, 0.0272, 0.0272, 0.0272, 0, 0.0272],
    [0.0272, 0.0272, 0, 0, 0.0272, 0, 0.0272, 0.0272],
    [1.9467, 1.8564, 1.6476, 1.4789, 1.4611, 1.0703, 0.3019, 0.2368],
]

# ``semiRealRapmResults.testDefWeights`` (``RapmUtils.test.ts:318-351``).
_SEMI_REAL_TEST_DEF_WEIGHTS = [
    [0.5342, 0.5342, 0.5342, 0.5342, 0.5342, 0, 0, 0],
    [0.3936, 0.3936, 0.3936, 0.3936, 0, 0.3936, 0, 0],
    [0.3782, 0.3782, 0.3782, 0, 0.3782, 0.3782, 0, 0],
    [0.3652, 0.3652, 0, 0.3652, 0.3652, 0.3652, 0, 0],
    [0.2073, 0.2073, 0.2073, 0, 0.2073, 0, 0.2073, 0],
    [0.1846, 0.1846, 0.1846, 0.1846, 0, 0, 0.1846, 0],
    [0.1806, 0, 0.1806, 0.1806, 0.1806, 0.1806, 0, 0],
    [0.1656, 0.1656, 0.1656, 0, 0, 0.1656, 0, 0.1656],
    [0.1388, 0.1388, 0.1388, 0, 0, 0, 0.1388, 0.1388],
    [0.1155, 0.1155, 0.1155, 0.1155, 0, 0, 0, 0.1155],
    [0.1186, 0.1186, 0, 0.1186, 0.1186, 0, 0.1186, 0],
    [0.1089, 0, 0.1089, 0.1089, 0.1089, 0, 0.1089, 0],
    [0.1155, 0, 0.1155, 0.1155, 0.1155, 0, 0, 0.1155],
    [0.1018, 0.1018, 0, 0.1018, 0, 0.1018, 0, 0.1018],
    [0.0903, 0.0903, 0, 0.0903, 0.0903, 0, 0, 0.0903],
    [0, 0.086, 0.086, 0.086, 0, 0.086, 0.086, 0],
    [0, 0.077, 0.077, 0.077, 0.077, 0.077, 0, 0],
    [0, 0.072, 0.072, 0, 0.072, 0.072, 0.072, 0],
    [0.086, 0, 0.086, 0, 0.086, 0, 0.086, 0.086],
    [0.0608, 0, 0.0608, 0, 0.0608, 0.0608, 0, 0.0608],
    [0, 0, 0.0608, 0.0608, 0.0608, 0.0608, 0, 0.0608],
    [0.0608, 0, 0, 0.0608, 0.0608, 0.0608, 0, 0.0608],
    [0.0544, 0, 0, 0, 0.0544, 0.0544, 0.0544, 0.0544],
    [0, 0.0544, 0.0544, 0.0544, 0.0544, 0, 0.0544, 0],
    [0.0544, 0.0544, 0, 0.0544, 0, 0, 0.0544, 0.0544],
    [0.0471, 0.0471, 0, 0, 0.0471, 0.0471, 0, 0.0471],
    [0.0544, 0.0544, 0.0544, 0, 0.0544, 0, 0, 0.0544],
    [0.0385, 0, 0, 0.0385, 0.0385, 0, 0.0385, 0.0385],
    [0, 0.0272, 0.0272, 0.0272, 0, 0, 0.0272, 0.0272],
    [0, 0.0272, 0, 0.0272, 0.0272, 0.0272, 0, 0.0272],
    [0.0385, 0.0385, 0, 0, 0.0385, 0, 0.0385, 0.0385],
    [1.9466, 1.8383, 1.6367, 1.4825, 1.4588, 1.0748, 0.3098, 0.252],
]


def _semi_real_test_context(reduced_filtered_lineups: list[dict]) -> dict:
    """Replay of ``semiRealRapmResults.testContext`` (``RapmUtils.test.ts:355-436``).

    ``filtered_lineups`` closes over ``reduced_filtered_lineups`` regardless
    of the ``"off"``/``"def"`` prefix passed in, matching the jest arrow
    function ``(prefix) => reducedFilteredLineups`` verbatim.
    """
    return {
        "unbias_weight": 2,
        "removed_players": {
            "Mitchell, Makhel": [0.21, 0.01, _empty_indiv()],
            "Tomaic, Joshua": [0.149, 0.02, _empty_indiv()],
            "Marial, Chol": [0.0208, 0.0208, _empty_indiv()],
            "Mona, Reese": [0.042, 0.042, _empty_indiv()],
            "Hart, Hakim": [0.237, 0.0237, _empty_indiv()],
            "Mitchell, Makhi": [0.264, 0.0264, _empty_indiv()],
        },
        "player_to_col": {p: i for i, p in enumerate(_SEMI_REAL_COL_TO_PLAYER)},
        "col_to_player": list(_SEMI_REAL_COL_TO_PLAYER),
        "avg_efficiency": 102.4,
        "num_players": 8,
        "num_off_lineups": 31,
        "num_def_lineups": 31,
        "off_lineup_poss": 1351,
        "def_lineup_poss": 1349,
        "prior_info": {
            "strong_weight": 0.5,
            "no_weak_prior": False,
            "use_recursive_weak_prior": False,
            "include_strong": {},
            "players_strong": [
                {"off_adj_ppp": 5.0},
                {"off_adj_ppp": 4.5},
                {"off_adj_ppp": 4.0},
                {"off_adj_ppp": 3.5},
                {"off_adj_ppp": 3.0},
                {"off_adj_ppp": 2.5},
                {"off_adj_ppp": 2.0},
                {"off_adj_ppp": 2.0},
            ],
            "players_weak": [
                {"off_adj_ppp": 5.0, "def_adj_ppp": -5.0},
                {"off_adj_ppp": 4.5, "def_adj_ppp": -4.5},
                {"off_adj_ppp": 4.0, "def_adj_ppp": -4.0},
                {"off_adj_ppp": 3.5, "def_adj_ppp": -3.5},
                {"off_adj_ppp": 3.0, "def_adj_ppp": -3.0},
                {"off_adj_ppp": 2.5, "def_adj_ppp": -2.5},
                {"off_adj_ppp": 2.0, "def_adj_ppp": -2.0},
                {"off_adj_ppp": 1.5, "def_adj_ppp": -1.5},
            ],
            "key_used": "value",
            "basis": {"off": 0, "def": 0},
        },
        "filtered_lineups": lambda prefix: reduced_filtered_lineups,
        "team_info": {
            "key": "teamInfo",
            "doc_count": 1,
            "off_adj_ppp": {"value": 112.4},
            "def_adj_ppp": {"value": 82.4},
            "off_poss": {"value": 101},
            "def_poss": {"value": 99},
        },
        "config": {"prior_mode": -1, "removal_pct": 0.1, "fixed_regression": -1},
    }


@pytest.fixture(scope="module")
def rapm_inputs() -> dict:
    return load_rapm_inputs()


def test_build_weak_prior_from_rapm() -> None:
    """``buildWeakPriorFromRapm`` (``RapmUtils.ts:410-419``) wraps a flat
    per-player RAPM vector into ``playersWeak``-shaped dicts keyed by
    ``f"{off_or_def}_adj_ppp"``. Not itself exercised by the oracle (see the
    function's own docstring for why -- ``useRecursiveWeakPrior`` is
    ``False`` on ``semiRealRapmResults.testContext``), so covered directly
    here in isolation.
    """
    assert build_weak_prior_from_rapm([5.0, 4.5], "off") == [{"off_adj_ppp": 5.0}, {"off_adj_ppp": 4.5}]
    assert build_weak_prior_from_rapm([-1.0], "def") == [{"def_adj_ppp": -1.0}]


@pytest.mark.parametrize("luck_adjusted", [True, False])
def test_pick_ridge_regression_matches_oracle(rapm_inputs: dict, luck_adjusted: bool) -> None:
    """Replay of ``RapmUtils.test.ts:631-772`` (``"RapmUtils - pickRidgeRegression"``),
    one parametrized case per ``luckAdjusted`` loop iteration (both assert
    identical numeric literals -- this fixture has no ``old_value`` fields at
    all, so the ``value``/``old_value`` key-choice doesn't crash or diverge,
    only exercises the plumbing).
    """
    off_weights = np.array(_SEMI_REAL_TEST_OFF_WEIGHTS)
    def_weights = np.array(_SEMI_REAL_TEST_DEF_WEIGHTS)
    reduced = rapm_inputs["reducedFilteredLineups"]
    context = _semi_real_test_context(reduced)

    agg_value_key = "value" if luck_adjusted else "old_value"
    lineup_value_keys = ("old_value", "old_value") if luck_adjusted else ("value", "value")

    off_results, def_results = pick_ridge_regression(
        off_weights, def_weights, context, None, False, agg_value_key, lineup_value_keys
    )

    # Deep-equality adaptive-weight assertions (RapmUtils.test.ts:651-682).
    # `filtered_lineups` (a lambda) survives `copy.deepcopy` unchanged --
    # the stdlib `copy` module treats function objects as atomic.
    context1 = copy.deepcopy(context)
    context1["prior_info"]["strong_weight"] = -1
    adaptive_weights1 = [0.5] * len(context["col_to_player"])
    off_results1, def_results1 = pick_ridge_regression(
        off_weights, def_weights, context1, adaptive_weights1, False, agg_value_key, lineup_value_keys
    )

    context2 = copy.deepcopy(context)
    context2["prior_info"]["strong_weight"] = -1
    adaptive_weights2 = [0.2] * len(context["col_to_player"])
    off_results2, def_results2 = pick_ridge_regression(
        off_weights, def_weights, context2, adaptive_weights2, False, agg_value_key, lineup_value_keys
    )

    assert off_results1 == off_results  # (same effective adaptive weight, 0.5 == fixed strong_weight)
    assert off_results2 != off_results  # (0.2 diverges)
    assert def_results1 == def_results  # (def side: adaptive weights not used, see docstring)
    assert def_results2 == def_results

    # Hand-checked literals (RapmUtils.test.ts:686-770).
    assert [f"{v:.2f}" for v in off_results["player_poss_pcts"]] == [
        "0.97",
        "0.93",
        "0.82",
        "0.74",
        "0.73",
        "0.54",
        "0.15",
        "0.12",
    ]
    assert [f"{v:.2f}" for v in def_results["player_poss_pcts"]] == [
        "0.97",
        "0.92",
        "0.82",
        "0.74",
        "0.73",
        "0.54",
        "0.15",
        "0.13",
    ]

    off_prev = [{"l": f"{a['ridge_lambda']:.2f}", "ex": f"{a['results'][0]:.2f}"} for a in off_results["prev_attempts"]]
    assert off_prev == [
        {"l": "1.10", "ex": "2.83"},
        {"l": "1.32", "ex": "2.87"},
        {"l": "1.54", "ex": "2.89"},
    ]
    assert f"{off_results['ridge_lambda']:.3f}" == "1.536"
    assert [f"{v:.2f}" for v in off_results["rapm_adj_ppp"][:3]] == ["2.89", "2.79", "2.67"]
    assert [f"{v:.2f}" for v in off_results["rapm_raw_adj_ppp"][:3]] == ["4.81", "4.71", "4.59"]

    def_prev = [{"l": f"{a['ridge_lambda']:.2f}", "ex": f"{a['results'][0]:.2f}"} for a in def_results["prev_attempts"]]
    assert def_prev == [
        {"l": "1.10", "ex": "-5.86"},
        {"l": "1.32", "ex": "-5.73"},
        {"l": "1.54", "ex": "-5.64"},
    ]
    assert f"{def_results['ridge_lambda']:.3f}" == "1.536"
    assert [f"{v:.2f}" for v in def_results["rapm_adj_ppp"][:3]] == ["-5.64", "-4.22", "-4.94"]
    assert [f"{v:.2f}" for v in def_results["rapm_raw_adj_ppp"][:3]] == ["-5.06", "-3.70", "-4.48"]
