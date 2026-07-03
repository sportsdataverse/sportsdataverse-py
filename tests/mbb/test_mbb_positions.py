"""Oracle-replay tests for ``sportsdataverse.mbb.mbb_positions`` (Tasks 4.2-4.3).

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

**In scope for Task 4.3 (the decision tree + roster reconciliation) --
consumed below:**

* ``PositionUtils - buildPosition`` (jest ``:53-172``) -- all 19 hand-checked
  ``testCases`` rows, the too-few-possessions fallback (17 of the 19 rows --
  excludes the row with ``confsNoHeight`` and the roster-override row, same
  as the jest ``if (!caseObj.roster && !caseObj.confsNoHeight)`` guard), and
  the 2 post-loop assertions (absolute-override plumbing +
  ``idToPosition`` lookups).
* ``PositionUtils - usingRosterPos`` (jest ``:207-225``) -- all 10 rows.

**Out of scope (deferred to Task 4.4, NOT tested here) -- accounted for so
none are silently skipped:**

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

import pytest

from sportsdataverse.mbb.mbb_positions import (
    AVERAGE_SCORES_BY_POS,
    ID_TO_POSITION,
    TRAD_POS_LIST,
    build_position,
    build_position_confidences,
    incorporate_height,
    regress_shot_quality,
    using_roster_pos,
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


# --- Test 4: PositionUtils - buildPosition (jest :53-172) ------------------

#: Verbatim transcription of the jest ``testCases`` array (``PositionUtils.test.ts:56-142``).
#: `confs`/`confs_no_height` are tradPosList-ordered `[pg, sg, sf, pf, c]`; `extra`
#: keys become `{field: {"value": v}}` player stats (`assist`->`off_assist`,
#: `3pr`->`off_3pr`, `poss`->`off_team_poss`, `usage`->`off_usage`); `roster` is
#: the optional `{"pos": ...}` override-plumbing case. This const fails json5
#: vendoring (backtick template-literal `diag` strings) -- see
#: ``tests/fixtures/hoop_explorer/README.md`` PositionUtils section item 4.
BUILD_POSITION_CASES: list[dict[str, Any]] = [
    {
        "confs": [0.9, 0.1, 0, 0, 0],
        "extra": {"assist": 0.10, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "PG",
        "fallback_pos": "G?",
        "diag": "(P[PG] >= 85%)",
        "name": "Pure PG",
    },
    {
        "confs": [0.9, 0.1, 0, 0, 0],
        "extra": {"assist": 0.05, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "WG",
        "fallback_pos": "G?",
        "diag": "(PG:)(P[PG] >= 85%) BUT (AST%[5.0] < 9%)",
    },
    {
        "confs": [0.6, 0.4, 0, 0, 0],
        "extra": {"assist": 0.10, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "s-PG",
        "fallback_pos": "G?",
        "diag": "(P[PG] >= 50%)",
        "name": "Scoring PG",
    },
    {
        "confs": [0.6, 0.4, 0, 0, 0],
        "confs_no_height": [0.9, 0.1, 0, 0, 0],
        "extra": {"assist": 0.10, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "PG",
        "fallback_pos": "G?",
        "diag": "(P[PG] >= 85%) ('PG' vs 's-PG', ignore height)",
        "name": "Pure PG",
    },
    {
        "confs": [0.6, 0.4, 0, 0, 0],
        "extra": {"assist": 0.05, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "WG",
        "fallback_pos": "G?",
        "diag": "(pG:)(P[PG] >= 50%) BUT (AST%[5.0] < 9%)",
    },
    {
        "confs": [0.4, 0.3, 0.2, 0.1, 0],
        "extra": {"assist": 0.10, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "CG",
        "fallback_pos": "G?",
        "diag": "(Max[P] == PG)",
        "name": "Combo Guard",
    },
    {
        "confs": [0.4, 0.3, 0.2, 0.1, 0],
        "extra": {"assist": 0.05, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "WG",
        "fallback_pos": "G?",
        "diag": "(CG:)(Max[P] == PG) BUT (AST%[5.0] < 9%)",
        "name": "Wing Guard",
    },
    {
        "confs": [0.2, 0.6, 0.1, 0.0, 0.1],
        "extra": {"assist": 0.10, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "CG",
        "fallback_pos": "G?",
        "diag": "(Max[P] == SG) AND (P[PG] >= P[SF] + P[PF] + P[C])",
    },
    {
        "confs": [0.2, 0.6, 0.1, 0.0, 0.1],
        "extra": {"assist": 0.05, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "WG",
        "fallback_pos": "G?",
        "diag": "(CG:)(Max[P] == SG) AND (P[PG] >= P[SF] + P[PF] + P[C]) BUT (AST%[5.0] < 9%)",
    },
    {
        "confs": [0.1, 0.6, 0.1, 0.1, 0.1],
        "extra": {"assist": 0.10, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "WG",
        "fallback_pos": "G?",
        "diag": "(Max[P] == SG) AND (P[PG] < P[SF] + P[PF] + P[C])",
    },
    {
        "confs": [0.2, 0.2, 0.3, 0.2, 0.1],
        "extra": {"assist": 0.10, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "WG",
        "fallback_pos": "G?",
        "diag": "(Max[P] == SF) AND (P[PG] + P[SG] >= P[PF] + P[C])",
    },
    {
        "confs": [0.2, 0.1, 0.3, 0.2, 0.2],
        "extra": {"assist": 0.10, "3pr": 0.20, "poss": 1000, "usage": 0.20},
        "pos": "WF",
        "fallback_pos": "F/C?",
        "diag": "(Max[P] == SF) AND (P[PG] + P[SG] < P[PF] + P[C])",
        "name": "Wing Forward",
    },
    {
        "confs": [0.0, 0.1, 0.1, 0.6, 0.2],
        "extra": {"assist": 0.10, "3pr": 0.25, "poss": 1000, "usage": 0.20},
        "pos": "S-PF",
        "fallback_pos": "F/C?",
        "diag": "(Max[P] == PF) AND (P[PG] + P[SG] + P[SF] >= P[C])",
        "name": "Stretch PF",
    },
    {
        "confs": [0.0, 0.1, 0.1, 0.6, 0.2],
        "extra": {"assist": 0.10, "3pr": 0.15, "poss": 1000, "usage": 0.20},
        "pos": "PF/C",
        "fallback_pos": "F/C?",
        "diag": "(S4:)(Max[P] == PF) AND (P[PG] + P[SG] + P[SF] >= P[C]) BUT 3PR%[15.0] < 20%",
    },
    {
        "confs": [0.0, 0.0, 0.1, 0.9, 0.0],
        "extra": {"assist": 0.10, "3pr": 0.25, "poss": 1000, "usage": 0.20},
        "pos": "PF/C",
        "fallback_pos": "F/C?",
        "diag": "(P[PF] >= 85%)",
        "name": "Power Forward/Center",
    },
    {
        "confs": [0.0, 0.0, 0.05, 0.8, 0.15],
        "extra": {"assist": 0.10, "3pr": 0.25, "poss": 1000, "usage": 0.20},
        "pos": "PF/C",
        "fallback_pos": "F/C?",
        "diag": "(Max[P] == C) OR ((Max[P] == PF) AND (P[PG] + P[SG] + P[SF] < P[C]))",
    },
    {
        "confs": [0.0, 0.0, 0.0, 0.2, 0.8],
        "extra": {"assist": 0.10, "3pr": 0.25, "poss": 1000, "usage": 0.20},
        "pos": "PF/C",
        "fallback_pos": "F/C?",
        "diag": "(Max[P] == C) OR ((Max[P] == PF) AND (P[PG] + P[SG] + P[SF] < P[C]))",
    },
    {
        "confs": [0.0, 0.0, 0.0, 0.1, 0.9],
        "extra": {"assist": 0.10, "3pr": 0.25, "poss": 1000, "usage": 0.20},
        "pos": "C",
        "fallback_pos": "F/C?",
        "diag": "(P[C] >= 85%)",
        "name": "Center",
    },
    {
        "confs": [0.0, 0.0, 0.0, 0.1, 0.9],
        "roster": {"pos": "G"},
        "extra": {"assist": 0.10, "3pr": 0.25, "poss": 1000, "usage": 0.20},
        "pos": "WF",
        "fallback_pos": "G?",
        "diag": "Roster info says 'G', stats say [C] - compromize at 'WF'. From stats: (P[C] >= 85%)",
        "name": "Wing Forward",
    },
]

_SAMPLE_TEAM_SEASON_1 = "Men_Boston College_2019/20"
_SAMPLE_TEAM_SEASON_2 = "RandomLookup"

#: `extra` key -> player stat field name (``PositionUtils.test.ts:148`` `_.mapValues`).
_EXTRA_FIELD_MAP = {"assist": "off_assist", "3pr": "off_3pr", "poss": "off_team_poss", "usage": "off_usage"}


def _case_player(case: dict[str, Any]) -> dict[str, Any]:
    """Per-case `player` replay (jest ``:148``): extra stats wrapped as
    ``{"value": v}``, plus the optional `roster` sub-dict."""
    player: dict[str, Any] = {_EXTRA_FIELD_MAP[k]: {"value": v} for k, v in case["extra"].items()}
    if "roster" in case:
        player["roster"] = case["roster"]
    return player


@pytest.mark.parametrize("case", BUILD_POSITION_CASES, ids=range(len(BUILD_POSITION_CASES)))
def test_build_position_cases(case: dict[str, Any]) -> None:
    """All 19 hand-checked ``buildPosition`` rows (jest ``:56-142``)."""
    conf_obj = dict(zip(TRAD_POS_LIST, case["confs"]))
    conf_obj_no_height = dict(zip(TRAD_POS_LIST, case["confs_no_height"])) if "confs_no_height" in case else None
    player = _case_player(case)

    assert build_position(conf_obj, conf_obj_no_height, player, _SAMPLE_TEAM_SEASON_1) == (
        case["pos"],
        case["diag"],
    )
    if "name" in case:
        assert ID_TO_POSITION[case["pos"]] == case["name"]


@pytest.mark.parametrize(
    "case",
    [c for c in BUILD_POSITION_CASES if "roster" not in c and "confs_no_height" not in c],
    ids=[i for i, c in enumerate(BUILD_POSITION_CASES) if "roster" not in c and "confs_no_height" not in c],
)
def test_build_position_too_few_possessions_fallback(case: dict[str, Any]) -> None:
    """Too-few-effective-possessions fallback (jest ``:155-160``) -- 17 of the
    19 rows (excludes the ``confsNoHeight`` row and the roster-override row,
    same as the jest ``if (!caseObj.roster && !caseObj.confsNoHeight)`` guard).
    """
    conf_obj = dict(zip(TRAD_POS_LIST, case["confs"]))
    player_too_few_pos = _case_player(case) | {"off_team_poss": {"value": 100}}

    assert build_position(conf_obj, None, player_too_few_pos, _SAMPLE_TEAM_SEASON_2) == (
        case["fallback_pos"],
        f"Too few used possessions [20.0]=[100]*[20.0]% < [25.0]. "
        f"Would have matched [{case['pos']}] from rule [{case['diag']}]",
    )


def test_build_position_absolute_override() -> None:
    """Absolute-override plumbing check (jest ``:163-168``) -- exercises
    ``ABSOLUTE_POSITION_FIXES``'s ``"Men_Boston College_2019/20"`` ->
    ``"Popovic, Nik"`` -> ``PF/C`` row."""
    conf_obj = dict(zip(TRAD_POS_LIST, BUILD_POSITION_CASES[0]["confs"]))
    player = {
        "key": "Popovic, Nik",
        "off_usage": {"value": 1},
        "off_team_poss": {"value": 200},
        "off_assist": {"value": 0.10},
    }
    assert build_position(conf_obj, None, player, _SAMPLE_TEAM_SEASON_1) == (
        "PF/C",
        "Override from [PG] which matched rule [(P[PG] >= 85%)]",
    )


def test_id_to_position_unknown_lookups() -> None:
    """Lookup-table checks (jest ``:170-171``)."""
    assert ID_TO_POSITION["G?"] == "Unknown - probably Guard"
    assert ID_TO_POSITION["F/C?"] == "Unknown - probably Forward/Center"


# --- Test 6: PositionUtils - usingRosterPos (jest :207-225) ----------------


def test_using_roster_pos() -> None:
    """All 10 rows of the vendored ``testCases`` (this key belongs to
    ``usingRosterPos``, not ``buildPosition`` -- see the README collision
    note in ``tests/fixtures/hoop_explorer/README.md``)."""
    inputs = load_fixture("position_utils_inputs.json")
    for case in inputs["testCases"]:
        expected_pos, info = using_roster_pos(case["stats"], case["roster"])
        assert expected_pos == case["expected"]
        assert (info is not None) == case["hasInfo"]
