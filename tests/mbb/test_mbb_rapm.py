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

See ``tests/fixtures/hoop_explorer/README.md``'s classification map for the
full accounting of ``RapmUtils.test.ts``'s 7 ``test()`` blocks -- this module
covers block 1 only (``buildPlayerContext``); blocks 2-7 (``calcPlayerWeights``
onward) belong to Tasks 3.3-3.6.
"""

from __future__ import annotations

import copy

import pytest

from sportsdataverse.mbb.mbb_lineup_stats import lineup_to_team_report
from sportsdataverse.mbb.mbb_rapm import DEFAULT_RAPM_CONFIG, build_player_context
from tests.mbb._hoop_explorer_replay import (
    first_lineup_list,
    insert_old_values,
    load_inputs,
    load_rapm_snap,
    load_rating_inputs,
)


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
