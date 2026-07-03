"""Shared jest-test-replay helpers for hoop-explorer oracle tests.

Extracted from ``tests/mbb/test_mbb_lineup_stats.py`` (Task 1.2) once a
second test module (Task 1.3's ``lineupToTeamReport`` oracle test) needed
the same replay logic. These are Python re-implementations of test-file
-local helpers in the upstream jest suite
(``cbb-on-off-analyzer/src/utils/stats/__tests__/LineupUtils.test.ts``) --
NOT part of the ``LineupUtils`` production module being ported -- so they
live under ``tests/``, not ``sportsdataverse/``.

See ``tests/fixtures/hoop_explorer/README.md`` for fixture provenance.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/hoop_explorer"

#: Verbatim from ``LuckUtils.affectedFieldSet`` (``LuckUtils.ts:159``) --
#: the jest test file's ``insertOldValues`` local helper stamps
#: ``old_value``/``override`` onto every stat whose key is in this set.
LUCK_AFFECTED_FIELDS = frozenset(
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


def load_fixture(name: str) -> dict:
    """Load any vendored fixture JSON by filename (e.g. ``"rating_utils_snap.json"``).

    Generic counterpart to :func:`load_snap` / :func:`load_inputs` (which
    remain hardcoded to the ``LineupUtils`` pair for backward compatibility
    with Task 1.x tests) -- added in Task 2.2 once a second jest-suite pair
    (``RatingUtils``) needed the same load-and-cache-per-module pattern
    without duplicating this function.
    """
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def load_snap() -> dict:
    """Load the vendored jest snapshot JSON (``lineup_utils_snap.json``)."""
    return load_fixture("lineup_utils_snap.json")


def load_inputs() -> dict:
    """Load the vendored jest test-input JSON (``lineup_utils_inputs.json``)."""
    return load_fixture("lineup_utils_inputs.json")


def load_rating_snap() -> dict:
    """Load the vendored ``RatingUtils`` jest snapshot JSON (``rating_utils_snap.json``)."""
    return load_fixture("rating_utils_snap.json")


def load_rating_inputs() -> dict:
    """Load the vendored ``RatingUtils`` jest test-input JSON (``rating_utils_inputs.json``)."""
    return load_fixture("rating_utils_inputs.json")


def approx_tree(a: Any, b: Any, path: str = "") -> None:
    """Recursively assert numeric equality to 1e-9 rel, exact otherwise."""
    if isinstance(b, dict):
        assert isinstance(a, dict), f"{path}: expected dict"
        for k, v in b.items():
            assert k in a, f"{path}.{k}: missing"
            approx_tree(a[k], v, f"{path}.{k}")
    elif isinstance(b, (int, float)) and not isinstance(b, bool):
        assert a == pytest.approx(b, rel=1e-9, abs=1e-12), f"{path}: {a} != {b}"
    elif isinstance(b, list):
        assert len(a) == len(b), f"{path}: length {len(a)} != {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            approx_tree(x, y, f"{path}[{i}]")
    else:
        assert a == b, f"{path}: {a!r} != {b!r}"


def first_lineup_list(inputs: dict) -> list[dict]:
    """Return the 3-lineup-bucket list from the vendored ``sampleLineupStatsResponse``.

    Known vendored shape (see ``tests/fixtures/hoop_explorer/README.md``):
    ``sampleLineupStatsResponse.responses[0].aggregations.lineups.buckets``.
    Falls back to a depth-bounded structural search if that exact path
    doesn't resolve (defensive against upstream fixture re-vendoring).
    """
    try:
        buckets = inputs["sampleLineupStatsResponse"]["responses"][0]["aggregations"]["lineups"]["buckets"]
        if isinstance(buckets, list) and buckets and isinstance(buckets[0], dict) and "off_poss" in buckets[0]:
            return buckets
    except (KeyError, IndexError, TypeError):
        pass

    def _search(node: Any, depth: int = 0) -> list[dict] | None:
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


def insert_old_values(lineup: dict) -> dict:
    """Python replay of the jest test file's local ``insertOldValues``
    helper (``LineupUtils.test.ts``, inside the ``describe("LineupUtils")``
    block): for every stat whose key is in :data:`LUCK_AFFECTED_FIELDS`
    and whose ``value`` is not nil, stamp ``old_value = value`` and
    ``override = "Test override"``. Mutates ``lineup`` in place and returns
    it (matching the jest helper's own mutate-and-return shape).
    """
    for key, stat in lineup.items():
        if key in LUCK_AFFECTED_FIELDS and isinstance(stat, dict) and stat.get("value") is not None:
            stat["old_value"] = stat["value"]
            stat["override"] = "Test override"
    return lineup


def to_fixed(obj: Any) -> Any:
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


def find_snapshot_for(snap: dict, needle: str) -> Any:
    """Find the (first) vendored snapshot entry whose name contains ``needle``."""
    for name, val in snap.items():
        if needle.lower() in name.lower() and isinstance(val, dict):
            return val
    pytest.skip(f"no parsed snapshot entry matching {needle!r}")


def find_snapshot_exact(snap: dict, full_name: str) -> Any:
    """Look up a vendored snapshot entry by its exact jest-generated name.

    Used for the ``lineupToTeamReport`` sweep, whose per-iteration
    ``describe``d name (``diagMode=[..] regressDiffs=[..] incOnOff=[..]``)
    disambiguates 12 entries that all share the ``lineupToTeamReport``
    substring -- a substring search (:func:`find_snapshot_for`) can't tell
    them apart.
    """
    if full_name not in snap:
        pytest.skip(f"no parsed snapshot entry matching {full_name!r}")
    return snap[full_name]
