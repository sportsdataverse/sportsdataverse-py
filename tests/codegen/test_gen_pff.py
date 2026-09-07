"""Offline tests for the PFF Premium Stats codegen generator (``tools/codegen/gen_pff.py``).

These regenerate the endpoint YAML + returns-schemas from the committed OpenAPI and assert
the flat-stem contract (normal stem, ``league`` as an extra_param, no ``league_shims``).
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
_SPEC = (
    Path(os.environ.get("SDV_INTERNAL_REFS_REPO", "C:/Users/saiem/Documents/sdv-internal-refs"))
    / "pff"
    / "pff-premium.openapi.yaml"
)
pytestmark = pytest.mark.skipif(not _SPEC.exists(), reason="sdv-internal-refs pff spec not present (local-only source)")


def test_gen_pff_emits_yaml_and_schemas():
    subprocess.run([sys.executable, "tools/codegen/gen_pff.py"], cwd=ROOT, check=True)
    doc = yaml.safe_load((ROOT / "tools/codegen/endpoints/pff.yaml").read_text(encoding="utf-8"))
    assert doc["api"] == "pff"
    assert doc["module"] == "pff_core"
    assert doc["auth"] is True
    assert doc["getter_module"] == "sportsdataverse.nfl.pff_runtime"
    assert doc["parser_module"] == "nfl.pff_parsers"
    assert "league_shims" not in doc  # normal flat stem -- league is an extra_param

    eps = {e["short"]: e for e in doc["endpoints"]}
    assert "facet_passing_summary" in eps
    assert "player_passing_summary" in eps
    assert "leagues" in eps

    # view-family endpoints carry a `league` enum extra_param with the exact wire key
    lp = [p for p in eps["facet_passing_summary"]["extra_params"] if p["name"] == "league"]
    assert lp and lp[0]["query_key"] == "league"

    # player_id keeps its snake_case wire name (NOT camelCase)
    pid = [p for p in eps["player_passing_summary"]["extra_params"] if p["name"] == "player_id"]
    assert pid and pid[0]["query_key"] == "player_id"

    # player-detail endpoints route to the detail parser; facet reports to the generic one
    assert eps["player_passing_summary"]["parser"] == "parse_pff_player_detail"
    assert eps["facet_passing_summary"]["parser"] == "parse_pff_report"

    # matrix report present under one of its two source paths
    assert "facet_receiving_coverage_stats" in eps or "facet_defense_coverage_matchup" in eps

    # returns-schema files exist for a flat report
    assert (ROOT / "tools/codegen/schemas/native/pff/passing_summary.yaml").exists()


def test_pff_registered_in_flat_apis():
    import tools.codegen.extract_residual_columns as x
    import tools.codegen.generate as g

    assert ("pff", "nfl") in g.FLAT_APIS
    assert "premium.pff.com" in g._FLAT_API_DOC["pff"]
    # native/pff graduated off the deferred-columns backlog (fully backfilled,
    # 2026-09-03) -- assert it stays fully covered rather than merely deferred.
    assert "native/pff" not in x._DEFERRED_BUCKETS
    uncovered = [r for r in x.iter_schema_columns() if r["bucket"] == "native/pff" and x._uncovered(r)]
    assert not uncovered, f"native/pff regressed: {len(uncovered)} columns lost their description"


def test_pff_descriptions_seeded():
    m = yaml.safe_load((ROOT / "tools/codegen/manual_column_descriptions.yaml").read_text(encoding="utf-8"))
    assert "grades_offense" in m.get("passing_summary", {})
    assert m["passing_summary"]["player_id"]  # non-empty


def test_gen_pff_idempotent():
    subprocess.run([sys.executable, "tools/codegen/gen_pff.py"], cwd=ROOT, check=True)
    first = (ROOT / "tools/codegen/endpoints/pff.yaml").read_text(encoding="utf-8")
    subprocess.run([sys.executable, "tools/codegen/gen_pff.py"], cwd=ROOT, check=True)
    second = (ROOT / "tools/codegen/endpoints/pff.yaml").read_text(encoding="utf-8")
    assert first == second
