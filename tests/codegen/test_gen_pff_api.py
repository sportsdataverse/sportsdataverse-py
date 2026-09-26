"""Offline tests for the PFF Developer API stem (``tools/codegen/gen_pff_api.py``).

Two layers: invariants of the COMMITTED ``pff_api.yaml`` (always run -- they guard the wire
contract even without the sibling checkout), and a byte-reproduction check that regenerates from
the vendored spec + captures in ``sdv-internal-refs`` (skipped when that checkout is absent).
"""

import importlib.util
import os
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
YAML = ROOT / "tools/codegen/endpoints/pff_api.yaml"
_REFS = Path(os.environ.get("SDV_INTERNAL_REFS_REPO", str(ROOT.parent / "sdv-internal-refs")))
_SPEC = _REFS / "pff" / "developer" / "pff-developer.openapi.json"


def _eps() -> dict:
    return {e["short"]: e for e in yaml.safe_load(YAML.read_text(encoding="utf-8"))["endpoints"]}


def test_facet_filters_use_snake_case_wire_keys():
    """api.pff.com SILENTLY IGNORES camelCase franchiseId/gameId (returns the whole leaderboard)."""
    eps = _eps()
    facets = [e for s, e in eps.items() if s.startswith("facet_")]
    assert len(facets) == 28
    for e in facets:
        keys = {p["query_key"] for p in e.get("extra_params", [])}
        assert {"franchise_id", "game_id"} <= keys, e["short"]
        assert not keys & {"franchiseId", "gameId"}, e["short"]


def test_v2_routes_take_league_as_a_path_param_and_camel_query_keys():
    eps = _eps()
    ts = eps["team_stats"]
    assert ts["path"] == "/v2/{league}/teams/stats"
    assert [p["name"] for p in ts["path_params"]] == ["league"]
    assert {p["query_key"] for p in ts["extra_params"]} >= {"weekGroup", "weekIds", "category", "scope"}
    assert all(e["parser"] == "parse_pff_v2_table" for e in eps.values() if e["path"].startswith("/v2"))


def test_csv_switches_are_not_wrapped_and_legacy_schemas_are_reused():
    eps = _eps()
    for e in eps.values():
        assert not {p["query_key"] for p in e.get("extra_params", [])} & {"export", "format", "table"}, e["short"]
    # /v1 routes with an unchanged wire format point at the LEGACY returns-schema
    assert eps["facet_passing_summary"]["returns_schema"] == "native/pff/passing_summary"
    assert eps["team_summary"]["returns_schema"] == "native/pff_api/team_summary"
    assert eps["player_offense_pass_blocking"]["parser"] == "parse_pff_player_detail"
    assert "parser" not in eps["whoami"]


@pytest.mark.skipif(not _SPEC.exists(), reason="sdv-internal-refs pff/developer spec not present (local-only source)")
def test_generator_reproduces_the_committed_yaml():
    spec = importlib.util.spec_from_file_location("gen_pff_api", ROOT / "tools/codegen/gen_pff_api.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    doc, schemas = mod.build()
    assert doc == yaml.safe_load(YAML.read_text(encoding="utf-8"))
    for slug, schema in schemas.items():
        path = ROOT / f"tools/codegen/schemas/native/pff_api/{slug}.yaml"
        assert schema == yaml.safe_load(path.read_text(encoding="utf-8")), slug
