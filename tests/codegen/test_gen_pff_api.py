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
_REAL_REFS = Path("C:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-internal-refs")
_REFS = Path(
    os.environ.get("SDV_INTERNAL_REFS_REPO")
    or next((str(c) for c in (ROOT.parent / "sdv-internal-refs", _REAL_REFS) if c.exists()), str(_REAL_REFS))
)
_SPEC = _REFS / "pff" / "developer" / "pff-developer.openapi.json"


def _gen():
    """A fresh import of the generator (its paths read ``SDV_INTERNAL_REFS_REPO`` at import)."""
    spec = importlib.util.spec_from_file_location("gen_pff_api", ROOT / "tools/codegen/gen_pff_api.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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


def test_new_returns_schemas_have_unique_schema_names():
    """Descriptions are keyed by ``schema:``; a bare ``team_roster`` shared ESPN's text with PFF's."""
    base = ROOT / "tools/codegen/schemas"
    ours = {}
    for p in (base / "native/pff_api").glob("*.yaml"):
        ours[p] = yaml.safe_load(p.read_text(encoding="utf-8"))["schema"]
        assert ours[p] == f"pff_api_{p.stem}", p.name
    taken = set(ours.values())
    for p in base.rglob("*.yaml"):
        if p in ours:
            continue
        d = yaml.safe_load(p.read_text(encoding="utf-8"))
        assert not (isinstance(d, dict) and d.get("schema") in taken), p


def test_every_parsed_route_documents_its_columns():
    """19 routes once shipped an EMPTY returns table: one player for every per-player report (a
    QB has no kickoff rows) and nested bodies the capture's shape could not see. Every parsed
    route now documents its columns, or says why it cannot (``unverified``)."""
    base = ROOT / "tools/codegen/schemas"
    eps = _eps()
    for e in eps.values():
        if "parser" not in e:
            continue
        d = yaml.safe_load((base / f"{e['returns_schema']}.yaml").read_text(encoding="utf-8"))
        tables = d.get("frames") if d.get("kind") == "frames" else [d]
        assert (tables and all(t.get("columns") for t in tables)) or d.get("unverified"), e["short"]
    # the two coverage-matrix routes answer the same envelope -> one schema, three frames
    for short in ("facet_receiving_coverage", "facet_defense_coverage_matchup"):
        assert eps[short]["returns_schema"] == "native/pff_api/receiving_coverage_stats"
    d = yaml.safe_load((base / "native/pff_api/receiving_coverage_stats.yaml").read_text(encoding="utf-8"))
    assert [f["section"] for f in d["frames"]] == ["defenders", "receivers", "versus"]


def test_generator_refuses_a_checkout_without_union_captures(tmp_path, monkeypatch):
    """Against an internal-refs commit older than the /v1 union captures, build() would quietly
    rebuild the old EMPTY schemas and main() would delete the parsed ones: it must stop instead."""
    dev = tmp_path / "pff" / "developer"
    (dev / "captures").mkdir(parents=True)
    (dev / "pff-developer.openapi.json").write_text('{"paths": {}}', encoding="utf-8")
    (dev / "captures" / "schemas_v1.json").write_text('{"v1/nfl/player_seasons": {"tables": {}}}', encoding="utf-8")
    (dev / "captures" / "schemas_v2.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("SDV_INTERNAL_REFS_REPO", str(tmp_path))
    mod = _gen()
    with pytest.raises(SystemExit, match="predates the /v1 union captures") as exc:
        mod.build()
    assert str(tmp_path) in str(exc.value) and "sdv-internal-refs#27" in str(exc.value)
    # with a union body present the guard lets it through (an empty spec -> no endpoints)
    (dev / "captures" / "schemas_v1.json").write_text('{"v1/nfl/player_seasons": {"union": {}}}', encoding="utf-8")
    assert mod.build()[0]["endpoints"] == []


def test_league_dtypes_widen_instead_of_first_league_winning():
    mod = _gen()
    assert mod._widen("Int64", "Float64") == mod._widen("Float64", "Int64") == "Float64"
    assert mod._widen("Null", "Int64") == "Int64" and mod._widen("Int64", "Null") == "Int64"
    bodies = [  # nfl: an integral stat, a column null in every nfl row; ncaa: the same stat fractional
        {"passing_summary": [{"player_id": 1, "ypa": 8, "one_percent": None}]},
        {"passing_summary": [{"player_id": 2, "ypa": 7.5, "one_percent": 50.0}]},
    ]
    cols = {c["name"]: c["type"] for c in mod._parsed_schema("x", "parse_pff_report", bodies)["columns"]}
    assert cols == {"player_id": "integer", "ypa": "numeric", "one_percent": "numeric"}


@pytest.mark.skipif(not _SPEC.exists(), reason="sdv-internal-refs pff/developer spec not present (local-only source)")
def test_generator_reproduces_the_committed_yaml():
    mod = _gen()
    try:
        doc, schemas = mod.build()
    except SystemExit as exc:  # a checkout older than the union captures: say so, not a dict diff
        pytest.fail(str(exc))
    assert doc == yaml.safe_load(YAML.read_text(encoding="utf-8"))
    for slug, schema in schemas.items():
        path = ROOT / f"tools/codegen/schemas/native/pff_api/{slug}.yaml"
        assert schema == yaml.safe_load(path.read_text(encoding="utf-8")), slug
