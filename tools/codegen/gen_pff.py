"""Generate the PFF Premium Stats flat-API stem from the reverse-engineered OpenAPI.

Reads ``pff/pff-premium.openapi.yaml`` from the ``sdv-internal-refs`` repo and emits a
**normal flat stem** (``tools/codegen/endpoints/pff.yaml``) plus per-report
returns-schemas (``tools/codegen/schemas/native/pff/*.yaml``). ``league`` is an ordinary
``extra_param`` (enum ``nfl/ncaa/aaf/ufl``) on every view-family endpoint -- there is NO
``league_shims`` block; the per-league binding lives entirely in the hand-written
``sportsdataverse.nfl.pff_league.make_pff_league_module`` helper.

Naming:
    * ``facet`` view  -> ``facet_<report_slug>`` where ``report_slug`` is the sole
      top-level property of the response schema (the response envelope key), e.g.
      ``/facet/defense/run`` -> ``run_defense_summary`` -> ``facet_run_defense_summary``.
    * ``player`` view -> ``player_<path tail>`` (the envelope is free-form), e.g.
      ``/player/passing/summary`` -> ``player_passing_summary``.
    * singletons      -> the path segments joined, e.g. ``/teams/overview`` -> ``teams_overview``.

Parser choice keys off the response schema: ``parse_pff_player_detail`` when the schema
is ``PlayerDetailEnvelope`` (per-week detail for one player), else ``parse_pff_report``
(facet leaderboards, matrix reports, and the meta singletons).

Idempotent: same spec -> byte-identical output.

Run: ``python tools/codegen/gen_pff.py``
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

ROOT = Path(__file__).resolve().parents[2]
_REFS = Path(os.environ.get("SDV_INTERNAL_REFS_REPO", r"C:/Users/saiem/Documents/sdv-internal-refs"))
SPEC_PATH = _REFS / "pff" / "pff-premium.openapi.yaml"

HOST = "https://premium.pff.com"

# OpenAPI JSON-schema primitive -> R-style returns-schema type
_DTYPE = {
    "number": "numeric",
    "integer": "integer",
    "string": "character",
    "boolean": "logical",
    "array": "list",
    "object": "list",
}

# component-parameter $ref key -> (python_name, wire query_key, python type, description).
# The wire query_key must stay the EXACT name the API expects -- PFF uses snake_case
# ``player_id`` (not camelCase), while ``franchiseId`` / ``gameId`` are camelCase.
_PARAM_MAP: Dict[str, tuple] = {
    "league": ("league", "league", "str", "League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules."),
    "season": ("season", "season", "int", "Season (starting year)."),
    "week": ("week", "week", "str", "Week or week-group key (e.g. 'REG', a week number, or a range)."),
    "weekSingle": ("week", "week", "int", "Single week number."),
    "playerId": ("player_id", "player_id", "int", "PFF player id (snake_case on the wire; matches the /players id)."),
    "franchiseId": ("franchise_id", "franchiseId", "int", "PFF franchise (team) id; filters a report 'By Team'."),
    "gameId": ("game_id", "gameId", "int", "PFF game id; filters a report 'By Game'."),
    "division": ("division", "division", "str", "Division filter (NCAA)."),
    "career": ("career", "career", "str", 'Career-rollup flag ("true"/"false"); player-detail views only.'),
    "name": ("name", "name", "str", "Player-name search prefix."),
    "id": ("id", "id", "int", "Entity id (player lookup)."),
}


def _load_spec() -> dict:
    return yaml.safe_load(SPEC_PATH.read_text(encoding="utf-8"))


def _param_ref(p: dict) -> Optional[str]:
    if "$ref" in p:
        return p["$ref"].split("/")[-1]
    return p.get("name")


def _schema_ref(op: dict) -> str:
    resp = op.get("responses", {}).get("200", {})
    content = resp.get("content")
    if not content:
        return ""
    return content.get("application/json", {}).get("schema", {}).get("$ref", "").split("/")[-1]


def _envelope_keys(schemas: dict, ref: str) -> List[str]:
    props = schemas.get(ref, {}).get("properties") if ref else None
    return list(props.keys()) if isinstance(props, dict) else []


def _item_props(schemas: dict, prop: dict) -> Dict[str, dict]:
    """Resolve the object-item properties of an array-valued envelope property."""
    items = prop.get("items", {}) or {}
    if "$ref" in items:
        items = schemas.get(items["$ref"].split("/")[-1], {})
    return items.get("properties", {}) or {}


def _extra_params(refs: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ref in refs:
        mapped = _PARAM_MAP.get(ref)
        if mapped is None:
            continue
        name, query_key, ptype, desc = mapped
        out.append({"name": name, "query_key": query_key, "type": ptype, "description": desc})
    return out


def _report_schema(schemas: dict, ref: str, env_keys: List[str], is_player_detail: bool) -> Dict[str, Any]:
    """Build a returns-schema dict from the response envelope's row shape.

    Player-detail + matrix + content-less endpoints have no per-row column list in the
    spec, so they emit a zero-column schema (descriptions are a deferred follow-up via the
    ``native/pff`` bucket in ``extract_residual_columns._DEFERRED_BUCKETS``).
    """
    slug = env_keys[0] if env_keys else ref
    if is_player_detail or not env_keys:
        return {
            "schema": slug,
            "kind": "dataframe",
            "columns": [],
            "note": "player_detail" if is_player_detail else "opaque",
        }
    prop = schemas[ref]["properties"][env_keys[0]]
    if prop.get("type") == "array":
        cols = [
            {"name": c, "type": _DTYPE.get((cv or {}).get("type"), "character"), "description": ""}
            for c, cv in _item_props(schemas, prop).items()
        ]
        return {"schema": slug, "kind": "dataframe", "columns": cols}
    # object-valued envelope (the coverage matrix) -> 3 sub-frames, no flat column list
    return {"schema": slug, "kind": "dataframe", "columns": [], "note": "matrix"}


def build() -> tuple[dict, Dict[str, dict]]:
    """Return (endpoint-yaml doc, {schema_slug: returns-schema})."""
    spec = _load_spec()
    schemas = spec["components"]["schemas"]
    endpoints: List[Dict[str, Any]] = []
    schema_docs: Dict[str, dict] = {}
    used: set = set()

    for path, item in spec["paths"].items():
        op = item["get"]
        segs = path.replace("/api/v1/", "").strip("/").split("/")
        view = segs[0] if segs[0] in ("facet", "player") else None
        ref = _schema_ref(op)
        env_keys = _envelope_keys(schemas, ref)
        is_player_detail = ref == "PlayerDetailEnvelope"

        if view == "facet":
            slug = env_keys[0] if env_keys else "_".join(segs[1:])
            short = f"facet_{slug}"
            schema_slug = slug
        elif view == "player":
            short = "player_" + "_".join(segs[1:])
            schema_slug = short
        else:
            short = "_".join(segs)
            schema_slug = env_keys[0] if env_keys else short

        # disambiguate an already-used short (the two coverage-matrix paths both resolve
        # to the report slug ``receiving_coverage_stats``) by falling back to the URL tail.
        if short in used:
            short = f"{segs[0]}_" + "_".join(segs[1:])
        used.add(short)

        endpoints.append(
            {
                "short": short,
                "summary": op.get("summary", ""),
                "path": path,
                "extra_params": _extra_params([r for r in (_param_ref(p) for p in op.get("parameters", [])) if r]),
                "parser": "parse_pff_player_detail" if is_player_detail else "parse_pff_report",
                "returns_schema": f"native/pff/{schema_slug}",
            }
        )
        schema_docs.setdefault(schema_slug, _report_schema(schemas, ref, env_keys, is_player_detail))

    doc = {
        "api": "pff",
        "host": HOST,
        "name_pattern": "pff_{short}",
        "module": "pff_core",
        "parser_module": "nfl.pff_parsers",
        "getter_module": "sportsdataverse.nfl.pff_runtime",
        "auth": True,
        "runtime_imports": ["_get"],
        "endpoints": endpoints,
    }
    return doc, schema_docs


def _write_yaml(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(obj, fh, sort_keys=True, default_flow_style=False, allow_unicode=True)


def _clean_generated_schema_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for schema_path in path.glob("*.yaml"):
        schema_path.unlink()


def main() -> None:
    doc, schema_docs = build()
    _write_yaml(ROOT / "tools/codegen/endpoints/pff.yaml", doc)
    schema_dir = ROOT / "tools/codegen/schemas/native/pff"
    _clean_generated_schema_dir(schema_dir)
    for slug, schema in schema_docs.items():
        _write_yaml(schema_dir / f"{slug}.yaml", schema)
    print(f"pff: {len(doc['endpoints'])} endpoints, {len(schema_docs)} schemas")


if __name__ == "__main__":
    main()
