"""Generate the ``on3`` flat-API endpoint YAML + returns-schemas from the On3
Recruit Database OpenAPI spec (``api.on3.com/public/rdb``, auth-free).

Idempotent: same spec -> byte-identical output. Modeled on ``gen_nba_stats.py``.

The generator reads the frozen OpenAPI spec (path via the ``SDV_INTERNAL_REFS_REPO``
env, default ``C:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-internal-refs``) and emits:

* ``tools/codegen/endpoints/on3.yaml`` -- one endpoint per usable GET op, host
  ``https://api.on3.com/public/rdb/v1`` (the single ``/rdb/v2`` op carries an
  endpoint-level ``host`` override so ``host + path`` == the real URL for both).
* ``tools/codegen/schemas/native/on3/<short>.yaml`` -- returns-schema per endpoint
  (see ``_schema``): the columns ``parse_on3_rdb`` emits on the committed capture
  ``tests/fixtures/on3/<short>.json``; without one (or with zero rows), no columns
  and an ``unverified`` reason. The two legacy scrape schemas
  (``on3_player_rankings`` / ``on3_team_rankings``) that back the demoted
  ``_next/data`` rankings shim are PRESERVED, never clobbered.

Run: ``python tools/codegen/gen_on3.py``
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from sportsdataverse.cfb.on3_parsers import parse_on3_rdb
from sportsdataverse.dl_utils import underscore

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = ROOT / "tests/fixtures/on3"
_POLARS_TO_R = {"Int64": "integer", "Float64": "numeric", "Boolean": "logical"}

HOST = "https://api.on3.com/public/rdb/v1"
HOST_V2 = "https://api.on3.com/public/rdb/v2"

# Legacy scrape schemas backing the demoted rankings shim -- never deleted/regenerated.
_PRESERVE = {"on3_player_rankings", "on3_team_rankings"}

# RDB shorts that would collide with a *deprecated* ``on3_rankings`` shim public
# name -- renamed so the shim keeps the exact legacy name (continuity) and the RDB
# native (``/player/{personKey}/rankings``, a player's current ranking) stays
# reachable under a distinct, stable name.
_SHORT_OVERRIDES = {"player_rankings": "player_person_rankings"}

# JSON-schema type -> R-style returns-schema type (mirrors gen_nba_stats._DTYPE).
_DTYPE = {
    "integer": "integer",
    "number": "numeric",
    "string": "character",
    "boolean": "logical",
    "object": "character",
    "array": "character",
    "unknown": "character",
}
# JSON-schema type -> python arg annotation for query/path params.
_PYTYPE = {"integer": "int", "string": "str", "boolean": "bool"}

# Representative example values (docs only), keyed by snake-cased path-param name.
# Sourced from the committed capture manifest (person 89617, org 1867, recruitment
# 270036, football-2025).
_EXAMPLE: Dict[str, Any] = {
    "person_key": 89617,
    "player_key": 89617,
    "ps_key": 89617,
    "connection_key": 89617,
    "user_key": 89617,
    "org_key": 1867,
    "organization_key": 1867,
    "recruitment_key": 270036,
    "rec_key": 270036,
    "video_key": 1,
    "key": 1,
    "sport_slug": "football",
    "year": 2025,
}

_TOKEN = re.compile(r"\{([^}]+)\}")


def _spec_path() -> Path:
    base = os.environ.get("SDV_INTERNAL_REFS_REPO", "C:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-internal-refs")
    return Path(base) / "on3" / "on3-recruit-database.openapi.yaml"


def _load_spec() -> dict:
    return yaml.safe_load(_spec_path().read_text(encoding="utf-8"))


def _short_from_path(path: str) -> str:
    """snake_case slug from an RDB path (version prefix + ``{...}`` params stripped).

    ``/rdb/v1/team-ranking/{sport}-{year}/team-rankings`` -> ``team_ranking_team_rankings``;
    ``/rdb/v2/nil-100`` -> ``nil_100_v2`` (the ``_v2`` suffix disambiguates the
    ``/rdb/v1/nil-100`` sibling). A "by-id" detail route ending in ``/{param}``
    appends the param name so it does not collapse onto its list sibling
    (``/collective-groups/{key}`` -> ``collective_groups_key`` vs
    ``/collective-groups`` -> ``collective_groups``).
    """
    is_v2 = path.startswith("/rdb/v2")
    rest = path[len("/rdb/v2") :] if is_v2 else path[len("/rdb/v1") :]
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", _TOKEN.sub("", rest)) if t]
    short = "_".join(t.lower() for t in tokens)
    stripped = rest.rstrip("/")
    if stripped.endswith("}"):  # trailing /{param} detail route -> keep the id in the slug
        suffix = _snake_token(_TOKEN.findall(stripped)[-1])
        short = f"{short}_{suffix}" if short else suffix
    if is_v2:
        short += "_v2"
    return _SHORT_OVERRIDES.get(short, short)


def _usable(op: dict) -> bool:
    """Keep an op unless the live probe flagged it 400/404 (dead route).

    204 (empty body -> tolerable zero-row frame) and unflagged/validated ops are
    kept. Handles both the dict form (``{"status": 404}``) and the bare-int form
    (``404``) the spec actually uses.
    """
    xls = op.get("x-live-status")
    status = xls.get("status") if isinstance(xls, dict) else xls
    return status not in (400, 404)


def _snake_token(tok: str) -> str:
    """snake_case a path-param token. ``sport`` -> ``sport_slug`` so it is NOT
    caught by the renderer's ESPN ``{sport}``/``{league}`` slug substitution
    (which would blank it for a flat, league-less API)."""
    if tok == "sport":
        return "sport_slug"
    return underscore(tok)


def _resolve_ref(spec: dict, schema: Any) -> dict:
    seen: set = set()
    while isinstance(schema, dict) and "$ref" in schema:
        name = schema["$ref"].split("/")[-1]
        if name in seen:
            return {}
        seen.add(name)
        schema = spec["components"]["schemas"].get(name, {})
    return schema if isinstance(schema, dict) else {}


def _row_schema(op: dict, spec: dict) -> dict:
    """Resolve the 200-response schema to the schema of ONE parsed row.

    Handles the three RDB shapes: a top-level array (unwrap ``items``), a
    ``*PagedData`` object (unwrap the ``list`` array's ``items``), or a plain
    object / ``On3*Live`` component (the object itself is the row).
    """
    resp = op.get("responses", {}).get("200") or op.get("responses", {}).get(200) or {}
    schema = _resolve_ref(spec, resp.get("content", {}).get("application/json", {}).get("schema") or {})
    if schema.get("type") == "array":
        return _resolve_ref(spec, schema.get("items", {}))
    lst = schema.get("properties", {}).get("list")
    if isinstance(lst, dict) and lst.get("type") == "array":
        return _resolve_ref(spec, lst.get("items", {}))
    return schema


def _spec_types(spec: dict, schema: dict, prefix: str = "", seen: frozenset = frozenset()) -> Dict[str, str]:
    """``{column: json type}`` for a row schema's leaves, named the way ``parse_on3_rdb``
    flattens them (``json_normalize(sep="_")`` then ``underscore``). Used only to type a
    captured column that is all-null in its capture."""
    out: Dict[str, str] = {}
    for name, pv in schema.get("properties", {}).items():
        pv = pv or {}
        if len(pv.get("allOf") or []) == 1:
            pv = pv["allOf"][0]
        ref = pv.get("$ref", "")
        node = _resolve_ref(spec, pv)
        if node.get("properties") and ref not in seen:
            out.update(_spec_types(spec, node, f"{prefix}{name}_", seen | {ref}))
        else:
            out[underscore(f"{prefix}{name}")] = node.get("type") or "unknown"
    return out


def _captured_columns(short: str, spec_types: Dict[str, str]) -> List[Dict[str, str]]:
    """Columns ``parse_on3_rdb`` emits on the committed capture (``[]`` when there is
    none, or it parses to no columns). An all-null column carries no type signal, so
    it takes the response type's declared type."""
    path = FIXTURE_DIR / f"{short}.json"
    if not path.exists():
        return []
    df = parse_on3_rdb(json.loads(path.read_text(encoding="utf-8")))
    cols = []
    for col, dtype in df.schema.items():
        if df[col].null_count() < df.height:
            rtype = _POLARS_TO_R.get(str(dtype), "character")
        else:
            rtype = _DTYPE.get(spec_types.get(col, "unknown"), "character")
        cols.append({"name": col, "type": rtype, "description": ""})
    return cols


# Why a capture-less endpoint publishes no table. Measured 2026-10-05: of the 9
# endpoints with a capture whose response type is authoritative (on3_ts_api) and flat
# -- the best case -- the spec's field names matched parse_on3_rdb's columns on only 7
# (filters_draft_rounds emits ``round``, not ``value``; people_latest_valuation emits 5
# fields the spec lacks), and nullable integers come back Float64.
_UNVERIFIED = (
    "no committed capture with rows; names derived from the OpenAPI response type matched "
    "parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published"
)


def _schema(short: str, op: dict, spec: dict) -> Dict[str, Any]:
    """Returns-schema from the committed capture; else no columns, marked ``unverified``."""
    doc: Dict[str, Any] = {"schema": short, "kind": "dataframe"}
    doc["columns"] = _captured_columns(short, _spec_types(spec, _row_schema(op, spec)))
    if not doc["columns"]:
        doc["unverified"] = _UNVERIFIED
    return doc


def _path_params(path: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for tok in _TOKEN.findall(path):
        name = _snake_token(tok)
        out.append({"name": name, "type": "str" if name == "sport_slug" else "int", "required": True})
    return out


def _emit_path(path: str, is_v2: bool) -> str:
    rest = path[len("/rdb/v2") :] if is_v2 else path[len("/rdb/v1") :]
    return _TOKEN.sub(lambda m: "{" + _snake_token(m.group(1)) + "}", rest)


def _query_params(op: dict, path_names: set) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if prm.get("in") != "query":
            continue
        wire = prm["name"]
        name = underscore(wire)
        if name in path_names:  # path token wins on a name clash
            continue
        jtype = (prm.get("schema") or {}).get("type", "string")
        out.append({"name": name, "query_key": wire, "type": _PYTYPE.get(jtype, "str")})
    return out


def _endpoint_entry(path: str, op: dict, spec: dict) -> Dict[str, Any]:
    is_v2 = path.startswith("/rdb/v2")
    short = _short_from_path(path)
    pps = _path_params(path)
    path_names = {p["name"] for p in pps}
    entry: Dict[str, Any] = {
        "short": short,
        "summary": op.get("summary") or f"GET {path}",
        "path": _emit_path(path, is_v2),
        "parser": "parse_on3_rdb",
        "returns_schema": f"native/on3/{short}",
    }
    if is_v2:
        entry["host"] = HOST_V2
    if pps:
        entry["path_params"] = pps
    qps = _query_params(op, path_names)
    if qps:
        entry["extra_params"] = qps
    example = {p["name"]: _EXAMPLE[p["name"]] for p in pps if p["name"] in _EXAMPLE}
    if example:
        entry["example_args"] = example
    return entry


def _usable_ops(spec: dict) -> List[Tuple[str, dict]]:
    return sorted(
        ((path, ops["get"]) for path, ops in spec["paths"].items() if "get" in ops and _usable(ops["get"])),
        key=lambda pv: _short_from_path(pv[0]),
    )


def _write_yaml(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(obj, fh, sort_keys=True, default_flow_style=False, allow_unicode=True)


def _clean_generated_schema_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for schema_path in path.glob("*.yaml"):
        if schema_path.stem in _PRESERVE:
            continue
        schema_path.unlink()


def main() -> None:
    spec = _load_spec()
    ops = _usable_ops(spec)
    shorts = [_short_from_path(p) for p, _ in ops]
    dupes = sorted({s for s in shorts if shorts.count(s) > 1})
    if dupes:
        raise ValueError(f"duplicate on3 shorts: {dupes}")
    doc = {
        "api": "on3",
        "host": HOST,
        "name_pattern": "on3_{short}",
        "module": "on3",
        "parser_module": "cfb.on3_parsers",
        "getter_module": "sportsdataverse.cfb.on3_runtime",
        "qualifier": "",
        "passthrough_query": True,
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(p, op, spec) for p, op in ops],
    }
    _write_yaml(ROOT / "tools/codegen/endpoints/on3.yaml", doc)
    schema_dir = ROOT / "tools/codegen/schemas/native/on3"
    _clean_generated_schema_dir(schema_dir)
    for path, op in ops:
        short = _short_from_path(path)
        if short in _PRESERVE:
            continue
        _write_yaml(schema_dir / f"{short}.yaml", _schema(short, op, spec))
    print(f"on3: {len(ops)} endpoints")


if __name__ == "__main__":
    main()
