"""Generate the ``cbs_napi`` flat-API endpoint YAML + returns-schemas from the
CBS Sports NAPI OpenAPI spec (``api.cbssports.com/napi``, auth-free).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_on3.py``.

The generator reads the frozen OpenAPI spec and the committed response captures
from the ``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else a
sibling checkout of this repo, else the Windows workspace default) and emits:

* ``tools/codegen/endpoints/cbs_napi.yaml`` -- one endpoint per spec GET op
  (82 of them), host ``https://api.cbssports.com/napi``. The spec is itself
  generated from the API's own ``/resource/endpoint/registry`` self-doc, so the
  parameter names, types and ``allowedValues`` enums are the API's own.
* ``tools/codegen/schemas/native/cbs_napi/<short>.yaml`` -- returns-schema per
  endpoint. Columns come from the **committed real captures** run through the
  actual parser (so the documented table is exactly what a caller gets) for the
  five resources CBS serves anonymously; the other 77 endpoints are documented
  by the spec as a free-form envelope and get an empty column list rather than
  an invented one.

Run: ``python tools/codegen/gen_cbs.py``
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import polars as pl
import yaml

from sportsdataverse.dl_utils import underscore
from sportsdataverse.cbs.cbs_napi_parsers import parse_cbs_napi, parse_cbs_napi_standings

ROOT = Path(__file__).resolve().parents[2]

HOST = "https://api.cbssports.com/napi"
PATH_PREFIX = "/resource"

# polars dtype -> R-style returns-schema type (mirrors gen_on3._DTYPE).
_DTYPE = {
    pl.Int64: "integer",
    pl.Int32: "integer",
    pl.Float64: "numeric",
    pl.Float32: "numeric",
    pl.Boolean: "logical",
    pl.String: "character",
}
# JSON-schema type -> python arg annotation for query/path params.
_PYTYPE = {"integer": "int", "string": "str", "boolean": "bool"}

# Representative example values, every one lifted from a committed capture:
# league/season 59 = NFL (captures/_sample/59_*.json), team 404 = Arizona and
# player 1751796 from those same bodies, sport 1 from the league body's sportId,
# client "cbs" from the registry's authSettings.allowOnly list.
_EXAMPLE: Dict[str, Any] = {
    "league_id": 59,
    "season_id": 59,
    "team_id": 404,
    "player_id": 1751796,
    "sport_id": 1,
    "client_name": "cbs",
}

# short -> (capture file relative to the cbs refs dir, parser) for the resources
# CBS actually serves anonymously. Everything else is a free-form envelope in the
# spec with no committed body, so its returns-schema stays empty rather than
# guessed. ``season_teams``/``team_players``/``team_standings``/``league`` have one
# capture per league; the union across all 17 is used.
_CAPTURES: Dict[str, Tuple[str, Any]] = {
    "league": ("captures/_sample/*_league_meta.json", parse_cbs_napi),
    "season_teams": ("captures/_sample/*_season_teams.json", parse_cbs_napi),
    "team_players": ("captures/_sample/*_team_players.json", parse_cbs_napi),
    "team_standings": ("captures/_sample/*_team_standings.json", parse_cbs_napi_standings),
    "endpoint_registry": ("captures/napi/_discovery/resource_endpoint_registry.json", parse_cbs_napi),
}

# Endpoints whose payload is the ``{year: {season_type: {...}}}`` standings map
# rather than a record list. Only the one with a committed capture is routed
# here -- the sportsline / player standings siblings have no captured body, so
# they keep the generic parser rather than a guessed shape.
_STANDINGS_PARSER = {"team_standings"}

# An ``allowedValues`` entry the registry leaked as a JS source fragment rather
# than a value (``seasonYear``); it is a pattern, not an enum member.
_REGEXP_VALUE = re.compile(r"^new RegExp\(")

_TOKEN = re.compile(r"\{([^}]+)\}")


def _refs_dir() -> Path:
    """Resolve the ``sdv-internal-refs/cbs`` directory (env -> sibling -> default)."""
    env = os.environ.get("SDV_INTERNAL_REFS_REPO")
    candidates = [Path(env)] if env else []
    candidates += [ROOT.parent / "sdv-internal-refs", Path("C:/Users/saiem/Documents/sdv-internal-refs")]
    for base in candidates:
        if (base / "cbs" / "cbssports-napi.openapi.yaml").exists():
            return base / "cbs"
    raise SystemExit("cbs: spec not found -- set SDV_INTERNAL_REFS_REPO to the sdv-internal-refs checkout")


def _load_spec(refs: Path) -> dict:
    return yaml.safe_load((refs / "cbssports-napi.openapi.yaml").read_text(encoding="utf-8"))


def _short_from_path(path: str) -> str:
    """snake_case slug from a NAPI path (``/resource`` prefix + ``{...}`` stripped).

    ``/resource/game/scoring/playerStats/{gameId}`` -> ``game_scoring_player_stats``;
    ``/resource/team/players/{teamId}`` -> ``team_players``. Each surviving path
    segment is snake-cased individually so camelCase compounds keep their word
    boundaries (``teamAssociations`` -> ``team_associations``, not
    ``teamassociations``).
    """
    rest = path[len(PATH_PREFIX) :] if path.startswith(PATH_PREFIX) else path
    segments = [seg for seg in _TOKEN.sub("", rest).split("/") if seg]
    return "_".join(underscore(seg) for seg in segments)


def _enum_note(schema: dict) -> str:
    values = [v for v in (schema.get("enum") or []) if isinstance(v, str) and not _REGEXP_VALUE.match(v)]
    return f" Allowed: {', '.join(values)}." if values else ""


def _param_description(prm: dict) -> str:
    desc = (prm.get("description") or "").strip()
    return f"{desc}{_enum_note(prm.get('schema') or {})}".strip()


def _path_params(op: dict, path: str) -> List[Dict[str, Any]]:
    by_name = {p["name"]: p for p in op.get("parameters", []) if p.get("in") == "path"}
    out: List[Dict[str, Any]] = []
    for tok in _TOKEN.findall(path):
        prm = by_name.get(tok, {})
        jtype = (prm.get("schema") or {}).get("type", "string")
        entry: Dict[str, Any] = {
            "name": underscore(tok),
            "type": _PYTYPE.get(jtype, "str"),
            "required": True,
        }
        desc = _param_description(prm)
        if desc:
            entry["description"] = desc
        out.append(entry)
    return out


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
        entry: Dict[str, Any] = {"name": name, "query_key": wire, "type": _PYTYPE.get(jtype, "str")}
        desc = _param_description(prm)
        if desc:
            entry["description"] = desc
        out.append(entry)
    return out


def _emit_path(path: str) -> str:
    rest = path[len(PATH_PREFIX) :] if path.startswith(PATH_PREFIX) else path
    return PATH_PREFIX + _TOKEN.sub(lambda m: "{" + underscore(m.group(1)) + "}", rest)


def _endpoint_entry(path: str, op: dict) -> Dict[str, Any]:
    short = _short_from_path(path)
    pps = _path_params(op, path)
    entry: Dict[str, Any] = {
        "short": short,
        "summary": op.get("summary") or f"GET {path}",
        "path": _emit_path(path),
        "parser": "parse_cbs_napi_standings" if short in _STANDINGS_PARSER else "parse_cbs_napi",
        "returns_schema": f"native/cbs_napi/{short}",
    }
    if pps:
        entry["path_params"] = pps
    qps = _query_params(op, {p["name"] for p in pps})
    if qps:
        entry["extra_params"] = qps
    example = {p["name"]: _EXAMPLE[p["name"]] for p in pps if p["name"] in _EXAMPLE}
    if example:
        entry["example_args"] = example
    return entry


def _capture_columns(refs: Path, pattern: str, parser: Any) -> List[Dict[str, str]]:
    """Union of parser-emitted columns across every committed capture body.

    Running the real parser (rather than reading the spec's response component)
    guarantees the documented table matches what a caller actually receives:
    snake-cased, ``json_normalize``-flattened, ids restored to ``Int64``. A
    column whose dtype differs between leagues (CBS standings mix
    ``Int64``/``String`` for the same stat) is documented as ``character``.
    ``Null`` contributes nothing -- a column CBS leaves empty for one league
    must not demote the dtype another league proves.
    """
    files = sorted((refs).glob(pattern)) if "*" in pattern else [refs / pattern]
    seen: Dict[str, set] = {}
    order: List[str] = []
    for src in files:
        if not src.exists():
            continue
        frame = parser(json.loads(src.read_text(encoding="utf-8")))
        for name, dtype in frame.schema.items():
            if name not in seen:
                seen[name] = set()
                order.append(name)
            if dtype != pl.Null:
                seen[name].add(_DTYPE.get(dtype, "character"))
    return [
        {"name": name, "type": next(iter(seen[name])) if len(seen[name]) == 1 else "character", "description": ""}
        for name in order
    ]


def _write_yaml(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(obj, fh, sort_keys=True, default_flow_style=False, allow_unicode=True)


def _clean_generated_schema_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for schema_path in path.glob("*.yaml"):
        schema_path.unlink()


def main() -> None:
    refs = _refs_dir()
    spec = _load_spec(refs)
    ops = sorted(
        ((path, item["get"]) for path, item in spec["paths"].items() if "get" in item),
        key=lambda pv: _short_from_path(pv[0]),
    )
    shorts = [_short_from_path(p) for p, _ in ops]
    dupes = sorted({s for s in shorts if shorts.count(s) > 1})
    if dupes:
        raise ValueError(f"duplicate cbs shorts: {dupes}")
    doc = {
        "api": "cbs_napi",
        "host": HOST,
        "name_pattern": "cbs_{short}",
        "module": "cbs_napi",
        "parser_module": "cbs.cbs_napi_parsers",
        "qualifier": "",
        "passthrough_query": False,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raises": [
                "requests.exceptions.RequestException: Connection-level failure after "
                "``dl_utils.download`` exhausts its retries. An unknown id is NOT an "
                "exception -- NAPI answers it with HTTP 200 plus an "
                "``{error|errors|warnings}`` envelope, which parses to a zero-row frame.",
            ],
            "see_also": [
                {
                    "name": "CBS Sports NAPI registry",
                    "url": "https://api.cbssports.com/napi/resource/endpoint/registry",
                    "note": "the API's own self-documenting endpoint catalog (data origin)",
                },
                {
                    "name": "nflfastR",
                    "url": "https://www.nflfastr.com",
                    "note": "R sister package; NAPI is cross-sport, this is its NFL counterpart",
                },
            ],
        },
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    _write_yaml(ROOT / "tools/codegen/endpoints/cbs_napi.yaml", doc)
    schema_dir = ROOT / "tools/codegen/schemas/native/cbs_napi"
    _clean_generated_schema_dir(schema_dir)
    documented = 0
    for short in shorts:
        cols: List[Dict[str, str]] = []
        if short in _CAPTURES:
            pattern, parser = _CAPTURES[short]
            cols = _capture_columns(refs, pattern, parser)
            documented += bool(cols)
        _write_yaml(schema_dir / f"{short}.yaml", {"schema": short, "kind": "dataframe", "columns": cols})
    print(f"cbs_napi: {len(ops)} endpoints, {documented} capture-backed returns-schemas")


if __name__ == "__main__":
    main()
