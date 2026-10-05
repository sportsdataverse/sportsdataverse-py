"""Generate the PFF Developer API (``api.pff.com``) flat stem from PFF's own OpenAPI spec.

PFF publishes an official, API-key-authenticated API whose ``/v1`` mirrors the legacy
Premium Stats routes byte-for-byte and whose ``/v2/{league}`` is a new camelCase
``{meta, columns, rows}`` contract. This generator turns PFF's spec into a normal flat
stem -- one ``pff_api_<operationId>`` wrapper per operation, mirroring the ``restish pff
<operationId>`` command names -- plus the returns-schemas the reference docs render.

Inputs:
    * ``pff/developer/pff-developer.openapi.json`` in the sibling ``sdv-internal-refs``
      checkout (``SDV_INTERNAL_REFS_REPO``), vendored byte-for-byte from PFF.
    * ``pff/developer/captures/schemas_v{1,2}.json`` there -- the live-captured columns
      (``/v2`` tables describe themselves; ``/v1`` columns are the union across rows). A ``/v1``
      target may also carry a ``union`` body (every row list folded into one row; sdv-internal-refs
      #27 onward, required): ANY parsed ``/v1`` route with one is documented as what its parser
      emits on that body (``_parsed_schema``), unless it reuses a non-empty legacy schema.
    * this repo's LEGACY stem ``tools/codegen/endpoints/pff.yaml``: a ``/v1`` route that
      already existed on premium.pff.com returns the identical wire format, so it reuses
      the legacy returns-schema instead of minting a duplicate.

Outputs (idempotent -- same inputs => byte-identical files):
    * ``tools/codegen/endpoints/pff_api.yaml``
    * ``tools/codegen/schemas/native/pff_api/*.yaml``

Wire details the stem encodes (all verified live 2026-09-26):
    * query keys are exactly the spec's ``name`` -- snake_case on ``/v1``
      (``franchise_id``/``game_id``; the camelCase the legacy site took is silently
      IGNORED here), camelCase on ``/v2`` (``weekGroup``/``weekIds``/``weekTo``);
    * CSV switches (``export``, ``format``, ``table``) are not wrapped: every wrapper
      returns JSON routed through a parser.

Run: ``python tools/codegen/gen_pff_api.py``
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List

import yaml

from sportsdataverse.dl_utils import underscore
from sportsdataverse.nfl import pff_parsers

ROOT = Path(__file__).resolve().parents[2]
_REAL_REFS = Path("C:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-internal-refs")
_REFS = Path(
    os.environ.get("SDV_INTERNAL_REFS_REPO")
    or next((str(c) for c in (ROOT.parent / "sdv-internal-refs", _REAL_REFS) if c.exists()), str(_REAL_REFS))
)
DEV = _REFS / "pff" / "developer"
SPEC_PATH = DEV / "pff-developer.openapi.json"
CAPTURES = DEV / "captures"
LEGACY_YAML = ROOT / "tools" / "codegen" / "endpoints" / "pff.yaml"
LEGACY_SCHEMAS = ROOT / "tools" / "codegen" / "schemas" / "native" / "pff"
HOST = "https://api.pff.com"

_SKIP_PARAMS = {"export", "format", "table"}  # CSV-only switches
_PY_TYPE = {"integer": "int", "number": "float", "boolean": "bool"}
_DTYPE = {"integer": "integer", "number": "numeric", "string": "character", "boolean": "logical"}
_POLARS_TO_R = {"Int64": "integer", "Float64": "numeric", "Boolean": "logical"}
_STALE_REFS = f"sdv-internal-refs at {_REFS} predates the /v1 union captures (sdv-internal-refs#27); pull it"


def snake(s: str) -> str:
    """The parser's own column rename, so documented names match ``parse_pff_v2_table`` output
    exactly (a local regex disagreed on capital runs: ``targetShareRBRank``)."""
    return underscore(s)


def _load() -> tuple[dict, dict, dict, dict]:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    s1 = json.loads((CAPTURES / "schemas_v1.json").read_text(encoding="utf-8"))
    if not any("union" in v for k, v in s1.items() if k.startswith("v1/")):
        # without the union bodies build() would silently fall back to the old empty schemas and
        # main() would DELETE the parsed ones
        raise SystemExit(_STALE_REFS)
    s2 = json.loads((CAPTURES / "schemas_v2.json").read_text(encoding="utf-8"))
    legacy = yaml.safe_load(LEGACY_YAML.read_text(encoding="utf-8"))
    legacy_schema = {e["path"].replace("/api/v1", "/v1", 1): e["returns_schema"] for e in legacy["endpoints"]}
    return spec, s1, s2, legacy_schema


def _params(spec: dict, op: dict) -> List[dict]:
    out = []
    for p in op.get("parameters", []):
        if "$ref" in p:
            p = spec["components"]["parameters"][p["$ref"].split("/")[-1]]
        out.append(p)
    return out


def _desc(p: dict) -> str:
    """First sentence of PFF's parameter description, flattened to one line."""
    text = re.sub(r"\s+", " ", (p.get("description") or "").strip())
    first = re.split(r"(?<=[.])\s", text, maxsplit=1)[0]
    return first.replace("`", "")


def _response_shape(spec: dict, op: dict) -> Dict[str, str]:
    """{envelope property -> referenced schema name or JSON type} of the JSON 200."""
    content = (op["responses"].get("200") or {}).get("content", {}).get("application/json", {})
    ref = content.get("schema", {}).get("$ref", "").split("/")[-1]
    props = spec["components"]["schemas"].get(ref, {}).get("properties", {})
    return {k: (v.get("$ref", "").split("/")[-1] or str(v.get("type"))) for k, v in props.items()}


def _parser(path: str, op: dict, shape: Dict[str, str]) -> str | None:
    if path.startswith("/v2"):
        return "parse_pff_v2_table"
    if op["operationId"] == "whoami":
        return None  # a flat account record, returned as the raw dict
    if "PlayerReportSummary" in shape.values():
        return "parse_pff_player_detail"
    return "parse_pff_report"


def _cols(columns: List[dict]) -> List[dict]:
    out, seen = [], set()
    for c in columns:
        name = snake(c["key"])
        if name in seen:
            continue
        seen.add(name)
        out.append({"name": name, "type": _DTYPE.get(c.get("type"), "character"), "description": ""})
    return out


def _v2_union(s2: dict, tid: str) -> List[dict]:
    """Union of a /v2 target's self-described columns across nfl + ncaa, first-seen order."""
    cols: List[dict] = []
    for lg in ("nfl", "ncaa"):
        cols += (s2.get(f"v2/{lg}/{tid}") or {}).get("columns") or []
    return _cols(cols)


def _schema_name(short: str) -> str:
    """The ``schema:`` value -- the key column descriptions are looked up by. Bare slugs collide:
    ``team_roster``/``team_schedule``/``team_stats``/``team_report`` are already ESPN, MLB, CBS,
    NWSL and NHL schema names, and a shared key renders one family's text on the other's page."""
    return f"pff_api_{short}"


def _v2_schema(s2: dict, short: str, variants: List[str] | None, prefix: str, by: str = "") -> Dict[str, Any]:
    if variants is None:
        return {"schema": _schema_name(short), "kind": "dataframe", "columns": _v2_union(s2, prefix)}
    frames = [{"section": v, "columns": _v2_union(s2, f"{prefix}__{v}")} for v in variants]
    # ``frames_by`` = the request parameter whose value picks the table: the function returns
    # ONE DataFrame, not a dict of them (generate.py reads the marker).
    return {
        "schema": _schema_name(short),
        "kind": "frames",
        "frames_by": by,
        "frames": [f for f in frames if f["columns"]],
    }


def _v1_schema(s1: dict, short: str, target: str, shape: Dict[str, str]) -> Dict[str, Any]:
    """A NEW /v1 route: union columns of its row array; player-detail envelopes are opaque."""
    cols: Dict[str, str] = {}
    for lg in ("nfl", "ncaa"):
        for tname, t in ((s1.get(f"v1/{lg}/{target}") or {}).get("tables") or {}).items():
            if tname == "restricted":
                continue
            for c, types in (t.get("columns") or {}).items():
                real = [x for x in types if x != "null"] or ["string"]
                cols.setdefault(c, real[0])
    if not cols:
        note = "player_detail" if "PlayerReportSummary" in shape.values() else "opaque"
        return {"schema": _schema_name(short), "kind": "dataframe", "columns": [], "note": note}
    return {
        "schema": _schema_name(short),
        "kind": "dataframe",
        "columns": [{"name": snake(c), "type": _DTYPE.get(t, "character"), "description": ""} for c, t in cols.items()],
    }


def _legacy_empty(rs: str) -> bool:
    """True when the LEGACY returns-schema ``rs`` documents no columns (its premium.pff.com
    capture predates role-appropriate players / nested bodies), so pff_api mints its own."""
    doc = yaml.safe_load((ROOT / "tools/codegen/schemas" / f"{rs}.yaml").read_text(encoding="utf-8")) or {}
    return not (doc.get("frames") if doc.get("kind") == "frames" else doc.get("columns"))


def _union_bodies(s1: dict, target: str) -> List[dict]:
    """The captured UNION bodies of a /v1 target (nfl, then ncaa): ``capture.py``'s
    ``union_body`` folds every row of a list into one, so a parser run on it emits every
    column any row carried -- the full surface that one row (or a trimmed sample) undercounts."""
    return [b for lg in ("nfl", "ncaa") if (b := (s1.get(f"v1/{lg}/{target}") or {}).get("union")) is not None]


def _widen(a: str, b: str) -> str:
    """Polars dtype of a column seen as ``a`` then ``b`` (e.g. nfl then ncaa): a null side takes
    the other, Int64 + Float64 is Float64 (what a parser infers over both), otherwise ``a``."""
    if a == "Null":
        return b
    return "Float64" if {a, b} == {"Int64", "Float64"} else a


def _parsed_schema(slug: str, parser: str, bodies: List[dict]) -> Dict[str, Any]:
    """Returns-schema = what ``parser`` emits on ``bodies``, one table per section it returns.

    A column's type is its dtype across the bodies (``_widen``: null takes the other, Int64 and
    Float64 make numeric); null in every capture, it is typed from no data and left
    ``character``. A ``restricted`` list (columns withheld by
    the account's entitlement) makes the schema ``unverified`` rather than a partial table."""
    doc: Dict[str, Any] = {"schema": _schema_name(slug), "kind": "dataframe"}
    withheld = sorted({c for b in bodies for c in b.get("restricted") or []})
    if withheld:
        doc.update(
            columns=[],
            unverified=f"PFF withheld {len(withheld)} columns from the capture under the account's entitlement ({', '.join(withheld)})",
        )
        return doc
    seen: Dict[Any, Dict[str, str]] = {}  # {section: {column: polars dtype}}, first-seen order
    for body in bodies:
        out = getattr(pff_parsers, parser)(body)
        for section, df in out.items() if isinstance(out, dict) else [(None, out)]:
            cols = seen.setdefault(section, {})
            for name, dtype in df.schema.items():
                cols[name] = _widen(cols.get(name, "Null"), str(dtype))

    def table(cols: Dict[str, str]) -> List[dict]:
        return [{"name": n, "type": _POLARS_TO_R.get(t, "character"), "description": ""} for n, t in cols.items()]

    if None in seen:
        doc["columns"] = table(seen[None])
    else:
        doc.update(kind="frames", frames=[{"section": s, "columns": table(c)} for s, c in seen.items()])
    return doc


def _example_args(op: dict, params: List[dict]) -> Dict[str, Any]:
    """Runnable example: required params + the first ``x-requires-one-of`` alternative
    (facets need ``league`` AND ``season`` together although neither is required alone)."""
    together = set(((op.get("x-requires-one-of") or {}).get("alternatives") or [[]])[0])
    args: Dict[str, Any] = {}
    for p in params:
        if p["name"] in _SKIP_PARAMS or p.get("example") is None:
            continue
        if p.get("required") or p["name"] in together or p["name"] == "season":
            args[snake(p["name"])] = p["example"]
    return args


def build() -> tuple[dict, Dict[str, dict]]:
    """Return (endpoint-yaml doc, {schema_slug: returns-schema})."""
    spec, s1, s2, legacy_schema = _load()
    endpoints: List[Dict[str, Any]] = []
    schemas: Dict[str, dict] = {}
    parsed: Dict[str, tuple] = {}  # {schema slug: (parser, union bodies)}
    for path, item in spec["paths"].items():
        op = item.get("get")
        # x-cli-hidden only hides a command from restish; facet-defense-coverage-matchup is
        # hidden there but is a live route the legacy stem wraps, so it is kept.
        if not op or not path.startswith(("/v1/", "/v2/")):
            continue
        short = op["operationId"].replace("-", "_")
        params = _params(spec, op)
        shape = _response_shape(spec, op)
        ep: Dict[str, Any] = {"short": short, "path": path, "summary": op.get("summary", "")}
        path_params, extra = [], []
        for p in params:
            if p["name"] in _SKIP_PARAMS:
                continue
            ptype = _PY_TYPE.get((p.get("schema") or {}).get("type"), "str")
            if p["in"] == "path":
                path_params.append({"name": p["name"], "type": ptype, "required": True, "description": _desc(p)})
            else:
                entry = {"name": snake(p["name"]), "query_key": p["name"], "type": ptype, "description": _desc(p)}
                if p.get("required"):
                    entry["required"] = True
                extra.append(entry)
        if path_params:
            ep["path_params"] = path_params
        if extra:
            ep["extra_params"] = extra
        example = _example_args(op, params)
        if example:
            ep["example_args"] = example
        parser = _parser(path, op, shape)
        if parser:
            ep["parser"] = parser

        legacy_rs = legacy_schema.get(path)
        env = next((k for k in shape if k != "restricted"), "")
        if not legacy_rs and path.startswith("/v1") and shape.get(env) == "ReportPlayerRows":
            # a NEW per-player route returning the SAME rows as a legacy facet report
            # (player/passing/depth == facet passing_depth rows): reuse that schema
            legacy_rs = f"native/pff/{env}" if (LEGACY_SCHEMAS / f"{env}.yaml").exists() else None
        bodies = _union_bodies(s1, short) if parser and path.startswith("/v1") else []
        if legacy_rs and not (bodies and _legacy_empty(legacy_rs)):
            ep["returns_schema"] = legacy_rs  # identical wire format / rows -> legacy schema
        elif bodies:
            # Deliberate: ANY parsed /v1 route with a captured union body (and no non-empty legacy
            # schema to reuse) is documented as what its parser emits on it -- the most faithful source (nested rows, game_* explosion, id
            # casts). Today only the 19 re-captured routes carry one; a full re-sweep would move
            # the other NEW routes (e.g. team_summary) here from _v1_schema's raw-key union: the
            # same underscore-d names (descriptions still match), parser dtypes and first-seen
            # instead of sorted column order. Routes sharing a legacy envelope share one schema
            # (both coverage-matrix routes answer `receiving_coverage_stats`).
            slug = legacy_rs.rsplit("/", 1)[-1] if legacy_rs else short
            parsed.setdefault(slug, (parser, []))[1].extend(bodies)
            ep["returns_schema"] = f"native/pff_api/{slug}"
        elif path.startswith("/v2"):
            variants, by = None, ""
            if short in ("team_report", "position_report"):
                by = "report"
            elif short == "team_stats":
                by = "category"
            elif short == "team_leaders":
                by = "group"
            if by:
                variants = next(p for p in params if p["name"] == by)["schema"]["enum"]
            capture_id = {"team_directory": "team_directory"}.get(short, short)
            schemas[short] = _v2_schema(s2, short, variants, capture_id, by)
            ep["returns_schema"] = f"native/pff_api/{short}"
        elif parser:
            schemas[short] = _v1_schema(s1, short, short, shape)
            ep["returns_schema"] = f"native/pff_api/{short}"
        endpoints.append(ep)
    for slug, (parser, bodies) in parsed.items():
        schemas[slug] = _parsed_schema(slug, parser, bodies)

    doc = {
        "api": "pff_api",
        "host": HOST,
        "name_pattern": "pff_api_{short}",
        "module": "pff_api",
        "parser_module": "nfl.pff_parsers",
        "getter_module": "sportsdataverse.nfl.pff_api_runtime",
        "auth": True,
        "runtime_imports": ["_get"],
        "docstring": {
            "headers_doc": (
                "optional headers dict reused across calls; an ``Authorization: Bearer <key>`` here "
                "wins over ``api_key=`` (a keyword accepted by every wrapper) and the "
                "``PFF_API_KEY`` / ``SDV_PY_PFF_API_KEY`` environment. With no key anywhere the "
                "call raises RuntimeError -- there is no anonymous access."
            ),
            "raw_doc": (
                "The decoded JSON body. /v1 routes return the Premium Stats envelope "
                "(``{report_slug: rows}``); /v2 routes return ``{..meta.., columns, rows}``."
            ),
            "raises": [
                "RuntimeError: No PFF API key (pass ``api_key=`` or set ``PFF_API_KEY``).",
                "NoDataError: PFF answered 404 (no such route, report or team).",
                "AssetFetchError: The request was refused or failed (401/403/429/5xx) -- the "
                "answer is unknown, never an empty frame.",
                "ValueError: PFF rejected the parameters (400 ``invalid_parameter`` / 422).",
            ],
            "see_also": [
                {"name": "PFF Developer API", "url": "https://developer.pff.com", "note": "reference + CLI guide"},
                {"name": "nflverse", "url": "https://nflverse.nflverse.com", "note": "NFL data ecosystem"},
            ],
            "example_import": True,
        },
        "endpoints": endpoints,
    }
    return doc, schemas


def _write_yaml(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(obj, fh, sort_keys=True, default_flow_style=False, allow_unicode=True)


def main() -> None:
    doc, schemas = build()
    _write_yaml(ROOT / "tools/codegen/endpoints/pff_api.yaml", doc)
    schema_dir = ROOT / "tools/codegen/schemas/native/pff_api"
    schema_dir.mkdir(parents=True, exist_ok=True)
    for old in schema_dir.glob("*.yaml"):
        old.unlink()
    for slug, schema in schemas.items():
        _write_yaml(schema_dir / f"{slug}.yaml", schema)
    print(f"pff_api: {len(doc['endpoints'])} endpoints, {len(schemas)} new schemas")


if __name__ == "__main__":
    main()
