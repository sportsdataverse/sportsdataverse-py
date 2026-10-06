"""Generate the ``openligadb`` flat-API endpoint YAML + returns-schemas from the
OpenLigaDB OpenAPI spec (``api.openligadb.de``, keyless, read-only, no query params).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_sleeper.py``, with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen spec and the committed captures from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/openligadb.yaml`` -- one endpoint per GET route (11).
* ``tools/codegen/schemas/native/openligadb/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running :func:`parse_openligadb` over the committed
  capture; a route without a capture is marked ``unverified``.

Run: ``python tools/codegen/gen_openligadb.py``
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from gen_soccer_common import (
    ROOT,
    columns_from_frame,
    get_ops,
    load_spec,
    markdown_descriptions,
    parse_capture,
    refs_dir,
    rewrite_schema_dir,
    spec_descriptions,
    write_yaml,
)

from sportsdataverse.dl_utils import underscore
from sportsdataverse.soccer.openligadb_parsers import parse_openligadb

HOST = "https://api.openligadb.de"
SPEC = "openligadb.openapi.yaml"
STEM = "openligadb"

# Wrapper short per spec path. Every spec path must be listed (asserted below) so
# a route added upstream fails loudly instead of inventing a name.
_SHORTS: Dict[str, str] = {
    "/getavailableleagues": "leagues",
    "/getavailablegroups/{league}/{season}": "groups",
    "/getcurrentgroup/{league}": "current_group",
    "/getavailableteams/{league}/{season}": "teams",
    "/getmatchdata/{league}/{season}": "season_matches",
    "/getmatchdata/{league}/{season}/{group}": "group_matches",
    "/getmatchdata/{match_id}": "match",
    "/getbltable/{league}/{season}": "table",
    "/getgoalgetters/{league}/{season}": "goalgetters",
    "/getnextmatchbyleagueteam/{league_id}/{team_id}": "next_match",
    "/getlastchangedate/{league}/{season}/{group}": "last_change_date",
}

# Review Focus 1: only the BARE {league} token collides with the module renderer
# (spec.py:304 treats {sport}/{league} as ESPN slugs and blanks the segment).
# {league_id} is a distinct name and is left alone. The gen_asa.py lesson.
_SLUG_RENAMES = {"league": "league_slug"}


def _capture_for(refs: Path, path: str) -> Path | None:
    """``capture.py``'s slug: ``/getmatchdata/{league}/{season}`` -> ``getmatchdata__league__season``."""
    slug = path.strip("/").replace("/", "__").replace("{", "").replace("}", "")
    candidate = refs / "captures" / f"{slug}.json"
    return candidate if candidate.exists() else None


def _endpoint_entry(path: str, op: dict) -> Dict[str, Any]:
    short = _SHORTS[path]
    out_path = path
    for raw_name, safe in _SLUG_RENAMES.items():
        out_path = out_path.replace(f"{{{raw_name}}}", f"{{{safe}}}")
    path_params: List[Dict[str, Any]] = []
    extra_params: List[Dict[str, Any]] = []
    example_args: Dict[str, Any] = {}
    for prm in op.get("parameters", []):
        name = _SLUG_RENAMES.get(prm["name"], prm["name"])
        desc = str(prm.get("description") or "").strip()
        if prm.get("in") == "path":
            path_params.append({"name": name, "type": "str", "required": True})
        elif prm.get("in") == "query":
            extra_params.append(
                {"name": underscore(name), "query_key": prm["name"], "type": "str", "description": desc}
            )
        if prm.get("example") is not None:
            # Every param is a string on the wire; a numeric id must never travel as an int.
            example_args[underscore(name)] = str(prm["example"])
    assert not {"league", "sport"} & {p["name"] for p in path_params}, path
    assert "{league}" not in out_path and "{sport}" not in out_path, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (op.get("summary") or f"Fetch {path}").rstrip(".") + ".",
        "path": out_path,
        "parser": "parse_openligadb",
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": example_args,
    }
    if path_params:
        entry["path_params"] = path_params
    if extra_params:
        entry["extra_params"] = extra_params
    return entry


def main() -> None:
    refs = refs_dir(STEM, SPEC)
    spec = load_spec(refs, SPEC)
    ops = get_ops(spec)
    missing = sorted(set(p for p, _ in ops) - set(_SHORTS))
    if missing:
        raise SystemExit(f"{STEM}: spec paths without a short: {missing}")
    ops.sort(key=lambda pv: _SHORTS[pv[0]])

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "openligadb_{short}",
        "module": STEM,
        "parser_module": "soccer.openligadb_parsers",
        "qualifier": "",
        "passthrough_query": False,
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: OpenLigaDB answered 404 (unknown league, season, matchday or match id -- /getnextmatchbyleagueteam 404s once the season is over).",
            ],
            "see_also": [
                {
                    "name": "OpenLigaDB",
                    "url": "https://api.openligadb.de/index.html",
                    "note": "keyless community API for German football; no query parameters, everything is a path segment",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    descriptions = [spec_descriptions(spec), markdown_descriptions(refs / f"{STEM}-returns.md")]
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    unverified = 0
    for path, _op in ops:
        short = _SHORTS[path]
        capture = _capture_for(refs, path)
        schema: Dict[str, Any] = {"schema": short, "kind": "dataframe"}
        if capture is None:
            unverified += 1
            schema["unverified"] = f"no committed capture in sdv-internal-refs/{STEM}/captures (see ENDPOINTS.md)"
            schema["columns"] = []
        else:
            schema["columns"] = columns_from_frame(
                parse_capture(capture, parse_openligadb), descriptions, leaf_fallback=False
            )
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints, {unverified} unverified")


if __name__ == "__main__":
    main()
