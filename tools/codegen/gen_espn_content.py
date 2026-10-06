"""Generate the ``espn_content`` flat-API endpoint YAML + returns-schemas from the
ESPN content.core v1 OpenAPI spec (``content.core.api.espn.com/v1``, keyless, read-only).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_sleeper.py``, with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen spec and the committed captures from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/espn_content.yaml`` -- one endpoint per GET route (3).
* ``tools/codegen/schemas/native/espn_content/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running :func:`parse_espn_content` over the
  committed capture; a route without a capture is marked ``unverified``.

Run: ``python tools/codegen/gen_espn_content.py``
"""

from __future__ import annotations

import re
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
from sportsdataverse.espn_content.espn_content_parsers import parse_espn_content

HOST = "https://content.core.api.espn.com/v1"
SPEC = "espn-content.openapi.yaml"
STEM = "espn_content"

_TOKEN = re.compile(r"\{(\w+)\}")

# Wrapper short per spec path. Every spec path must be listed (asserted below) so
# a route added upstream fails loudly instead of inventing a name.
_SHORTS: Dict[str, str] = {
    "/sports/news": "news",
    "/sports/news/{id}": "story",
    "/sports/{sport}/{league}/news": "league_news",
}

# Review Focus 1: {sport}/{league} are ESPN league slugs to the module renderer -- it
# substitutes them and blanks the segment (spec.py:304 excludes them from token
# validation). Emitted as plain params instead, the gen_asa.py LEAGUE_PARAM lesson.
_SLUG_RENAMES = {"sport": "sport_slug", "league": "league_slug"}


def _capture_for(refs: Path, path: str) -> Path | None:
    """``capture.py``'s slug: ``/sports/{sport}/{league}/news`` -> ``sports__sport__league__news``."""
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
            example_args[underscore(name)] = str(prm["example"])
    assert not {"league", "sport"} & {p["name"] for p in path_params}, path
    assert "{league}" not in out_path and "{sport}" not in out_path, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (op.get("summary") or f"Fetch {path}").rstrip(".") + ".",
        "path": out_path,
        "parser": "parse_espn_content",
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": example_args,
    }
    if path_params:
        entry["path_params"] = path_params
    if extra_params:
        entry["extra_params"] = extra_params
    return entry


def main() -> None:
    refs = refs_dir("espn-content", SPEC)
    spec = load_spec(refs, SPEC)
    ops = get_ops(spec)
    missing = sorted(set(p for p, _ in ops) - set(_SHORTS))
    if missing:
        raise SystemExit(f"espn_content: spec paths without a short: {missing}")
    ops.sort(key=lambda pv: _SHORTS[pv[0]])

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "espn_content_{short}",
        "module": STEM,
        "parser_module": "espn_content.espn_content_parsers",
        "qualifier": "",
        "passthrough_query": False,
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: content.core answered 404 (unknown story id, sport or league slug).",
            ],
            "see_also": [
                {
                    "name": "ESPN content.core endpoints",
                    "url": "https://content.core.api.espn.com/v1/sports/news",
                    "note": "keyless; list routes page with limit/offset, and team=/athlete= filters are ignored by the host",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    descriptions = [spec_descriptions(spec), markdown_descriptions(refs / "espn-content-returns.md")]
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    unverified = 0
    for path, _op in ops:
        short = _SHORTS[path]
        capture = _capture_for(refs, path)
        schema: Dict[str, Any] = {"schema": short, "kind": "dataframe"}
        if capture is None:
            unverified += 1
            schema["unverified"] = "no committed capture in sdv-internal-refs/espn-content/captures (see ENDPOINTS.md)"
            schema["columns"] = []
        else:
            schema["columns"] = columns_from_frame(
                parse_capture(capture, parse_espn_content), descriptions, leaf_fallback=False
            )
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints, {unverified} unverified")


if __name__ == "__main__":
    main()
