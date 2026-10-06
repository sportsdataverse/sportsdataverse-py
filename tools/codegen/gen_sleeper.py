"""Generate the ``sleeper`` flat-API endpoint YAML + returns-schemas from the Sleeper
API v1 OpenAPI spec (``api.sleeper.app/v1``, keyless, read-only).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_asa.py``, with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen spec and the committed captures from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/sleeper.yaml`` -- one endpoint per GET route (15).
* ``tools/codegen/schemas/native/sleeper/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running :func:`parse_sleeper` over the committed
  capture; a route without a capture is marked ``unverified``.

Run: ``python tools/codegen/gen_sleeper.py``
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
from sportsdataverse.nfl.sleeper_parsers import parse_sleeper

HOST = "https://api.sleeper.app/v1"
SPEC = "sleeper.openapi.yaml"
STEM = "sleeper"

_TOKEN = re.compile(r"\{(\w+)\}")

# Wrapper short per spec path. Every spec path must be listed (asserted below) so
# a route added upstream fails loudly instead of inventing a name.
_SHORTS: Dict[str, str] = {
    "/user/{username}": "user",
    "/user/{user_id}/leagues/nfl/{season}": "user_leagues",
    "/league/{league_id}": "league",
    "/league/{league_id}/rosters": "rosters",
    "/league/{league_id}/users": "users",
    "/league/{league_id}/matchups/{week}": "matchups",
    "/league/{league_id}/winners_bracket": "winners_bracket",
    "/league/{league_id}/transactions/{week}": "transactions",
    "/league/{league_id}/traded_picks": "traded_picks",
    "/league/{league_id}/drafts": "drafts",
    "/draft/{draft_id}": "draft",
    "/draft/{draft_id}/picks": "draft_picks",
    "/state/nfl": "state",
    "/players/nfl": "players",
    "/players/nfl/trending/add": "trending_adds",
}


def _capture_for(refs: Path, path: str) -> Path | None:
    """``capture.py``'s slug: ``/league/{league_id}/matchups/{week}`` -> ``league__league_id__matchups__week``."""
    slug = path.strip("/").replace("/", "__").replace("{", "").replace("}", "")
    candidate = refs / "captures" / f"{slug}.json"
    return candidate if candidate.exists() else None


def _endpoint_entry(path: str, op: dict) -> Dict[str, Any]:
    short = _SHORTS[path]
    path_params: List[Dict[str, Any]] = []
    extra_params: List[Dict[str, Any]] = []
    example_args: Dict[str, Any] = {}
    for prm in op.get("parameters", []):
        name = prm["name"]
        desc = str(prm.get("description") or "").strip()
        if prm.get("in") == "path":
            path_params.append({"name": name, "type": "str", "required": True})
        elif prm.get("in") == "query":
            if prm.get("required"):
                desc = f"Required. {desc}".strip()
            extra_params.append({"name": underscore(name), "query_key": name, "type": "str", "description": desc})
        if prm.get("example") is not None:
            # Every param is a string on the wire; 18-digit ids must never travel as ints.
            example_args[underscore(name)] = str(prm["example"])
    # Review Focus 4: the module renderer substitutes {league}/{sport} as ESPN slugs.
    assert not {"league", "sport"} & {p["name"] for p in path_params}, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (op.get("summary") or f"Fetch {path}").rstrip(".") + ".",
        "path": path,
        "parser": "parse_sleeper",
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
        raise SystemExit(f"sleeper: spec paths without a short: {missing}")
    ops.sort(key=lambda pv: _SHORTS[pv[0]])

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "sleeper_{short}",
        "module": STEM,
        "parser_module": "nfl.sleeper_parsers",
        "qualifier": "",
        "passthrough_query": False,
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: the Sleeper API returned 404 (unknown user, league or draft id).",
            ],
            "see_also": [
                {
                    "name": "Sleeper API docs",
                    "url": "https://docs.sleeper.com/",
                    "note": "official route reference (keyless, read-only, ~1000 req/min; fetch /players/nfl at most once a day)",
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
                parse_capture(capture, parse_sleeper), descriptions, leaf_fallback=False
            )
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints, {unverified} unverified")


if __name__ == "__main__":
    main()
