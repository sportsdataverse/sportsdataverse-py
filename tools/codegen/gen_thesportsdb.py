"""Generate the ``thesportsdb`` flat-API endpoint YAML + returns-schemas from the
TheSportsDB API v1 OpenAPI spec (``thesportsdb.com/api/v1/json/{key}``, read-only).

The API key is a **path segment**, not a query parameter, so the emitted ``host``
keeps the literal ``{key}`` placeholder and the YAML points ``getter_module`` at
``sportsdataverse.thesportsdb.thesportsdb_runtime``, which substitutes the
caller's key (``$THESPORTSDB_API_KEY``, else the free test key ``3``) at call time.

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_sleeper.py``, with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen spec and the committed captures from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/thesportsdb.yaml`` -- one endpoint per GET route (12).
* ``tools/codegen/schemas/native/thesportsdb/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running :func:`parse_thesportsdb` over the
  committed capture; a route without a capture is marked ``unverified``.

Run: ``python tools/codegen/gen_thesportsdb.py``
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
from sportsdataverse.thesportsdb.thesportsdb_parsers import parse_thesportsdb

HOST = "https://www.thesportsdb.com/api/v1/json/{key}"
SPEC = "thesportsdb.openapi.yaml"
STEM = "thesportsdb"

# Wrapper short per spec path. Every spec path must be listed (asserted below) so
# a route added upstream fails loudly instead of inventing a name.
_SHORTS: Dict[str, str] = {
    "/all_sports.php": "sports",
    "/all_leagues.php": "leagues",
    "/lookupleague.php": "league",
    "/search_all_teams.php": "league_teams",
    "/lookupteam.php": "team",
    "/lookup_all_players.php": "team_players",
    "/searchplayers.php": "player_search",
    "/lookupplayer.php": "player",
    "/eventsseason.php": "season_events",
    "/lookupevent.php": "event",
    "/eventsnextleague.php": "league_next_events",
    "/lookuptable.php": "table",
}


def _capture_for(refs: Path, path: str) -> Path | None:
    """``/lookupteam.php`` -> ``captures/lookupteam.json``."""
    candidate = refs / "captures" / f"{path.strip('/').removesuffix('.php')}.json"
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
            # Every param is a string on the wire; long ids must never travel as ints.
            example_args[underscore(name)] = str(prm["example"])
    # Review Focus 1: the module renderer substitutes {league}/{sport} as ESPN slugs.
    assert not {"league", "sport"} & {p["name"] for p in path_params}, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (op.get("summary") or f"Fetch {path}").rstrip(".") + ".",
        "path": path,
        "parser": "parse_thesportsdb",
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
        "name_pattern": "thesportsdb_{short}",
        "module": STEM,
        "parser_module": "thesportsdb.thesportsdb_parsers",
        "getter_module": "sportsdataverse.thesportsdb.thesportsdb_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: TheSportsDB answered 404 (unknown id or route).",
            ],
            "see_also": [
                {
                    "name": "TheSportsDB API docs",
                    "url": "https://www.thesportsdb.com/free_sports_api",
                    "note": "free test key 3 is the default (30 req/min); set $THESPORTSDB_API_KEY to use your own",
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
                parse_capture(capture, parse_thesportsdb), descriptions, leaf_fallback=False
            )
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints, {unverified} unverified")


if __name__ == "__main__":
    main()
