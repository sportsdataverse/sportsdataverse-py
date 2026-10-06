"""Generate the ``fotmob`` flat-API endpoint YAML + returns-schemas from the FotMob
OpenAPI spec (``www.fotmob.com/api/data``, unofficial, keyless as of 2026-10-05).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_asa.py`` / ``gen_mls.py``, with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen spec + captures from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/fotmob.yaml`` -- one endpoint per GET route (14). Every
  route is query-parameterised; the build asserts no path token is named ``{league}``
  / ``{sport}``, which the renderer would substitute as an ESPN slug and blank.
  ``/api/trendingnews`` sits at the host root and carries its own ``servers`` in the
  spec, so it gets an endpoint-level ``host`` override (the ``gen_mls.py`` mechanism).
* ``tools/codegen/schemas/native/fotmob/<short>.yaml`` -- returns-schema per route,
  columns from running :func:`parse_fotmob` over the committed capture. The spec is
  capture-derived and carries no property docs, so descriptions resolve through the
  shared tiers and otherwise stay empty (the ``native/fotmob`` deferred bucket).

The spec's ``required`` flags came from a live drop-one probe; flat ``extra_params``
are optional kwargs, so the flag is written into the description as ``Required.``.

Run: ``python tools/codegen/gen_fotmob.py``
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

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
from sportsdataverse.soccer.fotmob_parsers import parse_fotmob

HOST = "https://www.fotmob.com/api/data"
SPEC = "fotmob.openapi.yaml"
STEM = "fotmob"
MODULE = "fotmob"
NAME_PATTERN = "fotmob_{short}"
PARSER_MODULE = "soccer.fotmob_parsers"
PARSER = "parse_fotmob"

_TOKEN = re.compile(r"\{([^}]+)\}")

# Shorts the slug rule would get wrong: the one host-root route and the mouthful.
_SHORT_OVERRIDES = {
    "/api/trendingnews": "trending_news",
    "/team-of-the-week/rounds": "totw_rounds",
}

# The spec's ``summary`` is just the route name; these are the wrappers' one-liners.
_SUMMARIES = {
    "all_leagues": "Every league FotMob covers, grouped as popular / international / by country.",
    "audio_matches": "Matches with audio commentary available, with their languages.",
    "leagues": "League page: details, tabs, current table, fixtures, stats, transfers (one wide row).",
    "match_details": "Match page: general info, header, lineups, events, stats (one wide row).",
    "matches": "All matches on a date, one row per league with that league's matches nested.",
    "player_data": "Player page: bio, primary team, career history, recent matches, stats (one wide row).",
    "search_suggest": "Search suggestions for a term, one row per result group (players, teams, leagues).",
    "table": "A data.fotmob.com league table file: legend, filters and the all/home/away/xg tables.",
    "teams": "Team page: details, squad, table, fixtures, stats, transfers, history (one wide row).",
    "tlnews": "News timeline for a league or team, one row per story.",
    "top_transfers": "Top transfers across FotMob, one row per transfer.",
    "totw_rounds": "Team-of-the-week rounds for a league season, one row per round.",
    "trending_news": "Trending news stories (host-root route), one row per story.",
    "tvlistings": "TV listings for a country, one row per broadcast with the match id in ``id``.",
}


def _short_from_path(path: str) -> str:
    """``/allLeagues`` -> ``all_leagues``, ``/search/suggest`` -> ``search_suggest``."""
    if path in _SHORT_OVERRIDES:
        return _SHORT_OVERRIDES[path]
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", path) if t]
    return "_".join(underscore(t) for t in tokens)


def _capture_for(refs: Path, path: str) -> Optional[Path]:
    """The reference repo's slug rule (``tools/capture.py``): strip ``/`` + ``api/``, ``/`` -> ``__``."""
    candidate = refs / "captures" / (path.strip("/").replace("api/", "").replace("/", "__") + ".json")
    return candidate if candidate.exists() else None


def _query_params(op: dict) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if prm.get("in") != "query":
            continue
        wire = prm["name"]
        desc = str(prm.get("description") or "").strip()
        if prm.get("required"):
            desc = f"Required. {desc}".strip()
        out.append({"name": underscore(wire), "query_key": wire, "type": "str", "description": desc})
    return out


def _example_args(op: dict) -> Dict[str, str]:
    return {underscore(prm["name"]): str(prm["example"]) for prm in op.get("parameters", []) if "example" in prm}


def _endpoint_entry(spec: dict, path: str, op: dict) -> Dict[str, Any]:
    short = _short_from_path(path)
    path_names = {prm["name"] for prm in op.get("parameters", []) if prm.get("in") == "path"}
    assert not {"league", "sport"} & path_names, path
    assert set(_TOKEN.findall(path)) == path_names, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": _SUMMARIES.get(short) or op.get("summary") or f"GET {path}",
        # the renderer substitutes a token by the python argument of the same name
        "path": _TOKEN.sub(lambda m: "{" + underscore(m.group(1)) + "}", path),
        "parser": PARSER,
        "returns_schema": f"native/{STEM}/{short}",
    }
    servers = spec["paths"][path].get("servers") or op.get("servers")
    if servers and servers[0]["url"] != HOST:
        entry["host"] = servers[0]["url"]
    if path_names:
        entry["path_params"] = [{"name": underscore(t), "type": "str", "required": True} for t in sorted(path_names)]
    qps = _query_params(op)
    if qps:
        entry["extra_params"] = qps
    example = _example_args(op)
    if example:
        entry["example_args"] = example
    return entry


def main() -> None:
    refs = refs_dir(STEM, SPEC)
    spec = load_spec(refs, SPEC)
    ops = sorted(get_ops(spec), key=lambda pv: _short_from_path(pv[0]))
    shorts = [_short_from_path(p) for p, _ in ops]
    dupes = sorted({s for s in shorts if shorts.count(s) > 1})
    if dupes:
        raise SystemExit(f"duplicate {STEM} shorts: {dupes}")

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": NAME_PATTERN,
        "module": MODULE,
        "parser_module": PARSER_MODULE,
        "qualifier": "",
        "passthrough_query": False,
        "docstring": {
            "example_import": True,
            "raw_doc": (
                "the decoded JSON body (a page object, a one-list envelope, an id-keyed map or "
                "a list; ``{}`` or ``null`` for an unknown id)"
            ),
            "raises": [
                "sportsdataverse.errors.NoDataError: FotMob answered HTTP 404 (route withdrawn).",
            ],
            "see_also": [
                {
                    "name": "FotMob",
                    "url": "https://www.fotmob.com/",
                    "note": "the site these routes render; unofficial JSON, keyless as of 2026-10-05",
                },
                {
                    "name": "iamglitch404/fotmob",
                    "url": "https://github.com/iamglitch404/fotmob",
                    "note": "community route catalogue the capture set was seeded from",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(spec, p, op) for p, op in ops],
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    descriptions = [spec_descriptions(spec), markdown_descriptions(refs / f"{STEM}-returns.md")]
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    for path, _op in ops:
        short = _short_from_path(path)
        capture = _capture_for(refs, path)
        schema: Dict[str, Any] = {"schema": short, "kind": "dataframe"}
        if capture is None:
            schema["unverified"] = f"no committed capture in sdv-internal-refs/{STEM}/captures (see its ENDPOINTS.md)"
        schema["columns"] = columns_from_frame(parse_capture(capture, parse_fotmob), descriptions)
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints")


if __name__ == "__main__":
    main()
