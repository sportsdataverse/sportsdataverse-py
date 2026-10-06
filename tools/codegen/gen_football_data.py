"""Generate the ``football_data`` flat-API endpoint YAML + returns-schemas from the
frozen Football-Data.co.uk OpenAPI spec (``www.football-data.co.uk``, keyless).

football-data.co.uk is a static **CSV archive**, not a JSON API: every wrapped
route answers ``text/csv``, so the family points ``getter_module`` at
``sportsdataverse.soccer.football_data_runtime`` (raw text for a non-JSON body)
and declares ``raw_types: [Dict, str]`` so the ``return_parsed=False`` signature
is honest about the body it hands back.

Three routes are wrapped -- one league-season of results plus closing odds
(``/mmz4281/{season}/{div}.csv``, 132 columns), all seasons of one 'extra'
league (``/new/{country}.csv``, 25) and the upcoming-fixtures file
(``/fixtures.csv``, 94). The spec's fourth path, ``/notes.txt``, is the column
glossary rather than data; it is transcribed into
``football-data-co-uk-returns.md`` and deliberately **not** wrapped.

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_sleeper.py``, with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen spec and the committed captures from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/football_data.yaml`` -- one endpoint per wrapped route (3).
* ``tools/codegen/schemas/native/football_data/<short>.yaml`` -- returns-schema
  per endpoint, columns taken from running :func:`parse_football_data` over the
  committed CSV capture; a route without a capture is marked ``unverified``.

Run: ``python tools/codegen/gen_football_data.py``
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import polars as pl
from gen_soccer_common import (
    ROOT,
    columns_from_frame,
    get_ops,
    load_spec,
    markdown_descriptions,
    refs_dir,
    rewrite_schema_dir,
    spec_descriptions,
    write_yaml,
)

from sportsdataverse.dl_utils import underscore
from sportsdataverse.soccer.football_data_parsers import parse_football_data

HOST = "https://www.football-data.co.uk"
SPEC = "football-data-co-uk.openapi.yaml"
STEM = "football_data"

# Wrapper short per spec path. Every wrapped spec path must be listed (asserted
# below) so a file added upstream fails loudly instead of inventing a name.
_SHORTS: Dict[str, str] = {
    "/mmz4281/{season}/{div}.csv": "league_season",
    "/new/{country}.csv": "extra_league",
    "/fixtures.csv": "fixtures",
}

# /notes.txt is the column glossary, not data: it is transcribed into
# football-data-co-uk-returns.md and deliberately not wrapped.
_NOT_WRAPPED = frozenset({"/notes.txt"})


def _capture_for(refs: Path, path: str) -> Path | None:
    """``/mmz4281/{season}/{div}.csv`` -> ``captures/mmz4281__season__div.csv``."""
    slug = path.strip("/").removesuffix(".csv").replace("/", "__").replace("{", "").replace("}", "")
    candidate = refs / "captures" / f"{slug}.csv"
    return candidate if candidate.exists() else None


def _frame_for(capture: Path | None) -> pl.DataFrame:
    """Parse a committed CSV capture; an empty frame when there is none.

    ``parse_capture`` cannot be reused here: it does ``json.loads`` and this
    family has no JSON body anywhere.
    """
    if capture is None:
        return pl.DataFrame()
    return parse_football_data(capture.read_text(encoding="utf-8-sig"))


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
            # Every param is a string on the wire; a season like 2526 must not travel as an int.
            example_args[underscore(name)] = str(prm["example"])
    # Review Focus 1: the module renderer substitutes {league}/{sport} as ESPN slugs.
    assert not {"league", "sport"} & {p["name"] for p in path_params}, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (op.get("summary") or f"Fetch {path}").rstrip(".") + ".",
        "path": path,
        "parser": "parse_football_data",
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": example_args,
    }
    if path_params:
        entry["path_params"] = path_params
    if extra_params:
        entry["extra_params"] = extra_params
    return entry


def main() -> None:
    refs = refs_dir("football-data-co-uk", SPEC)
    spec = load_spec(refs, SPEC)
    ops = [(p, op) for p, op in get_ops(spec) if p not in _NOT_WRAPPED]
    missing = sorted(set(p for p, _ in ops) - set(_SHORTS))
    if missing:
        raise SystemExit(f"football_data: spec paths without a short: {missing}")
    ops.sort(key=lambda pv: _SHORTS[pv[0]])

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "football_data_{short}",
        "module": STEM,
        "parser_module": "soccer.football_data_parsers",
        "getter_module": "sportsdataverse.soccer.football_data_runtime",
        "qualifier": "",
        "passthrough_query": False,
        # The bodies are text/csv: football_data_runtime._get hands back the raw
        # response TEXT, so the return_parsed=False signature must allow str.
        "raw_types": ["Dict", "str"],
        "docstring": {
            "raw_doc": "the raw CSV response body (``str``)",
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: football-data.co.uk answered 404 (no file for that season or division).",
            ],
            "see_also": [
                {
                    "name": "Football-Data.co.uk",
                    "url": "https://www.football-data.co.uk/data.php",
                    "note": "static CSV archive with a /notes.txt column glossary; keep live use to about one request per second",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    descriptions = [spec_descriptions(spec), markdown_descriptions(refs / "football-data-co-uk-returns.md")]
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    unverified = 0
    for path, _op in ops:
        short = _SHORTS[path]
        capture = _capture_for(refs, path)
        schema: Dict[str, Any] = {"schema": short, "kind": "dataframe"}
        if capture is None:
            unverified += 1
            schema["unverified"] = (
                "no committed capture in sdv-internal-refs/football-data-co-uk/captures (see ENDPOINTS.md)"
            )
            schema["columns"] = []
        else:
            schema["columns"] = columns_from_frame(_frame_for(capture), descriptions, leaf_fallback=False)
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints, {unverified} unverified")


if __name__ == "__main__":
    main()
