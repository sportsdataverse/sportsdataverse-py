"""Generate the ``polymarket`` flat-API endpoint YAML + returns-schemas from the two
frozen Polymarket OpenAPI specs (``gamma-api.polymarket.com`` + ``clob.polymarket.com``,
both keyless, read-only).

Idempotent: same specs + same captures -> byte-identical output. Modeled on
``gen_sleeper.py``, with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen specs and the committed captures from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/polymarket.yaml`` -- one endpoint per GET route (8),
  each carrying a per-endpoint ``host:`` when it differs from the document
  default (the ``uefa.yaml`` pattern).
* ``tools/codegen/schemas/native/polymarket/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running :func:`parse_polymarket` over the
  committed capture; a route without a capture is marked ``unverified``.

Run: ``python tools/codegen/gen_polymarket.py``
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
from sportsdataverse.odds.polymarket_parsers import parse_polymarket

HOST = "https://gamma-api.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
SPEC = "polymarket-gamma.openapi.yaml"
CLOB_SPEC = "polymarket-clob.openapi.yaml"
STEM = "polymarket"

# (spec filename, host, capture prefix, {path: short}). Both hosts serve /markets,
# which is why refs needed two OpenAPI documents; sdv-py keys on `short` and
# overrides `host` per endpoint (the uefa.yaml pattern), so it is one family.
_SURFACES = [
    (
        SPEC,
        GAMMA_HOST,
        "gamma-api",
        {
            "/markets": "gamma_markets",
            "/markets/{id}": "gamma_market",
            "/events": "gamma_events",
            "/tags": "gamma_tags",
        },
    ),
    (
        CLOB_SPEC,
        CLOB_HOST,
        "clob",
        {
            "/markets": "clob_markets",
            "/book": "clob_book",
            "/midpoint": "clob_midpoint",
            "/price": "clob_price",
        },
    ),
]


# Curated prose summaries. The reverse-engineered specs carry only "<host><path>" as
# their summary, and these lines are shown verbatim on the reference index and in IDE
# hover -- the FIFA lesson from the wave-1 review (tests/codegen/test_intake_family_summaries.py).
_SUMMARIES = {
    "gamma_markets": "Markets on the Gamma metadata host, orderable by volume or liquidity.",
    "gamma_market": "One market by its Gamma id.",
    "gamma_events": "Events -- groups of related markets -- on the Gamma metadata host.",
    "gamma_tags": "Tags used to categorise Gamma events and markets.",
    "clob_markets": "Markets on the CLOB host, cursor-paged.",
    "clob_book": "Full order book, bids and asks, for one outcome token.",
    "clob_midpoint": "Midpoint between the best bid and the best ask for one outcome token.",
    "clob_price": "Best price on one side of the book for one outcome token.",
}


def _capture_for(refs: Path, prefix: str, path: str) -> Path | None:
    """``clob`` + ``/markets`` -> ``captures/clob__markets.json``."""
    slug = path.strip("/").replace("/", "__").replace("{", "").replace("}", "")
    candidate = refs / "captures" / f"{prefix}__{slug}.json"
    return candidate if candidate.exists() else None


def _endpoint_entry(path: str, op: dict, short: str, host: str) -> Dict[str, Any]:
    path_params: List[Dict[str, Any]] = []
    extra_params: List[Dict[str, Any]] = []
    example_args: Dict[str, Any] = {}
    for prm in op.get("parameters", []):
        name = prm["name"]
        desc = str(prm.get("description") or "").strip()
        if prm.get("in") == "path":
            # Every one of these specs documents its path params; dropping the text makes the
            # reference page print "<name> path parameter." filler instead.
            path_params.append({"name": name, "type": "str", "required": True, "description": desc})
        elif prm.get("in") == "query":
            if prm.get("required"):
                desc = f"Required. {desc}".strip()
            extra_params.append(
                {
                    "name": underscore(name),
                    "query_key": name,
                    "type": "str",
                    # The flag drives the page's Required column AND whether the generated
                    # signature makes the param positional. Prose alone leaves a required param
                    # defaulting to None, so the call goes out without it and fetches an error
                    # body where it should have been a TypeError.
                    "required": bool(prm.get("required")),
                    "description": desc,
                }
            )
        if prm.get("example") is not None:
            # A 77-digit token_id must never travel as an int.
            example_args[underscore(name)] = str(prm["example"])
    # Review Focus 1: the module renderer substitutes {league}/{sport} as ESPN slugs.
    assert not {"league", "sport"} & {p["name"] for p in path_params}, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": _SUMMARIES[short],
        "path": path,
        "parser": "parse_polymarket",
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": example_args,
    }
    if host != HOST:
        entry["host"] = host
    if path_params:
        entry["path_params"] = path_params
    if extra_params:
        entry["extra_params"] = extra_params
    return entry


def main() -> None:
    refs = refs_dir(STEM, SPEC)
    entries: List[Dict[str, Any]] = []
    schema_jobs: List[tuple] = []
    descriptions: List[Dict[str, str]] = []
    for spec_file, host, prefix, shorts in _SURFACES:
        spec = load_spec(refs, spec_file)
        descriptions.append(spec_descriptions(spec))
        ops = get_ops(spec)
        missing = sorted(set(p for p, _ in ops) - set(shorts))
        if missing:
            raise SystemExit(f"polymarket: {spec_file} paths without a short: {missing}")
        ops.sort(key=lambda pv: shorts[pv[0]])
        for path, op in ops:
            short = shorts[path]
            entries.append(_endpoint_entry(path, op, short, host))
            schema_jobs.append((short, _capture_for(refs, prefix, path)))

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "polymarket_{short}",
        "module": STEM,
        "parser_module": "odds.polymarket_parsers",
        "qualifier": "",
        "passthrough_query": False,
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: Polymarket answered 404 (unknown market id or token id).",
            ],
            "see_also": [
                {
                    "name": "Polymarket docs",
                    "url": "https://docs.polymarket.com/",
                    "note": (
                        "gamma = market/event/tag metadata, clob = order books and prices; "
                        "reads are keyless, trading needs keys"
                    ),
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": entries,
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    descriptions.append(markdown_descriptions(refs / f"{STEM}-returns.md"))
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    unverified = 0
    for short, capture in schema_jobs:
        schema: Dict[str, Any] = {"schema": short, "kind": "dataframe"}
        if capture is None:
            unverified += 1
            schema["unverified"] = f"no committed capture in sdv-internal-refs/{STEM}/captures (see ENDPOINTS.md)"
            schema["columns"] = []
        else:
            schema["columns"] = columns_from_frame(
                parse_capture(capture, parse_polymarket), descriptions, leaf_fallback=False
            )
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(entries)} endpoints, {unverified} unverified")


if __name__ == "__main__":
    main()
