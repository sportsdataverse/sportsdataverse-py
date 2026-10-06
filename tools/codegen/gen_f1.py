"""Generate the ``f1`` flat-API endpoint YAML + returns-schemas from the Jolpica F1
(Ergast-compatible) recon in ``sdv-internal-refs/f1/``.

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_euroleague.py`` (#704), with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen spec (``f1-jolpica.openapi.yaml``, 15 ``.json`` routes sharing the
``MRData`` envelope), the committed 3-row captures and the flattened returns tables
(``f1-returns.md``) from the ``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``,
else the sibling checkout) and emits:

* ``tools/codegen/endpoints/f1.yaml`` -- one endpoint per route, host
  ``https://api.jolpi.ca/ergast/f1``, the shared keyless ``_get``, parser
  ``parse_f1_mrdata``; every route takes the ``limit`` / ``offset`` paging params.
* ``tools/codegen/schemas/native/f1/<short>.yaml`` -- returns-schema per endpoint,
  columns taken from running the parser over the committed capture, descriptions
  from the route's own ``f1-returns.md`` table.

The laps route is generated as ``laps_page`` (one page, max 100 timings): the
paging loop that fetches a whole race is the hand-written
``sportsdataverse.f1.f1_extra.f1_laps``.

Wrapper names mirror f1dataR (``load_results`` -> ``f1_results``, ``load_quali`` ->
``f1_qualifying``, ``load_standings(type=)`` -> ``f1_driver_standings`` /
``f1_constructor_standings``); the two extra spec routes are ``f1_race`` (one race of
the schedule) and ``f1_driver`` (one driver by id).

Run: ``python tools/codegen/gen_f1.py``
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
from gen_soccer_common import (
    ROOT,
    columns_from_frame,
    get_ops,
    load_spec,
    parse_capture,
    refs_dir,
    rewrite_schema_dir,
    write_yaml,
)

from sportsdataverse.dl_utils import underscore
from sportsdataverse.f1.f1_parsers import parse_f1_mrdata

HOST = "https://api.jolpi.ca/ergast/f1"
SPEC = "f1-jolpica.openapi.yaml"
STEM = "f1"

TERMS = (
    "Data license CC BY-NC-SA 4.0 (non-commercial, attribution, share-alike): sdv-py wraps the "
    "API on the caller's behalf and never republishes Jolpica payloads as release assets."
)
RATE = (
    "Rate limit 4 requests/second burst, 500 requests/hour sustained, unauthenticated, per IP "
    "and shared by every process on the machine."
)

# Wrapper short per spec path (f1dataR names where one exists).
_SHORTS: Dict[str, str] = {
    "/seasons.json": "seasons",
    "/{season}.json": "schedule",
    "/{season}/{round}.json": "race",
    "/{season}/{round}/results.json": "results",
    "/{season}/{round}/qualifying.json": "qualifying",
    "/{season}/{round}/sprint.json": "sprint",
    "/{season}/{round}/laps.json": "laps_page",
    "/{season}/{round}/pitstops.json": "pitstops",
    "/{season}/driverStandings.json": "driver_standings",
    "/{season}/constructorStandings.json": "constructor_standings",
    "/{season}/drivers.json": "drivers",
    "/{season}/constructors.json": "constructors",
    "/circuits.json": "circuits",
    "/status.json": "status",
    "/drivers/{driverId}.json": "driver",
}

# The committed samples: season 2024, round 1 (round 5 = China for the sprint, the
# first 2024 sprint weekend), driver max_verstappen. ``current`` is a second
# schedule sample whose later rounds carry the sprint-weekend columns.
_CAPTURE_VALUES = {"season": "2024", "round": "1", "driverId": "max_verstappen"}
_SPRINT_ROUND = "5"
_EXTRA_CAPTURES = {"/{season}.json": ["current"]}

# Live-verified example arguments for the docs.
_EXAMPLE: Dict[str, Any] = {"season": 2024, "round": 1, "driver_id": "max_verstappen"}

_TOKEN = re.compile(r"\{([^}]+)\}")
_TABLE_ROW = re.compile(r"^\s*\|(.+)\|\s*$")


def _short(path: str) -> str:
    try:
        return _SHORTS[path]
    except KeyError:
        raise SystemExit(f"{STEM}: no wrapper short for spec path {path}; add it to _SHORTS") from None


def _snake_path(path: str) -> str:
    return _TOKEN.sub(lambda m: "{" + underscore(m.group(1)) + "}", path)


def _capture_for(refs: Path, path: str) -> List[Path]:
    """The committed capture(s) for ``path``: ``capture.py``'s slug rule over the pinned values."""
    values = dict(_CAPTURE_VALUES)
    if path.endswith("/sprint.json"):
        values["round"] = _SPRINT_ROUND
    route = _TOKEN.sub(lambda m: values[m.group(1)], path)
    slugs = [route.strip("/").removesuffix(".json").replace("/", "__")] + _EXTRA_CAPTURES.get(path, [])
    return [refs / "captures" / f"{slug}.json" for slug in slugs]


def route_descriptions(md: Path) -> Dict[str, Dict[str, str]]:
    """``{spec_path: {column: description}}`` from the per-route tables in ``f1-returns.md``.

    The reference repo scopes one table per route (``## `/{season}/{round}/results.json```),
    so a column reused across routes (``time``, ``url``) keeps its route-specific prose.
    """
    out: Dict[str, Dict[str, str]] = {}
    current: Dict[str, str] = {}
    for line in md.read_text(encoding="utf-8").splitlines():
        head = re.match(r"^## `([^`]+)`", line)
        if head:
            current = out.setdefault(head.group(1), {})
            continue
        row = _TABLE_ROW.match(line)
        if not row:
            continue
        cells = [c.strip() for c in row.group(1).split("|")]
        name = cells[0].strip("`")
        if len(cells) >= 3 and name and name != "col_name" and not set(name) <= set("-:"):
            current.setdefault(name, cells[-1])
    return out


def _path_params(op: dict) -> List[Dict[str, Any]]:
    return [
        {
            "name": underscore(prm["name"]),
            "type": "str",
            "required": True,
            "description": str(prm.get("description") or "").strip(),
        }
        for prm in op.get("parameters", [])
        if prm.get("in") == "path"
    ]


def _query_params(op: dict) -> List[Dict[str, Any]]:
    return [
        {
            "name": underscore(prm["name"]),
            "query_key": prm["name"],
            "type": "str",
            "description": str(prm.get("description") or "").strip(),
        }
        for prm in op.get("parameters", [])
        if prm.get("in") == "query"
    ]


def _endpoint_entry(path: str, op: dict) -> Dict[str, Any]:
    short = _short(path)
    pps = _path_params(op)
    names = {p["name"] for p in pps}
    assert not {"league", "sport"} & names, path
    assert {underscore(t) for t in _TOKEN.findall(path)} == names, path
    example = {k: v for k, v in _EXAMPLE.items() if k in names}
    if short == "sprint":
        example["round"] = int(_SPRINT_ROUND)
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (op.get("summary") or f"Fetch {path}").rstrip(".") + ".",
        "path": _snake_path(path),
        "parser": "parse_f1_mrdata",
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": example,
    }
    if pps:
        entry["path_params"] = pps
    qps = _query_params(op)
    if qps:
        entry["extra_params"] = qps
    return entry


def main() -> None:
    refs = refs_dir(STEM, SPEC)
    spec = load_spec(refs, SPEC)
    ops = sorted(get_ops(spec), key=lambda pv: _short(pv[0]))
    shorts = [_short(p) for p, _ in ops]
    dupes = sorted({s for s in shorts if shorts.count(s) > 1})
    if dupes:
        raise SystemExit(f"duplicate {STEM} shorts: {dupes}")

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "f1_{short}",
        "module": STEM,
        "parser_module": "f1.f1_parsers",
        "qualifier": "",
        "passthrough_query": False,
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: the Jolpica API returned 404 (unknown season, round or driver id).",
            ],
            "notes": [
                TERMS,
                RATE + " Paging: ``limit`` is capped at 100 (a larger value is clamped and echoed in "
                "``MRData.limit``); ``offset`` pages the innermost rows counted by ``MRData.total``.",
                "``season`` accepts ``current``; ``round`` accepts ``last`` / ``next``. Paths are case-insensitive.",
            ],
            "see_also": [
                {
                    "name": "Jolpica F1 API",
                    "url": "https://github.com/jolpica/jolpica-f1",
                    "note": "the Ergast-compatible API this family wraps (docs, terms, rate limit)",
                },
                {
                    "name": "f1dataR",
                    "url": "https://scottyd22.github.io/f1dataR/",
                    "note": "the R twin over the same API; wrapper names and columns follow it",
                },
                {
                    "name": "FastF1",
                    "url": "https://docs.fastf1.dev/",
                    "note": "timing, telemetry and session data that sdv-py does not wrap",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    by_route = route_descriptions(refs / f"{STEM}-returns.md")
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    for path, _op in ops:
        short = _short(path)
        captures = [c for c in _capture_for(refs, path) if c.exists()]
        schema: Dict[str, Any] = {"schema": short, "kind": "dataframe"}
        if captures:
            frames = [parse_capture(c, parse_f1_mrdata) for c in captures]
            # the widest sample leads so its (sorted) column order is the documented one
            frames.sort(key=lambda f: -f.width)
            df = pl.concat(frames, how="diagonal_relaxed") if len(frames) > 1 else frames[0]
            schema["columns"] = columns_from_frame(df, [by_route.get(path, {})], leaf_fallback=False)
            for col in schema["columns"]:  # the parser casts these; the wire prose says "as a string"
                if col["type"] != "character":
                    col["description"] = re.sub(r" (on the wire )?as a string", "", col["description"])
            blank = [c["name"] for c in schema["columns"] if not c["description"]]
            if blank:
                raise SystemExit(f"{STEM}/{short}: columns missing from {STEM}-returns.md: {blank}")
        else:
            schema["columns"] = []
            schema["unverified"] = f"no committed capture in sdv-internal-refs/{STEM}/captures ({path})"
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints")


if __name__ == "__main__":
    main()
