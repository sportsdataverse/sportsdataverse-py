"""Generate the ``asa`` flat-API endpoint YAML + returns-schemas from the American
Soccer Analysis OpenAPI spec (``app.americansocceranalysis.com/api/v1``, auth-free).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_on3.py``, with the shared plumbing in ``gen_soccer_common.py``.

The generator reads the frozen spec from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/asa.yaml`` -- one endpoint per GET route (15), host
  ``https://app.americansocceranalysis.com/api/v1``.
* ``tools/codegen/schemas/native/asa/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running the endpoint's parser over the committed
  capture and descriptions from the spec's property docs.

The spec's ``{league}`` path token is emitted as ``{league_slug}`` because the
module renderer substitutes ``{sport}``/``{league}`` as ESPN slugs, which would
blank the segment for this league-less flat API.

Run: ``python tools/codegen/gen_asa.py``
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
from sportsdataverse.soccer.asa_parsers import parse_asa, parse_asa_goals_added

HOST = "https://app.americansocceranalysis.com/api/v1"

# ASA's ``{league}`` is a real per-call argument, but the renderer treats a bare
# ``{league}`` token as an ESPN league slug and would substitute it away.
LEAGUE_PARAM = "league_slug"

# Capture directories, most-complete league first: the union of routes lives under
# mls, with nwsl covering the women's-only differences.
_CAPTURE_LEAGUES = ("mls", "nwsl", "uslc", "usl1", "mlsnp")

# Routes whose rows nest a per-action-type ``data[]`` breakdown.
_GOALS_ADDED_SUFFIX = "_goals_added"

_TOKEN = re.compile(r"\{([^}]+)\}")

# Representative example values for the docs (a live-verified league + season).
_EXAMPLE: Dict[str, Any] = {LEAGUE_PARAM: "mls"}


def _short_from_path(path: str) -> str:
    """``/{league}/players/goals-added`` -> ``players_goals_added``."""
    rest = _TOKEN.sub("", path)
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", rest) if t]
    return "_".join(underscore(t) for t in tokens)


def _parser_for(short: str) -> str:
    return "parse_asa_goals_added" if short.endswith(_GOALS_ADDED_SUFFIX) else "parse_asa"


def _capture_for(refs: Path, short: str) -> Path | None:
    """First committed capture for ``short`` across the league capture dirs."""
    stem = short.replace(_GOALS_ADDED_SUFFIX, "_goals-added")
    for league in _CAPTURE_LEAGUES:
        candidate = refs / "captures" / league / f"{stem}.json"
        if candidate.exists():
            return candidate
    return None


def _query_params(op: dict) -> List[Dict[str, Any]]:
    """Query params as ``extra_params`` entries (ASA wire keys are already snake)."""
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if prm.get("in") != "query":
            continue
        wire = prm["name"]
        out.append(
            {
                "name": underscore(wire),
                "query_key": wire,
                "type": "str",
                "description": str(prm.get("description") or "").strip(),
            },
        )
    return out


def _endpoint_entry(path: str, op: dict) -> Dict[str, Any]:
    short = _short_from_path(path)
    entry: Dict[str, Any] = {
        "short": short,
        "summary": op.get("summary") or f"GET {path}",
        "path": path.replace("{league}", "{" + LEAGUE_PARAM + "}"),
        "path_params": [{"name": LEAGUE_PARAM, "type": "str", "required": True}],
        "parser": _parser_for(short),
        "returns_schema": f"native/asa/{short}",
        "example_args": dict(_EXAMPLE),
    }
    qps = _query_params(op)
    if qps:
        entry["extra_params"] = qps
    return entry


def main() -> None:
    refs = refs_dir("asa", "asa.openapi.yaml")
    spec = load_spec(refs, "asa.openapi.yaml")
    ops = sorted(get_ops(spec), key=lambda pv: _short_from_path(pv[0]))
    shorts = [_short_from_path(p) for p, _ in ops]
    dupes = sorted({s for s in shorts if shorts.count(s) > 1})
    if dupes:
        raise SystemExit(f"duplicate asa shorts: {dupes}")

    doc = {
        "api": "asa",
        "host": HOST,
        "name_pattern": "asa_{short}",
        "module": "asa",
        "parser_module": "soccer.asa_parsers",
        "qualifier": "",
        "passthrough_query": True,
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoESPNDataError: the ASA API returned 404 "
                "(route not published for that league).",
                "requests.exceptions.RequestException: Connection-level failure after "
                "``dl_utils.download`` exhausts its retries.",
            ],
            "see_also": [
                {
                    "name": "itscalledsoccer",
                    "url": "https://github.com/American-Soccer-Analysis/itscalledsoccer",
                    "note": "official ASA R/Python client over the same API",
                },
                {
                    "name": "American Soccer Analysis",
                    "url": "https://www.americansocceranalysis.com/",
                    "note": "data origin (xG, g+, xPass models)",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/asa.yaml", doc)

    descriptions = [spec_descriptions(spec), markdown_descriptions(refs / "asa-returns.md")]
    schema_dir = ROOT / "tools/codegen/schemas/native/asa"
    rewrite_schema_dir(schema_dir)
    for path, _op in ops:
        short = _short_from_path(path)
        parser = parse_asa_goals_added if short.endswith(_GOALS_ADDED_SUFFIX) else parse_asa
        frame = parse_capture(_capture_for(refs, short), parser, primary="summary")
        write_yaml(
            schema_dir / f"{short}.yaml",
            {"schema": short, "kind": "dataframe", "columns": columns_from_frame(frame, descriptions)},
        )
    print(f"asa: {len(ops)} endpoints")


if __name__ == "__main__":
    main()
