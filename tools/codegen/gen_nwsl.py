"""Generate the ``nwsl_api`` flat-API endpoint YAML + returns-schemas from the
official NWSL (StatsPerform SDP) OpenAPI spec (``api-sdp.nwslsoccer.com``, auth-free).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_on3.py``, with the shared plumbing in ``gen_soccer_common.py``.

Emits:

* ``tools/codegen/endpoints/nwsl_api.yaml`` -- one endpoint per GET route (9).
  The ``/v1/nwsl/football`` prefix every route shares is folded into the family
  host, so the emitted paths are the route-specific tail.
* ``tools/codegen/schemas/native/nwsl_api/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running the endpoint's parser over the committed
  capture and descriptions from the spec plus ``nwsl-returns.md``.

``locale`` is marked required in the spec but is a constant in practice, so it is
emitted as an optional argument defaulting to ``"en-US"`` rather than as a
positional every caller would have to repeat.

Run: ``python tools/codegen/gen_nwsl.py``
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from gen_soccer_common import (
    ROOT,
    columns_from_frame,
    component_columns,
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
from sportsdataverse.soccer.nwsl.nwsl_api_parsers import (
    parse_nwsl_lineups,
    parse_nwsl_sdp,
    parse_nwsl_standings,
    parse_nwsl_stats,
)

# The ``/v1/nwsl/football`` base path is shared by every route, so it lives in the
# host and the emitted endpoint paths are the tails.
HOST = "https://api-sdp.nwslsoccer.com/v1/nwsl/football"
BASE_PATH = "/v1/nwsl/football"

_TOKEN = re.compile(r"\{([^}]+)\}")

# Curated per-route metadata:
#   path -> (short, parser, primary sub-frame, capture, spec component fallback)
# The fallback names a ``components.schemas`` entry whose properties supply the
# returns columns when the committed capture parses to nothing.
_ROUTES: Dict[str, tuple] = {
    "/v1/nwsl/football/competitions": ("competitions", "parse_nwsl_sdp", None, "sdp_competitions.json", None),
    "/v1/nwsl/football/seasons/multipleSeasonMatches": (
        "season_matches",
        "parse_nwsl_sdp",
        None,
        "sdp_multipleSeasonMatches.json",
        None,
    ),
    "/v1/nwsl/football/seasons/{seasonId}/standings/overall": (
        "standings",
        "parse_nwsl_standings",
        None,
        "sdp_standings_overall.json",
        None,
    ),
    "/v1/nwsl/football/seasons/{seasonId}/stats/players": (
        "player_stats",
        "parse_nwsl_stats",
        None,
        "sdp_stats_players.json",
        None,
    ),
    "/v1/nwsl/football/seasons/{seasonId}/stats/teams": (
        "team_stats",
        "parse_nwsl_stats",
        None,
        "sdp_stats_teams.json",
        None,
    ),
    "/v1/nwsl/football/seasons/{seasonId}/teams": ("teams", "parse_nwsl_sdp", None, "sdp_teams.json", None),
    "/v1/nwsl/football/seasons/{seasonId}/matchdays": ("matchdays", "parse_nwsl_sdp", None, "sdp_matchdays.json", None),
    # The committed stages capture is a real ``{"stages": null}`` body (a pure
    # league season has no stages), so its columns come from the spec component.
    "/v1/nwsl/football/seasons/{seasonId}/stages": ("stages", "parse_nwsl_sdp", None, "sdp_stages.json", "Stage"),
    "/v1/nwsl/football/seasons/{seasonId}/matches/{matchId}/lineups": (
        "match_lineups",
        "parse_nwsl_lineups",
        "players",
        "sdp_match_lineups.json",
        None,
    ),
}

_PARSERS = {
    "parse_nwsl_sdp": parse_nwsl_sdp,
    "parse_nwsl_standings": parse_nwsl_standings,
    "parse_nwsl_stats": parse_nwsl_stats,
    "parse_nwsl_lineups": parse_nwsl_lineups,
}

_PYTYPE = {"integer": "int", "boolean": "bool", "string": "str"}

# ``parse_nwsl_standings`` pivots each club's ``stats[]`` array to columns, but the
# committed standings capture trims that array to three cells -- so a purely
# capture-derived schema would document 3 of the 12 stats the route really
# returns. The full set is enumerated in ``nwsl-returns.md``; the missing ones are
# appended (descriptions still resolve through the shared lookup).
_EXTRA_STAT_COLUMNS = {
    "standings": [
        ("rank", "integer", "Table position for the club in this split."),
        ("team", "character", "Club display name as rendered in the standings table."),
        ("points", "integer", "Championship points earned."),
        ("matches_played", "integer", "Matches played in this split."),
        ("win", "integer", "Matches won."),
        ("draw", "integer", "Matches drawn."),
        ("lose", "integer", "Matches lost."),
        ("goals_for", "integer", "Goals scored."),
        ("goals_against", "integer", "Goals conceded."),
        ("goal_difference", "integer", "Goals scored minus goals conceded."),
        ("movement", "character", "Position movement versus the previous matchday."),
        ("form", "character", "Recent results sequence, JSON-encoded (the API sends an array)."),
    ],
}

# ``locale`` is nominally required but is always ``en-US``; default it rather than
# forcing every caller to pass it.
_QUERY_DEFAULTS = {"locale": "en-US"}

# Live-verified example ids from the committed captures.
_EXAMPLE: Dict[str, Any] = {
    "season_id": "nwsl::Football_Season::0b6761e4701749f593690c0f338da74c",
    "match_id": "nwsl::Football_Match::0b6761e4701749f593690c0f338da74c",
}


def _emit_path(path: str) -> str:
    tail = path[len(BASE_PATH) :]
    return _TOKEN.sub(lambda m: "{" + underscore(m.group(1)) + "}", tail)


def _path_params(path: str) -> List[Dict[str, Any]]:
    return [{"name": underscore(t), "type": "str", "required": True} for t in _TOKEN.findall(path)]


def _query_params(op: dict, path_names: set) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if prm.get("in") != "query":
            continue
        wire = prm["name"]
        name = underscore(wire)
        if name in path_names:
            name = f"filter_{name}"
        jtype = (prm.get("schema") or {}).get("type", "string")
        entry: Dict[str, Any] = {"name": name, "query_key": wire, "type": _PYTYPE.get(jtype, "str")}
        if name in _QUERY_DEFAULTS:
            entry["default"] = _QUERY_DEFAULTS[name]
        desc = str(prm.get("description") or "").strip()
        if desc:
            entry["description"] = desc
        out.append(entry)
    return out


def _endpoint_entry(path: str, op: dict) -> Dict[str, Any]:
    short, parser, _primary, _capture, _fallback = _ROUTES[path]
    pps = _path_params(path)
    entry: Dict[str, Any] = {
        "short": short,
        "summary": op.get("summary") or f"GET {path}",
        "path": _emit_path(path),
        "parser": parser,
        "returns_schema": f"native/nwsl_api/{short}",
    }
    if pps:
        entry["path_params"] = pps
        example = {p["name"]: _EXAMPLE[p["name"]] for p in pps if p["name"] in _EXAMPLE}
        if example:
            entry["example_args"] = example
    qps = _query_params(op, {p["name"] for p in pps})
    if qps:
        entry["extra_params"] = qps
    return entry


def _capture_path(refs: Path, filename: Optional[str]) -> Optional[Path]:
    return (refs / "captures" / filename) if filename else None


def main() -> None:
    refs = refs_dir("nwsl", "nwsl.openapi.yaml")
    spec = load_spec(refs, "nwsl.openapi.yaml")
    unknown = sorted(set(spec["paths"]) - set(_ROUTES))
    if unknown:
        raise SystemExit(f"nwsl: spec paths missing from the curated route table: {unknown}")
    ops = sorted(get_ops(spec), key=lambda pv: _ROUTES[pv[0]][0])

    doc = {
        "api": "nwsl_api",
        "host": HOST,
        "name_pattern": "nwsl_{short}",
        "module": "nwsl_api",
        "parser_module": "soccer.nwsl.nwsl_api_parsers",
        "qualifier": "",
        "passthrough_query": True,
        "getter_module": "sportsdataverse.soccer.nwsl.nwsl_api_runtime",
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoESPNDataError: the SDP host returned 404 "
                "(unknown composite id, or an unmapped match sub-resource).",
                "requests.exceptions.RequestException: Connection-level failure after "
                "``dl_utils.download`` exhausts its retries.",
            ],
            "see_also": [
                {
                    "name": "NWSL",
                    "url": "https://www.nwslsoccer.com/",
                    "note": "the public site this feed renders",
                },
                {
                    "name": "American Soccer Analysis",
                    "url": "https://www.americansocceranalysis.com/",
                    "note": "advanced NWSL xG / g+ metrics (``asa_*`` wrappers)",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/nwsl_api.yaml", doc)

    descriptions = [spec_descriptions(spec), markdown_descriptions(refs / "nwsl-returns.md")]
    schema_dir = ROOT / "tools/codegen/schemas/native/nwsl_api"
    rewrite_schema_dir(schema_dir)
    for path, _op in ops:
        short, parser, primary, capture, fallback = _ROUTES[path]
        frame = parse_capture(_capture_path(refs, capture), _PARSERS[parser], primary=primary)
        cols = columns_from_frame(frame, descriptions)
        if not cols and fallback:
            cols = component_columns(spec, fallback, descriptions)
        known = {c["name"] for c in cols}
        for name, ctype, desc in _EXTRA_STAT_COLUMNS.get(short, []):
            if name not in known:
                cols.append({"name": name, "type": ctype, "description": desc})
        write_yaml(schema_dir / f"{short}.yaml", {"schema": short, "kind": "dataframe", "columns": cols})
    print(f"nwsl_api: {len(ops)} endpoints")


if __name__ == "__main__":
    main()
