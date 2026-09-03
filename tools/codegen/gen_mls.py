"""Generate the ``mls_api`` flat-API endpoint YAML + returns-schemas from the
official MLS web API OpenAPI spec (three auth-free mlssoccer.com hosts).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_on3.py``, with the shared plumbing in ``gen_soccer_common.py``.

Emits:

* ``tools/codegen/endpoints/mls_api.yaml`` -- one endpoint per GET route (12).
  The family host is ``stats-api``; the ``sportapi`` and ``dapi`` routes carry an
  endpoint-level ``host`` override (the same mechanism ``on3.yaml`` uses for its
  ``/rdb/v2`` op) so ``host + path`` is the real URL for all three.
* ``tools/codegen/schemas/native/mls_api/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running the endpoint's parser over the committed
  capture.

The spec's declared 200 shapes are **wrong for four routes** (``/competitions``,
``/competitions/{id}/seasons``, ``/matches/seasons/{seasonId}`` and the standings
route declare bare arrays; the captures show envelopes). Deriving the returns
columns from the captures rather than from the spec's response components is what
keeps the generated docs honest -- see ``mls_api_parsers`` for the same note.

Run: ``python tools/codegen/gen_mls.py``
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
from sportsdataverse.soccer.mls.mls_api_parsers import (
    parse_mls_api,
    parse_mls_entity,
    parse_mls_match,
    parse_mls_standings,
)

STATS_API = "https://stats-api.mlssoccer.com"
SPORT_API = "https://sportapi.mlssoccer.com"
DAPI = "https://dapi.mlssoccer.com"

_TOKEN = re.compile(r"\{([^}]+)\}")

# Curated per-route metadata. Twelve routes across three hosts with no shared
# naming convention -- a slug algorithm would only produce worse names than this
# table (``matches_seasons_season_id`` for the season schedule, say).
#   path -> (short, host override, parser, primary sub-frame, capture file)
_ROUTES: Dict[str, tuple] = {
    "/competitions": ("competitions", None, "parse_mls_api", None, "statsapi_competitions.json"),
    "/competitions/{competitionId}/seasons": (
        "competition_seasons",
        None,
        "parse_mls_api",
        None,
        "statsapi_competitions_seasons.json",
    ),
    "/competitions/{competitionId}/seasons/{seasonId}/standings": (
        "standings",
        None,
        "parse_mls_standings",
        "entries",
        "statsapi_standings_conference.json",
    ),
    "/matches/seasons/{seasonId}": (
        "season_matches",
        None,
        "parse_mls_api",
        None,
        "statsapi_matches_by_season.json",
    ),
    "/matches/{matchId}": ("match", None, "parse_mls_match", "match_information", "statsapi_match_single.json"),
    "/clubs/{clubId}": ("club", None, "parse_mls_entity", None, "statsapi_club_single.json"),
    "/api/matches/{matchId}": (
        "sportapi_match",
        SPORT_API,
        "parse_mls_entity",
        None,
        "sportapi_match_single.json",
    ),
    "/api/matches/bySportecIds/{ids}": (
        "sportapi_matches_by_sportec_ids",
        SPORT_API,
        "parse_mls_api",
        None,
        "sportapi_matches_bysportecids.json",
    ),
    "/api/clubs/bySportecIds/{ids}": (
        "sportapi_clubs_by_sportec_ids",
        SPORT_API,
        "parse_mls_api",
        None,
        "sportapi_clubs_bysportecids.json",
    ),
    "/api/players/byClub/{clubId}": (
        "sportapi_club_players",
        SPORT_API,
        "parse_mls_api",
        None,
        "sportapi_players_byclub.json",
    ),
    "/v2/content/en-us/seasons": ("content_seasons", DAPI, "parse_mls_api", None, "dapi_seasons_query.json"),
    "/v2/content/en-us/seasons/{slug}": (
        "content_season",
        DAPI,
        "parse_mls_entity",
        None,
        "dapi_season_content.json",
    ),
}

_PARSERS = {
    "parse_mls_api": parse_mls_api,
    "parse_mls_entity": parse_mls_entity,
    "parse_mls_match": parse_mls_match,
    "parse_mls_standings": parse_mls_standings,
}

# Query wire keys whose snake_cased form is not a usable Python argument name.
# ``type`` shadows a builtin; the bracketed date-window keys and the dotted
# Contentful field filters are not identifiers at all.
_QUERY_NAMES = {
    "type": "standings_type",
    "match_date[gte]": "match_date_gte",
    "match_date[lte]": "match_date_lte",
    "fields.competitionSportecId": "competition_sportec_id",
    "fields.sportecId": "sportec_id",
}

# Wire keys that need a runtime coercion before they hit the query string.
_QUERY_TRANSFORMS = {"is_live": "bool_str"}

_PYTYPE = {"integer": "int", "boolean": "bool", "string": "str"}

# Live-verified example ids from the committed captures.
_EXAMPLE: Dict[str, Any] = {
    "competition_id": "MLS-COM-000001",
    "season_id": "MLS-SEA-0001KA",
    "match_id": "MLS-MAT-0009H8",
    "club_id": "MLS-CLU-000001",
    "ids": "MLS-MAT-0009H8",
    "slug": "mls-regular-season-2026",
}


def _snake_token(token: str) -> str:
    return underscore(token)


def _emit_path(path: str) -> str:
    return _TOKEN.sub(lambda m: "{" + _snake_token(m.group(1)) + "}", path)


def _path_params(path: str) -> List[Dict[str, Any]]:
    return [{"name": _snake_token(t), "type": "str", "required": True} for t in _TOKEN.findall(path)]


def _query_params(op: dict, path_names: set) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if prm.get("in") != "query":
            continue
        wire = prm["name"]
        name = _QUERY_NAMES.get(wire, underscore(wire))
        if name in path_names:
            name = f"filter_{name}"
        jtype = (prm.get("schema") or {}).get("type", "string")
        entry: Dict[str, Any] = {"name": name, "query_key": wire, "type": _PYTYPE.get(jtype, "str")}
        desc = str(prm.get("description") or "").strip()
        if desc:
            entry["description"] = desc
        if name in _QUERY_TRANSFORMS:
            entry["transform"] = _QUERY_TRANSFORMS[name]
        out.append(entry)
    return out


def _endpoint_entry(path: str, op: dict) -> Dict[str, Any]:
    short, host, parser, _primary, _capture = _ROUTES[path]
    pps = _path_params(path)
    entry: Dict[str, Any] = {
        "short": short,
        "summary": op.get("summary") or f"GET {path}",
        "path": _emit_path(path),
        "parser": parser,
        "returns_schema": f"native/mls_api/{short}",
    }
    if host:
        entry["host"] = host
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
    refs = refs_dir("mls", "mls.openapi.yaml")
    spec = load_spec(refs, "mls.openapi.yaml")
    ops = sorted(get_ops(spec), key=lambda pv: _ROUTES[pv[0]][0])
    unknown = sorted(set(spec["paths"]) - set(_ROUTES))
    if unknown:
        raise SystemExit(f"mls: spec paths missing from the curated route table: {unknown}")

    doc = {
        "api": "mls_api",
        "host": STATS_API,
        "name_pattern": "mls_{short}",
        "module": "mls_api",
        "parser_module": "soccer.mls.mls_api_parsers",
        "qualifier": "",
        "passthrough_query": True,
        "getter_module": "sportsdataverse.soccer.mls.mls_api_runtime",
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoESPNDataError: the MLS host returned 404 "
                "(unknown id, or an unplayed match on ``/matches/{matchId}``).",
                "requests.exceptions.RequestException: Connection-level failure after "
                "``dl_utils.download`` exhausts its retries.",
            ],
            "see_also": [
                {
                    "name": "MLS",
                    "url": "https://www.mlssoccer.com/",
                    "note": "the public site these hosts render",
                },
                {
                    "name": "American Soccer Analysis",
                    "url": "https://www.americansocceranalysis.com/",
                    "note": "advanced MLS xG / g+ metrics (``asa_*`` wrappers)",
                },
            ],
        },
        "runtime_imports": ["_get", "bool_str"],
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/mls_api.yaml", doc)

    descriptions = [spec_descriptions(spec), markdown_descriptions(refs / "mls-returns.md")]
    schema_dir = ROOT / "tools/codegen/schemas/native/mls_api"
    rewrite_schema_dir(schema_dir)
    for path, _op in ops:
        short, _host, parser, primary, capture = _ROUTES[path]
        frame = parse_capture(_capture_path(refs, capture), _PARSERS[parser], primary=primary)
        write_yaml(
            schema_dir / f"{short}.yaml",
            {"schema": short, "kind": "dataframe", "columns": columns_from_frame(frame, descriptions)},
        )
    print(f"mls_api: {len(ops)} endpoints")


if __name__ == "__main__":
    main()
