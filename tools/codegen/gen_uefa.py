"""Generate the ``uefa`` flat-API endpoint YAML + returns-schemas from the UEFA
front-end OpenAPI spec (``comp`` / ``match`` / ``standings`` / ``matchstats``
``.uefa.com``, keyless).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_asa.py``, with the shared plumbing in ``gen_soccer_common.py``.

The generator reads the frozen spec from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/uefa.yaml`` -- one endpoint per GET route (7). The
  family host is ``https://comp.uefa.com``; every path in the spec carries its own
  ``servers`` entry, and a route on another host gets an endpoint-level ``host:``
  override (``match.uefa.com``, ``standings.uefa.com``, ``matchstats.uefa.com``),
  the mechanism ``mls_api.yaml`` uses for its three hosts. The getter is
  ``sportsdataverse.soccer.uefa_runtime._get``: the hosts' Akamai edge answers 403
  to the shared runtime's ``python-requests`` User-Agent.
* ``tools/codegen/schemas/native/uefa/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running :func:`parse_uefa` over the committed
  capture; a route without a capture is marked ``unverified`` with no columns.
  The capture-built spec and ``uefa-returns.md`` carry no column prose, so most
  descriptions resolve empty and are counted in the ``native/uefa`` bucket of
  ``extract_residual_columns._DEFERRED_BUCKETS``.

The spec's only path token is ``{matchId}`` (emitted as ``{match_id}``); a
``{league}`` / ``{sport}`` token would be substituted away by the module renderer,
so the generator asserts none appears.

Run: ``python tools/codegen/gen_uefa.py``
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

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
from sportsdataverse.soccer.uefa_parsers import parse_uefa

HOST = "https://comp.uefa.com"
SPEC = "uefa.openapi.yaml"
STEM = "uefa"
MODULE = "uefa"
NAME_PATTERN = "uefa_{short}"
PARSER_MODULE = "soccer.uefa_parsers"
PARSER = "parse_uefa"
GETTER = "sportsdataverse.soccer.uefa_runtime"

# The spec's summaries are bare ``host/path`` strings; the docstring headline wants prose.
# Column prose the spec and the returns doc lack; consulted before them.
_COLUMN_NOTES: Dict[str, str] = {
    "statistics": "The team's full statistic list for the match, JSON-encoded (one {name, value} object per statistic).",
    "competition_phase": "Competition phase: TOURNAMENT (main draw) or QUALIFYING.",
}

_SUMMARIES: Dict[str, str] = {
    "competitions": "UEFA competitions by id (Champions League 1, Europa League 3, Conference League 2019)",
    "teams": "Teams entered in a UEFA competition season",
    "players": "Players registered in a UEFA competition season",
    "matches": "Matches of a UEFA competition season",
    "livescore": "Matches live right now across UEFA competitions",
    "standings": "Group / league-phase standings of a UEFA competition season",
    "team_statistics": "Per-team match statistics of one UEFA match (one row per team)",
}

_TOKEN = re.compile(r"\{([^}]+)\}")
# operationId = ``<host-stem>_v<N>_<route>[_<pathParam>]`` (``comp_v2_teams``).
_OPID_PREFIX = re.compile(r"^[a-z]+_v\d+_")

# Path-param examples the spec does not carry: the capture's match (a 2025-26
# UCL match from /v5/matches), so the docs example resolves to the committed file.
_EXAMPLE_PATH_ARGS: Dict[str, str] = {"match_id": "2047742"}

_UNVERIFIED = "no committed capture in sdv-internal-refs/uefa/captures (ENDPOINTS.md marks the route not captured)"


def _snake_path(path: str) -> str:
    """``/v1/team-statistics/{matchId}`` -> ``/v1/team-statistics/{match_id}``."""
    return _TOKEN.sub(lambda m: "{" + underscore(m.group(1)) + "}", path)


def _short(path: str, op: dict) -> str:
    """operationId with the host/version prefix and any path-param suffix stripped."""
    short = _OPID_PREFIX.sub("", op["operationId"])
    for token in _TOKEN.findall(path):
        short = short.removesuffix("_" + token)
    return short


def _host(spec: dict, path: str, op: dict) -> str:
    """The route's own server (path-level in this spec, op-level tolerated)."""
    servers = spec["paths"][path].get("servers") or op.get("servers")
    if not servers:
        raise SystemExit(f"{path}: no servers entry -- every UEFA route must name its host")
    return str(servers[0]["url"]).rstrip("/")


def _path_params(op: dict) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if prm.get("in") != "path":
            continue
        entry: Dict[str, Any] = {"name": underscore(prm["name"]), "type": "str", "required": True}
        if prm.get("description"):
            entry["description"] = str(prm["description"]).strip()
        out.append(entry)
    return out


def _query_params(op: dict) -> List[Dict[str, Any]]:
    """Query params as ``extra_params`` (python name snake, ``query_key`` = wire key)."""
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if prm.get("in") != "query":
            continue
        wire = prm["name"]
        description = str(prm.get("description") or "").strip()
        if prm.get("required"):
            description = ("Required. " + description).strip()
        out.append({"name": underscore(wire), "query_key": wire, "type": "str", "description": description})
    return out


def _example_args(op: dict) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for prm in op.get("parameters", []):
        name = underscore(prm["name"])
        if "example" in prm:
            out[name] = prm["example"]
        elif prm.get("in") == "path" and name in _EXAMPLE_PATH_ARGS:
            out[name] = _EXAMPLE_PATH_ARGS[name]
    return out


def _capture_for(refs: Path, host: str, path: str, example_args: Dict[str, Any]) -> Optional[Path]:
    """The committed capture, named by ``capture.py``'s slug of the example URL.

    ``slug(url) = hostname.split(".")[0] + "__" + path.strip("/").replace("/", "__")``
    with the path params substituted (``matchstats__v1__team-statistics__2047742``).
    """
    concrete = _TOKEN.sub(lambda m: str(example_args.get(underscore(m.group(1)), m.group(1))), path)
    parts = urlsplit(host + concrete)
    slug = (parts.hostname or "").split(".")[0] + "__" + parts.path.strip("/").replace("/", "__")
    candidate = refs / "captures" / f"{slug}.json"
    return candidate if candidate.exists() else None


def _endpoint_entry(spec: dict, path: str, op: dict) -> Dict[str, Any]:
    short = _short(path, op)
    pps = _path_params(op)
    names = {p["name"] for p in pps}
    assert not {"league", "sport"} & names, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": _SUMMARIES[short].rstrip(".") + ".",
        "path": _snake_path(path),
        "parser": PARSER,
        "returns_schema": f"native/{STEM}/{short}",
    }
    host = _host(spec, path, op)
    if host != HOST:
        entry["host"] = host
    if pps:
        entry["path_params"] = pps
    qps = _query_params(op)
    if qps:
        entry["extra_params"] = qps
    examples = _example_args(op)
    if examples:
        entry["example_args"] = examples
    return entry


def main() -> None:
    refs = refs_dir(STEM, SPEC)
    spec = load_spec(refs, SPEC)
    ops = sorted(get_ops(spec), key=lambda pv: _short(pv[0], pv[1]))
    shorts = [_short(p, op) for p, op in ops]
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
        "getter_module": GETTER,
        "docstring": {
            "example_import": True,
            # Spelled out: generate.py only adds the error-vocabulary lines for the
            # getters in its _VOCAB_GETTERS set, which this family's runtime is not in.
            "raises": [
                "NoDataError: The UEFA host answered 404 (unknown id or route).",
                "ValueError: The UEFA host answered 400 / 422 -- the request is wrong; retrying cannot help.",
                "AssetFetchError: The fetch failed (a non-2xx answer or a connection failure after "
                "retries, e.g. a 403 from the edge, 429 or 5xx, or an empty or unreadable 200 body) "
                "-- the answer is unknown, not empty.",
            ],
            "see_also": [
                {
                    "name": "UEFA.com",
                    "url": "https://www.uefa.com/",
                    "note": "the public site these keyless front-end hosts render",
                },
                {
                    "name": "uefa-api",
                    "url": "https://github.com/ErikMichelson/uefa-api",
                    "note": "community TypeScript wrapper over the same hosts",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(spec, p, op) for p, op in ops],
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    descriptions = [_COLUMN_NOTES, spec_descriptions(spec), markdown_descriptions(refs / f"{STEM}-returns.md")]
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    unverified = 0
    for path, op in ops:
        short = _short(path, op)
        capture = _capture_for(refs, _host(spec, path, op), path, _example_args(op))
        schema: Dict[str, Any] = {"schema": short, "kind": "dataframe"}
        if capture is None:
            unverified += 1
            schema["unverified"] = _UNVERIFIED
            schema["columns"] = []
        else:
            schema["columns"] = columns_from_frame(parse_capture(capture, parse_uefa), descriptions)
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints ({unverified} unverified)")


if __name__ == "__main__":
    main()
