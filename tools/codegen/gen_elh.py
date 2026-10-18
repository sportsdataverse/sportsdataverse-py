"""Generate the ``elh`` (Czech Extraliga) flat-API endpoint YAML + returns-schemas from the
hokej.cz OpenAPI spec (``sdv-internal-refs/hokejcz/``: the public S3 bucket behind hokej.cz
plus the eSports Media scoreboard; one spec, per-path ``servers``).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_asa.py``,
with the shared plumbing in ``gen_soccer_common.py``.

Emits:

* ``tools/codegen/endpoints/elh.yaml`` -- family host ``https://s3-eu-west-1.amazonaws.com/hokej.cz``;
  the scoreboard path carries its own ``host`` (``https://json.esports.cz/hokejcz``). The getter is
  ``sportsdataverse.hockey.elh.elh_runtime._get`` (throttle, the listing as XML text, the
  refs capture's refusal checks). The full play-by-play path is TWO wrappers: ``pbp`` (one row
  per entry) and ``pbp_goals`` (one row per on-ice player at each goal).
* ``tools/codegen/schemas/native/elh/<short>.yaml`` -- columns from running the endpoint's parser
  over a committed fixture: the FULL untruncated body under ``tests/fixtures/elh/full/`` when
  there is one (the dtype source), else the truncated refs copy under ``tests/fixtures/elh/2026/``.
  Descriptions are left blank on purpose: they live in ``manual_column_descriptions.yaml``
  (keyed ``elh_<short>``), never in ``schemas/**``.

The ``season`` path argument is the season's END year; the runtime transform
``elh_start_year`` maps it to the bucket's START-year folder.

Run: ``python tools/codegen/gen_elh.py``
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, get_ops, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.elh import elh_parsers

STEM = "elh"
SPEC = "hokejcz.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / STEM
GETTER = "sportsdataverse.hockey.elh.elh_runtime"

SEASON_DESC = (
    "Season by its END year (2027 = 2026-27); the bucket files it under the START year "
    "(match/2026/). Play-by-play starts in 2014-15 (season 2015)."
)
MATCH_DESC = (
    "hokej.cz game id (the S3 file name; hokejcz_game_id of elh_game_ids / elh_scoreboard), "
    "e.g. 2928321. Not the onlajny game id."
)
DATE_DESC = (
    "Day, YYYY-MM-DD. The scoreboard is a ROLLING window of recent days: an older day (and "
    "today) answers 404, raised as NoDataError; a day without an Extraliga game raises "
    "NoDataError too."
)
#: Path params whose spec description is replaced, with their python type and runtime transform.
PATH_PARAMS: Dict[str, Dict[str, Any]] = {
    "season": {"type": "int", "transform": "elh_start_year", "description": SEASON_DESC},
    "match_id": {"type": "int", "description": MATCH_DESC},
    "date": {"type": "str", "description": DATE_DESC},
}
#: Example values per path param (the 2026-27 example game; season as its END year).
EXAMPLES: Dict[str, Any] = {"season": 2027, "match_id": 2928321, "date": "2026-10-11"}

#: spec path -> [(short, parser, summary)]; one path may feed several wrappers.
ROUTES: Dict[str, List[tuple]] = {
    "/": [
        (
            "list_objects",
            "parse_elh_game_ids",
            "One page (up to 1,000 keys) of the hokej.cz S3 bucket listing; match keys carry the "
            "hokej.cz game id. The bucket is not Extraliga-only.",
        )
    ],
    "/match/{season}/full/{match_id}.json": [
        (
            "pbp",
            "parse_elh_pbp",
            "Play-by-play commentary of one game, every entry, oldest first: goals and penalties "
            "with team and players (onlajny ids).",
        ),
        (
            "pbp_goals",
            "parse_elh_pbp_goals",
            "Players on the ice at each goal of one game (onlajny ids), one row per player, with the strength.",
        ),
    ],
    "/match/{season}/short/{match_id}.json": [
        (
            "pbp_latest",
            "parse_elh_pbp",
            "The newest 20 play-by-play entries of one game (same columns as elh_pbp).",
        )
    ],
    "/match/{season}/mobile/{match_id}.json": [
        (
            "pbp_mobile",
            "parse_elh_pbp_mobile",
            "Play-by-play of one game as plain text (no team or player ids), oldest first.",
        )
    ],
    "/visualization/shots/{match_id}.json": [
        (
            "shots",
            "parse_elh_shots",
            "Shot attempts of one game with rink coordinates and result code (hokej.cz ids).",
        )
    ],
    "/visualization/events/{match_id}.json": [
        (
            "events",
            "parse_elh_events",
            "Cumulative shot attempts and shots on goal of one game, per team and game minute.",
        )
    ],
    "/scoreboard/{date}.json": [
        (
            "scoreboard",
            "parse_elh_scoreboard",
            "Extraliga games of one recent day with both id spaces (hokej.cz and onlajny) for games and teams.",
        )
    ],
}


def _slug(path: str) -> str:
    """The refs capture file stem of a path (``capture.py`` ``slug``)."""
    return path.strip("/").replace("/", "__").replace("{", "").replace("}", "").removesuffix(".json") or "list"


def _fixture(path: str) -> Optional[Path]:
    """The full body when committed, else the truncated 2026 refs copy."""
    name = _slug(path) + (".xml" if path == "/" else ".json")
    for candidate in (FIXTURES / "full" / name, FIXTURES / "2026" / name):
        if candidate.exists():
            return candidate
    return None


def _host(op: dict, path_item: dict, spec: dict) -> str:
    servers = op.get("servers") or path_item.get("servers") or spec["servers"]
    return str(servers[0]["url"])


def _params(op: dict, path_item: dict) -> List[dict]:
    return [*(path_item.get("parameters") or []), *(op.get("parameters") or [])]


def _entry(path: str, op: dict, path_item: dict, short: str, parser: str, summary: str, host: str, family_host: str):
    params = _params(op, path_item)
    path_params = [p["name"] for p in params if p.get("in") == "path"]
    assert not {"league", "sport"} & set(path_params), path  # the renderer would substitute them as ESPN slugs
    entry: Dict[str, Any] = {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser,
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": {name: EXAMPLES[name] for name in path_params},
    }
    if host != family_host:
        entry["host"] = host
    if path_params:
        entry["path_params"] = [{"name": n, "required": True, **PATH_PARAMS[n]} for n in path_params]
    query = [p for p in params if p.get("in") == "query"]
    if query:
        entry["extra_params"] = [
            {
                "name": underscore(p["name"]).replace("-", "_"),
                "query_key": p["name"],
                "type": "str",
                "description": " ".join(str(p.get("description") or "").split()),
            }
            for p in query
        ]
        entry["example_args"] = {"prefix": "match/2026/full/", "max_keys": "3"}
        entry["docstring"] = {
            "raw_doc": "the raw ListObjects V1 XML page as text (``str``)",
        }
    return entry


def _schema(path: str, short: str, parser: str) -> dict:
    fixture = _fixture(path)
    frame = pl.DataFrame()
    if fixture is not None:
        text = fixture.read_text(encoding="utf-8")
        frame = getattr(elh_parsers, parser)(text if fixture.suffix == ".xml" else json.loads(text))
    return {
        "schema": f"{STEM}_{short}",
        "kind": "dataframe",
        "columns": columns_from_frame(frame, [], leaf_fallback=False),
    }


def main() -> None:
    refs = refs_dir("hokejcz", SPEC)
    spec = load_spec(refs, SPEC)
    family_host = str(spec["servers"][0]["url"])
    ops = {path: op for path, op in get_ops(spec)}
    missing = sorted(set(ROUTES) ^ set(ops))
    if missing:
        raise SystemExit(f"{STEM}: spec paths and ROUTES differ: {missing}")

    endpoints = []
    schema_dir = ROOT / "tools/codegen/schemas/native" / STEM
    rewrite_schema_dir(schema_dir)
    for path in sorted(ROUTES):
        op = ops[path]
        host = _host(op, spec["paths"][path], spec)
        for short, parser, summary in ROUTES[path]:
            endpoints.append(_entry(path, op, spec["paths"][path], short, parser, summary, host, family_host))
            write_yaml(schema_dir / f"{short}.yaml", _schema(path, short, parser))

    doc = {
        "api": STEM,
        "host": family_host,
        "name_pattern": f"{STEM}_{{short}}",
        "module": STEM,
        "parser_module": "hockey.elh.elh_parsers",
        "getter_module": GETTER,
        "schema_prefix": f"{STEM}_",
        "qualifier": "",
        "passthrough_query": False,
        "raw_types": ["Dict", "str"],
        "runtime_imports": ["_get", "elh_start_year"],
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: a missing file (404 NoSuchKey), a scoreboard day "
                "outside the rolling window, or a stub body (no comments / shots / minutes, no "
                "Extraliga game that day).",
                "sportsdataverse.errors.AssetFetchError: an error envelope, a body for another game or "
                "day, or a broken listing page.",
            ],
            "see_also": [
                {
                    "name": "hokej.cz",
                    "url": "https://www.hokej.cz/tipsport-extraliga",
                    "note": "the Czech hockey federation site whose pages read these files",
                },
                {
                    "name": "fastRhockey",
                    "url": "https://fastRhockey.sportsdataverse.org",
                    "note": "hockey data in R",
                },
            ],
        },
        "endpoints": sorted(endpoints, key=lambda e: e["short"]),
    }
    write_yaml(ROOT / "tools/codegen/endpoints" / f"{STEM}.yaml", doc)
    print(f"{STEM}: {len(endpoints)} endpoints")


if __name__ == "__main__":
    main()
