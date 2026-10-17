"""Generate the ``magnus`` flat-API endpoint YAML + returns-schemas from the Ligue Magnus
OpenAPI spec (``liguemagnus.com``, keyless).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_asa.py``,
with the shared plumbing in ``gen_soccer_common.py``.

The spec (``sdv-internal-refs/ligue-magnus/``) models two sources on one host:

* seven **admin-ajax actions**, one physical endpoint dispatched by ``action``. The spec
  names each as a fragment path (``/wp-admin/admin-ajax.php#get_rencontres``); the wire
  path never carries the fragment. Each wrapper here keeps the bare path and sends the
  action as a ``fixed_params`` entry (sent on every request, never an argument).
* the **game sheet page** ``/rencontre/{id}/`` (text/html with the game JSON embedded in a
  Vue ``:data`` attribute). Four wrappers share that one route -- ``game`` (header),
  ``pbp``, ``lineups``, ``shootout`` -- each with its own parser. The flat-API YAML has no
  per-endpoint getter or response-format key, so the family getter
  (``sportsdataverse.hockey.magnus.magnus_runtime._get``) dispatches on the URL path: a
  ``/rencontre/<id>/`` URL is fetched as text and its embedded game sheet returned.

The season is the END year at the public API; ``season`` is sent as ``competition_id``
through the runtime transform ``_competition_id`` (seeded for 2021-2027, so rendering the
docs example never touches the network). ``par_page=700`` is fixed on ``schedule`` so one
request returns a whole season; the runtime refuses a partial page.

Emits ``tools/codegen/endpoints/magnus.yaml`` and
``tools/codegen/schemas/native/magnus/<short>.yaml``. Column descriptions are NOT written
into the schemas (they belong in ``manual_column_descriptions.yaml``).

Run: ``python tools/codegen/gen_magnus.py``
"""

from __future__ import annotations

import copy
import json
from typing import Any, Dict, List, Optional

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.hockey.magnus import magnus_parsers

HOST = "https://liguemagnus.com"
SPEC = "ligue-magnus.openapi.yaml"
STEM = "magnus"
AJAX = "/wp-admin/admin-ajax.php"
GAME_PATH = "/rencontre/{game_id}/"
SPEC_GAME_PATH = "/rencontre/{id}/"
FIXTURES = ROOT / "tests" / "fixtures" / STEM
FULL_GAME = FIXTURES / "full" / "rencontre__77779.json"

SEASON_DESCRIPTION = (
    "Required. Season by its END year (2027 = 2026-27), sent as the season's competition_id. "
    "Coverage starts in 2020-21 (2021); an earlier season, or one the federation does not list, "
    "raises NoDataError."
)


def _param(name: str, wire: str, typ: str, description: str, *, required: bool = False, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": description}
    if required:
        out["required"] = True
    out.update(extra)
    return out


SEASON = _param("season", "competition_id", "int", SEASON_DESCRIPTION, required=True, transform="_competition_id")

# short -> (admin-ajax action, parser, [(python name, wire key, type, required)], fixed params,
#           wire keys deliberately not exposed, example args)
AJAX_ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "competitions": {
        "action": "get_competition_by_type",
        "parser": "parse_magnus_competitions",
        "params": [("competition_type", "id_type", "int", False)],
        "defaults": {"competition_type": 1},
        "example": {},
    },
    "phases": {
        "action": "get_phases",
        "parser": "parse_magnus_phases",
        "params": [SEASON],
        "example": {"season": 2027},
    },
    "teams": {
        "action": "get_equipes",
        "parser": "parse_magnus_teams",
        # phase_id is optional upstream, but without it the API answers every team of every
        # competition in the federation database (~680 KB): the wrapper always scopes it.
        "params": [("phase_id", "phase_id", "int", True)],
        "example": {"phase_id": 714},
    },
    "schedule": {
        "action": "get_rencontres",
        "parser": "parse_magnus_schedule",
        "params": [
            SEASON,
            ("phase_id", "phase_id", "int", False),
            ("team_id", "equipe_id", "int", False),
            ("start_date", "date_min", "str", False),
            ("end_date", "date_max", "str", False),
            ("round", "journee", "int", False),
        ],
        "fixed": {"par_page": 700},
        "hidden": {"page"},
        "example": {"season": 2027},
    },
    "rounds": {
        "action": "get_liste_tours",
        "parser": "parse_magnus_rounds",
        "params": [("phase_id", "phase_id", "int", True)],
        "example": {"phase_id": 714},
    },
    "round_games": {
        "action": "get_rencontrestours",
        "parser": "parse_magnus_schedule",
        "params": [("phase_id", "phase_id", "int", True), ("round", "journee", "int", True)],
        "example": {"phase_id": 714, "round": 1},
    },
    "standings": {
        "action": "get_classementsphase",
        "parser": "parse_magnus_standings",
        "params": [("phase_id", "phase_id", "int", True)],
        "example": {"phase_id": 714},
    },
}

GAME_ENDPOINTS: Dict[str, Dict[str, str]] = {
    "game": {
        "parser": "parse_magnus_game",
        "summary": "Game header from the game sheet: teams, score, start time, status, attendance and rink (one row).",
    },
    "pbp": {
        "parser": "parse_magnus_pbp",
        "summary": "Play-by-play from the game sheet: goals, penalties, shots, faceoffs, goalie changes and "
        "timeouts in game order, with English event types and derived periods. No shot coordinates.",
    },
    "lineups": {
        "parser": "parse_magnus_lineups",
        "summary": "Game roster from the game sheet: one row per dressed player, with the person id, the "
        "per-game participant id, jersey, licence number, position, captaincy and MVP flag.",
    },
    "shootout": {
        "parser": "parse_magnus_shootout",
        "summary": "Shootout attempts from the game sheet: round, shooter, goalie and result (zero rows when "
        "the game had no shootout).",
    },
}

RAISES = [
    "sportsdataverse.errors.NoDataError: liguemagnus.com has no data for the request (success=false "
    "'Aucune donnée disponible', an empty data stub, zero rows, a season outside 2020-21 onwards, an unknown "
    "or unplayed game).",
    "sportsdataverse.errors.AssetFetchError: the fetch failed, or the answer is success=false with any other "
    "message, outside the requested scope, a partial schedule page, or a game page without its embedded game "
    "sheet.",
]
SEE_ALSO = [
    {"name": "Ligue Magnus", "url": "https://liguemagnus.com/", "note": "the league site whose data the wrappers read"},
]
FAMILY_DOC: Dict[str, Any] = {"example_import": True, "raises": RAISES, "see_also": SEE_ALSO}
GAME_DOC: Dict[str, Any] = {
    **FAMILY_DOC,
    "raw_doc": "the game sheet ``Dict`` extracted from the page's embedded JSON (staff birth dates removed)",
}


def _spec_param(op: dict, wire: str) -> dict:
    for prm in op.get("parameters", []):
        if prm.get("name") == wire and prm.get("in") == "query":
            return prm
    raise SystemExit(f"magnus: spec op {op.get('operationId')} has no query parameter {wire!r}")


def _description(op: dict, wire: str, required: bool) -> str:
    text = " ".join(str(_spec_param(op, wire).get("description") or "").split())
    return f"Required. {text}" if required and not text.startswith("Required.") else text


def _ajax_entry(short: str, cfg: Dict[str, Any], op: dict) -> Dict[str, Any]:
    params: List[Dict[str, Any]] = []
    for p in cfg["params"]:
        if isinstance(p, dict):
            params.append(dict(p))
            continue
        name, wire, typ, required = p
        entry = _param(name, wire, typ, _description(op, wire, required), required=required)
        if name in cfg.get("defaults", {}):
            entry["default"] = cfg["defaults"][name]
        params.append(entry)
    fixed = {"action": cfg["action"], **cfg.get("fixed", {})}
    spec_wire = {p["name"] for p in op.get("parameters", []) if p.get("in") == "query"}
    covered = {p["query_key"] for p in params} | set(fixed) | set(cfg.get("hidden", set()))
    if covered != spec_wire:  # every spec query parameter is exposed, fixed or deliberately hidden
        raise SystemExit(f"magnus {short}: spec params {sorted(spec_wire)} != covered {sorted(covered)}")
    return {
        "short": short,
        "summary": " ".join(str(op.get("summary") or "").split()).rstrip(".") + ".",  # prose, period-terminated
        "path": AJAX,
        "fixed_params": fixed,
        "extra_params": params,
        "example_args": cfg["example"],
        "parser": cfg["parser"],
        "returns_schema": f"native/{STEM}/{short}",
    }


def _game_entry(short: str, cfg: Dict[str, str], op: dict) -> Dict[str, Any]:
    (path_param,) = [p for p in op.get("parameters", []) if p.get("in") == "path"]
    return {
        "short": short,
        "summary": cfg["summary"],
        "path": GAME_PATH,
        "path_params": [
            {
                "name": "game_id",
                "type": "int",
                "required": True,
                "description": " ".join(str(path_param.get("description") or "").split()),
            }
        ],
        "example_args": {"game_id": 77779},
        "parser": cfg["parser"],
        "returns_schema": f"native/{STEM}/{short}",
        "docstring": copy.deepcopy(GAME_DOC),  # deep copies: shared objects would dump as YAML anchors
    }


def _fixture(short: str) -> Optional[dict]:
    """The body a route's returns-schema is derived from: the FULL game sheet for the game routes."""
    if short in GAME_ENDPOINTS:
        path = FULL_GAME
    else:
        path = FIXTURES / f"ajax__{AJAX_ENDPOINTS[short]['action']}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _columns(frame: pl.DataFrame) -> List[Dict[str, str]]:
    """Returns-schema columns with blank descriptions; list / date / datetime keep their own type names."""
    cols = columns_from_frame(frame, [], leaf_fallback=False)
    for col, dtype in zip(cols, frame.dtypes):
        if isinstance(dtype, pl.List):
            col["type"] = "list"
        elif isinstance(dtype, pl.Datetime):
            col["type"] = "datetime"
        elif dtype == pl.Date:
            col["type"] = "date"
    return cols


def main() -> None:
    refs = refs_dir("ligue-magnus", SPEC)
    spec = load_spec(refs, SPEC)
    paths = spec["paths"]
    endpoints = [_ajax_entry(s, c, paths[f"{AJAX}#{c['action']}"]["get"]) for s, c in AJAX_ENDPOINTS.items()]
    endpoints += [_game_entry(s, c, paths[SPEC_GAME_PATH]["get"]) for s, c in GAME_ENDPOINTS.items()]
    actions = {c["action"] for c in AJAX_ENDPOINTS.values()}
    spec_actions = {p.split("#", 1)[1] for p in paths if p.startswith(f"{AJAX}#")}
    if actions != spec_actions or SPEC_GAME_PATH not in paths:
        raise SystemExit(f"magnus: spec routes changed: {sorted(spec_actions)} vs {sorted(actions)}")
    for ep in endpoints:  # the renderer substitutes {league} / {sport} as ESPN slugs
        assert not {"league", "sport"} & {p["name"] for p in ep.get("path_params", [])}, ep["path"]
    endpoints.sort(key=lambda e: e["short"])

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": f"{STEM}_{{short}}",
        "module": STEM,
        "parser_module": f"hockey.{STEM}.{STEM}_parsers",
        "getter_module": f"sportsdataverse.hockey.{STEM}.{STEM}_runtime",
        "runtime_imports": ["_get", "_competition_id"],
        "qualifier": "",
        "passthrough_query": False,
        "schema_prefix": f"{STEM}_",
        "docstring": copy.deepcopy(FAMILY_DOC),
        "endpoints": endpoints,
    }
    write_yaml(ROOT / "tools/codegen/endpoints" / f"{STEM}.yaml", doc)

    schema_dir = ROOT / "tools/codegen/schemas/native" / STEM
    rewrite_schema_dir(schema_dir)
    for ep in endpoints:
        frame = getattr(magnus_parsers, ep["parser"])(_fixture(ep["short"]))
        if frame.is_empty():
            raise SystemExit(f"magnus {ep['short']}: the fixture parses to zero rows")
        write_yaml(
            schema_dir / f"{ep['short']}.yaml",
            {
                "schema": f"{STEM}_{ep['short']}",
                "kind": "dataframe",
                "columns": _columns(frame),
            },
        )
    print(f"{STEM}: {len(endpoints)} endpoints")


if __name__ == "__main__":
    main()
