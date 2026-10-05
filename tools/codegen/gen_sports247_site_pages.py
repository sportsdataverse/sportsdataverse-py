"""Generate the ``sports247_site_pages`` endpoint YAML + returns-schemas from the
247Sports site-pages OpenAPI spec.

Source of truth: ``sdv-internal-refs/247sports/site-pages.openapi.yaml`` (35
auth-free ``247sports.com/*.json`` front-end page-model routes, 17 component
schemas). Idempotent: the same spec yields byte-identical output. The spec path
is resolved from ``$SDV_INTERNAL_REFS_REPO`` (default: the local workspace
checkout).

Run: python tools/codegen/gen_sports247_site_pages.py
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from sportsdataverse.dl_utils import underscore

ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_REFS = Path(r"C:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-internal-refs")
SPEC = Path(os.environ.get("SDV_INTERNAL_REFS_REPO", str(_DEFAULT_REFS))) / "247sports/site-pages.openapi.yaml"

# raw OpenAPI path -> generator-derived short name (stable; see plan route table)
_SHORT: Dict[str, str] = {
    "/Season/{season}/Recruits.json": "season_recruits",
    "/Institution/{key}.json": "institution",
    "/Institution/{key}/Location.json": "institution_location",
    "/Institution.json": "institution_list",
    "/college/{schoolSlug}/Institution/{key}/TimelineEvents.json": "institution_timeline_events",
    "/League/{leagueId}/Institutions.json": "league_institutions",
    "/Player/{key}.json": "player",
    "/Player/{key}/PlayerHighSchool.json": "player_high_school",
    "/Player/{key}/PrimaryPlayerSport.json": "player_primary_sport",
    "/Player/{key}/CurrentPlayerInstitution.json": "player_current_institution",
    "/Player.json": "player_search",
    "/PlayerInstitution/{key}.json": "player_institution",
    "/playersport/{key}.json": "playersport",
    "/PlayerSport/{key}/RecruitRankHistory.json": "playersport_rank_history",
    "/PlayerSport/{key}/PlayerInstitution.json": "playersport_institution",
    "/Position/{key}/playersportrankings.json": "position_rankings",
    "/Page/{pageId}/Feeds.json": "page_feeds",
    "/Coach/{key}.json": "coach",
    "/Coach/{key}/CoachRankings.json": "coach_rankings",
    "/Coach/{key}/Hometown.json": "coach_hometown",
    "/Coach/{key}/AlmaMater.json": "coach_alma_mater",
    "/CoachRanking/{key}.json": "coach_ranking",
    "/Season/{season}/CurrentExpertPredictions.json": "season_current_expert_predictions",
    "/Event/{slug}.json": "event",
    "/RecruitInterest/{key}.json": "recruit_interest",
    "/Season/{season}/RecruitInterests.json": "season_recruit_interests",
    "/Season/{season}/RecruitInterestEvents.json": "season_recruit_interest_events",
    "/Recruitment/{key}/Institution.json": "recruitment_institution",
    "/Recruitment/{key}/Interests.json": "recruitment_interests",
    "/Recruitment/{key}/Offers.json": "recruitment_offers",
    "/Recruitment/{key}/PlayerSport.json": "recruitment_player_sport",
    "/Recruitment/{key}/FinalChoice.json": "recruitment_final_choice",
    "/Season/{season}/Roster/Embed.json": "season_roster_embed",
    "/PlayerInstitutionEvaluation/{key}.json": "player_institution_evaluation",
    "/League/{league}/DraftPicks/ConfigureEmbed/.json": "league_draft_picks",
}

# Real-capture fixture stem (tests/fixtures/sports247_site_pages/<stem>.json) per
# endpoint short. The OpenAPI spec types every numeric-string field as ``string``,
# but the parser (``_cast_numeric_strings``) widens them to Int64/Float64, so the
# returns-schemas take their column TYPES from parser output on these captures.
FIXTURE_FOR_SHORT: Dict[str, List[str]] = {
    "coach": ["coach"],
    "coach_alma_mater": ["coach_almamater"],
    "coach_hometown": ["coach_hometown"],
    "coach_ranking": ["coachranking"],
    "coach_rankings": ["coach_rankings"],
    "event": ["event"],
    "institution": ["institution"],
    "institution_list": ["institution_list"],
    "institution_location": ["institution_location"],
    "institution_timeline_events": ["institution_timeline"],
    "league_draft_picks": ["draft_picks"],
    "league_institutions": ["league_institutions"],
    "page_feeds": ["page_feeds"],
    "player": ["player"],
    "player_current_institution": ["player_currentinst"],
    "player_high_school": ["player_highschool"],
    "player_institution": ["player_institution"],
    "player_institution_evaluation": ["pi_evaluation"],
    "player_primary_sport": ["player_primarysport"],
    "player_search": ["player_search"],
    "playersport": ["playersport"],
    "playersport_institution": ["playersport_inst"],
    "playersport_rank_history": ["playersport_rankhist"],
    "position_rankings": ["position_ranks"],
    "recruit_interest": ["recruit_interest"],
    "recruitment_final_choice": ["recruitment_finalchoice"],
    "recruitment_institution": ["recruitment_inst"],
    "recruitment_interests": ["recruitment_interests"],
    "recruitment_offers": ["recruitment_offers"],
    "recruitment_player_sport": ["recruitment_playersport"],
    "season_current_expert_predictions": ["current_expert_pred", "expert_predictions"],
    "season_recruit_interest_events": ["season_recruit_int_ev"],
    "season_recruit_interests": ["season_recruit_ints"],
    "season_recruits": ["recruits_season"],
    "season_roster_embed": ["roster_embed"],
}
FIXTURE_DIR = ROOT / "tests/fixtures/sports247_site_pages"

_TYPE_RANK = {"logical": 0, "integer": 1, "numeric": 2, "character": 3}
_POLARS_TO_R = {"Int64": "integer", "Float64": "numeric", "Boolean": "logical"}


def observed_types(short: str) -> Dict[str, str]:
    """``{snake_col: r_type}`` from the parser run over this endpoint's fixtures.

    All-null columns carry no type signal and are skipped; if a column's type
    differs between fixtures (e.g. Int64 vs Float64) the widest wins
    (character > numeric > integer/logical).
    """
    import json

    from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page

    out: Dict[str, str] = {}
    for stem in FIXTURE_FOR_SHORT.get(short, []):
        df = parse_sports247_site_page(json.loads((FIXTURE_DIR / f"{stem}.json").read_text(encoding="utf-8")))
        for col, dt in df.schema.items():
            if df[col].null_count() == df.height:
                continue
            t = _POLARS_TO_R.get(str(dt), "character")
            if col not in out or _TYPE_RANK[t] > _TYPE_RANK[out[col]]:
                out[col] = t
    return out


# OpenAPI scalar type -> python-hint (params) and R-style (returns-schema)
_PY_TYPE = {"integer": "int", "number": "float", "string": "str", "boolean": "bool"}
_R_TYPE = {"integer": "integer", "number": "numeric", "string": "character", "boolean": "logical"}

# The flat-API renderer reserves ``{sport}`` / ``{league}`` path tokens for the
# ESPN-style league-slug substitution (generate.py::_sub_slugs), which would blank
# a path param literally named ``league``/``sport``. Bump such PATH-param names so
# the emitted ``{token}`` never collides with a reserved slug.
_RESERVED_SLUGS = {"sport", "league"}


def _path_py_name(snake: str) -> str:
    return f"{snake}_slug" if snake in _RESERVED_SLUGS else snake


# Returns-schema names are stem-prefixed (mirroring the sibling `sports247_teams`
# convention) because manual_column_descriptions.yaml is keyed by the bare
# ``schema:`` field across ALL stems — an un-prefixed entity name like ``coach`` /
# ``player`` / ``event`` would collide with another bucket's block.
_SCHEMA_PREFIX = "sports247_site_pages_"


def _schema_name(ref: str | None, short: str) -> str:
    return _SCHEMA_PREFIX + (underscore(ref) if ref else short)


def _write_yaml(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(obj, fh, sort_keys=True, default_flow_style=False, allow_unicode=True)


def _clean_generated_schema_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for schema_path in path.glob("*.yaml"):
        schema_path.unlink()


def _ref_name(schema_node: Dict[str, Any]) -> str | None:
    """Component name a 200-response schema resolves to (array items or object)."""
    if schema_node.get("type") == "array":
        ref = schema_node.get("items", {}).get("$ref")
    else:
        ref = schema_node.get("$ref")
    return ref.rsplit("/", 1)[-1] if ref else None


def _response_ref(op: Dict[str, Any]) -> str | None:
    for cc in op.get("responses", {}).get("200", {}).get("content", {}).values():
        name = _ref_name(cc.get("schema", {}))
        if name:
            return name
    return None


def _resolve(node: Dict[str, Any], comps: Dict[str, Any]) -> Dict[str, Any]:
    """Collapse an ``allOf: [$ref]`` / ``$ref`` node to the referenced schema."""
    if "$ref" in node:
        return comps[node["$ref"].rsplit("/", 1)[-1]]
    if "allOf" in node:
        for part in node["allOf"]:
            if "$ref" in part:
                return comps[part["$ref"].rsplit("/", 1)[-1]]
    return node


def _flatten_columns(props: Dict[str, Any], comps: Dict[str, Any], prefix: str = "") -> List[Tuple[str, str]]:
    """Flatten a schema's properties into ``(raw_camel_key, r_type)`` pairs.

    Scalars first, then nested object / ``allOf``-$ref expansions — mirroring the
    ``pandas.json_normalize(sep="_")`` column order the parser produces at runtime.
    """
    scalars: List[Tuple[str, str]] = []
    nested: List[Tuple[str, str]] = []
    for name, node in props.items():
        key = f"{prefix}{name}"
        resolved = _resolve(node, comps)
        sub = resolved.get("properties")
        if resolved.get("type") == "object" and sub:
            nested.extend(_flatten_columns(sub, comps, prefix=f"{key}_"))
        elif ("allOf" in node or "$ref" in node) and resolved.get("properties"):
            nested.extend(_flatten_columns(resolved["properties"], comps, prefix=f"{key}_"))
        else:
            scalars.append((key, _R_TYPE.get(resolved.get("type"), "character")))
    return scalars + nested


def _example_args(path: str, example_url: str | None, path_types: Dict[str, str]) -> Dict[str, Any]:
    """Extract path-arg example values from the route's ``x-example-url``."""
    if not example_url:
        return {}
    ex_path = example_url.split("://", 1)[-1].split("/", 1)[-1].split("?", 1)[0]
    tmpl_segs = path.lstrip("/").split("/")
    ex_segs = ("/" + ex_path).lstrip("/").split("/")
    if len(tmpl_segs) != len(ex_segs):
        return {}
    args: Dict[str, Any] = {}
    for tmpl, val in zip(tmpl_segs, ex_segs):
        if tmpl.startswith("{") and tmpl.endswith("}"):
            snake = _path_py_name(underscore(tmpl[1:-1]))
            args[snake] = int(val) if path_types.get(snake) == "int" and val.isdigit() else val
    return args


def _endpoint(path: str, op: Dict[str, Any], comps: Dict[str, Any]) -> Dict[str, Any]:
    short = _SHORT[path]
    ref = _response_ref(op)
    schema = _schema_name(ref, short)

    path_params: List[Dict[str, Any]] = []
    extra_params: List[Dict[str, Any]] = []
    emitted_path = path
    path_types: Dict[str, str] = {}
    for p in op.get("parameters", []):
        pname = p["name"]
        ptype = _PY_TYPE.get(p.get("schema", {}).get("type"), "str")
        base_snake = underscore(pname.replace(".", "_"))
        if p.get("in") == "path":
            snake = _path_py_name(base_snake)
            emitted_path = emitted_path.replace(f"{{{pname}}}", f"{{{snake}}}")
            path_types[snake] = ptype
            entry: Dict[str, Any] = {"name": snake, "type": ptype, "required": bool(p.get("required"))}
            if snake == "season":
                entry["description"] = "Season path segment in `{year}-{Sport}` form, e.g. `2026-Football`."
            path_params.append(entry)
        else:
            extra_params.append({"name": base_snake, "query_key": pname, "type": ptype})

    entry = {
        "short": short,
        "summary": op.get("summary", f"GET {path}"),
        "path": emitted_path,
        "returns_schema": f"native/sports247_site_pages/{schema}",
        "parser": "parse_sports247_site_page",
    }
    if path_params:
        entry["path_params"] = path_params
    if extra_params:
        entry["extra_params"] = extra_params
    ex = _example_args(path, op.get("x-example-url"), path_types)
    if ex:
        entry["example_args"] = ex
    return entry


def main() -> None:
    spec = yaml.safe_load(SPEC.read_text(encoding="utf-8"))
    comps = spec["components"]["schemas"]
    paths = spec["paths"]

    endpoints: List[Dict[str, Any]] = []
    used_schemas: Dict[str, str] = {}  # schema-file-name -> component name
    observed: Dict[str, Dict[str, str]] = {}  # schema-file-name -> {col: r_type}
    for path, item in paths.items():
        op = item.get("get")
        if not op:
            continue
        endpoints.append(_endpoint(path, op, comps))
        ref = _response_ref(op)
        if ref:
            sname = _schema_name(ref, _SHORT[path])
            used_schemas[sname] = ref
            for col, t in observed_types(_SHORT[path]).items():
                prev = observed.setdefault(sname, {}).get(col)
                observed[sname][col] = t if prev is None else max(prev, t, key=_TYPE_RANK.__getitem__)
    endpoints.sort(key=lambda e: e["short"])

    doc = {
        "api": "sports247_site_pages",
        "host": "https://247sports.com",
        "name_pattern": "sports247_site_pages_{short}",
        "module": "sports247_site_pages",
        "parser_module": "cfb.sports247_site_pages_parsers",
        "getter_module": "sportsdataverse.cfb.sports247_site_pages_runtime",
        "qualifier": "",
        "passthrough_query": True,
        "runtime_imports": ["_get"],
        "endpoints": endpoints,
    }
    _write_yaml(ROOT / "tools/codegen/endpoints/sports247_site_pages.yaml", doc)

    schema_dir = ROOT / "tools/codegen/schemas/native/sports247_site_pages"
    _clean_generated_schema_dir(schema_dir)
    for schema_name, comp_name in sorted(used_schemas.items()):
        cols = _flatten_columns(comps[comp_name].get("properties", {}), comps)
        # Parser output wins over the spec's types; columns the parser emits but the
        # spec omits (e.g. institution_list.website) are appended.
        obs = observed.get(schema_name, {})
        columns = [
            {"name": underscore(raw), "type": obs.get(underscore(raw), rtype), "description": ""} for raw, rtype in cols
        ]
        known = {c["name"] for c in columns}
        columns += [{"name": n, "type": t, "description": ""} for n, t in obs.items() if n not in known]
        schema = {"schema": schema_name, "kind": "dataframe", "columns": columns}
        _write_yaml(schema_dir / f"{schema_name}.yaml", schema)

    print(f"sports247_site_pages: {len(endpoints)} endpoints, {len(used_schemas)} schemas")


if __name__ == "__main__":
    main()
