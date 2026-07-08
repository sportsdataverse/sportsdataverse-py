"""Generate the ``sports247`` flat-API stem (247Sports Recruit Database, RDB) —
``endpoints/sports247.yaml`` + ``schemas/native/sports247/*.yaml``.

Idempotent: re-running reproduces the committed YAML byte-for-byte (the
regression guard in ``tests/codegen/test_gen_sports247.py``).

Run: ``python tools/codegen/gen_sports247.py``

Why the structure is curated, not spec-derived
-----------------------------------------------
The RDB's *live* API diverges from its published OpenAPI spec
(``247sports/recruit-database.openapi.yaml``): e.g. ``/rdb/v1/recruits`` accepts
``pagesize`` (lowercase) plus ``positionAbbreviation`` / ``stateAbbreviation``
filters that the spec never declares, and ``institutionrankings`` exposes a
different, reordered query set. So the per-endpoint param signatures live in the
``_OVERRIDES`` table (captured from the real API), and the returns-schema column
lists live in ``_SCHEMA_COLUMNS`` (mined from real captures) — neither is
reconstructable from the spec.

Guest-usability is likewise a curated allowlist (``_GUEST_USABLE``): *every* RDB
operation carries ``security: [bearer: []]`` + a ``403``, so the spec cannot
self-classify which routes the free guest JWT unlocks. This mirrors
``gen_nba_stats``'s ``_APPLICABLE = ("live",)`` capture-confirmed gate. The
free guest token unlocks these 11 GET routes; the remaining GET routes
(``playerSportRankings``, ``unrankedRecruits``, ``biggestMovers``,
``archivedPlayerRankings``, ``transferPlayerSportRankings``,
``playerSportsUnderSpecialEvaluation``, ``unrankedtransfers``, ``photos``, ...)
stay ``403`` without a logged-in/premium session and are **omitted** from the
wrapper set (functional-by-default). Adding a route = extend ``_GUEST_USABLE`` +
``_OVERRIDES`` + ``_SCHEMA_COLUMNS`` (see the Track-3 plan).

The spec is read only for optional provenance/validation (``--check`` style:
assert each allowlisted route is a real GET, print the bearer-only bucket). It
is NOT required to run the generator, and its absence never changes the emitted
bytes — output depends solely on the curated tables above.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

ROOT = Path(__file__).resolve().parents[2]
ENDPOINTS_PATH = ROOT / "tools/codegen/endpoints/sports247.yaml"
SCHEMA_DIR = ROOT / "tools/codegen/schemas/native/sports247"

# Static stem header (mirrors the committed endpoints/sports247.yaml).
_STEM: Dict[str, Any] = {
    "api": "sports247",
    "host": "https://ipa.247sports.com",
    "name_pattern": "sports247_{short}",
    "module": "sports247",
    "parser_module": "cfb.sports247_parsers",
    "getter_module": "sportsdataverse.cfb.sports247_runtime",
    "qualifier": "",
    "passthrough_query": True,
    "runtime_imports": ["_get"],
}

# ---------------------------------------------------------------------------
# Curated tables (see module docstring). Do not reorder existing entries — the
# list order fixes the wrapper emission order in the generated module.
# ---------------------------------------------------------------------------
_GUEST_USABLE: Tuple[str, ...] = (
    "teams",
    "institution_rankings",
    "recruits",
    "transfers",
    "coaches",
    "transfer_portal_player_feed",
    "composite_team_ranking_feed",
    "transfer_portal_team_feed",
    "target_predictions",
    "sport_years",
    "tags_autocomplete",
    "positions",
)

_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "teams": {
        "summary": "247Sports RDB college team directory (teamId / institutionKey / conference) for a sport.",
        "path": "/rdb/v1/teams/",
        "extra_params": [
            {
                "name": "sport_key",
                "query_key": "sportKey",
                "type": "int",
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
            {"name": "year", "query_key": "year", "type": "int"},
            {"name": "institution_type", "query_key": "institutionType", "type": "str"},
        ],
        "parser": "parse_sports247_teams",
    },
    "institution_rankings": {
        "summary": "247Sports team recruiting-class rankings (247 rank/rating + industry composite) for a sport and class year.",
        "path": "/rdb/v1/rankings/{sport_key}/{year}/institutionrankings/",
        "path_params": [
            {"name": "year", "type": "int|str", "required": True},
            {
                "name": "sport_key",
                "type": "int",
                "required": False,
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
        ],
        "extra_params": [
            {"name": "page_size", "query_key": "pagesize", "type": "int", "default": 50},
            {"name": "page", "query_key": "page", "type": "int"},
            {"name": "use_composite", "query_key": "useComposite", "type": "str"},
            {"name": "conference_abbreviation", "query_key": "conferenceAbbreviation", "type": "str"},
            {"name": "institution_key", "query_key": "institutionKey", "type": "int"},
        ],
        "parser": "parse_sports247_institution_rankings",
        "example_args": {"year": 2026, "sport_key": 1},
    },
    "recruits": {
        "summary": "247Sports individual recruit rankings for a sport and class year (247 + industry-composite ratings, stars, commit status; paginated).",
        "path": "/rdb/v1/recruits/",
        "extra_params": [
            {
                "name": "sport_key",
                "query_key": "sportKey",
                "type": "int",
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
            {"name": "year", "query_key": "year", "type": "int", "default": 2026},
            {"name": "page_size", "query_key": "pagesize", "type": "int", "default": 50},
            {"name": "page", "query_key": "page", "type": "int"},
            {"name": "position_abbreviation", "query_key": "positionAbbreviation", "type": "str"},
            {"name": "state_abbreviation", "query_key": "stateAbbreviation", "type": "str"},
        ],
        "parser": "parse_sports247_result_set",
        "example_args": {"year": 2026, "sport_key": 1},
    },
    "transfers": {
        "summary": "247Sports transfer-portal player entries for a sport and year (paginated).",
        "path": "/rdb/v1/transfers/",
        "extra_params": [
            {
                "name": "sport_key",
                "query_key": "sportKey",
                "type": "int",
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
            {"name": "year", "query_key": "year", "type": "int", "default": 2026},
            {"name": "page_size", "query_key": "pagesize", "type": "int", "default": 50},
            {"name": "page", "query_key": "page", "type": "int"},
        ],
        "parser": "parse_sports247_result_set",
        "example_args": {"year": 2026, "sport_key": 1},
    },
    "coaches": {
        "summary": "247Sports coach recruiting rankings for a sport and year (paginated).",
        "path": "/rdb/v1/coaches/",
        "extra_params": [
            {
                "name": "sport_key",
                "query_key": "sportKey",
                "type": "int",
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
            {"name": "year", "query_key": "year", "type": "int", "default": 2026},
            {"name": "page_size", "query_key": "pageSize", "type": "int", "default": 50},
            {"name": "page", "query_key": "page", "type": "int"},
        ],
        "parser": "parse_sports247_result_set",
        "example_args": {"year": 2026, "sport_key": 1},
    },
    "transfer_portal_player_feed": {
        "summary": "247Sports transfer-portal player ranking feed for a sport and class year.",
        "path": "/rdb/v1/rankings/{sport_key}/{year}/transferPortalPlayerfeed/",
        "path_params": [
            {"name": "year", "type": "int|str", "required": True},
            {
                "name": "sport_key",
                "type": "int",
                "required": False,
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
        ],
        "extra_params": [{"name": "page_size", "query_key": "pageSize", "type": "int", "default": 50}],
        "parser": "parse_sports247_result_set",
        "example_args": {"year": 2026, "sport_key": 1},
    },
    "composite_team_ranking_feed": {
        "summary": "247Sports composite team recruiting-class ranking feed for a sport and class year.",
        "path": "/rdb/v1/rankings/{sport_key}/{year}/compositeTeamRankingFeed/",
        "path_params": [
            {"name": "year", "type": "int|str", "required": True},
            {
                "name": "sport_key",
                "type": "int",
                "required": False,
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
        ],
        "extra_params": [{"name": "page_size", "query_key": "pageSize", "type": "int", "default": 50}],
        "parser": "parse_sports247_result_set",
        "example_args": {"year": 2026, "sport_key": 1},
    },
    "transfer_portal_team_feed": {
        "summary": "247Sports transfer-portal team ranking feed for a sport and class year.",
        "path": "/rdb/v1/rankings/{sport_key}/{year}/transferPortalOnlyTeamFeed/",
        "path_params": [
            {"name": "year", "type": "int|str", "required": True},
            {
                "name": "sport_key",
                "type": "int",
                "required": False,
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
        ],
        "extra_params": [{"name": "page_size", "query_key": "pageSize", "type": "int", "default": 50}],
        "parser": "parse_sports247_result_set",
        "example_args": {"year": 2026, "sport_key": 1},
    },
    "target_predictions": {
        "summary": '247Sports current expert target predictions ("crystal ball") for a site, class year, and sport.',
        "path": "/rdb/v1/sites/{site_key}/years/{year}/sports/{sport_key}/currentTargetPredictions/",
        "path_params": [
            {"name": "site_key", "type": "int|str", "required": True},
            {"name": "year", "type": "int|str", "required": True},
            {
                "name": "sport_key",
                "type": "int",
                "required": False,
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
        ],
        "extra_params": [{"name": "page_size", "query_key": "pageSize", "type": "int", "default": 50}],
        "parser": "parse_sports247_result_set",
        "example_args": {"site_key": 1, "year": 2026, "sport_key": 1},
    },
    "sport_years": {
        "summary": "Class years for which the 247Sports RDB has data for a given sport.",
        "path": "/rdb/v1/sports/{sport_key}/year/",
        "path_params": [
            {
                "name": "sport_key",
                "type": "int",
                "required": False,
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            }
        ],
        "parser": "parse_sports247_result_set",
        "example_args": {"sport_key": 1},
    },
    "tags_autocomplete": {
        "summary": "247Sports taggable-entity autocomplete (players / teams / institutions) by name prefix.",
        "path": "/rdb/v1/tags/autocomplete/",
        "extra_params": [
            {"name": "default_name", "query_key": "defaultName", "type": "str"},
            {"name": "items", "query_key": "items", "type": "int", "default": 10},
        ],
        "parser": "parse_sports247_result_set",
        "example_args": {"default_name": "smith"},
    },
    "positions": {
        "summary": "247Sports position lookup for a sport (position group, abbreviation, and key).",
        "path": "/rdb/v1/positions/",
        "extra_params": [
            {
                "name": "sport_key",
                "query_key": "sportKey",
                "type": "int",
                "default": 1,
                "description": "247Sports sport key (1 = football, 2 = basketball).",
            },
            {"name": "year", "query_key": "year", "type": "int"},
            {"name": "ranking_key", "query_key": "rankingKey", "type": "int"},
        ],
        "parser": "parse_sports247_result_set",
        "example_args": {"sport_key": 1},
    },
}

# Returns-schema column lists, mined from real captures. Descriptions live in
# tools/codegen/manual_column_descriptions.yaml (schema-keyed), NEVER here.
_SCHEMA_COLUMNS: Dict[str, List[Tuple[str, str]]] = {
    "teams": [
        ("name", "character"),
        ("team_id", "integer"),
        ("institution_key", "integer"),
        ("conference", "character"),
        ("conference_abbreviation", "character"),
        ("sport", "character"),
        ("type", "character"),
    ],
    "institution_rankings": [
        ("name", "character"),
        ("full_name", "character"),
        ("conference_rank", "integer"),
        ("conference_composite_rank", "integer"),
        ("rank", "integer"),
        ("composite_rank", "integer"),
        ("institution_key", "integer"),
        ("team_key", "integer"),
        ("average_rating", "double"),
        ("rating", "double"),
        ("composite_rating", "double"),
        ("average_composite_rating", "double"),
        ("default_asset", "character"),
        ("alternate_asset", "character"),
        ("light_asset", "character"),
        ("high_school_ranking_position", "character"),
        ("transfer_points", "character"),
        ("transfer_number", "integer"),
        ("five_stars", "integer"),
        ("composite_five_stars", "integer"),
        ("four_stars", "integer"),
        ("composite_four_stars", "integer"),
        ("three_stars", "integer"),
        ("composite_three_stars", "integer"),
        ("commits", "integer"),
        ("site_key", "integer"),
        ("institution_root_path", "character"),
        ("ranking_date", "character"),
        ("city", "character"),
        ("state", "character"),
        ("state_abbreviation", "character"),
        ("institution_ranking_url", "character"),
    ],
    "recruits": [
        ("key", "integer"),
        ("cbs_key", "integer"),
        ("first_name", "character"),
        ("last_name", "character"),
        ("profile_url", "character"),
        ("default_asset_url", "character"),
        ("primary_position", "character"),
        ("composite_rating", "double"),
        ("composite_star_rating", "integer"),
        ("composite_national_rank", "integer"),
        ("composite_position_rank", "integer"),
        ("composite_state_rank", "integer"),
        ("signed_institution", "double"),
        ("home_town_city", "character"),
        ("home_town_state", "character"),
        ("committed_institution_institution_key", "integer"),
        ("committed_institution_team_key", "integer"),
        ("committed_institution_cbs_key", "integer"),
        ("committed_institution_name", "character"),
        ("committed_institution_abbreviation", "character"),
        ("committed_institution_full_name", "character"),
        ("current_institution_institution_key", "integer"),
        ("current_institution_team_key", "double"),
        ("current_institution_cbs_key", "double"),
        ("current_institution_name", "character"),
        ("current_institution_abbreviation", "character"),
        ("current_institution_full_name", "character"),
        ("signed_institution_institution_key", "double"),
        ("signed_institution_team_key", "double"),
        ("signed_institution_cbs_key", "double"),
        ("signed_institution_name", "character"),
        ("signed_institution_abbreviation", "character"),
        ("signed_institution_full_name", "character"),
    ],
    "transfers": [
        ("player_key", "integer"),
        ("player_first_name", "character"),
        ("player_last_name", "character"),
        ("player_avatar", "character"),
        ("player_transfer_date", "character"),
        ("player_transfer_rating", "double"),
        ("player_high_school_rating", "double"),
        ("player_rating", "double"),
        ("player_star_rating", "double"),
        ("player_transfer_rank", "double"),
        ("player_high_school_rank", "double"),
        ("player_rank", "double"),
        ("player_rank_trend", "double"),
        ("player_institution_status", "character"),
        ("player_position", "character"),
        ("player_position_key", "integer"),
        ("player_eligibility_type", "character"),
        ("player_eligibility_years", "double"),
        ("player_state_rank", "double"),
        ("player_status", "character"),
        ("player_status_date", "character"),
        ("player_transfer_source_institution", "character"),
        ("player_transfer_source_institution_key", "integer"),
        ("player_transfer_source_logo", "character"),
        ("player_transfer_source_default_asset", "character"),
        ("player_transfer_source_alternate_asset", "character"),
        ("player_transfer_source_light_asset", "character"),
        ("player_transfer_source_institution_root_path", "character"),
        ("player_transfer_destination", "character"),
        ("player_position_group_key", "integer"),
        ("player_position_group_name", "character"),
        ("player_position_rank", "double"),
        ("player_last_update_date", "character"),
        ("player_transfer_commit_date_time", "character"),
        ("player_weight", "integer"),
        ("player_height", "character"),
        ("player_player_profile_url", "character"),
        ("player_start_date", "character"),
        ("player_end_date", "character"),
    ],
    "coaches": [
        ("key", "integer"),
        ("cbs_key", "integer"),
        ("first_name", "character"),
        ("last_name", "character"),
        ("profile_url", "character"),
        ("default_asset_url", "character"),
        ("composite_rating", "character"),
        ("current_job", "double"),
        ("average_composite_rating", "character"),
        ("overall_rank", "character"),
        ("division_rank", "character"),
        ("conference_rank", "character"),
        ("current_job_institution_key", "double"),
        ("current_job_team_key", "double"),
        ("current_job_cbs_key", "double"),
        ("current_job_name", "character"),
        ("current_job_abbreviation", "character"),
        ("current_job_full_name", "character"),
    ],
    "transfer_portal_player_feed": [
        ("key", "integer"),
        ("target_institution", "character"),
        ("target_institution_key", "character"),
        ("full_name", "character"),
        ("position_abbr", "character"),
        ("current_institution", "character"),
        ("current_institution_key", "integer"),
        ("current_institution_state_abbreviation", "character"),
        ("current_institution_city", "character"),
        ("first_name", "character"),
        ("last_name", "character"),
        ("state_abbreviation", "character"),
        ("player_image", "character"),
        ("star_rating", "integer"),
        ("group_rank", "integer"),
        ("position_rank", "integer"),
        ("state_rank", "integer"),
        ("formatted_height", "character"),
        ("weight", "double"),
    ],
    "composite_team_ranking_feed": [
        ("name", "character"),
        ("full_name", "character"),
        ("state_abbreviation", "character"),
        ("conference_name", "character"),
        ("key", "integer"),
        ("logo", "character"),
        ("alternate_logo", "character"),
        ("position", "integer"),
        ("team_ranking_position", "integer"),
        ("transfer_ranking_position", "integer"),
        ("overall_rank", "integer"),
        ("composite_overall_rank", "integer"),
        ("conference_rank", "integer"),
        ("composite_conference_rank", "integer"),
        ("five_stars", "integer"),
        ("composite_five_stars", "integer"),
        ("four_stars", "integer"),
        ("composite_four_stars", "integer"),
        ("three_stars", "integer"),
        ("composite_three_stars", "integer"),
        ("average_rating", "integer"),
        ("composite_average_rating", "double"),
        ("rating", "integer"),
        ("composite_rating", "double"),
    ],
    "transfer_portal_team_feed": [
        ("name", "character"),
        ("key", "integer"),
        ("logo", "character"),
        ("alternate_logo", "character"),
        ("position", "integer"),
        ("number_of_transfers", "integer"),
        ("transfer_points", "double"),
    ],
    "target_predictions": [
        ("player_key", "integer"),
        ("player_institution_key", "integer"),
        ("prediction_type", "integer"),
        ("rating", "double"),
        ("star_rating", "integer"),
        ("position", "character"),
        ("weight", "double"),
        ("height", "character"),
        ("prediction", "character"),
        ("prediction_level", "integer"),
        ("image", "character"),
        ("alt_image", "character"),
        ("light_image", "character"),
        ("player_name", "character"),
        ("player_image", "character"),
        ("prediction_date", "character"),
        ("expert_name", "character"),
        ("expert_alias", "character"),
        ("expert_key", "integer"),
        ("expert_role", "character"),
        ("expert_image", "character"),
        ("expert_prediction_year", "integer"),
        ("expert_yearly_total_correct", "integer"),
        ("expert_yearly_total_made", "integer"),
        ("expert_all_time_total_correct", "integer"),
        ("expert_all_time_total_made", "integer"),
        ("prediction_page_url", "character"),
    ],
    "sport_years": [
        ("value", "integer"),
    ],
    "tags_autocomplete": [
        ("id", "character"),
        ("name", "character"),
        ("type", "character"),
        ("annotation", "character"),
    ],
    "positions": [
        ("group", "character"),
        ("group_key", "integer"),
        ("name", "character"),
        ("label", "character"),
        ("value", "character"),
    ],
}


class _Sports247Dumper(yaml.SafeDumper):
    """SafeDumper that double-quotes prose scalars (any string with a space).

    Reproduces the hand-authored endpoints/sports247.yaml style: summaries and
    param ``description`` strings are double-quoted; keys, paths, snake_case
    names, ``int|str`` types, and the empty ``qualifier`` stay plain / single.
    """


def _repr_str(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    style = '"' if " " in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


_Sports247Dumper.add_representer(str, _repr_str)


def _dump_yaml(obj: Any) -> str:
    return yaml.dump(
        obj,
        Dumper=_Sports247Dumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=1_000_000,
    )


def _endpoint_entry(short: str) -> Dict[str, Any]:
    """Build one ordered endpoint dict: short, returns_schema, then curated body."""
    override = _OVERRIDES[short]
    entry: Dict[str, Any] = {
        "short": short,
        "returns_schema": f"native/sports247/sports247_{short}",
    }
    # Preserve the committed key order: summary, path, path_params?, extra_params?,
    # parser, example_args?.
    for key in ("summary", "path", "path_params", "extra_params", "parser", "example_args"):
        if key in override:
            entry[key] = override[key]
    return entry


def _schema_doc(short: str) -> Dict[str, Any]:
    return {
        "schema": f"sports247_{short}",
        "kind": "dataframe",
        "columns": [{"name": name, "type": type_, "description": ""} for name, type_ in _SCHEMA_COLUMNS[short]],
    }


def _spec_path(short: str) -> str:
    """Map a curated wrapper path to its OpenAPI spec path (camelCase, no slash)."""
    path = _OVERRIDES[short]["path"].rstrip("/")
    for snake, camel in (("{sport_key}", "{sportKey}"), ("{site_key}", "{siteKey}")):
        path = path.replace(snake, camel)
    return path


def _load_spec() -> Optional[Dict[str, Any]]:
    """Locate + load the RDB OpenAPI spec, or None if unavailable (never fatal)."""
    candidates: List[Path] = []
    env = os.environ.get("SDV_INTERNAL_REFS_REPO")
    if env:
        candidates.append(Path(env) / "247sports/recruit-database.openapi.yaml")
    candidates += [
        ROOT.parents[2] / "sdv-internal-refs/247sports/recruit-database.openapi.yaml",
        Path.home() / "Documents/sdv-internal-refs/247sports/recruit-database.openapi.yaml",
    ]
    for path in candidates:
        if path.is_file():
            return yaml.safe_load(path.read_text(encoding="utf-8"))
    return None


def _validate_against_spec() -> None:
    """Provenance/validation only — does not affect the emitted bytes."""
    spec = _load_spec()
    if spec is None:
        print("sports247: spec not found (set SDV_INTERNAL_REFS_REPO to validate); skipping spec cross-check")
        return
    paths = spec.get("paths", {})
    guest_spec_paths = set()
    for short in _GUEST_USABLE:
        sp = _spec_path(short)
        guest_spec_paths.add(sp)
        item = paths.get(sp)
        assert item and "get" in item, f"guest-usable route {short!r} ({sp}) is not a GET in the spec"
    bearer_only = sorted(p for p, item in paths.items() if "get" in item and p not in guest_spec_paths)
    print(f"sports247: {len(_GUEST_USABLE)} guest-usable GET routes validated against spec")
    print(f"sports247: {len(bearer_only)} bearer-only GET routes omitted from the wrapper set:")
    for p in bearer_only:
        print(f"    - {p}")


def _write(path: Path, text: str) -> None:
    # newline="\n" forces LF on Windows (repo policy: [tool.ruff.format] line-ending = "lf").
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)


def main() -> None:
    _validate_against_spec()
    doc = dict(_STEM)
    doc["endpoints"] = [_endpoint_entry(short) for short in _GUEST_USABLE]
    ENDPOINTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _write(ENDPOINTS_PATH, _dump_yaml(doc))
    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
    for short in _GUEST_USABLE:
        _write(SCHEMA_DIR / f"sports247_{short}.yaml", _dump_yaml(_schema_doc(short)))
    print(f"sports247: wrote {len(_GUEST_USABLE)} endpoints + {len(_GUEST_USABLE)} schemas")


if __name__ == "__main__":
    main()
