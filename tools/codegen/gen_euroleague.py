"""Generate the ``euroleague`` flat-API endpoint YAML + returns-schemas from the
EuroLeague Competition Engine OpenAPI spec (``api-live.euroleague.net/v2``, keyless).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_asa.py``, with the shared plumbing in ``gen_soccer_common.py``.

The generator reads the frozen spec from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/euroleague.yaml`` -- one endpoint per GET route (7),
  host ``https://api-live.euroleague.net/v2``, getter
  ``sportsdataverse.euroleague.euroleague_runtime._get`` (sends
  ``Accept: application/json``; the API answers XML otherwise).
* ``tools/codegen/schemas/native/euroleague/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running the parser over the committed capture.

Path tokens are emitted snake_cased (``{competitionCode}`` -> ``{competition_code}``)
because the renderer substitutes a path token by the python argument of the same
name. The spec uses no ``{league}`` / ``{sport}`` token; the build asserts it, since
the renderer would substitute those as ESPN slugs and blank the segment.

Run: ``python tools/codegen/gen_euroleague.py``
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
from sportsdataverse.euroleague.euroleague_parsers import parse_euroleague

HOST = "https://api-live.euroleague.net/v2"
SPEC = "euroleague.openapi.yaml"
STEM = "euroleague"

# The committed samples are pinned to one season + game (sdv-internal-refs
# euroleague/tools/capture.py SEASON / GAME); captures live under captures/<SEASON>/.
SEASON = "E2025"
GAME = 1
_CAPTURE_VALUES = {"competitionCode": "E", "seasonCode": SEASON, "gameCode": str(GAME)}

# Live-verified example arguments for the docs (E = EuroLeague, E2025 = 2025-26).
_EXAMPLE: Dict[str, Any] = {"competition_code": "E", "season_code": SEASON, "game_code": GAME}

# operationId prefixes the shorts drop: every route hangs off a competition + season.
_OP_PREFIXES = ("competitions_competitionCode_seasons_seasonCode_", "competitions_competitionCode_")

_TOKEN = re.compile(r"\{([^}]+)\}")

# The spec's paging params carry no description.
_QUERY_FALLBACK = {"limit": "Page size (number of rows to return).", "offset": "Row offset into the full list."}

# Capture-derived spec: no property docs. Curated prose for the stable EuroLeague
# vocabulary, keyed by the UNPREFIXED leaf so one entry covers ``code`` /
# ``season_code`` / ``local_club_code`` alike (container prefixes are stripped
# recursively by :class:`_Prefixed`).
_LEAF: Dict[str, str] = {
    "code": "EuroLeague code of the entity (competition, season, club, person or venue; Utf8 join key).",
    "name": "Display name.",
    "alias": "Short display alias.",
    "competition_code": "Competition code (E = EuroLeague, U = EuroCup; Utf8 join key).",
    "season_code": "Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key).",
    "game_code": "Game number within the season (1-based; Utf8 join key).",
    "id": "Provider identifier for the entity (Utf8 join key).",
    "identifier": "Season-qualified game identifier, e.g. E2025_1.",
    "external_id": "External (statistics-provider) identifier of the person (Utf8 join key).",
    "tv_code": "Three-letter broadcast abbreviation of the club.",
    "venue_code": "Code of the venue the club or game plays at (Utf8 join key).",
    "year": "Start year of the season.",
    "start_date": "Start date (ISO 8601).",
    "end_date": "End date (ISO 8601).",
    "activation_date": "Date the season was activated in the engine (ISO 8601).",
    "round": "Round number within the season.",
    "round_name": "Display name of the round.",
    "round_alias": "Short alias of the round.",
    "index": "Ordinal position of the round within the season.",
    "phase_type_code": "Phase type code (RS = regular season, PO = playoffs, ...).",
    "dates_formmated": "Human-readable date range of the round (sic: the API spells it this way).",
    "min_game_start_date": "Earliest game start in the round (ISO 8601).",
    "max_game_start_date": "Latest game start in the round (ISO 8601).",
    "abbreviated_name": "Abbreviated display name.",
    "editorial_name": "Editorial (long-form) display name.",
    "club_permanent_name": "Permanent club name independent of sponsor naming.",
    "club_permanent_alias": "Permanent club alias independent of sponsor naming.",
    "is_virtual": "Whether the club is a placeholder rather than a real club.",
    "images_crest": "URL of the club crest image.",
    "country_code": "ISO country code.",
    "country_name": "Country name.",
    "partials1": "Points scored in the 1st quarter.",
    "partials2": "Points scored in the 2nd quarter.",
    "partials3": "Points scored in the 3rd quarter.",
    "partials4": "Points scored in the 4th quarter.",
    "extra_periods": "Points scored per overtime period, JSON-encoded.",
    "city": "City the club is based in.",
    "address": "Street address.",
    "phone": "Contact phone number.",
    "president": "Club president.",
    "sponsor": "Club sponsor.",
    "tickets_url": "Ticketing URL.",
    "twitter_account": "Twitter / X handle.",
    "website": "Official website URL.",
    "active": "Whether the record is currently active.",
    "dorsal": "Jersey number as displayed.",
    "dorsal_raw": "Jersey number as stored by the engine.",
    "position": "Position code of the player (engine integer).",
    "position_name": "Position name of the player.",
    "type": "Person type code (J = player, E = coach, ...).",
    "type_name": "Person type name.",
    "order": "Display order within the roster.",
    "last_team": "Previous club of the person.",
    "date": "Scheduled tip-off (ISO 8601, venue local time).",
    "local_date": "Scheduled tip-off in venue local time (ISO 8601).",
    "utc_date": "Scheduled tip-off in UTC (ISO 8601).",
    "local_time_zone": "UTC offset of the venue, in hours.",
    "played": "Whether the game has been played.",
    "game_status": "Game status (scheduled, live, result).",
    "confirmed_date": "Whether the game date is confirmed.",
    "confirmed_hour": "Whether the tip-off time is confirmed.",
    "is_neutral_venue": "Whether the game is played at a neutral venue.",
    "audience": "Attendance.",
    "audience_confirmed": "Whether the attendance figure is confirmed.",
    "social_feed": "Social-media hashtag or feed tag for the game.",
    "operations_code": "Engine operations code of the game.",
    "score": "Final score of the side.",
    "standings_score": "Score of the side as counted for the standings.",
    "capacity": "Seating capacity of the venue.",
    "notes": "Venue notes.",
    "images_medium": "URL of the medium-size venue image.",
    "images_vertical_small": "URL of the small portrait image.",
    "is_group_phase": "Whether the phase is played in groups.",
    "raw_name": "Group name as stored by the engine.",
    "players": "Per-player box-score rows for the side, JSON-encoded.",
    "alias_raw": "Short alias as stored by the engine.",
    "passport_name": "Given name as on the passport.",
    "passport_surname": "Surname as on the passport.",
    "jersey_name": "Name printed on the jersey.",
    "height": "Height in centimetres.",
    "weight": "Weight in kilograms.",
    "birth_date": "Date of birth (ISO 8601).",
    "instagram_account": "Instagram handle.",
    "facebook_account": "Facebook handle.",
    "is_referee": "Whether the person is a referee.",
    "referee4": "Fourth referee (null unless a fourth official is assigned).",
    "winner": "Winning club of the season (null while in progress).",
}

# Box-score stat leaves; they appear as ``local_team_*`` / ``local_total_*`` /
# ``road_team_*`` / ``road_total_*`` columns and resolve through :class:`_Prefixed`.
_STAT: Dict[str, str] = {
    "time_played": "Seconds played.",
    "valuation": "Performance index rating (PIR).",
    "points": "Points.",
    "field_goals_made2": "Two-point field goals made.",
    "field_goals_attempted2": "Two-point field goals attempted.",
    "field_goals_made3": "Three-point field goals made.",
    "field_goals_attempted3": "Three-point field goals attempted.",
    "free_throws_made": "Free throws made.",
    "free_throws_attempted": "Free throws attempted.",
    "field_goals_made_total": "Field goals made.",
    "field_goals_attempted_total": "Field goals attempted.",
    "accuracy_made": "Shots made (accuracy numerator).",
    "accuracy_attempted": "Shots attempted (accuracy denominator).",
    "assistances": "Assists.",
    "steals": "Steals.",
    "turnovers": "Turnovers.",
    "blocks_favour": "Blocks made.",
    "blocks_against": "Shots blocked by the opponent.",
    "fouls_commited": "Personal fouls committed.",
    "fouls_received": "Fouls drawn.",
    "offensive_rebounds": "Offensive rebounds.",
    "defensive_rebounds": "Defensive rebounds.",
    "total_rebounds": "Total rebounds.",
    "plus_minus": "Plus/minus.",
    "start_five": "Whether the player started.",
    "coach_code": "Head coach code (Utf8 join key).",
    "coach_name": "Head coach name.",
}

# Container prefixes ``json_normalize`` bolts onto the nested EuroLeague objects,
# longest first; the label qualifies the leaf prose. Resolved recursively, so
# ``local_total_points`` -> "Home side: totals: points."
_PREFIXES = [
    ("birth_country_", "Birth country: "),
    ("phase_type_", "Phase type: "),
    ("referee1_", "First referee: "),
    ("referee2_", "Second referee: "),
    ("referee3_", "Third referee: "),
    ("referee4_", "Fourth referee: "),
    ("country_", "Country: "),
    ("partials_", "Quarter scores: "),
    ("winner_", "Winning club: "),
    ("person_", "Person: "),
    ("season_", "Season: "),
    ("venue_", "Venue: "),
    ("local_", "Home side: "),
    ("group_", "Group: "),
    ("total_", "Totals: "),
    ("team_", "Team-only (not attributed to a player): "),
    ("road_", "Away side: "),
    ("club_", "Club: "),
]


class _Prefixed(dict):
    """Description map that strips known container prefixes recursively on a miss."""

    def get(self, name: str, default: Any = None) -> Any:
        try:
            return self[name]
        except KeyError:
            return default

    def __getitem__(self, name: str) -> str:
        if dict.__contains__(self, name):
            return dict.__getitem__(self, name)
        for prefix, label in _PREFIXES:
            if name.startswith(prefix) and len(name) > len(prefix):
                inner = self.get(name[len(prefix) :])
                if inner:
                    # keep an acronym's capital (``ISO``), lower an ordinary first word
                    head = inner if inner[:2].isupper() else inner[:1].lower() + inner[1:]
                    return label + head
        raise KeyError(name)


def _descriptions() -> _Prefixed:
    return _Prefixed({**_LEAF, **_STAT})


def _short(op: dict) -> str:
    """``competitions_competitionCode_seasons_seasonCode_games_gameCode_stats`` -> ``game_stats``."""
    short = op["operationId"]
    for prefix in _OP_PREFIXES:
        if short.startswith(prefix) and len(short) > len(prefix):
            short = short[len(prefix) :]
            break
    return short.replace("games_gameCode_", "game_")


def _snake_path(path: str) -> str:
    return _TOKEN.sub(lambda m: "{" + underscore(m.group(1)) + "}", path)


def _capture_for(refs: Path, path: str) -> Path:
    """The committed capture: ``capture.py``'s slug rule over the pinned season/game."""
    route = _TOKEN.sub(lambda m: _CAPTURE_VALUES[m.group(1)], path)
    slug = route.strip("/").replace("/", "__") or "root"
    return refs / "captures" / SEASON / f"{slug}.json"


def _path_params(op: dict) -> List[Dict[str, Any]]:
    return [
        {
            "name": underscore(prm["name"]),
            "type": "str",
            "required": True,
            "description": str(prm.get("description") or ""),
        }
        for prm in op.get("parameters", [])
        if prm.get("in") == "path"
    ]


def _query_params(op: dict) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for prm in op.get("parameters", []):
        if prm.get("in") != "query":
            continue
        wire = prm["name"]
        desc = str(prm.get("description") or _QUERY_FALLBACK.get(wire, "")).strip()
        if prm.get("required"):
            desc = f"Required. {desc}".strip()
        out.append({"name": underscore(wire), "query_key": wire, "type": "str", "description": desc})
    return out


def _endpoint_entry(path: str, op: dict) -> Dict[str, Any]:
    short = _short(op)
    pps = _path_params(op)
    names = {p["name"] for p in pps}
    assert not {"league", "sport"} & names, path
    assert {underscore(t) for t in _TOKEN.findall(path)} == names, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (op.get("summary") or f"Fetch {path}").rstrip(".") + ".",
        "path": _snake_path(path),
        "parser": "parse_euroleague",
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": {k: v for k, v in _EXAMPLE.items() if k in names},
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
    ops = sorted(get_ops(spec), key=lambda pv: _short(pv[1]))
    shorts = [_short(op) for _, op in ops]
    dupes = sorted({s for s in shorts if shorts.count(s) > 1})
    if dupes:
        raise SystemExit(f"duplicate {STEM} shorts: {dupes}")

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "euroleague_{short}",
        "module": STEM,
        "parser_module": "euroleague.euroleague_parsers",
        "getter_module": "sportsdataverse.euroleague.euroleague_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: the EuroLeague API returned 404 "
                "(unknown competition, season or game code).",
            ],
            "see_also": [
                {
                    "name": "EuroLeague Basketball",
                    "url": "https://www.euroleaguebasketball.net/",
                    "note": "the site the Competition Engine API serves (competitions, seasons, clubs, games)",
                },
                {
                    "name": "euroleague-api",
                    "url": "https://github.com/giasemidis/euroleague_api",
                    "note": "community Python client over the same v2 API",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    descriptions = [spec_descriptions(spec), markdown_descriptions(refs / f"{STEM}-returns.md"), _descriptions()]
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    for path, op in ops:
        short = _short(op)
        capture = _capture_for(refs, path)
        schema: Dict[str, Any] = {"schema": short, "kind": "dataframe"}
        if capture.exists():
            schema["columns"] = columns_from_frame(parse_capture(capture, parse_euroleague), descriptions)
        else:
            schema["columns"] = []
            schema["unverified"] = (
                f"no committed capture in sdv-internal-refs/{STEM}/captures/{SEASON} ({capture.name})"
            )
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints")


if __name__ == "__main__":
    main()
