"""Generate the ``fifa`` flat-API endpoint YAML + returns-schemas from the FIFA public
API v3 OpenAPI spec (``api.fifa.com/api/v3``, keyless).

Idempotent: same spec + same captures -> byte-identical output. Modeled on
``gen_asa.py``, with the shared plumbing in ``gen_soccer_common.py``.

The generator reads the frozen spec from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/fifa.yaml`` -- one endpoint per GET route (9), host
  ``https://api.fifa.com/api/v3``.
* ``tools/codegen/schemas/native/fifa/<short>.yaml`` -- returns-schema per
  endpoint, columns taken from running :func:`parse_fifa` over the committed
  capture; a route without a capture is marked ``unverified`` instead.

The spec's camelCase parameters (``idCompetition``, ``idSeason``, ``language``,
``count``, ``name``) become snake_case Python arguments carrying the wire key as
``query_key``; a path token is renamed the same way (the renderer substitutes a
token by the argument of the same name). No spec path uses a ``{league}`` /
``{sport}`` token (asserted -- the renderer substitutes those as ESPN slugs). The
spec's ``x-pagination`` (paging request parameter unverified) is surfaced in the
family ``raw_doc``.

Column prose: the capture-built spec and ``fifa-returns.md`` carry no property
docs, so the FIFA vocabulary is curated here (:data:`_LEAF`, resolved through the
nested ``home_`` / ``away_team_`` / ``stadium_`` ... prefixes) and every column
must resolve from it -- the shared soccer fallback is MLS-tuned and would mislabel
FIFA columns.

Run: ``python tools/codegen/gen_fifa.py``
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

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
from sportsdataverse.soccer.fifa_parsers import parse_fifa

HOST = "https://api.fifa.com/api/v3"
SPEC = "fifa.openapi.yaml"
STEM = "fifa"

# operationId -> short, where the spec's ``<collection>_<idParam>`` form reads badly.
_SHORT_OVERRIDES = {"competitions_idCompetition": "competition", "teams_idTeam": "team"}

# The spec's ``summary`` is the route path; the reference index and IDE hover show this line.
_SUMMARIES = {
    "calendar_matches": "Match calendar of a competition season (first page; results and fixtures).",
    "competitions": "All FIFA competitions (first page).",
    "competition": "One competition by id.",
    "seasons": "Seasons of a competition (first page).",
    "live_football": "Matches live right now across FIFA competitions.",
    "players_search": "Players matching a name (first page).",
    "stadiums": "Stadiums (first page).",
    "teams_search": "Teams matching a name (first page).",
    "team": "One team by id.",
}

# Path-id examples the captures were taken with (``tools/capture.py`` ROUTES):
# 17 = FIFA World Cup, 43922 = Argentina. The spec carries no ``example`` on path params.
_PATH_EXAMPLES = {"idCompetition": "17", "idTeam": "43922"}

_TOKEN = re.compile(r"\{([^}]+)\}")

_LOC = "JSON list of `{Locale, Description}` objects"

# Whole column names whose meaning differs from the same leaf under a prefix.
_EXACT: Dict[str, str] = {
    "goals": "Goals credited to the player.",
    "weather_type": "Weather: condition code (null in the captures).",
    "weather_type_localized": f"Weather: localised condition, {_LOC}.",
}

# FIFA vocabulary keyed by the UNPREFIXED leaf, so one entry covers ``score`` /
# ``home_score`` / ``away_team_score`` alike.
_LEAF: Dict[str, str] = {
    # --- ids (Utf8 join keys) ---
    "id_competition": "FIFA competition id (17 = FIFA World Cup; Utf8 join key).",
    "id_season": "FIFA season id (285023 = FIFA World Cup 2026; Utf8 join key).",
    "id_stage": "Stage id within the season (Utf8 join key).",
    "id_group": "Group id within the stage (Utf8 join key).",
    "id_match": "FIFA match id (Utf8 join key).",
    "id_team": "FIFA team id (Utf8 join key).",
    "id_player": "FIFA player id (Utf8 join key).",
    "id_stadium": "FIFA stadium id (Utf8 join key).",
    "id_city": "City id (Utf8 join key).",
    "id_country": "Three-letter FIFA country code, e.g. ARG.",
    "id_association": "Three-letter code of the member association, e.g. ARG.",
    "id_confederation": "Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons.",
    "id_member_association": "Member-association codes (e.g. ENG), JSON list.",
    "id_owner": "Code of the body that owns the competition (FIFA or a confederation).",
    "id_ifes": "IFES cross-reference id (Utf8).",
    "id_stats_perform": "Stats Perform (Opta) id of the record (Utf8 cross-reference).",
    "stats_perform_ifes_id": "IFES id as carried by the Stats Perform feed (Utf8).",
    "providers": "Data-provider tag of the record.",
    # --- localised names ---
    "name": f"Localised name, {_LOC}.",
    "short_name": f"Localised short name, {_LOC} (often empty).",
    "display_name": f"Localised display name (e.g. ARG), {_LOC}.",
    "alias": f"Localised alias, {_LOC}.",
    "team_name": f"Localised team name, {_LOC}.",
    "competition_name": f"Localised competition name, {_LOC}.",
    "season_name": f"Localised season name, {_LOC}.",
    "season_short_name": f"Localised short season name, {_LOC} (often empty).",
    "stage_name": f"Localised stage name (e.g. First Stage), {_LOC}.",
    "group_name": f"Localised group name (e.g. Group A), {_LOC}.",
    "city_name": f"Localised city name, {_LOC}.",
    "abbreviation": "Abbreviation (e.g. ARG, FWC 2026).",
    "short_club_name": "Short English team name.",
    # --- classification enums ---
    "gender": "Gender code (FIFA integer enum: 1 = men, 2 = women).",
    "football_type": "Football discipline code (FIFA integer enum; 0 = eleven-a-side football).",
    "team_type": "Team-type code (FIFA integer enum: 0 = club, 1 = national team).",
    "type": "Team-type code (FIFA integer enum: 0 = club, 1 = national team).",
    "competition_type": "Competition-type code (FIFA integer enum).",
    "age_type": "Age-category code (FIFA integer enum; 7 = senior).",
    "sport_type": "Sport code (FIFA integer enum; 0 = football).",
    "active_status": "Activity-status code (FIFA integer enum).",
    "display_order": "Sort order for display.",
    "is_updateable": "IsUpdateable flag from the API (null in the captures).",
    # --- seasons / media ---
    "start_date": "Start date (ISO 8601).",
    "end_date": "End date (ISO 8601).",
    "picture_url": "Image URL template with `{format}` and `{size}` placeholders.",
    "mascot_picture_url": "Mascot image URL template with `{format}` and `{size}` placeholders.",
    "match_ball_picture_url": "Match-ball image URL template with `{format}` and `{size}` placeholders.",
    "thumbnail_url": "Thumbnail image URL (null in the captures).",
    "host_teams": "Host teams, JSON list of `{IdTeam}` objects.",
    "content": "Editorial content entries, JSON list (empty in the captures).",
    "media_content": "Media content entries, JSON list (empty in the captures).",
    # --- matches ---
    "date": "Kick-off in UTC (ISO 8601).",
    "local_date": "Kick-off in venue local time (ISO 8601; the trailing Z is not a UTC marker).",
    "attendance": "Attendance.",
    "match_day": "Match day (round) within the stage.",
    "match_number": "Match number within the season.",
    "match_status": "Match status code (FIFA integer enum: 0 = played, 1 = to be played).",
    "match_time": "Match clock as displayed, e.g. 98'.",
    "period": "Match period code (FIFA integer enum).",
    "result_type": "Result-type code (FIFA integer enum).",
    "officiality_status": "Officiality code of the result (FIFA integer enum).",
    "time_defined": "Whether the kick-off time is confirmed.",
    "coverage_level": "Data-coverage level of the match (integer).",
    "score": "Goals scored.",
    "penalty_score": "Penalty shoot-out score (null when there was no shoot-out).",
    "aggregate_home_team_score": "Home team aggregate score over two legs (null for a single-leg tie).",
    "aggregate_away_team_score": "Away team aggregate score over two legs (null for a single-leg tie).",
    "winner": "Team id of the winner (Utf8; null for a draw or an unplayed match).",
    "leg": "Leg number of a two-legged tie (null for a single-leg match).",
    "match_leg_info": "Two-legged tie details (null in the captures).",
    "is_home_match": "IsHomeMatch flag from the API (null in the captures).",
    "is_ticket_sales_allowed": "IsTicketSalesAllowed flag from the API (null in the captures).",
    "last_period_update": "Time of the last period change (null in the captures).",
    "first_half_time": "First-half timing field (null in the captures).",
    "second_half_time": "Second-half timing field (null in the captures).",
    "first_half_extra_time": "Stoppage time added to the first half, in minutes.",
    "second_half_extra_time": "Stoppage time added to the second half, in minutes.",
    "match_report_url": "URL of the match report (null in the captures).",
    "place_holder_a": "Draw placeholder for the home slot, e.g. A1 (group A, position 1).",
    "place_holder_b": "Draw placeholder for the away slot, e.g. A2 (group A, position 2).",
    "officials": "Match officials, JSON list (OfficialId, OfficialType, IdCountry, localised Name / NameShort).",
    "ball_possession": "Ball-possession block (null when not tracked).",
    "intervals": "possession per interval, JSON list.",
    "last_x": "possession over the most recent stretch, JSON list.",
    "overall_home": "home team share of possession, percent.",
    "overall_away": "away team share of possession, percent.",
    "territorial_possesion": "Territorial possession block (sic: the API's spelling; null in the captures).",
    "territorial_third_possesion": "Territorial possession by third (sic: the API's spelling; null in the captures).",
    "weather": "Weather block (null when not reported).",
    "humidity": "Humidity (null in the captures).",
    "temperature": "Temperature (null in the captures).",
    "wind_speed": "Wind speed (null in the captures).",
    # --- match sides ---
    "side": "Side marker (null in the captures).",
    "tactics": "Formation, e.g. 4-2-3-1.",
    "players": "Line-up, JSON list (IdPlayer, ShirtNumber, Status, Captain, Position, localised PlayerName, ...).",
    "coaches": "Coaches, JSON list (IdCoach, IdCountry, Role, localised Name / Alias, ...).",
    "staffs": "Team staff, JSON list (empty in the captures).",
    "bookings": "Cards, JSON list (Card, Period, Minute, IdPlayer, IdTeam, ...).",
    "goals": "Goals, JSON list (Type, Period, Minute, IdPlayer, IdAssistPlayer, IdTeam, ...).",
    "substitutions": "Substitutions, JSON list (Period, Minute, IdPlayerOff, IdPlayerOn, Reason, ...).",
    # --- teams ---
    "city": "City of the team's headquarters.",
    "street": "Street address.",
    "postal_code": "Postal code.",
    "region_name": "Region name (null in the captures).",
    "headquarters": "Headquarters (null in the captures).",
    "training_centre": "Training centre (null in the captures).",
    "official_site": "Official website (null in the captures).",
    "foundation_year": "Year the team or association was founded.",
    "stadium": "Home stadium block (null in the captures).",
    # --- players ---
    "birth_date": "Date of birth (ISO 8601).",
    "birth_place": "Place of birth (a city name or a country code).",
    "weight": "Weight in kilograms.",
    "height": "Height in centimetres.",
    "preferred_foot": "Preferred-foot code (FIFA integer enum; 9999 when unset).",
    "international_caps": "International appearances (caps).",
    "international_debut": "Date of the international debut (ISO 8601).",
    "top_competition_debut": "Debut date in a top competition (null in the captures).",
    "twitter_account": "Twitter / X handle (null in the captures).",
    "localized_twitter_accounts": "Localised Twitter / X handles (null in the captures).",
    "player_picture": "Player picture block (null in the captures).",
    # --- stadiums ---
    "capacity": "Seating capacity.",
    "built": "Year built, as an ISO 8601 date.",
    "roof": "Whether the stadium is roofed.",
    "turf": "Pitch surface (null in the captures).",
    "web_address": "Website URL.",
    "email": "Contact e-mail.",
    "fax": "Contact fax number.",
    "phone": "Contact phone number.",
    "affiliation_country": "Affiliation country (null in the captures).",
    "affiliation_region": "Affiliation region (null in the captures).",
    "latitude": "Latitude in decimal degrees.",
    "longitude": "Longitude in decimal degrees.",
    "length": "Pitch length (null in the captures).",
    "width": "Pitch width (null in the captures).",
}

# Container prefixes ``json_normalize`` bolts onto the nested FIFA objects, longest
# first. ``home_`` / ``away_`` are the calendar's ``Home`` / ``Away``;
# ``home_team_`` / ``away_team_`` the live feed's ``HomeTeam`` / ``AwayTeam``.
_PREFIXES = [
    ("ball_possession_", "Ball possession: "),
    ("properties_", ""),
    ("home_team_", "Home team: "),
    ("away_team_", "Away team: "),
    ("stadium_", "Stadium: "),
    ("weather_", "Weather: "),
    ("home_", "Home team: "),
    ("away_", "Away team: "),
]


def _leaf(name: str) -> str:
    """Curated prose for ``name``, stripping known container prefixes recursively."""
    if name in _LEAF:
        return _LEAF[name]
    for prefix, label in _PREFIXES:
        if name.startswith(prefix) and len(name) > len(prefix):
            inner = _leaf(name[len(prefix) :])
            if inner:
                # keep an acronym's capital (``IFES``), lower an ordinary first word
                return label + (inner if inner[:2].isupper() or not label else inner[:1].lower() + inner[1:])
    return ""


def _curated(columns: Iterable[str]) -> Dict[str, str]:
    """``{column: prose}`` for every column the curated vocabulary resolves."""
    out = {c: _EXACT.get(c) or _leaf(c) for c in columns}
    return {c: d for c, d in out.items() if d}


def _short(op: dict) -> str:
    return _SHORT_OVERRIDES.get(op["operationId"], op["operationId"])


def _capture_for(refs: Path, path: str) -> Path | None:
    """``/competitions/{idCompetition}`` -> ``captures/competitions__17.json`` (capture.py slug)."""
    filled = _TOKEN.sub(lambda m: _PATH_EXAMPLES[m.group(1)], path)
    candidate = refs / "captures" / (filled.strip("/").replace("/", "__") + ".json")
    return candidate if candidate.exists() else None


def _endpoint_entry(path: str, op: dict) -> Dict[str, Any]:
    short = _short(op)
    path_params: List[Dict[str, Any]] = []
    extra_params: List[Dict[str, Any]] = []
    example_args: Dict[str, Any] = {}
    for prm in op.get("parameters", []):
        wire = prm["name"]
        name = underscore(wire)
        desc = str(prm.get("description") or "").strip()
        if prm.get("in") == "path":
            path_params.append({"name": name, "type": "str", "required": True, "description": desc})
            example_args[name] = _PATH_EXAMPLES[wire]
        elif prm.get("in") == "query":
            if prm.get("required"):
                desc = f"Required. {desc}".strip()
            extra_params.append({"name": name, "query_key": wire, "type": "str", "description": desc})
            if prm.get("example") is not None:
                example_args[name] = str(prm["example"])
    names = {p["name"] for p in path_params}
    assert not {"league", "sport"} & names, path
    assert {underscore(t) for t in _TOKEN.findall(path)} == names, path
    entry: Dict[str, Any] = {
        "short": short,
        "summary": _SUMMARIES[short],
        "path": _TOKEN.sub(lambda m: "{" + underscore(m.group(1)) + "}", path),
        "parser": "parse_fifa",
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": example_args,
    }
    if path_params:
        entry["path_params"] = path_params
    if extra_params:
        entry["extra_params"] = extra_params
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
        "name_pattern": f"{STEM}_{{short}}",
        "module": STEM,
        "parser_module": f"soccer.{STEM}_parsers",
        "qualifier": "",
        "passthrough_query": False,
        "docstring": {
            "example_import": True,
            "raw_doc": (
                "the raw JSON ``Dict`` (paginated routes return the first page; the API's paging "
                "request parameter is undocumented, see the spec's x-pagination)"
            ),
            "raises": [
                "sportsdataverse.errors.NoDataError: the FIFA API returned 404 (unknown competition or team id).",
            ],
            "see_also": [
                {
                    "name": "FIFA",
                    "url": "https://www.fifa.com/",
                    "note": "data origin (keyless front-end API behind fifa.com; not official or supported)",
                },
                {
                    "name": "fifa-public-api-mcp",
                    "url": "https://github.com/chrispickford/fifa-public-api-mcp",
                    "note": "community MCP server over the same API",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint_entry(p, op) for p, op in ops],
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    sources = [spec_descriptions(spec), markdown_descriptions(refs / f"{STEM}-returns.md")]
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    for path, op in ops:
        short = _short(op)
        capture = _capture_for(refs, path)
        schema: Dict[str, Any] = {"schema": short, "kind": "dataframe", "columns": []}
        if capture is None:
            schema["unverified"] = f"no committed capture in sdv-internal-refs/{STEM}/captures (see ENDPOINTS.md)"
        else:
            frame = parse_capture(capture, parse_fifa)
            descriptions = [*sources, _curated(frame.columns)]
            missing = [c for c in frame.columns if not any(s.get(c) for s in descriptions)]
            if missing:
                raise SystemExit(f"{STEM}/{short}: no curated description for {missing} -- add them to _LEAF")
            schema["columns"] = columns_from_frame(frame, descriptions)
        write_yaml(schema_dir / f"{short}.yaml", schema)
    print(f"{STEM}: {len(ops)} endpoints")


if __name__ == "__main__":
    main()
