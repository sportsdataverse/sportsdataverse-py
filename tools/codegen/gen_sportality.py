"""Generate the ``sportality`` flat-API endpoint YAML + returns-schemas from the Basketligaen site API spec
(``www.basketligaen.dk/api``, keyless GET JSON; Sportality platform, stats by Genius Sports LiveStats).

Idempotent: same spec + same captures -> byte-identical output. Modeled on ``gen_shl.py`` (the same
vendor and route families), with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen spec and the committed captures from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/sportality.yaml`` -- one endpoint per route (24: the spec's 18 stats-info
  module paths are one ``stats_info`` endpoint with a ``module`` path parameter).
* ``tools/codegen/schemas/native/sportality/<short>.yaml`` -- returns-schema per endpoint: the columns
  of the endpoint's parser run over its capture, typed from the FULL bodies in
  ``tests/fixtures/sportality/full/`` where there is one (the 3-row captures mistype rates).
  ``stats_info`` documents the union of the 18 modules' columns. Descriptions are written empty here (a
  re-capture would clobber them).
* ``tools/codegen/notes/dbl_descriptions.yaml`` -- the column descriptions, keyed by schema key
  (``sportality_<short>``) then column, from the refs ``sportality-returns.md`` route tables, the spec's
  component schemas and the curated text below; Integration merges it into
  ``manual_column_descriptions.yaml``.
* ``sportsdataverse/basketball/dbl/sportality_dtypes.py`` -- every endpoint's column -> polars dtype, which
  the parsers use to type a zero-row frame and to backfill missing columns.
* ``tests/fixtures/sportality/spec_paths.json`` -- the spec's route list, so the tests need no refs checkout.

Season-keyed routes take ``season`` (END year) + ``game_type`` instead of the provider's ssgt / season
uuids: ``sportality_runtime._get`` maps them through layout-info.

Run: ``python tools/codegen/gen_sportality.py``
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import polars as pl
from gen_soccer_common import (
    ROOT,
    columns_from_frame,
    get_ops,
    load_spec,
    refs_dir,
    rewrite_schema_dir,
    spec_descriptions,
    write_yaml,
)

from sportsdataverse.basketball.dbl import sportality_parsers
from sportsdataverse.basketball.dbl.sportality_parsers import _column
from sportsdataverse.basketball.dbl.sportality_runtime import GAME_TYPES

HOST = "https://www.basketligaen.dk/api"
SPEC = "sportality.openapi.yaml"
STEM = "sportality"
FIXTURES = ROOT / "tests" / "fixtures" / "sportality"
FULL = FIXTURES / "full"
STATS_PREFIX = "/statistics-v2/stats-info/"
STATS_ROUTE = STATS_PREFIX + "{module}"

# spec path -> (short, parser). Every spec path must be listed (asserted below); the 18 stats-info module
# paths fold into STATS_ROUTE.
_ROUTES: Dict[str, Tuple[str, str]] = {
    "/sports-v2/season-series-game-types-filter": ("season_filter", "parse_sportality_filter"),
    "/statistics-v2/layout-info": ("layout_info", "parse_sportality_layout"),
    "/sports-v2/game-schedule": ("schedule", "parse_sportality_schedule"),
    "/sports-v2/game-info/{game_uuid}": ("game_header", "parse_sportality_page"),
    "/sports-v2/teams/{team_uuid}": ("team", "parse_sportality_page"),
    "/sports-v2/athletes/by-team-uuid/{team_uuid}": ("roster", "parse_sportality_roster"),
    "/sports-v2/athlete-details/{athlete_uuid}": ("athlete_details", "parse_sportality_athlete"),
    "/sports-v2/all-sites/{ssgt_uuid}": ("sites", "parse_sportality"),
    "/sports-v2/played-games/{team_uuid}": ("team_played_games", "parse_sportality_schedule"),
    "/sports-v2/upcoming-games/{team_uuid}": ("team_upcoming_games", "parse_sportality_schedule"),
    "/sports-v2/last-game-day": ("last_game_day", "parse_sportality_schedule"),
    "/sports-v2/next-game-day": ("next_game_day", "parse_sportality_schedule"),
    "/sports-v2/ssgt/{ssgt_uuid}/provider": ("ssgt_provider", "parse_sportality"),
    "/gameday/play-by-play/{game_uuid}": ("pbp", "parse_sportality_pbp"),
    "/gameday/boxscore/{game_uuid}": ("boxscore", "parse_sportality_player_box"),
    "/gameday/player-stats/{game_uuid}": ("player_box", "parse_sportality_player_box"),
    "/gameday/team-stats/{game_uuid}": ("team_stats", "parse_sportality_team_stats"),
    "/gameday/game-info/{game_uuid}": ("game_summary", "parse_sportality_page"),
    "/gameday/game-overview/{game_uuid}": ("game_overview", "parse_sportality_page"),
    "/gameday/team-standings/{ssgt_uuid}": ("standings", "parse_sportality_standings"),
    "/gameday/post-game-data/team-stats/{game_uuid}": ("post_game_team_stats", "parse_sportality"),
    STATS_ROUTE: ("stats_info", "parse_sportality_stats_info"),
    "/statistics-v2/athlete/profile-page": ("athlete_profile", "parse_sportality_page"),
    "/statistics-v2/athlete/playerProfile_gameLog": ("athlete_career", "parse_sportality_stats_info"),
    "/statistics-v2/player-h2h-v2/{ssgt_uuid}/{home_team_uuid}/{away_team_uuid}": (
        "player_h2h",
        "parse_sportality_h2h",
    ),
}

# Full (untruncated) bodies per short: their dtypes win over the 3-row captures'.
_FULL: Dict[str, List[str]] = {
    "pbp": ["gameday__play-by-play__c4hh5nig6t.json"],
    "boxscore": ["gameday__boxscore__c4hh5nig6t.json"],
    "team_stats": ["gameday__team-stats__c4hh5nig6t.json"],
    "player_box": ["gameday__boxscore__c4hh5nig6t.json"],
    "schedule": ["sports-v2__game-schedule__qzw34da6jg.json"],
    "stats_info": [
        "statistics-v2__stats-info__players_summary__qzw34da6jg.json",
        "statistics-v2__stats-info__teams_summary__qzw34da6jg.json",
        "statistics-v2__stats-info__games_games__qzw34da6jg.json",
    ],
    "layout_info": ["statistics-v2__layout-info__directory.json"],
}

# Columns null in every capture of a route (so typed Utf8) that hold numbers on the sibling routes of the same
# shape: an unplayed game has no score yet.
_SCORES = {"home_team_score": pl.Int64, "away_team_score": pl.Int64}
_NULL_IN_CAPTURE: Dict[str, Dict[str, Any]] = {"next_game_day": _SCORES, "team_upcoming_games": _SCORES}

# Wire query key -> wrapper argument name (an ssgt / athlete uuid is never called a season or player id).
_ARG_NAMES = {
    "playerUuid": "athlete_uuid",
    "seasonUuid": "season_uuid",
    "seriesUuid": "series_uuid",
    "gameTypeUuid": "game_type_uuid",
}

_SEASON_ARG = {
    "name": "season",
    "query_key": "season",
    "type": "int",
    "required": True,
    "description": "Required. Season END year (2026 = 2025/26). Mapped to the provider's ssgt through "
    "/statistics-v2/layout-info (Basketligaen 2002/03 on); a season it does not list raises NoDataError.",
}
_GAME_TYPE_ARG = {
    "name": "game_type",
    "query_key": "gameType",
    "type": "str",
    "default": "regular",
    "choices": list(GAME_TYPES),
    "description": "Game type: regular (the league season) or finals (the playoffs, Slutspil).",
}
# Routes whose provider keys the runtime derives from (season, game_type): the wire params it replaces are
# dropped from the wrapper.
_SEASON_ROUTES: Dict[str, Tuple[set, bool]] = {  # path -> (wire params replaced, season required)
    STATS_ROUTE: ({"ssgtUuid", "count"}, True),
    "/sports-v2/game-schedule": ({"seasonUuid", "seriesUuid", "gameTypeUuid"}, True),
    "/sports-v2/last-game-day": ({"seasonSeriesGameType"}, False),
    "/sports-v2/next-game-day": ({"seasonSeriesGameType"}, False),
}

# Descriptions for columns the dedicated shapers make (no refs table names them).
_EXTRA = {
    "player_id": "Genius person id (Int64). A string key in the players maps, an integer in play-by-play and "
    "box-score rows; cast here so the frames join, and join the genius family.",
    "athlete_uuid": "Platform athlete uuid (rosters and athlete routes key on it); not the Genius person id; "
    "null where the site has none.",
    "game_uuid": "Platform game uuid (c4hh5nig6t); every gameday route takes it.",
    "team_side": "home or away: the side the row belongs to.",
    "row_type": "Kind of row: player, team_coach (the bench row: team rebounds, turnovers and fouls charged to "
    "the team or coach) or team_totals.",
    "team_id": "Genius team id (Int64): a string in gameday bodies, an integer in standings and stats tables.",
    "team_name": "Team name.",
    "team_uuid": "Platform team uuid.",
    "team_code": "Platform team code (HOL, RAN).",
    "season": "Season END year (2026 = 2025/26).",
    "season_code": "Provider season code: the START year as a string (2025 = 2025/26).",
    "kind": "Directory entry kind: series, season or game_type.",
    "portrait_url": "URL of the player's latest rendered portrait.",
    "provider": "Stats provider of the ssgt (genius-basketball, a Genius Sports LiveStats relay).",
    "effective_ssgt_uuid": "The ssgt the head-to-head was computed for: the CURRENT season, whatever the path asked.",
    "team_short_name": "Team short name.",
    "game_id": "Genius match id (Int64), the join to the genius family; a string on play-by-play lines, an "
    "integer elsewhere.",
    "game_source_id": "Genius match id (Int64), the same number as game_id and sports-v2 game_info_ext_id.",
    "event_id": "Genius event number within the game (Int64), increasing through the game.",
    "qualifiers": "Event qualifiers joined by ';' (a list in recent games, a ';'-joined string or null in older ones).",
    "coordinates_x": "Shot or event x coordinate in Genius court units (Float64); origin and scale unverified.",
    "coordinates_y": "Shot or event y coordinate in Genius court units (Float64); origin and orientation unverified.",
    "period": "Period: 0 = the whole game, 1-4 the quarters, 5 and up overtime.",
    "assist_player_id": "Genius person id of the assisting player.",
}

_BOX_PERIOD = {"period": "Period of the line: 0 = the whole game (the only value seen)."}
_STATS_INFO_WTL = {
    "w": "Wins. Team and standings modules: wins of the team in the ssgt.",
    "l": "Losses. Standings module: losses of the team in the ssgt.",
}
_EXTRA_BY_SHORT = {
    "boxscore": _BOX_PERIOD,
    "player_box": _BOX_PERIOD,
    "stats_info": {
        **_STATS_INFO_WTL,
        "home_score": "Points scored by the home team (games_games; Int64, a numeric string on the wire).",
        "away_score": "Points scored by the away team (games_games; Int64, a numeric string on the wire).",
        "decided_in": "How the game was decided: null after regulation, otherwise the extra period (OT = overtime).",
    },
    "standings": {
        "strk": "Current streak as a signed integer: positive = wins in a row, negative = losses in a row.",
        "tp": "League table points (2 per win).",
    },
    "team_stats": {"tp": "Points scored by the team in the period (period 0 = the whole game)."},
    "game_header": {"game_info_ext_id": "Genius match id (Int64; the same number as gameday game_source_id)."},
    "pbp": {
        "home_place": "Side of the home block: always home.",
        "away_place": "Side of the away block: always away.",
        "event_place": "Side of the team the event belongs to: home or away.",
    },
}
# Refs prose too terse for the manual-descriptions rules (>= 15 characters, no restated name): the same fact,
# spelled out. Keyed by the resolved text, since many columns share one.
_EXPAND = {
    "Assists.": "Number of assists credited.",
    "Age in years.": "Age of the player in whole years.",
    "Given name.": "Given (first) name of the player.",
    "Surname.": "Surname (family name) of the player.",
    "Height in cm.": "Height of the player in centimetres.",
    "Weight in kg.": "Weight of the player in kilograms.",
    "Logo URL.": "URL of the team logo image.",
    "home or away.": "Side of the game: home or away.",
    "Game date.": "Date of the game (YYYY-MM-DD).",
    "Games played.": "Number of games played.",
    "Shirt number.": "Jersey (shirt) number of the player.",
    "Jersey number.": "Jersey (shirt) number of the player.",
    "Team name.": "Name of the team.",
    "Team short name.": "Short display name of the team.",
    "Arena.": "Name of the arena the game is played in.",
    "Arena name.": "Name of the arena (venue).",
    "Given name(s).": "Given (first) name(s) of the player.",
    "Player name.": "Full display name of the player.",
    "Full name.": "Full display name of the player.",
    "Family name.": "Family name (surname) of the player.",
    "Gender (male).": "Gender of the person as recorded by the site (male in every row seen).",
    "Height value.": "Height of the player in centimetres.",
    "Weight value.": "Weight of the player in kilograms.",
    "Team logo URL.": "URL of the team logo image.",
    "Blocks.": "Number of blocked shots made.",
    "Points.": "Number of points scored.",
    "Steals.": "Number of steals.",
    "Turnovers.": "Number of turnovers committed.",
    "Full team name.": "Full name of the team.",
    "Long team name.": "Long name of the team.",
    "Short team name.": "Short name of the team.",
    "Away points.": "Points scored by the away team.",
    "Home points.": "Points scored by the home team.",
    "Away team name.": "Name of the away team.",
    "Home team name.": "Name of the home team.",
    "Left value.": "Value of the left-hand statistic of the comparison row.",
    "Right value.": "Value of the right-hand statistic of the comparison row.",
    "Position code.": "Playing position code of the player (G, F, C, ...).",
    "Display name.": "Display name of the club.",
    "Points scored.": "Points scored by the team (points for).",
    "Wins.": "Number of wins.",
    "Losses.": "Number of losses.",
    "Total fouls.": "Total personal fouls committed.",
    "Total points.": "Total points scored.",
    "Venue name.": "Name of the venue the game was played in.",
}
NOTES = ROOT / "tools" / "codegen" / "notes" / "dbl_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
SCHEMA_MODULE = ROOT / "sportsdataverse" / "basketball" / "dbl" / "sportality_dtypes.py"
# polars dtype -> the code sportality_dtypes.py stores (sportality_parsers._SCHEMA_DTYPES reads it back).
_DTYPE_CODES = {"String": "str", "Int64": "int", "Float64": "float", "Boolean": "bool", "List(Int64)": "list[int]"}

# Wire-name parts the shapers rename, applied when matching refs-table names to columns.
_PART_RENAMES = {
    "homeTeamInfo": "homeTeam",
    "awayTeamInfo": "awayTeam",
    "venueInfo": "venue",
    "seriesInfo": "series",
}
_PBP_RENAMES = {"homeTeam": "home", "awayTeam": "away", "eventTeam": "event"}
# | `name` | type | description | -- the description may itself hold pipes (type|source|id).
_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*[^|]*?\s*\|\s*(.+?)\s*\|\s*$")


def _capture(refs: Path, path: str) -> Path:
    """``capture.py``'s slug: path ``/`` -> ``__``, braces dropped."""
    slug = path.strip("/").replace("/", "__").replace("{", "").replace("}", "")
    return refs / "captures" / (slug + ".json")


def _module_captures(refs: Path) -> List[Path]:
    """Every stats-info capture (``statistics-v2__stats-info__module__<module>[@ssgt].json``), sorted."""
    return sorted((refs / "captures").glob("statistics-v2__stats-info__module__*.json"))


def _md_tables(md: Path) -> Dict[str, Dict[str, str]]:
    """``{route path: {column: description}}`` from the refs returns doc.

    A table row's dotted wire name (``[].homeTeam.teamId``) is matched to snake_case columns by every suffix of
    its parts (``home_team_team_id``, ``team_id``), longest first, so the most specific prose wins. The 18
    ``stats-info/<module>`` sections all belong to the one ``stats_info`` route.
    """
    rows: Dict[str, List[Tuple[List[str], str]]] = {}
    route = ""
    for line in md.read_text(encoding="utf-8").splitlines():
        if line.startswith("## `"):
            route = line.split("`")[1].replace(HOST, "")
            if route.startswith(STATS_PREFIX):
                route = STATS_ROUTE
        m = _ROW.match(line)
        if route and m and m.group(1) != "col_name":
            parts = [p for p in m.group(1).split(".") if p != "[]"]
            renames = {**_PART_RENAMES, **(_PBP_RENAMES if "play-by-play" in route else {})}
            rows.setdefault(route, []).append(([renames.get(p, p) for p in parts], m.group(2)))
    out: Dict[str, Dict[str, str]] = {}
    for route, entries in rows.items():
        table: Dict[str, str] = {}
        for drop in range(max((len(p) for p, _ in entries), default=0)):
            for parts, desc in entries:
                if drop < len(parts):
                    table.setdefault(_column("_".join(parts[drop:])), desc)
        out[route] = table
    return out


# Wire-format phrases the parsers have already normalised (a numeric string is a Float64 here, an id an Int64, an
# empty string a null): reworded so a description matches the frame it documents.
_FIXES = [
    (re.compile(r"\(Genius key space; a string\)"), "(Genius key space; Int64)"),
    (re.compile(r",? \(?(as )?an? (numeric )?string( with (one|two) decimals?)?\)?"), ""),
    (
        re.compile(
            r" \((the integer 0|a numeric string), or (the integer 0|a numeric string|a string with two decimals)\)"
        ),
        "",
    ),
    (re.compile(r" \(the integer 0, or a string with two decimals\)"), ""),
    (re.compile(r"'' on (the )?team rows"), "null on the bench and totals rows"),
    (re.compile(r"; an integer for a played game, '' \(string\) before the game"), "; null before the game is played"),
    (re.compile(r" \(a number, or null on the bench and totals rows\)"), ""),
]


def _fix(desc: str) -> str:
    """``desc`` unescaped and reworded for the normalised dtypes (see :data:`_FIXES`)."""
    desc = html.unescape(desc)
    for pattern, repl in _FIXES:
        desc = pattern.sub(repl, desc)
    return desc.strip()


_FILLER = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.I)


def _bad(name: str, desc: str) -> bool:
    """Whether ``desc`` would fail the manual-descriptions filler rules (missing, short, restated)."""
    d = (desc or "").strip()
    return len(d) < 15 or d.lower() == name.lower() or bool(_FILLER.match(d))


def _write_schema_module(dtypes: Dict[str, Dict[str, str]]) -> None:
    lines = [
        '"""Per-endpoint column -> dtype code for the ``sportality`` parsers.',
        "",
        "Generated by tools/codegen/gen_sportality.py from the same frames as the returns-schemas: do not edit.",
        "Codes: str, int, float, bool, list[int] (see ``sportality_parsers._SCHEMA_DTYPES``).",
        '"""',
        "",
        "from typing import Dict",
        "",
        "SCHEMAS: Dict[str, Dict[str, str]] = {",
    ]
    for short in sorted(dtypes):
        lines.append(f"    {json.dumps(short)}: {{")  # double quotes: ruff format leaves the file alone
        lines += [f"        {json.dumps(name)}: {json.dumps(code)}," for name, code in dtypes[short].items()]
        lines.append("    },")
    lines += ["}", ""]
    SCHEMA_MODULE.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def _parse(parser: Callable[..., pl.DataFrame], path: Path) -> pl.DataFrame:
    return parser(json.loads(path.read_text(encoding="utf-8")))


def _widen(a: pl.DataType, b: pl.DataType) -> pl.DataType:
    """The dtype that holds both: a sample whose values happen to be whole numbers must not type a rate as Int64."""
    if a == b:
        return a
    if {a, b} == {pl.Int64, pl.Float64}:
        return pl.Float64()
    return pl.Utf8()


def _frame_for(refs: Path, path: str, short: str, parser: Callable[..., pl.DataFrame]) -> pl.DataFrame:
    """A zero-row frame with the union of the columns, a column that is whole numbers in one body and decimals in another typed Float64."""
    paths = [FULL / f for f in _FULL.get(short, [])]
    paths += _module_captures(refs) if short == "stats_info" else [_capture(refs, path)]
    schema: Dict[str, pl.DataType] = {}
    for f in paths:
        for name, dtype in _parse(parser, f).schema.items():
            schema[name] = _widen(schema[name], dtype) if name in schema else dtype
    schema.update({k: v for k, v in _NULL_IN_CAPTURE.get(short, {}).items() if k in schema})
    return pl.DataFrame(schema=schema)


def _params(path: str, op: dict, modules: List[str]) -> Tuple[List[dict], List[dict], Dict[str, Any]]:
    path_params: List[dict] = []
    extra_params: List[dict] = []
    examples: Dict[str, Any] = {}
    replaced, season_required = _SEASON_ROUTES.get(path, (set(), False))
    for prm in op.get("parameters", []):
        wire, desc = prm["name"], str(prm.get("description") or "").strip()
        if prm.get("in") == "path":
            entry: Dict[str, Any] = {"name": wire, "type": "str", "required": True, "description": desc}
            path_params.append(entry)
            if prm.get("example") not in (None, ""):
                examples[wire] = str(prm["example"])
        elif prm.get("in") == "query" and wire not in replaced:
            name = _ARG_NAMES.get(wire, _column(wire))
            extra: Dict[str, Any] = {"name": name, "query_key": wire, "type": "str", "description": desc}
            if prm.get("required"):
                extra.update(required=True, description=f"Required. {desc}".strip())
            extra_params.append(extra)
            if prm.get("example") not in (None, ""):
                examples[name] = str(prm["example"])
    if path == STATS_ROUTE:
        path_params.insert(
            0,
            {
                "name": "module",
                "type": "str",
                "required": True,
                "choices": modules,
                "description": "Stats table: players_* (summary, fieldGoal, rebound, point, offensive, defensive, foul), "
                "teams_* (summary, attendance, possession, fieldGoal, point, rebound, offensive, defensive, foul), "
                "games_games or standings_standings.",
            },
        )
        examples["module"] = "players_summary"
    if path in _SEASON_ROUTES:
        season = dict(_SEASON_ARG)
        if not season_required:
            season.pop("required")
            season["description"] = (
                "Season END year (2026 = 2025/26), mapped to its ssgt through layout-info. None = the active "
                "ssgt (these match-day views follow the current season)."
            )
        else:
            examples["season"] = 2026
        extra_params = [season, dict(_GAME_TYPE_ARG), *extra_params]
    assert not {"league", "sport"} & {p["name"] for p in path_params}, path  # Review Focus 4
    return path_params, extra_params, examples


def _endpoint(path: str, op: dict, modules: List[str]) -> Dict[str, Any]:
    short, parser = _ROUTES[path]
    path_params, extra_params, examples = _params(path, op, modules)
    summary = op.get("summary") or f"Fetch {path}"
    if path == STATS_ROUTE:
        summary = "Stats table of one module for a season (players, teams, games, standings)"
    entry: Dict[str, Any] = {
        "short": short,
        "summary": summary.rstrip(".") + ".",
        "path": path,
        "parser": parser,
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": examples,
    }
    if path_params:
        entry["path_params"] = path_params
    if extra_params:
        entry["extra_params"] = extra_params
    if path == STATS_ROUTE:
        entry["fixed_params"] = {"count": 1000}  # the runtime enforces it too
    return entry


def main() -> None:
    refs = refs_dir(STEM, SPEC)
    spec = load_spec(refs, SPEC)
    raw_ops = get_ops(spec)
    module_ops = {p[len(STATS_PREFIX) :]: op for p, op in raw_ops if p.startswith(STATS_PREFIX)}
    modules = sorted(module_ops)
    ops = [(p, op) for p, op in raw_ops if not p.startswith(STATS_PREFIX)]
    ops.append((STATS_ROUTE, module_ops["players_summary"]))
    missing = sorted({p for p, _ in ops} ^ set(_ROUTES))
    if missing:
        raise SystemExit(f"sportality: spec paths and _ROUTES differ: {missing}")
    absent = [m for m in modules if not (refs / "captures" / f"statistics-v2__stats-info__module__{m}.json").exists()]
    if absent:
        raise SystemExit(f"sportality: stats-info modules without a capture: {absent}")
    ops.sort(key=lambda pv: _ROUTES[pv[0]][0])
    SPEC_PATHS.write_text(
        json.dumps({"paths": sorted(p for p, _ in raw_ops), "modules": modules}, indent=1) + "\n", encoding="utf-8"
    )

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "sportality_{short}",
        "module": STEM,
        "parser_module": "basketball.dbl.sportality_parsers",
        "getter_module": "sportsdataverse.basketball.dbl.sportality_runtime",
        "qualifier": "",
        "schema_prefix": "sportality_",
        "passthrough_query": False,
        "parser_columns": True,
        "raw_types": ["Dict", "str"],
        "docstring": {
            "example_import": True,
            "notes": [
                "Unofficial: the JSON API behind www.basketligaen.dk, the Danish men's basketball league "
                "(Basketligaen, series code DBL). The site runs on the Sportality platform (the vendor behind the "
                "SHL and HockeyAllsvenskan wrappers). The terms page was not read; payloads are not redistributed "
                "as release assets.",
                "Stats provider: genius-basketball, a relay of Genius Sports LiveStats. The Genius match, team "
                "and person ids in play-by-play, box scores and stats tables (game_id, team_id, player_id, all "
                "Int64) join the genius family; the platform uuids (game, team, athlete, ssgt) are not "
                "Genius ids and exist only on this site. The Genius ORG code of Denmark is not exposed.",
                "Season = the END year (2026 = 2025/26) everywhere; the runtime maps it, with game_type "
                "('regular' or 'finals', the playoffs), to the site's ssgt (season + series + game type) through "
                "layout-info. Schedules, standings and season tables start in 2002/03; game-level data "
                "(play-by-play with shot x/y, box scores, team stats) starts in 2023/24.",
                "Source defects the runtime refuses (AssetFetchError): an unfinished game or a truncated "
                "play-by-play feed, a box score or feed that names another game, a cut stats table. The 2002/03 "
                "regular-season schedule lists 109 finished games where its own standings count 110: a source gap.",
                "Personal data is removed from every body: birth dates, e-mail addresses, phone numbers, "
                "addresses and contact blocks.",
                "No women's league: Dameligaen has its own domain (dameligaen.dk), which answers 401 to "
                "anonymous requests.",
            ],
            "raises": [
                "sportsdataverse.errors.NoDataError: the site answered 404, an empty body (gameday routes before "
                "2023/24, or an unknown game), a table with no rows, or the season and game type are not in "
                "layout-info.",
                "sportsdataverse.errors.AssetFetchError: an error envelope or non-JSON 200, another game / team / "
                "athlete / ssgt than the one asked for, an unfinished game or truncated play-by-play, a cut stats "
                "table, or any other failed fetch (a 500 is the site's answer for an unknown uuid and is not "
                "retried).",
            ],
            "see_also": [
                {
                    "name": "Basketligaen",
                    "url": "https://www.basketligaen.dk/",
                    "note": "the Danish men's basketball league site whose API this is",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "the SportsDataverse men's basketball package for R",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint(p, op, modules) for p, op in ops],
    }
    write_yaml(ROOT / f"tools/codegen/endpoints/{STEM}.yaml", doc)

    md = _md_tables(refs / f"{STEM}-returns.md")
    flat_md: Dict[str, str] = {}
    for table in md.values():
        for k, v in table.items():
            flat_md.setdefault(k, v)
    spec_desc = spec_descriptions(spec)
    schema_dir = ROOT / f"tools/codegen/schemas/native/{STEM}"
    rewrite_schema_dir(schema_dir)
    notes: Dict[str, Dict[str, str]] = {}
    dtypes: Dict[str, Dict[str, str]] = {}
    for path, _op in ops:
        short, parser_name = _ROUTES[path]
        frame = _frame_for(refs, path, short, getattr(sportality_parsers, parser_name))
        sources = [_EXTRA_BY_SHORT.get(short, {}), _EXTRA, md.get(path, {}), flat_md, spec_desc]
        cols = columns_from_frame(frame, sources, leaf_fallback=False)
        texts = notes.setdefault(f"{STEM}_{short}", {})
        for col in cols:
            desc = _fix(col["description"])
            texts[col["name"]] = _EXPAND.get(desc, desc)
            col["description"] = ""
        dtypes[short] = {
            name: _DTYPE_CODES[str(dtype).replace("Utf8", "String")] for name, dtype in frame.schema.items()
        }
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"{STEM}_{short}", "kind": "dataframe", "columns": cols})
    bad = sorted(f"{k}.{c}: {d!r}" for k, cs in notes.items() for c, d in cs.items() if _bad(c, d))
    if bad:
        raise SystemExit("sportality: missing or filler descriptions (add them to _EXTRA):\n" + "\n".join(bad))
    write_yaml(NOTES, notes)
    by_columns: Dict[Tuple[str, ...], Tuple[str, Dict[str, str]]] = {}
    for short, types in dtypes.items():  # the parsers find an endpoint's dtypes by its column list
        other, other_types = by_columns.setdefault(tuple(types), (short, types))
        if other_types != types:
            raise SystemExit(f"sportality: {other} and {short} share a column list but not its dtypes")
    _write_schema_module(dtypes)
    n = sum(map(len, notes.values()))
    print(f"{STEM}: {len(ops)} endpoints, stats_info over {len(modules)} modules, {n} descriptions")


if __name__ == "__main__":
    main()
