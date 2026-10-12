"""Generate the ``shl`` flat-API endpoint YAML + returns-schemas from the SHL site API spec
(``www.shl.se/api``, keyless GET JSON; Sportality platform, stats by StatNet).

Idempotent: same spec + same captures -> byte-identical output. Modeled on ``gen_asa.py``,
with the shared plumbing in ``gen_soccer_common.py``.

Reads the frozen spec and the committed captures from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/shl.yaml`` -- one endpoint per GET route (27).
* ``tools/codegen/schemas/native/shl/<short>.yaml`` -- returns-schema per endpoint: the
  columns of the endpoint's parser run over its capture, typed from the FULL bodies in
  ``tests/fixtures/shl/full/`` where there is one (the 3-row captures mistype rates).
  ``stats_info`` documents the union of the 34 modules' columns. Descriptions are written
  empty here (a re-capture would clobber them).
* ``tools/codegen/shl_column_descriptions.snippet.yaml`` -- the column descriptions, keyed by
  schema key (``shl_<short>``) then column, from the refs ``shl-returns.md`` route tables and
  the curated text below; merged into ``manual_column_descriptions.yaml``.
* ``sportsdataverse/hockey/shl/shl_schemas.py`` -- every endpoint's column -> polars dtype,
  which the parsers use to type a zero-row frame and to backfill missing columns.

Season-keyed routes take ``season`` (END year) + ``game_type`` instead of the provider's
ssgt / season uuids: ``shl_runtime._get`` maps them through layout-info.

Run: ``python tools/codegen/gen_shl.py``
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

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

from sportsdataverse.hockey.shl import shl_parsers
from sportsdataverse.hockey.shl.shl_parsers import _column
from sportsdataverse.hockey.shl.shl_runtime import GAME_TYPES

HOST = "https://www.shl.se/api"
SPEC = "shl.openapi.yaml"
STEM = "shl"
FULL = ROOT / "tests" / "fixtures" / "shl" / "full"

# spec path -> (short, parser). Every spec path must be listed (asserted below).
_ROUTES: Dict[str, Tuple[str, str]] = {
    "/sports-v2/season-series-game-types-filter": ("season_filter", "parse_shl_filter"),
    "/statistics-v2/layout-info": ("layout_info", "parse_shl_layout"),
    "/sports-v2/game-schedule": ("schedule", "parse_shl_schedule"),
    "/sports-v2/game-info/{game_uuid}": ("game_header", "parse_shl_page"),
    "/sports-v2/teams/{team_uuid}": ("team", "parse_shl_page"),
    "/sports-v2/athletes/by-team-uuid/{team_uuid}": ("roster", "parse_shl_roster"),
    "/sports-v2/athlete-details/{athlete_uuid}": ("athlete_details", "parse_shl_page"),
    "/sports-v2/all-sites/{ssgt_uuid}": ("sites", "parse_shl"),
    "/sports-v2/played-games/{team_uuid}": ("team_played_games", "parse_shl_schedule"),
    "/sports-v2/upcoming-games/{team_uuid}": ("team_upcoming_games", "parse_shl_schedule"),
    "/sports-v2/last-game-day": ("last_game_day", "parse_shl_schedule"),
    "/sports-v2/next-game-day": ("next_game_day", "parse_shl_schedule"),
    "/sports-v2/ssgt/{ssgt_uuid}/provider": ("ssgt_provider", "parse_shl"),
    "/gameday/play-by-play/{game_uuid}": ("pbp", "parse_shl_pbp"),
    "/gameday/boxscore/{game_uuid}": ("boxscore", "parse_shl_player_box"),
    "/gameday/player-stats/{game_uuid}": ("player_box", "parse_shl_player_box"),
    "/gameday/team-stats/{game_uuid}": ("team_stats", "parse_shl_team_stats"),
    "/gameday/game-info/{game_uuid}": ("game_summary", "parse_shl_page"),
    "/gameday/game-overview/{game_uuid}": ("game_overview", "parse_shl_page"),
    "/gameday/team-standings/{ssgt_uuid}": ("standings", "parse_shl_standings"),
    "/gameday/post-game-data/team-stats/{game_uuid}": ("post_game_team_stats", "parse_shl"),
    "/statistics-v2/stats-info/{module}": ("stats_info", "parse_shl_stats_info"),
    "/statistics-v2/athlete/profile-page": ("athlete_profile", "parse_shl_page"),
    "/statistics-v2/athlete/seasonList": ("athlete_seasons", "parse_shl"),
    "/statistics-v2/athlete/playerProfile_gameLog": ("athlete_career", "parse_shl_stats_info"),
    "/statistics-v2/athlete/playerProfile_seasonGameLog": ("athlete_game_log", "parse_shl_stats_info"),
    "/statistics-v2/player-h2h-v2/{ssgt_uuid}/{home_team_uuid}/{away_team_uuid}": ("player_h2h", "parse_shl_h2h"),
}

# Full (untruncated) bodies per short: their dtypes win over the 3-row captures'.
_FULL: Dict[str, List[str]] = {
    "pbp": ["gameday__play-by-play__bdhvuc5tex.json"],
    "boxscore": ["gameday__boxscore__bdhvuc5tex.json"],
    "player_box": ["gameday__boxscore__bdhvuc5tex.json"],
    "stats_info": [
        "statistics-v2__stats-info__teams_result__iuzqg7dqk9.json",
        "statistics-v2__stats-info__goalkeepers_summary__iuzqg7dqk9.json",
    ],
    "layout_info": ["statistics-v2__layout-info__directory.json"],
}

# Columns null in every capture of a route (so typed Utf8) that hold numbers on the sibling
# routes of the same shape: an unplayed game has no score yet.
_SCORES = {"home_team_score": pl.Int64, "away_team_score": pl.Int64}
_NULL_IN_CAPTURE: Dict[str, Dict[str, Any]] = {"next_game_day": _SCORES, "team_upcoming_games": _SCORES}

# Wire query key -> wrapper argument name (an ssgt / athlete uuid is never called a season or player id).
_ARG_NAMES = {
    "playerUuid": "athlete_uuid",
    "series": "series_uuid",
    "season": "season_uuid",  # the filter route's season is a season uuid
}

_SEASON_ARG = {
    "name": "season",
    "query_key": "season",
    "type": "int",
    "required": True,
    "description": "Required. Season END year (2026 = 2025/26). Mapped to the provider's ssgt through "
    "/statistics-v2/layout-info (SHL 1975/76 on); a season it does not list raises NoDataError.",
}
_GAME_TYPE_ARG = {
    "name": "game_type",
    "query_key": "gameType",
    "type": "str",
    "default": "regular",
    "choices": list(GAME_TYPES),
    "description": "Game type: regular (Grundserie), playoff (Slutspel), qualdown (relegation qualification) "
    "or playin (2023/24 on).",
}
# Routes whose provider keys the runtime derives from (season, game_type): the wire params it
# replaces are dropped from the wrapper.
_SEASON_ROUTES: Dict[str, Tuple[set, bool]] = {  # path -> (wire params replaced, season required)
    "/statistics-v2/stats-info/{module}": ({"ssgtUuid", "count"}, True),
    "/sports-v2/game-schedule": ({"seasonUuid", "seriesUuid", "gameTypeUuid"}, True),
    "/sports-v2/last-game-day": ({"seasonSeriesGameType"}, False),
    "/sports-v2/next-game-day": ({"seasonSeriesGameType"}, False),
}

# Descriptions for columns the dedicated shapers make (no refs table names them).
_EXTRA = {
    "player_id": "isa (StatNet) player id, Int64. Play-by-play sends it as a string, every other route as an "
    "integer; it is cast here so the frames join.",
    "athlete_uuid": "Platform athlete uuid (rosters and athlete routes key on it); null where SHL has none.",
    "game_uuid": "Platform game uuid (bdhvuc5tex); every gameday route takes it.",
    "team_side": "home or away.",
    "position_group": "skater or goalie (goalies come from the body's gkStats rows).",
    "pop_player_ids": "isa ids of the scoring side's players on the ice for the goal (List[Int64]).",
    "nep_player_ids": "isa ids of the conceding side's players on the ice for the goal (List[Int64]).",
    "assist1_player_id": "isa id of the first assist.",
    "assist1_first_name": "First assist: given name.",
    "assist1_family_name": "First assist: surname.",
    "assist1_jersey_today": "First assist: jersey number in this game (string).",
    "assist2_player_id": "isa id of the second assist.",
    "assist2_first_name": "Second assist: given name.",
    "assist2_family_name": "Second assist: surname.",
    "assist2_jersey_today": "Second assist: jersey number in this game (string).",
    "season": "Season END year (2026 = 2025/26).",
    "season_code": "Provider season code: the START year as a string (2025 = 2025/26).",
    "kind": "Directory entry kind: series, season or game_type.",
    "portrait_url": "URL of the player's latest rendered portrait.",
    "provider": "Stats provider of the ssgt (statnet, impleo).",
    "effective_ssgt_uuid": "The ssgt the head-to-head was computed for: the CURRENT season, whatever the path asked.",
    "team_code": "StatNet team code (SAIK, FHC).",
    "team_uuid": "Platform team uuid.",
    "team_name": "Team name.",
    "team_short_name": "Team short name.",
}

_GAME_PERIOD = {"period": "Period: 0 = the whole game, 1-3 the periods, 4 and up overtime."}
# W / T / L mean different things per route (refs shl-returns.md, "Same names, different
# meanings"; the career log's W + L + no-decisions = GPI in the capture).
_GOALIE_CAREER_WTL = {
    "w": "Goalie wins: games the goalie was credited with the win, overtime and shootout wins included.",
    "l": "Goalie losses: games the goalie was charged with the loss, overtime and shootout losses included.",
    "t": "Goalie ties: games that ended level with the goalie credited a tie; 0 in every sampled row, as "
    "overtime and shootouts now decide every game.",
}
_GOALIE_GAME_WTL = {
    "w": "1 when the goalie was credited with the win in this game (overtime and shootout included), else 0.",
    "l": "1 when the goalie was charged with the loss in this game (overtime and shootout included), else 0.",
    "t": "1 when the goalie was credited a tie in this game, else 0; 0 in every sampled row, as overtime "
    "and shootouts now decide every game.",
}
_STATS_INFO_WTL = {
    "w": "Wins. Team modules (teams_result): total wins, regulation + overtime + shootout; goalkeeper "
    "modules: wins credited to the goalie; standings_standings: regulation wins only.",
    "l": "Losses. Team modules (teams_result): total losses, regulation + overtime + shootout; goalkeeper "
    "modules: losses charged to the goalie; standings_standings: regulation losses only.",
    "t": "Goalie ties (goalkeeper modules only); 0 in every sampled row, as overtime and shootouts now "
    "decide every game.",
}
_EXTRA_BY_SHORT = {
    "boxscore": _GAME_PERIOD,
    "player_box": _GAME_PERIOD,
    "team_stats": _GAME_PERIOD,
    "athlete_career": _GOALIE_CAREER_WTL,
    "athlete_game_log": _GOALIE_GAME_WTL,
    "stats_info": _STATS_INFO_WTL,
}
# Refs prose too terse for the manual-descriptions rules (>= 15 characters, no restated name):
# the same fact, spelled out. Keyed by the resolved text, since many columns share one.
_EXPAND = {
    "Assists.": "Number of assists credited.",
    "Age in years.": "Age of the player in whole years.",
    "Given name.": "Given (first) name of the player.",
    "Surname.": "Surname (family name) of the player.",
    "Height in cm.": "Height of the player in centimetres.",
    "Weight in kg.": "Weight of the player in kilograms.",
    "Club email.": "Contact email address of the club.",
    "Logo URL.": "URL of the team logo image.",
    "home or away.": "Side of the game: home or away.",
    "Blocked shots.": "Number of blocked shots.",
    "Game date.": "Date of the game (YYYY-MM-DD).",
    "Face-offs won.": "Number of face-offs won.",
    "Goals.": "Number of goals scored.",
    "Goals against.": "Number of goals conceded (goals against).",
    "Away goals.": "Goals scored by the away team.",
    "Home goals.": "Goals scored by the home team.",
    "Goals for.": "Number of goals scored (goals for).",
    "Games played.": "Number of games played.",
    "Club chairman.": "Name of the club chairman.",
    "Club fax.": "Fax number of the club.",
    "Shirt number.": "Jersey (shirt) number of the player.",
    "Jersey number.": "Jersey (shirt) number of the player.",
    "Club phone.": "Telephone number of the club.",
    "Times leading.": "Times leading: how often the team was ahead (exact definition unverified).",
    "Line number.": "Line (unit) number the player was listed on.",
    "Club name.": "Full name of the club.",
    "Penalty shots.": "Number of penalty shots.",
    "Rank change.": "Change in standings rank, in places (the reference point is unverified).",
    "Shutouts.": "Number of shutouts (games without a goal against).",
    "Shots on goal.": "Number of shots on goal.",
    "Shootout wins.": "Number of shootout wins.",
    "Saves.": "Number of saves made by the goalie.",
    "Shots wide.": "Number of shots that missed the net.",
    "Team name.": "Name of the team.",
    "Image caption.": "Caption text of the image.",
    "Team short name.": "Short display name of the team.",
    "Arena.": "Name of the arena the game is played in.",
    "Arena name.": "Name of the arena (venue).",
}
SNIPPET = ROOT / "tools" / "codegen" / "shl_column_descriptions.snippet.yaml"
SCHEMA_MODULE = ROOT / "sportsdataverse" / "hockey" / "shl" / "shl_schemas.py"
# polars dtype -> the code shl_schemas.py stores (shl_parsers._SCHEMA_DTYPES reads it back).
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


def _capture(refs: Path, path: str, variant: Optional[str] = None) -> Path:
    """``capture.py``'s slug: path ``/`` -> ``__``, braces dropped, ``__<variant>`` for stats-info."""
    slug = path.strip("/").replace("/", "__").replace("{", "").replace("}", "")
    return refs / "captures" / (slug + (f"__{variant}" if variant else "") + ".json")


def _md_tables(md: Path) -> Dict[str, Dict[str, str]]:
    """``{route path: {column: description}}`` from the refs returns doc.

    A table row's dotted wire name (``[].homeTeam.teamId``) is matched to snake_case columns by
    every suffix of its parts (``home_team_team_id``, ``team_id``), longest first, so the most
    specific prose wins. Stats-module tables (``#### `players_summary```) belong to stats-info.
    """
    rows: Dict[str, List[Tuple[List[str], str]]] = {}
    route = ""
    for line in md.read_text(encoding="utf-8").splitlines():
        if line.startswith("## `"):
            route = line.split("`")[1]
        elif line.startswith("#### `"):
            route = "/statistics-v2/stats-info/{module}"
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


_FILLER = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.I)


def _bad(name: str, desc: str) -> bool:
    """Whether ``desc`` would fail the manual-descriptions filler rules (missing, short, restated)."""
    d = (desc or "").strip()
    return len(d) < 15 or d.lower() == name.lower() or bool(_FILLER.match(d))


def _write_schema_module(dtypes: Dict[str, Dict[str, str]]) -> None:
    lines = [
        '"""Per-endpoint column -> dtype code for the ``shl`` parsers.',
        "",
        "Generated by tools/codegen/gen_shl.py from the same frames as the returns-schemas: do not edit.",
        "Codes: str, int, float, bool, list[int] (see ``shl_parsers._SCHEMA_DTYPES``).",
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


def _frame_for(
    refs: Path, path: str, short: str, parser: Callable[..., pl.DataFrame], modules: List[str]
) -> pl.DataFrame:
    """A zero-row frame with the union of the columns: full bodies first (their dtypes win), then the captures."""
    paths = [FULL / f for f in _FULL.get(short, [])]
    paths += [_capture(refs, path, m) for m in modules] if short == "stats_info" else [_capture(refs, path)]
    schema: Dict[str, pl.DataType] = {}
    for f in paths:
        for name, dtype in _parse(parser, f).schema.items():
            schema.setdefault(name, dtype)
    schema.update({k: v for k, v in _NULL_IN_CAPTURE.get(short, {}).items() if k in schema})
    return pl.DataFrame(schema=schema)


def _params(path: str, op: dict) -> Tuple[List[dict], List[dict], Dict[str, Any]]:
    path_params: List[dict] = []
    extra_params: List[dict] = []
    examples: Dict[str, Any] = {}
    replaced, season_required = _SEASON_ROUTES.get(path, (set(), False))
    for prm in op.get("parameters", []):
        wire, desc = prm["name"], str(prm.get("description") or "").strip()
        if prm.get("in") == "path":
            entry: Dict[str, Any] = {"name": wire, "type": "str", "required": True, "description": desc}
            if (prm.get("schema") or {}).get("enum"):
                entry["choices"] = list(prm["schema"]["enum"])
            path_params.append(entry)
        elif prm.get("in") == "query" and wire not in replaced:
            name = _ARG_NAMES.get(wire, _column(wire))
            if path.endswith("playerProfile_seasonGameLog") and wire == "season":
                extra_params.append(
                    {
                        "name": "season",
                        "query_key": "season",
                        "type": "int",
                        "description": "Season END year (2026 = 2025/26); the runtime sends the START year this "
                        "route takes. None = the player's default (shorter) log.",
                    }
                )
                examples["season"] = 2026
                continue
            extra: Dict[str, Any] = {"name": name, "query_key": wire, "type": "str", "description": desc}
            if prm.get("required"):
                extra.update(required=True, description=f"Required. {desc}".strip())
            extra_params.append(extra)
            if prm.get("example") not in (None, ""):
                examples[name] = str(prm["example"])
            continue
        if prm.get("in") == "path" and prm.get("example") not in (None, ""):
            examples[wire] = str(prm["example"])
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


def _endpoint(path: str, op: dict) -> Dict[str, Any]:
    short, parser = _ROUTES[path]
    path_params, extra_params, examples = _params(path, op)
    entry: Dict[str, Any] = {
        "short": short,
        "summary": (op.get("summary") or f"Fetch {path}").rstrip(".") + ".",
        "path": path,
        "parser": parser,
        "returns_schema": f"native/{STEM}/{short}",
        "example_args": examples,
    }
    if path_params:
        entry["path_params"] = path_params
    if extra_params:
        entry["extra_params"] = extra_params
    if path == "/statistics-v2/stats-info/{module}":
        entry["fixed_params"] = {"count": 1000}  # the runtime enforces it too
    return entry


def main() -> None:
    refs = refs_dir(STEM, SPEC)
    spec = load_spec(refs, SPEC)
    ops = get_ops(spec)
    missing = sorted({p for p, _ in ops} ^ set(_ROUTES))
    if missing:
        raise SystemExit(f"shl: spec paths and _ROUTES differ: {missing}")
    module_op = dict(ops)["/statistics-v2/stats-info/{module}"]
    modules = next(p["schema"]["enum"] for p in module_op["parameters"] if p["name"] == "module")
    absent = [m for m in modules if not _capture(refs, "/statistics-v2/stats-info/{module}", m).exists()]
    if absent:
        raise SystemExit(f"shl: stats-info modules without a capture: {absent}")
    ops.sort(key=lambda pv: _ROUTES[pv[0]][0])

    doc = {
        "api": STEM,
        "host": HOST,
        "name_pattern": "shl_{short}",
        "module": STEM,
        "parser_module": "hockey.shl.shl_parsers",
        "getter_module": "sportsdataverse.hockey.shl.shl_runtime",
        "qualifier": "",
        "schema_prefix": "shl_",
        "passthrough_query": False,
        "parser_columns": True,
        "raw_types": ["Dict", "str"],
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: SHL answered 404, an empty body (gameday routes before "
                "2023/24), a table with no rows, another game / team / ssgt than the one asked for, or the "
                "season and game type are not in layout-info.",
            ],
            "see_also": [
                {
                    "name": "SHL",
                    "url": "https://www.shl.se/",
                    "note": "the Swedish Hockey League site whose API this is",
                },
                {
                    "name": "fastRhockey",
                    "url": "https://fastRhockey.sportsdataverse.org",
                    "note": "the SportsDataverse hockey package for R",
                },
            ],
        },
        "runtime_imports": ["_get"],
        "endpoints": [_endpoint(p, op) for p, op in ops],
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
    snippet: Dict[str, Dict[str, str]] = {}
    dtypes: Dict[str, Dict[str, str]] = {}
    for path, _op in ops:
        short, parser_name = _ROUTES[path]
        frame = _frame_for(refs, path, short, getattr(shl_parsers, parser_name), modules)
        sources = [_EXTRA_BY_SHORT.get(short, {}), _EXTRA, md.get(path, {}), flat_md, spec_desc]
        cols = columns_from_frame(frame, sources, leaf_fallback=False)
        texts = snippet.setdefault(f"{STEM}_{short}", {})
        for col in cols:
            desc = col["description"].strip()
            texts[col["name"]] = _EXPAND.get(desc, desc)
            col["description"] = ""
        dtypes[short] = {
            name: _DTYPE_CODES[str(dtype).replace("Utf8", "String")] for name, dtype in frame.schema.items()
        }
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"{STEM}_{short}", "kind": "dataframe", "columns": cols})
    bad = sorted(f"{k}.{c}: {d!r}" for k, cs in snippet.items() for c, d in cs.items() if _bad(c, d))
    if bad:
        raise SystemExit("shl: missing or filler descriptions (add them to _EXTRA):\n" + "\n".join(bad))
    write_yaml(SNIPPET, snippet)
    by_columns: Dict[Tuple[str, ...], Tuple[str, Dict[str, str]]] = {}
    for short, types in dtypes.items():  # the parsers find an endpoint's dtypes by its column list
        other, other_types = by_columns.setdefault(tuple(types), (short, types))
        if other_types != types:
            raise SystemExit(f"shl: {other} and {short} share a column list but not its dtypes")
    _write_schema_module(dtypes)
    n = sum(map(len, snippet.values()))
    print(f"{STEM}: {len(ops)} endpoints, stats_info over {len(modules)} modules, {n} descriptions")


if __name__ == "__main__":
    main()
