#!/usr/bin/env python3
"""Generate the ``cba`` flat-API endpoint YAML + returns-schemas (Chinese Basketball Association, data-server.cbaleague.com).

Idempotent: same spec + same refs + same fixtures -> byte-identical output. Reads the frozen spec, ``cba-returns.md``
and ``captures/presence`` from the ``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling
checkout) and emits:

* ``sportsdataverse/basketball/cba/cba_dtypes.py`` -- the column dtypes of every parser, from the refs' ``presence``
  sidecars (the type set of every column over the FULL bodies the committed samples were cut from; a 3-row cut mistypes
  rates). The parsers import it.
* ``tools/codegen/endpoints/cba.yaml`` -- 27 wrappers over the spec's 27 operations (12 GET, 15 POST). The POST
  wrappers set ``method: post`` (as the KHL / HA families do); ``cba_runtime._get`` sends the body fields as JSON.
  ``season`` parameters are the END year and the runtime sends ``season - 1``.
* ``tools/codegen/schemas/native/cba/<short>.yaml`` -- one returns-schema per wrapper (descriptions blank).
* ``tools/codegen/notes/cba_descriptions.yaml`` -- ``{cba_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (Integration merges it), with the translated glossaries folded in.
* ``tools/codegen/notes/cba_glossary.yaml`` -- the full nationality, position, team-name and play-by-play phrase
  glossaries (Chinese -> English) from the returns md.
* ``tests/fixtures/cba/spec_paths.json`` -- the spec's route list, so tests never read the private refs checkout.
"""

from __future__ import annotations

import copy
import importlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

sys.path.insert(0, str(ROOT))
from sportsdataverse.dl_utils import underscore  # noqa: E402

HOST = "https://data-server.cbaleague.com/api"
SPEC = "cba.openapi.yaml"
RETURNS = "cba-returns.md"
FIXTURES = ROOT / "tests" / "fixtures" / "cba"
DTYPES_PATH = ROOT / "sportsdataverse" / "basketball" / "cba" / "cba_dtypes.py"
NOTES = ROOT / "tools" / "codegen" / "notes" / "cba_descriptions.yaml"
GLOSSARY = ROOT / "tools" / "codegen" / "notes" / "cba_glossary.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
SCHEMA_PREFIX = "cba_"
GAME_ID = 100105799
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

SEASON = (
    "Required. Season END year (2026 = the 2025-26 season). The API takes the START year (2025), so cba_runtime sends "
    "season - 1."
)
SID = "Required. CBA game id (integer), the global 9-digit sequence 100xxxxxx (100105799 = 2025-26 round 1). Only a finished game is served."
MATCH_TYPE = (
    "Match type id: 1 regular season (default), 2 playoffs, 3 preseason, 4 all-star, 9 relegation play-off "
    "(cba_match_types)."
)
PAGE_NUMBER = "Page number of the paged list, from 1."
PAGE_SIZE = "Rows per page (the site uses 100)."


def _p(name: str, key: str, desc: str, typ: str = "int", **kw: Any) -> Dict[str, Any]:
    return {"name": name, "query_key": key, "type": typ, "description": desc, **kw}


def _req(name: str, key: str, desc: str, typ: str = "int") -> Dict[str, Any]:
    return _p(name, key, desc, typ, required=True)


def _sid_path() -> Dict[str, Any]:
    return {
        "path_params": [{"name": "schedule_id", "type": "int", "required": True, "description": SID}],
        "example_args": {"schedule_id": GAME_ID},
    }


def _sid_body() -> Dict[str, Any]:
    return {"extra_params": [_req("schedule_id", "scheduleId", SID)], "example_args": {"schedule_id": GAME_ID}}


def _paged() -> List[Dict[str, Any]]:
    return [
        _p("page_number", "pageNumber", PAGE_NUMBER, default=1),
        _p("page_size", "pageSize", PAGE_SIZE, default=100),
    ]


def _player_list(extra: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "extra_params": [
            _req("season", "season", SEASON),
            _p("match_type_id", "matchTypeId", MATCH_TYPE, default=1),
            _p(
                "count_ranger",
                "countRanger",
                "How counting statistics scale: 1 per game (default), 2 season total, 3 per 36 minutes, 4 per 100 possessions.",
                default=1,
            ),
            _p(
                "player_ranger",
                "playerRanger",
                "Which players: 1 all (default), 2 domestic (Chinese) only, 4 foreign only.",
                default=1,
            ),
            _p(
                "team_id",
                "teamId",
                "Optional CBA team id (clubs, stable across seasons and renames) to list one team's players.",
            ),
            *extra,
            *_paged(),
        ],
        "example_args": {"season": 2026, "team_id": 29128},
    }


_SORT = [
    _p(
        "sort",
        "sort",
        "Sort column code the site sends (3 is the default order); the API answers an empty body without it.",
        default=3,
    ),
    _p(
        "rank",
        "rank",
        "Sort direction code the site sends (1 default); the API answers an empty body without it.",
        default=1,
    ),
]
_TEAM_SEASON = {
    "example_args": {"team_id": 29128},
}

#: short -> (spec path, wrapper path, method, refs sample, presence prefixes, md section, summary, fields)
Entry = Tuple[str, str, str, str, Tuple[str, ...], str, str, Dict[str, Any]]
ENDPOINTS: Dict[str, Entry] = {
    "seasons": (
        "/api/com-code-tables/getSeason",
        "/com-code-tables/getSeason",
        "get",
        "com-code-tables__getSeason",
        ("[].",),
        "GET /api/com-code-tables/getSeason",
        "Every season of the CBA data site, newest first (the upcoming season is the default), 1995-96 on.",
        {"example_args": {}},
    ),
    "match_types": (
        "/api/com-code-tables/getMatchType",
        "/com-code-tables/getMatchType",
        "get",
        "com-code-tables__getMatchType",
        ("[].",),
        "GET /api/com-code-tables/getMatchType",
        "The match types (regular season, playoffs, preseason, all-star, relegation play-off) with their ids.",
        {"example_args": {}},
    ),
    "team_options": (
        "/api/com-code-tables/getTeam",
        "/com-code-tables/getTeam",
        "get",
        "com-code-tables__getTeam",
        ("[].",),
        "GET /api/com-code-tables/getTeam",
        "The teams of one season as a select list: team id and the season's display name.",
        {
            "extra_params": [_req("season", "season", SEASON)],
            "fixed_params": {"type": 2},
            "example_args": {"season": 2026},
        },
    ),
    "teams": (
        "/api/teams/teamList",
        "/teams/teamList",
        "get",
        "teams__teamList",
        ("[].",),
        "GET /api/teams/teamList",
        "The current team list: logo, team id, club name and display name.",
        {"example_args": {}},
    ),
    "schedule_month": (
        "/api/schedules/month",
        "/schedules/month",
        "get",
        "schedules__month",
        ("[].",),
        "GET /api/schedules/month",
        "Game count per day of one calendar month.",
        {
            "extra_params": [
                _req("year", "year", "Required. CALENDAR year (not the season)."),
                _req("month", "month", "Required. Calendar month, 1-12."),
            ],
            "example_args": {"year": 2025, "month": 12},
        },
    ),
    "schedule_day": (
        "/api/schedules/day",
        "/schedules/day",
        "get",
        "schedules__day",
        ("[].",),
        "GET /api/schedules/day",
        "The games of one day: ids, teams, status, score, round and the data flags.",
        {
            "extra_params": [
                _req(
                    "day",
                    "day",
                    "Required. Date 'YYYY-MM-DD'. The runtime always sends auto=0 (the site's auto mode jumps to the next game day) and refuses rows on another date.",
                    "str",
                )
            ],
            "fixed_params": {"auto": 0},
            "example_args": {"day": "2025-12-12"},
        },
    ),
    "game_info": (
        "/api/team-match-datas/getMatchDataReport/baseInfo/{scheduleId}",
        "/team-match-datas/getMatchDataReport/baseInfo/{schedule_id}",
        "get",
        "team-match-datas__getMatchDataReport__baseInfo__scheduleId",
        ("",),
        "GET /api/team-match-datas/getMatchDataReport/baseInfo/{scheduleId}",
        "Game report header: both teams, score, tip-off time and colours.",
        _sid_path(),
    ),
    "player_box": (
        "/api/player-match-datas/data-count",
        "/player-match-datas/data-count",
        "post",
        "player-match-datas__data-count",
        ("homeCounts.[].", "visitingCounts.[]."),
        "POST /api/player-match-datas/data-count",
        "Player box score of a finished game, both sides, plus a team rebounds/turnovers row per side (null player_id).",
        _sid_body(),
    ),
    "period_scores": (
        "/api/schedule-quarters/getQuarterInfo",
        "/schedule-quarters/getQuarterInfo",
        "post",
        "schedule-quarters__getQuarterInfo",
        (),
        "POST /api/schedule-quarters/getQuarterInfo",
        "Points each side scored in each period of a finished game (up to seven periods).",
        _sid_body(),
    ),
    "game_team_shots": (
        "/api/team-match-datas/schedule-shoot-list",
        "/team-match-datas/schedule-shoot-list",
        "post",
        "team-match-datas__schedule-shoot-list",
        ("[].",),
        "POST /api/team-match-datas/schedule-shoot-list",
        "Shot-zone profile of the two teams of a game. NOT game data: the teams' current season table.",
        _sid_body(),
    ),
    "player_advanced": (
        "/api/player-match-datas/advance-count",
        "/player-match-datas/advance-count",
        "post",
        "player-match-datas__advance-count",
        ("homes.[].", "visitings.[]."),
        "POST /api/player-match-datas/advance-count",
        "Advanced player box of a finished game (usage, true shooting, rebound/steal/block rates, zone splits), one row per player.",
        _sid_body(),
    ),
    "player_extras": (
        "/api/player-match-datas/schedule-other-data",
        "/player-match-datas/schedule-other-data",
        "post",
        "player-match-datas__schedule-other-data",
        ("homePlayers.[].", "visitingPlayers.[]."),
        "POST /api/player-match-datas/schedule-other-data",
        "Fast-break points, dunk points and paint points per player of a finished game, one row per player.",
        _sid_body(),
    ),
    "game_team_four": (
        "/api/teams/schedule-four-elements",
        "/teams/schedule-four-elements",
        "post",
        "teams__schedule-four-elements",
        ("[].",),
        "POST /api/teams/schedule-four-elements",
        "Four-factors table of the two teams of a game. NOT game data: the teams' previous season table.",
        _sid_body(),
    ),
    "best_player": (
        "/api/team-match-datas/getMatchDataReport/bestPlayer/{scheduleId}",
        "/team-match-datas/getMatchDataReport/bestPlayer/{schedule_id}",
        "get",
        "team-match-datas__getMatchDataReport__bestPlayer__scheduleId",
        ("hostBestPlayer.", "guestBestPlayer."),
        "GET /api/team-match-datas/getMatchDataReport/bestPlayer/{scheduleId}",
        "The best player of each side in the game report.",
        _sid_path(),
    ),
    "best_stats": (
        "/api/team-match-datas/getMatchDataReport/bestDataQuota/{scheduleId}",
        "/team-match-datas/getMatchDataReport/bestDataQuota/{schedule_id}",
        "get",
        "team-match-datas__getMatchDataReport__bestDataQuota__scheduleId",
        ("hostTeamBestData.[].", "guestTeamBestData.[]."),
        "GET /api/team-match-datas/getMatchDataReport/bestDataQuota/{scheduleId}",
        "Single-category leaders of each side in the game report.",
        _sid_path(),
    ),
    "rosters": (
        "/api/schedules/getPlayersByScheduleId/{scheduleId}",
        "/schedules/getPlayersByScheduleId/{schedule_id}",
        "get",
        "schedules__getPlayersByScheduleId__scheduleId",
        ("hostPlayers.[].", "guestPlayers.[]."),
        "GET /api/schedules/getPlayersByScheduleId/{scheduleId}",
        "Registered rosters of both sides with player bio fields (the season registration, not the game box).",
        _sid_path(),
    ),
    "report_status": (
        "/api/team-match-datas/getMatchDataReportStatues/{scheduleId}",
        "/team-match-datas/getMatchDataReportStatues/{schedule_id}",
        "get",
        "team-match-datas__getMatchDataReportStatues__scheduleId",
        ("",),
        "GET /api/team-match-datas/getMatchDataReportStatues/{scheduleId}",
        "Whether the game report page exists for the game.",
        _sid_path(),
    ),
    "pbp": (
        "/api/schedule-eventses/filterPage",
        "/schedule-eventses/filterPage",
        "post",
        "schedule-eventses__filterPage",
        ("records.[].",),
        "POST /api/schedule-eventses/filterPage",
        "Play-by-play of a finished game, every period merged, oldest first (Chinese text; no player or team ids).",
        {
            "extra_params": [
                _req("schedule_id", "scheduleId", SID),
                _p(
                    "quarter_num",
                    "quarterNum",
                    "Optional period number. Omitted = every period (the API serves one period per request; the runtime fetches them all and checks the set).",
                ),
            ],
            "fixed_params": {"startNum": 0},
            "example_args": {"schedule_id": GAME_ID},
        },
    ),
    "team_counts": (
        "/api/team-count/season",
        "/team-count/season",
        "get",
        "team-count__season",
        ("[].",),
        "GET /api/team-count/season",
        "Team counts of one season, keyed by match type.",
        {"extra_params": [_req("season", "season", SEASON)], "example_args": {"season": 2026}},
    ),
    "team_basic": (
        "/api/teams/basic-data",
        "/teams/basic-data",
        "post",
        "teams__basic-data",
        ("[].",),
        "POST /api/teams/basic-data",
        "A team's per-game averages or totals, one row per season of the team.",
        {
            "extra_params": [
                _req("team_id", "targetId", "Required. CBA team id (clubs, stable across seasons and renames)."),
                _p("match_type_id", "matchTypeId", MATCH_TYPE, default=1),
                _p(
                    "statistical_dimension",
                    "statisticalDimension",
                    "How counting columns are formatted: 1 per game (default), 2 season total, 3 and 4 per 100 possessions.",
                    default=1,
                ),
                _p(
                    "self_or_other",
                    "selfOrOther",
                    "Whose statistics: 1 the team's own (default), 2 its opponents'.",
                    default=1,
                ),
            ],
            **_TEAM_SEASON,
        },
    ),
    "team_four": (
        "/api/teams/four-elements",
        "/teams/four-elements",
        "post",
        "teams__four-elements",
        ("[].",),
        "POST /api/teams/four-elements",
        "A team's four-factors table, one row per season of the team.",
        {
            "extra_params": [
                _req("team_id", "targetId", "Required. CBA team id (clubs, stable across seasons and renames)."),
                _p("match_type_id", "matchTypeId", MATCH_TYPE, default=1),
            ],
            **_TEAM_SEASON,
        },
    ),
    "team_overview": (
        "/api/team-match-datas/team-entirety-list",
        "/team-match-datas/team-entirety-list",
        "post",
        "team-match-datas__team-entirety-list",
        ("records.[].",),
        "POST /api/team-match-datas/team-entirety-list",
        "A team's overview table (record, pace, per-100 ratings), one row per season of the team.",
        {
            "extra_params": [
                _req("team_id", "teamId", "Required. CBA team id (clubs, stable across seasons and renames)."),
                _p("match_type_id", "matchTypeId", MATCH_TYPE, default=1),
                *_paged(),
            ],
            **_TEAM_SEASON,
        },
    ),
    "team_shots": (
        "/api/team-match-datas/team-shoot-list",
        "/team-match-datas/team-shoot-list",
        "post",
        "team-match-datas__team-shoot-list",
        ("records.[].",),
        "POST /api/team-match-datas/team-shoot-list",
        "A team's shot-zone profile, one row per season of the team (zone splits from 2017-18).",
        {
            "extra_params": [
                _req("team_id", "teamId", "Required. CBA team id (clubs, stable across seasons and renames)."),
                _p("match_type_id", "matchTypeId", MATCH_TYPE, default=1),
                *_paged(),
            ],
            **_TEAM_SEASON,
        },
    ),
    "player_base": (
        "/api/player-base-list",
        "/player-base-list",
        "post",
        "player-base-list",
        ("records.[].",),
        "POST /api/player-base-list",
        "Season player box (one page of the season player table).",
        _player_list([]),
    ),
    "player_offensive": (
        "/api/player-offensive-list",
        "/player-offensive-list",
        "post",
        "player-offensive-list",
        ("records.[].",),
        "POST /api/player-offensive-list",
        "Season player offensive rates (one page).",
        _player_list(_SORT),
    ),
    "player_defensive": (
        "/api/player-defensive-list",
        "/player-defensive-list",
        "post",
        "player-defensive-list",
        ("records.[].",),
        "POST /api/player-defensive-list",
        "Season player defensive rates (one page).",
        _player_list(_SORT),
    ),
    "player_shots": (
        "/api/player-shoot-list",
        "/player-shoot-list",
        "post",
        "player-shoot-list",
        ("records.[].",),
        "POST /api/player-shoot-list",
        "Season player shot-zone profile (one page).",
        _player_list(_SORT),
    ),
}
#: Shorts whose raw body is stamped with ``_request`` (an object body of a game route).
STAMPED = (
    "game_info",
    "player_box",
    "period_scores",
    "player_advanced",
    "player_extras",
    "best_player",
    "best_stats",
    "rosters",
    "report_status",
    "pbp",
)
SIDED = ("player_box", "period_scores", "player_advanced", "player_extras", "best_player", "best_stats", "rosters")

NOTES_DOC = [
    "Unofficial and unsupported: the routes cbaleague.com's own data site (a Vue SPA) calls. SportsDataverse has not "
    "ruled on the site's terms, so payloads are not redistributed as release assets. Requests are spaced at least 1 s "
    "apart; keep use small.",
    "Access: every response body is AES-128-ECB encrypted with a public key shipped in the site's bundle; "
    "``cba_runtime`` decrypts it (stdlib implementation, no dependency) and refuses anything that does not decrypt. The "
    "host blocks datacenter IPs at TCP: set the environment variable CBA_PROXY to a residential proxy URL (it is never "
    "logged). Without it the request goes direct and fails from a datacenter.",
    "Seasons: the API's season is the START year; every ``season`` argument here is the END year (2026 = 2025-26) and "
    "the runtime sends season - 1. Columns named ``season`` keep the wire value (the START year, so sdv END = season + 1). "
    "The season list reaches 1995-96; box data before 2009-10 is unverified.",
    "Coverage: game wrappers serve FINISHED games only (an unfinished game raises AssetFetchError). Play-by-play is "
    "verified on 2017-18 and 2025-26 only and is absent for 2009-10 (NoDataError). There are NO shot x/y coordinates in "
    "this API: shot location exists only as team and player zone splits. Play-by-play has no player or team ids (Chinese "
    "sentence text plus surnames). Box data has 12-minute quarters in 2009-10 and 2017-18 and 10-minute quarters in "
    "2025-26; the women's CBA is not covered (no official source).",
    "Not game data: cba_game_team_shots and cba_game_team_four answer the two teams' SEASON tables (the shot zones are "
    "the teams' current profile; the four factors are the previous season), whichever game is asked for; rows cannot be "
    "scoped to the game. The reference lists (cba_team_options, cba_team_counts) and cba_report_status are not echoed by "
    "the API, so their scope is unchecked.",
    "Duplicate rows: in some games (2025-26 game 100105799) the advanced-box and extras routes repeat player rows (26 "
    "rows for 12 players); the repeats are not always copies. cba_player_advanced and cba_player_extras keep the FIRST "
    "row per player_id.",
    "Text fields are Chinese (names, sentences, position and nationality labels); the column descriptions carry the "
    "English glosses, and tools/codegen/notes/cba_glossary.yaml holds the full nationality, position, team-name and "
    "play-by-play phrase glossaries.",
    "Personal data: the operator ids (createdBy / lastModifiedBy, one holds a mobile number) are removed from every body. "
    "Players' public bio fields (birth date, height, weight) are kept.",
]


# ----------------------------------------------------------------------------- dtypes
def _code(types: List[str], name: str) -> str:
    t = set(types) - {"null"}
    if re.search(r"(^|_)id$", name):
        return "i"
    if t == {"boolean"}:
        return "b"
    if t == {"integer"}:
        return "i"
    if t and t <= {"integer", "number"}:
        return "f"
    return "s"


def _presence(refs: Path, sample: str) -> Dict[str, Any]:
    return json.loads((refs / "captures" / "presence" / f"{sample}.json").read_text(encoding="utf-8"))


def _dtypes(refs: Path) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    junk = {"created_by", "last_modified_by"}
    for short, (_sp, _wp, _m, sample, prefixes, _md, _sum, _f) in ENDPOINTS.items():
        pres = _presence(refs, sample)
        seen: Dict[str, List[str]] = {}
        for key, info in pres.items():
            for pre in prefixes:
                if key.startswith(pre) and pre != key:
                    leaf = key[len(pre) :]
                    if "." in leaf or not leaf:
                        continue
                    seen.setdefault(underscore(leaf), []).extend(info["types"])
        cols = {c: _code(t, c) for c, t in seen.items() if c not in junk}
        head: Dict[str, str] = {}
        if short in STAMPED:
            head["schedule_id"] = "i"
        if short in SIDED:
            head["side"] = "s"
        if short == "period_scores":
            head.update(period="i", points="i")
        out[short] = {**head, **cols}
    return out


def _write_dtypes(dtypes: Dict[str, Dict[str, str]]) -> None:
    lines = [
        '"""Column dtypes of the CBA parsers, one map per parser (``cba_parsers.ROUTES`` key).',
        "",
        "Generated by ``tools/codegen/gen_cba.py`` from the refs ``captures/presence`` sidecars (the type set of every",
        "column over the FULL bodies); do not edit. Codes: ``i`` Int64, ``f`` Float64, ``b`` Boolean, ``s`` String.",
        '"""',
        "",
        "from typing import Dict",
        "",
        "DTYPES: Dict[str, Dict[str, str]] = {",
    ]
    for short, cols in dtypes.items():
        lines.append(f'    "{short}": {{')
        lines += [f'        "{c}": "{k}",' for c, k in cols.items()]
        lines.append("    },")
    lines += ["}", ""]
    DTYPES_PATH.write_text("\n".join(lines), encoding="utf-8", newline="\n")


# ----------------------------------------------------------------------------- fixtures
def _cap(name: str) -> Any:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _full(name: str) -> Any:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def fixture(short: str) -> Any:
    """The body the schema (and the tests) run the parser over: a FULL body where one is committed."""
    full = {
        "seasons": "season_list",
        "team_options": "getTeam_2025",
        "game_info": f"baseinfo_{GAME_ID}",
        "player_box": f"data_count_{GAME_ID}",
        "period_scores": f"quarter_info_{GAME_ID}",
        "best_player": f"best_player_{GAME_ID}",
        "best_stats": f"best_quota_{GAME_ID}",
        "report_status": f"report_status_{GAME_ID}",
    }
    if short in full:
        body = _full(full[short])
    elif short == "pbp":
        pages = _full(f"events_{GAME_ID}")
        recs = [r for q in sorted(pages, key=int) for r in pages[q]["records"]]
        body = {"records": recs, "total": len(recs)}
    else:
        body = _cap(ENDPOINTS[short][3])
    if short in STAMPED and isinstance(body, dict):
        body = {**body, "_request": {"schedule_id": GAME_ID}}
    return body


# ------------------------------------------------------------------------- descriptions
_HEADING = re.compile(r"^## `((?:GET|POST) /api/\S+)`")
_ROW = re.compile(r"^\|\s*`([^`]*)`\s*\|[^|]*\|(.*)\|\s*$")


def _md(path: Path) -> Tuple[Dict[str, Dict[str, str]], Dict[str, List[List[str]]]]:
    """(``{heading: {record path: description}}``, ``{### heading: table rows}``) from the returns md."""
    rows: Dict[str, Dict[str, str]] = {}
    tables: Dict[str, List[List[str]]] = {}
    section = table = None
    for line in path.read_text(encoding="utf-8").splitlines():
        h = _HEADING.match(line)
        if h:
            section, table = h.group(1), None
            continue
        if line.startswith("### "):
            table, section = line[4:].strip(), None
            tables[table] = []
            continue
        if line.startswith("## "):
            section = table = None
            continue
        if section:
            m = _ROW.match(line)
            if m:
                rows.setdefault(section, {}).setdefault(m.group(1), m.group(2).strip().replace("&lt;", "<"))
        elif table and line.startswith("|") and not line.startswith("|---"):
            cells = [c.strip().strip("`") for c in line.strip().strip("|").split("|")]
            if cells and cells[0] not in ("code", "teamId", "chinese form"):
                tables[table].append(cells)
    return rows, tables


def _gloss(rows: List[List[str]], zh: int, en: int) -> str:
    return "; ".join(f"{r[zh]} = {r[en]}" for r in rows if len(r) > max(zh, en))


LOCAL: Dict[str, str] = {
    "schedule_id": "CBA game id (key space: CBA games, ONE global 9-digit sequence 100xxxxxx across seasons and match "
    "types; the {scheduleId} of every game route). Stamped by the runtime from the request; null on a body parsed "
    "without it.",
    "side": "Side of the game the row belongs to: home (the API's host / home block) or away (guest / visiting block).",
    "period": "Period number: 1-4 regulation, 5 and up overtime (the API holds up to seven periods).",
    "points": "Points the side scored IN this period (not cumulative); overtime periods included.",
}


def _lookup(short: str, col: str, md_rows: Dict[str, Dict[str, str]]) -> str:
    entry = ENDPOINTS[short]
    for path, text in md_rows.get(entry[5], {}).items():
        for pre in entry[4]:
            if path.startswith(pre) and path != pre:
                leaf = path[len(pre) :]
                if "." not in leaf and underscore(leaf) == col:
                    return text
    return ""


def _describe(short: str, col: str, md_rows: Dict[str, Dict[str, str]], tables: Dict[str, List[List[str]]]) -> str:
    text = LOCAL.get(col, "") if col in LOCAL and col != "points" else ""
    if short == "period_scores" and col in ("period", "points", "side"):
        text = LOCAL[col]
    text = text or _lookup(short, col, md_rows)
    if not text:
        base = re.sub(r"_(sort|rank)$", "", col)
        if col.endswith("_sort"):
            text = f"Unformatted numeric value behind the {base.replace('_', ' ')} text, the key the site sorts that column by."
        elif col.endswith("_rank"):
            text = f"League rank of {base.replace('_', ' ')} among the compared teams or players (1 = best); null when unranked."
    if 0 < len(text) < 15:
        text += " (as the API names it)"
    if short == "schedule_month" and col == "schedule_num":
        text = text or "Number of games scheduled on that day."
    extra = ""
    if col == "nationality":
        extra = (
            " Chinese country names seen: "
            + _gloss(tables["nationality (player rows, every value seen in the full bodies)"], 1, 2)
            + "."
        )
    elif col in ("position", "position_description", "first_position"):
        extra = (
            " Label glosses: "
            + _gloss(
                tables[
                    "position labels (position, positionDescription, firstPosition: every value seen in the full bodies)"
                ],
                0,
                1,
            )
            + "."
        )
    elif col == "textual_format":
        top = tables["Play-by-play phrase glossary"][:6]
        extra = (
            " Sentence forms ({P} a player, {T} a team): "
            + "; ".join(f"{r[0]} = {r[1]}" for r in top)
            + "; the full glossary is in cba_glossary.yaml."
        )
    elif col in (
        "team_cn_alias",
        "team_name",
        "home_team_name",
        "visiting_team_name",
        "host_team_name",
        "guest_team_name",
        "cn_alias",
    ) and short in (
        "teams",
        "team_options",
        "schedule_day",
        "game_info",
        "team_basic",
        "team_four",
        "team_overview",
        "team_shots",
        "player_base",
    ):
        extra = " Names change with the sponsor, the id does not; teamId -> English glossary in cba_glossary.yaml."
    return (text + extra).strip()


# ------------------------------------------------------------------------------ endpoints
def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    _sp, path, method, _sample, _pre, _md_h, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    doc: Dict[str, Any] = {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": f"parse_cba_{short}",
        "returns_schema": f"native/cba/{short}",
        **copy.deepcopy(fields),
    }
    if method == "post":
        doc["method"] = "post"
        doc["body"] = "json"  # paging keys stay in the query string, every other field is the JSON body
        doc["fixed_in_query"] = [k for k in doc.get("fixed_params", {}) if k in ("pageNumber", "pageSize", "startNum")]
        for p in doc.get("extra_params", []):
            if p["query_key"] in ("pageNumber", "pageSize", "startNum"):
                p["in"] = "query"
    return doc


def main() -> None:
    refs = refs_dir("cba", SPEC)
    spec = load_spec(refs, SPEC)
    differs = {e[0] for e in ENDPOINTS.values()} ^ {
        "/api" + p if not p.startswith("/api") else p for p in spec["paths"]
    }
    if differs:
        raise SystemExit(f"cba: spec paths and ENDPOINTS differ: {sorted(differs)}")
    assert len(ENDPOINTS) == 27 and sum(1 for e in ENDPOINTS.values() if e[2] == "post") == 15

    dtypes = _dtypes(refs)
    _write_dtypes(dtypes)
    from sportsdataverse.basketball.cba import cba_parsers  # after the dtypes exist

    importlib.reload(importlib.import_module("sportsdataverse.basketball.cba.cba_dtypes"))
    P = importlib.reload(cba_parsers)
    assert set(P.ROUTES) == set(ENDPOINTS)

    doc = {
        "api": "cba",
        "host": HOST,
        "name_pattern": "cba_{short}",
        "module": "cba",
        "parser_module": "basketball.cba.cba_parsers",
        "getter_module": "sportsdataverse.basketball.cba.cba_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw DECRYPTED JSON (``list`` or ``dict``); a game route that answers an object also carries "
            "``_request`` (the game id), and play-by-play is merged over every period into ``{records, total}``",
            "raises": [
                "sportsdataverse.errors.NoDataError: the host answered 404, or the game has no play-by-play (2009-10).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for (an "
                "unfinished game, a body that does not decrypt or an unencrypted error envelope, an empty body, a body "
                "for another game or season, a partial play-by-play set, a dead connection).",
            ],
            "notes": NOTES_DOC,
            "see_also": [
                {
                    "name": "CBA data",
                    "url": "https://www.cbaleague.com/data/",
                    "note": "the Chinese Basketball Association data site whose API these wrappers read",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/cba.yaml", doc)

    md_rows, tables = _md(refs / RETURNS)
    schema_dir = ROOT / "tools/codegen/schemas/native/cba"
    rewrite_schema_dir(schema_dir)
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        frame = getattr(P, f"parse_cba_{short}")(fixture(short))
        assert isinstance(frame, pl.DataFrame) and frame.height > 0, short
        cols = columns_from_frame(frame, [], leaf_fallback=False)
        block = notes.setdefault(f"{SCHEMA_PREFIX}{short}", {})
        for col in cols:
            text = _describe(short, col["name"], md_rows, tables)
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"{SCHEMA_PREFIX}{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(
            schema_dir / f"{short}.yaml", {"schema": f"{SCHEMA_PREFIX}{short}", "kind": "dataframe", "columns": cols}
        )
    if bad:
        raise SystemExit(f"cba: empty or filler descriptions (add them to LOCAL): {bad}")
    write_yaml(NOTES, notes)
    write_yaml(
        GLOSSARY,
        {
            "nationality": {
                r[0]: r[2] for r in tables["nationality (player rows, every value seen in the full bodies)"]
            },
            "position_labels": {
                r[0]: r[1]
                for r in tables[
                    "position labels (position, positionDescription, firstPosition: every value seen in the full bodies)"
                ]
            },
            "team_names": {
                r[0]: {"chinese": r[1], "english": r[2]} for r in tables["Team names (teamId -> Chinese -> English)"]
            },
            "play_by_play_phrases": {r[0]: r[1] for r in tables["Play-by-play phrase glossary"]},
        },
    )
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"cba: {len(ENDPOINTS)} endpoints, {sum(map(len, notes.values()))} descriptions -> {NOTES.name}")


if __name__ == "__main__":
    main()
