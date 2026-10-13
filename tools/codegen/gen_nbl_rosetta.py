"""Generate the ``nbl_rosetta`` flat-API endpoint YAML + returns-schemas (NBL / WNBL / NBL1 Rosetta proxy).

Idempotent: same spec + same captures -> byte-identical output. Modeled on ``gen_liiga.py``, with the
shared plumbing in ``gen_soccer_common.py``. Reads the frozen spec, the returns md and (for the column
descriptions) nothing else from the ``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the
sibling checkout), and emits:

* ``tools/codegen/endpoints/nbl_rosetta.yaml`` -- 22 wrappers over the spec's 18 paths. One route can feed
  several frames (``/match/{match_id}/live/all``: header, official player box, team box by period, player box
  by period and play-by-play), each with its own parser; the table :data:`ENDPOINTS` is the source of truth.
* ``tools/codegen/schemas/native/nbl_rosetta/<short>.yaml`` -- one returns-schema per wrapper: the union of
  the columns of its parser run over every committed variant of its route, stamped the way the runtime stamps
  a live body. The match frames use the FULL body in ``tests/fixtures/nbl_rosetta/full/`` first (the truncated
  captures cannot show that a rate arrives as an integer when whole). Schema key ``nbl_rosetta_<short>``;
  every ``description`` is ``''``.
* ``tools/codegen/nbl_rosetta_column_descriptions.snippet.yaml`` (gitignored) -- the column descriptions,
  keyed ``nbl_rosetta_<short>`` -> column, for ``manual_column_descriptions.yaml``. Each comes from
  :data:`ROUTE` (route-specific), :data:`LOCAL` / the id rules (parser-built columns), else the wrapper's
  own ``nbl-rosetta-returns.md`` row. A filler description stops the run.
* ``sportsdataverse/basketball/nbl_au/nbl_rosetta_dtypes.py`` -- each parser's column dtypes (the
  returns-schema types), so an empty or all-null frame is typed like a populated one.
* ``tests/fixtures/nbl_rosetta/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs).

Run: ``python tools/codegen/gen_nbl_rosetta.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.nbl_au import nbl_rosetta_parsers as P
from sportsdataverse.basketball.nbl_au.nbl_rosetta_runtime import ORGS, REQUEST_KEY, TYPES, end_year

HOST = "https://prod.rosetta.nbl.com.au/get"
SPEC = "nbl-rosetta.openapi.yaml"
RETURNS_MD = "nbl-rosetta-returns.md"
FIXTURES = ROOT / "tests" / "fixtures" / "nbl_rosetta"
SCHEMA_PREFIX = "nbl_rosetta_"
#: Column descriptions, keyed ``nbl_rosetta_<short>`` -> column; merged into manual_column_descriptions.yaml.
SNIPPET = ROOT / "tools" / "codegen" / "nbl_rosetta_column_descriptions.snippet.yaml"
#: The spec's route list, committed so the tests check ENDPOINTS without the refs checkout.
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: Each parser's column dtypes, from the returns-schemas, in a package-safe module.
DTYPES_MODULE = ROOT / "sportsdataverse" / "basketball" / "nbl_au" / "nbl_rosetta_dtypes.py"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

SEASON_P = (
    "Season END year (2026 = 2025-26). The runtime maps it to Rosetta's year: the START year for the NBL and WNBL "
    "from 1998-99, the calendar year for NBL1 and the NBL / WNBL winter seasons up to 1998. END 1998 and 1999 both "
    "read Rosetta year 1998, which holds two seasons; the matches frame keeps the one asked for."
)
ORG_P = "Organisation: nbl (men's NBL), wnbl (women's) or nbl1 (the second tier, men's and women's conferences)."
TYPE_P = (
    "Season type: all (every type of the year), regular (includes finals), preseason, in_season (cups, Blitz, "
    "NBAxNBL, All-Star) or test."
)
MATCH_ID = "DataCore fixture id of the match (match_id of nbl_rosetta_matches), a UUID, e.g. b217a699-4bef-11f0-bc08-933162362355."
PLAYER_ID = (
    "Player id: the DataCore person id (player_id of nbl_rosetta_players) or Rosetta's own id; either is accepted."
)
TEAM_ID = "Team id: the DataCore id (team_id of nbl_rosetta_teams) or Rosetta's own id; either is accepted."
SEASON_ID = (
    "Season id: the DataCore season id (season_id of nbl_rosetta_seasons) or Rosetta's own id "
    "(rosetta_season_id); either is accepted. Not the year."
)
TEAM_CODE = "Team code (team_code of nbl_rosetta_teams), e.g. PER for the Perth Wildcats."
LIMIT = "Rows to return. Omitted = a page large enough for the whole list (a short page cut by Rosetta's path cache is refused)."
OFFSET = "Rows to skip. Pages can overlap, so prefer one large limit."

PERTH_MATCH = "b217a699-4bef-11f0-bc08-933162362355"
PEPPER = "7619d3ea-410d-11f0-9235-2902fc2f936f"
PERTH = "befee10a-410c-11f0-b3f4-1b3c55488bbb"
NBL_SEASON_2025 = "1f8e4a79-e98b-457b-85a5-e4b898c6c0bd"


def _q(name: str, wire: str, desc: str, *, typ: str = "str", **extra: Any) -> Dict[str, Any]:
    return {"name": name, "query_key": wire, "type": typ, "description": desc, **extra}


def _p(name: str, desc: str, *, typ: str = "str", **extra: Any) -> Dict[str, Any]:
    return {"name": name, "type": typ, "required": True, "description": desc, **extra}


_ORG = {"name": "org", "type": "str", "required": False, "default": "nbl", "choices": list(ORGS), "description": ORG_P}
_SEASON = _p("season", SEASON_P, typ="int")
_TYPE = {
    "name": "season_type",
    "type": "str",
    "required": False,
    "default": "all",
    "choices": list(TYPES),
    "description": TYPE_P,
}
_LIMIT = _q("limit", "limit", LIMIT, typ="int")
_OFFSET = _q("offset", "offset", OFFSET, typ="int")
_MATCH = [_p("match_id", MATCH_ID)]

# short -> (spec path, wrapper path, parser, fixture prefixes (files <prefix>--*.json; a path under full/ first),
#           summary, endpoint fields)
Entry = Tuple[str, str, Callable[..., Any], List[str], str, Dict[str, Any]]
_LIVE = ["full/match__match_id__live__all--nbl_2025", "match__match_id__live__all"]
ENDPOINTS: Dict[str, Entry] = {
    "seasons": (
        "/{org}/seasons",
        "/{org}/seasons",
        P.parse_nbl_rosetta_seasons,
        ["org__seasons"],
        "Every season of an organisation: regular, preseason, in-season cups and test seasons, with dates.",
        {"path_params": [_ORG], "extra_params": [_LIMIT], "example_args": {"org": "nbl"}},
    ),
    "seasons_current": (
        "/{org}/seasons/current",
        "/{org}/seasons/current",
        P.parse_nbl_rosetta_seasons,
        ["org__seasons__current"],
        "The season in progress (empty between seasons: NoDataError).",
        {"path_params": [_ORG], "example_args": {"org": "nbl"}},
    ),
    "matches": (
        "/{org}/matches/in/season/{year}/{type}",
        "/{org}/matches/in/season/{season}/{season_type}",
        P.parse_nbl_rosetta_matches,
        ["org__matches__in__season__year__type"],
        "Matches of a season: ids, teams, venue, status, scores and start time (UTC).",
        {
            "path_params": [_SEASON, _ORG, _TYPE],
            "extra_params": [_LIMIT, _OFFSET],
            "example_args": {"season": 2026, "org": "nbl", "season_type": "regular"},
        },
    ),
    "teams": (
        "/{org}/teams",
        "/{org}/teams",
        P.parse_nbl_rosetta_teams,
        ["org__teams"],
        "Every team record of an organisation: current, defunct, exhibition and development sides.",
        {"path_params": [_ORG], "extra_params": [_LIMIT], "example_args": {"org": "nbl"}},
    ),
    "match_by_id": (
        "/match/{match_id}",
        "/match/{match_id}",
        P.parse_nbl_rosetta_match,
        ["match__match_id"],
        "One match's header (the same body as nbl_rosetta_match): status, scores, venue, season and teams.",
        {"path_params": _MATCH, "example_args": {"match_id": PERTH_MATCH}},
    ),
    "match": (
        "/match/{match_id}/live/all",
        "/match/{match_id}/live/all",
        P.parse_nbl_rosetta_match,
        _LIVE,
        "One match's header: status, scores, clock, venue, season and teams. Box score and play-by-play: the match_* frames.",
        {"path_params": _MATCH, "example_args": {"match_id": PERTH_MATCH}},
    ),
    "match_player_box": (
        "/match/{match_id}/live/all",
        "/match/{match_id}/live/all",
        P.parse_nbl_rosetta_match_player_box,
        _LIVE,
        "One match's official player box score, one row per player line (period 0 = whole game).",
        {"path_params": _MATCH, "example_args": {"match_id": PERTH_MATCH}},
    ),
    "match_team_box": (
        "/match/{match_id}/live/all",
        "/match/{match_id}/live/all",
        P.parse_nbl_rosetta_match_team_box,
        _LIVE,
        "One match's team box score by period, one row per team and period (0 = whole game).",
        {"path_params": _MATCH, "example_args": {"match_id": PERTH_MATCH}},
    ),
    "match_player_period_box": (
        "/match/{match_id}/live/all",
        "/match/{match_id}/live/all",
        P.parse_nbl_rosetta_match_player_period_box,
        _LIVE,
        "One match's player box score by period (Rosetta-computed from the event log), one row per player and period.",
        {"path_params": _MATCH, "example_args": {"match_id": PERTH_MATCH}},
    ),
    "match_pbp": (
        "/match/{match_id}/live/all",
        "/match/{match_id}/live/all",
        P.parse_nbl_rosetta_match_pbp,
        _LIVE,
        "One match's play-by-play with shot x/y, oldest first (NBL 2025-26 included, which ESPN's nbl feed lacks).",
        {"path_params": _MATCH, "example_args": {"match_id": PERTH_MATCH}},
    ),
    "standings": (
        "/{org}/standings/{year}/regular",
        "/{org}/standings/{season}/regular",
        P.parse_nbl_rosetta_standings,
        ["org__standings__year__regular"],
        "Regular-season ladder: position, record, points for and against, home and away splits and form.",
        {
            "path_params": [_SEASON, _ORG],
            "extra_params": [_LIMIT],
            "example_args": {"season": 2026, "org": "nbl"},
        },
    ),
    "players": (
        "/{org}/players/in/season/{year}",
        "/{org}/players/in/season/{season}",
        P.parse_nbl_rosetta_players,
        ["org__players__in__season__year"],
        "Roster rows of every team in a season: player bio, jersey, position and team.",
        {
            "path_params": [_SEASON, _ORG],
            "extra_params": [_LIMIT, _OFFSET],
            "example_args": {"season": 2026, "org": "nbl"},
        },
    ),
    "team_players": (
        "/{org}/players/for/team/{team_id}/in/season/{year}",
        "/{org}/players/for/team/{team_id}/in/season/{season}",
        P.parse_nbl_rosetta_players,
        ["org__players__for__team__team_id__in__season__year"],
        "Roster of one team in a season: player bio, jersey and position.",
        {
            "path_params": [_p("team_id", TEAM_ID), _SEASON, _ORG],
            "example_args": {"team_id": PERTH, "season": 2026, "org": "nbl"},
        },
    ),
    "outcomes": (
        "/match/outcomes/for/{org}/teams/in/season/{year}/{type}",
        "/match/outcomes/for/{org}/teams/in/season/{season}/{season_type}",
        P.parse_nbl_rosetta_outcomes,
        ["match__outcomes__for__org__teams__in__season__year__type"],
        "Win-loss record and last 10, 5 and 3 results per team, keyed by team name.",
        {
            "path_params": [
                _SEASON,
                _ORG,
                {**_TYPE, "default": "regular"},
            ],
            "example_args": {"season": 2026, "org": "nbl", "season_type": "regular"},
        },
    ),
    "player_game_log": (
        "/{org}/player_boxscores/for/{player_id}/in/season/{year}/regular",
        "/{org}/player_boxscores/for/{player_id}/in/season/{season}/regular",
        P.parse_nbl_rosetta_player_game_log,
        ["org__player_boxscores__for__player_id__in__season__year__regular"],
        "A player's game log for a season: the box-score line of each game (period 0 = whole game) and the game.",
        {
            "path_params": [_p("player_id", PLAYER_ID), _SEASON, _ORG],
            "extra_params": [
                _q(
                    "participated",
                    "filter[participated]",
                    "1 = only games the player played in (without it, games on the roster that he sat out too).",
                    typ="int",
                )
            ],
            "example_args": {"player_id": PEPPER, "season": 2026, "org": "nbl", "participated": 1},
        },
    ),
    "player_stats": (
        "/{org}/statistics/for/player/{player_id}",
        "/{org}/statistics/for/player/{player_id}",
        P.parse_nbl_rosetta_player_season_stats,
        ["org__statistics__for__player__player_id"],
        "A player's season totals and averages for every season, by period.",
        {"path_params": [_p("player_id", PLAYER_ID), _ORG], "example_args": {"player_id": PEPPER, "org": "nbl"}},
    ),
    "stats_leaders": (
        "/{org}/stats/leaders/for/season/id/{season_id}",
        "/{org}/stats/leaders/for/season/id/{season_id}",
        P.parse_nbl_rosetta_player_season_stats,
        ["org__stats__leaders__for__season__id__season_id"],
        "Player season totals and averages of a season (the leaderboard), one row per player and period.",
        {
            "path_params": [_p("season_id", SEASON_ID), _ORG],
            "extra_params": [_LIMIT],
            "example_args": {"season_id": NBL_SEASON_2025, "org": "nbl"},
        },
    ),
    "team_stats_leaders": (
        "/{org}/stats/leaders/for/season/id/{season_id}/team/{team_code}",
        "/{org}/stats/leaders/for/season/id/{season_id}/team/{team_code}",
        P.parse_nbl_rosetta_player_season_stats,
        ["org__stats__leaders__for__season__id__season_id__team__team_code"],
        "Player season totals of one team in a season, optionally one period and sorted.",
        {
            "path_params": [_p("season_id", SEASON_ID), _p("team_code", TEAM_CODE), _ORG],
            "extra_params": [
                _LIMIT,
                _q(
                    "period",
                    "filter[period]",
                    "Period to keep: 0 = whole game, 1-4 quarters, 5 and up overtime.",
                    typ="int",
                ),
                _q("sort", "sort", "Sort key; a leading minus sorts descending, e.g. -points.", typ="str"),
            ],
            "example_args": {
                "season_id": NBL_SEASON_2025,
                "team_code": "PER",
                "org": "nbl",
                "period": 0,
                "sort": "-points",
            },
        },
    ),
    "team_stats": (
        "/{org}/team/stats/for/season/{year}/regular",
        "/{org}/team/stats/for/season/{season}/regular",
        P.parse_nbl_rosetta_team_season_stats,
        ["org__team__stats__for__season__year__regular"],
        "Team season totals and averages by period (0 = whole game, 1-4 quarters, 5 and up overtime).",
        {
            "path_params": [_SEASON, _ORG],
            "extra_params": [_LIMIT],
            "example_args": {"season": 2026, "org": "nbl"},
        },
    ),
    "next_match": (
        "/next/match/for/{org}/{year}",
        "/next/match/for/{org}/{season}",
        P.parse_nbl_rosetta_next_match,
        ["next__match__for__org__year"],
        "The organisation's next scheduled match (empty between seasons).",
        {"path_params": [_ORG, _SEASON], "example_args": {"org": "nbl", "season": 2027}},
    ),
    "next_match_team": (
        "/next/match/for/team/{team_code}/{org}/{year}",
        "/next/match/for/team/{team_code}/{org}/{season}",
        P.parse_nbl_rosetta_next_match,
        ["next__match__for__team__team_code__org__year"],
        "Next match; the team segment is ignored by Rosetta (the body is the organisation's next match).",
        {
            "path_params": [_p("team_code", TEAM_CODE), _ORG, _SEASON],
            "example_args": {"team_code": "PER", "org": "nbl", "season": 2027},
        },
    ),
    "next_matches": (
        "/next/matches/for/teams/in/{org}/{year}",
        "/next/matches/for/teams/in/{org}/{season}",
        P.parse_nbl_rosetta_next_matches,
        ["next__matches__for__teams__in__org__year"],
        "Each team's next matches: opponent, venue and start time.",
        {"path_params": [_ORG, _SEASON], "example_args": {"org": "nbl", "season": 2027}},
    ),
}

#: short -> returns-md section heading (the spec path), the entity of a row's own ids, and the md column prefix
#: of the list block a match frame reads (``player_match_statistics`` ...).
SECTION: Dict[str, Tuple[str, Optional[str], Optional[str]]] = {
    "seasons": ("/{org}/seasons", "season", None),
    "seasons_current": ("/{org}/seasons/current", "season", None),
    "matches": ("/{org}/matches/in/season/{year}/{type}", "match", None),
    "teams": ("/{org}/teams", "team", None),
    "match_by_id": ("/match/{match_id}", "match", None),
    "match": ("/match/{match_id}/live/all", "match", None),
    "match_player_box": ("/match/{match_id}/live/all", "stat_line", "player_match_statistics"),
    "match_team_box": ("/match/{match_id}/live/all", "stat_line", "team_match_statistics"),
    "match_player_period_box": ("/match/{match_id}/live/all", None, "calculated_player_statistics"),
    "match_pbp": ("/match/{match_id}/live/all", None, "play_by_play"),
    "standings": ("/{org}/standings/{year}/regular", "standing", None),
    "players": ("/{org}/players/in/season/{year}", None, None),
    "team_players": ("/{org}/players/for/team/{team_id}/in/season/{year}", None, None),
    "outcomes": ("/match/outcomes/for/{org}/teams/in/season/{year}/{type}", None, None),
    "player_game_log": ("/{org}/player_boxscores/for/{player_id}/in/season/{year}/regular", None, None),
    "player_stats": ("/{org}/statistics/for/player/{player_id}", None, None),
    "stats_leaders": ("/{org}/stats/leaders/for/season/id/{season_id}", None, None),
    "team_stats_leaders": ("/{org}/stats/leaders/for/season/id/{season_id}/team/{team_code}", None, None),
    "team_stats": ("/{org}/team/stats/for/season/{year}/regular", None, None),
    "next_match": ("/next/match/for/{org}/{year}", "match", None),
    "next_match_team": ("/next/match/for/team/{team_code}/{org}/{year}", "match", None),
    "next_matches": ("/next/matches/for/teams/in/{org}/{year}", None, None),
}

# --- descriptions ------------------------------------------------------------------------------

_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")
_HEAD = re.compile(r"^##\s+`(.*?)`\s*$")


def _md_sections(path: Path) -> Dict[str, Dict[str, str]]:
    """``{section path: {dotted column: description}}`` of the returns md."""
    out: Dict[str, Dict[str, str]] = {}
    cur: Optional[Dict[str, str]] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        head = _HEAD.match(line)
        if head:
            cur = out.setdefault(head.group(1), {})
            continue
        row = _ROW.match(line)
        if row and cur is not None:
            cur[row.group(1)] = row.group(2).strip()
    return out


def _flat_name(dotted: str, entity: Optional[str], block: Optional[str]) -> Optional[str]:
    """The column the parser gives an md row's dotted path, or None (an array child, a dropped field)."""
    toks = dotted.split(".")
    if block is not None:
        if toks[:2] != [block, "[]"]:
            return None
        toks = toks[2:]
    if not toks or "[]" in toks:
        return None
    if len(toks) == 1 and toks[0] in ("id", "external_id") and entity:
        return f"{entity}_id" if toks[0] == "external_id" else f"rosetta_{entity}_id"
    nested: Any = None
    for tok in reversed(toks):
        nested = {tok: nested}
    flat = {k: v for k, v in P._flatten(nested).items() if not P._DROP.search(k)}
    if len(flat) != 1:
        return None
    (name,) = flat
    return P._RENAME.get(name, name)


def _md_index(md: Dict[str, Dict[str, str]], short: str) -> Dict[str, str]:
    section, entity, block = SECTION[short]
    out: Dict[str, str] = {}
    for dotted, desc in md[section].items():
        name = _flat_name(dotted, entity, block)
        if name and desc and name not in out:
            out[name] = desc
    return out


_START = "Rosetta year"
#: Parser-built columns and the id rules (a column no md row describes).
LOCAL: Dict[str, str] = {
    "org": "Organisation the row belongs to: nbl, wnbl or nbl1. From the row's competition, else from the request.",
    "season_end_year": (
        "sdv season key: the END year of the season (2026 = 2025-26), derived from season_year (a START year for the "
        "NBL and WNBL from 1998-99, a calendar year for NBL1 and the 1979-1997 winter seasons; year 1998 is split on "
        "the start date). Where the body carries no year it is the season asked for."
    ),
    "season_year": (
        "Rosetta's year of the season: the START year for the NBL and WNBL from 1998-99 (2025 = 2025-26), the calendar "
        "year for NBL1 and the 1979-1997 winter seasons. See season_end_year for the sdv key."
    ),
    "match_status": (
        "Status of the match in Rosetta's words (complete for a finished game): box score and play-by-play of any "
        "other status are partial."
    ),
    "rosetta_standing_id": "Rosetta's id of this ladder row (UUID v4); not an entity id.",
    "team_name": "Name of the team as Rosetta spells it, e.g. Perth Wildcats.",
}
_PLAYED = "Games played in the season (in the period rows, by the player or team)."
LOCAL.update(
    {
        "venue_name": "Name of the venue (arena), e.g. RAC Arena.",
        "player_first_name": "The player's given name, e.g. Elijah.",
        "player_last_name": "The player's family name, e.g. Pepper.",
        "season_competition_name": "Name of the competition the season belongs to (e.g. NBL, WNBL, NBL1 North Men).",
        "season_competition_league_name": "Name of the league the competition belongs to (NBL, WNBL or NBL1).",
        "competition_league_name": "Name of the league the season's competition belongs to (NBL, WNBL or NBL1).",
        "games": _PLAYED,
        "conference_name": "Name of the NBL1 conference the team plays in (e.g. North, South); null outside NBL1.",
        "home_team_conference_name": "Name of the NBL1 conference of the home team; null outside NBL1.",
        "away_team_conference_name": "Name of the NBL1 conference of the away team; null outside NBL1.",
    }
)
#: Route-specific descriptions (keyed by wrapper short) for what the md does not say or restates.
ROUTE: Dict[str, Dict[str, str]] = {
    "seasons": {
        "name": "Season name as Rosetta spells it (NBL26, WNBL 2024/25, 2025 South Men); names drift, key on season_year.",
    },
    "seasons_current": {
        "name": "Season name as Rosetta spells it (NBL26, WNBL 2024/25); names drift, key on season_year.",
        "competition_name": "Name of the competition the season belongs to (e.g. NBL).",
    },
    "teams": {"name": "Team name as Rosetta spells it, e.g. Perth Wildcats."},
    "standings": {
        "played": "Games played in the season (wins plus losses).",
        "points_for": "Points scored by the team in the season.",
    },
    "outcomes": {
        "team_name": "Name of the team, the key of Rosetta's outcomes map (the body carries no team id).",
        "wins": "Wins of the team in the season type asked for.",
        "losses": "Losses of the team in the season type asked for.",
        "last10": "Results of the team's last 10 games as a W/L string, oldest first.",
        "last5": "Results of the team's last 5 games as a W/L string, oldest first.",
        "last3": "Results of the team's last 3 games as a W/L string, oldest first.",
    },
    "next_matches": {
        "match_slug": "URL slug of the upcoming match, e.g. sydney-kings-v-nz-breakers-men-14-10-2026.",
        "match_title": "Title of the upcoming match, e.g. Sydney Kings v NZ Breakers men 14-10-2026.",
        "start_time_datetime": "Start of the upcoming match, UTC, ISO 8601 text without an offset.",
        "is_home": "True when the row's team is the home team of the upcoming match.",
        "opponent_name": "Name of the opponent in the upcoming match.",
        "venue": "Name of the venue (arena) of the upcoming match.",
    },
    "match_pbp": {
        "clock_seconds": "Game clock at the event as seconds (parsed from clock, MM:SS); null for events without a clock.",
        "shot_clock_seconds": "Shot clock at the event as seconds (parsed from shot_clock); in Genius-era backfills it repeats the game clock.",
        "score_home": "Home team's running score after the event (score_1 in the body).",
        "score_away": "Away team's running score after the event (score_2 in the body).",
        "sub_type_key": "sub_type folded to snake_case, so the Title Case 2025-26 spelling and the camelCase backfill spelling match.",
        "is_home": "True when the event's team is the match's home team; null when the event has no team.",
        "player_name": "Full name of the player the event belongs to; null for events no player owns (team rebounds, period starts).",
        "jersey_number": "Jersey number of the player at the event, as text (a WNBL backfill writes 6.0).",
        "x": (
            "Shot or event position along the court length, percent 0-100, in game orientation (each team's shots sit "
            "at x > 50 for two periods and x < 50 for the others). Unverified: which end is x = 0; DataCore-era "
            "free-throw, foul and turnover coordinates are not at the free-throw line. Null where none was recorded."
        ),
        "y": (
            "Shot or event position across the court width, percent 0-100 (free throws and close shots cluster near "
            "50). Unverified: which sideline is y = 0. Null where none was recorded."
        ),
        "feed": (
            "Feed shape: datacore (NBL and WNBL 2025-26: events carry fixtureId, arrive newest first, Title Case "
            "sub_type, a real shot clock) or backfill (older seasons and NBL1 2025: no fixtureId, oldest first, "
            "camelCase sub_type, shot_clock repeating the game clock)."
        ),
    },
}

_LABELS = {
    "season": "season",
    "team": "team",
    "player": "player",
    "venue": "venue",
    "match": "match",
    "home_team": "home team",
    "away_team": "away team",
    "stat_line": "box-score line",
    "standing": "ladder row",
    "opponent": "opponent",
}


def _label(stem: str) -> str:
    words = stem.replace("_", " ")
    for key, label in sorted(_LABELS.items(), key=lambda kv: -len(kv[0])):
        if stem.endswith(key):
            prefix = stem[: -len(key)].strip("_").replace("_", " ")
            return f"{prefix}'s {label}" if prefix else label
    return words


def _id_desc(col: str) -> Optional[str]:
    """Description of an entity id column, by the module's id rule."""
    m = re.match(r"^(rosetta_)?(.+)_id$", col)
    if not m or m.group(2) in ("stat_line_action",):
        return None
    rosetta, stem = m.groups()
    label = _label(stem)
    if stem == "match":
        return (
            "Rosetta's match RECORD id (UUID v4) as carried by box-score and play-by-play rows; no route accepts it. "
            "Join on match_id."
            if rosetta
            else "DataCore fixture id of the match (UUID v1): the {match_id} route key and fixtureId in play-by-play. Join on it."
        )
    if stem == "stat_line":
        return (
            "Rosetta's id of the box-score line (UUID v4)."
            if rosetta
            else "Opaque id (a 64-character hash) of the box-score line; not an entity id."
        )
    if rosetta:
        return f"Rosetta's own id (UUID v4; v5 on NBL1) of the {label}; routes that take this entity accept it as well as the DataCore id."
    return (
        f"DataCore id (UUID v1, Sportradar) of the {label}; stable across seasons and the id to join on across routes."
    )


def _filler(name: str, desc: str) -> bool:
    """True for a description the manual-descriptions test rejects, or one that only restates the name."""
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


def _tidy(desc: str) -> str:
    desc = " ".join(desc.split())
    if desc and "_" not in desc.split(" ", 1)[0]:
        desc = desc[0].upper() + desc[1:]
    return desc if desc.endswith((".", ")")) else desc + "."


_TEAM_CODE = re.compile(r"^(?:(.*)_)?team_code$")


def _describe(short: str, name: str, md: Dict[str, str]) -> str:
    for source in (ROUTE.get(short, {}), LOCAL):
        if name in source:
            return source[name]
    if name in md and not _filler(name, md[name]):
        return _tidy(md[name])
    code = _TEAM_CODE.match(name)
    if code and code.group(1):
        return f"Team code of the {_label(code.group(1).replace('_team', '') + '_team')}, e.g. PER or SYD."
    return _id_desc(name) or (_tidy(md[name]) if name in md else "")


# --- frames ------------------------------------------------------------------------------------


def _variants(prefix: str) -> List[Path]:
    """Committed fixtures of a route: ``<prefix>--*.json`` (a ``full/...`` prefix names one file, stem only)."""
    if prefix.startswith("full/"):
        return [FIXTURES / (prefix + ".json")]
    return sorted(FIXTURES.glob(f"{prefix}--*.json"))


def _stamp(path: Path, body: Dict[str, Any]) -> Dict[str, Any]:
    """The request stamp the runtime would add, read off the fixture name (``<route>--<org>[_<year>].json``)."""
    variant = path.stem.split("--", 1)[1]
    org, _, year = variant.partition("_")
    stamp: Dict[str, Any] = {"org": org}
    if year.isdigit():
        stamp.update(year=int(year), season=end_year(org, int(year), None) or int(year))
    data = body.get("data")
    if path.stem.startswith("match__match_id") and isinstance(data, list) and data:
        stamp["match_id"] = data[0].get("id")
    return {**body, REQUEST_KEY: stamp}


def _frames(entry: Entry) -> List[pl.DataFrame]:
    _spec, _path, parser, prefixes, _summary, _fields = entry
    out: List[pl.DataFrame] = []
    for prefix in prefixes:
        for path in _variants(prefix):
            out.append(parser(_stamp(path, json.loads(path.read_text(encoding="utf-8")))))
    return out


_CODE = {pl.Int64: "i", pl.Float64: "f", pl.Boolean: "b", pl.String: "s"}
_TYPE = {"i": "integer", "f": "numeric", "b": "logical", "s": "character"}


def _union(short: str, frames: List[pl.DataFrame]) -> Dict[str, str]:
    """``{column: dtype code}`` in first-seen order over the variants; a column takes its first valued dtype."""
    seen: Dict[str, Any] = {}
    for df in frames:
        for name, dtype in df.schema.items():
            valued = df[name].null_count() < df.height
            if name not in seen or (seen[name][1] is False and valued):
                seen[name] = (dtype, valued)
    out: Dict[str, str] = {}
    for name, (dtype, valued) in seen.items():
        if not valued:
            dtype = P._dtype_for(name)  # null in every fixture: the name rules decide
        if dtype not in _CODE:
            raise SystemExit(f"nbl_rosetta: {short}.{name} has dtype {dtype}, not one of {list(_CODE)}")
        out[name] = _CODE[dtype]
    return out


def _write_dtypes(codes: Dict[str, Dict[str, str]]) -> None:
    """Write :data:`DTYPES_MODULE`: each parser's column dtypes (a shared parser gets the union)."""
    by_parser: Dict[str, Dict[str, str]] = {}
    for short, cols in codes.items():
        merged = by_parser.setdefault(ENDPOINTS[short][2].__name__, {})
        for name, code in cols.items():
            if merged.setdefault(name, code) != code:
                raise SystemExit(f"nbl_rosetta: {ENDPOINTS[short][2].__name__}.{name} has two dtypes")
    lines = [
        '"""Column dtypes of the NBL Rosetta parsers, one map per parser function.',
        "",
        "Generated by ``tools/codegen/gen_nbl_rosetta.py`` from the returns-schemas",
        "(``tools/codegen/schemas/native/nbl_rosetta``); do not edit. Codes: ``i`` Int64, ``f`` Float64,",
        "``b`` Boolean, ``s`` String. ``nbl_rosetta_parsers`` gives these dtypes to an empty frame, a",
        "gap-filled column and an all-null column, so an empty frame stacks onto a populated one.",
        '"""',
        "",
        "from typing import Dict",
        "",
        "DTYPES: Dict[str, Dict[str, str]] = {",
    ]
    for parser in sorted(by_parser):
        lines.append(f'    "{parser}": {{')
        lines += [f'        "{name}": "{code}",' for name, code in by_parser[parser].items()]
        lines.append("    },")
    lines.append("}")
    DTYPES_MODULE.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    spec_path, path, parser, _fixtures, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/nbl_rosetta/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


NOTES = [
    "Unofficial: the JSON proxy behind nbl.com.au (not an API the NBL documents or supports). The terms of use "
    "were not reviewed; payloads are not redistributed as release assets.",
    "Gated by a referer, not a key: every request carries Origin / Referer set to nbl.com.au (the runtime sends "
    "them); without them the host answers 403 MISSING_API_KEY, which is raised as AssetFetchError.",
    "Fills a hole in ESPN: ESPN's nbl feed has box scores but no plays for the 2025-26 NBL season (the stats "
    "provider changed from Genius Sports to Sportradar DataCore that year). nbl_rosetta_match_pbp has the full "
    "event log with shot x/y for 2025-26 and, in the same shape, back to 2011-12 (WNBL about 2014-15). Matches "
    "from 1979 have box scores only.",
    "One host serves three organisations; pass org='nbl' (default), 'wnbl' or 'nbl1'. NBL1 is the second tier "
    "(men's and women's conferences, calendar-year seasons).",
    "Season = the END year (2026 = 2025-26) everywhere. Rosetta's own year is the START year for the NBL and WNBL "
    "from 1998-99 and a calendar year for NBL1 and the 1979-1997 winter seasons; the runtime maps it. Year 1998 "
    "holds two seasons (calendar 1998 and 1998-99).",
    "Ids: <entity>_id columns are DataCore ids (UUID v1; join on them), rosetta_<entity>_id Rosetta's own. "
    "match_id is the DataCore fixture id every match route is keyed on.",
    "Unknown ids, orgs and types answer 200 with no rows (NoDataError). Rosetta caches a body per path and serves "
    "it whatever the query string, so a page cut by someone else's limit is refused (AssetFetchError); retry "
    "after the cache entry expires.",
]


def main() -> None:
    refs = refs_dir("nbl-rosetta", SPEC)
    spec = load_spec(refs, SPEC)
    missing = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if missing:
        raise SystemExit(f"nbl_rosetta: spec paths and ENDPOINTS differ: {sorted(missing)}")
    if set(SECTION) != set(ENDPOINTS):
        raise SystemExit("nbl_rosetta: SECTION and ENDPOINTS differ")
    doc = {
        "api": "nbl_rosetta",
        "host": HOST,
        "name_pattern": "nbl_rosetta_{short}",
        "module": "nbl_rosetta",
        "parser_module": "basketball.nbl_au.nbl_rosetta_parsers",
        "getter_module": "sportsdataverse.basketball.nbl_au.nbl_rosetta_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (match, standings) name other families
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "notes": NOTES,
            "raises": [
                "sportsdataverse.errors.NoDataError: Rosetta answered 404 or no rows (an unknown match, team, player "
                "or season id, an org or season type with nothing, or no season in progress).",
                "sportsdataverse.errors.AssetFetchError: the 403 gate body, a stub or error envelope, a page cut by "
                "Rosetta's path cache, or a body for another org, season or entity than the one asked for.",
            ],
            "see_also": [
                {
                    "name": "NBL",
                    "url": "https://www.nbl.com.au/",
                    "note": "the site whose JSON proxy these wrappers read",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball (R)",
                },
                {
                    "name": "wehoop",
                    "url": "https://wehoop.sportsdataverse.org",
                    "note": "SportsDataverse women's basketball (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/nbl_rosetta.yaml", doc)
    md = _md_sections(refs / RETURNS_MD)
    schema_dir = ROOT / "tools/codegen/schemas/native/nbl_rosetta"
    rewrite_schema_dir(schema_dir)
    P.DTYPES.clear()  # parse by the name rules alone, so the last run's dtypes module cannot feed back
    codes = {short: _union(short, _frames(entry)) for short, entry in ENDPOINTS.items()}
    snippet: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short in ENDPOINTS:
        key = f"{SCHEMA_PREFIX}{short}"
        index = _md_index(md, short)
        snippet[key] = {}
        cols = []
        for name, code in codes[short].items():
            desc = _describe(short, name, index)
            if _filler(name, desc):
                bad.append(f"{key}.{name}: {desc!r}")
            snippet[key][name] = desc
            # descriptions live in manual_column_descriptions.yaml (via the snippet), never here
            cols.append({"name": name, "type": _TYPE[code], "description": ""})
        write_yaml(schema_dir / f"{short}.yaml", {"schema": key, "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit("nbl_rosetta: filler or missing descriptions:\n" + "\n".join(bad))
    write_yaml(SNIPPET, snippet)
    _write_dtypes(codes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"nbl_rosetta: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
