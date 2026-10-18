"""Generate the ``cebl`` flat-API endpoint YAML + returns-schemas (CEBL, the Canadian Elite Basketball League's own API).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_aba.py`` (itself ``gen_asa.py`` with
the shared plumbing in ``gen_soccer_common.py``). Reads the frozen spec and the returns md from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/cebl.yaml`` -- 12 wrappers, one per route of ``cebl.openapi.yaml`` (the API's own
  ``/openapi.json``); :data:`ENDPOINTS` is the source of truth. The wrappers take the END year (= the calendar
  year) as ``season`` and the filters the Stats Hub sends; ``cebl_runtime`` checks the closed sets.
* ``tools/codegen/schemas/native/cebl/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser run
  over its committed fixture (the FULL 2025 bodies in ``tests/fixtures/cebl/full/`` where there is one, the refs
  captures for the rest). Each parser emits a fixed column list with dtypes pinned by name
  (``cebl_parsers.SCHEMAS``), so a fixture cannot mistype a rate. The schemas carry ``description: ''``
  (descriptions never live in ``schemas/**``).
* ``tools/codegen/notes/cebl_descriptions.yaml`` -- ``{cebl_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (Integration merges it). A filler or missing description stops the run.
* ``tests/fixtures/cebl/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_cebl.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.cebl import cebl_parsers as P
from sportsdataverse.basketball.cebl.cebl_runtime import API, COMPETITIONS, CONFERENCES, MODES, SEGMENTS, STATISTICS

HOST = API
SPEC = "cebl.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "cebl"
#: Column descriptions, keyed ``cebl_<short>`` -> column; merged into manual_column_descriptions.yaml.
NOTES = ROOT / "tools" / "codegen" / "notes" / "cebl_descriptions.yaml"
#: The spec's route list, committed so the tests check ENDPOINTS without the refs checkout.
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

# ------------------------------------------------------------------------------------ params
SEASON_PATH = {
    "name": "season",
    "type": "int",
    "required": True,
    "description": (
        "Required. Season: the calendar year it is played in (May to August) = the sdv END year (2025 = the 2025 "
        "season). The CEBL's first season is 2019. A season with no data answers NoDataError."
    ),
}
SEASON_Q = {
    "name": "season",
    "query_key": "season",
    "type": "int",
    "description": (
        "Season: the calendar year (May to August) = the sdv END year. None = every season combined (the Stats Hub's "
        "All-Time). An unknown year answers NoDataError."
    ),
}
MODE = {
    "name": "mode",
    "query_key": "mode",
    "type": "str",
    "default": "PER_GAME",
    "choices": list(MODES),
    "description": "TOTALS (sums over the selected games) or PER_GAME (per-game averages); the API requires one.",
}
COMPETITION = {
    "name": "competition",
    "query_key": "competition",
    "type": "str",
    "choices": list(COMPETITIONS),
    "description": "REGULAR (regular season) or FINALS (the playoffs). None = both.",
}
SEGMENT = {
    "name": "segment",
    "query_key": "segment",
    "type": "str",
    "choices": list(SEGMENTS),
    "description": (
        "Game subset: HOME, AWAY, L7D (last 7 days), L5G (last 5 games), L10G (last 10 games), MAY, JUNE, JULY or "
        "AUGUST. None = all games. Values from the CEBL Stats Hub bundle."
    ),
}
TEAM_ID_Q = {
    "name": "team_id",
    "query_key": "team_id",
    "type": "int",
    "description": "CEBL team id (``team_id`` in cebl_teams): limits the rows to that team; the rows echo it.",
}
STATISTICS_P = {
    "name": "statistics",
    "query_key": "statistics",
    "type": "str",
    "default": "POINTS,REBOUNDS,ASSISTS,TARGET_SCORES",
    "description": "Comma-separated statistic names; the frame has one block per name. Names the Stats Hub sends: "
    + ", ".join(STATISTICS)
    + ".",
}
PLAYER_ID = {
    "name": "player_id",
    "type": "int",
    "required": True,
    "description": "Required. CEBL player id (global across seasons and teams), e.g. 570965 (Javonte Smart); "
    "``player_id`` in cebl_player_stats.",
}
TEAM_ID_PATH = {
    "name": "team_id",
    "type": "int",
    "required": True,
    "description": "Required. CEBL team id of that season (``team_id`` in cebl_standings), e.g. 20 (Ottawa BlackJacks).",
}


def _p(base: Dict[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(base)


def _json(rel: str) -> Any:
    return json.loads((FIXTURES / rel).read_text(encoding="utf-8"))


def _fixture(rel: str, **stamp: Any) -> Callable[[], Dict[str, Any]]:
    """Loader: the committed body wrapped as the runtime returns it (``rows`` + ``_request``)."""
    return lambda: {"rows": _json(rel), "_request": stamp}


FULL = "full/"
CAP = "captures/"
Entry = Tuple[str, str, Callable[..., Any], Callable[[], Dict[str, Any]], str, str, Dict[str, Any]]
# short -> (spec path, wrapper path, parser, fixture loader, returns-md route, summary, fields)
ENDPOINTS: Dict[str, Entry] = {
    "seasons": (
        "/seasons/",
        "/seasons/",
        P.parse_cebl_seasons,
        _fixture(CAP + "seasons.json", kind="seasons"),
        "/seasons/",
        "Every CEBL season (2019 on), with the active and live flags.",
        {"example_args": {}},
    ),
    "teams": (
        "/teams/",
        "/teams/",
        P.parse_cebl_teams,
        _fixture(CAP + "teams.json", kind="teams"),
        "/teams/",
        "Every franchise identity, past and present (15 team ids; a relocated franchise has a second id).",
        {"example_args": {}},
    ),
    "standings": (
        "/teams/{season}/",
        "/teams/{season}/",
        P.parse_cebl_standings,
        _fixture(FULL + "teams__season__2025.json", kind="standings", season=2025),
        "/teams/{season}/",
        "Standings of one season, one row per team: record, home/away splits, games behind, streak.",
        {"path_params": [_p(SEASON_PATH)], "example_args": {"season": 2025}},
    ),
    "roster": (
        "/teams/{team_id}/roster/{season}/",
        "/teams/{team_id}/roster/{season}/",
        P.parse_cebl_roster,
        _fixture(CAP + "teams__team_id__roster__season__14_2025.json", kind="roster", season=2025, team_id=14),
        "/teams/{team_id}/roster/{season}/",
        "One team's roster in one season: player, nationality, birth date, position and jersey.",
        {"path_params": [_p(TEAM_ID_PATH), _p(SEASON_PATH)], "example_args": {"team_id": 14, "season": 2025}},
    ),
    "team_stats": (
        "/teams/statistics/aggregated/",
        "/teams/statistics/aggregated/",
        P.parse_cebl_team_stats,
        _fixture(
            FULL + "teams__statistics__aggregated__TOTALS_2025_REGULAR.json",
            kind="team_stats",
            season=2025,
            mode="TOTALS",
            competition="REGULAR",
        ),
        "/teams/statistics/aggregated/",
        "Team statistic table (totals or per game) with a league row; filter by season, competition, segment, "
        "opponent team or opponent conference.",
        {
            "extra_params": [
                _p(MODE),
                _p(SEASON_Q),
                _p(COMPETITION),
                _p(SEGMENT),
                {
                    "name": "vs_team_id",
                    "query_key": "vs_team_id",
                    "type": "int",
                    "description": "CEBL team id of the opponent: only games against that team count.",
                },
                {
                    "name": "vs_conference",
                    "query_key": "vs_conference",
                    "type": "str",
                    "choices": list(CONFERENCES),
                    "description": "EAST or WEST: only games against that conference's teams count.",
                },
            ],
            "example_args": {"mode": "TOTALS", "season": 2025, "competition": "REGULAR"},
        },
    ),
    "games": (
        "/games/{season}/",
        "/games/{season}/",
        P.parse_cebl_games,
        _fixture(FULL + "games__season__2025.json", kind="games", season=2025),
        "/games/{season}/",
        "Schedule and results of one season (regular season and finals), with the Genius match id that joins "
        "play-by-play and shots; optionally one team's games.",
        {"path_params": [_p(SEASON_PATH)], "extra_params": [_p(TEAM_ID_Q)], "example_args": {"season": 2025}},
    ),
    "player_stats": (
        "/players/statistics/aggregated/",
        "/players/statistics/aggregated/",
        P.parse_cebl_player_stats,
        _fixture(
            FULL + "players__statistics__aggregated__PER_GAME_2025.json",
            kind="player_stats",
            season=2025,
            mode="PER_GAME",
        ),
        "/players/statistics/aggregated/",
        "Player statistic table (totals or per game) for a season or all-time; filter by competition, segment or team.",
        {
            "extra_params": [_p(MODE), _p(SEASON_Q), _p(COMPETITION), _p(SEGMENT), _p(TEAM_ID_Q)],
            "example_args": {"mode": "PER_GAME", "season": 2025},
        },
    ),
    "player_leaders": (
        "/players/statistics/leaders/",
        "/players/statistics/leaders/",
        P.parse_cebl_player_leaders,
        _fixture(
            CAP + "players__statistics__leaders__PER_GAME_2025.json",
            kind="leaders",
            season=2025,
            mode="PER_GAME",
            statistics="POINTS,REBOUNDS,ASSISTS,TARGET_SCORES",
        ),
        "/players/statistics/leaders/",
        "Top five players per requested statistic (long frame, one row per statistic and leader).",
        {
            "extra_params": [_p(STATISTICS_P), _p(MODE), _p(SEASON_Q), _p(COMPETITION), _p(TEAM_ID_Q)],
            "example_args": {"statistics": "POINTS,ASSISTS", "mode": "PER_GAME", "season": 2025},
        },
    ),
    "player_seasons": (
        "/players/{player_id}/statistics/",
        "/players/{player_id}/statistics/",
        P.parse_cebl_player_seasons,
        _fixture(
            CAP + "players__player_id__statistics__570965_PER_GAME.json",
            kind="player_seasons",
            player_id=570965,
            mode="PER_GAME",
            career_only=False,
        ),
        "/players/{player_id}/statistics/",
        "One player's per-season rows and career row (totals or per game).",
        {
            "path_params": [_p(PLAYER_ID)],
            "extra_params": [
                _p(MODE),
                {
                    "name": "career_only",
                    "query_key": "career_only",
                    "type": "bool",
                    "default": False,
                    "description": "True returns the career row alone; False the per-season rows and the career row.",
                },
                _p(COMPETITION),
            ],
            "example_args": {"player_id": 570965, "mode": "PER_GAME"},
        },
    ),
    "player": (
        "/players/{player_id}/",
        "/players/{player_id}/",
        P.parse_cebl_player,
        _fixture(CAP + "players__player_id__570965.json", kind="player", player_id=570965),
        "/players/{player_id}/",
        "One player's profile and current team: name, nationality, birth date, position, jersey.",
        {"path_params": [_p(PLAYER_ID)], "example_args": {"player_id": 570965}},
    ),
    "player_gamelog": (
        "/players/{player_id}/gamelog/",
        "/players/{player_id}/gamelog/",
        P.parse_cebl_player_gamelog,
        _fixture(CAP + "players__player_id__gamelog__570965_2025.json", kind="gamelog", player_id=570965, season=2025),
        "/players/{player_id}/gamelog/",
        "One player's game log: one row per game played, with the Genius match id of the game.",
        {
            "path_params": [_p(PLAYER_ID)],
            "extra_params": [_p(SEASON_Q), _p(COMPETITION)],
            "example_args": {"player_id": 570965, "season": 2025},
        },
    ),
    "news": (
        "/news/articles/{language}/",
        "/news/articles/{language}/",
        P.parse_cebl_news,
        lambda: {"rows": [], "_request": {"kind": "news", "language": "en"}},
        "/news/articles/{language}/",
        "News articles in one language. The API answered an empty list on every probe, so this raises NoDataError.",
        {
            "path_params": [
                {
                    "name": "language",
                    "type": "str",
                    "required": True,
                    "description": "Required. Language code, e.g. ``en``; every language tried answered an empty list.",
                }
            ],
            "extra_params": [
                {
                    "name": "team_source_id",
                    "query_key": "team_source_id",
                    "type": "int",
                    "description": "CEBL team id: only that team's articles.",
                }
            ],
            "example_args": {"language": "en"},
        },
    ),
}

NOTES_TEXT = [
    "CEBL own API, owner-approved gray area: these wrappers read the league's own FastAPI service "
    "(api.data.cebl.ca, the backend of the CEBL Stats Hub) with the X-Api-Key that the hub's own page bundle ships "
    "to every browser. The key is not issued to third parties and no developer terms were found; cebl.ca's terms of "
    "use were not reviewed. The runtime reads the key from the bundle at run time, falls back to a documented "
    "constant, never prints it, and raises AssetFetchError with a 'key rotated?' hint on a persistent 401. "
    "www.cebl.ca/robots.txt disallows nothing. Payloads are not redistributed as release assets; requests are spaced "
    "at least 1 s apart.",
    "Season is the calendar year (May to August) = the sdv END year: no mapping. The first season is 2019. An unknown "
    "season, team or player answers an empty list, which the runtime raises as NoDataError. Ids are CEBL's own; a "
    "relocated franchise has two team ids (cebl_teams lists all 15).",
    "This API has no box-score, play-by-play or shot route. They live in Genius LiveStats (organisation CEBL, "
    "see the genius family): cebl_games and cebl_player_gamelog carry the Genius match id as genius_match_id, parsed "
    "from the stats URLs (derived, not fetched; the match of a cancelled game may have no data). home_team_id is "
    "Genius team 1 and away_team_id team 2 (checked on match 2600563).",
    "Some completed schedule rows have null scores (2026), and a cancelled game has status CANELLED (the API's "
    "spelling) with null or zero scores. The schedule lists unplayed games too, so no wrapper refuses an unfinished "
    "game. Aggregates are decimal strings upstream and Float64 here. Players' names, nationality and birth date are "
    "the public bio; no contact data is served.",
]

#: Descriptions of the columns the parsers add, re-key or derive (they win over the returns md).
LOCAL: Dict[str, str] = {
    "season": "Season: the calendar year it is played in (May to August) = the sdv END year (2025). Taken from the "
    "request (the body does not echo it); null for the all-time table and for a bare body.",
    "mode": "Table mode of the request: TOTALS (sums over the selected games) or PER_GAME (per-game averages); the "
    "body does not echo it.",
    "competition": "Competition filter of the request: REGULAR or FINALS; null when both were asked. The body does "
    "not echo it.",
    "segment": "Game-subset filter of the request (HOME, AWAY, L5G, MAY ...); null for all games. The body does not "
    "echo it.",
    "vs_conference": "Opponent-conference filter of the request (EAST or WEST); null when not asked. The body does "
    "not echo it.",
    "vs_team_id": "Opponent team filter of the request (CEBL team id); null when not asked. The body does not echo it.",
    "team_id": "CEBL team id (its own key space, not a Genius id). Joins cebl_teams, cebl_standings and the games' "
    "home and away team ids. A relocated franchise has a second id (15 + 56, 17 + 53, 19 + 55, 16 + 54).",
    "player_id": "CEBL player id (its own key space, global across seasons and teams; not a Genius id). Genius "
    "data.json carries no person id, so a Genius play-by-play joins by name, team and jersey number (unverified).",
    "is_league_row": "True on the one league row of a team table (null team_id and names): the league's own figures, "
    "where played counts team-games and, in TOTALS, points equals points_against.",
    "genius_match_id": "Genius LiveStats match id parsed from the stats URL (the join to the genius family, "
    "organisation CEBL, where play-by-play and shots live). Derived from the URL, not fetched; null when the URL "
    "is absent.",
    "statistic": "Statistic this leader row ranks, the body's key for the list: POINTS, REBOUNDS, ASSISTS, "
    "TARGET_SCORES and the other names the request asked for.",
    "is_career": "True on the career row (null season): the sum or average over every season and team; false on a "
    "per-season row.",
    "language": "Language code of the request ('en'); the body does not echo it.",
    "game_id": "CEBL game id (568 in 2019, 8 digits from 2025). No route is keyed by it; the Genius match id "
    "genius_match_id is the join to play-by-play.",
    "season_id": "CEBL season row id (1 = 2019 ... 8 = 2026). No route takes it: routes take the year.",
    "news_id": "CEBL news article id; null when the API gives none. Unobserved: the route answered an empty list on "
    "every probe.",
    "is_active": "True for a current team identity or the current season; false for a former identity such as the "
    "Guelph Nighthawks.",
    "start_time_utc": "Scheduled start of the game in UTC (ISO-8601 'Z' in the body).",
    "game_date": "Start of the game in UTC (ISO-8601 'Z' in the body).",
    "broadcast_partner_logo_urls": "Logo URLs of the game's broadcast partners, joined with '; ' (empty list = null).",
    "value": "The player's figure for the statistic this row ranks, in the requested mode (TOTALS sum or PER_GAME "
    "average; percentages 0-100), as Float64.",
    "ranking": "Rank in this statistic, 1 = best, with competition ranking (ties share a rank and skip the next: "
    "1, 1, 1, 4). Each list is the top five rows.",
}
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "roster": {
        "team_id": "CEBL team id of the roster asked for (from the request; the body does not echo it).",
        "season": "Season of the roster asked for (from the request; the body does not echo it).",
    },
    "player_gamelog": {
        "player_id": "CEBL player id of the log asked for (from the request; the body does not echo it).",
        "season": "Season of the game: the year of game_date (the calendar year = the sdv END year).",
        "rebounds": "Total rebounds in the game; the API sends it as a string ('5'), cast to Int64.",
        "minutes": "Minutes played as decimal minutes (26.4), not mm:ss; the API sends a string.",
    },
    "games": {
        "home_team_logo_url": "URL of the home team's logo image as the games feed lists it (a team-level asset, the same on every game of that team in the season).",
        "away_team_logo_url": "URL of the away team's logo image as the games feed lists it (a team-level asset, the same on every game of that team in the season).",
        "season": "Season of the game: the year of start_time_utc (the calendar year = the sdv END year).",
        "period": "Last period played when the API sets it (seen only on a cancelled game, which carried period 3); "
        "null on a finished game.",
    },
    "news": {
        "title": "Article headline (upstream schema; unobserved, the route answered an empty list on every probe).",
        "published_at_utc": "Publication time in UTC (upstream schema; unobserved, no article was ever served).",
        "image_url": "Article image URL (upstream schema; unobserved, no article was ever served).",
        "html_content": "Article body as HTML (upstream schema; unobserved, no article was ever served).",
    },
    "standings": {
        "win_percentage": "Win fraction, 0-1 (0.583), not a percentage; the API sends a decimal string.",
        "games_behind": "Games behind the conference leader; the API sends a decimal string ('0.0').",
    },
}

_HEADING = re.compile(r"^## `(/\S*)`")
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")


def _md_rows(path: Path) -> Dict[str, Dict[str, str]]:
    """``{route: {wire path: description}}`` from the returns md (unobserved rows skipped)."""
    out: Dict[str, Dict[str, str]] = {}
    section: Optional[str] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        h = _HEADING.match(line)
        if h:
            section = h.group(1)
            continue
        if line.startswith("## "):
            section = None
        m = _ROW.match(line)
        if m and section and not m.group(2).strip().startswith("Unobserved"):
            out.setdefault(section, {}).setdefault(m.group(1), m.group(2).strip())
    return out


def _wire_names(short: str) -> Dict[str, str]:
    """``{column: wire key}`` for the columns that read a body key directly."""
    return {c: s for c, s, _d in P.SPECS[short] if isinstance(s, str)}


def _describe(short: str, col: str, entry: Entry, md: Dict[str, Dict[str, str]]) -> str:
    if col in LOCAL_BY.get(short, {}):
        return LOCAL_BY[short][col]
    wire = _wire_names(short).get(col)
    rows = md.get(entry[4], {})
    if wire is not None:
        for prefix in ("[].", "<STATISTIC>.[].", ""):
            if prefix + wire in rows:
                return rows[prefix + wire]
    return LOCAL.get(col, "")


def _frame(entry: Entry) -> pl.DataFrame:
    """The parser's frame over the entry's fixture."""
    frame = entry[2](entry[3]())
    assert isinstance(frame, pl.DataFrame)
    return frame


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    _sp, path, parser, _fix, _route, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/cebl/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("cebl", SPEC)
    spec = load_spec(refs, SPEC)
    differs = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if differs:
        raise SystemExit(f"cebl: spec paths and ENDPOINTS differ: {sorted(differs)}")
    assert {s for s in P.SPECS} == set(ENDPOINTS), "every parser spec needs an endpoint and the reverse"
    doc = {
        "api": "cebl",
        "host": HOST,
        "name_pattern": "cebl_{short}",
        "module": "cebl",
        "parser_module": "basketball.cebl.cebl_parsers",
        "getter_module": "sportsdataverse.basketball.cebl.cebl_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": False,
        "schema_prefix": "cebl_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw record (``dict``): ``{'rows': <API body>, '_request': {...}}`` with the request "
            "(kind, season, mode, competition, segment, ids) stamped under ``_request``",
            "raises": [
                "sportsdataverse.errors.NoDataError: the API has nothing for the request (an unknown season, team "
                "or player answers an empty list, an all-null profile; an empty news feed).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for "
                "(a rejected key after a re-read, a non-2xx, a non-JSON body, an error envelope, a body for another "
                "season, team or player).",
                "ValueError: a mode, competition, segment or conference outside its set, a season that is not a "
                "4-digit year, an id that is not an integer, or HTTP 400 / 422 (a missing required query parameter).",
            ],
            "notes": NOTES_TEXT,
            "see_also": [
                {
                    "name": "CEBL Stats Hub",
                    "url": "https://cebl-stats-hub.web.app",
                    "note": "the league's stats site whose API these wrappers read",
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
    write_yaml(ROOT / "tools/codegen/endpoints/cebl.yaml", doc)

    md = _md_rows(refs / "cebl-returns.md")
    schema_dir = ROOT / "tools/codegen/schemas/native/cebl"
    rewrite_schema_dir(schema_dir)
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        cols: List[Dict[str, str]] = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        block = notes.setdefault(f"cebl_{short}", {})
        for col in cols:
            text = " ".join(_describe(short, col["name"], entry, md).split())
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"cebl_{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"cebl_{short}", "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit(f"cebl: empty or filler descriptions (add them to LOCAL / LOCAL_BY): {bad}")
    write_yaml(NOTES, notes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"cebl: {len(ENDPOINTS)} endpoints, {sum(map(len, notes.values()))} descriptions -> {NOTES.name}")


if __name__ == "__main__":
    main()
