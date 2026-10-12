"""Generate the ``liiga`` flat-API endpoint YAML + returns-schemas (Liiga API v2, ``liiga.fi/api/v2``).

Idempotent: same spec + same captures -> byte-identical output. Modeled on ``gen_asa.py``, with
the shared plumbing in ``gen_soccer_common.py``. Reads the frozen spec and the captures from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/liiga.yaml`` -- 23 wrappers over the spec's 14 paths. One route can
  feed several frames (``/games/{season}/{id}``: the game row, goals, penalties, shootout and
  lineups), each with its own parser; the table :data:`ENDPOINTS` is the source of truth.
* ``tools/codegen/schemas/native/liiga/<short>.yaml`` -- one returns-schema per wrapper: the
  columns of its parser run over its committed fixture, stamped the way the runtime stamps a
  live body. The box score and the games list use the FULL bodies in
  ``tests/fixtures/liiga/full/`` (the truncated captures cannot show that a rate arrives as an
  integer when whole); every other wrapper uses its refs capture. Descriptions come from the
  spec, then the leaf of each ``liiga-returns.md`` row, then :data:`LOCAL` (the columns the
  parsers derive).

Run: ``python tools/codegen/gen_liiga.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from gen_soccer_common import (
    ROOT,
    columns_from_frame,
    load_spec,
    refs_dir,
    rewrite_schema_dir,
    spec_descriptions,
    write_yaml,
)

from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.liiga import liiga_parsers as P
from sportsdataverse.hockey.liiga.liiga_runtime import TOURNAMENTS, _stamp

HOST = "https://liiga.fi/api/v2"
SPEC = "liiga.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "liiga"

TEAM_DATA_TYPES = ["shots", "passes", "faceoffs", "penalties", "powerplay", "penalty_kill", "even_strength"]
PLAYER_DATA_TYPES = [
    "basicStats",
    "basicStatsGk",
    "advancedStats",
    "goalStats",
    "shotStats",
    "skatingStats",
    "faceOffStats",
    "penaltyStats",
    "powerplayPenaltykillStats",
    "passes",
    "winningShotComp",
    "winningShotCompGk",
    "gameTime",
]

_T = (
    "runkosarja (regular season), playoffs, playout, qualifications, valmistavat_ottelut (preseason) "
    "or chl (Champions Hockey League)"
)
SEASON_Q = "Season END year (2026 = 2025-26). Omitted = the season in progress."
SEASON_P = "Season END year (2026 = 2025-26). Game ids repeat across seasons, so a game is (season, game_id)."
GAME_ID = "Game id (game_id in liiga_schedule or liiga_games), e.g. 1; unique only within a season."
PLAYER_ID = "Player id (player_id in liiga_players_info or a lineup; equals fihaId), e.g. 21870028."


def _q(name: str, wire: str, desc: str, *, typ: str = "str", required: bool = False, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": desc}
    if required:
        out["required"] = True
    out.update(extra)
    return out


def _p(name: str, desc: str, *, typ: str = "int", **extra: Any) -> Dict[str, Any]:
    return {"name": name, "type": typ, "required": True, "description": desc, **extra}


def _season_q(desc: str = SEASON_Q) -> Dict[str, Any]:
    return _q("season", "season", desc, typ="int")


def _tournament_q(omitted: str, default: Optional[str] = None) -> Dict[str, Any]:
    # A rendered ``choices`` check rejects None, so an optional tournament without a default is
    # checked by the runtime (liiga_runtime.TOURNAMENTS) instead.
    extra: Dict[str, Any] = {} if default is None else {"choices": list(TOURNAMENTS), "default": default}
    return _q("tournament", "tournament", f"Tournament: {_T}. {omitted}", **extra)


_GAME_PATH = [_p("season", SEASON_P), _p("game_id", GAME_ID)]
_GAME_EX = {"season": 2026, "game_id": 1}
_RANGE = [
    _q("season_from", "seasonFrom", "First season (END year) of the range.", typ="int"),
    _q("season_to", "seasonTo", "Last season (END year) of the range.", typ="int"),
]

# short -> (spec path, wrapper path, parser, fixture, stamp (url, params) or None, summary, endpoint fields)
Entry = Tuple[str, str, Callable[..., Any], str, Optional[Tuple[str, Optional[dict]]], str, Dict[str, Any]]
ENDPOINTS: Dict[str, Entry] = {
    "tournament": (
        "/tournament",
        "/tournament",
        P.parse_liiga,
        "tournament.json",
        None,
        "Current state of the league: game week of the season in progress and its last game week.",
        {},
    ),
    "games": (
        "/games",
        "/games",
        P.parse_liiga_games,
        "full/games__playoffs__2026__3games.json",
        None,
        "Every game of a season and tournament: teams, score, xG, power plays, periods and rink.",
        {
            "extra_params": [_season_q(), _tournament_q("Omitted = every tournament, preseason included.")],
            "example_args": {"season": 2026, "tournament": "runkosarja"},
        },
    ),
    "goals": (
        "/games",
        "/games",
        P.parse_liiga_goals,
        "full/games__playoffs__2026__3games.json",
        None,
        "Every goal of a season and tournament: scorer, assists, strength codes and skaters on ice.",
        {
            "extra_params": [_season_q(), _tournament_q("Omitted = every tournament, preseason included.")],
            "example_args": {"season": 2026, "tournament": "runkosarja"},
        },
    ),
    "schedule": (
        "/schedule",
        "/schedule",
        P.parse_liiga_schedule,
        "schedule.json",
        None,
        "Schedule of a season and tournament: game ids, teams, goals, xG, start and rink.",
        {
            "extra_params": [_season_q(), _tournament_q("Omitted = runkosarja.")],
            "example_args": {"season": 2026, "tournament": "runkosarja"},
        },
    ),
    "gameweeks": (
        "/gameweeks",
        "/gameweeks",
        P.parse_liiga,
        "gameweeks.json",
        None,
        "Number of game weeks of a season and tournament.",
        {
            "extra_params": [_season_q(), _tournament_q("Required by the API; defaults to runkosarja.", "runkosarja")],
            "example_args": {"season": 2026},
        },
    ),
    "game": (
        "/games/{season}/{id}",
        "/games/{season}/{game_id}",
        P.parse_liiga_game,
        "games__season__id.json",
        ("/games/2026/1", None),
        "One game's header: teams, score, xG, power plays, periods, rink, referees and attendance.",
        {"path_params": _GAME_PATH, "example_args": _GAME_EX},
    ),
    "game_goals": (
        "/games/{season}/{id}",
        "/games/{season}/{game_id}",
        P.parse_liiga_game_goals,
        "games__season__id.json",
        ("/games/2026/1", None),
        "One game's goals: scorer, assists, strength codes and skaters on ice.",
        {"path_params": _GAME_PATH, "example_args": _GAME_EX},
    ),
    "game_penalties": (
        "/games/{season}/{id}",
        "/games/{season}/{game_id}",
        P.parse_liiga_game_penalties,
        "games__season__id.json",
        ("/games/2026/1", None),
        "One game's penalties, both teams: player, fault, minutes, start and end.",
        {"path_params": _GAME_PATH, "example_args": _GAME_EX},
    ),
    "game_shootout": (
        "/games/{season}/{id}",
        "/games/{season}/{game_id}",
        P.parse_liiga_game_shootout,
        "games__season__id.json",
        ("/games/2026/1", None),
        "One game's shootout attempts: shooter, goalkeeper and result (none for a game without one).",
        {"path_params": _GAME_PATH, "example_args": _GAME_EX},
    ),
    "game_lineups": (
        "/games/{season}/{id}",
        "/games/{season}/{game_id}",
        P.parse_liiga_game_lineups,
        "games__season__id.json",
        ("/games/2026/1", None),
        "One game's lineups, both teams: line, role, jersey, captaincy and bio.",
        {"path_params": _GAME_PATH, "example_args": _GAME_EX},
    ),
    "game_players_stats": (
        "/games/{season}/{id}",
        "/games/{season}/{game_id}",
        P.parse_liiga_game_players_stats,
        "games__season__id__playersWithStats.json",
        ("/games/2026/1", {"dataType": "playersWithStats"}),
        "One game's lineups with each player's all-time, season, season-in-team and versus-team stats.",
        {"path_params": _GAME_PATH, "fixed_params": {"dataType": "playersWithStats"}, "example_args": _GAME_EX},
    ),
    "game_stats": (
        "/games/stats/{season}/{id}",
        "/games/stats/{season}/{game_id}",
        P.parse_liiga_game_stats,
        "full/games__stats__2026__1.json",
        ("/games/stats/2026/1", None),
        "One game's per-period player box score, skaters and goalies, with tracking where Liiga has it.",
        {"path_params": _GAME_PATH, "example_args": _GAME_EX},
    ),
    "game_team_stats": (
        "/games/stats/{season}/{id}",
        "/games/stats/{season}/{game_id}",
        P.parse_liiga_game_team_stats,
        "full/games__stats__2026__1.json",
        ("/games/stats/2026/1", None),
        "One game's per-period team stats with puck possession.",
        {"path_params": _GAME_PATH, "example_args": _GAME_EX},
    ),
    "shotmap": (
        "/shotmap/{season}/{id}",
        "/shotmap/{season}/{game_id}",
        P.parse_liiga_shotmap,
        "shotmap__season__id.json",
        ("/shotmap/2026/1", None),
        "One game's shots with rink-image coordinates (pixels), shooter, blocker and skaters on ice; from 2016.",
        {"path_params": _GAME_PATH, "example_args": _GAME_EX},
    ),
    "standings": (
        "/standings/",
        "/standings/",
        P.parse_liiga_standings,
        "standings.json",
        ("/standings/", {"season": 2026}),
        "Standings of a season: regular season, playoffs, preseason, playout and qualifications tables.",
        {"extra_params": [_season_q()], "example_args": {"season": 2026}},
    ),
    "teams_history": (
        "/teams/info",
        "/teams/info",
        P.parse_liiga,
        "teams__info.json",
        None,
        "Every team's regular-season record, one row per team and season since 1976.",
        {"fixed_params": {"team": "all"}, "example_args": {}},
    ),
    "teams": (
        "/teams/info",
        "/teams/info",
        P.parse_liiga_teams,
        "teams__info__all.json",
        None,
        "Team directory: every current and past team with logos, venue capacity and club info.",
        {"example_args": {}},
    ),
    "team_stats": (
        "/teams/stats",
        "/teams/stats",
        P.parse_liiga,
        "teams__stats__shots.json",
        None,
        "Team stat table of a season range, by data_type.",
        {
            "extra_params": [
                _q(
                    "data_type",
                    "dataType",
                    "Stat table: " + ", ".join(TEAM_DATA_TYPES) + ".",
                    required=True,
                    choices=TEAM_DATA_TYPES,
                ),
                *_RANGE,
                _tournament_q("Required by the API; defaults to runkosarja.", "runkosarja"),
            ],
            "example_args": {"data_type": "shots", "season_from": 2026, "season_to": 2026},
        },
    ),
    "players_info": (
        "/players/info",
        "/players/info",
        P.parse_liiga_players_info,
        "players__info.json",
        None,
        "Roster and bio of every player in a season range.",
        {
            "extra_params": [
                _q("season_from", "fromSeason", "First season (END year) of the range.", typ="int"),
                _q("season_to", "toSeason", "Last season (END year) of the range.", typ="int"),
                _tournament_q("Required by the API; defaults to runkosarja.", "runkosarja"),
            ],
            "example_args": {"season_from": 2026, "season_to": 2026},
        },
    ),
    "player": (
        "/players/info/{playerId}",
        "/players/info/{player_id}",
        P.parse_liiga_player,
        "players__info__playerId.json",
        None,
        "One player's bio, teams by season and active seasons.",
        {"path_params": [_p("player_id", PLAYER_ID)], "example_args": {"player_id": 21870028}},
    ),
    "player_career": (
        "/players/info/{playerId}",
        "/players/info/{player_id}",
        P.parse_liiga_player_career,
        "players__info__playerId.json",
        None,
        "One player's career: season totals by tournament.",
        {"path_params": [_p("player_id", PLAYER_ID)], "example_args": {"player_id": 21870028}},
    ),
    "player_game_log": (
        "/players/info/{playerId}/games/{season}",
        "/players/info/{player_id}/games/{season}",
        P.parse_liiga_player_game_log,
        "players__info__playerId__games__season.json",
        ("/players/info/21870028/games/2026", None),
        "One player's game log for a season, every tournament.",
        {
            "path_params": [_p("player_id", PLAYER_ID), _p("season", "Season END year (2026 = 2025-26).")],
            "example_args": {"player_id": 21870028, "season": 2026},
        },
    ),
    "player_stats": (
        "/players/stats/summed/{from}/{to}/{tournament}/{bool}",
        "/players/stats/summed/{season_from}/{season_to}/{tournament}/true",
        P.parse_liiga,
        "players__stats__summed__from__to__tournament__bool__basicStats.json",
        None,
        "Player stat table summed over a season range, by data_type (the whole league, unfiltered).",
        {
            "path_params": [
                _p("season_from", "First season (END year) of the range, e.g. 2026."),
                _p("season_to", "Last season (END year) of the range, e.g. 2026."),
                _p("tournament", f"Tournament: {_T}.", typ="str", choices=list(TOURNAMENTS)),
            ],
            "extra_params": [
                _q(
                    "data_type",
                    "dataType",
                    "Stat table: " + ", ".join(PLAYER_DATA_TYPES) + ".",
                    default="basicStats",
                    choices=PLAYER_DATA_TYPES,
                ),
            ],
            "example_args": {"season_from": 2026, "season_to": 2026, "tournament": "runkosarja"},
        },
    ),
}

#: Descriptions of the columns the parsers derive or rename (not in the spec or returns md).
LOCAL: Dict[str, str] = {
    "season": "Season END year (2026 = 2025-26).",
    "game_id": "Game id, unique only within a season: key a game as (season, game_id).",
    "player_id": "Player id; the same number in lineups, rosters, stat tables and the profile (fihaId).",
    "team_id": "Team id, the number of Liiga's `<num>:<slug>` team key (sent as that string, a bare integer "
    "or a digit string depending on the route).",
    "team_slug": "Team slug, the part of Liiga's `<num>:<slug>` team key after the colon (e.g. hpk).",
    "home_team_id": "Home team id (the number of the `<num>:<slug>` key).",
    "home_team_slug": "Home team slug (e.g. hpk).",
    "away_team_id": "Away team id (the number of the `<num>:<slug>` key).",
    "away_team_slug": "Away team slug (e.g. tps).",
    "shooting_team_id": "Shooting team id.",
    "shooting_team_slug": "Shooting team slug.",
    "blocking_team_id": "Goalkeeper's team id.",
    "blocking_team_slug": "Goalkeeper's team slug.",
    "left_team_id": "Team whose shots are drawn at the right end of the rink image (x grows toward it).",
    "right_team_id": "Team whose shots are drawn at the left end of the rink image.",
    "serie_id": "Tournament id, the same every season: 1188 regular season, 1189 playoffs, 1194 preseason, 1890 CHL.",
    "side": "home or away.",
    "tournament": "Tournament.",
    "is_goalie": "Row is a goalkeeper's (goalie columns filled).",
    "jersey": "Jersey number.",
    "scorer_player_id": "Scorer's player id.",
    "scorer_first_name": "Scorer's first name.",
    "scorer_last_name": "Scorer's last name.",
    "assist1_player_id": "First assist's player id.",
    "assist1_first_name": "First assist's first name.",
    "assist1_last_name": "First assist's last name.",
    "assist1_season_assists": "First assister's season assist count after this goal.",
    "assist2_player_id": "Second assist's player id.",
    "assist2_first_name": "Second assist's first name.",
    "assist2_last_name": "Second assist's last name.",
    "assist2_season_assists": "Second assister's season assist count after this goal.",
    "goal_types": "Goal type codes, comma-separated Finnish abbreviations: YV power play, AV short-handed, "
    "TM empty net, RL penalty shot (others undocumented).",
    "plus_jerseys": "Jersey NUMBERS of this team's skaters on ice for the goal, space-separated.",
    "minus_jerseys": "Jersey NUMBERS of the other team's skaters on ice for the goal, space-separated.",
    "puck_control_duration": "Seconds this team controlled the puck in the period.",
    "opponent_puck_control_duration": "Seconds the other team controlled the puck in the period.",
    "contested_control_duration": "Seconds the puck was contested in the period.",
    "top_speed": "Player's top skating speed in the game (unit not stated; values near 9 fit m/s).",
    "hardest_shot": "Player's hardest shot in the game (unit not stated; values near 23 fit m/s).",
    "internal_id": "Team id (same number as team_id).",
    "placeholder": "Placeholder label for an undecided playoff team, else null.",
    "end": "Time the game ended, ISO 8601 UTC (absent from the refs captures; seen in the full 2026 playoff body).",
}

#: Column prefixes the parsers add, and the label each puts before the leaf's description.
_PREFIXES = (
    ("season_stats_in_team_", "Season stats with this team: "),
    ("all_time_stats_", "All-time stats: "),
    ("season_stats_", "Season stats: "),
    ("vs_team_stats_", "Stats versus this opponent: "),
    ("ice_rink_", "Rink: "),
    ("home_", "Home team: "),
    ("away_", "Away team: "),
    ("country_", "Country: "),
    ("birth_locality_", "Birth place: "),
    ("nationality_", "Nationality: "),
    ("team_logos_", "Team logo: "),
    ("logos_", "Logo: "),
)
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")


def _md_leaves(path: Path) -> Dict[str, str]:
    """``{underscored leaf: description}`` from the returns md rows (first definition wins)."""
    out: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        m = _ROW.match(line)
        if not m:
            continue
        leaf = m.group(1).split(".")[-1]
        if leaf in ("[]", "<key>") or not leaf:
            continue
        out.setdefault(underscore(leaf), m.group(2).strip())
    return out


def _describe(name: str, sources: List[Dict[str, str]]) -> str:
    for src in sources:
        if src.get(name):
            return src[name]
    for prefix, label in _PREFIXES:
        if name.startswith(prefix) and len(name) > len(prefix):
            inner = _describe(name[len(prefix) :], sources)
            if inner:
                return label + inner[:1].lower() + inner[1:]
    return ""


def _frame(entry: Entry) -> Any:
    _spec_path, _path, parser, fixture, stamp, _summary, _fields = entry
    body = json.loads((FIXTURES / fixture).read_text(encoding="utf-8"))
    if stamp is not None:
        url, params = stamp
        body = _stamp(HOST + url, params, body)
    return parser(body)


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    spec_path, path, parser, _fixture, _stamp_, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/liiga/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("liiga", SPEC)
    spec = load_spec(refs, SPEC)
    missing = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if missing:
        raise SystemExit(f"liiga: spec paths and ENDPOINTS differ: {sorted(missing)}")
    doc = {
        "api": "liiga",
        "host": HOST,
        "name_pattern": "liiga_{short}",
        "module": "liiga",
        "parser_module": "hockey.liiga.liiga_parsers",
        "getter_module": "sportsdataverse.hockey.liiga.liiga_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: Liiga has nothing for the request (an unknown game, "
                "a tournament not played that season, a shot map before 2016).",
            ],
            "see_also": [
                {"name": "Liiga", "url": "https://liiga.fi/", "note": "the site whose API v2 these wrappers read"},
                {
                    "name": "fastRhockey",
                    "url": "https://fastRhockey.sportsdataverse.org",
                    "note": "SportsDataverse hockey package (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/liiga.yaml", doc)

    sources = [LOCAL, spec_descriptions(spec), _md_leaves(refs / "liiga-returns.md")]
    schema_dir = ROOT / "tools/codegen/schemas/native/liiga"
    rewrite_schema_dir(schema_dir)
    for short, entry in ENDPOINTS.items():
        cols = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        for col in cols:
            col["description"] = _describe(col["name"], sources)
        write_yaml(schema_dir / f"{short}.yaml", {"schema": short, "kind": "dataframe", "columns": cols})
    print(f"liiga: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
