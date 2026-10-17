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
  integer when whole); every other wrapper uses its refs capture. Schema key ``liiga_<short>``
  (``schema_prefix``); every ``description`` is ``''``.
* ``tools/codegen/liiga_column_descriptions.snippet.yaml`` -- the column descriptions, keyed
  ``liiga_<short>`` -> column, for ``manual_column_descriptions.yaml``. Each comes from
  :data:`ROUTE` (route-specific), :data:`LOCAL` (parser-built columns), else the wrapper's own
  ``liiga-returns.md`` row (:data:`SECTIONS`; :data:`MD_REWRITE` replaces a row that only
  restates its field). A filler description stops the run.
* ``sportsdataverse/hockey/liiga/liiga_dtypes.py`` -- each parser's column dtypes (the
  returns-schema types), so an empty or all-null frame is typed like a populated one.
* ``tests/fixtures/liiga/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs).

Run: ``python tools/codegen/gen_liiga.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import (
    ROOT,
    load_spec,
    refs_dir,
    rewrite_schema_dir,
    write_yaml,
)

from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.liiga import liiga_parsers as P
from sportsdataverse.hockey.liiga.liiga_runtime import TOURNAMENTS, _stamp

HOST = "https://liiga.fi/api/v2"
SPEC = "liiga.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "liiga"
SCHEMA_PREFIX = "liiga_"
#: Column descriptions, keyed ``liiga_<short>`` -> column; merged into manual_column_descriptions.yaml.
SNIPPET = ROOT / "tools" / "codegen" / "liiga_column_descriptions.snippet.yaml"
#: The spec's route list, committed so the tests check ENDPOINTS without the refs checkout.
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: Each parser's column dtypes, from the returns-schemas, in a package-safe module.
DTYPES_MODULE = ROOT / "sportsdataverse" / "hockey" / "liiga" / "liiga_dtypes.py"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

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
    "shooting_team_id": "Team id of the shooter's team (the number of the `<num>:<slug>` key).",
    "shooting_team_slug": "Slug of the shooter's team (e.g. hpk).",
    "blocking_team_id": "Goalkeeper's team id.",
    "blocking_team_slug": "Goalkeeper's team slug.",
    "left_team_id": "Team whose shots are drawn at the right end of the rink image (x grows toward it).",
    "right_team_id": "Team whose shots are drawn at the left end of the rink image.",
    "serie_id": "Tournament id, the same every season: 1188 regular season, 1189 playoffs, 1194 preseason, 1890 CHL.",
    "side": "Which team of the game the row belongs to: home or away.",
    "is_goalie": "Row is a goalkeeper's (goalie columns filled).",
    "jersey": "Jersey (sweater) number the player wore.",
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

#: Team-block columns of a game row (``parsers._side``): ``home_<x>`` is the wire ``homeTeam.<x>``
#: (``homeTeam.teamName`` -> ``home_name``), so the md key is ``home_team_<x>`` or ``home_team_team_<x>``.
_SIDE_ALIASES = (("home_", "Home team: "), ("away_", "Away team: "))
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")
_HEAD = re.compile(r"^##\s+(.*?)\s*$")

#: The ``liiga-returns.md`` section (its ``##`` heading, backticks dropped) each wrapper reads, and the
#: wire paths of its rows (underscored, ``[]`` / ``<key>`` dropped) that one frame row flattens. A column
#: ``c`` takes the description of the md row ``<root><c>`` for the first root that has one, so a
#: qualifier such as "(player, all-time)" never reaches another route's or another object's column of
#: the same leaf name. Home and away rows document the same fields; the home path stands for both.
SECTIONS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "tournament": ("/tournament", ("",)),
    "games": ("/games", ("",)),
    "goals": ("/games", ("home_team_goal_events_",)),
    "schedule": ("/schedule", ("",)),
    "gameweeks": ("/gameweeks", ("",)),
    "game": ("/games/{season}/{id}", ("game_",)),
    "game_goals": ("/games/{season}/{id}", ("game_home_team_goal_events_",)),
    "game_penalties": ("/games/{season}/{id}", ("game_home_team_penalty_events_",)),
    "game_shootout": ("/games/{season}/{id}", ("game_winning_shot_competition_events_",)),
    "game_lineups": ("/games/{season}/{id}", ("home_team_players_",)),
    "game_players_stats": (
        "/games/{season}/{id} — dataType=playersWithStats",
        ("home_team_players_player_dto_", "home_team_players_"),
    ),
    "game_stats": (
        "/games/stats/{season}/{id}",
        (
            "home_team_period_player_stats_period_",
            "home_team_period_player_stats_",
            "home_team_game_player_stats_",
            "home_team_goalie_period_stats_period_",
            "home_team_goalie_period_stats_",
        ),
    ),
    "game_team_stats": ("/games/stats/{season}/{id}", ("home_team_",)),
    "shotmap": ("/shotmap/{season}/{id}", ("",)),
    "standings": ("/standings/", ("season_", "playoffs_", "valmistavat_ottelut_")),
    "teams_history": ("/teams/info", ("",)),
    "teams": ("/teams/info — without query params", ("teams_",)),
    "team_stats": ("/teams/stats — dataType=shots", ("team_stats_",)),
    "players_info": ("/players/info", ("",)),
    "player": ("/players/info/{playerId}", ("",)),
    "player_career": (
        "/players/info/{playerId}",
        ("historical_regular_", "historical_playoffs_", "historical_practice_"),
    ),
    "player_game_log": ("/players/info/{playerId}/games/{season}", ("regular_", "playoffs_", "practice_")),
    "player_stats": ("/players/stats/summed/{from}/{to}/{tournament}/{bool} — dataType=basicStats", ("",)),
}
#: Team-block columns of a game row (``parsers._side``): ``home_<x>`` is the wire ``homeTeam.<x>``
#: (``homeTeam.teamName`` -> ``home_name``), so its md row is ``<root>home_team_<x>`` or ``..._team_team_<x>``.
_SIDE_ALIASES = (("home_", "Home team: "), ("away_", "Away team: "))


def _md_sections(path: Path) -> Dict[str, Dict[str, str]]:
    """``{section heading: {underscored wire path: description}}`` from the returns md."""
    out: Dict[str, Dict[str, str]] = {}
    current: Optional[str] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        h = _HEAD.match(line)
        if h:
            current = h.group(1).replace("`", "")
            out.setdefault(current, {})
            continue
        m = _ROW.match(line)
        if m and current is not None:
            segs = [underscore(s) for s in m.group(1).split(".") if s not in ("[]", "<key>", "")]
            if segs:
                out[current].setdefault("_".join(segs), m.group(2).strip())
    return out


def _md(rows: Dict[str, str], roots: Tuple[str, ...], keys: Tuple[str, ...]) -> str:
    """The first md row ``<root><key>``, with a context-free :data:`MD_REWRITE` applied."""
    text = next((rows[r + k] for k in keys for r in roots if rows.get(r + k)), "")
    return MD_REWRITE.get(text, text)


def _describe(short: str, name: str, md: Dict[str, Dict[str, str]]) -> str:
    """A column's description: :data:`ROUTE`, then :data:`LOCAL`, then its route's md row.

    A game row's ``home_<x>`` / ``away_<x>`` (:data:`_SIDE_ALIASES`) takes the description of
    ``@<x>`` in :data:`ROUTE`, else of the md row ``<root>home_team_<x>``, behind a side label.
    """
    section, roots = SECTIONS[short]
    route = {**ROUTE.get(section, {}), **ROUTE.get(short, {})}
    rows = md.get(section, {})
    hit = route.get(name) or LOCAL.get(name) or _md(rows, roots, (name,))
    if hit:
        return hit
    for prefix, label in _SIDE_ALIASES:
        if name.startswith(prefix):
            rest = name[len(prefix) :]
            keys = (f"{prefix}team_{rest}", f"{prefix}team_team_{rest}")
            hit = route.get(f"@{rest}") or _md(rows, roots, keys)
            if hit:
                return label + hit[:1].lower() + hit[1:]
    return ""


#: Returns-md texts that only restate their field, rewritten. Keyed by the md text itself, so a
#: rewrite only applies where the md had no route context to lose.
MD_REWRITE: Dict[str, str] = {
    "Alternate captain.": "True when the player is an alternate captain (A).",
    "Attendance.": "Attendance (spectators) reported for the game.",
    "Captain.": "True when the player is the team captain (C).",
    "City.": "City where the rink is located.",
    "Contact info.": "Club contact details as HTML, in Finnish (company, address, phone).",
    "Current venue capacity.": "Capacity of the team's current home arena (spectators).",
    "Dark bg.": "Logo image URL for dark backgrounds.",
    "Dark bg original.": "Original-file logo URL for dark backgrounds; null in every capture.",
    "Externals.": "JSON list of the club's external links (url, label), e.g. its ticket shops.",
    "Extra description.": "Extra profile text for the player; null in every capture (meaning unverified).",
    "Extra rookie picture.": "Flag for an extra rookie picture on the player's profile (meaning unverified).",
    "Extra strengths.": "Extra profile field for the player's strengths; null in every capture (unverified).",
    "First name.": "Player's first name.",
    "General info.": "Club history and facts as an HTML table, in Finnish (founding year, titles, ...).",
    "Goalkeeper.": "True when the player is a goalkeeper.",
    "Height, cm.": "Player's height in centimetres.",
    "Injured.": "True when the player is listed as injured.",
    "Is u 2 #0.": "True when Liiga flags the player as under 20 (U20).",
    "Jersey number.": "Jersey (sweater) number the player wears.",
    "Last name.": "Player's last name.",
    "Latitude.": "Rink latitude, decimal degrees.",
    "League name.": "Name of the league of that career row, e.g. Liiga.",
    "Light bg.": "Logo image URL for light backgrounds.",
    "Light bg original.": "Original-file logo URL for light backgrounds; null in every capture.",
    "Locality.": "Club's home locality; null in every capture (unverified).",
    "Logo.": "Team logo image URL (old.liiga.fi media).",
    "Longitude.": "Rink longitude, decimal degrees.",
    "Penalty info.": "Extra penalty code ('' or a letter such as K; meaning unverified).",
    "Period number.": "Period number: 1-3 are regulation, 4 and up overtime (or the shootout).",
    "Periods.": "JSON list of the game's periods: index, home and away goals, category, start and end second.",
    "Photo URL.": "URL of the player's photo.",
    "Place of birth.": "Player's place of birth (town or city).",
    "Rink id.": "Liiga's id of the rink (ice arena).",
    "Rink name.": "Name of the rink (ice arena).",
    "Rookie.": "True when Liiga flags the player as a rookie.",
    "Sponsors.": "Player's sponsors; null in every capture (unverified).",
    "Street address.": "Street address of the rink.",
    "Suspended.": "True when the player is listed as suspended.",
    "Team display name.": "Team's display name, e.g. HPK or Tappara.",
    "Url.": "Club website URL.",
    "Video clip playlist url.": "URL of the goal's video clip playlist; null in every capture.",
    "Video clip url.": "URL of the goal's video clip; null in every capture.",
    "Video thumbnail url.": "URL of the goal video's thumbnail image; null in every capture.",
    "Weight, kg.": "Player's weight in kilograms.",
    "Zip.": "Postal code of the rink.",
    "plus minus minus.": "Plus/minus: on-ice goals for minus on-ice goals against.",
}

_GAME_SIDE = {  # a game row's home_<x> / away_<x>, keyed @<x> (never a column name; see _describe)
    "@name": "display name of the team, e.g. HPK.",
    "@goals": "goals scored by the team in the game.",
    "@powerplay_goals": "power-play goals the team scored in the game.",
    "@powerplay_instances": "power plays the team had in the game.",
    "@short_handed_goals": "short-handed goals the team scored in the game.",
    "@short_handed_instances": "times the team played short-handed in the game.",
    "@expected_goals": "expected goals (xG) for the team in the game, from Liiga's undocumented model.",
}
_SPANS = (
    ("all_time_stats_", "all-time"),
    ("season_stats_", "this season"),
    ("season_stats_in_team_", "this season with this team"),
    ("vs_team_stats_", "against this game's opponent; span unverified"),
)
_VS = {
    "assists": "Assists",
    "games": "Games played",
    "goals": "Goals",
    "penalty_minutes": "Penalty minutes",
    "points": "Points",
    "shots": "Shots",
}
_FACEOFF_ZONES = {"center": "at centre ice", "defence": "in the defensive zone", "offence": "in the offensive zone"}
_NULL26 = "null for every player in the 2026 captures"

#: Route-specific descriptions (keyed by wrapper short or md section) that replace a returns-md
#: row which only restates the column name, or name a parser-built column of that route.
ROUTE: Dict[str, Dict[str, str]] = {
    "/games": {
        **_GAME_SIDE,
        "buy_tickets_url": "Ticket link for the game, else null.",
        "play_off_pair": "Playoff pairing number, else null.",
        "play_off_phase": "Playoff round, else null.",
        "play_off_req_wins": "Wins needed to take the playoff series, else null.",
    },
    "/games/{season}/{id}": _GAME_SIDE,
    "/schedule": {
        "home_team_goals": "Goals scored by the home team in the game.",
        "away_team_goals": "Goals scored by the away team in the game.",
        "home_team_name": "Display name of the home team, e.g. HPK.",
        "away_team_name": "Display name of the away team, e.g. TPS.",
        "home_team_placeholder": "Placeholder label for an undecided playoff home team, else null.",
        "away_team_placeholder": "Placeholder label for an undecided playoff away team, else null.",
        "home_team_ranking": "Home team's table position; null in every capture.",
        "away_team_ranking": "Away team's table position; null in every capture.",
        "home_team_winner": "True when the home team won the game.",
    },
    "game_penalties": {"penalty_minutes": "Length of the penalty in minutes (2, 5, 10, 20, ...)."},
    "game_players_stats": {
        **{
            f"{prefix}{leaf}": f"{text} ({span})."
            for prefix, span in _SPANS
            for leaf, text in (
                ("plus", "On ice for goals for, the + of plus/minus"),
                ("minus", "On ice for goals against, the - of plus/minus"),
                ("plus_minus", "Plus/minus, on-ice goals for minus against"),
            )
        },
        **{
            f"vs_team_stats_{leaf}": f"{text} by the player against this game's opponent (span unverified)."
            for leaf, text in _VS.items()
        },
        "vs_team_stats_shot_percentage": "Shot percentage against this game's opponent, 0-100 (span unverified).",
    },
    "game_stats": {
        "faceoffs_total": f"Faceoffs the player took in the period; {_NULL26}.",
        "faceoffs_won": f"Faceoffs the player won in the period; {_NULL26}.",
        **{
            f"faceoffs_{zone}_{what}": f"Faceoffs the player {verb} {where} in the period; {_NULL26}."
            for zone, where in _FACEOFF_ZONES.items()
            for what, verb in (("total", "took"), ("won", "won"))
        },
    },
    "game_team_stats": {
        "power_play_instances": "JSON list of Liiga's codes for the team's power plays in the period, one per "
        "power play (code meaning undocumented).",
        "short_handed_instances": "JSON list of Liiga's codes for the team's short-handed spells in the period, "
        "one per spell (code meaning undocumented).",
    },
    "standings": {
        "tournament": "Table the row belongs to, in the request vocabulary: runkosarja, playoffs, "
        "valmistavat_ottelut, playout or qualifications.",
    },
    "teams": {
        "name": "Club name as Liiga lists it, e.g. HIFK or K-Espoo.",
        "country_code": "Club's country, 3-letter code (e.g. FIN).",
        "country_name": "Club's country name, e.g. Finland.",
    },
    "player": {
        "birth_locality_name": "Player's birth place (town or city).",
        "birth_locality_country_code": "Player's birth country, 3-letter code (e.g. FIN).",
        "birth_locality_country_name": "Player's birth country name, in Finnish (e.g. Suomi).",
        "nationality_code": "Player's nationality, 3-letter country code (e.g. FIN).",
        "nationality_name": "Player's nationality as a country name, in Finnish (e.g. Suomi).",
    },
    "player_career": {
        "tournament": "Tournament of the career row, uppercase: RUNKOSARJA (regular season), PLAYOFFS, "
        "PRACTICE (preseason), ...",
        "goals_against_average": "Goals against per game (goalkeeper), that season and tournament; sent as a string.",
        "shut_out": 'Shutouts (goalkeeper), that season and tournament; sent as a string (e.g. "0").',
    },
    "player_game_log": {
        "home_team_name": "Display name of the home team of the game, e.g. HPK.",
        "away_team_name": "Display name of the away team of the game, e.g. Lukko.",
        "won": "1 when the player's team won the game, else 0.",
        "lost": "1 when the player's team lost the game, else 0.",
        "tied": "1 when Liiga counts the game as a tie for the player's team, else 0 (overtime handling unverified).",
        "shut_out": "1 for a goalkeeper's shutout in this game, else 0.",
        "tournament": "Tournament of the game (the body's list key): regular, playoffs, practice, "
        "qualifications, playout, chl or pitsiturnaus.",
    },
    "player_stats": {
        "tournament": "Tournament of the summed row as Liiga labels it, e.g. regular.",
        "last_season": "Last season (END year) of the range in which the player appears.",
        "contest_won": "Contests (puck battles; definition unverified) the player won, summed over the season range.",
        "contest_lost": "Contests (puck battles; definition unverified) the player lost, summed over the season range.",
        "contest_won_percentage": "Share of contests won, 0-100, over the season range (definition unverified).",
        "golden_helmets": "Golden helmets (Liiga's team points-leader helmet; reading unverified), summed over the "
        "season range.",
        "alumni_points": "Alumni points (meaning undocumented by Liiga), summed over the season range.",
        "expected_goals": "Expected goals (xG) of the player's shots summed over the season range, from Liiga's "
        "undocumented model.",
    },
}


def _filler(name: str, desc: str) -> bool:
    """True for a description the manual-descriptions test rejects, or one that only restates the name."""
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


def _frame(entry: Entry) -> Any:
    _spec_path, _path, parser, fixture, stamp, _summary, _fields = entry
    body = json.loads((FIXTURES / fixture).read_text(encoding="utf-8"))
    if stamp is not None:
        url, params = stamp
        body = _stamp(HOST + url, params, body)
    return parser(body)


_CODE = {pl.Int64: "i", pl.Float64: "f", pl.Boolean: "b", pl.String: "s"}
_TYPE = {"i": "integer", "f": "numeric", "b": "logical", "s": "character"}


def _twin(name: str) -> str:
    """The other side's column (``home_x`` <-> ``away_x``, ``home_team_x`` <-> ``away_team_x``)."""
    for a, b in (("home_", "away_"), ("away_", "home_")):
        if name.startswith(a):
            return b + name[len(a) :]
    return name


def _dtype_codes(frames: Dict[str, pl.DataFrame]) -> Dict[str, Dict[str, str]]:
    """``{short: {column: dtype code}}`` in frame order.

    A column with a value takes its own dtype. One null in every fixture row takes the dtype the same
    name (or its other-side twin) has where the family sends a value (``home_ranking`` is null in
    the one-game capture but an integer in /games), else the dtype the parser gave it.
    """
    seen: Dict[str, set] = {}
    for df in frames.values():
        for name in df.columns:
            if df[name].null_count() < df.height:
                seen.setdefault(name, set()).add(df.schema[name])
    out: Dict[str, Dict[str, str]] = {}
    for short, df in frames.items():
        out[short] = {}
        for name, dtype in df.schema.items():
            twin = seen.get(name) or seen.get(_twin(name), set())  # away_time_out: as home_time_out
            if df[name].null_count() == df.height and len(twin) == 1:
                dtype = next(iter(twin))
            if dtype not in _CODE:
                raise SystemExit(f"liiga: {short}.{name} has dtype {dtype}, not one of {list(_CODE)}")
            out[short][name] = _CODE[dtype]
    return out


def _write_dtypes(codes: Dict[str, Dict[str, str]]) -> None:
    """Write :data:`DTYPES_MODULE`: each parser's column dtypes (a shared parser gets the union)."""
    by_parser: Dict[str, Dict[str, str]] = {}
    for short, cols in codes.items():
        merged = by_parser.setdefault(ENDPOINTS[short][2].__name__, {})
        for name, code in cols.items():
            if merged.setdefault(name, code) != code:
                raise SystemExit(f"liiga: {ENDPOINTS[short][2].__name__}.{name} has two dtypes")
    lines = [
        '"""Column dtypes of the Liiga parsers, one map per parser function.',
        "",
        "Generated by ``tools/codegen/gen_liiga.py`` from the returns-schemas",
        "(``tools/codegen/schemas/native/liiga``); do not edit. Codes: ``i`` Int64, ``f`` Float64,",
        "``b`` Boolean, ``s`` String. ``liiga_parsers`` gives these dtypes to an empty frame, a",
        "gap-filled column and an all-null column, so an empty frame stacks onto a populated one.",
        '"""',
        "",
        "from typing import Dict",
        "",
        "DTYPES: Dict[str, Dict[str, str]] = {",
    ]
    for parser in sorted(by_parser):
        lines.append(f'    "{parser}": {{')
        lines += [f'        "{name}": "{code}",' for name, code in sorted(by_parser[parser].items())]
        lines.append("    },")
    lines.append("}")
    DTYPES_MODULE.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


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
        # manual_column_descriptions.yaml keys: bare shorts (game, standings, goals) name other families
        "schema_prefix": SCHEMA_PREFIX,
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

    md = _md_sections(refs / "liiga-returns.md")
    unknown = {sec for sec, _ in SECTIONS.values()} - set(md)
    if unknown or set(SECTIONS) != set(ENDPOINTS):
        raise SystemExit(f"liiga: SECTIONS out of step with the returns md / ENDPOINTS: {sorted(unknown)}")
    schema_dir = ROOT / "tools/codegen/schemas/native/liiga"
    rewrite_schema_dir(schema_dir)
    P.DTYPES = {}  # parse by the name rules alone, so the last run's liiga_dtypes.py cannot feed back
    codes = _dtype_codes({short: _frame(entry) for short, entry in ENDPOINTS.items()})
    snippet: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short in ENDPOINTS:
        key = f"{SCHEMA_PREFIX}{short}"
        snippet[key] = {}
        cols = []
        for name, code in codes[short].items():
            desc = _describe(short, name, md)
            if _filler(name, desc):
                bad.append(f"{key}.{name}: {desc!r}")
            snippet[key][name] = desc
            # descriptions live in manual_column_descriptions.yaml (via the snippet), never here
            cols.append({"name": name, "type": _TYPE[code], "description": ""})
        write_yaml(schema_dir / f"{short}.yaml", {"schema": key, "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit("liiga: filler or missing descriptions:\n" + "\n".join(bad))
    write_yaml(SNIPPET, snippet)
    _write_dtypes(codes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"liiga: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
