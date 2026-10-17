"""Generate the ``tpbl`` flat-API endpoint YAML + returns-schemas (Taiwan's TPBL on api.tpbl.basketball).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_lkl.py`` (itself ``gen_asa.py`` with
the shared plumbing in ``gen_soccer_common.py``). Reads the frozen spec and ``tpbl-returns.md`` from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/tpbl.yaml`` -- 16 wrappers over the spec's 11 routes. One route can feed several frames
  (``/games/{id}`` feeds ``game`` and ``referees``; ``/games/{id}/stats`` feeds the four ``box_*`` frames; the
  standings feed ``standings`` and ``standings_against``), each with its own parser; :data:`ENDPOINTS` is the source
  of truth. The three season routes take ``season`` (END year), which ``tpbl_runtime`` maps to the API's own
  season id.
* ``tools/codegen/schemas/native/tpbl/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser run
  over its committed FULL body in ``tests/fixtures/tpbl/full/`` (stamped with ``_request`` the way ``tpbl_runtime``
  stamps a live body). Each parser emits a fixed column list with dtypes pinned by name (``tpbl_parsers.SPECS``), so
  a fixture cannot mistype a rate. The schemas carry ``description: ''`` (descriptions never live in ``schemas/**``).
* ``tools/codegen/notes/tpbl_descriptions.yaml`` -- ``{tpbl_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (Integration merges it). Text comes from :data:`LOCAL_BY` (a column whose
  meaning differs by wrapper), :data:`LOCAL` (ids and derived columns), then the ``tpbl-returns.md`` row of the same
  record path (the stat lines). An empty, short or filler text fails the run.
* ``tests/fixtures/tpbl/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_tpbl.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.tpbl import tpbl_parsers as P
from sportsdataverse.basketball.tpbl.tpbl_runtime import REQUEST_KEY
from sportsdataverse.dl_utils import underscore

HOST = "https://api.tpbl.basketball/api"
SPEC = "tpbl.openapi.yaml"
RETURNS = "tpbl-returns.md"
FIXTURES = ROOT / "tests" / "fixtures" / "tpbl"
SCHEMA_PREFIX = "tpbl_"
#: Column descriptions, keyed ``tpbl_<short>`` -> column; merged into manual_column_descriptions.yaml.
NOTES = ROOT / "tools" / "codegen" / "notes" / "tpbl_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

UNOFFICIAL = (
    "Unofficial and unsupported: these are the routes the tpbl.basketball site's own front end calls on "
    "api.tpbl.basketball (read from its JavaScript bundle). The API is keyless and needs no headers; "
    "tpbl.basketball/robots.txt answers 200 with an empty body (no rules, no crawl-delay). ``tpbl_runtime`` still "
    "spaces requests at least 1 s apart. SportsDataverse has not ruled on the site's terms, so payloads are not "
    "redistributed as release assets."
)
COVERAGE = (
    "Coverage: three seasons so far (2024-25 = season 2025, to 2026-27 = season 2027, whose regular season starts "
    "2026-10-17). There is NO play-by-play route (none of the site's 131 JavaScript chunks calls one); the two shot "
    "lists carry court coordinates only, with no shooter, game, quarter or time. Game wrappers serve FINISHED games "
    "only (an unfinished game raises AssetFetchError). Names (teams with their sponsor, venues, players, referees) "
    "are Traditional Chinese; the description of each code column translates it."
)
IDS = (
    "Ids: every route takes TPBL's own ids (season, division, team, game, player); the gohoops_* ids belong to "
    "GoHoops, the stats platform behind the league, and are echoed beside them. The box score keys its players by "
    "gohoops_roster_id (a person registered with one squad in one season), not by the TPBL player id: join "
    "tpbl_box_players to tpbl_roster on gohoops_roster_id. Season-keyed wrappers take the END year and the runtime "
    "maps it to the API's season id. Birth dates and contact fields are never returned."
)
CHECKS = (
    "Scope checks: the box-score wrappers fetch the game detail first (one extra request) and refuse a stats body "
    "whose teams, final scores, quarter count or player points disagree with it; the season game list, roster and "
    "player game log are checked against the object asked for. The two shot lists name no team, player or division "
    "in the body, so they cannot be scoped. tpbl_games(verify=True) additionally checks every division's completed "
    "games against its standings (falling back to the team-filtered lists for a division whose standings answer "
    "empty, as the 2024-25 playoffs do). tpbl_standings raises NoDataError for such a division."
)
SEASON = (
    "Required. Season END year (2026 = the 2025-26 season; 2025 is the first). The API keys a season by its own "
    "small integer (1, 2, 3 = 2024-25, 2025-26, 2026-27; the event id of /events/{id}/* is the same number): "
    "``tpbl_runtime`` reads /seasons once and sends that id, so the example URL shows the year. A year the list "
    "lacks raises NoDataError."
)
GAME = (
    "Required. TPBL game id (integer), e.g. 6; ``game_id`` in tpbl_games. Not sequential by date (2024-25 = 1-1367, "
    "2025-26 = 1371-8641, 2026-27 = 27535-27667). Only a finished game is served."
)
DIVISION = (
    "Required. TPBL division id (one competition phase of one season: 1, 2, 5, 6, 7 = 2024-25 preseason, regular "
    "season, playoffs, postseason challenge, finals; 8, 9, 11, 12, 35 = 2025-26; 655, 656 = 2026-27); "
    "``division_id`` in tpbl_divisions. Not the GoHoops division id."
)
TEAM = (
    "Required. TPBL team id (the franchise, 2-8, stable across seasons); ``team_id`` in tpbl_teams. Not the GoHoops id."
)
PLAYER = (
    "Required. TPBL player id (one per person across seasons and teams); ``player_id`` in tpbl_roster. Not the GoHoops "
    "person id and not the roster id."
)


def _full(name: str) -> Any:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _stamped(name: str, **request: Any) -> Dict[str, Any]:
    """A full body stamped with ``_request`` the way ``tpbl_runtime`` stamps a live one."""
    body = _full(name)
    out = {"data": body} if isinstance(body, list) else dict(body)
    out[REQUEST_KEY] = request
    return out


def _box() -> Dict[str, Any]:
    game = _full("game_6")
    return _stamped(
        "game_stats_6",
        game_id=6,
        home_team_id=game["home_team"]["id"],
        away_team_id=game["away_team"]["id"],
        periods=game["round"],
    )


def _season_req() -> Dict[str, Any]:
    return {"season": 2027, "season_id": 3, "gohoops_season_id": 275}


Source = Tuple[str, str]  # (returns-md section = spec path, record-path prefix)
# short -> (spec path, wrapper path, parser, fixture loader, md sources, summary, fields)
Entry = Tuple[str, str, Callable[..., Any], Callable[[], Dict[str, Any]], Tuple[Source, ...], str, Dict[str, Any]]


def _path(*names: str, **desc: str) -> Dict[str, Any]:
    texts = {"season": SEASON, "game_id": GAME, "division_id": DIVISION, "team_id": TEAM, "player_id": PLAYER, **desc}
    ex = {"season": 2026, "game_id": 6, "division_id": 2, "team_id": 4, "player_id": 75}
    return {
        "path_params": [{"name": n, "type": "int", "required": True, "description": texts[n]} for n in names],
        "example_args": {n: ex[n] for n in names},
    }


_GAME = ("/games/{game_id}", "/games/{game_id}")
_STATS = ("/games/{game_id}/stats", "/games/{game_id}/stats")
_STAND = ("/divisions/{division_id}/games/standings", "/divisions/{division_id}/games/standings")
_SEASON_GAMES = "/seasons/{season_id}/games"
ENDPOINTS: Dict[str, Entry] = {
    "seasons": (
        "/seasons",
        "/seasons",
        P.parse_tpbl_seasons,
        lambda: _stamped("seasons"),
        (("/seasons", "[]."),),
        "Every TPBL season with its TPBL and GoHoops ids, label, status and date window.",
        {"example_args": {}},
    ),
    "divisions": (
        "/events/{event_id}/divisions",
        "/events/{season}/divisions",
        P.parse_tpbl_divisions,
        lambda: _stamped("../captures/events__event_id__divisions@3", **_season_req()),
        (("/events/{event_id}/divisions", "[]."),),
        "The divisions (preseason, regular season, play-in, playoffs, finals) of one season.",
        _path("season"),
    ),
    "teams": (
        "/events/{event_id}/teams",
        "/events/{season}/teams",
        P.parse_tpbl_teams,
        lambda: _stamped("../captures/events__event_id__teams@2", **{**_season_req(), "season": 2026, "season_id": 2}),
        (("/events/{event_id}/teams", "[]."),),
        "The teams of one season (bracket placeholders flagged).",
        _path("season"),
    ),
    "games": (
        _SEASON_GAMES,
        "/seasons/{season}/games",
        P.parse_tpbl_games,
        lambda: _stamped("season_games_2027", **_season_req()),
        ((_SEASON_GAMES, "[]."),),
        "Every game of one season, all divisions, played and scheduled, with teams, venue, status and score.",
        {
            **_path("season"),
            "extra_params": [
                {
                    "name": "team_id",
                    "query_key": "team_id",
                    "type": "int",
                    "required": False,
                    "description": "TPBL team id (2-8): keep only that team's games (the API's own filter). Cannot be "
                    "combined with ``verify``.",
                },
                {
                    "name": "verify",
                    "query_key": "verify",
                    "type": "bool",
                    "required": False,
                    "description": "Default False. Cross-check the list for completeness: every division's completed games must equal "
                    "the matches of its own standings (one extra request per division; a division whose standings "
                    "are empty is checked against the team-filtered lists instead). A mismatch raises "
                    "AssetFetchError.",
                },
            ],
        },
    ),
    "standings": (
        _STAND[0],
        _STAND[1],
        P.parse_tpbl_standings,
        lambda: _stamped("standings_2", division_id=2),
        ((_STAND[0], "[]."),),
        "League table of one division: rank, record, games behind and streak.",
        _path("division_id"),
    ),
    "standings_against": (
        _STAND[0],
        _STAND[1],
        P.parse_tpbl_standings_against,
        lambda: _stamped("standings_2", division_id=2),
        ((_STAND[0], "[].against_result.[]."),),
        "Head-to-head record of every team against every other team in one division.",
        _path("division_id"),
    ),
    "game": (
        _GAME[0],
        _GAME[1],
        P.parse_tpbl_game,
        lambda: _stamped("game_6", game_id=6),
        ((_GAME[0], ""),),
        "Header of one finished game: teams with scores, venue, status, quarters, tip-off, attendance and links.",
        _path("game_id"),
    ),
    "referees": (
        _GAME[0],
        _GAME[1],
        P.parse_tpbl_referees,
        lambda: _stamped("game_6", game_id=6),
        ((_GAME[0], "referees.[]."),),
        "The officiating crew of one finished game, chief referee first (names only).",
        _path("game_id"),
    ),
    "box_players": (
        _STATS[0],
        _STATS[1],
        P.parse_tpbl_box_players,
        _box,
        ((_STATS[0], "home_team.players.total.{roster_id}."),),
        "Whole-game box score lines of every player on either team sheet, keyed by GoHoops roster id.",
        _path("game_id"),
    ),
    "box_player_periods": (
        _STATS[0],
        _STATS[1],
        P.parse_tpbl_box_player_periods,
        _box,
        ((_STATS[0], "home_team.players.rounds.{round}.{roster_id}."),),
        "Per-quarter box score lines of the players who were on court, overtimes included.",
        _path("game_id"),
    ),
    "box_teams": (
        _STATS[0],
        _STATS[1],
        P.parse_tpbl_box_teams,
        _box,
        ((_STATS[0], "home_team.teams.total."),),
        "Whole-game team totals of both teams: shooting, rebounds, fouls, assists, turnovers and paint points.",
        _path("game_id"),
    ),
    "box_team_periods": (
        _STATS[0],
        _STATS[1],
        P.parse_tpbl_box_team_periods,
        _box,
        ((_STATS[0], "home_team.teams.rounds.{round}."),),
        "Per-quarter team lines of both teams: points scored and conceded in the quarter, running totals and stats.",
        _path("game_id"),
    ),
    "roster": (
        "/divisions/{division_id}/teams/{team_id}/players",
        "/divisions/{division_id}/teams/{team_id}/players",
        P.parse_tpbl_roster,
        lambda: _stamped("roster_2_4", division_id=2, team_id=4),
        (("/divisions/{division_id}/teams/{team_id}/players", "[]."),),
        "The registered roster of one team in one division (birth dates omitted).",
        _path("division_id", "team_id"),
    ),
    "team_shots": (
        "/divisions/{division_id}/teams/{team_id}/shooting-spots",
        "/divisions/{division_id}/teams/{team_id}/shooting-spots",
        P.parse_tpbl_team_shots,
        lambda: _stamped("team_shots_2_4_r1", division_id=2, team_id=4, rounds=[1]),
        (("/divisions/{division_id}/teams/{team_id}/shooting-spots", "[]."),),
        "Every field-goal attempt of one team in one division with its court position (optionally per quarter).",
        {
            **_path("division_id", "team_id"),
            "extra_params": [
                {
                    "name": "rounds",
                    "query_key": "rounds",
                    "type": "list",
                    "required": False,
                    "description": "Quarter filter: a quarter number or a list of them (1-4, 5 and 6 for overtimes); "
                    "the runtime sends it as the API's repeatable ``rounds[]``. Omit for all quarters.",
                }
            ],
        },
    ),
    "player_shots": (
        "/divisions/{division_id}/players/{player_id}/shooting-spots",
        "/divisions/{division_id}/players/{player_id}/shooting-spots",
        P.parse_tpbl_player_shots,
        lambda: _stamped("team_shots_2_4_r1", division_id=2, player_id=75),
        (("/divisions/{division_id}/players/{player_id}/shooting-spots", "[]."),),
        "Every field-goal attempt of one player in one division with its court position.",
        _path("division_id", "player_id"),
    ),
    "player_games": (
        "/players/{player_id}/games/stats",
        "/players/{player_id}/games/stats",
        P.parse_tpbl_player_games,
        lambda: _stamped("player_games_75_div2", player_id=75, division_id=2),
        (
            ("/players/{player_id}/games/stats", "[].accumulated_stats."),
            ("/players/{player_id}/games/stats", "[].percentage_stats."),
            ("/players/{player_id}/games/stats", "[]."),
        ),
        "A player's game log in one division: one row per game played, with that game's whole stat line.",
        {
            **_path("player_id"),
            "extra_params": [
                {
                    "name": "division_id",
                    "query_key": "division_id",
                    "type": "int",
                    "required": True,
                    "description": DIVISION
                    + " Without it the API answers HTTP 422; an unknown id answers HTTP 500, which is not retried.",
                }
            ],
            "example_args": {"player_id": 75, "division_id": 2},
        },
    ),
}

# ----------------------------------------------------------------------------- descriptions
SEASON_ID = (
    "TPBL season id (key space: TPBL seasons on api.tpbl.basketball; 1 = 2024-25, 2 = 2025-26, 3 = 2026-27): the "
    "{season_id} of the season game list and, equal to it, the {event_id} of /events/{event_id}/*. Not the GoHoops "
    "season id. Null when the body was parsed without the request stamp."
)
GH_SEASON = (
    "GoHoops season id (key space: GoHoops seasons, the id system the API echoes beside its own; 101 = 2024-25, "
    "213 = 2025-26, 275 = 2026-27). Not the TPBL season id."
)
DIVISION_ID = (
    "TPBL division id (key space: TPBL divisions, one competition phase of one season; 1, 2, 5, 6, 7 = 2024-25, "
    "8, 9, 11, 12, 35 = 2025-26, 655, 656 = 2026-27): the {division_id} of every division route. Not the GoHoops "
    "division id."
)
GH_DIVISION = (
    "GoHoops division id (key space: GoHoops divisions; e.g. 64 = the 2024-25 preseason, 65 = the 2024-25 regular "
    "season). Not the TPBL division id."
)
GH_EVENT = (
    "GoHoops event id (key space: GoHoops events, one per TPBL season; 319 = 2024-25, 490 = 2025-26, 613 = "
    "2026-27). Not the TPBL season id and not the GoHoops season id."
)
TEAM_ID = (
    "TPBL team id (key space: TPBL teams = franchises, 2-8 so far; stable across seasons): the {team_id} of the "
    "division team routes. Not the GoHoops franchise id and not the squad id."
)
GH_TEAM = (
    "GoHoops franchise id (key space: GoHoops franchises, 940-946; stable across seasons; = the TPBL team id + 938)."
)
GH_SQUAD = (
    "GoHoops squad id (key space: GoHoops squads = one team in one season: 8209-8215 in 2024-25, 11931-11937 in "
    "2025-26, 15536-15542 in 2026-27; it changes every season)."
)
GAME_ID = (
    "TPBL game id (key space: TPBL games on api.tpbl.basketball): the {game_id} of the game routes. Not sequential "
    "by date (2024-25 = 1-1367, 2025-26 = 1371-8641, 2026-27 = 27535-27667). The box score echoes no game id, so a "
    "box frame parsed without the request stamp has a null game_id."
)
PLAYER_ID = (
    "TPBL player id (key space: TPBL players; one per person across seasons and teams): the {player_id} of the "
    "player routes. Not the GoHoops person id and not the roster id."
)
PERSON_ID = (
    "GoHoops person id (key space: GoHoops persons; one per human). Not the TPBL player id and not the roster id."
)
ROSTER_ID = (
    "GoHoops roster id (key space: GoHoops rosters = one player registered with one squad in one season; it changes "
    "when the player changes season or team). The key of the box score players: join tpbl_box_players to "
    "tpbl_roster on it. Not the TPBL player id and not the person id."
)
TEAM_NAME = (
    "Team name in Traditional Chinese with the sponsor (高雄全家海神 = Kaohsiung Aquas, 福爾摩沙夢想家 = Formosa "
    "Dreamers, 新竹御嵿攻城獅 = Hsinchu Lioneers, 桃園台啤永豐雲豹 = Taoyuan Leopards, 新北中信特攻 = New Taipei CTBC "
    "DEA, 新北國王 = New Taipei Kings, 臺北台新戰神 = Taipei Mars)."
)
ALT_NAME = "Lowercase English slug of the team: aquas, dreamers, lioneers, leopards, dea, kings, mars."
STATUS_PLAYER = (
    "Registration status of the player on the squad as of the request, not at game time: 'official', or 'withdrawn' "
    "/ 'WITHDRAWN' (one state in two spellings: the player left the squad later; most withdrawn players still played)."
)
SIDE = "Side of the game: home or away (the API lists the home team first and never infers it from the venue)."
PERIOD = "Quarter number of the row: 1-4 regulation, 5 = the first overtime, 6 = the second."
POSITION = (
    "Playing position as a PascalCase enum: PointGuard, ShootingGuard, SmallForward, PowerForward, Center, and the "
    "combinations PointGuardAndShootingGuard, ShootingGuardAndSmallForward, SmallForwardAndPowerForward and "
    "PowerForwardAndCenter; '' when unset."
)
SHOT_XY = (
    "Shot position in MILLIMETRES, {axis}. The hoops sit at x = 1575 (offense_basket 0) and x = 26425 "
    "(offense_basket 1), y = 7500. {note}"
)
SCORE_NOTE = "Taiwan local date and time (UTC+8 by every tip-off seen; the API sends no zone)."


#: Descriptions of the columns the parsers add, re-key or translate (they win over the returns md).
LOCAL: Dict[str, str] = {
    "season": "Season END year (2026 = the 2025-26 season), the year asked for. Null when the body was parsed "
    "without the request stamp.",
    "season_id": SEASON_ID,
    "gohoops_season_id": GH_SEASON,
    "division_id": DIVISION_ID,
    "gohoops_division_id": GH_DIVISION,
    "gohoops_event_id": GH_EVENT,
    "team_id": TEAM_ID,
    "gohoops_squad_id": GH_SQUAD,
    "game_id": GAME_ID,
    "gohoops_game_id": "GoHoops game id (key space: GoHoops games; 1242 for TPBL game 6). Not the TPBL game id.",
    "player_id": PLAYER_ID,
    "gohoops_person_id": PERSON_ID,
    "gohoops_roster_id": ROSTER_ID,
    "side": SIDE,
    "period": PERIOD,
    "team": TEAM_NAME,
    "alt_name": ALT_NAME,
    "number": "Jersey number as text (so '0' and '00' stay distinct).",
    "position": POSITION,
    "name": "Name as the API prints it (Traditional Chinese, or a transliteration for foreign players).",
}

_GAME_LOCAL: Dict[str, str] = {
    "game_id": GAME_ID,
    "code": "Game code inside the division: the game number as text in the regular season ('1' ... '42'), a series "
    "label in the playoffs ('AG1' = series A game 1, 'G7' = finals game 7), '' for preseason games.",
    "status": "Game status. Observed: COMPLETED (finished) and NOT_STARTED (scheduled). A game in progress has "
    "is_live true.",
    "is_live": "True while the game is being played; false for finished and scheduled games.",
    "periods": "Number of quarters played: 4 for a regulation game, 5 for one overtime, 6 for two; 0 before tip-off.",
    "game_date": "Game date (Taiwan local date).",
    "game_day_of_week": "Traditional Chinese weekday in parentheses: (一) Mon, (二) Tue, (三) Wed, (四) Thu, (五) Fri, "
    "(六) Sat, (日) Sun.",
    "game_time": "Scheduled tip-off 'HH:MM:SS'. " + SCORE_NOTE,
    "tipoff": "Scheduled tip-off as a naive timestamp (no zone). " + SCORE_NOTE,
    "venue": "Venue name in Traditional Chinese (新莊體育館 = Xinzhuang Gymnasium, 和平籃球館 = Heping Basketball "
    "Gymnasium, 臺中洲際迷你蛋 = Taichung Intercontinental Mini Dome, 新竹縣體育館 = Hsinchu County Gymnasium, "
    "高雄巨蛋 = Kaohsiung Arena, 桃園巨蛋 = Taoyuan Arena).",
    "attendance": "Attendance as an integer, parsed from the API's visitors string; null before the game, when the "
    "API sends none or on a postponed game.",
    "recap_url": "YouTube recap URL; null when the game has none (the list can omit it, the game detail carries it).",
    "live_stream_url": "YouTube live-stream URL of the game; null when the API omits it.",
    "judgement_report_url": "URL of the referees' judgement report (a Google Drive file); null when none.",
    "ticket_url": "URL of the ticket page; null on most rows.",
    "gohoops_season_id": GH_SEASON + " Read from the home team.",
    "gohoops_event_id": GH_EVENT + " Read from the home team.",
    **{
        f"{s}_{k}": v.replace("{side}", s.capitalize())
        for s in ("home", "away")
        for k, v in {
            "team_id": "{side} " + TEAM_ID,
            "gohoops_team_id": "{side} " + GH_TEAM,
            "gohoops_squad_id": "{side} " + GH_SQUAD,
            "team": "{side} team name. " + TEAM_NAME,
            "alt_name": "{side} team slug. " + ALT_NAME,
            "score": "{side} team final score (points scored); null before tip-off.",
        }.items()
    },
}

#: Per-wrapper descriptions where a column means something different or is derived.
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "seasons": {
        "season": "Season END year, read from the label '2024-2025 賽季' (賽季 = season): 2025 for 2024-25.",
        "season_id": SEASON_ID.replace(" Null when the body was parsed without the request stamp.", ""),
        "name": "Season label in Traditional Chinese: '2024-2025 賽季' = 'season 2024-2025'.",
        "status": "Season status: COMPLETED or IN_PROGRESS.",
        "started_at": "Season start, 'YYYY-MM-DD 00:00:00' text. Preseason games can be played before it (the "
        "2026-27 start is 2026-10-17, its first games were played 2026-10-09).",
        "ended_at": "Season end, 'YYYY-MM-DD 00:00:00' text (the planned end for a season in progress).",
    },
    "divisions": {
        "name": "Division name (Traditional Chinese: 熱身賽 = preseason (warm-up games), 例行賽 = regular season, "
        "季後挑戰賽 = postseason challenge (play-in round), 季後賽 = playoffs, 冠軍賽 = finals).",
        "status": "Division status: COMPLETED or IN_PROGRESS.",
    },
    "teams": {
        "gohoops_team_id": GH_TEAM,
        "name": TEAM_NAME,
        "is_real_team": "True for a real club; false for a bracket placeholder (67 '高順位隊伍' = higher-seed team, "
        "68 '低順位隊伍' = lower-seed team in 2025-26).",
        "status": "Team status. 'ACTIVE' is the only value seen in every sampled route; the full set of statuses is "
        "unknown.",
    },
    "games": _GAME_LOCAL,
    "game": _GAME_LOCAL,
    "referees": {
        "sequence": "Position in the crew as the API lists it: 1 = the chief referee.",
        "referee_id": "TPBL referee id (key space: TPBL referees on api.tpbl.basketball). Not the GoHoops referee id.",
        "gohoops_referee_id": "GoHoops referee id (key space: GoHoops referees). Not the TPBL referee id.",
        "name": "Referee name in Traditional Chinese. Names only: no contact or birth data is published.",
        "role": "Crew role enum: BASKETBALL_CHIEF_REFEREE = crew chief, BASKETBALL_FIRST_ASSISTANT_REFEREE = first "
        "assistant (umpire 1), BASKETBALL_SECOND_ASSISTANT_REFEREE = second assistant (umpire 2).",
    },
    "standings": {
        "rank": "Rank in the division, 1-based.",
        "gohoops_team_id": GH_TEAM,
        "won": "Games won in the division so far.",
        "lost": "Games lost in the division so far.",
        "drawn": "Games drawn (always 0 in basketball).",
        "games_behind": "Games behind the leader (0 for the leader); a whole number in every captured division, read as "
        "a float so half games cannot break the dtype.",
        "streak": "Current streak: 'W' + count for wins, 'L' + count for losses (W2 = two straight wins).",
    },
    "standings_against": {
        "team_id": TEAM_ID + " The ranked team.",
        "opponent_team_id": "TPBL team id of the opponent (franchise, 2-8). Joins team_id.",
        "opponent_gohoops_team_id": GH_TEAM + " The opponent.",
        "opponent": "Opponent name. " + TEAM_NAME,
        "won": "Games the ranked team won against this opponent in the division.",
        "lost": "Games the ranked team lost against this opponent in the division.",
        "drawn": "Games drawn against this opponent (always 0).",
    },
    "roster": {
        "team_id": TEAM_ID,
        "player": "Player name (Traditional Chinese, or a transliteration for foreign players).",
        "alt_name": "Romanized name (e.g. 'Chian Kuang-Chien'); '' when none.",
        "gohoops_event_id": GH_EVENT,
        "gohoops_season_id": GH_SEASON,
        "height_cm": "Height in centimetres.",
        "weight_kg": "Weight in kilograms (a number; fractions possible).",
        "nationality": "Nationality text: 'Local' for a Taiwan-born player, '' when unset.",
        "national_identity": "Player category: 'Local' (Taiwanese), '' when unset (foreign and naturalized categories use "
        "other strings).",
        "national_identity_detail": "Free-text detail on the category; '' in every sample.",
        "registered": "True when the player is registered with the squad.",
        "is_listing": "True when the player is shown on the team's public roster page.",
    },
    "team_shots": {
        "rounds": "The quarter filter asked for, comma-separated ('1' or '1,2'); null when all quarters were asked.",
        "is_made": "True when the shot went in.",
        "point_x": SHOT_XY.format(
            axis="0-28000 along the court length (a 28 m court)",
            note="Verified on 2,971 shots of one team: the made rate within 1.2 m of the hoop is 62-71 %. Which baseline "
            "is x = 0 is unverified.",
        ),
        "point_y": SHOT_XY.format(
            axis="0-15000 across the court (a 15 m court)", note="The direction of y is unverified."
        ),
        "offense_basket": "Basket the team attacked: 0 = the hoop at x = 1575, 1 = the hoop at x = 26425.",
    },
    "player_shots": {
        "is_made": "True when the shot went in.",
        "point_x": SHOT_XY.format(
            axis="0-28000 along the court length (a 28 m court)",
            note="Same frame as the team list, where it was verified; here it is only checked to stay on the court.",
        ),
        "point_y": SHOT_XY.format(
            axis="0-15000 across the court (a 15 m court)", note="The direction of y is unverified."
        ),
        "offense_basket": "Basket the player's team attacked: 0 = the hoop at x = 1575, 1 = the hoop at x = 26425.",
    },
    "box_players": {
        "team": TEAM_NAME,
        "player": "Player name (Traditional Chinese, or a transliteration for foreign players).",
        "status": STATUS_PLAYER,
        "roster_status": "Roster registration status as an upper-case enum: 'OFFICIAL' in the 2026-27 preseason game; "
        "null in the 2024-25 and 2025-26 box scores (an era difference in the feed).",
        "is_starting": "True when the player was in the starting five.",
        "entered": "True when the player has a stat line (he entered the game; a few such players have 0 seconds on "
        "court); false for a registered player who did not play, whose stats are all null.",
    },
    "box_player_periods": {
        "team": TEAM_NAME,
        "player": "Player name (Traditional Chinese, or a transliteration for foreign players).",
        "status": STATUS_PLAYER,
        "roster_status": "Roster registration status as an upper-case enum: 'OFFICIAL' in the 2026-27 preseason game; "
        "null in the 2024-25 and 2025-26 box scores (an era difference in the feed).",
        "is_starting": "True when the player was in the starting five of the game.",
        "entered": "Always true here: only quarters in which the player was on court are rows.",
    },
    "box_teams": {
        "team": TEAM_NAME,
        "opponent_team_id": "TPBL team id of the other side of the game (franchise, 2-8). Joins team_id.",
    },
    "box_team_periods": {"team": TEAM_NAME},
    "player_games": {
        "player": "Player name (Traditional Chinese, or a transliteration for foreign players).",
        "division_id": DIVISION_ID.replace(" Not the GoHoops", " The division asked for. Not the GoHoops"),
        "gohoops_roster_id": ROSTER_ID.replace(
            "The key of the box score players: join", "His roster in the division: join"
        ),
        "team_id": TEAM_ID + " The team the player played for in that game.",
        "team": "Name of the team the player played for in that game. " + TEAM_NAME,
        "game_title": "Game title: the division name and the game number counted from the end ('例行賽 Game 124' = "
        "regular season, game 124).",
        "home_team_id": "TPBL team id of the home team of the game (franchise, 2-8).",
        "away_team_id": "TPBL team id of the away team of the game (franchise, 2-8).",
        "home_score": "Home team final score in the game.",
        "away_score": "Away team final score in the game.",
        "venue": "Venue name in Traditional Chinese (the API's game location).",
        "tipoff": "Scheduled tip-off as a naive timestamp (no zone). " + SCORE_NOTE,
        "periods_played": "Number of quarters in which the player was on court in this game.",
        "pir": "Performance index rating: efficiency minus fouls (the API's performance_index_rating; checked on 189 "
        "games of the box-score lines).",
        "efg": "Effective field-goal percentage, 0-100 (the API's effective_field_goals_percentage, a one-decimal "
        "string).",
        "tsp": "True-shooting percentage, 0-100 (the API's true_shooting_percentage, a one-decimal string).",
        "turnovers_percentage": "Turnover percentage, 0-100 (a one-decimal string on the API; the exact formula is "
        "not documented).",
    },
}
for _k in ("box_players", "box_player_periods"):
    LOCAL_BY[_k].update(
        {
            "game_id": GAME_ID,
            "side": SIDE,
        }
    )
LOCAL_BY["box_player_periods"]["period"] = PERIOD
LOCAL_BY["box_team_periods"]["period"] = PERIOD

_HEADING = re.compile(r"^## `https://[^/]+/api(/\S*)`")
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")


def _md_rows(path: Path) -> Dict[str, Dict[str, str]]:
    """``{section path: {record path: description}}`` from the returns md (unobserved rows skipped)."""
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
            out.setdefault(section, {}).setdefault(m.group(1), m.group(2).strip().replace("&lt;", "<"))
    return out


def _describe(short: str, col: str, entry: Entry, md: Dict[str, Dict[str, str]]) -> str:
    if col in LOCAL_BY.get(short, {}):
        return LOCAL_BY[short][col]
    if col in LOCAL:
        return LOCAL[col]
    for section, prefix in entry[4]:
        for path, text in md.get(section, {}).items():
            leaf = path[len(prefix) :]
            if path.startswith(prefix) and "[" not in leaf and underscore(leaf.replace(".", "_")) == col:
                return text
    return ""


def _frame(entry: Entry) -> pl.DataFrame:
    """The parser's frame over the entry's fixture."""
    frame = entry[2](entry[3]())
    assert isinstance(frame, pl.DataFrame)
    return frame


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    _sp, path, parser, _fix, _src, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/tpbl/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("tpbl", SPEC)
    spec = load_spec(refs, SPEC)
    differs = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if differs:
        raise SystemExit(f"tpbl: spec paths and ENDPOINTS differ: {sorted(differs)}")
    assert set(P.SPECS) == set(ENDPOINTS), "every parser spec needs an endpoint and the reverse"
    doc = {
        "api": "tpbl",
        "host": HOST,
        "name_pattern": "tpbl_{short}",
        "module": "tpbl",
        "parser_module": "basketball.tpbl.tpbl_parsers",
        "getter_module": "sportsdataverse.basketball.tpbl.tpbl_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (game, roster, standings) name other families
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw body (``dict``): the API's JSON object, or a list body wrapped as ``{'data': [...]}``, "
            "plus ``_request`` (the season, game, division, team or player asked for and the ids the runtime resolved)",
            "raises": [
                "sportsdataverse.errors.NoDataError: the API has nothing for the request (an unknown id, a season it "
                "does not list, an empty list such as the standings of a playoff division).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for (an "
                "unfinished game, a box score or list that disagrees with the game it was asked for, an error "
                "envelope, a non-JSON body, a server error).",
            ],
            "notes": [UNOFFICIAL, COVERAGE, IDS, CHECKS],
            "see_also": [
                {
                    "name": "TPBL",
                    "url": "https://tpbl.basketball",
                    "note": "the Taiwan Professional Basketball League site whose API these wrappers read",
                },
                {
                    "name": "wehoop",
                    "url": "https://wehoop.sportsdataverse.org",
                    "note": "SportsDataverse women's basketball (R)",
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
    write_yaml(ROOT / "tools/codegen/endpoints/tpbl.yaml", doc)

    md = _md_rows(refs / RETURNS)
    schema_dir = ROOT / "tools/codegen/schemas/native/tpbl"
    rewrite_schema_dir(schema_dir)
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        cols: List[Dict[str, str]] = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        block = notes.setdefault(f"{SCHEMA_PREFIX}{short}", {})
        for col in cols:
            text = _describe(short, col["name"], entry, md).strip()
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"{SCHEMA_PREFIX}{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(
            schema_dir / f"{short}.yaml", {"schema": f"{SCHEMA_PREFIX}{short}", "kind": "dataframe", "columns": cols}
        )
    if bad:
        raise SystemExit(f"tpbl: empty or filler descriptions (add them to LOCAL / LOCAL_BY): {bad}")
    write_yaml(NOTES, notes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"tpbl: {len(ENDPOINTS)} endpoints, {sum(map(len, notes.values()))} descriptions -> {NOTES.name}")


if __name__ == "__main__":
    main()
