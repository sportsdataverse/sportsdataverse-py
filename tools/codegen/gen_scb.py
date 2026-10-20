"""Generate the ``scb`` flat-API endpoint YAML + returns-schemas (BBL and DBBL, the SCB API on two hosts).

Idempotent: same spec + same captures -> byte-identical output. Modeled on ``gen_liiga.py``, with the shared
plumbing in ``gen_soccer_common.py``. Reads the frozen spec and ``scb-returns.md`` from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/scb.yaml`` -- 23 wrappers over the spec's 17 routes. One route can feed several
  frames (``/games/{gameId}/stats``: players, teams, actions), each with its own parser; :data:`ENDPOINTS` is the
  source of truth. The generated wrappers take ``league`` (``bbl`` / ``dbbl``), which the runtime turns into the
  host, and the END-year ``season``, which the runtime's ``season_id`` transform maps to the API's START year.
* ``tools/codegen/schemas/native/scb/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser
  run over its committed fixture, stamped the way the runtime stamps a live body. The box score, the games page
  and the season stat tables use the FULL bodies in ``tests/fixtures/scb/full/`` (the 3-row refs captures mistype
  rates as integers); the rest use the refs captures copied to ``tests/fixtures/scb/{bbl,dbbl}/``. Schema key
  ``scb_<short>`` (``schema_prefix``); every ``description`` is ``''``.
* ``tools/codegen/notes/scb_descriptions.yaml`` -- the column descriptions, keyed ``scb_<short>`` -> column, for
  ``manual_column_descriptions.yaml`` (Integration merges it). Each comes from :data:`LOCAL`, :data:`COMMON` or the
  wrapper's own ``scb-returns.md`` section (:data:`CONTEXT`, :data:`ALIAS`). A filler or missing description stops
  the run.
* ``sportsdataverse/basketball/scb/scb_dtypes.py`` -- each parser's column dtypes (the returns-schema types), so an
  empty or all-null frame is typed like a populated one.
* ``tests/fixtures/scb/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_scb.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.scb import scb_parsers as P
from sportsdataverse.basketball.scb.scb_runtime import HOSTS, _stamp
from sportsdataverse.dl_utils import underscore

HOST = HOSTS["bbl"]
SPEC = "scb.openapi.yaml"
RETURNS = "scb-returns.md"
FIXTURES = ROOT / "tests" / "fixtures" / "scb"
SCHEMA_PREFIX = "scb_"
#: Column descriptions, keyed ``scb_<short>`` -> column; merged into manual_column_descriptions.yaml.
NOTES = ROOT / "tools" / "codegen" / "notes" / "scb_descriptions.yaml"
#: The spec's route list, committed so the tests check ENDPOINTS without the refs checkout.
SPEC_PATHS = FIXTURES / "spec_paths.json"
DTYPES_MODULE = ROOT / "sportsdataverse" / "basketball" / "scb" / "scb_dtypes.py"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

LEAGUE_Q = {
    "name": "league",
    "query_key": "league",
    "type": "str",
    "default": "bbl",
    "choices": ["bbl", "dbbl"],
    "description": (
        "League: `bbl` (men, easyCredit Basketball Bundesliga, api.basketball-bundesliga.de) or `dbbl` "
        "(women, Toyota Damen-Basketball-Bundesliga, api-1.dbbl.scb.world). The two share one API; game ids "
        "overlap, so a game is (league, game_id)."
    ),
}
SEASON_P = "Season END year (2026 = 2025-26, the sdv key). Sent to the API as its START-year seasonId (season - 1)."
SEASON_Q = SEASON_P.replace("Season END year", "Season END year", 1) + " Omitted = the season in progress."
GAME_ID = "Game id (`game_id` in `scb_games`), e.g. 2006920 (BBL) or 10130 (DBBL). One sequence per league."
DISPLAY_TYPES = ["MAIN_ROUND", "PLAYOFFS", "PLAYOFFS_TOTAL", "MAIN_ROUND_AND_PLAY_OFFS", "CUP"]
DISPLAY_Q = {
    "name": "display_type",
    "query_key": "displayType",
    "type": "str",
    "default": "MAIN_ROUND",
    "choices": DISPLAY_TYPES,
    "description": "Which games the table covers: MAIN_ROUND (regular season), PLAYOFFS, PLAYOFFS_TOTAL, MAIN_ROUND_AND_PLAY_OFFS or CUP.",
}
COMPETITIONS = ["BBL", "BBL_CUP", "ALLSTAR", "CHAMPIONS_CUP", "DBBL", "DBBL_CUP"]


def _season_p() -> Dict[str, Any]:
    return {"name": "season", "type": "int", "required": True, "transform": "season_id", "description": SEASON_P}


def _season_q(omitted: str = " Omitted = the season in progress.") -> Dict[str, Any]:
    return {
        "name": "season",
        "query_key": "seasonId",
        "type": "int",
        "transform": "season_id",
        "description": SEASON_P + omitted,
    }


def _page_q() -> Dict[str, Any]:
    return {
        "name": "page_size",
        "query_key": "pageSize",
        "type": "int",
        "default": 500,
        "description": "Rows per page. The wrapper reads page 1 and refuses a result that needs more pages, so keep it large enough for the whole result (500 covers a season).",
    }


# short -> (spec path, wrapper path, parser, fixture, request stamp, summary, endpoint fields, league param?)
Entry = Tuple[str, str, Callable[..., Any], str, Dict[str, Any], str, Dict[str, Any], bool]
_BBL = {"league": "bbl"}
_DBBL = {"league": "dbbl"}
ENDPOINTS: Dict[str, Entry] = {
    "seasons": (
        "/seasons",
        "/seasons",
        P.parse_scb_seasons,
        "bbl/seasons.json",
        _BBL,
        "Seasons, newest first, with the active flag (BBL back to 1966, DBBL 2024 on).",
        {"example_args": {}},
        True,
    ),
    "season_players": (
        "/seasons/{seasonId}",
        "/seasons/{season}",
        P.parse_scb_season_players,
        "bbl/seasons__seasonId__2026.json",
        {**_BBL, "season": 2027},
        "Every player registered in a season: the id crosswalk (season_player_id, real_player_id, season_team_id).",
        {"path_params": [_season_p()], "example_args": {"season": 2027}},
        True,
    ),
    "season_teams": (
        "/seasons/{seasonId}",
        "/seasons/{season}",
        P.parse_scb_season_teams,
        "bbl/seasons__seasonId__2026.json",
        {**_BBL, "season": 2027},
        "Every team of a season: club id, season-team id, name and three-letter code.",
        {"path_params": [_season_p()], "example_args": {"season": 2027}},
        True,
    ),
    "playoff_series": (
        "/seasons/{seasonId}/playoff-tree",
        "/seasons/{season}/playoff-tree",
        P.parse_scb_playoff_series,
        "bbl/seasons__seasonId__playoff-tree__2025.json",
        {**_BBL, "season": 2026},
        "Playoff bracket of a season: one row per series (quarter-finals, semi-finals, finals).",
        {"path_params": [_season_p()], "example_args": {"season": 2026}},
        True,
    ),
    "playoff_games": (
        "/seasons/{seasonId}/playoff-tree",
        "/seasons/{season}/playoff-tree",
        P.parse_scb_playoff_games,
        "bbl/seasons__seasonId__playoff-tree__2025.json",
        {**_BBL, "season": 2026},
        "Every playoff game of a season with its series and the points of each series team.",
        {"path_params": [_season_p()], "example_args": {"season": 2026}},
        True,
    ),
    "player_statistics": (
        "/seasons/{seasonId}/player-statistics",
        "/seasons/{season}/player-statistics",
        P.parse_scb_player_statistics,
        "full/dbbl_player_statistics_2025.json",
        {**_DBBL, "season": 2026, "display_type": "MAIN_ROUND"},
        "Season player stat table: totals and per-game values, one row per player and stat block.",
        {"path_params": [_season_p()], "example_args": {"season": 2026, "display_type": "MAIN_ROUND"}},
        True,
    ),
    "team_statistics": (
        "/seasons/{seasonId}/team-statistics",
        "/seasons/{season}/team-statistics",
        P.parse_scb_team_statistics,
        "full/dbbl_team_statistics_2025.json",
        {**_DBBL, "season": 2026, "display_type": "MAIN_ROUND"},
        "Season team stat table: totals and per-game values, one row per team and stat block.",
        {"path_params": [_season_p()], "example_args": {"season": 2026, "display_type": "MAIN_ROUND"}},
        True,
    ),
    "games": (
        "/games",
        "/games",
        P.parse_scb_games,
        "full/dbbl_games_2024_first40.json",
        _DBBL,
        "Games of a season (the whole season in one page): teams, score by period, officials, venue and top scorer.",
        {"fixed_params": {"currentPage": 1}, "example_args": {"season": 2026}},
        True,
    ),
    "games_recent": (
        "/games/recent",
        "/games/recent",
        P.parse_scb_games_recent,
        "bbl/games__recent.json",
        _BBL,
        "Recent, live and upcoming games, one row per game with its past / present / future bucket.",
        {"example_args": {}},
        True,
    ),
    "game": (
        "/games/{gameId}",
        "/games/{game_id}",
        P.parse_scb_games,
        "bbl/games__gameId__2026.json",
        _BBL,
        "One game header: teams, score by period, officials, venue and top scorer.",
        {
            "path_params": [{"name": "game_id", "type": "str", "required": True, "description": GAME_ID}],
            "example_args": {"game_id": "2006920"},
        },
        True,
    ),
    "game_players": (
        "/games/{gameId}/stats",
        "/games/{game_id}/stats",
        P.parse_scb_game_players,
        "full/bbl_game_stats_2006920.json",
        _BBL,
        "Box score of a finished game: one row per player line of both teams (2002-03 on).",
        {
            "path_params": [{"name": "game_id", "type": "str", "required": True, "description": GAME_ID}],
            "example_args": {"game_id": "2006920"},
        },
        True,
    ),
    "game_teams": (
        "/games/{gameId}/stats",
        "/games/{game_id}/stats",
        P.parse_scb_game_teams,
        "full/bbl_game_stats_2006920.json",
        _BBL,
        "Box score of a finished game: one row per team, with the head coach and the team line.",
        {
            "path_params": [{"name": "game_id", "type": "str", "required": True, "description": GAME_ID}],
            "example_args": {"game_id": "2006920"},
        },
        True,
    ),
    "game_actions": (
        "/games/{gameId}/stats",
        "/games/{game_id}/stats",
        P.parse_scb_game_actions,
        "full/bbl_game_stats_2006920.json",
        _BBL,
        "Play-by-play of a finished game with shot x/y (BBL 2008-09 on, DBBL 2024-25 on).",
        {
            "path_params": [{"name": "game_id", "type": "str", "required": True, "description": GAME_ID}],
            "example_args": {"game_id": "2006920"},
        },
        True,
    ),
    "standings": (
        "/standings",
        "/standings",
        P.parse_scb_standings,
        "dbbl/standings__2024.json",
        {**_DBBL, "season": 2025},
        "League table of a season: record, home / away split, streaks and tie-break detail (2005-06 on).",
        {"example_args": {"season": 2026}},
        True,
    ),
    "season_player": (
        "/season-players/{seasonPlayerId}",
        "/season-players/{season_player_id}",
        P.parse_scb_season_player,
        "bbl/season-players__seasonPlayerId.json",
        _BBL,
        "Player profile by season-player id: bio, position, club and previous clubs.",
        {
            "path_params": [
                {
                    "name": "season_player_id",
                    "type": "str",
                    "required": True,
                    "description": "Season-player id (`season_player_id` in a box score, an action or `scb_season_players`): one player's engagement with one club.",
                }
            ],
            "example_args": {"season_player_id": "56414"},
        },
        True,
    ),
    "team": (
        "/teams/{teamId}",
        "/teams/{team_id}",
        P.parse_scb_team,
        "bbl/teams__teamId.json",
        _BBL,
        "Club profile: names, code, home page, ticket shop, year joined and honours.",
        {
            "path_params": [
                {
                    "name": "team_id",
                    "type": "str",
                    "required": True,
                    "description": "Club id (`team_id` in `scb_season_teams`), e.g. 413 (ALBA BERLIN).",
                }
            ],
            "example_args": {"team_id": "413"},
        },
        True,
    ),
    "team_seasons": (
        "/teams/{teamId}",
        "/teams/{team_id}",
        P.parse_scb_team_seasons,
        "bbl/teams__teamId.json",
        _BBL,
        "Every season a club played: its name that season and its championship and cup titles.",
        {
            "path_params": [
                {
                    "name": "team_id",
                    "type": "str",
                    "required": True,
                    "description": "Club id (`team_id` in `scb_season_teams`), e.g. 413 (ALBA BERLIN).",
                }
            ],
            "example_args": {"team_id": "413"},
        },
        True,
    ),
    "referees": (
        "/referees",
        "/referees",
        P.parse_scb_referees,
        "bbl/referees.json",
        _BBL,
        "Current referees and commissioners (BBL only).",
        {"fixed_params": {"currentPage": 1}, "example_args": {}},
        False,
    ),
    "transfers": (
        "/transfers",
        "/transfers",
        P.parse_scb_transfers,
        "bbl/transfers.json",
        _BBL,
        "Latest transfers, one row per move (BBL only).",
        {"example_args": {}},
        False,
    ),
    "transfers_teams": (
        "/transfers/teams",
        "/transfers/teams",
        P.parse_scb_transfers_teams,
        "bbl/transfers__teams.json",
        _BBL,
        "Per club, the players joining and leaving, one row per move with its direction (BBL only).",
        {"example_args": {}},
        False,
    ),
    "team_rosters": (
        "/transfers/teams",
        "/transfers/teams",
        P.parse_scb_team_rosters,
        "bbl/transfers__teams.json",
        _BBL,
        "Per club, the current roster, one row per player (BBL only).",
        {"example_args": {}},
        False,
    ),
    "records_per_game": (
        "/records/per-game/{seasonId}",
        "/records/per-game/{season}",
        P.parse_scb_records_per_game,
        "bbl/records__per-game__seasonId__2025.json",
        {**_BBL, "season": 2026},
        "Single-game records: the season's and the all-time ones, one row per record.",
        {"path_params": [_season_p()], "example_args": {"season": 2026}},
        True,
    ),
    "records_season_averages": (
        "/records/season-averages",
        "/records/season-averages",
        P.parse_scb_records_season_averages,
        "bbl/records__season-averages.json",
        _BBL,
        "All-time season-average records (points, rebounds, assists, ...), one row per record.",
        {"example_args": {}},
        True,
    ),
}

# --- query params per wrapper (beyond league / path params) ------------------------------------
QUERY: Dict[str, List[Dict[str, Any]]] = {
    "player_statistics": [DISPLAY_Q],
    "team_statistics": [DISPLAY_Q],
    "games": [
        _season_q(" Omitted = the season in progress."),
        {
            "name": "competition",
            "query_key": "competition",
            "type": "str",
            "description": "Competition filter: BBL, BBL_CUP (Pokal), ALLSTAR, CHAMPIONS_CUP on the BBL host; DBBL, DBBL_CUP on the DBBL host. Omitted = all. The other host's competition answers an empty page.",
        },
        {
            "name": "game_type",
            "query_key": "gameType",
            "type": "str",
            "choices": ["finished", "scheduled", "all"],
            "description": "Finished, scheduled or all games. Omitted = all.",
        },
        {
            "name": "team_id",
            "query_key": "seasonTeamId",
            "type": "str",
            "description": "Only the games of this team, home or guest (the club id equals the season-team id).",
        },
        _page_q(),
    ],
    "games_recent": [
        {
            "name": "slot_size",
            "query_key": "slotSize",
            "type": "int",
            "description": "Games per bucket (past, present, future); the API's default is 9.",
        }
    ],
    "standings": [
        _season_q(" Omitted = the season in progress. Seasons before about 2000 answer an empty table."),
        {
            "name": "competition",
            "query_key": "competition",
            "type": "str",
            "description": "Competition: BBL, BBL_CUP, ALLSTAR, CHAMPIONS_CUP (BBL host) or DBBL, DBBL_CUP (DBBL host). BBL_CUP answers an empty table.",
        },
    ],
    "referees": [_page_q()],
}
for _short in ("referees",):
    QUERY[_short][0] = {
        **_page_q(),
        "default": 100,
        "description": "Rows per page. 100 returns all current officials in one page; a result that needs more pages is refused.",
    }

#: Where each wrapper's columns are described in scb-returns.md: a path substring a source row must contain.
CONTEXT: Dict[str, Dict[str, str]] = {
    "game_players": {"": "playerStats"},
    "game_teams": {"": "gameStat", "team_line_": "coachStat"},
    "game_actions": {"": "actions"},
    "player_statistics": {"": "[]"},
    "team_statistics": {"": "[]"},
    "season_players": {"": "players"},
    "season_teams": {"": "teams"},
    "playoff_series": {"": ""},
    "playoff_games": {"": "games"},
    "team_seasons": {"": "seasonTeams"},
    "team_rosters": {"": "roster"},
    "transfers_teams": {"": "joining"},
    "records_per_game": {"": "seasonRecords"},
}
#: Output column -> the source remainder path it comes from, where the names differ.
ALIAS: Dict[str, Dict[str, str]] = {
    "game_players": {
        "season_player_id": "seasonPlayer.id",
        "real_player_id": "seasonPlayer.realPlayerId",
        "first_name": "seasonPlayer.firstName",
        "last_name": "seasonPlayer.lastName",
        "shirt_number": "seasonPlayer.shirtNumber",
        "position": "seasonPlayer.position",
        "season_team_id": "seasonTeam.seasonTeamId",
        "team_name": "seasonTeam.name",
        "team_tlc": "seasonTeam.tlc",
        "season_id": "seasonTeam.seasonId",
    },
    "player_statistics": {
        "season_player_id": "seasonPlayer.id",
        "real_player_id": "seasonPlayer.realPlayerId",
        "first_name": "seasonPlayer.firstName",
        "last_name": "seasonPlayer.lastName",
        "shirt_number": "seasonPlayer.shirtNumber",
        "position": "seasonPlayer.position",
        "team_id": "seasonTeam.teamId",
        "season_team_id": "seasonTeam.seasonTeamId",
        "team_name": "seasonTeam.name",
        "team_tlc": "seasonTeam.tlc",
    },
    "team_statistics": {
        "team_id": "seasonTeam.teamId",
        "season_team_id": "seasonTeam.seasonTeamId",
        "team_name": "seasonTeam.name",
        "team_tlc": "seasonTeam.tlc",
        "season_id": "seasonTeam.seasonId",
    },
    "game_teams": {
        "season_team_id": "seasonTeam.seasonTeamId",
        "team_name": "seasonTeam.name",
        "team_tlc": "seasonTeam.tlc",
        "season_id": "seasonTeam.seasonId",
    },
}

COMMON: Dict[str, str] = {
    "league": "League the row belongs to: `bbl` (men) or `dbbl` (women); BBL and DBBL game ids overlap, so a game is (league, game_id).",
    "season": "Season END year (2026 = 2025-26), the sdv key; the API's `season_id` plus one.",
    "season_id": "The API's own season key, the START year (2025 = 2025-26); `season` minus one.",
    "game_id": "Game id, a string; unique within a league only (BBL and DBBL overlap), and not chronological.",
    "season_team_id": "Season-team id, a string; equal to the club `team_id` in every sample (BBL 2005-2026, DBBL 2024-2026).",
    "team_id": "Club id, a string; equal to `season_team_id` in every sample. Joins to `scb_team` and `scb_season_teams`.",
    "season_player_id": "Season-player id: one player's engagement with one club (kept across seasons while he stays, new after a move). Joins box-score lines, actions, records and `scb_season_player`.",
    "real_player_id": "Global person id (integer): the same player across clubs and seasons. Join players across seasons on this.",
    "first_name": "Player's given name.",
    "last_name": "Player's family name.",
    "team_name": "Team name as listed that season (sponsor names included).",
    "team_tlc": "Three-letter club code.",
    "tlc": "Three-letter club code.",
    "side": "`home` or `guest` (the away side), for the team the row belongs to.",
    "shirt_number": "Jersey number as sent (a string: it can be `00`).",
    "display_type": "Which games the table covers, echoed from the request (MAIN_ROUND, PLAYOFFS, PLAYOFFS_TOTAL, MAIN_ROUND_AND_PLAY_OFFS or CUP).",
    "scheduled_time": "Scheduled tip-off, ISO 8601 UTC string.",
    "field_goals_recomputed": "True when the API's field-goal totals disagreed with its 2-point plus 3-point parts (2005-06 box scores) and were recomputed from the parts.",
}

#: Wrapper-specific descriptions, for columns this parser builds or renames.
_TEAM_TXT = "series team {x} (the higher seed is A)"
_STAND = {
    "home_losses": "Home games: number of losses in the table.",
    "away_losses": "Away games: number of losses in the table.",
    "total_games": "Games played in the table (home and away together).",
    "total_losses": "Losses in the table over all games.",
    "standing_id": "Opaque id of the standings row, a string.",
    "team_name_short": "Short team name (null on DBBL).",
    "table_points": "Table points scored (2 per win), from the API's `totalPointsMade`; the provider's notes call its 'Points' fields table points.",
    "table_points_against": "Table points conceded (2 per loss), from the API's `totalPointsAgainst`; per the provider's notes, table points.",
}
for _side in ("home", "away", "total"):
    _w = {"home": "Home games", "away": "Away games", "total": "All games"}[_side]
    _STAND[f"{_side}_points_for"] = f"{_w}: basketball points scored (the API's football-style `GoalsMade`)."
    _STAND[f"{_side}_points_against"] = f"{_w}: basketball points allowed (the API's football-style `GoalsAgainst`)."
    _STAND[f"{_side}_point_difference"] = (
        f"{_w}: point differential, scored minus allowed (the API's `GoalDifference`)."
    )
_GAME_ALIAS_INNER = {
    "team_id": "teamId",
    "season_team_id": "seasonTeamId",
    "team_name": "name",
    "tlc": "tlc",
    "is_foreign": "isForeign",
    "rank": "rank",
    "streak": "streak",
    "streak_count": "streakCount",
    "last10_victories": "last10Victories",
    "last10_losses": "last10Losses",
    "total_games": "totalGames",
}
_GAMES_LOCAL = {
    "bucket": "Which list of `/games/recent` the game came from: `past` (finished), `present` (today or live) or `future` (scheduled).",
    "latest_period": "Number of the last period played (4 = Q4, 5 = OT1, ...); the API sends `Q4` on /games and 4 on /games/recent. Null before the game.",
    "game_id": COMMON["game_id"],
    "tv_channel": "TV channel(s) showing the game, comma-joined (a list on some routes, an empty string when none).",
    "venue_name": "Name of the arena the game is played in; `venue_id` is its stable arena id.",
    "home_total_games": "Games the home team has played so far (the API sends it as a string).",
    "guest_total_games": "Games the guest team has played so far (the API sends it as a string).",
    "is_hidden": "True when the API hides the game (0/1 on /games, a boolean on the header).",
}
LOCAL: Dict[str, Dict[str, str]] = {
    "playoff_series": {
        "round": "Round of the bracket: `quarter_finals`, `semi_finals` or `finals`.",
        "series": "1-based position of the series within its round, in the order the API lists them.",
        **{
            f"team_{x}_{k}": v.format(t=_TEAM_TXT.format(x=x.upper()))
            for x in "ab"
            for k, v in {
                "id": "Season-team id of {t}, a string (the playoff tree sends it as an integer); equals the club team id.",
                "name": "Name of {t} as listed that season.",
                "tlc": "Three-letter club code of {t}.",
                "rank": "Regular-season table position (the seed) of {t}.",
            }.items()
        },
    },
    "playoff_games": {
        "round": "Round of the bracket: `quarter_finals`, `semi_finals` or `finals`.",
        "series": "1-based position of the game's series within its round, in the order the API lists them.",
        "team_a_id": "Season-team id of the series' team A (the higher seed); `points_team_a` is its score, whichever side hosted.",
        "team_b_id": "Season-team id of the series' team B; `points_team_b` is its score, whichever side hosted.",
    },
    "player_statistics": {"shirt_number": "Jersey number of the player, kept as text so a `00` jersey survives."},
    "game_teams": {
        "head_coach_name": "Name of the team's head coach.",
        "status": "Game status: PRE (scheduled), LIVE, POST (ended, not yet official) or OFFICIAL (final).",
    },
    "game_actions": {
        "seconds_left": "Seconds left in the period at the action: `game_time` as seconds.",
        "shot_x": "Shot x on the site's half-court chart (0-280 across, 140 through the rim); null without a location. Scale and orientation unverified.",
        "shot_y": "Shot y along the court from behind the baseline (rim near 45, arc top near 147); null without a location. Scale and orientation unverified.",
        "action_id": "Action id, an integer unique across games of a league.",
    },
    "standings": _STAND,
    "team": {
        "name_full": "Full club name as the API lists it today.",
        "mascot": "Club mascot name (null when none).",
        "homepage_url": "Club website address as sent (it can lack the scheme).",
    },
    "referees": {
        "referee_id": "Official id (`refId`, equal to the API's `id`); joins `referee1_id` .. `commissioner_id` of `scb_games`."
    },
    "transfers": {},
    "records_per_game": {
        "scope": "`season` for the requested season's records, `all_time` for the all-time list.",
        "stat": "The record's stat: points, rebounds, assists, steals, blocks, turnovers, efficiency, free_throws_made, three_point_shots_made or free_throws_success_percentage.",
    },
    "records_season_averages": {
        "stat": "The record's stat list: assist_average, block_average, efficiency_average, foul_average, free_throws_success_percentage, points_average, rebound_average, steal_average, three_point_shot_average or turnover_average."
    },
    "games": _GAMES_LOCAL,
    "games_recent": _GAMES_LOCAL,
    "game": _GAMES_LOCAL,
}
_MOVE = {
    "transfer_date": "Date of the transfer, YYYY-MM-DD (the API's `sortingDate`).",
    "source_team_id": "Club id the player left; null when the source is not a league club.",
    "source_team_name": "Name of the club the player left (the free-text field, else the short club name).",
    "destination_team_id": "Club id the player joined; null when the destination is not a league club.",
    "destination_team_name": "Name of the club the player joined (the free-text field, else the short club name).",
}
LOCAL["transfers"].update(_MOVE)
LOCAL["transfers_teams"] = {
    **_MOVE,
    "direction": "`joining` or `leaving`: whether `team_id` is the destination or the source of the move.",
}
LOCAL["records_per_game"].update(
    {
        "home_tlc": "Three-letter code of the home team of the record game.",
        "guest_tlc": "Three-letter code of the guest team of the record game.",
    }
)


LOCAL["playoff_games"].update(
    {
        "game_id": "Game id, a string (the playoff tree sends an integer); joins `scb_games.game_id`.",
        "game_winner_id": "Season-team id of the game winner, a string (the playoff tree sends an integer).",
        "home_team_id": "Season-team id of the home team, a string (the playoff tree sends an integer).",
        "guest_team_id": "Season-team id of the guest (away) team, a string (the playoff tree sends an integer).",
    }
)
LOCAL["playoff_series"]["match_winner_id"] = (
    "Season-team id of the series winner, a string (the playoff tree sends an integer)."
)
LOCAL["game_players"] = {
    "plus_minus": "The player's plus-minus (point margin while on court); null when the API sends an empty string (2005-06).",
}
#: Wire-type remarks that do not hold for the typed column.
_TYPE_REMARK = re.compile(r"\s*An integer sent as a string \(e\.g\. '0'\)\.?")


def _alias(short: str, name: str) -> Optional[str]:
    """The source remainder path of a games-frame column whose name differs from its wire field."""
    if short not in ("games", "games_recent", "game"):
        return ALIAS.get(short, {}).get(name)
    m = re.fullmatch(r"(home|guest)_(.+)", name)
    if m:
        side, rest = m.groups()
        if rest in _GAME_ALIAS_INNER:
            return f"{side}Team.{_GAME_ALIAS_INNER[rest]}"
        if rest == "score":
            return f"result.{side}TeamFinalScore"
        q = re.fullmatch(r"(q|ot)(\d)_score", rest)
        if q:
            return f"result.{side}Team{q.group(1).upper()}{q.group(2)}Score"
    m = re.fullmatch(r"(referee\d|commissioner)_(id|first_name|last_name)", name)
    if m:
        return f"{m.group(1)}.{ {'id': 'refId', 'first_name': 'firstName', 'last_name': 'lastName'}[m.group(2)] }"
    m = re.fullmatch(r"venue_(id|name|address|capacity|is_main)", name)
    if m:
        return "venue." + {"is_main": "isMain"}.get(m.group(1), m.group(1))
    m = re.fullmatch(r"top_scorer_(season_player_id|real_player_id|first_name|last_name)", name)
    if m:
        return (
            "topScorer."
            + {
                "season_player_id": "playerId",
                "real_player_id": "realPlayerId",
                "first_name": "firstName",
                "last_name": "lastName",
            }[m.group(1)]
        )
    direct = {
        "stat_status": "statstatus",
        "game_progress": "gameProgress",
        "live_stream_url": "linkLiveStream",
        "top_score": "topScore",
        "is_tbd": "isTBD",
        "is_free_tv": "isFreeTV",
        "match_day": "matchDay",
        "latest_period": "result.latestPeriod",
        "spectators": "result.spectators",
        "scheduled_time": "scheduledTime",
        "tv_type": "tvType",
    }
    return direct.get(name)


def _filler(name: str, desc: str) -> bool:
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


# --- scb-returns.md ------------------------------------------------------------------------------

_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*([^|]*?)\s*\|\s*(.*?)\s*\|\s*$")


def _md_sections(path: Path) -> Dict[str, List[Tuple[str, str]]]:
    """``{route: [(dotted path with [] kept, description)]}`` from the returns markdown."""
    out: Dict[str, List[Tuple[str, str]]] = {}
    cur: Optional[str] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            cur = line[3:].strip().strip("`") if line[3:].strip().startswith("`/") else None
            if cur:
                out[cur] = []
            continue
        m = _ROW.match(line)
        if cur and m and m.group(1) != "col_name":
            out[cur].append((m.group(1), m.group(3)))
    return out


def _describe(short: str, name: str, rows: List[Tuple[str, str]]) -> Optional[str]:
    if name in LOCAL.get(short, {}):
        return LOCAL[short][name]
    ctx = CONTEXT.get(short, {})
    prefix = max((p for p in ctx if p and name.startswith(p)), key=len, default="")
    needle = ctx.get(prefix, "")
    bare = name[len(prefix) :]
    want = _alias(short, name)
    best: Optional[Tuple[int, str]] = None
    for path, desc in rows:
        segs = [s for s in path.split(".") if s != "[]"]
        if not desc or desc.startswith("Unobserved") or (needle and needle not in path):
            continue
        dotted = ".".join(segs)
        hit = (dotted == want or dotted.endswith("." + want)) if want else bool(segs) and underscore(segs[-1]) == bare
        if hit and (best is None or len(segs) < best[0]):
            best = (len(segs), desc)
    text = _TYPE_REMARK.sub("", best[1]).strip() if best else ""
    if text and not _filler(name, text):
        return text
    return COMMON.get(name)


# --- frames, dtypes ------------------------------------------------------------------------------


def _frame(entry: Entry) -> pl.DataFrame:
    _spec_path, _path, parser, fixture, request, _summary, _fields, _league = entry
    body = json.loads((FIXTURES / fixture).read_text(encoding="utf-8"))
    return parser(_stamp(body, request))


_CODE = {pl.Int64: "i", pl.Float64: "f", pl.Boolean: "b", pl.String: "s"}
_TYPE = {"i": "integer", "f": "numeric", "b": "logical", "s": "character"}


def _dtype_codes(frames: Dict[str, pl.DataFrame]) -> Dict[str, Dict[str, str]]:
    """``{short: {column: dtype code}}`` in frame order; an all-null column with no name rule stops the run."""
    out: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, df in frames.items():
        out[short] = {}
        for name, dtype in df.schema.items():
            if dtype not in _CODE:
                raise SystemExit(f"scb: {short}.{name} has dtype {dtype}, not one of {list(_CODE)}")
            if df[name].null_count() == df.height and df.height and P._rule(name) is None and dtype == pl.String:
                bad.append(f"{short}.{name}")
            out[short][name] = _CODE[dtype]
    if bad:
        raise SystemExit(
            "scb: null in every fixture row with no dtype rule (add one to scb_parsers._rule): " + ", ".join(bad)
        )
    return out


def _write_dtypes(codes: Dict[str, Dict[str, str]]) -> None:
    by_parser: Dict[str, Dict[str, str]] = {}
    for short, cols in codes.items():
        merged = by_parser.setdefault(ENDPOINTS[short][2].__name__, {})
        for name, code in cols.items():
            if merged.setdefault(name, code) != code:
                raise SystemExit(f"scb: {ENDPOINTS[short][2].__name__}.{name} has two dtypes")
    lines = [
        '"""Column dtypes of the SCB parsers, one map per parser function.',
        "",
        "Generated by ``tools/codegen/gen_scb.py`` from the returns-schemas",
        "(``tools/codegen/schemas/native/scb``); do not edit. Codes: ``i`` Int64, ``f`` Float64,",
        "``b`` Boolean, ``s`` String. ``scb_parsers`` gives these dtypes to an empty frame, a gap-filled",
        "column and an all-null column, so an empty frame stacks onto a populated one.",
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


# --- endpoint YAML -------------------------------------------------------------------------------

NOTES_TEXT = [
    "Unofficial use of a public source: these wrappers read the front-end API of the German Bundesliga sites "
    "(easycredit-bbl.de, toyota-dbbl.de), run by SCB; payloads are not redistributed as release assets.",
    "Legal notice: the BBL Impressum forbids storing BBL content (`Inhalte`) in an information system without "
    "BBL's written permission. That is a copyright term, not a robots rule (the API host has no robots.txt). The "
    "owner reviewed it and accepted the intake (2026-10-12 ruling); keep requests slow (the runtime waits at least "
    "1 s per host) and raise any published data release with the owner. The DBBL host is the same operator and "
    "API; its site notice was not separately reviewed.",
    "Both hosts need two headers: a constant `x-api-key` and an `x-api-secret` that rotates. The runtime derives "
    "the secret on first use from the key embedded in an easycredit-bbl.de page (reversed, `:` turned into `-`) and "
    "never logs or returns it. The derivation was reverse-engineered from observed requests and responses and has "
    "not been checked against the site's own front-end code; if the site changes how it signs requests, every call "
    "fails with AssetFetchError.",
    "Id key spaces: seasonId is the START year (2025 = 2025-26), so a wrapper's END-year `season` is sent as "
    "`season - 1`. Teams are keyed by teamId (seasonTeamId equals it in every sample). seasonPlayerId is per player "
    "per season and per club (a player traded mid-season has two); realPlayerId is the person. playerId is "
    "ambiguous (the seasonPlayerId in box scores, records and /season-players; the person in /transfers). Game ids "
    "are strings.",
]


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    spec_path, path, parser, _fixture, _request, summary, fields, league = entry
    fields = copy.deepcopy(fields)
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    extra = ([copy.deepcopy(LEAGUE_Q)] if league else []) + copy.deepcopy(QUERY.get(short, []))
    if extra:
        fields["extra_params"] = extra
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/scb/{short}",
        **fields,
    }


def main() -> None:
    refs = refs_dir("scb", SPEC)
    spec = load_spec(refs, SPEC)
    covered = {e[0] for e in ENDPOINTS.values()}
    if covered != set(spec["paths"]):
        raise SystemExit(f"scb: spec paths and ENDPOINTS differ: {sorted(covered ^ set(spec['paths']))}")
    doc = {
        "api": "scb",
        "host": HOST,
        "name_pattern": "scb_{short}",
        "module": "scb",
        "parser_module": "basketball.scb.scb_parsers",
        "getter_module": "sportsdataverse.basketball.scb.scb_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (games, standings, team) name other families
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raises": [
                "sportsdataverse.errors.NoDataError: the API has nothing for the request (an unknown id, a game "
                "before 2002-03 or not yet started, an empty table, a playoff bracket not yet drawn).",
            ],
            "see_also": [
                {
                    "name": "easyCredit BBL",
                    "url": "https://www.easycredit-bbl.de/",
                    "note": "the BBL site whose API these wrappers read",
                },
                {
                    "name": "Toyota DBBL",
                    "url": "https://www.toyota-dbbl.de/",
                    "note": "the DBBL site (same API, second host)",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball package (R)",
                },
                {
                    "name": "wehoop",
                    "url": "https://wehoop.sportsdataverse.org",
                    "note": "SportsDataverse women's basketball package (R)",
                },
            ],
            "notes": NOTES_TEXT,
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/scb.yaml", doc)

    md = _md_sections(refs / RETURNS)
    schema_dir = ROOT / "tools/codegen/schemas/native/scb"
    rewrite_schema_dir(schema_dir)
    P.DTYPES = {}  # parse by the name rules alone, so the last run's scb_dtypes.py cannot feed back
    codes = _dtype_codes({short: _frame(entry) for short, entry in ENDPOINTS.items()})
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        key = f"{SCHEMA_PREFIX}{short}"
        notes[key] = {}
        cols = []
        for name, code in codes[short].items():
            desc = _describe(short, name, md.get(entry[0], []))
            if desc is None or _filler(name, desc):
                bad.append(f"{key}.{name}: {desc!r}")
            else:
                notes[key][name] = " ".join(desc.split())
            # descriptions live in manual_column_descriptions.yaml (via the notes file), never here
            cols.append({"name": name, "type": _TYPE[code], "description": ""})
        write_yaml(schema_dir / f"{short}.yaml", {"schema": key, "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit("scb: filler or missing descriptions:\n" + "\n".join(bad))
    write_yaml(NOTES, notes)
    _write_dtypes(codes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"scb: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
