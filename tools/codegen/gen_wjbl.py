"""Generate the ``wjbl`` flat-API endpoint YAML + returns-schemas (W League, Japan women's basketball, ``api.wjbl.01core.app/api``).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_kzs.py`` with the shared plumbing in
``gen_soccer_common.py``. Reads the frozen spec and the returns md from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/wjbl.yaml`` -- 19 wrappers over the spec's 15 paths (one route can feed several frames: the
  game header feeds ``wjbl_game`` and ``wjbl_period_scores``, the box score two, the standings grid two, the roster two).
* ``tools/codegen/schemas/native/wjbl/<short>.yaml`` -- one returns-schema per wrapper: the columns and dtypes of its
  parser (``wjbl_parsers.SCHEMAS``, pinned from the FULL bodies in ``tests/fixtures/wjbl/full/``), after the parser ran
  clean over every fixture of the wrapper. Schema key ``wjbl_<short>``; every ``description`` is ``''``.
* ``tools/codegen/notes/wjbl_descriptions.yaml`` -- the column descriptions (committed: the generator owns the text),
  keyed ``wjbl_<short>`` -> column, for ``manual_column_descriptions.yaml``. Each comes from :data:`ROUTE`
  (route-specific), the route's own ``wjbl-returns.md`` row (:data:`SECTIONS`; the md carries the English translations
  of the Japanese values), else :data:`LOCAL` (parser-built columns). A filler or missing description stops the run.
* ``tests/fixtures/wjbl/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_wjbl.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.wjbl import wjbl_parsers as P
from sportsdataverse.dl_utils import underscore

HOST = "https://api.wjbl.01core.app/api"
SPEC = "wjbl.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "wjbl"
SCHEMA_PREFIX = "wjbl_"
SNIPPET = ROOT / "tools" / "codegen" / "notes" / "wjbl_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

SEASON_Q = (
    "Season END year (2026 = 2025-26, 2000 = 1999-2000); the wrapper maps it to the league edition id (league_id) with "
    "the live league list (wjbl_leagues, one extra cached request)."
)
GAME_ID = (
    "Game id (play_schedule_id in wjbl_schedule): ONE global sequence across every league edition and season, e.g. "
    "7987 (2025-26 playoff final game 1), 5533 (2018-19), 4243 (2013-14 final, box score only). The game must be final."
)
SEASON_IDS_Q = (
    "Season phase id or ids (season_id in wjbl_standing_seasons; a phase inside a league edition, not a year): an "
    "integer, a list or a comma string such as '169,170' (2025-26 regular season Premier and Future divisions)."
)
TEAM_ID = "Franchise id (team_id in wjbl_teams): stable across seasons and sponsor renames, e.g. 13 (Toyota Antelopes)."
PHASE_ID = "Season phase id (season_id in wjbl_standing_seasons), e.g. 169 (2025-26 regular season, Premier division)."
PLAYER_ID = "Player id (player_id in wjbl_box_score or wjbl_roster): the person, stable across seasons, e.g. 9799."

# A fixture is (path relative to FIXTURES, ...). Entry: short -> (route, parser, fixtures, summary, endpoint fields).
Entry = Tuple[str, Callable[..., Any], List[str], str, Dict[str, Any]]


def _q(name: str, wire: str, desc: str, *, typ: str = "str", required: bool = False, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": desc}
    if required:
        out["required"] = True
    out.update(extra)
    return out


def _game_params() -> Dict[str, Any]:
    return {
        "extra_params": [_q("play_schedule_id", "play_schedule_id", GAME_ID, typ="int", required=True)],
        "example_args": {"play_schedule_id": 7987},
    }


_G = {"play_schedule_id": 7987}
_DETAIL = ["full/play-schedule__detail@7987.json", "captures/play-schedule__detail.json"]
_DETAIL += [f"captures/play-schedule__detail@{g}.json" for g in (4243, 5533, 7360)]
_BOX = ["full/boxscore@7987.json", "captures/boxscore.json"] + [
    f"captures/boxscore@{g}.json" for g in (4243, 5533, 7360)
]
_PBP = ["full/pbp__play-by-play@7987.json", "captures/pbp__play-by-play.json"] + [
    f"captures/pbp__play-by-play@{g}.json" for g in (5533, 7360)
]
_TEAMSTATS = ["full/pbp__team-stats@7987.json", "captures/pbp__team-stats.json"] + [
    f"captures/pbp__team-stats@{g}.json" for g in (5533, 7360)
]
_PLAYERSTATS = ["full/pbp__player-stats@7987.json", "captures/pbp__player-stats.json"] + [
    f"captures/pbp__player-stats@{g}.json" for g in (5533, 7360)
]
_INFO = ["full/play-schedule@7987.json", "captures/play-schedule.json"] + [
    f"captures/play-schedule@{g}.json" for g in (4243, 5533, 7360)
]
_SCHEDULES = [
    "full/play-schedules@45_2026-04.json",
    "captures/play-schedules.json",
    "captures/play-schedules@20_2014-04.json",
    "captures/play-schedules@28_2018-10.json",
    "captures/play-schedules@45_2025-10_team13.json",
]
_MONTHS = ["captures/play-schedule-months.json", "captures/play-schedule-months@20.json"]
_STANDINGS = ["captures/team-standings.json"]
_ROSTER = ["captures/team__roster.json"]

ENDPOINTS: Dict[str, Entry] = {
    "leagues": (
        "/leagues",
        P.parse_wjbl_leagues,
        ["full/leagues.json", "captures/leagues.json"],
        "Every competition edition (league_id): the W League seasons back to 1999-2000, all-star games, cups and camps.",
        {"fixed_params": {"target": "play_schedule"}, "example_args": {}},
    ),
    "game_months": (
        "/play-schedule-months",
        P.parse_wjbl_game_months,
        _MONTHS,
        "Months (YYYY-MM) that have games in a season, newest first; each is one page of wjbl_schedule.",
        {
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True)],
            "example_args": {"season": 2026},
        },
    ),
    "schedule": (
        "/play-schedules",
        P.parse_wjbl_schedule,
        _SCHEDULES,
        "Games of a season in ONE month (default: the season's latest month) with teams, venue, status and score.",
        {
            "extra_params": [
                _q("season", "season", SEASON_Q, typ="int", required=True),
                _q(
                    "game_month",
                    "game_month",
                    "Month to list, 'YYYY-MM' (see wjbl_game_months). Omitted = the season's latest month only, NOT "
                    "the whole season: the API pages by month.",
                ),
                _q("team_id", "team_id", TEAM_ID + " Narrows the month to that team's games.", typ="int"),
            ],
            "example_args": {"season": 2026, "game_month": "2026-04"},
        },
    ),
    "game": (
        "/play-schedule/detail",
        P.parse_wjbl_game,
        _DETAIL,
        "Game header of a final game: both teams, the result flag and the score by period, one row.",
        _game_params(),
    ),
    "period_scores": (
        "/play-schedule/detail",
        P.parse_wjbl_period_scores,
        _DETAIL,
        "Points of each side in each period played (quarters and overtimes) from the game header.",
        _game_params(),
    ),
    "game_info": (
        "/play-schedule",
        P.parse_wjbl_game_info,
        _INFO,
        "Game information of a final game: venue, times, referees, links and the Japanese game report, one row.",
        _game_params(),
    ),
    "box_score": (
        "/boxscore",
        P.parse_wjbl_box_score,
        _BOX,
        "Box score of a final game (2013-14 on): one row per player of both teams.",
        _game_params(),
    ),
    "team_box_score": (
        "/boxscore",
        P.parse_wjbl_team_box_score,
        _BOX,
        "Team line and totals of each side of a final game's box score, with the shooting percentages.",
        _game_params(),
    ),
    "pbp": (
        "/pbp/play-by-play",
        P.parse_wjbl_pbp,
        _PBP,
        "Play-by-play of a final game (2018-19 on): one row per play, Japanese action vocabulary, no shot x/y.",
        _game_params(),
    ),
    "team_period_stats": (
        "/pbp/team-stats",
        P.parse_wjbl_team_period_stats,
        _TEAMSTATS,
        "Team stat summary per period, half and game (2018-19 on): percentages, rebounds, assists, turnovers, lead changes.",
        _game_params(),
    ),
    "player_period_leaders": (
        "/pbp/player-stats",
        P.parse_wjbl_player_period_leaders,
        _PLAYERSTATS,
        "Leading scorer, passer and rebounder of each team per period, half and game (2018-19 on).",
        _game_params(),
    ),
    "standing_seasons": (
        "/seasons/standings",
        P.parse_wjbl_standing_seasons,
        ["full/seasons__standings.json", "captures/seasons__standings.json"],
        "Every season phase (season_id) with its edition name and standings flags.",
        {"example_args": {}},
    ),
    "team_standings": (
        "/team-standings",
        P.parse_wjbl_team_standings,
        _STANDINGS,
        "League table of one or more season phases: rank and record of every team.",
        {
            "extra_params": [_q("season_ids", "season_ids", SEASON_IDS_Q, required=True)],
            "example_args": {"season_ids": "169"},
        },
    ),
    "head_to_head": (
        "/team-standings",
        P.parse_wjbl_head_to_head,
        _STANDINGS,
        "Head-to-head results grid (hoshitori-hyo) of one or more season phases: one row per game played.",
        {
            "extra_params": [_q("season_ids", "season_ids", SEASON_IDS_Q, required=True)],
            "example_args": {"season_ids": "169"},
        },
    ),
    "team_ranking": (
        "/team-ranking",
        P.parse_wjbl_team_ranking,
        ["captures/team-ranking.json"],
        "League table with per-game team averages (points, shooting, rebounds, assists) of one or more season phases.",
        {
            "extra_params": [_q("season_ids", "season_ids", SEASON_IDS_Q, required=True)],
            "example_args": {"season_ids": "169,170"},
        },
    ),
    "teams": (
        "/teams/base",
        P.parse_wjbl_teams,
        ["full/teams__base.json", "captures/teams__base.json"],
        "Every franchise (team_id) the site knows, all-star sides included.",
        {"example_args": {}},
    ),
    "roster": (
        "/team/roster",
        P.parse_wjbl_roster,
        _ROSTER,
        "Players of a team in a season phase, with the head coach.",
        {
            "extra_params": [
                _q("team_id", "team_id", TEAM_ID, typ="int", required=True),
                _q("season_id", "season_id", PHASE_ID, typ="int"),
            ],
            "example_args": {"team_id": 13, "season_id": 169},
        },
    ),
    "staff": (
        "/team/roster",
        P.parse_wjbl_staff,
        _ROSTER,
        "Staff of a team in a season phase (general manager, assistant coaches, support staff).",
        {
            "extra_params": [
                _q("team_id", "team_id", TEAM_ID, typ="int", required=True),
                _q("season_id", "season_id", PHASE_ID, typ="int"),
            ],
            "example_args": {"team_id": 13, "season_id": 169},
        },
    ),
    "player": (
        "/player",
        P.parse_wjbl_player,
        ["captures/player.json"],
        "Player profile: current team, names, bio and career notes, one row.",
        {
            "extra_params": [_q("player_id", "player_id", PLAYER_ID, typ="int", required=True)],
            "example_args": {"player_id": 9799},
        },
    ),
}

# short -> (md route section, roots tried in order). A column's md row is ``<root><column>`` over the underscored wire
# path (``seasons.{key}.play_schedules.[].home_team_id`` -> ``seasons_play_schedules_home_team_id``).
SECTIONS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "leagues": ("/leagues", ("",)),
    "game_months": ("/play-schedule-months", ("game_months_", "")),
    "schedule": ("/play-schedules", ("seasons_play_schedules_", "seasons_", "")),
    "game": ("/play-schedule/detail", ("",)),
    "period_scores": ("/play-schedule/detail", ("",)),
    "game_info": ("/play-schedule", ("",)),
    "box_score": ("/boxscore", ("home_players_", "away_players_", "")),
    "team_box_score": ("/boxscore", ("home_total_", "home_percentage_", "home_team_", "")),
    "pbp": ("/pbp/play-by-play", ("play_by_plays_", "")),
    "team_period_stats": ("/pbp/team-stats", ("periods_", "")),
    "player_period_leaders": ("/pbp/player-stats", ("periods_", "")),
    "standing_seasons": ("/seasons/standings", ("seasons_", "")),
    "team_standings": ("/team-standings", ("teams_", "")),
    "head_to_head": ("/team-standings", ("teams_vs_teams_scores_", "")),
    "team_ranking": ("/team-ranking", ("seasons_teams_", "seasons_", "")),
    "teams": ("/teams/base", ("",)),
    "roster": ("/team/roster", ("players_", "")),
    "staff": ("/team/roster", ("staffs_", "")),
    "player": ("/player", ("",)),
}
#: The wire name behind a parser-renamed column (the md row is the wire's).
ALIAS: Dict[str, Dict[str, str]] = {
    "team_period_stats": {
        "home_turnovers": "home_trun_over",
        "away_turnovers": "away_trun_over",
        "home_max_difference": "home_max_differenct",
    },
    "schedule": {"game_day": "game_date"},
}

_CTX_SEASON = (
    "Season END year (2025-26 = 2026, 1999-2000 = 2000) from league_title ('25-26 Wリーグ'); null for an all-star game, "
    "cup or camp."
)
_CTX_SIDE = "Which side of the game the row belongs to: home or away (the API's home / away blocks)."
_CTX_GAME = (
    "Game id: ONE global sequence across every league edition and season (7987 = 2025-26 playoff final game 1); the "
    "play_schedule_id of the request."
)
_CTX_TEAM = "Franchise id team_id of the side (stable across seasons and sponsor renames; 13 = Toyota Antelopes)."
_STAT = {
    "pts": "Points scored by the side (the sum of its players' points).",
    "p2m": "Two-point field goals made.",
    "p2a": "Two-point field goals attempted.",
    "p3m": "Three-point field goals made.",
    "p3a": "Three-point field goals attempted.",
    "ftm": "Free throws made.",
    "fta": "Free throws attempted.",
    "f": "Personal fouls committed.",
    "off": "Offensive rebounds.",
    "def": "Defensive rebounds.",
    "tot": "Total rebounds (offensive plus defensive).",
    "ast": "Assists credited to the side.",
    "stl": "Steals credited to the side.",
    "blk": "Blocked shots credited to the side.",
    "to": "Turnovers committed by the side.",
}

#: Descriptions of parser-built columns, by column name (apply to every wrapper that has the column unless a md row or
#: :data:`ROUTE` entry says better).
LOCAL: Dict[str, str] = {
    "season": _CTX_SEASON,
    "play_schedule_id": _CTX_GAME,
    "team_side": _CTX_SIDE,
    "team_id": _CTX_TEAM,
    "period_label": (
        "Readable period: Q1-Q4 quarters, OT1-OT10 overtimes (period categories 5-14), H1 / H2 halves (15 / 16), OT all "
        "overtimes together (17), GAME the whole game (18)."
    ),
    "game_day": "Game date as an ISO date (YYYY-MM-DD, Japan time), built from the game year and the game date.",
    "min_seconds": "Minutes played in whole seconds, from the 'mm:ss' clock string min (null when it does not parse).",
    "seconds_remaining": "Seconds remaining in the period, from the 'm:ss' clock string rest_time.",
    "home_score": "Home points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score.",
    "away_score": "Away points after the play, parsed from score ('HOME-AWAY'); null on plays that carry no score.",
    "wins": "Wins in the record, parsed from win_lost ('23勝5負' = 23 wins).",
    "losses": "Losses in the record, parsed from win_lost ('23勝5負' = 5 losses).",
    "player_height_cm": "Height in whole centimetres, parsed from player_height ('179cm').",
    "player_weight_kg": "Weight in whole kilograms, parsed from player_weight ('77kg').",
    "team_comment": "Free text the league shows with the roster (captains and other notes, HTML with <br>), as sent.",
    "coach_id": "Head coach id (coach_id of the site's coach page); the head coach is not in the staff rows.",
    "coach_name": "Head coach name, Japanese.",
    "staff_name": "Name of the staff member, Japanese.",
    "position": "Role or position as sent (a playing position code for players, a Japanese role for staff).",
    "league_id": (
        "Competition edition id league_id: ONE per season of the league and per all-star game, cup or camp "
        "(45 = '25-26 Wリーグ', 47 = '26-27'); not in chronological order."
    ),
    "season_id": "Season phase id season_id: a phase inside one league edition (169 = 2025-26 regular season Premier, 170 = Future, 176 = playoff final); not a year.",
    "season_team_id": "Team-season registration id (one per team per season phase; 1343 = Toyota in phase 169); not team_id.",
    "period_category": (
        "Period category: 1-4 quarters, 5-14 overtimes, 15 first half, 16 second half, 17 all overtimes together, 18 the "
        "whole game."
    ),
    "home_team_id": "Home franchise id team_id (stable across seasons and sponsor renames; 13 = Toyota Antelopes).",
    "away_team_id": "Away franchise id team_id (stable across seasons and sponsor renames; 12 = Denso Iris).",
    "game_number": "1-based order of the game within the pair of teams in the grid (the wire list has no number).",
    "opponent_team_id": "Franchise id team_id of the opponent in that game.",
    "row_type": "Which line of the side: team (stats credited to the team itself) or total (the whole side).",
    "game_status": "Game state: fixed = final (confirmed result), scheduled = not played yet; wrappers only serve final games except the schedule.",
}

#: Route-specific descriptions (keyed by wrapper short) that replace a returns-md row or fill a column it lacks.
ROUTE: Dict[str, Dict[str, str]] = {
    "game": {
        "play_schedule_id": _CTX_GAME,
        "game_status": "Game state; always fixed (final) here, the wrapper refuses a game that is not final.",
    },
    "period_scores": {
        "team_id": _CTX_TEAM,
        "score": "Points the side scored in that period (null periods, such as overtimes not played, have no row).",
        "period": "Period number: 1-4 are the quarters and 5-8 the first to fourth overtime (score_ot1 to score_ot4).",
    },
    "box_score": {
        "team_id": _CTX_TEAM,
    },
    "team_box_score": {
        "team_id": _CTX_TEAM,
        **_STAT,
        "min": "Minutes: summed minutes 'mmm:ss' on the total row ('200:00' = 5 x 40:00), always '00:00' on the team row.",
        "p2p": "Two-point field-goal percentage 0-100 (a string on the wire); total row only.",
        "p3p": "Three-point field-goal percentage 0-100 (a string on the wire); total row only.",
        "ftp": "Free-throw percentage 0-100 (a string on the wire); total row only.",
        "fgp": "Field-goal percentage 0-100, two- and three-pointers together (a string on the wire); total row only.",
    },
    "pbp": {
        "team_id": "Acting team: franchise id team_id; null on period and game-end lines.",
        "team_side": "Side of the acting team, home or away (from home_away: 1 home, 2 away); null on period and game-end lines.",
    },
    "team_period_stats": {
        "home_rebound_off": "Home offensive rebounds in the period, the first number of the wire's 'off-def-total' string.",
        "home_rebound_def": "Home defensive rebounds in the period, the second number of the wire's 'off-def-total' string.",
        "home_rebound_tot": "Home total rebounds in the period, the third number of the wire's 'off-def-total' string.",
        "away_rebound_off": "Away offensive rebounds in the period, the first number of the wire's 'off-def-total' string.",
        "away_rebound_def": "Away defensive rebounds in the period, the second number of the wire's 'off-def-total' string.",
        "away_rebound_tot": "Away total rebounds in the period, the third number of the wire's 'off-def-total' string.",
        "home_turnovers": "Home turnovers in the period, from the wire's counted string ('6本' = 6); the wire key is spelled home_trun_over.",
        "away_turnovers": "Away turnovers in the period, from the wire's counted string ('6本' = 6); the wire key is spelled away_trun_over.",
        "home_max_difference": "Home team's largest lead in the period, in points; the wire key is spelled home_max_differenct.",
        "lead_count": "Lead changes in the period, from the wire's counted string ('0回' = 0 times).",
        "tie_count": "Times the score was tied in the period, from the wire's counted string ('4回' = 4 times).",
    },
    "standing_seasons": {
        "season_full_name": "Edition name with the sponsor, Japanese, e.g. '26-27 Wリーグ 大樹生命 Wリーグ ユナイテッド' (truncated by the API).",
        "season": _CTX_SEASON + " Read from the start of season_full_name.",
    },
    "team_standings": {
        "position": "Rank of the team in the phase (a string on the wire, equal to the map key).",
    },
    "head_to_head": {
        "season_id": PHASE_ID.replace("(season_id in wjbl_standing_seasons)", "of the grid"),
        "team_id": "Franchise id team_id of the ROW team (the team whose points are win).",
        "win": "The ROW team's points in that game (a string on the wire, whatever the result; 56 on a loss).",
        "losg": "The OPPONENT's points in that game (a string on the wire; the wire key is spelled losg).",
    },
    "team_ranking": {
        "position": "Rank of the team in the phase (a string on the wire).",
    },
    "roster": {
        "position": "Playing position (G / F / C style code); empty in every sampled row.",
    },
    "staff": {
        "position": "Staff role, Japanese: アソシエイトヘッドコーチ associate head coach, アシスタントコーチ assistant coach, ゼネラルマネージャー general manager, 総括 team director, サポートスタッフ support staff.",
        "url": "Link attached to the staff entry; empty in most rows.",
    },
    "game_months": {
        "league_id": "Competition edition id league_id, cast to an integer (the API echoes a string here and an integer elsewhere).",
        "season": "Season END year of the request (2026 = 2025-26); the body itself does not carry it.",
    },
}


def _md_sections(path: Path) -> Dict[str, Dict[str, str]]:
    """``{route: {underscored wire path: description}}`` from the returns md (``{key}`` and ``[]`` segments dropped)."""
    out: Dict[str, Dict[str, str]] = {}
    current: Optional[str] = None
    head = re.compile(r"^## `https://api\.wjbl\.01core\.app/api([^`]+)`")
    row = re.compile(r"^\| `([^`]+)` \| [^|]+ \| (.*) \|$")
    for line in path.read_text(encoding="utf-8").splitlines():
        h = head.match(line)
        if h:
            current = h.group(1)
            out.setdefault(current, {})
            continue
        m = row.match(line)
        if m and current is not None:
            segs = [underscore(s) for s in m.group(1).split(".") if s not in ("[]", "") and not s.startswith("{")]
            if segs:
                out[current].setdefault("_".join(segs), m.group(2).strip())
    return out


def _filler(name: str, desc: str) -> bool:
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


def _describe(short: str, name: str, md: Dict[str, Dict[str, str]], dtype: Any = pl.String) -> str:
    """ROUTE, then the route's md row (under the wire name), then LOCAL."""
    section, roots = SECTIONS[short]
    route = ROUTE.get(short, {})
    if name in route:
        return route[name]
    rows = md.get(section, {})
    wire = ALIAS.get(short, {}).get(name, name)
    for key in (wire, name):
        for root in roots:
            hit = rows.get(root + key)
            if hit and not _filler(name, hit):
                return _typed(hit, dtype)
    return LOCAL.get(name, "")


def _typed(text: str, dtype: Any) -> str:
    """A md row that calls a column a string (the wire type) gains the type the parser casts it to."""
    if (
        dtype != pl.String
        and re.search(r"\bstring\b", text)
        and not re.search(r"\bwire\b|\bparsed\b|\bechoes\b|\bfrom the\b", text)
    ):
        return text.rstrip(". ") + ("; parsed to an integer." if dtype == pl.Int64 else "; parsed to a number.")
    return text


def _fixture_body(rel: str) -> Any:
    return json.loads((FIXTURES / rel).read_text(encoding="utf-8"))


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    route, parser, _fixtures, summary, fields = entry
    return {
        "short": short,
        "summary": summary,
        "path": route,
        "parser": parser.__name__,
        "returns_schema": f"native/wjbl/{short}",
        **copy.deepcopy(fields),
    }


_TYPE = {pl.Int64: "integer", pl.Float64: "numeric", pl.String: "character"}

NOTES = [
    "Unofficial and unsupported: these wrappers read the JSON backend of www.wjbl.org (api.wjbl.01core.app), keyless, "
    "found in the site's own JavaScript. The site's terms of use (www.wjbl.org/kiyaku/) forbid reproducing, "
    "republishing or otherwise using site content without permission: a copyright clause, not an anti-scraping one. "
    "The wrappers fetch on the user's behalf; a published data release or a hosted mirror needs the owner's decision "
    "first. The site has no robots.txt (404, checked 2026-10-12), so no Crawl-delay is published; the runtime waits at "
    "least 1 s before every request, retries included. Payloads are not redistributed as release assets.",
    "Women's league: the W League is Japan's top women's basketball league (the SportsDataverse R counterpart is "
    "wehoop). All text is Japanese (team names, phases, play-by-play actions); the column descriptions translate the "
    "vocabulary.",
    "Seasons are END years (2026 = 2025-26, 2000 = 1999-2000). The API keys a season edition by an opaque league_id "
    "(45 = '25-26 Wリーグ'; every all-star game, cup and camp has one too) and the wrappers map the END year with the "
    "live league list (one extra, cached request; END = 2000 + the second YY of league_title). Phases inside an edition "
    "(season_id: 169 = regular season Premier, 170 = Future, 176 = playoff final; two divisions from 2024-25) are native "
    "ids, passed as given: wjbl_standing_seasons lists them with their END year.",
    "The schedule is paged by month: wjbl_schedule answers ONE month (game_month, YYYY-MM; omitted = the season's "
    "latest month only). List the months with wjbl_game_months and loop. The game routes take only the global "
    "play_schedule_id and serve final games only: each makes one extra, cached request (the game header) to prove the "
    "game is final and that the answer is the game and the teams asked for.",
    "Errors are HTTP 200: an unknown id answers [] (NoDataError) and a missing or non-numeric parameter answers an "
    "error envelope (AssetFetchError). Coverage: schedules from 1999-2000, box scores from 2013-14, play-by-play and the "
    "period stats from 2018-19 (earlier games answer [] on the /pbp routes: NoDataError). No route carries shot "
    "coordinates.",
    "Personal data: the organiser's contact details (phones, e-mail) and the player's blood type are never output, and "
    "phone numbers and e-mail addresses inside free text are replaced by [redacted-phone] / [redacted-email]. Do not add "
    "them back. Player bio fields and referee names are public and stay.",
]


def main() -> None:
    refs = refs_dir("wjbl", SPEC)
    spec = load_spec(refs, SPEC)
    missing = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if missing:
        raise SystemExit(f"wjbl: spec paths and ENDPOINTS differ: {sorted(missing)}")
    for short, entry in ENDPOINTS.items():
        names = set(re.findall(r"\{([^}]+)\}", entry[0]))
        assert not {"league", "sport"} & names, short  # the renderer substitutes those as ESPN slugs
        assert entry[1].__name__ in P.SCHEMAS, short
        for rel in entry[2]:
            assert (FIXTURES / rel).exists(), f"{short}: missing fixture {rel}"
            frame = entry[1](_fixture_body(rel))
            if frame.schema != pl.Schema(P.SCHEMAS[entry[1].__name__]):
                raise SystemExit(f"wjbl: {short}: {rel} parses to a different schema than SCHEMAS")
    doc = {
        "api": "wjbl",
        "host": HOST,
        "name_pattern": "wjbl_{short}",
        "module": "wjbl",
        "parser_module": "basketball.wjbl.wjbl_parsers",
        "getter_module": "sportsdataverse.basketball.wjbl.wjbl_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "notes": NOTES,
            "raw_doc": (
                "the raw body (``dict``): the route's JSON (a bare list as ``{'items': [...]}``) plus ``_request`` "
                "(the ids asked for, with ``season`` the END year)"
            ),
            "raises": [
                "sportsdataverse.errors.NoDataError: the API has nothing for the request (an empty list, an id it "
                "does not know, a game without play-by-play before 2018-19, a season without months or games).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is unknown or partial: an "
                "error envelope (HTTP 200), any other non-2xx, a game that is not final, a truncated play-by-play, or a "
                "body for another game, league edition, month, team, season phase or player than the one asked for.",
                "ValueError: a season that is not the END year of a W League edition, an id that is not an integer, a "
                "game_month that is not YYYY-MM (raised before the request), or the API's HTTP 400 / 422.",
            ],
            "see_also": [
                {
                    "name": "W League",
                    "url": "https://www.wjbl.org",
                    "note": "the league site whose API these wrappers read",
                },
                {
                    "name": "wehoop",
                    "url": "https://wehoop.sportsdataverse.org",
                    "note": "SportsDataverse women's basketball package (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/wjbl.yaml", doc)

    md = _md_sections(refs / "wjbl-returns.md")
    unknown = {sec for sec, _ in SECTIONS.values()} - set(md)
    if unknown or set(SECTIONS) != set(ENDPOINTS):
        raise SystemExit(f"wjbl: SECTIONS out of step with the returns md / ENDPOINTS: {sorted(unknown)}")
    schema_dir = ROOT / "tools/codegen/schemas/native/wjbl"
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        key = f"{SCHEMA_PREFIX}{short}"
        snippet[key] = {}
        cols = []
        for name, dtype in P.SCHEMAS[entry[1].__name__].items():
            desc = _describe(short, name, md, dtype)
            if _filler(name, desc):
                bad.append(f"{key}.{name}: {desc!r}")
            snippet[key][name] = desc
            # descriptions live in manual_column_descriptions.yaml (via the snippet), never here
            cols.append({"name": name, "type": _TYPE[dtype], "description": ""})
        write_yaml(schema_dir / f"{short}.yaml", {"schema": key, "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit("wjbl: filler or missing descriptions:\n" + "\n".join(bad))
    write_yaml(SNIPPET, snippet)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"wjbl: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
