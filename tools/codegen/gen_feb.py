"""Generate the ``feb`` flat-API endpoint YAML + returns-schemas (Spanish FEB: LiveStats API + schedule pages).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_vhl.py`` (itself
``gen_asa.py`` with the shared plumbing in ``gen_soccer_common.py``). Reads the frozen spec and the returns md
from the ``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/feb.yaml`` -- 11 wrappers over the spec's 8 routes (6 JSON, 2 HTML). One route feeds
  several frames (``/TeamStats`` feeds ``game_info``, ``period_scores`` and ``team_stats``; ``/resultados.aspx``
  feeds ``results`` and ``standings``), each with its own parser; :data:`ENDPOINTS` is the source of truth. The
  game routes take ``game_id``; the schedule pages take ``group`` + ``season`` (END year) and ``feb_runtime`` maps
  the season to the site's START-year ``t``.
* ``tools/codegen/schemas/native/feb/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser run
  over its committed fixture (the FULL game bodies in ``tests/fixtures/feb/full/``, the refs page records for the
  schedule pages). Each parser emits a fixed column list with dtypes pinned by name (``feb_parsers.SPECS``), so a
  fixture cannot mistype a rate. The schemas carry ``description: ''`` (descriptions never live in ``schemas/**``).
* ``tools/codegen/feb_column_descriptions.snippet.yaml`` -- ``{feb_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (gitignored). Text comes from :data:`LOCAL` (derived and renamed columns),
  :data:`LOCAL_BY` (a column whose meaning differs by wrapper), then the ``feb-returns.md`` row of the same record
  path. An empty, short or filler text fails the run.
* ``tests/fixtures/feb/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_feb.py``
"""

from __future__ import annotations

import copy
import gzip
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import (
    ROOT,
    columns_from_frame,
    load_spec,
    refs_dir,
    rewrite_schema_dir,
    write_yaml,
)

from sportsdataverse.basketball.feb import feb_parsers as P
from sportsdataverse.basketball.feb.feb_runtime import REQUEST_KEY
from sportsdataverse.dl_utils import underscore

HOST = "https://intrafeb.feb.es/LiveStats.API/api/v1"
SITE = "https://baloncestoenvivo.feb.es"
SPEC = "feb.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "feb"
SNIPPET = ROOT / "tools" / "codegen" / "feb_column_descriptions.snippet.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)
GAME_ID_FIXTURE = 2535725

LEGAL = (
    "Legal notice (sdv-internal-refs/feb/README.md): the FEB's aviso legal asserts intellectual-property rights over "
    "the site's content and databases (the EU database right). It has no explicit scraping clause and robots.txt "
    "returns 404 on both hosts, but SportsDataverse has not ruled on it: fetching on your own machine is fine; a published "
    "data release or a hosted mirror needs a legal sign-off first."
)
UNOFFICIAL = (
    "Unofficial and unsupported: the LiveStats API is the federation's own widget backend, not a documented public "
    "API. It needs a bearer JWT, which is public: ``feb_runtime`` reads it from the site's home page at run time, "
    "keeps it in memory for the process (24 h validity) and never prints, logs or writes it."
)
COVERAGE = (
    "Coverage: the game routes answer from 2020-21 (Liga Femenina Endesa from its first game; Primera FEB from "
    "2021-03-12, so its earlier 2020-21 games answer 404, as do all earlier seasons). 2020-21 play-by-play is text "
    "only. The schedule pages go back to 1996-97 (``season=1997``). Segunda FEB and the youth and Tercera FEB groups "
    "share the same routes; ``group`` is the site's menu group (1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa)."
)
SEASON = "Season END year (2027 = 2026-27). ``feb_runtime`` sends the site's START-year key ``t = season - 1``."
SEASON_NOTE = (
    "Seasons are END years everywhere in this package (``season=2027`` is 2026-27); the site's own ``t`` is the "
    "START year, so the runtime sends ``t = season - 1`` (``t=2026`` is 2026-27)."
)
GROUP = (
    "Required. The site's menu group (integer): 1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa, 9 and 10 "
    "LF-2, 67 LF Challenge, 73 Copa de España, 74 Liga U. A group is not a competition id (see ``comp_id``); an "
    "unknown group raises NoDataError."
)
GAME = "Required. FEB game id (integer), e.g. 2535725; ``game_id`` in feb_schedule / feb_results / feb_overview."


def _game_path() -> Dict[str, Any]:
    return {
        "path_params": [{"name": "game_id", "type": "int", "required": True, "description": GAME}],
        "example_args": {"game_id": GAME_ID_FIXTURE},
    }


def _page_fields() -> Dict[str, Any]:
    return {
        "host": SITE,
        "extra_params": [
            {"name": "group", "query_key": "group", "type": "int", "required": True, "description": GROUP},
            {"name": "season", "query_key": "season", "type": "int", "required": True, "description": SEASON},
        ],
        "example_args": {"group": 1, "season": 2027},
    }


def _full(name: str) -> Dict[str, Any]:
    gz = FIXTURES / "full" / f"{name}.json.gz"
    text = (
        gzip.open(gz, "rt", encoding="utf-8").read()
        if gz.exists()
        else (FIXTURES / "full" / f"{name}.json").read_text("utf-8")
    )
    body = json.loads(text)
    body[REQUEST_KEY] = {"game_id": GAME_ID_FIXTURE}
    return body


def _capture(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


# short -> (spec path, wrapper path, parser, fixture loader, md sources, summary, fields)
Source = Tuple[str, str]  # (returns-md section, record-path prefix)
Entry = Tuple[str, str, Callable[..., Any], Callable[[], Dict[str, Any]], Tuple[Source, ...], str, Dict[str, Any]]
_BOX = "/BoxScore/{gameId}"
ENDPOINTS: Dict[str, Entry] = {
    "game_info": (
        "/TeamStats/{gameId}",
        "/TeamStats/{game_id}",
        P.parse_feb_game_info,
        lambda: _full("TeamStats"),
        (("TeamStats", "HEADER."), ("TeamStats", "HEADER.TEAM.[].")),
        "Game header: competition, tip-off, venue, officials, status, both teams and the score "
        "(every game route carries it; this wrapper reads the lightest, /TeamStats).",
        _game_path(),
    ),
    "period_scores": (
        "/TeamStats/{gameId}",
        "/TeamStats/{game_id}",
        P.parse_feb_period_scores,
        lambda: _full("TeamStats"),
        (("TeamStats", "HEADER.QUARTERS.QUARTER.[]."),),
        "Points each team scored in each period of a game.",
        _game_path(),
    ),
    "team_stats": (
        "/TeamStats/{gameId}",
        "/TeamStats/{game_id}",
        P.parse_feb_team_stats,
        lambda: _full("TeamStats"),
        (("TeamStats", "TEAMSTATS.TEAM.[]."), ("BoxScore", "BOXSCORE.TEAM.[].TOTAL.")),
        "Team totals of both teams in a game: shooting, rebounds, assists, turnovers, fouls.",
        _game_path(),
    ),
    "player_box": (
        _BOX,
        "/BoxScore/{game_id}",
        P.parse_feb_player_box,
        lambda: _full("BoxScore"),
        (("BoxScore", "BOXSCORE.TEAM.[].PLAYER.[]."),),
        "Per-player box score of a game: minutes, points, shooting splits, rebounds, assists, fouls and plus/minus.",
        _game_path(),
    ),
    "pbp": (
        "/KeyFacts/{gameId}",
        "/KeyFacts/{game_id}",
        P.parse_feb_pbp,
        lambda: _full("KeyFacts"),
        (("KeyFacts", "PLAYBYPLAY.LINES.[]."),),
        "Play-by-play of a game (KeyFacts): every shot, rebound, foul, substitution and timeout, "
        "in Spanish text with coded actions from 2025-26.",
        _game_path(),
    ),
    "shots": (
        "/ShotChart/{gameId}",
        "/ShotChart/{game_id}",
        P.parse_feb_shots,
        lambda: _full("ShotChart"),
        (("ShotChart", "SHOTCHART.SHOTS.[]."),),
        "Every field-goal attempt of a game with its court position (percent of a full-court image).",
        _game_path(),
    ),
    "leaders": (
        "/Ranking/{gameId}",
        "/Ranking/{game_id}",
        P.parse_feb_leaders,
        lambda: _full("Ranking"),
        (("Ranking", "RANKING.TEAMS.[].Puntos.[]."),),
        "Each team's top players of a game by points, rebounds, assists and fouls.",
        _game_path(),
    ),
    "overview": (
        "/overview",
        "/overview",
        P.parse_feb_overview,
        lambda: _full("overview"),
        (("overview", "OVERVIEW.COMPETITIONS.[].GAMES.[]."),),
        "Today's (or the latest) games of every live FEB competition, with scores and status.",
        {"example_args": {}},
    ),
    "schedule": (
        "/calendario.aspx",
        "/calendario.aspx",
        P.parse_feb_schedule,
        lambda: _capture("calendario"),
        (("calendario", "games.[]."), ("calendario", "")),
        "Full schedule of one group (phase) of a competition season: every round, game, result and team-season id.",
        _page_fields(),
    ),
    "results": (
        "/resultados.aspx",
        "/resultados.aspx",
        P.parse_feb_results,
        lambda: _capture("resultados"),
        (("resultados", "results.[]."), ("resultados", "next_round.[]."), ("resultados", "")),
        "Latest round's results and the next round's fixtures of a competition season.",
        _page_fields(),
    ),
    "standings": (
        "/resultados.aspx",
        "/resultados.aspx",
        P.parse_feb_standings,
        lambda: _capture("resultados"),
        (("resultados", "standings.[]."), ("resultados", "final_standings.[]."), ("resultados", "")),
        "League table of a competition season (live standings, or the final classification once it is over).",
        _page_fields(),
    ),
}

#: Descriptions of the columns the parsers add, re-key or translate (they win over the returns md).
LOCAL: Dict[str, str] = {
    "game_id": "FEB game id (one global sequence): the {gameId} of every LiveStats route and the p= of Partido.aspx. "
    "Game bodies do not print it, so it comes from the request; a frame parsed from a bare body has a null game_id.",
    "team_side": "Side of the game the row belongs to: home or away (the feed's TEAM[0] / TEAM[1], or its 1/2 and 0/1 "
    "side codes, normalised).",
    "team_id": "Team-season id (key space: FEB team registrations; a NEW id every season, e.g. HLA Alicante = 979989 in "
    "2025-26 and 1008687 in 2026-27). Joins the schedule's home_team_id / away_team_id; not a club or player id.",
    "team": "Team name as registered for the season (sponsor name, uppercase: 'HLA ALICANTE').",
    "team_code": "Federation team code, 8 digits as a string: the 6-digit club_code plus a 2-digit team suffix "
    "('03058600'). Null on 2020-21 games.",
    "club_code": "Federation club code, 6 digits as a string (the first 6 digits of team_code). Whether it is stable "
    "across seasons is unverified. Null on 2020-21 games.",
    "player_id": "FEB person id: stable across seasons and clubs (A. Jorda Rodriguez = 1676991 in 2025-26 and 2026-27). "
    "Joins the box score, play-by-play, shots and leaders; not the per-season licence_id.",
    "shirt_number": "Shirt number (dorsal) as a string, so '00' and '0' stay distinct. The shot feed names the shooter "
    "by this number.",
    "player": "Player name as 'I. SURNAME(S)', uppercase ('V. AGUILAR HERNANDEZ').",
    "starter": "True when the player was in the starting five (the page's I column), false when on the bench.",
    "captain": "True for the team captain. Null on 2020-21 games, where the feed does not send it.",
    "seconds_played": "Seconds on court (the feed's min, an integer count of seconds: 1064 = 17:44; a team row sums "
    "its players, 12000 = 200:00). Null on /TeamStats rows.",
    "period": "Period number (1-4 regulation; overtime numbering is unverified, no overtime game is in the samples).",
    "clock": "Game clock REMAINING in the period as 'mm:ss' (it counts down from 10:00).",
    "clock_seconds": "The clock as whole seconds remaining in the period (08:34 = 514), derived by the parser.",
    "x": "Court x position: percent (0-100) of the length of a horizontal full-court image, origin top-left. Teams "
    "change ends at half-time; metres (x/100 x 28) are an unverified inference.",
    "y": "Court y position: percent (0-100) of the width of a horizontal full-court image from the top edge; baskets "
    "sit near y = 50. Metres (y/100 x 15) are an unverified inference.",
    "comp_id": "Competition id (constant across seasons): 115 Primera FEB (LEB Oro), 118 Segunda FEB, 210 Liga "
    "Femenina Endesa, 214 LF Challenge, 218 LF-2, 111 Tercera FEB. Not the site's menu group.",
    "competition": "Competition name as printed that season, uppercase ('PRIMERA FEB'; 2020-21 'LIGA LEB ORO').",
    "status": "Game state code: 3 = finished, 1 = scheduled (seen 2026-10-12); the live codes are unverified.",
    "pts": "Points scored (the team's points on team rows; the final score when the clock is FINAL, else live).",
}

#: Per-wrapper descriptions where a column means something different.
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "game_info": {
        "group_label": "The game's group (grupo) label, trimmed: 'Único' = the single group, '\"A\"' = group A. It is "
        "the page's group label, not the site's menu group number.",
        "period": "Current or last period number; 4 for a finished regulation game.",
        "clock": "'FINAL' once the game is over, else the game clock 'mm:ss' remaining in the period.",
        "start_time": "Tip-off in Spanish local time (Europe/Madrid) as a naive datetime: the source carries no "
        "offset, so the zone is assumed.",
        "venue": "Venue (pabellon) name.",
        "address": "Venue street address, free text.",
        "referee1": "Crew chief (arbitro principal), 'SURNAMES, NAME'.",
        "referee2": "First umpire (arbitro auxiliar 1), 'SURNAMES, NAME'.",
        "referee3": "Second umpire (arbitro auxiliar 2), 'SURNAMES, NAME'; null when the game had two referees.",
        "status": "Game state code: 3 = finished (status_text FINISHED). Null on 2020-21 games.",
        "status_text": "Game state label in English ('FINISHED'). Null on 2020-21 games.",
        "home_team_id": "Home team-season id (key space: FEB team registrations, new every season).",
        "home_team": "Home team name for the season (sponsor name, uppercase).",
        "home_team_code": "Home federation team code, 8 digits as a string. Null on 2020-21 games.",
        "home_club_code": "Home federation club code, 6 digits as a string. Null on 2020-21 games.",
        "home_score": "Home points: the final score when clock is FINAL, else the live score.",
        "away_team_id": "Away team-season id (key space: FEB team registrations, new every season).",
        "away_team": "Away team name for the season (sponsor name, uppercase).",
        "away_team_code": "Away federation team code, 8 digits as a string. Null on 2020-21 games.",
        "away_club_code": "Away federation club code, 6 digits as a string. Null on 2020-21 games.",
        "away_score": "Away points: the final score when clock is FINAL, else the live score.",
    },
    "period_scores": {
        "home_points": "Points the home team scored IN this period (not cumulative).",
        "away_points": "Points the away team scored IN this period (not cumulative).",
    },
    "team_stats": {
        "min_formatted": "Time played as 'mm:ss'; '0:00' on /TeamStats rows (only box-score totals carry a value).",
        "fouls": "Team fouls committed in the game (equals pf on the samples).",
        "tc": "Shots of this team that were blocked (tapones en contra, page column TC). Null on /TeamStats rows.",
        "mt": "Dunks (mates, page column MT). Null on /TeamStats rows.",
        "val": "FEB efficiency rating (valoracion). Null on /TeamStats rows, filled on box-score totals.",
        "fastb": "Fast-break points. Null on every sampled game: the federation's stat crews do not fill it.",
        "ptsto": "Points off turnovers. Null on every sampled game: the federation's stat crews do not fill it.",
        "secpts": "Second-chance points. Null on every sampled game: the federation's stat crews do not fill it.",
        "bench": "Bench points. Null on every sampled game: the federation's stat crews do not fill it.",
        "paint": "Points in the paint. Null on every sampled game: the federation's stat crews do not fill it.",
        "pts": "Team points: the final score when the game is over, else the live score.",
    },
    "player_box": {
        "pllss": "Plus/minus while the player was on court (balance, page column +/-), signed integer.",
        "val": "FEB efficiency rating (valoracion): points + rebounds + assists + steals + blocks + fouls drawn - "
        "missed shots - turnovers - shots blocked - fouls committed.",
        "reb": "Total rebounds (the same count as rt).",
    },
    "pbp": {
        "play_num": "Play sequence number, 1 = the first play of the game; unique per game. The parser sorts by it "
        "(the feed lists newest first).",
        "period": "Period number of the play.",
        "team_side": "Side of the acting team: home (feed code 1) or away (2); null on period lines.",
        "team_id": "Team-season id of the acting team (new every season); null on period lines and 2020-21 games.",
        "player_id": "FEB person id of the actor (stable across seasons); null on team, period and 2020-21 lines.",
        "license_id": "Per-season player licence id of the actor (changes when the player re-registers; "
        "A. Jorda Rodriguez = 10901171 in 2025-26, 11266668 in 2026-27). Null on team and period lines and 2020-21 games.",
        "action": "Action code: shoot, fthrow, rebound, assist, recovery (steal), lose (turnover), blockshot, foul, "
        "subst, timeout, period. Null on 2020-21 games.",
        "text": "Play description in Spanish: '(TEAM) I. SURNAME: ACTION (running count)'; 2020-21 games use older "
        "wording and are text only.",
        "score_home": "Home score AFTER the play; filled on scoring plays only.",
        "score_away": "Away score AFTER the play; filled on scoring plays only.",
        "x": "Court x position of a shoot line: percent (0-100) of the length of a horizontal full-court image, "
        "origin top-left (the same scale as feb_shots). Null when the action has none and on 2020-21 games.",
        "y": "Court y position of a shoot line: percent (0-100) of the width from the top edge (the same scale as "
        "feb_shots). Null when the action has none and on 2020-21 games.",
        "log_time": "Wall-clock time the play was logged, Spanish local time, naive datetime. Null on 2020-21 games.",
        "log_time_utc": "The log time in UTC, naive datetime. Null on 2020-21 games.",
        **{
            f"log_param{n}": text
            for n, text in {
                1: "Action parameter 1 (string). Team and player actions: a pseudo-GUID '00000000-0000-000{side}-0000-"
                "{game id, 12 digits}' (side 1 home, 2 away); period lines: the period number.",
                2: "Action parameter 2 (string). The actor's licence id on shoot, fthrow, foul, rebound, assist, "
                "recovery and blockshot lines; period lines: '0' start, '1' end; lose: '0' or '1' (meaning unverified).",
                3: "Action parameter 3 (string). fthrow: '1' made, '0' missed; subst: the outgoing player's licence; "
                "foul: a foul-type code. Others unverified.",
                4: "Action parameter 4 (string). shoot and fthrow: '1' made, '0' missed; foul: the licence of the "
                "fouled player; lose: a licence (role unverified).",
                5: "Action parameter 5 (string); action-dependent, meaning unverified.",
                6: "Action parameter 6 (string). shoot: shot value '2' or '3'; fthrow: '1'; foul: free throws awarded.",
                7: "Action parameter 7 (string); action-dependent, meaning unverified.",
                8: "Action parameter 8 (string): a small integer on shoot, foul and lose lines (by distribution a "
                "zone or sub-type code); unverified.",
                9: "Action parameter 9 (string); blank or null in every sampled game.",
                10: "Action parameter 10 (string): '1' on almost every foul; meaning unverified.",
            }.items()
        },
    },
    "shots": {
        "period": "Period number of the attempt.",
        "clock": "Game clock REMAINING in the period at the shot, 'mm:ss' (the play-by-play line's time).",
        "team_side": "Side of the shooter: home (feed code 0) or away (feed code 1; play-by-play uses 1/2).",
        "team_id": "Team-season id of the shooter's team (from the shot chart's TEAM list); new every season.",
        "player_id": "FEB person id of the shooter, resolved from side + shirt number in the game's roster; null when "
        "that pair is not unique. The feed itself carries no player id.",
        "made": "True when the shot was made, false when missed. Free throws are not in the feed.",
    },
    "leaders": {
        "category": "Leader category: points, rebounds, assists or fouls (the feed's Puntos, Rebotes, Asistencias, "
        "Faltas).",
        "rank": "Place in the team's category list, 1 = the leader, in the order the feed lists them.",
        "value": "The player's count in the row's category (points, rebounds, assists or personal fouls).",
    },
    "overview": {
        "comp_code": "Season-specific competition edition code (e.g. 23473 = Primera FEB 2026-27).",
        "game_id": "FEB game id (one global sequence): the {gameId} of every LiveStats route.",
        "round_id": "Round id; 0 in every sample (the route does not fill it).",
        "stage": "Stage code as a string; '0' in every sample (meaning unverified).",
        "venue": "Name of the venue (pabellon) where the game is played.",
        "start_time": "Tip-off in Spanish local time as a naive datetime (the source has no offset).",
        "start_time_utc": "Tip-off in UTC as a naive datetime.",
        "period": "Current or last period (4 when finished).",
        "clock": "'FINAL' once finished, else the game clock ('00:00' before tip-off).",
        "status": "Game state code: 3 = finished, 1 = scheduled (seen 2026-10-12); the live codes are unverified.",
        "home_team_id": "Home team-season id (new every season; joins the schedule pages).",
        "home_team": "Home team name for the season.",
        "home_team_code": "Home federation team code, 8 digits as a string.",
        "home_club_code": "Home federation club code, 6 digits as a string.",
        "home_abbreviation": "Home team three-letter abbreviation ('PAL').",
        "home_score": "Home points (0 before tip-off).",
        "away_team_id": "Away team-season id (new every season; joins the schedule pages).",
        "away_team": "Away team name for the season.",
        "away_team_code": "Away federation team code, 8 digits as a string.",
        "away_club_code": "Away federation club code, 6 digits as a string.",
        "away_abbreviation": "Away team three-letter abbreviation.",
        "away_score": "Away points (0 before tip-off).",
        "period_scores": "Points per period as 'home-away' pairs joined by ';' ('16-16;19-16;10-21;14-14'); null "
        "before tip-off.",
    },
}
_PAGE_LOCAL = {
    "group": "The site's menu group (1 Primera FEB, 2 Segunda FEB, 4 Liga Femenina Endesa); not a competition id.",
    "season": "Season END year (2027 = 2026-27), the site's START-year key plus one.",
    "competition": "Competition name on the page header ('PRIMERA FEB').",
    "round": "Round number (jornada) the game belongs to.",
    "round_label": "Round heading as printed ('Jornada 4 16/10/2026').",
    "round_date": "Date of the round (the heading's date); played games carry no date of their own on this page.",
    "home_team_id": "Home team-season id (new every season; joins feb_overview and the game wrappers).",
    "home_team": "Home team name, uppercase.",
    "away_team_id": "Away team-season id (new every season).",
    "away_team": "Away team name, uppercase.",
    "result_text": "Result cell verbatim: 'home-away' points when played; before the game the calendar shows the Spanish "
    "weekday, date and time and the results page shows '*-*'.",
    "home_score": "Home points; null before the game.",
    "away_score": "Away points; null before the game.",
    "status": "played when the result cell holds a score, scheduled otherwise.",
    "weekday": "English weekday of a scheduled game (translated from the Spanish cell); null for played games.",
    "date": "Game date; null for played games on the schedule page (use round_date).",
    "time": "Tip-off 'HH:MM', Spanish local time; null for played games on the schedule page.",
    "section": "Which table the row comes from: results (the latest round) or next_round (the following fixtures).",
    "round_title": "Title of the table as printed ('Resultados Jornada 3(09/10/2026)').",
    "table": "live = the in-season league table; final = the final classification once the season is over.",
    "position": "Rank in the table (Po).",
    "team_id": "Team-season id (new every season; joins the schedule and game wrappers).",
    "team": "Team name as printed in the table (sponsor name, uppercase).",
    "played": "Games played (PJ); null in the final classification.",
    "won": "Games won (PG); null in the final classification.",
    "lost": "Games lost (PP); null in the final classification.",
    "points_for": "Points scored (PF); null in the final classification.",
    "points_against": "Points conceded (PC); null in the final classification.",
    "point_diff": "Point difference (DIF), signed; null in the final classification.",
    "win_pct": "Win percentage, 0-100 (the page's '100,0%', decimal comma converted).",
    "streak": "Current streak as a signed integer: +3 = three wins in a row, -2 = two losses.",
    "streak_text": "The streak cell verbatim ('+3').",
    "comp_id": "Competition id (CompID) read from the page; ties the site group to the API (group 1 -> 115, 2 -> 118, "
    "4 -> 210).",
}

_HEADING = re.compile(r"^## `https://[^/]+(/\S*)`")
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")
_SECTIONS = {
    "BoxScore": "/BoxScore/",
    "KeyFacts": "/KeyFacts/",
    "ShotChart": "/ShotChart/",
    "TeamStats": "/TeamStats/",
    "Ranking": "/Ranking/",
    "overview": "/overview",
    "calendario": "/calendario.aspx",
    "resultados": "/resultados.aspx",
}


def _md_rows(path: Path) -> Dict[str, Dict[str, str]]:
    """``{section: {record path: description}}`` from the returns md (unobserved rows skipped)."""
    out: Dict[str, Dict[str, str]] = {}
    section: Optional[str] = None
    for line in path.read_text(encoding="utf-8").splitlines():
        h = _HEADING.match(line)
        if h:
            section = next((k for k, frag in _SECTIONS.items() if frag in h.group(1)), None)
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
    if short in ("schedule", "results", "standings") and col in _PAGE_LOCAL:
        return _PAGE_LOCAL[col]
    if col in LOCAL:
        return LOCAL[col]
    for section, prefix in entry[4]:
        for path, text in md.get(section, {}).items():
            leaf = path[len(prefix) :]
            if path.startswith(prefix) and "." not in leaf and underscore(leaf) == col:
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
        "returns_schema": f"native/feb/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("feb", SPEC)
    spec = load_spec(refs, SPEC)
    differs = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if differs:
        raise SystemExit(f"feb: spec paths and ENDPOINTS differ: {sorted(differs)}")
    assert set(P.SPECS) == set(ENDPOINTS), "every parser spec needs an endpoint and the reverse"
    doc = {
        "api": "feb",
        "host": HOST,
        "name_pattern": "feb_{short}",
        "module": "feb",
        "parser_module": "basketball.feb.feb_parsers",
        "getter_module": "sportsdataverse.basketball.feb.feb_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (pbp, shots, schedule) name other families
        "schema_prefix": "feb_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw body (``dict``): the LiveStats envelope (all ten blocks, only the route's filled) "
            "or the schedule page record, plus ``_request`` (the game id, or group and season asked for)",
            "raises": [
                "sportsdataverse.errors.NoDataError: the FEB has nothing for the request (a game the API does not "
                "serve, an unknown group, a schedule page with no games, an empty overview window).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for "
                "(a rejected bearer token, an error or changed envelope, a page for another season or group).",
            ],
            "notes": [LEGAL, UNOFFICIAL, COVERAGE, SEASON_NOTE],
            "see_also": [
                {
                    "name": "Baloncesto en vivo",
                    "url": "https://baloncestoenvivo.feb.es",
                    "note": "the FEB site whose pages and widget API these wrappers read",
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
    write_yaml(ROOT / "tools/codegen/endpoints/feb.yaml", doc)

    md = _md_rows(refs / "feb-returns.md")
    schema_dir = ROOT / "tools/codegen/schemas/native/feb"
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        cols: List[Dict[str, str]] = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        block = snippet.setdefault(f"feb_{short}", {})
        for col in cols:
            text = _describe(short, col["name"], entry, md).strip()
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"feb_{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"feb_{short}", "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit(f"feb: empty or filler descriptions (add them to LOCAL / LOCAL_BY): {bad}")
    write_yaml(SNIPPET, snippet)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"feb: {len(ENDPOINTS)} endpoints, {sum(map(len, snippet.values()))} descriptions -> {SNIPPET.name}")


if __name__ == "__main__":
    main()
