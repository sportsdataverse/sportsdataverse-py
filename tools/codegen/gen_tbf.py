"""Generate the ``tbf`` flat-API endpoint YAML + returns-schemas (Turkish Basketball Federation app API).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_lkl.py`` (itself ``gen_asa.py`` with
the shared plumbing in ``gen_soccer_common.py``). Reads the frozen spec and ``tbf-returns.md`` from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/tbf.yaml`` -- 32 wrappers over 29 of the spec's 33 routes. One route can feed several
  frames (``mac-istatistik`` feeds ``player_box`` and ``team_box``, ``mac-ozet`` feeds ``period_scores`` and
  ``highlights``, ``mac-header`` feeds ``game_header`` and ``game_players``) and the nine ``/Player/get-*-leaders``
  routes share one parser. :data:`ENDPOINTS` is the source of truth; :data:`NOT_WRAPPED` names the four routes left
  out and why. League-season wrappers take ``season`` (END year) and ``competition`` (``bsl`` / ``tbl`` / ``kbsl``),
  which ``tbf_runtime`` maps to TBF's internal season and activity ids; game wrappers take ``match_id``.
* ``tools/codegen/schemas/native/tbf/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser run
  over its committed fixture. The game, standings, team-statistics and points-leaders wrappers use the FULL bodies in
  ``tests/fixtures/tbf/full/``; the others use the refs captures in ``tests/fixtures/tbf/captures/``. Each parser
  emits a fixed column list with dtypes pinned by name (``tbf_parsers.SPECS``). The schemas carry
  ``description: ''`` (descriptions never live in ``schemas/**``).
* ``tools/codegen/notes/tbf_descriptions.yaml`` -- ``{tbf_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (Integration merges it). Text comes from :data:`LOCAL_BY` (a column whose meaning
  differs by wrapper), :data:`LOCAL` (derived and renamed columns), then the ``tbf-returns.md`` row of the source
  field, whose Turkish names and values are already glossed in English. An empty, short or filler text fails the run.
* ``tests/fixtures/tbf/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_tbf.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.tbf import tbf_parsers as P
from sportsdataverse.basketball.tbf.tbf_runtime import COMPETITIONS, REQUEST_KEY

HOST = "https://miniappapi.tbf.org.tr/webapi-service/api"
SPEC = "tbf.openapi.yaml"
RETURNS = "tbf-returns.md"
FIXTURES = ROOT / "tests" / "fixtures" / "tbf"
SCHEMA_PREFIX = "tbf_"
#: Column descriptions, keyed ``tbf_<short>`` -> column; merged into manual_column_descriptions.yaml.
NOTES = ROOT / "tools" / "codegen" / "notes" / "tbf_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

#: Spec routes that have no wrapper, and why.
NOT_WRAPPED = {
    "/Kupalar/maclar-filtreler": "cup competition filter lists (seasons, teams, cities): navigation, not game data",
    "/Match/box-score": "the short box score: a strict subset of /Match/mac-istatistik, which tbf_player_box serves",
    "/Match/get-match-report-with-players": "a team comparison report whose totals tbf_team_box already carries",
    "/Team/get-team-detail-main": "season totals plus club contact details (redacted in the refs captures)",
}

UNOFFICIAL = (
    "Official TBF app API, unofficial for SportsDataverse: the backend of the Turkish Basketball Federation's mobile "
    "app (miniappapi.tbf.org.tr), which serves BSL, TBL, KBSL and the other TBF leagues. It needs no key; robots.txt "
    "answers 404 on the host, and ``tbf_runtime`` spaces requests at least 1 s apart. SportsDataverse has not ruled on "
    "TBF's terms, so payloads are not redistributed as release assets. The vendor's origin hosts (nikayazilim.com) "
    "serve the same data without Cloudflare but are deliberately not used, redirects are never followed, and "
    "www.tbf.org.tr's interactive Turnstile is neither used nor automated."
)
COVERAGE = (
    "Coverage: season and competition are parameters (``bsl`` Basketbol Super Ligi, back to 1966-67 in the league "
    "list; ``tbl`` second tier; ``kbsl`` women, both from 2015-16). Game wrappers serve FINISHED games only (an "
    "unfinished game raises AssetFetchError). Box scores exist from 2006-07 (no starters flagged that season); "
    "play-by-play and shots from 2025-26 only, an earlier finished game raises NoDataError. Every game carries a "
    "Genius LiveStats id (``genius_id``, joins the ``genius`` family) from 2018-19 on some seasons and on every "
    "game of 2025-26 and later. Birth dates, contact details, photos and logos are dropped. Names and category "
    "labels are Turkish; the column descriptions give the English gloss."
)
SEASON = (
    "Required. Season END year (2027 = the 2026-2027 season). TBF keys a league-season by internal season and "
    "activity ids; ``tbf_runtime`` reads them from the league's season list by the END year of the label. A year the "
    "list lacks raises NoDataError."
)
COMPETITION = (
    "TBF league: bsl (Basketbol Super Ligi), tbl (second tier) or kbsl (women's Super Ligi). The top flight was "
    "named TBL until 2007-08 and is listed under bsl."
)
WEEK = (
    "Week (hafta) number as returned by tbf_weeks, e.g. 1; playoff rounds continue the numbering (31-33 in "
    "2025-26). Default: the latest week for standings, every week for the schedule."
)
MATCH = (
    "Required. TBF game id (integer), e.g. 346279; match_id in tbf_schedule. One global sequence across leagues "
    "and seasons. Only a finished game is served."
)
TEAM = (
    "Required. Team-season id (team registration), e.g. 265995 = Tofas in BSL 2026-27; team_process_id in "
    "tbf_teams and tbf_schedule. New for every club in every league-season; not the stable club_id."
)


def _season() -> Dict[str, Any]:
    return {"name": "season", "query_key": "season", "type": "int", "required": True, "description": SEASON}


def _competition() -> Dict[str, Any]:
    return {
        "name": "competition",
        "query_key": "competition",
        "type": "str",
        "default": "bsl",
        "choices": list(COMPETITIONS),
        "description": COMPETITION,
    }


def _league_args(**extra: Any) -> Dict[str, Any]:
    ex = {"season": 2027, "competition": "bsl", **extra}
    return {"extra_params": [_season(), _competition()], "example_args": ex}


def _game_args() -> Dict[str, Any]:
    p = {"name": "match_id", "query_key": "match_id", "type": "int", "required": True, "description": MATCH}
    return {"extra_params": [p], "example_args": {"match_id": 346279}}


def _team_args() -> Dict[str, Any]:
    t = {
        "name": "team_process_id",
        "query_key": "team_process_id",
        "type": "int",
        "required": True,
        "description": TEAM,
    }
    return {"extra_params": [_season(), _competition(), t], "example_args": {"season": 2027, "team_process_id": 265995}}


def _full(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _cap(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


GAME_REQ = {"match_id": 346279, "home_team_process_id": 265995, "away_team_process_id": 265993}
LEAGUE_REQ = {"season": 2027, "competition": "bsl"}


def _stamped(body: Dict[str, Any], **req: Any) -> Dict[str, Any]:
    """A fixture body stamped with ``_request`` the way ``tbf_runtime`` stamps a live one."""
    return {**copy.deepcopy(body), REQUEST_KEY: req}


def _fix_game(name: str) -> Callable[[], Dict[str, Any]]:
    return lambda: _stamped(_full(name), **GAME_REQ)


def _fix_cap(name: str, **req: Any) -> Callable[[], Dict[str, Any]]:
    return lambda: _stamped(_cap(name), **req)


Source = Tuple[str, ...]  # md key prefixes tried in order
# short -> (route, parser table key, fixture loader, md prefixes, summary, fields)
Entry = Tuple[str, str, Callable[[], Dict[str, Any]], Source, str, Dict[str, Any]]
ENDPOINTS: Dict[str, Entry] = {
    "seasons": (
        "/Season/get-seasons",
        "seasons",
        _fix_cap("Season__get-seasons"),
        ("data.[].",),
        "The 20 most recent TBF seasons, domestic and international (a server cap), newest first.",
        {"example_args": {}},
    ),
    "leagues": (
        "/League/get-leagues-for-filter",
        "leagues",
        _fix_cap("League__get-leagues-for-filter", **LEAGUE_REQ),
        ("data.[].",),
        "The featured leagues (league-season activities) of one TBF season: BSL, TBL, KBSL and the others.",
        {"extra_params": [_season()], "example_args": {"season": 2027}},
    ),
    "league_seasons": (
        "/League/get-leagues-and-seasons-by-prefix",
        "league_seasons",
        _fix_cap("League__get-leagues-and-seasons-by-prefix", competition="bsl"),
        ("data.[].",),
        "Every season of one TBF league (bsl back to 1966-67) with its activity and season ids.",
        {"extra_params": [_competition()], "example_args": {"competition": "bsl"}},
    ),
    "weeks": (
        "/League/get-league-weeks",
        "weeks",
        _fix_cap("League__get-league-weeks", **LEAGUE_REQ, activity_id=22214),
        ("data.[].",),
        "The weeks (rounds) of one league-season, playoff rounds included.",
        _league_args(),
    ),
    "standings": (
        "/League/get-standings-table",
        "standings",
        lambda: _stamped(_full("standings"), **LEAGUE_REQ),
        ("data.[].standings.[].", "data.[]."),
        "League table of one league-season after a week (default the latest), one row per team and group.",
        {
            "extra_params": [
                _season(),
                _competition(),
                {"name": "week", "query_key": "week", "type": "int", "description": WEEK},
            ],
            "example_args": {"season": 2027, "competition": "bsl"},
        },
    ),
    "schedule": (
        "/Match/get-all-matches-for-filter",
        "schedule",
        _fix_cap("Match__get-all-matches-for-filter", **LEAGUE_REQ),
        ("data.[].",),
        "Schedule and results of one league-season: the games of one week, or of every week.",
        {
            "extra_params": [
                _season(),
                _competition(),
                {"name": "week", "query_key": "week", "type": "int", "description": WEEK},
            ],
            "example_args": {"season": 2027, "competition": "bsl", "week": 1},
        },
    ),
    "daily_matches": (
        "/Match/get-daily-matches",
        "daily_matches",
        _fix_cap("Match__get-daily-matches"),
        ("data.[].matches.[].", "data.[]."),
        "Every TBF game played on one date, grouped by competition, optionally for one league-season.",
        {
            "extra_params": [
                {
                    "name": "match_date",
                    "query_key": "match_date",
                    "type": "str",
                    "required": True,
                    "description": "Required. Game date 'YYYY-MM-DD' (Turkey local date), e.g. '2026-10-11'.",
                },
                {
                    "name": "season",
                    "query_key": "season",
                    "type": "int",
                    "description": SEASON.replace("Required. ", "Optional, with competition: ")
                    + " Default: games of every competition.",
                },
                {
                    "name": "competition",
                    "query_key": "competition",
                    "type": "str",
                    "choices": list(COMPETITIONS),
                    "description": COMPETITION + " Optional, with season. Default: every competition.",
                },
            ],
            "example_args": {"match_date": "2026-10-11"},
        },
    ),
    "game_header": (
        "/Match/mac-header",
        "game_header",
        _fix_game("header"),
        ("data.",),
        "Game header of a finished game: teams, coaches, final score, officials, venue and the Genius LiveStats id.",
        _game_args(),
    ),
    "game_players": (
        "/Match/mac-header",
        "game_players",
        _fix_game("header"),
        ("data.players.[].",),
        "Game roster from the header: every listed player of both sides (names and shirt numbers, no usable ids).",
        _game_args(),
    ),
    "player_box": (
        "/Match/mac-istatistik",
        "player_box",
        _fix_game("stats"),
        ("data.homeTopFive.[].",),
        "Player box score of a finished game: minutes, shooting splits, rebounds, assists, +/- and efficiency.",
        _game_args(),
    ),
    "team_box": (
        "/Match/mac-istatistik",
        "team_box",
        _fix_game("stats"),
        ("data.homeTeamTotal.", "data.homeTeamStats.", "data.homeTopFive.[]."),
        "Team totals of a finished game with the team-credited rebounds, turnovers and fouls.",
        _game_args(),
    ),
    "pbp": (
        "/Match/game-flow",
        "pbp",
        _fix_game("flow"),
        ("data.events.[].",),
        "Play-by-play of a finished game (2025-26 onward), in game order, with the running score and an English gloss.",
        _game_args(),
    ),
    "shots": (
        "/Match/shot-chart",
        "shots",
        _fix_game("shots"),
        ("data.shotInfos.[].",),
        "Every field-goal attempt of a finished game (2025-26 onward) with its court position.",
        _game_args(),
    ),
    "period_scores": (
        "/Match/mac-ozet",
        "period_scores",
        _fix_game("summary"),
        ("data.homeTeamScore.[].",),
        "Points each side scored in each period of a finished game, overtime included.",
        _game_args(),
    ),
    "highlights": (
        "/Match/mac-ozet",
        "highlights",
        _fix_game("summary"),
        ("data.matchHighlightStatistics.[].",),
        "Team comparison of a finished game: rebounds, paint points, fast-break points, lead changes and more.",
        _game_args(),
    ),
    "game_leaders": (
        "/Match/mac-leader",
        "game_leaders",
        _fix_game("leaders_game"),
        ("data.categories.[].leaders.[].", "data.categories.[]."),
        "Game leaders of a finished game per statistic category (top five each).",
        _game_args(),
    ),
    "players": (
        "/Player/get-player-list",
        "players",
        _fix_cap("Player__get-player-list", **LEAGUE_REQ),
        ("data.[].",),
        "Every player registered in one league-season (birth dates are not returned).",
        _league_args(),
    ),
    "player_career": (
        "/Player/get-player-detail-profile-and-career-info/{player_id}",
        "player_career",
        _fix_cap("Player__get-player-detail-profile-and-career-info", player_id=196413),
        ("data.careerInfo.[].", "data.playerProfile."),
        "One player's career: a row per season and competition with totals (birth date is not returned).",
        {
            "path_params": [
                {
                    "name": "player_id",
                    "type": "int",
                    "required": True,
                    "description": "Required. TBF person id, e.g. 196413; person_id in tbf_players and the box score.",
                }
            ],
            "example_args": {"player_id": 196413},
        },
    ),
    "teams": (
        "/Team/get-teams-by-leauge",
        "teams",
        _fix_cap("Team__get-teams-by-leauge", **LEAGUE_REQ),
        ("data.[].",),
        "Every team registered in one league-season with its team-season id (team_process_id) and club id.",
        _league_args(),
    ),
    "team_player_stats": (
        "/Team/get-team-detail-statistic",
        "team_player_stats",
        lambda: _stamped(_full("team_stats"), **LEAGUE_REQ, team_process_id=265995),
        ("data.playersTotal.[].", "data.playersAverage.[]."),
        "Season player statistics of one team-season: totals, percentages and per-game averages.",
        _team_args(),
    ),
    "team_games": (
        "/Team/get-team-detail-matches-by-season-and-league",
        "team_games",
        _fix_cap("Team__get-team-detail-matches-by-season-and-league", **LEAGUE_REQ, team_process_id=265995),
        ("data.maclar.[].",),
        "Games and results of one team-season, scheduled games included.",
        _team_args(),
    ),
    "team_roster": (
        "/Team/get-team-active-player-list-by-season-and-league",
        "team_roster",
        _fix_cap("Team__get-team-active-player-list-by-season-and-league", **LEAGUE_REQ, team_process_id=265995),
        ("data.[].",),
        "Roster of one team-season with games, minutes and per-game points and efficiency (birth dates dropped).",
        _team_args(),
    ),
    "team_staff": (
        "/Team/get-team-detail-team-staff-by-season-and-league",
        "team_staff",
        _fix_cap("Team__get-team-detail-team-staff-by-season-and-league", **LEAGUE_REQ, team_process_id=265995),
        ("data.[].",),
        "Coaching and club staff of one team-season.",
        _team_args(),
    ),
}
#: The nine season-leader routes: one parser, one table.
LEADER_ROUTES = {
    "points": "point",
    "assists": "asist",
    "blocks": "blok",
    "rebounds": "ribound",
    "steals": "top-calma",
    "minutes": "duration",
    "threes": "3Point",
    "turnovers": "top-kaybi",
    "efficiency": "mvp",
}
_LEADER_FIXTURE = {
    "points": lambda cat: _stamped(_full("leaders_points"), **LEAGUE_REQ, category=cat),
    **{
        cat: (lambda cat, route=route: _stamped(_cap(f"Player__get-{route}-leaders"), **LEAGUE_REQ, category=cat))
        for cat, route in LEADER_ROUTES.items()
        if cat != "points"
    },
}
_LEADER_WHAT = {
    "points": "points",
    "assists": "assists",
    "blocks": "blocks",
    "rebounds": "rebounds",
    "steals": "steals",
    "minutes": "minutes played",
    "threes": "three-point field goals made",
    "turnovers": "turnovers",
    "efficiency": "efficiency rating",
}
for _cat, _route in LEADER_ROUTES.items():
    ENDPOINTS[f"leaders_{_cat}"] = (
        f"/Player/get-{_route}-leaders",
        "leaders",
        (lambda cat=_cat: _LEADER_FIXTURE[cat](cat)),
        ("data.sum.[].",),
        f"Season leaders in {_LEADER_WHAT[_cat]}: the server's top 20 by season total and by per-game average.",
        _league_args(),
    )

#: Descriptions of the columns the parsers add, re-key or translate (they win over the returns md).
LOCAL: Dict[str, str] = {
    "match_id": "TBF game id (key space: TBF games, ONE global sequence across leagues and seasons: 62552 in 2006-07, "
    "346279 in 2026-27). The match_id argument of every game wrapper; the routes echo it as a string or null, the "
    "runtime stamps the integer asked for.",
    "genius_id": "Genius Sports LiveStats match id (TBF spells it genuisId); joins the genius family. Null when TBF "
    "stored none: every game before 2018-19, many 2021-25 games and games not yet assigned one.",
    "season": "Season END year (2027 = the 2026-2027 season), read from the 'YYYY-YYYY' label or the request; the sdv "
    "season key. Not the internal TBF season id (season_id).",
    "competition": "TBF league the row belongs to: bsl, tbl or kbsl (the request's competition, or the league prefix "
    "the API reports).",
    "season_id": "Internal TBF season id (key space: TBF seasons, global across leagues: 174 = 2026-2027, 172 = "
    "2025-2026; the odd ids are international UA- seasons). Never an sdv key: use season (END year).",
    "season_label": "Season label as TBF writes it, 'YYYY-YYYY' (start and end year).",
    "activity_id": "Activity id (faaliyet = activity; key space: TBF league-SEASONS, new every season: BSL 2026-27 = "
    "22214, BSL 2025-26 = 20728, TBL 2026-27 = 22215, KBSL 2026-27 = 22209). Cast to an integer from the 22214 / "
    "22214.0 the API sends.",
    "team_process_id": "Team-season id (teamProcessId = takimIslemId, a team REGISTRATION; key space: TBF team "
    "registrations, a NEW id per club per league-season: Tofas = 265995 in BSL 2026-27). The team_process_id "
    "argument of the team wrappers; not the stable club_id.",
    "club_id": "Stable club id (takimID / teamId of the team lists; key space: TBF clubs, the same across seasons and "
    "leagues: Tofas = 13). Not the team-season id (team_process_id).",
    "person_id": "TBF person id (kisi = person; key space: TBF persons, stable across seasons and clubs: Yigitcan "
    "Saybir = 196413). Equals the player_id of the box scores and the player_id of tbf_player_career.",
    "player_id": "TBF person id (key space: TBF persons, stable across seasons and clubs: 196413). The same value as "
    "person_id in the player and team tables and the player_id argument of tbf_player_career.",
    "player_name": "Player name as TBF prints it for that route (title case, Turkish letters kept).",
    "jersey_number": "Shirt number as a string, so '0' and '00' stay distinct.",
    "side": "Side of the game the row belongs to: home or away (from the route's isHome flag or home / away block).",
    "seconds_played": "Time on court in seconds, parsed from the 'mm:ss' or 'hh:mm:ss' text the API sends (team "
    "totals sum their players, so 200:00 = 12000).",
    "games": "Games played (macSayisi = game count).",
    "week": "Week (hafta) label or number: 'N. Hafta' on schedule rows, the number on standings rows, a playoff round "
    "name later in the season.",
    "tipoff": "Tip-off as a naive timestamp in Turkey local time (Europe/Istanbul, UTC+3); the API sends no offset.",
    "has_overtime": "Overtime flag (uzatma = overtime). Reliability unverified: false on every one of 64 BSL 2025-26 "
    "games checked.",
    "status_id": "Game status code: 1 = played (Oynandi), 4 = not played (Oynanmadi), 19 = fixture without a date "
    "and time. Cast to an integer from the 1 / 1.0 the API sends.",
    "period": "Period number: 1-4 regulation; overtime numbering is unverified (5 is expected for the first).",
    "period_type": "Period type code: 1 = regulation, 2 = overtime (from the app code; no overtime game in the "
    "samples).",
    "points": "Points scored in the game (or season) the row covers.",
    "rank": "Rank within the list, 1 = best (the API's own order).",
    "category": "Statistic category: the leaderCategory code (Turkish, e.g. VerimlilikPuani = efficiency rating) on "
    "game leaders, the wrapper's statistic (points, assists, ...) on season leaders.",
    "x": "Court x position: integer percent (0-100) of the LENGTH of a horizontal full court from the LEFT baseline "
    "(baskets near 5 and 95); teams change ends at half-time. Metres (x / 100 x 28) are an inference.",
    "y": "Court y position: integer percent (0-100) of the court WIDTH from the BOTTOM sideline (baskets near 50), "
    "verified from the app's drawing code (top = 100 - y). Metres (y / 100 x 15) are an inference.",
}

#: Per-wrapper descriptions where a column means something different or is derived.
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "seasons": {"international": "True for the international UA- seasons (odd season ids); false for domestic ones."},
    "league_seasons": {
        "url": "League-season slug, '<prefix>-<YYYY>-<YYYY>' (bsl-2026-2027).",
        "activity_name": "League-season name as TBF prints it ('BSL 2026-2027'; the top flight reads 'TBL 2006-2007' "
        "until 2007-08).",
    },
    "weeks": {
        "season": "Season END year of the request (2027 = 2026-2027).",
        "competition": "TBF league of the request: bsl, tbl or kbsl.",
        "activity_id": "Activity id of the request's league-season (key space: TBF league-seasons).",
        "phase": "Phase code (devre = half / phase): 1 = first half of the regular season, 2 = second half, 3 = "
        "playoff quarter-final, 4 = semi-final, 5 = final.",
        "phase_name": "Phase name in Turkish on playoff weeks (Playoff Final = final); null on regular-season weeks.",
        "label": "Week label: 'Hafta N' (week N) or the playoff round name.",
        "week": "Week number as a string; the value the week argument of tbf_standings and tbf_schedule takes.",
        "is_current": "True on the current (or last played) week; exactly one row.",
    },
    "standings": {
        "group": "Group name (grup); empty for a single-group league.",
        "week": "The week the table is computed after (hafta); equals the week argument, else the latest.",
        "rank": "League rank (sira) after that week.",
        "played": "Games played (o = oynanan).",
        "won": "Games won (g = galibiyet).",
        "lost": "Games lost (m = maglubiyet).",
        "points_for": "Points scored (a = atilan).",
        "points_against": "Points conceded (y = yenilen).",
        "points": "League points (puan): 2 for a win, 1 for a loss.",
        "head_to_head_points": "Head-to-head league points among teams tied on points (icP = ic puan).",
        "head_to_head_diff": "Head-to-head point difference among teams tied on points (icA = ic averaj).",
        "total_diff": "Total point difference over all games (toA = toplam averaj).",
        "excluded": "Exclusion marker (ihrac = expulsion); 0 for teams still in the league.",
    },
    "schedule": {
        "home_score": "Home points (null before the game; the API sends '' ).",
        "away_score": "Away points (null before the game; the API sends '' ).",
        "week": "Week label: 'N. Hafta' (week N) or the playoff round ('Playoff Final').",
        "phase": "Phase code as an integer: 1 = first half of the regular season; see tbf_weeks.",
        "province": "Province (il) of the venue, uppercase Turkish ('ISTANBUL').",
        "gender": "Gender in Turkish: Erkek = men, Kadin = women.",
    },
    "daily_matches": {
        "group_name": "Competition group the app shows the game under (grup adi).",
        "home_score": "Home points (null before the game).",
        "away_score": "Away points (null before the game).",
        "status": "Game status text in Turkish: Oynandi = played, Oynanmadi = not played.",
        "gender": "Gender in Turkish when sent; empty on get-daily-matches, so null.",
        "province": "Province (il) of the venue when sent, uppercase Turkish; null otherwise.",
    },
    "game_header": {
        "home_team_process_id": "Home team-season id (key space: TBF team registrations; the header calls it "
        "homeTeamProcessId).",
        "away_team_process_id": "Away team-season id (key space: TBF team registrations).",
        "home_team": "Home team name as registered for that season, uppercase with Turkish letters.",
        "away_team": "Away team name as registered for that season, uppercase with Turkish letters.",
        "home_coach": "Home head coach name, uppercase.",
        "away_coach": "Away head coach name, uppercase.",
        "home_score": "Home final score.",
        "away_score": "Away final score.",
        "referee": "Main referee's name (names only: no contact or birth data is returned).",
        "first_assistant_referee": "First assistant referee's name.",
        "second_assistant_referee": "Second assistant referee's name.",
        "commissioner": "Commissioner's (temsilci = representative) name.",
        "venue": "Venue name (location) as TBF prints it.",
        "attendance": "Spectator count (matchSpectatorCount); 0 when not recorded.",
        "status": "Game status text in Turkish: Oynandi = played. Only finished games are served.",
        "season_label": "Season label 'YYYY-YYYY' (seasonName).",
        "season": "Season END year, read from the season label (seasonName).",
        "tipoff": "Tip-off as a naive timestamp in Turkey local time (Europe/Istanbul, UTC+3).",
        "broadcast_channel": "Broadcaster (matchBroadcastChannel), e.g. beINSports, or null.",
    },
    "game_players": {
        "team": "Team name for the roster row; empty in the samples, so null.",
        "player_name": "Player name as the header roster lists it.",
        "is_starter": "True for a starter in the header roster (isStarter).",
        "is_active": "True when the player is flagged active in the header (isActive).",
    },
    "player_box": {
        "team_process_id": "Team-season id of the player's side (from the game header; key space: TBF team "
        "registrations).",
        "is_starter": "True for the starting five (homeTopFive / awayTopFive block); none are flagged in 2006-07.",
        "fg_made": "Field goals made, 2PT + 3PT (successfulShots).",
        "fg_attempted": "Field goals attempted, 2PT + 3PT (shotAttempts).",
        "fg_pct": "Field-goal percentage 0-100, rounded to an integer by the API.",
        "fg2_made": "Two-point field goals MADE (the API calls it twoPointShots; attempts are fg2_attempted).",
        "fg2_attempted": "Two-point field goals attempted (twoPointShotAttempts).",
        "fg2_pct": "Two-point percentage 0-100, an integer.",
        "fg3_made": "Three-point field goals MADE (the API calls it threePointShots).",
        "fg3_attempted": "Three-point field goals attempted (threePointShotAttempts).",
        "fg3_pct": "Three-point percentage 0-100, an integer.",
        "ft_made": "Free throws MADE (serbest atis; the API calls it freeThrows).",
        "ft_attempted": "Free throws attempted (freeThrowAttempts).",
        "ft_pct": "Free-throw percentage 0-100, an integer.",
        "fouls_drawn": "Fouls drawn (rivalFoul = foul committed by the opponent).",
        "blocks_received": "Own shots blocked by an opponent (blocksReceived).",
        "plus_minus": "Plus/minus while on court (plusMinus); null when the API sends ''.",
        "efficiency": "Efficiency rating (verimlilik puani, PIR): points + rebounds + assists + steals + blocks + "
        "fouls drawn - missed shots - turnovers - fouls. An integer on player rows.",
    },
    "team_box": {
        "team_process_id": "Team-season id of the side (from the game header; key space: TBF team registrations).",
        "efficiency": "Team efficiency rating (verimlilik puani): the players' ratings summed; a number on team rows.",
        "fg_made": "Team field goals made, 2PT + 3PT.",
        "fg_attempted": "Team field goals attempted, 2PT + 3PT.",
        "fg_pct": "Team field-goal percentage 0-100, rounded to an integer by the API.",
        "fg2_made": "Team two-point field goals MADE (the API calls it twoPointShots).",
        "fg2_attempted": "Team two-point field goals attempted.",
        "fg2_pct": "Team two-point percentage 0-100, an integer.",
        "fg3_made": "Team three-point field goals MADE (the API calls it threePointShots).",
        "fg3_attempted": "Team three-point field goals attempted.",
        "fg3_pct": "Team three-point percentage 0-100, an integer.",
        "ft_made": "Team free throws MADE.",
        "ft_attempted": "Team free throws attempted.",
        "ft_pct": "Team free-throw percentage 0-100, an integer.",
        "fouls_drawn": "Fouls drawn by the team (rivalFoul).",
        "blocks_received": "Team shots blocked by the opponent.",
        "team_off_rebounds": "Team offensive rebounds not credited to a player (homeTeamStats).",
        "team_def_rebounds": "Team defensive rebounds not credited to a player.",
        "team_rebounds": "Team rebounds not credited to a player (offensive + defensive).",
        "team_turnovers": "Team turnovers not credited to a player.",
        "team_fouls": "Team fouls not credited to a player (bench / coach technicals); 0 in the samples.",
    },
    "pbp": {
        "action_number": "Play sequence number (actionNumber); gaps are deleted or hidden actions. Rows are in the API's game order, and a period-end line can carry a LOWER number than the last play of the period (884 after 885 in the sample), so sort by period and clock, not by this number.",
        "period": "Period number (quarterNumber): 1-4; overtime numbering is unverified.",
        "clock": "Game clock REMAINING in the period as 'mm:ss' (counts down from 10:00).",
        "seconds_remaining": "Game clock remaining in the period, in seconds.",
        "home_score": "Running home score after the play (the home half of the 'home-away' score text).",
        "away_score": "Running away score after the play (the away half of the score text).",
        "side": "Side of the acting team (isHome); null on game events (period start / end, game end).",
        "action": "Play type in Turkish exactly as TBF sends it (77 values seen).",
        "action_en": "English gloss of the Turkish action, from the refs action vocabulary; null for an action not "
        "in it.",
        "player_name": "Acting player's name; null on game events. The feed carries no player id.",
    },
    "shots": {
        "period": "Period number of the attempt: 1-4; overtime numbering is unverified.",
        "side": "Side of the shooting team (isHome).",
        "shot_player_id": "Shooter id in the SHOT-CHART key space, which is NOT the TBF person id of the box score "
        "(Quentin Peterson = 2814324 here, 501076 as a TBF person): join shots to box rows on player_name + "
        "jersey_number + side.",
        "made": "True when the attempt was made (isSucceed).",
    },
    "period_scores": {
        "period": "Period number (quarter): 1-4 regulation; overtime numbering is unverified.",
        "points": "Points the side scored IN the period (not cumulative).",
        "side": "Side of the score: home (homeTeamScore) or away (awayTeamScore).",
    },
    "highlights": {
        "title": "Statistic name in Turkish (Ribaund = rebounds, Asist = assists, Top Kaybi = turnovers).",
        "category": "Statistic code (leaderCategory), e.g. PointsFromPaint, LeadChanges, TimeLeading.",
        "home_value": "Home value exactly as sent (a string: '90', or '28:38' for TimeLeading).",
        "away_value": "Away value exactly as sent (a string: '89', or '8:19' for TimeLeading).",
        "home_number": "Home value as a number; null for a time value such as TimeLeading.",
        "away_number": "Away value as a number; null for a time value such as TimeLeading.",
    },
    "game_leaders": {
        "category": "Leader category code (leaderCategory), e.g. VerimlilikPuani = efficiency rating.",
        "category_label": "Leader category label in Turkish (leaderCategoryDescription), e.g. Verimlilik Puani.",
        "rank": "Position in the category's list, 1 = best (top five per category).",
        "player_id": "TBF person id of the leader (key space: TBF persons; equals the box score player_id).",
        "value": "The leader's value in the category (points, rebounds, ...).",
        "side": "Side of the leader's team (isHome).",
    },
    "players": {
        "season": "Season END year of the request.",
        "competition": "League prefix the row reports (bsl, tbl or kbsl); equals the request's competition.",
        "nationality": "Nationality as a TURKISH country name (ABD = United States); see the nationality_code.",
        "nationality_code": "Nationality code (tabiyetKisa), ISO 3166-1 alpha-3 except GB for the United Kingdom.",
        "team_name": "Team name for that season, uppercase with Turkish letters.",
        "prefix": "League prefix of the row (bsl, tbl, kbsl); the runtime refuses any other than the request's.",
        "person_id": "TBF person id (kisiID; key space: TBF persons, stable across seasons and clubs).",
    },
    "player_career": {
        "person_id": "TBF person id of the player asked for (the player_id argument).",
        "player_name": "Player's full name from the profile (the same on every row).",
        "nationality": "Nationality as a TURKISH country name from the profile (country).",
        "height_cm": "Height in centimetres from the profile; null when TBF has none.",
        "season_label": "Career season as TBF writes it (seasonCode), e.g. 2025-2026.",
        "team_name": "Team the player represented that season, uppercase with Turkish letters (teamName).",
        "assists": "Assists in that season and competition.",
        "off_rebounds": "Offensive rebounds in that season and competition.",
        "def_rebounds": "Defensive rebounds in that season and competition.",
        "rebounds": "Total rebounds in that season and competition.",
        "steals": "Steals in that season and competition.",
        "blocks": "Blocks in that season and competition.",
        "season": "Career season END year, read from the season label.",
        "activity_name": "League-season name (activityName), e.g. 'BSL 2025-2026'.",
        "activity_type_id": "Competition type id (faaliyetTuruId): 1 = BSL, 4 = TBL, 79 = BGL.",
        "activity_id": "Activity id of the league-season (faaliyetAltAlaniId; key space: TBF league-seasons).",
        "seconds_played": "Total time on court that season in seconds (formattedTotalSeconds 'hh:mm:ss').",
        "fouls_drawn": "Fouls drawn (yapilanFaul).",
        "blocks_received": "Own shots blocked by an opponent (yenilenBlok).",
        "efficiency": "Efficiency rating total (playerEfficiency, PIR) for that season and competition.",
        "turnovers": "Turnovers (the API spells the field tournovers).",
        "fouls": "Personal fouls (the API spells the field fauls).",
        "ft_made": "Free throws made (the API spells the field freeThows).",
        "ft_attempted": "Free throws attempted (freeThowsAttempted).",
        "fg3_made": "Three-point field goals made (threePointsMade).",
        "fg3_attempted": "Three-point field goals attempted (threePointsAttempted).",
        "fg_made": "Field goals made (fieldGoalsMade), 2PT + 3PT.",
        "fg_attempted": "Field goals attempted (fieldGoalsAttempted), 2PT + 3PT.",
    },
    "leaders": {
        "kind": "total = the server's top 20 by season TOTAL; average = the top 20 by PER-GAME average.",
        "rank": "Rank in the list, 1 = best (the API's order).",
        "person_id": "TBF person id (oyuncuKodu, sent as a string; key space: TBF persons).",
        "team_process_id": "Team-season id (takimKodu, sent as a string; key space: TBF team registrations; despite "
        "the name NOT the club id).",
        "games": "Games played (macSayisi, sent as a string).",
        "value": "The ranked statistic: the season total for kind total, the per-game average for kind average (the "
        "API names the field grup and sends it as a string).",
        "category": "Statistic of the wrapper: points, assists, blocks, rebounds, steals, minutes, threes, turnovers "
        "or efficiency.",
    },
    "teams": {
        "team_name": "Team name as registered for that season, uppercase with Turkish letters.",
        "city": "Home city (teamCityName), uppercase Turkish.",
        "code": "Team short code (teamCode).",
        "season_label": "Season label 'YYYY-YYYY' of the league-season.",
    },
    "team_player_stats": {
        "team_process_id": "Team-season id of the request (key space: TBF team registrations).",
        "person_id": "TBF person id (code; key space: TBF persons, stable across seasons and clubs).",
        "jersey_number": "Shirt number (oyuncuNo) as a string.",
        "first_name": "Player's first name.",
        "last_name": "Player's last name.",
        "games": "Games played (macSayisi).",
        "seconds_played": "Season total time on court in seconds (sureToplam 'hh:mm:ss').",
        "points": "Season total points (sayiToplam; the API sends it as a string).",
        "fg_made": "Season field goals made (agBasarili; sent as a string).",
        "fg_attempted": "Season field goals attempted (agDeneme).",
        "fg2_made": "Season two-point field goals made (puan20Toplam).",
        "fg2_attempted": "Season two-point field goals attempted (puan21Toplam).",
        "fg3_made": "Season three-point field goals made (puan30Toplam).",
        "fg3_attempted": "Season three-point field goals attempted (puan31Toplam).",
        "ft_made": "Season free throws made (sA0Toplam).",
        "ft_attempted": "Season free throws attempted (sA1Toplam).",
        "def_rebounds": "Season defensive rebounds (drToplam).",
        "off_rebounds": "Season offensive rebounds (hrToplam).",
        "rebounds": "Season total rebounds (trToplam).",
        "assists": "Season assists (asistToplam).",
        "blocks": "Season blocks (blokToplam).",
        "steals": "Season steals (topCalmaToplam).",
        "turnovers": "Season turnovers (topKaybiToplam).",
        "fouls": "Season personal fouls (faulToplam).",
        "plus_minus": "Season plus/minus contribution (oyunaKatki = contribution to the game), a decimal string.",
        "efficiency": "Season efficiency rating total (vpToplam, verimlilik puani).",
        "fg_pct": "Field-goal percentage 0-100 with two decimals (agYuzdesi).",
        "fg2_pct": "Two-point percentage 0-100 with two decimals (ikilikYuzdesi).",
        "fg3_pct": "Three-point percentage 0-100 with two decimals (uclukYuzdesi).",
        "ft_pct": "Free-throw percentage 0-100 with two decimals (serbestAtisYuzdesi).",
        "seconds_per_game": "Average time on court per game in seconds (sureOrtalama 'hh:mm:ss').",
        "points_per_game": "Points per game (sayiOrtalama).",
        "rebounds_per_game": "Rebounds per game (trOrtalama).",
        "assists_per_game": "Assists per game (asistOrtalama).",
        "steals_per_game": "Steals per game (topCalmaOrtalama).",
        "blocks_per_game": "Blocks per game (blokOrtalama).",
        "turnovers_per_game": "Turnovers per game (topKaybiOrtalama).",
        "efficiency_per_game": "Efficiency rating per game (vpOrtalama).",
    },
    "team_games": {
        "team_process_id": "Team-season id of the request (key space: TBF team registrations).",
        "match_id": "TBF game id (key space: TBF games, one global sequence).",
        "tipoff": "Tip-off as a naive timestamp in Turkey local time (tarih).",
        "week": "Week label (formattedWeek), e.g. '1. Hafta'.",
        "city": "City of the venue (sehir), uppercase Turkish.",
        "venue": "Venue name (salon).",
        "home_team_process_id": "Home team-season id (takimAId; key space: TBF team registrations).",
        "home_team": "Home team name (takimA).",
        "home_score": "Home points (skorA); null before the game.",
        "away_team_process_id": "Away team-season id (takimBId).",
        "away_team": "Away team name (takimB).",
        "away_score": "Away points (skorB); null before the game.",
        "team_is_home": "True when the asked team was the home side (isHome).",
        "team_won": "True when the asked team won (isWin); false for a loss or an unplayed game.",
        "is_played": "True once the game has been played (isPlayed).",
        "status": "Game status text in Turkish: Oynandi = played, Oynanmadi = not played (durum).",
        "status_id": "Game status code (durumId): 1 = played, 4 = not played.",
    },
    "team_roster": {
        "team_process_id": "Team-season id of the request (key space: TBF team registrations).",
        "person_id": "TBF person id (personId; key space: TBF persons).",
        "nationality": "Nationality as a TURKISH country name (country); there is no code on this route.",
        "height_cm": "Height in centimetres; null when TBF has none.",
        "seconds_played": "Season time on court in seconds (sure 'hh:mm:ss').",
        "points_per_game": "Points per game (sayi).",
        "efficiency_per_game": "Efficiency rating per game (verimlilikPuani).",
    },
    "team_staff": {
        "team_process_id": "Team-season id of the request (key space: TBF team registrations).",
        "person_id": "TBF person id of the staff member (code; staff are TBF persons too).",
        "role": "Staff role in Turkish as sent (job), e.g. Antrenor = head coach.",
        "role_code": "Staff sub-role code (subRolesCode).",
    },
}

_HEADING = re.compile(r"^## `https://[^/]+/webapi-service/api(/\S*)`")
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")


def _md_rows(path: Path) -> Dict[str, Dict[str, str]]:
    """``{route: {record path: description}}`` from the returns md (unobserved rows skipped)."""
    out: Dict[str, Dict[str, str]] = {}
    route = None
    for line in path.read_text(encoding="utf-8").splitlines():
        h = _HEADING.match(line)
        if h:
            route = h.group(1)
            continue
        if line.startswith("## "):
            route = None
        m = _ROW.match(line)
        if m and route and not m.group(2).strip().startswith("Unobserved"):
            out.setdefault(route, {}).setdefault(m.group(1), m.group(2).strip().replace("&lt;", "<"))
    return out


def _md_route(route: str) -> str:
    """The md heading route of a wrapper route (the profile path drops its ``{player_id}``)."""
    return route.replace("/{player_id}", "")


def _describe(short: str, col: str, path: str, entry: Entry, md: Dict[str, Dict[str, str]]) -> str:
    if col in LOCAL_BY.get(entry[1], {}):
        return LOCAL_BY[entry[1]][col]
    if col in LOCAL:
        return LOCAL[col]
    rows = md.get(_md_route(entry[0]), {})
    for alt in path.split("|"):
        if alt.startswith("avg."):
            alt = alt[4:]
            prefixes: Tuple[str, ...] = ("data.playersAverage.[].",)
        else:
            prefixes = entry[3]
        for prefix in prefixes:
            if prefix + alt in rows:
                return rows[prefix + alt]
    return ""


def _frame(entry: Entry) -> pl.DataFrame:
    """The parser's frame over the entry's fixture."""
    frame = P.parse(entry[1], entry[2]())
    assert isinstance(frame, pl.DataFrame)
    return frame


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    route, table, _fix, _src, summary, fields = entry
    parser = f"parse_tbf_{table}"
    assert hasattr(P, parser), parser
    return {
        "short": short,
        "summary": summary,
        "path": route,
        "parser": parser,
        "returns_schema": f"native/tbf/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("tbf", SPEC)
    spec = load_spec(refs, SPEC)
    wrapped = {e[0].replace("{player_id}", "{playerId}") for e in ENDPOINTS.values()}
    differs = (wrapped | set(NOT_WRAPPED)) ^ set(spec["paths"])
    if differs:
        raise SystemExit(f"tbf: spec paths, ENDPOINTS and NOT_WRAPPED differ: {sorted(differs)}")
    assert set(P.TABLES) == {e[1] for e in ENDPOINTS.values()}, "every table needs an endpoint and the reverse"
    doc = {
        "api": "tbf",
        "host": HOST,
        "name_pattern": "tbf_{short}",
        "module": "tbf",
        "parser_module": "basketball.tbf.tbf_parsers",
        "getter_module": "sportsdataverse.basketball.tbf.tbf_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (pbp, shots, players) name other families
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw body (``dict``): the TBF envelope ``{data, isSuccess, message, problemDetails}`` with "
            "personal fields redacted, plus ``_request`` (the season, competition, game and team ids the runtime "
            "resolved)",
            "raises": [
                "sportsdataverse.errors.NoDataError: TBF has nothing for the request (an unknown game or player, a "
                "season the league list lacks, an empty list, a finished game before 2025-26 asked for play-by-play "
                "or shots).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for (an "
                "unfinished game, a body for another game, season, league or team, an error envelope, a redirect, a "
                "partial page or a changed capped list).",
            ],
            "notes": [UNOFFICIAL, COVERAGE],
            "see_also": [
                {
                    "name": "TBF",
                    "url": "https://www.tbf.org.tr",
                    "note": "the Turkish Basketball Federation whose app backend these wrappers read",
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
    write_yaml(ROOT / "tools/codegen/endpoints/tbf.yaml", doc)

    md = _md_rows(refs / RETURNS)
    schema_dir = ROOT / "tools/codegen/schemas/native/tbf"
    rewrite_schema_dir(schema_dir)
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        cols: List[Dict[str, str]] = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        paths = {c[0]: c[1] for c in P.TABLES[entry[1]]}
        block = notes.setdefault(f"{SCHEMA_PREFIX}{short}", {})
        for col in cols:
            text = _describe(short, col["name"], paths[col["name"]], entry, md).strip()
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"{SCHEMA_PREFIX}{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(
            schema_dir / f"{short}.yaml", {"schema": f"{SCHEMA_PREFIX}{short}", "kind": "dataframe", "columns": cols}
        )
    if bad:
        raise SystemExit(f"tbf: empty or filler descriptions (add them to LOCAL / LOCAL_BY): {bad}")
    write_yaml(NOTES, notes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"tbf: {len(ENDPOINTS)} endpoints, {sum(map(len, notes.values()))} descriptions -> {NOTES.name}")


if __name__ == "__main__":
    main()
