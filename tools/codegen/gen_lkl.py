"""Generate the ``lkl`` flat-API endpoint YAML + returns-schemas (Lithuanian LKL on lkl.lt).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_feb.py`` (itself ``gen_asa.py`` with
the shared plumbing in ``gen_soccer_common.py``). Reads the frozen spec and ``lkl-returns.md`` from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/lkl.yaml`` -- 14 wrappers over the spec's 10 routes (7 JSON, 3 HTML). One route can feed
  several frames (``/boxscore`` feeds ``player_box``, ``team_box`` and ``period_scores``; ``/rungtynes`` feeds
  ``game_info`` and ``game_players``; ``/game-stats`` feeds ``game_stats`` and ``lead_chart``), each with its own
  parser; :data:`ENDPOINTS` is the source of truth. Game wrappers take ``game_id``; ``lkl_results`` and
  ``lkl_standings`` take ``season`` (END year), which ``lkl_runtime`` maps to the site's two opaque season ids.
* ``tools/codegen/schemas/native/lkl/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser run
  over its committed fixture. The game wrappers use the FULL bodies of one game in ``tests/fixtures/lkl/full/``
  (stamped with ``_request`` the way ``lkl_runtime`` stamps a live body); the season wrappers use the refs page
  records in ``tests/fixtures/lkl/captures/``. Each parser emits a fixed column list with dtypes pinned by name
  (``lkl_parsers.SPECS``), so a fixture cannot mistype a rate. The schemas carry ``description: ''`` (descriptions
  never live in ``schemas/**``).
* ``tools/codegen/notes/lkl_descriptions.yaml`` -- ``{lkl_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (Integration merges it). Text comes from :data:`LOCAL_BY` (a column whose
  meaning differs by wrapper), :data:`LOCAL` (derived and renamed columns), then the ``lkl-returns.md`` row of the
  same record path. An empty, short or filler text fails the run.
* ``tests/fixtures/lkl/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_lkl.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, columns_from_frame, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.lkl import lkl_parsers as P
from sportsdataverse.basketball.lkl.lkl_runtime import REQUEST_KEY, _request
from sportsdataverse.dl_utils import underscore

HOST = "https://lkl.lt/api/livestream"
SITE = "https://lkl.lt"
SPEC = "lkl.openapi.yaml"
RETURNS = "lkl-returns.md"
FIXTURES = ROOT / "tests" / "fixtures" / "lkl"
SCHEMA_PREFIX = "lkl_"
#: Column descriptions, keyed ``lkl_<short>`` -> column; merged into manual_column_descriptions.yaml.
NOTES = ROOT / "tools" / "codegen" / "notes" / "lkl_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)
GAME_ID_FIXTURE = 11446

UNOFFICIAL = (
    "Unofficial and unsupported: these are the routes lkl.lt's own game page calls (found in its front-end bundle) "
    "and its server-rendered pages. The site's robots.txt allows everything and sets no crawl-delay; "
    "``lkl_runtime`` still spaces requests at least 1 s apart. SportsDataverse has not ruled on the site's terms, "
    "so payloads are not redistributed as release assets."
)
COVERAGE = (
    "Coverage: game wrappers serve FINISHED games only (an unfinished game raises AssetFetchError). The box score, "
    "summary, score line, statistics and leaders answer for every season from 1993-94 (before 2024-25 the scoring "
    "splits and lead statistics are 0). Play-by-play and shots exist from 2024-25 only; an earlier finished game "
    "raises NoDataError. From 2016-17 the results season id is also the Genius LiveStats competition id (see the "
    "``genius`` family) which holds play-by-play for older games; lkl.lt game, team and player ids are lkl.lt's own "
    "and do not join Genius ids. The King Mindaugas Cup shares the routes but is not wrapped. Text fields are "
    "Lithuanian."
)
SEASON = (
    "Required. Season END year (2026 = the 2025-26 season). The site keys a season by two opaque ids (a results id "
    "and a standings id, neither a year); ``lkl_runtime`` reads them from the site's season selects by the label "
    "``YYYY-YYYY m. sezonas``. A year the selects do not list raises NoDataError."
)
GAME = (
    "Required. lkl.lt game id (integer), e.g. 11446; ``game_id`` in lkl_results. Not sequential by season before "
    "2018-19. Only a finished game is served."
)


def _game_path() -> Dict[str, Any]:
    return {
        "path_params": [{"name": "game_id", "type": "int", "required": True, "description": GAME}],
        "example_args": {"game_id": GAME_ID_FIXTURE},
    }


def _full(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _stamped(name: str) -> Dict[str, Any]:
    """A full route body stamped with ``_request`` the way ``lkl_runtime`` stamps a live one."""
    body = _full(name)
    body[REQUEST_KEY] = _request(_full("rungtynes"))
    return body


def _capture(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


Source = Tuple[str, str]  # (returns-md section, record-path prefix)
# short -> (spec path, wrapper path, parser, fixture loader, md sources, summary, fields)
Entry = Tuple[str, str, Callable[..., Any], Callable[[], Dict[str, Any]], Tuple[Source, ...], str, Dict[str, Any]]
ENDPOINTS: Dict[str, Entry] = {
    "game_info": (
        "/rungtynes/{gameId}",
        "/rungtynes/{game_id}",
        P.parse_lkl_game_info,
        lambda: _full("rungtynes"),
        (("rungtynes", ""), ("rungtynes", "game_info.")),
        "Game header from the game page: status, tip-off, venue, attendance, officials and both teams.",
        {**_game_path(), "host": SITE},
    ),
    "game_players": (
        "/rungtynes/{gameId}",
        "/rungtynes/{game_id}",
        P.parse_lkl_game_players,
        lambda: _full("rungtynes"),
        (("rungtynes", "players.home.[]."),),
        "Game roster from the game page: every player on either team sheet with the lkl.lt player id and slug.",
        {**_game_path(), "host": SITE},
    ),
    "player_box": (
        "/boxscore/{gameId}",
        "/boxscore/{game_id}",
        P.parse_lkl_player_box,
        lambda: _stamped("boxscore"),
        (("boxscore", "boxscore.home.players.[]."),),
        "Per-player box score of a finished game: minutes, points, shooting splits, rebounds, assists and plus/minus.",
        _game_path(),
    ),
    "team_box": (
        "/boxscore/{gameId}",
        "/boxscore/{game_id}",
        P.parse_lkl_team_box,
        lambda: _stamped("boxscore"),
        (("boxscore", "gameStatistics.{team_id}."),),
        "Team game totals of both teams: shooting, rebounds, assists, turnovers, fouls, paint, bench and fast-break points.",
        _game_path(),
    ),
    "period_scores": (
        "/boxscore/{gameId}",
        "/boxscore/{game_id}",
        P.parse_lkl_period_scores,
        lambda: _stamped("boxscore"),
        (),
        "Points each team scored in each period of a game, overtime included.",
        _game_path(),
    ),
    "summary": (
        "/summary/{gameId}",
        "/summary/{game_id}",
        P.parse_lkl_summary,
        lambda: _stamped("summary"),
        (("summary", "comparison.[]."),),
        "Summary tab: the head-to-head comparison of both teams (shooting, rebounds, assists, bench points...).",
        _game_path(),
    ),
    "game_score": (
        "/game-score/{gameId}",
        "/game-score/{game_id}",
        P.parse_lkl_game_score,
        lambda: _stamped("game-score"),
        (("game-score", ""),),
        "Score line of a game: both scores and the clock text.",
        _game_path(),
    ),
    "game_stats": (
        "/game-stats/{gameId}",
        "/game-stats/{game_id}",
        P.parse_lkl_game_stats,
        lambda: _stamped("game-stats"),
        (("game-stats", "comparison.all.[]."),),
        "Statistics tab: the head-to-head comparison of both teams, one row per statistic.",
        _game_path(),
    ),
    "lead_chart": (
        "/game-stats/{gameId}",
        "/game-stats/{game_id}",
        P.parse_lkl_lead_chart,
        lambda: _stamped("game-stats"),
        (("game-stats", "leads.{period}.[]."),),
        "Lead chart of a game: the score margin at each chart point of each period.",
        _game_path(),
    ),
    "leaders": (
        "/leaders/{gameId}",
        "/leaders/{game_id}",
        P.parse_lkl_leaders,
        lambda: _stamped("leaders"),
        (("leaders", "leaders.points."),),
        "Leaders tab: each team's top players in points, rebounds, assists, efficiency, steals and blocks.",
        _game_path(),
    ),
    "pbp": (
        "/play-by-play/{gameId}",
        "/play-by-play/{game_id}",
        P.parse_lkl_pbp,
        lambda: _stamped("play-by-play"),
        (("play-by-play", "quarters.regular.{period}.[]."),),
        "Play-by-play of a finished game (2024-25 onward), oldest first, with the running score and the lkl.lt player id.",
        _game_path(),
    ),
    "shots": (
        "/shot-chart/{gameId}",
        "/shot-chart/{game_id}",
        P.parse_lkl_shots,
        lambda: _stamped("shot-chart"),
        (("shot-chart", "quarters.regular.{period}.[]."),),
        "Every field-goal attempt of a finished game (2024-25 onward) with its court position (Genius LiveStats data).",
        _game_path(),
    ),
    "results": (
        "/loadResults",
        "/loadResults",
        P.parse_lkl_results,
        lambda: _capture("loadResults@39252"),
        (("loadResults", "games.[]."), ("loadResults", "")),
        "Every played game of one LKL season (all pages of the site's results list, merged), newest first.",
        {
            "host": SITE,
            "extra_params": [
                {"name": "season", "query_key": "season", "type": "int", "required": True, "description": SEASON}
            ],
            "example_args": {"season": 2026},
        },
    ),
    "standings": (
        "/loadStandings/{seasonId}",
        "/loadStandings/{season}",
        P.parse_lkl_standings,
        lambda: _capture("loadStandings__seasonId@33"),
        (("loadStandings", "full.[]."), ("loadStandings", "")),
        "League table of one LKL season: overall, home and away records, and the final ranking once it is set.",
        {
            "host": SITE,
            "path_params": [{"name": "season", "type": "int", "required": True, "description": SEASON}],
            "example_args": {"season": 2026},
        },
    ),
}

#: Descriptions of the columns the parsers add, re-key or translate (they win over the returns md).
LOCAL: Dict[str, str] = {
    "game_id": "lkl.lt game id (key space: LKL games; the {gameId} of every route). Not sequential by season before "
    "2018-19; not the Genius matchId. A frame parsed from a bare body has a null game_id when the body does not "
    "echo it (the runtime stamps it under _request).",
    "team_side": "Side of the game the row belongs to: home or away (the game page's teams_info side; the box score, "
    "leaders and plays say home / away, and gameStatistics is keyed by team id so the runtime supplies the side).",
    "team_id": "lkl.lt team id (key space: lkl.lt clubs, STABLE across seasons and renamings: 6 = Rytas). Joins "
    "lkl_game_info home_team_id / away_team_id; not the Genius team id. Null on a frame parsed from a bare body.",
    "player_id": "lkl.lt player id (key space: lkl.lt persons, stable across seasons). Resolved from the player slug "
    "through the game roster the runtime stamps from the game page; null on a bare body or a slug the roster lacks.",
    "player_slug": "lkl.lt player slug (key space: lkl.lt persons; the {slug} of lkl.lt/zaidejai/{slug}, stable "
    "across seasons). Join key to lkl_game_players.slug.",
    "player": "Player name as the site prints it in this route (the box score short form 'I. Surname', other routes "
    "the full name).",
    "period": "Period number: 1-4 regulation, 5 the first overtime, 6 the second.",
    "period_type": "regular for periods 1-4, overtime from period 5.",
    "clock": "Game clock REMAINING in the period as 'mm:ss' (it counts down from 10:00, from 05:00 in overtime).",
    "x": "Court x position: percent (0-100) of the LENGTH of a horizontal full court from the left baseline (baskets "
    "near 5 and 95). Teams switch baskets at half-time; metres (x / 100 x 28) are an inference.",
    "y": "Court y position: percent (0-100) of the court WIDTH (baskets at 50). Metres (y / 100 x 15) are an "
    "inference; which sideline is y = 0 is unverified.",
    "season_id": "lkl.lt season id; the key space depends on the wrapper (see lkl_results and lkl_standings).",
    "season_end_year": "Season END year (2026 = 2025-26), read from the season label 'YYYY-YYYY m. sezonas'.",
}

#: Per-wrapper descriptions where a column means something different or is derived.
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "game_info": {
        "game_status": "Game state label in Lithuanian: 'Pasibaigė' = finished, the only value served (the runtime "
        "refuses an unfinished game).",
        "start_date": "Game date (Europe/Vilnius), parsed from the page header 'YYYY m. <month> D d., HH:MM'.",
        "start_time": "Tip-off 'HH:MM' local time (Europe/Vilnius by the site's wording; the page carries no offset).",
        "arena": "Arena name as printed on the game page header.",
        "attendance": "Attendance text as printed ('1097 žiūrovai' = 1097 spectators).",
        "attendance_count": "Attendance as an integer, parsed from the attendance text; null when the page has none.",
        "referees": "Referee names, comma-separated, crew chief first; 'Nepaskelbta' = not announced (old games). "
        "Names only: the site publishes no official's contact or birth data.",
        "has_actions": "True when the game has play-by-play events (2024-25 onward); false on older games.",
        "is_streaming": "True while a live stream is on; false on finished games.",
        **{
            f"{side}_team_id": f"{side.capitalize()} team id (key space: lkl.lt clubs, stable across seasons and "
            "renamings; joins the team_id of every other frame). Not the Genius team id."
            for side in ("home", "away")
        },
        **{
            f"{side}_team": f"{side.capitalize()} team name as of that game (historical: 'Lietuvos Rytas' in 2007-08)."
            for side in ("home", "away")
        },
        **{
            f"{side}_short_name": f"{side.capitalize()} three-letter team code ('RYT'); can be the club's current "
            "code on old games."
            for side in ("home", "away")
        },
        **{
            f"{side}_slug": f"{side.capitalize()} team slug (key space: lkl.lt clubs; the {{slug}} of "
            "lkl.lt/komandos/{slug}); joins the team slugs of lkl_results and lkl_standings."
            for side in ("home", "away")
        },
        **{f"{side}_winner": f"True when the {side} team won the game." for side in ("home", "away")},
        **{
            f"{side}_coach": f"{side.capitalize()} head coach's full name as stored for the team: on old games the "
            "CURRENT coach, not the historical one."
            for side in ("home", "away")
        },
        **{
            f"{side}_assistant_coach": f"{side.capitalize()} assistant coach's full name (same caveat as the head "
            "coach: current staff on old games)."
            for side in ("home", "away")
        },
    },
    "game_players": {
        "player": "Full player name as the game page prints it.",
        "jersey": "Shirt number as a string (so '00' and '0' stay distinct).",
        "slug": "Player slug (key space: lkl.lt persons; the {slug} of lkl.lt/zaidejai/{slug}). Join key to the "
        "player_slug of the box score, leaders and play-by-play.",
        "player_id": "lkl.lt player id (key space: lkl.lt persons, stable across seasons). Joins the shot chart's "
        "player_id; the box score, leaders and play-by-play carry the slug, which this frame maps.",
    },
    "player_box": {
        "seconds_played": "Time on court in seconds (the page's 'mm:ss' minutes: 32:54 = 1974).",
        "fg_made": "Field goals made, 2PT + 3PT (the 'made' half of the 'made/attempted' cell).",
        "fg_attempted": "Field goals attempted, 2PT + 3PT.",
        "fg_pct": "Field-goal percentage, 0-100 (the site's one-decimal string; 0 when there were no attempts).",
        "fg2_made": "Two-point field goals made.",
        "fg2_attempted": "Two-point field goals attempted.",
        "fg2_pct": "Two-point percentage, 0-100 (one-decimal string on the site; 0 when there were no attempts).",
        "fg3_made": "Three-point field goals made.",
        "fg3_attempted": "Three-point field goals attempted.",
        "fg3_pct": "Three-point percentage, 0-100 (one-decimal string on the site; 0 when there were no attempts).",
        "ft_made": "Free throws made.",
        "ft_attempted": "Free throws attempted.",
        "ft_pct": "Free-throw percentage, 0-100 (one-decimal string on the site; 0 when there were no attempts).",
    },
    "team_box": {
        "seconds_played": "Summed playing time of all the team's players in seconds (12000 = 5 x 40:00; 13500 with "
        "one overtime).",
        "field_goals_percentage": "Field-goal percentage, 0-100 float (the site sends a two-decimal string).",
        "field_goals_2pt_percentage": "Two-point percentage, 0-100 float (the site sends a two-decimal string).",
        "field_goals_3pt_percentage": "Three-point percentage, 0-100 float (the site sends a two-decimal string).",
        "free_throws_percentage": "Free-throw percentage, 0-100 float (the site sends a two-decimal string).",
        "blocks": "Blocks made by the team in the game (count).",
        "time_leading": "Seconds this team led during the game (the site sends it as a string; 1892 = 31:32).",
    },
    "period_scores": {
        "points": "Points the team scored IN this period (not cumulative); keyed by period in the site's "
        "quarter_scores.",
    },
    "summary": {
        "stat": "English code for the site's Lithuanian label (field_goals, two_pointers, three_pointers, free_throws, "
        "rebounds, assists, steals, blocks, turnovers, fouls, points, ...); null for a label this package does not know.",
        "group": "Always null here: the flat comparison list has no groups (lkl_game_stats labels them).",
    },
    "game_stats": {
        "stat": "English code for the site's Lithuanian label (field_goals, two_pointers, three_pointers, free_throws, "
        "rebounds, assists, steals, blocks, turnovers, fouls, points, ...); null for a label this package does not know.",
        "group": "Bar group the tab also shows the line in: shooting (2PT, 3PT, free throws) or main (rebounds, "
        "assists, steals); null for lines only in the full list.",
    },
    "game_score": {
        "home_score": "Home team points on the score line.",
        "away_score": "Away team points on the score line.",
        "clock": "Clock text, NOT a state flag: an old game keeps '1 KĖLINYS - 10:00' after the final; a finished "
        "overtime game reads '1 pratęsimas - 00:00'.",
    },
    "lead_chart": {
        "diff": "Home minus away score at that chart point (signed integer).",
        "clock": "Clock REMAINING in the period as the site's 'mm:ss:00' text (a trailing ':00'): '10:00:00' is the "
        "period start.",
        "score_home": "Home cumulative score at that point (the site sends a string).",
        "score_away": "Away cumulative score at that point (the site sends a string).",
        "column_height": "Bar height for the site's chart: |diff| / maxDiff x 50 (percent of the chart).",
    },
    "leaders": {
        "category": "Leader category: points, total_rebounds, assists, efficiency, steals or blocks.",
        "category_label": "Category label in Lithuanian ('Pelnomi taškai' = points scored, 'Naudingumas' = "
        "efficiency rating).",
        "rank": "Place in the team's category list: 1 = the category leader, 2 and up = the runners-up in the order "
        "the site lists them (up to 4).",
        "value": "The player's count in the row's category.",
    },
    "pbp": {
        "event_num": "Play sequence number the parser assigns, 1 = the first play of the game (the site lists plays "
        "newest first per period, with no id).",
        "period": "Period of the play. Regulation lines carry it in the site's body; overtime lines come as one "
        "merged list without it, so the parser derives it (oldest overtime = 5, each period-end line closes one): "
        "checked on a one-overtime game only, so multi-overtime numbering is unverified.",
        "clock_seconds": "The clock REMAINING as whole seconds (08:34 = 514), derived by the parser.",
        "team_side": "Side of the acting team (the line's is_home flag): home or away.",
        "type": "Event type (English code): 2pt, 3pt, freethrow, rebound, assist, steal, block, turnover, foul, "
        "foulon (foul drawn), substitution, timeout, jumpball, headcoachchallenge, period, game.",
        "description": "Event text in Lithuanian, one fixed phrase per type and subtype ('3 taškų metimas pataikytas' "
        "= 3-point shot made).",
        "success": "Shots and free throws: true = made. Every other type: always true.",
        "is_points_info": "True on lines that changed the score (made shots and free throws).",
        "score_home": "Home points after the play (on every line, not only scoring lines).",
        "score_away": "Away points after the play (on every line, not only scoring lines).",
    },
    "shots": {
        "shot_id": "Shot event id (key space: lkl.lt shot events, increasing over time); joins nothing else and is "
        "not the Genius action number.",
        "team_side": "Side of the shooter's team (the line's is_home flag): home or away.",
        "type": "2pt or 3pt (free throws are not in the chart).",
        "made": "True when the shot was made, false when missed.",
        "side": "Basket half the shot is at: LEFT (x < 50) or RIGHT, except backcourt heaves (area backcourt).",
        "area": "Court zone code (e.g. inthepaint, underbasket, outsideleft); left and right are from the shooter's "
        "view, so zones are point-symmetric between the two baskets.",
    },
    "results": {
        "season_id": "Results season id (key space: lkl.lt championships; LKL 2026-27 = 49436). From 2016-17 it is "
        "the Genius LiveStats competition id (leagueId 110).",
        "date": "Game date from the site's Lithuanian day header; checked to fall in the season (Aug 1 - Jul 31).",
        "home_score": "Home team points in the game (the final score).",
        "away_score": "Away team points in the game (the final score).",
        "winner_highlighted": "Which side's score the page sets in bold: home or away (the winner).",
    },
    "standings": {
        "season_id": "STANDINGS season id (a separate small key space: 35 = 2026-27, 1 = 2016-17, 24 = 1993-94); NOT "
        "the results season id.",
        "table": "Which table the row comes from: full (overall), home, away or final (the final ranking).",
        "position": "Rank in the table (Poz.); in the final table the final place.",
        "win_pct": "Win percentage 0-100 (Laim.%); null in the home, away and final tables and when the site prints "
        "'-' (2007-08).",
        "streak": "Current streak (Serija) as a signed integer: 27 = 27 wins in a row, -1 = one loss. Null outside "
        "the full table.",
        "played": "Games played (Rng., rungtynės); null in the final table.",
        "won": "Wins (Per., pergalės); null in the final table.",
        "lost": "Losses (Pr., pralaimėjimai); null in the final table.",
        "points_for_avg": "Points scored per game (first number of Tšk. vid.); null in the final table.",
        "points_against_avg": "Points allowed per game (second number of Tšk. vid.); null in the final table.",
        "last5": "Last five results (Paskutinės 5) as W / L letters in the order the page renders them, left to "
        "right; fewer than five early in a season; null in the final table.",
    },
}

_HEADING = re.compile(r"^## `https://[^/]+(/\S*)`")
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")
_SECTIONS = {
    "boxscore": "/boxscore/",
    "summary": "/summary/",
    "game-score": "/game-score/",
    "game-stats": "/game-stats/",
    "leaders": "/leaders/",
    "play-by-play": "/play-by-play/",
    "shot-chart": "/shot-chart/",
    "rungtynes": "/rungtynes/",
    "loadResults": "/loadResults",
    "loadStandings": "/loadStandings/",
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
        "returns_schema": f"native/lkl/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("lkl", SPEC)
    spec = load_spec(refs, SPEC)
    differs = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if differs:
        raise SystemExit(f"lkl: spec paths and ENDPOINTS differ: {sorted(differs)}")
    assert set(P.SPECS) == set(ENDPOINTS), "every parser spec needs an endpoint and the reverse"
    doc = {
        "api": "lkl",
        "host": HOST,
        "name_pattern": "lkl_{short}",
        "module": "lkl",
        "parser_module": "basketball.lkl.lkl_parsers",
        "getter_module": "sportsdataverse.basketball.lkl.lkl_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (pbp, shots, results) name other families
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw body (``dict``): the JSON of a game route, the game page record, or the merged "
            "results / standings record, plus ``_request`` (the game id, team ids and slug -> player id map, or the "
            "season asked for)",
            "raises": [
                "sportsdataverse.errors.NoDataError: lkl.lt has nothing for the request (an unknown game or "
                "standings season, a season the site's selects do not list, a finished game before 2024-25 asked "
                "for play-by-play or shots).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for "
                "(an unfinished game, a body or page for another game or season, an error or changed body, a "
                "truncated results list).",
            ],
            "notes": [UNOFFICIAL, COVERAGE],
            "see_also": [
                {
                    "name": "LKL",
                    "url": "https://lkl.lt",
                    "note": "the Lietuvos krepšinio lyga site whose pages and livestream API these wrappers read",
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
    write_yaml(ROOT / "tools/codegen/endpoints/lkl.yaml", doc)

    md = _md_rows(refs / RETURNS)
    schema_dir = ROOT / "tools/codegen/schemas/native/lkl"
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
        raise SystemExit(f"lkl: empty or filler descriptions (add them to LOCAL / LOCAL_BY): {bad}")
    write_yaml(NOTES, notes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"lkl: {len(ENDPOINTS)} endpoints, {sum(map(len, notes.values()))} descriptions -> {NOTES.name}")


if __name__ == "__main__":
    main()
