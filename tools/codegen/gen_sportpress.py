"""Generate the ``sportpress`` flat-API endpoint YAML + returns-schemas (BNXT League on the Sportpress CMS).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_feb.py`` (itself ``gen_asa.py``
with the shared plumbing in ``gen_soccer_common.py``). Reads the frozen spec and the returns md from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/sportpress.yaml`` -- 28 wrappers over the spec's 23 routes. One route can feed
  several frames (``/schedule/overview`` feeds ``game_overview``, ``game_top_performers`` and ``game_leaders``;
  ``/boxscore/game`` feeds ``game_box`` and ``game_team_box``; ``/competition-team/all`` feeds
  ``competition_teams`` and ``competition_staff``; ``/schedule/find`` feeds ``game_find`` and ``game_periods``),
  each with its own parser; :data:`ENDPOINTS` is the source of truth.
* ``tools/codegen/schemas/native/sportpress/<short>.yaml`` -- one returns-schema per wrapper: the columns of its
  parser run over its committed fixture. Every parser emits a fixed column list with dtypes pinned by name
  (``bnxt_parsers.SPECS``), so a fixture cannot mistype a rate. The schemas carry ``description: ''``.
* ``tools/codegen/notes/bnxt_descriptions.yaml`` -- ``{sportpress_<short>: {column: text}}``, the committed block
  Integration merges into ``manual_column_descriptions.yaml``. Text comes from :data:`LOCAL` (derived and renamed
  columns), :data:`LOCAL_BY` (a column whose meaning differs by wrapper), then the ``sportpress-returns.md`` row of
  the column's source path. An empty, short or filler text fails the run.
* ``tests/fixtures/sportpress/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_sportpress.py``
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

from sportsdataverse.basketball.bnxt import bnxt_parsers as P

HOST = "https://bnxt.sportpress.info/api/v1"
SPEC = "sportpress.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "sportpress"
NOTES = ROOT / "tools" / "codegen" / "notes" / "bnxt_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

UNOFFICIAL = (
    "Unofficial and unsupported: the Sportpress API is the JSON backend of bnxtleague.com, not a documented public "
    "API. Every route needs a header key; it is public (hard-coded in the site bundle), ``bnxt_runtime`` reads it "
    "from the bundle at run time, falls back to a documented value when the bundle cannot be read, and never "
    "prints or logs it. A 401 raises AssetFetchError (``key rotated?``). robots.txt allows everything; requests "
    "are spaced at least 1 s apart. Payloads are not redistributed as release assets."
)
SEASONS = (
    "Season key: the END year (``season=2026`` is 2025-26), the API's own key on the season routes. Every other "
    "route is keyed by the competition id (one per season: 1 = 2021-22, 2 = 2020-21, 3 = 2022-23, 12 = 2023-24, "
    "22 = 2024-25, 24 = 2025-26, 25 = 2026-27), the competition-team id (new every season; ``uu_team_id`` is "
    "the stable club), the roster id (player x team x season) or the game id (unique across seasons). "
    "``sportpress_phases(season)`` maps a season to its competition id."
)
COVERAGE = (
    "Coverage: schedule and rosters from 2021-22 (2020-21 is empty); box scores for 2021-22, 2024-25, 2025-26 "
    "and 2026-27 (2022-23 and 2023-24 answer empty); standings and averages for 2021-22 and 2024-25 on; "
    "play-by-play ('film', with court x/y) for 2021-22 and 2026-27 only: for 2022-23 to 2025-26 use Genius "
    "play-by-play through ``genius_id`` (``sportpress_game_slider``, organisation BNXT). The game routes "
    "(box score, film, overview) refuse a game that is not finished (AssetFetchError)."
)
DATA_NOTES = (
    "Data notes: a team's box total can differ from the schedule final score on about 18 of 1,246 sampled "
    "team boxes (mostly 2024-25, by 2-21 points); the frames report what the API serves. ``nationality_code`` "
    "is dirty (lower case, ``USA/BEL`` pairs, empty, ``HOL``). Names differ by language and sponsor: join on ids. "
    "Contact and social fields are dropped; only a player's public birth date is kept. The sibling tenants "
    "sopron.sportpress.info and beac.sportpress.info are not wrapped."
)

SEASON = "Required. Season END year (2026 = 2025-26), the API's own key; no mapping is applied."
COMPETITION = (
    "Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 "
    "(``sportpress_phases`` maps a season to it)."
)
PHASE = "Required. Phase id (integer) of the competition, from ``sportpress_phases`` (``phase_id``)."
TEAM = (
    "Required. Competition-team id (integer): a team in ONE competition, so new every season "
    "(e.g. 541 = Antwerp in 2026-27); from ``sportpress_competition_teams``."
)
ROSTER = "Required. Roster id (integer): one player on one team in one season; from the roster frames."
GAME = "Required. Game id (integer), unique across seasons; from ``sportpress_schedule`` (``game_id``)."


def _p(name: str, desc: str, typ: str = "int") -> Dict[str, Any]:
    return {"name": name, "type": typ, "required": True, "description": desc}


def _q(
    name: str, desc: str, *, key: Optional[str] = None, default: Any = None, required: bool = False
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": key or name, "type": "int", "description": desc}
    if required:
        out["required"] = True
    if default is not None:
        out["default"] = default
    return out


def _fx(
    *,
    path: List[Dict[str, Any]] = (),  # type: ignore[assignment]
    query: List[Dict[str, Any]] = (),  # type: ignore[assignment]
    fixed: Optional[Dict[str, Any]] = None,
    example: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"example_args": example or {}}
    if path:
        out["path_params"] = list(path)
    if query:
        out["extra_params"] = list(query)
    if fixed:
        out["fixed_params"] = dict(fixed)
    return out


CLUBS = {"clubs[0]": 1, "clubs[1]": 2}  # 1 = Belgium, 2 = Netherlands: the schedule route 422s without a clubs filter
PHASE_Q = _q("phase_id", "Restrict to one phase (integer), from ``sportpress_phases``. Omitted = every phase.")
LIMIT_Q = _q("limit", "Cap each ranking list at this many rows. Omitted = the complete lists.")
TYPE_Q = {
    "name": "type",
    "query_key": "type",
    "type": "str",
    "description": "Return one ranking category only (e.g. ``points``); ``all`` or omitted = the nine categories.",
}

# Fixture sources: ("full", file stem, ctx) | ("cap", file stem, ctx).  ctx is what the runtime stamps under _request.
Source = Tuple[str, str, Dict[str, Any]]
# md source: (returns-md section path, item prefix -> md prefix map, default md prefix)
Md = Tuple[str, Dict[str, str], str]
Entry = Tuple[str, str, Callable[..., Any], Source, Md, str, Dict[str, Any]]


def _e(
    spec_path: str,
    path: str,
    short: str,
    src: Source,
    md: Md,
    summary: str,
    fields: Dict[str, Any],
) -> Tuple[str, Entry]:
    return short, (spec_path, path, getattr(P, f"parse_sportpress_{short}"), src, md, summary, fields)


_SD = "/schedule/find/{gameId}"
_BX = "/boxscore/game/{competitionId}/{gameId}"
_OV = "/schedule/overview/{gameId}"
_CT = "/competition-team/all"
_G = {"game_id": 11295}
_GC = {"game_id": 11295, "competition_id": 25}
_COMPETITOR_HOME = {"home.": "data.competitors.[].", "away.": "data.competitors.[]."}
_COMPETITORS_ROWS = {"home.": "data.[].competitors.[].", "away.": "data.[].competitors.[]."}

ENDPOINTS: Dict[str, Entry] = dict(
    [
        _e(
            "/server-config",
            "/server-config",
            "ui_strings",
            ("cap", "server-config", {}),
            ("/server-config", {}, ""),
            "UI string tables of the site in English, French and Dutch (one row per language and key).",
            _fx(),
        ),
        _e(
            "/competition-team/club/seasons",
            "/competition-team/club/seasons",
            "club_seasons",
            ("cap", "competition-team__club__seasons", {}),
            ("/competition-team/club/seasons", {}, "data.[]."),
            "Seasons (END years) in which Belgian and Dutch clubs have teams.",
            _fx(fixed=CLUBS),
        ),
        _e(
            "/staff/list",
            "/staff/list",
            "referees",
            ("cap", "staff__list", {}),
            ("/staff/list", {}, "data.[]."),
            "Referees of the league (staff title 2).",
            _fx(fixed={"title_id": 2}),
        ),
        _e(
            "/phase/season/{season}",
            "/phase/season/{season}",
            "phases",
            ("cap", "phase__season__season", {"season": 2026}),
            ("/phase/season/{season}", {"phase.": "data.[].phases.[]."}, "data.[]."),
            "The competition of a season with its phases (regular season, playoffs, supercup).",
            _fx(path=[_p("season", SEASON)], example={"season": 2026}),
        ),
        _e(
            _CT,
            _CT,
            "competition_teams",
            ("cap", "competition-team__all", {"competition_id": 24}),
            (_CT, {}, "data.[]."),
            "Every competition team of one competition: ids, names and country.",
            _fx(query=[_q("competition_id", COMPETITION, required=True)], example={"competition_id": 24}),
        ),
        _e(
            _CT,
            _CT,
            "competition_staff",
            ("cap", "competition-team__all", {"competition_id": 24}),
            (_CT, {"team.": "data.[]."}, "data.[].staffs.[]."),
            "Coaching staff (head coach, assistants) of every competition team of one competition.",
            _fx(query=[_q("competition_id", COMPETITION, required=True)], example={"competition_id": 24}),
        ),
        _e(
            "/standings/competition/{competitionId}/phase/{phaseId}",
            "/standings/competition/{competition_id}/phase/{phase_id}",
            "standings",
            ("cap", "standings__competition__competitionId__phase__phaseId", {"competition_id": 24, "phase_id": 169}),
            ("/standings/competition/{competitionId}/phase/{phaseId}", {}, "data.[]."),
            "Standings of one phase of a competition: record, points, home/away split, last five and streak.",
            _fx(
                path=[_p("competition_id", COMPETITION), _p("phase_id", PHASE)],
                example={"competition_id": 24, "phase_id": 169},
            ),
        ),
        _e(
            "/schedule/club/{season}",
            "/schedule/club/{season}",
            "schedule",
            ("full", "schedule__club__season", {"season": 2027}),
            ("/schedule/club/{season}", _COMPETITORS_ROWS, "data.[]."),
            "Every game of a season for Belgian and Dutch clubs, with teams, scores, phase and venue.",
            _fx(
                path=[_p("season", SEASON)],
                query=[
                    _q(
                        "month",
                        "Calendar month filter; -1 (default) = the whole season in one body.",
                        default=-1,
                    ),
                    PHASE_Q,
                ],
                fixed=CLUBS,
                example={"season": 2026},
            ),
        ),
        _e(
            "/schedule/game-slider/{competitionId}",
            "/schedule/game-slider/{competition_id}",
            "game_slider",
            ("full", "schedule__game-slider__competitionId", {"competition_id": 25}),
            ("/schedule/game-slider/{competitionId}", _COMPETITORS_ROWS, "data.[]."),
            "The latest 14 finished games of a competition plus every upcoming game, with the Genius match id.",
            _fx(path=[_p("competition_id", COMPETITION)], example={"competition_id": 25}),
        ),
        _e(
            "/competition-team/average-rankings/{competitionId}",
            "/competition-team/average-rankings/{competition_id}",
            "team_rankings",
            ("cap", "competition-team__average-rankings__competitionId", {"competition_id": 24}),
            ("/competition-team/average-rankings/{competitionId}", {}, "efficiency.[]."),
            "Team per-game leaders in nine categories (one row per team and category).",
            _fx(path=[_p("competition_id", COMPETITION)], query=[PHASE_Q, LIMIT_Q], example={"competition_id": 24}),
        ),
        _e(
            "/player/average-rankings/{competitionId}",
            "/player/average-rankings/{competition_id}",
            "player_rankings",
            ("cap", "player__average-rankings__competitionId", {"competition_id": 24}),
            ("/player/average-rankings/{competitionId}", {}, "efficiency.[]."),
            "Qualified-player per-game leaders in nine categories (one row per player and category).",
            _fx(
                path=[_p("competition_id", COMPETITION)],
                query=[PHASE_Q, LIMIT_Q, TYPE_Q],
                example={"competition_id": 24},
            ),
        ),
        _e(
            "/roster/season-players/{season}",
            "/roster/season-players/{season}",
            "season_roster",
            ("full", "roster__season-players__season", {"season": 2027}),
            ("/roster/season-players/{season}", {}, "data.[]."),
            "Every roster entry of a season: player, team, jersey, height, weight and position.",
            _fx(path=[_p("season", SEASON)], example={"season": 2026}),
        ),
        _e(
            "/roster/season-nationality-codes/{season}",
            "/roster/season-nationality-codes/{season}",
            "nationality_codes",
            ("cap", "roster__season-nationality-codes__season", {"season": 2026}),
            ("/roster/season-nationality-codes/{season}", {}, ""),
            "Distinct player nationality codes of a season (dirty: lower case, pairs, empty).",
            _fx(path=[_p("season", SEASON)], example={"season": 2026}),
        ),
        _e(
            "/roster/team-players/{competitionTeamId}",
            "/roster/team-players/{competition_team_id}",
            "team_roster",
            ("cap", "roster__team-players__competitionTeamId", {"competition_team_id": 506}),
            ("/roster/team-players/{competitionTeamId}", {}, "data.[]."),
            "Roster of one competition team (current entries only).",
            _fx(path=[_p("competition_team_id", TEAM)], example={"competition_team_id": 541}),
        ),
        _e(
            "/competition-team/find/{competitionTeamId}",
            "/competition-team/find/{competition_team_id}",
            "team_find",
            ("cap", "competition-team__find__competitionTeamId", {"competition_team_id": 506}),
            ("/competition-team/find/{competitionTeamId}", {}, "data."),
            "One competition team with its season scoring averages.",
            _fx(path=[_p("competition_team_id", TEAM)], example={"competition_team_id": 541}),
        ),
        _e(
            "/competition-team/player-averages/{competitionTeamId}",
            "/competition-team/player-averages/{competition_team_id}",
            "team_player_averages",
            ("full", "competition-team__player-averages__competitionTeamId", {"competition_team_id": 541}),
            ("/competition-team/player-averages/{competitionTeamId}", {}, "data.[]."),
            "Per-game averages of each player of a competition team.",
            _fx(path=[_p("competition_team_id", TEAM)], example={"competition_team_id": 541}),
        ),
        _e(
            "/competition-team/statistics-by-game/total/{competitionTeamId}",
            "/competition-team/statistics-by-game/total/{competition_team_id}",
            "team_games",
            ("cap", "competition-team__statistics-by-game__total__competitionTeamId", {"competition_team_id": 506}),
            ("/competition-team/statistics-by-game/total/{competitionTeamId}", _COMPETITORS_ROWS, "data.[]."),
            "Every finished game of a competition team with that team's totals.",
            _fx(path=[_p("competition_team_id", TEAM)], example={"competition_team_id": 541}),
        ),
        _e(
            "/roster/find/{rosterId}",
            "/roster/find/{roster_id}",
            "roster_find",
            ("cap", "roster__find__rosterId", {"roster_id": 10609}),
            ("/roster/find/{rosterId}", {}, "data."),
            "One roster entry with its player, team and season.",
            _fx(path=[_p("roster_id", ROSTER)], example={"roster_id": 11193}),
        ),
        _e(
            "/player/career-averages/{rosterId}",
            "/player/career-averages/{roster_id}",
            "player_career",
            ("cap", "player__career-averages__rosterId", {"roster_id": 10609}),
            ("/player/career-averages/{rosterId}", {}, "data.[]."),
            "Season-by-season per-game averages of the player of a roster entry.",
            _fx(path=[_p("roster_id", ROSTER)], example={"roster_id": 11193}),
        ),
        _e(
            "/boxscore/player/season-games/{rosterId}",
            "/boxscore/player/season-games/{roster_id}",
            "player_games",
            ("cap", "boxscore__player__season-games__rosterId", {"roster_id": 10609}),
            (
                "/boxscore/player/season-games/{rosterId}",
                {"game.home.": "data.[].game.competitors.[].", "game.away.": "data.[].game.competitors.[]."},
                "data.[].",
            ),
            "Game-by-game box lines of a roster entry (including games with 0 minutes).",
            _fx(path=[_p("roster_id", ROSTER)], example={"roster_id": 11193}),
        ),
        _e(
            _SD,
            "/schedule/find/{game_id}",
            "game_find",
            ("full", "schedule__find__gameId", _G),
            (_SD, _COMPETITOR_HOME, "data."),
            "One game: teams, scores, phase, venue and the neighbouring game ids.",
            _fx(path=[_p("game_id", GAME)], example=_G),
        ),
        _e(
            _SD,
            "/schedule/find/{game_id}",
            "game_periods",
            ("full", "schedule__find__gameId", _G),
            (_SD, {}, "data.competitors.[].period_scores.[]."),
            "Points each side scored in each period of a game (periods 5 and up are overtimes).",
            _fx(path=[_p("game_id", GAME)], example=_G),
        ),
        _e(
            _OV,
            "/schedule/overview/{game_id}",
            "game_overview",
            ("full", "schedule__overview__gameId", _G),
            (_OV, {}, "data.team_totals.[]."),
            "Team totals of both sides of a finished game.",
            _fx(path=[_p("game_id", GAME)], example=_G),
        ),
        _e(
            _OV,
            "/schedule/overview/{game_id}",
            "game_top_performers",
            ("full", "schedule__overview__gameId", _G),
            (_OV, {}, "data.top_performer.[]."),
            "The top performer of each side of a finished game, with his full line.",
            _fx(path=[_p("game_id", GAME)], example=_G),
        ),
        _e(
            _OV,
            "/schedule/overview/{game_id}",
            "game_leaders",
            ("full", "schedule__overview__gameId", _G),
            (_OV, {"leader.": "data.game_leaders.[].leaders.points."}, "data.game_leaders.[]."),
            "The game leader of each side in every statistic (one row per side and statistic).",
            _fx(path=[_p("game_id", GAME)], example=_G),
        ),
        _e(
            _BX,
            "/boxscore/game/{competition_id}/{game_id}",
            "game_box",
            ("full", "boxscore__game__competitionId__gameId", _GC),
            (_BX, {"team.": "data.[].team."}, "data.[].players.[]."),
            "Per-player box score of a finished game (one row per player line).",
            _fx(path=[_p("competition_id", COMPETITION), _p("game_id", GAME)], example=_GC),
        ),
        _e(
            _BX,
            "/boxscore/game/{competition_id}/{game_id}",
            "game_team_box",
            ("full", "boxscore__game__competitionId__gameId", _GC),
            (_BX, {"team.": "data.[].team.", "total.": "data.[].total."}, "data.[]."),
            "Team totals of both sides from the box score of a finished game.",
            _fx(path=[_p("competition_id", COMPETITION), _p("game_id", GAME)], example=_GC),
        ),
        _e(
            "/film/game/{competitionId}/{gameId}",
            "/film/game/{competition_id}/{game_id}",
            "game_film",
            ("full", "film__game__competitionId__gameId", _GC),
            ("/film/game/{competitionId}/{gameId}", {}, "data.[]."),
            "Play-by-play ('film') of a finished game with court x/y (2021-22 and 2026-27 only).",
            _fx(path=[_p("competition_id", COMPETITION), _p("game_id", GAME)], example=_GC),
        ),
    ]
)

#: Descriptions of the columns the parsers add, re-key or derive (they win over the returns md).
LOCAL: Dict[str, str] = {
    "season": "Season END year (2026 = 2025-26), the sdv convention and the API's own season key.",
    "competition_id": "Competition id (key space: sportpress competitions): ONE season of the league "
    "(24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes.",
    "phase_id": "Phase id (key space: sportpress phases), a stage of one competition such as the regular season or "
    "the playoffs; new every season.",
    "game_id": "Game id (key space: sportpress games), unique across seasons: the key of every game route.",
    "competition_team_id": "Competition-team id (key space: sportpress competition teams): a team in ONE "
    "competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). "
    "uu_team_id is the stable club key.",
    "uu_team_id": "Stable club key across seasons (key space: sportpress clubs-as-teams); competition_team_id "
    "changes every season, this does not.",
    "roster_id": "Roster id (key space: sportpress roster entries): one player on one competition team in one "
    "season, so a player has a new roster_id for every team and season. player_id is the person.",
    "player_id": "Player id (key space: sportpress persons): stable across seasons and teams, distinct from "
    "roster_id. Not a Genius or FIBA person id.",
    "first_name": "Given name of the player as entered in the CMS.",
    "last_name": "Family name of the player as entered in the CMS.",
    "nationality_code": "Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, "
    "'/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining.",
    "birth_date": "Date of birth as shown on the public player page; null when unknown.",
    "team": "Team display name including the sponsor; varies with language and playoff suffixes: never join on it.",
    "team_short_name": "Three-letter club code (ANT, MON, ...), stable across seasons in the samples.",
    "club_id": "Country of the team (key space: sportpress clubs): 1 = Belgium, 2 = Netherlands. The provider calls "
    "the country a club.",
    "club_name": "Country name in English, Belgium or Netherlands.",
    "team_side": "Side of the game the row belongs to: home or away (the provider's is_home, or side 1 / 2).",
    "jersey": "Shirt number as text; the box score sends it as an integer, so a zero-padded '00' is already "
    "lost there.",
    "game_time": "Tip-off as a naive datetime. The source carries no offset; the league's local time is assumed "
    "and unverified.",
    "status": "Game status: finished, or ready for an upcoming game.",
    "arena": "Name of the arena where the game is played.",
    "round_name": "Round label as entered in the CMS; often empty.",
    "round_priority": "Sort order of the round within the phase (provider's priority); meaning of the values "
    "is unverified.",
    "period_name": "Name of the period the game is in or ended in; 'Extra2' means the second overtime.",
    "phase_name": "Phase name as entered in the CMS ('Regular season 2026-27').",
    "starter": "True when the player was in the starting five.",
    "minute": "Minutes played; for a team total, the sum over its players (200 + 25 per overtime).",
    "foul": "Personal fouls committed.",
    "defensive_foul": "Fouls DRAWN (suffered), despite the name: equals the opponent's foul count up to scorer "
    "corrections.",
    "dunk_made": "Dunks made (a subset of the two-point field goals).",
    "dunk_all": "Dunks attempted.",
    "dunk_percent": "Dunk percentage 0-100; 0 when none were attempted.",
    "two_point_made": "Two-point field goals made.",
    "two_point_all": "Two-point field goals attempted.",
    "two_point_percent": "Two-point percentage 0-100.",
    "three_point_made": "Three-point field goals made.",
    "three_point_all": "Three-point field goals attempted.",
    "three_point_percent": "Three-point percentage 0-100.",
    "throw_made": "All field goals made, two- plus three-pointers (the provider calls them 'throw').",
    "throw_all": "All field goals attempted, two- plus three-pointers (the provider calls them 'throw').",
    "throw_percent": "Field-goal percentage 0-100.",
    "free_throw_made": "Free throws made.",
    "free_throw_all": "Free throws attempted.",
    "free_throw_percent": "Free-throw percentage 0-100.",
    "offensive_rebound": "Offensive rebounds.",
    "defensive_rebound": "Defensive rebounds.",
    "total_rebound": "Total rebounds (offensive plus defensive).",
    "block": "Shots blocked by the player.",
    "block_against": "Own shots blocked by an opponent.",
    "turnover": "Turnovers committed.",
    "steal": "Steals (balls taken from an opponent).",
    "assist": "Assists (passes leading directly to a made basket).",
    "points": "Points scored (2 x two-pointers + 3 x three-pointers + free throws).",
    "value": "Efficiency rating (PIR with fouls drawn): points + rebounds + assists + steals + blocks + fouls "
    "drawn - missed field goals - missed free throws - turnovers.",
    "plusminus": "Plus/minus: the score margin while the player (or team) was on court; for a team total it is the "
    "game margin.",
}
_AVG = " Per-game mean over the games played (fractional)."
#: Per-wrapper descriptions where a column means something different.
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "ui_strings": {
        "language": "ISO 639-1 code of the UI language: en, fr (French) or nl (Dutch).",
        "key": "Dotted UI string key such as 'statistics.individual.points'; 302 keys per language.",
        "text": "The UI text in that language; 11 keys label statistics columns, the rest are menus and buttons.",
    },
    "club_seasons": {"season": "Season END year in which the clubs (countries) have teams; includes 2021 = 2020-21."},
    "referees": {
        "staff_id": "Staff member id (key space: sportpress staff); the referees are title 2.",
        "name": "Referee name 'First Last' (free text).",
        "title_id": "Staff title id: 1 = Head coach, 2 = Referee; Assistant coach has its own id.",
        "title_name": "Staff title in English: Head coach, Assistant coach or Referee.",
    },
    "phases": {
        "competition_name": "Competition name, 'BNXT League 2025-2026'.",
        "phase_id": "Phase id (key space: sportpress phases); the {phaseId} of the standings route. New every season.",
        "phase_name": "Phase name as entered in the CMS; names differ every season.",
        "standings_visible": "Whether the phase shows standings; false for playoff phases, which answer the "
        "standings route empty.",
    },
    "competition_teams": {
        "competition_id": "Competition id the team plays in (key space: sportpress competitions); the "
        "competition_id the route was asked for.",
        "season": "Season END year of the team's competition.",
    },
    "competition_staff": {
        "competition_id": "Competition id of the team the staff member works for.",
        "name": "Staff member name 'First Last' (free text).",
        "title_id": "Staff title id: 1 = Head coach; Assistant coach has its own id.",
        "title_name": "Staff title in English: Head coach or Assistant coach.",
        "staff_id": "Staff member id (key space: sportpress staff).",
    },
    "standings": {
        "position": "Rank in the table, 1 = first.",
        "played": "Games played in the phase (win + loss + even).",
        "score": "Points scored in the phase.",
        "against": "Points conceded in the phase.",
        "percent": "Win percentage 0-100, rounded to 1 decimal.",
        "point": "League points: 2 per win plus 1 per loss.",
        "point_percent": "Share of the possible league points won, 0-1 rounded to 2 decimals.",
        "home_win": "Wins in home games.",
        "home_loss": "Losses in home games.",
        "away_win": "Wins in away games.",
        "away_loss": "Losses in away games.",
        "last5_win": "Wins among the last five games.",
        "last5_loss": "Losses among the last five games.",
        "streak_is_win": "True for a winning streak, false for a losing one.",
        "streak_count": "Length of the current streak in games.",
        "group": "Group label for split phases; null in every sample.",
        "even": "Games drawn; 0 in every sample (basketball has no draws).",
    },
    "schedule": {"season": "Season END year of the game's competition."},
    "game_slider": {
        "genius_id": "Genius Sports LiveStats match id (organisation BNXT): the join to the genius family. "
        "Upcoming games already carry one; null for the 2020-21 competition.",
    },
    "team_rankings": {
        "category": "Ranking category: efficiency, points, total_rebound, block, assist, steal, two_point_percent, "
        "three_point_percent or free_throw_percent.",
        "rank": "Place in the category list, 1 = the leader (the list is sorted best first).",
        "value": "The team's per-game mean (or percentage 0-100) in the row's category.",
    },
    "player_rankings": {
        "category": "Ranking category: efficiency, points, total_rebound, block, assist, steal, two_point_percent, "
        "three_point_percent or free_throw_percent.",
        "rank": "Place in the category list, 1 = the leader (the list is sorted best first).",
        "value": "The player's per-game mean (or percentage 0-100) in the row's category.",
        "throw_made": "Field goals made per game; filled in the three percentage categories only.",
        "throw_attempted": "Field goals attempted per game; filled in the three percentage categories only.",
    },
    "season_roster": {"season": "Season END year the roster was asked for."},
    "nationality_codes": {
        "season": "Season END year the codes were asked for.",
        "nationality_code": "A distinct nationality code of the season's players, as served (dirty).",
    },
    "team_find": {
        "season": "Season END year of the team's competition.",
        "competition_name": "Competition name, 'BNXT League 2026-2027'.",
        "average_score_for": "Points scored per game by the team over the season; null for a team with no statistics.",
        "average_score_against": "Points conceded per game; null for a team with no statistics.",
        "field_goal_percent": "Team field-goal percentage 0-100; null for a team with no statistics.",
    },
    "team_player_averages": {
        "played_games": "Games the player played in the season.",
        "games_started": "Games started (a count, not a mean).",
        **{k: v + _AVG for k, v in LOCAL.items() if k in P.SPECS["team_player_averages"] and k in P._STATS},
    },
    "team_games": {
        **{k: "The team's own " + v[0].lower() + v[1:] for k, v in LOCAL.items() if k in P._STATS or k == "plusminus"},
        "competition_team_id": "Competition-team id the games were asked for (the team the totals belong to).",
        "season": "Season END year of the game's competition.",
    },
    "roster_find": {
        "competition_id": "Competition id the roster entry's team plays in.",
        "season": "Season END year of the roster entry's competition.",
        "team": "Team display name including the sponsor.",
        "team_short_name": "Three-letter club code of the team.",
        "uu_team_id": "Stable club key across seasons of the team.",
    },
    "player_career": {
        "roster_id": "Roster id the career was asked for; the rows are the person's seasons, each with its own roster.",
        "competition_id": "Competition id of the season row.",
        "season": "Season END year of the row.",
        "competition_name": "Competition name of the season row.",
        "position": "Playing position; null in every sample.",
        "played_games": "Games played in the season.",
        "games_started": "Games started (a count, not a mean).",
        **{k: v + _AVG for k, v in LOCAL.items() if k in P._STATS},
    },
    "player_games": {
        "roster_id": "Roster id the lines were asked for.",
        "competition_team_id": "Competition-team id the player played for in the game.",
        "season": "Season END year of the game's competition.",
        "starter": "True when the player started the game.",
    },
    "game_find": {
        "last_game_id": "Latest finished meeting of the same two teams in the competition, which can be LATER than "
        "this game; null when none.",
        "next_game_id": "Next meeting of the same two teams; null in every sample.",
    },
    "game_periods": {
        "competition_team_id": "Competition-team id of the side.",
        "period": "Period number: 1-4 are the quarters, 5 and up are overtimes (5 = first overtime).",
        "score": "Points the side scored IN that period (not cumulative).",
    },
    "game_overview": {
        "game_id": "Game id the overview was asked for (the body does not echo it).",
        "score": "Final score of the side.",
        **{
            k: "The team's total: " + v[0].lower() + v[1:]
            for k, v in LOCAL.items()
            if k in P._STATS or k == "plusminus"
        },
    },
    "game_top_performers": {
        "game_id": "Game id the overview was asked for (the body does not echo it).",
        "starter": "True when the player started the game.",
        "jersey": "Shirt number as text.",
    },
    "game_leaders": {
        "game_id": "Game id the overview was asked for (the body does not echo it).",
        "stat": "Statistic the leader leads, in the provider's column name (points, total_rebound, value, ...).",
        "value": "The leader's count (or percentage) in that statistic in this game.",
    },
    "game_box": {
        "game_id": "Game id the box score was asked for (the body does not echo it).",
        "competition_id": "Competition id the box score was asked for; checked against the game.",
        "position_id": "Playing position id (key space: sportpress positions); null on 21 of 14,142 sampled lines.",
        "position": "Playing position: point_guard, shooting_guard, small_forward, power_forward or center.",
        "roster_id": "Roster id of the line; null when the line has no player (15 of 14,142 sampled lines).",
        "starter": "True when the player was in the starting five.",
    },
    "game_team_box": {
        "game_id": "Game id the box score was asked for (the body does not echo it).",
        "competition_id": "Competition id the box score was asked for; checked against the game.",
        **{
            k: "The team's total: " + v[0].lower() + v[1:]
            for k, v in LOCAL.items()
            if k in P._STATS or k == "plusminus"
        },
        "points": "Team points in the box score; can differ from the schedule final score (about 18 of 1,246 "
        "sampled team boxes, by 2-21 points).",
    },
    "game_film": {
        "game_id": "Game id the film was asked for (the body does not echo it).",
        "competition_id": "Competition id the film was asked for; checked against the game.",
        "order": "Event sequence number in the game, strictly increasing from 0.",
        "period": "Period of the event: 1-4 quarters, 5 and up overtimes.",
        "minute": "Whole minutes ELAPSED in the period (not clock remaining).",
        "second": "Seconds elapsed within that minute.",
        "event_code": "Provider event code: 1000 made shot, 1001 missed shot, 1002 defensive rebound, 1003 offensive "
        "rebound, 1004 steal, 1005 turnover, 1006 foul drawn, 1007 foul committed, 1008 assist, 1009 block, 1011 "
        "substitution, 1101 period marker; 2xxx team events (meaning unverified).",
        "event_subcode": "Provider sub-type of the event: for shots 1, 2 and 5 = two-pointer, 3 = three-pointer, "
        "4 = free throw; turnover and foul sub-types are unnamed by the provider.",
        "x": "Court x position of the event in the provider's integer court coordinates (0 on events with none); "
        "the coordinate system and orientation are unverified.",
        "y": "Court y position of the event in the provider's integer court coordinates (0 on events with none); "
        "the coordinate system and orientation are unverified.",
        "home_score": "Running home score AFTER the event; the maximum is the final score.",
        "away_score": "Running away score AFTER the event; the maximum is the final score.",
        "player_id": "Player id of the actor; null on team events and period markers.",
        "player2_id": "Player id of the second player; filled on substitutions only.",
        "player2_first_name": "Given name of the second player; substitutions only.",
        "player2_last_name": "Family name of the second player; substitutions only.",
    },
}

_HEADING = re.compile(r"^## `(/\S*)`")
_ROW = re.compile(r"^\s*\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")


def _md_rows(path: Path) -> Dict[str, Dict[str, str]]:
    """``{route: {record path: description}}`` from the returns md (unobserved rows skipped)."""
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


def _md_path(md: Md, item_path: str) -> str:
    _section, aliases, default = md
    for prefix in sorted(aliases, key=len, reverse=True):
        if item_path.startswith(prefix):
            return aliases[prefix] + item_path[len(prefix) :]
    return default + item_path


def _describe(short: str, col: str, item_path: str, entry: Entry, md: Dict[str, Dict[str, str]]) -> str:
    if col in LOCAL_BY.get(short, {}):
        return LOCAL_BY[short][col]
    if col in LOCAL:
        return LOCAL[col]
    section = entry[4][0]
    text = md.get(section, {}).get(_md_path(entry[4], item_path), "")
    if (
        text
        and entry[4][1]
        and any(item_path.startswith(a) for a in entry[4][1])
        and short
        in (
            "schedule",
            "game_slider",
            "team_games",
            "player_games",
            "game_find",
        )
    ):
        side = "Home" if col.startswith("home_") else "Away"
        return f"{side} side. {text}"
    return text


def _full(name: str) -> Dict[str, Any]:
    gz = FIXTURES / "full" / f"{name}.json.gz"
    text = (
        gzip.open(gz, "rt", encoding="utf-8").read()
        if gz.exists()
        else (FIXTURES / "full" / f"{name}.json").read_text("utf-8")
    )
    return json.loads(text)


def _fixture(src: Source) -> Dict[str, Any]:
    kind, name, ctx = src
    body = _full(name) if kind == "full" else json.loads((FIXTURES / "captures" / f"{name}.json").read_text("utf-8"))
    if isinstance(body, list):
        body = {"data": body}
    body["_request"] = dict(ctx)
    return body


def _frame(entry: Entry) -> pl.DataFrame:
    """The parser's frame over the entry's fixture."""
    frame = entry[2](_fixture(entry[3]))
    assert isinstance(frame, pl.DataFrame)
    return frame


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    _sp, path, parser, _src, _md, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/sportpress/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("sportpress", SPEC)
    spec = load_spec(refs, SPEC)
    differs = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if differs:
        raise SystemExit(f"sportpress: spec paths and ENDPOINTS differ: {sorted(differs)}")
    assert set(P.SPECS) == set(ENDPOINTS) == set(P.ITEMS), "every parser spec needs an endpoint and the reverse"
    doc = {
        "api": "sportpress",
        "host": HOST,
        "name_pattern": "sportpress_{short}",
        "module": "sportpress",
        "parser_module": "basketball.bnxt.bnxt_parsers",
        "getter_module": "sportsdataverse.basketball.bnxt.bnxt_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (schedule, standings) name other families
        "schema_prefix": "sportpress_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw body (``dict``): ``{'data': ...}`` (the settings object for the site config, the "
            "nine-category object for the rankings) with contact fields dropped, plus ``_request`` (the season, "
            "competition, team, roster or game ids asked for)",
            "raises": [
                "sportsdataverse.errors.NoDataError: the API has nothing for the request (HTTP 404, an empty "
                "``data`` list: an unknown id or a season without data, an empty ranking).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for "
                "(a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a "
                "body for another season, competition, team or game).",
            ],
            "notes": [UNOFFICIAL, SEASONS, COVERAGE, DATA_NOTES],
            "see_also": [
                {
                    "name": "BNXT League",
                    "url": "https://bnxtleague.com",
                    "note": "the site whose Sportpress backend these wrappers read",
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
    write_yaml(ROOT / "tools/codegen/endpoints/sportpress.yaml", doc)

    md = _md_rows(refs / "sportpress-returns.md")
    schema_dir = ROOT / "tools/codegen/schemas/native/sportpress"
    rewrite_schema_dir(schema_dir)
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        cols: List[Dict[str, str]] = columns_from_frame(_frame(entry), [], leaf_fallback=False)
        block = notes.setdefault(f"sportpress_{short}", {})
        for col in cols:
            item_path = P.SPECS[short][col["name"]][1]
            text = _describe(short, col["name"], item_path, entry, md).strip()
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"sportpress_{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(
            schema_dir / f"{short}.yaml", {"schema": f"sportpress_{short}", "kind": "dataframe", "columns": cols}
        )
    if bad:
        raise SystemExit("sportpress: empty or filler descriptions (add them to LOCAL / LOCAL_BY):\n" + "\n".join(bad))
    NOTES.parent.mkdir(parents=True, exist_ok=True)
    write_yaml(NOTES, notes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"sportpress: {len(ENDPOINTS)} endpoints, {sum(map(len, notes.values()))} descriptions -> {NOTES.name}")


if __name__ == "__main__":
    main()
