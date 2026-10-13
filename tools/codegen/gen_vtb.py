"""Generate the ``vtb_league`` and ``vtb_infobasket`` flat-API endpoint YAML + returns-schemas (VTB United League).

Idempotent: same specs + same fixtures -> byte-identical output. Modeled on ``gen_kzs.py`` with the shared plumbing in
``gen_soccer_common.py``. Two families, one script (they share one descriptions file and one dtypes module). Reads the
frozen specs and the returns md from the ``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling
checkout) and emits:

* ``tools/codegen/endpoints/vtb_league.yaml`` (3 wrappers over the v2 API's 3 routes) and
  ``tools/codegen/endpoints/vtb_infobasket.yaml`` (35 wrappers over the 24 Infobasket routes; one route can feed several
  frames, :data:`INFOBASKET` is the source of truth).
* ``tools/codegen/schemas/native/vtb_league/<short>.yaml`` and ``.../vtb_infobasket/<short>.yaml`` -- one returns-schema per
  wrapper: the columns of its parser run over its committed fixtures (the FULL bodies in ``tests/fixtures/<stem>/full/``
  plus the refs captures in ``tests/fixtures/<stem>/captures/``). Schema key ``<stem>_<short>``; every ``description`` is
  ``''``.
* ``tools/codegen/notes/vtb_descriptions.yaml`` -- the column descriptions (committed: the generator owns the text), keyed
  ``<stem>_<short>`` -> column, for ``manual_column_descriptions.yaml``. Each comes from :data:`ROUTE` (route-specific),
  the route's own ``vtb-returns.md`` row (:data:`SECTIONS`), else :data:`LOCAL` / :func:`_team_name_desc` (parser-built
  columns). A filler or missing description stops the run.
* ``sportsdataverse/basketball/vtb/vtb_dtypes.py`` -- each parser's column dtypes (the returns-schema types), so an empty or
  all-null frame is typed like a populated one.
* ``tests/fixtures/vtb_league/spec_paths.json`` and ``tests/fixtures/vtb_infobasket/spec_paths.json`` -- the specs' routes, for
  the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_vtb.py``
"""

from __future__ import annotations

import copy
import gzip
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.vtb import vtb_frames as F
from sportsdataverse.basketball.vtb import vtb_infobasket_parsers as IB
from sportsdataverse.basketball.vtb import vtb_league_parsers as LG
from sportsdataverse.dl_utils import underscore

SNIPPET = ROOT / "tools" / "codegen" / "notes" / "vtb_descriptions.yaml"
DTYPES_MODULE = ROOT / "sportsdataverse" / "basketball" / "vtb" / "vtb_dtypes.py"
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

# A fixture is (path relative to tests/fixtures/<stem>, the request the runtime would stamp under ``_request``).
Fixture = Tuple[str, Dict[str, Any]]
# short -> (spec path, wrapper path, parser, fixtures, summary, endpoint fields)
Entry = Tuple[str, str, Callable[..., Any], List[Fixture], str, Dict[str, Any]]


def _q(name: str, wire: str, desc: str, *, typ: str = "str", required: bool = False, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": desc}
    if required:
        out["required"] = True
    out.update(extra)
    return out


def _p(name: str, desc: str, typ: str = "int", **extra: Any) -> Dict[str, Any]:
    return {"name": name, "type": typ, "required": True, "description": desc, **extra}


# --- shared parameter text ---------------------------------------------------------------------------------------------

LEAGUE_SLUG = "League slug (league in vtb_league_leagues): vtb (VTB United League), vtbyouth (youth league) or wbc (WINLINE Basket Cup)."
V2_SEASON = "Season END year (2026 = 2025-26). The v2 API holds 2025 onwards (vtb: 2025-2027; wbc: 2026-2027); earlier years answer no rows."
SEASON_P = (
    "Season END year (2026 = 2025-26); the wrapper maps it to the season-level Infobasket CompID with "
    "vtb_infobasket_seasons (one extra request per tag, cached)."
)
SEASON_Q = SEASON_P + " Required."
STAGE_ID = (
    "Stage CompID (comp_id in vtb_infobasket_comp_issue), not a season id and not an END year, e.g. 50720 (Regular Season "
    "2025-26) or 54281 (Playoff 2025-26)."
)
GAME_ID = "Infobasket GameID (game_id in vtb_infobasket_calendar; the v2 matches' match_id), e.g. 921391. Global across seasons."
PERSON_ID = (
    "Infobasket PersonID (person_id in a box score or roster): one person across clubs and seasons, e.g. 423011."
)
TEAM_ID = "Infobasket TeamID (team_id in the calendar, a box score or the v2 matches), e.g. 2650 (Lokomotiv Kuban)."
TAG = (
    "Competition tag scoping the profile: vtb (VTB United League, default), vtbyouth, wbc, wpremier or rfb. The season "
    "table is read for this tag."
)
LANG = "Label language (en or ru). Most bodies carry both languages whatever is asked; the leader categories' labels follow it."
FMT = {"format": "json"}


def _tag() -> Dict[str, Any]:
    return _q("tag", "tag", TAG, default="vtb")


def _lang() -> Dict[str, Any]:
    return _q("lang", "lang", LANG, default="en", choices=["en", "ru"])


def _caps(prefix: str, ctxs: Dict[str, Dict[str, Any]]) -> List[Fixture]:
    return [(f"captures/{prefix}__{key}.json", ctx) for key, ctx in ctxs.items()]


# --- vtb_league --------------------------------------------------------------------------------------------------------

LEAGUE: Dict[str, Entry] = {
    "leagues": (
        "/v2/leagues",
        "/v2/leagues",
        LG.parse_vtb_league_leagues,
        [("captures/v2__leagues.json", {})],
        "The leagues served by the API: vtb, vtbyouth, wbc.",
        {"example_args": {}},
    ),
    "seasons": (
        "/v2/leagues/{league}/seasons",
        "/v2/leagues/{league_slug}/seasons",
        LG.parse_vtb_league_seasons,
        [
            ("captures/v2__leagues__league__seasons__vtb.json", {"league_slug": "vtb"}),
            ("captures/v2__leagues__league__seasons__wbc.json", {"league_slug": "wbc"}),
        ],
        "The seasons the API serves for one league (new seasons only, from 2024-25).",
        {"path_params": [_p("league_slug", LEAGUE_SLUG, "str")], "example_args": {"league_slug": "vtb"}},
    ),
    "matches": (
        "/v2/leagues/{league}/seasons/{season}/matches",
        "/v2/leagues/{league_slug}/seasons/{season}/matches",
        LG.parse_vtb_league_matches,
        [
            ("full/matches_vtb_2026.json.gz", {"league_slug": "vtb", "season": 2026}),
            (
                "captures/v2__leagues__league__seasons__season__matches__vtb_2026.json",
                {"league_slug": "vtb", "season": 2026},
            ),
            (
                "captures/v2__leagues__league__seasons__season__matches__wbc_2026.json",
                {"league_slug": "wbc", "season": 2026},
            ),
        ],
        "Every match of one league-season (schedule, scores, quarter points, venue), in one request.",
        {
            "path_params": [_p("league_slug", LEAGUE_SLUG, "str"), _p("season", V2_SEASON)],
            "fixed_params": {"limit": 1000},
            "example_args": {"league_slug": "vtb", "season": 2026},
        },
    ),
}
LEAGUE_SECTIONS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "leagues": ("/v2/leagues", ("data_",)),
    "seasons": ("/v2/leagues/{league}/seasons", ("data_",)),
    "matches": ("/v2/leagues/{league}/seasons/{season}/matches", ("data_",)),
}

# --- vtb_infobasket ----------------------------------------------------------------------------------------------------

IBC = "captures/{}.json"
S26 = {"season": 2026, "season_comp_id": 50714, "tag": "vtb"}
S23 = {"season": 2023, "season_comp_id": 39799, "tag": "vtb"}
S14 = {"season": 2014, "season_comp_id": 9113, "tag": "vtb"}
G26, G23, G14 = {"game_id": 921391}, {"game_id": 647209}, {"game_id": 684495}
T = {"team_id": 2650, **S26}
PLAYER = {"person_id": 423011, "tag": "vtb"}


def _cap(name: str, ctx: Dict[str, Any]) -> Fixture:
    return (IBC.format(name), ctx)


_GAME_CAPS = [
    _cap("Widget__GetOnline__gameId__2026", G26),
    _cap("Widget__GetOnline__gameId__2023", G23),
    _cap("Widget__GetOnline__gameId__2014", G14),
]
_ONLINE: List[Fixture] = [("full/online_921391.json.gz", G26), *_GAME_CAPS]
_CAL: List[Fixture] = [
    ("full/calendar_50714.json.gz", S26),
    _cap("Comp__GetCalendar__2026", S26),
    _cap("Comp__GetCalendar__2023", S23),
    _cap("Comp__GetCalendar__2014", S14),
]
_TEAMSTATS: List[Fixture] = [("full/team_stats_50714.json.gz", S26), _cap("Comp__GetTeamStats__compId__2026", S26)]
_TEAMSEASON: List[Fixture] = [
    ("full/team_season_stats_2650_50714.json", T),
    _cap("Widget__TeamStats__teamId__2026", T),
]
_ROSTER: List[Fixture] = [("full/roster_2650_50714.json", T), _cap("Widget__TeamRoster__teamId__2026", T)]
_PSEASON: List[Fixture] = [
    ("full/player_season_stats_423011.json", PLAYER),
    _cap("Widget__PlayerSeasonStats__personId", PLAYER),
]
_PLOG = {**PLAYER, "team_id": 2650}
_PLOGS: List[Fixture] = [
    ("full/player_game_log_423011_50714.json.gz", _PLOG),
    _cap("Widget__PlayerStats__personId__2026", _PLOG),
]
_ONLINE_P = [_p("game_id", GAME_ID)]
_PERSON_P = [_p("person_id", PERSON_ID)]
_TEAM_P = [_p("team_id", TEAM_ID)]
_SEASON_PATH = [_p("season", SEASON_P)]
_STAGE_PATH = [_p("comp_id", STAGE_ID)]

INFOBASKET: Dict[str, Entry] = {
    "seasons": (
        "/Comp/GetSeasonsForTag",
        "/Comp/GetSeasonsForTag",
        IB.parse_vtb_infobasket_seasons,
        [_cap("Comp__GetSeasonsForTag__vtb", {"tag": "vtb"})],
        "Season END year -> season-level CompID for a competition tag (the table every season-keyed wrapper maps with).",
        {"extra_params": [_tag()], "fixed_params": FMT, "example_args": {}},
    ),
    "calendar": (
        "/Comp/GetCalendar/",
        "/Comp/GetCalendar/",
        IB.parse_vtb_infobasket_calendar,
        _CAL,
        "Every game of a season (finished, live and planned): teams, scores, date, venue, attendance.",
        {
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True), _tag()],
            "fixed_params": FMT,
            "example_args": {"season": 2026},
        },
    ),
    "team_stats": (
        "/Comp/GetTeamStats/{compId}",
        "/Comp/GetTeamStats/{season}",
        IB.parse_vtb_infobasket_team_stats,
        _TEAMSTATS,
        "Season totals of every team of a season (all stages).",
        {
            "path_params": _SEASON_PATH,
            "extra_params": [_tag()],
            "fixed_params": {"page": 0},
            "example_args": {"season": 2026},
        },
    ),
    "team_stats_players": (
        "/Comp/GetTeamStats/{compId}",
        "/Comp/GetTeamStats/{season}",
        IB.parse_vtb_infobasket_team_stats_players,
        _TEAMSTATS,
        "Season totals of every player of every team of a season.",
        {
            "path_params": _SEASON_PATH,
            "extra_params": [_tag()],
            "fixed_params": {"page": 0},
            "example_args": {"season": 2026},
        },
    ),
    "team_stats_coaches": (
        "/Comp/GetTeamStats/{compId}",
        "/Comp/GetTeamStats/{season}",
        IB.parse_vtb_infobasket_team_stats_coaches,
        _TEAMSTATS,
        "Technical fouls and games of every coach of every team of a season.",
        {
            "path_params": _SEASON_PATH,
            "extra_params": [_tag()],
            "fixed_params": {"page": 0},
            "example_args": {"season": 2026},
        },
    ),
    "best_players": (
        "/Comp/GetBestPlayers/{compId}",
        "/Comp/GetBestPlayers/{season}",
        IB.parse_vtb_infobasket_best_players,
        [_cap("Comp__GetBestPlayers__compId__2026", S26)],
        "Player leaders of a season for the first `count` leader categories.",
        {
            "path_params": _SEASON_PATH,
            "extra_params": [
                _q("count", "count", "Leader categories to return (the site's widget asks 5).", typ="int", default=5),
                _lang(),
                _tag(),
            ],
            "example_args": {"season": 2026},
        },
    ),
    "best_teams": (
        "/Comp/GetBestTeams/{compId}",
        "/Comp/GetBestTeams/{season}",
        IB.parse_vtb_infobasket_best_teams,
        [_cap("Comp__GetBestTeams__compId__2026", S26)],
        "Team leaders of a season for the first `count` leader categories.",
        {
            "path_params": _SEASON_PATH,
            "extra_params": [
                _q("count", "count", "Leader categories to return (the site's widget asks 5).", typ="int", default=5),
                _lang(),
                _tag(),
            ],
            "example_args": {"season": 2026},
        },
    ),
    "team_preview": (
        "/Comp/GetTeamStatsForPreview/{gameId}",
        "/Comp/GetTeamStatsForPreview/{game_id}",
        IB.parse_vtb_infobasket_team_preview,
        [_cap("Comp__GetTeamStatsForPreview__gameId__2026", {**G26, "season_comp_id": 50714})],
        "Season totals of a game's two teams (the pre-game preview).",
        {
            "path_params": _ONLINE_P,
            "extra_params": [
                _q("season", "season", SEASON_P + " Optional (the host accepts the call without it).", typ="int"),
                _tag(),
            ],
            "example_args": {"game_id": 921391, "season": 2026},
        },
    ),
    "game": (
        "/Widget/GetOnline/{gameId}",
        "/Widget/GetOnline/{game_id}",
        IB.parse_vtb_infobasket_game,
        _ONLINE,
        "Header of a finished game: stage, venue, rules, the two teams and their scores.",
        {"path_params": _ONLINE_P, "fixed_params": FMT, "example_args": {"game_id": 921391}},
    ),
    "box_score": (
        "/Widget/GetOnline/{gameId}",
        "/Widget/GetOnline/{game_id}",
        IB.parse_vtb_infobasket_box_score,
        _ONLINE,
        "Player box score of a finished game (one row per player of each team, DNPs included).",
        {"path_params": _ONLINE_P, "fixed_params": FMT, "example_args": {"game_id": 921391}},
    ),
    "team_box_score": (
        "/Widget/GetOnline/{gameId}",
        "/Widget/GetOnline/{game_id}",
        IB.parse_vtb_infobasket_team_box_score,
        _ONLINE,
        "Team box score of a finished game, with the head coach's technical fouls.",
        {"path_params": _ONLINE_P, "fixed_params": FMT, "example_args": {"game_id": 921391}},
    ),
    "pbp": (
        "/Widget/GetOnline/{gameId}",
        "/Widget/GetOnline/{game_id}",
        IB.parse_vtb_infobasket_pbp,
        _ONLINE,
        "Play-by-play of a finished game with shot x/y (2022-23 onwards; earlier games have no plays).",
        {"path_params": _ONLINE_P, "fixed_params": FMT, "example_args": {"game_id": 921391}},
    ),
    "game_staff": (
        "/Widget/GetOnline/{gameId}",
        "/Widget/GetOnline/{game_id}",
        IB.parse_vtb_infobasket_game_staff,
        _ONLINE,
        "The coaches and officials of a finished game (names and roles only).",
        {"path_params": _ONLINE_P, "fixed_params": FMT, "example_args": {"game_id": 921391}},
    ),
    "game_page": (
        "/Widget/GamePage/{gameId}",
        "/Widget/GamePage/{game_id}",
        IB.parse_vtb_infobasket_game_page,
        [
            _cap("Widget__GamePage__gameId__2026", G26),
            _cap("Widget__GamePage__gameId__2023", G23),
            _cap("Widget__GamePage__gameId__2014", G14),
        ],
        "Thin header of a finished game (use vtb_infobasket_game for teams, box score and plays).",
        {"path_params": _ONLINE_P, "fixed_params": FMT, "example_args": {"game_id": 921391}},
    ),
    "comp_issue": (
        "/Widget/CompIssue/{compId}",
        "/Widget/CompIssue/{season}",
        IB.parse_vtb_infobasket_comp_issue,
        [_cap("Widget__CompIssue__compId__2026", S26)],
        "Stage tree of a season (regular season, playoff, series): the stage ids the standings wrappers take.",
        {"path_params": _SEASON_PATH, "extra_params": [_tag()], "fixed_params": FMT, "example_args": {"season": 2026}},
    ),
    "standings": (
        "/Widget/CompTeamResults/{compId}",
        "/Widget/CompTeamResults/{comp_id}",
        IB.parse_vtb_infobasket_standings,
        [
            ("full/standings_50720.json", {"comp_id": 50720}),
            _cap("Widget__CompTeamResults__compId__2026", {"comp_id": 50720}),
        ],
        "Standings of one stage (a round-robin stage such as the regular season), one row per team.",
        {"path_params": _STAGE_PATH, "fixed_params": FMT, "example_args": {"comp_id": 50720}},
    ),
    "cross_table": (
        "/Widget/CrossTable/{compId}",
        "/Widget/CrossTable/{comp_id}",
        IB.parse_vtb_infobasket_cross_table,
        [_cap("Widget__CrossTable__compId__2026", {"comp_id": 50720})],
        "Head-to-head results grid of a round-robin stage (every game twice, once from each side).",
        {"path_params": _STAGE_PATH, "fixed_params": FMT, "example_args": {"comp_id": 50720}},
    ),
    "playoff": (
        "/Widget/PlayOff/{compId}",
        "/Widget/PlayOff/{comp_id}",
        IB.parse_vtb_infobasket_playoff,
        [_cap("Widget__PlayOff__compId__2026", {"comp_id": 54281})],
        "Playoff bracket of a playoff stage, one row per series.",
        {"path_params": _STAGE_PATH, "fixed_params": FMT, "example_args": {"comp_id": 54281}},
    ),
    "playoff_games": (
        "/Widget/PlayOff/{compId}",
        "/Widget/PlayOff/{comp_id}",
        IB.parse_vtb_infobasket_playoff_games,
        [_cap("Widget__PlayOff__compId__2026", {"comp_id": 54281})],
        "The games of every series of a playoff stage.",
        {"path_params": _STAGE_PATH, "fixed_params": FMT, "example_args": {"comp_id": 54281}},
    ),
    "best_player_parameters": (
        "/Widget/GetBestPlayerParameters/",
        "/Widget/GetBestPlayerParameters/",
        IB.parse_vtb_infobasket_best_player_parameters,
        [_cap("Widget__GetBestPlayerParameters", {})],
        "Player leader categories (a code table).",
        {"extra_params": [_lang()], "fixed_params": FMT, "example_args": {}},
    ),
    "best_team_parameters": (
        "/Widget/GetBestTeamParameters/",
        "/Widget/GetBestTeamParameters/",
        IB.parse_vtb_infobasket_best_team_parameters,
        [_cap("Widget__GetBestTeamParameters", {})],
        "Team leader categories (a code table).",
        {"extra_params": [_lang()], "fixed_params": FMT, "example_args": {}},
    ),
    "player": (
        "/Widget/PlayerPage/{personId}",
        "/Widget/PlayerPage/{person_id}",
        IB.parse_vtb_infobasket_player,
        [_cap("Widget__PlayerPage__personId", PLAYER)],
        "Player profile: names, birth date, height, weight, position.",
        {
            "path_params": _PERSON_P,
            "extra_params": [_tag()],
            "fixed_params": FMT,
            "example_args": {"person_id": 423011},
        },
    ),
    "player_season_stats": (
        "/Widget/PlayerSeasonStats/{personId}",
        "/Widget/PlayerSeasonStats/{person_id}",
        IB.parse_vtb_infobasket_player_season_stats,
        _PSEASON,
        "A player's season-by-season totals in the competition tag.",
        {
            "path_params": _PERSON_P,
            "extra_params": [_tag()],
            "fixed_params": FMT,
            "example_args": {"person_id": 423011},
        },
    ),
    "player_career_stats": (
        "/Widget/PlayerSeasonStats/{personId}",
        "/Widget/PlayerSeasonStats/{person_id}",
        IB.parse_vtb_infobasket_player_career_stats,
        _PSEASON,
        "A player's career totals in the competition tag.",
        {
            "path_params": _PERSON_P,
            "extra_params": [_tag()],
            "fixed_params": FMT,
            "example_args": {"person_id": 423011},
        },
    ),
    "player_game_log": (
        "/Widget/PlayerStats/{personId}",
        "/Widget/PlayerStats/{person_id}",
        IB.parse_vtb_infobasket_player_game_log,
        _PLOGS,
        "A player's game log for one season and one team, one row per game.",
        {
            "path_params": _PERSON_P,
            "extra_params": [
                _q("season", "season", SEASON_Q, typ="int", required=True),
                _q(
                    "team",
                    "team",
                    TEAM_ID + " Required: the body is the player's games for that club.",
                    typ="int",
                    required=True,
                ),
                _q(
                    "filter",
                    "filter",
                    "Site filter code; the widget always sends 1 (meaning unverified).",
                    typ="int",
                    default=1,
                ),
                _tag(),
            ],
            "fixed_params": FMT,
            "example_args": {"person_id": 423011, "season": 2026, "team": 2650},
        },
    ),
    "coach": (
        "/Widget/CoachPage/{personId}",
        "/Widget/CoachPage/{person_id}",
        IB.parse_vtb_infobasket_coach,
        [_cap("Widget__CoachPage__personId", {"person_id": 131047})],
        "Coach profile: names, height, weight (no birth data).",
        {"path_params": _PERSON_P, "fixed_params": FMT, "example_args": {"person_id": 131047}},
    ),
    "team": (
        "/Widget/TeamPage/{teamId}",
        "/Widget/TeamPage/{team_id}",
        IB.parse_vtb_infobasket_team,
        [_cap("Widget__TeamPage__teamId", {"team_id": 2650, "tag": "vtb"})],
        "Club profile: names, region, country, status.",
        {
            "path_params": _TEAM_P,
            "extra_params": [_lang(), _tag()],
            "fixed_params": FMT,
            "example_args": {"team_id": 2650},
        },
    ),
    "team_seasons": (
        "/Widget/GetTeamSeasons/{teamId}",
        "/Widget/GetTeamSeasons/{team_id}",
        IB.parse_vtb_infobasket_team_seasons,
        [_cap("Widget__GetTeamSeasons__teamId", {"team_id": 2650, "tag": "vtb"})],
        "The seasons a club has data for (END year and the club's competition id).",
        {
            "path_params": _TEAM_P,
            "extra_params": [_lang(), _tag()],
            "fixed_params": FMT,
            "example_args": {"team_id": 2650},
        },
    ),
    "team_comps": (
        "/Widget/GetTeamComps/{teamId}",
        "/Widget/GetTeamComps/{team_id}",
        IB.parse_vtb_infobasket_team_comps,
        [_cap("Widget__GetTeamComps__teamId__2026", T)],
        "The competitions a club played in one season.",
        {
            "path_params": _TEAM_P,
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True), _lang(), _tag()],
            "fixed_params": FMT,
            "example_args": {"team_id": 2650, "season": 2026},
        },
    ),
    "roster": (
        "/Widget/TeamRoster/{teamId}",
        "/Widget/TeamRoster/{team_id}",
        IB.parse_vtb_infobasket_roster,
        _ROSTER,
        "A club's player roster in one season.",
        {
            "path_params": _TEAM_P,
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True), _lang(), _tag()],
            "fixed_params": FMT,
            "example_args": {"team_id": 2650, "season": 2026},
        },
    ),
    "roster_coaches": (
        "/Widget/TeamRoster/{teamId}",
        "/Widget/TeamRoster/{team_id}",
        IB.parse_vtb_infobasket_roster_coaches,
        _ROSTER,
        "A club's coaches in one season (no birth data).",
        {
            "path_params": _TEAM_P,
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True), _lang(), _tag()],
            "fixed_params": FMT,
            "example_args": {"team_id": 2650, "season": 2026},
        },
    ),
    "team_games": (
        "/Widget/TeamGames/{teamId}",
        "/Widget/TeamGames/{team_id}",
        IB.parse_vtb_infobasket_team_games,
        [_cap("Widget__TeamGames__teamId__2026", T)],
        "A club's games in one season (the calendar's row shape).",
        {
            "path_params": _TEAM_P,
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True), _tag()],
            "fixed_params": FMT,
            "example_args": {"team_id": 2650, "season": 2026},
        },
    ),
    "team_season_stats": (
        "/Widget/TeamStats/{teamId}",
        "/Widget/TeamStats/{team_id}",
        IB.parse_vtb_infobasket_team_season_stats,
        _TEAMSEASON,
        "A club's season totals in one season.",
        {
            "path_params": _TEAM_P,
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True), _lang(), _tag()],
            "fixed_params": FMT,
            "example_args": {"team_id": 2650, "season": 2026},
        },
    ),
    "team_player_stats": (
        "/Widget/TeamStats/{teamId}",
        "/Widget/TeamStats/{team_id}",
        IB.parse_vtb_infobasket_team_player_stats,
        _TEAMSEASON,
        "Season totals of every player of a club in one season.",
        {
            "path_params": _TEAM_P,
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True), _lang(), _tag()],
            "fixed_params": FMT,
            "example_args": {"team_id": 2650, "season": 2026},
        },
    ),
    "team_coach_stats": (
        "/Widget/TeamStats/{teamId}",
        "/Widget/TeamStats/{team_id}",
        IB.parse_vtb_infobasket_team_coach_stats,
        _TEAMSEASON,
        "Technical fouls and games of every coach of a club in one season.",
        {
            "path_params": _TEAM_P,
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True), _lang(), _tag()],
            "fixed_params": FMT,
            "example_args": {"team_id": 2650, "season": 2026},
        },
    ),
}

# --- column descriptions -----------------------------------------------------------------------------------------------

_TS = "Stats_"
SECTIONS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "seasons": ("/Comp/GetSeasonsForTag", ("",)),
    "calendar": ("/Comp/GetCalendar/", ("",)),
    "team_stats": ("/Comp/GetTeamStats/{compId}", ("stats_",)),
    "team_stats_players": ("/Comp/GetTeamStats/{compId}", ("stats_players_", "stats_players_person_info_")),
    "team_stats_coaches": ("/Comp/GetTeamStats/{compId}", ("stats_coaches_", "stats_coaches_person_info_")),
    "best_players": ("/Comp/GetBestPlayers/{compId}", ("results_", "parameter_", "results_person_", "")),
    "best_teams": ("/Comp/GetBestTeams/{compId}", ("results_", "parameter_", "")),
    "team_preview": ("/Comp/GetTeamStatsForPreview/{gameId}", ("",)),
    "game": ("/Widget/GetOnline/{gameId}", ("online_", "")),
    "box_score": ("/Widget/GetOnline/{gameId}", ("game_teams_players_", "game_teams_")),
    "team_box_score": ("/Widget/GetOnline/{gameId}", ("game_teams_", "game_teams_coach_")),
    "pbp": ("/Widget/GetOnline/{gameId}", ("online_plays_",)),
    "game_staff": ("/Widget/GetOnline/{gameId}", ("online_starts_",)),
    "game_page": ("/Widget/GamePage/{gameId}", ("",)),
    "comp_issue": ("/Widget/CompIssue/{compId}", ("comps_", "parent_comp_")),
    "standings": ("/Widget/CompTeamResults/{compId}", ("standings_", "")),
    "cross_table": ("/Widget/CrossTable/{compId}", ("",)),
    "playoff": ("/Widget/PlayOff/{compId}", ("",)),
    "playoff_games": ("/Widget/PlayOff/{compId}", ("scores_", "")),
    "best_player_parameters": ("/Widget/GetBestPlayerParameters/", ("",)),
    "best_team_parameters": ("/Widget/GetBestTeamParameters/", ("",)),
    "player": ("/Widget/PlayerPage/{personId}", ("players_position_", "players_", "")),
    "coach": ("/Widget/CoachPage/{personId}", ("",)),
    "player_season_stats": ("/Widget/PlayerSeasonStats/{personId}", ("season_stats_", "season_stats_season_")),
    "player_career_stats": ("/Widget/PlayerSeasonStats/{personId}", ("",)),
    "player_game_log": ("/Widget/PlayerStats/{personId}", ("game_stats_", "game_stats_game_")),
    "team": ("/Widget/TeamPage/{teamId}", ("",)),
    "team_seasons": ("/Widget/GetTeamSeasons/{teamId}", ("",)),
    "team_comps": ("/Widget/GetTeamComps/{teamId}", ("",)),
    "roster": ("/Widget/TeamRoster/{teamId}", ("players_", "players_person_info_")),
    "roster_coaches": ("/Widget/TeamRoster/{teamId}", ("coaches_", "coaches_person_info_")),
    "team_games": ("/Widget/TeamGames/{teamId}", ("",)),
    "team_season_stats": ("/Widget/TeamStats/{teamId}", ("",)),
    "team_player_stats": ("/Widget/TeamStats/{teamId}", ("players_", "players_person_info_")),
    "team_coach_stats": ("/Widget/TeamStats/{teamId}", ("coaches_", "coaches_person_info_")),
}
#: Our column -> the wire column (underscored) when the parser renamed it.
INVERSE = {
    "parent_comp_id": "comp_i_dparent", "team1_id": "team1id", "team2_id": "team2id", "team_a_id": "team_aid",
    "team_b_id": "team_bid", "is_captain": "capitan", "birth_date": "person_birth",
    "nationality_code": "nationality_id", "country_code": "country_id",
}  # fmt: skip
_CTX_SEASON = "Season END year (2026 = 2025-26): the season the request asked for, stamped by the runtime."
_CTX_SEASON_COMP = (
    "Season-level Infobasket CompID of the season (50714 = 2025-26): the id every season-keyed route takes."
)
_CTX_GAME = "Infobasket GameID of the game (the v2 matches' match_id); global across seasons."
_CTX_TEAM = "Infobasket TeamID (2650 = Lokomotiv Kuban); the v2 teamId. Not a club-record id and not a registration id."
_CTX_PERSON = (
    "Infobasket PersonID: one person across clubs and seasons; players, coaches and officials share the space."
)
_CTX_STAGE = "Stage CompID the request asked for (50720 = the 2025-26 regular season): a stage below the season, not an END year."
_TEAM_NAME_STUB = {
    "short_name_ru": "short name in Russian",
    "short_name_en": "short name in English",
    "name_ru": "name in Russian",
    "name_en": "name in English",
    "region_name_ru": "region or city in Russian",
    "region_name_en": "region or city in English",
}
_SIDE_LABEL = {
    "team_": "the team", "team_a_": "team A (the home side in the calendar)", "team_b_": "team B (the away side)",
    "team1_": "team 1 of the series", "team2_": "team 2 of the series",
}  # fmt: skip

#: Descriptions of parser-built columns, by column name (apply to every wrapper that has the column).
LOCAL: Dict[str, str] = {
    "season": _CTX_SEASON,
    "season_comp_id": _CTX_SEASON_COMP,
    "game_id": _CTX_GAME,
    "team_id": _CTX_TEAM,
    "person_id": _CTX_PERSON,
    "tag": "Competition tag of the request (vtb is the VTB United League; vtbyouth, wbc, wpremier and rfb also answer).",
    "is_home": "True for team 1, the team listed first (team A, the home side in the calendar); the body has no explicit home flag (unverified).",
    "team_number": "Side of the game: 1 for the team listed first (team A), 2 for the other; 0 for neutral officials.",
    "birth_date": "Birth date (ISO) of a player: public bio, kept; coaches' and officials' birth data is never output.",
    "is_captain": "True when the player is the team captain in this game or roster.",
    "person_last_name_ru": "Person's last name in Russian.",
    "person_last_name_en": "Person's last name in English.",
    "person_first_name_ru": "Person's first name in Russian.",
    "person_first_name_en": "Person's first name in English.",
    "person_full_name_ru": "Person's full name in Russian, as 'Last First'.",
    "person_full_name_en": "Person's full name in English, as 'Last First'.",
    "person_gender": "Person gender code from the host: 1 on every sampled (male) person.",
    "player_position": "Player position code from the host (1 point guard, 2 shooting guard, 3 small forward ... 5 center; see position_id for the named position).",
    "position_id": "Position id of the player (PosID), with its names in position_name_ru / position_name_en.",
    "position_name_ru": "Position name in Russian.",
    "position_name_en": "Position name in English.",
    "player_hand": "Player's hand code from the host; null in every sample.",
    "is_naturalized": "True for a naturalised player.",
    "height": "Height in centimetres.",
    "weight": "Weight in kilograms.",
    "comp_id": "Competition id: a season-level id (50714 = 2025-26) or a stage id below it (50720), depending on the route; see the route's summary.",
    "param_id": "Leader category id (ParamID).",
    "param_name": "Leader category name (ParamName), e.g. 'Points'.",
    "param_short_name": "Leader category short label, e.g. 'Points in a game'.",
    "param_label": "Leader category label in the requested language (lang).",
    "param_group": "Leader category group code from the host; 0 in every sample.",
    "play_type": "English label of play_type_id from the site widget's table (null for a code the table lacks).",
    "play_period": "Period of the play (1-4; 5 and up are overtimes).",
    "play_second": "Time ELAPSED in the period, in tenths of a second (6000 = 600 s = the end of a 10-minute quarter).",
    "team_box_foul": "Personal fouls committed.",
    "coach_person_id": "Infobasket PersonID of the head coach of the team in this game.",
    "coach_name_en": "Head coach's name in English, as 'Last First'.",
    "coach_name_ru": "Head coach's name in Russian, as 'Last First'.",
    "coach_foul_c": "Technical fouls on the head coach for his own conduct (scoresheet 'C').",
    "coach_foul_b": "Technical fouls on the head coach for bench conduct (scoresheet 'B').",
    "coach_foul_d": "Disqualifying fouls on the head coach (scoresheet 'D').",
    "coach_foul": "Personal fouls on the head coach.",
    "role": "English role of the person: head coach, assistant coach, referee, commissioner, table official (from start_type and pos_id).",
    "level": "Depth in the stage tree: 1 = a stage of the season, 2 = a series or group below it.",
    "parent_comp_id": "CompID of the parent competition (the season for a stage, the stage for a series).",
    "score1": "Points of team 1, parsed from the 'a:b' score string.",
    "score2": "Points of team 2, parsed from the 'a:b' score string.",
    "comp_team_id": "The team's registration in this stage; not a team id.",
    "team_record_id": "Id of the club-record name (CurrentTeamName.TeamID, 2259 for team 2650): a different key space from team_id.",
    "season_name": "Season name in Russian ('Сезон 2026/2027') as the host sends it.",
    "season_name_ru": "Season name in Russian ('Сезон 2025/2026').",
    "season_name_en": "Season name in English ('Season 2025/2026').",
    "name_ru": "Name in Russian.",
    "name_en": "Name in English.",
    "db": "Name of the Infobasket database the competition lives in ('org').",
    "league_id": "v2 league id (1 vtb, 2 vtbyouth, 3 wbc); not an Infobasket id.",
    "match_id": "Infobasket GameID of the match: the same key as game_id in vtb_infobasket.",
    "phase_id": "v2 stage id of the match inside the league-season; not an Infobasket CompID.",
    "hosting_team_id": "Infobasket TeamID of the host (home) team.",
    "venue_id": "Infobasket ArenaId of the venue.",
    "home_ot": "Points of the home team in all overtimes together (null without overtime).",
    "away_ot": "Points of the away team in all overtimes together (null without overtime).",
    "points_by_quarters": "Points per period as a JSON list of [home, away] pairs (overtimes included), null when not recorded.",
    "league": "League slug (vtb, vtbyouth, wbc).",
    "region_ru": "City or region of the game's venue in Russian.",
    "region_en": "City or region of the game's venue in English.",
    "game_attendance": "Spectators at the game (null when not recorded).",
    "attendance": "Spectators at the game (null when not recorded).",
    "player_number": "Shirt number as an integer (0 when the player has none or wears 00).",
    "display_number": "Shirt number as displayed ('0', '7'); a coach's is the coach order (1 = head coach).",
    "start_count": "Games the person started.",
    "team_a_id": "Infobasket TeamID of team A, the team listed first (the home side in the calendar).",
    "team_b_id": "Infobasket TeamID of team B, the team listed second (the away side).",
    "score_a": "Final points of team A (listed first).",
    "score_b": "Final points of team B (listed second).",
    "region_name_ru": "City or region in Russian.",
    "region_name_en": "City or region in English.",
    "arena_name_ru": "Arena name in Russian.",
    "arena_name_en": "Arena name in English.",
    "arena_full_name_ru": "Arena full name in Russian.",
    "arena_full_name_en": "Arena full name in English.",
    "games": "Games played in the stage (top-level copy of standing_game).",
    "won": "Games won in the stage (top-level copy of standing_win).",
    "lost": "Games lost in the stage (top-level copy of standing_lose).",
    "team1_id": "Infobasket TeamID of team 1.",
    "team2_id": "Infobasket TeamID of team 2.",
    "person_height": "Height in centimetres.",
    "person_weight": "Weight in kilograms.",
}
for _i in range(1, 5):
    LOCAL[f"home_q{_i}"] = f"Points of the home team in period {_i}."
    LOCAL[f"away_q{_i}"] = f"Points of the away team in period {_i}."
for _side in ("home", "away"):
    LOCAL[f"{_side}_team_id"] = f"Infobasket TeamID of the {_side} team."
    LOCAL[f"{_side}_score"] = f"Final points of the {_side} team (an integer; 0 for a cancelled match)."
    for _k, _v in (("name", "name"), ("code", "short code")):
        for _lang_ in ("en", "ru"):
            LOCAL[f"{_side}_team_{_k}_{_lang_}"] = (
                f"The {_side} team's {_v} in {'English' if _lang_ == 'en' else 'Russian'}."
            )
    for _lang_ in ("en", "ru"):
        LOCAL[f"{_side}_region_name_{_lang_}"] = (
            f"Region or city of the {_side} team in {'English' if _lang_ == 'en' else 'Russian'}."
        )
for _k, _v in (
    ("phase_name", "stage name"),
    ("venue_name", "venue name"),
    ("venue_region_name", "venue city"),
    ("subtitle", "site subtitle of the match"),
    ("necessary_label", "site label of the match"),
    ("league_name", "league name"),
    ("league_abbrev", "league abbreviation"),
):
    for _lang_ in ("en", "ru"):
        LOCAL[f"{_k}_{_lang_}"] = f"The {_v} in {'English' if _lang_ == 'en' else 'Russian'}."

#: Route-specific overrides (wrapper short -> column -> text).
ROUTE: Dict[str, Dict[str, str]] = {
    "player_season_stats": {
        "season": "Season END year (2026 = 2025-26). The wire sends it as Season.CompID, a third meaning of CompID.",
    },
    "player_career_stats": {
        "has_more_seasons": "True when the host holds more seasons than listed (False in every sample)."
    },
    "calendar": {
        "season": _CTX_SEASON,
        "comp_id": _CTX_SEASON_COMP,
        "game_status": "Game status in the calendar code set: 0 planned, 1 completed, 2 live.",
    },
    "team_games": {"comp_id": _CTX_SEASON_COMP},
    "game": {
        "comp_id": "Stage CompID of the game (50720 = the 2025-26 regular season): a stage below the season.",
        "game_id": _CTX_GAME,
        "season_comp_id": "Season-level CompID of the game (the body calls it LeagueID).",
        "gender": "Competition gender code from the host (1 male, 0 unspecified).",
        "is_online": "True while the game is live; False for a completed game.",
        "game_date_time": "Tip-off as an ISO 8601 string in Moscow time (+03:00), from Online.GameTime.",
        "venue_en": "Venue, 'City, Arena', in English.",
        "venue_ru": "Venue, 'City, Arena', in Russian.",
        "period_minutes": "Period lengths in minutes, comma separated ('10,10,10,10').",
        "intervals_minutes": "Break lengths in minutes after each period, comma separated ('2,15,2,2').",
        "team_fouls": "Team-foul setting per period, comma separated (4 or 5 seen; exact meaning unverified).",
        "left_team_number": "Team number drawn on the left in the widget (1 in every sample).",
        "is3x3": "True for a 3x3 competition (False in every sample).",
        "game_time": "Tip-off time of day as 'HH.MM' (a dot, local time).",
        "game_time_msk": "Tip-off time of day as 'HH.MM' in Moscow time.",
        "game_date": "Game date (ISO), local.",
        "score_a": "Final points of team A (listed first).",
        "score_b": "Final points of team B.",
    },
    "game_page": {
        "comp_id": "Always 0 on this thin header; use vtb_infobasket_game for the stage.",
        "game_date": "Game date as an ISO 8601 string in Moscow time (+03:00), midnight.",
        "game_time": "Tip-off as an ISO 8601 string in Moscow time (+03:00).",
        "game_date_time": "Tip-off as an ISO 8601 string in Moscow time (+03:00).",
    },
    "box_score": {"game_id": _CTX_GAME, "team_id": _CTX_TEAM},
    "team_box_score": {
        "game_id": _CTX_GAME,
        "score": "Final points of the team.",
        "foul": "Personal fouls committed by the team.",
    },
    "pbp": {
        "game_id": _CTX_GAME,
        "play_id": "Play id within the game (the row-0 header play included); parent_play_id points at another play_id.",
        "parent_play_id": "play_id of the parent play (a made shot's children: shot type, assist, possession change).",
        "play_type_id": "Play type code (see play_type; the full table is in vtb-returns.md).",
        "play_sort_order": "Order among plays sharing the same play_second.",
        "play_flag": "Play flag; 0 in every sample (meaning unverified).",
        "start_id": "Line-up slot id within this game's line-up (0 = the placeholder row); not a person id.",
        "team_number": "Side (1 or 2) of the line-up slot the play belongs to; null for a play with no player.",
        "team_id": "Infobasket TeamID of the team the play belongs to; null for a play with no player.",
        "person_id": "Infobasket PersonID of the player of the line-up slot; null for a play with no player.",
        "play_zone": "Court zone code of a shot: 1-3 count as paint in the site's widget, 3-pointers sit in zones 7-12; the full map is unverified.",
        "play_x": "Shot x in decimetres across the court, -75..75, origin at the basket centre (orientation from the renderer, unverified).",
        "play_y": "Shot y in decimetres from the basket toward midcourt (about -16 baseline, 124 midcourt).",
        "video_from": "Video clip start, Unix epoch seconds; 0 when none.",
        "video_to": "Video clip end, Unix epoch seconds; 0 when none.",
    },
    "game_staff": {
        "game_id": _CTX_GAME,
        "start_id": "Line-up slot id within this game; not a person id.",
        "start_type": "Line-up row type: 3 coach, 4 official.",
        "pos_id": "Position code of the coach or official (coach 1000 head / 1001 assistant; referees 101-103, commissioner 104, table officials 111-123).",
        "person_name_en": "Name in English, as 'Last First'.",
        "person_name_ru": "Name in Russian, as 'Last First'.",
        "country_code_ioc": "Country code as the host sends it (ISO alpha-2 in most rows, IOC alpha-3 in some).",
    },
    "comp_issue": {
        "season": _CTX_SEASON,
        "season_comp_id": _CTX_SEASON_COMP,
        "comp_id": "Stage CompID (the id vtb_infobasket_standings, _cross_table and _playoff take).",
    },
    "standings": {
        "comp_id": _CTX_STAGE,
        "standing_game": "Games played in the stage.",
        "standing_win": "Games won in the stage.",
        "standing_lose": "Games lost in the stage.",
        "standing_draw": "Draws; null in basketball.",
        "standing_forfeit": "Games lost by forfeit in the stage.",
        "standing_points": "League points (2 per win, 1 per loss).",
        "standing_goal_plus": "Points scored in the stage.",
        "standing_goal_minus": "Points conceded in the stage.",
        "standing_goal_difference": "Point difference (scored minus conceded).",
        "standing_penalty": "Points deducted.",
        "standing_is_play": "True while the team is still playing in the stage.",
        "victory_percent": "Win percentage as a number (83 = 83%).",
        "avg_goal_plus": "Average points scored per game.",
        "avg_goal_minus": "Average points conceded per game.",
    },
    "cross_table": {
        "comp_id": _CTX_STAGE,
        "game_date": "Day and month of the game as the host sends them ('04.04.'): there is no year in this route.",
    },
    "playoff": {"comp_id": _CTX_STAGE},
    "playoff_games": {"comp_id": _CTX_STAGE},
    "player": {"person_id": _CTX_PERSON},
    "team_seasons": {
        "season": "Season END year of the club's season entry (2027 = 2026/2027).",
        "comp_id": "Competition id of the season in the club's rfb tag (56293 for 2027), not the VTB tag's season CompID.",
    },
    "team_comps": {"comp_id": "Competition id (the season or a stage of it).", "season": _CTX_SEASON},
    "player_game_log": {
        "team_id": "Infobasket TeamID the player played for in that game.",
        "team_number": "Side (1 or 2) the player's team had in the game.",
        "game_status": "Game status in the game-record code set: 21 completed.",
    },
    "team_stats": {"season": _CTX_SEASON},
}


def _team_name_desc(col: str) -> str:
    """Description of a ``<prefix>(short_name|name|region_name)_(ru|en)`` club-name column the parsers build."""
    for prefix, label in _SIDE_LABEL.items():
        for stub, text in _TEAM_NAME_STUB.items():
            if col == f"{prefix}{stub}":
                return f"The {label}'s {text} (as the competition names it)."
    return ""


def _md_sections(path: Path) -> Dict[str, Dict[str, str]]:
    """``{route: {underscored wire path: description}}`` from the returns md."""
    out: Dict[str, Dict[str, str]] = {}
    current: Optional[str] = None
    head = re.compile(r"^#{2,3} `([^`]+)`")
    row = re.compile(r"^\| `([^`]+)` \| [^|]+ \| (.*) \|$")
    for line in path.read_text(encoding="utf-8").splitlines():
        h = head.match(line)
        if h:
            current = h.group(1)
            out.setdefault(current, {})
            continue
        m = row.match(line)
        if m and current is not None:
            segs = [underscore(s) for s in m.group(1).split(".") if s not in ("[]", "") and not s.startswith("<")]
            if segs:
                out[current].setdefault("_".join(segs), m.group(2).strip())
    return out


_DECIMAL_NOTE = re.compile(
    r"(?:\s+as)?[;,.]?\s*(?:a string with a comma decimal separator.*|comma decimals?.*)$", re.IGNORECASE
)
_NULL_ZERO = re.compile(r"\s*Null means zero: Infobasket sends null instead of 0 for counting stats\.?")
#: Columns whose md text describes the wire form the parser converts; these replace it.
FORCE: Dict[str, str] = {
    "birth_date": "Birth date (ISO 8601 date) of a player: public bio, kept; coaches' and officials' birth data is never output.",
    "game_date_time": "Tip-off instant as an ISO 8601 string in Moscow time with its +03:00 offset (the wire sends a .NET /Date(ms)/ value).",
    "game_date_time_moscow": "Tip-off instant as an ISO 8601 string in Moscow time with its +03:00 offset (the wire's second .NET date; equal to game_date_time in every sample).",
    "game_date": "Game date as an ISO 8601 date (YYYY-MM-DD); the wire sends DD.MM.YYYY.",
}


def _polish(desc: str, name: str) -> str:
    """The md describes the wire form; the parsers convert it. Rewrite what the conversion changed."""
    if name.endswith("_percent") or (name.startswith("avg_") and name != "avg_played_time"):
        text = _DECIMAL_NOTE.sub("", desc).rstrip(" ;,.")
        return f"{text}; parsed to a number (the wire sends a comma-decimal string, null when there were no attempts)."
    if _NULL_ZERO.search(desc):
        if name in F.COUNTS:
            return _NULL_ZERO.sub("", desc).rstrip(" .") + ". The wire's null is 0 here (null counts are filled)."
        return _NULL_ZERO.sub("", desc).rstrip(" .") + ". Null where the host sends none."
    return desc


def _filler(name: str, desc: str) -> bool:
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


def _describe(family: str, short: str, name: str, md: Dict[str, Dict[str, str]]) -> str:
    """ROUTE, then the route's md row, then LOCAL / the club-name rule."""
    routes = ROUTE.get(short, {}) if family == "vtb_infobasket" else {}
    if name in routes:
        return routes[name]
    if name in FORCE and family == "vtb_infobasket":
        return FORCE[name]
    sections = SECTIONS if family == "vtb_infobasket" else LEAGUE_SECTIONS
    section, roots = sections[short]
    rows = md.get(section, {})
    keys = [name] + ([INVERSE[name]] if name in INVERSE else [])
    for key in keys:
        for root in roots:
            for cand in (root + key, root + "person_info_" + key):
                hit = rows.get(cand)
                if hit and not _filler(name, hit):
                    return _polish(hit, name)
    return LOCAL.get(name) or _team_name_desc(name)


# --- fixtures, dtypes ---------------------------------------------------------------------------------------------------

_CODE = {pl.Int64: "i", pl.Float64: "f", pl.Boolean: "b", pl.String: "s"}
_TYPE = {"i": "integer", "f": "numeric", "b": "logical", "s": "character"}
FLOAT_COLS = frozenset({"res"})


def _is_float_name(col: str) -> bool:
    return col in FLOAT_COLS or col.endswith("_percent") or (col.startswith("avg_") and col != "avg_played_time")


def load_fixture(family: str, rel: str, ctx: Dict[str, Any]) -> Any:
    """A fixture body as the runtime hands it to a parser: the body plus the ``_request`` stamp."""
    path = ROOT / "tests" / "fixtures" / family / rel
    text = gzip.open(path, "rt", encoding="utf-8").read() if rel.endswith(".gz") else path.read_text(encoding="utf-8")
    body = json.loads(text)
    body = {"items": body} if isinstance(body, list) else dict(body)
    body[F.REQUEST_KEY] = ctx
    return body


def _frames(family: str, entry: Entry) -> List[pl.DataFrame]:
    _spec, _path, parser, fixtures, _summary, _fields = entry
    return [parser(load_fixture(family, rel, ctx)) for rel, ctx in fixtures]


def _merge_codes(family: str, short: str, frames: List[pl.DataFrame]) -> Dict[str, str]:
    """``{column: dtype code}`` over every fixture of a wrapper, in first-seen order."""
    seen: Dict[str, set] = {}
    order: List[str] = []
    for df in frames:
        for col, dtype in df.schema.items():
            if col not in order:
                order.append(col)
            if df.height and df[col].null_count() < df.height:
                if dtype not in _CODE:
                    raise SystemExit(f"{family}: {short}.{col} has dtype {dtype}")
                seen.setdefault(col, set()).add(_CODE[dtype])
    out: Dict[str, str] = {}
    for col in order:
        codes = seen.get(col)
        if _is_float_name(col) and codes in (None, {"i"}, {"f"}, {"i", "f"}):
            out[col] = "f"
        elif not codes:
            out[col] = "i" if re.search(r"(^|_)id$", col) else "s"
        elif len(codes) == 1:
            out[col] = next(iter(codes))
        elif codes == {"i", "f"}:
            out[col] = "f"
        else:
            raise SystemExit(f"{family}: {short}.{col} has dtypes {sorted(codes)} across fixtures")
    return out


def _write_dtypes(by_parser: Dict[str, Dict[str, str]]) -> None:
    lines = [
        '"""Column dtypes of the VTB parsers, one map per parser function.',
        "",
        "Generated by ``tools/codegen/gen_vtb.py`` from the returns-schemas",
        "(``tools/codegen/schemas/native/vtb_league`` and ``.../vtb_infobasket``); do not edit. Codes: ``i`` Int64,",
        "``f`` Float64, ``b`` Boolean, ``s`` String. ``vtb_frames.finish`` gives these dtypes to an empty frame,",
        "a gap-filled column and an all-null column, so an empty frame stacks onto a populated one.",
        '"""',
        "",
        "from typing import Dict",
        "",
        "DTYPES: Dict[str, Dict[str, str]] = {",
    ]
    for parser in sorted(by_parser):
        lines.append(f'    "{parser}": {{')
        lines += [f'        "{name}": "{code}",' for name, code in by_parser[parser].items()]
        lines.append("    },")
    lines.append("}")
    DTYPES_MODULE.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    _spec_path, path, parser, _fixtures, summary, fields = entry
    fields = copy.deepcopy(fields)
    tokens = re.findall(r"\{([^}]+)\}", path)
    assert tokens == [p["name"] for p in fields.get("path_params", [])], short
    assert not {"league", "sport"} & set(tokens), short  # the renderer substitutes those as ESPN slugs
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/{{family}}/{short}",
        **fields,
    }


# --- family docs --------------------------------------------------------------------------------------------------------

OWNER_FLAG = (
    "Owner flag (sdv-internal-refs/vtb/README.md): the VTB United League is run from Russia and several clubs are owned or "
    "sponsored by sanctioned state companies (the KHL precedent); Infobasket is the Russian basketball federation's stats "
    "platform. Fetching on your own machine is fine; any published data release, hosted mirror or redistribution needs "
    "owner (sanctions / legal) sign-off first. No accounts or payments are involved."
)
COMMON_NOTES = [
    OWNER_FLAG,
    "Unofficial and unsupported: both hosts are keyless public JSON backends of vtb-league.com. No terms of use are "
    "published; robots.txt is a 404 on api.vtb-league.com and org.infobasket.su (no rules; vtb-league.com disallows only "
    "/wp/wp-admin/). The runtime waits at least 1 s before every request, retries included, and honours Retry-After on "
    "429 / 503. No key, cookie or proxy is used (live-verified direct from the droplet 2026-10-12).",
    "Seasons are END years (2026 = 2025-26) on both backends. Infobasket keys everything by competition id: "
    "vtb_infobasket_seasons maps the END year to the season-level CompID, and the season-keyed wrappers do that mapping "
    "for you. CompID means three things: a season (50714), a stage below it (50720 regular season, 54281 playoff; "
    "stage-level wrappers take the stage id from vtb_infobasket_comp_issue) and, in the player season stats, an END year.",
    "Ids: the v2 matchId is the Infobasket GameID, the v2 teamId the Infobasket TeamID and the v2 venueId the Infobasket "
    "ArenaId. team_id is the key on every team route; comp_team_id (a registration in one stage) and the club-record id "
    "of the team page are other key spaces. person_id is one person across clubs and seasons.",
]
LEAGUE_NOTES = [
    *COMMON_NOTES,
    "v2 pages: meta.count is the rows returned (not a total), page is ignored and the default limit is 20, so the matches "
    "wrapper asks for 1000 and refuses a body whose count reaches its limit. The API lists new seasons only (from "
    "2024-25; vtb: 2025-2027, wbc: 2026-2027): earlier years answer no rows (NoDataError).",
]
INFOBASKET_NOTES = [
    *COMMON_NOTES,
    "Errors are HTTP 200: an unknown id answers {MessageType, Message}, an unknown game the string 'BadRequest'; both are "
    "AssetFetchError (a failed fetch), never an empty frame. A body for another game, person, team, competition or "
    "season than the one asked for is AssetFetchError too, as is a game that is not final (box score, plays and header "
    "need GameStatus 1) and a partial GetTeamStats page. An empty list is NoDataError.",
    "Coverage: calendars and box scores from 2009-10; play-by-play and shot x/y from 2022-23 only (earlier games give an "
    "empty pbp frame), officials in the line-ups from 2022-23. Shot x/y are decimetres from the basket centre "
    "(orientation unverified). Counting statistics are null instead of 0 on the wire (the parsers fill 0), percentages "
    "and averages are comma-decimal strings (the parsers return floats), the paint statistics are broken and dropped, "
    "and dates come in three encodings (the parsers return ISO strings; .NET instants as Moscow time with its +03:00 "
    "offset).",
    "Personal data: the runtime removes licence numbers, contact and operator-account fields for everyone and the birth "
    "dates, birth years and ages of coaches, referees and other officials from every body (also with "
    "return_parsed=False). Players' public bio (birth date, height, weight, nationality) is kept.",
    "Stage CompIDs: the team routes (TeamRoster, TeamStats, TeamGames) also accept a stage CompID; pass it as "
    "params={'compId': 50720} instead of season. Other tags (vtbyouth, wbc, wpremier, rfb) answer through the tag "
    "argument; only vtb was captured.",
]
RAISES = [
    "sportsdataverse.errors.NoDataError: the host has nothing for the request (an empty list, a league the API does not "
    "know, a season with no rows).",
    "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is unknown or partial: an error envelope or "
    "string (HTTP 200), a game that is not final, a partial or possibly truncated page, or a body for another game, "
    "person, team, competition, league or season than the one asked for.",
    "ValueError: a season the competition tag does not have, or an argument that is not an integer id.",
]
SEE_ALSO = [
    {
        "name": "VTB United League",
        "url": "https://www.vtb-league.com/",
        "note": "the league site whose backends these wrappers read",
    },
    {
        "name": "hoopR",
        "url": "https://hoopR.sportsdataverse.org",
        "note": "SportsDataverse men's basketball package (R)",
    },
]


def _family_doc(family: str, host: str, module: str, notes: List[str], endpoints: Dict[str, Entry]) -> Dict[str, Any]:
    eps = [_endpoint(short, entry) for short, entry in endpoints.items()]
    for ep in eps:
        ep["returns_schema"] = ep["returns_schema"].replace("{family}", family)
    return {
        "api": family,
        "host": host,
        "name_pattern": family + "_{short}",
        "module": module,
        "parser_module": f"basketball.vtb.{family}_parsers",
        "getter_module": f"sportsdataverse.basketball.vtb.{family}_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": f"{family}_",
        "runtime_imports": ["_get"],
        "docstring": {"example_import": True, "notes": notes, "raises": RAISES, "see_also": SEE_ALSO},
        "endpoints": eps,
    }


def _norm(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "{}", path)


def _build(
    family: str,
    entries: Dict[str, Entry],
    sections: Dict[str, Tuple[str, Tuple[str, ...]]],
    spec: dict,
    md: Dict[str, Dict[str, str]],
    host: str,
    notes: List[str],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    """Write one family's YAML + schemas; return (snippet rows, dtype maps by parser)."""
    spec_paths = set(spec["paths"])
    used = {e[0] for e in entries.values()}
    if used != spec_paths:
        raise SystemExit(f"{family}: spec paths and entries differ: {sorted(used ^ spec_paths)}")
    for short, entry in entries.items():
        for rel, _ctx in entry[3]:
            assert (ROOT / "tests" / "fixtures" / family / rel).exists(), f"{short}: missing fixture {rel}"
    if set(sections) != set(entries):
        raise SystemExit(f"{family}: SECTIONS out of step with the entries")
    write_yaml(ROOT / f"tools/codegen/endpoints/{family}.yaml", _family_doc(family, host, family, notes, entries))
    schema_dir = ROOT / f"tools/codegen/schemas/native/{family}"
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    dtypes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in entries.items():
        codes = _merge_codes(family, short, _frames(family, entry))
        dtypes[entry[2].__name__] = codes
        key = f"{family}_{short}"
        snippet[key] = {}
        cols = []
        for name, code in codes.items():
            desc = _describe(family, short, name, md)
            if _filler(name, desc):
                bad.append(f"{key}.{name}: {desc!r}")
            snippet[key][name] = desc
            cols.append({"name": name, "type": _TYPE[code], "description": ""})  # descriptions live in the snippet
        write_yaml(schema_dir / f"{short}.yaml", {"schema": key, "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit(f"{family}: filler or missing descriptions:\n" + "\n".join(bad))
    snapshot = ROOT / "tests" / "fixtures" / family / "spec_paths.json"
    snapshot.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    return snippet, dtypes


def main() -> None:
    refs = refs_dir("vtb", "vtb-returns.md")
    md = _md_sections(refs / "vtb-returns.md")
    F.DTYPES = {}  # parse by the name rules alone, so the last run's vtb_dtypes.py cannot feed back
    snippet: Dict[str, Dict[str, str]] = {}
    dtypes: Dict[str, Dict[str, str]] = {}
    for family, entries, sections, spec_name, host, notes in (
        (
            "vtb_league",
            LEAGUE,
            LEAGUE_SECTIONS,
            "vtb-league-api.openapi.yaml",
            "https://api.vtb-league.com",
            LEAGUE_NOTES,
        ),
        (
            "vtb_infobasket",
            INFOBASKET,
            SECTIONS,
            "vtb-infobasket.openapi.yaml",
            "https://org.infobasket.su",
            INFOBASKET_NOTES,
        ),
    ):
        unknown = {sec for sec, _ in sections.values()} - set(md)
        if unknown:
            raise SystemExit(f"{family}: SECTIONS name routes the returns md lacks: {sorted(unknown)}")
        s, d = _build(family, entries, sections, load_spec(refs, spec_name), md, host, notes)
        snippet.update(s)
        dtypes.update(d)
    write_yaml(SNIPPET, snippet)
    _write_dtypes(dtypes)
    print(f"vtb: {len(LEAGUE)} + {len(INFOBASKET)} endpoints")


if __name__ == "__main__":
    main()
