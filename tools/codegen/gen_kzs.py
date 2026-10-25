"""Generate the ``kzs`` flat-API endpoint YAML + returns-schemas (KZS FMS public API, ``api.kzs.si/api/v1/public``).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_legabasket.py`` with the shared
plumbing in ``gen_soccer_common.py``. Reads the frozen spec and the returns md from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/kzs.yaml`` -- 23 wrappers over the spec's 18 paths. One route can feed several frames (the
  match header feeds ``kzs_match`` and ``kzs_period_scores``, the box score route three frames, the play-by-play route
  two, a competition two); :data:`ENDPOINTS` is the source of truth.
* ``tools/codegen/schemas/native/kzs/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser run
  over its committed fixtures (the FULL bodies in ``tests/fixtures/kzs/full/`` where there is one, plus the refs
  captures in ``tests/fixtures/kzs/captures/``). Schema key ``kzs_<short>`` (``schema_prefix``); every ``description``
  is ``''``.
* ``tools/codegen/notes/kzs_descriptions.yaml`` -- the column descriptions (committed: the generator owns the text),
  keyed ``kzs_<short>`` -> column, for ``manual_column_descriptions.yaml``. Each comes from :data:`ROUTE`
  (route-specific), :data:`LOCAL` (parser-built columns), else the route's own ``kzs-returns.md`` row
  (:data:`SECTIONS`). A filler or missing description stops the run.
* ``sportsdataverse/basketball/kzs/kzs_dtypes.py`` -- each parser's column dtypes (the returns-schema types), so an
  empty or all-null frame is typed like a populated one.
* ``tests/fixtures/kzs/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_kzs.py``
"""

from __future__ import annotations

import copy
import gzip
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, load_spec, refs_dir, rewrite_schema_dir, snake_path, write_yaml

from sportsdataverse.basketball.kzs import kzs_parsers as P
from sportsdataverse.dl_utils import underscore

HOST = "https://api.kzs.si/api/v1/public"
SPEC = "kzs.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "kzs"
SCHEMA_PREFIX = "kzs_"
SNIPPET = ROOT / "tools" / "codegen" / "notes" / "kzs_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
DTYPES_MODULE = ROOT / "sportsdataverse" / "basketball" / "kzs" / "kzs_dtypes.py"
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

SEASON_Q = "Season END year (2027 = 2026/2027); the wrapper maps it to KZS' opaque season id."
COMP_ID = "Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26)."
MATCH_ID = "Match id (match_id in kzs_matches), e.g. 101839. Global across seasons."
TEAM_ID = (
    "Team id (team_id in kzs_teams): one club team in one season, e.g. 17551. Not the match-team id nor the club id."
)
PLAYER_ID = "Player registry id (player_id in a box score or roster), e.g. 106285. Not the person id."

# A fixture is (path relative to FIXTURES, the request the runtime would stamp under ``_request``).
Fixture = Tuple[str, Dict[str, Any]]
# short -> (spec path, parser, fixtures, summary, endpoint fields)
Entry = Tuple[str, Callable[..., Any], List[Fixture], str, Dict[str, Any]]


def _q(name: str, wire: str, desc: str, *, typ: str = "str", required: bool = False, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": desc}
    if required:
        out["required"] = True
    out.update(extra)
    return out


def _p(name: str, desc: str) -> Dict[str, Any]:
    return {"name": name, "type": "int", "required": True, "description": desc}


def _caps(prefix: str, ctxs: Dict[str, Dict[str, Any]]) -> List[Fixture]:
    """The refs captures named ``<prefix>__<key>.json`` (those that exist), each with its request."""
    return [
        (f"captures/{prefix}__{key}.json", ctx)
        for key, ctx in ctxs.items()
        if (FIXTURES / "captures" / f"{prefix}__{key}.json").exists()
    ]


_LB = {"competition_id": 579, "stats_category": "PTS"}
_M = {"match_id": 101839}
_COMP_CTX = {
    "579": {"competition_id": 579},
    "30": {"competition_id": 30},
    "9": {"competition_id": 9},
    "1": {"competition_id": 1},
}
_GAMES = {
    "101839": {"match_id": 101839},
    "62586": {"match_id": 62586},
    "29150": {"match_id": 29150},
    "5787": {"match_id": 5787},
}
_BOX_GAMES = {k: v for k, v in _GAMES.items() if k != "5787"}  # 2007-08 has no box score (the capture refuses it)
_STAND = {"579": {"competition_id": 579}, "30": {"competition_id": 30}}

_R_SEASONS = "/seasons/"
_R_COMPS = "/competitions/"
_R_COMP = "/competitions/{competitionId}"
_R_ROUNDS = "/competitions/{competitionId}/rounds"
_R_PSTATS = "/competitions/{competitionId}/playerStats"
_R_TSTATS = "/competitions/{competitionId}/teamStats"
_R_MATCHES = "/matches/"
_R_MATCH = "/matches/{matchId}"
_R_PBP = "/matches/{matchId}/playByPlay"
_R_BOX = "/matches/{matchId}/stats"
_R_STAND = "/matches/standings"
_R_DAYS = "/matches/{competitionId}/playingDays"
_R_TEAMS = "/teams/"
_R_TEAM = "/teams/{teamId}"
_R_GENERAL = "/teams/{teamId}/general"
_R_ROSTER = "/teamPlayers/"
_R_PLAYER = "/players/{playerId}"
_R_PLSTATS = "/players/{playerId}/stats"

_STATS_CATEGORIES = [
    "PTS", "M_2", "A_2", "PERC_2", "M_3", "A_3", "PERC_3", "M_FG", "A_FG", "PERC_FG", "M_FT", "A_FT", "PERC_FT",
    "R", "R_D", "R_O", "ASS", "ST", "TOV", "PF_CM", "PF_RV", "ASS_TOV", "ST_TOV",
]  # fmt: skip


def _board_params() -> List[Dict[str, Any]]:
    return [
        _p("competition_id", COMP_ID),
        _q(
            "stats_category",
            "statsCategory",
            "Stat category (the site's statsCategory): PTS points, M_2/A_2/PERC_2 two-pointers, M_3/A_3/PERC_3 "
            "three-pointers, M_FG/A_FG/PERC_FG field goals, M_FT/A_FT/PERC_FT free throws, R rebounds (R_D defensive, "
            "R_O offensive), ASS assists, ST steals, TOV turnovers, PF_CM/PF_RV fouls committed/received, ASS_TOV and "
            "ST_TOV ratios. Competitions without live stats offer only PTS, M_2, M_3, M_FT, A_FT, PERC_FT.",
            default="PTS",
            choices=_STATS_CATEGORIES,
        ),
        _q(
            "order_type",
            "orderType",
            "Figure the leaderboard is built on: AVERAGE (per game), SUM (season total) or BEST_PERFORMANCE.",
            default="AVERAGE",
            choices=["AVERAGE", "SUM", "BEST_PERFORMANCE"],
        ),
        _q(
            "sort_by",
            "sortBy",
            "Row column the board is ordered by: avg (average), sum or max. The runtime checks the order.",
            default="avg",
            choices=["avg", "sum", "max"],
        ),
        _q("sort", "sort", "Sort direction.", default="DESC", choices=["ASC", "DESC"]),
    ]


def _one(route: str, key: str, ctx: Dict[str, Any]) -> List[Fixture]:
    prefix = route.strip("/").replace("/", "__").replace("{", "").replace("}", "")
    return [(f"captures/{prefix}__{key}.json", ctx)]


ENDPOINTS: Dict[str, Entry] = {
    "seasons": (
        _R_SEASONS,
        P.parse_kzs_seasons,
        [("full/seasons.json", {}), ("captures/seasons.json", {})],
        "Every season (39: 1991/1992 to 2029/2030) with its opaque id and the sdv END year.",
        {"fixed_params": {"limit": 1000}, "example_args": {}},
    ),
    "competitions": (
        _R_COMPS,
        P.parse_kzs_competitions,
        [("full/competitions__season26.json", {"season_id": 26, "season": 2026}), ("captures/competitions.json", {})],
        "Competitions of a season (leagues, cups, youth, 3x3, national teams), or of every season.",
        {
            "extra_params": [
                _q("season", "season", SEASON_Q + " Omitted = every season (627 competitions).", typ="int")
            ],
            "fixed_params": {"limit": 10000},
            "example_args": {"season": 2026},
        },
    ),
    "competition": (
        _R_COMP,
        P.parse_kzs_competition,
        _caps(
            "competitions__competitionId",
            {
                "579": {"competition_id": 579},
                "30": {"competition_id": 30},
                "9": {"competition_id": 9},
                "1": {"competition_id": 1},
            },
        ),
        "One competition: season, gender, category, rank and type (its phases are kzs_phases).",
        {"path_params": [_p("competition_id", COMP_ID)], "example_args": {"competition_id": 579}},
    ),
    "phases": (
        _R_COMP,
        P.parse_kzs_phases,
        _caps(
            "competitions__competitionId",
            {
                "579": {"competition_id": 579},
                "30": {"competition_id": 30},
                "9": {"competition_id": 9},
                "1": {"competition_id": 1},
            },
        ),
        "The phases of a competition (regular season, play-offs, finals) with dates and type.",
        {"path_params": [_p("competition_id", COMP_ID)], "example_args": {"competition_id": 579}},
    ),
    "rounds": (
        _R_ROUNDS,
        P.parse_kzs_rounds,
        _one(_R_ROUNDS, "579", {"competition_id": 579}),
        "Number of rounds of each phase of a competition.",
        {"path_params": [_p("competition_id", COMP_ID)], "example_args": {"competition_id": 579}},
    ),
    "player_leaderboard": (
        _R_PSTATS,
        P.parse_kzs_player_leaderboard,
        [("full/playerStats__579.json", _LB), *_one(_R_PSTATS, "579", _LB)],
        "Player leaderboard of a competition for one stat category (total, per-game average and single-game best).",
        {
            "path_params": [],
            "extra_params": _board_params(),
            "fixed_params": {"limit": 1000},
            "example_args": {"competition_id": 579},
        },
    ),
    "team_leaderboard": (
        _R_TSTATS,
        P.parse_kzs_team_leaderboard,
        [("full/teamStats__579.json", _LB), *_one(_R_TSTATS, "579", _LB)],
        "Team leaderboard of a competition for one stat category (total, per-game average and single-game best).",
        {
            "path_params": [],
            "extra_params": _board_params(),
            "fixed_params": {"limit": 1000},
            "example_args": {"competition_id": 579},
        },
    ),
    "matches": (
        _R_MATCHES,
        P.parse_kzs_matches,
        [
            ("full/matches__579_2025-10.json", {"competition_id": 579}),
            *_caps(
                "matches",
                {
                    "579_2025-10": {"competition_id": 579},
                    "30": {"competition_id": 30},
                    "9": {"competition_id": 9},
                    "1": {"competition_id": 1},
                },
            ),
        ],
        "Matches of a competition (optionally inside a from/to window) with the Genius LiveStats id.",
        {
            "extra_params": [
                _q("competition_id", "competitionId", COMP_ID, typ="int", required=True),
                _q(
                    "date_from",
                    "from",
                    "Earliest tip-off, an ISO instant such as 2025-10-01T00:00:00.000Z (a bare date is a 400). Omitted = the whole competition.",
                ),
                _q(
                    "date_to",
                    "to",
                    "Latest tip-off, an ISO instant such as 2025-10-31T23:59:59.000Z. Omitted = the whole competition.",
                ),
            ],
            "fixed_params": {"limit": 1000},
            "example_args": {
                "competition_id": 579,
                "date_from": "2025-10-01T00:00:00.000Z",
                "date_to": "2025-10-31T23:59:59.000Z",
            },
        },
    ),
    "match": (
        _R_MATCH,
        P.parse_kzs_match,
        [("full/match__101839.json", _M), *_caps("matches__matchId", _GAMES)],
        "Match header: teams, scores, venue, officials' names, best performers and status.",
        {"path_params": [_p("match_id", MATCH_ID)], "example_args": {"match_id": 101839}},
    ),
    "period_scores": (
        _R_MATCH,
        P.parse_kzs_period_scores,
        [("full/match__101839.json", _M), *_caps("matches__matchId", _GAMES)],
        "Points by match part (quarters and overtimes) from the match header.",
        {"path_params": [_p("match_id", MATCH_ID)], "example_args": {"match_id": 101839}},
    ),
    "pbp": (
        _R_PBP,
        P.parse_kzs_pbp,
        [("full/playByPlay__101839.json.gz", _M), *_caps("matches__matchId__playByPlay", _BOX_GAMES)],
        "Play-by-play of a final game (2015/16 on, with gaps), one row per action with shot x/y.",
        {"path_params": [_p("match_id", MATCH_ID)], "example_args": {"match_id": 101839}},
    ),
    "pbp_players": (
        _R_PBP,
        P.parse_kzs_pbp_players,
        [("full/playByPlay__101839.json.gz", _M), *_caps("matches__matchId__playByPlay", _BOX_GAMES)],
        "The players the play-by-play feed lists for each side of a final game.",
        {"path_params": [_p("match_id", MATCH_ID)], "example_args": {"match_id": 101839}},
    ),
    "box_score": (
        _R_BOX,
        P.parse_kzs_box_score,
        [("full/stats__101839.json", _M), *_caps("matches__matchId__stats", _BOX_GAMES)],
        "Box score of a final game (2015/16 on): one row per player of both teams.",
        {"path_params": [_p("match_id", MATCH_ID)], "example_args": {"match_id": 101839}},
    ),
    "team_box_score": (
        _R_BOX,
        P.parse_kzs_team_box_score,
        [("full/stats__101839.json", _M), *_caps("matches__matchId__stats", _BOX_GAMES)],
        "Team totals of a final game's box score, one row per side.",
        {"path_params": [_p("match_id", MATCH_ID)], "example_args": {"match_id": 101839}},
    ),
    "score_changes": (
        _R_BOX,
        P.parse_kzs_score_changes,
        [("full/stats__101839.json", _M), *_caps("matches__matchId__stats", _BOX_GAMES)],
        "The score at the end of each minute of a final game.",
        {"path_params": [_p("match_id", MATCH_ID)], "example_args": {"match_id": 101839}},
    ),
    "standings": (
        _R_STAND,
        P.parse_kzs_standings,
        [("full/standings__579.json", {"competition_id": 579}), *_caps("matches__standings", _STAND)],
        "Standings of a competition, one row per team in each phase (and group).",
        {
            "extra_params": [_q("competition_id", "competitionId", COMP_ID, typ="int", required=True)],
            "example_args": {"competition_id": 579},
        },
    ),
    "playing_days": (
        _R_DAYS,
        P.parse_kzs_playing_days,
        _one(_R_DAYS, "579", {"competition_id": 579}),
        "Match days of a competition and the number of games on each.",
        {"path_params": [_p("competition_id", COMP_ID)], "example_args": {"competition_id": 579}},
    ),
    "teams": (
        _R_TEAMS,
        P.parse_kzs_teams,
        _one(_R_TEAMS, "579", {"competition_id": 579}),
        "Teams entered in a competition.",
        {
            "extra_params": [_q("competition_id", "competitionId", COMP_ID, typ="int", required=True)],
            "fixed_params": {"limit": 1000},
            "example_args": {"competition_id": 579},
        },
    ),
    "team": (
        _R_TEAM,
        P.parse_kzs_team,
        _one(_R_TEAM, "17551", {"team_id": 17551}),
        "Team profile: club, home hall, colours and the competitions the team entered.",
        {"path_params": [_p("team_id", TEAM_ID)], "example_args": {"team_id": 17551}},
    ),
    "team_general": (
        _R_GENERAL,
        P.parse_kzs_team_general,
        _one(_R_GENERAL, "17551_579", {"team_id": 17551, "competition_id": 579}),
        "A team's record, form and best players in one competition (the body echoes neither id).",
        {
            "path_params": [_p("team_id", TEAM_ID)],
            "extra_params": [_q("competition_id", "competitionId", COMP_ID, typ="int", required=True)],
            "example_args": {"team_id": 17551, "competition_id": 579},
        },
    ),
    "roster": (
        _R_ROSTER,
        P.parse_kzs_roster,
        _one(_R_ROSTER[:-1], "17551_579", {"team_id": 17551, "competition_id": 579}),
        "A team's roster in one competition (the rows carry no team or competition id).",
        {
            "extra_params": [
                _q("team_id", "teamId", TEAM_ID, typ="int", required=True),
                _q("competition_id", "competitionId", COMP_ID, typ="int", required=True),
            ],
            "fixed_params": {"limit": 1000},
            "example_args": {"team_id": 17551, "competition_id": 579},
        },
    ),
    "player": (
        _R_PLAYER,
        P.parse_kzs_player,
        _one(_R_PLAYER, "106285", {"player_id": 106285}),
        "Player profile: names, year of birth, nationality and positions.",
        {"path_params": [_p("player_id", PLAYER_ID)], "example_args": {"player_id": 106285}},
    ),
    "player_stats": (
        _R_PLSTATS,
        P.parse_kzs_player_stats,
        [
            ("full/player_stats__106285_26.json", {"player_id": 106285, "season_id": 26, "season": 2026}),
            (
                "captures/players__playerId__stats__106285_26.json",
                {"player_id": 106285, "season_id": 26, "season": 2026},
            ),
        ],
        "A player's totals and per-game averages in each competition of one season.",
        {
            "path_params": [_p("player_id", PLAYER_ID)],
            "extra_params": [_q("season", "season", SEASON_Q, typ="int", required=True)],
            "example_args": {"player_id": 106285, "season": 2026},
        },
    ),
}

# short -> (returns md section, roots tried in order). A column's md row is ``<root><column>`` over the underscored
# wire path (``data.items.[].playerId`` -> ``data_items_player_id``).
SECTIONS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "seasons": (_R_SEASONS, ("data_items_",)),
    "competitions": (_R_COMPS, ("data_items_",)),
    "competition": (_R_COMP, ("data_",)),
    "phases": (_R_COMP, ("data_phases_",)),
    "rounds": (_R_ROUNDS, ("data_phases_",)),
    "player_leaderboard": (_R_PSTATS, ("data_items_",)),
    "team_leaderboard": (_R_TSTATS, ("data_items_",)),
    "matches": (_R_MATCHES, ("data_items_", "data_items_competitions_")),
    "match": (_R_MATCH, ("data_", "data_competitions_")),
    "period_scores": (_R_MATCH, ("data_scores_",)),
    "pbp": (_R_PBP, ("data_actions_",)),
    "pbp_players": (_R_PBP, ("data_first_team_players_", "data_second_team_players_")),
    "box_score": (_R_BOX, ("data_first_team_player_stats_", "data_second_team_player_stats_")),
    "team_box_score": (_R_BOX, ("data_first_team_team_stats_", "data_second_team_team_stats_")),
    "score_changes": (_R_BOX, ("data_score_changes_", "data_")),
    "standings": (_R_STAND, ("data_phases_standings_", "data_phases_groups_standings_", "data_phases_competition_")),
    "playing_days": (_R_DAYS, ("data_",)),
    "teams": (_R_TEAMS, ("data_items_team_application_", "data_items_")),
    "team": (_R_TEAM, ("data_team_application_", "data_")),
    "team_general": (_R_GENERAL, ("data_",)),
    "roster": (_R_ROSTER, ("data_items_player_person_", "data_items_player_", "data_items_")),
    "player": (_R_PLAYER, ("data_person_", "data_")),
    "player_stats": (_R_PLSTATS, ("data_competition_stats_",)),
}
#: The wire ``id`` of each short, renamed by the parser: the md row is ``<root>id``.
ID_ALIAS: Dict[str, Tuple[str, str]] = {
    "seasons": ("season_id", "id"),
    "competitions": ("competition_id", "id"),
    "competition": ("competition_id", "id"),
    "phases": ("phase_id", "id"),
    "rounds": ("phase_id", "id"),
    "matches": ("match_id", "id"),
    "match": ("match_id", "id"),
    "period_scores": ("score_id", "id"),
    "pbp_players": ("player_id", "id"),
    "teams": ("team_id", "id"),
    "team": ("team_id", "id"),
    "roster": ("roster_entry_id", "id"),
    "player": ("player_id", "id"),
}

_CTX_SEASON = "Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request."
_CTX_SEASON_ID = "Opaque KZS season id (27 = 2026/2027); the sdv season is the END year in season."
_CTX_COMP = "Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608)."
_CTX_MATCH = "Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match)."
_CTX_SIDE = "Which side of the match the row belongs to: home (the organiser, firstTeam on the wire) or away."
_CTX_TEAM = "Team id: one club team in one season (joins kzs_teams); not the match-team id and not the club id."

#: Descriptions of parser-built columns, by column name (apply to every wrapper that has the column).
LOCAL: Dict[str, str] = {
    "season": _CTX_SEASON,
    "season_id": _CTX_SEASON_ID,
    "competition_id": _CTX_COMP,
    "match_id": _CTX_MATCH,
    "team_side": _CTX_SIDE,
    "team_id": _CTX_TEAM,
    "stats_category": "Stat category of the board (PTS points, R rebounds, ASS assists ...): the request's, the rows do not echo it.",
    "position": "1-based position of the row in its table, in the order the API lists it (the wire has no rank column).",
    "group_id": "Group id inside the phase (null when the table is the phase's own).",
    "group_name": "Group name inside the phase (null when the table is the phase's own).",
    "phase_id": "Phase id inside one competition (regular season, play-offs, finals).",
    "genius_match_id": "Genius LiveStats match id parsed from fiba_live_stats_url (org KZS); null before 2015/16.",
    "organiser_team_id": "Team id of the organiser (home) side; null in older headers, which send an empty organiser.",
    "action_number": "1-based position of the action in the feed, which is in game order.",
    "total_minutes": "Length of the game in whole minutes, as sent with the minute-by-minute score.",
    "roster_entry_id": "Roster entry id (the team-player row; not the player_id).",
    "person_id": "Id of the person behind the player registry entry (another key space than player_id).",
    "first_name": "Given name.",
    "last_name": "Family name.",
    "year_of_birth": "Year of birth (an integer; the full birth date is not served).",
    "nationality_id": "Nationality id.",
    "nationality_alpha2_code": "ISO 3166 two-letter code of the nationality.",
    "nationality_alpha3_code": "ISO 3166 three-letter code of the nationality.",
    "nationality_name": "Nationality name in Slovenian.",
    "nationality_eng_name": "Nationality name in English.",
    "positions": "JSON list of the player's positions; empty for most players.",
    "social_medias": "JSON list of the social media accounts attached to the record.",
    "competitions": "JSON list of the competitions the team entered (id, name, rule, phases, type).",
    "competition_phase_id": "Phase id inside the competition the match belongs to.",
    "competition_phase_name": "Phase name (Slovenian), e.g. Redni del = regular season.",
    "competition_name": "Competition name (Slovenian); the sponsor name changes over the years.",
    "competition_type": "Competition type (LEAGUE, CUP ...).",
    "transferred": "True when the match was carried over from an earlier phase.",
    "identification_number": "KZS registration number (another key space than player_id and person_id).",
    "photo_uuid": "Photo id; the image is served at https://api.kzs.si/public/images/{uuid}.",
    "jersey_number": "Jersey number as sent (a string).",
    "homegrown": "True for a player counted as homegrown under the federation's rules.",
    "player_id": "Player registry id (joins kzs_player and the box score); not the person id.",
    "sport_hall_name": "Name of the venue (sports hall) where the game is played or the team is based.",
    "sport_hall_address_id": "Id of the venue's address record.",
    "team_name": "Name of the team for the season, sponsor included.",
    "pts_plus": "Points scored by the team through its games played.",
    "pts_minus": "Points conceded by the team through its games played.",
    "home_won": "Home games the team has won.",
    "home_lost": "Home games the team has lost.",
    "away_won": "Away games the team has won.",
    "away_lost": "Away games the team has lost.",
}

#: Route-specific descriptions (keyed by wrapper short) that replace a returns-md row which only restates the column.
ROUTE: Dict[str, Dict[str, str]] = {
    "player_stats": {
        "player_id": "Player registry id of the request (the body does not echo it).",
        "played": "Games the player played in the competition.",
        "startinglineup": "Games the player started in the competition.",
        "plus_minus": "Season total: the team's point difference while the player was on court.",
        "avgplus_minus": "Per game: the team's point difference while the player was on court.",
    },
}


def _md_sections(path: Path) -> Dict[str, Dict[str, str]]:
    """``{section path: {underscored wire path: description}}`` from the returns md."""
    out: Dict[str, Dict[str, str]] = {}
    current: Optional[str] = None
    head = re.compile(r"^## `([^`]+)`")
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


def _describe(short: str, name: str, md: Dict[str, Dict[str, str]]) -> str:
    """ROUTE, then the route's md row, then LOCAL; the renamed wire ``id`` looks up its md ``id`` row."""
    section, roots = SECTIONS[short]
    route = ROUTE.get(short, {})
    if name in route:
        return route[name]
    rows = md.get(section, {})
    keys = [name]
    alias = ID_ALIAS.get(short)
    if alias and name == alias[0]:
        keys.append(alias[1])
    for key in keys:
        for root in roots:
            hit = rows.get(root + key)
            if hit and not _filler(name, hit):
                return hit
    return LOCAL.get(name, "")


def _filler(name: str, desc: str) -> bool:
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


#: Advanced box-score fields that are whole zeros in every observed game (the feed does not compute them) but are
#: rates when it does: typed Float64 from the start, so a computed value never changes the dtype.
FLOAT_COLS = frozenset(
    {
        "assists_turnover_ratio", "defensive_rating", "offensive_rating", "usage_percentage",
        "true_shooting_percentage", "efficiency_custom", "efficiency_game_score", "p_er",
    }
)  # fmt: skip
_CODE = {pl.Int64: "i", pl.Float64: "f", pl.Boolean: "b", pl.String: "s"}
_TYPE = {"i": "integer", "f": "numeric", "b": "logical", "s": "character"}


def load_fixture(rel: str, ctx: Dict[str, Any]) -> Any:
    """A fixture body as the runtime hands it to a parser: ``data`` of the envelope plus the ``_request`` stamp."""
    path = FIXTURES / rel
    text = gzip.open(path, "rt", encoding="utf-8").read() if rel.endswith(".gz") else path.read_text(encoding="utf-8")
    data = json.loads(text)["data"]
    data = dict(data) if isinstance(data, dict) else {"items": data}
    data["_request"] = ctx
    return data


def _frames(entry: Entry) -> List[pl.DataFrame]:
    _path, parser, fixtures, _summary, _fields = entry
    return [parser(load_fixture(rel, ctx)) for rel, ctx in fixtures]


def _observed(short: str, frames: List[pl.DataFrame]) -> Tuple[List[str], Dict[str, set]]:
    """The columns of a wrapper in first-seen order, and the dtype codes of the ones some fixture fills."""
    seen: Dict[str, set] = {}
    order: List[str] = []
    for df in frames:
        for col, dtype in df.schema.items():
            if col not in order:
                order.append(col)
            if df.height and df[col].null_count() < df.height:
                if dtype not in _CODE:
                    raise SystemExit(f"kzs: {short}.{col} has dtype {dtype}")
                seen.setdefault(col, set()).add(_CODE[dtype])
    return order, seen


def _merge_codes(
    short: str, frames: List[pl.DataFrame], family_seen: Optional[Dict[str, set]] = None
) -> Dict[str, str]:
    """``{column: dtype code}`` over every fixture of a wrapper, in first-seen order.

    A column takes the dtype of the fixtures that hold a value (``i`` and ``f`` merge to ``f``). One that is null in
    every fixture takes the one dtype the family's other wrappers observed for it, so a column name has one dtype
    across the family; failing that it is an Int64 for an id and a String otherwise. Anything else stops the run.
    """
    order, seen = _observed(short, frames)
    out: Dict[str, str] = {}
    for col in order:
        codes = seen.get(col)
        if not codes and family_seen and (len(family_seen.get(col, ())) == 1 or family_seen.get(col) == {"i", "f"}):
            codes = family_seen[col]
        if col in FLOAT_COLS and codes in (None, {"i"}, {"f"}, {"i", "f"}):
            out[col] = "f"
        elif not codes:
            out[col] = "i" if re.search(r"(^|_)id$", col) else "s"
        elif len(codes) == 1:
            out[col] = next(iter(codes))
        elif codes == {"i", "f"}:
            out[col] = "f"
        else:
            raise SystemExit(f"kzs: {short}.{col} has dtypes {sorted(codes)} across fixtures")
    return out


def _write_dtypes(codes: Dict[str, Dict[str, str]]) -> None:
    lines = [
        '"""Column dtypes of the KZS parsers, one map per parser function.',
        "",
        "Generated by ``tools/codegen/gen_kzs.py`` from the returns-schemas",
        "(``tools/codegen/schemas/native/kzs``); do not edit. Codes: ``i`` Int64, ``f`` Float64,",
        "``b`` Boolean, ``s`` String. ``kzs_parsers`` gives these dtypes to an empty frame, a gap-filled",
        "column and an all-null column, so an empty frame stacks onto a populated one.",
        '"""',
        "",
        "from typing import Dict",
        "",
        "DTYPES: Dict[str, Dict[str, str]] = {",
    ]
    by_parser = {ENDPOINTS[short][1].__name__: cols for short, cols in codes.items()}
    for parser in sorted(by_parser):
        lines.append(f'    "{parser}": {{')
        lines += [f'        "{name}": "{code}",' for name, code in by_parser[parser].items()]
        lines.append("    },")
    lines.append("}")
    DTYPES_MODULE.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    spec_path, parser, _fixtures, summary, fields = entry
    fields = copy.deepcopy(fields)
    tokens = re.findall(r"\{([^}]+)\}", spec_path)
    # the board routes also take the competition id in the query: the runtime adds it from the path
    if short.endswith("_leaderboard"):
        path_params = [fields["extra_params"].pop(0)]
        path_params[0] = {k: v for k, v in path_params[0].items() if k != "query_key"}
        fields["path_params"] = path_params
    elif "path_params" in fields and not fields["path_params"]:
        del fields["path_params"]
    assert len(fields.get("path_params", [])) == len(tokens), short
    return {
        "short": short,
        "summary": summary,
        "path": snake_path(spec_path),
        "parser": parser.__name__,
        "returns_schema": f"native/kzs/{short}",
        **fields,
    }


NOTES = [
    "Unofficial and unsupported: these wrappers read the public read API (Java/Spring, built by OnixWeb) that the "
    "kzs.si site itself calls. No terms of use were located (unverified), api.kzs.si/robots.txt answers 401 and "
    "www.kzs.si/robots.txt 404, so no policy or Crawl-delay is published. The runtime waits at least 1 s before every "
    "request, retries included. Payloads are not redistributed as release assets.",
    "Personal data: the API serves officials' and coaches' emails, phones and home towns, and clubs' bank accounts and "
    "addresses, publicly. The parsers never output them (officials appear by name only; club addresses, personnel and "
    "the commissioner report are dropped). Do not add them back.",
    "Seasons are END years (2027 = 2026/2027); KZS' own season ids are opaque (27 = 2026/2027) and the runtime maps "
    "them. Competition ids are per season. Three team keys are never interchangeable: the match-team id (per match "
    "and side), team_id (one club team in one season) and club_id (across seasons). player_id is the registry id, "
    "person_id the person behind it.",
    "Errors are HTTP 400 for everything (an unknown id and a bad parameter alike), so the runtime raises "
    "ValueError for it, never an empty frame. List routes default to 150 rows with no truncation "
    "signal: every list wrapper asks for a large limit and refuses a full page. The play-by-play and box-score "
    "wrappers, the leaderboards, teams, rounds and playing days each make one extra, cached request (the match "
    "header or the competition) to prove the answer is the one asked for and the game is final.",
    "Coverage: scores from 2007/08; box scores and play-by-play from 2015/16. Some 2020/21 men's games have a box "
    "score but no play-by-play (NoDataError): the Genius data.json (genius_match_id in kzs_matches) is the fallback. "
    "Shot x/y are Genius court percent; side (LEFT/RIGHT) names the basket end and teams switch ends at half time, "
    "so normalise with side before stacking shots. Advanced box-score fields are 0 in every observed game.",
]


def main() -> None:
    refs = refs_dir("kzs", SPEC)
    spec = load_spec(refs, SPEC)
    missing = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if missing:
        raise SystemExit(f"kzs: spec paths and ENDPOINTS differ: {sorted(missing)}")
    for short, entry in ENDPOINTS.items():
        names = set(re.findall(r"\{([^}]+)\}", entry[0]))
        assert not {"league", "sport"} & names, short  # the renderer substitutes those as ESPN slugs
        for rel, _ctx in entry[2]:
            assert (FIXTURES / rel).exists(), f"{short}: missing fixture {rel}"
    doc = {
        "api": "kzs",
        "host": HOST,
        "name_pattern": "kzs_{short}",
        "module": "kzs",
        "parser_module": "basketball.kzs.kzs_parsers",
        "getter_module": "sportsdataverse.basketball.kzs.kzs_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "notes": NOTES,
            "raises": [
                "sportsdataverse.errors.NoDataError: the API has nothing for the request (an empty list, a "
                "competition without standings, a final game with no play-by-play or box score).",
                "ValueError: the API answered HTTP 400 (its only error status: an unknown id or a bad parameter).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is unknown or partial: a full "
                "list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, "
                "player or season than the one asked for.",
            ],
            "see_also": [
                {
                    "name": "Košarkarska zveza Slovenije",
                    "url": "https://www.kzs.si/",
                    "note": "the federation site whose API these wrappers read",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball package (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/kzs.yaml", doc)

    md = _md_sections(refs / "kzs-returns.md")
    unknown = {sec for sec, _ in SECTIONS.values()} - set(md)
    if unknown or set(SECTIONS) != set(ENDPOINTS):
        raise SystemExit(f"kzs: SECTIONS out of step with the returns md / ENDPOINTS: {sorted(unknown)}")
    schema_dir = ROOT / "tools/codegen/schemas/native/kzs"
    rewrite_schema_dir(schema_dir)
    P.DTYPES = {}  # parse by the name rules alone, so the last run's kzs_dtypes.py cannot feed back
    frames = {short: _frames(entry) for short, entry in ENDPOINTS.items()}
    family_seen: Dict[str, set] = {}
    for short, fr in frames.items():
        for col, got in _observed(short, fr)[1].items():
            family_seen.setdefault(col, set()).update(got)
    codes = {short: _merge_codes(short, frames[short], family_seen) for short in ENDPOINTS}
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
        raise SystemExit("kzs: filler or missing descriptions:\n" + "\n".join(bad))
    write_yaml(SNIPPET, snippet)
    _write_dtypes(codes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"kzs: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
