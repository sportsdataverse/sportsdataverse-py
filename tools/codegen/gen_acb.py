"""Generate the ``acb`` flat-API endpoint YAML + returns-schemas (Spain's ACB: Liga Endesa, Copa del Rey, ...).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_feb.py`` (itself ``gen_asa.py``
with the shared plumbing in ``gen_soccer_common.py``). Reads the frozen spec and the returns md from the
``sdv-internal-refs`` checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/acb.yaml`` -- 45 wrappers over the spec's 33 routes. One route feeds several frames
  (the box score feeds ``player_box``, ``team_box`` and ``box_info``; ``/Menu/competition-data`` feeds four), each
  with its own parser; :data:`ENDPOINTS` is the source of truth. The wrappers take ids as query arguments and the
  season as its END year (``acb_runtime`` maps it to the API's ``editionId``).
* ``tools/codegen/schemas/native/acb/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser run
  over its committed fixture (the FULL bodies of match 105401 in ``tests/fixtures/acb/full/`` where there is one,
  else the refs capture). Each parser emits a fixed column list with dtypes pinned by name
  (``acb_parsers.FIELDS``), so a fixture cannot mistype a rate. The schemas carry ``description: ''``
  (descriptions never live in ``schemas/**``).
* ``tools/codegen/acb_column_descriptions.snippet.yaml`` -- ``{acb_<short>: {column: text}}``, the block for
  ``manual_column_descriptions.yaml`` (gitignored). Text comes from :data:`LOCAL_BY` (a column whose meaning differs
  by wrapper), then the ``acb-returns.md`` row of the column's source path, then :data:`LOCAL` (derived columns).
  An empty, short (after the frame's noun is appended) or filler text fails the run.
* ``tests/fixtures/acb/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_acb.py``
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

from sportsdataverse.basketball.acb import acb_parsers as P
from sportsdataverse.basketball.acb.acb_runtime import REQUEST_KEY

HOST = "https://api2.acb.com/api"
SPEC = "acb.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "acb"
SNIPPET = ROOT / "tools" / "codegen" / "acb_column_descriptions.snippet.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

MATCH, MATCH_2016, CLUB, LICENCE, LICENCE_IN_MATCH = 105401, 17718, 2, 20209407, 20212243

LEGAL = (
    "Legal notice (sdv-internal-refs/acb/README.md, aviso legal of acb.com read 2026-10-12): ACEB, S.A.U. claims "
    "intellectual property in the site's databases (the EU sui generis database right) among its other content. It "
    "authorises viewing and private copies for personal or journalistic use with the source credited, and requires "
    "prior express consent for distribution, reproduction or public communication. These wrappers fetch on the "
    "user's behalf; a published data release or a hosted mirror needs the owner's decision first. robots.txt allows "
    "the paths."
)
UNOFFICIAL = (
    "Unofficial and unsupported: api2.acb.com is the .NET API behind acb.com and live.acb.com, not a documented public "
    "API. It needs the header ``X-APIKEY``, a public key shipped in acb.com's bundle: ``acb_runtime`` sends the "
    "documented value (``$ACB_API_KEY`` overrides it), re-reads the live bundle once on a 401, and never prints "
    "the key."
)
SEASONS = (
    "Seasons: the API keys a season by ``editionId`` = season START year - 1935 (91 = 2026-27, 48 = 1983-84, the "
    "first). The wrappers take ``season``, the END year (2027 = 2026-27), and ``acb_runtime`` sends "
    "``editionId = season - 1936``. A route that omits ``season`` serves the current edition. ``team_id`` is a "
    "season-team entry (new every season); ``club_id`` is the stable club."
)
COVERAGE = (
    "Coverage: box scores from 1983-84 (old seasons carry only the whole-game period, quarter 0); play-by-play and "
    "shots from 2016-17 (earlier games answer empty lists, which raise NoDataError). Shot coordinates come in two "
    "systems, split by match id: millimetres from the attacked basket from 2020-21 (``coord_system`` mm), a 0-100 "
    "full-court percent grid before (grid). A game that is not finished (``matchFinished`` false) raises "
    "AssetFetchError for the routes that report it."
)

# --- parameters -----------------------------------------------------------------------------------------
_D_MATCH = (
    "Required. ACB match id (integer), one global sequence for every competition: legacy ids (seasons up to 2019-20) "
    "sit below about 20000 (12559 = the 2005-06 final); ids from 2020-21 start around 100000 (105401 = Barca 89-86 "
    "Valencia Basket, Liga 2026-27 round 3)."
)
_D_SEASON = (
    "Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's "
    "``editionId`` = season - 1936. Omitted = the current season."
)
_D_COMP = (
    "Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the "
    "API's default competition."
)
_D_ROUND = (
    "Round id (integer): global across seasons, not the round number (6017 = Liga 2026-27 round 3). List them with "
    "``acb_competition_rounds`` or ``acb_calendar``. Omitted = the current (or last) round."
)
_D_WEEK = "Week id (integer): global across seasons (2989 = the week of 2026-10-10). List them with ``acb_competition_weeks``."
_D_CLUB = (
    "Required. ACB club id (integer), stable across seasons and sponsor names (2 = Barca, 9 = Real Madrid, 13 = "
    "Valencia Basket). Not the season-team ``team_id``."
)
_D_PLAYER = (
    "Required. ACB licence number (integer): the person id everywhere (box score, play-by-play, shots, rosters). "
    "20209407 = Edy Tavares, 20212243 = Olek Balcerowski."
)


def _p(name: str, key: str, typ: str, desc: str, required: bool = False) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": key, "type": typ, "description": desc}
    if required:
        out["required"] = True
    return out


def _match(**ex: Any) -> Dict[str, Any]:
    return {
        "extra_params": [_p("match_id", "matchId", "int", _D_MATCH, True)],
        "example_args": {"match_id": MATCH, **ex},
    }


def _season_comp(
    example_season: int = 2027, club: bool = False, extra: Tuple[Dict[str, Any], ...] = ()
) -> Dict[str, Any]:
    params = [_p("club_id", "clubId", "int", _D_CLUB, True)] if club else []
    params += list(extra)
    params += [_p("competition_id", "competitionId", "int", _D_COMP), _p("season", "season", "int", _D_SEASON)]
    ex: Dict[str, Any] = {"season": example_season}
    if club:
        ex["club_id"] = CLUB
    ex.update({p["name"]: v for p, v in zip(extra, (3,) * len(extra))})
    return {"extra_params": params, "example_args": ex}


def _club(**ex: Any) -> Dict[str, Any]:
    return {"extra_params": [_p("club_id", "clubId", "int", _D_CLUB, True)], "example_args": {"club_id": CLUB, **ex}}


def _player(**ex: Any) -> Dict[str, Any]:
    return {
        "extra_params": [_p("player_id", "playerLicenceId", "int", _D_PLAYER, True)],
        "example_args": {"player_id": LICENCE, **ex},
    }


_COMP_IDS = _p(
    "competition_ids",
    "competitionIds",
    "int",
    "Competition id (1, 2, 3, 10) or a list of ids for several. Omitted = the API's default competition.",
)

# --- fixtures -------------------------------------------------------------------------------------------


def _full(name: str, req: Dict[str, Any]) -> Dict[str, Any]:
    path = FIXTURES / "full" / name
    text = gzip.open(path, "rt", encoding="utf-8").read() if name.endswith(".gz") else path.read_text("utf-8")
    body = json.loads(text)
    body[REQUEST_KEY] = req
    return body


def _cap(name: str, req: Dict[str, Any]) -> Dict[str, Any]:
    body = json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))
    body[REQUEST_KEY] = req
    return body


def full(name: str, **req: Any) -> Callable[[], Dict[str, Any]]:
    return lambda: _full(name, req)


def cap(name: str, **req: Any) -> Callable[[], Dict[str, Any]]:
    return lambda: _cap(name, req)


# short -> (spec path, parser, fixture loader, md sources (section, record prefix), summary, fields)
Source = Tuple[str, str]
Entry = Tuple[str, Callable[..., Any], Callable[[], Dict[str, Any]], Tuple[Source, ...], str, Dict[str, Any]]
_MH, _BX, _PBP, _SH = (
    "/matchdata/MatchHeader/match-header",
    "/matchdata/Result/boxscores",
    "/matchdata/PlayByPlay/play-by-play",
    "/matchdata/MatchShots/match-shots",
)
_LT, _LU, _ML, _TC, _SE = (
    "/matchdata/Overview/lead-tracker",
    "/matchdata/Overview/lineup",
    "/matchdata/Overview/match-leaders",
    "/matchdata/Overview/match-team-comparison",
    "/matchdata/Overview/serie",
)
_QF, _SL, _MA, _PA, _MLIST, _CD = (
    "/matchdata/Preview/quickfacts",
    "/matchdata/Preview/season-leaders",
    "/matchdata/AdvancedStats/match-advanced-stats",
    "/matchdata/AdvancedStats/player-advanced-stats",
    "/matchdata/Menu/matchlist",
    "/matchdata/Menu/competition-data",
)
_CM, _CS, _CB, _CAL, _LZ = (
    "/seasondata/Competition/matches",
    "/seasondata/Competition/standings",
    "/seasondata/Competition/bracket",
    "/seasondata/Calendar/season-calendar",
    "/seasondata/ShotChart/league-zones",
)
_TS, _TM, _NM, _CLUB, _RO, _TL, _TST, _TR = (
    "/seasondata/TeamProfile/teams",
    "/seasondata/TeamProfile/team-matches",
    "/seasondata/TeamProfile/next-matches",
    "/seasondata/TeamProfile/club-data",
    "/seasondata/TeamProfile/current-roster",
    "/seasondata/TeamProfile/team-leaders",
    "/seasondata/TeamProfile/team-stats",
    "/seasondata/TeamProfile/team-records",
)
_PD, _PS, _PC, _PM, _SR = (
    "/seasondata/PlayerProfile/player-data",
    "/seasondata/PlayerProfile/player-stats",
    "/seasondata/PlayerProfile/career",
    "/seasondata/PlayerProfile/matches",
    "/seasondata/Search/search",
)
_PL = "teamBoxscores.[].statsByPeriods.[].stats.players.[]."
_TB = "teamBoxscores.[].statsByPeriods.[].stats."

ENDPOINTS: Dict[str, Entry] = {
    "match_header": (
        _MH,
        P.parse_acb_match_header,
        full("match-header.json", match_id=MATCH),
        ((_MH, ""),),
        "Match header: teams, status, score, bonus, current period and the content the site offers.",
        _match(),
    ),
    "period_scores": (
        _MH,
        P.parse_acb_period_scores,
        full("match-header.json", match_id=MATCH),
        ((_MH, "quarterScores.[]."),),
        "Points each team scored in each period of a game.",
        _match(),
    ),
    "player_box": (
        _BX,
        P.parse_acb_player_box,
        full("boxscores.json", match_id=MATCH),
        ((_BX, _PL),),
        "Per-player box score of a game, per period (quarter 0 = the whole game).",
        _match(),
    ),
    "team_box": (
        _BX,
        P.parse_acb_team_box,
        full("boxscores.json", match_id=MATCH),
        ((_BX, _TB + "total."), (_BX, _TB + "team.")),
        "Team totals of a game per period: everything the team did, and the part credited to the team itself.",
        _match(),
    ),
    "box_info": (
        _BX,
        P.parse_acb_box_info,
        full("boxscores.json", match_id=MATCH),
        ((_BX, "teamBoxscores.[]."), (_BX, "")),
        "Coaches and game facts per team: head coach, assistants, arena, attendance, referees.",
        _match(),
    ),
    "pbp": (
        _PBP,
        P.parse_acb_pbp,
        full("play-by-play.json.gz", match_id=MATCH),
        ((_PBP, "plays.[]."),),
        "Play-by-play of a game (2016-17 onward): every shot, rebound, foul, turnover, substitution and time-out.",
        _match(),
    ),
    "shots": (
        _SH,
        P.parse_acb_shots,
        full("match-shots.json", match_id=MATCH),
        ((_SH, "shotPoints.[]."),),
        "Every field-goal and free-throw attempt of a game (2016-17 onward) with its court position.",
        _match(),
    ),
    "lead_tracker": (
        _LT,
        P.parse_acb_lead_tracker,
        full("lead-tracker.json", match_id=MATCH),
        ((_LT, "leadTrackerInfo.[]."),),
        "Score through a game, one row per scoring moment, with the margin.",
        _match(),
    ),
    "lead_stats": (
        _LT,
        P.parse_acb_lead_stats,
        full("lead-tracker.json", match_id=MATCH),
        ((_LT, "stats.bestScoreRun.home."),),
        "Lead statistics of a game (best run, maximum lead, time ahead, lead changes) per side.",
        _match(),
    ),
    "lineup": (
        _LU,
        P.parse_acb_lineup,
        full("lineup.json", match_id=MATCH),
        ((_LU, "teamLineUps.[].players.[]."),),
        "Players of a game with their game line, starters first.",
        _match(),
    ),
    "match_leaders": (
        _ML,
        P.parse_acb_match_leaders,
        full("match-leaders.json", match_id=MATCH),
        ((_ML, "teamLeaders.[].leaders.rating.[]."),),
        "Each team's leaders of a game by category.",
        _match(),
    ),
    "team_comparison": (
        _TC,
        P.parse_acb_team_comparison,
        cap("matchdata__Overview__match-team-comparison", match_id=MATCH),
        (),
        "Team-versus-team comparison bars of a game per period.",
        _match(),
    ),
    "serie": (
        _SE,
        P.parse_acb_serie,
        cap("matchdata__Overview__serie", match_id=MATCH_2016),
        ((_SE, "matches.[]."), (_SE, "")),
        "The playoff series a game belongs to, one row per game of the series (HTTP 204 outside a series).",
        {"extra_params": [_p("match_id", "matchId", "int", _D_MATCH, True)], "example_args": {"match_id": MATCH_2016}},
    ),
    "quickfacts": (
        _QF,
        P.parse_acb_quickfacts,
        cap("matchdata__Preview__quickfacts", match_id=MATCH),
        ((_QF, "facts.[]."),),
        "Pre-match facts per team: standing, record, recent results.",
        _match(),
    ),
    "season_leaders": (
        _SL,
        P.parse_acb_season_leaders,
        cap("matchdata__Preview__season-leaders", match_id=MATCH),
        ((_SL, "teamLeaders.[].leaders.rating.[]."),),
        "Each team's season leaders before a game, by category.",
        _match(),
    ),
    "match_advanced_stats": (
        _MA,
        P.parse_acb_match_advanced_stats,
        cap("matchdata__AdvancedStats__match-advanced-stats", match_id=MATCH),
        ((_MA, "homeAdvancedStats.fourFactors.efgPct."),),
        "Team advanced statistics of a game against the team's season and win/loss splits.",
        _match(),
    ),
    "player_advanced_stats": (
        _PA,
        P.parse_acb_player_advanced_stats,
        cap("matchdata__AdvancedStats__player-advanced-stats", match_id=MATCH, player_id=LICENCE_IN_MATCH),
        ((_PA, "fourFactors.efgPct."),),
        "One player's advanced statistics in a game against his season and win/loss splits.",
        {
            "extra_params": [
                _p("match_id", "matchId", "int", _D_MATCH, True),
                _p("player_id", "playerLicense", "int", _D_PLAYER, True),
            ],
            "example_args": {"match_id": MATCH, "player_id": LICENCE_IN_MATCH},
        },
    ),
    "matchlist": (
        _MLIST,
        P.parse_acb_matchlist,
        cap("matchdata__Menu__matchlist", match_id=MATCH),
        ((_MLIST, "matches.[]."), (_MLIST, "")),
        "The games of the round (or week) around a game.",
        {
            "extra_params": [
                _p("match_id", "matchId", "int", _D_MATCH, True),
                _p("round_id", "roundId", "int", _D_ROUND),
                _p("week_id", "weekId", "int", _D_WEEK),
            ],
            "example_args": {"match_id": MATCH},
        },
    ),
    "competition_editions": (
        _CD,
        P.parse_acb_competition_editions,
        cap("matchdata__Menu__competition-data"),
        ((_CD, "competitions.[].editions.[]."), (_CD, "competitions.[].")),
        "Every competition edition (season) the API knows, one row per edition.",
        {"extra_params": [_COMP_IDS], "example_args": {}},
    ),
    "competition_rounds": (
        _CD,
        P.parse_acb_competition_rounds,
        cap("matchdata__Menu__competition-data"),
        ((_CD, "competitions.[].editions.[].rounds.[]."),),
        "The rounds of every competition edition: the round ids to crawl.",
        {"extra_params": [_COMP_IDS], "example_args": {}},
    ),
    "competition_weeks": (
        _CD,
        P.parse_acb_competition_weeks,
        cap("matchdata__Menu__competition-data"),
        ((_CD, "competitions.[].editions.[].weeks.[]."),),
        "The weeks of every competition edition: the week ids to crawl.",
        {"extra_params": [_COMP_IDS], "example_args": {}},
    ),
    "competition_current": (
        _CD,
        P.parse_acb_competition_current,
        cap("matchdata__Menu__competition-data"),
        ((_CD, ""),),
        "The API's current pointers: competition, edition, week, round and match.",
        {"extra_params": [_COMP_IDS], "example_args": {}},
    ),
    "round_matches": (
        _CM,
        P.parse_acb_round_matches,
        full("matches.json"),
        ((_CM, "matches.[]."), (_CM, "")),
        "Games of one round (or week) with scores, status and broadcasters.",
        {
            "extra_params": [
                _p("competition_id", "competitionId", "int", _D_COMP),
                _p("season", "season", "int", _D_SEASON),
                _p("round_id", "roundId", "int", _D_ROUND),
                _p("week_id", "weekId", "int", _D_WEEK),
                _p(
                    "is_round_selected",
                    "isRoundSelected",
                    "bool",
                    "With week_id: false = the games played that week across rounds; true resolves the "
                    "week to one round.",
                ),
            ],
            "example_args": {"season": 2027, "round_id": 6017},
        },
    ),
    "standings": (
        _CS,
        P.parse_acb_standings,
        full("standings.json"),
        ((_CS, "standings.[]."), (_CS, "")),
        "League table after a round (default: the current round).",
        {
            "extra_params": [
                _p("competition_id", "competitionId", "int", _D_COMP),
                _p("season", "season", "int", _D_SEASON),
                _p("round_id", "roundId", "int", "Standings after this round. " + _D_ROUND),
            ],
            "example_args": {"season": 2027},
        },
    ),
    "standings_rounds": (
        _CS,
        P.parse_acb_standings_rounds,
        full("standings.json"),
        ((_CS, "standings.[].rounds.[]."), (_CS, "")),
        "Standings history: each team's position after each round, with the round's game.",
        {
            "extra_params": [
                _p("competition_id", "competitionId", "int", _D_COMP),
                _p("season", "season", "int", _D_SEASON),
                _p("round_id", "roundId", "int", "Standings after this round. " + _D_ROUND),
            ],
            "example_args": {"season": 2027},
        },
    ),
    "bracket": (
        _CB,
        P.parse_acb_bracket,
        cap("seasondata__Competition__bracket"),
        ((_CB, "data.quarterFinals.series.[]."),),
        "Playoff bracket: one row per series (quarter-final, semifinal, final).",
        _season_comp(2026),
    ),
    "bracket_games": (
        _CB,
        P.parse_acb_bracket_games,
        cap("seasondata__Competition__bracket"),
        ((_CB, "data.quarterFinals.series.[].matches.[]."), (_CB, "data.quarterFinals.series.[].")),
        "The games of every playoff series of a bracket, one row per game.",
        _season_comp(2026),
    ),
    "calendar": (
        _CAL,
        P.parse_acb_calendar,
        cap("seasondata__Calendar__season-calendar"),
        ((_CAL, "rounds.[].matches.[]."), (_CAL, "rounds.[].")),
        "Every round of a season with its games: one call lists every game id.",
        _season_comp(),
    ),
    "league_zones": (
        _LZ,
        P.parse_acb_league_zones,
        cap("seasondata__ShotChart__league-zones"),
        ((_LZ, "zones.[]."), (_LZ, "")),
        "League-wide shooting by court zone for a season.",
        _season_comp(),
    ),
    "teams": (
        _TS,
        P.parse_acb_teams,
        cap("seasondata__TeamProfile__teams"),
        ((_TS, "teams.[].selected."), (_TS, "teams.[].")),
        "Teams entered in a competition edition.",
        _season_comp(),
    ),
    "team_matches": (
        _TM,
        P.parse_acb_team_matches,
        cap("seasondata__TeamProfile__team-matches", club_id=CLUB),
        ((_TM, "matches.[]."),),
        "A club's games in a competition edition.",
        _season_comp(club=True),
    ),
    "next_matches": (
        _NM,
        P.parse_acb_next_matches,
        cap("seasondata__TeamProfile__next-matches", club_id=CLUB),
        ((_NM, "matches.[]."),),
        "A club's most recent and upcoming games.",
        {
            "extra_params": [
                _p("club_id", "clubId", "int", _D_CLUB, True),
                _p("number_of_matches", "numberOfMatches", "int", "How many games to return (the site asks for 5)."),
                _p("competition_id", "competitionId", "int", _D_COMP),
                _p("season", "season", "int", _D_SEASON),
            ],
            "example_args": {"club_id": CLUB, "number_of_matches": 5},
        },
    ),
    "club_data": (
        _CLUB,
        P.parse_acb_club_data,
        cap("seasondata__TeamProfile__club-data", club_id=CLUB),
        ((_CLUB, "clubData."),),
        "Club profile: arena, contacts and links.",
        _club(),
    ),
    "current_roster": (
        _RO,
        P.parse_acb_current_roster,
        cap("seasondata__TeamProfile__current-roster", club_id=CLUB),
        ((_RO, "players.[]."), (_RO, "")),
        "A club's players and staff for an edition (default: the current one).",
        _season_comp(club=True),
    ),
    "team_leaders": (
        _TL,
        P.parse_acb_team_leaders,
        cap("seasondata__TeamProfile__team-leaders", club_id=CLUB),
        ((_TL, "leaders.points.[]."),),
        "A club's statistical leaders for an edition and all-time.",
        _season_comp(
            club=True,
            extra=(
                _p(
                    "number_of_players",
                    "numberOfPlayers",
                    "int",
                    "Required. Leaders per statistic; 0 is refused (HTTP 400).",
                    True,
                ),
            ),
        ),
    ),
    "team_stats": (
        _TST,
        P.parse_acb_team_stats,
        full("team-stats.json", club_id=CLUB),
        ((_TST, "players.[]."),),
        "A club's player season statistics for an edition: totals, averages and per-40 side by side.",
        _season_comp(club=True),
    ),
    "team_totals": (
        _TST,
        P.parse_acb_team_totals,
        full("team-stats.json", club_id=CLUB),
        ((_TST, "teamTotals."),),
        "A club's team season totals, averages and per-40 values for an edition.",
        _season_comp(club=True),
    ),
    "team_records": (
        _TR,
        P.parse_acb_team_records,
        cap("seasondata__TeamProfile__team-records", club_id=CLUB),
        ((_TR, "rating."),),
        "A club's single-game records (best performance per statistic).",
        _club(),
    ),
    "player_data": (
        _PD,
        P.parse_acb_player_data,
        cap("seasondata__PlayerProfile__player-data", player_id=LICENCE),
        ((_PD, "playerData."),),
        "Player biography: birth, nationality, height, current team and social links.",
        _player(),
    ),
    "player_stats": (
        _PS,
        P.parse_acb_player_stats,
        cap("seasondata__PlayerProfile__player-stats", player_id=LICENCE),
        ((_PS, "statsByTeam.[]."),),
        "A player's season statistics, one row per team and edition.",
        {
            "extra_params": [
                _p("player_id", "playerLicenceId", "int", _D_PLAYER, True),
                _p("competition_id", "competitionId", "int", _D_COMP),
                _p(
                    "season",
                    "season",
                    "int",
                    "Season END year used as an UPPER bound: the rows keep the "
                    "seasons up to this one (2026 drops the 2026-27 row). Omitted = all seasons.",
                ),
            ],
            "example_args": {"player_id": LICENCE},
        },
    ),
    "player_totals": (
        _PS,
        P.parse_acb_player_totals,
        cap("seasondata__PlayerProfile__player-stats", player_id=LICENCE),
        ((_PS, "playerTotals."),),
        "A player's career totals, averages and per-40 values.",
        {
            "extra_params": [
                _p("player_id", "playerLicenceId", "int", _D_PLAYER, True),
                _p("competition_id", "competitionId", "int", _D_COMP),
                _p(
                    "season",
                    "season",
                    "int",
                    "Season END year used as an UPPER bound for the career rows. Omitted = all seasons.",
                ),
            ],
            "example_args": {"player_id": LICENCE},
        },
    ),
    "career_stints": (
        _PC,
        P.parse_acb_career_stints,
        cap("seasondata__PlayerProfile__career", player_id=LICENCE),
        ((_PC, "stints.[]."),),
        "A player's club stints, one row per stint.",
        _player(),
    ),
    "career_trophies": (
        _PC,
        P.parse_acb_career_trophies,
        cap("seasondata__PlayerProfile__career", player_id=LICENCE),
        ((_PC, "trophies.[]."),),
        "A player's trophies, one row per competition.",
        _player(),
    ),
    "player_matches": (
        _PM,
        P.parse_acb_player_matches,
        cap("seasondata__PlayerProfile__matches", player_id=LICENCE),
        ((_PM, "matches.[]."),),
        "A player's game log for an edition (default: the current one).",
        {
            "extra_params": [
                _p("player_id", "playerLicenceId", "int", _D_PLAYER, True),
                _p("season", "season", "int", _D_SEASON),
            ],
            "example_args": {"player_id": LICENCE, "season": 2027},
        },
    ),
    "search": (
        _SR,
        P.parse_acb_search,
        cap("seasondata__Search__search"),
        ((_SR, "players.[]."),),
        "Search players, teams and coaches by name.",
        {
            "extra_params": [
                _p(
                    "search_term",
                    "searchTerm",
                    "str",
                    "Required. Name fragment (accent-insensitive on the player rows); a blank term is refused.",
                    True,
                ),
                _p("limit", "limit", "int", "Maximum results per kind (players, teams, coaches)."),
            ],
            "example_args": {"search_term": "tavares", "limit": 5},
        },
    ),
}

# --- descriptions ---------------------------------------------------------------------------------------
#: Noun appended to a too-short description so it reads on its own (per frame).
NOUN: Dict[str, str] = {
    s: n
    for s, n in {
        "match_header": "match header",
        "period_scores": "period score row",
        "player_box": "player box-score row",
        "team_box": "team box-score row",
        "box_info": "team row of the box score",
        "pbp": "play-by-play event",
        "shots": "shot attempt",
        "lead_tracker": "lead-chart point",
        "lead_stats": "lead statistic",
        "lineup": "lineup player row",
        "match_leaders": "game leader row",
        "team_comparison": "comparison bar",
        "serie": "game of a playoff series",
        "quickfacts": "pre-match team fact",
        "season_leaders": "season leader row",
        "match_advanced_stats": "team advanced statistic",
        "player_advanced_stats": "player advanced statistic",
        "matchlist": "listed game",
        "competition_editions": "competition edition",
        "competition_rounds": "round",
        "competition_weeks": "week",
        "competition_current": "current pointers",
        "round_matches": "game of a round",
        "standings": "standings row",
        "standings_rounds": "team-round standing",
        "bracket": "playoff series",
        "bracket_games": "playoff game",
        "calendar": "calendar game",
        "league_zones": "shot zone",
        "teams": "team entry",
        "team_matches": "club game",
        "next_matches": "club game",
        "club_data": "club profile",
        "current_roster": "roster member",
        "team_leaders": "club leader row",
        "team_stats": "player season row",
        "team_totals": "team season row",
        "team_records": "club record",
        "player_data": "player profile",
        "player_stats": "player season row",
        "player_totals": "player career row",
        "career_stints": "career stint",
        "career_trophies": "trophy row",
        "player_matches": "player game-log row",
        "search": "search hit",
    }.items()
}

#: Derived columns (and renamed ones) shared by many frames; a per-frame override wins (LOCAL_BY).
LOCAL: Dict[str, str] = {
    "match_id": "ACB match id (one global sequence for every competition; legacy ids are below about 20000 up to "
    "2019-20, ids from 2020-21 start around 100000). Taken from the request when the body does not name the game.",
    "edition_id": "Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, "
    "``season``.",
    "season": "Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take.",
    "team_side": "Side of the game the team played: home or away (the feed lists the home team first).",
    "team_id": "Season-team entry id: one club in one competition edition, NEW every season (Barca = 4467 in Liga "
    "2026-27). Not the club: use club_id to follow a club across seasons.",
    "club_id": "Stable ACB club id (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket): the same across seasons and "
    "sponsor names, and the club_id request argument.",
    "team": "Team name as registered for that season (sponsor names change; the club id does not).",
    "team_abbr": "Three-letter team abbreviation (BAR, RMB, VBC).",
    "quarter": "Period: 0 = the whole game, 1-4 the quarters, 5+ overtimes (games before about 2010 carry only 0).",
    "scope": "total = everything the team did (its players plus the team's own events); team = the part credited to the "
    "team itself rather than a player (team rebounds, team turnovers).",
    "play_index": "1-based position of the event in the feed (the feed's order key is not unique).",
    "clock_seconds": "Game clock REMAINING in the period as whole seconds (minute x 60 + second; the clock counts down "
    "from 600, overtimes from 300).",
    "play_name": "Bundle name of play_type (acb_codes.PLAY_TYPES, e.g. TWO_POINTER_MADE); null for a code outside the "
    "enum such as the minute marker 600.",
    "turnover_type": "Name of the turnover detail play_tag (acb_codes.PLAY_TAGS, e.g. BAD_PASS); null when there is no "
    "tag or the tag is unmapped.",
    "coord_system": "Shot coordinate system, told by the match id: mm = millimetres from the attacked basket (match "
    "ids from 2020-21), grid = 0-100 percent of the full court (up to 2019-20). Null when the match id is unknown.",
    "x_basket_m": "Shot x in metres from the centre of the attacked basket towards midcourt, when coord_system is mm and "
    "the attempt is a field goal; null otherwise. Units and origin are inferred by the refs (arc and sideline fit), "
    "not documented by ACB.",
    "y_basket_m": "Shot y in metres across the court from the basket line, negative = the shooter's left, when "
    "coord_system is mm and the attempt is a field goal; null otherwise (inferred by the refs, not documented by ACB).",
    "margin": "Home score minus away score at that moment (positive = the home team leads).",
    "stat": "Statistic name in snake_case (the API's key).",
    "side": "Side the value belongs to: home or away.",
    "lineup_order": "1-based position of the player in the team's list (the feed lists the starters first, then the bench).",
    "metric": "Leader category in snake_case (the API's key, e.g. rating, points, rebounds, assists).",
    "rank": "Place in the category list, 1 = the leader, in the order the feed lists them.",
    "group": "Statistic group in snake_case (four_factors, game_rhythm, ball_handling, shooting, points, ratings, rebounds).",
    "competition_id": "Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa.",
    "round_id": "Global round id (6017 = Liga 2026-27 round 3); not the round number.",
    "round_number": "Round number within its phase.",
    "subphase_id": "Sub-phase id of the round (playoff stages); null in the regular season.",
    "subphase_number": "Sub-phase number of the round; null in the regular season.",
    "week_id": "Global week id (2989 = the week of 2026-10-10).",
    "home_club_id": "Stable club id of the home team (looked up in the body's team list).",
    "home_team": "Home team name for the season (looked up in the body's team list).",
    "home_team_abbr": "Home team three-letter abbreviation (looked up in the body's team list).",
    "away_club_id": "Stable club id of the away team (looked up in the body's team list).",
    "away_team": "Away team name for the season (looked up in the body's team list).",
    "away_team_abbr": "Away team three-letter abbreviation (looked up in the body's team list).",
    "broadcasters": "Broadcast channel names of the game joined with '; ' (the first is the primary).",
    "match_grouping": "How the list was grouped: round or week.",
    "player_id": "ACB licence number: the person id everywhere (box score, play-by-play, shots, rosters, the player "
    "routes). Taken from the request when the body does not name the player.",
    "player_name": "Player display name as the site shows it (the nickname field, e.g. 'Edy Tavares').",
    "first_name": "Player's first name(s); null when the API does not send it.",
    "last_name": "Player's last name(s); null when the API does not send it.",
    "shirt_number": "Shirt number as a string ('0' and '00' differ); null when the API does not send it.",
    "game_role": "Position in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot "
    "(power forward), Pivot (centre); Entrenador (head coach). Often null.",
}

#: Per-wrapper descriptions where a column means something different.
LOCAL_BY: Dict[str, Dict[str, str]] = {
    "match_header": {
        "time_left": "Period clock remaining as 'mm:ss' ('00:00' once the game is over).",
        "home_score": "Home points: the final score once the game is over, else the live score.",
        "away_score": "Away points: the final score once the game is over, else the live score.",
        "home_bonus": "Home team-foul bonus counter of the current period (unverified meaning).",
        "away_bonus": "Away team-foul bonus counter of the current period (unverified meaning).",
        "status": "Game state: FINALIZED once over; the live and scheduled values are unverified.",
        "has_overview": "True when the site offers the overview panels (lead tracker, lineups, leaders) for the game.",
        "has_play_by_play": "True when the site offers play-by-play (2016-17 onward).",
        "has_boxscore": "True when the site offers a box score for the game.",
        "has_advanced": "True when the site offers advanced statistics for the game.",
        "highlights_url": "Highlights video URL; null when none is published.",
        "home_team_id": "Home season-team entry id (new every season).",
        "home_club_id": "Home stable club id (2 = Barca, 9 = Real Madrid).",
        "away_team_id": "Away season-team entry id (new every season).",
        "away_club_id": "Away stable club id.",
        "edition_id": "Season id (START year - 1935) read from the home team entry.",
    },
    "period_scores": {
        "home_score": "Points the home team scored IN this period (not cumulative).",
        "away_score": "Points the away team scored IN this period (not cumulative).",
        "quarter": "Period number: 1-4 quarters, 5+ overtimes (a finished game lists every period played).",
    },
    "player_box": {
        "free_throws_made": "Free throws the player made in the row's period (a count; quarter 0 is the whole game).",
        "free_throws_attempted": "Free throws the player attempted in the row's period (a count; quarter 0 is the whole game).",
        "on_court": "True while the player is on court (false once the game is over).",
        "play_time": "Time on court as 'mm:ss' in the row's period.",
        "seconds_played": "Time on court in whole seconds (play_time parsed).",
        "is_starter": "True when the player started the game (the API's isStarted).",
        "quarter": "Period of the row: 0 = the whole game, 1-4 quarters, 5+ overtimes.",
    },
    "box_info": {
        "head_coach": "Head coach's name.",
        "assistant_coaches": "Assistant coaches' names joined with '; '.",
        "arena": "Arena where the game was played; repeats on both team rows.",
        "attendance": "Announced attendance; repeats on both team rows.",
        "referees": "Referees' names joined with '; '; repeats on both team rows.",
    },
    "pbp": {
        "order": "Event order key, ascending through the game (10, 301, 401, ...); not contiguous and not unique (the "
        "starting five share one).",
        "quarter": "Period of the event: 1-4 quarters, 5+ overtimes.",
        "minute": "Game clock left in the period, minutes part (10 at tip-off; overtimes are 5 minutes).",
        "second": "Game clock left in the period, seconds part.",
        "play_type": "Event code (acb_codes.PLAY_TYPES decodes it; 599 = starting lineup, one row per starter at 10:00).",
        "play_tag": "Turnover detail code on turnovers (acb_codes.PLAY_TAGS); null otherwise.",
        "is_home": "True when the event belongs to the home team (the API's local); false for the away team and for "
        "neutral events.",
        "score_home": "Home score after the event.",
        "score_away": "Away score after the event.",
        "license_type": "Licence class of the player (202 on every player event); meaning unverified; null on team "
        "and clock events.",
        "player_id": "ACB licence number of the player; null on team and clock events.",
        "player_name": "Player display name; null on team and clock events.",
        "shirt_number": "Shirt number as a string; null on starting-lineup rows and on team events.",
        "series_wins": "Home team's series wins, on the END_OF_GAME event of a 2016-17 playoff game only.",
        "series_losses": "Home team's series losses, on the same event.",
        **{
            f"run_{c}": f"The acting player's running {c.replace('_', ' ')} AFTER this event (cumulative in the game)."
            for c, _ in P._RUN_STATS
        },
    },
    "shots": {
        "shot_id": "Event id of the attempt within one match; joins nothing outside this body.",
        "pos_x": "Court x as sent. mm system: millimetres from the attacked basket towards midcourt (never negative; 0 "
        "for free throws and dunks); grid system: 0-100 percent of the court length, not mirrored.",
        "pos_y": "Court y as sent. mm system: millimetres across the court (sidelines at -7500 and +7500, negative = "
        "the shooter's left); grid system: 0-100 percent of the width (which sideline is 0 is unverified). Free "
        "throws are (0, 0).",
        "play_type": "Event code (92 free throw made, 93 two made, 94 three made, 96 free throw missed, 97 two "
        "missed, 98 three missed, 100 dunk, 533 dunk missed).",
        "player_id": "ACB licence number of the shooter.",
        "player_name": "Shooter's short name from the game's shooter list (e.g. 'T. Martin').",
        "shirt_number": "Shooter's shirt number as a string, from the game's shooter list.",
        "zone": "Shot zone (acb_codes.SHOT_ZONES: restricted_area, paint, mid_range_*, corner_three_*, wing_three_*, "
        "top_three); null on free throws; legacy zones (up to 2019-20) are unreliable.",
        "made": "True when the shot went in; null on some free throws.",
        "points": "Points the attempt scored (2 or 3 on made field goals); null on free throws and misses.",
        "is_home": "True when the shooter's team is the home team (the API's local).",
        "score_home": "Home score after the attempt.",
        "score_away": "Away score after the attempt.",
        "quarter": "Period of the attempt: 1-4 quarters, 5+ overtimes.",
    },
    "lead_tracker": {
        "home_score": "Home score at that moment.",
        "away_score": "Away score at that moment.",
        "minute": "Game clock left in the period, minutes part.",
        "second": "Game clock left in the period, seconds part.",
    },
    "lead_stats": {
        "stat": "Statistic: best_score_run, max_difference, time_ahead or lead_changes.",
        "value": "Display value as sent: a count, or 'mm:ss' for time_ahead.",
        "bar_progress": "Length of the site's comparison bar (a drawing value, not a statistic).",
        "is_highlighted": "True for the side whose bar the site highlights as the better value.",
    },
    "lineup": {
        "player_id": "ACB licence number of the player.",
        "short_name": "Short display name ('T. Martin').",
        "play_time": "Time on court in the game as 'mm:ss'.",
        "seconds_played": "Time on court in whole seconds (play_time parsed).",
        "is_playing": "True while the player is on court (false once the game is over).",
        "points": "Points scored in the game.",
        "rebounds": "Total rebounds in the game.",
        "assists": "Assists in the game.",
        "fouls": "Personal fouls committed in the game.",
        "differential": "Point differential while the player was on court (plus/minus).",
        "rating": "ACB rating (valoracion) of the game.",
    },
    "match_leaders": {"value": "The player's count in the category (points, rebounds, assists, rating)."},
    "season_leaders": {
        "value": "The player's season value in the category before this game (a total or an average depending on the "
        "category).",
    },
    "team_comparison": {
        "quarter": "Period of the bar: 0 = the whole game, 1-4 quarters.",
        "stat": "Compared statistic (free_throws, two_pointers, three_pointers, field_goals, rebounds, assists, "
        "steals, turnovers, personal_fouls, time_outs, points_off_turnover, paint_points, second_chance_points, "
        "fast_break_points, bench_points ...).",
        "home_value": "Home value of the bar: a percentage 0-100 for the shooting statistics (the API's '79,17%' "
        "decimal comma parsed), a count otherwise.",
        "away_value": "Away value of the bar, same units as home_value.",
        "home_made": "Home shots made, from the 'made/attempted' subtitle; null for the non-shooting statistics.",
        "home_attempted": "Home shots attempted, from the subtitle; null for the non-shooting statistics.",
        "away_made": "Away shots made, from the subtitle; null for the non-shooting statistics.",
        "away_attempted": "Away shots attempted, from the subtitle; null for the non-shooting statistics.",
        "home_is_highlighted": "True when the site highlights the home bar as the better value.",
        "away_is_highlighted": "True when the site highlights the away bar as the better value.",
    },
    "serie": {
        "requested_match_id": "The match asked for (any game of the series); every row of the series repeats it.",
        "match_id": "ACB match id of this game of the series.",
        "phase_id": "Phase id of the series (111 = Play Off Titulo).",
        "subphase_id": "Sub-phase id of the series (the playoff stage).",
        "series_home_team_id": "Season-team entry of the series' home-court team (new every season).",
        "series_home_club_id": "Stable club id of the series' home-court team.",
        "series_away_team_id": "Season-team entry of the other team of the series.",
        "series_away_club_id": "Stable club id of the other team of the series.",
        "series_home_wins": "Games won by the home-court team in the series (the API's homeScore).",
        "series_away_wins": "Games won by the other team in the series (the API's awayScore).",
        "series_finished": "True once the series is decided.",
        "best_of": "Series length (best of N) as sent; 0 in the 2016-17 final sample, so read 0 as unknown.",
        "home_team_id": "Home season-team entry of THIS game (the series' home-court team is series_home_team_id).",
        "away_team_id": "Away season-team entry of THIS game.",
        "home_score": "Home points of this game.",
        "away_score": "Away points of this game.",
        "start": "Tip-off instant (UTC).",
        "status": "Game state: FINALIZED once over.",
        "is_on_half_time": "True during the half-time break of a live game.",
        "current_quarter": "Current or last period of the game (4 for a finished regulation game).",
    },
    "quickfacts": {
        "standing": "League position before the game.",
        "wins": "Wins before the game.",
        "losses": "Losses before the game (the API's currentLoses).",
        "last_results": "Recent results joined with '; ' (WIN / LOSS), in the order the API sends them.",
        "season": "Season END year (edition_id + 1936).",
        "team_id": "Season-team entry id (new every season).",
        "club_id": "Stable club id (2 = Barca, 9 = Real Madrid).",
        "team": "Team name for the season.",
        "team_abbr": "Three-letter team abbreviation.",
        "edition_id": "Season id (START year - 1935).",
    },
    "match_advanced_stats": {
        "stat": "Statistic in snake_case (efg_pct, orb_pct, tov_pct, f_tr = free-throw rate, possessions, pace, "
        "points_per_possession, ast_ratio, ..., net_rating, oer, der, trb_pct).",
        "match_value": "The statistic in this game (the API's partido).",
        "season_value": "The team's value over the season (the API's temporada).",
        "win_value": "The team's value in its wins (the API's win).",
        "loss_value": "The team's value in its losses (the API's loss).",
        "season_diff": "Game value minus season value (temporadaDiff).",
        "win_diff": "Game value minus the wins value (winDiff).",
        "loss_diff": "Game value minus the losses value (lossDiff).",
    },
    "player_advanced_stats": {
        "stat": "Statistic in snake_case (efg_pct, ts_pct, ast_pct, usage-style ratios, pace ...).",
        "match_value": "The statistic in this game (the API's partido).",
        "season_value": "The player's value over the season (temporada).",
        "win_value": "The player's value in his team's wins.",
        "loss_value": "The player's value in his team's losses.",
        "season_diff": "Game value minus season value (temporadaDiff).",
        "win_diff": "Game value minus the wins value (winDiff).",
        "loss_diff": "Game value minus the losses value (lossDiff).",
    },
    "matchlist": {
        "home_wins": "Home team's series wins before the game (playoff series only).",
        "home_losses": "Home team's series losses before the game (playoff series only).",
        "away_wins": "Away team's series wins before the game (playoff series only).",
        "away_losses": "Away team's series losses before the game (playoff series only).",
        "is_selected_local": "Null in this list (set only in a club's game list).",
    },
    "competition_editions": {
        "competition_name": "The API's competition name ('Liga Nacional' for the Liga).",
        "edition_id": "Season id (START year - 1935).",
        "season_start_year": "Calendar year the season starts (2026 for 2026-27).",
        "season": "Season END year (2027 for 2026-27) = seasonEndYear.",
        "n_rounds": "Rounds the edition lists.",
        "n_weeks": "Weeks the edition lists.",
    },
    "competition_rounds": {"edition_id": "Season id the round belongs to (START year - 1935)."},
    "competition_weeks": {
        "edition_id": "Season id the week belongs to (START year - 1935).",
        "description": "Spanish week label ('Semana 1').",
        "start_date": "Monday the week starts (UTC midnight).",
    },
    "competition_current": {
        "competition_id": "Competition the site opens by default.",
        "edition_id": "Edition (season) the site opens by default.",
        "week_id": "Week the site opens by default.",
        "round_id": "Round the site opens by default.",
        "match_id": "The current match (the featured game).",
        "status": "State of the current match.",
        "start": "Tip-off instant (UTC) of the current match.",
        "home_team_id": "Home season-team entry id of the current match.",
        "home_club_id": "Home stable club id of the current match.",
        "home_team": "Home team name of the current match.",
        "home_team_abbr": "Home team abbreviation of the current match.",
        "away_team_id": "Away season-team entry id of the current match.",
        "away_club_id": "Away stable club id of the current match.",
        "away_team": "Away team name of the current match.",
        "away_team_abbr": "Away team abbreviation of the current match.",
    },
    "round_matches": {
        "ticketing_link": "Ticket shop URL; null when none.",
        "round_number": "Round number within the phase, looked up from the body's round list.",
        "week_id": "Week selected in the request (the API's selectedFilters.week); null when none.",
        "home_team_id": "Home season-team entry id (new every season; joins the body's team list).",
        "away_team_id": "Away season-team entry id (new every season; joins the body's team list).",
    },
    "calendar": {
        "ticketing_link": "Ticket shop URL; null when none.",
        "week_id": "Not sent by the calendar route (always null).",
        "home_team_id": "Home season-team entry id (new every season; joins the body's team list).",
        "away_team_id": "Away season-team entry id (new every season; joins the body's team list).",
    },
    "standings": {
        "round_id": "Round the table is as of (the selected round).",
        "total_rounds": "Rounds in the phase.",
        "team_id": "Season-team entry id (new every season).",
        "position_trend": "Change in position since the previous round: positive = up (meaning of the sign unverified).",
        "win_pct": "Win percentage, 0-100.",
        "home_wins": "Wins at home (the API's localBalanceWins).",
        "home_losses": "Losses at home.",
        "away_wins": "Wins away from home (the API's awayBalanceWins).",
        "away_losses": "Losses away from home (the API's awayBalanceLosses).",
        "plus_minus": "Point differential (points for minus points against).",
    },
    "standings_rounds": {
        "round_id": "Global round id of the standing.",
        "position": "The team's league position after this round.",
        "match_id": "The team's game in this round.",
    },
    "bracket": {
        "stage": "Bracket stage: quarter_finals, semifinals, final (other_positions when present).",
        "n_games": "Games listed for the series.",
        "home_wins": "Games won by the home-court team in the series.",
        "away_wins": "Games won by the other team in the series.",
        "finished": "True once the series is decided.",
        "best_of": "Series length (best of N).",
        "phase_id": "Phase id of the series (111 = Play Off Titulo).",
        "subphase_id": "Sub-phase id (the playoff stage).",
    },
    "bracket_games": {"stage": "Bracket stage: quarter_finals, semifinals, final, other_positions."},
    "league_zones": {
        "phase_id": "Phase the statistics cover; null in the sample (read as all phases, unverified).",
        "zone": "Court zone (acb_codes.SHOT_ZONES): restricted_area, paint, mid_range_*, corner_three_*, wing_three_*, "
        "top_three.",
        "attempts": "Field-goal attempts in the zone, league-wide.",
        "made": "Field goals made in the zone, league-wide.",
        "points_scored": "Points scored from the zone, league-wide.",
    },
    "next_matches": {"is_selected_local": "Not sent by this route (always null)."},
    "teams": {
        "team_id": "Season-team entry of the selected edition (new every season).",
        "latest_team_id": "The club's most recent season-team entry.",
        "latest_team": "The club's most recent team name.",
        "competition_id": "Competition of the selected entry.",
        "edition_id": "Season id of the selected entry (START year - 1935).",
    },
    "team_matches": {
        "is_selected_local": "True when the requested club played at home.",
        "is_on_half_time": "Not sent by this route (always null).",
        "current_quarter": "Not sent by this route (always null).",
        "home_wins": "Not sent by this route (series records are in acb_matchlist).",
        "home_losses": "Not sent by this route (series records are in acb_matchlist).",
        "away_wins": "Not sent by this route (series records are in acb_matchlist).",
        "away_losses": "Not sent by this route (series records are in acb_matchlist).",
    },
    "club_data": {
        "stadium_name": "Arena name (the club's home venue).",
        "stadium_address": "Arena street address.",
        "stadium_capacity": "Arena capacity.",
        "president": "Club president's name; null for most clubs.",
        "foundation_year": "Year the club was founded.",
        "web_url": "Club website URL.",
        "ticketing_url": "Club ticket shop URL.",
        "merchandising_url": "Club merchandise shop URL (the API's merchanUrl).",
        "instagram_url": "Club Instagram URL.",
        "twitter_url": "Club X / Twitter URL.",
        "facebook_url": "Club Facebook URL.",
        "youtube_url": "Club YouTube URL.",
        "tiktok_url": "Club TikTok URL.",
        "validated": "True when the club profile is validated by the league.",
        "temporary_roster": "True while the club's roster is provisional.",
    },
    "current_roster": {
        "club_id": "Stable club id asked for (the body names no club).",
        "role": "player or staff (head coach and assistants).",
        "person_id": "ACB licence number of the person; a player's player_id elsewhere.",
        "person_name": "Display name as the site shows it.",
        "first_name": "First name(s); null when the API does not send it.",
        "last_name": "Last name(s); null when the API does not send it.",
        "shirt_number": "Shirt number as a string; null for staff.",
        "game_role": "Position or staff role in Spanish (Base, Escolta, Alero, Ala-pivot, Pivot; Entrenador, Segundo "
        "Entrenador).",
        "licensing": "Licence code (for example EUR, JFL, CRE); meaning unverified.",
        "age": "Age in years as sent by the roster route.",
        "nationality": "Nationality as sent, a Spanish country name ('Serbia', 'EE.UU.').",
        "nationality_iso": "ISO 3166-1 alpha-2 code of nationality, from the 31 observed names; null for an unseen name.",
        "height_cm": "Height in centimetres (players only).",
        "is_license_active": "True when the licence is active in the current season.",
        "edition_id": "Season id the person record was read for (START year - 1935).",
        "temporary_roster": "True while the roster is provisional.",
    },
    "team_leaders": {
        "club_id": "Stable club id asked for.",
        "competition_id": "Competition of the selected edition.",
        "scope": "all_time = club record holders (the API's leaders); edition = the selected season (seasonLeaders). "
        "The assignment is inferred from the values (689 games for J.C. Navarro is a career figure).",
        "metric": "Category in snake_case; avg_* are per-game averages, the rest totals.",
        "value": "The player's value in the category (a total or an average depending on the metric).",
    },
    "team_stats": {
        "club_id": "Stable club id of the club asked for (2 = Barca).",
        "team_id": "Season-team entry id of the selected edition.",
    },
    "team_totals": {
        "club_id": "Stable club id of the club asked for (2 = Barca).",
        "team_id": "Season-team entry id (the API's teamEditionId).",
    },
    "team_records": {
        "club_id": "Stable club id asked for.",
        "stat": "Record category in snake_case (rating, points, rebounds, assists, steals, ...).",
        "value": "The record value (a count; Float64 so every category fits).",
        "game_id": "Match id of the record game (legacy sequence for old games).",
        "game_date": "Date of the record game (UTC midnight).",
        "local_team_id": "Home season-team entry of the record game.",
        "local_club_id": "Home stable club id of the record game.",
        "local_team": "Home team name in the record game.",
        "local_team_abbr": "Home team abbreviation in the record game.",
        "visitor_team_id": "Away season-team entry of the record game.",
        "visitor_club_id": "Away stable club id of the record game.",
        "visitor_team": "Away team name in the record game.",
        "visitor_team_abbr": "Away team abbreviation in the record game.",
    },
    "player_data": {
        "player_id": "ACB licence number of the player.",
        "birth_date": "Birth date (the API sends DD-MM-YYYY).",
        "birth_place": "Birth place (town or city) as sent.",
        "birth_country": "Birth country as sent, a Spanish name.",
        "birth_country_iso": "ISO 3166-1 alpha-2 code of the birth country; null for an unseen name.",
        "nationality": "Nationality as sent, a Spanish country name.",
        "nationality_iso": "ISO 3166-1 alpha-2 code of nationality; null for an unseen name.",
        "height_m": "Height in metres (the API sends '2,20', a decimal comma).",
        "player_number": "Current shirt number (integer).",
        "team_id": "Current team's season-team entry id.",
        "club_id": "Current team's stable club id.",
        "team": "Current team name for the edition.",
        "team_abbr": "Current team abbreviation.",
        "edition_id": "Season id the player record was read for (START year - 1935).",
        "licensing": "Licence code (for example EUR, JFL); meaning unverified.",
        "instagram": "Instagram handle; null when none.",
        "twitter": "X / Twitter handle; null when none.",
        "facebook": "Facebook handle; null when none.",
        "youtube": "YouTube handle; null when none.",
        "tiktok": "TikTok handle; null when none.",
        "pronunciation_url": "URL of an audio clip of the name; null when none.",
        "is_license_active": "True when the licence is active in the current season.",
    },
    "player_stats": {
        "player_id": "ACB licence number asked for (the body names the player only in the totals block).",
        "team_id": "Season-team entry the row covers (the API's teamEditionId).",
        "competition_id": "Competition of the row.",
        "edition_id": "Season id of the row (START year - 1935).",
        "edition_year": "Season label as sent ('2026-2027').",
        "club_id": "Stable club id of the team.",
        "team": "Team name that season.",
        "team_abbr": "Team abbreviation that season.",
    },
    "player_totals": {"player_id": "ACB licence number of the player."},
    "career_stints": {
        "player_id": "ACB licence number asked for.",
        "initial_year": "Year the stint starts.",
        "end_year": "Year the stint ends (the season's END year).",
        "n_editions": "Editions the stint spans.",
        "has_no_validated_data": "True when the stint has no validated data.",
        "team_id": "Team entry of the stint's latest edition.",
        "edition_id": "Season id of the stint's team entry (START year - 1935).",
    },
    "career_trophies": {
        "player_id": "ACB licence number asked for.",
        "times_champion": "Times the player won the competition.",
        "times_runner_up": "Times the player was runner-up in the competition.",
    },
    "player_matches": {
        "free_throws_made": "Free throws the player made in that match (a count, not a season total).",
        "free_throws_attempted": "Free throws the player attempted in that match (a count, not a season total).",
        "offensive_rebounds": "Offensive rebounds the player took in that match (a count; part of the total rebounds).",
        "defensive_rebounds": "Defensive rebounds the player took in that match (a count; part of the total rebounds).",
        "player_id": "ACB licence number asked for.",
        "match_id": "ACB match id of the game.",
        "round_type": "Phase code of the game (LR regular season, PO playoff).",
        "round_number": "Round number within the phase.",
        "date": "Tip-off instant (UTC).",
        "is_selected_local": "True when the player's team played at home.",
        "has_won": "True when the player's team won the game.",
        "play_time": "Time on court as 'mm:ss'.",
        "seconds_played": "Time on court in whole seconds (play_time parsed).",
        "edition_id": "Season id read from the home team entry (START year - 1935).",
        "rebounds": "Total rebounds in the game.",
        "shots_rejected": "Shots of the player that were blocked.",
        "fouls_received": "Fouls drawn by the player in the game.",
        "rating": "ACB rating (valoracion) of the game.",
    },
    "search": {
        "kind": "player, team or coach.",
        "person_id": "ACB licence number (players, coaches) or the team entry id (teams).",
        "name": "Display name (nickname) of a player or coach; the team name of a team.",
        "short_name": "Short display name ('T. Martin').",
        "club_id": "Stable club id of a team hit; null for players.",
        "edition_id": "Season id of the record; null in search results.",
    },
}
for _col in (
    "phase_id",
    "subphase_id",
    "series_home_team_id",
    "series_home_club_id",
    "series_away_team_id",
    "series_away_club_id",
    "series_home_wins",
    "series_away_wins",
    "series_finished",
    "best_of",
):
    LOCAL_BY["bracket_games"][_col] = LOCAL_BY["serie"][_col]
del _col
_PAGE_STAT = {
    "numGames": "games played",
    "timePlayed": "time on court in seconds",
    "starter": "games started",
    "points": "points",
    "maxPoints": "most points in a single game",
    "threePointersMade": "three-point field goals made",
    "threePointersAttempted": "three-point field goals attempted",
    "threePointersPercentage": "three-point percentage (0-100)",
    "twoPointersMade": "two-point field goals made",
    "twoPointersAttempted": "two-point field goals attempted",
    "twoPointersPercentage": "two-point percentage (0-100)",
    "freeThrowsMade": "free throws made",
    "freeThrowsAttempted": "free throws attempted",
    "freeThrowsPercentage": "free-throw percentage (0-100)",
    "rebounds": "total rebounds",
    "offensiveRebounds": "offensive rebounds",
    "defensiveRebounds": "defensive rebounds",
    "assists": "assists",
    "steals": "steals",
    "turnovers": "turnovers",
    "blocks": "blocks made",
    "shotsRejected": "own shots blocked",
    "dunks": "dunks",
    "personalFouls": "personal fouls committed",
    "foulsReceived": "fouls drawn",
    "rating": "ACB rating (valoracion)",
    "plusMinus": "plus/minus",
    "wins": "team wins in the counted games (meaning unverified)",
    "losses": "team losses in the counted games (meaning unverified)",
}
_BASIS = {"total": "Season total of", "avg": "Per-game average of", "per40": "Per-40-minute rate of"}

_HEADING = re.compile(r"^## `(/\S+)`\s*$")
_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|[^|]*\|(.*)\|\s*$")
_UNOBS = re.compile(r";?\s*unobserved: null, absent or empty in every sampled row\.?\s*$")


def _md_rows(path: Path) -> Dict[str, Dict[str, str]]:
    """``{route: {record path: description}}`` from the returns md."""
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
        if m and section:
            text = _UNOBS.sub("", m.group(2).strip().replace("&lt;", "<")).strip()
            out.setdefault(section, {}).setdefault(m.group(1), text)
    return out


def _describe(short: str, col: str, path: Optional[str], md: Dict[str, Dict[str, str]]) -> str:
    entry = ENDPOINTS[short]
    if col in LOCAL_BY.get(short, {}):
        return LOCAL_BY[short][col]
    text = ""
    if path is not None:
        block, _, base = path.rpartition(".")
        if block.split(".")[-1] in ("totals", "averages", "per40") and base in _PAGE_STAT:
            tag = {"totals": "total", "averages": "avg", "per40": "per40"}[block.split(".")[-1]]
            if base == "timePlayed" and tag == "per40":
                return "Always null: the per-40 block carries no time played."
            return f"{_BASIS[tag]} {_PAGE_STAT[base]}."
        for section, prefix in entry[3]:
            cand = md.get(section, {}).get(prefix + path)
            if cand and not cand.lower().startswith("unobserved"):
                text = cand
                break
    if not text:
        text = LOCAL.get(col, "")
    if text and len(text) < 15:
        text = f"{text} ({NOUN[short]})"
    return text


def _frame(short: str) -> pl.DataFrame:
    """The parser's frame over the entry's fixture."""
    entry = ENDPOINTS[short]
    frame = entry[1](entry[2]())
    assert isinstance(frame, pl.DataFrame)
    return frame


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    path, parser, _fix, _src, summary, fields = entry
    names = [p["name"] for p in fields.get("path_params", [])]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert not {"league", "sport"} & {p["name"] for p in fields.get("extra_params", [])}, path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "parser": parser.__name__,
        "returns_schema": f"native/acb/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


def main() -> None:
    refs = refs_dir("acb", SPEC)
    spec = load_spec(refs, SPEC)
    differs = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if differs:
        raise SystemExit(f"acb: spec paths and ENDPOINTS differ: {sorted(differs)}")
    assert set(P.FIELDS) == set(ENDPOINTS) == set(NOUN), "every parser needs an endpoint, a noun and the reverse"
    doc = {
        "api": "acb",
        "host": HOST,
        "name_pattern": "acb_{short}",
        "module": "acb",
        "parser_module": "basketball.acb.acb_parsers",
        "getter_module": "sportsdataverse.basketball.acb.acb_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        # manual_column_descriptions.yaml keys: bare shorts (pbp, shots, standings) name other families
        "schema_prefix": "acb_",
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "raw_doc": "the raw body (``dict``): the route's JSON plus ``_request`` (the ids asked for, in snake_case, "
            "with ``season`` the END year)",
            "raises": [
                "sportsdataverse.errors.NoDataError: the ACB has nothing for the request (an unknown id, a game outside "
                "a playoff series, play-by-play or shots before 2016-17, a club with no games that season).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is not the one asked for (a "
                "rejected key, an error envelope or stub, a body for another season, round, club or player, a game that "
                "is not finished).",
                "ValueError: a missing or non-integer id, or a season before 1984 (raised before any request), or the "
                "API's HTTP 400 / 422.",
            ],
            "notes": [LEGAL, UNOFFICIAL, SEASONS, COVERAGE],
            "see_also": [
                {"name": "ACB", "url": "https://www.acb.com", "note": "the league site whose API these wrappers read"},
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
    write_yaml(ROOT / "tools/codegen/endpoints/acb.yaml", doc)

    md = _md_rows(refs / "acb-returns.md")
    schema_dir = ROOT / "tools/codegen/schemas/native/acb"
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short in ENDPOINTS:
        cols: List[Dict[str, str]] = columns_from_frame(_frame(short), [], leaf_fallback=False)
        paths = {c: p for c, _t, p in P.FIELDS[short]}
        block = snippet.setdefault(f"acb_{short}", {})
        for col in cols:
            text = _describe(short, col["name"], paths.get(col["name"]), md).strip()
            if len(text) < 15 or text.lower() == col["name"].lower() or _BANNED.match(text):
                bad.append(f"acb_{short}.{col['name']}: {text!r}")
            block[col["name"]] = text
            col["description"] = ""
        write_yaml(schema_dir / f"{short}.yaml", {"schema": f"acb_{short}", "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit(f"acb: empty or filler descriptions (add them to LOCAL / LOCAL_BY): {bad}")
    write_yaml(SNIPPET, snippet)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"acb: {len(ENDPOINTS)} endpoints, {sum(map(len, snippet.values()))} descriptions -> {SNIPPET.name}")


if __name__ == "__main__":
    main()
