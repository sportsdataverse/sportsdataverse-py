"""Generate the ``kbl`` flat-API endpoint YAML + returns-schemas (KBL, the Korean Basketball League, ``api.kbl.or.kr``).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_kzs.py`` with the shared plumbing in
``gen_soccer_common.py``. Reads the frozen spec, the README crosswalk and the returns md from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/kbl.yaml`` -- 22 wrappers over the spec's 17 paths. One route can feed several frames (the
  game header feeds ``kbl_game`` and ``kbl_period_scores``, the match chart three frames, team-tracks two, the playoff
  bracket three); :data:`ENDPOINTS` is the source of truth. The wrapper ``path`` is the PUBLIC route: a season route
  carries the END year (``/league/rank/{season}``) where the API has a ``glkey``, and ``kbl_runtime`` rewrites it.
* ``tools/codegen/schemas/native/kbl/<short>.yaml`` -- one returns-schema per wrapper: the columns of its parser
  (``kbl_parsers.SPECS``, which also types an empty frame) checked against the parser run over its committed fixtures
  (the FULL bodies in ``tests/fixtures/kbl/full/`` plus the refs captures in ``tests/fixtures/kbl/captures/``). Schema key
  ``kbl_<short>`` (``schema_prefix``); every ``description`` is ``''``.
* ``tools/codegen/notes/kbl_descriptions.yaml`` -- the column descriptions (committed: the generator owns the text),
  keyed ``kbl_<short>`` -> column, for ``manual_column_descriptions.yaml``. Each comes from :data:`ROUTE`
  (route-specific), the route's own ``kbl-returns.md`` row (:data:`SECTIONS`), else :data:`LOCAL` (columns the parser
  derives). Korean labels are carried with the refs' English translation. A filler or missing description stops the run.
* ``tests/fixtures/kbl/spec_paths.json`` -- the spec's routes and the route each wrapper reads, for the tests (CI has
  no refs checkout); ``tests/fixtures/kbl/season_crosswalk.json`` -- the README's seasonCode crosswalk, which the run
  also compares with ``kbl_codes.SEASONS``.

Run: ``python tools/codegen/gen_kbl.py``
"""

from __future__ import annotations

import copy
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from gen_soccer_common import load_spec, refs_dir, rewrite_schema_dir, write_yaml  # noqa: E402

from sportsdataverse.basketball.kbl import kbl_codes as codes  # noqa: E402
from sportsdataverse.basketball.kbl import kbl_parsers as P  # noqa: E402
from sportsdataverse.dl_utils import underscore  # noqa: E402

HOST = "https://api.kbl.or.kr"
SPEC = "kbl.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "kbl"
SCHEMA_PREFIX = "kbl_"
SNIPPET = ROOT / "tools" / "codegen" / "notes" / "kbl_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
CROSSWALK = FIXTURES / "season_crosswalk.json"
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

GMKEY = (
    "Game key S{seasonCode}G{gameCode}N{gameNo}, e.g. 'S47G01N49' (the 2025-26 regular season's game 49): "
    "season_code, game_code and game_no of kbl_schedule. Only final games are served; a malformed key is a ValueError."
)
SEASON = (
    "Season END year (2026 = 2025-26; the inaugural 1997 season is 1997, the latest 2027). kbl_runtime maps it to the "
    "KBL seasonCode (47 = 2025-26)."
)
DIVISION = (
    "'kbl' (the KBL itself, the default) or 'd_league' (the KBL D-League reserve league, seasonGrade 2, which has its "
    "own season codes; it starts with the 2009 summer edition)."
)
GAME_CODE = (
    "Competition inside the season, two digits: '01' regular season (default; the first stage of a D-League edition), "
    "'03' playoffs, '04' championship final, '07' EASL, '08' D-League stage 2, '10' All-Star, '11'/'13' cups, "
    "'14' Open Match Day. Only '01' was probed live."
)

# A fixture is (path relative to FIXTURES, the request the runtime would stamp under ``_request``).
Fixture = Tuple[str, Dict[str, Any]]
# short -> (spec path, parser, fixtures, summary, endpoint fields)
Entry = Tuple[str, Callable[..., Any], List[Fixture], str, Dict[str, Any]]

_GAMES = ["S47G01N49", "S37G01N90", "S18G01N4", "S2G01N1", "S11G01N1", "S45G01N95"]


def _p(name: str, typ: str, desc: str) -> Dict[str, Any]:
    return {"name": name, "type": typ, "required": True, "description": desc}


def _q(name: str, wire: str, desc: str, *, typ: str = "str", **extra: Any) -> Dict[str, Any]:
    return {"name": name, "query_key": wire, "type": typ, "description": desc, **extra}


def _game_fx(prefix: str, full: str) -> List[Fixture]:
    """The full body of S47G01N49 plus the six refs captures of a ``/match/{gmkey}...`` route."""
    out: List[Fixture] = [(f"full/{full}__S47G01N49.json", {"gmkey": "S47G01N49"})]
    out += [(f"captures/{prefix}__{k}.json", {"gmkey": k}) for k in _GAMES]
    return out


def _season_ctx(code: int, gc: str = "01", fmt: Optional[str] = None) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        "season": codes.season_end(code),
        "season_code": code,
        "game_code": gc,
        "glkey": f"S{code}G{gc}",
        "division": "kbl",
    }
    if fmt:
        ctx["format"] = fmt
    return ctx


_S45, _S2, _S43 = _season_ctx(45), _season_ctx(2), _season_ctx(43, fmt="old")
_GAME_PATH = "/match/{gmkey}"

#: The wire route behind each public path (the YAML ``path`` is the public one).
SPEC_ROUTE = {
    "/leagues/{season}/stats/players": "/leagues/{glkey}/stats/players",
    "/league/rank/{season}": "/league/rank/{glkey}",
    "/league/rank/{season}/team-tracks": "/league/rank/{glkey}/team-tracks",
    "/league/playoff/{season}": "/league/playoff/{glkey}",  # and /league/playoff/new/{glkey} from seasonCode 45
    "/league/round-mvp/{season}": "/league/round-mvp/{seasonCode}",
}

_SEASON_PARAMS = [
    _q("division", "division", DIVISION, default="kbl", choices=["kbl", "d_league"]),
    _q("game_code", "gameCode", GAME_CODE, default="01"),
]


def _game_fields(example: str = "S47G01N49") -> Dict[str, Any]:
    return {"path_params": [_p("gmkey", "str", GMKEY)], "example_args": {"gmkey": example}}


def _season_fields(*, extra: bool = True, example: int = 2025) -> Dict[str, Any]:
    out: Dict[str, Any] = {"path_params": [_p("season", "int", SEASON)], "example_args": {"season": example}}
    if extra:
        out["extra_params"] = copy.deepcopy(_SEASON_PARAMS)
    return out


ENDPOINTS: Dict[str, Entry] = {
    "seasons": (
        "/season/list",
        P.parse_kbl_seasons,
        [("captures/season__list.json", {})],
        "Every KBL regular season (1997 to the next one) with its competition key and date range, newest first.",
        {"example_args": {}},
    ),
    "teams": (
        "/common/teamList",
        P.parse_kbl_teams,
        [("captures/common__teamList.json", {})],
        "The current ten clubs with Korean and English names and the code of each club's D-League side.",
        {"example_args": {}},
    ),
    "schedule": (
        "/match/list",
        P.parse_kbl_schedule,
        [
            ("captures/match__list__20251101.json", {}),
            ("captures/match__list__19971108.json", {}),
        ],
        "Every game (all competitions, KBL and D-League) of a season or of a date window, with scores and venue.",
        {
            "extra_params": [
                _q(
                    "season",
                    "season",
                    "Season END year (2026 = 2025-26): the window from the first to the last game of that END year's "
                    "KBL and D-League editions, fetched as at most two requests (one per calendar year). Pass this or "
                    "from_date and to_date.",
                    typ="int",
                ),
                _q(
                    "from_date",
                    "fromDate",
                    "First date of a window, YYYYMMDD (KST). Needs to_date; inside one calendar year the API answers "
                    "the whole window in one request (up to 396 games a year, no paging).",
                ),
                _q("to_date", "toDate", "Last date of the window, YYYYMMDD (KST), not before from_date."),
            ],
            "example_args": {"season": 2026},
        },
    ),
    "game": (
        _GAME_PATH,
        P.parse_kbl_game,
        _game_fx("match__gmkey", "match__gmkey"),
        "Game header of a final game: teams, final score, overtimes, venue, attendance, officials and coaches.",
        _game_fields(),
    ),
    "period_scores": (
        _GAME_PATH,
        P.parse_kbl_period_scores,
        _game_fx("match__gmkey", "match__gmkey"),
        "Points by period (quarters and overtimes) and side of a final game, from the game header.",
        _game_fields(),
    ),
    "pbp": (
        "/match/{gmkey}/text-cast",
        P.parse_kbl_pbp,
        _game_fx("match__gmkey__text-cast", "match__gmkey__text-cast"),
        "Play-by-play (the site's text cast) of a final game, one row per event, from 1997-98 on.",
        _game_fields(),
    ),
    "player_box": (
        "/match/{gmkey}/player-stat",
        P.parse_kbl_player_box,
        _game_fx("match__gmkey__player-stat", "match__gmkey__player-stat"),
        "Player box score of a final game with the vendor's advanced rates, one row per player of both teams.",
        _game_fields(),
    ),
    "team_box": (
        "/match/{gmkey}/team-record",
        P.parse_kbl_team_box,
        _game_fx("match__gmkey__team-record", "match__gmkey__team-record"),
        "Team box score of a final game with advanced rates and team-only counts, one row per team.",
        _game_fields(),
    ),
    "shots": (
        "/match/{gmkey}/match-chart",
        P.parse_kbl_shots,
        _game_fx("match__gmkey__match-chart", "match__gmkey__match-chart"),
        "Shot chart of a final game (2006-07 on): every charted shot with x/y pixels on the full-court image.",
        _game_fields(),
    ),
    "score_track": (
        "/match/{gmkey}/match-chart",
        P.parse_kbl_score_track,
        _game_fx("match__gmkey__match-chart", "match__gmkey__match-chart"),
        "Lead tracker and score chart of a final game: home margin and both scores at eight checkpoints per period.",
        _game_fields(),
    ),
    "game_leaders": (
        "/match/{gmkey}/match-chart",
        P.parse_kbl_game_leaders,
        _game_fx("match__gmkey__match-chart", "match__gmkey__match-chart"),
        "Each team's leader per category (points, rebounds, assists, steals, blocks) in a final game.",
        _game_fields(),
    ),
    "player_totals": (
        "/leagues/{season}/stats/players",
        P.parse_kbl_player_totals,
        [
            ("full/leagues__glkey__stats__players__S45G01.json", _S45),
            ("captures/leagues__glkey__stats__players__S45G01.json", _S45),
            ("captures/leagues__glkey__stats__players__S2G01.json", _S2),
        ],
        "Season player totals of one competition (every player with at least one game).",
        _season_fields(),
    ),
    "standings": (
        "/league/rank/{season}",
        P.parse_kbl_standings,
        [
            ("full/league__rank__glkey__S45G01.json", _S45),
            ("captures/league__rank__glkey__S45G01.json", _S45),
            ("captures/league__rank__glkey__S2G01.json", _S2),
        ],
        "Standings of one competition with each team's season totals.",
        _season_fields(),
    ),
    "team_tracks": (
        "/league/rank/{season}/team-tracks",
        P.parse_kbl_team_tracks,
        [
            ("full/league__rank__glkey__team-tracks__S45G01.json", _S45),
            ("captures/league__rank__glkey__team-tracks__S45G01.json", _S45),
        ],
        "Each team's standing after every game of the season, one row per team and game.",
        _season_fields(),
    ),
    "rounds": (
        "/league/rank/{season}/team-tracks",
        P.parse_kbl_rounds,
        [
            ("full/league__rank__glkey__team-tracks__S45G01.json", _S45),
            ("captures/league__rank__glkey__team-tracks__S45G01.json", _S45),
        ],
        "The season's round calendar (the KBL plays six rounds) with each round's first and last date.",
        _season_fields(),
    ),
    "playoff_seeds": (
        "/league/playoff/{season}",
        P.parse_kbl_playoff_seeds,
        [
            ("full/league__playoff__new__glkey__S45G01.json", _season_ctx(45, fmt="new")),
            ("captures/league__playoff__new__glkey__S45G01.json", _season_ctx(45, fmt="new")),
            ("captures/league__playoff__glkey__S43G01.json", _S43),
        ],
        "The regular-season finish that seeds the playoffs (the top six).",
        _season_fields(extra=False),
    ),
    "playoff_series": (
        "/league/playoff/{season}",
        P.parse_kbl_playoff_series,
        [
            ("full/league__playoff__new__glkey__S45G01.json", _season_ctx(45, fmt="new")),
            ("captures/league__playoff__new__glkey__S45G01.json", _season_ctx(45, fmt="new")),
            ("captures/league__playoff__glkey__S43G01.json", _S43),
        ],
        "The playoff series (quarterfinals, semifinals, final): wins and losses of each team in each series.",
        _season_fields(extra=False),
    ),
    "playoff_games": (
        "/league/playoff/{season}",
        P.parse_kbl_playoff_games,
        [
            ("full/league__playoff__new__glkey__S45G01.json", _season_ctx(45, fmt="new")),
            ("captures/league__playoff__new__glkey__S45G01.json", _season_ctx(45, fmt="new")),
            ("captures/league__playoff__glkey__S43G01.json", _S43),
        ],
        "The games of the playoff bracket (2024-25 on; the older bracket format lists no games).",
        _season_fields(extra=False),
    ),
    "round_mvps": (
        "/league/round-mvp/{season}",
        P.parse_kbl_round_mvps,
        [("captures/league__round-mvp__seasonCode__45.json", _S45)],
        "The round MVPs of a season with their totals over the round.",
        _season_fields(extra=False),
    ),
    "current_standings": (
        "/league/rank/team",
        P.parse_kbl_current_standings,
        [("captures/league__rank__team.json", {})],
        "Current standings of the active season (no parameter): record, games behind, streaks, splits.",
        {"example_args": {}},
    ),
    "season_leaders": (
        "/league/season-leader",
        P.parse_kbl_season_leaders,
        [("captures/league__season-leader.json", {})],
        "Current season leaders per category (no parameter): points, rebounds, assists, steals, blocks, shooting.",
        {"example_args": {}},
    ),
    "players": (
        "/players",
        P.parse_kbl_players,
        [("full/players.json", {}), ("captures/players.json", {})],
        "The site's player directory (current and former players) with their current or last team.",
        {"example_args": {}},
    ),
}

# short -> [(returns md section, roots tried in order)]. A column's md row is ``<root><column>`` over the underscored
# wire path (``[].player.pcode`` -> ``player_pcode``).
_BOX = ("/match/{gmkey}/player-stat", ("",))
SECTIONS: Dict[str, List[Tuple[str, Tuple[str, ...]]]] = {
    "seasons": [("/season/list", ("",))],
    "teams": [("/common/teamList", ("",))],
    "schedule": [("/match/list", ("",))],
    "game": [(_GAME_PATH, ("",))],
    "period_scores": [(_GAME_PATH, ("",))],
    "pbp": [("/match/{gmkey}/text-cast", ("",))],
    "player_box": [_BOX],
    "team_box": [("/match/{gmkey}/team-record", ("",))],
    "shots": [("/match/{gmkey}/match-chart", ("shoot_log_",))],
    "score_track": [("/match/{gmkey}/match-chart", ("",))],
    "game_leaders": [("/match/{gmkey}/match-chart", ("",))],
    "player_totals": [("/leagues/{glkey}/stats/players", ("",))],
    "standings": [("/league/rank/{glkey}", ("",))],
    "team_tracks": [("/league/rank/{glkey}/team-tracks", ("tracks_",))],
    "rounds": [("/league/rank/{glkey}/team-tracks", ("rounds_",))],
    "playoff_seeds": [("/league/playoff/new/{glkey}", ("",))],
    "playoff_series": [("/league/playoff/{glkey}", ("six_result_", "four_result_", "final_result_"))],
    "playoff_games": [("/league/playoff/new/{glkey}", ("",))],
    "round_mvps": [("/league/round-mvp/{seasonCode}", ("",))],
    "current_standings": [("/league/rank/team", ("",))],
    "season_leaders": [("/league/season-leader", ("",))],
    "players": [("/players", ("",))],
}

_TEAM = (
    "Team code: the KBL club in that season (two-digit string with a leading zero, e.g. '50' = Changwon LG). Joins "
    "kbl_teams team_code; not stable across ownership changes (the Anyang club is '40' in 1997-98, '70' today)."
)
_BOXNOTE = " Wire key spelling is kept: fg is two-point field goals (dunks included), fgt all field goals."

#: Descriptions of columns the parser derives or renames (apply to every wrapper that has the column).
LOCAL: Dict[str, str] = {
    "gmkey": "Game key S{seasonCode}G{gameCode}N{gameNo} (e.g. S47G01N49): the id of every game route and game frame.",
    "glkey": "Competition key S{seasonCode}G{gameCode} (e.g. S47G01 = 2025-26 regular season): the id of the season routes.",
    "season": "Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS).",
    "season_code": "KBL season code: one sequential number over every season and D-League edition (47 = 2025-26, 48 = the 2025 D-League).",
    "game_code": "Competition inside the season, two digits: 01 regular season, 03 playoffs, 04 final, 07 EASL, 08 D-League stage 2, 10 All-Star, 11/13 cups, 14 Open Match Day.",
    "game_no": "Game number inside the competition (glkey): the N part of the gmkey.",
    "team_code": _TEAM,
    "side": "Which side of the game the row belongs to: home or away.",
    "period": "Period number: 1-4 for the quarters, 5, 6, ... for the overtimes (the wire says X1 in the play-by-play, EQ1 in the match chart).",
    "period_label": "Period label: Q1-Q4, then OT1, OT2, ... (normalised from the wire's X1 / EQ1 / eq1).",
    "score": "Points scored by the side in that period.",
    "action_number": "1-based position of the event in the feed, which is in game order (the wire's n is 0 on every row of some games).",
    "clock_seconds_left": "Seconds left in the period (minutes_left * 60 + seconds_left); quarters are 10 minutes, overtimes 5.",
    "action": "English meaning of action_code (kbl_codes.ACTIONS), translated from the site's Korean label; null for an unknown code.",
    "action_ko": "The site's own Korean label of action_code (e.g. 2점슛성공 = two-point field goal made).",
    "points": "Points the event scored: 2 for a made two-point shot or dunk (201, 207), 3 for a made three (205), 1 for a made free throw (203); null otherwise.",
    "substitution": "English meaning of substitution_code on substitution-out rows (e.g. 106_1 = fouled out, five fouls); null otherwise.",
    "foul": "English meaning of foul_code (e.g. TCF = technical foul). No sampled row carries a foul code, so this is unverified.",
    "minutes_played": "Playing time in minutes (play_min + play_sec / 60), derived.",
    "made": "True when the shot went in (wire o = 'O'), False when it missed ('X').",
    "checkpoint": "1-based checkpoint inside the period: eight per period, evenly spaced is assumed (every 75 s of a quarter; unverified), the last at the period's end.",
    "home_margin": "Home margin (home minus away points) at the checkpoint.",
    "home_score": "Home team points: cumulative at the checkpoint on the score track, the final total (all periods summed) on a game row, the full-time score in the schedule.",
    "away_score": "Away team points: cumulative at the checkpoint on the score track, the final total (all periods summed) on a game row, the full-time score in the schedule.",
    "overtimes": "Number of overtime periods played (0 for a game decided in regulation).",
    "referees_sub": "JSON list of the umpires' names (officials appear by name only).",
    "home_coaches": "JSON list of the home assistant coaches' names (staff appear by name only).",
    "away_coaches": "JSON list of the away assistant coaches' names (staff appear by name only).",
    "game_number": "1-based game number in the team's season: the n-th entry of the team's rank track.",
    "rank": "Standing (1 = first): after the game_number-th game on the rank track, the current rank on the standings routes.",
    "bracket_format": "Playoff bracket format: old (seasons through 2023-24, per-team series results) or new (2024-25 on, bracket numbers and games).",
    "round": "Playoff round: quarterfinal (6강, seeds 3-6), semifinal (4강) or final; on the round-MVP and rounds frames the round number of the season.",
    "series_no": "Series number inside the round (1 or 2 for the quarterfinals and semifinals, 1 for the final).",
    "opponent_code": "Team code of the series opponent; null when the opponent cannot be told from the body.",
    "seed": "Regular-season finish that seeded the team (1-6), from the bracket's seed table; null for a team outside it.",
    "total_seconds": "Playing time over the round in seconds (totalHour * 3600 + totalMinute * 60 + totalSecond; only the seconds part is non-zero in the samples).",
    "last_five": "The last five results as a comma-separated string, most recent first: 1 win, 0 loss.",
    "part_label": "English meaning of part (kbl_codes.LEADER_PARTS), e.g. score = points per game.",
    "wins": "Wins: series wins on a playoff row, season wins on a standings row.",
    "losses": "Losses: series losses on a playoff row, season losses on a standings row.",
    "draws": "Draws (always 0 in basketball).",
    "game_date": "Game date (KST).",
    "home_win": "True when the home team won the game.",
    "crowds": "Attendance of the game (spectators).",
    "foul_code": "Foul type code (the site reads an f field; no sampled row carries it): PNF personal, TCF technical, FRF unsportsmanlike, FTF fighting, PCF punching, EBF elbow.",
    "game_count": "Games played (this season or competition).",
    "home_wins": "Wins in the team's home games.",
    "home_losses": "Losses in the team's home games.",
    "away_wins": "Wins in the team's away games.",
    "away_losses": "Losses in the team's away games.",
    "ast": "Assists (action 211)." + _BOXNOTE,
    "defr": "Defensive rebounds (action 210). Team season total on the standings route.",
    "offr": "Offensive rebounds (action 209). Team season total on the standings route.",
    "to": "Turnovers (action 214). Team season total on the standings route.",
    "stl": "Steals (action 212). Team season total on the standings route.",
    "bs": "Blocked shots (action 213). Team season total on the standings route.",
    "team_r": "Team rebounds (action 218), unverified against the box score's team_r. Team season total on the standings route.",
    "pscore": "Points in the paint (2 * pp). Team season total on the standings route.",
    "foul_total": "Personal fouls over the season (the standings route's foulTot).",
    "player_name": "Player name in Korean (foreign players in Hangul transliteration).",
    "player_name_en": "Player name in English / romanized (sometimes Hangul for former players, e.g. 'CJ 레슬리').",
    "team_name": "Team short name (Korean, e.g. '창원 LG' = Changwon LG), as it was at the time.",
    "team_name_full": "Team full name (Korean, e.g. '창원 LG 세이커스' = Changwon LG Sakers).",
    "team_name_en": "Team short name in English.",
    "back_num": "Jersey number (a string).",
    "pos": "Position code: C center, FD forward, GD guard, FC forward-center, GF guard-forward (AA, AB, AC, AD, XX on former players; unverified).",
    "player_flag": "Player type: '0' domestic, '1' foreign import, '2' Asian-quota player.",
    "pcode": "KBL player code (six-digit string), the same on every route; the play-by-play carries none.",
    "last_career": "Code from the directory's playerLastCareerIf: seen values are season codes ('25', '27') and 'A'; meaning unverified.",
    "started": "1 when the player started the game, else 0.",
    "home_away": "Side: '1' home, '2' away; '3' on both teams' rows in the 1997-98 box scores, so join on team_code instead.",
    "team_code_d": "Team code of the club's D-League (reserve) side, in the same team-code space as team_code.",
    "start_date": "First game date of the season (KST).",
    "end_date": "Last game date of the season (KST).",
    "from_date": "First date of the round (KST).",
    "to_date": "Last date of the round (KST).",
    "updated_at": "When the standings row was written (ISO 8601 UTC timestamp).",
    "last_game_date": "Date (KST) of the team's latest game in the standings.",
    "conti_date": "Date (KST) the current streak started (inferred; unverified).",
}

#: Route-specific descriptions (keyed by wrapper short) that replace a returns-md row which is side-specific or only
#: restates the column.
ROUTE: Dict[str, Dict[str, str]] = {
    "game_leaders": {
        "side": "Side of the leader: home or away.",
        "pcode": "KBL player code (six-digit string) of the team's leader in the category.",
        "player_name": "Leader's name in Korean (foreign players in Hangul transliteration).",
        "player_name_en": "Leader's name in English / romanized.",
        "team_name": "The leader's team short name (Korean).",
        "value": "The leader's total in the category in this game.",
        "play_min": "Minutes the leader played this game; add play_sec seconds.",
        "play_sec": "Seconds part of the leader's playing time (0-59).",
        "part": "Category code: score points, rb rebounds, ast assists, stl steals, bs blocks.",
    },
    "shots": {
        "x": "Shot location x, pixels on the site's 726 x 412 full-court image (origin top-left, x along the court, right); see basket.",
        "y": "Shot location y, pixels on the 726 x 412 full-court image (origin top-left, y down).",
        "basket": "Target basket: '1' the right-hand one (x > 363), '2' the left-hand one; teams switch baskets at half-time.",
        "pcode": "KBL player code (six-digit string) of the shooter.",
        "player_name": "Shooter's name in Korean.",
        "player_name_en": "Shooter's name in English / romanized.",
    },
    "player_totals": {"pcode": "KBL player code (six-digit string)."},
    "current_standings": {
        "wins": "Wins (a count on this route; on /league/rank/{glkey} win is a boolean about the latest game).",
        "losses": "Losses (a count on this route).",
        "draws": "Draws (always 0 in basketball).",
        "rank": "Standing (1 = first).",
    },
    "standings": {
        "rank": "Standing (1 = first).",
        "wins": "Season wins of the team (the wire's TWin).",
        "losses": "Season losses of the team (the wire's TLoss).",
        "draws": "Draws (TDraw, always 0).",
        "last_game_won": "True when the team's latest game was a win (the wire's win; a boolean, not a count).",
        "last_game_lost": "True when the team's latest game was a loss (inferred from conti_loss).",
        "last_game_drawn": "True when the team's latest game was a draw (always false).",
        "game_flag": "Boolean flag in the standings row; meaning unverified (true in the sample).",
        "foul_total": "Personal fouls over the season (foulTot).",
        "team_code": _TEAM,
    },
    "playoff_series": {
        "wins": "Wins of the team in the series.",
        "losses": "Losses of the team in the series.",
        "game_date": "Date of a game of the series, KST (which one is unverified); old bracket format only.",
        "series_no": "Series number inside the round (the wire's gameHoi, '1' or '2'; 1 for the final).",
    },
    "playoff_games": {
        "season_code": "KBL season code of the game (from the bracket's gmkey object).",
        "game_code": "Competition type of the game: 03 playoffs, 04 championship final.",
        "game_no": "Game number inside the competition, from the bracket's gmkey object.",
        "game_date": "Game date, KST.",
        "home_team_code": "Home team code. " + _TEAM,
        "away_team_code": "Away team code. " + _TEAM,
        "home_team_name": "Home team short name (Korean).",
        "away_team_name": "Away team short name (Korean).",
        "home_score": "Home team points.",
        "away_score": "Away team points.",
    },
    "playoff_seeds": {"seed": "Final regular-season standing, i.e. the playoff seed (1-6)."},
    "round_mvps": {
        "pcode": "KBL player code (six-digit string) of the round MVP (the wire's playerNo).",
        "player_name": "Round MVP's name in Korean (kname).",
        "round": "Round number of the season (the KBL plays six rounds).",
    },
    "season_leaders": {
        "part": "Leader category code: score, rb, ast, bs, stl, fgt, threep, ftR, fgtR, threepR or dk.",
        "rank": "Rank in the category.",
        "value": "The player's value in the category: a per-game average, or a percentage for the *R categories.",
        "game_count": "Games played this season.",
    },
    "schedule": {
        "game_time": "Duration of the game in minutes (a numeric string on the wire; null on some rows).",
        "season_name": "Season label as sent: 'YYYY-YYYY' for a KBL season, '1997' for the inaugural one, a D-League edition label, or an empty string on EASL rows.",
        "game_date": "Game date (KST).",
        "home_score": "Home team points (scoreH).",
        "away_score": "Away team points (scoreA).",
        "team_code": _TEAM,
    },
    "game": {
        "game_time": "Duration of the game in minutes.",
        "home_score": "Home team final points, summed from the period scores (the header carries no totals).",
        "away_score": "Away team final points, summed from the period scores (the header carries no totals).",
        "home_manager": "Home head coach's name (officials and coaches appear by name only).",
        "away_manager": "Away head coach's name (officials and coaches appear by name only).",
        "referee_main": "Crew chief's name (officials appear by name only).",
        "game_date": "Game date (KST).",
    },
    "period_scores": {"score": "Points the side scored in the period."},
    "seasons": {
        "season_name": "Season label: 'YYYY-YYYY' for a KBL season (e.g. '2025-2026'), '1997' for the inaugural season.",
        "game_code": "Competition type within the season: 01 regular season (the only value the list returns).",
        "season": "Season END year (2026 = 2025-26), derived from season_code through the crosswalk (kbl_codes.SEASONS).",
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


def _filler(name: str, desc: str) -> bool:
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


def _describe(short: str, col: str, wire: str, md: Dict[str, Dict[str, str]]) -> str:
    """ROUTE, then the route's md row for the column's wire key, then LOCAL."""
    route = ROUTE.get(short, {})
    if col in route:
        return route[col]
    if wire:
        key = "_".join(underscore(s) for s in wire.split(".") if s)
        for section, roots in SECTIONS[short]:
            for root in roots:
                hit = md.get(section, {}).get(root + key)
                if hit and not _filler(col, hit):
                    return hit
    return LOCAL.get(col, "")


_TYPE = {pl.String: "character", pl.Int64: "integer", pl.Float64: "numeric", pl.Boolean: "logical", pl.Date: "date"}


def load_fixture(rel: str, ctx: Dict[str, Any]) -> Any:
    """A fixture body as the runtime hands it to a parser: the body plus the ``_request`` stamp."""
    data = json.loads((FIXTURES / rel).read_text(encoding="utf-8"))
    out: Dict[str, Any] = dict(data) if isinstance(data, dict) else {"items": data}
    out["_request"] = ctx
    return out


def _check_frames(short: str, entry: Entry) -> None:
    """Run the parser over every fixture: the frame must carry the documented schema exactly."""
    spec = {c: d for c, _w, d in P.SPECS[entry[1].__name__]}
    rows = 0
    for rel, ctx in entry[2]:
        df = entry[1](load_fixture(rel, ctx))
        if dict(df.schema) != spec:
            raise SystemExit(f"kbl: {short}: {rel} parses to {dict(df.schema)}, not the documented schema")
        rows += df.height
    if rows == 0:  # a pre-2006-07 shot chart is legitimately empty; some fixture must have rows
        raise SystemExit(f"kbl: {short}: no fixture parses to a row")


def _snake_path(path: str) -> str:
    return re.sub(r"\{([^}]+)\}", lambda m: "{" + underscore(m.group(1)) + "}", path)


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    spec_path, parser, _fixtures, summary, fields = entry
    fields = copy.deepcopy(fields)
    tokens = re.findall(r"\{([^}]+)\}", spec_path)
    assert len(fields.get("path_params", [])) == len(tokens), short
    return {
        "short": short,
        "summary": summary,
        "path": _snake_path(spec_path),
        "parser": parser.__name__,
        "returns_schema": f"native/kbl/{short}",
        **fields,
    }


def _crosswalk(readme: Path) -> List[List[Any]]:
    text = readme.read_text(encoding="utf-8")
    block = text[text.index("<!-- crosswalk:start") : text.index("<!-- crosswalk:end")]
    rows = []
    for line in block.splitlines():
        m = re.match(r"^\| (\d+) \| `([^`]*)`.*? \| (\d) \| (\d{8}) \| (\d{8}) \| .*? \| (\d{4}) \|$", line)
        if m:
            rows.append([int(m[1]), m[2], int(m[3]), m[4], m[5], int(m[6])])
    return rows


NOTES = [
    "Unofficial and unsupported: these wrappers read api.kbl.or.kr, the data API behind the www.kbl.or.kr React site "
    "(stats vendor sports2i). No key, cookie or proxy is needed, but two request headers must be present (Channel and "
    "TeamCode; the runtime sends WEB and XX). www.kbl.or.kr/robots.txt returns the SPA's HTML and "
    "api.kbl.or.kr/robots.txt a 500 error envelope, so no policy or Crawl-delay is published (checked 2026-10-12); the "
    "site's terms (/terms/list) were not reviewed for data reuse. The runtime waits at least 1 s before every request, "
    "retries included. Payloads are not redistributed as release assets.",
    "Not wrapped: the site's /record/* and /player/player/{pcode} pages are rendered by the sports2i widget from two "
    "other hosts (kbl-api.sports2i.com and api-stats.kbl.or.kr: team four factors, clutch, hustle, shot dashboards, "
    "all-time leaders) and need their own intake. WKBL (the women's league) is out of scope by owner ruling.",
    "Seasons are END years (2026 = 2025-26). KBL numbers seasons AND D-League (reserve league) editions with one "
    "sequential seasonCode; division picks 'kbl' or 'd_league' and game_code the competition inside the season, and "
    "kbl_runtime sends the API's glkey. The playoff routes take the REGULAR-SEASON key; the old route serves seasonCode "
    "below 45 and the new one 45 on, and the wrapper picks it.",
    "Ids: gmkey S{seasonCode}G{gameCode}N{gameNo} (a string, an object only inside the new playoff bracket), team_code "
    "(two-digit string with a leading zero, not stable across ownership changes) and pcode (six-digit string, the same on "
    "every route). The play-by-play carries no pcode: join its player_name to the game's box score on name and "
    "team_code. isEnded is unreliable (0 on every finished game through 2005-06), so a game is final when isEnded is 1 "
    "or when it is dated before today (KST), both teams scored and its play-by-play reached the game-end row; "
    "anything else raises AssetFetchError. The first game call also fetches the header (and, for such a game, the "
    "play-by-play) once; the answer is cached for the process.",
    "Coverage: box scores and play-by-play from the first season (1997); shot x/y from 2006-07 (an earlier game's shot "
    "chart is empty: NoDataError); advanced rates are filled from 1997-98 on (the vendor computes them from the box); "
    "screen assists and deflections are 0 before the recent seasons. fg is TWO-point field goals made (dunks included), "
    "fgt all field goals, and a made dunk is its own row in the play-by-play. Shot x/y are pixels on the site's 726 x 412 "
    "full-court image; basket names the target end and teams switch at half-time, so normalise with basket before "
    "stacking shots.",
]


def main() -> None:
    refs = refs_dir("kbl", SPEC)
    spec = load_spec(refs, SPEC)
    cross = _crosswalk(refs / "README.md")
    if [tuple(r) for r in cross] != [tuple(r) for r in codes.SEASONS]:
        raise SystemExit("kbl: kbl_codes.SEASONS differs from the refs README crosswalk; update kbl_codes.py")
    routes = {SPEC_ROUTE.get(e[0], e[0]) for e in ENDPOINTS.values()} | {"/league/playoff/new/{glkey}"}
    mapped = {re.sub(r"\{[^}]+\}", "{}", r) for r in routes}
    wanted = {re.sub(r"\{[^}]+\}", "{}", r) for r in spec["paths"]}
    if mapped != wanted:
        raise SystemExit(f"kbl: spec paths and ENDPOINTS differ: {sorted(mapped ^ wanted)}")
    for short, entry in ENDPOINTS.items():
        names = set(re.findall(r"\{([^}]+)\}", entry[0]))
        assert not {"league", "sport"} & names, short  # the renderer substitutes those as ESPN slugs
        for rel, _ctx in entry[2]:
            assert (FIXTURES / rel).exists(), f"{short}: missing fixture {rel}"
        _check_frames(short, entry)
    doc = {
        "api": "kbl",
        "host": HOST,
        "name_pattern": "kbl_{short}",
        "module": "kbl",
        "parser_module": "basketball.kbl.kbl_parsers",
        "getter_module": "sportsdataverse.basketball.kbl.kbl_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": False,
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "notes": NOTES,
            "raises": [
                "sportsdataverse.errors.NoDataError: the API has nothing for the request (an empty list, a final game "
                "with no box score or shot chart, no playoff bracket, no schedule rows in a window).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is unknown or partial: any "
                "non-2xx (an unknown id is the API's HTTP 500), the KBL error envelope, an empty or non-JSON body, a "
                "game that is not final, or a body for another game, season or window than the one asked for.",
                "ValueError: a malformed gmkey, a season that is not an END year between 1997 and 2027, an unknown "
                "division or game_code, or bad schedule dates (raised before any request).",
            ],
            "see_also": [
                {
                    "name": "Korean Basketball League",
                    "url": "https://www.kbl.or.kr",
                    "note": "the league site whose API these wrappers read",
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
    write_yaml(ROOT / "tools/codegen/endpoints/kbl.yaml", doc)

    md = _md_sections(refs / "kbl-returns.md")
    unknown = {sec for secs in SECTIONS.values() for sec, _ in secs} - set(md)
    if unknown or set(SECTIONS) != set(ENDPOINTS):
        raise SystemExit(f"kbl: SECTIONS out of step with the returns md / ENDPOINTS: {sorted(unknown)}")
    schema_dir = ROOT / "tools/codegen/schemas/native/kbl"
    rewrite_schema_dir(schema_dir)
    snippet: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        key = f"{SCHEMA_PREFIX}{short}"
        snippet[key] = {}
        cols = []
        for col, wire, dtype in P.SPECS[entry[1].__name__]:
            desc = _describe(short, col, wire, md)
            if _filler(col, desc):
                bad.append(f"{key}.{col}: {desc!r}")
            snippet[key][col] = desc
            # descriptions live in manual_column_descriptions.yaml (via the snippet), never here
            cols.append({"name": col, "type": _TYPE[dtype], "description": ""})
        write_yaml(schema_dir / f"{short}.yaml", {"schema": key, "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit("kbl: filler or missing descriptions:\n" + "\n".join(bad))
    write_yaml(SNIPPET, snippet)
    CROSSWALK.write_text(json.dumps(cross, indent=1, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    SPEC_PATHS.write_text(
        json.dumps(
            {
                "paths": sorted(spec["paths"]),
                "wrappers": {s: SPEC_ROUTE.get(e[0], e[0]) for s, e in ENDPOINTS.items()},
            },
            indent=1,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"kbl: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
