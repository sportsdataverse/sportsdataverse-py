"""Generate the ``legabasket`` flat-API endpoint YAML + returns-schemas (Lega Basket Serie A, ``legabasket.it/api``).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_liiga.py`` with the shared plumbing
in ``gen_soccer_common.py``. Reads the frozen spec and the returns md from the ``sdv-internal-refs`` checkout
(``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and emits:

* ``tools/codegen/endpoints/legabasket.yaml`` -- 19 wrappers over the spec's 16 paths. One route can feed several
  frames (the box score route feeds ``lba_box_score`` and ``lba_team_box_score``, the play-by-play route feeds
  ``lba_pbp`` and ``lba_pbp_legend``, the roster route two frames); :data:`ENDPOINTS` is the source of truth.
* ``tools/codegen/schemas/native/legabasket/<short>.yaml`` -- one returns-schema per wrapper: the columns of its
  parser run over its committed fixtures (the FULL bodies in ``tests/fixtures/legabasket/full/`` where there is one,
  plus every season's refs capture in ``tests/fixtures/legabasket/captures/``). Schema key ``lba_<short>``
  (``schema_prefix``); every ``description`` is ``''``.
* ``tools/codegen/notes/legabasket_descriptions.yaml`` -- the column descriptions (committed: the generator owns the
  text), keyed ``lba_<short>`` -> column, for ``manual_column_descriptions.yaml``. Each comes from :data:`ROUTE` (route-specific), :data:`LOCAL`
  (parser-built columns), else the route's own ``legabasket-returns.md`` row (:data:`SECTIONS`). A filler or missing
  description stops the run.
* ``sportsdataverse/basketball/lba/lba_dtypes.py`` -- each parser's column dtypes (the returns-schema types), so an
  empty or all-null frame is typed like a populated one.
* ``tests/fixtures/legabasket/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs checkout).

Run: ``python tools/codegen/gen_legabasket.py``
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.lba import lba_parsers as P
from sportsdataverse.dl_utils import underscore

HOST = "https://www.legabasket.it/api"
SPEC = "legabasket.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "legabasket"
SCHEMA_PREFIX = "lba_"
SNIPPET = ROOT / "tools" / "codegen" / "notes" / "legabasket_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
DTYPES_MODULE = ROOT / "sportsdataverse" / "basketball" / "lba" / "lba_dtypes.py"
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

SEASON_Q = "Season END year (2027 = 2026-27). Omitted = the site's default season (unverified)."
GAME_ID = "Game id (game_id in lba_calendar), e.g. 25602. Global across seasons."
TEAM_ID = "Team id (team_id in lba_teams), e.g. 1764. One team-season: ids are re-issued every season."
PLAYER_ID = "Player id (player_id in lba_players or a box score), e.g. 969. Global: one id for a whole career."

# short -> (spec path, parser, fixtures, summary, endpoint fields). A fixture is relative to FIXTURES; ``full/`` ones
# are the untruncated bodies, ``captures/<season>/`` the refs captures (3 rows per array).
Entry = Tuple[str, Callable[..., Any], List[str], str, Dict[str, Any]]


def _q(name: str, wire: str, desc: str, *, typ: str = "str", required: bool = False, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": desc}
    if required:
        out["required"] = True
    out.update(extra)
    return out


def _cap(route: str, seasons: Tuple[str, ...] = ("2026", "2017", "1995")) -> List[str]:
    slug = route.strip("/").replace("/", "__")
    return [f"captures/{s}/{slug}.json" for s in seasons if (FIXTURES / "captures" / s / f"{slug}.json").exists()]


def _season_q(desc: str = SEASON_Q, wire: str = "s") -> Dict[str, Any]:
    return _q("season", wire, desc, typ="int")


_GAME_Q = [_q("game_id", "id", GAME_ID, typ="int", required=True)]
_GAME_EX = {"game_id": 25602}
_R_CHAMP = "/championships/get-championships"
_R_CAL = "/championships/get-championships-calendar-by-id"
_R_GAME = "/championships/get-championships-matches-by-id"
_R_SCORES = "/championships/get-championships-matches-scores-by-id"
_R_PBP = "/championships/get-championships-matches-play-by-play"
_R_SHOTS = "/matches/get-championship-matches-shots-by-id"
_R_RANK = "/championships/get-championship-ranking"
_R_TEAMS = "/teams/get-teams"
_R_ROSTER = "/teams/get-team-roster"
_R_TPS = "/teams/get-team-players-stats"
_R_PLAYERS = "/players/get-players"
_R_PLAYER = "/players/get-player-by-id"
_R_PSTATS = "/players/get-player-stats"
_R_PMATCH = "/players/get-player-matches"
_R_PLB = "/statistics/get-players-statistics"
_R_TLB = "/statistics/get-teams-statistics-by-type"

ENDPOINTS: Dict[str, Entry] = {
    "championships": (
        _R_CHAMP,
        P.parse_lba_championships,
        ["full/championships__s2026.json", *_cap(_R_CHAMP)],
        "Championships of a season, one per phase (Regular Season, Play Off); every season when none is given.",
        {
            "extra_params": [
                _season_q("Season END year (2027 = 2026-27). Omitted = every season (310 championships).")
            ],
            "fixed_params": {"cs_id": 1, "items": 1000},
            "example_args": {"season": 2027},
        },
    ),
    "calendar": (
        _R_CAL,
        P.parse_lba_calendar,
        ["full/calendar__602__d1.json", *_cap(_R_CAL)],
        "Games of one round of a championship: teams, score, venue, referees and broadcasters.",
        {
            "extra_params": [
                _q(
                    "championship_id",
                    "id",
                    "Championship id (championship_id in lba_championships): one season AND one phase.",
                    typ="int",
                    required=True,
                ),
                _q(
                    "round",
                    "d",
                    "Round number (day_serial, 1, 2, ...). Omitted = the current round (the last played for a "
                    "finished championship).",
                    typ="int",
                ),
            ],
            "example_args": {"championship_id": 602, "round": 1},
        },
    ),
    "game": (
        _R_GAME,
        P.parse_lba_game,
        ["full/matches__25602.json", *_cap(_R_GAME)],
        "Game header: teams, venue, quarter scores, coaches, referees and broadcasters.",
        {"extra_params": _GAME_Q, "fixed_params": {"sc": 1}, "example_args": _GAME_EX},
    ),
    "box_score": (
        _R_SCORES,
        P.parse_lba_box_score,
        ["full/matches__25602.json", *_cap(_R_SCORES), *_cap(_R_GAME)],
        "Box score of a final game: one row per player of both teams (Italian-named stat columns).",
        {"extra_params": _GAME_Q, "fixed_params": {"info": "true"}, "example_args": _GAME_EX},
    ),
    "team_box_score": (
        _R_SCORES,
        P.parse_lba_team_box_score,
        ["full/matches__25602.json", *_cap(_R_SCORES), *_cap(_R_GAME)],
        "Team lines of a final game's box score: the team-booked stats and the totals of each side.",
        {"extra_params": _GAME_Q, "fixed_params": {"info": "true"}, "example_args": _GAME_EX},
    ),
    "pbp": (
        _R_PBP,
        P.parse_lba_pbp,
        ["full/pbp__25602.json", *_cap(_R_PBP)],
        "Play-by-play of a final game (2017-18 on), one row per action with x/y; coordinate frame by era.",
        {
            "extra_params": _GAME_Q,
            "fixed_params": {"info": "true", "sort": "asc"},
            "example_args": _GAME_EX,
        },
    ),
    "pbp_legend": (
        _R_PBP,
        P.parse_lba_pbp_legend,
        ["full/pbp__25602.json", *_cap(_R_PBP)],
        "The play-by-play qualifier code table (59 codes) with its Italian labels.",
        {
            "extra_params": _GAME_Q,
            "fixed_params": {"info": "true", "sort": "asc"},
            "example_args": _GAME_EX,
        },
    ),
    "shots": (
        _R_SHOTS,
        P.parse_lba_shots,
        ["captures/2026/matches__get-championship-matches-shots-by-id.json", *_cap(_R_SHOTS)],
        "Shooting by court zone per team for a final game (made / attempted aggregates; zone 7 includes free throws).",
        {"extra_params": _GAME_Q, "fixed_params": {"info": 1}, "example_args": _GAME_EX},
    ),
    "standings": (
        _R_RANK,
        P.parse_lba_standings,
        ["full/ranking__602__d3.json", *_cap(_R_RANK)],
        "Standings of a championship after a round, one row per team.",
        {
            "extra_params": [
                _q(
                    "championship_id",
                    "id",
                    "Championship id (championship_id in lba_championships; a Regular Season id carries standings).",
                    typ="int",
                    required=True,
                ),
                _q(
                    "round",
                    "d",
                    "Standings after this round. Required in practice for a finished championship: without it the "
                    "site sends none (NoDataError).",
                    typ="int",
                ),
            ],
            "example_args": {"championship_id": 602, "round": 3},
        },
    ),
    "teams": (
        _R_TEAMS,
        P.parse_lba_teams,
        ["full/teams__2026.json", *_cap(_R_TEAMS)],
        "Teams of a season, one row per team-season with the stable club id.",
        {
            "extra_params": [_season_q(wire="year")],
            "fixed_params": {"items": 50},
            "example_args": {"season": 2027},
        },
    ),
    "roster": (
        _R_ROSTER,
        P.parse_lba_roster,
        _cap(_R_ROSTER),
        "Players of one team-season with bio, number and role.",
        {"extra_params": [_q("team_id", "id", TEAM_ID, typ="int", required=True)], "example_args": {"team_id": 1764}},
    ),
    "roster_coach": (
        _R_ROSTER,
        P.parse_lba_roster_coach,
        _cap(_R_ROSTER),
        "Head coach of one team-season.",
        {"extra_params": [_q("team_id", "id", TEAM_ID, typ="int", required=True)], "example_args": {"team_id": 1764}},
    ),
    "team_players_stats": (
        _R_TPS,
        P.parse_lba_team_players_stats,
        ["full/team_players_stats__1764.json", *_cap(_R_TPS)],
        "Season totals of each player of a team-season.",
        {
            "extra_params": [
                _q("team_id", "id", TEAM_ID, typ="int", required=True),
                _season_q(
                    "Season END year (2027 = 2026-27) of the team id. Omitted = the site's default (unverified)."
                ),
            ],
            "fixed_params": {"cs_id": 1},
            "example_args": {"team_id": 1764, "season": 2027},
        },
    ),
    "players": (
        _R_PLAYERS,
        P.parse_lba_players,
        _cap(_R_PLAYERS),
        "Players of a season, 50 per page (walk the pages with page).",
        {
            "extra_params": [
                _season_q(wire="year"),
                _q("page", "page", "Page number, 1-based. Omitted = the first page.", typ="int"),
            ],
            "fixed_params": {"items": 50},
            "example_args": {"season": 2027},
        },
    ),
    "player": (
        _R_PLAYER,
        P.parse_lba_player,
        _cap(_R_PLAYER),
        "Player bio with current-season totals, highs and averages (one row).",
        {
            "extra_params": [_q("player_id", "id", PLAYER_ID, typ="int", required=True)],
            "fixed_params": {"stats": 1},
            "example_args": {"player_id": 969},
        },
    ),
    "player_stats": (
        _R_PSTATS,
        P.parse_lba_player_stats,
        _cap(_R_PSTATS),
        "A player's career by championship: per-game averages, single-game highs and totals.",
        {
            "extra_params": [_q("player_id", "id", PLAYER_ID, typ="int", required=True)],
            "example_args": {"player_id": 969},
        },
    ),
    "game_log": (
        _R_PMATCH,
        P.parse_lba_game_log,
        ["full/player_matches__969.json", *_cap(_R_PMATCH)],
        "A player's game log for one season, one row per game.",
        {
            "extra_params": [
                _q("player_id", "id", PLAYER_ID, typ="int", required=True),
                _season_q(),
            ],
            "fixed_params": {"cs_id": 1},
            "example_args": {"player_id": 969, "season": 2027},
        },
    ),
    "players_leaderboard": (
        _R_PLB,
        P.parse_lba_players_leaderboard,
        ["full/players_statistics__points.json", *_cap(_R_PLB)],
        "Season player leaderboard for one category (per-game average).",
        {
            "extra_params": [
                _season_q(),
                _q(
                    "category",
                    "cat",
                    "Category: points, rating_lega, total_rebound, assists, shots_3p_perc, ... (the site's "
                    "filters.categories).",
                    default="points",
                ),
            ],
            "fixed_params": {"cs_id": 1},
            "example_args": {"season": 2027, "category": "points"},
        },
    ),
    "teams_leaderboard": (
        _R_TLB,
        P.parse_lba_teams_leaderboard,
        ["full/teams_statistics__offensive.json", *_cap(_R_TLB)],
        "Season team leaderboards, offensive or defensive, one row per team and category.",
        {
            "extra_params": [
                _q(
                    "board",
                    "type",
                    "Board: teams_offensive or teams_defensive.",
                    required=True,
                    choices=["teams_offensive", "teams_defensive"],
                ),
                _season_q(),
            ],
            "fixed_params": {"cs_id": 1},
            "example_args": {"board": "teams_offensive", "season": 2027},
        },
    ),
}

# short -> (returns md section, roots tried in order). A column's md row is ``<root><column>`` over the underscored
# wire path (``scores.ht.rows.[].pun`` -> ``scores_ht_rows_pun``).
SECTIONS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "championships": (_R_CHAMP, ("competitions_",)),
    "calendar": (_R_CAL, ("matches_",)),
    "game": (_R_GAME, ("match_",)),
    "box_score": (_R_SCORES, ("scores_ht_rows_", "scores_vt_rows_")),
    "team_box_score": (_R_SCORES, ("scores_ht_totals_", "scores_ht_team_", "scores_vt_totals_")),
    "pbp": (_R_PBP, ("pbp_actions_",)),
    "pbp_legend": (_R_PBP, ("legend_",)),
    "shots": (_R_SHOTS, ("shots_ht_zone_", "shots_ht_")),
    "standings": (_R_RANK, ("standings_",)),
    "teams": (_R_TEAMS, ("teams_",)),
    "roster": (_R_ROSTER, ("players_", "team_")),
    "roster_coach": (_R_ROSTER, ("coach_", "team_")),
    "team_players_stats": (_R_TPS, ("players_", "team_")),
    "players": (_R_PLAYERS, ("players_",)),
    "player": (_R_PLAYER, ("player_", "")),
    "player_stats": (_R_PSTATS, ("stats_data_avg_", "stats_data_sum_", "stats_data_max_")),
    "game_log": (_R_PMATCH, ("matches_stats_data_",)),
    "players_leaderboard": (_R_PLB, ("stats_",)),
    "teams_leaderboard": (_R_TLB, ("stats_",)),
}
#: The wire ``id`` of each short, renamed by the parser: the md row is ``<root>id``.
ID_ALIAS: Dict[str, Tuple[str, str]] = {
    "championships": ("championship_id", "id"),
    "calendar": ("game_id", "id"),
    "game": ("game_id", "id"),
    "standings": ("standing_id", "id"),
    "teams": ("team_id", "id"),
    "roster": ("player_id", "id"),
    "roster_coach": ("coach_id", "id"),
    "players": ("player_id", "id"),
    "player": ("player_id", "id"),
    "game_log": ("stat_line_id", "id"),
}

_CTX_SEASON = "Season END year (2027 = 2026-27): the row's START year + 1."
_CTX_GAME = "Game id (global across seasons); joins lba_calendar and lba_game."
_CTX_TEAM = "Team-season id (re-issued every season); join lba_teams for the stable club_id."
_CTX_SIDE = "Which team of the game the row belongs to: home or visitor."

#: Descriptions of parser-built columns, by column name (apply to every wrapper that has the column).
LOCAL: Dict[str, str] = {
    "season": _CTX_SEASON,
    "game_id": _CTX_GAME,
    "side": _CTX_SIDE,
    "referees": "JSON list of the game's referees (id, name, surname); null when the site lists none.",
    "broadcasters": "JSON list of the game's broadcasters (name, logo key); null when the site lists none.",
    "row_type": "team = stats booked to the team itself (team rebounds, team turnovers); totals = the whole side.",
    "home_team_id": "Home team-season id of the game (the play-by-play ht_id).",
    "visitor_team_id": "Visitor team-season id of the game (the play-by-play vt_id).",
    "team_side": "Side of the acting team, home or visitor; null for a clock action without a team.",
    "home_score": "Home points after the action, parsed from the score text (2021-22 on; else null).",
    "visitor_score": "Visitor points after the action, parsed from the score text (2021-22 on; else null).",
    "coord_frame": "Frame of x and y: half_court_px (2017-18 and 2018-19 image pixels) or court_pct (2019-20 on, 0-100).",
    "code": "Qualifier code, the value of action_1_qualifier_code / action_2_qualifier_code on the actions.",
    "label": "Italian label of the qualifier code, as sent (translated in sdv-internal-refs legabasket-returns.md).",
    "zone": "Court zone 1-10 (the play-by-play side_area_code); zone 7 also holds the free throws.",
    "made": "Shots made in the zone, parsed from num.",
    "attempts": "Shots attempted in the zone, parsed from num.",
    "stat_type": "Statistic of the row: avg (per game), max (single-game high) or sum (total).",
    "club_id": "Club id of the team-season; the key that follows a club across seasons (stability unverified).",
    "club_code": "Club code of the team-season (three letters, e.g. TVB).",
}

#: Route-specific descriptions (keyed by wrapper short) that replace a returns-md row which only restates the column.
_PLAYER_NAMES = {"name": "Player's given name.", "surname": "Player's family name."}
_TEAM_NAME_BITS = {
    "v_team_name": "Visitor team name (the sponsor name of that season).",
    "plant_name": "Name of the arena where the game is played.",
}
ROUTE: Dict[str, Dict[str, str]] = {
    "championships": {"cserie_name": "Name of the championship series, e.g. Serie A."},
    "calendar": dict(_TEAM_NAME_BITS),
    "game": {**_TEAM_NAME_BITS, "spectators": "Attendance: spectators at the game."},
    "box_score": {
        "team_id": _CTX_TEAM,
        "player_num": "Jersey number the player wore in the game.",
        "player_name": "Player's given name.",
        "ass": "Assists credited to the player in the game.",
    },
    "team_box_score": {"team_id": _CTX_TEAM},
    "pbp": {"player_name": "Given name of the acting player.", "player_surname": "Family name of the acting player."},
    "shots": {"team_id": _CTX_TEAM},
    "roster": {
        "team_id": _CTX_TEAM,
        "year": "Season START year of the player entry (2026 = 2026-27).",
        **_PLAYER_NAMES,
        "height": "Player's height in centimetres.",
        "weight": "Player's weight in kilograms.",
    },
    "roster_coach": {
        "team_id": _CTX_TEAM,
        "name": "Coach's given name.",
        "surname": "Coach's family name.",
    },
    "team_players_stats": {
        "team_id": _CTX_TEAM,
        "player_name": "Player's given name.",
        "player_surname": "Player's family name.",
        "played_matches": "Games the player appeared in, in the request's championship series.",
        "start_formation": "Games the player started, in the request's championship series.",
    },
    "players": _PLAYER_NAMES,
    "player": {
        **_PLAYER_NAMES,
        "height": "Player's height in centimetres.",
        "weight": "Player's weight in kilograms.",
    },
    "player_stats": {"player_id": "Player id (global: one id for a whole career); the request's player."},
    "standings": {
        "season_start_year": "The wire season field: the season START year (2026 = 2026-27), same as year.",
        "wins": "Games the team has won through the round.",
        "defeats": "Games the team has lost through the round.",
        "points_made": "Points the team has scored through the round.",
        "game_played": "Games the team has played through the round.",
        "home_defeats": "Home games the team has lost through the round.",
        "away_defeats": "Away games the team has lost through the round.",
    },
    "game_log": {
        "stat_line_id": "Id of the player's stat line for the game (one per player and game).",
        "avg_points": "The player's plus-minus in the game (the site's name is misleading).",
        "match_adp": "The player's plus-minus in the game (the site's name is misleading).",
        "championships_match_id": "Game id (global across seasons); joins lba_calendar and lba_game.",
        "number": "Jersey number the player wore in the game.",
        "player_team_name": "Short name of the player's team in the game (without the season sponsor).",
    },
    "players_leaderboard": {
        "surname": "Player's family name.",
        "presences": "Games the player appeared in, counted for the leaderboard.",
    },
    "teams_leaderboard": {
        "short_name": "Short name of the team, e.g. BC Roma.",
        "total_played_matches": "Games the team has played, counted for the leaderboard.",
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
    """ROUTE, then LOCAL, then the route's md row; the renamed wire ``id`` looks up its md ``id`` row."""
    section, roots = SECTIONS[short]
    route = ROUTE.get(short, {})
    if name in route:
        return route[name]
    if name in LOCAL and name not in ("club_id", "club_code", "code", "label"):
        return LOCAL[name]
    rows = md.get(section, {})
    keys = [name]
    alias = ID_ALIAS.get(short)
    if alias and name == alias[0]:
        keys.append(alias[1])
    for key in keys:
        for root in roots:
            hit = rows.get(root + key)
            if hit:
                return hit
    return LOCAL.get(name, "")


def _filler(name: str, desc: str) -> bool:
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


_CODE = {pl.Int64: "i", pl.Float64: "f", pl.Boolean: "b", pl.String: "s"}
_TYPE = {"i": "integer", "f": "numeric", "b": "logical", "s": "character"}


def _frames(entry: Entry) -> List[pl.DataFrame]:
    _path, parser, fixtures, _summary, _fields = entry
    out = []
    for name in fixtures:
        body = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
        out.append(parser(body))
    return out


def _merge_codes(short: str, frames: List[pl.DataFrame]) -> Dict[str, str]:
    """``{column: dtype code}`` over every fixture of a wrapper, in first-seen order.

    A column takes the dtype of the fixtures that hold a value (``i`` and ``f`` merge to ``f``); one that is null in
    every fixture is an Int64 for an id and a String otherwise. Anything else stops the run.
    """
    seen: Dict[str, set] = {}
    order: List[str] = []
    for df in frames:
        for col, dtype in df.schema.items():
            if col not in order:
                order.append(col)
            if df.height and df[col].null_count() < df.height:
                if dtype not in _CODE:
                    raise SystemExit(f"legabasket: {short}.{col} has dtype {dtype}")
                seen.setdefault(col, set()).add(_CODE[dtype])
    out: Dict[str, str] = {}
    for col in order:
        codes = seen.get(col)
        if not codes:
            out[col] = "i" if re.search(r"(^|_)id$", col) else "s"
        elif len(codes) == 1:
            out[col] = next(iter(codes))
        elif codes == {"i", "f"}:
            out[col] = "f"
        else:
            raise SystemExit(f"legabasket: {short}.{col} has dtypes {sorted(codes)} across fixtures")
    return out


def _write_dtypes(codes: Dict[str, Dict[str, str]]) -> None:
    lines = [
        '"""Column dtypes of the Lega Basket parsers, one map per parser function.',
        "",
        "Generated by ``tools/codegen/gen_legabasket.py`` from the returns-schemas",
        "(``tools/codegen/schemas/native/legabasket``); do not edit. Codes: ``i`` Int64, ``f`` Float64,",
        "``b`` Boolean, ``s`` String. ``lba_parsers`` gives these dtypes to an empty frame, a gap-filled",
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
    return {
        "short": short,
        "summary": summary,
        "path": spec_path,
        "parser": parser.__name__,
        "returns_schema": f"native/legabasket/{short}",
        **copy.deepcopy(fields),  # no shared objects, so the YAML carries no anchors
    }


NOTES = [
    "Unofficial and unsupported: these wrappers read the JSON API that the Next.js site www.legabasket.it itself "
    'calls. The site\'s terms say "Non è consentito copiare … dati e informazioni senza autorizzazione scritta" '
    "(copying data and information without written authorisation is not allowed), except for personal, "
    "non-commercial use. The owner accepted the restriction for this intake on 2026-10-12. Payloads are not "
    "redistributed as release assets, and any published data release of this source needs separate sign-off.",
    "robots.txt allows /api and sets Crawl-delay: 2. The runtime waits 2 s before every request, retries included. "
    "Crawl incrementally: fetch a final game once (keyed by game_id), find new games with one lba_calendar call per "
    "round, and do not backfill a history in a burst.",
    "Seasons are END years (2027 = 2026-27); the site sends START years and the runtime maps them. Game and player "
    "ids are global; team ids are re-issued every season (join lba_teams for the stable club_id); a championship id "
    "is one season AND one phase (Regular Season and Play Off differ).",
    "Play-by-play starts in 2017-18 and changes shape in 2019-20 (pixel frame to court percent) and 2021-22; see "
    "lba_pbp. Box scores reach back to 1995-96, where the period scores were not recorded (0 means missing).",
]


def main() -> None:
    refs = refs_dir("legabasket", SPEC)
    spec = load_spec(refs, SPEC)
    missing = {e[0] for e in ENDPOINTS.values()} ^ set(spec["paths"])
    if missing:
        raise SystemExit(f"legabasket: spec paths and ENDPOINTS differ: {sorted(missing)}")
    for short, entry in ENDPOINTS.items():
        names = set(re.findall(r"\{([^}]+)\}", entry[0]))
        assert not {"league", "sport"} & names, short  # the renderer substitutes those as ESPN slugs
    doc = {
        "api": "legabasket",
        "host": HOST,
        "name_pattern": "lba_{short}",
        "module": "legabasket",
        "parser_module": "basketball.lba.lba_parsers",
        "getter_module": "sportsdataverse.basketball.lba.lba_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "parser_columns": True,
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "notes": NOTES,
            "raises": [
                "sportsdataverse.errors.NoDataError: the site has nothing for the request (an unknown season, a "
                "play-by-play before 2017-18, standings of a finished championship without a round, no rows).",
                "sportsdataverse.errors.AssetFetchError: the fetch failed or the answer is unknown or partial: an "
                "unknown id (the site answers HTTP 500), a game that is not final, a truncated page, or a body for "
                "another season, championship, game, team or player than the one asked for.",
            ],
            "see_also": [
                {
                    "name": "Lega Basket Serie A",
                    "url": "https://www.legabasket.it/",
                    "note": "the site whose API these wrappers read",
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
    write_yaml(ROOT / "tools/codegen/endpoints/legabasket.yaml", doc)

    md = _md_sections(refs / "legabasket-returns.md")
    unknown = {sec for sec, _ in SECTIONS.values()} - set(md)
    if unknown or set(SECTIONS) != set(ENDPOINTS):
        raise SystemExit(f"legabasket: SECTIONS out of step with the returns md / ENDPOINTS: {sorted(unknown)}")
    schema_dir = ROOT / "tools/codegen/schemas/native/legabasket"
    rewrite_schema_dir(schema_dir)
    P.DTYPES = {}  # parse by the name rules alone, so the last run's lba_dtypes.py cannot feed back
    codes = {short: _merge_codes(short, _frames(entry)) for short, entry in ENDPOINTS.items()}
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
        raise SystemExit("legabasket: filler or missing descriptions:\n" + "\n".join(bad))
    write_yaml(SNIPPET, snippet)
    _write_dtypes(codes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"legabasket: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
