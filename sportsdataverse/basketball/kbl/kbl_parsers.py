"""Parsers for the generated ``kbl`` wrappers (Korean Basketball League, ``api.kbl.or.kr``).

Every parser follows the package contract: a polars frame by default, pandas with ``return_as_pandas=True``,
snake_cased columns, and a zero-row frame (never an exception) for an empty or malformed body. The zero-row frame
carries the parser's documented returns schema (:data:`SPECS`, the single source of the schemas, the column order and
the descriptions' wire keys), so two empty frames, or an empty one and a populated one, stack.

**Input.** ``kbl_runtime._get`` stamps the request under ``_request`` (``gmkey`` for a game route; ``season``,
``season_code``, ``game_code``, ``glkey`` and ``division`` for a season route) and wraps a bare list as
``{"items": [...]}``. The parsers also accept the bare body, but then a frame that takes its season or game from the
request has it only when the body itself carries it. One body can feed several frames (the game header feeds
``kbl_game`` and ``kbl_period_scores``, the match chart ``kbl_shots``, ``kbl_score_track`` and ``kbl_game_leaders``, the
playoff bracket ``kbl_playoff_seeds``, ``kbl_playoff_series`` and ``kbl_playoff_games``, team-tracks
``kbl_team_tracks`` and ``kbl_rounds``): fetch it once with ``return_parsed=False`` and parse it as many ways as needed.

**Row rule.** Each route has its own shaper. Nested objects flatten to ``<parent>_<child>`` names chosen for the
frame, lists become JSON strings (or one row per element where the list is the data), and wire keys keep their KBL
spelling in snake case: ``fg`` is TWO-point field goals made (dunks included) and ``fg_a`` its attempts, ``fgt`` /
``fgt_a`` are the two- and three-point totals, ``to`` turnovers, ``threep`` three-pointers. The standings route spells
some counts in capitals (``AS``, ``BS``, ``DR``, ``OR``, ``ST``, ``TO``, ``TR``, ``PScore``) beside lower-case ones;
they are renamed to the box-score names (``ast``, ``bs``, ``defr``, ``offr``, ``stl``, ``to``, ``team_r``,
``pscore``). Site styling (logo classes), photo and shop URLs and the all-null ``x-unobserved`` fields are not
carried; officials and coaches appear by name only (the API serves no contact detail, and none would be emitted).

**Season.** The sdv season is the END year. KBL numbers seasons and D-League editions with one ``season_code``;
``season`` (Int64) is derived from the crosswalk in ``kbl_codes.SEASONS`` (a D-League edition belongs to the END
year of its winter, and ``season_grade`` 2 tells it from the KBL).

**Id key spaces** (all KBL ids are strings or small integers; none is a float):

* ``gmkey`` (String) ``S{season_code}G{game_code}N{game_no}`` identifies a game; ``glkey`` ``S{season_code}G{game_code}``
  a competition of a season. ``season_code`` and ``game_no`` are Int64, ``game_code`` a two-digit String.
* ``team_code`` (String, two digits with a leading zero, ``'06'``) is the club in that season and is **not stable
  across ownership changes** (the Anyang club is ``'40'`` in 1997-98, ``'70'`` today); D-League sides have their own
  codes (``kbl_teams.team_code_d``). ``team_code`` joins ``kbl_teams``, ``home_team_code`` / ``away_team_code`` and
  every box score.
* ``pcode`` (String, six digits) is the player, the same on every route (box score, shots, leaders, totals, round
  MVPs, the directory). The play-by-play carries NO ``pcode``: join its ``player_name`` to the game's box score
  ``player_name`` **and** ``team_code`` (names can repeat across teams).
* ``period`` (Int64) is 1-4 for the quarters and 5, 6, ... for overtimes; ``period_label`` is ``Q1``-``Q4``, ``OT1`` ...
  The wire says ``X1`` in the play-by-play but ``EQ1`` / ``eq1`` in the match chart; both are normalised.

**Dates and clock.** ``game_date`` and the other dates are ``Date`` (KST, parsed from ``YYYYMMDD``). ``game_start`` /
``game_end`` stay ``HHMM`` strings. Percentages are 0-100. Minutes are ``play_min`` plus ``play_sec`` seconds.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball.kbl import kbl_codes as codes
from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_kbl_current_standings",
    "parse_kbl_game",
    "parse_kbl_game_leaders",
    "parse_kbl_pbp",
    "parse_kbl_period_scores",
    "parse_kbl_player_box",
    "parse_kbl_player_totals",
    "parse_kbl_players",
    "parse_kbl_playoff_games",
    "parse_kbl_playoff_seeds",
    "parse_kbl_playoff_series",
    "parse_kbl_round_mvps",
    "parse_kbl_rounds",
    "parse_kbl_schedule",
    "parse_kbl_score_track",
    "parse_kbl_season_leaders",
    "parse_kbl_seasons",
    "parse_kbl_shots",
    "parse_kbl_standings",
    "parse_kbl_team_box",
    "parse_kbl_team_tracks",
    "parse_kbl_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``kbl_runtime.REQUEST_KEY``).
_REQUEST = "_request"

_S, _I, _F, _B, _D = pl.String, pl.Int64, pl.Float64, pl.Boolean, pl.Date
Spec = List[Tuple[str, str, Any]]  # (column, wire path or "" for a derived column, dtype)


# --- value coercion (the boundary: one dtype per column) ----------------------------------------------------------


def _s(v: Any) -> Optional[str]:
    """A string as sent; an int becomes its decimal text; a float or bool (never an id) is refused."""
    if isinstance(v, str):
        return v
    return str(v) if isinstance(v, int) and not isinstance(v, bool) else None


def _i(v: Any) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v == v and v == int(v) else None
    if isinstance(v, str) and re.fullmatch(r"\s*-?\d+\s*", v):
        return int(v)
    return None


def _f(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v) if v == v else None
    if isinstance(v, str):
        try:
            out = float(v)
        except ValueError:
            return None
        return out if out == out else None
    return None


def _b(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    return bool(v) if isinstance(v, int) and v in (0, 1) else None


def _d(v: Any) -> Optional[dt.date]:
    if isinstance(v, str) and re.fullmatch(r"\d{8}", v):
        try:
            return dt.date(int(v[:4]), int(v[4:6]), int(v[6:]))
        except ValueError:
            return None
    return None


_COERCE = {_S: _s, _I: _i, _F: _f, _B: _b, _D: _d}


# --- schemas -------------------------------------------------------------------------------------------------------

_GAME: Spec = [
    ("gmkey", "", _S),
    ("season_code", "", _I),
    ("season", "", _I),
    ("game_code", "", _S),
    ("game_no", "", _I),
]
_SEASON: Spec = [("season", "", _I), ("season_code", "", _I), ("game_code", "", _S)]
_PLAYER: Spec = [
    ("pcode", "player.pcode", _S),
    ("player_name", "player.pname", _S),
    ("player_name_en", "player.ename", _S),
    ("team_code", "player.tcode", _S),
    ("team_name", "player.tname", _S),
    ("back_num", "player.backNum", _S),
    ("pos", "player.pos", _S),
    ("player_flag", "player.playerFlag", _S),
]


def _rec(prefix: str, ints: str, floats: str = "") -> Spec:
    """Columns of a ``records`` object: the wire key snake-cased, Int64 for ``ints`` and Float64 for ``floats``."""
    return [(underscore(k), f"{prefix}{k}", _I) for k in ints.split()] + [
        (underscore(k), f"{prefix}{k}", _F) for k in floats.split()
    ]


_BOX_INTS = (
    "score fg fg_a fgt fgt_a threep threep_a ft ft_a dk dk_a rb offr defr ast stl bs to foul wft woft pp pp_a pscore "
    "sast dfl gd idf fb ef tf inout inout1 play_min play_sec"
)
_BOX_INTS_WIRE = re.sub(r"_([a-z])", lambda m: m.group(1).upper(), _BOX_INTS)
_RATES = "offrtg defrtg netrtg efgRt tsRt astRt astTo astRatio orebRt drebRt rebRt pace tovRt poss"

SPECS: Dict[str, Spec] = {
    "parse_kbl_seasons": [
        ("season", "", _I),
        ("season_code", "seasonCode", _I),
        ("glkey", "glkey", _S),
        ("season_name", "seasonName", _S),
        ("season_name_full", "seasonNameFull", _S),
        ("season_grade", "seasonGrade", _I),
        ("season_category", "seasonCategory", _S),
        ("game_code", "gameCode", _S),
        ("start_date", "gamedateStart", _D),
        ("end_date", "gamedateEnd", _D),
    ],
    "parse_kbl_teams": [
        ("team_code", "teamCode", _S),
        ("team_code_d", "teamCodeD", _S),
        ("team_name", "teamName", _S),
        ("team_name_full", "teamNameFull", _S),
        ("team_name_en", "teamNameEn", _S),
        ("team_name_full_en", "teamNameFullEn", _S),
    ],
    "parse_kbl_schedule": [
        ("gmkey", "gmkey", _S),
        ("glkey", "glkey", _S),
        ("season", "", _I),
        ("season_code", "seasonCode", _I),
        ("season_name", "seasonName1", _S),
        ("season_grade", "seasonGrade", _I),
        ("game_code", "gameCode", _S),
        ("game_no", "gameNo", _I),
        ("season_category", "seasonCategory", _S),
        ("season_category_name", "seasonCategoryName", _S),
        ("game_date", "gameDate", _D),
        ("week_day", "weekDay", _S),
        ("game_start", "gameStart", _S),
        ("game_end", "gameEnd", _S),
        ("game_time", "gameTime", _I),
        ("home_team_code", "tcodeH", _S),
        ("away_team_code", "tcodeA", _S),
        ("home_team_name", "tnameH", _S),
        ("away_team_name", "tnameA", _S),
        ("home_team_name_full", "tnameFH", _S),
        ("away_team_name_full", "tnameFA", _S),
        ("home_score", "scoreH", _I),
        ("away_score", "scoreA", _I),
        ("is_started", "isStarted", _I),
        ("is_ended", "isEnded", _I),
        ("playing_quarter", "playingQuarter", _S),
        ("stadium_name", "stadiumname", _S),
        ("stadium_name_full", "stadiumnameF", _S),
        ("stadium_name_en", "stadiumnameEn", _S),
        ("tv", "tv", _S),
        ("tv_code", "tvCode", _S),
        ("article_id", "artcSq", _I),
        ("challenge_id", "challengeSq", _I),
    ],
    "parse_kbl_game": [
        ("gmkey", "game.gmkey", _S),
        ("glkey", "game.glkey", _S),
        ("season", "", _I),
        ("season_code", "game.seasonCode", _I),
        ("season_name", "game.seasonName", _S),
        ("season_grade", "game.seasonGrade", _I),
        ("game_code", "game.gameCode", _S),
        ("game_no", "game.gameNo", _I),
        ("season_category_name", "game.seasonCategoryName", _S),
        ("game_date", "game.gameDate", _D),
        ("week_day", "game.weekDay", _S),
        ("game_start", "game.gameStart", _S),
        ("game_end", "game.gameEnd", _S),
        ("game_time", "game.gameTime", _I),
        ("home_team_code", "game.tcodeH", _S),
        ("away_team_code", "game.tcodeA", _S),
        ("home_team_name", "game.tnameH", _S),
        ("away_team_name", "game.tnameA", _S),
        ("home_team_name_full", "game.tnameFH", _S),
        ("away_team_name_full", "game.tnameFA", _S),
        ("home_score", "", _I),
        ("away_score", "", _I),
        ("overtimes", "", _I),
        ("is_started", "game.isStarted", _I),
        ("is_ended", "game.isEnded", _I),
        ("playing_quarter", "game.playingQuarter", _S),
        ("stadium_name", "game.stadiumname", _S),
        ("stadium_name_full", "game.stadiumnameF", _S),
        ("stadium_name_en", "game.stadiumnameEn", _S),
        ("crowds", "game.crowds", _I),
        ("tv", "game.tv", _S),
        ("tv_code", "game.tvCode", _S),
        ("detail_url", "game.detailUrl", _S),
        ("highlight_url", "game.highlightUrl", _S),
        ("referee_main", "game.judges.main.name", _S),
        ("referees_sub", "", _S),
        ("home_manager", "teams.home.staffs.manager.name", _S),
        ("away_manager", "teams.away.staffs.manager.name", _S),
        ("home_coaches", "", _S),
        ("away_coaches", "", _S),
    ],
    "parse_kbl_period_scores": _GAME
    + [
        ("side", "", _S),
        ("team_code", "", _S),
        ("period", "", _I),
        ("period_label", "", _S),
        ("score", "", _I),
    ],
    "parse_kbl_pbp": _GAME
    + [
        ("action_number", "", _I),
        ("event_number", "n", _I),
        ("period", "", _I),
        ("period_label", "", _S),
        ("minutes_left", "m", _I),
        ("seconds_left", "s", _I),
        ("clock_seconds_left", "", _I),
        ("team_code", "t", _S),
        ("player_name", "p", _S),
        ("player_name_en", "e", _S),
        ("action_code", "a", _S),
        ("action", "", _S),
        ("action_ko", "", _S),
        ("points", "", _I),
        ("substitution_code", "c", _S),
        ("substitution", "", _S),
        ("foul_code", "f", _S),
        ("foul", "", _S),
    ],
    "parse_kbl_player_box": _GAME
    + [("team_code", "player.tcode", _S), ("home_away", "homeAway", _S), ("started", "startFlag", _I)]
    + _PLAYER[:3]
    + _PLAYER[4:]
    + _rec("records.", _BOX_INTS_WIRE + " fo marginCn", _RATES + " per pie usgRt toRatio")
    + [("minutes_played", "", _F)],
    "parse_kbl_team_box": _GAME
    + [("team_code", "tcode", _S)]
    + _rec(
        "records.",
        _BOX_INTS_WIRE + " bf foulout teamR tfb tto fbScoreCn turnoverScoreCn secChanceScoreCn benchScoreCn "
        "maxContiScoreCn maxLeadScoreCn scoreHighTimeCn",
        _RATES,
    )
    + [("minutes_played", "", _F)],
    "parse_kbl_shots": _GAME
    + [
        ("pcode", "pcode", _S),
        ("player_name", "pname", _S),
        ("player_name_en", "ename", _S),
        ("team_code", "tcode", _S),
        ("period", "", _I),
        ("period_label", "", _S),
        ("x", "logs.x", _I),
        ("y", "logs.y", _I),
        ("made", "", _B),
        ("basket", "logs.d", _S),
    ],
    "parse_kbl_score_track": _GAME
    + [
        ("period", "", _I),
        ("period_label", "", _S),
        ("checkpoint", "", _I),
        ("home_margin", "", _I),
        ("home_score", "", _I),
        ("away_score", "", _I),
    ],
    "parse_kbl_game_leaders": _GAME
    + [
        ("part", "bestPlayer.part", _S),
        ("side", "", _S),
        ("pcode", "bestPlayer.home.pcode", _S),
        ("player_name", "bestPlayer.home.pname", _S),
        ("player_name_en", "bestPlayer.home.pnameEn", _S),
        ("team_name", "bestPlayer.home.tname", _S),
        ("value", "bestPlayer.home.value", _F),
        ("play_min", "bestPlayer.home.playMin", _I),
        ("play_sec", "bestPlayer.home.playSec", _I),
    ],
    "parse_kbl_player_totals": _SEASON
    + [("glkey", "", _S)]
    + _PLAYER
    + [("game_count", "gameCount", _I), ("start_count", "startCount", _I)]
    + _rec("records.", _BOX_INTS_WIRE + " foulout")
    + [("minutes_played", "", _F)],
    "parse_kbl_standings": _SEASON[:2]
    + [
        ("game_code", "gameCode", _S),
        ("rank", "rank", _I),
        ("team_code", "teamCode", _S),
        ("team_name", "teamName1", _S),
        ("team_name_full", "teamName2", _S),
        ("game_count", "gameCount", _I),
        ("wins", "TWin", _I),
        ("losses", "TLoss", _I),
        ("draws", "TDraw", _I),
        ("win_rate", "winRate", _F),
        ("win_diff", "winDiff", _F),
        ("home_wins", "hwin", _I),
        ("home_losses", "hloss", _I),
        ("away_wins", "awin", _I),
        ("away_losses", "aloss", _I),
        ("conti_win", "contiWin", _I),
        ("conti_loss", "contiLoss", _I),
        ("conti_date", "contiDate", _D),
        ("max_win", "maxWin", _I),
        ("max_loss", "maxLoss", _I),
        ("last_game_date", "gameDate", _D),
        ("last_five", "lastRecor", _S),
        ("last_game_won", "win", _B),
        ("last_game_lost", "loss", _B),
        ("last_game_drawn", "draw", _B),
        ("game_flag", "gameFlag", _B),
        ("updated_at", "inputtime", _S),
        ("score", "score", _I),
        ("fg", "fg", _I),
        ("fg_a", "fgA", _I),
        ("threep", "threep", _I),
        ("threep_a", "threepA", _I),
        ("ft", "ft", _I),
        ("ft_a", "ftA", _I),
        ("dk", "dk", _I),
        ("dk_a", "dkA", _I),
        ("ast", "AS", _I),
        ("bs", "BS", _I),
        ("defr", "DR", _I),
        ("offr", "OR", _I),
        ("stl", "ST", _I),
        ("to", "TO", _I),
        ("team_r", "TR", _I),
        ("pp", "pp", _I),
        ("pp_a", "ppA", _I),
        ("pscore", "PScore", _I),
        ("wft", "wft", _I),
        ("woft", "woft", _I),
        ("foul_total", "foulTot", _I),
        ("tfb", "tfb", _I),
        ("tto", "tto", _I),
        ("tf", "tf", _I),
        ("fb", "fb", _I),
        ("ef", "ef", _I),
        ("bf", "bf", _I),
        ("gd", "gd", _I),
        ("idf", "idf", _I),
        ("inout", "inout", _I),
        ("inout1", "inout1", _I),
        ("foulout", "foulout", _I),
        ("play_min", "playMin", _I),
        ("play_sec", "playSec", _I),
    ],
    "parse_kbl_team_tracks": _SEASON
    + [
        ("glkey", "", _S),
        ("team_code", "tcode", _S),
        ("team_name", "tname", _S),
        ("game_number", "", _I),
        ("rank", "", _I),
    ],
    "parse_kbl_rounds": _SEASON
    + [("glkey", "", _S), ("round", "gameRound", _I), ("from_date", "fromDate", _D), ("to_date", "toDate", _D)],
    "parse_kbl_playoff_seeds": [
        ("season", "", _I),
        ("season_code", "defaultRank.seasonCode", _I),
        ("game_code", "defaultRank.gameCode", _S),
        ("seed", "defaultRank.rank", _I),
        ("team_code", "defaultRank.teamCode", _S),
        ("team_name", "defaultRank.teamName1", _S),
        ("team_name_full", "defaultRank.teamName2", _S),
    ],
    "parse_kbl_playoff_series": [
        ("season", "", _I),
        ("season_code", "", _I),
        ("bracket_format", "", _S),
        ("round", "", _S),
        ("series_no", "gameHoi", _I),
        ("team_code", "teamCode", _S),
        ("opponent_code", "", _S),
        ("seed", "", _I),
        ("wins", "win", _I),
        ("losses", "loss", _I),
        ("game_date", "gameDate", _D),
    ],
    "parse_kbl_playoff_games": [
        ("season", "", _I),
        ("season_code", "result.game1.gmkey.seasonCode", _I),
        ("gmkey", "", _S),
        ("game_code", "result.game1.gmkey.gameCode", _S),
        ("game_no", "result.game1.gmkey.gameNo", _I),
        ("round", "", _S),
        ("series_no", "", _I),
        ("game_date", "result.game1.gameDate", _D),
        ("home_team_code", "result.game1.tcodeH", _S),
        ("away_team_code", "result.game1.tcodeA", _S),
        ("home_team_name", "result.game1.tnameH", _S),
        ("away_team_name", "result.game1.tnameA", _S),
        ("home_score", "result.game1.scoreH", _I),
        ("away_score", "result.game1.scoreA", _I),
        ("home_win", "result.game1.homeWin", _B),
    ],
    "parse_kbl_round_mvps": [
        ("season", "", _I),
        ("season_code", "seasonCode", _I),
        ("round", "gameRound", _I),
        ("pcode", "playerNo", _S),
        ("team_code", "teamCode", _S),
        ("player_name", "kname", _S),
        ("player_name_en", "ename", _S),
        ("game_count", "gameCount", _I),
        ("total_seconds", "", _I),
        ("total_score", "totalScore", _I),
        ("total_rebound", "totalRebound", _I),
        ("total_assist", "totalAssist", _I),
    ],
    "parse_kbl_current_standings": [
        ("rank", "rank", _I),
        ("team_code", "tcode", _S),
        ("team_name", "tname", _S),
        ("team_name_full", "tnameF", _S),
        ("wins", "win", _I),
        ("losses", "loss", _I),
        ("draws", "draw", _I),
        ("win_diff", "winDiff", _F),
        ("home_wins", "hwin", _I),
        ("home_losses", "hloss", _I),
        ("away_wins", "awin", _I),
        ("away_losses", "aloss", _I),
        ("conti_win", "contiWin", _I),
        ("conti_loss", "contiLoss", _I),
        ("max_win", "maxWin", _I),
        ("max_loss", "maxLoss", _I),
        ("last_five", "", _S),
    ],
    "parse_kbl_season_leaders": [
        ("part", "part", _S),
        ("part_label", "", _S),
        ("rank", "list.rank", _I),
        ("value", "list.value", _F),
        ("game_count", "list.gameCount", _I),
        ("pcode", "list.player.pcode", _S),
        ("player_name", "list.player.pname", _S),
        ("player_name_en", "list.player.ename", _S),
        ("team_code", "list.player.tcode", _S),
        ("team_name", "list.player.tname", _S),
        ("back_num", "list.player.backNum", _S),
        ("pos", "list.player.pos", _S),
        ("player_flag", "list.player.playerFlag", _S),
    ],
    "parse_kbl_players": [
        ("pcode", "pcode", _S),
        ("player_name", "pname", _S),
        ("player_name_en", "ename", _S),
        ("team_code", "tcode", _S),
        ("team_name", "tname", _S),
        ("team_name_en", "tnameEn", _S),
        ("back_num", "backNum", _S),
        ("pos", "pos", _S),
        ("player_flag", "playerFlag", _S),
        ("last_career", "playerLastCareerIf", _S),
    ],
}

#: Playoff rounds by series position (new format) and by wire key (old format).
_ROUND_NAMES = {"sixResult": "quarterfinal", "fourResult": "semifinal", "finalResult": "final"}
#: New-format series: (round, series_no, result key for the games, wins keys, team slot keys).
_SERIES = (
    ("quarterfinal", 1, "game1", ("resultFinalSix1", "resultFinalSix2"), ("finalSix4", "finalSix5")),
    ("quarterfinal", 2, "game2", ("resultFinalSix3", "resultFinalSix4"), ("finalSix3", "finalSix6")),
    ("semifinal", 1, "game3", ("resultFinalFour1", "resultFinalFour2"), ("finalSix1", "finalSix1vs2Winner")),
    ("semifinal", 2, "game4", ("resultFinalFour3", "resultFinalFour4"), ("finalSix2", "finalSix3vs4Winner")),
    ("final", 1, "game5", ("resultFinal1", "resultFinal2"), ("finalFour1vs2Winner", "finalFour3vs4Winner")),
)


# --- helpers -------------------------------------------------------------------------------------------------------


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _request(raw: Any) -> Mapping[str, Any]:
    return _obj(_obj(raw).get(_REQUEST))


def _items(raw: Any) -> List[Mapping[str, Any]]:
    """Rows of a list route: a bare list, or ``items`` of the wrapped body."""
    return _list(raw) if isinstance(raw, list) else _list(_obj(raw).get("items"))


def _path(obj: Any, path: str) -> Any:
    for part in path.split(".") if path else ():
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


def _row(name: str, obj: Mapping[str, Any], **derived: Any) -> Dict[str, Any]:
    """One row of parser ``name``: every column of its spec, coerced to the column's dtype."""
    out: Dict[str, Any] = {}
    for col, wire, dtype in SPECS[name]:
        value = derived[col] if col in derived else (_path(obj, wire) if wire else None)
        out[col] = _COERCE[dtype](value)
    return out


def _frame(name: str, rows: Iterable[Mapping[str, Any]]) -> pl.DataFrame:
    """The frame of parser ``name`` over ``rows`` (zero rows keep the documented schema)."""
    schema = {col: dtype for col, _wire, dtype in SPECS[name]}
    rows = list(rows)
    return pl.DataFrame({c: [r.get(c) for r in rows] for c in schema}, schema=schema)


def _finish(df: pl.DataFrame, return_as_pandas: bool) -> Frame:
    return df.to_pandas() if return_as_pandas else df


def _json(value: Any) -> Optional[str]:
    return json.dumps(value, ensure_ascii=False) if isinstance(value, list) and value else None


def _game_ctx(gmkey: Any) -> Dict[str, Any]:
    """The columns every game frame starts with, derived from the ``gmkey`` alone."""
    parts = codes.game_key_parts(gmkey) if isinstance(gmkey, str) else None
    if parts is None:
        return {"gmkey": gmkey if isinstance(gmkey, str) else None}
    return {
        "gmkey": gmkey,
        "season_code": parts[0],
        "season": codes.season_end(parts[0]),
        "game_code": parts[1],
        "game_no": parts[2],
    }


def _game_key(raw: Any) -> Optional[str]:
    """The game a body belongs to: the request stamp, else the header's own ``game.gmkey``."""
    key = _request(raw).get("gmkey")
    return key if isinstance(key, str) else _path(raw, "game.gmkey")


def _season_ctx(raw: Any, season_code: Any = None, game_code: Any = None) -> Dict[str, Any]:
    """``season`` / ``season_code`` / ``game_code`` / ``glkey`` from the request stamp, else from a row's own."""
    req = _request(raw)
    code = _i(req.get("season_code"))
    code = code if code is not None else _i(season_code)
    end = _i(req.get("season"))
    end = end if end is not None else (codes.season_end(code) if code is not None else None)
    gc = _s(req.get("game_code")) or _s(game_code)
    glkey = _s(req.get("glkey")) or (f"S{code}G{gc}" if code is not None and gc else None)
    return {"season": end, "season_code": code, "game_code": gc, "glkey": glkey}


def _minutes(play_min: Any, play_sec: Any) -> Optional[float]:
    m, s = _i(play_min), _i(play_sec)
    return round(m + s / 60, 4) if m is not None and s is not None else None


# --- shapers -------------------------------------------------------------------------------------------------------


def _shape_seasons(raw: Any) -> List[Dict[str, Any]]:
    return [_row("parse_kbl_seasons", r, season=codes.season_end(_i(r.get("seasonCode")) or 0)) for r in _items(raw)]


def _shape_teams(raw: Any) -> List[Dict[str, Any]]:
    return [_row("parse_kbl_teams", r) for r in _items(raw)]


def _shape_schedule(raw: Any) -> List[Dict[str, Any]]:
    return [_row("parse_kbl_schedule", r, season=codes.season_end(_i(r.get("seasonCode")) or 0)) for r in _items(raw)]


def _totals(records: Any) -> Tuple[Optional[int], Optional[int]]:
    """Points by side: the sum of the four quarters and every overtime (the header carries no totals)."""
    out: List[Optional[int]] = []
    for side in ("home", "away"):
        rec = _obj(_obj(records).get(side))
        quarters = [_i(rec.get(f"scoreq{i}")) for i in range(1, 5)]
        extra = [_i(x) for x in rec.get("scoreeq") or []] if isinstance(rec.get("scoreeq"), list) else []
        out.append(sum(quarters) + sum(extra) if None not in quarters and None not in extra else None)  # type: ignore[arg-type]
    return out[0], out[1]


def _overtimes(records: Any) -> Optional[int]:
    counts = [
        len(_obj(_obj(records).get(side)).get("scoreeq") or [])
        for side in ("home", "away")
        if isinstance(_obj(_obj(records).get(side)).get("scoreeq"), list)
    ]
    return max(counts) if counts else None


def _shape_game(raw: Any) -> List[Dict[str, Any]]:
    body = _obj(raw)
    game = _obj(body.get("game"))
    if not game:
        return []
    home, away = _totals(body.get("teamrecords"))
    ctx = _game_ctx(game.get("gmkey"))
    teams = _obj(body.get("teams"))
    subs = [n for n in (_obj(j).get("name") for j in _list(_obj(game.get("judges")).get("sub"))) if isinstance(n, str)]

    def coaches(side: str) -> Optional[str]:
        names = [
            n for n in (_obj(c).get("name") for c in _list(_obj(_obj(teams.get(side)).get("staffs")).get("coaches")))
        ]
        return _json([n for n in names if isinstance(n, str)])

    return [
        _row(
            "parse_kbl_game",
            body,
            season=ctx.get("season"),
            home_score=home,
            away_score=away,
            overtimes=_overtimes(body.get("teamrecords")),
            referees_sub=_json(subs),
            home_coaches=coaches("home"),
            away_coaches=coaches("away"),
        )
    ]


def _shape_period_scores(raw: Any) -> List[Dict[str, Any]]:
    body = _obj(raw)
    game, records = _obj(body.get("game")), _obj(body.get("teamrecords"))
    ctx = _game_ctx(game.get("gmkey") if game else _game_key(raw))
    rows = []
    for side in ("home", "away"):
        rec = _obj(records.get(side))
        code = _path(body, f"teams.{side}.tcode") or game.get("tcodeH" if side == "home" else "tcodeA")
        scores = [(f"Q{i}", rec.get(f"scoreq{i}")) for i in range(1, 5)] + [
            (f"X{n}", v) for n, v in enumerate(rec.get("scoreeq") or [], 1)
        ]
        for label, score in scores:
            period, period_label = codes.period_of(label)
            rows.append(
                _row(
                    "parse_kbl_period_scores",
                    {},
                    **ctx,
                    side=side,
                    team_code=code,
                    period=period,
                    period_label=period_label,
                    score=score,
                )
            )
    return rows if records else []


def _shape_pbp(raw: Any) -> List[Dict[str, Any]]:
    ctx = _game_ctx(_game_key(raw))
    rows = []
    for n, r in enumerate(_items(raw), 1):
        action = _s(r.get("a"))
        period, label = codes.period_of(r.get("q"))
        team = _s(r.get("t"))
        m, s = _i(r.get("m")), _i(r.get("s"))
        sub, foul = _s(r.get("c")), _s(r.get("f"))
        rows.append(
            _row(
                "parse_kbl_pbp",
                r,
                **ctx,
                action_number=n,
                period=period,
                period_label=label,
                clock_seconds_left=m * 60 + s if m is not None and s is not None else None,
                team_code=team.strip() or None if team is not None else None,
                action=codes.ACTIONS[action][1] if action in codes.ACTIONS else None,
                action_ko=codes.ACTIONS[action][0] if action in codes.ACTIONS else None,
                points=codes.ACTION_POINTS.get(action or ""),
                substitution=codes.SUBSTITUTIONS[sub][1] if sub in codes.SUBSTITUTIONS else None,
                foul=codes.FOULS[foul][1] if foul in codes.FOULS else None,
            )
        )
    return rows


def _shape_box(name: str, raw: Any) -> List[Dict[str, Any]]:
    ctx = _game_ctx(_game_key(raw))
    out = []
    for r in _items(raw):
        rec = _obj(r.get("records"))
        out.append(_row(name, r, **ctx, minutes_played=_minutes(rec.get("playMin"), rec.get("playSec"))))
    return out


def _shape_shots(raw: Any) -> List[Dict[str, Any]]:
    body = _obj(raw)
    ctx = _game_ctx(_game_key(raw))
    rows = []
    for player in _list(body.get("shootLog")):
        for shot in _list(player.get("logs")):
            period, label = codes.period_of(shot.get("q"))
            row = {**player, "logs": shot}
            made = shot.get("o")
            rows.append(
                _row(
                    "parse_kbl_shots",
                    row,
                    **ctx,
                    period=period,
                    period_label=label,
                    made=(made == "O") if made in ("O", "X") else None,
                )
            )
    return rows


def _shape_score_track(raw: Any) -> List[Dict[str, Any]]:
    body = _obj(raw)
    ctx = _game_ctx(_game_key(raw))
    lead, chart = _obj(body.get("leadTrack")), _obj(body.get("scoreChart"))
    keys = {k for k in [*lead, *_obj(chart.get("home")), *_obj(chart.get("away"))] if codes.period_of(k)[0]}
    rows = []
    for key in sorted(keys, key=lambda k: codes.period_of(k)[0] or 0):
        period, label = codes.period_of(key)
        series = [lead.get(key), _obj(chart.get("home")).get(key), _obj(chart.get("away")).get(key)]
        lists: List[List[Any]] = [s if isinstance(s, list) else [] for s in series]
        for i in range(max(len(s) for s in lists)):
            at = [s[i] if i < len(s) else None for s in lists]
            rows.append(
                _row(
                    "parse_kbl_score_track",
                    {},
                    **ctx,
                    period=period,
                    period_label=label,
                    checkpoint=i + 1,
                    home_margin=at[0],
                    home_score=at[1],
                    away_score=at[2],
                )
            )
    return rows


def _shape_game_leaders(raw: Any) -> List[Dict[str, Any]]:
    ctx = _game_ctx(_game_key(raw))
    rows = []
    for best in _list(_obj(raw).get("bestPlayer")):
        for side in ("home", "away"):
            p = _obj(best.get(side))
            if not p:
                continue
            wired = {"bestPlayer": {"part": best.get("part"), "home": p}}
            rows.append(_row("parse_kbl_game_leaders", wired, **ctx, side=side))
    return rows


def _shape_player_totals(raw: Any) -> List[Dict[str, Any]]:
    rows = []
    for r in _items(raw):
        rec = _obj(r.get("records"))
        ctx = _season_ctx(raw)
        rows.append(
            _row("parse_kbl_player_totals", r, **ctx, minutes_played=_minutes(rec.get("playMin"), rec.get("playSec")))
        )
    return rows


def _shape_standings(raw: Any) -> List[Dict[str, Any]]:
    rows = []
    for r in _items(raw):
        ctx = _season_ctx(raw, r.get("seasonCode"), r.get("gameCode"))
        rows.append(_row("parse_kbl_standings", r, season=ctx["season"], season_code=ctx["season_code"]))
    return rows


def _shape_team_tracks(raw: Any) -> List[Dict[str, Any]]:
    ctx = _season_ctx(raw)
    rows = []
    for team in _list(_obj(raw).get("tracks")):
        ranks: List[Any] = team["tracks"] if isinstance(team.get("tracks"), list) else []
        rows += [
            _row("parse_kbl_team_tracks", team, **ctx, game_number=n, rank=rank) for n, rank in enumerate(ranks, 1)
        ]
    return rows


def _shape_rounds(raw: Any) -> List[Dict[str, Any]]:
    ctx = _season_ctx(raw)
    return [_row("parse_kbl_rounds", r, **ctx) for r in _list(_obj(raw).get("rounds"))]


def _playoff_ctx(raw: Any) -> Dict[str, Any]:
    seeds = _list(_obj(raw).get("defaultRank"))
    first = seeds[0] if seeds else {}
    return _season_ctx(raw, first.get("seasonCode"), first.get("gameCode"))


def _shape_playoff_seeds(raw: Any) -> List[Dict[str, Any]]:
    ctx = _playoff_ctx(raw)
    out = []
    for s in _list(_obj(raw).get("defaultRank")):
        out.append(_row("parse_kbl_playoff_seeds", {"defaultRank": s}, season=ctx["season"]))
    return out


def _seed_of(raw: Any) -> Dict[str, Optional[int]]:
    return {_s(s.get("teamCode")) or "": _i(s.get("rank")) for s in _list(_obj(raw).get("defaultRank"))}


def _shape_playoff_series(raw: Any) -> List[Dict[str, Any]]:
    body = _obj(raw)
    ctx = _playoff_ctx(raw)
    seeds = _seed_of(raw)
    result = _obj(body.get("result"))
    rows: List[Dict[str, Any]] = []
    if result:  # new format (seasonCode 45 on): the bracket as numbers, teams from the seed slots
        for rnd, no, games_key, (w1, w2), (t1, t2) in _SERIES:
            codes_ = [_s(result.get(t1)), _s(result.get(t2))]
            wins = [_i(result.get(w1)), _i(result.get(w2))]
            played = {c for g in _list(result.get(games_key)) for c in (_s(g.get("tcodeH")), _s(g.get("tcodeA"))) if c}
            if played and played != {c for c in codes_ if c}:
                codes_ = [None, None]  # the slot rule does not hold for this bracket: do not guess the teams
            for i in (0, 1):
                rows.append(
                    _row(
                        "parse_kbl_playoff_series",
                        {},
                        season=ctx["season"],
                        season_code=ctx["season_code"],
                        bracket_format="new",
                        round=rnd,
                        series_no=no,
                        team_code=codes_[i],
                        opponent_code=codes_[1 - i],
                        seed=seeds.get(codes_[i] or ""),
                        wins=wins[i],
                        losses=wins[1 - i],
                    )
                )
        return rows
    for key, rnd in _ROUND_NAMES.items():  # old format: one row per team per round
        for r in _list(body.get(key)):
            rows.append(
                _row(
                    "parse_kbl_playoff_series",
                    r,
                    season=ctx["season"],
                    season_code=ctx["season_code"],
                    bracket_format="old",
                    round=rnd,
                    seed=seeds.get(_s(r.get("teamCode")) or ""),
                )
            )
    # the opponent: the other team of the same round and series
    by: Dict[Tuple[Any, Any], List[Any]] = {}
    for row in rows:
        by.setdefault((row["round"], row["series_no"]), []).append(row["team_code"])
    for row in rows:
        others = [c for c in by[(row["round"], row["series_no"])] if c != row["team_code"]]
        row["opponent_code"] = others[0] if len(others) == 1 else None
    return rows


def _shape_playoff_games(raw: Any) -> List[Dict[str, Any]]:
    result = _obj(_obj(raw).get("result"))
    ctx = _playoff_ctx(raw)
    rows = []
    for rnd, no, games_key, _wins, _slots in _SERIES:
        for g in _list(result.get(games_key)):
            gk = _obj(g.get("gmkey"))
            parts = (_i(gk.get("seasonCode")), _s(gk.get("gameCode")), _i(gk.get("gameNo")))
            key = f"S{parts[0]}G{parts[1]}N{parts[2]}" if None not in parts else None
            wired = {"result": {"game1": {**g, "gmkey": gk}}}
            rows.append(
                _row("parse_kbl_playoff_games", wired, season=ctx["season"], gmkey=key, round=rnd, series_no=no)
            )
    return rows


def _shape_round_mvps(raw: Any) -> List[Dict[str, Any]]:
    rows = []
    for r in _items(raw):
        parts = [_i(r.get(k)) for k in ("totalHour", "totalMinute", "totalSecond")]
        total = sum((p or 0) * f for p, f in zip(parts, (3600, 60, 1))) if None not in parts else None
        ctx = _season_ctx(raw, r.get("seasonCode"))
        rows.append(_row("parse_kbl_round_mvps", r, season=ctx["season"], total_seconds=total))
    return rows


def _shape_current_standings(raw: Any) -> List[Dict[str, Any]]:
    rows = []
    for r in _items(raw):
        last = r.get("lastRecord")
        five = (
            ",".join(str(x) for x in last) if isinstance(last, list) and all(_i(x) is not None for x in last) else None
        )
        rows.append(_row("parse_kbl_current_standings", r, last_five=five))
    return rows


def _shape_season_leaders(raw: Any) -> List[Dict[str, Any]]:
    rows = []
    for cat in _items(raw):
        part = _s(cat.get("part"))
        for entry in _list(cat.get("list")):
            rows.append(
                _row(
                    "parse_kbl_season_leaders",
                    {"part": part, "list": entry},
                    part_label=codes.LEADER_PARTS.get(part or ""),
                )
            )
    return rows


def _shape_players(raw: Any) -> List[Dict[str, Any]]:
    return [_row("parse_kbl_players", r) for r in _items(raw)]


# --- public parsers --------------------------------------------------------------


def parse_kbl_seasons(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/season/list`` -> one row per KBL regular season (and its D-League editions' gameCode 01 rows, if listed).

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per season: ``season`` (END year), ``season_code``, ``glkey``, ``season_name`` (``2025-2026``, ``1997``, or a D-League edition label), ``season_grade`` (1 KBL, 2 D-League), ``season_category``, ``game_code``, ``start_date`` and ``end_date``. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_seasons, parse_kbl_seasons
            raw = kbl_seasons(return_parsed=False)
            parse_kbl_seasons(raw)

        Pandas::

            parse_kbl_seasons(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_seasons", _shape_seasons(raw)), return_as_pandas)


def parse_kbl_teams(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/common/teamList`` -> the current clubs.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per club: ``team_code`` (two-digit string), ``team_code_d`` (its D-League side's code), Korean and English short and full names. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_teams, parse_kbl_teams
            raw = kbl_teams(return_parsed=False)
            parse_kbl_teams(raw)

        Pandas::

            parse_kbl_teams(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_teams", _shape_teams(raw)), return_as_pandas)


def parse_kbl_schedule(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/match/list`` -> one row per game (every competition) dated inside the window.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per game with the home/away codes, names, scores, venue, broadcaster and the unreliable ``is_started`` / ``is_ended`` flags (0 on every finished game through 2005-06). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_schedule, parse_kbl_schedule
            raw = kbl_schedule(season=2026, return_parsed=False)
            parse_kbl_schedule(raw)

        Pandas::

            parse_kbl_schedule(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_schedule", _shape_schedule(raw)), return_as_pandas)


def parse_kbl_game(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/match/{gmkey}`` -> the game header, one row.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: ids, date and times, teams, the final score (``home_score`` / ``away_score``, summed from the period scores because the header carries no totals), ``overtimes``, venue, attendance, the crew chief (``referee_main``), umpires and both head coaches by name, and the assistant coaches as JSON lists. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_game, parse_kbl_game
            raw = kbl_game("S47G01N49", return_parsed=False)
            parse_kbl_game(raw)

        Pandas::

            parse_kbl_game(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_game", _shape_game(raw)), return_as_pandas)


def parse_kbl_period_scores(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/match/{gmkey}`` -> points by period and side.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per side and period (``Q1``-``Q4`` and each overtime) with ``team_code`` and ``score``. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_period_scores, parse_kbl_period_scores
            raw = kbl_period_scores("S47G01N49", return_parsed=False)
            parse_kbl_period_scores(raw)

        Pandas::

            parse_kbl_period_scores(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_period_scores", _shape_period_scores(raw)), return_as_pandas)


def parse_kbl_pbp(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/match/{gmkey}/text-cast`` -> play-by-play, one row per event in game order.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per event: period, clock, team code, player names (Korean and English; NO ``pcode``), the action code with its English and Korean labels, ``points`` for a made shot, and the substitution sub-type. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_pbp, parse_kbl_pbp
            raw = kbl_pbp("S47G01N49", return_parsed=False)
            parse_kbl_pbp(raw)

        Pandas::

            parse_kbl_pbp(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_pbp", _shape_pbp(raw)), return_as_pandas)


def parse_kbl_player_box(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/match/{gmkey}/player-stat`` -> the player box score with advanced rates.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per player of both teams: identity, ``started``, the counting stats (``fg`` is TWO-point field goals made, ``fgt`` all field goals), minutes (``play_min`` + ``play_sec``, and ``minutes_played``) and the vendor's rates (``per``, ``offrtg``, ``usg_rt`` ...). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_player_box, parse_kbl_player_box
            raw = kbl_player_box("S47G01N49", return_parsed=False)
            parse_kbl_player_box(raw)

        Pandas::

            parse_kbl_player_box(raw, return_as_pandas=True)
    """
    return _finish(
        _frame("parse_kbl_player_box", (lambda raw: _shape_box("parse_kbl_player_box", raw))(raw)), return_as_pandas
    )


def parse_kbl_team_box(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/match/{gmkey}/team-record`` -> the team box score with team-only counts.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team: the counting stats and rates plus team-only counts (team rebounds ``team_r``, ``tfb``, ``tto``, second-chance, bench and fast-break points, longest run, largest lead). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_team_box, parse_kbl_team_box
            raw = kbl_team_box("S47G01N49", return_parsed=False)
            parse_kbl_team_box(raw)

        Pandas::

            parse_kbl_team_box(raw, return_as_pandas=True)
    """
    return _finish(
        _frame("parse_kbl_team_box", (lambda raw: _shape_box("parse_kbl_team_box", raw))(raw)), return_as_pandas
    )


def parse_kbl_shots(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/match/{gmkey}/match-chart`` -> every charted shot with its court position.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per shot (2006-07 on; none before): shooter, team, period, ``x`` / ``y`` in pixels on the site's 726 x 412 full-court image (origin top-left, x along the court, y down), ``made`` and ``basket`` (``'1'`` right-hand, ``'2'`` left-hand; teams switch at half-time). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_shots, parse_kbl_shots
            raw = kbl_shots("S47G01N49", return_parsed=False)
            parse_kbl_shots(raw)

        Pandas::

            parse_kbl_shots(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_shots", _shape_shots(raw)), return_as_pandas)


def parse_kbl_score_track(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/match/{gmkey}/match-chart`` -> the lead tracker and score chart.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per period and checkpoint (eight per period, evenly spaced is assumed and unverified, the last at the period's end) with ``home_margin`` and both cumulative scores. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_score_track, parse_kbl_score_track
            raw = kbl_score_track("S47G01N49", return_parsed=False)
            parse_kbl_score_track(raw)

        Pandas::

            parse_kbl_score_track(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_score_track", _shape_score_track(raw)), return_as_pandas)


def parse_kbl_game_leaders(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/match/{gmkey}/match-chart`` -> each team's leader per category.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per category (``part``) and side with the leader's ``pcode``, names, ``value`` and minutes. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_game_leaders, parse_kbl_game_leaders
            raw = kbl_game_leaders("S47G01N49", return_parsed=False)
            parse_kbl_game_leaders(raw)

        Pandas::

            parse_kbl_game_leaders(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_game_leaders", _shape_game_leaders(raw)), return_as_pandas)


def parse_kbl_player_totals(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/leagues/{glkey}/stats/players`` -> season player totals for one competition.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per player: identity, ``game_count``, ``start_count`` (meaning unverified) and season totals of the counting stats. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_player_totals, parse_kbl_player_totals
            raw = kbl_player_totals(season=2025, return_parsed=False)
            parse_kbl_player_totals(raw)

        Pandas::

            parse_kbl_player_totals(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_player_totals", _shape_player_totals(raw)), return_as_pandas)


def parse_kbl_standings(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/league/rank/{glkey}`` -> the standings of one competition with team season totals.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team: rank, record, home/away splits, streaks, last five results, and the team's season totals (``ast``, ``defr``, ``offr``, ``to`` ... renamed from the route's capitalised keys). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_standings, parse_kbl_standings
            raw = kbl_standings(season=2025, return_parsed=False)
            parse_kbl_standings(raw)

        Pandas::

            parse_kbl_standings(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_standings", _shape_standings(raw)), return_as_pandas)


def parse_kbl_team_tracks(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/league/rank/{glkey}/team-tracks`` -> each team's standing after every game.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team and game number with the team's ``rank`` after that game. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_team_tracks, parse_kbl_team_tracks
            raw = kbl_team_tracks(season=2025, return_parsed=False)
            parse_kbl_team_tracks(raw)

        Pandas::

            parse_kbl_team_tracks(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_team_tracks", _shape_team_tracks(raw)), return_as_pandas)


def parse_kbl_rounds(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/league/rank/{glkey}/team-tracks`` -> the season's round calendar.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per round with its first and last date. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_rounds, parse_kbl_rounds
            raw = kbl_rounds(season=2025, return_parsed=False)
            parse_kbl_rounds(raw)

        Pandas::

            parse_kbl_rounds(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_rounds", _shape_rounds(raw)), return_as_pandas)


def parse_kbl_playoff_seeds(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/league/playoff`` -> the regular-season finish that seeds the playoffs.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per seeded team (the top six) with ``seed`` and names. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_playoff_seeds, parse_kbl_playoff_seeds
            raw = kbl_playoff_seeds(season=2025, return_parsed=False)
            parse_kbl_playoff_seeds(raw)

        Pandas::

            parse_kbl_playoff_seeds(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_playoff_seeds", _shape_playoff_seeds(raw)), return_as_pandas)


def parse_kbl_playoff_series(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/league/playoff`` -> the playoff series, one row per team and series.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team in each series (quarterfinal, semifinal, final) with ``wins`` and ``losses`` in the series. ``bracket_format`` is ``old`` (seasons through 2023-24, series from the per-team results) or ``new`` (2024-25 on, from the bracket numbers; ``team_code`` is null for a series whose seed slots do not match its games). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_playoff_series, parse_kbl_playoff_series
            raw = kbl_playoff_series(season=2025, return_parsed=False)
            parse_kbl_playoff_series(raw)

        Pandas::

            parse_kbl_playoff_series(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_playoff_series", _shape_playoff_series(raw)), return_as_pandas)


def parse_kbl_playoff_games(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/league/playoff/new`` -> the playoff games of the bracket (2024-25 on).

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per game with the series position and the score; zero rows for a season in the old bracket format, which lists no games. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_playoff_games, parse_kbl_playoff_games
            raw = kbl_playoff_games(season=2025, return_parsed=False)
            parse_kbl_playoff_games(raw)

        Pandas::

            parse_kbl_playoff_games(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_playoff_games", _shape_playoff_games(raw)), return_as_pandas)


def parse_kbl_round_mvps(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/league/round-mvp/{seasonCode}`` -> the round MVPs of a season.

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per round with the MVP's ``pcode``, names, games, ``total_seconds`` played and totals. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_round_mvps, parse_kbl_round_mvps
            raw = kbl_round_mvps(season=2025, return_parsed=False)
            parse_kbl_round_mvps(raw)

        Pandas::

            parse_kbl_round_mvps(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_round_mvps", _shape_round_mvps(raw)), return_as_pandas)


def parse_kbl_current_standings(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/league/rank/team`` -> the current standings (the active season; no parameter).

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team: rank, wins and losses as counts, games behind, streaks, home/away records and the last five results. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_current_standings, parse_kbl_current_standings
            raw = kbl_current_standings(return_parsed=False)
            parse_kbl_current_standings(raw)

        Pandas::

            parse_kbl_current_standings(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_current_standings", _shape_current_standings(raw)), return_as_pandas)


def parse_kbl_season_leaders(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/league/season-leader`` -> the current season's leaders per category (no parameter).

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per category (``part``) and rank with the player and ``value`` (a per-game average, or a percentage for the ``*R`` categories). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_season_leaders, parse_kbl_season_leaders
            raw = kbl_season_leaders(return_parsed=False)
            parse_kbl_season_leaders(raw)

        Pandas::

            parse_kbl_season_leaders(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_season_leaders", _shape_season_leaders(raw)), return_as_pandas)


def parse_kbl_players(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/players`` -> the player directory (current and former players).

    Args:
        raw: the body ``kbl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per player: ``pcode``, names, the current or last team, jersey number, position and type. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.kbl import kbl_players, parse_kbl_players
            raw = kbl_players(return_parsed=False)
            parse_kbl_players(raw)

        Pandas::

            parse_kbl_players(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_kbl_players", _shape_players(raw)), return_as_pandas)
