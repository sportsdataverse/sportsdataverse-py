"""Parsers for the generated ``bleague`` wrappers (Japan B.LEAGUE: B1, B2, B3).

Two kinds of body feed this family. A **game context** (the S3 JSON, or the copy a ``game_detail`` page embeds)
is one object: ``Game`` (header), ``PlayByPlays``, ``HomeBoxscores``, ``AwayBoxscores``, ``Summaries`` and, on the
``v1`` feed only, ``Leaders`` and ``Points``. The ``schedule/`` and ``standings/`` pages are HTML, which this
module reduces to the same records the refs captures bank (sdv-internal-refs ``bleague/tools/capture.py``) with a
stdlib ``html.parser`` DOM that refuses layout drift (:class:`_NotParsed`) instead of guessing.

Each public ``parse_bleague_*`` takes what the wrapper returns with ``return_parsed=False`` (the context, or the
page record, with the request stamped under ``_request`` by ``bleague_runtime``) and follows the package parser
contract: a polars frame by default, pandas with ``return_as_pandas=True``, snake_case columns, and a zero-row
frame with the documented columns and dtypes, never an exception, for an empty or unrecognised input.

**Two feeds, one frame.** The ``v2`` feed (B1/B2 from 2020-21) and the ``v1`` feed (B3, B1/B2 2018-2020, and the
2016-17 / 2017-18 embed) differ in id types (strings vs integers), date formats, placeholders and a few codes.
Every parser normalises them at the boundary: **dtypes are pinned by column name** (:data:`SPECS`), never sampled,
and a ``feed`` column (``v1`` / ``v2``) says which one a row came from. Where a code differs between feeds (the
team-rebound codes 18 / 19) the English label in ``action`` follows the feed.

**Id key spaces** (one dtype each, cast at the parser boundary; none is ever stringified from a float):

* ``schedule_key`` (Int64): B.LEAGUE games, ONE global integer sequence across B1, B2, B3 and every season.
* ``team_id`` (Utf8): B.LEAGUE clubs, stable across seasons AND club renames (703 = Tochigi Brex in 2016-17 and
  Utsunomiya Brex in 2025-26). A string on ``v2`` and in the HTML records, an integer on ``v1``: both become text.
* ``player_id`` / ``player2_id`` (Utf8): B.LEAGUE persons, stable across seasons and clubs. On the ``v1`` team row
  of a box score it is the head coach. Empty strings (team and totals rows) become null.
* ``convention_key`` (Utf8): one competition-season; key on (``feed``, ``convention_key``) since the feeds may
  number independently. ``event`` (Int64) is the competition type of the schedule page and of ``v1`` headers.

**Season key.** The site's ``Year`` is the season START year; every frame also carries ``season``, the END year
(2026 = 2025-26), which is ``Year + 1``.

**Time.** ``recorded_at`` / ``game_datetime`` are UTC instants. The feed's wall clock is JST (UTC+9); the schedule
page's ``tip_time`` is a bare JST ``HH:MM``.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball.bleague.bleague_codes import (
    SHOT_ACTIONS,
    action_name,
    area_name,
    qualifier_name,
)

__all__ = [
    "parse_bleague_game_info",
    "parse_bleague_leaders",
    "parse_bleague_pbp",
    "parse_bleague_period_scores",
    "parse_bleague_player_box",
    "parse_bleague_schedule",
    "parse_bleague_schedule_clubs",
    "parse_bleague_schedule_days",
    "parse_bleague_schedule_events",
    "parse_bleague_score_track",
    "parse_bleague_shots",
    "parse_bleague_standings",
    "parse_bleague_team_box",
    "parse_bleague_team_summary",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Spec = Dict[str, pl.DataType]

#: Where the runtime stamps the request (``bleague_runtime.REQUEST_KEY``).
_REQUEST = "_request"

INT, F, S, B, D = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date
DT = pl.Datetime("ms", "UTC")

#: Top-level keys of each feed's game context (drift is refused by the runtime).
FEED_KEYS: Dict[str, frozenset] = {
    "v2": frozenset({"PlayByPlays", "HomeBoxscores", "AwayBoxscores", "Summaries", "Game"}),
    "v1": frozenset({"PlayByPlays", "HomeBoxscores", "AwayBoxscores", "Summaries", "Game", "Leaders", "Points"}),
}
#: The S3 feed (and the embed's ``admin_name``) of each feed label.
FEED_NAMES: Dict[str, str] = {"v2": "v2_genius_contexts", "v1": "v1_contexts"}

PAGE_SIZE = 20  # the schedule pager's page; no league day has that many games (B1: 13 at most)


class _NotParsed(Exception):
    """The page is not the data asked for: a missing selector or embed, header drift, a row of the wrong width,
    a truncated (paged) list, or another scope (season, tab, event, day).

    ``empty`` marks a page that is the right one but holds no games; the runtime raises it as ``NoDataError``.
    Every other refusal is an ``AssetFetchError``.
    """

    def __init__(self, msg: str, *, empty: bool = False) -> None:
        super().__init__(msg)
        self.empty = empty


# ------------------------------------------------------------------------------------------ casts
def _same(a: Any, b: Any) -> bool:
    """Type-strict equality (``1 != "1"``, ``True != 1``)."""
    return type(a) is type(b) and a == b


def _i(v: Any) -> Optional[int]:
    """A count / key from an integer or an integer string (``""`` and anything else give ``None``). Never via float."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+\s*", v):
        return int(v)
    return None


def _f(v: Any) -> Optional[float]:
    """A number from a number or a decimal string (``""`` gives ``None``)."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str) and re.fullmatch(r"\s*[+-]?\d+(?:\.\d+)?\s*", v):
        return float(v)
    return None


def _s(v: Any) -> Optional[str]:
    """A stripped non-empty string, else ``None``. Integers are stringified, floats and booleans are not text."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        t = v.strip()
        return t or None
    return None


def _idstr(v: Any) -> Optional[str]:
    """An id as text: an integer or a non-empty string. A float (``706.0``) is NOT an id and gives ``None``."""
    return _s(v)


def _b(v: Any) -> Optional[bool]:
    """A flag from a boolean or 0 / 1 (``""`` and anything else give ``None``)."""
    if isinstance(v, bool):
        return v
    if isinstance(v, int) and v in (0, 1):
        return bool(v)
    return None


_NET_DATE = re.compile(r"/Date\((-?\d+)(?:[+-]\d{4})?\)/")
_EPOCH_S = re.compile(r"\d{9,11}")
_STAMP = re.compile(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.\d+)?")


def _dt(v: Any) -> Optional[datetime]:
    """A UTC instant from the feeds' three date forms: ``/Date(ms+0900)/`` (v1 / v2 header), epoch SECONDS as a
    string (the 2016-17 embed) and ``YYYY-MM-DD HH:MM:SS`` (v2 rows, already UTC); anything else gives ``None``."""
    if isinstance(v, bool) or not isinstance(v, str):
        return None
    t = v.strip()
    m = _NET_DATE.fullmatch(t)
    if m:
        return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc).replace(microsecond=0)
    if _EPOCH_S.fullmatch(t):
        return datetime.fromtimestamp(int(t), tz=timezone.utc)
    m = _STAMP.fullmatch(t)
    if m:
        return datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}").replace(tzinfo=timezone.utc)
    return None


def _mmss(v: Any) -> Optional[int]:
    """Seconds in an ``mm:ss`` clock (``"8:50"`` = 530, ``"200:00"`` = 12000); ``"DNP"`` and ``""`` give ``None``."""
    if isinstance(v, str):
        m = re.fullmatch(r"\s*(\d{1,3}):(\d{2})\s*", v)
        if m:
            return int(m.group(1)) * 60 + int(m.group(2))
    return None


def _is_id_col(name: str) -> bool:
    return name.endswith("_id") or name in {"convention_key", "stadium_cd"}


def _cast(name: str, dtype: pl.DataType, v: Any) -> Any:
    if dtype == INT:
        return _i(v)
    if dtype == F:
        return _f(v)
    if dtype == B:
        return _b(v)
    if dtype == DT:
        return _dt(v)
    return _idstr(v) if _is_id_col(name) else _s(v)


def _frame(rows: List[Dict[str, Any]], spec: Spec, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    """``rows`` as a frame with exactly ``spec``'s columns and dtypes (a zero-row frame when there are none).

    ``columns`` is the wrapper's documented column list; a name the spec lacks is added as a null string column.
    """
    full: Dict[str, pl.DataType] = dict(spec)
    for c in columns or ():
        full.setdefault(c, S)
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in full}, schema=full)
    return df.to_pandas() if as_pandas else df


def _row(src: Dict[str, Any], table: Sequence[Tuple[str, str, pl.DataType]]) -> Dict[str, Any]:
    return {out: _cast(out, dt, src.get(k)) for k, out, dt in table}


def _req(raw: Any) -> Dict[str, Any]:
    r = raw.get(_REQUEST) if isinstance(raw, dict) else None
    return r if isinstance(r, dict) else {}


def _dicts(v: Any) -> List[Dict[str, Any]]:
    return [r for r in v if isinstance(r, dict)] if isinstance(v, list) else []


# ----------------------------------------------------------------------------------- game context
def _game(raw: Any) -> Dict[str, Any]:
    g = raw.get("Game") if isinstance(raw, dict) else None
    return g if isinstance(g, dict) else {}


def _feed(raw: Any) -> Optional[str]:
    """``"v1"`` / ``"v2"``: what the runtime stamped, else inferred (v2 sends team ids as strings, v1 as integers)."""
    f = _req(raw).get("feed")
    if f in ("v1", "v2"):
        return f
    tid = _game(raw).get("HomeTeamID")
    return "v2" if isinstance(tid, str) else "v1" if isinstance(tid, int) and not isinstance(tid, bool) else None


def _key(raw: Any) -> Optional[int]:
    return _i(_game(raw).get("ScheduleKey")) if _game(raw) else _i(_req(raw).get("schedule_key"))


def _side_ids(raw: Any) -> Dict[str, Optional[str]]:
    g = _game(raw)
    return {"home": _idstr(g.get("HomeTeamID")), "away": _idstr(g.get("AwayTeamID"))}


def _season(year: Any) -> Optional[int]:
    y = _i(year)
    return y + 1 if y is not None else None


def _base(raw: Any) -> Dict[str, Any]:
    """The columns every game frame starts with."""
    g = _game(raw)
    return {"schedule_key": _key(raw), "season": _season(g.get("Year")), "feed": _feed(raw)}


_GAME_TEXT = (
    ("ConventionNameJ", "convention_name_j"),
    ("ConventionNameE", "convention_name_e"),
    ("ConventionTitleJ", "convention_title_j"),
    ("ConventionTitleE", "convention_title_e"),
    ("StadiumNameJ", "stadium_name_j"),
    ("StadiumNameE", "stadium_name_e"),
    ("Court", "court"),
    ("Report", "report"),
    ("Weather", "weather"),
    ("ScheduleClassNameJ", "schedule_class_name_j"),
    ("ScheduleClassNameE", "schedule_class_name_e"),
    ("ScheduleClassShortNameJ", "schedule_class_short_name_j"),
    ("ScheduleClassShortNameE", "schedule_class_short_name_e"),
    ("CommissionerNameJ", "commissioner_name_j"),
    ("CommissionerNameE", "commissioner_name_e"),
    ("RefereeNameJ", "referee_name_j"),
    ("RefereeNameE", "referee_name_e"),
    ("SubRefereeNameJ1", "sub_referee_name_j1"),
    ("SubRefereeNameE1", "sub_referee_name_e1"),
    ("SubRefereeNameJ2", "sub_referee_name_j2"),
    ("SubRefereeNameE2", "sub_referee_name_e2"),
)
_GAME_TEAM_FIELDS = (
    ("ID", "team_id", S),
    ("NameJ", "team_name_j", S),
    ("NameE", "team_name_e", S),
    ("ShortNameJ", "team_short_name_j", S),
    ("ShortNameE", "team_short_name_e", S),
    ("SubNameJ", "team_sub_name_j", S),
    ("SubNameE", "team_sub_name_e", S),
    ("Score", "score", INT),
    ("WinningCount", "winning_count", INT),
    ("LosingCount", "losing_count", INT),
)
#: ``(wire key, column, dtype)`` of the ``Game`` header, in column order.
_GAME_TABLE: List[Tuple[str, str, pl.DataType]] = (
    [
        ("ScheduleKey", "schedule_key", INT),
        ("Year", "year", INT),
        ("Code", "code", INT),
        ("Event", "event", INT),
        ("ConventionKey", "convention_key", S),
        ("ConventionClass", "convention_class", INT),
        ("Setu", "round", INT),
        ("GameNo", "game_no", INT),
        ("ScheduleState", "schedule_state", INT),
        ("ScheduleConference", "schedule_conference", INT),
        ("InterleagueFlg", "interleague", B),
        ("StadiumCD", "stadium_cd", S),
        ("Attendance", "attendance", INT),
        ("GameDateTime", "game_datetime", DT),
        ("GameStartTime", "game_start_time", DT),
        ("GameEndTime", "game_end_time", DT),
        ("MaxPeriod", "max_period", INT),
        ("GameCurrentPeriod", "current_period", INT),
        ("GameRestTime", "rest_time", INT),
        ("GameEndedFlg", "game_ended", B),
        ("RecordFixedFlg", "record_fixed", B),
        ("BoxscoreExistsFlg", "box_exists", B),
        ("PlayByPlayExistsFlg", "pbp_exists", B),
        ("PictureExistsFlg", "picture_exists", B),
        ("ReportExistsFlg", "report_exists", B),
        ("HomeTeamWinFlg", "home_win", B),
        ("CommissionerID", "commissioner_id", S),
        ("RefereeID", "referee_id", S),
        ("SubRefereeID1", "sub_referee_id1", S),
        ("SubRefereeID2", "sub_referee_id2", S),
        ("Temperature", "temperature", F),
        ("Humidity", "humidity", F),
    ]
    + [(k, o, S) for k, o in _GAME_TEXT]
    + [(f"{side.title()}Team{k}", f"{side}_{o}", dt) for side in ("home", "away") for k, o, dt in _GAME_TEAM_FIELDS]
    + [
        (f"{side.title()}TeamScore{p:02d}", f"{side}_score_p{p}", INT)
        for side in ("home", "away")
        for p in range(1, 15)
    ]
)
_GAME_SPEC: Spec = {"season": INT, "feed": S, **{o: dt for _k, o, dt in _GAME_TABLE}}
_GAME_ORDER = ["schedule_key", "season", "feed"]
_GAME_SPEC = {c: _GAME_SPEC[c] for c in _GAME_ORDER + [c for c in _GAME_SPEC if c not in _GAME_ORDER]}


def parse_bleague_game_info(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Game header of one B.LEAGUE game: competition, round, venue, officials, tip-off, both teams and the scores.

    Args:
        raw: the context ``bleague_game_info(..., return_parsed=False)`` returns (any feed).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row. ``year`` is the season START year and ``season`` the END year (``year + 1``). Team ids are text
        (the ``v1`` feed sends integers, ``v2`` strings). ``home_score_p1`` ... ``p14`` are the points scored IN each
        period (the ``v1`` header carries 14 slots, ``v2`` four: a period beyond the feed's slots is null here, use
        ``bleague_period_scores`` for the played periods). The ``v1`` header has many columns ``v2`` lacks (round
        number ``game_no``, ``event``, winning / losing streaks, weather, report text); they are null on ``v2``.
        ``temperature`` and ``humidity`` were null in every observed body, so their dtype is an assumption.
        A body without a ``Game`` object gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_game_info

            info = bleague_game_info(schedule_key=504734)
    """
    g = _game(raw)
    if not g:
        return _frame([], _GAME_SPEC, columns, return_as_pandas)
    row = _row(g, _GAME_TABLE)
    row["season"] = _season(g.get("Year"))
    row["feed"] = _feed(raw)
    row["schedule_key"] = _key(raw)
    return _frame([row], _GAME_SPEC, columns, return_as_pandas)


# --------------------------------------------------------------------------- period scores
_PERIOD_SPEC: Spec = {
    "schedule_key": INT,
    "season": INT,
    "feed": S,
    "period": INT,
    "team_side": S,
    "team_id": S,
    "points": INT,
}


def parse_bleague_period_scores(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Points each team scored in each period of one game, overtime included (from the ``Game`` header).

    Args:
        raw: the context ``bleague_period_scores(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per (period, team side): ``points`` are scored IN the period (not cumulative). Periods run 1 to the
        game's ``MaxPeriod`` (4 when the header has none), and a period whose score slot is missing is skipped.
        A body without a ``Game`` object gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_period_scores

            periods = bleague_period_scores(schedule_key=504734)
    """
    g = _game(raw)
    if not g:
        return _frame([], _PERIOD_SPEC, columns, return_as_pandas)
    last = max(_i(g.get("MaxPeriod")) or 4, 4)
    ids = _side_ids(raw)
    rows = []
    for p in range(1, min(last, 14) + 1):
        for side in ("home", "away"):
            pts = _i(g.get(f"{side.title()}TeamScore{p:02d}"))
            if pts is not None:
                rows.append({**_base(raw), "period": p, "team_side": side, "team_id": ids[side], "points": pts})
    return _frame(rows, _PERIOD_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------------ box scores
#: ``(wire key, column, dtype)`` shared by the player and team box frames.
_BOX_STATS: List[Tuple[str, str, pl.DataType]] = [
    ("Point", "points", INT),
    ("PT3M", "fg3_made", INT),
    ("PT3A", "fg3_attempted", INT),
    ("PT2M", "fg2_made", INT),
    ("PT2A", "fg2_attempted", INT),
    ("FTM", "ft_made", INT),
    ("FTA", "ft_attempted", INT),
    ("FOUL", "fouls", INT),
    ("FOULON", "fouls_drawn", INT),
    ("RB_OFF", "reb_off", INT),
    ("RB_DEF", "reb_def", INT),
    ("RB_TOT", "reb", INT),
    ("TO", "turnovers", INT),
    ("AS", "assists", INT),
    ("ST", "steals", INT),
    ("BS", "blocks", INT),
    ("BSON", "blocks_received", INT),
    ("PTFB", "ptfb", INT),
    ("PT2IN", "pt2in", INT),
    ("PT2ND", "pt2nd", INT),
    ("PTPFT", "ptpft", INT),
    ("PT2DUNK", "dunks", INT),
    ("EFF", "efficiency", INT),
    ("PLUSMINUS", "plus_minus", INT),
    ("AST_TO", "ast_to", F),
    ("EFG", "efg", F),
    ("TS", "ts", F),
    ("USG", "usg", F),
    ("OFFRTG", "off_rtg", F),
    ("DEFRTG", "def_rtg", F),
    ("NETRTG", "net_rtg", F),
    ("POSS", "poss", F),
]
_BOX_IDENT: List[Tuple[str, str, pl.DataType]] = [
    ("Category", "category", INT),
    ("PeriodCategory", "period_category", INT),
    ("TeamNameJ", "team_name_j", S),
    ("TeamNameE", "team_name_e", S),
    ("PlayerID", "player_id", S),
    ("PlayerNo", "player_no", S),
    ("PlayerNameJ", "player_name_j", S),
    ("PlayerNameE", "player_name_e", S),
    ("PlayTime", "play_time", S),
    ("PlayingFlg", "on_court", B),
    ("No", "display_order", INT),
]
_BOX_SPEC: Spec = {
    "schedule_key": INT,
    "season": INT,
    "feed": S,
    "team_side": S,
    "team_id": S,
    **{o: dt for _k, o, dt in _BOX_IDENT},
    "starter": B,
    "seconds_played": INT,
    **{o: dt for _k, o, dt in _BOX_STATS},
}


def _box_rows(raw: Any, categories: Tuple[int, ...]) -> List[Dict[str, Any]]:
    ids = _side_ids(raw)
    out: List[Dict[str, Any]] = []
    for side, name in (("home", "HomeBoxscores"), ("away", "AwayBoxscores")):
        for r in _dicts(raw.get(name) if isinstance(raw, dict) else None):
            cat = _i(r.get("Category"))
            if cat not in categories:
                continue
            row = {**_base(raw), "team_side": side, "team_id": _idstr(r.get("TeamID")) or ids[side]}
            row.update(_row(r, _BOX_IDENT))
            row.update(_row(r, _BOX_STATS))
            row["starter"] = (r.get("StartingFlg") in (1, True, "1")) if cat == 1 else None
            row["seconds_played"] = _mmss(r.get("PlayTime"))
            out.append(row)
    return out


def parse_bleague_player_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Player box score of one game: one row per (player, period category), both teams.

    Args:
        raw: the context ``bleague_player_box(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Rows with ``category`` 1 (a player), one per ``period_category`` (1-4 quarters, 15 / 16 halves, 18 the whole
        game; overtime categories are unverified). ``seconds_played`` is ``play_time`` in seconds (``DNP`` = null).
        The ``v2`` feed sends ``""`` as a placeholder (not 0) in some cells; those are null here. The efficiency
        definition differs by feed (see ``efficiency``), the advanced columns (``plus_minus``, ``ast_to``, ``efg``,
        ``ts``, ``usg``) exist from 2022-23 on ``v2`` only, and ``dunks`` / ``ptpft`` exist on ``v1`` only. A body
        without box rows gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_player_box

            box = bleague_player_box(schedule_key=504734)
    """
    return _frame(_box_rows(raw, (1,)), _BOX_SPEC, columns, return_as_pandas)


def parse_bleague_team_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team rows of the box score of one game: the team / coaches row and the team totals, per period category.

    Args:
        raw: the context ``bleague_team_box(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Rows with ``category`` 2 (team-credited stats; on ``v1`` the row names the head coach in ``player_id`` and
        ``player_name_*``) and 3 (the team totals, whose ``play_time`` is five times the period length: ``200:00``
        for the game), one per ``period_category``. The same columns as ``bleague_player_box``. The ``v2`` totals
        row carries no ``TeamID``; ``team_id`` is filled from the game header. A body without box rows gives a
        zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_team_box

            teams = bleague_team_box(schedule_key=504734)
    """
    return _frame(_box_rows(raw, (2, 3)), _BOX_SPEC, columns, return_as_pandas)


# --------------------------------------------------------------------------- team summary
_SUMMARY_FIELDS: List[Tuple[str, str, pl.DataType]] = [
    ("ID", "team_id", S),
    ("NameJ", "team_name_j", S),
    ("NameE", "team_name_e", S),
    ("PTM", "fg_made", INT),
    ("PTA", "fg_attempted", INT),
    ("PTR", "fg_pct", F),
    ("PT2M", "fg2_made", INT),
    ("PT2A", "fg2_attempted", INT),
    ("PT2R", "fg2_pct", F),
    ("PT3M", "fg3_made", INT),
    ("PT3A", "fg3_attempted", INT),
    ("PT3R", "fg3_pct", F),
    ("FTM", "ft_made", INT),
    ("FTA", "ft_attempted", INT),
    ("FTR", "ft_pct", F),
    ("RB_OFF", "reb_off", INT),
    ("RB_DEF", "reb_def", INT),
    ("RB_TOT", "reb", INT),
    ("AS", "assists", INT),
    ("TO", "turnovers", INT),
    ("ST", "steals", INT),
    ("BS", "blocks", INT),
    ("BSON", "blocks_received", INT),
    ("FOUL", "fouls", INT),
    ("FOULON", "fouls_drawn", INT),
    ("PT2IN", "paint_points", INT),
    ("PTPFT", "points_off_turnover", INT),
    ("PT2ND", "second_chance_points", INT),
    ("PTFB", "fast_break_points", INT),
    ("MaxDifference", "max_lead", INT),
    ("LeadLast", "lead_last", INT),
    ("LeadLastHome", "lead_last_home", INT),
    ("LeadLastAway", "lead_last_away", INT),
]
_SUMMARY_SPEC: Spec = {
    "schedule_key": INT,
    "season": INT,
    "feed": S,
    "period_category": INT,
    "team_side": S,
    **{o: dt for _k, o, dt in _SUMMARY_FIELDS},
    "lead_changes": INT,
    "ties": INT,
}


def parse_bleague_team_summary(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team summary of one game per period category, in long form (one row per team side).

    Args:
        raw: the context ``bleague_team_summary(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        The feed's ``Summaries`` rows, which hold both teams side by side in ``Home*`` / ``Away*`` columns, split
        into two rows (``team_side`` home / away) with the prefix removed. ``period_category`` is 1-4 (quarters),
        15 / 16 (halves) or 18 (the whole game). ``paint_points``, ``points_off_turnover``, ``second_chance_points``
        and ``fast_break_points`` are POINTS here (unlike the box score's counts of the same wire names).
        ``lead_changes`` and ``ties`` belong to the whole row and repeat on both sides. A body without ``Summaries``
        gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_team_summary

            summary = bleague_team_summary(schedule_key=504734)
    """
    rows = []
    for r in _dicts(raw.get("Summaries") if isinstance(raw, dict) else None):
        for side in ("home", "away"):
            pre = f"{side.title()}Team"
            row = {
                **_base(raw),
                "period_category": _i(r.get("PeriodCategory")),
                "team_side": side,
                "lead_changes": _i(r.get("LeadCount")),
                "ties": _i(r.get("TieCount")),
            }
            row.update({o: _cast(o, dt, r.get(pre + k)) for k, o, dt in _SUMMARY_FIELDS})
            rows.append(row)
    return _frame(rows, _SUMMARY_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------------ play-by-play
_PBP_SPEC: Spec = {
    "schedule_key": INT,
    "season": INT,
    "feed": S,
    "period": INT,
    "event_no": INT,
    "action_code": INT,
    "action": S,
    "qualifier_code2": INT,
    "qualifier2": S,
    "qualifier_code3": INT,
    "qualifier3": S,
    "team_side": S,
    "team_id": S,
    "team_name_j": S,
    "player_id": S,
    "player_no": S,
    "player_name_j": S,
    "player2_id": S,
    "player2_no": S,
    "player2_name_j": S,
    "rest_time": S,
    "clock_seconds": INT,
    "score": S,
    "home_score": INT,
    "away_score": INT,
    "period_end": B,
    "game_end": B,
    "x": F,
    "y": F,
    "area_code": INT,
    "area": S,
    "success": B,
    "side": S,
    "play_text": S,
    "recorded_at": DT,
    "edited_at": DT,
    "code": INT,
}


def _pbp_rows(raw: Any) -> List[Dict[str, Any]]:
    feed = _feed(raw)
    out = []
    for r in _dicts(raw.get("PlayByPlays") if isinstance(raw, dict) else None):
        a1, a2, a3, area = _i(r.get("ActionCD1")), _i(r.get("ActionCD2")), _i(r.get("ActionCD3")), _i(r.get("AreaCD"))
        away_home = _i(r.get("HomeAway"))
        score = _s(r.get("Score"))
        m = re.fullmatch(r"(\d+)-(\d+)", score or "")
        out.append(
            {
                **_base(raw),
                "period": _i(r.get("Period")),
                "event_no": _i(r.get("No")),
                "action_code": a1,
                "action": action_name(a1, feed),
                "qualifier_code2": a2,
                "qualifier2": qualifier_name(a2),
                "qualifier_code3": a3,
                "qualifier3": qualifier_name(a3),
                "team_side": "home" if away_home == 1 else "away" if away_home == 2 else None,
                "team_id": _idstr(r.get("TeamID")),
                "team_name_j": _s(r.get("TeamNameJ")),
                "player_id": _idstr(r.get("PlayerID1")),
                "player_no": _s(r.get("PlayerNo1")),
                "player_name_j": _s(r.get("PlayerNameJ1")),
                "player2_id": _idstr(r.get("PlayerID2")),
                "player2_no": _s(r.get("PlayerNo2")),
                "player2_name_j": _s(r.get("PlayerNameJ2")),
                "rest_time": _s(r.get("RestTime")),
                "clock_seconds": _mmss(r.get("RestTime")),
                "score": score,
                "home_score": int(m.group(1)) if m else None,
                "away_score": int(m.group(2)) if m else None,
                "period_end": _b(r.get("PeriodEndRowFlg")),
                "game_end": _b(r.get("GameEndRowFlg")),
                "x": _f(r.get("X")),
                "y": _f(r.get("Y")),
                "area_code": area,
                "area": area_name(area),
                "success": _b(r.get("Success")),
                "side": _s(r.get("Side")),
                "play_text": _s(r.get("PlayText")),
                "recorded_at": _dt(r.get("RecordDateTime")),
                "edited_at": _dt(r.get("RecordEditDateTime")),
                "code": _i(r.get("Code")),
            }
        )
    return out


def parse_bleague_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Play-by-play of one finished game, in game order, every feed in one shape.

    Args:
        raw: the context ``bleague_pbp(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per logged event, oldest first (``event_no`` ascends; ``v2`` counts in steps of 1000). ``action``,
        ``qualifier2`` and ``qualifier3`` are English slugs for the numeric codes (null for a code no sample
        contains); the team-rebound codes 18 / 19 mean opposite things on the two feeds and ``action`` follows the
        ``feed`` column. A ``v1`` substitution is ONE row (``action`` ``substitution``, ``player_id`` out,
        ``player2_id`` in), a ``v2`` one is two rows (``player_out`` / ``player_in``). ``home_score`` / ``away_score``
        split the running ``score`` (null on rows without one). ``x`` / ``y`` / ``area_code`` / ``success`` exist
        on ``v2`` from 2022-23 and ``side`` from 2025-26 (see ``bleague_shots``). ``recorded_at`` is a UTC instant.
        Game-level rows (start, period, clock) carry null team and player columns. A body without
        ``PlayByPlays`` gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_pbp

            pbp = bleague_pbp(schedule_key=504734)
    """
    return _frame(_pbp_rows(raw), _PBP_SPEC, columns, return_as_pandas)


_SHOT_SPEC: Spec = {
    "schedule_key": INT,
    "season": INT,
    "feed": S,
    "period": INT,
    "event_no": INT,
    "team_side": S,
    "team_id": S,
    "player_id": S,
    "player_no": S,
    "player_name_j": S,
    "shot_type": S,
    "in_paint": B,
    "made": B,
    "qualifier2": S,
    "qualifier3": S,
    "x": F,
    "y": F,
    "area_code": INT,
    "area": S,
    "side": S,
    "rest_time": S,
    "clock_seconds": INT,
    "score": S,
}


def parse_bleague_shots(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Field-goal attempts of one game with their court position (play-by-play rows that carry x / y).

    Args:
        raw: the context ``bleague_shots(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per field-goal attempt (``ActionCD1`` 1-6) with a location, oldest first. ``x`` is the percent
        (0-100) of the court LENGTH from the ``left`` baseline and ``y`` the percent of the WIDTH (the baskets sit
        near x = 5 and 95 at y = 50). Which sideline is y = 0 is unverified, and metres (x / 100 x 28,
        y / 100 x 15) are an inference. ``side`` (``left`` / ``right``) names the basket a shot is charted at, from
        2025-26 only; teams change ends at half-time. ``in_paint`` is null on the B3 feed, whose 2PT codes do not
        split by paint. Locations exist from 2022-23 on the ``v2`` feed only: any other game gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_shots

            shots = bleague_shots(schedule_key=504734)
    """
    rows = []
    for r in _pbp_rows(raw):
        info = SHOT_ACTIONS.get(r["action_code"]) if r["action_code"] is not None else None
        if info is None or r["x"] is None or r["y"] is None:
            continue
        shot_type, made, paint = info
        rows.append({**r, "shot_type": shot_type, "made": made, "in_paint": paint})
    return _frame(rows, _SHOT_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------- v1 leaders, score track
_LEADER_CATEGORIES = {1: "points", 2: "assists", 3: "steals", 4: "blocks", 5: "rebounds", 8: "blocks_received"}
_LEADER_SPEC: Spec = {
    "schedule_key": INT,
    "season": INT,
    "feed": S,
    "period_category": INT,
    "category": INT,
    "category_name": S,
    "rank": INT,
    "tie_order": INT,
    "team_side": S,
    "team_id": S,
    "team_name_j": S,
    "player_id": S,
    "player_no": S,
    "player_name_j": S,
    "count": INT,
    "reb_off_count": INT,
    "reb_def_count": INT,
}


def parse_bleague_leaders(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team leaders of one game per statistic and period category (``v1`` feed only).

    Args:
        raw: the context ``bleague_leaders(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per (period category, category, team, player), ranked within the team (``rank`` 1 = best; ties
        share a rank and ``tie_order`` orders them). ``category`` 1 points, 2 assists, 3 steals, 4 blocks,
        5 rebounds, 8 shots blocked (decoded by matching ``count`` to the box score; other codes are unnamed).
        Only the ``v1`` feed (B3, B1/B2 2018-2020, the 2016-17 embed) publishes leaders: a ``v2`` game gives a
        zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_leaders

            leaders = bleague_leaders(schedule_key=10693)
    """
    ids = _side_ids(raw)
    side_of = {v: k for k, v in ids.items() if v is not None}
    rows = []
    for r in _dicts(raw.get("Leaders") if isinstance(raw, dict) else None):
        cat = _i(r.get("Category"))
        tid = _idstr(r.get("TeamID"))
        rows.append(
            {
                **_base(raw),
                "period_category": _i(r.get("PeriodCategory")),
                "category": cat,
                "category_name": _LEADER_CATEGORIES.get(cat) if cat is not None else None,
                "rank": _i(r.get("Ranking")),
                "tie_order": _i(r.get("No")),
                "team_side": side_of.get(tid) if tid is not None else None,
                "team_id": tid,
                "team_name_j": _s(r.get("TeamNameJ")),
                "player_id": _idstr(r.get("PlayerID")),
                "player_no": _s(r.get("PlayerNo")),
                "player_name_j": _s(r.get("PlayerNameJ")),
                "count": _i(r.get("Count")),
                "reb_off_count": _i(r.get("RB_OFFCount")),
                "reb_def_count": _i(r.get("RB_DEFCount")),
            }
        )
    return _frame(rows, _LEADER_SPEC, columns, return_as_pandas)


_TRACK_SPEC: Spec = {
    "schedule_key": INT,
    "season": INT,
    "feed": S,
    "period": INT,
    "event_index": INT,
    "home_points": INT,
    "away_points": INT,
}


def parse_bleague_score_track(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Running score after each scoring event of one game (``v1`` feed only; the site's score-track chart).

    Args:
        raw: the context ``bleague_score_track(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per scoring event: ``event_index`` is the index of the event (0 = the 0-0 start row, then 1, 2, ...;
        it is the chart's x axis, NOT a clock) and ``home_points`` / ``away_points`` the running totals. Only the
        ``v1`` feed publishes it: a ``v2`` game gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_score_track

            track = bleague_score_track(schedule_key=10693)
    """
    rows = [
        {
            **_base(raw),
            "period": _i(r.get("Period")),
            "event_index": _i(r.get("Time")),
            "home_points": _i(r.get("HomePointsTotal")),
            "away_points": _i(r.get("AwayPointsTotal")),
        }
        for r in _dicts(raw.get("Points") if isinstance(raw, dict) else None)
    ]
    return _frame(rows, _TRACK_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------------- HTML reader
_VOID = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
)


class _Node:
    __slots__ = ("tag", "attrs", "children", "parent")

    def __init__(self, tag: str, attrs: Any, parent: Optional["_Node"]) -> None:
        children: List[Any] = []
        self.tag, self.attrs, self.children, self.parent = tag, dict(attrs), children, parent

    def iter(self) -> Iterator["_Node"]:
        """Descendants in document order (explicit stack: deep pages cannot recurse)."""
        stack = list(reversed(self.children))
        while stack:
            node = stack.pop()
            if isinstance(node, _Node):
                yield node
                stack.extend(reversed(node.children))

    def text(self) -> str:
        parts: List[str] = []
        stack = list(reversed(self.children))
        while stack:
            node = stack.pop()
            if isinstance(node, _Node):
                if node.tag in ("script", "style"):
                    continue
                stack.extend(reversed(node.children))
            else:
                parts.append(node)
        return re.sub(r"\s+", " ", "".join(parts)).strip()

    def classes(self) -> set:
        return set((self.attrs.get("class") or "").split())

    def findall(self, tag: Optional[str] = None, cls: Optional[str] = None, **attrs: Any) -> Iterator["_Node"]:
        for n in self.iter():
            if tag and n.tag != tag:
                continue
            if cls and cls not in n.classes():
                continue
            if any(n.attrs.get(k) != v for k, v in attrs.items()):
                continue
            yield n

    def find(self, tag: Optional[str] = None, cls: Optional[str] = None, **attrs: Any) -> Optional["_Node"]:
        return next(self.findall(tag, cls, **attrs), None)


class _Builder(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = self.cur = _Node("#root", {}, None)

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        node = _Node(tag, attrs, self.cur)
        self.cur.children.append(node)
        if tag not in _VOID:
            self.cur = node

    def handle_startendtag(self, tag: str, attrs: Any) -> None:
        self.cur.children.append(_Node(tag, attrs, self.cur))

    def handle_endtag(self, tag: str) -> None:
        n: Optional[_Node] = self.cur
        while n is not None and n.tag != tag:
            n = n.parent
        if n is not None and n.parent is not None:  # unmatched end tags are ignored
            self.cur = n.parent

    def handle_data(self, data: str) -> None:
        self.cur.children.append(data)


def _dom(html: str) -> _Node:
    b = _Builder()
    b.feed(html)
    b.close()
    return b.root


def _options(select: Optional[_Node]) -> List[Dict[str, Any]]:
    if select is None:
        return []
    return [
        {"value": o.attrs.get("value"), "label": o.text(), "selected": "selected" in o.attrs}
        for o in select.findall("option")
    ]


def _selected(root: _Node, name: str) -> Optional[str]:
    sel = [o["value"] for o in _options(root.find("select", name=name)) if o["selected"]]
    return sel[0] if len(sel) == 1 else None


def _active_tabs(root: _Node) -> set:
    """``data-tab`` of every division tab button marked ``is-active`` (on the button or its ``li``)."""
    out = set()
    for b in root.findall("button", cls="tab-btn"):
        if "data-tab" in b.attrs and (
            "is-active" in b.classes() or (b.parent is not None and "is-active" in b.parent.classes())
        ):
            out.add(b.attrs["data-tab"])
    return out


def _check(what: str, got: Any, want: Any) -> None:
    if not _same(got, want):
        raise _NotParsed(f"out of scope: {what} {got!r}, asked for {want!r}")


# ---- game_detail embed
_EMBED = re.compile(r"_contexts_s3id\.data\s*=\s*")


def _embed_context(html: str) -> Dict[str, Any]:
    """The object a ``game_detail`` page assigns to ``_contexts_s3id.data`` (a CMS topic row wrapping the context).

    Raises:
        _NotParsed: no embed, or one that is not a JSON object.
    """
    m = _EMBED.search(html or "")
    if not m:
        raise _NotParsed("no _contexts_s3id.data embed on the page")
    try:
        rec, _end = json.JSONDecoder().raw_decode(html, m.end())
    except ValueError as e:
        raise _NotParsed(f"embed is not JSON: {e}") from None
    if not isinstance(rec, dict):
        raise _NotParsed("embed is not an object")
    return rec


# ---- schedule
_ROUND = re.compile(r"第(\d+)節")
_MORE = re.compile(r"schedule_b\d_more")


def _paged(root: _Node) -> bool:
    """A 'more' pager: by its element id, its ``schedule_bN_more()`` link, or its label もっと見る (see more)."""
    for n in root.iter():
        if _MORE.search(n.attrs.get("id") or "") or _MORE.search(n.attrs.get("href") or ""):
            return True
        if n.tag in ("a", "button") and n.text() == "もっと見る":
            return True
    return False


def _season_date(year: int, mon: int, day: int) -> str:
    """Seasons run September to June: months 7-12 fall in the START year, 1-6 in the next one."""
    return date(year if mon >= 7 else year + 1, mon, day).isoformat()


def _schedule_record(html: str, tab: int, year: int, event: int, mon: int, day: int) -> Dict[str, Any]:
    """One game day of one competition and division, as the refs capture banks it. Fails closed on any scope drift.

    Raises:
        _NotParsed: another tab / year / event / month / day than asked, a paged list, layout drift, no games.
    """
    root = _dom(html)
    _check("active tab", _active_tabs(root), {str(tab)})
    _check("selected year", _selected(root, "year"), str(year))
    _check("selected event", _selected(root, "event"), str(event))
    _check("selected month", _selected(root, "mon"), f"{mon:02d}")
    current = set()
    for slide in root.findall("div", cls="is-current"):
        a = slide.find("a")
        month = slide.find("span", cls="month")
        if a is not None and "data-day" in a.attrs and month is not None:
            current.add((month.text().rstrip("/"), a.attrs["data-day"]))
    _check("current day (month, day)", current, {(f"{mon:02d}", f"{day:02d}")})
    if _paged(root):
        raise _NotParsed("list is paged (a 'more' button is present): truncated, refused")
    clubs = [
        {"team_id": o["value"], "name": o["label"]} for o in _options(root.find("select", name="club")) if o["value"]
    ]
    by_name = {c["name"]: c["team_id"] for c in clubs}
    lists = list(root.findall("ul", cls="round-list"))
    if [u.attrs.get("id") for u in lists if u.find("li", cls="list-item")] != [f"schedule-b{tab}"]:
        raise _NotParsed(f"game list ids {[u.attrs.get('id') for u in lists]} != ['schedule-b{tab}']")
    games = []
    for li in lists[[u.attrs.get("id") for u in lists].index(f"schedule-b{tab}")].findall("li", cls="list-item"):
        key = _i(li.attrs.get("id"))
        link = li.find("a", cls="data-game")
        href = link.attrs.get("href", "") if link else ""
        if key is None or f"ScheduleKey={key}&" not in href + "&":
            raise _NotParsed(f"game item {li.attrs.get('id')!r} without a matching game_detail link")
        teams = {}
        for side in ("home", "away"):
            span = next((s for s in li.findall("span", cls="team") if side in s.classes()), None)
            name_node = span.find("span", cls="team-name") if span is not None else None
            name = name_node.text() if name_node is not None else None
            if not name or name not in by_name:
                raise _NotParsed(f"game {key}: {side} team {name!r} not in the club select")
            score = li.find("span", cls=f"{side}-score")
            teams[side] = (by_name[name], name, _i(score.text()) if score is not None else None)
        arena = li.find("div", cls="info-arena")
        spans = [s.text() for s in arena.findall("span")] if arena is not None else []
        if len(spans) < 2:
            raise _NotParsed(f"game {key}: info-arena drift {spans!r}")
        place = spans[1].split("|")
        state = li.find("div", cls="info-scorestate")
        rnd = _ROUND.fullmatch(spans[0])
        games.append(
            {
                "schedule_key": key,
                "date": _season_date(year, mon, day),
                "round_label": spans[0],
                "round": int(rnd.group(1)) if rnd else None,
                "home_team_id": teams["home"][0],
                "home_team": teams["home"][1],
                "home_score": teams["home"][2],
                "away_team_id": teams["away"][0],
                "away_team": teams["away"][1],
                "away_score": teams["away"][2],
                "prefecture": place[0].strip() if len(place) == 2 else None,
                "venue": place[-1].strip(),
                "tip_time": spans[2] if len(spans) > 2 and re.fullmatch(r"\d{1,2}:\d{2}", spans[2]) else None,
                "state": (state.text() or None) if state is not None else None,
            }
        )
    if not games:
        raise _NotParsed("schedule day has zero games", empty=True)
    if len(games) >= PAGE_SIZE:  # a full first page may be a paged list whose button was renamed
        raise _NotParsed(f"{len(games)} games: as many as one page of the site's pager, possibly truncated")
    return {
        "tab": tab,
        "year": year,
        "event": event,
        "mon": mon,
        "day": day,
        "season_label": next((o["label"] for o in _options(root.find("select", name="year")) if o["selected"]), None),
        "event_label": next((o["label"] for o in _options(root.find("select", name="event")) if o["selected"]), None),
        "events": [{"event": _i(o["value"]), "label": o["label"]} for o in _options(root.find("select", name="event"))],
        "game_days": sorted(
            {int(a.attrs["data-day"]) for a in root.findall("a", cls="js_click_data") if _i(a.attrs.get("data-day"))}
        ),
        "clubs": clubs,
        "games": games,
    }


# ---- standings
_STANDINGS_HEADER = [
    "順位",
    "クラブ",
    "勝",
    "負",
    "勝率",
    "差",
    "得点",
    "失点",
    "得失点差",
    "ホーム（W-L）",
    "アウェー（W-L）",
    None,
    "連勝/連敗",
    "地区優勝",
    "対戦成績",
]
_LAST_N = {"過去5試合": 5, "過去10試合": 10}
_SECTIONS = {"東地区": "East", "中地区": "Central", "西地区": "West", "ワイルドカード": "Wild card"}


def _num(s: Any) -> Optional[float]:
    s = (s or "").strip()
    return float(s) if re.fullmatch(r"\d*\.\d+|\d+", s) else None


def _standings_record(html: str, year: int, tab: int) -> Dict[str, Any]:
    """Standings of one season and division, as the refs capture banks it. Fails closed on any scope drift.

    Raises:
        _NotParsed: another tab or year than asked, header or row drift, no table.
    """
    root = _dom(html)
    _check("active tab", _active_tabs(root), {str(tab)})
    _check("selected year", _selected(root, f"year-b{tab}"), str(year))
    rows: List[Dict[str, Any]] = []
    section: Optional[str] = None
    for n in root.iter():
        if n.tag == "h3" and "box-container" in n.classes():
            section = n.text()
        elif n.tag == "table" and "table-standings" in n.classes():
            trs = list(n.findall("tr"))
            head = [c.text() for c in trs[0].children if isinstance(c, _Node) and c.tag in ("td", "th")] if trs else []
            if (
                len(head) != len(_STANDINGS_HEADER)
                or any(w is not None and h != w for h, w in zip(head, _STANDINGS_HEADER))
                or head[11] not in _LAST_N
            ):
                raise _NotParsed(f"standings header drift: {head}")
            body = [[c for c in tr.children if isinstance(c, _Node) and c.tag == "td"] for tr in trs[1:]]
            if not body:
                raise _NotParsed(f"standings table {section!r} has zero rows")
            for c in body:
                if len(c) != len(_STANDINGS_HEADER):
                    raise _NotParsed(f"standings row width {len(c)} != {len(_STANDINGS_HEADER)}")
                a = c[1].find("a", cls="team")
                m = re.search(r"TeamID=(\d+)", a.attrs.get("href", "")) if a is not None else None
                full = c[1].find("span", cls="sp-dn")
                short = c[1].find("span", cls="pc-tab-dn")
                if not m:
                    raise _NotParsed("standings row without a club_detail TeamID link")
                t = [x.text() for x in c]
                rows.append(
                    {
                        "section": section,
                        "section_en": _SECTIONS.get(section) if section else None,
                        "rank": _i(t[0]),
                        "team_id": m.group(1),
                        "team": full.text() if full is not None else None,
                        "team_short": short.text() if short is not None else None,
                        "wins": _i(t[2]),
                        "losses": _i(t[3]),
                        "win_pct": _num(t[4]),
                        "games_behind": _num(t[5]),
                        "points_for": _i(t[6]),
                        "points_against": _i(t[7]),
                        "point_diff": _i(t[8]),
                        "home_record": t[9],
                        "away_record": t[10],
                        "last_n": _LAST_N[head[11]],
                        "last_n_record": t[11],
                        "streak": t[12],
                        "division_title": None if t[13] in ("--", "") else t[13],
                    }
                )
            section = None
    if not rows:
        raise _NotParsed("no standings table")
    return {
        "year": year,
        "tab": tab,
        "season_label": next(
            (o["label"] for o in _options(root.find("select", name=f"year-b{tab}")) if o["selected"]), None
        ),
        "rows": rows,
    }


# ---- the page records as frames
def _rec(raw: Any) -> Dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


_SCHEDULE_SPEC: Spec = {
    "season": INT,
    "tab": INT,
    "event": INT,
    "schedule_key": INT,
    "date": D,
    "round_label": S,
    "round": INT,
    "home_team_id": S,
    "home_team": S,
    "home_score": INT,
    "away_team_id": S,
    "away_team": S,
    "away_score": INT,
    "prefecture": S,
    "venue": S,
    "tip_time": S,
    "state": S,
}


def _page_base(rec: Dict[str, Any]) -> Dict[str, Any]:
    y = _i(rec.get("year"))
    return {"season": y + 1 if y is not None else None, "tab": _i(rec.get("tab")), "event": _i(rec.get("event"))}


def parse_bleague_schedule(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Games of one day of one competition and division from the site's schedule page.

    Args:
        raw: the record ``bleague_schedule(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game of that day with its ``schedule_key`` (the key every game wrapper takes), local date,
        round, both clubs (``team_id`` is text; names are the page's short Japanese names), final score, venue and
        tip-off time. ``tab`` is the division (1 = B1, 2 = B2, 3 = B3) and ``event`` the competition type;
        ``season`` is the END year. A record without games gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_schedule

            games = bleague_schedule(season=2026, tab=1, month=10, day=4)
    """
    rec = _rec(raw)
    base = _page_base(rec)
    rows = [
        {**base, **{k: _cast(k, dt, g.get(k)) for k, dt in _SCHEDULE_SPEC.items() if k not in base and k != "date"}}
        | {"date": _date(g.get("date"))}
        for g in _dicts(rec.get("games"))
    ]
    return _frame(rows, _SCHEDULE_SPEC, columns, return_as_pandas)


def _date(v: Any) -> Optional[date]:
    if isinstance(v, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return date.fromisoformat(v)
    return None


_CLUB_SPEC: Spec = {"season": INT, "tab": INT, "event": INT, "team_id": S, "name": S}


def parse_bleague_schedule_clubs(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The club filter of the schedule page: every club of the season and division with its team id.

    Args:
        raw: the record ``bleague_schedule_clubs(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per club the page's filter lists, which maps a team id to the short Japanese name the schedule
        uses. A record without clubs gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_schedule_clubs

            clubs = bleague_schedule_clubs(season=2026, tab=1, month=10, day=4)
    """
    rec = _rec(raw)
    base = _page_base(rec)
    rows = [{**base, "team_id": _idstr(c.get("team_id")), "name": _s(c.get("name"))} for c in _dicts(rec.get("clubs"))]
    return _frame(rows, _CLUB_SPEC, columns, return_as_pandas)


_EVENT_SPEC: Spec = {"season": INT, "tab": INT, "event_id": INT, "label": S, "selected": B}


def parse_bleague_schedule_events(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The competitions the schedule page offers for one season and division (the ``event`` filter).

    Args:
        raw: the record ``bleague_schedule_events(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per competition type (2 B1 league, 3 B1 Championship, 5 all-star, 7 B2 league, 15 B3 league, ...)
        with its Japanese label; ``selected`` marks the one the page was asked for. A record without events
        gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_schedule_events

            events = bleague_schedule_events(season=2026, tab=1, month=10, day=4)
    """
    rec = _rec(raw)
    base = _page_base(rec)
    rows = [
        {
            "season": base["season"],
            "tab": base["tab"],
            "event_id": _i(e.get("event")),
            "label": _s(e.get("label")),
            "selected": _i(e.get("event")) == base["event"] if _i(e.get("event")) is not None else None,
        }
        for e in _dicts(rec.get("events"))
    ]
    return _frame(rows, _EVENT_SPEC, columns, return_as_pandas)


_DAY_SPEC: Spec = {"season": INT, "tab": INT, "event": INT, "date": D}


def parse_bleague_schedule_days(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The days of the asked month on which the competition has games (the page's date strip).

    Args:
        raw: the record ``bleague_schedule_days(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per game day of the requested month, as a date: loop over them with ``bleague_schedule`` to list
        every ``schedule_key`` of a season. A record without game days gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_schedule_days

            days = bleague_schedule_days(season=2026, tab=1, month=10, day=4)
    """
    rec = _rec(raw)
    base = _page_base(rec)
    year, mon = _i(rec.get("year")), _i(rec.get("mon"))
    rows = []
    for d in rec.get("game_days") or []:
        day = _i(d)
        if year is not None and mon is not None and day is not None:
            try:
                rows.append({**base, "date": date.fromisoformat(_season_date(year, mon, day))})
            except ValueError:
                continue
    return _frame(rows, _DAY_SPEC, columns, return_as_pandas)


_STANDINGS_SPEC: Spec = {
    "season": INT,
    "tab": INT,
    "season_label": S,
    "section": S,
    "section_en": S,
    "rank": INT,
    "team_id": S,
    "team": S,
    "team_short": S,
    "wins": INT,
    "losses": INT,
    "win_pct": F,
    "games_behind": F,
    "points_for": INT,
    "points_against": INT,
    "point_diff": INT,
    "home_record": S,
    "away_record": S,
    "last_n": INT,
    "last_n_record": S,
    "streak": S,
    "division_title": S,
}


def parse_bleague_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Standings of one season and division, one row per club per table (conference, division or wild card).

    Args:
        raw: the record ``bleague_standings(..., return_parsed=False)`` returns.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per club per table. ``section`` is the table heading (``東地区`` East, ``中地区`` Central,
        ``西地区`` West, ``ワイルドカード`` wild card; B2 and B3 may have one table with a different heading) and
        ``section_en`` its English name for those four. A club can appear in a division table and in the wild-card
        table. ``win_pct`` is a 0-1 fraction. The page shows the standings as of the request, not a season end.
        A record without rows gives a zero-row frame.
        Bad input is a typed zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.bleague import bleague_standings

            table = bleague_standings(season=2026, tab=1)
    """
    rec = _rec(raw)
    y = _i(rec.get("year"))
    base = {
        "season": y + 1 if y is not None else None,
        "tab": _i(rec.get("tab")),
        "season_label": _s(rec.get("season_label")),
    }
    skip = set(base)
    rows = [
        {**base, **{k: _cast(k, dt, r.get(k)) for k, dt in _STANDINGS_SPEC.items() if k not in skip}}
        for r in _dicts(rec.get("rows"))
    ]
    return _frame(rows, _STANDINGS_SPEC, columns, return_as_pandas)


#: Each parser's column dtypes, for the generator and the tests.
SPECS: Dict[str, Spec] = {
    "game_info": _GAME_SPEC,
    "period_scores": _PERIOD_SPEC,
    "player_box": _BOX_SPEC,
    "team_box": _BOX_SPEC,
    "team_summary": _SUMMARY_SPEC,
    "pbp": _PBP_SPEC,
    "shots": _SHOT_SPEC,
    "leaders": _LEADER_SPEC,
    "score_track": _TRACK_SPEC,
    "schedule": _SCHEDULE_SPEC,
    "schedule_clubs": _CLUB_SPEC,
    "schedule_events": _EVENT_SPEC,
    "schedule_days": _DAY_SPEC,
    "standings": _STANDINGS_SPEC,
}
_ = timedelta  # kept for symmetry with the runtime's date arithmetic
