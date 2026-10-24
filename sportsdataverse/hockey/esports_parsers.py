"""Parsers for the eSports hokejovyzapis S3 families (DEL, ICE Hockey League, EIHL, Metal Ligaen).

One scoring system writes the same key layout into four buckets (``de.``, ``icehl.``, ``eihl.``,
``den.``), so the four families (``del_esports``, ``icehl_esports``, ``eihl_esports``,
``metal_esports``) share these parsers. Contract: polars by default (pandas via
``return_as_pandas=True``), snake_case columns (``dl_utils.underscore``), never raises; an empty
or malformed body is a zero-row frame, with the documented columns when the wrapper passes
``columns``. Nested objects flatten to ``<parent>_<child>`` columns; list cells are JSON text.
A column holding one JSON type keeps it (bool, Int64, Float64); a mixed one is String. Rate
columns (``*percent*``, ``*per_game*``, ``xg`` ...) are Float64 even when one body happens to send
only whole numbers, so a column's dtype does not change from game to game.

Dtypes come from the returns schemas (``_esports_schemas.SCHEMAS``, written by the codegen from
``tools/codegen/schemas/native/<stem>/<short>.yaml``). When the wrapper passes ``columns`` (the
documented column list, which names one schema), every frame, empty, partial or full, carries all
of those columns, in that order, with the schema's dtypes: a documented column the body does not
send (EIHL ``penalty_*`` in a game without penalties) is a null column of its schema dtype, and
columns the schema does not know follow at the end. So ``pl.concat`` of any two answers of one
route never fails on width or dtype. Without ``columns`` an all-null column takes the dtype the
schemas give its name (when they agree), else a name rule (ids Int64, rates Float64, else String).

Body shapes and their parsers:

* ``league-matches/{season}/{league_id}.json`` ``{"matches": [...]}`` ->
  :func:`parse_esports_schedule` (one row per match; ``season`` re-keyed to the END year).
* ``matches/{id}/period-events.json`` ``{"1": [...], "2": [...], "3": [...], "overtime": [...],
  "shootout": [...]}`` -> :func:`parse_esports_pbp` (one row per event, across periods).
* ``matches/{id}/team-stats/{team}.json``, ``league-team-stats/...``,
  ``league-part-players-stats/...`` (players with a nested ``statistics``) ->
  :func:`parse_esports_player_stats`.
* ``visualization/shots/{id}.json`` ``{"match": {..., "shots": [...]}}`` ->
  :func:`parse_esports_shots`.
* ``tables/...`` (DEL ``[{leaguePartId, tableData: {table: [...]}}]``) and ``table/...`` /
  ICEHL ``tables/...`` (``[{groupId, groupName, complete, clubs: [...]}]``) ->
  :func:`parse_esports_standings`.
* ``matches/{id}/shiftsSC.json`` (DEL) -> :func:`parse_esports_shifts`.
* ``matches/{id}/roster.json`` (DEL) and the ``roster`` of ``game-header`` / ``game-info`` ->
  :func:`parse_esports_lineup`.
* ``matches/{id}/game-header.json`` / ``game-info.json`` -> :func:`parse_esports_game_info`.
* Everything else (top goalies, top scorers, faceoffs, the widget bundle) ->
  :func:`parse_esports` (a list is rows; any other object is one row).

Id key spaces. Every id is Int64 and every key space is **per bucket**: DEL ``match_id`` 4444
and ICEHL ``match_id`` 4444 are different games, and none of them is a league website's game id.

* ``match_id``: the match key space (``league-matches`` ``matches[].id``, the ``{match_id}`` path
  param). The shot file sends it as a string (``"4444"``); it is cast to Int64 here.
* ``team_id``, ``home_id``, ``guest_id``, ``visitor_id``: the team key space, stable across seasons
  (the ``{team_id}`` path param). ``part_team_id`` is a team's entry in one league part.
* ``player_id`` and every ``*_player_id``: the ``playerId`` key space, stable across games and
  seasons. ``match_player_id`` (lineups only) is the per-game roster entry (``matchPlayerId``) and
  never joins to ``player_id``; the shot file's ``player_id`` is ``playerId``, not ``matchPlayerId``.
* ``league_id`` / ``league_part_id``: per-bucket competition / competition-stage ids.
* ``event_id``, ``shot_id``, ``shift_id``: row ids within their own file type.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64
from sportsdataverse.dl_utils import underscore
from sportsdataverse.errors import _data_only
from sportsdataverse.hockey._esports_schemas import SCHEMAS

__all__ = [
    "parse_esports",
    "parse_esports_game_info",
    "parse_esports_lineup",
    "parse_esports_pbp",
    "parse_esports_player_stats",
    "parse_esports_schedule",
    "parse_esports_shifts",
    "parse_esports_shots",
    "parse_esports_standings",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Row = Dict[str, Any]

#: Wire keys ``underscore`` would split badly (``xG`` -> ``x_g``) or that two buckets spell differently.
_KEYS = {
    "xG": "xg",
    "xGA": "xga",
    "xS": "xs",
    "GSAx": "gsax",
    "xG_type": "xg_type",
    "xGStory": "xg_story",
    "homeD2W": "home_d2w",
    "awayD2W": "away_d2w",
    "onlyOTwins": "only_ot_wins",
    "onlyOTlosts": "only_ot_losts",
    "onlySOwins": "only_so_wins",
    "onlySOlosts": "only_so_losts",
    "losser": "loser",  # DEL faceoffs
    "looser": "loser",  # ICEHL faceoffs
}
_ID = re.compile(r"(^|_)id$")
_FLOAT = re.compile(
    r"(percent|per_game|efficiency|average|^ppg$|(^|_)(xg|xga|xs|gsax)$|ex_goals|d2w|milestone|minutes_in_play"
    r"|^coordinate_[xy]$|timeonice)"
)
#: period-events keys that are not numbered periods: (label, period_number).
_EXTRA_PERIODS = {"overtime": ("OT", 4), "shootout": ("SO", 5)}
#: period-events ``data`` objects that hold one player -> column prefix.
_PEOPLE = {
    "scorer": "scorer",
    "disciplinedPlayer": "penalized",
    "replacedPlayer": "replaced",
    "fouledPlayer": "fouled",
    "player": "goalie",
    "outgoingGoalkeeper": "outgoing_goalie",
}
_EVENT_SCALARS = {"id": "event_id", "team": "team_side", "ea": "extra_attacker", "en": "empty_net"}
#: shot ``match_shot_resutl_id`` (sic) code of a goal; 1/2/3 are unverified (on goal / missed / blocked).
GOAL_RESULT = 4


def _key(k: Any) -> str:
    """Wire key -> column name: ``_KEYS``, else ``underscore`` with any other character run as ``_`` (``toi/gp``)."""
    return _KEYS.get(str(k)) or re.sub(r"\W+", "_", underscore(str(k))).strip("_")


def _flatten(obj: Dict[Any, Any], prefix: str = "", out: Optional[Row] = None) -> Row:
    """Nested dict -> one flat ``{snake_case_path: value}`` row (an empty dict is a null cell)."""
    out = {} if out is None else out
    for k, v in obj.items():
        name = f"{prefix}_{_key(k)}" if prefix else _key(k)
        if isinstance(v, dict) and v:
            _flatten(v, name, out)
        else:
            out[name] = None if isinstance(v, dict) else v
    return out


def _text(v: Any) -> str:
    return v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)


def _series(name: str, values: List[Any]) -> pl.Series:
    """One column with an explicit dtype, so a heterogeneous body never makes polars raise."""
    kinds = {type(v) for v in values if v is not None}
    if not kinds:
        return pl.Series(name, values, dtype=pl.Null)
    if kinds == {bool}:
        return pl.Series(name, values, dtype=pl.Boolean)
    if kinds == {int}:
        return pl.Series(name, values, dtype=pl.Int64)
    if kinds <= {int, float}:
        return pl.Series(name, [None if v is None else float(v) for v in values], dtype=pl.Float64)
    return pl.Series(name, [None if v is None else _text(v) for v in values], dtype=pl.String)


#: R-style returns-schema type -> polars dtype.
_SCHEMA_TYPES = {"integer": pl.Int64, "numeric": pl.Float64, "double": pl.Float64, "logical": pl.Boolean}


def _schema_index() -> Tuple[Dict[Tuple[str, ...], Dict[str, Any]], Dict[str, Any]]:
    """``({documented column tuple: {column: dtype}}, {column: dtype where every schema agrees})``."""
    by_columns: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    seen: Dict[str, set] = {}
    for shorts in SCHEMAS.values():
        for cols in shorts.values():
            by_columns[tuple(cols)] = {c: _SCHEMA_TYPES.get(t, pl.String) for c, t in cols.items()}
            for c, t in cols.items():
                seen.setdefault(c, set()).add(_SCHEMA_TYPES.get(t, pl.String))
    return by_columns, {c: next(iter(ts)) for c, ts in seen.items() if len(ts) == 1}


_BY_COLUMNS, _BY_NAME = _schema_index()


def _empty_dtype(name: str, schema: Optional[Dict[str, Any]] = None) -> Any:
    """The dtype of an all-null column: its route's schema, else every schema's, else the name rule."""
    dtype = (schema or {}).get(name) or _BY_NAME.get(name)
    if dtype is not None:
        return dtype
    if _ID.search(name) or name.endswith("_count"):  # e.g. overtime_count, null until a game goes to overtime
        return pl.Int64
    return pl.Float64 if _FLOAT.search(name) else pl.String


def _widens(src: Any, dst: Any) -> bool:
    """True when ``src`` casts to the schema dtype ``dst`` without losing a value."""
    if src == pl.Int64 and dst == pl.Float64:
        return True
    return dst == pl.String and src in (pl.Int64, pl.Float64, pl.Boolean)


def _frame(rows: List[Row], columns: Optional[Sequence[str]], return_as_pandas: bool) -> Frame:
    """Rows -> frame with the schema dtypes (see the module docstring); ``columns`` are backfilled."""
    schema = _BY_COLUMNS.get(tuple(columns or ()), {})
    names: Dict[str, None] = {}
    for row in rows:
        names.update(dict.fromkeys(row))
    if rows:
        df = pl.DataFrame([_series(n, [row.get(n) for row in rows]) for n in names])
        casts = {}
        for name, dtype in df.schema.items():
            want = schema.get(name) or _BY_NAME.get(name)
            if dtype == pl.Null:
                casts[name] = _empty_dtype(name, schema)
            elif want is not None and want != dtype and _widens(dtype, want):
                casts[name] = want
            elif dtype == pl.Int64 and _FLOAT.search(name) and not _ID.search(name):
                casts[name] = pl.Float64
        df = df.with_columns(pl.col(n).cast(t) for n, t in casts.items()) if casts else df
        df = _cast_ids_int64(df, [n for n, t in df.schema.items() if _ID.search(n) and t == pl.String])
        if columns:
            missing = [c for c in columns if c not in df.columns]
            df = df.with_columns(pl.lit(None, dtype=_empty_dtype(c, schema)).alias(c) for c in missing)
            documented = set(columns)
            df = df.select([*columns, *(c for c in df.columns if c not in documented)])
    else:
        df = pl.DataFrame(schema={c: _empty_dtype(c, schema) for c in columns or []})
    return df.to_pandas() if return_as_pandas else df


def _dicts(value: Any) -> List[Dict[Any, Any]]:
    return [x for x in value if isinstance(x, dict)] if isinstance(value, list) else []


def _person(prefix: str, p: Any) -> Row:
    """A ``{playerId, jersey, name, surname}`` object -> ``<prefix>_player_id`` / ``_jersey`` / ``_name``."""
    if not isinstance(p, dict):
        return {}
    name = " ".join(str(p[k]) for k in ("name", "surname") if p.get(k))
    return {
        f"{prefix}_player_id": p.get("playerId"),
        f"{prefix}_jersey": p.get("jersey"),
        f"{prefix}_name": name or None,
    }


@_data_only
def parse_esports(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Generic eSports body -> frame: a list is one row per object, any other object is one row.

    Args:
        raw (Any): a decoded bucket body (top goalies, top scorers, faceoffs, the widget bundle).
        return_as_pandas (bool): return a pandas frame instead of polars.
        columns (Sequence[str] | None): the documented columns, used only for the zero-row frame.

    Returns:
        pl.DataFrame | pd.DataFrame: one row per list item (one row for an object), nested
        objects flattened to ``<parent>_<child>`` columns; zero rows when ``raw`` is unusable.

    Example:
        Quick start::

            from sportsdataverse.hockey.icehl import parse_esports
            df = parse_esports([{"playerId": 1, "savesPercent": 91.2}])

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_top_scorers` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.del_.del_faceoffs` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.del_.del_goalie_box` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.icehl.icehl_top_scorers` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    if isinstance(raw, list):
        rows = [_flatten(x) for x in _dicts(raw)]
    else:
        rows = [_flatten(raw)] if isinstance(raw, dict) and raw else []
    return _frame(rows, columns, return_as_pandas)


def parse_esports_schedule(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``league-matches`` body -> one row per match, played or not.

    ``id`` becomes ``match_id``; ``season`` is re-keyed to the season's END year (the body sends the
    START year, ``"2026"`` = 2026/27 -> ``2027``); the ``results`` block flattens to ``extra_time``,
    ``shooting``, ``contumacy`` and ``<span>_score_home`` / ``<span>_score_guest`` (``final``,
    ``first_period`` ...); DEL's byte-order mark in ``round`` is stripped.

    Args:
        raw (Any): decoded ``{"matches": [...]}`` body.
        return_as_pandas (bool): return a pandas frame instead of polars.
        columns (Sequence[str] | None): the documented columns, used only for the zero-row frame.

    Returns:
        pl.DataFrame | pd.DataFrame: one row per match; zero rows when ``raw`` is unusable.

    Example:
        Quick start::

            from sportsdataverse.hockey.del_ import del_schedule
            df = del_schedule(season=2027)

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_schedule` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.icehl.icehl_schedule` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.eihl.eihl_schedule` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.metal.metal_schedule` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows = []
    for match in _dicts(raw.get("matches") if isinstance(raw, dict) else raw):
        row: Row = {}
        for k, v in _flatten(match).items():
            k = "match_id" if k == "id" else k.removeprefix("results_score_").removeprefix("results_")
            if k == "season":
                v = int(v) + 1 if isinstance(v, (int, str)) and str(v).isdigit() else None
            elif k == "round" and isinstance(v, str):
                v = v.replace("﻿", "")
            row[k] = v
        rows.append(row)
    return _frame(rows, columns, return_as_pandas)


def _event_data(data: Any) -> Row:
    out: Row = {}
    if not isinstance(data, dict):
        return out
    for k, v in data.items():
        if k == "time":
            if isinstance(v, dict):  # a penalty's {from, to}{realTime, scoreboardTime}; else the event time again
                out.update(_flatten(v, "penalty"))
        elif k == "assistants":
            for i, p in enumerate(_dicts(v), 1):
                out.update(_person(f"assist{i}", p))
        elif k == "attendants":
            for side, label in (("positive", "plus"), ("negative", "minus")):
                people = _dicts(v.get(side)) if isinstance(v, dict) else []
                out[f"{label}_player_ids"] = ",".join(str(p.get("playerId")) for p in people) or None
        elif k in _PEOPLE:
            out.update(_person(_PEOPLE[k], v))
        elif isinstance(v, dict):
            out.update(_flatten(v, _key(k)))
        else:
            out[_EVENT_SCALARS.get(k) or _key(k)] = v
    return out


def _period_keys(raw: Dict[Any, Any]) -> List[Any]:
    keys = [k for k, v in raw.items() if isinstance(v, list)]
    return sorted(keys, key=lambda k: (0, int(k), "") if str(k).isdigit() else (1, 0, str(k)))


def parse_esports_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``period-events`` body -> one row per event, flattened across periods, overtime and shootout.

    ``period`` is the body's key (``"1"`` .. ``"3"``, ``OT``, ``SO``) and ``period_number`` its number
    (overtime 4, shootout 5); ``time`` is the game second; ``event_type`` is ``goal``, ``penalty``,
    ``goalkeeperChange``, ``timeout``, ``periodStart``, ``periodEnd`` (DEL), ``videoReview`` ... A
    player object becomes ``<role>_player_id`` / ``_jersey`` / ``_name`` (roles ``scorer``,
    ``assist1``, ``assist2``, ``penalized``, ``replaced``, ``fouled``, ``goalie``, ``outgoing_goalie``),
    all in the ``playerId`` key space. The goal's on-ice players become comma-joined
    ``plus_player_ids`` / ``minus_player_ids``. A penalty's ``data.time`` ``{from, to}`` object becomes
    ``penalty_from_real_time`` / ``penalty_from_scoreboard_time`` / ``penalty_to_*``. The body carries
    no match id.

    Args:
        raw (Any): decoded ``period-events`` body.
        return_as_pandas (bool): return a pandas frame instead of polars.
        columns (Sequence[str] | None): the documented columns, used only for the zero-row frame.

    Returns:
        pl.DataFrame | pd.DataFrame: one row per event, in period then file order; zero rows when
        ``raw`` is unusable.

    Example:
        Quick start::

            from sportsdataverse.hockey.icehl import icehl_pbp
            df = icehl_pbp(match_id=8161)

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_pbp` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.icehl.icehl_pbp` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.eihl.eihl_pbp` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.metal.metal_pbp` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows: List[Row] = []
    if isinstance(raw, dict):
        for key in _period_keys(raw):
            label, number = (str(key), int(key)) if str(key).isdigit() else _EXTRA_PERIODS.get(key, (str(key), None))
            for event in _dicts(raw[key]):
                row: Row = {
                    "event_index": len(rows),
                    "period": label,
                    "period_number": number,
                    # DEL writes periodEnd times as floats (1200.0); a game second is an integer.
                    "time": int(t) if isinstance(t := event.get("time"), float) and t.is_integer() else t,
                    "event_type": event.get("type"),
                }
                row.update(_event_data(event.get("data")))
                rows.append(row)
    return _frame(rows, columns, return_as_pandas)


@_data_only
def parse_esports_player_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Players with a nested ``statistics`` object -> one row per player.

    Serves the game boxscore (``matches/{id}/team-stats/{team}``: one team's players in one game) and
    the season tables (``league-team-stats``, ``league-part-players-stats``). ``id`` becomes
    ``player_id``; ``statistics`` flattens into the row. A stat the body splits by venue
    (``{"home": 1, "away": 0}``) gives its total under the plain name plus ``<name>_home`` /
    ``<name>_away``. The game boxscore carries no match or team id, only ``team_shortcut``.

    Args:
        raw (Any): decoded list of players.
        return_as_pandas (bool): return a pandas frame instead of polars.
        columns (Sequence[str] | None): the documented columns, used only for the zero-row frame.

    Returns:
        pl.DataFrame | pd.DataFrame: one row per player; zero rows when ``raw`` is unusable.

    Example:
        Quick start::

            from sportsdataverse.hockey.eihl import eihl_player_box
            df = eihl_player_box(match_id=4440, team_id=96)

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_team_player_stats` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.del_.del_player_box` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.icehl.icehl_league_part_player_stats` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.icehl.icehl_team_player_stats` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows = []
    for player in _dicts(raw):
        row: Row = {"player_id": player.get("id")}
        row.update(_flatten({k: v for k, v in player.items() if k not in ("id", "statistics")}))
        stats = player.get("statistics")
        for k, v in stats.items() if isinstance(stats, dict) else []:
            name = _key(k)
            split = isinstance(v, dict) and set(v) == {"home", "away"}
            if split and all(type(x) in (int, float) for x in v.values()):
                row[name] = v["home"] + v["away"]
                row[f"{name}_home"], row[f"{name}_away"] = v["home"], v["away"]
            else:
                row.update(_flatten({k: v}))
        rows.append(row)
    return _frame(rows, columns, return_as_pandas)


def parse_esports_shots(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``visualization/shots`` body -> one row per shot, with the match's ids and teams on each row.

    Coordinates: ``coordinate_x`` runs along the rink length and ``coordinate_y`` across it, in the
    provider's units (DEL two decimals, ICEHL / EIHL whole numbers). Each team keeps one sign of x
    for the whole game, home positive, so x is the attacking direction, not a rink end: all 291
    shots of DEL 4444, ICEHL 8161 and EIHL 4440 but one (an EIHL 4440 visitor goal recorded at
    x = +70) follow it. ``match_shot_resutl_id`` keeps the provider's
    spelling; ``shot_result_id`` is the same code under a readable name and ``is_goal`` marks code 4
    (goal, verified). Codes 1 / 2 / 3 (on goal / missed / blocked in the widgets) are unverified.
    ``xg`` / ``xg_type`` exist on ICEHL and EIHL only; elsewhere they are null columns, so the four
    leagues share one schema. ``team_side`` is ``home`` / ``visitor`` from ``team_id``.
    ``player_id`` is the ``playerId`` key space.

    Args:
        raw (Any): decoded ``{"match": {..., "shots": [...]}}`` body.
        return_as_pandas (bool): return a pandas frame instead of polars.
        columns (Sequence[str] | None): the documented columns, used only for the zero-row frame.

    Returns:
        pl.DataFrame | pd.DataFrame: one row per shot; zero rows when ``raw`` is unusable or holds no
        shots (every Metal Ligaen shot file is an empty shell).

    Example:
        Quick start::

            from sportsdataverse.hockey.icehl import icehl_shots
            df = icehl_shots(match_id=8161)

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_shots` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.icehl.icehl_shots` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.eihl.eihl_shots` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    match = raw.get("match") if isinstance(raw, dict) else None
    match = match if isinstance(match, dict) else {}
    context: Row = {"match_id": match.get("id")}
    context.update({_key(k): v for k, v in match.items() if k != "id" and not isinstance(v, (dict, list))})
    rows = []
    for shot in _dicts(match.get("shots")):
        row = dict(context)
        for k, v in _flatten(shot).items():
            row["shot_id" if k == "id" else k] = None if k == "polygon" and not isinstance(v, str) else v
        code = row.get("match_shot_resutl_id")
        row["shot_result_id"] = code
        row["is_goal"] = None if code is None else code == GOAL_RESULT
        team = row.get("team_id")
        row["team_side"] = (
            "home" if team == context.get("home_id") else "visitor" if team == context.get("visitor_id") else None
        )
        row.setdefault("xg", None)
        row.setdefault("xg_type", None)
        rows.append(row)
    return _frame(rows, columns, return_as_pandas)


def parse_esports_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``tables`` / ``table`` body -> one row per team.

    Two shapes: DEL ``tables`` is ``[{leaguePartId, leaguePartName, tableData: {table, state,
    datetime}}]`` (rows carry ``league_part_id``, ``league_part_name``, ``state``, ``datetime``);
    ICEHL ``tables`` and EIHL / Metal Ligaen ``table`` are ``[{groupId, groupName, complete, clubs}]``
    (rows carry ``group_id``, ``group_name``, ``complete``). ``id`` becomes ``team_id``. The team
    columns follow each bucket's own names (DEL ``losses``, the others ``losts``).

    Args:
        raw (Any): decoded standings body.
        return_as_pandas (bool): return a pandas frame instead of polars.
        columns (Sequence[str] | None): the documented columns, used only for the zero-row frame.

    Returns:
        pl.DataFrame | pd.DataFrame: one row per team per table; zero rows when ``raw`` is unusable.

    Example:
        Quick start::

            from sportsdataverse.hockey.metal import metal_standings
            df = metal_standings(season=2027)

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_standings` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.icehl.icehl_standings` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.eihl.eihl_standings` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.metal.metal_standings` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows = []
    for block in _dicts(raw):
        data = block.get("tableData")
        if isinstance(data, dict):
            context = _flatten({k: v for k, v in block.items() if k != "tableData"})
            context.update(_flatten({k: v for k, v in data.items() if k != "table"}))
            teams = _dicts(data.get("table"))
        else:
            context = _flatten({k: v for k, v in block.items() if k != "clubs"})
            teams = _dicts(block.get("clubs"))
        for team in teams:
            row = dict(context)
            row.update({("team_id" if k == "id" else k): v for k, v in _flatten(team).items()})
            rows.append(row)
    return _frame(rows, columns, return_as_pandas)


@_data_only
def parse_esports_shifts(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """DEL ``shiftsSC`` body -> one row per shift.

    Columns: ``shift_id``, ``start_time`` / ``end_time`` (game seconds), ``start_real_time`` /
    ``end_real_time`` (local wall clock), ``duration`` (seconds, end minus start), ``player_id``
    (``playerId`` key space), ``player_name``, ``team_id``, ``team_name``.

    Args:
        raw (Any): decoded list of shifts.
        return_as_pandas (bool): return a pandas frame instead of polars.
        columns (Sequence[str] | None): the documented columns, used only for the zero-row frame.

    Returns:
        pl.DataFrame | pd.DataFrame: one row per shift; zero rows when ``raw`` is unusable.

    Example:
        Quick start::

            from sportsdataverse.hockey.del_ import del_shifts
            df = del_shifts(match_id=4444)

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_shifts` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rename = {
        "id": "shift_id",
        "start_time_time": "start_time",
        "start_time_realtime": "start_real_time",
        "end_time_time": "end_time",
        "end_time_realtime": "end_real_time",
    }
    rows = []
    for shift in _dicts(raw):
        row = {rename.get(k, k): v for k, v in _flatten(shift).items()}
        start, end = row.get("start_time"), row.get("end_time")
        row["duration"] = end - start if type(start) is int and type(end) is int else None
        rows.append(row)
    return _frame(rows, columns, return_as_pandas)


def parse_esports_lineup(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Lineup (``{home: {slot: player}, visitor: {...}}``) -> one row per player.

    Reads DEL's ``roster.json`` and the ``roster`` object inside an ICEHL / EIHL / Metal Ligaen
    ``game-header`` (``game-info``) body. ``slot`` is the three-digit line slot
    ``<group><line><position>``: group 1 goalies (101, 102), 2 defence pairs, 3 forward lines.
    ``player_id`` is the ``playerId`` key space; ``match_player_id`` is the per-game roster entry and
    is the id the pbp and shot files do NOT use.

    Args:
        raw (Any): decoded ``roster.json`` body, or a game-header body holding ``roster``.
        return_as_pandas (bool): return a pandas frame instead of polars.
        columns (Sequence[str] | None): the documented columns, used only for the zero-row frame.

    Returns:
        pl.DataFrame | pd.DataFrame: one row per dressed player, home then visitor, in slot order;
        zero rows when ``raw`` is unusable.

    Example:
        Quick start::

            from sportsdataverse.hockey.icehl import icehl_game_info, parse_esports_lineup
            lineup = parse_esports_lineup(icehl_game_info(match_id=8161, return_parsed=False))

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_roster` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    roster = raw.get("roster") if isinstance(raw, dict) and isinstance(raw.get("roster"), dict) else raw
    rows = []
    for side in ("home", "visitor"):
        team = roster.get(side) if isinstance(roster, dict) else None
        for slot, player in sorted(team.items(), key=lambda kv: str(kv[0])) if isinstance(team, dict) else []:
            if isinstance(player, dict):
                flat = _flatten({k: v for k, v in player.items() if k != "roster"})
                rows.append({"team_side": side, "slot": str(slot), **flat})
    return _frame(rows, columns, return_as_pandas)


@_data_only
def parse_esports_game_info(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``game-header`` / ``game-info`` body -> one row of game info (lineup left out).

    ICEHL, EIHL and Metal Ligaen wrap the info in ``gameInfo`` beside a ``roster``; DEL sends the
    info unwrapped, with referees and coaches as ``{id, name}`` objects (``referees_head_referee1_id``
    / ``_name``) where the others send name strings. Read the lineup with
    :func:`parse_esports_lineup` on the same raw body.

    Args:
        raw (Any): decoded game-header body.
        return_as_pandas (bool): return a pandas frame instead of polars.
        columns (Sequence[str] | None): the documented columns, used only for the zero-row frame.

    Returns:
        pl.DataFrame | pd.DataFrame: one row; zero rows when ``raw`` is unusable.

    Example:
        Quick start::

            from sportsdataverse.hockey.eihl import eihl_game_info
            df = eihl_game_info(match_id=4440)

    See Also:
        * :func:`sportsdataverse.hockey.del_.del_game_info` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.icehl.icehl_game_info` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.eihl.eihl_game_info` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.metal.metal_game_info` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    info: Any = None
    if isinstance(raw, dict):
        info = (
            raw["gameInfo"]
            if isinstance(raw.get("gameInfo"), dict)
            else {k: v for k, v in raw.items() if k != "roster"}
        )
    return _frame([_flatten(info)] if info else [], columns, return_as_pandas)
