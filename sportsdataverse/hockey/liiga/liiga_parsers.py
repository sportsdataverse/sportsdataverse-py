"""Parsers for the generated ``liiga`` wrappers (Liiga API v2, ``liiga.fi/api/v2``).

Every parser follows the package contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_cased columns (``dl_utils.underscore``), and a zero-row
frame (never an exception) for an empty or malformed body. Given ``columns`` (the generated
wrappers pass the endpoint's documented returns-schema), an empty body keeps those columns
and a body missing one of them gains it as nulls (tracking columns before they existed).

**Row rule** (:func:`parse_liiga`, the generic parser): a list is rows; a dict whose
values are all objects keyed by an id (``"123"`` / ``"123:slug"``) is rows; a dict with one
list-valued key (``/teams/stats`` ``{"teamStats": [...]}``) is that list; any other dict is
one row. Nested objects flatten into ``<parent>_<child>`` columns; lists and id-keyed maps
(``assistsSoFarInSeason``, ``teamSeasons``) become JSON strings.

**Dtypes** are pinned, not taken from a sample: each parser's returns-schema dtypes
(``liiga_dtypes.DTYPES``, generated with the schemas) type an empty frame, a gap-filled
column and an all-null column, so an empty frame stacks onto a populated one; a column the
schema does not list is typed by name. Liiga sends ``2`` for an xG of 2.0 and ``"92.86"``
for a percentage, so a column whose name is a rate (``percent``, ``per_game``, ``expected``,
``average``, ``speed``, ``distance``, ``duration``, ``hardest``) is ``Float64``. Percentage
strings parse with either comma (``"0,00"`` decimal mark, ``"4,583.33"`` / ``"4,583"``
thousands separator; see :func:`_number`). The /standings/ ``power_play_percentage``,
``short_handed_percentage`` and ``win_percentage`` stay 0-1 fractions, as sent; every other
percentage is 0-100.

**Id key spaces.** Every id column is ``Int64``:

* ``team_id``: the club's numeric id. Liiga sends three forms: ``"55786244:hpk"`` (games,
  schedule, lineups, awards, shootout, box score, standings, roster), the bare integer
  ``55786244`` (shot map, team and player stat tables, team history) and the digit string
  ``"495643563"`` (team directory). Each becomes ``team_id`` (the number) plus
  ``team_slug`` (``"hpk"``) where the string form carries one. The same rule names
  ``home_team_id`` / ``away_team_id``, ``shooting_team_id`` / ``blocking_team_id`` and the
  shot map's ``left_team_id`` / ``right_team_id``.
* ``game_id``: unique only within a season; key a game as ``(season, game_id)``. Game
  frames rename the body's ``id`` to ``game_id``.
* ``player_id``: one id per player everywhere (lineup ``id``, ``playerId``, ``fihaId``);
  lineup and roster frames rename ``id`` to ``player_id``.
* ``event_id`` (within a game), ``official_id``, ``ice_rink_id``, ``serie_id`` (the
  /schedule tournament id: 1188 regular season, 1189 playoffs, 1194 preseason, 1890 CHL,
  the same every season; /schedule's ``serie``) and the team-history row ``id``.

Jersey NUMBERS are not ids: the box score's ``jerseyId`` becomes ``jersey`` and a goal's
``plusPlayerIds`` / ``minusPlayerIds`` (space-separated jerseys on ice) become
``plus_jerseys`` / ``minus_jerseys``.

**Season.** Seasons are END years (2026 = 2025-26). Frames of season- or game-scoped
routes carry ``season``; where the body does not say it (box score, shot map, lineup
stats, standings, player game log) it comes from the request the runtime stamps under
``_request`` (see ``liiga_runtime``). The range tables (``/teams/stats``,
``/players/stats/summed``, ``/players/info``) are summed over ``season_from``..``season_to``
and carry no season; the team directory and the current-state ``/tournament`` neither.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64
from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.liiga.liiga_dtypes import DTYPES

__all__ = [
    "parse_liiga",
    "parse_liiga_game",
    "parse_liiga_game_goals",
    "parse_liiga_game_lineups",
    "parse_liiga_game_penalties",
    "parse_liiga_game_players_stats",
    "parse_liiga_game_shootout",
    "parse_liiga_game_stats",
    "parse_liiga_game_team_stats",
    "parse_liiga_games",
    "parse_liiga_goals",
    "parse_liiga_player",
    "parse_liiga_player_career",
    "parse_liiga_player_game_log",
    "parse_liiga_players_info",
    "parse_liiga_schedule",
    "parse_liiga_shotmap",
    "parse_liiga_standings",
    "parse_liiga_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``liiga_runtime.REQUEST_KEY``).
_REQUEST = "_request"
#: Wire keys that hold a team id, in any of its three forms.
_TEAM_KEYS = frozenset(
    {"teamId", "homeTeamId", "awayTeamId", "shootingTeamId", "blockingTeamId", "leftTeam", "rightTeam"}
)
_TEAM_ID = re.compile(r"^\s*(\d+)(?::(.*))?\s*$")
_ID_COL = re.compile(r"(^|_)id$")
_FLOAT_COL = re.compile(r"percent|per_game|expected|average|speed|distance|duration|hardest")
_MAP_KEY = re.compile(r"^\d+(:|$)")
#: A comma-grouped integer part (``4,583`` / ``1,234,567``): the comma is a thousands separator.
_THOUSANDS = r"^-?\d{1,3}(,\d{3})+$"
_DTYPE_CODES = {"i": pl.Int64, "f": pl.Float64, "b": pl.Boolean, "s": pl.String}
#: An all-null column with a season, period or count-like name is Int64, not String (a box score's faceoff
#: counts are null for every player of some games).
_COUNT_COL = re.compile(
    r"(^|_)(season|period|total|won|lost|goals|assists|points|shots|passes|saves|games|wins|losses|ranking)$"
)
#: Maps keyed by a team NAME (``{"HIFK": [...]}``), kept as JSON like the id-keyed ones.
_NAME_KEYED = frozenset({"teamList"})
#: Per-team event lists of a game; their own frames, never columns of the game row.
_EVENT_KEYS = frozenset({"goalEvents", "penaltyEvents", "goalKeeperEvents", "goalKeeperChanges"})
_SIDES = (("home", "homeTeam"), ("away", "awayTeam"))
#: /standings/ list keys -> the tournament (request vocabulary) each table covers.
_STANDINGS_TABLES = {
    "season": "runkosarja",
    "playoffs": "playoffs",
    "valmistavat_ottelut": "valmistavat_ottelut",
    "playout": "playout",
    "qualifications": "qualifications",
}


# --- helpers -----------------------------------------------------------------------------------


def _split_team_id(value: Any) -> Tuple[Optional[int], Optional[str]]:
    """Normalise any of Liiga's three team-id forms to ``(number, slug)``.

    Args:
        value: ``"55786244:hpk"``, ``55786244`` or ``"55786244"`` (or anything else).

    Returns:
        ``(55786244, "hpk")``, ``(55786244, None)``, ``(55786244, None)``; ``(None, None)``
        for a value that is none of the three (never a guess).
    """
    if isinstance(value, bool):
        return None, None
    if isinstance(value, int):
        return value, None
    if isinstance(value, str):
        m = _TEAM_ID.match(value)
        if m:
            return int(m.group(1)), (m.group(2) or None)
    return None, None


def _team(name: str, value: Any) -> Dict[str, Any]:
    """``teamId`` -> ``team_id`` (+ ``team_slug`` when the wire value carries one)."""
    col = name if name.endswith("_id") else f"{name}_id"
    num, slug = _split_team_id(value)
    out: Dict[str, Any] = {col: num}
    if isinstance(value, str) and ":" in value:
        out[col[: -len("_id")] + "_slug"] = slug
    return out


def _is_map(value: Any) -> bool:
    return isinstance(value, dict) and bool(value) and all(_MAP_KEY.match(str(k)) for k in value)


def _flatten(obj: Mapping[str, Any], prefix: str = "") -> Dict[str, Any]:
    """One flat row: nested objects as ``<parent>_<child>``, lists and id maps as JSON."""
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        if key == _REQUEST:
            continue
        name = prefix + underscore(str(key))
        if key in _TEAM_KEYS:
            out.update(_team(name, value))
        elif isinstance(value, list) or _is_map(value) or (key in _NAME_KEYED and isinstance(value, dict)):
            out[name] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, dict):
            out.update(_flatten(value, name + "_"))
        else:
            out[name] = value
    return out


def _request(obj: Any) -> Dict[str, Any]:
    req = obj.get(_REQUEST) if isinstance(obj, dict) else None
    return dict(req) if isinstance(req, dict) else {}


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _key(value: Any) -> Any:
    """``value`` when it can key a dict (a period number), else None (a list or object never raises)."""
    return value if isinstance(value, (int, str)) and not isinstance(value, bool) else None


def _list(value: Any) -> List[Any]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _dtype_for(col: str, schema: Optional[Mapping[str, str]] = None) -> Any:
    """A column's dtype without a value to go by: the parser's returns-schema, else the name rules."""
    code = (schema or {}).get(col)
    if code:
        return _DTYPE_CODES[code]
    if _ID_COL.search(col):
        return pl.Int64
    if _FLOAT_COL.search(col):
        return pl.Float64
    if _COUNT_COL.search(col):
        return pl.Int64
    return pl.String


def _number(expr: pl.Expr) -> pl.Expr:
    """A numeric string -> Float64 (unparseable -> null).

    Liiga writes a comma two ways (player career, 2026): the thousands separator of an English
    number (``"4,583.33"``) and the decimal mark of a Finnish one (``"0,00"``). A comma is a
    thousands separator when the string also has a ``.`` or every comma is followed by exactly
    three digits (``"4,583"`` -> 4583); otherwise it is the decimal mark (``"12,5"`` -> 12.5).
    """
    text = expr.str.strip_chars()
    grouped = text.str.contains(".", literal=True) | text.str.contains(_THOUSANDS)
    fixed = pl.when(grouped).then(text.str.replace_all(",", "", literal=True))
    return fixed.otherwise(text.str.replace(",", ".", literal=True)).cast(pl.Float64, strict=False)


def _typed(df: pl.DataFrame) -> pl.DataFrame:
    """Ids -> Int64 (lossless only), rate columns -> Float64 (numbers and numeric strings)."""
    df = _cast_ids_int64(df, [c for c in df.columns if _ID_COL.search(c)])
    casts = []
    for col, dtype in df.schema.items():
        if dtype == pl.Null:
            casts.append(pl.col(col).cast(_dtype_for(col)))
            continue
        if not _FLOAT_COL.search(col) or _ID_COL.search(col):
            continue
        if dtype == pl.String:
            casts.append(_number(pl.col(col)).alias(col))
        elif dtype.is_numeric() and dtype != pl.Float64:
            casts.append(pl.col(col).cast(pl.Float64))
    return df.with_columns(casts) if casts else df


def _frame(rows: Iterable[Mapping[str, Any]]) -> pl.DataFrame:
    rows = [r for r in rows if r]
    if not rows:
        return pl.DataFrame()
    try:
        df = pl.from_dicts(rows, infer_schema_length=None, strict=False)
    except Exception:  # noqa: BLE001 -- a malformed body parses to nothing, never raises
        return pl.DataFrame()
    return _typed(df)


def _finish(df: pl.DataFrame, columns: Optional[Sequence[str]], return_as_pandas: bool, parser: str = "") -> Frame:
    """Apply the parser's returns-schema dtypes and the documented columns.

    An all-null column (and a whole-number rate) takes the schema dtype, an empty frame keeps the
    documented columns and a missing one is added as nulls, all typed like a populated frame.
    """
    schema = DTYPES.get(parser, {})
    if schema and df.width:
        casts = []
        for col, dtype in df.schema.items():
            want = _dtype_for(col, schema) if col in schema else dtype
            if dtype != want and (df[col].null_count() == df.height or (want == pl.Float64 and dtype.is_numeric())):
                casts.append(pl.col(col).cast(want))
        df = df.with_columns(casts) if casts else df
    if columns:
        if df.width == 0:
            df = pl.DataFrame(schema={c: _dtype_for(c, schema) for c in columns})
        else:
            missing = [pl.lit(None, dtype=_dtype_for(c, schema)).alias(c) for c in columns if c not in df.columns]
            if missing:
                df = df.with_columns(missing)
    return df.to_pandas() if return_as_pandas else df


def _rename(row: Dict[str, Any], old: str, new: str) -> Dict[str, Any]:
    if old in row:
        row = {(new if k == old else k): v for k, v in row.items()}
    return row


def _stamp(row: Dict[str, Any], req: Mapping[str, Any]) -> Dict[str, Any]:
    """Request ids first (``season``, ``game_id``, ``player_id``), never overriding the body."""
    head = {k: req[k] for k in ("season", "game_id", "player_id") if k in req and k not in row}
    return {**head, **row}


# --- generic ------------------------------------------------------------------------------------


def _rows(raw: Any) -> List[Mapping[str, Any]]:
    if isinstance(raw, list):
        return _list(raw)
    if not isinstance(raw, dict) or not raw:
        return []
    body = {k: v for k, v in raw.items() if k != _REQUEST}
    if len(body) >= 2 and all(isinstance(v, dict) for v in body.values()) and all(_MAP_KEY.match(k) for k in body):
        return [{"id": k, **v} for k, v in body.items()]
    lists = [k for k, v in body.items() if isinstance(v, list)]
    if len(lists) == 1 and all(not isinstance(v, (list, dict)) for k, v in body.items() if k != lists[0]):
        return _list(body[lists[0]])
    return [body]


def parse_liiga(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Generic Liiga body -> frame by the module's row rule.

    Serves ``/tournament``, ``/gameweeks`` (one row), ``/teams/info`` with ``team`` (the team
    history), ``/teams/stats`` (rows from ``teamStats``) and ``/players/stats/summed``.

    Args:
        raw: the decoded JSON body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per record; zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga
            parse_liiga({"teamStats": [{"teamId": 55786244, "goals": 3}]})
    """
    return _finish(_frame(_flatten(r) for r in _rows(raw)), columns, return_as_pandas, "parse_liiga")


# --- games and schedule -------------------------------------------------------------------------


def _side(prefix: str, block: Mapping[str, Any]) -> Dict[str, Any]:
    """A team block of a game as ``<prefix>_*`` columns (``teamName`` -> ``<prefix>_name``)."""
    out: Dict[str, Any] = {}
    for key, value in block.items():
        if key in _EVENT_KEYS:
            continue
        if key == "teamId":
            out.update(_team(f"{prefix}_team_id", value))
            continue
        name = key[4].lower() + key[5:] if key.startswith("team") and key[4:5].isupper() else key
        out.update(_flatten({name: value}, f"{prefix}_"))
    return out


def _game_row(game: Mapping[str, Any]) -> Dict[str, Any]:
    head = {k: v for k, v in game.items() if k not in ("homeTeam", "awayTeam", "winningShotCompetitionEvents")}
    row = _rename(_flatten(head), "id", "game_id")
    for prefix, key in _SIDES:
        row.update(_side(prefix, _obj(game.get(key))))
    return row


def parse_liiga_games(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/games`` -> one row per game: teams, goals, xG, power plays, rink, periods (JSON).

    Goal and penalty events are their own frames (:func:`parse_liiga_goals`).

    Args:
        raw: the ``/games`` body (a list of games).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per game keyed by ``(season, game_id)``; ``home_*`` / ``away_*`` team columns.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_games
            parse_liiga_games([{"id": 1, "season": 2026, "homeTeam": {"teamId": "55786244:hpk"}}])
    """
    return _finish(_frame(_game_row(g) for g in _list(raw)), columns, return_as_pandas, "parse_liiga_games")


def _goal_rows(game: Mapping[str, Any], req: Mapping[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    base = {"season": game.get("season", req.get("season")), "game_id": game.get("id", req.get("game_id"))}
    for side, key in _SIDES:
        team = _obj(game.get(key))
        tid = _team("team_id", team.get("teamId"))
        for ev in _list(team.get("goalEvents")):
            ev = dict(ev)
            scorer = _obj(ev.pop("scorerPlayer", None))
            ids = ev.pop("assistantPlayerIds", None)
            assists = _list(ev.pop("assistantPlayers", None))
            so_far = _obj(ev.pop("assistsSoFarInSeason", None))
            types = ev.pop("goalTypes", None)
            plus, minus = ev.pop("plusPlayerIds", None), ev.pop("minusPlayerIds", None)
            row: Dict[str, Any] = {**base, "side": side, **tid}
            row.update(_flatten({k: v for k, v in ev.items() if k != "scorerPlayerId"}))
            row["scorer_player_id"] = ev.get("scorerPlayerId", scorer.get("playerId"))
            row["scorer_first_name"], row["scorer_last_name"] = scorer.get("firstName"), scorer.get("lastName")
            ids = ids if isinstance(ids, list) else [a.get("playerId") for a in assists]
            for n in (1, 2):
                pid = ids[n - 1] if len(ids) >= n else None
                who: Mapping[str, Any] = (
                    next((a for a in assists if a.get("playerId") == pid), {}) if pid is not None else {}
                )
                row[f"assist{n}_player_id"] = pid
                row[f"assist{n}_first_name"], row[f"assist{n}_last_name"] = who.get("firstName"), who.get("lastName")
                row[f"assist{n}_season_assists"] = so_far.get(str(pid)) if pid is not None else None
            row["goal_types"] = ",".join(str(t) for t in types) if isinstance(types, list) else None
            row["plus_jerseys"], row["minus_jerseys"] = plus, minus
            out.append(row)
    return out


def parse_liiga_goals(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/games`` -> one row per goal of every game (the games' ``goalEvents``, flattened).

    ``goal_types`` joins Liiga's Finnish codes with ``,`` (YV power play, AV short-handed,
    TM empty net, RL penalty shot); ``plus_jerseys`` / ``minus_jerseys`` are the jersey
    numbers on ice (space-separated), not player ids. Up to two assists, each with the
    assister's season assist count after the goal.

    Args:
        raw: the ``/games`` body (a list of games).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per goal keyed by ``(season, game_id, event_id)``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_goals
            parse_liiga_goals([])  # zero-row frame
    """
    rows = [r for g in _list(raw) for r in _goal_rows(g, {})]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_goals")


def parse_liiga_schedule(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/schedule`` -> one row per game (ids, teams, goals, xG, start, rink).

    The schedule's ``serie`` is the integer tournament id (``serie_id``), unlike the
    uppercase code of ``/games``.

    Args:
        raw: the ``/schedule`` body (a list of games).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per game keyed by ``(season, game_id)``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_schedule
            parse_liiga_schedule([{"id": 1, "season": 2026, "serie": 1188, "homeTeamId": "55786244:hpk"}])
    """
    rows = (_rename(_rename(_flatten(r), "id", "game_id"), "serie", "serie_id") for r in _list(raw))
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_schedule")


# --- one game -----------------------------------------------------------------------------------


def _game_of(raw: Any) -> Tuple[Mapping[str, Any], Dict[str, Any]]:
    """``(game block, {season, game_id})`` of a ``/games/{season}/{id}`` body."""
    game = _obj(raw.get("game")) if isinstance(raw, dict) else {}
    req = _request(raw)
    ids = {"season": game.get("season", req.get("season")), "game_id": game.get("id", req.get("game_id"))}
    return game, ids


def parse_liiga_game(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/games/{season}/{id}`` -> one row: the game header (teams, score, xG, rink, referees).

    Args:
        raw: the game body (``{game, awards, homeTeamPlayers, awayTeamPlayers}``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row keyed by ``(season, game_id)``; ``periods`` and ``referees`` as JSON.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_game
            parse_liiga_game({"game": {"id": 1, "season": 2026}})
    """
    game, _ = _game_of(raw)
    return _finish(_frame([_game_row(game)] if game else []), columns, return_as_pandas, "parse_liiga_game")


def parse_liiga_game_goals(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/games/{season}/{id}`` -> one row per goal (same columns as :func:`parse_liiga_goals`).

    Args:
        raw: the game body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per goal keyed by ``(season, game_id, event_id)``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_game_goals
            parse_liiga_game_goals({})  # zero-row frame
    """
    game, ids = _game_of(raw)
    return _finish(_frame(_goal_rows(game, ids) if game else []), columns, return_as_pandas, "parse_liiga_game_goals")


def parse_liiga_game_penalties(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/games/{season}/{id}`` -> one row per penalty, both teams.

    ``penalty_fault_type`` / ``penalty_fault_name`` are Finnish (``KII`` Kiinnipitäminen,
    holding); begin and end times are seconds from the start of the game.

    Args:
        raw: the game body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per penalty keyed by ``(season, game_id, event_id)``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_game_penalties
            parse_liiga_game_penalties({})  # zero-row frame
    """
    game, ids = _game_of(raw)
    rows = []
    for side, key in _SIDES:
        team = _obj(game.get(key))
        tid = _team("team_id", team.get("teamId"))
        rows += [{**ids, "side": side, **tid, **_flatten(ev)} for ev in _list(team.get("penaltyEvents"))]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_game_penalties")


def parse_liiga_game_shootout(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/games/{season}/{id}`` -> one row per shootout attempt (``winningShotCompetitionEvents``).

    Args:
        raw: the game body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per attempt, in order (``shot_number``); zero rows for a game without one.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_game_shootout
            parse_liiga_game_shootout({})  # zero-row frame
    """
    game, ids = _game_of(raw)
    rows = [{**ids, **_flatten(ev)} for ev in _list(game.get("winningShotCompetitionEvents"))]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_game_shootout")


def parse_liiga_game_lineups(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/games/{season}/{id}`` -> both lineups, one row per player (line, role, jersey, bio).

    Args:
        raw: the game body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per player keyed by ``(season, game_id, player_id)``, with ``side``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_game_lineups
            parse_liiga_game_lineups({})  # zero-row frame
    """
    _, ids = _game_of(raw)
    body = raw if isinstance(raw, dict) else {}
    rows = [
        {**ids, "side": side, **_rename(_flatten(p), "id", "player_id")}
        for side, key in (("home", "homeTeamPlayers"), ("away", "awayTeamPlayers"))
        for p in _list(body.get(key))
    ]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_game_lineups")


def parse_liiga_game_players_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/games/{season}/{id}?dataType=playersWithStats`` -> lineups with career and season stats.

    Each lineup row (``playerDTO``) gains ``all_time_stats_*``, ``season_stats_*``,
    ``season_stats_in_team_*`` and ``vs_team_stats_*`` columns.

    Args:
        raw: the ``playersWithStats`` body (the two lineups only).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per player keyed by ``(season, game_id, player_id)``, with ``side``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_game_players_stats
            parse_liiga_game_players_stats({})  # zero-row frame
    """
    req = _request(raw)
    ids = {"season": req.get("season"), "game_id": req.get("game_id")}
    body = raw if isinstance(raw, dict) else {}
    rows = []
    for side, key in (("home", "homeTeamPlayers"), ("away", "awayTeamPlayers")):
        for p in _list(body.get(key)):
            row = {**ids, "side": side, **_rename(_flatten(_obj(p.get("playerDTO"))), "id", "player_id")}
            row.update(_flatten({k: v for k, v in p.items() if k != "playerDTO"}))
            rows.append(row)
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_game_players_stats")


# --- box score ----------------------------------------------------------------------------------


def parse_liiga_game_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/games/stats/{season}/{id}`` -> the per-period player box score, skaters and goalies.

    One row per player and period. Goalies (``is_goalie``) add ``saves``,
    ``goals_allowed``, ``saves_percentage`` and ``shots_on_goal``. Tracking columns
    (``distance``, the pass counts, ``expected_goals_*``) are null where Liiga has no
    tracking; ``top_speed`` and ``hardest_shot`` (unit not stated, values fit m/s) are per
    game, repeated on each period row. Tracking is absent in 2015-16 and present by 2020-21
    (the start between was not narrowed); the box score itself goes back to at least 2015.

    Args:
        raw: the box-score body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per ``(season, game_id, player_id, period)``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_game_stats
            parse_liiga_game_stats({})  # zero-row frame
    """
    body = raw if isinstance(raw, dict) else {}
    req = _request(raw)
    ids = {"season": req.get("season"), "game_id": req.get("game_id")}
    rows: List[Dict[str, Any]] = []
    for side, key in _SIDES:
        for tp in _list(body.get(key)):
            head = {**ids, "side": side, **_team("team_id", tp.get("teamId")), "period": tp.get("period")}
            for is_goalie, group in ((False, "periodPlayerStats"), (True, "goaliePeriodStats")):
                for ps in _list(tp.get(group)):
                    stats = {k: v for k, v in _obj(ps.get("period")).items() if k != "period"}
                    rest = {k: v for k, v in ps.items() if k not in ("period", "jerseyId", "playerId")}
                    row = {**head, "player_id": ps.get("playerId"), "jersey": ps.get("jerseyId")}
                    row["is_goalie"] = is_goalie
                    row.update(_flatten(stats))
                    row.update(_flatten(rest))
                    rows.append(row)
    df = _frame(rows)
    tracking = _frame(
        _flatten(r) for key in ("homeTeamGamePlayerStats", "awayTeamGamePlayerStats") for r in _list(body.get(key))
    )
    if df.height and tracking.height and "player_id" in tracking.columns:
        tracking = tracking.unique(subset="player_id", keep="first", maintain_order=True)
        assert df.schema["player_id"] == tracking.schema["player_id"], "player_id dtypes differ"
        df = df.join(tracking, on="player_id", how="left", suffix="_game")
    return _finish(df, columns, return_as_pandas, "parse_liiga_game_stats")


def parse_liiga_game_team_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/games/stats/{season}/{id}`` -> per-period team stats, with puck possession.

    ``power_play_instances`` / ``short_handed_instances`` are JSON lists of Liiga's codes.
    ``puck_control_duration`` is this team's puck-control seconds in the period (from
    ``puckStats``), ``opponent_puck_control_duration`` the other team's and
    ``contested_control_duration`` the contested seconds.

    Args:
        raw: the box-score body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per ``(season, game_id, team_id, period)``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_game_team_stats
            parse_liiga_game_team_stats({})  # zero-row frame
    """
    body = raw if isinstance(raw, dict) else {}
    req = _request(raw)
    ids = {"season": req.get("season"), "game_id": req.get("game_id")}
    puck = {_key(p.get("periodNumber")): p for p in _list(body.get("puckStats"))}
    rows = []
    for side, key in _SIDES:
        other = "away" if side == "home" else "home"
        for tp in _list(body.get(key)):
            row = {**ids, "side": side, "period": tp.get("period")}
            row.update(_flatten({k: v for k, v in tp.items() if k not in ("periodPlayerStats", "goaliePeriodStats")}))
            period = _key(tp.get("period"))
            p = _obj(puck.get(period) if period is not None else None)
            row["puck_control_duration"] = p.get(f"{side}TeamControlDuration")
            row["opponent_puck_control_duration"] = p.get(f"{other}TeamControlDuration")
            row["contested_control_duration"] = p.get("contestedControlDuration")
            rows.append(row)
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_game_team_stats")


def parse_liiga_shotmap(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/shotmap/{season}/{id}`` -> one row per shot.

    ``shot_x`` / ``shot_y`` are rink-image PIXELS, origin and orientation undocumented
    (``left_team_id`` / ``right_team_id`` say which team attacks which end); there is no
    per-shot xG. Shot maps exist from 2016 (2015-16); earlier games answer ``[]``.

    Args:
        raw: the shot-map body (a list of shots).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per shot keyed by ``(season, game_id)`` and game time.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_shotmap
            parse_liiga_shotmap([{"shootingTeamId": 651304385, "shotX": 684, "shotY": 491}])
    """
    rows = (_stamp(_flatten(r), _request(r)) for r in _list(raw))
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_shotmap")


# --- season tables ------------------------------------------------------------------------------


def parse_liiga_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/standings/`` -> one row per team and table (regular season, playoffs, preseason, ...).

    ``tournament`` names the table in the request vocabulary (``runkosarja``,
    ``playoffs``, ``valmistavat_ottelut``, ``playout``, ``qualifications``).
    ``power_play_percentage``, ``short_handed_percentage`` and ``win_percentage`` are 0-1
    fractions (Liiga sends them as strings); ``internal_id`` equals ``team_id``.

    Args:
        raw: the standings body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per ``(season, tournament, team_id)``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_standings
            parse_liiga_standings({"season": [{"teamId": "362185137:tappara", "ranking": 1}]})
    """
    body = raw if isinstance(raw, dict) else {}
    req = _request(raw)
    rows = [
        {"season": req.get("season"), "tournament": tournament, **_flatten(r)}
        for key, tournament in _STANDINGS_TABLES.items()
        for r in _list(body.get(key))
    ]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_standings")


def parse_liiga_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/teams/info`` without ``team`` -> the team directory, one row per current or past team.

    ``teamtournamentstats`` and ``externals`` stay JSON; ``contact_info`` /
    ``general_info`` are HTML as sent.

    Args:
        raw: the ``{teams, logos}`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per team keyed by ``team_id`` (+ ``team_slug``).

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_teams
            parse_liiga_teams({"teams": {"55786244:hpk": {"id": "55786244", "slug": "hpk", "name": "HPK"}}})
    """
    teams = _obj(raw.get("teams")) if isinstance(raw, dict) else {}
    rows = []
    for key, team in teams.items():
        if not isinstance(team, dict):
            continue
        num, slug = _split_team_id(key)
        row = {k: v for k, v in _flatten(team).items() if k not in ("id", "slug")}
        rows.append({"team_id": _split_team_id(team.get("id"))[0] or num, "team_slug": team.get("slug") or slug, **row})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_teams")


def parse_liiga_players_info(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/players/info`` -> the roster: one row per player and team in the season range.

    Args:
        raw: the roster body (a list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per player keyed by ``player_id`` (+ ``team_id``, ``team_slug``).

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_players_info
            parse_liiga_players_info([{"id": 21415137, "teamId": "219244634:jyp"}])
    """
    rows = (_rename(_flatten(r), "id", "player_id") for r in _list(raw))
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_players_info")


# --- one player ---------------------------------------------------------------------------------


def parse_liiga_player(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``/players/info/{playerId}`` -> one row: the player's bio.

    ``teams`` (season -> team, jersey, position), ``teamSeasons``, ``teamList`` and
    ``activeSeasons`` stay JSON; the career rows are :func:`parse_liiga_player_career`.

    Args:
        raw: the player-profile body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row keyed by ``player_id`` (the body's ``fihaId``).

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_player
            parse_liiga_player({"fihaId": 21870028, "firstName": "Harri"})
    """
    body = {k: v for k, v in raw.items() if k != "historical"} if isinstance(raw, dict) else {}
    row = _rename(_flatten(body), "fiha_id", "player_id")
    return _finish(_frame([row] if body else []), columns, return_as_pandas, "parse_liiga_player")


def parse_liiga_player_career(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/players/info/{playerId}`` -> the career: one row per season and tournament.

    Percentage strings become numbers (``"0,00"`` and ``"4,583.33"`` included; Liiga's
    ``win_percentage`` here is not a 0-100 rate, it is passed through as sent).
    ``end_time`` is an ISO 8601 timestamp here, not seconds.

    Args:
        raw: the player-profile body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per ``(player_id, season, tournament)``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_player_career
            parse_liiga_player_career({"fihaId": 1, "historical": {"regular": [{"season": 2026}]}})
    """
    body = raw if isinstance(raw, dict) else {}
    pid = body.get("fihaId")
    rows = [{"player_id": pid, **_flatten(r)} for v in _obj(body.get("historical")).values() for r in _list(v)]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_player_career")


def parse_liiga_player_game_log(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/players/info/{playerId}/games/{season}`` -> one row per game the player played.

    ``tournament`` is the body's list key (``regular``, ``playoffs``, ``practice``,
    ``qualifications``, ``playout``, ``chl``, ``pitsiturnaus``).

    Args:
        raw: the game-log body (lists keyed by tournament).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per ``(player_id, season, tournament, game_id)``.

    Example:
        Quick start::

            from sportsdataverse.hockey.liiga import parse_liiga_player_game_log
            parse_liiga_player_game_log({"regular": [{"gameId": 1, "date": "2025-09-09T15:30:00Z"}]})
    """
    body = raw if isinstance(raw, dict) else {}
    req = _request(raw)
    ids = {"player_id": req.get("player_id"), "season": req.get("season")}
    rows = [
        {**ids, "tournament": key, **_flatten(r)}
        for key, value in body.items()
        if key != _REQUEST
        for r in _list(value)
    ]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_liiga_player_game_log")
