"""Parsers for the KHL site REST family (``khl_rest``: ``POST https://www.khl.ru/rest/<route>/``).

Every body is the Bitrix envelope ``{"status": "success", "data": {...}, "errors": []}``. An
envelope whose ``status`` is not ``"success"`` or whose ``errors`` is non-empty parses to a zero-row
frame here (the runtime raises on it before a parser ever sees it). Each route has its own shaper,
because no generic row rule fits these deeply nested page objects:

* ``/calendar/list/`` -> :func:`parse_khl_schedule`: one row per game of the tournament. The body
  also carries the All-Star games under their OWN tournament id (1423 inside 1369, 1353 inside
  1288); those rows are dropped by keeping only the requested tournament (``data.values``).
* ``/game/text/`` feeds four shapers: :func:`parse_khl_game_info` (one row),
  :func:`parse_khl_player_box` (``tab_stats.playerstats.{A,B}[period]``),
  :func:`parse_khl_pbp` (``tab_playbyplay.gameLegend``) and :func:`parse_khl_shots`
  (``tab_shotsmap.shots``, 2024/25 on).
* ``/game/header/`` -> :func:`parse_khl_game_header`; ``/game/protocol/`` -> :func:`parse_khl_game_protocol`
  (one row per goal); ``/standings/regular/`` -> :func:`parse_khl_standings`;
  ``/standings/playoff/`` -> :func:`parse_khl_playoff_bracket`; ``/stat/players/`` ->
  :func:`parse_khl_player_stats`; ``/players/profile/`` -> :func:`parse_khl_player_profile`;
  ``/players/stats/`` -> :func:`parse_khl_player_career`; ``/clubs/team/`` -> :func:`parse_khl_roster`;
  ``/filters/statplayers/`` -> :func:`parse_khl_stat_filters`.

**Id key spaces** (every id is ``Int64``, cast here at the boundary; never a float-origin string):

* ``game_id`` -- khl.ru game id (calendar ``id``); equals webcaster ``event.khl_id`` / ``match_id``.
* ``tournament_id`` -- khl.ru tournament (stage) id ``tnId``: one per season stage (1369 = 2025/26
  regular season, 1370 = its playoff). Equals webcaster ``stage.khl_id``.
* ``*club_id`` -- the STABLE club id (``teama`` / ``teamb`` / ``clubid`` / ``team.gid``; 7 = Spartak).
  Only the club id joins webcaster ``team.khl_id``.
* ``*team_season_id`` -- a per-tournament team-season id (``homeId`` / ``visitorId`` / ``cid`` /
  ``clubidt`` / ``teid`` / ``tid``; Spartak in 1369 = 14934). It joins nothing in the webcaster API;
  never join it to a club id.
* ``player_id`` -- khl.ru player id; equals webcaster ``player.khl_id``.
* ``jn`` -- a jersey number, NOT an id. Box score, play-by-play and shot rows carry ``jn`` and a side
  (``A`` = home, ``B`` = visitor); ``player_id`` is resolved through the same body's
  ``players.{A,B}.<jn>.id``. ``gameItem.gkA`` / ``gkB`` are jerseys too (``gk_a_jn`` / ``gk_b_jn``).

Contract: polars by default, pandas with ``return_as_pandas=True``; empty or malformed input returns
a zero-row frame, never an exception, typed exactly like a populated one (the returns schemas in
:data:`SCHEMAS`, so ``pl.concat`` of an empty and a full answer works); a documented column a body
lacks is backfilled as typed nulls; snake_case columns. Values stay as the site sends them: www.khl.ru names are Russian
(``*_en`` siblings carry English where the site has them).
"""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64
from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.khl.khl_schemas import SCHEMAS as _SPECS

__all__ = [
    "parse_khl_game_header",
    "parse_khl_game_info",
    "parse_khl_game_protocol",
    "parse_khl_pbp",
    "parse_khl_player_box",
    "parse_khl_player_career",
    "parse_khl_player_profile",
    "parse_khl_player_stats",
    "parse_khl_playoff_bracket",
    "parse_khl_roster",
    "parse_khl_schedule",
    "parse_khl_shots",
    "parse_khl_standings",
    "parse_khl_stat_filters",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Columns = Optional[Sequence[str]]

#: Generic id columns (wire names snake_cased), cast losslessly to Int64.
_ID = re.compile(r"(^|_)(id|khl_id|tn_id|clubid|clubidt|cid|gid|tid|teid|teama|teamb|arenaid|key)$")
_PLAYER_LINK = re.compile(r"/players/(\d+)/")
_PDF_IDS = re.compile(r"/pdf/(\d+)/(\d+)/")
_MMSS = re.compile(r"^(\d+):(\d{2})$")
#: Rate columns: always Float64, even when the first rows happen to be whole numbers.
_FLOAT_STATS = frozenset({"ts", "dt", "as", "pcd", "dt_a", "dt_b"})


# --- shared helpers -----------------------------------------------------------------------


def _out(df: pl.DataFrame, return_as_pandas: bool) -> Frame:
    return df.to_pandas() if return_as_pandas else df


def _data(raw: Any) -> Dict[str, Any]:
    """The ``data`` object of a success envelope (``{}`` for an error envelope or anything else)."""
    if not isinstance(raw, dict):
        return {}
    if "status" in raw and (str(raw.get("status")).lower() != "success" or raw.get("errors")):
        return {}
    data = raw.get("data", raw)
    return data if isinstance(data, dict) else {}


def _obj(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _int(value: Any) -> Optional[int]:
    """An int from an int, an integral float or a numeric string; ``None`` for anything else."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    text = str(value).strip()
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def _float(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def _str(value: Any) -> Optional[str]:
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value)
    return text if text.strip() else None


def _coerce(value: Any, dtype: Any) -> Any:
    if dtype == pl.Int64:
        return _int(value)
    if dtype == pl.Float64:
        return _float(value)
    if dtype == pl.Boolean:
        return None if value is None or value == "" else bool(value)
    return _str(value)


def _typed(rows: Iterable[Mapping[str, Any]], schema: Mapping[str, Any]) -> pl.DataFrame:
    """Rows -> a frame with exactly ``schema`` (zero rows keep the schema)."""
    rows = list(rows)
    return pl.DataFrame({c: [_coerce(r.get(c), t) for r in rows] for c, t in schema.items()}, schema=dict(schema))


_CODES = {"i": pl.Int64, "f": pl.Float64, "s": pl.Utf8, "b": pl.Boolean}
#: Every KHL returns schema (``khl_<short>`` / ``khl_webcaster_<short>``) as ``{column: dtype}``, from
#: :mod:`sportsdataverse.hockey.khl.khl_schemas` (generated from ``tools/codegen/schemas/native/khl_*``).
SCHEMAS: Dict[str, Dict[str, Any]] = {
    key: {name: _CODES[code] for name, code in (tok.rsplit(":", 1) for tok in spec.split())}
    for key, spec in _SPECS.items()
}
_BY_COLUMNS: Dict[tuple, Dict[str, Any]] = {}
for _schema in SCHEMAS.values():
    _BY_COLUMNS.setdefault(tuple(_schema), _schema)


def _columns(columns: Columns, key: str) -> Sequence[str]:
    """The wrapper's documented ``columns``, else the parser's own returns schema ``key``."""
    return list(columns) if columns is not None else list(SCHEMAS[key])


def _documented(columns: Columns) -> Dict[str, Any]:
    """``{column: dtype}`` of the returns schema whose column list is exactly ``columns`` (else ``{}``)."""
    return _BY_COLUMNS.get(tuple(columns or ()), {})


def _empty(columns: Columns) -> pl.DataFrame:
    """A zero-row frame for the documented columns, typed as the returns schema types them.

    ``columns`` that match no KHL returns schema fall back to ids ``Int64``, everything else ``Utf8``.
    """
    schema = _documented(columns) or {c: (pl.Int64 if _ID.search(c) else pl.Utf8) for c in columns or ()}
    return pl.DataFrame(schema=schema)


def _conform(df: pl.DataFrame, columns: Columns) -> pl.DataFrame:
    """Documented columns first, typed as documented, then any extra the body carried.

    A documented column the body lacks is backfilled as typed nulls and an all-null one is cast, so a
    sparse body has the dtypes of a full one; a column holding values keeps them (never nulled by a cast).
    """
    schema = _documented(columns)
    if not schema:
        return df
    if df.height == 0:
        return pl.DataFrame(schema=schema)
    fixes = [
        pl.lit(None, dtype=t).alias(c) if c not in df.columns else pl.col(c).cast(t)
        for c, t in schema.items()
        if c not in df.columns or (df.schema[c] != t and df[c].null_count() == df.height)
    ]
    df = df.with_columns(fixes) if fixes else df
    return df.select([*schema, *[c for c in df.columns if c not in schema]])


def _cell(value: Any) -> Any:
    return json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value


def _generic(rows: Iterable[Any], columns: Columns = None) -> pl.DataFrame:
    """Row dicts -> flattened (dotted keys) -> snake_case -> list cells as JSON text -> lossless Int64 ids."""
    flat = [_flatten(r) for r in rows if isinstance(r, dict)]
    if not flat:
        return _empty(columns)
    keys = list(dict.fromkeys(k for r in flat for k in r))
    data: Dict[str, List[Any]] = {}
    for key in keys:
        name = underscore(key).replace(".", "_")
        while name in data:  # ``homeId`` and ``home_id`` in one body: keep both
            name += "_"
        values = [_cell(r.get(key)) for r in flat]
        kinds = {type(v) for v in values if v is not None}
        if len(kinds) > 1 and kinds != {int, float}:  # mixed (1 and "1"): keep it text
            values = [None if v is None else str(v) for v in values]
        data[name] = values
    df = pl.DataFrame(data, strict=False)
    df = df.with_columns(pl.col(c).cast(pl.Utf8) for c, t in df.schema.items() if t == pl.Null)
    return _cast_ids_int64(df, [c for c in df.columns if _ID.search(c)])


def _flatten(obj: Mapping[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Nested dicts -> one level of dotted keys; lists stay cells (no pandas dtype coercion)."""
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        name = f"{prefix}{key}"
        if isinstance(value, dict) and value:
            out.update(_flatten(value, name + "."))
        else:
            out[name] = None if value == {} else value
    return out


def _stat_cast(df: pl.DataFrame, cols: Iterable[str]) -> pl.DataFrame:
    """Type stat columns sent as numbers or numeric strings.

    A column with any ``mm:ss`` value stays text; a rate (``*_pct`` / ``*_avg`` / ``ts`` / ``dt`` /
    ``as`` / ``pcd``) is always ``Float64``, so a page whose first rows are whole numbers never types a
    rate as an integer; any other all-integer column is ``Int64``, an all-numeric one ``Float64``.
    ``"-"`` / ``""`` are null.
    """
    exprs = []
    for c in cols:
        if c not in df.columns:
            continue
        text = df[c].cast(pl.Utf8).str.strip_chars()
        values = [v for v in text.to_list() if v not in (None, "", "-")]
        if any(":" in v for v in values):
            continue
        if c.endswith(("_pct", "_avg")) or c in _FLOAT_STATS:
            exprs.append(text.cast(pl.Float64, strict=False).alias(c))
        elif all(re.fullmatch(r"-?\d+", v) for v in values):
            exprs.append(text.cast(pl.Int64, strict=False).alias(c))
        elif all(_float(v) is not None for v in values):
            exprs.append(text.cast(pl.Float64, strict=False).alias(c))
    return df.with_columns(exprs) if exprs else df


def _player_id(link: Any) -> Optional[int]:
    m = _PLAYER_LINK.search(str(link or ""))
    return int(m.group(1)) if m else None


def _seconds(value: Any) -> Optional[int]:
    """Game-clock seconds: an int, a numeric string (2013-era bodies) or ``mm:ss``."""
    secs = _int(value)
    if secs is not None:
        return secs
    m = _MMSS.match(str(value or "").strip())
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _game(raw: Any) -> Dict[str, Any]:
    return _obj(_data(raw).get("game"))


def _ids(game: Mapping[str, Any]) -> Dict[str, Optional[int]]:
    data = _obj(_obj(game.get("info")).get("data"))
    return {"game_id": _int(data.get("id")), "tournament_id": _int(data.get("tnId"))}


def _roster(game: Mapping[str, Any]) -> pl.DataFrame:
    """``players.{A,B}.<jn>`` -> ``(team_side, jn, player_id, player_name, position)``."""
    rows = []
    for side in ("A", "B"):
        for jn, p in _obj(_obj(game.get("players")).get(side)).items():
            p = _obj(p)
            rows.append(
                {
                    "team_side": side,
                    "jn": p.get("jn", jn),
                    "player_id": p.get("id"),
                    "player_name": p.get("name"),
                    "position": p.get("pos"),
                }
            )
    schema = {"team_side": pl.Utf8, "jn": pl.Int64, "player_id": pl.Int64, "player_name": pl.Utf8, "position": pl.Utf8}
    return _typed(rows, schema).unique(["team_side", "jn"], keep="first", maintain_order=True)


def _with_players(df: pl.DataFrame, roster: pl.DataFrame, side: str, jn: str, out: str) -> pl.DataFrame:
    """Left-join ``roster.player_id`` onto ``(side, jn)`` as ``out`` (dtypes asserted first)."""
    right = roster.select(pl.col("team_side").alias(side), pl.col("jn").alias(jn), pl.col("player_id").alias(out))
    for key in (side, jn):
        assert df.schema[key] == right.schema[key], f"{key}: {df.schema[key]} != {right.schema[key]}"
    return df.join(right, on=[side, jn], how="left")


# --- game pages ----------------------------------------------------------------------------

#: Wire key -> column, for the game objects of game/header and game/text (the id traps renamed).
_GAME_RENAMES = {
    "id": "game_id",
    "tnId": "tournament_id",
    "teama": "home_club_id",
    "homeId": "home_team_season_id",
    "teamb": "away_club_id",
    "visitorId": "away_team_season_id",
    "arenaid": "arena_id",
    "homeScore": "home_score",
    "visitorScore": "away_score",
    "gkA": "gk_a_jn",
    "gkB": "gk_b_jn",
}
_GAME_INTS = (
    *("game_id", "tournament_id", "home_club_id", "home_team_season_id", "away_club_id"),
    *("away_team_season_id", "arena_id", "home_score", "away_score", "round", "number"),
    *("dayofweek", "changes", "attendance", "gk_a_jn", "gk_b_jn", "gk_a_player_id", "gk_b_player_id"),
)


def _game_row(*objs: Any) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for obj in objs:
        for key, value in _obj(obj).items():
            if isinstance(value, (dict, list)):
                continue
            row.setdefault(_GAME_RENAMES.get(key) or underscore(key), value)
    return row


def _game_frame(row: Dict[str, Any], columns: Columns) -> pl.DataFrame:
    if not row.get("game_id"):
        return _empty(columns)
    lead = [c for c in _GAME_INTS[:9] if c in row]
    ordered = {c: row[c] for c in [*lead, *[k for k in row if k not in lead]]}
    df = pl.DataFrame({k: [_str(v) if not isinstance(v, (int, float, bool)) else v] for k, v in ordered.items()})
    df = df.with_columns(pl.col(c).cast(pl.Utf8) for c, t in df.schema.items() if t == pl.Null)
    return df.with_columns(pl.col(c).cast(pl.Utf8).cast(pl.Int64, strict=False) for c in _GAME_INTS if c in df.columns)


def parse_khl_game_header(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/game/header/`` -> one row: the game (``data.game``), with the id traps renamed.

    Args:
        raw: the ``/game/header/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row: ``game_id``, ``tournament_id``, ``home_club_id`` / ``away_club_id`` (club ids),
        ``home_team_season_id`` / ``away_team_season_id`` (team-season ids), ``arena_id``, the score
        and every other scalar of ``data.game`` snake_cased; zero rows when unusable.

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_game_header
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_game_header
            raw = khl_game_header(game_id=898236, season=2026, return_parsed=False)
            df = parse_khl_game_header(raw)

        As pandas::

            df_pd = parse_khl_game_header(raw, return_as_pandas=True)

        Next step::

            df.select("game_id", "home_score", "away_score", "ots")
    """
    columns = _columns(columns, "khl_game_header")
    return _out(_conform(_game_frame(_game_row(_game(raw)), columns), columns), return_as_pandas)


def parse_khl_game_info(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/game/text/`` -> one row: game info (``info.data``) plus the game status (``gameItem``).

    ``gameItem.gkA`` / ``gkB`` are the starting goalies' JERSEY numbers: they land as ``gk_a_jn`` /
    ``gk_b_jn`` and are resolved to ``gk_a_player_id`` / ``gk_b_player_id`` through ``players``.

    Args:
        raw: the ``/game/text/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_game_info
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_game_info
            raw = khl_game_info(game_id=898236, season=2026, return_parsed=False)
            df = parse_khl_game_info(raw)

        As pandas::

            df_pd = parse_khl_game_info(raw, return_as_pandas=True)

        Next step::

            df.select("game_id", "gk_a_player_id", "gk_b_player_id")
    """
    columns = _columns(columns, "khl_game_info")
    game = _game(raw)
    row = _game_row(_obj(game.get("info")).get("data"), game.get("gameItem"))
    roster = {(r["team_side"], r["jn"]): r["player_id"] for r in _roster(game).iter_rows(named=True)}
    for side, key in (("A", "gk_a_jn"), ("B", "gk_b_jn")):
        if key in row:
            row[key.replace("_jn", "_player_id")] = roster.get((side, _int(row[key])))
    return _out(_conform(_game_frame(row, columns), columns), return_as_pandas)


_BOX_STATS = {
    "g": pl.Int64,
    "a": pl.Int64,
    "p": pl.Utf8,
    "shots": pl.Int64,
    "pim": pl.Int64,
    "fo": pl.Int64,
    "fow": pl.Int64,
    "fow_pct": pl.Float64,
    "bls": pl.Int64,
    "hits": pl.Int64,
    "fa": pl.Int64,
    "p_intc": pl.Int64,
    "sft": pl.Utf8,
    "sfte": pl.Utf8,
    "sftpp": pl.Utf8,
    "sftsh": pl.Utf8,
    "sften": pl.Utf8,
    "tiams": pl.Utf8,
    "ts": pl.Float64,
    "dt": pl.Float64,
    "as": pl.Float64,
    "pcd": pl.Float64,
}
_BOX_KEYS = {"game_id": pl.Int64, "tournament_id": pl.Int64, "period": pl.Int64, "team_side": pl.Utf8, "jn": pl.Int64}
_BOX_SCHEMA = {**_BOX_KEYS, "player_id": pl.Int64, "player_name": pl.Utf8, "position": pl.Utf8, **_BOX_STATS}


def parse_khl_player_box(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/game/text/`` -> skater box score: one row per (period, side, jersey).

    Rows come from ``tab_stats.playerstats.{A,B}[]`` (``period`` 0 = the whole game, then each period
    and overtime). Rows are keyed by jersey ``jn``; ``player_id`` comes from ``players.{A,B}.<jn>.id``.
    Goalies have no rows here. Time-on-ice cells stay ``mm:ss(shifts)`` text (``sft`` all strengths,
    ``sfte`` even, ``sftpp`` power play, ``sftsh`` short-handed, ``sften`` empty net); ``fow_pct``
    arrives as ``0`` or ``"50.00"`` and is always ``Float64``.

    Args:
        raw: the ``/game/text/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: accepted for the generated wrapper; the schema is fixed.

    Returns:
        The box score with a fixed schema (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_player_box
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_player_box
            raw = khl_player_box(game_id=898236, season=2026, return_parsed=False)
            df = parse_khl_player_box(raw)

        As pandas::

            df_pd = parse_khl_player_box(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("period") == 0).sort("g", descending=True)
    """
    game = _game(raw)
    ids = _ids(game)
    rows = []
    for side in ("A", "B"):
        for block in _list(_obj(_obj(game.get("tab_stats")).get("playerstats")).get(side)):
            block = _obj(block)
            for item in _list(block.get("data")):
                item = _obj(item)
                stats = {underscore(k): v for k, v in _obj(item.get("stats")).items()}
                rows.append({**ids, "period": block.get("period"), "team_side": side, "jn": item.get("jn"), **stats})
    box = _typed(rows, {**_BOX_KEYS, **_BOX_STATS})
    roster = _roster(game)
    assert box.schema["jn"] == roster.schema["jn"] and box.schema["team_side"] == roster.schema["team_side"]
    df = box.join(roster, on=["team_side", "jn"], how="left").select(list(_BOX_SCHEMA))
    return _out(df, return_as_pandas)


_PBP_SCHEMA = {
    "game_id": pl.Int64,
    "tournament_id": pl.Int64,
    "period": pl.Int64,
    "event_index": pl.Int64,
    "event_type": pl.Utf8,
    "time": pl.Utf8,
    "time_seconds": pl.Int64,
    "team_side": pl.Utf8,
    "jn": pl.Int64,
    "player_id": pl.Int64,
    "result": pl.Utf8,
    "result_text": pl.Utf8,
    "penalty_minutes": pl.Int64,
    "score_a": pl.Int64,
    "score_b": pl.Int64,
    "won_side": pl.Utf8,
    "won_jn": pl.Int64,
    "won_player_id": pl.Int64,
    "lost_side": pl.Utf8,
    "lost_jn": pl.Int64,
    "lost_player_id": pl.Int64,
    "won_zone": pl.Utf8,
    "shooter_side": pl.Utf8,
    "shooter_jn": pl.Int64,
    "shooter_player_id": pl.Int64,
    "period_action": pl.Utf8,
}


def parse_khl_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/game/text/`` -> play-by-play: one row per event of ``tab_playbyplay.gameLegend``.

    ``gameLegend`` maps a period number to its events, newest first; rows are returned oldest first
    (``event_index`` counts from 0 over the whole game). ``time`` DRIFTS upstream: game-elapsed seconds
    as an int (2025), as a numeric string (2013) and, on ``period`` marker rows, a wall-clock ``HH:MM``.
    ``time_seconds`` is the game-elapsed second (null on marker rows) and ``time`` its ``mm:ss`` text (the
    marker rows keep their clock text). Players are jerseys plus a side (``A`` home, ``B`` visitor);
    each jersey is resolved to a ``*player_id`` through ``players``. ``jn`` 0 is a team event
    (``player_id`` null). Faceoffs carry ``won_*`` / ``lost_*``, blocks the blocked ``shooter_*``,
    penalties ``penalty_minutes``, goals the score after the goal (``score_a`` home, ``score_b`` visitor).

    Args:
        raw: the ``/game/text/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: accepted for the generated wrapper; the schema is fixed.

    Returns:
        The play-by-play with a fixed schema (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_pbp
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_pbp
            raw = khl_pbp(game_id=898236, season=2026, return_parsed=False)
            df = parse_khl_pbp(raw)

        As pandas::

            df_pd = parse_khl_pbp(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("event_type") == "goal")
    """
    game = _game(raw)
    ids = _ids(game)
    legend = _obj(_obj(game.get("tab_playbyplay")).get("gameLegend"))
    rows: List[Dict[str, Any]] = []
    for period in sorted(legend, key=lambda k: (_int(k) is None, _int(k) or 0)):
        for ev in reversed(_list(legend[period])):
            ev = _obj(ev)
            marker = ev.get("type") == "period"
            secs = None if marker else _seconds(ev.get("time"))
            clock = ev.get("timems") or ev.get("timeMS")
            if marker or (clock is None and secs is None):
                clock = ev.get("time")
            elif clock is None and secs is not None:
                clock = f"{secs // 60:02d}:{secs % 60:02d}"
            rows.append(
                {
                    **ids,
                    "period": period,
                    "event_index": len(rows),
                    "event_type": ev.get("type"),
                    "time": clock,
                    "time_seconds": secs,
                    "team_side": ev.get("teamAB"),
                    "jn": ev.get("jn"),
                    "result": ev.get("result"),
                    "result_text": ev.get("resultText"),
                    "penalty_minutes": ev.get("mins"),
                    "score_a": ev.get("scA"),
                    "score_b": ev.get("scB"),
                    "won_side": ev.get("wonAB"),
                    "won_jn": ev.get("wonJn"),
                    "lost_side": ev.get("lostAB"),
                    "lost_jn": ev.get("lostJn"),
                    "won_zone": ev.get("wonZone"),
                    "shooter_side": ev.get("shooterAB"),
                    "shooter_jn": ev.get("shooterJn"),
                    "period_action": ev.get("action") if marker else None,
                }
            )
    df = _typed(rows, {c: t for c, t in _PBP_SCHEMA.items() if not c.endswith("player_id")})
    roster = _roster(game)
    for side, jn, out in (
        ("team_side", "jn", "player_id"),
        ("won_side", "won_jn", "won_player_id"),
        ("lost_side", "lost_jn", "lost_player_id"),
        ("shooter_side", "shooter_jn", "shooter_player_id"),
    ):
        df = _with_players(df, roster, side, jn, out)
    return _out(df.select(list(_PBP_SCHEMA)), return_as_pandas)


_SHOTS_SCHEMA = {
    "game_id": pl.Int64,
    "tournament_id": pl.Int64,
    "period": pl.Int64,
    "time": pl.Utf8,
    "time_seconds": pl.Int64,
    "team_side": pl.Utf8,
    "jn": pl.Int64,
    "player_id": pl.Int64,
    "shot_type": pl.Int64,
    "x": pl.Int64,
    "y": pl.Int64,
    "indent_left": pl.Float64,
    "indent_top": pl.Float64,
    "indent_right": pl.Float64,
    "indent_bottom": pl.Float64,
}


def parse_khl_shots(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/game/text/`` -> shot map: one row per shot of ``tab_shotsmap.shots`` (2024/25 on).

    ``x`` / ``y`` are on the site's 1000 x 500 rink grid (``indent_*`` the same point as percent offsets
    from each edge); the direction normalisation is unverified. ``shot_type`` 1 = goal, 2 = saved is
    inferred (unverified upstream). ``time_seconds`` is game-elapsed seconds; ``jn`` is resolved to
    ``player_id`` through ``players``. Earlier seasons have no shot map: zero rows.

    Args:
        raw: the ``/game/text/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: accepted for the generated wrapper; the schema is fixed.

    Returns:
        The shots with a fixed schema (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_shots
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_shots
            raw = khl_shots(game_id=898236, season=2026, return_parsed=False)
            df = parse_khl_shots(raw)

        As pandas::

            df_pd = parse_khl_shots(raw, return_as_pandas=True)

        Next step::

            df.group_by("team_side", "shot_type").len()
    """
    game = _game(raw)
    ids = _ids(game)
    rows = []
    for shot in _list(_obj(game.get("tab_shotsmap")).get("shots")):
        shot = _obj(shot)
        secs = _seconds(shot.get("time"))
        rows.append(
            {
                **ids,
                "period": shot.get("period"),
                "time": shot.get("timeMS") or (f"{secs // 60:02d}:{secs % 60:02d}" if secs is not None else None),
                "time_seconds": secs,
                "team_side": shot.get("teamAB"),
                "jn": shot.get("jn"),
                "shot_type": shot.get("shotType"),
                **{
                    underscore(k): shot.get(k)
                    for k in ("x", "y", "indentLeft", "indentTop", "indentRight", "indentBottom")
                },
            }
        )
    df = _with_players(
        _typed(rows, {c: t for c, t in _SHOTS_SCHEMA.items() if c != "player_id"}),
        _roster(game),
        "team_side",
        "jn",
        "player_id",
    )
    return _out(df.select(list(_SHOTS_SCHEMA)), return_as_pandas)


# --- schedule, protocol ---------------------------------------------------------------------

_SCHEDULE_SCHEMA = {
    "game_id": pl.Int64,
    "tournament_id": pl.Int64,
    "date": pl.Utf8,
    "time": pl.Utf8,
    "home_club_id": pl.Int64,
    "home_name": pl.Utf8,
    "away_club_id": pl.Int64,
    "away_name": pl.Utf8,
    "home_score": pl.Int64,
    "away_score": pl.Int64,
    "decision": pl.Utf8,
    "period_scores": pl.Utf8,
    "winner_club_id": pl.Int64,
    "approved": pl.Int64,
    "arena_id": pl.Int64,
    "arena_name": pl.Utf8,
}


def parse_khl_schedule(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/calendar/list/`` -> one row per game of the requested tournament.

    ``data.GAMES`` is a list of weeks, each mapping a date to its games (weeks and days can be null).
    The body also lists the All-Star games under their own tournament id (4 of 752 rows in 1369, under
    1423); only games of the requested tournament (``data.values.tournament``, else the most common
    one) are kept. Team names come from ``data.TEAMS`` and arena names from ``data.ARENAS``, both keyed
    by club / arena id. ``decision`` is ``OT`` / ``SO`` (null in regulation); ``period_scores`` joins
    the per-period ``home:away`` scores with ``", "``.

    Args:
        raw: the ``/calendar/list/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: accepted for the generated wrapper; the schema is fixed.

    Returns:
        The schedule with a fixed schema (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_schedule
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_schedule
            raw = khl_schedule(season=2026, return_parsed=False)
            df = parse_khl_schedule(raw)

        As pandas::

            df_pd = parse_khl_schedule(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("decision") == "SO").select("game_id", "home_name", "away_name")
    """
    data = _data(raw)
    games = [
        g for week in _list(data.get("GAMES")) for day in _obj(week).values() for g in _list(day) if isinstance(g, dict)
    ]
    wanted = _int(_obj(data.get("values")).get("tournament"))
    if wanted is None and games:
        wanted = Counter(_int(g.get("tnId")) for g in games).most_common(1)[0][0]
    teams, arenas = _obj(data.get("TEAMS")), _obj(data.get("ARENAS"))
    rows = []
    for g in games:
        if _int(g.get("tnId")) != wanted:
            continue
        rows.append(
            {
                "game_id": g.get("id"),
                "tournament_id": g.get("tnId"),
                "date": g.get("date"),
                "time": g.get("time_format"),
                "home_club_id": g.get("teama"),
                "home_name": _obj(teams.get(str(g.get("teama")))).get("NAME"),
                "away_club_id": g.get("teamb"),
                "away_name": _obj(teams.get(str(g.get("teamb")))).get("NAME"),
                "home_score": g.get("homeScore"),
                "away_score": g.get("visitorScore"),
                "decision": g.get("ots"),
                "period_scores": ", ".join(str(s) for s in _list(g.get("scP"))) or None,
                "winner_club_id": g.get("win") or None,
                "approved": g.get("approved"),
                "arena_id": g.get("arenaid"),
                "arena_name": arenas.get(str(g.get("arenaid"))),
            }
        )
    df = _typed(rows, _SCHEDULE_SCHEMA).sort(["date", "time", "game_id"], nulls_last=True)
    return _out(df, return_as_pandas)


_PROTOCOL_SCHEMA = {
    "game_id": pl.Int64,
    "tournament_id": pl.Int64,
    "period": pl.Int64,
    "goal_number": pl.Int64,
    "time": pl.Utf8,
    "team_side": pl.Utf8,
    "score_a": pl.Int64,
    "score_b": pl.Int64,
    "strength": pl.Utf8,
    "strength_text": pl.Utf8,
    **{
        f"{who}_{c}": t
        for who in ("scorer", "assist_1", "assist_2")
        for c, t in (("player_id", pl.Int64), ("name", pl.Utf8), ("jn", pl.Int64))
    },
    "on_ice_a": pl.Utf8,
    "on_ice_b": pl.Utf8,
}


def parse_khl_game_protocol(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/game/protocol/`` -> one row per goal (``data.goals``, keyed by period).

    The protocol body names its game only in its PDF links (``/pdf/<tournament>/<game>/``), which give
    ``game_id`` / ``tournament_id``. Scorer and assistants carry the player id (from their
    ``/players/<id>/`` link), their name as printed (with the season goal / assist count in brackets)
    and jersey; ``on_ice_a`` / ``on_ice_b`` list the jerseys on the ice. ``strength`` is the site's
    Russian code (``РАВ`` even, ``БОЛ`` power play, ...), ``strength_text`` its label.

    Args:
        raw: the ``/game/protocol/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: accepted for the generated wrapper; the schema is fixed.

    Returns:
        The goals with a fixed schema (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_game_protocol
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_game_protocol
            raw = khl_game_protocol(game_id=898236, season=2026, return_parsed=False)
            df = parse_khl_game_protocol(raw)

        As pandas::

            df_pd = parse_khl_game_protocol(raw, return_as_pandas=True)

        Next step::

            df.select("period", "time", "scorer_player_id", "strength")
    """
    data = _data(raw)
    ids: Dict[str, Optional[int]] = {"tournament_id": None, "game_id": None}
    for pdf in _obj(data.get("pdf")).values():
        m = _PDF_IDS.search(str(_obj(pdf).get("URL") or ""))
        if m:
            ids = {"tournament_id": int(m.group(1)), "game_id": int(m.group(2))}
            break
    rows = []
    for period, goals in _obj(data.get("goals")).items():
        for goal in _list(goals):
            goal = _obj(goal)
            row = {
                **ids,
                "period": goal.get("per", period),
                "goal_number": goal.get("num"),
                "time": goal.get("pmg_time"),
                "team_side": goal.get("teamAB"),
                "score_a": goal.get("scoreA"),
                "score_b": goal.get("scoreB"),
                "strength": goal.get("pmg"),
                "strength_text": goal.get("pmg_trans"),
                "on_ice_a": goal.get("oniceA"),
                "on_ice_b": goal.get("oniceB"),
            }
            for who in ("scorer", "assist_1", "assist_2"):
                person = _obj(goal.get(who))
                row.update(
                    {
                        f"{who}_player_id": _player_id(person.get("link")),
                        f"{who}_name": person.get("name"),
                        f"{who}_jn": person.get("num"),
                    }
                )
            rows.append(row)
    return _out(_typed(rows, _PROTOCOL_SCHEMA).sort(["period", "goal_number"]), return_as_pandas)


# --- standings ----------------------------------------------------------------------------

_STANDINGS_SCHEMA = {
    "table": pl.Utf8,
    "rank": pl.Int64,
    "club_id": pl.Int64,
    "name": pl.Utf8,
    "gp": pl.Int64,
    "w": pl.Int64,
    "otw": pl.Int64,
    "sow": pl.Int64,
    "sol": pl.Int64,
    "otl": pl.Int64,
    "otl_en": pl.Int64,
    "l": pl.Int64,
    "pts": pl.Int64,
    "goals_for": pl.Int64,
    "goals_against": pl.Int64,
    "is_out_playoff": pl.Boolean,
    "link": pl.Utf8,
}


def parse_khl_standings(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/standings/regular/`` -> one row per team per table (league, each conference, each division).

    ``table`` names the table (``league``, ``west`` / ``east``, or a division key such as
    ``bobrov``); ``rank`` is the position in that table. ``g`` (``"252-184"``) is split into
    ``goals_for`` / ``goals_against``. ``club_id`` is the stable club id.

    Args:
        raw: the ``/standings/regular/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: accepted for the generated wrapper; the schema is fixed.

    Returns:
        The standings with a fixed schema (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_standings
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_standings
            raw = khl_standings(season=2026, return_parsed=False)
            df = parse_khl_standings(raw)

        As pandas::

            df_pd = parse_khl_standings(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("table") == "league").select("rank", "name", "pts")
    """
    tables = _obj(_data(raw).get("json"))
    rows = []
    for kind in ("league", "conferences", "divisions"):
        for table in _list(tables.get(kind)):
            table = _obj(table)
            for team in _list(table.get("rows")):
                team = _obj(team)
                stats = _obj(team.get("s"))
                gf, _, ga = str(stats.get("g") or "").partition("-")
                rows.append(
                    {
                        "table": table.get("table_tab") or table.get("item") or kind,
                        "rank": team.get("r"),
                        "club_id": team.get("clubid"),
                        "name": team.get("name"),
                        **stats,
                        "goals_for": gf,
                        "goals_against": ga,
                        "is_out_playoff": team.get("is_out_playoff"),
                        "link": team.get("link"),
                    }
                )
    return _out(_typed(rows, _STANDINGS_SCHEMA), return_as_pandas)


_BRACKET_SCHEMA = {
    "round": pl.Int64,
    "wins_to_advance": pl.Int64,
    "pair": pl.Int64,
    "conference": pl.Utf8,
    "seed": pl.Int64,
    "club_id": pl.Int64,
    "name": pl.Utf8,
    "series_wins": pl.Int64,
    "game_ids": pl.Utf8,
}


def parse_khl_playoff_bracket(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/standings/playoff/`` -> one row per team per playoff series.

    ``data.LIST[]`` is one entry per round; its ``couple`` maps a pair number to the two teams of the
    series. ``seed`` is the team's conference seed, ``series_wins`` its wins in the series and
    ``game_ids`` the comma-joined khl.ru game ids of its series games (unplayed slots are skipped).

    Args:
        raw: the ``/standings/playoff/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: accepted for the generated wrapper; the schema is fixed.

    Returns:
        The bracket with a fixed schema (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_playoff_bracket
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_playoff_bracket
            raw = khl_playoff_bracket(season=2026, return_parsed=False)
            df = parse_khl_playoff_bracket(raw)

        As pandas::

            df_pd = parse_khl_playoff_bracket(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("round") == 1)
    """
    data = _data(raw)
    clubs = _obj(data.get("CLUBS"))
    rows = []
    for rnd in _list(data.get("LIST")):
        rnd = _obj(rnd)
        for pair, teams in _obj(rnd.get("couple")).items():
            for team in _obj(teams).values():
                team = _obj(team)
                games = [str(g.get("id")) for g in _list(team.get("games")) if isinstance(g, dict) and g.get("id")]
                rows.append(
                    {
                        "round": rnd.get("number"),
                        "wins_to_advance": rnd.get("wins"),
                        "pair": team.get("pair", pair),
                        "conference": team.get("conf"),
                        "seed": team.get("num"),
                        "club_id": team.get("id"),
                        "name": _obj(clubs.get(str(team.get("id")))).get("name"),
                        "series_wins": team.get("score"),
                        "game_ids": ",".join(games) or None,
                    }
                )
    return _out(_typed(rows, _BRACKET_SCHEMA), return_as_pandas)


# --- players and clubs ----------------------------------------------------------------------


def parse_khl_player_stats(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/stat/players/`` -> one row per player of the requested page.

    Rows give the player only as a name cell linking ``/players/<id>/``; ``player_id`` is parsed from
    that link. ``group`` is the position group of the list (``forwards``, ``goalies``, ...); every stat
    column is snake_cased and typed (rates ``Float64``, ``mm:ss`` averages text).

    Args:
        raw: the ``/stat/players/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        The page of player stats (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_player_stats
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_player_stats
            raw = khl_player_stats(position="forwards", season=2026, return_parsed=False)
            df = parse_khl_player_stats(raw)

        As pandas::

            df_pd = parse_khl_player_stats(raw, return_as_pandas=True)

        Next step::

            df.sort("pts", descending=True).head()
    """
    columns = _columns(columns, "khl_player_stats")
    rows = []
    for group in _list(_data(raw).get("list")):
        group = _obj(group)
        for item in _list(group.get("list")):
            item = _obj(item)
            cells = [_obj(c) for c in _list(item.get("first"))]
            rank = next((c.get("val") for c in cells if c.get("type") == "t"), None)
            who = next((c for c in cells if c.get("link")), {})
            rows.append(
                {
                    "group": group.get("code"),
                    "rank": rank,
                    "player_id": _player_id(who.get("link")),
                    "player_name": who.get("val"),
                    "image": who.get("image"),
                    **_obj(item.get("cols")),
                }
            )
    df = _generic(rows, columns)
    df = _stat_cast(df, [c for c in df.columns if c not in ("group", "player_name", "image")])
    return _out(_conform(df, columns), return_as_pandas)


def parse_khl_player_profile(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/players/profile/`` -> one row: the player (``data.DATA``), nested objects flattened.

    ``player_id`` is parsed from ``pagelink``. Height, weight and age keep their Russian unit text.

    Args:
        raw: the ``/players/profile/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        One row (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_player_profile
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_player_profile
            raw = khl_player_profile(player_id=41928, return_parsed=False)
            df = parse_khl_player_profile(raw)

        As pandas::

            df_pd = parse_khl_player_profile(raw, return_as_pandas=True)

        Next step::

            df.select("player_id", "summary_stats_gp", "summary_stats_pts")
    """
    columns = _columns(columns, "khl_player_profile")
    person = _obj(_data(raw).get("DATA"))
    if not person:
        return _out(_empty(columns), return_as_pandas)
    df = _generic([{"player_id": _player_id(person.get("pagelink")), **person}], columns)
    df = _stat_cast(df, [c for c in df.columns if c.startswith("summary_stats_")])
    return _out(_conform(df, columns), return_as_pandas)


def parse_khl_player_career(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/players/stats/`` -> one row per season tournament the player played in.

    ``data.tournament_summary[]`` is one entry per season (``season_label`` such as ``26/27``), each with
    its tournaments (``tournamnets``, sic). ``tournament_id`` is the tnId, ``team_season_id`` the
    per-tournament team id (``tid``) and ``club_id`` the stable club id (``team.gid``).

    Args:
        raw: the ``/players/stats/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        The career rows (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_player_career
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_player_career
            raw = khl_player_career(player_id=41928, return_parsed=False)
            df = parse_khl_player_career(raw)

        As pandas::

            df_pd = parse_khl_player_career(raw, return_as_pandas=True)

        Next step::

            df.select("season_label", "tournament_id", "gp", "pts")
    """
    columns = _columns(columns, "khl_player_career")
    data = _data(raw)
    rows = []
    for season in _list(data.get("tournament_summary")):
        season = _obj(season)
        for tn in _list(season.get("tournamnets")):
            tn = _obj(tn)
            team = _obj(tn.get("team"))
            rows.append(
                {
                    "player_id": data.get("id"),
                    "season_label": season.get("name"),
                    "tournament_id": tn.get("key"),
                    "team_season_id": tn.get("tid"),
                    "club_id": team.get("gid"),
                    "team_name": team.get("name"),
                    **_obj(tn.get("stats")),
                }
            )
    df = _generic(rows, columns)
    df = _cast_ids_int64(
        df, [c for c in ("player_id", "tournament_id", "team_season_id", "club_id") if c in df.columns]
    )
    keep = ("player_id", "season_label", "tournament_id", "team_season_id", "club_id", "team_name")
    return _out(_conform(_stat_cast(df, [c for c in df.columns if c not in keep]), columns), return_as_pandas)


def parse_khl_roster(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/clubs/team/`` -> one row per player on the club's current roster.

    ``group`` is the roster section (``goalkeepers``, ``defenders``, ``forwards``, ``injured``; an
    injured player is listed there AND under his position). ``id`` lands as ``player_id``.

    Args:
        raw: the ``/clubs/team/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        The roster (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_roster
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_roster
            raw = khl_roster(club_id=7, return_parsed=False)
            df = parse_khl_roster(raw)

        As pandas::

            df_pd = parse_khl_roster(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("group") == "goalkeepers")
    """
    columns = _columns(columns, "khl_roster")
    rows = [
        {"group": group, "player_id": _obj(p).get("id"), **{k: v for k, v in _obj(p).items() if k != "id"}}
        for group, players in _obj(_data(raw).get("players")).items()
        for p in _list(players)
    ]
    return _out(_conform(_generic(rows, columns), columns), return_as_pandas)


def parse_khl_stat_filters(raw: Any, *, return_as_pandas: bool = False, columns: Columns = None) -> Frame:
    """``/filters/statplayers/`` -> one row per option of each stats filter.

    ``filter`` is the filter code (``season``, ``tournament``, ``amplua_full``, ``clubs_full``) and
    ``value`` the option's code (``i``: a tournament id, a season code such as ``s19``, a club slug);
    a season option's ``def`` is its default tournament id.

    Args:
        raw: the ``/filters/statplayers/`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: the documented column names, for the zero-row frame of an empty body.

    Returns:
        The filter options (zero rows when unusable).

    Example:
        Parse a raw body::

            import polars as pl
            from sportsdataverse.hockey.khl import khl_stat_filters
            from sportsdataverse.hockey.khl.khl_rest_parsers import parse_khl_stat_filters
            raw = khl_stat_filters(season=2026, return_parsed=False)
            df = parse_khl_stat_filters(raw)

        As pandas::

            df_pd = parse_khl_stat_filters(raw, return_as_pandas=True)

        Next step::

            df.filter(pl.col("filter") == "season")
    """
    columns = _columns(columns, "khl_stat_filters")
    rows = [
        {
            "filter": code,
            "filter_name": _obj(f).get("NAME"),
            "value": _obj(o).get("i"),
            **{k: v for k, v in _obj(o).items() if k != "i"},
        }
        for code, f in _obj(_data(raw).get("LIST")).items()
        for o in _list(_obj(f).get("VALUE"))
    ]
    df = _generic(rows, columns)
    if "value" in df.columns:
        df = df.with_columns(pl.col("value").cast(pl.Utf8))
    df = _cast_ids_int64(df, [c for c in ("def",) if c in df.columns])
    return _out(_conform(df, columns), return_as_pandas)
