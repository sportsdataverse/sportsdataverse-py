"""Parsers for the generated ``legabasket`` wrappers (Lega Basket Serie A, ``www.legabasket.it/api``).

Every parser follows the package contract: a polars frame by default, pandas with ``return_as_pandas=True``,
snake_cased columns (``dl_utils.underscore``), and a zero-row frame (never an exception) for an empty or malformed
body. The zero-row frame carries the parser's documented returns schema (``lba_dtypes.DTYPES``, generated with the
schemas), so two empty frames, or an empty one and a populated one, stack.

**Row rule.** Each route has its own shaper (a game body feeds the header, the box score, the team box score, the
play-by-play and the shots as separate frames); inside a shaper, nested objects flatten into ``<parent>_<child>``
columns and lists or maps become JSON strings. A wire ``id`` is renamed for what it identifies (``championship_id``,
``game_id``, ``team_id``, ``player_id``, ``coach_id``, ``standing_id``).

**Season.** The sdv season is the END year (2026-27 = 2027) and every season-scoped frame carries it as ``season``.
The site speaks START years (``year``, ``player_season``, ``championship_year``, ``s``): those columns are kept as
sent (a START year) and ``season`` is derived as ``start + 1`` from the row, else from the request the runtime
stamps under ``_request`` (see ``lba_runtime``). The standings' own wire ``season`` (a START year) is renamed
``season_start_year`` so it cannot be mistaken for the sdv key.

**Id key spaces.** Every id column is ``Int64``:

* ``championship_id``: one season AND one phase (Regular Season ``RS`` and Play Off ``PO`` differ; 602 =
  2026-27 Regular Season). Calendar and standings take it; ``round`` is ``event_serial``, not ``championships_day_id``.
* ``game_id`` (``match_id``, ``championships_match_id``): global across seasons. ``websocket_match_id`` is the
  live-feed id, a different number, sent as a string and cast to ``Int64`` here.
* ``player_id``: global, one per career (it joins box scores, play-by-play, rosters and game logs).
* ``team_id``: ONE TEAM-SEASON (re-issued every season; Milano is 1649 in 2024-25 and 1751 in 2026-27). To follow a
  club across seasons join ``team_id`` to ``lba_teams`` and use its ``club_id``; game, standings and play-by-play rows
  carry only the per-season ``team_id``. Whether ``club_id`` / ``club_code`` is stable across all history is
  unverified.
* ``coach_id``: global. ``championships_day_id``: one round of one championship. ``action_id`` /
  ``linked_action_id``: an action within one game.

**Numbers as strings.** The leaderboards' ``minutes`` and ``score``, the player ``*_avg`` averages and the standings'
``*_percent`` (``"100%"``) arrive as strings; they are parsed to ``Float64`` (a percent is 0-100). ``game_status``,
``quarter``, ``sf``, ``player_number`` and the qualifier and zone codes stay strings, as sent.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _cast_ids_int64
from sportsdataverse.basketball.lba.lba_dtypes import DTYPES
from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_lba_box_score",
    "parse_lba_calendar",
    "parse_lba_championships",
    "parse_lba_game",
    "parse_lba_game_log",
    "parse_lba_pbp",
    "parse_lba_pbp_legend",
    "parse_lba_player",
    "parse_lba_player_stats",
    "parse_lba_players",
    "parse_lba_players_leaderboard",
    "parse_lba_roster",
    "parse_lba_roster_coach",
    "parse_lba_shots",
    "parse_lba_standings",
    "parse_lba_team_box_score",
    "parse_lba_team_players_stats",
    "parse_lba_teams",
    "parse_lba_teams_leaderboard",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``lba_runtime.REQUEST_KEY``).
_REQUEST = "_request"
_ID_COL = re.compile(r"(^|_)id$")
_DTYPE_CODES = {"i": pl.Int64, "f": pl.Float64, "b": pl.Boolean, "s": pl.String}
#: Wire fields that hold a season START year, in the order they are tried for the row's ``season``.
_START_YEAR = ("year", "championship_year", "player_season")
#: A score text such as ``"12 - 9"`` (home - visitor).
_SCORE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")
#: Numeric strings, per parser family (see the module docstring).
_NUM_LEADERBOARD = re.compile(r"^(minutes|score)$")
_NUM_PLAYER = re.compile(r"_avg$")
_NUM_STANDINGS = re.compile(r"_percent$")
_SIDES = (("ht", "home", "h_team_id"), ("vt", "visitor", "v_team_id"))
#: Half-court image pixels in 2017-18 and 2018-19; full-court percent from 2019-20 on (no actions before 2017-18).
_PIXEL_STARTS = (2017, 2018)


# --- helpers -----------------------------------------------------------------------------------------------------


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _int(value: Any) -> Optional[int]:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _request_season(raw: Any) -> Optional[int]:
    return _int(_obj(_obj(raw).get(_REQUEST)).get("season"))


def _season(row: Mapping[str, Any], fallback: Optional[int]) -> Optional[int]:
    """The END-year season of a row: its own START year + 1, else ``fallback``."""
    for key in _START_YEAR:
        start = _int(row.get(key))
        if start is not None:
            return start + 1
    return fallback


def _flat(obj: Mapping[str, Any], prefix: str = "") -> Dict[str, Any]:
    """One flat row: nested objects as ``<parent>_<child>``, lists and maps as JSON strings."""
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        if key == _REQUEST:
            continue
        name = prefix + underscore(str(key))
        if isinstance(value, list):
            out[name] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, dict):
            out.update(_flat(value, name + "_"))
        else:
            out[name] = value
    return out


def _named(
    obj: Mapping[str, Any], id_name: Optional[str] = None, rename: Optional[Mapping[str, str]] = None
) -> Dict[str, Any]:
    """Flatten ``obj``; a wire ``id`` becomes ``id_name`` and ``rename`` maps other wire names."""
    row = _flat(obj)
    for old, new in {**({"id": id_name} if id_name else {}), **(rename or {})}.items():
        if old in row:
            row = {(new if k == old else k): v for k, v in row.items()}
    return row


def _dtype_for(col: str, schema: Optional[Mapping[str, str]] = None) -> Any:
    """A column's dtype without a value to go by: the returns schema, else Int64 for an id, else String."""
    code = (schema or {}).get(col)
    if code:
        return _DTYPE_CODES[code]
    return pl.Int64 if _ID_COL.search(col) else pl.String


def _number(expr: pl.Expr) -> pl.Expr:
    """A numeric string (``"28.66"``, ``"100%"``) -> Float64; unparseable -> null."""
    return expr.str.strip_chars().str.replace(r"%$", "").cast(pl.Float64, strict=False)


def _typed(df: pl.DataFrame, num_str: Optional["re.Pattern[str]"] = None) -> pl.DataFrame:
    """Ids -> Int64 (lossless only), numeric-string columns -> Float64, all-null columns -> a concrete dtype."""
    df = _cast_ids_int64(df, [c for c in df.columns if _ID_COL.search(c)])
    casts = []
    for col, dtype in df.schema.items():
        if dtype == pl.Null:
            casts.append(pl.col(col).cast(_dtype_for(col)))
        elif dtype == pl.String and num_str is not None and num_str.search(col):
            casts.append(_number(pl.col(col)).alias(col))
    return df.with_columns(casts) if casts else df


def _frame(rows: Iterable[Mapping[str, Any]], num_str: Optional["re.Pattern[str]"] = None) -> pl.DataFrame:
    rows = [r for r in rows if r]
    if not rows:
        return pl.DataFrame()
    try:
        df = pl.from_dicts(rows, infer_schema_length=None, strict=False)
    except Exception:  # noqa: BLE001 -- a malformed body parses to nothing, never raises
        return pl.DataFrame()
    return _typed(df, num_str)


def _finish(df: pl.DataFrame, columns: Optional[Sequence[str]], return_as_pandas: bool, parser: str) -> Frame:
    """Apply the returns-schema dtypes and the documented columns.

    An all-null column (and a whole-number rate) takes the schema dtype; an empty frame keeps the documented
    columns and a missing column is added as nulls, all typed like a populated frame.
    """
    schema = DTYPES.get(parser, {})
    if schema and df.width:
        casts = []
        for col, dtype in df.schema.items():
            want = _dtype_for(col, schema) if col in schema else dtype
            if dtype != want and (df[col].null_count() == df.height or (want == pl.Float64 and dtype.is_numeric())):
                casts.append(pl.col(col).cast(want))
        df = df.with_columns(casts) if casts else df
    columns = list(columns) if columns else list(schema)
    if columns:
        if df.width == 0:
            df = pl.DataFrame(schema={c: _dtype_for(c, schema) for c in columns})
        else:
            missing = [pl.lit(None, dtype=_dtype_for(c, schema)).alias(c) for c in columns if c not in df.columns]
            if missing:
                df = df.with_columns(missing)
            # the documented order first, so a populated frame stacks onto an empty one
            df = df.select([*columns, *[c for c in df.columns if c not in columns]])
    return df.to_pandas() if return_as_pandas else df


def _by_id(value: Any) -> Dict[str, Any]:
    """An id-keyed object (``referees``, ``broadcasters``) as ``{str(id): value}``."""
    return {str(k): v for k, v in value.items()} if isinstance(value, dict) else {}


def _json(value: Any) -> Optional[str]:
    return json.dumps(value, ensure_ascii=False) if value is not None else None


# --- competition, calendar, game header ---------------------------------------------------------------------------


def parse_lba_championships(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``get-championships`` -> one row per championship (one per phase of a season).

    Args:
        raw: the ``get-championships`` body (``competitions`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per championship: ``championship_id`` (one season AND phase), ``season`` (END year), ``year`` (the
        START year as sent) and the phase (``ctype_code`` ``RS`` / ``PO``). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_championships
            parse_lba_championships({"competitions": [{"id": 602, "year": 2026, "ctype_code": "RS"}]})
    """
    rows = []
    for c in _list(_obj(raw).get("competitions")):
        row = _named(c, "championship_id")
        rows.append({"season": _season(c, _request_season(raw)), **row})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_championships")


def parse_lba_calendar(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-championships-calendar-by-id`` -> one row per game of one round.

    Args:
        raw: the calendar body (``matches`` list, with the ``referees`` and ``broadcasters`` maps keyed by game id).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per game: ``game_id`` (global), ``championship_id``, ``season``, home and visitor team ids and
        names, scores, venue, and ``referees`` / ``broadcasters`` as JSON strings. ``h_club_code`` / ``v_club_code``
        are null before 2018-19. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_calendar
            parse_lba_calendar({"matches": [{"id": 25602, "championship_id": 602, "year": 2026}]})
    """
    body = _obj(raw)
    referees, broadcasters = _by_id(body.get("referees")), _by_id(body.get("broadcasters"))
    rows = []
    for m in _list(body.get("matches")):
        row = _named(m, "game_id")
        key = str(m.get("id"))
        row["referees"] = _json(referees.get(key))
        row["broadcasters"] = _json(broadcasters.get(key))
        rows.append({"season": _season(m, _request_season(raw)), **row})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_calendar")


def parse_lba_game(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-championships-matches-by-id`` -> the game header, one row.

    Args:
        raw: the game body (``match`` header with the ``referees`` and ``broadcasters`` lists).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``game_id``, ``championship_id``, ``season``, teams, venue, quarter scores (``q1_hs`` ... ``ot_vs``),
        coaches, ``game_status`` (``"0"`` scheduled, ``"1"`` live, ``"2"`` final) and ``referees`` /
        ``broadcasters`` as JSON strings. In 1995-96 the period scores are not recorded (a 0 there is missing).
        Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_game
            parse_lba_game({"match": {"id": 25602, "year": 2026, "game_status": "2"}})
    """
    body = _obj(raw)
    match = _obj(body.get("match"))
    if not match:
        return _finish(pl.DataFrame(), columns, return_as_pandas, "parse_lba_game")
    row = _named(match, "game_id")
    row["referees"] = _json(body.get("referees"))
    row["broadcasters"] = _json(body.get("broadcasters"))
    rows = [{"season": _season(match, _request_season(raw)), **row}]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_game")


# --- box score ----------------------------------------------------------------------------------------------------


def _side_rows(raw: Any) -> Iterable[Tuple[str, str, Optional[int], Mapping[str, Any], Dict[str, Any]]]:
    """``(wire key, side, team_id, side block, context)`` for the home and visitor blocks of a game body."""
    body = _obj(raw)
    match = _obj(body.get("match"))
    ctx = {"game_id": _int(match.get("id")), "season": _season(match, _request_season(raw))}
    for wire, side, team_key in _SIDES:
        yield wire, side, _int(match.get(team_key)), _obj(_obj(body.get("scores")).get(wire)), ctx


def parse_lba_box_score(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The box score of a game -> one row per player, home team first.

    Args:
        raw: a game body with ``scores`` (``get-championships-matches-scores-by-id`` or the box score that
            ``get-championships-matches-by-id`` carries).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per player: ``game_id``, ``season``, ``side`` (``home`` / ``visitor``), ``team_id`` (a team-season id),
        ``player_id`` and the Italian-named stat columns (``pun`` points, ``t2_r`` / ``t2_t`` two-pointers made /
        attempted, ``t3_*``, ``tl_*`` free throws, ``rimbalzi_*`` rebounds, ``ass`` assists, ``val_lega`` the
        league rating). ``sf`` (starter flag) is a string as sent. ``plus_minus`` is empty in some eras. Zero rows
        for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_box_score
            parse_lba_box_score({"match": {"id": 25602}, "scores": {"ht": {"rows": [{"player_id": 969, "pun": 15}]}}})
    """
    rows = []
    for _wire, side, team_id, block, ctx in _side_rows(raw):
        for p in _list(block.get("rows")):
            rows.append({**ctx, "side": side, "team_id": team_id, **_flat(p)})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_box_score")


def parse_lba_team_box_score(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The team lines of a game's box score -> two rows per team (``team`` and ``totals``).

    Args:
        raw: a game body with ``scores`` (as for :func:`parse_lba_box_score`).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        Per side, a ``row_type`` ``team`` row (the stats the league books to the team itself, not to a player: team
        rebounds, team turnovers) and a ``totals`` row (the whole team, players plus team). ``game_id``, ``season``,
        ``side`` and ``team_id`` as in :func:`parse_lba_box_score`. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_team_box_score
            parse_lba_team_box_score({"match": {"id": 25602}, "scores": {"ht": {"totals": {"pun": 109}}}})
    """
    rows = []
    for _wire, side, team_id, block, ctx in _side_rows(raw):
        for kind in ("team", "totals"):
            line = _obj(block.get(kind))
            if line:
                rows.append({**ctx, "side": side, "team_id": team_id, "row_type": kind, **_flat(line)})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_team_box_score")


# --- play-by-play, shots ------------------------------------------------------------------------------------------


def parse_lba_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-championships-matches-play-by-play`` -> one row per action, in the order requested.

    Args:
        raw: the play-by-play body (``pbp.actions`` with the ``match`` header).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per action. Added columns: ``game_id``, ``season``, ``home_team_id`` / ``visitor_team_id``,
        ``team_side`` (``home`` / ``visitor`` / null for a clock action), ``home_score`` / ``visitor_score`` (parsed
        from ``score``, 2021-22 on) and ``coord_frame``: ``half_court_px`` (2017-18 and 2018-19: image pixels, an
        unlocated action at 0, 0) or ``court_pct`` (2019-20 on: 0-100 along the 28 m court). ``description`` is the
        action in Italian (``legend`` codes translate the qualifiers: :func:`parse_lba_pbp_legend`). ``side`` changes
        meaning by era; see ``ENDPOINTS.md`` in sdv-internal-refs. Play-by-play exists from 2017-18 only. Zero rows
        for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_pbp
            parse_lba_pbp({"match": {"id": 25602, "year": 2026}, "pbp": {"ht_id": 1764, "vt_id": 1761, "actions": []}})
    """
    body = _obj(raw)
    match, pbp = _obj(body.get("match")), _obj(body.get("pbp"))
    season = _season(match, _request_season(raw))
    home, visitor = _int(pbp.get("ht_id")), _int(pbp.get("vt_id"))
    frame = None if season is None else ("half_court_px" if season - 1 in _PIXEL_STARTS else "court_pct")
    rows = []
    for a in _list(pbp.get("actions")):
        row = _flat(a)
        team = _int(a.get("team_id"))
        score = _SCORE.match(a["score"]) if isinstance(a.get("score"), str) else None
        rows.append(
            {
                "game_id": _int(match.get("id")),
                "season": season,
                "home_team_id": home,
                "visitor_team_id": visitor,
                "team_side": None
                if team is None
                else ("home" if team == home else "visitor" if team == visitor else None),
                "home_score": int(score.group(1)) if score else None,
                "visitor_score": int(score.group(2)) if score else None,
                "coord_frame": frame,
                **row,
            }
        )
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_pbp")


def parse_lba_pbp_legend(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """The play-by-play ``legend`` -> one row per qualifier code (Italian label).

    Args:
        raw: the play-by-play body (``legend`` map of code to label).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per code: ``code`` (the value of ``action_1_qualifier_code`` / ``action_2_qualifier_code``, a string
        on the actions and an ``Int64`` here) and ``label`` (Italian, as sent; sdv-internal-refs ``legabasket-returns.md``
        translates all 59). The same table in every game sampled. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_pbp_legend
            parse_lba_pbp_legend({"legend": {"1": "Tiro in sospensione", "9": "Schiacciata"}})
    """
    legend = _obj(_obj(raw).get("legend"))
    rows = [{"code": int(k), "label": v} for k, v in legend.items() if str(k).isdigit() and isinstance(v, str)]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_pbp_legend")


def parse_lba_shots(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-championship-matches-shots-by-id`` -> shooting by court zone, one row per team and zone.

    Args:
        raw: the shots body (``shots.ht`` / ``shots.vt`` maps of zone to made / attempted).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per ``side`` and ``zone`` (``1``-``10``; zone codes are the play-by-play ``side_area_code``). ``made``
        and ``attempts`` are parsed from ``num`` (``"17/21"``); ``perc`` is the percentage (0-100). Aggregates only,
        with no x/y. **Zone 7 also holds the free throws** (mid-range wings and elbows plus the line), and 2017-18
        sends only zone 7 for every field goal and free throw. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_shots
            parse_lba_shots({"match": {"id": 25602}, "shots": {"ht": {"3": {"perc": 80.9, "num": "17/21"}}}})
    """
    body = _obj(raw)
    shots = _obj(body.get("shots"))
    match = _obj(body.get("match"))
    ctx = {"game_id": _int(match.get("id")), "season": _season(match, _request_season(raw))}
    rows = []
    for wire, side, team_key in _SIDES:
        for zone, cell in _obj(shots.get(wire)).items():
            if not str(zone).isdigit() or not isinstance(cell, dict):
                continue
            made = attempts = None
            hit = re.match(r"^\s*(\d+)\s*/\s*(\d+)\s*$", str(cell.get("num", "")))
            if hit:
                made, attempts = int(hit.group(1)), int(hit.group(2))
            rows.append(
                {
                    **ctx,
                    "side": side,
                    "team_id": _int(match.get(team_key)),
                    "zone": int(zone),
                    "made": made,
                    "attempts": attempts,
                    **_flat(cell),
                }
            )
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_shots")


# --- standings, teams, rosters --------------------------------------------------------------------------------------


def parse_lba_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-championship-ranking`` -> the standings after a round, one row per team.

    Args:
        raw: the ranking body (``standings`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per team: ``team_id`` (a team-season id), ``position``, ``wins``, ``defeats``, points made and
        suffered, home and away splits, ``wins_percent`` (0-100; sent as ``"100%"``), ``movement`` and the round
        (``day_code``, ``day_name``, ``championships_day_id``). ``season`` is the END year; the wire ``season`` is
        ``season_start_year``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_standings
            parse_lba_standings({"standings": [{"id": 44970, "team_id": 1772, "position": 1, "year": 2026}]})
    """
    rows = []
    for s in _list(_obj(raw).get("standings")):
        row = _named(s, "standing_id", {"season": "season_start_year"})
        rows.append({"season": _season(s, _request_season(raw)), **row})
    return _finish(_frame(rows, _NUM_STANDINGS), columns, return_as_pandas, "parse_lba_standings")


def parse_lba_teams(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-teams`` -> the teams of a season, one row each.

    Args:
        raw: the teams body (``teams`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per team-season: ``team_id`` (re-issued every season), ``club_id`` / ``club_code`` (the key that
        follows a club across seasons; stability unverified), ``name``, ``season`` and ``logo_key``. Zero rows for an
        empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_teams
            parse_lba_teams({"teams": [{"id": 1765, "name": "Acqua S.Bernardo Cantu", "year": 2026, "club_id": 12}]})
    """
    rows = []
    for t in _list(_obj(raw).get("teams")):
        rows.append({"season": _season(t, _request_season(raw)), **_named(t, "team_id")})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_teams")


def _team_ctx(raw: Any) -> Dict[str, Any]:
    team = _obj(_obj(raw).get("team"))
    return {
        "team_id": _int(team.get("id")),
        "club_id": _int(team.get("club_id")),
        "club_code": team.get("club_code"),
        "season": _season(team, _request_season(raw)),
    }


def parse_lba_roster(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-team-roster`` players -> one row per player of a team-season.

    Args:
        raw: the roster body (``team`` header and ``players`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per player: the team context (``team_id``, ``club_id``, ``club_code``, ``season``), ``player_id``
        (global), bio (``birth_date``, ``height``, ``weight``, ``country``), ``player_number`` (a string) and role.
        Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_roster
            parse_lba_roster({"team": {"id": 1764, "year": 2026}, "players": [{"id": 969, "name": "Alessandro"}]})
    """
    ctx = _team_ctx(raw)
    rows = [{**ctx, **_named(p, "player_id")} for p in _list(_obj(raw).get("players"))]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_roster")


def parse_lba_roster_coach(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``get-team-roster`` head coach -> one row.

    Args:
        raw: the roster body (``team`` header and ``coach`` object).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: the team context (``team_id``, ``club_id``, ``club_code``, ``season``), ``coach_id`` (global; joins
        the game header's ``home_coach_id`` / ``visitor_coach_id``), name, birth date and place, and the role.
        Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_roster_coach
            parse_lba_roster_coach({"team": {"id": 1764, "year": 2026}, "coach": {"id": 339, "surname": "Nicola"}})
    """
    coach = _obj(_obj(raw).get("coach"))
    rows = [{**_team_ctx(raw), **_named(coach, "coach_id")}] if coach else []
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_roster_coach")


def parse_lba_team_players_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``get-team-players-stats`` -> season totals of each player of a team, one row each.

    Args:
        raw: the body (``team`` header and ``players`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per player: the team context (``team_id``, ``club_id``, ``club_code``, ``season``), ``player_id`` and
        the ``*_sum`` columns (the default ``st=sum``). ``*_perc_sum`` are rates (0-100), not sums;
        ``rating_oer_sum`` is a sum of per-game OERs. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_team_players_stats
            parse_lba_team_players_stats({"team": {"id": 1764, "year": 2026}, "players": [{"player_id": 6191, "points_sum": 65}]})
    """
    ctx = _team_ctx(raw)
    rows = [{**ctx, **_flat(p)} for p in _list(_obj(raw).get("players"))]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_team_players_stats")


# --- players ------------------------------------------------------------------------------------------------------


def parse_lba_players(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-players`` -> one page of the players of a season.

    Args:
        raw: the players body (``players`` list and ``pagination``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per player on the page: ``player_id`` (global), ``code``, name, ``team_name`` and ``season``. The
        route is paged: ``lba_players(page=...)`` walks the pages. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_players
            parse_lba_players({"players": [{"id": 45, "surname": "Akele", "year": 2026}]})
    """
    rows = [
        {"season": _season(p, _request_season(raw)), **_named(p, "player_id")} for p in _list(_obj(raw).get("players"))
    ]
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_players")


def parse_lba_player(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-player-by-id`` -> the player bio with current-season totals, highs and averages, one row.

    Args:
        raw: the body (``player`` and, with ``stats=1``, ``stats``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: the bio (``player_id`` global, name, ``birth_date``, ``height``, ``weight``, ``team_id`` of the
        current team-season, ``year`` the START year of the latest season) and ``stats_*`` columns
        (``stats_totals_highs_rs_*`` totals ``*_sum`` and highs ``*_max``, ``stats_avg_rs_*`` per-game ``*_avg``
        parsed to ``Float64``). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_player
            parse_lba_player({"player": {"id": 969, "surname": "Cappelletti"}, "stats": {"played_matches": 3}})
    """
    body = _obj(raw)
    player = _obj(body.get("player"))
    if not player:
        return _finish(pl.DataFrame(), columns, return_as_pandas, "parse_lba_player")
    row = _named(player, "player_id")
    row.update(_flat(_obj(body.get("stats")), "stats_"))
    return _finish(_frame([row], _NUM_PLAYER), columns, return_as_pandas, "parse_lba_player")


def parse_lba_player_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``get-player-stats`` -> a player's career by championship, one row per championship and statistic.

    Args:
        raw: the body (``player`` and ``stats.data`` holding ``avg``, ``max`` and ``sum`` lists).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per championship and ``stat_type`` (``avg`` per game, ``max`` single-game highs, ``sum`` totals):
        ``player_id``, ``stat_type``, ``season`` (END year of ``championship_year``), the championship name
        (Regular Season, Play Off, Coppa Italia), ``team_name`` and the stat columns. Zero rows for an empty or
        malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_player_stats
            parse_lba_player_stats({"player": {"id": 969}, "stats": {"data": {"sum": [{"championship_year": 2013, "points": 4}]}}})
    """
    body = _obj(raw)
    pid = _int(_obj(body.get("player")).get("id"))
    data = _obj(_obj(body.get("stats")).get("data"))
    rows = []
    for stat_type in ("avg", "max", "sum"):
        for r in _list(data.get(stat_type)):
            rows.append({"player_id": pid, "stat_type": stat_type, "season": _season(r, None), **_flat(r)})
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_player_stats")


def parse_lba_game_log(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """``get-player-matches`` -> a player's game log for one season, one row per game.

    Args:
        raw: the body (``player`` and ``matches_stats.data``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per game: ``player_id``, ``championships_match_id`` (the game id), ``team_id``, ``season`` (END
        year, from the request), minutes, the stat columns and the game context (scores, teams, ``match_date``). The
        site's ``avg_points`` and ``match_adp`` are the player's PLUS-MINUS, not scoring averages. Zero rows for an
        empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_game_log
            parse_lba_game_log({"player": {"id": 969}, "matches_stats": {"data": [{"player_id": 969, "points": 15}]}})
    """
    body = _obj(raw)
    season = _request_season(raw)
    rows = []
    for g in _list(_obj(body.get("matches_stats")).get("data")):
        code = g.get("player_team_code")
        start = (
            int(str(code).split("|", 1)[0]) if isinstance(code, str) and str(code).split("|", 1)[0].isdigit() else None
        )
        rows.append(
            {
                "season": season if season is not None else (start + 1 if start is not None else None),
                **_named(g, "stat_line_id"),
            }
        )
    return _finish(_frame(rows), columns, return_as_pandas, "parse_lba_game_log")


# --- leaderboards -------------------------------------------------------------------------------------------------


def parse_lba_players_leaderboard(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``get-players-statistics`` -> a season leaderboard for one category, one row per player.

    Args:
        raw: the body (``stats`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per ranked player: ``category`` (the requested ``cat``), ``player_id``, ``team_id`` (a team-season
        id), ``season``, ``category_filter_total`` (the category's total), ``presences`` and ``minutes`` / ``score``
        (per-game averages; strings on the wire, ``Float64`` here). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_players_leaderboard
            parse_lba_players_leaderboard({"stats": [{"category": "points", "player_id": 6191, "year": 2026, "score": "21.67"}]})
    """
    rows = [{"season": _season(s, _request_season(raw)), **_flat(s)} for s in _list(_obj(raw).get("stats"))]
    return _finish(_frame(rows, _NUM_LEADERBOARD), columns, return_as_pandas, "parse_lba_players_leaderboard")


def parse_lba_teams_leaderboard(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``get-teams-statistics-by-type`` -> team leaderboards, one row per team and category.

    Args:
        raw: the body (``stats`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per team and ``category``: ``team_id`` (a team-season id), ``name``, ``season``,
        ``total_played_matches``, ``total_points`` and the per-game ``score`` and per-40-minute ``score_40_min``.
        The offensive (``teams_offensive``) and defensive (``teams_defensive``) boards are separate requests. Zero rows
        for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.lba import parse_lba_teams_leaderboard
            parse_lba_teams_leaderboard({"stats": [{"category": "points", "team_id": 1761, "year": 2026, "score": 99}]})
    """
    rows = [{"season": _season(s, _request_season(raw)), **_flat(s)} for s in _list(_obj(raw).get("stats"))]
    return _finish(_frame(rows, _NUM_LEADERBOARD), columns, return_as_pandas, "parse_lba_teams_leaderboard")
