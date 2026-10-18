"""Parsers for the generated ``sportality`` wrappers (``www.basketligaen.dk/api``, Basketligaen, Denmark).

Basketligaen's site runs on the Sportality platform, the vendor family behind ``shl`` and
``ha``; its stats provider is ``genius-basketball``, a relay of Genius Sports LiveStats. The
site answers keyless JSON in a handful of shapes. Every parser follows the package parser
contract: a polars frame by default, pandas via ``return_as_pandas=True``, snake_case columns,
list / object cells JSON-encoded, and a zero-row frame (never an exception) on an empty or
malformed payload -- with the documented columns when the generated wrapper passes
``columns=``.

Shapes and their parsers:

* **Schedules** -- ``game-schedule`` ``{gameInfo: [...], teamList, ssgtUuid}``, the match-day
  routes ``{date, games: [...]}`` and a team's ``{playedGames|upcomingGames: [...]}``:
  :func:`parse_sportality_schedule`, one row per game.
* **Play-by-play** -- a bare event list: :func:`parse_sportality_pbp`, one row per event (shots
  carry ``coordinates_x`` / ``coordinates_y``).
* **Box scores** -- ``boxscore`` / ``player-stats``: per-side rows in
  ``stats.{homeTeamValue,awayTeamValue}[]`` (players, then the bench and team-totals rows) and
  the ``players`` name maps keyed by Genius person id: :func:`parse_sportality_player_box`.
* **Team stats** -- ``{home, away}``, each with ``statistics[]`` per period (period 0 = the
  game): :func:`parse_sportality_team_stats`, one row per side and period.
* **Standings** -- the gameday widget ``{leagueStandings: [...], dataColumn, groupings}``:
  :func:`parse_sportality_standings`.
* **Stats tables** -- ``stats-info/{module}`` and the athlete career log, a one-item list
  ``[{dataColumns, stats: [...], players: {<genius id>: ...}, teams: {...}, ...}]``:
  :func:`parse_sportality_stats_info`, one row per ``stats[]`` entry.
  :func:`parse_sportality_player_crosswalk` reads the ``players{}`` map instead (Genius person
  id -> athlete uuid).
* **Directories** -- ``layout-info`` (every ssgt): :func:`parse_sportality_layout`;
  ``season-series-game-types-filter``: :func:`parse_sportality_filter`; a team roster grouped by
  position: :func:`parse_sportality_roster`; player head-to-head: :func:`parse_sportality_h2h`;
  one athlete's bio with the Genius person id: :func:`parse_sportality_athlete`.
* **Page objects** (a team, a game header or overview, an athlete profile):
  :func:`parse_sportality_page`, always one row.
* **Everything else** -- :func:`parse_sportality`, the generic row rule: a list -> one row per
  item; a dict holding exactly one list -> that list's items; any other non-empty dict -> one
  row; the ``ssgt/{ssgt}/provider`` route's bare word (``"genius-basketball"``) -> one
  ``provider`` row.

Id key spaces (one dtype each, cast here at the boundary; the wire mixes strings and integers):

* ``player_id`` and every ``*_player_id`` -- the **Genius person id** (e.g. 1887576): a string
  key in the ``players`` maps, an integer in play-by-play and box-score rows, ``''`` on team
  summary rows. ``Int64`` in every frame. It joins the ``genius`` family.
* ``game_id`` / ``game_source_id`` / ``*_ext_id`` -- the **Genius match id** (2868126;
  sports-v2 ``gameInfo.extId`` is the same number as a string; ``gameSourceId`` is a string on
  play-by-play lines and an integer elsewhere): ``Int64``.
* ``team_id`` / ``*_team_id`` -- the **Genius team id** (153747): a string in gameday bodies, an
  integer in standings and stats tables. ``Int64``. In stats-info *player* rows the wire calls
  it ``teamCode`` (and the team *uuid* ``teamId``); both are renamed here to the names above.
* ``event_id`` -- the Genius event number within a game: ``Int64``.
* ``*_uuid`` -- platform uuids (10 characters: game ``c4hh5nig6t``, team ``rfo82vr830``,
  athlete ``dpu0901c0t``, ssgt ``xs07bd3fmp``): ``Utf8``. Sportality mints its own uuids per
  instance, so none equals an id of ``shl`` or ``ha``. The athlete uuid is NOT the Genius
  person id.
* ``team_code`` -- the platform's 3-letter team code (``HOL``): ``Utf8``. Gameday bodies spell it
  ``Hol`` in places.

Stat cells of the stats tables, box scores and team stats are ``Int64`` when every value is a whole
number (counts) and ``Float64`` when any value is a decimal or a numeric string, and every ``*_perc``
rate and ``*pg`` per-game average (``ppg``, ``blkpg``, ``plus_minus_pg``) is ``Float64``: the API mixes ``"27.3"`` and ``0`` in one column, so a count-looking sample
could otherwise type a rate as an integer (the generated wrappers conform every frame to the
documented dtypes, which come from full bodies). A column holding any other string (``"09:07"``
minutes, ``"30-76"`` made-attempted) stays ``Utf8``.
Empty strings (the box score's blank cells on team rows) are read as null.

**Personal data.** Birth dates (of every person: the academy squads hold minors), e-mail
addresses, phone numbers, addresses and contact blocks never leave the parsers (``_scrub``);
the runtime applies it to every body before it is parsed, as the reference capture does. A
player's height, weight, nationality, jersey number and age in years are kept.

When the generated wrapper passes ``columns=``, the frame is conformed to that endpoint's
returns schema (:data:`~sportsdataverse.basketball.dbl.sportality_dtypes.SCHEMAS`): documented
columns first, in order and with the documented dtypes (a missing one is a null column),
anything the API added later after them. An empty or malformed body gives the same columns and
dtypes with zero rows, so a frame from an empty body concatenates with a populated one.
"""

from __future__ import annotations

import functools
import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball.dbl.sportality_dtypes import SCHEMAS
from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_sportality",
    "parse_sportality_athlete",
    "parse_sportality_filter",
    "parse_sportality_h2h",
    "parse_sportality_layout",
    "parse_sportality_page",
    "parse_sportality_pbp",
    "parse_sportality_player_box",
    "parse_sportality_player_crosswalk",
    "parse_sportality_roster",
    "parse_sportality_schedule",
    "parse_sportality_standings",
    "parse_sportality_stats_info",
    "parse_sportality_team_stats",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

# Genius person / match / team / event ids: Int64 everywhere (see the module docstring).
_INT_ID = re.compile(r"(^|_)(player_id|game_id|game_source_id|event_id|ext_id|team_id)$")
# Stat cells that are labels, ids or placeholders, not measurements: never forced to Float64.
_NOT_STATS = frozenset({"NR", "POS", "Strk", "Attendance", "pno", "teamNumber", "Rank"})
# Stats-table display slots that hold a constant 0 / null (the site fills them from the info record): dropped.
_PLACEHOLDERS = frozenset({"Rank", "Player", "Team", "Season", "Series", "GameType"})
# games_games ids duplicated by info.homeTeamId / info.awayTeamId (and clashing with the team-name cells).
_DUPLICATE_CELLS = frozenset({"HomeTeam", "GuestTeam"})
_BOX_SIDES = (("homeTeamValue", "home"), ("awayTeamValue", "away"))
_SCHEDULE_KEYS = ("gameInfo", "games", "playedGames", "upcomingGames")
_TEAM_PREFIX = {"homeTeamInfo": "homeTeam", "awayTeamInfo": "awayTeam", "venueInfo": "venue", "seriesInfo": "series"}
_BOX_LABELS = {"sports.stats.team_coach": "team_coach", "sports.stats.team_totals": "team_totals"}
# Keys whose value is personal data, dropped whatever the value type (mirrors the refs capture's PERSONAL_KEYS).
_PERSONAL = re.compile(r"(birth|^dob$|e_?mail|phone|mobile|^tel$|address|^contact)", re.I)
# sportality_dtypes dtype codes -> polars dtypes.
_SCHEMA_DTYPES: Dict[str, pl.DataType] = {
    "str": pl.Utf8(),
    "int": pl.Int64(),
    "float": pl.Float64(),
    "bool": pl.Boolean(),
    "list[int]": pl.List(pl.Int64),
}
# A wrapper's documented column list -> that endpoint's {column: dtype} (lists are unique per endpoint).
_BY_COLUMNS: Dict[tuple, Dict[str, pl.DataType]] = {
    tuple(cols): {c: _SCHEMA_DTYPES[code] for c, code in cols.items()} for cols in SCHEMAS.values()
}


def _scrub(obj: Any) -> Any:
    """``obj`` without personal data: birth dates, e-mail, phone, address and contact keys, at any depth.

    A key matching ``birth``, ``dob``, ``email``, ``phone``, ``mobile``, ``tel``, ``address`` or
    ``contact`` is removed wherever it sits, whatever its value; the rest of the body is
    returned as a copy. The runtime runs it on every body, and every parser runs it again, so a
    body fed straight to a parser is just as clean.

    Args:
        obj (Any): a decoded JSON body.

    Returns:
        Any: the same structure without the personal keys.

    Example:
        Drop a birth date::

            from sportsdataverse.basketball.dbl.sportality_parsers import _scrub
            _scrub({"firstName": "Luka", "birthDate": "2007-01-01"})   # {"firstName": "Luka"}
    """
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items() if not _PERSONAL.search(str(k))}
    if isinstance(obj, list):
        return [_scrub(v) for v in obj]
    return obj


def _column(key: str) -> str:
    """Wire key -> snake_case column (``FG%`` -> ``fg_perc``, ``+/-`` -> ``plus_minus``)."""
    key = key.replace("+/-", "PlusMinus").replace("%", "Perc")
    return re.sub(r"[^0-9a-z_]+", "_", underscore(key)).strip("_")


def _flatten(obj: Mapping[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Nested dicts -> one flat dict; keys joined by ``_`` (lists stay as values)."""
    out: Dict[str, Any] = {}
    for k, v in obj.items():
        key = f"{prefix}_{k}" if prefix else str(k)
        if isinstance(v, dict) and v:
            out.update(_flatten(v, key))
        else:
            out[key] = None if isinstance(v, dict) else v
    return out


def _dict(v: Any) -> Dict[str, Any]:
    """``v`` when it is a dict, else ``{}`` (keeps a malformed body from raising)."""
    return v if isinstance(v, dict) else {}


def _list(v: Any) -> List[Any]:
    """``v`` when it is a list, else ``[]`` (a wrong-typed value reads as absent)."""
    return v if isinstance(v, list) else []


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _as_float(v: Any) -> Optional[float]:
    """A number or numeric string as float; ``None`` when it is neither."""
    if _is_number(v):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return None
    return None


def _as_int(v: Any) -> Optional[int]:
    """An integer or an integer-looking string as ``int``; ``None`` otherwise (never ``"123.0"`` -> 123)."""
    if isinstance(v, int) and not isinstance(v, bool):
        return v
    if isinstance(v, str) and re.fullmatch(r"-?\d+", v.strip()):
        return int(v.strip())
    return None


def _text(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False, sort_keys=True)
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _blank(v: Any) -> Any:
    """``''`` -> ``None`` (the box score fills team-row cells with empty strings)."""
    return None if isinstance(v, str) and v == "" else v


def _series(name: str, values: List[Any], stat: bool) -> pl.Series:
    """One column with a dtype taken from the Python types of its values (deterministic)."""
    present = [v for v in values if v is not None]
    if stat and present and all(_as_float(v) is not None for v in present):
        # Whole numbers stay Int64 (counts); a rate, a decimal or a numeric string makes the column Float64.
        if all(isinstance(v, int) and not isinstance(v, bool) for v in present) and not name.endswith(("_perc", "pg")):
            return pl.Series(name, values, dtype=pl.Int64)
        return pl.Series(name, [_as_float(v) for v in values], dtype=pl.Float64)
    if present and all(isinstance(v, bool) for v in present):
        return pl.Series(name, values, dtype=pl.Boolean)
    if present and all(_is_number(v) for v in present):
        dtype = pl.Int64 if all(isinstance(v, int) for v in present) else pl.Float64
        return pl.Series(name, values, dtype=dtype)
    return pl.Series(name, [_text(v) for v in values], dtype=pl.Utf8)


def _frame(rows: Iterable[Mapping[str, Any]], stat_keys: Iterable[str] = ()) -> pl.DataFrame:
    """Flat wire-keyed rows -> polars frame: snake_case columns, typed cells, Int64 ids."""
    rows = [r for r in rows if isinstance(r, Mapping) and any(v is not None for v in r.values())]
    if not rows:
        return pl.DataFrame()
    stats = set(stat_keys)
    keys: Dict[str, None] = {}
    for r in rows:
        keys.update(dict.fromkeys(r))
    cols: Dict[str, pl.Series] = {}
    for k in keys:
        name = _column(k)
        if not name or name in cols:
            continue
        values = [r.get(k) for r in rows]
        if any(isinstance(v, list) and v and all(_is_number(x) for x in v) for v in values) and name.endswith("_ids"):
            cols[name] = pl.Series(name, values, dtype=pl.List(pl.Int64))
            continue
        cols[name] = _series(name, values, k in stats or name.endswith("_perc"))
    return _cast_ids(pl.DataFrame(list(cols.values())))


def _cast_ids(df: pl.DataFrame) -> pl.DataFrame:
    """Every Genius id column -> ``Int64`` (a non-numeric value becomes null)."""
    ids = [c for c in df.columns if _INT_ID.search(c) and df.schema[c] != pl.Int64]
    return df.with_columns(pl.col(c).cast(pl.Int64, strict=False) for c in ids) if ids else df


def _dtype(column: str) -> pl.DataType:
    if _INT_ID.search(column):
        return pl.Int64()
    if column.endswith("_ids"):
        return pl.List(pl.Int64)
    return pl.Utf8()


def _finish(df: pl.DataFrame, columns: Optional[Sequence[str]], return_as_pandas: bool) -> Frame:
    """Conform ``df`` to the documented ``columns`` (dtypes from the endpoint's returns schema)."""
    if columns:
        schema = _BY_COLUMNS.get(tuple(columns)) or {c: _dtype(c) for c in columns}
        if df.height == 0:
            df = pl.DataFrame(schema=schema)
        else:
            extra = [c for c in df.columns if c not in schema]
            missing = [pl.lit(None, dtype=dtype).alias(c) for c, dtype in schema.items() if c not in df.columns]
            # ponytail: a value the documented dtype cannot hold becomes null (strict=False).
            df = df.with_columns(missing).select(
                *(pl.col(c).cast(dtype, strict=False) for c, dtype in schema.items()), *extra
            )
    elif df.height == 0:
        df = pl.DataFrame()
    return df.to_pandas() if return_as_pandas else df


def _body(raw: Any) -> Any:
    """The one dict of a one-item-list body (stats-info, career log); ``raw`` otherwise."""
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        return raw[0]
    return raw


# --------------------------------------------------------------------------- generic


def parse_sportality(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse any Basketligaen body by the generic row rule (see the module docstring).

    Args:
        raw (Any): the decoded JSON body (or the bare text of the ``provider`` route).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): the documented column names; an empty or malformed
            body gives a zero-row frame with these columns.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per record; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse the provider route's bare word::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality
            parse_sportality("genius-basketball")
    """
    rows: List[Any] = []
    if isinstance(raw, str) and re.fullmatch(r"[a-z_-]{1,40}", raw.strip()):
        rows = [{"provider": raw.strip()}]
    elif isinstance(raw, list):
        rows = raw
    elif isinstance(raw, dict) and raw:
        lists = [v for v in raw.values() if isinstance(v, list)]
        rows = lists[0] if len(lists) == 1 else [raw]
    flat = [_flatten(_scrub(r)) for r in rows if isinstance(r, dict)]
    return _finish(_frame(flat), columns, return_as_pandas)


def parse_sportality_page(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse a page object (a team, a game header or overview, an athlete profile) into ONE row.

    Nested objects become prefixed columns (``home_team_names_code``); lists are JSON-encoded
    (a team page's ``contact`` list must not become the rows, which the generic rule would do).
    ``game_info_ext_id`` (sports-v2 game-info) is the Genius match id, ``Int64``.

    Args:
        raw (Any): the decoded JSON body (a dict).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row; zero rows when the body is not a non-empty dict.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse a team page::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_page
            parse_sportality_page({"uuid": "rfo82vr830", "names": {"code": "HOL"}})
    """
    rows = [_flatten(_scrub(raw))] if isinstance(raw, dict) and raw else []
    return _finish(_frame(rows), columns, return_as_pandas)


def parse_sportality_athlete(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse ``/sports-v2/athlete-details/{athlete_uuid}`` into ONE row with the Genius person id.

    ``athlete_uuid`` is the platform uuid; ``player_id`` is the Genius person id read from the
    ``playerExtIds`` entry of type ``genius-basketball`` (``Int64``, null when the player has
    none): the key every gameday route and stats table uses, and the join to ``genius``.
    Birth date and contact data are dropped.

    Args:
        raw (Any): the decoded JSON body (``{"athleteData": {...}}``).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row; zero rows when the body has no athlete.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one athlete::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_athlete
            parse_sportality_athlete({"athleteData": {"uuid": "dpu0901c0t", "playerExtIds": [
                {"extId": "1917467", "extIdType": {"code": "genius-basketball"}}]}})
    """
    data = _dict(_dict(raw).get("athleteData"))
    if not data:
        return _finish(pl.DataFrame(), columns, return_as_pandas)
    genius = next(
        (
            _as_int(e.get("extId"))
            for e in _list(data.get("playerExtIds"))
            if isinstance(e, dict) and _dict(e.get("extIdType")).get("code") == "genius-basketball"
        ),
        None,
    )
    rec = _scrub({k: v for k, v in data.items() if k not in ("playerExtIds", "uuid")})
    row = {"athleteUuid": data.get("uuid"), "playerId": genius, **_flatten(rec)}
    return _finish(_frame([row]), columns, return_as_pandas)


# --------------------------------------------------------------------------- dedicated shapers


def parse_sportality_schedule(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse a schedule body: ``game-schedule``, ``last|next-game-day``, ``played|upcoming-games``.

    One row per game. ``game_uuid`` is the platform game uuid every gameday route takes; the
    team blocks become ``home_team_*`` / ``away_team_*`` (``*_uuid`` = team uuid, ``*_code`` =
    team code, ``*_status`` = WIN / LOSE, ``*_score``), plus ``venue_*`` and ``series_*``.
    ``start_date_time`` is LOCAL Copenhagen time without a zone; ``raw_start_date_time`` is UTC.
    ``state`` is ``post-game`` for a finished game (the sports-v2 header spells it
    ``post_game``). The schedule carries no Genius match id: take it from the game header.

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per game; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one match day::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_schedule
            parse_sportality_schedule({"date": "2026-10-10", "games": [{"uuid": "xoqu2x6851"}]})
    """
    games: List[Any] = []
    if isinstance(raw, dict):
        games = next((raw[k] for k in _SCHEDULE_KEYS if isinstance(raw.get(k), list)), [])
    rows = []
    for g in games:
        if not isinstance(g, dict):
            continue
        row: Dict[str, Any] = {}
        for k, v in _scrub(g).items():
            if k == "uuid":
                row["gameUuid"] = v
            elif k in _TEAM_PREFIX and isinstance(v, dict):
                row.update(_flatten(v, _TEAM_PREFIX[k]))
            else:
                row.update(_flatten({k: v}))
        for key in [k for k in row if k.endswith("_score")]:
            row[key] = _blank(row[key])  # '' before the game is played
        rows.append(row)
    return _finish(_frame(rows), columns, return_as_pandas)


def _player(prefix: str, rec: Any) -> Dict[str, Any]:
    """A pbp player record -> ``<prefix>player_id`` (Genius) + its names."""
    if not isinstance(rec, dict):
        return {}
    rec = dict(rec)
    out = {f"{prefix}playerId": rec.pop("playerId", None)}
    out.update(_flatten(rec, prefix.rstrip("_") or "player"))
    return out


# Team-block fields kept on a play-by-play line; the block's own ``id`` (a document id) and its always-empty
# ``players`` list are dropped, ``name`` repeats ``teamName``.
_PBP_TEAM_KEYS = ("teamId", "teamCode", "teamName", "abbreviation", "teamNumber", "place", "score")


def parse_sportality_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/gameday/play-by-play/{game_uuid}``: one row per event, in upstream order (2023/24 on).

    ``type`` / ``sub_type`` name the event (``2pt`` / ``layup``, ``3pt`` / ``jumpshot``,
    ``rebound`` / ``defensive``, ``substitution`` / ``in``, ``clock`` / ``stop``, ``game`` /
    ``end``, ...). Shots and fouls carry ``coordinates_x`` / ``coordinates_y`` (Genius court
    units; the origin and orientation of y are unverified). ``home_*`` / ``away_*`` give the
    running score (``home_score``) and the teams (``home_team_id`` = Genius team id);
    ``event_team_*`` is the team the event belongs to, ``player_*`` the player
    (``player_id`` = Genius person id, ``player_pno`` the jersey number in the game).
    ``is_successful`` is 1/0 on shots and absent on other events. ``qualifiers`` is
    normalised to a ``;``-joined string (the API sends a list in recent games, a ``;``-joined
    string or null in older ones). ``time`` is the clock remaining in the period.

    ``game_source_id`` and ``game_id`` are the Genius match id -- a STRING in this body, cast to
    ``Int64`` like everywhere else.

    Args:
        raw (Any): the decoded JSON body (a list of events).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per event; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one shot::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_pbp
            parse_sportality_pbp([{"type": "2pt", "subType": "layup", "player": {"playerId": 7},
                                   "coordinates": {"x": 8.5, "y": 58.0}}])
    """
    rows = []
    for ev in raw if isinstance(raw, list) else []:
        if not isinstance(ev, dict):
            continue
        row: Dict[str, Any] = {}
        for k, v in ev.items():
            if k in ("homeTeam", "awayTeam", "eventTeam") and isinstance(v, dict):
                prefix = k[: -len("Team")]
                row.update(_flatten({f: v[f] for f in _PBP_TEAM_KEYS if f in v}, prefix))
            elif k == "player":
                row.update(_player("", v))
            elif k == "qualifiers":
                row[k] = ";".join(map(str, v)) if isinstance(v, list) else v
            else:
                row.update(_flatten({k: v}))
        rows.append(row)
    return _finish(_frame(rows), columns, return_as_pandas)


def parse_sportality_player_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse ``/gameday/boxscore/{game_uuid}`` or ``/gameday/player-stats/{game_uuid}`` (2023/24 on).

    One row per player and game, then each side's two summary rows: ``row_type`` is ``player``,
    ``team_coach`` (the bench: team rebounds, turnovers and fouls charged to the team or coach)
    or ``team_totals``. ``team_side`` is home / away, taken from the side the row sits under.
    ``player_id`` is the Genius person id (``Int64``, null on summary rows) with the name record
    joined from the body's ``players`` map (keyed by the same id).

    ``team_id`` is the Genius team id of the SIDE: the wire's ``info.teamId`` on player rows
    held the AWAY team's id for both sides in every game checked, so it is replaced by the id
    on the side's own summary rows (null where a side has none, as on ``player-stats``, whose
    rows carry no team id). ``mins`` is ``mm:ss``, ``fg`` / ``2p`` / ``3p`` / ``ft`` are
    ``made-attempted`` strings, and every ``*_perc`` is a 0-100 percentage.
    ``player-stats`` rows add ``team_number`` (1 = home, 2 = away) and ``pno`` (the player's number in Genius's shirt table).

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player and summary row; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one player row::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_player_box
            parse_sportality_player_box({"stats": {"homeTeamValue": [{"PTS": 12, "info": {"playerId": 7}}]}})
    """
    rows: List[Dict[str, Any]] = []
    stats: set = set()
    body = _scrub(_dict(raw))
    sides = _dict(body.get("stats"))
    names = _dict(body.get("players"))
    for side_key, side in _BOX_SIDES:
        recs = [r for r in _list(sides.get(side_key)) if isinstance(r, dict)]
        side_names = _dict(names.get(side_key))
        team_id = next(
            (
                _dict(r.get("info")).get("teamId")
                for r in recs
                if _dict(r.get("info")).get("label") == "sports.stats.team_totals"
            ),
            None,
        )
        for rec in recs:
            info = _dict(rec.get("info"))
            pid = _blank(info.get("playerId"))
            row: Dict[str, Any] = {
                "teamSide": side,
                "rowType": _BOX_LABELS.get(str(info.get("label")), "player"),
                "playerId": pid,
                "teamId": team_id,
                "teamName": info.get("team"),
                "period": info.get("period"),
            }
            row.update({k: v for k, v in _dict(side_names.get(str(pid))).items() if k != "extIds"})
            cells = {k: _blank(v) for k, v in rec.items() if k != "info"}
            stats.update(k for k, v in cells.items() if k not in _NOT_STATS and not isinstance(v, (dict, list)))
            row.update(cells)
            rows.append(row)
        side_team = next((r["teamName"] for r in rows if r["teamSide"] == side and r["teamName"]), None)
        for r in rows:
            if r["teamSide"] == side:
                r["teamName"] = side_team  # the summary rows carry no team name
    return _finish(_frame(rows, stats), columns, return_as_pandas)


def parse_sportality_team_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse ``/gameday/team-stats/{game_uuid}``: one row per side and period (2023/24 on).

    ``period`` 0 is the whole game, 1-4 the quarters (5+ overtime). The game header of each
    side (``game_uuid``, ``game_id`` = Genius match id, ``team_id`` = Genius team id,
    ``team_code``) repeats on its rows; the per-period ``parsedTotalStatistics`` key/value pairs
    become one column each (``tp`` points, ``fgm``, ``fga``, ...), ``Float64``.

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per side and period; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one side::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_team_stats
            parse_sportality_team_stats({"home": {"teamId": "153747", "statistics": [
                {"period": 0, "parsedTotalStatistics": [{"key": "TP", "value": 84}]}]}})
    """
    rows: List[Dict[str, Any]] = []
    stat_keys: List[str] = []
    body = _dict(raw)
    for side in ("home", "away"):
        team = body.get(side)
        if not isinstance(team, dict):
            continue
        head = {"teamSide": side, **{k: v for k, v in team.items() if k not in ("statistics", "place")}}
        for per in _list(team.get("statistics")):
            if not isinstance(per, dict):
                continue
            row = {**head, "period": per.get("period"), "periodType": per.get("periodType")}
            for kv in _list(per.get("parsedTotalStatistics")):
                if isinstance(kv, dict) and kv.get("key"):
                    row[str(kv["key"])] = kv.get("value")
                    stat_keys.append(str(kv["key"]))
            rows.append(row)
    return _finish(_frame(rows, stat_keys), columns, return_as_pandas)


def parse_sportality_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse ``/gameday/team-standings/{ssgt_uuid}``: one row per team.

    ``team_id`` is the Genius team id, ``team_uuid`` the platform team uuid, ``team_code`` the
    platform code (``TFN``). ``gp`` games played, ``w`` / ``l`` wins / losses, ``tp`` table
    points, ``for`` / ``ga`` points scored / conceded, ``plus_minus`` their difference and
    ``strk`` the current streak as a signed integer (positive = wins in a row, negative =
    losses in a row). The body names neither its season nor its ssgt: the rows are the ssgt in
    the request path.

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per team; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one row::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_standings
            parse_sportality_standings({"leagueStandings": [{"Rank": 1, "GP": 4, "W": 4, "L": 0,
                "info": {"teamId": 22255}}]})
    """
    body = _dict(raw)
    rows = []
    for rec in _list(body.get("leagueStandings")):
        if not isinstance(rec, dict):
            continue
        info = _dict(rec.get("info"))
        team = _dict(info.get("teamInfo"))
        names = _dict(team.get("teamNames"))
        row: Dict[str, Any] = {
            "teamId": info.get("teamId"),
            "teamUuid": team.get("teamUuid"),
            "teamCode": names.get("code"),
            "teamName": names.get("long") or info.get("code"),
        }
        row.update({k: v for k, v in rec.items() if k not in ("info", "Team")})
        row["Strk"] = _as_int(row.get("Strk"))
        rows.append(row)
    return _finish(_frame(rows), columns, return_as_pandas)


def _genius_by_uuid(players: Any) -> Dict[str, int]:
    """``players{<genius id>: {uuid}}`` -> ``{uuid: genius id}``."""
    out: Dict[str, int] = {}
    if isinstance(players, dict):
        for k, v in players.items():
            if isinstance(v, dict) and v.get("uuid") and str(k).isdigit():
                out.setdefault(str(v["uuid"]), int(k))
    return out


# The team record of a stats-info team row, cut to the fields that identify the team.
_TEAM_INFO_KEYS = ("id", "uuid", "teamCode", "name", "siteDisplayName", "ownerInstanceId", "gender", "nationality")


def _stats_info(info: Dict[str, Any]) -> Dict[str, Any]:
    """A stats-info row's ``info`` record with the team ids named consistently.

    Player rows spell the Genius team id ``teamCode`` (an integer) and the team uuid ``teamId``;
    both are renamed to ``teamId`` (Genius) / the existing ``team.uuid``. A team row's ``id`` is
    its Genius team id and its record is cut to :data:`_TEAM_INFO_KEYS`.
    """
    info = {k: v for k, v in info.items() if k != "playerMedia"}  # portrait plumbing
    if isinstance(info.get("teamCode"), int) and not isinstance(info.get("teamCode"), bool):
        info["teamId"] = info.pop("teamCode")  # Genius team id; the old teamId (uuid) repeats team.uuid
    elif info.get("type") == "team":
        info = {("teamId" if k == "id" else k): info[k] for k in _TEAM_INFO_KEYS if k in info}
    return info


def parse_sportality_stats_info(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse ``/statistics-v2/stats-info/{module}`` and the athlete career log.

    One row per ``stats[]`` entry. The stat cells are the module's columns (``gp``, ``ppg``,
    ``eff``, ...; see the per-module tables in the refs returns doc), ``Float64`` unless a column
    holds non-numeric text (``mins`` ``"09:07"`` stays ``Utf8``). The row's ``info`` record
    becomes ``info_*``: for player modules ``info_uuid`` is the athlete uuid, ``team_id`` the
    Genius team id (the wire's ``info.teamCode``) and ``player_id`` is added -- the Genius person
    id, looked up from the body's ``players{<genius id>}`` map (null where the map has no uuid,
    which is common before about 2005). For team modules ``info_team_id`` is the Genius team id;
    ``games_games`` carries ``info_game_id`` (the Genius match id), ``info_home_team_id`` and
    ``info_away_team_id`` and splits ``GameScore`` into ``home_score`` / ``away_score`` /
    ``decided_in``. The placeholder cells ``Rank`` (``standings_standings`` excepted), ``Player``
    and ``Team`` are dropped. Birth dates are dropped.

    Args:
        raw (Any): the decoded JSON body (a one-item list).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per table row; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one row::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_stats_info
            parse_sportality_stats_info([{"stats": [{"GP": 5, "PPG": "20.0", "info": {"uuid": "u1"}}],
                                          "players": {"1887576": {"uuid": "u1"}}}])
    """
    body = _scrub(_body(raw))
    if not isinstance(body, dict):
        return _finish(pl.DataFrame(), columns, return_as_pandas)
    genius = _genius_by_uuid(body.get("players")) if body.get("type") != "athlete_single" else {}
    rows = []
    stats: set = set()
    for rec in _list(body.get("stats")):
        if not isinstance(rec, dict):
            continue
        info = _stats_info(_dict(rec.get("info")))
        row: Dict[str, Any] = {}
        if genius:
            row["playerId"] = genius.get(str(info.get("uuid")))
        cells = {
            k: _blank(v)
            for k, v in rec.items()
            if k != "info" and k not in _DUPLICATE_CELLS and not (k in _PLACEHOLDERS and (v in (None, "", 0)))
        }
        score = cells.pop("GameScore", None)
        if isinstance(score, dict):
            cells.update(
                homeScore=_as_int(score.get("homeScore")),
                awayScore=_as_int(score.get("awayScore")),
                decidedIn=_blank(score.get("decidedIn")),
            )
        stats.update(
            k
            for k, v in cells.items()
            if k not in _NOT_STATS and not isinstance(v, (dict, list)) and k not in ("homeScore", "awayScore")
        )
        row.update(_flatten(cells))
        row.update(_flatten(info, "info"))
        rows.append(row)
    return _finish(_frame(rows, stats), columns, return_as_pandas)


def parse_sportality_player_crosswalk(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse the ``players{}`` map of a stats-info body: one row per player (Genius id -> athlete uuid).

    ``player_id`` is the Genius person id (the map key, ``Int64``), ``athlete_uuid`` the platform
    athlete uuid (null where the map has none, common before about 2005), plus the name,
    nationality, gender, position, jersey number, height (cm) and weight (kg) the map carries.
    Birth dates are dropped.

    Args:
        raw (Any): the decoded stats-info JSON body (a one-item list).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse a map::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_player_crosswalk
            parse_sportality_player_crosswalk([{"players": {"1887576": {"uuid": "dpu0901c0t"}}}])
    """
    body = _scrub(_body(raw))
    players = body.get("players") if isinstance(body, dict) else None
    rows = []
    for k, v in players.items() if isinstance(players, dict) else []:
        if not isinstance(v, dict) or not str(k).isdigit():
            continue
        rec = {kk: vv for kk, vv in v.items() if kk not in ("id", "uuid", "type", "playerMedia", "teamPlayers")}
        rows.append({"playerId": int(k), "athleteUuid": v.get("uuid"), **_flatten(rec)})
    return _finish(_frame(rows), columns, return_as_pandas)


def parse_sportality_layout(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse ``/statistics-v2/layout-info``: one row per ssgt (season + series + game type).

    ``season`` is the END year (``season_code`` 2025 = 2025/26 -> 2026); ``game_type`` is
    ``regular`` or ``finals`` (the playoffs); ``ssgt_uuid`` is what the stats and standings
    routes key on, and ``season_uuid`` / ``series_uuid`` / ``game_type_uuid`` are the three uuids
    ``game-schedule`` takes. ``state`` is active or closed. The per-ssgt team lists are left out.

    Args:
        raw (Any): the decoded JSON body (a one-item list; the first item is read).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ssgt; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one ssgt::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_layout
            parse_sportality_layout([{"seasonSeriesGameTypeTeams": [{"ssgtUuid": "xs07bd3fmp",
                "season": {"code": "2026"}, "gameType": {"code": "regular"}}]}])
    """
    body = _body(raw)
    ssgts = body.get("seasonSeriesGameTypeTeams") if isinstance(body, dict) else None
    rows = []
    for s in ssgts if isinstance(ssgts, list) else []:
        if not isinstance(s, dict):
            continue
        season = _dict(s.get("season"))
        gtype = _dict(s.get("gameType"))
        series = _dict(s.get("series"))
        code = str(season.get("code") or "")
        rows.append(
            {
                "season": int(code) + 1 if code.isdigit() else None,
                "seasonCode": code or None,
                "seasonName": season.get("name"),
                "gameType": gtype.get("code"),
                "gameTypeName": gtype.get("name"),
                "seriesCode": series.get("code"),
                "ssgtUuid": s.get("ssgtUuid"),
                "state": s.get("state"),
                "seasonUuid": season.get("uuid"),
                "seriesUuid": series.get("uuid"),
                "gameTypeUuid": gtype.get("uuid"),
            }
        )
    return _finish(_frame(rows), columns, return_as_pandas)


def parse_sportality_filter(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse ``/sports-v2/season-series-game-types-filter``: one row per series, season and game type.

    ``kind`` is series, season or game_type; ``code`` is the series code (DBL), the season
    START year (``2025`` = 2025/26) or the game type code; ``name`` is the English label when
    there is one, else the first; ``uuid`` is the value the ``game-schedule`` parameters take.
    The filter lists only the regular game type: the playoffs are in ``layout-info``.

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per entry; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one season::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_filter
            parse_sportality_filter({"season": [{"uuid": "ggp2aapun2", "code": "2025"}]})
    """
    body = _dict(raw)
    rows = []
    for key, kind in (("series", "series"), ("season", "season"), ("gameType", "game_type")):
        for rec in _list(body.get(key)):
            if not isinstance(rec, dict):
                continue
            names = {n.get("language"): n.get("translation") for n in _list(rec.get("names")) if isinstance(n, dict)}
            name = names.get("en") or next(iter(names.values()), None)
            rows.append(
                {
                    "kind": kind,
                    "code": rec.get("code"),
                    "name": name,
                    "uuid": rec.get("uuid"),
                    "deleted": rec.get("deleted"),
                }
            )
    return _finish(_frame(rows), columns, return_as_pandas)


def parse_sportality_roster(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse ``/sports-v2/athletes/by-team-uuid/{team_uuid}``: one row per player of the current roster.

    ``position`` / ``position_code`` come from the group the player is listed under (Guards,
    G, ...); ``uuid`` is the athlete uuid (NOT the Genius person id: use
    :func:`~sportsdataverse.basketball.dbl.sportality.sportality_athlete_details`);
    ``portrait_url`` is the latest rendered portrait. Birth dates and contact data are dropped.

    Args:
        raw (Any): the decoded JSON body (a list of position groups).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one group::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_roster
            parse_sportality_roster([{"positionCode": "G", "players": [{"uuid": "cj81dc09w3"}]}])
    """
    rows = []
    for group in raw if isinstance(raw, list) else []:
        if not isinstance(group, dict):
            continue
        for p in _list(group.get("players")):
            if not isinstance(p, dict):
                continue
            p = _scrub(p)
            portrait = _dict(p.get("renderedLatestPortrait"))
            rec = {
                k: v
                for k, v in p.items()
                if k not in ("portraitList", "renderedLatestPortrait", "globalPortraitFallback")
            }
            rows.append(
                {
                    "position": group.get("position"),
                    "positionCode": group.get("positionCode"),
                    **_flatten(rec),
                    "portraitUrl": portrait.get("url"),
                }
            )
    return _finish(_frame(rows), columns, return_as_pandas)


def parse_sportality_h2h(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/statistics-v2/player-h2h-v2/...``: one row per player of the head-to-head.

    ``uuid`` is the athlete uuid; ``match_stats_*`` the player's stats in the matchup. The API
    answers for the CURRENT season whatever ssgt the path names (``effective_ssgt_uuid``, kept
    on every row, shows which). Birth dates are dropped.

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player; zero rows when unusable.

    Raises:
        None: a payload that cannot be parsed gives a zero-row frame, never an exception.

    Example:
        Parse one player::

            from sportsdataverse.basketball.dbl.sportality_parsers import parse_sportality_h2h
            parse_sportality_h2h({"effectiveSsgtUuid": "xs07bd3fmp", "players": [{"uuid": "zu0qp4mq65"}]})
    """
    body = _scrub(_dict(raw))
    rows = [
        {**_flatten(p), "effectiveSsgtUuid": body.get("effectiveSsgtUuid")}
        for p in _list(body.get("players"))
        if isinstance(p, dict)
    ]
    return _finish(_frame(rows), columns, return_as_pandas)


def _documented_when_empty(fn: Any, short: str) -> Any:
    """``fn`` that, with no body and no ``columns=``, returns the zero-row frame of its documented schema ``short``."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Frame:
        out = fn(*args, **kwargs)
        if kwargs.get("columns") is None and len(out.columns) == 0:
            return _finish(pl.DataFrame(), list(SCHEMAS[short]), bool(kwargs.get("return_as_pandas", False)))
        return out

    return wrapper


# A parser that serves exactly one schema (its name ends in the schema key) answers an unusable body with that
# schema's zero-row frame even without ``columns=``. The generic parsers (``parse_sportality``, ``_athlete``, ``_filter``,
# ``_h2h``, ``_layout``, ``_page``, ``_player_crosswalk``) serve several schemas: pass ``columns=`` for those.
for _name in __all__:
    _short = _name.removeprefix("parse_sportality_")
    if _name != "parse_sportality" and _short in SCHEMAS:
        globals()[_name] = _documented_when_empty(globals()[_name], _short)
