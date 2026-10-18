"""Parsers for the generated ``shl`` wrappers (``www.shl.se/api``, the SHL site API).

The SHL site (Sportality platform, stats by StatNet) answers keyless JSON in a handful of
shapes. Every parser follows the package parser contract: a polars frame by default, pandas
via ``return_as_pandas=True``, snake_case columns, list / object cells JSON-encoded, and a
zero-row frame (never an exception) on an empty or malformed payload -- with the documented
columns when the generated wrapper passes ``columns=``.

Shapes and their parsers:

* **Schedules** -- ``game-schedule`` ``{gameInfo: [...], teamList, ssgtUuid}``, the match-day
  routes ``{date, games: [...]}`` and a team's ``{playedGames|upcomingGames: [...]}``:
  :func:`parse_shl_schedule`, one row per game.
* **Play-by-play** -- a bare event list: :func:`parse_shl_pbp`, one row per event.
* **Box scores** -- ``boxscore`` / ``player-stats``: per-side skater rows in
  ``stats.{homeTeamValue,awayTeamValue}[]``, goalie rows in ``gkStats.{...}[]``, and the
  ``players`` / ``goalkeepers`` maps of isa id -> name record: :func:`parse_shl_player_box`.
* **Team stats** -- ``{home, away}``, each with ``statistics[]`` per period (period 0 = the
  game): :func:`parse_shl_team_stats`, one row per side and period.
* **Standings** -- the gameday widget ``{ssgtUuid, season, leagueStandings: [...]}``:
  :func:`parse_shl_standings`.
* **Stats tables** -- ``stats-info/{module}`` and the athlete game logs, a one-item list
  ``[{dataColumns, stats: [...], players: {<isa>: ...}, teams: {...}, ...}]``:
  :func:`parse_shl_stats_info`, one row per ``stats[]`` entry. :func:`parse_shl_player_crosswalk`
  reads the ``players{}`` map instead (isa -> athlete uuid).
* **Directories** -- ``layout-info`` (every SHL ssgt): :func:`parse_shl_layout`;
  ``season-series-game-types-filter``: :func:`parse_shl_filter`; a team roster grouped by
  position: :func:`parse_shl_roster`; player head-to-head: :func:`parse_shl_h2h`.
* **Page objects** (a team, a game header or overview, an athlete profile):
  :func:`parse_shl_page`, always one row.
* **Everything else** -- :func:`parse_shl`, the generic row rule: a list -> one row per item;
  a dict holding exactly one list -> that list's items; any other non-empty dict -> one row;
  the ``ssgt/{ssgt}/provider`` route's bare word (``"statnet"``) -> one ``provider`` row.

Id key spaces (one dtype each, cast here at the boundary):

* ``player_id`` and every ``*_player_id`` -- the **isa** id (StatNet player id, e.g. 2427).
  Play-by-play sends it as a STRING, everything else as an integer; it is ``Int64`` in every
  frame. Lists of isa ids (``pop_player_ids`` / ``nep_player_ids``) are ``List[Int64]``.
* ``game_id`` / ``*_game_id`` / ``*_ext_id`` -- the StatNet game id (22003; sports-v2
  ``gameInfo.extId`` is the same number as a string): ``Int64``.
* ``event_id`` -- the StatNet event id within a game: ``Int64``.
* ``*_uuid`` -- platform uuids (game ``bdhvuc5tex``, athlete ``qQ9-349c7WDv6``, team
  ``087a-087aTQv9u``, ssgt ``iuzqg7dqk9``): ``Utf8``. The athlete uuid is the third player id
  system next to isa and ``ramses_legacy`` (athlete-details ``playerExtIds`` only).
* ``team_id`` / ``*_team_id`` / ``team_code`` -- the StatNet team code (``FHC``): ``Utf8``.

Stat cells of the stats tables, box scores and team stats, and every ``*_perc`` rate, are
``Float64`` whenever every value is a number or a numeric string: the API mixes ``"1.29"`` and
``0`` in one column, so a count-looking sample would otherwise type a rate as an integer. A
column holding any other string (``"19:02"`` time on ice) stays ``Utf8``.

When the generated wrapper passes ``columns=``, the frame is conformed to that endpoint's
returns schema (:data:`~sportsdataverse.hockey.shl.shl_schemas.SCHEMAS`): documented columns
first, in order and with the documented dtypes (a missing one is a null column), anything the
API added later after them. An empty or malformed body gives the same columns and dtypes with
zero rows, so a frame from an empty body concatenates with a populated one.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.shl.shl_schemas import SCHEMAS

__all__ = [
    "parse_shl",
    "parse_shl_filter",
    "parse_shl_h2h",
    "parse_shl_layout",
    "parse_shl_page",
    "parse_shl_pbp",
    "parse_shl_player_box",
    "parse_shl_player_crosswalk",
    "parse_shl_roster",
    "parse_shl_schedule",
    "parse_shl_standings",
    "parse_shl_stats_info",
    "parse_shl_team_stats",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

# isa player ids, StatNet game / event ids: Int64 everywhere (see the module docstring).
_INT_ID = re.compile(r"(^|_)(player_id|game_id|event_id|ext_id)$")
# Stat cells that are labels or placeholders, not measurements: never forced to Float64.
_NOT_STATS = frozenset({"Rank", "NR", "LINE", "POS"})
# Stats-table display slots that hold a constant 0 (the site fills them from the info record): dropped.
_PLACEHOLDERS = frozenset({"Player", "Team", "Season", "Series", "GameType"})
_BOX_SIDES = (("homeTeamValue", "home"), ("awayTeamValue", "away"))
_SCHEDULE_KEYS = ("gameInfo", "games", "playedGames", "upcomingGames")
_TEAM_PREFIX = {"homeTeamInfo": "homeTeam", "awayTeamInfo": "awayTeam", "venueInfo": "venue", "seriesInfo": "series"}
# shl_schemas dtype codes -> polars dtypes.
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


def _column(key: str) -> str:
    """Wire key -> snake_case column (``SVS%`` -> ``svs_perc``, ``+/-`` -> ``plus_minus``)."""
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


def _text(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False, sort_keys=True)
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _series(name: str, values: List[Any], stat: bool) -> pl.Series:
    """One column with a dtype taken from the Python types of its values (deterministic)."""
    present = [v for v in values if v is not None]
    if stat and present and all(_as_float(v) is not None for v in present):
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
    """Every isa / StatNet id column -> ``Int64`` (a non-numeric value becomes null)."""
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
    """The one dict of a one-item-list body (stats-info, game logs); ``raw`` otherwise."""
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        return raw[0]
    return raw


# --------------------------------------------------------------------------- generic


def parse_shl(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse any SHL body by the generic row rule (see the module docstring).

    Args:
        raw (Any): the decoded JSON body (or the bare text of the ``provider`` route).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): the documented column names; an empty or malformed
            body gives a zero-row frame with these columns.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per record; zero rows when unusable.

    Example:
        Parse a team page::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl
            parse_shl({"uuid": "087a-087aTQv9u", "names": {"code": "FHC"}})

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_athlete_seasons` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_post_game_team_stats` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_sites` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_ssgt_provider` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows: List[Any] = []
    if isinstance(raw, str) and re.fullmatch(r"[a-z_]{1,40}", raw.strip()):
        rows = [{"provider": raw.strip()}]
    elif isinstance(raw, list):
        rows = raw
    elif isinstance(raw, dict) and raw:
        lists = [v for v in raw.values() if isinstance(v, list)]
        rows = lists[0] if len(lists) == 1 else [raw]
    flat = [_flatten(r) for r in rows if isinstance(r, dict)]
    return _finish(_frame(flat), columns, return_as_pandas)


def parse_shl_page(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse a page object (a team, a game header, an athlete profile) into ONE row.

    Nested objects become prefixed columns (``home_team_names_code``); lists are JSON-encoded
    (a team page's ``contact`` list must not become the rows, which the generic rule would do).
    ``game_info_ext_id`` (sports-v2 game-info) is the StatNet game id, ``Int64``.

    Args:
        raw (Any): the decoded JSON body (a dict).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row; zero rows when the body is not a non-empty dict.

    Example:
        Parse a team page::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_page
            parse_shl_page({"uuid": "087a-087aTQv9u", "contact": [{"type": "facebook"}]})

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_athlete_details` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_athlete_profile` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_game_header` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_game_overview` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows = [_flatten(raw)] if isinstance(raw, dict) and raw else []
    return _finish(_frame(rows), columns, return_as_pandas)


# --------------------------------------------------------------------------- dedicated shapers


def parse_shl_schedule(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse a schedule body: ``game-schedule``, ``last|next-game-day``, ``played|upcoming-games``.

    One row per game. ``game_uuid`` is the platform game uuid every gameday route takes; the
    team blocks become ``home_team_*`` / ``away_team_*`` (``*_uuid`` = team uuid, ``*_code`` =
    team code, ``*_status`` = WIN / LOSE), plus ``venue_*`` and ``series_*``.

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per game; zero rows when unusable.

    Example:
        Parse one match day::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_schedule
            parse_shl_schedule({"date": "2026-10-10", "games": [{"uuid": "f0qxq2l2lo"}]})

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_last_game_day` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_next_game_day` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_schedule` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_team_played_games` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    games: List[Any] = []
    if isinstance(raw, dict):
        games = next((raw[k] for k in _SCHEDULE_KEYS if isinstance(raw.get(k), list)), [])
    rows = []
    for g in games:
        if not isinstance(g, dict):
            continue
        row: Dict[str, Any] = {}
        for k, v in g.items():
            if k == "uuid":
                row["gameUuid"] = v
            elif k in _TEAM_PREFIX and isinstance(v, dict):
                row.update(_flatten(v, _TEAM_PREFIX[k]))
            else:
                row.update(_flatten({k: v}))
        rows.append(row)
    return _finish(_frame(rows), columns, return_as_pandas)


def _player(prefix: str, rec: Any) -> Dict[str, Any]:
    """A pbp player record -> ``<prefix>player_id`` (isa) + its names; ``statistics`` dropped."""
    if not isinstance(rec, dict):
        return {}
    rec = {k: v for k, v in rec.items() if k != "statistics"}
    out = {f"{prefix}playerId": rec.pop("playerId", None)}
    out.update(_flatten(rec, prefix.rstrip("_") or "player"))
    return out


def _isa_list(recs: Any) -> Optional[List[Optional[int]]]:
    if not isinstance(recs, list):
        return None
    out: List[Optional[int]] = []
    for r in recs:
        pid = str(_dict(r).get("playerId"))
        out.append(int(pid) if pid.isdigit() else None)
    return out


def parse_shl_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/gameday/play-by-play/{game_uuid}``: one row per event, in upstream order.

    ``type`` is goal, shot, penalty, goalkeeper (in / out), period or timeout. Shots and goals
    carry ``location_x`` / ``location_y`` (over one game x ran -29..481 and y -140..129; units,
    origin and orientation unverified) and ``goal_section`` (``-3`` lines up with blocked shots
    in one game: an inference). Goals add ``goal_status`` (PP1, EQ, ...), the running
    ``home_goals`` / ``away_goals``, ``assist1_*`` / ``assist2_*`` and the on-ice players:
    ``pop_player_ids`` (players on the scoring side) and ``nep_player_ids`` (the other side),
    both ``List[Int64]`` of isa ids. Penalties add ``offence`` and ``variant_*``.

    ``player_id`` is the isa id -- a STRING in this body, cast to ``Int64`` like everywhere else.

    Args:
        raw (Any): the decoded JSON body (a list of events).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per event; zero rows when unusable.

    Example:
        Parse one shot::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_pbp
            parse_shl_pbp([{"type": "shot", "player": {"playerId": "3622"}, "locationX": 12}])

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_pbp` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows = []
    for ev in raw if isinstance(raw, list) else []:
        if not isinstance(ev, dict):
            continue
        row: Dict[str, Any] = {}
        for k, v in ev.items():
            if k in ("homeTeam", "awayTeam", "eventTeam") and isinstance(v, dict):
                row.update(_flatten(v, k[: -len("Team")]))
            elif k == "player":
                row.update(_player("", v))
            elif k == "assists" and isinstance(v, dict):
                row.update(_player("assist1_", v.get("first")))
                row.update(_player("assist2_", v.get("second")))
            elif k in ("pop", "nep"):
                row[f"{k}PlayerIds"] = _isa_list(v)
            else:
                row.update(_flatten({k: v}))
        rows.append(row)
    return _finish(_frame(rows), columns, return_as_pandas)


def parse_shl_player_box(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/gameday/boxscore/{game_uuid}`` or ``/gameday/player-stats/{game_uuid}``.

    One row per player and game: skaters from ``stats.{homeTeamValue,awayTeamValue}[]``
    (``position_group = "skater"``) and goalies from ``gkStats.{...}[]`` (``"goalie"``), with
    ``team_side`` (home / away), ``player_id`` (isa, ``Int64``) and the name record joined from
    the body's ``players`` / ``goalkeepers`` map (keyed by the same isa id). Skater and goalie
    stat columns differ; a column a row does not have is null. ``svs_perc`` is a 0-1 fraction.
    ``player-stats`` rows add ``tp``, ``shg``, ``nep``, ``pop``, ``fo_tot``, ``num_pen``,
    ``ms`` and ``bk_s``.

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player; zero rows when unusable.

    Example:
        Parse one skater row::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_player_box
            parse_shl_player_box({"stats": {"homeTeamValue": [{"G": 1, "info": {"playerId": 7}}]}})

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_boxscore` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_player_box` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows: List[Dict[str, Any]] = []
    stats: set = set()
    body = _dict(raw)
    for group, stats_key, map_key in (("skater", "stats", "players"), ("goalie", "gkStats", "goalkeepers")):
        sides = body.get(stats_key)
        names = _dict(body.get(map_key))
        for side_key, side in _BOX_SIDES:
            recs = sides.get(side_key) if isinstance(sides, dict) else None
            side_names = _dict(names.get(side_key))
            for rec in recs if isinstance(recs, list) else []:
                if not isinstance(rec, dict):
                    continue
                info = _dict(rec.get("info"))
                pid = info.get("playerId")
                name = _dict(side_names.get(str(pid)))
                row: Dict[str, Any] = {"teamSide": side, "positionGroup": group, "playerId": pid}
                row["teamId"] = info.get("teamId")
                row["period"] = info.get("period")
                row.update({k: v for k, v in name.items() if k != "extIds"})
                cells = {k: v for k, v in rec.items() if k != "info"}
                stats.update(k for k, v in cells.items() if k not in _NOT_STATS and not isinstance(v, (dict, list)))
                row.update(cells)
                rows.append(row)
    return _finish(_frame(rows, stats), columns, return_as_pandas)


def parse_shl_team_stats(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/gameday/team-stats/{game_uuid}``: one row per side and period.

    ``period`` 0 is the whole game, 1-3 the periods (4+ overtime). The game header of each side
    (``game_uuid``, ``game_id`` = StatNet id, ``team_id`` / ``team_code``, ``score``) repeats on
    its rows; the per-period ``parsedTotalStatistics`` key/value pairs become one column each
    (``g``, ``pim``, ``fow``, ...), ``Float64``.

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per side and period; zero rows when unusable.

    Example:
        Parse one side::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_team_stats
            parse_shl_team_stats({"home": {"teamId": "FHC", "statistics": [
                {"period": 0, "parsedTotalStatistics": [{"key": "G", "value": 2}]}]}})

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_team_stats` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows: List[Dict[str, Any]] = []
    stat_keys: List[str] = []
    body = _dict(raw)
    for side in ("home", "away"):
        team = body.get(side)
        if not isinstance(team, dict):
            continue
        head = {"teamSide": side, **{k: v for k, v in team.items() if k != "statistics"}}
        for per in _list(team.get("statistics")):
            if not isinstance(per, dict):
                continue
            row = {**head, "period": per.get("period")}
            for kv in _list(per.get("parsedTotalStatistics")):
                if isinstance(kv, dict) and kv.get("key"):
                    row[str(kv["key"])] = kv.get("value")
                    stat_keys.append(str(kv["key"]))
            rows.append(row)
    return _finish(_frame(rows, stat_keys), columns, return_as_pandas)


def parse_shl_standings(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/gameday/team-standings/{ssgt_uuid}``: one row per team.

    ``team_code`` is the StatNet team code (SAIK), ``team_uuid`` the platform team uuid.
    ``w`` / ``l`` are REGULATION wins / losses, ``t`` the games tied after regulation, and
    ``otw`` / ``otl`` the overtime-or-shootout wins / losses, so ``gp = w + t + l``. (The
    stats-info ``teams_result`` module counts W / L differently: totals including overtime and
    shootouts.) ``season`` is the END year (the body's ``season`` 2025 = 2025/26 -> 2026).

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per team; zero rows when unusable.

    Example:
        Parse one row::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_standings
            parse_shl_standings({"season": 2025, "leagueStandings": [{"id": "SAIK", "rank": 1, "T": 12}]})

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_standings` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    body = _dict(raw)
    start = body.get("season")
    rows = []
    for rec in _list(body.get("leagueStandings")):
        if not isinstance(rec, dict):
            continue
        info = _dict(rec.get("info"))
        team = _dict(info.get("teamInfo"))
        row: Dict[str, Any] = {
            "season": int(str(start)) + 1 if str(start).isdigit() else None,
            "ssgtUuid": body.get("ssgtUuid"),
            "teamCode": rec.get("id"),
            "teamUuid": team.get("teamUuid"),
            "teamName": _dict(team.get("teamNames")).get("long"),
            "teamShortName": info.get("code"),
        }
        row.update({k: v for k, v in rec.items() if k not in ("id", "info")})
        rows.append(row)
    return _finish(_frame(rows), columns, return_as_pandas)


def _isa_by_uuid(players: Any) -> Dict[str, int]:
    """``players{<isa>: {uuid}}`` -> ``{uuid: isa}``."""
    out: Dict[str, int] = {}
    if isinstance(players, dict):
        for k, v in players.items():
            if isinstance(v, dict) and v.get("uuid") and str(k).isdigit():
                out.setdefault(str(v["uuid"]), int(k))
    return out


def parse_shl_stats_info(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/statistics-v2/stats-info/{module}`` and the athlete game logs.

    One row per ``stats[]`` entry. The stat cells are the module's columns (``gp``, ``tp``,
    ``svs_perc``, ...; see the per-module tables in the refs returns doc), ``Float64`` unless a
    column holds non-numeric text (``toi_gp`` ``"19:02"`` stays ``Utf8``). The row's ``info``
    record becomes ``info_*``: for player and goalie modules ``info_uuid`` is the athlete uuid
    and ``player_id`` is added -- the isa id, looked up from the body's ``players{<isa>}`` map
    (null where the map has no uuid, which is common before about 2005). For team modules
    ``info_id`` / ``info_team_code`` are the team code; ``games_games`` carries
    ``info_game_id`` (StatNet). ``teams_result`` W / L are TOTAL wins / losses (including
    overtime and shootouts); the ``standings_standings`` module's are regulation only.

    Args:
        raw (Any): the decoded JSON body (a one-item list).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per table row; zero rows when unusable.

    Example:
        Parse one row::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_stats_info
            parse_shl_stats_info([{"stats": [{"GP": 52, "TPPG": "1.29", "info": {"uuid": "u1"}}],
                                   "players": {"2427": {"uuid": "u1"}}}])

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_athlete_career` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_athlete_game_log` - the wrapper whose body this parses.
        * :func:`sportsdataverse.hockey.shl.shl_stats_info` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    body = _body(raw)
    if not isinstance(body, dict):
        return _finish(pl.DataFrame(), columns, return_as_pandas)
    isa = _isa_by_uuid(body.get("players")) if body.get("type") != "athlete_single" else {}
    rows = []
    stats: set = set()
    for rec in _list(body.get("stats")):
        if not isinstance(rec, dict):
            continue
        info = _dict(rec.get("info"))
        row: Dict[str, Any] = {}
        if isa:
            row["playerId"] = isa.get(str(info.get("uuid")))
        cells = {k: v for k, v in rec.items() if k != "info" and not (k in _PLACEHOLDERS and _is_number(v))}
        stats.update(k for k, v in cells.items() if k not in _NOT_STATS and not isinstance(v, (dict, list)))
        row.update(_flatten(cells))
        row.update(_flatten(info, "info"))
        rows.append(row)
    return _finish(_frame(rows, stats), columns, return_as_pandas)


def parse_shl_player_crosswalk(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Parse the ``players{}`` map of a stats-info body: one row per player (isa -> athlete uuid).

    ``player_id`` is the isa id (the map key, ``Int64``), ``athlete_uuid`` the platform athlete
    uuid (null where the map has none, common before about 2005), plus the name, birth date,
    nationality, position and jersey number the map carries. Skaters and goalies both appear in
    the ``players_*`` modules.

    Args:
        raw (Any): the decoded stats-info JSON body (a one-item list).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player; zero rows when unusable.

    Example:
        Parse a map::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_player_crosswalk
            parse_shl_player_crosswalk([{"players": {"2427": {"uuid": "qQ9-349c7WDv6"}}}])

    See Also:
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    body = _body(raw)
    players = body.get("players") if isinstance(body, dict) else None
    rows = []
    for k, v in players.items() if isinstance(players, dict) else []:
        if not isinstance(v, dict) or not str(k).isdigit():
            continue
        rec = {kk: vv for kk, vv in v.items() if kk not in ("id", "uuid", "type", "playerMedia", "teamPlayers")}
        rows.append({"playerId": int(k), "athleteUuid": v.get("uuid"), **_flatten(rec)})
    return _finish(_frame(rows), columns, return_as_pandas)


def parse_shl_layout(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/statistics-v2/layout-info``: one row per SHL ssgt (season + series + game type).

    ``season`` is the END year (``season_code`` 2025 = 2025/26 -> 2026); ``game_type`` is
    regular, playoff, qualdown or playin; ``ssgt_uuid`` is what the stats and standings routes
    key on, and ``season_uuid`` / ``series_uuid`` / ``game_type_uuid`` are the three uuids
    ``game-schedule`` takes. ``state`` is active or closed. The per-ssgt team lists are left out.

    Args:
        raw (Any): the decoded JSON body (one item per stats provider; the first is read).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per ssgt; zero rows when unusable.

    Example:
        Parse one ssgt::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_layout
            parse_shl_layout([{"seasonSeriesGameTypeTeams": [{"ssgtUuid": "iuzqg7dqk9",
                "season": {"code": "2025"}, "gameType": {"code": "regular"}}]}])

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_layout_info` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
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


def parse_shl_filter(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/sports-v2/season-series-game-types-filter``: one row per series, season and game type.

    ``kind`` is series, season or game_type; ``code`` is the series code (SHL, CHL), the season
    START year (``2025`` = 2025/26) or the game type code; ``name`` is the English label when
    there is one, else the first; ``uuid`` is the value the ``game-schedule`` parameters take.

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per entry; zero rows when unusable.

    Example:
        Parse one season::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_filter
            parse_shl_filter({"season": [{"uuid": "xs4m9qupsi", "code": "2025"}]})

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_season_filter` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
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


def parse_shl_roster(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/sports-v2/athletes/by-team-uuid/{team_uuid}``: one row per player of the current roster.

    ``position`` / ``position_code`` come from the group the player is listed under (GK, ...);
    ``uuid`` is the athlete uuid; ``portrait_url`` is the latest rendered portrait.

    Args:
        raw (Any): the decoded JSON body (a list of position groups).
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player; zero rows when unusable.

    Example:
        Parse one group::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_roster
            parse_shl_roster([{"positionCode": "GK", "players": [{"uuid": "qd1-1JVKWgydZ"}]}])

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_roster` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows = []
    for group in raw if isinstance(raw, list) else []:
        if not isinstance(group, dict):
            continue
        for p in _list(group.get("players")):
            if not isinstance(p, dict):
                continue
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


def parse_shl_h2h(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Parse ``/statistics-v2/player-h2h-v2/...``: one row per player of the head-to-head.

    ``uuid`` is the athlete uuid; ``match_stats_*`` the player's stats in the matchup. The API
    answers for the CURRENT season whatever ssgt the path names (``effective_ssgt_uuid``, kept
    on every row, shows which).

    Args:
        raw (Any): the decoded JSON body.
        return_as_pandas (bool): return a pandas DataFrame instead of polars.
        columns (Sequence[str] | None): documented columns for the zero-row frame.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per player; zero rows when unusable.

    Example:
        Parse one player::

            from sportsdataverse.hockey.shl.shl_parsers import parse_shl_h2h
            parse_shl_h2h({"effectiveSsgtUuid": "qa98unlbd6", "players": [{"uuid": "qQ9-9d31rLEXi"}]})

    See Also:
        * :func:`sportsdataverse.hockey.shl.shl_player_h2h` - the wrapper whose body this parses.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    body = _dict(raw)
    rows = [
        {**_flatten(p), "effectiveSsgtUuid": body.get("effectiveSsgtUuid")}
        for p in _list(body.get("players"))
        if isinstance(p, dict)
    ]
    return _finish(_frame(rows), columns, return_as_pandas)
