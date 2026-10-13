"""Parsers for the generated ``nbl_rosetta`` wrappers (NBL / WNBL / NBL1, ``prod.rosetta.nbl.com.au``).

Rosetta is the JSON proxy behind nbl.com.au. For the 2025-26 NBL season it is the only play-by-play
source this package has: ESPN's ``nbl`` feed carries box scores but **no plays** for 2025-26 (the stats
provider changed from Genius Sports to Sportradar DataCore that year), while
:func:`parse_nbl_rosetta_match_pbp` reads the full event log (982 events with shot x/y for the sample game),
and Rosetta backfills the Genius era into the same shape (box scores from 1979, play-by-play from 2011-12).

Every parser follows the package contract: a polars frame by default, pandas with ``return_as_pandas=True``,
snake_cased columns (:func:`sportsdataverse.dl_utils.underscore`), and a zero-row frame (never an exception)
for an empty or malformed body. Given ``columns`` (the generated wrappers pass the endpoint's documented
returns-schema), an empty body keeps those columns and a body missing one of them gains it as nulls. Each
parser takes the whole envelope ``{type, fetched, ttlRemaining, count, source, data}`` (or its ``data``).

**Row rule.** ``data`` is a list of rows (one frame row each) or, on the outcomes route, a map keyed by team
name (rows carry ``team_name``). A row's nested objects flatten into ``<parent>_<child>`` columns; lists become
JSON strings. Presentation fields are dropped (logos, colours, blurhashes, social links, ticket and shop URLs,
broadcasters, photo URLs) and so are the betting fields, which the live bodies carry.

**Id key spaces.** Rosetta has three. Every id column here is a String and every entity id is normalised the
same way, so frames join without a lookup:

* ``<entity>_id`` (``season_id``, ``team_id``, ``player_id``, ``match_id``, ``venue_id``) is the **DataCore**
  id (UUID v1, Sportradar), the ``external_id`` of the body. It is stable across seasons and accepted by every
  route that takes that entity. ``match_id`` is the DataCore fixture id, the key of ``nbl_rosetta_match`` (and
  ``fixtureId`` in play-by-play). Join on these.
* ``rosetta_<entity>_id`` is Rosetta's own id (UUID v4; v5 on NBL1), present where it differs. A team or
  player reached without a DataCore id in the body (a game log's ``match_home_team``) only has this one.
* ``rosetta_match_id`` is the match RECORD id (UUID v4) that box-score and play-by-play rows carry in
  ``match``. No route accepts it.
* ``stat_line_id`` / ``rosetta_stat_line_id`` identify one box-score line (an opaque hash / UUID).
* ``external_media_id`` holds the Genius Sports match id up to NBL25 and 8-digit numbers or date-like codes of
  unverified origin from NBL26. It is text, never joined on.

**Season key.** Rosetta's ``year`` is a START year for the NBL and WNBL from 1998-99 and a calendar year for
NBL1 and the 1979-1997 winter seasons (``season_year``, text in the body, Int64 here). ``season_end_year`` is
the sdv key (2026 = 2025-26) derived from it (:func:`sportsdataverse.basketball.nbl_au.nbl_rosetta_runtime.end_year`);
year 1998 holds two seasons and is split on the start date. ``org`` (``nbl``, ``wnbl``, ``nbl1``) comes from
the row's competition or, where the body does not say, from the request the runtime stamps under ``_request``.

**Dtypes** are pinned by the returns-schema (``nbl_rosetta_dtypes.DTYPES``, generated with the schemas): an
empty frame, a gap-filled column and an all-null column are typed like a populated frame, so frames stack.
Rosetta sends numbers as strings (``home_score``, ``attendance``, ``period``, a box score's ``field_goals_made``
on some rows), writes a rate as ``1`` when whole, and gives ``minutes`` as ``"MM:SS"`` on the live box score and
decimal minutes elsewhere; every ``*_percentage``, ``*_average`` and ``minutes`` column is Float64 (minutes as
decimal minutes) and every count Int64. Percentages are 0-1 fractions (``points_percentage`` in standings is a
ratio: 1.1379 = 113.79 %).
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball.nbl_au.nbl_rosetta_dtypes import DTYPES
from sportsdataverse.basketball.nbl_au.nbl_rosetta_runtime import REQUEST_KEY, end_year
from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_nbl_rosetta_match",
    "parse_nbl_rosetta_match_pbp",
    "parse_nbl_rosetta_match_player_box",
    "parse_nbl_rosetta_match_player_period_box",
    "parse_nbl_rosetta_match_team_box",
    "parse_nbl_rosetta_matches",
    "parse_nbl_rosetta_next_match",
    "parse_nbl_rosetta_next_matches",
    "parse_nbl_rosetta_outcomes",
    "parse_nbl_rosetta_player_game_log",
    "parse_nbl_rosetta_player_season_stats",
    "parse_nbl_rosetta_players",
    "parse_nbl_rosetta_seasons",
    "parse_nbl_rosetta_standings",
    "parse_nbl_rosetta_team_season_stats",
    "parse_nbl_rosetta_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Nested objects that are entities: their ``external_id`` becomes ``<name>_id`` (DataCore), their ``id``
#: ``rosetta_<name>_id`` (see the module docstring).
_ENTITY_KEYS = frozenset({"season", "team", "home_team", "away_team", "player", "venue", "match"})
#: Columns that are presentation or internals, not data.
_DROP = re.compile(
    r"blurhash|logo|colou?r|socials?_|ticket|broadcaster|betting|odds|override|match_bg|image|shop_url|"
    r"membership_url|nickname|user_created|user_updated|date_created|biography|(^|_)sort$|mqtt_|nexus_|"
    r"db_call|latest_play|boxscore_field|(^|_)on_court$|(^|_)match_video"
)
#: Keys whose value is a bulky list with no use in a frame (Rosetta match record ids no route accepts; per-player
#: season stats and rosters embedded in a roster row, which have their own routes).
_DROP_KEYS = frozenset(
    {
        "matches",
        "season_statistics",
        "rosters",
        "marquee_players",
        "override_seasons",
        # the blocks of a match body, each with its own parser
        "player_match_statistics",
        "team_match_statistics",
        "calculated_player_statistics",
        "calculated_team_statistics",
        "play_by_play",
    }
)
_ID_COL = re.compile(r"(^|_)(id|external_media_id)$")
#: ``action_id`` is an event number within a match, not an id key.
_ACTION_ID = "action_id"
_FLOAT_COL = re.compile(r"percentage|_average$|(^|_)minutes$|(^|_)(x|y)$|latitude|longitude|(^|_)(height|weight)$")
_INT_COL = re.compile(
    r"(^|_)(points|rebounds|assists|blocks|steals|turnovers|fouls|games|wins|losses|won|lost|played|streak|"
    r"efficiency|plus_minus|points_for|points_against|attempted|made|score|period|attendance|capacity|"
    r"total_score|action_id|season_year|season_end_year)$|^position$"
)
_NUMERIC = (pl.Int64, pl.Float64)
_DTYPE_CODES = {"i": pl.Int64, "f": pl.Float64, "b": pl.Boolean, "s": pl.String}
#: Names ``underscore`` spells badly.
_RENAME = {
    "sc_total_score_without_d_dand_td": "sc_total_score_without_dd_and_td",
    "_synthetic_from_roster": "synthetic_from_roster",
}
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


# --- helpers -----------------------------------------------------------------------------------


def _dtype_for(col: str, schema: Optional[Mapping[str, str]] = None) -> Any:
    """A column's dtype without a value to go by: the parser's returns-schema, else the name rules."""
    code = (schema or {}).get(col)
    if code:
        return _DTYPE_CODES[code]
    if col == _ACTION_ID:
        return pl.Int64
    if _ID_COL.search(col):
        return pl.String
    if _FLOAT_COL.search(col):
        return pl.Float64
    if _INT_COL.search(col):
        return pl.Int64
    return pl.String


def _minutes(expr: pl.Expr) -> pl.Expr:
    """``"MM:SS"`` (or a numeric string) -> decimal minutes; unparseable -> null."""
    clock = expr.str.extract_groups(r"^\s*(\d+):(\d{2})(?::(\d{2}))?\s*$")
    mm = clock.struct.field("1").cast(pl.Float64, strict=False)
    ss = clock.struct.field("2").cast(pl.Float64, strict=False)
    hh = clock.struct.field("3")
    # "H:MM:SS" is not a shape Rosetta sends for minutes; a 3-part value falls through to null
    return (
        pl.when(hh.is_not_null())
        .then(None)
        .otherwise(pl.when(mm.is_not_null()).then(mm + ss / 60.0).otherwise(expr.cast(pl.Float64, strict=False)))
    )


def _typed(df: pl.DataFrame) -> pl.DataFrame:
    """Ids -> String, rates and minutes -> Float64, counts -> Int64 (numbers and numeric strings alike)."""
    casts: List[pl.Expr] = []
    for col, dtype in df.schema.items():
        if dtype == pl.Null:
            casts.append(pl.col(col).cast(_dtype_for(col)))
        elif col == _ACTION_ID:
            if dtype != pl.Int64:
                casts.append(pl.col(col).cast(pl.Int64, strict=False))
        elif _ID_COL.search(col):
            if dtype != pl.String:
                expr = pl.col(col).cast(pl.Int64, strict=False) if dtype.is_float() else pl.col(col)
                casts.append(expr.cast(pl.String))
        elif col.endswith("minutes") and dtype == pl.String:
            casts.append(_minutes(pl.col(col)).alias(col))
        elif _FLOAT_COL.search(col):
            if dtype == pl.String:
                casts.append(pl.col(col).cast(pl.Float64, strict=False))
            elif dtype.is_numeric() and dtype != pl.Float64:
                casts.append(pl.col(col).cast(pl.Float64))
        elif _INT_COL.search(col) and dtype == pl.String:
            casts.append(pl.col(col).cast(pl.Int64, strict=False))
        elif col.endswith("jersey_number") and dtype != pl.String:
            casts.append(pl.col(col).cast(pl.String))
    return df.with_columns(casts) if casts else df


def _frame(rows: Sequence[Mapping[str, Any]]) -> pl.DataFrame:
    rows = [r for r in rows if r]
    if not rows:
        return pl.DataFrame()
    try:
        df = pl.from_dicts(list(rows), infer_schema_length=None, strict=False)
    except Exception:  # noqa: BLE001 -- a malformed body parses to nothing, never raises
        return pl.DataFrame()
    return _typed(df)


def _finish(df: pl.DataFrame, columns: Optional[Sequence[str]], return_as_pandas: bool, parser: str) -> Frame:
    """Apply the parser's returns-schema dtypes and the documented columns.

    An all-null column (and a whole-number rate) takes the schema dtype, an empty frame keeps the documented
    columns and a missing one is added as nulls, all typed like a populated frame.
    """
    schema = DTYPES.get(parser, {})
    if schema and df.width:
        casts = []
        for col, dtype in df.schema.items():
            want = _dtype_for(col, schema) if col in schema else dtype
            if dtype == want:
                continue
            if (
                df[col].null_count() == df.height
                or want == pl.String
                or (want in _NUMERIC and (dtype == pl.String or dtype.is_numeric()))
            ):
                casts.append(pl.col(col).cast(want, strict=False))
        df = df.with_columns(casts) if casts else df
    if columns:
        if df.width == 0:
            df = pl.DataFrame(schema={c: _dtype_for(c, schema) for c in columns})
        else:
            missing = [pl.lit(None, dtype=_dtype_for(c, schema)).alias(c) for c in columns if c not in df.columns]
            if missing:
                df = df.with_columns(missing)
            # documented columns first, in documented order (so frames stack), then any column the body added
            df = df.select([*columns, *[c for c in df.columns if c not in set(columns)]])
    return df.to_pandas() if return_as_pandas else df


def _entity(name: str, value: Mapping[str, Any]) -> Dict[str, Any]:
    """An entity object -> ``<name>_id`` (DataCore) / ``rosetta_<name>_id`` + its other fields prefixed."""
    ext, rid = value.get("external_id"), value.get("id")
    out: Dict[str, Any] = {}
    if ext is not None:
        out[f"{name}_id"] = ext
    if rid is not None and rid != ext:
        out[f"rosetta_{name}_id"] = rid
    rest = {k: v for k, v in value.items() if k not in ("id", "external_id")}
    out.update(_flatten(rest, name + "_", base=name.split("_")[-1]))
    return out


def _flatten(obj: Mapping[str, Any], prefix: str = "", base: str = "") -> Dict[str, Any]:
    """One flat row: nested objects as ``<parent>_<child>``, lists as JSON, entity ids normalised."""
    out: Dict[str, Any] = {}
    for key, value in obj.items():
        if key == REQUEST_KEY or key in _DROP_KEYS:
            continue
        leaf = underscore(str(key))
        if base and leaf.startswith(base + "_"):  # home_team.team_code -> home_team_code, not home_team_team_code
            leaf = leaf[len(base) + 1 :]
        name = prefix + leaf
        if key in _ENTITY_KEYS and isinstance(value, dict) and ("id" in value or "external_id" in value):
            out.update(_entity(name, value))
        elif key in _ENTITY_KEYS and value is None:
            continue
        elif key == "match" and isinstance(value, str):
            out["rosetta_match_id"] = value
        elif isinstance(value, dict):
            out.update(_flatten(value, name + "_", base=leaf))
        elif isinstance(value, list):
            out[name] = json.dumps(value, ensure_ascii=False)
        else:
            out[name] = value
    return out


def _own_ids(row: Mapping[str, Any], entity: Optional[str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The row's own ``id`` / ``external_id`` as ``<entity>_id`` / ``rosetta_<entity>_id``, and the rest."""
    if entity is None:
        return {}, dict(row)
    ids = _entity(entity, {"id": row.get("id"), "external_id": row.get("external_id")})
    return ids, {k: v for k, v in row.items() if k not in ("id", "external_id")}


def _rows(raw: Any) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """``(rows, request stamp)`` of an envelope, a bare ``data`` list, or the outcomes map."""
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)], {}
    if not isinstance(raw, dict):
        return [], {}
    req = _dict(raw.get(REQUEST_KEY))
    data = raw.get("data")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)], dict(req)
    if isinstance(data, dict) and data and all(isinstance(v, dict) for v in data.values()):
        return [{"team_name": k, **v} for k, v in data.items()], dict(req)
    return [], dict(req)


def _enrich(flat: Dict[str, Any], req: Mapping[str, Any]) -> Dict[str, Any]:
    """Add ``org`` and ``season_end_year`` (the sdv key) to a flat row; fold the org shortcode into ``org``."""
    org = None
    for key in [k for k in flat if k.endswith("competition_league_organisation_shortcode")]:
        org = org or flat.pop(key)
    org = org or req.get("org")
    out: Dict[str, Any] = {"org": org} if org else {}
    out.update(flat)
    year_col = next((c for c in ("season_year", "match_season_year") if out.get(c) not in (None, "")), None)
    start = next(
        (
            out[c]
            for c in ("start_date", "start_time_datetime", "start_time", "match_start_time_datetime")
            if out.get(c)
        ),
        None,
    )
    if year_col and org:
        try:
            out["season_end_year"] = end_year(str(org), int(out[year_col]), str(start) if start else None)
        except (TypeError, ValueError):
            out["season_end_year"] = None
    elif req.get("season") is not None:
        out["season_end_year"] = req["season"]
    return out


def _flat_row(
    row: Mapping[str, Any], entity: Optional[str], req: Mapping[str, Any], rename: Optional[Mapping[str, str]] = None
) -> Dict[str, Any]:
    ids, rest = _own_ids(row, entity)
    flat = {k: v for k, v in {**ids, **_flatten(rest)}.items() if not _DROP.search(k)}
    names = {**_RENAME, **(rename or {})}
    return _enrich({names.get(k, k): v for k, v in flat.items()}, req)


def _parse(
    raw: Any,
    parser: str,
    columns: Optional[Sequence[str]],
    return_as_pandas: bool,
    shape: Callable[[Any], List[Dict[str, Any]]],
) -> Frame:
    """Run ``shape`` over the body; a body it cannot read is a zero-row frame, never an exception."""
    try:
        rows = shape(raw)
    except Exception:  # noqa: BLE001 -- malformed input parses to nothing
        rows = []
    return _finish(_frame(rows), columns, return_as_pandas, parser)


def _list_shape(
    entity: Optional[str], rename: Optional[Mapping[str, str]] = None
) -> Callable[[Any], List[Dict[str, Any]]]:
    def shape(raw: Any) -> List[Dict[str, Any]]:
        rows, req = _rows(raw)
        return [_flat_row(r, entity, req, rename) for r in rows]

    return shape


# --- list routes -------------------------------------------------------------------------------


def parse_nbl_rosetta_seasons(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Seasons of an organisation (``/{org}/seasons`` and ``/{org}/seasons/current``) -> one row per season.

    ``season_id`` is the DataCore season id (it joins the Sportradar Atrium embed API's ``seasonId`` for
    NBL1), ``rosetta_season_id`` the id the leaders route is keyed on. ``season_year`` is Rosetta's year and
    ``season_end_year`` the sdv key. The embedded list of match record ids is dropped (no route accepts them).

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per season; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import parse_nbl_rosetta_seasons
            parse_nbl_rosetta_seasons({"data": [{"id": "a", "external_id": "b", "year": "2025", "start_date": "2025-09-19T00:00:00"}]})
    """
    return _parse(
        raw, "parse_nbl_rosetta_seasons", columns, return_as_pandas, _list_shape("season", {"year": "season_year"})
    )


def parse_nbl_rosetta_matches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Matches of a season year (``/{org}/matches/in/season/{season}/{type}``) -> one row per match.

    ``match_id`` is the DataCore fixture id (the key of :func:`nbl_rosetta_match`); team and venue ids follow
    the module's id rule. Scores and attendance are Int64 (the body sends text); ``start_time`` is UTC text
    (some rows carry a ``Z``). Rows are not in date order: sort on ``start_time``. Year 1998 holds two seasons;
    when the runtime stamped the season asked for, rows of the other one are dropped.

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per match; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_matches
            matches = nbl_rosetta_matches(season=2026, org="nbl", season_type="regular")
            matches.sort("start_time").select("match_id", "home_team_name", "away_team_name", "home_score")
    """

    def shape(body: Any) -> List[Dict[str, Any]]:
        rows, req = _rows(body)
        out = [_flat_row(r, "match", req) for r in rows]
        asked = req.get("season")
        if asked is not None:
            out = [r for r in out if r.get("season_end_year") in (None, asked)]
        return out

    return _parse(raw, "parse_nbl_rosetta_matches", columns, return_as_pandas, shape)


def parse_nbl_rosetta_teams(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team directory of an organisation (``/{org}/teams``) -> one row per team record.

    The directory lists current, defunct, exhibition and development sides, so a club can appear under
    several records. ``team_id`` is the DataCore id (stable across seasons), ``rosetta_team_id`` Rosetta's.

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per team record; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_teams
            nbl_rosetta_teams(org="wnbl").select("team_id", "name", "team_code")
    """
    return _parse(raw, "parse_nbl_rosetta_teams", columns, return_as_pandas, _list_shape("team"))


def parse_nbl_rosetta_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Regular-season ladder (``/{org}/standings/{season}/regular``) -> one row per team.

    ``points_percentage`` is a ratio (points for / points against: 1.1379 = 113.79 %), ``win_percentage`` a
    0-1 fraction. ``last_5`` is a W/L string, oldest first; despite its name it can hold the whole season.
    ``rosetta_standing_id`` is the id of the ladder row. NBL1 ladders are capped at 100 rows by the path cache
    unless the page was fetched with a larger limit (the runtime sends one).

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per ladder entry; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_standings
            nbl_rosetta_standings(season=2026).sort("position").select("position", "team_name", "won", "lost")
    """
    return _parse(raw, "parse_nbl_rosetta_standings", columns, return_as_pandas, _list_shape("standing"))


def parse_nbl_rosetta_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Roster rows (``/{org}/players/in/season/{season}`` and ``.../players/for/team/{team_id}/...``).

    One row per player and team in the season year. ``player_id`` is the DataCore person id (equal to the
    play-by-play ``player_id``), ``rosetta_player_id`` Rosetta's. Roster pages overlap when paged (16 of 500
    rows repeated in a probe), so the runtime fetches one large page. Date of birth, height and weight are
    what nbl.com.au publishes for each player.

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per roster entry; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_players
            nbl_rosetta_players(season=2026).select("player_id", "player_full_name", "team_name")
    """
    return _parse(raw, "parse_nbl_rosetta_players", columns, return_as_pandas, _list_shape(None))


def parse_nbl_rosetta_outcomes(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Win-loss record and form per team (``/match/outcomes/for/{org}/teams/in/season/{season}/{type}``).

    The body is a map keyed by team NAME (no id), so the key lands in ``team_name``. ``last10``, ``last5`` and
    ``last3`` are W/L strings.

    Args:
        raw: the decoded JSON envelope (or its ``data`` map).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per team name; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import parse_nbl_rosetta_outcomes
            parse_nbl_rosetta_outcomes({"data": {"Perth Wildcats": {"wins": 20, "losses": 8}}})
    """
    return _parse(raw, "parse_nbl_rosetta_outcomes", columns, return_as_pandas, _list_shape(None))


def parse_nbl_rosetta_player_game_log(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's game log for a season (``/{org}/player_boxscores/for/{player_id}/in/season/{season}/regular``).

    One row per game: the player's line (``period`` 0 is the whole game) plus the game (``match_id`` the
    DataCore fixture id, ``rosetta_match_id`` the record id, teams under ``match_home_team_*`` /
    ``match_away_team_*`` with Rosetta ids only). ``field_goals_made`` arrives as text on some rows and is
    Int64 here. ``sc_*`` are nbl.com.au's fantasy-style score components.

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per game played; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_player_game_log
            nbl_rosetta_player_game_log(player_id="7619d3ea-410d-11f0-9235-2902fc2f936f", season=2026)
    """
    return _parse(raw, "parse_nbl_rosetta_player_game_log", columns, return_as_pandas, _list_shape(None))


def parse_nbl_rosetta_player_season_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Player season totals and averages (``/{org}/statistics/for/player/{player_id}`` and the leaders routes).

    One row per player and season (and ``period``: 0 is the whole game, 1-4 quarters, 5 and up overtime).
    ``*_average`` columns are per game, ``minutes`` is total minutes and ``minutes_average`` per game, both
    decimal. ``games_started`` counts starts. The leaders routes carry the season in ``season_*``.

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per player, season and period; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_stats_leaders
            leaders = nbl_rosetta_stats_leaders(season_id="1f8e4a79-e98b-457b-85a5-e4b898c6c0bd")
            leaders.sort("points_average", descending=True).head()
    """
    return _parse(raw, "parse_nbl_rosetta_player_season_stats", columns, return_as_pandas, _list_shape(None))


def parse_nbl_rosetta_team_season_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team season totals and averages by period (``/{org}/team/stats/for/season/{season}/regular``).

    One row per team and ``period`` (0 = whole game, 1-4 quarters, 5 and up overtime). ``minutes`` are
    player-minutes (five players on court: 200 per regulation game). ``results_string`` is a W/L string.

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per team and period; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_team_stats
            nbl_rosetta_team_stats(season=2026).filter(pl.col("period") == 0)
    """
    return _parse(raw, "parse_nbl_rosetta_team_season_stats", columns, return_as_pandas, _list_shape(None))


def parse_nbl_rosetta_next_match(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The organisation's next scheduled match (``/next/match/for/{org}/{season}`` and the per-team variant).

    The per-team route ignores its team segment and returns the same body. Same columns as
    :func:`parse_nbl_rosetta_matches`, plus the venue and both teams' ids.

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row (the next match); zero rows between seasons or for a malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_next_match
            nbl_rosetta_next_match(season=2027, org="nbl")
    """
    return _parse(raw, "parse_nbl_rosetta_next_match", columns, return_as_pandas, _list_shape("match"))


def parse_nbl_rosetta_next_matches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Each team's next matches (``/next/matches/for/teams/in/{org}/{season}``) -> one row per team and match.

    The team row's ``id`` is Rosetta's (``rosetta_team_id``); ``opponent_id`` is Rosetta's too
    (``rosetta_opponent_id``); the match's ``external_id`` is the DataCore fixture id (``match_id``).

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per team and upcoming match; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_next_matches
            nbl_rosetta_next_matches(season=2027).select("team_code", "opponent_name", "start_time_datetime")
    """

    def shape(body: Any) -> List[Dict[str, Any]]:
        rows, req = _rows(body)
        out: List[Dict[str, Any]] = []
        for team in rows:
            head = {
                "rosetta_team_id": team.get("id"),
                "team_name": team.get("name"),
                "team_code": team.get("team_code"),
            }
            head["division"], head["conference"] = team.get("division"), team.get("conference")
            for nxt in team.get("nextMatches") or [{}]:
                if not isinstance(nxt, dict):
                    continue
                flat = {k: v for k, v in _flatten(nxt).items() if not _DROP.search(k)}
                flat["match_id"] = flat.pop("external_id", None)
                flat["rosetta_match_id"] = flat.pop("id", None)
                flat["rosetta_opponent_id"] = flat.pop("opponent_id", None)
                out.append(_enrich({**head, **flat}, req))
        return out

    return _parse(raw, "parse_nbl_rosetta_next_matches", columns, return_as_pandas, shape)


# --- one match ---------------------------------------------------------------------------------


def _match_of(raw: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    rows, req = _rows(raw)
    return (rows[0] if rows else {}), req


def _dict(value: Any) -> Dict[str, Any]:
    """``value`` when it is a dict, else an empty one."""
    return value if isinstance(value, dict) else {}


def _match_id(match: Mapping[str, Any]) -> Optional[str]:
    mid = match.get("external_id") or match.get("id")
    return mid if isinstance(mid, str) else None


def _block_shape(key: str, line: Callable[[Dict[str, Any]], Dict[str, Any]]) -> Callable[[Any], List[Dict[str, Any]]]:
    """Rows of one list block of the match body, each tagged with the match's id and status."""

    def shape(raw: Any) -> List[Dict[str, Any]]:
        match, _ = _match_of(raw)
        head = {"match_id": _match_id(match), "match_status": match.get("match_status")}
        return [{**head, **line(r)} for r in match.get(key) or [] if isinstance(r, dict)]

    return shape


def _stat_line(row: Dict[str, Any]) -> Dict[str, Any]:
    """A box-score line: its own ids are the line's (``stat_line_id``), not an entity's."""
    out = _entity("stat_line", {"id": row.get("id"), "external_id": row.get("external_id")})
    rest = {k: v for k, v in row.items() if k not in ("id", "external_id")}
    out.update(_flatten(rest))
    return {_RENAME.get(k, k): v for k, v in out.items() if not _DROP.search(k)}


def parse_nbl_rosetta_match(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One match's header (``/match/{match_id}`` and ``/match/{match_id}/live/all``) -> a single row.

    Status, scores, clock, round, venue, season and both teams. The box-score, play-by-play and period blocks
    of the same body have their own parsers (:func:`parse_nbl_rosetta_match_player_box`,
    :func:`parse_nbl_rosetta_match_team_box`, :func:`parse_nbl_rosetta_match_player_period_box`,
    :func:`parse_nbl_rosetta_match_pbp`); fetch once with ``return_parsed=False`` and parse as many ways as
    needed. In this body ``home_team_id`` / ``away_team_id`` are the DataCore ids. ``match_status`` is
    ``complete`` for a finished game: box scores and play-by-play of any other status are partial.
    ``external_media_id`` is the Genius Sports match id up to NBL25 (text).

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_match
            nbl_rosetta_match(match_id="b217a699-4bef-11f0-bc08-933162362355")
    """

    def shape(body: Any) -> List[Dict[str, Any]]:
        match, req = _match_of(body)
        return [_flat_row(match, "match", req)] if match else []

    return _parse(raw, "parse_nbl_rosetta_match", columns, return_as_pandas, shape)


def parse_nbl_rosetta_match_player_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One match's player box score -> one row per player line (``player_match_statistics`` of the match body).

    ``player_id`` / ``team_id`` are the DataCore ids. ``period`` 0 is the whole game. ``minutes`` is decimal
    minutes (the body sends ``"MM:SS"``; ``minutes_string`` keeps the ISO duration). A 1979-era match has
    results-only lines: 0 in every counting stat the era did not record (assists, rebounds, minutes
    ``00:00``). ``sc`` is nbl.com.au's fantasy-style score for the line.

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per player line; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_match_player_box
            box = nbl_rosetta_match_player_box(match_id="b217a699-4bef-11f0-bc08-933162362355")
            box.sort("points", descending=True).head()
    """
    return _parse(
        raw,
        "parse_nbl_rosetta_match_player_box",
        columns,
        return_as_pandas,
        _block_shape("player_match_statistics", _stat_line),
    )


def parse_nbl_rosetta_match_team_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One match's team box score by period -> one row per team and period (``team_match_statistics``).

    ``period`` 0 is the whole game, 1-4 quarters, 5 and up overtime. ``minutes`` are player-minutes.
    A results-only match has two rows (period 0 only).

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per team and period; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_match_team_box
            nbl_rosetta_match_team_box(match_id="b217a699-4bef-11f0-bc08-933162362355")
    """
    return _parse(
        raw,
        "parse_nbl_rosetta_match_team_box",
        columns,
        return_as_pandas,
        _block_shape("team_match_statistics", _stat_line),
    )


def parse_nbl_rosetta_match_player_period_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One match's player box score by period -> one row per player and period (``calculated_player_statistics``).

    Rosetta computes these from the event log, so unlike the official lines they split a game into quarters
    (``period`` 0 whole game, 1-4 quarters, 5 and up overtime). In this block ``player.id`` / ``team.id`` are
    Rosetta's (``rosetta_player_id``, ``rosetta_team_id``) and ``player_id`` / ``team_id`` the DataCore ids.
    ``minutes`` is decimal. Absent for results-only matches (1979).

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per player and period; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_match_player_period_box
            nbl_rosetta_match_player_period_box(match_id="b217a699-4bef-11f0-bc08-933162362355")
    """
    return _parse(
        raw,
        "parse_nbl_rosetta_match_player_period_box",
        columns,
        return_as_pandas,
        _block_shape(
            "calculated_player_statistics", lambda r: {k: v for k, v in _flatten(r).items() if not _DROP.search(k)}
        ),
    )


def _seconds(value: Any) -> Optional[float]:
    """``"MM:SS"`` / ``"HH:MM:SS"`` / a number -> seconds; anything else -> None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    parts = value.strip().split(":")
    try:
        nums = [float(p) for p in parts]
    except ValueError:
        return None
    if len(nums) == 1:
        return nums[0]
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    if len(nums) == 3:
        return nums[0] * 3600 + nums[1] * 60 + nums[2]
    return None


def _sub_type_key(value: Any) -> Optional[str]:
    """``"Driving Layup"`` (2025-26 feed) and ``"drivingLayup"`` (backfills) -> ``"driving_layup"``."""
    if not isinstance(value, str) or not value.strip():
        return None
    return re.sub(r"[\s\-]+", "_", _CAMEL.sub("_", value.strip())).lower()


def _event(e: Dict[str, Any], match: Mapping[str, Any]) -> Dict[str, Any]:
    player = _dict(e.get("player"))
    team = _dict(e.get("team"))
    coords = _dict(e.get("coordinates"))
    home = (match.get("home_team") or {}).get("external_id") or (match.get("home_team") or {}).get("id")
    team_id = team.get("external_id") or team.get("id") or None
    person = e.get("personId") or player.get("external_id") or None
    return {
        "match_id": e.get("fixtureId") or _match_id(match),
        "rosetta_match_id": e.get("match") if isinstance(e.get("match"), str) else None,
        "match_status": match.get("match_status"),
        "action_id": e.get("action_id"),
        "period": e.get("period"),
        "period_type": e.get("period_type"),
        "clock": e.get("clock"),
        "clock_seconds": _seconds(e.get("clock")),
        "shot_clock": e.get("shot_clock"),
        "shot_clock_seconds": _seconds(e.get("shot_clock")),
        "score_home": e.get("score_1"),
        "score_away": e.get("score_2"),
        "action_type": e.get("action_type"),
        "sub_type": e.get("sub_type"),
        "sub_type_key": _sub_type_key(e.get("sub_type")),
        "success": e.get("success") if isinstance(e.get("success"), bool) else None,
        "team_id": team_id,
        "team_code": team.get("team_code"),
        "team_name": team.get("name"),
        "is_home": (team_id == home) if team_id and home else None,
        "player_id": person,
        "player_name": player.get("full_name") or None,
        "jersey_number": None if player.get("jersey_number") is None else str(player["jersey_number"]),
        "x": coords.get("x"),
        "y": coords.get("y"),
        "readable_action_type": e.get("readable_action_type"),
        "system_action_type": e.get("system_action_type"),
        "timestamp": e.get("timestamp"),
        "feed": "datacore" if e.get("fixtureId") else "backfill",
    }


def parse_nbl_rosetta_match_pbp(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One match's play-by-play -> one row per event, oldest first (``play_by_play`` of the match body).

    This is the NBL 2025-26 play-by-play that ESPN lacks (ESPN's ``nbl`` feed has 0 plays for the season;
    Rosetta has the full log, with shot x/y), and it reaches back to 2011-12 for the NBL and about 2014-15 for
    the WNBL. A results-only match (1979) has no play-by-play: zero rows.

    **Two feed shapes**, told apart by ``feed``: ``datacore`` (NBL and WNBL 2025-26: events carry
    ``fixtureId``, arrive newest first, Title Case ``sub_type`` such as "Driving Layup", a real shot clock)
    and ``backfill`` (older seasons and NBL1 2025: no ``fixtureId``, oldest first, camelCase, and a shot clock
    that repeats the game clock; ``success`` is true on every non-shot event). ``sub_type_key`` folds both
    spellings to snake_case; rows are sorted on ``action_id``.

    ``x`` / ``y`` are percent of the court, 0-100, x along the length and y across the width, in game
    orientation (each team's shots sit at x > 50 for two periods and x < 50 for the others). Unverified: which
    end is x = 0, which sideline is y = 0, and what the coordinates of DataCore-era free-throw, foul and
    turnover events mean. 2011-12 has no coordinates; the Genius-era backfills have them on shots only.
    ``score_home`` / ``score_away`` are the running score after the event. ``player_id`` / ``team_id`` are
    DataCore ids (null for events no player owns: rebounds by the team, period starts, timeouts).
    ``is_home`` is null when the team is unknown.

    Args:
        raw: the decoded JSON envelope (or its ``data`` list).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring).

    Returns:
        One row per event, sorted on ``action_id``; zero rows for an empty or malformed body.

    Raises:
        This parser never raises: a malformed body parses to a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.basketball.nbl_au import nbl_rosetta_match_pbp
            pbp = nbl_rosetta_match_pbp(match_id="b217a699-4bef-11f0-bc08-933162362355")
            pbp.filter(pl.col("x").is_not_null()).select("period", "clock", "player_name", "success", "x", "y")
    """

    def shape(body: Any) -> List[Dict[str, Any]]:
        match, _ = _match_of(body)
        events = [_event(e, match) for e in match.get("play_by_play") or [] if isinstance(e, dict)]
        return sorted(events, key=lambda r: (r["action_id"] is None, r["action_id"] or 0))

    return _parse(raw, "parse_nbl_rosetta_match_pbp", columns, return_as_pandas, shape)
