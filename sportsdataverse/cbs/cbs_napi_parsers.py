"""Parsers for the generated ``cbs_napi`` wrappers (``api.cbssports.com/napi``).

NAPI is CBS Sports' auth-free, cross-sport REST surface (17 leagues: NFL, MLB,
NBA, NHL, NCAAF, NCAAB-M/W, MLS + the soccer/golf ids). It self-documents at
``/resource/endpoint/registry`` and serves every resource through one of a small
number of envelope shapes, so **one generic parser covers 81 of the 82 endpoints**
and a second handles the one genuinely different payload (team standings, which
is a ``{year: {season_type: {...}}}`` map rather than a record list).

Envelope shapes actually observed in the committed captures
(``sdv-internal-refs/cbs/captures/``):

* ``{"data": [...]}`` -- ``/resource/season/teams/{seasonId}``,
  ``/resource/team/players/{teamId}``: rows are ``data``.
* plain object -- ``/resource/league/{leagueId}``: one row.
* dict-of-dicts -- ``/resource/endpoint/registry``: one row per key, the key
  surfaced in a ``key`` column.
* ``{year: {season_type: {...}}}`` -- ``/resource/team/standings/{teamId}``:
  handled by :func:`parse_cbs_napi_standings`.
* ``{"plays": [...]}`` / ``{"drives": [...]}`` --
  ``/resource/game/scoring/plays/{gameId}`` and ``/scoring/drives/{gameId}``:
  handled by :func:`parse_cbs_napi_scoring_plays` and
  :func:`parse_cbs_napi_scoring_drives` (one row per play / drive).
* ``{"error"|"errors"|"warnings": ...}`` -- NAPI answers an invalid id with
  **HTTP 200** plus this envelope and no ``data``; it parses to a zero-row frame.

The parsers follow the package-wide parser contract: polars by default, pandas
via ``return_as_pandas=True``, a zero-row frame (never an exception) on empty /
malformed payloads, snake-cased columns via
:func:`sportsdataverse.dl_utils.underscore`, ``pandas.json_normalize`` for nested
flattening, and list/dict-valued cells stringified so polars accepts the frame.

**ID discipline.** CBS ships ``playerId`` / ``teamId`` / ``leagueId`` /
``seasonId`` as JSON integers, but several id columns are nullable
(``conferenceId``, ``divisionId``, ``homeVenueId``, ``stubHubTeamId``), and
pandas widens int-with-null to ``float64``. Left alone that yields ``123.0``
ids -- the exact float->string id corruption the repo bans. Every all-integral
``Float64`` column is therefore cast back to ``Int64`` before the frame is
returned, so an id never round-trips through a float. An id column CBS leaves
entirely empty for a given league (``stubHubTeamId`` outside the US leagues)
would likewise arrive as ``String`` and refuse to concat with the same column's
``Int64`` in another league, so all-null columns are typed ``Null``.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_cbs_napi",
    "parse_cbs_napi_scoring_drives",
    "parse_cbs_napi_scoring_plays",
    "parse_cbs_napi_standings",
]

DataFrameT = Union[pl.DataFrame, pd.DataFrame]

# Keys NAPI uses for its HTTP-200 failure envelope (no ``data`` alongside them).
_ERROR_KEYS = ("error", "errors", "warnings")


def _dedupe(names: Iterable[str]) -> List[str]:
    """snake_case column names with a ``_2``/``_3`` suffix on collisions.

    ``json_normalize`` can produce two source keys that snake-case to the same
    name (``team-code_id`` vs ``teamCode_id``); pandas tolerates duplicate
    columns but polars rejects the frame.
    """
    seen: Dict[str, int] = {}
    out: List[str] = []
    for raw in names:
        name = underscore(str(raw))
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        out.append(name)
    return out


def _stringify(value: Any) -> Any:
    """JSON-encode list/dict cells and str() other non-string scalars.

    Only ever applied to pandas ``object`` columns, i.e. columns that already
    hold mixed types -- polars refuses those otherwise. Homogeneous numeric
    columns keep their numeric dtype.
    """
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    if value is None or isinstance(value, str):
        return value
    return str(value)


def _canonicalize_dtypes(df: pl.DataFrame) -> pl.DataFrame:
    """Pin one honest dtype per column at the parser boundary.

    Two pandas artifacts would otherwise corrupt ids:

    * pandas widens an integer column to ``float64`` the moment one value is
      ``None``, which is the normal case for CBS's nullable id columns
      (``conferenceId``, ``divisionId``, ``homeVenueId``, ``stubHubTeamId``).
      Every all-integral ``Float64`` column is cast back to ``Int64`` so
      ``teamId`` stays ``415``, never ``415.0``. Genuinely fractional columns
      (rates, points-per-game) fail the integral test and stay floats.
    * an **all-null** object column arrives as ``String``, so the same id column
      is ``String`` for a league where CBS never populates it and ``Int64``
      where it does -- which makes a vertical concat of two leagues' frames
      raise (or, worse, a join match nothing). All-null columns are typed
      ``Null`` instead, which polars resolves against any supertype.
    """
    for name, dtype in df.schema.items():
        col = df[name]
        if col.null_count() == col.len():
            if dtype != pl.Null:
                df = df.with_columns(col.cast(pl.Null).alias(name))
            continue
        if dtype != pl.Float64:
            continue
        non_null = col.drop_nulls()
        if (non_null == non_null.round(0)).all():
            df = df.with_columns(col.cast(pl.Int64).alias(name))
    return df


def _rows_to_frame(rows: List[Any]) -> pl.DataFrame:
    """Flatten a list of NAPI records into a tidy polars frame."""
    if not rows:
        return pl.DataFrame()
    if not any(isinstance(r, dict) for r in rows):
        # a bare scalar array (e.g. a list of ids) has no keys to flatten
        return pl.DataFrame({"value": [str(r) for r in rows]})
    dict_rows = [r if isinstance(r, dict) else {"value": r} for r in rows]
    frame = pd.json_normalize(dict_rows, sep="_")
    frame.columns = _dedupe(frame.columns)
    for col in frame.columns:
        if frame[col].dtype == object:
            frame[col] = frame[col].map(_stringify)
    return _canonicalize_dtypes(pl.from_pandas(frame))


def _envelope_rows(raw: Any) -> List[Any]:
    """Resolve any NAPI envelope to the record list the frame is built from."""
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict) or not raw:
        return []
    if "data" in raw:
        data = raw["data"]
        if isinstance(data, list):
            return data
        return [data] if isinstance(data, dict) and data else []
    if any(k in raw for k in _ERROR_KEYS):
        return []
    values = list(raw.values())
    if values and all(isinstance(v, dict) for v in values):
        # keyed collection (``/resource/endpoint/registry``): one row per key
        return [{"key": key, **value} for key, value in raw.items()]
    return [raw]


def parse_cbs_napi(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> DataFrameT:
    """Parse any CBS NAPI resource payload into a tidy frame.

    Handles every envelope NAPI serves: a ``{"data": [...]}`` record list, a
    ``{"data": {...}}`` single resource, a bare list, a plain object, a
    dict-of-dicts keyed collection (one row per key, the key kept in a ``key``
    column), and the HTTP-200 ``{"error"|"errors"|"warnings": ...}`` failure
    envelope NAPI returns for an unknown id.

    Args:
        raw: a NAPI JSON body as returned by
            :func:`sportsdataverse._codegen_runtime._get`.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record, with snake-cased ``json_normalize``-flattened
        columns (list/dict-valued cells JSON-encoded, all-integral float
        columns restored to ``Int64`` so ids never render as ``123.0``). A
        zero-row frame when the payload is ``None`` / empty / an error
        envelope, so callers can chain without a null-check.

    Raises:
        No exception is raised for empty or malformed payloads.

    Example:
        Quick start::

            from sportsdataverse.cbs import cbs_team_players
            df = cbs_team_players(team_id=404)
            print(df.shape)

        Raw payload plus an explicit parse::

            from sportsdataverse.cbs import cbs_season_teams, parse_cbs_napi
            raw = cbs_season_teams(season_id=59, return_parsed=False)
            df = parse_cbs_napi(raw, return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("league_id") == 59).head()

    See Also:
        * `nflfastR`_ -- NFL play-by-play in R.
        * `hoopR`_ -- men's basketball data in R.

    .. _nflfastR: https://www.nflfastr.com
    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    frame = _rows_to_frame(_envelope_rows(raw))
    return frame.to_pandas() if return_as_pandas else frame


def parse_cbs_napi_standings(
    raw: Union[Dict[str, Any], None],
    *,
    return_as_pandas: bool = False,
) -> DataFrameT:
    """Parse a CBS NAPI team-standings payload (``/resource/team/standings/{teamId}``).

    Standings are the one NAPI resource that is not a record list: the body is a
    ``{year: {season_type: {stat: {...}}}}`` map (season type is ``pre`` /
    ``regular`` / ``post``). This flattens it to one row per
    ``(season_year, season_type)`` with the stat blocks normalized into columns
    (``wins_number``, ``winning_percentage_percentage``, ...). The stat keys are
    sport-shaped -- an NHL body carries ``goals_for_goals``, an NFL body
    ``points_for_number`` -- so the column set varies by league.

    Args:
        raw: a ``/resource/team/standings/{teamId}`` JSON body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per season/season-type, with ``season_year`` (``Int64`` when the
        payload's year keys are numeric) and ``season_type`` leading the
        snake-cased stat columns. Zero-row frame on an empty / malformed / error
        payload.

    Raises:
        No exception is raised for empty or malformed payloads.

    Example:
        Quick start::

            from sportsdataverse.cbs import cbs_team_standings
            df = cbs_team_standings(team_id=404)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("season_type") == "regular").sort("season_year").head()

    See Also:
        * `nflfastR`_ -- NFL play-by-play in R.
        * `hoopR`_ -- men's basketball data in R.

    .. _nflfastR: https://www.nflfastr.com
    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    if not isinstance(raw, dict) or not raw or any(k in raw for k in _ERROR_KEYS):
        frame = pl.DataFrame()
        return frame.to_pandas() if return_as_pandas else frame
    numeric_years = all(str(year).isdigit() for year in raw)
    rows: List[Dict[str, Any]] = []
    for year, by_type in raw.items():
        if not isinstance(by_type, dict):
            continue
        for season_type, block in by_type.items():
            if not isinstance(block, dict):
                continue
            rows.append(
                {
                    "season_year": int(year) if numeric_years else str(year),
                    "season_type": str(season_type),
                    **block,
                },
            )
    frame = _rows_to_frame(rows)
    return frame.to_pandas() if return_as_pandas else frame


# Column order is pinned (CBS key order is not stable across games) so frames
# from different games stack with a plain vertical concat.
_PLAY_COLUMNS = (
    "id",
    "game_id",
    "drive_id",
    "quarter",
    "time_remaining",
    "down",
    "distance",
    "side",
    "yardline",
    "team_in_possession",
    "description",
    "medium",
    "short",
    "score_on_play",
    "score_type",
    "short_score",
    "under_review",
    "home_timeouts_remaining",
    "away_timeouts_remaining",
    "real_clock",
    "subplays",
)
_DRIVE_COLUMNS = (
    "id",
    "team_id",
    "quarter",
    "starting_time",
    "ending_time",
    "time_of_possession",
    "starting_yardline",
    "ending_yardline",
    "starting_play_id",
    "ending_play_id",
    "drive_plays",
    "yards_on_drive",
    "drive_yards_total",
    "penalty_yards",
    "first_downs_on_drive",
    "inside_the_20",
    "score_on_drive",
    "result",
)
# CBS ships every scoring-feed value as a JSON string. These columns always hold
# integers (ids, counts, yard lines), so they are pinned to Int64 regardless of
# the game -- matching the Int64 teamId/playerId of the other NAPI resources.
# ``distance`` is NOT here: CBS sends the literal "Goal" on goal-to-go downs.
_PLAY_INT_COLUMNS = (
    "id",
    "game_id",
    "drive_id",
    "quarter",
    "down",
    "yardline",
    "team_in_possession",
    "home_timeouts_remaining",
    "away_timeouts_remaining",
)
_DRIVE_INT_COLUMNS = (
    "id",
    "team_id",
    "quarter",
    "drive_plays",
    "starting_play_id",
    "ending_play_id",
    "yards_on_drive",
    "drive_yards_total",
    "penalty_yards",
    "first_downs_on_drive",
)
# "Yes"/"No" flags.
_PLAY_BOOL_COLUMNS = ("score_on_play", "under_review")
_DRIVE_BOOL_COLUMNS = ("score_on_drive", "inside_the_20")
# Dtype of ``subplays`` when a body carries none: the two fields every sub-event
# has. A populated frame's struct has more fields; ``vertical_relaxed`` unions them.
_SUBPLAYS_DTYPE = pl.List(pl.Struct({"type": pl.String, "order": pl.String}))


def _flat_subplays(subplays: Any) -> List[Dict[str, Any]]:
    """``{"subplay": [{"type", "order", "<event>": {...}}]}`` -> flat event dicts.

    Each sub-event's type-named block (``kickoff``, ``complete_pass``, ...) is
    merged into the event, so the column holds one struct shape across types.
    """
    events = subplays.get("subplay") if isinstance(subplays, dict) else subplays
    if isinstance(events, dict):  # a lone sub-event may arrive unwrapped
        events = [events]
    out: List[Dict[str, Any]] = []
    for event in events if isinstance(events, list) else []:
        if not isinstance(event, dict):
            continue
        flat: Dict[str, Any] = {}
        for key, value in event.items():
            if isinstance(value, dict):
                flat.update(value)
            else:
                flat[key] = value
        out.append(flat)
    return out


def _scoring_frame(
    raw: Any,
    key: str,
    order: Iterable[str],
    int_columns: Iterable[str],
    bool_columns: Iterable[str],
    return_as_pandas: bool,
) -> DataFrameT:
    # The documented columns are always present (typed null when CBS omits one,
    # as older games do) so frames from any games -- or none -- stack.
    schema: Dict[str, Any] = {
        c: (
            pl.Int64
            if c in int_columns
            else pl.Boolean
            if c in bool_columns
            else _SUBPLAYS_DTYPE
            if c == "subplays"
            else pl.String
        )
        for c in order
    }
    rows = raw.get(key) if isinstance(raw, dict) else None
    records = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
    if not records:
        empty = pl.DataFrame(schema=schema)
        return empty.to_pandas() if return_as_pandas else empty
    if key == "plays":
        records = [{**r, "subplays": _flat_subplays(r.get("subplays"))} for r in records]
    frame = pl.DataFrame(records, infer_schema_length=None)
    frame = frame.with_columns(
        *[pl.col(c).cast(pl.Int64, strict=False) for c in int_columns if c in frame.columns],
        *[pl.col(c).cast(pl.String) == "Yes" for c in bool_columns if c in frame.columns],
        *[pl.lit(None, dtype=dtype).alias(c) for c, dtype in schema.items() if c not in frame.columns],
    )
    frame = frame.select([*schema, *[c for c in frame.columns if c not in schema]])
    return frame.to_pandas() if return_as_pandas else frame


def parse_cbs_napi_scoring_plays(
    raw: Union[Dict[str, Any], None],
    *,
    return_as_pandas: bool = False,
) -> DataFrameT:
    """Parse a CBS NAPI play-by-play body (``/resource/game/scoring/plays/{gameId}``).

    The body is ``{"plays": [...]}``, one record per play with every value sent
    as a string. This returns one row per play in feed order. Id, count and
    yard-line columns are cast to ``Int64``, the ``"Yes"``/``"No"`` flags to
    ``Boolean``, and each play's ``subplays`` (kickoff + return, pass + fumble
    return, penalty, ...) is kept as a list of structs, the event block merged
    into each sub-event. The sub-event struct carries only the fields that game
    used, so stack several games with ``pl.concat(..., how="vertical_relaxed")``.
    NFL play ``id`` values are GSIS play ids; NCAAF ids are CBS epoch-style stamps.

    Args:
        raw: a ``/resource/game/scoring/plays/{gameId}`` JSON body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per play. Zero rows (same columns and dtypes) when the payload
        is ``None`` / empty / malformed or NAPI's HTTP-200
        ``{"errors"|"warnings": ...}`` not-found envelope. Columns CBS omits
        for older games (``game_id``, ``real_clock``, the timeout columns) are
        present and null.

        | Column | Type | Description |
        |---|---|---|
        | ``id`` | Int64 | CBS play id (GSIS play id for NFL). |
        | ``game_id`` | Int64 | CBS game id. |
        | ``drive_id`` | Int64 | Drive number; joins ``id`` of :func:`parse_cbs_napi_scoring_drives`. |
        | ``quarter`` | Int64 | Period of the snap. |
        | ``time_remaining`` | String | Game clock at the snap (``"15:00"``). |
        | ``down`` | Int64 | Down (``0`` on kickoffs and tries). |
        | ``distance`` | String | Yards to go, or ``"Goal"`` on goal-to-go. |
        | ``side`` | String | Team abbreviation for the ``yardline`` side of the field. |
        | ``yardline`` | Int64 | Yard line on ``side``. |
        | ``team_in_possession`` | Int64 | CBS team id with the ball at the snap. |
        | ``description`` | String | Full play text. |
        | ``medium`` | String | Medium-length play summary. |
        | ``short`` | String | Short play summary. |
        | ``score_on_play`` | Boolean | Whether the play scored. |
        | ``score_type`` | String | Scoring type (``Touchdown``, ``FieldGoal``, ...) or null. |
        | ``short_score`` | String | Short scoring summary (empty when no score). |
        | ``under_review`` | Boolean | Whether the play was under review. |
        | ``home_timeouts_remaining`` | Int64 | Home timeouts left after the play. |
        | ``away_timeouts_remaining`` | Int64 | Away timeouts left after the play. |
        | ``real_clock`` | String | UTC wall-clock time of the play (ISO 8601). |
        | ``subplays`` | List(Struct) | Sub-events: ``type``, ``order``, player ids/names, yards. |

    Raises:
        No exception is raised for empty or malformed payloads.

    Example:
        Quick start::

            from sportsdataverse.cbs import cbs_game_scoring_plays
            plays = cbs_game_scoring_plays(game_id=50029216)
            print(plays.shape)

        Explicit parse of a raw body::

            from sportsdataverse.cbs import cbs_game_scoring_plays, parse_cbs_napi_scoring_plays
            raw = cbs_game_scoring_plays(game_id=50029216, return_parsed=False)
            plays = parse_cbs_napi_scoring_plays(raw, return_as_pandas=True)

        Pipeline next step (one line)::

            plays.select("id", "subplays").explode("subplays").unnest("subplays").select("id", "type", "yards_on_play")

    See Also:
        * `nflfastR`_ -- NFL play-by-play in R.
        * `cfbfastR`_ -- college football play-by-play in R.

    .. _nflfastR: https://www.nflfastr.com
    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    return _scoring_frame(raw, "plays", _PLAY_COLUMNS, _PLAY_INT_COLUMNS, _PLAY_BOOL_COLUMNS, return_as_pandas)


def parse_cbs_napi_scoring_drives(
    raw: Union[Dict[str, Any], None],
    *,
    return_as_pandas: bool = False,
) -> DataFrameT:
    """Parse a CBS NAPI drive-chart body (``/resource/game/scoring/drives/{gameId}``).

    The body is ``{"drives": [...]}`` with every value sent as a string. This
    returns one row per drive in feed order, with id, count and yardage columns
    cast to ``Int64`` and the ``"Yes"``/``"No"`` flags to ``Boolean``.

    Args:
        raw: a ``/resource/game/scoring/drives/{gameId}`` JSON body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per drive. Zero rows (same columns and dtypes) when the payload
        is ``None`` / empty / malformed or an ``{"errors"|"warnings": ...}``
        envelope.

        | Column | Type | Description |
        |---|---|---|
        | ``id`` | Int64 | Drive number; joins ``drive_id`` of :func:`parse_cbs_napi_scoring_plays`. |
        | ``team_id`` | Int64 | CBS team id of the offense. |
        | ``quarter`` | Int64 | Period the drive started in. |
        | ``starting_time`` | String | Game clock at the drive's first snap. |
        | ``ending_time`` | String | Game clock at the drive's last play. |
        | ``time_of_possession`` | String | Drive duration (``"m:ss"``). |
        | ``starting_yardline`` | String | Start field position (``"TEXAS 22"``). |
        | ``ending_yardline`` | String | End field position. |
        | ``starting_play_id`` | Int64 | CBS play id of the first play. |
        | ``ending_play_id`` | Int64 | CBS play id of the last play. |
        | ``drive_plays`` | Int64 | Plays in the drive. |
        | ``yards_on_drive`` | Int64 | Net yards gained. |
        | ``drive_yards_total`` | Int64 | Total drive yards as CBS reports them. |
        | ``penalty_yards`` | Int64 | Penalty yards on the drive. |
        | ``first_downs_on_drive`` | Int64 | First downs gained. |
        | ``inside_the_20`` | Boolean | Whether the drive reached the red zone. |
        | ``score_on_drive`` | Boolean | Whether the drive scored. |
        | ``result`` | String | Drive result (``Punt``, ``Touchdown``, ``Field Goal``, ...). |

    Raises:
        No exception is raised for empty or malformed payloads.

    Example:
        Quick start::

            from sportsdataverse.cbs import cbs_game_scoring_drives
            drives = cbs_game_scoring_drives(game_id=50029216)
            print(drives.shape)

        Pipeline next step (one line)::

            drives.group_by("team_id").agg(pl.col("score_on_drive").sum())

    See Also:
        * `nflfastR`_ -- NFL play-by-play in R.
        * `cfbfastR`_ -- college football play-by-play in R.

    .. _nflfastR: https://www.nflfastr.com
    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    return _scoring_frame(raw, "drives", _DRIVE_COLUMNS, _DRIVE_INT_COLUMNS, _DRIVE_BOOL_COLUMNS, return_as_pandas)
