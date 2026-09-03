"""Parsers for the generated ``mls_api`` wrappers (official MLS web API, Sportec).

Three auth-free MLS-owned hosts back ``mlssoccer.com`` and this module covers all
of them: ``stats-api`` (competitions / seasons / standings / schedule / match
detail / clubs), ``sportapi`` (match + club batch lookups, club rosters) and
``dapi`` (Contentful season content). They share no envelope convention, so there
are four parsers rather than one:

* :func:`parse_mls_api` -- list bodies and single-rows-key envelopes
  (``{"competitions": [...]}``, ``{"seasons": [...]}``, ``{"schedule": [...]}``,
  ``{"items": [...]}``).
* :func:`parse_mls_entity` -- a body that *is* one record (a club, a sportapi
  match, a season content entity), where the generic rows-key search would
  wrongly latch onto an incidental nested array such as ``broadcasters``.
* :func:`parse_mls_standings` -- ``{"tables": [{..., "entries": [...]}]}``.
* :func:`parse_mls_match` -- the stats-api match detail, a genuinely multi-table
  payload (match info, environment, both team blocks, lineups, staff, referees,
  recent meetings).

**Spec-vs-capture note.** The committed OpenAPI spec declares bare arrays for
``/competitions``, ``/competitions/{id}/seasons``, ``/matches/seasons/{seasonId}``
and the standings route; the committed captures show all four wrapped in an
envelope (``competitions`` / ``seasons`` / ``schedule`` / ``tables``). The
captures are ground truth and these parsers follow them, while still accepting a
bare array should the spec's shape ever appear.

Ids are opaque Sportec strings (``MLS-COM/SEA/MAT/CLU/OBJ-*``) and are pinned to
``Utf8``; the parallel ``optaId`` integer namespace is routed through ``Int64``
before stringifying so it can never become ``"123.0"``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.soccer._frames import as_output, as_tables, rows_to_frame

__all__ = [
    "parse_mls_api",
    "parse_mls_entity",
    "parse_mls_match",
    "parse_mls_standings",
]

# Envelope keys that carry metadata rather than rows -- never treated as the rows
# array by the generic parser.
_META_KEYS = frozenset({"meta", "pagination", "next_page_token"})

# The stats-api match-detail sub-blocks, in the order they are returned.
_MATCH_TABLES = ("match_information", "environment", "teams", "players", "staff", "referees", "last_matches")


def _rows_key(raw: Dict[str, Any]) -> Union[List[Any], None]:
    """First non-metadata list-of-dicts value in an envelope, else ``None``."""
    for key, value in raw.items():
        if key in _META_KEYS or not isinstance(value, list) or not value:
            continue
        if all(isinstance(v, dict) for v in value):
            return value
    return None


def parse_mls_api(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a list-shaped MLS payload (bare array or single-rows-key envelope).

    Used by ``mls_competitions``, ``mls_competition_seasons``,
    ``mls_season_matches``, ``mls_content_seasons`` and the three ``sportapi``
    batch/roster routes.

    Args:
        raw: an MLS JSON body -- a top-level list, or an envelope whose one
            non-metadata array holds the rows.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record, snake_cased and ``json_normalize``-flattened, with
        every ``*_id`` column pinned to ``Utf8``. A zero-row frame when the
        payload is ``None``, empty or malformed.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.mls import mls_competitions

            df = mls_competitions()
            print(df.select("competition_id", "competition_name").head())

        Pipeline next step (one line)::

            df.filter(pl.col("competition_type") == "League").head()

    See Also:
        * `MLS Season Pass`_ -- the public site these hosts render.

    .. _MLS Season Pass: https://www.mlssoccer.com/
    """
    if isinstance(raw, list):
        rows: List[Any] = raw
    elif isinstance(raw, dict) and raw:
        found = _rows_key(raw)
        rows = found if found is not None else [raw]
    else:
        rows = []
    return as_output(rows_to_frame(rows), return_as_pandas=return_as_pandas)


def parse_mls_entity(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a single-record MLS payload into a one-row frame.

    Used by ``mls_club``, ``mls_sportapi_match`` and ``mls_content_season``. These
    bodies are one entity whose nested arrays (``broadcasters``, ``tags``,
    ``references``) are attributes, not rows -- so the generic rows-key search of
    :func:`parse_mls_api` would return the wrong table.

    Args:
        raw: an MLS JSON body that represents one record. A list is accepted and
            parsed as-is, so a batch route can share this parser.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A single row (or one row per element for a list body) with nested objects
        flattened to dotted, snake_cased columns and nested arrays JSON-encoded.
        A zero-row frame when the payload is ``None``, empty or malformed.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.mls import mls_club

            df = mls_club(club_id="MLS-CLU-000001")
            print(df.select("club_id", "club_name", "stadium_name"))

        Pipeline next step (one line)::

            df.select(pl.col("club_id").cast(pl.Utf8), "city", "country")

    See Also:
        * `MLS Season Pass`_ -- the public site these hosts render.

    .. _MLS Season Pass: https://www.mlssoccer.com/
    """
    if isinstance(raw, list):
        rows: List[Any] = raw
    elif isinstance(raw, dict) and raw:
        rows = [raw]
    else:
        rows = []
    return as_output(rows_to_frame(rows), return_as_pandas=return_as_pandas)


def parse_mls_standings(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Dict[str, Union[pl.DataFrame, pd.DataFrame]]:
    """Parse an MLS standings payload into table metadata + ranked entries.

    The body is ``{"tables": [{category, group, season_id, ..., entries: [...]}]}``
    -- one table per conference/split. Both levels are useful, so both are
    returned instead of collapsing them.

    Args:
        raw: a stats-api standings JSON body.
        return_as_pandas: return pandas DataFrames instead of polars.

    Returns:
        Two sub-frames, both present even when the payload is empty:

        * ``"tables"`` -- one row per standings table (``category``, ``group``,
          ``type``, ``time_scope``, ``competition_id``, ``season_id``,
          ``match_day``), with the nested ``entries`` dropped.
        * ``"entries"`` -- one row per ranked club, carrying the parent table's
          ``competition_id`` / ``season_id`` / ``group`` / ``category`` plus
          ``position``, ``team_id``, record and per-game rate columns.

    Raises:
        None: malformed payloads yield zero-row frames rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.mls import mls_standings

            tables = mls_standings(competition_id="MLS-COM-000001", season_id="MLS-SEA-0001KA")
            print(tables["entries"].select("group", "position", "team", "points").head())

        Pipeline next step (one line)::

            tables["entries"].filter(pl.col("group") == "Eastern Conference").head()

    See Also:
        * `MLS Season Pass`_ -- the public site these hosts render.

    .. _MLS Season Pass: https://www.mlssoccer.com/
    """
    if isinstance(raw, dict):
        raw_tables = raw.get("tables")
    elif isinstance(raw, list):
        raw_tables = raw
    else:
        raw_tables = None
    tables = [t for t in (raw_tables or []) if isinstance(t, dict)]
    meta = [{k: v for k, v in t.items() if k != "entries"} for t in tables]
    entries: List[Dict[str, Any]] = []
    for table in tables:
        keys = {k: table.get(k) for k in ("competition_id", "season_id", "group", "category", "type") if k in table}
        for entry in table.get("entries") or []:
            if isinstance(entry, dict):
                entries.append({**keys, **entry})
    out = {"tables": rows_to_frame(meta), "entries": rows_to_frame(entries)}
    return as_tables(out, return_as_pandas=return_as_pandas)


def _match_side_rows(match: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Split the ``home`` / ``away`` team blocks into team, lineup and staff rows."""
    teams: List[Dict[str, Any]] = []
    players: List[Dict[str, Any]] = []
    staff: List[Dict[str, Any]] = []
    for side in ("home", "away"):
        block = match.get(side)
        if not isinstance(block, dict):
            continue
        scalars = {k: v for k, v in block.items() if not isinstance(v, (list, dict))}
        teams.append({"side": side, **scalars})
        keys = {"side": side, "team_id": block.get("team_id"), "team_name": block.get("team_name")}
        for person in block.get("players") or []:
            if isinstance(person, dict):
                players.append({**keys, **person})
        for group in ("trainer_staff", "official_staff"):
            for person in block.get(group) or []:
                if isinstance(person, dict):
                    staff.append({**keys, "staff_group": group, **person})
    return {"teams": teams, "players": players, "staff": staff}


def parse_mls_match(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Dict[str, Union[pl.DataFrame, pd.DataFrame]]:
    """Parse a stats-api match-detail payload into its seven sub-tables.

    The body is ``{match_information, environment, home, away, referees,
    last_matches}``. ``home`` / ``away`` each mix team metadata with a lineup
    (``players``) and two staff arrays, so this parser splits them by role rather
    than flattening the whole match into an unusable single row.

    Note the route answers 200 only for **played** matches -- an unplayed match id
    returns 404, which reaches this parser as an empty body and therefore as
    zero-row frames.

    Args:
        raw: a stats-api ``/matches/{matchId}`` JSON body.
        return_as_pandas: return pandas DataFrames instead of polars.

    Returns:
        Seven sub-frames, all present even when the payload is empty:
        ``"match_information"`` (one row: competition, season, kickoff, result,
        period timings), ``"environment"`` (one row: stadium, weather, attendance),
        ``"teams"`` (two rows, one per ``side``: formation, kit colours, team ids),
        ``"players"`` (lineup rows with ``side``, ``person_id``,
        ``playing_position``, ``shirt_number``, ``starting``), ``"staff"``
        (coaching + official staff with a ``staff_group`` column), ``"referees"``
        (one row per official with ``role``), and ``"last_matches"`` (recent
        head-to-head meetings).

    Raises:
        None: malformed payloads yield zero-row frames rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.mls import mls_match

            tables = mls_match(match_id="MLS-MAT-0009H8")
            print(tables["players"].select("side", "person_id", "playing_position").head())

        Pipeline next step (one line)::

            tables["players"].filter(pl.col("starting") == "true").group_by("side").len()

    See Also:
        * `MLS Season Pass`_ -- the public site these hosts render.

    .. _MLS Season Pass: https://www.mlssoccer.com/
    """
    match = raw if isinstance(raw, dict) else {}
    sides = _match_side_rows(match)
    blocks: Dict[str, List[Any]] = {
        "match_information": [match["match_information"]] if isinstance(match.get("match_information"), dict) else [],
        "environment": [match["environment"]] if isinstance(match.get("environment"), dict) else [],
        "teams": sides["teams"],
        "players": sides["players"],
        "staff": sides["staff"],
        "referees": [r for r in (match.get("referees") or []) if isinstance(r, dict)],
        "last_matches": [m for m in (match.get("last_matches") or []) if isinstance(m, dict)],
    }
    out = {name: rows_to_frame(blocks[name]) for name in _MATCH_TABLES}
    return as_tables(out, return_as_pandas=return_as_pandas)
