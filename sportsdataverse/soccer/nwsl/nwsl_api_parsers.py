"""Parsers for the generated ``nwsl_api`` wrappers (NWSL StatsPerform SDP API).

``api-sdp.nwslsoccer.com/v1/nwsl/football/...`` is auth-free and consistently
enveloped: rows sit under a named key (``competitions``, ``matches``, ``teams``,
``players``, ``standings``, ``matchdays``, ``stages``) alongside an
``apiCallRequestTime`` assembly timestamp. :func:`parse_nwsl_sdp` unwraps that for
the five plain-list routes; three routes carry a second level of nesting and get
their own parser.

Composite ids -- ``nwsl::Football_{Entity}::{32-hex}`` for competitions, seasons,
stages, match days, matches, teams, stadia, players and officials -- are ``Utf8``
join keys and are pinned as such; each row also carries the underlying
StatsPerform ``providerId`` (``opta:...``).

**Long vs wide stats.** Standings, player-stats and team-stats rows all carry an
ordered ``stats[]`` array of ``{statsId, statsLabel, statsValue, ...}`` cells. The
standings set is a fixed twelve keys, so :func:`parse_nwsl_standings` pivots it to
one column per stat with native dtypes. The ``/stats/*`` sets instead vary by the
``category`` argument and run to ~170 keys, so :func:`parse_nwsl_stats` keeps them
**long** -- one row per entity per stat -- which gives a stable, documentable
schema rather than a column set that changes with the request.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.soccer._frames import as_output, as_tables, rows_to_frame

__all__ = [
    "parse_nwsl_lineups",
    "parse_nwsl_sdp",
    "parse_nwsl_standings",
    "parse_nwsl_stats",
]

# Rows-key preference order. ``competitions`` is last because
# ``multipleSeasonMatches`` returns BOTH a ``competitions`` context array and the
# ``matches`` rows -- the caller wants the matches.
_ROWS_KEYS = ("matches", "matchdays", "stages", "standings", "players", "teams", "competitions")

# Envelope keys that are context/metadata rather than rows.
_META_KEYS = frozenset({"apiCallRequestTime", "competition", "pagination"})

# Identity fields lifted onto each long stat row (and dropped from the wide pivot).
_NESTED_STAT_KEYS = frozenset({"stats"})


def _envelope_rows(raw: Union[Dict[str, Any], List[Any], None]) -> List[Any]:
    """Rows array from an SDP envelope (``[]`` for anything unusable)."""
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict):
        return []
    for key in _ROWS_KEYS:
        value = raw.get(key)
        if isinstance(value, list) and value:
            return value
    for key, value in raw.items():
        if key in _META_KEYS or not isinstance(value, list) or not value:
            continue
        if all(isinstance(v, dict) for v in value):
            return value
    return []


def _stat_cells(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The ``stats[]`` cells of one entity row."""
    return [c for c in (row.get("stats") or []) if isinstance(c, dict)]


def _scalar(value: Any) -> Any:
    """Native scalar for a wide stat cell; nested values are JSON-encoded."""
    return json.dumps(value) if isinstance(value, (list, dict)) else value


def parse_nwsl_sdp(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a plain NWSL SDP envelope into a tidy frame.

    Covers ``nwsl_competitions``, ``nwsl_teams``, ``nwsl_matchdays``,
    ``nwsl_stages`` and ``nwsl_season_matches``.

    Args:
        raw: an SDP JSON body -- an envelope whose named array holds the rows, or
            a bare list.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record, snake_cased and ``json_normalize``-flattened, with
        every ``*_id`` column pinned to ``Utf8``. A zero-row frame when the
        payload is ``None``, empty or malformed -- which includes the real
        ``{"stages": null}`` body a pure-league season returns.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.nwsl import nwsl_teams

            df = nwsl_teams(season_id="nwsl::Football_Season::0b6761e4701749f593690c0f338da74c")
            print(df.select("team_id", "official_name").head())

        Pipeline next step (one line)::

            df.select("team_id", "official_name", "stadium_name").sort("official_name")

    See Also:
        * `NWSL`_ -- the public site this feed renders.

    .. _NWSL: https://www.nwslsoccer.com/
    """
    return as_output(rows_to_frame(_envelope_rows(raw)), return_as_pandas=return_as_pandas)


def parse_nwsl_standings(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse an NWSL standings payload into one wide row per club per split.

    The body nests ``standings[]`` (one entry per split -- ``table``, ``home``,
    ``away``) over ``teams[]`` (the ranked clubs). Each club's fixed twelve-cell
    ``stats[]`` array is pivoted to columns so the result reads as a table.

    Args:
        raw: an SDP ``/standings/overall`` JSON body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per club per split: ``split_type`` (``table`` / ``home`` /
        ``away``), the club identity columns (``team_id`` pinned to ``Utf8``,
        ``provider_id``, ``official_name``, ``short_name``, ``acronym_name``,
        ``qualification_*``, ``note``) and the pivoted stat columns (``rank``,
        ``points``, ``matches_played``, ``win``, ``draw``, ``lose``,
        ``goals_for``, ``goals_against``, ``goal_difference``, ``movement``,
        ``form``). A zero-row frame when the payload is empty or malformed.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.nwsl import nwsl_standings

            df = nwsl_standings(season_id="nwsl::Football_Season::0b6761e4701749f593690c0f338da74c")
            print(df.filter(pl.col("split_type") == "table").select("rank", "official_name", "points"))

        Pipeline next step (one line)::

            df.filter(pl.col("split_type") == "table").sort("rank").head(4)

    See Also:
        * `NWSL`_ -- the public site this feed renders.

    .. _NWSL: https://www.nwslsoccer.com/
    """
    splits = raw.get("standings") if isinstance(raw, dict) else raw
    rows: List[Dict[str, Any]] = []
    for split in splits or []:
        if not isinstance(split, dict):
            continue
        split_type = split.get("type")
        for club in split.get("teams") or []:
            if not isinstance(club, dict):
                continue
            row: Dict[str, Any] = {"split_type": split_type}
            row.update({k: v for k, v in club.items() if k not in _NESTED_STAT_KEYS})
            for cell in _stat_cells(club):
                stat_id = cell.get("statsId")
                if stat_id:
                    row[underscore(str(stat_id))] = _scalar(cell.get("statsValue"))
            rows.append(row)
    return as_output(rows_to_frame(rows), return_as_pandas=return_as_pandas)


def parse_nwsl_stats(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse an NWSL player/team stats leaderboard into a long frame.

    Covers ``nwsl_player_stats`` and ``nwsl_team_stats``. The ``stats[]`` key set
    depends on the ``category`` argument and spans ~170 keys for the general
    player category, so the cells stay long -- one row per entity per stat --
    rather than being pivoted into a request-dependent column set.

    Args:
        raw: an SDP ``/stats/players`` or ``/stats/teams`` JSON body.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per entity per stat: the flattened entity identity columns
        (``player_id`` / ``team_id`` pinned to ``Utf8``, ``display_name``,
        ``role_label``, ``team_*`` for players) plus ``stats_id``,
        ``stats_label``, ``stats_label_abbreviation``, ``stats_value``,
        ``stats_unit`` and ``stats_unit_abbreviation``. ``stats_value`` is
        ``Utf8`` because the API mixes integers, strings and arrays in it.
        A zero-row frame when the payload is empty or malformed.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.nwsl import nwsl_player_stats

            df = nwsl_player_stats(
                season_id="nwsl::Football_Season::0b6761e4701749f593690c0f338da74c",
                category="general",
            )
            print(df.filter(pl.col("stats_id") == "goals").head())

        Pipeline next step (one line)::

            df.filter(pl.col("stats_id") == "goals").sort("stats_value", descending=True).head(10)

    See Also:
        * `NWSL`_ -- the public site this feed renders.

    .. _NWSL: https://www.nwslsoccer.com/
    """
    rows: List[Dict[str, Any]] = []
    for entity in _envelope_rows(raw):
        if not isinstance(entity, dict):
            continue
        identity = {k: v for k, v in entity.items() if k not in _NESTED_STAT_KEYS}
        for cell in _stat_cells(entity):
            value = cell.get("statsValue")
            rows.append(
                {
                    **identity,
                    **cell,
                    "statsValue": json.dumps(value) if isinstance(value, (list, dict)) else value,
                },
            )
    df = rows_to_frame(rows)
    if "stats_value" in df.columns and df.schema["stats_value"] != pl.String:
        df = df.with_columns(pl.col("stats_value").cast(pl.String))
    return as_output(df, return_as_pandas=return_as_pandas)


def parse_nwsl_lineups(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Dict[str, Union[pl.DataFrame, pd.DataFrame]]:
    """Parse an NWSL match-lineups payload into team, player and staff frames.

    The body is ``{matchId, pitchSizeX, pitchSizeY, home: {...}, away: {...}}``
    where each side mixes team metadata with three arrays (``fielded``,
    ``benched``, ``staff``). Splitting by role keeps each frame's columns
    meaningful instead of collapsing the match into one unusable row.

    Args:
        raw: an SDP ``/matches/{matchId}/lineups`` JSON body.
        return_as_pandas: return pandas DataFrames instead of polars.

    Returns:
        Three sub-frames, all present even when the payload is empty:

        * ``"teams"`` -- two rows, one per ``side``, with ``match_id``,
          ``team_id``, ``tactical_formation`` and kit colours.
        * ``"players"`` -- the starting XI and bench, with ``side``, ``team_id``,
          ``selection`` (``fielded`` / ``benched``), ``player_id``,
          ``bib_number``, ``role_label`` and ``display_name``.
        * ``"staff"`` -- coaching staff with ``side``, ``team_id``, ``staff_id``
          and ``role_label``.

    Raises:
        None: malformed payloads yield zero-row frames rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.nwsl import nwsl_match_lineups

            tables = nwsl_match_lineups(
                season_id="nwsl::Football_Season::0b6761e4701749f593690c0f338da74c",
                match_id="nwsl::Football_Match::abc",
            )
            print(tables["players"].select("side", "selection", "display_name").head())

        Pipeline next step (one line)::

            tables["players"].filter(pl.col("selection") == "fielded").group_by("side").len()

    See Also:
        * `NWSL`_ -- the public site this feed renders.

    .. _NWSL: https://www.nwslsoccer.com/
    """
    body = raw if isinstance(raw, dict) else {}
    match_id = body.get("matchId")
    teams: List[Dict[str, Any]] = []
    players: List[Dict[str, Any]] = []
    staff: List[Dict[str, Any]] = []
    for side in ("home", "away"):
        block = body.get(side)
        if not isinstance(block, dict):
            continue
        scalars = {k: v for k, v in block.items() if not isinstance(v, (list, dict))}
        teams.append({"matchId": match_id, "side": side, **scalars})
        keys = {"matchId": match_id, "side": side, "teamId": block.get("teamId")}
        for selection in ("fielded", "benched"):
            for person in block.get(selection) or []:
                if isinstance(person, dict):
                    players.append({**keys, "selection": selection, **person})
        for person in block.get("staff") or []:
            if isinstance(person, dict):
                staff.append({**keys, **person})
    out = {"teams": rows_to_frame(teams), "players": rows_to_frame(players), "staff": rows_to_frame(staff)}
    return as_tables(out, return_as_pandas=return_as_pandas)
