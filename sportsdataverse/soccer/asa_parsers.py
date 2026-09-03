"""Parsers for the generated ``asa`` wrappers (American Soccer Analysis public API).

``app.americansocceranalysis.com/api/v1/{league}/...`` is auth-free and uniform:
every route answers with a **flat top-level JSON array** -- no envelope, no
pagination metadata, no total count -- so one generic parser covers 12 of the 15
routes. The three ``goals-added`` routes are the single exception: each row
carries a nested ``data[]`` breakdown of Goals Added per action type, which is a
genuinely second table and gets its own parser.

Ids (``player_id``, ``team_id``, ``game_id``, ``stadium_id``, ``manager_id``,
``referee_id``) are short base62 **strings**, not integers; they are pinned to
``Utf8`` by :mod:`sportsdataverse.soccer._frames`. On ``players/xgoals`` and
``players/goals-added`` the API serializes ``team_id`` as a *list* for a player
who featured for several clubs in the window -- those cells are comma-joined so
the column stays a single Utf8 join key.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, and snake_cased columns.
"""

from __future__ import annotations

from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.soccer._frames import as_output, as_tables, rows_to_frame

__all__ = [
    "parse_asa",
    "parse_asa_goals_added",
]

# Row-identity keys copied onto every exploded ``data[]`` action row so the long
# ``actions`` frame joins back to ``summary`` without a positional index.
_GOALS_ADDED_KEYS = ("player_id", "team_id", "general_position", "minutes_played", "minutes")


def _as_rows(raw: Union[Dict[str, Any], List[Any], None]) -> List[Any]:
    """Normalize an ASA body to a row list (``[]`` for anything unusable)."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and raw:
        return [raw]
    return []


def parse_asa(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a flat American Soccer Analysis array into a tidy frame.

    Covers every ASA route except the three ``goals-added`` ones (see
    :func:`parse_asa_goals_added`): the entity tables (``teams``, ``players``,
    ``games``, ``stadia``, ``managers``, ``referees``) and the flat stat tables
    (``players/xgoals``, ``players/salaries``, ``teams/xgoals``, ``teams/xpass``,
    ``goalkeepers/xgoals``, ``games/xgoals``).

    Args:
        raw: an ASA JSON body -- normally a top-level list of row objects. A bare
            dict is accepted and becomes a single row.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record, snake_cased, with every ``*_id`` column pinned to
        ``Utf8``. A zero-row frame when the payload is ``None``, empty or
        malformed -- callers can chain without a null-check.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.asa import asa_players_xgoals

            df = asa_players_xgoals(league_slug="mls", season_name="2023", minimum_minutes="500")
            print(df.shape)

        Pipeline next step (one line)::

            df.sort("xgoals", descending=True).head(10)

    See Also:
        * `itscalledsoccer`_ -- the official ASA R/Python client over the same API.

    .. _itscalledsoccer: https://github.com/American-Soccer-Analysis/itscalledsoccer
    """
    return as_output(rows_to_frame(_as_rows(raw)), return_as_pandas=return_as_pandas)


def parse_asa_goals_added(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Dict[str, Union[pl.DataFrame, pd.DataFrame]]:
    """Parse an ASA Goals Added (g+) payload into a summary + long action frame.

    ``players/goals-added``, ``teams/goals-added`` and ``goalkeepers/goals-added``
    nest a per-action-type breakdown under ``data[]`` -- the only nested shape ASA
    serves. Keeping it as a stringified cell would make it unusable, so it is
    exploded into a second, long frame instead.

    Args:
        raw: an ASA ``goals-added`` JSON body (a top-level list of row objects).
        return_as_pandas: return pandas DataFrames instead of polars.

    Returns:
        Two sub-frames, both present even when the payload is empty:

        * ``"summary"`` -- one row per entity (``player_id`` / ``team_id``,
          ``general_position``, ``minutes_played`` / ``minutes``) with the nested
          ``data`` column dropped.
        * ``"actions"`` -- one row per entity per action type, carrying the entity
          keys plus ``action_type`` and the g+ measures (``goals_added_raw``,
          ``goals_added_above_avg``, ``count_actions`` for player/goalkeeper rows;
          ``num_actions_for``, ``goals_added_for``, ``num_actions_against``,
          ``goals_added_against`` for team rows).

    Raises:
        None: malformed payloads yield zero-row frames rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.asa import asa_players_goals_added

            tables = asa_players_goals_added(league_slug="mls", season_name="2023")
            print(tables["actions"].shape)

        Pipeline next step (one line)::

            tables["actions"].group_by("action_type").agg(pl.col("goals_added_above_avg").sum())

    See Also:
        * `itscalledsoccer`_ -- the official ASA R/Python client over the same API.

    .. _itscalledsoccer: https://github.com/American-Soccer-Analysis/itscalledsoccer
    """
    rows = [r for r in _as_rows(raw) if isinstance(r, dict)]
    summary = [{k: v for k, v in row.items() if k != "data"} for row in rows]
    actions: List[Dict[str, Any]] = []
    for row in rows:
        keys = {k: row[k] for k in _GOALS_ADDED_KEYS if k in row}
        for action in row.get("data") or []:
            if isinstance(action, dict):
                actions.append({**keys, **action})
    tables = {"summary": rows_to_frame(summary), "actions": rows_to_frame(actions)}
    return as_tables(tables, return_as_pandas=return_as_pandas)
