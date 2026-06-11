"""Polars parsers for the modern ``api.nfl.com`` ``/football/v2`` + ``/experience`` surface.

Each ``parse_nfl_*`` here flattens one :mod:`sportsdataverse.nfl.nfl_api` wrapper's
raw payload into a tidy frame. The records of interest live under a different key
per endpoint (``weeks[].standings``, ``rosters``, ``teams``, ``picks``, ``data``,
or a bare list / single object), so each parser does its own record extraction and
then flattens with ``pl.json_normalize(..., separator="_", max_level=2)``.

Every parser returns a ``polars.DataFrame`` by default; pass
``return_as_pandas=True`` for pandas.
"""

from __future__ import annotations

from typing import Dict, List, Union

__all__ = [
    "parse_nfl_standings",
    "parse_nfl_rosters",
    "parse_nfl_teams_history",
    "parse_nfl_team",
    "parse_nfl_weeks",
    "parse_nfl_weeks_by_date",
    "parse_nfl_combine_profiles",
    "parse_nfl_draft_picks",
    "parse_nfl_injuries",
    "parse_nfl_game_summaries",
    "parse_nfl_weekly_game_details",
]


def _to_frame(records: List, return_as_pandas: bool):
    """Flatten a list of nested dicts into a polars (or pandas) DataFrame."""
    import polars as pl

    df = pl.json_normalize(records or [], separator="_", max_level=2, infer_schema_length=None)
    return df.to_pandas() if return_as_pandas else df


def parse_nfl_standings(raw: Dict, return_as_pandas: bool = False):
    """``/football/v2/standings`` -> one row per team (``weeks[].standings[]``)."""
    records: List = []
    for wk in raw.get("weeks", []) or []:
        records.extend(wk.get("standings", []) or [])
    return _to_frame(records, return_as_pandas)


def parse_nfl_rosters(raw: Dict, return_as_pandas: bool = False):
    """``/football/v2/rosters`` -> one row per team roster (``rosters``)."""
    return _to_frame(raw.get("rosters", []), return_as_pandas)


def parse_nfl_teams_history(raw: Dict, return_as_pandas: bool = False):
    """``/football/v2/teams/history`` -> one row per team (``teams``)."""
    return _to_frame(raw.get("teams", []), return_as_pandas)


def parse_nfl_team(raw: Union[Dict, List], return_as_pandas: bool = False):
    """``/football/v2/teams/{id}`` -> one-row frame (single team object)."""
    return _to_frame([raw] if isinstance(raw, dict) else raw, return_as_pandas)


def parse_nfl_weeks(raw: Dict, return_as_pandas: bool = False):
    """``/football/v2/weeks/season/...`` -> one row per week (``weeks``)."""
    return _to_frame(raw.get("weeks", []), return_as_pandas)


def parse_nfl_weeks_by_date(raw: Union[Dict, List], return_as_pandas: bool = False):
    """``/football/v2/weeks/date/{date}`` -> one-row frame (single week object)."""
    return _to_frame([raw] if isinstance(raw, dict) else raw, return_as_pandas)


def parse_nfl_combine_profiles(raw: Dict, return_as_pandas: bool = False):
    """``/football/v2/combine/profiles`` -> one row per profile (``combineProfiles``)."""
    return _to_frame(raw.get("combineProfiles", []), return_as_pandas)


def parse_nfl_draft_picks(raw: Dict, return_as_pandas: bool = False):
    """``/football/v2/draft/picks/report`` -> one row per pick (``picks``)."""
    return _to_frame(raw.get("picks", []), return_as_pandas)


def parse_nfl_injuries(raw: Dict, return_as_pandas: bool = False):
    """``/football/v2/injuries`` -> one row per player (``injuries``)."""
    return _to_frame(raw.get("injuries", []), return_as_pandas)


def parse_nfl_game_summaries(raw: Dict, return_as_pandas: bool = False):
    """``/football/v2/stats/live/game-summaries`` -> one row per game (``data``)."""
    return _to_frame(raw.get("data", []), return_as_pandas)


def parse_nfl_weekly_game_details(raw: Union[Dict, List], return_as_pandas: bool = False):
    """``/football/v2/experience/weekly-game-details`` -> one row per game (bare list)."""
    records = raw if isinstance(raw, list) else raw.get("games", []) or raw.get("data", [])
    return _to_frame(records, return_as_pandas)
