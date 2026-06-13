"""ESPN soccer parsers — tidy polars frames for the soccer endpoint family.

Contract (shared with the universal parsers): return a polars.DataFrame by default,
pandas when return_as_pandas=True; empty/malformed payloads return a zero-row frame
(never raise); columns are snake_cased.
"""

from __future__ import annotations

from typing import Any, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _out(df: pl.DataFrame, return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    return df.to_pandas() if return_as_pandas else df


def _competitor(comp: dict, home_away: str) -> dict:
    for c in comp.get("competitors", []) or []:
        if c.get("homeAway") == home_away:
            return c
    return {}


def parse_soccer_standings(payload: Any, *, return_as_pandas: bool = False):
    """Parse an ESPN soccer standings payload into a tidy flat DataFrame.

    Each row is one team in one group/conference. The ``group`` column holds
    the child (group/conference) name so multi-group leagues (MLS, World Cup
    groups) can be filtered directly.

    Args:
        payload: Raw dict from an ESPN ``standings`` endpoint response.
        return_as_pandas: When True, return a :class:`pandas.DataFrame` instead.

    Returns:
        pl.DataFrame or pd.DataFrame — zero rows when payload is empty/malformed.

    Example:
        Quick start::

            from sportsdataverse.soccer import espn_epl_standings
            df = espn_epl_standings(return_parsed=True)
            print(df.shape)
    """
    children = (payload or {}).get("children") if isinstance(payload, dict) else None
    rows: list[dict] = []
    for child in children or []:
        group = child.get("name")
        standings_block = child.get("standings", {})
        entries = standings_block.get("entries") or []
        for entry in entries:
            team = entry.get("team") or {}
            note = entry.get("note") or {}
            row: dict = {
                "group": group,
                "team": team.get("displayName"),
                "team_id": team.get("id"),
                "team_abbreviation": team.get("abbreviation"),
                "note": note.get("description") if isinstance(note, dict) else None,
            }
            for stat in entry.get("stats") or []:
                col = underscore(stat["name"])
                val = stat.get("value")
                if val is None:
                    val = stat.get("displayValue")
                row[col] = val
            rows.append(row)
    if not rows:
        return _out(pl.DataFrame(), return_as_pandas)
    return _out(pl.DataFrame(rows), return_as_pandas)


def parse_soccer_scoreboard(payload: Any, *, return_as_pandas: bool = False):
    events = (payload or {}).get("events") if isinstance(payload, dict) else None
    rows = []
    for ev in events or []:
        comp = (ev.get("competitions") or [{}])[0]
        home, away = _competitor(comp, "home"), _competitor(comp, "away")
        rows.append(
            {
                "event_id": ev.get("id"),
                "date": ev.get("date"),
                "name": ev.get("name"),
                "short_name": ev.get("shortName"),
                "home_team": (home.get("team") or {}).get("displayName"),
                "home_team_id": (home.get("team") or {}).get("id"),
                "home_score": home.get("score"),
                "away_team": (away.get("team") or {}).get("displayName"),
                "away_team_id": (away.get("team") or {}).get("id"),
                "away_score": away.get("score"),
                "status": ((ev.get("status") or {}).get("type") or {}).get("name"),
                "venue": (comp.get("venue") or {}).get("fullName"),
            }
        )
    if not rows:
        return _out(pl.DataFrame(), return_as_pandas)
    return _out(pl.DataFrame(rows), return_as_pandas)
