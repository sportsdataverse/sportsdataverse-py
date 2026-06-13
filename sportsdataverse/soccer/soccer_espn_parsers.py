"""ESPN soccer parsers — tidy polars frames for the soccer endpoint family.

Contract (shared with the universal parsers): return a polars.DataFrame by default,
pandas when return_as_pandas=True; empty/malformed payloads return a zero-row frame
(never raise); columns are snake_cased.
"""

from __future__ import annotations

from typing import Any, Union

import pandas as pd
import polars as pl


def _out(df: pl.DataFrame, return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    return df.to_pandas() if return_as_pandas else df


def _competitor(comp: dict, home_away: str) -> dict:
    for c in comp.get("competitors", []) or []:
        if c.get("homeAway") == home_away:
            return c
    return {}


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
