"""Pure HockeyTech parsers: parsed JSON (dict/list) -> snake_cased polars frame.

No network here -- tests drive these from captured fixtures. Every parser
tolerates empty/None payloads by returning a zero-row frame.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all columns to snake_case and deduplicate by keeping the first occurrence."""
    seen: dict[str, int] = {}
    new_cols: list[str] = []
    for c in df.columns:
        snake = underscore(str(c)).replace(".", "_").replace("__", "_")
        if snake in seen:
            # suffix subsequent duplicates so polars doesn't raise on non-unique columns
            seen[snake] += 1
            snake = f"{snake}_{seen[snake]}"
        else:
            seen[snake] = 0
        new_cols.append(snake)
    df.columns = new_cols
    return df


def _to_frame(records: List[Dict[str, Any]], return_as_pandas: bool) -> Any:
    pdf = pd.json_normalize(records or [], sep="_")
    pdf = _snake_columns(pdf)
    if return_as_pandas:
        return pdf
    return pl.from_pandas(pdf) if len(pdf) else pl.DataFrame()


def _sitekit(payload: Any, key: str) -> Any:
    return ((payload or {}).get("SiteKit", {}) or {}).get(key)


def _derive_season_year(name: str) -> Optional[int]:
    m = re.search(r"(\d{4})-(\d{2})", name or "")
    if m:
        return int(m.group(1)[:2]) * 100 + int(m.group(2))
    m2 = re.search(r"(\d{4})", name or "")
    return int(m2.group(1)) if m2 else None


def _game_type_label(name: str) -> str:
    n = (name or "").lower()
    if re.search(r"pre[- ]?season", n):
        return "preseason"
    if re.search(r"playoff|post", n):
        return "playoffs"
    return "regular"


_SCOREBAR_RENAME = {
    "ID": "game_id",
    "GameDateISO8601": "game_date",
    "GameStatusStringLong": "game_status",
    "HomeLongName": "home_team",
    "HomeID": "home_team_id",
    "HomeGoals": "home_score",
    "VisitorLongName": "away_team",
    "VisitorID": "away_team_id",
    "VisitorGoals": "away_score",
    "venue_name": "venue",
    "SeasonID": "season_id",
}


def parse_schedule(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/scorebar`` JSON payload into a flat frame.

    One row per game. Returns a :class:`polars.DataFrame` by default; pass
    ``return_as_pandas=True`` for a :class:`pandas.DataFrame`. An empty/None
    payload returns a zero-row frame of the same type, never raises.
    """
    games = _sitekit(payload, "Scorebar") or []
    rows = []
    for g in games:
        row = {new: g.get(old) for old, new in _SCOREBAR_RENAME.items()}
        row["game_type"] = g.get("game_type")
        rows.append(row)
    return _to_frame(rows, return_as_pandas)


def parse_standings(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/standings`` JSON payload into a flat frame.

    The payload is a top-level LIST of section dicts, each containing a
    ``sections`` list with ``data`` items whose ``row`` key holds the per-team
    stats dict. Returns a :class:`polars.DataFrame` by default; pass
    ``return_as_pandas=True`` for a :class:`pandas.DataFrame`. An empty/None
    payload returns a zero-row frame of the same type, never raises.
    """
    rows: List[Dict[str, Any]] = []
    sections = payload if isinstance(payload, list) else (payload or {}).get("sections", [])
    for sec in sections or []:
        if not isinstance(sec, dict):
            continue
        for blk in sec.get("sections", [sec]):
            for item in blk.get("data", []) or []:
                r = item.get("row") if isinstance(item, dict) else None
                if isinstance(r, dict):
                    rows.append(r)
    df = _to_frame(rows, False)
    # Rename raw keys -> required column names:
    # - "rank" -> "team_rank"  (standings position integer already present)
    # - "name" -> "team"       (long team name)
    # - "regulation_wins" -> "wins"  (W column; raw key is "regulation_wins")
    ren = {
        "rank": "team_rank",
        "name": "team",
        "regulation_wins": "wins",
    }
    df = df.rename({k: v for k, v in ren.items() if k in df.columns})
    return df.to_pandas() if return_as_pandas else df


def parse_teams(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/teamsbyseason`` JSON payload into a flat frame.

    Returns a :class:`polars.DataFrame` by default; pass ``return_as_pandas=True``
    for a :class:`pandas.DataFrame`. An empty/None payload returns a zero-row
    frame of the same type, never raises.
    """
    raw = _sitekit(payload, "Teamsbyseason") or []
    rows: List[Dict[str, Any]] = []
    for t in raw:
        rows.append(
            {
                "team_name": t.get("name"),
                "team_id": t.get("id"),
                "team_code": t.get("code"),
                "team_nickname": t.get("nickname"),
                "team_label": t.get("city"),
                "division": t.get("division_id") or t.get("division"),
                "team_logo": t.get("team_logo_url") or t.get("logo"),
            }
        )
    return _to_frame(rows, return_as_pandas)


def parse_roster(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/roster`` JSON payload into a flat frame.

    Returns a :class:`polars.DataFrame` by default; pass ``return_as_pandas=True``
    for a :class:`pandas.DataFrame`. An empty/None payload returns a zero-row
    frame of the same type, never raises.

    List-valued fields (e.g. ``draftinfo``) are dropped before normalization
    because ``pd.json_normalize`` cannot flatten bare lists.
    """
    raw = _sitekit(payload, "Roster") or []
    rows: List[Dict[str, Any]] = []
    for player in raw:
        if not isinstance(player, dict):
            continue
        rows.append({k: v for k, v in player.items() if not isinstance(v, list)})
    return _to_frame(rows, return_as_pandas)


def parse_seasons(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/seasons`` JSON payload into a flat frame.

    Returns a :class:`polars.DataFrame` by default; pass ``return_as_pandas=True``
    for a :class:`pandas.DataFrame`. An empty/None payload returns a zero-row
    frame of the same type, never raises.
    """
    raw = _sitekit(payload, "Seasons") or []
    rows = []
    for s in raw:
        name = s.get("season_name")
        rows.append(
            {
                "season_id": int(s.get("season_id")) if s.get("season_id") else None,
                "season_name": name,
                "season_short": s.get("shortname"),
                "career": s.get("career", "0"),
                "playoff": s.get("playoff", "0"),
                "start_date": s.get("start_date"),
                "end_date": s.get("end_date"),
                "season_yr": _derive_season_year(name),
                "game_type_label": _game_type_label(name),
            }
        )
    return _to_frame(rows, return_as_pandas)
