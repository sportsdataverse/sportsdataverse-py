"""Shared parsing layer for Fox Sports "Bifrost" wrappers (``fox_<sport>_*``).

The Bifrost API is a layout API (sections -> tables -> rows -> cells) that is
uniform across sports; only the ``{sport}`` slug and the play-by-play shape
differ. This module centralizes the HTTP call, the generic table flattener, and
the per-shape parsers so each league module (``cfb``/``nba``/``mbb``/``nhl``/
``mlb``) stays a thin set of public ``fox_<sport>_*`` wrappers.

pbp shapes:
  - period-based (nba/mbb/nhl): ``pbp.sections[0].groups[]`` are periods
    (QUARTER/HALF/PERIOD) each with ``plays[]``.
  - drive-based (cfb): handled in ``sportsdataverse.cfb.cfb_fox_ext``.
Reverse-engineering notes + an OpenAPI 3.1 spec live in the ``sdv-internal-refs``
repo.
"""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

from sportsdataverse._codegen_runtime import _get

API = "https://api.foxsports.com/bifrost/v1"
# Public data-tier key shipped in the foxsports.com web bundle. Overridable via
# the SDV_PY_FOX_DATA_KEY env var so a key rotation does not require a release.
DATA_KEY = os.getenv("SDV_PY_FOX_DATA_KEY", "jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq")
_HEADERS = {"Origin": "https://www.foxsports.com", "Referer": "https://www.foxsports.com/"}


def fox_get(path: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a Bifrost path with the public data-tier key + api-version.

    Args:
        path: Bifrost path under ``/bifrost/v1`` (e.g. ``"cbk/team/11/roster"``).
        params: Extra query params merged on top of ``apikey`` / ``api-version``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        The parsed JSON response body as a ``dict``.
    """
    merged = {"apikey": DATA_KEY, "api-version": "1.1"}
    if params:
        merged.update(params)
    return _get(f"{API}/{path}", params=merged, headers=_HEADERS, **kwargs)


def frame(rows: List[Dict[str, Any]], return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Materialize parsed rows as a polars (default) or pandas DataFrame.

    Args:
        rows: Flattened row dicts produced by a ``parse_*`` function.
        return_as_pandas: If ``True`` return a pandas DataFrame; else polars.

    Returns:
        A ``polars.DataFrame`` (default) or ``pandas.DataFrame``.
    """
    if return_as_pandas:
        import pandas as pd

        return pd.DataFrame(rows)
    return pl.DataFrame(rows)


def _cells(columns) -> List[Optional[str]]:
    return [c.get("text") if isinstance(c, dict) else c for c in (columns or [])]


def _uri_id(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    m = re.search(r"(\d+)$", uri)
    return m.group(1) if m else None


def _clean(name) -> str:
    return re.sub(r"\W+", "_", str(name)).strip("_").lower() or "v"


def _table_rows(tbl: Optional[dict], extra: Optional[dict] = None) -> List[Dict]:
    """A Bifrost table ``{headers, rows}`` -> list of wide dict rows."""
    extra = extra or {}
    if not tbl:
        return []
    headers = _cells((tbl.get("headers") or [{}])[0].get("columns"))
    names = [_clean(h) if h not in (None, "") else f"v{i}" for i, h in enumerate(headers)]
    out: List[Dict] = []
    for r in tbl.get("rows", []) or []:
        cells = _cells(r.get("columns"))
        row = dict(extra)
        for name, val in zip(names, cells):
            row[name] = val
        row["entity_id"] = _uri_id((r.get("entityLink") or {}).get("contentUri"))
        out.append(row)
    return out


# ---- entity/league parsers (generic across sports) ------------------------
def parse_roster(raw: Dict, team_id) -> List[Dict]:
    """team/{id}/roster groups -> one row per player (athletes only)."""
    rows: List[Dict] = []
    for g in raw.get("groups", []) or []:
        headers = _cells((g.get("headers") or [{}])[0].get("columns"))
        group_label = g.get("title") or (headers[0] if headers else None)
        col_names = ["player"] + [str(h).lower() for h in headers[1:]]
        for r in g.get("rows", []) or []:
            uri = (r.get("entityLink") or {}).get("contentUri")
            if not uri or "athletes/" not in uri:  # players only
                continue
            cells = _cells(r.get("columns"))
            row = {"team_id": str(team_id), "position_group": group_label}
            for name, val in zip(col_names, cells):
                row[name] = val
            row["athlete_id"] = _uri_id(uri)
            rows.append(row)
    return rows


def parse_team_stats(raw: Dict, team_id) -> List[Dict]:
    """team/{id}/stats leadersSections -> one row per category stat leader."""
    rows: List[Dict] = []
    for sec in raw.get("leadersSections", []) or []:
        for ld in sec.get("leaders", []) or []:
            rows.append(
                {
                    "team_id": str(team_id),
                    "category": sec.get("title"),
                    "stat": ld.get("title"),
                    "stat_abbreviation": ld.get("statAbbreviation"),
                    "player": ld.get("name"),
                    "value": ld.get("statValue"),
                }
            )
    return rows


def parse_team_gamelog(raw: Dict, team_id) -> List[Dict]:
    """sectionList -> tables; long: one row per (game, category, stat)."""
    rows: List[Dict] = []
    for sec in raw.get("sectionList", []) or []:
        category = sec.get("id")
        for tbl in sec.get("tables", []) or []:
            headers = _cells((tbl.get("headers") or [{}])[0].get("columns"))
            season_type = headers[0] if headers else None
            stat_names, seen = [], {}
            for h in headers[2:]:  # skip date + opponent columns
                base = _clean(h)
                seen[base] = seen.get(base, 0) + 1
                stat_names.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
            for r in tbl.get("rows", []) or []:
                cells = _cells(r.get("columns"))
                gid = _uri_id((r.get("entityLink") or {}).get("contentUri"))
                game_date = cells[0] if len(cells) > 0 else None
                opponent = cells[1] if len(cells) > 1 else None
                for name, val in zip(stat_names, cells[2:]):
                    rows.append(
                        {
                            "team_id": str(team_id),
                            "season_type": season_type,
                            "category": category,
                            "game_id": gid,
                            "game_date": game_date,
                            "opponent": opponent,
                            "stat": name,
                            "value": val,
                        }
                    )
    return rows


def parse_standings(raw: Dict, team_id=None) -> List[Dict]:
    """team/{id}/standings (or league/standings): standingsSections[].standings
    is a *list* of tables."""
    rows: List[Dict] = []
    for sec in raw.get("standingsSections", []) or []:
        extra = {"section": sec.get("title")}
        if team_id is not None:
            extra = {"team_id": str(team_id), **extra}
        for tbl in sec.get("standings", []) or []:
            rows += _table_rows(tbl, extra=extra)
    return rows


def parse_league_leaders(raw: Dict) -> List[Dict]:
    """league/stats-con sectionList tables -> one row per ranked entity."""
    rows: List[Dict] = []
    for sec in raw.get("sectionList", []) or []:
        rows += _table_rows(sec.get("table"))
    return rows


def parse_odds(raw: Dict, game_id) -> List[Dict]:
    """event/{id}/odds sixPack -> one row per team (spread/to-win/total)."""
    rows: List[Dict] = []
    odds = (raw.get("sixPack") or {}).get("odds")
    if odds:
        names = [_clean(c) for c in _cells(odds.get("columnHeaders"))]
        for r in odds.get("rows", []) or []:
            row = {"game_id": str(game_id), "team": r.get("fullText") or r.get("text")}
            for name, v in zip(names, r.get("values", []) or []):
                row[name] = (v or {}).get("odds")
            rows.append(row)
    return rows


def parse_boxscore(raw: Dict, game_id) -> List[Dict]:
    """event/{id}/data boxscore -> long (one row per player-stat). Sections with
    no boxscoreItems (e.g. the "MATCHUP" summary in nba/nhl) are skipped."""
    rows: List[Dict] = []
    for sec in (raw.get("boxscore", {}) or {}).get("boxscoreSections", []) or []:
        team = sec.get("title")
        for item in sec.get("boxscoreItems", []) or []:
            tbl = item.get("boxscoreTable")
            if not tbl:
                continue
            headers = _cells((tbl.get("headers") or [{}])[0].get("columns"))
            stat_group = headers[0] if headers else None
            stat_names = [_clean(h) for h in headers[1:]]
            for r in tbl.get("rows", []) or []:
                cells = _cells(r.get("columns"))
                player = cells[0] if cells else None
                aid = _uri_id((r.get("entityLink") or {}).get("contentUri"))
                for name, val in zip(stat_names, cells[1:]):
                    rows.append(
                        {
                            "game_id": str(game_id),
                            "team": team,
                            "stat_group": stat_group,
                            "player": player,
                            "athlete_id": aid,
                            "stat": name,
                            "value": val,
                        }
                    )
    return rows


def parse_period_pbp(raw: Dict, game_id) -> List[Dict]:
    """event/{id}/data pbp for period sports (nba/mbb/nhl): one row per play.

    Structure: ``pbp.sections[0].groups[]`` are periods (1ST QUARTER / 1ST HALF /
    1ST PERIOD) each with ``plays[]``.
    """
    rows: List[Dict] = []
    for sec in (raw.get("pbp", {}) or {}).get("sections", []) or []:
        for grp in sec.get("groups", []) or []:
            period = grp.get("title")
            left, right = grp.get("leftTeamAbbr"), grp.get("rightTeamAbbr")
            for p in grp.get("plays", []) or []:
                rows.append(
                    {
                        "game_id": str(game_id),
                        "period": period,
                        "left_team": left,
                        "right_team": right,
                        "play_id": p.get("id"),
                        "clock": p.get("timeOfPlay"),
                        "team": (p.get("entityLink") or {}).get("title") or p.get("imageAltText"),
                        "left_score_change": p.get("leftTeamScoreChange"),
                        "right_score_change": p.get("rightTeamScoreChange"),
                        "play_text": p.get("playDescription"),
                    }
                )
    return rows
