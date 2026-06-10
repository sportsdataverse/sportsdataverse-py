"""Yahoo Sports college-football wrappers (hand-written ext).

Read-only wrappers over Yahoo's shangrila stats graph
(``graphite-secure.sports.yahoo.com/v1/query/shangrila``) and editorial feed
(``api-secure.sports.yahoo.com/v1/editorial/s``). Reverse-engineering notes +
per-host OpenAPI specs live in the sdv-internal-refs repo
(``_notes/ysportsapi/`` and ``yahoo/``). NCAAF vertical slice. No auth: the
hosts only require Origin/Referer headers.

Mirrors the sibling ``cfb_fox_ext.py`` contract (``return_parsed`` /
``return_as_pandas``; polars by default, raw ``Dict`` when ``return_parsed=False``).
"""

from __future__ import annotations

import re
from typing import Dict, List, Union

import polars as pl

from sportsdataverse._codegen_runtime import _get

__all__ = [  # noqa: F822 — public functions added incrementally across tasks
    "yahoo_cfb_player_season_stats",
    "yahoo_cfb_team_season_stats",
    "yahoo_cfb_player_season_stats_legacy",
    "yahoo_cfb_team_season_stats_legacy",
    "yahoo_cfb_scoreboard",
    "yahoo_cfb_boxscore",
]

EDITORIAL_BASE = "https://api-secure.sports.yahoo.com/v1/editorial/s"
SHANGRILA_BASE = "https://graphite-secure.sports.yahoo.com/v1/query/shangrila"
_HEADERS = {"Origin": "https://sports.yahoo.com", "Referer": "https://sports.yahoo.com/"}

# valid legacy categories (from sdv-internal-refs catalog crawl, Pass A)
LEGACY_PLAYER_CATEGORIES = ("Passing", "Rushing", "Receiving", "Defense", "Kicking", "Punting", "Returns")
LEGACY_TEAM_CATEGORIES = LEGACY_PLAYER_CATEGORIES + ("Kickoffs", "Offense")


def _clean(name) -> str:
    return re.sub(r"\W+", "_", str(name)).strip("_").lower() or "v"


def _shangrila_get(query_name: str, params: dict, **kwargs) -> Dict:
    merged = {"lang": "en-US", "region": "US", "tz": "America/Chicago", **params}
    return _get(f"{SHANGRILA_BASE}/{query_name}", params=merged, headers=_HEADERS, **kwargs)


def _editorial_get(path: str, params: dict, **kwargs) -> Dict:
    merged = {"lang": "en-US", "region": "US", "tz": "America/Chicago", **params}
    return _get(f"{EDITORIAL_BASE}/{path}", params=merged, headers=_HEADERS, **kwargs)


def _entity_cols(row: Dict) -> Dict:
    """player|team header -> flat id/name columns."""
    out = {}
    ent = row.get("player") or row.get("team") or {}
    if "playerId" in ent:
        out["player_id"] = ent.get("playerId")
        out["display_name"] = ent.get("displayName")
        team = ent.get("team") or {}
        out["team"] = team.get("displayName")
        out["team_abbreviation"] = team.get("abbreviation")
    else:
        out["team"] = ent.get("displayName")
        out["team_abbreviation"] = ent.get("abbreviation")
    return out


def _flatten_modern(payload: Dict, sport_key: str) -> List[Dict]:
    """data.leagues[0].<sport_key>[] -> wide rows (one column per statId)."""
    leagues = (payload.get("data") or {}).get("leagues") or [{}]
    rows_in = leagues[0].get(sport_key, []) if leagues else []
    out: List[Dict] = []
    for row in rows_in:
        rec = _entity_cols(row)
        for s in row.get("stats", []) or []:
            sid = s.get("statId")
            if sid:
                rec[_clean(sid)] = s.get("value")
        out.append(rec)
    return out


def _flatten_legacy(payload: Dict) -> List[Dict]:
    """data.leagues[0].leaders[] -> wide rows (one column per statId)."""
    leagues = (payload.get("data") or {}).get("leagues") or [{}]
    leaders = leagues[0].get("leaders", []) if leagues else []
    out: List[Dict] = []
    for row in leaders:
        rec = _entity_cols(row)
        for s in row.get("stats", []) or []:
            sid = s.get("statId")
            if sid:
                rec[_clean(sid)] = s.get("value")
        out.append(rec)
    return out


def _frame(rows: List[Dict], return_as_pandas: bool):
    if return_as_pandas:
        import pandas as pd

        return pd.DataFrame(rows)
    return pl.DataFrame(rows)


def yahoo_cfb_player_season_stats(
    season: int = 2024,
    *,
    league_structure: str = "ncaaf.struct.div.1",
    count: int = 200,
    qualified: bool = False,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """Yahoo CFB player season stats (modern; one wide row per player).

    Endpoint: ``GET .../shangrila/leagueStatsIndividual?leagues=ncaaf&season=...``
    Returns all stat groups (passing/rushing/receiving/...) pivoted wide. NCAAF
    data is available 2013-present. ``return_parsed=False`` returns raw JSON.

    Example:
        >>> yahoo_cfb_player_season_stats(season=2024)
    """
    raw = _shangrila_get(
        "leagueStatsIndividual",
        {
            "leagues": "ncaaf",
            "season": season,
            "count": count,
            "leagueStructureId": league_structure,
            "qualified": str(qualified).lower(),
        },
        **kwargs,
    )
    if not return_parsed:
        return raw
    rows = _flatten_modern(raw, "footballStats")
    for r in rows:
        r["season"] = season  # self-describing
    return _frame(rows, return_as_pandas)


def yahoo_cfb_team_season_stats(
    season: int = 2024,
    *,
    league_structure: str = "ncaaf.struct.div.1",
    count: int = 200,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """Yahoo CFB team season stats (modern; one wide row per team).

    Endpoint: ``GET .../shangrila/leagueStatsByTeam?leagues=ncaaf&season=...``

    Example:
        >>> yahoo_cfb_team_season_stats(season=2024)
    """
    raw = _shangrila_get(
        "leagueStatsByTeam",
        {
            "leagues": "ncaaf",
            "season": season,
            "count": count,
            "leagueStructureId": league_structure,
        },
        **kwargs,
    )
    if not return_parsed:
        return raw
    rows = _flatten_modern(raw, "footballStats")
    for r in rows:
        r["season"] = season
    return _frame(rows, return_as_pandas)


def yahoo_cfb_player_season_stats_legacy(
    season: int = 2024,
    category: str = "Passing",
    sort_stat: str = "PASSING_YARDS",
    *,
    league_structure: str = "ncaaf.struct.div.1",
    count: int = 200,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """Yahoo CFB legacy per-category player leaders (one wide row per player).

    Endpoint: ``GET .../shangrila/seasonStatsFootball{Category}Ncaaf``
    ``category`` in {Passing, Rushing, Receiving, Defense, Kicking, Punting, Returns}.
    ``sort_stat`` is a required FootballStatId (see the catalog vocab).

    Example:
        >>> yahoo_cfb_player_season_stats_legacy(season=2024, category="Rushing",
        ...                                      sort_stat="RUSHING_YARDS")
    """
    if category not in LEGACY_PLAYER_CATEGORIES:
        raise ValueError(f"category must be one of {LEGACY_PLAYER_CATEGORIES}")
    raw = _shangrila_get(
        f"seasonStatsFootball{category}Ncaaf",
        {
            "season": season,
            "league": "ncaaf",
            "leagueStructure": league_structure,
            "count": count,
            "sortStatId": sort_stat,
        },
        **kwargs,
    )
    if not return_parsed:
        return raw
    rows = _flatten_legacy(raw)
    for r in rows:
        r["season"] = season
        r["category"] = category
    return _frame(rows, return_as_pandas)


def yahoo_cfb_team_season_stats_legacy(
    season: int = 2024,
    category: str = "Passing",
    sort_stat: str = "PASSING_YARDS",
    *,
    league_structure: str = "ncaaf.struct.div.1",
    count: int = 200,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """Yahoo CFB legacy per-category team stats (one wide row per team).

    Endpoint: ``GET .../shangrila/seasonTeamStatsFootball{Category}``
    ``category`` in {Passing, Rushing, Receiving, Defense, Kicking, Punting,
    Returns, Kickoffs, Offense}.

    Example:
        >>> yahoo_cfb_team_season_stats_legacy(season=2024, category="Rushing",
        ...                                    sort_stat="RUSHING_YARDS")
    """
    if category not in LEGACY_TEAM_CATEGORIES:
        raise ValueError(f"category must be one of {LEGACY_TEAM_CATEGORIES}")
    raw = _shangrila_get(
        f"seasonTeamStatsFootball{category}",
        {
            "season": season,
            "league": "ncaaf",
            "leagueStructure": league_structure,
            "count": count,
            "sortStatId": sort_stat,
        },
        **kwargs,
    )
    if not return_parsed:
        return raw
    rows = _flatten_legacy(raw)
    for r in rows:
        r["season"] = season
        r["category"] = category
    return _frame(rows, return_as_pandas)


def _flatten_editorial_map(payload: Dict, *keys: str) -> List[Dict]:
    """service.<keys...> dynamic-id map -> list of its values (rows)."""
    node = payload.get("service", {})
    for k in keys:
        node = node.get(k, {})
    return list((node or {}).values())


def yahoo_cfb_scoreboard(
    season: int,
    week: int = 1,
    *,
    count: int = 500,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """Yahoo CFB scoreboard (one row per game).

    Endpoint: ``GET .../editorial/s/scoreboard?leagues=ncaaf&week=...&season=...``
    The full payload also carries teams/leagues/odds maps (use ``return_parsed=False``).

    Example:
        >>> yahoo_cfb_scoreboard(season=2024, week=1)
    """
    raw = _editorial_get(
        "scoreboard",
        {"leagues": "ncaaf", "week": week, "season": season, "count": count, "v": 2},
        **kwargs,
    )
    if not return_parsed:
        return raw
    rows = _flatten_editorial_map(raw, "scoreboard", "games")
    for r in rows:
        r["season"] = season
        r["week"] = week
    return _frame(rows, return_as_pandas)


def yahoo_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """Yahoo CFB boxscore (SCAFFOLD).

    Endpoint: ``GET .../editorial/s/boxscore/{game_id}?v=4``

    The editorial boxscore uses a normalized decoder-dictionary schema
    (``player_stats[playerId][variation][stat_type]=value`` joined against
    ``stat_types``/``stat_categories``). Full decoding is a follow-up; for now
    this returns the raw JSON ``Dict`` so callers can access it.

    TODO: implement the player_stats/team_stats decode + gameplay_by_play flatten.

    Example:
        >>> yahoo_cfb_boxscore("ncaaf.g.202509200023", return_parsed=False)
    """
    raw = _editorial_get(f"boxscore/{game_id}", {"v": 4}, **kwargs)
    # TODO(scaffold): decode service.boxscore.player_stats via stat_types.
    return raw
