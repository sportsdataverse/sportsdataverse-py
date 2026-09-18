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
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Union, overload

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

from sportsdataverse._codegen_runtime import _get

__all__ = [
    "yahoo_cfb_boxscore",
    "yahoo_cfb_player_season_stats",
    "yahoo_cfb_player_season_stats_legacy",
    "yahoo_cfb_scoreboard",
    "yahoo_cfb_teams",
    "yahoo_cfb_team_season_stats",
    "yahoo_cfb_team_season_stats_legacy",
]

EDITORIAL_BASE = "https://api-secure.sports.yahoo.com/v1/editorial/s"
SHANGRILA_BASE = "https://graphite-secure.sports.yahoo.com/v1/query/shangrila"
_HEADERS = {"Origin": "https://sports.yahoo.com", "Referer": "https://sports.yahoo.com/"}

# valid legacy categories (from sdv-internal-refs catalog crawl, Pass A)
LEGACY_PLAYER_CATEGORIES = ("Passing", "Rushing", "Receiving", "Defense", "Kicking", "Punting", "Returns")
LEGACY_TEAM_CATEGORIES = (*LEGACY_PLAYER_CATEGORIES, "Kickoffs", "Offense")


def _clean(name: Any) -> str:
    """Slugify a Yahoo statId into a snake_case column name."""
    return re.sub(r"\W+", "_", str(name)).strip("_").lower() or "v"


def _shangrila_get(query_name: str, params: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
    """GET a shangrila persisted query, merging the common locale params."""
    merged = {"lang": "en-US", "region": "US", "tz": "America/Chicago", **params}
    return _get(f"{SHANGRILA_BASE}/{query_name}", params=merged, headers=_HEADERS, **kwargs)


def _editorial_get(path: str, params: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
    """GET an editorial resource, merging the common locale params."""
    merged = {"lang": "en-US", "region": "US", "tz": "America/Chicago", **params}
    return _get(f"{EDITORIAL_BASE}/{path}", params=merged, headers=_HEADERS, **kwargs)


def _entity_cols(row: Dict[str, Any]) -> Dict[str, Any]:
    """player|team header -> flat id/name columns."""
    out: Dict[str, Any] = {}
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


def _flatten_modern(payload: Dict[str, Any], sport_key: str) -> List[Dict[str, Any]]:
    """data.leagues[0].<sport_key>[] -> wide rows (one column per statId)."""
    leagues = (payload.get("data") or {}).get("leagues") or [{}]
    rows_in = leagues[0].get(sport_key, []) if leagues else []
    out: List[Dict[str, Any]] = []
    for row in rows_in:
        rec = _entity_cols(row)
        for s in row.get("stats", []) or []:
            sid = s.get("statId")
            if sid:
                rec[_clean(sid)] = s.get("value")
        out.append(rec)
    return out


def _flatten_legacy(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """data.leagues[0].leaders[] -> wide rows (one column per statId)."""
    leagues = (payload.get("data") or {}).get("leagues") or [{}]
    leaders = leagues[0].get("leaders", []) if leagues else []
    out: List[Dict[str, Any]] = []
    for row in leaders:
        rec = _entity_cols(row)
        for s in row.get("stats", []) or []:
            sid = s.get("statId")
            if sid:
                rec[_clean(sid)] = s.get("value")
        out.append(rec)
    return out


def _frame(rows: List[Dict[str, Any]], return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Materialize flattened rows as a polars (default) or pandas DataFrame.

    ``strict=False`` is required because Yahoo's scoreboard mixes value types
    within a field across games (e.g. ``last_updated`` is a timestamp string for
    played games but ``False`` for unplayed ones); strict construction raises
    ``unexpected value while building Series`` on that bool/str mix.
    """
    if return_as_pandas:
        import pandas as pd

        return pd.DataFrame(rows)
    return pl.DataFrame(rows, strict=False)


@overload
def yahoo_cfb_player_season_stats(
    season: int = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    qualified: bool = ...,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def yahoo_cfb_player_season_stats(
    season: int = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    qualified: bool = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def yahoo_cfb_player_season_stats(
    season: int = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    qualified: bool = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def yahoo_cfb_player_season_stats(
    season: int = 2024,
    *,
    league_structure: str = "ncaaf.struct.div.1",
    count: int = 200,
    qualified: bool = False,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Yahoo CFB player season stats (modern; one wide row per player).

    Wraps the shangrila ``leagueStatsIndividual`` query, which returns every
    stat group (passing/rushing/receiving/...) in one call, pivoted wide with
    one column per ``statId``. NCAAF data is available 2013-present.

    Args:
        season: Season year (2013-present). Defaults to ``2024``.
        league_structure: Yahoo league-structure id (division filter).
            Defaults to ``"ncaaf.struct.div.1"`` (FBS).
        count: Maximum number of players to request. Defaults to ``200``.
        qualified: Restrict to qualified leaders only. Defaults to ``False``.
        return_parsed: If ``True`` (default) flatten to a DataFrame; if
            ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A wide polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``. Includes a self-describing ``season`` column.

    Raises:
        requests.exceptions.RequestException: Propagated from the underlying
            HTTP request on a network/transport failure.

    Example:
        Pull the 2024 player leaders as a polars frame::

            from sportsdataverse.cfb import yahoo_cfb_player_season_stats
            df = yahoo_cfb_player_season_stats(season=2024)
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


@overload
def yahoo_cfb_team_season_stats(
    season: int = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def yahoo_cfb_team_season_stats(
    season: int = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def yahoo_cfb_team_season_stats(
    season: int = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def yahoo_cfb_team_season_stats(
    season: int = 2024,
    *,
    league_structure: str = "ncaaf.struct.div.1",
    count: int = 200,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Yahoo CFB team season stats (modern; one wide row per team).

    Wraps the shangrila ``leagueStatsByTeam`` query (all stat groups in one
    call, pivoted wide with one column per ``statId``).

    Args:
        season: Season year (2013-present). Defaults to ``2024``.
        league_structure: Yahoo league-structure id (division filter).
            Defaults to ``"ncaaf.struct.div.1"`` (FBS).
        count: Maximum number of teams to request. Defaults to ``200``.
        return_parsed: If ``True`` (default) flatten to a DataFrame; if
            ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A wide polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``. Includes a self-describing ``season`` column.

    Raises:
        requests.exceptions.RequestException: Propagated from the underlying
            HTTP request on a network/transport failure.

    Example:
        Pull the 2024 team stats as a polars frame::

            from sportsdataverse.cfb import yahoo_cfb_team_season_stats
            df = yahoo_cfb_team_season_stats(season=2024)
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


@overload
def yahoo_cfb_player_season_stats_legacy(
    season: int = ...,
    category: str = ...,
    sort_stat: str = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def yahoo_cfb_player_season_stats_legacy(
    season: int = ...,
    category: str = ...,
    sort_stat: str = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def yahoo_cfb_player_season_stats_legacy(
    season: int = ...,
    category: str = ...,
    sort_stat: str = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def yahoo_cfb_player_season_stats_legacy(
    season: int = 2024,
    category: str = "Passing",
    sort_stat: str = "PASSING_YARDS",
    *,
    league_structure: str = "ncaaf.struct.div.1",
    count: int = 200,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Yahoo CFB legacy per-category player leaders (one wide row per player).

    Wraps the legacy ``seasonStatsFootball{Category}Ncaaf`` query (one stat
    category per call), pivoted wide with one column per ``statId``.

    Args:
        season: Season year (2013-present). Defaults to ``2024``.
        category: Stat category, one of
            ``{"Passing", "Rushing", "Receiving", "Defense", "Kicking",
            "Punting", "Returns"}``. Defaults to ``"Passing"``.
        sort_stat: Required ``FootballStatId`` to sort by (see the catalog
            vocab). Defaults to ``"PASSING_YARDS"``.
        league_structure: Yahoo league-structure id (division filter).
            Defaults to ``"ncaaf.struct.div.1"`` (FBS).
        count: Maximum number of players to request. Defaults to ``200``.
        return_parsed: If ``True`` (default) flatten to a DataFrame; if
            ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A wide polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``. Includes self-describing ``season`` and
        ``category`` columns.

    Raises:
        ValueError: ``category`` is not one of ``LEGACY_PLAYER_CATEGORIES``.

    Example:
        Pull the 2024 rushing leaders::

            from sportsdataverse.cfb import yahoo_cfb_player_season_stats_legacy
            df = yahoo_cfb_player_season_stats_legacy(
                season=2024, category="Rushing", sort_stat="RUSHING_YARDS"
            )
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


@overload
def yahoo_cfb_team_season_stats_legacy(
    season: int = ...,
    category: str = ...,
    sort_stat: str = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def yahoo_cfb_team_season_stats_legacy(
    season: int = ...,
    category: str = ...,
    sort_stat: str = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def yahoo_cfb_team_season_stats_legacy(
    season: int = ...,
    category: str = ...,
    sort_stat: str = ...,
    *,
    league_structure: str = ...,
    count: int = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def yahoo_cfb_team_season_stats_legacy(
    season: int = 2024,
    category: str = "Passing",
    sort_stat: str = "PASSING_YARDS",
    *,
    league_structure: str = "ncaaf.struct.div.1",
    count: int = 200,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Yahoo CFB legacy per-category team stats (one wide row per team).

    Wraps the legacy ``seasonTeamStatsFootball{Category}`` query (one stat
    category per call), pivoted wide with one column per ``statId``.

    Args:
        season: Season year (2013-present). Defaults to ``2024``.
        category: Stat category, one of
            ``{"Passing", "Rushing", "Receiving", "Defense", "Kicking",
            "Punting", "Returns", "Kickoffs", "Offense"}``. Defaults to
            ``"Passing"``.
        sort_stat: Required ``FootballStatId`` to sort by. Defaults to
            ``"PASSING_YARDS"``.
        league_structure: Yahoo league-structure id (division filter).
            Defaults to ``"ncaaf.struct.div.1"`` (FBS).
        count: Maximum number of teams to request. Defaults to ``200``.
        return_parsed: If ``True`` (default) flatten to a DataFrame; if
            ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A wide polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``. Includes self-describing ``season`` and
        ``category`` columns.

    Raises:
        ValueError: ``category`` is not one of ``LEGACY_TEAM_CATEGORIES``.

    Example:
        Pull the 2024 team rushing stats::

            from sportsdataverse.cfb import yahoo_cfb_team_season_stats_legacy
            df = yahoo_cfb_team_season_stats_legacy(
                season=2024, category="Rushing", sort_stat="RUSHING_YARDS"
            )
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


def _flatten_editorial_map(payload: Dict[str, Any], *keys: str) -> List[Dict[str, Any]]:
    """service.<keys...> dynamic-id map -> list of its values (rows)."""
    node: Any = payload.get("service", {})
    for k in keys:
        node = node.get(k, {})
    return list((node or {}).values())


@overload
def yahoo_cfb_scoreboard(
    season: int,
    week: int = ...,
    *,
    count: int = ...,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def yahoo_cfb_scoreboard(
    season: int,
    week: int = ...,
    *,
    count: int = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def yahoo_cfb_scoreboard(
    season: int,
    week: int = ...,
    *,
    count: int = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def yahoo_cfb_scoreboard(
    season: int,
    week: int = 1,
    *,
    count: int = 500,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Yahoo CFB scoreboard (one row per game).

    Wraps the editorial ``scoreboard`` resource and flattens the ``games`` map.
    ``season`` is required — there is no meaningful default for a weekly
    scoreboard and the API has no concept of "current season". The full raw
    payload also carries teams/leagues/odds maps (use ``return_parsed=False``).

    Args:
        season: Season year (required).
        week: Schedule week number. Defaults to ``1``.
        count: Maximum number of games to request. Defaults to ``500``.
        return_parsed: If ``True`` (default) flatten the games map to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default) with one row per game, a pandas DataFrame
        when ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``. Includes self-describing ``season`` and
        ``week`` columns.

    Raises:
        requests.exceptions.RequestException: Propagated from the underlying
            HTTP request on a network/transport failure.

    Example:
        Pull week 1 of the 2024 season::

            from sportsdataverse.cfb import yahoo_cfb_scoreboard
            df = yahoo_cfb_scoreboard(season=2024, week=1)
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


_YAHOO_TEAM_FIELDS = (
    ("team_id", "team_id"),
    ("abbreviation", "abbr"),
    ("display_name", "display_name"),
    ("full_name", "full_name"),
    ("location", "first_name"),
    ("nickname", "last_name"),
    ("conference", "conference"),
    ("conference_abbreviation", "conference_abbr"),
    ("conference_id", "conference_id"),
    ("division", "division"),
    ("division_id", "division_id"),
    ("seatgeek_id", "seatgeek_id"),
)


@overload
def yahoo_cfb_teams(
    season: int,
    week: int = ...,
    *,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def yahoo_cfb_teams(
    season: int,
    week: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def yahoo_cfb_teams(
    season: int,
    week: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def yahoo_cfb_teams(
    season: int,
    week: int = 1,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Yahoo CFB team directory (one row per team).

    Yahoo has no standalone teams resource (the documented
    ``sports.league.teams`` resource 404s without auth). Instead the editorial
    ``scoreboard`` payload is "fat": one call embeds the full ~186-team
    directory under ``service.scoreboard.teams`` keyed by the dotted
    ``ncaaf.t.<id>`` team id. This wrapper pulls that map for the requested
    ``(season, week)`` and projects it to the directory columns -- it is the
    Yahoo side of :func:`sportsdataverse.cfb.cfb_teams_crosswalk`.

    Args:
        season: Season year (required; the scoreboard is fetched to obtain the
            embedded teams map).
        week: Schedule week used to fetch the scoreboard. Defaults to ``1``.
            The embedded directory is the full league list regardless of week.
        return_parsed: If ``True`` (default) flatten the teams map to a
            DataFrame; if ``False`` return the raw scoreboard JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default) with one row per team -- columns
        ``team_id``, ``abbreviation``, ``display_name``, ``full_name``,
        ``location``, ``nickname``, ``conference``,
        ``conference_abbreviation``, ``conference_id``, ``division``,
        ``division_id``, ``seatgeek_id`` -- a pandas DataFrame when
        ``return_as_pandas=True``, or the raw scoreboard JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        requests.exceptions.RequestException: Propagated from the underlying
            HTTP request on a network/transport failure.

    Example:
        Build a dotted-id -> abbreviation lookup::

            from sportsdataverse.cfb import yahoo_cfb_teams
            teams = yahoo_cfb_teams(season=2024)
            abbr = dict(zip(teams["team_id"], teams["abbreviation"]))
    """
    raw = _editorial_get(
        "scoreboard",
        {"leagues": "ncaaf", "week": week, "season": season, "count": 500, "v": 2},
        **kwargs,
    )
    if not return_parsed:
        return raw
    teams_map = ((raw.get("service") or {}).get("scoreboard") or {}).get("teams") or {}
    rows: List[Dict[str, Any]] = []
    for tid, team in teams_map.items():
        row = {out: team.get(src) for out, src in _YAHOO_TEAM_FIELDS}
        row["team_id"] = team.get("team_id") or tid  # fall back to the map key
        rows.append(row)
    return _frame(rows, return_as_pandas)


_BOXSCORE_SCHEMA: Dict[str, Any] = {
    "game_id": pl.Utf8,
    "team_id": pl.Utf8,
    "home_away": pl.Utf8,
    "player_id": pl.Utf8,
    "stat_category": pl.Utf8,
    "stat_type_id": pl.Utf8,
    "stat_name": pl.Utf8,
    "stat_abbreviation": pl.Utf8,
    "stat_variation": pl.Utf8,
    "value": pl.Utf8,
}


def _boxscore_rows(raw: Any) -> List[Dict[str, Any]]:
    """Decode ``service.boxscore`` team/player stat maps into one row per entity stat.

    ``{team_stats|player_stats}[entity][variation][stat_type] = value`` is joined
    against the ``stat_types`` / ``stat_categories`` / ``stat_variations``
    dictionaries; a player's team comes from the game's home/away lineups.
    """
    service = raw.get("service") if isinstance(raw, dict) else None
    box = service.get("boxscore") if isinstance(service, dict) else None
    if not isinstance(box, dict):
        return []
    games = box.get("games") or {}
    game_id = next(iter(games), None)
    game = games.get(game_id) or {}
    lineups = (box.get("gamelineups") or {}).get(game_id) or {}
    side_of: Dict[str, str] = {}
    team_of: Dict[str, Any] = {}
    for side in ("home", "away"):
        team_id = game.get(f"{side}_team_id")
        if team_id:
            side_of[team_id] = side
            team_of[team_id] = team_id
        for player_id in (lineups.get(f"{side}_lineup") or {}).get("all") or {}:
            side_of[player_id] = side
            team_of[player_id] = team_id
    types = box.get("stat_types") or {}
    variations = box.get("stat_variations") or {}
    category_of = {
        stat: category.get("name")
        for category in (box.get("stat_categories") or {}).values()
        for stat in (category.get("stats") or [])
    }
    rows: List[Dict[str, Any]] = []
    for kind in ("team_stats", "player_stats"):
        for entity_id, by_variation in (box.get(kind) or {}).items():
            for variation, stats in (by_variation or {}).items():
                for stat_id, value in (stats or {}).items():
                    stat = types.get(stat_id) or {}
                    rows.append(
                        {
                            "game_id": game_id,
                            "team_id": team_of.get(entity_id),
                            "home_away": side_of.get(entity_id),
                            "player_id": entity_id if kind == "player_stats" else None,
                            "stat_category": category_of.get(stat_id),
                            "stat_type_id": stat_id,
                            "stat_name": stat.get("name"),
                            "stat_abbreviation": stat.get("short_name"),
                            "stat_variation": (variations.get(variation) or {}).get("name"),
                            "value": None if value is None else str(value),
                        }
                    )
    return rows


@overload
def yahoo_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[False] = ...,
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def yahoo_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True],
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def yahoo_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True],
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def yahoo_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Yahoo CFB box score: team and player stats, one row per entity stat.

    Wraps the editorial ``boxscore/{game_id}`` resource. Its box score is a
    decoder-dictionary schema
    (``player_stats[playerId][variation][stat_type] = value``, same for
    ``team_stats``) that this decodes against the payload's ``stat_types`` /
    ``stat_categories`` dictionaries into a long frame: one row per team stat
    and per player stat. Pivot on ``stat_type_id`` for a wide box. The editorial
    payload carries no player names; a player's team comes from the game's
    home/away lineups. Unlike its siblings this defaults to the raw payload
    (``return_parsed=False``), which also carries play-by-play and drives.

    Args:
        game_id: Dotted Yahoo game id (e.g. ``"ncaaf.g.202509200023"``).
        return_parsed: If ``True``, decode the box score into a DataFrame; if
            ``False`` (the default) return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        The raw editorial boxscore JSON ``dict`` by default. With
        ``return_parsed=True``, a polars DataFrame (pandas when
        ``return_as_pandas=True``) with one row per team or player stat, every
        column ``Utf8``, and zero rows (same columns) for an empty payload:

        | Column | Type | Description |
        |---|---|---|
        | ``game_id`` | Utf8 | Dotted Yahoo game id (``ncaaf.g.<date><n>``). |
        | ``team_id`` | Utf8 | Dotted Yahoo team id (``ncaaf.t.<n>``); null for a player missing from the lineups. |
        | ``home_away`` | Utf8 | ``"home"`` or ``"away"``. |
        | ``player_id`` | Utf8 | Dotted Yahoo player id (``ncaaf.p.<n>``); null on team-stat rows. |
        | ``stat_category`` | Utf8 | ``Passing``, ``Rushing``, ``Receiving``, ``Kicking``, ``Returns``, ``Punting``, ``Defense`` or ``Team``. |
        | ``stat_type_id`` | Utf8 | Yahoo stat type id (``ncaaf.stat_type.105``). |
        | ``stat_name`` | Utf8 | Stat name (``Yards``, ``Third Down Efficiency``). |
        | ``stat_abbreviation`` | Utf8 | Short stat label (``Yds``, ``3DE``). |
        | ``stat_variation`` | Utf8 | Stat variation name (``Game``). |
        | ``value`` | Utf8 | Stat value as Yahoo sends it (``"188"``, ``"73.2"``, ``"1-14"``). |

    Raises:
        requests.exceptions.RequestException: Propagated from the underlying
            HTTP request on a network/transport failure.

    Example:
        Decoded box score for one game::

            from sportsdataverse.cfb import yahoo_cfb_boxscore
            box = yahoo_cfb_boxscore("ncaaf.g.202509200023", return_parsed=True)

        Raw JSON (includes play-by-play and drives)::

            raw = yahoo_cfb_boxscore("ncaaf.g.202509200023")

        Wide team box (one line)::

            box.filter(pl.col("player_id").is_null()).pivot("stat_name", index="team_id", values="value")
    """
    raw = _editorial_get(f"boxscore/{game_id}", {"v": 4}, **kwargs)
    if not return_parsed:
        return raw
    frame = pl.DataFrame(_boxscore_rows(raw), schema=_BOXSCORE_SCHEMA)
    return frame.to_pandas() if return_as_pandas else frame
