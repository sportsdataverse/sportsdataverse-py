"""Yahoo Sports college-football wrappers (hand-written ext).

Read-only wrappers over Yahoo's shangrila stats graph
(``graphite-secure.sports.yahoo.com/v1/query/shangrila``). Reverse-engineering
notes + per-host OpenAPI specs are kept in the maintainers' private reference
notes. NCAAF vertical slice. No auth: the host only requires Origin/Referer
headers.

Yahoo retired its editorial feed (``api-secure.sports.yahoo.com/v1/editorial/s``)
on 2026-11-20 -- every league answers 404 -- so the scoreboard, team directory
and box score that used it now read shangrila queries instead.

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

SHANGRILA_BASE = "https://graphite-secure.sports.yahoo.com/v1/query/shangrila"
_HEADERS = {"Origin": "https://sports.yahoo.com", "Referer": "https://sports.yahoo.com/"}

# valid legacy categories (from a crawl of the shangrila category catalog)
LEGACY_PLAYER_CATEGORIES = ("Passing", "Rushing", "Receiving", "Defense", "Kicking", "Punting", "Returns")
LEGACY_TEAM_CATEGORIES = (*LEGACY_PLAYER_CATEGORIES, "Kickoffs", "Offense")


def _clean(name: Any) -> str:
    """Slugify a Yahoo statId into a snake_case column name."""
    return re.sub(r"\W+", "_", str(name)).strip("_").lower() or "v"


def _shangrila_get(query_name: str, params: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
    """GET a shangrila persisted query, merging the common locale params."""
    merged = {"lang": "en-US", "region": "US", "tz": "America/Chicago", **params}
    return _get(f"{SHANGRILA_BASE}/{query_name}", params=merged, headers=_HEADERS, **kwargs)


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

    ``strict=False`` is required because Yahoo payloads can mix value types
    within a field across rows (the retired editorial scoreboard sent
    ``last_updated`` as a timestamp string or ``False``); strict construction
    raises ``unexpected value while building Series`` on such a mix.
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
        AssetFetchError: The fetch failed (a non-2xx answer or a connection failure
            after retries, or an empty or unreadable 200 body).

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
        AssetFetchError: The fetch failed (a non-2xx answer or a connection failure
            after retries, or an empty or unreadable 200 body).

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


#: scoreboardGame takes a comma-joined ``gameId`` list; a full week (~140 games,
#: ~3 KB of URL) answers in one call, so chunk only well past that.
_SCOREBOARD_CHUNK = 150

_SCOREBOARD_SCHEMA: Dict[str, Any] = {
    "game_id": pl.Utf8,
    "display_name": pl.Utf8,
    "start_time": pl.Utf8,
    "start_date": pl.Utf8,
    "status": pl.Utf8,
    "status_display_name": pl.Utf8,
    "season": pl.Int64,
    "season_phase": pl.Utf8,
    "week": pl.Int64,
    "home_team_id": pl.Utf8,
    "home_team": pl.Utf8,
    "home_abbreviation": pl.Utf8,
    "home_score": pl.Int64,
    "away_team_id": pl.Utf8,
    "away_team": pl.Utf8,
    "away_abbreviation": pl.Utf8,
    "away_score": pl.Int64,
    "winning_team_id": pl.Utf8,
    "display_result": pl.Utf8,
    "venue": pl.Utf8,
}


def _games(payload: Any) -> List[Dict[str, Any]]:
    """``data.games`` of a shangrila game query, or ``[]``."""
    data = payload.get("data") if isinstance(payload, dict) else None
    games = data.get("games") if isinstance(data, dict) else None
    return [g for g in games or [] if isinstance(g, dict)]


def _scoreboard_row(game: Dict[str, Any]) -> Dict[str, Any]:
    """One scoreboardGame entry -> one flat scoreboard row."""
    home = game.get("homeTeam") or {}
    away = game.get("awayTeam") or {}
    return {
        "game_id": game.get("gameId"),
        "display_name": game.get("displayName"),
        "start_time": game.get("startTime"),
        "start_date": game.get("startDate"),
        "status": game.get("status"),
        "status_display_name": game.get("statusDisplayName"),
        "season": game.get("season"),
        "season_phase": game.get("seasonPhase"),
        "week": game.get("week"),
        "home_team_id": game.get("homeTeamId"),
        "home_team": home.get("fullName") or home.get("displayName"),
        "home_abbreviation": home.get("abbreviation"),
        "home_score": game.get("homeScore"),
        "away_team_id": game.get("awayTeamId"),
        "away_team": away.get("fullName") or away.get("displayName"),
        "away_abbreviation": away.get("abbreviation"),
        "away_score": game.get("awayScore"),
        "winning_team_id": game.get("winningTeamId"),
        "display_result": game.get("displayResult"),
        "venue": (game.get("venue") or {}).get("displayName"),
    }


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

    Two shangrila calls: ``leagueGameIdsByDate`` lists the week's game ids
    (every division Yahoo carries, FBS through NAIA), then ``scoreboardGame``
    fetches those games in one comma-joined batch. ``season`` is required --
    there is no meaningful default for a weekly scoreboard. Postseason games
    carry on the week count (bowls and the CFP run to about week 21); a week
    with no games returns zero rows. Unscheduled postseason slots are listed
    too, as ``TBD @ TBD`` games whose team ids are both ``ncaaf.t.0``.

    Yahoo retired the editorial ``scoreboard`` this used to wrap on
    2026-11-20. Shangrila has no counterpart for its ``global_gameid``, and
    the columns are new: they are listed below.

    Args:
        season: Season year (required).
        week: Schedule week number. Defaults to ``1``.
        count: Ignored. Kept so existing calls still work; ``leagueGameIdsByDate``
            returns the whole week.
        return_parsed: If ``True`` (default) flatten the games to a
            DataFrame; if ``False`` return ``{"data": {"games": [...]}}``, the
            merged raw ``scoreboardGame`` entries (odds, records and venue
            included).
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default; pandas when ``return_as_pandas=True``)
        with one row per game, and zero rows with the same columns for an
        empty week:

        | Column | Type | Description |
        |---|---|---|
        | ``game_id`` | Utf8 | Dotted Yahoo game id (``ncaaf.g.202508230021``). |
        | ``display_name`` | Utf8 | ``"Away @ Home"`` label. |
        | ``start_time`` | Utf8 | Kickoff, ISO-8601 with the US Central offset; null while the time is TBA. |
        | ``start_date`` | Utf8 | Kickoff date with offset (``2025-08-23-05:00``). |
        | ``status`` | Utf8 | ``PREGAME``, ``FINAL``, or an in-game status while live. |
        | ``status_display_name`` | Utf8 | Display status (``Final``). |
        | ``season`` | Int64 | Season year. |
        | ``season_phase`` | Utf8 | ``REGULAR_SEASON`` or ``POSTSEASON``. |
        | ``week`` | Int64 | Yahoo week number. |
        | ``home_team_id`` | Utf8 | Dotted home team id (``ncaaf.t.<n>``). |
        | ``home_team`` | Utf8 | Home team full name (``Kansas State Wildcats``). |
        | ``home_abbreviation`` | Utf8 | Home team abbreviation. |
        | ``home_score`` | Int64 | Home points; null before kickoff. |
        | ``away_team_id`` | Utf8 | Dotted away team id. |
        | ``away_team`` | Utf8 | Away team full name. |
        | ``away_abbreviation`` | Utf8 | Away team abbreviation. |
        | ``away_score`` | Int64 | Away points; null before kickoff. |
        | ``winning_team_id`` | Utf8 | Dotted id of the winner; null until final. |
        | ``display_result`` | Utf8 | Score line (``24-21``). |
        | ``venue`` | Utf8 | Stadium name. |

    Raises:
        NoDataError: Yahoo answered 404.
        AssetFetchError: The fetch failed (a non-2xx answer or a connection failure
            after retries, or an empty or unreadable 200 body).

    Example:
        Pull week 1 of the 2024 season::

            from sportsdataverse.cfb import yahoo_cfb_scoreboard
            df = yahoo_cfb_scoreboard(season=2024, week=1)
    """
    ids_raw = _shangrila_get("leagueGameIdsByDate", {"leagues": "ncaaf", "season": season, "week": week}, **kwargs)
    leagues = ((ids_raw or {}).get("data") or {}).get("leagues") or [{}]
    ids = [g.get("gameId") for g in (leagues[0] or {}).get("games") or [] if g.get("gameId")]
    games: List[Dict[str, Any]] = []
    for i in range(0, len(ids), _SCOREBOARD_CHUNK):
        chunk = ",".join(ids[i : i + _SCOREBOARD_CHUNK])
        games += _games(_shangrila_get("scoreboardGame", {"gameId": chunk}, **kwargs))
    if not return_parsed:
        return {"data": {"games": games}}
    frame = pl.DataFrame([_scoreboard_row(g) for g in games], schema=_SCOREBOARD_SCHEMA, strict=False)
    return frame.to_pandas() if return_as_pandas else frame


_TEAMS_SCHEMA: Dict[str, Any] = {
    "team_id": pl.Utf8,
    "abbreviation": pl.Utf8,
    "display_name": pl.Utf8,
    "full_name": pl.Utf8,
    "location": pl.Utf8,
    "nickname": pl.Utf8,
    "conference": pl.Utf8,
    "conference_abbreviation": pl.Utf8,
    "conference_id": pl.Utf8,
    "division": pl.Utf8,
    "division_id": pl.Utf8,
    "seatgeek_id": pl.Utf8,
}


def _str_or_none(value: Any) -> Any:
    """Shangrila sends conference/division ids as ints; the columns stay Utf8."""
    return None if value is None else str(value)


def _team_row(team: Dict[str, Any]) -> Dict[str, Any]:
    """One leagueTeams entry -> one team-directory row."""
    conference = team.get("conference") or {}
    division = team.get("division") or {}
    return {
        "team_id": team.get("teamId"),
        "abbreviation": team.get("abbreviation"),
        "display_name": team.get("displayName"),
        "full_name": team.get("fullName"),
        "location": team.get("location"),
        "nickname": team.get("nickname"),
        "conference": conference.get("name"),
        "conference_abbreviation": None,  # shangrila carries no conference abbreviation
        "conference_id": _str_or_none(conference.get("conferenceId")),
        "division": division.get("name"),
        "division_id": _str_or_none(division.get("divisionId")),
        "seatgeek_id": None,  # shangrila carries no SeatGeek id
    }


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

    Wraps the shangrila ``leagueTeams`` query (``league=ncaaf``): every team
    Yahoo carries, about 540 across FBS, FCS, D2, D3, NAIA and USCAA, keyed by
    the dotted ``ncaaf.t.<n>`` id. It is the Yahoo side of
    :func:`sportsdataverse.cfb.cfb_teams_crosswalk`.

    Until 2026-11-20 this read the team map embedded in the editorial
    scoreboard (about 265 FBS + FCS teams). Yahoo retired that feed. Shangrila
    has no conference abbreviation and no SeatGeek id, so
    ``conference_abbreviation`` and ``seatgeek_id`` stay as all-null columns,
    and ``conference_id`` / ``division_id`` are shangrila's own ids (ACC is
    ``"1"``), not the editorial ones.

    Args:
        season: Season year. Sent to Yahoo, but ``leagueTeams`` answers with
            the current directory for any season (2004 and 2025 return the same
            ids).
        week: Ignored. Kept so existing calls still work.
        return_parsed: If ``True`` (default) project the teams to a
            DataFrame; if ``False`` return the raw ``leagueTeams`` JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default; pandas when ``return_as_pandas=True``)
        with one row per team, every column ``Utf8``:

        | Column | Type | Description |
        |---|---|---|
        | ``team_id`` | Utf8 | Dotted Yahoo team id (``ncaaf.t.1``). |
        | ``abbreviation`` | Utf8 | Team abbreviation (``CLEM``). |
        | ``display_name`` | Utf8 | Short name (``Clemson``). |
        | ``full_name`` | Utf8 | School and nickname (``Clemson Tigers``). |
        | ``location`` | Utf8 | School / place name. |
        | ``nickname`` | Utf8 | Team nickname (``Tigers``). |
        | ``conference`` | Utf8 | Conference name (``Atlantic Coast``). |
        | ``conference_abbreviation`` | Utf8 | Always null: shangrila carries none. |
        | ``conference_id`` | Utf8 | Shangrila conference id. |
        | ``division`` | Utf8 | ``FBS``, ``FCS``, ``D2``, ``D3``, ``NAIA`` or ``USCAA``. |
        | ``division_id`` | Utf8 | Shangrila division id (``"1"`` is FBS). |
        | ``seatgeek_id`` | Utf8 | Always null: shangrila carries none. |

    Raises:
        NoDataError: Yahoo answered 404.
        AssetFetchError: The fetch failed (a non-2xx answer or a connection failure
            after retries, or an empty or unreadable 200 body).

    Example:
        Build a dotted-id -> abbreviation lookup::

            from sportsdataverse.cfb import yahoo_cfb_teams
            teams = yahoo_cfb_teams(season=2024)
            abbr = dict(zip(teams["team_id"], teams["abbreviation"]))
    """
    raw = _shangrila_get("leagueTeams", {"league": "ncaaf", "season": season}, **kwargs)
    if not return_parsed:
        return raw
    leagues = ((raw or {}).get("data") or {}).get("leagues") or [{}]
    teams = [t for t in (leagues[0] or {}).get("teams") or [] if isinstance(t, dict)]
    frame = pl.DataFrame([_team_row(t) for t in teams], schema=_TEAMS_SCHEMA)
    return frame.to_pandas() if return_as_pandas else frame


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


#: playbookBoxscore statId prefix -> the editorial box score's category name.
_STAT_CATEGORY = {
    "PASSING": "Passing",
    "RUSHING": "Rushing",
    "RECEIVING": "Receiving",
    "RECEPTIONS": "Receiving",
    "SOLO": "Defense",
    "TACKLE": "Defense",
    "SACKS": "Defense",
    "FIELD": "Kicking",
    "EXTRA": "Kicking",
}


def _boxscore_rows(raw: Any) -> List[Dict[str, Any]]:
    """Decode playbookBoxscore lineups into one row per player stat.

    ``games[0].{home,away}TeamLineup[].player.playerGameStats[].stats[]`` holds
    ``{statId, value}`` pairs; names come from ``data.footballStatTypes``. Null
    values (a stat that does not apply to the player) are dropped.
    """
    data = raw.get("data") if isinstance(raw, dict) else None
    if not isinstance(data, dict):
        return []
    game = (_games(raw) or [{}])[0]
    types = {t.get("statId"): t for t in data.get("footballStatTypes") or [] if isinstance(t, dict)}
    rows: List[Dict[str, Any]] = []
    for side in ("home", "away"):
        side_team = game.get(f"{side}TeamId")
        for entry in game.get(f"{side}TeamLineup") or []:
            player = (entry or {}).get("player") or {}
            for block in player.get("playerGameStats") or []:
                for stat in (block or {}).get("stats") or []:
                    stat_id, value = stat.get("statId"), stat.get("value")
                    if not stat_id or value is None:
                        continue
                    meta = types.get(stat_id) or {}
                    rows.append(
                        {
                            "game_id": game.get("gameId"),
                            "team_id": player.get("teamId") or side_team,
                            "home_away": side,
                            "player_id": player.get("playerId"),
                            "stat_category": _STAT_CATEGORY.get(stat_id.split("_")[0]),
                            "stat_type_id": stat_id,
                            "stat_name": meta.get("displayName"),
                            "stat_abbreviation": meta.get("abbreviation"),
                            "stat_variation": "Game",
                            "value": str(value),
                        }
                    )
    return rows


@overload
def yahoo_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def yahoo_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def yahoo_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def yahoo_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Yahoo CFB box score: player stats from the game lineups, one row per player stat.

    Wraps the shangrila ``playbookBoxscore`` query. Each side's lineup lists
    the players Yahoo features, each with up to 12 headline stats (passing,
    rushing and receiving yards and touchdowns, receptions, tackles, sacks,
    field goals and extra points made); names come from the payload's
    ``footballStatTypes``. Pivot on ``stat_type_id`` for a wide box.

    Yahoo retired the editorial ``boxscore`` this used to wrap on 2026-11-20.
    Shangrila has no full box score: there are no team-stat rows, a lineup
    covers about a dozen players per side, and stat ids are shangrila names
    (``PASSING_YARDS``) rather than ``ncaaf.stat_type.<n>``. Pass
    ``return_parsed=False`` for the raw payload, which also carries drives,
    play-by-play and the win-probability timeline.

    Args:
        game_id: Dotted Yahoo game id (e.g. ``"ncaaf.g.202509200023"``).
        return_parsed: If ``True`` (default) decode the box score into a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame by default (pandas when ``return_as_pandas=True``)
        with one row per player stat, every column ``Utf8``, and zero rows
        (same columns) for an empty payload; the raw ``playbookBoxscore`` JSON
        ``dict`` when ``return_parsed=False``:

        | Column | Type | Description |
        |---|---|---|
        | ``game_id`` | Utf8 | Dotted Yahoo game id (``ncaaf.g.<date><n>``). |
        | ``team_id`` | Utf8 | Dotted Yahoo team id (``ncaaf.t.<n>``). |
        | ``home_away`` | Utf8 | ``"home"`` or ``"away"``. |
        | ``player_id`` | Utf8 | Dotted Yahoo player id (``ncaaf.p.<n>``). |
        | ``stat_category`` | Utf8 | ``Passing``, ``Rushing``, ``Receiving``, ``Defense`` or ``Kicking``. |
        | ``stat_type_id`` | Utf8 | Shangrila stat id (``PASSING_YARDS``). |
        | ``stat_name`` | Utf8 | Stat name (``Passing Yards``). |
        | ``stat_abbreviation`` | Utf8 | Short stat label. |
        | ``stat_variation`` | Utf8 | Always ``Game``. |
        | ``value`` | Utf8 | Stat value as Yahoo sends it (``"188"``). |

    Raises:
        NoDataError: Yahoo answered 404.
        AssetFetchError: The fetch failed (a non-2xx answer or a connection failure
            after retries, or an empty or unreadable 200 body).

    Example:
        Decoded box score for one game::

            from sportsdataverse.cfb import yahoo_cfb_boxscore
            box = yahoo_cfb_boxscore("ncaaf.g.202509200023")

        Raw JSON (includes drives and play-by-play)::

            raw = yahoo_cfb_boxscore("ncaaf.g.202509200023", return_parsed=False)

        Wide player box (one line)::

            box.pivot("stat_type_id", index="player_id", values="value")
    """
    raw = _shangrila_get("playbookBoxscore", {"gameId": game_id, "isFootball": "true"}, **kwargs)
    if not return_parsed:
        return raw
    frame = pl.DataFrame(_boxscore_rows(raw), schema=_BOXSCORE_SCHEMA)
    return frame.to_pandas() if return_as_pandas else frame
