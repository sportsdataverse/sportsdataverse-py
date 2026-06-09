"""Parsed ``api.nfl.com`` ``/football/v2`` + ``/experience`` wrappers.

Tidy DataFrame wrappers over the modern NFL.com data surface (standings, rosters,
teams, weeks, combine, draft, injuries, live game summaries, weekly game details).
Every endpoint here was verified to return ``200`` on the anonymous ``WEB_DESKTOP``
bearer token minted by :func:`sportsdataverse.nfl.nfl_games.nfl_headers_gen`.

Auth is shared with :mod:`sportsdataverse.nfl.nfl_games`: each public function takes
an optional ``headers`` arg and defaults to a fresh :func:`nfl_headers_gen` call.
Reuse one header dict across many calls to avoid re-minting tokens::

    from sportsdataverse.nfl.nfl_games import nfl_headers_gen
    from sportsdataverse.nfl.nfl_api import nfl_standings, nfl_rosters
    hdrs = nfl_headers_gen()
    standings = nfl_standings(season=2024, season_type="REG", week=1, headers=hdrs)
    rosters = nfl_rosters(season=2024, headers=hdrs)

Each wrapper returns a tidy **polars** ``DataFrame`` by default (records flattened
with ``pl.json_normalize(..., separator="_", max_level=2)``); pass
``return_as_pandas=True`` for a pandas frame.
"""

from __future__ import annotations

from typing import Dict, List, Optional

import requests

from sportsdataverse.nfl.nfl_games import API_HOST, nfl_headers_gen


def _get(url: str, headers: Optional[Dict[str, str]], params: Optional[Dict] = None) -> Dict:
    """GET a JSON payload from ``api.nfl.com``, minting headers if needed."""
    if headers is None:
        headers = nfl_headers_gen()
    resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _to_frame(records: List, return_as_pandas: bool):
    """Flatten a list of nested dicts into a polars (or pandas) DataFrame."""
    import polars as pl

    df = pl.json_normalize(records or [], separator="_", max_level=2, infer_schema_length=None)
    return df.to_pandas() if return_as_pandas else df


def nfl_standings(
    season: int = 2024,
    season_type: str = "REG",
    week: int = 1,
    limit: int = 40,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` standings -- one row per team (``/football/v2/standings``).

    The payload nests records under ``weeks[].standings[]``; this flattens every
    team standing across the returned week(s) into a single frame with
    ``team_*``, ``overall_*``, ``division_*``, ``conference_*``, ``home_*``,
    ``road_*``, ``last5_*``, and ``clinched`` columns.

    Args:
        season (int): season year (e.g. ``2024``).
        season_type (str): season type. One of ``"PRE"``, ``"REG"``, ``"POST"``.
        week (int): week number whose standings snapshot to return.
        limit (int): max teams per page (default ``40``; there are 32 teams).
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict;
            defaults to a fresh mint.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per team standing.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_standings
        >>> standings = nfl_standings(season=2024, season_type="REG", week=18)
        >>> standings.select(["team_abbreviation", "overall_wins", "overall_losses"]).head()
    """
    payload = _get(
        f"{API_HOST}/football/v2/standings",
        headers,
        {"season": season, "seasonType": season_type, "week": week, "limit": limit},
    )
    records: List = []
    for wk in payload.get("weeks", []) or []:
        records.extend(wk.get("standings", []) or [])
    return _to_frame(records, return_as_pandas)


def nfl_rosters(
    season: int = 2024,
    limit: int = 40,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` team rosters -- one row per team (``/football/v2/rosters``).

    Records live under the ``rosters`` key. Each row is one team's roster for the
    season, carrying ``season``, ``seasonType``, ``team_*`` columns and a nested
    ``persons`` list of players (kept as a list column).

    Args:
        season (int): season year (e.g. ``2024``).
        limit (int): page size -- max rosters to return (this endpoint is
            paginated; default ``40`` covers all teams in one page).
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per team roster.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_rosters
        >>> rosters = nfl_rosters(season=2024, limit=40)
        >>> rosters.select(["team_abbreviation", "season", "seasonType"]).head()
    """
    payload = _get(f"{API_HOST}/football/v2/rosters", headers, {"season": season, "limit": limit})
    return _to_frame(payload.get("rosters", []), return_as_pandas)


def nfl_teams_history(
    season: int = 2024,
    limit: int = 40,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` teams for a season -- one row per team (``/football/v2/teams/history``).

    Records live under the ``teams`` key. Each row carries ``id`` (the uuid team
    id used by :func:`nfl_team`), ``abbreviation``, ``fullName``, ``nickName``,
    ``location``, ``conferenceAbbr``/``divisionFullName``, ``currentLogo`` and the
    nested ``venues`` list.

    Args:
        season (int): season year (e.g. ``2024``).
        limit (int): page size (default ``40``).
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per team.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_teams_history
        >>> teams = nfl_teams_history(season=2024)
        >>> teams.select(["id", "abbreviation", "fullName"]).head()
    """
    payload = _get(f"{API_HOST}/football/v2/teams/history", headers, {"season": season, "limit": limit})
    return _to_frame(payload.get("teams", []), return_as_pandas)


def nfl_team(
    team_id: str,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` single-team detail -- one row (``/football/v2/teams/{id}``).

    The endpoint returns one team object (not a list); this wraps it into a
    one-row frame with ``id``, ``fullName``, ``currentCoach_*``, ``primaryColor``,
    ``secondaryColor``, ``yearEstablished``, ``owners``, ``socials`` and more.

    Args:
        team_id (str): the uuid team id from :func:`nfl_teams_history`
            (e.g. ``'10403800-517c-7b8c-65a3-c61b95d86123'`` for ARI).
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame`` with a single team row.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_team
        >>> team = nfl_team(team_id="10403800-517c-7b8c-65a3-c61b95d86123")
        >>> team.select(["id", "fullName", "yearEstablished"]).head()
    """
    payload = _get(f"{API_HOST}/football/v2/teams/{team_id}", headers)
    return _to_frame([payload] if isinstance(payload, dict) else payload, return_as_pandas)


def nfl_weeks(
    season: int = 2024,
    season_type: str = "REG",
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` week calendar -- one row per week (``/football/v2/weeks/...``).

    Records live under the ``weeks`` key. Each row carries ``season``,
    ``seasonType``, ``week``, ``weekType``, ``dateBegin``, ``dateEnd`` and a
    ``byeTeams`` list.

    Args:
        season (int): season year (e.g. ``2024``).
        season_type (str): season type. One of ``"PRE"``, ``"REG"``, ``"POST"``.
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per week.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_weeks
        >>> weeks = nfl_weeks(season=2024, season_type="REG")
        >>> weeks.select(["week", "weekType", "dateBegin", "dateEnd"]).head()
    """
    payload = _get(f"{API_HOST}/football/v2/weeks/season/{season}/seasonType/{season_type}", headers)
    return _to_frame(payload.get("weeks", []), return_as_pandas)


def nfl_weeks_by_date(
    date: str,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` week-for-a-date -- one row (``/football/v2/weeks/date/{YYYY-MM-DD}``).

    The endpoint returns one week object (not a list); this wraps it into a
    one-row frame with ``season``, ``seasonType``, ``week``, ``weekType``,
    ``dateBegin``, ``dateEnd`` and ``byeTeams``.

    Args:
        date (str): calendar date in ``YYYY-MM-DD`` form (e.g. ``'2024-09-08'``).
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame`` with a single week row.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_weeks_by_date
        >>> wk = nfl_weeks_by_date(date="2024-09-08")
        >>> wk.select(["season", "seasonType", "week"]).head()
    """
    payload = _get(f"{API_HOST}/football/v2/weeks/date/{date}", headers)
    return _to_frame([payload] if isinstance(payload, dict) else payload, return_as_pandas)


def nfl_combine_profiles(
    year: int = 2024,
    limit: int = 40,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` combine profiles -- one row per prospect (``/football/v2/combine/profiles``).

    Records live under the ``combineProfiles`` key. Each row carries ``id``,
    ``year``, the nested ``person`` object, measurables (``armLength``,
    ``benchPress``, ``broadJump``, ``fortyYardDash``, ``handSize``, ``height``,
    ``proFortyYardDash``, ``sixtyYardShuttle``), scout scores and grades.

    Args:
        year (int): combine/draft year (e.g. ``2024``).
        limit (int): page size -- max profiles to return (paginated endpoint).
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per combine profile.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_combine_profiles
        >>> combine = nfl_combine_profiles(year=2024, limit=50)
        >>> combine.select(["id", "year", "fortyYardDash", "benchPress"]).head()
    """
    payload = _get(f"{API_HOST}/football/v2/combine/profiles", headers, {"year": year, "limit": limit})
    return _to_frame(payload.get("combineProfiles", []), return_as_pandas)


def nfl_draft_picks(
    year: int = 2024,
    limit: int = 40,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` draft pick report -- one row per pick (``/football/v2/draft/picks/report``).

    The payload carries draft-state scalars plus a ``days`` list and a ``picks``
    list; the records of interest are under ``picks``. Each row carries ``year``,
    ``draftRound``, ``draftPosition``, ``draftNumberOverall``, ``personId``,
    ``teamId``, ``pickIsIn`` and ``tradeNote``.

    Args:
        year (int): draft year (e.g. ``2024``).
        limit (int): page size -- max picks to return (paginated endpoint).
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per draft pick.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_draft_picks
        >>> picks = nfl_draft_picks(year=2024, limit=300)
        >>> picks.select(["draftRound", "draftNumberOverall", "teamId", "personId"]).head()
    """
    payload = _get(f"{API_HOST}/football/v2/draft/picks/report", headers, {"year": year, "limit": limit})
    return _to_frame(payload.get("picks", []), return_as_pandas)


def nfl_injuries(
    season: int = 2024,
    season_type: str = "REG",
    week: int = 1,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` injury report -- one row per player (``/football/v2/injuries``).

    Records live under the ``injuries`` key. Each row carries ``season``,
    ``seasonType``, ``week``, ``team_*``, the nested ``person`` object,
    ``injuryStatus``, ``position``, ``practiceStatus`` and a nested ``injuries``
    list of body-part detail.

    Args:
        season (int): season year (e.g. ``2024``).
        season_type (str): season type. One of ``"PRE"``, ``"REG"``, ``"POST"``.
        week (int): week number.
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per injured player.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_injuries
        >>> inj = nfl_injuries(season=2024, season_type="REG", week=1)
        >>> inj.select(["team_abbreviation", "injuryStatus", "position"]).head()
    """
    payload = _get(
        f"{API_HOST}/football/v2/injuries",
        headers,
        {"season": season, "seasonType": season_type, "week": week},
    )
    return _to_frame(payload.get("injuries", []), return_as_pandas)


def nfl_game_summaries(
    season: int = 2024,
    season_type: str = "REG",
    week: int = 1,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` live game summaries -- one row per game (``/football/v2/stats/live/game-summaries``).

    Records live under the ``data`` key. Each row carries ``gameId``, the live
    game-state fields (``clock``, ``quarter``, ``phase``, ``down``, ``distance``,
    ``yardLine``, ``isRedZone``, ``isGoalToGo``), ``attendance``, ``weather`` and
    nested ``homeTeam`` / ``awayTeam`` summary objects.

    Args:
        season (int): season year (e.g. ``2024``).
        season_type (str): season type. One of ``"PRE"``, ``"REG"``, ``"POST"``.
        week (int): week number.
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per game.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_game_summaries
        >>> summaries = nfl_game_summaries(season=2024, season_type="REG", week=1)
        >>> summaries.select(["gameId", "quarter", "phase", "homeTeam", "awayTeam"]).head()
    """
    payload = _get(
        f"{API_HOST}/football/v2/stats/live/game-summaries",
        headers,
        {"season": season, "seasonType": season_type, "week": week},
    )
    return _to_frame(payload.get("data", []), return_as_pandas)


def nfl_weekly_game_details(
    season: int = 2024,
    season_type: str = "REG",
    week: int = 1,
    include_drive_chart: bool = True,
    include_replays: bool = False,
    include_standings: bool = False,
    include_tagged_videos: bool = False,
    headers: Optional[Dict[str, str]] = None,
    return_as_pandas: bool = False,
):
    """Parsed ``api.nfl.com`` weekly game details -- one row per game.

    Wraps ``/football/v2/experience/weekly-game-details``, which returns a **bare
    list** of game objects (no wrapper key). Each row carries ``id`` (uuid game
    id), nested ``homeTeam`` / ``awayTeam``, ``date``, ``time``, ``venue``,
    ``status``, ``broadcastInfo``, ``externalIds``, a ``summary`` object and,
    when requested, ``driveChart`` / ``replays`` / ``taggedVideos``.

    Args:
        season (int): season year (e.g. ``2024``).
        season_type (str): season type. One of ``"PRE"``, ``"REG"``, ``"POST"``;
            sent on the wire as the ``type`` query param.
        week (int): week number.
        include_drive_chart (bool): include the per-game ``driveChart`` block.
        include_replays (bool): include the ``replays`` block.
        include_standings (bool): include the ``standings`` block.
        include_tagged_videos (bool): include the ``taggedVideos`` block.
        headers (Dict[str, str] | None): reuse a :func:`nfl_headers_gen` dict.
        return_as_pandas (bool): return a pandas frame instead of polars.

    Returns:
        A polars (or pandas) ``DataFrame``, one row per game.

    Example:
        >>> from sportsdataverse.nfl.nfl_api import nfl_weekly_game_details
        >>> details = nfl_weekly_game_details(season=2024, season_type="REG", week=1)
        >>> details.select(["id", "date", "homeTeam", "awayTeam"]).head()
    """
    payload = _get(
        f"{API_HOST}/football/v2/experience/weekly-game-details",
        headers,
        {
            "season": season,
            "type": season_type,
            "week": week,
            "includeDriveChart": str(include_drive_chart).lower(),
            "includeReplays": str(include_replays).lower(),
            "includeStandings": str(include_standings).lower(),
            "includeTaggedVideos": str(include_tagged_videos).lower(),
        },
    )
    records = payload if isinstance(payload, list) else payload.get("games", []) or payload.get("data", [])
    return _to_frame(records, return_as_pandas)


__all__ = [
    "nfl_standings",
    "nfl_rosters",
    "nfl_teams_history",
    "nfl_team",
    "nfl_weeks",
    "nfl_weeks_by_date",
    "nfl_combine_profiles",
    "nfl_draft_picks",
    "nfl_injuries",
    "nfl_game_summaries",
    "nfl_weekly_game_details",
]
