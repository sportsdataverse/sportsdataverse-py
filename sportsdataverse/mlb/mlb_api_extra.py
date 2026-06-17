"""Hand-written MLB Stats API wrappers the URL-builder codegen can't express.

Live alongside the generated :mod:`sportsdataverse.mlb.mlb_api`. These use
conditional `_csv` query inclusion, multi-param shaping, or the /api/v1.1/ host
override. Listed in ``tests/codegen/test_parity_native.py::_IRREGULAR``.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Union  # noqa: F401

from sportsdataverse.dl_utils import download

_BASE = "https://statsapi.mlb.com"

__all__ = [
    "mlb_attendance",
    "mlb_divisions",
    "mlb_draft_prospects",
    "mlb_pbp_diff",
    "mlb_pbp_live",
    "mlb_person_stats",
    "mlb_schedule",
    "mlb_seasons",
    "mlb_standings",
    "mlb_stats",
    "mlb_stats_leaders",
    "mlb_stats_streaks",
    "mlb_team_leaders",
    "mlb_team_stats",
    "mlb_teams",
]


def _get(path: str, params: Optional[dict] = None, **kwargs) -> Dict:
    """GET ``{_BASE}{path}`` as JSON. Returns ``{}`` on failure.

    Strips ``None`` values from ``params`` before sending so caller wrappers can
    pass every optional param uniformly without producing ``?foo=&bar=`` urls.
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    url = f"{_BASE}{path}"
    resp = download(url=url, params=clean, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}


def _csv(values) -> Optional[str]:
    """Join a list/tuple into a comma-separated string; pass-through scalars / None."""
    if values is None:
        return None
    if isinstance(values, (list, tuple)):
        return ",".join(str(v) for v in values)
    return str(values)


def mlb_schedule(
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    team_id: Optional[int] = None,
    opponent_id: Optional[int] = None,
    season: Optional[Union[int, str]] = None,
    sport_id: int = 1,
    game_type: Optional[str] = None,
    league_id: Optional[Union[int, str]] = None,
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/schedule — schedule of games for a date, range, team, or season.

    Response: ``dates[].games[]``.
    """
    return _get(
        "/api/v1/schedule",
        params={
            "date": date,
            "startDate": start_date,
            "endDate": end_date,
            "teamId": team_id,
            "opponentId": opponent_id,
            "season": season,
            "sportId": sport_id,
            "gameType": game_type,
            "leagueId": _csv(league_id),
            "hydrate": hydrate,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_pbp_live(
    game_pk: int,
    language: Optional[str] = None,
    timecode: Optional[str] = None,
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1).

    Top-level keys: ``copyright, gamePk, link, metaData, gameData, liveData``.
    Includes Statcast metrics where available. The historical name
    ``mlb_pbp`` is preserved as an alias in the generated module.
    """
    return _get(
        f"/api/v1.1/game/{game_pk}/feed/live",
        params={
            "language": language,
            "timecode": timecode,
            "hydrate": hydrate,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_pbp_diff(game_pk: int, start_timecode: str, end_timecode: Optional[str] = None, **kwargs) -> Dict:
    """GET /api/v1/game/{gamePk}/feed/live/diffPatch — JSON-patch diff of the live feed.

    Replays of in-game state for low-bandwidth clients.
    """
    return _get(
        f"/api/v1/game/{game_pk}/feed/live/diffPatch",
        params={
            "startTimecode": start_timecode,
            "endTimecode": end_timecode,
        },
        **kwargs,
    )


def mlb_teams(
    season: Optional[Union[int, str]] = None,
    sport_id: int = 1,
    league_ids: Optional[Union[int, List[int], str]] = None,
    active_status: Optional[str] = None,
    all_star_statuses: Optional[str] = None,
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/teams — list teams. ``sport_id=1`` = MLB."""
    return _get(
        "/api/v1/teams",
        params={
            "season": season,
            "sportId": sport_id,
            "leagueIds": _csv(league_ids),
            "activeStatus": active_status,
            "allStarStatuses": all_star_statuses,
            "hydrate": hydrate,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_team_stats(
    team_id: int,
    season: Union[int, str],
    stats: str = "season",
    group: str = "hitting",
    sport_ids: Optional[Union[int, List[int]]] = None,
    game_type: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/teams/{teamId}/stats — team-level stats.

    ``stats``: ``season``, ``career``, ``yearByYear``, ``byMonth``, ``byDayOfWeek``, …
    ``group``: ``hitting``, ``pitching``, ``fielding``.
    """
    return _get(
        f"/api/v1/teams/{team_id}/stats",
        params={
            "season": season,
            "stats": stats,
            "group": group,
            "sportIds": _csv(sport_ids),
            "gameType": game_type,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_team_leaders(
    team_id: int,
    leader_categories: str,
    season: Optional[Union[int, str]] = None,
    leader_game_types: Optional[str] = None,
    limit: int = 10,
    **kwargs,
) -> Dict:
    """GET /api/v1/teams/{teamId}/leaders — team leaders.

    ``leader_categories`` e.g. ``homeRuns``, ``battingAverage``, ``wins``,
    ``earnedRunAverage`` (comma-separated for multi).
    """
    return _get(
        f"/api/v1/teams/{team_id}/leaders",
        params={
            "leaderCategories": leader_categories,
            "season": season,
            "leaderGameTypes": leader_game_types,
            "limit": limit,
        },
        **kwargs,
    )


def mlb_person_stats(
    person_id: int,
    stats: str,
    group: str = "hitting",
    season: Optional[Union[int, str]] = None,
    season_type: Optional[str] = None,
    sport_ids: Optional[Union[int, List[int]]] = None,
    game_type: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/people/{personId}/stats — player aggregate stats.

    ``stats``: ``season``, ``career``, ``yearByYear``, ``vsTeam``, ``vsPlayer``,
    ``byMonth``, ``byDayOfWeek``, ``homeAndAway``, ``gameLog``, ``lastXGames``, …
    """
    return _get(
        f"/api/v1/people/{person_id}/stats",
        params={
            "stats": stats,
            "group": group,
            "season": season,
            "seasonType": season_type,
            "sportIds": _csv(sport_ids),
            "gameType": game_type,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_standings(
    league_id: Union[int, str, List[int]] = "103,104",
    season: Optional[Union[int, str]] = None,
    date: Optional[str] = None,
    standings_types: Optional[str] = None,
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/standings — league standings.

    ``league_id``: ``103`` AL, ``104`` NL (comma-separated for both, the default).
    ``standings_types`` e.g. ``regularSeason``, ``wildCard``, ``divisionLeaders``.
    """
    return _get(
        "/api/v1/standings",
        params={
            "leagueId": _csv(league_id),
            "season": season,
            "date": date,
            "standingsTypes": standings_types,
            "hydrate": hydrate,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_stats(
    stats: str,
    group: str,
    season: Optional[Union[int, str]] = None,
    sport_id: int = 1,
    league_id: Optional[Union[int, str]] = None,
    team_id: Optional[int] = None,
    player_pool: Optional[str] = None,
    game_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/stats — generic stats query.

    ``stats`` selects the slice (``season``, ``career``, ``yearByYear``, …) and
    ``group`` selects the stat group (``hitting``, ``pitching``, ``fielding``).
    Filters: ``season``, ``team_id``, ``league_id``, ``game_type``, ``player_pool``.
    """
    return _get(
        "/api/v1/stats",
        params={
            "stats": stats,
            "group": group,
            "season": season,
            "sportId": sport_id,
            "leagueId": _csv(league_id),
            "teamId": team_id,
            "playerPool": player_pool,
            "gameType": game_type,
            "limit": limit,
            "offset": offset,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_stats_leaders(
    leader_categories: str,
    season: Optional[Union[int, str]] = None,
    leader_game_types: Optional[str] = None,
    stat_group: Optional[str] = None,
    league_id: Optional[Union[int, str]] = None,
    sport_id: int = 1,
    limit: int = 10,
    **kwargs,
) -> Dict:
    """GET /api/v1/stats/leaders — top-N leaders for a stat category."""
    return _get(
        "/api/v1/stats/leaders",
        params={
            "leaderCategories": leader_categories,
            "season": season,
            "leaderGameTypes": leader_game_types,
            "statGroup": stat_group,
            "leagueId": _csv(league_id),
            "sportId": sport_id,
            "limit": limit,
        },
        **kwargs,
    )


def mlb_stats_streaks(
    streak_type: str,
    streak_threshold: int = 1,
    season: Optional[Union[int, str]] = None,
    stat_group: Optional[str] = None,
    active_streak: Optional[bool] = None,
    sport_id: int = 1,
    **kwargs,
) -> Dict:
    """GET /api/v1/stats/streaks — active or historical streaks.

    ``streak_type`` e.g. ``hittingStreakOverall``, ``onBaseOverall``.
    """
    return _get(
        "/api/v1/stats/streaks",
        params={
            "streakType": streak_type,
            "streakThreshold": streak_threshold,
            "season": season,
            "statGroup": stat_group,
            "activeStreak": str(active_streak).lower() if active_streak is not None else None,
            "sportId": sport_id,
        },
        **kwargs,
    )


def mlb_divisions(
    sport_id: int = 1,
    league_id: Optional[Union[int, str]] = None,
    division_id: Optional[int] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/divisions — list divisions."""
    return _get(
        "/api/v1/divisions",
        params={
            "sportId": sport_id,
            "leagueId": _csv(league_id),
            "divisionId": division_id,
        },
        **kwargs,
    )


def mlb_seasons(
    sport_id: int = 1,
    season: Optional[Union[int, str]] = None,
    all_seasons: bool = False,
    **kwargs,
) -> Dict:
    """GET /api/v1/seasons — list of seasons for a sport."""
    return _get(
        "/api/v1/seasons",
        params={
            "sportId": sport_id,
            "season": season,
            "all": str(all_seasons).lower() if all_seasons else None,
        },
        **kwargs,
    )


def mlb_draft_prospects(
    year: Union[int, str],
    scouting_report: Optional[bool] = None,
    limit: int = 100,
    **kwargs,
) -> Dict:
    """GET /api/v1/draft/prospects/{year} — draft prospect list for a year."""
    return _get(
        f"/api/v1/draft/prospects/{year}",
        params={
            "scoutingReport": str(scouting_report).lower() if scouting_report is not None else None,
            "limit": limit,
        },
        **kwargs,
    )


def mlb_attendance(
    team_id: Optional[int] = None,
    league_id: Optional[Union[int, str]] = None,
    season: Optional[Union[int, str]] = None,
    league_list_id: Optional[str] = None,
    game_type: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/attendance — game attendance figures."""
    return _get(
        "/api/v1/attendance",
        params={
            "teamId": team_id,
            "leagueId": _csv(league_id),
            "season": season,
            "leagueListId": league_list_id,
            "gameType": game_type,
        },
        **kwargs,
    )
