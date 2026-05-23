"""sportsdataverse.mlb.mlb_api — wrappers for the MLB Stats API.

Host: ``statsapi.mlb.com``. Spec: ``sdv-internal-refs/mlb/mlb-stats-api.openapi.yaml``.
Sister module for the per-pitch Statcast surface at ``baseballsavant.mlb.com``:
:mod:`sportsdataverse.mlb.mlb_statcast`.

The MLB Stats API is reverse-engineered (not officially documented). Endpoint
catalog consolidated from ``toddrob99/MLB-StatsAPI``, ``pseudo-r/Public-MLB-API``,
and MLB's 2025 hackathon dataset.

Conventions
-----------

* **season**: 4-digit year (int or string).
* **sport_id**: ``1`` = MLB (default). Minor leagues: ``11`` AAA, ``12`` AA,
  ``13`` A+, ``14`` A, ``16`` Rookie. International: ``31`` NPB, ``32`` KBO.
* **league_id**: ``103`` AL, ``104`` NL.
* **game_type**: ``R`` regular, ``F`` wild card, ``D`` DS, ``L`` LCS, ``W`` WS,
  ``S`` spring, ``A`` all-star, ``E`` exhibition, ``PO`` postseason.
* **hydrate**: comma-separated nested-paren list (e.g. ``"team(roster(person))"``)
  that expands related resources inline.
* **fields**: comma-separated JSON key allow-list that trims the payload.
* **game_pk**: MLB-side game identifier — NOT the same as ESPN's ``event_id``.
* **person_id** = MLBAM id; identical to Statcast's ``batter`` / ``pitcher`` id.

All wrappers return ``Dict`` (the raw JSON payload). Response envelopes vary
substantially per endpoint (``{teams: [...]}``, ``{dates: [{games: [...]}]}``,
``{stats: [{splits: [...]}]}``, …); plan a normalization layer in callers.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Union

from sportsdataverse.dl_utils import download

_BASE = "https://statsapi.mlb.com"


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# schedule
# ---------------------------------------------------------------------------


def mlb_api_schedule(
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


def mlb_api_schedule_postseason(
    season: Optional[Union[int, str]] = None,
    sport_id: int = 1,
    hydrate: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/schedule/postseason — postseason-only schedule for a season."""
    return _get(
        "/api/v1/schedule/postseason",
        params={
            "season": season,
            "sportId": sport_id,
            "hydrate": hydrate,
        },
        **kwargs,
    )


# ---------------------------------------------------------------------------
# game (single-game data)
# ---------------------------------------------------------------------------


def mlb_api_pbp_live(
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
    ``mlb_api_pbp`` is preserved as an alias.
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


# Backwards-compatible alias — preserved from the original 3-function stub.
mlb_api_pbp = mlb_api_pbp_live


def mlb_api_pbp_diff(game_pk: int, start_timecode: str, end_timecode: Optional[str] = None, **kwargs) -> Dict:
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


def mlb_api_boxscore(game_pk: int, timecode: Optional[str] = None, fields: Optional[str] = None, **kwargs) -> Dict:
    """GET /api/v1/game/{gamePk}/boxscore — team + player boxscore for one game."""
    return _get(f"/api/v1/game/{game_pk}/boxscore", params={"timecode": timecode, "fields": fields}, **kwargs)


def mlb_api_linescore(game_pk: int, timecode: Optional[str] = None, fields: Optional[str] = None, **kwargs) -> Dict:
    """GET /api/v1/game/{gamePk}/linescore — inning-by-inning + current game state."""
    return _get(f"/api/v1/game/{game_pk}/linescore", params={"timecode": timecode, "fields": fields}, **kwargs)


def mlb_api_play_by_play(game_pk: int, timecode: Optional[str] = None, fields: Optional[str] = None, **kwargs) -> Dict:
    """GET /api/v1/game/{gamePk}/playByPlay — play-by-play with at-bat detail."""
    return _get(f"/api/v1/game/{game_pk}/playByPlay", params={"timecode": timecode, "fields": fields}, **kwargs)


def mlb_api_game_context_metrics(game_pk: int, fields: Optional[str] = None, **kwargs) -> Dict:
    """GET /api/v1/game/{gamePk}/contextMetrics — WP, leverage index, in-game context."""
    return _get(f"/api/v1/game/{game_pk}/contextMetrics", params={"fields": fields}, **kwargs)


def mlb_api_win_probability(game_pk: int, fields: Optional[str] = None, **kwargs) -> Dict:
    """GET /api/v1/game/{gamePk}/winProbability — per-play WP timeline."""
    return _get(f"/api/v1/game/{game_pk}/winProbability", params={"fields": fields}, **kwargs)


def mlb_api_game_content(game_pk: int, **kwargs) -> Dict:
    """GET /api/v1/game/{gamePk}/content — articles, highlights, editorial content."""
    return _get(f"/api/v1/game/{game_pk}/content", **kwargs)


# ---------------------------------------------------------------------------
# teams
# ---------------------------------------------------------------------------


def mlb_api_teams(
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


def mlb_api_team(
    team_id: int,
    season: Optional[Union[int, str]] = None,
    sport_id: int = 1,
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/teams/{teamId} — single team detail."""
    return _get(
        f"/api/v1/teams/{team_id}",
        params={
            "season": season,
            "sportId": sport_id,
            "hydrate": hydrate,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_api_team_roster(
    team_id: int,
    season: Optional[Union[int, str]] = None,
    roster_type: str = "active",
    date: Optional[str] = None,
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/teams/{teamId}/roster — team roster.

    ``roster_type``: ``active``, ``40Man``, ``allTime``, ``fullSeason``,
    ``fullRoster``, ``nonRosterInvitees``, ``depthChart``, ``coach``.
    """
    return _get(
        f"/api/v1/teams/{team_id}/roster",
        params={
            "season": season,
            "rosterType": roster_type,
            "date": date,
            "hydrate": hydrate,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_api_team_stats(
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


def mlb_api_team_leaders(
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


def mlb_api_team_alumni(
    team_id: int,
    season: Union[int, str],
    group: str = "hitting",
    hydrate: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/teams/{teamId}/alumni — players who played for this team in a season."""
    return _get(
        f"/api/v1/teams/{team_id}/alumni",
        params={
            "season": season,
            "group": group,
            "hydrate": hydrate,
        },
        **kwargs,
    )


def mlb_api_team_affiliates(
    team_ids: Union[int, List[int]],
    sport_id: int = 1,
    season: Optional[Union[int, str]] = None,
    hydrate: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/teams/affiliates — org affiliates (MLB parent → minor league chain)."""
    return _get(
        "/api/v1/teams/affiliates",
        params={
            "teamIds": _csv(team_ids),
            "sportId": sport_id,
            "season": season,
            "hydrate": hydrate,
        },
        **kwargs,
    )


# ---------------------------------------------------------------------------
# people (players, coaches, umpires)
# ---------------------------------------------------------------------------


def mlb_api_people(
    person_ids: Union[int, List[int]],
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/people?personIds=... — bulk person lookup by MLBAM id."""
    return _get(
        "/api/v1/people",
        params={
            "personIds": _csv(person_ids),
            "hydrate": hydrate,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_api_person(
    person_id: int,
    season: Optional[Union[int, str]] = None,
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/people/{personId} — single person detail."""
    return _get(
        f"/api/v1/people/{person_id}",
        params={
            "season": season,
            "hydrate": hydrate,
            "fields": fields,
        },
        **kwargs,
    )


def mlb_api_person_stats(
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


def mlb_api_person_game_stats(person_id: int, game_pk: Union[int, str], fields: Optional[str] = None, **kwargs) -> Dict:
    """GET /api/v1/people/{personId}/stats/game/{gamePk} — one player, one game.

    Use ``game_pk="current"`` to get a player's stats in their current/last game.
    """
    return _get(f"/api/v1/people/{person_id}/stats/game/{game_pk}", params={"fields": fields}, **kwargs)


def mlb_api_sport_players(
    sport_id: int = 1,
    season: Optional[Union[int, str]] = None,
    hydrate: Optional[str] = None,
    fields: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/sports/{sportId}/players — every player in a sport for a season."""
    return _get(
        f"/api/v1/sports/{sport_id}/players",
        params={
            "season": season,
            "hydrate": hydrate,
            "fields": fields,
        },
        **kwargs,
    )


# ---------------------------------------------------------------------------
# standings
# ---------------------------------------------------------------------------


def mlb_api_standings(
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


# ---------------------------------------------------------------------------
# stats (aggregate / leaderboard queries)
# ---------------------------------------------------------------------------


def mlb_api_stats(
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


def mlb_api_stats_leaders(
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


def mlb_api_stats_streaks(
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


# ---------------------------------------------------------------------------
# structure (sports, leagues, divisions, seasons, venues)
# ---------------------------------------------------------------------------


def mlb_api_sports(sport_id: Optional[int] = None, **kwargs) -> Dict:
    """GET /api/v1/sports — list known sports (MLB, MiLB, KBO, NPB, …)."""
    return _get("/api/v1/sports", params={"sportId": sport_id}, **kwargs)


def mlb_api_leagues(
    sport_id: int = 1,
    season: Optional[Union[int, str]] = None,
    league_ids: Optional[Union[int, List[int]]] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/leagues — list leagues."""
    return _get(
        "/api/v1/leagues",
        params={
            "sportId": sport_id,
            "season": season,
            "leagueIds": _csv(league_ids),
        },
        **kwargs,
    )


def mlb_api_divisions(
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


def mlb_api_seasons(
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


def mlb_api_season(season_id: Union[int, str], sport_id: int = 1, **kwargs) -> Dict:
    """GET /api/v1/seasons/{seasonId} — single season detail."""
    return _get(f"/api/v1/seasons/{season_id}", params={"sportId": sport_id}, **kwargs)


def mlb_api_venues(
    season: Optional[Union[int, str]] = None,
    sport_ids: Optional[Union[int, List[int]]] = None,
    hydrate: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/venues — list venues."""
    return _get(
        "/api/v1/venues",
        params={
            "season": season,
            "sportIds": _csv(sport_ids),
            "hydrate": hydrate,
        },
        **kwargs,
    )


def mlb_api_venue(
    venue_id: int,
    season: Optional[Union[int, str]] = None,
    hydrate: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/venues/{venueId} — single venue detail."""
    return _get(
        f"/api/v1/venues/{venue_id}",
        params={
            "season": season,
            "hydrate": hydrate,
        },
        **kwargs,
    )


# ---------------------------------------------------------------------------
# meta (enum lookups)
# ---------------------------------------------------------------------------


def mlb_api_meta(meta_type: str, **kwargs) -> Dict:
    """GET /api/v1/{metaType} — enum lookup (the API's self-describing surface).

    Known ``meta_type`` values: ``awards``, ``baseballStats``, ``eventTypes``,
    ``gameStatus``, ``gameTypes``, ``hitTrajectories``, ``jobTypes``,
    ``languages``, ``leagueLeaderTypes``, ``logicalEvents``, ``metrics``,
    ``pitchCodes``, ``pitchTypes``, ``platforms``, ``positions``, ``reviewReasons``,
    ``rosterTypes``, ``scheduleEventTypes``, ``situationCodes``, ``sky``,
    ``standingsTypes``, ``statGroups``, ``statTypes``, ``windDirection``.
    """
    return _get(f"/api/v1/{meta_type}", **kwargs)


# ---------------------------------------------------------------------------
# misc (awards, draft, attendance, umpires)
# ---------------------------------------------------------------------------


def mlb_api_awards(sport_id: Optional[int] = None, **kwargs) -> Dict:
    """GET /api/v1/awards — list award IDs (call with no params to enumerate)."""
    return _get("/api/v1/awards", params={"sportId": sport_id}, **kwargs)


def mlb_api_award_recipients(
    award_id: str,
    season: Optional[Union[int, str]] = None,
    sport_id: int = 1,
    hydrate: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /api/v1/awards/{awardId}/recipients — historical winners of one award."""
    return _get(
        f"/api/v1/awards/{award_id}/recipients",
        params={
            "season": season,
            "sportId": sport_id,
            "hydrate": hydrate,
        },
        **kwargs,
    )


def mlb_api_draft(
    year: Union[int, str],
    round_: Optional[Union[int, str]] = None,
    team_id: Optional[int] = None,
    player_id: Optional[int] = None,
    limit: int = 100,
    **kwargs,
) -> Dict:
    """GET /api/v1/draft/{year} — draft results for a year (optionally one round)."""
    return _get(
        f"/api/v1/draft/{year}",
        params={
            "round": round_,
            "teamId": team_id,
            "playerId": player_id,
            "limit": limit,
        },
        **kwargs,
    )


def mlb_api_draft_prospects(
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


def mlb_api_attendance(
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


def mlb_api_umpires(**kwargs) -> Dict:
    """GET /api/v1/jobs/umpires — current umpire crew assignments."""
    return _get("/api/v1/jobs/umpires", **kwargs)
