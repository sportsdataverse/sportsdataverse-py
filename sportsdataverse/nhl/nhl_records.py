"""sportsdataverse.nhl.nhl_records — wrappers for ``records.nhl.com/site/api/``.

**Documentation**:

* NHL Records endpoint reference: https://py.sportsdataverse.org/docs/nhl/records
* Parser module: :mod:`sportsdataverse.nhl.nhl_records_parsers`

Covers the most useful ~35 endpoints across awards, coaches, skaters,
goaltenders, franchises, draft, all-star, milestones, and other historical
records.  All queries support the standard NHL Records API filter kwargs:
``cayenneExp``, ``factCayenneExp``, ``include``, ``limit``, ``start``,
``sort`` — pass them as keyword arguments and they are forwarded as query
parameters.

Endpoint catalog sourced from the OpenAPI spec at
``fastRhockey/data-raw/nhl_records_openapi.yaml``
(base URL: ``https://records.nhl.com/site/api``).

Conventions
-----------

* All functions return ``Dict`` (the raw JSON payload decoded from the
  API response).  The top-level shape is always
  ``{"data": [...], "total": N}``.
* Path parameters (``franchise_id``, ``id``, ``season_id``, …) map to
  optional positional/keyword arguments.  Pass ``None`` (or omit) to get
  the list endpoint; pass a value to get the single-resource variant.
* ``**filters`` accepts any extra query parameters supported by the Records
  API (e.g. ``cayenneExp="franchiseId=1"``, ``limit=50``, ``sort="points"``).
"""

from __future__ import annotations

from typing import Dict, Optional

from sportsdataverse.dl_utils import download

_RECORDS_BASE = "https://records.nhl.com/site/api"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fetch(path: str, params: Optional[dict] = None, **kwargs) -> Dict:
    """Internal ``download() → .json()`` helper.  Returns ``{}`` on failure."""
    url = f"{_RECORDS_BASE}{path}"
    resp = download(url=url, params=params, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}


def _build_params(**filters) -> Optional[dict]:
    """Convert caller-supplied filter kwargs into a query-param dict.

    Returns ``None`` when no filters are present so ``download()`` skips
    appending a bare ``?`` to the URL.
    """
    clean = {k: v for k, v in filters.items() if v is not None}
    return clean if clean else None


# ---------------------------------------------------------------------------
# Awards & Trophies
# ---------------------------------------------------------------------------


def nhl_records_awards(**filters) -> Dict:
    """List all NHL award / trophy records.

    Wraps ``GET /award-details``.

    Args:
        **filters: Optional query parameters such as ``cayenneExp``,
            ``include``, ``limit``, ``start``, ``sort``.

    Returns:
        Dict: ``{"data": [...], "total": N}`` where each entry describes
        an award, its winner, the season, and the franchise.

    Example::

        from sportsdataverse.nhl import nhl_records_awards
        awards = nhl_records_awards(limit=25)
        print(awards["total"])
    """
    return _fetch("/award-details", params=_build_params(**filters))


def nhl_records_awards_by_franchise(franchise_id: int, **filters) -> Dict:
    """List award records for a single franchise.

    Wraps ``GET /award-details/{franchiseId}``.

    Args:
        franchise_id (int): NHL Records franchise identifier
            (e.g. ``1`` for NJ Devils).
        **filters: Optional query parameters.

    Returns:
        Dict: Award entries filtered to the requested franchise.

    Example::

        from sportsdataverse.nhl import nhl_records_awards_by_franchise
        devils_awards = nhl_records_awards_by_franchise(1)
    """
    return _fetch(f"/award-details/{franchise_id}", params=_build_params(**filters))


def nhl_records_awards_trophy_season(trophy_id: int, season_id: int, **filters) -> Dict:
    """Retrieve the trophy winner for a specific season.

    Wraps ``GET /award-details/trophy/{trophyId}/season/{seasonId}``.

    Args:
        trophy_id (int): Numeric trophy identifier
            (e.g. ``5`` for the Hart Trophy).
        season_id (int): 8-digit season identifier
            (e.g. ``20242025`` for the 2024-25 season).
        **filters: Optional query parameters.

    Returns:
        Dict: Award entry for that trophy and season.

    Example::

        from sportsdataverse.nhl import nhl_records_awards_trophy_season
        hart = nhl_records_awards_trophy_season(5, 20242025)
    """
    return _fetch(
        f"/award-details/trophy/{trophy_id}/season/{season_id}",
        params=_build_params(**filters),
    )


# ---------------------------------------------------------------------------
# Coaches
# ---------------------------------------------------------------------------


def nhl_records_coaches(**filters) -> Dict:
    """List NHL head coaches.

    Wraps ``GET /coach``.

    Args:
        **filters: Optional query parameters (``cayenneExp``, ``limit``,
            ``start``, ``sort``).

    Returns:
        Dict: ``{"data": [...], "total": N}`` with coach biographical
        and career-summary fields.

    Example::

        from sportsdataverse.nhl import nhl_records_coaches
        coaches = nhl_records_coaches(limit=50)
    """
    return _fetch("/coach", params=_build_params(**filters))


def nhl_records_coach(coach_id: int, **filters) -> Dict:
    """Retrieve one coach by their numeric ID.

    Wraps ``GET /coach/{id}``.

    Args:
        coach_id (int): NHL Records coach identifier.
        **filters: Optional query parameters.

    Returns:
        Dict: Single coach record.
    """
    return _fetch(f"/coach/{coach_id}", params=_build_params(**filters))


def nhl_records_coach_career(coach_id: Optional[int] = None, **filters) -> Dict:
    """Coach career-records (regular season).

    Wraps ``GET /coach-career-records`` or
    ``GET /coach-career-records/{id}`` when *coach_id* is supplied.

    Args:
        coach_id (int, optional): Restrict to a single coach.
        **filters: Optional query parameters.

    Returns:
        Dict: Career wins, losses, ties, OT losses, points-pct per coach.

    Example::

        from sportsdataverse.nhl import nhl_records_coach_career
        all_careers = nhl_records_coach_career(limit=100)
        one_coach   = nhl_records_coach_career(coach_id=1)
    """
    path = f"/coach-career-records/{coach_id}" if coach_id is not None else "/coach-career-records"
    return _fetch(path, params=_build_params(**filters))


def nhl_records_coach_career_with_playoffs(**filters) -> Dict:
    """Coach career records inclusive of regular season + playoffs.

    Wraps ``GET /coach-career-records-regular-plus-playoffs``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Combined regular-season and playoff win/loss totals per coach.
    """
    return _fetch("/coach-career-records-regular-plus-playoffs", params=_build_params(**filters))


def nhl_records_coach_franchise(coach_id: Optional[int] = None, **filters) -> Dict:
    """Coach records scoped to individual franchise stints.

    Wraps ``GET /coach-franchise-records`` or
    ``GET /coach-franchise-records/{id}``.

    Args:
        coach_id (int, optional): Single coach.
        **filters: Optional query parameters.

    Returns:
        Dict: Per-franchise-stint win/loss rows for the coach(es).
    """
    path = (
        f"/coach-franchise-records/{coach_id}"
        if coach_id is not None
        else "/coach-franchise-records"
    )
    return _fetch(path, params=_build_params(**filters))


def nhl_records_coach_milestone_wins(wins: int, playoffs: bool = False, **filters) -> Dict:
    """Coaches who reached a wins milestone in fewest games.

    Wraps one of the ``/coach-fewest-games-to-{N}-wins`` or
    ``/coach-fewest-games-to-{N}-playoff-wins`` paths.

    Supported *wins* values: ``50, 100, 150, 200, 300, 400, 500, 600, 700,
    800, 900, 1000`` (regular season); ``50, 100, 150`` (playoffs).

    Args:
        wins (int): Milestone win total (e.g. ``100``).
        playoffs (bool): If ``True``, use the playoff-wins path.
        **filters: Optional query parameters.

    Returns:
        Dict: Coaches who hit the milestone, sorted by games needed.

    Example::

        from sportsdataverse.nhl import nhl_records_coach_milestone_wins
        fastest_100 = nhl_records_coach_milestone_wins(100)
        fastest_playoff_100 = nhl_records_coach_milestone_wins(100, playoffs=True)
    """
    suffix = f"{wins}-playoff-wins" if playoffs else f"{wins}-wins"
    return _fetch(f"/coach-fewest-games-to-{suffix}", params=_build_params(**filters))


def nhl_records_coach_stanley_cup(**filters) -> Dict:
    """Coach Stanley Cup Final win streak and consecutive-cup records.

    Wraps ``GET /coach-stanley-cup-streak``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Coaches with the longest Stanley Cup winning streaks.
    """
    return _fetch("/coach-stanley-cup-streak", params=_build_params(**filters))


# ---------------------------------------------------------------------------
# Franchises
# ---------------------------------------------------------------------------


def nhl_records_franchises(**filters) -> Dict:
    """List all NHL franchises (historical and active).

    Wraps ``GET /franchise``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: ``{"data": [...], "total": N}`` with ``franchiseId``,
        ``fullName``, ``mostRecentTeamId``, ``firstSeasonId``,
        ``lastSeasonId``, ``teamCommonName``, etc.

    Example::

        from sportsdataverse.nhl import nhl_records_franchises
        frx = nhl_records_franchises()
        print(frx["total"])
    """
    return _fetch("/franchise", params=_build_params(**filters))


def nhl_records_franchise_detail(**filters) -> Dict:
    """Franchise detail records (extended metadata per franchise).

    Wraps ``GET /franchise-detail``.

    Args:
        **filters: Optional query parameters such as
            ``cayenneExp="mostRecentTeamId=1"`` to scope to one franchise.

    Returns:
        Dict: Extended per-franchise metadata including captains, GMs,
        head coaches, and retired numbers.
    """
    return _fetch("/franchise-detail", params=_build_params(**filters))


def nhl_records_franchise_team_totals(**filters) -> Dict:
    """All-time team totals per franchise (regular season).

    Wraps ``GET /franchise-team-totals``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Cumulative win/loss/goal/points totals for every franchise
        in regular-season play.

    Example::

        from sportsdataverse.nhl import nhl_records_franchise_team_totals
        totals = nhl_records_franchise_team_totals(
            cayenneExp="franchiseId=1"
        )
    """
    return _fetch("/franchise-team-totals", params=_build_params(**filters))


def nhl_records_franchise_season_results(**filters) -> Dict:
    """Season-by-season results for each franchise.

    Wraps ``GET /franchise-season-results``.

    Args:
        **filters: Optional query parameters (e.g.
            ``cayenneExp="franchiseId=1"``).

    Returns:
        Dict: One row per franchise-season with GP, W, L, T, OTL, PTS,
        goals for/against, and playoff seed.
    """
    return _fetch("/franchise-season-results", params=_build_params(**filters))


def nhl_records_franchise_playoff_appearances(**filters) -> Dict:
    """Franchise playoff appearance counts and streak information.

    Wraps ``GET /franchise-playoff-appearances``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Franchise playoff-appearance totals and consecutive
        appearance streaks.
    """
    return _fetch("/franchise-playoff-appearances", params=_build_params(**filters))


def nhl_records_franchise_totals(**filters) -> Dict:
    """League-wide franchise totals (all-time aggregate per franchise).

    Wraps ``GET /franchise-totals``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: All-time wins, losses, ties, OTL, and points totals for
        every franchise (regular season and playoffs combined).
    """
    return _fetch("/franchise-totals", params=_build_params(**filters))


def nhl_records_all_time_record_vs_franchise(**filters) -> Dict:
    """All-time head-to-head records between every franchise pairing.

    Wraps ``GET /all-time-record-vs-franchise``.

    Args:
        **filters: Optional query parameters (e.g.
            ``cayenneExp="franchiseId=1"`` to scope to one franchise).

    Returns:
        Dict: Wins, losses, ties, OTL for every franchise-vs-franchise
        matchup since 1917.

    Example::

        from sportsdataverse.nhl import nhl_records_all_time_record_vs_franchise
        h2h = nhl_records_all_time_record_vs_franchise(
            cayenneExp="franchiseId=1"
        )
    """
    return _fetch("/all-time-record-vs-franchise", params=_build_params(**filters))


# ---------------------------------------------------------------------------
# Skater career records
# ---------------------------------------------------------------------------


def nhl_records_skater_career_stats(**filters) -> Dict:
    """Skater career statistics (all-time, regular season).

    Wraps ``GET /goalie-career-stats`` … wait — this is the **skater**
    variant.  Wraps ``GET /skater-career-statistics`` if it exists in the
    spec; falls back to the aggregate skater endpoint.

    Wraps ``GET /skater-career-statistics``.

    Args:
        **filters: Optional query parameters such as
            ``cayenneExp="seasonId=20242025"``,
            ``sort=[{"property":"points","direction":"DESC"}]``,
            ``limit=25``.

    Returns:
        Dict: Career GP, G, A, PTS, PIM, +/- per skater.

    Example::

        from sportsdataverse.nhl import nhl_records_skater_career_stats
        top_scorers = nhl_records_skater_career_stats(
            sort='[{"property":"points","direction":"DESC"}]',
            limit=25,
        )
    """
    return _fetch("/skater-career-statistics", params=_build_params(**filters))


def nhl_records_skater_career_leaders(**filters) -> Dict:
    """All-time skater career leaderboards.

    Wraps ``GET /skater-career-leaders``.

    Args:
        **filters: Optional query parameters.  Use
            ``cayenneExp="categoryType=goals"`` (or ``"assists"``,
            ``"points"``, ``"penaltyMinutes"``) to pick the leaderboard.

    Returns:
        Dict: Career stat leaders with rank, player name, and value.
    """
    return _fetch("/skater-career-leaders", params=_build_params(**filters))


def nhl_records_consecutive_100pt_seasons(**filters) -> Dict:
    """Skaters with the most consecutive 100-point seasons.

    Wraps ``GET /consecutive-100-point-seasons``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Skaters sorted by streak length, with season range.
    """
    return _fetch("/consecutive-100-point-seasons", params=_build_params(**filters))


def nhl_records_consecutive_goal_seasons(goals: int = 50, **filters) -> Dict:
    """Skaters with the most consecutive N-goal seasons.

    Wraps one of:
      * ``GET /consecutive-20-goal-seasons``
      * ``GET /consecutive-30-goal-seasons``
      * ``GET /consecutive-40-goal-seasons``
      * ``GET /consecutive-50-goal-seasons``
      * ``GET /consecutive-60-goal-seasons``

    Args:
        goals (int): Goal threshold — one of ``20, 30, 40, 50, 60``.
        **filters: Optional query parameters.

    Returns:
        Dict: Skaters sorted by consecutive-season streak.

    Example::

        from sportsdataverse.nhl import nhl_records_consecutive_goal_seasons
        streaks = nhl_records_consecutive_goal_seasons(50)
    """
    valid = {20, 30, 40, 50, 60}
    if goals not in valid:
        raise ValueError(f"goals must be one of {sorted(valid)}, got {goals!r}.")
    return _fetch(f"/consecutive-{goals}-goal-seasons", params=_build_params(**filters))


def nhl_records_games_played_streak_skaters(active_only: bool = False, **filters) -> Dict:
    """Consecutive games-played streaks for skaters.

    Wraps ``GET /games-played-streak-skaters`` (career) or
    ``GET /games-played-active-streak-skaters`` (currently active streaks).

    Args:
        active_only (bool): If ``True``, return only active streaks.
        **filters: Optional query parameters.

    Returns:
        Dict: Skaters sorted by streak length.
    """
    path = (
        "/games-played-active-streak-skaters"
        if active_only
        else "/games-played-streak-skaters"
    )
    return _fetch(path, params=_build_params(**filters))


# ---------------------------------------------------------------------------
# Goaltender career records
# ---------------------------------------------------------------------------


def nhl_records_goalie_career_stats(**filters) -> Dict:
    """Goaltender career statistics (regular season).

    Wraps ``GET /goalie-career-stats``.

    Args:
        **filters: Optional query parameters (``limit``, ``start``,
            ``sort``, ``cayenneExp``).

    Returns:
        Dict: Career GP, W, L, T/OTL, GAA, SV%, SO per goaltender.

    Example::

        from sportsdataverse.nhl import nhl_records_goalie_career_stats
        goalies = nhl_records_goalie_career_stats(
            sort='[{"property":"wins","direction":"DESC"}]',
            limit=25,
        )
    """
    return _fetch("/goalie-career-stats", params=_build_params(**filters))


def nhl_records_goalie_career_stats_with_playoffs(**filters) -> Dict:
    """Goaltender career stats inclusive of regular season and playoffs.

    Wraps ``GET /goalie_career_stats_incl_playoffs``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Combined regular-season + playoff career totals.
    """
    return _fetch("/goalie_career_stats_incl_playoffs", params=_build_params(**filters))


def nhl_records_goalie_season_stats(**filters) -> Dict:
    """Goaltender single-season statistics.

    Wraps ``GET /goalie-season-stats``.

    Args:
        **filters: Optional query parameters (e.g.
            ``cayenneExp="seasonId=20242025"``).

    Returns:
        Dict: Per-goaltender per-season rows.
    """
    return _fetch("/goalie-season-stats", params=_build_params(**filters))


def nhl_records_goalie_win_streak(**filters) -> Dict:
    """Goaltenders with the longest consecutive-win streaks.

    Wraps ``GET /goalie-win-streak``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Goaltenders sorted by streak length.
    """
    return _fetch("/goalie-win-streak", params=_build_params(**filters))


def nhl_records_goalie_shutout_streak(**filters) -> Dict:
    """Goaltenders with the longest consecutive-shutout streaks.

    Wraps ``GET /goalie-shutout-streak``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Goaltenders sorted by streak length.
    """
    return _fetch("/goalie-shutout-streak", params=_build_params(**filters))


def nhl_records_goalie_win_plateaus(**filters) -> Dict:
    """Goaltenders who reached each win plateau (100, 200, 300 …).

    Wraps ``GET /goalie-win-plateaus``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Goalies listed at each plateau milestone with the date
        and game in which they reached it.
    """
    return _fetch("/goalie-win-plateaus", params=_build_params(**filters))


def nhl_records_goalie_playoff_streak(**filters) -> Dict:
    """Goaltender consecutive playoff-win streaks.

    Wraps ``GET /goalie-playoff-streak``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Playoff win streaks sorted by length.
    """
    return _fetch("/goalie-playoff-streak", params=_build_params(**filters))


def nhl_records_goalie_undefeated_streak(**filters) -> Dict:
    """Goaltender longest undefeated streaks (wins + ties).

    Wraps ``GET /goalie-undefeated-streak``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Streaks sorted descending by length.
    """
    return _fetch("/goalie-undefeated-streak", params=_build_params(**filters))


# ---------------------------------------------------------------------------
# Draft
# ---------------------------------------------------------------------------


def nhl_records_draft(draft_id: Optional[int] = None, **filters) -> Dict:
    """Retrieve NHL Entry Draft picks.

    Wraps ``GET /draft`` (all years) or ``GET /draft/{id}`` when
    *draft_id* is supplied.

    Args:
        draft_id (int, optional): Draft year (e.g. ``2024``).
        **filters: Optional query parameters (``cayenneExp``,
            ``limit``, ``start``, ``sort``).

    Returns:
        Dict: Draft pick records with player, team, round, and
        overall-pick number.

    Example::

        from sportsdataverse.nhl import nhl_records_draft
        picks_2024 = nhl_records_draft(2024)
        first_rounders = nhl_records_draft(2024, cayenneExp="roundNumber=1")
    """
    path = f"/draft/{draft_id}" if draft_id is not None else "/draft"
    return _fetch(path, params=_build_params(**filters))


def nhl_records_draft_by_team(team_id: int, **filters) -> Dict:
    """All draft picks made by a single team.

    Wraps ``GET /draft/byTeam/{teamId}``.

    Args:
        team_id (int): NHL team identifier.
        **filters: Optional query parameters.

    Returns:
        Dict: Draft picks by that franchise across all years.
    """
    return _fetch(f"/draft/byTeam/{team_id}", params=_build_params(**filters))


def nhl_records_draft_prospect(prospect_id: Optional[int] = None, **filters) -> Dict:
    """Draft prospect records.

    Wraps ``GET /draft-prospect`` or ``GET /draft-prospect/{id}``.

    Args:
        prospect_id (int, optional): Individual prospect.
        **filters: Optional query parameters.

    Returns:
        Dict: Prospect biographical and scouting-ranking data.
    """
    path = (
        f"/draft-prospect/{prospect_id}"
        if prospect_id is not None
        else "/draft-prospect"
    )
    return _fetch(path, params=_build_params(**filters))


def nhl_records_draft_lottery_odds(**filters) -> Dict:
    """Draft lottery odds (current year or filtered by season).

    Wraps ``GET /draft-lottery-odds``.

    Args:
        **filters: Optional query parameters (e.g.
            ``cayenneExp="seasonId=20242025"``).

    Returns:
        Dict: Per-team draft lottery odds.
    """
    return _fetch("/draft-lottery-odds", params=_build_params(**filters))


def nhl_records_expansion_draft_picks(**filters) -> Dict:
    """Expansion draft picks (e.g. Vegas 2017, Seattle 2021).

    Wraps ``GET /expansion-draft-picks``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Players selected in each expansion draft.
    """
    return _fetch("/expansion-draft-picks", params=_build_params(**filters))


# ---------------------------------------------------------------------------
# All-Star records
# ---------------------------------------------------------------------------


def nhl_records_allstar_skater_career(**filters) -> Dict:
    """All-Star Game career statistics for skaters.

    Wraps ``GET /all-star-skater-career-stats``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Career All-Star GP, G, A, PTS, PIM per skater.

    Example::

        from sportsdataverse.nhl import nhl_records_allstar_skater_career
        stars = nhl_records_allstar_skater_career(
            sort='[{"property":"goals","direction":"DESC"}]',
            limit=25,
        )
    """
    return _fetch("/all-star-skater-career-stats", params=_build_params(**filters))


def nhl_records_allstar_goalie_career(**filters) -> Dict:
    """All-Star Game career statistics for goaltenders.

    Wraps ``GET /all-star-goaltender-career-stats``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Career All-Star GP, GAA, SV% per goaltender.
    """
    return _fetch("/all-star-goaltender-career-stats", params=_build_params(**filters))


def nhl_records_allstar_coach_career(**filters) -> Dict:
    """All-Star Game career records for coaches.

    Wraps ``GET /all-star-coach-career-stats``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: All-Star coaching appearances and W/L records.
    """
    return _fetch("/all-star-coach-career-stats", params=_build_params(**filters))


def nhl_records_allstar_skater_game(**filters) -> Dict:
    """All-Star Game single-game scoring records for skaters.

    Wraps ``GET /all-star-skater-game-stats``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Individual All-Star game stat lines per skater.
    """
    return _fetch("/all-star-skater-game-stats", params=_build_params(**filters))


def nhl_records_allstar_goalie_game(**filters) -> Dict:
    """All-Star Game single-game stats for goaltenders.

    Wraps ``GET /all-star-goaltender-game-stats``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Individual All-Star game stat lines per goaltender.
    """
    return _fetch("/all-star-goaltender-game-stats", params=_build_params(**filters))


# ---------------------------------------------------------------------------
# Attendance
# ---------------------------------------------------------------------------


def nhl_records_attendance(**filters) -> Dict:
    """NHL arena attendance records.

    Wraps ``GET /attendance``.

    Args:
        **filters: Optional query parameters (e.g.
            ``cayenneExp="franchiseId=1"`` to scope to one franchise,
            ``sort='[{"property":"attendance","direction":"DESC"}]'``).

    Returns:
        Dict: Per-game or per-season attendance entries.

    Example::

        from sportsdataverse.nhl import nhl_records_attendance
        att = nhl_records_attendance(
            sort='[{"property":"attendance","direction":"DESC"}]',
            limit=10,
        )
    """
    return _fetch("/attendance", params=_build_params(**filters))


# ---------------------------------------------------------------------------
# Milestone / fastest records
# ---------------------------------------------------------------------------


def nhl_records_fastest_goals(n_goals: int = 2, **filters) -> Dict:
    """Fastest N goals by one team in a single game.

    Wraps one of:
      * ``GET /fastest-2-goals-one-team``
      * ``GET /fastest-3-goals-one-team``
      * ``GET /fastest-4-goals-one-team``
      * ``GET /fastest-5-goals-one-team``

    Args:
        n_goals (int): Goal count — one of ``2, 3, 4, 5``.
        **filters: Optional query parameters.

    Returns:
        Dict: Games where the milestone was set, sorted by elapsed
        time (fastest first).

    Example::

        from sportsdataverse.nhl import nhl_records_fastest_goals
        fastest_3 = nhl_records_fastest_goals(3)
    """
    valid = {2, 3, 4, 5}
    if n_goals not in valid:
        raise ValueError(f"n_goals must be one of {sorted(valid)}, got {n_goals!r}.")
    return _fetch(f"/fastest-{n_goals}-goals-one-team", params=_build_params(**filters))


def nhl_records_fastest_goals_both_teams(n_goals: int = 2, **filters) -> Dict:
    """Fastest N goals combined (both teams) in a single game.

    Wraps one of:
      * ``GET /fastest-2-goals-both-teams``
      * ``GET /fastest-3-goals-both-teams``
      * ``GET /fastest-4-goals-both-teams``
      * ``GET /fastest-5-goals-both-teams``
      * ``GET /fastest-6-goals-both-teams``

    Args:
        n_goals (int): Combined goal count — one of ``2, 3, 4, 5, 6``.
        **filters: Optional query parameters.

    Returns:
        Dict: Sorted by elapsed time (fastest first).
    """
    valid = {2, 3, 4, 5, 6}
    if n_goals not in valid:
        raise ValueError(f"n_goals must be one of {sorted(valid)}, got {n_goals!r}.")
    return _fetch(f"/fastest-{n_goals}-goals-both-teams", params=_build_params(**filters))


# ---------------------------------------------------------------------------
# Hall of Fame
# ---------------------------------------------------------------------------


def nhl_records_hof_players(**filters) -> Dict:
    """Hockey Hall of Fame player inductees.

    Wraps ``GET /hof/players``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: HOF player entries with induction year, position, and
        career summary.

    Example::

        from sportsdataverse.nhl import nhl_records_hof_players
        hof = nhl_records_hof_players()
        print(hof["total"])
    """
    return _fetch("/hof/players", params=_build_params(**filters))


def nhl_records_hof_players_by_office(office_id: int, **filters) -> Dict:
    """Hall of Fame players for a specific induction office/category.

    Wraps ``GET /hof/players/{officeId}``.

    Args:
        office_id (int): HOF office identifier (e.g. ``1`` for
            Player, ``2`` for Builder, ``3`` for Referee/Linesman).
        **filters: Optional query parameters.

    Returns:
        Dict: HOF inductees in that category.
    """
    return _fetch(f"/hof/players/{office_id}", params=_build_params(**filters))


# ---------------------------------------------------------------------------
# General Manager records
# ---------------------------------------------------------------------------


def nhl_records_gm_career(gm_id: Optional[int] = None, **filters) -> Dict:
    """General Manager career records.

    Wraps ``GET /general-manager-career-records`` or
    ``GET /general-manager/{id}`` (biography) when *gm_id* is given.

    Args:
        gm_id (int, optional): Restrict to a single GM.
        **filters: Optional query parameters.

    Returns:
        Dict: Career W/L/T/OTL and points-pct for each GM's regular-season
        tenures.
    """
    if gm_id is not None:
        return _fetch(f"/general-manager/{gm_id}", params=_build_params(**filters))
    return _fetch("/general-manager-career-records", params=_build_params(**filters))


def nhl_records_gm_franchise(**filters) -> Dict:
    """General Manager records scoped to franchise stints.

    Wraps ``GET /general-manager-franchise-records``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Per-franchise-stint records for every GM.
    """
    return _fetch("/general-manager-franchise-records", params=_build_params(**filters))


# ---------------------------------------------------------------------------
# Comeback / team game records
# ---------------------------------------------------------------------------


def nhl_records_comeback_wins(scope: str = "league", **filters) -> Dict:
    """Comeback wins from a multi-goal deficit.

    Wraps:
      * ``GET /comeback-league-wins`` when *scope* is ``"league"``.
      * ``GET /comeback-franchise-wins`` when *scope* is ``"franchise"``.

    Args:
        scope (str): ``"league"`` (default) or ``"franchise"``.
        **filters: Optional query parameters (e.g.
            ``cayenneExp="franchiseId=1"``).

    Returns:
        Dict: Games where the team overcame a deficit to win.
    """
    if scope == "franchise":
        return _fetch("/comeback-franchise-wins", params=_build_params(**filters))
    return _fetch("/comeback-league-wins", params=_build_params(**filters))


def nhl_records_home_team_record(**filters) -> Dict:
    """League-wide home-team win/loss record by season.

    Wraps ``GET /home-team-record``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Home-team record aggregated by season.
    """
    return _fetch("/home-team-record", params=_build_params(**filters))


def nhl_records_away_team_record(**filters) -> Dict:
    """League-wide away-team win/loss record by season.

    Wraps ``GET /away-team-record``.

    Args:
        **filters: Optional query parameters.

    Returns:
        Dict: Away-team record aggregated by season.
    """
    return _fetch("/away-team-record", params=_build_params(**filters))
