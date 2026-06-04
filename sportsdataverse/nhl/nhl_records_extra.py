"""Hand-written NHL records wrappers the URL-builder codegen can't express.

Live alongside the generated :mod:`sportsdataverse.nhl.nhl_records`
(``tools/codegen/endpoints/nhl_records.yaml``). These embed a value mid-path or
branch on scope -- shapes the single-URL-builder template cannot represent. Listed
in ``tests/codegen/test_parity_native.py::_IRREGULAR``.
"""

from __future__ import annotations

from typing import Dict, Optional

from sportsdataverse.dl_utils import download

_RECORDS_BASE = "https://records.nhl.com/site/api"

__all__ = [
    "nhl_records_coach_milestone_wins",
    "nhl_records_comeback_wins",
    "nhl_records_consecutive_goal_seasons",
    "nhl_records_fastest_goals",
    "nhl_records_fastest_goals_both_teams",
    "nhl_records_games_played_streak_skaters",
]


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
    path = "/games-played-active-streak-skaters" if active_only else "/games-played-streak-skaters"
    return _fetch(path, params=_build_params(**filters))


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
