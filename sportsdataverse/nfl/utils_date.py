"""Date utility helpers for the NFL module.

Ported from nflreadpy's `utils_date.py` (`get_current_season`,
`get_current_week`) so sdv-py users get the same conventions for
"what season are we in?" and "what week are we in?".

NFL season convention (matching nflreadpy):

* A new season "starts" on the Thursday following Labor Day. Before that
  date, the current season is the previous calendar year (e.g. early Aug
  2026 is still season 2025).
* Roster year flips on March 15 instead.
* Week is 1-22 (1-18 regular season, 19-22 playoffs).
"""

from __future__ import annotations

from datetime import date


def get_current_nfl_season(roster: bool = False) -> int:
    """Return the current NFL season year.

    Args:
        roster: If True, use roster-year logic (current calendar year on/after
            March 15, otherwise previous year). If False, use season logic
            (current calendar year on/after the Thursday following Labor Day,
            otherwise previous year).

    Returns:
        int: The current season (or roster) year.

    Raises:
        TypeError: If `roster` is not a bool.

    Example:
        Default season-year semantics::

            from sportsdataverse.nfl import get_current_nfl_season
            season = get_current_nfl_season()
            print(season)

        Roster-year semantics (March 15 cutover)::

            roster_year = get_current_nfl_season(roster=True)

        Pair with a loader to fetch only the active season::

            from sportsdataverse.nfl import load_nfl_schedule
            schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])

        See Also:
            * `nflreadpy`_ -- mirrors this convention
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _nflreadpy: https://github.com/nflverse/nflreadpy
        .. _nflverse: https://nflverse.nflverse.com
    """
    if not isinstance(roster, bool):
        raise TypeError("argument `roster` must be boolean")

    today = date.today()
    current_year = today.year

    if roster:
        march_15 = date(current_year, 3, 15)
        return current_year if today >= march_15 else current_year - 1

    # Season logic: first Monday of September is Labor Day; season starts
    # the Thursday after (Labor Day + 3 days).
    labor_day = next(date(current_year, 9, day) for day in range(1, 8) if date(current_year, 9, day).weekday() == 0)
    season_start = date(labor_day.year, labor_day.month, labor_day.day + 3)
    return current_year if today >= season_start else current_year - 1


def get_current_nfl_week(use_date: bool = True, roster: bool = False) -> int:
    """Return the current NFL week (1-22).

    Args:
        use_date: If True (default), compute the week purely from the calendar
            (number of weeks since the first Thursday of September of the
            current season). If False, hit the live schedule via
            `load_nfl_schedule()` and return the week of the next unplayed
            game (matches nflreadpy's `use_date=False` path).
        roster: Forwarded to `get_current_nfl_season()` for season inference.

    Returns:
        int: The current week, capped at 22.

    Raises:
        TypeError: If `use_date` or `roster` is not a bool.

    Example:
        Calendar-driven week (default, no network)::

            from sportsdataverse.nfl import get_current_nfl_week
            week = get_current_nfl_week()

        Schedule-driven week (hits the live schedule parquet)::

            week_live = get_current_nfl_week(use_date=False)

        Roster-year season inference::

            week_roster = get_current_nfl_week(roster=True)

        Pair with a PBP fetch to grab only the most recent season+week::

            import polars as pl
            from sportsdataverse.nfl import (
                get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
            )
            current_pbp = (
                load_nfl_pbp(seasons=[get_current_nfl_season()])
                .filter(pl.col("week") == get_current_nfl_week())
            )

        See Also:
            * `nflreadpy`_ -- mirrors this convention
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _nflreadpy: https://github.com/nflverse/nflreadpy
        .. _nflverse: https://nflverse.nflverse.com
    """
    if not isinstance(use_date, bool):
        raise TypeError("argument `use_date` must be boolean")
    if not isinstance(roster, bool):
        raise TypeError("argument `roster` must be boolean")

    season_year = get_current_nfl_season(roster=roster)

    if use_date:
        today = date.today()
        # First Thursday of September is the season opener.
        season_start = next(
            date(season_year, 9, day) for day in range(1, 8) if date(season_year, 9, day).weekday() == 3
        )
        if today < season_start:
            return 1
        days_since_start = (today - season_start).days
        return int(min(days_since_start // 7 + 1, 22))

    # Schedule-driven path: lazy import to avoid circular imports at module load.
    from sportsdataverse.nfl.nfl_loaders import load_nfl_schedule

    sched = load_nfl_schedule(seasons=[season_year])
    # Some schedule columns differ from nflreadpy's: nflreadr uses `result`,
    # nflverse parquet sometimes ships `result` too; if absent, fall back to
    # max of week. Defensive on both fronts.
    if "result" in sched.columns:
        if sched.select("result").null_count().item() == 0:
            return int(sched.select("week").drop_nulls().max().item())
        return int(sched.filter(sched["result"].is_null()).select("week").drop_nulls().min().item())
    return int(sched.select("week").drop_nulls().max().item())


def most_recent_nfl_season(roster: bool = False) -> int:
    """Alias for `get_current_nfl_season()` mirroring nflreadr's
    `most_recent_season()`.

    Example:
        Bare alias call (matches the R-side ``most_recent_season()``)::

            from sportsdataverse.nfl.utils_date import most_recent_nfl_season
            season = most_recent_nfl_season()

        Roster-year flavor::

            roster_year = most_recent_nfl_season(roster=True)
    """
    return get_current_nfl_season(roster=roster)


__all__ = ["get_current_nfl_season", "get_current_nfl_week", "most_recent_nfl_season"]
