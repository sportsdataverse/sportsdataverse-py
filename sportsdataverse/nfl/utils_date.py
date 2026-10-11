"""Date utility helpers for the NFL module: the one owner of the season and week rules.

Ported from nflreadr's `R/utils_date.R` (nflverse/nflreadr @ 23f915a5, 2026-09-17), the rule
nflfastR calls (`nflreadr::most_recent_season()` in its `R/database.R` and `R/calculate_stats.R`).
`nfl_schedule.most_recent_nfl_season`, `nfl_schedule.get_current_week` and the package aliases
`get_current_season` / `get_current_week` are these same functions.

NFL season convention (nflreadr):

* A new season starts on the Wednesday following Labor Day (`labor_day + 2` in
  `most_recent_season()`, R/utils_date.R:20-39; nflreadr moved it from Thursday in
  March 2026 for the Wednesday 2026 opener). Before that date, the current season is
  the previous calendar year (e.g. early Aug 2026 is still season 2025).
* Roster year flips on March 15 instead.
* Week 1 starts on the Thursday following Labor Day (`monday1_sep + 3` in
  `get_current_week(use_date = TRUE)`, R/utils_date.R:94-118); calendar weeks
  clamped to 1-22 as in nflreadr (17 regular-season weeks through 2020, 18 from 2021).
"""

from __future__ import annotations

from datetime import date, timedelta


def _labor_day(year: int) -> date:
    """First Monday of September (nflreadr `compute_labor_day()`, R/utils_date.R:131-139)."""
    sep1 = date(year, 9, 1)
    return sep1 + timedelta(days=(7 - sep1.weekday()) % 7)


def get_current_nfl_season(roster: bool = False) -> int:
    """Return the current NFL season year.

    Args:
        roster: If True, use roster-year logic (current calendar year on/after
            March 15, otherwise previous year). If False, use season logic
            (current calendar year on/after the Wednesday following Labor Day,
            otherwise previous year), as nflreadr's `most_recent_season()`.

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
            * `nflreadr`_ -- the rule this ports (`most_recent_season()`)
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _nflreadr: https://nflreadr.nflverse.com/reference/latest_season.html
        .. _nflverse: https://nflverse.nflverse.com
    """
    if not isinstance(roster, bool):
        raise TypeError("argument `roster` must be boolean")

    today = date.today()
    current_year = today.year

    if roster:
        march_15 = date(current_year, 3, 15)
        return current_year if today >= march_15 else current_year - 1

    # nflreadr most_recent_season(): the season flips on the Wednesday after Labor Day.
    return current_year if today >= _labor_day(current_year) + timedelta(days=2) else current_year - 1


def get_current_nfl_week(use_date: bool = True, roster: bool = False) -> int:
    """Return the current NFL week (1-22).

    Args:
        use_date: If True (default), compute the week purely from the calendar
            (number of weeks since the Thursday after Labor Day of the current
            season, as nflreadr's `get_current_week(use_date = TRUE)`). If
            False, hit the live schedule via `load_nfl_schedule()` and return
            the week of the next unplayed game (matches nflreadr's
            `use_date = FALSE` path). nflreadr defaults `use_date = FALSE`;
            sdv-py keeps `True` so the bare call stays offline.
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
            * `nflreadr`_ -- the rule this ports (`get_current_week()`)
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _nflreadr: https://nflreadr.nflverse.com/reference/get_current_week.html
        .. _nflverse: https://nflverse.nflverse.com
    """
    if not isinstance(use_date, bool):
        raise TypeError("argument `use_date` must be boolean")
    if not isinstance(roster, bool):
        raise TypeError("argument `roster` must be boolean")

    season_year = get_current_nfl_season(roster=roster)

    if use_date:
        # nflreadr get_current_week(use_date = TRUE): week 1 starts on Labor Day + 3, clamped to [1, 22].
        first_game = _labor_day(season_year) + timedelta(days=3)
        return min(max((date.today() - first_game).days // 7 + 1, 1), 22)

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

    Args:
        roster: Use roster-year logic (the current year from March 15) instead of season
            logic.

    Returns:
        int: The current NFL season (or roster) year, from :func:`get_current_nfl_season`.

    Example:
        Bare alias call (matches the R-side ``most_recent_season()``)::

            from sportsdataverse.nfl.utils_date import most_recent_nfl_season
            season = most_recent_nfl_season()

        Roster-year flavor::

            roster_year = most_recent_nfl_season(roster=True)
    """
    return get_current_nfl_season(roster=roster)


__all__ = ["get_current_nfl_season", "get_current_nfl_week", "most_recent_nfl_season"]
