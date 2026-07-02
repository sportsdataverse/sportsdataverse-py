"""Per-player-season age (for the DARKO aging curve), from leaguedashplayerbiostats."""

from __future__ import annotations

from typing import Callable, Optional

import polars as pl

from sportsdataverse.nba.nba_stats import nba_stats_leaguedashplayerbiostats


def nba_player_ages(
    season: str, *, league_id: str = "00", fetch: Optional[Callable[..., pl.DataFrame]] = None
) -> pl.DataFrame:
    """Per-player age for a season (bulk), for the DARKO aging curve.

    Args:
        season: NBA season, e.g. ``"2023-24"``.
        league_id: LeagueID (``"00"`` NBA).
        fetch: Injectable ``nba_stats_leaguedashplayerbiostats`` replacement for offline tests.

    Returns:
        Frame ``player_id:Int64, age:Float64``.

    Example:
        Ages for a season (residential IP)::

            from sportsdataverse.nba import nba_player_ages
            ages = nba_player_ages("2023-24")
            print(ages.head())
    """
    get = fetch or nba_stats_leaguedashplayerbiostats
    raw = get(season=season, league_id=league_id)
    return raw.select(
        pl.col("player_id").cast(pl.Int64),
        pl.col("age").cast(pl.Float64),
    )
