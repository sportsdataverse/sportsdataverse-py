"""NBA / WNBA luck and close-game record (BB-D22): ``nba.team_luck``.

Pythagorean expected wins against actual wins, and the record in close games, one row per team
and season. The definitions are in :mod:`sportsdataverse._common.team_luck`. ``league_id="10"``
reads the WNBA releases; :mod:`sportsdataverse.wnba.wnba_team_luck` binds it.

The ``nba_stats_schedules`` release gives the final scores (games, wins, points for and
against). The ``nba_stats_possessions`` release, mapped by BB-D3's
:func:`~sportsdataverse.nba.nba_team_season_rates.nba_possession_rate_rows`, gives the close
games. Both are keyed on the stats.nba.com game id.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse._common.team_luck import GAME_RESULTS_SCHEMA, PYTHAG_EXPONENT, close_game_flags, team_luck
from sportsdataverse.nba.nba_team_season_rates import _check_args, _league_loaders, nba_possession_rate_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_team_luck"]

_EXPONENT_KEY = {"00": "nba", "10": "wnba"}


def nba_game_results(schedule: pl.DataFrame, *, season_type: str = "regular-season") -> pl.DataFrame:
    """Two rows per completed game of ``season_type``: each side's points and its opponent's.

    Args:
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows (``game_id``,
            ``season``, ``season_type``, ``home_team_id``, ``away_team_id``, ``home_pts``,
            ``away_pts``).
        season_type: The schedule ``season_type`` to keep.

    Returns:
        Completed games in :data:`~sportsdataverse._common.team_luck.GAME_RESULTS_SCHEMA`.
    """
    if schedule.height == 0:
        return pl.DataFrame(schema=GAME_RESULTS_SCHEMA)
    done = schedule.filter(
        (pl.col("season_type") == season_type) & pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null()
    )
    sides = (("home", "away"), ("away", "home"))
    return pl.concat(
        [
            done.select(
                "season",
                pl.col("game_id").cast(pl.Utf8),
                pl.col(f"{us}_team_id").alias("team_id"),
                pl.col(f"{them}_team_id").alias("opp_team_id"),
                pl.col(f"{us}_pts").alias("pts"),
                pl.col(f"{them}_pts").alias("opp_pts"),
            ).cast(GAME_RESULTS_SCHEMA)
            for us, them in sides
        ]
    )


@overload
def nba_team_luck(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_team_luck(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_team_luck(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Luck and close-game record: Pythagorean expected wins vs actual wins, and close-game W-L.

    One row per team and season. ``exp_wins`` is the Pythagorean expectation on the season's
    final scores, ``games * pts^x / (pts^x + opp_pts^x)``, with the exponent ``x`` fitted per
    league on past seasons (:data:`~sportsdataverse._common.team_luck.PYTHAG_EXPONENT`).
    ``luck_wins`` is ``wins - exp_wins``. A close game is one within 5 points at the start of a
    possession in play in the last 5:00 of the 4th quarter or in overtime: one that starts there,
    or starts before 5:00 and ends after it (stats.nba.com's clutch window; BB-D3's ``close`` split).
    For the WNBA (``league_id="10"``) through 2005, two 20-minute halves, the window is the last
    5:00 of the 2nd half.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or
            ``"play-in"``, matched to the schedule's ``season_type``. The exponent was fitted on
            regular seasons; on a handful of playoff or play-in games ``exp_wins`` is only indicative.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)``; ``team_id`` is the stats.nba.com team id (Int64).
        ``games``, ``wins``, ``losses``, ``pts`` and ``opp_pts`` are from the final scores;
        ``win_pct``, ``pythag_win_pct``, ``exp_wins`` and ``luck_wins`` (``wins - exp_wins``)
        follow. ``pbp_games`` counts the games with published possessions, and ``close_games``,
        ``close_wins``, ``close_losses`` and ``close_win_pct`` (null with no close game) are the
        close games among them. ``pythag_win_pct``, ``luck_wins`` and ``close_win_pct`` have
        ``<m>_rank`` (1 = highest), ``<m>_pct`` (the Weibull percentile) and ``<m>_n`` (games,
        or close games); ranks are within the season, among teams with at least half the most
        games (``close_win_pct`` also needs possessions for half of them and five close games),
        and null under five teams. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or ``season_type`` is not one
            of the four above (checked before any data is loaded).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_luck import nba_team_luck
            luck = nba_team_luck(2026)
            luck.sort("luck_wins", descending=True).select("team_id", "wins", "exp_wins", "luck_wins").head()

        WNBA via ``league_id``::

            wnba = nba_team_luck(2025, league_id="10")
    """
    _check_args(league_id, season_type)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    loader_seasons = [s - offset for s in seasons_list]
    schedule = load_schedule(loader_seasons)
    rows = nba_possession_rate_rows(
        load_possessions(loader_seasons), schedule, league_id=league_id, season_type=season_type
    )
    out = team_luck(
        nba_game_results(schedule, season_type=season_type),
        close_game_flags(rows),
        exponent=PYTHAG_EXPONENT[_EXPONENT_KEY[league_id]],
    )
    return out.to_pandas() if return_as_pandas else out
