"""College luck and close-game record (BB-D22): ``mbb.team_luck`` / ``wbb.team_luck``.

Pythagorean expected wins against actual wins, and the record in close games, one row per team
and season. The definitions are in :mod:`sportsdataverse._common.team_luck`.

Not :mod:`sportsdataverse.mbb.mbb_luck`, which is hoop-explorer's 3-point shooting luck
(regressing a lineup's 3P% to its expected rate); this module is the season W-L kind.

* The ESPN schedule gives the final scores of every completed game (games, wins, points for and
  against). ``team_id`` is the ESPN team id (Int64); non-D1 opponents get rows of their own, a
  game or two each, and stay out of the ranks.
* The ``ncaa_{mbb,wbb}_possessions`` release, mapped by BB-D3's
  :func:`~sportsdataverse.mbb.mbb_team_season_rates.ncaa_possession_rate_rows`, gives the close
  games: the last 5:00 of the 2nd half (the 4th quarter in women's basketball from 2016) or an
  overtime, within 5. The possessions are keyed on the NCAA contest id and carry ESPN's game id;
  a contest with no ESPN id cannot be matched to a result and is left unclassed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.team_luck import (
    CLOSE_SCHEMA,
    GAME_RESULTS_SCHEMA,
    PYTHAG_EXPONENT,
    close_game_flags,
    team_luck,
)
from sportsdataverse.mbb.mbb_team_season_rates import _check_league, _league_loaders, ncaa_possession_rate_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_team_luck"]

_EXPONENT_KEY = {"mens": "mbb", "womens": "wbb"}


def espn_game_results(schedule: pl.DataFrame) -> pl.DataFrame:
    """Two rows per completed ESPN game: each side's points and its opponent's.

    Args:
        schedule: ``load_mbb_schedule`` / ``load_wbb_schedule`` rows (``game_id``, ``season``,
            ``status_type_completed``, ``home_id``, ``away_id``, ``home_score``, ``away_score``).

    Returns:
        Completed games in :data:`~sportsdataverse._common.team_luck.GAME_RESULTS_SCHEMA`.
    """
    if schedule.height == 0:
        return pl.DataFrame(schema=GAME_RESULTS_SCHEMA)
    # completed rows first, then one per game: an earlier unfinished copy cannot hide the final
    done = schedule.filter(
        (pl.col("status_type_completed") == True)  # noqa: E712
        & pl.col("home_score").is_not_null()
        & pl.col("away_score").is_not_null()
    ).unique("game_id", keep="first", maintain_order=True)
    sides = (("home", "away"), ("away", "home"))
    return pl.concat(
        [
            done.select(
                "season",
                pl.col("game_id").cast(pl.Utf8),
                pl.col(f"{us}_id").alias("team_id"),
                pl.col(f"{them}_id").alias("opp_team_id"),
                pl.col(f"{us}_score").alias("pts"),
                pl.col(f"{them}_score").alias("opp_pts"),
            ).cast(GAME_RESULTS_SCHEMA)
            for us, them in sides
        ]
    )


def _espn_close_flags(possessions: pl.DataFrame, rows: pl.DataFrame) -> pl.DataFrame:
    """:func:`close_game_flags` re-keyed from the NCAA contest id to ESPN's game id."""
    if rows.height == 0:
        return pl.DataFrame(schema=CLOSE_SCHEMA)
    ids = (
        possessions.filter(pl.col("espn_game_id").is_not_null())
        .select(pl.col("contest_id").cast(pl.Utf8).alias("game_id"), pl.col("espn_game_id").cast(pl.Utf8))
        .unique()
    )
    flags = close_game_flags(rows)
    if flags.schema["game_id"] != ids.schema["game_id"]:
        raise TypeError(
            f"contest id dtype differs: flags {flags.schema['game_id']}, possessions {ids.schema['game_id']}"
        )
    return (
        flags.join(ids, on="game_id", how="inner", validate="1:1")
        .select(pl.col("espn_game_id").alias("game_id"), "close")
        .cast(CLOSE_SCHEMA)
    )


def mbb_team_luck(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College luck and close-game record: Pythagorean expected wins vs actual wins, and close-game W-L.

    One row per team and season. ``exp_wins`` is the Pythagorean expectation on the season's
    final scores, ``games * pts^x / (pts^x + opp_pts^x)``, with the exponent ``x`` fitted per
    league on past seasons (:data:`~sportsdataverse._common.team_luck.PYTHAG_EXPONENT`).
    ``luck_wins`` is ``wins - exp_wins``. A close game is one within 5 points at the start of a
    possession in the last 5:00 of the 2nd half (4th quarter, women's, from 2016) or overtime.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_team_luck.wbb_team_luck`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)``; ``team_id`` is the ESPN team id (Int64). ``games``,
        ``wins``, ``losses``, ``pts`` and ``opp_pts`` are from ESPN's completed games (postseason
        included); ``win_pct``, ``pythag_win_pct``, ``exp_wins`` and ``luck_wins``
        (``wins - exp_wins``) follow. ``pbp_games`` counts the games with published NCAA
        possessions, and ``close_games``, ``close_wins``, ``close_losses`` and ``close_win_pct``
        (null with no close game) are the close games among them. ``pythag_win_pct``,
        ``luck_wins`` and ``close_win_pct`` have ``<m>_rank`` (1 = highest), ``<m>_pct`` (the
        Weibull percentile) and ``<m>_n`` (games, or close games); ranks are within the season,
        among teams with at least half the most games (which leaves out the non-D1 opponents;
        ``close_win_pct`` also needs possessions for half of them and five close games), and null
        under five teams. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.
        SeasonNotFoundError: For a season before 2010, the first with NCAA possessions.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_team_luck import mbb_team_luck
            luck = mbb_team_luck(2025)
            luck.sort("luck_wins", descending=True).select("team_id", "wins", "exp_wins", "luck_wins").head()
    """
    _check_league(league)
    load_possessions, _, load_schedule = _league_loaders(league)
    schedule = load_schedule(seasons)
    possessions = load_possessions(seasons)
    rows = ncaa_possession_rate_rows(possessions, schedule, league=league)
    out = team_luck(
        espn_game_results(schedule),
        _espn_close_flags(possessions, rows),
        exponent=PYTHAG_EXPONENT[_EXPONENT_KEY[league]],
    )
    return out.to_pandas() if return_as_pandas else out
