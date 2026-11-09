"""NBA / WNBA neutral-site games: a rule plus a small curated list.

The stats.nba.com schedule release has no neutral-site or arena column, and ESPN's ``neutral_site``
flag is wrong both ways: it misses the 2020 bubble, every game before 2013 and some Cup and abroad
games since, and it flags the All-Star games. So a game is neutral when either:

* it is in the bubble: the NBA's 2020 restart at Walt Disney World (2020-07-30 to 2020-10-11,
  seeding games, play-in and playoffs) or the WNBA's 2020 season at IMG Academy (2020-07-25 to
  2020-10-06); or
* its stats ``game_id`` is in ``sportsdataverse/nba/data/neutral_site_games.csv``: the
  regular-season games played abroad (Tokyo, Saitama, Mexico City, London, Paris, Berlin,
  Manchester, Vancouver) and the NBA Cup knockout games in Las Vegas (2023-24 to 2025-26). Each row
  cites a source; ``neutral_site_games.md`` beside it says what is left out and why.

A team's designated "home" games in another of its own arenas (the 2020-21 Raptors in Tampa, the
Spurs in Austin, the 2005-07 Hornets in New Orleans) stay home games.

Example:
    Quick start::

        from sportsdataverse.nba import nba_neutral_site_games
        abroad = nba_neutral_site_games([2025, 2026])
"""

from __future__ import annotations

import datetime as dt
import io
from functools import lru_cache
from importlib.resources import files
from typing import Literal, overload

import pandas as pd
import polars as pl

__all__ = ["nba_neutral_site_games"]

NEUTRAL_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "league": pl.Utf8(),
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "game_date": pl.Date(),
    "season_type": pl.Utf8(),
    "home_team": pl.Utf8(),
    "away_team": pl.Utf8(),
    "arena": pl.Utf8(),
    "city": pl.Utf8(),
    "country": pl.Utf8(),
    "reason": pl.Utf8(),
    "source_url": pl.Utf8(),
}
_LEAGUE = {"00": "nba", "10": "wnba"}
#: Every game in these date ranges was played in the league's 2020 bubble.
BUBBLES: dict[str, tuple[dt.date, dt.date]] = {
    "00": (dt.date(2020, 7, 30), dt.date(2020, 10, 11)),
    "10": (dt.date(2020, 7, 25), dt.date(2020, 10, 6)),
}


@lru_cache(maxsize=1)
def _bundled() -> pl.DataFrame:
    raw = (files("sportsdataverse.nba") / "data" / "neutral_site_games.csv").read_bytes()
    return pl.read_csv(io.BytesIO(raw), schema=NEUTRAL_GAMES_SCHEMA)


def _check_league(league_id: str) -> str:
    if league_id not in _LEAGUE:
        raise ValueError(f"league_id must be '00' or '10', got {league_id!r}")
    return _LEAGUE[league_id]


@overload
def nba_neutral_site_games(
    seasons: list[int] | None = None, *, league_id: str = "00", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def nba_neutral_site_games(
    seasons: list[int] | None = None, *, league_id: str = "00", return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def nba_neutral_site_games(
    seasons: list[int] | None = None, *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """The curated NBA / WNBA neutral-site games: played abroad, or NBA Cup knockout games in Las Vegas.

    The 2020 bubbles are a date rule (:data:`BUBBLES`), not rows. The team ratings, the season sim,
    the rated priors and game flow drop the home-court term for every game in either.

    Args:
        seasons: Seasons to keep, named by the year they end (``2026`` is 2025-26; the WNBA season
            is its calendar year). ``None`` keeps all.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per game: ``league``, ``season``, ``game_id`` (the stats.nba.com id, Utf8),
        ``game_date``, ``season_type`` (``regular-season``, or ``nba-cup`` for a Cup final),
        ``home_team`` / ``away_team`` (stats abbreviations, the designated home team first),
        ``arena``, ``city``, ``country``, ``reason`` (``international`` or ``cup_knockout``) and
        ``source_url``.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_neutral_site_games
            games = nba_neutral_site_games([2026])

        WNBA::

            nba_neutral_site_games(league_id="10")

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    out = _bundled().filter(pl.col("league") == _check_league(league_id))
    if seasons is not None:
        out = out.filter(pl.col("season").is_in(list(seasons)))
    return out.to_pandas() if return_as_pandas else out


def _neutral_site(frame: pl.DataFrame, league_id: str, game_id: str = "game_id") -> pl.Expr:
    """True for a neutral-site game of ``frame``: a stats ``game_id`` on the curated list, or a bubble date.

    The bubble needs a ``game_date`` column; a frame without one (a trimmed fixture) gets the list only.
    """
    ids = nba_neutral_site_games(league_id=league_id).get_column("game_id").to_list()
    out = pl.col(game_id).cast(pl.Utf8).is_in(ids)
    if "game_date" in frame.columns:
        lo, hi = BUBBLES[league_id]
        out = out | pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date().is_between(lo, hi)
    return out.fill_null(False)
