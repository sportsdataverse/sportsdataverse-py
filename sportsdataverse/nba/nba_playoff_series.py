"""NBA / WNBA playoff series (BB-D43): ``nba.playoff_series_games`` / ``_teams`` / ``_players``.

One series per pair of teams per postseason, keyed and scored from the ESPN schedule, with the
team and player totals summed from the ESPN box scores. The rules are in
:mod:`sportsdataverse._common.playoff_series`: play-in games (ESPN ``season_type`` 5) are not a
series, game 1's home team is ``team_a``, and a series is decided when its leader has the wins
to clinch or plays a later series. ``league_id="10"`` reads the WNBA releases;
:mod:`sportsdataverse.wnba.wnba_playoff_series` binds it.

Every id is ESPN's (game, team, athlete), the keys of the ``nba`` / ``wnba`` game schemas.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.playoff_series import playoff_series_games, playoff_series_players, playoff_series_teams

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_playoff_series", "nba_playoff_series_players", "nba_playoff_series_teams"]


def _loaders(league_id: str):  # type: ignore[no-untyped-def]
    """``(league, schedule, team box, player box)`` loaders for ``league_id`` (ESPN releases)."""
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import (
            load_wnba_player_boxscore,
            load_wnba_schedule,
            load_wnba_team_boxscore,
        )

        return "wnba", load_wnba_schedule, load_wnba_team_boxscore, load_wnba_player_boxscore
    if league_id != "00":
        raise ValueError(f"league_id must be '00' (NBA) or '10' (WNBA), not {league_id!r}")
    from sportsdataverse.nba.nba_loaders import load_nba_player_boxscore, load_nba_schedule, load_nba_team_boxscore

    return "nba", load_nba_schedule, load_nba_team_boxscore, load_nba_player_boxscore


def _games(seasons: int | list[int], league_id: str) -> tuple[pl.DataFrame, list[int], tuple]:
    league, load_schedule, load_team_box, load_player_box = _loaders(league_id)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    games = playoff_series_games(load_schedule(seasons_list), league=league)
    return games, seasons_list, (load_team_box, load_player_box)


def nba_playoff_series(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Every postseason game keyed to its series, with the series score after it.

    A series is every postseason game between one pair of teams in a season; play-in games are
    not a series. ``series_id`` is ``"{season}-{lower ESPN team id}-{higher}"``. ``team_a`` is
    game 1's home team. A series is ``decided`` once its leader has the wins to clinch
    (NBA: four, every round since 2003; WNBA: the Finals only, two through 2004, three 2005-2024,
    four from 2025) or plays a later series that postseason.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per postseason game (ESPN ``season_type`` 3): ``season``, ``series_id``,
        ``round`` (1 = first round .. 4 = Finals, from ESPN's ``type_abbreviation``; null where
        ESPN gives none), ``round_code``, ``round_name`` (the game headline without "Game N"),
        ``game_number`` (by tip-off), ``game_id``, ``game_date``, ``completed``, the home and away
        ``team_id`` / ``abbreviation`` / ``display_name`` / ``score``, ``winner_team_id`` (null
        until final), ``team_a_id``, ``team_b_id``, ``team_a_wins`` / ``team_b_wins`` (the
        series score after this game), and on every row of the series ``series_games``,
        ``decided`` and ``series_winner_team_id`` (null until decided). Empty input returns that
        schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"`` (checked before any load).

    See Also:
        :func:`nba_playoff_series_teams`, :func:`nba_playoff_series_players`; the cores in :mod:`sportsdataverse._common.playoff_series`.

    Example:
        The 2025 Finals::

            from sportsdataverse.nba.nba_playoff_series import nba_playoff_series
            games = nba_playoff_series(2025)
            games.filter(games["round"] == 4).select("game_number", "home_score", "away_score", "team_a_wins", "team_b_wins")
    """
    games, _, _ = _games(seasons, league_id)
    return games.to_pandas() if return_as_pandas else games


def nba_playoff_series_teams(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Each team's record and box totals in each playoff series: the sum of its game boxes.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        Two rows per series with a final game, keyed ``(season, series_id, team_id)`` with
        ``opponent_team_id`` and ``team_abbreviation``: ``games``, ``wins``, ``losses``, ``pts``,
        ``opp_pts`` and the summed counts ``fgm``, ``fga``, ``fg3m``, ``fg3a``, ``ftm``, ``fta``,
        ``oreb``, ``dreb``, ``reb``, ``ast``, ``stl``, ``blk``, ``tov``, ``pf`` over the series'
        final games.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    See Also:
        :func:`nba_playoff_series`, :func:`nba_playoff_series_players`; the cores in :mod:`sportsdataverse._common.playoff_series`.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_playoff_series import nba_playoff_series_teams
            nba_playoff_series_teams(2025).sort("pts", descending=True).head()
    """
    games, seasons_list, (load_team_box, _) = _games(seasons, league_id)
    out = playoff_series_teams(games, load_team_box(seasons_list))
    return out.to_pandas() if return_as_pandas else out


def nba_playoff_series_players(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Each player's totals and per-game line in each playoff series, against the player's regular season.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per player who played (minutes above zero) in a final game of the series, keyed
        ``(season, series_id, team_id, athlete_id)`` with ``athlete_display_name``: ``games``,
        ``min`` and the summed counts as :func:`nba_playoff_series_teams`; ``<stat>_pg`` per
        game for ``min``, ``pts``, ``reb``, ``ast``, ``stl``, ``blk``, ``tov`` and ``fg3m``,
        plus ``ts_pct`` (``PTS / (2 * (FGA + 0.44 * FTA))``); ``reg_games`` and ``reg_<stat>_pg``
        / ``reg_ts_pct`` over the player's regular season (every team played for; null with none);
        and ``<stat>_pg_diff`` / ``ts_pct_diff``, series minus regular season.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    See Also:
        :func:`nba_playoff_series`, :func:`nba_playoff_series_teams`; the cores in :mod:`sportsdataverse._common.playoff_series`.

    Example:
        Who rose the most in the 2025 Finals::

            from sportsdataverse.nba.nba_playoff_series import nba_playoff_series_players
            p = nba_playoff_series_players(2025)
            p.filter(p["series_id"] == "2025-11-25").sort("pts_pg_diff", descending=True).head()
    """
    games, seasons_list, (_, load_player_box) = _games(seasons, league_id)
    out = playoff_series_players(games, load_player_box(seasons_list))
    return out.to_pandas() if return_as_pandas else out
