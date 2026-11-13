"""NBA per-36 box rates and the usage-adjusted box expectation: ``nba.player_season_box_rates`` / ``nba.player_game_expected_box``.

Thin wrappers over :mod:`sportsdataverse._common.player_box_expected` on the ESPN
``load_nba_player_boxscore`` release.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.player_box_expected import league_player_box_expected

__all__ = ["nba_player_game_expected_box", "nba_player_season_box_rates"]


def nba_player_season_box_rates(
    seasons: int | list[int], *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """NBA per-36 points, rebounds and assists per player-season, ranked, with the season usage.

    Over the regular season (a playoff or play-in game is measured against it), from ESPN's player box. ``pts_per36`` / ``reb_per36`` /
    ``ast_per36`` are 36 x the total / minutes, each with ``_n`` (the minutes) and
    ``_rank`` / ``_pct`` among the season's players whose minutes reach the rate's gate (the
    minutes at which it is half signal on odd / even games). ``usg_pct`` is the season usage
    (FGA + 0.44 FTA + TOV over the team's while on the floor), not ranked.

    Args:
        seasons: A season or list of seasons, named by the year it ends (2026 is 2025-26).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, athlete_id)`` (the ESPN id): ``team_id`` (his last game's team),
        ``games``, ``minutes``, ``pts`` / ``reb`` / ``ast``, ``usg_pct`` with
        ``usg_pct_n``, and the three per-36 rates with ``_n`` / ``_rank`` / ``_pct``. A season
        without a release returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_box_expected import nba_player_season_box_rates
            rates = nba_player_season_box_rates(2026)
            rates.sort("pts_per36_rank").select("athlete_id", "minutes", "pts_per36", "pts_per36_pct").head()

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college basketball

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    out = league_player_box_expected("nba", seasons)["player_season_box_rates"]
    return out.to_pandas() if return_as_pandas else out


def nba_player_game_expected_box(
    seasons: int | list[int], *, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """NBA points, rebounds and assists per player-game against his season, per minute and usage-adjusted.

    For every played player-game: ``<s>_base`` is his season per-minute rate times the game's
    minutes; ``<s>_exp`` scales that by the game's usage and minutes against his season's
    (a Poisson model fitted on earlier seasons, scored on later held-out ones). Both use the
    season without this game (over the regular season (a playoff or play-in game is measured against it)), so a game is never in its own baseline.
    ``<s>_exp`` is null before the model's first held-out season and where a usage is unknown.

    Args:
        seasons: A season or list of seasons, named by the year it ends (2026 is 2025-26).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(game_id, athlete_id)``: ``season``, ``season_type``, ``team_id``,
        ``minutes``, ``usg_pct``, ``pts`` / ``reb`` / ``ast``, the season-without-the-game
        ``rate_games`` / ``rate_minutes`` / ``rate_usg_pct``, and ``pts_base`` / ``reb_base`` /
        ``ast_base`` / ``pts_exp`` / ``reb_exp`` / ``ast_exp``. A season without a release returns
        that schema with zero rows.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_player_box_expected import nba_player_game_expected_box
            games = nba_player_game_expected_box(2026)
            games.with_columns((pl.col("pts") - pl.col("pts_exp")).alias("pts_vs_exp")).sort("pts_vs_exp").tail()

        See Also:
            * `hoopR`_ -- the R companion for NBA and men's college basketball

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    out = league_player_box_expected("nba", seasons)["player_game_expected_box"]
    return out.to_pandas() if return_as_pandas else out
