"""WNBA EPV and EPV added (BB-D24): ``wnba.epv_plays`` / ``_player_games`` / ``_lineup_games`` / ``_team_games``.

The states, labels, EPV added and credit are :mod:`sportsdataverse.nba.nba_epv`'s, on the
``wnba_stats_pbp`` / ``_possessions`` / ``_game_lineups`` releases, scored by the WNBA booster
(``league_id`` ``"10"``). That booster carries an as-of scoring level
(:func:`sportsdataverse.nba.nba_epv.epv_level`), which reads each game's date from the
``wnba_stats_schedules`` release, so a game never sees one played the same day or later.

Seasons are calendar years (the WNBA season is its year), served from the booster's first
training season (2022) on. Playoff games are scored with the same booster and level, but the
fit and its gates use regular-season games only. The player, lineup and team tables are
accounting sums of EPV added (like +/-), with no interval. The booster's fit and gates are in
``tools/models/nba_epv/epv_wnba_holdout.json`` and its card, ``sportsdataverse/nba/models/wnba_epv.card.json``:
it passes every gate on the 2025 holdout and the 2026 confirmation but one state bucket on 2026,
which the owner accepted as a stated exception (a shooting foul in a transition possession,
over-priced by about 0.12 points).

Example:
    Quick start::

        from sportsdataverse.wnba.wnba_epv import wnba_epv_team_games
        teams = wnba_epv_team_games(2026)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse.nba import nba_epv as E

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["wnba_epv_lineup_games", "wnba_epv_player_games", "wnba_epv_plays", "wnba_epv_team_games"]

_LEAGUE = "10"


def _game_order(schedule: pl.DataFrame) -> pl.DataFrame:
    """``game_id`` and ``game_order`` (the game's date) from the ``wnba_stats_schedules`` release."""
    return schedule.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d").alias("game_order"),
    ).unique()


def _load(seasons: int | list[int], *, lineups: bool = False) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    """The scored event rows of ``seasons`` and, asked, the game lineups."""
    from sportsdataverse.wnba import wnba_loaders as W

    seasons = E._check_seasons(seasons, _LEAGUE)
    pbp, poss = W.load_wnba_stats_pbp(seasons), W.load_wnba_stats_possessions(seasons)
    order = _game_order(W.load_wnba_stats_schedules(seasons))
    tens = W.load_wnba_stats_game_lineups(seasons) if lineups else None
    return E.add_epv(E.epv_states(poss, pbp), league_id=_LEAGUE, game_order=order), tens


def _out(frame: pl.DataFrame, return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    return frame.to_pandas() if return_as_pandas else frame


def wnba_epv_plays(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """EPV and EPV added on every WNBA play-by-play event inside a possession.

    Args:
        seasons: A season or list of seasons (the WNBA season is its year), from the booster's first
            training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        The columns of :func:`sportsdataverse.nba.nba_epv.nba_epv_plays`, one row per event in game
        order, with stats.wnba.com ids. Every game of every season type is scored.

    Raises:
        ValueError: If a season is before the first one served, or a game has no date in the
            schedule release (a schedule that lags the play-by-play fails the season loudly).

    Example:
        The biggest plays of a season::

            from sportsdataverse.wnba.wnba_epv import wnba_epv_plays
            plays = wnba_epv_plays(2026)
            plays.sort("epv_added", descending=True).head(5)
    """
    return _out(_load(seasons)[0], return_as_pandas)


def wnba_epv_player_games(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per WNBA player and game: EPV added on her own actions and on-court EPV +/-.

    Args:
        seasons: A season or list of seasons (the WNBA season is its year), from the booster's first
            training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        :func:`sportsdataverse.nba.nba_epv.epv_player_games` of the seasons' plays, on-court from
        the ``wnba_stats_game_lineups`` release: one row per ``(season, game_id, team_id,
        player_id)`` with ``epv_own``, ``own_actions``, ``off_epv_on``, ``def_epv_on`` and ``epv_on``.

    Raises:
        ValueError: If a season is before the first one served, or a game has no date in the
            schedule release (a schedule that lags the play-by-play fails the season loudly).

    Example:
        A season's EPV boxes::

            from sportsdataverse.wnba.wnba_epv import wnba_epv_player_games
            wnba_epv_player_games(2026).sort("epv_on", descending=True).head()
    """
    return _out(E.epv_player_games(*_load(seasons, lineups=True)), return_as_pandas)


def wnba_epv_lineup_games(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per WNBA five-woman unit and game: the EPV added with the unit on the floor.

    Args:
        seasons: A season or list of seasons (the WNBA season is its year), from the booster's first
            training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        :func:`sportsdataverse.nba.nba_epv.epv_lineup_games` of the seasons' plays: one row per
        ``(season, game_id, team_id, lineup_id)`` with ``off_epv``, ``def_epv``, ``net_epv`` and ``plays``.

    Raises:
        ValueError: If a season is before the first one served, or a game has no date in the
            schedule release (a schedule that lags the play-by-play fails the season loudly).

    Example:
        A season's units::

            from sportsdataverse.wnba.wnba_epv import wnba_epv_lineup_games
            wnba_epv_lineup_games(2026).sort("net_epv", descending=True).head()
    """
    return _out(E.epv_lineup_games(*_load(seasons, lineups=True)), return_as_pandas)


def wnba_epv_team_games(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per WNBA team and game: EPV added on offense and allowed on defense.

    Args:
        seasons: A season or list of seasons (the WNBA season is its year), from the booster's first
            training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        :func:`sportsdataverse.nba.nba_epv.epv_team_games` of the seasons' plays: one row per
        ``(season, game_id, team_id)`` with ``opp_team_id``, ``off_epv``, ``def_epv``, ``net_epv``,
        ``poss``, ``opp_poss``, ``off_epv_per_poss`` and ``def_epv_per_poss``.

    Raises:
        ValueError: If a season is before the first one served, or a game has no date in the
            schedule release (a schedule that lags the play-by-play fails the season loudly).

    Example:
        A season's offenses by EPV added per possession::

            import polars as pl
            from sportsdataverse.wnba.wnba_epv import wnba_epv_team_games
            teams = wnba_epv_team_games(2026)
            teams.group_by("team_id").agg(pl.col("off_epv_per_poss").mean()).sort("off_epv_per_poss")
    """
    return _out(E.epv_team_games(_load(seasons)[0]), return_as_pandas)
