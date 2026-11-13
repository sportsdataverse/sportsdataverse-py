"""College player on/off and WOWY pairs (BB-D5): ``mbb.player_on_off`` / ``mbb.player_wowy`` and the wbb twins.

Builds :mod:`sportsdataverse._common.player_on_off` from the two stats.ncaa.org releases the
college game impact box reads (:mod:`sportsdataverse.mbb.mbb_game_impact`): a possession belongs to
the lineup that started it, so a player's on and off possessions add up to his team's.

``team_id`` is the ESPN team id (Int64) and the player ids are stats.ncaa.org ids (String), as in
the game impact box. A non-D1 opponent has no ESPN team id, so its players get no row. In WOWY,
``player_id_a`` sorts before ``player_id_b`` as a string.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.rate_gates import NCAA_LEVEL
from sportsdataverse._common.player_on_off import player_on_off, player_wowy
from sportsdataverse.mbb.mbb_game_impact import _check_league, _load_releases, _load_rows, ncaa_impact_rows
from sportsdataverse.mbb.mbb_lineup_seasons import MIN_POSS

__all__ = ["mbb_player_on_off", "mbb_player_wowy"]

_PLAYER = pl.Utf8()


def _game_dates(pbp: pl.DataFrame) -> pl.DataFrame:
    """``game_id`` (the contest id, String) and ``game_date`` (Date) from stats.ncaa.org pbp (``MM/DD/YYYY``)."""
    if pbp.height == 0 or "game_date" not in pbp.columns:
        return pl.DataFrame(schema={"game_id": pl.Utf8, "game_date": pl.Date})
    return (
        pbp.select(
            pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
            pl.col("game_date").cast(pl.Utf8).str.to_date("%m/%d/%Y", strict=False),
        )
        .drop_nulls()
        .unique("game_id")
    )


def mbb_player_on_off(
    seasons: int | list[int],
    *,
    league: str = "mens",
    min_poss: int = MIN_POSS,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """College player on/off: each player's team with him on the floor and without him, per team-season.

    The definitions are in :mod:`sportsdataverse._common.player_on_off`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_player_on_off.wbb_player_on_off`).
        min_poss: The rank gate, offensive plus defensive possessions both on and off the floor
            (default 200).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id)``: ``games`` on the floor and ``team_games``, then for ``_on`` and
        ``_off`` the ``minutes``, ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``, ``plus_minus``
        and ``off_rtg`` / ``def_rtg`` / ``net_rtg`` (per 100 possessions); ``<rating>_diff`` (on
        minus off); ``<m>_rank`` / ``<m>_pct`` for the on and diff ratings (within the season,
        among players with at least ``min_poss`` both ways) and ``<m>_n`` for every rating.
        ``poss_on + poss_off`` is the team's possessions. Then his span with the team:
        ``team_games_span`` (the team's games from his first game for it, or the day after his
        last for an earlier team that season, to the day before his first for a later team, else
        the season's end, by the pbp's ``game_date``), ``span_first_date`` / ``span_last_date``, the off side over that span
        (``<total>_off_span`` and ``<rating>_off_span``, not ranked) and ``<rating>_diff_span``
        (on minus it). ``team_id`` is the ESPN team id (Int64),
        ``player_id`` the NCAA id (String). Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_player_on_off import mbb_player_on_off
            on_off = mbb_player_on_off(2025)
            on_off.sort("net_rtg_diff_rank").head()
    """
    _check_league(league)
    pbp, possessions = _load_releases(seasons, league)
    out = player_on_off(
        ncaa_impact_rows(pbp, possessions, league=league),
        player_dtype=_PLAYER,
        min_poss=min_poss,
        level=NCAA_LEVEL[league],
        game_dates=_game_dates(pbp),
    )
    return out.to_pandas() if return_as_pandas else out


def mbb_player_wowy(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College WOWY pairs: a team's season with two teammates together, apart and both off, per pair.

    The definitions are in :mod:`sportsdataverse._common.player_on_off`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_player_on_off.wbb_player_wowy`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id_a, player_id_b)`` (``player_id_a`` sorting first
        as a string) for every pair of the team's players: for ``_both`` (together), ``_a_only``,
        ``_b_only`` and ``_neither`` the ``minutes``, ``poss`` / ``opp_poss``, ``pts`` /
        ``opp_pts``, ``plus_minus`` and ``off_rtg`` / ``def_rtg`` / ``net_rtg``. The four splits'
        totals add up to the team's season; the ratings do not add. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_player_on_off import mbb_player_wowy
            pairs = mbb_player_wowy(2025)
            pairs.sort("minutes_both", descending=True).head()
    """
    _check_league(league)
    out = player_wowy(_load_rows(seasons, league), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out
