"""NBA / WNBA player on/off and WOWY pairs (BB-D5): ``nba.player_on_off`` and ``nba.player_wowy``.

Builds :mod:`sportsdataverse._common.player_on_off` from the four stats.nba.com releases the game
impact box reads (:mod:`sportsdataverse.nba.nba_game_impact`), with the additive rule of the lineup
seasons: a possession belongs to the lineup that started it, so a player's on and off possessions
add up to his team's. ``league_id="10"`` reads the WNBA twins;
:mod:`sportsdataverse.wnba.wnba_player_on_off` binds it.

Checked against stats.nba.com's ``teamplayeronoffsummary`` (2024-25 regular season, three teams'
whole seasons): on the 41 players over 400 possessions each way whose stats.nba.com off-court spans
the team's season, the on-court net rating is a median 0.48 from stats.nba.com's and the on-off
difference a median 0.68 (correlation 0.96). stats.nba.com's off-court covers only the games a
player was on the roster, so for a player traded or signed mid-season its off side is shorter than
the team's season used here. Its ORtg and DRtg each run about 3 below these: it credits a possession
to every player on the floor during it, not only to those who started it.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.player_on_off import player_on_off, player_wowy
from sportsdataverse.nba.nba_game_impact import _check_league, _load_releases, nba_impact_rows
from sportsdataverse.nba.nba_lineup_seasons import _MIN_POSS, _SEASON_TYPES

__all__ = ["nba_player_on_off", "nba_player_wowy"]

_PLAYER = pl.Int64()


def _rows(seasons: int | list[int], league_id: str, season_type: str) -> pl.DataFrame:
    """The game impact rows of ``seasons``, one ``season_type`` of games."""
    _check_league(league_id)
    if season_type not in _SEASON_TYPES:
        raise ValueError(f"season_type must be one of {list(_SEASON_TYPES)}, got {season_type!r}")
    pbp, game_lineups, possessions, schedule = _load_releases(seasons, league_id)
    if schedule.height:
        schedule = schedule.filter(pl.col("season_type") == season_type)
    return nba_impact_rows(pbp, game_lineups, possessions, schedule, league_id=league_id)


def nba_player_on_off(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    min_poss: int | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Player on/off: each player's team with him on the floor and without him, per team-season.

    The definitions are in :mod:`sportsdataverse._common.player_on_off`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or
            ``"play-in"``, matched to the schedule's ``season_type``.
        min_poss: The rank gate, offensive plus defensive possessions both on and off the floor.
            Defaults to 400 (NBA) or 200 (WNBA).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id)``: ``games`` on the floor and ``team_games``, then for ``_on`` and
        ``_off`` the ``minutes``, ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``, ``plus_minus``
        and ``off_rtg`` / ``def_rtg`` / ``net_rtg`` (per 100 possessions); ``<rating>_diff`` (on
        minus off); ``<m>_rank`` / ``<m>_pct`` for the on and diff ratings (within the season,
        among players with at least ``min_poss`` both ways) and ``<m>_n`` for every rating.
        ``poss_on + poss_off`` is the team's possessions. Ids are the stats.nba.com ids (Int64).
        Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"`` or ``season_type`` is not one
            of the four above.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_on_off import nba_player_on_off
            on_off = nba_player_on_off(2025)
            on_off.sort("net_rtg_diff_rank").head()
    """
    rows = _rows(seasons, league_id, season_type)
    gate = _MIN_POSS[league_id] if min_poss is None else min_poss
    out = player_on_off(rows, player_dtype=_PLAYER, min_poss=gate)
    return out.to_pandas() if return_as_pandas else out


def nba_player_wowy(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """WOWY pairs: a team's season with two teammates together, apart and both off, per pair.

    The definitions are in :mod:`sportsdataverse._common.player_on_off`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or
            ``"play-in"``, matched to the schedule's ``season_type``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id_a, player_id_b)`` (``player_id_a`` the smaller
        id) for every pair of the team's players: for ``_both`` (together), ``_a_only``,
        ``_b_only`` and ``_neither`` the ``minutes``, ``poss`` / ``opp_poss``, ``pts`` /
        ``opp_pts``, ``plus_minus`` and ``off_rtg`` / ``def_rtg`` / ``net_rtg``. The four splits'
        totals add up to the team's season; the ratings do not add. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"`` or ``season_type`` is not one
            of the four above.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_on_off import nba_player_wowy
            pairs = nba_player_wowy(2025)
            pairs.filter(team_id=1610612738).head()
    """
    out = player_wowy(_rows(seasons, league_id, season_type), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out
