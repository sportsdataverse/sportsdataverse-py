"""NBA / WNBA player on/off and WOWY pairs (BB-D5): ``nba.player_on_off`` and ``nba.player_wowy``.

Builds :mod:`sportsdataverse._common.player_on_off` from the four stats.nba.com releases the game
impact box reads (:mod:`sportsdataverse.nba.nba_game_impact`), with the additive rule of the lineup
seasons: a possession belongs to the lineup that started it, so a player's on and off possessions
add up to his team's. ``league_id="10"`` reads the WNBA twins;
:mod:`sportsdataverse.wnba.wnba_player_on_off` binds it.

Checked against stats.nba.com's ``teamplayeronoffsummary`` (2024-25 regular season, three teams'
whole seasons): on the 56 players over 400 possessions each way the on-off difference is a median
0.80 from stats.nba.com's (max 3.96, correlation 0.968). stats.nba.com's off-court covers only the
games a player was on the roster; the ``_off`` side here (``tenure=True``) approximates that window:
his span with the team, from his first game listed for it (on the floor, or in its box score as a
DNP, or on its inactive list; or the day after his last for an earlier team that season) to his last
listed game, or the day before his first for a later team. On the 26 players whose stats.nba.com
window is shorter than the season (traded, signed or waived) the span cuts the median off-court net
rating miss from 2.96 (the whole season) to 0.59; on the 44 whose window is the whole season it is
the whole season's 0.26, and the off-court minutes are a median 2.9 from stats.nba.com's over all 70.
From game dates alone (a span opening at a late debut) those were 1.27, 0.40 and 128 minutes
(``tests/nba/test_nba_player_on_off.py``). Its ORtg and DRtg each run about 3 below these: it credits
a possession to every player on the floor during it, not only to those who started it.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.rate_gates import NBA_LEVEL
from sportsdataverse._common.player_on_off import player_on_off, player_wowy
from sportsdataverse.nba.nba_game_impact import _check_league, _load_releases, nba_impact_rows
from sportsdataverse.nba.nba_lineup_seasons import _MIN_POSS, _SEASON_TYPES

__all__ = ["nba_player_on_off", "nba_player_wowy"]

_PLAYER = pl.Int64()
_ROSTER = {"team_id": pl.Int64(), "player_id": _PLAYER, "game_id": pl.Utf8()}


def _rows_and_dates(seasons: int | list[int], league_id: str, season_type: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The game impact rows of ``seasons``, one ``season_type`` of games, and those games' dates."""
    _check_league(league_id)
    if season_type not in _SEASON_TYPES:
        raise ValueError(f"season_type must be one of {list(_SEASON_TYPES)}, got {season_type!r}")
    pbp, game_lineups, possessions, schedule = _load_releases(seasons, league_id)
    if schedule.height:
        schedule = schedule.filter(pl.col("season_type") == season_type)
    rows = nba_impact_rows(pbp, game_lineups, possessions, schedule, league_id=league_id)
    return rows, _game_dates(schedule)


def _game_dates(schedule: pl.DataFrame) -> pl.DataFrame:
    """``game_id`` (String) and ``game_date`` (Date) from a stats.nba.com schedule (``YYYY-MM-DD``)."""
    if "game_date" not in schedule.columns:
        return pl.DataFrame(schema={"game_id": pl.Utf8, "game_date": pl.Date})
    return schedule.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False),
    )


def _rosters(seasons: int | list[int], league_id: str) -> pl.DataFrame:
    """``team_id``, ``player_id``, ``game_id`` of every player listed for a game: its box score (DNPs
    included) and its inactive list, the roster the span reads (:func:`player_on_off`'s ``rosters``)."""
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if league_id == "10":
        from sportsdataverse.wnba import wnba_loaders as W

        # ponytail: the WNBA box release starts at 2026; before it only the (sparse) inactive lists
        box_seasons = [s for s in seasons_list if s >= 2026]
        box = W.load_wnba_stats_player_boxscores(box_seasons) if box_seasons else pl.DataFrame()
        inactive = W.load_wnba_stats_game_rosters(seasons_list)
    else:
        from sportsdataverse.nba import nba_loaders as N

        args = [s - 1 for s in seasons_list]  # the NBA loaders take the start year
        box, inactive = N.load_nba_stats_player_boxscores(args), N.load_nba_stats_game_rosters(args)
    cols = [pl.col("team_id").cast(pl.Int64), pl.col("player_id").cast(_PLAYER), pl.col("game_id").cast(pl.Utf8)]
    parts = [f.rename({"person_id": "player_id"}, strict=False) for f in (box, inactive) if f.height]
    return pl.concat([f.select(cols) for f in parts]) if parts else pl.DataFrame(schema=_ROSTER)


def _rows(seasons: int | list[int], league_id: str, season_type: str) -> pl.DataFrame:
    """The game impact rows of ``seasons``, one ``season_type`` of games."""
    return _rows_and_dates(seasons, league_id, season_type)[0]


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
        The off side counts only the team's games in his span with it, as stats.nba.com does:
        ``team_games_span`` (the team's games from his first game listed for it, on the floor,
        as a box-score DNP or on its inactive list, or the day after his last for an earlier
        team that season, to his last listed game or the day before his first for a later team,
        by the schedule's ``game_date``), ``span_first_date`` /
        ``span_last_date``. So ``poss_on + poss_off`` is the team's possessions over his span, and
        for a player traded or signed mid-season the off side and the difference are his time with
        the team. ``<total>_off_span`` / ``<rating>_off_span`` / ``<rating>_diff_span`` repeat the
        ``_off`` / ``_diff`` columns (a player with no dated game keeps the whole season in ``_off``
        and null spans). Ids are the stats.nba.com ids (Int64).
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
    rows, dates = _rows_and_dates(seasons, league_id, season_type)
    gate = _MIN_POSS[league_id] if min_poss is None else min_poss
    out = player_on_off(
        rows,
        player_dtype=_PLAYER,
        min_poss=gate,
        level=NBA_LEVEL[league_id],
        game_dates=dates,
        tenure=True,
        rosters=_rosters(seasons, league_id),
    )
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
