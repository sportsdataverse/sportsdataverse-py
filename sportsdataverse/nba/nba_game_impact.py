"""NBA / WNBA game impact box (BB-D6): ``nba.game_player_impact`` and ``nba.game_lineups_used``.

Builds the :mod:`sportsdataverse._common.game_impact` tables from four stats.nba.com releases:

* ``nba_stats_pbp`` -- each action's period clock (``seconds_remaining``), running score and
  free-throw / foul flags;
* ``nba_stats_game_lineups`` -- the ten players on the floor at each action, keyed on
  ``(game_id, action_number)``;
* ``nba_stats_possessions`` -- the possessions and their lineups (every row counts, as in
  :mod:`sportsdataverse.nba.nba_team_season_rates`);
* ``nba_stats_schedules`` -- the home and away team of each game.

``league_id="10"`` reads the WNBA twins; :mod:`sportsdataverse.wnba.wnba_game_impact` binds it.

Points come from the change in the running score at each action, so a team's points over a
game are its final score. An action's clock is the time since the previous action in the period
(the period length before the first), which tiles each period exactly, so a team's ``seconds``
over a game are the game's length.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_impact import (
    credit_free_throws_to_foul_lineup,
    game_lineups_used,
    game_player_impact,
    impact_row_schema,
)

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_game_lineups_used", "nba_game_player_impact", "nba_impact_rows"]

#: league_id -> quarter seconds.
_QUARTER = {"00": 720.0, "10": 600.0}
_OT_SECONDS = 300.0
_PLAYER = pl.Int64()
_ROWS = impact_row_schema(_PLAYER)


def _check_league(league_id: str) -> None:
    if league_id not in _QUARTER:
        raise ValueError(f"league_id must be one of {sorted(_QUARTER)}, got {league_id!r}")


def _action_rows(pbp: pl.DataFrame, lineups: pl.DataFrame, games: pl.DataFrame, quarter: float) -> pl.DataFrame:
    """One impact row per play-by-play action, free throws on the lineup of the last foul."""
    on_floor = lineups.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("action_number").cast(pl.Int64),
        *[pl.col(f"home_player_{i}").cast(_PLAYER).alias(f"home_{i}") for i in range(1, 6)],
        *[pl.col(f"away_player_{i}").cast(_PLAYER).alias(f"away_{i}") for i in range(1, 6)],
    ).unique(["game_id", "action_number"], keep="first")
    period_len = pl.when(pl.col("period") <= 4).then(pl.lit(quarter)).otherwise(pl.lit(_OT_SECONDS))
    score = {
        side: pl.col(f"score_{side}").cast(pl.Int64, strict=False).forward_fill().over("game_id").fill_null(0)
        for side in ("home", "away")
    }
    actions = (
        pbp.with_columns(pl.col("game_id").cast(pl.Utf8), pl.col("action_number").cast(pl.Int64))
        .join(games, on="game_id", how="inner", validate="m:1")
        .sort(["game_id", "order_index"])
        .with_columns(
            *[score[s].alias(f"_score_{s}") for s in score],
            (
                pl.col("seconds_remaining").shift(1).over(["game_id", "period"]).fill_null(period_len)
                - pl.col("seconds_remaining")
            ).alias("seconds"),
        )
        .with_columns(
            *[
                (pl.col(f"_score_{s}") - pl.col(f"_score_{s}").shift(1).over("game_id").fill_null(0)).alias(f"{s}_pts")
                for s in score
            ]
        )
        .join(on_floor, on=["game_id", "action_number"], how="left")
    )
    actions = credit_free_throws_to_foul_lineup(
        actions,
        is_free_throw=pl.col("is_free_throw").eq(True),
        is_foul=pl.col("is_foul").eq(True),
    )
    return actions.with_columns(pl.lit(0).alias("home_poss"), pl.lit(0).alias("away_poss"))


def _possession_rows(possessions: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """One impact row per possession, its offense and defense mapped to home and away."""
    poss = possessions.with_columns(pl.col("game_id").cast(pl.Utf8)).join(
        games, on="game_id", how="inner", validate="m:1"
    )
    home_off = pl.col("offense_team_id").cast(pl.Int64) == pl.col("home_team_id")
    return poss.with_columns(
        *[
            pl.when(home_off).then(pl.col(f"off_player_{i}")).otherwise(pl.col(f"def_player_{i}")).alias(f"home_{i}")
            for i in range(1, 6)
        ],
        *[
            pl.when(home_off).then(pl.col(f"def_player_{i}")).otherwise(pl.col(f"off_player_{i}")).alias(f"away_{i}")
            for i in range(1, 6)
        ],
        pl.lit(0.0).alias("seconds"),
        pl.lit(0).alias("home_pts"),
        pl.lit(0).alias("away_pts"),
        home_off.cast(pl.Int64).alias("home_poss"),
        (~home_off).cast(pl.Int64).alias("away_poss"),
    )


def nba_impact_rows(
    pbp: pl.DataFrame,
    game_lineups: pl.DataFrame,
    possessions: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    league_id: str = "00",
) -> pl.DataFrame:
    """Map the stats.nba.com releases onto the game-impact row frame.

    One row per play-by-play action (its clock and points, on the lineup at that action; a free
    throw on the lineup at the last foul) and one per possession (its lineup and which side had
    the ball). Games not in ``schedule`` are dropped.

    Args:
        pbp: ``load_nba_stats_pbp`` / ``load_wnba_stats_pbp`` rows (``game_id``, ``order_index``,
            ``action_number``, ``period``, ``seconds_remaining``, ``score_home``, ``score_away``,
            ``is_free_throw``, ``is_foul``, ``season``).
        game_lineups: ``load_nba_stats_game_lineups`` / ``load_wnba_stats_game_lineups`` rows.
        possessions: ``load_nba_stats_possessions`` / ``load_wnba_stats_possessions`` rows.
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows
            (``game_id``, ``home_team_id``, ``away_team_id``), every season type.
        league_id: ``"00"`` NBA (12-minute quarters) or ``"10"`` WNBA (10-minute quarters).

    Returns:
        Rows in :func:`sportsdataverse._common.game_impact.impact_row_schema` with Int64 player ids.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_game_impact import nba_impact_rows
            rows = nba_impact_rows(L.load_nba_stats_pbp(2025), L.load_nba_stats_game_lineups(2025),
                                   L.load_nba_stats_possessions(2025), L.load_nba_stats_schedules(2025))
    """
    _check_league(league_id)
    if pbp.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema=_ROWS)
    games = schedule.select(
        pl.col("game_id").cast(pl.Utf8), pl.col("home_team_id").cast(pl.Int64), pl.col("away_team_id").cast(pl.Int64)
    ).unique("game_id")
    cols = list(_ROWS)
    parts = [_action_rows(pbp, game_lineups, games, _QUARTER[league_id]).select(cols)]
    if possessions.height:
        parts.append(_possession_rows(possessions, games).select(cols))
    return pl.concat([p.cast(_ROWS) for p in parts])


def _load_rows(seasons: int | list[int], league_id: str) -> pl.DataFrame:
    """Load the four releases for ``seasons`` (named by the year they end) and build the rows."""
    _check_league(league_id)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if league_id == "10":
        from sportsdataverse.wnba import wnba_loaders as L

        args = seasons_list
        return nba_impact_rows(
            L.load_wnba_stats_pbp(args),
            L.load_wnba_stats_game_lineups(args),
            L.load_wnba_stats_possessions(args),
            L.load_wnba_stats_schedules(args),
            league_id=league_id,
        )
    from sportsdataverse.nba import nba_loaders as N

    args = [s - 1 for s in seasons_list]  # the NBA loaders take the start year
    return nba_impact_rows(
        N.load_nba_stats_pbp(args),
        N.load_nba_stats_game_lineups(args),
        N.load_nba_stats_possessions(args),
        N.load_nba_stats_schedules(args),
        league_id=league_id,
    )


def nba_game_player_impact(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """The game impact box: per player and game, minutes, on-court possessions, +/- and ratings.

    The on-court +/- is the official box score's: on the 2025-26 NBA season it equals
    stats.nba.com's ``plus_minus_points`` on 91% of player-games, the rest being on-court lineup
    errors in the source. The definitions are in :mod:`sportsdataverse._common.game_impact`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id)``: ``opp_team_id``, ``minutes`` and
        ``seconds`` on the floor, ``poss`` / ``opp_poss`` (offensive / defensive possessions on the
        floor), ``pts`` / ``opp_pts`` (points scored / allowed while on), ``plus_minus``,
        ``off_rtg`` / ``def_rtg`` / ``net_rtg`` (per 100 possessions) and their ``_n`` sample sizes.
        Ids are the stats.nba.com ids (Int64; ``game_id`` a String). Every game of every season
        type is included. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_impact import nba_game_player_impact
            box = nba_game_player_impact(2026)
            box.filter(game_id="0022500101")
    """
    out = game_player_impact(_load_rows(seasons, league_id), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def nba_game_lineups_used(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """The lineups used: per five-man unit and game, minutes, possessions, +/- and ratings.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, lineup_id)``: ``lineup_id`` (the five player ids
        sorted and joined with ``-``), ``player_1..5``, and the same totals and ratings as
        :func:`nba_game_player_impact`, sorted by minutes within each team and game. Empty input
        returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_impact import nba_game_lineups_used
            units = nba_game_lineups_used(2026)
            units.filter(game_id="0022500101").group_by("team_id").head(5)
    """
    out = game_lineups_used(_load_rows(seasons, league_id), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out
