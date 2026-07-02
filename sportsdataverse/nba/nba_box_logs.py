"""NBA per-game box logs + per-100 feature builder (SPM/BPM input substrate)."""

from __future__ import annotations

from typing import Callable, Dict, List, Optional

import polars as pl

from sportsdataverse.nba.nba_stats import nba_stats_leaguegamelog

# per-100 feature columns (snake-cased leaguegamelog player fields)
_STATS = ["pts", "fg3m", "fga", "fta", "ast", "oreb", "dreb", "stl", "blk", "tov", "pf"]


def box_features(
    player_logs: pl.DataFrame,
    team_logs: pl.DataFrame,
    *,
    game_ids: Optional[List[str]] = None,
) -> pl.DataFrame:
    """Aggregate per-player per-100-possession box features over a set of games.

    Restricting ``game_ids`` to a fold's games is the harness leakage guard.

    Args:
        player_logs: Per-player-per-game box lines (``game_id``, ``team_id``,
            ``player_id``, ``min``, and the counting stats in ``_STATS``).
        team_logs: Per-team-per-game lines (``game_id``, ``team_id``, ``min``,
            ``fga``, ``oreb``, ``tov``, ``fta``) for the possession estimate.
        game_ids: Optional subset of ``game_id`` to include (default: all).

    Returns:
        One row per player: ``player_id``, the ``_STATS`` per-100 rates, ``min``
        (total), ``gp`` (games). Empty frame with that schema on empty input.
    """
    if game_ids is not None:
        player_logs = player_logs.filter(pl.col("game_id").is_in(game_ids))
        team_logs = team_logs.filter(pl.col("game_id").is_in(game_ids))
    team = (
        team_logs.with_columns((pl.col("fga") - pl.col("oreb") + pl.col("tov") + 0.44 * pl.col("fta")).alias("poss"))
        .group_by("team_id")
        .agg(pl.col("poss").sum().alias("team_poss"), pl.col("min").sum().alias("team_min"))
    )
    agg = player_logs.group_by("player_id").agg(
        pl.col("team_id").first(),
        pl.col("min").sum().alias("min"),
        pl.len().alias("gp"),
        *[pl.col(s).sum().alias(s) for s in _STATS],
    )
    joined = agg.join(team, on="team_id", how="left")
    player_poss = pl.col("team_poss") * (pl.col("min") / pl.col("team_min"))
    per100 = [pl.when(player_poss > 0).then(pl.col(s) / player_poss * 100.0).otherwise(0.0).alias(s) for s in _STATS]
    return joined.with_columns(per100).select(
        pl.col("player_id").cast(pl.Int64),
        *_STATS,
        pl.col("min").cast(pl.Float64),
        pl.col("gp").cast(pl.Int64),
    )


def nba_box_logs(
    season: str,
    *,
    league_id: str = "00",
    season_type: str = "Regular Season",
    fetch: Optional[Callable[..., pl.DataFrame]] = None,
) -> Dict[str, pl.DataFrame]:
    """Fetch per-player and per-team game logs for a season (bulk, one call each).

    Args:
        season: NBA season in ``"2023-24"`` form.
        league_id: LeagueID (``"00"`` NBA).
        season_type: SeasonType (``"Regular Season"``).
        fetch: Injectable ``nba_stats_leaguegamelog`` replacement for offline tests.

    Returns:
        ``{"player": <per-player-game logs>, "team": <per-team-game logs>}``
        as snake-cased polars frames.

    Example:
        Fetch a season's logs (residential IP)::

            from sportsdataverse.nba import nba_box_logs
            logs = nba_box_logs("2023-24")
            print(logs["player"].shape)
    """
    get = fetch or nba_stats_leaguegamelog
    player = get(
        player_or_team_abbreviation="P",
        season=season,
        league_id=league_id,
        season_type_all_star=season_type,
    )
    team = get(
        player_or_team_abbreviation="T",
        season=season,
        league_id=league_id,
        season_type_all_star=season_type,
    )
    return {"player": player, "team": team}
