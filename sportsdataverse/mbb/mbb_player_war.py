"""College WAR and VORP (BB-D26): ``mbb.player_war`` (and wbb).

Our own wins and value above replacement from model ① of the player-value spine
(``specs/2026-07-07-mbb-wbb-player-value-projection-design.md``): box BPM
(:func:`~sportsdataverse.mbb.mbb_box_bpm.mbb_box_bpm`), the per-player-season currency every
other college value model reads. :mod:`sportsdataverse._common.player_war` documents the method,
the replacement level and the minutes basis.

College has no BOBAN, and its RAPM (``ncaa_{mbb,wbb}_rapm``) is keyed on stats.ncaa.org ids with
no crosswalk to the ESPN ids of the box score, so the rating is box BPM alone. Box BPM is
already team-constrained to the adjusted efficiency margin; it is re-anchored and centred on the
Division I teams (10 or more games in :func:`~sportsdataverse.mbb.mbb_team_ratings.mbb_team_ratings`,
box BPM's own centring set), so ``war`` is wins above replacement against an average D1
schedule. Possessions are minutes / 40 times the team's adjusted tempo.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.player_war import player_war
from sportsdataverse.mbb.mbb_loaders import load_mbb_player_boxscore
from sportsdataverse.mbb.mbb_box_bpm import mbb_box_bpm
from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_player_war"]

_LEAGUE = {"mens": "mbb", "womens": "wbb"}
#: A Division I team: box BPM's centring set (the ratings also list every non-D1 opponent).
_D1_MIN_GAMES = 10
_GAME_MINUTES = 40
_D1 = pl.col("games") >= _D1_MIN_GAMES


def _war_stints(box_bpm: pl.DataFrame, ratings: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The rating stints and team net ratings :func:`~sportsdataverse._common.player_war.player_war` reads.

    Args:
        box_bpm: :func:`~sportsdataverse.mbb.mbb_box_bpm.mbb_box_bpm` rows.
        ratings: :func:`~sportsdataverse.mbb.mbb_team_ratings.mbb_team_ratings` rows of the same seasons.

    Returns:
        ``(stints, net)``: one row per ``(season, team_id, player_id)`` of a D1 team with
        ``player_name``, ``min``, ``poss`` (minutes / 40 * adj_tempo) and ``bpm``; and one row per
        D1 ``(season, team_id)`` with ``net``, its ``adj_em`` minus the D1 mean.
    """
    d1 = ratings.filter(_D1).select(
        pl.col("season").cast(pl.Int64),
        pl.col("team_id").cast(pl.Utf8),
        "adj_tempo",
        (pl.col("adj_em") - pl.col("adj_em").mean().over("season")).alias("net"),
    )
    box = box_bpm.select(
        pl.col("season").cast(pl.Int64),
        pl.col("team_id").cast(pl.Utf8),
        pl.col("player_id").cast(pl.Utf8),
        pl.col("player").alias("player_name"),
        "min",
        pl.col("box_bpm").alias("bpm"),
    )
    assert box.schema["team_id"] == d1.schema["team_id"], "team_id dtype differs between box BPM and ratings"
    stints = box.join(
        d1.select("season", "team_id", "adj_tempo"), on=["season", "team_id"], how="inner", validate="m:1"
    )
    stints = stints.with_columns((pl.col("min") / _GAME_MINUTES * pl.col("adj_tempo")).alias("poss")).drop("adj_tempo")
    return stints, d1.select("season", "team_id", "net")


def _game_rows(box: pl.DataFrame, ratings: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Per-game possessions behind :func:`_war_stints`, for the WAR interval.

    Args:
        box: ``load_mbb_player_boxscore`` / ``load_wbb_player_boxscore`` rows of the seasons.
        ratings: The same seasons' team ratings.

    Returns:
        ``(player_games, team_games)`` for the D1 teams: one row per ``(season, team_id, game_id,
        player_id)`` with ``poss`` (minutes / 40 * adj_tempo, the minutes box BPM sums: positive
        and set), and one per ``(season, team_id, game_id)`` with the team's ``poss`` (its players'
        over 5) and ``margin`` (``team_score - opponent_team_score``).
    """
    d1 = ratings.filter(_D1).select(pl.col("season").cast(pl.Int64), pl.col("team_id").cast(pl.Utf8), "adj_tempo")
    rows = box.filter(pl.col("minutes").is_not_null() & (pl.col("minutes") > 0)).select(
        pl.col("season").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("game_id").cast(pl.Utf8),
        pl.col("athlete_id").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
        pl.col("minutes").cast(pl.Float64),
        (pl.col("team_score").cast(pl.Float64) - pl.col("opponent_team_score").cast(pl.Float64)).alias("margin"),
    )
    assert rows.schema["team_id"] == d1.schema["team_id"], "team_id dtype differs between box and ratings"
    rows = rows.join(d1, on=["season", "team_id"], how="inner", validate="m:1").with_columns(
        (pl.col("minutes") / _GAME_MINUTES * pl.col("adj_tempo")).alias("poss")
    )
    keys = ["season", "team_id", "game_id"]
    player_games = rows.group_by(*keys, "player_id").agg(pl.col("poss").sum())
    team_games = rows.group_by(keys).agg((pl.col("poss").sum() / 5).alias("poss"), pl.col("margin").first())
    return player_games, team_games


def mbb_player_war(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """WAR and VORP per player-season, from our box BPM.

    Box BPM is anchored to the team and centred on Division I, so 0 is an average D1 player.
    ``vorp`` is points per 100 team possessions above a replacement player over one team's
    season of possessions (Basketball-Reference's VORP scale); ``war`` is wins above replacement
    against an average D1 schedule. A replacement-level player has 0, and a season's ``war``
    adds up to the D1 teams' wins minus those of a team of replacement players. The replacement
    level and points per win are fitted per league
    (:data:`~sportsdataverse._common.player_war.REPLACEMENT_LEVEL`,
    :data:`~sportsdataverse._common.player_war.PTS_PER_WIN`).

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_player_war.wbb_player_war`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, player_id)`` (ESPN ids, String) of a D1 team -- one with 10 or
        more games in :func:`~sportsdataverse.mbb.mbb_team_ratings.mbb_team_ratings`, box BPM's
        own centring set, so a season is empty until its teams reach 10 games: ``player_name``,
        ``team_id`` (the team he played most possessions for), ``teams``, ``min``, ``poss``
        (minutes / 40 * the team's adjusted tempo), ``rating`` (anchored box BPM, per 100),
        ``vorp`` and ``war`` with ``war_lo`` / ``war_hi`` (a 90% interval from a bootstrap over each
        team-season's games), with ``_rank`` (1 = most), ``_pct`` and ``_n`` (possessions) for
        ``vorp`` and ``war`` among every player of the season. Sorted by season and ``war``.
        Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"`` (checked before any data is loaded).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_player_war import mbb_player_war
            war = mbb_player_war(2025)
            war.select("player_name", "poss", "rating", "vorp", "war").head(10)
    """
    if league not in _LEAGUE:
        raise ValueError(f"league must be 'mens' or 'womens', got {league!r}")
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    ratings = mbb_team_ratings(seasons_list, league=league)
    stints, net = _war_stints(mbb_box_bpm(seasons_list, league=league), ratings)
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_player_boxscore as load_box
    else:
        load_box = load_mbb_player_boxscore
    player_games, team_games = _game_rows(load_box(seasons_list), ratings)
    out = player_war(
        stints, net, league=_LEAGUE[league], sources=["bpm"], team_games=team_games, player_games=player_games
    )
    return out.to_pandas() if return_as_pandas else out
