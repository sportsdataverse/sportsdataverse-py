"""CFB player-level havoc counts: ``cfb.cfb_player_havoc``.

Team havoc (``havoc`` on the play-by-play, ``def_havoc`` in :func:`cfb_advanced_stats`) is a share
of plays. This is the player version, made from box counts: tackles for loss, sacks, pass breakups,
interceptions and forced fumbles per player-season, each per game, with the season's ``_rank`` /
``_pct`` / ``_n``. It counts events. It is not a grade and is not adjusted for opponent, role or
snaps.
"""

from __future__ import annotations

import warnings

import pandas as pd
import polars as pl

from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS, attach_ranks, attach_sample_sizes
from sportsdataverse.cfb.cfb_loaders import load_cfb_adv_defensive_players, load_cfb_player_box

__all__ = ["cfb_player_havoc"]

#: Counting columns -> their per-game rate. ``havoc`` is the sum of the other components except ``sacks``.
_RATES: dict[str, str] = {
    "havoc": "havoc_per_game",
    "tfl": "tfl_per_game",
    "sacks": "sacks_per_game",
    "pass_breakups": "pass_breakups_per_game",
    "interceptions": "interceptions_per_game",
    "forced_fumbles": "forced_fumbles_per_game",
}

_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "athlete_id": pl.Int64(),
    "athlete_name": pl.Utf8(),
    "games": pl.Int64(),
    **{c: pl.Float64() for c in _RATES},
    **{
        f"{r}{s}": (pl.Int64() if s == "_n" else pl.Float64())
        for r in _RATES.values()
        for s in ("", "_n", "_rank", "_pct")
    },
}

_KEYS = ["season", "game_id", "team_id", "athlete_id"]


def _stat(df: pl.DataFrame, col: str, category: str) -> pl.Expr:
    """Sum of a string box stat over one category's rows (0 when the column is absent)."""
    if col not in df.columns:
        return pl.lit(0.0)
    return pl.col(col).cast(pl.Float64, strict=False).filter(pl.col("category") == category).sum()


def _box_games(player_box: pl.DataFrame) -> pl.DataFrame:
    """One row per player-game from ESPN's ``defensive`` and ``interceptions`` box lines."""
    box = player_box.filter(pl.col("category").is_in(["defensive", "interceptions"]))
    return box.group_by(_KEYS).agg(
        pl.col("athlete_name").drop_nulls().first(),
        _stat(box, "tacklesForLoss", "defensive").alias("tfl"),
        _stat(box, "sacks", "defensive").alias("sacks"),
        _stat(box, "passesDefended", "defensive").alias("pass_breakups"),
        # the ``interceptions`` category is the defender's; the ``passing`` one is the thrower's
        _stat(box, "interceptions", "interceptions").alias("interceptions"),
    )


def _forced_fumbles(defensive_players: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """Play-by-play forced fumbles per player-game, keyed to the box's ESPN athlete id by name."""
    ff = (
        defensive_players.filter(pl.col("forced_fumbles").fill_null(0) > 0)
        .select(
            pl.col("season").cast(pl.Int64),
            pl.col("game_id").cast(pl.Int64),
            pl.col("def_pos_team_id").cast(pl.Int64).alias("team_id"),
            pl.col("player_name").alias("athlete_name"),
            pl.col("forced_fumbles").cast(pl.Float64),
        )
        .group_by("season", "game_id", "team_id", "athlete_name")
        .agg(pl.col("forced_fumbles").sum())
    )
    # The pbp release names a defender but carries no id; his season box lines (same team, same
    # display name) give it, also for a game where he has no box line of his own.
    ids = (
        games.select("season", "team_id", "athlete_name", "athlete_id")
        .drop_nulls()
        .unique(["season", "team_id", "athlete_name"], keep="first")
    )
    for k in ("season", "team_id"):
        assert ff.schema[k] == ids.schema[k], k
    ff = ff.join(ids, on=["season", "team_id", "athlete_name"], how="left")
    unmatched = ff.filter(pl.col("athlete_id").is_null())
    if unmatched.height:
        # ponytail: exact-name match only; a fuzzy or roster-id join is the upgrade if this share grows.
        warnings.warn(
            f"cfb_player_havoc: {int(unmatched['forced_fumbles'].sum())} of {int(ff['forced_fumbles'].sum())} "
            "forced fumbles name a player with no box line for his team that season; they are not counted.",
            UserWarning,
            stacklevel=3,
        )
    return ff.filter(pl.col("athlete_id").is_not_null()).select(*_KEYS, "athlete_name", "forced_fumbles")


def _season_totals(player_box: pl.DataFrame, defensive_players: pl.DataFrame) -> pl.DataFrame:
    """Season totals and per-game rates per ``(season, team_id, athlete_id)``, box lines joined to forced fumbles."""
    player_box = player_box.with_columns(pl.col(k).cast(pl.Int64) for k in _KEYS)
    games = _box_games(player_box)
    if defensive_players.height and "forced_fumbles" in defensive_players.columns:
        ff = _forced_fumbles(defensive_players, games)
        for k in _KEYS:
            assert games.schema[k] == ff.schema[k], k
        games = games.join(ff, on=_KEYS, how="full", coalesce=True).with_columns(
            pl.coalesce("athlete_name", "athlete_name_right").alias("athlete_name")
        )
    else:
        games = games.with_columns(pl.lit(0.0).alias("forced_fumbles"))
    counts = [c for c in _RATES if c != "havoc"]
    games = games.with_columns(pl.col(c).fill_null(0.0) for c in counts)
    return (
        games.sort("game_id")
        .group_by("season", "team_id", "athlete_id")
        .agg(
            pl.col("athlete_name").drop_nulls().last(),
            pl.len().cast(pl.Int64).alias("games"),
            *[pl.col(c).sum() for c in counts],
        )
        .with_columns(
            (pl.col("tfl") + pl.col("pass_breakups") + pl.col("interceptions") + pl.col("forced_fumbles")).alias(
                "havoc"
            )
        )
        .with_columns((pl.col(c) / pl.col("games")).alias(r) for c, r in _RATES.items())
    )


def cfb_player_havoc(
    seasons: int | list[int],
    *,
    min_games: int = 6,
    player_box: pl.DataFrame | None = None,
    defensive_players: pl.DataFrame | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """CFB player havoc per season: tackles for loss, sacks, pass breakups, interceptions and forced fumbles, per game.

    These are counting stats, not a grade. A defender's havoc is his tackles for loss, pass
    breakups, interceptions and forced fumbles, the player version of the team havoc rate. Sacks
    are listed and ranked on their own but not added again, because a sack is already a tackle for
    loss. Events are counted, not plays: a strip-sack is two events for the same player, where team
    havoc counts that play once. Nothing is adjusted for opponent, scheme or position, so an edge
    rusher and a corner are ranked on the same scale.

    Tackles for loss (half credit on a shared one), sacks, pass breakups and interceptions come
    from ESPN's player box score. In the college box, ``passesDefended`` excludes the defender's
    interceptions, so the two are added without double counting. Forced fumbles come from the
    play-by-play (:func:`load_cfb_adv_defensive_players`), which only credits a forced fumble when
    ESPN's play text names the player. Measured on 2025, that is about 0.5 a game, so this column
    undercounts. College football has no public snap counts, so every rate is per game, and a game
    counts only when the player has a defensive or interception box line in it or the play text
    credits him with a forced fumble in it. ESPN's defensive
    box covers 935 of 958 games in 2025 but 473 in 2023 and 154 in 2021, so earlier seasons are
    partial. No PFF data is used.

    Args:
        seasons: A season or list of seasons (2004 or later for the box release; the defensive
            box lines are near-complete from 2025).
        min_games: A rate is ranked only for a player with at least this many games (its ``_n``).
            Defaults to 6, half a regular season.
        player_box: An ESPN player box frame shaped like :func:`load_cfb_player_box` (no download).
        defensive_players: A frame shaped like :func:`load_cfb_adv_defensive_players` (no download).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, athlete_id)`` (ESPN ids, Int64): ``athlete_name``,
        ``games``, the season totals ``havoc`` / ``tfl`` / ``sacks`` / ``pass_breakups`` /
        ``interceptions`` / ``forced_fumbles`` (Float64, since a shared tackle for loss is a half),
        and for each total its ``<stat>_per_game`` with ``_n`` (games), ``_rank`` (1 is the most)
        and the Weibull ``_pct``. These are ranked within the season among players with at least
        ``min_games`` games, and are null below that, or when fewer than 10 players qualify.
        A season with no box lines gives that schema with zero rows.

    Raises:
        SeasonNotFoundError: If a season is before 2004 (raised by the loaders).

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_player_havoc
            havoc = cfb_player_havoc(2025)
            havoc.sort("havoc_per_game_rank").select(
                "athlete_name", "games", "havoc", "havoc_per_game", "havoc_per_game_pct"
            ).head()

        See Also:
            * :func:`sportsdataverse.cfb.cfb_advanced_stats` -- the team havoc rate per play
            * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package
    """
    if player_box is None:
        player_box = load_cfb_player_box(seasons)
    if defensive_players is None:
        defensive_players = load_cfb_adv_defensive_players(seasons)
    player_box, defensive_players = (
        pl.from_pandas(f) if isinstance(f, pd.DataFrame) else f for f in (player_box, defensive_players)
    )
    if player_box.height == 0 or "category" not in player_box.columns:
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    season = _season_totals(player_box, defensive_players)
    rates = list(_RATES.values())
    season = attach_sample_sizes(season, {r: pl.col("games") for r in rates})
    season = attach_ranks(
        season, rates, min_cohort=MIN_COHORT_PLAYERS, by=("season",), min_n=dict.fromkeys(rates, min_games)
    )
    out = season.select([pl.col(c).cast(t) for c, t in _SCHEMA.items()]).sort(
        "season", "havoc_per_game_rank", "athlete_id", nulls_last=True
    )
    return out.to_pandas() if return_as_pandas else out
