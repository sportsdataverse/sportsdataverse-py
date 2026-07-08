"""NFL player projection: season rates, delta-method aging curves, and the
Marcel-style projection engine (compute-on-demand, no bundled artifacts).

Methodology (cited, no code copied): Tom Tango's "Marcel the Monkey" forecaster
(recency-weighted rates regressed to the positional mean) and the standard
delta-method aging curve (paired same-player consecutive seasons, playing-time
weighted, chained and peak-normalized).
"""

from __future__ import annotations

from typing import List

import polars as pl

# Counting stats aggregated to season level (all Float64 in the fixtures).
_COUNTING_STATS: List[str] = [
    "completions",
    "attempts",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "receptions",
    "targets",
    "receiving_yards",
    "receiving_tds",
    "receiving_air_yards",
    "fumbles_lost",
]

_RATE_SCHEMA_BASE: dict = {
    "player_id": pl.Utf8,
    "season": pl.Int64,
    "position_group": pl.Utf8,
    "age": pl.Float64,
    "games": pl.Int64,
    "volume": pl.Float64,
    "ppg": pl.Float64,
}


def _col_or_zero(name: str, cols: List[str]) -> pl.Expr:
    return pl.col(name) if name in cols else pl.lit(0.0)


def season_player_rates(weekly: pl.DataFrame, rosters: pl.DataFrame) -> pl.DataFrame:
    """Aggregate weekly player stats to one row per (player_id, season).

    Computes games played, position-specific volume (QB = pass attempts,
    RB = carries + targets, WR/TE and default = targets), PPR points per game,
    and per-game component rates (``<stat>_rate``) for every counting stat
    present in ``weekly``.

    Args:
        weekly (pl.DataFrame): nflverse weekly offense stats (at minimum
            ``player_id, season, week, position_group`` + counting stats +
            ``fantasy_points_ppr``).
        rosters (pl.DataFrame): Season rosters carrying ``player_id, season,
            age``.

    Returns:
        pl.DataFrame: One row per (player_id, season) with ``player_id:Utf8,
        season:Int64, position_group:Utf8, age:Float64, games:Int64,
        volume:Float64, ppg:Float64`` plus ``<stat>_rate`` columns. Empty or
        malformed input returns a zero-row frame with the documented schema.

    Example:
        Quick start::

            import sportsdataverse.nfl as nfl
            from sportsdataverse.nfl.nfl_projection import season_player_rates
            weekly = nfl.load_nfl_player_stats()
            rosters = nfl.load_nfl_rosters([2023])
            rates = season_player_rates(weekly, rosters)

    See Also:
        * `nflverse`_ -- full data ecosystem (R + Python)

    .. _nflverse: https://nflverse.nflverse.com
    """
    stats = [c for c in _COUNTING_STATS if c in weekly.columns]
    schema = dict(_RATE_SCHEMA_BASE)
    for c in stats:
        schema[f"{c}_rate"] = pl.Float64
    if weekly.height == 0 or not {"player_id", "season"}.issubset(weekly.columns):
        return pl.DataFrame(schema=schema)

    wk = weekly.with_columns(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
    )
    games_expr = pl.col("week").n_unique() if "week" in wk.columns else pl.len()
    agg = wk.group_by("player_id", "season").agg(
        pl.col("position_group").drop_nulls().first().alias("position_group"),
        games_expr.cast(pl.Int64).alias("games"),
        *[pl.col(c).sum().alias(c) for c in stats],
        _col_or_zero("fantasy_points_ppr", wk.columns).sum().alias("_fp_ppr"),
    )
    cols = agg.columns
    volume = (
        pl.when(pl.col("position_group") == "QB")
        .then(_col_or_zero("attempts", cols))
        .when(pl.col("position_group") == "RB")
        .then(_col_or_zero("carries", cols) + _col_or_zero("targets", cols))
        .otherwise(_col_or_zero("targets", cols))
        .cast(pl.Float64)
        .alias("volume")
    )
    agg = agg.with_columns(
        volume,
        (pl.col("_fp_ppr") / pl.col("games")).cast(pl.Float64).alias("ppg"),
        *[(pl.col(c) / pl.col("games")).cast(pl.Float64).alias(f"{c}_rate") for c in stats],
    ).drop("_fp_ppr", *stats)

    if {"player_id", "season", "age"}.issubset(rosters.columns) and rosters.height > 0:
        ros = rosters.select(
            pl.col("player_id").cast(pl.Utf8),
            pl.col("season").cast(pl.Int64),
            pl.col("age").cast(pl.Float64),
        ).unique(subset=["player_id", "season"], keep="first")
        assert agg.schema["player_id"] == ros.schema["player_id"]
        assert agg.schema["season"] == ros.schema["season"]
        agg = agg.join(ros, on=["player_id", "season"], how="left")
    else:
        agg = agg.with_columns(pl.lit(None, dtype=pl.Float64).alias("age"))

    ordered = ["player_id", "season", "position_group", "age", "games", "volume", "ppg"] + [f"{c}_rate" for c in stats]
    return agg.select(ordered)
