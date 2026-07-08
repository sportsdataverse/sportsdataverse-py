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

from sportsdataverse.nfl.nfl_projection_constants import get_position_constants

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


def aging_curve(season_rates: pl.DataFrame, *, position_group: str, rate_col: str = "ppg") -> pl.DataFrame:
    """Delta-method aging curve for a position group, peak-normalized to 1.0.

    Pairs each player's season with the same player's next season, computes the
    playing-time-weighted mean transition multiplier ``rate_next / rate`` per
    starting age, chains the transitions into a level curve (``cum_prod``), and
    normalizes so the peak age has multiplier 1.0. Only paired consecutive
    seasons contribute (the standard survivorship mitigation).

    Args:
        season_rates (pl.DataFrame): Output of :func:`season_player_rates`
            (needs ``player_id, season, position_group, age, volume`` +
            ``rate_col``).
        position_group (str): Position group to fit (``"QB"``/``"RB"``/...).
        rate_col (str): Rate column the curve is fit on.

    Returns:
        pl.DataFrame: ``age:Float64, aging_mult:Float64`` sorted by age. Empty
        input returns a zero-row frame with that schema.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_projection import aging_curve, season_player_rates
            curve = aging_curve(rates, position_group="RB")
    """
    consts = get_position_constants(position_group)
    df = season_rates.filter(
        (pl.col("position_group") == position_group) & (pl.col("volume") >= consts.min_volume)
    ).select("player_id", "season", "age", "volume", pl.col(rate_col).alias("rate"))
    # match season s (age a) to the same player's season s+1 (age a+1)
    nxt = df.select(
        "player_id",
        (pl.col("season") - 1).alias("season"),
        pl.col("rate").alias("rate_next"),
        pl.col("volume").alias("volume_next"),
    )
    paired = (
        df.join(nxt, on=["player_id", "season"], how="inner")
        .filter(pl.col("rate") > 0)
        .with_columns(
            (pl.col("rate_next") / pl.col("rate")).alias("delta"),
            pl.min_horizontal("volume", "volume_next").alias("w"),
        )
    )
    trans = (
        paired.group_by("age")
        .agg(((pl.col("delta") * pl.col("w")).sum() / pl.col("w").sum()).alias("mult_step"))
        .sort("age")
    )
    if trans.height == 0:
        return pl.DataFrame(schema={"age": pl.Float64, "aging_mult": pl.Float64})
    # chain transitions into a level curve anchored at the youngest age, then peak-normalize
    curve = trans.with_columns(pl.col("mult_step").cum_prod().alias("level"))
    curve = curve.with_columns((pl.col("age") + 1.0).alias("age")).select("age", "level")
    anchor = pl.DataFrame({"age": [float(trans["age"][0])], "level": [1.0]})
    curve = pl.concat([anchor.with_columns(pl.col("age").cast(pl.Float64)), curve], how="vertical").sort("age")
    peak = curve["level"].max()
    return curve.with_columns((pl.col("level") / peak).cast(pl.Float64).alias("aging_mult")).select(
        pl.col("age").cast(pl.Float64), "aging_mult"
    )
