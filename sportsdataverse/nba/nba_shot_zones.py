"""Shot-zone classification for the NBA v3 engine (pbpstats-core aligned)."""

from __future__ import annotations

import polars as pl

SHOT_ZONES: list[str] = [
    "restricted_area",
    "in_the_paint_non_ra",
    "mid_range",
    "corner_3",
    "above_the_break_3",
]


def add_shot_zones(enhanced_pbp: pl.DataFrame) -> pl.DataFrame:
    """Add shot-zone classification to enhanced PBP.

    Classifies field-goal attempts into NBA zones per pbpstats-core rules:
    - 3PT shots: ``corner_3`` if ``|x_legacy| >= 220`` **and**
      ``y_legacy <= 87.5``; else ``above_the_break_3``.
    - 2PT shots: ``restricted_area`` if ``shot_distance < 4``;
      ``in_the_paint_non_ra`` if ``shot_distance < 8``
      **and** ``|x_legacy| <= 80``; else ``mid_range``.
    - All other event types: ``null``.

    Args:
        enhanced_pbp: Enhanced PBP frame from :func:`enhanced_pbp_from_payload`,
            with columns ``is_field_goal``, ``shot_value``, ``shot_distance``,
            ``x_legacy``, ``y_legacy``.

    Returns:
        Frame with appended ``shot_zone`` column (Utf8, null on non-FG events).
        Null-safe: empty input returns zero-row frame with the column schema.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_shot_zones import add_shot_zones
            pbp = enhanced_pbp_from_payload(payload_dict)
            pbp_zones = add_shot_zones(pbp)
            print(pbp_zones[["shot_distance", "shot_zone"]].head())

        See Also:
            * `enhanced_pbp_from_payload` -- the Phase-1 engine
                that produces the input frame.
    """
    if enhanced_pbp.is_empty():
        return enhanced_pbp.with_columns(pl.lit(None, dtype=pl.Utf8).alias("shot_zone"))

    x = pl.col("x_legacy").abs()
    dist = pl.col("shot_distance")
    is_fg = pl.col("is_field_goal") == 1
    is_three = pl.col("shot_value") == 3

    zone = (
        pl.when(~is_fg)
        .then(None)
        .when(is_three & (x >= 220) & (pl.col("y_legacy") <= 87.5))
        .then(pl.lit("corner_3"))
        .when(is_three)
        .then(pl.lit("above_the_break_3"))
        .when(dist < 4)
        .then(pl.lit("restricted_area"))
        .when((dist < 8) & (x <= 80))
        .then(pl.lit("in_the_paint_non_ra"))
        .otherwise(pl.lit("mid_range"))
        .alias("shot_zone")
    )

    return enhanced_pbp.with_columns(zone)
