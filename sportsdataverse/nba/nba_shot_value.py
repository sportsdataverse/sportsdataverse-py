"""Shot-value spine: per-shot expected points (qSP) + downstream models.

Five league-agnostic models switched by ``league_id`` (``"00"`` NBA, ``"10"``
WNBA, ``"20"`` G-League):

1. ``score_shot_xpoints`` — per-shot expected points from the ``LeagueAverages``
   FG%-by-zone table that ``nba_stats_shotchartdetail`` returns for free.
2. ``make_prob_by_context`` / ``make_prob_joint`` — FG% by defender distance and
   shot clock (aggregate buckets, the only form the public API exposes).
3. ``shooter_talent`` — regressed make%-above-expected per shooter.
4. ``shot_selection_quality`` — player expected value per shot vs the league.
5. ``zone_value_map`` — per-player per-zone points / expected-points maps.

Everything downstream consumes the single scored frame from
``score_shot_xpoints`` — no divergent recomputation.
"""

from __future__ import annotations

from typing import Union

import pandas as pd
import polars as pl

_ZONE_KEYS = ["shot_zone_basic", "shot_zone_area", "shot_zone_range"]
_BASELINE_SCHEMA = {
    "shot_zone_basic": pl.Utf8,
    "shot_zone_area": pl.Utf8,
    "shot_zone_range": pl.Utf8,
    "base_fg_pct": pl.Float64,
    "is_three": pl.Boolean,
}
_SCORED_EXTRA = {
    "shot_value": pl.Int64,
    "base_fg_pct": pl.Float64,
    "xpoints": pl.Float64,
    "actual_points": pl.Float64,
}


def xpoints_baseline(league_avgs: pl.DataFrame) -> pl.DataFrame:
    """League-average FG% baseline table keyed by the three shot-zone columns.

    Args:
        league_avgs: The ``LeagueAverages`` result set from
            ``nba_stats_shotchartdetail`` (``shot_zone_basic`` /
            ``shot_zone_area`` / ``shot_zone_range`` / ``fga`` / ``fgm`` /
            ``fg_pct``).

    Returns:
        One row per ``(shot_zone_basic, shot_zone_area, shot_zone_range)``:
        ``... base_fg_pct:Float64, is_three:Boolean`` (``is_three`` = the
        basic zone names a three). Empty input returns the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_stats
            from sportsdataverse.nba.nba_shot_value import xpoints_baseline
            base = xpoints_baseline(league_avgs)
    """
    if league_avgs.is_empty():
        return pl.DataFrame(schema=_BASELINE_SCHEMA)
    return league_avgs.select(
        *_ZONE_KEYS,
        pl.col("fg_pct").cast(pl.Float64).alias("base_fg_pct"),
        pl.col("shot_zone_basic").str.contains("3").alias("is_three"),
    ).unique(subset=_ZONE_KEYS, keep="first")


def score_shot_xpoints(
    shots: pl.DataFrame,
    league_avgs: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Score each shot with expected points from the league-average baseline.

    Joins the per-shot frame to the zone baseline (falling back to the
    within-``shot_zone_range`` mean when a zone triple is unmatched) and adds
    ``shot_value`` (3 for a ``3PT`` shot else 2), ``xpoints = base_fg_pct *
    shot_value``, and ``actual_points = shot_made_flag * shot_value``.

    Args:
        shots: Per-shot ``Shot_Chart_Detail`` frame (needs ``shot_type`` +
            the three zone keys + ``shot_made_flag``).
        league_avgs: The ``LeagueAverages`` frame (see :func:`xpoints_baseline`).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The input ``shots`` plus ``shot_value:Int64, base_fg_pct:Float64,
        xpoints:Float64, actual_points:Float64``. Empty input returns the
        augmented schema with zero rows.

    Raises:
        ValueError: A zone join key has a different dtype on the two frames.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value import score_shot_xpoints
            scored = score_shot_xpoints(shots, league_avgs)

        Pipeline next step (one line)::

            scored.group_by("player_id").agg(pl.col("xpoints").sum())

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
    """
    if shots.is_empty():
        out = shots.with_columns([pl.lit(None, dtype=t).alias(c) for c, t in _SCORED_EXTRA.items()])
        return out.to_pandas() if return_as_pandas else out

    base = xpoints_baseline(league_avgs)
    for k in _ZONE_KEYS:
        if shots.schema[k] != base.schema[k]:
            raise ValueError(f"join-key dtype mismatch on {k}: {shots.schema[k]} vs {base.schema[k]}")

    scored = shots.with_columns(
        pl.when(pl.col("shot_type").str.starts_with("3")).then(3).otherwise(2).cast(pl.Int64).alias("shot_value")
    ).join(base.select(*_ZONE_KEYS, "base_fg_pct"), on=_ZONE_KEYS, how="left")
    scored = scored.with_columns(
        pl.col("base_fg_pct").fill_null(pl.col("base_fg_pct").mean().over("shot_zone_range")).alias("base_fg_pct")
    ).with_columns(
        (pl.col("base_fg_pct") * pl.col("shot_value")).alias("xpoints"),
        (pl.col("shot_made_flag").cast(pl.Float64) * pl.col("shot_value")).alias("actual_points"),
    )
    return scored.to_pandas() if return_as_pandas else scored
