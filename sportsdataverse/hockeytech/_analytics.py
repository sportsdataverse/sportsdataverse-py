"""Pure HockeyTech analytics: frame(s) -> frame. No network.

Corsi/Fenwick caveat: the HockeyTech feed has no missed-shot event, so shot
attempts = shot + blocked_shot + goal. Both metrics are proxies; outputs carry
``corsi_includes_missed = False`` (added in a later task).
"""

from __future__ import annotations

import math

import polars as pl

_SCORING_CHANCE_FT = 25.0  # distance threshold from net (feet)
_SHOT_EVENTS = ["shot", "blocked_shot", "goal"]


def add_shot_distance_angle(pbp: pl.DataFrame, goal_x: float = 89.0) -> pl.DataFrame:
    """Add ``shot_distance``/``shot_angle`` (feet/degrees) for shot-type events.

    Assumes coordinates are already in a standard rink frame (offensive net at
    +goal_x, y=0). Non-shot rows receive null values for both columns.

    Parameters
    ----------
    pbp:
        Play-by-play frame with at least ``event``, ``x_coord``, ``y_coord``
        columns. Coordinates must already be in feet in the standard rink frame.
    goal_x:
        X-coordinate of the offensive net in feet. Default is 89.0 (NHL).

    Returns
    -------
    pl.DataFrame
        Input frame with ``shot_distance`` (Float64) and ``shot_angle``
        (Float64, degrees) columns appended.
    """
    if pbp.height == 0:
        return pbp.with_columns(
            shot_distance=pl.lit(None, dtype=pl.Float64),
            shot_angle=pl.lit(None, dtype=pl.Float64),
        )

    dx = pl.lit(goal_x) - pl.col("x_coord").abs()
    dy = pl.col("y_coord")
    dist = (dx**2 + dy**2).sqrt()
    # pl.arctan2(y, x) -> radians; convert to degrees and take absolute value
    angle = pl.arctan2(dy.abs(), dx).abs() * (180.0 / math.pi)

    is_shot = pl.col("event").is_in(_SHOT_EVENTS)
    return pbp.with_columns(
        shot_distance=pl.when(is_shot).then(dist).otherwise(None),
        shot_angle=pl.when(is_shot).then(angle).otherwise(None),
    )


def scoring_chances(pbp: pl.DataFrame, threshold_ft: float = _SCORING_CHANCE_FT) -> pl.DataFrame:
    """Flag ``scoring_chance`` for shot-type events within ``threshold_ft`` of net.

    Parameters
    ----------
    pbp:
        Play-by-play frame. If ``shot_distance`` is not already present,
        :func:`add_shot_distance_angle` is called automatically.
    threshold_ft:
        Distance threshold in feet. Shots at or inside this distance from the
        net are flagged as scoring chances. Default is 25.0 ft.

    Returns
    -------
    pl.DataFrame
        Input frame with a ``scoring_chance`` (Boolean) column appended.
    """
    if "shot_distance" not in pbp.columns:
        pbp = add_shot_distance_angle(pbp)
    return pbp.with_columns(
        scoring_chance=(pl.col("shot_distance").is_not_null() & (pl.col("shot_distance") <= threshold_ft))
    )


def player_toi(shifts: pl.DataFrame) -> pl.DataFrame:
    """Compute per-player TOI from a parsed shifts frame.

    The HockeyTech shift feed uses a countdown clock where ``start_s`` is the
    clock value at the start of the shift and ``end_s`` is the clock value at
    the end (``start_s >= end_s``); shift length is therefore
    ``start_s - end_s``.

    Parameters
    ----------
    shifts:
        Shifts frame with at least ``player_id``, ``first_name``,
        ``last_name``, ``start_s``, ``end_s`` columns.

    Returns
    -------
    pl.DataFrame
        One row per player with ``player_id``, ``first_name``, ``last_name``,
        ``toi_seconds`` (Int64), ``num_shifts`` (UInt32), and
        ``avg_shift_s`` (Float64), sorted by ``toi_seconds`` descending.
    """
    if shifts.height == 0:
        return pl.DataFrame(
            schema={
                "player_id": pl.Int64,
                "first_name": pl.Utf8,
                "last_name": pl.Utf8,
                "toi_seconds": pl.Int64,
                "num_shifts": pl.UInt32,
                "avg_shift_s": pl.Float64,
            }
        )
    per_shift = shifts.with_columns(shift_s=(pl.col("start_s") - pl.col("end_s")))
    return (
        per_shift.group_by("player_id", "first_name", "last_name")
        .agg(
            toi_seconds=pl.col("shift_s").sum(),
            num_shifts=pl.len(),
            avg_shift_s=pl.col("shift_s").mean(),
        )
        .sort("toi_seconds", descending=True)
    )
