"""Union the per-season schedules into a master + a coverage index.

The master computes NO flags of its own: every ``has_*`` column is inherited
from the season files by union. Ragged inputs are reconciled with a diagonal
concat so a column present in one season is null-filled in the others -- which
is what fixes the historical drift where the master carried ``venue_capacity``
and the yearly files did not.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.scrape.espn.ids import with_int64_ids

ID_COLUMNS = ("game_id", "home_id", "away_id", "venue_id")


def build_master(season_frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Concatenate season schedules into one frame with a pinned column order.

    Args:
        season_frames: One frame per season, as written by step 01.

    Returns:
        The union, ids canonicalized to Int64, columns in a stable order and
        rows sorted by ``(season, game_id)``.

    Raises:
        ValueError: If no frames are given.
    """
    if not season_frames:
        raise ValueError("build_master() requires at least one season frame")
    frames = [with_int64_ids(df, *ID_COLUMNS) for df in season_frames]
    master = pl.concat(frames, how="diagonal_relaxed")
    ordered = sorted(master.columns)
    sort_keys = [k for k in ("season", "game_id") if k in master.columns]
    master = master.select(ordered)
    return master.sort(sort_keys) if sort_keys else master


def build_coverage(master: pl.DataFrame) -> pl.DataFrame:
    """One row per ``(season, season_type)`` with capture percentages.

    Args:
        master: The frame from :func:`build_master`.

    Returns:
        ``season``, ``season_type``, ``n_games``, ``first_date``, ``last_date``,
        a ``pct_<flag>`` per ``has_*`` column, and ``pct_json_captured`` as a
        convenience alias for ``pct_has_game_json``.

    Raises:
        ValueError: If the master carries neither ``season`` nor ``season_type``.
    """
    flags = [c for c in master.columns if c.startswith("has_")]
    group_keys = [k for k in ("season", "season_type") if k in master.columns]
    if not group_keys:
        raise ValueError("master frame has neither season nor season_type")

    aggs: list[pl.Expr] = [pl.len().alias("n_games")]
    if "date" in master.columns:
        aggs += [
            pl.col("date").min().alias("first_date"),
            pl.col("date").max().alias("last_date"),
        ]
    aggs += [pl.col(flag).mean().alias(f"pct_{flag}") for flag in flags]

    # maintain_order keeps the group order deterministic; the sort below then
    # pins it regardless of polars' internal grouping strategy.
    coverage = master.group_by(group_keys, maintain_order=True).agg(aggs)
    if "pct_has_game_json" in coverage.columns:
        coverage = coverage.with_columns(pl.col("pct_has_game_json").alias("pct_json_captured"))
    return coverage.sort(group_keys)
