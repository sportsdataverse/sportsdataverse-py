"""EP/WP/CP label sources: field_goal_result + final scores + result.

``sp`` (scoring-play indicator) and ``touchdown`` / ``td_team`` / ``safety``
are already produced upstream (parse + stat_ids). This module adds the
remaining nflverse label columns the Track 6 EP/WP labelers consume:
``field_goal_result``, game-level ``home_score`` / ``away_score``, and
``result`` (home final - away final).
"""

from __future__ import annotations

from typing import Any, Dict

import polars as pl


def add_labels(df: pl.DataFrame, game: Dict[str, Any]) -> pl.DataFrame:
    """Add field_goal_result + final home_score/away_score + result.

    Args:
        df: Play frame (post parse). Must carry ``field_goal_made`` /
            ``field_goal_missed`` / ``field_goal_blocked`` / ``field_goal_attempt``.
        game: Raw Shield payload (for final scores from ``summary``).

    Returns:
        The frame with ``field_goal_result`` (made/missed/blocked/None),
        ``home_score``, ``away_score`` (final, broadcast to every row), and
        ``result`` (home_score - away_score).
    """
    if df.height == 0:
        return df

    summary = game.get("summary") or {}
    home_total = ((summary.get("homeTeam") or {}).get("score") or {}).get("total")
    away_total = ((summary.get("awayTeam") or {}).get("score") or {}).get("total")
    home_total = int(home_total) if home_total is not None else None
    away_total = int(away_total) if away_total is not None else None
    result = (home_total - away_total) if (home_total is not None and away_total is not None) else None

    df = df.with_columns(
        field_goal_result=pl.when(pl.col("field_goal_made") == 1)
        .then(pl.lit("made"))
        .when(pl.col("field_goal_missed") == 1)
        .then(pl.lit("missed"))
        .when(pl.col("field_goal_blocked") == 1)
        .then(pl.lit("blocked"))
        .otherwise(None),
        home_score=pl.lit(home_total, dtype=pl.Int64),
        away_score=pl.lit(away_total, dtype=pl.Int64),
        result=pl.lit(result, dtype=pl.Int64),
    )
    return df
