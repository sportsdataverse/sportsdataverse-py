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

from sportsdataverse.nfl.shield_pbp.live import is_final


def add_labels(df: pl.DataFrame, game: Dict[str, Any]) -> pl.DataFrame:
    """Add field_goal_result + final home_score/away_score + result.

    Args:
        df: Play frame (post parse). Must carry ``field_goal_made`` /
            ``field_goal_missed`` / ``field_goal_blocked`` / ``field_goal_attempt``.
        game: Raw Shield payload (for final scores from ``summary``).

    Returns:
        The frame with ``field_goal_result`` (made/missed/blocked/None),
        ``home_score``, ``away_score`` (final, broadcast to every row), and
        ``result`` (home_score - away_score). The three game-outcome columns are
        null unless the payload's ``summary.phase`` is FINAL / FINAL_OVERTIME.
    """
    if df.height == 0:
        return df

    summary = game.get("summary") or {}
    # ``summary.*.score.total`` is the score AS OF the payload, so on an
    # in-progress game it is the CURRENT score, not the final one. These three
    # columns are nflverse *game-outcome* columns (``result`` feeds the
    # margin-based labels and every "did the home team win" consumer), so
    # stamping a mid-game score into them silently mislabels the game. They stay
    # null until the feed says the game is over (``phase`` FINAL / FINAL_OVERTIME
    # — the only status signal Shield gives; top-level ``status`` is always
    # SCHEDULED). Archived finals are unaffected: every nfl/raw payload
    # 1999-2026 carries phase FINAL or FINAL_OVERTIME.
    if not is_final(game):
        home_total = away_total = result = None
    else:
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
