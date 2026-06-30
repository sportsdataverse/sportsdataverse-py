"""Constants + schemas for the stats.nba.com v3 play-by-play engine."""

from __future__ import annotations

import polars as pl

ACTION_TYPE_EVENT: dict[str, str] = {
    "Made Shot": "made_shot",
    "Missed Shot": "missed_shot",
    "Free Throw": "free_throw",
    "Rebound": "rebound",
    "Turnover": "turnover",
    "Foul": "foul",
    "Substitution": "substitution",
    "Timeout": "timeout",
    "Jump Ball": "jump_ball",
    "period": "period",
    "Instant Replay": "replay",
    "": "other",
}

EVENT_FLAG_COLUMNS: list[str] = [
    "is_made_shot",
    "is_missed_shot",
    "is_free_throw",
    "is_rebound",
    "is_turnover",
    "is_foul",
    "is_substitution",
    "is_jump_ball",
    "is_timeout",
    "is_period",
]


def iso_clock_to_seconds(expr: pl.Expr) -> pl.Expr:
    """'PTmmMss.ssS' -> seconds remaining (Float64)."""
    mins = expr.str.extract(r"PT(\d+)M", 1).cast(pl.Float64)
    secs = expr.str.extract(r"M([\d.]+)S", 1).cast(pl.Float64)
    return mins * 60 + secs


ENHANCED_PBP_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "action_number": pl.Int64,
    "period": pl.Int64,
    "action_type": pl.Utf8,
    "sub_type": pl.Utf8,
    "event_type": pl.Utf8,  # string slug (e.g. "made_shot", "substitution")
    "clock": pl.Utf8,
    "seconds_remaining": pl.Float64,
    "person_id": pl.Int64,
    "team_id": pl.Int64,
    "location": pl.Utf8,
    "description": pl.Utf8,
    "is_field_goal": pl.Int64,
    "shot_result": pl.Utf8,
    "shot_value": pl.Int64,
    "shot_distance": pl.Float64,
    "x_legacy": pl.Float64,
    "y_legacy": pl.Float64,
    "score_home": pl.Utf8,
    "score_away": pl.Utf8,
    "order_index": pl.Int64,
    **{c: pl.Boolean for c in EVENT_FLAG_COLUMNS},
}

LINEUPS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "action_number": pl.Int64,
    "period": pl.Int64,
    **{f"home_player_{i}": pl.Int64 for i in range(1, 6)},
    **{f"away_player_{i}": pl.Int64 for i in range(1, 6)},
}
