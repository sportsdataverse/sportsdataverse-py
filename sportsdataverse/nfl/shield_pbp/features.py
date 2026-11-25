"""Game-state derivations: running score_differential + timeouts remaining.

Produces the base nflverse columns Track 6's labelers consume. The downstream
model-feature engineering (era / down one-hots / elapsed_share / spread_time /
Diff_Time_Ratio / receive_2h_ko) lives in Track 6's own ``features.py`` and is
derived from these base columns — so it is intentionally NOT done here.

``roof``, ``spread_line``, and ``total_line`` are game-level and come from a
schedules join (nflverse schedules / Lee Sharpe games, the same source nflfastR
uses); they are passed in as scalars rather than parsed from the Shield feed,
which omits them.
"""

from __future__ import annotations

from typing import Optional

import polars as pl


def add_game_state(
    df: pl.DataFrame,
    *,
    roof: Optional[str] = None,
    spread_line: Optional[float] = None,
    total_line: Optional[float] = None,
) -> pl.DataFrame:
    """Add score_differential, posteam/defteam timeouts, roof, spread_line, total_line.

    Args:
        df: Base play frame (post parse, ideally post description) — must carry
            ``play_seq``, ``game_id``, ``game_half``, ``posteam``, ``home_team``,
            ``away_team``, ``timeout``, ``timeout_team``, and the per-play
            ``_points_home`` / ``_points_away`` produced by the parser.
        roof: Game roof (``outdoors``/``dome``/``closed``/``open``/``retractable``)
            from a schedules join; ``None`` left as-is for downstream model_roof
            handling.
        spread_line: Closing spread (home-relative) from a schedules join. Feeds
            the spread-aware win-probability model (``vegas_wp``); ``None`` leaves
            the column null and ``vegas_wp`` falls back to a default spread.
        total_line: Game over/under from a schedules join. Required by the
            fourth-down decision step; ``None`` leaves the column null (the
            decision step is then skipped downstream).

    Returns:
        The frame with ``score_differential``, ``posteam_type``, the ``home`` / ``away`` / ``posteam`` /
        ``defteam`` ``_timeouts_remaining``, the post-play ``total_home_score`` / ``total_away_score`` /
        ``posteam_score_post`` / ``defteam_score_post`` / ``score_differential_post``, ``roof``,
        ``spread_line`` and ``total_line`` added.
    """
    if df.height == 0:
        return df

    df = df.sort("play_seq")

    # --- running score (PRE-snap) from per-play points ---
    # Cumulative points include the current play; subtracting the current play's
    # points yields the score the play STARTED with (what nflverse reports), so a
    # PAT shows post-TD (not post-PAT) and a FG shows pre-FG.
    df = df.with_columns(
        _home_pre=(pl.col("_points_home").cum_sum().over("game_id") - pl.col("_points_home")),
        _away_pre=(pl.col("_points_away").cum_sum().over("game_id") - pl.col("_points_away")),
    )
    df = (
        df.with_columns(
            posteam_score=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("_home_pre"))
            .otherwise(pl.col("_away_pre")),
            defteam_score=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("_away_pre"))
            .otherwise(pl.col("_home_pre")),
        )
        .with_columns(
            score_differential=(pl.col("posteam_score") - pl.col("defteam_score")),
        )
        .drop("_home_pre", "_away_pre")
    )

    # --- timeouts remaining (cumsum per half, capped at 3) ---
    df = df.with_columns(
        _home_to=((pl.col("timeout") == 1) & (pl.col("timeout_team") == pl.col("home_team"))).cast(pl.Int64),
        _away_to=((pl.col("timeout") == 1) & (pl.col("timeout_team") == pl.col("away_team"))).cast(pl.Int64),
    )
    df = df.with_columns(
        _home_used=pl.col("_home_to").cum_sum().over("game_half").clip(upper_bound=3),
        _away_used=pl.col("_away_to").cum_sum().over("game_half").clip(upper_bound=3),
    )
    # nflfastR: three timeouts a half, two in overtime
    per_half = pl.when(pl.col("qtr") <= 4).then(3).otherwise(2)
    df = (
        df.with_columns(
            home_timeouts_remaining=(per_half - pl.col("_home_used")),
            away_timeouts_remaining=(per_half - pl.col("_away_used")),
        )
        .with_columns(
            posteam_timeouts_remaining=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("home_timeouts_remaining"))
            .otherwise(pl.col("away_timeouts_remaining")),
            defteam_timeouts_remaining=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("away_timeouts_remaining"))
            .otherwise(pl.col("home_timeouts_remaining")),
        )
        .drop("_home_to", "_away_to", "_home_used", "_away_used")
    )
    df = _add_post_play_score(df)

    # --- game-level joins (scalars) ---
    df = df.with_columns(
        roof=pl.lit(roof),
        spread_line=pl.lit(spread_line, dtype=pl.Float64),
        total_line=pl.lit(total_line, dtype=pl.Float64),
    )
    return df


def _add_post_play_score(df: pl.DataFrame) -> pl.DataFrame:
    """nflfastR's post-play score columns and ``posteam_type`` (``add_nflscrapr_mutations``).

    ``total_home_score`` / ``total_away_score`` are the running score after the play;
    ``posteam_score_post`` / ``defteam_score_post`` / ``score_differential_post`` read it from each side, and
    are null with ``posteam_type`` where the play has no possession team (R's ``fifelse`` on an NA test).
    """
    home_has_ball = pl.col("posteam") == pl.col("home_team")
    no_ball = pl.col("posteam").is_null()
    df = df.with_columns(
        total_home_score=pl.col("_points_home").cum_sum().over("game_id"),
        total_away_score=pl.col("_points_away").cum_sum().over("game_id"),
        posteam_type=pl.when(no_ball).then(None).when(home_has_ball).then(pl.lit("home")).otherwise(pl.lit("away")),
    )
    df = df.with_columns(
        posteam_score_post=pl.when(no_ball)
        .then(None)
        .when(home_has_ball)
        .then(pl.col("total_home_score"))
        .otherwise(pl.col("total_away_score")),
        defteam_score_post=pl.when(no_ball)
        .then(None)
        .when(home_has_ball)
        .then(pl.col("total_away_score"))
        .otherwise(pl.col("total_home_score")),
    )
    return df.with_columns(score_differential_post=pl.col("posteam_score_post") - pl.col("defteam_score_post"))
