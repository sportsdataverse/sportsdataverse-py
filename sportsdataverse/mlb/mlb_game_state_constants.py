"""Shared substrate for the MLB game-state model spine (T6.4).

Owns the constants, generic metric helpers, the statsapi season
play-by-play collector, and the as-of-date leakage-boundary split that
every downstream game-state model (RE24, win-expectancy, umpire zone,
team projection, prop projection) is built on.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Tango, Lichtman & Dolphin, *The Book: Playing the Percentages in
      Baseball* (2007) -- source of the published RE24 reference table
      and the empirical leverage-index definition used elsewhere in
      this spine.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

import time
from typing import Any, List

import numpy as np
import polars as pl
from scipy.stats import rankdata

#: Smyth-Patriot "pythagenpat" run-environment-adaptive exponent (Task 4.1).
PYTHAGENPAT_EXPONENT: float = 0.287
#: Elo seeds (538 MLB-Elo methodology); refit in Task 4.2 -- see dev/mlb_game_state/fit_elo.py.
ELO_INIT: float = 1500.0
ELO_K: float = 4.0
ELO_HFA: float = 24.0
#: The 8 base-occupancy codes, "F"irst/"S"econd/"T"hird, "_" = empty.
BASE_STATES: List[str] = ["___", "1__", "_2_", "__3", "12_", "1_3", "_23", "123"]

_PBP_SCHEMA = {
    "game_id": pl.Utf8,
    "about_inning": pl.Int64,
    "about_half_inning": pl.Utf8,
    "about_at_bat_index": pl.Int64,
    "count_outs": pl.Int64,
    "result_home_score": pl.Int64,
    "result_away_score": pl.Int64,
    "matchup_post_on_first_id": pl.Utf8,
    "matchup_post_on_second_id": pl.Utf8,
    "matchup_post_on_third_id": pl.Utf8,
}


def mae(a: "np.ndarray", b: "np.ndarray") -> float:
    """Mean absolute error between two same-length arrays.

    Args:
        a: First array (e.g. predictions).
        b: Second array (e.g. observed values).

    Returns:
        The mean of ``|a - b|`` as a plain Python ``float``.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_game_state_constants import mae
            mae([1.0, 2.0], [1.5, 2.5])
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def spearman_corr(a: "np.ndarray", b: "np.ndarray") -> float:
    """Spearman rank correlation between two same-length arrays.

    Args:
        a: First array.
        b: Second array.

    Returns:
        The Pearson correlation of the rank-transformed arrays, as a
        plain Python ``float``.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_game_state_constants import spearman_corr
            spearman_corr([1, 2, 3], [9, 8, 10])
    """
    return float(np.corrcoef(rankdata(a), rankdata(b))[0, 1])


def brier_score(y_true: "np.ndarray", p_pred: "np.ndarray") -> float:
    """Brier score (mean squared error of a probability forecast).

    Args:
        y_true: Binary outcomes (0/1).
        p_pred: Predicted probabilities in ``[0, 1]``.

    Returns:
        The mean of ``(p_pred - y_true) ** 2`` as a plain Python ``float``.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_game_state_constants import brier_score
            brier_score([1, 0], [0.75, 0.25])
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def calibration_table(y_true: "np.ndarray", p_pred: "np.ndarray", n_bins: int = 10) -> pl.DataFrame:
    """Bucket predicted probabilities into deciles and compare to realized rate.

    Args:
        y_true: Binary outcomes (0/1).
        p_pred: Predicted probabilities in ``[0, 1]``.
        n_bins: Number of equal-width probability buckets (default 10).

    Returns:
        pl.DataFrame: one row per non-empty bucket.

        | Column | Type | Description |
        |---|---|---|
        | bin_mid | Float64 | Bucket midpoint probability |
        | mean_pred | Float64 | Mean predicted probability in the bucket |
        | mean_actual | Float64 | Realized outcome rate in the bucket |
        | n | UInt32 | Row count in the bucket |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_game_state_constants import calibration_table
            calibration_table([1, 0, 1, 1], [0.8, 0.2, 0.6, 0.9])
    """
    df = pl.DataFrame({"y": np.asarray(y_true, dtype=float), "p": np.asarray(p_pred, dtype=float)})
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    return (
        df.group_by("bin")
        .agg(pl.col("p").mean().alias("mean_pred"), pl.col("y").mean().alias("mean_actual"), pl.len().alias("n"))
        .sort("bin")
        .with_columns(((pl.col("bin") + 0.5) / n_bins).alias("bin_mid"))
        .select("bin_mid", "mean_pred", "mean_actual", "n")
    )


def collect_statsapi_pbp(game_pks: List[int], *, sleep: float = 0.0) -> pl.DataFrame:
    """Fetch + parse MLB Stats API play-by-play for a list of games.

    Spine-local season collector (no ``load_mlb_pbp`` exists yet -- every
    ``load_mlb_*`` loader is a stub). Promotable to a shared loader once a
    ``-data`` release pipeline exists for MLB.

    Args:
        game_pks: List of statsapi ``gamePk`` integers.
        sleep: Seconds to sleep between requests (politeness throttle).

    Returns:
        pl.DataFrame: one row per plate appearance, concatenated across
        games via ``diagonal_relaxed``, with a ``game_id:Utf8`` column
        appended (the raw integer ``game_pk``, stringified). Empty input
        or all-empty responses return a zero-row frame with the
        documented pbp schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_game_state_constants import collect_statsapi_pbp
            pbp = collect_statsapi_pbp([716390])
    """
    from sportsdataverse.mlb.mlb_api import mlb_play_by_play

    frames: List[pl.DataFrame] = []
    for pk in game_pks:
        df = mlb_play_by_play(int(pk), return_parsed=True)
        if isinstance(df, pl.DataFrame) and df.height:
            frames.append(
                df.with_columns(pl.col("game_id") if "game_id" in df.columns else pl.lit(str(int(pk))).alias("game_id"))
            )
        if sleep:
            time.sleep(sleep)
    if not frames:
        return pl.DataFrame(schema=_PBP_SCHEMA)
    return pl.concat(frames, how="diagonal_relaxed")


def as_of_split(results: pl.DataFrame, cutoff_date: Any, *, date_col: str = "date") -> pl.DataFrame:
    """Filter a results frame to rows strictly before a cutoff date.

    The as-of-date leakage boundary every predictive model (pythagenpat,
    Elo, prop projections) must apply: features for game G use only
    games dated strictly before G.

    Args:
        results: Frame with a date column.
        cutoff_date: The cutoff (exclusive) -- typically the projected game's date.
        date_col: Name of the date column (default ``"date"``).

    Returns:
        pl.DataFrame: rows with ``date_col < cutoff_date`` only.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.mlb.mlb_game_state_constants import as_of_split
            as_of_split(results, dt.date(2024, 6, 1))
    """
    return results.filter(pl.col(date_col) < cutoff_date)
