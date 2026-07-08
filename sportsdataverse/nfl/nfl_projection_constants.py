"""Projection-spine shared constants, scoring formats, validation metrics, and the
as-of-date leakage split (position-agnostic).

Methodology references (no code copied): Tom Tango's "Marcel the Monkey" forecaster;
Pro-Football-Reference Approximate Value; the WOPR formula (``1.5*target_share +
0.7*air_yards_share``). Cited here per the project's methodology-attribution
convention.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.stats import rankdata


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two 1-D arrays.

    Args:
        a (np.ndarray): First array.
        b (np.ndarray): Second array, same length as ``a``.

    Returns:
        float: Spearman correlation in ``[-1, 1]``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_projection_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([2.0, 4.0, 9.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two arrays.

    Args:
        a (np.ndarray): Predictions.
        b (np.ndarray): Targets, same length as ``a``.

    Returns:
        float: Mean of ``|a - b|``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_projection_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Brier score (mean squared error of probabilities).

    Args:
        y_true (np.ndarray): Binary outcomes in ``{0, 1}``.
        p_pred (np.ndarray): Predicted probabilities in ``[0, 1]``.

    Returns:
        float: Mean of ``(p_pred - y_true) ** 2`` (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_projection_constants import brier_score
            brier_score(np.array([1, 0]), np.array([0.75, 0.25]))
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary log loss (cross-entropy) with probability clipping.

    Args:
        y_true (np.ndarray): Binary outcomes in ``{0, 1}``.
        p_pred (np.ndarray): Predicted probabilities in ``[0, 1]``.
        eps (float): Clip bound keeping probabilities in ``[eps, 1 - eps]``.

    Returns:
        float: Mean negative log-likelihood (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_projection_constants import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    p = np.clip(np.asarray(p_pred, dtype=float), eps, 1 - eps)
    y = np.asarray(y_true, dtype=float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Decile (or ``n_bins``) calibration table for probability predictions.

    Args:
        y_true (np.ndarray): Binary (or rate-valued) outcomes.
        p_pred (np.ndarray): Predicted probabilities in ``[0, 1]``.
        n_bins (int): Number of equal-width probability bins.

    Returns:
        pl.DataFrame: Columns ``bin_mid:Float64, mean_pred:Float64,
        mean_actual:Float64, n:Int64``, one row per non-empty bin.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_projection_constants import calibration_table
            tbl = calibration_table(np.array([1, 0, 1]), np.array([0.8, 0.2, 0.7]))
    """
    df = pl.DataFrame({"y": np.asarray(y_true, dtype=float), "p": np.asarray(p_pred, dtype=float)})
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    return (
        df.group_by("bin")
        .agg(
            pl.col("p").mean().alias("mean_pred"),
            pl.col("y").mean().alias("mean_actual"),
            pl.len().alias("n"),
        )
        .sort("bin")
        .with_columns(((pl.col("bin") + 0.5) / n_bins).alias("bin_mid"))
        .select("bin_mid", "mean_pred", "mean_actual", pl.col("n").cast(pl.Int64))
    )


def as_of_season_split(df: pl.DataFrame, target_season: int, *, season_col: str = "season") -> pl.DataFrame:
    """Leakage boundary: only seasons strictly before ``target_season`` are visible.

    Args:
        df (pl.DataFrame): Frame carrying a season column.
        target_season (int): The season being projected; excluded along with
            everything after it.
        season_col (str): Name of the season column.

    Returns:
        pl.DataFrame: Rows with ``season < target_season``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl.nfl_projection_constants import as_of_season_split
            hist = as_of_season_split(pl.DataFrame({"season": [2022, 2023, 2024]}), 2024)
    """
    return df.filter(pl.col(season_col) < target_season)
