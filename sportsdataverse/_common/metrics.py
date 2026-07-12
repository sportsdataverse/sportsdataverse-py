"""League-agnostic validation metrics + the as-of leakage split (T7.2).

These six helpers were duplicated verbatim across every per-sport
``*_prediction_constants`` / ``*_projection_constants`` module (Brier / log-loss /
Spearman / MAE / calibration table + the leakage-boundary date split). They are
extracted here so the identical implementation lives exactly once; the per-sport
modules re-export these names (redundant-alias re-export) and keep their own
league-specific constants.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package;
per-sport modules import from here.
"""

from __future__ import annotations

import datetime

import numpy as np
import polars as pl
from scipy.stats import rankdata


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Mean squared error between predicted probabilities and binary outcomes.

    Args:
        y_true: Array of binary outcomes (0/1).
        p_pred: Array of predicted probabilities in [0, 1].

    Returns:
        The Brier score (0.0 is a perfect forecast).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse._common.metrics import brier_score
            brier_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary cross-entropy loss between predicted probabilities and outcomes.

    Args:
        y_true: Array of binary outcomes (0/1).
        p_pred: Array of predicted probabilities in [0, 1].
        eps: Clipping bound to avoid ``log(0)``.

    Returns:
        The mean log loss.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse._common.metrics import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    p = np.clip(np.asarray(p_pred, dtype=float), eps, 1 - eps)
    y = np.asarray(y_true, dtype=float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two arrays.

    Args:
        a: First array of values.
        b: Second array of values (same length as ``a``).

    Returns:
        The Spearman rank correlation coefficient.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse._common.metrics import spearman_corr
            spearman_corr(np.array([1, 2, 3]), np.array([3, 1, 2]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two arrays.

    Args:
        a: First array of values.
        b: Second array of values (same length as ``a``).

    Returns:
        The mean absolute error.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse._common.metrics import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bucket predicted probabilities into bins and compare to actual outcome rates.

    Args:
        y_true: Array of binary outcomes (0/1).
        p_pred: Array of predicted probabilities in [0, 1].
        n_bins: Number of equal-width probability bins.

    Returns:
        A ``polars.DataFrame`` with columns ``bin_mid``, ``mean_pred``,
        ``mean_actual``, ``n`` (one row per non-empty bin).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse._common.metrics import calibration_table
            calibration_table(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
    """
    df = pl.DataFrame(
        {
            "y": np.asarray(y_true, dtype=float),
            "p": np.asarray(p_pred, dtype=float),
        }
    )
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
        .select("bin_mid", "mean_pred", "mean_actual", "n")
    )


def as_of_ratings_split(results: pl.DataFrame, cutoff_date: datetime.date) -> pl.DataFrame:
    """Filter a results frame to games strictly before a cutoff date (leakage boundary).

    Args:
        results: A ``polars.DataFrame`` with a ``date`` column.
        cutoff_date: Games on or after this date are excluded.

    Returns:
        A ``polars.DataFrame`` containing only rows with ``date < cutoff_date``.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse._common.metrics import as_of_ratings_split
            as_of_ratings_split(results, dt.date(2023, 9, 8))
    """
    return results.filter(pl.col("date") < cutoff_date)
