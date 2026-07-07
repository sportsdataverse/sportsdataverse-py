"""Prediction-stack shared constants + validation metrics (league-agnostic).

Home of the small, dependency-light pieces every module in the MBB/WBB
prediction & tournament stack shares:

* **Validation metrics** (:func:`brier_score`, :func:`log_loss_score`,
  :func:`spearman_corr`, :func:`mae`, :func:`calibration_table`) used by the
  phase oracle/backtest gates.

Later phases extend this module with the per-league constants table
(``LEAGUE_CONSTANTS`` / :func:`get_constants`) and the as-of-date leakage
split (:func:`as_of_ratings_split`).
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.stats import rankdata


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Mean squared error between binary outcomes and predicted probabilities.

    Args:
        y_true: Array of realized binary outcomes (0/1).
        p_pred: Array of predicted probabilities in ``[0, 1]``.

    Returns:
        The Brier score (lower is better; 0.0 is perfect).

    Example:
        Perfect predictions score zero::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import brier_score
            brier_score(np.array([1, 0]), np.array([1.0, 0.0]))
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary cross-entropy (log loss) between outcomes and probabilities.

    Args:
        y_true: Array of realized binary outcomes (0/1).
        p_pred: Array of predicted probabilities in ``[0, 1]``.
        eps: Clipping bound to keep the log finite at 0/1.

    Returns:
        The mean log loss (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    p = np.clip(np.asarray(p_pred, dtype=float), eps, 1 - eps)
    y = np.asarray(y_true, dtype=float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two arrays.

    Args:
        a: First array.
        b: Second array (same length as ``a``).

    Returns:
        The Spearman rank-correlation coefficient in ``[-1, 1]``.

    Example:
        A monotonic relationship scores 1.0::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0, 30.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two arrays.

    Args:
        a: First array.
        b: Second array (same length as ``a``).

    Returns:
        The mean absolute error.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bin predicted probabilities and compare mean-predicted vs mean-actual.

    Args:
        y_true: Array of realized binary outcomes (0/1).
        p_pred: Array of predicted probabilities in ``[0, 1]``.
        n_bins: Number of equal-width probability bins.

    Returns:
        A ``polars.DataFrame`` with columns ``bin_mid``, ``mean_pred``,
        ``mean_actual``, ``n`` (one row per non-empty bin, sorted ascending).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import calibration_table
            y = np.random.default_rng(0).integers(0, 2, 200)
            p = np.random.default_rng(1).random(200)
            calibration_table(y, p, n_bins=10)
    """
    df = pl.DataFrame(
        {
            "y": np.asarray(y_true, dtype=float),
            "p": np.asarray(p_pred, dtype=float),
        }
    )
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    out = (
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
    return out
