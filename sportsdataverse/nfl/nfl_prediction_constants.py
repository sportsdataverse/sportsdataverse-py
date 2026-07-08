"""NFL ratings-and-market spine constants + validation metrics (compute-on-demand).

The single home for the spine's fitted constants (ridge lambda, HFA, margin
sigma, points-per-net, totals, prop shrinkage/priors), the shared validation
metric helpers (Brier / log-loss / Spearman / MAE / calibration table), and the
as-of-date leakage split. League-agnostic algorithms, league-specific
constants: no fitted number is hard-coded inside an algorithm module.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.stats import rankdata


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Mean squared error between predicted probabilities and binary outcomes.

    Args:
        y_true: Binary outcomes (0/1).
        p_pred: Predicted probabilities in ``[0, 1]``.

    Returns:
        The Brier score (lower is better; 0.0 is perfect).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import brier_score
            brier_score(np.array([1, 0]), np.array([0.75, 0.25]))
    """
    return float(np.mean((np.asarray(p_pred, float) - np.asarray(y_true, float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary cross-entropy between predicted probabilities and outcomes.

    Args:
        y_true: Binary outcomes (0/1).
        p_pred: Predicted probabilities in ``[0, 1]`` (clipped by ``eps``).
        eps: Clip bound keeping ``log`` finite.

    Returns:
        The mean negative log-likelihood (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    p = np.clip(np.asarray(p_pred, float), eps, 1 - eps)
    y = np.asarray(y_true, float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two vectors.

    Args:
        a: First vector.
        b: Second vector (same length).

    Returns:
        The rank correlation in ``[-1, 1]``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0, 30.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two vectors.

    Args:
        a: First vector.
        b: Second vector (same length).

    Returns:
        The mean absolute difference.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, float) - np.asarray(b, float))))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bucket predictions into probability bins and compare to realized rates.

    Args:
        y_true: Binary outcomes (0/1).
        p_pred: Predicted probabilities in ``[0, 1]``.
        n_bins: Number of equal-width probability bins.

    Returns:
        pl.DataFrame: One row per non-empty bin with columns ``bin_mid``
        (Float64), ``mean_pred`` (Float64), ``mean_actual`` (Float64), ``n``
        (Int64/UInt32 count).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import calibration_table
            tbl = calibration_table(np.array([1, 0, 1, 1]), np.array([0.9, 0.2, 0.7, 0.6]))
            print(tbl.shape)
    """
    df = pl.DataFrame({"y": np.asarray(y_true, float), "p": np.asarray(p_pred, float)})
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
