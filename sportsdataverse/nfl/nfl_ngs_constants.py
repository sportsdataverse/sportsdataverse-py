"""NGS over-expected stat machinery (shrinkage, context ridge, metrics).

Compute-on-demand: nothing here loads data or bundles an artifact. The
empirical-Bayes prior (``mu``, ``tau2``, ``sigma2``) and the
expected-separation ridge are fit from the caller-supplied panel at call
time. Shared by every model in
:mod:`sportsdataverse.nfl.nfl_ngs_tracking`.
"""

from __future__ import annotations

from typing import Optional, Tuple

import numpy as np
import polars as pl
from scipy.stats import rankdata

RECEIVER_POSITIONS: Tuple[str, ...] = ("WR", "TE", "RB", "FB")
SEP_RIDGE_LAMBDA: float = 1.0
MIN_RECEPTIONS: int = 10
MIN_TARGETS: int = 20
MIN_ATTEMPTS: int = 20


def empirical_bayes_shrink(
    x: np.ndarray, n: np.ndarray, *, prior_mean: Optional[float] = None
) -> Tuple[np.ndarray, np.ndarray]:
    """Efron-Morris empirical-Bayes shrinkage of per-unit averages.

    Model ``x_i = theta_i + e_i`` with ``e_i ~ (0, sigma2/n_i)`` and
    ``theta_i ~ (mu, tau2)``. ``tau2`` (intercept) and ``sigma2`` (slope)
    are moment-estimated by OLS of ``(x_i - mu)**2`` on ``1/n_i``.

    Args:
        x (np.ndarray): Per-unit observed averages (e.g. YAC over expected
            per reception).
        n (np.ndarray): Per-unit sample sizes (e.g. receptions). Rows with
            ``n <= 0`` get zero reliability and shrink fully to the prior.
        prior_mean (Optional[float]): Override for the prior mean ``mu``.
            Defaults to the size-weighted mean of ``x``.

    Returns:
        Tuple[np.ndarray, np.ndarray]: ``(shrunk, reliability)`` where
        ``reliability = tau2 / (tau2 + sigma2 / n)`` in ``[0, 1]`` and
        ``shrunk = mu + reliability * (x - mu)``.

    Example:
        Shrink noisy small-sample rates::

            import numpy as np
            from sportsdataverse.nfl.nfl_ngs_constants import empirical_bayes_shrink
            shrunk, rel = empirical_bayes_shrink(
                np.array([4.0, 0.5]), np.array([3.0, 90.0])
            )
    """
    x = np.asarray(x, dtype=float)
    n = np.asarray(n, dtype=float)
    if x.size == 0:
        return x.copy(), np.zeros_like(x)
    n = np.where(n <= 0, np.nan, n)
    w = np.nan_to_num(n, nan=0.0)
    mu = prior_mean if prior_mean is not None else float(np.sum(w * x) / max(w.sum(), 1e-9))
    inv_n = 1.0 / n
    d2 = (x - mu) ** 2
    ok = np.isfinite(inv_n) & np.isfinite(d2)
    if ok.sum() >= 2 and np.ptp(inv_n[ok]) > 0:
        design = np.column_stack([np.ones(int(ok.sum())), inv_n[ok]])
        coef, *_ = np.linalg.lstsq(design, d2[ok], rcond=None)
        tau2 = max(float(coef[0]), 1e-12)
        sigma2 = max(float(coef[1]), 0.0)
    else:  # degenerate panel: no reliable split -> shrink nothing
        tau2, sigma2 = 1.0, 0.0
    reliability = tau2 / (tau2 + sigma2 * np.nan_to_num(inv_n, nan=np.inf))
    shrunk = mu + reliability * (np.nan_to_num(x, nan=mu) - mu)
    return shrunk, reliability


def expected_separation_ridge(
    y: np.ndarray,
    features: np.ndarray,
    weights: np.ndarray,
    *,
    lam: float = SEP_RIDGE_LAMBDA,
) -> Tuple[np.ndarray, np.ndarray]:
    """Weighted ridge regression, closed form, intercept unpenalized.

    Args:
        y (np.ndarray): Target vector (e.g. ``avg_separation``).
        features (np.ndarray): 2-D feature matrix (no intercept column).
        weights (np.ndarray): Non-negative row weights (e.g. targets).
        lam (float): Ridge penalty on the non-intercept coefficients.

    Returns:
        Tuple[np.ndarray, np.ndarray]: ``(expected, coef)`` where ``coef``
        includes the intercept as element 0.

    Example:
        Fit and predict in one call::

            import numpy as np
            from sportsdataverse.nfl.nfl_ngs_constants import expected_separation_ridge
            yhat, beta = expected_separation_ridge(
                np.array([3.0, 4.0]), np.array([[4.0], [6.0]]), np.array([50.0, 40.0])
            )
    """
    y = np.asarray(y, dtype=float)
    feature_matrix = np.column_stack([np.ones(len(y)), np.asarray(features, dtype=float)])
    w = np.nan_to_num(np.asarray(weights, dtype=float), nan=0.0)
    weighted = feature_matrix * w[:, None]
    pen = lam * np.eye(feature_matrix.shape[1])
    pen[0, 0] = 0.0  # do not penalize intercept
    beta = np.linalg.solve(feature_matrix.T @ weighted + pen, weighted.T @ y)
    return feature_matrix @ beta, beta


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation of two same-length vectors."""
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two same-length vectors."""
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def next_season_stability(cur: pl.DataFrame, nxt: pl.DataFrame, key: str, cur_col: str, nxt_col: str) -> float:
    """Pearson correlation of a season-N metric with its season-N+1 value.

    Joins ``cur`` and ``nxt`` on ``key`` (inner, dtype-guarded) and
    correlates ``cur_col`` against ``nxt_col`` over players present in
    both seasons. Returns ``nan`` when fewer than 3 players overlap.
    """
    assert cur.schema[key] == nxt.schema[key], f"{key} dtype mismatch: {cur.schema[key]} vs {nxt.schema[key]}"
    joined = cur.select(key, cur_col).join(nxt.select(key, nxt_col), on=key, how="inner")
    if joined.height < 3:
        return float("nan")
    return float(np.corrcoef(joined[cur_col].to_numpy(), joined[nxt_col].to_numpy())[0, 1])
