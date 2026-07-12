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
from sportsdataverse._common.metrics import (
    mae as mae,
    spearman_corr as spearman_corr,
)

RECEIVER_POSITIONS: Tuple[str, ...] = ("WR", "TE", "RB", "FB")
# Separation-OE ridge penalty. A sweep over lambda {0.1, 1, 10} x min_targets
# {10, 20, 30} on held-out 2019-2022 season transitions moved the stability
# deltas by < 1e-4 (dev/nfl_ngs/stability_transitions.py) -- the surface is flat,
# so 1.0 is a neutral, non-tuned default (never fit on the evaluation fold).
SEP_RIDGE_LAMBDA: float = 1.0
# Per-player volume floors mirroring the NGS qualification convention (enough
# events for a stable rate); confirmed neutral in the same held-out sweep.
MIN_RECEPTIONS: int = 10
MIN_TARGETS: int = 20
MIN_ATTEMPTS: int = 20


def empirical_bayes_shrink(
    x: np.ndarray,
    n: np.ndarray,
    *,
    prior_mean: Optional[float] = None,
    sigma2: Optional[float] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Efron-Morris empirical-Bayes shrinkage of per-unit averages.

    Model ``x_i = theta_i + e_i`` with ``e_i ~ (0, sigma2/n_i)`` and
    ``theta_i ~ (mu, tau2)``. When ``sigma2`` is supplied (preferred —
    identify it from weekly rows via :func:`weekly_sigma2`), ``tau2`` is
    moment-estimated as ``mean((x_i - mu)**2 - sigma2/n_i)`` floored at a
    tiny positive value. Otherwise both ``tau2`` (intercept) and ``sigma2``
    (slope) come from OLS of ``(x_i - mu)**2`` on ``1/n_i`` — beware: that
    OLS is weakly identified when the spread of ``1/n_i`` is narrow (e.g.
    a season panel of only qualified rushers) and can floor ``tau2``,
    collapsing reliability to ~0.

    Args:
        x (np.ndarray): Per-unit observed averages (e.g. YAC over expected
            per reception).
        n (np.ndarray): Per-unit sample sizes (e.g. receptions). Rows with
            ``n <= 0`` get zero reliability and shrink fully to the prior.
        prior_mean (Optional[float]): Override for the prior mean ``mu``.
            Defaults to the size-weighted mean of ``x``.
        sigma2 (Optional[float]): Known within-unit sampling variance (per
            single trial). When given, only ``tau2`` is estimated.

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
    if sigma2 is not None and ok.any():
        sigma2 = float(sigma2)
        tau2 = max(float(np.mean(d2[ok] - sigma2 * inv_n[ok])), 1e-12)
    elif ok.sum() >= 2 and np.ptp(inv_n[ok]) > 0:
        design = np.column_stack([np.ones(int(ok.sum())), inv_n[ok]])
        coef, *_ = np.linalg.lstsq(design, d2[ok], rcond=None)
        tau2 = max(float(coef[0]), 1e-12)
        sigma2 = max(float(coef[1]), 0.0)
    else:  # degenerate panel: no reliable split -> shrink nothing
        tau2, sigma2 = 1.0, 0.0
    # n<=0 rows carry no data -> inv_n is +inf. When sigma2==0 (degenerate
    # panel, or an OLS slope floored to 0) the product 0*inf is an expected
    # NaN that would poison the shrunk estimate; silence that invalid-op and
    # force those rows to reliability 0 so they shrink fully to the prior.
    with np.errstate(invalid="ignore"):
        reliability = tau2 / (tau2 + sigma2 * np.nan_to_num(inv_n, nan=np.inf))
    reliability = np.nan_to_num(reliability, nan=0.0)
    shrunk = mu + reliability * (np.nan_to_num(x, nan=mu) - mu)
    return shrunk, reliability


def weekly_sigma2(
    weekly: pl.DataFrame,
    raw_col: str,
    weight_col: str,
    *,
    key: str = "player_gsis_id",
) -> Optional[float]:
    """Pooled within-unit sampling variance from weekly rows.

    For unit ``i`` in week ``w`` the model is ``x_iw ~ (theta_i,
    sigma2 / n_iw)``; since ``theta_i`` is constant within a unit-season,
    the weight-scaled squared deviations from the unit's weighted mean
    identify ``sigma2`` directly: ``sigma2_hat = sum_i sum_w n_iw *
    (x_iw - xbar_i)**2 / sum_i (W_i - 1)`` over units with 2+ weeks.
    This is the preferred identification for
    :func:`empirical_bayes_shrink` — the season-panel OLS fallback is
    weakly identified when all units have similar ``n``.

    Args:
        weekly (pl.DataFrame): Weekly rows for ONE season (caller filters).
        raw_col (str): Weekly per-trial average column (e.g.
            ``rush_yards_over_expected_per_att``).
        weight_col (str): Weekly trial count column (e.g. ``rush_attempts``).
        key (str): Unit id column. Defaults to ``player_gsis_id``.

    Returns:
        Optional[float]: Pooled ``sigma2``, or ``None`` when the input has
        no usable rows (caller should fall back to the OLS path).
    """
    if weekly.height == 0 or raw_col not in weekly.columns or weight_col not in weekly.columns:
        return None
    df = weekly.drop_nulls([raw_col, weight_col]).filter(pl.col(weight_col) > 0)
    ss = 0.0
    dof = 0
    for (_unit,), grp in df.group_by(key):
        if grp.height < 2:
            continue
        x = grp[raw_col].to_numpy().astype(float)
        n = grp[weight_col].to_numpy().astype(float)
        xbar = float(np.sum(n * x) / n.sum())
        ss += float(np.sum(n * (x - xbar) ** 2))
        dof += grp.height - 1
    if dof <= 0:
        return None
    return ss / dof


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


def next_season_stability(
    cur: pl.DataFrame, nxt: pl.DataFrame, key: str, cur_col: str, nxt_col: str, *, min_n: int = 3
) -> float:
    """Pearson correlation of a season-N metric with its season-N+1 value.

    Joins ``cur`` and ``nxt`` on ``key`` (inner, dtype-guarded) and
    correlates ``cur_col`` against ``nxt_col`` over players present in both
    seasons. Asserts the overlap is at least ``min_n`` so a shrunken /
    re-captured fixture cannot let a stability gate pass on a handful of
    players — callers pin ``min_n`` to the documented observed overlap.
    """
    assert cur.schema[key] == nxt.schema[key], f"{key} dtype mismatch: {cur.schema[key]} vs {nxt.schema[key]}"
    joined = cur.select(key, cur_col).join(nxt.select(key, nxt_col), on=key, how="inner")
    assert joined.height >= min_n, f"stability join {joined.height} < min_n {min_n} for {cur_col}"
    return float(np.corrcoef(joined[cur_col].to_numpy(), joined[nxt_col].to_numpy())[0, 1])
