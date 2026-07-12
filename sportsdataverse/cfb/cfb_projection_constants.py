"""Shared substrate for the CFB recruiting/roster projection spine (T2.2).

Division-agnostic algorithms; FBS/FCS-specific numbers live in ``DIVISION_CONSTANTS``.
Star->points and returning-production weights are seeded from published methodology
(247Sports Team Talent Composite; Bill Connelly returning production) and refined by
the concrete fitting tasks in the implementation plan — never invented finals.

Holds: the ``ProjectionConstants`` table + ``get_constants``; the validation metrics
(``spearman_corr`` / ``mae`` / ``rmse`` / ``r2_score`` / ``roc_auc`` / ``brier_score``);
the leakage-boundary ``as_of_season_split``; and closed-form ``fit_ridge`` /
``fit_logistic`` (+ their ``predict_*``) so every model fits on demand — no bundled artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl
from scipy.optimize import minimize
from scipy.stats import rankdata
from sportsdataverse._common.metrics import (
    brier_score as brier_score,
    mae as mae,
    spearman_corr as spearman_corr,
)


@dataclass(frozen=True)
class ProjectionConstants:
    """Division-specific constants for the projection spine (algorithms stay generic).

    Args:
        division: Division slug (``"fbs"`` / ``"fcs"``).
        blue_chip_star_min: Minimum recruit star rating that counts as "blue chip".
        star_points: Recruit star rating -> talent points (monotone; 247-style).
        class_recency_weights: Eligibility-decay weights per recruiting class,
            most-recent class first (length 4).
        returning_prod_weights: Offense/defense weights for the returning-production
            composite (seeded from Connelly; refit in Task 2.3).
        bluechip_title_base_rate: Base rate that a national champion is a blue-chip-ratio
            team (the well-known blue-chip -> title invariant).
        avg_wins: League-average wins per team-season (baseline for projections).
    """

    division: str
    blue_chip_star_min: int
    star_points: dict[int, float]
    class_recency_weights: tuple[float, ...]  # len 4, most-recent class first
    returning_prod_weights: dict[str, float]  # seeded from Connelly; Task 2.3 refits
    bluechip_title_base_rate: float
    avg_wins: float


# Seeds: 247-style star->points (monotone), 4-class eligibility decay, Connelly unit
# weights. Task 1.4 calibrates class_recency_weights; Task 2.3 refits returning weights.
DIVISION_CONSTANTS: dict[str, ProjectionConstants] = {
    "fbs": ProjectionConstants(
        division="fbs",
        blue_chip_star_min=4,
        star_points={5: 100.0, 4: 70.0, 3: 45.0, 2: 25.0, 1: 10.0, 0: 20.0},
        class_recency_weights=(1.0, 0.9, 0.75, 0.55),
        # Fitted by dev/cfb_projection/fit_returning_weights.py on FBS 2018-2023
        # (n=794 team-seasons): std-coefs off=+2.52, def=-0.86 margin-pts/SD --
        # the splash-event defensive measure carries no positive signal, so the
        # overall combination weights offense only (defense clamped at 0).
        returning_prod_weights={"offense": 1.0, "defense": 0.0},
        bluechip_title_base_rate=0.50,
        avg_wins=6.0,
    ),
    "fcs": ProjectionConstants(
        division="fcs",
        blue_chip_star_min=3,
        star_points={5: 100.0, 4: 70.0, 3: 45.0, 2: 25.0, 1: 10.0, 0: 15.0},
        class_recency_weights=(1.0, 0.9, 0.75, 0.55),
        returning_prod_weights={"offense": 1.0, "defense": 1.0},
        bluechip_title_base_rate=0.30,
        avg_wins=6.0,
    ),
}


def get_constants(division: str = "fbs") -> ProjectionConstants:
    """Look up the :class:`ProjectionConstants` for a division (case-insensitive).

    Args:
        division: Division slug; ``"fbs"`` or ``"fcs"``.

    Returns:
        The registered :class:`ProjectionConstants`.

    Raises:
        ValueError: If ``division`` is not a registered slug.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_projection_constants import get_constants
            get_constants("fbs").blue_chip_star_min
    """
    key = division.lower()
    if key not in DIVISION_CONSTANTS:
        raise ValueError(f"unknown division {division!r}; expected one of {sorted(DIVISION_CONSTANTS)}")
    return DIVISION_CONSTANTS[key]


def rmse(a: np.ndarray, b: np.ndarray) -> float:
    """Root-mean-squared error between two arrays."""
    return float(np.sqrt(np.mean((np.asarray(a, float) - np.asarray(b, float)) ** 2)))


def r2_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Coefficient of determination (1 - SS_res / SS_tot); 0.0 when the target is constant."""
    y = np.asarray(y_true, float)
    ss_res = float(np.sum((y - np.asarray(y_pred, float)) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    return 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0


def roc_auc(y_true: np.ndarray, score: np.ndarray) -> float:
    """ROC AUC via the Mann-Whitney U statistic (ties -> average rank); 0.5 if one class empty."""
    y = np.asarray(y_true, float)
    s = np.asarray(score, float)
    n_pos, n_neg = float((y == 1).sum()), float((y == 0).sum())
    if n_pos == 0 or n_neg == 0:
        return 0.5
    order = rankdata(s)
    auc = (order[y == 1].sum() - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
    return float(auc)


def as_of_season_split(df: pl.DataFrame, cutoff_season: int, *, season_col: str = "season") -> pl.DataFrame:
    """Filter to rows strictly before ``cutoff_season`` (the leakage boundary).

    Args:
        df: A frame carrying a season column.
        cutoff_season: Seasons on or after this are excluded.
        season_col: Name of the season column.

    Returns:
        Rows with ``season < cutoff_season``.
    """
    return df.filter(pl.col(season_col) < cutoff_season)


def fit_ridge(X: np.ndarray, y: np.ndarray, alpha: float = 1.0) -> tuple[float, np.ndarray]:
    """Closed-form ridge regression with an unpenalized intercept.

    Args:
        X: Design matrix, shape ``(n, d)``.
        y: Target vector, shape ``(n,)``.
        alpha: L2 penalty on the slopes (the intercept is not penalized).

    Returns:
        ``(intercept, coef)`` where ``coef`` has shape ``(d,)``.
    """
    X = np.asarray(X, float)
    y = np.asarray(y, float)
    n, d = X.shape
    xb = np.hstack([np.ones((n, 1)), X])
    a = xb.T @ xb + alpha * np.eye(d + 1)
    a[0, 0] -= alpha  # do not penalize the intercept
    beta = np.linalg.solve(a, xb.T @ y)
    return float(beta[0]), beta[1:]


def predict_ridge(intercept: float, coef: np.ndarray, X: np.ndarray) -> np.ndarray:
    """Linear prediction ``intercept + X @ coef``."""
    return intercept + np.asarray(X, float) @ np.asarray(coef, float)


def _sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))


def fit_logistic(X: np.ndarray, y: np.ndarray, l2: float = 1.0) -> tuple[float, np.ndarray]:
    """L2-penalized logistic regression via L-BFGS on the negative log-likelihood.

    Args:
        X: Design matrix, shape ``(n, d)``.
        y: Binary target vector (0/1), shape ``(n,)``.
        l2: L2 penalty on the slopes (the intercept is not penalized).

    Returns:
        ``(intercept, coef)`` where ``coef`` has shape ``(d,)``.
    """
    X = np.asarray(X, float)
    y = np.asarray(y, float)
    n, d = X.shape
    xb = np.hstack([np.ones((n, 1)), X])

    def nll(w: np.ndarray) -> float:
        p = _sigmoid(xb @ w)
        eps = 1e-12
        ll = np.sum(y * np.log(p + eps) + (1 - y) * np.log(1 - p + eps))
        pen = 0.5 * l2 * np.sum(w[1:] ** 2)
        return -ll + pen

    def grad(w: np.ndarray) -> np.ndarray:
        p = _sigmoid(xb @ w)
        g = xb.T @ (p - y)
        g[1:] += l2 * w[1:]
        return g

    res = minimize(nll, np.zeros(d + 1), jac=grad, method="L-BFGS-B")
    w = res.x
    return float(w[0]), w[1:]


def predict_logistic(intercept: float, coef: np.ndarray, X: np.ndarray) -> np.ndarray:
    """Logistic prediction ``sigmoid(intercept + X @ coef))``."""
    return _sigmoid(intercept + np.asarray(X, float) @ np.asarray(coef, float))
