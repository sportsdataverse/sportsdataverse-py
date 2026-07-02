"""Tests for nba_adj_rapm._fit_prior_ridge.

Validates the residualized ridge point estimate and the RTO posterior
against the closed-form Bayesian posterior on a tiny dense design.
"""

from __future__ import annotations

import numpy as np
from scipy.sparse import csr_matrix

from sportsdataverse.nba.nba_adj_rapm import _fit_prior_ridge


def test_rto_posterior_matches_closed_form() -> None:
    rng = np.random.default_rng(0)
    n, twoP = 400, 6  # tiny P=3 design
    Xd = rng.normal(0, 1, (n, twoP))
    beta_true = np.array([0.4, -0.2, 0.1, 0.3, -0.1, 0.2])
    y = Xd @ beta_true + rng.normal(0, 0.5, n)
    X = csr_matrix(Xd)
    prior_mean = np.array([0.1, 0.0, 0.0, 0.0, 0.0, 0.0])
    fit = _fit_prior_ridge(X, y, prior_mean, alphas=np.array([10.0]), n_samples=4000, seed=1)
    lam = 10.0
    XtX = Xd.T @ Xd
    beta_hat = prior_mean + np.linalg.solve(XtX + lam * np.eye(twoP), Xd.T @ (y - Xd @ prior_mean))
    resid = y - Xd @ beta_hat
    sigma2 = float(resid @ resid) / (n - twoP)
    cov_cf = sigma2 * np.linalg.inv(XtX + lam * np.eye(twoP))
    # RTO posterior mean ≈ β̂ ; RTO posterior cov ≈ closed-form (loose tol — sampling noise)
    assert np.allclose(fit.coef, beta_hat, atol=1e-6)
    assert np.allclose(fit.posterior.mean(axis=0), beta_hat, atol=0.03)
    assert np.allclose(np.cov(fit.posterior.T), cov_cf, atol=0.02)
