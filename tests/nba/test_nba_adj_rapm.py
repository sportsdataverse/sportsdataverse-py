"""Tests for nba_adj_rapm._fit_prior_ridge.

Validates the residualized ridge point estimate and the RTO posterior
against the closed-form Bayesian posterior on a tiny dense design.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.sparse import csr_matrix

from sportsdataverse.nba.nba_adj_rapm import AdjRapmModel, _fit_prior_ridge, nba_adj_rapm
from sportsdataverse.nba.nba_model_validation import _synthetic_possessions, validate_model


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


def test_from_spm_maps_columns() -> None:
    spm = pl.DataFrame(
        {
            "player_id": [1, 2],
            "ospm": [3.0, -1.0],
            "dspm": [1.0, 2.0],
            "spm": [4.0, 1.0],
            "min": [500.0, 400.0],
            "gp": [20, 18],
        }
    )
    m = AdjRapmModel.from_spm(spm)
    assert m.prior[1] == (3.0, 1.0) and m.prior[2] == (-1.0, 2.0)


def test_adj_rapm_routes_and_calibrates() -> None:
    o = {p: 0.03 for p in list(range(100, 108)) + list(range(200, 208))}
    d = {p: 0.01 for p in o}
    poss = _synthetic_possessions(o, d, n_games=30, poss_per_game=40, noise_sd=0.3, seed=1)
    prior = {p: (o[p] * 100, d[p] * 100) for p in o}  # per-100 prior ≈ the truth
    model = AdjRapmModel(prior, n_samples=100, seed=0)
    rep = validate_model(model, [poss], model_name="adj_rapm", oracles=("retrodiction", "calibration"))
    assert rep.retrodiction is not None
    assert rep.calibration is not None  # posterior -> calibration ACTIVATES


def test_nba_adj_rapm_public_schema() -> None:
    o = {p: 0.02 for p in list(range(100, 106)) + list(range(200, 206))}
    d = {p: 0.01 for p in o}
    poss = _synthetic_possessions(o, d, n_games=20, poss_per_game=40, noise_sd=0.3, seed=2)
    prior = {p: (0.0, 0.0) for p in o}
    out = nba_adj_rapm(poss, prior, n_samples=50)
    assert set(out.columns) == {"player_id", "o_adj_rapm", "d_adj_rapm", "adj_rapm", "off_poss", "def_poss"}
    assert out.schema["player_id"] == pl.Int64 and out.schema["adj_rapm"] == pl.Float64
