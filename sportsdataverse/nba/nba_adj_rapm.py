"""Prior-informed (Bayesian) RAPM: ridge toward a box prior + randomize-then-optimize posterior."""

from __future__ import annotations

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse import eye as sp_eye
from scipy.sparse.linalg import factorized
from sklearn.linear_model import RidgeCV

from sportsdataverse.nba.nba_model_validation import FitResult
from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS


def _fit_prior_ridge(
    X: csr_matrix,
    y: np.ndarray,
    prior_mean: np.ndarray,
    *,
    alphas: np.ndarray = DEFAULT_RAPM_ALPHAS,
    n_samples: int = 200,
    seed: int = 0,
) -> FitResult:
    """Residualized ridge toward ``prior_mean`` + RTO Gaussian posterior.

    Residualizes the response ``y' = y - X @ prior_mean``, fits a
    :class:`~sklearn.linear_model.RidgeCV` (no intercept) over ``alphas`` to
    obtain ``δ̂`` and the selected regularisation strength ``λ``, then derives
    the point estimate ``β̂ = prior_mean + δ̂``.

    The randomize-then-optimize (RTO) posterior draws ``S`` samples from the
    Gaussian posterior whose covariance is ``σ̂²(XᵀX + λI)⁻¹``:

    * ``ey ~ N(0, σ̂²·Iₙ)``
    * ``ep ~ N(0, λσ̂²·I₂ₚ)``
    * Each sample solves ``(XᵀX + λI)δ = Xᵀ(y' + ey) + ep``,
      then maps back via ``β_s = prior_mean + δ``.

    ``A = XᵀX + λI`` is factorized once via :func:`scipy.sparse.linalg.factorized`
    and back-solved ``S`` times, keeping the loop cheap.

    Args:
        X: Sparse design ``(n, 2P)`` from :func:`~sportsdataverse.nba.nba_rapm.build_rapm_design`.
        y: Possession points ``(n,)``.
        prior_mean: Per-possession prior mean ``(2P,)`` (the harness-aligned μ).
        alphas: RidgeCV grid for λ (prior strength).
        n_samples: Number of RTO posterior samples ``S``.
        seed: RNG seed for reproducibility.

    Returns:
        :class:`~sportsdataverse.nba.nba_model_validation.FitResult` with
        ``coef=β̂``, ``intercept=0.0``, and ``posterior`` of shape ``(S, 2P)``.

    Example:
        Quick start::

            import numpy as np
            from scipy.sparse import csr_matrix
            from sportsdataverse.nba.nba_adj_rapm import _fit_prior_ridge

            rng = np.random.default_rng(42)
            n, two_p = 500, 6
            X = csr_matrix(rng.normal(0, 1, (n, two_p)))
            y = rng.normal(0, 1, n)
            prior_mean = np.zeros(two_p)
            fit = _fit_prior_ridge(X, y, prior_mean, n_samples=200, seed=0)
            print(fit.coef.shape, fit.posterior.shape)
    """
    n, two_p = X.shape
    prior_mean = np.asarray(prior_mean, dtype=np.float64)

    # Residualize: y' = y - X @ prior_mean
    yprime = np.asarray(y, dtype=np.float64) - X @ prior_mean

    # Ridge on residualized problem; select λ via cross-validation
    ridge = RidgeCV(alphas=alphas, fit_intercept=False).fit(X, yprime)
    lam = float(ridge.alpha_)
    delta_hat = np.asarray(ridge.coef_, dtype=np.float64)
    beta_hat = prior_mean + delta_hat

    # Residual variance estimate
    resid = yprime - X @ delta_hat
    dof = max(n - two_p, 1)
    sigma2 = float(resid @ resid) / dof
    sigma = float(np.sqrt(sigma2))

    # Factorize A = XᵀX + λI once; back-solve S times
    A = (X.T @ X + lam * sp_eye(two_p, format="csc")).tocsc()
    solve = factorized(A)

    # Xᵀy' is the deterministic part of the RHS
    Xt_yprime = np.asarray(X.T @ yprime, dtype=np.float64).ravel()

    rng = np.random.default_rng(seed)
    samples = np.empty((n_samples, two_p), dtype=np.float64)
    for s in range(n_samples):
        ey = rng.normal(0.0, sigma, size=n)
        ep = rng.normal(0.0, np.sqrt(lam) * sigma, size=two_p)
        rhs = Xt_yprime + np.asarray(X.T @ ey, dtype=np.float64).ravel() + ep
        samples[s] = prior_mean + solve(rhs)

    return FitResult(coef=beta_hat, intercept=0.0, posterior=samples)
