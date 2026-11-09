"""Prior-informed (Bayesian) RAPM: ridge toward a box prior + randomize-then-optimize posterior."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Tuple, Union

import numpy as np
import pandas as pd
import polars as pl
from scipy.linalg import cho_factor, cho_solve
from scipy.sparse import csr_matrix
from sklearn.linear_model import RidgeCV

from sportsdataverse.nba.nba_model_validation import FitResult
from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, build_rapm_design


def _fit_prior_ridge(
    X: csr_matrix,
    y: np.ndarray,
    prior_mean: np.ndarray,
    *,
    alphas: np.ndarray = DEFAULT_RAPM_ALPHAS,
    n_samples: int = 200,
    seed: int = 0,
) -> FitResult:
    """Residualized ridge toward ``prior_mean`` with an unpenalized intercept + RTO Gaussian posterior.

    Residualizes the response ``y' = y - X @ prior_mean``, picks the regularisation strength
    ``λ`` with an intercept-fitted :class:`~sklearn.linear_model.RidgeCV` over ``alphas``, and
    solves the intercept-fitted ridge exactly for ``δ̂`` (the same centred-Gram solve as
    :func:`~sportsdataverse.nba.nba_rapm.ridge_rapm_coef`). The point estimate is
    ``β̂ = prior_mean + δ̂``. The intercept carries the league's points per possession; without it
    the penalty splits that ~1.1 points between every offensive and defensive coefficient (the
    o/d ratings then sat near +11 / -11 per 100, and a thinly-used player kept less of the offset).

    The randomize-then-optimize (RTO) posterior draws ``S`` samples from the
    Gaussian posterior whose covariance is ``σ̂²(XcᵀXc + λI)⁻¹``, ``Xc`` the column-centred design:

    * ``ey ~ N(0, σ̂²·Iₙ)``
    * ``ep ~ N(0, λσ̂²·I₂ₚ)``
    * Each sample solves ``(XcᵀXc + λI)δ = Xcᵀ(y' + ey) + ep``,
      then maps back via ``β_s = prior_mean + δ``.

    ``A = XcᵀXc + λI`` (dense ``2P x 2P``) is Cholesky-factorized once and back-solved ``S``
    times, keeping the loop cheap.

    Args:
        X: Sparse design ``(n, 2P)`` from :func:`~sportsdataverse.nba.nba_rapm.build_rapm_design`.
        y: Possession points ``(n,)``.
        prior_mean: Per-possession prior mean ``(2P,)`` (the harness-aligned μ).
        alphas: RidgeCV grid for λ (prior strength).
        n_samples: Number of RTO posterior samples ``S``.
        seed: RNG seed for reproducibility.

    Returns:
        :class:`~sportsdataverse.nba.nba_model_validation.FitResult` with
        ``coef=β̂``, the fitted ``intercept`` (points per possession at the prior), and
        ``posterior`` of shape ``(S, 2P)``.

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

    # λ by cross-validation, with the intercept fitted (unpenalized), as nba_rapm picks it
    lam = float(RidgeCV(alphas=alphas, fit_intercept=True).fit(X, yprime).alpha_)

    # Intercept-fitted ridge: centre the design through its Gram matrix, A = XcᵀXc + λI
    mean = np.asarray(X.mean(axis=0), dtype=np.float64).ravel()
    A = (X.T @ X).toarray() - n * np.outer(mean, mean)
    A[np.diag_indices_from(A)] += lam
    chol = cho_factor(A)

    def xct(v: np.ndarray) -> np.ndarray:  # Xcᵀ v without densifying X
        return np.asarray(X.T @ v, dtype=np.float64).ravel() - n * mean * float(v.mean())

    Xct_yprime = xct(yprime)  # the deterministic part of the RHS
    delta_hat = cho_solve(chol, Xct_yprime)
    intercept = float(yprime.mean() - mean @ delta_hat)
    beta_hat = prior_mean + delta_hat

    # Residual variance estimate (the intercept is one more fitted parameter)
    resid = yprime - X @ delta_hat - intercept
    dof = max(n - two_p - 1, 1)
    sigma2 = float(resid @ resid) / dof
    sigma = float(np.sqrt(sigma2))

    rng = np.random.default_rng(seed)
    samples = np.empty((n_samples, two_p), dtype=np.float64)
    for s in range(n_samples):
        ey = rng.normal(0.0, sigma, size=n)
        ep = rng.normal(0.0, np.sqrt(lam) * sigma, size=two_p)
        samples[s] = prior_mean + cho_solve(chol, Xct_yprime + xct(ey) + ep)

    return FitResult(coef=beta_hat, intercept=intercept, posterior=samples)


@dataclass
class AdjRapmModel:
    """Prior-informed RAPM: ridge toward a per-player box prior with an RTO posterior.

    Implements the :class:`~sportsdataverse.nba.nba_model_validation.PriorModel`
    protocol so the validation harness routes through ``fit_with_prior`` and the
    resulting :class:`~sportsdataverse.nba.nba_model_validation.FitResult` carries
    a posterior — enabling Oracle ④ (interval calibration).

    Attributes:
        prior: Per-player ``{player_id: (o_prior, d_prior)}`` in per-100 units.
        alphas: RidgeCV alpha grid forwarded to ``_fit_prior_ridge``.
        n_samples: Number of RTO posterior samples.
        seed: RNG seed for the RTO sampler.

    Example:
        Fit adj-RAPM with an SPM prior and validate (calibration now populated)::

            from sportsdataverse.nba import AdjRapmModel, nba_spm
            from sportsdataverse.nba.nba_model_validation import validate_model
            prior = AdjRapmModel.from_spm(nba_spm(box_feats, coef))
            report = validate_model(prior, season_frames, model_name="adj_rapm")
            print(report.calibration.coverage)      # non-None: the prior model has a posterior
    """

    prior: Dict[int, Tuple[float, float]]
    alphas: np.ndarray = field(default_factory=lambda: DEFAULT_RAPM_ALPHAS)
    n_samples: int = 200
    seed: int = 0

    def fit_with_prior(self, X: csr_matrix, y: np.ndarray, prior_mean: np.ndarray) -> FitResult:
        """Delegate to ``_fit_prior_ridge`` using this model's hyperparameters.

        Args:
            X: Sparse design ``(n, 2P)`` from :func:`~sportsdataverse.nba.nba_rapm.build_rapm_design`.
            y: Possession points ``(n,)``.
            prior_mean: Per-possession prior mean ``(2P,)`` built by the harness.

        Returns:
            :class:`~sportsdataverse.nba.nba_model_validation.FitResult` with posterior
            of shape ``(n_samples, 2P)``.
        """
        return _fit_prior_ridge(
            X,
            y,
            prior_mean,
            alphas=self.alphas,
            n_samples=self.n_samples,
            seed=self.seed,
        )

    @classmethod
    def from_spm(cls, spm: pl.DataFrame, **kw: Any) -> "AdjRapmModel":
        """Construct from an SPM output frame (``ospm`` / ``dspm`` columns).

        Args:
            spm: Frame with ``player_id``, ``ospm``, ``dspm`` columns (per-100 units).
            **kw: Extra keyword arguments forwarded to the constructor
                (``alphas``, ``n_samples``, ``seed``).

        Returns:
            ``AdjRapmModel`` whose ``prior`` maps each player_id to ``(ospm, dspm)``.
        """
        prior: Dict[int, Tuple[float, float]] = {
            int(r["player_id"]): (float(r["ospm"]), float(r["dspm"])) for r in spm.iter_rows(named=True)
        }
        return cls(prior, **kw)

    @classmethod
    def from_bpm(cls, bpm: pl.DataFrame, **kw: Any) -> "AdjRapmModel":
        """Construct from a BPM output frame (``obpm`` / ``dbpm`` columns).

        Args:
            bpm: Frame with ``player_id``, ``obpm``, ``dbpm`` columns (per-100 units).
            **kw: Extra keyword arguments forwarded to the constructor
                (``alphas``, ``n_samples``, ``seed``).

        Returns:
            ``AdjRapmModel`` whose ``prior`` maps each player_id to ``(obpm, dbpm)``.
        """
        prior: Dict[int, Tuple[float, float]] = {
            int(r["player_id"]): (float(r["obpm"]), float(r["dbpm"])) for r in bpm.iter_rows(named=True)
        }
        return cls(prior, **kw)


def nba_adj_rapm(
    possessions: pl.DataFrame,
    prior: Dict[int, Tuple[float, float]],
    *,
    alphas: np.ndarray = DEFAULT_RAPM_ALPHAS,
    n_samples: int = 200,
    seed: int = 0,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """One-shot prior-informed RAPM over a possession frame -> per-player ratings.

    Builds the sparse design matrix via
    :func:`~sportsdataverse.nba.nba_rapm.build_rapm_design`, constructs the
    per-possession ``prior_mean`` vector from ``prior``, fits a residualized
    ridge with an RTO posterior via :func:`_fit_prior_ridge`, and returns the
    per-player offensive, defensive, and combined adj-RAPM ratings alongside
    possession counts.

    Sign convention (matches :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`):
    ``d_adj_rapm`` is positive for a good defender (lowers opponent points);
    ``adj_rapm = o_adj_rapm + d_adj_rapm`` exactly.

    Centring: the ridge fits an unpenalized intercept (the league's points per possession), as
    :func:`~sportsdataverse.nba.nba_rapm.nba_rapm` does, so ``o_adj_rapm`` and ``d_adj_rapm`` are
    each relative to an average player: their possession-weighted means sit near 0 (2025-26 NBA
    +0.32 / +0.06, 2025 WNBA +0.45 / +0.11, against o_rapm's +0.41 / +0.11). Fit without one,
    as every release fit before #1137 was, the penalty split that level between every offensive and defensive
    coefficient: o / d read about +12 / -11 (2025-26 NBA +12.03 / -10.84), a thinly-used
    player kept less of the offset (-8.9 on offense under 200 possessions), and so ``adj_rapm``
    itself leaned toward heavy-minute players.

    Args:
        possessions: A possession+lineup frame produced by the possession engine
            (``game_id``, ``offense_team_id``, ``points``,
            ``off_player_1..5``, ``def_player_1..5``).
        prior: Per-player ``{player_id: (o_prior, d_prior)}`` in per-100 units.
            Players absent from ``prior`` receive a ``(0.0, 0.0)`` default.
        alphas: RidgeCV alpha grid for the regularisation strength (default
            ``DEFAULT_RAPM_ALPHAS``).
        n_samples: Number of RTO posterior samples (default 200).
        seed: RNG seed for the RTO sampler (default 0).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        Frame with columns ``player_id`` (Int64), ``o_adj_rapm`` (Float64),
        ``d_adj_rapm`` (Float64), ``adj_rapm`` (Float64), ``off_poss`` (Int64),
        ``def_poss`` (Int64).

    Example:
        Fit with an SPM prior::

            from sportsdataverse.nba import nba_adj_rapm
            ratings = nba_adj_rapm(possessions, spm_prior_dict)
            print(ratings.sort("adj_rapm", descending=True).head())
    """
    X, y, pids = build_rapm_design(possessions)
    P = len(pids)
    prior_mean: np.ndarray = np.zeros(2 * P, dtype=np.float64)
    for k, pid in enumerate(pids):
        o, d = prior.get(int(pid), (0.0, 0.0))
        prior_mean[k] = o / 100.0
        prior_mean[P + k] = -d / 100.0
    fit = _fit_prior_ridge(X, y, prior_mean, alphas=alphas, n_samples=n_samples, seed=seed)
    o_r = fit.coef[:P] * 100.0
    d_r = -fit.coef[P:] * 100.0
    off_poss = np.asarray(X[:, :P].sum(axis=0)).ravel().astype(np.int64)
    def_poss = np.asarray(X[:, P:].sum(axis=0)).ravel().astype(np.int64)
    out = pl.DataFrame(
        {
            "player_id": pl.Series(pids, dtype=pl.Int64),
            "o_adj_rapm": pl.Series(o_r, dtype=pl.Float64),
            "d_adj_rapm": pl.Series(d_r, dtype=pl.Float64),
            "adj_rapm": pl.Series(o_r + d_r, dtype=pl.Float64),
            "off_poss": pl.Series(off_poss, dtype=pl.Int64),
            "def_poss": pl.Series(def_poss, dtype=pl.Int64),
        }
    )
    return out.to_pandas() if return_as_pandas else out
