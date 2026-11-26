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

from sportsdataverse._deprecation import warn_deprecated
from sportsdataverse.nba.nba_model_validation import FitResult
from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, _possessions_league, build_rapm_design

#: The adjusted-RAPM ridge penalty per league id, one value for every season and season type (as
#: :data:`~sportsdataverse.nba.nba_rapm.RAPM_PENALTY` is for plain RAPM, #1121). Fitted by
#: ``tools/fit_adj_rapm_penalty.py`` (refit 2026-11-25 for the minute-weighted SPM prior; first fit
#: 2026-11-09, #1150): the grid point (steps of x1.18) minimizing the game-grouped 10-fold CV error
#: summed over the complete regular seasons from 1997 (NBA through 2025-26, WNBA through 2026; 30
#: each), on the prior-residualized problem at the frozen prior scale below. The prior is the
#: previous season's SPM (RS + playoffs by minutes), trained on that season's plain RAPM by
#: :func:`~sportsdataverse.nba.nba_spm.train_spm` at its defaults, as the ``player_impact`` producers
#: chain it. The refit picks the same two values the equal-weight prior did. Before #1150 each fit
#: took RidgeCV's per-season pick over ``DEFAULT_RAPM_ALPHAS``, which flipped between 1,931 and 5,179
#: (WNBA 2004: 13,895) and collapsed the spread in those seasons (WNBA 2004 sd 0.68 per 100 against
#: plain RAPM's 1.50). At the frozen values the spread is 0.85 to 1.58 times plain RAPM's in every
#: NBA season and 0.83 to 1.39 in every WNBA one (off_poss >= 300). Next-season fit to plain RAPM (R^2
#: of a line fit the pair before): NBA 0.198, WNBA 0.107, against plain RAPM's 0.156 / 0.073 and the
#: SPM prior's own 0.168 / 0.093 (0.129 / 0.062 before minute weighting); the #1150 constants with the
#: equal-weight prior read 0.196 / 0.110. Year-over-year Spearman of adjusted RAPM itself: 0.581
#: (NBA) and 0.549 (WNBA), from 0.530 / 0.519. The G League (``"20"``) is seeded with the NBA value.
ADJ_RAPM_PENALTY: dict[str, float] = {"00": 3727.593720314938, "10": 2682.6957952797247}
ADJ_RAPM_PENALTY["20"] = ADJ_RAPM_PENALTY["00"]
#: The multiplier on the SPM prior per league id. SPM is a ridge fit to shrunk RAPM, so the prior is
#: narrower than the ratings it shrinks toward (minute-weighted SPM sd about 0.6 of plain RAPM's at
#: off_poss >= 300). On the grid 0 to 4, scored on the same held-out games, 1.5 lowers the pooled
#: error against the unscaled prior by 0.31 basis points (NBA, SE 0.13 over 30 seasons; WNBA 1.08,
#: SE 0.18), and next-season R^2 and Spearman peak at 1 to 1.5 (NBA R^2 0.198 at 1, 0.198 at 1.5,
#: 0.192 at 2; WNBA 0.107, 0.107, 0.101). NBA held-out error is lowest at 1.5 and rises at 2 (+0.22
#: bp against 1, SE 0.28); WNBA's keeps falling to 2 (-1.52, SE 0.37). 1.5 is where both criteria
#: meet in both leagues. The equal-weight prior was about half as wide and was scaled by 2 (#1150);
#: at 2 the weighted prior would put NBA 2000's spread at 3.5 per 100 (off_poss >= 500).
#: ``nba_adj_rapm(..., prior_scale=1.0)`` fits the prior as given.
ADJ_RAPM_PRIOR_SCALE: dict[str, float] = {"00": 1.5, "10": 1.5}
ADJ_RAPM_PRIOR_SCALE["20"] = ADJ_RAPM_PRIOR_SCALE["00"]


def _fit_prior_ridge(
    X: csr_matrix,
    y: np.ndarray,
    prior_mean: np.ndarray,
    *,
    alpha: float | None = None,
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
        alpha: The penalty λ; ``None`` picks it by RidgeCV over ``alphas`` (the validation harness).
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

    # λ as given, else by cross-validation with the intercept fitted (unpenalized)
    lam = float(alpha) if alpha is not None else float(RidgeCV(alphas=alphas, fit_intercept=True).fit(X, yprime).alpha_)

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


def _penalty(league: str, alpha: float | None, alphas: np.ndarray | None) -> float | None:
    """The ridge penalty of a fit: ``alpha``, else the league's; ``None`` (RidgeCV) when ``alphas`` is passed."""
    if alphas is None:
        penalty = ADJ_RAPM_PENALTY[league] if alpha is None else alpha
        if not penalty > 0:
            raise ValueError(f"the ridge penalty must be positive, got {penalty!r}")
        return penalty
    warn_deprecated(
        "nba_adj_rapm(alphas=...)",
        replacement="nba_adj_rapm(alpha=...) or the league penalty (league_id=...)",
        since="0.2.0",
        removed_in="0.4.0",
        extra="It picks the penalty per fit, so its scale is not comparable across seasons.",
    )
    return None


def nba_adj_rapm(
    possessions: pl.DataFrame,
    prior: Dict[int, Tuple[float, float]],
    *,
    league_id: str | None = None,
    alpha: float | None = None,
    alphas: np.ndarray | None = None,
    prior_scale: float | None = None,
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

    Penalty and prior scale: one per league, :data:`ADJ_RAPM_PENALTY` and
    :data:`ADJ_RAPM_PRIOR_SCALE` (fitted by ``tools/fit_adj_rapm_penalty.py``; the constants'
    comment carries the evidence), as :func:`~sportsdataverse.nba.nba_rapm.nba_rapm` takes
    :data:`~sportsdataverse.nba.nba_rapm.RAPM_PENALTY`. Before #1150 each fit took RidgeCV's
    per-season pick at the unscaled prior, which collapsed the spread in seven published WNBA
    seasons (2004: sd 0.64 against plain RAPM's 1.47) and held NBA 2025-26 at 0.77 of plain RAPM's.

    Args:
        possessions: A possession+lineup frame produced by the possession engine
            (``game_id``, ``offense_team_id``, ``points``,
            ``off_player_1..5``, ``def_player_1..5``).
        prior: Per-player ``{player_id: (o_prior, d_prior)}`` in per-100 units.
            Players absent from ``prior`` receive a ``(0.0, 0.0)`` default.
        league_id: ``"00"``, ``"10"`` or ``"20"``: the league whose penalty and prior scale are
            used. ``None`` reads it from the ``game_id`` prefix (NBA without one).
        alpha: A penalty instead of the league's (positive; ``ValueError`` otherwise).
        alphas: Deprecated: pick the penalty per fit by RidgeCV over this grid (the old rule,
            whose scale is not comparable across seasons).
        prior_scale: A multiplier on ``prior`` instead of the league's.
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
    league = _possessions_league(possessions, league_id)
    scale = ADJ_RAPM_PRIOR_SCALE[league] if prior_scale is None else prior_scale
    prior_mean: np.ndarray = np.zeros(2 * P, dtype=np.float64)
    for k, pid in enumerate(pids):
        o, d = prior.get(int(pid), (0.0, 0.0))
        prior_mean[k] = scale * o / 100.0
        prior_mean[P + k] = -scale * d / 100.0
    fit = _fit_prior_ridge(
        X, y, prior_mean, alpha=_penalty(league, alpha, alphas),
        alphas=DEFAULT_RAPM_ALPHAS if alphas is None else alphas, n_samples=n_samples, seed=seed,
    )  # fmt: skip
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
