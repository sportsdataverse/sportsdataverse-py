"""Season impact rating: RAPM shrunk toward a box prior, its offense and defense penalties chosen by REML."""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np
import pandas as pd
import polars as pl
from scipy.linalg import cho_factor, cho_solve
from scipy.optimize import minimize
from scipy.sparse import csr_matrix

from sportsdataverse.nba.nba_rapm import build_rapm_design

#: Bounds on each REML penalty (points-per-possession scale, as ``RAPM_PENALTY``). With the v2 box prior the
#: offense penalty reaches the upper bound in 3 of the NBA seasons 1998-2026 (2018, 2019, 2023;
#: ``tools/fit_box_prior_v2.py``): the likelihood is flat there and offense is all prior (weight above
#: 0.999), so a higher bound changes no rating. The lower bound keeps the search finite on tiny inputs.
REML_PENALTY_BOUNDS: tuple[float, float] = (10.0, 1e7)

#: Output columns of :func:`nba_reml_rapm`, in order.
REML_RAPM_SCHEMA: dict[str, type[pl.DataType]] = {
    "player_id": pl.Int64,
    "o_reml_rapm": pl.Float64,
    "d_reml_rapm": pl.Float64,
    "reml_rapm": pl.Float64,
    "o_box_prior": pl.Float64,
    "d_box_prior": pl.Float64,
    "o_prior_weight": pl.Float64,
    "d_prior_weight": pl.Float64,
    "off_poss": pl.Int64,
    "def_poss": pl.Int64,
    "lambda_o": pl.Float64,
    "lambda_d": pl.Float64,
}


@dataclass(frozen=True)
class RemlFit:
    """A prior-informed ridge fit with separate offense / defense penalties.

    Attributes:
        coef: Posterior mean ``(2P,)`` per possession (offense then defense, as the design).
        intercept: Points per possession at the prior (unpenalized).
        lambda_o: Offense penalty (noise variance over the offense prior variance).
        lambda_d: Defense penalty.
        sigma2: Residual variance per possession.
        prior_weight: ``(2P,)`` weight of a player's own prior in his posterior mean, the diagonal of
            ``M^-1 diag(lambda)`` (1 for a player with no possessions, toward 0 as his possessions swamp the
            prior). His teammates' and opponents' priors also reach him through the off-diagonal terms.
        reml: The minimized ``-2`` restricted log likelihood (up to a constant).
        converged: ``False`` when the REML search stopped without converging (a warning is raised too);
            ``True`` for given penalties.

    No posterior standard deviation is reported: ``sigma2 (M^-1)_jj`` is conditional on the chosen
    penalties and an exact prior, ignores the game clustering of possessions, and falls to about 0.04 per
    100 when a penalty reaches its upper bound, so it is not a usable interval.
    """

    coef: np.ndarray
    intercept: float
    lambda_o: float
    lambda_d: float
    sigma2: float
    prior_weight: np.ndarray
    reml: float
    converged: bool


def _centred(X: csr_matrix, y: np.ndarray) -> tuple[np.ndarray, np.ndarray, float, np.ndarray]:
    """``(Xc'Xc, Xc'yc, yc'yc, column means)`` with ``Xc`` / ``yc`` centred, without densifying ``X``."""
    n = X.shape[0]
    mean = np.asarray(X.mean(axis=0), dtype=np.float64).ravel()
    gram = (X.T @ X).toarray() - n * np.outer(mean, mean)
    yc = y - y.mean()
    rhs = np.asarray(X.T @ yc, dtype=np.float64).ravel()
    return gram, rhs, float(yc @ yc), mean


def _reml_solve(
    gram: np.ndarray, rhs: np.ndarray, yy: float, n: int, is_o: np.ndarray, log_lam: np.ndarray
) -> tuple[float, np.ndarray, tuple[np.ndarray, bool], np.ndarray, float]:
    """``(-2 restricted log likelihood, delta, Cholesky of M, lambda per column, RSS)`` at ``log_lam``."""
    P = int(is_o.sum())
    lam = np.where(is_o, np.exp(log_lam[0]), np.exp(log_lam[1]))
    M = gram.copy()
    M[np.diag_indices_from(M)] += lam
    chol = cho_factor(M)
    delta = cho_solve(chol, rhs)
    rss = max(yy - float(rhs @ delta), 1e-12)
    logdet = 2.0 * float(np.log(np.diag(chol[0])).sum())
    obj = (n - 1) * np.log(rss) + logdet - P * (log_lam[0] + log_lam[1])
    return obj, delta, chol, lam, rss


def fit_reml_prior_ridge(
    X: csr_matrix,
    y: np.ndarray,
    prior_mean: np.ndarray,
    *,
    lambdas: tuple[float, float] | None = None,
    bounds: tuple[float, float] = REML_PENALTY_BOUNDS,
) -> RemlFit:
    """Ridge toward ``prior_mean`` with one penalty for offense and one for defense, chosen by REML.

    The model is ``y = c + X b + e``, ``e ~ N(0, s2)``, with each offensive coefficient
    ``b_j ~ N(prior_j, s2 / lambda_o)`` and each defensive one ``~ N(prior_j, s2 / lambda_d)``, and a flat
    prior on the intercept ``c``. The restricted (REML) likelihood integrates ``b`` and ``c`` out; with
    ``s2`` profiled, ``-2`` times it is, up to a constant,

    .. code-block:: text

        (n - 1) log RSS + log |M| - P log lambda_o - P log lambda_d,
        M = Xc'Xc + diag(lambda),  RSS = min_d |yc' - Xc d|^2 + d' diag(lambda) d,

    ``Xc`` / ``yc'`` the centred design and prior-residualized response. Its gradient in
    ``log lambda_o`` is ``lambda_o ((n - 1) |d_o|^2 / RSS + tr_o(M^-1)) - P`` (and likewise for defense),
    so a bounded quasi-Newton search finds the pair in a few dozen ``2P x 2P`` Cholesky factorizations.

    Args:
        X: Sparse design ``(n, 2P)`` from :func:`~sportsdataverse.nba.nba_rapm.build_rapm_design`.
        y: Possession points ``(n,)``.
        prior_mean: Prior mean per possession ``(2P,)`` (offense ``o / 100``, defense ``-d / 100``).
        lambdas: ``(lambda_o, lambda_d)`` to fit at instead of the REML choice.
        bounds: Search bounds on each penalty.

    Returns:
        A :class:`RemlFit`.

    Raises:
        ValueError: If ``X`` has no rows or an odd number of columns, or a given penalty is not positive.

    Example:
        Fit one season toward a prior vector::

            from sportsdataverse.nba.nba_rapm import build_rapm_design
            from sportsdataverse.nba.nba_reml_rapm import fit_reml_prior_ridge
            X, y, pids = build_rapm_design(possessions)
            fit = fit_reml_prior_ridge(X, y, prior_mean)
            print(fit.lambda_o, fit.lambda_d)
    """
    n, two_p = X.shape
    if n == 0 or two_p % 2:
        raise ValueError(f"the design must have rows and offense + defense columns, got shape {X.shape}")
    P = two_p // 2
    mu = np.asarray(prior_mean, dtype=np.float64)
    yprime = np.asarray(y, dtype=np.float64) - X @ mu
    gram, rhs, yy, mean = _centred(X, yprime)
    is_o = np.arange(two_p) < P

    def solve(log_lam: np.ndarray) -> tuple[float, np.ndarray, tuple[np.ndarray, bool], np.ndarray, float]:
        return _reml_solve(gram, rhs, yy, n, is_o, log_lam)

    def objective(log_lam: np.ndarray) -> tuple[float, np.ndarray]:
        obj, delta, chol, lam, rss = solve(log_lam)
        minv_diag = np.diag(cho_solve(chol, np.eye(two_p)))
        g = lam * ((n - 1) * delta**2 / rss + minv_diag)
        return obj, np.array([g[is_o].sum() - P, g[~is_o].sum() - P])

    if lambdas is None:
        lo, hi = np.log(bounds[0]), np.log(bounds[1])
        res = minimize(
            objective,
            x0=np.log([3000.0, 3000.0]),
            jac=True,
            method="L-BFGS-B",
            bounds=[(lo, hi)] * 2,
            options={"ftol": 1e-14, "gtol": 1e-6},
        )
        log_lam = np.asarray(res.x, dtype=np.float64)
        converged = bool(res.success)
        if not converged:
            warnings.warn(f"the REML penalty search did not converge: {res.message}", RuntimeWarning, stacklevel=2)
    else:
        converged = True
        if not min(lambdas) > 0:
            raise ValueError(f"the penalties must be positive, got {lambdas!r}")
        log_lam = np.log(np.asarray(lambdas, dtype=np.float64))
    obj, delta, chol, lam, rss = solve(log_lam)
    minv_diag = np.diag(cho_solve(chol, np.eye(two_p)))
    return RemlFit(
        coef=mu + delta,
        intercept=float(yprime.mean() - mean @ delta),
        lambda_o=float(np.exp(log_lam[0])),
        lambda_d=float(np.exp(log_lam[1])),
        sigma2=float(rss / (n - 1)),
        prior_weight=lam * minv_diag,
        reml=float(obj),
        converged=converged,
    )


def _empty_reml_frame() -> pl.DataFrame:
    return pl.DataFrame({c: pl.Series([], dtype=t) for c, t in REML_RAPM_SCHEMA.items()})


def nba_reml_rapm(
    possessions: pl.DataFrame,
    prior: dict[int, tuple[float, float]],
    *,
    lambdas: tuple[float, float] | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Season impact rating: RAPM shrunk toward a box prior, offense and defense penalties chosen by REML.

    The posterior mean of a prior-informed RAPM (the same design and sign convention as
    :func:`~sportsdataverse.nba.nba_rapm.nba_rapm` and :func:`~sportsdataverse.nba.nba_adj_rapm.nba_adj_rapm`)
    whose two ridge penalties, one for offense and one for defense, are chosen per call by restricted
    maximum likelihood (:func:`fit_reml_prior_ridge`) instead of a frozen league constant. The prior is
    used as given (no scale multiplier): REML's penalty is the ratio of the per-possession noise to how far
    players sit from their prior, so a weak prior earns a small penalty and a sharp one a large one.

    Measured by ``tools/fit_box_prior_v2.py`` on NBA regular seasons 1998-2026 with the v2 box prior, it
    beats today's :func:`~sportsdataverse.nba.nba_adj_rapm.nba_adj_rapm` on held-out games (0.73 bp of
    MSE, SE 0.17, 22 of 29 seasons) and after a mid-season cut date (2006-2026: 1.03 bp, SE 0.49). REML
    puts 90% of the weight on the prior on offense and 81% on defense (possession-weighted; offense reaches
    the upper penalty bound in 2018, 2019 and 2023). The same prior at ``nba_adj_rapm``'s frozen penalty,
    ``lambdas=(ADJ_RAPM_PENALTY["00"],) * 2``, scored better still on held-out games (0.91 bp over the REML
    pick, SE 0.14, 27 of 29 seasons), so REML is the requested rule here, not the best-scoring one.

    Built for the box prior of :mod:`sportsdataverse.nba.nba_box_prior` (``o_prior`` / ``d_prior`` of
    :func:`~sportsdataverse.nba.nba_box_prior.apply_box_prior`), but any per-100 ``(o, d)`` prior works.

    Args:
        possessions: A possession+lineup frame (``points``, ``off_player_1..5``, ``def_player_1..5``).
        prior: ``{player_id: (o_prior, d_prior)}`` per 100 possessions (``d`` positive = good defense).
            A player absent from it gets ``(0, 0)``: a league-average prior.
        lambdas: ``(lambda_o, lambda_d)`` to fit at instead of the REML choice.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per player, columns :data:`REML_RAPM_SCHEMA`: ``o_reml_rapm`` / ``d_reml_rapm`` /
        ``reml_rapm`` (per 100, ``d`` positive = good defense, ``reml_rapm = o + d``), the prior used
        (``o_box_prior`` / ``d_box_prior``: whatever ``prior`` held, the box prior or another), the weight of
        a player's own prior in his posterior mean (``o_prior_weight`` / ``d_prior_weight``, 0 to 1; see
        :class:`RemlFit`), possession counts, and the chosen
        penalties (``lambda_o`` / ``lambda_d``, the same on every row). Zero rows with that schema when no
        possession has a full lineup.

    Example:
        A season rating from the box prior::

            from sportsdataverse.nba import nba_reml_rapm
            prior = {r["player_id"]: (r["o_prior"], r["d_prior"]) for r in priors.iter_rows(named=True)}
            rating = nba_reml_rapm(possessions, prior)
            print(rating.sort("reml_rapm", descending=True).head())
    """
    X, y, pids = build_rapm_design(possessions)
    if not pids:
        out = _empty_reml_frame()
        return out.to_pandas() if return_as_pandas else out
    P = len(pids)
    o_pr = np.array([prior.get(int(p), (0.0, 0.0))[0] for p in pids], dtype=np.float64)
    d_pr = np.array([prior.get(int(p), (0.0, 0.0))[1] for p in pids], dtype=np.float64)
    fit = fit_reml_prior_ridge(X, y, np.concatenate([o_pr, -d_pr]) / 100.0, lambdas=lambdas)
    counts = np.asarray(X.sum(axis=0)).ravel().astype(np.int64)
    o_r, d_r = fit.coef[:P] * 100.0, -fit.coef[P:] * 100.0
    out = pl.DataFrame(
        {
            "player_id": pl.Series(pids, dtype=pl.Int64),
            "o_reml_rapm": o_r,
            "d_reml_rapm": d_r,
            "reml_rapm": o_r + d_r,
            "o_box_prior": o_pr,
            "d_box_prior": d_pr,
            "o_prior_weight": fit.prior_weight[:P],
            "d_prior_weight": fit.prior_weight[P:],
            "off_poss": pl.Series(counts[:P], dtype=pl.Int64),
            "def_poss": pl.Series(counts[P:], dtype=pl.Int64),
            "lambda_o": np.full(P, fit.lambda_o),
            "lambda_d": np.full(P, fit.lambda_d),
        },
        schema=REML_RAPM_SCHEMA,
    )
    return out.to_pandas() if return_as_pandas else out
