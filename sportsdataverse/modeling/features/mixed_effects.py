"""Random-intercept mixed-effects fitting — numpy-only, no new deps.

The reference model stack leaned on mixed-effects for entity-level partial
pooling (player rates shrunk toward the population). This module ships the
workhorse case — the one-way random-intercept model

.. math:: y_{ij} = \\mu + b_i + e_{ij},\\quad b_i \\sim N(0, \\tau^2),\\ e_{ij} \\sim N(0, \\sigma^2)

— fitted by closed-form EM, so it needs nothing beyond numpy/polars. Group
effects come back as BLUPs (posterior means), which for unbalanced panels
are exactly the usual shrinkage estimator

.. math:: \\hat b_i = \\frac{n_i \\tau^2}{\\sigma^2 + n_i \\tau^2} (\\bar y_i - \\mu).

For richer structures (crossed effects, slopes) install ``statsmodels`` and
use ``MixedLM`` — the optional parity test pins this implementation to it.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict

import numpy as np
import polars as pl


@dataclasses.dataclass
class RandomInterceptFit:
    """A fitted one-way random-intercept model.

    Attributes:
        mu: Grand mean (fixed intercept).
        tau2: Between-group variance component.
        sigma2: Within-group (residual) variance component.
        effects: ``{group: BLUP}`` posterior-mean group effects.
        counts: ``{group: n_obs}`` observation counts.
        n_iter: EM iterations run.
        converged: Whether the parameter change fell below tolerance.
    """

    mu: float
    tau2: float
    sigma2: float
    effects: Dict[Any, float]
    counts: Dict[Any, int]
    n_iter: int
    converged: bool

    def predict(self, group: Any) -> float:
        """Posterior-mean prediction for a group (``mu`` when unseen)."""
        return self.mu + self.effects.get(group, 0.0)

    def shrinkage(self, group: Any) -> float:
        """The pooling weight ``n tau^2 / (sigma^2 + n tau^2)`` for a group
        (0 = fully pooled to the grand mean; 1 = the raw group mean)."""
        n = self.counts.get(group, 0)
        denominator = self.sigma2 + n * self.tau2
        return float(n * self.tau2 / denominator) if denominator > 0 else 0.0


def fit_random_intercepts(
    df: pl.DataFrame,
    *,
    response: str,
    group: str,
    max_iter: int = 5000,
    tol: float = 1e-8,
) -> RandomInterceptFit:
    """Fit ``response ~ 1 + (1 | group)`` by EM.

    Args:
        df: Long frame with one row per observation.
        response: Numeric response column.
        group: Grouping column (players, teams, ...).
        max_iter: EM iteration cap (variance-component EM converges
            linearly — thousands of the O(n) iterations are normal and cheap).
        tol: Convergence tolerance on the parameter vector.

    Returns:
        The :class:`RandomInterceptFit`.

    Raises:
        ValueError: On an empty frame or all-null response.

    Example:
        Shrink player scoring toward the population::

            from sportsdataverse.modeling.features.mixed_effects import fit_random_intercepts
            fit = fit_random_intercepts(logs, response="pts", group="player_id")
            fit.predict(some_player_id), fit.shrinkage(some_player_id)
    """
    data = df.select(pl.col(group).alias("g"), pl.col(response).cast(pl.Float64).alias("y")).drop_nulls()
    if data.height == 0:
        raise ValueError("fit_random_intercepts needs at least one non-null observation")
    y = data["y"].to_numpy().astype(float)
    groups = data["g"].to_list()
    labels = sorted(set(groups), key=str)
    index = {label: i for i, label in enumerate(labels)}
    gi = np.array([index[g] for g in groups], dtype=int)
    m = len(labels)
    counts: np.ndarray = np.bincount(gi, minlength=m).astype(float)
    sums = np.bincount(gi, weights=y, minlength=m)
    means = sums / counts

    mu = float(y.mean())
    tau2 = max(float(np.var(means)), 1e-8)
    sigma2 = max(float(np.var(y)), 1e-8)
    n_iter = 0
    converged = False
    for n_iter in range(1, max_iter + 1):
        gain = counts * tau2 / (sigma2 + counts * tau2)
        b = gain * (means - mu)
        b_var = tau2 * sigma2 / (sigma2 + counts * tau2)
        mu_new = float((y - b[gi]).mean())
        tau2_new = float(np.mean(b**2 + b_var))
        resid = y - mu_new - b[gi]
        sigma2_new = float(np.mean(resid**2 + b_var[gi]))
        delta = max(abs(mu_new - mu), abs(tau2_new - tau2), abs(sigma2_new - sigma2))
        mu, tau2, sigma2 = mu_new, max(tau2_new, 1e-12), max(sigma2_new, 1e-12)
        if delta < tol:
            converged = True
            break
    gain = counts * tau2 / (sigma2 + counts * tau2)
    b = gain * (means - mu)
    return RandomInterceptFit(
        mu=mu,
        tau2=tau2,
        sigma2=sigma2,
        effects={label: float(b[index[label]]) for label in labels},
        counts={label: int(counts[index[label]]) for label in labels},
        n_iter=n_iter,
        converged=converged,
    )


@dataclasses.dataclass
class PooledInterceptFit:
    """A fitted two-factor random-intercept model (pool + group).

    The cross-league pooling structure: ``pool`` absorbs level differences
    (league scoring environments), ``group`` carries one GLOBAL entity
    effect informed by that entity's observations across every pool — the
    thin-sample player borrows strength from all their samples while the
    pool effect keeps leagues comparable.

    Attributes:
        mu: Grand mean (fixed intercept).
        tau2_pool: Between-pool variance component.
        tau2_group: Between-group variance component.
        sigma2: Residual variance component.
        pool_effects: ``{pool: BLUP}`` pool effects.
        group_effects: ``{group: BLUP}`` global group effects.
        pool_counts: ``{pool: n_obs}``.
        group_counts: ``{group: n_obs}``.
        n_iter: EM iterations run.
        converged: Whether the parameter change fell below tolerance.
    """

    mu: float
    tau2_pool: float
    tau2_group: float
    sigma2: float
    pool_effects: Dict[Any, float]
    group_effects: Dict[Any, float]
    pool_counts: Dict[Any, int]
    group_counts: Dict[Any, int]
    n_iter: int
    converged: bool

    def predict(self, pool: Any, group: Any) -> float:
        """Posterior-mean prediction for a (pool, group) pair.

        Unseen pools/groups contribute nothing — an entirely unseen pair
        predicts the grand mean.
        """
        return self.mu + self.pool_effects.get(pool, 0.0) + self.group_effects.get(group, 0.0)

    def shrinkage(self, group: Any) -> float:
        """The group pooling weight ``n tau2 / (sigma2 + n tau2)``
        (0 = fully pooled to its pool line; 1 = the raw residual mean)."""
        n = self.group_counts.get(group, 0)
        denominator = self.sigma2 + n * self.tau2_group
        return float(n * self.tau2_group / denominator) if denominator > 0 else 0.0


def fit_pooled_intercepts(
    df: pl.DataFrame,
    *,
    response: str,
    group: str,
    pool: str,
    max_iter: int = 50_000,
    tol: float = 1e-8,
) -> PooledInterceptFit:
    """Fit ``response ~ 1 + (1 | pool) + (1 | group)`` by backfitting EM.

    The two factors may be nested (each group appears in one pool — the
    disjoint-league case) or crossed (a group observed in several pools —
    call-ups appearing in two leagues); the same BLUP updates cover both.
    With a single pool level this reduces to
    :func:`fit_random_intercepts` (the reduction is a test gate, which
    transitively anchors this fit to the statsmodels-pinned one-way EM).

    Args:
        df: Long frame with one row per observation.
        response: Numeric response column.
        group: Entity column pooled globally (players, teams, ...).
        pool: Level column absorbing environment differences (league).
        max_iter: EM iteration cap — higher than the one-way default
            because a pool variance estimated from a handful of league
            levels converges slowly (tens of thousands of the O(n)
            iterations still run in well under a second).
        tol: Convergence tolerance on the stored state (parameters AND
            effect vectors) across iterations.

    Returns:
        The :class:`PooledInterceptFit`.

    Raises:
        ValueError: On an empty frame, all-null response, or
            ``pool == group`` (the factors would be confounded).

    Example:
        Pool player scoring across leagues::

            from sportsdataverse.modeling.features.mixed_effects import (
                fit_pooled_intercepts,
            )
            fit = fit_pooled_intercepts(
                rows, response="pts", group="player_key", pool="league",
            )
            fit.predict("nba", some_player), fit.shrinkage(some_player)
    """
    if pool == group:
        raise ValueError("pool and group must be different columns")
    data = df.select(
        pl.col(pool).alias("p"),
        pl.col(group).alias("g"),
        pl.col(response).cast(pl.Float64).alias("y"),
    ).drop_nulls()
    if data.height == 0:
        raise ValueError("fit_pooled_intercepts needs at least one non-null observation")
    y = data["y"].to_numpy().astype(float)
    pool_labels = sorted(set(data["p"].to_list()), key=str)
    group_labels = sorted(set(data["g"].to_list()), key=str)
    p_index = {label: i for i, label in enumerate(pool_labels)}
    g_index = {label: i for i, label in enumerate(group_labels)}
    pi = np.array([p_index[v] for v in data["p"].to_list()], dtype=int)
    gi = np.array([g_index[v] for v in data["g"].to_list()], dtype=int)
    n_pools, n_groups = len(pool_labels), len(group_labels)
    counts_p: np.ndarray = np.bincount(pi, minlength=n_pools).astype(float)
    counts_g: np.ndarray = np.bincount(gi, minlength=n_groups).astype(float)

    mu = float(y.mean())
    pool_means = np.bincount(pi, weights=y, minlength=n_pools) / counts_p
    a = pool_means - mu
    b = np.zeros(n_groups)
    tau2_a = max(float(np.var(pool_means)), 1e-8)
    group_means = np.bincount(gi, weights=y - mu - a[pi], minlength=n_groups) / counts_g
    tau2_b = max(float(np.var(group_means)), 1e-8)
    sigma2 = max(float(np.var(y)), 1e-8)

    n_iter = 0
    converged = False
    for n_iter in range(1, max_iter + 1):
        mu0, tau2_a0, tau2_b0, sigma2_0 = mu, tau2_a, tau2_b, sigma2
        a0, b0 = a.copy(), b.copy()
        gain_a = counts_p * tau2_a / (sigma2 + counts_p * tau2_a)
        a = gain_a * (np.bincount(pi, weights=y - mu - b[gi], minlength=n_pools) / counts_p)
        var_a = tau2_a * sigma2 / (sigma2 + counts_p * tau2_a)
        # gauge fix: sweep the pool effects' observation-weighted mean into
        # mu BEFORE the group update. Predictions (mu + a + b) are invariant;
        # without it the location split between mu and a few pool levels
        # mixes glacially (the 3-league fit hit the iteration cap). In the
        # single-pool case the shift vanishes at the fixed point (sharing
        # the one-way fixed point); with several pools it settles to a
        # CONSTANT — which is why convergence below is measured on the
        # stored state across iterations, not on within-iteration moves.
        shift = float(counts_p @ a / len(y))
        a -= shift
        mu += shift
        gain_b = counts_g * tau2_b / (sigma2 + counts_g * tau2_b)
        b = gain_b * (np.bincount(gi, weights=y - mu - a[pi], minlength=n_groups) / counts_g)
        var_b = tau2_b * sigma2 / (sigma2 + counts_g * tau2_b)
        mu_new = float((y - a[pi] - b[gi]).mean())
        resid = y - mu_new - a[pi] - b[gi]
        mu = mu_new
        tau2_a = max(float(np.mean(a**2 + var_a)), 1e-12)
        tau2_b = max(float(np.mean(b**2 + var_b)), 1e-12)
        sigma2 = max(float(np.mean(resid**2 + var_a[pi] + var_b[gi])), 1e-12)
        delta = max(
            abs(mu - mu0),
            abs(tau2_a - tau2_a0),
            abs(tau2_b - tau2_b0),
            abs(sigma2 - sigma2_0),
            float(np.max(np.abs(a - a0))),
            float(np.max(np.abs(b - b0))),
        )
        if delta < tol:
            converged = True
            break
    gain_a = counts_p * tau2_a / (sigma2 + counts_p * tau2_a)
    a = gain_a * (np.bincount(pi, weights=y - mu - b[gi], minlength=n_pools) / counts_p)
    gain_b = counts_g * tau2_b / (sigma2 + counts_g * tau2_b)
    b = gain_b * (np.bincount(gi, weights=y - mu - a[pi], minlength=n_groups) / counts_g)
    return PooledInterceptFit(
        mu=mu,
        tau2_pool=tau2_a,
        tau2_group=tau2_b,
        sigma2=sigma2,
        pool_effects={label: float(a[p_index[label]]) for label in pool_labels},
        group_effects={label: float(b[g_index[label]]) for label in group_labels},
        pool_counts={label: int(counts_p[p_index[label]]) for label in pool_labels},
        group_counts={label: int(counts_g[g_index[label]]) for label in group_labels},
        n_iter=n_iter,
        converged=converged,
    )


def shrunk_group_means(
    df: pl.DataFrame,
    *,
    response: str,
    group: str,
) -> pl.DataFrame:
    """Per-group raw vs partially-pooled means from the fitted model.

    Args:
        df: Long observation frame.
        response: Numeric response column.
        group: Grouping column.

    Returns:
        One row per group: ``group``, ``n``, ``raw_mean``, ``shrunk_mean``,
        ``shrinkage`` (pooling weight toward the raw mean).

    Example:
        Quick start::

            from sportsdataverse.modeling.features.mixed_effects import shrunk_group_means
            table = shrunk_group_means(logs, response="pts", group="player_id")
    """
    fit = fit_random_intercepts(df, response=response, group=group)
    raw = (
        df.select(pl.col(group), pl.col(response).cast(pl.Float64))
        .drop_nulls()
        .group_by(group, maintain_order=True)
        .agg(pl.len().alias("n"), pl.col(response).mean().alias("raw_mean"))
    )
    rows = [
        {
            group: label,
            "n": int(n),
            "raw_mean": float(mean),
            "shrunk_mean": fit.predict(label),
            "shrinkage": fit.shrinkage(label),
        }
        for label, n, mean in raw.iter_rows()
    ]
    return pl.DataFrame(rows)
