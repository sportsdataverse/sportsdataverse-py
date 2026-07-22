"""Odds / market math — pure-function pricing utilities (WS5).

Ported from the reference probability-distribution-engine and cross-book
aggregation layer: odds-format conversion, no-vig margin removal, empirical
PMF/CDF stats from simulated sample vectors, parlay leg combination, pairwise
correlation-matrix assembly, and Gaussian-copula correlated sampling for
pricing correlated player-prop parlays straight off simulated boxscore
distributions.

Everything here is numpy/scipy-only: no network, no storage, no state.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np
from scipy import stats as scipy_stats

__all__ = [
    "american_to_decimal",
    "american_to_prob",
    "build_correlation_matrix",
    "calc_stats",
    "combine_legs",
    "decimal_to_american",
    "fractional_to_american",
    "frozen_dist",
    "market_margin",
    "no_vig_probs",
    "parse_american",
    "prob_over",
    "prob_to_american",
    "sample_using_copula",
]


def parse_american(odds: Union[str, int, float]) -> int:
    """Parse an American odds string ("+150", "-110", "EVEN") to an int.

    Args:
        odds: American odds as str/int/float; "EVEN"/"EV" mean +100.

    Returns:
        Integer American odds.

    Raises:
        ValueError: When the value cannot be parsed.

    Example:
        Quick start::

            from sportsdataverse.odds.odds_math import parse_american
            parse_american("+150")  # 150
            parse_american("-110")  # -110
    """
    if isinstance(odds, str):
        cleaned = odds.strip().upper().replace("−", "-")
        if cleaned in ("EVEN", "EV", "PK"):
            return 100
        try:
            return int(float(cleaned))
        except ValueError as exc:
            raise ValueError(f"Unparseable American odds: {odds!r}") from exc
    value = int(odds)
    if value == 0:
        raise ValueError("American odds cannot be 0")
    return value


def american_to_decimal(odds: Union[str, int, float]) -> float:
    """Convert American odds to European decimal odds.

    Args:
        odds: American odds (any :func:`parse_american` input).

    Returns:
        Decimal odds (payout multiple including stake), e.g. -110 -> 1.909.

    Example:
        Quick start::

            from sportsdataverse.odds.odds_math import american_to_decimal
            american_to_decimal(-110)
    """
    a = parse_american(odds)
    return 1.0 + (a / 100.0 if a > 0 else 100.0 / abs(a))


def decimal_to_american(decimal_odds: float) -> int:
    """Convert decimal odds to American odds.

    Args:
        decimal_odds: Decimal odds > 1.0.

    Returns:
        Integer American odds.

    Raises:
        ValueError: When ``decimal_odds <= 1``.

    Example:
        Quick start::

            from sportsdataverse.odds.odds_math import decimal_to_american
            decimal_to_american(2.5)  # 150
    """
    if decimal_odds <= 1.0:
        raise ValueError(f"Decimal odds must exceed 1.0, got {decimal_odds}")
    profit = decimal_odds - 1.0
    return round(profit * 100) if profit >= 1.0 else round(-100.0 / profit)


def fractional_to_american(fractional: str) -> int:
    """Convert fractional odds ("3/2", "1/4") to American odds.

    Args:
        fractional: Fraction string ``numerator/denominator``.

    Returns:
        Integer American odds.

    Raises:
        ValueError: On a malformed fraction.

    Example:
        Quick start::

            from sportsdataverse.odds.odds_math import fractional_to_american
            fractional_to_american("3/2")  # 150
    """
    try:
        num_str, den_str = fractional.strip().split("/")
        num, den = float(num_str), float(den_str)
    except ValueError as exc:
        raise ValueError(f"Unparseable fractional odds: {fractional!r}") from exc
    if den <= 0 or num <= 0:
        raise ValueError(f"Fractional odds must be positive: {fractional!r}")
    return decimal_to_american(1.0 + num / den)


def american_to_prob(odds: Union[str, int, float]) -> float:
    """Implied probability (vig included) of American odds.

    Args:
        odds: American odds (any :func:`parse_american` input).

    Returns:
        Implied probability in (0, 1).

    Example:
        Quick start::

            from sportsdataverse.odds.odds_math import american_to_prob
            american_to_prob(-110)  # 0.5238...
    """
    a = parse_american(odds)
    return 100.0 / (a + 100.0) if a > 0 else abs(a) / (abs(a) + 100.0)


def prob_to_american(prob: float) -> int:
    """Fair American odds for a probability (no margin added).

    Args:
        prob: Probability in (0, 1).

    Returns:
        Integer American odds.

    Raises:
        ValueError: When ``prob`` is outside (0, 1).

    Example:
        Quick start::

            from sportsdataverse.odds.odds_math import prob_to_american
            prob_to_american(0.6)  # -150
    """
    if not 0.0 < prob < 1.0:
        raise ValueError(f"Probability must be in (0, 1), got {prob}")
    return decimal_to_american(1.0 / prob)


def market_margin(odds: Iterable[Union[str, int, float]]) -> float:
    """Bookmaker margin (overround) of a full market.

    Args:
        odds: American odds for every mutually exclusive outcome.

    Returns:
        The margin: sum of implied probabilities minus 1 (0 = fair market).

    Example:
        Two-way -110/-110::

            from sportsdataverse.odds.odds_math import market_margin
            market_margin([-110, -110])  # ~0.0476
    """
    return float(sum(american_to_prob(o) for o in odds) - 1.0)


def no_vig_probs(odds: Iterable[Union[str, int, float]]) -> List[float]:
    """Remove the vig from a market: proportional margin removal.

    Args:
        odds: American odds for every mutually exclusive outcome.

    Returns:
        Fair probabilities summing to 1.0.

    Example:
        Quick start::

            from sportsdataverse.odds.odds_math import no_vig_probs
            no_vig_probs([-110, -110])  # [0.5, 0.5]
    """
    implied = [american_to_prob(o) for o in odds]
    total = sum(implied)
    return [p / total for p in implied]


def calc_stats(
    raw_values: np.ndarray,
    stats: Sequence[str] = ("pdf", "cdf", "mean", "median"),
) -> Dict[str, Union[float, Dict[float, float]]]:
    """Empirical PMF / CDF / mean / median from a simulated sample vector.

    The reference projection-from-PMF recipe: the sample vector is typically one
    player-stat column across n_sim simulated games; ``pdf`` prices exact
    outcomes, ``cdf`` prices unders, mean/median are the projections.

    Args:
        raw_values: Sample vector (e.g. simulated points across sims).
        stats: Which of ``pdf`` / ``cdf`` / ``mean`` / ``median`` to compute.

    Returns:
        Dict keyed by requested stat; ``pdf`` / ``cdf`` are value->prob dicts.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.odds.odds_math import calc_stats
            s = calc_stats(np.array([10, 12, 12, 15]), stats=("pdf", "mean"))
            s["pdf"][12.0]  # 0.5
    """
    values = np.asarray(raw_values, dtype=float)
    out: Dict[str, Union[float, Dict[float, float]]] = {}
    if values.size == 0:
        return {s: ({} if s in ("pdf", "cdf") else float("nan")) for s in stats}
    uniques, counts = np.unique(values, return_counts=True)
    pmf = counts / counts.sum()
    for stat in stats:
        if stat == "pdf":
            out["pdf"] = {float(v): float(p) for v, p in zip(uniques, pmf)}
        elif stat == "cdf":
            cumulative: np.ndarray = np.cumsum(pmf)
            out["cdf"] = {float(v): float(c) for v, c in zip(uniques, cumulative)}
        elif stat == "mean":
            out["mean"] = float(values.mean())
        elif stat == "median":
            out["median"] = float(np.median(values))
        else:
            raise ValueError(f"Unknown stat {stat!r}")
    return out


def prob_over(raw_values: np.ndarray, line: float) -> float:
    """P(stat > line) read straight off a simulated sample vector.

    Args:
        raw_values: Sample vector across sims.
        line: The market line (use the .5 convention for integer stats).

    Returns:
        Empirical exceedance probability.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.odds.odds_math import prob_over
            prob_over(np.array([10, 12, 15, 20]), 12.5)  # 0.5
    """
    values = np.asarray(raw_values, dtype=float)
    if values.size == 0:
        return float("nan")
    return float((values > line).mean())


def combine_legs(legs: Sequence[np.ndarray], operand: str = "+") -> np.ndarray:
    """Elementwise combine per-leg sample vectors into one derived vector.

    Same-index elements come from the SAME simulated world, so combining
    elementwise preserves whatever correlation the simulation induced
    (e.g. points + rebounds + assists for a PRA prop).

    Args:
        legs: Equal-length sample vectors, one per leg.
        operand: ``"+"`` | ``"-"`` | ``"*"``.

    Returns:
        The combined sample vector (feed to :func:`calc_stats` /
        :func:`prob_over` to price the derived market).

    Raises:
        ValueError: On no legs, mismatched lengths, or an unknown operand.

    Example:
        PRA prop::

            import numpy as np
            from sportsdataverse.odds.odds_math import combine_legs, prob_over
            pra = combine_legs([pts, reb, ast])
            prob_over(pra, 39.5)
    """
    if not legs:
        raise ValueError("combine_legs needs at least one leg")
    arrays = [np.asarray(leg, dtype=float) for leg in legs]
    length = arrays[0].size
    if any(a.size != length for a in arrays):
        raise ValueError("All legs must have the same sample length")
    ops = {"+": np.add, "-": np.subtract, "*": np.multiply}
    if operand not in ops:
        raise ValueError(f"Unknown operand {operand!r}; valid: {sorted(ops)}")
    out = arrays[0].copy()
    for arr in arrays[1:]:
        out = ops[operand](out, arr)
    return out


def build_correlation_matrix(
    n: int,
    pairwise: Dict[Tuple[int, int], float],
) -> np.ndarray:
    """Assemble a symmetric correlation matrix from sparse pairwise entries.

    Args:
        n: Number of variables.
        pairwise: ``{(i, j): rho}`` entries (order-insensitive); unspecified
            off-diagonal pairs default to 0.

    Returns:
        An (n, n) correlation matrix with unit diagonal.

    Raises:
        ValueError: On an out-of-range index or |rho| > 1.

    Example:
        Quick start::

            from sportsdataverse.odds.odds_math import build_correlation_matrix
            build_correlation_matrix(3, {(0, 1): 0.35})
    """
    corr = np.eye(n)
    for (i, j), rho in pairwise.items():
        if not (0 <= i < n and 0 <= j < n) or i == j:
            raise ValueError(f"Bad pair index ({i}, {j}) for n={n}")
        if abs(rho) > 1.0:
            raise ValueError(f"Correlation out of range for ({i}, {j}): {rho}")
        corr[i, j] = corr[j, i] = rho
    return corr


def frozen_dist(family: str, **params: float) -> Any:
    """Build a frozen scipy distribution by family name.

    Args:
        family: ``poisson`` | ``norm`` | ``gamma`` | ``bernoulli`` |
            ``nbinom`` (scipy parameterizations).
        **params: Passed straight to the scipy family constructor.

    Returns:
        The frozen distribution (has ``ppf`` / ``cdf`` / ``rvs``).

    Raises:
        ValueError: On an unknown family.

    Example:
        Quick start::

            from sportsdataverse.odds.odds_math import frozen_dist
            frozen_dist("poisson", mu=5.5).ppf(0.5)
    """
    families = {
        "poisson": scipy_stats.poisson,
        "norm": scipy_stats.norm,
        "gamma": scipy_stats.gamma,
        "bernoulli": scipy_stats.bernoulli,
        "nbinom": scipy_stats.nbinom,
    }
    if family not in families:
        raise ValueError(f"Unknown distribution family {family!r}; valid: {sorted(families)}")
    return families[family](**params)


def sample_using_copula(
    corr: np.ndarray,
    dists: Sequence[Any],
    n: int,
    *,
    rng: Optional[np.random.Generator] = None,
) -> np.ndarray:
    """Gaussian-copula correlated sampling across arbitrary marginals.

    Draw multivariate normal with the target correlation, map each margin
    through the normal CDF to uniforms, then through each marginal's ``ppf``.
    The reference correlated-parlay pricer: correlated player-stat draws whose
    marginals match each player's projected distribution.

    Args:
        corr: (k, k) correlation matrix (see :func:`build_correlation_matrix`).
        dists: k frozen distributions (``ppf`` required), one per variable.
        n: Number of joint samples.
        rng: Optional numpy Generator for reproducibility.

    Returns:
        An (n, k) sample matrix; column j follows ``dists[j]``.

    Raises:
        ValueError: When ``corr`` shape disagrees with ``len(dists)``.

    Example:
        Correlated two-leg parlay::

            import numpy as np
            from sportsdataverse.odds.odds_math import (
                build_correlation_matrix, frozen_dist, sample_using_copula,
            )
            corr = build_correlation_matrix(2, {(0, 1): 0.4})
            draws = sample_using_copula(
                corr, [frozen_dist("poisson", mu=22.0), frozen_dist("poisson", mu=8.5)],
                n=10_000, rng=np.random.default_rng(7),
            )
            both = ((draws[:, 0] > 24.5) & (draws[:, 1] > 9.5)).mean()
    """
    corr = np.asarray(corr, dtype=float)
    k = len(dists)
    if corr.shape != (k, k):
        raise ValueError(f"corr shape {corr.shape} does not match {k} marginals")
    rng = rng or np.random.default_rng()
    normals = rng.multivariate_normal(np.zeros(k), corr, size=n)
    uniforms = scipy_stats.norm.cdf(normals)
    # clip away exact 0/1 so discrete ppf never returns -1/inf
    uniforms = np.clip(uniforms, 1e-12, 1.0 - 1e-12)
    columns = [np.asarray(dists[j].ppf(uniforms[:, j]), dtype=float) for j in range(k)]
    return np.column_stack(columns)
