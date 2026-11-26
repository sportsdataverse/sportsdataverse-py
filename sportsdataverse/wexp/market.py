"""Market-price utilities for the win-expectancy bake-off harness.

Conversions from prices to implied probabilities, vig removal
(multiplicative default, Shin as the sensitivity check), the spread->WP
normal-CDF mapping, and the 70/30 logit-space spread/moneyline
blend. Conventions:

- ``spread`` is the expected HOME margin: positive = home favored
  (matches nflverse ``spread_line`` and the cfb_line_odds consensus).
- All probabilities are HOME win probabilities unless noted.
"""

from __future__ import annotations

import math
import warnings
from collections.abc import Sequence

from scipy.optimize import brentq
from scipy.stats import norm

__all__ = [
    "devig_multiplicative",
    "devig_shin",
    "fair_win_probs",
    "logit_blend",
    "moneyline_pair_prob",
    "prob_from_american",
    "prob_from_decimal",
    "spread_to_prob",
]


def prob_from_american(price: float) -> float:
    """Convert an American moneyline price to its raw implied probability.

    Args:
        price: American odds (e.g. ``-110`` or ``+150``). Must be nonzero.

    Returns:
        The implied probability including vig (does not sum to 1 across sides).

    Raises:
        ValueError: If ``price`` is 0.

    Example:
        Quick start::

            from sportsdataverse.wexp.market import prob_from_american
            prob_from_american(-110)  # 0.5238...
    """
    if price == 0:
        raise ValueError("American price cannot be 0")
    p = float(price)
    return -p / (-p + 100) if p < 0 else 100 / (p + 100)


def prob_from_decimal(price: float) -> float:
    """Convert a decimal (European) price to its raw implied probability.

    Args:
        price: Decimal odds, > 1 (e.g. ``1.91``).

    Returns:
        The implied probability including vig.

    Raises:
        ValueError: If ``price`` is not greater than 1.

    Example:
        Quick start::

            from sportsdataverse.wexp.market import prob_from_decimal
            prob_from_decimal(2.0)  # 0.5
    """
    if price <= 1:
        raise ValueError("Decimal price must be > 1")
    return 1.0 / float(price)


def devig_multiplicative(p_raw: Sequence[float]) -> list[float]:
    """Remove vig by normalizing raw implied probabilities to sum to 1.

    The basic multiplicative method — as good as or better than fancier
    methods at the close on log probability score (Matter of Stats study).

    Args:
        p_raw: Raw implied probabilities for all outcomes (with vig).

    Returns:
        Probabilities scaled to sum to 1, order preserved.

    Example:
        Quick start::

            from sportsdataverse.wexp.market import devig_multiplicative
            devig_multiplicative([0.5238, 0.5238])  # [0.5, 0.5]
    """
    total = sum(p_raw)
    return [p / total for p in p_raw]


def devig_shin(p_raw: Sequence[float]) -> list[float]:
    """Remove vig with Shin's method (insider-trading model).

    Attributes the overround disproportionately to longshots; kept as a
    sensitivity check for CFB big-dog moneylines. With zero overround this
    reduces to the identity.

    Args:
        p_raw: Raw implied probabilities for all outcomes (with vig).

    Returns:
        Shin-devigged probabilities summing to 1, order preserved.

    Example:
        Sensitivity check vs the multiplicative default::

            from sportsdataverse.wexp.market import devig_shin
            devig_shin([0.85, 0.25])
    """
    booksum = sum(p_raw)
    if booksum <= 1.0:
        return devig_multiplicative(p_raw)

    def _shin_probs(z: float) -> list[float]:
        # Shin (1993): p_i = (sqrt(z^2 + 4(1-z) pi_i^2 / booksum) - z) / (2(1-z))
        return [(math.sqrt(z * z + 4 * (1 - z) * (p * p) / booksum) - z) / (2 * (1 - z)) for p in p_raw]

    def _excess(z: float) -> float:
        return sum(_shin_probs(z)) - 1.0

    # At z=0 the probs sum to sqrt(booksum) > 1; as z -> 1 they sum to
    # sum(pi^2)/booksum < 1, so the root is bracketed on (0, 1).
    try:
        z_star = float(brentq(_excess, 0.0, 1.0 - 1e-9, xtol=1e-12))
    except ValueError:
        # provenance matters: a harness sweep must be able to see that the
        # "Shin" variant actually degraded to multiplicative on this input.
        warnings.warn(
            f"Shin solver failed to bracket (booksum={booksum:.4f}); falling back to multiplicative devig",
            stacklevel=2,
        )
        return devig_multiplicative(p_raw)
    probs = _shin_probs(z_star)
    total = sum(probs)
    return [p / total for p in probs]


def spread_to_prob(spread: float, sigma: float) -> float:
    """Map an expected home margin to a home win probability via a normal CDF.

    Args:
        spread: Expected HOME margin (positive = home favored).
        sigma: Margin standard deviation (league-specific, tuned on the
            harness — e.g. ~13.45 NFL).

    Returns:
        P(home win) = Phi(spread / sigma).

    Example:
        Quick start::

            from sportsdataverse.wexp.market import spread_to_prob
            spread_to_prob(7.0, sigma=13.45)
    """
    return float(norm.cdf(spread / sigma))


def logit_blend(p_a: float, p_b: float, weight_a: float = 0.7) -> float:
    """Blend two probabilities in logit space (70/30 by default).

    Args:
        p_a: First probability in (0, 1).
        p_b: Second probability in (0, 1).
        weight_a: Weight on ``p_a``; ``p_b`` gets ``1 - weight_a``.

    Returns:
        The logit-space weighted blend, back on the probability scale.

    Example:
        Spread/moneyline blend at 70/30::

            from sportsdataverse.wexp.market import logit_blend
            logit_blend(0.61, 0.64, weight_a=0.7)
    """
    la = math.log(p_a / (1 - p_a))
    lb = math.log(p_b / (1 - p_b))
    lz = weight_a * la + (1 - weight_a) * lb
    return 1 / (1 + math.exp(-lz))


def moneyline_pair_prob(home_price: float, away_price: float, method: str = "multiplicative") -> float:
    """Vig-removed home win probability from a two-way American moneyline pair.

    Args:
        home_price: American price on the home side.
        away_price: American price on the away side.
        method: ``"multiplicative"`` (default) or ``"shin"``.

    Returns:
        The devigged home win probability.

    Raises:
        ValueError: If ``method`` is not recognized.

    Example:
        Quick start::

            from sportsdataverse.wexp.market import moneyline_pair_prob
            moneyline_pair_prob(-150, 130)
    """
    raw = [prob_from_american(home_price), prob_from_american(away_price)]
    if method == "multiplicative":
        return devig_multiplicative(raw)[0]
    if method == "shin":
        return devig_shin(raw)[0]
    raise ValueError(f"unknown devig method: {method!r}")


def fair_win_probs(
    home: float | None,
    away: float | None,
    draw: float | None = None,
    *,
    draw_possible: bool = False,
    method: str = "multiplicative",
) -> tuple[list[float] | None, str | None]:
    """Vig-removed win probabilities from raw implied ones, or ``None`` and the reason.

    The guard is the point: where a game can end level and no draw price is
    posted, the home and away prices alone do not cover every outcome, and
    normalizing them to 1 would push the draw's share onto both teams. That
    returns ``None`` with a reason instead of a biased pair.

    Args:
        home: raw implied home win probability (a price already in 0-1, from
            :func:`prob_from_american`, :func:`prob_from_decimal` or a
            prediction-market midpoint).
        away: raw implied away win probability.
        draw: raw implied draw probability, when one is posted.
        draw_possible: the league's games can end level (soccer).
        method: ``"multiplicative"`` (default) or ``"shin"``.

    Returns:
        ``([home, away], None)``, or ``([home, away, draw], None)`` when a draw
        price is given; ``(None, reason)`` when a needed price is missing or not
        a probability.

    Raises:
        ValueError: If ``method`` is not recognized.

    Example:
        Quick start::

            from sportsdataverse.wexp.market import fair_win_probs
            fair_win_probs(0.55, 0.50)                    # ([0.5238, 0.4762], None)
            fair_win_probs(0.45, 0.30, draw_possible=True)  # (None, "draw is possible ...")
    """
    if method not in ("multiplicative", "shin"):
        raise ValueError(f"unknown devig method: {method!r}")
    if home is None and away is None:
        return None, "no home or away price"
    if home is None:
        return None, "missing home price"
    if away is None:
        return None, "missing away price"
    if draw_possible and draw is None:
        return None, "draw is possible but no draw price is posted; a two-way de-vig would be biased"
    raw = [home, away] if draw is None else [home, away, draw]
    if any(not (0 < p < 1) for p in raw):
        return None, "a price is not a probability between 0 and 1"
    return (devig_multiplicative(raw) if method == "multiplicative" else devig_shin(raw)), None
