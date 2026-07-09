"""Play-type / impact spine (T3.5) -- shared constants + validation metrics (league-agnostic).

Single import home for all four models (``nba_playtype``, ``nba_matchup_drapm``,
``nba_foul_drawing``, ``nba_expected_turnovers``): the canonical Synergy play-type
list, the league_id lookup, the truly-fixed config knobs, and the shared metric
helpers used by every oracle gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from scipy.stats import rankdata

from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS

#: Synergy's canonical 11 play types (``play_type_nullable`` values), pinned from a
#: live ``nba_stats_synergyplaytypes`` capture (2023-24 season) -- note ``PRRollMan``
#: (capital M) matches the API's returned ``play_type`` column exactly.
SYNERGY_PLAY_TYPES: list[str] = [
    "Transition",
    "Isolation",
    "PRBallHandler",
    "PRRollMan",
    "Postup",
    "Spotup",
    "Handoff",
    "Cut",
    "OffScreen",
    "OffRebound",
    "Misc",
]

#: League slug -> stats-API ``league_id`` string.
LEAGUE_ID_MAP: dict[str, str] = {"nba": "00", "wnba": "10", "gleague": "20"}


@dataclass(frozen=True)
class PlaytypeConfig:
    """Physics-fixed constants for the play-type/impact spine (NOT league-fitted).

    League baselines (mean PPP by type, FT/TO rates by type) are computed from
    the fetched season data itself -- only truly-fixed knobs live here.

    Attributes:
        ft_points_per_trip: League-invariant expected points per free-throw trip.
        min_matchup_poss: DRAPM inclusion floor on partial matchup possessions.
        ridge_alphas: Ridge penalty grid for :class:`~sklearn.linear_model.RidgeCV`,
            reusing the shipped :data:`~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS`.
    """

    ft_points_per_trip: float = 1.53
    min_matchup_poss: float = 25.0
    ridge_alphas: np.ndarray = field(default_factory=lambda: np.asarray(DEFAULT_RAPM_ALPHAS))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two equal-length arrays.

    Args:
        a: First array.
        b: Second array, same length as *a*.

    Returns:
        The Pearson correlation of the rank-transformed arrays.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_playtype_constants import spearman_corr
            spearman_corr(np.array([1, 2, 3]), np.array([10, 20, 30]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two equal-length arrays.

    Args:
        a: First array.
        b: Second array, same length as *a*.

    Returns:
        ``mean(abs(a - b))``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_playtype_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def sum_consistency_residual(parts: np.ndarray, whole: np.ndarray) -> float:
    """Max absolute gap between per-entity component sums and the aggregate.

    Used by models ①③④ to prove a per-component decomposition reconstructs the
    reported aggregate (an algebraic identity, not a statistical fit).

    Args:
        parts: 2-D array, shape ``(n_entities, n_components)``.
        whole: 1-D array, shape ``(n_entities,)`` -- the claimed aggregate per entity.

    Returns:
        ``max(abs(parts.sum(axis=1) - whole))``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_playtype_constants import sum_consistency_residual
            parts = np.array([[1.0, 2.0], [3.0, 4.0]])
            sum_consistency_residual(parts, parts.sum(axis=1))
    """
    return float(np.max(np.abs(np.asarray(parts, dtype=float).sum(axis=1) - np.asarray(whole, dtype=float))))


def calibration_slope(expected: np.ndarray, actual: np.ndarray) -> float:
    """OLS slope of *actual* regressed on *expected* (through the data, centered).

    A slope of 1.0 means the expected-value model is calibrated on average --
    used by models ③④ to gate the expected-vs-actual residual models.

    Args:
        expected: 1-D array of model-expected values.
        actual: 1-D array of observed values, same length as *expected*.

    Returns:
        The OLS slope; ``0.0`` when *expected* has zero variance.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_playtype_constants import calibration_slope
            x = np.array([1.0, 2.0, 3.0, 4.0])
            calibration_slope(x, x)
    """
    x = np.asarray(expected, dtype=float)
    y = np.asarray(actual, dtype=float)
    xc = x - x.mean()
    denom = float(np.sum(xc * xc))
    if denom == 0.0:
        return 0.0
    return float(np.sum(xc * (y - y.mean())) / denom)
