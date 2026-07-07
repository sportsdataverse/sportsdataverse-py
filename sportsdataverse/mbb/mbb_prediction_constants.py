"""Prediction-stack shared constants + validation metrics (league-agnostic).

Home of the small, dependency-light pieces every module in the MBB/WBB
prediction & tournament stack shares:

* **Validation metrics** (:func:`brier_score`, :func:`log_loss_score`,
  :func:`spearman_corr`, :func:`mae`, :func:`calibration_table`) used by the
  phase oracle/backtest gates.

Later phases extend this module with the per-league constants table
(``LEAGUE_CONSTANTS`` / :func:`get_constants`) and the as-of-date leakage
split (:func:`as_of_ratings_split`).
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass

import numpy as np
import polars as pl
from scipy.stats import rankdata


@dataclass(frozen=True)
class LeagueConstants:
    """Per-league fitted constants for the prediction & tournament stack.

    Algorithms in the stack are league-agnostic; every men's/women's-specific
    number lives here so a WBB caller is a by-reference shim plus this table
    (the same pattern ``wbb_rapm`` / ``wbb_ratings`` already use).

    Attributes:
        hfa: Home-court advantage in points (fitted on the 2024 backtest).
        margin_sd: Std. dev. of the game-margin residual (fitted on the 2024
            backtest; the Brier-minimizing sigma agrees to within 0.04).
        em_scale: Slope applied to the AdjEM difference when predicting a game
            margin. AdjEM is per-100-possessions, so a game margin scales by
            ~tempo/100 (~0.67); the fitted value is lower still because the
            as-of AdjEM estimate is noisy and the optimal predictive slope is
            attenuated (regression dilution). Fitted jointly with ``hfa``.
        avg_tempo: League baseline possessions per game (adjusted-tempo anchor).
        avg_efficiency: League baseline points per 100 possessions.
        quad_thresholds: NET-style quadrant opponent-rank upper bounds, keyed by
            venue (``home`` / ``neutral`` / ``away``) then ``q1`` / ``q2`` / ``q3``
            (Quad 4 is any opponent ranked worse than ``q3``).
        in_game_wp_artifact: Filename of the bundled in-game-WP coefficients under
            ``sportsdataverse/mbb/models`` (fitted + committed in Phase 3).
    """

    hfa: float
    margin_sd: float
    em_scale: float
    avg_tempo: float
    avg_efficiency: float
    quad_thresholds: dict[str, dict[str, int]]
    in_game_wp_artifact: str


# NET-style quadrant opponent-rank upper bounds by venue (Quad 4 = worse than q3).
# Men's and women's Division I both use the NET with this quadrant structure; the
# thresholds are seeded identically and may be re-fit per league later.
_NET_QUAD_THRESHOLDS: dict[str, dict[str, int]] = {
    "home": {"q1": 30, "q2": 75, "q3": 160},
    "neutral": {"q1": 50, "q2": 100, "q3": 200},
    "away": {"q1": 75, "q2": 135, "q3": 240},
}

# Men's hfa / margin_sd / em_scale were fitted on the 2024 as-of-date backtest
# (``dev/mbb_prediction/fit_pregame.py``: joint least squares of actual margin on
# em_diff + non-neutral indicator over 4,359 eligible games; residual std 11.224
# vs Brier-minimizing 11.193). Women's values remain published-reference seeds
# until the Phase-7 refit. Quad thresholds are canonical NET definitions.
LEAGUE_CONSTANTS: dict[str, LeagueConstants] = {
    "mens": LeagueConstants(
        hfa=2.9281,
        margin_sd=11.2196,
        em_scale=0.5766,
        avg_tempo=69.6255,
        avg_efficiency=104.0,
        quad_thresholds=_NET_QUAD_THRESHOLDS,
        in_game_wp_artifact="mbb_in_game_wp.json",
    ),
    "womens": LeagueConstants(
        hfa=3.0,
        margin_sd=12.0,
        em_scale=0.5794,
        avg_tempo=70.0,
        avg_efficiency=95.0,
        quad_thresholds=_NET_QUAD_THRESHOLDS,
        in_game_wp_artifact="wbb_in_game_wp.json",
    ),
}


def get_constants(league: str) -> LeagueConstants:
    """Return the :class:`LeagueConstants` for a league.

    Args:
        league: Either ``"mens"`` or ``"womens"``.

    Returns:
        The league's :class:`LeagueConstants`.

    Raises:
        ValueError: If ``league`` is not a known key of ``LEAGUE_CONSTANTS``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_prediction_constants import get_constants
            get_constants("mens").hfa
    """
    try:
        return LEAGUE_CONSTANTS[league]
    except KeyError:
        raise ValueError(f"Unknown league {league!r}; expected one of {sorted(LEAGUE_CONSTANTS)}") from None


def as_of_ratings_split(results: pl.DataFrame, cutoff_date: datetime.date) -> pl.DataFrame:
    """Return only games strictly before ``cutoff_date`` (the leakage boundary).

    Predictive backtests must rate a game using only games that finished before
    it — this split enforces that as-of-date rule so no future information leaks
    into a game's own prediction.

    Args:
        results: A frame with a ``date`` column of dtype ``pl.Date``.
        cutoff_date: The date of the game being predicted; games on or after it
            are dropped.

    Returns:
        The subset of ``results`` with ``date < cutoff_date``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_prediction_constants import as_of_ratings_split
            prior = as_of_ratings_split(results, some_game_date)
    """
    return results.filter(pl.col("date") < cutoff_date)


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Mean squared error between binary outcomes and predicted probabilities.

    Args:
        y_true: Array of realized binary outcomes (0/1).
        p_pred: Array of predicted probabilities in ``[0, 1]``.

    Returns:
        The Brier score (lower is better; 0.0 is perfect).

    Example:
        Perfect predictions score zero::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import brier_score
            brier_score(np.array([1, 0]), np.array([1.0, 0.0]))
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary cross-entropy (log loss) between outcomes and probabilities.

    Args:
        y_true: Array of realized binary outcomes (0/1).
        p_pred: Array of predicted probabilities in ``[0, 1]``.
        eps: Clipping bound to keep the log finite at 0/1.

    Returns:
        The mean log loss (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    p = np.clip(np.asarray(p_pred, dtype=float), eps, 1 - eps)
    y = np.asarray(y_true, dtype=float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two arrays.

    Args:
        a: First array.
        b: Second array (same length as ``a``).

    Returns:
        The Spearman rank-correlation coefficient in ``[-1, 1]``.

    Example:
        A monotonic relationship scores 1.0::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0, 30.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two arrays.

    Args:
        a: First array.
        b: Second array (same length as ``a``).

    Returns:
        The mean absolute error.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bin predicted probabilities and compare mean-predicted vs mean-actual.

    Args:
        y_true: Array of realized binary outcomes (0/1).
        p_pred: Array of predicted probabilities in ``[0, 1]``.
        n_bins: Number of equal-width probability bins.

    Returns:
        A ``polars.DataFrame`` with columns ``bin_mid``, ``mean_pred``,
        ``mean_actual``, ``n`` (one row per non-empty bin, sorted ascending).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mbb.mbb_prediction_constants import calibration_table
            y = np.random.default_rng(0).integers(0, 2, 200)
            p = np.random.default_rng(1).random(200)
            calibration_table(y, p, n_bins=10)
    """
    df = pl.DataFrame(
        {
            "y": np.asarray(y_true, dtype=float),
            "p": np.asarray(p_pred, dtype=float),
        }
    )
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    out = (
        df.group_by("bin")
        .agg(
            pl.col("p").mean().alias("mean_pred"),
            pl.col("y").mean().alias("mean_actual"),
            pl.len().alias("n"),
        )
        .sort("bin")
        .with_columns(((pl.col("bin") + 0.5) / n_bins).alias("bin_mid"))
        .select("bin_mid", "mean_pred", "mean_actual", "n")
    )
    return out
