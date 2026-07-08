"""CFB prediction-spine constants + validation metrics (compute-on-demand)."""

from __future__ import annotations

import datetime
from dataclasses import dataclass

import numpy as np
import polars as pl
from scipy.stats import rankdata


@dataclass
class RatingsConfig:
    """Tunable knobs for the CFB ratings engine (ridge regression + competitiveness filter).

    Args:
        ridge_lambda: L2 regularization strength for the ridge-regression rating
            fit. ``cfb_adjusted_epa._fit_opponent_ridge`` scales the sklearn
            penalty as ``alpha = ridge_lambda * n_plays``, so ``ridge_lambda`` is
            a per-observation penalty (scale-invariant in ``n``). The default
            ``0.05`` is validated on a full FBS season (139k plays): it puts
            ``adj_net`` at Spearman 0.928 vs ESPN FPI and ``fei_net`` at 0.967 vs
            Fremeau FEI. (``cfb_adjusted_epa``'s own ``_RIDGE_LAMBDA=325`` default
            is calibrated for far smaller per-call play counts and would crush
            every team effect to ~0 at season scale -> ~0.76.)
        min_competitive_wp: Lower win-probability bound a game must clear to count as
            "competitive" (garbage-time / blowout filtering).
        max_competitive_wp: Upper win-probability bound a game must clear to count as
            "competitive".
        division: NCAA division slug the ratings are scoped to (e.g. ``"fbs"``).
    """

    ridge_lambda: float = 0.05
    min_competitive_wp: float = 0.1
    max_competitive_wp: float = 0.9
    division: str = "fbs"


@dataclass
class PredictConfig:
    """Era-specific coefficients for the CFB game-outcome prediction model.

    ``net_points_scale``, ``hfa``, ``margin_sd``, ``total_intercept`` and
    ``total_scale`` are **fitted (in-sample) on the 2023 backtest** by
    ``dev/cfb_prediction/fit_pregame.py``. The ratings that feed the fit use a
    leakage-free week-by-week as-of boundary, but these five coefficients are fit
    on the same 2023 games the backtest gate then scores -- so the gate is an
    in-sample regression guard, not an out-of-sample generalization result (a 2024
    holdout is a documented follow-up). See :mod:`sportsdataverse.cfb.cfb_game_predict`. ``adj_net`` from
    the ratings engine is on an EPA-per-play scale, so ``net_points_scale`` is the
    fitted EPA/play -> points conversion (without it the rating differential is
    negligible next to a points-scale HFA and the model is near-constant).

    Args:
        hfa: Home-field advantage, in points (fitted).
        margin_sd: Standard deviation of the margin residuals, used to convert a
            predicted margin into a win probability via the Gaussian CDF (fitted).
        net_points_scale: Points per unit of net adjusted-EPA/play differential --
            the fitted slope mapping ``home_adj_net - away_adj_net`` to points.
        total_intercept: Fitted baseline point total (intercept of the totals fit).
        total_scale: Fitted slope on the summed four efficiency ratings for totals.
        avg_drives: Average number of offensive drives per team per game (reserved
            for the season Monte Carlo in Phase 4).
        points_per_epa: Conversion factor from expected-points-added to points
            (reserved for Phase 4).
        quality_win_threshold: Minimum rating differential for a win to count as a
            "quality win" in résumé-style summaries (Phase 3).
        bubble_adj_net: Net rating adjustment applied to bubble-team comparisons
            (Phase 3).
    """

    hfa: float
    margin_sd: float
    net_points_scale: float
    total_intercept: float
    total_scale: float
    avg_drives: float
    points_per_epa: float
    quality_win_threshold: float
    bubble_adj_net: float


CFB_CONSTANTS: dict[str, PredictConfig] = {
    # net_points_scale / hfa / margin_sd / total_* fitted on the 2023 backtest by
    # dev/cfb_prediction/fit_pregame.py (see that script for the exact procedure).
    "modern": PredictConfig(
        hfa=3.1369,
        margin_sd=17.1184,
        net_points_scale=33.6318,
        total_intercept=51.7496,
        total_scale=21.4705,
        avg_drives=12.0,
        points_per_epa=1.0,
        quality_win_threshold=0.0,
        bubble_adj_net=0.0,
    ),
}


def get_constants(era: str = "modern") -> PredictConfig:
    """Look up the :class:`PredictConfig` for a given era.

    Args:
        era: Era key into :data:`CFB_CONSTANTS` (e.g. ``"modern"``).

    Returns:
        The :class:`PredictConfig` registered for ``era``.

    Raises:
        ValueError: If ``era`` is not a registered key.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_prediction_constants import get_constants
            cfg = get_constants("modern")
            cfg.hfa
    """
    try:
        return CFB_CONSTANTS[era]
    except KeyError:
        valid = ", ".join(sorted(CFB_CONSTANTS))
        raise ValueError(f"Unknown era {era!r}; valid eras are: {valid}") from None


def as_of_ratings_split(results: pl.DataFrame, cutoff_date: datetime.date) -> pl.DataFrame:
    """Filter a results frame to games strictly before a cutoff date (leakage boundary).

    Args:
        results: A ``polars.DataFrame`` with a ``date`` column.
        cutoff_date: Games on or after this date are excluded.

    Returns:
        A ``polars.DataFrame`` containing only rows with ``date < cutoff_date``.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.cfb.cfb_prediction_constants import as_of_ratings_split
            as_of_ratings_split(results, dt.date(2023, 9, 8))
    """
    return results.filter(pl.col("date") < cutoff_date)


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Mean squared error between predicted probabilities and binary outcomes.

    Args:
        y_true: Array of binary outcomes (0/1).
        p_pred: Array of predicted probabilities in [0, 1].

    Returns:
        The Brier score (0.0 is a perfect forecast).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.cfb.cfb_prediction_constants import brier_score
            brier_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary cross-entropy loss between predicted probabilities and outcomes.

    Args:
        y_true: Array of binary outcomes (0/1).
        p_pred: Array of predicted probabilities in [0, 1].
        eps: Clipping bound to avoid ``log(0)``.

    Returns:
        The mean log loss.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.cfb.cfb_prediction_constants import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    p = np.clip(np.asarray(p_pred, dtype=float), eps, 1 - eps)
    y = np.asarray(y_true, dtype=float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two arrays.

    Args:
        a: First array of values.
        b: Second array of values (same length as ``a``).

    Returns:
        The Spearman rank correlation coefficient.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.cfb.cfb_prediction_constants import spearman_corr
            spearman_corr(np.array([1, 2, 3]), np.array([3, 1, 2]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two arrays.

    Args:
        a: First array of values.
        b: Second array of values (same length as ``a``).

    Returns:
        The mean absolute error.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.cfb.cfb_prediction_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bucket predicted probabilities into bins and compare to actual outcome rates.

    Args:
        y_true: Array of binary outcomes (0/1).
        p_pred: Array of predicted probabilities in [0, 1].
        n_bins: Number of equal-width probability bins.

    Returns:
        A ``polars.DataFrame`` with columns ``bin_mid``, ``mean_pred``,
        ``mean_actual``, ``n`` (one row per non-empty bin).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.cfb.cfb_prediction_constants import calibration_table
            calibration_table(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
    """
    df = pl.DataFrame(
        {
            "y": np.asarray(y_true, dtype=float),
            "p": np.asarray(p_pred, dtype=float),
        }
    )
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    return (
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
