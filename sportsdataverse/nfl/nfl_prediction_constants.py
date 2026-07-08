"""NFL ratings-and-market spine constants + validation metrics (compute-on-demand).

The single home for the spine's fitted constants (ridge lambda, HFA, margin
sigma, points-per-net, totals, prop shrinkage/priors), the shared validation
metric helpers (Brier / log-loss / Spearman / MAE / calibration table), and the
as-of-date leakage split. League-agnostic algorithms, league-specific
constants: no fitted number is hard-coded inside an algorithm module.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Dict

import numpy as np
import polars as pl
from scipy.stats import rankdata


@dataclass(frozen=True)
class RatingsConfig:
    """Knobs for the opponent-adjusted EPA ridge (model 1).

    ``ridge_lambda`` is fitted by ``dev/nfl_prediction/fit_ridge_lambda.py``
    (2023 out-of-sample next-week agreement); the competitive-``wp`` window
    drops garbage-time plays before the ridge.
    """

    # Fitted by dev/nfl_prediction/fit_ridge_lambda.py: grid {25,50,100,200,400,800},
    # mean spearman(as-of adj_net diff, realized margin) over 2023 weeks 6-18.
    ridge_lambda: float = 200.0
    min_competitive_wp: float = 0.05
    max_competitive_wp: float = 0.95


@dataclass(frozen=True)
class PredictConfig:
    """Closed-form pregame constants (model 2), one row per era.

    Seed values are published references (NFL modern HFA ~1.8 pts, margin SD
    ~13.5); the real numbers are fitted by ``dev/nfl_prediction/fit_pregame.py``
    on the committed 2023 backtest fixtures and pasted here.
    """

    hfa: float
    margin_sd: float
    points_per_net: float
    avg_total: float
    total_scale: float


@dataclass(frozen=True)
class PropConfig:
    """Empirical-Bayes prop-projection constants (model 3), one row per era.

    ``shrink_*`` are the EB kappas per stat family; ``pos_priors`` maps
    position -> stat -> league prior. Seeds are league per-game averages;
    fitted values come from ``dev/nfl_prediction/fit_props.py``.
    """

    shrink_pass: float
    shrink_rush: float
    shrink_rec: float
    pos_priors: Dict[str, Dict[str, float]] = field(default_factory=dict)


NFL_CONSTANTS: Dict[str, PredictConfig] = {
    # Seed values (published references); overwritten by dev/nfl_prediction/fit_pregame.py.
    "modern": PredictConfig(
        hfa=1.8,
        margin_sd=13.5,
        points_per_net=65.0,
        avg_total=43.0,
        total_scale=120.0,
    ),
}

PROP_CONSTANTS: Dict[str, PropConfig] = {
    # Seed values (league per-game averages); overwritten by dev/nfl_prediction/fit_props.py.
    "modern": PropConfig(
        shrink_pass=4.0,
        shrink_rush=5.0,
        shrink_rec=6.0,
        pos_priors={
            "QB": {
                "attempts": 30.0,
                "ypa": 7.0,
                "pass_td_rate": 0.045,
                "carries": 4.0,
                "ypc": 4.0,
                "rush_td_rate": 0.03,
                "targets": 0.0,
                "ypt": 0.0,
                "rec_td_rate": 0.0,
            },
            "RB": {
                "attempts": 0.0,
                "ypa": 0.0,
                "pass_td_rate": 0.0,
                "carries": 10.0,
                "ypc": 4.2,
                "rush_td_rate": 0.03,
                "targets": 3.0,
                "ypt": 6.0,
                "rec_td_rate": 0.02,
            },
            "WR": {
                "attempts": 0.0,
                "ypa": 0.0,
                "pass_td_rate": 0.0,
                "carries": 0.3,
                "ypc": 6.0,
                "rush_td_rate": 0.02,
                "targets": 5.5,
                "ypt": 8.0,
                "rec_td_rate": 0.05,
            },
            "TE": {
                "attempts": 0.0,
                "ypa": 0.0,
                "pass_td_rate": 0.0,
                "carries": 0.1,
                "ypc": 4.0,
                "rush_td_rate": 0.02,
                "targets": 4.0,
                "ypt": 7.5,
                "rec_td_rate": 0.05,
            },
        },
    ),
}


def get_constants(era: str = "modern") -> PredictConfig:
    """Return the pregame prediction constants for an era.

    Args:
        era: Era key in ``NFL_CONSTANTS`` (currently only ``"modern"``).

    Returns:
        The frozen :class:`PredictConfig` for that era.

    Raises:
        ValueError: If ``era`` is not a known key.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_prediction_constants import get_constants
            cfg = get_constants("modern")
            print(cfg.hfa)
    """
    if era not in NFL_CONSTANTS:
        raise ValueError(f"Unknown era {era!r}; known: {sorted(NFL_CONSTANTS)}")
    return NFL_CONSTANTS[era]


def get_prop_constants(era: str = "modern") -> PropConfig:
    """Return the player-prop projection constants for an era.

    Args:
        era: Era key in ``PROP_CONSTANTS`` (currently only ``"modern"``).

    Returns:
        The frozen :class:`PropConfig` for that era.

    Raises:
        ValueError: If ``era`` is not a known key.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_prediction_constants import get_prop_constants
            cfg = get_prop_constants("modern")
            print(cfg.shrink_pass)
    """
    if era not in PROP_CONSTANTS:
        raise ValueError(f"Unknown era {era!r}; known: {sorted(PROP_CONSTANTS)}")
    return PROP_CONSTANTS[era]


def as_of_ratings_split(
    results: pl.DataFrame,
    cutoff_date: dt.date,
    *,
    date_col: str = "gameday",
) -> pl.DataFrame:
    """Return only rows strictly before ``cutoff_date`` (the leakage boundary).

    Every predictive backtest rates a game using only games strictly earlier
    than that game's date — same-day and later rows are excluded.

    Args:
        results: Frame with a date column (e.g. the results fixture).
        cutoff_date: Exclusive upper bound.
        date_col: Name of the date column (default ``"gameday"``).

    Returns:
        pl.DataFrame: The strict ``date < cutoff_date`` subset.

    Example:
        Quick start::

            import datetime as dt
            import polars as pl
            from sportsdataverse.nfl.nfl_prediction_constants import as_of_ratings_split
            past = as_of_ratings_split(results, dt.date(2023, 11, 1))
    """
    return results.filter(pl.col(date_col) < cutoff_date)


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Mean squared error between predicted probabilities and binary outcomes.

    Args:
        y_true: Binary outcomes (0/1).
        p_pred: Predicted probabilities in ``[0, 1]``.

    Returns:
        The Brier score (lower is better; 0.0 is perfect).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import brier_score
            brier_score(np.array([1, 0]), np.array([0.75, 0.25]))
    """
    return float(np.mean((np.asarray(p_pred, float) - np.asarray(y_true, float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary cross-entropy between predicted probabilities and outcomes.

    Args:
        y_true: Binary outcomes (0/1).
        p_pred: Predicted probabilities in ``[0, 1]`` (clipped by ``eps``).
        eps: Clip bound keeping ``log`` finite.

    Returns:
        The mean negative log-likelihood (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    p = np.clip(np.asarray(p_pred, float), eps, 1 - eps)
    y = np.asarray(y_true, float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two vectors.

    Args:
        a: First vector.
        b: Second vector (same length).

    Returns:
        The rank correlation in ``[-1, 1]``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0, 30.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two vectors.

    Args:
        a: First vector.
        b: Second vector (same length).

    Returns:
        The mean absolute difference.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, float) - np.asarray(b, float))))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bucket predictions into probability bins and compare to realized rates.

    Args:
        y_true: Binary outcomes (0/1).
        p_pred: Predicted probabilities in ``[0, 1]``.
        n_bins: Number of equal-width probability bins.

    Returns:
        pl.DataFrame: One row per non-empty bin with columns ``bin_mid``
        (Float64), ``mean_pred`` (Float64), ``mean_actual`` (Float64), ``n``
        (Int64/UInt32 count).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_prediction_constants import calibration_table
            tbl = calibration_table(np.array([1, 0, 1, 1]), np.array([0.9, 0.2, 0.7, 0.6]))
            print(tbl.shape)
    """
    df = pl.DataFrame({"y": np.asarray(y_true, float), "p": np.asarray(p_pred, float)})
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
