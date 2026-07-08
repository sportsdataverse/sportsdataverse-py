"""Scheme/special-teams shared constants + validation metrics (league-agnostic).

This module is the single home for the NFL scheme/special-teams spine's
fitted-constant tables, the as-of leakage split, and the numpy-only
validation metrics every model module imports.  Algorithms stay
league-agnostic; every NFL-specific number lives in a table here.

Fitted values are produced by the committed ``dev/nfl_scheme/fit_*.py``
scripts (see each table's comment); they are never invented inline.
"""

from __future__ import annotations

from typing import Dict

import numpy as np
import polars as pl
from scipy.stats import rankdata

# --------------------------------------------------------------------------- #
# validation metrics (numpy-only, no sklearn)
# --------------------------------------------------------------------------- #


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Mean squared error between binary outcomes and predicted probabilities.

    Args:
        y_true: Binary outcome array (0/1).
        p_pred: Predicted probability array, same length.

    Returns:
        The Brier score (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_scheme_constants import brier_score
            brier_score(np.array([1, 0]), np.array([0.75, 0.25]))
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary cross-entropy of predicted probabilities against outcomes.

    Args:
        y_true: Binary outcome array (0/1).
        p_pred: Predicted probability array, same length.
        eps: Probability clip to avoid log(0).

    Returns:
        Mean negative log-likelihood (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_scheme_constants import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    y = np.asarray(y_true, dtype=float)
    p = np.clip(np.asarray(p_pred, dtype=float), eps, 1.0 - eps)
    return float(-np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))


def auc_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """ROC AUC via the rank (Mann-Whitney U) formulation.

    Args:
        y_true: Binary outcome array (0/1).
        p_pred: Predicted score/probability array, same length.

    Returns:
        AUC in [0, 1]; ``nan`` when only one class is present.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_scheme_constants import auc_score
            auc_score(np.array([0, 0, 1, 1]), np.array([0.1, 0.2, 0.8, 0.9]))
    """
    y = np.asarray(y_true, dtype=int)
    n_pos = int(y.sum())
    n_neg = int(y.size - n_pos)
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    ranks = rankdata(np.asarray(p_pred, dtype=float))
    return float((ranks[y == 1].sum() - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two arrays.

    Args:
        a: First array.
        b: Second array, same length.

    Returns:
        Rank correlation in [-1, 1].

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_scheme_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([2.0, 4.0, 6.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two arrays.

    Args:
        a: First array.
        b: Second array, same length.

    Returns:
        Mean of ``|a - b|``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_scheme_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bin predictions and compare mean predicted vs mean actual per bin.

    Args:
        y_true: Binary outcome array (0/1).
        p_pred: Predicted probability array, same length.
        n_bins: Number of equal-width probability bins.

    Returns:
        A polars frame with ``bin_mid``, ``mean_pred``, ``mean_actual``, ``n``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_scheme_constants import calibration_table
            tbl = calibration_table(np.array([1, 0, 1]), np.array([0.9, 0.2, 0.7]))
            print(tbl.shape)
    """
    df = pl.DataFrame({"y": np.asarray(y_true, dtype=float), "p": np.asarray(p_pred, dtype=float)})
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
        .select("bin_mid", "mean_pred", "mean_actual", pl.col("n").cast(pl.Int64))
    )


# --------------------------------------------------------------------------- #
# constants tables (league-specific numbers live here, not in algorithms)
# --------------------------------------------------------------------------- #

#: Stadium altitude (feet above sea level) per current home-team abbreviation.
#: Non-trivial venues: Denver (Empower Field ~5280 ft) and Arizona (State Farm
#: Stadium, Glendale ~1070 ft, retractable roof).  Mexico City / international
#: one-offs are ignored (they carry the home team's abbr).  Source:
#: https://en.wikipedia.org/wiki/List_of_current_NFL_stadiums (elevations).
STADIUM_ALTITUDE: Dict[str, float] = {
    "ARI": 1070.0,
    "ATL": 0.0,
    "BAL": 0.0,
    "BUF": 0.0,
    "CAR": 0.0,
    "CHI": 0.0,
    "CIN": 0.0,
    "CLE": 0.0,
    "DAL": 0.0,
    "DEN": 5280.0,
    "DET": 0.0,
    "GB": 0.0,
    "HOU": 0.0,
    "IND": 0.0,
    "JAX": 0.0,
    "KC": 0.0,
    "LA": 0.0,
    "LAC": 0.0,
    "LV": 0.0,
    "MIA": 0.0,
    "MIN": 0.0,
    "NE": 0.0,
    "NO": 0.0,
    "NYG": 0.0,
    "NYJ": 0.0,
    "PHI": 0.0,
    "PIT": 0.0,
    "SEA": 0.0,
    "SF": 0.0,
    "TB": 0.0,
    "TEN": 0.0,
    "WAS": 0.0,
}

#: Environment FG make-prob logit slopes.  Seeded 0.0 at scaffold time;
#: overwritten with the fitted values from ``dev/nfl_scheme/fit_env_fg_coef.py``
#: (Task 3.2) — logistic fit of made ~ offset(logit(base)) + wind + (temp-60) + alt_kft.
ENVIRONMENT_FG_COEF: Dict[str, float] = {
    "wind": 0.0,
    "temp": 0.0,
    "altitude_kft": 0.0,
    "temp_baseline": 60.0,
}

#: Expected-plays OLS coefficients (realized offensive plays per team-game ~
#: [1, neutral_sec_per_play, opp_neutral_sec_per_play, total_line]).  Seeded at
#: scaffold time; overwritten by ``dev/nfl_scheme/fit_pace_constants.py`` (Task 2.2).
PACE_CONSTANTS: Dict[str, float] = {
    "intercept": 63.0,
    "b_pace": 0.0,
    "b_opp_pace": 0.0,
    "b_total": 0.0,
}

#: Empirical-Bayes shrinkage pseudo-counts.  Seeds; overwritten with the
#: split-half fits from ``dev/nfl_scheme/fit_eb_priors.py`` (Tasks 3.3 / 5.2).
EB_PRIOR: Dict[str, float] = {
    "K_fg": 15.0,
    "K_pressure": 8.0,
}

#: Bundled play-call classifier artifact filename (Phase 1).
PLAYCALL_ARTIFACT: str = "nfl_playcall.ubj"


def as_of_split(pbp: pl.DataFrame, season: int, week: int) -> pl.DataFrame:
    """Return rows strictly before ``(season, week)`` — the single leakage boundary.

    Every as-of rating/backtest in the scheme spine slices its training data
    through this function so game *G* is rated using only data strictly
    before *G*.

    Args:
        pbp: Any frame carrying ``season`` and ``week`` columns.
        week: Evaluation week; rows from this week (and later) are excluded.
        season: Evaluation season; later seasons are excluded.

    Returns:
        The subset of ``pbp`` strictly before ``(season, week)``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl.nfl_scheme_constants import as_of_split
            df = pl.DataFrame({"season": [2022, 2023], "week": [18, 1]})
            as_of_split(df, season=2023, week=1).height
    """
    return pbp.filter((pl.col("season") < season) | ((pl.col("season") == season) & (pl.col("week") < week)))
