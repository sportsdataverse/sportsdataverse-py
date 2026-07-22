"""League-agnostic validation metrics + the as-of leakage split (T7.2).

These six helpers were duplicated verbatim across every per-sport
``*_prediction_constants`` / ``*_projection_constants`` module (Brier / log-loss /
Spearman / MAE / calibration table + the leakage-boundary date split). They are
extracted here so the identical implementation lives exactly once; the per-sport
modules re-export these names (redundant-alias re-export) and keep their own
league-specific constants.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package;
per-sport modules import from here.
"""

from __future__ import annotations

import dataclasses
import datetime

import numpy as np
import polars as pl
from scipy.stats import rankdata


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
            from sportsdataverse.modeling.eval.metrics import brier_score
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
            from sportsdataverse.modeling.eval.metrics import log_loss_score
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
            from sportsdataverse.modeling.eval.metrics import spearman_corr
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
            from sportsdataverse.modeling.eval.metrics import mae
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
            from sportsdataverse.modeling.eval.metrics import calibration_table
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
            from sportsdataverse.modeling.eval.metrics import as_of_ratings_split
            as_of_ratings_split(results, dt.date(2023, 9, 8))
    """
    return results.filter(pl.col("date") < cutoff_date)


_BASELINE_METRICS = {
    "brier": brier_score,
    "log_loss": log_loss_score,
    "mae": mae,
}


@dataclasses.dataclass(frozen=True)
class BaselineResult:
    """Outcome of a beat-the-baseline comparison (WS2).

    Attributes:
        metric: Metric name the comparison ran on (lower is better).
        model_metric: Model score.
        baseline_metric: Baseline score on the SAME metric.
        delta: ``model_metric - baseline_metric`` (negative = model wins).
        beat_baseline: True when the model strictly beats the baseline.
    """

    metric: str
    model_metric: float
    baseline_metric: float
    delta: float
    beat_baseline: bool


def baseline_test(
    y_true: np.ndarray,
    pred_model: np.ndarray,
    pred_baseline: np.ndarray,
    *,
    metric: str = "brier",
) -> BaselineResult:
    """Score model and baseline with the SAME metric; ship only on a win.

    The reference rule: a model release must beat its named baseline on the very
    metric it is evaluated with — scored identically, never a different
    metric for each side. All supported metrics are lower-is-better.

    Args:
        y_true: Observed outcomes (binary for ``brier`` / ``log_loss``,
            continuous for ``mae``).
        pred_model: Model predictions (probabilities or point values).
        pred_baseline: Baseline predictions on the same scale.
        metric: ``"brier"`` | ``"log_loss"`` | ``"mae"``.

    Returns:
        A :class:`BaselineResult`; ``beat_baseline`` False means do not ship
        (or record a logged override in the experiment ledger).

    Raises:
        ValueError: On an unknown metric name.

    Example:
        Release gate::

            import numpy as np
            from sportsdataverse.modeling.eval.metrics import baseline_test
            res = baseline_test(y, p_model, p_vegas, metric="brier")
            assert res.beat_baseline, f"model loses to baseline by {res.delta:+.4f}"
    """
    if metric not in _BASELINE_METRICS:
        raise ValueError(f"Unknown metric {metric!r}; valid: {sorted(_BASELINE_METRICS)}")
    scorer = _BASELINE_METRICS[metric]
    model_metric = scorer(y_true, pred_model)
    baseline_metric = scorer(y_true, pred_baseline)
    return BaselineResult(
        metric=metric,
        model_metric=model_metric,
        baseline_metric=baseline_metric,
        delta=model_metric - baseline_metric,
        beat_baseline=model_metric < baseline_metric,
    )


def group_error_metrics(
    df: pl.DataFrame,
    *,
    pred_col: str,
    actual_col: str,
    group_cols: "list[str]",
    probabilistic: bool = False,
) -> pl.DataFrame:
    """Error metrics by any cut — one row per (group, group_value) (WS2).

    The reference "programmable group-cut" backtest shape: the same metric suite
    computed per value of each grouping column, long-form keyed by
    ``group`` / ``group_value`` so model cards can slice arbitrarily.

    Args:
        df: Frame holding predictions and actuals.
        pred_col: Prediction column (probability of the over when
            ``probabilistic``; point projection otherwise).
        actual_col: Observed outcome column (binary when ``probabilistic``).
        group_cols: Grouping columns; each contributes its own rows.
        probabilistic: Add ``brier`` / ``log_loss`` / ``hit_rate`` columns.

    Returns:
        A ``polars.DataFrame`` with ``group``, ``group_value`` (Utf8), ``n``,
        ``rmse``, ``mae`` and, when ``probabilistic``, ``brier`` /
        ``log_loss`` / ``hit_rate``.

    Example:
        Model-card cuts::

            from sportsdataverse.modeling.eval.metrics import group_error_metrics
            group_error_metrics(preds, pred_col="p_over", actual_col="is_over",
                                group_cols=["season", "position"], probabilistic=True)
    """
    err = pl.col(pred_col) - pl.col(actual_col)
    aggs = [
        pl.len().alias("n"),
        (err.pow(2).mean()).sqrt().alias("rmse"),
        err.abs().mean().alias("mae"),
    ]
    if probabilistic:
        p = pl.col(pred_col).clip(1e-15, 1.0 - 1e-15)
        y = pl.col(actual_col)
        aggs += [
            err.pow(2).mean().alias("brier"),
            (-(y * p.log() + (1 - y) * (1 - p).log())).mean().alias("log_loss"),
            ((pl.col(pred_col) > 0.5).cast(pl.Float64) == y.cast(pl.Float64)).cast(pl.Float64).mean().alias("hit_rate"),
        ]
    parts = [
        df.group_by(col)
        .agg(aggs)
        .with_columns(
            pl.lit(col).alias("group"),
            pl.col(col).cast(pl.Utf8).alias("group_value"),
        )
        .drop(col)
        for col in group_cols
    ]
    out = pl.concat(parts, how="vertical")
    front = ["group", "group_value", "n", "rmse", "mae"]
    return out.select([*front, *[c for c in out.columns if c not in front]]).sort("group", "group_value")
