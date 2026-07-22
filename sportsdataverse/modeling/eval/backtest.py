"""Season-scale backtest harness — score predictions against realized outcomes.

The adapted role of the sim-engine's oracle-comparison pipeline: run a
prediction function over a set of units (games, snapshots, matchups), score
it against the realized outcomes with the shared :mod:`~sportsdataverse.modeling.eval.metrics`,
and compare to a naive baseline (the beat-the-baseline gate) and, when a
market line is available, to that reference (the reference-ratio evaluator).

The harness is model-agnostic — the caller supplies ``predict_fn`` and
``outcome_fn``. :func:`as_of_holdout` enforces the leakage boundary: only
units strictly before the as-of date may inform the model, and the caller
builds ``predict_fn`` over that history. The market half degrades gracefully
— pass ``reference_fn=None`` (the default) until odds history exists and the
ratio is simply absent.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Callable, Optional, Sequence, Tuple

import numpy as np
import polars as pl

from sportsdataverse.modeling.eval.metrics import (
    BaselineResult,
    baseline_test,
    brier_score,
    calibration_table,
    log_loss_score,
    mae,
)

_METRIC_FNS = {"brier": brier_score, "log_loss": log_loss_score, "mae": mae}
_PROB_METRICS = {"brier", "log_loss"}


def as_of_holdout(
    frame: pl.DataFrame,
    *,
    as_of: Any,
    date_key: str = "date",
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Split a frame at the as-of date into (history, holdout).

    Args:
        frame: The full unit frame.
        as_of: The leakage boundary; units with ``date_key < as_of`` are
            history (may inform the model), the rest are the holdout.
        date_key: The date/order column.

    Returns:
        ``(history, holdout)`` — disjoint by construction.

    Raises:
        ValueError: When ``date_key`` is absent.

    Example:
        Quick start::

            import datetime as dt
            history, holdout = as_of_holdout(games, as_of=dt.date(2024, 1, 1))
    """
    if date_key not in frame.columns:
        raise ValueError(f"date_key {date_key!r} not in frame")
    return frame.filter(pl.col(date_key) < as_of), frame.filter(pl.col(date_key) >= as_of)


@dataclasses.dataclass(frozen=True)
class BacktestResult:
    """Outcome of a backtest run.

    Attributes:
        predictions: One row per unit — ``label``, ``prediction``,
            ``outcome``, ``baseline``, and ``reference`` (null when no
            reference was supplied).
        metric: The scoring metric (lower is better).
        score: Model score on ``metric``.
        baseline: The beat-the-baseline comparison vs the naive baseline.
        reference_ratio: ``model_score / reference_score`` when a reference
            was supplied (``< 1`` = model beats the market), else ``None``.
        calibration: Reliability table for probability metrics, else ``None``.
        n: Number of scored units.
    """

    predictions: pl.DataFrame
    metric: str
    score: float
    baseline: BaselineResult
    reference_ratio: Optional[float]
    calibration: Optional[pl.DataFrame]
    n: int


def backtest(
    units: Sequence[Any],
    predict_fn: Callable[[Any], float],
    outcome_fn: Callable[[Any], float],
    *,
    metric: str = "brier",
    baseline_fn: Optional[Callable[[Any], float]] = None,
    reference_fn: Optional[Callable[[Any], float]] = None,
    label_fn: Optional[Callable[[Any], Any]] = None,
) -> BacktestResult:
    """Score ``predict_fn`` over ``units`` against realized outcomes.

    Args:
        units: The prediction units (games, snapshots, matchups).
        predict_fn: ``unit -> prediction`` (probability for ``brier`` /
            ``log_loss``, a value for ``mae``).
        outcome_fn: ``unit -> realized outcome`` (0/1 for probability
            metrics, a value for ``mae``).
        metric: ``brier`` | ``log_loss`` | ``mae`` (lower is better).
        baseline_fn: ``unit -> naive prediction``; defaults to 0.5 for
            probability metrics and the mean realized outcome for ``mae``.
        reference_fn: ``unit -> market/reference prediction``; when given,
            the result carries ``reference_ratio``.
        label_fn: ``unit -> label`` for the predictions frame (defaults to
            the unit index).

    Returns:
        The :class:`BacktestResult`.

    Raises:
        ValueError: On empty ``units`` or an unknown ``metric``.

    Example:
        Backtest an in-game win-probability model vs a coin-flip baseline::

            from sportsdataverse.modeling.eval import backtest
            res = backtest(snapshots, predict_home_wp, realized_home_win,
                           metric="brier", baseline_fn=lambda _s: 0.5)
            res.score, res.baseline.beat_baseline
    """
    if not units:
        raise ValueError("backtest needs at least one unit")
    if metric not in _METRIC_FNS:
        raise ValueError(f"unknown metric {metric!r}; choose from {sorted(_METRIC_FNS)}")
    metric_fn = _METRIC_FNS[metric]

    labels = [label_fn(u) if label_fn else i for i, u in enumerate(units)]
    preds = np.array([float(predict_fn(u)) for u in units], dtype=float)
    outcomes = np.array([float(outcome_fn(u)) for u in units], dtype=float)
    if baseline_fn is not None:
        baseline_preds = np.array([float(baseline_fn(u)) for u in units], dtype=float)
    elif metric in _PROB_METRICS:
        baseline_preds = np.full_like(preds, 0.5)
    else:
        baseline_preds = np.full_like(preds, float(outcomes.mean()))
    references = np.array([float(reference_fn(u)) for u in units], dtype=float) if reference_fn else None

    score = metric_fn(outcomes, preds)
    baseline = baseline_test(outcomes, preds, baseline_preds, metric=metric)
    reference_ratio: Optional[float] = None
    if references is not None:
        reference_score = metric_fn(outcomes, references)
        reference_ratio = float(score / reference_score) if reference_score > 0 else float("inf")
    calibration = calibration_table(outcomes, preds) if metric in _PROB_METRICS else None

    frame = pl.DataFrame(
        {
            "label": labels,
            "prediction": preds,
            "outcome": outcomes,
            "baseline": baseline_preds,
            "reference": references if references is not None else [None] * len(units),
        }
    )
    return BacktestResult(
        predictions=frame,
        metric=metric,
        score=float(score),
        baseline=baseline,
        reference_ratio=reference_ratio,
        calibration=calibration,
        n=len(units),
    )
