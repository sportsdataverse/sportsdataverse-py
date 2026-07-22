"""Model evaluation — metrics, the beat-the-baseline gate, the experiment
ledger, and season backtests."""

from __future__ import annotations

from sportsdataverse.modeling.eval.backtest import (
    BacktestResult,
    as_of_holdout,
    backtest,
)
from sportsdataverse.modeling.eval.experiment_ledger import (
    INGEST_TOKEN_ENV,
    INGEST_URL_ENV,
    ExperimentRun,
    log_run,
    push_run,
    run_row,
)
from sportsdataverse.modeling.eval.metrics import (
    BaselineResult,
    as_of_ratings_split,
    baseline_test,
    brier_score,
    calibration_table,
    group_error_metrics,
    log_loss_score,
    mae,
    spearman_corr,
)

__all__ = [
    "BacktestResult",
    "BaselineResult",
    "ExperimentRun",
    "INGEST_TOKEN_ENV",
    "INGEST_URL_ENV",
    "as_of_holdout",
    "as_of_ratings_split",
    "backtest",
    "baseline_test",
    "brier_score",
    "calibration_table",
    "group_error_metrics",
    "log_loss_score",
    "log_run",
    "mae",
    "push_run",
    "run_row",
    "spearman_corr",
]
