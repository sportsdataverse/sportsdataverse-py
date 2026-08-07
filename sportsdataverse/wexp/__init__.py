"""Win-expectancy bake-off harness (NFL + CFB).

Vintage-keyed feature store, market oracle, scoring, baseline oracles, and
the walk-forward backtest driver. See
``ClaudeCowork/plans/win-expectancy-bakeoff.md`` for the program plan.
"""

from sportsdataverse.wexp.market import (
    devig_multiplicative,
    devig_shin,
    logit_blend,
    moneyline_pair_prob,
    prob_from_american,
    prob_from_decimal,
    spread_to_prob,
)
from sportsdataverse.wexp.scoring import (
    RESULT_SCHEMA,
    append_results,
    brier_score,
    calibration_table,
    closing_line_value,
    ece,
    favorite_bucket_table,
    log_loss_score,
    mae,
    result_rows,
    spearman_corr,
    winner_accuracy,
)

__all__ = [
    "RESULT_SCHEMA",
    "append_results",
    "brier_score",
    "calibration_table",
    "closing_line_value",
    "devig_multiplicative",
    "devig_shin",
    "ece",
    "favorite_bucket_table",
    "log_loss_score",
    "logit_blend",
    "mae",
    "moneyline_pair_prob",
    "prob_from_american",
    "prob_from_decimal",
    "result_rows",
    "spearman_corr",
    "spread_to_prob",
    "winner_accuracy",
]
