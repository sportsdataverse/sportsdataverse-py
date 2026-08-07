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
from sportsdataverse.wexp.baselines import (
    BASELINE_HOME_RATE,
    baseline_probs,
    score_baselines,
)
from sportsdataverse.wexp.elo import (
    EloConfig,
    elo_ratings,
)
from sportsdataverse.wexp.oracle_market import (
    ORACLE_COLUMNS,
    build_cfb_market_oracle,
    build_nfl_market_oracle,
    cfb_market_oracle_from_lines,
    nfl_market_oracle_from_schedule,
)
from sportsdataverse.wexp.store import (
    VINTAGE_KEYS,
    VintageStore,
)
from sportsdataverse.wexp.variants import (
    AXES,
    VariantConfig,
    enumerate_variants,
    variant_hash,
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
    "AXES",
    "BASELINE_HOME_RATE",
    "EloConfig",
    "ORACLE_COLUMNS",
    "RESULT_SCHEMA",
    "VINTAGE_KEYS",
    "VariantConfig",
    "VintageStore",
    "append_results",
    "baseline_probs",
    "build_cfb_market_oracle",
    "build_nfl_market_oracle",
    "cfb_market_oracle_from_lines",
    "nfl_market_oracle_from_schedule",
    "brier_score",
    "calibration_table",
    "closing_line_value",
    "devig_multiplicative",
    "devig_shin",
    "ece",
    "elo_ratings",
    "enumerate_variants",
    "favorite_bucket_table",
    "log_loss_score",
    "logit_blend",
    "mae",
    "moneyline_pair_prob",
    "prob_from_american",
    "prob_from_decimal",
    "result_rows",
    "score_baselines",
    "spearman_corr",
    "spread_to_prob",
    "variant_hash",
    "winner_accuracy",
]
