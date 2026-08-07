"""Win-expectancy bake-off harness (NFL + CFB).

Vintage-keyed feature store, market oracles, scoring, baseline oracles,
axis engines, the walk-forward backtest driver, and post-game deserved-win
estimation. Committed tune-window leaderboards live under
``results/wexp/`` on the development branch.
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
from sportsdataverse.wexp.backtest import (
    OUTCOME_COLUMNS,
    POSTSEASON_WEEK_OFFSET,
    elo_predictor,
    run_backtest,
)
from sportsdataverse.wexp.elo import (
    EloConfig,
    elo_ratings,
)
from sportsdataverse.wexp.engines import (
    GSConfig,
    build_predictor,
    cfb_continuity_shifts,
    cfb_drive_deltas,
    cfb_drive_ep_responses,
    glickman_stern_predictor,
    net_vintages_view,
    ratings_predictor,
    response_ridge_vintages,
    ridge_margin_vintages,
)
from sportsdataverse.wexp.features import (
    carry_forward_weights,
    sos_sor_vintages,
)
from sportsdataverse.wexp.postgame import (
    postgame_we,
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
    score_probs,
    spearman_corr,
    winner_accuracy,
)

__all__ = [
    "AXES",
    "BASELINE_HOME_RATE",
    "EloConfig",
    "GSConfig",
    "ORACLE_COLUMNS",
    "OUTCOME_COLUMNS",
    "POSTSEASON_WEEK_OFFSET",
    "RESULT_SCHEMA",
    "VINTAGE_KEYS",
    "VariantConfig",
    "VintageStore",
    "append_results",
    "baseline_probs",
    "build_cfb_market_oracle",
    "build_nfl_market_oracle",
    "build_predictor",
    "cfb_continuity_shifts",
    "cfb_drive_deltas",
    "cfb_drive_ep_responses",
    "cfb_market_oracle_from_lines",
    "nfl_market_oracle_from_schedule",
    "brier_score",
    "calibration_table",
    "carry_forward_weights",
    "closing_line_value",
    "devig_multiplicative",
    "devig_shin",
    "ece",
    "elo_predictor",
    "elo_ratings",
    "enumerate_variants",
    "favorite_bucket_table",
    "glickman_stern_predictor",
    "log_loss_score",
    "logit_blend",
    "mae",
    "moneyline_pair_prob",
    "net_vintages_view",
    "postgame_we",
    "prob_from_american",
    "prob_from_decimal",
    "ratings_predictor",
    "ridge_margin_vintages",
    "response_ridge_vintages",
    "result_rows",
    "run_backtest",
    "score_baselines",
    "sos_sor_vintages",
    "score_probs",
    "spearman_corr",
    "spread_to_prob",
    "variant_hash",
    "winner_accuracy",
]
