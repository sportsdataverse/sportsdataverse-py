"""NBA possession-level Monte Carlo simulation (WS4).

Real ``playbyplayv3`` actions → classified possession outcomes → gamestate-
keyed PMF shelf → Node event-tree game simulation → ensemble score/total/
margin distributions + in-game win probability. See the design spec
(``ClaudeCowork/specs/2026-07-17-nba-possession-sim-design.md``) and the
module docstrings for the architecture.
"""

from __future__ import annotations

from sportsdataverse.nba.nba_possession_sim.attribution import (
    PlayerAttribution,
    TeamAttribution,
    minutes_from_gamerotation,
    simulate_player_boxscores,
)
from sportsdataverse.nba.nba_possession_sim.expanded_nodes import (
    aux_params_from_espn,
    aux_params_from_pbp,
    simulate_possession_expanded,
)
from sportsdataverse.nba.nba_possession_sim.factors import FactorAdjustment
from sportsdataverse.nba.nba_possession_sim.engine import (
    GameState,
    in_game_win_prob,
    simulate_ensemble,
    simulate_game,
    simulate_game_pbp,
)
from sportsdataverse.nba.nba_possession_sim.rules import (
    MBB_RULES,
    NBA_RULES,
    NBAGL_RULES,
    RULES_BY_LEAGUE,
    SportRules,
    WBB_RULES,
    WNBA_RULES,
)
from sportsdataverse.nba.nba_possession_sim.ensemble_frames import (
    ensemble_market_summary,
    ensemble_samples,
    player_points_long,
)
from sportsdataverse.nba.nba_possession_sim.keygen import (
    LearnedGamestateKeyer,
    clock_bin,
    fit_learned_gamestate_keyer,
    gamestate_key,
    parse_clock,
    period_bin,
    score_diff_bin,
)
from sportsdataverse.nba.nba_possession_sim.node_models import (
    fit_outcome_node_model,
    fit_rebound_node_model,
    models_to_shelf,
)
from sportsdataverse.nba.nba_possession_sim.nodes import (
    FreeThrowNode,
    OutcomeNode,
    PossessionState,
    ReboundNode,
    simulate_possession,
)
from sportsdataverse.nba.nba_possession_sim.props import (
    PropPrice,
    player_prop_distributions,
    price_board,
    price_prop,
)
from sportsdataverse.nba.nba_possession_sim.shelf import (
    OUTCOMES,
    SHOT_MIX_SPEC,
    USAGE_SPEC,
    Shelf,
    build_shelf,
    player_box_from_boxscorev3,
    player_game_logs_from_pbp,
    player_shot_mix_priors,
    player_usage_priors,
    possessions_from_pbp,
    shelf_from_parquet,
    shelf_to_parquet,
)
from sportsdataverse.nba.nba_possession_sim.wp_surface import (
    WPSurface,
    fit_wp_surface,
    held_out_calibration,
    real_path_snapshots,
    simulate_score_paths,
)

__all__ = [
    "MBB_RULES",
    "NBAGL_RULES",
    "NBA_RULES",
    "OUTCOMES",
    "RULES_BY_LEAGUE",
    "SHOT_MIX_SPEC",
    "USAGE_SPEC",
    "FactorAdjustment",
    "FreeThrowNode",
    "GameState",
    "OutcomeNode",
    "PlayerAttribution",
    "PossessionState",
    "ReboundNode",
    "Shelf",
    "SportRules",
    "TeamAttribution",
    "WBB_RULES",
    "WNBA_RULES",
    "build_shelf",
    "aux_params_from_espn",
    "aux_params_from_pbp",
    "clock_bin",
    "fit_outcome_node_model",
    "fit_rebound_node_model",
    "gamestate_key",
    "in_game_win_prob",
    "minutes_from_gamerotation",
    "models_to_shelf",
    "parse_clock",
    "period_bin",
    "PropPrice",
    "player_prop_distributions",
    "price_board",
    "price_prop",
    "player_box_from_boxscorev3",
    "player_game_logs_from_pbp",
    "player_shot_mix_priors",
    "player_usage_priors",
    "possessions_from_pbp",
    "score_diff_bin",
    "shelf_from_parquet",
    "shelf_to_parquet",
    "simulate_ensemble",
    "simulate_game",
    "simulate_game_pbp",
    "simulate_player_boxscores",
    "simulate_possession",
    "simulate_possession_expanded",
    "WPSurface",
    "fit_wp_surface",
    "held_out_calibration",
    "real_path_snapshots",
    "simulate_score_paths",
    "LearnedGamestateKeyer",
    "fit_learned_gamestate_keyer",
    "ensemble_samples",
    "ensemble_market_summary",
    "player_points_long",
]
