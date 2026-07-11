from __future__ import annotations

from sportsdataverse.mlb.mlb_api import *
from sportsdataverse.mlb.mlb_api_extra import *
from sportsdataverse.mlb.mlb_api_parsers import (
    MLB_API_ENDPOINT_PARSERS,
    parse_mlb_api_list,
    parse_mlb_api_person_stats,
    parse_mlb_api_schedule,
    parse_mlb_api_standings,
    parse_mlb_api_team_roster,
    parse_mlb_api_teams,
    parser_for_mlb_api,
)
from sportsdataverse.mlb.mlb_baserunning import advancement_opportunities, mlb_baserunning_value
from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_blocking, mlb_catcher_throwing
from sportsdataverse.mlb.mlb_catcher_framing import called_strike_prob_grid, mlb_catcher_framing
from sportsdataverse.mlb.mlb_espn_ext import *
from sportsdataverse.mlb.mlb_fielding_oaa import bip_trajectory_features, catch_prob_surface, mlb_fielding_oaa
from sportsdataverse.mlb.mlb_fox_ext import *
from sportsdataverse.mlb.mlb_game_rosters import *
from sportsdataverse.mlb.mlb_loaders import *
from sportsdataverse.mlb.mlb_pbp import *
from sportsdataverse.mlb.mlb_player_stats import *
from sportsdataverse.mlb.mlb_prop_projection import mlb_prop_strikeouts, mlb_prop_team_runs, mlb_props, prop_over_prob
from sportsdataverse.mlb.mlb_run_expectancy import mlb_run_expectancy_matrix, pbp_base_out_states, run_value
from sportsdataverse.mlb.mlb_run_values import (
    RUN_VALUES,
    as_of_split,
    count_strike_run_value,
    event_run_value,
    mae,
    pearson_corr,
    spearman_corr,
)
from sportsdataverse.mlb.mlb_schedule import *
from sportsdataverse.mlb.mlb_statcast import *
from sportsdataverse.mlb.mlb_statcast_extra import (
    mlb_statcast_player,
    mlb_statcast_search,
    mlb_statcast_search_minors,
    mlb_statcast_search_wbc,
)
from sportsdataverse.mlb.mlb_statcast_parsers import (
    parse_mlb_statcast_gamefeed,
    parse_mlb_statcast_html_leaderboard,
    parse_mlb_statcast_leaderboard,
    parse_mlb_statcast_player,
    parse_mlb_statcast_schedule,
    parse_mlb_statcast_search,
)
from sportsdataverse.mlb.mlb_stolen_base import (
    mlb_stolen_base_value,
    predict_sb_success,
    sb_attempts_from_pitches,
    sb_success_surface,
)
from sportsdataverse.mlb.mlb_team_projection import (
    mlb_pythagenpat,
    mlb_pythagenpat_table,
    mlb_team_elo,
    mlb_team_projection,
)
from sportsdataverse.mlb.mlb_teams import *
from sportsdataverse.mlb.mlb_umpire_zone import fit_zone_model, mlb_umpire_bias, mlb_umpire_called_strike_prob
from sportsdataverse.mlb.mlb_win_expectancy import (
    build_we_table,
    leverage_index,
    mlb_win_expectancy,
    mlb_win_probability_added,
)

# Re-export MLB Stats API wrappers that share a name with a submodule
# (mlb_schedule, mlb_teams). The submodule imports above set the package
# attribute to the module object; re-importing the functions here restores
# them as callable names in the sportsdataverse.mlb namespace.
from sportsdataverse.mlb.mlb_api_extra import mlb_schedule, mlb_teams  # noqa: E402
