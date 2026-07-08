from __future__ import annotations

# ---------------------------------------------------------------------------
# Caching + configuration surface
# ---------------------------------------------------------------------------
# ``clear_cache``, ``get_config``, ``update_config``, and ``NflConfig`` form
# the public API for sdv-py's caching layer. The bare ``clear_cache`` name
# (no ``nfl_`` prefix) matches nflreadpy's convention so the drop-in
# replacement story is preserved. Cross-sport lift to
# ``sportsdataverse.cache`` / ``sportsdataverse.config`` is intentionally
# additive — these names will keep working.
from sportsdataverse.nfl.cache import clear_cache
from sportsdataverse.nfl.config import (
    NflConfig,
    get_config,
    reset_config,
    update_config,
)

# ---------------------------------------------------------------------------
# nflreadpy-parity static datasets
# ---------------------------------------------------------------------------
# Three module-level dicts (team abbreviation mappings + player name
# canonicalization) shipped at import time so callers can do:
#
#     from sportsdataverse.nfl import team_abbr_mapping
#     team_abbr_mapping["OAK"]  # -> "LV"
#
# Mirrors nflreadpy's ``team_abbr_mapping`` / ``team_abbr_mapping_norelocate``
# / ``player_name_mapping`` exports. See ``datasets.py`` for refresh notes.
from sportsdataverse.nfl.datasets import (
    player_name_mapping,
    team_abbr_mapping,
    team_abbr_mapping_norelocate,
)
from sportsdataverse.nfl.nfl_api import *
from sportsdataverse.nfl.nfl_espn_ext import *
from sportsdataverse.nfl.nfl_game_rosters import *
from sportsdataverse.nfl.nfl_games import *
from sportsdataverse.nfl.nfl_ngs import *

# ---------------------------------------------------------------------------
# nflreadpy parity aliases
# ---------------------------------------------------------------------------
# The canonical sdv-py names use the ``load_nfl_*`` prefix to disambiguate
# from cfb / wbb / wnba / etc. loaders that share the top-level
# ``sportsdataverse`` namespace. nflreadpy users, however, are accustomed to
# the bare ``load_*`` shape (``load_pbp``, ``load_schedules``, ...) because
# its top-level package IS ``nflreadpy``.
#
# These aliases let users do a near drop-in replacement::
#
#     import sportsdataverse.nfl as nfl
#     pbp = nfl.load_pbp([2024])           # nflreadpy-style alias
#     pbp = nfl.load_nfl_pbp([2024])       # sdv-py canonical, still works
#
# The aliases are intentionally NOT re-exported at the top-level
# ``sportsdataverse`` package — only via ``sportsdataverse.nfl.X``. That
# preserves the cross-sport disambiguation for users importing the umbrella
# package while still giving nflreadpy parity within the NFL submodule.
from sportsdataverse.nfl.nfl_loaders import *
from sportsdataverse.nfl.nfl_loaders import load_nfl_combine as load_combine
from sportsdataverse.nfl.nfl_loaders import load_nfl_contracts as load_contracts
from sportsdataverse.nfl.nfl_loaders import load_nfl_depth_charts as load_depth_charts
from sportsdataverse.nfl.nfl_loaders import load_nfl_draft_picks as load_draft_picks
from sportsdataverse.nfl.nfl_loaders import load_nfl_espn_qbr as load_espn_qbr
from sportsdataverse.nfl.nfl_loaders import (
    load_nfl_ff_opportunity as load_ff_opportunity,
)
from sportsdataverse.nfl.nfl_loaders import load_nfl_ff_playerids as load_ff_playerids
from sportsdataverse.nfl.nfl_loaders import load_nfl_ff_rankings as load_ff_rankings
from sportsdataverse.nfl.nfl_loaders import load_nfl_ftn_charting as load_ftn_charting
from sportsdataverse.nfl.nfl_loaders import load_nfl_injuries as load_injuries
from sportsdataverse.nfl.nfl_loaders import load_nfl_nextgen_stats as load_nextgen_stats
from sportsdataverse.nfl.nfl_loaders import load_nfl_officials as load_officials
from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp as load_pbp
from sportsdataverse.nfl.nfl_loaders import (
    load_nfl_pbp_participation as load_participation,
)
from sportsdataverse.nfl.nfl_loaders import load_nfl_pfr_advstats as load_pfr_advstats
from sportsdataverse.nfl.nfl_loaders import load_nfl_player_stats as load_player_stats
from sportsdataverse.nfl.nfl_loaders import load_nfl_players as load_players
from sportsdataverse.nfl.nfl_loaders import load_nfl_rosters as load_rosters
from sportsdataverse.nfl.nfl_loaders import load_nfl_schedule as load_schedules
from sportsdataverse.nfl.nfl_loaders import load_nfl_snap_counts as load_snap_counts
from sportsdataverse.nfl.nfl_loaders import load_nfl_team_stats as load_team_stats
from sportsdataverse.nfl.nfl_loaders import load_nfl_teams as load_teams
from sportsdataverse.nfl.nfl_loaders import load_nfl_trades as load_trades
from sportsdataverse.nfl.nfl_loaders import (
    load_nfl_weekly_rosters as load_rosters_weekly,
)
from sportsdataverse.nfl.ep_wp import (
    calculate_completion_probability,
    calculate_expected_points,
    calculate_win_probability,
    calculate_xpass,
    calculate_xyac,
)
from sportsdataverse.nfl.nfl_clean import clean_nfl_pbp, team_name_fn
from sportsdataverse.nfl.nfl_fourth_down import *
from sportsdataverse.nfl.nfl_ngs_tracking import (
    nfl_ngs_ryoe,
    nfl_ngs_separation_oe,
    nfl_ngs_yac_oe,
)
from sportsdataverse.nfl.nfl_pbp import *
from sportsdataverse.nfl.nfl_player_stats import *
from sportsdataverse.nfl.nfl_stats import (
    build_nfl_player_stats,
    build_nfl_player_stats_def,
    build_nfl_player_stats_kicking,
    build_nfl_team_stats,
)
from sportsdataverse.nfl.nfl_series import calculate_nfl_series_conversion_rates
from sportsdataverse.nfl.nfl_standings_calc import calculate_nfl_standings
from sportsdataverse.nfl.nfl_schedule import *
from sportsdataverse.nfl.nfl_teams import *
from sportsdataverse.nfl.utils_date import *
from sportsdataverse.nfl.utils_date import get_current_nfl_season as get_current_season
from sportsdataverse.nfl.utils_date import get_current_nfl_week as get_current_week
from sportsdataverse.nfl.nfl_build import build_nfl_season
from sportsdataverse.nfl.nfl_players import build_nfl_players, nfl_players_crosswalk
from sportsdataverse.nfl.nfl_roster_builder import build_nfl_rosters
from sportsdataverse.nfl.nfl_season_standings import nfl_season_standings
from sportsdataverse.nfl.nfl_simulations import nfl_compute_results, nfl_simulations
