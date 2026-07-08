from __future__ import annotations

from sportsdataverse.nhl.nhl_api_web import *
from sportsdataverse.nhl.nhl_api_web_extra import *
from sportsdataverse.nhl.nhl_api_web_parsers import (
    NHL_API_WEB_ENDPOINT_PARSERS,
    parse_nhl_web_boxscore,
    parse_nhl_web_club_schedule,
    parse_nhl_web_club_stats,
    parse_nhl_web_draft_picks,
    parse_nhl_web_landing,
    parse_nhl_web_leaders,
    parse_nhl_web_pbp,
    parse_nhl_web_player_game_log,
    parse_nhl_web_player_landing,
    parse_nhl_web_right_rail,
    parse_nhl_web_roster,
    parse_nhl_web_schedule,
    parse_nhl_web_score,
    parse_nhl_web_scoreboard,
    parse_nhl_web_standings,
    parse_nhl_web_standings_season,
    parser_for_nhl_api_web,
)
from sportsdataverse.nhl.nhl_edge import *
from sportsdataverse.nhl.nhl_edge_parsers import (
    EDGE_ENDPOINT_PARSERS,
    EDGE_SUBFRAME_PARSERS,
    parse_edge_detail,
    parse_edge_hardest_shots,
    parse_edge_payload,
    parse_edge_shot_location,
    parse_edge_sog_details,
    parse_edge_sog_summary,
    parse_edge_top10,
    parse_edge_zone_time,
    parser_for_edge,
)
from sportsdataverse.nhl.nhl_records_parsers import (
    parse_nhl_records,
    parser_for_nhl_records,
)
from sportsdataverse.nhl.nhl_stats_rest import *
from sportsdataverse.nhl.nhl_stats_rest_parsers import (
    NHL_STATS_REST_ENDPOINT_PARSERS,
    parse_nhl_stats_rest,
    parser_for_nhl_stats_rest,
)
from sportsdataverse.nhl.nhl_game_rosters import *
from sportsdataverse.nhl.nhl_loaders import *
from sportsdataverse.nhl.nhl_loaders_extra import *
from sportsdataverse.nhl.nhl_pbp import *
from sportsdataverse.nhl.nhl_records import *
from sportsdataverse.nhl.nhl_records_extra import *
from sportsdataverse.nhl.nhl_schedule import *
from sportsdataverse.nhl.nhl_teams import *
from sportsdataverse.nhl.nhl_espn_ext import *
from sportsdataverse.nhl.nhl_fox_ext import *
from sportsdataverse.nhl.nhl_player_stats import *
from sportsdataverse.nhl.nhl_player_impact_constants import *
from sportsdataverse.nhl.nhl_xg import *
from sportsdataverse.nhl.nhl_gsax import *
