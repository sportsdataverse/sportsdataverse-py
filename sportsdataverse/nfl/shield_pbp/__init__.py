"""Shield (api.nfl.com) driveChart -> nflverse-shape NFL play-by-play.

Graduated from nfl-data's ``native_pbp`` package (a pure move). Ports nflfastR's play
parser to polars so ``nfl_model_pbp`` can be built from the committed
``nfl/raw/{season}/{game_id}.json`` library, and so the same code can serve live games.

Modules (build order):
    game_id     -- nflverse game_id + club abbreviation helpers
    stat_ids    -- GSIS statType decode + per-play stats summation (sum_play_stats)
    parse       -- driveChart -> base play frame (down/dist/yardline/clock/posteam/...)
    repairs     -- nflfastR's hardcoded per-game repairs, scramble backfill, weird passes
    description -- play-text regex layer (pass_location, run_location, penalties)
    features    -- timeouts, score_differential, game_half, seconds, roof, era, spread join
    live        -- phase / provisional rows / current-situation row / game context
    labels      -- EP/WP/CP label sources (sp/touchdown/td_team/field_goal_result/safety/result)
    drives      -- fixed_drive / fixed_drive_result + drive_* detail columns
    series      -- series / series_result / series_success
    playstats   -- long-format play-stats table (nflverse ``play_stats``)
    build       -- build_pbp / build_pbp_from_file / build_season / shield_nfl_pbp
    box         -- Shield player/team statistics -> ESPN ``summary["boxscore"]`` (Phase 5)
    to_espn_summary -- the ESPN-summary projection of the same parsed rows, for NFLPlayProcess
"""

from __future__ import annotations

from sportsdataverse.nfl.shield_pbp.build import (
    build_pbp,
    build_pbp_from_file,
    build_season,
    shield_nfl_pbp,
)
from sportsdataverse.nfl.shield_pbp.game_id import nflverse_game_id
from sportsdataverse.nfl.shield_pbp.live import is_final
from sportsdataverse.nfl.shield_pbp.playstats import build_playstats_frame, build_playstats_season
from sportsdataverse.nfl.shield_pbp.to_espn_summary import shield_to_espn_summary

__all__ = [
    "build_pbp",
    "build_pbp_from_file",
    "build_playstats_frame",
    "build_playstats_season",
    "build_season",
    "is_final",
    "nflverse_game_id",
    "shield_nfl_pbp",
    "shield_to_espn_summary",
]
