from __future__ import annotations

import logging

logger = logging.getLogger("sdv.dl_utils")
logger.addHandler(logging.NullHandler())


SGITHUB = "https://raw.githubusercontent.com/sportsdataverse/"
SDVRELEASES = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"

CFB_BASE_URL = SDVRELEASES + "espn_cfb_pbp/play_by_play_{season}.parquet"
CFB_ROSTER_URL = SGITHUB + "cfbfastR-data/main/rosters/parquet/cfb_rosters_{season}.parquet"
CFB_TEAM_LOGO_URL = f"{SGITHUB}cfbfastR-data/main/teams/teams_colors_logos.parquet"
CFB_TEAM_SCHEDULE_URL = SDVRELEASES + "cfb_schedules/cfb_schedules_{season}.parquet"
CFB_TEAM_INFO_URL = SGITHUB + "cfbfastR-data/main/team_info/parquet/cfb_team_info_{season}.parquet"
CFB_BETTING_LINES_URL = f"{SGITHUB}cfbfastR-data/main/betting/parquet/cfb_line_odds.parquet"
# Rosters crosswalk is a single current snapshot (ESPN/Fox roster endpoints are
# current-only), not a per-season historical asset -- hence no {season} token.
CFB_ROSTERS_CROSSWALK_URL = f"{SDVRELEASES}cfb_crosswalk/cfb_rosters_crosswalk.parquet"

NHL_BASE_URL = SDVRELEASES + "nhl_pbp_full/play_by_play_{season}.parquet"
NHL_PLAYER_BOX_URL = SDVRELEASES + "nhl_player_boxscores/player_box_{season}.parquet"
NHL_TEAM_BOX_URL = SDVRELEASES + "nhl_team_boxscores/team_box_{season}.parquet"
NHL_TEAM_SCHEDULE_URL = SDVRELEASES + "nhl_schedules/nhl_schedule_{season}.parquet"
NHL_TEAM_LOGO_URL = f"{SGITHUB}fastRhockey-data/main/nhl_teams_colors_logos.csv"

# PHF (Premier Hockey Federation) is a frozen dataset -- the league ceased
# operations in June 2023. Assets were migrated from the fastRhockey-data git
# tree into dedicated release tags (2026-07-11): seasons 2016-2023, except pbp
# which covers 2016 + 2020-2023 (2017-2019 pbp was never published upstream).
PHF_BASE_URL = SDVRELEASES + "phf_pbp/play_by_play_{season}.parquet"
PHF_PLAYER_BOX_URL = SDVRELEASES + "phf_player_boxscores/player_box_{season}.parquet"
PHF_TEAM_BOX_URL = SDVRELEASES + "phf_team_boxscores/team_box_{season}.parquet"
PHF_TEAM_SCHEDULE_URL = SDVRELEASES + "phf_schedules/phf_schedule_{season}.parquet"

MBB_BASE_URL = SDVRELEASES + "espn_mens_college_basketball_pbp/play_by_play_{season}.parquet"
MBB_TEAM_BOX_URL = SDVRELEASES + "espn_mens_college_basketball_team_boxscores/team_box_{season}.parquet"
MBB_PLAYER_BOX_URL = SDVRELEASES + "espn_mens_college_basketball_player_boxscores/player_box_{season}.parquet"
MBB_TEAM_SCHEDULE_URL = SDVRELEASES + "espn_mens_college_basketball_schedules/mbb_schedule_{season}.parquet"

NBA_BASE_URL = SDVRELEASES + "espn_nba_pbp/play_by_play_{season}.parquet"
NBA_TEAM_BOX_URL = SDVRELEASES + "espn_nba_team_boxscores/team_box_{season}.parquet"
NBA_PLAYER_BOX_URL = SDVRELEASES + "espn_nba_player_boxscores/player_box_{season}.parquet"
NBA_TEAM_SCHEDULE_URL = SDVRELEASES + "espn_nba_schedules/nba_schedule_{season}.parquet"

WBB_BASE_URL = SDVRELEASES + "espn_womens_college_basketball_pbp/play_by_play_{season}.parquet"
WBB_TEAM_BOX_URL = SDVRELEASES + "espn_womens_college_basketball_team_boxscores/team_box_{season}.parquet"
WBB_PLAYER_BOX_URL = SDVRELEASES + "espn_womens_college_basketball_player_boxscores/player_box_{season}.parquet"
WBB_TEAM_SCHEDULE_URL = SDVRELEASES + "espn_womens_college_basketball_schedules/wbb_schedule_{season}.parquet"

WNBA_BASE_URL = SDVRELEASES + "espn_wnba_pbp/play_by_play_{season}.parquet"
WNBA_TEAM_BOX_URL = SDVRELEASES + "espn_wnba_team_boxscores/team_box_{season}.parquet"
WNBA_PLAYER_BOX_URL = SDVRELEASES + "espn_wnba_player_boxscores/player_box_{season}.parquet"
WNBA_TEAM_SCHEDULE_URL = SDVRELEASES + "espn_wnba_schedules/wnba_schedule_{season}.parquet"


NFLVERSEGITHUB = "https://github.com/nflverse/nflverse-data/releases/download/"
NFLVERSEGITHUBPBP = "https://raw.githubusercontent.com/nflverse/"
DYNASTYPROCESSGITHUB = "https://github.com/dynastyprocess/data/raw/master/files/"
FFOPPORTUNITYGITHUB = "https://github.com/ffverse/ffopportunity/releases/download/"
NFL_BASE_URL = NFLVERSEGITHUB + "pbp/play_by_play_{season}.parquet"  # done
NFL_MODEL_PBP_URL = SDVRELEASES + "nfl_model_pbp/model_pbp_{season}.parquet"
NFL_PLAYER_URL = f"{NFLVERSEGITHUB}players/players.parquet"
NFL_SDV_PLAYER_URL = SDVRELEASES + "nfl_players/players.parquet"
NFL_PLAYER_STATS_URL = f"{NFLVERSEGITHUB}player_stats/player_stats.parquet"
NFL_SDV_PLAYER_STATS_URL = SDVRELEASES + "nfl_player_stats/player_stats.parquet"
NFL_PLAYER_KICKING_STATS_URL = f"{NFLVERSEGITHUB}player_stats/player_stats_kicking.parquet"
NFL_PFR_SEASON_DEF_URL = f"{NFLVERSEGITHUB}pfr_advstats/advstats_season_def.parquet"
NFL_PFR_WEEK_DEF_URL = NFLVERSEGITHUB + "pfr_advstats/advstats_week_def_{season}.parquet"
NFL_PFR_SEASON_PASS_URL = f"{NFLVERSEGITHUB}pfr_advstats/advstats_season_pass.parquet"
NFL_PFR_WEEK_PASS_URL = NFLVERSEGITHUB + "pfr_advstats/advstats_week_pass_{season}.parquet"
NFL_PFR_SEASON_REC_URL = f"{NFLVERSEGITHUB}pfr_advstats/advstats_season_rec.parquet"
NFL_PFR_WEEK_REC_URL = NFLVERSEGITHUB + "pfr_advstats/advstats_week_rec_{season}.parquet"
NFL_PFR_SEASON_RUSH_URL = f"{NFLVERSEGITHUB}pfr_advstats/advstats_season_rush.parquet"
NFL_PFR_WEEK_RUSH_URL = NFLVERSEGITHUB + "pfr_advstats/advstats_week_rush_{season}.parquet"
NFL_NGS_RUSHING_URL = f"{NFLVERSEGITHUB}nextgen_stats/ngs_rushing.parquet"
NFL_NGS_PASSING_URL = f"{NFLVERSEGITHUB}nextgen_stats/ngs_passing.parquet"
NFL_NGS_RECEIVING_URL = f"{NFLVERSEGITHUB}nextgen_stats/ngs_receiving.parquet"
NFL_ROSTER_URL = NFLVERSEGITHUB + "rosters/roster_{season}.parquet"  # done
NFL_SDV_ROSTER_URL = SDVRELEASES + "nfl_rosters/roster_{season}.parquet"
NFL_WEEKLY_ROSTER_URL = NFLVERSEGITHUB + "weekly_rosters/roster_weekly_{season}.parquet"  # done
NFL_SNAP_COUNTS_URL = NFLVERSEGITHUB + "snap_counts/snap_counts_{season}.parquet"
NFL_PBP_PARTICIPATION_URL = NFLVERSEGITHUB + "pbp_participation/pbp_participation_{season}.parquet"
NFL_CONTRACTS_URL = f"{NFLVERSEGITHUB}contracts/historical_contracts.parquet"
NFL_DRAFT_PICKS_URL = f"{NFLVERSEGITHUB}draft_picks/draft_picks.parquet"
NFL_COMBINE_URL = f"{NFLVERSEGITHUB}combine/combine.parquet"
NFL_INJURIES_URL = NFLVERSEGITHUB + "injuries/injuries_{season}.parquet"
NFL_DEPTH_CHARTS_URL = NFLVERSEGITHUB + "depth_charts/depth_charts_{season}.parquet"
NFL_OFFICIALS_URL = f"{NFLVERSEGITHUB}officials/officials.parquet"
NFL_TEAM_LOGO_URL = f"{NFLVERSEGITHUBPBP}nflverse-pbp/master/teams_colors_logos.csv"
# Single combined parquet covering all seasons (1999-present); filtered post-load by `season`.
# Mirrors nflreadpy's `schedules/games` asset.
NFL_TEAM_SCHEDULE_URL = f"{NFLVERSEGITHUB}schedules/games.parquet"

# nflreadpy parity additions: coverage gaps the canonical loaders fill
NFL_TEAM_STATS_URL = NFLVERSEGITHUB + "stats_team/stats_team_{level}_{season}.parquet"
NFL_SDV_TEAM_STATS_URL = SDVRELEASES + "nfl_team_stats/team_stats.parquet"
# ESPN Total QBR. nflverse republishes ESPN's QBR through the espn_data release as
# two combined files (all seasons, 2006-present), one per summary_type. Mirrors
# nflreadr::load_espn_qbr; filtered post-load by `season`.
NFL_ESPN_QBR_SEASON_URL = f"{NFLVERSEGITHUB}espn_data/qbr_season_level.parquet"
NFL_ESPN_QBR_WEEK_URL = f"{NFLVERSEGITHUB}espn_data/qbr_week_level.parquet"
# SDV-native QBR (forthcoming `nfl_espn_qbr` release; see CFB<->NFL parity backlog).
NFL_SDV_ESPN_QBR_SEASON_URL = SDVRELEASES + "nfl_espn_qbr/qbr_season_level.parquet"
NFL_SDV_ESPN_QBR_WEEK_URL = SDVRELEASES + "nfl_espn_qbr/qbr_week_level.parquet"
NFL_RATINGS_WEEKLY_URL = SDVRELEASES + "nfl_ratings_weekly/nfl_ratings_weekly_{season}.parquet"
NFL_FTN_CHARTING_URL = NFLVERSEGITHUB + "ftn_charting/ftn_charting_{season}.parquet"
NFL_TRADES_URL = f"{NFLVERSEGITHUB}trades/trades.parquet"
NFL_FF_PLAYERIDS_URL = f"{DYNASTYPROCESSGITHUB}db_playerids.csv"
NFL_FF_RANKINGS_DRAFT_URL = f"{DYNASTYPROCESSGITHUB}db_fpecr_latest.csv"
NFL_FF_RANKINGS_WEEK_URL = f"{DYNASTYPROCESSGITHUB}fp_latest_weekly.csv"
NFL_FF_RANKINGS_ALL_URL = f"{DYNASTYPROCESSGITHUB}db_fpecr.parquet"
NFL_FF_OPPORTUNITY_URL = FFOPPORTUNITYGITHUB + "{model_version}-data/ep_{stat_type}_{season}.parquet"
