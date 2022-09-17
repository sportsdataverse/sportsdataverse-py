SGITHUB = 'https://raw.githubusercontent.com/sportsdataverse/'
NFLVERSEGITHUB='https://github.com/nflverse/nflverse-data/releases/download/'
NFLVERSEGITHUBPBP='https://raw.githubusercontent.com/nflverse/'
CFB_BASE_URL = SGITHUB+'cfbfastR-data/main/pbp/parquet/play_by_play_{season}.parquet'
CFB_ROSTER_URL = SGITHUB+'cfbfastR-data/main/rosters/parquet/rosters_{season}.parquet'
CFB_TEAM_LOGO_URL  = SGITHUB+'cfbfastR-data/main/teams/teams_colors_logos.parquet'
CFB_TEAM_SCHEDULE_URL = SGITHUB+'cfbfastR-data/main/schedules/parquet/schedules_{season}.parquet'
CFB_TEAM_INFO_URL = SGITHUB+'cfbfastR-data/main/team_info/parquet/team_info_{season}.parquet'

NFL_BASE_URL = NFLVERSEGITHUB+'pbp/play_by_play_{season}.parquet' #done
NFL_PLAYER_URL = NFLVERSEGITHUB+'players/players.parquet' #done
NFL_PLAYER_STATS_URL = NFLVERSEGITHUB+'player_stats/player_stats.parquet'  #done
NFL_PLAYER_KICKING_STATS_URL = NFLVERSEGITHUB+'player_stats/player_stats_kicking.parquet'  #done
NFL_PFR_SEASON_DEF_URL = NFLVERSEGITHUB+'pfr_advstats/advstats_season_def.parquet'
NFL_PFR_WEEK_DEF_URL = NFLVERSEGITHUB+'pfr_advstats/advstats_week_def_{season}.parquet'
NFL_PFR_SEASON_PASS_URL = NFLVERSEGITHUB+'pfr_advstats/advstats_season_pass.parquet'
NFL_PFR_WEEK_PASS_URL = NFLVERSEGITHUB+'pfr_advstats/advstats_week_pass_{season}.parquet'
NFL_PFR_SEASON_REC_URL = NFLVERSEGITHUB+'pfr_advstats/advstats_season_rec.parquet'
NFL_PFR_WEEK_REC_URL = NFLVERSEGITHUB+'pfr_advstats/advstats_week_rec_{season}.parquet'
NFL_PFR_SEASON_RUSH_URL = NFLVERSEGITHUB+'pfr_advstats/advstats_season_rush.parquet'
NFL_PFR_WEEK_RUSH_URL = NFLVERSEGITHUB+'pfr_advstats/advstats_week_rush_{season}.parquet'
NFL_NGS_RUSHING_URL = NFLVERSEGITHUB+'nextgen_stats/ngs_rushing.parquet'
NFL_NGS_PASSING_URL = NFLVERSEGITHUB+'nextgen_stats/ngs_passing.parquet'
NFL_NGS_RECEIVING_URL = NFLVERSEGITHUB+'nextgen_stats/ngs_receiving.parquet'
NFL_ROSTER_URL = NFLVERSEGITHUB+'rosters/roster_{season}.parquet'  #done
NFL_WEEKLY_ROSTER_URL = NFLVERSEGITHUB+'weekly_rosters/roster_weekly_{season}.parquet'  #done
NFL_SNAP_COUNTS_URL = NFLVERSEGITHUB+'snap_counts/snap_counts_{season}.parquet'
NFL_PBP_PARTICIPATION_URL = NFLVERSEGITHUB+'pbp_participation/pbp_participation_{season}.parquet'
NFL_CONTRACTS_URL = NFLVERSEGITHUB+'contracts/historical_contracts.parquet'
NFL_OTC_PLAYER_DETAILS_URL = NFLVERSEGITHUB+'contracts/otc_player_details.rds'
NFL_DRAFT_PICKS_URL = NFLVERSEGITHUB+'draft_picks/draft_picks.parquet'
NFL_COMBINE_URL = NFLVERSEGITHUB+'combine/combine.parquet'
NFL_INJURIES_URL = NFLVERSEGITHUB+'injuries/injuries_{season}.parquet'
NFL_DEPTH_CHARTS_URL = NFLVERSEGITHUB+'depth_charts/depth_charts_{season}.parquet'
NFL_OFFICIALS_URL = NFLVERSEGITHUB+'officials/officials.parquet'
NFL_TEAM_LOGO_URL  = NFLVERSEGITHUBPBP+'nflverse-pbp/master/teams_colors_logos.csv'
NFL_TEAM_SCHEDULE_URL = NFLVERSEGITHUBPBP+'nflverse-pbp/master/schedules/sched_{season}.rds'

NHL_BASE_URL = SGITHUB+'fastRhockey-data/main/nhl/pbp/parquet/play_by_play_{season}.parquet'
NHL_PLAYER_BOX_URL = SGITHUB+'fastRhockey-data/main/nhl/player_box/parquet/player_box_{season}.parquet'
NHL_TEAM_BOX_URL = SGITHUB+'fastRhockey-data/main/nhl/team_box/parquet/team_box_{season}.parquet'
NHL_TEAM_SCHEDULE_URL = SGITHUB+'fastRhockey-data/main/nhl/schedules/parquet/nhl_schedule_{season}.parquet'
NHL_TEAM_LOGO_URL  = SGITHUB+'fastRhockey-data/main/nhl/nhl_teams_colors_logos.csv'

PHF_BASE_URL = SGITHUB+'fastRhockey-data/main/phf/pbp/parquet/play_by_play_{season}.parquet'
PHF_PLAYER_BOX_URL = SGITHUB+'fastRhockey-data/main/phf/player_box/parquet/player_box_{season}.parquet'
PHF_TEAM_BOX_URL = SGITHUB+'fastRhockey-data/main/phf/team_box/parquet/team_box_{season}.parquet'
PHF_TEAM_SCHEDULE_URL = SGITHUB+'fastRhockey-data/main/phf/schedules/parquet/phf_schedule_{season}.parquet'

MBB_BASE_URL = SGITHUB+'hoopR-data/master/mbb/pbp/parquet/play_by_play_{season}.parquet'
MBB_TEAM_BOX_URL = SGITHUB+'hoopR-data/master/mbb/team_box/parquet/team_box_{season}.parquet'
MBB_PLAYER_BOX_URL = SGITHUB+'hoopR-data/master/mbb/player_box/parquet/player_box_{season}.parquet'
MBB_TEAM_LOGO_URL  = SGITHUB+'hoopR-data/master/mbb/teams_colors_logos.csv'
MBB_TEAM_SCHEDULE_URL = SGITHUB+'hoopR-data/master/mbb/schedules/parquet/mbb_schedule_{season}.parquet'

NBA_BASE_URL = SGITHUB+'hoopR-data/master/nba/pbp/parquet/play_by_play_{season}.parquet'
NBA_TEAM_BOX_URL = SGITHUB+'hoopR-data/master/nba/team_box/parquet/team_box_{season}.parquet'
NBA_PLAYER_BOX_URL  = SGITHUB+'hoopR-data/master/nba/player_box/parquet/player_box_{season}.parquet'
NBA_TEAM_SCHEDULE_URL = SGITHUB+'hoopR-data/master/nba/schedules/parquet/nba_schedule_{season}.parquet'

WBB_BASE_URL = SGITHUB+'wehoop-data/master/wbb/pbp/parquet/play_by_play_{season}.parquet'
WBB_TEAM_BOX_URL = SGITHUB+'wehoop-data/master/wbb/team_box/parquet/team_box_{season}.parquet'
WBB_PLAYER_BOX_URL = SGITHUB+'wehoop-data/master/wbb/player_box/parquet/player_box_{season}.parquet'
WBB_TEAM_LOGO_URL  = SGITHUB+'wehoop-data/master/wbb/teams_colors_logos.csv'
WBB_TEAM_SCHEDULE_URL = SGITHUB+'wehoop-data/master/wbb/schedules/parquet/wbb_schedule_{season}.parquet'

WNBA_BASE_URL = SGITHUB+'wehoop-data/master/wnba/pbp/parquet/play_by_play_{season}.parquet'
WNBA_TEAM_BOX_URL = SGITHUB+'wehoop-data/master/wnba/team_box/parquet/team_box_{season}.parquet'
WNBA_PLAYER_BOX_URL  = SGITHUB+'wehoop-data/master/wnba/player_box/parquet/player_box_{season}.parquet'
WNBA_TEAM_SCHEDULE_URL = SGITHUB+'wehoop-data/master/wnba/schedules/parquet/wnba_schedule_{season}.parquet'

