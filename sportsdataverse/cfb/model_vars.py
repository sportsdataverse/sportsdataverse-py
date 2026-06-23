ep_class_to_score_mapping = {0: 7, 1: -7, 2: 3, 3: -3, 4: 2, 5: -2, 6: 0}

wp_start_touchback_columns = [
    "start.pos_team_receives_2H_kickoff",
    "start.spread_time",
    "start.TimeSecsRem",
    "start.adj_TimeSecsRem",
    "start.ExpScoreDiff_Time_Ratio_touchback",
    "pos_score_diff_start",
    "start.down",
    "start.distance",
    "start.yardsToEndzone.touchback",
    "start.is_home",
    "start.posTeamTimeouts",
    "start.defPosTeamTimeouts",
    "period",
]
wp_start_columns = [
    "start.pos_team_receives_2H_kickoff",
    "start.spread_time",
    "start.TimeSecsRem",
    "start.adj_TimeSecsRem",
    "start.ExpScoreDiff_Time_Ratio",
    "pos_score_diff_start",
    "start.down",
    "start.distance",
    "start.yardsToEndzone",
    "start.is_home",
    "start.posTeamTimeouts",
    "start.defPosTeamTimeouts",
    "period",
]
wp_end_columns = [
    "end.pos_team_receives_2H_kickoff",
    "end.spread_time",
    "end.TimeSecsRem",
    "end.adj_TimeSecsRem",
    "end.ExpScoreDiff_Time_Ratio",
    "end.pos_score_diff",
    "end.down",
    "end.distance",
    "end.yardsToEndzone",
    "end.is_home",
    "end.posTeamTimeouts",
    "end.defPosTeamTimeouts",
    "period",
]

ep_start_touchback_columns = [
    "start.TimeSecsRem",
    "start.yardsToEndzone.touchback",
    "distance",
    "down_1",
    "down_2",
    "down_3",
    "down_4",
    "pos_score_diff_start",
]
ep_start_columns = [
    "start.TimeSecsRem",
    "start.yardsToEndzone",
    "start.distance",
    "down_1",
    "down_2",
    "down_3",
    "down_4",
    "pos_score_diff_start",
]
ep_end_columns = [
    "end.TimeSecsRem",
    "end.yardsToEndzone",
    "end.distance",
    "down_1_end",
    "down_2_end",
    "down_3_end",
    "down_4_end",
    "pos_score_diff_end",
]

ep_final_names = [
    "TimeSecsRem",
    "yards_to_goal",
    "distance",
    "down_1",
    "down_2",
    "down_3",
    "down_4",
    "pos_score_diff_start",
]
wp_final_names = [
    "pos_team_receives_2H_kickoff",
    "spread_time",
    "TimeSecsRem",
    "adj_TimeSecsRem",
    "ExpScoreDiff_Time_Ratio",
    "pos_score_diff_start",
    "down",
    "distance",
    "yards_to_goal",
    "is_home",
    "pos_team_timeouts_rem_before",
    "def_pos_team_timeouts_rem_before",
    "period",
]

# Naive (spread-free) WP feature set = spread WP minus the ``spread_time``
# feature (12-feat). The column-source lists below are positionally aligned to
# ``wp_final_names``, so the naive analogues are derived by dropping the single
# ``spread_time`` slot (index 1) from each list — keeping every other column in
# place. This mirrors the cfbscrapR naive recipe and the bundled ``wp_naive.ubj``.
_spread_time_idx = wp_final_names.index("spread_time")
wp_naive_final_names = [c for i, c in enumerate(wp_final_names) if i != _spread_time_idx]
wp_naive_start_touchback_columns = [c for i, c in enumerate(wp_start_touchback_columns) if i != _spread_time_idx]
wp_naive_start_columns = [c for i, c in enumerate(wp_start_columns) if i != _spread_time_idx]
wp_naive_end_columns = [c for i, c in enumerate(wp_end_columns) if i != _spread_time_idx]

# -------Play type vectors-------------
scores_vec = [
    "Blocked Punt Touchdown",
    "Blocked Punt (Safety)",
    "Punt (Safety)",
    "Blocked Field Goal Touchdown",
    "Missed Field Goal Return Touchdown",
    "Fumble Recovery (Opponent) Touchdown",
    "Fumble Return Touchdown",
    "Interception Return Touchdown",
    "Pass Interception Return Touchdown",
    "Punt Touchdown",
    "Punt Return Touchdown",
    "Sack Touchdown",
    "Uncategorized Touchdown",
    "Defensive 2pt Conversion",
    "Uncategorized",
    "Two Point Rush",
    "Safety",
    "Penalty (Safety)",
    "Punt Team Fumble Recovery Touchdown",
    "Kickoff Team Fumble Recovery Touchdown",
    "Kickoff (Safety)",
    "Passing Touchdown",
    "Rushing Touchdown",
    "Field Goal Good",
    "Pass Reception Touchdown",
    "Fumble Recovery (Own) Touchdown",
]
defense_score_vec = [
    "Blocked Punt Touchdown",
    "Blocked Field Goal Touchdown",
    "Missed Field Goal Return Touchdown",
    "Punt Return Touchdown",
    "Fumble Recovery (Opponent) Touchdown",
    "Fumble Return Touchdown",
    "Kickoff Touchdown",  # <--- Kickoff Team recovers the return team fumble and scores
    "Defensive 2pt Conversion",
    "Safety",
    "Sack Touchdown",
    "Interception Return Touchdown",
    "Pass Interception Return Touchdown",
    "Uncategorized Touchdown",
]
turnover_vec = [
    "Blocked Field Goal",
    "Blocked Field Goal Touchdown",
    "Blocked Punt",
    "Blocked Punt Touchdown",
    "Field Goal Missed",
    "Missed Field Goal Return",
    "Missed Field Goal Return Touchdown",
    "Fumble Recovery (Opponent)",
    "Fumble Recovery (Opponent) Touchdown",
    "Fumble Return Touchdown",
    "Defensive 2pt Conversion",
    "Interception",
    "Interception Return",
    "Interception Return Touchdown",
    "Pass Interception Return",
    "Pass Interception Return Touchdown",
    "Kickoff Team Fumble Recovery",
    "Kickoff Team Fumble Recovery Touchdown",
    "Punt Touchdown",
    "Punt Return Touchdown",
    "Sack Touchdown",
    "Uncategorized Touchdown",
]
normalplay = [
    "Rush",
    "Pass",
    "Pass Reception",
    "Pass Incompletion",
    "Pass Completion",
    "Sack",
    "Fumble Recovery (Own)",
]
penalty = ["Penalty", "Penalty (Kickoff)", "Penalty (Safety)"]
offense_score_vec = [
    "Passing Touchdown",
    "Rushing Touchdown",
    "Field Goal Good",
    "Pass Reception Touchdown",
    "Fumble Recovery (Own) Touchdown",
    "Punt Touchdown",  # <--- Punting Team recovers the return team fumble and scores
    "Punt Team Fumble Recovery Touchdown",
    "Kickoff Return Touchdown",
    "Kickoff Team Fumble Recovery Touchdown",
]
punt_vec = [
    "Blocked Punt",
    "Blocked Punt Touchdown",
    "Blocked Punt (Safety)",
    "Punt (Safety)",
    "Punt",
    "Punt Return",
    "Punt Touchdown",
    "Punt Team Fumble Recovery",
    "Punt Team Fumble Recovery Touchdown",
    "Punt Return Touchdown",
]
kickoff_vec = [
    "Kickoff",
    "Kickoff Return (Offense)",
    "Kickoff Return Touchdown",
    "Kickoff Touchdown",
    "Kickoff Team Fumble Recovery",
    "Kickoff Team Fumble Recovery Touchdown",
    "Kickoff (Safety)",
    "Penalty (Kickoff)",
]
int_vec = [
    "Interception",
    "Interception Return",
    "Interception Return Touchdown",
    "Pass Interception",
    "Pass Interception Return",
    "Pass Interception Return Touchdown",
]
end_change_vec = [
    "Blocked Field Goal",
    "Blocked Field Goal Touchdown",
    "Field Goal Missed",
    "Missed Field Goal Return",
    "Missed Field Goal Return Touchdown",
    "Blocked Punt",
    "Blocked Punt Touchdown",
    "Punt",
    "Punt Return",
    "Punt Touchdown",
    "Punt Return Touchdown",
    "Kickoff Team Fumble Recovery",
    "Kickoff Team Fumble Recovery Touchdown",
    "Fumble Recovery (Opponent)",
    "Fumble Recovery (Opponent) Touchdown",
    "Fumble Return Touchdown",
    "Sack Touchdown",
    "Defensive 2pt Conversion",
    "Interception",
    "Interception Return",
    "Interception Return Touchdown",
    "Pass Interception Return",
    "Pass Interception Return Touchdown",
    "Uncategorized Touchdown",
]
kickoff_turnovers = ["Kickoff Team Fumble Recovery", "Kickoff Team Fumble Recovery Touchdown"]

# QBR model features. The base 6 EPA/spread aggregates plus the one-hot rule-era
# dummies (era0..era3, cuts 2006/2013/2020 -> 2004-06 / 2007-13 / 2014-20 / 2021+),
# which materially improved QBR out-of-fold (LOSO RMSE 17.9 -> 17.4). Order MUST match
# the trained qbr_model.ubj feature_names.
qbr_vars = ["qbr_epa", "sack_epa", "pass_epa", "rush_epa", "pen_epa", "spread", "era0", "era1", "era2", "era3"]
