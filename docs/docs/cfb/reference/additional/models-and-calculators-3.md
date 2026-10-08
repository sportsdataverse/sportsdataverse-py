---
title: "CFB — additional Python functions — Models and calculators: normalize_pbp–win_prob"
sidebar_label: "Models and calculators: normalize_pbp–win_prob"
sidebar_position: 9
description: "CFB — additional Python functions — Models and calculators: normalize_pbp–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Models and calculators: normalize_pbp–win_prob

### normalize_pbp_columns {#normalize_pbp_columns}

`normalize_pbp_columns(df: 'pl.DataFrame', model: 'str') -> 'pl.DataFrame'`

Add card-named copies of any play-by-play columns `df` already carries.

A hand-built frame using the card's own names passes through untouched; a
pbp frame gains the names the card asks for. Copies rather than renames, so
nothing the caller passed in is removed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Caller's frame. |
| `model` | `str` |  | Bundle stem, used to look up which features are wanted. |

**Returns**

`df` plus any alias columns that could be resolved.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season (4-digit year). |
| `game_id` | integer | ESPN game identifier. |
| `game_play_number` | integer | Sequential play number within the game (excludes timeouts/end markers). |
| `pos_team_id` | integer |  |
| `pos_team` | character | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team_id` | integer |  |
| `def_pos_team` | character | Team name on defense at the start of the play. |
| `pos_team_score` | integer | Score for the team in possession at the start of the play. |
| `def_pos_team_score` | integer | Score for the defensive team at the start of the play. |
| `half` | integer | Half indicator (1 or 2). |
| `period` | integer | Period (quarter) number. |
| `down` | integer | Down of the play (1-4). |
| `distance` | integer | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `EPA` | double | Expected Points Added on the play (cfbfastR EPA model output). |
| `wpa` | double | Win Probability Added on the play (cfbfastR WP model output). |
| `wp_before` | double | Win probability for the possession team before the play (0-1). |
| `wp_after` | double | Win probability for the possession team after the play (0-1). |
| `def_wp_before` | double | Win probability for the defensive team before the play (0-1). |
| `def_wp_after` | double | Win probability for the defensive team after the play (0-1). |
| `penalty_detail` | character | Parsed penalty description extracted from play text. |
| `yds_penalty` | character | Yardage assessed on the penalty. |
| `penalty_1st_conv` | logical | TRUE when the penalty resulted in a first down conversion. |
| `new_series` | logical | Binary flag for the start of a new series of downs. |
| `firstD_by_kickoff` | logical | Binary flag for a new first down arising from a kickoff. |
| `firstD_by_poss` | logical | Binary flag for a new first down via change of possession. |
| `firstD_by_penalty` | logical | Binary flag for a new first down via penalty. |
| `firstD_by_yards` | logical | Binary flag for a new first down via yards gained. |
| `def_EPA` | double | EPA for the defensive team on the play (sign-flipped offense EPA). |
| `rz_play` | logical | Binary flag for a red-zone play (yards_to_goal <= 20). |
| `scoring_opp` | logical | Binary flag for a scoring opportunity (yards_to_goal <= 40). |
| `middle_8` | logical | TRUE for plays in the middle-8 window (final 4 min of 1H, first 4 min of 2H). |
| `stuffed_run` | logical | Binary flag for a stuffed run (zero or negative yards gained). |
| `change_of_pos_team` | logical | Binary flag for change of possession-team on the play. |
| `downs_turnover` | logical | Binary flag for a turnover on downs. |
| `pos_score_diff_start` | integer | Score differential for the possession team at the start of the play. |
| `pos_score_pts` | integer | Points scored on the play attributed to the possession team. |
| `home_wp_before` | double | Home team win probability before the play (0-1). |
| `away_wp_before` | double | Away team win probability before the play (0-1). |
| `home_wp_after` | double | Home team win probability after the play (0-1). |
| `away_wp_after` | double | Away team win probability after the play (0-1). |
| `end_of_half` | logical | Binary flag for the last play of a half. |
| `orig_play_type` | character | Original CFBD play type label before cfbfastR cleaning. |
| `offense_score_play` | logical | Binary flag for an offensive scoring play. |
| `defense_score_play` | logical | Binary flag for a defensive scoring play. |
| `pos_score_diff` | integer | Score differential from the possession team's perspective. |
| `change_of_poss` | logical | Binary flag for change of possession on the play (CFBD offense field). |
| `rusher_player_name` | character | Name of the rusher on a rushing play. |
| `yds_rushed` | integer | Rushing yards gained on the play. |
| `passer_player_name` | character | Name of the passer on a passing play. |
| `receiver_player_name` | character | Name of the receiver on a passing play. |
| `yds_receiving` | integer | Receiving yards gained on the play. |
| `yds_sacked` | integer | Yards lost on the sack. |
| `sack_players` | character | Combined names of all sack participants. |
| `sack_player_name` | character | Primary sack player name. |
| `sack_player_name2` | character | Secondary sack player name (when split between two defenders). |
| `pass_breakup_player_name` | character | Name of the defender credited with the pass breakup. |
| `interception_player_name` | character | Name of the defender credited with the interception. |
| `yds_int_return` | integer | Yards gained on an interception return. |
| `fumble_player_name` | character | Name of the player who fumbled. |
| `fumble_forced_player_name` | character | Name of the player who forced the fumble. |
| `fumble_recovered_player_name` | character | Name of the player who recovered the fumble. |
| `yds_fumble_return` | integer | Yards gained on a fumble return. |
| `punter_player_name` | character | Name of the punter. |
| `yds_punted` | integer | Yards the ball traveled on the punt. |
| `yds_punt_return` | integer | Yards gained on the punt return. |
| `yds_punt_gained` | integer | Net yards gained on the punt (punt distance minus return). |
| `punt_block_player_name` | character | Name of the player credited with blocking the punt. |
| `punt_block_return_player_name` | character | Name of the player returning a blocked punt. |
| `fg_kicker_player_name` | character | Name of the field goal kicker. |
| `yds_fg` | integer | Distance of the field goal attempt in yards. |
| `fg_block_player_name` | character | Name of the player credited with blocking the field goal. |
| `fg_return_player_name` | character | Name of the player returning the blocked/missed field goal. |
| `kickoff_player_name` | character | Name of the kickoff specialist. |
| `yds_kickoff` | integer | Yards the ball traveled on the kickoff. |
| `yds_kickoff_return` | integer | Yards gained on the kickoff return. |
| `rush` | logical | Binary flag for a rushing play. |
| `rush_td` | logical | Binary flag for a rushing touchdown. |
| `pass` | logical | Binary flag for a passing play (includes sacks). |
| `pass_td` | logical | Binary flag for a passing touchdown. |
| `completion` | logical | Binary flag for a completed pass. |
| `pass_attempt` | logical | Binary flag for a pass attempt. |
| `target` | logical | Binary flag for a targeted receiver on the play. |
| `sack` | logical | Binary flag for a sack (duplicate of sack_vec for downstream use). |
| `int` | logical | Binary flag for an interception. |
| `int_td` | logical | Binary flag for an interception returned for a touchdown. |
| `turnover_vec` | logical | Binary flag for any play classified as a turnover. |
| `kickoff_play` | logical | Binary flag for a kickoff play. |
| `scoring_play` | logical | `TRUE` if the play resulted in a score. |
| `td_play` | logical | Binary flag for a touchdown play. |
| `touchdown` | logical | Binary flag for a touchdown (duplicate of td_play for downstream use). |
| `safety` | logical | Binary flag for a safety. |
| `fumble_vec` | logical | Binary flag for a play involving a fumble. |
| `kickoff_tb` | logical | Binary flag for a kickoff touchback. |
| `kickoff_onside` | logical | Binary flag for an onside kickoff attempt. |
| `kickoff_oob` | logical | Binary flag for a kickoff out of bounds. |
| `kickoff_fair_catch` | logical | Binary flag for a kickoff fair catch. |
| `kickoff_downed` | logical | Binary flag for a kickoff downed in the field of play. |
| `kickoff_safety` | logical | Binary flag for a kickoff safety. |
| `punt` | logical | Binary flag for a punt play. |
| `punt_play` | logical | Binary flag for any punt-related play (includes blocks/returns). |
| `punt_tb` | logical | Binary flag for a punt touchback. |
| `punt_oob` | logical | Binary flag for a punt out of bounds. |
| `punt_fair_catch` | logical | Binary flag for a punt fair catch. |
| `punt_downed` | logical | Binary flag for a punt downed in the field of play. |
| `punt_safety` | logical | Binary flag for a punt safety. |
| `punt_blocked` | logical | Binary flag for a blocked punt. |
| `penalty_safety` | logical | Binary flag for a safety scored on a penalty. |
| `fg_made` | logical | TRUE when the field goal attempt was successful. |
| `fg_make_prob` | double | Predicted probability of making the field goal (cfbfastR FG model, 0-1). |
| `penalty_flag` | logical | TRUE when a penalty was flagged on the play. |
| `penalty_declined` | logical | TRUE when the penalty was declined. |
| `penalty_no_play` | logical | TRUE when the penalty nullified the play (no play counted). |
| `penalty_offset` | logical | TRUE when offsetting penalties were called. |
| `penalty_text` | character | TRUE when penalty information is detectable in the play text. |
| `lead_wp_before2` | double | Win probability two plays ahead (lead 2 of wp_before). |
| `lead_wp_before` | double | Win probability on the next play (lead of wp_before). |
| `lead_pos_team2` | integer | Possession team two plays ahead (lead 2 of pos_team). |
| `id` | integer | 247Sports referencing id for the recruit. |
| `sequenceNumber` | integer |  |
| `text` | character | Full play description. |
| `awayScore` | integer |  |
| `homeScore` | integer |  |
| `scoringPlay` | logical |  |
| `priority` | logical | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | character | ISO timestamp the play record was last modified. |
| `wallclock` | character | Real-world ISO timestamp of the play. |
| `teamParticipants` | character |  |
| `isPenalty` | logical |  |
| `statYardage` | integer |  |
| `isTurnover` | logical |  |
| `type.id` | character |  |
| `type.text` | character |  |
| `type.abbreviation` | character |  |
| `period.number` | integer |  |
| `clock.displayValue` | character |  |
| `start.down` | integer |  |
| `start.distance` | integer |  |
| `start.yardLine` | integer |  |
| `start.yardsToEndzone` | integer |  |
| `start.team.id` | integer |  |
| `end.down` | integer |  |
| `end.distance` | integer |  |
| `end.yardLine` | integer |  |
| `end.yardsToEndzone` | integer |  |
| `end.downDistanceText` | character |  |
| `end.shortDownDistanceText` | character |  |
| `end.possessionText` | character |  |
| `end.team.id` | integer |  |
| `start.downDistanceText` | character |  |
| `start.shortDownDistanceText` | character |  |
| `start.possessionText` | character |  |
| `scoringType.name` | character |  |
| `scoringType.displayName` | character |  |
| `scoringType.abbreviation` | character |  |
| `pointAfterAttempt.id` | double |  |
| `pointAfterAttempt.text` | character |  |
| `pointAfterAttempt.abbreviation` | character |  |
| `pointAfterAttempt.value` | double |  |
| `drive.id` | character |  |
| `drive.displayResult` | character |  |
| `drive.isScore` | logical |  |
| `drive.team.shortDisplayName` | character |  |
| `drive.team.displayName` | character |  |
| `drive.team.name` | character |  |
| `drive.team.abbreviation` | character |  |
| `drive.yards` | integer |  |
| `drive.offensivePlays` | integer |  |
| `drive.result` | character |  |
| `drive.description` | character |  |
| `drive.shortDisplayResult` | character |  |
| `drive.timeElapsed.displayValue` | character |  |
| `drive.start.period.number` | integer |  |
| `drive.start.period.type` | character |  |
| `drive.start.yardLine` | integer |  |
| `drive.start.clock.displayValue` | character |  |
| `drive.start.text` | character |  |
| `drive.end.period.number` | integer |  |
| `drive.end.period.type` | character |  |
| `drive.end.yardLine` | integer |  |
| `drive.end.clock.displayValue` | character |  |
| `seasonType` | integer |  |
| `week` | integer | Game week of the season. |
| `status_type_completed` | logical |  |
| `homeTeamId` | integer |  |
| `awayTeamId` | integer |  |
| `homeFinalScore` | integer |  |
| `awayFinalScore` | integer |  |
| `homeTeamName` | character |  |
| `awayTeamName` | character |  |
| `homeTeamMascot` | character |  |
| `awayTeamMascot` | character |  |
| `homeTeamAbbrev` | character |  |
| `awayTeamAbbrev` | character |  |
| `homeTeamNameAlt` | character |  |
| `awayTeamNameAlt` | character |  |
| `gameSpread` | double |  |
| `homeFavorite` | logical |  |
| `gameSpreadAvailable` | logical |  |
| `overUnder` | double |  |
| `homeTeamSpread` | double |  |
| `clock.minutes` | integer |  |
| `clock.seconds` | integer |  |
| `lag_half` | integer |  |
| `lead_half` | integer |  |
| `start.TimeSecsRem` | integer |  |
| `start.adj_TimeSecsRem` | integer |  |
| `lead_text` | character |  |
| `lead_start_team` | character |  |
| `lead_start_yardsToEndzone` | integer |  |
| `lead_start_down` | integer |  |
| `lead_start_distance` | integer |  |
| `lead_scoringPlay` | logical |  |
| `text_dupe` | logical |  |
| `end_state_missing` | logical |  |
| `start.pos_team.id` | integer |  |
| `start.def_pos_team.id` | integer |  |
| `end.def_pos_team.id` | integer |  |
| `end.pos_team.id` | integer |  |
| `start.pos_team.name` | character |  |
| `start.def_pos_team.name` | character |  |
| `end.pos_team.name` | character |  |
| `end.def_pos_team.name` | character |  |
| `start.is_home` | logical |  |
| `end.is_home` | logical |  |
| `homeTimeoutCalled` | logical |  |
| `awayTimeoutCalled` | logical |  |
| `end.homeTeamTimeouts` | integer |  |
| `end.awayTeamTimeouts` | integer |  |
| `start.homeTeamTimeouts` | integer |  |
| `start.awayTeamTimeouts` | integer |  |
| `end.TimeSecsRem` | integer |  |
| `end.adj_TimeSecsRem` | integer |  |
| `start.posTeamTimeouts` | integer |  |
| `start.defPosTeamTimeouts` | integer |  |
| `end.posTeamTimeouts` | integer |  |
| `end.defPosTeamTimeouts` | integer |  |
| `firstHalfKickoffTeamId` | integer |  |
| `start.yard` | integer |  |
| `end.yard` | integer |  |
| `lag_scoringPlay` | logical |  |
| `down_1` | logical |  |
| `down_2` | logical |  |
| `down_3` | logical |  |
| `down_4` | logical |  |
| `down_1_end` | logical |  |
| `down_2_end` | logical |  |
| `down_3_end` | logical |  |
| `down_4_end` | logical |  |
| `td_check` | logical |  |
| `forced_fumble` | logical |  |
| `is_home` | logical |  |
| `lag_HA_score_diff` | integer |  |
| `HA_score_diff` | integer |  |
| `net_HA_score_pts` | integer |  |
| `H_score_diff` | integer |  |
| `A_score_diff` | integer |  |
| `lag_homeScore` | integer |  |
| `lag_awayScore` | integer |  |
| `start.homeScore` | integer |  |
| `start.awayScore` | integer |  |
| `end.homeScore` | integer |  |
| `end.awayScore` | integer |  |
| `start.pos_team_score` | integer |  |
| `start.def_pos_team_score` | integer |  |
| `start.pos_score_diff` | integer |  |
| `end.pos_team_score` | integer |  |
| `end.def_pos_team_score` | integer |  |
| `end.pos_score_diff` | integer |  |
| `start.pos_team_receives_2H_kickoff` | logical |  |
| `end.pos_team_receives_2H_kickoff` | logical |  |
| `penalty_in_text` | logical |  |
| `penalty_count` | integer |  |
| `penalty_declined_count` | integer |  |
| `penalty_all_declined` | logical |  |
| `penalty_enforcement` | character |  |
| `penalty_negated_play` | logical |  |
| `pass_breakup` | logical |  |
| `pass_depth` | character |  |
| `pass_direction` | character |  |
| `rush_direction` | character |  |
| `qb_hurry` | logical |  |
| `fg_attempt` | logical |  |
| `pos_unit` | character | Possession-team unit label (offense or special teams). |
| `def_pos_unit` | character | Defensive possession-team unit label (defense or special teams). |
| `sp` | logical |  |
| `play` | logical | Binary flag indicating the row is a counted play (excludes end markers/timeouts/penalties). |
| `cleaned_text` | character |  |
| `kneel_down` | logical |  |
| `scrimmage_play` | logical |  |
| `pos_score_diff_end` | integer |  |
| `fumble_lost` | logical |  |
| `fumble_recovered` | logical |  |
| `field_goal_result` | character |  |
| `extra_point_result` | character |  |
| `two_point_conv_result` | character |  |
| `defensive_two_point_attempt` | logical |  |
| `defensive_two_point_conv` | logical |  |
| `yds_punted_source` | character |  |
| `yds_kickoff_source` | character |  |
| `yds_punt_return_source` | character |  |
| `air_yardsToEndzone` | integer |  |
| `air_yards` | integer |  |
| `yards_after_catch` | integer |  |
| `kickoff_return_player_name` | character |  |
| `punt_return_player_name` | character |  |
| `xp_attempt` | logical |  |
| `xp_made` | logical |  |
| `xp_kicker_player_name` | character |  |
| `kicking_team` | integer |  |
| `return_team` | integer |  |
| `fumble_or_muff` | logical |  |
| `recovery_team` | integer |  |
| `recovery_team_2` | integer |  |
| `penalty_spot_yardline` | integer |  |
| `penalty_spot_side` | character |  |
| `penalty_spot_yardsToEndzone` | integer |  |
| `fumbling_team` | integer |  |
| `int_turnover` | logical |  |
| `pos_fumble_lost` | logical |  |
| `def_fumble_lost` | logical |  |
| `is_pos_team_turnover` | logical |  |
| `is_def_pos_team_turnover` | logical |  |
| `is_turnover` | logical | `TRUE` if the play was a turnover. |
| `turnover_team` | integer |  |
| `is_st_turnover` | logical |  |
| `is_blocked_punt_turnover` | logical |  |
| `is_blocked_fg_turnover` | logical |  |
| `sack_team` | integer |  |
| `interception_team` | integer |  |
| `pass_breakup_team` | integer |  |
| `forced_fumble_team` | integer |  |
| `fumble_recovery_team` | integer |  |
| `punt_return_team` | integer |  |
| `kick_return_team` | integer |  |
| `fg_team` | integer |  |
| `punt_team` | integer |  |
| `penalized_team` | integer |  |
| `penalty_yards_signed` | integer |  |
| `penalty_side` | character |  |
| `penalty_yards_net` | integer |  |
| `penalty_team_id` | integer |  |
| `new_down` | integer |  |
| `new_distance` | integer |  |
| `under_2` | logical |  |
| `goal_to_go` | logical |  |
| `stopped_run` | logical |  |
| `opportunity_run` | logical |  |
| `highlight_run` | logical |  |
| `adj_rush_yardage` | integer |  |
| `line_yards` | double |  |
| `second_level_yards` | double |  |
| `open_field_yards` | integer |  |
| `highlight_yards` | double |  |
| `opp_highlight_yards` | double |  |
| `short_rush_success` | logical |  |
| `short_rush_attempt` | logical |  |
| `early_down` | logical |  |
| `late_down` | logical |  |
| `power_rush_attempt` | logical |  |
| `power_rush_success` | logical |  |
| `early_down_pass` | logical |  |
| `early_down_rush` | logical |  |
| `late_down_pass` | logical |  |
| `late_down_rush` | logical |  |
| `standard_down` | logical |  |
| `passing_down` | logical |  |
| `TFL` | logical |  |
| `TFL_pass` | logical |  |
| `TFL_rush` | logical |  |
| `havoc` | logical |  |
| `first_down_yards` | logical |  |
| `first_down_penalty` | logical |  |
| `first_down_earned` | logical |  |
| `start.pos_team_spread` | double |  |
| `start.elapsed_share` | double |  |
| `start.spread_time` | double |  |
| `end.pos_team_spread` | double |  |
| `end.elapsed_share` | double |  |
| `end.spread_time` | double |  |
| `penalty_assessed_on_kickoff` | logical |  |
| `start.yardsToEndzone.touchback` | integer |  |
| `EP_start_touchback` | double |  |
| `EP_start` | double |  |
| `EP_end` | double |  |
| `EP_penalty_cf` | double |  |
| `penalty_cf_yardsToEndzone` | integer |  |
| `lag_EP_end` | double |  |
| `EP_between` | double |  |
| `EPA_scrimmage` | double |  |
| `EPA_rush` | double |  |
| `EPA_pass` | double |  |
| `EPA_explosive` | logical |  |
| `EPA_non_explosive` | double |  |
| `EPA_explosive_pass` | logical |  |
| `EPA_explosive_rush` | logical |  |
| `first_down_created` | logical |  |
| `EPA_success` | logical |  |
| `EPA_success_early_down` | logical |  |
| `EPA_success_early_down_pass` | logical |  |
| `EPA_success_early_down_rush` | logical |  |
| `EPA_success_late_down` | logical |  |
| `EPA_success_late_down_pass` | logical |  |
| `EPA_success_late_down_rush` | logical |  |
| `EPA_success_standard_down` | logical |  |
| `EPA_success_passing_down` | logical |  |
| `EPA_success_pass` | logical |  |
| `EPA_success_rush` | logical |  |
| `EPA_success_EPA` | double |  |
| `EPA_success_standard_down_EPA` | double |  |
| `EPA_success_passing_down_EPA` | double |  |
| `EPA_success_pass_EPA` | double |  |
| `EPA_success_rush_EPA` | double |  |
| `EPA_middle_8_success` | logical |  |
| `EPA_middle_8_success_pass` | logical |  |
| `EPA_middle_8_success_rush` | logical |  |
| `EPA_penalty` | double |  |
| `EPA_penalty_direct` | double |  |
| `EPA_sp` | double |  |
| `EPA_fg` | double |  |
| `EPA_punt` | double |  |
| `EPA_kickoff` | double |  |
| `start.ExpScoreDiff_touchback` | double |  |
| `start.ExpScoreDiff` | double |  |
| `start.ExpScoreDiff_Time_Ratio_touchback` | double |  |
| `start.ExpScoreDiff_Time_Ratio` | double |  |
| `end.ExpScoreDiff` | double |  |
| `end.ExpScoreDiff_Time_Ratio` | double |  |
| `wp_touchback` | double |  |
| `wp_before_naive` | double |  |
| `wp_touchback_naive` | double |  |
| `wp_after_naive` | double |  |
| `def_wp_before_naive` | double |  |
| `home_wp_before_naive` | double |  |
| `away_wp_before_naive` | double |  |
| `lead_wp_before_naive` | double |  |
| `lead_wp_before2_naive` | double |  |
| `def_wp_after_naive` | double |  |
| `home_wp_after_naive` | double |  |
| `away_wp_after_naive` | double |  |
| `wpa_naive` | double |  |
| `cp` | double |  |
| `cp_game_state` | double |  |
| `cp_model` | character |  |
| `cpoe` | double |  |
| `era` | integer |  |
| `xpass` | double |  |
| `pass_oe` | double |  |
| `drive_start` | double |  |
| `drive_stopped` | logical |  |
| `drive_play_index` | integer |  |
| `drive_offense_plays` | integer |  |
| `prog_drive_EPA` | double |  |
| `prog_drive_WPA` | double |  |
| `drive_offense_yards` | integer |  |
| `drive_total_yards` | integer |  |
| `qbr_epa` | double |  |
| `weight` | double | Listed weight (lbs). |
| `non_fumble_sack` | logical |  |
| `sack_epa` | double |  |
| `pass_epa` | double |  |
| `rush_epa` | double |  |
| `pen_epa` | double |  |
| `sack_weight` | double |  |
| `pass_weight` | double |  |
| `rush_weight` | double |  |
| `pen_weight` | double |  |
| `action_play` | logical |  |
| `athlete_name` | character | Player full name. |
| `rusher_player_id` | integer |  |
| `passer_player_id` | integer |  |
| `receiver_player_id` | integer |  |
| `fumble_player_id` | integer | CFBD athlete_id of the player who fumbled. |
| `sack_player_id` | integer | Comma-separated CFBD athlete_id(s) of the sacking defender(s). |
| `sack_player_id2` | integer |  |
| `interception_player_id` | integer | CFBD athlete_id of the defender credited with an interception. |
| `pass_breakup_player_id` | integer | CFBD athlete_id of the defender credited with the pass breakup (PBU). |
| `fumble_forced_player_id` | integer | CFBD athlete_id of the defender credited with forcing the fumble. |
| `fumble_recovered_player_id` | integer | CFBD athlete_id of the player recovering the fumble. |
| `fg_kicker_player_id` | integer |  |
| `punter_player_id` | integer |  |
| `kickoff_player_id` | integer |  |
| `kickoff_return_player_id` | integer |  |
| `punt_return_player_id` | integer |  |
| `fg_block_player_id` | integer |  |
| `punt_block_player_id` | character |  |
| `fg_return_player_id` | character |  |
| `punt_block_return_player_id` | character |  |
| `go_wp` | double |  |
| `first_down_prob` | double |  |
| `wp_succeed` | double |  |
| `wp_fail` | double |  |
| `make_fg_wp` | double |  |
| `miss_fg_wp` | double |  |
| `fg_wp` | double |  |
| `punt_wp` | double |  |
| `go_boost` | double |  |
| `go_wp_diff` | double |  |
| `fg_wp_diff` | double |  |
| `punt_wp_diff` | double |  |
| `fourth_down_recommendation` | character |  |
| `two_pt_wp` | double |  |
| `xp_wp` | double |  |
| `prob_2pt` | double |  |
| `two_pt_recommendation` | character |  |
| `two_pt_wp_diff` | double |  |
| `yards_to_goal` | integer | Distance in yards from the offense's spot to the opponent's goal line (0-100). |
| `TimeSecsRem` | integer | Seconds remaining in the half at the start of the play. |

**Example**

```python
normalize_pbp_columns(pbp, "xpass_model")
```

### predict_from_card {#predict_from_card}

`predict_from_card(df: 'pl.DataFrame', model: 'str', booster: 'Any') -> 'np.ndarray'`

Score `df` with `booster`, validated and ordered by the model's card.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Frame carrying at least the model's declared features. Extra columns are ignored, so a full pbp frame passes through unchanged. |
| `model` | `str` |  | Bundle stem, used to look up the card and to name the model in any error. |
| `booster` | `Any` |  | The loaded `xgboost.Booster`. |

**Returns**

The booster's raw predictions.

**Example**

```python
from sportsdataverse.cfb.model_calculators import predict_from_card
predict_from_card(pbp, "xpass_model", booster)
```

### predict_margin {#predict_margin}

`predict_margin(home_adj_net: 'float', away_adj_net: 'float', neutral: 'bool', *, era: 'str' = 'modern', games_played: 'float | None' = None) -> 'float'`

Expected home scoring margin from the two net ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_net` | `float` |  | Home team's opponent-adjusted net rating (`adj_net` from `cfb_ratings.efficiency_ratings`). |
| `away_adj_net` | `float` |  | Away team's opponent-adjusted net rating. |
| `neutral` | `bool` |  | Whether the game is at a neutral site (no home-field advantage). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying the fitted slope, `hfa_points` and the attenuation curve. |
| `games_played` | `float \| None` | `None` | Games behind the WEAKER of the two as-of ratings. Supplying it selects the games-played slope (see `slope_for_games`) and is worth ~0.6 MAE; omitting it falls back to the flat `net_points_scale`, which is the average over the curve. |

**Returns**

The expected margin (home minus away), in points: `slope * (home_adj_net - away_adj_net) + hfa_points` on a home field, or without the HFA term on a neutral one. HFA is added in POINTS, not routed through the slope. The previous form multiplied an EPA-scale `2 * hfa_epa` by `net_points_scale`, which tied the two together and let them drift apart unnoticed -- the shipped pair implied ~1.65 points against a measured ~3.0.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import predict_margin
predict_margin(0.30, 0.10, neutral=False)

# With games-played, which selects the attenuation-corrected slope

predict_margin(0.30, 0.10, neutral=False, games_played=9)
```

### predict_total {#predict_total}

`predict_total(home_adj_off: 'float', home_adj_def: 'float', away_adj_off: 'float', away_adj_def: 'float', game_pace: 'float', *, era: 'str' = 'modern') -> 'float'`

Expected combined point total from the four efficiency ratings + tempo.

Fitted linear model `total_intercept + total_scale * sum4 + total_pace_scale *
game_pace`, where `sum4 = home_adj_off + away_adj_def + away_adj_off +
home_adj_def`. The four ratings are summed because each side's scoring rises
with its own offense and with the opponent's EPA-*allowed* (`adj_def` is
lower = better defense). `game_pace` (the matchup's expected scrimmage plays,
`home_off_pace * away_off_pace / league_avg_pace`) enters because a total is a
*sum* -- tempo scales both sides' points the same way, so it compounds into the
total (whereas in the margin, a differential, pace cancels). All three
coefficients are fitted on 2023 actual totals.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_off` | `float` |  | Home offense adjusted EPA/play (`adj_off_epa`). |
| `home_adj_def` | `float` |  | Home defense adjusted EPA/play allowed (`adj_def_epa`). |
| `away_adj_off` | `float` |  | Away offense adjusted EPA/play. |
| `away_adj_def` | `float` |  | Away defense adjusted EPA/play allowed. |
| `game_pace` | `float` |  | Expected scrimmage plays for the matchup, i.e. `home_off_pace * away_off_pace / league_avg_pace` from the ratings' `off_pace` column (`cfb_predict_games` computes this for you). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying the fitted `total_intercept` / `total_scale` / `total_pace_scale`. |

**Returns**

The expected combined total points.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import predict_total
predict_total(0.20, -0.05, 0.10, 0.02, game_pace=66.0)
```

### slope_for_games {#slope_for_games}

`slope_for_games(games_played: 'float | None', *, era: 'str' = 'modern') -> 'float'`

Points per unit of rating differential, given how many games back it.

A single slope is wrong. An as-of rating built on two games is a far
noisier predictor than one built on twelve, and OLS slopes attenuate
toward zero as predictor noise grows -- so the correct multiplier is
smaller early and grows through the season. Measured, walk-forward on
2014-2025:

    0-3 games -> 10.62      6-7 games -> 42.00
    4-5 games -> 26.06      8+  games -> 54.49

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games_played` | `float \| None` |  | Games behind the as-of rating. When two ratings back a prediction this should be the WEAKER (smaller) of the two, since the noisier rating binds the attenuation. `None` selects the flat `net_points_scale`. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |

**Returns**

The points-per-rating-unit slope for that bucket, or the flat `net_points_scale` when `games_played` is `None` or falls outside every bucket. The flat value is the average over the curve, so it is a safe default rather than a silent zero.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import slope_for_games
slope_for_games(2)      # early season -- heavily attenuated
slope_for_games(11)     # late season -- near the full slope

# Unknown game count falls back to the flat scale

slope_for_games(None)
```

### special_teams_ratings {#special_teams_ratings}

`special_teams_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: a per-unit special-teams EPA composite.

Special teams was empirically found NOT to obey the offense-minus-defense
symmetry `efficiency_ratings` / `fei_ratings` rely on, and not
to benefit from opponent adjustment, when validated against the 2023 SP+
special-teams oracle (`tests/fixtures/cfb_prediction/sp_plus_2023.parquet`
`sp_special`):

* The executing `pos_team` owns the EPA on a kickoff / punt / field
  goal. The `def_pos_team` "coverage" side reflects the opposing
  returner's skill, not the coverage team's, and is not recoverable from
  EPA -- adding any coverage unit *lowers* SP+ agreement (0.77 -> 0.58),
  so coverage/defense units are excluded entirely (see the module's
  special-teams unit patterns).
* The opponent-adjustment ridge (`cfb_adjusted_epa._fit_opponent_ridge`)
  *hurts* agreement (0.72 vs 0.77) -- special teams is only weakly
  opponent-dependent, so this function does not fit a ridge at all.
* Splitting the offense-side plays into per-phase units (field goal, punt,
  kick return) is what helps. Each unit's per-team mean EPA/play is
  centered on that unit's league-wide per-play mean and the three
  centered deviations are summed -- true EPA units. This centered form
  reached Spearman 0.865 against SP+ special teams, beating both the
  originally-shipped z-scored composite (0.768 -- dimensionless, std
  ~1.7, range +-5 under an epa` column name; replaced 2026-07-28)
  and a single-unit offense-minus-intercept ridge fit (0.703).

`adj_st_epa` is therefore the sum, over the three special-teams units
(field goal, punt, kick return), of each unit's per-team mean EPA/play
above the unit's league average. A team with no plays in a given unit
contributes 0 for that unit (not a penalty). `config` is accepted for
signature parity with
`efficiency_ratings` / `fei_ratings` but is unused -- there is
no ridge (and therefore no `ridge_lambda`) in this recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying `game_id`, `pos_team_id`, `EPA`, and `play_type`. Not pre-filtered to special-teams plays -- this function does that filtering itself. |
| `config` | `RatingsConfig \| None` | `None` | Unused (kept for signature parity across the three rating functions). See the note above. |

**Returns**

A `polars.DataFrame` with one row per `team_id` appearing anywhere in `plays`: `team_id` (Utf8), `adj_st_epa` (Float64, the sum of per-unit executing-team mean EPA/play above each unit's league average). Teams with no special-teams plays get `adj_st_epa == 0.0`. Zero-row (correctly-typed) when `plays` has no special-teams plays.

No returns table is published for this function: no capture: it needs play-by-play joined with schedule fields (home, neutral_site, pos_team_id) that neither load_cfb_pbp nor load_cfb_pbp_r carries; only cfb_ratings builds that join, internally.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import special_teams_ratings
st = special_teams_ratings(pbp)
st.sort("adj_st_epa", descending=True).head()
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, era: 'str' = 'modern') -> 'float'`

Home win probability from an expected margin via the Gaussian CDF.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home margin in points (e.g. from `predict_margin`). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying `margin_sd`. |

**Returns**

`Phi(exp_margin / margin_sd)` -- the probability the home team wins under a `Normal(exp_margin, margin_sd**2)` margin model. `0.5` at a zero expected margin.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import win_prob_from_margin
win_prob_from_margin(7.0)
```
