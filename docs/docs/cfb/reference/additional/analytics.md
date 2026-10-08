---
title: "CFB — additional Python functions — Analytics"
sidebar_label: "Analytics"
sidebar_position: 10
description: "CFB — additional Python functions — Analytics — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Analytics

### add_play_type_canonical {#add_play_type_canonical}

`add_play_type_canonical(df: 'pl.DataFrame', *, source: 'str' = 'type.text', with_family: 'bool' = True) -> 'pl.DataFrame'`

Append `play_type_canonical` (and optionally `play_type_family`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | A play-by-play frame. |
| `source` | `str` | `'type.text'` | Name of the raw play-type column. |
| `with_family` | `bool` | `True` | Also append the coarse `play_type_family` column. |

**Returns**

The frame with the canonical column(s) appended. Returned unchanged when `source` is absent, so the helper is safe to apply to frames that have already been projected down.

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
| `play_type_canonical` | character |  |
| `play_type_family` | character |  |

**Example**

```python
import polars as pl
from sportsdataverse.cfb import add_play_type_canonical

pbp = pl.DataFrame({"type.text": ["Rush", "Pass Reception", "Timeout"]})
out = add_play_type_canonical(pbp)
out.group_by("play_type_family").agg(pl.len())
```

### canonical_play_type_expr {#canonical_play_type_expr}

`canonical_play_type_expr(source: 'str' = 'type.text') -> 'pl.Expr'`

Build the polars expression mapping raw `type.text` to a canonical type.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source` | `str` | `'type.text'` | Name of the raw play-type column. |

**Returns**

A `pl.Expr` aliased `play_type_canonical`. Values absent from `PLAY_TYPE_CANONICAL` (and nulls) yield null, so upstream vocabulary drift surfaces rather than silently creating a category.

**Example**

```python
import polars as pl
from sportsdataverse.cfb import canonical_play_type_expr

pbp = pl.DataFrame({"type.text": ["Pass Reception", "Punt Return"]})
pbp.with_columns(canonical_play_type_expr())
```

### cfb_adjusted_tempo {#cfb_adjusted_tempo}

`cfb_adjusted_tempo(seasons: 'Union[int, list[int]]', *, exclude_garbage: 'bool' = True, config: 'Optional[AdjustConfig]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team-season situation-neutral, opponent-adjusted tempo / pace.

Counts scrimmage plays per team-game (garbage time and kneels/spikes
dropped) and per-play elapsed seconds, then opponent-adjusts both with
the iterative solver on the per-game values (a fast team facing slow
defenses gets `adj_plays_game > raw_plays_game`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | season or list of seasons (hosted pbp covers 2002-2021). |
| `exclude_garbage` | `bool` | `True` | drop Connelly garbage-time plays. |
| `config` | `Optional[AdjustConfig]` | `None` | `AdjustConfig` for the solver. |
| `return_as_pandas` | `bool` | `False` | return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (season, team_id): `games, raw_plays_game, adj_plays_game, raw_sec_play, adj_sec_play, pace_rank` (rank 1 = fastest adjusted pace). Zero-row frame with the documented schema on empty input.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the pace covers. |
| `team_id` | character | Team ESPN id (character join key). |
| `games` | integer | Games with situation-neutral offensive snaps in the loaded seasons. |
| `raw_plays_game` | double | Situation-neutral scrimmage plays per game (garbage time and kneels/spikes excluded). |
| `adj_plays_game` | double | Opponent-adjusted situation-neutral plays per game (iterative solver; higher = faster). |
| `raw_sec_play` | double | Mean seconds elapsed per situation-neutral play (season total seconds over total plays). |
| `adj_sec_play` | double | Opponent-adjusted seconds elapsed per situation-neutral play (lower = faster). |
| `pace_rank` | integer | Dense rank on adj_plays_game descending (fastest adjusted pace = 1). |

**Example**

```python
from sportsdataverse.cfb import cfb_adjusted_tempo
df = cfb_adjusted_tempo([2021])
print(df.shape)

# Pipeline next step (one line)

df.sort("pace_rank").head()
```

### cfb_games_from_schedule {#cfb_games_from_schedule}

`cfb_games_from_schedule(schedule: 'FrameLike', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, Any]'`

Map a `load_cfb_schedule()` frame into the seedr engine `games` schema.

Derives `game_type` heuristically: games whose `notes` mention a
championship (but not the CFP / national championship) are
`CONF_CHAMP`; otherwise `season_type == "regular"` maps to `REG`
and everything else to `POST`. `result` is the home margin
(`home_points - away_points`; null when either score is missing) and
`neutral` comes from `neutral_site`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `FrameLike` |  | Output of `sportsdataverse.cfb.load_cfb_schedule` (needs `season`, `week`, `season_type`, `home_team`, `away_team`, `home_points`, `away_points`, `neutral_site` and optionally `notes`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame with columns `season`, `week`, `game_type`, `home_team`, `away_team`, `result`, `neutral`, `home_points`, `away_points` — the `cfb_standings` / `cfb_simulations` input schema. The trailing per-game points columns feed the SEC `capped_scoring_margin` official tiebreaker rung (see `CONFERENCE_TIEBREAKERS`); `cfb_standings` skips that rung when they're absent, so passing this frame straight through is always safe.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the game belongs to; consumed as the sim/season identifier by cfb_standings and cfb_simulations. |
| `week` | integer | Week of the season the game is scheduled in, passed through from the schedule frame. |
| `game_type` | character | Derived game classification - CONF_CHAMP when the schedule notes mention a (non-CFP, non-national) championship, REG for regular-season rows, POST otherwise. |
| `home_team` | character | Team name of the home team, passed through from the schedule frame. |
| `away_team` | character | Team name of the away team, passed through from the schedule frame. |
| `result` | double | Home-team margin (home_points minus away_points); null when either score is missing, marking the game as unplayed for the simulation engine. |
| `neutral` | integer | Neutral-site flag derived from the schedule's neutral_site column (1 = neutral site, 0 = home game). |
| `home_points` | double | Home team's final score, passed through from the schedule frame; null when missing (unplayed game). Feeds the SEC capped_scoring_margin official conference tiebreaker rung in cfb_standings - the rung is skipped when absent. |
| `away_points` | double | Away team's final score, passed through from the schedule frame; null when missing (unplayed game). Feeds the SEC capped_scoring_margin official conference tiebreaker rung in cfb_standings - the rung is skipped when absent. |

**Example**

```python
import polars as pl
from sportsdataverse.cfb import (
    load_cfb_schedule, cfb_games_from_schedule, cfb_standings,
)

sched = load_cfb_schedule(seasons=2024)
games = cfb_games_from_schedule(sched)
teams = (
    sched.select(team=pl.col("home_team"), conference=pl.col("home_conference"))
    .vstack(sched.select(team=pl.col("away_team"), conference=pl.col("away_conference")))
    .unique(subset=["team"], keep="first")
)
st = cfb_standings(games, teams)
print(st.head())
```

### cfb_playoff_seeds {#cfb_playoff_seeds}

`cfb_playoff_seeds(standings: 'FrameLike', rankings: 'Optional[FrameLike]' = None, playoff_seeds: 'int' = 12, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, Any]'`

Assign College Football Playoff seeds (current straight-seeding rule).

Implements the 2025 CFP rule: the field is the `playoff_seeds` (12)
best-ranked teams with the 5 highest-ranked conference champions
guaranteed inclusion; seeds are assigned straight by ranking order (no
champion bump to the top four). The rule evolves — it lives in this ONE
function so it can be updated in one place.

When `rankings` is None the ordering falls back to the standings
tiebreaker metrics — `win_pct` desc, then `sov`, `sos`, `pd`
desc, then team name (documented deterministic fallback; a committee
ranking is the intended input).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `standings` | `FrameLike` |  | Output of `cfb_standings` (needs `sim`, `team`, `conf_champ`, `win_pct`, `sov`, `sos`, `pd`). |
| `rankings` | `Optional[FrameLike]` | `None` | Optional frame with columns `team` and `rank` (1 = best). Unranked teams order after ranked ones by the fallback. |
| `playoff_seeds` | `int` | `12` | Field size (default 12). The champion guarantee is `min(5, number of champions, playoff_seeds)`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The standings frame with a `seed` column (Int64; null for teams outside the field), sorted by sim and seed.

| col_name | type | description |
|---|---|---|
| `sim` | integer | Season or simulation identifier the standings row belongs to. |
| `team` | character | Team name (join key across the seedr engine frames). |
| `conference` | character | Conference the team belongs to; null or "FBS Independents" marks an independent. |
| `games` | integer | Total games played across all game types (regular season, conference championship and postseason). |
| `wins` | integer | Wins across all played games (conference championship and postseason included). |
| `losses` | integer | Losses across all played games. |
| `ties` | integer | Ties across all played games. |
| `win_pct` | double | Overall win percentage - (wins + 0.5 * ties) / games, 0.0 when no games have been played. |
| `pd` | double | Point differential (points for minus points against, via game margins) summed over all played games. |
| `conf_games` | integer | Number of conference regular-season games played (both teams in the same conference; CONF_CHAMP games excluded). |
| `conf_wins` | integer | Wins in conference regular-season games. |
| `conf_losses` | integer | Losses in conference regular-season games. |
| `conf_ties` | integer | Ties in conference regular-season games. |
| `conf_pct` | double | Conference win percentage - (conf_wins + 0.5 * conf_ties) / conf_games, 0.0 with no conference games; the primary sort key for conference ranks. |
| `conf_pd` | double | Point differential summed over conference regular-season games only; the POINTS-depth tiebreaker rung. |
| `sov` | double | Strength of victory, conference-REG-scoped (unlike nflseedR's overall games-weighted version) - mean of defeated conference opponents' conference win pct, one term per conference victory; 0.0 for independents or teams without conference wins. |
| `sos` | double | Strength of schedule, conference-REG-scoped (unlike nflseedR's overall games-weighted version) - mean of conference opponents' conference win pct across all conference games played; 0.0 for independents. |
| `conf_rank` | integer | Rank within the conference from the tiebreaker cascade (1 = best); null for independents. |
| `conf_champ` | logical | Whether the team is its conference's champion - the CONF_CHAMP game winner when one was played, otherwise the conference's rank-1 team; always false for independents. |
| `seed` | integer | College Football Playoff seed under the straight-seeding rule (1 = best); null for teams outside the field. The five best-ordered conference champions are guaranteed inclusion. |

**Example**

```python
from sportsdataverse.cfb import cfb_standings, cfb_playoff_seeds
st = cfb_standings(games, teams)
seeded = cfb_playoff_seeds(st, rankings=ranks_df, playoff_seeds=12)
print(seeded.filter(pl.col("seed").is_not_null()))
```

### cfb_resume {#cfb_resume}

`cfb_resume(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Rating-based résumé metrics: SoS, quality wins, game control, wins-above-bubble.

For each team, joins every played opponent to its `cfb_ratings.cfb_ratings`
strength and rolls the games up into:

- `sos` -- mean opponent `adj_net` over played games (rating-based strength
  of schedule; complements the record-based SOV/SOS in `cfb_standings`).
- `quality_wins` -- count of wins over opponents with `adj_net` at or above
  the era `quality_win_threshold`.
- `game_control` -- mean postgame win expectancy `Phi(actual_margin /
  margin_sd)`, i.e. how *dominant* the results were, not just win/loss.
- `wab` -- wins above bubble: actual wins minus the expected wins of a
  bubble-quality team (`bubble_adj_net`) playing the same schedule, using the
  Phase-2 predictors with the HFA applied on the team's actual home/away side.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season or list of seasons. |
| `as_of_date` | `date \| None` | `None` | Leakage boundary forwarded to `cfb_ratings.cfb_ratings` (ratings use only games before this date). `None` uses the full season. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per team: `season`, `team_id` (Utf8), `sos`, `sos_rank` (Int64 dense rank, best = 1), `quality_wins` (Int64), `game_control` (Float64), `wab` (Float64). Zero-row (typed) when no games are available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the résumé covers (null for a pooled multi-season fit). |
| `team_id` | character | Team ESPN id (character join key). |
| `sos` | double | Rating-based strength of schedule - mean opponent adj_net over played games. |
| `sos_rank` | integer | Dense rank on sos descending (toughest schedule = 1). |
| `quality_wins` | integer | Count of wins over opponents with adj_net at or above the era quality-win threshold. |
| `game_control` | double | Mean postgame win expectancy Phi(actual_margin / margin_sd) across played games - how dominant the results were, not just win/loss. |
| `wab` | double | Wins above bubble - actual wins minus a bubble-quality team's expected wins over the same schedule. |

**Example**

```python
from sportsdataverse.cfb.cfb_resume import cfb_resume
resume = cfb_resume(2023)
resume.sort("sos_rank").head()
```

### cfb_returning_production {#cfb_returning_production}

`cfb_returning_production(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Returning production per team-season (offense / defense / overall).

For each requested season S, computes the fraction of season S-1 unit
production attributable to players on the season-S roster (Bill Connelly's
returning-production concept; unit weights from `get_constants`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Target season or list of seasons (production is drawn from S-1). |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per `(season, team_id)`: `off_returning`, `def_returning`, `overall_returning` (Float64 fractions in [0, 1]), `n_returning` (Int64 count of returning contributors), `def_basis`, `overall_basis` (Utf8, below), `is_estimated` (Boolean). `team_id` is the ESPN team id as Utf8 -- BREAKING vs the previous release, which emitted a normalized team NAME under `team` and joined at 57.7%. Zero-row (typed) when the box data is unavailable. `is_estimated` is True for **2004 only**, where season-2003 production is parsed from CFBD play text because ESPN's player box starts in 2004. Back-tested on 2005, where both sources exist, that route tracks the box route at r=0.73 (MAE 0.11) but reads about 0.085 LOW. It ranks teams well; it is not on the same level as its neighbours, so filter on this flag before comparing 2004 against another season. 2004 also carries a null `def_returning` -- 2003 play text has no defensive ids. `def_basis` names the defensive measure: `"participants"` when the production season is 2014+ (tackles, assists, tackles for loss, shared sacks, passes defended -- 92-100% of teams), `"pbp_splash"` for 2004-2013 (sacks, interceptions, pass breakups, forced fumbles; no tackle volume, so not on the same scale), `"box"` when that source's release is missing, null with `def_returning`. `overall_basis` is `"offense+defense"`, or `"offense"` for a team with no defensive value, whose overall then equals `off_returning`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the returning fractions describe (production drawn from the prior season). |
| `team_id` | character | ESPN team id as a string (integer-origin). |
| `off_returning` | double | Fraction of prior-season attributed offensive yardage (passing + rushing + receiving) returning on the current roster. |
| `def_returning` | double | Fraction of prior-season weighted defensive production returning on the current roster; the measure is named in def_basis. |
| `overall_returning` | double | Unit fractions combined with the fitted returning_prod_weights (FBS offense 0.49 / defense 0.51, the 2018-2025 fit in fit_returning_weights.py). |
| `n_returning` | integer | Count of prior-season contributors present on the current roster. |
| `def_basis` | character | Defensive measure behind def_returning: participants (production season 2014+: tackles, assists, tackles for loss, shared sacks, passes defended), pbp_splash (2004-2013: sacks, interceptions, pass breakups, forced fumbles; no tackle volume, so not on the same scale), box (the source release was missing), or null with def_returning. |
| `overall_basis` | character | Units in overall_returning: offense+defense, or offense for a team with no defensive value, whose overall then equals off_returning. |
| `is_estimated` | logical | True for 2004 only, where season-2003 production is parsed from play text; it reads about 0.085 low against the box route. |

**Example**

```python
from sportsdataverse.cfb import cfb_returning_production
rp = cfb_returning_production(2023)
rp.sort("overall_returning", descending=True).head(10)
```

### cfb_season_odds {#cfb_season_odds}

`cfb_season_odds(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, n_sims: 'int' = 10000, playoff_seeds: 'int' = 12, seed: 'int' = 0, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Ratings-driven season Monte Carlo: conference / playoff / championship odds.

Thin wrapper over `cfb_simulations.cfb_simulations` -- it builds the ratings
with `cfb_ratings.cfb_ratings`, converts the schedule to the engine format
with `cfb_standings.cfb_games_from_schedule` (re-keyed on ESPN `team_id` so
the ratings align), and feeds `make_ratings_compute_results` as the sampler.
All season / standings / bracket machinery is reused; unplayed games are simulated,
played games (before `as_of_date`) are kept. Only FBS programs (schedule
`division == "fbs"`) enter the simulated universe; non-FBS opponents stay in the
game set -- their games still count toward FBS records -- but can never reach the
standings, the playoff field, or the output.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (an `int`, or a one-element list). Multiple seasons raise `ValueError` -- the simulation engine is single-season. |
| `as_of_date` | `date \| None` | `None` | Leakage boundary applied to BOTH the ratings vintage and the game set. Ratings are fit only on plays from games with `date < as_of_date` (`cfb_ratings.cfb_ratings`), and schedule results from `start_date` on/after `as_of_date` are masked so those games are simulated instead of replayed; masked postseason rows are dropped (the matchup is itself an outcome) and regenerated from each sim's own standings. `None` uses the full season as-is. |
| `n_sims` | `int` | `10000` | Number of simulated seasons. |
| `playoff_seeds` | `int` | `12` | CFP field size. |
| `seed` | `int` | `0` | RNG seed for reproducibility. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per team: `season`, `team_id` (Utf8), `exp_wins`, `conf_title_prob`, `playoff_prob`, `first_round_bye_prob`, `cfp_champ_prob` (Float64 probabilities in [0, 1]). Zero-row (typed) when no ratings/schedule are available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season simulated (null for a pooled multi-season call). |
| `team_id` | character | Team ESPN id (character join key). |
| `exp_wins` | double | Mean wins per simulated season. |
| `conf_title_prob` | double | Share of simulations in which the team won its conference. |
| `playoff_prob` | double | Share of simulations in which the team made the College Football Playoff field. |
| `first_round_bye_prob` | double | Share of simulations in which the team earned a CFP first-round bye. |
| `cfp_champ_prob` | double | Share of simulations in which the team won the College Football Playoff national championship. |

**Example**

```python
from sportsdataverse.cfb.cfb_season_odds import cfb_season_odds
odds = cfb_season_odds(2023, n_sims=2000)
odds.sort("cfp_champ_prob", descending=True).head()
```

### cfb_transfer_impact {#cfb_transfer_impact}

`cfb_transfer_impact(target_season: 'int | list[int]', *, division: 'str' = 'fbs', alpha: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Net transfer talent and its projected win-total impact per team-season.

`pred_win_delta` comes from an on-demand ridge of realized win deltas on
`net_transfer_talent` fitted over strictly-prior seasons (the as-of
boundary is enforced internally per target season).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int \| list[int]` |  | Season (or list) to score. |
| `division` | `str` | `'fbs'` | Division slug for the star-points constants. |
| `alpha` | `float` | `1.0` | Ridge L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per (season, team_id): `net_transfer_talent` (Float64), `pred_win_delta` (Float64). Zero-row (typed) when no data.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the net transfer talent describes. |
| `team_id` | character | ESPN team id as a string (rosters team_id, cast Int64 to Utf8). |
| `net_transfer_talent` | double | Incoming minus outgoing transfer talent points for the season. |
| `pred_win_delta` | double | Ridge-projected win-total change from net transfer talent (as-of fit; weak observed validity - see the strict-xfail gate). |

**Example**

```python
from sportsdataverse.cfb import cfb_transfer_impact
imp = cfb_transfer_impact(2024)
imp.sort("net_transfer_talent", descending=True).head(10)
```

### cfb_transfer_moves {#cfb_transfer_moves}

`cfb_transfer_moves(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Transfer moves inferred from year-over-year roster diffs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Destination season(s) to extract moves for (each compares S-1 -> S). |
| `division` | `str` | `'fbs'` | Division slug for the star-points constants. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per move side: `season` (Int64, the destination season), `team_id` (Utf8 ESPN team id), `player_id` (Utf8 ESPN athlete id), `direction` ("in" | "out"), `prior_team_id` (Utf8 ESPN team id of the season S-1 team), `talent_points` (Float64, name-joined to the `cfb_recruits` release; the 0-star default when the player has no recruit rating). Zero-row (typed) when rosters are unavailable.

| col_name | type | description |
|---|---|---|
| `season` | integer | Destination season of the move (compares rosters S-1 to S). |
| `team_id` | character | ESPN team id (as a string) of the side this row describes (destination for "in", origin for "out"). |
| `player_id` | character | ESPN athlete id as a string. |
| `direction` | character | Move side - "in" (arriving at team_id) or "out" (leaving team_id). |
| `prior_team_id` | character | ESPN team id (as a string) of the season S-1 team. |
| `talent_points` | double | Recruit-star talent points (name-matched to the 247 recruit record; 0-star default when unrated). |

**Example**

```python
from sportsdataverse.cfb import cfb_transfer_moves
moves = cfb_transfer_moves(2024)
moves.filter(pl.col("direction") == "in").group_by("team_id").len()
```

### create_drive_summary {#create_drive_summary}

`create_drive_summary(drives: list[dict] | dict, frame: polars.dataframe.frame.DataFrame, home_id: str | int, away_id: str | int, periods: set[int] | str | None = None) -> dict | None`

Build the StatBroadcast-style drive summary, chart, and long-play lists.

A drive belongs to the quarter it STARTED in. On a windowed build the
full drive sequence still provides context (running score, the previous
drive for OBTAINED and points-off-turnovers), but only in-window drives
are counted, charted, or listed. `largest_lead` and the time-leading /
time-tied split window too: the score state is read from the whole
regulation play sequence and then clipped to the window's clock intervals
(one per contiguous run of quarters, so a gapped set never charges the
quarter it skipped). Under `"ot"` the clock has no axis to integrate over
and only `largest_lead` ships.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drives` | `list[dict]` |  | the ESPN drives grouping, in game order (`previous` plus the in-progress `current` drive, if any). |
| `frame` | `pl.DataFrame` |  | the enriched plays frame from `CFBPlayProcess.run_processing_pipeline` (`plays_frame`). |
| `home_id` | `str \| int` |  | ESPN home team id. |
| `away_id` | `str \| int` |  | ESPN away team id. |
| `periods` | `set[int] \| str \| None` | `None` | optional window -- a set of quarter numbers (e.g. `{1, 2}`) or the string `"ot"` (every period > 4). `None` = full game. |

**Returns**

`{"teams": {...}, "chart": [...], "scores": [...], "longPlays": {...}}` keyed by team id, or `None` when the inputs are unusable (no drives, empty frame, or an empty window).

**Example**

```python
summary = create_drive_summary(drives, game.plays_frame, "52", "61")
```

### create_situational_stats {#create_situational_stats}

`create_situational_stats(frame: polars.dataframe.frame.DataFrame, home_id: str | int, away_id: str | int, window_expr: polars.expr.expr.Expr | None = None) -> dict | None`

Build the situational team-stats block from a plays frame.

`two_minute` and `middle_8` are omitted from a windowed build: both name
a clock window of their own, so intersecting them with another window
describes neither (middle-8 inside Q1 is empty). Every other section,
`pace` and `non_garbage` included, is computed on the windowed slice and
ships with it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `frame` | `pl.DataFrame` |  | the enriched plays frame from `CFBPlayProcess.run_processing_pipeline` (`plays_frame`). |
| `home_id` | `str \| int` |  | ESPN home team id. |
| `away_id` | `str \| int` |  | ESPN away team id. |
| `window_expr` | `Expr \| None` | `None` | optional polars filter expression windowing the windowable sections to that slice (e.g. `pl.col("period") == 3`). `None` = full game. |

**Returns**

`{"teams": {<team_id>: {<section>: ...}}}` or `None` when the frame is unusable or the window is empty.

**Example**

```python
stats = create_situational_stats(game.plays_frame, "52", "61")
```

### make_ratings_compute_results {#make_ratings_compute_results}

`make_ratings_compute_results(ratings: 'pl.DataFrame', *, era: 'str' = 'modern') -> 'ComputeResultsFn'`

Build a `cfb_simulations` `compute_results` closure from fixed ratings.

The returned closure implements the engine's results contract -- `(teams, games,
week_num, *, rng, **kwargs) -> {"teams", "games"}` -- filling every unplayed
`week == week_num` game's `result` with a sampled home margin
`round(Normal(exp_margin, margin_sd))`, where `exp_margin` is
`cfb_game_predict.predict_margin` on the two teams' `adj_net` (home-field
applied unless `neutral`). Unlike the default elo sampler the ratings are
**fixed**, so `teams` passes through unchanged (no elo update). Postseason games
(`game_type != "REG"`) re-break a sampled tie by win probability.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | A `cfb_ratings.cfb_ratings`-style frame with `team_id` and `adj_net`. Teams absent from it are treated as league-average (0.0). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |

**Returns**

A `compute_results` callable suitable for `cfb_simulations(..., compute_results=...)`.

**Example**

```python
import numpy as np, polars as pl
from sportsdataverse.cfb.cfb_season_odds import make_ratings_compute_results
cr = make_ratings_compute_results(pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.3, -0.3]}))
teams = pl.DataFrame({"sim": [1, 1], "team": ["A", "B"], "conference": ["X", "X"]})
games = pl.DataFrame({"sim": [1], "week": [1], "home_team": ["A"], "away_team": ["B"],
                      "neutral": [0], "result": [None]})
cr(teams, games, 1, rng=np.random.default_rng(0))["games"]
```

### play_type_family_expr {#play_type_family_expr}

`play_type_family_expr(source: 'str' = 'play_type_canonical') -> 'pl.Expr'`

Build the polars expression mapping a canonical type to its phase family.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source` | `str` | `'play_type_canonical'` | Name of the canonical play-type column. |

**Returns**

A `pl.Expr` aliased `play_type_family`; unmapped values yield null.

**Example**

```python
import polars as pl
from sportsdataverse.cfb import add_play_type_canonical

pbp = pl.DataFrame({"type.text": ["Rush", "Timeout"]})
add_play_type_canonical(pbp).filter(
    pl.col("play_type_family") != "administrative"
)
```
