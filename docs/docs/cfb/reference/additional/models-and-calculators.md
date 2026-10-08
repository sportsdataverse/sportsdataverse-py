---
title: "CFB — additional Python functions — Models and calculators: add_era–efficiency_ratings"
sidebar_label: "Models and calculators: add_era–efficiency_ratings"
sidebar_position: 7
description: "CFB — additional Python functions — Models and calculators: add_era–efficiency_ratings — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Models and calculators: add_era–efficiency_ratings

### add_era_columns {#add_era_columns}

`add_era_columns(df: 'pl.DataFrame', model: 'str', season: 'int | None' = None) -> 'pl.DataFrame'`

Add the era column(s) `model` consumes, using ITS card's cuts.

The cuts are read from the published contract, never restated here. That is
the fix for cfbfastR-cfb-data#70, where both consumers kept a private copy of
the era boundary, both drifted to a 2017 cut the trainer never used, and
2018-2020 scored an era off the models trained with them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Frame carrying a `season` column, or any frame when `season` is given explicitly. |
| `model` | `str` |  | Bundle stem, used to look up the era contract. |
| `season` | `int \| None` | `None` | Season to use when `df` has no `season` column -- the hand-built-row case. |

**Returns**

`df` with the contract's columns added. Returned unchanged when the model declares no era contract, or when the columns are already present.

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

**Example**

```python
from sportsdataverse.cfb.model_calculators import add_era_columns
add_era_columns(pl.DataFrame({"season": [2018]}), "xpass_model")
```

### assert_rating_scale {#assert_rating_scale}

`assert_rating_scale(ratings: 'pl.DataFrame', *, era: 'str' = 'modern', tol: 'float' = 1.6) -> 'float'`

Warn if the ratings have drifted off the scale the constants were fit on.

THE FAILURE THIS PREVENTS. `net_points_scale` is a frozen statement about
a relationship between two things: rating units and points. When the
ratings change -- a different ridge penalty, a rescale, a rebuilt corpus --
the constant silently becomes wrong while every function keeps returning
plausible numbers. That is exactly what happened: the shipped 44.5367 was
fit on 2026-07-28, the ridge lambda moved on 08-01, the corpus was rebuilt
on 08-02, and nothing failed. Measured out-of-sample the result was a
calibration slope of 0.55 -- predictions stretched nearly 2x wider than
reality -- for two days, undetected.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | Team ratings frame carrying an `adj_net` column, as returned by `cfb_ratings.efficiency_ratings`. Frames without that column, or with fewer than 30 rows, are too thin to judge and return `1.0` unchecked. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`, used only to name the era in the warning text. |
| `tol` | `float` | `1.6` | Fold-change tolerance. The check fires outside `[1/tol, tol]`. |

**Returns**

The observed/fitted sd ratio. `1.0` when the frame is too thin to judge, so a caller can treat "1.0" as "no evidence of drift" either way.

**Example**

```python
from sportsdataverse.cfb import cfb_ratings
from sportsdataverse.cfb.cfb_game_predict import assert_rating_scale
ratings = cfb_ratings.efficiency_ratings(2024)
ratio = assert_rating_scale(ratings)

# Treat a large drift as a refit signal, not a nuisance warning

assert ratio < 1.6, "refit the constants before trusting predictions"
```

### calculate_completion_probability {#calculate_completion_probability}

`calculate_completion_probability(df, *, season=None, return_as_pandas=False)`

Completion probability for each pass attempt.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `down`, `distance`, `yards_to_goal`, `score_diff`, `seconds_remaining`, `is_home`, `period`, `passing_down`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `cp` column appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_completion_probability
calculate_completion_probability(df, season=2024)
```

### calculate_epa {#calculate_epa}

`calculate_epa(df, *, season=None, return_as_pandas=False)`

Expected points added: the change in EP across a play.

Recomputes `ep` when it is absent, matching nflfastR's behaviour. Requires
`ep_end` -- the expected points after the play -- because EPA is a
difference and this function scores rows, not sequences.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame with the EP features and an `ep_end` column. |
| `season` |  | `None` | Unused by the EP model; accepted for signature consistency. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with `ep` (if it was absent) and `epa` appended.

**Example**

```python
from sportsdataverse.cfb import calculate_epa
calculate_epa(pbp)
```

### calculate_expected_points {#calculate_expected_points}

`calculate_expected_points(df, *, season=None, return_as_pandas=False)`

Expected points for each row.

Mirrors `sportsdataverse.nfl.calculate_expected_points()`. The EP booster
is `multi:softprob` over seven next-score classes; this collapses those
probabilities to a points expectation using the package's own
`ep_class_to_score_mapping` rather than restating the class order.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `TimeSecsRem`, `yards_to_goal`, `distance`, `down_1` through `down_4` and `pos_score_diff_start`. |
| `season` |  | `None` | Unused by this model (EP consumes no era feature); accepted so every calculator shares one signature. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with the seven class probability columns and an `ep` column appended.

**Example**

```python
from sportsdataverse.cfb import calculate_expected_points
calculate_expected_points(pbp)
```

### calculate_field_goal_probability {#calculate_field_goal_probability}

`calculate_field_goal_probability(df, *, season=None, return_as_pandas=False)`

Field-goal make probability for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `yards_to_goal`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with an `fg_make_prob` column appended. Named `fg_make_prob`, not `fg_prob`: `calculate_expected_points` emits `fg_prob` for the probability the NEXT SCORE is a field goal, which is a different quantity. Sharing the name made chaining the two silently lossy. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_field_goal_probability
calculate_field_goal_probability(df, season=2024)
```

### calculate_fourth_down {#calculate_fourth_down}

`calculate_fourth_down(df, *, season=None, return_as_pandas=False)`

Fourth-down conversion model output for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `down`, `distance`, `yards_to_goal`, `posteam_total`, `posteam_spread`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with `fd_conversion_prob` (probability the gain reaches `distance`) and `fd_expected_yards` appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_fourth_down
calculate_fourth_down(df, season=2024)
```

### calculate_qbr {#calculate_qbr}

`calculate_qbr(df, *, season=None, return_as_pandas=False)`

Model QBR for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `qbr_epa`, `sack_epa`, `pass_epa`, `rush_epa`, `pen_epa`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `qbr` column appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_qbr
calculate_qbr(df, season=2024)
```

### calculate_two_point_probability {#calculate_two_point_probability}

`calculate_two_point_probability(df, *, season=None, return_as_pandas=False)`

Two-point conversion success probability.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `posteam_spread`, `posteam_total`, `pos_score_diff`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `two_pt_prob` column appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_two_point_probability
calculate_two_point_probability(df, season=2024)
```

### calculate_win_probability {#calculate_win_probability}

`calculate_win_probability(df, *, season=None, return_as_pandas=False)`

Win probability for each row.

Selects the booster the way the pipeline does: `wp_spread` when the frame
carries a `spread_time` column, `wp_naive` otherwise. The naive model is
the spread model minus that single feature, so which one applies is decided
by whether the caller has spread information at all.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying the win-probability features. Include `spread_time` to use the spread model. |
| `season` |  | `None` | Unused by these models; accepted for signature consistency. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `wp` column appended.

**Example**

```python
from sportsdataverse.cfb import calculate_win_probability
calculate_win_probability(pbp)
```

### calculate_wpa {#calculate_wpa}

`calculate_wpa(df, *, season=None, return_as_pandas=False)`

Win probability added: the change in WP across a play.

Recomputes `wp` when it is absent. Requires `wp_end` for the same reason
`calculate_epa` requires `ep_end`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame with the WP features and a `wp_end` column. |
| `season` |  | `None` | Unused by these models; accepted for signature consistency. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with `wp` (if it was absent) and `wpa` appended.

**Example**

```python
from sportsdataverse.cfb import calculate_wpa
calculate_wpa(pbp)
```

### calculate_xpass {#calculate_xpass}

`calculate_xpass(df, *, season=None, return_as_pandas=False)`

Expected pass probability for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `down`, `distance`, `yards_to_goal`, `pos_score_diff`, `TimeSecsRem`, `period`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with an `xpass` column (probability the play is a pass) appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_xpass
calculate_xpass(df, season=2024)
```

### cfb_adjusted_epa {#cfb_adjusted_epa}

`cfb_adjusted_epa(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float | None' = None, method: "Literal['current', 'pre598']" = 'current', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season opponent-adjusted per-team EPA from a season's play-by-play.

Fits one ridge of per-play `EPA` on offense-team, defense-team, and
home-field indicators (every team shrunk toward the league average by its own
play count) over the `0.05 <= wp_before_naive <= 0.95` pass
and rush plays, nets each team's per-game raw EPA against the opponent's
fitted strength, and averages to a season figure. In-sample/descriptive (the
fit uses the whole season); for leak-free per-game values use
`cfb_adjusted_epa_by_game`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the columns listed in the module docstring. One season at a time. |
| `ridge_lambda` | `float \| None` | `None` | Ridge penalty. Under `method="current"` it is per play of a full team season: each team keeps `n / (n + ridge_lambda * 577)` of its own signal for its `n` fit plays and is shrunk toward the league average by the rest (~7% at a full season, most of it on a handful of plays); must be > 0. Under `method="pre598"` it is passed unscaled to the old standardized ridge (the per-observation penalty; no 577 scaling, no positivity check). `None` (default) means 0.075 for `"current"` (the owner's choice, ADJ_EPA_LAMBDA`) and 0.035 for `"pre598"`. |
| `method` | `Literal['current', 'pre598']` | `'current'` | `"current"` (default) or `"pre598"`, the fit this function used before #598 (`0.1 <= wp_before <= 0.9` band, standardized ridge with the first team id as the reference level, lambda 0.035). pre598 reads `wp_before` instead of `wp_before_naive`. It exists for nfl-data's NFL team summaries, is not validated for NFL either, and is kept only for continuity until NFL is validated. |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per team (>= 2 valid games): `team_id`, `pos_team`, `valid_games`, `adj_off_epa`, `adj_def_epa`, `off_strength_faced`, `def_strength_faced`, `net_adj_epa` and their `*_rank` columns.

No returns table is published for this function: no capture: it needs play-by-play joined with schedule fields (home, neutral_site, pos_team_id) that neither load_cfb_pbp nor load_cfb_pbp_r carries; only cfb_ratings builds that join, internally.

**Example**

```python
import sportsdataverse.cfb as cfb
pbp = cfb.load_cfb_pbp(seasons=[2023])
cfb.cfb_adjusted_epa(pbp).sort("net_adj_epa_rank").head()

# NFL team summaries (the pre-#598 method; reads wp_before)

cfb.cfb_adjusted_epa(nfl_plays, method="pre598")
```

### cfb_adjusted_epa_by_game {#cfb_adjusted_epa_by_game}

`cfb_adjusted_epa_by_game(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float | None' = None, method: "Literal['current', 'pre598']" = 'current', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Walk-forward (point-in-time) opponent-adjusted EPA, one row per team-game.

For each week `w` the opponent-strength ridge is fit on FIT_WP`-band plays
from **weeks before `w` only**, then that week's games are adjusted with
those as-of strengths -- so the value uses no future information and is valid
as an in-season power-rating / model feature. Week 1 (no prior) yields null
adjustments; not-yet-seen opponents fall back to the league baseline (an
average team), and teams seen on few plays are shrunk most of the way there.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the module-docstring columns **plus** `week`. One season at a time. |
| `ridge_lambda` | `float \| None` | `None` | Ridge penalty. Under `method="current"` it is per play of a full team season: each team keeps `n / (n + ridge_lambda * 577)` of its own signal for its `n` fit plays and is shrunk toward the league average by the rest (~7% at a full season, most of it on a handful of plays); must be > 0. Under `method="pre598"` it is passed unscaled to the old standardized ridge (the per-observation penalty; no 577 scaling, no positivity check). `None` (default) means 0.075 for `"current"` (the owner's choice, ADJ_EPA_LAMBDA`) and 0.035 for `"pre598"`. |
| `method` | `Literal['current', 'pre598']` | `'current'` | `"current"` (default) or `"pre598"`, the fit this function used before #598 (see `cfb_adjusted_epa`). pre598 also keeps the old week order: it sorts by `week` alone and does not read `seasonType`, so postseason games that restart at week 1 are fit with (and leak into) the regular season, exactly as before. It exists for nfl-data's NFL team summaries, is not validated for NFL either, and is kept only for continuity until NFL is validated. |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (game, team), sorted by `week` then `team_id`: `game_id`, `week`, `team_id`, `opponent_id`, `pos_team`, `raw_off_epa`, `adj_off_epa`, `raw_def_epa`, `adj_def_epa`, `off_strength_faced` (opponent offense), `def_strength_faced` (opponent defense), `net_adj_epa`. The `adj_*` / `net` columns are null for week 1 (and any week with no prior fit).

No returns table is published for this function: no capture: it needs play-by-play joined with schedule fields (home, neutral_site, pos_team_id) that neither load_cfb_pbp nor load_cfb_pbp_r carries; only cfb_ratings builds that join, internally.

**Example**

```python
import sportsdataverse.cfb as cfb
pbp = cfb.load_cfb_pbp(seasons=[2023])
tg = cfb.cfb_adjusted_epa_by_game(pbp)
tg.filter(pl.col("week") >= 5).sort("net_adj_epa", descending=True).head()
```

### cfb_compute_results {#cfb_compute_results}

`cfb_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'int', *, rng: 'Optional[np.random.Generator]' = None, elo: 'Optional[Dict[str, float]]' = None, **kwargs: 'Any') -> 'Dict[str, pl.DataFrame]'`

Default results generator — nflseedR's dynamic ELO model for CFB.

Fills `result` for week `week_num` games that are still unplayed and
updates each team's ELO rating from that week's results (real results
included). Constants are nflseedR's `nflseedR_compute_results` exactly,
minus the NFL rest-day adjustment (CFB plays weekly — documented
simplification).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `DataFrame` |  | Per-sim team table (`sim`, `team`, `conference`, optionally `elo` carried over from the previous week). |
| `games` | `DataFrame` |  | Per-sim games table (engine schema; see `sportsdataverse.cfb.cfb_standings`). |
| `week_num` | `int` |  | The week to fill. |
| `rng` | `Optional[Generator]` | `None` | numpy Generator (seeded by `cfb_simulations`). A fresh default generator is created when omitted. |
| `elo` | `Optional[Dict[str, float]]` | `None` | Optional initial ratings `{team: elo}` applied to every sim. Teams missing from the dict start at 1500. When neither `elo` nor a `teams.elo` column exists, ratings initialize randomly at `N(1500, 150)` per (sim, team) — nflseedR behavior. |

**Returns**

`{"teams": ..., "games": ...}` — updated frames, mirroring nflseedR's returned list.

| col_name | type | description |
|---|---|---|
| `sim` | integer | Simulation identifier the game row belongs to (1..n simulated seasons; ELO ratings never mix across simulations). |
| `week` | integer | Week of the season the game is played in; only games matching the requested week_num are filled. |
| `game_type` | character | Game classification in the seedr engine schema - REG (regular season), CONF_CHAMP (conference championship) or POST (postseason/CFP). |
| `home_team` | character | Team name of the home team in the simulated game (returned games frame). |
| `away_team` | character | Team name of the away team in the simulated game (returned games frame). |
| `result` | double | Home-team margin of victory (home score minus away score) - real results are preserved and the target week's unplayed games are filled from the ELO model. |
| `neutral` | integer | Neutral-site flag (1 = neutral site, 0 = true home game; only non-neutral games receive the ELO home bump). |

**Example**

```python
from sportsdataverse.cfb.cfb_simulations import cfb_compute_results
out = cfb_compute_results(teams, games, 5, rng=rng)
teams, games = out["teams"], out["games"]
```

### cfb_draft_projection {#cfb_draft_projection}

`cfb_draft_projection(target_draft_year: 'int', *, division: 'str' = 'fbs', history_years: 'list[int] | None' = None, l2: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame]'`

Project NFL-draft probability per player + expected picks per team.

Fits an L2 logistic of `drafted` on `[recruit_stars, talent_points,
career_production_z, class_year]` over draft years strictly before the
target (the as-of boundary, enforced internally), then scores the target
year's eligible players.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_draft_year` | `int` |  | Draft year to project. |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `history_years` | `list[int] \| None` | `None` | Training draft years (default: the five before target). |
| `l2` | `float` | `1.0` | Logistic L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, both frames return as pandas. |

**Returns**

`{"players": ..., "teams": ...}` — players: `draft_year` (Int64), `team_id` / `player_id` / `player_name` (Utf8), `draft_prob` (Float64); teams: `draft_year`, `team_id`, `proj_draft_picks` (Float64, the sum of member draft probabilities). Zero-row (typed) frames when no data is available.

**Example**

```python
from sportsdataverse.cfb import cfb_draft_projection
out = cfb_draft_projection(2024)
out["teams"].sort("proj_draft_picks", descending=True).head(10)
```

### cfb_field_position {#cfb_field_position}

`cfb_field_position(seasons: 'Union[int, list[int]]', *, exclude_garbage: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team-season field-position value: avg start, drive EP, margin, pts/drive.

Derives one row per drive from `load_cfb_pbp`, values each starting
yard line with the bundled EP curve, and aggregates per (season, team):
`avg_start_yardline` (yards from own goal, higher = better),
`fp_ep` (mean drive-start EP), `fp_margin` (own `fp_ep` minus the
mean drive-start EP of opponents' drives faced), and
`points_per_drive` (mean realized offensive points: TD=7, FG=3;
non-offensive negative results such as safeties and defensive return
TDs are floored to 0 before averaging).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | season or list of seasons (hosted pbp covers 2002-2021). |
| `exclude_garbage` | `bool` | `True` | drop drives that start in Connelly garbage time. |
| `return_as_pandas` | `bool` | `False` | return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (season, team_id); zero-row frame with the documented schema on empty input.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the field-position stats cover. |
| `team_id` | character | Team ESPN id (character join key). |
| `drives` | integer | Offensive drives counted (garbage-time drives excluded by default). |
| `avg_start_yardline` | double | Mean drive-start yard line from the team's own goal (higher = better field position). |
| `fp_ep` | double | Mean bundled expected points of the team's drive starts. |
| `fp_margin` | double | Own fp_ep minus the mean drive-start EP of opponents' drives faced. |
| `points_per_drive` | double | Mean realized offensive points per drive (TD=7, FG=3). |

**Example**

```python
from sportsdataverse.cfb import cfb_field_position
df = cfb_field_position([2021])
print(df.shape)

# Pipeline next step (one line)

df.sort("fp_margin", descending=True).head()
```

### cfb_predict_games {#cfb_predict_games}

`cfb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Predict a whole schedule of games from a ratings frame (vectorized).

Applies the three closed-form predictors across every row of `games` in
one pass. `ratings` is joined twice -- once on `home_team_id` and once on
`away_team_id` -- so each game carries both teams' `adj_net` / `adj_off_epa`
/ `adj_def_epa` / `off_pace`. The totals model's `game_pace` factor is
computed here as `home_off_pace * away_off_pace / league_avg_pace`, where the
league average is the mean `off_pace` of the passed ratings frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | Schedule frame with `game_id`, `home_team_id`, `away_team_id`, and `neutral_site` columns. The two team-id columns must share the dtype of `ratings["team_id"]` (asserted before the join). |
| `ratings` | `DataFrame` |  | A `cfb_ratings.cfb_ratings`-style frame with `team_id`, `adj_net`, `adj_off_epa`, `adj_def_epa`, and `off_pace`. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per game with `game_id`, `home_team_id`, `away_team_id`, `neutral_site`, `exp_margin`, `home_win_prob`, `exp_total`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Game identifier carried through from the input schedule. |
| `home_team_id` | character | Home team ESPN id (character; the ratings `team_id` join key). |
| `away_team_id` | character | Away team ESPN id (character; the ratings `team_id` join key). |
| `neutral_site` | logical | Whether the game is at a neutral site (home-field advantage is dropped when true). |
| `exp_margin` | double | Expected home scoring margin in points (net_points_scale * net rating differential + the ridge-native home-field advantage on non-neutral fields). |
| `home_win_prob` | double | Home win probability, Phi(exp_margin / margin_sd) under a Gaussian margin model. |
| `exp_total` | double | Expected combined point total from the fitted efficiency + pace totals model. |

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import cfb_predict_games
from sportsdataverse.cfb import cfb_ratings
from sportsdataverse.cfb.cfb_schedule import cfb_schedule  # schedule loader
ratings = cfb_ratings(2023)
preds = cfb_predict_games(schedule_2023, ratings)
```

### cfb_ratings {#cfb_ratings}

`cfb_ratings(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, config: 'RatingsConfig | None' = None, fbs_only: 'bool' = True, drop_kneels: 'bool' = True, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

One row per team: the full CFB ratings spine (off/def/ST EPA + FEI).

Public orchestrator over `efficiency_ratings`,
`special_teams_ratings`, and `fei_ratings`. Loads play-by-play
+ schedule via `sportsdataverse.cfb.cfb_loaders.load_cfb_pbp` /
`sportsdataverse.cfb.cfb_loaders.load_cfb_schedule`, joins the
schedule's per-game date onto the plays, optionally applies the
as-of-date leakage boundary
(`sportsdataverse.cfb.cfb_prediction_constants.as_of_ratings_split`),
then fits all three component ratings on the (optionally filtered) plays
and reshapes them into one wide per-team table with dense ranks and a
net-rating z-score.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (e.g. `2023`) or a list of seasons to pool into one combined fit. |
| `as_of_date` | `date \| None` | `None` | When given, the leakage boundary -- only plays from games with `date < as_of_date` are used to fit the ratings (mirrors what was knowable heading into that date). `None` (default) uses the full season(s), unfiltered. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs forwarded to all three component functions. Defaults to `RatingsConfig` when omitted. |
| `fbs_only` | `bool` | `True` | Keep only FBS-vs-FBS games (gameonpaper `cfb-team-summaries` parity) -- both of the schedule's `home_division` / `away_division` must be `"fbs"`. Default True. Skipped (all games kept) when the schedule lacks the division columns; pass False to rate FCS opponents as regular teams. |
| `drop_kneels` | `bool` | `True` | Strip kneel-downs before fitting (gameonpaper parity). Default True. Uses a pipeline `kneel_down` flag when present, otherwise the play-text regex (`kneel` / `takes a knee`) plus the end-of-half anonymized-TEAM-run clock heuristic; skipped when neither a flag nor a play-text column exists. Pass False to let kneels with non-null EPA flow into the fit. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

A DataFrame with one row per `team_id`, columns in this order: `season` (Int64 -- the single passed season for the common single-season call; `null` for a pooled multi-season call, since no single season applies to every row), `team_id` (Utf8), `adj_off_epa`, `adj_def_epa` (Float64, from `efficiency_ratings`), `adj_st_epa` (Float64, from `special_teams_ratings`), `adj_net` (Float64 -- offense minus defense only; special teams is a separate column, not folded in), `fei_off`, `fei_def`, `fei_net` (Float64, from `fei_ratings`), `games` (Int64), `off_pace` (Float64 -- scrimmage plays per game, the tempo input the totals model uses), `off_rank` (Int64, dense rank on `adj_off_epa` descending), `def_rank` (Int64, dense rank on `adj_def_epa` **ascending** -- fewer EPA allowed ranks better), `net_rank` (Int64, dense rank on `adj_net` descending), `net_z` (Float64, z-score of `adj_net`), `fei_off_rank` (Int64, dense rank on `fei_off` descending), `fei_def_rank` (Int64, dense rank on `fei_def` **ascending** -- fewer drive EPA allowed ranks better), `fei_net_rank` (Int64, dense rank on `fei_net` descending). Zero-row (correctly-typed) when the requested season(s) have no published pbp/schedule asset, or when `as_of_date` filters out every play.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season (4-digit year). |
| `team_id` | character | ESPN team id. |
| `adj_off_epa` | double |  |
| `adj_def_epa` | double |  |
| `adj_st_epa` | double |  |
| `adj_net` | double |  |
| `fei_off` | double |  |
| `fei_def` | double |  |
| `fei_net` | double |  |
| `games` | integer | Number of games included in the ATS summary. |
| `off_pace` | double |  |
| `off_rank` | integer |  |
| `def_rank` | integer |  |
| `net_rank` | integer |  |
| `net_z` | double |  |
| `fei_off_rank` | integer |  |
| `fei_def_rank` | integer |  |
| `fei_net_rank` | integer |  |

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import cfb_ratings
ratings = cfb_ratings(2023)
ratings.sort("net_rank").head()

# As-of-date leakage boundary

import datetime as dt
week3 = cfb_ratings(2023, as_of_date=dt.date(2023, 9, 18))

# Pandas round-trip

ratings_pd = cfb_ratings(2023, return_as_pandas=True)
```

### cfb_recruiting_projection {#cfb_recruiting_projection}

`cfb_recruiting_projection(target_season: 'int', *, division: 'str' = 'fbs', history_seasons: 'list[int] | None' = None, alpha: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Project team wins / scoring margin for a season from preseason roster features.

Fits a ridge regression of realized wins (and average scoring margin) on
`[talent_composite, blue_chip_ratio, off_returning, def_returning,
prior_wins]` over strictly-prior seasons, then predicts the target season
from its preseason-known features. The as-of boundary is enforced
internally: rows with `season >= target_season` never enter training even
if `history_seasons` includes them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int` |  | Season to project. |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `history_seasons` | `list[int] \| None` | `None` | Seasons to draw training rows from (default: the six seasons before `target_season`). |
| `alpha` | `float` | `1.0` | Ridge L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per team: `season` (Int64, = target), `team_id` (Utf8 ESPN id), `pred_wins`, `pred_margin` (Float64), `pred_net_epa` (Float64, currently null -- the adjusted-EPA target's hosted pbp source 404s). Zero-row (typed) when no history is available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Target season being projected (equals the requested target_season). |
| `team_id` | character | ESPN team id as a string (integer-origin). |
| `pred_wins` | double | Ridge-projected season win total from preseason roster features. |
| `pred_margin` | double | Ridge-projected average scoring margin per game. |
| `pred_net_epa` | double | Reserved adjusted-EPA projection - currently null (the hosted pbp source 404s). |

**Example**

```python
from sportsdataverse.cfb import cfb_recruiting_projection
proj = cfb_recruiting_projection(2024)
proj.sort("pred_wins", descending=True).head(10)
```

### cfb_roster_talent {#cfb_roster_talent}

`cfb_roster_talent(seasons: 'int | list[int]', *, division: 'str' = 'fbs', composite_247: 'pl.DataFrame | None' = None, max_class_size: 'int' = 25, rank_decay: 'float' = 0.75, recruits: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Team-talent composite per team-season (247 Team Talent Composite style).

Talent is the class-recency-weighted sum of per-recruit star points over the
trailing eligible recruiting classes (window = the length of the division's
`class_recency_weights`). When a 247 team-talent snapshot is supplied via
`composite_247`, its value overrides the derived composite for matched
team-seasons (the derived value remains the fallback).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Target season or list of seasons to rate. |
| `division` | `str` | `'fbs'` | Division slug for `get_constants` (star points, weights). |
| `composite_247` | `DataFrame \| None` | `None` | Optional frame with `season` (Int64), `team_id` (Utf8), `talent_247` (Float64). Join-key dtypes are asserted. |
| `max_class_size` | `int` | `25` | Top-N recruits per class that count toward `talent_composite`, ranked by star points. Defaults to the FBS limit of 25 initial counters. Raise it only deliberately: an uncapped sum measures class VOLUME, which put Air Force 7th nationally on 200 signees at a 0.000 blue-chip ratio. Largely superseded by `rank_decay`; retained as a hard floor. |
| `rank_decay` | `float` | `0.75` | Diminishing-returns exponent on a recruit's rank within their class (see RANK_DECAY`). 0.0 restores the flat sum. The default 0.75 was selected by sweeping against Spearman with actual wins, not chosen by taste. |
| `recruits` | `DataFrame \| None` | `None` | Pre-loaded per-recruit frame (the `load_recruit_classes` contract). Supplying it SKIPS the 247 fetch entirely, which is what the cfbfastR-cfb-data producer does when compiling from the raw store: a class is immutable once signed, but the composite spans a 4-season window, so fetching live re-pulled the same frozen classes once per target season (~20 min per call). Callers passing this own the frame's completeness. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per `(season, team_id)`: `team` (Utf8), `talent_composite` (Float64), `talent_rank` (Int64 dense rank desc within season), `blue_chip_ratio` (Float64), `n_recruits` (Int64). Zero-row (typed) when no recruits load.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the talent composite describes (trailing eligible classes aggregated). |
| `team_id` | character | 247Sports signed-institution team key as a string (integer-origin; joins to the recruit feed, not ESPN). |
| `team` | character | 247Sports full team name - the cross-source name-join key (the recruit-feed and talent-feed id spaces differ). |
| `talent_composite` | double | Class-recency-weighted sum of per-recruit star points (247 Team Talent Composite style); the 247 snapshot value when composite_247 is supplied. |
| `talent_rank` | integer | Dense rank on talent_composite descending within season (best = 1). |
| `blue_chip_ratio` | double | Share of the trailing four signing classes rated 4+ stars. |
| `n_recruits` | integer | Total signees across the trailing recruiting-class window. |

**Example**

```python
from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent
tal = cfb_roster_talent(2023)
tal.sort("talent_rank").head(10)
```

### cfb_simulations {#cfb_simulations}

`cfb_simulations(games: 'FrameLike', teams: 'FrameLike', compute_results: 'Optional[ComputeResultsFn]' = None, *, simulations: 'int' = 10000, playoff_seeds: 'int' = 12, tiebreaker_depth: 'str' = 'SOS', sim_include: 'str' = 'POST', rankings: 'Optional[FrameLike]' = None, seed: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Dict[str, Union[pl.DataFrame, Any]]'`

Simulate college football seasons (nflseedR-style week loop).

Replicates the input season `simulations` times, fills unplayed games
week by week through the pluggable `compute_results`, then simulates
the postseason (conference championships + CFP bracket) and aggregates
per-team probabilities. See the module docstring for every documented
CFB simplification.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `FrameLike` |  | One season of games in the engine schema (`season` or `sim`, `week`, `game_type`, `home_team`, `away_team`, `result` — null = unplayed, `neutral`). Played results are kept as-is. |
| `teams` | `FrameLike` |  | Team table (`team`, `conference`). |
| `compute_results` | `Optional[ComputeResultsFn]` | `None` | Results generator with the signature `fn(teams, games, week_num, **kwargs) -> {"teams": ..., "games": ...}` filling `result` for that week's unplayed games only. Defaults to `cfb_compute_results` (dynamic ELO). |
| `simulations` | `int` | `10000` | Number of simulated seasons (sequential, no chunking). |
| `playoff_seeds` | `int` | `12` | CFP field size passed to `cfb_playoff_seeds`. |
| `tiebreaker_depth` | `str` | `'SOS'` | nflseedR depth ladder (`RANDOM` < `PRE-SOV` < `SOS` < `POINTS`) used by every standings computation. |
| `sim_include` | `str` | `'POST'` | How deep to simulate: `"REG"` (regular season only), `"CONF"` (+ conference championships) or `"POST"` (+ CFP bracket, default). |
| `rankings` | `Optional[FrameLike]` | `None` | Optional committee rankings (`team`, `rank`) forwarded to `cfb_playoff_seeds`. When None, seeding falls back to the per-sim standings ordering (documented in `cfb_playoff_seeds`). |
| `seed` | `Optional[int]` | `None` | Seed for the numpy RNG (deterministic runs). |
| `return_as_pandas` | `bool` | `False` | Return pandas DataFrames instead of polars. |

**Returns**

Dict of frames mirroring the nflseedR summary list: * `"standings"` — per (sim, team) standings incl. `conf_rank`, `conf_champ` and (`sim_include="POST"`) `seed`. * `"games"` — all games incl. simulated results and generated postseason rows. * `"overall"` — per-team probabilities (`won_conf`, `made_playoff`, `first_round_bye`, `won_cfp`) and mean record columns. * `"game_summary"` — per unique matchup: games played, home win / tie rates and mean margin.

| col_name | type | description |
|---|---|---|
| `team` | character | Team name the simulated probabilities belong to (overall summary frame). |
| `conference` | character | Conference the team belongs to; null or "FBS Independents" marks an independent. |
| `wins` | double | Mean wins per simulated season (all game types through the conference championship). |
| `losses` | double | Mean losses per simulated season (all game types through the conference championship). |
| `ties` | double | Mean ties per simulated season. |
| `win_pct` | double | Mean overall win percentage across the simulated seasons. |
| `won_conf` | double | Share of simulations in which the team won its conference (CONF_CHAMP game winner, or rank-1 fallback). |
| `made_playoff` | double | Share of simulations in which the team made the College Football Playoff field. |
| `first_round_bye` | double | Share of simulations in which the team earned a CFP first-round bye (seed 4 or better). |
| `won_cfp` | double | Share of simulations in which the team won the College Football Playoff national championship. |

**Example**

```python
from sportsdataverse.cfb import cfb_simulations
out = cfb_simulations(games, teams, simulations=100, seed=42,
                      playoff_seeds=12)
print(out["overall"].sort("won_cfp", descending=True).head())

# Regular season only

out = cfb_simulations(games, teams, simulations=100,
                      sim_include="REG", seed=1)
```

### efficiency_ratings {#efficiency_ratings}

`efficiency_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted offensive/defensive efficiency.

Fits the offense/defense ridge from `cfb_adjusted_epa` on the
competitive plays in `plays` (`min_competitive_wp <= wp_before <=
max_competitive_wp`), then nets each team's raw per-game EPA (all
pass/rush plays, garbage time included) against the opponent's fitted
strength and averages across games -- the R `adjust_epa` /
gameonpaper `team_agg.R` statistic and scale (a top team nets
~0.30-0.40/play; the pre-2026-07-28 coefficient+intercept scale ran
~1.8x hotter). The ridge's dropped reference team nets normally from
its own games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying every column in `cfb_adjusted_epa._REQUIRED_COLUMNS` (`game_id`, `pos_team`, `pos_team_id`, `def_pos_team_id`, `home`, `neutral_site`, `EPA`, `pass`, `rush`, `wp_before`). Callers pass an already as-of-date-filtered frame; this function is pure. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs. Only `ridge_lambda` is consulted here; defaults to `RatingsConfig` when omitted. |

**Returns**

A `polars.DataFrame` with one row per `team_id`: `team_id` (Utf8), `adj_off_epa` / `adj_def_epa` / `adj_net` (Float64), `games` (Int64), `off_pace` (Float64 -- scrimmage plays per game, the tempo input the totals model consumes). Empty (zero-row, correctly-typed) when `plays` has no competitive plays.

No returns table is published for this function: no capture: it needs play-by-play joined with schedule fields (home, neutral_site, pos_team_id) that neither load_cfb_pbp nor load_cfb_pbp_r carries; only cfb_ratings builds that join, internally.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
ratings = efficiency_ratings(pbp)
ratings.sort("adj_net", descending=True).head()

# Custom ridge penalty

from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig
ratings = efficiency_ratings(pbp, config=RatingsConfig(ridge_lambda=100.0))
```
