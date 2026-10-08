---
title: "NFL — additional Python functions — Models and calculators: get_4th"
sidebar_label: "Models and calculators: get_4th"
sidebar_position: 20
description: "NFL — additional Python functions — Models and calculators: get_4th — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Models and calculators: get_4th

### get_4th_down_probs {#get_4th_down_probs}

`get_4th_down_probs(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Full 4th-down decision surface (nfl4th `add_4th_probs`) + recommendation.

Runs `get_go_wp`, `get_fg_wp`, `get_punt_wp` on the
fourth-down rows and adds the combined option columns plus:

* `go_boost` -- nfl4th's headline number: `100 * (go_wp - max(fg_wp,
  punt_wp))` in percentage points (a NaN `punt_wp` is treated as 0).
* `fourth_down_recommendation` -- the max-WP choice among `{go, punt,
  field_goal}` (NaN options are excluded).
* `go_wp_diff` / `punt_wp_diff` / `fg_wp_diff` -- each option's WP minus
  the recommended option's WP (the recommended option's diff is 0, the others
  <= 0).  NaN where the option WP is NaN.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations (the nflverse-shape output of `load_nfl_pbp`; see module docstring for required columns). |

**Returns**

A pandas copy of `pbp_df` with the decision columns added. Empty input returns the input plus empty decision columns.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `play_id` | integer | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `play_seq` | double |  |
| `posteam` | character | String abbreviation for the team with possession. |
| `defteam` | character | String abbreviation for the team on defense. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `away_team` | character | String abbreviation for the away team. |
| `home` | integer |  |
| `qtr` | integer | Quarter of the game (5 is overtime). |
| `game_half` | character | String indicating which half the play is in, either Half1, Half2, or Overtime. |
| `down` | double | The down for the given play. |
| `ydstogo` | integer | Numeric yards in distance from either the first down marker or the endzone in goal down situations. |
| `yardline_100` | double | Numeric distance in the number of yards from the opponent's endzone for the posteam. |
| `goal_to_go` | integer | Binary indicator for whether or not the posteam is in a goal down situation. |
| `quarter_seconds_remaining` | integer | Numeric seconds remaining in the quarter. |
| `half_seconds_remaining` | integer | Numeric seconds remaining in the half. |
| `game_seconds_remaining` | integer | Numeric seconds remaining in the game. |
| `play_type` | character | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `yards_gained` | double | Numeric yards gained (or lost) by the possessing team, excluding yards gained via fumble recoveries and laterals. |
| `desc` | character | Detailed string description for the given play. |
| `shield_play_type` | character |  |
| `special_teams_play_type` | character |  |
| `sp` | integer | Binary indicator for whether or not a score occurred on the play. |
| `pass_attempt` | integer | Binary indicator for if the play was a pass attempt (includes sacks). |
| `complete_pass` | integer | Binary indicator for if the pass was completed. |
| `incomplete_pass` | integer | Binary indicator for if the pass was incomplete. |
| `interception` | integer | Binary indicator for if the pass was intercepted. |
| `rush_attempt` | integer | Binary indicator for if the play was a run. |
| `sack` | integer | Binary indicator for if the play ended in a sack. |
| `touchdown` | integer | Binary indicator for if the play resulted in a TD. |
| `pass_touchdown` | integer | Binary indicator for if the play resulted in a passing TD. |
| `rush_touchdown` | integer | Binary indicator for if the play resulted in a rushing TD. |
| `return_touchdown` | integer | Binary indicator for if the play resulted in a return TD. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `field_goal_attempt` | integer | Binary indicator for field goal attempt. |
| `field_goal_made` | integer |  |
| `field_goal_missed` | integer |  |
| `field_goal_blocked` | integer |  |
| `extra_point_attempt` | integer | Binary indicator for extra point attempt. |
| `two_point_attempt` | integer | Binary indicator for two point conversion attempt. |
| `punt_attempt` | integer | Binary indicator for punts. |
| `kickoff_attempt` | integer | Binary indicator for kickoff. |
| `penalty` | integer | Binary indicator for whether or not a penalty occurred. |
| `fumble` | integer | Binary indicator for if a fumble occurred. |
| `fumble_lost` | integer | Binary indicator for if the fumble was lost. |
| `qb_hit` | integer | Binary indicator if the QB was hit on the play. |
| `safety` | integer | Binary indicator for whether or not a safety occurred. |
| `timeout` | integer | Binary indicator for whether or not a timeout was called by either team. |
| `first_down_rush` | integer | Binary indicator for if a running play converted the first down. |
| `first_down_pass` | integer | Binary indicator for if a passing play converted the first down. |
| `first_down_penalty` | integer | Binary indicator for if a penalty converted the first down. |
| `solo_tackle` | integer | Binary indicator if the play had a solo tackle (could be multiple due to fumbles). |
| `assist_tackle` | integer | Binary indicator for if an assist tackle occurred. |
| `tackle_with_assist` | integer | Binary indicator for if there has been a tackle with assist. |
| `tackled_for_loss` | integer | Binary indicator for whether or not a tackle for loss on a run play occurred. |
| `fumble_forced` | integer | Binary indicator for if the fumble was forced. |
| `fumble_not_forced` | integer | Binary indicator for if the fumble was not forced. |
| `fumble_out_of_bounds` | integer | Binary indicator for if the fumble went out of bounds. |
| `punt_fair_catch` | integer | Binary indicator for if the punt was caught with a fair catch. |
| `punt_downed` | integer | Binary indicator for if the punt was downed. |
| `punt_out_of_bounds` | integer | Binary indicator for if the punt went out of bounds. |
| `kickoff_fair_catch` | integer | Binary indicator for if the kickoff was caught with a fair catch. |
| `kickoff_out_of_bounds` | integer | Binary indicator for if the kickoff went out of bounds. |
| `extra_point_good` | integer |  |
| `extra_point_failed` | integer |  |
| `extra_point_blocked` | integer |  |
| `extra_point_safety` | integer |  |
| `extra_point_aborted` | integer |  |
| `two_point_rush_good` | integer |  |
| `two_point_rush_failed` | integer |  |
| `two_point_rush_safety` | integer |  |
| `two_point_pass_good` | integer |  |
| `two_point_pass_failed` | integer |  |
| `two_point_pass_safety` | integer |  |
| `two_point_pass_reception_good` | integer |  |
| `two_point_pass_reception_failed` | integer |  |
| `two_point_return` | integer |  |
| `def_tackles_for_loss` | integer | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | integer | Yards lost from TFLs involving this player |
| `td_ids_touchdown` | integer |  |
| `misc_yards` | integer |  |
| `fumble_recovery_own_lateral_yards` | integer |  |
| `fumble_recovery_opp_lateral_yards` | integer |  |
| `air_yards` | double | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `yards_after_catch` | double | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `penalty_yards` | double | Yards gained (or lost) by the posteam from the penalty. |
| `kick_distance` | double | Numeric distance in yards for kickoffs, field goals, and punts. |
| `return_yards` | double | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `lateral_rushing_yards` | character | Numeric yards by the `lateral_rusher_player_name` in run plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_receiving_yards` | character | Numeric yards by the `lateral_receiver_player_name` in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `passer_player_name` | character | String name for the player that attempted the pass. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `rusher_player_name` | character | String name for the player that attempted the run. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `receiver_player_name` | character | String name for the targeted receiver. |
| `td_player_id` | character | Unique identifier of the player who scored a touchdown. |
| `td_player_name` | character | String name of the player who scored a touchdown. |
| `td_team` | character | String abbreviation for which team scored the touchdown. |
| `penalty_team` | character | String abbreviation of the team with the penalty. |
| `timeout_team` | character | String abbreviation for which team called the timeout. |
| `kicker_player_id` | character | Unique identifier for the kicker on FG or kickoff. |
| `kicker_player_name` | character | String name for the kicker on FG or kickoff. |
| `punter_player_id` | character | Unique identifier for the punter. |
| `punter_player_name` | character | String name for the punter. |
| `punt_returner_player_id` | character | Unique identifier for the punt returner. |
| `punt_returner_player_name` | character | String name for the punt returner. |
| `kickoff_returner_player_id` | character | Unique identifier for the kickoff returner. |
| `kickoff_returner_player_name` | character | String name for the kickoff returner. |
| `return_team` | character | String abbreviation of the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `interception_player_id` | character | Unique identifier for the player that intercepted the pass. |
| `interception_player_name` | character | String name for the player that intercepted the pass. |
| `sack_player_id` | character | Unique identifier of the player who recorded a solo sack. |
| `sack_player_name` | character | String name of the player who recorded a solo sack. |
| `safety_player_id` | character | Unique identifier for the player who scored a safety. |
| `safety_player_name` | character | String name for the player who scored a safety. |
| `blocked_player_id` | character | Unique identifier for the player that blocked the punt or FG. |
| `blocked_player_name` | character | String name for the player that blocked the punt or FG. |
| `penalty_player_id` | character | Unique identifier for the player with the penalty. |
| `penalty_player_name` | character | String name for the player with the penalty. |
| `solo_tackle_1_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_1_player_name` | character | String name of one of the players with a solo tackle. |
| `solo_tackle_1_team` | character | Team of one of the players with a solo tackle. |
| `solo_tackle_2_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_2_player_name` | character | String name of one of the players with a solo tackle. |
| `solo_tackle_2_team` | character | Team of one of the players with a solo tackle. |
| `assist_tackle_1_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_1_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_1_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_2_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_2_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_2_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_3_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_3_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_3_team` | character | Team of one of the players with a tackle assist. |
| `assist_tackle_4_player_id` | character | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_4_player_name` | character | String name of one of the players with a tackle assist. |
| `assist_tackle_4_team` | character | Team of one of the players with a tackle assist. |
| `tackle_with_assist_1_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_1_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_1_team` | character | Team of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_2_team` | character | Team of one of the players with a tackle with assist. |
| `tackle_for_loss_1_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_1_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `half_sack_1_player_id` | character | Unique identifier of the first player who recorded half a sack. |
| `half_sack_1_player_name` | character | String name of the first player who recorded half a sack. |
| `half_sack_2_player_id` | character | Unique identifier of the second player who recorded half a sack. |
| `half_sack_2_player_name` | character | String name of the second player who recorded half a sack. |
| `qb_hit_1_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_1_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `pass_defense_1_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_1_player_name` | character | String name of one of the players with a pass defense. |
| `pass_defense_2_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_2_player_name` | character | String name of one of the players with a pass defense. |
| `forced_fumble_player_1_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_name` | character | String name of one of the players with a forced fumble. |
| `forced_fumble_player_1_team` | character | Team of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_name` | character | String name of one of the players with a forced fumble. |
| `forced_fumble_player_2_team` | character | Team of one of the players with a forced fumble. |
| `fumbled_1_player_id` | character | Unique identifier of the first player who fumbled on the play. |
| `fumbled_1_player_name` | character | String name of one of the first player who fumbled on the play. |
| `fumbled_1_team` | character | Team of one of the first player with a fumble. |
| `fumbled_2_player_id` | character | Unique identifier of the second player who fumbled on the play. |
| `fumbled_2_player_name` | character | String name of one of the second player who fumbled on the play. |
| `fumbled_2_team` | character | Team of one of the second player with a fumble. |
| `fumble_recovery_1_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_1_player_name` | character | String name of one of the players with a fumble recovery. |
| `fumble_recovery_1_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_1_yards` | double | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_2_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_2_player_name` | character | String name of one of the players with a fumble recovery. |
| `fumble_recovery_2_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_2_yards` | character | Yards gained by one of the players with a fumble recovery. |
| `two_point_conv_result` | character | String indicator for result of two point conversion attempt: success, failure, safety (touchback in defensive endzone is 1 point apparently), or return. |
| `extra_point_result` | character | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `special` | integer | Binary indicator if "play_type" is one of "extra_point", "field_goal", "kickoff", or "punt". |
| `pass_length` | character | String indicator for pass length: short or deep. |
| `pass_location` | character | String indicator for pass location: left, middle, or right. |
| `qb_kneel` | integer | Binary indicator for whether or not the QB took a knee. |
| `qb_spike` | integer | Binary indicator for whether or not the QB spiked the ball. |
| `qb_scramble` | integer | Binary indicator for whether or not the QB scrambled. |
| `shotgun` | integer | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | integer | Binary indicator for whether or not the play was in no_huddle formation. |
| `run_location` | character | String indicator for location of run: left, middle, or right. |
| `run_gap` | character | String indicator for line gap of run: end, guard, or tackle |
| `pass` | integer | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `rush` | integer | Binary indicator if the play was a rushing play. |
| `qb_dropback` | integer | Binary indicator for whether or not the QB dropped back on the play (pass attempt, sack, or scrambled). |
| `posteam_score` | integer | Score the posteam at the start of the play. |
| `defteam_score` | integer | Score the defteam at the start of the play. |
| `score_differential` | integer | Score differential between the posteam and defteam at the start of the play. |
| `posteam_timeouts_remaining` | integer | Number of timeouts remaining for the possession team. |
| `defteam_timeouts_remaining` | integer | Number of timeouts remaining for the team on defense. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `field_goal_result` | character | String indicator for result of field goal attempt: made, missed, or blocked. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `fixed_drive` | integer | Manually created drive number in a game. |
| `fixed_drive_result` | character | Manually created drive result. |
| `drive_play_count` | integer | Numeric value of how many regular plays happened in a given drive. |
| `drive_first_downs` | integer | Number of first downs in a given drive. |
| `drive_inside20` | integer | Binary indicator if the offense was able to get inside the opponents 20 yard line. |
| `drive_ended_with_score` | integer | Binary indicator the drive ended with a score. |
| `drive_quarter_start` | integer | Numeric value indicating in which quarter the given drive has started. |
| `drive_quarter_end` | integer | Numeric value indicating in which quarter the given drive has ended. |
| `drive_yards_penalized` | integer | Numeric value of how many yards the offense gained or lost through penalties in the given drive. |
| `drive_start_transition` | character | String indicating how the offense got the ball. |
| `drive_end_transition` | character | String indicating how the offense lost the ball. |
| `drive_game_clock_start` | character | Game time at the beginning of a given drive. |
| `drive_game_clock_end` | character | Game time at the end of a given drive. |
| `drive_start_yard_line` | double | String indicating where a given drive started consisting of team half and yard line number. |
| `drive_end_yard_line` | double | String indicating where a given drive ended consisting of team half and yard line number. |
| `drive_play_id_started` | integer | Play_id of the first play in the given drive. |
| `drive_play_id_ended` | integer | Play_id of the last play in the given drive. |
| `drive_time_of_possession` | character | Time of possession in a given drive. |
| `series` | integer | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_result` | character | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `series_success` | integer | 1: scored touchdown, gained enough yards for first down. |
| `live_phase` | character |  |
| `is_play` | integer |  |
| `provisional` | integer |  |
| `ep` | double | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |
| `td_prob` | double | Predicted probability of the posteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `opp_td_prob` | double | Predicted probability of the defteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `fg_prob` | double | Predicted probability of the posteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `opp_fg_prob` | double | Predicted probability of the defteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `safety_prob` | double | Predicted probability of the posteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `opp_safety_prob` | double | Predicted probability of the defteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `no_score_prob` | double | Predicted probability of no score occurring for the rest of the half based on the expected points model. |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `total_home_epa` | double | Cumulative total EPA for the home team in the game so far. |
| `total_away_epa` | double | Cumulative total EPA for the away team in the game so far. |
| `total_home_rush_epa` | double | Cumulative total rushing EPA for the home team in the game so far. |
| `total_away_rush_epa` | double | Cumulative total rushing EPA for the away team in the game so far. |
| `total_home_pass_epa` | double | Cumulative total passing EPA for the home team in the game so far. |
| `total_away_pass_epa` | double | Cumulative total passing EPA for the away team in the game so far. |
| `qb_epa` | double | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `air_epa` | double | EPA from the air yards alone. For completions this represents the actual value provided through the air. For incompletions this represents the hypothetical value that could've been added through the air if the pass was completed. |
| `yac_epa` | double | EPA from the yards after catch alone. For completions this represents the actual value provided after the catch. For incompletions this represents the difference between the hypothetical air_epa and the play's raw observed EPA (how much the incomplete pass cost the posteam). |
| `comp_air_epa` | double | EPA from the air yards alone only for completions. |
| `comp_yac_epa` | double | EPA from the yards after catch alone only for completions. |
| `total_home_comp_air_epa` | double | Cumulative total completions air EPA for the home team in the game so far. |
| `total_away_comp_air_epa` | double | Cumulative total completions air EPA for the away team in the game so far. |
| `total_home_comp_yac_epa` | double | Cumulative total completions yac EPA for the home team in the game so far. |
| `total_away_comp_yac_epa` | double | Cumulative total completions yac EPA for the away team in the game so far. |
| `total_home_raw_air_epa` | double | Cumulative total raw air EPA for the home team in the game so far. |
| `total_away_raw_air_epa` | double | Cumulative total raw air EPA for the away team in the game so far. |
| `total_home_raw_yac_epa` | double | Cumulative total raw yac EPA for the home team in the game so far. |
| `total_away_raw_yac_epa` | double | Cumulative total raw yac EPA for the away team in the game so far. |
| `receive_2h_ko` | integer |  |
| `posteam_spread` | double |  |
| `elapsed_share` | double |  |
| `spread_time` | double |  |
| `Diff_Time_Ratio` | double |  |
| `wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `vegas_wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `home_wp` | double | Estimated win probability for the home team. |
| `away_wp` | double | Estimated win probability for the away team. |
| `def_wp` | double | Estimated win probability for the defteam. |
| `vegas_home_wpa` | double | Win probability added (WPA) for the home team: spread_adjusted model. |
| `vegas_wpa` | double | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `wpa` | double | Win probability added (WPA) for the posteam. |
| `total_home_rush_wpa` | double | Cumulative total rushing WPA for the home team in the game so far. |
| `total_away_rush_wpa` | double | Cumulative total rushing WPA for the away team in the game so far. |
| `total_home_pass_wpa` | double | Cumulative total passing WPA for the home team in the game so far. |
| `total_away_pass_wpa` | double | Cumulative total passing WPA for the away team in the game so far. |
| `air_wpa` | double | WPA through the air (same logic as air_epa). |
| `yac_wpa` | double | WPA from yards after the catch (same logic as yac_epa). |
| `comp_air_wpa` | double | The air_wpa for completions only. |
| `comp_yac_wpa` | double | The yac_wpa for completions only. |
| `total_home_comp_air_wpa` | double | Cumulative total completions air WPA for the home team in the game so far. |
| `total_away_comp_air_wpa` | double | Cumulative total completions air WPA for the away team in the game so far. |
| `total_home_comp_yac_wpa` | double | Cumulative total completions yac WPA for the home team in the game so far. |
| `total_away_comp_yac_wpa` | double | Cumulative total completions yac WPA for the away team in the game so far. |
| `total_home_raw_air_wpa` | double | Cumulative total raw air WPA for the home team in the game so far. |
| `total_away_raw_air_wpa` | double | Cumulative total raw air WPA for the away team in the game so far. |
| `total_home_raw_yac_wpa` | double | Cumulative total raw yac WPA for the home team in the game so far. |
| `total_away_raw_yac_wpa` | double | Cumulative total raw yac WPA for the away team in the game so far. |
| `cp` | double | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | double | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `xpass` | double | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | double | Dropback percent over expected on a given play scaled from 0 to 100. |
| `xyac_epa` | double | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | double | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | double | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | double | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | double | Probability play earns a first down based on where the ball was caught. |
| `qbr_epa` | double |  |
| `weight` | double | Official weight, in pounds |
| `non_fumble_sack` | logical |  |
| `sack_epa` | double |  |
| `pass_epa` | double |  |
| `rush_epa` | double |  |
| `pen_epa` | double |  |
| `sack_weight` | double |  |
| `pass_weight` | double |  |
| `rush_weight` | double |  |
| `pen_weight` | double |  |
| `action_play` | character |  |
| `home_opening_kickoff` | double | 1 if the home team received the opening kickoff, 0 otherwise. |
| `go_wp` | double |  |
| `first_down_prob` | double |  |
| `wp_succeed` | double |  |
| `wp_fail` | double |  |
| `fg_make_prob` | double |  |
| `make_fg_wp` | double |  |
| `miss_fg_wp` | double |  |
| `fg_wp` | double |  |
| `punt_wp` | double |  |
| `go_boost` | double |  |
| `go_wp_diff` | double |  |
| `punt_wp_diff` | double |  |
| `fg_wp_diff` | double |  |
| `fourth_down_recommendation` | character |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_4th_down_probs

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_4th_down_probs(fourth)
print(out[["go_wp", "punt_wp", "fg_wp", "go_boost", "fourth_down_recommendation"]].head())
```
