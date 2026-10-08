---
title: "NFL — additional Python functions — Models and calculators: calculate_win"
sidebar_label: "Models and calculators: calculate_win"
sidebar_position: 17
description: "NFL — additional Python functions — Models and calculators: calculate_win — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Models and calculators: calculate_win

### calculate_win_probability {#calculate_win_probability}

`calculate_win_probability(pbp_data: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Compute win probability for provided plays.

Mirrors nflfastR's `calculate_win_probability()`.  Uses the
spread-adjusted model (`wp_spread.ubj`) when `spread_line` is
non-null, and falls back to the naive model (`wp_naive.ubj`) for plays
with a missing spread line.  Drops and recomputes any existing `wp` /
`vegas_wp` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | Play-by-play DataFrame. Required: all EP columns plus `score_differential`, `game_seconds_remaining`, `spread_line`, `receive_2h_ko`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus: `wp` (naive WP) and `vegas_wp` (spread-adjusted WP).

| col_name | type | description |
|---|---|---|
| `play_id` | double | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `game_id` | character | Ten digit identifier for NFL game. |
| `old_game_id` | character | Legacy NFL game ID. |
| `home_team` | character | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `away_team` | character | String abbreviation for the away team. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `week` | integer | Season week. |
| `posteam` | character | String abbreviation for the team with possession. |
| `posteam_type` | character | String indicating whether the posteam team is home or away. |
| `defteam` | character | String abbreviation for the team on defense. |
| `side_of_field` | character | String abbreviation for which team's side of the field the team with possession is currently on. |
| `yardline_100` | double | Numeric distance in the number of yards from the opponent's endzone for the posteam. |
| `game_date` | character | Date of the game. |
| `quarter_seconds_remaining` | double | Numeric seconds remaining in the quarter. |
| `half_seconds_remaining` | double | Numeric seconds remaining in the half. |
| `game_seconds_remaining` | double | Numeric seconds remaining in the game. |
| `game_half` | character | String indicating which half the play is in, either Half1, Half2, or Overtime. |
| `quarter_end` | double | Binary indicator for whether or not the row of the data is marking the end of a quarter. |
| `drive` | double | Numeric drive number in the game. |
| `sp` | double | Binary indicator for whether or not a score occurred on the play. |
| `qtr` | double | Quarter of the game (5 is overtime). |
| `down` | double | The down for the given play. |
| `goal_to_go` | double | Binary indicator for whether or not the posteam is in a goal down situation. |
| `time` | character | Time at start of play provided in string format as minutes:seconds remaining in the quarter. |
| `yrdln` | character | String indicating the current field position for a given play. |
| `ydstogo` | double | Numeric yards in distance from either the first down marker or the endzone in goal down situations. |
| `ydsnet` | double | Numeric value for total yards gained on the given drive. |
| `desc` | character | Detailed string description for the given play. |
| `play_type` | character | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `yards_gained` | double | Numeric yards gained (or lost) by the possessing team, excluding yards gained via fumble recoveries and laterals. |
| `shotgun` | double | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | double | Binary indicator for whether or not the play was in no_huddle formation. |
| `qb_dropback` | double | Binary indicator for whether or not the QB dropped back on the play (pass attempt, sack, or scrambled). |
| `qb_kneel` | double | Binary indicator for whether or not the QB took a knee. |
| `qb_spike` | double | Binary indicator for whether or not the QB spiked the ball. |
| `qb_scramble` | double | Binary indicator for whether or not the QB scrambled. |
| `pass_length` | character | String indicator for pass length: short or deep. |
| `pass_location` | character | String indicator for pass location: left, middle, or right. |
| `air_yards` | double | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `yards_after_catch` | double | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `run_location` | character | String indicator for location of run: left, middle, or right. |
| `run_gap` | character | String indicator for line gap of run: end, guard, or tackle |
| `field_goal_result` | character | String indicator for result of field goal attempt: made, missed, or blocked. |
| `kick_distance` | double | Numeric distance in yards for kickoffs, field goals, and punts. |
| `extra_point_result` | character | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | character | String indicator for result of two point conversion attempt: success, failure, safety (touchback in defensive endzone is 1 point apparently), or return. |
| `home_timeouts_remaining` | double | Numeric timeouts remaining in the half for the home team. |
| `away_timeouts_remaining` | double | Numeric timeouts remaining in the half for the away team. |
| `timeout` | double | Binary indicator for whether or not a timeout was called by either team. |
| `timeout_team` | character | String abbreviation for which team called the timeout. |
| `td_team` | character | String abbreviation for which team scored the touchdown. |
| `td_player_name` | character | String name of the player who scored a touchdown. |
| `td_player_id` | character | Unique identifier of the player who scored a touchdown. |
| `posteam_timeouts_remaining` | double | Number of timeouts remaining for the possession team. |
| `defteam_timeouts_remaining` | double | Number of timeouts remaining for the team on defense. |
| `total_home_score` | double | Score for the home team at the start of the play. |
| `total_away_score` | double | Score for the away team at the start of the play. |
| `posteam_score` | double | Score the posteam at the start of the play. |
| `defteam_score` | double | Score the defteam at the start of the play. |
| `score_differential` | double | Score differential between the posteam and defteam at the start of the play. |
| `posteam_score_post` | double | Score for the posteam at the end of the play. |
| `defteam_score_post` | double | Score for the defteam at the end of the play. |
| `score_differential_post` | double | Score differential between the posteam and defteam at the end of the play. |
| `no_score_prob` | double | Predicted probability of no score occurring for the rest of the half based on the expected points model. |
| `opp_fg_prob` | double | Predicted probability of the defteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `opp_safety_prob` | double | Predicted probability of the defteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `opp_td_prob` | double | Predicted probability of the defteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `fg_prob` | double | Predicted probability of the posteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `safety_prob` | double | Predicted probability of the posteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `td_prob` | double | Predicted probability of the posteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `extra_point_prob` | double | Predicted probability of the posteam scoring an extra point. |
| `two_point_conversion_prob` | double | Predicted probability of the posteam scoring the two point conversion. |
| `ep` | double | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |
| `epa` | double | Expected points added (EPA) by the posteam for the given play. |
| `total_home_epa` | double | Cumulative total EPA for the home team in the game so far. |
| `total_away_epa` | double | Cumulative total EPA for the away team in the game so far. |
| `total_home_rush_epa` | double | Cumulative total rushing EPA for the home team in the game so far. |
| `total_away_rush_epa` | double | Cumulative total rushing EPA for the away team in the game so far. |
| `total_home_pass_epa` | double | Cumulative total passing EPA for the home team in the game so far. |
| `total_away_pass_epa` | double | Cumulative total passing EPA for the away team in the game so far. |
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
| `def_wp` | double | Estimated win probability for the defteam. |
| `home_wp` | double | Estimated win probability for the home team. |
| `away_wp` | double | Estimated win probability for the away team. |
| `wpa` | double | Win probability added (WPA) for the posteam. |
| `vegas_wpa` | double | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `vegas_home_wpa` | double | Win probability added (WPA) for the home team: spread_adjusted model. |
| `home_wp_post` | double | Estimated win probability for the home team at the end of the play. |
| `away_wp_post` | double | Estimated win probability for the away team at the end of the play. |
| `vegas_home_wp` | double | Estimated win probability for the home team incorporating pre-game Vegas line. |
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
| `punt_blocked` | double | Binary indicator for if the punt was blocked. |
| `first_down_rush` | double | Binary indicator for if a running play converted the first down. |
| `first_down_pass` | double | Binary indicator for if a passing play converted the first down. |
| `first_down_penalty` | double | Binary indicator for if a penalty converted the first down. |
| `third_down_converted` | double | Binary indicator for if the first down was converted on third down. |
| `third_down_failed` | double | Binary indicator for if the posteam failed to convert first down on third down. |
| `fourth_down_converted` | double | Binary indicator for if the first down was converted on fourth down. |
| `fourth_down_failed` | double | Binary indicator for if the posteam failed to convert first down on fourth down. |
| `incomplete_pass` | double | Binary indicator for if the pass was incomplete. |
| `touchback` | double | Binary indicator for if a touchback occurred on the play. |
| `interception` | double | Binary indicator for if the pass was intercepted. |
| `punt_inside_twenty` | double | Binary indicator for if the punt ended inside the twenty yard line. |
| `punt_in_endzone` | double | Binary indicator for if the punt was in the endzone. |
| `punt_out_of_bounds` | double | Binary indicator for if the punt went out of bounds. |
| `punt_downed` | double | Binary indicator for if the punt was downed. |
| `punt_fair_catch` | double | Binary indicator for if the punt was caught with a fair catch. |
| `kickoff_inside_twenty` | double | Binary indicator for if the kickoff ended inside the twenty yard line. |
| `kickoff_in_endzone` | double | Binary indicator for if the kickoff was in the endzone. |
| `kickoff_out_of_bounds` | double | Binary indicator for if the kickoff went out of bounds. |
| `kickoff_downed` | double | Binary indicator for if the kickoff was downed. |
| `kickoff_fair_catch` | double | Binary indicator for if the kickoff was caught with a fair catch. |
| `fumble_forced` | double | Binary indicator for if the fumble was forced. |
| `fumble_not_forced` | double | Binary indicator for if the fumble was not forced. |
| `fumble_out_of_bounds` | double | Binary indicator for if the fumble went out of bounds. |
| `solo_tackle` | double | Binary indicator if the play had a solo tackle (could be multiple due to fumbles). |
| `safety` | double | Binary indicator for whether or not a safety occurred. |
| `penalty` | double | Binary indicator for whether or not a penalty occurred. |
| `tackled_for_loss` | double | Binary indicator for whether or not a tackle for loss on a run play occurred. |
| `fumble_lost` | double | Binary indicator for if the fumble was lost. |
| `own_kickoff_recovery` | double | Binary indicator for if the kicking team recovered the kickoff. |
| `own_kickoff_recovery_td` | double | Binary indicator for if the kicking team recovered the kickoff and scored a TD. |
| `qb_hit` | double | Binary indicator if the QB was hit on the play. |
| `rush_attempt` | double | Binary indicator for if the play was a run. |
| `pass_attempt` | double | Binary indicator for if the play was a pass attempt (includes sacks). |
| `sack` | double | Binary indicator for if the play ended in a sack. |
| `touchdown` | double | Binary indicator for if the play resulted in a TD. |
| `pass_touchdown` | double | Binary indicator for if the play resulted in a passing TD. |
| `rush_touchdown` | double | Binary indicator for if the play resulted in a rushing TD. |
| `return_touchdown` | double | Binary indicator for if the play resulted in a return TD. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `extra_point_attempt` | double | Binary indicator for extra point attempt. |
| `two_point_attempt` | double | Binary indicator for two point conversion attempt. |
| `field_goal_attempt` | double | Binary indicator for field goal attempt. |
| `kickoff_attempt` | double | Binary indicator for kickoff. |
| `punt_attempt` | double | Binary indicator for punts. |
| `fumble` | double | Binary indicator for if a fumble occurred. |
| `complete_pass` | double | Binary indicator for if the pass was completed. |
| `assist_tackle` | double | Binary indicator for if an assist tackle occurred. |
| `lateral_reception` | double | Binary indicator for if a lateral occurred on the reception. |
| `lateral_rush` | double | Binary indicator for if a lateral occurred on a run. |
| `lateral_return` | double | Binary indicator for if a lateral occurred on a return. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `lateral_recovery` | double | Binary indicator for if a lateral occurred on a fumble recovery. |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `passer_player_name` | character | String name for the player that attempted the pass. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `receiver_player_name` | character | String name for the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `rusher_player_name` | character | String name for the player that attempted the run. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_receiver_player_id` | character | Unique identifier for the player that received the last(!) lateral on a pass play. |
| `lateral_receiver_player_name` | character | String name for the player that received the last(!) lateral on a pass play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral receiving yards. |
| `lateral_receiving_yards` | double | Numeric yards by the `lateral_receiver_player_name` in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `lateral_rusher_player_id` | character | Unique identifier for the player that received the last(!) lateral on a run play. |
| `lateral_rusher_player_name` | character | String name for the player that received the last(!) lateral on a run play. If there were multiple laterals in the same play, this will only be the last player who received a lateral. Please see <https://github.com/mrcaseb/nfl-data/tree/master/data/lateral_yards> for a list of plays where multiple players recorded lateral rushing yards. |
| `lateral_rushing_yards` | double | Numeric yards by the `lateral_rusher_player_name` in run plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_sack_player_id` | character | Unique identifier for the player that received the lateral on a sack. |
| `lateral_sack_player_name` | character | String name for the player that received the lateral on a sack. |
| `interception_player_id` | character | Unique identifier for the player that intercepted the pass. |
| `interception_player_name` | character | String name for the player that intercepted the pass. |
| `lateral_interception_player_id` | character | Unique identifier for the player that received the lateral on an interception. |
| `lateral_interception_player_name` | character | String name for the player that received the lateral on an interception. |
| `punt_returner_player_id` | character | Unique identifier for the punt returner. |
| `punt_returner_player_name` | character | String name for the punt returner. |
| `lateral_punt_returner_player_id` | character | Unique identifier for the player that received the lateral on a punt return. |
| `lateral_punt_returner_player_name` | character | String name for the player that received the lateral on a punt return. |
| `kickoff_returner_player_name` | character | String name for the kickoff returner. |
| `kickoff_returner_player_id` | character | Unique identifier for the kickoff returner. |
| `lateral_kickoff_returner_player_id` | character | Unique identifier for the player that received the lateral on a kickoff return. |
| `lateral_kickoff_returner_player_name` | character | String name for the player that received the lateral on a kickoff return. |
| `punter_player_id` | character | Unique identifier for the punter. |
| `punter_player_name` | character | String name for the punter. |
| `kicker_player_name` | character | String name for the kicker on FG or kickoff. |
| `kicker_player_id` | character | Unique identifier for the kicker on FG or kickoff. |
| `own_kickoff_recovery_player_id` | character | Unique identifier for the player that recovered their own kickoff. |
| `own_kickoff_recovery_player_name` | character | String name for the player that recovered their own kickoff. |
| `blocked_player_id` | character | Unique identifier for the player that blocked the punt or FG. |
| `blocked_player_name` | character | String name for the player that blocked the punt or FG. |
| `tackle_for_loss_1_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_1_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_id` | character | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_name` | character | String name for one of the potential players with the tackle for loss. |
| `qb_hit_1_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_1_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_id` | character | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_name` | character | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `forced_fumble_player_1_team` | character | Team of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_name` | character | String name of one of the players with a forced fumble. |
| `forced_fumble_player_2_team` | character | Team of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_id` | character | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_name` | character | String name of one of the players with a forced fumble. |
| `solo_tackle_1_team` | character | Team of one of the players with a solo tackle. |
| `solo_tackle_2_team` | character | Team of one of the players with a solo tackle. |
| `solo_tackle_1_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_2_player_id` | character | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_1_player_name` | character | String name of one of the players with a solo tackle. |
| `solo_tackle_2_player_name` | character | String name of one of the players with a solo tackle. |
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
| `tackle_with_assist` | double | Binary indicator for if there has been a tackle with assist. |
| `tackle_with_assist_1_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_1_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_1_team` | character | Team of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_id` | character | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_name` | character | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_2_team` | character | Team of one of the players with a tackle with assist. |
| `pass_defense_1_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_1_player_name` | character | String name of one of the players with a pass defense. |
| `pass_defense_2_player_id` | character | Unique identifier of one of the players with a pass defense. |
| `pass_defense_2_player_name` | character | String name of one of the players with a pass defense. |
| `fumbled_1_team` | character | Team of one of the first player with a fumble. |
| `fumbled_1_player_id` | character | Unique identifier of the first player who fumbled on the play. |
| `fumbled_1_player_name` | character | String name of one of the first player who fumbled on the play. |
| `fumbled_2_player_id` | character | Unique identifier of the second player who fumbled on the play. |
| `fumbled_2_player_name` | character | String name of one of the second player who fumbled on the play. |
| `fumbled_2_team` | character | Team of one of the second player with a fumble. |
| `fumble_recovery_1_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_1_yards` | double | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_1_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_1_player_name` | character | String name of one of the players with a fumble recovery. |
| `fumble_recovery_2_team` | character | Team of one of the players with a fumble recovery. |
| `fumble_recovery_2_yards` | double | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_2_player_id` | character | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_2_player_name` | character | String name of one of the players with a fumble recovery. |
| `sack_player_id` | character | Unique identifier of the player who recorded a solo sack. |
| `sack_player_name` | character | String name of the player who recorded a solo sack. |
| `half_sack_1_player_id` | character | Unique identifier of the first player who recorded half a sack. |
| `half_sack_1_player_name` | character | String name of the first player who recorded half a sack. |
| `half_sack_2_player_id` | character | Unique identifier of the second player who recorded half a sack. |
| `half_sack_2_player_name` | character | String name of the second player who recorded half a sack. |
| `return_team` | character | String abbreviation of the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `return_yards` | double | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `penalty_team` | character | String abbreviation of the team with the penalty. |
| `penalty_player_id` | character | Unique identifier for the player with the penalty. |
| `penalty_player_name` | character | String name for the player with the penalty. |
| `penalty_yards` | double | Yards gained (or lost) by the posteam from the penalty. |
| `replay_or_challenge` | double | Binary indicator for whether or not a replay or challenge. |
| `replay_or_challenge_result` | character | String indicating the result of the replay or challenge. |
| `penalty_type` | character | String indicating the penalty type of the first penalty in the given play. Will be `NA` if `desc` is missing the type. |
| `defensive_two_point_attempt` | double | Binary indicator whether or not the defense was able to have an attempt on a two point conversion, this results following a turnover. |
| `defensive_two_point_conv` | double | Binary indicator whether or not the defense successfully scored on the two point conversion. |
| `defensive_extra_point_attempt` | double | Binary indicator whether or not the defense was able to have an attempt on an extra point attempt, this results following a blocked attempt that the defense recovers the ball. |
| `defensive_extra_point_conv` | double | Binary indicator whether or not the defense successfully scored on an extra point attempt. |
| `safety_player_name` | character | String name for the player who scored a safety. |
| `safety_player_id` | character | Unique identifier for the player who scored a safety. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `cp` | double | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | double | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `series` | double | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_success` | double | 1: scored touchdown, gained enough yards for first down. |
| `series_result` | character | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `order_sequence` | double | Column provided by NFL to fix out-of-order plays. Available 2011 and beyond with source "nfl". |
| `start_time` | character | Kickoff time in eastern time zone. |
| `time_of_day` | character | Time of day of play in UTC "HH:MM:SS" format. Available 2011 and beyond with source "nfl". |
| `stadium` | character | Name of the stadium |
| `weather` | character | String describing the weather including temperature, humidity and wind (direction and speed). Doesn't change during the game! |
| `nfl_api_id` | character | UUID of the game in the new NFL API. |
| `play_clock` | character | Time on the playclock when the ball was snapped. |
| `play_deleted` | double | Binary indicator for deleted plays. |
| `play_type_nfl` | character | Play type as listed in the NFL source. Slightly different to the regular play_type variable. |
| `special_teams_play` | double | Binary indicator for whether play is special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `st_play_type` | character | Type of special teams play from NFL source. Available 2011 and beyond with source "nfl". |
| `end_clock_time` | character | Game time at the end of a given play. |
| `end_yard_line` | character | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `fixed_drive` | double | Manually created drive number in a game. |
| `fixed_drive_result` | character | Manually created drive result. |
| `drive_real_start_time` | character | Local day time when the drive started (currently not used by the NFL and therefore mostly 'NA'). |
| `drive_play_count` | double | Numeric value of how many regular plays happened in a given drive. |
| `drive_time_of_possession` | character | Time of possession in a given drive. |
| `drive_first_downs` | double | Number of first downs in a given drive. |
| `drive_inside20` | double | Binary indicator if the offense was able to get inside the opponents 20 yard line. |
| `drive_ended_with_score` | double | Binary indicator the drive ended with a score. |
| `drive_quarter_start` | double | Numeric value indicating in which quarter the given drive has started. |
| `drive_quarter_end` | double | Numeric value indicating in which quarter the given drive has ended. |
| `drive_yards_penalized` | double | Numeric value of how many yards the offense gained or lost through penalties in the given drive. |
| `drive_start_transition` | character | String indicating how the offense got the ball. |
| `drive_end_transition` | character | String indicating how the offense lost the ball. |
| `drive_game_clock_start` | character | Game time at the beginning of a given drive. |
| `drive_game_clock_end` | character | Game time at the end of a given drive. |
| `drive_start_yard_line` | character | String indicating where a given drive started consisting of team half and yard line number. |
| `drive_end_yard_line` | character | String indicating where a given drive ended consisting of team half and yard line number. |
| `drive_play_id_started` | double | Play_id of the first play in the given drive. |
| `drive_play_id_ended` | double | Play_id of the last play in the given drive. |
| `away_score` | integer | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `home_score` | integer | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `result` | integer | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `total` | integer | The sum of each team's score in the game. Equals h_score + v_score. Is NA for games which haven't yet been played. Convenient for evaluating over/under total bets. |
| `spread_line` | double | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `total_line` | double | The closing total line for the game. (Source: Pro-Football-Reference) |
| `div_game` | integer | Binary indicator of whether or not game was played by 2 teams in the same division. |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `surface` | character | What type of ground the game was played on. (Source: Pro-Football-Reference) |
| `temp` | integer | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | integer | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `game_stadium` | character | Name of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `aborted_play` | double | Binary indicator if the play description indicates "Aborted". |
| `success` | double | Binary indicator whether epa > 0 in the given play. |
| `passer` | character | Name of the dropback player (scrambles included) including plays with penalties. |
| `passer_jersey_number` | integer | Jersey number of the passer. |
| `rusher` | character | Name of the rusher (no scrambles) including plays with penalties. |
| `rusher_jersey_number` | integer | Jersey number of the rusher. |
| `receiver` | character | Name of the receiver including plays with penalties. |
| `receiver_jersey_number` | integer | Jersey number of the receiver. |
| `pass` | double | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `rush` | double | Binary indicator if the play was a rushing play. |
| `first_down` | double | Binary indicator if the play ended in a first down. |
| `special` | double | Binary indicator if "play_type" is one of "extra_point", "field_goal", "kickoff", or "punt". |
| `play` | double | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `passer_id` | character | ID of the player in the 'passer' column. |
| `rusher_id` | character | ID of the player in the 'rusher' column. |
| `receiver_id` | character | ID of the player in the 'receiver' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `id` | character | ID of the player in the 'name' column. |
| `fantasy_player_name` | character | Name of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy_player_id` | character | ID of the rusher on rush plays or receiver on pass plays (from official stats). |
| `fantasy` | character | Name of the rusher on rush plays or receiver on pass plays. |
| `fantasy_id` | character | ID of the rusher on rush plays or receiver on pass plays. |
| `out_of_bounds` | double | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `home_opening_kickoff` | double | 1 if the home team received the opening kickoff, 0 otherwise. |
| `qb_epa` | double | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `xyac_epa` | double | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | double | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | integer | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | double | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | double | Probability play earns a first down based on where the ball was caught. |
| `xpass` | double | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | double | Dropback percent over expected on a given play scaled from 0 to 100. |
| `era0` | integer |  |
| `era1` | integer |  |
| `era2` | integer |  |
| `era3` | integer |  |
| `era4` | integer |  |
| `down1` | integer |  |
| `down2` | integer |  |
| `down3` | integer |  |
| `down4` | integer |  |
| `home` | integer |  |
| `retractable` | integer |  |
| `dome` | integer |  |
| `outdoors` | integer |  |
| `receive_2h_ko` | integer |  |
| `posteam_spread` | double |  |
| `elapsed_share` | double |  |
| `spread_time` | double |  |
| `Diff_Time_Ratio` | double |  |
| `wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `vegas_wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_win_probability

pbp = load_nfl_pbp([2023])
pbp_wp = calculate_win_probability(pbp)
print(pbp_wp.select("wp", "vegas_wp").head())
```
