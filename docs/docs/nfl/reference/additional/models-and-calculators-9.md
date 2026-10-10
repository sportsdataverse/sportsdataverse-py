---
title: "NFL — additional Python functions — Models and calculators: get_fg–nfl_ratings"
sidebar_label: "Models and calculators: get_fg–nfl_ratings"
sidebar_position: 22
description: "NFL — additional Python functions — Models and calculators: get_fg–nfl_ratings — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Models and calculators: get_fg–nfl_ratings

### get_fg_wp {#get_fg_wp}

`get_fg_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Expected win probability of attempting a field goal (nfl4th `get_fg_wp`).

The make probability comes from the self-trained `fg_model` (a
`binary:logistic` XGBoost re-train of the original mgcv GAM, features
`[yardline_100, fg_roof, era0..era4]`), shrunk by 0.9 for kicks at/beyond
`yardline_100 = 38` and zeroed at/beyond `yardline_100 = 45`
(>= ~63-yard kicks).  The made-FG state (opponent receives a touchback
kickoff at the 25, kicking team +3) and the missed-FG state (opponent takes
over 8 yards back of the spot, capped at the 80) are each scored with win
probability; `fg_wp = make_prob * make_wp + (1 - make_prob) * miss_wp`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `fg_make_prob`, `make_fg_wp`, `miss_fg_wp` and `fg_wp` (from the kicking team's perspective). All four are NaN when the FG model or WP model is unavailable.

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
| `wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `def_wp` | double | Estimated win probability for the defteam. |
| `home_wp` | double | Estimated win probability for the home team. |
| `away_wp` | double | Estimated win probability for the away team. |
| `wpa` | double | Win probability added (WPA) for the posteam. |
| `vegas_wpa` | double | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `vegas_home_wpa` | double | Win probability added (WPA) for the home team: spread_adjusted model. |
| `home_wp_post` | double | Estimated win probability for the home team at the end of the play. |
| `away_wp_post` | double | Estimated win probability for the away team at the end of the play. |
| `vegas_wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
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
| `temp` | double | The temperature at the stadium only for 'roof' = 'outdoors' or 'open'.(Source: Pro-Football-Reference) |
| `wind` | double | The speed of the wind in miles/hour only for 'roof' = 'outdoors' or 'open'. (Source: Pro-Football-Reference) |
| `home_coach` | character | First and last name of the home team coach. (Source: Pro-Football-Reference) |
| `away_coach` | character | First and last name of the away team coach. (Source: Pro-Football-Reference) |
| `stadium_id` | character | ID of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `game_stadium` | character | Name of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `aborted_play` | double | Binary indicator if the play description indicates "Aborted". |
| `success` | double | Binary indicator whether epa > 0 in the given play. |
| `passer` | character | Name of the dropback player (scrambles included) including plays with penalties. |
| `passer_jersey_number` | double | Jersey number of the passer. |
| `rusher` | character | Name of the rusher (no scrambles) including plays with penalties. |
| `rusher_jersey_number` | double | Jersey number of the rusher. |
| `receiver` | character | Name of the receiver including plays with penalties. |
| `receiver_jersey_number` | double | Jersey number of the receiver. |
| `pass` | double | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `rush` | double | Binary indicator if the play was a rushing play. |
| `first_down` | double | Binary indicator if the play ended in a first down. |
| `special` | double | Binary indicator if "play_type" is one of "extra_point", "field_goal", "kickoff", or "punt". |
| `play` | double | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `passer_id` | character | ID of the player in the 'passer' column. |
| `rusher_id` | character | ID of the player in the 'rusher' column. |
| `receiver_id` | character | ID of the player in the 'receiver' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `jersey_number` | double | Jersey number. Often useful for joins by name/team/jersey. |
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
| `xyac_median_yardage` | double | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | double | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | double | Probability play earns a first down based on where the ball was caught. |
| `xpass` | double | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | double | Dropback percent over expected on a given play scaled from 0 to 100. |
| `fg_make_prob` | double |  |
| `make_fg_wp` | double |  |
| `miss_fg_wp` | double |  |
| `fg_wp` | double |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_fg_wp

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_fg_wp(fourth)
print(out[["fg_make_prob", "fg_wp"]].head())
```

### get_go_wp {#get_go_wp}

`get_go_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Expected win probability of going for it on 4th down (nfl4th `get_go_wp`).

The fd_model 76-class yards-gained distribution is expanded per play; each
outcome's hypothetical post-play game state (turnover-on-downs flip, +6
touchdown with the PAT/2-pt branch routed through `get_2pt_wp`, 6-second
runoff, goal-to-go distance shrink) is scored with win probability and the
end-of-game kneel-out clamps are applied; the option value is the
prob-weighted WP.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the prepared state columns (see module docstring). The frame is prepared internally if it lacks the derived columns. |

**Returns**

A pandas copy of `pbp_df` plus `go_wp` (prob-weighted WP of going for it), `first_down_prob` (P(conversion)), `wp_succeed` (mean WP over conversion outcomes) and `wp_fail` (mean WP over failure outcomes). All are NaN when the fourth-down / WP models are unavailable (`FD_MODEL_AVAILABLE` / `WP_MODEL_AVAILABLE`).

No returns table is published for this function: no capture: it skips its own input preparation when posteam_spread is present, so it raises KeyError on enriched play-by-play, and a full unenriched season does not fit in memory.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_go_wp

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_go_wp(fourth)
print(out[["go_wp", "first_down_prob"]].head())
```

### get_punt_wp {#get_punt_wp}

`get_punt_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Expected win probability of punting on 4th down (nfl4th `get_punt_wp`).

The punt landing distribution (`punt_data`: `yardline_after` / `pct` /
`muff` per `yardline_100`) is joined per play; possession is flipped to
the receiving team, with return-touchdown (`yardline_after == 100`) and muff
(`muff == 1`) recoveries flipping the ball back to the punting team; each
landing spot's ensuing-drive WP is scored and the option value is the
prob-weighted WP from the punting team's perspective.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `punt_wp`. `punt_wp` is NaN where the punt distribution has no support for the play's `yardline_100` (inside the punting team's own 31, where the table is empty — matching the R reference's left-join NA behavior) or when the WP model is unavailable.

No returns table is published for this function: no capture: it skips its own input preparation when posteam_spread is present, so it raises KeyError on enriched play-by-play, and a full unenriched season does not fit in memory.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_fourth_down import get_punt_wp

pbp = load_nfl_pbp([2023])
fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
out = get_punt_wp(fourth)
print(out[["punt_wp"]].head())
```

### load_nfl_fp_curve {#load_nfl_fp_curve}

`load_nfl_fp_curve() -> 'pl.DataFrame'`

Load the bundled NFL EP-by-yardline curve (no network).

**Returns**

`yardline_own: Int64 (1..99), ep: Float64`.

| col_name | type | description |
|---|---|---|
| `yardline_own` | integer | Starting yard line from the offense's own goal (1-99); one row per yard line of the bundled NFL EP-by-starting-yardline curve. |
| `ep` | double | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_fp_curve
curve = load_nfl_fp_curve()
curve.filter(curve["yardline_own"] == 30)
```

### nfl_compute_results {#nfl_compute_results}

`nfl_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'Union[str, int]', *, rng: 'Optional[np.random.Generator]' = None, elo: 'Optional[Mapping[str, float]]' = None, **kwargs: 'Any') -> 'Dict[str, pl.DataFrame]'`

Compute NFL game results for one week of a season simulation.

Faithful port of `nflseedR_compute_results` (simulations_utils.R
L183-290) — the 538-style dynamic ELO model initially coded by Lee
Sharpe and rewritten by Sebastian Carl: home/away ELO difference plus
rest (+25 per extra week), home field (+20), and a 1.2x postseason
multiplier produce a win probability and a point spread `estimate`
(`elo_diff / 25`); missing results for `week_num` are drawn from
`Normal(estimate, 13)` and rounded away from zero. ELO ratings are
updated from all of the week's results and carried to the next week
via the returned `teams` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `DataFrame` |  | Teams frame with `sim` and `team` columns. An `elo` column is added on first call (from `elo` or random `Normal(1500, 150)` initial ratings shared across sims) and must be carried between calls. |
| `games` | `DataFrame` |  | Games frame with `sim`, `week`, `game_type`, `location`, `home_team`/`away_team`, `home_rest`/ `away_rest`, and `result` columns. |
| `week_num` | `Union[str, int]` |  | The week to simulate. Only rows with `week == week_num` and a missing `result` are filled. |
| `rng` | `Optional[Generator]` | `None` | numpy random generator; a fresh one is created when `None`. |
| `elo` | `Optional[Mapping[str, float]]` | `None` | Optional mapping of team abbreviation to initial ELO rating. |

**Returns**

`{"teams": teams, "games": games}` with updated ELO ratings and filled results.

| col_name | type | description |
|---|---|---|
| `teams.sim` | integer | Simulated season identifier the team row belongs to, carried through from the input teams frame. |
| `teams.team` | character | Team abbreviation, carried through from the input teams frame. |
| `teams.conf` | character | Conference of the team (AFC or NFC), carried through from the input teams frame. |
| `teams.division` | character | Division of the team (e.g. "AFC East"), carried through from the input teams frame. |
| `teams.elo` | double | Dynamic ELO rating after applying the shifts from the simulated week's results; carried into the next week's call so ratings evolve over the simulated season. |
| `games.sim` | integer | Simulated season identifier the game row belongs to. |
| `games.game_type` | character | Game type of the row - REG for regular season or the playoff round (WC, DIV, CON, SB). |
| `games.week` | character | Week key used by the simulation engine - regular season week numbers as strings and postseason rounds as WC/DIV/CON/SB. |
| `games.away_team` | character | Team abbreviation of the away team. |
| `games.home_team` | character | Team abbreviation of the home team. |
| `games.away_rest` | integer | Days of rest for the away team before the game (feeds the ELO rest adjustment of 25 points per extra week). |
| `games.home_rest` | integer | Days of rest for the home team before the game. |
| `games.location` | character | Game site indicator - "Home" applies the +20 ELO home-field adjustment, "Neutral" (Super Bowl) does not. |
| `games.result` | integer | Home margin (home score minus away score). Rows of the simulated week that were missing are filled from Normal(estimate, 13) rounded away from zero; all other rows pass through unchanged. |

**Example**

```python
from sportsdataverse.nfl.nfl_simulations import nfl_compute_results
out = nfl_compute_results(teams, games, week_num="5")
teams, games = out["teams"], out["games"]
```

### nfl_draft_projection {#nfl_draft_projection}

`nfl_draft_projection(seasons: 'List[int]', target_class: 'int', *, lam: 'float' = 100.0, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Draft outcome projection for one draft class.

Trains the closed-form ridge (expected `car_av`) and the IRLS logistic
(`hit_prob` = P(`seasons_started >= 3`)) on **matured** classes
(`season <= target_class - 5`) and scores the `target_class`
prospects. Features: standardized combine measurables (+ imputation
flags), draft `round`/`pick`/`log(pick)`, position one-hots.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Draft classes to load (training classes beyond the maturity boundary are filtered out automatically). |
| `target_class` | `int` |  | The draft class to score. |
| `lam` | `float` | `100.0` | Ridge regularization strength. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

One row per `target_class` prospect: `gsis_id:Utf8, target_class:Int64, position:Utf8, pred_car_av:Float64, hit_prob:Float64, outcome_rank:Int64` (dense rank, best first). Empty training or prediction slice returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `gsis_id` | character | nflverse gsis player id of the drafted prospect (character join key). |
| `target_class` | integer | The draft class scored (training uses matured classes <= target_class - 5). |
| `position` | character | Draft position group of the prospect. |
| `pred_car_av` | double | Predicted career value - closed-form ridge on standardized combine measurables + round/pick/log(pick) + position one-hots; the label is nflverse w_av (PFR weighted career Approximate Value). |
| `hit_prob` | double | P(multi-year starter) - ridge-regularized IRLS logistic on the same features, hit := seasons_started >= 3. |
| `outcome_rank` | integer | Dense rank of pred_car_av within the class (best prospect = 1). |

**Example**

```python
from sportsdataverse.nfl.nfl_draft_model import nfl_draft_projection
proj = nfl_draft_projection(list(range(2000, 2020)), 2019)
proj.sort("outcome_rank").head()
```

### nfl_fantasy_projection {#nfl_fantasy_projection}

`nfl_fantasy_projection(seasons: 'List[int]', target_season: 'int', *, scoring: 'Union[Dict[str, float], str]' = 'ppr', calibrate: 'bool' = True, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Fantasy-points projection: deterministic scoring of the Marcel component

stats plus a fitted per-position linear calibration.

Scores `nfl_player_projection`'s projected component *counting* stats
(rate x projected games) under the scoring format, then applies the fitted
`fp_calibration` `(a, b)` from `POSITION_CONSTANTS`
(`calibrated = a + b * raw`). The FantasyPros consensus is used only as a
concurrent-validity oracle in the tests — never as an input.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load. |
| `target_season` | `int` |  | The season being projected. |
| `scoring` | `Union[Dict[str, float], str]` | `'ppr'` | `"ppr"` / `"half"` / `"standard"` or a custom points-per-unit dict. |
| `calibrate` | `bool` | `True` | Apply the fitted per-position calibration. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

`player_id:Utf8, target_season:Int64, position_group:Utf8, proj_fantasy_points:Float64, proj_fantasy_points_per_game:Float64, position_rank:Int64`.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only). |
| `position_group` | character | nflverse offensive position group (QB/RB/WR/TE plus fringe groups). |
| `proj_fantasy_points` | double | Projected season fantasy points - the Marcel component rates x projected games scored under the scoring format, with the fitted per-position linear calibration applied by default. |
| `proj_fantasy_points_per_game` | double | Projected fantasy points per game (proj_fantasy_points / projected games). |
| `position_rank` | integer | Dense rank of proj_fantasy_points within the position group (best = 1). |

**Example**

```python
from sportsdataverse.nfl.nfl_projection import nfl_fantasy_projection
fp = nfl_fantasy_projection([2021, 2022, 2023], 2024)
fp.filter(pl.col("position_group") == "WR").head()

# Custom scoring

fp_std = nfl_fantasy_projection([2021, 2022, 2023], 2024, scoring="standard")
```

### nfl_kicker_rating {#nfl_kicker_rating}

`nfl_kicker_rating(seasons: 'Union[int, List[int]]', *, as_of: 'Optional[Tuple[int, int]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Environment-adjusted kicker FG-over-expected ratings.

Loads pbp FG attempts for `seasons`, computes the environment-adjusted
expected make probability per kick, and aggregates to per
`(season, kicker)` FGOE (raw + EB-shrunk with the fitted `K_fg`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons. |
| `as_of` | `Optional[Tuple[int, int]]` | `None` | Optional `(season, week)`; uses only kicks strictly before that point (the as-of leakage boundary for mid-season ratings). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, kicker_player_id)`: `kicker`, `team`, `fg_att`, `fg_made`, `exp_made`, `fgoe`, `fgoe_per_att`, `fgoe_shrunk`, `rating` (100 +/- 15 z of `fgoe_shrunk`). Empty seasons yield a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the rating. |
| `kicker_player_id` | character | nflverse kicker GSIS id (Utf8 join key). |
| `kicker` | character | Display name of the kicker (e.g. J.Tucker), from kicker_player_name. |
| `team` | character | Team of the kicker's most recent attempt in the window. |
| `fg_att` | integer | Field-goal attempts. |
| `fg_made` | integer | Field goals made. |
| `exp_made` | double | Sum of environment-adjusted make probabilities (expected makes). |
| `fgoe` | double | Field goals made over expected (fg_made - exp_made). |
| `fgoe_per_att` | double | FGOE per attempt. |
| `fgoe_shrunk` | double | Empirical-Bayes shrunk FGOE per attempt, fgoe_per_att * att / (att + K_fg). |
| `rating` | double | 100 +/- 15 z-score of fgoe_shrunk within the frame. |

**Example**

```python
from sportsdataverse.nfl.nfl_kicker_rating import nfl_kicker_rating
r = nfl_kicker_rating([2023])
print(r.head())

# Mid-season as-of rating

r = nfl_kicker_rating([2023], as_of=(2023, 10))
```

### nfl_line_grades {#nfl_line_grades}

`nfl_line_grades(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Team-season OL pass-block + DL pass-rush grades (opponent-adjusted, EB-shrunk).

Loads pbp, builds the matchup pressure grid, opponent-adjusts it, grades
both units on a 0-100 board (`50 + 15*z*n/(n+K_pressure)`), and joins
PFR's independent team pressure measurement
(`load_nfl_pfr_advstats(stat_type="def", summary_level="season")`,
`prss` summed to team / pbp dropbacks faced) as `pfr_pressure_pct`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons (PFR advstats coverage is 2018+). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, team)`: raw + adjusted pressure rates and dropback counts, `ol_pass_block_grade`, `dl_pass_rush_grade`, `pfr_pressure_pct`. Empty seasons yield a zero-row frame.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the grade. |
| `team` | character | Team abbreviation. |
| `dropbacks_off` | integer | Offensive dropbacks (qb_dropback plays). |
| `pressures_allowed` | integer | Sacks plus QB hits allowed on the team's own dropbacks. |
| `pressure_rate_allowed` | double | pressures_allowed / dropbacks_off (raw). |
| `dropbacks_def` | integer | Opponent dropbacks faced on defense. |
| `pressures_generated` | integer | Sacks plus QB hits generated against opponent dropbacks. |
| `pressure_rate_generated` | double | pressures_generated / dropbacks_def (raw). |
| `adj_pressure_rate_allowed` | double | Opponent-adjusted allowed pressure rate (additive fixed point, league-mean-centered). |
| `adj_pressure_rate_generated` | double | Opponent-adjusted generated pressure rate (additive fixed point, league-mean-centered). |
| `ol_pass_block_grade` | double | OL pass-block grade, 50 + 15 * z * n/(n + K_pressure) on the inverted adjusted allowed rate. |
| `dl_pass_rush_grade` | double | DL pass-rush grade, 50 + 15 * z * n/(n + K_pressure) on the adjusted generated rate. |
| `pfr_pressure_pct` | double | PFR team pressures (prss summed, traded 2TM/3TM rows excluded) divided by pbp dropbacks faced. |

**Example**

```python
from sportsdataverse.nfl.nfl_line_grades import nfl_line_grades
g = nfl_line_grades([2023])
print(g.sort("dl_pass_rush_grade", descending=True).head())
```

### nfl_player_projection {#nfl_player_projection}

`nfl_player_projection(seasons: 'List[int]', target_season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Marcel-style next-season player projection with delta-method aging.

Loads weekly player stats + rosters, aggregates to season rates, and for
every player visible in seasons **strictly before** `target_season`
(the as-of-date leakage boundary) produces a recency-weighted rate blend
regressed toward the volume-weighted position mean by
`k / (k + reliability)`, scaled by the position aging-curve ratio
`aging_mult(proj_age) / aging_mult(current_age)`. The aging curve is fit
only on the same pre-target history.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load (seasons `>= target_season` are discarded by the leakage split). |
| `target_season` | `int` |  | The season being projected. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

One row per projected player: `player_id:Utf8, target_season:Int64, position_group:Utf8, proj_age:Float64, proj_ppg:Float64, proj_volume:Float64, proj_games:Float64, aging_mult:Float64, reliability:Float64` plus `proj_<stat>_rate` component-rate columns. Empty history returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only - the as-of-date leakage boundary). |
| `position_group` | character | nflverse offensive position group (QB/RB/WR/TE plus fringe groups). |
| `proj_age` | double | Projected age at the target season (age at last visible season + season gap). |
| `proj_ppg` | double | Projected PPR fantasy points per game - recency-weighted rate blend regressed toward the volume-weighted position mean by k/(k + reliability), scaled by the damped aging-curve ratio. |
| `proj_volume` | double | Projected position-specific opportunity volume (QB = pass attempts, RB = carries + targets, WR/TE = targets). |
| `proj_games` | double | Recency-weighted mean of historical games played. |
| `aging_mult` | double | Applied aging multiplier - the damped, clamped ratio aging_curve(proj_age) / aging_curve(current_age). |
| `reliability` | double | Recency-weighted volume sum - the shrinkage evidence weight. |
| `proj_completions_rate` | double | Projected per-game pass completions (Marcel blend x aging ratio). |
| `proj_attempts_rate` | double | Projected per-game pass attempts (Marcel blend x aging ratio). |
| `proj_passing_yards_rate` | double | Projected per-game passing yards (Marcel blend x aging ratio). |
| `proj_passing_tds_rate` | double | Projected per-game passing touchdowns (Marcel blend x aging ratio). |
| `proj_interceptions_rate` | double | Projected per-game interceptions thrown (Marcel blend x aging ratio). |
| `proj_carries_rate` | double | Projected per-game rush attempts (Marcel blend x aging ratio). |
| `proj_rushing_yards_rate` | double | Projected per-game rushing yards (Marcel blend x aging ratio). |
| `proj_rushing_tds_rate` | double | Projected per-game rushing touchdowns (Marcel blend x aging ratio). |
| `proj_receptions_rate` | double | Projected per-game receptions (Marcel blend x aging ratio). |
| `proj_targets_rate` | double | Projected per-game targets (Marcel blend x aging ratio). |
| `proj_receiving_yards_rate` | double | Projected per-game receiving yards (Marcel blend x aging ratio). |
| `proj_receiving_tds_rate` | double | Projected per-game receiving touchdowns (Marcel blend x aging ratio). |
| `proj_receiving_air_yards_rate` | double | Projected per-game receiving air yards (Marcel blend x aging ratio). |
| `proj_fumbles_lost_rate` | double | Projected per-game fumbles lost (Marcel blend x aging ratio). |

**Example**

```python
from sportsdataverse.nfl.nfl_projection import nfl_player_projection
proj = nfl_player_projection([2021, 2022, 2023], 2024)
proj.sort("proj_ppg", descending=True).head()

# Pandas round-trip

proj_pd = nfl_player_projection([2021, 2022, 2023], 2024, return_as_pandas=True)
```

### nfl_ratings {#nfl_ratings}

`nfl_ratings(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, config: 'RatingsConfig | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

One row per team: the native NFL ratings spine (off/def/ST EPA).

Public orchestrator over `efficiency_ratings` +
`special_teams_ratings`. Loads play-by-play + schedule via
`load_nfl_pbp` / `load_nfl_schedule`, joins each game's `gameday`
onto the plays, optionally applies the as-of-date leakage boundary
(only plays from games with `gameday < as_of_date` are used), then
fits both components and reshapes into one wide per-team table with
dense ranks and a net z-score.

The loaded pbp is down-selected to the ridge columns *before* any fit so
no market column (`spread_line` / `vegas_wp`) can leak into the
ratings (the binding non-market boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (e.g. `2023`) or a list of seasons pooled into one combined fit. |
| `as_of_date` | `date \| None` | `None` | When given, only plays from games strictly before this date are used (mirrors what was knowable heading into that date). `None` (default) uses the full season(s). |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs forwarded to both component fits; defaults to `RatingsConfig`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

A DataFrame with one row per `team_id`: `season` (Int64 -- the single passed season, `null` for a pooled multi-season call), `team_id` (Utf8), `adj_off_epa` / `adj_def_epa` / `adj_st_epa` / `adj_net` (Float64; `adj_net` is offense minus defense -- special teams stays a separate column), `games` (Int64), `off_rank` / `def_rank` / `net_rank` (Int64; `def_rank` ascends -- fewer EPA allowed ranks better), `net_z` (Float64). Zero-row, correctly-typed when the seasons have no data or `as_of_date` filters out every play.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the ratings cover (null for a pooled multi-season fit). |
| `team_id` | character | nflverse team abbreviation (character join key, e.g. "KC"). |
| `adj_off_epa` | double | Opponent-adjusted offensive EPA per play (higher is better); competitive-play ridge fit. |
| `adj_def_epa` | double | Opponent-adjusted defensive EPA allowed per play (lower is better); competitive-play ridge fit. |
| `adj_st_epa` | double | Opponent-adjusted special-teams EPA per play (ridge on special==1 plays; 0.0 for teams with no special-teams plays in the window). |
| `adj_net` | double | Opponent-adjusted net efficiency (adj_off_epa minus adj_def_epa; special teams not folded in). |
| `games` | integer | Number of games the team played in the fitted window. |
| `off_rank` | integer | Dense rank on adj_off_epa descending (best offense = 1). |
| `def_rank` | integer | Dense rank on adj_def_epa ascending (fewer EPA allowed ranks better). |
| `net_rank` | integer | Dense rank on adj_net descending (best net rating = 1). |
| `net_z` | double | Z-score of adj_net across the 32 teams. |

**Example**

```python
from sportsdataverse.nfl import nfl_ratings
ratings = nfl_ratings(2023)
ratings.sort("net_rank").head()

# As-of-date leakage boundary

import datetime as dt
week6 = nfl_ratings(2023, as_of_date=dt.date(2023, 10, 12))
```
