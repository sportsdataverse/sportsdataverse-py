---
title: "NFL — additional Python functions — nflverse data releases: pbp–rosters"
sidebar_label: "nflverse data releases: pbp–rosters"
sidebar_position: 8
description: "NFL — additional Python functions — nflverse data releases: pbp–rosters — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — nflverse data releases: pbp–rosters

### load_pbp {#load_pbp}

`load_pbp(seasons: 'List[int]', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load NFL play by play data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which enriched play-by-play release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse/nflfastR `play_by_play_{season}.parquet` releases — full history from 1999, unchanged behavior. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_model_pbp` release: a Python-built, nflfastR-faithful enriched frame (ep/epa, wp/wpa/vegas_wp, cp/cpoe, xyac_*/air_epa) that covers 1999+ (28 assets, verified 2026-09-17) and drops administrative / timeout rows for a clean modeling subset. Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing the play-by-plays available for the requested seasons. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

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

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
pbp = load_nfl_pbp(seasons=[2024])
print(pbp.shape)

# Multi-season range

pbp = load_nfl_pbp(seasons=range(2020, 2025))

# SDV-native enriched play-by-play (Python-built, nflfastR-faithful EP/WP/CP/xYAC; published seasons only; admin/timeout rows dropped)

pbp_sdv = load_nfl_pbp(seasons=[2024], source="sdv")
pbp_sdv.select(["ep", "epa", "wp", "wpa", "cp", "cpoe", "xyac_epa"]).head()

# With cache off (development workflow)

from sportsdataverse.nfl import load_nfl_pbp, update_config
update_config(cache_mode="off")
pbp = load_nfl_pbp(seasons=[2024])

# Pandas round-trip

pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

### load_pfr_advstats {#load_pfr_advstats}

`load_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level
PFR advstats accessors. Mirrors the API surface of nflreadpy's
`load_pfr_advstats` so downstream code can swap engines without
changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For `summary_level='week'` this drives the per-season parquet fan-out; for `summary_level='season'` it post-filters the combined parquet by the `season` column. `None` loads the current season, `True` every published season, and `[]` an empty frame (no request). |
| `stat_type` | `str` | `'pass'` | One of `"pass"`, `"rush"`, `"rec"`, `"def"`. Defaults to `"pass"`. |
| `summary_level` | `str` | `'week'` | One of `"week"` or `"season"`. Defaults to `"week"`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested `stat_type`, `summary_level`, and `seasons`. `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Ten digit identifier for NFL game. |
| `pfr_game_id` | character | PFR game ID |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `opponent` | character | Opposing team of player |
| `pfr_player_name` | character | Player's name as recorded by PFR |
| `pfr_player_id` | character | ID from Pro Football Reference |
| `passing_drops` | double | Raw count of catchable passes dropped by the intended receiver, from Pro Football Reference charting. |
| `passing_drop_pct` | double | Percentage of pass attempts dropped by the receiver, isolating receiver-side incompletions from passer error. |
| `receiving_drop` | double | Number of catchable targets dropped by the receiver in the given game or season, from Pro Football Reference advanced receiving data. |
| `receiving_drop_pct` | double | Percentage of targets that resulted in a drop by the receiver, from Pro Football Reference advanced receiving data. |
| `passing_bad_throws` | double | Raw count of bad throws by the passer, as charted and defined by Pro Football Reference advanced passing data. |
| `passing_bad_throw_pct` | double | Percentage of pass attempts classified as bad throws by Pro Football Reference (passes the passer should not have attempted or severely underthreww/overthrew). |
| `times_sacked` | double | Total number of times the defensive player recorded a sack of the quarterback, from Pro Football Reference. |
| `times_blitzed` | double | Number of times blitzed |
| `times_hurried` | double | Number of times hurried |
| `times_hit` | double | Number of times hit |
| `times_pressured` | double | Number of times pressured |
| `times_pressured_pct` | double | Percentage of pass-blocking snaps on which the lineman or back allowed the quarterback to be pressured, from Pro Football Reference. |
| `def_times_blitzed` | double | Number of plays on which the defensive player sent five or more pass rushers, from Pro Football Reference advanced defensive stats. |
| `def_times_hurried` | double | Number of times the defensive player hurried (pressured but did not sack or hit) the quarterback, from Pro Football Reference. |
| `def_times_hitqb` | double | Number of times the defensive player hit the quarterback on a pass play without recording a sack, from Pro Football Reference. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
pass_week = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)

# Season-level rushing summaries (one row per player per season)

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

# Defensive stats with a follow-up filter

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

# Pandas round-trip

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### load_player_stats {#load_player_stats}

`load_player_stats(seasons: 'List[int] | None' = None, kicking=False, return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load NFL player stats data

Week-level player stats. For the default `source="nflverse"` this reads the
live `stats_player` release (`stats_player_week_{season}.parquet`, one
asset per season, 1999-2026) -- **not** the combined `player_stats.parquet`,
which nflverse froze in 2025-05 and which therefore ends at season 2024.

The weekly release is a 150-column superset of the old combined file. To keep
every downstream consumer working, the output is reconciled to ONE stable
schema -- the legacy column set, in the legacy order, at the legacy dtypes:

* **Renamed back:** `team` -> `recent_team`, `passing_interceptions` ->
  `interceptions`, `sacks_suffered` -> `sacks`.
* **Sign-flipped:** `sack_yards_lost` (negative upstream) is negated into
  `sack_yards` (positive yards lost), matching the legacy frame.
* **Kept null:** `dakota` is no longer published upstream; the column
  remains, all-null, so the column set does not move.
* **Dropped:** the ~100 added columns (`def_*`, `pt_*`, punt/kickoff
  returns, yardage buckets, `game_id`, `passing_cpoe`, ...) are not
  emitted. `kicking=True` returns the legacy kicking contract, which the
  weekly release still carries in full (44/44 columns).
* **Rows:** a row is kept when at least one contracted stat is non-zero, so
  the weekly release's defensive / offensive-line rows -- which have no
  column to land in under this contract -- do not arrive as all-null noise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` | `None` | Seasons to load. 1999 is the earliest available season. `None` (the default) loads the current season, `True` every published season from 1999, and `[]` an empty frame (no request); nflreadpy's contract (`None` used to load every season from 1999). |
| `kicking` | `bool` | `False` | If True, load kicking stats. If False, load all other stats. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |
| `source` | `str` | `'nflverse'` | Which player-stats release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse published `stats_player` weekly release, reconciled to the legacy schema described above. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_player_stats` release built by `sportsdataverse.nfl.build_nfl_player_stats` from SDV-native play-by-play (1999-present, week-level, REG+POST) with its own columns, season-filtered but otherwise untouched. Any other value raises `ValueError`. |

**Returns**

Polars dataframe containing player stats. `seasons=[]` gives the contracted columns with no rows (nflverse source) or an empty frame with no columns (SDV source).

| col_name | type | description |
|---|---|---|
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | character | Full name of player |
| `player_display_name` | character | Full name of the player |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `recent_team` | character | Most recent team player appears in `pbp` with. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character | Abbreviation of the opposing team the player faced in the game or week represented by this row. |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `interceptions` | double | The number of interceptions thrown. |
| `sacks` | double | The Number of times sacked. |
| `sack_yards` | double | Yards lost on sack plays. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | double | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | double | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | double | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `pacr` | double | Passing (yards) Air (yards) Conversion Ratio - the number of passing yards per air yards thrown per game |
| `dakota` | double | Adjusted EPA + CPOE composite based on coefficients which best predict adjusted EPA/play in the following year. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | double | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | double | The number of rushes with a lost fumble. |
| `rushing_first_downs` | double | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | double | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | double | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | double | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | double | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | double | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `racr` | double | Receiving (yards) Air (yards) Conversion Ratio - the number of receiving yards per air yards targeted per game |
| `target_share` | double | "Player's share of team receiving targets in this game" |
| `air_yards_share` | double | Player's share of the team's air yards in this game |
| `wopr` | double | Weighted OPportunity Rating - 1.5 x target_share + 0.7 x air_yards_share - a weighted average that contextualizes total fantasy usage. |
| `special_teams_tds` | double | Total number of kick/punt return touchdowns |
| `fantasy_points` | double | Standard fantasy points. |
| `fantasy_points_ppr` | double | PPR fantasy points. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

# SDV-native player stats (week-level, built from SDV play-by-play)

stats_sdv = load_nfl_player_stats(source="sdv")
stats_sdv.select(["season", "week", "player_id", "attempts"]).head()

# Kicking-only stats (nflverse source only)

kicking = load_nfl_player_stats(seasons=[2025], kicking=True)

# A single season (2025 and 2026 live only in the weekly release)

stats_2025 = load_nfl_player_stats(seasons=[2025])
```

### load_players {#load_players}

`load_players(return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load the nflverse NFL player-identity master.

Reads nflverse's published `players.parquet` — a one-row-per-player
identity master that is the union of **seven** upstream systems (GSIS, ESPN,
NGS roster, Pro-Football-Reference, OverTheCap, PFF, and the Sleeper / Yahoo
cross-walk). It is the canonical source for cross-system identifier
columns (`gsis_id`, `espn_id`, `pfr_id`, `pff_id`, `otc_id`,
`smart_id`, `esb_id`, `nfl_id`) plus name, position, physical, draft,
and status fields.

This is the **full identity master**. For an SDV-native, public-source-only
alternative that does not depend on the nflverse release, see
`sportsdataverse.nfl.build_nfl_players` (ESPN-athletes tier only) and
`sportsdataverse.nfl.nfl_players_crosswalk` (a thin ID-only slice of
this same parquet).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |
| `source` | `str` | `'nflverse'` | Which player-master release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse seven-system `players.parquet` identity master described above. `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_players` release built by `sportsdataverse.nfl.build_nfl_players` from the **public NFL Shield / ESPN-athletes** surface, with `gsis_id` and the other cross-system IDs enriched by a best-effort join against the nflverse player master. The SDV tier is a partial build: its columns are a subset of nflverse's and cross-system IDs are sparser (notably pre-2016), though `espn_id` is populated. The default stays `"nflverse"`. Any other value raises `ValueError`. |

**Returns**

One-row-per-player identity master. `return_as_pandas` narrows the return to a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `gsis_id` | character | NFL Game Statistics & Information System player identifier, the canonical nflverse player key. |
| `display_name` | character | Player's full display name as published by nflverse. |
| `common_first_name` | character | Player's commonly used first name (the name they go by, which may differ from their legal first name). |
| `first_name` | character | Player's legal first name. |
| `last_name` | character | Player's last name. |
| `short_name` | character | Abbreviated name (typically first initial plus last name). |
| `football_name` | character | Player's preferred on-field name as used in broadcast and box-score contexts. |
| `suffix` | character | Generational or honorific name suffix (e.g., Jr., Sr., III), when present. |
| `esb_id` | character | Elias Sports Bureau player identifier. |
| `nfl_id` | character | NFL.com / Shield player identifier. |
| `pfr_id` | character | Pro-Football-Reference player identifier. |
| `pff_id` | character | Pro Football Focus player identifier. |
| `otc_id` | character | OverTheCap player identifier (salary-cap data source). |
| `espn_id` | character | ESPN athlete identifier. |
| `smart_id` | character | NFL SMART (Standard Media and Reference Table) globally unique player identifier. |
| `birth_date` | character | Player's date of birth (ISO YYYY-MM-DD). |
| `position_group` | character | Broad positional grouping the player belongs to (e.g., QB, RB, WR, DL). |
| `position` | character | Player's specific listed position abbreviation. |
| `ngs_position_group` | character | Positional grouping as classified by NFL Next Gen Stats. |
| `ngs_position` | character | Specific position as classified by NFL Next Gen Stats. |
| `height` | integer | Player's height in inches. |
| `weight` | integer | Player's listed weight in pounds. |
| `headshot` | character | URL to the player's official headshot image. |
| `college_name` | character | Name of the college the player attended. |
| `college_conference` | character | Athletic conference of the player's college. |
| `jersey_number` | character | Player's uniform / jersey number. |
| `rookie_season` | integer | Season (year) the player entered the league as a rookie. |
| `last_season` | integer | Most recent season (year) the player appeared on an NFL roster. |
| `latest_team` | character | Abbreviation of the most recent team the player was rostered on. |
| `status` | character | Player's current roster status (e.g., active, retired, free agent). |
| `ngs_status` | character | Player status as reported by NFL Next Gen Stats. |
| `ngs_status_short_description` | character | Short human-readable description of the NFL Next Gen Stats status. |
| `years_of_experience` | integer | Number of accrued NFL seasons of experience. |
| `pff_position` | character | Player's position as classified by Pro Football Focus. |
| `pff_status` | character | Player's status as classified by Pro Football Focus. |
| `draft_year` | integer | Year the player was selected in the NFL Draft (null if undrafted). |
| `draft_round` | integer | Round in which the player was drafted (null if undrafted). |
| `draft_pick` | integer | Overall pick number at which the player was drafted (null if undrafted). |
| `draft_team` | character | Abbreviation of the team that drafted the player (null if undrafted). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
print(players.shape)

# Pandas round-trip

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()

# SDV-native player master (public Shield/ESPN-athletes build; subset of nflverse columns, sparser cross-IDs)

players_sdv = load_nfl_players(source="sdv")
players_sdv.select(["display_name", "position", "espn_id"]).head()

# Pipeline next step (one line)

import polars as pl
load_nfl_players().select(["gsis_id", "display_name", "position"]).head()
```

### load_rosters {#load_rosters}

`load_rosters(seasons: 'List[int]', return_as_pandas=False, *, source: 'str' = 'nflverse') -> 'pl.DataFrame'`

Load NFL season roster data for the requested seasons.

Reads nflverse's published season-roster parquet (one row per player per
season). nflverse's roster product is the union of three upstream tiers --
NFL Next Gen Stats (2016+), the credentialed NFL Data Exchange (2002-2015),
and the public NFL Shield endpoint (all seasons) -- so it carries densely
populated cross-system identifier columns (`espn_id`, `sportradar_id`,
`yahoo_id`, `pff_id`, `pfr_id`, ...) alongside biographical and
depth-chart fields. This is the richest roster surface; prefer it whenever a
network round trip to nflverse is acceptable.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Seasons to load (e.g. `[2024]` or `range(2020, 2025)`). A single `int` is accepted and wrapped. 1920 is the earliest available season. `None` loads the current roster year (it turns on March 15), `True` every published season, and `[]` an empty frame (no request). |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe (default). |
| `source` | `str` | `'nflverse'` | Which roster release to read. `"nflverse"` (the default, also accepts `None`) returns the nflverse season-roster releases described above -- the full multi-source product (1920+, densely populated cross-system IDs). `"sportsdataverse"` / `"sdv"` returns the SDV-native `nfl_rosters` release built by `sportsdataverse.nfl.build_nfl_rosters` from the **public NFL Shield / ESPN** surface, with cross-system IDs and `college` enriched by a best-effort join against the nflverse player master (`sportsdataverse.nfl.load_nfl_players`, on `gsis_id`; skipped if that load fails). The SDV tier is a partial build: its 30 columns are a subset of nflverse's 36, and cross-system IDs are sparser pre-2016. It covers only the published seasons (rosters 2022+). The default stays `"nflverse"`. Any other value raises `ValueError`. |

**Returns**

Polars dataframe of season rosters for the requested seasons (`pandas.DataFrame` when `return_as_pandas=True`). `seasons=[]` gives an empty frame with no columns; schema'd empty frames come with the dtype contract.

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `depth_chart_position` | character | Position assigned on depth chart. Not always accurate! |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `status` | character | Roster status code nflverse assigns the player for that season, e.g. ACT (active), RES (reserve) or INA (inactive). |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `birth_date` | character | Player birth date (sourced from NFL. Other sources may differ) |
| `height` | double | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `espn_id` | character | ESPN ID - usual format is an integer with ~5 digits |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `rotowire_id` | character | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `fantasy_data_id` | character | FantasyData ID - usual format five digit integer |
| `sleeper_id` | character | Sleeper ID - usually an integer with ~4 digits. |
| `years_exp` | integer | Years played in league |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `week` | integer | Season week. |
| `game_type` | character | The most recent game type of that season that a player appeared on the roster. |
| `status_description_abbr` | character | A code corresponding to a particular NFL status. |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `esb_id` | character | Player ID for Elias Sports Bureau |
| `gsis_it_id` | character | Player ID for the GSIS IT API |
| `smart_id` | character | SMART ID for player (that's in raw pbp. It includes a hashed ESB_ID) |
| `entry_year` | integer | The year a player first became eligible to play in the NFL. |
| `rookie_year` | integer | The year a player lost their rookie eligibility. |
| `draft_club` | character | The team that originally drafted a player. NA if a player went undrafted in their draft-eligible year. |
| `draft_number` | integer | The number pick that was used to select a given player. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_rosters
rosters = load_nfl_rosters(seasons=[2024])

# Multi-season range

rosters = load_nfl_rosters(seasons=range(2020, 2025))

# Filter to a single team

import polars as pl
kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")

# SDV-native rosters (public Shield/ESPN build; published seasons 2022+; 30-column subset of nflverse, sparser cross-IDs pre-2016)

rosters_sdv = load_nfl_rosters(seasons=[2023], source="sdv")
rosters_sdv.select(["season", "team", "full_name", "gsis_id"]).head()
```
