---
title: "NFL — additional Python functions — Models and calculators: calculate_xyac–efficiency_ratings"
sidebar_label: "Models and calculators: calculate_xyac–efficiency_ratings"
sidebar_position: 21
description: "NFL — additional Python functions — Models and calculators: calculate_xyac–efficiency_ratings — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Models and calculators: calculate_xyac–efficiency_ratings

### calculate_xyac {#calculate_xyac}

`calculate_xyac(pbp_data: 'pl.DataFrame', *, models_dir: 'Optional[Union[str, Path]]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Compute expected yards after catch (xYAC) for intended pass plays.

Faithful polars port of nflfastR's `add_xyac`.  Unlike a per-statistic
regressor, xYAC is **one** `multi:softprob` model (`num_class=76`) that
predicts a distribution over YAC buckets (`yac = -5..70`); the five output
columns are *derived* from that distribution by re-scoring expected points on
every outcome.  `ep` is **not** required on the input — it is recomputed on
the outcome rows via `calculate_expected_points`.  The play's pre-snap
`ep` (`original_ep`) is the EPA baseline; `air_epa` is also part of the
baseline (`xyac_epa = Σ((ep − original_ep)·prob) − air_epa`).  `air_epa`
is **optional**: when present (the nflverse path) it is used verbatim so
parity is byte-for-byte preserved; when absent (the Shield-native / ESPN
path) it is computed from the already-scored `yac == 0` (catch-spot)
outcome — `air_epa = ep(yac == 0) − original_ep` — and, since it was
genuinely missing, surfaced as an extra `air_epa` output column.

Inference filter (nflfastR `valid_pass` ∧ `distance_to_goal != 0`):
`complete_pass == 1` OR `incomplete_pass == 1` OR `interception == 1`,
`air_yards` in `[-15, 70)`, non-null `receiver_player_name` and
`pass_location`, and `distance_to_goal != 0`.  Non-qualifying rows
receive null in all five columns.  Drops and recomputes any existing xYAC
output columns.

The xYAC model (`xyac_model.ubj`, ~34 MB) is **not** bundled in the
wheel: on first use it is downloaded from the `nfl_model_artifacts`
GitHub release and cached under `<cache_dir>/models/` (see
`sportsdataverse.nfl.get_config`).  Subsequent calls load it from the
cache; `clear_cache()` deliberately preserves the `models/` subdir so a
data-cache clear does not force a re-download.  Pass `models_dir=` to
point at a local directory containing `xyac_model.ubj` (offline / custom
model override).  If the model is genuinely unavailable (no cache + no
network) the underlying loader raises `FileNotFoundError`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_data` | `DataFrame` |  | nflverse-format play-by-play DataFrame. Required: `air_yards`, `season`, `half_seconds_remaining`, `yardline_100`, `ydstogo`, `down`, `posteam`, `home_team`, `roof`, `ep`, `posteam_timeouts_remaining`, `defteam_timeouts_remaining`, `complete_pass`, `incomplete_pass`, `interception`, `pass_location`, `receiver_player_name`. Optional: `air_epa` (used verbatim when present for byte-for-byte nflverse parity; computed from the `yac == 0` outcome and added as an output column when absent), `qb_hit`. |
| `models_dir` | `Optional[Union[str, Path]]` | `None` | Optional directory to load `xyac_model.ubj` from instead of downloading/caching it (offline use or a custom-trained model). When `None` (default) the model is resolved bundled → cache → downloaded-from-release. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

DataFrame with the original columns plus the five nflfastR xYAC columns (`Float64`, null on non-qualifying rows): `xyac_epa`, `xyac_mean_yardage`, `xyac_median_yardage`, `xyac_success`, `xyac_fd`. When the input lacked `air_epa` and at least one qualifying pass was scored, a computed `air_epa` column (catch-spot air EPA) is also added.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Ten digit identifier for NFL game. |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | Int64 | Season week. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `play_id` | Int64 | Numeric play id that when used with game_id and drive provides the unique identifier for a single play. |
| `play_seq` | Float64 |  |
| `posteam` | String | String abbreviation for the team with possession. |
| `defteam` | String | String abbreviation for the team on defense. |
| `home_team` | String | The home team. Note that this contains the designated home team for games which no team is playing at home such as Super Bowls or NFL International games. |
| `away_team` | String | String abbreviation for the away team. |
| `home` | Int64 |  |
| `qtr` | Int64 | Quarter of the game (5 is overtime). |
| `game_half` | String | String indicating which half the play is in, either Half1, Half2, or Overtime. |
| `down` | Int64 | The down for the given play. |
| `ydstogo` | Int64 | Numeric yards in distance from either the first down marker or the endzone in goal down situations. |
| `yardline_100` | Int64 | Numeric distance in the number of yards from the opponent's endzone for the posteam. |
| `goal_to_go` | Int64 | Binary indicator for whether or not the posteam is in a goal down situation. |
| `quarter_seconds_remaining` | Int64 | Numeric seconds remaining in the quarter. |
| `half_seconds_remaining` | Int64 | Numeric seconds remaining in the half. |
| `game_seconds_remaining` | Int64 | Numeric seconds remaining in the game. |
| `play_type` | String | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `yards_gained` | Int64 | Numeric yards gained (or lost) by the possessing team, excluding yards gained via fumble recoveries and laterals. |
| `desc` | String | Detailed string description for the given play. |
| `shield_play_type` | String |  |
| `special_teams_play_type` | String |  |
| `sp` | Int64 | Binary indicator for whether or not a score occurred on the play. |
| `pass_attempt` | Int64 | Binary indicator for if the play was a pass attempt (includes sacks). |
| `complete_pass` | Int64 | Binary indicator for if the pass was completed. |
| `incomplete_pass` | Int64 | Binary indicator for if the pass was incomplete. |
| `interception` | Int64 | Binary indicator for if the pass was intercepted. |
| `rush_attempt` | Int64 | Binary indicator for if the play was a run. |
| `sack` | Int64 | Binary indicator for if the play ended in a sack. |
| `touchdown` | Int64 | Binary indicator for if the play resulted in a TD. |
| `pass_touchdown` | Int64 | Binary indicator for if the play resulted in a passing TD. |
| `rush_touchdown` | Int64 | Binary indicator for if the play resulted in a rushing TD. |
| `return_touchdown` | Int64 | Binary indicator for if the play resulted in a return TD. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `field_goal_attempt` | Int64 | Binary indicator for field goal attempt. |
| `field_goal_made` | Int64 |  |
| `field_goal_missed` | Int64 |  |
| `field_goal_blocked` | Int64 |  |
| `extra_point_attempt` | Int64 | Binary indicator for extra point attempt. |
| `two_point_attempt` | Int64 | Binary indicator for two point conversion attempt. |
| `punt_attempt` | Int64 | Binary indicator for punts. |
| `kickoff_attempt` | Int64 | Binary indicator for kickoff. |
| `penalty` | Int64 | Binary indicator for whether or not a penalty occurred. |
| `fumble` | Int64 | Binary indicator for if a fumble occurred. |
| `fumble_lost` | Int64 | Binary indicator for if the fumble was lost. |
| `qb_hit` | Int64 | Binary indicator if the QB was hit on the play. |
| `safety` | Int64 | Binary indicator for whether or not a safety occurred. |
| `timeout` | Int64 | Binary indicator for whether or not a timeout was called by either team. |
| `first_down_rush` | Int64 | Binary indicator for if a running play converted the first down. |
| `first_down_pass` | Int64 | Binary indicator for if a passing play converted the first down. |
| `first_down_penalty` | Int64 | Binary indicator for if a penalty converted the first down. |
| `solo_tackle` | Int64 | Binary indicator if the play had a solo tackle (could be multiple due to fumbles). |
| `assist_tackle` | Int64 | Binary indicator for if an assist tackle occurred. |
| `tackle_with_assist` | Int64 | Binary indicator for if there has been a tackle with assist. |
| `tackled_for_loss` | Int64 | Binary indicator for whether or not a tackle for loss on a run play occurred. |
| `fumble_forced` | Int64 | Binary indicator for if the fumble was forced. |
| `fumble_not_forced` | Int64 | Binary indicator for if the fumble was not forced. |
| `fumble_out_of_bounds` | Int64 | Binary indicator for if the fumble went out of bounds. |
| `punt_fair_catch` | Int64 | Binary indicator for if the punt was caught with a fair catch. |
| `punt_downed` | Int64 | Binary indicator for if the punt was downed. |
| `punt_out_of_bounds` | Int64 | Binary indicator for if the punt went out of bounds. |
| `kickoff_fair_catch` | Int64 | Binary indicator for if the kickoff was caught with a fair catch. |
| `kickoff_out_of_bounds` | Int64 | Binary indicator for if the kickoff went out of bounds. |
| `extra_point_good` | Int64 |  |
| `extra_point_failed` | Int64 |  |
| `extra_point_blocked` | Int64 |  |
| `extra_point_safety` | Int64 |  |
| `extra_point_aborted` | Int64 |  |
| `two_point_rush_good` | Int64 |  |
| `two_point_rush_failed` | Int64 |  |
| `two_point_rush_safety` | Int64 |  |
| `two_point_pass_good` | Int64 |  |
| `two_point_pass_failed` | Int64 |  |
| `two_point_pass_safety` | Int64 |  |
| `two_point_pass_reception_good` | Int64 |  |
| `two_point_pass_reception_failed` | Int64 |  |
| `two_point_return` | Int64 |  |
| `def_tackles_for_loss` | Int64 | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | Int64 | Yards lost from TFLs involving this player |
| `td_ids_touchdown` | Int64 |  |
| `misc_yards` | Int64 |  |
| `fumble_recovery_own_lateral_yards` | Int64 |  |
| `fumble_recovery_opp_lateral_yards` | Int64 |  |
| `air_yards` | Int64 | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `yards_after_catch` | Int64 | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `passing_yards` | Int64 | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `rushing_yards` | Int64 | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `receiving_yards` | Int64 | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `penalty_yards` | Int64 | Yards gained (or lost) by the posteam from the penalty. |
| `kick_distance` | Int64 | Numeric distance in yards for kickoffs, field goals, and punts. |
| `return_yards` | Int64 | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `lateral_rushing_yards` | String | Numeric yards by the `lateral_rusher_player_name` in run plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `lateral_receiving_yards` | String | Numeric yards by the `lateral_receiver_player_name` in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `passer_player_id` | String | Unique identifier for the player that attempted the pass. |
| `passer_player_name` | String | String name for the player that attempted the pass. |
| `rusher_player_id` | String | Unique identifier for the player that attempted the run. |
| `rusher_player_name` | String | String name for the player that attempted the run. |
| `receiver_player_id` | String | Unique identifier for the receiver that was targeted on the pass. |
| `receiver_player_name` | String | String name for the targeted receiver. |
| `td_player_id` | String | Unique identifier of the player who scored a touchdown. |
| `td_player_name` | String | String name of the player who scored a touchdown. |
| `td_team` | String | String abbreviation for which team scored the touchdown. |
| `penalty_team` | String | String abbreviation of the team with the penalty. |
| `timeout_team` | String | String abbreviation for which team called the timeout. |
| `kicker_player_id` | String | Unique identifier for the kicker on FG or kickoff. |
| `kicker_player_name` | String | String name for the kicker on FG or kickoff. |
| `punter_player_id` | String | Unique identifier for the punter. |
| `punter_player_name` | String | String name for the punter. |
| `punt_returner_player_id` | String | Unique identifier for the punt returner. |
| `punt_returner_player_name` | String | String name for the punt returner. |
| `kickoff_returner_player_id` | String | Unique identifier for the kickoff returner. |
| `kickoff_returner_player_name` | String | String name for the kickoff returner. |
| `return_team` | String | String abbreviation of the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `interception_player_id` | String | Unique identifier for the player that intercepted the pass. |
| `interception_player_name` | String | String name for the player that intercepted the pass. |
| `sack_player_id` | String | Unique identifier of the player who recorded a solo sack. |
| `sack_player_name` | String | String name of the player who recorded a solo sack. |
| `safety_player_id` | String | Unique identifier for the player who scored a safety. |
| `safety_player_name` | String | String name for the player who scored a safety. |
| `blocked_player_id` | String | Unique identifier for the player that blocked the punt or FG. |
| `blocked_player_name` | String | String name for the player that blocked the punt or FG. |
| `penalty_player_id` | String | Unique identifier for the player with the penalty. |
| `penalty_player_name` | String | String name for the player with the penalty. |
| `solo_tackle_1_player_id` | String | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_1_player_name` | String | String name of one of the players with a solo tackle. |
| `solo_tackle_1_team` | String | Team of one of the players with a solo tackle. |
| `solo_tackle_2_player_id` | String | Unique identifier of one of the players with a solo tackle. |
| `solo_tackle_2_player_name` | String | String name of one of the players with a solo tackle. |
| `solo_tackle_2_team` | String | Team of one of the players with a solo tackle. |
| `assist_tackle_1_player_id` | String | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_1_player_name` | String | String name of one of the players with a tackle assist. |
| `assist_tackle_1_team` | String | Team of one of the players with a tackle assist. |
| `assist_tackle_2_player_id` | String | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_2_player_name` | String | String name of one of the players with a tackle assist. |
| `assist_tackle_2_team` | String | Team of one of the players with a tackle assist. |
| `assist_tackle_3_player_id` | String | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_3_player_name` | String | String name of one of the players with a tackle assist. |
| `assist_tackle_3_team` | String | Team of one of the players with a tackle assist. |
| `assist_tackle_4_player_id` | String | Unique identifier of one of the players with a tackle assist. |
| `assist_tackle_4_player_name` | String | String name of one of the players with a tackle assist. |
| `assist_tackle_4_team` | String | Team of one of the players with a tackle assist. |
| `tackle_with_assist_1_player_id` | String | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_1_player_name` | String | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_1_team` | String | Team of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_id` | String | Unique identifier of one of the players with a tackle with assist. |
| `tackle_with_assist_2_player_name` | String | String name of one of the players with a tackle with assist. |
| `tackle_with_assist_2_team` | String | Team of one of the players with a tackle with assist. |
| `tackle_for_loss_1_player_id` | String | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_1_player_name` | String | String name for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_id` | String | Unique identifier for one of the potential players with the tackle for loss. |
| `tackle_for_loss_2_player_name` | String | String name for one of the potential players with the tackle for loss. |
| `half_sack_1_player_id` | String | Unique identifier of the first player who recorded half a sack. |
| `half_sack_1_player_name` | String | String name of the first player who recorded half a sack. |
| `half_sack_2_player_id` | String | Unique identifier of the second player who recorded half a sack. |
| `half_sack_2_player_name` | String | String name of the second player who recorded half a sack. |
| `qb_hit_1_player_id` | String | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_1_player_name` | String | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_id` | String | Unique identifier for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `qb_hit_2_player_name` | String | String name for one of the potential players that hit the QB. No sack as the QB was not the ball carrier. For sacks please see `sack_player` or `half_sack_*_player`. |
| `pass_defense_1_player_id` | String | Unique identifier of one of the players with a pass defense. |
| `pass_defense_1_player_name` | String | String name of one of the players with a pass defense. |
| `pass_defense_2_player_id` | String | Unique identifier of one of the players with a pass defense. |
| `pass_defense_2_player_name` | String | String name of one of the players with a pass defense. |
| `forced_fumble_player_1_player_id` | String | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_name` | String | String name of one of the players with a forced fumble. |
| `forced_fumble_player_1_team` | String | Team of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_id` | String | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_2_player_name` | String | String name of one of the players with a forced fumble. |
| `forced_fumble_player_2_team` | String | Team of one of the players with a forced fumble. |
| `fumbled_1_player_id` | String | Unique identifier of the first player who fumbled on the play. |
| `fumbled_1_player_name` | String | String name of one of the first player who fumbled on the play. |
| `fumbled_1_team` | String | Team of one of the first player with a fumble. |
| `fumbled_2_player_id` | String | Unique identifier of the second player who fumbled on the play. |
| `fumbled_2_player_name` | String | String name of one of the second player who fumbled on the play. |
| `fumbled_2_team` | String | Team of one of the second player with a fumble. |
| `fumble_recovery_1_player_id` | String | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_1_player_name` | String | String name of one of the players with a fumble recovery. |
| `fumble_recovery_1_team` | String | Team of one of the players with a fumble recovery. |
| `fumble_recovery_1_yards` | Int64 | Yards gained by one of the players with a fumble recovery. |
| `fumble_recovery_2_player_id` | String | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_2_player_name` | String | String name of one of the players with a fumble recovery. |
| `fumble_recovery_2_team` | String | Team of one of the players with a fumble recovery. |
| `fumble_recovery_2_yards` | String | Yards gained by one of the players with a fumble recovery. |
| `two_point_conv_result` | String | String indicator for result of two point conversion attempt: success, failure, safety (touchback in defensive endzone is 1 point apparently), or return. |
| `extra_point_result` | String | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `special` | Int64 | Binary indicator if "play_type" is one of "extra_point", "field_goal", "kickoff", or "punt". |
| `pass_length` | String | String indicator for pass length: short or deep. |
| `pass_location` | String | String indicator for pass location: left, middle, or right. |
| `qb_kneel` | Int64 | Binary indicator for whether or not the QB took a knee. |
| `qb_spike` | Int64 | Binary indicator for whether or not the QB spiked the ball. |
| `qb_scramble` | Int64 | Binary indicator for whether or not the QB scrambled. |
| `shotgun` | Int64 | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | Int64 | Binary indicator for whether or not the play was in no_huddle formation. |
| `run_location` | String | String indicator for location of run: left, middle, or right. |
| `run_gap` | String | String indicator for line gap of run: end, guard, or tackle |
| `pass` | Int64 | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `rush` | Int64 | Binary indicator if the play was a rushing play. |
| `qb_dropback` | Int64 | Binary indicator for whether or not the QB dropped back on the play (pass attempt, sack, or scrambled). |
| `posteam_score` | Int64 | Score the posteam at the start of the play. |
| `defteam_score` | Int64 | Score the defteam at the start of the play. |
| `score_differential` | Int64 | Score differential between the posteam and defteam at the start of the play. |
| `posteam_timeouts_remaining` | Int64 | Number of timeouts remaining for the possession team. |
| `defteam_timeouts_remaining` | Int64 | Number of timeouts remaining for the team on defense. |
| `roof` | String | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `spread_line` | Float64 | The closing spread line for the game. A positive number means the home team was favored by that many points, a negative number means the away team was favored by that many points. (Source: Pro-Football-Reference) |
| `total_line` | Float64 | The closing total line for the game. (Source: Pro-Football-Reference) |
| `field_goal_result` | String | String indicator for result of field goal attempt: made, missed, or blocked. |
| `home_score` | Int64 | The number of points the home team scored. Is NA for games which haven't yet been played. |
| `away_score` | Int64 | The number of points the away team scored. Is NA for games which haven't yet been played. |
| `result` | Int64 | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `fixed_drive` | Int64 | Manually created drive number in a game. |
| `fixed_drive_result` | String | Manually created drive result. |
| `drive_play_count` | Int64 | Numeric value of how many regular plays happened in a given drive. |
| `drive_first_downs` | Int64 | Number of first downs in a given drive. |
| `drive_inside20` | Int64 | Binary indicator if the offense was able to get inside the opponents 20 yard line. |
| `drive_ended_with_score` | Int64 | Binary indicator the drive ended with a score. |
| `drive_quarter_start` | Int64 | Numeric value indicating in which quarter the given drive has started. |
| `drive_quarter_end` | Int64 | Numeric value indicating in which quarter the given drive has ended. |
| `drive_yards_penalized` | Int64 | Numeric value of how many yards the offense gained or lost through penalties in the given drive. |
| `drive_start_transition` | String | String indicating how the offense got the ball. |
| `drive_end_transition` | String | String indicating how the offense lost the ball. |
| `drive_game_clock_start` | String | Game time at the beginning of a given drive. |
| `drive_game_clock_end` | String | Game time at the end of a given drive. |
| `drive_start_yard_line` | Int64 | String indicating where a given drive started consisting of team half and yard line number. |
| `drive_end_yard_line` | Int64 | String indicating where a given drive ended consisting of team half and yard line number. |
| `drive_play_id_started` | Int64 | Play_id of the first play in the given drive. |
| `drive_play_id_ended` | Int64 | Play_id of the last play in the given drive. |
| `drive_time_of_possession` | String | Time of possession in a given drive. |
| `series` | Int64 | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_result` | String | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `series_success` | Int64 | 1: scored touchdown, gained enough yards for first down. |
| `live_phase` | String |  |
| `is_play` | Int64 |  |
| `provisional` | Int64 |  |
| `ep` | Float64 | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |
| `td_prob` | Float64 | Predicted probability of the posteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `opp_td_prob` | Float64 | Predicted probability of the defteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `fg_prob` | Float64 | Predicted probability of the posteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `opp_fg_prob` | Float64 | Predicted probability of the defteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `safety_prob` | Float64 | Predicted probability of the posteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `opp_safety_prob` | Float64 | Predicted probability of the defteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `no_score_prob` | Float64 | Predicted probability of no score occurring for the rest of the half based on the expected points model. |
| `epa` | Float64 | Expected points added (EPA) by the posteam for the given play. |
| `total_home_epa` | Float64 | Cumulative total EPA for the home team in the game so far. |
| `total_away_epa` | Float64 | Cumulative total EPA for the away team in the game so far. |
| `total_home_rush_epa` | Float64 | Cumulative total rushing EPA for the home team in the game so far. |
| `total_away_rush_epa` | Float64 | Cumulative total rushing EPA for the away team in the game so far. |
| `total_home_pass_epa` | Float64 | Cumulative total passing EPA for the home team in the game so far. |
| `total_away_pass_epa` | Float64 | Cumulative total passing EPA for the away team in the game so far. |
| `qb_epa` | Float64 | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `air_epa` | Float64 | EPA from the air yards alone. For completions this represents the actual value provided through the air. For incompletions this represents the hypothetical value that could've been added through the air if the pass was completed. |
| `yac_epa` | Float64 | EPA from the yards after catch alone. For completions this represents the actual value provided after the catch. For incompletions this represents the difference between the hypothetical air_epa and the play's raw observed EPA (how much the incomplete pass cost the posteam). |
| `comp_air_epa` | Float64 | EPA from the air yards alone only for completions. |
| `comp_yac_epa` | Float64 | EPA from the yards after catch alone only for completions. |
| `total_home_comp_air_epa` | Float64 | Cumulative total completions air EPA for the home team in the game so far. |
| `total_away_comp_air_epa` | Float64 | Cumulative total completions air EPA for the away team in the game so far. |
| `total_home_comp_yac_epa` | Float64 | Cumulative total completions yac EPA for the home team in the game so far. |
| `total_away_comp_yac_epa` | Float64 | Cumulative total completions yac EPA for the away team in the game so far. |
| `total_home_raw_air_epa` | Float64 | Cumulative total raw air EPA for the home team in the game so far. |
| `total_away_raw_air_epa` | Float64 | Cumulative total raw air EPA for the away team in the game so far. |
| `total_home_raw_yac_epa` | Float64 | Cumulative total raw yac EPA for the home team in the game so far. |
| `total_away_raw_yac_epa` | Float64 | Cumulative total raw yac EPA for the away team in the game so far. |
| `receive_2h_ko` | Int64 |  |
| `posteam_spread` | Float64 |  |
| `elapsed_share` | Float64 |  |
| `spread_time` | Float64 |  |
| `Diff_Time_Ratio` | Float64 |  |
| `wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `vegas_wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `home_wp` | Float64 | Estimated win probability for the home team. |
| `away_wp` | Float64 | Estimated win probability for the away team. |
| `def_wp` | Float64 | Estimated win probability for the defteam. |
| `vegas_home_wpa` | Float64 | Win probability added (WPA) for the home team: spread_adjusted model. |
| `vegas_wpa` | Float64 | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `wpa` | Float64 | Win probability added (WPA) for the posteam. |
| `total_home_rush_wpa` | Float64 | Cumulative total rushing WPA for the home team in the game so far. |
| `total_away_rush_wpa` | Float64 | Cumulative total rushing WPA for the away team in the game so far. |
| `total_home_pass_wpa` | Float64 | Cumulative total passing WPA for the home team in the game so far. |
| `total_away_pass_wpa` | Float64 | Cumulative total passing WPA for the away team in the game so far. |
| `air_wpa` | Float64 | WPA through the air (same logic as air_epa). |
| `yac_wpa` | Float64 | WPA from yards after the catch (same logic as yac_epa). |
| `comp_air_wpa` | Float64 | The air_wpa for completions only. |
| `comp_yac_wpa` | Float64 | The yac_wpa for completions only. |
| `total_home_comp_air_wpa` | Float64 | Cumulative total completions air WPA for the home team in the game so far. |
| `total_away_comp_air_wpa` | Float64 | Cumulative total completions air WPA for the away team in the game so far. |
| `total_home_comp_yac_wpa` | Float64 | Cumulative total completions yac WPA for the home team in the game so far. |
| `total_away_comp_yac_wpa` | Float64 | Cumulative total completions yac WPA for the away team in the game so far. |
| `total_home_raw_air_wpa` | Float64 | Cumulative total raw air WPA for the home team in the game so far. |
| `total_away_raw_air_wpa` | Float64 | Cumulative total raw air WPA for the away team in the game so far. |
| `total_home_raw_yac_wpa` | Float64 | Cumulative total raw yac WPA for the home team in the game so far. |
| `total_away_raw_yac_wpa` | Float64 | Cumulative total raw yac WPA for the away team in the game so far. |
| `cp` | Float64 | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | Float64 | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `xpass` | Float64 | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | Float64 | Dropback percent over expected on a given play scaled from 0 to 100. |
| `qbr_epa` | Float64 |  |
| `weight` | Float64 | Official weight, in pounds |
| `non_fumble_sack` | Boolean |  |
| `sack_epa` | Float64 |  |
| `pass_epa` | Float64 |  |
| `rush_epa` | Float64 |  |
| `pen_epa` | Float64 |  |
| `sack_weight` | Float64 |  |
| `pass_weight` | Float64 |  |
| `rush_weight` | Float64 |  |
| `pen_weight` | Float64 |  |
| `action_play` | Boolean |  |
| `home_opening_kickoff` | Float64 | 1 if the home team received the opening kickoff, 0 otherwise. |
| `go_wp` | Float64 |  |
| `first_down_prob` | Float64 |  |
| `wp_succeed` | Float64 |  |
| `wp_fail` | Float64 |  |
| `fg_make_prob` | Float64 |  |
| `make_fg_wp` | Float64 |  |
| `miss_fg_wp` | Float64 |  |
| `fg_wp` | Float64 |  |
| `punt_wp` | Float64 |  |
| `go_boost` | Float64 |  |
| `go_wp_diff` | Float64 |  |
| `punt_wp_diff` | Float64 |  |
| `fg_wp_diff` | Float64 |  |
| `fourth_down_recommendation` | String |  |
| `xyac_epa` | Float64 | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | Float64 | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | Float64 | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | Float64 | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | Float64 | Probability play earns a first down based on where the ball was caught. |

**Example**

```python
import polars as pl

from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xyac

pbp = load_nfl_pbp([2023])
pbp = calculate_xyac(pbp)
print(pbp.select("xyac_epa", "xyac_mean_yardage").head())

# Pipeline next step (one line)

pbp.filter(pl.col("xyac_epa").is_not_null()).select("xyac_epa", "xyac_fd").head()
```

### efficiency_ratings {#efficiency_ratings}

`efficiency_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted offense/defense EPA per play.

Filters `plays` to competitive non-special-teams scrimmage plays
(`special != 1`, `qb_kneel != 1`, `qb_spike != 1`,
`min_competitive_wp <= wp <= max_competitive_wp`, non-null
`epa`/`posteam`/`defteam`) and fits
`opponent_adjusted_ridge` on `epa`. Callers pass an already
as-of-date-filtered frame (the public `nfl_ratings` entry point does
the date filter) -- this function is pure.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | An `load_nfl_pbp`-schema frame carrying `game_id`, `posteam`, `defteam`, `home_team`, `epa`, `wp`, `special`, `qb_kneel`, `qb_spike`. |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs (`ridge_lambda` + the competitive-`wp` window); defaults to `RatingsConfig`. |

**Returns**

One row per `team_id` (Utf8) with `adj_off_epa` / `adj_def_epa` / `adj_net` (Float64, `adj_net = adj_off_epa - adj_def_epa`) and `games` (Int64). Zero-row, correctly-typed on empty/fully-filtered input.

| col_name | type | description |
|---|---|---|
| `team_id` | String |  |
| `adj_off_epa` | Float64 |  |
| `adj_def_epa` | Float64 |  |
| `adj_net` | Float64 |  |
| `games` | Int64 | Games played in career |

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import efficiency_ratings
ratings = efficiency_ratings(pbp)
ratings.sort("adj_net", descending=True).head()
```
