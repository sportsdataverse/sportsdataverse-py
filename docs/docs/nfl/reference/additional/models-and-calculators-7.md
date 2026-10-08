---
title: "NFL — additional Python functions — Models and calculators: calculate_xyac–get_2pt"
sidebar_label: "Models and calculators: calculate_xyac–get_2pt"
sidebar_position: 19
description: "NFL — additional Python functions — Models and calculators: calculate_xyac–get_2pt — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Models and calculators: calculate_xyac–get_2pt

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
| `down` | integer | The down for the given play. |
| `ydstogo` | integer | Numeric yards in distance from either the first down marker or the endzone in goal down situations. |
| `yardline_100` | integer | Numeric distance in the number of yards from the opponent's endzone for the posteam. |
| `goal_to_go` | integer | Binary indicator for whether or not the posteam is in a goal down situation. |
| `quarter_seconds_remaining` | integer | Numeric seconds remaining in the quarter. |
| `half_seconds_remaining` | integer | Numeric seconds remaining in the half. |
| `game_seconds_remaining` | integer | Numeric seconds remaining in the game. |
| `play_type` | character | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `yards_gained` | integer | Numeric yards gained (or lost) by the possessing team, excluding yards gained via fumble recoveries and laterals. |
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
| `air_yards` | integer | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `yards_after_catch` | integer | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `passing_yards` | integer | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `rushing_yards` | integer | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `receiving_yards` | integer | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `kick_distance` | integer | Numeric distance in yards for kickoffs, field goals, and punts. |
| `return_yards` | integer | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
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
| `fumble_recovery_1_yards` | integer | Yards gained by one of the players with a fumble recovery. |
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
| `drive_start_yard_line` | integer | String indicating where a given drive started consisting of team half and yard line number. |
| `drive_end_yard_line` | integer | String indicating where a given drive ended consisting of team half and yard line number. |
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
| `action_play` | logical |  |
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
| `xyac_epa` | double | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | double | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | double | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | double | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | double | Probability play earns a first down based on where the ball was caught. |

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
| `team_id` | character |  |
| `adj_off_epa` | double |  |
| `adj_def_epa` | double |  |
| `adj_net` | double |  |
| `games` | integer | Games played in career |

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import efficiency_ratings
ratings = efficiency_ratings(pbp)
ratings.sort("adj_net", descending=True).head()
```

### env_adjusted_make_prob {#env_adjusted_make_prob}

`env_adjusted_make_prob(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Add `base_make_prob` + environment-adjusted `exp_make_prob`.

`exp_make_prob = sigmoid(logit(base) + b_wind*wind + b_temp*(temp-baseline)
+ b_alt*altitude_kft)` with coefficients from
`sportsdataverse.nfl.nfl_scheme_constants.ENVIRONMENT_FG_COEF` and
altitude from `STADIUM_ALTITUDE[home_team]`.  Dome / closed-roof kicks
(and missing readings) are treated as neutral (wind 0, temp = baseline).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | FG-attempt rows with `yardline_100` / `roof` / `temp` / `wind` / `home_team` (+ `season` or `era0..era4` / `fg_roof`). |

**Returns**

The input plus `base_make_prob` and `exp_make_prob` (Float64).

| col_name | type | description |
|---|---|---|
| `base_make_prob` | double | Shipped fg_model make probability (with nfl4th long-kick clamps applied). |
| `exp_make_prob` | double | Environment-adjusted make probability (logit shift for long-kick clamp correction, wind, temperature and altitude). |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.nfl_kicker_rating import env_adjusted_make_prob
fg = pl.read_parquet("tests/fixtures/nfl_scheme/fg_attempts_2019_2023.parquet")
out = env_adjusted_make_prob(fg)
print(out.select("base_make_prob", "exp_make_prob").describe())
```

### fg_make_probability {#fg_make_probability}

`fg_make_probability(yardline_100: 'np.ndarray', fg_roof: 'np.ndarray', era: 'np.ndarray') -> 'Optional[np.ndarray]'`

Predict FG make probability from the bundled `fg_model` (public wrapper).

Thin supported alias over the private underscore-prefixed helper so downstream
consumers (e.g. the kicker-rating spine) reuse the shipped model through a
public import instead of a private reach.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `yardline_100` | `ndarray` |  | Kick spot (yards from the opponent end zone); the attempt distance is `yardline_100 + 18`. |
| `fg_roof` | `ndarray` |  | 1.0 when `roof == "outdoors"` else 0.0, per kick. |
| `era` | `ndarray` |  | `(n, 5)` one-hot era matrix (`era0`..`era4`, season cuts 2001/2005/2013/2017). |

**Returns**

Make probabilities (with nfl4th's long-kick clamps), or `None` when the bundled model is unavailable.

**Example**

```python
import numpy as np
from sportsdataverse.nfl.nfl_fourth_down import fg_make_probability
p = fg_make_probability(
    np.array([30.0]), np.array([1.0]),
    np.array([[0.0, 0.0, 0.0, 0.0, 1.0]]),
)
print(p)
```

### fit_nfl_field_position_ep {#fit_nfl_field_position_ep}

`fit_nfl_field_position_ep(pbp: 'pl.DataFrame', *, exclude_garbage: 'bool' = True) -> 'pl.DataFrame'`

Fit the NFL EP-by-starting-yardline curve from released `espn_nfl_pbp` plays.

Extracts one row per drive (starting yard line from the offense's own
goal, realized drive points) and fits the monotone curve with
`sportsdataverse.cfb.cfb_field_position.fit_field_position_ep` --
the same estimator and target the college curve uses. This is how the
bundled artifact was produced; re-run it on newer seasons to refresh it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | plays in the released `espn_nfl_pbp` shape (any number of seasons concatenated). Needs the drive fields (`drive.id`, `drive.result`, `drive.start.yardLine`), `homeTeamId`, `period` and `start.pos_team.id` / `start.def_pos_team.id`. |
| `exclude_garbage` | `bool` | `True` | drop drives that start in garbage time. |

**Returns**

`yardline_own: Int64 (1..99), ep: Float64` -- monotone non-decreasing. Empty input returns a zero-row frame.

No returns table is published for this function: no capture: it raises DuplicateError ('down') on real play-by-play, because it renames start.down to down even when down already exists.

**Example**

```python
import polars as pl
from sportsdataverse.nfl import fit_nfl_field_position_ep
pbp = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
curve = fit_nfl_field_position_ep(pbp)
curve.write_parquet("nfl_field_position_ep.parquet")
```

### get_2pt_probs {#get_2pt_probs}

`get_2pt_probs(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

The PAT-vs-2pt decision surface for post-touchdown states (CFB-shaped).

The NFL twin of `sportsdataverse.cfb.cfb_two_point.get_2pt_probs`. It
runs the same three-outcome enumeration `get_2pt_wp` uses, but returns
the decision columns instead of folding them into `wp_td`. The two option

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Post-touchdown states in nflverse column space (the same inputs `get_4th_down_probs` takes; `score_differential` is the scoring team's lead **after** the six points). Prepared frames are accepted as-is. |

**Returns**

A pandas frame with `go_index` plus `two_pt_wp`, `xp_wp`, `prob_2pt`, `two_pt_recommendation` (`"go_for_2"` iff `two_pt_wp > xp_wp` else `"kick_xp"`) and `two_pt_wp_diff` (`two_pt_wp - xp_wp`). All NaN / null when the models are unavailable.

| col_name | type | description |
|---|---|---|
| `go_index` | integer |  |
| `two_pt_wp` | double |  |
| `xp_wp` | double |  |
| `prob_2pt` | double |  |
| `two_pt_wp_diff` | double |  |
| `two_pt_recommendation` | character |  |

**Example**

```python
from sportsdataverse.nfl.nfl_fourth_down import get_2pt_probs
out = get_2pt_probs(touchdown_states)
print(out[["two_pt_wp", "xp_wp", "two_pt_recommendation"]].head())
```

### get_2pt_wp {#get_2pt_wp}

`get_2pt_wp(pbp_df: "Union[pl.DataFrame, 'pd.DataFrame']") -> 'pd.DataFrame'`

Win probability of the PAT-vs-2pt choice after a touchdown (nfl4th `get_2pt_wp`).

For each row, scores the post-touchdown state under three scoring outcomes
(0 / 1 / 2 added points) from the kicking-off team's ensuing-drive WP, and
combines them with the 2-pt conversion probability (`two_pt_model`) and the
PAT make probability (the FG model at `yardline_100 = 15`) into `wp_td` —
the better of go-for-2 and kick-the-PAT.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Union[DataFrame, 'DataFrame']` |  | Play-by-play frame (polars or pandas) of post-touchdown states, already carrying the prepared state columns (see module docstring). |

**Returns**

A pandas frame with `go_index`, `yardline_100` (always 0) and `wp_td`. `wp_td` is NaN when the WP / 2-pt models are unavailable.

No returns table is published for this function: no capture: it never prepares its input, so it raises KeyError (era3) on real play-by-play.

**Example**

```python
from sportsdataverse.nfl.nfl_fourth_down import get_2pt_wp
out = get_2pt_wp(touchdown_states)
print(out[["go_index", "wp_td"]].head())
```
