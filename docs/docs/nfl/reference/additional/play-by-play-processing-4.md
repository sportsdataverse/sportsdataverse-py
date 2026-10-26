---
title: "NFL — additional Python functions — Play-by-play processing: shield_nfl–team_name"
sidebar_label: "Play-by-play processing: shield_nfl–team_name"
sidebar_position: 14
description: "NFL — additional Python functions — Play-by-play processing: shield_nfl–team_name — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Play-by-play processing: shield_nfl–team_name

### shield_nfl_pbp {#shield_nfl_pbp}

`shield_nfl_pbp(game_detail: 'Optional[Dict[str, Any]]' = None, shield_game_id: 'Optional[str]' = None, *, enrich: 'bool' = True, context: 'Optional[Dict[str, Any]]' = None, game_id: 'Optional[str]' = None) -> 'pl.DataFrame'`

Build one NFL game's nflverse-shape play-by-play from Shield, at ANY game phase.

The live entry point: the same parser `build_pbp` runs on the archived
`nfl/raw` finals, plus the four things a game still being played needs — the
in-progress drive's possession, game-outcome columns held null until the feed says
FINAL, a next-snap row from `summary`, and provisional rows flagged (see
`sportsdataverse.nfl.shield_pbp.live`). Safe to poll: pass the payload you
already have via *game_detail* (no network), or a *shield_game_id* to fetch it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_detail` | `Optional[Dict[str, Any]]` | `None` | A Shield `experience/v2/gamedetails` payload (the raw body, or a `{"data": ...}` envelope). Takes precedence over *shield_game_id*, so tests and pollers that already hold a payload never touch the network. |
| `shield_game_id` | `Optional[str]` | `None` | Shield game uuid, fetched via `sportsdataverse.nfl.nfl_game_details_v2` with `include_drive_chart=True, return_parsed=False` when *game_detail* is None. |
| `enrich` | `bool` | `True` | Run `sportsdataverse.nfl.ep_wp.enrich_nfl_pbp` on the result (default True) for the `nfl_model_pbp` EP/EPA/WP/WPA/CP/CPOE columns. Pass False for the base frame only (no model loads). |
| `context` | `Optional[Dict[str, Any]]` | `None` | Game context `{"roof": ..., "spread_line": ..., "total_line": ...}` the Shield feed omits. Unset fields fall back to the nflverse schedule row for this game, then to `live.DEFAULT_CONTEXT` (`outdoors` / 2.5 / 55.5, the same default the ESPN processor uses). |
| `game_id` | `Optional[str]` | `None` | Override the nflverse game_id (computed from the payload when None). |

**Returns**

A polars DataFrame, one row per play (plus, while `summary.phase` is `INGAME`, one current-situation row), carrying the `nfl_model_pbp` columns — the `build_pbp` base frame, the EP/WP enrichment when *enrich* is True, and: | col_name | type | description | |----------|------|-------------| | `live_phase` | `str` | The payload's `summary.phase`: `PREGAME`, `INGAME`, `HALFTIME`, `FINAL` or `FINAL_OVERTIME`. | | `is_play` | `int` | `1` for a real play; `0` for the feed's `GAME_START` / `END_QUARTER` / `END_GAME` markers and the current-situation row. | | `provisional` | `int` | `1` when the feed has not closed the play (`playEndTime` null) and it is in the trailing run of such plays of a non-final game — its text, yardage and stats may still change. Always `0` on a final game. | `home_score` / `away_score` / `result` are null until the game is final. The current-situation row is not inert once *enrich* is True: it is the next state, so it also completes the **previous** play's lead-diff columns (`epa`, `qb_epa`, `wpa`, `vegas_wpa`, the `total_*` running sums). That play is usually still `provisional`, so those values can move on the next poll. A payload Shield has not populated a drive chart for (every scheduled game before kickoff) returns a zero-row frame carrying only the three live columns — check `df.is_empty()` before selecting anything else.

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
| `xyac_epa` | Float64 | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | Float64 | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | Float64 | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | Float64 | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | Float64 | Probability play earns a first down based on where the ball was caught. |
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

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | Neither *game_detail* nor *shield_game_id* was given. |
| `NoDataError` | Shield has no game for *shield_game_id* (HTTP 404). |
| `AssetFetchError` | The Shield fetch failed or exhausted its retries (HTTP 403 and up). |

**Example**

```python
import polars as pl
from sportsdataverse.nfl import shield_nfl_pbp

df = shield_nfl_pbp(shield_game_id="a9a8944e-4feb-11f1-abca-2c54536568a9")
df.filter(pl.col("is_play") == 0).select("posteam", "down", "ydstogo", "wp")
```

### shield_to_espn_summary {#shield_to_espn_summary}

`shield_to_espn_summary(game_detail: 'Mapping[str, Any]', idmap_row: 'Mapping[str, Any]', *, parsed: 'Optional[pl.DataFrame]' = None, odds: 'Optional[Mapping[str, Any]]' = None, player_stats: 'Optional[Mapping[str, Any]]' = None, team_stats: 'Optional[Mapping[str, Any]]' = None) -> 'Tuple[Dict[str, Any], List[str]]'`

Project one Shield game (any phase) onto an ESPN-summary-shaped dict.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_detail` | `Mapping[str, Any]` |  | A Shield `experience/v2/gamedetails` payload (raw body or a `{"data": ...}` envelope) -- the same object `sportsdataverse.nfl.shield_pbp.build.shield_nfl_pbp` consumes. |
| `idmap_row` | `Mapping[str, Any]` |  | The game's pre-kickoff id-map row (`sportsdataverse.football.sources.idmap.GAME_SCHEMA`): `espn_event_id`, `home_espn_team_id` and `away_espn_team_id` are required; the optional `home_team` / `away_team` sub-dicts supply the era-correct `espn_abbr`. |
| `parsed` | `Optional[DataFrame]` | `None` | The frame `shield_nfl_pbp(game_detail, enrich=False)` already produced. Built here when None -- pass it to parse the payload once for both projections. |
| `odds` | `Optional[Mapping[str, Any]]` | `None` | `{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}` (the stored closing line, `sportsdataverse.football.sources.idmap._odds_override_from_row`). Becomes the summary's one-provider `pickcenter`. |
| `player_stats` | `Optional[Mapping[str, Any]]` | `None` | A Shield `/football/v2/stats/live/player-statistics/{gameId}` body. Becomes `boxscore.players` in ESPN's exact shape (ten categories, athletes carrying ESPN ids from the players crosswalk). Omitted -> the box stays empty and no ESPN athlete id is attached to any play. |
| `team_stats` | `Optional[Mapping[str, Any]]` | `None` | A Shield `/football/v2/stats/live/team-statistics/{gameId}` body. Becomes `boxscore.teams` -- the authoritative countable team totals `NFLPlayProcess.create_box_score` prefers over its play-by-play derivation. |

**Returns**

`(summary, notes)`. | item | type | description | |---|---|---| | summary | dict | An ESPN-summary-shaped payload: `header` (season/week/competitions/competitors/status), `drives.previous` (+ `drives.current` while the game is live), `gameInfo`, `pickcenter`, `boxscore` (filled when `player_stats`/`team_stats` are given) and passthrough arrays. Feed it to `espn_nfl_pbp(summary=)`. | | notes | list[str] | Adapter-side degradations worth surfacing in provenance: a missing `summary.timeouts` block, a missing `summary.homeTeam`/`awayTeam` team id, a PAT with no touchdown to fold into, plays outside the drive chart, and (pre-2014) play ids that do not join ESPN's own. |

**Raises**

| Exception | Description |
|---|---|
| `KeyError` | `idmap_row` is missing `espn_event_id` or a team id. |
| `ValueError` | A team id is empty, None, NaN or `pandas.NA` (possession cannot be attributed). |

**Example**

```python
import json
from sportsdataverse.nfl import NFLPlayProcess, shield_to_espn_summary

# any Shield gamedetails body -- here the copy nfl-raw keeps
with open("nfl/raw/2025/2025_07_LA_JAX.json") as fh:
    game = json.load(fh)
row = {"espn_event_id": "401772635", "home_espn_team_id": "30", "away_espn_team_id": "14"}
summary, notes = shield_to_espn_summary(game, row)
proc = NFLPlayProcess(gameId=401772635, join_participants=False)
proc.espn_nfl_pbp(summary=summary)
result = proc.run_processing_pipeline()
```

### team_name_fn {#team_name_fn}

`team_name_fn(expr: 'pl.Expr') -> 'pl.Expr'`

Fold historical/relocated team codes onto their current abbreviation.

Verbatim port of nflfastR's `team_name_fn` (a plain
`stringr::str_replace_all` over a 10-entry named vector). Operates as a
**substring** replace (not a full-value lookup) so it also fixes
embedded codes like `"SD 49" -> "LAC 49"` on yard-line columns. The
10 from-codes are disjoint from all of their to-values, so the order of
the 10 sequential replacements does not matter (verified in
`tests.nfl.test_nfl_clean`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `expr` | `Expr` |  | A `polars.Expr` over a Utf8 column (e.g. `pl.col("posteam")`). |

**Returns**

The same expression with every occurrence of the 10 historical codes replaced by their current-franchise code.
