---
title: "CFB dataset loaders — Play-by-play: pbp_r"
sidebar_label: "Play-by-play: pbp_r"
sidebar_position: 6
description: "CFB dataset loaders — Play-by-play: pbp_r — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB dataset loaders — Play-by-play: pbp_r

## load_cfb_pbp_r

Release: [cfbfastR_cfb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfbfastR_cfb_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfbfastR_cfb_pbp/play_by_play_{season}.parquet`
### Returns {#load_cfb_pbp_r-returns}

| col_name | type | description |
|---|---|---|
| `year` | Int32 | Four-digit season year (e.g. 2019). |
| `week` | Int32 | Game week of the season. |
| `id_play` | Float64 | Unique CFBD play identifier (concatenates game_id and play index). |
| `game_id` | Int32 | ESPN game identifier. |
| `game_play_number` | Float64 | Sequential play number within the game (excludes timeouts/end markers). |
| `half_play_number` | Float64 | Sequential play number within the current half. |
| `drive_play_number` | Float64 | Sequential play number within the current drive. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team` | String | Team name on defense at the start of the play. |
| `pos_team_score` | Int32 | Score for the team in possession at the start of the play. |
| `def_pos_team_score` | Int32 | Score for the defensive team at the start of the play. |
| `half` | Float64 | Half indicator (1 or 2). |
| `period` | Int32 | Period (quarter) number. |
| `clock_minutes` | Int32 | Minutes remaining on the period clock at the start of the play. |
| `clock_seconds` | Int32 | Game clock value in seconds at the play. |
| `play_type` | String | CFBD play type label (e.g. "Rush", "Pass Reception", "Field Goal Good"). |
| `play_text` | String | Free-form text description of the play from the CFBD feed. |
| `down` | Float64 | Down of the play (1-4). |
| `distance` | Float64 | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `yards_to_goal` | Float64 | Distance in yards from the offense's spot to the opponent's goal line (0-100). |
| `yards_gained` | Float64 | Net yards gained by the offense on the play. |
| `EPA` | Float64 | Expected Points Added on the play (cfbfastR EPA model output). |
| `ep_before` | Float64 | Expected points value before the play (cfbfastR EPA model). |
| `ep_after` | Float64 | Expected points value after the play (cfbfastR EPA model). |
| `wpa` | Float64 | Win Probability Added on the play (cfbfastR WP model output). |
| `wp_before` | Float64 | Win probability for the possession team before the play (0-1). |
| `wp_after` | Float64 | Win probability for the possession team after the play (0-1). |
| `def_wp_before` | Float64 | Win probability for the defensive team before the play (0-1). |
| `def_wp_after` | Float64 | Win probability for the defensive team after the play (0-1). |
| `penalty_detail` | String | Parsed penalty description extracted from play text. |
| `yds_penalty` | Float64 | Yardage assessed on the penalty. |
| `penalty_1st_conv` | Boolean | TRUE when the penalty resulted in a first down conversion. |
| `new_series` | Float64 | Binary flag for the start of a new series of downs. |
| `firstD_by_kickoff` | Float64 | Binary flag for a new first down arising from a kickoff. |
| `firstD_by_poss` | Float64 | Binary flag for a new first down via change of possession. |
| `firstD_by_penalty` | Float64 | Binary flag for a new first down via penalty. |
| `firstD_by_yards` | Float64 | Binary flag for a new first down via yards gained. |
| `def_EPA` | Float64 | EPA for the defensive team on the play (sign-flipped offense EPA). |
| `home_EPA` | Float64 | EPA for the home team on the play. |
| `away_EPA` | Float64 | EPA for the away team on the play. |
| `home_EPA_rush` | Float64 | Rushing EPA for the home team on the play. |
| `away_EPA_rush` | Float64 | Rushing EPA for the away team on the play. |
| `home_EPA_pass` | Float64 | Passing EPA for the home team on the play. |
| `away_EPA_pass` | Float64 | Passing EPA for the away team on the play. |
| `total_home_EPA` | Float64 | Cumulative total EPA for the home team through the play. |
| `total_away_EPA` | Float64 | Cumulative total EPA for the away team through the play. |
| `total_home_EPA_rush` | Float64 | Cumulative rushing EPA for the home team through the play. |
| `total_away_EPA_rush` | Float64 | Cumulative rushing EPA for the away team through the play. |
| `total_home_EPA_pass` | Float64 | Cumulative passing EPA for the home team through the play. |
| `total_away_EPA_pass` | Float64 | Cumulative passing EPA for the away team through the play. |
| `net_home_EPA` | Float64 | Net EPA differential (home minus away) through the play. |
| `net_away_EPA` | Float64 | Net EPA differential (away minus home) through the play. |
| `net_home_EPA_rush` | Float64 | Net rushing EPA differential for the home team through the play. |
| `net_away_EPA_rush` | Float64 | Net rushing EPA differential for the away team through the play. |
| `net_home_EPA_pass` | Float64 | Net passing EPA differential for the home team through the play. |
| `net_away_EPA_pass` | Float64 | Net passing EPA differential for the away team through the play. |
| `success` | Float64 | Binary success-rate flag using the 50/70/100 percent down-state thresholds. |
| `epa_success` | Float64 | Binary flag for plays with positive EPA (EPA > 0). |
| `rz_play` | Float64 | Binary flag for a red-zone play (yards_to_goal <= 20). |
| `scoring_opp` | Float64 | Binary flag for a scoring opportunity (yards_to_goal <= 40). |
| `middle_8` | Boolean | TRUE for plays in the middle-8 window (final 4 min of 1H, first 4 min of 2H). |
| `stuffed_run` | Float64 | Binary flag for a stuffed run (zero or negative yards gained). |
| `change_of_pos_team` | Float64 | Binary flag for change of possession-team on the play. |
| `downs_turnover` | Float64 | Binary flag for a turnover on downs. |
| `turnover` | Float64 | Binary flag for any turnover on the play. |
| `pos_score_diff_start` | Float64 | Score differential for the possession team at the start of the play. |
| `pos_score_pts` | Float64 | Points scored on the play attributed to the possession team. |
| `log_ydstogo` | Float64 | Natural log of distance-to-go (model feature). |
| `ExpScoreDiff` | Float64 | Expected score differential at the start of the play (EPA-adjusted). |
| `ExpScoreDiff_Time_Ratio` | Float64 | Expected score differential scaled by share of time remaining. |
| `half_clock_minutes` | Float64 | Minutes remaining in the half (15 + clock_minutes when in Q1/Q3). |
| `TimeSecsRem` | Float64 | Seconds remaining in the half at the start of the play. |
| `adj_TimeSecsRem` | Float64 | Adjusted seconds remaining used by the EPA/WP models. |
| `Goal_To_Go` | Boolean | TRUE when the offense is in a goal-to-go situation. |
| `Under_two` | Boolean | TRUE when under two minutes remain in the half. |
| `home` | String | Home team name. |
| `away` | String | Away team name. |
| `home_wp_before` | Float64 | Home team win probability before the play (0-1). |
| `away_wp_before` | Float64 | Away team win probability before the play (0-1). |
| `home_wp_after` | Float64 | Home team win probability after the play (0-1). |
| `away_wp_after` | Float64 | Away team win probability after the play (0-1). |
| `end_of_half` | Float64 | Binary flag for the last play of a half. |
| `pos_team_receives_2H_kickoff` | Float64 | Binary flag indicating possession team receives the second-half kickoff. |
| `lead_pos_team` | String | Possession team on the next play (lead value). |
| `lead_play_type` | String | Play type on the next play (lead value). |
| `lag_pos_team` | String | Possession team on the previous play (lag value). |
| `lag_play_type` | String | Play type on the previous play (lag value). |
| `orig_play_type` | String | Original CFBD play type label before cfbfastR cleaning. |
| `Under_three` | Boolean | TRUE when under three minutes remain in the half. |
| `row` | Int32 | Row index within the game grouping (sequencing helper). |
| `drive_event_number` | Float64 | Sequential event number within the current drive. |
| `play_number` | Int32 | Sequential play number within the game (1-indexed). |
| `wallclock` | String | Real-world ISO timestamp of the play. |
| `provider` | String | Sportsbook provider used for spread/over_under joined onto the play. |
| `spread` | Float64 | Pre-game point spread from the selected provider. |
| `formatted_spread` | String | Human-readable formatted spread string from the betting provider. |
| `over_under` | Float64 | Pre-game over/under total from the selected provider. |
| `drive_is_home_offense` | Boolean | TRUE when the home team is on offense for the drive. |
| `drive_start_offense_score` | Int32 | Offense score at the start of the drive. |
| `drive_start_defense_score` | Int32 | Defense score at the start of the drive. |
| `drive_end_offense_score` | Int32 | Offense score at the end of the drive. |
| `drive_end_defense_score` | Int32 | Defense score at the end of the drive. |
| `play` | Float64 | Binary flag indicating the row is a counted play (excludes end markers/timeouts/penalties). |
| `event` | Float64 | Binary flag indicating the row is a counted game event (excludes end markers). |
| `game_event_number` | Float64 | Sequential event number within the game. |
| `game_row_number` | Int32 | Row index within the game grouping. |
| `half_play` | Float64 | Binary flag indicating a counted play within the half. |
| `half_event` | Float64 | Binary flag indicating a counted event within the half. |
| `half_event_number` | Float64 | Sequential event number within the half. |
| `half_row_number` | Int32 | Row index within the half grouping. |
| `pos_unit` | String | Possession-team unit label (offense or special teams). |
| `def_pos_unit` | String | Defensive possession-team unit label (defense or special teams). |
| `drive_play` | Float64 | Binary flag indicating a counted play within the drive. |
| `drive_event` | Float64 | Binary flag indicating a counted event within the drive. |
| `venue_id` | Int32 | Referencing venue id. |
| `venue` | String | Venue name. |
| `neutral_site` | Boolean | TRUE/FALSE flag for if the game took place at a neutral site. |
| `conference_game` | Boolean | TRUE/FALSE flag for this game qualifying as a conference game. |
| `season_type` | String | ESPN season type (2 = regular, 3 = postseason). |
| `start_date` | String | Season start timestamp (ISO 8601, UTC). |
| `completed` | Boolean | `TRUE` if the game is complete. |
| `home_team_id` | Int32 | ESPN home team id (parsed from `home_team_ref`). |
| `home_team` | String | Home team name. |
| `home_team_division` | String |  |
| `home_team_conference` | String | Conference name of the home team. |
| `home_team_pregame_elo` | Int32 | Home team's pregame Elo rating, carried on the cfbfastR-shaped schema. |
| `away_team_id` | Int32 | ESPN away team id (parsed from `away_team_ref`). |
| `away_team` | String | Away team name. |
| `away_team_division` | String |  |
| `away_team_conference` | String | Conference name of the away team. |
| `away_team_pregame_elo` | Int32 | Away team's pregame Elo rating, carried on the cfbfastR-shaped schema. |
| `season` | Int32 | Season (4-digit year). |
| `team` | String | Team name. |
| `conference` | String | Conference of the team. |
| `opponent` | String | Opponent team name. |
| `team_score` | Int32 | Offense team score at the time of the play. |
| `opponent_score` | Int32 | Defense / opponent team score at the time of the play. |
| `down_end` | Float64 | Down number at the end of the play (post-play state). |
| `distance_end` | Float64 | Distance-to-go at the end of the play (post-play state). |
| `log_ydstogo_end` | Float64 | Natural log of post-play distance-to-go (model feature). |
| `yards_to_goal_end` | Float64 | Yards to opponent end zone at the end of the play. |
| `TimeSecsRem_end` | Float64 | Seconds remaining in the half at the end of the play. |
| `Goal_To_Go_end` | Boolean | TRUE when the post-play state is goal-to-go. |
| `Under_two_end` | Boolean | TRUE when the post-play state is under two minutes. |
| `offense_score_play` | Float64 | Binary flag for an offensive scoring play. |
| `defense_score_play` | Float64 | Binary flag for a defensive scoring play. |
| `ppa` | Float64 | Predicted Points Added from the CFBD ppa endpoint (CFB-EPA analogue). |
| `yard_line` | Int32 | Field-position yard line at the start of the play (0-50 scale from the offense's side). |
| `scoring` | Boolean | TRUE when the play results in a score (TD, FG, safety, two-point conversion). |
| `pos_team_timeouts_rem_before` | Float64 | Possession team timeouts remaining before the play. |
| `def_pos_team_timeouts_rem_before` | Float64 | Defensive team timeouts remaining before the play. |
| `pos_team_timeouts` | Int32 | Possession team timeouts remaining after the play. |
| `def_pos_team_timeouts` | Int32 | Defensive team timeouts remaining after the play. |
| `pos_score_diff` | Int32 | Score differential from the possession team's perspective. |
| `pos_score_diff_start_end` | Float64 | Score differential aggregated from start to end of the play. |
| `offense_play` | String | Offensive team name as labeled by CFBD on the play. |
| `defense_play` | String | Defensive team name as labeled by CFBD on the play. |
| `offense_receives_2H_kickoff` | Float64 | Binary flag indicating offense receives the second-half kickoff. |
| `change_of_poss` | Float64 | Binary flag for change of possession on the play (CFBD offense field). |
| `score_pts` | Float64 | Points scored on the play. |
| `score_diff_start` | Float64 | Score differential at the start of the play. |
| `score_diff` | Int32 | Score differential (offense_score - defense_score) at the start. |
| `offense_score` | Int32 | Offense team score at the start of the play. |
| `defense_score` | Int32 | Defense team score at the start of the play. |
| `offense_conference` | String | Conference name of the offense (e.g. "SEC", "ACC"). |
| `defense_conference` | String | Conference name of the defense (e.g. "SEC", "ACC"). |
| `off_timeout_called` | Float64 | Binary flag for an offensive timeout called during the play. |
| `def_timeout_called` | Float64 | Binary flag for a defensive timeout called during the play. |
| `offense_timeouts` | Int32 | Timeouts remaining for the offense at the end of the play. |
| `defense_timeouts` | Int32 | Timeouts remaining for the defense at the end of the play. |
| `off_timeouts_rem_before` | Float64 | Offense timeouts remaining before the play. |
| `def_timeouts_rem_before` | Float64 | Defense timeouts remaining before the play. |
| `rusher_player_name` | String | Name of the rusher on a rushing play. |
| `yds_rushed` | Float64 | Rushing yards gained on the play. |
| `passer_player_name` | String | Name of the passer on a passing play. |
| `receiver_player_name` | String | Name of the receiver on a passing play. |
| `yds_receiving` | Float64 | Receiving yards gained on the play. |
| `yds_sacked` | Float64 | Yards lost on the sack. |
| `sack_players` | String | Combined names of all sack participants. |
| `sack_player_name` | String | Primary sack player name. |
| `sack_player_name2` | String | Secondary sack player name (when split between two defenders). |
| `pass_breakup_player_name` | String | Name of the defender credited with the pass breakup. |
| `interception_player_name` | String | Name of the defender credited with the interception. |
| `yds_int_return` | Float64 | Yards gained on an interception return. |
| `fumble_player_name` | String | Name of the player who fumbled. |
| `fumble_forced_player_name` | String | Name of the player who forced the fumble. |
| `fumble_recovered_player_name` | String | Name of the player who recovered the fumble. |
| `yds_fumble_return` | Float64 | Yards gained on a fumble return. |
| `punter_player_name` | String | Name of the punter. |
| `yds_punted` | Float64 | Yards the ball traveled on the punt. |
| `punt_returner_player_name` | String | Name of the punt returner. |
| `yds_punt_return` | Float64 | Yards gained on the punt return. |
| `yds_punt_gained` | Float64 | Net yards gained on the punt (punt distance minus return). |
| `punt_block_player_name` | String | Name of the player credited with blocking the punt. |
| `punt_block_return_player_name` | String | Name of the player returning a blocked punt. |
| `fg_kicker_player_name` | String | Name of the field goal kicker. |
| `yds_fg` | Float64 | Distance of the field goal attempt in yards. |
| `fg_block_player_name` | String | Name of the player credited with blocking the field goal. |
| `fg_return_player_name` | String | Name of the player returning the blocked/missed field goal. |
| `kickoff_player_name` | String | Name of the kickoff specialist. |
| `yds_kickoff` | Float64 | Yards the ball traveled on the kickoff. |
| `kickoff_returner_player_name` | String | Name of the kickoff returner. |
| `yds_kickoff_return` | Float64 | Yards gained on the kickoff return. |
| `new_id` | Float64 | Numeric play index within the game (id_play with game_id stripped). |
| `orig_drive_number` | Int32 | Original CFBD drive number for the play. |
| `drive_number` | Int32 | Sequential drive number within the game (1-indexed). |
| `drive_result_detailed` | String | Detailed drive result label (e.g. "Punt", "Passing Touchdown", "Downs Turnover"). |
| `new_drive_pts` | Float64 | Points scored on the drive (signed for offense/defense). |
| `drive_id` | Float64 | CFBD drive identifier the play belongs to. |
| `drive_result` | String | Drive result code (`drive_`-prefixed; every drive-level column is carried with this prefix). |
| `drive_start_yards_to_goal` | Float64 | Yards to opponent's end zone at drive start (0-100). |
| `drive_end_yards_to_goal` | Int32 | Yards to opponent's end zone at drive end (0-100). |
| `drive_yards` | Int32 | Net yards gained on the drive. |
| `drive_scoring` | Float64 | Binary flag for a scoring drive. |
| `drive_pts` | Float64 | Points scored on the drive (CFBD/cfbfastR reconciled value). |
| `drive_start_period` | Int32 | Period (quarter) at the start of the drive. |
| `drive_end_period` | Int32 | Period (quarter) at the end of the drive. |
| `drive_time_minutes_start` | Int32 | Minutes on the clock at the start of the drive. |
| `drive_time_seconds_start` | Int32 | Seconds on the clock at the start of the drive. |
| `drive_time_minutes_end` | Int32 | Minutes on the clock at the end of the drive. |
| `drive_time_seconds_end` | Int32 | Seconds on the clock at the end of the drive. |
| `drive_time_minutes_elapsed` | Int32 | Minutes elapsed during the drive. |
| `drive_time_seconds_elapsed` | Int32 | Seconds elapsed during the drive. |
| `drive_numbers` | Float64 | Binary flag marking the first play of a new drive. |
| `number_of_drives` | Float64 | Cumulative count of drives in the game. |
| `pts_scored` | Float64 | Points scored on the play, signed by play_type rule. |
| `drive_result_detailed_flag` | String | Pre-fill copy of drive_result_detailed used during drive reconciliation. |
| `drive_result2` | String | Short-form drive result label (e.g. "TD", "PUNT", "DOWNS"). |
| `drive_num` | Float64 | Game-scoped drive sequence number. |
| `lag_drive_result_detailed` | String | Drive result detailed on the previous play (lag value). |
| `lead_drive_result_detailed` | String | Drive result detailed on the next play (lead value). |
| `lag_new_drive_pts` | Float64 | Drive points on the previous play (lag value). |
| `id_drive` | Float64 | Composite drive identifier (game_id concatenated with drive_num). |
| `rush` | Float64 | Binary flag for a rushing play. |
| `rush_td` | Float64 | Binary flag for a rushing touchdown. |
| `pass` | Float64 | Binary flag for a passing play (includes sacks). |
| `pass_td` | Float64 | Binary flag for a passing touchdown. |
| `completion` | Float64 | Binary flag for a completed pass. |
| `pass_attempt` | Float64 | Binary flag for a pass attempt. |
| `target` | Float64 | Binary flag for a targeted receiver on the play. |
| `sack_vec` | Float64 | Binary flag for a sack play. |
| `sack` | Float64 | Binary flag for a sack (duplicate of sack_vec for downstream use). |
| `int` | Float64 | Binary flag for an interception. |
| `int_td` | Float64 | Binary flag for an interception returned for a touchdown. |
| `turnover_vec` | Float64 | Binary flag for any play classified as a turnover. |
| `turnover_vec_lag` | Float64 | Lag of turnover_vec (previous-play turnover flag). |
| `turnover_indicator` | Float64 | Composite turnover indicator including failed 4th downs. |
| `kickoff_play` | Float64 | Binary flag for a kickoff play. |
| `receives_2H_kickoff` | Float64 | Binary flag for the team receiving the second-half kickoff. |
| `missing_yard_flag` | Boolean | TRUE when post-play yardage had to be imputed. |
| `scoring_play` | Float64 | `TRUE` if the play resulted in a score. |
| `td_play` | Float64 | Binary flag for a touchdown play. |
| `touchdown` | Float64 | Binary flag for a touchdown (duplicate of td_play for downstream use). |
| `safety` | Float64 | Binary flag for a safety. |
| `fumble_vec` | Float64 | Binary flag for a play involving a fumble. |
| `kickoff_tb` | Float64 | Binary flag for a kickoff touchback. |
| `kickoff_onside` | Float64 | Binary flag for an onside kickoff attempt. |
| `kickoff_oob` | Float64 | Binary flag for a kickoff out of bounds. |
| `kickoff_fair_catch` | Float64 | Binary flag for a kickoff fair catch. |
| `kickoff_downed` | Float64 | Binary flag for a kickoff downed in the field of play. |
| `kickoff_safety` | Float64 | Binary flag for a kickoff safety. |
| `kick_play` | Float64 | Binary flag for any kicking play (kickoff or field goal). |
| `punt` | Float64 | Binary flag for a punt play. |
| `punt_play` | Float64 | Binary flag for any punt-related play (includes blocks/returns). |
| `punt_tb` | Float64 | Binary flag for a punt touchback. |
| `punt_oob` | Float64 | Binary flag for a punt out of bounds. |
| `punt_fair_catch` | Float64 | Binary flag for a punt fair catch. |
| `punt_downed` | Float64 | Binary flag for a punt downed in the field of play. |
| `punt_safety` | Float64 | Binary flag for a punt safety. |
| `punt_blocked` | Float64 | Binary flag for a blocked punt. |
| `penalty_safety` | Float64 | Binary flag for a safety scored on a penalty. |
| `fg_inds` | Float64 | Binary flag for a field goal attempt. |
| `fg_made` | Boolean | TRUE when the field goal attempt was successful. |
| `fg_make_prob` | Float64 | Predicted probability of making the field goal (cfbfastR FG model, 0-1). |
| `No_Score_before` | Float64 | Pre-play predicted probability of no score before end of half (cfbfastR EP model, 0-1). |
| `FG_before` | Float64 | Pre-play predicted probability of a posteam field goal next (0-1). |
| `Opp_FG_before` | Float64 | Pre-play predicted probability of a defteam field goal next (0-1). |
| `Opp_Safety_before` | Float64 | Pre-play predicted probability of a defteam safety next (0-1). |
| `Opp_TD_before` | Float64 | Pre-play predicted probability of a defteam touchdown next (0-1). |
| `Safety_before` | Float64 | Pre-play predicted probability of a posteam safety next (0-1). |
| `TD_before` | Float64 | Pre-play predicted probability of a posteam touchdown next (0-1). |
| `No_Score_after` | Float64 | Post-play predicted probability of no score before end of half (0-1). |
| `FG_after` | Float64 | Post-play predicted probability of a posteam field goal next (0-1). |
| `Opp_FG_after` | Float64 | Post-play predicted probability of a defteam field goal next (0-1). |
| `Opp_Safety_after` | Float64 | Post-play predicted probability of a defteam safety next (0-1). |
| `Opp_TD_after` | Float64 | Post-play predicted probability of a defteam touchdown next (0-1). |
| `Safety_after` | Float64 | Post-play predicted probability of a posteam safety next (0-1). |
| `TD_after` | Float64 | Post-play predicted probability of a posteam touchdown next (0-1). |
| `position_reception` | String | Position of the player credited with the reception on the play (cfbfastR-shaped player-position column). |
| `position_target` | String | Position of the receiver targeted on the play (cfbfastR-shaped player-position column). |
| `position_completion` | String | Position of the passer credited with the completion (cfbfastR-shaped player-position column). |
| `position_incompletion` | String | Position of the passer charged with the incompletion (cfbfastR-shaped player-position column). |
| `position_sack_taken` | String | Position of the quarterback who took the sack (cfbfastR-shaped player-position column). |
| `position_sack` | String | Position of the defender credited with the sack (cfbfastR-shaped player-position column). |
| `position_interception_thrown` | String | Position of the passer who threw the interception (cfbfastR-shaped player-position column). |
| `position_interception` | String | Position of the defender who made the interception (cfbfastR-shaped player-position column). |
| `position_fumble` | String | Position of the player who fumbled (cfbfastR-shaped player-position column). |
| `position_fumble_forced` | String | Position of the defender who forced the fumble (cfbfastR-shaped player-position column). |
| `position_fumble_recovered` | String | Position of the player who recovered the fumble (cfbfastR-shaped player-position column). |
| `position_pass_breakup` | String | Position of the defender credited with the pass breakup (cfbfastR-shaped player-position column). |
| `position_rush` | String | Position of the player credited with the rush on the play (cfbfastR-shaped player-position column). |
| `position_touchdown` | String | Position of the player who scored the touchdown (cfbfastR-shaped player-position column). |
| `rush_player_id` | Float64 | CFBD athlete_id of the player credited with a rush attempt. |
| `rush_player` | String | Name of the player credited with a rush attempt. |
| `rush_yds` | Int32 | Rushing yards gained on the play. |
| `reception_player_id` | Float64 | CFBD athlete_id of the receiver credited with a reception. |
| `reception_player` | String | Name of the receiver credited with a reception. |
| `reception_yds` | Int32 | Reception yards gained on the play. |
| `completion_player_id` | Float64 | CFBD athlete_id of the passer credited with a completion. |
| `completion_player` | String | Name of the passer credited with a completion. |
| `completion_yds` | Int32 | Passing yards gained on the completion. |
| `interception_player_id` | Float64 | CFBD athlete_id of the defender credited with an interception. |
| `interception_player` | String | Name of the defender credited with an interception. |
| `interception_stat` | Int32 | Interception stat value reported by CFBD (typically 1 per INT). |
| `interception_thrown_player_id` | Float64 | CFBD athlete_id of the passer charged with the interception. |
| `interception_thrown_player` | String | Name of the passer charged with the interception. |
| `interception_thrown_stat` | Int32 | Interception-thrown stat value reported by CFBD (typically 1 per INT thrown). |
| `touchdown_player_id` | Float64 | CFBD athlete_id of the player credited with the touchdown. |
| `touchdown_player` | String | Name of the player credited with the touchdown. |
| `touchdown_stat` | Int32 | Touchdown stat value reported by CFBD (typically 1 per TD scored). |
| `incompletion_player_id` | Float64 | CFBD athlete_id of the targeted receiver on an incompletion. |
| `incompletion_player` | String | Name of the targeted receiver on an incompletion. |
| `incompletion_stat` | Int32 | Incompletion stat value reported by CFBD (typically 1 per incompletion). |
| `target_player_id` | Float64 | CFBD athlete_id of the targeted receiver on a pass. |
| `target_player` | String | Name of the targeted receiver on a pass. |
| `target_stat` | Int32 | Target stat value reported by CFBD (typically 1 per target). |
| `fumble_recovered_player_id` | Float64 | CFBD athlete_id of the player recovering the fumble. |
| `fumble_recovered_player` | String | Name of the player recovering the fumble. |
| `fumble_recovered_stat` | Int32 | Fumble-recovered stat value reported by CFBD (typically 1 per recovery). |
| `fumble_forced_player_id` | Float64 | CFBD athlete_id of the defender credited with forcing the fumble. |
| `fumble_forced_player` | String | Name of the defender credited with forcing the fumble. |
| `fumble_forced_stat` | Int32 | Fumble-forced stat value reported by CFBD (typically 1 per forced fumble). |
| `fumble_player_id` | Float64 | CFBD athlete_id of the player who fumbled. |
| `fumble_player` | String | Name of the player who fumbled. |
| `fumble_stat` | Int32 | Fumble stat value reported by CFBD (typically 1 per fumble). |
| `sack_player_id` | Float64 | Comma-separated CFBD athlete_id(s) of the sacking defender(s). |
| `sack_player` | String | Comma-separated name(s) of the sacking defender(s). |
| `sack_stat` | Int32 | Sack stat value reported by CFBD (sack credit can be split between defenders). |
| `sack_taken_player_id` | Float64 | CFBD athlete_id of the QB charged with taking the sack. |
| `sack_taken_player` | String | Name of the QB charged with taking the sack. |
| `sack_taken_stat` | Int32 | Sack-taken stat value reported by CFBD (typically 1 per sack taken). |
| `pass_breakup_player_id` | Float64 | CFBD athlete_id of the defender credited with the pass breakup (PBU). |
| `pass_breakup_player` | Boolean | Name of the defender credited with the pass breakup (PBU). |
| `pass_breakup_stat` | Boolean | Pass breakup (PBU) stat value reported by CFBD (typically 1 per PBU). |
| `field_goal_attempt_player_id` | String | CFBD athlete_id of the kicker attempting the field goal. |
| `field_goal_attempt_player` | String | Name of the kicker attempting the field goal. |
| `field_goal_attempt_stat` | Int32 | Field goal attempt distance in yards reported by CFBD. |
| `field_goal_made_player_id` | String | CFBD athlete_id of the kicker on a made field goal. |
| `field_goal_made_player` | String | Name of the kicker on a made field goal. |
| `field_goal_made_stat` | Int32 | Made-field-goal distance in yards reported by CFBD. |
| `field_goal_missed_player_id` | String | CFBD athlete_id of the kicker on a missed field goal. |
| `field_goal_missed_player` | String | Name of the kicker on a missed field goal. |
| `field_goal_missed_stat` | Int32 | Missed-field-goal distance in yards reported by CFBD. |
| `field_goal_blocked_player_id` | String | CFBD athlete_id of the defender credited with blocking the field goal. |
| `field_goal_blocked_player` | String | Name of the defender credited with blocking the field goal. |
| `field_goal_blocked_stat` | Int32 | Blocked-field-goal distance in yards reported by CFBD. |
| `penalty_flag` | Boolean | TRUE when a penalty was flagged on the play. |
| `penalty_declined` | Boolean | TRUE when the penalty was declined. |
| `penalty_no_play` | Boolean | TRUE when the penalty nullified the play (no play counted). |
| `penalty_offset` | Boolean | TRUE when offsetting penalties were called. |
| `penalty_text` | Boolean | TRUE when penalty information is detectable in the play text. |
| `penalty_play_text` | String | Penalty-related substring extracted from the play text. |

```python
load_cfb_pbp_r(seasons=2024)
```
