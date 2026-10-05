---
title: "CFB dataset loaders — Play-by-play: pbp"
sidebar_label: "Play-by-play: pbp"
sidebar_position: 5
description: "CFB dataset loaders — Play-by-play: pbp — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB dataset loaders — Play-by-play: pbp

## load_cfb_pbp

Release: [espn_cfb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_pbp/play_by_play_{season}.parquet`
### Returns {#load_cfb_pbp-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `game_id` | Int64 | ESPN game identifier. |
| `game_play_number` | Int64 | Sequential play number within the game (excludes timeouts/end markers). |
| `pos_team_id` | Int64 | Team id of the offense (possession team) on the play. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team_id` | Int64 | Team id of the defense on the play. |
| `def_pos_team` | String | Team name on defense at the start of the play. |
| `pos_team_score` | Int64 | Score for the team in possession at the start of the play. |
| `def_pos_team_score` | Int64 | Score for the defensive team at the start of the play. |
| `half` | Int64 | Half indicator (1 or 2). |
| `period` | Int64 | Period (quarter) number. |
| `down` | Int64 | Down of the play (1-4). |
| `distance` | Int64 | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `EPA` | Float64 | Expected Points Added on the play (cfbfastR EPA model output). |
| `wpa` | Float64 | Win Probability Added on the play (cfbfastR WP model output). |
| `wp_before` | Float64 | Win probability for the possession team before the play (0-1). |
| `wp_after` | Float64 | Win probability for the possession team after the play (0-1). |
| `def_wp_before` | Float64 | Win probability for the defensive team before the play (0-1). |
| `def_wp_after` | Float64 | Win probability for the defensive team after the play (0-1). |
| `penalty_detail` | String | Parsed penalty description extracted from play text. |
| `yds_penalty` | String | Yardage assessed on the penalty. |
| `penalty_1st_conv` | Boolean | TRUE when the penalty resulted in a first down conversion. |
| `new_series` | Boolean | Binary flag for the start of a new series of downs. |
| `firstD_by_kickoff` | Boolean | Binary flag for a new first down arising from a kickoff. |
| `firstD_by_poss` | Boolean | Binary flag for a new first down via change of possession. |
| `firstD_by_penalty` | Boolean | Binary flag for a new first down via penalty. |
| `firstD_by_yards` | Boolean | Binary flag for a new first down via yards gained. |
| `def_EPA` | Float64 | EPA for the defensive team on the play (sign-flipped offense EPA). |
| `rz_play` | Boolean | Binary flag for a red-zone play (yards_to_goal <= 20). |
| `scoring_opp` | Boolean | Binary flag for a scoring opportunity (yards_to_goal <= 40). |
| `middle_8` | Boolean | TRUE for plays in the middle-8 window (final 4 min of 1H, first 4 min of 2H). |
| `stuffed_run` | Boolean | Binary flag for a stuffed run (zero or negative yards gained). |
| `change_of_pos_team` | Boolean | Binary flag for change of possession-team on the play. |
| `downs_turnover` | Boolean | Binary flag for a turnover on downs. |
| `pos_score_diff_start` | Int64 | Score differential for the possession team at the start of the play. |
| `pos_score_pts` | Int64 | Points scored on the play attributed to the possession team. |
| `home_wp_before` | Float64 | Home team win probability before the play (0-1). |
| `away_wp_before` | Float64 | Away team win probability before the play (0-1). |
| `home_wp_after` | Float64 | Home team win probability after the play (0-1). |
| `away_wp_after` | Float64 | Away team win probability after the play (0-1). |
| `end_of_half` | Boolean | Binary flag for the last play of a half. |
| `orig_play_type` | String | Original CFBD play type label before cfbfastR cleaning. |
| `offense_score_play` | Boolean | Binary flag for an offensive scoring play. |
| `defense_score_play` | Boolean | Binary flag for a defensive scoring play. |
| `pos_score_diff` | Int64 | Score differential from the possession team's perspective. |
| `change_of_poss` | Boolean | Binary flag for change of possession on the play (CFBD offense field). |
| `rusher_player_name` | String | Name of the rusher on a rushing play. |
| `yds_rushed` | Int64 | Rushing yards gained on the play. |
| `passer_player_name` | String | Name of the passer on a passing play. |
| `receiver_player_name` | String | Name of the receiver on a passing play. |
| `yds_receiving` | Int64 | Receiving yards gained on the play. |
| `yds_sacked` | Int64 | Yards lost on the sack. |
| `sack_players` | String | Combined names of all sack participants. |
| `sack_player_name` | String | Primary sack player name. |
| `sack_player_name2` | String | Secondary sack player name (when split between two defenders). |
| `pass_breakup_player_name` | String | Name of the defender credited with the pass breakup. |
| `interception_player_name` | String | Name of the defender credited with the interception. |
| `yds_int_return` | Int64 | Yards gained on an interception return. |
| `fumble_player_name` | String | Name of the player who fumbled. |
| `fumble_forced_player_name` | String | Name of the player who forced the fumble. |
| `fumble_recovered_player_name` | String | Name of the player who recovered the fumble. |
| `yds_fumble_return` | Int64 | Yards gained on a fumble return. |
| `punter_player_name` | String | Name of the punter. |
| `yds_punted` | Int64 | Yards the ball traveled on the punt. |
| `yds_punt_return` | Int64 | Yards gained on the punt return. |
| `yds_punt_gained` | Int64 | Net yards gained on the punt (punt distance minus return). |
| `punt_block_player_name` | String | Name of the player credited with blocking the punt. |
| `punt_block_return_player_name` | String | Name of the player returning a blocked punt. |
| `fg_kicker_player_name` | String | Name of the field goal kicker. |
| `yds_fg` | Int64 | Distance of the field goal attempt in yards. |
| `fg_block_player_name` | String | Name of the player credited with blocking the field goal. |
| `fg_return_player_name` | String | Name of the player returning the blocked/missed field goal. |
| `kickoff_player_name` | String | Name of the kickoff specialist. |
| `yds_kickoff` | Int64 | Yards the ball traveled on the kickoff. |
| `yds_kickoff_return` | Int64 | Yards gained on the kickoff return. |
| `rush` | Boolean | Binary flag for a rushing play. |
| `rush_td` | Boolean | Binary flag for a rushing touchdown. |
| `pass` | Boolean | Binary flag for a passing play (includes sacks). |
| `pass_td` | Boolean | Binary flag for a passing touchdown. |
| `completion` | Boolean | Binary flag for a completed pass. |
| `pass_attempt` | Boolean | Binary flag for a pass attempt. |
| `target` | Boolean | Binary flag for a targeted receiver on the play. |
| `sack` | Boolean | Binary flag for a sack (duplicate of sack_vec for downstream use). |
| `int` | Boolean | Binary flag for an interception. |
| `int_td` | Boolean | Binary flag for an interception returned for a touchdown. |
| `turnover_vec` | Boolean | Binary flag for any play classified as a turnover. |
| `kickoff_play` | Boolean | Binary flag for a kickoff play. |
| `scoring_play` | Boolean | `TRUE` if the play resulted in a score. |
| `td_play` | Boolean | Binary flag for a touchdown play. |
| `touchdown` | Boolean | Binary flag for a touchdown (duplicate of td_play for downstream use). |
| `safety` | Boolean | Binary flag for a safety. |
| `fumble_vec` | Boolean | Binary flag for a play involving a fumble. |
| `kickoff_tb` | Boolean | Binary flag for a kickoff touchback. |
| `kickoff_onside` | Boolean | Binary flag for an onside kickoff attempt. |
| `kickoff_oob` | Boolean | Binary flag for a kickoff out of bounds. |
| `kickoff_fair_catch` | Boolean | Binary flag for a kickoff fair catch. |
| `kickoff_downed` | Boolean | Binary flag for a kickoff downed in the field of play. |
| `kickoff_safety` | Boolean | Binary flag for a kickoff safety. |
| `punt` | Boolean | Binary flag for a punt play. |
| `punt_play` | Boolean | Binary flag for any punt-related play (includes blocks/returns). |
| `punt_tb` | Boolean | Binary flag for a punt touchback. |
| `punt_oob` | Boolean | Binary flag for a punt out of bounds. |
| `punt_fair_catch` | Boolean | Binary flag for a punt fair catch. |
| `punt_downed` | Boolean | Binary flag for a punt downed in the field of play. |
| `punt_safety` | Boolean | Binary flag for a punt safety. |
| `punt_blocked` | Boolean | Binary flag for a blocked punt. |
| `penalty_safety` | Boolean | Binary flag for a safety scored on a penalty. |
| `fg_made` | Boolean | TRUE when the field goal attempt was successful. |
| `fg_make_prob` | Float64 | Predicted probability of making the field goal (cfbfastR FG model, 0-1). |
| `penalty_flag` | Boolean | TRUE when a penalty was flagged on the play. |
| `penalty_declined` | Boolean | TRUE when the penalty was declined. |
| `penalty_no_play` | Boolean | TRUE when the penalty nullified the play (no play counted). |
| `penalty_offset` | Boolean | TRUE when offsetting penalties were called. |
| `penalty_text` | String | TRUE when penalty information is detectable in the play text. |
| `lead_wp_before2` | Float64 | Value of wp_before 2 plays ahead, used for sequence-aware derivations. |
| `lead_wp_before` | Float64 | Value of wp_before on the next play, used for sequence-aware derivations. |
| `lead_pos_team2` | Int64 | Value of pos_team 2 plays ahead, used for sequence-aware derivations. |
| `id` | Int64 | 247Sports referencing id for the recruit. |
| `sequenceNumber` | Int64 | Broadcast sequence order number. |
| `text` | String | Full play description. |
| `awayScore` | Int64 | Away team score after the goal. |
| `homeScore` | Int64 | Home team score after the goal. |
| `scoringPlay` | Boolean | ESPN flag marking the play as a scoring play. |
| `priority` | Boolean | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | String | ISO timestamp the play record was last modified. |
| `wallclock` | String | Real-world ISO timestamp of the play. |
| `teamParticipants` | String | Raw ESPN team-level participants payload carried through from the plays feed (stringified). |
| `isPenalty` | Boolean | ESPN's per-play flag that a penalty occurred on the play. |
| `statYardage` | Int64 | Yardage ESPN credits to the play for statistical purposes. |
| `isTurnover` | Boolean | ESPN's per-play turnover flag as shipped in the plays feed (broader than the giveaway-based is_turnover derivation). |
| `type.id` | String | ESPN's numeric identifier for the play type. |
| `type.text` | String | ESPN's text label for the play type. |
| `type.abbreviation` | String | ESPN's abbreviation for the play type. |
| `period.number` | Int64 | Period (quarter) number in which the play occurred. |
| `clock.displayValue` | String | Game clock at the play, as the displayed mm:ss string. |
| `start.down` | Int64 | ESPN's `down` value for the play state at the start of the play. |
| `start.distance` | Int64 | ESPN's `distance` value for the play state at the start of the play. |
| `start.yardLine` | Int64 | ESPN's `yardLine` value for the play state at the start of the play. |
| `start.yardsToEndzone` | Int64 | ESPN's `yardsToEndzone` value for the play state at the start of the play. |
| `start.team.id` | Int64 | ESPN's `team.id` value for the play state at the start of the play. |
| `end.down` | Int64 | ESPN's `down` value for the play state at the end of the play. |
| `end.distance` | Int64 | ESPN's `distance` value for the play state at the end of the play. |
| `end.yardLine` | Int64 | ESPN's `yardLine` value for the play state at the end of the play. |
| `end.yardsToEndzone` | Int64 | ESPN's `yardsToEndzone` value for the play state at the end of the play. |
| `end.downDistanceText` | String | ESPN's `downDistanceText` value for the play state at the end of the play. |
| `end.shortDownDistanceText` | String | ESPN's `shortDownDistanceText` value for the play state at the end of the play. |
| `end.possessionText` | String | ESPN's `possessionText` value for the play state at the end of the play. |
| `end.team.id` | Int64 | ESPN's `team.id` value for the play state at the end of the play. |
| `start.downDistanceText` | String | ESPN's `downDistanceText` value for the play state at the start of the play. |
| `start.shortDownDistanceText` | String | ESPN's `shortDownDistanceText` value for the play state at the start of the play. |
| `start.possessionText` | String | ESPN's `possessionText` value for the play state at the start of the play. |
| `scoringType.name` | String | ESPN's name for the scoring type (e.g. touchdown, field goal). |
| `scoringType.displayName` | String | ESPN's display label for the scoring type. |
| `scoringType.abbreviation` | String | ESPN's abbreviation for the scoring type. |
| `pointAfterAttempt.id` | Float64 | ESPN identifier for the point-after attempt type on the scoring play. |
| `pointAfterAttempt.text` | String | ESPN description of the point-after attempt and its result. |
| `pointAfterAttempt.abbreviation` | String | ESPN abbreviation of the point-after attempt type; drives the extra-point / two-point result derivation. |
| `pointAfterAttempt.value` | Float64 | Points ESPN credits for the point-after attempt (1.0 made extra point, 2.0 made two-point try). |
| `drive.id` | String | ESPN's `id` field for the drive containing this play. |
| `drive.displayResult` | String | ESPN's `displayResult` field for the drive containing this play. |
| `drive.isScore` | Boolean | ESPN's `isScore` field for the drive containing this play. |
| `drive.team.shortDisplayName` | String | ESPN's `team.shortDisplayName` field for the drive containing this play. |
| `drive.team.displayName` | String | ESPN's `team.displayName` field for the drive containing this play. |
| `drive.team.name` | String | ESPN's `team.name` field for the drive containing this play. |
| `drive.team.abbreviation` | String | ESPN's `team.abbreviation` field for the drive containing this play. |
| `drive.yards` | Int64 | ESPN's `yards` field for the drive containing this play. |
| `drive.offensivePlays` | Int64 | ESPN's `offensivePlays` field for the drive containing this play. |
| `drive.result` | String | ESPN's `result` field for the drive containing this play. |
| `drive.description` | String | ESPN's `description` field for the drive containing this play. |
| `drive.shortDisplayResult` | String | ESPN's `shortDisplayResult` field for the drive containing this play. |
| `drive.timeElapsed.displayValue` | String | ESPN's `timeElapsed.displayValue` field for the drive containing this play. |
| `drive.start.period.number` | Int64 | ESPN's `start.period.number` field for the drive containing this play. |
| `drive.start.period.type` | String | ESPN's `start.period.type` field for the drive containing this play. |
| `drive.start.yardLine` | Int64 | ESPN's `start.yardLine` field for the drive containing this play. |
| `drive.start.clock.displayValue` | String | ESPN's `start.clock.displayValue` field for the drive containing this play. |
| `drive.start.text` | String | ESPN's `start.text` field for the drive containing this play. |
| `drive.end.period.number` | Int64 | ESPN's `end.period.number` field for the drive containing this play. |
| `drive.end.period.type` | String | ESPN's `end.period.type` field for the drive containing this play. |
| `drive.end.yardLine` | Int64 | ESPN's `end.yardLine` field for the drive containing this play. |
| `drive.end.clock.displayValue` | String | ESPN's `end.clock.displayValue` field for the drive containing this play. |
| `seasonType` | Int64 | ESPN season type for the game (2 = regular season, 3 = postseason). |
| `week` | Int64 | Game week of the season. |
| `status_type_completed` | Boolean | Whether the game is complete. |
| `homeTeamId` | Int64 | ESPN's home-team Id for the game, stamped on every play. |
| `awayTeamId` | Int64 | ESPN's away-team Id for the game, stamped on every play. |
| `homeFinalScore` | Int64 | Final score of the home team from the ESPN game header, repeated on every play of the game; the processing step checks the running score at the last play against it. |
| `awayFinalScore` | Int64 | Final score of the away team from the ESPN game header, repeated on every play of the game; the processing step checks the running score at the last play against it. |
| `homeTeamName` | String | ESPN's home-team Name for the game, stamped on every play. |
| `awayTeamName` | String | ESPN's away-team Name for the game, stamped on every play. |
| `homeTeamMascot` | String | ESPN's home-team Mascot for the game, stamped on every play. |
| `awayTeamMascot` | String | ESPN's away-team Mascot for the game, stamped on every play. |
| `homeTeamAbbrev` | String | ESPN's home-team Abbrev for the game, stamped on every play. |
| `awayTeamAbbrev` | String | ESPN's away-team Abbrev for the game, stamped on every play. |
| `homeTeamNameAlt` | String | ESPN's home-team NameAlt for the game, stamped on every play. |
| `awayTeamNameAlt` | String | ESPN's away-team NameAlt for the game, stamped on every play. |
| `gameSpread` | Float64 | Point spread used as an input to the win-probability model. |
| `homeFavorite` | Boolean | True when the home team was favoured by the spread. |
| `gameSpreadAvailable` | Boolean | True when a spread was available for the game. |
| `overUnder` | Float64 | Over/under total used as a model input. |
| `homeTeamSpread` | Float64 | ESPN's home-team Spread for the game, stamped on every play. |
| `clock.minutes` | Int64 | Minutes remaining on the game clock at the play. |
| `clock.seconds` | Int64 | Seconds component of the game clock at the play. |
| `lag_half` | Int64 | Value of half on the previous play, used for sequence-aware derivations. |
| `lead_half` | Int64 | Value of half on the next play, used for sequence-aware derivations. |
| `start.TimeSecsRem` | Int64 | Seconds remaining in the half from ESPN's clock stamp for this play, which is the end-of-play time in 2005 and 2007+ (the snap time in 2004 and most of 2006); tops out at 1800. |
| `start.adj_TimeSecsRem` | Int64 | ESPN's `adj_TimeSecsRem` value for the play state at the start of the play. |
| `lead_text` | String | Value of text on the next play, used for sequence-aware derivations. |
| `lead_start_team` | String | Value of start_team on the next play, used for sequence-aware derivations. |
| `lead_start_yardsToEndzone` | Int64 | Value of start_yardsToEndzone on the next play, used for sequence-aware derivations. |
| `lead_start_down` | Int64 | Value of start_down on the next play, used for sequence-aware derivations. |
| `lead_start_distance` | Int64 | Value of start_distance on the next play, used for sequence-aware derivations. |
| `lead_scoringPlay` | Boolean | Value of scoringPlay on the next play, used for sequence-aware derivations. |
| `text_dupe` | Boolean | Always False in the emitted frame -- the duplicate-row filter it gates runs before the column is returned, so it marks nothing and is retained only for schema stability. |
| `end_state_missing` | Boolean | Flag that ESPN's end-of-play state (end.team.id) was absent and the end state was imputed. |
| `start.pos_team.id` | Int64 | ESPN's `pos_team.id` value for the play state at the start of the play. |
| `start.def_pos_team.id` | Int64 | ESPN's `def_pos_team.id` value for the play state at the start of the play. |
| `end.def_pos_team.id` | Int64 | ESPN's `def_pos_team.id` value for the play state at the end of the play. |
| `end.pos_team.id` | Int64 | ESPN's `pos_team.id` value for the play state at the end of the play. |
| `start.pos_team.name` | String | ESPN's `pos_team.name` value for the play state at the start of the play. |
| `start.def_pos_team.name` | String | ESPN's `def_pos_team.name` value for the play state at the start of the play. |
| `end.pos_team.name` | String | ESPN's `pos_team.name` value for the play state at the end of the play. |
| `end.def_pos_team.name` | String | ESPN's `def_pos_team.name` value for the play state at the end of the play. |
| `start.is_home` | Boolean | ESPN's `is_home` value for the play state at the start of the play. |
| `end.is_home` | Boolean | ESPN's `is_home` value for the play state at the end of the play. |
| `homeTimeoutCalled` | Boolean | True when the home team called a timeout on the play. |
| `awayTimeoutCalled` | Boolean | True when the away team called a timeout on the play. |
| `end.homeTeamTimeouts` | Int64 | ESPN's `homeTeamTimeouts` value for the play state at the end of the play. |
| `end.awayTeamTimeouts` | Int64 | ESPN's `awayTeamTimeouts` value for the play state at the end of the play. |
| `start.homeTeamTimeouts` | Int64 | ESPN's `homeTeamTimeouts` value for the play state at the start of the play. |
| `start.awayTeamTimeouts` | Int64 | ESPN's `awayTeamTimeouts` value for the play state at the start of the play. |
| `end.TimeSecsRem` | Int64 | Seconds remaining in the half carried as this play's end state; currently the preceding row's clock stamp. |
| `end.adj_TimeSecsRem` | Int64 | ESPN's `adj_TimeSecsRem` value for the play state at the end of the play. |
| `start.posTeamTimeouts` | Int64 | ESPN's `posTeamTimeouts` value for the play state at the start of the play. |
| `start.defPosTeamTimeouts` | Int64 | ESPN's `defPosTeamTimeouts` value for the play state at the start of the play. |
| `end.posTeamTimeouts` | Int64 | ESPN's `posTeamTimeouts` value for the play state at the end of the play. |
| `end.defPosTeamTimeouts` | Int64 | ESPN's `defPosTeamTimeouts` value for the play state at the end of the play. |
| `firstHalfKickoffTeamId` | Int64 | ESPN id of the team that received the opening kickoff. |
| `start.yard` | Int64 | ESPN's `yard` value for the play state at the start of the play. |
| `end.yard` | Int64 | ESPN's `yard` value for the play state at the end of the play. |
| `lag_scoringPlay` | Boolean | Value of scoringPlay on the previous play, used for sequence-aware derivations. |
| `down_1` | Boolean | True when it is 1st down at the start of the play. |
| `down_2` | Boolean | True when it is 2nd down at the start of the play. |
| `down_3` | Boolean | True when it is 3rd down at the start of the play. |
| `down_4` | Boolean | True when it is 4th down at the start of the play. |
| `down_1_end` | Boolean | True when it is 1st down at the end of the play. |
| `down_2_end` | Boolean | True when it is 2nd down at the end of the play. |
| `down_3_end` | Boolean | True when it is 3rd down at the end of the play. |
| `down_4_end` | Boolean | True when it is 4th down at the end of the play. |
| `td_check` | Boolean | Internal flag used while reconciling whether the play produced a touchdown. |
| `forced_fumble` | Boolean | True when the defense forced a fumble on the play. |
| `is_home` | Boolean | Whether the subject team was the home team. |
| `lag_HA_score_diff` | Int64 | Value of HA_score_diff on the previous play, used for sequence-aware derivations. |
| `HA_score_diff` | Int64 | Home score minus away score for the play. |
| `net_HA_score_pts` | Int64 | Net points the play added to the home-minus-away score margin. |
| `H_score_diff` | Int64 | Home team's score minus the away team's, from the home perspective. |
| `A_score_diff` | Int64 | Away team's score minus the home team's, from the away perspective. |
| `lag_homeScore` | Int64 | Value of homeScore on the previous play, used for sequence-aware derivations. |
| `lag_awayScore` | Int64 | Value of awayScore on the previous play, used for sequence-aware derivations. |
| `start.homeScore` | Int64 | ESPN's `homeScore` value for the play state at the start of the play. |
| `start.awayScore` | Int64 | ESPN's `awayScore` value for the play state at the start of the play. |
| `end.homeScore` | Int64 | ESPN's `homeScore` value for the play state at the end of the play. |
| `end.awayScore` | Int64 | ESPN's `awayScore` value for the play state at the end of the play. |
| `start.pos_team_score` | Int64 | ESPN's `pos_team_score` value for the play state at the start of the play. |
| `start.def_pos_team_score` | Int64 | ESPN's `def_pos_team_score` value for the play state at the start of the play. |
| `start.pos_score_diff` | Int64 | ESPN's `pos_score_diff` value for the play state at the start of the play. |
| `end.pos_team_score` | Int64 | ESPN's `pos_team_score` value for the play state at the end of the play. |
| `end.def_pos_team_score` | Int64 | ESPN's `def_pos_team_score` value for the play state at the end of the play. |
| `end.pos_score_diff` | Int64 | ESPN's `pos_score_diff` value for the play state at the end of the play. |
| `start.pos_team_receives_2H_kickoff` | Boolean | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the start of the play. |
| `end.pos_team_receives_2H_kickoff` | Boolean | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the end of the play. |
| `penalty_in_text` | Boolean | True when the play description mentions a penalty. |
| `penalty_count` | Int64 | Number of penalties flagged on the play (0-4 observed). |
| `penalty_declined_count` | Int64 | Number of the flagged penalties that were declined. |
| `penalty_all_declined` | Boolean | Whether every penalty flagged on the play was declined. |
| `penalty_enforcement` | String | How the penalty was resolved: one of no_play, declined, offsetting, negating_foul, play_stands, unknown. |
| `penalty_negated_play` | Boolean | Whether the penalty negated the play's result. |
| `pass_breakup` | Boolean | True when a defender broke up the pass. |
| `pass_depth` | String | Thrown-pass depth parsed from ESPN play text ("short" or "deep"); null when the text omits it (sacks, screens, pre-2025 text). |
| `pass_direction` | String | Pass direction parsed from ESPN play text ("left", "middle", or "right"); null when the text omits it. |
| `rush_direction` | String | Rush direction parsed from ESPN play text ("left", "middle", or "right"); null when the text omits it. |
| `qb_hurry` | Boolean | Whether ESPN's play text says the quarterback was hurried into the throw ("hurried by ..."). |
| `fg_attempt` | Boolean | True when the play was a field-goal attempt. |
| `pos_unit` | String | Possession-team unit label (offense or special teams). |
| `def_pos_unit` | String | Defensive possession-team unit label (defense or special teams). |
| `sp` | Boolean | Binary indicator for whether or not a score occurred on the play. |
| `play` | Boolean | Binary flag indicating the row is a counted play (excludes end markers/timeouts/penalties). |
| `cleaned_text` | String | Play description with overturned-call prefixes stripped; the text the name and team extractors run against. |
| `kneel_down` | Boolean | Whether the play is an offensive kneel, from explicit kneel text plus an end-of-half TEAM-rush heuristic. |
| `scrimmage_play` | Boolean | True when the play is a play from scrimmage rather than a special-teams or administrative row. |
| `pos_score_diff_end` | Int64 | Score differential from the possessing team's perspective at the end of the play. |
| `fumble_lost` | Boolean | Binary indicator for if the fumble was lost. |
| `fumble_recovered` | Boolean | True when a fumble on the play was recovered. |
| `field_goal_result` | String | String indicator for result of field goal attempt: made, missed, or blocked. |
| `extra_point_result` | String | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | String | String result of the two-point conversion attempt: success, failure, or safety (touchback in the defensive end zone). |
| `defensive_two_point_attempt` | Boolean | Binary indicator whether or not the defense was able to have an attempt on a two point conversion, this results following a turnover. |
| `defensive_two_point_conv` | Boolean | Binary indicator whether or not the defense successfully scored on the two point conversion. |
| `yds_punted_source` | String | Provenance of yds_punted: "text" when the value was present before the special-teams derivation step (parsed from the play text, or set by a flag convention such as a blocked punt's 0), "derived" when that step filled it from field position, null when there is no value. |
| `yds_kickoff_source` | String | Provenance of yds_kickoff: "text" when the value was present before the special-teams derivation step (parsed from the play text, or set by a flag convention such as a blocked punt's 0), "derived" when that step filled it from field position, null when there is no value. |
| `yds_punt_return_source` | String | Provenance of yds_punt_return: "text" when the value was present before the special-teams derivation step (parsed from the play text, or set by a flag convention such as a blocked punt's 0), "derived" when that step filled it from field position, null when there is no value. |
| `air_yardsToEndzone` | Int64 | Yards to the endzone at the catch spot, parsed from the 2025+ vendor catch-spot text; null before 2025 or when unresolvable. |
| `air_yards` | Int64 | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `yards_after_catch` | Int64 | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `kickoff_return_player_name` | String | Name of the player returning the kickoff, when the play was returned. |
| `punt_return_player_name` | String | Name of the player returning the punt, when the punt was returned. |
| `xp_attempt` | Boolean | Whether an extra-point kick was attempted on the play. |
| `xp_made` | Boolean | Whether the extra-point kick was successful. |
| `xp_kicker_player_name` | String | Name of the kicker attempting the extra point. |
| `kicking_team` | Int64 | Team id of the kicking team on kickoff, punt, and field-goal plays. |
| `return_team` | Int64 | Team id of the returning side; set on interception, fumble, kickoff, punt, and blocked-kick returns. |
| `fumble_or_muff` | Boolean | Whether the play includes a fumble or a muffed kick or punt (widened beyond ESPN's fumble play types). |
| `recovery_team` | Int64 | Team id parsed from the play text as recovering the fumble or muff. |
| `recovery_team_2` | Int64 | Team id of the second recovery in a multi-recovery scramble, parsed from the play text. |
| `penalty_spot_yardline` | Int64 | Yard line (0-50) at which the penalty was spotted. |
| `penalty_spot_side` | String | Side of the field the penalty was spotted on: 'home', 'away' or 'mid' (midfield). |
| `penalty_spot_yardsToEndzone` | Int64 | Yards from the penalty spot to the end zone (0-100). |
| `fumbling_team` | Int64 | Team id of the side that fumbled or muffed the ball, parsed from the play text. |
| `int_turnover` | Boolean | Whether the play is an interception giveaway. |
| `pos_fumble_lost` | Boolean | Whether the possession team fumbled and lost the ball. |
| `def_fumble_lost` | Boolean | Whether the defending team (e.g. a returner after a takeaway) fumbled and lost the ball back. |
| `is_pos_team_turnover` | Boolean | Whether the possession team committed a giveaway (interception or fumble lost). |
| `is_def_pos_team_turnover` | Boolean | Whether the defending team gave the ball back via a lost fumble. |
| `is_turnover` | Boolean | True when the play is a giveaway-based turnover (interception thrown or fumble lost); blocked kicks recovered by the defense are carried by the blocked-kick fields instead. |
| `turnover_team` | Int64 | Team id charged with the giveaway on the play. |
| `is_st_turnover` | Boolean | Whether the giveaway happened on a special-teams play (kick or punt snap, or a return). |
| `is_blocked_punt_turnover` | Boolean | Blocked-punt possession loss (blocked-punt TD, or the defense recovered); kept out of is_turnover to match ESPN's giveaway-only box. |
| `is_blocked_fg_turnover` | Boolean | Blocked-field-goal possession loss (blocked-FG TD, or the defense recovered); kept out of is_turnover to match ESPN's giveaway-only box. |
| `sack_team` | Int64 | Team id credited with the sack (the defense). |
| `interception_team` | Int64 | Team id credited with the interception (the defense). |
| `pass_breakup_team` | Int64 | Team id credited with the pass breakup (the defense). |
| `forced_fumble_team` | Int64 | Team id credited with forcing the fumble -- the side opposite the fumbling player (the covering team on returns). |
| `fumble_recovery_team` | Int64 | Team id that recovered the fumble or muff, from parsed text with a giveaway / own-recovery fallback. |
| `punt_return_team` | Int64 | Team id of the punt-returning side. |
| `kick_return_team` | Int64 | Team id of the kick-returning side. |
| `fg_team` | Int64 | Team id attempting the field goal (the kicking team). |
| `punt_team` | Int64 | Team id punting the ball (the kicking team). |
| `penalized_team` | Int64 | Team id the penalty was assessed against, from the home/away text resolver with a foul-direction fallback. |
| `penalty_yards_signed` | Int64 | Penalty yardage parsed from the play text with era-aware bounds; the printed sign is retained but is not a reliable enforcement direction. |
| `penalty_side` | String | Which side committed the penalty -- 'off' (offense) or 'def' (defense). |
| `penalty_yards_net` | Int64 | Net yardage assessed for the penalty, signed relative to the possession team (observed -25 to 25). |
| `penalty_team_id` | Int64 | Team id of the side that committed the penalty. |
| `new_down` | Int64 | Down after the play, including any penalty enforcement. |
| `new_distance` | Int64 | Distance to go after the play, including any penalty enforcement. |
| `under_2` | Boolean | Whether the play began with two minutes or less remaining in the half. |
| `goal_to_go` | Boolean | Binary indicator for whether or not the posteam is in a goal down situation. |
| `stopped_run` | Boolean | True when the rush was stopped at or behind the line of scrimmage. |
| `opportunity_run` | Boolean | True when a rush reached 4 yards -- the carries on which the blocking did its job. Matches cfbfastR's espn_cfb_15 definition. Assets published before the 2026-08 fix carry the inverted (4 yards or fewer) flag. |
| `highlight_run` | Boolean | True when the rush gained 8 or more yards. |
| `adj_rush_yardage` | Int64 | Rushing yards capped at 8, the input to the line-yards decomposition. |
| `line_yards` | Float64 | Yards credited to the offensive line on a rush, using the standard sliding scale: 1.2x the capped yardage on a loss, all of it through 3 yards, half of each yard from 4 to 8, and a 5.5-yard ceiling beyond that. |
| `second_level_yards` | Float64 | Rushing yards earned from 4 to 8, split evenly between line and carrier under the line-yards decomposition. |
| `open_field_yards` | Int64 | Rushing yards gained beyond 8, credited to the ball carrier rather than the line. |
| `highlight_yards` | Float64 | Second-level plus open-field yards -- the yardage credited to the carrier. |
| `opp_highlight_yards` | Float64 | Highlight yards earned on opportunity runs, isolating carrier production on carries where the blocking succeeded. Assets published before the 2026-08 fix are identically 0 here, because the inverted opportunity_run gate could never co-occur with non-zero highlight yards. |
| `short_rush_success` | Boolean | True when a short-yardage rush gained the yardage needed. |
| `short_rush_attempt` | Boolean | True when the play is a rush in a short-yardage situation. |
| `early_down` | Boolean | True when the play is a scrimmage play on first or second down. |
| `late_down` | Boolean | True when the play is a scrimmage play on third or fourth down. |
| `power_rush_attempt` | Boolean | True when the play is a short-yardage power rushing attempt. |
| `power_rush_success` | Boolean | True when a power rushing attempt gained the yardage needed. |
| `early_down_pass` | Boolean | True when the play is a pass on an early down. |
| `early_down_rush` | Boolean | True when the play is a rush on an early down. |
| `late_down_pass` | Boolean | True when the play is a pass on a late down. |
| `late_down_rush` | Boolean | True when the play is a rush on a late down. |
| `standard_down` | Boolean | True when the offense is on schedule for the series -- first down, second down needing fewer than 8, or third/fourth down needing fewer than 5. |
| `passing_down` | Boolean | True when the offense is behind schedule for the series -- second down needing 8 or more, or third/fourth down needing 5 or more. |
| `TFL` | Boolean | True when the play was a tackle for loss. |
| `TFL_pass` | Boolean | True when the play was a tackle for loss on a pass play (a sack). |
| `TFL_rush` | Boolean | True when the play was a tackle for loss on a rush play. |
| `havoc` | Boolean | True when the defense disrupted the play: a pass breakup, tackle for loss, interception or forced fumble. |
| `first_down_yards` | Boolean | Whether the play gained enough yardage to earn a first down. |
| `first_down_penalty` | Boolean | Binary indicator for if a penalty converted the first down. |
| `first_down_earned` | Boolean | Whether the play earned a first down by means other than yardage (e.g. by penalty). |
| `start.pos_team_spread` | Float64 | ESPN's `pos_team_spread` value for the play state at the start of the play. |
| `start.elapsed_share` | Float64 | ESPN's `elapsed_share` value for the play state at the start of the play. |
| `start.spread_time` | Float64 | ESPN's `spread_time` value for the play state at the start of the play. |
| `end.pos_team_spread` | Float64 | ESPN's `pos_team_spread` value for the play state at the end of the play. |
| `end.elapsed_share` | Float64 | ESPN's `elapsed_share` value for the play state at the end of the play. |
| `end.spread_time` | Float64 | ESPN's `spread_time` value for the play state at the end of the play. |
| `penalty_assessed_on_kickoff` | Boolean | Whether a penalty was assessed on a kickoff; such plays take the kickoff/touchback win-probability handling. |
| `start.yardsToEndzone.touchback` | Int64 | ESPN's `yardsToEndzone.touchback` value for the play state at the start of the play. |
| `EP_start_touchback` | Float64 | Expected points the offense would have had from a touchback on this play. |
| `EP_start` | Float64 | Expected points for the offense at the start of the play. |
| `EP_end` | Float64 | Expected points for the offense at the end of the play. |
| `EP_penalty_cf` | Float64 | Counterfactual expected points for the penalty branch -- the EP had the alternative penalty outcome been taken (null unless a penalty decision existed). |
| `penalty_cf_yardsToEndzone` | Int64 | Yards to the end zone in the counterfactual penalty branch. |
| `lag_EP_end` | Float64 | Value of EP_end on the previous play, used for sequence-aware derivations. |
| `EP_between` | Float64 | Change in expected points across the play, before penalty adjustment. |
| `EPA_scrimmage` | Float64 | EPA credited to the play on plays from scrimmage. |
| `EPA_rush` | Float64 | EPA credited to the play on rush plays. |
| `EPA_pass` | Float64 | EPA credited to the play on pass plays. |
| `EPA_explosive` | Boolean | True when the play was explosive. |
| `EPA_non_explosive` | Float64 | EPA credited to the play on non-explosive plays. |
| `EPA_explosive_pass` | Boolean | True when the pass play was explosive. |
| `EPA_explosive_rush` | Boolean | True when the rush play was explosive. |
| `first_down_created` | Boolean | True when the play produced a first down for the offense. |
| `EPA_success` | Boolean | True when the play was successful by EPA. |
| `EPA_success_early_down` | Boolean | True when the play on an early down was successful by EPA. |
| `EPA_success_early_down_pass` | Boolean | True when the pass play on an early down was successful by EPA. |
| `EPA_success_early_down_rush` | Boolean | True when the rush play on an early down was successful by EPA. |
| `EPA_success_late_down` | Boolean | True when the play on a late down was successful by EPA. |
| `EPA_success_late_down_pass` | Boolean | True when the pass play on a late down was successful by EPA. |
| `EPA_success_late_down_rush` | Boolean | True when the rush play on a late down was successful by EPA. |
| `EPA_success_standard_down` | Boolean | True when the play on a standard down was successful by EPA. |
| `EPA_success_passing_down` | Boolean | True when the play on a passing down was successful by EPA. |
| `EPA_success_pass` | Boolean | True when the pass play was successful by EPA. |
| `EPA_success_rush` | Boolean | True when the rush play was successful by EPA. |
| `EPA_success_EPA` | Float64 | EPA on successful plays. |
| `EPA_success_standard_down_EPA` | Float64 | EPA on successful plays on a standard down. |
| `EPA_success_passing_down_EPA` | Float64 | EPA on successful plays on a passing down. |
| `EPA_success_pass_EPA` | Float64 | EPA on successful pass plays. |
| `EPA_success_rush_EPA` | Float64 | EPA on successful rush plays. |
| `EPA_middle_8_success` | Boolean | True when the play in the middle eight was successful by EPA. |
| `EPA_middle_8_success_pass` | Boolean | True when the pass play in the middle eight was successful by EPA. |
| `EPA_middle_8_success_rush` | Boolean | True when the rush play in the middle eight was successful by EPA. |
| `EPA_penalty` | Float64 | EPA credited to the play attributable to penalties. |
| `EPA_penalty_direct` | Float64 | EPA attributable directly to the penalty on the play, separated from the EPA of the play itself (observed -11.7 to 8.05; null when no penalty applied). |
| `EPA_sp` | Float64 | EPA credited to the play on special-teams plays. |
| `EPA_fg` | Float64 | EPA credited to the play on field-goal attempts. |
| `EPA_punt` | Float64 | EPA credited to the play on punt plays. |
| `EPA_kickoff` | Float64 | EPA credited to the play on kickoff plays. |
| `start.ExpScoreDiff_touchback` | Float64 | ESPN's `ExpScoreDiff_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff` | Float64 | ESPN's `ExpScoreDiff` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio_touchback` | Float64 | ESPN's `ExpScoreDiff_Time_Ratio_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio` | Float64 | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the start of the play. |
| `end.ExpScoreDiff` | Float64 | ESPN's `ExpScoreDiff` value for the play state at the end of the play. |
| `end.ExpScoreDiff_Time_Ratio` | Float64 | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the end of the play. |
| `wp_touchback` | Float64 | Win probability the offense would have had starting from a touchback. |
| `wp_before_naive` | Float64 | Pre-snap possession-team win probability from the spread-free (naive) WP model. |
| `wp_touchback_naive` | Float64 | Naive-model win probability for the kickoff-touchback substitute state, used as the pre-snap WP on kickoffs. |
| `wp_after_naive` | Float64 | End-of-play possession-team win probability from the naive model, after the game-logic adjustment chain. |
| `def_wp_before_naive` | Float64 | Pre-snap defense win probability under the naive model (1 - wp_before_naive). |
| `home_wp_before_naive` | Float64 | Pre-snap naive win probability mapped to the home team. |
| `away_wp_before_naive` | Float64 | Pre-snap naive win probability mapped to the away team. |
| `lead_wp_before_naive` | Float64 | Next play's pre-snap naive win probability, used in the end-of-half and change-of-possession adjustments. |
| `lead_wp_before2_naive` | Float64 | Pre-snap naive win probability two plays ahead, used where the immediately following row is a non-play. |
| `def_wp_after_naive` | Float64 | End-of-play defense win probability under the naive model. |
| `home_wp_after_naive` | Float64 | End-of-play naive win probability mapped to the home team. |
| `away_wp_after_naive` | Float64 | End-of-play naive win probability mapped to the away team. |
| `wpa_naive` | Float64 | Win probability added on the play under the spread-free (naive) model. |
| `cp` | Float64 | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cp_game_state` | Float64 | Completion probability from the 8-feature game-state booster, scored on every pass play regardless of which model produced cp. On one scale across seasons, so use it (not cp) for anything summed or averaged; null on non-pass plays. |
| `cp_model` | String | Which completion-probability booster scored cp on the play: "air_yards" (the 11-feature model, used where ESPN's play text gives a catch/target spot -- essentially 2025 onward) or "game_state" (the 8-feature model used everywhere else). The two are not on one scale, so group any cpoe aggregate by this column; null on non-pass plays. |
| `cpoe` | Float64 | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `era` | Int64 | one of pre2018 (2006-2017) or post2018 (2018+) |
| `xpass` | Float64 | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | Float64 | Dropback percent over expected on a given play scaled from 0 to 100. |
| `drive_start` | Float64 | Yard line at which the drive began. |
| `drive_stopped` | Boolean | True when the play ended the drive. |
| `drive_play_index` | Int64 | Sequence number of the play within its drive. |
| `drive_offense_plays` | Int64 | Offensive plays run on the drive. |
| `prog_drive_EPA` | Float64 | Cumulative EPA accrued by the drive up to and including this play. |
| `prog_drive_WPA` | Float64 | Cumulative win-probability added by the drive up to and including this play. |
| `drive_offense_yards` | Int64 | Offensive yards gained on the drive. |
| `drive_total_yards` | Int64 | Total yards gained on the drive. |
| `qbr_epa` | Float64 | EPA variant used as an input to the QBR calculation. |
| `weight` | Float64 | Listed weight (lbs). |
| `non_fumble_sack` | Boolean | True when the play was a sack that did not produce a fumble. |
| `sack_epa` | Float64 | EPA credited to the play when it is a sack. |
| `pass_epa` | Float64 | EPA credited to the play when it is a pass. |
| `rush_epa` | Float64 | EPA credited to the play when it is a rush. |
| `pen_epa` | Float64 | EPA attributable to a penalty on the play. |
| `sack_weight` | Float64 | Weighting applied to the sack component of the play. |
| `pass_weight` | Float64 | Weighting applied to the pass component of the play. |
| `rush_weight` | Float64 | Weighting applied to the rush component of the play. |
| `pen_weight` | Float64 | Weighting applied to the penalty component of the play. |
| `action_play` | Boolean | True when the play advanced the game state -- excludes timeouts, end-of-period markers and other non-action rows. |
| `athlete_name` | String | Player full name. |
| `rusher_player_id` | Int64 | Unique identifier for the player that attempted the run. |
| `passer_player_id` | Int64 | Unique identifier for the player that attempted the pass. |
| `receiver_player_id` | Int64 | Unique identifier for the receiver that was targeted on the pass. |
| `fumble_player_id` | Int64 | CFBD athlete_id of the player who fumbled. |
| `sack_player_id` | Int64 | Comma-separated CFBD athlete_id(s) of the sacking defender(s). |
| `sack_player_id2` | Int64 | ESPN athlete id of the second sacker on a split sack (regex fallback for an ESPN sidecar blind spot). |
| `interception_player_id` | Int64 | CFBD athlete_id of the defender credited with an interception. |
| `pass_breakup_player_id` | Int64 | CFBD athlete_id of the defender credited with the pass breakup (PBU). |
| `fumble_forced_player_id` | Int64 | CFBD athlete_id of the defender credited with forcing the fumble. |
| `fumble_recovered_player_id` | Int64 | CFBD athlete_id of the player recovering the fumble. |
| `fg_kicker_player_id` | Int64 | ESPN athlete id of the field-goal kicker. |
| `punter_player_id` | Int64 | Unique identifier for the punter. |
| `kickoff_player_id` | Int64 | ESPN athlete id of the player kicking off. |
| `kickoff_return_player_id` | Int64 | ESPN athlete id of the kickoff returner. |
| `punt_return_player_id` | Int64 | ESPN athlete id of the punt returner. |
| `fg_block_player_id` | Int64 | ESPN athlete id of the player who blocked the field goal. |
| `punt_block_player_id` | Int64 | ESPN athlete id of the player who blocked the punt. |
| `fg_return_player_id` | Int64 | ESPN athlete id of the player who returned the blocked or missed field goal. |
| `punt_block_return_player_id` | Null | ESPN athlete id of the player who returned the blocked punt. |
| `go_wp` | Float64 | Win probability from going for it on fourth down: conversion-probability-weighted mean of the success and failure states (cfb4th port). |
| `first_down_prob` | Float64 | Modeled probability of converting the fourth down when going for it. |
| `wp_succeed` | Float64 | Mean win probability across yardage outcomes given the fourth-down attempt converts. |
| `wp_fail` | Float64 | Mean win probability given the fourth-down attempt fails. |
| `make_fg_wp` | Float64 | Win probability given the field-goal attempt is made. |
| `miss_fg_wp` | Float64 | Win probability given the field-goal attempt misses. |
| `fg_wp` | Float64 | Make-probability-weighted win probability of attempting the field goal. |
| `punt_wp` | Float64 | Win probability of punting, from the bundled punt-outcome distribution. |
| `go_boost` | Float64 | cfb4th's headline number: 100 * (go_wp - max(fg_wp, punt_wp)), in percentage points. |
| `go_wp_diff` | Float64 | go_wp minus the recommended option's WP (0 when going for it is the recommendation, otherwise <= 0). |
| `fg_wp_diff` | Float64 | fg_wp minus the recommended option's WP (0 when the field goal is the recommendation, otherwise <= 0). |
| `punt_wp_diff` | Float64 | punt_wp minus the recommended option's WP (0 when punting is the recommendation, otherwise <= 0). |
| `fourth_down_recommendation` | String | Max-WP fourth-down choice among "go", "punt", and "field_goal". |
| `two_pt_wp` | Float64 | Win probability of going for two: conversion-probability-weighted mean of the 2-point and 0-point outcomes (cfb4th port). |
| `xp_wp` | Float64 | Win probability of kicking the extra point, weighting the make by the empirical CFB extra-point make rate. |
| `prob_2pt` | Float64 | Two-point conversion probability from the bundled CFB two-point model. |
| `two_pt_recommendation` | String | Point-after recommendation: "go_for_2" when two_pt_wp exceeds xp_wp, otherwise "kick_xp". |
| `two_pt_wp_diff` | Float64 | two_pt_wp minus xp_wp; positive favors going for two. |
| `mediaId` | String | Identifier of the ESPN media item (video clip) attached to the play in ESPN's play feed, passed through as published; null when no clip is attached. |

```python
load_cfb_pbp(seasons=2024)
```
