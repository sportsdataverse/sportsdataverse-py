---
title: "CFB — additional Python functions — Models and calculators: add_era–cfb_adjusted"
sidebar_label: "Models and calculators: add_era–cfb_adjusted"
sidebar_position: 7
description: "CFB — additional Python functions — Models and calculators: add_era–cfb_adjusted — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Models and calculators: add_era–cfb_adjusted

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
| `pos_team_id` | integer | Team id of the offense (possession team) on the play. |
| `pos_team` | character | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team_id` | integer | Team id of the defense on the play. |
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
| `lead_wp_before2` | double | Value of wp_before 2 plays ahead, used for sequence-aware derivations. |
| `lead_wp_before` | double | Value of wp_before on the next play, used for sequence-aware derivations. |
| `lead_pos_team2` | integer | Value of pos_team 2 plays ahead, used for sequence-aware derivations. |
| `id` | integer | 247Sports referencing id for the recruit. |
| `sequenceNumber` | integer |  |
| `text` | character | Full play description. |
| `awayScore` | integer |  |
| `homeScore` | integer |  |
| `scoringPlay` | logical | ESPN flag marking the play as a scoring play. |
| `priority` | logical | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | character | ISO timestamp the play record was last modified. |
| `wallclock` | character | Real-world ISO timestamp of the play. |
| `teamParticipants` | character | Raw ESPN team-level participants payload carried through from the plays feed (stringified). |
| `isPenalty` | logical | ESPN's per-play flag that a penalty occurred on the play. |
| `statYardage` | integer | Yardage ESPN credits to the play for statistical purposes. |
| `isTurnover` | logical | ESPN's per-play turnover flag as shipped in the plays feed (broader than the giveaway-based is_turnover derivation). |
| `type.id` | character | ESPN's numeric identifier for the play type. |
| `type.text` | character | ESPN's text label for the play type. |
| `type.abbreviation` | character | ESPN's abbreviation for the play type. |
| `period.number` | integer | Period (quarter) number in which the play occurred. |
| `clock.displayValue` | character | Game clock at the play, as the displayed mm:ss string. |
| `start.down` | integer | ESPN's `down` value for the play state at the start of the play. |
| `start.distance` | integer | ESPN's `distance` value for the play state at the start of the play. |
| `start.yardLine` | integer | ESPN's `yardLine` value for the play state at the start of the play. |
| `start.yardsToEndzone` | integer | ESPN's `yardsToEndzone` value for the play state at the start of the play. |
| `start.team.id` | integer | ESPN's `team.id` value for the play state at the start of the play. |
| `end.down` | integer | ESPN's `down` value for the play state at the end of the play. |
| `end.distance` | integer | ESPN's `distance` value for the play state at the end of the play. |
| `end.yardLine` | integer | ESPN's `yardLine` value for the play state at the end of the play. |
| `end.yardsToEndzone` | integer | ESPN's `yardsToEndzone` value for the play state at the end of the play. |
| `end.downDistanceText` | character | ESPN's `downDistanceText` value for the play state at the end of the play. |
| `end.shortDownDistanceText` | character | ESPN's `shortDownDistanceText` value for the play state at the end of the play. |
| `end.possessionText` | character | ESPN's `possessionText` value for the play state at the end of the play. |
| `end.team.id` | integer | ESPN's `team.id` value for the play state at the end of the play. |
| `start.downDistanceText` | character | ESPN's `downDistanceText` value for the play state at the start of the play. |
| `start.shortDownDistanceText` | character | ESPN's `shortDownDistanceText` value for the play state at the start of the play. |
| `start.possessionText` | character | ESPN's `possessionText` value for the play state at the start of the play. |
| `scoringType.name` | character | ESPN's name for the scoring type (e.g. touchdown, field goal). |
| `scoringType.displayName` | character | ESPN's display label for the scoring type. |
| `scoringType.abbreviation` | character | ESPN's abbreviation for the scoring type. |
| `pointAfterAttempt.id` | double | ESPN identifier for the point-after attempt type on the scoring play. |
| `pointAfterAttempt.text` | character | ESPN description of the point-after attempt and its result. |
| `pointAfterAttempt.abbreviation` | character | ESPN abbreviation of the point-after attempt type; drives the extra-point / two-point result derivation. |
| `pointAfterAttempt.value` | double | Points ESPN credits for the point-after attempt (1.0 made extra point, 2.0 made two-point try). |
| `drive.id` | character | ESPN's `id` field for the drive containing this play. |
| `drive.displayResult` | character | ESPN's `displayResult` field for the drive containing this play. |
| `drive.isScore` | logical | ESPN's `isScore` field for the drive containing this play. |
| `drive.team.shortDisplayName` | character | ESPN's `team.shortDisplayName` field for the drive containing this play. |
| `drive.team.displayName` | character | ESPN's `team.displayName` field for the drive containing this play. |
| `drive.team.name` | character | ESPN's `team.name` field for the drive containing this play. |
| `drive.team.abbreviation` | character | ESPN's `team.abbreviation` field for the drive containing this play. |
| `drive.yards` | integer | ESPN's `yards` field for the drive containing this play. |
| `drive.offensivePlays` | integer | ESPN's `offensivePlays` field for the drive containing this play. |
| `drive.result` | character | ESPN's `result` field for the drive containing this play. |
| `drive.description` | character | ESPN's `description` field for the drive containing this play. |
| `drive.shortDisplayResult` | character | ESPN's `shortDisplayResult` field for the drive containing this play. |
| `drive.timeElapsed.displayValue` | character | ESPN's `timeElapsed.displayValue` field for the drive containing this play. |
| `drive.start.period.number` | integer | ESPN's `start.period.number` field for the drive containing this play. |
| `drive.start.period.type` | character | ESPN's `start.period.type` field for the drive containing this play. |
| `drive.start.yardLine` | integer | ESPN's `start.yardLine` field for the drive containing this play. |
| `drive.start.clock.displayValue` | character | ESPN's `start.clock.displayValue` field for the drive containing this play. |
| `drive.start.text` | character | ESPN's `start.text` field for the drive containing this play. |
| `drive.end.period.number` | integer | ESPN's `end.period.number` field for the drive containing this play. |
| `drive.end.period.type` | character | ESPN's `end.period.type` field for the drive containing this play. |
| `drive.end.yardLine` | integer | ESPN's `end.yardLine` field for the drive containing this play. |
| `drive.end.clock.displayValue` | character | ESPN's `end.clock.displayValue` field for the drive containing this play. |
| `seasonType` | integer | ESPN season type for the game (2 = regular season, 3 = postseason). |
| `week` | integer | Game week of the season. |
| `status_type_completed` | logical |  |
| `homeTeamId` | integer | ESPN's home-team Id for the game, stamped on every play. |
| `awayTeamId` | integer | ESPN's away-team Id for the game, stamped on every play. |
| `homeFinalScore` | integer | Final score of the home team from the ESPN game header, repeated on every play of the game; the processing step checks the running score at the last play against it. |
| `awayFinalScore` | integer | Final score of the away team from the ESPN game header, repeated on every play of the game; the processing step checks the running score at the last play against it. |
| `homeTeamName` | character | ESPN's home-team Name for the game, stamped on every play. |
| `awayTeamName` | character | ESPN's away-team Name for the game, stamped on every play. |
| `homeTeamMascot` | character | ESPN's home-team Mascot for the game, stamped on every play. |
| `awayTeamMascot` | character | ESPN's away-team Mascot for the game, stamped on every play. |
| `homeTeamAbbrev` | character | ESPN's home-team Abbrev for the game, stamped on every play. |
| `awayTeamAbbrev` | character | ESPN's away-team Abbrev for the game, stamped on every play. |
| `homeTeamNameAlt` | character | ESPN's home-team NameAlt for the game, stamped on every play. |
| `awayTeamNameAlt` | character | ESPN's away-team NameAlt for the game, stamped on every play. |
| `gameSpread` | double | Point spread used as an input to the win-probability model. |
| `homeFavorite` | logical | True when the home team was favoured by the spread. |
| `gameSpreadAvailable` | logical | True when a spread was available for the game. |
| `overUnder` | double | Over/under total used as a model input. |
| `homeTeamSpread` | double | ESPN's home-team Spread for the game, stamped on every play. |
| `clock.minutes` | integer | Minutes remaining on the game clock at the play. |
| `clock.seconds` | integer | Seconds component of the game clock at the play. |
| `lag_half` | integer | Value of half on the previous play, used for sequence-aware derivations. |
| `lead_half` | integer | Value of half on the next play, used for sequence-aware derivations. |
| `start.TimeSecsRem` | integer | Seconds remaining in the half from ESPN's clock stamp for this play, which is the end-of-play time in 2005 and 2007+ (the snap time in 2004 and most of 2006); tops out at 1800. |
| `start.adj_TimeSecsRem` | integer | ESPN's `adj_TimeSecsRem` value for the play state at the start of the play. |
| `lead_text` | character | Value of text on the next play, used for sequence-aware derivations. |
| `lead_start_team` | character | Value of start_team on the next play, used for sequence-aware derivations. |
| `lead_start_yardsToEndzone` | integer | Value of start_yardsToEndzone on the next play, used for sequence-aware derivations. |
| `lead_start_down` | integer | Value of start_down on the next play, used for sequence-aware derivations. |
| `lead_start_distance` | integer | Value of start_distance on the next play, used for sequence-aware derivations. |
| `lead_scoringPlay` | logical | Value of scoringPlay on the next play, used for sequence-aware derivations. |
| `text_dupe` | logical | Always False in the emitted frame -- the duplicate-row filter it gates runs before the column is returned, so it marks nothing and is retained only for schema stability. |
| `end_state_missing` | logical | Flag that ESPN's end-of-play state (end.team.id) was absent and the end state was imputed. |
| `start.pos_team.id` | integer | ESPN's `pos_team.id` value for the play state at the start of the play. |
| `start.def_pos_team.id` | integer | ESPN's `def_pos_team.id` value for the play state at the start of the play. |
| `end.def_pos_team.id` | integer | ESPN's `def_pos_team.id` value for the play state at the end of the play. |
| `end.pos_team.id` | integer | ESPN's `pos_team.id` value for the play state at the end of the play. |
| `start.pos_team.name` | character | ESPN's `pos_team.name` value for the play state at the start of the play. |
| `start.def_pos_team.name` | character | ESPN's `def_pos_team.name` value for the play state at the start of the play. |
| `end.pos_team.name` | character | ESPN's `pos_team.name` value for the play state at the end of the play. |
| `end.def_pos_team.name` | character | ESPN's `def_pos_team.name` value for the play state at the end of the play. |
| `start.is_home` | logical | ESPN's `is_home` value for the play state at the start of the play. |
| `end.is_home` | logical | ESPN's `is_home` value for the play state at the end of the play. |
| `homeTimeoutCalled` | logical | True when the home team called a timeout on the play. |
| `awayTimeoutCalled` | logical | True when the away team called a timeout on the play. |
| `end.homeTeamTimeouts` | integer | ESPN's `homeTeamTimeouts` value for the play state at the end of the play. |
| `end.awayTeamTimeouts` | integer | ESPN's `awayTeamTimeouts` value for the play state at the end of the play. |
| `start.homeTeamTimeouts` | integer | ESPN's `homeTeamTimeouts` value for the play state at the start of the play. |
| `start.awayTeamTimeouts` | integer | ESPN's `awayTeamTimeouts` value for the play state at the start of the play. |
| `end.TimeSecsRem` | integer | Seconds remaining in the half carried as this play's end state; currently the preceding row's clock stamp. |
| `end.adj_TimeSecsRem` | integer | ESPN's `adj_TimeSecsRem` value for the play state at the end of the play. |
| `start.posTeamTimeouts` | integer | ESPN's `posTeamTimeouts` value for the play state at the start of the play. |
| `start.defPosTeamTimeouts` | integer | ESPN's `defPosTeamTimeouts` value for the play state at the start of the play. |
| `end.posTeamTimeouts` | integer | ESPN's `posTeamTimeouts` value for the play state at the end of the play. |
| `end.defPosTeamTimeouts` | integer | ESPN's `defPosTeamTimeouts` value for the play state at the end of the play. |
| `firstHalfKickoffTeamId` | integer | ESPN id of the team that received the opening kickoff. |
| `start.yard` | integer | ESPN's `yard` value for the play state at the start of the play. |
| `end.yard` | integer | ESPN's `yard` value for the play state at the end of the play. |
| `lag_scoringPlay` | logical | Value of scoringPlay on the previous play, used for sequence-aware derivations. |
| `down_1` | logical | True when it is 1st down at the start of the play. |
| `down_2` | logical | True when it is 2nd down at the start of the play. |
| `down_3` | logical | True when it is 3rd down at the start of the play. |
| `down_4` | logical | True when it is 4th down at the start of the play. |
| `down_1_end` | logical | True when it is 1st down at the end of the play. |
| `down_2_end` | logical | True when it is 2nd down at the end of the play. |
| `down_3_end` | logical | True when it is 3rd down at the end of the play. |
| `down_4_end` | logical | True when it is 4th down at the end of the play. |
| `td_check` | logical | Internal flag used while reconciling whether the play produced a touchdown. |
| `forced_fumble` | logical | True when the defense forced a fumble on the play. |
| `is_home` | logical |  |
| `lag_HA_score_diff` | integer | Value of HA_score_diff on the previous play, used for sequence-aware derivations. |
| `HA_score_diff` | integer | Home score minus away score for the play. |
| `net_HA_score_pts` | integer | Net points the play added to the home-minus-away score margin. |
| `H_score_diff` | integer | Home team's score minus the away team's, from the home perspective. |
| `A_score_diff` | integer | Away team's score minus the home team's, from the away perspective. |
| `lag_homeScore` | integer | Value of homeScore on the previous play, used for sequence-aware derivations. |
| `lag_awayScore` | integer | Value of awayScore on the previous play, used for sequence-aware derivations. |
| `start.homeScore` | integer | ESPN's `homeScore` value for the play state at the start of the play. |
| `start.awayScore` | integer | ESPN's `awayScore` value for the play state at the start of the play. |
| `end.homeScore` | integer | ESPN's `homeScore` value for the play state at the end of the play. |
| `end.awayScore` | integer | ESPN's `awayScore` value for the play state at the end of the play. |
| `start.pos_team_score` | integer | ESPN's `pos_team_score` value for the play state at the start of the play. |
| `start.def_pos_team_score` | integer | ESPN's `def_pos_team_score` value for the play state at the start of the play. |
| `start.pos_score_diff` | integer | ESPN's `pos_score_diff` value for the play state at the start of the play. |
| `end.pos_team_score` | integer | ESPN's `pos_team_score` value for the play state at the end of the play. |
| `end.def_pos_team_score` | integer | ESPN's `def_pos_team_score` value for the play state at the end of the play. |
| `end.pos_score_diff` | integer | ESPN's `pos_score_diff` value for the play state at the end of the play. |
| `start.pos_team_receives_2H_kickoff` | logical | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the start of the play. |
| `end.pos_team_receives_2H_kickoff` | logical | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the end of the play. |
| `penalty_in_text` | logical | True when the play description mentions a penalty. |
| `penalty_count` | integer | Number of penalties flagged on the play (0-4 observed). |
| `penalty_declined_count` | integer | Number of the flagged penalties that were declined. |
| `penalty_all_declined` | logical | Whether every penalty flagged on the play was declined. |
| `penalty_enforcement` | character | How the penalty was resolved: one of no_play, declined, offsetting, negating_foul, play_stands, unknown. |
| `penalty_negated_play` | logical | Whether the penalty negated the play's result. |
| `pass_breakup` | logical | True when a defender broke up the pass. |
| `pass_depth` | character | Thrown-pass depth parsed from ESPN play text ("short" or "deep"); null when the text omits it (sacks, screens, pre-2025 text). |
| `pass_direction` | character | Pass direction parsed from ESPN play text ("left", "middle", or "right"); null when the text omits it. |
| `rush_direction` | character | Rush direction parsed from ESPN play text ("left", "middle", or "right"); null when the text omits it. |
| `qb_hurry` | logical | Whether ESPN's play text says the quarterback was hurried into the throw ("hurried by ..."). |
| `fg_attempt` | logical | True when the play was a field-goal attempt. |
| `pos_unit` | character | Possession-team unit label (offense or special teams). |
| `def_pos_unit` | character | Defensive possession-team unit label (defense or special teams). |
| `sp` | logical |  |
| `play` | logical | Binary flag indicating the row is a counted play (excludes end markers/timeouts/penalties). |
| `cleaned_text` | character | Play description with overturned-call prefixes stripped; the text the name and team extractors run against. |
| `kneel_down` | logical | Whether the play is an offensive kneel, from explicit kneel text plus an end-of-half TEAM-rush heuristic. |
| `scrimmage_play` | logical | True when the play is a play from scrimmage rather than a special-teams or administrative row. |
| `pos_score_diff_end` | integer | Score differential from the possessing team's perspective at the end of the play. |
| `fumble_lost` | logical |  |
| `fumble_recovered` | logical | True when a fumble on the play was recovered. |
| `field_goal_result` | character |  |
| `extra_point_result` | character |  |
| `two_point_conv_result` | character | String result of the two-point conversion attempt: success, failure, or safety (touchback in the defensive end zone). |
| `defensive_two_point_attempt` | logical |  |
| `defensive_two_point_conv` | logical |  |
| `yds_punted_source` | character | Provenance of yds_punted: "text" when the value was present before the special-teams derivation step (parsed from the play text, or set by a flag convention such as a blocked punt's 0), "derived" when that step filled it from field position, null when there is no value. |
| `yds_kickoff_source` | character | Provenance of yds_kickoff: "text" when the value was present before the special-teams derivation step (parsed from the play text, or set by a flag convention such as a blocked punt's 0), "derived" when that step filled it from field position, null when there is no value. |
| `yds_punt_return_source` | character | Provenance of yds_punt_return: "text" when the value was present before the special-teams derivation step (parsed from the play text, or set by a flag convention such as a blocked punt's 0), "derived" when that step filled it from field position, null when there is no value. |
| `air_yardsToEndzone` | integer | Yards to the endzone at the catch spot, parsed from the 2025+ vendor catch-spot text; null before 2025 or when unresolvable. |
| `air_yards` | integer |  |
| `yards_after_catch` | integer |  |
| `kickoff_return_player_name` | character | Name of the player returning the kickoff, when the play was returned. |
| `punt_return_player_name` | character | Name of the player returning the punt, when the punt was returned. |
| `xp_attempt` | logical | Whether an extra-point kick was attempted on the play. |
| `xp_made` | logical | Whether the extra-point kick was successful. |
| `xp_kicker_player_name` | character | Name of the kicker attempting the extra point. |
| `kicking_team` | integer | Team id of the kicking team on kickoff, punt, and field-goal plays. |
| `return_team` | integer | Team id of the returning side; set on interception, fumble, kickoff, punt, and blocked-kick returns. |
| `fumble_or_muff` | logical | Whether the play includes a fumble or a muffed kick or punt (widened beyond ESPN's fumble play types). |
| `recovery_team` | integer | Team id parsed from the play text as recovering the fumble or muff. |
| `recovery_team_2` | integer | Team id of the second recovery in a multi-recovery scramble, parsed from the play text. |
| `penalty_spot_yardline` | integer | Yard line (0-50) at which the penalty was spotted. |
| `penalty_spot_side` | character | Side of the field the penalty was spotted on: 'home', 'away' or 'mid' (midfield). |
| `penalty_spot_yardsToEndzone` | integer | Yards from the penalty spot to the end zone (0-100). |
| `fumbling_team` | integer | Team id of the side that fumbled or muffed the ball, parsed from the play text. |
| `int_turnover` | logical | Whether the play is an interception giveaway. |
| `pos_fumble_lost` | logical | Whether the possession team fumbled and lost the ball. |
| `def_fumble_lost` | logical | Whether the defending team (e.g. a returner after a takeaway) fumbled and lost the ball back. |
| `is_pos_team_turnover` | logical | Whether the possession team committed a giveaway (interception or fumble lost). |
| `is_def_pos_team_turnover` | logical | Whether the defending team gave the ball back via a lost fumble. |
| `is_turnover` | logical | True when the play is a giveaway-based turnover (interception thrown or fumble lost); blocked kicks recovered by the defense are carried by the blocked-kick fields instead. |
| `turnover_team` | integer | Team id charged with the giveaway on the play. |
| `is_st_turnover` | logical | Whether the giveaway happened on a special-teams play (kick or punt snap, or a return). |
| `is_blocked_punt_turnover` | logical | Blocked-punt possession loss (blocked-punt TD, or the defense recovered); kept out of is_turnover to match ESPN's giveaway-only box. |
| `is_blocked_fg_turnover` | logical | Blocked-field-goal possession loss (blocked-FG TD, or the defense recovered); kept out of is_turnover to match ESPN's giveaway-only box. |
| `sack_team` | integer | Team id credited with the sack (the defense). |
| `interception_team` | integer | Team id credited with the interception (the defense). |
| `pass_breakup_team` | integer | Team id credited with the pass breakup (the defense). |
| `forced_fumble_team` | integer | Team id credited with forcing the fumble -- the side opposite the fumbling player (the covering team on returns). |
| `fumble_recovery_team` | integer | Team id that recovered the fumble or muff, from parsed text with a giveaway / own-recovery fallback. |
| `punt_return_team` | integer | Team id of the punt-returning side. |
| `kick_return_team` | integer | Team id of the kick-returning side. |
| `fg_team` | integer | Team id attempting the field goal (the kicking team). |
| `punt_team` | integer | Team id punting the ball (the kicking team). |
| `penalized_team` | integer | Team id the penalty was assessed against, from the home/away text resolver with a foul-direction fallback. |
| `penalty_yards_signed` | integer | Penalty yardage parsed from the play text with era-aware bounds; the printed sign is retained but is not a reliable enforcement direction. |
| `penalty_side` | character | Which side committed the penalty -- 'off' (offense) or 'def' (defense). |
| `penalty_yards_net` | integer | Net yardage assessed for the penalty, signed relative to the possession team (observed -25 to 25). |
| `penalty_team_id` | integer | Team id of the side that committed the penalty. |
| `new_down` | integer | Down after the play, including any penalty enforcement. |
| `new_distance` | integer | Distance to go after the play, including any penalty enforcement. |
| `under_2` | logical | Whether the play began with two minutes or less remaining in the half. |
| `goal_to_go` | logical |  |
| `stopped_run` | logical | True when the rush was stopped at or behind the line of scrimmage. |
| `opportunity_run` | logical | True when a rush reached 4 yards -- the carries on which the blocking did its job. Matches cfbfastR's espn_cfb_15 definition. Assets published before the 2026-08 fix carry the inverted (4 yards or fewer) flag. |
| `highlight_run` | logical | True when the rush gained 8 or more yards. |
| `adj_rush_yardage` | integer | Rushing yards capped at 8, the input to the line-yards decomposition. |
| `line_yards` | double | Yards credited to the offensive line on a rush, using the standard sliding scale: 1.2x the capped yardage on a loss, all of it through 3 yards, half of each yard from 4 to 8, and a 5.5-yard ceiling beyond that. |
| `second_level_yards` | double | Rushing yards earned from 4 to 8, split evenly between line and carrier under the line-yards decomposition. |
| `open_field_yards` | integer | Rushing yards gained beyond 8, credited to the ball carrier rather than the line. |
| `highlight_yards` | double | Second-level plus open-field yards -- the yardage credited to the carrier. |
| `opp_highlight_yards` | double | Highlight yards earned on opportunity runs, isolating carrier production on carries where the blocking succeeded. Assets published before the 2026-08 fix are identically 0 here, because the inverted opportunity_run gate could never co-occur with non-zero highlight yards. |
| `short_rush_success` | logical | True when a short-yardage rush gained the yardage needed. |
| `short_rush_attempt` | logical | True when the play is a rush in a short-yardage situation. |
| `early_down` | logical | True when the play is a scrimmage play on first or second down. |
| `late_down` | logical | True when the play is a scrimmage play on third or fourth down. |
| `power_rush_attempt` | logical | True when the play is a short-yardage power rushing attempt. |
| `power_rush_success` | logical | True when a power rushing attempt gained the yardage needed. |
| `early_down_pass` | logical | True when the play is a pass on an early down. |
| `early_down_rush` | logical | True when the play is a rush on an early down. |
| `late_down_pass` | logical | True when the play is a pass on a late down. |
| `late_down_rush` | logical | True when the play is a rush on a late down. |
| `standard_down` | logical | True when the offense is on schedule for the series -- first down, second down needing fewer than 8, or third/fourth down needing fewer than 5. |
| `passing_down` | logical | True when the offense is behind schedule for the series -- second down needing 8 or more, or third/fourth down needing 5 or more. |
| `TFL` | logical | True when the play was a tackle for loss. |
| `TFL_pass` | logical | True when the play was a tackle for loss on a pass play (a sack). |
| `TFL_rush` | logical | True when the play was a tackle for loss on a rush play. |
| `havoc` | logical | True when the defense disrupted the play: a pass breakup, tackle for loss, interception or forced fumble. |
| `first_down_yards` | logical | Whether the play gained enough yardage to earn a first down. |
| `first_down_penalty` | logical |  |
| `first_down_earned` | logical | Whether the play earned a first down by means other than yardage (e.g. by penalty). |
| `start.pos_team_spread` | double | ESPN's `pos_team_spread` value for the play state at the start of the play. |
| `start.elapsed_share` | double | ESPN's `elapsed_share` value for the play state at the start of the play. |
| `start.spread_time` | double | ESPN's `spread_time` value for the play state at the start of the play. |
| `end.pos_team_spread` | double | ESPN's `pos_team_spread` value for the play state at the end of the play. |
| `end.elapsed_share` | double | ESPN's `elapsed_share` value for the play state at the end of the play. |
| `end.spread_time` | double | ESPN's `spread_time` value for the play state at the end of the play. |
| `penalty_assessed_on_kickoff` | logical | Whether a penalty was assessed on a kickoff; such plays take the kickoff/touchback win-probability handling. |
| `start.yardsToEndzone.touchback` | integer | ESPN's `yardsToEndzone.touchback` value for the play state at the start of the play. |
| `EP_start_touchback` | double | Expected points the offense would have had from a touchback on this play. |
| `EP_start` | double | Expected points for the offense at the start of the play. |
| `EP_end` | double | Expected points for the offense at the end of the play. |
| `EP_penalty_cf` | double | Counterfactual expected points for the penalty branch -- the EP had the alternative penalty outcome been taken (null unless a penalty decision existed). |
| `penalty_cf_yardsToEndzone` | integer | Yards to the end zone in the counterfactual penalty branch. |
| `lag_EP_end` | double | Value of EP_end on the previous play, used for sequence-aware derivations. |
| `EP_between` | double | Change in expected points across the play, before penalty adjustment. |
| `EPA_scrimmage` | double | EPA credited to the play on plays from scrimmage. |
| `EPA_rush` | double | EPA credited to the play on rush plays. |
| `EPA_pass` | double | EPA credited to the play on pass plays. |
| `EPA_explosive` | logical | True when the play was explosive. |
| `EPA_non_explosive` | double | EPA credited to the play on non-explosive plays. |
| `EPA_explosive_pass` | logical | True when the pass play was explosive. |
| `EPA_explosive_rush` | logical | True when the rush play was explosive. |
| `first_down_created` | logical | True when the play produced a first down for the offense. |
| `EPA_success` | logical | True when the play was successful by EPA. |
| `EPA_success_early_down` | logical | True when the play on an early down was successful by EPA. |
| `EPA_success_early_down_pass` | logical | True when the pass play on an early down was successful by EPA. |
| `EPA_success_early_down_rush` | logical | True when the rush play on an early down was successful by EPA. |
| `EPA_success_late_down` | logical | True when the play on a late down was successful by EPA. |
| `EPA_success_late_down_pass` | logical | True when the pass play on a late down was successful by EPA. |
| `EPA_success_late_down_rush` | logical | True when the rush play on a late down was successful by EPA. |
| `EPA_success_standard_down` | logical | True when the play on a standard down was successful by EPA. |
| `EPA_success_passing_down` | logical | True when the play on a passing down was successful by EPA. |
| `EPA_success_pass` | logical | True when the pass play was successful by EPA. |
| `EPA_success_rush` | logical | True when the rush play was successful by EPA. |
| `EPA_success_EPA` | double | EPA on successful plays. |
| `EPA_success_standard_down_EPA` | double | EPA on successful plays on a standard down. |
| `EPA_success_passing_down_EPA` | double | EPA on successful plays on a passing down. |
| `EPA_success_pass_EPA` | double | EPA on successful pass plays. |
| `EPA_success_rush_EPA` | double | EPA on successful rush plays. |
| `EPA_middle_8_success` | logical | True when the play in the middle eight was successful by EPA. |
| `EPA_middle_8_success_pass` | logical | True when the pass play in the middle eight was successful by EPA. |
| `EPA_middle_8_success_rush` | logical | True when the rush play in the middle eight was successful by EPA. |
| `EPA_penalty` | double | EPA credited to the play attributable to penalties. |
| `EPA_penalty_direct` | double | EPA attributable directly to the penalty on the play, separated from the EPA of the play itself (observed -11.7 to 8.05; null when no penalty applied). |
| `EPA_sp` | double | EPA credited to the play on special-teams plays. |
| `EPA_fg` | double | EPA credited to the play on field-goal attempts. |
| `EPA_punt` | double | EPA credited to the play on punt plays. |
| `EPA_kickoff` | double | EPA credited to the play on kickoff plays. |
| `start.ExpScoreDiff_touchback` | double | ESPN's `ExpScoreDiff_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff` | double | ESPN's `ExpScoreDiff` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio_touchback` | double | ESPN's `ExpScoreDiff_Time_Ratio_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio` | double | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the start of the play. |
| `end.ExpScoreDiff` | double | ESPN's `ExpScoreDiff` value for the play state at the end of the play. |
| `end.ExpScoreDiff_Time_Ratio` | double | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the end of the play. |
| `wp_touchback` | double | Win probability the offense would have had starting from a touchback. |
| `wp_before_naive` | double | Pre-snap possession-team win probability from the spread-free (naive) WP model. |
| `wp_touchback_naive` | double | Naive-model win probability for the kickoff-touchback substitute state, used as the pre-snap WP on kickoffs. |
| `wp_after_naive` | double | End-of-play possession-team win probability from the naive model, after the game-logic adjustment chain. |
| `def_wp_before_naive` | double | Pre-snap defense win probability under the naive model (1 - wp_before_naive). |
| `home_wp_before_naive` | double | Pre-snap naive win probability mapped to the home team. |
| `away_wp_before_naive` | double | Pre-snap naive win probability mapped to the away team. |
| `lead_wp_before_naive` | double | Next play's pre-snap naive win probability, used in the end-of-half and change-of-possession adjustments. |
| `lead_wp_before2_naive` | double | Pre-snap naive win probability two plays ahead, used where the immediately following row is a non-play. |
| `def_wp_after_naive` | double | End-of-play defense win probability under the naive model. |
| `home_wp_after_naive` | double | End-of-play naive win probability mapped to the home team. |
| `away_wp_after_naive` | double | End-of-play naive win probability mapped to the away team. |
| `wpa_naive` | double | Win probability added on the play under the spread-free (naive) model. |
| `cp` | double |  |
| `cp_game_state` | double | Completion probability from the 8-feature game-state booster, scored on every pass play regardless of which model produced cp. On one scale across seasons, so use it (not cp) for anything summed or averaged; null on non-pass plays. |
| `cp_model` | character | Which completion-probability booster scored cp on the play: "air_yards" (the 11-feature model, used where ESPN's play text gives a catch/target spot -- essentially 2025 onward) or "game_state" (the 8-feature model used everywhere else). The two are not on one scale, so group any cpoe aggregate by this column; null on non-pass plays. |
| `cpoe` | double |  |
| `era` | integer |  |
| `xpass` | double |  |
| `pass_oe` | double |  |
| `drive_start` | double | Yard line at which the drive began. |
| `drive_stopped` | logical | True when the play ended the drive. |
| `drive_play_index` | integer | Sequence number of the play within its drive. |
| `drive_offense_plays` | integer | Offensive plays run on the drive. |
| `prog_drive_EPA` | double | Cumulative EPA accrued by the drive up to and including this play. |
| `prog_drive_WPA` | double | Cumulative win-probability added by the drive up to and including this play. |
| `drive_offense_yards` | integer | Offensive yards gained on the drive. |
| `drive_total_yards` | integer | Total yards gained on the drive. |
| `qbr_epa` | double | EPA variant used as an input to the QBR calculation. |
| `weight` | double | Listed weight (lbs). |
| `non_fumble_sack` | logical | True when the play was a sack that did not produce a fumble. |
| `sack_epa` | double | EPA credited to the play when it is a sack. |
| `pass_epa` | double | EPA credited to the play when it is a pass. |
| `rush_epa` | double | EPA credited to the play when it is a rush. |
| `pen_epa` | double | EPA attributable to a penalty on the play. |
| `sack_weight` | double | Weighting applied to the sack component of the play. |
| `pass_weight` | double | Weighting applied to the pass component of the play. |
| `rush_weight` | double | Weighting applied to the rush component of the play. |
| `pen_weight` | double | Weighting applied to the penalty component of the play. |
| `action_play` | logical | True when the play advanced the game state -- excludes timeouts, end-of-period markers and other non-action rows. |
| `athlete_name` | character | Player full name. |
| `rusher_player_id` | integer |  |
| `passer_player_id` | integer |  |
| `receiver_player_id` | integer |  |
| `fumble_player_id` | integer | CFBD athlete_id of the player who fumbled. |
| `sack_player_id` | integer | Comma-separated CFBD athlete_id(s) of the sacking defender(s). |
| `sack_player_id2` | integer | ESPN athlete id of the second sacker on a split sack (regex fallback for an ESPN sidecar blind spot). |
| `interception_player_id` | integer | CFBD athlete_id of the defender credited with an interception. |
| `pass_breakup_player_id` | integer | CFBD athlete_id of the defender credited with the pass breakup (PBU). |
| `fumble_forced_player_id` | integer | CFBD athlete_id of the defender credited with forcing the fumble. |
| `fumble_recovered_player_id` | integer | CFBD athlete_id of the player recovering the fumble. |
| `fg_kicker_player_id` | integer | ESPN athlete id of the field-goal kicker. |
| `punter_player_id` | integer |  |
| `kickoff_player_id` | integer | ESPN athlete id of the player kicking off. |
| `kickoff_return_player_id` | integer | ESPN athlete id of the kickoff returner. |
| `punt_return_player_id` | integer | ESPN athlete id of the punt returner. |
| `fg_block_player_id` | integer | ESPN athlete id of the player who blocked the field goal. |
| `punt_block_player_id` | character | ESPN athlete id of the player who blocked the punt. |
| `fg_return_player_id` | character | ESPN athlete id of the player who returned the blocked or missed field goal. |
| `punt_block_return_player_id` | character | ESPN athlete id of the player who returned the blocked punt. |
| `go_wp` | double | Win probability from going for it on fourth down: conversion-probability-weighted mean of the success and failure states (cfb4th port). |
| `first_down_prob` | double | Modeled probability of converting the fourth down when going for it. |
| `wp_succeed` | double | Mean win probability across yardage outcomes given the fourth-down attempt converts. |
| `wp_fail` | double | Mean win probability given the fourth-down attempt fails. |
| `make_fg_wp` | double | Win probability given the field-goal attempt is made. |
| `miss_fg_wp` | double | Win probability given the field-goal attempt misses. |
| `fg_wp` | double | Make-probability-weighted win probability of attempting the field goal. |
| `punt_wp` | double | Win probability of punting, from the bundled punt-outcome distribution. |
| `go_boost` | double | cfb4th's headline number: 100 * (go_wp - max(fg_wp, punt_wp)), in percentage points. |
| `go_wp_diff` | double | go_wp minus the recommended option's WP (0 when going for it is the recommendation, otherwise <= 0). |
| `fg_wp_diff` | double | fg_wp minus the recommended option's WP (0 when the field goal is the recommendation, otherwise <= 0). |
| `punt_wp_diff` | double | punt_wp minus the recommended option's WP (0 when punting is the recommendation, otherwise <= 0). |
| `fourth_down_recommendation` | character | Max-WP fourth-down choice among "go", "punt", and "field_goal". |
| `two_pt_wp` | double | Win probability of going for two: conversion-probability-weighted mean of the 2-point and 0-point outcomes (cfb4th port). |
| `xp_wp` | double | Win probability of kicking the extra point, weighting the make by the empirical CFB extra-point make rate. |
| `prob_2pt` | double | Two-point conversion probability from the bundled CFB two-point model. |
| `two_pt_recommendation` | character | Point-after recommendation: "go_for_2" when two_pt_wp exceeds xp_wp, otherwise "kick_xp". |
| `two_pt_wp_diff` | double | two_pt_wp minus xp_wp; positive favors going for two. |

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
