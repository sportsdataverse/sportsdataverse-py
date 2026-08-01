---
title: CFB dataset loaders
sidebar_label: Loaders
sidebar_position: 1
---
# CFB dataset loaders

```mermaid
flowchart LR
  raw["scrape / raw"] --> enrich["enrich"] --> rel["release asset"] --> load["load_*()"]
```

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_cfb_pbp` | [espn_cfb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_pbp) | — |
| `load_cfb_ratings` | [cfb_ratings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_ratings) | — |
| `load_cfb_recruiting_proj` | [cfb_recruiting_proj](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_recruiting_proj) | — |
| `load_cfb_rosters` | [cfbfastR-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfbfastR-data) | — |
| `load_cfb_schedule` | [cfbfastR-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfbfastR-data) | — |
| `load_cfb_team_info` | [cfbfastR-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfbfastR-data) | — |
| `load_cfb_teams_crosswalk` | [cfb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_crosswalk) | — |
| `load_cfb_schedule_crosswalk` | [cfb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_crosswalk) | — |
| `load_cfb_team_box` | [espn_cfb_team_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_team_box) | — |
| `load_cfb_player_box` | [espn_cfb_player_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_player_box) | — |
| `load_cfb_drives` | [espn_cfb_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_drives) | — |
| `load_cfb_play_participants` | [espn_cfb_play_participants](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_play_participants) | — |
| `load_cfb_game_rosters` | [espn_cfb_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_game_rosters) | — |
| `load_cfb_linescores` | [espn_cfb_linescores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_linescores) | — |
| `load_cfb_betting` | [espn_cfb_betting](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_betting) | — |
| `load_cfb_power_index` | [espn_cfb_power_index](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_power_index) | — |
| `load_cfb_adv_team` | [espn_cfb_adv_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_team) | — |
| `load_cfb_adv_passing` | [espn_cfb_adv_passing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_passing) | — |
| `load_cfb_adv_rushing` | [espn_cfb_adv_rushing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_rushing) | — |
| `load_cfb_adv_receiving` | [espn_cfb_adv_receiving](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_receiving) | — |
| `load_cfb_adv_defensive` | [espn_cfb_adv_defensive](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_defensive) | — |
| `load_cfb_adv_defensive_players` | [espn_cfb_adv_defensive_players](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_defensive_players) | — |
| `load_cfb_adv_drives` | [espn_cfb_adv_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_drives) | — |
| `load_cfb_adv_situational` | [espn_cfb_adv_situational](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_situational) | — |
| `load_cfb_adv_specialists` | [espn_cfb_adv_specialists](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_specialists) | — |
| `load_cfb_adv_turnover` | [espn_cfb_adv_turnover](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_turnover) | — |
| `load_cfb_model_pbp` | [espn_cfb_model_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_model_pbp) | — |
| `load_cfb_passing` | [espn_cfb_passing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_passing) | — |
| `load_cfb_percentiles` | [espn_cfb_percentiles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_percentiles) | — |
| `load_cfb_receiving` | [espn_cfb_receiving](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_receiving) | — |
| `load_cfb_rushing` | [espn_cfb_rushing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_rushing) | — |
| `load_cfb_team_summaries` | [espn_cfb_team_summaries](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_team_summaries) | — |
| `load_cfb_adv_team_gamelog` | [espn_cfb_adv_team_gamelog](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_team_gamelog) | — |
| `load_cfb_ratings_weekly` | [cfb_ratings_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_ratings_weekly) | — |
| `load_cfb_team_summaries_weekly` | [cfb_team_summaries_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_summaries_weekly) | — |

## `load_cfb_pbp`

Release: [espn_cfb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_pbp/play_by_play_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `id` | Int32 | 247Sports referencing id for the recruit. |
| `sequenceNumber` | String | Broadcast sequence order number. |
| `text` | String | Full play description. |
| `awayScore` | Int32 | Away team score after the goal. |
| `homeScore` | Int32 | Home team score after the goal. |
| `scoringPlay` | Boolean | ESPN flag marking the play as a scoring play. |
| `priority` | Boolean | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | String | ISO timestamp the play record was last modified. |
| `statYardage` | Int32 | Yardage ESPN credits to the play for statistical purposes. |
| `type.id` | String | ESPN's numeric identifier for the play type. |
| `type.text` | String | ESPN's text label for the play type. |
| `period.number` | Int32 | Period (quarter) number in which the play occurred. |
| `clock.displayValue` | String | Game clock at the play, as the displayed mm:ss string. |
| `start.down` | Int32 | ESPN's `down` value for the play state at the start of the play. |
| `start.distance` | Int32 | ESPN's `distance` value for the play state at the start of the play. |
| `start.yardLine` | Int32 | ESPN's `yardLine` value for the play state at the start of the play. |
| `start.yardsToEndzone` | Int32 | ESPN's `yardsToEndzone` value for the play state at the start of the play. |
| `start.downDistanceText` | String | ESPN's `downDistanceText` value for the play state at the start of the play. |
| `start.shortDownDistanceText` | String | ESPN's `shortDownDistanceText` value for the play state at the start of the play. |
| `start.possessionText` | String | ESPN's `possessionText` value for the play state at the start of the play. |
| `start.team.id` | Int32 | ESPN's `team.id` value for the play state at the start of the play. |
| `end.down` | Int32 | ESPN's `down` value for the play state at the end of the play. |
| `end.distance` | Int32 | ESPN's `distance` value for the play state at the end of the play. |
| `end.yardLine` | Int32 | ESPN's `yardLine` value for the play state at the end of the play. |
| `end.yardsToEndzone` | Int32 | ESPN's `yardsToEndzone` value for the play state at the end of the play. |
| `end.downDistanceText` | String | ESPN's `downDistanceText` value for the play state at the end of the play. |
| `end.shortDownDistanceText` | String | ESPN's `shortDownDistanceText` value for the play state at the end of the play. |
| `end.possessionText` | String | ESPN's `possessionText` value for the play state at the end of the play. |
| `end.team.id` | Int32 | ESPN's `team.id` value for the play state at the end of the play. |
| `drive.id` | String | ESPN's `id` field for the drive containing this play. |
| `drive.displayResult` | String | ESPN's `displayResult` field for the drive containing this play. |
| `drive.isScore` | Boolean | ESPN's `isScore` field for the drive containing this play. |
| `drive.team.shortDisplayName` | String | ESPN's `team.shortDisplayName` field for the drive containing this play. |
| `drive.team.displayName` | String | ESPN's `team.displayName` field for the drive containing this play. |
| `drive.team.name` | String | ESPN's `team.name` field for the drive containing this play. |
| `drive.team.abbreviation` | String | ESPN's `team.abbreviation` field for the drive containing this play. |
| `drive.yards` | Int32 | ESPN's `yards` field for the drive containing this play. |
| `drive.offensivePlays` | Int32 | ESPN's `offensivePlays` field for the drive containing this play. |
| `drive.result` | String | ESPN's `result` field for the drive containing this play. |
| `drive.description` | String | ESPN's `description` field for the drive containing this play. |
| `drive.shortDisplayResult` | String | ESPN's `shortDisplayResult` field for the drive containing this play. |
| `drive.timeElapsed.displayValue` | String | ESPN's `timeElapsed.displayValue` field for the drive containing this play. |
| `drive.start.period.number` | Int32 | ESPN's `start.period.number` field for the drive containing this play. |
| `drive.start.period.type` | String | ESPN's `start.period.type` field for the drive containing this play. |
| `drive.start.yardLine` | Int32 | ESPN's `start.yardLine` field for the drive containing this play. |
| `drive.start.clock.displayValue` | String | ESPN's `start.clock.displayValue` field for the drive containing this play. |
| `drive.start.text` | String | ESPN's `start.text` field for the drive containing this play. |
| `drive.end.period.number` | Int32 | ESPN's `end.period.number` field for the drive containing this play. |
| `drive.end.period.type` | String | ESPN's `end.period.type` field for the drive containing this play. |
| `drive.end.yardLine` | Int32 | ESPN's `end.yardLine` field for the drive containing this play. |
| `drive.end.clock.displayValue` | String | ESPN's `end.clock.displayValue` field for the drive containing this play. |
| `game_id` | Int32 | ESPN game identifier. |
| `season` | Int32 | Season (4-digit year). |
| `seasonType` | Int32 | ESPN season type for the game (2 = regular season, 3 = postseason). |
| `homeTeamId` | Int32 | ESPN's home-team Id for the game, stamped on every play. |
| `awayTeamId` | Int32 | ESPN's away-team Id for the game, stamped on every play. |
| `homeTeamName` | String | ESPN's home-team Name for the game, stamped on every play. |
| `awayTeamName` | String | ESPN's away-team Name for the game, stamped on every play. |
| `homeTeamMascot` | String | ESPN's home-team Mascot for the game, stamped on every play. |
| `awayTeamMascot` | String | ESPN's away-team Mascot for the game, stamped on every play. |
| `homeTeamAbbrev` | String | ESPN's home-team Abbrev for the game, stamped on every play. |
| `awayTeamAbbrev` | String | ESPN's away-team Abbrev for the game, stamped on every play. |
| `homeTeamNameAlt` | String | ESPN's home-team NameAlt for the game, stamped on every play. |
| `awayTeamNameAlt` | String | ESPN's away-team NameAlt for the game, stamped on every play. |
| `homeTeamSpread` | Float64 | ESPN's home-team Spread for the game, stamped on every play. |
| `gameSpread` | Float64 | Point spread used as an input to the win-probability model. |
| `gameSpreadAvailable` | Boolean | True when a spread was available for the game. |
| `overUnder` | Float64 | Over/under total used as a model input. |
| `homeFavorite` | Boolean | True when the home team was favoured by the spread. |
| `clock.minutes` | String | Minutes remaining on the game clock at the play. |
| `clock.seconds` | String | Seconds component of the game clock at the play. |
| `half` | Int32 | Half indicator (1 or 2). |
| `lead_half` | Int32 | Value of half on the next play, used for sequence-aware derivations. |
| `start.TimeSecsRem` | Int32 | ESPN's `TimeSecsRem` value for the play state at the start of the play. |
| `start.adj_TimeSecsRem` | Int32 | ESPN's `adj_TimeSecsRem` value for the play state at the start of the play. |
| `lead_text` | String | Value of text on the next play, used for sequence-aware derivations. |
| `lead_start_team` | String | Value of start_team on the next play, used for sequence-aware derivations. |
| `lead_start_yardsToEndzone` | Int32 | Value of start_yardsToEndzone on the next play, used for sequence-aware derivations. |
| `lead_start_down` | Int32 | Value of start_down on the next play, used for sequence-aware derivations. |
| `lead_start_distance` | Int32 | Value of start_distance on the next play, used for sequence-aware derivations. |
| `lead_scoringPlay` | Boolean | Value of scoringPlay on the next play, used for sequence-aware derivations. |
| `text_dupe` | Boolean | True when the play description duplicates the previous row's text. |
| `game_play_number` | Int32 | Sequential play number within the game (excludes timeouts/end markers). |
| `start.pos_team.id` | Int32 | ESPN's `pos_team.id` value for the play state at the start of the play. |
| `start.def_pos_team.id` | Int32 | ESPN's `def_pos_team.id` value for the play state at the start of the play. |
| `end.def_team.id` | Int32 | ESPN's `def_team.id` value for the play state at the end of the play. |
| `end.pos_team.id` | Int32 | ESPN's `pos_team.id` value for the play state at the end of the play. |
| `end.def_pos_team.id` | Int32 | ESPN's `def_pos_team.id` value for the play state at the end of the play. |
| `start.pos_team.name` | String | ESPN's `pos_team.name` value for the play state at the start of the play. |
| `start.def_pos_team.name` | String | ESPN's `def_pos_team.name` value for the play state at the start of the play. |
| `end.pos_team.name` | String | ESPN's `pos_team.name` value for the play state at the end of the play. |
| `end.def_pos_team.name` | String | ESPN's `def_pos_team.name` value for the play state at the end of the play. |
| `start.is_home` | Boolean | ESPN's `is_home` value for the play state at the start of the play. |
| `end.is_home` | Boolean | ESPN's `is_home` value for the play state at the end of the play. |
| `homeTimeoutCalled` | Boolean | True when the home team called a timeout on the play. |
| `awayTimeoutCalled` | Boolean | True when the away team called a timeout on the play. |
| `end.homeTeamTimeouts` | Int32 | ESPN's `homeTeamTimeouts` value for the play state at the end of the play. |
| `end.awayTeamTimeouts` | Int32 | ESPN's `awayTeamTimeouts` value for the play state at the end of the play. |
| `start.homeTeamTimeouts` | Int32 | ESPN's `homeTeamTimeouts` value for the play state at the start of the play. |
| `start.awayTeamTimeouts` | Int32 | ESPN's `awayTeamTimeouts` value for the play state at the start of the play. |
| `end.TimeSecsRem` | Int32 | ESPN's `TimeSecsRem` value for the play state at the end of the play. |
| `end.adj_TimeSecsRem` | Int32 | ESPN's `adj_TimeSecsRem` value for the play state at the end of the play. |
| `start.posTeamTimeouts` | Int32 | ESPN's `posTeamTimeouts` value for the play state at the start of the play. |
| `start.defPosTeamTimeouts` | Int32 | ESPN's `defPosTeamTimeouts` value for the play state at the start of the play. |
| `end.posTeamTimeouts` | Int32 | ESPN's `posTeamTimeouts` value for the play state at the end of the play. |
| `end.defPosTeamTimeouts` | Int32 | ESPN's `defPosTeamTimeouts` value for the play state at the end of the play. |
| `firstHalfKickoffTeamId` | Int32 | ESPN id of the team that received the opening kickoff. |
| `period` | Int32 | Period (quarter) number. |
| `start.yard` | Int32 | ESPN's `yard` value for the play state at the start of the play. |
| `end.yard` | Int32 | ESPN's `yard` value for the play state at the end of the play. |
| `playType` | String | ESPN's play-type label for the play. |
| `week` | Int32 | Game week of the season. |
| `end_of_half` | Boolean | Binary flag for the last play of a half. |
| `down_1` | Boolean | True when it is 1st down at the start of the play. |
| `down_2` | Boolean | True when it is 2nd down at the start of the play. |
| `down_3` | Boolean | True when it is 3rd down at the start of the play. |
| `down_4` | Boolean | True when it is 4th down at the start of the play. |
| `down_1_end` | Boolean | True when it is 1st down at the end of the play. |
| `down_2_end` | Boolean | True when it is 2nd down at the end of the play. |
| `down_3_end` | Boolean | True when it is 3rd down at the end of the play. |
| `down_4_end` | Boolean | True when it is 4th down at the end of the play. |
| `scoring_play` | Boolean | `TRUE` if the play resulted in a score. |
| `td_play` | Boolean | Binary flag for a touchdown play. |
| `touchdown` | Boolean | Binary flag for a touchdown (duplicate of td_play for downstream use). |
| `td_check` | Boolean | Internal flag used while reconciling whether the play produced a touchdown. |
| `safety` | Boolean | Binary flag for a safety. |
| `fumble_vec` | Boolean | Binary flag for a play involving a fumble. |
| `forced_fumble` | Boolean | True when the defense forced a fumble on the play. |
| `kickoff_play` | Boolean | Binary flag for a kickoff play. |
| `kickoff_tb` | Boolean | Binary flag for a kickoff touchback. |
| `kickoff_onside` | Boolean | Binary flag for an onside kickoff attempt. |
| `kickoff_oob` | Boolean | Binary flag for a kickoff out of bounds. |
| `kickoff_fair_catch` | Boolean | Binary flag for a kickoff fair catch. |
| `kickoff_downed` | Boolean | Binary flag for a kickoff downed in the field of play. |
| `kick_play` | Boolean | Binary flag for any kicking play (kickoff or field goal). |
| `kickoff_safety` | Boolean | Binary flag for a kickoff safety. |
| `punt` | Boolean | Binary flag for a punt play. |
| `punt_play` | Boolean | Binary flag for any punt-related play (includes blocks/returns). |
| `punt_tb` | Boolean | Binary flag for a punt touchback. |
| `punt_oob` | Boolean | Binary flag for a punt out of bounds. |
| `punt_fair_catch` | Boolean | Binary flag for a punt fair catch. |
| `punt_downed` | Boolean | Binary flag for a punt downed in the field of play. |
| `punt_safety` | Boolean | Binary flag for a punt safety. |
| `penalty_safety` | Boolean | Binary flag for a safety scored on a penalty. |
| `punt_blocked` | Boolean | Binary flag for a blocked punt. |
| `rush` | Boolean | Binary flag for a rushing play. |
| `pass` | Boolean | Binary flag for a passing play (includes sacks). |
| `sack_vec` | Boolean | Binary flag for a sack play. |
| `pos_team` | Int32 | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team` | Int32 | Team name on defense at the start of the play. |
| `is_home` | Boolean | Whether the subject team was the home team. |
| `HA_score_diff` | Int32 | Home score minus away score for the play. |
| `lag_homeScore` | Int32 | Value of homeScore on the previous play, used for sequence-aware derivations. |
| `lag_awayScore` | Int32 | Value of awayScore on the previous play, used for sequence-aware derivations. |
| `start.homeScore` | Int32 | ESPN's `homeScore` value for the play state at the start of the play. |
| `start.awayScore` | Int32 | ESPN's `awayScore` value for the play state at the start of the play. |
| `end.homeScore` | Int32 | ESPN's `homeScore` value for the play state at the end of the play. |
| `end.awayScore` | Int32 | ESPN's `awayScore` value for the play state at the end of the play. |
| `pos_team_score` | Int32 | Score for the team in possession at the start of the play. |
| `def_pos_team_score` | Int32 | Score for the defensive team at the start of the play. |
| `start.pos_team_score` | Int32 | ESPN's `pos_team_score` value for the play state at the start of the play. |
| `start.def_pos_team_score` | Int32 | ESPN's `def_pos_team_score` value for the play state at the start of the play. |
| `start.pos_score_diff` | Int32 | ESPN's `pos_score_diff` value for the play state at the start of the play. |
| `end.pos_team_score` | Int32 | ESPN's `pos_team_score` value for the play state at the end of the play. |
| `end.def_pos_team_score` | Int32 | ESPN's `def_pos_team_score` value for the play state at the end of the play. |
| `end.pos_score_diff` | Int32 | ESPN's `pos_score_diff` value for the play state at the end of the play. |
| `lag_pos_team` | Int32 | Value of pos_team on the previous play, used for sequence-aware derivations. |
| `lead_pos_team` | Int32 | Value of pos_team on the next play, used for sequence-aware derivations. |
| `lead_pos_team2` | Int32 | Value of pos_team on the next 2 plays play, used for sequence-aware derivations. |
| `pos_score_diff` | Int32 | Score differential from the possession team's perspective. |
| `lag_pos_score_diff` | Int32 | Value of pos_score_diff on the previous play, used for sequence-aware derivations. |
| `pos_score_pts` | Int32 | Points scored on the play attributed to the possession team. |
| `pos_score_diff_start` | Int32 | Score differential for the possession team at the start of the play. |
| `start.pos_team_receives_2H_kickoff` | Boolean | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the start of the play. |
| `end.pos_team_receives_2H_kickoff` | Boolean | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the end of the play. |
| `change_of_poss` | Int32 | Binary flag for change of possession on the play (CFBD offense field). |
| `penalty_flag` | Boolean | TRUE when a penalty was flagged on the play. |
| `penalty_declined` | Boolean | TRUE when the penalty was declined. |
| `penalty_no_play` | Boolean | TRUE when the penalty nullified the play (no play counted). |
| `penalty_offset` | Boolean | TRUE when offsetting penalties were called. |
| `penalty_1st_conv` | Boolean | TRUE when the penalty resulted in a first down conversion. |
| `penalty_in_text` | Boolean | True when the play description mentions a penalty. |
| `sack` | Boolean | Binary flag for a sack (duplicate of sack_vec for downstream use). |
| `int` | Boolean | Binary flag for an interception. |
| `int_td` | Boolean | Binary flag for an interception returned for a touchdown. |
| `completion` | Boolean | Binary flag for a completed pass. |
| `pass_attempt` | Boolean | Binary flag for a pass attempt. |
| `target` | Boolean | Binary flag for a targeted receiver on the play. |
| `pass_breakup` | Boolean | True when a defender broke up the pass. |
| `pass_td` | Boolean | Binary flag for a passing touchdown. |
| `rush_td` | Boolean | Binary flag for a rushing touchdown. |
| `turnover_vec` | Boolean | Binary flag for any play classified as a turnover. |
| `offense_score_play` | Boolean | Binary flag for an offensive scoring play. |
| `defense_score_play` | Boolean | Binary flag for a defensive scoring play. |
| `downs_turnover` | Boolean | Binary flag for a turnover on downs. |
| `fg_attempt` | Boolean | True when the play was a field-goal attempt. |
| `fg_made` | Boolean | TRUE when the field goal attempt was successful. |
| `pos_unit` | String | Possession-team unit label (offense or special teams). |
| `def_pos_unit` | String | Defensive possession-team unit label (defense or special teams). |
| `lead_play_type` | String | Value of play_type on the next play, used for sequence-aware derivations. |
| `sp` | Boolean | Binary indicator for whether or not a score occurred on the play. |
| `play` | Boolean | Binary flag indicating the row is a counted play (excludes end markers/timeouts/penalties). |
| `scrimmage_play` | Boolean | True when the play is a play from scrimmage rather than a special-teams or administrative row. |
| `change_of_pos_team` | Boolean | Binary flag for change of possession-team on the play. |
| `pos_score_diff_end` | Int32 | Score differential from the possessing team's perspective at the end of the play. |
| `fumble_lost` | Boolean | Binary indicator for if the fumble was lost. |
| `fumble_recovered` | Boolean | True when a fumble on the play was recovered. |
| `receiver_player_name` | String | Name of the receiver on a passing play. |
| `passer_player_name` | String | Name of the passer on a passing play. |
| `new_down` | Int32 | Down after the play, including any penalty enforcement. |
| `new_distance` | Int32 | Distance to go after the play, including any penalty enforcement. |
| `middle_8` | Boolean | TRUE for plays in the middle-8 window (final 4 min of 1H, first 4 min of 2H). |
| `rz_play` | Boolean | Binary flag for a red-zone play (yards_to_goal <= 20). |
| `scoring_opp` | Boolean | Binary flag for a scoring opportunity (yards_to_goal <= 40). |
| `stuffed_run` | Boolean | Binary flag for a stuffed run (zero or negative yards gained). |
| `stopped_run` | Boolean | True when the rush was stopped at or behind the line of scrimmage. |
| `opportunity_run` | Boolean | True when a rush reached 4 yards -- the carries on which the blocking did its job. Matches cfbfastR's espn_cfb_15 definition. Assets published before the 2026-08 fix carry the inverted (4 yards or fewer) flag. |
| `highlight_run` | Boolean | True when the rush gained 8 or more yards. |
| `short_rush_success` | Boolean | True when a short-yardage rush gained the yardage needed. |
| `short_rush_attempt` | Boolean | True when the play is a rush in a short-yardage situation. |
| `power_rush_success` | Boolean | True when a power rushing attempt gained the yardage needed. |
| `power_rush_attempt` | Boolean | True when the play is a short-yardage power rushing attempt. |
| `early_down` | Boolean | True when the play is a scrimmage play on first or second down. |
| `late_down` | Boolean | True when the play is a scrimmage play on third or fourth down. |
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
| `start.pos_team_spread` | Float64 | ESPN's `pos_team_spread` value for the play state at the start of the play. |
| `start.elapsed_share` | Float64 | ESPN's `elapsed_share` value for the play state at the start of the play. |
| `start.spread_time` | Float64 | ESPN's `spread_time` value for the play state at the start of the play. |
| `end.pos_team_spread` | Float64 | ESPN's `pos_team_spread` value for the play state at the end of the play. |
| `end.elapsed_share` | Float64 | ESPN's `elapsed_share` value for the play state at the end of the play. |
| `end.spread_time` | Float64 | ESPN's `spread_time` value for the play state at the end of the play. |
| `start.yardsToEndzone.touchback` | Int32 | ESPN's `yardsToEndzone.touchback` value for the play state at the start of the play. |
| `EP_start_touchback` | Float64 | Expected points the offense would have had from a touchback on this play. |
| `EP_start` | Float64 | Expected points for the offense at the start of the play. |
| `EP_end` | Float64 | Expected points for the offense at the end of the play. |
| `lag_change_of_pos_team` | Boolean | Value of change_of_pos_team on the previous play, used for sequence-aware derivations. |
| `EPA` | Float64 | Expected Points Added on the play (cfbfastR EPA model output). |
| `def_EPA` | Float64 | EPA for the defensive team on the play (sign-flipped offense EPA). |
| `EPA_scrimmage` | Float64 | EPA credited to the play on plays from scrimmage. |
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
| `EPA_success_rush_EPA` | Boolean | EPA on successful rush plays. |
| `EPA_middle_8_success` | Boolean | True when the play in the middle eight was successful by EPA. |
| `EPA_middle_8_success_pass` | Boolean | True when the pass play in the middle eight was successful by EPA. |
| `EPA_middle_8_success_rush` | Boolean | True when the rush play in the middle eight was successful by EPA. |
| `EPA_sp` | Float64 | EPA credited to the play on special-teams plays. |
| `start.ExpScoreDiff_touchback` | Float64 | ESPN's `ExpScoreDiff_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff` | Float64 | ESPN's `ExpScoreDiff` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio_touchback` | Float64 | ESPN's `ExpScoreDiff_Time_Ratio_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio` | Float64 | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the start of the play. |
| `end.ExpScoreDiff` | Float64 | ESPN's `ExpScoreDiff` value for the play state at the end of the play. |
| `end.ExpScoreDiff_Time_Ratio` | Float64 | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the end of the play. |
| `wp_before` | Float64 | Win probability for the possession team before the play (0-1). |
| `wp_touchback` | Float64 | Win probability the offense would have had starting from a touchback. |
| `def_wp_before` | Float64 | Win probability for the defensive team before the play (0-1). |
| `home_wp_before` | Float64 | Home team win probability before the play (0-1). |
| `away_wp_before` | Float64 | Away team win probability before the play (0-1). |
| `lead_wp_before` | Float64 | Value of wp_before on the next play, used for sequence-aware derivations. |
| `lead_wp_before2` | Float64 | Value of wp_before on the next 2 plays play, used for sequence-aware derivations. |
| `wp_after` | Float64 | Win probability for the possession team after the play (0-1). |
| `def_wp_after` | Float64 | Win probability for the defensive team after the play (0-1). |
| `home_wp_after` | Float64 | Home team win probability after the play (0-1). |
| `away_wp_after` | Float64 | Away team win probability after the play (0-1). |
| `wpa` | Float64 | Win Probability Added on the play (cfbfastR WP model output). |
| `drive_start` | Int32 | Yard line at which the drive began. |
| `drive_stopped` | Boolean | True when the play ended the drive. |
| `drive_play_index` | Int32 | Sequence number of the play within its drive. |
| `drive_offense_plays` | Int32 | Offensive plays run on the drive. |
| `prog_drive_EPA` | Float64 | Cumulative EPA accrued by the drive up to and including this play. |
| `prog_drive_WPA` | Float64 | Cumulative win-probability added by the drive up to and including this play. |
| `drive_offense_yards` | Int32 | Offensive yards gained on the drive. |
| `drive_total_yards` | Int32 | Total yards gained on the drive. |
| `qbr_epa` | Float64 | EPA variant used as an input to the QBR calculation. |
| `weight` | Float64 | Listed weight (lbs). |
| `non_fumble_sack` | Boolean | True when the play was a sack that did not produce a fumble. |
| `pass_epa` | Float64 | EPA credited to the play when it is a pass. |
| `pass_weight` | Float64 | Weighting applied to the pass component of the play. |
| `action_play` | Boolean | True when the play advanced the game state -- excludes timeouts, end-of-period markers and other non-action rows. |
| `athlete_name` | String | Player full name. |
| `type.abbreviation` | String | ESPN's abbreviation for the play type. |
| `lag_half` | String | Value of half on the previous play, used for sequence-aware derivations. |
| `lag_scoringPlay` | Boolean | Value of scoringPlay on the previous play, used for sequence-aware derivations. |
| `lag_HA_score_diff` | Int32 | Value of HA_score_diff on the previous play, used for sequence-aware derivations. |
| `net_HA_score_pts` | Int32 | Net points the play added to the home-minus-away score margin. |
| `H_score_diff` | Int32 | Home team's score minus the away team's, from the home perspective. |
| `A_score_diff` | Int32 | Away team's score minus the home team's, from the away perspective. |
| `yds_rushed` | Int32 | Rushing yards gained on the play. |
| `rusher_player_name` | String | Name of the rusher on a rushing play. |
| `adj_rush_yardage` | Int32 | Rushing yards capped at 8, the input to the line-yards decomposition. |
| `line_yards` | Float64 | Yards credited to the offensive line on a rush, using the standard sliding scale: 1.2x the capped yardage on a loss, all of it through 3 yards, half of each yard from 4 to 8, and a 5.5-yard ceiling beyond that. |
| `second_level_yards` | Float64 | Rushing yards earned from 4 to 8, split evenly between line and carrier under the line-yards decomposition. |
| `open_field_yards` | Int32 | Rushing yards gained beyond 8, credited to the ball carrier rather than the line. |
| `highlight_yards` | Float64 | Second-level plus open-field yards -- the yardage credited to the carrier. |
| `opp_highlight_yards` | Float64 | Highlight yards earned on opportunity runs, isolating carrier production on carries where the blocking succeeded. Assets published before the 2026-08 fix are identically 0 here, because the inverted opportunity_run gate could never co-occur with non-zero highlight yards. |
| `lag_EP_end` | Float64 | Value of EP_end on the previous play, used for sequence-aware derivations. |
| `EP_between` | Float64 | Change in expected points across the play, before penalty adjustment. |
| `EPA_rush` | Float64 | EPA credited to the play on rush plays. |
| `EPA_success_EPA` | Float64 | EPA on successful plays. |
| `EPA_success_passing_down_EPA` | Float64 | EPA on successful plays on a passing down. |
| `rush_epa` | Float64 | EPA credited to the play when it is a rush. |
| `rush_weight` | Float64 | Weighting applied to the rush component of the play. |
| `penalty_detail` | String | Parsed penalty description extracted from play text. |
| `penalty_text` | String | TRUE when penalty information is detectable in the play text. |
| `yds_penalty` | Int32 | Yardage assessed on the penalty. |
| `EPA_penalty` | Float64 | EPA credited to the play attributable to penalties. |
| `pen_epa` | Float64 | EPA attributable to a penalty on the play. |
| `pen_weight` | Float64 | Weighting applied to the penalty component of the play. |
| `yds_punt_gained` | Int32 | Net yards gained on the punt (punt distance minus return). |
| `yds_punt_return` | Int32 | Yards gained on the punt return. |
| `punter_player_name` | String | Name of the punter. |
| `punt_return_player_name` | String | Name of the player returning the punt. |
| `EPA_punt` | Float64 | EPA credited to the play on punt plays. |
| `EPA_success_standard_down_EPA` | Float64 | EPA on successful plays on a standard down. |
| `yds_receiving` | Int32 | Receiving yards gained on the play. |
| `EPA_success_pass_EPA` | Float64 | EPA on successful pass plays. |
| `interception_player_name` | String | Name of the defender credited with the interception. |
| `scoringType.name` | String | ESPN's name for the scoring type (e.g. touchdown, field goal). |
| `scoringType.displayName` | String | ESPN's display label for the scoring type. |
| `scoringType.abbreviation` | String | ESPN's abbreviation for the scoring type. |
| `yds_kickoff_return` | Int32 | Yards gained on the kickoff return. |
| `kickoff_player_name` | String | Name of the kickoff specialist. |
| `kickoff_return_player_name` | String | Name of the player returning the kickoff. |
| `down` | Int32 | Down of the play (1-4). |
| `distance` | Int32 | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `EPA_kickoff` | Float64 | EPA credited to the play on kickoff plays. |
| `yds_fg` | Int32 | Distance of the field goal attempt in yards. |
| `EPA_fg` | Float64 | EPA credited to the play on field-goal attempts. |
| `fumble_player_name` | String | Name of the player who fumbled. |
| `fumble_recovered_player_name` | String | Name of the player who recovered the fumble. |
| `yds_sacked` | Int32 | Yards lost on the sack. |
| `sack_epa` | Float64 | EPA credited to the play when it is a sack. |
| `sack_weight` | Float64 | Weighting applied to the sack component of the play. |
| `date` | String | Date of the poll release. |
| `fg_kicker_player_name` | String | Name of the field goal kicker. |
| `yds_int_return` | Int32 | Yards gained on an interception return. |
| `yds_punted` | Int32 | Yards the ball traveled on the punt. |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `game_date` | Date | Kickoff date-time (ISO 8601, UTC). |

```python
load_cfb_pbp(seasons=2024)
```

## `load_cfb_ratings`

Release: [cfb_ratings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_ratings) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_ratings/cfb_ratings_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | Int64 | ESPN team id. |
| `adj_off_epa` | Float64 |  |
| `adj_def_epa` | Float64 |  |
| `adj_st_epa` | Float64 |  |
| `adj_net` | Float64 |  |
| `fei_off` | Float64 |  |
| `fei_def` | Float64 |  |
| `fei_net` | Float64 |  |
| `games` | Int64 | Number of games included in the ATS summary. |
| `off_pace` | Float64 |  |
| `off_rank` | Int64 |  |
| `def_rank` | Int64 |  |
| `net_rank` | Int64 |  |
| `net_z` | Float64 |  |

```python
load_cfb_ratings(seasons=2024)
```

## `load_cfb_recruiting_proj`

Release: [cfb_recruiting_proj](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_recruiting_proj) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_recruiting_proj/cfb_recruiting_proj_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | Int64 | ESPN team id. |
| `pred_wins` | Float64 |  |
| `pred_margin` | Float64 |  |
| `pred_net_epa` | Float64 |  |

```python
load_cfb_recruiting_proj(seasons=2024)
```

## `load_cfb_rosters`

Release: [cfbfastR-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfbfastR-data) · asset `https://raw.githubusercontent.com/sportsdataverse/cfbfastR-data/main/rosters/parquet/cfb_rosters_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `athlete_id` | String | ESPN athlete id. |
| `first_name` | String | Athlete first name. |
| `last_name` | String | Athlete last name. |
| `team` | String | Team name. |
| `weight` | Int32 | Listed weight (lbs). |
| `height` | Int32 | Listed height (inches). |
| `jersey` | Int32 | Jersey number. |
| `year` | Int32 | Four-digit season year (e.g. 2019). |
| `position` | String | Athlete position. |
| `home_city` | String | Hometown of the athlete. |
| `home_state` | String | Hometown state of the athlete. |
| `home_country` | String | Hometown country of the athlete. |
| `home_latitude` | String | Hometown latitude. |
| `home_longitude` | String | Hometown longitude. |
| `home_county_fips` | String | Hometown FIPS code. |
| `recruit_ids` | List(Int32) |  |
| `headshot_url` | String | Player ESPN headshot url. |
| `season` | Int32 | Season (4-digit year). |

```python
load_cfb_rosters(seasons=2024)
```

## `load_cfb_schedule`

Release: [cfbfastR-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfbfastR-data) · asset `https://raw.githubusercontent.com/sportsdataverse/cfbfastR-data/main/schedules/parquet/cfb_schedules_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | ESPN game identifier. |
| `season` | Int32 | Season (4-digit year). |
| `week` | Int32 | Game week of the season. |
| `season_type` | String | ESPN season type (2 = regular, 3 = postseason). |
| `start_date` | String | Season start timestamp (ISO 8601, UTC). |
| `start_time_tbd` | Boolean | TRUE/FALSE flag for if the game's start time is to be determined. |
| `completed` | Boolean | `TRUE` if the game is complete. |
| `neutral_site` | Boolean | TRUE/FALSE flag for if the game took place at a neutral site. |
| `conference_game` | Boolean | TRUE/FALSE flag for this game qualifying as a conference game. |
| `attendance` | Int32 | Reported attendance at the game. |
| `venue_id` | Int32 | Referencing venue id. |
| `venue` | String | Venue name. |
| `home_id` | Int32 | Home team referencing id. |
| `home_team` | String | Home team name. |
| `home_conference` | String | Home team conference. |
| `home_division` | String | Home team division. |
| `home_points` | Int32 | Home team total points scored in the game so far. |
| `home_post_win_prob` | Boolean | Home team post-game win probability. |
| `home_pregame_elo` | Int32 | Home team pre-game ELO rating. |
| `home_postgame_elo` | Int32 | Home team post-game ELO rating. |
| `away_id` | Int32 | Away team referencing id. |
| `away_team` | String | Away team name. |
| `away_conference` | String | Away team conference. |
| `away_division` | String | Away team division. |
| `away_points` | Int32 | Away team total points scored in the game so far. |
| `away_post_win_prob` | Boolean | Away team post-game win probability. |
| `away_pregame_elo` | Int32 | Away team pre-game ELO rating. |
| `away_postgame_elo` | Int32 | Away team post-game ELO rating. |
| `excitement_index` | Boolean | Game excitement index. |
| `highlights` | Boolean | Game highlight urls. |
| `notes` | String | Game notes. |

```python
load_cfb_schedule(seasons=2024)
```

## `load_cfb_team_info`

Release: [cfbfastR-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfbfastR-data) · asset `https://raw.githubusercontent.com/sportsdataverse/cfbfastR-data/main/team_info/parquet/cfb_team_info_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `school` | String | Team name. |
| `mascot` | String | Team mascot. |
| `abbreviation` | String | Metric abbreviation. |
| `alt_name1` | String | Team alternate name 1 (as it appears in `play_text`). |
| `alt_name2` | String | Team alternate name 2 (as it appears in `play_text`). |
| `alt_name3` | String | Team alternate name 3 (as it appears in `play_text`). |
| `conference` | String | Conference of the team. |
| `classification` | String | Conference classification (fbs, fcs, ii, iii). |
| `color` | String | Primary team color (hex, no `#`). |
| `alt_color` | String | Team color (alternate). |
| `logo` | String | Team or league logo URL. |
| `logo_2` | String |  |
| `twitter` | String |  |
| `venue_id` | Int32 | Referencing venue id. |
| `venue_name` | String | Full name of the franchise's venue. |
| `city` | String | Venue city. |
| `state` | String | Venue state. |
| `zip` | String | Team/venue zip code. |
| `country_code` | String | Team/venue country code. |
| `timezone` | String | Time zone in which the venue resides (i.e. Eastern Time -> "America/New_York"). |
| `latitude` | Float64 | Venue latitude in decimal degrees. |
| `longitude` | Float64 | Venue longitude in decimal degrees. |
| `elevation` | String | Venue elevation above sea level. |
| `capacity` | Int32 | Stadium capacity. |
| `year_constructed` | Int32 | Year in which the venue was constructed. |
| `grass` | Boolean | TRUE/FALSE response on whether the field is grass or not. |
| `dome` | Boolean | TRUE/FALSE response to whether the venue has a dome or not. |

```python
load_cfb_team_info(seasons=2024)
```

## `load_cfb_teams_crosswalk`

Release: [cfb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_crosswalk/cfb_teams_crosswalk_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `norm_key` | String |  |
| `espn_team_id` | Int64 | ESPN team id for the crosswalk row. |
| `espn_team` | String |  |
| `espn_abbreviation` | String | ESPN abbreviation. |
| `fox_team_id` | String | Fox Sports team id for the same team. |
| `fox_team` | String |  |
| `fox_abbreviation` | String |  |
| `yahoo_team_id` | String | Yahoo Sports team id for the same team. |
| `yahoo_team` | String |  |
| `yahoo_abbreviation` | String |  |
| `matched_sources` | String |  |

```python
load_cfb_teams_crosswalk(seasons=2024)
```

## `load_cfb_schedule_crosswalk`

Release: [cfb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_crosswalk/cfb_schedule_crosswalk_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `matchup_key` | String |  |
| `espn_game_id` | Int64 | ESPN game id for the crosswalk row. |
| `fox_game_id` | String | Fox Sports game id for the same game. |
| `yahoo_game_id` | String | Yahoo Sports game id for the same game. |
| `yahoo_global_game_id` | String |  |
| `home_team` | String | Home team name. |
| `away_team` | String | Away team name. |
| `espn_date` | String |  |
| `fox_date` | String |  |
| `yahoo_date` | String |  |
| `matched_sources` | String |  |

```python
load_cfb_schedule_crosswalk(seasons=2024)
```

## `load_cfb_team_box`

Release: [espn_cfb_team_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_team_box) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_team_box/team_box_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `firstDowns` | String |  |
| `thirdDownEff` | String |  |
| `fourthDownEff` | String |  |
| `totalYards` | String |  |
| `netPassingYards` | String |  |
| `completionAttempts` | String |  |
| `yardsPerPass` | String |  |
| `rushingYards` | String | Net rushing yards gained. |
| `rushingAttempts` | String | Rushing attempts. |
| `yardsPerRushAttempt` | String | Yards gained per rushing attempt. |
| `totalPenaltiesYards` | String |  |
| `turnovers` | String | Turnovers total. |
| `fumblesLost` | String |  |
| `interceptions` | String | Passing interceptions. |
| `possessionTime` | String |  |
| `team_id` | Int64 | ESPN team id. |
| `team_abbreviation` | String | Team abbreviation; `team_detail = TRUE` only. |
| `team_name` | String | Team nickname; `team_detail = TRUE` only. |
| `home_away` | String | `home` or `away`. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |

```python
load_cfb_team_box(seasons=2024)
```

## `load_cfb_player_box`

Release: [espn_cfb_player_box](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_player_box) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_player_box/player_box_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `completions/passingAttempts` | String | Completions and pass attempts, as ESPN's combined string. |
| `passingYards` | String | Net passing yards gained. |
| `yardsPerPassAttempt` | String | Yards gained per pass attempt. |
| `passingTouchdowns` | String | Passing touchdowns. |
| `interceptions` | String | Passing interceptions. |
| `adjQBR` | String | Adjusted Total QBR for the quarterback. |
| `category` | String | CFBD stats category name (e.g. passing, rushing, defensive). |
| `athlete_id` | Int64 | ESPN athlete id. |
| `athlete_name` | String | Player full name. |
| `jersey` | Null | Jersey number. |
| `team_id` | Int64 | ESPN team id. |
| `rushingAttempts` | String | Rushing attempts. |
| `rushingYards` | String | Net rushing yards gained. |
| `yardsPerRushAttempt` | String | Yards gained per rushing attempt. |
| `rushingTouchdowns` | String | Rushing touchdowns. |
| `longRushing` | String | Longest rush of the game, in yards. |
| `receptions` | String | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receivingYards` | String | Receiving yards gained. |
| `yardsPerReception` | String | Yards gained per reception. |
| `receivingTouchdowns` | String | Receiving touchdowns. |
| `longReception` | String | Longest reception of the game, in yards. |
| `interceptionYards` | String | Yards returned on interceptions. |
| `interceptionTouchdowns` | String | Touchdowns scored on interception returns. |
| `puntReturns` | String | Punt returns attempted. |
| `puntReturnYards` | String | Yards gained on punt returns. |
| `yardsPerPuntReturn` | String | Yards gained per punt return. |
| `longPuntReturn` | String | Longest punt return of the game, in yards. |
| `puntReturnTouchdowns` | String | Touchdowns scored on punt returns. |
| `fieldGoalsMade/fieldGoalAttempts` | String | Field goals made and attempted, as ESPN's combined string. |
| `fieldGoalPct` | String | Field-goal percentage. |
| `longFieldGoalMade` | String | Longest field goal made, in yards. |
| `extraPointsMade/extraPointAttempts` | String | Extra points made and attempted, as ESPN's combined string. |
| `totalKickingPoints` | String | Total points scored by kicking. |
| `punts` | String | Punts attempted. |
| `puntYards` | String | Total punt yards. |
| `grossAvgPuntYards` | String | Gross average yards per punt, before return yardage. |
| `touchbacks` | String | Punts or kickoffs that resulted in a touchback. |
| `puntsInside20` | String | Punts downed inside the opponent 20-yard line. |
| `longPunt` | String | Longest punt of the game, in yards. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `stat_1` | String |  |
| `stat_2` | String |  |
| `stat_3` | String |  |
| `stat_4` | String |  |
| `stat_5` | String |  |

```python
load_cfb_player_box(seasons=2024)
```

## `load_cfb_drives`

Release: [espn_cfb_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_drives) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_drives/drives_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `drive_id` | String | CFBD drive identifier the play belongs to. |
| `team_id` | Int64 | ESPN team id. |
| `result` | String | Drive result code (e.g. `PUNT`, `TD`). |
| `display_result` | String | Drive-result label (e.g. `Punt`, `Touchdown`). |
| `short_display_result` | String | Short drive-result label. |
| `description` | String | ESPN's description of the stat. |
| `yards` | Int64 | Total yards gained on the drive. |
| `offensive_plays` | Int64 | Number of offensive plays on the drive. |
| `is_score` | Boolean | `TRUE` if the drive resulted in a score. |
| `start_period` | Int64 | Period (quarter) in which the drive starts. |
| `start_yard_line` | Int64 | Yard line at the start of the play. |
| `start_clock` | String | Game clock display value at the start of the drive. |
| `start_text` | String | Field-position text at the start of the drive. |
| `end_period` | Int64 | Period (quarter) in which the drive ends. |
| `end_yard_line` | Int64 | Yard line at the end of the play. |
| `end_clock` | String | Game clock display value at the end of the drive. |
| `time_elapsed` | String | Elapsed game time for the drive (`MM:SS`). |
| `n_plays` | Int64 |  |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |

```python
load_cfb_drives(seasons=2024)
```

## `load_cfb_play_participants`

Release: [espn_cfb_play_participants](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_play_participants) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_play_participants/play_participants_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game identifier. |
| `play_id` | Int64 | ESPN play id. |
| `kicker_player_name` | String | Display name of the kicker -- the FIRST participant in that role on the play. |
| `returner_player_name` | String | Display name of the player returning the kick or punt -- the FIRST participant in that role on the play. |
| `passer_player_name` | String | Display name of the passer -- the FIRST participant in that role on the play. |
| `receiver_player_name` | String | Display name of the targeted receiver -- the FIRST participant in that role on the play. |
| `rusher_player_name` | String | Display name of the ball carrier on a rush -- the FIRST participant in that role on the play. |
| `penalized_player_name` | String | Display name of the penalized player -- the FIRST participant in that role on the play. |
| `scorer_player_name` | String | Display name of the player credited with the score -- the FIRST participant in that role on the play. |
| `pass_defender_player_name` | String | Display name of the defender credited with defending the pass -- the FIRST participant in that role on the play. |
| `punter_player_name` | String | Display name of the punter -- the FIRST participant in that role on the play. |
| `pat_scorer_player_name` | String | Display name of the player credited with the point-after score -- the FIRST participant in that role on the play. |
| `sacked_by_player_name` | String | Display name of a defender credited with the sack -- the FIRST participant in that role on the play. |
| `kicker_player_id` | String | ESPN athlete id of the kicker -- the FIRST participant in that role on the play. |
| `returner_player_id` | String | ESPN athlete id of the player returning the kick or punt -- the FIRST participant in that role on the play. |
| `passer_player_id` | String | ESPN athlete id of the passer -- the FIRST participant in that role on the play. |
| `receiver_player_id` | String | ESPN athlete id of the targeted receiver -- the FIRST participant in that role on the play. |
| `rusher_player_id` | String | ESPN athlete id of the ball carrier on a rush -- the FIRST participant in that role on the play. |
| `penalized_player_id` | String | ESPN athlete id of the penalized player -- the FIRST participant in that role on the play. |
| `scorer_player_id` | String | ESPN athlete id of the player credited with the score -- the FIRST participant in that role on the play. |
| `pass_defender_player_id` | String | ESPN athlete id of the defender credited with defending the pass -- the FIRST participant in that role on the play. |
| `punter_player_id` | String | ESPN athlete id of the punter -- the FIRST participant in that role on the play. |
| `pat_scorer_player_id` | String | ESPN athlete id of the player credited with the point-after score -- the FIRST participant in that role on the play. |
| `sacked_by_player_id` | String | ESPN athlete id of a defender credited with the sack -- the FIRST participant in that role on the play. |
| `kicker_player_names` | String | List of the display names of EVERY participant credited as the kicker on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `returner_player_names` | String | List of the display names of EVERY participant credited as the player returning the kick or punt on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `passer_player_names` | String | List of the display names of EVERY participant credited as the passer on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `receiver_player_names` | String | List of the display names of EVERY participant credited as the targeted receiver on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `rusher_player_names` | String | List of the display names of EVERY participant credited as the ball carrier on a rush on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `penalized_player_names` | String | List of the display names of EVERY participant credited as the penalized player on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `scorer_player_names` | String | List of the display names of EVERY participant credited as the player credited with the score on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pass_defender_player_names` | String | List of the display names of EVERY participant credited as the defender credited with defending the pass on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `punter_player_names` | String | List of the display names of EVERY participant credited as the punter on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pat_scorer_player_names` | String | List of the display names of EVERY participant credited as the player credited with the point-after score on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `sacked_by_player_names` | String | List of the display names of EVERY participant credited as a defender credited with the sack on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `kicker_player_ids` | String | List of the athlete ids of EVERY participant credited as the kicker on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `returner_player_ids` | String | List of the athlete ids of EVERY participant credited as the player returning the kick or punt on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `passer_player_ids` | String | List of the athlete ids of EVERY participant credited as the passer on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `receiver_player_ids` | String | List of the athlete ids of EVERY participant credited as the targeted receiver on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `rusher_player_ids` | String | List of the athlete ids of EVERY participant credited as the ball carrier on a rush on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `penalized_player_ids` | String | List of the athlete ids of EVERY participant credited as the penalized player on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `scorer_player_ids` | String | List of the athlete ids of EVERY participant credited as the player credited with the score on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pass_defender_player_ids` | String | List of the athlete ids of EVERY participant credited as the defender credited with defending the pass on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `punter_player_ids` | String | List of the athlete ids of EVERY participant credited as the punter on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pat_scorer_player_ids` | String | List of the athlete ids of EVERY participant credited as the player credited with the point-after score on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `sacked_by_player_ids` | String | List of the athlete ids of EVERY participant credited as a defender credited with the sack on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `recoverer_player_name` | String | Display name of the player who recovered the fumble -- the FIRST participant in that role on the play. |
| `recoverer_player_id` | String | ESPN athlete id of the player who recovered the fumble -- the FIRST participant in that role on the play. |
| `recoverer_player_names` | String | List of the display names of EVERY participant credited as the player who recovered the fumble on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `recoverer_player_ids` | String | List of the athlete ids of EVERY participant credited as the player who recovered the fumble on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `tackler_player_name` | String | Display name of a defender credited with the tackle -- the FIRST participant in that role on the play. |
| `assisted_by_player_name` | String | Display name of a defender credited with an assisted tackle -- the FIRST participant in that role on the play. |
| `forced_by_player_name` | String | Display name of the defender who forced the fumble -- the FIRST participant in that role on the play. |
| `tackler_player_id` | String | ESPN athlete id of a defender credited with the tackle -- the FIRST participant in that role on the play. |
| `assisted_by_player_id` | String | ESPN athlete id of a defender credited with an assisted tackle -- the FIRST participant in that role on the play. |
| `forced_by_player_id` | String | ESPN athlete id of the defender who forced the fumble -- the FIRST participant in that role on the play. |
| `tackler_player_names` | String | List of the display names of EVERY participant credited as a defender credited with the tackle on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `assisted_by_player_names` | String | List of the display names of EVERY participant credited as a defender credited with an assisted tackle on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `forced_by_player_names` | String | List of the display names of EVERY participant credited as the defender who forced the fumble on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `tackler_player_ids` | String | List of the athlete ids of EVERY participant credited as a defender credited with the tackle on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `assisted_by_player_ids` | String | List of the athlete ids of EVERY participant credited as a defender credited with an assisted tackle on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `forced_by_player_ids` | String | List of the athlete ids of EVERY participant credited as the defender who forced the fumble on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pat_passer_player_name` | String | Display name of the passer on the point-after attempt -- the FIRST participant in that role on the play. |
| `pat_passer_player_id` | String | ESPN athlete id of the passer on the point-after attempt -- the FIRST participant in that role on the play. |
| `pat_passer_player_names` | String | List of the display names of EVERY participant credited as the passer on the point-after attempt on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |
| `pat_passer_player_ids` | String | List of the athlete ids of EVERY participant credited as the passer on the point-after attempt on the play, so multi-entry roles such as split sacks or gang tackles are not collapsed to one. |

```python
load_cfb_play_participants(seasons=2024)
```

## `load_cfb_game_rosters`

Release: [espn_cfb_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_game_rosters/game_rosters_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 | ESPN athlete id. |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_type` | String | Athlete type / class. |
| `first_name` | String | Athlete first name. |
| `last_name` | String | Athlete last name. |
| `full_name` | String | Venue full name (e.g. `Tenney Stadium`). |
| `athlete_display_name` | String | Player display name; `athlete_detail = TRUE` only. |
| `short_name` | String | Ranking source short name (e.g. `AP Poll`). |
| `weight` | Float64 | Listed weight (lbs). |
| `display_weight` | String | Human-readable weight (e.g. `205 lbs`). |
| `height` | Float64 | Listed height (inches). |
| `display_height` | String | Human-readable height (e.g. `6' 1"`). |
| `age` | Float64 | Player age (in years). |
| `date_of_birth` | String | Player date of birth (if published). |
| `slug` | String | URL slug for the team. |
| `jersey` | String | Jersey number. |
| `linked` | Boolean | TRUE if the record is linked to a related entity. |
| `active` | Boolean | `TRUE` if the player was active for the game. |
| `alternate_ids_sdr` | String | Alternate ids sdr. |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |
| `experience_years` | Float64 | Years of experience. |
| `experience_display_value` | String | Experience display value. |
| `experience_abbreviation` | String | Experience abbreviation. |
| `status_id` | String | ESPN commitment status id. |
| `status_name` | String | Status-type key (e.g. `STATUS_FINAL`). |
| `status_type` | String | Status type. |
| `status_abbreviation` | String | Status abbreviation. |
| `birth_place_country` | String | Birth place country. |
| `birth_country_alternate_id` | String |  |
| `birth_country_abbreviation` | String | Birth country abbreviation. |
| `flag_href` | String |  |
| `flag_alt` | String |  |
| `flag_rel` | String |  |
| `starter` | Boolean | `TRUE` if the athlete started the game. |
| `valid` | Boolean | `TRUE` if the roster entry is flagged valid by ESPN. |
| `did_not_play` | Boolean | `TRUE` if the athlete did not play. |
| `athlete_href` | String |  |
| `position_href` | String |  |
| `statistics_href` | String |  |
| `team_id` | Int64 | ESPN team id. |
| `order` | Int64 | Team order within the competition (0 = first). |
| `home_away` | String | `home` or `away`. |
| `winner` | Boolean | `TRUE` if this team won the game. |
| `team_guid` | String | ESPN team GUID. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | Team slug for the stat row. |
| `team_location` | String | Team location / school name; `team_detail = TRUE` only. |
| `team_name` | String | Team nickname; `team_detail = TRUE` only. |
| `team_nickname` | String | Team nickname label; `team_detail = TRUE` only. |
| `team_abbreviation` | String | Team abbreviation; `team_detail = TRUE` only. |
| `team_display_name` | String | Full team display name; `team_detail = TRUE` only. |
| `team_short_display_name` | String | Short team display name; `team_detail = TRUE` only. |
| `team_color` | String | Primary team color; `team_detail = TRUE` only. |
| `team_alternate_color` | String | Alternate team color; `team_detail = TRUE` only. |
| `is_active` | Boolean | Whether the team is currently active. |
| `is_all_star` | Boolean | Whether the team is an all-star team. |
| `team_alternate_ids_sdr` | String |  |
| `logo_href` | String | URL of the default team logo. |
| `logo_dark_href` | String | URL of the dark-variant team logo. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `draft_display_text` | String | Draft display text. |
| `draft_round` | Float64 | Round of the draft selection. |
| `draft_year` | Float64 | Draft year (4-digit). |
| `draft_selection` | Float64 | Draft selection. |
| `draft_team_href` | String |  |
| `middle_name` | String | Middle name of the player. |
| `headshot_href` | String | URL of the athlete headshot image. |
| `headshot_alt` | String | Alternative-text label for the headshot. |

```python
load_cfb_game_rosters(seasons=2024)
```

## `load_cfb_linescores`

Release: [espn_cfb_linescores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_linescores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_linescores/linescores_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `period` | Int64 | Period (quarter) number. |
| `value` | String | Metric value. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |

```python
load_cfb_linescores(seasons=2024)
```

## `load_cfb_betting`

Release: [espn_cfb_betting](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_betting) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_betting/betting_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `game_spread` | Float64 | Game spread in (-X Team) format. There are almost none, I would recommend not trusting any of these three columns |
| `over_under` | Float64 | Pre-game over/under total from the selected provider. |
| `home_favorite` | Boolean | `TRUE` if the home team is the favorite. |
| `home_team_spread` | Float64 | The game spread with respect to the home team |
| `game_spread_available` | Boolean | Logical (TRUE/FALSE) indicating whether the spread was available from ESPN. Basically, I would just not recommend using any of the spread information, I think I defaulted a lot of them to -2.5 for the home team. Most games probably do not have spread information. This column should really be listed first |
| `odds_source` | String |  |

```python
load_cfb_betting(seasons=2024)
```

## `load_cfb_power_index`

Release: [espn_cfb_power_index](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_power_index) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_power_index/power_index_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_power_index(seasons=2024)
```

## `load_cfb_adv_team`

Release: [espn_cfb_adv_team](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_team) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_team/adv_team_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `rushing_highlight_yards_per_opp` | Float64 | Highlight yards per rushing opportunity. |
| `total_pen_yards` | Int64 | Total penalty yards assessed. |
| `EPA_penalty` | Float64 | Total EPA attributed to penalties. |
| `penalty_first_downs_created` | Int64 | Number of first downs the team gained via opponent penalty. |
| `penalty_first_downs_created_rate` | Float64 | Share of the team's first downs that came via opponent penalty. |
| `penalties` | Int64 | Total number of penalties. |
| `penalty_yards` | Int64 | Yards gained (or lost) by the posteam from the penalty. |
| `special_teams_plays` | Int64 | Number of special-teams plays. |
| `EPA_sp` | Float64 | Total special-teams EPA, ESPN's abbreviated field for the same phase. |
| `EPA_special_teams` | Float64 | Total EPA generated on special-teams plays. |
| `field_goals` | Int64 | Number of field-goal attempts. |
| `EPA_fg` | Float64 | Total EPA on field-goal attempts. |
| `punt_plays` | Int64 | Number of punt plays. |
| `EPA_punt` | Float64 | Total EPA on punt plays. |
| `kickoff_plays` | Int64 | Number of kickoff plays. |
| `EPA_kickoff` | Float64 | Total EPA on kickoff plays. |
| `rushes` | Int64 | Number of rushing attempts. |
| `rush_yards` | Float64 | The number of rushing yards gained |
| `yards_per_rush` | Float64 | Yards gained per rushing attempt. |
| `rushing_power_rate` | Float64 | Share of carries that were power rushing attempts. |
| `rushing_first_downs_created` | Int64 | Number of first downs created on rush plays. |
| `rushing_first_downs_created_rate` | Float64 | Share of rush plays that created a first down. |
| `EPA_rushing_overall` | Float64 | Total EPA on rush plays. |
| `EPA_rushing_per_play` | Float64 | EPA per rush play. |
| `EPA_explosive_rushing` | Int64 | Count of explosive rush plays. A play count, not an EPA total. |
| `EPA_explosive_rushing_rate` | Float64 | Explosive-play rate on rush plays, over ESPN's qualifying-play denominator. |
| `EPA_non_explosive_rushing` | Float64 | Total EPA on rush plays with explosive plays excluded. |
| `EPA_non_explosive_rushing_per_play` | Float64 | EPA per rush play with explosive plays excluded. |
| `passes` | Int64 | Passes. |
| `pass_yards` | Float64 | Number of yards gained on pass plays |
| `yards_per_pass` | Float64 | Team game yards per pass. |
| `passing_first_downs_created` | Int64 | Number of first downs created on pass plays. |
| `passing_first_downs_created_rate` | Float64 | Share of pass plays that created a first down. |
| `EPA_passing_overall` | Float64 | Total EPA on pass plays. |
| `EPA_passing_per_play` | Float64 | EPA per pass play. |
| `EPA_explosive_passing` | Int64 | Count of explosive pass plays. A play count, not an EPA total. |
| `EPA_explosive_passing_rate` | Float64 | Explosive-play rate on pass plays, over ESPN's qualifying-play denominator. |
| `EPA_non_explosive_passing` | Float64 | Total EPA on pass plays with explosive plays excluded. |
| `EPA_non_explosive_passing_per_play` | Float64 | EPA per pass play with explosive plays excluded. |
| `scrimmage_plays` | Int64 | Number of plays from scrimmage (rushes plus passes), excluding special teams. |
| `EPA_overall_off` | Float64 | Total offensive EPA for the team. Duplicated exactly by EPA_overall_offense in every published season checked -- prefer one and ignore the other. |
| `EPA_overall_offense` | Float64 | Total offensive EPA. An exact duplicate of EPA_overall_off. |
| `EPA_per_play` | Float64 | Offensive EPA per play. |
| `EPA_non_explosive` | Float64 | Total EPA with explosive plays excluded, isolating the team's routine-down production. |
| `EPA_non_explosive_per_play` | Float64 | EPA per play with explosive plays excluded. |
| `EPA_explosive` | Int64 | Count of explosive plays, per ESPN's advanced box score. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_explosive_rate` | Float64 | Explosive-play rate. Note this is NOT EPA_explosive divided by EPA_plays -- ESPN divides by its own smaller qualifying-play count, so deriving it yourself will not reproduce this value. |
| `passes_rate` | Float64 | Share of the team's plays from scrimmage that were pass plays. |
| `off_yards` | Int64 | Offensive yards gained from scrimmage. |
| `total_off_yards` | Int64 | Total offensive yards across all plays. |
| `yards_per_play` | Float64 | Yards gained per play. |
| `EPA_plays` | Int64 | Number of plays ESPN's advanced box score scored for the team. |
| `total_yards` | Int64 | Team total yards. |
| `EPA_overall_total` | Float64 | Total EPA across all phases, which is why it differs from the offense-only EPA_overall_off. |
| `rushes_rate` | Float64 | Share of the team's plays from scrimmage that were rush plays. |
| `first_downs_created` | Int64 | Number of first downs the team created. |
| `first_downs_created_rate` | Float64 | Share of the team's plays that created a first down. |
| `EPA_rushing_power` | Float64 | Total EPA on power rushing situations, as classified by ESPN's advanced box score. |
| `EPA_rushing_power_per_play` | Float64 | EPA per play on power rushing situations. |
| `rushing_power_success` | Int64 | Rushing power success rate. |
| `rushing_power_success_rate` | Float64 | Share of power rushing attempts that succeeded. |
| `rushing_power` | Int64 | Count of power rushing attempts, in short-yardage situations as classified by ESPN's advanced box score. |
| `rushing_stuff` | Int64 | Count of stuffed rushing attempts. |
| `rushing_stuff_rate` | Float64 | Rushing stuff rate. |
| `rushing_stopped` | Int64 | Count of rushing attempts stopped at or behind the line of scrimmage. |
| `rushing_stopped_rate` | Float64 | Share of carries stopped at or behind the line of scrimmage. |
| `rushing_opportunity` | Int64 | Count of rushing opportunities -- carries that reached ESPN's opportunity threshold. |
| `rushing_opportunity_rate` | Float64 | Share of carries that qualified as rushing opportunities. |
| `rushing_highlight` | Int64 | Highlight yards -- rushing yardage credited to the back rather than the offensive line. |
| `rushing_highlight_rate` | Float64 | Share of rushing yardage that was highlight (back-credited) yardage. |
| `rushing_highlight_yards` | Float64 | Opponent-adjusted offensive highlight yards per opportunity rush. |
| `line_yards` | Float64 | Line yards -- the portion of rushing yardage credited to the offensive line under the standard rushing decomposition. ESPN applies its own qualifying threshold for the yardage split. |
| `line_yards_per_carry` | Float64 | Line yards per rushing attempt. |
| `second_level_yards` | Float64 | Second-level yards -- rushing yardage earned just beyond the line of scrimmage. |
| `open_field_yards` | Float64 | Open-field yards -- rushing yardage earned well downfield, past the second level. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_team(seasons=2024)
```

## `load_cfb_adv_passing`

Release: [espn_cfb_adv_passing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_passing) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_passing/adv_passing_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `passer_player_name` | String | Display name of the passer -- the FIRST participant in that role on the play. |
| `Comp` | Int64 | Completed passes recorded in the advanced box score. |
| `Att` | Int64 | Pass attempts recorded in the advanced box score. |
| `xComp` | Float64 | Expected completions, summed from the per-play completion model. |
| `Yds` | Float64 | Passing yards from the advanced box score. |
| `Pass_TD` | Int64 | Passing touchdowns. |
| `Int` | Int64 | Interceptions thrown. |
| `YPA` | Float64 | Yards per pass attempt. |
| `EPA` | Float64 | Expected Points Added on the play (cfbfastR EPA model output). |
| `EPA_per_Play` | Float64 | EPA per play on the passer's plays. |
| `WPA` | Float64 | Win Probability Added. |
| `SR` | Float64 | Success rate on the passer's plays. |
| `Sck` | Int64 | Times the passer was sacked. |
| `CompPct` | Float64 | Completion percentage from the advanced box score. |
| `xCompPct` | Float64 | Expected completion percentage from the per-play completion model. |
| `CPOE` | Float64 | Completion percentage over expected -- actual minus modelled completion rate. |
| `qbr_epa` | Float64 | EPA variant used as an input to the QBR calculation. |
| `sack_epa` | Float64 | EPA credited to the player's sacks taken. |
| `pass_epa` | Float64 | EPA credited to the player's pass plays. |
| `rush_epa` | Float64 | EPA credited to the player's rush plays. |
| `pen_epa` | Float64 | EPA attributable to penalties on the player's plays. |
| `spread` | Float64 | Pre-game point spread from the selected provider. |
| `era0` | Int64 | Rule-era indicator for the earliest modelled era. |
| `era1` | Int64 | Rule-era indicator for the second modelled era. |
| `era2` | Int64 | Rule-era indicator for the third modelled era. |
| `era3` | Int64 | Rule-era indicator for the most recent modelled era. |
| `exp_qbr` | Float64 | Expected QBR for the passer. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_passing(seasons=2024)
```

## `load_cfb_adv_rushing`

Release: [espn_cfb_adv_rushing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_rushing) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_rushing/adv_rushing_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `rusher_player_name` | String | Display name of the ball carrier on a rush -- the FIRST participant in that role on the play. |
| `Car` | Int64 |  |
| `Yds` | Float64 | Passing yards from the advanced box score. |
| `Rush_TD` | Int64 |  |
| `YPC` | Float64 |  |
| `EPA` | Float64 | Expected Points Added on the play (cfbfastR EPA model output). |
| `EPA_per_Play` | Float64 | EPA per play on the passer's plays. |
| `WPA` | Float64 | Win Probability Added. |
| `SR` | Float64 | Success rate on the passer's plays. |
| `Fum` | Int64 |  |
| `Fum_Lost` | Int64 |  |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_rushing(seasons=2024)
```

## `load_cfb_adv_receiving`

Release: [espn_cfb_adv_receiving](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_receiving) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_receiving/adv_receiving_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `receiver_player_name` | String | Display name of the targeted receiver -- the FIRST participant in that role on the play. |
| `Rec` | Int64 |  |
| `Tar` | Int64 |  |
| `Yds` | Float64 | Passing yards from the advanced box score. |
| `Rec_TD` | Int64 |  |
| `YPT` | Float64 |  |
| `EPA` | Float64 | Expected Points Added on the play (cfbfastR EPA model output). |
| `EPA_per_Play` | Float64 | EPA per play on the passer's plays. |
| `WPA` | Float64 | Win Probability Added. |
| `SR` | Float64 | Success rate on the passer's plays. |
| `Fum` | Int64 |  |
| `Fum_Lost` | Int64 |  |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_receiving(seasons=2024)
```

## `load_cfb_adv_defensive`

Release: [espn_cfb_adv_defensive](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_defensive) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_defensive/adv_defensive_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `def_pos_team_id` | Int64 |  |
| `def_pos_team` | String | Team name on defense at the start of the play. |
| `scrimmage_plays` | Int64 | Number of plays from scrimmage (rushes plus passes), excluding special teams. |
| `TFL` | Int64 |  |
| `TFL_pass` | Int64 |  |
| `TFL_rush` | Int64 |  |
| `havoc_total` | Int64 | Total havoc rate. |
| `havoc_total_rate` | Float64 |  |
| `fumbles` | Int64 |  |
| `def_int` | Int64 |  |
| `drive_stopped_rate` | Float64 |  |
| `num_pass_plays` | Int64 |  |
| `havoc_total_pass` | Int64 |  |
| `havoc_total_pass_rate` | Float64 |  |
| `sacks` | Int64 | Team sacks. |
| `sacks_rate` | Float64 |  |
| `pass_breakups` | Int64 |  |
| `havoc_total_rush` | Int64 |  |
| `havoc_total_rush_rate` | Float64 |  |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_defensive(seasons=2024)
```

## `load_cfb_adv_defensive_players`

Release: [espn_cfb_adv_defensive_players](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_defensive_players) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_defensive_players/adv_defensive_players_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `def_pos_team_id` | Int64 | ESPN team id of the team on defense. Present for every season 2004+. |
| `def_pos_team` | String | Display name of the team on defense (e.g. 'Ohio State Buckeyes'). Held an ESPN team id until the 2026-08 republish; the id now lives in def_pos_team_id. |
| `player_name` | String | Display name of the defender. |
| `fumble_recoveries` | Int64 | Fumbles recovered by the defender. Available for every season 2004+. |
| `fumble_recoveries_yards` | Int64 | Yards returned on the defender's fumble recoveries. Available for every season 2004+. |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `sacks` | Int64 | Sacks recorded by the defender. Available from 2005 on; null for 2004. |
| `sacks_yards` | Int64 | Yards lost by the offense on the defender's sacks. Available from 2005 on; null for 2004. |
| `pass_breakups` | Int64 | Passes broken up by the defender. Available from 2005 on; null for 2004. |
| `forced_fumbles` | Int64 | Fumbles forced by the defender. Available from 2005 on; null for 2004, which ESPN ships with only the fumble-recovery statistics. |
| `interceptions` | Int64 | Passes intercepted by the defender. Available from 2014 on; null for 2004-2013, which ESPN ships without interception statistics in this block. |
| `interceptions_yards` | Int64 | Yards returned on the defender's interceptions. Available from 2014 on; null for 2004-2013. |

```python
load_cfb_adv_defensive_players(seasons=2024)
```

## `load_cfb_adv_drives`

Release: [espn_cfb_adv_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_drives) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_drives/adv_drives_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `drive_total_available_yards` | Float64 |  |
| `drive_total_gained_yards` | Int64 |  |
| `avg_field_position` | Float64 |  |
| `plays_per_drive` | Float64 |  |
| `yards_per_drive` | Float64 |  |
| `drives` | Int64 |  |
| `drive_total_gained_yards_rate` | Float64 |  |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_drives(seasons=2024)
```

## `load_cfb_adv_situational`

Release: [espn_cfb_adv_situational](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_situational) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_situational/adv_situational_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Display name of the team on offense (e.g. 'Ohio State Buckeyes'). |
| `EPA_success` | Int64 | Count of successful plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_rate` | Float64 | Success rate -- the share of those plays ESPN scored as successful. |
| `EPA_success_pass` | Int64 | Count of successful plays on pass plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_pass_rate` | Float64 | Success rate on pass plays -- the share of those plays ESPN scored as successful. |
| `EPA_success_rush` | Int64 | Count of successful plays on rush plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_rush_rate` | Float64 | Success rate on rush plays -- the share of those plays ESPN scored as successful. |
| `EPA_success_rz` | Int64 | Count of successful plays on the red zone. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_rate_rz` | Float64 | Success rate on the red zone -- the share of those plays ESPN scored as successful. |
| `EPA_success_third` | Int64 | Count of successful plays on third down. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_rate_third` | Float64 | Success rate on third down -- the share of those plays ESPN scored as successful. |
| `EPA_success_early_down` | Int64 | Count of successful plays on early downs. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_early_down_rate` | Float64 | Success rate on early downs -- the share of those plays ESPN scored as successful. |
| `early_downs` | Int64 | Number of plays the team ran on early downs. |
| `early_down_pass_rate` | Float64 | Share of the team's plays on early downs that were pass plays. |
| `early_down_rush_rate` | Float64 | Share of the team's plays on early downs that were rush plays. |
| `EPA_early_down` | Float64 | Total EPA the team generated on early downs. |
| `EPA_early_down_per_play` | Float64 | EPA per play on early downs. |
| `early_down_first_down` | Int64 | Number of early-down plays that produced a first down. |
| `early_down_first_down_rate` | Float64 | Share of early-down plays that produced a first down. |
| `early_down_pass` | Int64 | Number of pass plays the team ran on early downs. |
| `EPA_early_down_pass` | Float64 | Total EPA the team generated on early downs on pass plays. |
| `EPA_early_down_pass_per_play` | Float64 | EPA per play on early downs on pass plays. |
| `EPA_success_early_down_pass` | Int64 | Count of successful plays on early downs on pass plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_early_down_pass_rate` | Float64 | Success rate on early downs on pass plays -- the share of those plays ESPN scored as successful. |
| `early_down_rush` | Int64 | Number of rush plays the team ran on early downs. |
| `EPA_early_down_rush` | Float64 | Total EPA the team generated on early downs on rush plays. |
| `EPA_early_down_rush_per_play` | Float64 | EPA per play on early downs on rush plays. |
| `EPA_success_early_down_rush` | Int64 | Count of successful plays on early downs on rush plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_early_down_rush_rate` | Float64 | Success rate on early downs on rush plays -- the share of those plays ESPN scored as successful. |
| `middle_8` | Int64 | Number of plays the team ran in the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `middle_8_pass_rate` | Float64 | Share of the team's plays on the middle eight -- the closing minutes of the first half and opening minutes of the second that were pass plays. |
| `middle_8_rush_rate` | Float64 | Share of the team's plays on the middle eight -- the closing minutes of the first half and opening minutes of the second that were rush plays. |
| `EPA_middle_8` | Float64 | Total EPA the team generated on the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `EPA_middle_8_per_play` | Float64 | EPA per play on the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `EPA_middle_8_success` | Int64 | Count of successful plays on the middle eight -- the closing minutes of the first half and opening minutes of the second. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_middle_8_success_rate` | Float64 | Success rate on the middle eight -- the closing minutes of the first half and opening minutes of the second -- the share of those plays ESPN scored as successful. |
| `middle_8_pass` | Int64 | Number of pass plays the team ran on the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `EPA_middle_8_pass` | Float64 | Total EPA the team generated on the middle eight -- the closing minutes of the first half and opening minutes of the second on pass plays. |
| `EPA_middle_8_pass_per_play` | Float64 | EPA per play on the middle eight -- the closing minutes of the first half and opening minutes of the second on pass plays. |
| `EPA_middle_8_success_pass` | Int64 | Count of successful plays on the middle eight -- the closing minutes of the first half and opening minutes of the second on pass plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_middle_8_success_pass_rate` | Float64 | Success rate on the middle eight -- the closing minutes of the first half and opening minutes of the second on pass plays -- the share of those plays ESPN scored as successful. |
| `middle_8_rush` | Int64 | Number of rush plays the team ran on the middle eight -- the closing minutes of the first half and opening minutes of the second. |
| `EPA_middle_8_rush` | Float64 | Total EPA the team generated on the middle eight -- the closing minutes of the first half and opening minutes of the second on rush plays. |
| `EPA_middle_8_rush_per_play` | Float64 | EPA per play on the middle eight -- the closing minutes of the first half and opening minutes of the second on rush plays. |
| `EPA_middle_8_success_rush` | Int64 | Count of successful plays on the middle eight -- the closing minutes of the first half and opening minutes of the second on rush plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_middle_8_success_rush_rate` | Float64 | Success rate on the middle eight -- the closing minutes of the first half and opening minutes of the second on rush plays -- the share of those plays ESPN scored as successful. |
| `EPA_success_late_down` | Int64 | Count of successful plays on late downs. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_late_down_pass` | Int64 | Count of successful plays on late downs on pass plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_late_down_rush` | Int64 | Count of successful plays on late downs on rush plays. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `late_downs` | Int64 | Number of plays the team ran on late downs. |
| `late_down_pass` | Int64 | Number of pass plays the team ran on late downs. |
| `late_down_rush` | Int64 | Number of rush plays the team ran on late downs. |
| `EPA_late_down` | Float64 | Total EPA the team generated on late downs. |
| `EPA_late_down_per_play` | Float64 | EPA per play on late downs. |
| `EPA_success_late_down_rate` | Float64 | Success rate on late downs -- the share of those plays ESPN scored as successful. |
| `EPA_success_late_down_pass_rate` | Float64 | Success rate on late downs on pass plays -- the share of those plays ESPN scored as successful. |
| `EPA_success_late_down_rush_rate` | Float64 | Success rate on late downs on rush plays -- the share of those plays ESPN scored as successful. |
| `late_down_pass_rate` | Float64 | Share of the team's plays on late downs that were pass plays. |
| `late_down_rush_rate` | Float64 | Share of the team's plays on late downs that were rush plays. |
| `EPA_success_standard_down` | Int64 | Count of successful plays on standard downs (the team ahead of schedule for the series). Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_standard_down_rate` | Float64 | Success rate on standard downs (the team ahead of schedule for the series) -- the share of those plays ESPN scored as successful. |
| `EPA_standard_down` | Float64 | Total EPA the team generated on standard downs (the team ahead of schedule for the series). |
| `EPA_standard_down_per_play` | Float64 | EPA per play on standard downs (the team ahead of schedule for the series). |
| `standard_downs` | Int64 | Number of plays the team ran on standard downs (the team ahead of schedule for the series). |
| `EPA_success_passing_down` | Int64 | Count of successful plays on passing downs (the team behind schedule for the series). Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_success_passing_down_rate` | Float64 | Success rate on passing downs (the team behind schedule for the series) -- the share of those plays ESPN scored as successful. |
| `EPA_passing_down` | Float64 | Total EPA the team generated on passing downs (the team behind schedule for the series). |
| `EPA_passing_down_per_play` | Float64 | EPA per play on passing downs (the team behind schedule for the series). |
| `passing_downs` | Int64 | Number of plays the team ran on passing downs (the team behind schedule for the series). |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_situational(seasons=2024)
```

## `load_cfb_adv_specialists`

Release: [espn_cfb_adv_specialists](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_specialists) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_specialists/adv_specialists_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `player_name` | String | Player name. |
| `punts` | Int64 | Punts attempted. |
| `punts_yards` | Int64 |  |
| `kick_returns` | Int64 | Number of kick returns. |
| `kick_returns_yards` | Int64 |  |
| `punt_returns` | Int64 | Number of punt returns. |
| `punt_returns_yards` | Int64 |  |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `field_goals` | Int64 | Number of field-goal attempts. |
| `field_goals_yards` | Int64 |  |

```python
load_cfb_adv_specialists(seasons=2024)
```

## `load_cfb_adv_turnover`

Release: [espn_cfb_adv_turnover](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_turnover) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_turnover/adv_turnover_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pos_team_id` | Int64 | ESPN team id of the team on offense. Present for every season 2004+. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `turnovers` | Int64 | Turnovers total. |
| `st_turnovers_lost` | Int64 |  |
| `Int` | Int64 | Interceptions thrown. |
| `fumbles_lost` | Int64 | Fumbles lost. |
| `pass_breakups` | Int64 |  |
| `total_fumbles` | Int64 | Team total fumbles. |
| `fumbles_recovered` | Int64 | Team fumbles recovered. |
| `team_id` | Int64 | ESPN team id. |
| `turnovers_pbp` | Int64 |  |
| `Int_pbp` | Int64 |  |
| `fumbles_lost_pbp` | Int64 |  |
| `espn_sourced` | Boolean |  |
| `expected_turnovers` | Float64 |  |
| `expected_turnover_margin` | Float64 |  |
| `turnover_margin` | Int64 |  |
| `turnover_luck` | Float64 |  |
| `takeaways` | Int64 | Takeaways. |
| `st_turnovers_gained` | Int64 |  |
| `fumble_recoveries_gained` | Int64 |  |
| `game_id` | Int64 | ESPN game identifier. |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |

```python
load_cfb_adv_turnover(seasons=2024)
```

## `load_cfb_model_pbp`

Release: [espn_cfb_model_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_model_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_model_pbp/model_pbp_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game identifier. |
| `id` | String | 247Sports referencing id for the recruit. |
| `sequenceNumber` | String | Broadcast sequence order number. |
| `game_play_number` | Int64 | Sequential play number within the game (excludes timeouts/end markers). |
| `drive.id` | String |  |
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `period` | Int64 | Period (quarter) number. |
| `pos_team` | Int64 | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team` | Int64 | Team name on defense at the start of the play. |
| `start.pos_team.name` | String |  |
| `homeTeamId` | Int64 |  |
| `awayTeamId` | Int64 |  |
| `homeTeamName` | String |  |
| `awayTeamName` | String |  |
| `type.text` | String |  |
| `text` | String | Full play description. |
| `start.down` | Int64 |  |
| `start.distance` | Int64 |  |
| `start.yardsToEndzone` | Int64 |  |
| `pos_score_diff_start` | Int64 | Score differential for the possession team at the start of the play. |
| `start.TimeSecsRem` | Int64 |  |
| `start.is_home` | Boolean |  |
| `passing_down` | Boolean |  |
| `pass` | Boolean | Binary flag for a passing play (includes sacks). |
| `rush` | Boolean | Binary flag for a rushing play. |
| `completion` | Boolean | Binary flag for a completed pass. |
| `scoring_play` | Boolean | `TRUE` if the play resulted in a score. |
| `statYardage` | Int64 |  |
| `passer_player_name` | String | Display name of the passer -- the FIRST participant in that role on the play. |
| `ep_before` | Float64 | Expected points value before the play (cfbfastR EPA model). |
| `ep_after` | Float64 | Expected points value after the play (cfbfastR EPA model). |
| `epa` | Float64 | Expected points added (EPA) by the posteam for the given play. |
| `wp_before` | Float64 | Win probability for the possession team before the play (0-1). |
| `wp_after` | Float64 | Win probability for the possession team after the play (0-1). |
| `wpa` | Float64 | Win Probability Added on the play (cfbfastR WP model output). |
| `completion_prob` | Float64 | Modelled probability the pass is completed. |
| `cpoe` | Float64 | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `model_pbp_version` | String | Version of the model-scored play-by-play build. |
| `cp_model_version` | String | Version of the completion-probability model that scored the play. |
| `ep_model_version` | String | Version of the expected-points model that scored the play. |
| `wp_model_version` | String | Version of the win-probability model that scored the play. |
| `scored_date` | String | Date on which the play was scored by the models. |

```python
load_cfb_model_pbp(seasons=2024)
```

## `load_cfb_passing`

Release: [espn_cfb_passing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_passing) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_passing/cfb_passing_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `division` | String | Division in the conference for the team. |
| `conference` | String | Conference of the team. |
| `season` | Int64 | Season (4-digit year). |
| `player_id` | Int64 | ESPN player id from the roster entry. |
| `passer_player_name` | String | Display name of the passer -- the FIRST participant in that role on the play. |
| `plays` | UInt32 | Total qualifying passing plays included in the WEPA calculation. |
| `games` | UInt32 | Number of games included in the ATS summary. |
| `team_games` | UInt32 | Games the team played, used as the per-game denominator. |
| `playsgame` | Float64 | Plays per game. |
| `TEPA` | Float64 | Total EPA summed over every play. |
| `EPAplay` | Float64 | EPA generated per play. |
| `EPAgame` | Float64 | EPA generated per game. |
| `yards` | Int64 | Total yards gained on the drive. |
| `yardsplay` | Float64 | Yards per play. |
| `yardsgame` | Float64 | Yards per game. |
| `success` | Float64 | Success rate across the team plays. |
| `comp` | Float64 | Completed passes. |
| `att` | Float64 | Pass attempts thrown. |
| `comppct` | Float64 | Completion percentage. |
| `passing_td` | Float64 | Passing touchdowns thrown. |
| `sacked` | UInt32 | Times the passer was sacked. |
| `sack_yds` | Int64 | Yards lost to sacks. |
| `pass_int` | UInt32 | Interceptions thrown. |
| `detmer` | Float64 | Detmer rating -- the composite passing-efficiency measure this pipeline publishes, named for the college passing-efficiency tradition. |
| `detmergame` | Float64 | Detmer rating expressed per game. |
| `dropbacks` | Float64 | Dropbacks taken by the passer. |
| `sack_adj_yards` | Int64 | Passing yards adjusted for sack yardage lost. |
| `yardsdropback` | Float64 | Yards per dropback. |
| `TEPA_rank` | Float64 | National rank of the team's total EPA summed over every play, where 1 is best. |
| `EPAgame_rank` | Float64 | National rank of the team's EPA generated per game, where 1 is best. |
| `EPAplay_rank` | Float64 | National rank of the team's EPA generated per play, where 1 is best. |
| `success_rank` | Float64 | National rank of the team's success rate across the team plays, where 1 is best. |
| `comppct_rank` | Float64 | National rank of the team's completion percentage, where 1 is best. |
| `yards_rank` | Float64 | National rank of the team's total yards, where 1 is best. |
| `yardsplay_rank` | Float64 | National rank of the team's yards per play, where 1 is best. |
| `yardsgame_rank` | Float64 | National rank of the team's yards per game, where 1 is best. |
| `sack_adj_yards_rank` | Float64 | National rank of the team's passing yards adjusted for sack yardage lost, where 1 is best. |
| `yardsdropback_rank` | Float64 | National rank of the team's yards per dropback, where 1 is best. |
| `detmer_rank` | Float64 | National rank of the team's detmer rating -- the composite passing-efficiency measure this pipeline publishes, named for the college passing-efficiency tradition, where 1 is best. |
| `detmergame_rank` | Float64 | National rank of the team's detmer rating expressed per game, where 1 is best. |
| `fbs_class` | String | Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, derived from conference membership. Null for teams outside FBS. |

```python
load_cfb_passing(seasons=2024)
```

## `load_cfb_percentiles`

Release: [espn_cfb_percentiles](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_percentiles) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_percentiles/cfb_percentiles_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `pctile` | Float64 | Percentile bucket the row reports, from 0 to 100. |
| `GEI` | Float64 | Value of game excitement index at the percentile this row reports. |
| `EPAplay` | Float64 | Value of EPA generated per play at the percentile this row reports. |
| `pass_success` | Float64 | Value of success rate on pass plays at the percentile this row reports. |
| `rush_success` | Float64 | Value of success rate on rush plays at the percentile this row reports. |
| `early_down_success` | Float64 | Value of success rate on early downs at the percentile this row reports. |
| `early_down_EPA` | Float64 | Value of EPA per early-down play at the percentile this row reports. |
| `late_down_success` | Float64 | Value of success rate on late downs at the percentile this row reports. |
| `success` | Float64 | Value of success rate across the team plays at the percentile this row reports. |
| `yardsplay` | Float64 | Value of yards per play at the percentile this row reports. |
| `dropbacks` | Float64 | Value of dropbacks taken by the passer at the percentile this row reports. |
| `rushes` | Float64 | Value of rushing attempts at the percentile this row reports. |
| `EPAdropback` | Float64 | Value of EPA generated per dropback at the percentile this row reports. |
| `EPArush` | Float64 | Value of EPA generated per rushing attempt at the percentile this row reports. |
| `yardsdropback` | Float64 | Value of yards per dropback at the percentile this row reports. |
| `pass_explosive` | Float64 | Value of explosive-play rate on pass plays at the percentile this row reports. |
| `rush_explosive` | Float64 | Value of explosive-play rate on rush plays at the percentile this row reports. |
| `explosive` | Float64 | Value of explosive-play rate at the percentile this row reports. |
| `third_down_success` | Float64 | Value of success rate on third down at the percentile this row reports. |
| `red_zone_success` | Float64 | Value of success rate in the red zone at the percentile this row reports. |
| `play_stuffed` | Float64 | Value of stuffed-play rate at the percentile this row reports. |
| `nonExplosiveEpaPerPlay` | Float64 | Value of EPA per play excluding explosive plays at the percentile this row reports. |
| `havoc` | Float64 | Value of havoc rate at the percentile this row reports. |
| `yardsrush` | Float64 | Value of yards per rush at the percentile this row reports. |
| `lineyards` | Float64 | Value of line yards per rush at the percentile this row reports. |
| `opportunity_run` | Float64 | Value of opportunity-run rate at the percentile this row reports. |
| `third_down_distance` | Float64 | Value of average yards to go on third down at the percentile this row reports. |

```python
load_cfb_percentiles(seasons=2024)
```

## `load_cfb_receiving`

Release: [espn_cfb_receiving](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_receiving) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_receiving/cfb_receiving_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `division` | String | Division in the conference for the team. |
| `conference` | String | Conference of the team. |
| `season` | Int64 | Season (4-digit year). |
| `player_id` | Int64 | ESPN player id from the roster entry. |
| `receiver_player_name` | String | Display name of the targeted receiver -- the FIRST participant in that role on the play. |
| `plays` | UInt32 | Total qualifying passing plays included in the WEPA calculation. |
| `games` | UInt32 | Number of games included in the ATS summary. |
| `team_games` | UInt32 | Games the team played, used as the per-game denominator. |
| `playsgame` | Float64 | Plays per game. |
| `TEPA` | Float64 | Total EPA summed over every play. |
| `EPAplay` | Float64 | EPA generated per play. |
| `EPAgame` | Float64 | EPA generated per game. |
| `yards` | Int64 | Total yards gained on the drive. |
| `yardsplay` | Float64 | Yards per play. |
| `yardsgame` | Float64 | Yards per game. |
| `success` | Float64 | Success rate across the team plays. |
| `comp` | UInt32 | Completed passes. |
| `targets` | UInt32 | The number of pass plays where the player was the targeted receiver. |
| `catchpct` | Float64 |  |
| `passing_td` | Float64 | Passing touchdowns thrown. |
| `fumbles` | Float64 |  |
| `TEPA_rank` | Float64 | National rank of the team's total EPA summed over every play, where 1 is best. |
| `EPAgame_rank` | Float64 | National rank of the team's EPA generated per game, where 1 is best. |
| `EPAplay_rank` | Float64 | National rank of the team's EPA generated per play, where 1 is best. |
| `success_rank` | Float64 | National rank of the team's success rate across the team plays, where 1 is best. |
| `catchpct_rank` | Float64 |  |
| `yards_rank` | Float64 | National rank of the team's total yards, where 1 is best. |
| `yardsplay_rank` | Float64 | National rank of the team's yards per play, where 1 is best. |
| `yardsgame_rank` | Float64 | National rank of the team's yards per game, where 1 is best. |
| `fbs_class` | String | Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, derived from conference membership. Null for teams outside FBS. |

```python
load_cfb_receiving(seasons=2024)
```

## `load_cfb_rushing`

Release: [espn_cfb_rushing](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_rushing) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_rushing/cfb_rushing_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `division` | String | Division in the conference for the team. |
| `conference` | String | Conference of the team. |
| `season` | Int64 | Season (4-digit year). |
| `player_id` | Int64 | ESPN player id from the roster entry. |
| `rusher_player_name` | String | Display name of the ball carrier on a rush -- the FIRST participant in that role on the play. |
| `plays` | UInt32 | Total qualifying passing plays included in the WEPA calculation. |
| `games` | UInt32 | Number of games included in the ATS summary. |
| `team_games` | UInt32 | Games the team played, used as the per-game denominator. |
| `playsgame` | Float64 | Plays per game. |
| `TEPA` | Float64 | Total EPA summed over every play. |
| `EPAplay` | Float64 | EPA generated per play. |
| `EPAgame` | Float64 | EPA generated per game. |
| `yards` | Int64 | Total yards gained on the drive. |
| `yardsplay` | Float64 | Yards per play. |
| `yardsgame` | Float64 | Yards per game. |
| `success` | Float64 | Success rate across the team plays. |
| `rushing_td` | Float64 | Rushing touchdowns. |
| `fumbles` | Float64 |  |
| `TEPA_rank` | Float64 | National rank of the team's total EPA summed over every play, where 1 is best. |
| `EPAgame_rank` | Float64 | National rank of the team's EPA generated per game, where 1 is best. |
| `EPAplay_rank` | Float64 | National rank of the team's EPA generated per play, where 1 is best. |
| `success_rank` | Float64 | National rank of the team's success rate across the team plays, where 1 is best. |
| `yards_rank` | Float64 | National rank of the team's total yards, where 1 is best. |
| `yardsplay_rank` | Float64 | National rank of the team's yards per play, where 1 is best. |
| `yardsgame_rank` | Float64 | National rank of the team's yards per game, where 1 is best. |
| `fbs_class` | String | Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, derived from conference membership. Null for teams outside FBS. |

```python
load_cfb_rushing(seasons=2024)
```

## `load_cfb_team_summaries`

Release: [espn_cfb_team_summaries](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_team_summaries) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_team_summaries/cfb_team_summaries_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `division` | String | Division in the conference for the team. |
| `conference` | String | Conference of the team. |
| `season` | Int64 | Season (4-digit year). |
| `plays_off` | UInt32 | Plays run, with the team on offense. |
| `playsgame_off` | Float64 | Plays run per game, with the team on offense. |
| `passrate_off` | Float64 | Share of plays that were pass plays, with the team on offense. |
| `rushrate_off` | Float64 | Share of plays that were rush plays, with the team on offense. |
| `havoc_off` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag, with the team on offense. |
| `explosive_off` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag, with the team on offense. |
| `TEPA_off` | Float64 | Total EPA summed over every play, with the team on offense. |
| `EPAplay_off` | Float64 | EPA per play, with the team on offense. |
| `EPAdrive_off` | Float64 | EPA per drive (total EPA divided by drives), with the team on offense. |
| `EPAgame_off` | Float64 | EPA per game (total EPA divided by games), with the team on offense. |
| `yards_off` | Int64 | Total yards gained, with the team on offense. |
| `yardsplay_off` | Float64 | Yards gained per play, with the team on offense. |
| `yardsgame_off` | Float64 | Yards gained per game, with the team on offense. |
| `play_stuffed_off` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag, with the team on offense. |
| `drives_off` | UInt32 | Offensive drives, with the team on offense. |
| `drivesgame_off` | Float64 | Drives per game, with the team on offense. |
| `yardsdrive_off` | Float64 | Yards gained per drive, with the team on offense. |
| `playsdrive_off` | Float64 | Plays run per drive, with the team on offense. |
| `success_off` | Float64 | Success rate -- the share of plays flagged as successful by EPA, with the team on offense. |
| `red_zone_success_off` | Float64 | Success rate on red-zone plays, with the team on offense. |
| `third_down_success_off` | Float64 | Success rate on third-down plays, with the team on offense. |
| `third_down_distance_off` | Float64 | Average yards to go on third down, with the team on offense. |
| `late_down_success_off` | Float64 | Success rate on late-down plays, with the team on offense. |
| `early_down_EPA_off` | Float64 | EPA per early-down play, with the team on offense. |
| `start_position_off` | Float64 | Average drive start position, measured in yards from the opponent goal line, with the team on offense. |
| `nonExplosiveEpaPerPlay_off` | Float64 | EPA per play with explosive plays excluded, with the team on offense. |
| `line_yards_off` | Float64 | Average line yards credited to the offensive line on rushes, with the team on offense. |
| `opportunity_rate_off` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag, with the team on offense. |
| `playsgame_off_rank` | Float64 | National rank of the team's plays run per game with the team on offense, where 1 is best. |
| `TEPA_off_rank` | Float64 | National rank of the team's total EPA summed over every play with the team on offense, where 1 is best. |
| `EPAgame_off_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) with the team on offense, where 1 is best. |
| `EPAplay_off_rank` | Float64 | National rank of the team's EPA per play with the team on offense, where 1 is best. |
| `EPAdrive_off_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) with the team on offense, where 1 is best. |
| `early_down_EPA_off_rank` | Float64 | National rank of the team's EPA per early-down play with the team on offense, where 1 is best. |
| `success_off_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA with the team on offense, where 1 is best. |
| `yards_off_rank` | Float64 | National rank of the team's total yards gained with the team on offense, where 1 is best. |
| `yardsplay_off_rank` | Float64 | National rank of the team's yards gained per play with the team on offense, where 1 is best. |
| `yardsgame_off_rank` | Float64 | National rank of the team's yards gained per game with the team on offense, where 1 is best. |
| `drivesgame_off_rank` | Float64 | National rank of the team's drives per game with the team on offense, where 1 is best. |
| `yardsdrive_off_rank` | Float64 | National rank of the team's yards gained per drive with the team on offense, where 1 is best. |
| `playsdrive_off_rank` | Float64 | National rank of the team's plays run per drive with the team on offense, where 1 is best. |
| `play_stuffed_off_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag with the team on offense, where 1 is best. |
| `red_zone_success_off_rank` | Float64 | National rank of the team's success rate on red-zone plays with the team on offense, where 1 is best. |
| `third_down_success_off_rank` | Float64 | National rank of the team's success rate on third-down plays with the team on offense, where 1 is best. |
| `late_down_success_off_rank` | Float64 | National rank of the team's success rate on late-down plays with the team on offense, where 1 is best. |
| `third_down_distance_off_rank` | Float64 | National rank of the team's average yards to go on third down with the team on offense, where 1 is best. |
| `start_position_off_rank` | Float64 | National rank of the team's average drive start position, measured in yards from the opponent goal line with the team on offense, where 1 is best. |
| `havoc_off_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag with the team on offense, where 1 is best. |
| `explosive_off_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag with the team on offense, where 1 is best. |
| `passrate_off_rank` | Float64 | National rank of the team's share of plays that were pass plays with the team on offense, where 1 is best. |
| `rushrate_off_rank` | Float64 | National rank of the team's share of plays that were rush plays with the team on offense, where 1 is best. |
| `nonExplosiveEpaPerPlay_off_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded with the team on offense, where 1 is best. |
| `line_yards_off_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes with the team on offense, where 1 is best. |
| `opportunity_rate_off_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag with the team on offense, where 1 is best. |
| `plays_def` | UInt32 | Plays run, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def` | Float64 | Plays run per game, with the team on defense (i.e. allowed to opponents). |
| `passrate_def` | Float64 | Share of plays that were pass plays, with the team on defense (i.e. allowed to opponents). |
| `rushrate_def` | Float64 | Share of plays that were rush plays, with the team on defense (i.e. allowed to opponents). |
| `havoc_def` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag, with the team on defense (i.e. allowed to opponents). |
| `explosive_def` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag, with the team on defense (i.e. allowed to opponents). |
| `TEPA_def` | Float64 | Total EPA summed over every play, with the team on defense (i.e. allowed to opponents). |
| `EPAplay_def` | Float64 | EPA per play, with the team on defense (i.e. allowed to opponents). |
| `EPAdrive_def` | Float64 | EPA per drive (total EPA divided by drives), with the team on defense (i.e. allowed to opponents). |
| `EPAgame_def` | Float64 | EPA per game (total EPA divided by games), with the team on defense (i.e. allowed to opponents). |
| `yards_def` | Int64 | Total yards gained, with the team on defense (i.e. allowed to opponents). |
| `yardsplay_def` | Float64 | Yards gained per play, with the team on defense (i.e. allowed to opponents). |
| `yardsgame_def` | Float64 | Yards gained per game, with the team on defense (i.e. allowed to opponents). |
| `play_stuffed_def` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag, with the team on defense (i.e. allowed to opponents). |
| `drives_def` | UInt32 | Offensive drives, with the team on defense (i.e. allowed to opponents). |
| `drivesgame_def` | Float64 | Drives per game, with the team on defense (i.e. allowed to opponents). |
| `yardsdrive_def` | Float64 | Yards gained per drive, with the team on defense (i.e. allowed to opponents). |
| `playsdrive_def` | Float64 | Plays run per drive, with the team on defense (i.e. allowed to opponents). |
| `success_def` | Float64 | Success rate -- the share of plays flagged as successful by EPA, with the team on defense (i.e. allowed to opponents). |
| `red_zone_success_def` | Float64 | Success rate on red-zone plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_success_def` | Float64 | Success rate on third-down plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_distance_def` | Float64 | Average yards to go on third down, with the team on defense (i.e. allowed to opponents). |
| `late_down_success_def` | Float64 | Success rate on late-down plays, with the team on defense (i.e. allowed to opponents). |
| `early_down_EPA_def` | Float64 | EPA per early-down play, with the team on defense (i.e. allowed to opponents). |
| `start_position_def` | Float64 | Average drive start position, measured in yards from the opponent goal line, with the team on defense (i.e. allowed to opponents). |
| `nonExplosiveEpaPerPlay_def` | Float64 | EPA per play with explosive plays excluded, with the team on defense (i.e. allowed to opponents). |
| `line_yards_def` | Float64 | Average line yards credited to the offensive line on rushes, with the team on defense (i.e. allowed to opponents). |
| `opportunity_rate_def` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_rank` | Float64 | National rank of the team's plays run per game with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_def_rank` | Float64 | National rank of the team's total EPA summed over every play with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAgame_def_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAplay_def_rank` | Float64 | National rank of the team's EPA per play with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAdrive_def_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `early_down_EPA_def_rank` | Float64 | National rank of the team's EPA per early-down play with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `success_def_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yards_def_rank` | Float64 | National rank of the team's total yards gained with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsplay_def_rank` | Float64 | National rank of the team's yards gained per play with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsgame_def_rank` | Float64 | National rank of the team's yards gained per game with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `drivesgame_def_rank` | Float64 | National rank of the team's drives per game with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsdrive_def_rank` | Float64 | National rank of the team's yards gained per drive with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `playsdrive_def_rank` | Float64 | National rank of the team's plays run per drive with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `play_stuffed_def_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `red_zone_success_def_rank` | Float64 | National rank of the team's success rate on red-zone plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_success_def_rank` | Float64 | National rank of the team's success rate on third-down plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `late_down_success_def_rank` | Float64 | National rank of the team's success rate on late-down plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_distance_def_rank` | Float64 | National rank of the team's average yards to go on third down with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `start_position_def_rank` | Float64 | National rank of the team's average drive start position, measured in yards from the opponent goal line with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `havoc_def_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `explosive_def_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `passrate_def_rank` | Float64 | National rank of the team's share of plays that were pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `rushrate_def_rank` | Float64 | National rank of the team's share of plays that were rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `nonExplosiveEpaPerPlay_def_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `line_yards_def_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `opportunity_rate_def_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_margin` | Float64 | Margin in total EPA summed over every play: the team's offensive value minus the value it allowed on defense. |
| `EPAplay_margin` | Float64 | Margin in EPA per play: the team's offensive value minus the value it allowed on defense. |
| `EPAdrive_margin` | Float64 | Margin in EPA per drive (total EPA divided by drives): the team's offensive value minus the value it allowed on defense. |
| `EPAgame_margin` | Float64 | Margin in EPA per game (total EPA divided by games): the team's offensive value minus the value it allowed on defense. |
| `success_margin` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA: the team's offensive value minus the value it allowed on defense. |
| `yardsplay_margin` | Float64 | Margin in yards gained per play: the team's offensive value minus the value it allowed on defense. |
| `TEPA_margin_rank` | Float64 | Margin in total EPA summed over every play: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAgame_margin_rank` | Float64 | Margin in EPA per game (total EPA divided by games): the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAdrive_margin_rank` | Float64 | Margin in EPA per drive (total EPA divided by drives): the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAplay_margin_rank` | Float64 | Margin in EPA per play: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `success_margin_rank` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `yardsplay_margin_rank` | Float64 | Margin in yards gained per play: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `start_position_margin` | Float64 | Field-position margin: the team's own average starting field position minus the average starting field position it allowed, both measured as yards gained from their own goal line. Positive means the team started closer to scoring than its opponents. |
| `start_position_margin_rank` | Float64 | Field-position margin: the team's own average starting field position minus the average starting field position it allowed, both measured as yards gained from their own goal line. Positive means the team started closer to scoring than its opponents. National rank of that margin, 1 = largest. |
| `total_available_yards_off` | Float64 | Available yards are the yards a drive could theoretically gain, summed from each drive's starting distance to the opponent goal line. Total available yards on the team's own drives. |
| `total_gained_yards_off` | Int64 | Total yards the team actually gained across its own drives. |
| `available_yards_pct_off` | Float64 | Share of available yards the team's offense actually gained (total_gained_yards_off divided by total_available_yards_off). Higher is better. |
| `available_yards_pct_off_rank` | Float64 | National rank of the team's offensive available-yards share, where 1 is best. |
| `total_available_yards_def` | Float64 | Available yards are the yards a drive could theoretically gain, summed from each drive's starting distance to the opponent goal line. Total available yards on drives the team defended. |
| `total_gained_yards_def` | Int64 | Total yards the team allowed across the drives it defended. |
| `available_yards_pct_def` | Float64 | Share of available yards the team's defense allowed opponents to gain. Lower is better. |
| `available_yards_pct_def_rank` | Float64 | National rank of the team's defensive available-yards share, where 1 is best. |
| `total_available_yards_margin` | Float64 | Available yards on the team's own drives minus available yards on drives it defended. |
| `total_gained_yards_margin` | Int64 | Yards the team gained minus yards it allowed. |
| `available_yards_pct_margin` | Float64 | Available-yards share gained by the offense minus the share allowed by the defense. Higher is better. |
| `total_available_yards_margin_rank` | Float64 | National rank of total_available_yards_margin, 1 = largest margin. |
| `total_gained_yards_margin_rank` | Float64 | National rank of total_gained_yards_margin, 1 = largest margin. |
| `available_yards_pct_margin_rank` | Float64 | National rank of available_yards_pct_margin, 1 = largest margin. |
| `plays_off_pass` | UInt32 | Plays run on pass plays, with the team on offense. |
| `playsgame_off_pass` | Float64 | Plays run per game on pass plays, with the team on offense. |
| `passrate_off_pass` | Float64 | Share of plays that were pass plays on pass plays, with the team on offense. |
| `rushrate_off_pass` | Float64 | Share of plays that were rush plays on pass plays, with the team on offense. |
| `havoc_off_pass` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag on pass plays, with the team on offense. |
| `explosive_off_pass` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag on pass plays, with the team on offense. |
| `TEPA_off_pass` | Float64 | Total EPA summed over every play on pass plays, with the team on offense. |
| `EPAplay_off_pass` | Float64 | EPA per play on pass plays, with the team on offense. |
| `EPAdrive_off_pass` | Float64 | EPA per drive (total EPA divided by drives) on pass plays, with the team on offense. |
| `EPAgame_off_pass` | Float64 | EPA per game (total EPA divided by games) on pass plays, with the team on offense. |
| `yards_off_pass` | Int64 | Total yards gained on pass plays, with the team on offense. |
| `yardsplay_off_pass` | Float64 | Yards gained per play on pass plays, with the team on offense. |
| `yardsgame_off_pass` | Float64 | Yards gained per game on pass plays, with the team on offense. |
| `play_stuffed_off_pass` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag on pass plays, with the team on offense. |
| `drives_off_pass` | UInt32 | Offensive drives on pass plays, with the team on offense. |
| `drivesgame_off_pass` | Float64 | Drives per game on pass plays, with the team on offense. |
| `yardsdrive_off_pass` | Float64 | Yards gained per drive on pass plays, with the team on offense. |
| `playsdrive_off_pass` | Float64 | Plays run per drive on pass plays, with the team on offense. |
| `success_off_pass` | Float64 | Success rate -- the share of plays flagged as successful by EPA on pass plays, with the team on offense. |
| `red_zone_success_off_pass` | Float64 | Success rate on red-zone plays on pass plays, with the team on offense. |
| `third_down_success_off_pass` | Float64 | Success rate on third-down plays on pass plays, with the team on offense. |
| `third_down_distance_off_pass` | Float64 | Average yards to go on third down on pass plays, with the team on offense. |
| `late_down_success_off_pass` | Float64 | Success rate on late-down plays on pass plays, with the team on offense. |
| `early_down_EPA_off_pass` | Float64 | EPA per early-down play on pass plays, with the team on offense. |
| `nonExplosiveEpaPerPlay_off_pass` | Float64 | EPA per play with explosive plays excluded on pass plays, with the team on offense. |
| `line_yards_off_pass` | Float64 | Average line yards credited to the offensive line on rushes on pass plays, with the team on offense. |
| `opportunity_rate_off_pass` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag on pass plays, with the team on offense. |
| `playsgame_off_pass_rank` | Float64 | National rank of the team's plays run per game on pass plays with the team on offense, where 1 is best. |
| `TEPA_off_pass_rank` | Float64 | National rank of the team's total EPA summed over every play on pass plays with the team on offense, where 1 is best. |
| `EPAgame_off_pass_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) on pass plays with the team on offense, where 1 is best. |
| `EPAplay_off_pass_rank` | Float64 | National rank of the team's EPA per play on pass plays with the team on offense, where 1 is best. |
| `EPAdrive_off_pass_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) on pass plays with the team on offense, where 1 is best. |
| `early_down_EPA_off_pass_rank` | Float64 | National rank of the team's EPA per early-down play on pass plays with the team on offense, where 1 is best. |
| `success_off_pass_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA on pass plays with the team on offense, where 1 is best. |
| `yards_off_pass_rank` | Float64 | National rank of the team's total yards gained on pass plays with the team on offense, where 1 is best. |
| `yardsplay_off_pass_rank` | Float64 | National rank of the team's yards gained per play on pass plays with the team on offense, where 1 is best. |
| `yardsgame_off_pass_rank` | Float64 | National rank of the team's yards gained per game on pass plays with the team on offense, where 1 is best. |
| `drivesgame_off_pass_rank` | Float64 | National rank of the team's drives per game on pass plays with the team on offense, where 1 is best. |
| `yardsdrive_off_pass_rank` | Float64 | National rank of the team's yards gained per drive on pass plays with the team on offense, where 1 is best. |
| `playsdrive_off_pass_rank` | Float64 | National rank of the team's plays run per drive on pass plays with the team on offense, where 1 is best. |
| `play_stuffed_off_pass_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag on pass plays with the team on offense, where 1 is best. |
| `red_zone_success_off_pass_rank` | Float64 | National rank of the team's success rate on red-zone plays on pass plays with the team on offense, where 1 is best. |
| `third_down_success_off_pass_rank` | Float64 | National rank of the team's success rate on third-down plays on pass plays with the team on offense, where 1 is best. |
| `late_down_success_off_pass_rank` | Float64 | National rank of the team's success rate on late-down plays on pass plays with the team on offense, where 1 is best. |
| `third_down_distance_off_pass_rank` | Float64 | National rank of the team's average yards to go on third down on pass plays with the team on offense, where 1 is best. |
| `havoc_off_pass_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag on pass plays with the team on offense, where 1 is best. |
| `explosive_off_pass_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag on pass plays with the team on offense, where 1 is best. |
| `passrate_off_pass_rank` | Float64 | National rank of the team's share of plays that were pass plays on pass plays with the team on offense, where 1 is best. |
| `rushrate_off_pass_rank` | Float64 | National rank of the team's share of plays that were rush plays on pass plays with the team on offense, where 1 is best. |
| `nonExplosiveEpaPerPlay_off_pass_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded on pass plays with the team on offense, where 1 is best. |
| `line_yards_off_pass_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes on pass plays with the team on offense, where 1 is best. |
| `opportunity_rate_off_pass_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag on pass plays with the team on offense, where 1 is best. |
| `plays_def_pass` | UInt32 | Plays run on pass plays, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_pass` | Float64 | Plays run per game on pass plays, with the team on defense (i.e. allowed to opponents). |
| `passrate_def_pass` | Float64 | Share of plays that were pass plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `rushrate_def_pass` | Float64 | Share of plays that were rush plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `havoc_def_pass` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag on pass plays, with the team on defense (i.e. allowed to opponents). |
| `explosive_def_pass` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag on pass plays, with the team on defense (i.e. allowed to opponents). |
| `TEPA_def_pass` | Float64 | Total EPA summed over every play on pass plays, with the team on defense (i.e. allowed to opponents). |
| `EPAplay_def_pass` | Float64 | EPA per play on pass plays, with the team on defense (i.e. allowed to opponents). |
| `EPAdrive_def_pass` | Float64 | EPA per drive (total EPA divided by drives) on pass plays, with the team on defense (i.e. allowed to opponents). |
| `EPAgame_def_pass` | Float64 | EPA per game (total EPA divided by games) on pass plays, with the team on defense (i.e. allowed to opponents). |
| `yards_def_pass` | Int64 | Total yards gained on pass plays, with the team on defense (i.e. allowed to opponents). |
| `yardsplay_def_pass` | Float64 | Yards gained per play on pass plays, with the team on defense (i.e. allowed to opponents). |
| `yardsgame_def_pass` | Float64 | Yards gained per game on pass plays, with the team on defense (i.e. allowed to opponents). |
| `play_stuffed_def_pass` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag on pass plays, with the team on defense (i.e. allowed to opponents). |
| `drives_def_pass` | UInt32 | Offensive drives on pass plays, with the team on defense (i.e. allowed to opponents). |
| `drivesgame_def_pass` | Float64 | Drives per game on pass plays, with the team on defense (i.e. allowed to opponents). |
| `yardsdrive_def_pass` | Float64 | Yards gained per drive on pass plays, with the team on defense (i.e. allowed to opponents). |
| `playsdrive_def_pass` | Float64 | Plays run per drive on pass plays, with the team on defense (i.e. allowed to opponents). |
| `success_def_pass` | Float64 | Success rate -- the share of plays flagged as successful by EPA on pass plays, with the team on defense (i.e. allowed to opponents). |
| `red_zone_success_def_pass` | Float64 | Success rate on red-zone plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_success_def_pass` | Float64 | Success rate on third-down plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_distance_def_pass` | Float64 | Average yards to go on third down on pass plays, with the team on defense (i.e. allowed to opponents). |
| `late_down_success_def_pass` | Float64 | Success rate on late-down plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `early_down_EPA_def_pass` | Float64 | EPA per early-down play on pass plays, with the team on defense (i.e. allowed to opponents). |
| `nonExplosiveEpaPerPlay_def_pass` | Float64 | EPA per play with explosive plays excluded on pass plays, with the team on defense (i.e. allowed to opponents). |
| `line_yards_def_pass` | Float64 | Average line yards credited to the offensive line on rushes on pass plays, with the team on defense (i.e. allowed to opponents). |
| `opportunity_rate_def_pass` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag on pass plays, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_pass_rank` | Float64 | National rank of the team's plays run per game on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_def_pass_rank` | Float64 | National rank of the team's total EPA summed over every play on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAgame_def_pass_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAplay_def_pass_rank` | Float64 | National rank of the team's EPA per play on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAdrive_def_pass_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `early_down_EPA_def_pass_rank` | Float64 | National rank of the team's EPA per early-down play on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `success_def_pass_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yards_def_pass_rank` | Float64 | National rank of the team's total yards gained on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsplay_def_pass_rank` | Float64 | National rank of the team's yards gained per play on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsgame_def_pass_rank` | Float64 | National rank of the team's yards gained per game on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `drivesgame_def_pass_rank` | Float64 | National rank of the team's drives per game on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsdrive_def_pass_rank` | Float64 | National rank of the team's yards gained per drive on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `playsdrive_def_pass_rank` | Float64 | National rank of the team's plays run per drive on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `play_stuffed_def_pass_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `red_zone_success_def_pass_rank` | Float64 | National rank of the team's success rate on red-zone plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_success_def_pass_rank` | Float64 | National rank of the team's success rate on third-down plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `late_down_success_def_pass_rank` | Float64 | National rank of the team's success rate on late-down plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_distance_def_pass_rank` | Float64 | National rank of the team's average yards to go on third down on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `havoc_def_pass_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `explosive_def_pass_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `passrate_def_pass_rank` | Float64 | National rank of the team's share of plays that were pass plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `rushrate_def_pass_rank` | Float64 | National rank of the team's share of plays that were rush plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `nonExplosiveEpaPerPlay_def_pass_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `line_yards_def_pass_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `opportunity_rate_def_pass_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_margin_pass` | Float64 | Margin in total EPA summed over every play on pass plays: the team's offensive value minus the value it allowed on defense. |
| `EPAplay_margin_pass` | Float64 | Margin in EPA per play on pass plays: the team's offensive value minus the value it allowed on defense. |
| `EPAdrive_margin_pass` | Float64 | Margin in EPA per drive (total EPA divided by drives) on pass plays: the team's offensive value minus the value it allowed on defense. |
| `EPAgame_margin_pass` | Float64 | Margin in EPA per game (total EPA divided by games) on pass plays: the team's offensive value minus the value it allowed on defense. |
| `success_margin_pass` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA on pass plays: the team's offensive value minus the value it allowed on defense. |
| `yardsplay_margin_pass` | Float64 | Margin in yards gained per play on pass plays: the team's offensive value minus the value it allowed on defense. |
| `TEPA_margin_pass_rank` | Float64 | Margin in total EPA summed over every play on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAgame_margin_pass_rank` | Float64 | Margin in EPA per game (total EPA divided by games) on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAdrive_margin_pass_rank` | Float64 | Margin in EPA per drive (total EPA divided by drives) on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAplay_margin_pass_rank` | Float64 | Margin in EPA per play on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `success_margin_pass_rank` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `yardsplay_margin_pass_rank` | Float64 | Margin in yards gained per play on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `plays_off_rush` | UInt32 | Plays run on rush plays, with the team on offense. |
| `playsgame_off_rush` | Float64 | Plays run per game on rush plays, with the team on offense. |
| `passrate_off_rush` | Float64 | Share of plays that were pass plays on rush plays, with the team on offense. |
| `rushrate_off_rush` | Float64 | Share of plays that were rush plays on rush plays, with the team on offense. |
| `havoc_off_rush` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag on rush plays, with the team on offense. |
| `explosive_off_rush` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag on rush plays, with the team on offense. |
| `TEPA_off_rush` | Float64 | Total EPA summed over every play on rush plays, with the team on offense. |
| `EPAplay_off_rush` | Float64 | EPA per play on rush plays, with the team on offense. |
| `EPAdrive_off_rush` | Float64 | EPA per drive (total EPA divided by drives) on rush plays, with the team on offense. |
| `EPAgame_off_rush` | Float64 | EPA per game (total EPA divided by games) on rush plays, with the team on offense. |
| `yards_off_rush` | Int64 | Total yards gained on rush plays, with the team on offense. |
| `yardsplay_off_rush` | Float64 | Yards gained per play on rush plays, with the team on offense. |
| `yardsgame_off_rush` | Float64 | Yards gained per game on rush plays, with the team on offense. |
| `play_stuffed_off_rush` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag on rush plays, with the team on offense. |
| `drives_off_rush` | UInt32 | Offensive drives on rush plays, with the team on offense. |
| `drivesgame_off_rush` | Float64 | Drives per game on rush plays, with the team on offense. |
| `yardsdrive_off_rush` | Float64 | Yards gained per drive on rush plays, with the team on offense. |
| `playsdrive_off_rush` | Float64 | Plays run per drive on rush plays, with the team on offense. |
| `success_off_rush` | Float64 | Success rate -- the share of plays flagged as successful by EPA on rush plays, with the team on offense. |
| `red_zone_success_off_rush` | Float64 | Success rate on red-zone plays on rush plays, with the team on offense. |
| `third_down_success_off_rush` | Float64 | Success rate on third-down plays on rush plays, with the team on offense. |
| `third_down_distance_off_rush` | Float64 | Average yards to go on third down on rush plays, with the team on offense. |
| `late_down_success_off_rush` | Float64 | Success rate on late-down plays on rush plays, with the team on offense. |
| `early_down_EPA_off_rush` | Float64 | EPA per early-down play on rush plays, with the team on offense. |
| `nonExplosiveEpaPerPlay_off_rush` | Float64 | EPA per play with explosive plays excluded on rush plays, with the team on offense. |
| `line_yards_off_rush` | Float64 | Average line yards credited to the offensive line on rushes on rush plays, with the team on offense. |
| `opportunity_rate_off_rush` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag on rush plays, with the team on offense. |
| `playsgame_off_rush_rank` | Float64 | National rank of the team's plays run per game on rush plays with the team on offense, where 1 is best. |
| `TEPA_off_rush_rank` | Float64 | National rank of the team's total EPA summed over every play on rush plays with the team on offense, where 1 is best. |
| `EPAgame_off_rush_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) on rush plays with the team on offense, where 1 is best. |
| `EPAplay_off_rush_rank` | Float64 | National rank of the team's EPA per play on rush plays with the team on offense, where 1 is best. |
| `EPAdrive_off_rush_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) on rush plays with the team on offense, where 1 is best. |
| `early_down_EPA_off_rush_rank` | Float64 | National rank of the team's EPA per early-down play on rush plays with the team on offense, where 1 is best. |
| `success_off_rush_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA on rush plays with the team on offense, where 1 is best. |
| `yards_off_rush_rank` | Float64 | National rank of the team's total yards gained on rush plays with the team on offense, where 1 is best. |
| `yardsplay_off_rush_rank` | Float64 | National rank of the team's yards gained per play on rush plays with the team on offense, where 1 is best. |
| `yardsgame_off_rush_rank` | Float64 | National rank of the team's yards gained per game on rush plays with the team on offense, where 1 is best. |
| `drivesgame_off_rush_rank` | Float64 | National rank of the team's drives per game on rush plays with the team on offense, where 1 is best. |
| `yardsdrive_off_rush_rank` | Float64 | National rank of the team's yards gained per drive on rush plays with the team on offense, where 1 is best. |
| `playsdrive_off_rush_rank` | Float64 | National rank of the team's plays run per drive on rush plays with the team on offense, where 1 is best. |
| `play_stuffed_off_rush_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag on rush plays with the team on offense, where 1 is best. |
| `red_zone_success_off_rush_rank` | Float64 | National rank of the team's success rate on red-zone plays on rush plays with the team on offense, where 1 is best. |
| `third_down_success_off_rush_rank` | Float64 | National rank of the team's success rate on third-down plays on rush plays with the team on offense, where 1 is best. |
| `late_down_success_off_rush_rank` | Float64 | National rank of the team's success rate on late-down plays on rush plays with the team on offense, where 1 is best. |
| `third_down_distance_off_rush_rank` | Float64 | National rank of the team's average yards to go on third down on rush plays with the team on offense, where 1 is best. |
| `havoc_off_rush_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag on rush plays with the team on offense, where 1 is best. |
| `explosive_off_rush_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag on rush plays with the team on offense, where 1 is best. |
| `passrate_off_rush_rank` | Float64 | National rank of the team's share of plays that were pass plays on rush plays with the team on offense, where 1 is best. |
| `rushrate_off_rush_rank` | Float64 | National rank of the team's share of plays that were rush plays on rush plays with the team on offense, where 1 is best. |
| `nonExplosiveEpaPerPlay_off_rush_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded on rush plays with the team on offense, where 1 is best. |
| `line_yards_off_rush_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes on rush plays with the team on offense, where 1 is best. |
| `opportunity_rate_off_rush_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag on rush plays with the team on offense, where 1 is best. |
| `plays_def_rush` | UInt32 | Plays run on rush plays, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_rush` | Float64 | Plays run per game on rush plays, with the team on defense (i.e. allowed to opponents). |
| `passrate_def_rush` | Float64 | Share of plays that were pass plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `rushrate_def_rush` | Float64 | Share of plays that were rush plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `havoc_def_rush` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag on rush plays, with the team on defense (i.e. allowed to opponents). |
| `explosive_def_rush` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag on rush plays, with the team on defense (i.e. allowed to opponents). |
| `TEPA_def_rush` | Float64 | Total EPA summed over every play on rush plays, with the team on defense (i.e. allowed to opponents). |
| `EPAplay_def_rush` | Float64 | EPA per play on rush plays, with the team on defense (i.e. allowed to opponents). |
| `EPAdrive_def_rush` | Float64 | EPA per drive (total EPA divided by drives) on rush plays, with the team on defense (i.e. allowed to opponents). |
| `EPAgame_def_rush` | Float64 | EPA per game (total EPA divided by games) on rush plays, with the team on defense (i.e. allowed to opponents). |
| `yards_def_rush` | Int64 | Total yards gained on rush plays, with the team on defense (i.e. allowed to opponents). |
| `yardsplay_def_rush` | Float64 | Yards gained per play on rush plays, with the team on defense (i.e. allowed to opponents). |
| `yardsgame_def_rush` | Float64 | Yards gained per game on rush plays, with the team on defense (i.e. allowed to opponents). |
| `play_stuffed_def_rush` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag on rush plays, with the team on defense (i.e. allowed to opponents). |
| `drives_def_rush` | UInt32 | Offensive drives on rush plays, with the team on defense (i.e. allowed to opponents). |
| `drivesgame_def_rush` | Float64 | Drives per game on rush plays, with the team on defense (i.e. allowed to opponents). |
| `yardsdrive_def_rush` | Float64 | Yards gained per drive on rush plays, with the team on defense (i.e. allowed to opponents). |
| `playsdrive_def_rush` | Float64 | Plays run per drive on rush plays, with the team on defense (i.e. allowed to opponents). |
| `success_def_rush` | Float64 | Success rate -- the share of plays flagged as successful by EPA on rush plays, with the team on defense (i.e. allowed to opponents). |
| `red_zone_success_def_rush` | Float64 | Success rate on red-zone plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_success_def_rush` | Float64 | Success rate on third-down plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_distance_def_rush` | Float64 | Average yards to go on third down on rush plays, with the team on defense (i.e. allowed to opponents). |
| `late_down_success_def_rush` | Float64 | Success rate on late-down plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `early_down_EPA_def_rush` | Float64 | EPA per early-down play on rush plays, with the team on defense (i.e. allowed to opponents). |
| `nonExplosiveEpaPerPlay_def_rush` | Float64 | EPA per play with explosive plays excluded on rush plays, with the team on defense (i.e. allowed to opponents). |
| `line_yards_def_rush` | Float64 | Average line yards credited to the offensive line on rushes on rush plays, with the team on defense (i.e. allowed to opponents). |
| `opportunity_rate_def_rush` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag on rush plays, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_rush_rank` | Float64 | National rank of the team's plays run per game on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_def_rush_rank` | Float64 | National rank of the team's total EPA summed over every play on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAgame_def_rush_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAplay_def_rush_rank` | Float64 | National rank of the team's EPA per play on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAdrive_def_rush_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `early_down_EPA_def_rush_rank` | Float64 | National rank of the team's EPA per early-down play on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `success_def_rush_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yards_def_rush_rank` | Float64 | National rank of the team's total yards gained on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsplay_def_rush_rank` | Float64 | National rank of the team's yards gained per play on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsgame_def_rush_rank` | Float64 | National rank of the team's yards gained per game on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `drivesgame_def_rush_rank` | Float64 | National rank of the team's drives per game on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsdrive_def_rush_rank` | Float64 | National rank of the team's yards gained per drive on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `playsdrive_def_rush_rank` | Float64 | National rank of the team's plays run per drive on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `play_stuffed_def_rush_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `red_zone_success_def_rush_rank` | Float64 | National rank of the team's success rate on red-zone plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_success_def_rush_rank` | Float64 | National rank of the team's success rate on third-down plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `late_down_success_def_rush_rank` | Float64 | National rank of the team's success rate on late-down plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_distance_def_rush_rank` | Float64 | National rank of the team's average yards to go on third down on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `havoc_def_rush_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `explosive_def_rush_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `passrate_def_rush_rank` | Float64 | National rank of the team's share of plays that were pass plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `rushrate_def_rush_rank` | Float64 | National rank of the team's share of plays that were rush plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `nonExplosiveEpaPerPlay_def_rush_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `line_yards_def_rush_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `opportunity_rate_def_rush_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_margin_rush` | Float64 | Margin in total EPA summed over every play on rush plays: the team's offensive value minus the value it allowed on defense. |
| `EPAplay_margin_rush` | Float64 | Margin in EPA per play on rush plays: the team's offensive value minus the value it allowed on defense. |
| `EPAdrive_margin_rush` | Float64 | Margin in EPA per drive (total EPA divided by drives) on rush plays: the team's offensive value minus the value it allowed on defense. |
| `EPAgame_margin_rush` | Float64 | Margin in EPA per game (total EPA divided by games) on rush plays: the team's offensive value minus the value it allowed on defense. |
| `success_margin_rush` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA on rush plays: the team's offensive value minus the value it allowed on defense. |
| `yardsplay_margin_rush` | Float64 | Margin in yards gained per play on rush plays: the team's offensive value minus the value it allowed on defense. |
| `TEPA_margin_rush_rank` | Float64 | Margin in total EPA summed over every play on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAgame_margin_rush_rank` | Float64 | Margin in EPA per game (total EPA divided by games) on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAdrive_margin_rush_rank` | Float64 | Margin in EPA per drive (total EPA divided by drives) on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAplay_margin_rush_rank` | Float64 | Margin in EPA per play on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `success_margin_rush_rank` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `yardsplay_margin_rush_rank` | Float64 | Margin in yards gained per play on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `fbs_class` | String | Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, derived from conference membership (Notre Dame is classified with the power group). Null for teams outside FBS. |
| `valid_games` | UInt32 | Number of the team's games that produced both an offensive and a defensive adjusted-EPA value; teams below two valid games are dropped from the adjusted ratings. |
| `adj_off_epa` | Float64 | Offensive opponent-adjusted EPA per play from the ridge (RAPM-style) regression on offense/defense team indicators plus home field -- cfbfastR's adjust_epa adjustment, fit in-sample across the season, so the value is descriptive of that window rather than predictive. |
| `adj_def_epa` | Float64 | Defensive opponent-adjusted EPA per play from the ridge (RAPM-style) regression on offense/defense team indicators plus home field -- cfbfastR's adjust_epa adjustment, fit in-sample across the season, so the value is descriptive of that window rather than predictive. Lower is better -- it is EPA allowed. |
| `def_strength_faced` | Float64 | Average opponent-offense strength the team's defense faced, taken as the mean of the ridge's offensive coefficients across its opponents. Higher means a tougher slate. |
| `off_strength_faced` | Float64 | Average opponent-defense strength the team's offense faced, taken as the mean of the ridge's defensive coefficients across its opponents. Higher means a tougher slate. |
| `net_adj_epa` | Float64 | Net opponent-adjusted EPA per play: adj_off_epa minus adj_def_epa. Higher is better. |
| `adj_off_epa_rank` | Float64 | National rank of the team's adj_off_epa, where 1 is best. |
| `adj_def_epa_rank` | Float64 | National rank of the team's adj_def_epa, where 1 is best (fewest EPA allowed). |
| `net_adj_epa_rank` | Float64 | National rank of the team's net_adj_epa, 1 = largest net adjusted EPA. |

```python
load_cfb_team_summaries(seasons=2024)
```

## `load_cfb_adv_team_gamelog`

Release: [espn_cfb_adv_team_gamelog](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_cfb_adv_team_gamelog) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_adv_team_gamelog/adv_team_gamelog_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `week` | Int64 | Game week of the season. |
| `season_type` | Int32 | ESPN season type (2 = regular, 3 = postseason). |
| `game_id` | Int64 | ESPN game identifier. |
| `start_date` | String | Season start timestamp (ISO 8601, UTC). |
| `team_id` | Int64 | ESPN team id. |
| `team` | String | Team name. |
| `opponent_id` | Int64 | ESPN team id of the opponent. |
| `opponent` | String | Opponent team name. |
| `is_home` | Boolean | Whether the subject team was the home team. |
| `neutral_site` | Boolean | TRUE/FALSE flag for if the game took place at a neutral site. |
| `points_for` | Int64 | Goals/points scored. |
| `points_against` | Int64 | Points allowed. |
| `margin` | Int64 |  |
| `win` | Boolean | Whether the game was a win (goalie). |
| `rushing_highlight_yards_per_opp` | Float64 | Highlight yards per rushing opportunity. |
| `total_pen_yards` | Int64 | Total penalty yards assessed. |
| `EPA_penalty` | Float64 | Total EPA attributed to penalties. |
| `penalty_first_downs_created` | Int64 | Number of first downs the team gained via opponent penalty. |
| `penalty_first_downs_created_rate` | Float64 | Share of the team's first downs that came via opponent penalty. |
| `penalties` | Int64 | Total number of penalties. |
| `penalty_yards` | Int64 | Yards gained (or lost) by the posteam from the penalty. |
| `special_teams_plays` | Int64 | Number of special-teams plays. |
| `EPA_sp` | Float64 | Total special-teams EPA, ESPN's abbreviated field for the same phase. |
| `EPA_special_teams` | Float64 | Total EPA generated on special-teams plays. |
| `field_goals` | Int64 | Number of field-goal attempts. |
| `EPA_fg` | Float64 | Total EPA on field-goal attempts. |
| `punt_plays` | Int64 | Number of punt plays. |
| `EPA_punt` | Float64 | Total EPA on punt plays. |
| `kickoff_plays` | Int64 | Number of kickoff plays. |
| `EPA_kickoff` | Float64 | Total EPA on kickoff plays. |
| `rushes` | Int64 | Number of rushing attempts. |
| `rush_yards` | Float64 | The number of rushing yards gained |
| `yards_per_rush` | Float64 | Yards gained per rushing attempt. |
| `rushing_power_rate` | Float64 | Share of carries that were power rushing attempts. |
| `rushing_first_downs_created` | Int64 | Number of first downs created on rush plays. |
| `rushing_first_downs_created_rate` | Float64 | Share of rush plays that created a first down. |
| `EPA_rushing_overall` | Float64 | Total EPA on rush plays. |
| `EPA_rushing_per_play` | Float64 | EPA per rush play. |
| `EPA_explosive_rushing` | Int64 | Count of explosive rush plays. A play count, not an EPA total. |
| `EPA_explosive_rushing_rate` | Float64 | Explosive-play rate on rush plays, over ESPN's qualifying-play denominator. |
| `EPA_non_explosive_rushing` | Float64 | Total EPA on rush plays with explosive plays excluded. |
| `EPA_non_explosive_rushing_per_play` | Float64 | EPA per rush play with explosive plays excluded. |
| `passes` | Int64 | Passes. |
| `pass_yards` | Float64 | Number of yards gained on pass plays |
| `yards_per_pass` | Float64 | Team game yards per pass. |
| `passing_first_downs_created` | Int64 | Number of first downs created on pass plays. |
| `passing_first_downs_created_rate` | Float64 | Share of pass plays that created a first down. |
| `EPA_passing_overall` | Float64 | Total EPA on pass plays. |
| `EPA_passing_per_play` | Float64 | EPA per pass play. |
| `EPA_explosive_passing` | Int64 | Count of explosive pass plays. A play count, not an EPA total. |
| `EPA_explosive_passing_rate` | Float64 | Explosive-play rate on pass plays, over ESPN's qualifying-play denominator. |
| `EPA_non_explosive_passing` | Float64 | Total EPA on pass plays with explosive plays excluded. |
| `EPA_non_explosive_passing_per_play` | Float64 | EPA per pass play with explosive plays excluded. |
| `scrimmage_plays` | Int64 | Number of plays from scrimmage (rushes plus passes), excluding special teams. |
| `EPA_overall_off` | Float64 | Total offensive EPA for the team. Duplicated exactly by EPA_overall_offense in every published season checked -- prefer one and ignore the other. |
| `EPA_overall_offense` | Float64 | Total offensive EPA. An exact duplicate of EPA_overall_off. |
| `EPA_per_play` | Float64 | Offensive EPA per play. |
| `EPA_non_explosive` | Float64 | Total EPA with explosive plays excluded, isolating the team's routine-down production. |
| `EPA_non_explosive_per_play` | Float64 | EPA per play with explosive plays excluded. |
| `EPA_explosive` | Int64 | Count of explosive plays, per ESPN's advanced box score. Despite the EPA_ prefix this is a play COUNT, not an EPA total. |
| `EPA_explosive_rate` | Float64 | Explosive-play rate. Note this is NOT EPA_explosive divided by EPA_plays -- ESPN divides by its own smaller qualifying-play count, so deriving it yourself will not reproduce this value. |
| `passes_rate` | Float64 | Share of the team's plays from scrimmage that were pass plays. |
| `off_yards` | Int64 | Offensive yards gained from scrimmage. |
| `total_off_yards` | Int64 | Total offensive yards across all plays. |
| `yards_per_play` | Float64 | Yards gained per play. |
| `EPA_plays` | Int64 | Number of plays ESPN's advanced box score scored for the team. |
| `total_yards` | Int64 | Team total yards. |
| `EPA_overall_total` | Float64 | Total EPA across all phases, which is why it differs from the offense-only EPA_overall_off. |
| `rushes_rate` | Float64 | Share of the team's plays from scrimmage that were rush plays. |
| `first_downs_created` | Int64 | Number of first downs the team created. |
| `first_downs_created_rate` | Float64 | Share of the team's plays that created a first down. |
| `EPA_rushing_power` | Float64 | Total EPA on power rushing situations, as classified by ESPN's advanced box score. |
| `EPA_rushing_power_per_play` | Float64 | EPA per play on power rushing situations. |
| `rushing_power_success` | Int64 | Rushing power success rate. |
| `rushing_power_success_rate` | Float64 | Share of power rushing attempts that succeeded. |
| `rushing_power` | Int64 | Count of power rushing attempts, in short-yardage situations as classified by ESPN's advanced box score. |
| `rushing_stuff` | Int64 | Count of stuffed rushing attempts. |
| `rushing_stuff_rate` | Float64 | Rushing stuff rate. |
| `rushing_stopped` | Int64 | Count of rushing attempts stopped at or behind the line of scrimmage. |
| `rushing_stopped_rate` | Float64 | Share of carries stopped at or behind the line of scrimmage. |
| `rushing_opportunity` | Int64 | Count of rushing opportunities -- carries that reached ESPN's opportunity threshold. |
| `rushing_opportunity_rate` | Float64 | Share of carries that qualified as rushing opportunities. |
| `rushing_highlight` | Int64 | Highlight yards -- rushing yardage credited to the back rather than the offensive line. |
| `rushing_highlight_rate` | Float64 | Share of rushing yardage that was highlight (back-credited) yardage. |
| `rushing_highlight_yards` | Float64 | Opponent-adjusted offensive highlight yards per opportunity rush. |
| `line_yards` | Float64 | Line yards -- the portion of rushing yardage credited to the offensive line under the standard rushing decomposition. ESPN applies its own qualifying threshold for the yardage split. |
| `line_yards_per_carry` | Float64 | Line yards per rushing attempt. |
| `second_level_yards` | Float64 | Second-level yards -- rushing yardage earned just beyond the line of scrimmage. |
| `open_field_yards` | Float64 | Open-field yards -- rushing yardage earned well downfield, past the second level. |

```python
load_cfb_adv_team_gamelog(seasons=2024)
```

## `load_cfb_ratings_weekly`

Release: [cfb_ratings_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_ratings_weekly) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_ratings_weekly/cfb_ratings_weekly_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season (4-digit year). |
| `team_id` | Int64 | ESPN team id. |
| `adj_off_epa` | Float64 |  |
| `adj_def_epa` | Float64 |  |
| `adj_st_epa` | Float64 |  |
| `adj_net` | Float64 |  |
| `fei_off` | Float64 |  |
| `fei_def` | Float64 |  |
| `fei_net` | Float64 |  |
| `games` | Int64 | Number of games included in the ATS summary. |
| `off_pace` | Float64 |  |
| `off_rank` | Int64 |  |
| `def_rank` | Int64 |  |
| `net_rank` | Int64 |  |
| `net_z` | Float64 |  |
| `through_week` | Int32 |  |

```python
load_cfb_ratings_weekly(seasons=2024)
```

## `load_cfb_team_summaries_weekly`

Release: [cfb_team_summaries_weekly](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/cfb_team_summaries_weekly) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_team_summaries_weekly/cfb_team_summaries_weekly_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | ESPN team id. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `division` | String | Division in the conference for the team. |
| `conference` | String | Conference of the team. |
| `season` | Int64 | Season (4-digit year). |
| `plays_off` | UInt32 | Plays run, with the team on offense. |
| `passrate_off` | Float64 | Share of plays that were pass plays, with the team on offense. |
| `rushrate_off` | Float64 | Share of plays that were rush plays, with the team on offense. |
| `havoc_off` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag, with the team on offense. |
| `explosive_off` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag, with the team on offense. |
| `TEPA_off` | Float64 | Total EPA summed over every play, with the team on offense. |
| `EPAplay_off` | Float64 | EPA per play, with the team on offense. |
| `yards_off` | Int64 | Total yards gained, with the team on offense. |
| `yardsplay_off` | Float64 | Yards gained per play, with the team on offense. |
| `play_stuffed_off` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag, with the team on offense. |
| `success_off` | Float64 | Success rate -- the share of plays flagged as successful by EPA, with the team on offense. |
| `red_zone_success_off` | Float64 | Success rate on red-zone plays, with the team on offense. |
| `third_down_success_off` | Float64 | Success rate on third-down plays, with the team on offense. |
| `third_down_distance_off` | Float64 | Average yards to go on third down, with the team on offense. |
| `late_down_success_off` | Float64 | Success rate on late-down plays, with the team on offense. |
| `early_down_EPA_off` | Float64 | EPA per early-down play, with the team on offense. |
| `start_position_off` | Float64 | Average drive start position, measured in yards from the opponent goal line, with the team on offense. |
| `nonExplosiveEpaPerPlay_off` | Float64 | EPA per play with explosive plays excluded, with the team on offense. |
| `line_yards_off` | Float64 | Average line yards credited to the offensive line on rushes, with the team on offense. |
| `opportunity_rate_off` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag, with the team on offense. |
| `playsgame_off` | Float64 | Plays run per game, with the team on offense. |
| `EPAdrive_off` | Float64 | EPA per drive (total EPA divided by drives), with the team on offense. |
| `EPAgame_off` | Float64 | EPA per game (total EPA divided by games), with the team on offense. |
| `yardsgame_off` | Float64 | Yards gained per game, with the team on offense. |
| `drives_off` | UInt32 | Offensive drives, with the team on offense. |
| `drivesgame_off` | Float64 | Drives per game, with the team on offense. |
| `yardsdrive_off` | Float64 | Yards gained per drive, with the team on offense. |
| `playsdrive_off` | Float64 | Plays run per drive, with the team on offense. |
| `playsgame_off_rank` | Float64 | National rank of the team's plays run per game with the team on offense, where 1 is best. |
| `TEPA_off_rank` | Float64 | National rank of the team's total EPA summed over every play with the team on offense, where 1 is best. |
| `EPAgame_off_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) with the team on offense, where 1 is best. |
| `EPAplay_off_rank` | Float64 | National rank of the team's EPA per play with the team on offense, where 1 is best. |
| `EPAdrive_off_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) with the team on offense, where 1 is best. |
| `early_down_EPA_off_rank` | Float64 | National rank of the team's EPA per early-down play with the team on offense, where 1 is best. |
| `success_off_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA with the team on offense, where 1 is best. |
| `yards_off_rank` | Float64 | National rank of the team's total yards gained with the team on offense, where 1 is best. |
| `yardsplay_off_rank` | Float64 | National rank of the team's yards gained per play with the team on offense, where 1 is best. |
| `yardsgame_off_rank` | Float64 | National rank of the team's yards gained per game with the team on offense, where 1 is best. |
| `drivesgame_off_rank` | Float64 | National rank of the team's drives per game with the team on offense, where 1 is best. |
| `yardsdrive_off_rank` | Float64 | National rank of the team's yards gained per drive with the team on offense, where 1 is best. |
| `playsdrive_off_rank` | Float64 | National rank of the team's plays run per drive with the team on offense, where 1 is best. |
| `play_stuffed_off_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag with the team on offense, where 1 is best. |
| `red_zone_success_off_rank` | Float64 | National rank of the team's success rate on red-zone plays with the team on offense, where 1 is best. |
| `third_down_success_off_rank` | Float64 | National rank of the team's success rate on third-down plays with the team on offense, where 1 is best. |
| `late_down_success_off_rank` | Float64 | National rank of the team's success rate on late-down plays with the team on offense, where 1 is best. |
| `third_down_distance_off_rank` | Float64 | National rank of the team's average yards to go on third down with the team on offense, where 1 is best. |
| `start_position_off_rank` | Float64 | National rank of the team's average drive start position, measured in yards from the opponent goal line with the team on offense, where 1 is best. |
| `havoc_off_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag with the team on offense, where 1 is best. |
| `explosive_off_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag with the team on offense, where 1 is best. |
| `passrate_off_rank` | Float64 | National rank of the team's share of plays that were pass plays with the team on offense, where 1 is best. |
| `rushrate_off_rank` | Float64 | National rank of the team's share of plays that were rush plays with the team on offense, where 1 is best. |
| `nonExplosiveEpaPerPlay_off_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded with the team on offense, where 1 is best. |
| `line_yards_off_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes with the team on offense, where 1 is best. |
| `opportunity_rate_off_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag with the team on offense, where 1 is best. |
| `plays_def` | UInt32 | Plays run, with the team on defense (i.e. allowed to opponents). |
| `passrate_def` | Float64 | Share of plays that were pass plays, with the team on defense (i.e. allowed to opponents). |
| `rushrate_def` | Float64 | Share of plays that were rush plays, with the team on defense (i.e. allowed to opponents). |
| `havoc_def` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag, with the team on defense (i.e. allowed to opponents). |
| `explosive_def` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag, with the team on defense (i.e. allowed to opponents). |
| `TEPA_def` | Float64 | Total EPA summed over every play, with the team on defense (i.e. allowed to opponents). |
| `EPAplay_def` | Float64 | EPA per play, with the team on defense (i.e. allowed to opponents). |
| `yards_def` | Int64 | Total yards gained, with the team on defense (i.e. allowed to opponents). |
| `yardsplay_def` | Float64 | Yards gained per play, with the team on defense (i.e. allowed to opponents). |
| `play_stuffed_def` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag, with the team on defense (i.e. allowed to opponents). |
| `success_def` | Float64 | Success rate -- the share of plays flagged as successful by EPA, with the team on defense (i.e. allowed to opponents). |
| `red_zone_success_def` | Float64 | Success rate on red-zone plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_success_def` | Float64 | Success rate on third-down plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_distance_def` | Float64 | Average yards to go on third down, with the team on defense (i.e. allowed to opponents). |
| `late_down_success_def` | Float64 | Success rate on late-down plays, with the team on defense (i.e. allowed to opponents). |
| `early_down_EPA_def` | Float64 | EPA per early-down play, with the team on defense (i.e. allowed to opponents). |
| `start_position_def` | Float64 | Average drive start position, measured in yards from the opponent goal line, with the team on defense (i.e. allowed to opponents). |
| `nonExplosiveEpaPerPlay_def` | Float64 | EPA per play with explosive plays excluded, with the team on defense (i.e. allowed to opponents). |
| `line_yards_def` | Float64 | Average line yards credited to the offensive line on rushes, with the team on defense (i.e. allowed to opponents). |
| `opportunity_rate_def` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def` | Float64 | Plays run per game, with the team on defense (i.e. allowed to opponents). |
| `EPAdrive_def` | Float64 | EPA per drive (total EPA divided by drives), with the team on defense (i.e. allowed to opponents). |
| `EPAgame_def` | Float64 | EPA per game (total EPA divided by games), with the team on defense (i.e. allowed to opponents). |
| `yardsgame_def` | Float64 | Yards gained per game, with the team on defense (i.e. allowed to opponents). |
| `drives_def` | UInt32 | Offensive drives, with the team on defense (i.e. allowed to opponents). |
| `drivesgame_def` | Float64 | Drives per game, with the team on defense (i.e. allowed to opponents). |
| `yardsdrive_def` | Float64 | Yards gained per drive, with the team on defense (i.e. allowed to opponents). |
| `playsdrive_def` | Float64 | Plays run per drive, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_rank` | Float64 | National rank of the team's plays run per game with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_def_rank` | Float64 | National rank of the team's total EPA summed over every play with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAgame_def_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAplay_def_rank` | Float64 | National rank of the team's EPA per play with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAdrive_def_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `early_down_EPA_def_rank` | Float64 | National rank of the team's EPA per early-down play with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `success_def_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yards_def_rank` | Float64 | National rank of the team's total yards gained with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsplay_def_rank` | Float64 | National rank of the team's yards gained per play with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsgame_def_rank` | Float64 | National rank of the team's yards gained per game with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `drivesgame_def_rank` | Float64 | National rank of the team's drives per game with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsdrive_def_rank` | Float64 | National rank of the team's yards gained per drive with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `playsdrive_def_rank` | Float64 | National rank of the team's plays run per drive with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `play_stuffed_def_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `red_zone_success_def_rank` | Float64 | National rank of the team's success rate on red-zone plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_success_def_rank` | Float64 | National rank of the team's success rate on third-down plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `late_down_success_def_rank` | Float64 | National rank of the team's success rate on late-down plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_distance_def_rank` | Float64 | National rank of the team's average yards to go on third down with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `start_position_def_rank` | Float64 | National rank of the team's average drive start position, measured in yards from the opponent goal line with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `havoc_def_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `explosive_def_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `passrate_def_rank` | Float64 | National rank of the team's share of plays that were pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `rushrate_def_rank` | Float64 | National rank of the team's share of plays that were rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `nonExplosiveEpaPerPlay_def_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `line_yards_def_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `opportunity_rate_def_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_margin` | Float64 | Margin in total EPA summed over every play: the team's offensive value minus the value it allowed on defense. |
| `EPAplay_margin` | Float64 | Margin in EPA per play: the team's offensive value minus the value it allowed on defense. |
| `EPAdrive_margin` | Float64 | Margin in EPA per drive (total EPA divided by drives): the team's offensive value minus the value it allowed on defense. |
| `EPAgame_margin` | Float64 | Margin in EPA per game (total EPA divided by games): the team's offensive value minus the value it allowed on defense. |
| `success_margin` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA: the team's offensive value minus the value it allowed on defense. |
| `yardsplay_margin` | Float64 | Margin in yards gained per play: the team's offensive value minus the value it allowed on defense. |
| `TEPA_margin_rank` | Float64 | Margin in total EPA summed over every play: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAplay_margin_rank` | Float64 | Margin in EPA per play: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAdrive_margin_rank` | Float64 | Margin in EPA per drive (total EPA divided by drives): the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAgame_margin_rank` | Float64 | Margin in EPA per game (total EPA divided by games): the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `success_margin_rank` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `yardsplay_margin_rank` | Float64 | Margin in yards gained per play: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `start_position_margin` | Float64 | Field-position margin: the team's own average starting field position minus the average starting field position it allowed, both measured as yards gained from their own goal line. Positive means the team started closer to scoring than its opponents. |
| `start_position_margin_rank` | Float64 | Field-position margin: the team's own average starting field position minus the average starting field position it allowed, both measured as yards gained from their own goal line. Positive means the team started closer to scoring than its opponents. National rank of that margin, 1 = largest. |
| `total_available_yards_off` | Float64 | Available yards are the yards a drive could theoretically gain, summed from each drive's starting distance to the opponent goal line. Total available yards on the team's own drives. |
| `total_gained_yards_off` | Int64 | Total yards the team actually gained across its own drives. |
| `available_yards_pct_off` | Float64 | Share of available yards the team's offense actually gained (total_gained_yards_off divided by total_available_yards_off). Higher is better. |
| `available_yards_pct_off_rank` | Float64 | National rank of the team's offensive available-yards share, where 1 is best. |
| `total_available_yards_def` | Float64 | Available yards are the yards a drive could theoretically gain, summed from each drive's starting distance to the opponent goal line. Total available yards on drives the team defended. |
| `total_gained_yards_def` | Int64 | Total yards the team allowed across the drives it defended. |
| `available_yards_pct_def` | Float64 | Share of available yards the team's defense allowed opponents to gain. Lower is better. |
| `available_yards_pct_def_rank` | Float64 | National rank of the team's defensive available-yards share, where 1 is best. |
| `total_available_yards_margin` | Float64 | Available yards on the team's own drives minus available yards on drives it defended. |
| `total_gained_yards_margin` | Int64 | Yards the team gained minus yards it allowed. |
| `available_yards_pct_margin` | Float64 | Available-yards share gained by the offense minus the share allowed by the defense. Higher is better. |
| `total_available_yards_margin_rank` | Float64 | National rank of total_available_yards_margin, 1 = largest margin. |
| `total_gained_yards_margin_rank` | Float64 | National rank of total_gained_yards_margin, 1 = largest margin. |
| `available_yards_pct_margin_rank` | Float64 | National rank of available_yards_pct_margin, 1 = largest margin. |
| `plays_off_pass` | UInt32 | Plays run on pass plays, with the team on offense. |
| `passrate_off_pass` | Float64 | Share of plays that were pass plays on pass plays, with the team on offense. |
| `rushrate_off_pass` | Float64 | Share of plays that were rush plays on pass plays, with the team on offense. |
| `havoc_off_pass` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag on pass plays, with the team on offense. |
| `explosive_off_pass` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag on pass plays, with the team on offense. |
| `TEPA_off_pass` | Float64 | Total EPA summed over every play on pass plays, with the team on offense. |
| `EPAplay_off_pass` | Float64 | EPA per play on pass plays, with the team on offense. |
| `yards_off_pass` | Int64 | Total yards gained on pass plays, with the team on offense. |
| `yardsplay_off_pass` | Float64 | Yards gained per play on pass plays, with the team on offense. |
| `play_stuffed_off_pass` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag on pass plays, with the team on offense. |
| `success_off_pass` | Float64 | Success rate -- the share of plays flagged as successful by EPA on pass plays, with the team on offense. |
| `red_zone_success_off_pass` | Float64 | Success rate on red-zone plays on pass plays, with the team on offense. |
| `third_down_success_off_pass` | Float64 | Success rate on third-down plays on pass plays, with the team on offense. |
| `third_down_distance_off_pass` | Float64 | Average yards to go on third down on pass plays, with the team on offense. |
| `late_down_success_off_pass` | Float64 | Success rate on late-down plays on pass plays, with the team on offense. |
| `early_down_EPA_off_pass` | Float64 | EPA per early-down play on pass plays, with the team on offense. |
| `nonExplosiveEpaPerPlay_off_pass` | Float64 | EPA per play with explosive plays excluded on pass plays, with the team on offense. |
| `line_yards_off_pass` | Float64 | Average line yards credited to the offensive line on rushes on pass plays, with the team on offense. |
| `opportunity_rate_off_pass` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag on pass plays, with the team on offense. |
| `playsgame_off_pass` | Float64 | Plays run per game on pass plays, with the team on offense. |
| `EPAdrive_off_pass` | Float64 | EPA per drive (total EPA divided by drives) on pass plays, with the team on offense. |
| `EPAgame_off_pass` | Float64 | EPA per game (total EPA divided by games) on pass plays, with the team on offense. |
| `yardsgame_off_pass` | Float64 | Yards gained per game on pass plays, with the team on offense. |
| `drives_off_pass` | UInt32 | Offensive drives on pass plays, with the team on offense. |
| `drivesgame_off_pass` | Float64 | Drives per game on pass plays, with the team on offense. |
| `yardsdrive_off_pass` | Float64 | Yards gained per drive on pass plays, with the team on offense. |
| `playsdrive_off_pass` | Float64 | Plays run per drive on pass plays, with the team on offense. |
| `playsgame_off_pass_rank` | Float64 | National rank of the team's plays run per game on pass plays with the team on offense, where 1 is best. |
| `TEPA_off_pass_rank` | Float64 | National rank of the team's total EPA summed over every play on pass plays with the team on offense, where 1 is best. |
| `EPAgame_off_pass_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) on pass plays with the team on offense, where 1 is best. |
| `EPAplay_off_pass_rank` | Float64 | National rank of the team's EPA per play on pass plays with the team on offense, where 1 is best. |
| `EPAdrive_off_pass_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) on pass plays with the team on offense, where 1 is best. |
| `early_down_EPA_off_pass_rank` | Float64 | National rank of the team's EPA per early-down play on pass plays with the team on offense, where 1 is best. |
| `success_off_pass_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA on pass plays with the team on offense, where 1 is best. |
| `yards_off_pass_rank` | Float64 | National rank of the team's total yards gained on pass plays with the team on offense, where 1 is best. |
| `yardsplay_off_pass_rank` | Float64 | National rank of the team's yards gained per play on pass plays with the team on offense, where 1 is best. |
| `yardsgame_off_pass_rank` | Float64 | National rank of the team's yards gained per game on pass plays with the team on offense, where 1 is best. |
| `drivesgame_off_pass_rank` | Float64 | National rank of the team's drives per game on pass plays with the team on offense, where 1 is best. |
| `yardsdrive_off_pass_rank` | Float64 | National rank of the team's yards gained per drive on pass plays with the team on offense, where 1 is best. |
| `playsdrive_off_pass_rank` | Float64 | National rank of the team's plays run per drive on pass plays with the team on offense, where 1 is best. |
| `play_stuffed_off_pass_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag on pass plays with the team on offense, where 1 is best. |
| `red_zone_success_off_pass_rank` | Float64 | National rank of the team's success rate on red-zone plays on pass plays with the team on offense, where 1 is best. |
| `third_down_success_off_pass_rank` | Float64 | National rank of the team's success rate on third-down plays on pass plays with the team on offense, where 1 is best. |
| `late_down_success_off_pass_rank` | Float64 | National rank of the team's success rate on late-down plays on pass plays with the team on offense, where 1 is best. |
| `third_down_distance_off_pass_rank` | Float64 | National rank of the team's average yards to go on third down on pass plays with the team on offense, where 1 is best. |
| `havoc_off_pass_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag on pass plays with the team on offense, where 1 is best. |
| `explosive_off_pass_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag on pass plays with the team on offense, where 1 is best. |
| `passrate_off_pass_rank` | Float64 | National rank of the team's share of plays that were pass plays on pass plays with the team on offense, where 1 is best. |
| `rushrate_off_pass_rank` | Float64 | National rank of the team's share of plays that were rush plays on pass plays with the team on offense, where 1 is best. |
| `nonExplosiveEpaPerPlay_off_pass_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded on pass plays with the team on offense, where 1 is best. |
| `line_yards_off_pass_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes on pass plays with the team on offense, where 1 is best. |
| `opportunity_rate_off_pass_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag on pass plays with the team on offense, where 1 is best. |
| `plays_def_pass` | UInt32 | Plays run on pass plays, with the team on defense (i.e. allowed to opponents). |
| `passrate_def_pass` | Float64 | Share of plays that were pass plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `rushrate_def_pass` | Float64 | Share of plays that were rush plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `havoc_def_pass` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag on pass plays, with the team on defense (i.e. allowed to opponents). |
| `explosive_def_pass` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag on pass plays, with the team on defense (i.e. allowed to opponents). |
| `TEPA_def_pass` | Float64 | Total EPA summed over every play on pass plays, with the team on defense (i.e. allowed to opponents). |
| `EPAplay_def_pass` | Float64 | EPA per play on pass plays, with the team on defense (i.e. allowed to opponents). |
| `yards_def_pass` | Int64 | Total yards gained on pass plays, with the team on defense (i.e. allowed to opponents). |
| `yardsplay_def_pass` | Float64 | Yards gained per play on pass plays, with the team on defense (i.e. allowed to opponents). |
| `play_stuffed_def_pass` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag on pass plays, with the team on defense (i.e. allowed to opponents). |
| `success_def_pass` | Float64 | Success rate -- the share of plays flagged as successful by EPA on pass plays, with the team on defense (i.e. allowed to opponents). |
| `red_zone_success_def_pass` | Float64 | Success rate on red-zone plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_success_def_pass` | Float64 | Success rate on third-down plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_distance_def_pass` | Float64 | Average yards to go on third down on pass plays, with the team on defense (i.e. allowed to opponents). |
| `late_down_success_def_pass` | Float64 | Success rate on late-down plays on pass plays, with the team on defense (i.e. allowed to opponents). |
| `early_down_EPA_def_pass` | Float64 | EPA per early-down play on pass plays, with the team on defense (i.e. allowed to opponents). |
| `nonExplosiveEpaPerPlay_def_pass` | Float64 | EPA per play with explosive plays excluded on pass plays, with the team on defense (i.e. allowed to opponents). |
| `line_yards_def_pass` | Float64 | Average line yards credited to the offensive line on rushes on pass plays, with the team on defense (i.e. allowed to opponents). |
| `opportunity_rate_def_pass` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag on pass plays, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_pass` | Float64 | Plays run per game on pass plays, with the team on defense (i.e. allowed to opponents). |
| `EPAdrive_def_pass` | Float64 | EPA per drive (total EPA divided by drives) on pass plays, with the team on defense (i.e. allowed to opponents). |
| `EPAgame_def_pass` | Float64 | EPA per game (total EPA divided by games) on pass plays, with the team on defense (i.e. allowed to opponents). |
| `yardsgame_def_pass` | Float64 | Yards gained per game on pass plays, with the team on defense (i.e. allowed to opponents). |
| `drives_def_pass` | UInt32 | Offensive drives on pass plays, with the team on defense (i.e. allowed to opponents). |
| `drivesgame_def_pass` | Float64 | Drives per game on pass plays, with the team on defense (i.e. allowed to opponents). |
| `yardsdrive_def_pass` | Float64 | Yards gained per drive on pass plays, with the team on defense (i.e. allowed to opponents). |
| `playsdrive_def_pass` | Float64 | Plays run per drive on pass plays, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_pass_rank` | Float64 | National rank of the team's plays run per game on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_def_pass_rank` | Float64 | National rank of the team's total EPA summed over every play on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAgame_def_pass_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAplay_def_pass_rank` | Float64 | National rank of the team's EPA per play on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAdrive_def_pass_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `early_down_EPA_def_pass_rank` | Float64 | National rank of the team's EPA per early-down play on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `success_def_pass_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yards_def_pass_rank` | Float64 | National rank of the team's total yards gained on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsplay_def_pass_rank` | Float64 | National rank of the team's yards gained per play on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsgame_def_pass_rank` | Float64 | National rank of the team's yards gained per game on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `drivesgame_def_pass_rank` | Float64 | National rank of the team's drives per game on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsdrive_def_pass_rank` | Float64 | National rank of the team's yards gained per drive on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `playsdrive_def_pass_rank` | Float64 | National rank of the team's plays run per drive on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `play_stuffed_def_pass_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `red_zone_success_def_pass_rank` | Float64 | National rank of the team's success rate on red-zone plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_success_def_pass_rank` | Float64 | National rank of the team's success rate on third-down plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `late_down_success_def_pass_rank` | Float64 | National rank of the team's success rate on late-down plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_distance_def_pass_rank` | Float64 | National rank of the team's average yards to go on third down on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `havoc_def_pass_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `explosive_def_pass_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `passrate_def_pass_rank` | Float64 | National rank of the team's share of plays that were pass plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `rushrate_def_pass_rank` | Float64 | National rank of the team's share of plays that were rush plays on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `nonExplosiveEpaPerPlay_def_pass_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `line_yards_def_pass_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `opportunity_rate_def_pass_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag on pass plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_margin_pass` | Float64 | Margin in total EPA summed over every play on pass plays: the team's offensive value minus the value it allowed on defense. |
| `EPAplay_margin_pass` | Float64 | Margin in EPA per play on pass plays: the team's offensive value minus the value it allowed on defense. |
| `EPAdrive_margin_pass` | Float64 | Margin in EPA per drive (total EPA divided by drives) on pass plays: the team's offensive value minus the value it allowed on defense. |
| `EPAgame_margin_pass` | Float64 | Margin in EPA per game (total EPA divided by games) on pass plays: the team's offensive value minus the value it allowed on defense. |
| `success_margin_pass` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA on pass plays: the team's offensive value minus the value it allowed on defense. |
| `yardsplay_margin_pass` | Float64 | Margin in yards gained per play on pass plays: the team's offensive value minus the value it allowed on defense. |
| `TEPA_margin_pass_rank` | Float64 | Margin in total EPA summed over every play on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAplay_margin_pass_rank` | Float64 | Margin in EPA per play on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAdrive_margin_pass_rank` | Float64 | Margin in EPA per drive (total EPA divided by drives) on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAgame_margin_pass_rank` | Float64 | Margin in EPA per game (total EPA divided by games) on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `success_margin_pass_rank` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `yardsplay_margin_pass_rank` | Float64 | Margin in yards gained per play on pass plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `plays_off_rush` | UInt32 | Plays run on rush plays, with the team on offense. |
| `passrate_off_rush` | Float64 | Share of plays that were pass plays on rush plays, with the team on offense. |
| `rushrate_off_rush` | Float64 | Share of plays that were rush plays on rush plays, with the team on offense. |
| `havoc_off_rush` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag on rush plays, with the team on offense. |
| `explosive_off_rush` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag on rush plays, with the team on offense. |
| `TEPA_off_rush` | Float64 | Total EPA summed over every play on rush plays, with the team on offense. |
| `EPAplay_off_rush` | Float64 | EPA per play on rush plays, with the team on offense. |
| `yards_off_rush` | Int64 | Total yards gained on rush plays, with the team on offense. |
| `yardsplay_off_rush` | Float64 | Yards gained per play on rush plays, with the team on offense. |
| `play_stuffed_off_rush` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag on rush plays, with the team on offense. |
| `success_off_rush` | Float64 | Success rate -- the share of plays flagged as successful by EPA on rush plays, with the team on offense. |
| `red_zone_success_off_rush` | Float64 | Success rate on red-zone plays on rush plays, with the team on offense. |
| `third_down_success_off_rush` | Float64 | Success rate on third-down plays on rush plays, with the team on offense. |
| `third_down_distance_off_rush` | Float64 | Average yards to go on third down on rush plays, with the team on offense. |
| `late_down_success_off_rush` | Float64 | Success rate on late-down plays on rush plays, with the team on offense. |
| `early_down_EPA_off_rush` | Float64 | EPA per early-down play on rush plays, with the team on offense. |
| `nonExplosiveEpaPerPlay_off_rush` | Float64 | EPA per play with explosive plays excluded on rush plays, with the team on offense. |
| `line_yards_off_rush` | Float64 | Average line yards credited to the offensive line on rushes on rush plays, with the team on offense. |
| `opportunity_rate_off_rush` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag on rush plays, with the team on offense. |
| `playsgame_off_rush` | Float64 | Plays run per game on rush plays, with the team on offense. |
| `EPAdrive_off_rush` | Float64 | EPA per drive (total EPA divided by drives) on rush plays, with the team on offense. |
| `EPAgame_off_rush` | Float64 | EPA per game (total EPA divided by games) on rush plays, with the team on offense. |
| `yardsgame_off_rush` | Float64 | Yards gained per game on rush plays, with the team on offense. |
| `drives_off_rush` | UInt32 | Offensive drives on rush plays, with the team on offense. |
| `drivesgame_off_rush` | Float64 | Drives per game on rush plays, with the team on offense. |
| `yardsdrive_off_rush` | Float64 | Yards gained per drive on rush plays, with the team on offense. |
| `playsdrive_off_rush` | Float64 | Plays run per drive on rush plays, with the team on offense. |
| `playsgame_off_rush_rank` | Float64 | National rank of the team's plays run per game on rush plays with the team on offense, where 1 is best. |
| `TEPA_off_rush_rank` | Float64 | National rank of the team's total EPA summed over every play on rush plays with the team on offense, where 1 is best. |
| `EPAgame_off_rush_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) on rush plays with the team on offense, where 1 is best. |
| `EPAplay_off_rush_rank` | Float64 | National rank of the team's EPA per play on rush plays with the team on offense, where 1 is best. |
| `EPAdrive_off_rush_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) on rush plays with the team on offense, where 1 is best. |
| `early_down_EPA_off_rush_rank` | Float64 | National rank of the team's EPA per early-down play on rush plays with the team on offense, where 1 is best. |
| `success_off_rush_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA on rush plays with the team on offense, where 1 is best. |
| `yards_off_rush_rank` | Float64 | National rank of the team's total yards gained on rush plays with the team on offense, where 1 is best. |
| `yardsplay_off_rush_rank` | Float64 | National rank of the team's yards gained per play on rush plays with the team on offense, where 1 is best. |
| `yardsgame_off_rush_rank` | Float64 | National rank of the team's yards gained per game on rush plays with the team on offense, where 1 is best. |
| `drivesgame_off_rush_rank` | Float64 | National rank of the team's drives per game on rush plays with the team on offense, where 1 is best. |
| `yardsdrive_off_rush_rank` | Float64 | National rank of the team's yards gained per drive on rush plays with the team on offense, where 1 is best. |
| `playsdrive_off_rush_rank` | Float64 | National rank of the team's plays run per drive on rush plays with the team on offense, where 1 is best. |
| `play_stuffed_off_rush_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag on rush plays with the team on offense, where 1 is best. |
| `red_zone_success_off_rush_rank` | Float64 | National rank of the team's success rate on red-zone plays on rush plays with the team on offense, where 1 is best. |
| `third_down_success_off_rush_rank` | Float64 | National rank of the team's success rate on third-down plays on rush plays with the team on offense, where 1 is best. |
| `late_down_success_off_rush_rank` | Float64 | National rank of the team's success rate on late-down plays on rush plays with the team on offense, where 1 is best. |
| `third_down_distance_off_rush_rank` | Float64 | National rank of the team's average yards to go on third down on rush plays with the team on offense, where 1 is best. |
| `havoc_off_rush_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag on rush plays with the team on offense, where 1 is best. |
| `explosive_off_rush_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag on rush plays with the team on offense, where 1 is best. |
| `passrate_off_rush_rank` | Float64 | National rank of the team's share of plays that were pass plays on rush plays with the team on offense, where 1 is best. |
| `rushrate_off_rush_rank` | Float64 | National rank of the team's share of plays that were rush plays on rush plays with the team on offense, where 1 is best. |
| `nonExplosiveEpaPerPlay_off_rush_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded on rush plays with the team on offense, where 1 is best. |
| `line_yards_off_rush_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes on rush plays with the team on offense, where 1 is best. |
| `opportunity_rate_off_rush_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag on rush plays with the team on offense, where 1 is best. |
| `plays_def_rush` | UInt32 | Plays run on rush plays, with the team on defense (i.e. allowed to opponents). |
| `passrate_def_rush` | Float64 | Share of plays that were pass plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `rushrate_def_rush` | Float64 | Share of plays that were rush plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `havoc_def_rush` | Float64 | Havoc rate -- the share of plays carrying the defensive-disruption flag on rush plays, with the team on defense (i.e. allowed to opponents). |
| `explosive_def_rush` | Float64 | Explosive-play rate -- the share of plays carrying the explosive flag on rush plays, with the team on defense (i.e. allowed to opponents). |
| `TEPA_def_rush` | Float64 | Total EPA summed over every play on rush plays, with the team on defense (i.e. allowed to opponents). |
| `EPAplay_def_rush` | Float64 | EPA per play on rush plays, with the team on defense (i.e. allowed to opponents). |
| `yards_def_rush` | Int64 | Total yards gained on rush plays, with the team on defense (i.e. allowed to opponents). |
| `yardsplay_def_rush` | Float64 | Yards gained per play on rush plays, with the team on defense (i.e. allowed to opponents). |
| `play_stuffed_def_rush` | Float64 | Stuffed-play rate -- the share of plays carrying the stuffed flag on rush plays, with the team on defense (i.e. allowed to opponents). |
| `success_def_rush` | Float64 | Success rate -- the share of plays flagged as successful by EPA on rush plays, with the team on defense (i.e. allowed to opponents). |
| `red_zone_success_def_rush` | Float64 | Success rate on red-zone plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_success_def_rush` | Float64 | Success rate on third-down plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `third_down_distance_def_rush` | Float64 | Average yards to go on third down on rush plays, with the team on defense (i.e. allowed to opponents). |
| `late_down_success_def_rush` | Float64 | Success rate on late-down plays on rush plays, with the team on defense (i.e. allowed to opponents). |
| `early_down_EPA_def_rush` | Float64 | EPA per early-down play on rush plays, with the team on defense (i.e. allowed to opponents). |
| `nonExplosiveEpaPerPlay_def_rush` | Float64 | EPA per play with explosive plays excluded on rush plays, with the team on defense (i.e. allowed to opponents). |
| `line_yards_def_rush` | Float64 | Average line yards credited to the offensive line on rushes on rush plays, with the team on defense (i.e. allowed to opponents). |
| `opportunity_rate_def_rush` | Float64 | Opportunity rate -- the share of rushes carrying the opportunity flag on rush plays, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_rush` | Float64 | Plays run per game on rush plays, with the team on defense (i.e. allowed to opponents). |
| `EPAdrive_def_rush` | Float64 | EPA per drive (total EPA divided by drives) on rush plays, with the team on defense (i.e. allowed to opponents). |
| `EPAgame_def_rush` | Float64 | EPA per game (total EPA divided by games) on rush plays, with the team on defense (i.e. allowed to opponents). |
| `yardsgame_def_rush` | Float64 | Yards gained per game on rush plays, with the team on defense (i.e. allowed to opponents). |
| `drives_def_rush` | UInt32 | Offensive drives on rush plays, with the team on defense (i.e. allowed to opponents). |
| `drivesgame_def_rush` | Float64 | Drives per game on rush plays, with the team on defense (i.e. allowed to opponents). |
| `yardsdrive_def_rush` | Float64 | Yards gained per drive on rush plays, with the team on defense (i.e. allowed to opponents). |
| `playsdrive_def_rush` | Float64 | Plays run per drive on rush plays, with the team on defense (i.e. allowed to opponents). |
| `playsgame_def_rush_rank` | Float64 | National rank of the team's plays run per game on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_def_rush_rank` | Float64 | National rank of the team's total EPA summed over every play on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAgame_def_rush_rank` | Float64 | National rank of the team's EPA per game (total EPA divided by games) on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAplay_def_rush_rank` | Float64 | National rank of the team's EPA per play on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `EPAdrive_def_rush_rank` | Float64 | National rank of the team's EPA per drive (total EPA divided by drives) on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `early_down_EPA_def_rush_rank` | Float64 | National rank of the team's EPA per early-down play on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `success_def_rush_rank` | Float64 | National rank of the team's success rate -- the share of plays flagged as successful by EPA on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yards_def_rush_rank` | Float64 | National rank of the team's total yards gained on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsplay_def_rush_rank` | Float64 | National rank of the team's yards gained per play on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsgame_def_rush_rank` | Float64 | National rank of the team's yards gained per game on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `drivesgame_def_rush_rank` | Float64 | National rank of the team's drives per game on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `yardsdrive_def_rush_rank` | Float64 | National rank of the team's yards gained per drive on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `playsdrive_def_rush_rank` | Float64 | National rank of the team's plays run per drive on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `play_stuffed_def_rush_rank` | Float64 | National rank of the team's stuffed-play rate -- the share of plays carrying the stuffed flag on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `red_zone_success_def_rush_rank` | Float64 | National rank of the team's success rate on red-zone plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_success_def_rush_rank` | Float64 | National rank of the team's success rate on third-down plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `late_down_success_def_rush_rank` | Float64 | National rank of the team's success rate on late-down plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `third_down_distance_def_rush_rank` | Float64 | National rank of the team's average yards to go on third down on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `havoc_def_rush_rank` | Float64 | National rank of the team's havoc rate -- the share of plays carrying the defensive-disruption flag on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `explosive_def_rush_rank` | Float64 | National rank of the team's explosive-play rate -- the share of plays carrying the explosive flag on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `passrate_def_rush_rank` | Float64 | National rank of the team's share of plays that were pass plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `rushrate_def_rush_rank` | Float64 | National rank of the team's share of plays that were rush plays on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `nonExplosiveEpaPerPlay_def_rush_rank` | Float64 | National rank of the team's EPA per play with explosive plays excluded on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `line_yards_def_rush_rank` | Float64 | National rank of the team's average line yards credited to the offensive line on rushes on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `opportunity_rate_def_rush_rank` | Float64 | National rank of the team's opportunity rate -- the share of rushes carrying the opportunity flag on rush plays with the team on defense (i.e. allowed to opponents), where 1 is best. |
| `TEPA_margin_rush` | Float64 | Margin in total EPA summed over every play on rush plays: the team's offensive value minus the value it allowed on defense. |
| `EPAplay_margin_rush` | Float64 | Margin in EPA per play on rush plays: the team's offensive value minus the value it allowed on defense. |
| `EPAdrive_margin_rush` | Float64 | Margin in EPA per drive (total EPA divided by drives) on rush plays: the team's offensive value minus the value it allowed on defense. |
| `EPAgame_margin_rush` | Float64 | Margin in EPA per game (total EPA divided by games) on rush plays: the team's offensive value minus the value it allowed on defense. |
| `success_margin_rush` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA on rush plays: the team's offensive value minus the value it allowed on defense. |
| `yardsplay_margin_rush` | Float64 | Margin in yards gained per play on rush plays: the team's offensive value minus the value it allowed on defense. |
| `TEPA_margin_rush_rank` | Float64 | Margin in total EPA summed over every play on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAplay_margin_rush_rank` | Float64 | Margin in EPA per play on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAdrive_margin_rush_rank` | Float64 | Margin in EPA per drive (total EPA divided by drives) on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `EPAgame_margin_rush_rank` | Float64 | Margin in EPA per game (total EPA divided by games) on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `success_margin_rush_rank` | Float64 | Margin in success rate -- the share of plays flagged as successful by EPA on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `yardsplay_margin_rush_rank` | Float64 | Margin in yards gained per play on rush plays: the team's offensive value minus the value it allowed on defense. National rank of that margin, 1 = largest. |
| `fbs_class` | String | Power/Group classification for the season: P4 or G6 from 2024 on, P5 or G5 through 2023, derived from conference membership (Notre Dame is classified with the power group). Null for teams outside FBS. |
| `valid_games` | UInt32 | Number of the team's games that produced both an offensive and a defensive adjusted-EPA value; teams below two valid games are dropped from the adjusted ratings. |
| `adj_off_epa` | Float64 | Offensive opponent-adjusted EPA per play from the ridge (RAPM-style) regression on offense/defense team indicators plus home field -- cfbfastR's adjust_epa adjustment, fit in-sample across the season, so the value is descriptive of that window rather than predictive. |
| `adj_def_epa` | Float64 | Defensive opponent-adjusted EPA per play from the ridge (RAPM-style) regression on offense/defense team indicators plus home field -- cfbfastR's adjust_epa adjustment, fit in-sample across the season, so the value is descriptive of that window rather than predictive. Lower is better -- it is EPA allowed. |
| `off_strength_faced` | Float64 | Average opponent-defense strength the team's offense faced, taken as the mean of the ridge's defensive coefficients across its opponents. Higher means a tougher slate. |
| `def_strength_faced` | Float64 | Average opponent-offense strength the team's defense faced, taken as the mean of the ridge's offensive coefficients across its opponents. Higher means a tougher slate. |
| `net_adj_epa` | Float64 | Net opponent-adjusted EPA per play: adj_off_epa minus adj_def_epa. Higher is better. |
| `adj_off_epa_rank` | Float64 | National rank of the team's adj_off_epa, where 1 is best. |
| `adj_def_epa_rank` | Float64 | National rank of the team's adj_def_epa, where 1 is best (fewest EPA allowed). |
| `net_adj_epa_rank` | Float64 | National rank of the team's net_adj_epa, 1 = largest net adjusted EPA. |
| `through_week` | Int32 | Regular-season week this cumulative snapshot covers -- the row reflects the team's state through the end of that week. One asset holds every week, so filter on this column. |

```python
load_cfb_team_summaries_weekly(seasons=2024)
```
