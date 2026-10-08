---
title: "NFL — additional Python functions — Models and calculators: calculate_wpa"
sidebar_label: "Models and calculators: calculate_wpa"
sidebar_position: 17
description: "NFL — additional Python functions — Models and calculators: calculate_wpa — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Models and calculators: calculate_wpa

### calculate_wpa {#calculate_wpa}

`calculate_wpa(df: 'pl.DataFrame') -> 'pl.DataFrame'`

Derive win probability added (WPA) from pre-scored WP point estimates.

This is the **derivation half** of `NFLPlayProcess.__process_wpa` lifted
into a shared, model-free function so the same nflfastR-faithful WPA logic
can be reused by the streaming `enrich_nfl_pbp` pipeline and by
process_wpa` itself.  It performs **no** model inference — the caller
must already have scored the per-play WP point estimates
(`wp_spread.ubj`) for the start / touchback / end feature views and
attached them as `wp_before` / `wp_touchback` / `wp_after`.  This
mirrors `calculate_epa`, which likewise consumes pre-scored EP point
estimates and leaves prediction to the orchestrator.

Derivation rules (mirror the original process_wpa`):

* **Leading overlay (do not drop):** on a kickoff (`type.text` in
  `kickoff_vec`) `wp_before` is replaced by `wp_touchback` — the
  win-probability scored from the touchback feature view — before any
  other column derives.  This is the WP analogue of the EPA `0.92`
  scoring-attempt overlay and must fire first.
* **Try rows:** a standalone try row (`Extra Point Good`, `Two Point
  Pass`, `Defensive 2pt Conversion`, ...) takes the `wp_after` of the
  touchdown before it (the last play that is not a clock stoppage) as its
  `wp_before` when the try is the touchdown's end team's, so the
  touchdown hands over to the try. A clock stoppage just before the try
  inherits too, restated for the team ESPN credits it to, and the try
  still hands over from the touchdown. The model cannot score the try's
  own start state (ESPN's down-0 placeholder). A return or defensive
  touchdown (a `scoringPlay` whose end team is the scorer, not its start
  team) hands over only a `wp_after` scored for the scorer, as
  `NFLPlayProcess` scores it.
* `def_wp_before = 1 - wp_before`; `home_wp_before` / `away_wp_before`
  are the posteam->home perspective columns (the offense's `wp_before`
  flows to home when the start possession team is the home team, otherwise
  to the defense `def_wp_before`).
* `wp_after` is rewritten by the end-of-half / end-of-game / OT two-path:
  timeouts hold `wp_before`; a completed final play resolves to `1.0` /
  `0.0` by the winner; end-of-half and `End Period` / `End of Half`
  lead plays take `lead_wp_before` (or `1 - lead_wp_before` on a
  possession change); a possession change otherwise flips the lead;
  everything else keeps the model `wp_after`.
* `def_wp_after = 1 - wp_after`; `home_wp_after` / `away_wp_after`
  use the **end** possession team for the perspective flip.
* `wpa = wp_after - wp_before`.

**Every** `shift` / forward reference is grouped `.over("game_id")` so a
concatenated multi-game frame never leaks WP across game boundaries — the
`lead_wp_before` / `lead_wp_before2` shifts and the end-of-game
`game_play_number == max()` lookup are all per-game.  This differs from
process_wpa` (which runs one game per instance and therefore needs no
grouping).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Play-by-play DataFrame that already carries the WP point estimates `wp_before` (start feature view), `wp_touchback` (touchback feature view) and `wp_after` (end feature view), plus the play-classification / perspective columns `game_id`, `type.text`, `homeTeamId`, `start.pos_team.id`, `end.pos_team.id`, `start.pos_team_receives_2H_kickoff`, `change_of_pos_team`, `scoringPlay`, `kickoff_onside`, `end_of_half`, `status_type_completed`, `pos_score_diff_end`, `lead_play_type`, `lead_pos_team` and `game_play_number`. See WPA_REQUIRED_COLUMNS`. This function does **not** score WP itself — score it first via `calculate_win_probability` / the `wp_spread` feature pipeline. |

**Returns**

The input frame with the WPA derivation applied: `wp_before` rewritten by the kickoff-touchback overlay; `def_wp_before`, `home_wp_before`, `away_wp_before`, `lead_wp_before`, `lead_wp_before2`, the rewritten `wp_after`, `def_wp_after`, `home_wp_after`, `away_wp_after` and `wpa` added; plus first-class lowercase aliases `wp` (`= wp_before`), `def_wp` (`= def_wp_before`), `home_wp` (`= home_wp_before`) and `away_wp` (`= away_wp_before`) for downstream contract parity (the per-play offense win probability is the pre-snap `wp_before`, matching nflfastR's `wp` semantics).

| col_name | type | description |
|---|---|---|
| `game_play_number` | integer |  |
| `id` | integer | ID of the player in the 'name' column. |
| `sequenceNumber` | integer |  |
| `text` | character |  |
| `awayScore` | integer |  |
| `homeScore` | integer |  |
| `scoringPlay` | logical |  |
| `priority` | logical |  |
| `modified` | character |  |
| `wallclock` | character |  |
| `teamParticipants` | integer |  |
| `isPenalty` | logical |  |
| `statYardage` | integer |  |
| `isTurnover` | logical |  |
| `type.id` | character |  |
| `type.text` | character |  |
| `period.number` | integer |  |
| `clock.displayValue` | character |  |
| `start.down` | integer |  |
| `start.distance` | integer |  |
| `start.yardLine` | integer |  |
| `start.yardsToEndzone` | integer |  |
| `start.team.id` | integer |  |
| `end.down` | integer |  |
| `end.distance` | integer |  |
| `end.yardLine` | integer |  |
| `end.yardsToEndzone` | integer |  |
| `end.team.id` | integer |  |
| `type.abbreviation` | character |  |
| `start.downDistanceText` | character |  |
| `start.shortDownDistanceText` | character |  |
| `start.possessionText` | character |  |
| `end.downDistanceText` | character |  |
| `end.shortDownDistanceText` | character |  |
| `end.possessionText` | character |  |
| `scoringType.name` | character |  |
| `scoringType.displayName` | character |  |
| `scoringType.abbreviation` | character |  |
| `pointAfterAttempt.id` | double |  |
| `pointAfterAttempt.text` | character |  |
| `pointAfterAttempt.abbreviation` | character |  |
| `pointAfterAttempt.value` | double |  |
| `drive.id` | character |  |
| `drive.displayResult` | character |  |
| `drive.isScore` | logical |  |
| `drive.team.shortDisplayName` | character |  |
| `drive.team.displayName` | character |  |
| `drive.team.name` | character |  |
| `drive.team.abbreviation` | character |  |
| `drive.yards` | integer |  |
| `drive.offensivePlays` | integer |  |
| `drive.result` | character |  |
| `drive.description` | character |  |
| `drive.shortDisplayResult` | character |  |
| `drive.timeElapsed.displayValue` | character |  |
| `drive.start.period.number` | integer |  |
| `drive.start.period.type` | character |  |
| `drive.start.yardLine` | integer |  |
| `drive.start.clock.displayValue` | character |  |
| `drive.start.text` | character |  |
| `drive.end.period.number` | integer |  |
| `drive.end.period.type` | character |  |
| `drive.end.yardLine` | integer |  |
| `drive.end.clock.displayValue` | character |  |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | integer |  |
| `week` | integer | Season week. |
| `status_type_completed` | logical |  |
| `homeTeamId` | integer |  |
| `awayTeamId` | integer |  |
| `homeTeamName` | character |  |
| `awayTeamName` | character |  |
| `homeTeamMascot` | character |  |
| `awayTeamMascot` | character |  |
| `homeTeamAbbrev` | character |  |
| `awayTeamAbbrev` | character |  |
| `homeTeamNameAlt` | character |  |
| `awayTeamNameAlt` | character |  |
| `gameSpread` | double |  |
| `homeFavorite` | logical |  |
| `gameSpreadAvailable` | logical |  |
| `overUnder` | double |  |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `homeTeamSpread` | double |  |
| `clock.minutes` | integer |  |
| `clock.seconds` | integer |  |
| `half` | integer |  |
| `lag_half` | integer |  |
| `lead_half` | integer |  |
| `start.TimeSecsRem` | integer |  |
| `start.adj_TimeSecsRem` | integer |  |
| `orig_play_type` | character |  |
| `lead_text` | character |  |
| `lead_start_team` | character |  |
| `lead_start_yardsToEndzone` | integer |  |
| `lead_start_down` | integer |  |
| `lead_start_distance` | integer |  |
| `lead_scoringPlay` | logical |  |
| `text_dupe` | logical |  |
| `start.pos_team.id` | integer |  |
| `start.def_pos_team.id` | integer |  |
| `end.def_pos_team.id` | integer |  |
| `end.pos_team.id` | integer |  |
| `start.pos_team.name` | character |  |
| `start.def_pos_team.name` | character |  |
| `end.pos_team.name` | character |  |
| `end.def_pos_team.name` | character |  |
| `start.is_home` | logical |  |
| `end.is_home` | logical |  |
| `homeTimeoutCalled` | logical |  |
| `awayTimeoutCalled` | logical |  |
| `end.homeTeamTimeouts` | integer |  |
| `end.awayTeamTimeouts` | integer |  |
| `start.homeTeamTimeouts` | integer |  |
| `start.awayTeamTimeouts` | integer |  |
| `end.TimeSecsRem` | integer |  |
| `end.adj_TimeSecsRem` | integer |  |
| `start.posTeamTimeouts` | integer |  |
| `start.defPosTeamTimeouts` | integer |  |
| `end.posTeamTimeouts` | integer |  |
| `end.defPosTeamTimeouts` | integer |  |
| `firstHalfKickoffTeamId` | integer |  |
| `period` | integer |  |
| `start.yard` | integer |  |
| `end.yard` | integer |  |
| `lag_scoringPlay` | logical |  |
| `end_of_half` | logical |  |
| `down_1` | logical |  |
| `down_2` | logical |  |
| `down_3` | logical |  |
| `down_4` | logical |  |
| `down_1_end` | logical |  |
| `down_2_end` | logical |  |
| `down_3_end` | logical |  |
| `down_4_end` | logical |  |
| `scoring_play` | logical |  |
| `td_play` | logical |  |
| `touchdown` | logical | Binary indicator for if the play resulted in a TD. |
| `td_check` | logical |  |
| `safety` | logical | Binary indicator for whether or not a safety occurred. |
| `fumble_vec` | logical |  |
| `forced_fumble` | logical |  |
| `kickoff_play` | logical |  |
| `kickoff_tb` | logical |  |
| `kickoff_onside` | logical |  |
| `kickoff_oob` | logical |  |
| `kickoff_fair_catch` | logical | Binary indicator for if the kickoff was caught with a fair catch. |
| `kickoff_downed` | logical | Binary indicator for if the kickoff was downed. |
| `kick_play` | logical |  |
| `kickoff_safety` | logical |  |
| `punt` | logical |  |
| `punt_play` | logical |  |
| `punt_tb` | logical |  |
| `punt_oob` | logical |  |
| `punt_fair_catch` | logical | Binary indicator for if the punt was caught with a fair catch. |
| `punt_downed` | logical | Binary indicator for if the punt was downed. |
| `punt_safety` | logical |  |
| `punt_blocked` | logical | Binary indicator for if the punt was blocked. |
| `penalty_safety` | logical |  |
| `rush` | logical | Binary indicator if the play was a rushing play. |
| `pass` | logical | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `sack_vec` | logical |  |
| `pos_team` | integer |  |
| `def_pos_team` | integer |  |
| `is_home` | logical |  |
| `lag_HA_score_diff` | integer |  |
| `HA_score_diff` | integer |  |
| `net_HA_score_pts` | integer |  |
| `H_score_diff` | integer |  |
| `A_score_diff` | integer |  |
| `lag_homeScore` | integer |  |
| `lag_awayScore` | integer |  |
| `start.homeScore` | integer |  |
| `start.awayScore` | integer |  |
| `end.homeScore` | integer |  |
| `end.awayScore` | integer |  |
| `pos_team_score` | integer |  |
| `def_pos_team_score` | integer |  |
| `start.pos_team_score` | integer |  |
| `start.def_pos_team_score` | integer |  |
| `start.pos_score_diff` | integer |  |
| `end.pos_team_score` | integer |  |
| `end.def_pos_team_score` | integer |  |
| `end.pos_score_diff` | integer |  |
| `lag_pos_team` | integer |  |
| `lead_pos_team` | integer |  |
| `lead_pos_team2` | integer |  |
| `pos_score_diff` | integer |  |
| `lag_pos_score_diff` | integer |  |
| `pos_score_pts` | integer |  |
| `pos_score_diff_start` | integer |  |
| `start.pos_team_receives_2H_kickoff` | logical |  |
| `end.pos_team_receives_2H_kickoff` | logical |  |
| `change_of_poss` | logical |  |
| `penalty_flag` | logical |  |
| `penalty_declined` | logical |  |
| `penalty_no_play` | logical |  |
| `penalty_offset` | logical |  |
| `penalty_1st_conv` | logical |  |
| `penalty_in_text` | logical |  |
| `penalty_detail` | character |  |
| `penalty_text` | character |  |
| `yds_penalty` | character |  |
| `penalty_count` | integer |  |
| `penalty_declined_count` | integer |  |
| `penalty_all_declined` | logical |  |
| `penalty_enforcement` | character |  |
| `penalty_negated_play` | logical |  |
| `sack` | logical | Binary indicator for if the play ended in a sack. |
| `int` | logical |  |
| `int_td` | logical |  |
| `completion` | logical |  |
| `pass_attempt` | logical | Binary indicator for if the play was a pass attempt (includes sacks). |
| `target` | logical |  |
| `pass_breakup` | logical |  |
| `pass_td` | logical |  |
| `rush_td` | logical |  |
| `pass_depth` | character |  |
| `pass_direction` | character |  |
| `rush_direction` | character |  |
| `turnover_vec` | logical |  |
| `offense_score_play` | logical |  |
| `defense_score_play` | logical |  |
| `downs_turnover` | logical |  |
| `yds_punted` | integer |  |
| `yds_punt_gained` | integer |  |
| `fg_attempt` | logical |  |
| `fg_made` | logical |  |
| `yds_fg` | integer |  |
| `pos_unit` | character |  |
| `def_pos_unit` | character |  |
| `lead_play_type` | character |  |
| `sp` | logical | Binary indicator for whether or not a score occurred on the play. |
| `play` | logical | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `scrimmage_play` | logical |  |
| `change_of_pos_team` | logical |  |
| `pos_score_diff_end` | integer |  |
| `fumble_lost` | logical | Binary indicator for if the fumble was lost. |
| `fumble_recovered` | logical |  |
| `field_goal_result` | character | String indicator for result of field goal attempt: made, missed, or blocked. |
| `extra_point_result` | character | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | character | String indicator for result of two point conversion attempt: success, failure, safety (touchback in defensive endzone is 1 point apparently), or return. |
| `kneel_down` | logical |  |
| `qb_hurry` | logical |  |
| `xp_attempt` | logical |  |
| `xp_made` | logical |  |
| `two_point_attempt` | logical | Binary indicator for two point conversion attempt. |
| `defensive_two_point_attempt` | logical | Binary indicator whether or not the defense was able to have an attempt on a two point conversion, this results following a turnover. |
| `defensive_two_point_conv` | logical | Binary indicator whether or not the defense successfully scored on the two point conversion. |
| `two_point_pass` | logical |  |
| `two_point_rush` | logical |  |
| `yds_rushed` | integer |  |
| `yds_receiving` | integer |  |
| `yds_int_return` | character |  |
| `yds_kickoff` | integer |  |
| `yds_kickoff_return` | integer |  |
| `yds_punt_return` | integer |  |
| `yds_fumble_return` | character |  |
| `yds_sacked` | integer |  |
| `sack_players` | character |  |
| `xp_kicker_player_name` | character |  |
| `passer_player_name` | character | String name for the player that attempted the pass. |
| `rusher_player_name` | character | String name for the player that attempted the run. |
| `receiver_player_name` | character | String name for the targeted receiver. |
| `sack_player_name` | character | String name of the player who recorded a solo sack. |
| `sack_player_name2` | character |  |
| `pass_breakup_player_name` | character |  |
| `interception_player_name` | character | String name for the player that intercepted the pass. |
| `fg_kicker_player_name` | character |  |
| `fg_block_player_name` | character |  |
| `fg_return_player_name` | character |  |
| `kickoff_player_name` | character |  |
| `kickoff_return_player_name` | character |  |
| `punter_player_name` | character | String name for the punter. |
| `punt_block_player_name` | character |  |
| `punt_return_player_name` | character |  |
| `punt_block_return_player_name` | character |  |
| `fumble_player_name` | character |  |
| `fumble_forced_player_name` | character |  |
| `fumble_recovered_player_name` | character |  |
| `kicking_team` | integer |  |
| `return_team` | integer | String abbreviation of the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `fumble_or_muff` | logical |  |
| `recovery_team` | character |  |
| `recovery_team_2` | character |  |
| `penalty_spot_yardline` | integer |  |
| `penalty_spot_side` | character |  |
| `penalty_spot_yardsToEndzone` | integer |  |
| `fumbling_team` | character |  |
| `int_turnover` | logical |  |
| `pos_fumble_lost` | logical |  |
| `def_fumble_lost` | logical |  |
| `is_pos_team_turnover` | logical |  |
| `is_def_pos_team_turnover` | logical |  |
| `is_turnover` | logical |  |
| `turnover_team` | character |  |
| `is_st_turnover` | logical |  |
| `is_blocked_punt_turnover` | logical |  |
| `is_blocked_fg_turnover` | logical |  |
| `sack_team` | integer |  |
| `interception_team` | integer |  |
| `pass_breakup_team` | integer |  |
| `forced_fumble_team` | integer |  |
| `fumble_recovery_team` | character |  |
| `punt_return_team` | integer |  |
| `kick_return_team` | integer |  |
| `fg_team` | integer |  |
| `punt_team` | integer |  |
| `penalized_team` | integer |  |
| `penalty_yards_signed` | integer |  |
| `penalty_side` | character |  |
| `penalty_yards_net` | integer |  |
| `penalty_team_id` | integer |  |
| `lateral_player_name` | character |  |
| `yds_lateral` | character |  |
| `yards_after_catch` | character | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `air_yards` | character | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `air_yardsToEndzone` | character |  |
| `new_down` | integer |  |
| `new_distance` | integer |  |
| `middle_8` | logical |  |
| `rz_play` | logical |  |
| `under_2` | logical |  |
| `goal_to_go` | logical | Binary indicator for whether or not the posteam is in a goal down situation. |
| `scoring_opp` | logical |  |
| `stuffed_run` | logical |  |
| `stopped_run` | logical |  |
| `opportunity_run` | logical |  |
| `highlight_run` | logical |  |
| `adj_rush_yardage` | integer |  |
| `line_yards` | double |  |
| `second_level_yards` | double |  |
| `open_field_yards` | integer |  |
| `highlight_yards` | double |  |
| `opp_highlight_yards` | double |  |
| `short_rush_success` | logical |  |
| `short_rush_attempt` | logical |  |
| `power_rush_success` | logical |  |
| `power_rush_attempt` | logical |  |
| `early_down` | logical |  |
| `late_down` | logical |  |
| `early_down_pass` | logical |  |
| `early_down_rush` | logical |  |
| `late_down_pass` | logical |  |
| `late_down_rush` | logical |  |
| `standard_down` | logical |  |
| `passing_down` | logical |  |
| `TFL` | logical |  |
| `TFL_pass` | logical |  |
| `TFL_rush` | logical |  |
| `havoc` | logical |  |
| `first_down_yards` | logical |  |
| `first_down_penalty` | logical | Binary indicator for if a penalty converted the first down. |
| `first_down_earned` | logical |  |
| `new_series` | logical |  |
| `firstD_by_kickoff` | logical |  |
| `firstD_by_poss` | logical |  |
| `firstD_by_penalty` | logical |  |
| `firstD_by_yards` | logical |  |
| `start.pos_team_spread` | double |  |
| `start.elapsed_share` | double |  |
| `start.spread_time` | double |  |
| `end.pos_team_spread` | double |  |
| `end.elapsed_share` | double |  |
| `end.spread_time` | double |  |
| `pass_length` | character | String indicator for pass length: short or deep. |
| `pass_location` | character | String indicator for pass location: left, middle, or right. |
| `shotgun` | integer | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | integer | Binary indicator for whether or not the play was in no_huddle formation. |
| `pass_middle` | integer |  |
| `down` | integer | The down for the given play. |
| `distance` | integer |  |
| `start.yardsToEndzone.touchback` | integer |  |
| `penalty_assessed_on_kickoff` | logical |  |
| `EP_start_touchback` | double |  |
| `EP_start` | double |  |
| `EP_end` | double |  |
| `EP_penalty_cf` | character |  |
| `penalty_cf_yardsToEndzone` | character |  |
| `lag_EP_end` | double |  |
| `lag_change_of_pos_team` | logical |  |
| `EP_between` | double |  |
| `EPA` | double |  |
| `def_EPA` | double |  |
| `EPA_scrimmage` | double |  |
| `EPA_rush` | double |  |
| `EPA_pass` | double |  |
| `EPA_explosive` | logical |  |
| `EPA_non_explosive` | double |  |
| `EPA_explosive_pass` | logical |  |
| `EPA_explosive_rush` | logical |  |
| `first_down_created` | logical |  |
| `EPA_success` | logical |  |
| `EPA_success_early_down` | logical |  |
| `EPA_success_early_down_pass` | logical |  |
| `EPA_success_early_down_rush` | logical |  |
| `EPA_success_late_down` | logical |  |
| `EPA_success_late_down_pass` | logical |  |
| `EPA_success_late_down_rush` | logical |  |
| `EPA_success_standard_down` | logical |  |
| `EPA_success_passing_down` | logical |  |
| `EPA_success_pass` | logical |  |
| `EPA_success_rush` | logical |  |
| `EPA_success_EPA` | double |  |
| `EPA_success_standard_down_EPA` | double |  |
| `EPA_success_passing_down_EPA` | double |  |
| `EPA_success_pass_EPA` | double |  |
| `EPA_success_rush_EPA` | double |  |
| `EPA_middle_8_success` | logical |  |
| `EPA_middle_8_success_pass` | logical |  |
| `EPA_middle_8_success_rush` | logical |  |
| `EPA_penalty` | double |  |
| `EPA_penalty_direct` | double |  |
| `EPA_sp` | double |  |
| `EPA_fg` | double |  |
| `EPA_punt` | double |  |
| `EPA_kickoff` | double |  |
| `qb_epa` | double | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `start.ExpScoreDiff_touchback` | double |  |
| `start.ExpScoreDiff` | double |  |
| `start.ExpScoreDiff_Time_Ratio_touchback` | double |  |
| `start.ExpScoreDiff_Time_Ratio` | double |  |
| `end.ExpScoreDiff` | double |  |
| `end.ExpScoreDiff_Time_Ratio` | double |  |
| `wp_before` | double |  |
| `wp_touchback` | double |  |
| `wp_after` | double |  |
| `def_wp_before` | double |  |
| `home_wp_before` | double |  |
| `away_wp_before` | double |  |
| `lead_wp_before` | double |  |
| `lead_wp_before2` | double |  |
| `def_wp_after` | double |  |
| `home_wp_after` | double |  |
| `away_wp_after` | double |  |
| `wpa` | double | Win probability added (WPA) for the posteam. |
| `wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `vegas_wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `def_wp` | double | Estimated win probability for the defteam. |
| `home_wp` | double | Estimated win probability for the home team. |
| `away_wp` | double | Estimated win probability for the away team. |
| `wp_before_naive` | double |  |
| `wp_after_naive` | double |  |
| `wpa_naive` | double |  |
| `def_wp_before_naive` | double |  |
| `def_wp_after_naive` | double |  |
| `home_wp_before_naive` | double |  |
| `home_wp_after_naive` | double |  |
| `lead_wp_before_naive` | double |  |
| `lead_wp_before2_naive` | double |  |
| `wp_touchback_naive` | double |  |
| `away_wp_before_naive` | double |  |
| `away_wp_after_naive` | double |  |
| `cp` | character | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | character | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `xpass` | double | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | double | Dropback percent over expected on a given play scaled from 0 to 100. |
| `xyac_epa` | character | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | character | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | character | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | character | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | character | Probability play earns a first down based on where the ball was caught. |
| `drive_start` | double |  |
| `drive_stopped` | logical |  |
| `drive_play_index` | integer |  |
| `drive_offense_plays` | integer |  |
| `prog_drive_EPA` | double |  |
| `prog_drive_WPA` | double |  |
| `drive_offense_yards` | integer |  |
| `drive_total_yards` | integer |  |
| `fixed_drive` | integer | Manually created drive number in a game. |
| `fixed_drive_result` | character | Manually created drive result. |
| `series` | integer | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_result` | character | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `series_success` | integer | 1: scored touchdown, gained enough yards for first down. |
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
| `two_pt_wp` | double |  |
| `xp_wp` | double |  |
| `prob_2pt` | double |  |
| `two_pt_wp_diff` | double |  |
| `two_pt_recommendation` | character |  |
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
| `athlete_name` | character |  |
| `sack_player_id2` | character |  |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `punter_player_id` | character | Unique identifier for the punter. |
| `fg_kicker_player_id` | character |  |
| `sack_player_id` | character | Unique identifier of the player who recorded a solo sack. |
| `punt_return_player_id` | character |  |
| `kickoff_return_player_id` | character |  |
| `interception_player_id` | character | Unique identifier for the player that intercepted the pass. |
| `pass_breakup_player_id` | character |  |
| `fumble_forced_player_id` | character |  |
| `fumble_recovered_player_id` | character |  |
| `fumble_player_id` | character |  |
| `punt_block_player_id` | character |  |
| `punt_block_return_player_id` | character |  |
| `kickoff_player_id` | character |  |
| `fg_block_player_id` | character |  |
| `fg_return_player_id` | character |  |
| `xp_kicker_player_id` | character |  |

**Example**

```python
# For most use cases, call the high-level entry point instead. ``enrich_nfl_pbp`` scores WP, derives WPA, and adds EP/EPA/CP/CPOE in one shot on any nflverse-shape frame

    from sportsdataverse.nfl import load_nfl_pbp
    from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

    pbp = load_nfl_pbp([2023])
    enriched = enrich_nfl_pbp(pbp)
    print(enriched.select("game_id", "wp", "def_wp", "home_wp", "away_wp", "wpa").head())

``calculate_wpa`` directly requires ESPN-internal columns
(``wp_before``, ``wp_touchback``, ``wp_after``, ``homeTeamId``,
``start.pos_team.id``, etc.) produced by ``NFLPlayProcess``.  It is
called internally by ``NFLPlayProcess.__process_wpa`` and by the
``enrich_nfl_pbp`` orchestrator — a naked
``calculate_wpa(load_nfl_pbp([2023]))`` will raise ``KeyError``
because those columns are absent from a nflverse frame.
```
