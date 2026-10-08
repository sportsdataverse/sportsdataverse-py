---
title: "NFL — additional Python functions — Models and calculators: calculate_wpa"
sidebar_label: "Models and calculators: calculate_wpa"
sidebar_position: 18
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
| `scoringPlay` | logical | ESPN flag marking the play as a scoring play. |
| `priority` | logical |  |
| `modified` | character |  |
| `wallclock` | character |  |
| `teamParticipants` | integer | Raw ESPN team-level participants payload carried through from the plays feed (stringified). |
| `isPenalty` | logical | ESPN's per-play flag that a penalty occurred on the play. |
| `statYardage` | integer | Yardage ESPN credits to the play for statistical purposes. |
| `isTurnover` | logical | ESPN's per-play turnover flag as shipped in the plays feed (broader than the giveaway-based is_turnover derivation). |
| `type.id` | character | ESPN's numeric identifier for the play type. |
| `type.text` | character | ESPN's text label for the play type. |
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
| `end.team.id` | integer | ESPN's `team.id` value for the play state at the end of the play. |
| `type.abbreviation` | character | ESPN's abbreviation for the play type. |
| `start.downDistanceText` | character | ESPN's `downDistanceText` value for the play state at the start of the play. |
| `start.shortDownDistanceText` | character | ESPN's `shortDownDistanceText` value for the play state at the start of the play. |
| `start.possessionText` | character | ESPN's `possessionText` value for the play state at the start of the play. |
| `end.downDistanceText` | character | ESPN's `downDistanceText` value for the play state at the end of the play. |
| `end.shortDownDistanceText` | character | ESPN's `shortDownDistanceText` value for the play state at the end of the play. |
| `end.possessionText` | character | ESPN's `possessionText` value for the play state at the end of the play. |
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
| `game_id` | integer | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | integer | ESPN season type for the game (2 = regular season, 3 = postseason). |
| `week` | integer | Season week. |
| `status_type_completed` | logical |  |
| `homeTeamId` | integer | ESPN's home-team Id for the game, stamped on every play. |
| `awayTeamId` | integer | ESPN's away-team Id for the game, stamped on every play. |
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
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `homeTeamSpread` | double | ESPN's home-team Spread for the game, stamped on every play. |
| `clock.minutes` | integer | Minutes remaining on the game clock at the play. |
| `clock.seconds` | integer | Seconds component of the game clock at the play. |
| `half` | integer |  |
| `lag_half` | integer | Value of half on the previous play, used for sequence-aware derivations. |
| `lead_half` | integer | Value of half on the next play, used for sequence-aware derivations. |
| `start.TimeSecsRem` | integer | Seconds remaining in the half from ESPN's clock stamp for this play, which is the end-of-play time in 2005 and 2007+ (the snap time in 2004 and most of 2006); tops out at 1800. |
| `start.adj_TimeSecsRem` | integer | ESPN's `adj_TimeSecsRem` value for the play state at the start of the play. |
| `orig_play_type` | character |  |
| `lead_text` | character | Value of text on the next play, used for sequence-aware derivations. |
| `lead_start_team` | character | Value of start_team on the next play, used for sequence-aware derivations. |
| `lead_start_yardsToEndzone` | integer | Value of start_yardsToEndzone on the next play, used for sequence-aware derivations. |
| `lead_start_down` | integer | Value of start_down on the next play, used for sequence-aware derivations. |
| `lead_start_distance` | integer | Value of start_distance on the next play, used for sequence-aware derivations. |
| `lead_scoringPlay` | logical | Value of scoringPlay on the next play, used for sequence-aware derivations. |
| `text_dupe` | logical | Always False in the emitted frame -- the duplicate-row filter it gates runs before the column is returned, so it marks nothing and is retained only for schema stability. |
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
| `period` | integer |  |
| `start.yard` | integer | ESPN's `yard` value for the play state at the start of the play. |
| `end.yard` | integer | ESPN's `yard` value for the play state at the end of the play. |
| `lag_scoringPlay` | logical | Value of scoringPlay on the previous play, used for sequence-aware derivations. |
| `end_of_half` | logical |  |
| `down_1` | logical | True when it is 1st down at the start of the play. |
| `down_2` | logical | True when it is 2nd down at the start of the play. |
| `down_3` | logical | True when it is 3rd down at the start of the play. |
| `down_4` | logical | True when it is 4th down at the start of the play. |
| `down_1_end` | logical | True when it is 1st down at the end of the play. |
| `down_2_end` | logical | True when it is 2nd down at the end of the play. |
| `down_3_end` | logical | True when it is 3rd down at the end of the play. |
| `down_4_end` | logical | True when it is 4th down at the end of the play. |
| `scoring_play` | logical |  |
| `td_play` | logical |  |
| `touchdown` | logical | Binary indicator for if the play resulted in a TD. |
| `td_check` | logical | Internal flag used while reconciling whether the play produced a touchdown. |
| `safety` | logical | Binary indicator for whether or not a safety occurred. |
| `fumble_vec` | logical |  |
| `forced_fumble` | logical | True when the defense forced a fumble on the play. |
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
| `pos_team_score` | integer |  |
| `def_pos_team_score` | integer |  |
| `start.pos_team_score` | integer | ESPN's `pos_team_score` value for the play state at the start of the play. |
| `start.def_pos_team_score` | integer | ESPN's `def_pos_team_score` value for the play state at the start of the play. |
| `start.pos_score_diff` | integer | ESPN's `pos_score_diff` value for the play state at the start of the play. |
| `end.pos_team_score` | integer | ESPN's `pos_team_score` value for the play state at the end of the play. |
| `end.def_pos_team_score` | integer | ESPN's `def_pos_team_score` value for the play state at the end of the play. |
| `end.pos_score_diff` | integer | ESPN's `pos_score_diff` value for the play state at the end of the play. |
| `lag_pos_team` | integer |  |
| `lead_pos_team` | integer |  |
| `lead_pos_team2` | integer | Value of pos_team 2 plays ahead, used for sequence-aware derivations. |
| `pos_score_diff` | integer |  |
| `lag_pos_score_diff` | integer |  |
| `pos_score_pts` | integer |  |
| `pos_score_diff_start` | integer |  |
| `start.pos_team_receives_2H_kickoff` | logical | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the start of the play. |
| `end.pos_team_receives_2H_kickoff` | logical | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the end of the play. |
| `change_of_poss` | logical |  |
| `penalty_flag` | logical |  |
| `penalty_declined` | logical |  |
| `penalty_no_play` | logical |  |
| `penalty_offset` | logical |  |
| `penalty_1st_conv` | logical |  |
| `penalty_in_text` | logical | True when the play description mentions a penalty. |
| `penalty_detail` | character |  |
| `penalty_text` | character |  |
| `yds_penalty` | character |  |
| `penalty_count` | integer | Number of penalties flagged on the play (0-4 observed). |
| `penalty_declined_count` | integer | Number of the flagged penalties that were declined. |
| `penalty_all_declined` | logical | Whether every penalty flagged on the play was declined. |
| `penalty_enforcement` | character | How the penalty was resolved: one of no_play, declined, offsetting, negating_foul, play_stands, unknown. |
| `penalty_negated_play` | logical | Whether the penalty negated the play's result. |
| `sack` | logical | Binary indicator for if the play ended in a sack. |
| `int` | logical |  |
| `int_td` | logical |  |
| `completion` | logical |  |
| `pass_attempt` | logical | Binary indicator for if the play was a pass attempt (includes sacks). |
| `target` | logical |  |
| `pass_breakup` | logical | True when a defender broke up the pass. |
| `pass_td` | logical |  |
| `rush_td` | logical |  |
| `pass_depth` | character | Thrown-pass depth parsed from ESPN play text ("short" or "deep"); null when the text omits it (sacks, screens, pre-2025 text). |
| `pass_direction` | character | Pass direction parsed from ESPN play text ("left", "middle", or "right"); null when the text omits it. |
| `rush_direction` | character | Rush direction parsed from ESPN play text ("left", "middle", or "right"); null when the text omits it. |
| `turnover_vec` | logical |  |
| `offense_score_play` | logical |  |
| `defense_score_play` | logical |  |
| `downs_turnover` | logical |  |
| `yds_punted` | integer |  |
| `yds_punt_gained` | integer |  |
| `fg_attempt` | logical | True when the play was a field-goal attempt. |
| `fg_made` | logical |  |
| `yds_fg` | integer |  |
| `pos_unit` | character |  |
| `def_pos_unit` | character |  |
| `lead_play_type` | character |  |
| `sp` | logical | Binary indicator for whether or not a score occurred on the play. |
| `play` | logical | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `scrimmage_play` | logical | True when the play is a play from scrimmage rather than a special-teams or administrative row. |
| `change_of_pos_team` | logical |  |
| `pos_score_diff_end` | integer | Score differential from the possessing team's perspective at the end of the play. |
| `fumble_lost` | logical | Binary indicator for if the fumble was lost. |
| `fumble_recovered` | logical | True when a fumble on the play was recovered. |
| `field_goal_result` | character | String indicator for result of field goal attempt: made, missed, or blocked. |
| `extra_point_result` | character | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | character | String result of the two-point conversion attempt: success, failure, or safety (touchback in the defensive end zone). |
| `kneel_down` | logical | Whether the play is an offensive kneel, from explicit kneel text plus an end-of-half TEAM-rush heuristic. |
| `qb_hurry` | logical | Whether ESPN's play text says the quarterback was hurried into the throw ("hurried by ..."). |
| `xp_attempt` | logical | Whether an extra-point kick was attempted on the play. |
| `xp_made` | logical | Whether the extra-point kick was successful. |
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
| `xp_kicker_player_name` | character | Name of the kicker attempting the extra point. |
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
| `kickoff_return_player_name` | character | Name of the player returning the kickoff, when the play was returned. |
| `punter_player_name` | character | String name for the punter. |
| `punt_block_player_name` | character |  |
| `punt_return_player_name` | character | Name of the player returning the punt, when the punt was returned. |
| `punt_block_return_player_name` | character |  |
| `fumble_player_name` | character |  |
| `fumble_forced_player_name` | character |  |
| `fumble_recovered_player_name` | character |  |
| `kicking_team` | integer | Team id of the kicking team on kickoff, punt, and field-goal plays. |
| `return_team` | integer | Team id of the returning side; set on interception, fumble, kickoff, punt, and blocked-kick returns. |
| `fumble_or_muff` | logical | Whether the play includes a fumble or a muffed kick or punt (widened beyond ESPN's fumble play types). |
| `recovery_team` | character | Team id parsed from the play text as recovering the fumble or muff. |
| `recovery_team_2` | character | Team id of the second recovery in a multi-recovery scramble, parsed from the play text. |
| `penalty_spot_yardline` | integer | Yard line (0-50) at which the penalty was spotted. |
| `penalty_spot_side` | character | Side of the field the penalty was spotted on: 'home', 'away' or 'mid' (midfield). |
| `penalty_spot_yardsToEndzone` | integer | Yards from the penalty spot to the end zone (0-100). |
| `fumbling_team` | character | Team id of the side that fumbled or muffed the ball, parsed from the play text. |
| `int_turnover` | logical | Whether the play is an interception giveaway. |
| `pos_fumble_lost` | logical | Whether the possession team fumbled and lost the ball. |
| `def_fumble_lost` | logical | Whether the defending team (e.g. a returner after a takeaway) fumbled and lost the ball back. |
| `is_pos_team_turnover` | logical | Whether the possession team committed a giveaway (interception or fumble lost). |
| `is_def_pos_team_turnover` | logical | Whether the defending team gave the ball back via a lost fumble. |
| `is_turnover` | logical | True when the play is a giveaway-based turnover (interception thrown or fumble lost); blocked kicks recovered by the defense are carried by the blocked-kick fields instead. |
| `turnover_team` | character | Team id charged with the giveaway on the play. |
| `is_st_turnover` | logical | Whether the giveaway happened on a special-teams play (kick or punt snap, or a return). |
| `is_blocked_punt_turnover` | logical | Blocked-punt possession loss (blocked-punt TD, or the defense recovered); kept out of is_turnover to match ESPN's giveaway-only box. |
| `is_blocked_fg_turnover` | logical | Blocked-field-goal possession loss (blocked-FG TD, or the defense recovered); kept out of is_turnover to match ESPN's giveaway-only box. |
| `sack_team` | integer | Team id credited with the sack (the defense). |
| `interception_team` | integer | Team id credited with the interception (the defense). |
| `pass_breakup_team` | integer | Team id credited with the pass breakup (the defense). |
| `forced_fumble_team` | integer | Team id credited with forcing the fumble -- the side opposite the fumbling player (the covering team on returns). |
| `fumble_recovery_team` | character | Team id that recovered the fumble or muff, from parsed text with a giveaway / own-recovery fallback. |
| `punt_return_team` | integer | Team id of the punt-returning side. |
| `kick_return_team` | integer | Team id of the kick-returning side. |
| `fg_team` | integer | Team id attempting the field goal (the kicking team). |
| `punt_team` | integer | Team id punting the ball (the kicking team). |
| `penalized_team` | integer | Team id the penalty was assessed against, from the home/away text resolver with a foul-direction fallback. |
| `penalty_yards_signed` | integer | Penalty yardage parsed from the play text with era-aware bounds; the printed sign is retained but is not a reliable enforcement direction. |
| `penalty_side` | character | Which side committed the penalty -- 'off' (offense) or 'def' (defense). |
| `penalty_yards_net` | integer | Net yardage assessed for the penalty, signed relative to the possession team (observed -25 to 25). |
| `penalty_team_id` | integer | Team id of the side that committed the penalty. |
| `lateral_player_name` | character |  |
| `yds_lateral` | character |  |
| `yards_after_catch` | character | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `air_yards` | character | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `air_yardsToEndzone` | character | Yards to the endzone at the catch spot, parsed from the 2025+ vendor catch-spot text; null before 2025 or when unresolvable. |
| `new_down` | integer | Down after the play, including any penalty enforcement. |
| `new_distance` | integer | Distance to go after the play, including any penalty enforcement. |
| `middle_8` | logical |  |
| `rz_play` | logical |  |
| `under_2` | logical | Whether the play began with two minutes or less remaining in the half. |
| `goal_to_go` | logical | Binary indicator for whether or not the posteam is in a goal down situation. |
| `scoring_opp` | logical |  |
| `stuffed_run` | logical |  |
| `stopped_run` | logical | True when the rush was stopped at or behind the line of scrimmage. |
| `opportunity_run` | logical |  |
| `highlight_run` | logical | True when the rush gained 8 or more yards. |
| `adj_rush_yardage` | integer | Rushing yards capped at 8, the input to the line-yards decomposition. |
| `line_yards` | double | Yards credited to the offensive line on a rush, using the standard sliding scale: 1.2x the capped yardage on a loss, all of it through 3 yards, half of each yard from 4 to 8, and a 5.5-yard ceiling beyond that. |
| `second_level_yards` | double | Rushing yards earned from 4 to 8, split evenly between line and carrier under the line-yards decomposition. |
| `open_field_yards` | integer | Rushing yards gained beyond 8, credited to the ball carrier rather than the line. |
| `highlight_yards` | double | Second-level plus open-field yards -- the yardage credited to the carrier. |
| `opp_highlight_yards` | double | Highlight yards earned on opportunity runs, isolating carrier production on carries where the blocking succeeded. Assets published before the 2026-08 fix are identically 0 here, because the inverted opportunity_run gate could never co-occur with non-zero highlight yards. |
| `short_rush_success` | logical | True when a short-yardage rush gained the yardage needed. |
| `short_rush_attempt` | logical | True when the play is a rush in a short-yardage situation. |
| `power_rush_success` | logical | True when a power rushing attempt gained the yardage needed. |
| `power_rush_attempt` | logical | True when the play is a short-yardage power rushing attempt. |
| `early_down` | logical | True when the play is a scrimmage play on first or second down. |
| `late_down` | logical | True when the play is a scrimmage play on third or fourth down. |
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
| `first_down_penalty` | logical | Binary indicator for if a penalty converted the first down. |
| `first_down_earned` | logical | Whether the play earned a first down by means other than yardage (e.g. by penalty). |
| `new_series` | logical |  |
| `firstD_by_kickoff` | logical |  |
| `firstD_by_poss` | logical |  |
| `firstD_by_penalty` | logical |  |
| `firstD_by_yards` | logical |  |
| `start.pos_team_spread` | double | ESPN's `pos_team_spread` value for the play state at the start of the play. |
| `start.elapsed_share` | double | ESPN's `elapsed_share` value for the play state at the start of the play. |
| `start.spread_time` | double | ESPN's `spread_time` value for the play state at the start of the play. |
| `end.pos_team_spread` | double | ESPN's `pos_team_spread` value for the play state at the end of the play. |
| `end.elapsed_share` | double | ESPN's `elapsed_share` value for the play state at the end of the play. |
| `end.spread_time` | double | ESPN's `spread_time` value for the play state at the end of the play. |
| `pass_length` | character | String indicator for pass length: short or deep. |
| `pass_location` | character | String indicator for pass location: left, middle, or right. |
| `shotgun` | integer | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | integer | Binary indicator for whether or not the play was in no_huddle formation. |
| `pass_middle` | integer |  |
| `down` | integer | The down for the given play. |
| `distance` | integer |  |
| `start.yardsToEndzone.touchback` | integer | ESPN's `yardsToEndzone.touchback` value for the play state at the start of the play. |
| `penalty_assessed_on_kickoff` | logical | Whether a penalty was assessed on a kickoff; such plays take the kickoff/touchback win-probability handling. |
| `EP_start_touchback` | double | Expected points the offense would have had from a touchback on this play. |
| `EP_start` | double | Expected points for the offense at the start of the play. |
| `EP_end` | double | Expected points for the offense at the end of the play. |
| `EP_penalty_cf` | character | Counterfactual expected points for the penalty branch -- the EP had the alternative penalty outcome been taken (null unless a penalty decision existed). |
| `penalty_cf_yardsToEndzone` | character | Yards to the end zone in the counterfactual penalty branch. |
| `lag_EP_end` | double | Value of EP_end on the previous play, used for sequence-aware derivations. |
| `lag_change_of_pos_team` | logical |  |
| `EP_between` | double | Change in expected points across the play, before penalty adjustment. |
| `EPA` | double |  |
| `def_EPA` | double |  |
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
| `qb_epa` | double | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `start.ExpScoreDiff_touchback` | double | ESPN's `ExpScoreDiff_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff` | double | ESPN's `ExpScoreDiff` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio_touchback` | double | ESPN's `ExpScoreDiff_Time_Ratio_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio` | double | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the start of the play. |
| `end.ExpScoreDiff` | double | ESPN's `ExpScoreDiff` value for the play state at the end of the play. |
| `end.ExpScoreDiff_Time_Ratio` | double | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the end of the play. |
| `wp_before` | double |  |
| `wp_touchback` | double | Win probability the offense would have had starting from a touchback. |
| `wp_after` | double |  |
| `def_wp_before` | double |  |
| `home_wp_before` | double |  |
| `away_wp_before` | double |  |
| `lead_wp_before` | double | Value of wp_before on the next play, used for sequence-aware derivations. |
| `lead_wp_before2` | double | Value of wp_before 2 plays ahead, used for sequence-aware derivations. |
| `def_wp_after` | double |  |
| `home_wp_after` | double |  |
| `away_wp_after` | double |  |
| `wpa` | double | Win probability added (WPA) for the posteam. |
| `wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `vegas_wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `def_wp` | double | Estimated win probability for the defteam. |
| `home_wp` | double | Estimated win probability for the home team. |
| `away_wp` | double | Estimated win probability for the away team. |
| `wp_before_naive` | double | Pre-snap possession-team win probability from the spread-free (naive) WP model. |
| `wp_after_naive` | double | End-of-play possession-team win probability from the naive model, after the game-logic adjustment chain. |
| `wpa_naive` | double | Win probability added on the play under the spread-free (naive) model. |
| `def_wp_before_naive` | double | Pre-snap defense win probability under the naive model (1 - wp_before_naive). |
| `def_wp_after_naive` | double | End-of-play defense win probability under the naive model. |
| `home_wp_before_naive` | double | Pre-snap naive win probability mapped to the home team. |
| `home_wp_after_naive` | double | End-of-play naive win probability mapped to the home team. |
| `lead_wp_before_naive` | double | Next play's pre-snap naive win probability, used in the end-of-half and change-of-possession adjustments. |
| `lead_wp_before2_naive` | double | Pre-snap naive win probability two plays ahead, used where the immediately following row is a non-play. |
| `wp_touchback_naive` | double | Naive-model win probability for the kickoff-touchback substitute state, used as the pre-snap WP on kickoffs. |
| `away_wp_before_naive` | double | Pre-snap naive win probability mapped to the away team. |
| `away_wp_after_naive` | double | End-of-play naive win probability mapped to the away team. |
| `cp` | character | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | character | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `xpass` | double | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | double | Dropback percent over expected on a given play scaled from 0 to 100. |
| `xyac_epa` | character | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | character | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | character | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | character | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | character | Probability play earns a first down based on where the ball was caught. |
| `drive_start` | double | Yard line at which the drive began. |
| `drive_stopped` | logical | True when the play ended the drive. |
| `drive_play_index` | integer | Sequence number of the play within its drive. |
| `drive_offense_plays` | integer | Offensive plays run on the drive. |
| `prog_drive_EPA` | double | Cumulative EPA accrued by the drive up to and including this play. |
| `prog_drive_WPA` | double | Cumulative win-probability added by the drive up to and including this play. |
| `drive_offense_yards` | integer | Offensive yards gained on the drive. |
| `drive_total_yards` | integer | Total yards gained on the drive. |
| `fixed_drive` | integer | Manually created drive number in a game. |
| `fixed_drive_result` | character | Manually created drive result. |
| `series` | integer | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_result` | character | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `series_success` | integer | 1: scored touchdown, gained enough yards for first down. |
| `go_wp` | double | Win probability from going for it on fourth down: conversion-probability-weighted mean of the success and failure states (cfb4th port). |
| `first_down_prob` | double | Modeled probability of converting the fourth down when going for it. |
| `wp_succeed` | double | Mean win probability across yardage outcomes given the fourth-down attempt converts. |
| `wp_fail` | double | Mean win probability given the fourth-down attempt fails. |
| `fg_make_prob` | double |  |
| `make_fg_wp` | double | Win probability given the field-goal attempt is made. |
| `miss_fg_wp` | double | Win probability given the field-goal attempt misses. |
| `fg_wp` | double | Make-probability-weighted win probability of attempting the field goal. |
| `punt_wp` | double | Win probability of punting, from the bundled punt-outcome distribution. |
| `go_boost` | double | cfb4th's headline number: 100 * (go_wp - max(fg_wp, punt_wp)), in percentage points. |
| `go_wp_diff` | double | go_wp minus the recommended option's WP (0 when going for it is the recommendation, otherwise <= 0). |
| `punt_wp_diff` | double | punt_wp minus the recommended option's WP (0 when punting is the recommendation, otherwise <= 0). |
| `fg_wp_diff` | double | fg_wp minus the recommended option's WP (0 when the field goal is the recommendation, otherwise <= 0). |
| `fourth_down_recommendation` | character | Max-WP fourth-down choice among "go", "punt", and "field_goal". |
| `two_pt_wp` | double | Win probability of going for two: conversion-probability-weighted mean of the 2-point and 0-point outcomes (cfb4th port). |
| `xp_wp` | double |  |
| `prob_2pt` | double |  |
| `two_pt_wp_diff` | double | two_pt_wp minus xp_wp; positive favors going for two. |
| `two_pt_recommendation` | character | Point-after recommendation: "go_for_2" when two_pt_wp exceeds xp_wp, otherwise "kick_xp". |
| `qbr_epa` | double | EPA variant used as an input to the QBR calculation. |
| `weight` | double | Official weight, in pounds |
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
| `athlete_name` | character |  |
| `sack_player_id2` | character | ESPN athlete id of the second sacker on a split sack (regex fallback for an ESPN sidecar blind spot). |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `punter_player_id` | character | Unique identifier for the punter. |
| `fg_kicker_player_id` | character | ESPN athlete id of the field-goal kicker. |
| `sack_player_id` | character | Unique identifier of the player who recorded a solo sack. |
| `punt_return_player_id` | character | ESPN athlete id of the punt returner. |
| `kickoff_return_player_id` | character | ESPN athlete id of the kickoff returner. |
| `interception_player_id` | character | Unique identifier for the player that intercepted the pass. |
| `pass_breakup_player_id` | character |  |
| `fumble_forced_player_id` | character |  |
| `fumble_recovered_player_id` | character |  |
| `fumble_player_id` | character |  |
| `punt_block_player_id` | character | ESPN athlete id of the player who blocked the punt. |
| `punt_block_return_player_id` | character | ESPN athlete id of the player who returned the blocked punt. |
| `kickoff_player_id` | character | ESPN athlete id of the player kicking off. |
| `fg_block_player_id` | character | ESPN athlete id of the player who blocked the field goal. |
| `fg_return_player_id` | character | ESPN athlete id of the player who returned the blocked or missed field goal. |
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
