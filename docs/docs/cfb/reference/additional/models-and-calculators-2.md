---
title: "CFB — additional Python functions — Models and calculators: fei_ratings–load_recruit"
sidebar_label: "Models and calculators: fei_ratings–load_recruit"
sidebar_position: 8
description: "CFB — additional Python functions — Models and calculators: fei_ratings–load_recruit — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Models and calculators: fei_ratings–load_recruit

### fei_ratings {#fei_ratings}

`fei_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted per-drive efficiency (FEI-style).

The Fremeau Efficiency Index rates teams on drive value above expectation
given starting field position. The cfbfastR-schema `plays` frame this
package works with carries no starting-field-position column, so this
function uses the documented fallback: per-play EPA summed within each
`(game_id, drive_id)` group stands in for drive value, and that
aggregate is fit through the same opponent-adjustment ridge as
`efficiency_ratings` / `special_teams_ratings` -- no forked
solver. Offline validation against the Fremeau FEI oracle put this
fallback's team ranking at Spearman 0.967.

`cfb_adjusted_epa._prepare` filters to individual pass/rush plays and
is not reused here (drive value should reflect every play on the drive,
special-teams snaps included); the `hfa` treatment is reproduced
directly, matching `special_teams_ratings`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying every column in `cfb_adjusted_epa._REQUIRED_COLUMNS` (`game_id`, `pos_team`, `pos_team_id`, `def_pos_team_id`, `home`, `neutral_site`, `EPA`, `pass`, `rush`, `wp_before`) plus `drive_id`. Not pre-aggregated to drives -- this function does that grouping itself. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs. Only `ridge_lambda` is consulted here; defaults to `RatingsConfig` when omitted. |

**Returns**

A `polars.DataFrame` with one row per `team_id` appearing as `pos_team_id` on at least one drive: `team_id` (Utf8), `fei_off` / `fei_def` / `fei_net` (Float64). The ridge's dropped reference team is re-added at the shared intercept (`fei_net == 0.0`). Zero-row (correctly-typed) when `plays` has no rows with a non-null `EPA`.

No returns table is published for this function: no capture: it needs play-by-play joined with schedule fields (home, neutral_site, pos_team_id) that neither load_cfb_pbp nor load_cfb_pbp_r carries; only cfb_ratings builds that join, internally.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import fei_ratings
fei = fei_ratings(pbp)
fei.sort("fei_net", descending=True).head()
```

### fit_field_position_ep {#fit_field_position_ep}

`fit_field_position_ep(drives: 'pl.DataFrame', *, start_col: 'str' = 'drive_start_yardline', pts_col: 'str' = 'drive_next_score_pts') -> 'pl.DataFrame'`

Fit the monotone EP-by-starting-yardline curve from a drives frame.

Groups drives by starting yard line (from own goal), takes the mean
next-score points, and applies sample-count-weighted isotonic regression
(weight = number of drives at each starting yard line, non-decreasing),
interpolated onto the full 1..99 grid.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drives` | `DataFrame` |  | one row per drive. |
| `start_col` | `str` | `'drive_start_yardline'` | starting yard line from own goal (1..99). |
| `pts_col` | `str` | `'drive_next_score_pts'` | net next-score points for the drive's offense. |

**Returns**

`yardline_own: Int64 (1..99), ep: Float64` -- monotone non-decreasing. Empty input returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `yardline_own` | integer | Starting yard line from the offense's own goal (1-99). |
| `ep` | double | Fitted expected points for a drive starting at this yard line (isotonic, non-decreasing). |

**Example**

```python
import polars as pl
from sportsdataverse.cfb.cfb_field_position import fit_field_position_ep
curve = fit_field_position_ep(drives_frame)
```

### get_2pt_probs {#get_2pt_probs}

`get_2pt_probs(pbp_df: 'Any') -> 'pd.DataFrame'`

Two-point-conversion decision surface (cfb4th `get_2pt_wp`).

Treats each row as "the scoring team just made a touchdown; decide between
the extra point and going for two". Enumerates the three point outcomes
(`0` / `1` / `2`) of the try, scores the opponent's ensuing-drive WP for
each from the scoring team's perspective, and combines them with the
two-point conversion probability (bundled CFB model) and the empirical CFB
extra-point make rate (XP_MAKE_PROB`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Any` |  | Play-by-play frame (polars or pandas) carrying the `start.*` state columns in `sportsdataverse.cfb.cfb_fourth_down._PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` plus: * `two_pt_wp` -- `prob_2pt * wp(pts=2) + (1 - prob_2pt) * wp(pts=0)`. * `xp_wp` -- `prob_xp * wp(pts=1) + (1 - prob_xp) * wp(pts=0)` with `prob_xp = _XP_MAKE_PROB`. * `prob_2pt` -- the bundled-model two-point conversion probability. * `two_pt_recommendation` -- `"go_for_2"` iff `two_pt_wp > xp_wp` else `"kick_xp"` (None where the inputs are NaN). * `two_pt_wp_diff` -- `two_pt_wp - xp_wp` (positive => go for 2). When the two-point model isn't bundled (`TWO_PT_MODEL_AVAILABLE` is False) or the required state columns are missing, all decision columns are null -- probabilities are never fabricated.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season (4-digit year). |
| `game_id` | integer | ESPN game identifier. |
| `game_play_number` | integer | Sequential play number within the game (excludes timeouts/end markers). |
| `pos_team_id` | integer |  |
| `pos_team` | character | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team_id` | integer |  |
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
| `yds_rushed` | double | Rushing yards gained on the play. |
| `passer_player_name` | character | Name of the passer on a passing play. |
| `receiver_player_name` | character | Name of the receiver on a passing play. |
| `yds_receiving` | double | Receiving yards gained on the play. |
| `yds_sacked` | double | Yards lost on the sack. |
| `sack_players` | character | Combined names of all sack participants. |
| `sack_player_name` | character | Primary sack player name. |
| `sack_player_name2` | character | Secondary sack player name (when split between two defenders). |
| `pass_breakup_player_name` | character | Name of the defender credited with the pass breakup. |
| `interception_player_name` | character | Name of the defender credited with the interception. |
| `yds_int_return` | double | Yards gained on an interception return. |
| `fumble_player_name` | character | Name of the player who fumbled. |
| `fumble_forced_player_name` | character | Name of the player who forced the fumble. |
| `fumble_recovered_player_name` | character | Name of the player who recovered the fumble. |
| `yds_fumble_return` | double | Yards gained on a fumble return. |
| `punter_player_name` | character | Name of the punter. |
| `yds_punted` | double | Yards the ball traveled on the punt. |
| `yds_punt_return` | double | Yards gained on the punt return. |
| `yds_punt_gained` | double | Net yards gained on the punt (punt distance minus return). |
| `punt_block_player_name` | character | Name of the player credited with blocking the punt. |
| `punt_block_return_player_name` | character | Name of the player returning a blocked punt. |
| `fg_kicker_player_name` | character | Name of the field goal kicker. |
| `yds_fg` | double | Distance of the field goal attempt in yards. |
| `fg_block_player_name` | character | Name of the player credited with blocking the field goal. |
| `fg_return_player_name` | character | Name of the player returning the blocked/missed field goal. |
| `kickoff_player_name` | character | Name of the kickoff specialist. |
| `yds_kickoff` | double | Yards the ball traveled on the kickoff. |
| `yds_kickoff_return` | double | Yards gained on the kickoff return. |
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
| `lead_wp_before2` | double | Win probability two plays ahead (lead 2 of wp_before). |
| `lead_wp_before` | double | Win probability on the next play (lead of wp_before). |
| `lead_pos_team2` | double | Possession team two plays ahead (lead 2 of pos_team). |
| `id` | integer | 247Sports referencing id for the recruit. |
| `sequenceNumber` | integer |  |
| `text` | character | Full play description. |
| `awayScore` | integer |  |
| `homeScore` | integer |  |
| `scoringPlay` | logical |  |
| `priority` | logical | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | character | ISO timestamp the play record was last modified. |
| `wallclock` | character | Real-world ISO timestamp of the play. |
| `teamParticipants` | character |  |
| `isPenalty` | logical |  |
| `statYardage` | integer |  |
| `isTurnover` | logical |  |
| `type.id` | character |  |
| `type.text` | character |  |
| `type.abbreviation` | character |  |
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
| `end.yardsToEndzone` | double |  |
| `end.downDistanceText` | character |  |
| `end.shortDownDistanceText` | character |  |
| `end.possessionText` | character |  |
| `end.team.id` | integer |  |
| `start.downDistanceText` | character |  |
| `start.shortDownDistanceText` | character |  |
| `start.possessionText` | character |  |
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
| `drive.end.period.number` | double |  |
| `drive.end.period.type` | character |  |
| `drive.end.yardLine` | double |  |
| `drive.end.clock.displayValue` | character |  |
| `seasonType` | integer |  |
| `week` | integer | Game week of the season. |
| `status_type_completed` | logical |  |
| `homeTeamId` | integer |  |
| `awayTeamId` | integer |  |
| `homeFinalScore` | integer |  |
| `awayFinalScore` | integer |  |
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
| `homeTeamSpread` | double |  |
| `clock.minutes` | integer |  |
| `clock.seconds` | integer |  |
| `lag_half` | double |  |
| `lead_half` | integer |  |
| `start.TimeSecsRem` | integer |  |
| `start.adj_TimeSecsRem` | integer |  |
| `lead_text` | character |  |
| `lead_start_team` | character |  |
| `lead_start_yardsToEndzone` | double |  |
| `lead_start_down` | double |  |
| `lead_start_distance` | double |  |
| `lead_scoringPlay` | character |  |
| `text_dupe` | logical |  |
| `end_state_missing` | logical |  |
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
| `start.yard` | integer |  |
| `end.yard` | integer |  |
| `lag_scoringPlay` | character |  |
| `down_1` | logical |  |
| `down_2` | logical |  |
| `down_3` | logical |  |
| `down_4` | logical |  |
| `down_1_end` | logical |  |
| `down_2_end` | logical |  |
| `down_3_end` | logical |  |
| `down_4_end` | logical |  |
| `td_check` | logical |  |
| `forced_fumble` | logical |  |
| `is_home` | logical |  |
| `lag_HA_score_diff` | double |  |
| `HA_score_diff` | integer |  |
| `net_HA_score_pts` | double |  |
| `H_score_diff` | double |  |
| `A_score_diff` | double |  |
| `lag_homeScore` | integer |  |
| `lag_awayScore` | integer |  |
| `start.homeScore` | integer |  |
| `start.awayScore` | integer |  |
| `end.homeScore` | integer |  |
| `end.awayScore` | integer |  |
| `start.pos_team_score` | integer |  |
| `start.def_pos_team_score` | integer |  |
| `start.pos_score_diff` | integer |  |
| `end.pos_team_score` | integer |  |
| `end.def_pos_team_score` | integer |  |
| `end.pos_score_diff` | integer |  |
| `start.pos_team_receives_2H_kickoff` | logical |  |
| `end.pos_team_receives_2H_kickoff` | logical |  |
| `penalty_in_text` | logical |  |
| `penalty_count` | integer |  |
| `penalty_declined_count` | integer |  |
| `penalty_all_declined` | logical |  |
| `penalty_enforcement` | character |  |
| `penalty_negated_play` | character |  |
| `pass_breakup` | logical |  |
| `pass_depth` | character |  |
| `pass_direction` | character |  |
| `rush_direction` | character |  |
| `qb_hurry` | logical |  |
| `fg_attempt` | logical |  |
| `pos_unit` | character | Possession-team unit label (offense or special teams). |
| `def_pos_unit` | character | Defensive possession-team unit label (defense or special teams). |
| `sp` | logical |  |
| `play` | logical | Binary flag indicating the row is a counted play (excludes end markers/timeouts/penalties). |
| `cleaned_text` | character |  |
| `kneel_down` | logical |  |
| `scrimmage_play` | logical |  |
| `pos_score_diff_end` | integer |  |
| `fumble_lost` | logical |  |
| `fumble_recovered` | logical |  |
| `field_goal_result` | character |  |
| `extra_point_result` | character |  |
| `two_point_conv_result` | character |  |
| `defensive_two_point_attempt` | logical |  |
| `defensive_two_point_conv` | logical |  |
| `yds_punted_source` | character |  |
| `yds_kickoff_source` | character |  |
| `yds_punt_return_source` | character |  |
| `air_yardsToEndzone` | double |  |
| `air_yards` | double |  |
| `yards_after_catch` | double |  |
| `kickoff_return_player_name` | character |  |
| `punt_return_player_name` | character |  |
| `xp_attempt` | logical |  |
| `xp_made` | logical |  |
| `xp_kicker_player_name` | character |  |
| `kicking_team` | double |  |
| `return_team` | double |  |
| `fumble_or_muff` | logical |  |
| `recovery_team` | double |  |
| `recovery_team_2` | double |  |
| `penalty_spot_yardline` | double |  |
| `penalty_spot_side` | character |  |
| `penalty_spot_yardsToEndzone` | double |  |
| `fumbling_team` | double |  |
| `int_turnover` | logical |  |
| `pos_fumble_lost` | logical |  |
| `def_fumble_lost` | logical |  |
| `is_pos_team_turnover` | logical |  |
| `is_def_pos_team_turnover` | logical |  |
| `is_turnover` | logical | `TRUE` if the play was a turnover. |
| `turnover_team` | double |  |
| `is_st_turnover` | logical |  |
| `is_blocked_punt_turnover` | logical |  |
| `is_blocked_fg_turnover` | logical |  |
| `sack_team` | integer |  |
| `interception_team` | integer |  |
| `pass_breakup_team` | integer |  |
| `forced_fumble_team` | integer |  |
| `fumble_recovery_team` | double |  |
| `punt_return_team` | double |  |
| `kick_return_team` | double |  |
| `fg_team` | double |  |
| `punt_team` | double |  |
| `penalized_team` | double |  |
| `penalty_yards_signed` | integer |  |
| `penalty_side` | character |  |
| `penalty_yards_net` | double |  |
| `penalty_team_id` | double |  |
| `new_down` | integer |  |
| `new_distance` | integer |  |
| `under_2` | logical |  |
| `goal_to_go` | logical |  |
| `stopped_run` | logical |  |
| `opportunity_run` | logical |  |
| `highlight_run` | logical |  |
| `adj_rush_yardage` | double |  |
| `line_yards` | double |  |
| `second_level_yards` | double |  |
| `open_field_yards` | double |  |
| `highlight_yards` | double |  |
| `opp_highlight_yards` | double |  |
| `short_rush_success` | character |  |
| `short_rush_attempt` | character |  |
| `early_down` | logical |  |
| `late_down` | logical |  |
| `power_rush_attempt` | character |  |
| `power_rush_success` | character |  |
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
| `first_down_penalty` | logical |  |
| `first_down_earned` | logical |  |
| `start.pos_team_spread` | double |  |
| `start.elapsed_share` | double |  |
| `start.spread_time` | double |  |
| `end.pos_team_spread` | double |  |
| `end.elapsed_share` | double |  |
| `end.spread_time` | double |  |
| `penalty_assessed_on_kickoff` | logical |  |
| `start.yardsToEndzone.touchback` | integer |  |
| `EP_start_touchback` | double |  |
| `EP_start` | double |  |
| `EP_end` | double |  |
| `EP_penalty_cf` | double |  |
| `penalty_cf_yardsToEndzone` | double |  |
| `lag_EP_end` | double |  |
| `EP_between` | double |  |
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
| `start.ExpScoreDiff_touchback` | double |  |
| `start.ExpScoreDiff` | double |  |
| `start.ExpScoreDiff_Time_Ratio_touchback` | double |  |
| `start.ExpScoreDiff_Time_Ratio` | double |  |
| `end.ExpScoreDiff` | double |  |
| `end.ExpScoreDiff_Time_Ratio` | double |  |
| `wp_touchback` | double |  |
| `wp_before_naive` | double |  |
| `wp_touchback_naive` | double |  |
| `wp_after_naive` | double |  |
| `def_wp_before_naive` | double |  |
| `home_wp_before_naive` | double |  |
| `away_wp_before_naive` | double |  |
| `lead_wp_before_naive` | double |  |
| `lead_wp_before2_naive` | double |  |
| `def_wp_after_naive` | double |  |
| `home_wp_after_naive` | double |  |
| `away_wp_after_naive` | double |  |
| `wpa_naive` | double |  |
| `cp` | double |  |
| `cp_game_state` | double |  |
| `cp_model` | character |  |
| `cpoe` | double |  |
| `era` | integer |  |
| `xpass` | double |  |
| `pass_oe` | double |  |
| `drive_start` | double |  |
| `drive_stopped` | logical |  |
| `drive_play_index` | integer |  |
| `drive_offense_plays` | integer |  |
| `prog_drive_EPA` | double |  |
| `prog_drive_WPA` | double |  |
| `drive_offense_yards` | integer |  |
| `drive_total_yards` | integer |  |
| `qbr_epa` | double |  |
| `weight` | double | Listed weight (lbs). |
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
| `athlete_name` | character | Player full name. |
| `rusher_player_id` | double |  |
| `passer_player_id` | double |  |
| `receiver_player_id` | double |  |
| `fumble_player_id` | double | CFBD athlete_id of the player who fumbled. |
| `sack_player_id` | double | Comma-separated CFBD athlete_id(s) of the sacking defender(s). |
| `sack_player_id2` | double |  |
| `interception_player_id` | double | CFBD athlete_id of the defender credited with an interception. |
| `pass_breakup_player_id` | double | CFBD athlete_id of the defender credited with the pass breakup (PBU). |
| `fumble_forced_player_id` | double | CFBD athlete_id of the defender credited with forcing the fumble. |
| `fumble_recovered_player_id` | double | CFBD athlete_id of the player recovering the fumble. |
| `fg_kicker_player_id` | double |  |
| `punter_player_id` | double |  |
| `kickoff_player_id` | double |  |
| `kickoff_return_player_id` | double |  |
| `punt_return_player_id` | double |  |
| `fg_block_player_id` | double |  |
| `punt_block_player_id` | character |  |
| `fg_return_player_id` | character |  |
| `punt_block_return_player_id` | character |  |
| `go_wp` | double |  |
| `first_down_prob` | double |  |
| `wp_succeed` | double |  |
| `wp_fail` | double |  |
| `make_fg_wp` | double |  |
| `miss_fg_wp` | double |  |
| `fg_wp` | double |  |
| `punt_wp` | double |  |
| `go_boost` | double |  |
| `go_wp_diff` | double |  |
| `fg_wp_diff` | double |  |
| `punt_wp_diff` | double |  |
| `fourth_down_recommendation` | character |  |
| `two_pt_wp` | double |  |
| `xp_wp` | double |  |
| `prob_2pt` | double |  |
| `two_pt_recommendation` | character |  |
| `two_pt_wp_diff` | double |  |

**Example**

```python
from sportsdataverse.cfb.cfb_two_point import get_2pt_probs
out = get_2pt_probs(touchdown_rows)
print(out[["two_pt_wp", "xp_wp", "two_pt_recommendation"]].head())
```

### get_4th_down_probs {#get_4th_down_probs}

`get_4th_down_probs(pbp_df) -> 'pd.DataFrame'`

Full 4th-down decision surface (cfb4th `add_4th_probs`) + recommendation.

Runs `get_go_wp`, `get_fg_wp`, `get_punt_wp` on the
fourth-down rows and adds the combined option columns plus:

* `fourth_down_recommendation` -- the max-WP choice among `{go, punt,
  field_goal}` (NaN options are excluded; when the FG model isn't bundled,
  `field_goal` is excluded from the comparison).
* `go_wp_diff` / `punt_wp_diff` / `fg_wp_diff` -- each option's WP minus
  the recommended option's WP (the recommended option's diff is 0, the others
  <= 0). NaN where the option WP is NaN.
* `go_boost` -- cfb4th's headline number: `100 * (go_wp - max(fg_wp,
  punt_wp))` in percentage points.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the `start.*` state columns in PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` with the decision columns added. Empty input returns the input plus empty decision columns.

No returns table is published for this function: no capture: on the smallest real input (one released season of play-by-play) it runs longer than the capture allows.

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_4th_down_probs

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_4th_down_probs(fourth_down_rows)
print(out[["go_wp", "punt_wp", "fg_wp", "fourth_down_recommendation"]].head())
```

### get_fg_wp {#get_fg_wp}

`get_fg_wp(pbp_df) -> 'pd.DataFrame'`

Expected win probability of attempting a field goal (cfb4th `get_fg_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `fg_make_prob`, `make_fg_wp`, `miss_fg_wp` and `fg_wp` (= make_prob*make_wp + (1-make_prob)*miss_wp, from the kicking team's perspective). All four are NaN when the FG model is not bundled (`FG_MODEL_AVAILABLE` is False) -- probabilities are never fabricated.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season (4-digit year). |
| `game_id` | integer | ESPN game identifier. |
| `game_play_number` | integer | Sequential play number within the game (excludes timeouts/end markers). |
| `pos_team_id` | integer |  |
| `pos_team` | character | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team_id` | integer |  |
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
| `yds_rushed` | double | Rushing yards gained on the play. |
| `passer_player_name` | character | Name of the passer on a passing play. |
| `receiver_player_name` | character | Name of the receiver on a passing play. |
| `yds_receiving` | double | Receiving yards gained on the play. |
| `yds_sacked` | double | Yards lost on the sack. |
| `sack_players` | character | Combined names of all sack participants. |
| `sack_player_name` | character | Primary sack player name. |
| `sack_player_name2` | character | Secondary sack player name (when split between two defenders). |
| `pass_breakup_player_name` | character | Name of the defender credited with the pass breakup. |
| `interception_player_name` | character | Name of the defender credited with the interception. |
| `yds_int_return` | double | Yards gained on an interception return. |
| `fumble_player_name` | character | Name of the player who fumbled. |
| `fumble_forced_player_name` | character | Name of the player who forced the fumble. |
| `fumble_recovered_player_name` | character | Name of the player who recovered the fumble. |
| `yds_fumble_return` | double | Yards gained on a fumble return. |
| `punter_player_name` | character | Name of the punter. |
| `yds_punted` | double | Yards the ball traveled on the punt. |
| `yds_punt_return` | double | Yards gained on the punt return. |
| `yds_punt_gained` | double | Net yards gained on the punt (punt distance minus return). |
| `punt_block_player_name` | character | Name of the player credited with blocking the punt. |
| `punt_block_return_player_name` | character | Name of the player returning a blocked punt. |
| `fg_kicker_player_name` | character | Name of the field goal kicker. |
| `yds_fg` | double | Distance of the field goal attempt in yards. |
| `fg_block_player_name` | character | Name of the player credited with blocking the field goal. |
| `fg_return_player_name` | character | Name of the player returning the blocked/missed field goal. |
| `kickoff_player_name` | character | Name of the kickoff specialist. |
| `yds_kickoff` | double | Yards the ball traveled on the kickoff. |
| `yds_kickoff_return` | double | Yards gained on the kickoff return. |
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
| `lead_wp_before2` | double | Win probability two plays ahead (lead 2 of wp_before). |
| `lead_wp_before` | double | Win probability on the next play (lead of wp_before). |
| `lead_pos_team2` | double | Possession team two plays ahead (lead 2 of pos_team). |
| `id` | integer | 247Sports referencing id for the recruit. |
| `sequenceNumber` | integer |  |
| `text` | character | Full play description. |
| `awayScore` | integer |  |
| `homeScore` | integer |  |
| `scoringPlay` | logical |  |
| `priority` | logical | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | character | ISO timestamp the play record was last modified. |
| `wallclock` | character | Real-world ISO timestamp of the play. |
| `teamParticipants` | character |  |
| `isPenalty` | logical |  |
| `statYardage` | integer |  |
| `isTurnover` | logical |  |
| `type.id` | character |  |
| `type.text` | character |  |
| `type.abbreviation` | character |  |
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
| `end.yardsToEndzone` | double |  |
| `end.downDistanceText` | character |  |
| `end.shortDownDistanceText` | character |  |
| `end.possessionText` | character |  |
| `end.team.id` | integer |  |
| `start.downDistanceText` | character |  |
| `start.shortDownDistanceText` | character |  |
| `start.possessionText` | character |  |
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
| `drive.end.period.number` | double |  |
| `drive.end.period.type` | character |  |
| `drive.end.yardLine` | double |  |
| `drive.end.clock.displayValue` | character |  |
| `seasonType` | integer |  |
| `week` | integer | Game week of the season. |
| `status_type_completed` | logical |  |
| `homeTeamId` | integer |  |
| `awayTeamId` | integer |  |
| `homeFinalScore` | integer |  |
| `awayFinalScore` | integer |  |
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
| `homeTeamSpread` | double |  |
| `clock.minutes` | integer |  |
| `clock.seconds` | integer |  |
| `lag_half` | double |  |
| `lead_half` | integer |  |
| `start.TimeSecsRem` | integer |  |
| `start.adj_TimeSecsRem` | integer |  |
| `lead_text` | character |  |
| `lead_start_team` | character |  |
| `lead_start_yardsToEndzone` | double |  |
| `lead_start_down` | double |  |
| `lead_start_distance` | double |  |
| `lead_scoringPlay` | character |  |
| `text_dupe` | logical |  |
| `end_state_missing` | logical |  |
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
| `start.yard` | integer |  |
| `end.yard` | integer |  |
| `lag_scoringPlay` | character |  |
| `down_1` | logical |  |
| `down_2` | logical |  |
| `down_3` | logical |  |
| `down_4` | logical |  |
| `down_1_end` | logical |  |
| `down_2_end` | logical |  |
| `down_3_end` | logical |  |
| `down_4_end` | logical |  |
| `td_check` | logical |  |
| `forced_fumble` | logical |  |
| `is_home` | logical |  |
| `lag_HA_score_diff` | double |  |
| `HA_score_diff` | integer |  |
| `net_HA_score_pts` | double |  |
| `H_score_diff` | double |  |
| `A_score_diff` | double |  |
| `lag_homeScore` | integer |  |
| `lag_awayScore` | integer |  |
| `start.homeScore` | integer |  |
| `start.awayScore` | integer |  |
| `end.homeScore` | integer |  |
| `end.awayScore` | integer |  |
| `start.pos_team_score` | integer |  |
| `start.def_pos_team_score` | integer |  |
| `start.pos_score_diff` | integer |  |
| `end.pos_team_score` | integer |  |
| `end.def_pos_team_score` | integer |  |
| `end.pos_score_diff` | integer |  |
| `start.pos_team_receives_2H_kickoff` | logical |  |
| `end.pos_team_receives_2H_kickoff` | logical |  |
| `penalty_in_text` | logical |  |
| `penalty_count` | integer |  |
| `penalty_declined_count` | integer |  |
| `penalty_all_declined` | logical |  |
| `penalty_enforcement` | character |  |
| `penalty_negated_play` | character |  |
| `pass_breakup` | logical |  |
| `pass_depth` | character |  |
| `pass_direction` | character |  |
| `rush_direction` | character |  |
| `qb_hurry` | logical |  |
| `fg_attempt` | logical |  |
| `pos_unit` | character | Possession-team unit label (offense or special teams). |
| `def_pos_unit` | character | Defensive possession-team unit label (defense or special teams). |
| `sp` | logical |  |
| `play` | logical | Binary flag indicating the row is a counted play (excludes end markers/timeouts/penalties). |
| `cleaned_text` | character |  |
| `kneel_down` | logical |  |
| `scrimmage_play` | logical |  |
| `pos_score_diff_end` | integer |  |
| `fumble_lost` | logical |  |
| `fumble_recovered` | logical |  |
| `field_goal_result` | character |  |
| `extra_point_result` | character |  |
| `two_point_conv_result` | character |  |
| `defensive_two_point_attempt` | logical |  |
| `defensive_two_point_conv` | logical |  |
| `yds_punted_source` | character |  |
| `yds_kickoff_source` | character |  |
| `yds_punt_return_source` | character |  |
| `air_yardsToEndzone` | double |  |
| `air_yards` | double |  |
| `yards_after_catch` | double |  |
| `kickoff_return_player_name` | character |  |
| `punt_return_player_name` | character |  |
| `xp_attempt` | logical |  |
| `xp_made` | logical |  |
| `xp_kicker_player_name` | character |  |
| `kicking_team` | double |  |
| `return_team` | double |  |
| `fumble_or_muff` | logical |  |
| `recovery_team` | double |  |
| `recovery_team_2` | double |  |
| `penalty_spot_yardline` | double |  |
| `penalty_spot_side` | character |  |
| `penalty_spot_yardsToEndzone` | double |  |
| `fumbling_team` | double |  |
| `int_turnover` | logical |  |
| `pos_fumble_lost` | logical |  |
| `def_fumble_lost` | logical |  |
| `is_pos_team_turnover` | logical |  |
| `is_def_pos_team_turnover` | logical |  |
| `is_turnover` | logical | `TRUE` if the play was a turnover. |
| `turnover_team` | double |  |
| `is_st_turnover` | logical |  |
| `is_blocked_punt_turnover` | logical |  |
| `is_blocked_fg_turnover` | logical |  |
| `sack_team` | integer |  |
| `interception_team` | integer |  |
| `pass_breakup_team` | integer |  |
| `forced_fumble_team` | integer |  |
| `fumble_recovery_team` | double |  |
| `punt_return_team` | double |  |
| `kick_return_team` | double |  |
| `fg_team` | double |  |
| `punt_team` | double |  |
| `penalized_team` | double |  |
| `penalty_yards_signed` | integer |  |
| `penalty_side` | character |  |
| `penalty_yards_net` | double |  |
| `penalty_team_id` | double |  |
| `new_down` | integer |  |
| `new_distance` | integer |  |
| `under_2` | logical |  |
| `goal_to_go` | logical |  |
| `stopped_run` | logical |  |
| `opportunity_run` | logical |  |
| `highlight_run` | logical |  |
| `adj_rush_yardage` | double |  |
| `line_yards` | double |  |
| `second_level_yards` | double |  |
| `open_field_yards` | double |  |
| `highlight_yards` | double |  |
| `opp_highlight_yards` | double |  |
| `short_rush_success` | character |  |
| `short_rush_attempt` | character |  |
| `early_down` | logical |  |
| `late_down` | logical |  |
| `power_rush_attempt` | character |  |
| `power_rush_success` | character |  |
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
| `first_down_penalty` | logical |  |
| `first_down_earned` | logical |  |
| `start.pos_team_spread` | double |  |
| `start.elapsed_share` | double |  |
| `start.spread_time` | double |  |
| `end.pos_team_spread` | double |  |
| `end.elapsed_share` | double |  |
| `end.spread_time` | double |  |
| `penalty_assessed_on_kickoff` | logical |  |
| `start.yardsToEndzone.touchback` | integer |  |
| `EP_start_touchback` | double |  |
| `EP_start` | double |  |
| `EP_end` | double |  |
| `EP_penalty_cf` | double |  |
| `penalty_cf_yardsToEndzone` | double |  |
| `lag_EP_end` | double |  |
| `EP_between` | double |  |
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
| `start.ExpScoreDiff_touchback` | double |  |
| `start.ExpScoreDiff` | double |  |
| `start.ExpScoreDiff_Time_Ratio_touchback` | double |  |
| `start.ExpScoreDiff_Time_Ratio` | double |  |
| `end.ExpScoreDiff` | double |  |
| `end.ExpScoreDiff_Time_Ratio` | double |  |
| `wp_touchback` | double |  |
| `wp_before_naive` | double |  |
| `wp_touchback_naive` | double |  |
| `wp_after_naive` | double |  |
| `def_wp_before_naive` | double |  |
| `home_wp_before_naive` | double |  |
| `away_wp_before_naive` | double |  |
| `lead_wp_before_naive` | double |  |
| `lead_wp_before2_naive` | double |  |
| `def_wp_after_naive` | double |  |
| `home_wp_after_naive` | double |  |
| `away_wp_after_naive` | double |  |
| `wpa_naive` | double |  |
| `cp` | double |  |
| `cp_game_state` | double |  |
| `cp_model` | character |  |
| `cpoe` | double |  |
| `era` | integer |  |
| `xpass` | double |  |
| `pass_oe` | double |  |
| `drive_start` | double |  |
| `drive_stopped` | logical |  |
| `drive_play_index` | integer |  |
| `drive_offense_plays` | integer |  |
| `prog_drive_EPA` | double |  |
| `prog_drive_WPA` | double |  |
| `drive_offense_yards` | integer |  |
| `drive_total_yards` | integer |  |
| `qbr_epa` | double |  |
| `weight` | double | Listed weight (lbs). |
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
| `athlete_name` | character | Player full name. |
| `rusher_player_id` | double |  |
| `passer_player_id` | double |  |
| `receiver_player_id` | double |  |
| `fumble_player_id` | double | CFBD athlete_id of the player who fumbled. |
| `sack_player_id` | double | Comma-separated CFBD athlete_id(s) of the sacking defender(s). |
| `sack_player_id2` | double |  |
| `interception_player_id` | double | CFBD athlete_id of the defender credited with an interception. |
| `pass_breakup_player_id` | double | CFBD athlete_id of the defender credited with the pass breakup (PBU). |
| `fumble_forced_player_id` | double | CFBD athlete_id of the defender credited with forcing the fumble. |
| `fumble_recovered_player_id` | double | CFBD athlete_id of the player recovering the fumble. |
| `fg_kicker_player_id` | double |  |
| `punter_player_id` | double |  |
| `kickoff_player_id` | double |  |
| `kickoff_return_player_id` | double |  |
| `punt_return_player_id` | double |  |
| `fg_block_player_id` | double |  |
| `punt_block_player_id` | character |  |
| `fg_return_player_id` | character |  |
| `punt_block_return_player_id` | character |  |
| `go_wp` | double |  |
| `first_down_prob` | double |  |
| `wp_succeed` | double |  |
| `wp_fail` | double |  |
| `make_fg_wp` | double |  |
| `miss_fg_wp` | double |  |
| `fg_wp` | double |  |
| `punt_wp` | double |  |
| `go_boost` | double |  |
| `go_wp_diff` | double |  |
| `fg_wp_diff` | double |  |
| `punt_wp_diff` | double |  |
| `fourth_down_recommendation` | character |  |
| `two_pt_wp` | double |  |
| `xp_wp` | double |  |
| `prob_2pt` | double |  |
| `two_pt_recommendation` | character |  |
| `two_pt_wp_diff` | double |  |

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_fg_wp

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_fg_wp(fourth_down_rows)
print(out[["fg_make_prob", "fg_wp"]].head())
```

### get_go_wp {#get_go_wp}

`get_go_wp(pbp_df) -> 'pd.DataFrame'`

Expected win probability of going for it on 4th down (cfb4th `get_go_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the `start.*` state columns in PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` plus `go_wp` (prob-weighted WP of going for it), `first_down_prob` (P(conversion)), `wp_succeed` (mean WP over conversion outcomes) and `wp_fail` (mean WP over failure outcomes). `go_wp` is always in [0, 1]; the conditional columns are in [0, 1] but can be NaN for degenerate goal-line plays where one outcome bucket is empty (matches the R reference `pivot_wider` NA behavior).

No returns table is published for this function: no capture: on the smallest real input (one released season of play-by-play) it runs longer than the capture allows.

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_go_wp

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_go_wp(fourth_down_rows)
print(out[["go_wp", "first_down_prob"]].head())
```

### get_punt_wp {#get_punt_wp}

`get_punt_wp(pbp_df) -> 'pd.DataFrame'`

Expected win probability of punting on 4th down (cfb4th `get_punt_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `punt_wp` (prob-weighted WP of punting, from the punting team's perspective). `punt_wp` is NaN where the punt end-yardline distribution has no support for the play's `yards_to_goal` (e.g. inside the 31, where punting is dominated and the cfb4th table is empty -- matching the R reference's left-join NA behavior).

No returns table is published for this function: no capture: on the smallest real input (one released season of play-by-play) it runs longer than the capture allows.

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_punt_wp

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_punt_wp(fourth_down_rows)
print(out[["punt_wp"]].head())
```

### load_draft_outcomes {#load_draft_outcomes}

`load_draft_outcomes(years: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NFL draft picks with the college of each pick, for the requested draft years.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `years` | `int \| list[int]` |  | A draft year or list of draft years. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per pick: `draft_year` (Int64), `college` (Utf8 PFR-style college name), `player_id` (Utf8 ESPN college athlete id; null for older drafts), `player_name` (Utf8), `round` / `pick` (Int64), `position` (Utf8). Zero-row (typed) when the source is unavailable.

| col_name | type | description |
|---|---|---|
| `draft_year` | integer | NFL draft year of the pick. |
| `college` | character | College of the pick (PFR-style name, e.g. "Ohio St."). |
| `player_id` | character | ESPN college athlete id as a string (null for older drafts). |
| `player_name` | character | Player name as listed on the pick record. |
| `round` | integer | Round of the NFL draft the player was selected in (1-7 in the modern format). |
| `pick` | integer | Overall pick number. |
| `position` | character | Position drafted at (PFR abbreviation). |

**Example**

```python
from sportsdataverse.cfb import load_draft_outcomes
picks = load_draft_outcomes([2023, 2024])
picks.group_by("college").len().sort("len", descending=True).head()
```

### load_fp_curve {#load_fp_curve}

`load_fp_curve() -> 'pl.DataFrame'`

Load the bundled EP-by-yardline curve (no network, no first-use download).

**Returns**

`yardline_own: Int64 (1..99), ep: Float64`.

| col_name | type | description |
|---|---|---|
| `yardline_own` | integer | Starting yard line from the offense's own goal (1-99). |
| `ep` | double | Bundled expected points for a drive starting at this yard line (2018-2021 fit). |

**Example**

```python
from sportsdataverse.cfb.cfb_field_position import load_fp_curve
curve = load_fp_curve()
```

### load_recruit_classes {#load_recruit_classes}

`load_recruit_classes(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load recruiting classes as per-recruit rows from the 247 RDB feed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single recruiting-class year or a list of them. |
| `division` | `str` | `'fbs'` | Division slug (reserved for constant lookups downstream; the feed itself is queried for all of college football). |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per committed recruit: `season` (Int64), `team_id` (Utf8 — the 247 committed-team key), `team` (Utf8 full name — the downstream name-join key, since the 247 recruit-team key differs from the 247 talent-composite key), `recruit_id` (Utf8), `stars` (Int64), `grade` (Float64 247 composite rating), `position` (Utf8). Zero-row (typed) when no data is available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Recruiting-class year the recruit signed in. |
| `team_id` | character | 247Sports signed-institution team key as a string (falls back to the committed institution when unsigned). |
| `team` | character | Signed-institution full name (falls back to committed) - the downstream name-join key. |
| `recruit_id` | character | 247Sports recruit key as a string (integer-origin). |
| `stars` | integer | 247 composite star rating (1-5; null for unrated recruits). |
| `grade` | double | 247 composite rating on the 0-100 scale. |
| `position` | character | Primary position abbreviation from the 247 recruit record. |

**Example**

```python
from sportsdataverse.cfb.cfb_roster_talent import load_recruit_classes
rec = load_recruit_classes([2022, 2023])
rec.group_by("team").len().sort("len", descending=True).head()
```
