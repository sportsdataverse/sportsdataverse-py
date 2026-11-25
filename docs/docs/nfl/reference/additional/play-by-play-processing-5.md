---
title: "NFL — additional Python functions — Play-by-play processing: split_pat–team_name"
sidebar_label: "Play-by-play processing: split_pat–team_name"
sidebar_position: 15
description: "NFL — additional Python functions — Play-by-play processing: split_pat–team_name — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Play-by-play processing: split_pat–team_name

### split_pat_rows {#split_pat_rows}

`split_pat_rows(plays: 'pl.DataFrame') -> 'pl.DataFrame'`

Give every PAT that ESPN folded into its touchdown row a row of its own, as nflverse does.

From about 2014 ESPN reports the extra point or two-point try inside the touchdown play
(`xp_attempt` / `two_point_attempt` on the touchdown row); nflverse keeps it as its own
`XP_KICK` / `PAT2` row. This is opt-in: `NFLPlayProcess` output keeps ESPN's rows, and this
function returns a new frame with one extra row after each such touchdown. Older seasons, where ESPN
already emits the try as its own row, are returned as they are, and so is a try a penalty wiped out
(a `Penalty` / no-play row that carries the PAT flags; ESPN lists the re-try as its own row).

The split row copies the touchdown row's game / clock / possession context, takes the try's part of
the play text, and carries the try: `xp_attempt` / `two_point_attempt`, `extra_point_result`
(`good` / `failed` / `blocked`; ESPN leaves a blocked PAT null), `two_point_conv_result`
(`success` / `failure`), `play_type_nfl` (`XP_KICK` / `PAT2`) and `type.text`. The results come
from ESPN's columns where the feed has them, else from the text. The touchdown row hands the try over
(its PAT flags become `False` and its PAT results null). Every other column of a split row is null.

ESPN gives the try no id, so the split row's `id` is the touchdown's `id + 1` and `pat_of_id` names
the touchdown (null on every other row). Its `game_play_number` is the touchdown's.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | `NFLPlayProcess(...).plays_frame` (one game or many). |

**Returns**

The rows of `plays` in their original order, each folded try right after its touchdown, plus `pat_of_id` (Int64).

| col_name | type | description |
|---|---|---|
| `game_play_number` | Int64 | 1-based order of the row in the game, numbered after duplicate plays are dropped; a split try row repeats its touchdown's. |
| `id` | Int64 | ESPN play id; on a split try row, the touchdown's id + 1 (ESPN gives the try no id of its own). |
| `sequenceNumber` | Int64 | ESPN's sequenceNumber, the feed's ordering number for the play within the game. |
| `text` | String | ESPN's play description, with the HTML entities `&apos;`, `&quot;` and `&amp;` unescaped. |
| `awayScore` | Int64 | Away team's running score after the play -- ESPN's awayScore with feed errors repaired (a score rises only on or right after a scoring play). |
| `homeScore` | Int64 | Home team's running score after the play -- ESPN's homeScore with feed errors repaired (a score rises only on or right after a scoring play). |
| `scoringPlay` | Boolean | ESPN flag marking the play as a scoring play. |
| `priority` | Boolean | ESPN's priority flag for the play, passed through as sent. |
| `modified` | String | ESPN's modified timestamp for the play record (ISO 8601, UTC). |
| `wallclock` | String | ESPN's wallclock -- the real-world time of the play (ISO 8601, UTC). |
| `teamParticipants` | Int64 | Raw ESPN team-level participants payload carried through from the plays feed (stringified). |
| `isPenalty` | Boolean | ESPN's per-play flag that a penalty occurred on the play. |
| `statYardage` | Int64 | Yardage ESPN credits to the play for statistical purposes. |
| `isTurnover` | Boolean | ESPN's per-play turnover flag as shipped in the plays feed (broader than the giveaway-based is_turnover derivation). |
| `type.id` | String | ESPN's numeric identifier for the play type. |
| `type.text` | String | ESPN's text label for the play type. |
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
| `end.team.id` | Int64 | ESPN's `team.id` value for the play state at the end of the play. |
| `type.abbreviation` | String | ESPN's abbreviation for the play type. |
| `start.downDistanceText` | String | ESPN's `downDistanceText` value for the play state at the start of the play. |
| `start.shortDownDistanceText` | String | ESPN's `shortDownDistanceText` value for the play state at the start of the play. |
| `start.possessionText` | String | ESPN's `possessionText` value for the play state at the start of the play. |
| `end.downDistanceText` | String | ESPN's `downDistanceText` value for the play state at the end of the play. |
| `end.shortDownDistanceText` | String | ESPN's `shortDownDistanceText` value for the play state at the end of the play. |
| `end.possessionText` | String | ESPN's `possessionText` value for the play state at the end of the play. |
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
| `game_id` | Int64 | Ten digit identifier for NFL game. |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | Int64 | ESPN season type for the game (2 = regular season, 3 = postseason). |
| `week` | Int64 | Season week. |
| `status_type_completed` | Boolean | True when ESPN's game header marks the game completed (one value per game). |
| `homeTeamId` | Int64 | ESPN's home-team Id for the game, stamped on every play. |
| `awayTeamId` | Int64 | ESPN's away-team Id for the game, stamped on every play. |
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
| `roof` | String | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `homeTeamSpread` | Float64 | ESPN's home-team Spread for the game, stamped on every play. |
| `clock.minutes` | Int64 | Minutes remaining on the game clock at the play. |
| `clock.seconds` | Int64 | Seconds component of the game clock at the play. |
| `half` | Int64 | Half of the game -- 1 for periods 1-2, 2 for period 3 onward (overtime included). |
| `lag_half` | Int64 | Value of half on the previous play, used for sequence-aware derivations. |
| `lead_half` | Int64 | Value of half on the next play, used for sequence-aware derivations. |
| `start.TimeSecsRem` | Int64 | Seconds remaining in the half from ESPN's clock stamp for this play, which is the end-of-play time in 2005 and 2007+ (the snap time in 2004 and most of 2006); tops out at 1800. |
| `start.adj_TimeSecsRem` | Int64 | ESPN's `adj_TimeSecsRem` value for the play state at the start of the play. |
| `orig_play_type` | String | ESPN's play type (type.text) as the feed sent it, before the processor re-classifies plays (only a mistyped two-minute warning is relabelled first). |
| `lead_text` | String | Value of text on the next play, used for sequence-aware derivations. |
| `lead_start_team` | String | Value of start_team on the next play, used for sequence-aware derivations. |
| `lead_start_yardsToEndzone` | Int64 | Value of start_yardsToEndzone on the next play, used for sequence-aware derivations. |
| `lead_start_down` | Int64 | Value of start_down on the next play, used for sequence-aware derivations. |
| `lead_start_distance` | Int64 | Value of start_distance on the next play, used for sequence-aware derivations. |
| `lead_scoringPlay` | Boolean | Value of scoringPlay on the next play, used for sequence-aware derivations. |
| `text_dupe` | Boolean | Always False in the emitted frame -- the duplicate-row filter it gates runs before the column is returned, so it marks nothing and is retained only for schema stability. |
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
| `firstHalfKickoffTeamId` | Int64 | ESPN id of the team that kicked the opening kickoff (it receives the second-half kickoff), one value per game. |
| `period` | Int64 | Quarter of the game from ESPN's period.number (5 and up in overtime), with out-of-place periods repaired. |
| `start.yard` | Int64 | ESPN's `yard` value for the play state at the start of the play. |
| `end.yard` | Int64 | ESPN's `yard` value for the play state at the end of the play. |
| `lag_scoringPlay` | Boolean | Value of scoringPlay on the previous play, used for sequence-aware derivations. |
| `end_of_half` | Boolean | True on the play that ends a half -- the first half's last play, regulation's last play before overtime, and a completed game's last play (clock stoppages excluded). |
| `down_1` | Boolean | True when it is 1st down at the start of the play. |
| `down_2` | Boolean | True when it is 2nd down at the start of the play. |
| `down_3` | Boolean | True when it is 3rd down at the start of the play. |
| `down_4` | Boolean | True when it is 4th down at the start of the play. |
| `down_1_end` | Boolean | True when it is 1st down at the end of the play. |
| `down_2_end` | Boolean | True when it is 2nd down at the end of the play. |
| `down_3_end` | Boolean | True when it is 3rd down at the end of the play. |
| `down_4_end` | Boolean | True when it is 4th down at the end of the play. |
| `scoring_play` | Boolean | True when ESPN's play type is in the processor's scoring list (touchdowns other than kickoff returns, a made field goal, safeties, a defensive two-point return); a try is not one. |
| `td_play` | Boolean | True when the play text or type names a touchdown that stood ("touchdown", "for a TD"). |
| `touchdown` | Boolean | Binary indicator for if the play resulted in a TD. |
| `td_check` | Boolean | Internal flag used while reconciling whether the play produced a touchdown. |
| `safety` | Boolean | Binary indicator for whether or not a safety occurred. |
| `fumble_vec` | Boolean | True when the play text mentions a fumble. |
| `forced_fumble` | Boolean | True when the defense forced a fumble on the play. |
| `kickoff_play` | Boolean | True when ESPN's play type is a kickoff type (kickoff, kickoff return or touchdown, kicking-team recovery, kickoff safety, kickoff penalty). |
| `kickoff_tb` | Boolean | True on a kickoff whose text says touchback (or ends in "kickoff"). |
| `kickoff_onside` | Boolean | True on a kickoff whose text calls it onside. |
| `kickoff_oob` | Boolean | True on a kickoff whose text says it went out of bounds. |
| `kickoff_fair_catch` | Boolean | Binary indicator for if the kickoff was caught with a fair catch. |
| `kickoff_downed` | Boolean | Binary indicator for if the kickoff was downed. |
| `kick_play` | Boolean | True when the play text mentions a kick (kickoffs, and any text naming a kick). |
| `kickoff_safety` | Boolean | True on a safety whose text mentions a kickoff (a blocked punt or penalty excluded). |
| `punt` | Boolean | True when ESPN's play type is a punt type (punt, punt return or touchdown, blocked punt, punt safety, ...). |
| `punt_play` | Boolean | True when the play text mentions a punt (including the "(Punt formation)" annotation). |
| `punt_tb` | Boolean | True on a punt whose text says touchback. |
| `punt_oob` | Boolean | True on a punt whose text says it went out of bounds. |
| `punt_fair_catch` | Boolean | Binary indicator for if the punt was caught with a fair catch. |
| `punt_downed` | Boolean | Binary indicator for if the punt was downed. |
| `punt_safety` | Boolean | True on a safety whose text mentions a punt. |
| `punt_blocked` | Boolean | Binary indicator for if the punt was blocked. |
| `penalty_safety` | Boolean | True on a safety that ESPN types "Penalty". |
| `rush` | Boolean | Binary indicator if the play was a rushing play. |
| `pass` | Boolean | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `sack_vec` | Boolean | True when ESPN types the play a sack, or a fumble recovery on a pass whose text says "sacked". |
| `pos_team` | Int64 | ESPN team id of the team in possession at the start of the play (the receiving team on a kickoff). |
| `def_pos_team` | Int64 | ESPN team id of the team on defense at the start of the play (the kicking team on a kickoff). |
| `is_home` | Boolean | True when the possession team (pos_team) is the home team. |
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
| `pos_team_score` | Int64 | Possession team's score after the play. |
| `def_pos_team_score` | Int64 | Defensive team's score after the play. |
| `start.pos_team_score` | Int64 | ESPN's `pos_team_score` value for the play state at the start of the play. |
| `start.def_pos_team_score` | Int64 | ESPN's `def_pos_team_score` value for the play state at the start of the play. |
| `start.pos_score_diff` | Int64 | ESPN's `pos_score_diff` value for the play state at the start of the play. |
| `end.pos_team_score` | Int64 | ESPN's `pos_team_score` value for the play state at the end of the play. |
| `end.def_pos_team_score` | Int64 | ESPN's `def_pos_team_score` value for the play state at the end of the play. |
| `end.pos_score_diff` | Int64 | ESPN's `pos_score_diff` value for the play state at the end of the play. |
| `lag_pos_team` | Int64 | Previous row's possession team id; this row's own on the first row. |
| `lead_pos_team` | Int64 | Next row's possession team id; null on the last row. |
| `lead_pos_team2` | Int64 | Value of pos_team 2 plays ahead, used for sequence-aware derivations. |
| `pos_score_diff` | Int64 | Possession team's score minus the defense's, after the play. |
| `lag_pos_score_diff` | Int64 | Previous row's pos_score_diff (from that row's possession team's side); 0 on the first row. |
| `pos_score_pts` | Int64 | Points the play put on the board, from the possession team's side (negative when the defense scored). |
| `pos_score_diff_start` | Int64 | Possession team's score differential before the play -- the previous row's pos_score_diff, sign-flipped when that row's possession team was the other team. |
| `start.pos_team_receives_2H_kickoff` | Boolean | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the start of the play. |
| `end.pos_team_receives_2H_kickoff` | Boolean | ESPN's `pos_team_receives_2H_kickoff` value for the play state at the end of the play. |
| `change_of_poss` | Boolean | True when ESPN's end-of-play team differs from the start-of-play possession team. |
| `penalty_flag` | Boolean | True when ESPN types the play "Penalty" or the text reads "PENALTY on". |
| `penalty_declined` | Boolean | True when the play text mentions a penalty that was declined. |
| `penalty_no_play` | Boolean | True when the penalty wiped the play out ("No Play" / "nullified by penalty"). |
| `penalty_offset` | Boolean | True when the play text mentions offsetting penalties. |
| `penalty_1st_conv` | Boolean | True when an accepted foul by the defense gave the offense a first down -- an automatic first-down foul, or one whose yardage reached the line to gain (never on a kickoff, touchdown, try, possession change or offsetting fouls). |
| `penalty_in_text` | Boolean | True when the play description mentions a penalty. |
| `penalty_detail` | String | Name of the foul -- ESPN's penalty.type.text when the feed has it, else matched from the text; "Offsetting" / "Declined" / "Missing" when no foul name is found, null on plays with no penalty. |
| `penalty_text` | String | The play text after "Penalty" (" on NE-M.Onwenu, False Start, 5 yards, ..."); null when no penalty is flagged. |
| `yds_penalty` | String | Penalty yards as a signed whole number in text form -- from the penalty clause ("5 yards") or ESPN's penalty.yards, else ESPN's statYardage less the play's own gain; null when no penalty or an implausible number. |
| `penalty_count` | Int64 | Number of penalties flagged on the play (0-4 observed). |
| `penalty_declined_count` | Int64 | Number of the flagged penalties that were declined. |
| `penalty_all_declined` | Boolean | Whether every penalty flagged on the play was declined. |
| `penalty_enforcement` | String | How the penalty was resolved: one of no_play, declined, offsetting, negating_foul, play_stands, unknown. |
| `penalty_negated_play` | Boolean | Whether the penalty negated the play's result. |
| `sack` | Boolean | Binary indicator for if the play ended in a sack. |
| `int` | Boolean | True when ESPN types the play "Interception Return" or "Interception Return Touchdown". |
| `int_td` | Boolean | True when ESPN types the play "Interception Return Touchdown". |
| `completion` | Boolean | True on a completed pass; False when a penalty wiped the play out. |
| `pass_attempt` | Boolean | Binary indicator for if the play was a pass attempt (includes sacks). |
| `target` | Boolean | True on a pass attempt that targeted a receiver (any pass that is not a sack); False when a penalty wiped the play out. |
| `pass_breakup` | Boolean | True when a defender broke up the pass. |
| `pass_td` | Boolean | True on the offense's passing touchdown; False on a return touchdown and on a touchdown that did not stand. |
| `rush_td` | Boolean | True on the offense's rushing touchdown; False on a return touchdown and on a touchdown that did not stand. |
| `pass_depth` | String | Thrown-pass depth parsed from ESPN play text ("short" or "deep"); null when the text omits it (sacks, screens, pre-2025 text). |
| `pass_direction` | String | Pass direction parsed from ESPN play text ("left", "middle", or "right"); null when the text omits it. |
| `rush_direction` | String | Rush direction parsed from ESPN play text ("left", "middle", or "right"); null when the text omits it. |
| `turnover_vec` | Boolean | True when ESPN's play type is a turnover type (interception, opponent fumble recovery, blocked or missed field goal, blocked punt, punt-return touchdown, ...). |
| `offense_score_play` | Boolean | True when ESPN's play type is an offensive score (passing, rushing or kick-return touchdown, made field goal, ...). |
| `defense_score_play` | Boolean | True when ESPN's play type is a defensive score (return touchdown, safety, defensive two-point return, ...). |
| `downs_turnover` | Boolean | True on a 4th-down rush, pass or sack whose ESPN statYardage fell short of the distance and drew no penalty first down. |
| `yds_punted` | Int64 | Distance of the punt in yards ("punts 46 yards"), 0 on a blocked punt; null on other plays. |
| `yds_punt_gained` | Int64 | ESPN's statYardage on a punt play; null on other plays. |
| `fg_attempt` | Boolean | True when the play was a field-goal attempt. |
| `fg_made` | Boolean | True when ESPN types the play "Field Goal Good". |
| `yds_fg` | Int64 | Distance of the field-goal attempt in yards, parsed from the play text; null on other plays. |
| `pos_unit` | String | Possession team's unit on the play -- "Punt Offense", "Kickoff Return", "Field Goal Offense" or "Offense". |
| `def_pos_unit` | String | Defensive unit on the play -- "Punt Return", "Kickoff Defense", "Field Goal Defense" or "Defense". |
| `lead_play_type` | String | Play type (type.text, as the processor has re-typed it) of the next row; null on the last row. |
| `sp` | Boolean | Binary indicator for whether or not a score occurred on the play. |
| `play` | Boolean | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `kneel_down` | Boolean | Whether the play is an offensive kneel, from explicit kneel text plus an end-of-half TEAM-rush heuristic. |
| `scrimmage_play` | Boolean | True when the play is a play from scrimmage rather than a special-teams or administrative row. |
| `change_of_pos_team` | Boolean | True when the next row's possession team differs from this one's, and on the game's last row. |
| `pos_score_diff_end` | Int64 | Score differential from the possessing team's perspective at the end of the play. |
| `fumble_lost` | Boolean | Binary indicator for if the fumble was lost. |
| `fumble_recovered` | Boolean | True when a fumble on the play was recovered. |
| `field_goal_result` | String | String indicator for result of field goal attempt: made, missed, or blocked. |
| `extra_point_result` | String | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | String | String result of the two-point conversion attempt: success, failure, or safety (touchback in the defensive end zone). |
| `qb_hurry` | Boolean | Whether ESPN's play text says the quarterback was hurried into the throw ("hurried by ..."). |
| `xp_attempt` | Boolean | Whether an extra-point kick was attempted on the play. |
| `xp_made` | Boolean | Whether the extra-point kick was successful. |
| `two_point_attempt` | Boolean | Binary indicator for two point conversion attempt. |
| `defensive_two_point_attempt` | Boolean | Binary indicator whether or not the defense was able to have an attempt on a two point conversion, this results following a turnover. |
| `defensive_two_point_conv` | Boolean | Binary indicator whether or not the defense successfully scored on the two point conversion. |
| `two_point_pass` | Boolean | True on a two-point try whose text describes a pass; null on a split try row (its type.text / play_type_nfl carry the try). |
| `two_point_rush` | Boolean | True on a two-point try whose text describes no pass; null on a split try row (its type.text / play_type_nfl carry the try). |
| `yds_rushed` | Int64 | Rushing yards credited on a rush (ESPN's statYardage on a plain run, the gain to the spot of an offensive foul, 0 when a penalty wiped it out); null on non-rush plays. |
| `yds_receiving` | Int64 | Receiving yards credited on a pass play (0 on an incompletion, sack or interception, the gain to the spot of an offensive foul); null on non-pass plays. |
| `yds_int_return` | String | Yards the interception was returned (0 when the text gives none); null on other plays. |
| `yds_kickoff` | Int64 | Distance of the kickoff in yards ("kicks 65 yards"); null on other plays. |
| `yds_kickoff_return` | Int64 | Kickoff return yards (0 on a fair catch or no gain); on a touchback, the season's touchback yard line (the 30 in 2024); null on other plays. |
| `yds_punt_return` | Int64 | Punt return yards (0 on a fair catch, touchback, downed or out-of-bounds punt); null on other plays. |
| `yds_fumble_return` | String | Yards the recovering player returned the fumble (0 when the text gives none); null when there was no fumble. |
| `yds_sacked` | Int64 | Yards lost on the sack, as a negative number; null on other plays. |
| `sack_players` | String | The play text after "sacked by" on a sack or fumbled pass (the college phrasing); null when the text has none. |
| `xp_kicker_player_name` | String | Name of the kicker attempting the extra point. |
| `passer_player_name` | String | String name for the player that attempted the pass. |
| `rusher_player_name` | String | String name for the player that attempted the run. |
| `receiver_player_name` | String | String name for the targeted receiver. |
| `sack_player_name` | String | String name of the player who recorded a solo sack. |
| `sack_player_name2` | String | Second defender on a split sack ("sack split by A and B") -- ESPN's second sackedBy participant, else parsed from the play text. |
| `pass_breakup_player_name` | String | Name of the defender credited with defending the pass (not an interceptor) -- ESPN's per-play participant name, else parsed from the play text. |
| `interception_player_name` | String | String name for the player that intercepted the pass. |
| `fg_kicker_player_name` | String | Name of the field-goal kicker -- ESPN's per-play participant name, else parsed from the play text; null on other plays. |
| `fg_block_player_name` | String | Name of the player who blocked the field goal, parsed from the play text; null when none. |
| `fg_return_player_name` | String | Name of the player returning a blocked or missed field goal, parsed from the play text. |
| `kickoff_player_name` | String | Name of the kicker on a kickoff, parsed from the play text ("C.Little kicks 65 yards ..."). |
| `kickoff_return_player_name` | String | Name of the player returning the kickoff, when the play was returned. |
| `punter_player_name` | String | String name for the punter. |
| `punt_block_player_name` | String | Name of the player who blocked the punt, parsed from the play text; null when none. |
| `punt_return_player_name` | String | Name of the player returning the punt, when the punt was returned. |
| `punt_block_return_player_name` | String | Name of the player returning a blocked punt, parsed from the play text. |
| `fumble_player_name` | String | Name of the player who fumbled or muffed, parsed from the play text; null on non-fumble plays. |
| `fumble_forced_player_name` | String | Name of the player who forced the fumble -- ESPN's per-play participant name, else parsed from the play text. |
| `fumble_recovered_player_name` | String | Name of the player who recovered the fumble -- ESPN's per-play participant name, else parsed from the play text. |
| `kicking_team` | Int64 | Team id of the kicking team on kickoff, punt, and field-goal plays. |
| `return_team` | Int64 | Team id of the returning side; set on interception, fumble, kickoff, punt, and blocked-kick returns. |
| `fumble_or_muff` | Boolean | Whether the play includes a fumble or a muffed kick or punt (widened beyond ESPN's fumble play types). |
| `recovery_team` | String | Team id parsed from the play text as recovering the fumble or muff. |
| `recovery_team_2` | String | Team id of the second recovery in a multi-recovery scramble, parsed from the play text. |
| `penalty_spot_yardline` | Int64 | Yard line (0-50) at which the penalty was spotted. |
| `penalty_spot_side` | String | Side of the field the penalty was spotted on: 'home', 'away' or 'mid' (midfield). |
| `penalty_spot_yardsToEndzone` | Int64 | Yards from the penalty spot to the end zone (0-100). |
| `fumbling_team` | String | Team id of the side that fumbled or muffed the ball, parsed from the play text. |
| `int_turnover` | Boolean | Whether the play is an interception giveaway. |
| `pos_fumble_lost` | Boolean | Whether the possession team fumbled and lost the ball. |
| `def_fumble_lost` | Boolean | Whether the defending team (e.g. a returner after a takeaway) fumbled and lost the ball back. |
| `is_pos_team_turnover` | Boolean | Whether the possession team committed a giveaway (interception or fumble lost). |
| `is_def_pos_team_turnover` | Boolean | Whether the defending team gave the ball back via a lost fumble. |
| `is_turnover` | Boolean | True when the play is a giveaway-based turnover (interception thrown or fumble lost); blocked kicks recovered by the defense are carried by the blocked-kick fields instead. |
| `turnover_team` | String | Team id charged with the giveaway on the play. |
| `is_st_turnover` | Boolean | Whether the giveaway happened on a special-teams play (kick or punt snap, or a return). |
| `is_blocked_punt_turnover` | Boolean | Blocked-punt possession loss (blocked-punt TD, or the defense recovered); kept out of is_turnover to match ESPN's giveaway-only box. |
| `is_blocked_fg_turnover` | Boolean | Blocked-field-goal possession loss (blocked-FG TD, or the defense recovered); kept out of is_turnover to match ESPN's giveaway-only box. |
| `sack_team` | Int64 | Team id credited with the sack (the defense). |
| `interception_team` | Int64 | Team id credited with the interception (the defense). |
| `pass_breakup_team` | Int64 | Team id credited with the pass breakup (the defense). |
| `forced_fumble_team` | Int64 | Team id credited with forcing the fumble -- the side opposite the fumbling player (the covering team on returns). |
| `fumble_recovery_team` | String | Team id that recovered the fumble or muff, from parsed text with a giveaway / own-recovery fallback. |
| `punt_return_team` | Int64 | Team id of the punt-returning side. |
| `kick_return_team` | Int64 | Team id of the kick-returning side. |
| `fg_team` | Int64 | Team id attempting the field goal (the kicking team). |
| `punt_team` | Int64 | Team id punting the ball (the kicking team). |
| `penalized_team` | Int64 | Team id the penalty was assessed against, from the home/away text resolver with a foul-direction fallback. |
| `penalty_yards_signed` | Int64 | Penalty yardage parsed from the play text with era-aware bounds; the printed sign is retained but is not a reliable enforcement direction. |
| `penalty_side` | String | Which side committed the penalty -- 'off' (offense) or 'def' (defense). |
| `penalty_yards_net` | Int64 | Net yardage assessed for the penalty, signed relative to the possession team (observed -25 to 25). |
| `penalty_team_id` | Int64 | Team id of the side that committed the penalty. |
| `lateral_player_name` | String | After a completed pass, the player the ball was lateraled to ("Lateral to ..."); null otherwise. |
| `yds_lateral` | String | After a completed pass, the yards gained by the player the ball was lateraled to; null otherwise. |
| `yards_after_catch` | String | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `air_yards` | String | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `air_yardsToEndzone` | String | Yards to the endzone at the catch spot, parsed from the 2025+ vendor catch-spot text; null before 2025 or when unresolvable. |
| `new_down` | Int64 | Down after the play, including any penalty enforcement. |
| `new_distance` | Int64 | Distance to go after the play, including any penalty enforcement. |
| `middle_8` | Boolean | True in the middle eight minutes -- the last four of the first half and the first four of the second (34:00 to 26:00 of game time remaining). |
| `rz_play` | Boolean | True when the play starts 20 or fewer yards from the end zone. |
| `under_2` | Boolean | Whether the play began with two minutes or less remaining in the half. |
| `goal_to_go` | Boolean | Binary indicator for whether or not the posteam is in a goal down situation. |
| `scoring_opp` | Boolean | True when the play starts 40 or fewer yards from the end zone. |
| `stuffed_run` | Boolean | True on a play ESPN types "Rush" that gained zero or fewer yards. |
| `stopped_run` | Boolean | True when the rush was stopped at or behind the line of scrimmage. |
| `opportunity_run` | Boolean | True on a rush that gained 4 or more yards. |
| `highlight_run` | Boolean | True when the rush gained 8 or more yards. |
| `adj_rush_yardage` | Int64 | Rushing yards capped at 10, the input to the line-yards decomposition. Assets published before the 2026-10 line-yards fix cap at 8. |
| `line_yards` | Float64 | Yards credited to the offensive line on a rush, on the standard adjusted-line-yards band: 1.2x the yardage on a loss, all of yards 0-4, half of each yard from 5 to 10, and nothing beyond 10 (a 7-yard ceiling). Line, second-level and open-field yards sum to the carry on every gain. Assets published before the 2026-10 line-yards fix carry the old band (all of yards 0-3, half of 4-8, a 5.5-yard ceiling). |
| `second_level_yards` | Float64 | The ball carrier's half of yards 5-10 of a rush (the line takes the other half); 0 on carries of 4 yards or fewer. Assets published before the 2026-10 line-yards fix carry the old band here: half of yards 4-8 in college football, half of yards 5-8 in the NFL (where the old parts came up half a yard short of the carry). |
| `open_field_yards` | Int64 | Rushing yards gained beyond 10, credited to the ball carrier rather than the line. Assets published before the 2026-10 line-yards fix count the yards beyond 8. |
| `highlight_yards` | Float64 | Second-level plus open-field yards -- the yardage credited to the carrier. |
| `opp_highlight_yards` | Float64 | Highlight yards earned on opportunity runs, isolating carrier production on carries where the blocking succeeded. Assets published before the 2026-08 fix are identically 0 here, because the inverted opportunity_run gate could never co-occur with non-zero highlight yards. |
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
| `first_down_yards` | Boolean | Whether the play gained enough yardage to earn a first down. |
| `first_down_penalty` | Boolean | Binary indicator for if a penalty converted the first down. |
| `first_down_earned` | Boolean | Whether the play earned a first down by means other than yardage (e.g. by penalty). |
| `new_series` | Boolean | True on the first play of a series of downs -- a half's first row, a new drive, or the play after a first down earned by yards or penalty (cfbfastR's rule). |
| `firstD_by_kickoff` | Boolean | Series-start flag (cfbfastR's rule) set on every kickoff; not an earned first down. |
| `firstD_by_poss` | Boolean | Series-start flag (cfbfastR's rule), True on a possession's first play -- after a kickoff, punt, turnover, turnover on downs or scoring play, or a drive's first play that is not a kickoff; not an earned first down. |
| `firstD_by_penalty` | Boolean | Series-start flag (cfbfastR's rule), True on the play after a first down earned by penalty with no change of possession (a timeout or end-of-period row in between is skipped). |
| `firstD_by_yards` | Boolean | Series-start flag (cfbfastR's rule), True on the play after a first down earned by yards with no change of possession (a timeout or end-of-period row in between is skipped). |
| `start.pos_team_spread` | Float64 | ESPN's `pos_team_spread` value for the play state at the start of the play. |
| `start.elapsed_share` | Float64 | ESPN's `elapsed_share` value for the play state at the start of the play. |
| `start.spread_time` | Float64 | ESPN's `spread_time` value for the play state at the start of the play. |
| `end.pos_team_spread` | Float64 | ESPN's `pos_team_spread` value for the play state at the end of the play. |
| `end.elapsed_share` | Float64 | ESPN's `elapsed_share` value for the play state at the end of the play. |
| `end.spread_time` | Float64 | ESPN's `spread_time` value for the play state at the end of the play. |
| `pass_length` | String | String indicator for pass length: short or deep. |
| `pass_location` | String | String indicator for pass location: left, middle, or right. |
| `shotgun` | Int64 | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | Int64 | Binary indicator for whether or not the play was in no_huddle formation. |
| `aborted_play` | Int64 | Binary indicator (1/0) that the play text contains "Aborted" (a botched snap or exchange), nflfastR's clean_pbp rule. |
| `play_type_nfl` | String | nflverse NFL play type (PASS, RUSH, SACK, INTERCEPTION, KICK_OFF, PENALTY, ...) mapped from ESPN's type.text and the play flags; XP_KICK / PAT2 on a split try row, UNSPECIFIED for an ESPN type with no mapping. |
| `pass_middle` | Int64 | 1 when the pass location parsed from the text is "middle", else 0. |
| `down` | Int64 | The down for the given play. |
| `distance` | Int64 | Yards to go for a first down at the start of the play (10 on a kickoff). |
| `start.yardsToEndzone.touchback` | Int64 | ESPN's `yardsToEndzone.touchback` value for the play state at the start of the play. |
| `penalty_assessed_on_kickoff` | Boolean | Whether a penalty was assessed on a kickoff; such plays take the kickoff/touchback win-probability handling. |
| `EP_start_touchback` | Float64 | Expected points the offense would have had from a touchback on this play. |
| `EP_start` | Float64 | Expected points for the offense at the start of the play. |
| `EP_end` | Float64 | Expected points for the offense at the end of the play. |
| `EP_penalty_cf` | String | Counterfactual expected points for the penalty branch -- the EP had the alternative penalty outcome been taken (null unless a penalty decision existed). |
| `penalty_cf_yardsToEndzone` | String | Yards to the end zone in the counterfactual penalty branch. |
| `lag_EP_end` | Float64 | Value of EP_end on the previous play, used for sequence-aware derivations. |
| `lag_change_of_pos_team` | Boolean | Previous row's change_of_pos_team within the game; False on the first row. |
| `EP_between` | Float64 | Change in expected points across the play, before penalty adjustment. |
| `EPA` | Float64 | Expected points added, EP_end - EP_start from the bundled ESPN EP model with nflfastR's rules -- 0 on a clock stoppage; -EP_start on an unscored play that ends a half (this rule wins); otherwise plus EP_between (the gap from the previous play's end EP, sign-adjusted for a possession change) on a play whose text carries a penalty and that is neither a kickoff nor typed "Penalty". |
| `def_EPA` | Float64 | EPA for the defensive team on the play (-1 * EPA). |
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
| `qb_epa` | Float64 | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `start.ExpScoreDiff_touchback` | Float64 | ESPN's `ExpScoreDiff_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff` | Float64 | ESPN's `ExpScoreDiff` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio_touchback` | Float64 | ESPN's `ExpScoreDiff_Time_Ratio_touchback` value for the play state at the start of the play. |
| `start.ExpScoreDiff_Time_Ratio` | Float64 | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the start of the play. |
| `end.ExpScoreDiff` | Float64 | ESPN's `ExpScoreDiff` value for the play state at the end of the play. |
| `end.ExpScoreDiff_Time_Ratio` | Float64 | ESPN's `ExpScoreDiff_Time_Ratio` value for the play state at the end of the play. |
| `wp_before` | Float64 | Win probability before the play for the possession team (0-1), from the bundled spread-adjusted wp_spread model; a kickoff takes the touchback state's value. |
| `wp_touchback` | Float64 | Win probability the offense would have had starting from a touchback. |
| `wp_after` | Float64 | Win probability after the play for the team that had the ball at the snap (0-1), from the bundled wp_spread model; 1 or 0 on a completed game's last play. |
| `def_wp_before` | Float64 | Defensive team's win probability before the play (1 - wp_before). |
| `home_wp_before` | Float64 | Home team's win probability before the play (0-1). |
| `away_wp_before` | Float64 | Away team's win probability before the play (0-1). |
| `lead_wp_before` | Float64 | Value of wp_before on the next play, used for sequence-aware derivations. |
| `lead_wp_before2` | Float64 | Value of wp_before 2 plays ahead, used for sequence-aware derivations. |
| `def_wp_after` | Float64 | Defensive team's win probability after the play (1 - wp_after). |
| `home_wp_after` | Float64 | Home team's win probability after the play (0-1). |
| `away_wp_after` | Float64 | Away team's win probability after the play (0-1). |
| `wpa` | Float64 | Win probability added (WPA) for the posteam. |
| `wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `vegas_wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `def_wp` | Float64 | Estimated win probability for the defteam. |
| `home_wp` | Float64 | Estimated win probability for the home team. |
| `away_wp` | Float64 | Estimated win probability for the away team. |
| `wp_before_naive` | Float64 | Pre-snap possession-team win probability from the spread-free (naive) WP model. |
| `wp_after_naive` | Float64 | End-of-play possession-team win probability from the naive model, after the game-logic adjustment chain. |
| `wpa_naive` | Float64 | Win probability added on the play under the spread-free (naive) model. |
| `def_wp_before_naive` | Float64 | Pre-snap defense win probability under the naive model (1 - wp_before_naive). |
| `def_wp_after_naive` | Float64 | End-of-play defense win probability under the naive model. |
| `home_wp_before_naive` | Float64 | Pre-snap naive win probability mapped to the home team. |
| `home_wp_after_naive` | Float64 | End-of-play naive win probability mapped to the home team. |
| `lead_wp_before_naive` | Float64 | Next play's pre-snap naive win probability, used in the end-of-half and change-of-possession adjustments. |
| `lead_wp_before2_naive` | Float64 | Pre-snap naive win probability two plays ahead, used where the immediately following row is a non-play. |
| `wp_touchback_naive` | Float64 | Naive-model win probability for the kickoff-touchback substitute state, used as the pre-snap WP on kickoffs. |
| `away_wp_before_naive` | Float64 | Pre-snap naive win probability mapped to the away team. |
| `away_wp_after_naive` | Float64 | End-of-play naive win probability mapped to the away team. |
| `cp` | String | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | String | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `xpass` | Float64 | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | Float64 | Dropback percent over expected on a given play scaled from 0 to 100. |
| `xyac_epa` | String | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | String | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | String | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | String | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | String | Probability play earns a first down based on where the ball was caught. |
| `drive_start` | Float64 | Yard line at which the drive began. |
| `drive_stopped` | Boolean | True when the play ended the drive. |
| `drive_play_index` | Int64 | Sequence number of the play within its drive. |
| `drive_offense_plays` | Int64 | Offensive plays run on the drive. |
| `prog_drive_EPA` | Float64 | Cumulative EPA accrued by the drive up to and including this play. |
| `prog_drive_WPA` | Float64 | Cumulative win-probability added by the drive up to and including this play. |
| `drive_offense_yards` | Int64 | Offensive yards gained on the drive. |
| `drive_total_yards` | Int64 | Total yards gained on the drive. |
| `fixed_drive` | Int64 | Manually created drive number in a game. |
| `fixed_drive_result` | String | Manually created drive result. |
| `series` | Int64 | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_result` | String | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `series_success` | Int64 | 1: scored touchdown, gained enough yards for first down. |
| `go_wp` | Float64 | Win probability from going for it on fourth down: conversion-probability-weighted mean of the success and failure states (cfb4th port). |
| `first_down_prob` | Float64 | Modeled probability of converting the fourth down when going for it. |
| `wp_succeed` | Float64 | Mean win probability across yardage outcomes given the fourth-down attempt converts. |
| `wp_fail` | Float64 | Mean win probability given the fourth-down attempt fails. |
| `fg_make_prob` | Float64 | Fourth downs only. Probability a field-goal attempt from this spot is good, from the bundled nfl4th fg_model; null on every other row. |
| `make_fg_wp` | Float64 | Win probability given the field-goal attempt is made. |
| `miss_fg_wp` | Float64 | Win probability given the field-goal attempt misses. |
| `fg_wp` | Float64 | Make-probability-weighted win probability of attempting the field goal. |
| `punt_wp` | Float64 | Win probability of punting, from the bundled punt-outcome distribution. |
| `go_boost` | Float64 | cfb4th's headline number: 100 * (go_wp - max(fg_wp, punt_wp)), in percentage points. |
| `go_wp_diff` | Float64 | go_wp minus the recommended option's WP (0 when going for it is the recommendation, otherwise <= 0). |
| `punt_wp_diff` | Float64 | punt_wp minus the recommended option's WP (0 when punting is the recommendation, otherwise <= 0). |
| `fg_wp_diff` | Float64 | fg_wp minus the recommended option's WP (0 when the field goal is the recommendation, otherwise <= 0). |
| `fourth_down_recommendation` | String | Max-WP fourth-down choice among "go", "punt", and "field_goal". |
| `two_pt_wp` | Float64 | Win probability of going for two: conversion-probability-weighted mean of the 2-point and 0-point outcomes (cfb4th port). |
| `xp_wp` | Float64 | Touchdowns with a try only. Win probability of kicking the extra point for the scoring team (the make probability at the PAT spot weighs the 1- and 0-point outcomes, nfl4th's method); null on every other row. |
| `prob_2pt` | Float64 | Touchdowns with a try only. Two-point conversion probability for the scoring team, from the bundled nfl4th two-point model; null on every other row. |
| `two_pt_wp_diff` | Float64 | two_pt_wp minus xp_wp; positive favors going for two. |
| `two_pt_recommendation` | String | Point-after recommendation: "go_for_2" when two_pt_wp exceeds xp_wp, otherwise "kick_xp". |
| `qbr_epa` | Float64 | EPA variant used as an input to the QBR calculation. |
| `weight` | Float64 | Official weight, in pounds |
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
| `athlete_name` | String | The play's passer, else its rusher (passer_player_name, then rusher_player_name); null when neither. |
| `sack_player_id2` | String | ESPN athlete id of the second sacker on a split sack (regex fallback for an ESPN sidecar blind spot). |
| `passer_player_id` | Int64 | Unique identifier for the player that attempted the pass. |
| `rusher_player_id` | Int64 | Unique identifier for the player that attempted the run. |
| `receiver_player_id` | Int64 | Unique identifier for the receiver that was targeted on the pass. |
| `punter_player_id` | Int64 | Unique identifier for the punter. |
| `fg_kicker_player_id` | Int64 | ESPN athlete id of the field-goal kicker. |
| `sack_player_id` | Int64 | Unique identifier of the player who recorded a solo sack. |
| `punt_return_player_id` | Int64 | ESPN athlete id of the punt returner. |
| `kickoff_return_player_id` | Int64 | ESPN athlete id of the kickoff returner. |
| `interception_player_id` | String | Unique identifier for the player that intercepted the pass. |
| `pass_breakup_player_id` | Int64 | ESPN athlete id of the defender credited with defending the pass (the play's participants, else matched by name in the game's box score). |
| `fumble_forced_player_id` | String | ESPN athlete id of the player who forced the fumble (the play's participants, else matched by name in the game's box score). |
| `fumble_recovered_player_id` | String | ESPN athlete id of the player who recovered the fumble (the play's participants, else matched by name in the game's box score). |
| `fumble_player_id` | String | ESPN athlete id of the player who fumbled, matched by name and team in the game's box score. |
| `punt_block_player_id` | String | ESPN athlete id of the player who blocked the punt. |
| `punt_block_return_player_id` | String | ESPN athlete id of the player who returned the blocked punt. |
| `kickoff_player_id` | Int64 | ESPN athlete id of the player kicking off. |
| `fg_block_player_id` | String | ESPN athlete id of the player who blocked the field goal. |
| `fg_return_player_id` | String | ESPN athlete id of the player who returned the blocked or missed field goal. |
| `xp_kicker_player_id` | Int64 | ESPN athlete id of the extra-point kicker, matched by name and team in the game's box score. |
| `pat_of_id` | Int64 | On a split PAT / two-point try row, the id of the touchdown it was folded into (the row's own id is that id + 1); null on every other row. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a synthetic id (touchdown `id + 1`) is already a play's id. |

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess, split_pat_rows
proc = NFLPlayProcess(gameId=401671740)
proc.espn_nfl_pbp()
proc.run_processing_pipeline()
plays = split_pat_rows(proc.plays_frame)
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
