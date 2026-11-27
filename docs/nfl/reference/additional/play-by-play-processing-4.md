# NFL — additional Python functions — Play-by-play processing: espn_nfl

> NFL — additional Python functions — Play-by-play processing: espn_nfl — function reference in sdv-py, the SportsDataverse Python package.

### espn_nfl_pbp_nflverse {#espn_nfl_pbp_nflverse}

`espn_nfl_pbp_nflverse(plays: 'pl.DataFrame', schedule: 'Optional[pl.DataFrame]' = None, *, split_pats: 'bool' = True, participants: 'Optional[pl.DataFrame]' = None, players: 'Optional[pl.DataFrame]' = None, enrich: 'bool' = False) -> 'pl.DataFrame'`

ESPN NFL play-by-play in nflverse `play_by_play` names, conventions and dtypes.

Turns `~sportsdataverse.nfl.NFLPlayProcess` output (`plays_frame`, one game or many, or the
published `espn_nfl_pbp` release) into nflverse's play-by-play: the keys, teams, field position,
clock, down and distance, scores, timeouts, play types, the 0/1 play flags, yardage, nflfastR's
description columns (`run_gap`, `shotgun`, `out_of_bounds`, `penalty_type`, ...), drive and series
detail, with `schedule` nflverse's game columns (nflfastR's `add_game_data`), with `players` the
GSIS player ids and names, and with `enrich` nflfastR's model columns
(`~sportsdataverse.nfl.ep_wp.enrich_nfl_pbp`: EP / EPA / WP / WPA, try values, the air / YAC
splits). It is additive: the cfbfastR-shape ESPN frame is not changed. Nothing is fetched unless
`enrich` needs the xYAC model, which is not bundled (it is downloaded and cached on first use; without
ESPN air yards its step is skipped).

Conventions follow nflverse, not ESPN: `game_id` is nflverse's `YYYY_WW_AWAY_HOME` and `play_id` the
GSIS play id ESPN embeds in its play `id` (2006 on), so the frame joins nflverse `play_by_play` on
`(game_id, play_id)`. Teams come from the team ids and the game id, never from ESPN's abbreviations, in
the franchise's current abbreviation as nflverse writes them (`LA` for the 2007 Rams). A kickoff
belongs to the receiving team; a timeout row has no possession, down or field position; an offensive
touchdown counts as a first down; a declined penalty is no penalty. With `split_pats` the try ESPN
folds into a touchdown row (2014 on) gets its own `extra_point` / two-point row
(`~sportsdataverse.nfl.split_pat_rows`) and the touchdown row's scores leave the try's points out.

Agreement with nflverse on whole seasons (2007, 2014, 2019, 2024) is 99%+ for most columns. The known
gaps are ESPN's: `desc` is ESPN's text, not the NFL feed's; ESPN stamps some plays (scoring plays,
whole 2024 games) with the clock after the snap; before 2009 many plays carry a stub text ("Rush",
"Pass Completion"), so the text-derived columns (pass / run direction, completion, yards) are thin
there; the timeouts are the processor's own count; a split PAT row's `play_id` is the touchdown's
plus one (ESPN gives the try no id), so it does not join nflverse's PAT row.

Players: ESPN participant roles go to nflverse's slots by rules measured on whole seasons (two sackers are
`half_sack_1/2`, a no-play keeps only the penalized player, a split try takes its touchdown's PAT players),
and ESPN athlete ids go to GSIS through the players table's `espn_id`, never by name. `fixed_drive` is
ESPN's own drive number, `fixed_drive_result` / `series` nflfastR's cascades on it. ESPN's air yards are
empty, so `cp` / `cpoe` / xYAC / the air-yards splits stay null; `qb_hit` and jersey numbers are not built.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | `NFLPlayProcess(...).plays_frame` or the `espn_nfl_pbp` release (one or many games). |
| `schedule` | `Optional[DataFrame]` | `None` | The nflverse schedule (`~sportsdataverse.nfl.load_nfl_schedule`). Adds the game columns, and supplies `nflverse_game_id` (from its `espn` id) when `plays` has none (`NFLPlayProcess` stamps it from ESPN's header since this release). |
| `split_pats` | `bool` | `True` | Give each folded PAT / two-point try its own row, as nflverse does (default `True`). |
| `participants` | `Optional[DataFrame]` | `None` | `~sportsdataverse.nfl.espn_nfl_play_participants` output (one or many games, or the `espn_nfl_play_participants` release), keyed on the ESPN play id. Its roles win; without it (or for a role it leaves empty) the processor's own athlete ids are read. Used only with `players`. |
| `players` | `Optional[DataFrame]` | `None` | The nflverse players table (`~sportsdataverse.nfl.load_nfl_players`; `gsis_id`, `espn_id` and `short_name`). Adds the `<slot>_player_id` / `_player_name` columns and the tackler / fumble teams. |
| `enrich` | `bool` | `False` | Run `~sportsdataverse.nfl.ep_wp.enrich_nfl_pbp` (without the fourth-down surface, which nflverse's play-by-play does not carry) on the view (default `False`). |

**Returns**

One row per ESPN play (plus one per split try) in nflverse's column names, flags as `Int64` 0/1, measures as `Float64`, ids and names as strings, plus `espn_game_id` / `espn_play_id` to join back to ESPN. The schedule columns are present only with `schedule`, the player columns only with `players`, the model columns only with `enrich`.

| col_name | type | description |
|---|---|---|
| `play_id` | Int64 | GSIS play id, the digits ESPN's play id carries after its game id (2006 on); a split PAT row is its touchdown's play_id plus 1. Joins nflverse play_by_play with game_id. |
| `game_id` | String | nflverse game id, YYYY_WW_AWAY_HOME (the old abbreviation, e.g. 2007_14_SD_TEN). |
| `espn_game_id` | Int64 | ESPN event id of the game. |
| `espn_play_id` | Int64 | ESPN play id (participants and the ESPN frame are keyed on it); a split PAT row's is the touchdown's plus 1. |
| `home_team` | String | Home team, nflverse's current franchise abbreviation (LA, LAC, LV for the Rams, Chargers, Raiders of every era). |
| `away_team` | String | Away team, nflverse's current franchise abbreviation. |
| `season_type` | String | REG, POST or PRE. |
| `week` | Int64 | nflverse week (playoff weeks continue the regular-season count), from game_id. |
| `season` | Int64 | Season, from game_id. |
| `posteam` | String | Team with possession (a kickoff belongs to the receiving team); null on a timeout row. |
| `posteam_type` | String | home or away: which side posteam is. |
| `defteam` | String | Team on defense; null on a timeout row. |
| `side_of_field` | String | Abbreviation of the team whose half the ball is in, or 50 at midfield. |
| `yardline_100` | Float64 | Yards from the opponent's end zone at the snap (a kickoff from the receiving team's side). |
| `yrdln` | String | Field position as nflverse writes it (e.g. KC 35, MID 50). |
| `qtr` | Float64 | Quarter (5 = overtime). |
| `quarter_seconds_remaining` | Float64 | Seconds left in the quarter at the snap: the clock the NFL text opens with where it has one, else ESPN's clock. |
| `half_seconds_remaining` | Float64 | Seconds left in the half at the snap (overtime counts only its own period). |
| `game_seconds_remaining` | Float64 | Seconds left in regulation at the snap (overtime counts only its own period). |
| `game_half` | String | Half1, Half2 or Overtime. |
| `time` | String | Game clock at the snap, MM:SS. |
| `down` | Float64 | Down; null on kickoffs, tries and timeouts. |
| `ydstogo` | Float64 | Yards to go for a first down (0 on kickoffs and timeouts). |
| `posteam_score` | Float64 | posteam's score before the play. |
| `defteam_score` | Float64 | defteam's score before the play. |
| `score_differential` | Float64 | posteam_score minus defteam_score. |
| `posteam_score_post` | Float64 | posteam's score after the play (a touchdown row leaves out its split try's points). |
| `defteam_score_post` | Float64 | defteam's score after the play. |
| `score_differential_post` | Float64 | posteam_score_post minus defteam_score_post. |
| `total_home_score` | Float64 | Home score after the play (a touchdown row leaves out its split try's points). |
| `total_away_score` | Float64 | Away score after the play (a touchdown row leaves out its split try's points). |
| `home_timeouts_remaining` | Float64 | Home timeouts left before the play (after it on a timeout row), from the ESPN processor's count. |
| `away_timeouts_remaining` | Float64 | Away timeouts left before the play (after it on a timeout row), from the ESPN processor's count. |
| `posteam_timeouts_remaining` | Float64 | posteam's timeouts left before the play; null on a timeout row. |
| `defteam_timeouts_remaining` | Float64 | defteam's timeouts left before the play; null on a timeout row. |
| `complete_pass` | Int64 | 0/1 flag: completed pass. |
| `interception` | Int64 | 0/1 flag: intercepted pass. |
| `sack` | Int64 | 0/1 flag: sack. |
| `rush_attempt` | Int64 | 0/1 flag: rushing attempt (scrambles and kneels included). |
| `touchdown` | Int64 | 0/1 flag: touchdown scored on the play. |
| `pass_touchdown` | Int64 | 0/1 flag: passing touchdown. |
| `rush_touchdown` | Int64 | 0/1 flag: rushing touchdown. |
| `safety` | Int64 | 0/1 flag: safety. |
| `fumble` | Int64 | 0/1 flag: fumble (or muff) on the play. |
| `fumble_lost` | Int64 | 0/1 flag: fumble recovered by the other team. |
| `first_down_penalty` | Int64 | 0/1 flag: first down awarded by penalty. |
| `kickoff_attempt` | Int64 | 0/1 flag: kickoff. |
| `kickoff_out_of_bounds` | Int64 | 0/1 flag: kickoff out of bounds. |
| `kickoff_downed` | Int64 | 0/1 flag: kickoff downed. |
| `kickoff_fair_catch` | Int64 | 0/1 flag: kickoff fair catch. |
| `punt_attempt` | Int64 | 0/1 flag: punt. |
| `punt_blocked` | Int64 | 0/1 flag: blocked punt. |
| `punt_out_of_bounds` | Int64 | 0/1 flag: punt out of bounds. |
| `punt_downed` | Int64 | 0/1 flag: punt downed. |
| `punt_fair_catch` | Int64 | 0/1 flag: punt fair catch. |
| `field_goal_attempt` | Int64 | 0/1 flag: field goal attempt. |
| `extra_point_attempt` | Int64 | 0/1 flag: extra point attempt. |
| `two_point_attempt` | Int64 | 0/1 flag: two-point conversion attempt. |
| `qb_kneel` | Int64 | 0/1 flag: quarterback kneel. |
| `special` | Int64 | 0/1 flag: special-teams play (kickoff, punt, field goal, extra point). |
| `tackled_for_loss` | Int64 | 0/1 flag: run tackled for a loss. |
| `goal_to_go` | Int64 | 0/1 flag: goal-to-go situation. |
| `pass_attempt` | Int64 | 0/1 flag: pass attempt, sacks included (nflverse's convention). |
| `incomplete_pass` | Int64 | 0/1 flag: incomplete pass (not intercepted, not a sack). |
| `fumble_forced` | Int64 | 0/1 flag: forced fumble (the text names the forcer, FUMBLES (J.Doe)). |
| `special_teams_play` | Int64 | 0/1 flag: special-teams play as nflverse flags it (a made field goal is left out). |
| `fumble_not_forced` | Int64 | 0/1 flag: fumble with no forcer named. |
| `penalty` | Int64 | 0/1 flag: accepted penalty (a declined one is 0, as in nflverse). |
| `return_touchdown` | Int64 | 0/1 flag: touchdown that is neither a passing nor a rushing one (returns, recoveries). |
| `touchback` | Int64 | 0/1 flag: kickoff or punt touchback. |
| `qb_spike` | Int64 | 0/1 flag: spike (the text says spiked). |
| `qb_scramble` | Int64 | 0/1 flag: quarterback scramble (the text says scrambles). |
| `qb_dropback` | Int64 | 0/1 flag: dropback: pass attempt, sack, scramble or spike. |
| `first_down` | Int64 | 0/1 flag: first down by yards, penalty or an offensive touchdown. |
| `first_down_rush` | Int64 | 0/1 flag: rushing first down (a rushing touchdown counts). |
| `first_down_pass` | Int64 | 0/1 flag: passing first down (a passing touchdown counts). |
| `third_down_converted` | Int64 | 0/1 flag: third down converted (first down or offensive touchdown). |
| `third_down_failed` | Int64 | 0/1 flag: third-down pass or run that did not convert. |
| `fourth_down_converted` | Int64 | 0/1 flag: fourth down converted (first down or offensive touchdown). |
| `fourth_down_failed` | Int64 | 0/1 flag: fourth-down pass or run that did not convert. |
| `aborted_play` | Int64 | 0/1 flag: aborted snap (the text contains Aborted, nflfastR's rule). |
| `drive_ended_with_score` | Int64 | 0/1 flag: the play's drive ended in a score. |
| `desc` | String | ESPN's play text (close to, but not, the NFL feed's description nflverse carries). |
| `field_goal_result` | String | made, missed or blocked. |
| `extra_point_result` | String | good, failed or blocked. |
| `two_point_conv_result` | String | success or failure. |
| `run_location` | String | left, middle or right, from the text. |
| `pass_location` | String | left, middle or right, from the text. |
| `pass_length` | String | short or deep, from the text. |
| `drive_play_count` | Float64 | Offensive plays in the play's ESPN drive. |
| `drive_time_of_possession` | String | Time of possession of the play's ESPN drive, M:SS. |
| `drive_quarter_start` | Float64 | Quarter the play's ESPN drive started in. |
| `drive_quarter_end` | Float64 | Quarter the play's ESPN drive ended in. |
| `ydsnet` | Float64 | Net yards of the play's ESPN drive. |
| `play_type_nfl` | String | nflverse's NFL play type (PASS, RUSH, SACK, INTERCEPTION, KICK_OFF, PUNT, FIELD_GOAL, XP_KICK, PAT2, PENALTY, TIMEOUT, UNSPECIFIED). |
| `play_type` | String | nflverse's play type: pass, run, punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play, or null. |
| `yards_gained` | Float64 | Yards gained on the play itself (an accepted penalty's yards excluded; 0 on kicks and no-play penalties). |
| `passing_yards` | Float64 | Yards on a completed pass; null otherwise. |
| `receiving_yards` | Float64 | Yards on a completed pass; null otherwise. |
| `rushing_yards` | Float64 | Yards on a run; null otherwise. |
| `penalty_yards` | Float64 | Yards of an accepted penalty; null otherwise. |
| `penalty_team` | String | Team of an accepted penalty, from ESPN's penalized team id. |
| `td_team` | String | Team that scored the touchdown. |
| `spread_line` | Float64 | Home team's point spread (positive when the home team is favored): the schedule's when given, else ESPN's. |
| `total_line` | Float64 | Game total (over/under): the schedule's when given, else ESPN's. |
| `drive` | Float64 | ESPN's drive number within the game (the digits of ESPN's drive id after the game id). |
| `old_game_id` | String | NFL's old game id (YYYYMMDDGG), from the schedule. |
| `game_date` | String | Game date (YYYY-MM-DD), the schedule's gameday. |
| `location` | String | Home or Neutral, from the schedule. |
| `result` | Int64 | Final home score minus away score, from the schedule. |
| `total` | Int64 | Final combined score, from the schedule. |
| `home_score` | Int64 | Final home score, from the schedule. |
| `away_score` | Int64 | Final away score, from the schedule. |
| `div_game` | Int64 | 1 for a divisional game, from the schedule. |
| `roof` | String | outdoors, dome, closed or open, from the schedule. |
| `surface` | String | Playing surface, from the schedule. |
| `temp` | Int64 | Temperature (F), from the schedule; null indoors. |
| `wind` | Int64 | Wind speed (mph), from the schedule; null indoors. |
| `home_coach` | String | Home head coach, from the schedule. |
| `away_coach` | String | Away head coach, from the schedule. |
| `game_stadium` | String | Stadium name, from the schedule (nflverse's stadium column is the feed's own name). |
| `stadium_id` | String | nflverse stadium id, from the schedule. |
| `time_of_day` | String | UTC time of the play, ESPN's wallclock (to the second; nflverse's NFL-feed stamp has milliseconds). Null where ESPN has none. |
| `timeout` | Int64 | 0/1 flag: a timeout row (ESPN type Timeout). |
| `timeout_team` | String | Team that called the timeout, from ESPN's home / away timeout flags (nflverse current abbreviation). |
| `air_yards` | Float64 | ESPN's air yards (empty in ESPN's feed, so cp / cpoe / xYAC and the air / YAC splits stay null). |
| `yards_after_catch` | Float64 | ESPN's yards after catch (empty in ESPN's feed). |
| `passer_player_id` | String | GSIS id of the passer: ESPN's passer participant (or the processor's id) mapped through the nflverse players table's espn_id. |
| `passer_player_name` | String | String name for the player that attempted the pass. |
| `receiver_player_id` | String | GSIS id of the targeted receiver, from ESPN's receiver participant through the players table's espn_id. |
| `receiver_player_name` | String | String name for the targeted receiver. |
| `rusher_player_id` | String | GSIS id of the rusher, from ESPN's rusher participant through the players table's espn_id. |
| `rusher_player_name` | String | String name for the player that attempted the run. |
| `interception_player_id` | String | GSIS id of the interceptor (ESPN's first pass defender on an interception). |
| `interception_player_name` | String | String name for the player that intercepted the pass. |
| `punt_returner_player_id` | String | Unique identifier for the punt returner. |
| `punt_returner_player_name` | String | String name for the punt returner. |
| `kickoff_returner_player_id` | String | Unique identifier for the kickoff returner. |
| `kickoff_returner_player_name` | String | String name for the kickoff returner. |
| `punter_player_id` | String | Unique identifier for the punter. |
| `punter_player_name` | String | String name for the punter. |
| `kicker_player_id` | String | GSIS id of the kicker on a kickoff, field goal or try (a split try takes its touchdown row's PAT kicker). |
| `kicker_player_name` | String | String name for the kicker on FG or kickoff. |
| `blocked_player_id` | String | Unique identifier for the player that blocked the punt or FG. |
| `blocked_player_name` | String | String name for the player that blocked the punt or FG. |
| `tackle_for_loss_1_player_id` | String | GSIS id of the tackler on a tackle for loss, or of a lone sacker on a sack for loss. |
| `tackle_for_loss_1_player_name` | String | String name for one of the potential players with the tackle for loss. |
| `forced_fumble_player_1_player_id` | String | Unique identifier of one of the players with a forced fumble. |
| `forced_fumble_player_1_player_name` | String | String name of one of the players with a forced fumble. |
| `solo_tackle_1_player_id` | String | GSIS id of the tackler on a tackle with no ESPN assist (or the lone sacker when ESPN names no tackler). |
| `solo_tackle_1_player_name` | String | String name of one of the players with a solo tackle. |
| `tackle_with_assist_1_player_id` | String | GSIS id of ESPN's tackler on a tackle ESPN also credits an assist on. |
| `tackle_with_assist_1_player_name` | String | String name of one of the players with a tackle with assist. |
| `assist_tackle_1_player_id` | String | GSIS id of ESPN's first assisted_by participant (or the first of two sackers). |
| `assist_tackle_1_player_name` | String | String name of one of the players with a tackle assist. |
| `assist_tackle_2_player_id` | String | GSIS id of ESPN's second assisted_by participant (or the second of two sackers). |
| `assist_tackle_2_player_name` | String | String name of one of the players with a tackle assist. |
| `pass_defense_1_player_id` | String | Unique identifier of one of the players with a pass defense. |
| `pass_defense_1_player_name` | String | String name of one of the players with a pass defense. |
| `fumbled_1_player_id` | String | Unique identifier of the first player who fumbled on the play. |
| `fumbled_1_player_name` | String | String name of one of the first player who fumbled on the play. |
| `fumble_recovery_1_player_id` | String | Unique identifier of one of the players with a fumble recovery. |
| `fumble_recovery_1_player_name` | String | String name of one of the players with a fumble recovery. |
| `sack_player_id` | String | GSIS id of the sacker when ESPN names one (two are half_sack_1 / half_sack_2). |
| `sack_player_name` | String | String name of the player who recorded a solo sack. |
| `half_sack_1_player_id` | String | Unique identifier of the first player who recorded half a sack. |
| `half_sack_1_player_name` | String | String name of the first player who recorded half a sack. |
| `half_sack_2_player_id` | String | Unique identifier of the second player who recorded half a sack. |
| `half_sack_2_player_name` | String | String name of the second player who recorded half a sack. |
| `penalty_player_id` | String | GSIS id of ESPN's penalized participant (the only player slot a no-play keeps). |
| `penalty_player_name` | String | String name for the player with the penalty. |
| `td_player_id` | String | Unique identifier of the player who scored a touchdown. |
| `td_player_name` | String | String name of the player who scored a touchdown. |
| `solo_tackle_1_team` | String | Team of one of the players with a solo tackle. |
| `tackle_with_assist_1_team` | String | Team of one of the players with a tackle with assist. |
| `assist_tackle_1_team` | String | Team of one of the players with a tackle assist. |
| `assist_tackle_2_team` | String | Team of one of the players with a tackle assist. |
| `forced_fumble_player_1_team` | String | Team of one of the players with a forced fumble. |
| `fumbled_1_team` | String | Team of one of the first player with a fumble. |
| `fumble_recovery_1_team` | String | Team of one of the players with a fumble recovery. |
| `run_gap` | String | String indicator for line gap of run: end, guard, or tackle |
| `shotgun` | Int64 | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | Int64 | Binary indicator for whether or not the play was in no_huddle formation. |
| `out_of_bounds` | Int64 | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `replay_or_challenge` | Int64 | Binary indicator for whether or not a replay or challenge. |
| `replay_or_challenge_result` | String | String indicating the result of the replay or challenge. |
| `penalty_type` | String | String indicating the penalty type of the first penalty in the given play. Will be `NA` if `desc` is missing the type. |
| `pass` | Int64 | 0/1 flag: a dropback (pass, sack or scramble text), nflfastR clean_pbp's pass. |
| `rush` | Int64 | 0/1 flag: a designed run with a rusher (not a kneel or a pass), nflfastR clean_pbp's rush. |
| `fixed_drive` | Float64 | Drive number in the game: ESPN's own drive (nflfastR's new_drive cascade on this frame agrees less often with nflverse). |
| `fixed_drive_result` | String | How the drive ended (Touchdown, Punt, Field goal, Turnover, ...): nflfastR's drive-result cascade over ESPN's drive. |
| `series` | Float64 | Series number in the game (a new series on each first down or new drive), nflfastR's add_series_data on ESPN's drives. |
| `series_result` | String | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `series_success` | Int64 | 1: scored touchdown, gained enough yards for first down. |
| `drive_first_downs` | Float64 | First downs (rush, pass or penalty) on the drive's plays. |
| `drive_inside20` | Float64 | 1 if the drive reached the opponent's 20-yard line, else 0. |
| `drive_play_id_started` | Float64 | play_id of the drive's first play. |
| `drive_game_clock_start` | String | Game clock at the drive's start (MM:SS), from ESPN's drive record. |
| `drive_game_clock_end` | String | Game clock at the drive's end (MM:SS), from ESPN's drive record. |
| `drive_start_yard_line` | String | Where the drive started (e.g. KC 30), from ESPN's drive record in nflverse's current abbreviations. |
| `ep` | Float64 | Using the scoring event probabilities, the estimated expected points with respect to the possession team for the given play. |
| `td_prob` | Float64 | Predicted probability of the posteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `opp_td_prob` | Float64 | Predicted probability of the defteam scoring a TD next. 'Next' in this context means the next score in the same game half. |
| `fg_prob` | Float64 | Predicted probability of the posteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `opp_fg_prob` | Float64 | Predicted probability of the defteam scoring a FG next. 'Next' in this context means the next score in the same game half. |
| `safety_prob` | Float64 | Predicted probability of the posteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `opp_safety_prob` | Float64 | Predicted probability of the defteam scoring a safety next. 'Next' in this context means the next score in the same game half. |
| `no_score_prob` | Float64 | Predicted probability of no score occurring for the rest of the half based on the expected points model. |
| `extra_point_prob` | Float64 | Predicted probability of the posteam scoring an extra point. |
| `two_point_conversion_prob` | Float64 | Predicted probability of the posteam scoring the two point conversion. |
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
| `era0` | Int64 | 1 for seasons through 2001, else 0 (model era feature). |
| `era1` | Int64 | 1 for seasons 2002-2005, else 0 (model era feature). |
| `era2` | Int64 | 1 for seasons 2006-2013, else 0 (model era feature). |
| `era3` | Int64 | 1 for seasons 2014-2017, else 0 (model era feature). |
| `era4` | Int64 | 1 for seasons from 2018 on, else 0 (model era feature). |
| `down1` | Int64 | 1 on a first-down play, else 0 (expected-points model feature). |
| `down2` | Int64 | 1 on a second-down play, else 0 (expected-points model feature). |
| `down3` | Int64 | 1 on a third-down play, else 0 (expected-points model feature). |
| `down4` | Int64 | 1 on a fourth-down play, else 0 (expected-points model feature). |
| `home` | Int64 | 1 when the possession team is the home team, else 0 (model feature). |
| `retractable` | Int64 | 1 only when the roof is the literal value "retractable", which nflverse does not ship, else 0 (model feature). |
| `dome` | Int64 | 1 when the game is played under a dome or a closed roof, else 0 (model feature; a null roof counts as outdoors). |
| `outdoors` | Int64 | 1 when the game is played outdoors or under an open roof, else 0 (model feature; a null roof counts as outdoors). |
| `receive_2h_ko` | Int64 | 1 on a first-half play when the possession team receives the second-half kickoff, else 0 (win-probability model feature). |
| `posteam_spread` | Float64 | Point spread from the possession team's side, nflverse spread_line for the home team and its negation for the away team. |
| `elapsed_share` | Float64 | Share of regulation elapsed at the snap, (3600 - game_seconds_remaining) / 3600 clipped to [0, 1]. |
| `spread_time` | Float64 | Win-probability model feature, posteam_spread * exp(-4 * elapsed_share), so the spread fades as the game runs down; 0 when spread_line is missing. |
| `Diff_Time_Ratio` | Float64 | Win-probability model feature, score_differential divided by exp(-4 * elapsed_share), so a lead weighs more as the game runs down. |
| `wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `vegas_wp` | Float64 | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `home_wp` | Float64 | Estimated win probability for the home team. |
| `vegas_home_wp` | Float64 | Estimated win probability for the home team incorporating pre-game Vegas line. |
| `away_wp` | Float64 | Estimated win probability for the away team. |
| `def_wp` | Float64 | Estimated win probability for the defteam. |
| `vegas_home_wpa` | Float64 | Win probability added (WPA) for the home team: spread_adjusted model. |
| `vegas_wpa` | Float64 | Win probability added (WPA) for the posteam: spread_adjusted model. |
| `wpa` | Float64 | Win probability added (WPA) for the posteam. |
| `total_home_rush_wpa` | Float64 | Cumulative total rushing WPA for the home team in the game so far. |
| `total_away_rush_wpa` | Float64 | Cumulative total rushing WPA for the away team in the game so far. |
| `total_home_pass_wpa` | Float64 | Cumulative total passing WPA for the home team in the game so far. |
| `total_away_pass_wpa` | Float64 | Cumulative total passing WPA for the away team in the game so far. |
| `home_wp_post` | Float64 | Estimated win probability for the home team at the end of the play. |
| `away_wp_post` | Float64 | Estimated win probability for the away team at the end of the play. |
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
| `qbr_epa` | Float64 | The quarterback's EPA on the play as the QBR model reads it, floored at -5 and set to -3.5 on a fumble. |
| `weight` | Float64 | Official weight, in pounds |
| `non_fumble_sack` | Boolean | True for a sack with no fumble on the play (QBR model input). |
| `sack_epa` | Float64 | qbr_epa on sacks without a fumble, null otherwise (QBR model input). |
| `pass_epa` | Float64 | qbr_epa on pass plays, null otherwise (QBR model input). |
| `rush_epa` | Float64 | qbr_epa on rushing plays, null otherwise (QBR model input). |
| `pen_epa` | Float64 | qbr_epa on plays with a penalty, null otherwise (QBR model input). |
| `sack_weight` | Float64 | The play's QBR weight on sacks without a fumble, null otherwise. |
| `pass_weight` | Float64 | The play's QBR weight on pass plays, null otherwise. |
| `rush_weight` | Float64 | The play's QBR weight on rushing plays, null otherwise. |
| `pen_weight` | Float64 | The play's QBR weight on plays with a penalty, null otherwise. |
| `action_play` | Boolean | True when the play's qb_epa is non-zero, i.e. the play counts toward the QBR model's inputs. |
| `success` | Float64 | Binary indicator whether epa > 0 in the given play. |
| `play` | Float64 | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `plays` lacks the processor's columns, or carries no `nflverse_game_id` and no `schedule` (with `game_id` and `espn`) is given or the schedule lacks one of its games, or an id column is a float (a play / game id, or an athlete id in `plays`, `participants` or `players`), or `players` lacks `gsis_id` / `espn_id`, or a split try's synthetic id is already a play's (`~sportsdataverse.nfl.split_pat_rows`). |

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess, espn_nfl_pbp_nflverse, load_nfl_schedule
proc = NFLPlayProcess(gameId=401671740)
proc.espn_nfl_pbp()
proc.run_processing_pipeline()
pbp = espn_nfl_pbp_nflverse(proc.plays_frame, load_nfl_schedule([2024]))

# With GSIS player ids from ESPN's participants, and the model columns

from sportsdataverse.nfl import espn_nfl_play_participants, load_nfl_players
pbp = espn_nfl_pbp_nflverse(
    proc.plays_frame,
    participants=espn_nfl_play_participants(401671740),
    players=load_nfl_players(),
    enrich=True,
)
```
