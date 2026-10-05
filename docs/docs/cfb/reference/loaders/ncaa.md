---
title: "CFB dataset loaders — NCAA (stats.ncaa.org)"
sidebar_label: "NCAA (stats.ncaa.org)"
sidebar_position: 4
description: "CFB dataset loaders — NCAA (stats.ncaa.org) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB dataset loaders — NCAA (stats.ncaa.org)

## load_ncaa_mfb_pbp

Release: [ncaa_mfb_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_pbp/ncaa_mfb_pbp_{season}.parquet`
### Returns {#load_ncaa_mfb_pbp-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `drive_number` | Int64 | Sequential drive number within the game (1-indexed). |
| `play_number` | Int64 | Sequential play number within the game (1-indexed). |
| `offense` | String | Full name of the offense (team in possession) on the play. |
| `drive_result` | String | Drive result code (`drive_`-prefixed; every drive-level column is carried with this prefix). |
| `drive_scored` | Boolean | Whether the possession containing this play ended in points for the offense. |
| `down` | Int64 | Down of the play (1-4). |
| `distance` | Int64 | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `yard_line` | String | Field-position yard line at the start of the play (0-50 scale from the offense's side). |
| `yard_line_side` | String | Which team's territory the ball was on at the snap ('OFF' own side, 'DEF' opponent side); pairs with yard_line_number for absolute field position. |
| `yard_line_number` | Int64 | Yard line at the snap (0-50); pairs with yard_line_side for absolute field position. |
| `play_type` | String | CFBD play type label (e.g. "Rush", "Pass Reception", "Field Goal Good"). |
| `clock` | String | Game clock display value at the play (`MM:SS`). |
| `yards_gained` | Int64 | Net yards gained by the offense on the play. |
| `formation` | String | Offensive formation or personnel grouping reported for the play (e.g. 'Shotgun', 'I-Formation'), when the source narrative names it. |
| `passer` | String | Name of the dropback player (scrambles included) including plays with penalties. |
| `rusher` | String | Name of the rusher (no scrambles) including plays with penalties. |
| `receiver` | String | Name of the receiver including plays with penalties. |
| `kicker` | String | Name of the player who kicked off, punted, or attempted the field goal/PAT on this play. |
| `punter` | String | Name of the player who punted on this play. |
| `returner` | String | Name of the player who fielded or returned the kickoff or punt on this play. |
| `run_direction` | String | Hole or side the ball carrier ran through on a rush play (e.g. 'left end', 'right guard'), when the narrative reports it. |
| `qb_scramble` | Boolean | Binary indicator for whether or not the QB scrambled. |
| `pass_complete` | Boolean | Whether a pass attempt on this play was completed. |
| `pass_depth` | String | Depth classification of a pass attempt (e.g. 'short', 'deep'), when the narrative reports it. |
| `pass_direction` | String | Side of the field the pass was thrown to (e.g. 'left', 'middle', 'right'), when the narrative reports it. |
| `tackler_1` | String | Name of the primary (first-listed) tackler on the play. |
| `tackler_2` | String | Name of the secondary (assisting) tackler on the play, when the narrative credits an assist. |
| `kick_yards` | Int64 | Yards traveled on a kickoff. |
| `return_yards` | Int64 | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `punt_yards` | Int64 | Gross yards traveled on a punt, before any return. |
| `fg_distance` | Int64 | Distance in yards of a field goal attempt. |
| `fg_made` | Boolean | TRUE when the field goal attempt was successful. |
| `is_first_down` | Boolean | Whether the play resulted in a first down for the offense. |
| `is_touchdown` | Boolean | Whether the play resulted in a touchdown. |
| `is_safety` | Boolean | Whether the play resulted in a safety. |
| `is_fumble` | Boolean | Whether a fumble occurred on the play, regardless of which team recovered it. |
| `is_turnover` | Boolean | `TRUE` if the play was a turnover. |
| `turnover_type` | String | Kind of turnover on the play, if any (e.g. 'interception', 'fumble lost'); null when no turnover occurred. |
| `out_of_bounds` | Boolean | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `no_play` | Boolean | Whether the play was negated (e.g. by a penalty on the preceding down) and is excluded from drive/stat totals. |
| `fair_catch` | Boolean | Whether the returner called a fair catch on a kickoff or punt. |
| `penalty_flag` | Boolean | TRUE when a penalty was flagged on the play. |
| `penalty_team` | String | String abbreviation of the team with the penalty. |
| `penalty_type` | String | String indicating the penalty type of the first penalty in the given play. Will be `NA` if `desc` is missing the type. |
| `penalty_player` | String | Name of the player penalized on the play, when a penalty occurred. |
| `penalty_yards` | Int64 | Yards gained (or lost) by the posteam from the penalty. |
| `end_yard_line` | String | Yard line at the end of the play. |
| `play_text` | String | Free-form text description of the play from the CFBD feed. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_mfb_pbp(seasons=2024)
```

## load_ncaa_mfb_pbp_cfbfastr

Release: [ncaa_mfb_pbp_cfbfastr](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_pbp_cfbfastr) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_pbp_cfbfastr/ncaa_mfb_pbp_cfbfastr_{season}.parquet`
### Returns {#load_ncaa_mfb_pbp_cfbfastr-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game identifier. |
| `id_play` | Int64 | Unique CFBD play identifier (concatenates game_id and play index). |
| `drive_id` | Int64 | CFBD drive identifier the play belongs to. |
| `game_play_number` | Int64 | Sequential play number within the game (excludes timeouts/end markers). |
| `half_play_number` | Int64 | Sequential play number within the current half. |
| `drive_play_number` | Int64 | Sequential play number within the current drive. |
| `drive_number` | Int64 | Sequential drive number within the game (1-indexed). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `year` | Int64 | Four-digit season year (e.g. 2019). |
| `week` | Int64 | Game week of the season. |
| `period` | Int64 | Period (quarter) number. |
| `half` | Int64 | Half indicator (1 or 2). |
| `clock.minutes` | Int64 | Minutes remaining on the game clock at the start of the play (cfbfastR schema column). |
| `clock.seconds` | Int64 | Seconds remaining within the current minute on the game clock at the start of the play (cfbfastR schema column). |
| `TimeSecsRem` | Int64 | Seconds remaining in the half at the start of the play. |
| `Under_two` | Boolean | TRUE when under two minutes remain in the half. |
| `pos_team` | String | Team name in possession at the start of the play (offense, kickoff-aware). |
| `def_pos_team` | String | Team name on defense at the start of the play. |
| `offense_play` | String | Offensive team name as labeled by CFBD on the play. |
| `defense_play` | String | Defensive team name as labeled by CFBD on the play. |
| `home` | String | Home team name. |
| `away` | String | Away team name. |
| `pos_team_score` | Int64 | Score for the team in possession at the start of the play. |
| `def_pos_team_score` | Int64 | Score for the defensive team at the start of the play. |
| `offense_score` | Int64 | Offense team score at the start of the play. |
| `defense_score` | Int64 | Defense team score at the start of the play. |
| `pos_score_diff` | Int64 | Score differential from the possession team's perspective. |
| `score_pts` | Int64 | Points scored on the play. |
| `scoring_play` | Boolean | `TRUE` if the play resulted in a score. |
| `scoring` | Boolean | TRUE when the play results in a score (TD, FG, safety, two-point conversion). |
| `down` | Int64 | Down of the play (1-4). |
| `distance` | Int64 | Yards to gain for a first down (or to the goal line in goal-to-go situations). |
| `yard_line` | String | Field-position yard line at the start of the play (0-50 scale from the offense's side). |
| `yards_to_goal` | Int64 | Distance in yards from the offense's spot to the opponent's goal line (0-100). |
| `yards_to_goal_end` | Int64 | Yards to opponent end zone at the end of the play. |
| `Goal_To_Go` | Boolean | TRUE when the offense is in a goal-to-go situation. |
| `log_ydstogo` | Float64 | Natural log of distance-to-go (model feature). |
| `yards_gained` | Int64 | Net yards gained by the offense on the play. |
| `play_type` | String | CFBD play type label (e.g. "Rush", "Pass Reception", "Field Goal Good"). |
| `orig_play_type` | String | Original CFBD play type label before cfbfastR cleaning. |
| `play_text` | String | Free-form text description of the play from the CFBD feed. |
| `rush` | Boolean | Binary flag for a rushing play. |
| `rush_td` | Boolean | Binary flag for a rushing touchdown. |
| `pass` | Boolean | Binary flag for a passing play (includes sacks). |
| `pass_td` | Boolean | Binary flag for a passing touchdown. |
| `pass_attempt` | Boolean | Binary flag for a pass attempt. |
| `completion` | Boolean | Binary flag for a completed pass. |
| `target` | Boolean | Binary flag for a targeted receiver on the play. |
| `sack` | Boolean | Binary flag for a sack (duplicate of sack_vec for downstream use). |
| `sack_vec` | Boolean | Binary flag for a sack play. |
| `int` | Boolean | Binary flag for an interception. |
| `int_td` | Boolean | Binary flag for an interception returned for a touchdown. |
| `turnover_vec` | Boolean | Binary flag for any play classified as a turnover. |
| `downs_turnover` | Boolean | Binary flag for a turnover on downs. |
| `touchdown` | Boolean | Binary flag for a touchdown (duplicate of td_play for downstream use). |
| `td_play` | Boolean | Binary flag for a touchdown play. |
| `safety` | Boolean | Binary flag for a safety. |
| `fumble_vec` | Boolean | Binary flag for a play involving a fumble. |
| `punt` | Boolean | Binary flag for a punt play. |
| `punt_play` | Boolean | Binary flag for any punt-related play (includes blocks/returns). |
| `kickoff_play` | Boolean | Binary flag for a kickoff play. |
| `kick_play` | Boolean | Binary flag for any kicking play (kickoff or field goal). |
| `fg_inds` | Boolean | Binary flag for a field goal attempt. |
| `fg_made` | Boolean | TRUE when the field goal attempt was successful. |
| `punt_blocked` | Boolean | Binary flag for a blocked punt. |
| `punt_fair_catch` | Boolean | Binary flag for a punt fair catch. |
| `firstD_by_yards` | Boolean | Binary flag for a new first down via yards gained. |
| `firstD_by_penalty` | Boolean | Binary flag for a new first down via penalty. |
| `penalty_flag` | Boolean | TRUE when a penalty was flagged on the play. |
| `penalty_no_play` | Boolean | TRUE when the penalty nullified the play (no play counted). |
| `penalty_declined` | Boolean | TRUE when the penalty was declined. |
| `penalty_offset` | Boolean | TRUE when offsetting penalties were called. |
| `penalty_text` | String | TRUE when penalty information is detectable in the play text. |
| `yds_penalty` | Int64 | Yardage assessed on the penalty. |
| `rusher_player_name` | String | Name of the rusher on a rushing play. |
| `passer_player_name` | String | Name of the passer on a passing play. |
| `receiver_player_name` | String | Name of the receiver on a passing play. |
| `interception_player_name` | String | Name of the defender credited with the interception. |
| `punter_player_name` | String | Name of the punter. |
| `punt_returner_player_name` | String | Name of the punt returner. |
| `fg_kicker_player_name` | String | Name of the field goal kicker. |
| `kickoff_player_name` | String | Name of the kickoff specialist. |
| `kickoff_returner_player_name` | String | Name of the kickoff returner. |
| `yds_rushed` | Int64 | Rushing yards gained on the play. |
| `yds_receiving` | Int64 | Receiving yards gained on the play. |
| `yds_sacked` | Int64 | Yards lost on the sack. |
| `yds_punted` | Int64 | Yards the ball traveled on the punt. |
| `yds_punt_return` | Int64 | Yards gained on the punt return. |
| `yds_kickoff` | Int64 | Yards the ball traveled on the kickoff. |
| `yds_kickoff_return` | Int64 | Yards gained on the kickoff return. |
| `yds_int_return` | Int64 | Yards gained on an interception return. |
| `yds_fg` | Int64 | Distance of the field goal attempt in yards. |
| `drive_result` | String | Drive result code (`drive_`-prefixed; every drive-level column is carried with this prefix). |
| `drive_scoring` | Boolean | Binary flag for a scoring drive. |
| `ot_synthesized` | Boolean | Whether this row's overtime clock/period fields were synthesized rather than sourced directly -- stats.ncaa.org reports no running clock in overtime, so OT plays are assigned a synthetic countdown. |
| `lag_pos_team` | String | Possession team on the previous play (lag value). |
| `lead_pos_team` | String | Possession team on the next play (lead value). |
| `lag_play_type` | String | Play type on the previous play (lag value). |
| `lead_play_type` | String | Play type on the next play (lead value). |
| `lag_play_text` | String | Play text from the previous play (lag value). |
| `lead_play_text` | String | Play text from the next play (lead value). |
| `change_of_pos_team` | Boolean | Binary flag for change of possession-team on the play. |
| `play_after_turnover` | Boolean | Binary flag indicating the play immediately following a turnover. |
| `n_plays_in_game` | UInt32 | Total number of plays in the game this row belongs to, repeated on every row for convenience. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |

```python
load_ncaa_mfb_pbp_cfbfastr(seasons=2024)
```

## load_ncaa_mfb_drives

Release: [ncaa_mfb_drives](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_drives) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_drives/ncaa_mfb_drives_{season}.parquet`
### Returns {#load_ncaa_mfb_drives-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `drive_number` | Int64 | Sequential drive number within the game (1-indexed). |
| `quarter` | Int64 | Quarter in which the drive started (1-4 for regulation, 5+ for overtime periods). |
| `period` | Int64 | Period (quarter) number. |
| `team` | String | Team name. |
| `start_period` | Int64 | Period (quarter) in which the drive starts. |
| `start_how` | String | How the drive began (e.g. 'Kickoff', 'Interception', 'Fumble', 'Downs', 'Punt'). |
| `start_clock` | String | Game clock display value at the start of the drive. |
| `start_yard_line` | String | Yard line at the start of the play. |
| `end_period` | Int64 | Period (quarter) in which the drive ends. |
| `end_how` | String | How the drive ended (e.g. 'Touchdown', 'Punt', 'Interception', 'Fumble', 'End of Half'). |
| `end_clock` | String | Game clock display value at the end of the drive. |
| `end_yard_line` | String | Yard line at the end of the play. |
| `n_plays` | Int64 | Number of plays run during the drive. |
| `yards` | Int64 | Total yards gained on the drive. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_mfb_drives(seasons=2024)
```

## load_ncaa_mfb_schedule

Release: [ncaa_mfb_schedule](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_schedule) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_schedule/ncaa_mfb_schedule_{season}.parquet`
### Returns {#load_ncaa_mfb_schedule-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | String | ESPN team id. |
| `team_name` | String | Team nickname; `team_detail = TRUE` only. |
| `date` | String | Date of the poll release. |
| `opponent_id` | String | ESPN team id of the opponent. |
| `opponent` | String | Opponent team name. |
| `result` | String | Drive result code (e.g. `PUNT`, `TD`). |
| `outcome` | String | Result of the game from the home team's perspective (e.g. 'W', 'L', 'T'). |
| `team_score` | Int64 | Offense team score at the time of the play. |
| `opponent_score` | Int64 | Defense / opponent team score at the time of the play. |
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `attendance` | Int64 | Reported attendance at the game. |
| `academic_year` | Int32 | Academic year the game was played in (the ENDING year of the fall/spring split, e.g. 2025 for the 2024 fall season) -- distinct from `season`, which is the STARTING year. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_mfb_schedule(seasons=2024)
```

## load_ncaa_mfb_rosters

Release: [ncaa_mfb_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_rosters/ncaa_mfb_rosters_{season}.parquet`
### Returns {#load_ncaa_mfb_rosters-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | String | ESPN team id. |
| `team_name` | String | Team nickname; `team_detail = TRUE` only. |
| `player_id` | String | ESPN player id from the roster entry. |
| `player_name` | String | Full name of player |
| `jersey` | String | Jersey number. |
| `statcrew_jersey` | String | Jersey number as recorded in the NCAA StatCrew roster feed; can differ from the number a player actually wears on a given game day. |
| `player_class` | String | Player's academic/eligibility class (e.g. 'FR', 'SO', 'JR', 'SR', 'GR'). |
| `position` | String | Athlete position. |
| `height` | String | Listed height (inches). |
| `weight` | Int64 | Listed weight (lbs). |
| `hometown` | String | Prospect hometown. |
| `high_school` | String | High school |
| `games_played` | Int64 | Games played. |
| `games_started` | Int64 | Games started (goalies). |
| `academic_year` | Int32 | Academic year the roster snapshot covers (the ENDING year of the fall/spring split) -- distinct from `season`, which is the STARTING year. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_mfb_rosters(seasons=2024)
```

## load_ncaa_mfb_teams

Release: [ncaa_mfb_teams](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_teams) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_teams/ncaa_mfb_teams_{season}.parquet`

:::caution[Coverage]
No conference column: for conference membership by season use load_cfb_team_group_seasons (the cfb_groups release).
:::

### Returns {#load_ncaa_mfb_teams-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | String | stats.ncaa.org team id. It is issued per season, so a school's id changes from year to year; it is not an ESPN id. |
| `team_name` | String | School name as stats.ncaa.org lists it, without the mascot. |
| `academic_year` | Int32 | Academic year the team record covers (the ENDING year of the fall/spring split) -- distinct from `season`, which is the STARTING year. |
| `division` | Int32 | stats.ncaa.org division code, 11 = FBS and 12 = FCS; not a conference division (the table has no conference column). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_mfb_teams(seasons=2024)
```

## load_ncaa_mfb_team_stats

Release: [ncaa_mfb_team_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_team_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_team_stats/ncaa_mfb_team_stats_{season}.parquet`
### Returns {#load_ncaa_mfb_team_stats-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `category` | String | stats.ncaa.org box-score section the stat belongs to (Passing, Rushing, First Downs, Total Offense, Kicking, Punt Returns, Kickoffs and KO Returns, Sacks, Passes Defended); not a CFBD category. |
| `stat` | String | Stat. |
| `period` | String | Period (quarter) number. |
| `away_team` | String | Away team name. |
| `away_value` | String | Team-stat value for the away team; each row is one stat category for one game, wide by side. |
| `home_team` | String | Home team name. |
| `home_value` | String | Team-stat value for the home team; each row is one stat category for one game, wide by side. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_mfb_team_stats(seasons=2024)
```

## load_ncaa_mfb_player_stats

Release: [ncaa_mfb_player_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_player_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_player_stats/ncaa_mfb_player_stats_{season}.parquet`
### Returns {#load_ncaa_mfb_player_stats-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `team_id` | String | ESPN team id. |
| `number` | String | Week number as returned by the API. |
| `name` | String | Position name (e.g. `Quarterback`). |
| `position` | String | Athlete position. |
| `rush_attempts` | String | The number of rushing attempts |
| `rush_yds_gained` | String | Total positive rushing yards gained on carries, before subtracting yards lost to tackles for loss. |
| `rush_yds_lost` | String | Total rushing yards lost to tackles for loss on carries. |
| `yds_rush` | String | Net rushing yards (rush_yds_gained minus rush_yds_lost). |
| `rush_tds` | String | Rushing touchdowns for the game. Populated only on this player-game's 'rushing' category row (null on the 'passing'/'receiving' rows for the same player-game) -- the loader returns one row per player-game-category. |
| `rush_long` | String | Longest single rush of the game. |
| `category` | String | CFBD stats category name (e.g. passing, rushing, defensive). |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `pass_attempts` | String | Pass attempts for the game. Populated only on this player-game's 'passing' category row (null on the 'rushing'/'receiving' rows for the same player-game) -- the loader returns one row per player-game-category. |
| `completions` | String | The number of completed passes. |
| `pass_yards` | String | Number of yards gained on pass plays |
| `interceptions` | String | Passing interceptions. |
| `pass_tds` | String | Passing touchdowns thrown for the game. Populated only on this player-game's 'passing' category row (null on the 'rushing'/'receiving' rows for the same player-game) -- the loader returns one row per player-game-category. |
| `pass_eff` | String | Passer efficiency rating for the game, per the NCAA passer-rating formula. |
| `yds_per_completion` | String | Passing yards divided by completions. |
| `pct` | String | Win percentage. |
| `long_pass` | String | Longest completed pass of the game. |
| `rec` | String | Total receptions for the game. |
| `receiving_yards` | String | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | String | Receiving yards divided by receptions. |
| `rec_td` | String | Receiving touchdowns. |
| `long_rec` | String | Longest reception of the game. |
| `yds` | String | Yards recorded for the player in the stat category of the row (can be negative). |
| `plays` | String | Total qualifying passing plays included in the WEPA calculation. |
| `pbu` | String | Passes broken up. |
| `int` | String | Binary flag for an interception. |
| `intyds` | String | Interception return yards (can be negative). |
| `int_ret_tds` | String | Touchdowns scored on interception returns. |
| `pdef` | String | Passes defended, as emitted by the feed with a decimal component (e.g. '7.00'). |
| `ko_ret` | String | Kickoff returns. |
| `ko_ret_yds` | String | Kickoff return yards. |
| `kick_ret_tds` | String | Touchdowns scored on kick returns. |
| `long_kor` | String | Longest kickoff return, in yards. |
| `sacks` | String | Team sacks. |
| `solo_tack` | String | Solo (unassisted) tackles. |
| `asst_tack` | String | Assisted tackles. |
| `tackles` | String | Team tackles. |
| `fgm` | String | Field goals made. |
| `fga` | String | Field goal attempts. |
| `fg_blocks_allowed` | String | Field goals blocked against the player's unit. |
| `punt_ret` | String | Punt returns fielded by the player. |
| `punt_ret_yds` | String | Punt return yards (can be negative). |
| `punt_ret_tds` | String | Touchdowns scored on punt returns. |
| `long_pr` | String | Longest punt return, in yards. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_mfb_player_stats(seasons=2024)
```

## load_ncaa_mfb_officials

Release: [ncaa_mfb_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_officials) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_officials/ncaa_mfb_officials_{season}.parquet`
### Returns {#load_ncaa_mfb_officials-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `role` | String | Grouped official role (Referee/Linesperson). |
| `official` | String | Flag indicating that the media item comes from the official league feed rather than an editorial source. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_mfb_officials(seasons=2024)
```

## load_ncaa_mfb_linescore

Release: [ncaa_mfb_linescore](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/ncaa_mfb_linescore) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/ncaa_mfb_linescore/ncaa_mfb_linescore_{season}.parquet`
### Returns {#load_ncaa_mfb_linescore-returns}

| col_name | type | description |
|---|---|---|
| `contest_id` | String | stats.ncaa.org contest (game) identifier. |
| `team` | String | Team name. |
| `home_away` | String | `home` or `away`. |
| `period` | String | Period (quarter) number. |
| `points` | Int64 | Total points accumulated by the school in the poll's weighted voting. |
| `final` | Int64 | Flag for whether the game is final. |
| `game_date` | String | Kickoff date-time (ISO 8601, UTC). |
| `venue` | String | Venue name. |
| `attendance` | Int64 | Reported attendance at the game. |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_ncaa_mfb_linescore(seasons=2024)
```
