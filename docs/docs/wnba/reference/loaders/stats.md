---
title: "WNBA dataset loaders — Stats"
sidebar_label: "Stats"
sidebar_position: 2
description: "WNBA dataset loaders — Stats — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA dataset loaders — Stats

## load_wnba_stats_coaches

Release: [wnba_stats_coaches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_coaches) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_coaches/coaches_{season}.parquet`
### Returns {#load_wnba_stats_coaches-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `coach_id` | Int64 | Unique identifier for coach. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `coach_name` | String | Full name of the staff member as the feed renders it, exactly first_name plus a space plus last_name on every published row. |
| `is_assistant` | Int64 | Numeric staff-role code rather than a boolean flag: 1 head coach, 2 assistant coach, 3 trainer, 9 associate head coach, mapping one-to-one onto coach_type. |
| `coach_type` | String | Job title of the staff member, one of Head Coach, Associate Head Coach, Assistant Coach or Trainer in the published data. |
| `sort_sequence` | Null | Ordering field passed through unchanged from the stats.wnba.com coaches result set; it arrives empty, so every published row is null. |
| `sub_sort_sequence` | Int64 | Secondary display-ordering rank that tracks coach_type exactly: 1 head coach, 2 associate head coach, 5 assistant coach, 7 trainer. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

```python
load_wnba_stats_coaches(seasons=2026)
```

## load_wnba_stats_draft

Release: [wnba_stats_draft](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_draft) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_draft/draft_{season}.parquet`
### Returns {#load_wnba_stats_draft-returns}

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `round_number` | Int64 | Numeric round. |
| `round_pick` | Int64 | Round pick. |
| `overall_pick` | Int64 | Overall pick. |
| `draft_type` | String | CONSTANT in the published asset: every row reads 'Draft', so it does not currently distinguish the main draft from any other selection event. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `organization` | String | Organization. |
| `organization_type` | String | Organization type. |
| `player_profile_flag` | Int64 | Player profile flag. |

```python
load_wnba_stats_draft(seasons=2025)
```

## load_wnba_stats_game_rosters

Release: [wnba_stats_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_game_rosters/game_rosters_{season}.parquet`
### Returns {#load_wnba_stats_game_rosters-returns}

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_num` | String | Jersey number worn by the player. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `game_id` | String | Unique game identifier. |

```python
load_wnba_stats_game_rosters(seasons=2026)
```

## load_wnba_stats_officials

Release: [wnba_stats_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_officials) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_officials/officials_{season}.parquet`
### Returns {#load_wnba_stats_officials-returns}

| col_name | type | description |
|---|---|---|
| `official_id` | Int64 | Unique official / referee identifier. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_num` | String | Jersey number worn by the player. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `game_id` | String | Unique game identifier. |

```python
load_wnba_stats_officials(seasons=2026)
```

## load_wnba_stats_pbp

Release: [wnba_stats_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_pbp/wnba_play_by_play_{season}.parquet`
### Returns {#load_wnba_stats_pbp-returns}

| col_name | type | description |
|---|---|---|
| `order_index` | Int64 | Stable ordering index of the event within the game's stats.wnba.com play-by-play. |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `clock` | String | Game clock value. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | Int64 | Unique team identifier. |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `player_name_i` | String | Player name i. |
| `x_legacy` | Int64 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int64 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `shot_distance` | Int64 | Shot distance from the basket, in feet. |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `is_field_goal` | Int64 | 1 if the action was a field goal; 0 otherwise. |
| `score_home` | String | Score home. |
| `score_away` | String | Score away. |
| `points_total` | Int64 | Running total of points scored. |
| `location` | String | Filter results by game location. |
| `description` | String | Long-form description text. |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `video_available` | Int64 | Video available. |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `action_id` | Int64 | Unique action identifier within a game (V3 PBP). |
| `game_id` | String | Unique game identifier. |
| `seconds_remaining` | Float64 | Seconds remaining in the period. |
| `event_type` | String | Event / play type code (V2 PBP). |
| `is_made_shot` | Boolean | True when the event is a made field goal. |
| `is_missed_shot` | Boolean | True when the event is a missed field goal. |
| `is_free_throw` | Boolean | True when the event is a free throw attempt. |
| `is_rebound` | Boolean | True when the event is a rebound (player or team). |
| `is_turnover` | Boolean | `TRUE` if the play was a turnover. |
| `is_foul` | Boolean | True when the event is a foul. |
| `is_substitution` | Boolean | True when the event is a substitution. |
| `is_jump_ball` | Boolean | True when the event is a jump ball. |
| `is_timeout` | Boolean | True when the event is a timeout. |
| `is_period` | Boolean | True for period-start and period-end marker events. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_wnba_stats_pbp(seasons=2025)
```

## load_wnba_stats_possessions

Release: [wnba_stats_possessions](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_possessions) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_possessions/wnba_possessions_{season}.parquet`
### Returns {#load_wnba_stats_possessions-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 | stats.wnba.com identifier of the defending team for the possession. |
| `start_order_index` | Int64 | order_index of the first event in the possession; joins to the wnba_stats_pbp table. |
| `end_order_index` | Int64 | order_index of the last event in the possession; joins to the wnba_stats_pbp table. |
| `start_seconds_remaining` | Float64 | Seconds remaining in the period when the possession began. |
| `end_seconds_remaining` | Float64 | Seconds remaining in the period when the possession ended. |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean | True when the possession continued after an offensive rebound (contains a second-chance segment). |
| `number_in_period` | Int64 | Possession number within the period, resetting to 1 at each period start. |
| `possession_start_type` | String | How the possession began: OffDeadball, OffTimeout, OffMadeShot, OffMissedShot, or OffLiveBallTurnover. |
| `count_as_possession` | Boolean | False only for a possession starting with 2 seconds or less left in the period and no made basket before the period ends. |
| `fg2a` | Int64 | Two-point field goals attempted by the offense during the possession. |
| `fg2m` | Int64 | Two-point field goals made by the offense during the possession. |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `off_player_1` | Int64 | stats.wnba.com identifier of offensive on-court player 1 of 5 for the possession (unordered slot). |
| `off_player_2` | Int64 | stats.wnba.com identifier of offensive on-court player 2 of 5 for the possession (unordered slot). |
| `off_player_3` | Int64 | stats.wnba.com identifier of offensive on-court player 3 of 5 for the possession (unordered slot). |
| `off_player_4` | Int64 | stats.wnba.com identifier of offensive on-court player 4 of 5 for the possession (unordered slot). |
| `off_player_5` | Int64 | stats.wnba.com identifier of offensive on-court player 5 of 5 for the possession (unordered slot). |
| `def_player_1` | Int64 | stats.wnba.com identifier of defensive on-court player 1 of 5 for the possession (unordered slot). |
| `def_player_2` | Int64 | stats.wnba.com identifier of defensive on-court player 2 of 5 for the possession (unordered slot). |
| `def_player_3` | Int64 | stats.wnba.com identifier of defensive on-court player 3 of 5 for the possession (unordered slot). |
| `def_player_4` | Int64 | stats.wnba.com identifier of defensive on-court player 4 of 5 for the possession (unordered slot). |
| `def_player_5` | Int64 | stats.wnba.com identifier of defensive on-court player 5 of 5 for the possession (unordered slot). |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_wnba_stats_possessions(seasons=2025)
```

## load_wnba_stats_game_lineups

Release: [wnba_stats_game_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_game_lineups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_game_lineups/wnba_lineups_{season}.parquet`
### Returns {#load_wnba_stats_game_lineups-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `home_player_1` | Int64 | stats.wnba.com identifier of home on-court player 1 of 5 for the lineup stint. |
| `home_player_2` | Int64 | stats.wnba.com identifier of home on-court player 2 of 5 for the lineup stint. |
| `home_player_3` | Int64 | stats.wnba.com identifier of home on-court player 3 of 5 for the lineup stint. |
| `home_player_4` | Int64 | stats.wnba.com identifier of home on-court player 4 of 5 for the lineup stint. |
| `home_player_5` | Int64 | stats.wnba.com identifier of home on-court player 5 of 5 for the lineup stint. |
| `away_player_1` | Int64 | stats.wnba.com identifier of away on-court player 1 of 5 for the lineup stint. |
| `away_player_2` | Int64 | stats.wnba.com identifier of away on-court player 2 of 5 for the lineup stint. |
| `away_player_3` | Int64 | stats.wnba.com identifier of away on-court player 3 of 5 for the lineup stint. |
| `away_player_4` | Int64 | stats.wnba.com identifier of away on-court player 4 of 5 for the lineup stint. |
| `away_player_5` | Int64 | stats.wnba.com identifier of away on-court player 5 of 5 for the lineup stint. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_wnba_stats_game_lineups(seasons=2025)
```

## load_wnba_stats_player_boxscores

Release: [wnba_stats_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_player_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_player_boxscores/player_boxscores_{season}.parquet`
### Returns {#load_wnba_stats_player_boxscores-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `side` | String | Side label (e.g. 'home', 'away', or 'overUnder'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `first_name` | String | Player's first name. |
| `family_name` | String | Player's family / last name. |
| `name_i` | String | Initialed name (e.g. 'A. Wilson'). |
| `player_slug` | String | URL-safe player identifier. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `field_goals_made` | Int64 | Field goals made (2-pt + 3-pt). |
| `field_goals_attempted` | Int64 | Field goal attempts (2-pt + 3-pt). |
| `field_goals_percentage` | Float64 | Field goal percentage (0-1 decimal). |
| `three_pointers_made` | Int64 | Three-point field goals made. |
| `three_pointers_attempted` | Int64 | Three-point field goal attempts. |
| `three_pointers_percentage` | Float64 | Three-point field goal percentage (0-1 decimal). |
| `free_throws_made` | Int64 | Free throws made. |
| `free_throws_attempted` | Int64 | Free throw attempts. |
| `free_throws_percentage` | Float64 | Free throw percentage (0-1 decimal). |
| `rebounds_offensive` | Int64 | Offensive rebounds. |
| `rebounds_defensive` | Int64 | Defensive rebounds. |
| `rebounds_total` | Int64 | Total rebounds. |
| `assists` | Int64 | Total assists. |
| `steals` | Int64 | Total steals. |
| `blocks` | Int64 | Total blocks. |
| `turnovers` | Int64 | Total turnovers. |
| `fouls_personal` | Int64 | Personal fouls. |
| `points` | Int64 | Points scored. |
| `plus_minus_points` | Float64 | Plus/minus point differential while on court. |
| `game_id` | String | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_wnba_stats_player_boxscores(seasons=2026)
```

## load_wnba_stats_player_game_logs

Release: [wnba_stats_player_game_logs](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_player_game_logs) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_player_game_logs/player_game_logs_{season}.parquet`
### Returns {#load_wnba_stats_player_game_logs-returns}

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `wl` | String | Wl. |
| `min` | Int64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goals attempted. |
| `fg_pct` | Float64 | Field-goal percentage. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fg3a` | Int64 | Three-point field goals attempted. |
| `fg3_pct` | Float64 | Three-point percentage. |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throws attempted. |
| `ft_pct` | Float64 | Free-throw percentage. |
| `oreb` | Int64 | Offensive rebounds collected. |
| `dreb` | Int64 | Defensive rebounds collected. |
| `reb` | Int64 | Total rebounds collected. |
| `ast` | Int64 | Assists credited. |
| `stl` | Int64 | Steals recorded. |
| `blk` | Int64 | Total shots blocked. |
| `tov` | Int64 | Turnovers committed. |
| `pf` | Int64 | Personal fouls committed. |
| `pts` | Int64 | Total points scored. |
| `plus_minus` | Int64 | Plus-minus point differential. |
| `video_available` | Int64 | Video available. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fantasy_pts` | Float64 | Fantasy points. |
| `measure_type` | String | Stats API measure-type slice the row was pulled from (e.g. 'Base'). |

```python
load_wnba_stats_player_game_logs(seasons=2025)
```

## load_wnba_stats_rosters

Release: [wnba_stats_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_rosters/rosters_{season}.parquet`
### Returns {#load_wnba_stats_rosters-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `player` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `player_slug` | String | URL-safe player identifier. |
| `num` | String | Jersey number worn by the player. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | String | Player weight in pounds. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `age` | Float64 | Player age (in years). |
| `exp` | String | Years of WNBA playing experience entering the season ('R' = rookie). |
| `school` | String | Player's school / college (when distinct from 'college'). |
| `player_id` | Int64 | Unique player identifier. |
| `how_acquired` | String | How the team acquired the player (draft, trade, free agency). |
| `supplemental_status` | Int64 | Numeric supplemental roster-status code from the stats.wnba.com roster feed. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

```python
load_wnba_stats_rosters(seasons=2026)
```

## load_wnba_stats_schedules

Release: [wnba_stats_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_schedules/wnba_schedule_{season}.parquet`
### Returns {#load_wnba_stats_schedules-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `home_team_abbreviation` | String | Home team abbreviation. |
| `home_team_name` | String | Home team name. |
| `home_pts` | Int64 | Final points scored by the home team. |
| `home_wl` | String | Result for the home team ('W' or 'L'); null before the game is final. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `away_team_abbreviation` | String | Away team abbreviation. |
| `away_team_name` | String | Away team name. |
| `away_pts` | Int64 | Final points scored by the away team. |
| `away_wl` | String | Result for the away team ('W' or 'L'); null before the game is final. |

```python
load_wnba_stats_schedules(seasons=2025)
```

## load_wnba_stats_shots

Release: [wnba_stats_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_shots) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_shots/shots_{season}.parquet`
### Returns {#load_wnba_stats_shots-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | String | Game clock value. |
| `team_id` | Int64 | Unique team identifier. |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `shot_distance` | Int64 | Shot distance from the basket, in feet. |
| `x_legacy` | Int64 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int64 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `description` | String | Long-form description text. |
| `score_home` | String | Score home. |
| `score_away` | String | Score away. |

```python
load_wnba_stats_shots(seasons=2026)
```

## load_wnba_stats_team_boxscores

Release: [wnba_stats_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_team_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_team_boxscores/team_boxscores_{season}.parquet`
### Returns {#load_wnba_stats_team_boxscores-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `side` | String | Side label (e.g. 'home', 'away', or 'overUnder'). |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `field_goals_made` | Int64 | Field goals made (2-pt + 3-pt). |
| `field_goals_attempted` | Int64 | Field goal attempts (2-pt + 3-pt). |
| `field_goals_percentage` | Float64 | Field goal percentage (0-1 decimal). |
| `three_pointers_made` | Int64 | Three-point field goals made. |
| `three_pointers_attempted` | Int64 | Three-point field goal attempts. |
| `three_pointers_percentage` | Float64 | Three-point field goal percentage (0-1 decimal). |
| `free_throws_made` | Int64 | Free throws made. |
| `free_throws_attempted` | Int64 | Free throw attempts. |
| `free_throws_percentage` | Float64 | Free throw percentage (0-1 decimal). |
| `rebounds_offensive` | Int64 | Offensive rebounds. |
| `rebounds_defensive` | Int64 | Defensive rebounds. |
| `rebounds_total` | Int64 | Total rebounds. |
| `assists` | Int64 | Total assists. |
| `steals` | Int64 | Total steals. |
| `blocks` | Int64 | Total blocks. |
| `turnovers` | Int64 | Total turnovers. |
| `fouls_personal` | Int64 | Personal fouls. |
| `points` | Int64 | Points scored. |
| `plus_minus_points` | Float64 | Plus/minus point differential while on court. |
| `game_id` | String | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |

```python
load_wnba_stats_team_boxscores(seasons=2026)
```
