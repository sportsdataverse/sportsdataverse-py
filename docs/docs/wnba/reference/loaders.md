---
title: WNBA dataset loaders
sidebar_label: Loaders
sidebar_position: 1
---
# WNBA dataset loaders

```mermaid
flowchart LR
  raw["scrape / raw"] --> enrich["enrich"] --> rel["release asset"] --> load["load_*()"]
```

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_wnba_pbp` | [espn_wnba_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_pbp) | — |
| `load_wnba_player_boxscore` | [espn_wnba_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_boxscores) | — |
| `load_wnba_schedule` | [espn_wnba_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_schedules) | — |
| `load_wnba_team_boxscore` | [espn_wnba_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_boxscores) | — |
| `load_wnba_draft` | [espn_wnba_draft](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_draft) | — |
| `load_wnba_game_rosters` | [espn_wnba_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_game_rosters) | — |
| `load_wnba_officials` | [espn_wnba_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_officials) | — |
| `load_wnba_player_season_stats` | [espn_wnba_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_season_stats) | — |
| `load_wnba_rosters` | [espn_wnba_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_rosters) | — |
| `load_wnba_shots` | [espn_wnba_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_shots) | — |
| `load_wnba_standings` | [espn_wnba_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_standings) | — |
| `load_wnba_team_season_stats` | [espn_wnba_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_season_stats) | — |
| `load_wnba_stats_coaches` | [wnba_stats_coaches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_coaches) | — |
| `load_wnba_stats_draft` | [wnba_stats_draft](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_draft) | — |
| `load_wnba_stats_game_rosters` | [wnba_stats_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_game_rosters) | — |
| `load_wnba_stats_officials` | [wnba_stats_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_officials) | — |
| `load_wnba_stats_pbp` | [wnba_stats_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_pbp) | — |
| `load_wnba_stats_player_game_logs` | [wnba_stats_player_game_logs](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_player_game_logs) | — |
| `load_wnba_stats_rosters` | [wnba_stats_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_rosters) | — |
| `load_wnba_stats_schedules` | [wnba_stats_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_schedules) | — |
| `load_wnba_stats_shots` | [wnba_stats_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_shots) | — |

## `load_wnba_pbp`

Release: [espn_wnba_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_pbp/play_by_play_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `id` | Float64 | Unique play identifcation number |
| `sequence_number` | String | Sequence number representing a shot-possession (V3 PBP). |
| `type_id` | Int32 | Type identifier (numeric). |
| `type_text` | String | Display text for the type field. |
| `text` | String | Text description of the play / record. |
| `away_score` | Int32 | Away team score at the time of the play. |
| `home_score` | Int32 | Home team score at the time of the play. |
| `period_number` | Int32 | Numeric period (1-4 for quarters; 5+ for OT). |
| `period_display_value` | String | Period display label (e.g. '1st Quarter', 'OT'). |
| `clock_display_value` | String | Game clock display string (e.g. '8:32'). |
| `scoring_play` | Boolean | TRUE if the play resulted in points scored. |
| `score_value` | Int32 | Point value of the play (2 / 3 / 1). |
| `shooting_play` | Boolean | TRUE if the play was a shooting attempt. |
| `coordinate_x_raw` | Float64 | X coordinate as returned by the API before any adjustment. |
| `coordinate_y_raw` | Float64 | Y coordinate as returned by the API before any adjustment. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `away_team_id` | Int32 | Unique identifier for the away team. |
| `away_team_name` | String | Away team name. |
| `away_team_mascot` | String | Away team mascot. |
| `away_team_abbrev` | String | Away team three-letter abbreviation. |
| `away_team_name_alt` | String | Alternate versions of the away team abbreviation |
| `home_team_id` | Int32 | Unique identifier for the home team. |
| `home_team_name` | String | Home team name. |
| `home_team_mascot` | String | Home team mascot. |
| `home_team_abbrev` | String | Home team three-letter abbreviation. |
| `home_team_name_alt` | String | Alternate versions of the home team abbreviation |
| `home_team_spread` | Float64 | The game spread with respect to the home team |
| `game_spread` | Float64 | Game spread in (-X Team) format. There are almost none, I would recommend not trusting any of these three columns |
| `home_favorite` | Boolean | Logical (TRUE/FALSE) indicating whether the home team is favored |
| `game_spread_available` | Boolean | Logical (TRUE/FALSE) indicating whether the spread was available from ESPN. Basically, I would just not recommend using any of the spread information, I think I defaulted a lot of them to -2.5 for the home team. Most games probably do not have spread information. This column should really be listed first |
| `game_id` | Int32 | Unique game identifier. |
| `qtr` | Int32 | Quarter of the game |
| `time` | String | Time left within the period |
| `clock_minutes` | Int32 | Clock minutes split from seconds for developer convenience |
| `clock_seconds` | Float64 | Clock seconds split from minutes for developer convenience |
| `half` | String | Half of the game |
| `game_half` | String | Half of the game |
| `lead_qtr` | Int32 | A lead column on the quarter |
| `lead_game_half` | String | A lead column on the half |
| `start_quarter_seconds_remaining` | Int32 | Quarter seconds remaining at the start of the play (these are more or less code artifacts from other sports, but may eventually be used more seriously) |
| `start_half_seconds_remaining` | Int32 | Game half seconds remaining at the start of the play (these are more or less code artifacts from other sports, but may eventually be used more seriously) |
| `start_game_seconds_remaining` | Int32 | Game seconds remaining at the start of the play (''') |
| `game_play_number` | Int32 | Game play number |
| `end_quarter_seconds_remaining` | Int32 | Quarter seconds remaining at the end of the play (''') |
| `end_half_seconds_remaining` | Int32 | Game half seconds remaining at the end of the play (''') |
| `end_game_seconds_remaining` | Int32 | Game seconds remaining at the end of the play (''') |
| `period` | Int32 | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | Int32 | Unique team identifier. |
| `athlete_id_1` | Int32 | Primary athlete identifier (e.g. shooter). |
| `athlete_id_2` | Int32 | Secondary athlete identifier (e.g. assister / fouler). |
| `athlete_id_3` | Int32 | Athlete id 3. |
| `lag_qtr` | Int32 | A lag column on the quarter |
| `lag_game_half` | String | A lag column on the half |
| `coordinate_x` | Float64 | X coordinate on the court (half-court layout). |
| `coordinate_y` | Float64 | Y coordinate on the court (half-court layout). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `type_abbreviation` | String | Play type abbreviation |

```python
load_wnba_pbp(seasons=2024)
```

## `load_wnba_player_boxscore`

Release: [espn_wnba_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_player_boxscores/player_box_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `athlete_display_name` | String | Athlete display name (full). |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `min` | String | Minutes played. |
| `fg` | String |  |
| `fg3` | String |  |
| `ft` | String |  |
| `oreb` | String | Offensive rebounds. |
| `dreb` | String | Defensive rebounds. |
| `reb` | String | Total rebounds. |
| `ast` | String | Assists. |
| `stl` | String | Steals. |
| `blk` | String | Blocks. |
| `to` | String | Final season played in NFL |
| `pf` | String | Personal fouls. |
| `plus_minus` | String | Plus/minus point differential while on court. |
| `pts` | String | Points scored. |
| `starter` | Boolean | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `ejected` | Boolean | TRUE if the player was ejected from the game. |
| `did_not_play` | Boolean | TRUE if the player did not appear in the game. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `athlete_jersey` | String | Athlete jersey number. |
| `athlete_id` | String | Unique athlete identifier (ESPN). |
| `athlete_short_name` | String | Athlete short display name. |
| `athlete_position_name` | String | Athlete position ('Guard', 'Forward', 'Center'). |
| `athlete_position_abbreviation` | String | Athlete position abbreviation (G / F / C). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_logo` | String | Team logo image URL. |
| `team_id` | String | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `athlete_headshot_href` | String | Athlete headshot image URL. |

```python
load_wnba_player_boxscore(seasons=2024)
```

## `load_wnba_schedule`

Release: [espn_wnba_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_schedules/wnba_schedule_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `id` | Int32 | Unique play identifcation number |
| `uid` | String | ESPN UID string. |
| `date` | String | Date in YYYY-MM-DD format. |
| `attendance` | Float64 | Reported attendance. |
| `time_valid` | Boolean | Whether the start time is confirmed. |
| `neutral_site` | Boolean | Neutral site. |
| `conference_competition` | Boolean | Conference competition. |
| `recent` | Boolean | Whether the game is recent. |
| `start_date` | String | Start date (YYYY-MM-DD). |
| `notes_type` | String | Notes type. |
| `notes_headline` | String | Notes headline. |
| `type_id` | Int32 | Type identifier (numeric). |
| `type_abbreviation` | String | Play type abbreviation |
| `status_clock` | Float64 | Game clock in seconds. |
| `status_display_clock` | String | Status display clock. |
| `status_period` | Float64 | Current period. |
| `status_type_id` | Int32 | Unique identifier for status type. |
| `status_type_name` | String | Status type name. |
| `status_type_state` | String | Status state (pre/in/post). |
| `status_type_completed` | Boolean | Whether the game is complete. |
| `status_type_description` | String | Status type description. |
| `status_type_detail` | String | Status type detail. |
| `status_type_short_detail` | String | Status type short detail. |
| `format_regulation_periods` | Float64 | Format regulation periods. |
| `home_id` | Int32 | Unique identifier for home. |
| `home_uid` | String | Home team's uid. |
| `home_location` | String | Home team's location. |
| `home_name` | String | Home team display name. |
| `home_abbreviation` | String | Home team's abbreviation. |
| `home_display_name` | String | Home team display name. |
| `home_short_display_name` | String | Home short display name. |
| `home_color` | String | Home team primary color hex. |
| `home_alternate_color` | String | Color code (hex) for home alternate. |
| `home_is_active` | Boolean | Home team's is active. |
| `home_venue_id` | Int32 | Unique identifier for home venue. |
| `home_logo` | String | Home team logo URL. |
| `home_score` | Int32 | Home team score at the time of the play. |
| `home_winner` | Boolean | Whether the home team won. |
| `away_id` | Int32 | Unique identifier for away. |
| `away_uid` | String | Away team's uid. |
| `away_location` | String | Away team's location. |
| `away_name` | String | Away team display name. |
| `away_abbreviation` | String | Away team's abbreviation. |
| `away_display_name` | String | Away team display name. |
| `away_short_display_name` | String | Away short display name. |
| `away_is_active` | Boolean | Away team's is active. |
| `away_venue_id` | Int32 | Unique identifier for away venue. |
| `away_score` | Int32 | Away team score at the time of the play. |
| `away_winner` | Boolean | Whether the away team won. |
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `venue_id` | Int32 | Unique venue identifier. |
| `venue_full_name` | String | Venue full name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state / region. |
| `venue_capacity` | Float64 | Venue seating capacity. |
| `venue_indoor` | Boolean | Whether the home venue is indoors. |
| `away_color` | String | Away team primary color hex. |
| `away_alternate_color` | String | Color code (hex) for away alternate. |
| `away_logo` | String | Away team logo URL. |
| `status_type_alt_detail` | String | Status type alt detail. |
| `game_json` | Boolean | Whether processed game JSON is available. |
| `game_json_url` | String | URL to the processed game JSON. |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `PBP` | Boolean | Whether play-by-play data is available. |
| `team_box` | Boolean | Whether team box score data is available. |
| `player_box` | Boolean | Whether player box score data is available. |

```python
load_wnba_schedule(seasons=2024)
```

## `load_wnba_team_boxscore`

Release: [espn_wnba_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_team_boxscores/team_box_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `team_id` | Int32 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |
| `team_home_away` | String | Team home away. |
| `team_score` | Int32 | Team's score / final score. |
| `team_winner` | Boolean | TRUE if the team won this game. |
| `assists` | Int32 | Total assists. |
| `blocks` | Int32 | Total blocks. |
| `defensive_rebounds` | Int32 | Defensive rebounds. |
| `fast_break_points` | String | Fast-break points scored. |
| `field_goal_pct` | Float64 | Field goal percentage (0-1). |
| `field_goals_made` | Int32 | Field goals made (2-pt + 3-pt). |
| `field_goals_attempted` | Int32 | Field goal attempts (2-pt + 3-pt). |
| `flagrant_fouls` | Int32 | Total flagrant fouls. |
| `fouls` | Int32 | Personal fouls. |
| `free_throw_pct` | Float64 | Free throw percentage (0-1). |
| `free_throws_made` | Int32 | Free throws made. |
| `free_throws_attempted` | Int32 | Free throw attempts. |
| `largest_lead` | String | Largest lead during the game. |
| `offensive_rebounds` | Int32 | Offensive rebounds. |
| `points_in_paint` | String | Points scored in the paint. |
| `steals` | Int32 | Total steals. |
| `team_turnovers` | Int32 | Team turnovers (turnovers credited to the team rather than a player). |
| `technical_fouls` | Int32 | Total technical fouls. |
| `three_point_field_goal_pct` | Float64 | Three-point field goal percentage (0-1). |
| `three_point_field_goals_made` | Int32 | Three-point field goals made. |
| `three_point_field_goals_attempted` | Int32 | Three-point field goal attempts. |
| `total_rebounds` | Int32 | Total rebounds. |
| `total_technical_fouls` | Int32 | Total technical fouls (player + team). |
| `total_turnovers` | Int32 | Total turnovers (player + team). |
| `turnover_points` | String | Turnover points. |
| `turnovers` | Int32 | Total turnovers. |
| `opponent_team_id` | Int32 | Unique identifier for the opponent team. |
| `opponent_team_uid` | String | Opponent team uid. |
| `opponent_team_slug` | String | Opponent team slug. |
| `opponent_team_location` | String | Opponent team city / location. |
| `opponent_team_name` | String | Opponent team display name. |
| `opponent_team_abbreviation` | String | Opponent team abbreviation. |
| `opponent_team_display_name` | String | Opponent team full display name. |
| `opponent_team_short_display_name` | String | Opponent team short display name. |
| `opponent_team_color` | String | Opponent team primary color (hex). |
| `opponent_team_alternate_color` | String | Opponent team alternate color (hex). |
| `opponent_team_logo` | String | Opponent team logo URL. |
| `opponent_team_score` | Int32 | Opponent team's score. |

```python
load_wnba_team_boxscore(seasons=2024)
```

## `load_wnba_draft`

Release: [espn_wnba_draft](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_draft) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_draft/draft_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `round` | Int32 | Tournament / playoff round. |
| `round_display_name` | String |  |
| `pick` | Int32 | Pick. |
| `overall_pick` | Int32 | Overall pick. |
| `pick_traded` | String |  |
| `pick_notes` | String |  |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_first_name` | String | Player first name; `athlete_detail = TRUE` only. |
| `athlete_last_name` | String | Athlete last name. |
| `athlete_full_name` | String | Drafted player full name. |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_short_name` | String | Athlete short display name. |
| `athlete_height` | String | Athlete height. |
| `athlete_weight` | String | Athlete weight. |
| `athlete_position_abbreviation` | String | Athlete position abbreviation (G / F / C). |
| `athlete_position_name` | String | Athlete position ('Guard', 'Forward', 'Center'). |
| `athlete_headshot_href` | String | Athlete headshot image URL. |
| `college_id` | Int32 | Unique identifier for college. |
| `college_name` | String | College name. |
| `college_short_name` | String | College short name. |
| `college_abbreviation` | String |  |
| `team_id` | Int32 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |

```python
load_wnba_draft(seasons=2026)
```

## `load_wnba_game_rosters`

Release: [espn_wnba_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_game_rosters/game_rosters_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `game_id` | String | Unique game identifier. |
| `team_id` | Int32 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `home_away` | String | Game venue label ('home' or 'away'). |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_short_name` | String | Athlete short display name. |
| `athlete_first_name` | String | Player first name; `athlete_detail = TRUE` only. |
| `athlete_last_name` | String | Athlete last name. |
| `athlete_jersey` | String | Athlete jersey number. |
| `athlete_position` | String | Athlete position. |
| `athlete_headshot` | String |  |
| `starter` | Boolean | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `did_not_play` | Boolean | TRUE if the player did not appear in the game. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `ejected` | Boolean | TRUE if the player was ejected from the game. |
| `reason` | String | Reason. |

```python
load_wnba_game_rosters(seasons=2024)
```

## `load_wnba_officials`

Release: [espn_wnba_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_officials) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_officials/officials_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `game_id` | String | Unique game identifier. |
| `official_id` | Int32 | Unique official / referee identifier. |
| `official_uid` | String |  |
| `official_full_name` | String |  |
| `official_display_name` | String |  |
| `official_first_name` | String |  |
| `official_last_name` | String |  |
| `official_order` | Int32 |  |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | String | Position display name. |

```python
load_wnba_officials(seasons=2024)
```

## `load_wnba_player_season_stats`

Release: [espn_wnba_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_player_season_stats/player_season_stats_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_first_name` | String | Player first name; `athlete_detail = TRUE` only. |
| `athlete_last_name` | String | Athlete last name. |
| `athlete_position_abbreviation` | String | Athlete position abbreviation (G / F / C). |
| `athlete_jersey` | String | Athlete jersey number. |
| `team_id` | Int32 | Unique team identifier. |
| `team_display_name` | String | Full team display name. |
| `category` | String | Category label. |
| `stat_label` | String | Human-readable label of the statistic (e.g. 'At bats'). |
| `stat_name` | String | Internal stat key. |
| `stat_display_name` | String | Stat display name. |
| `stat_description` | String |  |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_wnba_player_season_stats(seasons=2024)
```

## `load_wnba_rosters`

Release: [espn_wnba_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_rosters/rosters_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `team_id` | Int32 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |
| `athlete_id` | String | Unique athlete identifier (ESPN). |
| `uid` | String | ESPN UID string. |
| `guid` | String | Stable cross-league team GUID. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short display name. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey` | String | Jersey number worn by the player. |
| `position_abbreviation` | String | Position abbreviation ('G' / 'F' / 'C'). |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_id` | String | Unique position identifier. |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | String | Player weight in pounds. |
| `age` | String | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |
| `birth_place_country` | String | Birth place country. |
| `experience_years` | String | Experience years. |
| `experience_display_value` | String | Experience display value. |
| `headshot_href` | String | Headshot image URL. |
| `headshot_alt` | String | Alternative-text label for the headshot. |
| `link_web` | String | Web link / URL. |
| `status_id` | String | Status identifier. |
| `status_name` | String | Status label. |
| `status_type` | String | Status type. |

```python
load_wnba_rosters(seasons=2024)
```

## `load_wnba_shots`

Release: [espn_wnba_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_shots) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_shots/shots_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `period_number` | Int32 | Numeric period (1-4 for quarters; 5+ for OT). |
| `clock_display_value` | String | Game clock display string (e.g. '8:32'). |
| `team_id` | Int32 | Unique team identifier. |
| `athlete_id_1` | Int32 | Primary athlete identifier (e.g. shooter). |
| `athlete_id_2` | Int32 | Secondary athlete identifier (e.g. assister / fouler). |
| `type_id` | Int32 | Type identifier (numeric). |
| `type_text` | String | Display text for the type field. |
| `scoring_play` | Boolean | TRUE if the play resulted in points scored. |
| `score_value` | Int32 | Point value of the play (2 / 3 / 1). |
| `coordinate_x` | Float64 | X coordinate on the court (half-court layout). |
| `coordinate_y` | Float64 | Y coordinate on the court (half-court layout). |
| `coordinate_x_raw` | Float64 | X coordinate as returned by the API before any adjustment. |
| `coordinate_y_raw` | Float64 | Y coordinate as returned by the API before any adjustment. |

```python
load_wnba_shots(seasons=2024)
```

## `load_wnba_standings`

Release: [espn_wnba_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_standings) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_standings/standings_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `group_id` | String | ESPN group id. |
| `group_name` | String | Group name (conference / division). |
| `group_abbreviation` | String | Group abbreviation. |
| `group_short_name` | String |  |
| `team_id` | Int32 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |
| `stat_name` | String | Internal stat key. |
| `stat_display_name` | String | Stat display name. |
| `stat_short_display_name` | String | Short human-readable stat name. |
| `stat_description` | String |  |
| `stat_abbreviation` | String |  |
| `stat_type` | String | Stat type code (e.g. "win", "loss"). |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_wnba_standings(seasons=2024)
```

## `load_wnba_team_season_stats`

Release: [espn_wnba_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_team_season_stats/team_season_stats_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `team_id` | Int32 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |
| `category` | String | Category label. |
| `stat_label` | String | Human-readable label of the statistic (e.g. 'At bats'). |
| `stat_name` | String | Internal stat key. |
| `stat_display_name` | String | Stat display name. |
| `stat_description` | String | Human-readable description of the statistic the row reports. |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_wnba_team_season_stats(seasons=2024)
```

## `load_wnba_stats_coaches`

Release: [wnba_stats_coaches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_coaches) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_coaches/coaches_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `team_id` | String | Unique team identifier. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `coach_id` | String | Unique identifier for coach. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `coach_name` | String |  |
| `is_assistant` | String |  |
| `coach_type` | String |  |
| `sort_sequence` | String |  |
| `sub_sort_sequence` | String |  |
| `season_2` | Int32 |  |
| `team_id_lookup` | Int32 |  |

```python
load_wnba_stats_coaches(seasons=2026)
```

## `load_wnba_stats_draft`

Release: [wnba_stats_draft](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_draft) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_draft/draft_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `person_id` | String | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `round_number` | String | Numeric round. |
| `round_pick` | String | Round pick. |
| `overall_pick` | String | Overall pick. |
| `draft_type` | String |  |
| `team_id` | String | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `organization` | String | Organization. |
| `organization_type` | String | Organization type. |
| `player_profile_flag` | String | Player profile flag. |
| `season_2` | Int32 |  |

```python
load_wnba_stats_draft(seasons=2025)
```

## `load_wnba_stats_game_rosters`

Release: [wnba_stats_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_game_rosters/game_rosters_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_num` | String | Jersey number worn by the player. |
| `team_id` | String | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `game_id` | String | Unique game identifier. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |

```python
load_wnba_stats_game_rosters(seasons=2026)
```

## `load_wnba_stats_officials`

Release: [wnba_stats_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_officials) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_officials/officials_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `official_id` | String | Unique official / referee identifier. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_num` | String | Jersey number worn by the player. |
| `game_id` | String | Unique game identifier. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |

```python
load_wnba_stats_officials(seasons=2026)
```

## `load_wnba_stats_pbp`

Release: [wnba_stats_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_pbp/play_by_play_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `event_num` | String | Sequential event number within the game (V2 PBP). |
| `event_type` | String | Event / play type code (V2 PBP). |
| `event_action_type` | String | Numeric event-action-type code (V2 PBP). |
| `period` | Int32 | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | String | Game clock value. |
| `minute_game` | Float64 | Minute game. |
| `time_remaining` | Float64 | Time remaining. |
| `time_quarter` | String | Time quarter. |
| `minute_remaining_quarter` | Int32 | Minute remaining quarter. |
| `seconds_remaining_quarter` | Int32 | Seconds remaining quarter. |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `neutral_description` | String | Neutral description. |
| `description` | String | Long-form description text. |
| `location` | String | Filter results by game location. |
| `score` | String | Final score. |
| `away_score` | Int32 | Away team score at the time of the play. |
| `home_score` | Int32 | Home team score at the time of the play. |
| `score_margin` | String | Score margin. |
| `person1type` | String | Person1type. |
| `player1_id` | String | V2 PBP primary player ID (e.g. shooter / fouler). |
| `player1_name` | String | V2 PBP primary player name. |
| `player1_team_id` | String | Team ID of player1. |
| `player1_team_abbreviation` | String | Player1 team abbreviation. |
| `video_available_flag` | String | Video available flag. |
| `team_leading` | String | Team leading. |
| `x_legacy` | Int32 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int32 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `shot_distance` | Int32 | Shot distance from the basket, in feet. |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `is_field_goal` | Int32 | 1 if the action was a field goal; 0 otherwise. |
| `points_total` | Int32 | Running total of points scored. |
| `shot_value` | Int32 | Point value of the shot (2 or 3). |
| `action_number` | Int32 | Sequential action number within a game (V3 PBP). |
| `team_id` | Int32 | Unique team identifier. |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | Int32 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `player_name_i` | String | Player name i. |
| `score_home` | String | Score home. |
| `score_away` | String | Score away. |
| `video_available` | Int32 | Video available. |
| `action_id` | Int32 | Unique action identifier within a game (V3 PBP). |
| `away_player1` | Int32 | Away team's player1. |
| `away_player2` | Int32 | Away team's player2. |
| `away_player3` | Int32 | Away team's player3. |
| `away_player4` | Int32 | Away team's player4. |
| `away_player5` | Int32 | Away team's player5. |
| `home_player1` | Int32 | Home team's player1. |
| `home_player2` | Int32 | Home team's player2. |
| `home_player3` | Int32 | Home team's player3. |
| `home_player4` | Int32 | Home team's player4. |
| `home_player5` | Int32 | Home team's player5. |
| `home_description` | String | Home team's description. |
| `player1_team_city` | String | Player1 team city. |
| `player1_team_nickname` | String | Player1 team nickname. |
| `visitor_description` | String | Visitor description. |
| `player2_id` | String | V2 PBP secondary player ID (e.g. assister / fouled-by). |
| `player2_name` | String | V2 PBP secondary player name. |
| `player2_team_id` | String | Team ID of player2. |
| `player2_team_city` | String | Player2 team city. |
| `player2_team_nickname` | String | Player2 team nickname. |
| `player2_team_abbreviation` | String | Player2 team abbreviation. |
| `player3_id` | String | V2 PBP tertiary player ID (e.g. blocker). |
| `player3_name` | String | V2 PBP tertiary player name. |
| `player3_team_id` | String | Team ID of player3. |
| `player3_team_city` | String | Player3 team city. |
| `player3_team_nickname` | String | Player3 team nickname. |
| `player3_team_abbreviation` | String | Player3 team abbreviation. |
| `score_value` | Int32 | Point value of the play (2 / 3 / 1). |
| `msg_type` | Int32 | Message-type code for the play-by-play event. |
| `act_type` | Int32 | Action-type code for the play-by-play event. |
| `slug_team` | String | Slug of the team credited with the event. |
| `shot_pts` | Int32 | Points scored on the shot, if the event was a made field goal. |
| `secs_passed_game` | Float64 | Seconds elapsed in the game at the event. |
| `team_away` | String | Slug of the away team. |
| `team_home` | String | Slug of the home team. |
| `off_slug_team` | String | Slug of the team on offense for the event. |
| `number_event` | Int32 | Sequence number of the event within the game. |
| `possession` | Int32 | Abbreviation of the team currently in possession. |
| `total_starters_home` | Int32 | Number of the home team's starters on the floor for the event. |
| `total_starters_away` | Int32 | Number of the away team's starters on the floor for the event. |
| `garbage_time` | Int32 | TRUE if the play occurred during garbage time. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |

```python
load_wnba_stats_pbp(seasons=2026)
```

## `load_wnba_stats_player_game_logs`

Release: [wnba_stats_player_game_logs](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_player_game_logs) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_player_game_logs/player_game_logs_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |
| `player_id` | Int32 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int32 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `wl` | String | Wl. |
| `min` | String | Minutes played. |
| `fgm` | String | Field goals made. |
| `fga` | String | Field goals attempted. |
| `fg_pct` | String | Field-goal percentage. |
| `fg3m` | String | Three-point field goals made. |
| `fg3a` | String | Three-point field goals attempted. |
| `fg3_pct` | String | Three-point percentage. |
| `ftm` | String | Free throws made. |
| `fta` | String | Free throws attempted. |
| `ft_pct` | String | Free-throw percentage. |
| `oreb` | String | Offensive rebounds collected. |
| `dreb` | String | Defensive rebounds collected. |
| `reb` | String | Total rebounds collected. |
| `ast` | String | Assists credited. |
| `stl` | String | Steals recorded. |
| `blk` | String | Total shots blocked. |
| `tov` | String | Turnovers committed. |
| `pf` | String | Personal fouls committed. |
| `pts` | String | Total points scored. |
| `plus_minus` | String | Plus-minus point differential. |
| `fantasy_pts` | String | Fantasy points. |
| `video_available` | String | Video available. |
| `team_location` | String | Team city or location string. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |

```python
load_wnba_stats_player_game_logs(seasons=2025)
```

## `load_wnba_stats_rosters`

Release: [wnba_stats_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_rosters/rosters_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `team_id` | String | Unique team identifier. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `league_id` | String | League identifier ('10' = WNBA). |
| `player` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `player_slug` | String | URL-safe player identifier. |
| `num` | String | Inning number. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | String | Player weight in pounds. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `age` | String | Player age (in years). |
| `exp` | String | Years of MLB service experience. |
| `school` | String | Player's school / college (when distinct from 'college'). |
| `player_id` | String | Unique player identifier. |
| `how_acquired` | String | How the team acquired the player (draft, trade, free agency). |
| `season_2` | Int32 | Season label in the league's second display form. |
| `team_id_lookup` | Int32 | Team id used to join the row back to the team tables. |

```python
load_wnba_stats_rosters(seasons=2026)
```

## `load_wnba_stats_schedules`

Release: [wnba_stats_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_schedules/wnba_stats_schedule_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `SEASON_ID` | String | Unique season identifier. |
| `TEAM_ID` | String | Unique team identifier. |
| `TEAM_ABBREVIATION` | String | Short team abbreviation (e.g. 'LAS'). |
| `TEAM_NAME` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `GAME_ID` | String | Unique game identifier. |
| `GAME_DATE` | String | Game date (YYYY-MM-DD). |
| `MATCHUP` | String | Matchup. |
| `WL` | String | Wl. |
| `MIN` | String | Minutes played. |
| `PTS` | String | Points scored. |
| `FGM` | String | Field goals made. |
| `FGA` | String | Field goal attempts. |
| `FG_PCT` | String | Field goal percentage (0-1). |
| `FG3M` | String | Three-point field goals made. |
| `FG3A` | String | Three-point field goal attempts. |
| `FG3_PCT` | String | Three-point field goal percentage (0-1). |
| `FTM` | String | Free throws made. |
| `FTA` | String | Free throw attempts. |
| `FT_PCT` | String | Free throw percentage (0-1). |
| `OREB` | String | Offensive rebounds. |
| `DREB` | String | Defensive rebounds. |
| `REB` | String | Total rebounds. |
| `AST` | String | Assists. |
| `STL` | String | Steals. |
| `BLK` | String | Blocks. |
| `TOV` | String | Turnovers. |
| `PF` | String | Personal fouls. |
| `PLUS_MINUS` | String | Plus/minus point differential while on court. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |

```python
load_wnba_stats_schedules(seasons=2025)
```

## `load_wnba_stats_shots`

Release: [wnba_stats_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_stats_shots) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_stats_shots/shots_{season}.parquet`
### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `season` | Int32 | Season identifier (4-digit year or 'YYYY-YY' string). |
| `period` | Int32 | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | String | Game clock value. |
| `team_id` | Int32 | Unique team identifier. |
| `person_id` | Int32 | Unique player identifier (V3 endpoints). |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `description` | String | Long-form description text. |
| `x_legacy` | Int32 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int32 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `shot_distance` | Int32 | Shot distance from the basket, in feet. |
| `shot_value` | Int32 | Point value of the shot (2 or 3). |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `points_total` | Int32 | Running total of points scored. |

```python
load_wnba_stats_shots(seasons=2026)
```
