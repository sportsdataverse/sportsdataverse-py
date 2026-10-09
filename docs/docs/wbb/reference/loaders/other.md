---
title: "WBB dataset loaders — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "WBB dataset loaders — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WBB dataset loaders — Other

## load_wbb_pbp

Release: [espn_womens_college_basketball_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_pbp/play_by_play_{season}.parquet`
### Returns {#load_wbb_pbp-returns}

| col_name | type | description |
|---|---|---|
| `game_play_number` | Int32 | Game play number |
| `id` | Int64 | Unique play identification number |
| `sequence_number` | Int32 | Sequence number representing a shot-possession (V3 PBP). |
| `type_id` | Int32 | Type identifier (numeric). |
| `type_text` | String | Play type text, passed through verbatim from ESPN. ESPN labels the free-throw play type "MadeFreeThrow" for made AND missed free throws; filter makes vs. misses with scoring_play, not type_text. |
| `text` | String | Text description of the play / record. |
| `away_score` | Int32 | Away team score at the time of the play. |
| `home_score` | Int32 | Home team score at the time of the play. |
| `period_number` | Int32 | Numeric period (1-4 for quarters; 5+ for OT). |
| `period_display_value` | String | Period display label (e.g. '1st Quarter', 'OT'). |
| `clock_display_value` | String | Game clock display string (e.g. '8:32'). |
| `scoring_play` | Boolean | TRUE if the play resulted in points scored. |
| `score_value` | Int32 | Point value of the attempt (1 / 2 / 3), carried even on misses (a missed free throw still shows 1); use scoring_play to identify points actually scored. |
| `wallclock` | String | Wallclock. |
| `shooting_play` | Boolean | TRUE if the play was a shooting attempt. |
| `coordinate_x_raw` | Float64 | X coordinate as returned by the API before any adjustment. |
| `coordinate_y_raw` | Float64 | Y coordinate as returned by the API before any adjustment. |
| `points_attempted` | Int32 | Point value at stake on the shot attempt (3 for threes, 2 for other field goals, 1 for free throws), from the ESPN play type. |
| `short_description` | String | Shortened version of ESPN's play description text, without score context. |
| `team_id` | Int32 | Unique team identifier. |
| `athlete_id_1` | Int32 | Primary athlete identifier (e.g. shooter). |
| `athlete_id_2` | Int32 | Secondary athlete identifier (e.g. assister / fouler). |
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `home_team_id` | Int32 | Unique identifier for the home team. |
| `home_team_name` | String | Home team name. |
| `home_team_mascot` | String | Home team mascot. |
| `home_team_abbrev` | String | Home team three-letter abbreviation. |
| `home_team_name_alt` | String | Alternate versions of the home team abbreviation |
| `away_team_id` | Int32 | Unique identifier for the away team. |
| `away_team_name` | String | Away team name. |
| `away_team_mascot` | String | Away team mascot. |
| `away_team_abbrev` | String | Away team three-letter abbreviation. |
| `away_team_name_alt` | String | Alternate versions of the away team abbreviation |
| `game_spread` | Float64 | Game spread in (-X Team) format. There are almost none, I would recommend not trusting any of these three columns |
| `home_favorite` | Boolean | Logical (TRUE/FALSE) indicating whether the home team is favored |
| `game_spread_available` | Boolean | Logical (TRUE/FALSE) indicating whether the spread was available from ESPN. Basically, I would just not recommend using any of the spread information, I think I defaulted a lot of them to -2.5 for the home team. Most games probably do not have spread information. This column should really be listed first |
| `home_team_spread` | Float64 | The game spread with respect to the home team |
| `qtr` | Int32 | Quarter of the game |
| `time` | String | Time left within the period |
| `clock_minutes` | Int32 | Clock minutes split from seconds for developer convenience |
| `clock_seconds` | Int32 | Clock seconds split from minutes for developer convenience |
| `home_timeout_called` | Boolean | Flag set on plays where a timeout was charged to the home team. |
| `away_timeout_called` | Boolean | Flag set on plays where a timeout was charged to the away team. |
| `half` | Int32 | Half of the game |
| `game_half` | Int32 | Half of the game |
| `lag_qtr` | Int32 | A lag column on the quarter |
| `lead_qtr` | Int32 | A lead column on the quarter |
| `lag_half` | Int32 | A lag column on the half |
| `lead_half` | Int32 | A lead column on the half |
| `start_quarter_seconds_remaining` | Int32 | Quarter seconds remaining at the start of the play (these are more or less code artifacts from other sports, but may eventually be used more seriously) |
| `start_half_seconds_remaining` | Int32 | Game half seconds remaining at the start of the play (these are more or less code artifacts from other sports, but may eventually be used more seriously) |
| `start_game_seconds_remaining` | Int32 | Game seconds remaining at the start of the play (''') |
| `end_quarter_seconds_remaining` | Int32 | Quarter seconds remaining at the end of the play (''') |
| `end_half_seconds_remaining` | Int32 | Game half seconds remaining at the end of the play (''') |
| `end_game_seconds_remaining` | Int32 | Game seconds remaining at the end of the play (''') |
| `period` | Int32 | Period of the game (1-4 quarters; 5+ for OT). |
| `coordinate_x` | Float64 | X coordinate on the court (half-court layout). |
| `coordinate_y` | Float64 | Y coordinate on the court (half-court layout). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `espn_home_wp` | Float64 | ESPN's own published win probability for the home team (0-1), carried through unchanged. |
| `espn_tie_percentage` | Float64 | ESPN's published tie probability; present in the feed but always 0.0 in the published asset. |
| `espn_away_wp` | Float64 | ESPN's own published win probability for the away team (0-1), carried through unchanged. |
| `athlete_name_1` | String | Display name of the first athlete in the ESPN play participants (e.g., the shooter on a shot attempt). |
| `athlete_name_2` | String | Display name of the second athlete in the ESPN play participants (e.g., the assisting player), when present. |
| `athlete_name_3` | String | Display name of the third athlete in the ESPN play participants, when present. |
| `pregame_home_prob` | Float64 | Model's pre-game win probability for the home team (0-1), constant within a game. Present only on assets published with the WP model (2012-13 onward); absent on earlier seasons (rules/wbb.yaml#wbb-2013-pbp-wp-columns). |
| `home_win_prob` | Float64 |  |

```python
load_wbb_pbp(seasons=2024)
```

## load_wbb_schedule

Release: [espn_womens_college_basketball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_schedules/wbb_schedule_{season}.parquet`
### Returns {#load_wbb_schedule-returns}

| col_name | type | description |
|---|---|---|
| `id` | Int32 | Unique play identification number |
| `uid` | String | ESPN UID string. |
| `date` | String | Date in YYYY-MM-DD format. |
| `attendance` | Float64 | Reported attendance. |
| `time_valid` | Boolean | Time valid. |
| `neutral_site` | Boolean | Neutral site. |
| `conference_competition` | Boolean | Conference competition. |
| `play_by_play_available` | Boolean |  |
| `recent` | Boolean | Recent. |
| `start_date` | String | Start date (YYYY-MM-DD). |
| `broadcast` | String | Broadcast information string. |
| `highlights` | String |  |
| `notes_type` | String | Notes type. |
| `notes_headline` | String | Notes headline. |
| `broadcast_market` | String | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | String | Broadcast name. |
| `type_id` | Int32 | Type identifier (numeric). |
| `type_abbreviation` | String | Play type abbreviation |
| `venue_id` | Int32 | Unique venue identifier. |
| `venue_full_name` | String | Venue full name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state / region. |
| `venue_indoor` | Boolean | TRUE if the venue is indoors. |
| `status_clock` | Float64 | Status clock. |
| `status_display_clock` | String | Status display clock. |
| `status_period` | Float64 | Status period. |
| `status_type_id` | Int32 | Unique identifier for status type. |
| `status_type_name` | String | Status type name. |
| `status_type_state` | String | Status type state. |
| `status_type_completed` | Boolean | Status type completed. |
| `status_type_description` | String | Status type description. |
| `status_type_detail` | String | Status type detail. |
| `status_type_short_detail` | String | Status type short detail. |
| `format_regulation_periods` | Float64 | Format regulation periods. |
| `home_id` | Int32 | Unique identifier for home. |
| `home_uid` | String | Home team's uid. |
| `home_location` | String | Home team's location. |
| `home_name` | String | Home name. |
| `home_abbreviation` | String | Home team's abbreviation. |
| `home_display_name` | String | Home display name. |
| `home_short_display_name` | String | Home short display name. |
| `home_color` | String | Color code (hex) for home. |
| `home_alternate_color` | String | Color code (hex) for home alternate. |
| `home_is_active` | Boolean | Home team's is active. |
| `home_venue_id` | Int32 | Unique identifier for home venue. |
| `home_logo` | String | Home team logo URL. |
| `home_conference_id` | Int32 | Unique identifier for home conference. |
| `home_score` | Int32 | Home team score at the time of the play. |
| `home_winner` | Boolean | Home team's winner. |
| `home_current_rank` | Float64 | Poll ranking ESPN listed for the home team at game time (unranked teams carry a sentinel value). |
| `home_linescores` | String | Period-by-period scores for the home team as a delimited string from ESPN's schedule feed. |
| `home_records` | String | Record strings (overall and split records) for the home team from ESPN's schedule feed. |
| `away_id` | Int32 | Unique identifier for away. |
| `away_uid` | String | Away team's uid. |
| `away_location` | String | Away team's location. |
| `away_name` | String | Away name. |
| `away_abbreviation` | String | Away team's abbreviation. |
| `away_display_name` | String | Away display name. |
| `away_short_display_name` | String | Away short display name. |
| `away_color` | String | Color code (hex) for away. |
| `away_alternate_color` | String | Color code (hex) for away alternate. |
| `away_is_active` | Boolean | Away team's is active. |
| `away_venue_id` | Int32 | Unique identifier for away venue. |
| `away_logo` | String | Away team logo URL. |
| `away_conference_id` | Int32 | Unique identifier for away conference. |
| `away_score` | Int32 | Away team score at the time of the play. |
| `away_winner` | Boolean | Away team's winner. |
| `away_current_rank` | Float64 | Poll ranking ESPN listed for the away team at game time (unranked teams carry a sentinel value). |
| `away_linescores` | String | Period-by-period scores for the away team as a delimited string from ESPN's schedule feed. |
| `away_records` | String | Record strings (overall and split records) for the away team from ESPN's schedule feed. |
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `status_type_alt_detail` | String | Status type alt detail. |
| `tournament_id` | Int32 | ESPN tournament identifier. |
| `groups_id` | Int32 | Unique identifier for groups. |
| `groups_name` | String | Groups name. |
| `groups_short_name` | String | Groups short name. |
| `groups_is_conference` | Boolean | Groups is conference. |
| `game_json` | Boolean |  |
| `game_json_url` | String |  |
| `has_game_json` | Boolean | Whether the raw game JSON payload exists in the raw store for this game. |
| `game_json_raw_url` | String | raw.githubusercontent.com URL of the game's raw JSON payload in wehoop-wbb-raw. |
| `game_rosters_json_url` | String | raw.githubusercontent.com URL of the game's rosters JSON payload in wehoop-wbb-raw. |
| `has_game_rosters_json` | Boolean | Whether the rosters JSON payload exists in the raw store for this game. |
| `officials_json_url` | String | raw.githubusercontent.com URL of the game's officials JSON payload in wehoop-wbb-raw. |
| `has_officials_json` | Boolean | Whether the officials JSON payload exists in the raw store for this game. |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `PBP` | Boolean |  |
| `team_box` | Boolean | Team box. |
| `player_box` | Boolean | Player box. |
| `in_shots` | Boolean | Whether the game is represented in the built shots dataset. |
| `in_game_rosters` | Boolean | Whether the game is represented in the built game-rosters dataset. |
| `in_officials` | Boolean | Whether the game is represented in the built officials dataset. |

```python
load_wbb_schedule(seasons=2024)
```

## load_wbb_team_boxscore

Release: [espn_womens_college_basketball_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_team_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_team_boxscores/team_box_{season}.parquet`
### Returns {#load_wbb_team_boxscore-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
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
| `lead_changes` | String | Lead changes. |
| `lead_percentage` | String | Share of game time the team held the lead, as reported in ESPN's team boxscore. |

```python
load_wbb_team_boxscore(seasons=2024)
```

## load_wbb_ratings

Release: [wbb_ratings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_ratings) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_ratings/wbb_ratings_{season}.parquet`
### Returns {#load_wbb_ratings-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `team_id` | String | Unique team identifier. |
| `adj_o` | Float64 | Adj o. |
| `adj_d` | Float64 | Adj d. |
| `adj_em` | Float64 | Adj em. |
| `adj_tempo` | Float64 | Opponent-adjusted tempo in possessions per 40 minutes, produced by the same fixed-point adjustment as the efficiencies applied to game possessions under the additive model poss = tempo_i + tempo_j minus the league baseline. |
| `raw_o` | Float64 | Raw o. |
| `raw_d` | Float64 | Raw d. |
| `games` | Int64 | Games played. |
| `rank` | Int64 | Rank. |
| `adj_em_z` | Float64 | Within-season z-score of adj_em, computed as adj_em minus the season mean divided by the season standard deviation over every team in the frame, so it is mean 0 and standard deviation 1 per season. |

```python
load_wbb_ratings(seasons=2025)
```

## load_wbb_game_rosters

Release: [espn_womens_college_basketball_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_game_rosters/game_rosters_{season}.parquet`
### Returns {#load_wbb_game_rosters-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `game_id` | Int32 | Unique game identifier. |
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
| `athlete_first_name` | String |  |
| `athlete_last_name` | String |  |
| `athlete_jersey` | String | Athlete jersey number. |
| `athlete_position` | String | Athlete position. |
| `athlete_headshot` | String | URL of the player's ESPN headshot image on a.espncdn.com, whose filename is the athlete_id; null when ESPN publishes no headshot for that player. |
| `starter` | Boolean | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `did_not_play` | Boolean | TRUE if the player did not appear in the game. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `ejected` | Boolean | TRUE if the player was ejected from the game. |
| `reason` | String | Reason. |

```python
load_wbb_game_rosters(seasons=2026)
```

## load_wbb_officials

Release: [espn_womens_college_basketball_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_officials) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_officials/officials_{season}.parquet`
### Returns {#load_wbb_officials-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `game_id` | Int32 | Unique game identifier. |
| `official_id` | Int32 | Unique official / referee identifier. |
| `official_uid` | String | ESPN's globally unique resource identifier for the official, read from the core-api items[] uid key; that payload never ships it, so the column is null for every published row. |
| `official_full_name` | String | ESPN's fullName for the official, falling back to displayName when fullName is absent; ESPN sometimes ships it with a middle initial or a doubled internal space, so it is not simply first plus last name. |
| `official_display_name` | String | ESPN's displayName for the official, which is identical to official_full_name in every published row of the released data. |
| `official_first_name` | String | The official's given name as ESPN splits it out separately from the full name, excluding any middle initial that appears in official_full_name. |
| `official_last_name` | String | The official's surname as ESPN splits it out; joined to official_first_name it reconstructs roughly 98 percent of official_full_name values, the rest differing by middle initials or spacing. |
| `official_order` | Int32 | ESPN's 1-based sequence of the official within that game's crew listing, unique within a game and running 1 to 3 for the standard three-person crew. |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | String | Position display name. |

```python
load_wbb_officials(seasons=2026)
```

## load_wbb_rosters

Release: [espn_womens_college_basketball_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_rosters/rosters_{season}.parquet`
### Returns {#load_wbb_rosters-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
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
load_wbb_rosters(seasons=2026)
```

## load_wbb_shots

Release: [espn_womens_college_basketball_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_shots) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_shots/shots_{season}.parquet`
### Returns {#load_wbb_shots-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
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
| `athlete_name_1` | String | Display name of the shooter credited on the attempt in ESPN's play participants. |
| `athlete_name_2` | String | Display name of the second athlete tied to the attempt (typically the assister), when present. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_mascot` | String | Mascot/nickname of the shooting team from ESPN's team record. |
| `team_abbrev` | String | Abbreviation for team. |

```python
load_wbb_shots(seasons=2026)
```

## load_wbb_standings

Release: [espn_womens_college_basketball_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_standings) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_standings/standings_{season}.parquet`
### Returns {#load_wbb_standings-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `group_id` | Int32 | ESPN group id. |
| `group_name` | String |  |
| `group_abbreviation` | String |  |
| `group_short_name` | String | Short display name of the conference or division grouping the row belongs to. |
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
| `stat_short_display_name` | String |  |
| `stat_description` | String | ESPN's longer wording for the standings stat, for example Overall Record for the Team Season Record entry and Current Streak for Streak; null for stats ESPN ships without one, such as vs AP Top 25. |
| `stat_abbreviation` | String | ESPN's abbreviation for the standings stat, such as GB, OPP PPG or VS CONF; always populated and matching stat_short_display_name for about 90 percent of rows. |
| `stat_type` | String | Stat type code (e.g. "win", "loss"). |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_wbb_standings(seasons=2026)
```

## load_wbb_team_season_stats

Release: [espn_womens_college_basketball_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_team_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_team_season_stats/team_season_stats_{season}.parquet`
### Returns {#load_wbb_team_season_stats-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `team_id` | Int32 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |
| `category` | String | Category label. |
| `stat_label` | String |  |
| `stat_name` | String | Internal stat key. |
| `stat_display_name` | String | Stat display name. |
| `stat_description` | String | ESPN's prose definition of the team statistic named in stat_name, for example The average blocks per game for avgBlocks or the full sentence defining a blocked shot for blocks. |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_wbb_team_season_stats(seasons=2026)
```

## load_wbb_schedule_crosswalk

Release: [wbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_crosswalk/wbb_schedule_crosswalk_{season}.parquet`
### Returns {#load_wbb_schedule_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `home_espn_team_id` | Int32 | ESPN home team id (NA for bart-only rows). |
| `away_espn_team_id` | Int32 | ESPN away team id (NA for bart-only rows). |
| `espn_game_id` | String | ESPN game id (NA for bart-only rows). |
| `bart_muid` | String | Torvik muid (NA for espn-only rows). |
| `bart_team1` | String | Torvik team1 name (NA for espn-only rows). |
| `bart_team2` | String | Torvik team2 name (NA for espn-only rows). |
| `bart_winner` | String | Torvik winner name (NA for espn-only rows). |
| `fox_game_id` | String | Fox game id (NA placeholder). |
| `yahoo_game_id` | String | Yahoo game id (NA placeholder). |
| `match_method` | String | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | Float64 | Jaro-Winkler score or 1 for exact (NA if none). |

```python
load_wbb_schedule_crosswalk(seasons=2026)
```

## load_wbb_team_crosswalk

Release: [wbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_crosswalk/wbb_team_crosswalk_{season}.parquet`
### Returns {#load_wbb_team_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `espn_team_id` | Int32 | ESPN team id (canonical key). |
| `espn_abbreviation` | String | ESPN abbreviation. |
| `espn_display_name` | String | ESPN display name (school + mascot). |
| `espn_short_name` | String | ESPN short name. |
| `espn_location` | String | ESPN school/location only. |
| `espn_mascot` | String | ESPN team mascot/nickname. |
| `espn_conference` | String | ESPN conference name. |
| `fox_team_id` | String | Fox Bifrost team id (NA if unmatched). |
| `fox_team_name` | String | Fox team name (NA if unmatched). |
| `fox_section` | String | Fox conference/section label (NA if unmatched). |
| `bart_team` | String | Torvik team name (NA if unmatched). |
| `bart_conf` | String | Torvik conference abbreviation (NA if unmatched). |
| `yahoo_team_id` | String | Yahoo team id (NA placeholder). |
| `yahoo_team_name` | String | Yahoo team name (NA placeholder). |
| `fox_match_confidence` | Float64 | 1 for matched, NA for unmatched. |
| `bart_match_confidence` | Float64 | 1 for matched, NA for unmatched. |
| `match_method` | String | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |

```python
load_wbb_team_crosswalk(seasons=2026)
```

## load_wbb_groups

Release: [wbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_groups/wbb_groups.parquet`

:::caution[Coverage]
One season-less file: one row per group lineage (the league, subdivisions, conferences, divisions) with the first and last season it had members. group_id is SDV's own id (e.g. wbb:big-east) and names a lineage: a rename that keeps continuity keeps the id, a new body gets a new one, and notes records each call. Seasons are the ENDING year (2025 = the 2024-25 season).
:::

### Returns {#load_wbb_groups-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("wbb"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `first_season` | Int32 | First season in which the group had at least one member (ENDING year: 2025 = the 2024-25 season). |
| `last_season` | Int32 | Last season in which the group had at least one member (ENDING year: 2025 = the 2024-25 season). |
| `notes` | String | Builder notes on the group: the lineage decisions behind its group_id and any source caveats. |

```python
load_wbb_groups()
```

## load_wbb_group_seasons

Release: [wbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_groups/wbb_group_seasons.parquet`

:::caution[Coverage]
One season-less file: one row per group per season it existed, with its name, short name, abbreviation and parent group AS OF that season (never today's label applied to the past) and its member count. season is the ENDING year (2025 = the 2024-25 season).
:::

### Returns {#load_wbb_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("wbb"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `season` | Int32 | Season the row describes (ENDING year: 2025 = the 2024-25 season). |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `name` | String | Full name of the group as of that season -- the label in use then, not today's name. |
| `short_name` | String | Short display name of the group as of that season. |
| `abbreviation` | String | Abbreviation of the group as of that season. |
| `parent_group_id` | String | group_id one level up as of that season (division -> conference -> subdivision -> league); null at the top level or where no higher group applied that season. |
| `n_teams` | Int32 | Number of member teams in the group that season. |

```python
load_wbb_group_seasons()
```

## load_wbb_group_aliases

Release: [wbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_groups/wbb_group_aliases.parquet`

:::caution[Coverage]
One season-less file: every name, abbreviation, slug and source id that a source (espn, ncaa, sdv) uses for a group, each with the seasons it is valid for (valid_from / valid_to, inclusive; null = unbounded). Match a source's conference or division label here to reach group_id.
:::

### Returns {#load_wbb_group_aliases-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("wbb"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `source` | String | Source that uses this label or id (in this table: espn, ncaa, sdv); "sdv" marks SDV's own labels. |
| `source_id` | String | The source's own id for the group (ESPN group id, NCAA conf_id, CFBD id, MLB division id) when it has one; null otherwise. |
| `name_kind` | String | Kind of label in value: "name", "short_name", "abbreviation", "slug" or "code". |
| `value` | String | The label exactly as the source writes it; match a source's conference or division label against it to reach group_id. |
| `valid_from` | Int32 | First season the alias is valid for, inclusive (ENDING year: 2025 = the 2024-25 season); null = unbounded. |
| `valid_to` | Int32 | Last season the alias is valid for, inclusive (ENDING year: 2025 = the 2024-25 season); null = unbounded (still in use). |

```python
load_wbb_group_aliases()
```

## load_wbb_team_group_seasons

Release: [wbb_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_groups/wbb_team_group_seasons_{season}.parquet`

:::caution[Coverage]
One row per team per season: the SDV subdivision, conference and division group ids the team belonged to that season (null where a level does not apply), the team name as of that season, where the membership came from, and whether a second source agreed (null when only one source covers the season). team_id is a string: the ESPN team id; team_id_source names the id space. season is the ENDING year (2025 = the 2024-25 season); seasons 2002-2027.
:::

### Returns {#load_wbb_team_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("wbb"); the prefix of every group_id in it. |
| `season` | Int32 | Season of the membership (ENDING year: 2025 = the 2024-25 season). |
| `team_id` | String | Team id as a string: the ESPN team id where ESPN covers the team, otherwise the league's own id; team_id_source says which. |
| `team_id_source` | String | Id space of team_id (in this table: espn). |
| `team_name` | String | Team name as of that season, not today's. |
| `subdivision_id` | String | SDV group_id of the team's subdivision that season (e.g. FBS / FCS, Division I); null where the league has no subdivision level. |
| `conference_id` | String | SDV group_id of the team's conference that season; null where the team had no conference (an independent, or a season played without conferences). |
| `division_id` | String | SDV group_id of the team's division that season; null where the level does not apply. |
| `source` | String | Source the membership was taken from -- the most reliable per-season source for that era. |
| `sources_agree` | Boolean | Whether a second source agreed on the membership; null when only one source covers the season. |
| `notes` | String | Builder notes on the team-season, such as a source disagreement or which of several listed memberships was kept. |

```python
load_wbb_team_group_seasons(seasons=2024)
```
