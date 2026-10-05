---
title: "MBB dataset loaders — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "MBB dataset loaders — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MBB dataset loaders — Player

## load_mbb_player_boxscore

Release: [espn_mens_college_basketball_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_player_boxscores/player_box_{season}.parquet`
### Returns {#load_mbb_player_boxscore-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season year. |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_display_name` | String | Athlete display name (full). |
| `team_id` | Int32 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_location` | String | Team city or location string. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `minutes` | Float64 | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `field_goals_made` | Int32 | Field goals made (2-pt + 3-pt). |
| `field_goals_attempted` | Int32 | Field goal attempts (2-pt + 3-pt). |
| `three_point_field_goals_made` | Int32 | Three-point field goals made. |
| `three_point_field_goals_attempted` | Int32 | Three-point field goal attempts. |
| `free_throws_made` | Int32 | Free throws made. |
| `free_throws_attempted` | Int32 | Free throw attempts. |
| `offensive_rebounds` | Int32 | Offensive rebounds. |
| `defensive_rebounds` | Int32 | Defensive rebounds. |
| `rebounds` | Int32 | Total rebounds. |
| `assists` | Int32 | Total assists. |
| `steals` | Int32 | Total steals. |
| `blocks` | Int32 | Total blocks. |
| `turnovers` | Int32 | Total turnovers. |
| `fouls` | Int32 | Personal fouls. |
| `points` | Int32 | Points scored. |
| `starter` | Boolean | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `ejected` | Boolean | TRUE if the player was ejected from the game. |
| `did_not_play` | Boolean | TRUE if the player did not appear in the game. |
| `athlete_jersey` | String | Athlete jersey number. |
| `athlete_short_name` | String | Athlete short display name. |
| `athlete_headshot_href` | String | Athlete headshot image URL. |
| `athlete_position_name` | String | Athlete position ('Guard', 'Forward', 'Center'). |
| `athlete_position_abbreviation` | String | Athlete position abbreviation (G / F / C). |
| `team_display_name` | String | Full team display name. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_logo` | String | Team logo image URL. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `home_away` | String | Game venue label ('home' or 'away'). |
| `team_winner` | Boolean | TRUE if the team won this game. |
| `team_score` | Int32 | Team's score / final score. |
| `opponent_team_id` | Int32 | Unique identifier for the opponent team. |
| `opponent_team_name` | String | Opponent team display name. |
| `opponent_team_location` | String | Opponent team city / location. |
| `opponent_team_display_name` | String | Opponent team full display name. |
| `opponent_team_abbreviation` | String | Opponent team abbreviation. |
| `opponent_team_logo` | String | Opponent team logo URL. |
| `opponent_team_color` | String | Opponent team primary color (hex). |
| `opponent_team_alternate_color` | String | Opponent team alternate color (hex). |
| `opponent_team_score` | Int32 | Opponent team's score. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |

```python
load_mbb_player_boxscore(seasons=2024)
```

## load_mbb_player_value

Release: [mbb_player_value](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_player_value) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_player_value/mbb_player_value_{season}.parquet`
### Returns {#load_mbb_player_value-returns}

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `player` | String | Player name. |
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `min` | Float64 | Minutes played. |
| `box_obpm` | Float64 | Box-score offensive plus/minus for the player, the offensive half of box BPM. |
| `box_dbpm` | Float64 | Box-score defensive plus/minus for the player, the defensive half of box BPM. |
| `box_bpm` | Float64 | Total box plus/minus in points per 100 possessions above an average player, exactly box_obpm plus box_dbpm (verified to zero residual across all 9,805 rows of 2025). |
| `qualified` | Boolean | True/False indicator of whether or not player meets minimum play requirement |

```python
load_mbb_player_value(seasons=2025)
```

## load_mbb_player_season_stats

Release: [espn_mens_college_basketball_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_player_season_stats/player_season_stats_{season}.parquet`
### Returns {#load_mbb_player_season_stats-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season year. |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_position_abbreviation` | String | Athlete position abbreviation (G / F / C). |
| `athlete_jersey` | String | Athlete jersey number. |
| `team_id` | Int32 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_display_name` | String | Full team display name. |
| `category` | String | Category label. |
| `stat_label` | String | Human-readable label of the statistic (e.g. 'At bats'). |
| `stat_name` | String | Stat key. |
| `stat_display_name` | String | Stat display name. |
| `stat_description` | String | ESPN's prose glossary definition of the statistic named in stat_name, for example defining assists as a pass to a teammate that leads directly to a field goal. |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_mbb_player_season_stats(seasons=2025)
```

## load_mbb_player_crosswalk

Release: [mbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/mbb_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/mbb_crosswalk/mbb_player_crosswalk_{season}.parquet`
### Returns {#load_mbb_player_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season year. |
| `espn_team_id` | Int32 | ESPN team id (canonical key). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_name` | String | Player name. |
| `espn_athlete_id` | String | ESPN athlete id. |
| `espn_full_name` | String | ESPN full name. |
| `espn_jersey` | String | ESPN jersey number. |
| `espn_position` | String | ESPN position abbreviation. |
| `fox_athlete_id` | String | Fox athlete id (NA if unmatched). |
| `fox_player` | String | Fox player name (NA if unmatched). |
| `fox_jersey` | String | Fox jersey number (NA if unmatched). |
| `fox_position_group` | String | Fox position group label (NA if unmatched). |
| `yahoo_player_id` | String | Yahoo player id (NA placeholder). |
| `yahoo_player_name` | String | Yahoo player name (NA placeholder). |
| `match_method` | String | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | Float64 | Jaro-Winkler score or 1 for exact (NA if none). |
| `match_keys` | String | NA (reserved for future use). |

```python
load_mbb_player_crosswalk(seasons=2026)
```

## load_mbb_player_core

Release: [espn_mens_college_basketball_player_core](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_core) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_player_core/player_core_{season}.parquet`
### Returns {#load_mbb_player_core-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season year. |
| `athlete_id` | Int64 | Unique athlete identifier (ESPN). |
| `guid` | String | Stable cross-league team GUID. |
| `uid` | String | ESPN UID string. |
| `slug` | String | URL-safe identifier. |
| `type` | String | Record type / category. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short display name. |
| `height` | Float64 | Player height (string e.g. '6-2' or inches). |
| `display_height` | String | Player height in display format (e.g. '6-2'). |
| `weight` | Float64 | Player weight in pounds. |
| `display_weight` | String | Player weight in display format (e.g. '180 lbs'). |
| `age` | Int32 | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `birth_city` | String | Birth city. |
| `birth_state` | String | Birth state / region. |
| `birth_country` | String | Player birth country. |
| `jersey` | String | Jersey number worn by the player. |
| `position_id` | Int32 | Unique position identifier. |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_abbreviation` | String | Position abbreviation ('G' / 'F' / 'C'). |
| `position_display_name` | String | Position display name. |
| `college_id` | Int32 | Unique identifier for college. |
| `current_team_id` | Int32 | Player's current team identifier. |
| `headshot_href` | String | Headshot image URL. |
| `experience_years` | Int32 | Experience years. |
| `status_id` | Int32 | Status identifier. |
| `status_name` | String | Status label. |
| `status_type` | String | Status type. |
| `draft_year` | Int32 | Draft year (4-digit). |
| `draft_round` | Int32 | Round of the draft selection. |
| `draft_selection` | Int32 | Draft selection. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |

```python
load_mbb_player_core(seasons=2025)
```
