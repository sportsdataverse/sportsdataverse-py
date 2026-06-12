---
title: MBB dataset loaders
sidebar_label: Loaders
sidebar_position: 1
---
# MBB dataset loaders

```mermaid
flowchart LR
  raw["scrape / raw"] --> enrich["enrich"] --> rel["release asset"] --> load["load_*()"]
```

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_mbb_pbp` | [espn_mens_college_basketball_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_pbp) | — |
| `load_mbb_player_boxscore` | [espn_mens_college_basketball_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_boxscores) | — |
| `load_mbb_schedule` | [espn_mens_college_basketball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_schedules) | — |
| `load_mbb_team_boxscore` | [espn_mens_college_basketball_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_boxscores) | — |
| `load_mbb_shots` | [espn_mens_college_basketball_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_shots) | — |
| `load_mbb_standings` | [espn_mens_college_basketball_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_standings) | — |
| `load_mbb_player_season_stats` | [espn_mens_college_basketball_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats) | — |
| `load_mbb_rosters` | [espn_mens_college_basketball_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_rosters) | — |
| `load_mbb_officials` | [espn_mens_college_basketball_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_officials) | — |
| `load_mbb_game_rosters` | [espn_mens_college_basketball_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters) | — |
| `load_mbb_team_season_stats` | [espn_mens_college_basketball_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_season_stats) | — |

## `load_mbb_pbp`

Release: [espn_mens_college_basketball_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_pbp/play_by_play_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `game_play_number` | Int32 |
| `id` | Float64 |
| `sequence_number` | Int32 |
| `type_id` | Int32 |
| `type_text` | String |
| `text` | String |
| `away_score` | Int32 |
| `home_score` | Int32 |
| `period_number` | Int32 |
| `period_display_value` | String |
| `clock_display_value` | String |
| `scoring_play` | Boolean |
| `score_value` | Int32 |
| `team_id` | Int32 |
| `athlete_id_1` | Int32 |
| `wallclock` | String |
| `shooting_play` | Boolean |
| `game_id` | Int32 |
| `season` | Int32 |
| `season_type` | Int32 |
| `home_team_id` | Int32 |
| `home_team_name` | String |
| `home_team_mascot` | String |
| `home_team_abbrev` | String |
| `home_team_name_alt` | String |
| `away_team_id` | Int32 |
| `away_team_name` | String |
| `away_team_mascot` | String |
| `away_team_abbrev` | String |
| `away_team_name_alt` | String |
| `game_spread` | Float64 |
| `home_favorite` | Boolean |
| `game_spread_available` | Boolean |
| `home_team_spread` | Float64 |
| `half` | Int32 |
| `time` | String |
| `clock_minutes` | Int32 |
| `clock_seconds` | Int32 |
| `home_timeout_called` | Boolean |
| `away_timeout_called` | Boolean |
| `lead_period` | Int32 |
| `lead_half` | Int32 |
| `start_period_seconds_remaining` | Int32 |
| `start_game_seconds_remaining` | Int32 |
| `end_period_seconds_remaining` | Int32 |
| `end_game_seconds_remaining` | Int32 |
| `lag_period` | Int32 |
| `lag_half` | Int32 |
| `athlete_id_2` | Int32 |
| `game_date` | Date |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') |
| `coordinate_x_raw` | Float64 |
| `coordinate_y_raw` | Float64 |
| `coordinate_x` | Float64 |
| `coordinate_y` | Float64 |
| `media_id` | String |

```python
load_mbb_pbp(seasons=2024)
```

## `load_mbb_player_boxscore`

Release: [espn_mens_college_basketball_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_player_boxscores/player_box_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `game_id` | Int32 |
| `season` | Int32 |
| `season_type` | Int32 |
| `game_date` | Date |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') |
| `athlete_id` | Int32 |
| `athlete_display_name` | String |
| `team_id` | Int32 |
| `team_name` | String |
| `team_location` | String |
| `team_short_display_name` | String |
| `minutes` | Float64 |
| `field_goals_made` | Int32 |
| `field_goals_attempted` | Int32 |
| `three_point_field_goals_made` | Int32 |
| `three_point_field_goals_attempted` | Int32 |
| `free_throws_made` | Int32 |
| `free_throws_attempted` | Int32 |
| `offensive_rebounds` | Int32 |
| `defensive_rebounds` | Int32 |
| `rebounds` | Int32 |
| `assists` | Int32 |
| `steals` | Int32 |
| `blocks` | Int32 |
| `turnovers` | Int32 |
| `fouls` | Int32 |
| `points` | Int32 |
| `starter` | Boolean |
| `ejected` | Boolean |
| `did_not_play` | Boolean |
| `active` | Boolean |
| `athlete_jersey` | String |
| `athlete_short_name` | String |
| `athlete_headshot_href` | String |
| `athlete_position_name` | String |
| `athlete_position_abbreviation` | String |
| `team_display_name` | String |
| `team_uid` | String |
| `team_slug` | String |
| `team_logo` | String |
| `team_abbreviation` | String |
| `team_color` | String |
| `team_alternate_color` | String |
| `home_away` | String |
| `team_winner` | Boolean |
| `team_score` | Int32 |
| `opponent_team_id` | Int32 |
| `opponent_team_name` | String |
| `opponent_team_location` | String |
| `opponent_team_display_name` | String |
| `opponent_team_abbreviation` | String |
| `opponent_team_logo` | String |
| `opponent_team_color` | String |
| `opponent_team_alternate_color` | String |
| `opponent_team_score` | Int32 |

```python
load_mbb_player_boxscore(seasons=2024)
```

## `load_mbb_schedule`

Release: [espn_mens_college_basketball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_schedules/mbb_schedule_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `id` | Int32 |
| `uid` | String |
| `date` | String |
| `attendance` | Float64 |
| `time_valid` | Boolean |
| `neutral_site` | Boolean |
| `conference_competition` | Boolean |
| `recent` | Boolean |
| `start_date` | String |
| `notes_type` | String |
| `notes_headline` | String |
| `type_id` | Int32 |
| `type_abbreviation` | String |
| `venue_id` | Int32 |
| `venue_full_name` | String |
| `venue_address_city` | String |
| `venue_address_state` | String |
| `venue_capacity` | Float64 |
| `venue_indoor` | Boolean |
| `status_clock` | Float64 |
| `status_display_clock` | String |
| `status_period` | Float64 |
| `status_type_id` | Int32 |
| `status_type_name` | String |
| `status_type_state` | String |
| `status_type_completed` | Boolean |
| `status_type_description` | String |
| `status_type_detail` | String |
| `status_type_short_detail` | String |
| `format_regulation_periods` | Float64 |
| `home_id` | Int32 |
| `home_uid` | String |
| `home_location` | String |
| `home_name` | String |
| `home_abbreviation` | String |
| `home_display_name` | String |
| `home_short_display_name` | String |
| `home_color` | String |
| `home_alternate_color` | String |
| `home_is_active` | Boolean |
| `home_venue_id` | Int32 |
| `home_logo` | String |
| `home_conference_id` | Int32 |
| `home_score` | Int32 |
| `home_winner` | Boolean |
| `away_id` | Int32 |
| `away_uid` | String |
| `away_location` | String |
| `away_name` | String |
| `away_abbreviation` | String |
| `away_display_name` | String |
| `away_short_display_name` | String |
| `away_color` | String |
| `away_alternate_color` | String |
| `away_is_active` | Boolean |
| `away_venue_id` | Int32 |
| `away_logo` | String |
| `away_conference_id` | Int32 |
| `away_score` | Int32 |
| `away_winner` | Boolean |
| `game_id` | Int32 |
| `season` | Int32 |
| `season_type` | Int32 |
| `status_type_alt_detail` | String |
| `groups_id` | Int32 |
| `groups_name` | String |
| `groups_short_name` | String |
| `groups_is_conference` | Boolean |
| `tournament_id` | Int32 |
| `game_json` | Boolean |
| `game_json_url` | Boolean |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') |
| `game_date` | Date |
| `PBP` | Boolean |
| `team_box` | Boolean |
| `player_box` | Boolean |

```python
load_mbb_schedule(seasons=2024)
```

## `load_mbb_team_boxscore`

Release: [espn_mens_college_basketball_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_team_boxscores/team_box_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `game_id` | Int32 |
| `season` | Int32 |
| `season_type` | Int32 |
| `game_date` | Date |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') |
| `team_id` | Int32 |
| `team_uid` | String |
| `team_slug` | String |
| `team_location` | String |
| `team_name` | String |
| `team_abbreviation` | String |
| `team_display_name` | String |
| `team_short_display_name` | String |
| `team_color` | String |
| `team_alternate_color` | String |
| `team_logo` | String |
| `team_home_away` | String |
| `team_score` | Int32 |
| `team_winner` | Boolean |
| `assists` | Int32 |
| `blocks` | Int32 |
| `defensive_rebounds` | Int32 |
| `field_goal_pct` | Float64 |
| `field_goals_made` | Int32 |
| `field_goals_attempted` | Int32 |
| `flagrant_fouls` | Int32 |
| `fouls` | Int32 |
| `free_throw_pct` | Float64 |
| `free_throws_made` | Int32 |
| `free_throws_attempted` | Int32 |
| `largest_lead` | String |
| `offensive_rebounds` | Int32 |
| `steals` | Int32 |
| `team_turnovers` | Int32 |
| `technical_fouls` | Int32 |
| `three_point_field_goal_pct` | Float64 |
| `three_point_field_goals_made` | Int32 |
| `three_point_field_goals_attempted` | Int32 |
| `total_rebounds` | Int32 |
| `total_technical_fouls` | Int32 |
| `total_turnovers` | Int32 |
| `turnovers` | Int32 |
| `opponent_team_id` | Int32 |
| `opponent_team_uid` | String |
| `opponent_team_slug` | String |
| `opponent_team_location` | String |
| `opponent_team_name` | String |
| `opponent_team_abbreviation` | String |
| `opponent_team_display_name` | String |
| `opponent_team_short_display_name` | String |
| `opponent_team_color` | String |
| `opponent_team_alternate_color` | String |
| `opponent_team_logo` | String |
| `opponent_team_score` | Int32 |

```python
load_mbb_team_boxscore(seasons=2024)
```

## `load_mbb_shots`

Release: [espn_mens_college_basketball_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_shots) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_shots/shots_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `game_id` | Int32 |
| `season` | Int32 |
| `period_number` | Int32 |
| `clock_display_value` | String |
| `team_id` | Int32 |
| `athlete_id_1` | Int32 |
| `athlete_id_2` | Int32 |
| `type_id` | Int32 |
| `type_text` | String |
| `scoring_play` | Boolean |
| `score_value` | Int32 |
| `coordinate_x` | Float64 |
| `coordinate_y` | Float64 |
| `coordinate_x_raw` | Float64 |
| `coordinate_y_raw` | Float64 |

```python
load_mbb_shots(seasons=2025)
```

## `load_mbb_standings`

Release: [espn_mens_college_basketball_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_standings) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_standings/standings_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `season` | Int32 |
| `group_id` | String |
| `group_name` | String |
| `group_abbreviation` | String |
| `group_short_name` | String |
| `team_id` | Int32 |
| `team_uid` | String |
| `team_slug` | String |
| `team_location` | String |
| `team_name` | String |
| `team_abbreviation` | String |
| `team_display_name` | String |
| `team_short_display_name` | String |
| `team_color` | String |
| `team_alternate_color` | String |
| `team_logo` | String |
| `stat_name` | String |
| `stat_display_name` | String |
| `stat_short_display_name` | String |
| `stat_description` | String |
| `stat_abbreviation` | String |
| `stat_type` | String |
| `display_value` | String |
| `value` | Float64 |

```python
load_mbb_standings(seasons=2025)
```

## `load_mbb_player_season_stats`

Release: [espn_mens_college_basketball_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_player_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_player_season_stats/player_season_stats_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `season` | Int32 |
| `athlete_id` | Int32 |
| `athlete_display_name` | String |
| `athlete_position_abbreviation` | String |
| `athlete_jersey` | String |
| `team_id` | Int32 |
| `team_slug` | String |
| `team_display_name` | String |
| `category` | String |
| `stat_label` | String |
| `stat_name` | String |
| `stat_display_name` | String |
| `stat_description` | String |
| `display_value` | String |
| `value` | Float64 |

```python
load_mbb_player_season_stats(seasons=2025)
```

## `load_mbb_rosters`

Release: [espn_mens_college_basketball_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_rosters/rosters_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `season` | Int32 |
| `team_id` | Int32 |
| `team_slug` | String |
| `team_abbreviation` | String |
| `team_display_name` | String |
| `team_short_display_name` | String |
| `team_color` | String |
| `team_alternate_color` | String |
| `team_logo` | String |
| `athlete_id` | String |
| `uid` | String |
| `guid` | String |
| `full_name` | String |
| `display_name` | String |
| `short_name` | String |
| `first_name` | String |
| `last_name` | String |
| `jersey` | String |
| `position_abbreviation` | String |
| `position_name` | String |
| `position_id` | String |
| `height` | String |
| `weight` | String |
| `age` | String |
| `date_of_birth` | String |
| `birth_place_city` | String |
| `birth_place_state` | String |
| `birth_place_country` | String |
| `experience_years` | String |
| `experience_display_value` | String |
| `headshot_href` | String |
| `headshot_alt` | String |
| `link_web` | String |
| `status_id` | String |
| `status_name` | String |
| `status_type` | String |

```python
load_mbb_rosters(seasons=2025)
```

## `load_mbb_officials`

Release: [espn_mens_college_basketball_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_officials) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_officials/officials_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `season` | Int32 |
| `game_id` | Int32 |
| `official_full_name` | String |
| `official_display_name` | String |
| `official_position` | String |
| `official_position_id` | Int32 |
| `official_order` | Int32 |

```python
load_mbb_officials(seasons=2025)
```

## `load_mbb_game_rosters`

Release: [espn_mens_college_basketball_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_game_rosters/game_rosters_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `season` | Int32 |
| `game_id` | String |
| `team_id` | Int32 |
| `team_slug` | String |
| `team_abbreviation` | String |
| `team_display_name` | String |
| `home_away` | String |
| `athlete_id` | Int32 |
| `athlete_uid` | String |
| `athlete_guid` | String |
| `athlete_display_name` | String |
| `athlete_short_name` | String |
| `athlete_first_name` | String |
| `athlete_last_name` | String |
| `athlete_jersey` | String |
| `athlete_position` | String |
| `athlete_headshot` | String |
| `starter` | Boolean |
| `did_not_play` | Boolean |
| `active` | Boolean |
| `ejected` | Boolean |
| `reason` | String |

```python
load_mbb_game_rosters(seasons=2025)
```

## `load_mbb_team_season_stats`

Release: [espn_mens_college_basketball_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_mens_college_basketball_team_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_team_season_stats/team_season_stats_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `season` | Int32 |
| `team_id` | Int32 |
| `team_slug` | String |
| `team_abbreviation` | String |
| `team_display_name` | String |
| `team_short_display_name` | String |
| `team_color` | String |
| `team_alternate_color` | String |
| `team_logo` | String |
| `category` | String |
| `stat_label` | String |
| `stat_name` | String |
| `stat_display_name` | String |
| `stat_description` | String |
| `display_value` | String |
| `value` | Float64 |

```python
load_mbb_team_season_stats(seasons=2025)
```
