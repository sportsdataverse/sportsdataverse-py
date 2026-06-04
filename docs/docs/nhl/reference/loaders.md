---
title: NHL dataset loaders
sidebar_label: Loaders
---
# NHL dataset loaders

```mermaid
flowchart LR
  raw["scrape / raw"] --> enrich["enrich"] --> rel["release asset"] --> load["load_*()"]
```

## Automation status

| Dataset | Release tag | Pipeline |
|---|---|---|
| `load_nhl_pbp` | [fastRhockey-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/fastRhockey-data) | — |
| `load_nhl_player_boxscore` | [fastRhockey-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/fastRhockey-data) | — |
| `load_nhl_schedule` | [fastRhockey-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/fastRhockey-data) | — |
| `load_nhl_team_boxscore` | [fastRhockey-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/fastRhockey-data) | — |
| `load_nhl_game_info` | [nhl_game_info](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_game_info) | — |
| `load_nhl_game_rosters` | [nhl_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_game_rosters) | — |
| `load_nhl_goalie_boxscores` | [nhl_goalie_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_goalie_boxscores) | — |
| `load_nhl_linescore` | [nhl_linescore](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_linescore) | — |
| `load_nhl_officials` | [nhl_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_officials) | — |
| `load_nhl_pbp_full` | [nhl_pbp_full](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_full) | — |
| `load_nhl_pbp_lite` | [nhl_pbp_lite](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_lite) | — |
| `load_nhl_penalties` | [nhl_penalties](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_penalties) | — |
| `load_nhl_player_boxscores` | [nhl_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_player_boxscores) | — |
| `load_nhl_rosters` | [nhl_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_rosters) | — |
| `load_nhl_schedules` | [nhl_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_schedules) | — |
| `load_nhl_scoring` | [nhl_scoring](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_scoring) | — |
| `load_nhl_scratches` | [nhl_scratches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_scratches) | — |
| `load_nhl_shifts` | [nhl_shifts](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_shifts) | — |
| `load_nhl_shootout` | [nhl_shootout](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_shootout) | — |
| `load_nhl_shots_by_period` | [nhl_shots_by_period](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_shots_by_period) | — |
| `load_nhl_skater_boxscores` | [nhl_skater_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_skater_boxscores) | — |
| `load_nhl_team_boxscores` | [nhl_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_team_boxscores) | — |
| `load_nhl_three_stars` | [nhl_three_stars](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_three_stars) | — |

## `load_nhl_pbp`

Release: [fastRhockey-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/fastRhockey-data) · asset `https://raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/nhl/pbp/parquet/play_by_play_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `event_type` | String |
| `event` | String |
| `description` | String |
| `period` | Int32 |
| `period_seconds` | Int32 |
| `period_seconds_remaining` | Int32 |
| `game_seconds` | Int32 |
| `game_seconds_remaining` | Int32 |
| `home_score` | Int32 |
| `away_score` | Int32 |
| `strength_state` | String |
| `event_idx` | String |
| `extra_attacker` | Boolean |
| `home_skaters` | Int32 |
| `away_skaters` | Int32 |
| `game_id` | Int32 |
| `period_type` | String |
| `ordinal_num` | String |
| `period_time` | String |
| `period_time_remaining` | String |
| `date_time` | String |
| `home_final` | Int32 |
| `away_final` | Int32 |
| `season` | Int32 |
| `season_type` | String |
| `game_date` | String |
| `game_start` | String |
| `game_end` | String |
| `game_length` | Int32 |
| `game_state` | String |
| `detailed_state` | String |
| `venue_name` | String |
| `venue_link` | String |
| `home_name` | String |
| `home_abbreviation` | String |
| `home_division_name` | String |
| `home_conference_name` | String |
| `home_id` | String |
| `away_name` | String |
| `away_abbreviation` | String |
| `away_division_name` | String |
| `away_conference_name` | String |
| `away_id` | String |
| `event_id` | Float64 |
| `event_team` | String |
| `event_team_type` | String |
| `num_on` | Int32 |
| `players_on` | String |
| `players_off` | String |
| `away_on_1` | String |
| `away_on_2` | String |
| `away_on_3` | String |
| `away_on_4` | String |
| `away_on_5` | String |
| `away_goalie` | String |
| `ids_on` | String |
| `ids_off` | String |
| `secondary_type` | String |
| `home_on_1` | String |
| `home_on_2` | String |
| `home_on_3` | String |
| `home_on_4` | String |
| `home_on_5` | String |
| `home_goalie` | String |
| `event_player_1_name` | String |
| `event_player_1_type` | String |
| `event_player_2_name` | String |
| `event_player_2_type` | String |
| `strength_code` | String |
| `strength` | String |
| `x` | Int32 |
| `y` | Int32 |
| `x_fixed` | Int32 |
| `y_fixed` | Int32 |
| `event_player_1_id` | Int32 |
| `event_player_1_link` | String |
| `event_player_2_id` | Int32 |
| `event_player_2_link` | String |
| `event_team_id` | Int32 |
| `event_team_link` | String |
| `event_team_abbr` | String |
| `num_off` | Int32 |
| `event_goalie_name` | String |
| `shot_distance` | Float64 |
| `shot_angle` | Float64 |
| `event_goalie_id` | Int32 |
| `event_goalie_link` | String |
| `event_goalie_type` | String |
| `event_player_3_name` | String |
| `event_player_3_type` | String |
| `game_winning_goal` | Boolean |
| `empty_net` | Boolean |
| `event_player_3_id` | Int32 |
| `event_player_3_link` | String |
| `event_player_4_type` | String |
| `event_player_4_id` | Int32 |
| `event_player_4_name` | String |
| `event_player_4_link` | String |
| `penalty_severity` | String |
| `penalty_minutes` | Int32 |
| `home_on_6` | String |
| `venue_id` | Int32 |
| `away_on_6` | String |

```python
load_nhl_pbp(seasons=2024)
```

## `load_nhl_player_boxscore`

Release: [fastRhockey-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/fastRhockey-data) · asset `https://raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/nhl/player_box/parquet/player_box_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `player_id` | Int32 |
| `player_full_name` | String |
| `link` | String |
| `shoots_catches` | String |
| `roster_status` | String |
| `jersey_number` | String |
| `position_code` | String |
| `position_name` | String |
| `position_type` | String |
| `position_abbreviation` | String |
| `skater_stats_time_on_ice` | String |
| `skater_stats_assists` | Int32 |
| `skater_stats_goals` | Int32 |
| `skater_stats_shots` | Int32 |
| `skater_stats_hits` | Int32 |
| `skater_stats_power_play_goals` | Int32 |
| `skater_stats_power_play_assists` | Int32 |
| `skater_stats_penalty_minutes` | Int32 |
| `skater_stats_face_off_wins` | Int32 |
| `skater_stats_faceoff_taken` | Int32 |
| `skater_stats_takeaways` | Int32 |
| `skater_stats_giveaways` | Int32 |
| `skater_stats_short_handed_goals` | Int32 |
| `skater_stats_short_handed_assists` | Int32 |
| `skater_stats_blocked` | Int32 |
| `skater_stats_plus_minus` | Int32 |
| `skater_stats_even_time_on_ice` | String |
| `skater_stats_power_play_time_on_ice` | String |
| `skater_stats_short_handed_time_on_ice` | String |
| `home_away` | String |
| `skater_stats_face_off_pct` | Float64 |
| `goalie_stats_time_on_ice` | String |
| `goalie_stats_assists` | Int32 |
| `goalie_stats_goals` | Int32 |
| `goalie_stats_pim` | Int32 |
| `goalie_stats_shots` | Int32 |
| `goalie_stats_saves` | Int32 |
| `goalie_stats_power_play_saves` | Int32 |
| `goalie_stats_short_handed_saves` | Int32 |
| `goalie_stats_even_saves` | Int32 |
| `goalie_stats_short_handed_shots_against` | Int32 |
| `goalie_stats_even_shots_against` | Int32 |
| `goalie_stats_power_play_shots_against` | Int32 |
| `goalie_stats_decision` | String |
| `goalie_stats_save_percentage` | Float64 |
| `goalie_stats_power_play_save_percentage` | Float64 |
| `goalie_stats_even_strength_save_percentage` | Float64 |
| `goalie_stats_short_handed_save_percentage` | Float64 |
| `game_id` | Int32 |
| `season` | Int32 |

```python
load_nhl_player_boxscore(seasons=2024)
```

## `load_nhl_schedule`

Release: [fastRhockey-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/fastRhockey-data) · asset `https://raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/nhl/schedules/parquet/nhl_schedule_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `game_id` | Int32 |
| `link` | String |
| `game_type_abbreviation` | String |
| `season_full` | Int32 |
| `game_date_time` | Datetime(time_unit='us', time_zone='UTC') |
| `status_abstract_game_state` | String |
| `status_coded_game_state` | Int32 |
| `status_detailed_state` | String |
| `status_status_code` | Int32 |
| `status_start_time_tbd` | Boolean |
| `away_score` | Int32 |
| `away_team_id` | Int32 |
| `away_team_name` | String |
| `away_team_link` | String |
| `home_score` | Int32 |
| `home_team_id` | Int32 |
| `home_team_name` | String |
| `home_team_link` | String |
| `venue_name` | String |
| `venue_link` | String |
| `venue_id` | Int32 |
| `content_link` | String |
| `game_type` | String |
| `game_date` | Date |
| `season` | Int32 |
| `PBP` | Boolean |
| `team_box` | Boolean |
| `player_box` | Boolean |

```python
load_nhl_schedule(seasons=2024)
```

## `load_nhl_team_boxscore`

Release: [fastRhockey-data](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/fastRhockey-data) · asset `https://raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/nhl/team_box/parquet/team_box_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `team_id` | Int32 |
| `team_name` | String |
| `link` | String |
| `abbreviation` | String |
| `tri_code` | String |
| `goals` | Int32 |
| `pim` | Int32 |
| `shots` | Int32 |
| `power_play_percentage` | String |
| `power_play_goals` | Int32 |
| `power_play_opportunities` | Int32 |
| `face_off_win_percentage` | String |
| `blocked` | Int32 |
| `takeaways` | Int32 |
| `giveaways` | Int32 |
| `hits` | Int32 |
| `game_id` | Int32 |
| `season` | Int32 |

```python
load_nhl_team_boxscore(seasons=2024)
```

## `load_nhl_game_info`

Release: [nhl_game_info](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_game_info) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_game_info/game_info_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `game_id` | Int32 |
| `season` | Int32 |
| `game_type` | String |
| `game_date` | String |
| `venue` | String |
| `home_team_abbr` | String |
| `away_team_abbr` | String |
| `home_score` | Int32 |
| `away_score` | Int32 |
| `game_state` | String |

```python
load_nhl_game_info(seasons=2024)
```

## `load_nhl_game_rosters`

Release: [nhl_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_game_rosters/game_rosters_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `player_id` | Int32 |
| `full_name` | String |
| `first_name` | String |
| `last_name` | String |
| `team_abbr` | String |
| `team_id` | Int32 |
| `position_code` | String |
| `sweater_number` | Int32 |
| `game_id` | Int32 |
| `season` | Int32 |
| `game_date` | String |

```python
load_nhl_game_rosters(seasons=2024)
```

## `load_nhl_goalie_boxscores`

Release: [nhl_goalie_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_goalie_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_goalie_boxscores/goalie_box_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `home_away` | String |
| `team_id` | Int32 |
| `team_abbrev` | String |
| `player_id` | Int32 |
| `player_name` | String |
| `sweater_number` | Int32 |
| `even_strength_shots_against` | String |
| `power_play_shots_against` | String |
| `shorthanded_shots_against` | String |
| `save_shots_against` | String |
| `save_pctg` | Float64 |
| `even_strength_goals_against` | Int32 |
| `power_play_goals_against` | Int32 |
| `shorthanded_goals_against` | Int32 |
| `pim` | Int32 |
| `goals_against` | Int32 |
| `toi` | String |
| `starter` | Boolean |
| `decision` | String |
| `shots_against` | Int32 |
| `saves` | Int32 |
| `game_id` | Int32 |
| `season` | Int32 |
| `game_date` | String |

```python
load_nhl_goalie_boxscores(seasons=2024)
```

## `load_nhl_linescore`

Release: [nhl_linescore](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_linescore) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_linescore/linescore_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `game_id` | Int32 |
| `home_team_id` | Int32 |
| `home_team_abbr` | String |
| `home_goals` | Int32 |
| `home_shots` | Int32 |
| `away_team_id` | Int32 |
| `away_team_abbr` | String |
| `away_goals` | Int32 |
| `away_shots` | Int32 |
| `has_shootout` | Boolean |

```python
load_nhl_linescore(seasons=2024)
```

## `load_nhl_officials`

Release: [nhl_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_officials) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_officials/officials_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `role` | String |
| `name` | String |
| `game_id` | Int32 |
| `season` | Int32 |
| `game_date` | String |

```python
load_nhl_officials(seasons=2025)
```

## `load_nhl_pbp_full`

Release: [nhl_pbp_full](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_full) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_pbp_full/play_by_play_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `event_type` | String |
| `event` | String |
| `secondary_type` | String |
| `event_team_abbr` | String |
| `event_team_type` | String |
| `description` | String |
| `period` | Int32 |
| `period_type` | String |
| `period_time` | String |
| `period_seconds` | Int32 |
| `period_seconds_remaining` | Int32 |
| `period_time_remaining` | String |
| `game_seconds` | Int32 |
| `game_seconds_remaining` | Int32 |
| `home_score` | Int32 |
| `away_score` | Int32 |
| `event_player_1_name` | String |
| `event_player_1_type` | String |
| `event_player_1_id` | Int32 |
| `event_player_2_name` | String |
| `event_player_2_type` | String |
| `event_player_2_id` | Int32 |
| `event_player_3_name` | String |
| `event_player_3_type` | String |
| `event_player_3_id` | Int32 |
| `event_goalie_name` | String |
| `event_goalie_id` | Int32 |
| `penalty_severity` | String |
| `penalty_minutes` | Int32 |
| `empty_net` | Boolean |
| `extra_attacker` | Boolean |
| `x` | Int32 |
| `y` | Int32 |
| `x_fixed` | Int32 |
| `y_fixed` | Int32 |
| `shot_distance` | Float64 |
| `shot_angle` | Float64 |
| `home_skaters` | Int32 |
| `away_skaters` | Int32 |
| `players_on` | Boolean |
| `players_off` | Boolean |
| `game_id` | Int32 |
| `season` | Int32 |
| `season_type` | String |
| `home_abbr` | String |
| `away_abbr` | String |
| `event_idx` | Int32 |
| `event_id` | Int32 |
| `away_goalie_in` | Int32 |
| `home_goalie_in` | Int32 |
| `reason` | String |
| `secondaryReason` | String |
| `xg` | Float64 |

```python
load_nhl_pbp_full(seasons=2010)
```

## `load_nhl_pbp_lite`

Release: [nhl_pbp_lite](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_lite) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_pbp_lite/play_by_play_{season}_lite.parquet`
### Returns

| col_name | type |
|---|---|
| `event_type` | String |
| `event` | String |
| `secondary_type` | String |
| `event_team_abbr` | String |
| `event_team_type` | String |
| `description` | String |
| `period` | Int32 |
| `period_type` | String |
| `period_time` | String |
| `period_seconds` | Int32 |
| `period_seconds_remaining` | Int32 |
| `period_time_remaining` | String |
| `game_seconds` | Int32 |
| `game_seconds_remaining` | Int32 |
| `home_score` | Int32 |
| `away_score` | Int32 |
| `event_player_1_name` | String |
| `event_player_1_type` | String |
| `event_player_1_id` | Int32 |
| `event_player_2_name` | String |
| `event_player_2_type` | String |
| `event_player_2_id` | Int32 |
| `event_player_3_name` | String |
| `event_player_3_type` | String |
| `event_player_3_id` | Int32 |
| `event_goalie_name` | String |
| `event_goalie_id` | Int32 |
| `penalty_severity` | String |
| `penalty_minutes` | Int32 |
| `empty_net` | Boolean |
| `extra_attacker` | Boolean |
| `x` | Int32 |
| `y` | Int32 |
| `x_fixed` | Int32 |
| `y_fixed` | Int32 |
| `shot_distance` | Float64 |
| `shot_angle` | Float64 |
| `home_skaters` | Int32 |
| `away_skaters` | Int32 |
| `players_on` | Boolean |
| `players_off` | Boolean |
| `game_id` | Int32 |
| `season` | Int32 |
| `season_type` | String |
| `home_abbr` | String |
| `away_abbr` | String |
| `event_idx` | Int32 |
| `event_id` | Int32 |
| `away_goalie_in` | Int32 |
| `home_goalie_in` | Int32 |
| `reason` | String |
| `secondaryReason` | String |
| `xg` | Float64 |

```python
load_nhl_pbp_lite(seasons=2010)
```

## `load_nhl_penalties`

Release: [nhl_penalties](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_penalties) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_penalties/penalties_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `timeInPeriod` | String |
| `type` | String |
| `duration` | Int32 |
| `descKey` | String |
| `game_id` | Int32 |
| `period_number` | Int32 |
| `period_type` | String |
| `committedByPlayer.sweaterNumber` | Int32 |
| `committedByPlayer.firstName.default` | String |
| `committedByPlayer.firstName.cs` | String |
| `committedByPlayer.firstName.de` | String |
| `committedByPlayer.firstName.es` | String |
| `committedByPlayer.firstName.fi` | String |
| `committedByPlayer.firstName.sk` | String |
| `committedByPlayer.firstName.sv` | String |
| `committedByPlayer.firstName.fr` | String |
| `committedByPlayer.lastName.default` | String |
| `committedByPlayer.lastName.cs` | String |
| `committedByPlayer.lastName.fi` | String |
| `committedByPlayer.lastName.sk` | String |
| `committedByPlayer.lastName.sv` | String |
| `committedByPlayer.lastName.de` | String |
| `committedByPlayer.lastName.es` | String |
| `committedByPlayer.lastName.fr` | String |
| `teamAbbrev.default` | String |
| `drawnBy.sweaterNumber` | Int32 |
| `drawnBy.firstName.default` | String |
| `drawnBy.firstName.cs` | String |
| `drawnBy.firstName.fi` | String |
| `drawnBy.firstName.sk` | String |
| `drawnBy.firstName.de` | String |
| `drawnBy.firstName.es` | String |
| `drawnBy.firstName.sv` | String |
| `drawnBy.firstName.fr` | String |
| `drawnBy.lastName.default` | String |
| `drawnBy.lastName.cs` | String |
| `drawnBy.lastName.fi` | String |
| `drawnBy.lastName.sk` | String |
| `drawnBy.lastName.sv` | String |
| `drawnBy.lastName.de` | String |
| `drawnBy.lastName.es` | String |
| `drawnBy.lastName.fr` | String |
| `servedBy.default` | String |
| `servedBy.cs` | String |
| `servedBy.fi` | String |
| `servedBy.sk` | String |
| `servedBy.de` | String |
| `servedBy.es` | String |
| `servedBy.sv` | String |

```python
load_nhl_penalties(seasons=2024)
```

## `load_nhl_player_boxscores`

Release: [nhl_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_player_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_player_boxscores/player_box_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `home_away` | String |
| `team_id` | Int32 |
| `team_abbrev` | String |
| `player_id` | Int32 |
| `player_name` | String |
| `sweater_number` | Int32 |
| `position` | String |
| `goals` | Int32 |
| `assists` | Int32 |
| `points` | Int32 |
| `plus_minus` | Int32 |
| `pim` | Int32 |
| `hits` | Int32 |
| `power_play_goals` | Int32 |
| `shots_on_goal` | Int32 |
| `faceoff_winning_pctg` | Float64 |
| `toi` | String |
| `blocked_shots` | Int32 |
| `shifts` | Int32 |
| `giveaways` | Int32 |
| `takeaways` | Int32 |
| `even_strength_shots_against` | String |
| `power_play_shots_against` | String |
| `shorthanded_shots_against` | String |
| `save_shots_against` | String |
| `save_pctg` | Float64 |
| `even_strength_goals_against` | Int32 |
| `power_play_goals_against` | Int32 |
| `shorthanded_goals_against` | Int32 |
| `goals_against` | Int32 |
| `starter` | Boolean |
| `decision` | String |
| `shots_against` | Int32 |
| `saves` | Int32 |

```python
load_nhl_player_boxscores(seasons=2010)
```

## `load_nhl_rosters`

Release: [nhl_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_rosters/rosters_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `player_id` | Int32 |
| `full_name` | String |
| `first_name` | String |
| `last_name` | String |
| `team_abbr` | String |
| `team_id` | Int32 |
| `position_code` | String |
| `sweater_number` | Int32 |
| `season` | Int32 |

```python
load_nhl_rosters(seasons=2010)
```

## `load_nhl_schedules`

Release: [nhl_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_schedules/nhl_schedule_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `game_id` | Int32 |
| `season_full` | String |
| `game_type` | String |
| `game_date` | String |
| `game_time` | String |
| `home_team_abbr` | String |
| `away_team_abbr` | String |
| `home_team_name` | String |
| `away_team_name` | String |
| `home_score` | Int32 |
| `away_score` | Int32 |
| `game_state` | String |
| `venue` | String |
| `season` | Int32 |
| `game_json` | Boolean |
| `game_json_url` | String |
| `PBP` | Boolean |
| `team_box` | Boolean |
| `player_box` | Boolean |

```python
load_nhl_schedules(seasons=2010)
```

## `load_nhl_scoring`

Release: [nhl_scoring](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_scoring) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_scoring/scoring_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `situationCode` | String |
| `eventId` | Int32 |
| `strength` | String |
| `playerId` | Int32 |
| `headshot` | String |
| `highlightClipSharingUrl` | String |
| `highlightClip` | Float64 |
| `goalsToDate` | Int32 |
| `awayScore` | Int32 |
| `homeScore` | Int32 |
| `timeInPeriod` | String |
| `shotType` | String |
| `goalModifier` | String |
| `assists` | String |
| `pptReplayUrl` | String |
| `homeTeamDefendingSide` | String |
| `isHome` | Boolean |
| `game_id` | Int32 |
| `period_number` | Int32 |
| `period_type` | String |
| `highlightClipSharingUrlFr` | String |
| `highlightClipFr` | Float64 |
| `discreteClip` | Float64 |
| `discreteClipFr` | Float64 |
| `firstName.default` | String |
| `firstName.cs` | String |
| `firstName.de` | String |
| `firstName.es` | String |
| `firstName.fi` | String |
| `firstName.sk` | String |
| `firstName.sv` | String |
| `firstName.fr` | String |
| `lastName.default` | String |
| `lastName.cs` | String |
| `lastName.fi` | String |
| `lastName.sk` | String |
| `lastName.sv` | String |
| `lastName.de` | String |
| `lastName.es` | String |
| `lastName.fr` | String |
| `name.default` | String |
| `name.cs` | String |
| `name.fi` | String |
| `name.sk` | String |
| `name.sv` | String |
| `name.de` | String |
| `name.es` | String |
| `name.fr` | String |
| `teamAbbrev.default` | String |
| `leadingTeamAbbrev.default` | String |

```python
load_nhl_scoring(seasons=2024)
```

## `load_nhl_scratches`

Release: [nhl_scratches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_scratches) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_scratches/scratches_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `id` | Int32 |
| `firstName` | String |
| `lastName` | String |
| `game_id` | Int32 |

```python
load_nhl_scratches(seasons=2024)
```

## `load_nhl_shifts`

Release: [nhl_shifts](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_shifts) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_shifts/shifts_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `event_team` | String |
| `period` | Int32 |
| `period_time` | String |
| `period_seconds` | Int32 |
| `game_seconds` | Int32 |
| `num_on` | Int32 |
| `players_on` | String |
| `ids_on` | String |
| `num_off` | Int32 |
| `players_off` | String |
| `ids_off` | String |
| `event` | String |
| `event_type` | String |
| `game_seconds_remaining` | Int32 |
| `game_id` | Int32 |
| `season` | Int32 |
| `game_date` | String |

```python
load_nhl_shifts(seasons=2025)
```

## `load_nhl_shootout`

Release: [nhl_shootout](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_shootout) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_shootout/shootout_summary_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `home` | Int32 |
| `away` | Int32 |
| `sequence` | Int32 |
| `playerId` | Int32 |
| `shotType` | String |
| `result` | String |
| `headshot` | String |
| `gameWinner` | Boolean |
| `homeScore` | Int32 |
| `awayScore` | Int32 |
| `game_id` | Int32 |
| `season` | Int32 |
| `game_date` | String |
| `discreteClip` | Float64 |
| `discreteClipFr` | Float64 |
| `teamAbbrev.default` | String |
| `firstName.default` | String |
| `firstName.cs` | String |
| `firstName.sk` | String |
| `firstName.fi` | String |
| `firstName.de` | String |
| `firstName.es` | String |
| `firstName.sv` | String |
| `lastName.default` | String |
| `lastName.cs` | String |
| `lastName.fi` | String |
| `lastName.sk` | String |
| `lastName.sv` | String |

```python
load_nhl_shootout(seasons=2025)
```

## `load_nhl_shots_by_period`

Release: [nhl_shots_by_period](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_shots_by_period) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_shots_by_period/shots_by_period_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `away` | Int32 |
| `home` | Int32 |
| `game_id` | Int32 |
| `season` | Int32 |
| `game_date` | String |
| `period` | Int32 |
| `period_type` | String |
| `max_regulation_periods` | Int32 |
| `ot_periods` | Int32 |

```python
load_nhl_shots_by_period(seasons=2025)
```

## `load_nhl_skater_boxscores`

Release: [nhl_skater_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_skater_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_skater_boxscores/skater_box_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `home_away` | String |
| `team_id` | Int32 |
| `team_abbrev` | String |
| `player_id` | Int32 |
| `player_name` | String |
| `sweater_number` | Int32 |
| `position` | String |
| `goals` | Int32 |
| `assists` | Int32 |
| `points` | Int32 |
| `plus_minus` | Int32 |
| `pim` | Int32 |
| `hits` | Int32 |
| `power_play_goals` | Int32 |
| `shots_on_goal` | Int32 |
| `faceoff_winning_pctg` | Float64 |
| `toi` | String |
| `blocked_shots` | Int32 |
| `shifts` | Int32 |
| `giveaways` | Int32 |
| `takeaways` | Int32 |
| `game_id` | Int32 |
| `season` | Int32 |
| `game_date` | String |

```python
load_nhl_skater_boxscores(seasons=2024)
```

## `load_nhl_team_boxscores`

Release: [nhl_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_team_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_team_boxscores/team_box_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `home_away` | String |
| `team_id` | Int32 |
| `team_abbrev` | String |
| `team_name` | String |
| `goals` | Int32 |
| `shots_on_goal` | Int32 |
| `pim` | Int32 |
| `hits` | Int32 |
| `blocked_shots` | Int32 |
| `giveaways` | Int32 |
| `takeaways` | Int32 |
| `power_play_goals` | Int32 |
| `faceoff_win_pctg` | Float64 |
| `saves` | Int32 |
| `save_pctg` | Float64 |
| `goals_against` | Int32 |

```python
load_nhl_team_boxscores(seasons=2010)
```

## `load_nhl_three_stars`

Release: [nhl_three_stars](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_three_stars) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_three_stars/three_stars_{season}.parquet`
### Returns

| col_name | type |
|---|---|
| `star` | Int32 |
| `playerId` | Int32 |
| `teamAbbrev` | String |
| `headshot` | String |
| `sweaterNo` | Int32 |
| `position` | String |
| `goals` | Int32 |
| `assists` | Int32 |
| `points` | Int32 |
| `game_id` | Int32 |
| `winner_id` | Int32 |
| `winner_name` | String |
| `loser_id` | Int32 |
| `loser_name` | String |
| `goalsAgainstAverage` | Float64 |
| `savePctg` | Float64 |
| `name.default` | String |
| `name.cs` | String |
| `name.sk` | String |
| `name.fi` | String |
| `name.sv` | String |
| `name.de` | String |
| `name.es` | String |
| `name.fr` | String |

```python
load_nhl_three_stars(seasons=2024)
```
