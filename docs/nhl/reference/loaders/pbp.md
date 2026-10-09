# NHL dataset loaders — Play-by-play

> NHL dataset loaders — Play-by-play — function reference in sdv-py, the SportsDataverse Python package.

## load_nhl_pbp

Release: [nhl_pbp_full](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_full) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_pbp_full/play_by_play_{season}.parquet`
### Returns {#load_nhl_pbp-returns}

| col_name | type | description |
|---|---|---|
| `event_type` | String | Standardized event type code. |
| `event` | String | Event description label. |
| `secondary_type` | String | Secondary event type (e.g. shot type). |
| `event_team_abbr` | String | Abbreviation of the team credited with the event. |
| `event_team_type` | String | Whether the event team is home or away. |
| `description` | String | Full text description of the event. |
| `period` | Int64 | Period number. |
| `period_type` | String | Period type (REG/OT/SO). |
| `period_time` | String | Elapsed time in the period (MM:SS). |
| `period_seconds` | Int64 | Elapsed seconds in the period. |
| `period_seconds_remaining` | Int64 | Seconds remaining in the period. |
| `period_time_remaining` | String | Time remaining in the period (MM:SS). |
| `game_seconds` | Int64 | Elapsed seconds in the game. |
| `game_seconds_remaining` | Int64 | Seconds remaining in regulation. |
| `home_score` | Int64 | Home team final score. |
| `away_score` | Int64 | Away team final score. |
| `event_player_1_name` | String | Name of the primary event player. |
| `event_player_1_type` | String | Role of the primary event player. |
| `event_player_1_id` | Int64 | Player id of the primary event player. |
| `event_player_2_name` | String | Name of the secondary event player. |
| `event_player_2_type` | String | Role of the secondary event player. |
| `event_player_2_id` | Int64 | Player id of the secondary event player. |
| `event_player_3_name` | String | Name of the tertiary event player. |
| `event_player_3_type` | String | Role of the tertiary event player. |
| `event_player_3_id` | Int64 | Player ID of the tertiary event player. |
| `event_goalie_name` | String | Name of the goalie on the event. |
| `event_goalie_id` | Int64 | Player id of the goalie on the event. |
| `penalty_severity` | String | Severity of the penalty. |
| `penalty_minutes` | Int64 | Penalty minutes. |
| `strength_state` | String | Strength state (e.g. 5v5, 5v4). |
| `strength_code` | String | Strength state code (e.g., all, even, pp, pk). |
| `strength` | String | Strength label (Even, Power Play, Shorthanded). |
| `empty_net` | Boolean | Whether the net was empty. |
| `extra_attacker` | Boolean | Whether an extra attacker was on the ice. |
| `x` | Int64 | Raw x-coordinate of the event. |
| `y` | Int64 | Raw y-coordinate of the event. |
| `x_fixed` | Int64 | Normalized x coordinate (home shoots right). |
| `y_fixed` | Int64 | Normalized y coordinate (home shoots right). |
| `shot_distance` | Float64 | Distance of the shot from the net. |
| `shot_angle` | Float64 | Angle of the shot relative to the net. |
| `home_skaters` | Int64 | Number of home skaters on the ice. |
| `away_skaters` | Int64 | Number of away skaters on the ice. |
| `home_on_1` | String | Name of home skater 1 on the ice. |
| `home_on_2` | String | Name of home skater 2 on the ice. |
| `home_on_3` | String | Name of home skater 3 on the ice. |
| `home_on_4` | String | Name of home skater 4 on the ice. |
| `home_on_5` | String | Name of home skater 5 on the ice. |
| `home_on_6` | String | Name of home skater 6 on the ice. |
| `home_on_7` | String | Name of home skater 7 on the ice. |
| `away_on_1` | String | Name of away skater 1 on the ice. |
| `away_on_2` | String | Name of away skater 2 on the ice. |
| `away_on_3` | String | Name of away skater 3 on the ice. |
| `away_on_4` | String | Name of away skater 4 on the ice. |
| `away_on_5` | String | Name of away skater 5 on the ice. |
| `away_on_6` | String | Name of away skater 6 on the ice. |
| `away_on_7` | String | Name of away skater 7 on the ice. |
| `home_goalie` | String | Name of the home goalie on the ice. |
| `away_goalie` | String | Name of the away goalie on the ice. |
| `num_on` | Int64 | Number of players coming on (line change). |
| `players_on` | String | Names of players coming on. |
| `num_off` | Int64 | Number of players going off (line change). |
| `players_off` | String | Names of players going off. |
| `game_id` | Int64 | Unique game identifier. |
| `season` | String | Season year (echoed from arg). |
| `season_type` | String | Season type code (echoed from arg). |
| `home_abbr` | String | Home team abbreviation. |
| `away_abbr` | String | Away team abbreviation. |
| `event_idx` | Int64 | Sequential event index within the game. |
| `event_id` | Int64 | ESPN event id (echoed from arg). |
| `pptReplayUrl` | String | URL to the play replay, if available. |
| `away_goalie_in` | Int64 | Whether the away goalie is on the ice (1/0). |
| `home_goalie_in` | Int64 | Whether the home goalie is on the ice (1/0). |
| `reason` | String | Reason for the event (e.g. stoppage reason). |
| `secondaryReason` | String | Secondary reason for a stoppage. |
| `ids_on` | String | Player ids coming on. |
| `ids_off` | String | Player ids going off. |
| `home_on_1_id` | Int64 | Player id of home skater 1 on the ice. |
| `away_on_1_id` | Int64 | Player id of away skater 1 on the ice. |
| `home_on_2_id` | Int64 | Player id of home skater 2 on the ice. |
| `away_on_2_id` | Int64 | Player id of away skater 2 on the ice. |
| `home_on_3_id` | Int64 | Player id of home skater 3 on the ice. |
| `away_on_3_id` | Int64 | Player id of away skater 3 on the ice. |
| `home_on_4_id` | Int64 | Player id of home skater 4 on the ice. |
| `away_on_4_id` | Int64 | Player id of away skater 4 on the ice. |
| `home_on_5_id` | Int64 | Player id of home skater 5 on the ice. |
| `away_on_5_id` | Int64 | Player id of away skater 5 on the ice. |
| `home_on_6_id` | Int64 | Player id of home skater 6 on the ice. |
| `away_on_6_id` | Int64 | Player id of away skater 6 on the ice. |
| `home_on_7_id` | Int64 | Player id of home skater 7 on the ice. |
| `away_on_7_id` | Int64 | Player id of away skater 7 on the ice. |
| `home_goalie_id` | Int64 | Player ID of the home goalie on the ice. |
| `away_goalie_id` | Int64 | Player ID of the away goalie on the ice. |
| `xg` | Float64 | Expected goals value for the shot event. |
| `game_date` | String | Game date. |

```python
load_nhl_pbp(seasons=2024)
```

## load_nhl_pbp_full

Release: [nhl_pbp_full](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_full) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_pbp_full/play_by_play_{season}.parquet`
### Returns {#load_nhl_pbp_full-returns}

| col_name | type | description |
|---|---|---|
| `event_type` | String | Standardized event type code. |
| `event` | String | Event description label. |
| `secondary_type` | String | Secondary event type (e.g. shot type). |
| `event_team_abbr` | String | Abbreviation of the team credited with the event. |
| `event_team_type` | String | Whether the event team is home or away. |
| `description` | String | Full text description of the event. |
| `period` | Int64 | Period number. |
| `period_type` | String | Period type (REG/OT/SO). |
| `period_time` | String | Elapsed time in the period (MM:SS). |
| `period_seconds` | Int64 | Elapsed seconds in the period. |
| `period_seconds_remaining` | Int64 | Seconds remaining in the period. |
| `period_time_remaining` | String | Time remaining in the period (MM:SS). |
| `game_seconds` | Int64 | Elapsed seconds in the game. |
| `game_seconds_remaining` | Int64 | Seconds remaining in regulation. |
| `home_score` | Int64 | Home team final score. |
| `away_score` | Int64 | Away team final score. |
| `event_player_1_name` | String | Name of the primary event player. |
| `event_player_1_type` | String | Role of the primary event player. |
| `event_player_1_id` | Int64 | Player id of the primary event player. |
| `event_player_2_name` | String | Name of the secondary event player. |
| `event_player_2_type` | String | Role of the secondary event player. |
| `event_player_2_id` | Int64 | Player id of the secondary event player. |
| `event_player_3_name` | String | Name of the tertiary event player. |
| `event_player_3_type` | String | Role of the tertiary event player. |
| `event_player_3_id` | Int64 | Player ID of the tertiary event player. |
| `event_goalie_name` | String | Name of the goalie on the event. |
| `event_goalie_id` | Int64 | Player id of the goalie on the event. |
| `penalty_severity` | String | Severity of the penalty. |
| `penalty_minutes` | Int64 | Penalty minutes. |
| `strength_state` | String | Strength state (e.g. 5v5, 5v4). |
| `strength_code` | String | Strength state code (e.g., all, even, pp, pk). |
| `strength` | String | Strength label (Even, Power Play, Shorthanded). |
| `empty_net` | Boolean | Whether the net was empty. |
| `extra_attacker` | Boolean | Whether an extra attacker was on the ice. |
| `x` | Int64 | Raw x-coordinate of the event. |
| `y` | Int64 | Raw y-coordinate of the event. |
| `x_fixed` | Int64 | Normalized x coordinate (home shoots right). |
| `y_fixed` | Int64 | Normalized y coordinate (home shoots right). |
| `shot_distance` | Float64 | Distance of the shot from the net. |
| `shot_angle` | Float64 | Angle of the shot relative to the net. |
| `home_skaters` | Int64 | Number of home skaters on the ice. |
| `away_skaters` | Int64 | Number of away skaters on the ice. |
| `home_on_1` | String | Name of home skater 1 on the ice. |
| `home_on_2` | String | Name of home skater 2 on the ice. |
| `home_on_3` | String | Name of home skater 3 on the ice. |
| `home_on_4` | String | Name of home skater 4 on the ice. |
| `home_on_5` | String | Name of home skater 5 on the ice. |
| `home_on_6` | String | Name of home skater 6 on the ice. |
| `home_on_7` | String | Name of home skater 7 on the ice. |
| `away_on_1` | String | Name of away skater 1 on the ice. |
| `away_on_2` | String | Name of away skater 2 on the ice. |
| `away_on_3` | String | Name of away skater 3 on the ice. |
| `away_on_4` | String | Name of away skater 4 on the ice. |
| `away_on_5` | String | Name of away skater 5 on the ice. |
| `away_on_6` | String | Name of away skater 6 on the ice. |
| `away_on_7` | String | Name of away skater 7 on the ice. |
| `home_goalie` | String | Name of the home goalie on the ice. |
| `away_goalie` | String | Name of the away goalie on the ice. |
| `num_on` | Int64 | Number of players coming on (line change). |
| `players_on` | String | Names of players coming on. |
| `num_off` | Int64 | Number of players going off (line change). |
| `players_off` | String | Names of players going off. |
| `game_id` | Int64 | Unique game identifier. |
| `season` | String | Season year (echoed from arg). |
| `season_type` | String | Season type code (echoed from arg). |
| `home_abbr` | String | Home team abbreviation. |
| `away_abbr` | String | Away team abbreviation. |
| `event_idx` | Int64 | Sequential event index within the game. |
| `event_id` | Int64 | ESPN event id (echoed from arg). |
| `pptReplayUrl` | String | URL to the play replay, if available. |
| `away_goalie_in` | Int64 | Whether the away goalie is on the ice (1/0). |
| `home_goalie_in` | Int64 | Whether the home goalie is on the ice (1/0). |
| `reason` | String | Reason for the event (e.g. stoppage reason). |
| `secondaryReason` | String | Secondary reason for a stoppage. |
| `ids_on` | String | Player ids coming on. |
| `ids_off` | String | Player ids going off. |
| `home_on_1_id` | Int64 | Player id of home skater 1 on the ice. |
| `away_on_1_id` | Int64 | Player id of away skater 1 on the ice. |
| `home_on_2_id` | Int64 | Player id of home skater 2 on the ice. |
| `away_on_2_id` | Int64 | Player id of away skater 2 on the ice. |
| `home_on_3_id` | Int64 | Player id of home skater 3 on the ice. |
| `away_on_3_id` | Int64 | Player id of away skater 3 on the ice. |
| `home_on_4_id` | Int64 | Player id of home skater 4 on the ice. |
| `away_on_4_id` | Int64 | Player id of away skater 4 on the ice. |
| `home_on_5_id` | Int64 | Player id of home skater 5 on the ice. |
| `away_on_5_id` | Int64 | Player id of away skater 5 on the ice. |
| `home_on_6_id` | Int64 | Player id of home skater 6 on the ice. |
| `away_on_6_id` | Int64 | Player id of away skater 6 on the ice. |
| `home_on_7_id` | Int64 | Player id of home skater 7 on the ice. |
| `away_on_7_id` | Int64 | Player id of away skater 7 on the ice. |
| `home_goalie_id` | Int64 | Player ID of the home goalie on the ice. |
| `away_goalie_id` | Int64 | Player ID of the away goalie on the ice. |
| `xg` | Float64 | Expected goals value for the shot event. |
| `game_date` | String | Game date. |

```python
load_nhl_pbp_full(seasons=2010)
```

## load_nhl_pbp_lite

Release: [nhl_pbp_lite](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nhl_pbp_lite) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_pbp_lite/play_by_play_{season}_lite.parquet`
### Returns {#load_nhl_pbp_lite-returns}

| col_name | type | description |
|---|---|---|
| `event_type` | String | Standardized event type code. |
| `event` | String | Event description label. |
| `secondary_type` | String | Secondary event type (e.g. shot type). |
| `event_team_abbr` | String | Abbreviation of the team credited with the event. |
| `event_team_type` | String | Whether the event team is home or away. |
| `description` | String | Full text description of the event. |
| `period` | Int32 | Period number. |
| `period_type` | String | Period type (REG/OT/SO). |
| `period_time` | String | Elapsed time in the period (MM:SS). |
| `period_seconds` | Int32 | Elapsed seconds in the period. |
| `period_seconds_remaining` | Int32 | Seconds remaining in the period. |
| `period_time_remaining` | String | Time remaining in the period (MM:SS). |
| `game_seconds` | Int32 | Elapsed seconds in the game. |
| `game_seconds_remaining` | Int32 | Seconds remaining in regulation. |
| `home_score` | Int32 | Home team final score. |
| `away_score` | Int32 | Away team final score. |
| `event_player_1_name` | String | Name of the primary event player. |
| `event_player_1_type` | String | Role of the primary event player. |
| `event_player_1_id` | Int32 | Player id of the primary event player. |
| `event_player_2_name` | String | Name of the secondary event player. |
| `event_player_2_type` | String | Role of the secondary event player. |
| `event_player_2_id` | Int32 | Player id of the secondary event player. |
| `event_player_3_name` | String | Name of the tertiary event player. |
| `event_player_3_type` | String | Role of the tertiary event player. |
| `event_player_3_id` | Int32 | Player ID of the tertiary event player. |
| `event_goalie_name` | String | Name of the goalie on the event. |
| `event_goalie_id` | Int32 | Player id of the goalie on the event. |
| `penalty_severity` | String | Severity of the penalty. |
| `penalty_minutes` | Int32 | Penalty minutes. |
| `strength_state` | String | Strength state (e.g. 5v5, 5v4). |
| `strength_code` | String | Strength state code (e.g., all, even, pp, pk). |
| `strength` | String | Strength label (Even, Power Play, Shorthanded). |
| `empty_net` | Boolean | Whether the net was empty. |
| `extra_attacker` | Boolean | Whether an extra attacker was on the ice. |
| `x` | Int32 | Raw x-coordinate of the event. |
| `y` | Int32 | Raw y-coordinate of the event. |
| `x_fixed` | Int32 | Normalized x coordinate (home shoots right). |
| `y_fixed` | Int32 | Normalized y coordinate (home shoots right). |
| `shot_distance` | Float64 | Distance of the shot from the net. |
| `shot_angle` | Float64 | Angle of the shot relative to the net. |
| `home_skaters` | Int32 | Number of home skaters on the ice. |
| `away_skaters` | Int32 | Number of away skaters on the ice. |
| `home_on_1` | String | Name of home skater 1 on the ice. |
| `home_on_2` | String | Name of home skater 2 on the ice. |
| `home_on_3` | String | Name of home skater 3 on the ice. |
| `home_on_4` | String | Name of home skater 4 on the ice. |
| `home_on_5` | String | Name of home skater 5 on the ice. |
| `home_on_6` | String | Name of home skater 6 on the ice. |
| `home_on_7` | String | Name of home skater 7 on the ice. |
| `away_on_1` | String | Name of away skater 1 on the ice. |
| `away_on_2` | String | Name of away skater 2 on the ice. |
| `away_on_3` | String | Name of away skater 3 on the ice. |
| `away_on_4` | String | Name of away skater 4 on the ice. |
| `away_on_5` | String | Name of away skater 5 on the ice. |
| `away_on_6` | String | Name of away skater 6 on the ice. |
| `away_on_7` | String | Name of away skater 7 on the ice. |
| `home_goalie` | String | Name of the home goalie on the ice. |
| `away_goalie` | String | Name of the away goalie on the ice. |
| `num_on` | Int32 | Number of players coming on (line change). |
| `players_on` | String | Names of players coming on. |
| `num_off` | Int32 | Number of players going off (line change). |
| `players_off` | String | Names of players going off. |
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season year (echoed from arg). |
| `season_type` | String | Season type code (echoed from arg). |
| `home_abbr` | String | Home team abbreviation. |
| `away_abbr` | String | Away team abbreviation. |
| `event_idx` | Int32 | Sequential event index within the game. |
| `event_id` | Int32 | ESPN event id (echoed from arg). |
| `pptReplayUrl` | String | URL to the play replay, if available. |
| `away_goalie_in` | Int32 | Whether the away goalie is on the ice (1/0). |
| `home_goalie_in` | Int32 | Whether the home goalie is on the ice (1/0). |
| `reason` | String | Reason for the event (e.g. stoppage reason). |
| `secondaryReason` | String | Secondary reason for a stoppage. |
| `ids_on` | String | Player ids coming on. |
| `ids_off` | String | Player ids going off. |
| `home_on_1_id` | Int32 | Player id of home skater 1 on the ice. |
| `away_on_1_id` | Int32 | Player id of away skater 1 on the ice. |
| `home_on_2_id` | Int32 | Player id of home skater 2 on the ice. |
| `away_on_2_id` | Int32 | Player id of away skater 2 on the ice. |
| `home_on_3_id` | Int32 | Player id of home skater 3 on the ice. |
| `away_on_3_id` | Int32 | Player id of away skater 3 on the ice. |
| `home_on_4_id` | Int32 | Player id of home skater 4 on the ice. |
| `away_on_4_id` | Int32 | Player id of away skater 4 on the ice. |
| `home_on_5_id` | Int32 | Player id of home skater 5 on the ice. |
| `away_on_5_id` | Int32 | Player id of away skater 5 on the ice. |
| `home_on_6_id` | Int32 | Player id of home skater 6 on the ice. |
| `away_on_6_id` | Int32 | Player id of away skater 6 on the ice. |
| `home_on_7_id` | Int32 | Player id of home skater 7 on the ice. |
| `away_on_7_id` | Int32 | Player id of away skater 7 on the ice. |
| `home_goalie_id` | Int32 | Player ID of the home goalie on the ice. |
| `away_goalie_id` | Int32 | Player ID of the away goalie on the ice. |
| `xg` | Float64 | Expected goals value for the shot event. |

```python
load_nhl_pbp_lite(seasons=2010)
```
