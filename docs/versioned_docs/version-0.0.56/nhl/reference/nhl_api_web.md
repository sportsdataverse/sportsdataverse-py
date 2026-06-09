---
title: NHL — NHL Web API
sidebar_label: NHL Web API
sidebar_position: 10
---
# NHL — NHL Web API

`sportsdataverse.nhl` — 27 endpoints.

## `nhl_web_pbp`

Pull the play-by-play feed for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play](https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `event_id` | integer | ESPN event id (echoed from arg). |
| `time_in_period` | character | Time elapsed in the period when the shot occurred. |
| `time_remaining` | character | Time remaining. |
| `situation_code` | character | Code identifying the game situation. |
| `home_team_defending_side` | character |  |
| `type_code` | integer |  |
| `type_desc_key` | character |  |
| `sort_order` | integer | Display sort order for the sport. |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `details_event_owner_team_id` | double |  |
| `details_losing_player_id` | double |  |
| `details_winning_player_id` | double |  |
| `details_x_coord` | double |  |
| `details_y_coord` | double |  |
| `details_zone_code` | character |  |
| `details_shot_type` | character |  |
| `details_shooting_player_id` | double |  |
| `details_goalie_in_net_id` | double |  |
| `details_away_sog` | double |  |
| `details_home_sog` | double |  |
| `details_reason` | character |  |
| `details_blocking_player_id` | double |  |
| `details_hitting_player_id` | double |  |
| `details_hittee_player_id` | double |  |
| `details_player_id` | double |  |
| `details_type_code` | character |  |
| `details_desc_key` | character |  |
| `details_duration` | double |  |
| `details_committed_by_player_id` | double |  |
| `details_drawn_by_player_id` | double |  |
| `ppt_replay_url` | character |  |
| `details_scoring_player_id` | double |  |
| `details_scoring_player_total` | double |  |
| `details_assist1_player_id` | double |  |
| `details_assist1_player_total` | double |  |
| `details_assist2_player_id` | double |  |
| `details_assist2_player_total` | double |  |
| `details_away_score` | double |  |
| `details_home_score` | double |  |
| `details_highlight_clip_sharing_url` | character |  |
| `details_highlight_clip` | double |  |
| `details_discrete_clip` | double |  |
| `details_discrete_clip_fr` | double |  |
| `details_highlight_clip_sharing_url_fr` | character |  |
| `details_highlight_clip_fr` | double |  |
| `details_secondary_reason` | character |  |

### Example

```python
nhl_web_pbp(game_id=2024020001)
```

_Last validated n/a._

## `nhl_boxscore`

Pull the boxscore for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore](https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `home_away` | character | Home or away indicator. |
| `position_group` | character | Position group name (e.g. Centers). |
| `player_id` | integer | Unique player identifier. |
| `sweater_number` | integer | Jersey number. |
| `position` | character | Player position. |
| `goals` | double | Goals scored. |
| `assists` | double | Assists. |
| `points` | double | Total points (goals + assists). |
| `plus_minus` | double | Plus/minus rating. |
| `pim` | integer | Penalty minutes. |
| `hits` | double | Hits. |
| `power_play_goals` | double | Power-play goals. |
| `sog` | double | Shots on goal from the area. |
| `faceoff_winning_pctg` | double | Faceoff win percentage. |
| `toi` | character | Time on ice. |
| `blocked_shots` | double | Blocked shots. |
| `shifts` | double | Number of shifts. |
| `giveaways` | double | Giveaways. |
| `takeaways` | double | Takeaways. |
| `name_default` | character | Player name (default localization). |
| `even_strength_shots_against` | character | Even-strength shots against (saves/total). |
| `power_play_shots_against` | character | Power-play shots against (saves/total). |
| `shorthanded_shots_against` | character | Shorthanded shots against (saves/total). |
| `save_shots_against` | character | Total shots against (saves/total). |
| `even_strength_goals_against` | double | Even-strength goals against. |
| `power_play_goals_against` | double | Power-play goals against. |
| `shorthanded_goals_against` | double | Shorthanded goals against. |
| `goals_against` | double | Goals against. |
| `starter` | logical | Whether the goalie started the game. |
| `shots_against` | double | Shots faced. |
| `saves` | double | Saves made. |
| `save_pctg` | double | Save percentage. |
| `decision` | character | Goalie decision (W/L/O). |
| `name_cs` | character | Player name (Czech localization). |
| `name_fi` | character | Player name (Finnish localization). |
| `name_sk` | character | Player name (Slovak localization). |

### Example

```python
nhl_boxscore(game_id=2024020001)
```

_Last validated n/a._

## `nhl_landing`

Pull the gamecenter landing payload for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/landing](https://api-web.nhle.com/v1/gamecenter/2024020001/landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `limited_scoring` | logical |  |
| `game_date` | character | Game date. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `venue_timezone` | character | Venue time zone. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `shootout_in_use` | logical |  |
| `reg_periods` | integer |  |
| `ot_in_use` | logical |  |
| `ties_in_use` | logical | Whether ties were in use that season. |
| `venue_default` | character | Venue name (default language). |
| `venue_location_default` | character |  |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `away_team_id` | integer | Away team identifier. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_place_name_default` | character | Away team place name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_score` | integer | Away team final score. |
| `away_team_sog` | integer | Away team shots on goal. |
| `away_team_logo` | character | URL to the away team logo. |
| `away_team_dark_logo` | character | URL to the away team dark logo. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_place_name_default` | character | Home team place name (default language). |
| `home_team_place_name_fr` | character | Home team place name (French). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_score` | integer | Home team final score. |
| `home_team_sog` | integer | Home team shots on goal. |
| `home_team_logo` | character | URL to the home team logo. |
| `home_team_dark_logo` | character | URL to the home team dark logo. |
| `summary_scoring` | character |  |
| `summary_three_stars` | character |  |
| `summary_penalties` | character |  |
| `clock_time_remaining` | character |  |
| `clock_seconds_remaining` | integer |  |
| `clock_running` | logical |  |
| `clock_in_intermission` | logical |  |

### Example

```python
nhl_landing(game_id=2024020001)
```

_Last validated n/a._

## `nhl_right_rail`

Pull the gamecenter right-rail payload (in-game widgets).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/right-rail`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail](https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns

Pull the gamecenter right-rail payload (in-game widgets).

### Example

```python
nhl_right_rail(game_id=2024020001)
```

_Last validated n/a._

## `nhl_web_schedule`

Pull the week-of NHL schedule rooted at `date`.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule](https://api-web.nhle.com/v1/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `schedule_date` | character |  |
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `neutral_site` | logical | Whether the game is at a neutral site. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `venue_timezone` | character | Venue time zone. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `series_url` | character |  |
| `three_min_recap` | character | Link to the three-minute recap. |
| `game_center_link` | character | Link to the NHL game center page. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_place_name_default` | character | Away team place name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_logo` | character | URL to the away team logo. |
| `away_team_dark_logo` | character | URL to the away team dark logo. |
| `away_team_away_split_squad` | logical | Whether the away team is a split squad. |
| `away_team_score` | integer | Away team final score. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_default` | character | Home team place name (default language). |
| `home_team_place_name_fr` | character | Home team place name (French). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_logo` | character | URL to the home team logo. |
| `home_team_dark_logo` | character | URL to the home team dark logo. |
| `home_team_home_split_squad` | logical | Whether the home team is a split squad. |
| `home_team_score` | integer | Home team final score. |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | character | Period type in which the game ended. |
| `winning_goalie_player_id` | integer | Winning goalie player identifier. |
| `winning_goalie_first_initial_default` | character | Winning goalie first initial (default language). |
| `winning_goalie_last_name_default` | character | Winning goalie last name (default language). |
| `winning_goalie_last_name_cs` | character | Winning goalie last name (Czech). |
| `winning_goalie_last_name_fi` | character | Winning goalie last name (Finnish). |
| `winning_goalie_last_name_sk` | character | Winning goalie last name (Slovak). |
| `winning_goal_scorer_player_id` | integer | Winning goal scorer player identifier. |
| `winning_goal_scorer_first_initial_default` | character | Winning goal scorer first initial (default). |
| `winning_goal_scorer_last_name_default` | character | Winning goal scorer last name (default language). |
| `series_status_round` | integer |  |
| `series_status_series_abbrev` | character |  |
| `series_status_series_title` | character |  |
| `series_status_series_letter` | character |  |
| `series_status_needed_to_win` | integer |  |
| `series_status_top_seed_team_abbrev` | character |  |
| `series_status_top_seed_wins` | integer |  |
| `series_status_bottom_seed_team_abbrev` | character |  |
| `series_status_bottom_seed_wins` | integer |  |
| `series_status_game_number_of_series` | integer |  |

### Example

```python
nhl_web_schedule()
```

_Last validated n/a._

## `nhl_score`

Pull the single-day scoreboard for `date`.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/score/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/score](https://api-web.nhle.com/v1/score)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `game_center_link` | character | Link to the NHL game center page. |
| `series_url` | character |  |
| `three_min_recap` | character | Link to the three-minute recap. |
| `neutral_site` | logical | Whether the game is at a neutral site. |
| `venue_timezone` | character | Venue time zone. |
| `period` | integer | Period number. |
| `goals` | character | Goals scored. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_name_default` | character |  |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_score` | integer | Away team final score. |
| `away_team_sog` | integer | Away team shots on goal. |
| `away_team_logo` | character | URL to the away team logo. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_name_default` | character |  |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_score` | integer | Home team final score. |
| `home_team_sog` | integer | Home team shots on goal. |
| `home_team_logo` | character | URL to the home team logo. |
| `series_status_round` | integer |  |
| `series_status_series_abbrev` | character |  |
| `series_status_series_title` | character |  |
| `series_status_series_letter` | character |  |
| `series_status_needed_to_win` | integer |  |
| `series_status_top_seed_team_abbrev` | character |  |
| `series_status_top_seed_wins` | integer |  |
| `series_status_bottom_seed_team_abbrev` | character |  |
| `series_status_bottom_seed_wins` | integer |  |
| `series_status_game_number_of_series` | integer |  |
| `clock_time_remaining` | character |  |
| `clock_seconds_remaining` | integer |  |
| `clock_running` | logical |  |
| `clock_in_intermission` | logical |  |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | character | Period type in which the game ended. |

### Example

```python
nhl_score()
```

_Last validated n/a._

## `nhl_schedule_calendar`

Pull the calendar of game-days for the season.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule-calendar/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule-calendar](https://api-web.nhle.com/v1/schedule-calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

Pull the calendar of game-days for the season.

### Example

```python
nhl_schedule_calendar()
```

_Last validated n/a._

## `nhl_playoff_series`

Pull a single playoff series payload.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/playoff-series/{season}/{series_letter}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule/playoff-series/2025/a](https://api-web.nhle.com/v1/schedule/playoff-series/2025/a)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `series_letter` | `series_letter` |  | `Y` |  | series_letter path parameter. |

### Returns

Pull a single playoff series payload.

### Example

```python
nhl_playoff_series(season=2025, series_letter='a')
```

_Last validated n/a._

## `nhl_standings`

Pull the NHL standings.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/standings](https://api-web.nhle.com/v1/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `clinch_indicator` | character | Playoff clinch indicator (e.g. 'x' clinched playoff, 'e' eliminated). |
| `conference_abbrev` | character | Conference abbreviation. |
| `conference_home_sequence` | integer |  |
| `conference_l10_sequence` | integer |  |
| `conference_name` | character | Conference name. |
| `conference_road_sequence` | integer |  |
| `conference_sequence` | integer | Team's seeding position within the conference. |
| `date` | character | Game date (ISO 8601 datetime string). |
| `division_abbrev` | character | Division abbreviation. |
| `division_home_sequence` | integer |  |
| `division_l10_sequence` | integer |  |
| `division_name` | character | Division name. |
| `division_road_sequence` | integer |  |
| `division_sequence` | integer | Team's seeding position within the division. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Games played. |
| `goal_differential` | integer | Goal differential. |
| `goal_differential_pctg` | double |  |
| `goal_against` | integer |  |
| `goal_for` | integer |  |
| `goals_for_pctg` | double |  |
| `home_games_played` | integer |  |
| `home_goal_differential` | integer |  |
| `home_goals_against` | integer |  |
| `home_goals_for` | integer |  |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | integer | Home overtime losses. |
| `home_points` | integer | Home team total points scored in the game so far. |
| `home_regulation_plus_ot_wins` | integer |  |
| `home_regulation_wins` | integer |  |
| `home_ties` | integer | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `l10_games_played` | integer |  |
| `l10_goal_differential` | integer |  |
| `l10_goals_against` | integer |  |
| `l10_goals_for` | integer |  |
| `l10_losses` | integer | Losses in the last ten games. |
| `l10_ot_losses` | integer | Overtime losses in the last ten games. |
| `l10_points` | integer |  |
| `l10_regulation_plus_ot_wins` | integer |  |
| `l10_regulation_wins` | integer |  |
| `l10_ties` | integer |  |
| `l10_wins` | integer | Wins in the last ten games. |
| `league_home_sequence` | integer |  |
| `league_l10_sequence` | integer |  |
| `league_road_sequence` | integer |  |
| `league_sequence` | integer | Team's seeding position within the league. |
| `losses` | integer | Losses. |
| `ot_losses` | integer | Overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `regulation_plus_ot_win_pctg` | double |  |
| `regulation_plus_ot_wins` | integer | Wins in regulation plus overtime. |
| `regulation_win_pctg` | double |  |
| `regulation_wins` | integer | Wins in regulation. |
| `road_games_played` | integer |  |
| `road_goal_differential` | integer |  |
| `road_goals_against` | integer |  |
| `road_goals_for` | integer |  |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | integer | Road overtime losses. |
| `road_points` | integer |  |
| `road_regulation_plus_ot_wins` | integer |  |
| `road_regulation_wins` | integer |  |
| `road_ties` | integer | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `season_id` | integer | Season identifier. |
| `shootout_losses` | integer | Shootout losses. |
| `shootout_wins` | integer | Shootout wins. |
| `streak_code` | character | Current streak code (W/L/OT). |
| `streak_count` | integer | Length of the current streak. |
| `team_logo` | character | URL to the team logo image. |
| `ties` | integer | Total ties. |
| `waivers_sequence` | integer |  |
| `wildcard_sequence` | integer | Team's wild card seeding position. |
| `win_pctg` | double |  |
| `wins` | integer | Wins. |
| `place_name_default` | character |  |
| `team_name_default` | character | Team name (default locale). |
| `team_name_fr` | character | Team name (French locale). |
| `team_common_name_default` | character | Team common name (default language). |
| `team_abbrev_default` | character |  |
| `place_name_fr` | character |  |
| `team_common_name_fr` | character | Team common name (French localization). |

### Example

```python
nhl_standings()
```

_Last validated n/a._

## `nhl_standings_season`

Pull the per-season standings cutover dates.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings-season`

**Valid URL:** [https://api-web.nhle.com/v1/standings-season](https://api-web.nhle.com/v1/standings-season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `conferences_in_use` | logical | Whether conferences were in use that season. |
| `divisions_in_use` | logical | Whether divisions were in use that season. |
| `point_for_o_tloss_in_use` | logical | Whether a point for overtime losses was in use. |
| `regulation_wins_in_use` | logical | Whether regulation wins were tracked. |
| `row_in_use` | logical | Whether the regulation/overtime/shootout format was in use. |
| `standings_end` | character | End date of the standings period. |
| `standings_start` | character | Start date of the standings period. |
| `ties_in_use` | logical | Whether ties were in use that season. |
| `wildcard_in_use` | logical | Whether the wild-card playoff format was in use this season. |

### Example

```python
nhl_standings_season()
```

_Last validated n/a._

## `nhl_club_schedule_season`

Pull a team's full-season schedule.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule-season/TOR](https://api-web.nhle.com/v1/club-schedule-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `club_previous_season` | integer |  |
| `club_current_season` | integer |  |
| `club_next_season` | integer |  |
| `club_timezone` | character |  |
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `neutral_site` | logical | Whether the game is at a neutral site. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `venue_timezone` | character | Venue time zone. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_center_link` | character | Link to the NHL game center page. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_place_name_default` | character | Away team place name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_logo` | character | URL to the away team logo. |
| `away_team_dark_logo` | character | URL to the away team dark logo. |
| `away_team_away_split_squad` | logical | Whether the away team is a split squad. |
| `away_team_score` | integer | Away team final score. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_default` | character | Home team place name (default language). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_logo` | character | URL to the home team logo. |
| `home_team_dark_logo` | character | URL to the home team dark logo. |
| `home_team_home_split_squad` | logical | Whether the home team is a split squad. |
| `home_team_airline_link` | character | Link to home team airline info. |
| `home_team_airline_desc` | character | Home team airline description. |
| `home_team_hotel_link` | character | Link to home team hotel info. |
| `home_team_hotel_desc` | character | Home team hotel description. |
| `home_team_score` | integer | Home team final score. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | character | Period type in which the game ended. |
| `winning_goalie_player_id` | integer | Winning goalie player identifier. |
| `winning_goalie_first_initial_default` | character | Winning goalie first initial (default language). |
| `winning_goalie_last_name_default` | character | Winning goalie last name (default language). |
| `away_team_airline_link` | character | Link to away team airline info. |
| `away_team_airline_desc` | character | Away team airline description. |
| `winning_goal_scorer_player_id` | double | Winning goal scorer player identifier. |
| `winning_goal_scorer_first_initial_default` | character | Winning goal scorer first initial (default). |
| `winning_goal_scorer_last_name_default` | character | Winning goal scorer last name (default language). |
| `three_min_recap` | character | Link to the three-minute recap. |
| `home_team_place_name_fr` | character | Home team place name (French). |
| `condensed_game` | character | Link to the condensed game video. |
| `venue_es` | character | Venue name (Spanish). |
| `venue_fr` | character | Venue name (French). |
| `special_event_parent_id` | double |  |
| `special_event_name_default` | character |  |
| `special_event_name_fr` | character |  |
| `away_team_hotel_link` | character | Link to away team hotel info. |
| `away_team_hotel_desc` | character | Away team hotel description. |
| `three_min_recap_fr` | character | Link to the French three-minute recap. |
| `winning_goalie_last_name_cs` | character | Winning goalie last name (Czech). |
| `winning_goalie_last_name_fi` | character | Winning goalie last name (Finnish). |
| `winning_goalie_last_name_sk` | character | Winning goalie last name (Slovak). |
| `away_team_place_name_fr` | character | Away team place name (French). |
| `away_team_common_name_fr` | character | Away team common name (French). |
| `home_team_common_name_fr` | character | Home team common name (French). |
| `series_url` | character |  |
| `series_status_round` | double |  |
| `series_status_series_abbrev` | character |  |
| `series_status_series_title` | character |  |
| `series_status_series_letter` | character |  |
| `series_status_needed_to_win` | double |  |
| `series_status_top_seed_wins` | double |  |
| `series_status_bottom_seed_wins` | double |  |
| `series_status_game_number_of_series` | double |  |

### Example

```python
nhl_club_schedule_season(team='TOR')
```

_Last validated n/a._

## `nhl_club_schedule_month`

Pull a team's schedule for one month.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/month/{month}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/month](https://api-web.nhle.com/v1/club-schedule/TOR/month)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `month` | `month` |  |  | `Y` | month path parameter. |

### Returns

Pull a team's schedule for one month.

### Example

```python
nhl_club_schedule_month(team='TOR')
```

_Last validated n/a._

## `nhl_club_schedule_week`

Pull a team's schedule for one week.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/week/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/week](https://api-web.nhle.com/v1/club-schedule/TOR/week)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns

Pull a team's schedule for one week.

### Example

```python
nhl_club_schedule_week(team='TOR')
```

_Last validated n/a._

## `nhl_club_stats`

Pull a team's season stat block.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats/{team}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats/TOR](https://api-web.nhle.com/v1/club-stats/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull a team's season stat block.

### Example

```python
nhl_club_stats(team='TOR')
```

_Last validated n/a._

## `nhl_club_stats_season`

Pull the seasons a team has stats for.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats-season/TOR](https://api-web.nhle.com/v1/club-stats-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |

### Returns

Pull the seasons a team has stats for.

### Example

```python
nhl_club_stats_season(team='TOR')
```

_Last validated n/a._

## `nhl_roster`

Pull a team's roster.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/roster/TOR](https://api-web.nhle.com/v1/roster/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `position_group` | character | Position group name (e.g. Centers). |
| `id` | integer | Unique player identifier. |
| `headshot` | character | URL to the player headshot image. |
| `sweater_number` | integer | Jersey number. |
| `position_code` | character | Player position code. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `height_in_inches` | integer | Height in inches. |
| `weight_in_pounds` | integer | Weight in pounds. |
| `height_in_centimeters` | integer | Height in centimeters. |
| `weight_in_kilograms` | integer | Weight in kilograms. |
| `birth_date` | character | Player birth date. |
| `birth_country` | character | Player birth country. |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |
| `birth_city_default` | character | Birth city (default localization). |
| `birth_state_province_default` | character | Birth state/province (default localization). |
| `birth_city_cs` | character | Birth city (Czech localization). |
| `birth_city_de` | character | Birth city (German localization). |
| `birth_city_fi` | character | Birth city (Finnish localization). |
| `birth_city_sk` | character | Birth city (Slovak localization). |
| `birth_city_sv` | character | Birth city (Swedish localization). |

### Example

```python
nhl_roster(team='TOR')
```

_Last validated n/a._

## `nhl_roster_season`

Pull every season a team has had on file.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/roster-season/TOR](https://api-web.nhle.com/v1/roster-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |

### Returns

Pull every season a team has had on file.

### Example

```python
nhl_roster_season(team='TOR')
```

_Last validated n/a._

## `nhl_player_landing`

Pull the player profile / overview.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player/{player_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/player/8480801/landing](https://api-web.nhle.com/v1/player/8480801/landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `is_active` | logical | Whether the team is active. |
| `current_team_id` | integer | Player's current team identifier. |
| `current_team_abbrev` | character |  |
| `badges` | character |  |
| `team_logo` | character | URL to the team logo image. |
| `sweater_number` | integer | Jersey number. |
| `position` | character | Player position. |
| `headshot` | character | URL to the player headshot image. |
| `hero_image` | character |  |
| `height_in_inches` | integer | Height in inches. |
| `height_in_centimeters` | integer | Height in centimeters. |
| `weight_in_pounds` | integer | Weight in pounds. |
| `weight_in_kilograms` | integer | Weight in kilograms. |
| `birth_date` | character | Player birth date. |
| `birth_country` | character | Player birth country. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `player_slug` | character | URL slug for the player. |
| `in_top100_all_time` | integer |  |
| `in_hhof` | integer |  |
| `shop_link` | character |  |
| `twitter_link` | character |  |
| `watch_link` | character |  |
| `last5_games` | character |  |
| `season_totals` | character |  |
| `awards` | character |  |
| `current_team_roster` | character |  |
| `full_team_name_default` | character |  |
| `full_team_name_fr` | character |  |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |
| `birth_city_default` | character | Birth city (default localization). |
| `birth_state_province_default` | character | Birth state/province (default localization). |
| `draft_details_year` | integer |  |
| `draft_details_team_abbrev` | character |  |
| `draft_details_round` | integer |  |
| `draft_details_pick_in_round` | integer |  |
| `draft_details_overall_pick` | integer |  |
| `featured_stats_season` | integer |  |
| `featured_stats_regular_season_sub_season_assists` | integer |  |
| `featured_stats_regular_season_sub_season_game_winning_goals` | integer |  |
| `featured_stats_regular_season_sub_season_games_played` | integer |  |
| `featured_stats_regular_season_sub_season_goals` | integer |  |
| `featured_stats_regular_season_sub_season_ot_goals` | integer |  |
| `featured_stats_regular_season_sub_season_pim` | integer |  |
| `featured_stats_regular_season_sub_season_plus_minus` | integer |  |
| `featured_stats_regular_season_sub_season_points` | integer |  |
| `featured_stats_regular_season_sub_season_power_play_goals` | integer |  |
| `featured_stats_regular_season_sub_season_power_play_points` | integer |  |
| `featured_stats_regular_season_sub_season_shooting_pctg` | double |  |
| `featured_stats_regular_season_sub_season_shorthanded_goals` | integer |  |
| `featured_stats_regular_season_sub_season_shorthanded_points` | integer |  |
| `featured_stats_regular_season_sub_season_shots` | integer |  |
| `featured_stats_regular_season_career_assists` | integer |  |
| `featured_stats_regular_season_career_game_winning_goals` | integer |  |
| `featured_stats_regular_season_career_games_played` | integer |  |
| `featured_stats_regular_season_career_goals` | integer |  |
| `featured_stats_regular_season_career_ot_goals` | integer |  |
| `featured_stats_regular_season_career_pim` | integer |  |
| `featured_stats_regular_season_career_plus_minus` | integer |  |
| `featured_stats_regular_season_career_points` | integer |  |
| `featured_stats_regular_season_career_power_play_goals` | integer |  |
| `featured_stats_regular_season_career_power_play_points` | integer |  |
| `featured_stats_regular_season_career_shooting_pctg` | double |  |
| `featured_stats_regular_season_career_shorthanded_goals` | integer |  |
| `featured_stats_regular_season_career_shorthanded_points` | integer |  |
| `featured_stats_regular_season_career_shots` | integer |  |
| `featured_stats_playoffs_sub_season_assists` | integer |  |
| `featured_stats_playoffs_sub_season_game_winning_goals` | integer |  |
| `featured_stats_playoffs_sub_season_games_played` | integer |  |
| `featured_stats_playoffs_sub_season_goals` | integer |  |
| `featured_stats_playoffs_sub_season_ot_goals` | integer |  |
| `featured_stats_playoffs_sub_season_pim` | integer |  |
| `featured_stats_playoffs_sub_season_plus_minus` | integer |  |
| `featured_stats_playoffs_sub_season_points` | integer |  |
| `featured_stats_playoffs_sub_season_power_play_goals` | integer |  |
| `featured_stats_playoffs_sub_season_power_play_points` | integer |  |
| `featured_stats_playoffs_sub_season_shooting_pctg` | double |  |
| `featured_stats_playoffs_sub_season_shorthanded_goals` | integer |  |
| `featured_stats_playoffs_sub_season_shorthanded_points` | integer |  |
| `featured_stats_playoffs_sub_season_shots` | integer |  |
| `featured_stats_playoffs_career_assists` | integer |  |
| `featured_stats_playoffs_career_game_winning_goals` | integer |  |
| `featured_stats_playoffs_career_games_played` | integer |  |
| `featured_stats_playoffs_career_goals` | integer |  |
| `featured_stats_playoffs_career_ot_goals` | integer |  |
| `featured_stats_playoffs_career_pim` | integer |  |
| `featured_stats_playoffs_career_plus_minus` | integer |  |
| `featured_stats_playoffs_career_points` | integer |  |
| `featured_stats_playoffs_career_power_play_goals` | integer |  |
| `featured_stats_playoffs_career_power_play_points` | integer |  |
| `featured_stats_playoffs_career_shooting_pctg` | double |  |
| `featured_stats_playoffs_career_shorthanded_goals` | integer |  |
| `featured_stats_playoffs_career_shorthanded_points` | integer |  |
| `featured_stats_playoffs_career_shots` | integer |  |
| `career_totals_regular_season_assists` | integer |  |
| `career_totals_regular_season_avg_toi` | character |  |
| `career_totals_regular_season_faceoff_winning_pctg` | double |  |
| `career_totals_regular_season_game_winning_goals` | integer |  |
| `career_totals_regular_season_games_played` | integer |  |
| `career_totals_regular_season_goals` | integer |  |
| `career_totals_regular_season_ot_goals` | integer |  |
| `career_totals_regular_season_pim` | integer |  |
| `career_totals_regular_season_plus_minus` | integer |  |
| `career_totals_regular_season_points` | integer |  |
| `career_totals_regular_season_power_play_goals` | integer |  |
| `career_totals_regular_season_power_play_points` | integer |  |
| `career_totals_regular_season_shooting_pctg` | double |  |
| `career_totals_regular_season_shorthanded_goals` | integer |  |
| `career_totals_regular_season_shorthanded_points` | integer |  |
| `career_totals_regular_season_shots` | integer |  |
| `career_totals_playoffs_assists` | integer |  |
| `career_totals_playoffs_avg_toi` | character |  |
| `career_totals_playoffs_faceoff_winning_pctg` | double |  |
| `career_totals_playoffs_game_winning_goals` | integer |  |
| `career_totals_playoffs_games_played` | integer |  |
| `career_totals_playoffs_goals` | integer |  |
| `career_totals_playoffs_ot_goals` | integer |  |
| `career_totals_playoffs_pim` | integer |  |
| `career_totals_playoffs_plus_minus` | integer |  |
| `career_totals_playoffs_points` | integer |  |
| `career_totals_playoffs_power_play_goals` | integer |  |
| `career_totals_playoffs_power_play_points` | integer |  |
| `career_totals_playoffs_shooting_pctg` | double |  |
| `career_totals_playoffs_shorthanded_goals` | integer |  |
| `career_totals_playoffs_shorthanded_points` | integer |  |
| `career_totals_playoffs_shots` | integer |  |

### Example

```python
nhl_player_landing(player_id=8480801)
```

_Last validated n/a._

## `nhl_player_game_log`

Pull a player's game-by-game log.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player/{player_id}/game-log/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/player/8480801/game-log](https://api-web.nhle.com/v1/player/8480801/game-log)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `team_abbrev` | character | Team abbreviation. |
| `home_road_flag` | character | Home or road indicator. |
| `game_date` | character | Game date. |
| `goals` | integer | Goals scored. |
| `assists` | integer | Assists. |
| `points` | integer | Total points (goals + assists). |
| `plus_minus` | integer | Plus/minus rating. |
| `power_play_goals` | integer | Power-play goals. |
| `power_play_points` | integer | Power play points. |
| `game_winning_goals` | integer | Game-winning goals. |
| `ot_goals` | integer | Overtime goals. |
| `shots` | integer | Shots on goal. |
| `shifts` | integer | Number of shifts. |
| `shorthanded_goals` | integer | Shorthanded goals. |
| `shorthanded_points` | integer | Shorthanded points. |
| `opponent_abbrev` | character | Opponent team abbreviation. |
| `pim` | integer | Penalty minutes. |
| `toi` | character | Time on ice. |
| `common_name_default` | character | Player's team common name. |
| `opponent_common_name_default` | character | Opponent team common name. |
| `opponent_common_name_fr` | character |  |

### Example

```python
nhl_player_game_log(player_id=8480801)
```

_Last validated n/a._

## `nhl_player_spotlight`

Pull the league's currently featured players.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/player-spotlight`

**Valid URL:** [https://api-web.nhle.com/v1/player-spotlight](https://api-web.nhle.com/v1/player-spotlight)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Pull the league's currently featured players.

### Example

```python
nhl_player_spotlight()
```

_Last validated n/a._

## `nhl_skater_leaders`

Pull skater stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/skater-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/skater-stats-leaders](https://api-web.nhle.com/v1/skater-stats-leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `category` | character | Stat leader category. |
| `id` | integer | Unique player identifier. |
| `sweater_number` | integer | Jersey number. |
| `headshot` | character | URL to the player headshot image. |
| `team_abbrev` | character | Team abbreviation. |
| `team_logo` | character | URL to the team logo image. |
| `position` | character | Player position. |
| `value` | integer | Leader stat numeric value. |
| `first_name_default` | character | Player first name (default language). |
| `first_name_cs` | character | Player first name (Czech localization). |
| `first_name_de` | character | Player first name (German). |
| `first_name_es` | character | Player first name (Spanish). |
| `first_name_fi` | character | Player first name (Finnish). |
| `first_name_sk` | character | Player first name (Slovak localization). |
| `first_name_sv` | character | Player first name (Swedish). |
| `last_name_default` | character | Player last name (default language). |
| `team_name_default` | character | Team name (default locale). |
| `last_name_cs` | character | Player last name (Czech localization). |
| `last_name_fi` | character | Player last name (Finnish localization). |
| `last_name_sk` | character | Player last name (Slovak localization). |

### Example

```python
nhl_skater_leaders()
```

_Last validated n/a._

## `nhl_goalie_leaders`

Pull goalie stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/goalie-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/goalie-stats-leaders](https://api-web.nhle.com/v1/goalie-stats-leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `category` | character | Stat leader category. |
| `id` | integer | Unique player identifier. |
| `sweater_number` | integer | Jersey number. |
| `headshot` | character | URL to the player headshot image. |
| `team_abbrev` | character | Team abbreviation. |
| `team_logo` | character | URL to the team logo image. |
| `position` | character | Player position. |
| `value` | integer | Leader stat numeric value. |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |
| `team_name_default` | character | Team name (default locale). |
| `first_name_cs` | character | Player first name (Czech localization). |
| `first_name_sk` | character | Player first name (Slovak localization). |
| `last_name_cs` | character | Player last name (Czech localization). |
| `last_name_sk` | character | Player last name (Slovak localization). |
| `last_name_fi` | character | Player last name (Finnish localization). |

### Example

```python
nhl_goalie_leaders()
```

_Last validated n/a._

## `nhl_draft_picks`

Pull NHL draft picks for a year (and optionally one round).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/picks/{year}/{round_}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/picks/2024](https://api-web.nhle.com/v1/draft/picks/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `round_` | `round_` |  |  | `Y` | round_ path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `round` | integer | Shootout round number. |
| `pick_in_round` | integer | Pick number within the round. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `team_id` | integer | Unique team identifier. |
| `team_abbrev` | character | Team abbreviation. |
| `team_logo_light` | character | URL to the team logo (light variant). |
| `team_logo_dark` | character | URL to the team logo (dark variant). |
| `team_pick_history` | character | History of the team's picks at this slot. |
| `position_code` | character | Player position code. |
| `country_code` | character | Player country code. |
| `height` | integer | Player height in inches. |
| `weight` | integer | Player weight in pounds. |
| `amateur_league` | character | Amateur league the player played in. |
| `amateur_club_name` | character | Amateur club the player played for. |
| `team_name_default` | character | Team name (default locale). |
| `team_name_fr` | character | Team name (French locale). |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `display_abbrev_default` | character |  |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |
| `team_common_name_fr` | character | Team common name (French localization). |

### Example

```python
nhl_draft_picks(year=2024)
```

_Last validated n/a._

## `nhl_draft_rankings`

Pull NHL Central Scouting rankings for a draft year.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/rankings/{year}/{category}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/rankings/2024](https://api-web.nhle.com/v1/draft/rankings/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `category` | `category` |  |  | `Y` | category path parameter. |

### Returns

Pull NHL Central Scouting rankings for a draft year.

### Example

```python
nhl_draft_rankings(year=2024)
```

_Last validated n/a._

## `nhl_draft_picks_now`

Pull the current / most recent draft pick set.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/picks/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft/picks/now](https://api-web.nhle.com/v1/draft/picks/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Pull the current / most recent draft pick set.

### Example

```python
nhl_draft_picks_now()
```

_Last validated n/a._

## `nhl_draft_rankings_now`

Pull the current Central Scouting rankings.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/rankings/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft/rankings/now](https://api-web.nhle.com/v1/draft/rankings/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Pull the current Central Scouting rankings.

### Example

```python
nhl_draft_rankings_now()
```

_Last validated n/a._

## `nhl_draft_tracker_picks_now`

Pull the live draft-tracker pick list (during the draft itself).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft-tracker/picks/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft-tracker/picks/now](https://api-web.nhle.com/v1/draft-tracker/picks/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Pull the live draft-tracker pick list (during the draft itself).

### Example

```python
nhl_draft_tracker_picks_now()
```

_Last validated n/a._
