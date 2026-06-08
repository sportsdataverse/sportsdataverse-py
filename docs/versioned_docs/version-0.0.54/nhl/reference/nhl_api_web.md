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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_id` | `game_id` |  | `Y` |  |

### Returns

| col_name | type | description |
|---|---|---|
| `event_id` | integer |  |
| `time_in_period` | character |  |
| `time_remaining` | character |  |
| `situation_code` | character |  |
| `home_team_defending_side` | character |  |
| `type_code` | integer |  |
| `type_desc_key` | character |  |
| `sort_order` | integer |  |
| `period_descriptor_number` | integer |  |
| `period_descriptor_period_type` | character |  |
| `period_descriptor_max_regulation_periods` | integer |  |
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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_id` | `game_id` |  | `Y` |  |

### Returns

| col_name | type | description |
|---|---|---|
| `home_away` | character |  |
| `position_group` | character |  |
| `player_id` | integer |  |
| `sweater_number` | integer |  |
| `position` | character |  |
| `goals` | double |  |
| `assists` | double |  |
| `points` | double |  |
| `plus_minus` | double |  |
| `pim` | integer |  |
| `hits` | double |  |
| `power_play_goals` | double |  |
| `sog` | double |  |
| `faceoff_winning_pctg` | double |  |
| `toi` | character |  |
| `blocked_shots` | double |  |
| `shifts` | double |  |
| `giveaways` | double |  |
| `takeaways` | double |  |
| `name_default` | character |  |
| `even_strength_shots_against` | character |  |
| `power_play_shots_against` | character |  |
| `shorthanded_shots_against` | character |  |
| `save_shots_against` | character |  |
| `even_strength_goals_against` | double |  |
| `power_play_goals_against` | double |  |
| `shorthanded_goals_against` | double |  |
| `goals_against` | double |  |
| `starter` | logical |  |
| `shots_against` | double |  |
| `saves` | double |  |
| `save_pctg` | double |  |
| `decision` | character |  |
| `name_cs` | character |  |
| `name_fi` | character |  |
| `name_sk` | character |  |

### Example

```python
nhl_boxscore(game_id=2024020001)
```

_Last validated n/a._

## `nhl_landing`

Pull the gamecenter landing payload for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/landing](https://api-web.nhle.com/v1/gamecenter/2024020001/landing)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_id` | `game_id` |  | `Y` |  |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `season` | integer |  |
| `game_type` | integer |  |
| `limited_scoring` | logical |  |
| `game_date` | character |  |
| `start_time_utc` | character |  |
| `eastern_utc_offset` | character |  |
| `venue_utc_offset` | character |  |
| `venue_timezone` | character |  |
| `tv_broadcasts` | character |  |
| `game_state` | character |  |
| `game_schedule_state` | character |  |
| `shootout_in_use` | logical |  |
| `reg_periods` | integer |  |
| `ot_in_use` | logical |  |
| `ties_in_use` | logical |  |
| `venue_default` | character |  |
| `venue_location_default` | character |  |
| `period_descriptor_number` | integer |  |
| `period_descriptor_period_type` | character |  |
| `period_descriptor_max_regulation_periods` | integer |  |
| `away_team_id` | integer |  |
| `away_team_common_name_default` | character |  |
| `away_team_abbrev` | character |  |
| `away_team_place_name_default` | character |  |
| `away_team_place_name_with_preposition_default` | character |  |
| `away_team_place_name_with_preposition_fr` | character |  |
| `away_team_score` | integer |  |
| `away_team_sog` | integer |  |
| `away_team_logo` | character |  |
| `away_team_dark_logo` | character |  |
| `home_team_id` | integer |  |
| `home_team_common_name_default` | character |  |
| `home_team_abbrev` | character |  |
| `home_team_place_name_default` | character |  |
| `home_team_place_name_fr` | character |  |
| `home_team_place_name_with_preposition_default` | character |  |
| `home_team_place_name_with_preposition_fr` | character |  |
| `home_team_score` | integer |  |
| `home_team_sog` | integer |  |
| `home_team_logo` | character |  |
| `home_team_dark_logo` | character |  |
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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `game_id` | `game_id` |  | `Y` |  |

### Returns

Pull the gamecenter right-rail payload (in-game widgets).

### Example

```python
nhl_right_rail(game_id=2024020001)
```

_Last validated n/a._

## `nhl_web_schedule`

Pull the week-of NHL schedule rooted at ``date``.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule](https://api-web.nhle.com/v1/schedule)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `date` | `date` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `schedule_date` | character |  |
| `id` | integer |  |
| `season` | integer |  |
| `game_type` | integer |  |
| `neutral_site` | logical |  |
| `start_time_utc` | character |  |
| `eastern_utc_offset` | character |  |
| `venue_utc_offset` | character |  |
| `venue_timezone` | character |  |
| `game_state` | character |  |
| `game_schedule_state` | character |  |
| `tv_broadcasts` | character |  |
| `series_url` | character |  |
| `three_min_recap` | character |  |
| `game_center_link` | character |  |
| `venue_default` | character |  |
| `away_team_id` | integer |  |
| `away_team_common_name_default` | character |  |
| `away_team_place_name_default` | character |  |
| `away_team_place_name_with_preposition_default` | character |  |
| `away_team_place_name_with_preposition_fr` | character |  |
| `away_team_abbrev` | character |  |
| `away_team_logo` | character |  |
| `away_team_dark_logo` | character |  |
| `away_team_away_split_squad` | logical |  |
| `away_team_score` | integer |  |
| `home_team_id` | integer |  |
| `home_team_common_name_default` | character |  |
| `home_team_place_name_default` | character |  |
| `home_team_place_name_fr` | character |  |
| `home_team_place_name_with_preposition_default` | character |  |
| `home_team_place_name_with_preposition_fr` | character |  |
| `home_team_abbrev` | character |  |
| `home_team_logo` | character |  |
| `home_team_dark_logo` | character |  |
| `home_team_home_split_squad` | logical |  |
| `home_team_score` | integer |  |
| `period_descriptor_number` | integer |  |
| `period_descriptor_period_type` | character |  |
| `period_descriptor_max_regulation_periods` | integer |  |
| `game_outcome_last_period_type` | character |  |
| `winning_goalie_player_id` | integer |  |
| `winning_goalie_first_initial_default` | character |  |
| `winning_goalie_last_name_default` | character |  |
| `winning_goalie_last_name_cs` | character |  |
| `winning_goalie_last_name_fi` | character |  |
| `winning_goalie_last_name_sk` | character |  |
| `winning_goal_scorer_player_id` | integer |  |
| `winning_goal_scorer_first_initial_default` | character |  |
| `winning_goal_scorer_last_name_default` | character |  |
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

Pull the single-day scoreboard for ``date``.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/score/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/score](https://api-web.nhle.com/v1/score)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `date` | `date` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `season` | integer |  |
| `game_type` | integer |  |
| `game_date` | character |  |
| `start_time_utc` | character |  |
| `eastern_utc_offset` | character |  |
| `venue_utc_offset` | character |  |
| `tv_broadcasts` | character |  |
| `game_state` | character |  |
| `game_schedule_state` | character |  |
| `game_center_link` | character |  |
| `series_url` | character |  |
| `three_min_recap` | character |  |
| `neutral_site` | logical |  |
| `venue_timezone` | character |  |
| `period` | integer |  |
| `goals` | character |  |
| `venue_default` | character |  |
| `away_team_id` | integer |  |
| `away_team_name_default` | character |  |
| `away_team_abbrev` | character |  |
| `away_team_score` | integer |  |
| `away_team_sog` | integer |  |
| `away_team_logo` | character |  |
| `home_team_id` | integer |  |
| `home_team_name_default` | character |  |
| `home_team_abbrev` | character |  |
| `home_team_score` | integer |  |
| `home_team_sog` | integer |  |
| `home_team_logo` | character |  |
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
| `period_descriptor_number` | integer |  |
| `period_descriptor_period_type` | character |  |
| `period_descriptor_max_regulation_periods` | integer |  |
| `game_outcome_last_period_type` | character |  |

### Example

```python
nhl_score()
```

_Last validated n/a._

## `nhl_schedule_calendar`

Pull the calendar of game-days for the season.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule-calendar/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule-calendar](https://api-web.nhle.com/v1/schedule-calendar)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `date` | `date` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  | `Y` |  |
| `series_letter` | `series_letter` |  | `Y` |  |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `date` | `date` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `clinch_indicator` | character |  |
| `conference_abbrev` | character |  |
| `conference_home_sequence` | integer |  |
| `conference_l10_sequence` | integer |  |
| `conference_name` | character |  |
| `conference_road_sequence` | integer |  |
| `conference_sequence` | integer |  |
| `date` | character |  |
| `division_abbrev` | character |  |
| `division_home_sequence` | integer |  |
| `division_l10_sequence` | integer |  |
| `division_name` | character |  |
| `division_road_sequence` | integer |  |
| `division_sequence` | integer |  |
| `game_type_id` | integer |  |
| `games_played` | integer |  |
| `goal_differential` | integer |  |
| `goal_differential_pctg` | double |  |
| `goal_against` | integer |  |
| `goal_for` | integer |  |
| `goals_for_pctg` | double |  |
| `home_games_played` | integer |  |
| `home_goal_differential` | integer |  |
| `home_goals_against` | integer |  |
| `home_goals_for` | integer |  |
| `home_losses` | integer |  |
| `home_ot_losses` | integer |  |
| `home_points` | integer |  |
| `home_regulation_plus_ot_wins` | integer |  |
| `home_regulation_wins` | integer |  |
| `home_ties` | integer |  |
| `home_wins` | integer |  |
| `l10_games_played` | integer |  |
| `l10_goal_differential` | integer |  |
| `l10_goals_against` | integer |  |
| `l10_goals_for` | integer |  |
| `l10_losses` | integer |  |
| `l10_ot_losses` | integer |  |
| `l10_points` | integer |  |
| `l10_regulation_plus_ot_wins` | integer |  |
| `l10_regulation_wins` | integer |  |
| `l10_ties` | integer |  |
| `l10_wins` | integer |  |
| `league_home_sequence` | integer |  |
| `league_l10_sequence` | integer |  |
| `league_road_sequence` | integer |  |
| `league_sequence` | integer |  |
| `losses` | integer |  |
| `ot_losses` | integer |  |
| `point_pctg` | double |  |
| `points` | integer |  |
| `regulation_plus_ot_win_pctg` | double |  |
| `regulation_plus_ot_wins` | integer |  |
| `regulation_win_pctg` | double |  |
| `regulation_wins` | integer |  |
| `road_games_played` | integer |  |
| `road_goal_differential` | integer |  |
| `road_goals_against` | integer |  |
| `road_goals_for` | integer |  |
| `road_losses` | integer |  |
| `road_ot_losses` | integer |  |
| `road_points` | integer |  |
| `road_regulation_plus_ot_wins` | integer |  |
| `road_regulation_wins` | integer |  |
| `road_ties` | integer |  |
| `road_wins` | integer |  |
| `season_id` | integer |  |
| `shootout_losses` | integer |  |
| `shootout_wins` | integer |  |
| `streak_code` | character |  |
| `streak_count` | integer |  |
| `team_logo` | character |  |
| `ties` | integer |  |
| `waivers_sequence` | integer |  |
| `wildcard_sequence` | integer |  |
| `win_pctg` | double |  |
| `wins` | integer |  |
| `place_name_default` | character |  |
| `team_name_default` | character |  |
| `team_name_fr` | character |  |
| `team_common_name_default` | character |  |
| `team_abbrev_default` | character |  |
| `place_name_fr` | character |  |
| `team_common_name_fr` | character |  |

### Example

```python
nhl_standings()
```

_Last validated n/a._

## `nhl_standings_season`

Pull the per-season standings cutover dates.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings-season`

**Valid URL:** [https://api-web.nhle.com/v1/standings-season](https://api-web.nhle.com/v1/standings-season)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

| col_name | type | description |
|---|---|---|
| `id` | integer |  |
| `conferences_in_use` | logical |  |
| `divisions_in_use` | logical |  |
| `point_for_o_tloss_in_use` | logical |  |
| `regulation_wins_in_use` | logical |  |
| `row_in_use` | logical |  |
| `standings_end` | character |  |
| `standings_start` | character |  |
| `ties_in_use` | logical |  |
| `wildcard_in_use` | logical |  |

### Example

```python
nhl_standings_season()
```

_Last validated n/a._

## `nhl_club_schedule_season`

Pull a team's full-season schedule.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule-season/TOR](https://api-web.nhle.com/v1/club-schedule-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `club_previous_season` | integer |  |
| `club_current_season` | integer |  |
| `club_next_season` | integer |  |
| `club_timezone` | character |  |
| `id` | integer |  |
| `season` | integer |  |
| `game_type` | integer |  |
| `game_date` | character |  |
| `neutral_site` | logical |  |
| `start_time_utc` | character |  |
| `eastern_utc_offset` | character |  |
| `venue_utc_offset` | character |  |
| `venue_timezone` | character |  |
| `game_state` | character |  |
| `game_schedule_state` | character |  |
| `tv_broadcasts` | character |  |
| `game_center_link` | character |  |
| `venue_default` | character |  |
| `away_team_id` | integer |  |
| `away_team_common_name_default` | character |  |
| `away_team_place_name_default` | character |  |
| `away_team_place_name_with_preposition_default` | character |  |
| `away_team_place_name_with_preposition_fr` | character |  |
| `away_team_abbrev` | character |  |
| `away_team_logo` | character |  |
| `away_team_dark_logo` | character |  |
| `away_team_away_split_squad` | logical |  |
| `away_team_score` | integer |  |
| `home_team_id` | integer |  |
| `home_team_common_name_default` | character |  |
| `home_team_place_name_default` | character |  |
| `home_team_place_name_with_preposition_default` | character |  |
| `home_team_place_name_with_preposition_fr` | character |  |
| `home_team_abbrev` | character |  |
| `home_team_logo` | character |  |
| `home_team_dark_logo` | character |  |
| `home_team_home_split_squad` | logical |  |
| `home_team_airline_link` | character |  |
| `home_team_airline_desc` | character |  |
| `home_team_hotel_link` | character |  |
| `home_team_hotel_desc` | character |  |
| `home_team_score` | integer |  |
| `period_descriptor_period_type` | character |  |
| `period_descriptor_max_regulation_periods` | integer |  |
| `game_outcome_last_period_type` | character |  |
| `winning_goalie_player_id` | integer |  |
| `winning_goalie_first_initial_default` | character |  |
| `winning_goalie_last_name_default` | character |  |
| `away_team_airline_link` | character |  |
| `away_team_airline_desc` | character |  |
| `winning_goal_scorer_player_id` | double |  |
| `winning_goal_scorer_first_initial_default` | character |  |
| `winning_goal_scorer_last_name_default` | character |  |
| `three_min_recap` | character |  |
| `home_team_place_name_fr` | character |  |
| `condensed_game` | character |  |
| `venue_es` | character |  |
| `venue_fr` | character |  |
| `special_event_parent_id` | double |  |
| `special_event_name_default` | character |  |
| `special_event_name_fr` | character |  |
| `away_team_hotel_link` | character |  |
| `away_team_hotel_desc` | character |  |
| `three_min_recap_fr` | character |  |
| `winning_goalie_last_name_cs` | character |  |
| `winning_goalie_last_name_fi` | character |  |
| `winning_goalie_last_name_sk` | character |  |
| `away_team_place_name_fr` | character |  |
| `away_team_common_name_fr` | character |  |
| `home_team_common_name_fr` | character |  |
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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `month` | `month` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `date` | `date` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `position_group` | character |  |
| `id` | integer |  |
| `headshot` | character |  |
| `sweater_number` | integer |  |
| `position_code` | character |  |
| `shoots_catches` | character |  |
| `height_in_inches` | integer |  |
| `weight_in_pounds` | integer |  |
| `height_in_centimeters` | integer |  |
| `weight_in_kilograms` | integer |  |
| `birth_date` | character |  |
| `birth_country` | character |  |
| `first_name_default` | character |  |
| `last_name_default` | character |  |
| `birth_city_default` | character |  |
| `birth_state_province_default` | character |  |
| `birth_city_cs` | character |  |
| `birth_city_de` | character |  |
| `birth_city_fi` | character |  |
| `birth_city_sk` | character |  |
| `birth_city_sv` | character |  |

### Example

```python
nhl_roster(team='TOR')
```

_Last validated n/a._

## `nhl_roster_season`

Pull every season a team has had on file.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/roster-season/TOR](https://api-web.nhle.com/v1/roster-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team` | `team` |  | `Y` |  |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |

### Returns

| col_name | type | description |
|---|---|---|
| `player_id` | integer |  |
| `is_active` | logical |  |
| `current_team_id` | integer |  |
| `current_team_abbrev` | character |  |
| `badges` | character |  |
| `team_logo` | character |  |
| `sweater_number` | integer |  |
| `position` | character |  |
| `headshot` | character |  |
| `hero_image` | character |  |
| `height_in_inches` | integer |  |
| `height_in_centimeters` | integer |  |
| `weight_in_pounds` | integer |  |
| `weight_in_kilograms` | integer |  |
| `birth_date` | character |  |
| `birth_country` | character |  |
| `shoots_catches` | character |  |
| `player_slug` | character |  |
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
| `team_common_name_default` | character |  |
| `team_place_name_with_preposition_default` | character |  |
| `team_place_name_with_preposition_fr` | character |  |
| `first_name_default` | character |  |
| `last_name_default` | character |  |
| `birth_city_default` | character |  |
| `birth_state_province_default` | character |  |
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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `player_id` | `player_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | integer |  |
| `team_abbrev` | character |  |
| `home_road_flag` | character |  |
| `game_date` | character |  |
| `goals` | integer |  |
| `assists` | integer |  |
| `points` | integer |  |
| `plus_minus` | integer |  |
| `power_play_goals` | integer |  |
| `power_play_points` | integer |  |
| `game_winning_goals` | integer |  |
| `ot_goals` | integer |  |
| `shots` | integer |  |
| `shifts` | integer |  |
| `shorthanded_goals` | integer |  |
| `shorthanded_points` | integer |  |
| `opponent_abbrev` | character |  |
| `pim` | integer |  |
| `toi` | character |  |
| `common_name_default` | character |  |
| `opponent_common_name_default` | character |  |
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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `category` | character |  |
| `id` | integer |  |
| `sweater_number` | integer |  |
| `headshot` | character |  |
| `team_abbrev` | character |  |
| `team_logo` | character |  |
| `position` | character |  |
| `value` | integer |  |
| `first_name_default` | character |  |
| `first_name_cs` | character |  |
| `first_name_de` | character |  |
| `first_name_es` | character |  |
| `first_name_fi` | character |  |
| `first_name_sk` | character |  |
| `first_name_sv` | character |  |
| `last_name_default` | character |  |
| `team_name_default` | character |  |
| `last_name_cs` | character |  |
| `last_name_fi` | character |  |
| `last_name_sk` | character |  |

### Example

```python
nhl_skater_leaders()
```

_Last validated n/a._

## `nhl_goalie_leaders`

Pull goalie stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/goalie-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/goalie-stats-leaders](https://api-web.nhle.com/v1/goalie-stats-leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `game_type` | `game_type` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `category` | character |  |
| `id` | integer |  |
| `sweater_number` | integer |  |
| `headshot` | character |  |
| `team_abbrev` | character |  |
| `team_logo` | character |  |
| `position` | character |  |
| `value` | integer |  |
| `first_name_default` | character |  |
| `last_name_default` | character |  |
| `team_name_default` | character |  |
| `first_name_cs` | character |  |
| `first_name_sk` | character |  |
| `last_name_cs` | character |  |
| `last_name_sk` | character |  |
| `last_name_fi` | character |  |

### Example

```python
nhl_goalie_leaders()
```

_Last validated n/a._

## `nhl_draft_picks`

Pull NHL draft picks for a year (and optionally one round).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/picks/{year}/{round_}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/picks/2024](https://api-web.nhle.com/v1/draft/picks/2024)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `year` | `year` |  | `Y` |  |
| `round_` | `round_` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `round` | integer |  |
| `pick_in_round` | integer |  |
| `overall_pick` | integer |  |
| `team_id` | integer |  |
| `team_abbrev` | character |  |
| `team_logo_light` | character |  |
| `team_logo_dark` | character |  |
| `team_pick_history` | character |  |
| `position_code` | character |  |
| `country_code` | character |  |
| `height` | integer |  |
| `weight` | integer |  |
| `amateur_league` | character |  |
| `amateur_club_name` | character |  |
| `team_name_default` | character |  |
| `team_name_fr` | character |  |
| `team_common_name_default` | character |  |
| `team_place_name_with_preposition_default` | character |  |
| `team_place_name_with_preposition_fr` | character |  |
| `display_abbrev_default` | character |  |
| `first_name_default` | character |  |
| `last_name_default` | character |  |
| `team_common_name_fr` | character |  |

### Example

```python
nhl_draft_picks(year=2024)
```

_Last validated n/a._

## `nhl_draft_rankings`

Pull NHL Central Scouting rankings for a draft year.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/rankings/{year}/{category}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/rankings/2024](https://api-web.nhle.com/v1/draft/rankings/2024)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `year` | `year` |  | `Y` |  |
| `category` | `category` |  |  | `Y` |

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

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

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Pull the live draft-tracker pick list (during the draft itself).

### Example

```python
nhl_draft_tracker_picks_now()
```

_Last validated n/a._
