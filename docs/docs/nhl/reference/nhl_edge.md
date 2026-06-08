---
title: NHL — NHL EDGE API
sidebar_label: NHL EDGE API
sidebar_position: 11
---
# NHL — NHL EDGE API

`sportsdataverse.nhl` — 35 endpoints.

## `nhl_edge_skater_detail`

Pull EDGE detail stats for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-detail/8480801](https://api-web.nhle.com/v1/edge/skater-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `sog_summary` | character |  |
| `sog_details` | character |  |
| `player_id` | integer |  |
| `player_first_name_default` | character |  |
| `player_last_name_default` | character |  |
| `player_birth_date` | character |  |
| `player_shoots_catches` | character |  |
| `player_sweater_number` | integer |  |
| `player_position` | character |  |
| `player_slug` | character |  |
| `player_headshot` | character |  |
| `player_goals` | integer |  |
| `player_assists` | integer |  |
| `player_points` | integer |  |
| `player_games_played` | integer |  |
| `player_team_common_name_default` | character |  |
| `player_team_place_name_with_preposition_default` | character |  |
| `player_team_place_name_with_preposition_fr` | character |  |
| `player_team_abbrev` | character |  |
| `player_team_team_logo_light` | character |  |
| `player_team_team_logo_dark` | character |  |
| `top_shot_speed_imperial` | double |  |
| `top_shot_speed_metric` | double |  |
| `top_shot_speed_percentile` | double |  |
| `top_shot_speed_league_avg_imperial` | double |  |
| `top_shot_speed_league_avg_metric` | double |  |
| `top_shot_speed_overlay_player_first_name_default` | character |  |
| `top_shot_speed_overlay_player_last_name_default` | character |  |
| `top_shot_speed_overlay_game_date` | character |  |
| `top_shot_speed_overlay_away_team_abbrev` | character |  |
| `top_shot_speed_overlay_away_team_score` | integer |  |
| `top_shot_speed_overlay_home_team_abbrev` | character |  |
| `top_shot_speed_overlay_home_team_score` | integer |  |
| `top_shot_speed_overlay_game_outcome_last_period_type` | character |  |
| `top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `top_shot_speed_overlay_period_descriptor_number` | integer |  |
| `top_shot_speed_overlay_period_descriptor_period_type` | character |  |
| `top_shot_speed_overlay_time_in_period` | character |  |
| `top_shot_speed_overlay_game_type` | integer |  |
| `skating_speed_speed_max_imperial` | double |  |
| `skating_speed_speed_max_metric` | double |  |
| `skating_speed_speed_max_percentile` | double |  |
| `skating_speed_speed_max_league_avg_imperial` | double |  |
| `skating_speed_speed_max_league_avg_metric` | double |  |
| `skating_speed_speed_max_overlay_player_first_name_default` | character |  |
| `skating_speed_speed_max_overlay_player_last_name_default` | character |  |
| `skating_speed_speed_max_overlay_game_date` | character |  |
| `skating_speed_speed_max_overlay_away_team_abbrev` | character |  |
| `skating_speed_speed_max_overlay_away_team_score` | integer |  |
| `skating_speed_speed_max_overlay_home_team_abbrev` | character |  |
| `skating_speed_speed_max_overlay_home_team_score` | integer |  |
| `skating_speed_speed_max_overlay_game_outcome_last_period_type` | character |  |
| `skating_speed_speed_max_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `skating_speed_speed_max_overlay_period_descriptor_number` | integer |  |
| `skating_speed_speed_max_overlay_period_descriptor_period_type` | character |  |
| `skating_speed_speed_max_overlay_time_in_period` | character |  |
| `skating_speed_speed_max_overlay_game_type` | integer |  |
| `skating_speed_bursts_over20_value` | integer |  |
| `skating_speed_bursts_over20_percentile` | double |  |
| `skating_speed_bursts_over20_league_avg_value` | double |  |
| `total_distance_skated_imperial` | double |  |
| `total_distance_skated_metric` | double |  |
| `total_distance_skated_percentile` | double |  |
| `total_distance_skated_league_avg_imperial` | double |  |
| `total_distance_skated_league_avg_metric` | double |  |
| `distance_max_game_imperial` | double |  |
| `distance_max_game_metric` | double |  |
| `distance_max_game_percentile` | double |  |
| `distance_max_game_league_avg_imperial` | double |  |
| `distance_max_game_league_avg_metric` | double |  |
| `distance_max_game_overlay_player_first_name_default` | character |  |
| `distance_max_game_overlay_player_last_name_default` | character |  |
| `distance_max_game_overlay_game_date` | character |  |
| `distance_max_game_overlay_away_team_abbrev` | character |  |
| `distance_max_game_overlay_away_team_score` | integer |  |
| `distance_max_game_overlay_home_team_abbrev` | character |  |
| `distance_max_game_overlay_home_team_score` | integer |  |
| `distance_max_game_overlay_game_outcome_last_period_type` | character |  |
| `distance_max_game_overlay_game_outcome_ot_periods` | integer |  |
| `distance_max_game_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `distance_max_game_overlay_period_descriptor_number` | integer |  |
| `distance_max_game_overlay_period_descriptor_period_type` | character |  |
| `distance_max_game_overlay_game_type` | integer |  |
| `zone_time_details_offensive_zone_pctg` | double |  |
| `zone_time_details_offensive_zone_percentile` | double |  |
| `zone_time_details_offensive_zone_league_avg` | double |  |
| `zone_time_details_offensive_zone_ev_pctg` | double |  |
| `zone_time_details_offensive_zone_ev_percentile` | double |  |
| `zone_time_details_offensive_zone_ev_league_avg` | double |  |
| `zone_time_details_neutral_zone_pctg` | double |  |
| `zone_time_details_neutral_zone_percentile` | double |  |
| `zone_time_details_neutral_zone_league_avg` | double |  |
| `zone_time_details_defensive_zone_pctg` | double |  |
| `zone_time_details_defensive_zone_percentile` | double |  |
| `zone_time_details_defensive_zone_league_avg` | double |  |

### Example

```python
nhl_edge_skater_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_comparison`

Pull EDGE comparison data for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-comparison/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-comparison/8480801](https://api-web.nhle.com/v1/edge/skater-comparison/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE comparison data for a single skater.

### Example

```python
nhl_edge_skater_comparison(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_shot_location_detail`

Pull EDGE shot-location detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-location-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-location-detail/8480801](https://api-web.nhle.com/v1/edge/skater-shot-location-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE shot-location detail for a single skater.

### Example

```python
nhl_edge_skater_shot_location_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_shot_location_top_10`

Pull the EDGE top-10 skaters for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/{position}/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/forwards/shots/points](https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/forwards/shots/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position` | `position` |  | `Y` |  | position path parameter. |
| `category` | `category` |  | `Y` |  | category path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 skaters for a shot-location category.

### Example

```python
nhl_edge_skater_shot_location_top_10(position='forwards', category='shots', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_shot_speed_detail`

Pull EDGE shot-speed detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/8480801](https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `hardest_shots` | character |  |
| `shot_speed_details_top_shot_speed_imperial` | double |  |
| `shot_speed_details_top_shot_speed_metric` | double |  |
| `shot_speed_details_top_shot_speed_percentile` | double |  |
| `shot_speed_details_top_shot_speed_league_avg_imperial` | double |  |
| `shot_speed_details_top_shot_speed_league_avg_metric` | double |  |
| `shot_speed_details_top_shot_speed_overlay_player_first_name_default` | character |  |
| `shot_speed_details_top_shot_speed_overlay_player_last_name_default` | character |  |
| `shot_speed_details_top_shot_speed_overlay_game_date` | character |  |
| `shot_speed_details_top_shot_speed_overlay_away_team_abbrev` | character |  |
| `shot_speed_details_top_shot_speed_overlay_away_team_score` | integer |  |
| `shot_speed_details_top_shot_speed_overlay_home_team_abbrev` | character |  |
| `shot_speed_details_top_shot_speed_overlay_home_team_score` | integer |  |
| `shot_speed_details_top_shot_speed_overlay_game_outcome_last_period_type` | character |  |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_number` | integer |  |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_period_type` | character |  |
| `shot_speed_details_top_shot_speed_overlay_time_in_period` | character |  |
| `shot_speed_details_top_shot_speed_overlay_game_type` | integer |  |
| `shot_speed_details_avg_shot_speed_imperial` | double |  |
| `shot_speed_details_avg_shot_speed_metric` | double |  |
| `shot_speed_details_avg_shot_speed_percentile` | double |  |
| `shot_speed_details_avg_shot_speed_league_avg_imperial` | double |  |
| `shot_speed_details_avg_shot_speed_league_avg_metric` | double |  |
| `shot_speed_details_shot_attempts_over100_value` | integer |  |
| `shot_speed_details_shot_attempts_over100_percentile` | double |  |
| `shot_speed_details_shot_attempts_over100_league_avg` | double |  |
| `shot_speed_details_shot_attempts90_to100_value` | integer |  |
| `shot_speed_details_shot_attempts90_to100_percentile` | double |  |
| `shot_speed_details_shot_attempts90_to100_league_avg` | double |  |
| `shot_speed_details_shot_attempts80_to90_value` | integer |  |
| `shot_speed_details_shot_attempts80_to90_percentile` | double |  |
| `shot_speed_details_shot_attempts80_to90_league_avg` | double |  |
| `shot_speed_details_shot_attempts70_to80_value` | integer |  |
| `shot_speed_details_shot_attempts70_to80_percentile` | double |  |
| `shot_speed_details_shot_attempts70_to80_league_avg` | double |  |

### Example

```python
nhl_edge_skater_shot_speed_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_shot_speed_top_10`

Pull the EDGE top-10 skaters by shot speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/defense/points](https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/defense/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 skaters by shot speed.

### Example

```python
nhl_edge_skater_shot_speed_top_10(positions='defense', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_skating_distance_detail`

Pull EDGE skating-distance detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/8480801](https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE skating-distance detail for a single skater.

### Example

```python
nhl_edge_skater_skating_distance_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_skating_speed_detail`

Pull EDGE skating-speed detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/8480801](https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE skating-speed detail for a single skater.

### Example

```python
nhl_edge_skater_skating_speed_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_speed_top_10`

Pull the EDGE top-10 skaters by skating speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-speed-top-10/defense/points](https://api-web.nhle.com/v1/edge/skater-speed-top-10/defense/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 skaters by skating speed.

### Example

```python
nhl_edge_skater_speed_top_10(positions='defense', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_distance_top_10`

Pull the EDGE top-10 skaters by skating distance.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-distance-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-distance-top-10/defense/ev/points](https://api-web.nhle.com/v1/edge/skater-distance-top-10/defense/ev/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 skaters by skating distance.

### Example

```python
nhl_edge_skater_distance_top_10(positions='defense', strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_zone_time`

Pull EDGE zone-time detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-zone-time/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-zone-time/8480801](https://api-web.nhle.com/v1/edge/skater-zone-time/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `strength_code` | character |  |
| `offensive_zone_pctg` | double |  |
| `offensive_zone_percentile` | double |  |
| `offensive_zone_league_avg` | double |  |
| `neutral_zone_pctg` | double |  |
| `neutral_zone_percentile` | double |  |
| `neutral_zone_league_avg` | double |  |
| `defensive_zone_pctg` | double |  |
| `defensive_zone_percentile` | double |  |
| `defensive_zone_league_avg` | double |  |

### Example

```python
nhl_edge_skater_zone_time(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_skater_zone_time_top_10`

Pull the EDGE top-10 skaters by zone time.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/defense/ev/points](https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/defense/ev/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 skaters by zone time.

### Example

```python
nhl_edge_skater_zone_time_top_10(positions='defense', strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_skater_landing`

Pull the EDGE skater landing page (summary across all skaters).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-landing](https://api-web.nhle.com/v1/edge/skater-landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE skater landing page (summary across all skaters).

### Example

```python
nhl_edge_skater_landing()
```

_Last validated n/a._

## `nhl_edge_goalie_detail`

Pull EDGE detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `shot_location_summary` | character |  |
| `shot_location_details` | character |  |
| `player_id` | integer |  |
| `player_first_name_default` | character |  |
| `player_last_name_default` | character |  |
| `player_birth_date` | character |  |
| `player_shoots_catches` | character |  |
| `player_sweater_number` | integer |  |
| `player_slug` | character |  |
| `player_headshot` | character |  |
| `player_wins` | integer |  |
| `player_losses` | integer |  |
| `player_overtime_losses` | integer |  |
| `player_goals_against_avg` | double |  |
| `player_save_pctg` | double |  |
| `player_games_played` | integer |  |
| `player_team_common_name_default` | character |  |
| `player_team_place_name_with_preposition_default` | character |  |
| `player_team_place_name_with_preposition_fr` | character |  |
| `player_team_abbrev` | character |  |
| `player_team_team_logo_light` | character |  |
| `player_team_team_logo_dark` | character |  |
| `stats_goals_against_avg_value` | double |  |
| `stats_goals_against_avg_percentile` | double |  |
| `stats_goals_against_avg_league_avg` | double |  |
| `stats_games_above900_value` | double |  |
| `stats_games_above900_percentile` | double |  |
| `stats_games_above900_league_avg` | double |  |
| `stats_goal_differential_per60_value` | double |  |
| `stats_goal_differential_per60_percentile` | double |  |
| `stats_goal_differential_per60_league_avg` | double |  |
| `stats_goal_support_avg_value` | double |  |
| `stats_goal_support_avg_percentile` | double |  |
| `stats_goal_support_avg_league_avg` | double |  |
| `stats_point_pctg_value` | double |  |
| `stats_point_pctg_percentile` | double |  |
| `stats_point_pctg_league_avg` | double |  |

### Example

```python
nhl_edge_goalie_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_5v5_detail`

Pull EDGE 5-on-5 detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-5v5-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-5v5-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-5v5-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE 5-on-5 detail stats for a single goalie.

### Example

```python
nhl_edge_goalie_5v5_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_5v5_top_10`

Pull the EDGE top-10 goalies by 5-on-5 metrics.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/points](https://api-web.nhle.com/v1/edge/goalie-5v5-top-10/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 goalies by 5-on-5 metrics.

### Example

```python
nhl_edge_goalie_5v5_top_10(sort_by='points')
```

_Last validated n/a._

## `nhl_edge_goalie_comparison`

Pull EDGE comparison data for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-comparison/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-comparison/8480801](https://api-web.nhle.com/v1/edge/goalie-comparison/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE comparison data for a single goalie.

### Example

```python
nhl_edge_goalie_comparison(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_save_percentage_detail`

Pull EDGE save-percentage detail for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-save-percentage-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE save-percentage detail for a single goalie.

### Example

```python
nhl_edge_goalie_save_percentage_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_edge_save_pctg_top_10`

Pull the EDGE top-10 goalies by save-percentage.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/points](https://api-web.nhle.com/v1/edge/goalie-edge-save-pctg-top-10/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 goalies by save-percentage.

### Example

```python
nhl_edge_goalie_edge_save_pctg_top_10(sort_by='points')
```

_Last validated n/a._

## `nhl_edge_goalie_shot_location_detail`

Pull EDGE shot-location detail for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/8480801](https://api-web.nhle.com/v1/edge/goalie-shot-location-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `area` | character |  |
| `shots_against` | integer |  |
| `saves` | integer |  |
| `goals_against` | integer |  |
| `save_pctg` | double |  |
| `shots_against_percentile` | double |  |
| `saves_percentile` | double |  |
| `goals_against_percentile` | double |  |
| `save_pctg_percentile` | double |  |

### Example

```python
nhl_edge_goalie_shot_location_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_goalie_shot_location_top_10`

Pull the EDGE top-10 goalies for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/shots/points](https://api-web.nhle.com/v1/edge/goalie-shot-location-top-10/shots/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  | `Y` |  | category path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 goalies for a shot-location category.

### Example

```python
nhl_edge_goalie_shot_location_top_10(category='shots', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_goalie_landing`

Pull the EDGE goalie landing page (summary across all goalies).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/goalie-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/goalie-landing](https://api-web.nhle.com/v1/edge/goalie-landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE goalie landing page (summary across all goalies).

### Example

```python
nhl_edge_goalie_landing()
```

_Last validated n/a._

## `nhl_edge_team_detail`

Pull EDGE detail stats for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-detail/10](https://api-web.nhle.com/v1/edge/team-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `sog_summary` | character |  |
| `sog_details` | character |  |
| `team_id` | integer |  |
| `team_common_name_default` | character |  |
| `team_place_name_with_preposition_default` | character |  |
| `team_place_name_with_preposition_fr` | character |  |
| `team_abbrev` | character |  |
| `team_team_logo_light` | character |  |
| `team_team_logo_dark` | character |  |
| `team_slug` | character |  |
| `team_conference` | character |  |
| `team_division` | character |  |
| `team_wins` | integer |  |
| `team_losses` | integer |  |
| `team_ot_losses` | integer |  |
| `team_games_played` | integer |  |
| `team_points` | integer |  |
| `shot_speed_shot_attempts_over90_value` | integer |  |
| `shot_speed_shot_attempts_over90_rank` | integer |  |
| `shot_speed_top_shot_speed_imperial` | double |  |
| `shot_speed_top_shot_speed_metric` | double |  |
| `shot_speed_top_shot_speed_rank` | integer |  |
| `shot_speed_top_shot_speed_league_avg_imperial` | double |  |
| `shot_speed_top_shot_speed_league_avg_metric` | double |  |
| `shot_speed_top_shot_speed_overlay_player_first_name_default` | character |  |
| `shot_speed_top_shot_speed_overlay_player_last_name_default` | character |  |
| `shot_speed_top_shot_speed_overlay_game_date` | character |  |
| `shot_speed_top_shot_speed_overlay_away_team_abbrev` | character |  |
| `shot_speed_top_shot_speed_overlay_away_team_score` | integer |  |
| `shot_speed_top_shot_speed_overlay_home_team_abbrev` | character |  |
| `shot_speed_top_shot_speed_overlay_home_team_score` | integer |  |
| `shot_speed_top_shot_speed_overlay_game_outcome_last_period_type` | character |  |
| `shot_speed_top_shot_speed_overlay_game_outcome_ot_periods` | integer |  |
| `shot_speed_top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `shot_speed_top_shot_speed_overlay_period_descriptor_number` | integer |  |
| `shot_speed_top_shot_speed_overlay_period_descriptor_period_type` | character |  |
| `shot_speed_top_shot_speed_overlay_time_in_period` | character |  |
| `shot_speed_top_shot_speed_overlay_game_type` | integer |  |
| `skating_speed_bursts_over22_value` | integer |  |
| `skating_speed_bursts_over22_rank` | integer |  |
| `skating_speed_bursts_over20_value` | integer |  |
| `skating_speed_bursts_over20_rank` | integer |  |
| `skating_speed_bursts_over20_league_avg_value` | integer |  |
| `skating_speed_speed_max_imperial` | double |  |
| `skating_speed_speed_max_metric` | double |  |
| `skating_speed_speed_max_rank` | integer |  |
| `skating_speed_speed_max_league_avg_imperial` | double |  |
| `skating_speed_speed_max_league_avg_metric` | double |  |
| `skating_speed_speed_max_overlay_player_first_name_default` | character |  |
| `skating_speed_speed_max_overlay_player_last_name_default` | character |  |
| `skating_speed_speed_max_overlay_game_date` | character |  |
| `skating_speed_speed_max_overlay_away_team_abbrev` | character |  |
| `skating_speed_speed_max_overlay_away_team_score` | integer |  |
| `skating_speed_speed_max_overlay_home_team_abbrev` | character |  |
| `skating_speed_speed_max_overlay_home_team_score` | integer |  |
| `skating_speed_speed_max_overlay_game_outcome_last_period_type` | character |  |
| `skating_speed_speed_max_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `skating_speed_speed_max_overlay_period_descriptor_number` | integer |  |
| `skating_speed_speed_max_overlay_period_descriptor_period_type` | character |  |
| `skating_speed_speed_max_overlay_time_in_period` | character |  |
| `skating_speed_speed_max_overlay_game_type` | integer |  |
| `distance_skated_total_imperial` | double |  |
| `distance_skated_total_metric` | double |  |
| `distance_skated_total_rank` | integer |  |
| `distance_skated_total_league_avg_imperial` | double |  |
| `distance_skated_total_league_avg_metric` | double |  |
| `zone_time_details_offensive_zone_pctg` | double |  |
| `zone_time_details_offensive_zone_rank` | integer |  |
| `zone_time_details_offensive_zone_league_avg` | double |  |
| `zone_time_details_offensive_zone_ev_pctg` | double |  |
| `zone_time_details_offensive_zone_ev_rank` | integer |  |
| `zone_time_details_offensive_zone_ev_league_avg` | double |  |
| `zone_time_details_neutral_zone_pctg` | double |  |
| `zone_time_details_neutral_zone_rank` | integer |  |
| `zone_time_details_neutral_zone_league_avg` | double |  |
| `zone_time_details_defensive_zone_pctg` | double |  |
| `zone_time_details_defensive_zone_rank` | integer |  |
| `zone_time_details_defensive_zone_league_avg` | double |  |

### Example

```python
nhl_edge_team_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_landing`

Pull the EDGE team landing page (summary across all teams).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-landing](https://api-web.nhle.com/v1/edge/team-landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE team landing page (summary across all teams).

### Example

```python
nhl_edge_team_landing()
```

_Last validated n/a._

## `nhl_edge_team_shot_location_detail`

Pull EDGE shot-location detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-location-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-location-detail/10](https://api-web.nhle.com/v1/edge/team-shot-location-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `area` | character |  |
| `sog` | integer |  |
| `sog_rank` | integer |  |
| `goals` | integer |  |
| `goals_rank` | integer |  |
| `shooting_pctg` | double |  |
| `shooting_pctg_rank` | integer |  |

### Example

```python
nhl_edge_team_shot_location_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_shot_location_top_10`

Pull the EDGE top-10 teams for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-location-top-10/{position}/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-location-top-10/forwards/shots/points](https://api-web.nhle.com/v1/edge/team-shot-location-top-10/forwards/shots/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position` | `position` |  | `Y` |  | position path parameter. |
| `category` | `category` |  | `Y` |  | category path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 teams for a shot-location category.

### Example

```python
nhl_edge_team_shot_location_top_10(position='forwards', category='shots', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_team_shot_speed_detail`

Pull EDGE shot-speed detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-speed-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-speed-detail/10](https://api-web.nhle.com/v1/edge/team-shot-speed-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE shot-speed detail for a single team.

### Example

```python
nhl_edge_team_shot_speed_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_skating_distance_detail`

Pull EDGE skating-distance detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-distance-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-distance-detail/10](https://api-web.nhle.com/v1/edge/team-skating-distance-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE skating-distance detail for a single team.

### Example

```python
nhl_edge_team_skating_distance_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_skating_distance_top_10`

Pull the EDGE top-10 teams by skating distance.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-distance-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-distance-top-10/defense/ev/points](https://api-web.nhle.com/v1/edge/team-skating-distance-top-10/defense/ev/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 teams by skating distance.

### Example

```python
nhl_edge_team_skating_distance_top_10(positions='defense', strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_team_skating_speed_detail`

Pull EDGE skating-speed detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-speed-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-speed-detail/10](https://api-web.nhle.com/v1/edge/team-skating-speed-detail/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE skating-speed detail for a single team.

### Example

```python
nhl_edge_team_skating_speed_detail(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_skating_speed_top_10`

Pull the EDGE top-10 teams by skating speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-speed-top-10/defense/points](https://api-web.nhle.com/v1/edge/team-skating-speed-top-10/defense/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 teams by skating speed.

### Example

```python
nhl_edge_team_skating_speed_top_10(positions='defense', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_team_zone_time_details`

Pull EDGE zone-time details for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-zone-time-details/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-zone-time-details/10](https://api-web.nhle.com/v1/edge/team-zone-time-details/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull EDGE zone-time details for a single team.

### Example

```python
nhl_edge_team_zone_time_details(team_id=10)
```

_Last validated n/a._

## `nhl_edge_team_zone_time_top_10`

Pull the EDGE top-10 teams by zone time.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-zone-time-top-10/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-zone-time-top-10/ev/points](https://api-web.nhle.com/v1/edge/team-zone-time-top-10/ev/points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull the EDGE top-10 teams by zone time.

### Example

```python
nhl_edge_team_zone_time_top_10(strength='ev', sort_by='points')
```

_Last validated n/a._

## `nhl_edge_cat_skater_detail`

Pull categorized (cat) EDGE detail stats for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/cat/edge/skater-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/cat/edge/skater-detail/8480801](https://api-web.nhle.com/v1/cat/edge/skater-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull categorized (cat) EDGE detail stats for a single skater.

### Example

```python
nhl_edge_cat_skater_detail(player_id=8480801)
```

_Last validated n/a._

## `nhl_edge_cat_goalie_detail`

Pull categorized (cat) EDGE detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/cat/edge/goalie-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/cat/edge/goalie-detail/8480801](https://api-web.nhle.com/v1/cat/edge/goalie-detail/8480801)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns

Pull categorized (cat) EDGE detail stats for a single goalie.

### Example

```python
nhl_edge_cat_goalie_detail(player_id=8480801)
```

_Last validated n/a._
