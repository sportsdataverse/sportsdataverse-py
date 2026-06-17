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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | Comma-separated list or serialized array of seasons for which NHL EDGE player-tracking data is available for this skater. |
| `sog_summary` | character | Serialized summary-level shots-on-goal statistics for the skater, as returned in the NHL EDGE skater detail payload. |
| `sog_details` | character | Serialized detail breakdown of shots on goal by game or other sub-category, as returned in the NHL EDGE skater detail payload. |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character | Participant birth date (YYYY-MM-DD). |
| `player_shoots_catches` | character | Handedness indicator for the skater showing the side they shoot from ('L' for left, 'R' for right). |
| `player_sweater_number` | integer | Player jersey number. |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_goals` | integer | Total regular-season goals scored by the skater in the current NHL season, as returned in the EDGE skater detail. |
| `player_assists` | integer | Total regular-season assists recorded by the skater in the current NHL season, as returned in the EDGE skater detail. |
| `player_points` | integer | Player points. |
| `player_games_played` | integer | Total number of regular-season games played by the skater in the current NHL season, as returned in the EDGE skater detail. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `top_shot_speed_imperial` | double | Player's highest recorded shot speed for the season measured in miles per hour (imperial), as captured by NHL EDGE puck-tracking. |
| `top_shot_speed_metric` | double | Player's highest recorded shot speed for the season measured in kilometers per hour (metric), as captured by NHL EDGE puck-tracking. |
| `top_shot_speed_percentile` | double | Percentile rank of the player's top shot speed relative to all qualifying skaters in the NHL EDGE dataset. |
| `top_shot_speed_league_avg_imperial` | double | League-average top shot speed among qualifying skaters for the season, measured in miles per hour (imperial). |
| `top_shot_speed_league_avg_metric` | double | League-average top shot speed among qualifying skaters for the season, measured in kilometers per hour (metric). |
| `top_shot_speed_overlay_player_first_name_default` | character | Player's first name as stored in the NHL api-web system, included in the overlay for the top-shot-speed game. |
| `top_shot_speed_overlay_player_last_name_default` | character | Player's last name as stored in the NHL api-web system, included in the overlay for the top-shot-speed game. |
| `top_shot_speed_overlay_game_date` | character | Date (YYYY-MM-DD) of the game in which the player recorded their top shot speed for the season. |
| `top_shot_speed_overlay_away_team_abbrev` | character | Three-letter abbreviation for the away team in the game where the player recorded their top shot speed this season. |
| `top_shot_speed_overlay_away_team_score` | integer | Away team's final score in the game where the player recorded their season-high shot speed. |
| `top_shot_speed_overlay_home_team_abbrev` | character | Three-letter abbreviation for the home team in the game where the player achieved their top shot speed this season. |
| `top_shot_speed_overlay_home_team_score` | integer | Home team's final score in the game where the player recorded their season-high shot speed. |
| `top_shot_speed_overlay_game_outcome_last_period_type` | character | Type of period that ended the game where the player set their top shot speed (e.g., 'REG', 'OT', 'SO'). |
| `top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods defined for the game type in which the player recorded their top shot speed. |
| `top_shot_speed_overlay_period_descriptor_number` | integer | Period number in which the player recorded their top shot speed during the referenced game. |
| `top_shot_speed_overlay_period_descriptor_period_type` | character | Period type label (e.g., 'REG', 'OT') for the period in which the player hit their top shot speed. |
| `top_shot_speed_overlay_time_in_period` | character | Time elapsed within the period (MM:SS) when the player released their top-speed shot for the season. |
| `top_shot_speed_overlay_game_type` | integer | Numeric code for the game type of the game in which the player recorded their top shot speed (e.g., 2 = regular season). |
| `skating_speed_speed_max_imperial` | double | Player's top recorded skating speed for the season measured in miles per hour (imperial), as captured by NHL EDGE player tracking. |
| `skating_speed_speed_max_metric` | double | Player's top recorded skating speed for the season measured in kilometers per hour (metric), as captured by NHL EDGE player tracking. |
| `skating_speed_speed_max_percentile` | double | Percentile rank of the player's top skating speed relative to all qualifying skaters in the NHL EDGE dataset. |
| `skating_speed_speed_max_league_avg_imperial` | double | League-average top skating speed among qualifying skaters for the season, measured in miles per hour (imperial). |
| `skating_speed_speed_max_league_avg_metric` | double | League-average top skating speed among qualifying skaters for the season, measured in kilometers per hour (metric). |
| `skating_speed_speed_max_overlay_player_first_name_default` | character | Player's first name as stored in the NHL api-web system, included in the overlay for the top-skating-speed game. |
| `skating_speed_speed_max_overlay_player_last_name_default` | character | Player's last name as stored in the NHL api-web system, included in the overlay for the top-skating-speed game. |
| `skating_speed_speed_max_overlay_game_date` | character | Date (YYYY-MM-DD) of the game in which the player recorded their top skating speed for the season. |
| `skating_speed_speed_max_overlay_away_team_abbrev` | character | Three-letter abbreviation for the away team in the game where the player achieved their top skating speed this season. |
| `skating_speed_speed_max_overlay_away_team_score` | integer | Away team's final score in the game where the player achieved their season-high skating speed. |
| `skating_speed_speed_max_overlay_home_team_abbrev` | character | Three-letter abbreviation for the home team in the game where the player achieved their top skating speed this season. |
| `skating_speed_speed_max_overlay_home_team_score` | integer | Home team's final score in the game where the player achieved their season-high skating speed. |
| `skating_speed_speed_max_overlay_game_outcome_last_period_type` | character | Type of period that ended the game where the player set their top skating speed (e.g., 'REG', 'OT', 'SO'). |
| `skating_speed_speed_max_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods defined for the game type in which the player set their top skating speed. |
| `skating_speed_speed_max_overlay_period_descriptor_number` | integer | Period number in which the player recorded their top skating speed during the referenced game. |
| `skating_speed_speed_max_overlay_period_descriptor_period_type` | character | Period type label (e.g., 'REG', 'OT') for the period in which the player hit their top skating speed. |
| `skating_speed_speed_max_overlay_time_in_period` | character | Time elapsed within the period (MM:SS) when the player recorded their top skating speed for the season. |
| `skating_speed_speed_max_overlay_game_type` | integer | Numeric code for the game type of the game in which the player recorded their top skating speed (e.g., 2 = regular season). |
| `skating_speed_bursts_over20_value` | integer | Number of distinct skating speed bursts exceeding 20 mph recorded for the player across the season in NHL EDGE tracking data. |
| `skating_speed_bursts_over20_percentile` | double | Percentile rank of the player's count of skating speed bursts exceeding 20 mph relative to all qualifying skaters in the NHL EDGE dataset. |
| `skating_speed_bursts_over20_league_avg_value` | double | League-average season total of skating speed bursts exceeding 20 mph among qualifying skaters, the EDGE baseline comparator. |
| `total_distance_skated_imperial` | double | Total cumulative distance skated by the player across all tracked games in the season, measured in miles (imperial). |
| `total_distance_skated_metric` | double | Total cumulative distance skated by the player across all tracked games in the season, measured in kilometers (metric). |
| `total_distance_skated_percentile` | double | Percentile rank of the player's total season skating distance relative to all qualifying skaters in the NHL EDGE dataset. |
| `total_distance_skated_league_avg_imperial` | double | League-average total season skating distance among qualifying skaters, measured in miles (imperial). |
| `total_distance_skated_league_avg_metric` | double | League-average total season skating distance among qualifying skaters, measured in kilometers (metric). |
| `distance_max_game_imperial` | double | Maximum distance skated by the player in their single best game of the season, measured in miles (imperial). |
| `distance_max_game_metric` | double | Maximum distance skated by the player in their single best game of the season, measured in kilometers (metric). |
| `distance_max_game_percentile` | double | Percentile rank of the player's maximum single-game skating distance relative to all qualifying skaters in the NHL EDGE dataset. |
| `distance_max_game_league_avg_imperial` | double | League-average maximum single-game distance skated among all qualifying skaters, measured in miles (imperial). |
| `distance_max_game_league_avg_metric` | double | League-average maximum single-game distance skated among all qualifying skaters, measured in kilometers (metric). |
| `distance_max_game_overlay_player_first_name_default` | character | Player's first name as stored in the NHL api-web system, included in the overlay context for the max-distance game. |
| `distance_max_game_overlay_player_last_name_default` | character | Player's last name as stored in the NHL api-web system, included in the overlay context for the max-distance game. |
| `distance_max_game_overlay_game_date` | character | Date (YYYY-MM-DD) of the game where the player achieved their maximum single-game skating distance. |
| `distance_max_game_overlay_away_team_abbrev` | character | Three-letter abbreviation for the away team in the game where the player achieved their maximum single-game skating distance. |
| `distance_max_game_overlay_away_team_score` | integer | Away team's final score in the game where the player achieved their maximum single-game skating distance. |
| `distance_max_game_overlay_home_team_abbrev` | character | Three-letter abbreviation for the home team in the game where the player achieved their maximum single-game skating distance. |
| `distance_max_game_overlay_home_team_score` | integer | Home team's final score in the game where the player achieved their maximum single-game skating distance. |
| `distance_max_game_overlay_game_outcome_last_period_type` | character | Type of period that ended the game where the player set their maximum single-game skating distance (e.g., 'REG', 'OT', 'SO'). |
| `distance_max_game_overlay_game_outcome_ot_periods` | integer | Number of overtime periods played in the game where the player achieved their maximum single-game skating distance. |
| `distance_max_game_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods defined for the game type in which the player set their max-distance performance. |
| `distance_max_game_overlay_period_descriptor_number` | integer | Period number during which the player's max-distance game context is anchored in the NHL EDGE overlay data. |
| `distance_max_game_overlay_period_descriptor_period_type` | character | Period type label (e.g., 'REG', 'OT') for the period referenced in the max-distance game overlay. |
| `distance_max_game_overlay_game_type` | integer | Numeric code for the game type (e.g., 2 = regular season, 3 = playoffs) of the max-distance game. |
| `zone_time_details_offensive_zone_pctg` | double | Percentage of the player's total tracked ice time spent in the offensive zone across all situations, as measured by NHL EDGE zone-time tracking. |
| `zone_time_details_offensive_zone_percentile` | double | Percentile rank of the player's overall offensive zone time percentage relative to all qualifying skaters in the NHL EDGE dataset. |
| `zone_time_details_offensive_zone_league_avg` | double | League-average percentage of all-situation ice time spent in the offensive zone among qualifying skaters, used as a comparison baseline. |
| `zone_time_details_offensive_zone_ev_pctg` | double | Percentage of the player's even-strength ice time spent in the offensive zone, as measured by NHL EDGE zone-time tracking. |
| `zone_time_details_offensive_zone_ev_percentile` | double | Percentile rank of the player's even-strength offensive zone time percentage relative to all qualifying skaters in the NHL EDGE dataset. |
| `zone_time_details_offensive_zone_ev_league_avg` | double | League-average percentage of even-strength ice time spent in the offensive zone among qualifying skaters, used as a comparison baseline. |
| `zone_time_details_neutral_zone_pctg` | double | Percentage of the player's total tracked ice time spent in the neutral zone, as measured by NHL EDGE zone-time tracking. |
| `zone_time_details_neutral_zone_percentile` | double | Percentile rank of the player's neutral zone time percentage relative to all qualifying skaters in the NHL EDGE dataset. |
| `zone_time_details_neutral_zone_league_avg` | double | League-average percentage of ice time spent in the neutral zone among qualifying skaters, used as a comparison baseline. |
| `zone_time_details_defensive_zone_pctg` | double | Percentage of the player's total tracked ice time spent in the defensive zone, as measured by NHL EDGE zone-time tracking. |
| `zone_time_details_defensive_zone_percentile` | double | Percentile rank of the player's defensive zone time percentage relative to all qualifying skaters in the NHL EDGE dataset. |
| `zone_time_details_defensive_zone_league_avg` | double | League-average percentage of ice time spent in the defensive zone among qualifying skaters, used as a comparison baseline. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `skating_distance_last10` | character |  |
| `shot_location_details` | character |  |
| `shot_location_totals` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character | Participant birth date (YYYY-MM-DD). |
| `player_shoots_catches` | character |  |
| `player_sweater_number` | integer | Player jersey number. |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_goals` | integer |  |
| `player_assists` | integer |  |
| `player_points` | integer | Player points. |
| `player_games_played` | integer |  |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `player_team_slug` | character | Player team URL-friendly slug. |
| `shot_speed_details_top_shot_speed_imperial` | double |  |
| `shot_speed_details_top_shot_speed_metric` | double |  |
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
| `shot_speed_details_shot_attempts_over100` | integer |  |
| `shot_speed_details_shot_attempts90_to100` | integer |  |
| `shot_speed_details_shot_attempts80_to90` | integer |  |
| `shot_speed_details_shot_attempts70_to80` | integer |  |
| `skating_speed_details_max_skating_speed_imperial` | double |  |
| `skating_speed_details_max_skating_speed_metric` | double |  |
| `skating_speed_details_max_skating_speed_overlay_player_first_name_default` | character |  |
| `skating_speed_details_max_skating_speed_overlay_player_last_name_default` | character |  |
| `skating_speed_details_max_skating_speed_overlay_game_date` | character |  |
| `skating_speed_details_max_skating_speed_overlay_away_team_abbrev` | character |  |
| `skating_speed_details_max_skating_speed_overlay_away_team_score` | integer |  |
| `skating_speed_details_max_skating_speed_overlay_home_team_abbrev` | character |  |
| `skating_speed_details_max_skating_speed_overlay_home_team_score` | integer |  |
| `skating_speed_details_max_skating_speed_overlay_game_outcome_last_period_type` | character |  |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_number` | integer |  |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_period_type` | character |  |
| `skating_speed_details_max_skating_speed_overlay_time_in_period` | character |  |
| `skating_speed_details_max_skating_speed_overlay_game_type` | integer |  |
| `skating_speed_details_bursts_over22` | integer |  |
| `skating_speed_details_bursts20_to22` | integer |  |
| `skating_speed_details_bursts18_to20` | integer |  |
| `skating_distance_details_distance_total_imperial` | double |  |
| `skating_distance_details_distance_total_metric` | double |  |
| `skating_distance_details_distance_per60_imperial` | double |  |
| `skating_distance_details_distance_per60_metric` | double |  |
| `skating_distance_details_distance_max_game_imperial` | double |  |
| `skating_distance_details_distance_max_game_metric` | double |  |
| `skating_distance_details_distance_max_game_overlay_player_first_name_default` | character |  |
| `skating_distance_details_distance_max_game_overlay_player_last_name_default` | character |  |
| `skating_distance_details_distance_max_game_overlay_game_date` | character |  |
| `skating_distance_details_distance_max_game_overlay_away_team_abbrev` | character |  |
| `skating_distance_details_distance_max_game_overlay_away_team_score` | integer |  |
| `skating_distance_details_distance_max_game_overlay_home_team_abbrev` | character |  |
| `skating_distance_details_distance_max_game_overlay_home_team_score` | integer |  |
| `skating_distance_details_distance_max_game_overlay_game_outcome_last_period_type` | character |  |
| `skating_distance_details_distance_max_game_overlay_game_outcome_ot_periods` | integer |  |
| `skating_distance_details_distance_max_game_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `skating_distance_details_distance_max_game_overlay_period_descriptor_number` | integer |  |
| `skating_distance_details_distance_max_game_overlay_period_descriptor_period_type` | character |  |
| `skating_distance_details_distance_max_game_overlay_game_type` | integer |  |
| `skating_distance_details_distance_max_period_imperial` | double |  |
| `skating_distance_details_distance_max_period_metric` | double |  |
| `skating_distance_details_distance_max_period_overlay_player_first_name_default` | character |  |
| `skating_distance_details_distance_max_period_overlay_player_last_name_default` | character |  |
| `skating_distance_details_distance_max_period_overlay_game_date` | character |  |
| `skating_distance_details_distance_max_period_overlay_away_team_abbrev` | character |  |
| `skating_distance_details_distance_max_period_overlay_away_team_score` | integer |  |
| `skating_distance_details_distance_max_period_overlay_home_team_abbrev` | character |  |
| `skating_distance_details_distance_max_period_overlay_home_team_score` | integer |  |
| `skating_distance_details_distance_max_period_overlay_game_outcome_last_period_type` | character |  |
| `skating_distance_details_distance_max_period_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `skating_distance_details_distance_max_period_overlay_period_descriptor_number` | integer |  |
| `skating_distance_details_distance_max_period_overlay_period_descriptor_period_type` | character |  |
| `skating_distance_details_distance_max_period_overlay_game_type` | integer |  |
| `zone_time_details_offensive_zone_pctg` | double |  |
| `zone_time_details_offensive_zone_league_avg` | double |  |
| `zone_time_details_neutral_zone_pctg` | double |  |
| `zone_time_details_neutral_zone_league_avg` | double |  |
| `zone_time_details_defensive_zone_pctg` | double |  |
| `zone_time_details_defensive_zone_league_avg` | double |  |
| `zone_starts_offensive_zone_starts` | double |  |
| `zone_starts_neutral_zone_starts` | double |  |
| `zone_starts_defensive_zone_starts` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `area` | character | Net/ice zone the shots were taken from. |
| `sog` | integer | Shots on goal from the area. |
| `goals` | integer | Goals scored. |
| `shooting_pctg` | double | Shooting percentage from the area. |
| `sog_percentile` | double | League percentile rank for shots on goal. |
| `goals_percentile` | double | League percentile rank for goals. |
| `shooting_pctg_percentile` | double | League percentile rank for shooting percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `hardest_shots` | character | Serialized list or JSON array of the player's hardest individual shot efforts, including speed and context metadata from NHL EDGE puck tracking. |
| `shot_speed_details_top_shot_speed_imperial` | double | Player's single highest recorded shot speed for the season measured in miles per hour (imperial), from NHL EDGE puck-tracking. |
| `shot_speed_details_top_shot_speed_metric` | double | Player's single highest recorded shot speed for the season measured in kilometers per hour (metric), from NHL EDGE puck-tracking. |
| `shot_speed_details_top_shot_speed_percentile` | double | Percentile rank of the player's top shot speed relative to all qualifying skaters in the NHL EDGE shot-speed dataset. |
| `shot_speed_details_top_shot_speed_league_avg_imperial` | double | League-average highest shot speed among qualifying skaters for the season, measured in miles per hour (imperial). |
| `shot_speed_details_top_shot_speed_league_avg_metric` | double | League-average highest shot speed among qualifying skaters for the season, measured in kilometers per hour (metric). |
| `shot_speed_details_top_shot_speed_overlay_player_first_name_default` | character | Player's first name as stored in the NHL api-web system, included in the overlay for the top-shot-speed event. |
| `shot_speed_details_top_shot_speed_overlay_player_last_name_default` | character | Player's last name as stored in the NHL api-web system, included in the overlay for the top-shot-speed event. |
| `shot_speed_details_top_shot_speed_overlay_game_date` | character | Date (YYYY-MM-DD) of the game in which the player recorded their top shot speed for the season. |
| `shot_speed_details_top_shot_speed_overlay_away_team_abbrev` | character | Three-letter abbreviation for the away team in the game where the player recorded their top shot speed this season. |
| `shot_speed_details_top_shot_speed_overlay_away_team_score` | integer | Away team's final score in the game where the player recorded their season-high shot speed. |
| `shot_speed_details_top_shot_speed_overlay_home_team_abbrev` | character | Three-letter abbreviation for the home team in the game where the player achieved their top shot speed this season. |
| `shot_speed_details_top_shot_speed_overlay_home_team_score` | integer | Home team's final score in the game where the player recorded their season-high shot speed. |
| `shot_speed_details_top_shot_speed_overlay_game_outcome_last_period_type` | character | Type of period that ended the game where the player set their top shot speed (e.g., 'REG', 'OT', 'SO'). |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods defined for the game type in which the player recorded their top shot speed. |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_number` | integer | Period number in which the player released their top-speed shot during the referenced game. |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_period_type` | character | Period type label (e.g., 'REG', 'OT') for the period in which the player recorded their top shot speed. |
| `shot_speed_details_top_shot_speed_overlay_time_in_period` | character | Time elapsed within the period (MM:SS) when the player released their top-speed shot for the season. |
| `shot_speed_details_top_shot_speed_overlay_game_type` | integer | Numeric code for the game type of the game in which the player recorded their top shot speed (e.g., 2 = regular season). |
| `shot_speed_details_avg_shot_speed_imperial` | double | Player's average shot speed across all tracked shot attempts for the season, measured in miles per hour (imperial). |
| `shot_speed_details_avg_shot_speed_metric` | double | Player's average shot speed across all tracked shot attempts for the season, measured in kilometers per hour (metric). |
| `shot_speed_details_avg_shot_speed_percentile` | double | Percentile rank of the player's average shot speed relative to all qualifying skaters in the NHL EDGE shot-speed dataset. |
| `shot_speed_details_avg_shot_speed_league_avg_imperial` | double | League-average shot speed across all qualifying skaters' tracked attempts for the season, measured in miles per hour (imperial). |
| `shot_speed_details_avg_shot_speed_league_avg_metric` | double | League-average shot speed across all qualifying skaters' tracked attempts for the season, measured in kilometers per hour (metric). |
| `shot_speed_details_shot_attempts_over100_value` | integer | Number of the player's tracked shot attempts for the season with a recorded speed exceeding 100 mph. |
| `shot_speed_details_shot_attempts_over100_percentile` | double | Percentile rank of the player's count of shot attempts exceeding 100 mph relative to all qualifying skaters in the NHL EDGE dataset. |
| `shot_speed_details_shot_attempts_over100_league_avg` | double | League-average number of shot attempts with a recorded speed above 100 mph among qualifying skaters for the season. |
| `shot_speed_details_shot_attempts90_to100_value` | integer | Number of the player's tracked shot attempts for the season with a recorded speed between 90 and 100 mph. |
| `shot_speed_details_shot_attempts90_to100_percentile` | double | Percentile rank of the player's count of shot attempts in the 90–100 mph speed band relative to all qualifying skaters in the NHL EDGE dataset. |
| `shot_speed_details_shot_attempts90_to100_league_avg` | double | League-average number of shot attempts falling in the 90–100 mph speed band among qualifying skaters for the season. |
| `shot_speed_details_shot_attempts80_to90_value` | integer | Number of the player's tracked shot attempts for the season with a recorded speed between 80 and 90 mph. |
| `shot_speed_details_shot_attempts80_to90_percentile` | double | Percentile rank of the player's count of shot attempts in the 80–90 mph speed band relative to all qualifying skaters in the NHL EDGE dataset. |
| `shot_speed_details_shot_attempts80_to90_league_avg` | double | League-average number of shot attempts falling in the 80–90 mph speed band among qualifying skaters for the season. |
| `shot_speed_details_shot_attempts70_to80_value` | integer | Number of the player's tracked shot attempts for the season with a recorded speed between 70 and 80 mph. |
| `shot_speed_details_shot_attempts70_to80_percentile` | double | Percentile rank of the player's count of shot attempts in the 70–80 mph speed band relative to all qualifying skaters in the NHL EDGE dataset. |
| `shot_speed_details_shot_attempts70_to80_league_avg` | double | League-average number of shot attempts falling in the 70–80 mph speed band among qualifying skaters for the season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `skating_distance_last10` | character |  |
| `skating_distance_details` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `top_skating_speeds` | character |  |
| `skating_speed_details_max_skating_speed_imperial` | double |  |
| `skating_speed_details_max_skating_speed_metric` | double |  |
| `skating_speed_details_max_skating_speed_percentile` | double |  |
| `skating_speed_details_max_skating_speed_league_avg_imperial` | double |  |
| `skating_speed_details_max_skating_speed_league_avg_metric` | double |  |
| `skating_speed_details_max_skating_speed_overlay_player_first_name_default` | character |  |
| `skating_speed_details_max_skating_speed_overlay_player_last_name_default` | character |  |
| `skating_speed_details_max_skating_speed_overlay_game_date` | character |  |
| `skating_speed_details_max_skating_speed_overlay_away_team_abbrev` | character |  |
| `skating_speed_details_max_skating_speed_overlay_away_team_score` | integer |  |
| `skating_speed_details_max_skating_speed_overlay_home_team_abbrev` | character |  |
| `skating_speed_details_max_skating_speed_overlay_home_team_score` | integer |  |
| `skating_speed_details_max_skating_speed_overlay_game_outcome_last_period_type` | character |  |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_number` | integer |  |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_period_type` | character |  |
| `skating_speed_details_max_skating_speed_overlay_time_in_period` | character |  |
| `skating_speed_details_max_skating_speed_overlay_game_type` | integer |  |
| `skating_speed_details_bursts_over22_value` | integer |  |
| `skating_speed_details_bursts_over22_percentile` | double |  |
| `skating_speed_details_bursts_over22_league_avg` | double |  |
| `skating_speed_details_bursts20_to22_value` | integer |  |
| `skating_speed_details_bursts20_to22_percentile` | double |  |
| `skating_speed_details_bursts20_to22_league_avg` | double |  |
| `skating_speed_details_bursts18_to20_value` | integer |  |
| `skating_speed_details_bursts18_to20_percentile` | double |  |
| `skating_speed_details_bursts18_to20_league_avg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `strength_code` | character | Strength state code (e.g., all, even, pp, pk). |
| `offensive_zone_pctg` | double | Percentage of time spent in the offensive zone. |
| `offensive_zone_percentile` | double | League percentile rank for offensive-zone time. |
| `offensive_zone_league_avg` | double | League average offensive-zone time percentage. |
| `neutral_zone_pctg` | double | Percentage of time spent in the neutral zone. |
| `neutral_zone_percentile` | double | League percentile rank for neutral-zone time. |
| `neutral_zone_league_avg` | double | League average neutral-zone time percentage. |
| `defensive_zone_pctg` | double | Percentage of time spent in the defensive zone. |
| `defensive_zone_percentile` | double | League percentile rank for defensive-zone time. |
| `defensive_zone_league_avg` | double | League average defensive-zone time percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `leaders_hardest_shot_player_id` | integer |  |
| `leaders_hardest_shot_player_first_name_default` | character |  |
| `leaders_hardest_shot_player_last_name_default` | character |  |
| `leaders_hardest_shot_player_sweater_number` | integer |  |
| `leaders_hardest_shot_player_position` | character |  |
| `leaders_hardest_shot_player_slug` | character |  |
| `leaders_hardest_shot_player_headshot` | character |  |
| `leaders_hardest_shot_player_team_common_name_default` | character |  |
| `leaders_hardest_shot_player_team_place_name_with_preposition_default` | character |  |
| `leaders_hardest_shot_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_hardest_shot_player_team_abbrev` | character |  |
| `leaders_hardest_shot_player_team_team_logo_light` | character |  |
| `leaders_hardest_shot_player_team_team_logo_dark` | character |  |
| `leaders_hardest_shot_overlay_player_first_name_default` | character |  |
| `leaders_hardest_shot_overlay_player_last_name_default` | character |  |
| `leaders_hardest_shot_overlay_game_date` | character |  |
| `leaders_hardest_shot_overlay_away_team_abbrev` | character |  |
| `leaders_hardest_shot_overlay_away_team_score` | integer |  |
| `leaders_hardest_shot_overlay_home_team_abbrev` | character |  |
| `leaders_hardest_shot_overlay_home_team_score` | integer |  |
| `leaders_hardest_shot_overlay_game_outcome_last_period_type` | character |  |
| `leaders_hardest_shot_overlay_game_outcome_ot_periods` | integer |  |
| `leaders_hardest_shot_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `leaders_hardest_shot_overlay_period_descriptor_number` | integer |  |
| `leaders_hardest_shot_overlay_period_descriptor_period_type` | character |  |
| `leaders_hardest_shot_overlay_time_in_period` | character |  |
| `leaders_hardest_shot_overlay_game_type` | integer |  |
| `leaders_hardest_shot_shot_speed_imperial` | double |  |
| `leaders_hardest_shot_shot_speed_metric` | double |  |
| `leaders_max_skating_speed_player_id` | integer |  |
| `leaders_max_skating_speed_player_first_name_default` | character |  |
| `leaders_max_skating_speed_player_last_name_default` | character |  |
| `leaders_max_skating_speed_player_sweater_number` | integer |  |
| `leaders_max_skating_speed_player_position` | character |  |
| `leaders_max_skating_speed_player_slug` | character |  |
| `leaders_max_skating_speed_player_headshot` | character |  |
| `leaders_max_skating_speed_player_team_common_name_default` | character |  |
| `leaders_max_skating_speed_player_team_place_name_with_preposition_default` | character |  |
| `leaders_max_skating_speed_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_max_skating_speed_player_team_abbrev` | character |  |
| `leaders_max_skating_speed_player_team_team_logo_light` | character |  |
| `leaders_max_skating_speed_player_team_team_logo_dark` | character |  |
| `leaders_max_skating_speed_overlay_player_first_name_default` | character |  |
| `leaders_max_skating_speed_overlay_player_last_name_default` | character |  |
| `leaders_max_skating_speed_overlay_game_date` | character |  |
| `leaders_max_skating_speed_overlay_away_team_abbrev` | character |  |
| `leaders_max_skating_speed_overlay_away_team_score` | integer |  |
| `leaders_max_skating_speed_overlay_home_team_abbrev` | character |  |
| `leaders_max_skating_speed_overlay_home_team_score` | integer |  |
| `leaders_max_skating_speed_overlay_game_outcome_last_period_type` | character |  |
| `leaders_max_skating_speed_overlay_game_outcome_ot_periods` | integer |  |
| `leaders_max_skating_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `leaders_max_skating_speed_overlay_period_descriptor_number` | integer |  |
| `leaders_max_skating_speed_overlay_period_descriptor_period_type` | character |  |
| `leaders_max_skating_speed_overlay_time_in_period` | character |  |
| `leaders_max_skating_speed_overlay_game_type` | integer |  |
| `leaders_max_skating_speed_skating_speed_imperial` | double |  |
| `leaders_max_skating_speed_skating_speed_metric` | double |  |
| `leaders_total_distance_skated_player_id` | integer |  |
| `leaders_total_distance_skated_player_first_name_default` | character |  |
| `leaders_total_distance_skated_player_last_name_default` | character |  |
| `leaders_total_distance_skated_player_sweater_number` | integer |  |
| `leaders_total_distance_skated_player_position` | character |  |
| `leaders_total_distance_skated_player_slug` | character |  |
| `leaders_total_distance_skated_player_headshot` | character |  |
| `leaders_total_distance_skated_player_team_common_name_default` | character |  |
| `leaders_total_distance_skated_player_team_place_name_with_preposition_default` | character |  |
| `leaders_total_distance_skated_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_total_distance_skated_player_team_abbrev` | character |  |
| `leaders_total_distance_skated_player_team_team_logo_light` | character |  |
| `leaders_total_distance_skated_player_team_team_logo_dark` | character |  |
| `leaders_total_distance_skated_distance_skated_imperial` | double |  |
| `leaders_total_distance_skated_distance_skated_metric` | double |  |
| `leaders_distance_max_game_player_id` | integer |  |
| `leaders_distance_max_game_player_first_name_default` | character |  |
| `leaders_distance_max_game_player_last_name_default` | character |  |
| `leaders_distance_max_game_player_sweater_number` | integer |  |
| `leaders_distance_max_game_player_position` | character |  |
| `leaders_distance_max_game_player_slug` | character |  |
| `leaders_distance_max_game_player_headshot` | character |  |
| `leaders_distance_max_game_player_team_common_name_default` | character |  |
| `leaders_distance_max_game_player_team_place_name_with_preposition_default` | character |  |
| `leaders_distance_max_game_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_distance_max_game_player_team_abbrev` | character |  |
| `leaders_distance_max_game_player_team_team_logo_light` | character |  |
| `leaders_distance_max_game_player_team_team_logo_dark` | character |  |
| `leaders_distance_max_game_distance_skated_imperial` | double |  |
| `leaders_distance_max_game_distance_skated_metric` | double |  |
| `leaders_distance_max_game_overlay_player_first_name_default` | character |  |
| `leaders_distance_max_game_overlay_player_last_name_default` | character |  |
| `leaders_distance_max_game_overlay_game_date` | character |  |
| `leaders_distance_max_game_overlay_away_team_abbrev` | character |  |
| `leaders_distance_max_game_overlay_away_team_score` | integer |  |
| `leaders_distance_max_game_overlay_home_team_abbrev` | character |  |
| `leaders_distance_max_game_overlay_home_team_score` | integer |  |
| `leaders_distance_max_game_overlay_game_outcome_last_period_type` | character |  |
| `leaders_distance_max_game_overlay_game_outcome_ot_periods` | integer |  |
| `leaders_distance_max_game_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `leaders_distance_max_game_overlay_period_descriptor_number` | integer |  |
| `leaders_distance_max_game_overlay_period_descriptor_period_type` | character |  |
| `leaders_distance_max_game_overlay_game_type` | integer |  |
| `leaders_high_danger_sog_player_id` | integer |  |
| `leaders_high_danger_sog_player_first_name_default` | character |  |
| `leaders_high_danger_sog_player_last_name_default` | character |  |
| `leaders_high_danger_sog_player_sweater_number` | integer |  |
| `leaders_high_danger_sog_player_position` | character |  |
| `leaders_high_danger_sog_player_slug` | character |  |
| `leaders_high_danger_sog_player_headshot` | character |  |
| `leaders_high_danger_sog_player_team_common_name_default` | character |  |
| `leaders_high_danger_sog_player_team_place_name_with_preposition_default` | character |  |
| `leaders_high_danger_sog_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_high_danger_sog_player_team_abbrev` | character |  |
| `leaders_high_danger_sog_player_team_team_logo_light` | character |  |
| `leaders_high_danger_sog_player_team_team_logo_dark` | character |  |
| `leaders_high_danger_sog_sog` | integer |  |
| `leaders_high_danger_sog_shot_location_details` | character |  |
| `leaders_offensive_zone_time_player_id` | integer |  |
| `leaders_offensive_zone_time_player_first_name_default` | character |  |
| `leaders_offensive_zone_time_player_last_name_default` | character |  |
| `leaders_offensive_zone_time_player_sweater_number` | integer |  |
| `leaders_offensive_zone_time_player_position` | character |  |
| `leaders_offensive_zone_time_player_slug` | character |  |
| `leaders_offensive_zone_time_player_headshot` | character |  |
| `leaders_offensive_zone_time_player_team_common_name_default` | character |  |
| `leaders_offensive_zone_time_player_team_place_name_with_preposition_default` | character |  |
| `leaders_offensive_zone_time_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_offensive_zone_time_player_team_abbrev` | character |  |
| `leaders_offensive_zone_time_player_team_team_logo_light` | character |  |
| `leaders_offensive_zone_time_player_team_team_logo_dark` | character |  |
| `leaders_offensive_zone_time_zone_time` | double |  |
| `leaders_defensive_zone_time_player_id` | integer |  |
| `leaders_defensive_zone_time_player_first_name_default` | character |  |
| `leaders_defensive_zone_time_player_last_name_default` | character |  |
| `leaders_defensive_zone_time_player_sweater_number` | integer |  |
| `leaders_defensive_zone_time_player_position` | character |  |
| `leaders_defensive_zone_time_player_slug` | character |  |
| `leaders_defensive_zone_time_player_headshot` | character |  |
| `leaders_defensive_zone_time_player_team_common_name_default` | character |  |
| `leaders_defensive_zone_time_player_team_place_name_with_preposition_default` | character |  |
| `leaders_defensive_zone_time_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_defensive_zone_time_player_team_abbrev` | character |  |
| `leaders_defensive_zone_time_player_team_team_logo_light` | character |  |
| `leaders_defensive_zone_time_player_team_team_logo_dark` | character |  |
| `leaders_defensive_zone_time_zone_time` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | Serialized list of seasons for which NHL EDGE player-tracking data is available for this goalie. |
| `shot_location_summary` | character | Serialized summary of shot-location zones faced by the goalie, aggregated from NHL EDGE tracking data. |
| `shot_location_details` | character | Serialized detailed breakdown of shot locations faced by the goalie, derived from NHL EDGE tracking data. |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character | Participant birth date (YYYY-MM-DD). |
| `player_shoots_catches` | character | Hand on which the goalie catches (glove side), typically 'L' for left or 'R' for right. |
| `player_sweater_number` | integer | Player jersey number. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_wins` | integer | Number of wins credited to the goalie for the season in this NHL EDGE detail record. |
| `player_losses` | integer | Number of regulation losses credited to the goalie for the season in this NHL EDGE detail record. |
| `player_overtime_losses` | integer | Number of overtime or shootout losses (OTL) credited to the goalie during the season. |
| `player_goals_against_avg` | double | Goals-against average (GAA) for the goalie during the season, reflecting the average number of goals allowed per 60 minutes played. |
| `player_save_pctg` | double | Save percentage (SV%) for the goalie during the season, expressed as a decimal ratio of saves to shots faced. |
| `player_games_played` | integer | Total number of regular-season games the goalie appeared in for the season covered by this NHL EDGE detail record. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `stats_goals_against_avg_value` | double | Goalie's goals-against average value as reported in the NHL EDGE detail stat block (mirrors player_goals_against_avg at the EDGE layer). |
| `stats_goals_against_avg_percentile` | double | Percentile rank among all NHL goalies for goals-against average as reported in the NHL EDGE detail. |
| `stats_goals_against_avg_league_avg` | double | League-average GAA value used as the EDGE comparative baseline for this goalie's goals-against-average metric. |
| `stats_games_above900_value` | double | Goalie's own count of games in which save percentage exceeded .900, as tracked by the NHL EDGE system. |
| `stats_games_above900_percentile` | double | Percentile rank among all NHL goalies for the 'games above .900 save percentage' EDGE metric during the season. |
| `stats_games_above900_league_avg` | double | League-average value for the 'games above .900 save percentage' EDGE metric, used as a comparative baseline for the goalie. |
| `stats_goal_differential_per60_value` | double | Goalie's net goal differential (team goals scored minus goals allowed while in net) per 60 minutes of play, as tracked by the NHL EDGE system. |
| `stats_goal_differential_per60_percentile` | double | Percentile rank among all NHL goalies for the goals-differential-per-60 EDGE metric during the season. |
| `stats_goal_differential_per60_league_avg` | double | League-average net goal differential (team goals scored minus goals allowed while in net) per 60 minutes, the EDGE baseline comparator for the goalie. |
| `stats_goal_support_avg_value` | double | Average number of goals scored by the goalie's team per game while this goalie was in net, as tracked by NHL EDGE. |
| `stats_goal_support_avg_percentile` | double | Percentile rank among all NHL goalies for average goal support received while the goalie was in net. |
| `stats_goal_support_avg_league_avg` | double | League-average goal-support value (average goals scored for the goalie while in net), used as the EDGE baseline comparator. |
| `stats_point_pctg_value` | double | Team points percentage in games started by this goalie during the season, as tracked by the NHL EDGE system. |
| `stats_point_pctg_percentile` | double | Percentile rank among all NHL goalies for team points percentage in games the goalie started, per NHL EDGE. |
| `stats_point_pctg_league_avg` | double | League-average points percentage (team winning percentage when the goalie starts) used as the EDGE comparative baseline. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `save_pctg5v5_last10` | character |  |
| `save_pctg5v5_details_save_pctg_value` | double |  |
| `save_pctg5v5_details_save_pctg_league_avg` | double |  |
| `save_pctg5v5_details_save_pctg_percentile` | double |  |
| `save_pctg5v5_details_save_pctg_close_value` | double |  |
| `save_pctg5v5_details_save_pctg_close_league_avg` | double |  |
| `save_pctg5v5_details_save_pctg_close_percentile` | double |  |
| `save_pctg5v5_details_shots_value` | integer |  |
| `save_pctg5v5_details_shots_league_avg` | integer |  |
| `save_pctg5v5_details_shots_percentile` | double |  |
| `save_pctg5v5_details_shots_per60_value` | double |  |
| `save_pctg5v5_details_shots_per60_league_avg` | double |  |
| `save_pctg5v5_details_shots_per60_percentile` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `shot_location_summary` | character |  |
| `shot_location_details` | character |  |
| `save_pctg5v5_last10` | character |  |
| `save_pctg_last10` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character | Participant birth date (YYYY-MM-DD). |
| `player_shoots_catches` | character |  |
| `player_sweater_number` | integer | Player jersey number. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_wins` | integer |  |
| `player_losses` | integer |  |
| `player_overtime_losses` | integer |  |
| `player_goals_against_avg` | double |  |
| `player_save_pctg` | double |  |
| `player_games_played` | integer |  |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `save_pctg5v5_details_save_pctg` | double |  |
| `save_pctg5v5_details_save_pctg_close` | double |  |
| `save_pctg5v5_details_shots` | integer |  |
| `save_pctg5v5_details_shots_per60` | double |  |
| `save_pctg_details_games_above900` | integer |  |
| `save_pctg_details_pctg_games_above900` | double |  |
| `save_pctg_details_point_pctg` | double |  |
| `save_pctg_details_goals_against_avg` | double |  |
| `save_pctg_details_save_pctg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `save_pctg_last10` | character |  |
| `save_pctg_details_games_above900_value` | integer |  |
| `save_pctg_details_games_above900_percentile` | double |  |
| `save_pctg_details_games_above900_league_avg` | double |  |
| `save_pctg_details_pctg_games_above900_value` | double |  |
| `save_pctg_details_pctg_games_above900_percentile` | double |  |
| `save_pctg_details_pctg_games_above900_league_avg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `area` | character | Net/ice zone the shots were taken from. |
| `shots_against` | integer | Shots faced. |
| `saves` | integer | Saves made. |
| `goals_against` | integer | Goals against. |
| `save_pctg` | double | Save percentage. |
| `shots_against_percentile` | double | League percentile rank for shots against. |
| `saves_percentile` | double | League percentile rank for saves. |
| `goals_against_percentile` | double | League percentile rank for goals against. |
| `save_pctg_percentile` | double | League percentile rank for save percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `minimum_minutes_played` | integer |  |
| `leaders_high_danger_save_pctg_player_id` | integer |  |
| `leaders_high_danger_save_pctg_player_first_name_default` | character |  |
| `leaders_high_danger_save_pctg_player_last_name_default` | character |  |
| `leaders_high_danger_save_pctg_player_last_name_cs` | character |  |
| `leaders_high_danger_save_pctg_player_last_name_sk` | character |  |
| `leaders_high_danger_save_pctg_player_sweater_number` | integer |  |
| `leaders_high_danger_save_pctg_player_position` | character |  |
| `leaders_high_danger_save_pctg_player_slug` | character |  |
| `leaders_high_danger_save_pctg_player_headshot` | character |  |
| `leaders_high_danger_save_pctg_player_team_common_name_default` | character |  |
| `leaders_high_danger_save_pctg_player_team_place_name_with_preposition_default` | character |  |
| `leaders_high_danger_save_pctg_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_high_danger_save_pctg_player_team_abbrev` | character |  |
| `leaders_high_danger_save_pctg_player_team_team_logo_light` | character |  |
| `leaders_high_danger_save_pctg_player_team_team_logo_dark` | character |  |
| `leaders_high_danger_save_pctg_save_pctg` | double |  |
| `leaders_high_danger_save_pctg_shot_location_details` | character |  |
| `leaders_high_danger_saves_player_id` | integer |  |
| `leaders_high_danger_saves_player_first_name_default` | character |  |
| `leaders_high_danger_saves_player_last_name_default` | character |  |
| `leaders_high_danger_saves_player_sweater_number` | integer |  |
| `leaders_high_danger_saves_player_position` | character |  |
| `leaders_high_danger_saves_player_slug` | character |  |
| `leaders_high_danger_saves_player_headshot` | character |  |
| `leaders_high_danger_saves_player_team_common_name_default` | character |  |
| `leaders_high_danger_saves_player_team_place_name_with_preposition_default` | character |  |
| `leaders_high_danger_saves_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_high_danger_saves_player_team_abbrev` | character |  |
| `leaders_high_danger_saves_player_team_team_logo_light` | character |  |
| `leaders_high_danger_saves_player_team_team_logo_dark` | character |  |
| `leaders_high_danger_saves_saves` | integer |  |
| `leaders_high_danger_saves_shot_location_details` | character |  |
| `leaders_high_danger_goals_against_player_id` | integer |  |
| `leaders_high_danger_goals_against_player_first_name_default` | character |  |
| `leaders_high_danger_goals_against_player_last_name_default` | character |  |
| `leaders_high_danger_goals_against_player_sweater_number` | integer |  |
| `leaders_high_danger_goals_against_player_position` | character |  |
| `leaders_high_danger_goals_against_player_slug` | character |  |
| `leaders_high_danger_goals_against_player_headshot` | character |  |
| `leaders_high_danger_goals_against_player_team_common_name_default` | character |  |
| `leaders_high_danger_goals_against_player_team_place_name_with_preposition_default` | character |  |
| `leaders_high_danger_goals_against_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_high_danger_goals_against_player_team_abbrev` | character |  |
| `leaders_high_danger_goals_against_player_team_team_logo_light` | character |  |
| `leaders_high_danger_goals_against_player_team_team_logo_dark` | character |  |
| `leaders_high_danger_goals_against_goals_against` | integer |  |
| `leaders_save_pctg5v5_player_id` | integer |  |
| `leaders_save_pctg5v5_player_first_name_default` | character |  |
| `leaders_save_pctg5v5_player_last_name_default` | character |  |
| `leaders_save_pctg5v5_player_last_name_cs` | character |  |
| `leaders_save_pctg5v5_player_last_name_sk` | character |  |
| `leaders_save_pctg5v5_player_sweater_number` | integer |  |
| `leaders_save_pctg5v5_player_position` | character |  |
| `leaders_save_pctg5v5_player_slug` | character |  |
| `leaders_save_pctg5v5_player_headshot` | character |  |
| `leaders_save_pctg5v5_player_team_common_name_default` | character |  |
| `leaders_save_pctg5v5_player_team_place_name_with_preposition_default` | character |  |
| `leaders_save_pctg5v5_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_save_pctg5v5_player_team_abbrev` | character |  |
| `leaders_save_pctg5v5_player_team_team_logo_light` | character |  |
| `leaders_save_pctg5v5_player_team_team_logo_dark` | character |  |
| `leaders_save_pctg5v5_save_pctg` | double |  |
| `leaders_games_above900_player_id` | integer |  |
| `leaders_games_above900_player_first_name_default` | character |  |
| `leaders_games_above900_player_last_name_default` | character |  |
| `leaders_games_above900_player_sweater_number` | integer |  |
| `leaders_games_above900_player_position` | character |  |
| `leaders_games_above900_player_slug` | character |  |
| `leaders_games_above900_player_headshot` | character |  |
| `leaders_games_above900_player_team_common_name_default` | character |  |
| `leaders_games_above900_player_team_place_name_with_preposition_default` | character |  |
| `leaders_games_above900_player_team_place_name_with_preposition_fr` | character |  |
| `leaders_games_above900_player_team_abbrev` | character |  |
| `leaders_games_above900_player_team_team_logo_light` | character |  |
| `leaders_games_above900_player_team_team_logo_dark` | character |  |
| `leaders_games_above900_games` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | Comma-separated list of season identifiers for which NHL EDGE player-tracking statistics are available for this team. |
| `sog_summary` | character | Serialized summary string of total shots-on-goal across all periods for this team, flattened from the NHL api-web team detail payload. |
| `sog_details` | character | Serialized JSON-like string containing per-period or per-game shots-on-goal detail for this team, flattened from the NHL api-web team detail payload. |
| `team_id` | integer | Unique team identifier. |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `team_abbrev` | character | Team abbreviation. |
| `team_team_logo_light` | character | URL to the team light logo. |
| `team_team_logo_dark` | character | URL to the team dark logo. |
| `team_slug` | character | Team URL slug. |
| `team_conference` | character | Name of the NHL conference (e.g., Eastern, Western) to which this team belongs, as returned by the NHL api-web team detail endpoint. |
| `team_division` | character | Name of the NHL division (e.g., Atlantic, Metro, Central, Pacific) to which this team belongs, as returned by the NHL api-web team detail endpoint. |
| `team_wins` | integer | Team wins. |
| `team_losses` | integer | Team losses. |
| `team_ot_losses` | integer | Total number of games this team has lost in overtime or a shootout (earning one standings point each) in the current season. |
| `team_games_played` | integer | Total number of regular-season or playoff games this team has played in the current season, from the NHL api-web team detail endpoint. |
| `team_points` | integer | Total points scored by the player's team in this game. |
| `shot_speed_shot_attempts_over90_value` | integer | Total count of shot attempts recorded at a speed exceeding 90 mph by this team's players during the season, from NHL EDGE tracking data. |
| `shot_speed_shot_attempts_over90_rank` | integer | Team's league rank by number of shot attempts exceeding 90 mph in shot speed, with rank 1 indicating the highest count, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_imperial` | double | Fastest recorded shot speed by any player on this team during the season, expressed in miles per hour, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_metric` | double | Fastest recorded shot speed by any player on this team during the season, expressed in kilometers per hour, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_rank` | integer | Team's league rank by top shot speed for the season, where rank 1 indicates the team whose fastest shot was the quickest in the league. |
| `shot_speed_top_shot_speed_league_avg_imperial` | double | League-average of the top shot speed across all teams for the same period, expressed in miles per hour, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_league_avg_metric` | double | League-average of the top shot speed across all teams for the same period, expressed in kilometers per hour, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_overlay_player_first_name_default` | character | Default-language first name of the player who recorded this team's top shot speed, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_player_last_name_default` | character | Default-language last name of the player who recorded this team's top shot speed, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_game_date` | character | Calendar date of the game in which this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_away_team_abbrev` | character | Three-letter abbreviation of the away team in the game where this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_away_team_score` | integer | Away team's final score in the game where this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_home_team_abbrev` | character | Three-letter abbreviation of the home team in the game where this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_home_team_score` | integer | Home team's final score in the game where this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_game_outcome_last_period_type` | character | Type of the final period played (e.g., REG, OT, SO) in the game where this team's top shot speed was recorded. |
| `shot_speed_top_shot_speed_overlay_game_outcome_ot_periods` | integer | Number of overtime periods played in the game where this team's top shot speed was recorded, or zero if decided in regulation. |
| `shot_speed_top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods in the game format where this team's top shot speed was recorded (typically 3 for NHL). |
| `shot_speed_top_shot_speed_overlay_period_descriptor_number` | integer | Period number within the game during which this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_period_descriptor_period_type` | character | Type label for the period (e.g., REG, OT) during which this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_time_in_period` | character | Elapsed time within the period (MM:SS format) at which this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_game_type` | integer | Numeric game-type code (e.g., 2 = regular season, 3 = playoffs) for the game in which this team's top shot speed was recorded. |
| `skating_speed_bursts_over22_value` | integer | Total count of skating speed bursts exceeding 22 mph recorded by this team's skaters during the season, from NHL EDGE tracking data. |
| `skating_speed_bursts_over22_rank` | integer | Team's league rank by total count of skating speed bursts exceeding 22 mph, where rank 1 indicates the most elite-speed bursts, from NHL EDGE tracking data. |
| `skating_speed_bursts_over20_value` | integer | Total count of skating speed bursts exceeding 20 mph recorded by this team's skaters during the season, from NHL EDGE tracking data. |
| `skating_speed_bursts_over20_rank` | integer | Team's league rank by total count of skating speed bursts exceeding 20 mph, where rank 1 indicates the most such bursts, from NHL EDGE tracking data. |
| `skating_speed_bursts_over20_league_avg_value` | integer | League-average number of skating speed bursts exceeding 20 mph recorded per team over the same season window, from NHL EDGE tracking data. |
| `skating_speed_speed_max_imperial` | double | Fastest skating speed reached by any player on this team during the season, expressed in miles per hour, from NHL EDGE tracking data. |
| `skating_speed_speed_max_metric` | double | Fastest skating speed reached by any player on this team during the season, expressed in kilometers per hour, from NHL EDGE tracking data. |
| `skating_speed_speed_max_rank` | integer | Team's league rank by maximum skating speed for the season, where rank 1 indicates the team whose fastest skater reached the highest speed in the league. |
| `skating_speed_speed_max_league_avg_imperial` | double | League-average of the maximum skating speed across all teams for the same period, expressed in miles per hour, from NHL EDGE tracking data. |
| `skating_speed_speed_max_league_avg_metric` | double | League-average of the maximum skating speed across all teams for the same period, expressed in kilometers per hour, from NHL EDGE tracking data. |
| `skating_speed_speed_max_overlay_player_first_name_default` | character | Default-language first name of the player who recorded this team's top skating speed, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_player_last_name_default` | character | Default-language last name of the player who recorded this team's top skating speed, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_game_date` | character | Calendar date of the game in which this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_away_team_abbrev` | character | Three-letter abbreviation of the away team in the game where this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_away_team_score` | integer | Away team's final score in the game where this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_home_team_abbrev` | character | Three-letter abbreviation of the home team in the game where this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_home_team_score` | integer | Home team's final score in the game where this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_game_outcome_last_period_type` | character | Type of the final period played (e.g., REG, OT, SO) in the game where this team's top skating speed was recorded. |
| `skating_speed_speed_max_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods in the game format where this team's top skating speed was recorded (typically 3 for NHL). |
| `skating_speed_speed_max_overlay_period_descriptor_number` | integer | Period number within the game during which this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_period_descriptor_period_type` | character | Type label for the period (e.g., REG, OT) during which this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_time_in_period` | character | Elapsed time within the period (MM:SS format) at which this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_game_type` | integer | Numeric game-type code (e.g., 2 = regular season, 3 = playoffs) for the game in which this team's top skating speed was recorded. |
| `distance_skated_total_imperial` | double | Total cumulative skating distance logged by all skaters on this team across the season, expressed in miles (imperial), from NHL EDGE tracking data. |
| `distance_skated_total_metric` | double | Total cumulative skating distance logged by all skaters on this team across the season, expressed in kilometers (metric), from NHL EDGE tracking data. |
| `distance_skated_total_rank` | integer | Team's league rank by total cumulative skating distance for the season, where rank 1 indicates the team with the most distance skated. |
| `distance_skated_total_league_avg_imperial` | double | League-average cumulative skating distance for all teams over the same period as this team's totals, expressed in miles (imperial), from NHL EDGE tracking data. |
| `distance_skated_total_league_avg_metric` | double | League-average cumulative skating distance for all teams over the same period as this team's totals, expressed in kilometers (metric), from NHL EDGE tracking data. |
| `zone_time_details_offensive_zone_pctg` | double | Percentage of all-situation ice time this team spends in the offensive zone, as measured by NHL EDGE player-tracking data. |
| `zone_time_details_offensive_zone_rank` | integer | Team's league rank by overall offensive-zone time percentage (all situations), where rank 1 indicates the team with the most offensive-zone presence. |
| `zone_time_details_offensive_zone_league_avg` | double | League-average percentage of all-situation ice time that teams spend in the offensive zone, from NHL EDGE zone-time tracking data. |
| `zone_time_details_offensive_zone_ev_pctg` | double | Percentage of even-strength ice time this team spends in the offensive zone, as measured by NHL EDGE player-tracking data. |
| `zone_time_details_offensive_zone_ev_rank` | integer | Team's league rank by even-strength offensive-zone time percentage, where rank 1 indicates the team spending the most time in the offensive zone at even strength. |
| `zone_time_details_offensive_zone_ev_league_avg` | double | League-average percentage of even-strength ice time that teams spend in the offensive zone, from NHL EDGE zone-time tracking data. |
| `zone_time_details_neutral_zone_pctg` | double | Percentage of five-on-five ice time this team spends in the neutral zone, as measured by NHL EDGE player-tracking data. |
| `zone_time_details_neutral_zone_rank` | integer | Team's league rank by neutral-zone time percentage, where rank 1 indicates the team that spends the most time in the neutral zone during five-on-five play. |
| `zone_time_details_neutral_zone_league_avg` | double | League-average percentage of time that teams spend in the neutral zone during five-on-five play, from NHL EDGE zone-time tracking data. |
| `zone_time_details_defensive_zone_pctg` | double | Percentage of five-on-five ice time this team spends in its own defensive zone, as measured by NHL EDGE player-tracking data. |
| `zone_time_details_defensive_zone_rank` | integer | Team's league rank by defensive-zone time percentage, where rank 1 indicates the team that spends the most time in its own zone during five-on-five play. |
| `zone_time_details_defensive_zone_league_avg` | double | League-average percentage of time that teams spend in the defensive zone during five-on-five play, from NHL EDGE zone-time tracking data. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `leaders_shot_attempts_over90_team_id` | integer |  |
| `leaders_shot_attempts_over90_team_common_name_default` | character |  |
| `leaders_shot_attempts_over90_team_place_name_with_preposition_default` | character |  |
| `leaders_shot_attempts_over90_team_place_name_with_preposition_fr` | character |  |
| `leaders_shot_attempts_over90_team_abbrev` | character |  |
| `leaders_shot_attempts_over90_team_team_logo_light` | character |  |
| `leaders_shot_attempts_over90_team_team_logo_dark` | character |  |
| `leaders_shot_attempts_over90_team_slug` | character |  |
| `leaders_shot_attempts_over90_team_wins` | integer |  |
| `leaders_shot_attempts_over90_team_losses` | integer |  |
| `leaders_shot_attempts_over90_team_ot_losses` | integer |  |
| `leaders_shot_attempts_over90_attempts` | integer |  |
| `leaders_bursts_over22_team_id` | integer |  |
| `leaders_bursts_over22_team_common_name_default` | character |  |
| `leaders_bursts_over22_team_place_name_with_preposition_default` | character |  |
| `leaders_bursts_over22_team_place_name_with_preposition_fr` | character |  |
| `leaders_bursts_over22_team_abbrev` | character |  |
| `leaders_bursts_over22_team_team_logo_light` | character |  |
| `leaders_bursts_over22_team_team_logo_dark` | character |  |
| `leaders_bursts_over22_team_slug` | character |  |
| `leaders_bursts_over22_team_wins` | integer |  |
| `leaders_bursts_over22_team_losses` | integer |  |
| `leaders_bursts_over22_team_ot_losses` | integer |  |
| `leaders_bursts_over22_bursts` | integer |  |
| `leaders_distance_per60_team_id` | integer |  |
| `leaders_distance_per60_team_common_name_default` | character |  |
| `leaders_distance_per60_team_place_name_with_preposition_default` | character |  |
| `leaders_distance_per60_team_place_name_with_preposition_fr` | character |  |
| `leaders_distance_per60_team_abbrev` | character |  |
| `leaders_distance_per60_team_team_logo_light` | character |  |
| `leaders_distance_per60_team_team_logo_dark` | character |  |
| `leaders_distance_per60_team_slug` | character |  |
| `leaders_distance_per60_team_wins` | integer |  |
| `leaders_distance_per60_team_losses` | integer |  |
| `leaders_distance_per60_team_ot_losses` | integer |  |
| `leaders_distance_per60_distance_skated_imperial` | double |  |
| `leaders_distance_per60_distance_skated_metric` | double |  |
| `leaders_high_danger_sog_team_id` | integer |  |
| `leaders_high_danger_sog_team_common_name_default` | character |  |
| `leaders_high_danger_sog_team_place_name_with_preposition_default` | character |  |
| `leaders_high_danger_sog_team_place_name_with_preposition_fr` | character |  |
| `leaders_high_danger_sog_team_abbrev` | character |  |
| `leaders_high_danger_sog_team_team_logo_light` | character |  |
| `leaders_high_danger_sog_team_team_logo_dark` | character |  |
| `leaders_high_danger_sog_team_slug` | character |  |
| `leaders_high_danger_sog_team_wins` | integer |  |
| `leaders_high_danger_sog_team_losses` | integer |  |
| `leaders_high_danger_sog_team_ot_losses` | integer |  |
| `leaders_high_danger_sog_sog` | integer |  |
| `leaders_high_danger_sog_shot_location_details` | character |  |
| `leaders_offensive_zone_time_team_id` | integer |  |
| `leaders_offensive_zone_time_team_common_name_default` | character |  |
| `leaders_offensive_zone_time_team_place_name_with_preposition_default` | character |  |
| `leaders_offensive_zone_time_team_place_name_with_preposition_fr` | character |  |
| `leaders_offensive_zone_time_team_abbrev` | character |  |
| `leaders_offensive_zone_time_team_team_logo_light` | character |  |
| `leaders_offensive_zone_time_team_team_logo_dark` | character |  |
| `leaders_offensive_zone_time_team_slug` | character |  |
| `leaders_offensive_zone_time_team_wins` | integer |  |
| `leaders_offensive_zone_time_team_losses` | integer |  |
| `leaders_offensive_zone_time_team_ot_losses` | integer |  |
| `leaders_offensive_zone_time_zone_time` | double |  |
| `leaders_neutral_zone_time_team_id` | integer |  |
| `leaders_neutral_zone_time_team_common_name_default` | character |  |
| `leaders_neutral_zone_time_team_place_name_with_preposition_default` | character |  |
| `leaders_neutral_zone_time_team_place_name_with_preposition_fr` | character |  |
| `leaders_neutral_zone_time_team_abbrev` | character |  |
| `leaders_neutral_zone_time_team_team_logo_light` | character |  |
| `leaders_neutral_zone_time_team_team_logo_dark` | character |  |
| `leaders_neutral_zone_time_team_slug` | character |  |
| `leaders_neutral_zone_time_team_wins` | integer |  |
| `leaders_neutral_zone_time_team_losses` | integer |  |
| `leaders_neutral_zone_time_team_ot_losses` | integer |  |
| `leaders_neutral_zone_time_zone_time` | double |  |
| `leaders_defensive_zone_time_team_id` | integer |  |
| `leaders_defensive_zone_time_team_common_name_default` | character |  |
| `leaders_defensive_zone_time_team_place_name_with_preposition_default` | character |  |
| `leaders_defensive_zone_time_team_place_name_with_preposition_fr` | character |  |
| `leaders_defensive_zone_time_team_abbrev` | character |  |
| `leaders_defensive_zone_time_team_team_logo_light` | character |  |
| `leaders_defensive_zone_time_team_team_logo_dark` | character |  |
| `leaders_defensive_zone_time_team_slug` | character |  |
| `leaders_defensive_zone_time_team_wins` | integer |  |
| `leaders_defensive_zone_time_team_losses` | integer |  |
| `leaders_defensive_zone_time_team_ot_losses` | integer |  |
| `leaders_defensive_zone_time_zone_time` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `area` | character | Net/ice zone the shots were taken from. |
| `sog` | integer | Shots on goal from the area. |
| `sog_rank` | integer | League rank for shots on goal from the area. |
| `goals` | integer | Goals scored. |
| `goals_rank` | integer | League rank for goals scored from the area. |
| `shooting_pctg` | double | Shooting percentage from the area. |
| `shooting_pctg_rank` | integer | League rank for shooting percentage from the area. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `hardest_shots` | character |  |
| `shot_speed_details` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `strength_code` | character | Strength state code (e.g., all, even, pp, pk). |
| `offensive_zone_pctg` | double | Percentage of time spent in the offensive zone. |
| `offensive_zone_rank` | integer | League rank for offensive zone time. |
| `offensive_zone_league_avg` | double | League average offensive-zone time percentage. |
| `neutral_zone_pctg` | double | Percentage of time spent in the neutral zone. |
| `neutral_zone_rank` | integer | League rank for neutral zone time. |
| `neutral_zone_league_avg` | double | League average neutral-zone time percentage. |
| `defensive_zone_pctg` | double | Percentage of time spent in the defensive zone. |
| `defensive_zone_rank` | integer | League rank for defensive zone time. |
| `defensive_zone_league_avg` | double | League average defensive-zone time percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_edge_top10`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `sog_summary` | character |  |
| `sog_details` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character | Participant birth date (YYYY-MM-DD). |
| `player_shoots_catches` | character |  |
| `player_sweater_number` | integer | Player jersey number. |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_goals` | integer |  |
| `player_assists` | integer |  |
| `player_points` | integer | Player points. |
| `player_games_played` | integer |  |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `top_shot_speed_imperial` | double |  |
| `top_shot_speed_metric` | double |  |
| `top_shot_speed_percentile` | double |  |
| `top_shot_speed_league_avg_imperial` | double |  |
| `top_shot_speed_league_avg_metric` | double |  |
| `skating_speed_speed_max_imperial` | double |  |
| `skating_speed_speed_max_metric` | double |  |
| `skating_speed_speed_max_percentile` | double |  |
| `skating_speed_speed_max_league_avg_imperial` | double |  |
| `skating_speed_speed_max_league_avg_metric` | double |  |
| `skating_speed_speed_max_overlay_player_first_name_default` | character |  |
| `skating_speed_speed_max_overlay_player_last_name_default` | character |  |
| `skating_speed_speed_max_overlay_time_in_period` | character |  |
| `skating_speed_bursts_over20_value` | integer |  |
| `skating_speed_bursts_over20_percentile` | double |  |
| `skating_speed_bursts_over20_league_avg_value` | double |  |
| `total_distance_skated_imperial` | double |  |
| `total_distance_skated_metric` | double |  |
| `total_distance_skated_percentile` | double |  |
| `total_distance_skated_league_avg_imperial` | double |  |
| `total_distance_skated_league_avg_metric` | double |  |
| `zone_time_details_offensive_zone_pctg` | double |  |
| `zone_time_details_offensive_zone_percentile` | double |  |
| `zone_time_details_offensive_zone_league_avg` | double |  |
| `zone_time_details_neutral_zone_pctg` | double |  |
| `zone_time_details_neutral_zone_percentile` | double |  |
| `zone_time_details_neutral_zone_league_avg` | double |  |
| `zone_time_details_defensive_zone_pctg` | double |  |
| `zone_time_details_defensive_zone_percentile` | double |  |
| `zone_time_details_defensive_zone_league_avg` | double |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character |  |
| `shot_location_summary` | character |  |
| `shot_location_details` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character | Participant birth date (YYYY-MM-DD). |
| `player_shoots_catches` | character |  |
| `player_sweater_number` | integer | Player jersey number. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_wins` | integer |  |
| `player_losses` | integer |  |
| `player_overtime_losses` | integer |  |
| `player_goals_against_avg` | double |  |
| `player_save_pctg` | double |  |
| `player_games_played` | integer |  |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_edge_cat_goalie_detail(player_id=8480801)
```

_Last validated n/a._
