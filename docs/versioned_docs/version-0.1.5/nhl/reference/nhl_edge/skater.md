---
title: "NHL — NHL EDGE API — Skater: detail–zone"
sidebar_label: "Skater: detail–zone"
sidebar_position: 3
description: "NHL — NHL EDGE API — Skater: detail–zone — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL EDGE API — Skater: detail–zone

## nhl_edge_skater_detail

Pull EDGE detail stats for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-detail/8480801/now](https://api-web.nhle.com/v1/edge/skater-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | Comma-separated list or serialized array of seasons for which NHL EDGE player-tracking data is available for this skater. |
| `sog_summary` | character | Serialized summary-level shots-on-goal statistics for the skater, as returned in the NHL EDGE skater detail payload. |
| `sog_details` | character | Serialized detail breakdown of shots on goal by game or other sub-category, as returned in the NHL EDGE skater detail payload. |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character |  |
| `player_shoots_catches` | character | Handedness indicator for the skater showing the side they shoot from ('L' for left, 'R' for right). |
| `player_sweater_number` | integer | Player jersey number. |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_goals` | integer | Total regular-season goals scored by the skater in the current NHL season, as returned in the EDGE skater detail. |
| `player_assists` | integer | Total regular-season assists recorded by the skater in the current NHL season, as returned in the EDGE skater detail. |
| `player_points` | integer |  |
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

### Example {#nhl_edge_skater_detail-example}

```python
nhl_edge_skater_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_skater_comparison

Pull EDGE comparison data for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-comparison/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-comparison/8480801/now](https://api-web.nhle.com/v1/edge/skater-comparison/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_comparison-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | Serialized list of seasons for which NHL EDGE player-tracking data is available for the skater. |
| `skating_distance_last10` | character | Serialized skating-distance trend data for the skater's most recent 10 games. |
| `shot_location_details` | character | Serialized shot-location breakdown object containing zone-based shot attempt counts and percentages. |
| `shot_location_totals` | character | Serialized aggregate shot-location totals across all zones for the skater. |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character |  |
| `player_shoots_catches` | character | Handedness of the skater's shot or, for goalies, their catching hand (L or R). |
| `player_sweater_number` | integer | Player jersey number. |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_goals` | integer | Total regular-season goals scored by the skater in the current season. |
| `player_assists` | integer | Total regular-season assists recorded by the skater in the current season. |
| `player_points` | integer |  |
| `player_games_played` | integer | Number of regular-season games the skater appeared in during the current season. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `player_team_slug` | character | Player team URL-friendly slug. |
| `shot_speed_details_top_shot_speed_imperial` | double | Skater's single highest recorded shot speed in miles per hour for the season. |
| `shot_speed_details_top_shot_speed_metric` | double | Skater's single highest recorded shot speed in kilometres per hour for the season. |
| `shot_speed_details_top_shot_speed_overlay_player_first_name_default` | character | Skater's first name as displayed in the top-shot-speed overlay. |
| `shot_speed_details_top_shot_speed_overlay_player_last_name_default` | character | Skater's last name as displayed in the top-shot-speed overlay. |
| `shot_speed_details_top_shot_speed_overlay_game_date` | character | Calendar date of the game in which the skater recorded their top shot speed. |
| `shot_speed_details_top_shot_speed_overlay_away_team_abbrev` | character | Abbreviation of the away team in the game where the skater recorded their top shot speed. |
| `shot_speed_details_top_shot_speed_overlay_away_team_score` | integer | Away team's final score in the game where the skater recorded their top shot speed. |
| `shot_speed_details_top_shot_speed_overlay_home_team_abbrev` | character | Abbreviation of the home team in the game where the skater recorded their top shot speed. |
| `shot_speed_details_top_shot_speed_overlay_home_team_score` | integer | Home team's final score in the game where the skater recorded their top shot speed. |
| `shot_speed_details_top_shot_speed_overlay_game_outcome_last_period_type` | character | Period type (REG, OT, or SO) of the final period in the game where the skater's top shot speed was recorded. |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods played in the top-shot-speed game (typically 3). |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_number` | integer | Period number in which the skater recorded their top shot speed. |
| `shot_speed_details_top_shot_speed_overlay_period_descriptor_period_type` | character | Period type label (REG, OT, or SO) of the period in which the skater's top shot speed occurred. |
| `shot_speed_details_top_shot_speed_overlay_time_in_period` | character | Elapsed time within the period (mm:ss) when the skater's top shot speed was recorded. |
| `shot_speed_details_top_shot_speed_overlay_game_type` | integer | Numeric game-type code (e.g. 2 for regular season, 3 for playoffs) for the top-shot-speed game. |
| `shot_speed_details_avg_shot_speed_imperial` | double | Skater's average shot speed in miles per hour, measured by NHL EDGE puck-tracking. |
| `shot_speed_details_avg_shot_speed_metric` | double | Skater's average shot speed in kilometres per hour, measured by NHL EDGE puck-tracking. |
| `shot_speed_details_shot_attempts_over100` | integer | Number of shot attempts the skater recorded at speeds exceeding 100 mph. |
| `shot_speed_details_shot_attempts90_to100` | integer | Number of shot attempts the skater recorded at speeds between 90 and 100 mph. |
| `shot_speed_details_shot_attempts80_to90` | integer | Number of shot attempts the skater recorded at speeds between 80 and 90 mph. |
| `shot_speed_details_shot_attempts70_to80` | integer | Number of shot attempts the skater recorded at speeds between 70 and 80 mph. |
| `skating_speed_details_max_skating_speed_imperial` | double | Skater's single highest skating speed recorded during the season, in miles per hour. |
| `skating_speed_details_max_skating_speed_metric` | double | Skater's single highest skating speed recorded during the season, in kilometres per hour. |
| `skating_speed_details_max_skating_speed_overlay_player_first_name_default` | character | Skater's first name as displayed in the top-skating-speed overlay. |
| `skating_speed_details_max_skating_speed_overlay_player_last_name_default` | character | Skater's last name as displayed in the top-skating-speed overlay. |
| `skating_speed_details_max_skating_speed_overlay_game_date` | character | Calendar date of the game in which the skater reached their maximum skating speed. |
| `skating_speed_details_max_skating_speed_overlay_away_team_abbrev` | character | Abbreviation of the away team in the game where the skater recorded their top skating speed. |
| `skating_speed_details_max_skating_speed_overlay_away_team_score` | integer | Away team's final score in the game where the skater recorded their top skating speed. |
| `skating_speed_details_max_skating_speed_overlay_home_team_abbrev` | character | Abbreviation of the home team in the game where the skater recorded their top skating speed. |
| `skating_speed_details_max_skating_speed_overlay_home_team_score` | integer | Home team's final score in the game where the skater recorded their top skating speed. |
| `skating_speed_details_max_skating_speed_overlay_game_outcome_last_period_type` | character | Period type (REG, OT, or SO) of the final period in the game where the skater's top speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods in the game where the skater reached their top skating speed (typically 3). |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_number` | integer | Period number in which the skater reached their maximum skating speed. |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_period_type` | character | Period type label (REG or OT) for the period in which the skater's top speed occurred. |
| `skating_speed_details_max_skating_speed_overlay_time_in_period` | character | Elapsed time within the period (mm:ss) when the skater reached their maximum skating speed. |
| `skating_speed_details_max_skating_speed_overlay_game_type` | integer | Numeric game-type code for the game in which the skater recorded their maximum skating speed. |
| `skating_speed_details_bursts_over22` | integer | Number of skating speed bursts the skater recorded exceeding 22 mph during the season. |
| `skating_speed_details_bursts20_to22` | integer | Number of skating speed bursts the skater recorded between 20 and 22 mph during the season. |
| `skating_speed_details_bursts18_to20` | integer | Number of skating speed bursts the skater recorded between 18 and 20 mph during the season. |
| `skating_distance_details_distance_total_imperial` | double | Total cumulative skating distance the skater covered across all games in the season, in miles. |
| `skating_distance_details_distance_total_metric` | double | Total cumulative skating distance the skater covered across all games in the season, in kilometres. |
| `skating_distance_details_distance_per60_imperial` | double | Skater's average skating distance per 60 minutes of ice time during the season, in miles. |
| `skating_distance_details_distance_per60_metric` | double | Skater's average skating distance per 60 minutes of ice time during the season, in kilometres. |
| `skating_distance_details_distance_max_game_imperial` | double | Greatest total distance the skater covered in any single game during the season, in miles. |
| `skating_distance_details_distance_max_game_metric` | double | Greatest total distance the skater covered in any single game during the season, in kilometres. |
| `skating_distance_details_distance_max_game_overlay_player_first_name_default` | character | Skater's first name as displayed in the single-game maximum distance overlay. |
| `skating_distance_details_distance_max_game_overlay_player_last_name_default` | character | Skater's last name as displayed in the single-game maximum distance overlay. |
| `skating_distance_details_distance_max_game_overlay_game_date` | character | Calendar date of the game in which the skater covered their maximum single-game distance. |
| `skating_distance_details_distance_max_game_overlay_away_team_abbrev` | character | Abbreviation of the away team in the game where the skater set their single-game distance record. |
| `skating_distance_details_distance_max_game_overlay_away_team_score` | integer | Away team's final score in the game where the skater set their single-game distance record. |
| `skating_distance_details_distance_max_game_overlay_home_team_abbrev` | character | Abbreviation of the home team in the game where the skater set their single-game distance record. |
| `skating_distance_details_distance_max_game_overlay_home_team_score` | integer | Home team's final score in the game where the skater set their single-game distance record. |
| `skating_distance_details_distance_max_game_overlay_game_outcome_last_period_type` | character | Period type (REG, OT, or SO) of the final period in the skater's maximum single-game distance game. |
| `skating_distance_details_distance_max_game_overlay_game_outcome_ot_periods` | integer | Number of overtime periods played in the game where the skater set their single-game distance record. |
| `skating_distance_details_distance_max_game_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods in the game where the skater covered their greatest single-game distance. |
| `skating_distance_details_distance_max_game_overlay_period_descriptor_number` | integer | Period number being referenced in the skater's maximum single-game distance overlay context. |
| `skating_distance_details_distance_max_game_overlay_period_descriptor_period_type` | character | Period type label for the period referenced in the skater's maximum single-game distance overlay. |
| `skating_distance_details_distance_max_game_overlay_game_type` | integer | Numeric game-type code for the game in which the skater covered their maximum single-game distance. |
| `skating_distance_details_distance_max_period_imperial` | double | Greatest distance the skater covered in any single period during the season, in miles. |
| `skating_distance_details_distance_max_period_metric` | double | Greatest distance the skater covered in any single period during the season, in kilometres. |
| `skating_distance_details_distance_max_period_overlay_player_first_name_default` | character | Skater's first name as displayed in the single-period maximum distance overlay. |
| `skating_distance_details_distance_max_period_overlay_player_last_name_default` | character | Skater's last name as displayed in the single-period maximum distance overlay. |
| `skating_distance_details_distance_max_period_overlay_game_date` | character | Calendar date of the game in which the skater covered their maximum single-period distance. |
| `skating_distance_details_distance_max_period_overlay_away_team_abbrev` | character | Abbreviation of the away team in the game where the skater set their single-period distance record. |
| `skating_distance_details_distance_max_period_overlay_away_team_score` | integer | Away team's final score in the game where the skater set their single-period distance record. |
| `skating_distance_details_distance_max_period_overlay_home_team_abbrev` | character | Abbreviation of the home team in the game where the skater set their single-period distance record. |
| `skating_distance_details_distance_max_period_overlay_home_team_score` | integer | Home team's final score in the game where the skater set their single-period distance record. |
| `skating_distance_details_distance_max_period_overlay_game_outcome_last_period_type` | character | Period type (REG, OT, or SO) of the final period in the skater's maximum single-period distance game. |
| `skating_distance_details_distance_max_period_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods in the game where the skater covered their greatest single-period distance. |
| `skating_distance_details_distance_max_period_overlay_period_descriptor_number` | integer | Period number in which the skater covered their maximum single-period skating distance. |
| `skating_distance_details_distance_max_period_overlay_period_descriptor_period_type` | character | Period type label (REG, OT) for the period in which the skater's single-period distance record was set. |
| `skating_distance_details_distance_max_period_overlay_game_type` | integer | Numeric game-type code for the game in which the skater covered their maximum single-period distance. |
| `zone_time_details_offensive_zone_pctg` | double | Percentage of the skater's total ice time spent in the offensive zone, per EDGE tracking. |
| `zone_time_details_offensive_zone_league_avg` | double | League-average percentage of ice time that skaters spend in the offensive zone. |
| `zone_time_details_neutral_zone_pctg` | double | Percentage of the skater's total ice time spent in the neutral zone, per EDGE tracking. |
| `zone_time_details_neutral_zone_league_avg` | double | League-average percentage of ice time that skaters spend in the neutral zone. |
| `zone_time_details_defensive_zone_pctg` | double | Percentage of the skater's total ice time spent in the defensive zone, per EDGE tracking. |
| `zone_time_details_defensive_zone_league_avg` | double | League-average percentage of ice time that skaters spend in the defensive zone. |
| `zone_starts_offensive_zone_starts` | double | Percentage of the skater's on-ice faceoffs that were taken in the offensive zone. |
| `zone_starts_neutral_zone_starts` | double | Percentage of the skater's on-ice faceoffs that were taken in the neutral zone. |
| `zone_starts_defensive_zone_starts` | double | Percentage of the skater's on-ice faceoffs that were taken in the defensive zone. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_skater_comparison-example}

```python
nhl_edge_skater_comparison(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_skater_shot_location_detail

Pull EDGE shot-location detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-location-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-location-detail/8480801/now](https://api-web.nhle.com/v1/edge/skater-shot-location-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_shot_location_detail-returns}

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

### Example {#nhl_edge_skater_shot_location_detail-example}

```python
nhl_edge_skater_shot_location_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_skater_shot_location_top_10

Pull the EDGE top-10 skaters for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/{position}/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/forwards/shots/points/now](https://api-web.nhle.com/v1/edge/skater-shot-location-top-10/forwards/shots/points/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position` | `position` |  | `Y` |  | position path parameter. |
| `category` | `category` |  | `Y` |  | category path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_shot_location_top_10-returns}

**`return_parsed=True`** (default) — the output of `parse_edge_top10`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: every value tried 404s for season 20242025 (sort_by points/total/savePctg/save-pctg, position F/forwards/all, category shots/high/all/high-danger, fastRhockey's examples included), so the valid path values are unconfirmed.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_skater_shot_location_top_10-example}

```python
nhl_edge_skater_shot_location_top_10(position='forwards', category='shots', sort_by='points')
```

_Last validated n/a._

## nhl_edge_skater_shot_speed_detail

Pull EDGE shot-speed detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/8480801/now](https://api-web.nhle.com/v1/edge/skater-shot-speed-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_shot_speed_detail-returns}

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

### Example {#nhl_edge_skater_shot_speed_detail-example}

```python
nhl_edge_skater_shot_speed_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_skater_shot_speed_top_10

Pull the EDGE top-10 skaters by shot speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/defense/max/20242025/2](https://api-web.nhle.com/v1/edge/skater-shot-speed-top-10/defense/max/20242025/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_shot_speed_top_10-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `shot_attempts70_to80` | integer |  |
| `shot_attempts80_to90` | integer |  |
| `shot_attempts90_to100` | integer |  |
| `shot_attempts_over100` | integer |  |
| `hardest_shot_imperial` | double |  |
| `hardest_shot_metric` | double |  |
| `hardest_shot_overlay_away_team_abbrev` | character |  |
| `hardest_shot_overlay_away_team_score` | integer |  |
| `hardest_shot_overlay_game_date` | character |  |
| `hardest_shot_overlay_game_outcome_last_period_type` | character |  |
| `hardest_shot_overlay_game_outcome_ot_periods` | double |  |
| `hardest_shot_overlay_game_type` | integer |  |
| `hardest_shot_overlay_home_team_abbrev` | character |  |
| `hardest_shot_overlay_home_team_score` | integer |  |
| `hardest_shot_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `hardest_shot_overlay_period_descriptor_number` | integer |  |
| `hardest_shot_overlay_period_descriptor_period_type` | character |  |
| `hardest_shot_overlay_player_first_name_default` | character |  |
| `hardest_shot_overlay_player_last_name_default` | character |  |
| `hardest_shot_overlay_time_in_period` | character |  |
| `player_first_name_default` | character | Player first name (default language). |
| `player_headshot` | character | URL to the player headshot image. |
| `player_last_name_default` | character | Player last name (default language). |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_sweater_number` | integer | Player jersey number. |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_slug` | character | Player team URL-friendly slug. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_common_name_fr` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_skater_shot_speed_top_10-example}

```python
nhl_edge_skater_shot_speed_top_10(positions='defense', sort_by='max', season=20242025)
```

_Last validated n/a._

## nhl_edge_skater_skating_distance_detail

Pull EDGE skating-distance detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/8480801/now](https://api-web.nhle.com/v1/edge/skater-skating-distance-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_skating_distance_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `skating_distance_last10` | character | JSON-serialized rolling summary of total distance skated by the player across the last 10 games. |
| `skating_distance_details` | character | JSON-serialized per-game breakdown of total distance skated by the player during the tracking period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_skater_skating_distance_detail-example}

```python
nhl_edge_skater_skating_distance_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_skater_skating_speed_detail

Pull EDGE skating-speed detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/8480801/now](https://api-web.nhle.com/v1/edge/skater-skating-speed-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_skating_speed_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `top_skating_speeds` | character | JSON-serialized list of the skater's top individual speed bursts, typically the ten highest speed readings recorded during the tracking period. |
| `skating_speed_details_max_skating_speed_imperial` | double | Skater's maximum recorded skating speed in miles per hour captured by EDGE tracking during the tracking period. |
| `skating_speed_details_max_skating_speed_metric` | double | Skater's maximum recorded skating speed in kilometers per hour captured by EDGE tracking during the tracking period. |
| `skating_speed_details_max_skating_speed_percentile` | double | Percentile rank among all NHL skaters for maximum skating speed recorded during the tracking period. |
| `skating_speed_details_max_skating_speed_league_avg_imperial` | double | League-average maximum skating speed in miles per hour, used as the baseline for this skater's max-speed percentile. |
| `skating_speed_details_max_skating_speed_league_avg_metric` | double | League-average maximum skating speed in kilometers per hour, used as the baseline for this skater's max-speed percentile. |
| `skating_speed_details_max_skating_speed_overlay_player_first_name_default` | character | First name of the skater for whom the maximum skating speed overlay data is displayed. |
| `skating_speed_details_max_skating_speed_overlay_player_last_name_default` | character | Last name of the skater for whom the maximum skating speed overlay data is displayed. |
| `skating_speed_details_max_skating_speed_overlay_game_date` | character | Date on which the game occurred where the skater's maximum skating speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_away_team_abbrev` | character | Abbreviation of the away team in the game where the skater's maximum skating speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_away_team_score` | integer | Score of the away team at the conclusion of the game in which the skater's maximum skating speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_home_team_abbrev` | character | Abbreviation of the home team in the game where the skater's maximum skating speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_home_team_score` | integer | Score of the home team at the conclusion of the game in which the skater's maximum skating speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_game_outcome_last_period_type` | character | Type of the final period that determined the outcome of the game where the max speed was recorded (REG, OT, or SO). |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_max_regulation_periods` | integer | Number of regulation periods in the game format where the skater's maximum skating speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_number` | integer | Period number within the game in which the skater's maximum skating speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_period_descriptor_period_type` | character | Period type (REG, OT) identifying the phase of the game when the skater's maximum skating speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_time_in_period` | character | Elapsed time within the period (mm:ss) at which the skater's career or season maximum skating speed was recorded. |
| `skating_speed_details_max_skating_speed_overlay_game_type` | integer | NHL game type code (e.g., 2 = regular season, 3 = playoffs) for the game where the skater's maximum speed was recorded. |
| `skating_speed_details_bursts_over22_value` | integer | Average number of skating bursts per game in which the skater reached or exceeded 22 mph during the tracking period. |
| `skating_speed_details_bursts_over22_percentile` | double | Percentile rank among all NHL skaters for frequency of skating speed bursts exceeding 22 mph per game. |
| `skating_speed_details_bursts_over22_league_avg` | double | League-average number of skating speed bursts exceeding 22 mph per game, used as the baseline for this skater's percentile. |
| `skating_speed_details_bursts20_to22_value` | integer | Average number of skating bursts per game in which the skater's speed fell in the 20-22 mph range. |
| `skating_speed_details_bursts20_to22_percentile` | double | Percentile rank among all NHL skaters for frequency of skating speed bursts in the 20-22 mph speed band. |
| `skating_speed_details_bursts20_to22_league_avg` | double | League-average number of skating speed bursts in the 20-22 mph band per game, used as the baseline for percentile calculation. |
| `skating_speed_details_bursts18_to20_value` | integer | Average number of skating bursts per game in which the skater's speed fell in the 18-20 mph range. |
| `skating_speed_details_bursts18_to20_percentile` | double | Percentile rank among all NHL skaters for frequency of skating speed bursts in the 18-20 mph speed band. |
| `skating_speed_details_bursts18_to20_league_avg` | double | League-average number of skating speed bursts in the 18-20 mph band per game, used as the baseline for percentile calculation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_skater_skating_speed_detail-example}

```python
nhl_edge_skater_skating_speed_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_skater_speed_top_10

Pull the EDGE top-10 skaters by skating speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-speed-top-10/defense/max/20242025/2](https://api-web.nhle.com/v1/edge/skater-speed-top-10/defense/max/20242025/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_speed_top_10-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `bursts18_to20` | integer |  |
| `bursts20_to22` | integer |  |
| `bursts_over22` | integer |  |
| `max_speed_imperial` | double |  |
| `max_speed_metric` | double |  |
| `max_speed_overlay_away_team_abbrev` | character |  |
| `max_speed_overlay_away_team_score` | integer |  |
| `max_speed_overlay_game_date` | character |  |
| `max_speed_overlay_game_outcome_last_period_type` | character |  |
| `max_speed_overlay_game_type` | integer |  |
| `max_speed_overlay_home_team_abbrev` | character |  |
| `max_speed_overlay_home_team_score` | integer |  |
| `max_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `max_speed_overlay_period_descriptor_number` | integer |  |
| `max_speed_overlay_period_descriptor_period_type` | character |  |
| `max_speed_overlay_player_first_name_default` | character |  |
| `max_speed_overlay_player_last_name_default` | character |  |
| `max_speed_overlay_time_in_period` | character |  |
| `player_first_name_default` | character | Player first name (default language). |
| `player_headshot` | character | URL to the player headshot image. |
| `player_last_name_default` | character | Player last name (default language). |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_sweater_number` | integer | Player jersey number. |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_slug` | character | Player team URL-friendly slug. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `max_speed_overlay_game_outcome_ot_periods` | double |  |
| `player_team_common_name_fr` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_skater_speed_top_10-example}

```python
nhl_edge_skater_speed_top_10(positions='defense', sort_by='max', season=20242025)
```

_Last validated n/a._

## nhl_edge_skater_distance_top_10

Pull the EDGE top-10 skaters by skating distance.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-distance-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-distance-top-10/defense/all/total/20242025/2](https://api-web.nhle.com/v1/edge/skater-distance-top-10/defense/all/total/20242025/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_distance_top_10-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `distance_max_per_game_imperial` | double | Maximum single-game skating distance (imperial units). |
| `distance_max_per_game_metric` | double | Maximum distance skated in a single game, in kilometers. |
| `distance_max_per_game_overlay_away_team_abbrev` | character | Away team abbreviation in the max-per-game game. |
| `distance_max_per_game_overlay_away_team_score` | integer | Away team score in the max-per-game game. |
| `distance_max_per_game_overlay_game_date` | character | Game date of the max-per-game performance. |
| `distance_max_per_game_overlay_game_outcome_last_period_type` | character | Last period type of the max-per-game game outcome. |
| `distance_max_per_game_overlay_game_type` | integer | Game type of the max-per-game performance. |
| `distance_max_per_game_overlay_home_team_abbrev` | character | Home team abbreviation in the max-per-game game. |
| `distance_max_per_game_overlay_home_team_score` | integer | Home team score in the max-per-game game. |
| `distance_max_per_game_overlay_period_descriptor_max_regulation_periods` | integer | Maximum regulation periods for the max-per-game game. |
| `distance_max_per_game_overlay_period_descriptor_number` | integer | Period number for the max-per-game game descriptor. |
| `distance_max_per_game_overlay_period_descriptor_period_type` | character | Period type for the max-per-game game descriptor. |
| `distance_max_per_game_overlay_player_first_name_default` | character | Max-per-game player first name (default locale). |
| `distance_max_per_game_overlay_player_last_name_default` | character | Max-per-game player last name (default locale). |
| `distance_max_per_period_imperial` | double | Maximum single-period skating distance (imperial units). |
| `distance_max_per_period_metric` | double | Maximum distance skated in a single period, in kilometers. |
| `distance_max_per_period_overlay_away_team_abbrev` | character | Away team abbreviation in the max-per-period game. |
| `distance_max_per_period_overlay_away_team_score` | integer | Away team score in the max-per-period game. |
| `distance_max_per_period_overlay_game_date` | character | Game date of the max-per-period performance. |
| `distance_max_per_period_overlay_game_outcome_last_period_type` | character | Last period type of the max-per-period game outcome. |
| `distance_max_per_period_overlay_game_type` | integer | Game type of the max-per-period performance. |
| `distance_max_per_period_overlay_home_team_abbrev` | character | Home team abbreviation in the max-per-period game. |
| `distance_max_per_period_overlay_home_team_score` | integer | Home team score in the max-per-period game. |
| `distance_max_per_period_overlay_period_descriptor_max_regulation_periods` | integer | Maximum regulation periods for the max-per-period game. |
| `distance_max_per_period_overlay_period_descriptor_number` | integer | Period number for the max-per-period game descriptor. |
| `distance_max_per_period_overlay_period_descriptor_period_type` | character | Period type for the max-per-period game descriptor. |
| `distance_max_per_period_overlay_player_first_name_default` | character | Max-per-period player first name (default locale). |
| `distance_max_per_period_overlay_player_last_name_default` | character | Max-per-period player last name (default locale). |
| `distance_per60_imperial` | double | Skating distance per 60 minutes (imperial units). |
| `distance_per60_metric` | double | Skating distance per 60 minutes (metric units). |
| `distance_total_imperial` | double | Total skating distance (imperial units). |
| `distance_total_metric` | double | Total skating distance (metric units). |
| `player_first_name_default` | character | Player first name (default language). |
| `player_headshot` | character | URL to the player headshot image. |
| `player_last_name_default` | character | Player last name (default language). |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_sweater_number` | integer | Player jersey number. |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_slug` | character | Player team URL-friendly slug. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `distance_max_per_game_overlay_game_outcome_ot_periods` | double | Number of overtime periods in the max-per-game game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_skater_distance_top_10-example}

```python
nhl_edge_skater_distance_top_10(positions='defense', strength='all', sort_by='total', season=20242025)
```

_Last validated n/a._

## nhl_edge_skater_zone_time

Pull EDGE zone-time detail for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-zone-time/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-zone-time/8480801/now](https://api-web.nhle.com/v1/edge/skater-zone-time/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_zone_time-returns}

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

### Example {#nhl_edge_skater_zone_time-example}

```python
nhl_edge_skater_zone_time(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_skater_zone_time_top_10

Pull the EDGE top-10 skaters by zone time.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/defense/all/offensive/20242025/2](https://api-web.nhle.com/v1/edge/skater-zone-time-top-10/defense/all/offensive/20242025/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_zone_time_top_10-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `defensive_zone_time` | double | Percentage of time spent in the defensive zone. |
| `neutral_zone_time` | double | Percentage of time spent in the neutral zone. |
| `offensive_zone_time` | double | Percentage of time spent in the offensive zone. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_headshot` | character | URL to the player headshot image. |
| `player_last_name_default` | character | Player last name (default language). |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_sweater_number` | integer | Player jersey number. |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_slug` | character | Player team URL-friendly slug. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_skater_zone_time_top_10-example}

```python
nhl_edge_skater_zone_time_top_10(positions='defense', strength='all', sort_by='offensive', season=20242025)
```

_Last validated n/a._
