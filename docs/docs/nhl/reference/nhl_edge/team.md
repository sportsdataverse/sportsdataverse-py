---
title: "NHL — NHL EDGE API — Team"
sidebar_label: "Team"
sidebar_position: 5
description: "NHL — NHL EDGE API — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL EDGE API — Team

## nhl_edge_team_detail

Pull EDGE detail stats for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-detail/10/now](https://api-web.nhle.com/v1/edge/team-detail/10/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | String | Comma-separated list of season identifiers for which NHL EDGE player-tracking statistics are available for this team. |
| `sog_summary` | String | Serialized summary string of total shots-on-goal across all periods for this team, flattened from the NHL api-web team detail payload. |
| `sog_details` | String | Serialized JSON-like string containing per-period or per-game shots-on-goal detail for this team, flattened from the NHL api-web team detail payload. |
| `team_id` | Int64 | Unique team identifier. |
| `team_common_name_default` | String | Team common name (default language). |
| `team_place_name_with_preposition_default` | String | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | String | Team place name with preposition (French). |
| `team_abbrev` | String | Team abbreviation. |
| `team_team_logo_light` | String | URL to the team light logo. |
| `team_team_logo_dark` | String | URL to the team dark logo. |
| `team_slug` | String | Team URL slug. |
| `team_conference` | String | Name of the NHL conference (e.g., Eastern, Western) to which this team belongs, as returned by the NHL api-web team detail endpoint. |
| `team_division` | String | Name of the NHL division (e.g., Atlantic, Metro, Central, Pacific) to which this team belongs, as returned by the NHL api-web team detail endpoint. |
| `team_wins` | Int64 |  |
| `team_losses` | Int64 |  |
| `team_ot_losses` | Int64 | Total number of games this team has lost in overtime or a shootout (earning one standings point each) in the current season. |
| `team_games_played` | Int64 | Total number of regular-season or playoff games this team has played in the current season, from the NHL api-web team detail endpoint. |
| `team_points` | Int64 |  |
| `shot_speed_shot_attempts_over90_value` | Int64 | Total count of shot attempts recorded at a speed exceeding 90 mph by this team's players during the season, from NHL EDGE tracking data. |
| `shot_speed_shot_attempts_over90_rank` | Int64 | Team's league rank by number of shot attempts exceeding 90 mph in shot speed, with rank 1 indicating the highest count, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_imperial` | Float64 | Fastest recorded shot speed by any player on this team during the season, expressed in miles per hour, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_metric` | Float64 | Fastest recorded shot speed by any player on this team during the season, expressed in kilometers per hour, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_rank` | Int64 | Team's league rank by top shot speed for the season, where rank 1 indicates the team whose fastest shot was the quickest in the league. |
| `shot_speed_top_shot_speed_league_avg_imperial` | Float64 | League-average of the top shot speed across all teams for the same period, expressed in miles per hour, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_league_avg_metric` | Float64 | League-average of the top shot speed across all teams for the same period, expressed in kilometers per hour, from NHL EDGE tracking data. |
| `shot_speed_top_shot_speed_overlay_player_first_name_default` | String | Default-language first name of the player who recorded this team's top shot speed, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_player_last_name_default` | String | Default-language last name of the player who recorded this team's top shot speed, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_game_date` | String | Calendar date of the game in which this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_away_team_abbrev` | String | Three-letter abbreviation of the away team in the game where this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_away_team_score` | Int64 | Away team's final score in the game where this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_home_team_abbrev` | String | Three-letter abbreviation of the home team in the game where this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_home_team_score` | Int64 | Home team's final score in the game where this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_game_outcome_last_period_type` | String | Type of the final period played (e.g., REG, OT, SO) in the game where this team's top shot speed was recorded. |
| `shot_speed_top_shot_speed_overlay_game_outcome_ot_periods` | Int64 | Number of overtime periods played in the game where this team's top shot speed was recorded, or zero if decided in regulation. |
| `shot_speed_top_shot_speed_overlay_period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods in the game format where this team's top shot speed was recorded (typically 3 for NHL). |
| `shot_speed_top_shot_speed_overlay_period_descriptor_number` | Int64 | Period number within the game during which this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_period_descriptor_period_type` | String | Type label for the period (e.g., REG, OT) during which this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_time_in_period` | String | Elapsed time within the period (MM:SS format) at which this team's top shot speed was recorded, from the NHL EDGE overlay context. |
| `shot_speed_top_shot_speed_overlay_game_type` | Int64 | Numeric game-type code (e.g., 2 = regular season, 3 = playoffs) for the game in which this team's top shot speed was recorded. |
| `skating_speed_bursts_over22_value` | Int64 | Total count of skating speed bursts exceeding 22 mph recorded by this team's skaters during the season, from NHL EDGE tracking data. |
| `skating_speed_bursts_over22_rank` | Int64 | Team's league rank by total count of skating speed bursts exceeding 22 mph, where rank 1 indicates the most elite-speed bursts, from NHL EDGE tracking data. |
| `skating_speed_bursts_over20_value` | Int64 | Total count of skating speed bursts exceeding 20 mph recorded by this team's skaters during the season, from NHL EDGE tracking data. |
| `skating_speed_bursts_over20_rank` | Int64 | Team's league rank by total count of skating speed bursts exceeding 20 mph, where rank 1 indicates the most such bursts, from NHL EDGE tracking data. |
| `skating_speed_bursts_over20_league_avg_value` | Int64 | League-average number of skating speed bursts exceeding 20 mph recorded per team over the same season window, from NHL EDGE tracking data. |
| `skating_speed_speed_max_imperial` | Float64 | Fastest skating speed reached by any player on this team during the season, expressed in miles per hour, from NHL EDGE tracking data. |
| `skating_speed_speed_max_metric` | Float64 | Fastest skating speed reached by any player on this team during the season, expressed in kilometers per hour, from NHL EDGE tracking data. |
| `skating_speed_speed_max_rank` | Int64 | Team's league rank by maximum skating speed for the season, where rank 1 indicates the team whose fastest skater reached the highest speed in the league. |
| `skating_speed_speed_max_league_avg_imperial` | Float64 | League-average of the maximum skating speed across all teams for the same period, expressed in miles per hour, from NHL EDGE tracking data. |
| `skating_speed_speed_max_league_avg_metric` | Float64 | League-average of the maximum skating speed across all teams for the same period, expressed in kilometers per hour, from NHL EDGE tracking data. |
| `skating_speed_speed_max_overlay_player_first_name_default` | String | Default-language first name of the player who recorded this team's top skating speed, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_player_last_name_default` | String | Default-language last name of the player who recorded this team's top skating speed, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_game_date` | String | Calendar date of the game in which this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_away_team_abbrev` | String | Three-letter abbreviation of the away team in the game where this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_away_team_score` | Int64 | Away team's final score in the game where this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_home_team_abbrev` | String | Three-letter abbreviation of the home team in the game where this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_home_team_score` | Int64 | Home team's final score in the game where this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_game_outcome_last_period_type` | String | Type of the final period played (e.g., REG, OT, SO) in the game where this team's top skating speed was recorded. |
| `skating_speed_speed_max_overlay_period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods in the game format where this team's top skating speed was recorded (typically 3 for NHL). |
| `skating_speed_speed_max_overlay_period_descriptor_number` | Int64 | Period number within the game during which this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_period_descriptor_period_type` | String | Type label for the period (e.g., REG, OT) during which this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_time_in_period` | String | Elapsed time within the period (MM:SS format) at which this team's top skating speed was recorded, from the NHL EDGE overlay context. |
| `skating_speed_speed_max_overlay_game_type` | Int64 | Numeric game-type code (e.g., 2 = regular season, 3 = playoffs) for the game in which this team's top skating speed was recorded. |
| `distance_skated_total_imperial` | Float64 | Total cumulative skating distance logged by all skaters on this team across the season, expressed in miles (imperial), from NHL EDGE tracking data. |
| `distance_skated_total_metric` | Float64 | Total cumulative skating distance logged by all skaters on this team across the season, expressed in kilometers (metric), from NHL EDGE tracking data. |
| `distance_skated_total_rank` | Int64 | Team's league rank by total cumulative skating distance for the season, where rank 1 indicates the team with the most distance skated. |
| `distance_skated_total_league_avg_imperial` | Float64 | League-average cumulative skating distance for all teams over the same period as this team's totals, expressed in miles (imperial), from NHL EDGE tracking data. |
| `distance_skated_total_league_avg_metric` | Float64 | League-average cumulative skating distance for all teams over the same period as this team's totals, expressed in kilometers (metric), from NHL EDGE tracking data. |
| `zone_time_details_offensive_zone_pctg` | Float64 | Percentage of all-situation ice time this team spends in the offensive zone, as measured by NHL EDGE player-tracking data. |
| `zone_time_details_offensive_zone_rank` | Int64 | Team's league rank by overall offensive-zone time percentage (all situations), where rank 1 indicates the team with the most offensive-zone presence. |
| `zone_time_details_offensive_zone_league_avg` | Float64 | League-average percentage of all-situation ice time that teams spend in the offensive zone, from NHL EDGE zone-time tracking data. |
| `zone_time_details_offensive_zone_ev_pctg` | Float64 | Percentage of even-strength ice time this team spends in the offensive zone, as measured by NHL EDGE player-tracking data. |
| `zone_time_details_offensive_zone_ev_rank` | Int64 | Team's league rank by even-strength offensive-zone time percentage, where rank 1 indicates the team spending the most time in the offensive zone at even strength. |
| `zone_time_details_offensive_zone_ev_league_avg` | Float64 | League-average percentage of even-strength ice time that teams spend in the offensive zone, from NHL EDGE zone-time tracking data. |
| `zone_time_details_neutral_zone_pctg` | Float64 | Percentage of five-on-five ice time this team spends in the neutral zone, as measured by NHL EDGE player-tracking data. |
| `zone_time_details_neutral_zone_rank` | Int64 | Team's league rank by neutral-zone time percentage, where rank 1 indicates the team that spends the most time in the neutral zone during five-on-five play. |
| `zone_time_details_neutral_zone_league_avg` | Float64 | League-average percentage of time that teams spend in the neutral zone during five-on-five play, from NHL EDGE zone-time tracking data. |
| `zone_time_details_defensive_zone_pctg` | Float64 | Percentage of five-on-five ice time this team spends in its own defensive zone, as measured by NHL EDGE player-tracking data. |
| `zone_time_details_defensive_zone_rank` | Int64 | Team's league rank by defensive-zone time percentage, where rank 1 indicates the team that spends the most time in its own zone during five-on-five play. |
| `zone_time_details_defensive_zone_league_avg` | Float64 | League-average percentage of time that teams spend in the defensive zone during five-on-five play, from NHL EDGE zone-time tracking data. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_edge_team_detail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_detail-example}

```python
nhl_edge_team_detail(team_id=10)
```

## nhl_edge_team_landing

Pull the EDGE team landing page (summary across all teams).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-landing/now](https://api-web.nhle.com/v1/edge/team-landing/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_landing-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | String | Serialized list of NHL seasons for which EDGE player-tracking data is available for this team. |
| `leaders_shot_attempts_over90_team_id` | Int64 | NHL identifier for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_common_name_default` | String | Common team name for the leader in the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_place_name_with_preposition_default` | String | Default-language place name with preposition for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_place_name_with_preposition_fr` | String | French-language place name with preposition for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_abbrev` | String | Three-letter abbreviation for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_team_logo_light` | String | URL of the light-background logo for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_team_logo_dark` | String | URL of the dark-background logo for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_slug` | String | URL-friendly slug identifier for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_wins` | Int64 | Regular-season wins for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_losses` | Int64 | Regular-season losses for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_ot_losses` | Int64 | Overtime losses for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_attempts` | Int64 | Number of shot attempts above the 90-mph threshold recorded by the leading team in the EDGE shot-speed leaderboard period. |
| `leaders_bursts_over22_team_id` | Int64 | NHL identifier for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_common_name_default` | String | Common team name (e.g., 'Maple Leafs') for the leader in the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_place_name_with_preposition_default` | String | Default-language place name with preposition (e.g., 'in Toronto') for the team leading the speed-bursts-over-22-mph EDGE category. |
| `leaders_bursts_over22_team_place_name_with_preposition_fr` | String | French-language place name with preposition for the team leading the speed-bursts-over-22-mph EDGE category. |
| `leaders_bursts_over22_team_abbrev` | String | Three-letter abbreviation for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_team_logo_light` | String | URL of the light-background logo for the team leading the speed-bursts-over-22-mph EDGE category. |
| `leaders_bursts_over22_team_team_logo_dark` | String | URL of the dark-background logo for the team leading the speed-bursts-over-22-mph EDGE category. |
| `leaders_bursts_over22_team_slug` | String | URL-friendly slug identifier for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_wins` | Int64 | Regular-season wins for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_losses` | Int64 | Regular-season losses for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_ot_losses` | Int64 | Overtime losses for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_bursts` | Int64 | Number of speed bursts above 22 mph recorded by skaters on the team in the EDGE tracking leaderboard period. |
| `leaders_distance_per60_team_id` | Int64 | NHL identifier for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_common_name_default` | String | Common team name for the leader in the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_place_name_with_preposition_default` | String | Default-language place name with preposition for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_place_name_with_preposition_fr` | String | French-language place name with preposition for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_abbrev` | String | Three-letter abbreviation for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_team_logo_light` | String | URL of the light-background logo for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_team_logo_dark` | String | URL of the dark-background logo for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_slug` | String | URL-friendly slug identifier for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_wins` | Int64 | Regular-season wins for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_losses` | Int64 | Regular-season losses for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_ot_losses` | Int64 | Overtime losses for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_distance_skated_imperial` | Float64 | Average distance skated per 60 minutes of ice time by the leading team's skaters, measured in miles. |
| `leaders_distance_per60_distance_skated_metric` | Float64 | Average distance skated per 60 minutes of ice time by the leading team's skaters, measured in kilometers. |
| `leaders_high_danger_sog_team_id` | Int64 | NHL identifier for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_common_name_default` | String | Common team name for the leader in the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_place_name_with_preposition_default` | String | Default-language place name with preposition for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_place_name_with_preposition_fr` | String | French-language place name with preposition for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_abbrev` | String | Three-letter abbreviation for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_team_logo_light` | String | URL of the light-background logo for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_team_logo_dark` | String | URL of the dark-background logo for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_slug` | String | URL-friendly slug identifier for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_wins` | Int64 | Regular-season wins for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_losses` | Int64 | Regular-season losses for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_ot_losses` | Int64 | Overtime losses for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_sog` | Int64 | Number of high-danger shots on goal recorded by the leading team in the EDGE tracking leaderboard period. |
| `leaders_high_danger_sog_shot_location_details` | String | Serialized details describing the high-danger shot locations used to define this EDGE tracking leaderboard category. |
| `leaders_offensive_zone_time_team_id` | Int64 | NHL identifier for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_common_name_default` | String | Common team name for the leader in the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_place_name_with_preposition_default` | String | Default-language place name with preposition for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_place_name_with_preposition_fr` | String | French-language place name with preposition for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_abbrev` | String | Three-letter abbreviation for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_team_logo_light` | String | URL of the light-background logo for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_team_logo_dark` | String | URL of the dark-background logo for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_slug` | String | URL-friendly slug identifier for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_wins` | Int64 | Regular-season wins for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_losses` | Int64 | Regular-season losses for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_ot_losses` | Int64 | Overtime losses for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_zone_time` | Float64 | Total time spent in the offensive zone by the leading team in the EDGE tracking leaderboard period. |
| `leaders_neutral_zone_time_team_id` | Int64 | NHL identifier for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_common_name_default` | String | Common team name for the leader in the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_place_name_with_preposition_default` | String | Default-language place name with preposition for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_place_name_with_preposition_fr` | String | French-language place name with preposition for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_abbrev` | String | Three-letter abbreviation for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_team_logo_light` | String | URL of the light-background logo for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_team_logo_dark` | String | URL of the dark-background logo for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_slug` | String | URL-friendly slug identifier for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_wins` | Int64 | Regular-season wins for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_losses` | Int64 | Regular-season losses for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_ot_losses` | Int64 | Overtime losses for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_zone_time` | Float64 | Total time spent in the neutral zone by the leading team in the EDGE tracking leaderboard period. |
| `leaders_defensive_zone_time_team_id` | Int64 | NHL identifier for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_common_name_default` | String | Common team name for the leader in the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_place_name_with_preposition_default` | String | Default-language place name with preposition for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_place_name_with_preposition_fr` | String | French-language place name with preposition for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_abbrev` | String | Three-letter abbreviation for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_team_logo_light` | String | URL of the light-background logo for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_team_logo_dark` | String | URL of the dark-background logo for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_slug` | String | URL-friendly slug identifier for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_wins` | Int64 | Regular-season wins for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_losses` | Int64 | Regular-season losses for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_ot_losses` | Int64 | Overtime losses for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_zone_time` | Float64 | Total time spent in the defensive zone by the leading team in the EDGE tracking leaderboard period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_edge_team_landing-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_landing-example}

```python
nhl_edge_team_landing()
```

## nhl_edge_team_shot_location_detail

Pull EDGE shot-location detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-location-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-location-detail/10/now](https://api-web.nhle.com/v1/edge/team-shot-location-detail/10/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_shot_location_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `area` | String | Net/ice zone the shots were taken from. |
| `sog` | Int64 | Shots on goal from the area. |
| `sog_rank` | Int64 | League rank for shots on goal from the area. |
| `goals` | Int64 | Goals scored. |
| `goals_rank` | Int64 | League rank for goals scored from the area. |
| `shooting_pctg` | Float64 | Shooting percentage from the area. |
| `shooting_pctg_rank` | Int64 | League rank for shooting percentage from the area. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_edge_team_shot_location_detail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_shot_location_detail-example}

```python
nhl_edge_team_shot_location_detail(team_id=10)
```

## nhl_edge_team_shot_location_top_10

Pull the EDGE top-10 teams for a shot-location category.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-location-top-10/{position}/{category}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-location-top-10/forwards/shots/points/now](https://api-web.nhle.com/v1/edge/team-shot-location-top-10/forwards/shots/points/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position` | `position` |  | `Y` |  | position path parameter. |
| `category` | `category` |  | `Y` |  | category path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_shot_location_top_10-returns}

**`return_parsed=True`** (default) — the output of `parse_edge_top10`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: every value tried 404s for season 20242025 (sort_by points/total/savePctg/save-pctg, position F/forwards/all, category shots/high/all/high-danger, fastRhockey's examples included), so the valid path values are unconfirmed.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_edge_team_shot_location_top_10-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_shot_location_top_10-example}

```python
nhl_edge_team_shot_location_top_10(position='forwards', category='shots', sort_by='points')
```

## nhl_edge_team_shot_speed_detail

Pull EDGE shot-speed detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-shot-speed-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-shot-speed-detail/10/now](https://api-web.nhle.com/v1/edge/team-shot-speed-detail/10/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_shot_speed_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `hardest_shots` | String | Serialized list of the hardest individual shot records associated with the team's players. |
| `shot_speed_details` | String | Serialized shot-speed breakdown object containing aggregate and top-speed metrics for the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_edge_team_shot_speed_detail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_shot_speed_detail-example}

```python
nhl_edge_team_shot_speed_detail(team_id=10)
```

## nhl_edge_team_skating_distance_detail

Pull EDGE skating-distance detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-distance-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-distance-detail/10/now](https://api-web.nhle.com/v1/edge/team-skating-distance-detail/10/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_skating_distance_detail-returns}

Pull EDGE skating-distance detail for a single team.

### Raises {#nhl_edge_team_skating_distance_detail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_skating_distance_detail-example}

```python
nhl_edge_team_skating_distance_detail(team_id=10)
```

## nhl_edge_team_skating_distance_top_10

Pull the EDGE top-10 teams by skating distance.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-distance-top-10/{positions}/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-distance-top-10/defense/all/total/20242025/2](https://api-web.nhle.com/v1/edge/team-skating-distance-top-10/defense/all/total/20242025/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_skating_distance_top_10-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `distance_max_per_game_imperial` | Float64 | Maximum single-game skating distance (imperial units). |
| `distance_max_per_game_metric` | Float64 | Maximum distance skated in a single game, in kilometers. |
| `distance_max_per_game_overlay_away_team_abbrev` | String | Away team abbreviation in the max-per-game game. |
| `distance_max_per_game_overlay_away_team_score` | Int64 | Away team score in the max-per-game game. |
| `distance_max_per_game_overlay_game_date` | String | Game date of the max-per-game performance. |
| `distance_max_per_game_overlay_game_outcome_last_period_type` | String | Last period type of the max-per-game game outcome. |
| `distance_max_per_game_overlay_game_type` | Int64 | Game type of the max-per-game performance. |
| `distance_max_per_game_overlay_home_team_abbrev` | String | Home team abbreviation in the max-per-game game. |
| `distance_max_per_game_overlay_home_team_score` | Int64 | Home team score in the max-per-game game. |
| `distance_max_per_game_overlay_period_descriptor_max_regulation_periods` | Int64 | Maximum regulation periods for the max-per-game game. |
| `distance_max_per_game_overlay_period_descriptor_number` | Int64 | Period number for the max-per-game game descriptor. |
| `distance_max_per_game_overlay_period_descriptor_period_type` | String | Period type for the max-per-game game descriptor. |
| `distance_max_per_period_imperial` | Float64 | Maximum single-period skating distance (imperial units). |
| `distance_max_per_period_metric` | Float64 | Maximum distance skated in a single period, in kilometers. |
| `distance_max_per_period_overlay_away_team_abbrev` | String | Away team abbreviation in the max-per-period game. |
| `distance_max_per_period_overlay_away_team_score` | Int64 | Away team score in the max-per-period game. |
| `distance_max_per_period_overlay_game_date` | String | Game date of the max-per-period performance. |
| `distance_max_per_period_overlay_game_outcome_last_period_type` | String | Last period type of the max-per-period game outcome. |
| `distance_max_per_period_overlay_game_type` | Int64 | Game type of the max-per-period performance. |
| `distance_max_per_period_overlay_home_team_abbrev` | String | Home team abbreviation in the max-per-period game. |
| `distance_max_per_period_overlay_home_team_score` | Int64 | Home team score in the max-per-period game. |
| `distance_max_per_period_overlay_period_descriptor_max_regulation_periods` | Int64 | Maximum regulation periods for the max-per-period game. |
| `distance_max_per_period_overlay_period_descriptor_number` | Int64 | Period number for the max-per-period game descriptor. |
| `distance_max_per_period_overlay_period_descriptor_period_type` | String | Period type for the max-per-period game descriptor. |
| `distance_per60_imperial` | Float64 | Skating distance per 60 minutes (imperial units). |
| `distance_per60_metric` | Float64 | Skating distance per 60 minutes (metric units). |
| `distance_total_imperial` | Float64 | Total skating distance (imperial units). |
| `distance_total_metric` | Float64 | Total skating distance (metric units). |
| `team_abbrev` | String | Team abbreviation. |
| `team_common_name_default` | String | Team common name (default language). |
| `team_place_name_with_preposition_default` | String | Team place name with preposition (default). |
| `team_slug` | String | Team URL slug. |
| `team_team_logo_dark` | String | URL to the team dark logo. |
| `team_team_logo_light` | String | URL to the team light logo. |
| `distance_max_per_game_overlay_game_outcome_ot_periods` | Float64 | Number of overtime periods in the max-per-game game. |
| `distance_max_per_period_overlay_game_outcome_ot_periods` | Float64 | Number of overtime periods in the max-per-period game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_edge_team_skating_distance_top_10-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_skating_distance_top_10-example}

```python
nhl_edge_team_skating_distance_top_10(positions='defense', strength='all', sort_by='total', season=20242025)
```

## nhl_edge_team_skating_speed_detail

Pull EDGE skating-speed detail for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-speed-detail/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-speed-detail/10/now](https://api-web.nhle.com/v1/edge/team-skating-speed-detail/10/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_skating_speed_detail-returns}

Pull EDGE skating-speed detail for a single team.

### Raises {#nhl_edge_team_skating_speed_detail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_skating_speed_detail-example}

```python
nhl_edge_team_skating_speed_detail(team_id=10)
```

## nhl_edge_team_skating_speed_top_10

Pull the EDGE top-10 teams by skating speed.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-skating-speed-top-10/{positions}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-skating-speed-top-10/defense/max/20242025/2](https://api-web.nhle.com/v1/edge/team-skating-speed-top-10/defense/max/20242025/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `positions` | `positions` |  | `Y` |  | positions path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_skating_speed_top_10-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `bursts18_to20` | Int64 |  |
| `bursts20_to22` | Int64 |  |
| `bursts_over22` | Int64 |  |
| `max_skating_speed_imperial` | Float64 |  |
| `max_skating_speed_metric` | Float64 |  |
| `max_skating_speed_overlay_away_team_abbrev` | String |  |
| `max_skating_speed_overlay_away_team_score` | Int64 |  |
| `max_skating_speed_overlay_game_date` | String |  |
| `max_skating_speed_overlay_game_outcome_last_period_type` | String |  |
| `max_skating_speed_overlay_game_type` | Int64 |  |
| `max_skating_speed_overlay_home_team_abbrev` | String |  |
| `max_skating_speed_overlay_home_team_score` | Int64 |  |
| `max_skating_speed_overlay_period_descriptor_max_regulation_periods` | Int64 |  |
| `max_skating_speed_overlay_period_descriptor_number` | Int64 |  |
| `max_skating_speed_overlay_period_descriptor_period_type` | String |  |
| `max_skating_speed_overlay_player_first_name_default` | String |  |
| `max_skating_speed_overlay_player_last_name_default` | String |  |
| `max_skating_speed_overlay_time_in_period` | String |  |
| `team_abbrev` | String | Team abbreviation. |
| `team_common_name_default` | String | Team common name (default language). |
| `team_id` | Int64 | Unique team identifier. |
| `team_place_name_with_preposition_default` | String | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | String | Team place name with preposition (French). |
| `team_slug` | String | Team URL slug. |
| `team_team_logo_dark` | String | URL to the team dark logo. |
| `team_team_logo_light` | String | URL to the team light logo. |
| `max_skating_speed_overlay_game_outcome_ot_periods` | Float64 |  |
| `team_common_name_fr` | String | Team common name (French localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_edge_team_skating_speed_top_10-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_skating_speed_top_10-example}

```python
nhl_edge_team_skating_speed_top_10(positions='defense', sort_by='max', season=20242025)
```

## nhl_edge_team_zone_time_details

Pull EDGE zone-time details for a single team.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-zone-time-details/{team_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-zone-time-details/10/now](https://api-web.nhle.com/v1/edge/team-zone-time-details/10/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_zone_time_details-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `strength_code` | String | Strength state code (e.g., all, even, pp, pk). |
| `offensive_zone_pctg` | Float64 | Percentage of time spent in the offensive zone. |
| `offensive_zone_rank` | Int64 | League rank for offensive zone time. |
| `offensive_zone_league_avg` | Float64 | League average offensive-zone time percentage. |
| `neutral_zone_pctg` | Float64 | Percentage of time spent in the neutral zone. |
| `neutral_zone_rank` | Int64 | League rank for neutral zone time. |
| `neutral_zone_league_avg` | Float64 | League average neutral-zone time percentage. |
| `defensive_zone_pctg` | Float64 | Percentage of time spent in the defensive zone. |
| `defensive_zone_rank` | Int64 | League rank for defensive zone time. |
| `defensive_zone_league_avg` | Float64 | League average defensive-zone time percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_edge_team_zone_time_details-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_zone_time_details-example}

```python
nhl_edge_team_zone_time_details(team_id=10)
```

## nhl_edge_team_zone_time_top_10

Pull the EDGE top-10 teams by zone time.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/team-zone-time-top-10/{strength}/{sort_by}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/team-zone-time-top-10/all/offensive/20242025/2](https://api-web.nhle.com/v1/edge/team-zone-time-top-10/all/offensive/20242025/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `strength` | `strength` |  | `Y` |  | strength path parameter. |
| `sort_by` | `sort_by` |  | `Y` |  | sort_by path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_team_zone_time_top_10-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `defensive_zone_time` | Float64 | Percentage of time spent in the defensive zone. |
| `neutral_zone_time` | Float64 | Percentage of time spent in the neutral zone. |
| `offensive_zone_time` | Float64 | Percentage of time spent in the offensive zone. |
| `team_abbrev` | String | Team abbreviation. |
| `team_common_name_default` | String | Team common name (default language). |
| `team_place_name_with_preposition_default` | String | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | String | Team place name with preposition (French). |
| `team_slug` | String | Team URL slug. |
| `team_team_logo_dark` | String | URL to the team dark logo. |
| `team_team_logo_light` | String | URL to the team light logo. |
| `team_common_name_fr` | String | Team common name (French localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_edge_team_zone_time_top_10-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_edge_team_zone_time_top_10-example}

```python
nhl_edge_team_zone_time_top_10(strength='all', sort_by='offensive', season=20242025)
```
