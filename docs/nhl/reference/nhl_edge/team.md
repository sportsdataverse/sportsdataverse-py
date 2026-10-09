# NHL — NHL EDGE API — Team

> NHL — NHL EDGE API — Team — function reference in sdv-py, the SportsDataverse Python package.

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
| `team_wins` | integer |  |
| `team_losses` | integer |  |
| `team_ot_losses` | integer | Total number of games this team has lost in overtime or a shootout (earning one standings point each) in the current season. |
| `team_games_played` | integer | Total number of regular-season or playoff games this team has played in the current season, from the NHL api-web team detail endpoint. |
| `team_points` | integer |  |
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

### Example {#nhl_edge_team_detail-example}

```python
nhl_edge_team_detail(team_id=10)
```

_Last validated n/a._

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
| `seasons_with_edge_stats` | character | Serialized list of NHL seasons for which EDGE player-tracking data is available for this team. |
| `leaders_shot_attempts_over90_team_id` | integer | NHL identifier for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_common_name_default` | character | Common team name for the leader in the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_place_name_with_preposition_default` | character | Default-language place name with preposition for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_place_name_with_preposition_fr` | character | French-language place name with preposition for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_abbrev` | character | Three-letter abbreviation for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_team_logo_light` | character | URL of the light-background logo for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_team_logo_dark` | character | URL of the dark-background logo for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_slug` | character | URL-friendly slug identifier for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_wins` | integer | Regular-season wins for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_losses` | integer | Regular-season losses for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_team_ot_losses` | integer | Overtime losses for the team leading the EDGE shot-attempts-over-90-mph category. |
| `leaders_shot_attempts_over90_attempts` | integer | Number of shot attempts above the 90-mph threshold recorded by the leading team in the EDGE shot-speed leaderboard period. |
| `leaders_bursts_over22_team_id` | integer | NHL identifier for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_common_name_default` | character | Common team name (e.g., 'Maple Leafs') for the leader in the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_place_name_with_preposition_default` | character | Default-language place name with preposition (e.g., 'in Toronto') for the team leading the speed-bursts-over-22-mph EDGE category. |
| `leaders_bursts_over22_team_place_name_with_preposition_fr` | character | French-language place name with preposition for the team leading the speed-bursts-over-22-mph EDGE category. |
| `leaders_bursts_over22_team_abbrev` | character | Three-letter abbreviation for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_team_logo_light` | character | URL of the light-background logo for the team leading the speed-bursts-over-22-mph EDGE category. |
| `leaders_bursts_over22_team_team_logo_dark` | character | URL of the dark-background logo for the team leading the speed-bursts-over-22-mph EDGE category. |
| `leaders_bursts_over22_team_slug` | character | URL-friendly slug identifier for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_wins` | integer | Regular-season wins for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_losses` | integer | Regular-season losses for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_team_ot_losses` | integer | Overtime losses for the team leading the speed-bursts-over-22-mph EDGE tracking category. |
| `leaders_bursts_over22_bursts` | integer | Number of speed bursts above 22 mph recorded by skaters on the team in the EDGE tracking leaderboard period. |
| `leaders_distance_per60_team_id` | integer | NHL identifier for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_common_name_default` | character | Common team name for the leader in the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_place_name_with_preposition_default` | character | Default-language place name with preposition for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_place_name_with_preposition_fr` | character | French-language place name with preposition for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_abbrev` | character | Three-letter abbreviation for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_team_logo_light` | character | URL of the light-background logo for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_team_logo_dark` | character | URL of the dark-background logo for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_slug` | character | URL-friendly slug identifier for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_wins` | integer | Regular-season wins for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_losses` | integer | Regular-season losses for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_team_ot_losses` | integer | Overtime losses for the team leading the EDGE distance-skated-per-60 category. |
| `leaders_distance_per60_distance_skated_imperial` | double | Average distance skated per 60 minutes of ice time by the leading team's skaters, measured in miles. |
| `leaders_distance_per60_distance_skated_metric` | double | Average distance skated per 60 minutes of ice time by the leading team's skaters, measured in kilometers. |
| `leaders_high_danger_sog_team_id` | integer | NHL identifier for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_common_name_default` | character | Common team name for the leader in the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_place_name_with_preposition_default` | character | Default-language place name with preposition for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_place_name_with_preposition_fr` | character | French-language place name with preposition for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_abbrev` | character | Three-letter abbreviation for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_team_logo_light` | character | URL of the light-background logo for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_team_logo_dark` | character | URL of the dark-background logo for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_slug` | character | URL-friendly slug identifier for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_wins` | integer | Regular-season wins for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_losses` | integer | Regular-season losses for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_team_ot_losses` | integer | Overtime losses for the team leading the EDGE high-danger shots-on-goal category. |
| `leaders_high_danger_sog_sog` | integer | Number of high-danger shots on goal recorded by the leading team in the EDGE tracking leaderboard period. |
| `leaders_high_danger_sog_shot_location_details` | character | Serialized details describing the high-danger shot locations used to define this EDGE tracking leaderboard category. |
| `leaders_offensive_zone_time_team_id` | integer | NHL identifier for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_common_name_default` | character | Common team name for the leader in the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_place_name_with_preposition_default` | character | Default-language place name with preposition for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_place_name_with_preposition_fr` | character | French-language place name with preposition for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_abbrev` | character | Three-letter abbreviation for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_team_logo_light` | character | URL of the light-background logo for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_team_logo_dark` | character | URL of the dark-background logo for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_slug` | character | URL-friendly slug identifier for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_wins` | integer | Regular-season wins for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_losses` | integer | Regular-season losses for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_team_ot_losses` | integer | Overtime losses for the team leading the EDGE offensive-zone time category. |
| `leaders_offensive_zone_time_zone_time` | double | Total time spent in the offensive zone by the leading team in the EDGE tracking leaderboard period. |
| `leaders_neutral_zone_time_team_id` | integer | NHL identifier for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_common_name_default` | character | Common team name for the leader in the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_place_name_with_preposition_default` | character | Default-language place name with preposition for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_place_name_with_preposition_fr` | character | French-language place name with preposition for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_abbrev` | character | Three-letter abbreviation for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_team_logo_light` | character | URL of the light-background logo for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_team_logo_dark` | character | URL of the dark-background logo for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_slug` | character | URL-friendly slug identifier for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_wins` | integer | Regular-season wins for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_losses` | integer | Regular-season losses for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_team_ot_losses` | integer | Overtime losses for the team leading the EDGE neutral-zone time category. |
| `leaders_neutral_zone_time_zone_time` | double | Total time spent in the neutral zone by the leading team in the EDGE tracking leaderboard period. |
| `leaders_defensive_zone_time_team_id` | integer | NHL identifier for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_common_name_default` | character | Common team name for the leader in the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_place_name_with_preposition_default` | character | Default-language place name with preposition for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_place_name_with_preposition_fr` | character | French-language place name with preposition for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_abbrev` | character | Three-letter abbreviation for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_team_logo_light` | character | URL of the light-background logo for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_team_logo_dark` | character | URL of the dark-background logo for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_slug` | character | URL-friendly slug identifier for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_wins` | integer | Regular-season wins for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_losses` | integer | Regular-season losses for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_team_ot_losses` | integer | Overtime losses for the team leading the EDGE defensive-zone time category. |
| `leaders_defensive_zone_time_zone_time` | double | Total time spent in the defensive zone by the leading team in the EDGE tracking leaderboard period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_team_landing-example}

```python
nhl_edge_team_landing()
```

_Last validated n/a._

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
| `area` | character | Net/ice zone the shots were taken from. |
| `sog` | integer | Shots on goal from the area. |
| `sog_rank` | integer | League rank for shots on goal from the area. |
| `goals` | integer | Goals scored. |
| `goals_rank` | integer | League rank for goals scored from the area. |
| `shooting_pctg` | double | Shooting percentage from the area. |
| `shooting_pctg_rank` | integer | League rank for shooting percentage from the area. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_team_shot_location_detail-example}

```python
nhl_edge_team_shot_location_detail(team_id=10)
```

_Last validated n/a._

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

### Example {#nhl_edge_team_shot_location_top_10-example}

```python
nhl_edge_team_shot_location_top_10(position='forwards', category='shots', sort_by='points')
```

_Last validated n/a._

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
| `hardest_shots` | character | Serialized list of the hardest individual shot records associated with the team's players. |
| `shot_speed_details` | character | Serialized shot-speed breakdown object containing aggregate and top-speed metrics for the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_team_shot_speed_detail-example}

```python
nhl_edge_team_shot_speed_detail(team_id=10)
```

_Last validated n/a._

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

### Example {#nhl_edge_team_skating_distance_detail-example}

```python
nhl_edge_team_skating_distance_detail(team_id=10)
```

_Last validated n/a._

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
| `distance_per60_imperial` | double | Skating distance per 60 minutes (imperial units). |
| `distance_per60_metric` | double | Skating distance per 60 minutes (metric units). |
| `distance_total_imperial` | double | Total skating distance (imperial units). |
| `distance_total_metric` | double | Total skating distance (metric units). |
| `team_abbrev` | character | Team abbreviation. |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_slug` | character | Team URL slug. |
| `team_team_logo_dark` | character | URL to the team dark logo. |
| `team_team_logo_light` | character | URL to the team light logo. |
| `distance_max_per_game_overlay_game_outcome_ot_periods` | double | Number of overtime periods in the max-per-game game. |
| `distance_max_per_period_overlay_game_outcome_ot_periods` | double | Number of overtime periods in the max-per-period game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_team_skating_distance_top_10-example}

```python
nhl_edge_team_skating_distance_top_10(positions='defense', strength='all', sort_by='total', season=20242025)
```

_Last validated n/a._

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

### Example {#nhl_edge_team_skating_speed_detail-example}

```python
nhl_edge_team_skating_speed_detail(team_id=10)
```

_Last validated n/a._

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
| `bursts18_to20` | integer |  |
| `bursts20_to22` | integer |  |
| `bursts_over22` | integer |  |
| `max_skating_speed_imperial` | double |  |
| `max_skating_speed_metric` | double |  |
| `max_skating_speed_overlay_away_team_abbrev` | character |  |
| `max_skating_speed_overlay_away_team_score` | integer |  |
| `max_skating_speed_overlay_game_date` | character |  |
| `max_skating_speed_overlay_game_outcome_last_period_type` | character |  |
| `max_skating_speed_overlay_game_type` | integer |  |
| `max_skating_speed_overlay_home_team_abbrev` | character |  |
| `max_skating_speed_overlay_home_team_score` | integer |  |
| `max_skating_speed_overlay_period_descriptor_max_regulation_periods` | integer |  |
| `max_skating_speed_overlay_period_descriptor_number` | integer |  |
| `max_skating_speed_overlay_period_descriptor_period_type` | character |  |
| `max_skating_speed_overlay_player_first_name_default` | character |  |
| `max_skating_speed_overlay_player_last_name_default` | character |  |
| `max_skating_speed_overlay_time_in_period` | character |  |
| `team_abbrev` | character | Team abbreviation. |
| `team_common_name_default` | character | Team common name (default language). |
| `team_id` | integer | Unique team identifier. |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `team_slug` | character | Team URL slug. |
| `team_team_logo_dark` | character | URL to the team dark logo. |
| `team_team_logo_light` | character | URL to the team light logo. |
| `max_skating_speed_overlay_game_outcome_ot_periods` | double |  |
| `team_common_name_fr` | character | Team common name (French localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_team_skating_speed_top_10-example}

```python
nhl_edge_team_skating_speed_top_10(positions='defense', sort_by='max', season=20242025)
```

_Last validated n/a._

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

### Example {#nhl_edge_team_zone_time_details-example}

```python
nhl_edge_team_zone_time_details(team_id=10)
```

_Last validated n/a._

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
| `defensive_zone_time` | double | Percentage of time spent in the defensive zone. |
| `neutral_zone_time` | double | Percentage of time spent in the neutral zone. |
| `offensive_zone_time` | double | Percentage of time spent in the offensive zone. |
| `team_abbrev` | character | Team abbreviation. |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `team_slug` | character | Team URL slug. |
| `team_team_logo_dark` | character | URL to the team dark logo. |
| `team_team_logo_light` | character | URL to the team light logo. |
| `team_common_name_fr` | character | Team common name (French localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_team_zone_time_top_10-example}

```python
nhl_edge_team_zone_time_top_10(strength='all', sort_by='offensive', season=20242025)
```

_Last validated n/a._
