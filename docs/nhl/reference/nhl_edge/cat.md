# NHL — NHL EDGE API — Cat

> NHL — NHL EDGE API — Cat — function reference in sdv-py, the SportsDataverse Python package.

## nhl_edge_cat_skater_detail

Pull categorized (cat) EDGE detail stats for a single skater.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/cat/edge/skater-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/cat/edge/skater-detail/8480801/now](https://api-web.nhle.com/v1/cat/edge/skater-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_cat_skater_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | JSON-serialized list of NHL season identifiers for which EDGE puck-and-player tracking data is available for this skater. |
| `sog_summary` | character | JSON-serialized summary of shot-on-goal tracking data aggregated over the tracking period for the skater. |
| `sog_details` | character | JSON-serialized per-game shot-on-goal tracking details for the skater across the tracking period. |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character |  |
| `player_shoots_catches` | character | Side on which the skater shoots — L (left) or R (right). |
| `player_sweater_number` | integer | Player jersey number. |
| `player_position` | character | Primary player position. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_goals` | integer | Number of goals scored by the skater during the relevant tracking period. |
| `player_assists` | integer | Number of assists credited to the skater during the relevant tracking period. |
| `player_points` | integer |  |
| `player_games_played` | integer | Number of games in which the skater appeared during the relevant tracking period. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `top_shot_speed_imperial` | double | Skater's highest recorded shot speed in miles per hour during the tracking period. |
| `top_shot_speed_metric` | double | Skater's highest recorded shot speed in kilometers per hour during the tracking period. |
| `top_shot_speed_percentile` | double | Percentile rank among all NHL skaters for highest recorded shot speed during the tracking period. |
| `top_shot_speed_league_avg_imperial` | double | League-average top shot speed in miles per hour, used as the baseline for this skater's shot-speed percentile. |
| `top_shot_speed_league_avg_metric` | double | League-average top shot speed in kilometers per hour, used as the baseline for this skater's shot-speed percentile. |
| `skating_speed_speed_max_imperial` | double | Skater's maximum recorded skating speed in miles per hour during the tracking period. |
| `skating_speed_speed_max_metric` | double | Skater's maximum recorded skating speed in kilometers per hour during the tracking period. |
| `skating_speed_speed_max_percentile` | double | Percentile rank among all NHL skaters for maximum skating speed recorded during the tracking period. |
| `skating_speed_speed_max_league_avg_imperial` | double | League-average maximum skating speed in miles per hour, used as the baseline for this skater's percentile. |
| `skating_speed_speed_max_league_avg_metric` | double | League-average maximum skating speed in kilometers per hour, used as the baseline for this skater's percentile. |
| `skating_speed_speed_max_overlay_player_first_name_default` | character | First name of the skater who achieved the maximum skating speed in the context of the overlay comparison play. |
| `skating_speed_speed_max_overlay_player_last_name_default` | character | Last name of the skater who achieved the maximum skating speed in the context of the overlay comparison play. |
| `skating_speed_speed_max_overlay_time_in_period` | character | Elapsed time within the period (mm:ss) at which the skater's maximum skating speed was recorded. |
| `skating_speed_bursts_over20_value` | integer | Number of skating bursts the skater reached or exceeded 20 mph per game on average during the tracking period. |
| `skating_speed_bursts_over20_percentile` | double | Percentile rank among all NHL skaters for frequency of skating speed bursts exceeding 20 mph per game. |
| `skating_speed_bursts_over20_league_avg_value` | double | League-average number of skating speed bursts exceeding 20 mph per game, used as the baseline for this skater's percentile. |
| `total_distance_skated_imperial` | double | Total cumulative distance skated by the player in miles during the tracking period. |
| `total_distance_skated_metric` | double | Total cumulative distance skated by the player in kilometers during the tracking period. |
| `total_distance_skated_percentile` | double | Percentile rank among all NHL skaters for total distance skated per game during the tracking period. |
| `total_distance_skated_league_avg_imperial` | double | League-average total distance skated in miles per game, used as the baseline for this skater's distance percentile. |
| `total_distance_skated_league_avg_metric` | double | League-average total distance skated in kilometers per game, used as the baseline for this skater's distance percentile. |
| `zone_time_details_offensive_zone_pctg` | double | Percentage of the skater's total on-ice time spent in the offensive zone during the tracking period. |
| `zone_time_details_offensive_zone_percentile` | double | Percentile rank among all NHL skaters for percentage of ice time spent in the offensive zone. |
| `zone_time_details_offensive_zone_league_avg` | double | League-average percentage of on-ice time skaters spend in the offensive zone, used as the baseline for this player's percentile. |
| `zone_time_details_neutral_zone_pctg` | double | Percentage of the skater's total on-ice time spent in the neutral zone during the tracking period. |
| `zone_time_details_neutral_zone_percentile` | double | Percentile rank among all NHL skaters for percentage of ice time spent in the neutral zone. |
| `zone_time_details_neutral_zone_league_avg` | double | League-average percentage of on-ice time skaters spend in the neutral zone, used as the baseline for this player's percentile. |
| `zone_time_details_defensive_zone_pctg` | double | Percentage of the skater's total on-ice time spent in the defensive zone during the tracking period. |
| `zone_time_details_defensive_zone_percentile` | double | Percentile rank among all NHL skaters for percentage of ice time spent in the defensive zone. |
| `zone_time_details_defensive_zone_league_avg` | double | League-average percentage of on-ice time skaters spend in the defensive zone, used as the baseline for this player's percentile. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_cat_skater_detail-example}

```python
nhl_edge_cat_skater_detail(player_id=8480801)
```

_Last validated n/a._

## nhl_edge_cat_goalie_detail

Pull categorized (cat) EDGE detail stats for a single goalie.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/cat/edge/goalie-detail/{player_id}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/cat/edge/goalie-detail/8480801/now](https://api-web.nhle.com/v1/cat/edge/goalie-detail/8480801/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | player_id path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_cat_goalie_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | JSON-serialized list of NHL season identifiers for which EDGE puck-and-player tracking data is available for this goalie. |
| `shot_location_summary` | character | JSON-serialized summary of shot-location tracking data aggregated across all zones for the goalie. |
| `shot_location_details` | character | JSON-serialized per-zone shot-location tracking breakdown showing where shots were attempted against the goalie. |
| `player_id` | integer | Unique player identifier. |
| `player_first_name_default` | character | Player first name (default language). |
| `player_last_name_default` | character | Player last name (default language). |
| `player_birth_date` | character |  |
| `player_shoots_catches` | character | Side on which the goalie catches — L (left) or R (right). |
| `player_sweater_number` | integer | Player jersey number. |
| `player_slug` | character | URL slug for the player. |
| `player_headshot` | character | URL to the player headshot image. |
| `player_wins` | integer | Number of regulation and overtime wins credited to the goalie during the tracking period. |
| `player_losses` | integer | Number of regulation losses credited to the goalie during the tracking period. |
| `player_overtime_losses` | integer | Number of overtime or shootout losses credited to the goalie during the tracking period. |
| `player_goals_against_avg` | double | Goalie's goals-against average — goals allowed per 60 minutes of ice time — for the tracking period. |
| `player_save_pctg` | double | Goalie's save percentage, expressed as the proportion of shots on goal stopped. |
| `player_games_played` | integer | Number of games in which the goalie appeared during the relevant tracking period. |
| `player_team_common_name_default` | character | Player team common name (default locale). |
| `player_team_place_name_with_preposition_default` | character | Player team place name with preposition (default locale). |
| `player_team_place_name_with_preposition_fr` | character | Player team place name with preposition (French locale). |
| `player_team_abbrev` | character | Player team abbreviation. |
| `player_team_team_logo_light` | character | Player team light-mode logo URL. |
| `player_team_team_logo_dark` | character | Player team dark-mode logo URL. |
| `stats_goals_against_avg_value` | double | Goalie's goals-against average for the tracking period. |
| `stats_goals_against_avg_percentile` | double | Percentile rank among all NHL goalies for goals-against average (lower GAA = higher percentile). |
| `stats_goals_against_avg_league_avg` | double | League-average goals-against average used as the baseline for this goalie's GAA percentile calculation. |
| `stats_games_above900_value` | double | Number of games the goalie posted a save percentage above .900 during the tracking period. |
| `stats_games_above900_percentile` | double | Percentile rank among all NHL goalies for games played in which the goalie posted a save percentage above .900. |
| `stats_games_above900_league_avg` | double | League-average games-above-.900-save-percentage rate used as the baseline for this goalie's percentile calculation. |
| `stats_goal_differential_per60_value` | double | Goalie's goals-saved-above-expected per 60 minutes, measuring performance relative to shot quality faced. |
| `stats_goal_differential_per60_percentile` | double | Percentile rank among all NHL goalies for goals-saved-above-expected per 60 minutes of ice time. |
| `stats_goal_differential_per60_league_avg` | double | League-average goals-saved-above-expected per 60 minutes used as the baseline for this goalie's percentile. |
| `stats_goal_support_avg_value` | double | Average number of goals scored for the goalie per game started during the tracking period. |
| `stats_goal_support_avg_percentile` | double | Percentile rank among all NHL goalies for average offensive goal support received per game started. |
| `stats_goal_support_avg_league_avg` | double | League-average goal support (goals scored for the goalie per game) used as the baseline for percentile calculation. |
| `stats_point_pctg_value` | double | Fraction of available standings points the goalie's team earned in games the goalie started. |
| `stats_point_pctg_percentile` | double | Percentile rank among all NHL goalies for team point percentage in games the goalie started. |
| `stats_point_pctg_league_avg` | double | League-average team point percentage in games the goalie started, used as the baseline for percentile calculation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_cat_goalie_detail-example}

```python
nhl_edge_cat_goalie_detail(player_id=8480801)
```

_Last validated n/a._
