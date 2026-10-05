---
title: "NHL — NHL EDGE API — Skater (2)"
sidebar_label: "Skater (2)"
sidebar_position: 4
description: "NHL — NHL EDGE API — Skater (2) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL EDGE API — Skater (2)

## nhl_edge_skater_landing

Pull the EDGE skater landing page (summary across all skaters).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/edge/skater-landing/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/edge/skater-landing](https://api-web.nhle.com/v1/edge/skater-landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_edge_skater_landing-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `seasons_with_edge_stats` | character | List of NHL seasons for which this skater has NHL EDGE player-tracking statistics available. |
| `leaders_hardest_shot_player_id` | integer | Unique NHL identifier for the skater atop the hardest shot speed leaderboard. |
| `leaders_hardest_shot_player_first_name_default` | character | First name of the skater leading the hardest shot speed leaderboard, in the default display language. |
| `leaders_hardest_shot_player_last_name_default` | character | Last name of the skater leading the hardest shot speed leaderboard, in the default display language. |
| `leaders_hardest_shot_player_sweater_number` | integer | Jersey number worn by the skater leading the hardest shot speed leaderboard. |
| `leaders_hardest_shot_player_position` | character | Ice position (e.g., C, LW, RW, D) of the hardest shot speed leaderboard leader. |
| `leaders_hardest_shot_player_slug` | character | URL-safe slug identifying the hardest shot leaderboard leader on the NHL website. |
| `leaders_hardest_shot_player_headshot` | character | URL of the headshot image for the hardest shot leaderboard leader. |
| `leaders_hardest_shot_player_team_common_name_default` | character | Common team name of the hardest shot leaderboard leader's club, in the default language. |
| `leaders_hardest_shot_player_team_place_name_with_preposition_default` | character | Team place name with grammatical preposition in the default language for the hardest shot leader's team. |
| `leaders_hardest_shot_player_team_place_name_with_preposition_fr` | character | Team place name with grammatical preposition in French for the hardest shot leader's team. |
| `leaders_hardest_shot_player_team_abbrev` | character | Three-letter abbreviation of the team the hardest shot leaderboard leader plays for. |
| `leaders_hardest_shot_player_team_team_logo_light` | character | URL of the light-background version of the team logo for the hardest shot leaderboard leader's club. |
| `leaders_hardest_shot_player_team_team_logo_dark` | character | URL of the dark-background version of the team logo for the hardest shot leaderboard leader's club. |
| `leaders_hardest_shot_overlay_player_first_name_default` | character | First name of the skater shown in the hardest shot game overlay, in the default language. |
| `leaders_hardest_shot_overlay_player_last_name_default` | character | Last name of the skater shown in the hardest shot game overlay, in the default language. |
| `leaders_hardest_shot_overlay_game_date` | character | Calendar date of the game in which the hardest shot leader recorded their fastest shot speed. |
| `leaders_hardest_shot_overlay_away_team_abbrev` | character | Three-letter abbreviation of the away team in the game where the hardest shot leader recorded their top shot speed. |
| `leaders_hardest_shot_overlay_away_team_score` | integer | Final score of the away team in the game where the hardest shot leader recorded their top shot speed. |
| `leaders_hardest_shot_overlay_home_team_abbrev` | character | Three-letter abbreviation of the home team in the game where the hardest shot leader recorded their top shot speed. |
| `leaders_hardest_shot_overlay_home_team_score` | integer | Final score of the home team in the game where the hardest shot leader recorded their top shot speed. |
| `leaders_hardest_shot_overlay_game_outcome_last_period_type` | character | Period type (REG, OT, SO) that decided the outcome of the game where the hardest shot leader set their mark. |
| `leaders_hardest_shot_overlay_game_outcome_ot_periods` | integer | Number of overtime periods played in the game where the hardest shot leader recorded their top speed. |
| `leaders_hardest_shot_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods in the game where the hardest shot leader set their record (typically 3). |
| `leaders_hardest_shot_overlay_period_descriptor_number` | integer | Period number in which the hardest shot leader's fastest shot was recorded. |
| `leaders_hardest_shot_overlay_period_descriptor_period_type` | character | Period type label (e.g., REG, OT) for the period in which the hardest shot was recorded. |
| `leaders_hardest_shot_overlay_time_in_period` | character | Elapsed time within the period when the hardest shot leader's fastest recorded shot occurred. |
| `leaders_hardest_shot_overlay_game_type` | integer | Game type code (e.g., regular season, playoffs) for the game where the hardest shot leader set their record. |
| `leaders_hardest_shot_shot_speed_imperial` | double | Fastest NHL EDGE-tracked shot speed (in mph) recorded by the hardest shot leaderboard leader. |
| `leaders_hardest_shot_shot_speed_metric` | double | Fastest NHL EDGE-tracked shot speed (in km/h) recorded by the hardest shot leaderboard leader. |
| `leaders_max_skating_speed_player_id` | integer | Unique NHL identifier for the skater atop the maximum skating speed leaderboard. |
| `leaders_max_skating_speed_player_first_name_default` | character | First name of the skater leading the maximum skating speed leaderboard, in the default display language. |
| `leaders_max_skating_speed_player_last_name_default` | character | Last name of the skater leading the maximum skating speed leaderboard, in the default display language. |
| `leaders_max_skating_speed_player_sweater_number` | integer | Jersey number worn by the skater leading the maximum skating speed leaderboard. |
| `leaders_max_skating_speed_player_position` | character | Ice position (e.g., C, LW, RW, D) of the maximum skating speed leaderboard leader. |
| `leaders_max_skating_speed_player_slug` | character | URL-safe slug identifying the maximum skating speed leaderboard leader on the NHL website. |
| `leaders_max_skating_speed_player_headshot` | character | URL of the headshot image for the maximum skating speed leaderboard leader. |
| `leaders_max_skating_speed_player_team_common_name_default` | character | Common team name of the maximum skating speed leaderboard leader's club, in the default language. |
| `leaders_max_skating_speed_player_team_place_name_with_preposition_default` | character | Team place name with grammatical preposition in the default language for the max skating speed leader's team. |
| `leaders_max_skating_speed_player_team_place_name_with_preposition_fr` | character | Team place name with grammatical preposition in French for the max skating speed leader's team. |
| `leaders_max_skating_speed_player_team_abbrev` | character | Three-letter abbreviation of the team the maximum skating speed leaderboard leader plays for. |
| `leaders_max_skating_speed_player_team_team_logo_light` | character | URL of the light-background version of the team logo for the maximum skating speed leaderboard leader's club. |
| `leaders_max_skating_speed_player_team_team_logo_dark` | character | URL of the dark-background version of the team logo for the maximum skating speed leaderboard leader's club. |
| `leaders_max_skating_speed_overlay_player_first_name_default` | character | First name of the skater shown in the max skating speed game overlay, in the default language. |
| `leaders_max_skating_speed_overlay_player_last_name_default` | character | Last name of the skater shown in the max skating speed game overlay, in the default language. |
| `leaders_max_skating_speed_overlay_game_date` | character | Calendar date of the game in which the max skating speed leader recorded their fastest burst speed. |
| `leaders_max_skating_speed_overlay_away_team_abbrev` | character | Three-letter abbreviation of the away team in the game where the max skating speed leader recorded their top speed. |
| `leaders_max_skating_speed_overlay_away_team_score` | integer | Final score of the away team in the game where the max skating speed leader set their top burst speed. |
| `leaders_max_skating_speed_overlay_home_team_abbrev` | character | Three-letter abbreviation of the home team in the game where the max skating speed leader recorded their top speed. |
| `leaders_max_skating_speed_overlay_home_team_score` | integer | Final score of the home team in the game where the max skating speed leader set their top burst speed. |
| `leaders_max_skating_speed_overlay_game_outcome_last_period_type` | character | Period type (REG, OT, SO) that decided the game where the max skating speed leader set their top mark. |
| `leaders_max_skating_speed_overlay_game_outcome_ot_periods` | integer | Number of overtime periods played in the game where the max skating speed leader recorded their fastest speed. |
| `leaders_max_skating_speed_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods in the game where the max skating speed leader set their record (typically 3). |
| `leaders_max_skating_speed_overlay_period_descriptor_number` | integer | Period number in which the max skating speed leader's fastest burst was recorded. |
| `leaders_max_skating_speed_overlay_period_descriptor_period_type` | character | Period type label (e.g., REG, OT) for the period in which the max skating speed was recorded. |
| `leaders_max_skating_speed_overlay_time_in_period` | character | Elapsed time within the period when the max skating speed leader's fastest burst was recorded. |
| `leaders_max_skating_speed_overlay_game_type` | integer | Game type code (e.g., regular season, playoffs) for the game where the max skating speed leader set their record. |
| `leaders_max_skating_speed_skating_speed_imperial` | double | Peak skating burst speed (in mph) recorded by the maximum skating speed leaderboard leader, per NHL EDGE tracking. |
| `leaders_max_skating_speed_skating_speed_metric` | double | Peak skating burst speed (in km/h) recorded by the maximum skating speed leaderboard leader, per NHL EDGE tracking. |
| `leaders_total_distance_skated_player_id` | integer | Unique NHL identifier for the skater atop the total distance skated leaderboard. |
| `leaders_total_distance_skated_player_first_name_default` | character | First name of the skater leading the total distance skated leaderboard, in the default display language. |
| `leaders_total_distance_skated_player_last_name_default` | character | Last name of the skater leading the total distance skated leaderboard, in the default display language. |
| `leaders_total_distance_skated_player_sweater_number` | integer | Jersey number worn by the skater leading the total distance skated leaderboard. |
| `leaders_total_distance_skated_player_position` | character | Ice position (e.g., C, LW, RW, D) of the total distance skated leaderboard leader. |
| `leaders_total_distance_skated_player_slug` | character | URL-safe slug identifying the total distance skated leaderboard leader on the NHL website. |
| `leaders_total_distance_skated_player_headshot` | character | URL of the headshot image for the total distance skated leaderboard leader. |
| `leaders_total_distance_skated_player_team_common_name_default` | character | Common team name of the total distance skated leaderboard leader's club, in the default language. |
| `leaders_total_distance_skated_player_team_place_name_with_preposition_default` | character | Team place name with grammatical preposition in the default language for the total distance skated leader's team. |
| `leaders_total_distance_skated_player_team_place_name_with_preposition_fr` | character | Team place name with grammatical preposition in French for the total distance skated leader's team. |
| `leaders_total_distance_skated_player_team_abbrev` | character | Three-letter abbreviation of the team the total distance skated leaderboard leader plays for. |
| `leaders_total_distance_skated_player_team_team_logo_light` | character | URL of the light-background version of the team logo for the total distance skated leaderboard leader's club. |
| `leaders_total_distance_skated_player_team_team_logo_dark` | character | URL of the dark-background version of the team logo for the total distance skated leaderboard leader's club. |
| `leaders_total_distance_skated_distance_skated_imperial` | double | Cumulative skating distance (in miles) recorded by the total distance skated leaderboard leader, per NHL EDGE tracking. |
| `leaders_total_distance_skated_distance_skated_metric` | double | Cumulative skating distance (in kilometres) recorded by the total distance skated leaderboard leader, per NHL EDGE tracking. |
| `leaders_distance_max_game_player_id` | integer | Unique NHL identifier for the skater who recorded the highest single-game skating distance. |
| `leaders_distance_max_game_player_first_name_default` | character | First name of the skater who skated the farthest distance in a single game, in the default display language. |
| `leaders_distance_max_game_player_last_name_default` | character | Last name of the skater who skated the farthest distance in a single game, in the default display language. |
| `leaders_distance_max_game_player_sweater_number` | integer | Jersey number worn by the skater who recorded the highest single-game skating distance. |
| `leaders_distance_max_game_player_position` | character | Ice position (e.g., C, LW, RW, D) of the single-game distance skating leader. |
| `leaders_distance_max_game_player_slug` | character | URL-safe slug identifying the single-game distance skating leader on the NHL website. |
| `leaders_distance_max_game_player_headshot` | character | URL of the headshot image for the skater atop the single-game distance leaderboard. |
| `leaders_distance_max_game_player_team_common_name_default` | character | Common team name of the single-game distance skating leader's club, in the default language. |
| `leaders_distance_max_game_player_team_place_name_with_preposition_default` | character | Team place name with grammatical preposition in the default language for the single-game distance leader's team. |
| `leaders_distance_max_game_player_team_place_name_with_preposition_fr` | character | Team place name with grammatical preposition in French for the single-game distance leader's team. |
| `leaders_distance_max_game_player_team_abbrev` | character | Three-letter abbreviation of the team the single-game distance skating leader plays for. |
| `leaders_distance_max_game_player_team_team_logo_light` | character | URL of the light-background version of the team logo for the single-game distance skating leader's club. |
| `leaders_distance_max_game_player_team_team_logo_dark` | character | URL of the dark-background version of the team logo for the single-game distance skating leader's club. |
| `leaders_distance_max_game_distance_skated_imperial` | double | Farthest single-game skating distance (in miles) recorded by the distance-max-game leaderboard leader, per NHL EDGE tracking. |
| `leaders_distance_max_game_distance_skated_metric` | double | Farthest single-game skating distance (in kilometres) recorded by the distance-max-game leaderboard leader, per NHL EDGE tracking. |
| `leaders_distance_max_game_overlay_player_first_name_default` | character | First name of the skater displayed in the distance-max-game game overlay, in the default language. |
| `leaders_distance_max_game_overlay_player_last_name_default` | character | Last name of the skater displayed in the distance-max-game game overlay, in the default language. |
| `leaders_distance_max_game_overlay_game_date` | character | Calendar date of the game in which the distance-max-game leader skated their record single-game distance. |
| `leaders_distance_max_game_overlay_away_team_abbrev` | character | Three-letter abbreviation of the away team in the game where the distance-max-game leader set their top mark. |
| `leaders_distance_max_game_overlay_away_team_score` | integer | Final score of the away team in the game where the distance-max-game leader set their single-game distance record. |
| `leaders_distance_max_game_overlay_home_team_abbrev` | character | Three-letter abbreviation of the home team in the game where the distance-max-game leader set their top mark. |
| `leaders_distance_max_game_overlay_home_team_score` | integer | Final score of the home team in the game where the distance-max-game leader set their single-game distance record. |
| `leaders_distance_max_game_overlay_game_outcome_last_period_type` | character | Period type (REG, OT, SO) that decided the outcome of the game where the distance-max-game leader set their record. |
| `leaders_distance_max_game_overlay_game_outcome_ot_periods` | integer | Number of overtime periods played in the game where the distance-max-game leader set their single-game distance record. |
| `leaders_distance_max_game_overlay_period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods in the game where the distance-max-game leader set their record (typically 3). |
| `leaders_distance_max_game_overlay_period_descriptor_number` | integer | Period number in which the distance-max-game leader's record tracking event was most notable. |
| `leaders_distance_max_game_overlay_period_descriptor_period_type` | character | Period type label (e.g., REG, OT) for the period highlighted in the distance-max-game overlay. |
| `leaders_distance_max_game_overlay_game_type` | integer | Game type code (e.g., regular season, playoffs) for the game where the distance-max-game leader set their record. |
| `leaders_high_danger_sog_player_id` | integer | Unique NHL identifier for the skater atop the high-danger shots-on-goal leaderboard. |
| `leaders_high_danger_sog_player_first_name_default` | character | First name of the skater leading the high-danger shots-on-goal leaderboard, in the default display language. |
| `leaders_high_danger_sog_player_last_name_default` | character | Last name of the skater leading the high-danger shots-on-goal leaderboard, in the default display language. |
| `leaders_high_danger_sog_player_sweater_number` | integer | Jersey number worn by the skater leading the high-danger shots-on-goal leaderboard. |
| `leaders_high_danger_sog_player_position` | character | Ice position (e.g., C, LW, RW, D) of the high-danger shots-on-goal leaderboard leader. |
| `leaders_high_danger_sog_player_slug` | character | URL-safe slug identifying the high-danger shots-on-goal leaderboard leader on the NHL website. |
| `leaders_high_danger_sog_player_headshot` | character | URL of the headshot image for the high-danger shots-on-goal leaderboard leader. |
| `leaders_high_danger_sog_player_team_common_name_default` | character | Common team name of the high-danger shots-on-goal leaderboard leader's club, in the default language. |
| `leaders_high_danger_sog_player_team_place_name_with_preposition_default` | character | Team place name with grammatical preposition in the default language for the high-danger shots-on-goal leader's team. |
| `leaders_high_danger_sog_player_team_place_name_with_preposition_fr` | character | Team place name with grammatical preposition in French for the high-danger shots-on-goal leader's team. |
| `leaders_high_danger_sog_player_team_abbrev` | character | Three-letter abbreviation of the team the high-danger shots-on-goal leader plays for. |
| `leaders_high_danger_sog_player_team_team_logo_light` | character | URL of the light-background version of the team logo for the high-danger shots-on-goal leader's club. |
| `leaders_high_danger_sog_player_team_team_logo_dark` | character | URL of the dark-background version of the team logo for the high-danger shots-on-goal leader's club. |
| `leaders_high_danger_sog_sog` | integer | Total number of high-danger shots on goal recorded by the leaderboard leader, as tracked by NHL EDGE puck tracking. |
| `leaders_high_danger_sog_shot_location_details` | character | Structured details describing the ice zones or slot locations from which the high-danger shots-on-goal leader's tracked shots originated. |
| `leaders_offensive_zone_time_player_id` | integer | Unique NHL identifier for the skater atop the offensive zone time leaderboard. |
| `leaders_offensive_zone_time_player_first_name_default` | character | First name of the skater leading the offensive zone time leaderboard, in the default display language. |
| `leaders_offensive_zone_time_player_last_name_default` | character | Last name of the skater leading the offensive zone time leaderboard, in the default display language. |
| `leaders_offensive_zone_time_player_sweater_number` | integer | Jersey number worn by the skater leading the offensive zone time leaderboard. |
| `leaders_offensive_zone_time_player_position` | character | Ice position (e.g., C, LW, RW, D) of the offensive zone time leaderboard leader. |
| `leaders_offensive_zone_time_player_slug` | character | URL-safe slug identifying the offensive zone time leaderboard leader on the NHL website. |
| `leaders_offensive_zone_time_player_headshot` | character | URL of the headshot image for the offensive zone time leaderboard leader. |
| `leaders_offensive_zone_time_player_team_common_name_default` | character | Common team name of the offensive zone time leaderboard leader's club, in the default language. |
| `leaders_offensive_zone_time_player_team_place_name_with_preposition_default` | character | Team place name with grammatical preposition in the default language for the offensive zone time leader's team. |
| `leaders_offensive_zone_time_player_team_place_name_with_preposition_fr` | character | Team place name with grammatical preposition in French for the offensive zone time leader's team. |
| `leaders_offensive_zone_time_player_team_abbrev` | character | Three-letter abbreviation of the team the offensive zone time leaderboard leader plays for. |
| `leaders_offensive_zone_time_player_team_team_logo_light` | character | URL of the light-background version of the team logo for the offensive zone time leaderboard leader's club. |
| `leaders_offensive_zone_time_player_team_team_logo_dark` | character | URL of the dark-background version of the team logo for the offensive zone time leaderboard leader's club. |
| `leaders_offensive_zone_time_zone_time` | double | Total time the offensive zone time leaderboard leader spent in the offensive zone, as tracked by NHL EDGE player tracking. |
| `leaders_defensive_zone_time_player_id` | integer | Unique NHL identifier for the skater atop the defensive zone time leaderboard. |
| `leaders_defensive_zone_time_player_first_name_default` | character | First name of the skater leading the defensive zone time leaderboard, in the default display language. |
| `leaders_defensive_zone_time_player_last_name_default` | character | Last name of the skater leading the defensive zone time leaderboard, in the default display language. |
| `leaders_defensive_zone_time_player_sweater_number` | integer | Jersey number worn by the skater leading the defensive zone time leaderboard. |
| `leaders_defensive_zone_time_player_position` | character | Ice position (e.g., C, LW, RW, D) of the defensive zone time leaderboard leader. |
| `leaders_defensive_zone_time_player_slug` | character | URL-safe slug identifying the defensive zone time leaderboard leader on the NHL website. |
| `leaders_defensive_zone_time_player_headshot` | character | URL of the headshot image for the defensive zone time leaderboard leader. |
| `leaders_defensive_zone_time_player_team_common_name_default` | character | Common team name (e.g., Maple Leafs) of the defensive zone time leaderboard leader's club, in the default language. |
| `leaders_defensive_zone_time_player_team_place_name_with_preposition_default` | character | Team place name with grammatical preposition in the default language (e.g., "in Toronto") for the defensive zone time leader's team. |
| `leaders_defensive_zone_time_player_team_place_name_with_preposition_fr` | character | Team place name with grammatical preposition in French for the defensive zone time leader's team. |
| `leaders_defensive_zone_time_player_team_abbrev` | character | Three-letter abbreviation of the team the defensive zone time leader plays for. |
| `leaders_defensive_zone_time_player_team_team_logo_light` | character | URL of the light-background version of the team logo for the defensive zone time leaderboard leader's club. |
| `leaders_defensive_zone_time_player_team_team_logo_dark` | character | URL of the dark-background version of the team logo for the defensive zone time leaderboard leader's club. |
| `leaders_defensive_zone_time_zone_time` | double | Total time the defensive zone time leaderboard leader spent in the defensive zone, as tracked by NHL EDGE player tracking. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_edge_skater_landing-example}

```python
nhl_edge_skater_landing()
```

_Last validated n/a._
