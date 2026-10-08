---
title: "NHL — NHL Web API — Web"
sidebar_label: "Web"
sidebar_position: 4
description: "NHL — NHL Web API — Web — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Web API — Web

## nhl_web_pbp

Pull the play-by-play feed for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play](https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns {#nhl_web_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_id` | integer | ESPN event id (echoed from arg). |
| `time_in_period` | character | Time elapsed in the period when the shot occurred. |
| `time_remaining` | character |  |
| `situation_code` | character |  |
| `home_team_defending_side` | character | Ice end ('left' or 'right') that the home team is defending in the current period, used to orient x/y coordinates in the NHL api-web play-by-play feed. |
| `type_code` | integer | Numeric event-type code identifying the category of play (e.g., goal, shot, hit, penalty, faceoff) in the NHL api-web play-by-play feed. |
| `type_desc_key` | character | String key describing the event type category (e.g., 'goal', 'shot-on-goal', 'hit', 'faceoff') in the NHL api-web play-by-play feed. |
| `sort_order` | integer |  |
| `period_descriptor_number` | integer | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | integer | Maximum number of regulation periods. |
| `details_event_owner_team_id` | double | NHL api-web team identifier for the team credited with or responsible for the play event in the play-by-play feed. |
| `details_losing_player_id` | double | NHL api-web player identifier for the skater who lost the faceoff on a faceoff event in the play-by-play feed. |
| `details_winning_player_id` | double | NHL api-web player identifier for the skater who won the faceoff on a faceoff event in the play-by-play feed. |
| `details_x_coord` | double | Horizontal rink coordinate (feet from centre ice, positive toward right side) of the play event location in the NHL api-web play-by-play feed. |
| `details_y_coord` | double | Vertical rink coordinate (feet from centre ice, positive toward one end) of the play event location in the NHL api-web play-by-play feed. |
| `details_zone_code` | character | Ice zone where the play event occurred, coded as 'O' (offensive), 'D' (defensive), or 'N' (neutral) relative to the event owner team in the play-by-play feed. |
| `details_shot_type` | character | Classification of the shot attempt (e.g., wrist shot, slap shot, backhand, deflection) as provided in the NHL api-web play-by-play details. |
| `details_shooting_player_id` | double | NHL api-web player identifier for the skater who took the shot on a shot-on-goal, missed-shot, or blocked-shot event in the play-by-play feed. |
| `details_goalie_in_net_id` | double | NHL api-web player identifier for the goaltender who was in the net at the time of the shot, goal, or missed-shot event in the play-by-play feed. |
| `details_away_sog` | double | Cumulative shots on goal by the away team at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_home_sog` | double | Cumulative shots on goal by the home team at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_reason` | character | Primary reason or description for the play event (e.g., specific penalty infraction name) as provided in the NHL api-web play-by-play details. |
| `details_blocking_player_id` | double | NHL api-web player identifier for the skater who blocked a shot on the blocked-shot event in the play-by-play feed. |
| `details_hitting_player_id` | double | NHL api-web player identifier for the skater who delivered the body check on a hit event in the play-by-play feed. |
| `details_hittee_player_id` | double | NHL api-web player identifier for the skater who received the body check on a hit event in the play-by-play feed. |
| `details_player_id` | double | NHL api-web player identifier for the primary player involved in the play event (used on giveaway, takeaway, and similar single-player events). |
| `details_type_code` | character | Structured sub-type code providing additional classification within the play event category in the NHL api-web play-by-play feed. |
| `details_desc_key` | character | Short descriptor key providing additional classification of the play event (e.g., penalty type or shot outcome) in the NHL api-web play-by-play feed. |
| `details_duration` | double | Duration of the penalty in minutes as specified in the play event details of the NHL api-web play-by-play feed. |
| `details_committed_by_player_id` | double | NHL api-web player identifier for the player who committed the infraction on a penalty event in the play-by-play feed. |
| `details_drawn_by_player_id` | double | NHL api-web player identifier for the player who drew (was the victim of) the penalty on a penalty event in the play-by-play feed. |
| `ppt_replay_url` | character | URL to the power-play tracking replay video associated with this play event in the NHL api-web play-by-play feed. |
| `details_scoring_player_id` | double | NHL api-web player identifier for the skater who scored the goal on a goal event in the play-by-play feed. |
| `details_scoring_player_total` | double | Running season goal total for the scoring player at the time of the goal event in the NHL api-web play-by-play feed. |
| `details_assist1_player_id` | double | NHL api-web player identifier for the primary (first) assist credited on a goal event in the play-by-play feed. |
| `details_assist1_player_total` | double | Running season assist total for the primary assist player at the time of the goal event in the play-by-play feed. |
| `details_assist2_player_id` | double | NHL api-web player identifier for the secondary (second) assist credited on a goal event in the play-by-play feed. |
| `details_assist2_player_total` | double | Running season assist total for the secondary assist player at the time of the goal event in the play-by-play feed. |
| `details_away_score` | double | Cumulative away-team score at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_home_score` | double | Cumulative home-team score at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_highlight_clip_sharing_url` | character | Public sharing URL for the English-language broadcast highlight clip of this play event from the NHL api-web play-by-play feed. |
| `details_highlight_clip` | double | NHL api-web identifier for the broadcast highlight clip associated with this play event in the play-by-play feed (English feed). |
| `details_discrete_clip` | double | NHL api-web clip identifier for the discrete video clip of this play event in the play-by-play feed (English feed). |
| `details_discrete_clip_fr` | double | NHL api-web clip identifier for the discrete video clip of this play event in the play-by-play feed (French feed). |
| `details_highlight_clip_sharing_url_fr` | character | Public sharing URL for the French-language broadcast highlight clip of this play event from the NHL api-web play-by-play feed. |
| `details_highlight_clip_fr` | double | NHL api-web identifier for the broadcast highlight clip associated with this play event in the play-by-play feed (French feed). |
| `details_secondary_reason` | character | Secondary descriptive reason or sub-classification for the play event as provided in the NHL api-web play-by-play details (e.g., penalty sub-type). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_web_pbp-example}

```python
nhl_web_pbp(game_id=2024020001)
```

_Last validated n/a._

## nhl_web_schedule

Pull the week-of NHL schedule rooted at `date`.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule/now](https://api-web.nhle.com/v1/schedule/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns {#nhl_web_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_date` | character | Calendar date of the game in YYYY-MM-DD format as returned by the NHL api-web schedule endpoint. |
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
| `series_url` | character | NHL api-web URL path to the dedicated page for the playoff series associated with this scheduled game. |
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
| `series_status_round` | integer | Playoff round number (1 through 4) for the series containing this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_series_abbrev` | character | Short abbreviation identifying the specific playoff series slot within the bracket for this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_series_title` | character | Human-readable display title for the playoff series containing this scheduled game (e.g., 'Eastern Conference Second Round'). |
| `series_status_series_letter` | character | Single-letter label assigned to the playoff series in the bracket structure for this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_needed_to_win` | integer | Number of additional wins needed by the series leader to clinch and advance at the time of this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_top_seed_team_abbrev` | character | Three-letter team abbreviation for the higher-seeded team in the playoff series associated with this scheduled game. |
| `series_status_top_seed_wins` | integer | Number of wins accumulated by the higher-seeded team in the playoff series as of this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_bottom_seed_team_abbrev` | character | Three-letter team abbreviation for the lower-seeded team in the playoff series associated with this scheduled game. |
| `series_status_bottom_seed_wins` | integer | Number of wins accumulated by the lower-seeded team in the playoff series as of this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_game_number_of_series` | integer | Sequential game number within the playoff series for this scheduled game (e.g., 1 through 7 for a best-of-seven) from the NHL api-web schedule endpoint. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_web_schedule-example}

```python
nhl_web_schedule()
```

_Last validated n/a._
