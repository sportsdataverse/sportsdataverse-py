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
| `event_id` | Int64 | ESPN event id (echoed from arg). |
| `time_in_period` | String | Time elapsed in the period when the shot occurred. |
| `time_remaining` | String |  |
| `situation_code` | String |  |
| `home_team_defending_side` | String | Ice end ('left' or 'right') that the home team is defending in the current period, used to orient x/y coordinates in the NHL api-web play-by-play feed. |
| `type_code` | Int64 | Numeric event-type code identifying the category of play (e.g., goal, shot, hit, penalty, faceoff) in the NHL api-web play-by-play feed. |
| `type_desc_key` | String | String key describing the event type category (e.g., 'goal', 'shot-on-goal', 'hit', 'faceoff') in the NHL api-web play-by-play feed. |
| `sort_order` | Int64 |  |
| `period_descriptor_number` | Int64 | Period number. |
| `period_descriptor_period_type` | String | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods. |
| `details_event_owner_team_id` | Int64 | NHL api-web team identifier for the team credited with or responsible for the play event in the play-by-play feed. |
| `details_losing_player_id` | Int64 | NHL api-web player identifier for the skater who lost the faceoff on a faceoff event in the play-by-play feed. |
| `details_winning_player_id` | Int64 | NHL api-web player identifier for the skater who won the faceoff on a faceoff event in the play-by-play feed. |
| `details_x_coord` | Float64 | Horizontal rink coordinate (feet from centre ice, positive toward right side) of the play event location in the NHL api-web play-by-play feed. |
| `details_y_coord` | Float64 | Vertical rink coordinate (feet from centre ice, positive toward one end) of the play event location in the NHL api-web play-by-play feed. |
| `details_zone_code` | String | Ice zone where the play event occurred, coded as 'O' (offensive), 'D' (defensive), or 'N' (neutral) relative to the event owner team in the play-by-play feed. |
| `details_shot_type` | String | Classification of the shot attempt (e.g., wrist shot, slap shot, backhand, deflection) as provided in the NHL api-web play-by-play details. |
| `details_shooting_player_id` | Int64 | NHL api-web player identifier for the skater who took the shot on a shot-on-goal, missed-shot, or blocked-shot event in the play-by-play feed. |
| `details_goalie_in_net_id` | Int64 | NHL api-web player identifier for the goaltender who was in the net at the time of the shot, goal, or missed-shot event in the play-by-play feed. |
| `details_away_sog` | Float64 | Cumulative shots on goal by the away team at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_home_sog` | Float64 | Cumulative shots on goal by the home team at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_reason` | String | Primary reason or description for the play event (e.g., specific penalty infraction name) as provided in the NHL api-web play-by-play details. |
| `details_blocking_player_id` | Int64 | NHL api-web player identifier for the skater who blocked a shot on the blocked-shot event in the play-by-play feed. |
| `details_hitting_player_id` | Int64 | NHL api-web player identifier for the skater who delivered the body check on a hit event in the play-by-play feed. |
| `details_hittee_player_id` | Int64 | NHL api-web player identifier for the skater who received the body check on a hit event in the play-by-play feed. |
| `details_player_id` | Int64 | NHL api-web player identifier for the primary player involved in the play event (used on giveaway, takeaway, and similar single-player events). |
| `details_type_code` | String | Structured sub-type code providing additional classification within the play event category in the NHL api-web play-by-play feed. |
| `details_desc_key` | String | Short descriptor key providing additional classification of the play event (e.g., penalty type or shot outcome) in the NHL api-web play-by-play feed. |
| `details_duration` | Float64 | Duration of the penalty in minutes as specified in the play event details of the NHL api-web play-by-play feed. |
| `details_committed_by_player_id` | Int64 | NHL api-web player identifier for the player who committed the infraction on a penalty event in the play-by-play feed. |
| `details_drawn_by_player_id` | Int64 | NHL api-web player identifier for the player who drew (was the victim of) the penalty on a penalty event in the play-by-play feed. |
| `ppt_replay_url` | String | URL to the power-play tracking replay video associated with this play event in the NHL api-web play-by-play feed. |
| `details_scoring_player_id` | Int64 | NHL api-web player identifier for the skater who scored the goal on a goal event in the play-by-play feed. |
| `details_scoring_player_total` | Float64 | Running season goal total for the scoring player at the time of the goal event in the NHL api-web play-by-play feed. |
| `details_assist1_player_id` | Int64 | NHL api-web player identifier for the primary (first) assist credited on a goal event in the play-by-play feed. |
| `details_assist1_player_total` | Float64 | Running season assist total for the primary assist player at the time of the goal event in the play-by-play feed. |
| `details_assist2_player_id` | Int64 | NHL api-web player identifier for the secondary (second) assist credited on a goal event in the play-by-play feed. |
| `details_assist2_player_total` | Float64 | Running season assist total for the secondary assist player at the time of the goal event in the play-by-play feed. |
| `details_away_score` | Float64 | Cumulative away-team score at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_home_score` | Float64 | Cumulative home-team score at the moment of the play event in the NHL api-web play-by-play feed. |
| `details_highlight_clip_sharing_url` | String | Public sharing URL for the English-language broadcast highlight clip of this play event from the NHL api-web play-by-play feed. |
| `details_highlight_clip` | Float64 | NHL api-web identifier for the broadcast highlight clip associated with this play event in the play-by-play feed (English feed). |
| `details_discrete_clip` | Float64 | NHL api-web clip identifier for the discrete video clip of this play event in the play-by-play feed (English feed). |
| `details_discrete_clip_fr` | Float64 | NHL api-web clip identifier for the discrete video clip of this play event in the play-by-play feed (French feed). |
| `details_highlight_clip_sharing_url_fr` | String | Public sharing URL for the French-language broadcast highlight clip of this play event from the NHL api-web play-by-play feed. |
| `details_highlight_clip_fr` | Float64 | NHL api-web identifier for the broadcast highlight clip associated with this play event in the play-by-play feed (French feed). |
| `details_secondary_reason` | String | Secondary descriptive reason or sub-classification for the play event as provided in the NHL api-web play-by-play details (e.g., penalty sub-type). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_web_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_web_pbp-example}

```python
nhl_web_pbp(game_id=2024020001)
```

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
| `schedule_date` | String | Calendar date of the game in YYYY-MM-DD format as returned by the NHL api-web schedule endpoint. |
| `id` | Int64 | Unique player identifier. |
| `season` | Int64 | Season year (echoed from arg). |
| `game_type` | Int64 | Game type the row belongs to. |
| `neutral_site` | Boolean | Whether the game is at a neutral site. |
| `start_time_utc` | String | Scheduled start time in UTC. |
| `eastern_utc_offset` | String | Eastern time UTC offset. |
| `venue_utc_offset` | String | Venue UTC offset. |
| `venue_timezone` | String | Venue time zone. |
| `game_state` | String | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | String | Schedule state of the game. |
| `tv_broadcasts` | String | Nested list of TV broadcast details. |
| `series_url` | String | NHL api-web URL path to the dedicated page for the playoff series associated with this scheduled game. |
| `three_min_recap` | String | Link to the three-minute recap. |
| `game_center_link` | String | Link to the NHL game center page. |
| `venue_default` | String | Venue name (default language). |
| `away_team_id` | Int64 | Away team identifier. |
| `away_team_common_name_default` | String | Away team common name (default language). |
| `away_team_place_name_default` | String | Away team place name (default language). |
| `away_team_place_name_with_preposition_default` | String | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | String | Away team place name with preposition (French). |
| `away_team_abbrev` | String | Away team abbreviation. |
| `away_team_logo` | String | URL to the away team logo. |
| `away_team_dark_logo` | String | URL to the away team dark logo. |
| `away_team_away_split_squad` | Boolean | Whether the away team is a split squad. |
| `away_team_score` | Int64 | Away team final score. |
| `home_team_id` | Int64 | Home team identifier. |
| `home_team_common_name_default` | String | Home team common name (default language). |
| `home_team_place_name_default` | String | Home team place name (default language). |
| `home_team_place_name_fr` | String | Home team place name (French). |
| `home_team_place_name_with_preposition_default` | String | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | String | Home team place name with preposition (French). |
| `home_team_abbrev` | String | Home team abbreviation. |
| `home_team_logo` | String | URL to the home team logo. |
| `home_team_dark_logo` | String | URL to the home team dark logo. |
| `home_team_home_split_squad` | Boolean | Whether the home team is a split squad. |
| `home_team_score` | Int64 | Home team final score. |
| `period_descriptor_number` | Int64 | Period number. |
| `period_descriptor_period_type` | String | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | String | Period type in which the game ended. |
| `winning_goalie_player_id` | Int64 | Winning goalie player identifier. |
| `winning_goalie_first_initial_default` | String | Winning goalie first initial (default language). |
| `winning_goalie_last_name_default` | String | Winning goalie last name (default language). |
| `winning_goalie_last_name_cs` | String | Winning goalie last name (Czech). |
| `winning_goalie_last_name_fi` | String | Winning goalie last name (Finnish). |
| `winning_goalie_last_name_sk` | String | Winning goalie last name (Slovak). |
| `winning_goal_scorer_player_id` | Int64 | Winning goal scorer player identifier. |
| `winning_goal_scorer_first_initial_default` | String | Winning goal scorer first initial (default). |
| `winning_goal_scorer_last_name_default` | String | Winning goal scorer last name (default language). |
| `series_status_round` | Int64 | Playoff round number (1 through 4) for the series containing this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_series_abbrev` | String | Short abbreviation identifying the specific playoff series slot within the bracket for this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_series_title` | String | Human-readable display title for the playoff series containing this scheduled game (e.g., 'Eastern Conference Second Round'). |
| `series_status_series_letter` | String | Single-letter label assigned to the playoff series in the bracket structure for this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_needed_to_win` | Int64 | Number of additional wins needed by the series leader to clinch and advance at the time of this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_top_seed_team_abbrev` | String | Three-letter team abbreviation for the higher-seeded team in the playoff series associated with this scheduled game. |
| `series_status_top_seed_wins` | Int64 | Number of wins accumulated by the higher-seeded team in the playoff series as of this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_bottom_seed_team_abbrev` | String | Three-letter team abbreviation for the lower-seeded team in the playoff series associated with this scheduled game. |
| `series_status_bottom_seed_wins` | Int64 | Number of wins accumulated by the lower-seeded team in the playoff series as of this scheduled game from the NHL api-web schedule endpoint. |
| `series_status_game_number_of_series` | Int64 | Sequential game number within the playoff series for this scheduled game (e.g., 1 through 7 for a best-of-seven) from the NHL api-web schedule endpoint. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_web_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_web_schedule-example}

```python
nhl_web_schedule()
```
