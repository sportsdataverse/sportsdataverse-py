---
title: "NHL — NHL Web API — Club"
sidebar_label: "Club"
sidebar_position: 1
description: "NHL — NHL Web API — Club — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Web API — Club

## nhl_club_schedule_season

Pull a team's full-season schedule.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule-season/TOR/now](https://api-web.nhle.com/v1/club-schedule-season/TOR/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |

### Returns {#nhl_club_schedule_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `club_previous_season` | Int64 | Indicator for whether the game belongs to the club's prior completed season (1 = previous season, 0 otherwise). |
| `club_current_season` | Int64 | Indicator for whether the game falls within the current season for the requesting club (1 = current season, 0 otherwise). |
| `club_next_season` | Int64 | Indicator for whether the game belongs to the club's next upcoming season (1 = next season, 0 otherwise). |
| `club_timezone` | String | IANA timezone identifier for the home club's arena, used to localise game start times in the schedule. |
| `id` | Int64 | Unique player identifier. |
| `season` | Int64 | Season year (echoed from arg). |
| `game_type` | Int64 | Game type the row belongs to. |
| `game_date` | String | Game date. |
| `neutral_site` | Boolean | Whether the game is at a neutral site. |
| `start_time_utc` | String | Scheduled start time in UTC. |
| `eastern_utc_offset` | String | Eastern time UTC offset. |
| `venue_utc_offset` | String | Venue UTC offset. |
| `venue_timezone` | String | Venue time zone. |
| `game_state` | String | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | String | Schedule state of the game. |
| `tv_broadcasts` | String | Nested list of TV broadcast details. |
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
| `home_team_place_name_with_preposition_default` | String | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | String | Home team place name with preposition (French). |
| `home_team_abbrev` | String | Home team abbreviation. |
| `home_team_logo` | String | URL to the home team logo. |
| `home_team_dark_logo` | String | URL to the home team dark logo. |
| `home_team_home_split_squad` | Boolean | Whether the home team is a split squad. |
| `home_team_airline_link` | String | Link to home team airline info. |
| `home_team_airline_desc` | String | Home team airline description. |
| `home_team_hotel_link` | String | Link to home team hotel info. |
| `home_team_hotel_desc` | String | Home team hotel description. |
| `home_team_score` | Int64 | Home team final score. |
| `period_descriptor_period_type` | String | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | String | Period type in which the game ended. |
| `winning_goalie_player_id` | Int64 | Winning goalie player identifier. |
| `winning_goalie_first_initial_default` | String | Winning goalie first initial (default language). |
| `winning_goalie_last_name_default` | String | Winning goalie last name (default language). |
| `away_team_airline_link` | String | Link to away team airline info. |
| `away_team_airline_desc` | String | Away team airline description. |
| `winning_goal_scorer_player_id` | Int64 | Winning goal scorer player identifier. |
| `winning_goal_scorer_first_initial_default` | String | Winning goal scorer first initial (default). |
| `winning_goal_scorer_last_name_default` | String | Winning goal scorer last name (default language). |
| `three_min_recap` | String | Link to the three-minute recap. |
| `home_team_place_name_fr` | String | Home team place name (French). |
| `condensed_game` | String | Link to the condensed game video. |
| `venue_es` | String | Venue name (Spanish). |
| `venue_fr` | String | Venue name (French). |
| `special_event_parent_id` | Int64 | NHL api-web identifier for the parent special-event record grouping multiple games under the same marquee event umbrella. |
| `special_event_name_default` | String | English display name for a special promotional or marquee event designation attached to the game (e.g., 'Winter Classic', 'Heritage Classic'). |
| `special_event_name_fr` | String | French display name for a special promotional or marquee event designation attached to the game, used in bilingual NHL communications. |
| `away_team_hotel_link` | String | Link to away team hotel info. |
| `away_team_hotel_desc` | String | Away team hotel description. |
| `three_min_recap_fr` | String | Link to the French three-minute recap. |
| `winning_goalie_last_name_cs` | String | Winning goalie last name (Czech). |
| `winning_goalie_last_name_fi` | String | Winning goalie last name (Finnish). |
| `winning_goalie_last_name_sk` | String | Winning goalie last name (Slovak). |
| `away_team_place_name_fr` | String | Away team place name (French). |
| `away_team_common_name_fr` | String | Away team common name (French). |
| `home_team_common_name_fr` | String | Home team common name (French). |
| `series_url` | String | NHL api-web URL path to the dedicated page for the current playoff series associated with this scheduled game. |
| `series_status_round` | Float64 | Playoff round number to which the current series belongs (1 = first round, 4 = Stanley Cup Final). |
| `series_status_series_abbrev` | String | Short abbreviation identifying the specific playoff series slot (e.g., 'A', 'B') within the bracket for this game. |
| `series_status_series_title` | String | Human-readable display title for the playoff series (e.g., 'Eastern Conference First Round'). |
| `series_status_series_letter` | String | Single-letter label assigned to the playoff series in the bracket structure, used to pair teams across rounds. |
| `series_status_needed_to_win` | Float64 | Wins still required by the leading team to clinch and advance in the current playoff series. |
| `series_status_top_seed_wins` | Float64 | Number of wins accumulated by the higher-seeded team in the current playoff series as of this scheduled game. |
| `series_status_bottom_seed_wins` | Float64 | Number of wins accumulated by the lower-seeded team in the current playoff series as of this scheduled game. |
| `series_status_game_number_of_series` | Float64 | Sequential game number within the playoff series (e.g., 1 through 7 for a best-of-seven). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_club_schedule_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_club_schedule_season-example}

```python
nhl_club_schedule_season(team='TOR')
```

## nhl_club_schedule_month

Pull a team's schedule for one month.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/month/{month}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/month/now](https://api-web.nhle.com/v1/club-schedule/TOR/month/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `month` | `month` |  |  | `Y` | month path parameter. |

### Returns {#nhl_club_schedule_month-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_club_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_club_schedule_month-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_club_schedule_month-example}

```python
nhl_club_schedule_month(team='TOR')
```

## nhl_club_schedule_week

Pull a team's schedule for one week.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-schedule/{team}/week/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/club-schedule/TOR/week/now](https://api-web.nhle.com/v1/club-schedule/TOR/week/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns {#nhl_club_schedule_week-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_club_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_club_schedule_week-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_club_schedule_week-example}

```python
nhl_club_schedule_week(team='TOR')
```

## nhl_club_stats

Pull a team's season stat block.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats/{team}/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats/TOR/now](https://api-web.nhle.com/v1/club-stats/TOR/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_club_stats-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**skaters**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `headshot` | String | URL to the player headshot image. |
| `position_code` | String | Player position code. |
| `games_played` | Int64 | Games played. |
| `goals` | Int64 | Goals scored. |
| `assists` | Int64 | Assists. |
| `points` | Int64 | Total points (goals + assists). |
| `plus_minus` | Int64 | Plus/minus rating. |
| `penalty_minutes` | Int64 | Penalty minutes. |
| `power_play_goals` | Int64 | Power-play goals. |
| `shorthanded_goals` | Int64 | Shorthanded goals. |
| `game_winning_goals` | Int64 | Game-winning goals. |
| `overtime_goals` | Int64 | Overtime goals. |
| `shots` | Int64 | Shots on goal. |
| `shooting_pctg` | Float64 | Shooting percentage from the area. |
| `avg_time_on_ice_per_game` | Float64 | Average time on ice per game. |
| `avg_shifts_per_game` | Float64 | Average shifts per game. |
| `faceoff_win_pctg` | Float64 | Faceoff win percentage. |
| `first_name_default` | String | Player first name (default language). |
| `last_name_default` | String | Player last name (default language). |

**goalies**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `headshot` | String | URL to the player headshot image. |
| `games_played` | Int64 | Games played. |
| `games_started` | Int64 | Games started (goalies). |
| `wins` | Int64 | Wins. |
| `losses` | Int64 | Losses. |
| `overtime_losses` | Int64 | Total overtime losses. |
| `goals_against_average` | Float64 | Goals against average. |
| `save_percentage` | Float64 | Save percentage (goalies). |
| `shots_against` | Int64 | Shots faced. |
| `saves` | Int64 | Saves made. |
| `goals_against` | Int64 | Goals against. |
| `shutouts` | Int64 | Shutouts recorded. |
| `goals` | Int64 | Goals scored. |
| `assists` | Int64 | Assists. |
| `points` | Int64 | Total points (goals + assists). |
| `penalty_minutes` | Int64 | Penalty minutes. |
| `time_on_ice` | Int64 | Time on ice in seconds. |
| `first_name_default` | String | Player first name (default language). |
| `last_name_default` | String | Player last name (default language). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_club_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_club_stats-example}

```python
nhl_club_stats(team='TOR')
```

## nhl_club_stats_season

Pull the seasons a team has stats for.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/club-stats-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/club-stats-season/TOR](https://api-web.nhle.com/v1/club-stats-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |

### Returns {#nhl_club_stats_season-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s (parser: `parse_nhl_web_club_stats`); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_club_stats_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_club_stats_season-example}

```python
nhl_club_stats_season(team='TOR')
```
