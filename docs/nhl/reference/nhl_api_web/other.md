# NHL — NHL Web API — Other

> NHL — NHL Web API — Other — function reference in sdv-py, the SportsDataverse Python package.

## nhl_boxscore

Pull the boxscore for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore](https://api-web.nhle.com/v1/gamecenter/2024020001/boxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns {#nhl_boxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `home_away` | String | Home or away indicator. |
| `position_group` | String | Position group name (e.g. Centers). |
| `player_id` | Int64 | Unique player identifier. |
| `sweater_number` | Int64 | Jersey number. |
| `position` | String | Player position. |
| `goals` | Float64 | Goals scored. |
| `assists` | Float64 | Assists. |
| `points` | Float64 | Total points (goals + assists). |
| `plus_minus` | Float64 | Plus/minus rating. |
| `pim` | Int64 | Penalty minutes. |
| `hits` | Float64 | Hits. |
| `power_play_goals` | Float64 | Power-play goals. |
| `sog` | Float64 | Shots on goal from the area. |
| `faceoff_winning_pctg` | Float64 | Faceoff win percentage. |
| `toi` | String | Time on ice. |
| `blocked_shots` | Float64 | Blocked shots. |
| `shifts` | Float64 | Number of shifts. |
| `giveaways` | Float64 | Giveaways. |
| `takeaways` | Float64 | Takeaways. |
| `name_default` | String | Player name (default localization). |
| `even_strength_shots_against` | String | Even-strength shots against (saves/total). |
| `power_play_shots_against` | String | Power-play shots against (saves/total). |
| `shorthanded_shots_against` | String | Shorthanded shots against (saves/total). |
| `save_shots_against` | String | Total shots against (saves/total). |
| `even_strength_goals_against` | Float64 | Even-strength goals against. |
| `power_play_goals_against` | Float64 | Power-play goals against. |
| `shorthanded_goals_against` | Float64 | Shorthanded goals against. |
| `goals_against` | Float64 | Goals against. |
| `starter` | Boolean | Whether the goalie started the game. |
| `shots_against` | Float64 | Shots faced. |
| `saves` | Float64 | Saves made. |
| `save_pctg` | Float64 | Save percentage. |
| `decision` | String | Goalie decision (W/L/O). |
| `name_cs` | String | Player name (Czech localization). |
| `name_fi` | String | Player name (Finnish localization). |
| `name_sk` | String | Player name (Slovak localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_boxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_boxscore-example}

```python
nhl_boxscore(game_id=2024020001)
```

## nhl_landing

Pull the gamecenter landing payload for one NHL game.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/landing`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/landing](https://api-web.nhle.com/v1/gamecenter/2024020001/landing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns {#nhl_landing-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `season` | Int64 | Season year (echoed from arg). |
| `game_type` | Int64 | Game type the row belongs to. |
| `limited_scoring` | Boolean | Boolean flag indicating whether the game is subject to a limited-scoring designation (e.g., a low-scoring or shootout-resolved game) per NHL api-web metadata. |
| `game_date` | String | Game date. |
| `start_time_utc` | String | Scheduled start time in UTC. |
| `eastern_utc_offset` | String | Eastern time UTC offset. |
| `venue_utc_offset` | String | Venue UTC offset. |
| `venue_timezone` | String | Venue time zone. |
| `tv_broadcasts` | String | Nested list of TV broadcast details. |
| `game_state` | String | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | String | Schedule state of the game. |
| `shootout_in_use` | Boolean | Boolean flag indicating whether a shootout is in use as the tiebreaker format for this game per NHL api-web game landing metadata. |
| `reg_periods` | Int64 | Number of regulation periods scheduled for this game (typically 3 for NHL, may differ for special-format games). |
| `ot_in_use` | Boolean | Boolean flag indicating whether overtime rules are in effect for this game, as determined by the NHL api-web game landing endpoint. |
| `ties_in_use` | Boolean | Whether ties were in use that season. |
| `venue_default` | String | Venue name (default language). |
| `venue_location_default` | String | Default-language display string for the city or location associated with the game's venue, as provided by the NHL api-web landing endpoint. |
| `period_descriptor_number` | Int64 | Period number. |
| `period_descriptor_period_type` | String | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods. |
| `away_team_id` | Int64 | Away team identifier. |
| `away_team_common_name_default` | String | Away team common name (default language). |
| `away_team_abbrev` | String | Away team abbreviation. |
| `away_team_place_name_default` | String | Away team place name (default language). |
| `away_team_place_name_with_preposition_default` | String | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | String | Away team place name with preposition (French). |
| `away_team_score` | Int64 | Away team final score. |
| `away_team_sog` | Int64 | Away team shots on goal. |
| `away_team_logo` | String | URL to the away team logo. |
| `away_team_dark_logo` | String | URL to the away team dark logo. |
| `home_team_id` | Int64 | Home team identifier. |
| `home_team_common_name_default` | String | Home team common name (default language). |
| `home_team_abbrev` | String | Home team abbreviation. |
| `home_team_place_name_default` | String | Home team place name (default language). |
| `home_team_place_name_fr` | String | Home team place name (French). |
| `home_team_place_name_with_preposition_default` | String | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | String | Home team place name with preposition (French). |
| `home_team_score` | Int64 | Home team final score. |
| `home_team_sog` | Int64 | Home team shots on goal. |
| `home_team_logo` | String | URL to the home team logo. |
| `home_team_dark_logo` | String | URL to the home team dark logo. |
| `summary_scoring` | String | Serialized summary of scoring events for the game, flattened from the nested NHL api-web landing payload scoring sub-object. |
| `summary_three_stars` | String | Serialized representation of the three-star selections for the game, flattened from the nested NHL api-web landing payload. |
| `summary_penalties` | String | Serialized summary of penalty events for the game, flattened from the nested NHL api-web landing payload penalties sub-object. |
| `clock_time_remaining` | String | Remaining time in the current period formatted as MM:SS, as provided by the NHL api-web game landing endpoint. |
| `clock_seconds_remaining` | Int64 | Integer count of seconds remaining in the current period at the time the NHL api-web landing payload was captured. |
| `clock_running` | Boolean | Boolean flag indicating whether the game clock is actively counting down at the time the NHL api-web landing payload was captured. |
| `clock_in_intermission` | Boolean | Boolean flag indicating whether the game clock is currently paused during an intermission period between regulation periods. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_landing-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_landing-example}

```python
nhl_landing(game_id=2024020001)
```

## nhl_right_rail

Pull the gamecenter right-rail payload (in-game widgets).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/gamecenter/{game_id}/right-rail`

**Valid URL:** [https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail](https://api-web.nhle.com/v1/gamecenter/2024020001/right-rail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | game_id path parameter. |

### Returns {#nhl_right_rail-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**season_series**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `season` | Int64 | Season year (echoed from arg). |
| `game_type` | Int64 | Game type the row belongs to. |
| `game_date` | String | Game date. |
| `start_time_utc` | String | Scheduled start time in UTC. |
| `eastern_utc_offset` | String | Eastern time UTC offset. |
| `venue_utc_offset` | String | Venue UTC offset. |
| `game_state` | String | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | String | Schedule state of the game. |
| `game_center_link` | String | Link to the NHL game center page. |
| `away_team_id` | Int64 | Away team identifier. |
| `away_team_abbrev` | String | Away team abbreviation. |
| `away_team_logo` | String | URL to the away team logo. |
| `away_team_score` | Int64 | Away team final score. |
| `home_team_id` | Int64 | Home team identifier. |
| `home_team_abbrev` | String | Home team abbreviation. |
| `home_team_logo` | String | URL to the home team logo. |
| `home_team_score` | Int64 | Home team final score. |
| `period_descriptor_number` | Int64 | Period number. |
| `period_descriptor_period_type` | String | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | String | Period type in which the game ended. |

**shots_by_period**

| col_name | type | description |
|---|---|---|
| `away` | Int64 | Away team shots in the period. |
| `home` | Int64 | Whether the player's team was home. |
| `period_descriptor_number` | Int64 | Period number. |
| `period_descriptor_period_type` | String | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods. |

**team_game_stats**

| col_name | type | description |
|---|---|---|
| `category` | String | Stat leader category. |
| `away_value` | String |  |
| `home_value` | String |  |

**game_info**

| col_name | type | description |
|---|---|---|
| `referees` | String |  |
| `linesmen` | String |  |
| `away_team_head_coach_default` | String |  |
| `away_team_scratches` | String |  |
| `home_team_head_coach_default` | String |  |
| `home_team_scratches` | String |  |

**linescore_by_period**

| col_name | type | description |
|---|---|---|
| `away` | Int64 | Away team shots in the period. |
| `home` | Int64 | Whether the player's team was home. |
| `period_descriptor_number` | Int64 | Period number. |
| `period_descriptor_period_type` | String | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods. |

**season_series_wins**

| col_name | type | description |
|---|---|---|
| `away_team_wins` | Int64 |  |
| `home_team_wins` | Int64 |  |
| `needed_to_win` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_right_rail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_right_rail-example}

```python
nhl_right_rail(game_id=2024020001)
```

## nhl_score

Pull the single-day scoreboard for `date`.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/score/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/score/now](https://api-web.nhle.com/v1/score/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns {#nhl_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `season` | Int64 | Season year (echoed from arg). |
| `game_type` | Int64 | Game type the row belongs to. |
| `game_date` | String | Game date. |
| `start_time_utc` | String | Scheduled start time in UTC. |
| `eastern_utc_offset` | String | Eastern time UTC offset. |
| `venue_utc_offset` | String | Venue UTC offset. |
| `tv_broadcasts` | String | Nested list of TV broadcast details. |
| `game_state` | String | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | String | Schedule state of the game. |
| `game_center_link` | String | Link to the NHL game center page. |
| `series_url` | String | URL path to the NHL api-web series landing page providing full details about this playoff series matchup. |
| `three_min_recap` | String | Link to the three-minute recap. |
| `neutral_site` | Boolean | Whether the game is at a neutral site. |
| `venue_timezone` | String | Venue time zone. |
| `period` | Int64 | Period number. |
| `goals` | String | Goals scored. |
| `venue_default` | String | Venue name (default language). |
| `away_team_id` | Int64 | Away team identifier. |
| `away_team_name_default` | String | Default-language full team name for the away team in this game, as provided by the NHL api-web scoreboard endpoint. |
| `away_team_abbrev` | String | Away team abbreviation. |
| `away_team_score` | Int64 | Away team final score. |
| `away_team_sog` | Int64 | Away team shots on goal. |
| `away_team_logo` | String | URL to the away team logo. |
| `home_team_id` | Int64 | Home team identifier. |
| `home_team_name_default` | String | Default-language full team name for the home team in this game, as provided by the NHL api-web scoreboard endpoint. |
| `home_team_abbrev` | String | Home team abbreviation. |
| `home_team_score` | Int64 | Home team final score. |
| `home_team_sog` | Int64 | Home team shots on goal. |
| `home_team_logo` | String | URL to the home team logo. |
| `series_status_round` | Int64 | Numeric identifier for the playoff round (e.g., 1 = First Round, 2 = Second Round) in which this game is being played. |
| `series_status_series_abbrev` | String | Short abbreviation code identifying the specific playoff series matchup, as provided by the NHL api-web score endpoint. |
| `series_status_series_title` | String | Human-readable title describing the playoff series matchup (e.g., team abbreviations and round name), as returned by the NHL api-web score endpoint. |
| `series_status_series_letter` | String | Single-letter label (e.g., 'A', 'B') assigned to the playoff series by the NHL api-web score endpoint to distinguish simultaneous matchups within a round. |
| `series_status_needed_to_win` | Int64 | Number of additional wins required by the series leader to clinch the playoff round at the time of this game. |
| `series_status_top_seed_team_abbrev` | String | Three-letter abbreviation for the higher-seeded team in this playoff series, as reported by the NHL api-web score endpoint. |
| `series_status_top_seed_wins` | Int64 | Number of wins accumulated by the higher-seeded team in the current playoff series as of this game. |
| `series_status_bottom_seed_team_abbrev` | String | Three-letter abbreviation for the lower-seeded team in this playoff series, as reported by the NHL api-web score endpoint. |
| `series_status_bottom_seed_wins` | Int64 | Number of wins accumulated by the lower-seeded team in the current playoff series as of this game. |
| `series_status_game_number_of_series` | Int64 | Sequential game number within the playoff series (e.g., 1 through 7 for a best-of-seven round). |
| `clock_time_remaining` | String | Remaining time in the current period in MM:SS format, as provided by the NHL api-web score endpoint. |
| `clock_seconds_remaining` | Int64 | Integer count of seconds remaining in the current period as reported by the NHL api-web score endpoint. |
| `clock_running` | Boolean | Boolean flag indicating whether the game clock is actively running at the time the NHL api-web score payload was captured. |
| `clock_in_intermission` | Boolean | Boolean flag indicating whether the game clock is paused during an intermission at the time the NHL api-web score payload was captured. |
| `period_descriptor_number` | Int64 | Period number. |
| `period_descriptor_period_type` | String | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods. |
| `game_outcome_last_period_type` | String | Period type in which the game ended. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_score-example}

```python
nhl_score()
```

## nhl_schedule_calendar

Pull the calendar of game-days for the season.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule-calendar/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule-calendar/now](https://api-web.nhle.com/v1/schedule-calendar/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns {#nhl_schedule_calendar-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_schedule_calendar-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_schedule_calendar-example}

```python
nhl_schedule_calendar()
```

## nhl_playoff_series

Pull a single playoff series payload.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/schedule/playoff-series/{season}/{series_letter}`

**Valid URL:** [https://api-web.nhle.com/v1/schedule/playoff-series/20242025/a](https://api-web.nhle.com/v1/schedule/playoff-series/20242025/a)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | season path parameter. |
| `series_letter` | `series_letter` |  | `Y` |  | series_letter path parameter. |

### Returns {#nhl_playoff_series-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `round` | Int64 | Shootout round number. |
| `series_letter` | String | Single-letter label identifying this series within its playoff round (e.g., 'A', 'B'), as used by the NHL api-web. |
| `top_seed_team_id` | Int64 | Unique NHL api-web identifier for the higher-seeded team in this playoff series. |
| `top_seed_team_abbrev` | String | Three-letter abbreviation of the higher-seeded team in this NHL playoff series (e.g., 'TOR'). |
| `bottom_seed_team_id` | Int64 | Unique NHL api-web identifier for the lower-seeded team in this playoff series. |
| `bottom_seed_team_abbrev` | String | Three-letter abbreviation of the lower-seeded team in this NHL playoff series (e.g., 'BOS'). |
| `id` | Int64 | Unique player identifier. |
| `season` | Int64 | Season year (echoed from arg). |
| `game_type` | Int64 | Game type the row belongs to. |
| `game_number` | Int64 | Game number within the schedule. |
| `if_necessary` | Boolean |  |
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
| `away_team_score` | Int64 | Away team final score. |
| `home_team_id` | Int64 | Home team identifier. |
| `home_team_common_name_default` | String | Home team common name (default language). |
| `home_team_place_name_default` | String | Home team place name (default language). |
| `home_team_place_name_fr` | String | Home team place name (French). |
| `home_team_place_name_with_preposition_default` | String | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | String | Home team place name with preposition (French). |
| `home_team_abbrev` | String | Home team abbreviation. |
| `home_team_score` | Int64 | Home team final score. |
| `period_descriptor_number` | Int64 | Period number. |
| `period_descriptor_period_type` | String | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | Int64 | Maximum number of regulation periods. |
| `series_status_top_seed_wins` | Int64 | Number of wins accumulated by the higher-seeded team in this NHL playoff series to date. |
| `series_status_bottom_seed_wins` | Int64 | Number of wins accumulated by the lower-seeded team in this NHL playoff series to date. |
| `game_outcome_last_period_type` | String | Period type in which the game ended. |
| `game_outcome_ot_periods` | Float64 | Number of overtime periods played in the game that concluded this series or scheduled game, where applicable. |
| `away_team_place_name_fr` | String | Away team place name (French). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_playoff_series-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_playoff_series-example}

```python
nhl_playoff_series(season=2025, series_letter='a')
```

## nhl_standings

Pull the NHL standings.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings/{date}`

**Valid URL:** [https://api-web.nhle.com/v1/standings/now](https://api-web.nhle.com/v1/standings/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date path parameter. |

### Returns {#nhl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `clinch_indicator` | String |  |
| `conference_abbrev` | String | Conference abbreviation. |
| `conference_home_sequence` | Int64 | Team's rank within its conference based solely on home-game results in the NHL api-web standings. |
| `conference_l10_sequence` | Int64 | Team's rank within its conference based on performance in the last 10 games played, as reported by the NHL api-web standings endpoint. |
| `conference_name` | String | Conference name. |
| `conference_road_sequence` | Int64 | Team's rank within its conference based solely on road-game results in the NHL api-web standings. |
| `conference_sequence` | Int64 | Team's seeding position within the conference. |
| `date` | String | Game date (ISO 8601 datetime string). |
| `division_abbrev` | String | Division abbreviation. |
| `division_home_sequence` | Int64 | Team's rank within its division based solely on home-game results in the NHL api-web standings. |
| `division_l10_sequence` | Int64 | Team's rank within its division based on performance in the last 10 games played, as reported by the NHL api-web standings endpoint. |
| `division_name` | String | Division name. |
| `division_road_sequence` | Int64 | Team's rank within its division based solely on road-game results in the NHL api-web standings. |
| `division_sequence` | Int64 | Team's seeding position within the division. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games_played` | Int64 | Matches played. |
| `goal_differential` | Int64 | Goal differential. |
| `goal_differential_pctg` | Float64 | Team's goal differential normalized to a per-game (or percentage) basis, as published in the NHL standings feed. |
| `goal_against` | Int64 | Total number of goals allowed by the team across all games played in the current standings snapshot. |
| `goal_for` | Int64 | Total number of goals scored by the team across all games played in the current standings snapshot. |
| `goals_for_pctg` | Float64 | Team's share of total goals scored in all games involving this team, calculated as goals-for divided by (goals-for + goals-against). |
| `home_games_played` | Int64 | Number of home games the team has completed in the current season as of this standings snapshot. |
| `home_goal_differential` | Int64 | Net goal differential (goals-for minus goals-against) accumulated across all home games played in the current season. |
| `home_goals_against` | Int64 | Total number of goals allowed by the team in home games during the current season. |
| `home_goals_for` | Int64 | Total number of goals scored by the team in home games during the current season. |
| `home_losses` | Int64 | Losses at home. |
| `home_ot_losses` | Int64 | Home overtime losses. |
| `home_points` | Int64 |  |
| `home_regulation_plus_ot_wins` | Int64 | Number of home wins achieved in regulation or overtime (excluding shootout decisions) in the current season. |
| `home_regulation_wins` | Int64 | Number of home wins achieved in regulation time (within 60 minutes) in the current season. |
| `home_ties` | Int64 | Ties at home. |
| `home_wins` | Int64 | Wins at home. |
| `l10_games_played` | Int64 | Number of games included in the team's last-10-games performance window (typically 10, may be lower early in the season). |
| `l10_goal_differential` | Int64 | Net goal differential (goals-for minus goals-against) across the team's most recent 10 games. |
| `l10_goals_against` | Int64 | Total goals allowed by the team across its most recent 10 games. |
| `l10_goals_for` | Int64 | Total goals scored by the team across its most recent 10 games. |
| `l10_losses` | Int64 | Losses in the last ten games. |
| `l10_ot_losses` | Int64 | Overtime losses in the last ten games. |
| `l10_points` | Int64 | Total standings points earned by the team across its most recent 10 games. |
| `l10_regulation_plus_ot_wins` | Int64 | Number of wins in regulation or overtime (excluding shootouts) within the team's most recent 10 games. |
| `l10_regulation_wins` | Int64 | Number of regulation-time wins within the team's most recent 10 games. |
| `l10_ties` | Int64 | Number of tied results recorded within the team's most recent 10 games (applicable to seasons using tie rules). |
| `l10_wins` | Int64 | Wins in the last ten games. |
| `league_home_sequence` | Int64 | Team's rank league-wide based solely on home-game results in the NHL api-web standings. |
| `league_l10_sequence` | Int64 | Team's rank league-wide based on performance in the last 10 games played, as reported by the NHL api-web standings endpoint. |
| `league_road_sequence` | Int64 | Team's rank league-wide based solely on road-game results in the NHL api-web standings. |
| `league_sequence` | Int64 | Team's seeding position within the league. |
| `losses` | Int64 | Number of matches the team has lost. |
| `ot_losses` | Int64 | Overtime losses. |
| `point_pctg` | Float64 | Points percentage. |
| `points` | Int64 | Competition points. |
| `regulation_plus_ot_win_pctg` | Float64 | Fraction of games won in regulation or overtime (excluding shootout decisions), used as a tiebreaker metric in NHL standings. |
| `regulation_plus_ot_wins` | Int64 | Wins in regulation plus overtime. |
| `regulation_win_pctg` | Float64 | Fraction of games won in regulation time only, used as a secondary tiebreaker in NHL standings. |
| `regulation_wins` | Int64 | Wins in regulation. |
| `road_games_played` | Int64 | Number of road games the team has completed in the current season as of this standings snapshot. |
| `road_goal_differential` | Int64 | Net goal differential (goals-for minus goals-against) accumulated across all road games played in the current season. |
| `road_goals_against` | Int64 | Total number of goals allowed by the team in road games during the current season. |
| `road_goals_for` | Int64 | Total number of goals scored by the team in road games during the current season. |
| `road_losses` | Int64 | Losses on the road. |
| `road_ot_losses` | Int64 | Road overtime losses. |
| `road_points` | Int64 | Total standings points earned by the team in road games during the current season. |
| `road_regulation_plus_ot_wins` | Int64 | Number of road wins achieved in regulation or overtime (excluding shootout decisions) in the current season. |
| `road_regulation_wins` | Int64 | Number of road wins achieved in regulation time (within 60 minutes) in the current season. |
| `road_ties` | Int64 | Ties on the road. |
| `road_wins` | Int64 | Wins on the road. |
| `season_id` | Int64 | Season identifier. |
| `shootout_losses` | Int64 | Shootout losses. |
| `shootout_wins` | Int64 | Shootout wins. |
| `streak_code` | String | Current streak code (W/L/OT). |
| `streak_count` | Int64 | Length of the current streak. |
| `team_logo` | String | URL to the team logo image. |
| `ties` | Int64 | Number of matches the team has drawn. |
| `waivers_sequence` | Int64 | Team's position in the NHL waiver-claim priority order for the current season, determined by reverse standings order. |
| `wildcard_sequence` | Int64 | Team's wild card seeding position. |
| `win_pctg` | Float64 | Team's overall win percentage (wins divided by games played), as reported by the NHL api-web standings endpoint. |
| `wins` | Int64 | Number of matches the team has won. |
| `place_name_default` | String | Default-language city or place name associated with the team's market (e.g., "Toronto"), as returned by the NHL api-web standings endpoint. |
| `team_name_default` | String | Team name (default locale). |
| `team_name_fr` | String | Team name (French locale). |
| `team_common_name_default` | String | Team common name (default language). |
| `team_abbrev_default` | String | Default three-letter abbreviation for the team (e.g., "TOR"), as provided by the NHL api-web standings endpoint. |
| `place_name_fr` | String | French-language city or place name associated with the team's market, as returned by the NHL api-web standings endpoint. |
| `team_common_name_fr` | String | Team common name (French localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_standings-example}

```python
nhl_standings()
```

## nhl_standings_season

Pull the per-season standings cutover dates.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/standings-season`

**Valid URL:** [https://api-web.nhle.com/v1/standings-season](https://api-web.nhle.com/v1/standings-season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_standings_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `conferences_in_use` | Boolean | Whether conferences were in use that season. |
| `divisions_in_use` | Boolean | Whether divisions were in use that season. |
| `point_for_o_tloss_in_use` | Boolean | Whether a point for overtime losses was in use. |
| `regulation_wins_in_use` | Boolean | Whether regulation wins were tracked. |
| `row_in_use` | Boolean | Whether the regulation/overtime/shootout format was in use. |
| `standings_end` | String | End date of the standings period. |
| `standings_start` | String | Start date of the standings period. |
| `ties_in_use` | Boolean | Whether ties were in use that season. |
| `wildcard_in_use` | Boolean | Whether the wild-card playoff format was in use this season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_standings_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_standings_season-example}

```python
nhl_standings_season()
```

## nhl_roster

Pull a team's roster.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster/{team}/{season}`

**Valid URL:** [https://api-web.nhle.com/v1/roster/TOR/current](https://api-web.nhle.com/v1/roster/TOR/current)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |
| `season` | `season` |  |  | `Y` | season path parameter. |

### Returns {#nhl_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `position_group` | String | Position group name (e.g. Centers). |
| `id` | Int64 | Unique player identifier. |
| `headshot` | String | URL to the player headshot image. |
| `sweater_number` | Int64 | Jersey number. |
| `position_code` | String | Player position code. |
| `shoots_catches` | String | Handedness (shoots/catches). |
| `height_in_inches` | Int64 | Height in inches. |
| `weight_in_pounds` | Int64 | Weight in pounds. |
| `height_in_centimeters` | Int64 | Height in centimeters. |
| `weight_in_kilograms` | Int64 | Weight in kilograms. |
| `birth_date` | String | Player birth date. |
| `birth_country` | String | Player birth country. |
| `first_name_default` | String | Player first name (default language). |
| `last_name_default` | String | Player last name (default language). |
| `birth_city_default` | String | Birth city (default localization). |
| `birth_state_province_default` | String | Birth state/province (default localization). |
| `birth_city_cs` | String | Birth city (Czech localization). |
| `birth_city_de` | String | Birth city (German localization). |
| `birth_city_fi` | String | Birth city (Finnish localization). |
| `birth_city_sk` | String | Birth city (Slovak localization). |
| `birth_city_sv` | String | Birth city (Swedish localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_roster-example}

```python
nhl_roster(team='TOR')
```

## nhl_roster_season

Pull every season a team has had on file.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/roster-season/{team}`

**Valid URL:** [https://api-web.nhle.com/v1/roster-season/TOR](https://api-web.nhle.com/v1/roster-season/TOR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  | `Y` |  | team path parameter. |

### Returns {#nhl_roster_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_web_roster`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_roster_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_roster_season-example}

```python
nhl_roster_season(team='TOR')
```

## nhl_skater_leaders

Pull skater stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/skater-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/skater-stats-leaders/current](https://api-web.nhle.com/v1/skater-stats-leaders/current)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_skater_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | String | Stat leader category. |
| `id` | Int64 | Unique player identifier. |
| `sweater_number` | Int64 | Jersey number. |
| `headshot` | String | URL to the player headshot image. |
| `team_abbrev` | String | Team abbreviation. |
| `team_logo` | String | URL to the team logo image. |
| `position` | String | Player position. |
| `value` | Int64 | Leader stat numeric value. |
| `first_name_default` | String | Player first name (default language). |
| `first_name_cs` | String | Player first name (Czech localization). |
| `first_name_de` | String | Player first name (German). |
| `first_name_es` | String | Player first name (Spanish). |
| `first_name_fi` | String | Player first name (Finnish). |
| `first_name_sk` | String | Player first name (Slovak localization). |
| `first_name_sv` | String | Player first name (Swedish). |
| `last_name_default` | String | Player last name (default language). |
| `team_name_default` | String | Team name (default locale). |
| `last_name_cs` | String | Player last name (Czech localization). |
| `last_name_fi` | String | Player last name (Finnish localization). |
| `last_name_sk` | String | Player last name (Slovak localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_skater_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_skater_leaders-example}

```python
nhl_skater_leaders()
```

## nhl_goalie_leaders

Pull goalie stat leaders.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/goalie-stats-leaders/{season}/{game_type}`

**Valid URL:** [https://api-web.nhle.com/v1/goalie-stats-leaders/current](https://api-web.nhle.com/v1/goalie-stats-leaders/current)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `game_type` | `game_type` |  |  | `Y` | game_type path parameter. |

### Returns {#nhl_goalie_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | String | Stat leader category. |
| `id` | Int64 | Unique player identifier. |
| `sweater_number` | Int64 | Jersey number. |
| `headshot` | String | URL to the player headshot image. |
| `team_abbrev` | String | Team abbreviation. |
| `team_logo` | String | URL to the team logo image. |
| `position` | String | Player position. |
| `value` | Int64 | Leader stat numeric value. |
| `first_name_default` | String | Player first name (default language). |
| `last_name_default` | String | Player last name (default language). |
| `team_name_default` | String | Team name (default locale). |
| `first_name_cs` | String | Player first name (Czech localization). |
| `first_name_sk` | String | Player first name (Slovak localization). |
| `last_name_cs` | String | Player last name (Czech localization). |
| `last_name_sk` | String | Player last name (Slovak localization). |
| `last_name_fi` | String | Player last name (Finnish localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_goalie_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_goalie_leaders-example}

```python
nhl_goalie_leaders()
```
