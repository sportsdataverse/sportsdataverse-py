---
title: "MLB — MLB Stats API — Schedule"
sidebar_label: "Schedule"
sidebar_position: 6
description: "MLB — MLB Stats API — Schedule — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — Schedule

## mlb_schedule_postseason

GET /api/v1/schedule/postseason — postseason-only schedule for a season.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason?sportId=1](https://statsapi.mlb.com/api/v1/schedule/postseason?sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns {#mlb_schedule_postseason-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_date` | String | The calendar date grouping postseason games in this response row, as returned by the MLB Stats API schedule endpoint. |
| `game_pk` | Int64 | Unique game identifier. |
| `game_guid` | String | Globally unique game identifier (GUID). |
| `link` | String | API link to the game feed. |
| `game_type` | String | Game type code (R, P, etc.). |
| `season` | String | Season year. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `official_date` | String | Official game date (YYYY-MM-DD). |
| `is_tie` | Boolean | Whether the game ended in a tie. |
| `is_featured_game` | Boolean | Whether the game is a featured game. |
| `game_number` | Int64 | Game number within a doubleheader. |
| `public_facing` | Boolean | Whether the game is public-facing. |
| `double_header` | String | Doubleheader indicator ('N', 'S', 'Y'). |
| `gameday_type` | String | Gameday data feed type. |
| `tiebreaker` | String | Whether the game is a tiebreaker. |
| `calendar_event_id` | String | Calendar event identifier. |
| `season_display` | String | Display string for the season. |
| `day_night` | String | Day or night game indicator. |
| `description` | String | Long-form description text. |
| `scheduled_innings` | Int64 | Scheduled number of innings. |
| `reverse_home_away_status` | Boolean | Whether home/away teams are reversed. |
| `inning_break_length` | Int64 | Length of inning breaks in seconds. |
| `games_in_series` | Int64 | Number of games in the series. |
| `series_game_number` | Int64 | Game number within the series. |
| `series_description` | String | Description of the series. |
| `record_source` | String | Source of the schedule record. |
| `if_necessary` | String | Whether the game is played only if necessary. |
| `if_necessary_description` | String | Description of the if-necessary status. |
| `status_abstract_game_state` | String | Abstract game state (e.g. 'Final'). |
| `status_coded_game_state` | String | Coded game state. |
| `status_detailed_state` | String | Detailed game state. |
| `status_status_code` | String | Status code for the game. |
| `status_start_time_tbd` | Boolean | Whether the start time is TBD. |
| `status_abstract_game_code` | String | Abstract game state code. |
| `teams_away_team_id` | Int64 | Away team MLBAM ID. |
| `teams_away_team_name` | String | Away team name. |
| `teams_away_team_link` | String | API link to the away team. |
| `teams_away_league_record_wins` | Int64 | Away team league-record wins. |
| `teams_away_league_record_losses` | Int64 | Away team league-record losses. |
| `teams_away_league_record_ties` | Int64 | Away team league-record ties. |
| `teams_away_league_record_pct` | String | Away team winning percentage. |
| `teams_away_score` | Int64 | Away team score. |
| `teams_away_is_winner` | Boolean | Whether the away team won. |
| `teams_away_split_squad` | Boolean | Whether the away team is a split squad. |
| `teams_away_series_number` | Int64 | Away team's series number. |
| `teams_home_team_id` | Int64 | Home team MLBAM ID. |
| `teams_home_team_name` | String | Home team name. |
| `teams_home_team_link` | String | API link to the home team. |
| `teams_home_league_record_wins` | Int64 | Home team league-record wins. |
| `teams_home_league_record_losses` | Int64 | Home team league-record losses. |
| `teams_home_league_record_ties` | Int64 | Home team league-record ties. |
| `teams_home_league_record_pct` | String | Home team winning percentage. |
| `teams_home_score` | Int64 | Home team score. |
| `teams_home_is_winner` | Boolean | Whether the home team won. |
| `teams_home_split_squad` | Boolean | Whether the home team is a split squad. |
| `teams_home_series_number` | Int64 | Home team's series number. |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_name` | String | Venue name. |
| `venue_link` | String | API link to the venue. |
| `content_link` | String | API link to the game content. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_schedule_postseason-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_schedule_postseason-example}

```python
mlb_schedule_postseason()
```

## mlb_schedule_tied

View tied game schedule info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/games/tied`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/games/tied?season=2016](https://statsapi.mlb.com/api/v1/schedule/games/tied?season=2016)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameTypes` | `game_types` |  |  | `Y` | gameTypes query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_schedule_tied-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_date` | String | The calendar date grouping tied (suspended and resumed) games in this response row, as returned by the MLB Stats API schedule endpoint. |
| `game_pk` | Int64 | Unique game identifier. |
| `game_guid` | String | Globally unique game identifier (GUID). |
| `link` | String | API link to the game feed. |
| `game_type` | String | Game type code (R, P, etc.). |
| `season` | String | Season year. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `official_date` | String | Official game date (YYYY-MM-DD). |
| `is_tie` | Boolean | Whether the game ended in a tie. |
| `game_number` | Int64 | Game number within a doubleheader. |
| `public_facing` | Boolean | Whether the game is public-facing. |
| `double_header` | String | Doubleheader indicator ('N', 'S', 'Y'). |
| `gameday_type` | String | Gameday data feed type. |
| `tiebreaker` | String | Whether the game is a tiebreaker. |
| `calendar_event_id` | String | Calendar event identifier. |
| `season_display` | String | Display string for the season. |
| `day_night` | String | Day or night game indicator. |
| `scheduled_innings` | Int64 | Scheduled number of innings. |
| `reverse_home_away_status` | Boolean | Whether home/away teams are reversed. |
| `inning_break_length` | Int64 | Length of inning breaks in seconds. |
| `games_in_series` | Int64 | Number of games in the series. |
| `series_game_number` | Int64 | Game number within the series. |
| `series_description` | String | Description of the series. |
| `record_source` | String | Source of the schedule record. |
| `if_necessary` | String | Whether the game is played only if necessary. |
| `if_necessary_description` | String | Description of the if-necessary status. |
| `status_abstract_game_state` | String | Abstract game state (e.g. 'Final'). |
| `status_coded_game_state` | String | Coded game state. |
| `status_detailed_state` | String | Detailed game state. |
| `status_status_code` | String | Status code for the game. |
| `status_start_time_tbd` | Boolean | Whether the start time is TBD. |
| `status_reason` | String | Reason for the game status (e.g. 'Rain'). |
| `status_abstract_game_code` | String | Abstract game state code. |
| `teams_away_team_id` | Int64 | Away team MLBAM ID. |
| `teams_away_team_name` | String | Away team name. |
| `teams_away_team_link` | String | API link to the away team. |
| `teams_away_league_record_wins` | Int64 | Away team league-record wins. |
| `teams_away_league_record_losses` | Int64 | Away team league-record losses. |
| `teams_away_league_record_ties` | Int64 | Away team league-record ties. |
| `teams_away_league_record_pct` | String | Away team winning percentage. |
| `teams_away_score` | Int64 | Away team score. |
| `teams_away_split_squad` | Boolean | Whether the away team is a split squad. |
| `teams_away_series_number` | Int64 | Away team's series number. |
| `teams_home_team_id` | Int64 | Home team MLBAM ID. |
| `teams_home_team_name` | String | Home team name. |
| `teams_home_team_link` | String | API link to the home team. |
| `teams_home_league_record_wins` | Int64 | Home team league-record wins. |
| `teams_home_league_record_losses` | Int64 | Home team league-record losses. |
| `teams_home_league_record_ties` | Int64 | Home team league-record ties. |
| `teams_home_league_record_pct` | String | Home team winning percentage. |
| `teams_home_score` | Int64 | Home team score. |
| `teams_home_split_squad` | Boolean | Whether the home team is a split squad. |
| `teams_home_series_number` | Int64 | Home team's series number. |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_name` | String | Venue name. |
| `venue_link` | String | API link to the venue. |
| `content_link` | String | API link to the game content. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_schedule_tied-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_schedule_tied-example}

```python
mlb_schedule_tied(season='2016')
```

## mlb_schedule_postseason_series

View schedule info for postseason based on series.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason/series`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason/series?season=2023](https://statsapi.mlb.com/api/v1/schedule/postseason/series?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameTypes` | `game_types` |  |  | `Y` | gameTypes query parameter. |
| `seriesNumber` | `series_number` |  |  | `Y` | seriesNumber query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_schedule_postseason_series-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `total_items` | Int64 | Total schedule items on the date. |
| `total_games` | Int64 | Total games on the date. |
| `total_games_in_progress` | Int64 | Games currently in progress on the date. |
| `games` | String |  |
| `sort_order` | Int64 | Display sort order for the sport. |
| `series_id` | String | Series identifier (e.g. 'W_1'). |
| `series_sort_number` | Int64 | Sort number for the series. |
| `series_is_default` | Boolean | Whether the series is the default series. |
| `series_game_type` | String | Game type code for the series. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_schedule_postseason_series-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_schedule_postseason_series-example}

```python
mlb_schedule_postseason_series(season='2023')
```

## mlb_schedule_postseason_tunein

View schedule info for the tuneIn application.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn`

**Valid URL:** [https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn?season=2023](https://statsapi.mlb.com/api/v1/schedule/postseason/tuneIn?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_schedule_postseason_tunein-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_schedule_postseason_tunein-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_schedule_postseason_tunein-example}

```python
mlb_schedule_postseason_tunein(season='2023')
```
