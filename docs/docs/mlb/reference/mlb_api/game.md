---
title: "MLB — MLB Stats API — Game"
sidebar_label: "Game"
sidebar_position: 3
description: "MLB — MLB Stats API — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — Game

## mlb_game_context_metrics

GET /api/v1/game/{gamePk}/contextMetrics — WP, leverage index, in-game context.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/contextMetrics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/contextMetrics](https://statsapi.mlb.com/api/v1/game/716390/contextMetrics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_game_context_metrics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_game_context_metrics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_game_context_metrics-example}

```python
mlb_game_context_metrics(game_pk=716390)
```

## mlb_game_content

GET /api/v1/game/{gamePk}/content — articles, highlights, editorial content.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/content`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/content](https://statsapi.mlb.com/api/v1/game/716390/content)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |

### Returns {#mlb_game_content-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_game_content-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_game_content-example}

```python
mlb_game_content(game_pk=716390)
```

## mlb_game_timestamps

Retrieve all of the play timecodes for a game in GUMBO feed.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live/timestamps`

**Valid URL:** [https://statsapi.mlb.com/api/v1.1/game/716390/feed/live/timestamps](https://statsapi.mlb.com/api/v1.1/game/716390/feed/live/timestamps)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |

### Returns {#mlb_game_timestamps-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `timecode` | String | A timestamp string representing a specific point in time used to query the MLB Stats API for game state changes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_game_timestamps-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_game_timestamps-example}

```python
mlb_game_timestamps(game_pk=716390)
```

## mlb_game_changes

View corrected non Statcast information for games

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/changes`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/changes?updatedSince=2023-09-01T00%3A00%3A00Z&sportId=1](https://statsapi.mlb.com/api/v1/game/changes?updatedSince=2023-09-01T00%3A00%3A00Z&sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `updatedSince` | `updated_since` |  |  | `Y` | updatedSince query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_game_changes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `schedule_date` | String | The calendar date for which schedule changes are being reported, identifying when rescheduled or suspended games occurred. |
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
| `games_in_series` | Float64 | Number of games in the series. |
| `series_game_number` | Float64 | Game number within the series. |
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
| `teams_away_series_number` | Float64 | Away team's series number. |
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
| `teams_home_series_number` | Float64 | Home team's series number. |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_name` | String | Venue name. |
| `venue_link` | String | API link to the venue. |
| `content_link` | String | API link to the game content. |
| `rescheduled_from` | String | Original date-time the game was rescheduled from. |
| `rescheduled_from_date` | String | Original date the game was rescheduled from. |
| `description` | String | Long-form description text. |
| `status_reason` | String | Reason for the game status (e.g. 'Rain'). |
| `resumed_from` | String | Original date-time if the game was resumed. |
| `resumed_from_date` | String | Original date if the game was resumed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_game_changes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_game_changes-example}

```python
mlb_game_changes(sport_id=1, updated_since='2023-09-01T00:00:00Z')
```

## mlb_game_guids

View Statcast data for a specific game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/guids`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/guids](https://statsapi.mlb.com/api/v1/game/716390/guids)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `gameModeId` | `game_mode_id` |  |  | `Y` | gameModeId query parameter. |
| `updatedSince` | `updated_since` |  |  | `Y` | updatedSince query parameter. |
| `isPitch` | `is_pitch` |  |  | `Y` | isPitch query parameter. |
| `isHit` | `is_hit` |  |  | `Y` | isHit query parameter. |
| `isPickoff` | `is_pickoff` |  |  | `Y` | isPickoff query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `parsed/raw` | `parsed_raw` |  |  | `Y` | parsed/raw query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_game_guids-returns}

**`return_parsed=True`** (default) — the output of `parse_mlb_api_list`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 401 'Please Login' through the residential proxy; the route needs an MLB login session the package does not hold.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_game_guids-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_game_guids-example}

```python
mlb_game_guids(game_pk=716390)
```

## mlb_game_color

View game color commentary info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/color`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/feed/color](https://statsapi.mlb.com/api/v1/game/716390/feed/color)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_game_color-returns}

**`return_parsed=True`** (default) — the output of `parse_mlb_api_list`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 404 through the residential proxy with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_game_color-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_game_color-example}

```python
mlb_game_color(game_pk=716390)
```

## mlb_game_color_diff

View game color feed.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/color/diffPatch`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/feed/color/diffPatch](https://statsapi.mlb.com/api/v1/game/716390/feed/color/diffPatch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `startTimecode` | `start_timecode` |  |  | `Y` | startTimecode query parameter. |
| `endTimecode` | `end_timecode` |  |  | `Y` | endTimecode query parameter. |

### Returns {#mlb_game_color_diff-returns}

**`return_parsed=True`** (default) — the output of `parse_mlb_api_list`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 404 through the residential proxy with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_game_color_diff-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_game_color_diff-example}

```python
mlb_game_color_diff(game_pk=716390)
```

## mlb_game_color_timestamps

View all of the color timecodes for a game.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/color/timestamps`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/716390/feed/color/timestamps](https://statsapi.mlb.com/api/v1/game/716390/feed/color/timestamps)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |

### Returns {#mlb_game_color_timestamps-returns}

**`return_parsed=True`** (default) — the output of `parse_mlb_api_timecodes`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 404 through the residential proxy with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_game_color_timestamps-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_game_color_timestamps-example}

```python
mlb_game_color_timestamps(game_pk=716390)
```

## mlb_game_pace

View time of game info.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/gamePace`

**Valid URL:** [https://statsapi.mlb.com/api/v1/gamePace?season=2023](https://statsapi.mlb.com/api/v1/gamePace?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `leagueIds` | `league_ids` |  |  | `Y` | leagueIds query parameter. |
| `leagueListId` | `league_list_id` |  |  | `Y` | leagueListId query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `startDate` | `start_date` |  |  | `Y` | startDate query parameter. |
| `endDate` | `end_date` |  |  | `Y` | endDate query parameter. |
| `venueIds` | `venue_ids` |  |  | `Y` | venueIds query parameter. |
| `orgType` | `org_type` |  |  | `Y` | orgType query parameter. |
| `includeChildren` | `include_children` |  |  | `Y` | includeChildren query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_game_pace-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `hits_per9_inn` | Float64 | Average number of hits allowed per nine innings across all games in the sample period. |
| `runs_per9_inn` | Float64 | Average number of runs scored per nine innings across all games in the sample period. |
| `pitches_per9_inn` | Float64 | Average number of pitches thrown per nine innings across all games in the sample period. |
| `plate_appearances_per9_inn` | Float64 | Average number of plate appearances occurring per nine innings across all games in the sample period. |
| `hits_per_game` | Float64 | Hits per game. |
| `runs_per_game` | Float64 | Runs per game. |
| `innings_played_per_game` | Float64 | Innings played per game. |
| `pitches_per_game` | Float64 | Pitches per game. |
| `pitchers_per_game` | Float64 | Pitchers used per game. |
| `plate_appearances_per_game` | Float64 | Plate appearances per game. |
| `total_game_time` | String | Total game time (HHH:MM:SS). |
| `total_innings_played` | Float64 | Total innings played. |
| `total_hits` | Int64 | Total hits. |
| `total_runs` | Int64 | Total runs. |
| `total_plate_appearances` | Int64 | Total plate appearances. |
| `total_pitchers` | Int64 | Total pitchers used. |
| `total_pitches` | Int64 | Total pitches thrown. |
| `total_games` | Int64 | Total games on the date. |
| `total7_inn_games` | Int64 | Total number of seven-inning games played (including doubleheader games). |
| `total9_inn_games` | Float64 | Total number of nine-inning games played in the sample period. |
| `total_extra_inn_games` | Int64 | Total extra-inning games. |
| `time_per_game` | String | Average time per game (HH:MM:SS). |
| `time_per_pitch` | String | Average time per pitch (HH:MM:SS). |
| `time_per_hit` | String | Average time per hit (HH:MM:SS). |
| `time_per_run` | String | Average time per run (HH:MM:SS). |
| `time_per_plate_appearance` | String | Average time per plate appearance (HH:MM:SS). |
| `time_per9_inn` | String | Average elapsed clock time per nine-inning game formatted as hours and minutes. |
| `time_per77_plate_appearances` | String | Average time per 77 plate appearances, used as a normalized pace benchmark by MLB. |
| `total_extra_inn_time` | String | Total extra-inning time (HHH:MM:SS). |
| `time_per7_inn_game_without_extra_inn` | String | Average elapsed clock time per seven-inning game excluding games that went to extra innings. |
| `total9_inn_games_completed_early` | Int64 | Number of nine-inning games that were called or suspended before completing nine full innings. |
| `total9_inn_games_without_extra_inn` | Float64 | Number of nine-inning games completed without requiring extra innings. |
| `total9_inn_games_scheduled` | Int64 | Total number of nine-inning games that were scheduled in the sample period. |
| `hits_per_run` | Float64 | Hits per run. |
| `pitches_per_pitcher` | Float64 | Pitches per pitcher. |
| `season` | String | Season year. |
| `sport_id` | Int64 | Sport MLBAM ID. |
| `sport_code` | String | Short sport code (e.g. 'mlb', 'aaa'). |
| `sport_link` | String | API link to the sport. |
| `pr_portal_calculated_fields_total7_inn_games` | Int64 | Calculated total count of seven-inning games as tallied by the MLB Stats API pace portal. |
| `pr_portal_calculated_fields_total9_inn_games` | Float64 | Calculated total count of nine-inning games as tallied by the MLB Stats API pace portal. |
| `pr_portal_calculated_fields_total_extra_inn_games` | Int64 | Portal-calculated total extra-inning games. |
| `pr_portal_calculated_fields_time_per7_inn_game` | String | Calculated average game time per seven-inning game as produced by the MLB Stats API pace portal. |
| `pr_portal_calculated_fields_time_per9_inn_game` | String | Calculated average game time per nine-inning game as produced by the MLB Stats API pace portal. |
| `pr_portal_calculated_fields_time_per_extra_inn_game` | String | Portal-calculated time per extra-inning game. |
| `time_per7_inn_game` | String | Average elapsed clock time per seven-inning game formatted as hours and minutes. |
| `total7_inn_games_scheduled` | Float64 | Total number of seven-inning games that were scheduled in the sample period. |
| `total7_inn_games_without_extra_inn` | Float64 | Number of seven-inning games completed without requiring extra innings. |
| `total7_inn_games_completed_early` | Float64 | Number of seven-inning games that were called or completed before the full seven innings were played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_game_pace-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_game_pace-example}

```python
mlb_game_pace(season='2023')
```
