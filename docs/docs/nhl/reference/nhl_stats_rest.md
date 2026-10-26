---
title: NHL — NHL Stats REST API
sidebar_label: NHL Stats REST API
description: "NHL — NHL Stats REST API — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 12
toc_max_heading_level: 2
---
# NHL — NHL Stats REST API

`sportsdataverse.nhl` — 21 endpoints.

## nhl_stats_rest_ping

Ping the NHL Stats REST API database.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/ping`

**Valid URL:** [https://api.nhle.com/stats/rest/ping](https://api.nhle.com/stats/rest/ping)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_stats_rest_ping-returns}

Ping the NHL Stats REST API database.

### Raises {#nhl_stats_rest_ping-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_ping-example}

```python
nhl_stats_rest_ping()
```

## nhl_stats_rest_component_season

Retrieve the component-season configuration.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/componentSeason`

**Valid URL:** [https://api.nhle.com/stats/rest/en/componentSeason](https://api.nhle.com/stats/rest/en/componentSeason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_component_season-returns}

Retrieve the component-season configuration.

### Raises {#nhl_stats_rest_component_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_component_season-example}

```python
nhl_stats_rest_component_season()
```

## nhl_stats_rest_config

Retrieve the Stats REST API configuration payload.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/config`

**Valid URL:** [https://api.nhle.com/stats/rest/en/config](https://api.nhle.com/stats/rest/en/config)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_config-returns}

Retrieve the Stats REST API configuration payload.

### Raises {#nhl_stats_rest_config-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_config-example}

```python
nhl_stats_rest_config()
```

## nhl_stats_rest_content_module

Retrieve a content module by template key.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/content/module/{template_key}`

**Valid URL:** [https://api.nhle.com/stats/rest/en/content/module/X](https://api.nhle.com/stats/rest/en/content/module/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `template_key` | `template_key` |  | `Y` |  | template_key path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_content_module-returns}

Retrieve a content module by template key.

### Raises {#nhl_stats_rest_content_module-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_content_module-example}

```python
nhl_stats_rest_content_module(template_key='X')
```

## nhl_stats_rest_country

Retrieve the list of countries used in NHL data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/country`

**Valid URL:** [https://api.nhle.com/stats/rest/en/country](https://api.nhle.com/stats/rest/en/country)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_country-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Unique player identifier. |
| `country3_code` | String | Three-letter ISO country code used by the NHL api-web country reference endpoint. |
| `country_code` | String | Player country code. |
| `country_name` | String | Full English display name of the country as provided by the NHL api-web country reference. |
| `has_player_stats` | Int64 | Flag (1/0) indicating whether the NHL api-web tracks player statistics for this country. |
| `image_url` | String | Player headshot URL. |
| `ioc_code` | String | International Olympic Committee three-letter country code assigned to this nation. |
| `is_active` | Int64 | Whether the team is active. |
| `nationality_name` | String | Nationality label string used on player profiles in the NHL api-web (e.g., 'Canadian', 'American'). |
| `olympic_url` | String | URL to the country's Olympic profile page linked from the NHL api-web country record. |
| `thumbnail_url` | String | URL to a small thumbnail image representing the country's flag or emblem in the NHL api-web. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_country-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_country-example}

```python
nhl_stats_rest_country()
```

## nhl_stats_rest_draft

Retrieve draft data, optionally filtered with Cayenne expressions.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/draft`

**Valid URL:** [https://api.nhle.com/stats/rest/en/draft](https://api.nhle.com/stats/rest/en/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_draft-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `draft_year` | Int64 | Draft year the lottery applies to. |
| `rounds` | Int64 | Number of rounds in the draft. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_draft-example}

```python
nhl_stats_rest_draft()
```

## nhl_stats_rest_franchise

Retrieve franchise data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/franchise`

**Valid URL:** [https://api.nhle.com/stats/rest/en/franchise](https://api.nhle.com/stats/rest/en/franchise)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `full_name` | String | Player full name. |
| `team_common_name` | String | Team common (nickname) name. |
| `team_place_name` | String | Team place (city/location) name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_franchise-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_franchise-example}

```python
nhl_stats_rest_franchise()
```

## nhl_stats_rest_game

Retrieve game-level data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/game`

**Valid URL:** [https://api.nhle.com/stats/rest/en/game](https://api.nhle.com/stats/rest/en/game)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `eastern_start_time` | String | Game start time in Eastern time. |
| `game_date` | String | Game date. |
| `game_number` | Int64 | Game number within the schedule. |
| `game_schedule_state_id` | Int64 | Schedule state identifier. |
| `game_state_id` | Int64 | Game state identifier. |
| `game_type` | Int64 | Game type the row belongs to. |
| `home_score` | Int64 | Home team final score. |
| `home_team_id` | Int64 | Home team identifier. |
| `period` | Int64 | Period number. |
| `season` | Int64 | Season year (echoed from arg). |
| `visiting_score` | Int64 | Visiting team score. |
| `visiting_team_id` | Int64 | Visiting team identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_game-example}

```python
nhl_stats_rest_game()
```

## nhl_stats_rest_glossary

Retrieve the NHL Stats glossary of stat definitions.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/glossary`

**Valid URL:** [https://api.nhle.com/stats/rest/en/glossary](https://api.nhle.com/stats/rest/en/glossary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_glossary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `abbreviation` | String | Team abbreviation. |
| `definition` | String | Definition of the stat or term. |
| `first_season_for_stat` | Float64 | First season the stat was tracked (YYYYYYYY). |
| `full_name` | String | Player full name. |
| `language_code` | String | Language code of the entry. |
| `last_updated` | String | Timestamp the entry was last updated. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_glossary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_glossary-example}

```python
nhl_stats_rest_glossary()
```

## nhl_stats_rest_goalie_report

Retrieve a goalie statistical report.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/goalie/{report}`

**Valid URL:** [https://api.nhle.com/stats/rest/en/goalie/summary](https://api.nhle.com/stats/rest/en/goalie/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `report` | `report` |  | `Y` |  | report path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_goalie_report-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | Int64 | Assists. |
| `games_played` | Int64 | Games played. |
| `games_started` | Int64 | Games started (goalies). |
| `goalie_full_name` | String | Goalie full name. |
| `goals` | Int64 | Goals scored. |
| `goals_against` | Int64 | Goals against. |
| `goals_against_average` | Float64 | Goals against average. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `ot_losses` | Int64 | Overtime losses. |
| `penalty_minutes` | Int64 | Penalty minutes. |
| `player_id` | Int64 | Unique player identifier. |
| `points` | Int64 | Total points (goals + assists). |
| `save_pct` | Float64 | Save percentage. |
| `saves` | Int64 | Saves made. |
| `season_id` | Int64 | Season identifier. |
| `shoots_catches` | String | Handedness (shoots/catches). |
| `shots_against` | Int64 | Shots faced. |
| `shutouts` | Int64 | Shutouts recorded. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `ties` | String | Total ties. |
| `time_on_ice` | Int64 | Time on ice in seconds. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_goalie_report-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_goalie_report-example}

```python
nhl_stats_rest_goalie_report(report='summary')
```

## nhl_stats_rest_leaders_goalies

Retrieve league leaders for a goalie statistical attribute.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/leaders/goalies/{attribute}`

**Valid URL:** [https://api.nhle.com/stats/rest/en/leaders/goalies/X](https://api.nhle.com/stats/rest/en/leaders/goalies/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `attribute` | `attribute` |  | `Y` |  | attribute path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_leaders_goalies-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `save_pctg` | Float64 | Save percentage. |
| `player_id` | Int64 | Unique player identifier. |
| `player_current_team_id` | Int64 | Player's current team identifier. |
| `player_first_name` | String | Player first name. |
| `player_full_name` | String | Player full name. |
| `player_last_name` | String | Player last name. |
| `player_position_code` | String | Player position code. |
| `player_sweater_number` | Float64 | Player jersey number. |
| `team_id` | Int64 | Unique team identifier. |
| `team_franchise_id` | Int64 | Team franchise identifier. |
| `team_full_name` | String | Full team name. |
| `team_league_id` | Int64 | League identifier of the team. |
| `team_logos` | String | Team logo metadata. |
| `team_raw_tricode` | String | Team raw three-letter code. |
| `team_tri_code` | String | Team tri-code abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_leaders_goalies-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_leaders_goalies-example}

```python
nhl_stats_rest_leaders_goalies(attribute='X')
```

## nhl_stats_rest_leaders_skaters

Retrieve league leaders for a skater statistical attribute.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/leaders/skaters/{attribute}`

**Valid URL:** [https://api.nhle.com/stats/rest/en/leaders/skaters/X](https://api.nhle.com/stats/rest/en/leaders/skaters/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `attribute` | `attribute` |  | `Y` |  | attribute path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_leaders_skaters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `goals` | Int64 | Goals scored. |
| `player_id` | Int64 | Unique player identifier. |
| `player_current_team_id` | Int64 | Player's current team identifier. |
| `player_first_name` | String | Player first name. |
| `player_full_name` | String | Player full name. |
| `player_last_name` | String | Player last name. |
| `player_position_code` | String | Player position code. |
| `player_sweater_number` | Int64 | Player jersey number. |
| `team_id` | Int64 | Unique team identifier. |
| `team_franchise_id` | Int64 | Team franchise identifier. |
| `team_full_name` | String | Full team name. |
| `team_league_id` | Int64 | League identifier of the team. |
| `team_logos` | String | Team logo metadata. |
| `team_raw_tricode` | String | Team raw three-letter code. |
| `team_tri_code` | String | Team tri-code abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_leaders_skaters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_leaders_skaters-example}

```python
nhl_stats_rest_leaders_skaters(attribute='X')
```

## nhl_stats_rest_milestones_goalies

Retrieve milestone data for goalies.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/milestones/goalies`

**Valid URL:** [https://api.nhle.com/stats/rest/en/milestones/goalies](https://api.nhle.com/stats/rest/en/milestones/goalies)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_milestones_goalies-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `current_team_id` | Int64 | Player's current team identifier. |
| `first_name` | String | Player first name. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games_played` | Int64 | Games played. |
| `last_name` | String | Player last name. |
| `milestone` | String | Milestone category. |
| `milestone_amount` | Int64 | Amount remaining to reach the milestone. |
| `player_full_name` | String | Player full name. |
| `player_id` | Int64 | Unique player identifier. |
| `so` | Int64 | Shutouts. |
| `team_abbrev` | String | Team abbreviation. |
| `team_common_name` | String | Team common (nickname) name. |
| `team_full_name` | String | Full team name. |
| `team_place_name` | String | Team place (city/location) name. |
| `toi_minutes` | Int64 | Time on ice in minutes. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_milestones_goalies-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_milestones_goalies-example}

```python
nhl_stats_rest_milestones_goalies()
```

## nhl_stats_rest_milestones_skaters

Retrieve milestone data for skaters.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/milestones/skaters`

**Valid URL:** [https://api.nhle.com/stats/rest/en/milestones/skaters](https://api.nhle.com/stats/rest/en/milestones/skaters)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_milestones_skaters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `assists` | Int64 | Assists. |
| `current_team_id` | Int64 | Player's current team identifier. |
| `first_name` | String | Player first name. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games_played` | Int64 | Games played. |
| `goals` | Int64 | Goals scored. |
| `last_name` | String | Player last name. |
| `milestone` | String | Milestone category. |
| `milestone_amount` | Int64 | Amount remaining to reach the milestone. |
| `player_full_name` | String | Player full name. |
| `player_id` | Int64 | Unique player identifier. |
| `points` | Int64 | Total points (goals + assists). |
| `team_abbrev` | String | Team abbreviation. |
| `team_common_name` | String | Team common (nickname) name. |
| `team_full_name` | String | Full team name. |
| `team_place_name` | String | Team place (city/location) name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_milestones_skaters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_milestones_skaters-example}

```python
nhl_stats_rest_milestones_skaters()
```

## nhl_stats_rest_players

Retrieve the NHL player registry.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/players`

**Valid URL:** [https://api.nhle.com/stats/rest/en/players](https://api.nhle.com/stats/rest/en/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_stats_rest`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_players-example}

```python
nhl_stats_rest_players()
```

## nhl_stats_rest_season

Retrieve the list of all NHL seasons.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/season`

**Valid URL:** [https://api.nhle.com/stats/rest/en/season](https://api.nhle.com/stats/rest/en/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `all_star_game_in_use` | Int64 | Whether an All-Star Game was held this season. |
| `conferences_in_use` | Int64 | Whether conferences were in use that season. |
| `divisions_in_use` | Int64 | Whether divisions were in use that season. |
| `end_date` | String | Season end date. |
| `entry_draft_in_use` | Int64 | Whether an entry draft was in use this season. |
| `formatted_season_id` | String | Human-readable season string (e.g. "2023-24"). |
| `minimum_playoff_minutes_for_goalie_stats_leaders` | Int64 | Minimum playoff minutes to qualify for goalie stats leaders. |
| `minimum_regular_games_for_goalie_stats_leaders` | Int64 | Minimum regular-season games to qualify for goalie stats leaders. |
| `nhl_stanley_cup_owner` | Int64 | Whether the NHL owned the Stanley Cup this season. |
| `number_of_games` | Int64 | Number of games per team this season. |
| `olympics_participation` | Int64 | Whether NHL players participated in the Olympics this season. |
| `point_for_ot_loss_in_use` | Int64 | Whether the overtime-loss point was in use this season. |
| `preseason_startdate` | String | Preseason start date. |
| `regular_season_end_date` | String | Regular-season end date. |
| `row_in_use` | Int64 | Whether the regulation/overtime/shootout format was in use. |
| `season_ordinal` | Int64 | Ordinal sequence number of the season. |
| `start_date` | String | Season start date. |
| `supplemental_draft_in_use` | Int64 | Whether a supplemental draft was in use this season. |
| `ties_in_use` | Int64 | Whether ties were in use that season. |
| `total_playoff_games` | Int64 | Total number of playoff games this season. |
| `total_regular_season_games` | Int64 | Total number of regular-season games this season. |
| `wildcard_in_use` | Int64 | Whether the wild-card playoff format was in use this season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_season-example}

```python
nhl_stats_rest_season()
```

## nhl_stats_rest_shiftcharts

Retrieve shift-chart data.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/shiftcharts`

**Valid URL:** [https://api.nhle.com/stats/rest/en/shiftcharts](https://api.nhle.com/stats/rest/en/shiftcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_shiftcharts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `detail_code` | Int64 | Numeric code identifying the specific detail type or sub-category within the shift or event record. |
| `duration` | String | Penalty duration in minutes. |
| `end_time` | String | Shift end time (MM:SS countdown clock). |
| `event_description` | String |  |
| `event_details` | String | Serialized details describing the on-ice event associated with the shift, such as play type and participants. |
| `event_number` | Int64 |  |
| `first_name` | String | Player first name. |
| `game_id` | Int64 | Unique game identifier. |
| `hex_value` | String | Hexadecimal color code associated with the event or team, used for display rendering. |
| `last_name` | String | Player last name. |
| `period` | Int64 | Period number. |
| `player_id` | Int64 | Unique player identifier. |
| `shift_number` | Int64 | Sequential number identifying the shift within the game, ordered chronologically by start time. |
| `start_time` | String | Shift start time (MM:SS countdown clock). |
| `team_abbrev` | String | Team abbreviation. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `type_code` | Int64 | Numeric code identifying the high-level event type (e.g., faceoff, goal, penalty) for this shift record. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_shiftcharts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_shiftcharts-example}

```python
nhl_stats_rest_shiftcharts()
```

## nhl_stats_rest_skater_report

Retrieve a skater statistical report.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/skater/{report}`

**Valid URL:** [https://api.nhle.com/stats/rest/en/skater/summary](https://api.nhle.com/stats/rest/en/skater/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `report` | `report` |  | `Y` |  | report path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_skater_report-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | Int64 | Assists. |
| `ev_goals` | Int64 | Even-strength goals. |
| `ev_points` | Int64 | Even-strength points. |
| `faceoff_win_pct` | Float64 | Faceoff win percentage. |
| `game_winning_goals` | Int64 | Game-winning goals. |
| `games_played` | Int64 | Games played. |
| `goals` | Int64 | Goals scored. |
| `last_name` | String | Player last name. |
| `ot_goals` | Int64 | Overtime goals. |
| `penalty_minutes` | Int64 | Penalty minutes. |
| `player_id` | Int64 | Unique player identifier. |
| `plus_minus` | Int64 | Plus/minus rating. |
| `points` | Int64 | Total points (goals + assists). |
| `points_per_game` | Float64 | Points per game. |
| `position_code` | String | Player position code. |
| `pp_goals` | Int64 | Power-play goals. |
| `pp_points` | Int64 | Power-play points. |
| `season_id` | Int64 | Season identifier. |
| `sh_goals` | Int64 | Short-handed goals. |
| `sh_points` | Int64 | Short-handed points. |
| `shooting_pct` | Float64 | Shooting percentage. |
| `shoots_catches` | String | Handedness (shoots/catches). |
| `shots` | Int64 | Shots on goal. |
| `skater_full_name` | String | Player full name. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `time_on_ice_per_game` | Float64 | Average time on ice per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_skater_report-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_skater_report-example}

```python
nhl_stats_rest_skater_report(report='summary')
```

## nhl_stats_rest_team

Retrieve the list of all NHL teams.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/team`

**Valid URL:** [https://api.nhle.com/stats/rest/en/team](https://api.nhle.com/stats/rest/en/team)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `full_name` | String | Player full name. |
| `league_id` | Int64 | League identifier of the team. |
| `raw_tricode` | String | Team raw three-letter code. |
| `tri_code` | String | Team three-letter code. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_team-example}

```python
nhl_stats_rest_team()
```

## nhl_stats_rest_team_by_id

Retrieve a single team by its numeric ID.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/team/id/{team_id}`

**Valid URL:** [https://api.nhle.com/stats/rest/en/team/id/10](https://api.nhle.com/stats/rest/en/team/id/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_team_by_id-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `full_name` | String | Player full name. |
| `league_id` | Int64 | League identifier of the team. |
| `raw_tricode` | String | Team raw three-letter code. |
| `tri_code` | String | Team three-letter code. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_team_by_id-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_team_by_id-example}

```python
nhl_stats_rest_team_by_id(team_id=10)
```

## nhl_stats_rest_team_report

Retrieve a team statistical report.

**Endpoint URL:** `GET https://api.nhle.com/stats/rest/{lang}/team/{report}`

**Valid URL:** [https://api.nhle.com/stats/rest/en/team/summary](https://api.nhle.com/stats/rest/en/team/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `report` | `report` |  | `Y` |  | report path parameter. |
| `lang` | `lang` |  |  | `Y` | lang path parameter. |

### Returns {#nhl_stats_rest_team_report-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `faceoff_win_pct` | Float64 | Faceoff win percentage. |
| `games_played` | Int64 | Games played. |
| `goals_against` | Int64 | Goals against. |
| `goals_against_per_game` | Float64 | Goals against per game. |
| `goals_for` | Int64 | Goals for. |
| `goals_for_per_game` | Float64 | Goals for per game. |
| `losses` | Int64 | Losses. |
| `ot_losses` | Int64 | Overtime losses. |
| `penalty_kill_net_pct` | Float64 | Net penalty kill percentage. |
| `penalty_kill_pct` | Float64 | Penalty kill percentage. |
| `point_pct` | Float64 | Points percentage. |
| `points` | Int64 | Total points (goals + assists). |
| `power_play_net_pct` | Float64 | Net power play percentage. |
| `power_play_pct` | Float64 | Power play percentage. |
| `regulation_and_ot_wins` | Int64 | Wins in regulation and overtime. |
| `season_id` | Int64 | Season identifier. |
| `shots_against_per_game` | Float64 | Shots against per game. |
| `shots_for_per_game` | Float64 | Shots for per game. |
| `team_full_name` | String | Full team name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_shutouts` | Int64 | Team shutouts. |
| `ties` | String | Total ties. |
| `wins` | Int64 | Wins. |
| `wins_in_regulation` | Int64 | Wins in regulation. |
| `wins_in_shootout` | Int64 | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_stats_rest_team_report-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_stats_rest_team_report-example}

```python
nhl_stats_rest_team_report(report='summary')
```
