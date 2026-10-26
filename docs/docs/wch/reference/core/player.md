---
title: "Women's college hockey — ESPN core API (v2) — Player"
sidebar_label: "Player"
sidebar_position: 2
description: "Women's college hockey — ESPN core API (v2) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Women's college hockey — ESPN core API (v2) — Player

## espn_wch_players_index

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes?active=true&limit=100&page=1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes?active=true&limit=100&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `active` | `active` |  |  | `Y` | active query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_wch_players_index-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_players_index-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_players_index-example}

```python
espn_wch_players_index()
```

## espn_wch_player_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wch_player_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `active` | Boolean |  |
| `age` | Int64 |  |
| `date_of_birth` | String |  |
| `debut_year` | Int64 |  |
| `display_height` | String |  |
| `display_name` | String |  |
| `display_weight` | String |  |
| `first_name` | String |  |
| `full_name` | String |  |
| `guid` | String |  |
| `height` | Float64 |  |
| `id` | Int64 |  |
| `jersey` | String |  |
| `last_name` | String |  |
| `linked` | Boolean |  |
| `links` | String |  |
| `short_name` | String |  |
| `slug` | String |  |
| `type` | String |  |
| `uid` | String |  |
| `weight` | Float64 |  |
| `alternate_ids_sdr` | String |  |
| `birth_place_city` | String |  |
| `birth_place_state` | String |  |
| `college_$ref` | String |  |
| `college_athlete_$ref` | String |  |
| `contracts_$ref` | String |  |
| `draft_display_text` | String |  |
| `draft_pick_$ref` | String |  |
| `draft_round` | Int64 |  |
| `draft_selection` | Int64 |  |
| `draft_team_$ref` | String |  |
| `draft_year` | Int64 |  |
| `experience_years` | Int64 |  |
| `hand_abbreviation` | String |  |
| `hand_display_value` | String |  |
| `hand_type` | String |  |
| `headshot_alt` | String |  |
| `headshot_href` | String |  |
| `position_$ref` | String |  |
| `position_abbreviation` | String |  |
| `position_display_name` | String |  |
| `position_id` | Int64 |  |
| `position_leaf` | Boolean |  |
| `position_name` | String |  |
| `seasons_$ref` | String |  |
| `statistics_$ref` | String |  |
| `statisticslog_$ref` | String |  |
| `status_abbreviation` | String |  |
| `status_id` | Int64 |  |
| `status_name` | String |  |
| `status_type` | String |  |
| `team_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_core-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_core-example}

```python
espn_wch_player_core(athlete_id='4239')
```

## espn_wch_player_career_stats

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/statistics[/{stat_type}]`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/statistics](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `stat_type` | `stat_type` |  |  | `Y` | stat_type path parameter. |

### Returns {#espn_wch_player_career_stats-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, mlb, mbb, 2026-10-07); its rows sit under keys it does not read (top level: $ref, athlete, splits).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_career_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_career_stats-example}

```python
espn_wch_player_career_stats(athlete_id='4239')
```

## espn_wch_player_statisticslog

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/statisticslog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/statisticslog](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/statisticslog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wch_player_statisticslog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `statistics` | String |  |
| `season_$ref` | String |  |
| `count` | Int64 | Always null, since the statisticslog payload ships its rows under entries with no page envelope. |
| `page_count` | Int64 | Always null, since the statisticslog payload ships its rows under entries with no page envelope. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_statisticslog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_statisticslog-example}

```python
espn_wch_player_statisticslog(athlete_id='4239')
```

## espn_wch_player_eventlog

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/eventlog`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/eventlog](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/eventlog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wch_player_eventlog-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_eventlog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_eventlog-example}

```python
espn_wch_player_eventlog(athlete_id='4239')
```

## espn_wch_player_contracts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/contracts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/contracts](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/contracts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wch_player_contracts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_contracts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_contracts-example}

```python
espn_wch_player_contracts(athlete_id='4239')
```

## espn_wch_player_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/awards](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wch_player_awards-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_awards-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_awards-example}

```python
espn_wch_player_awards(athlete_id='4239')
```

## espn_wch_player_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/seasons](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wch_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_seasons-example}

```python
espn_wch_player_seasons(athlete_id='4239')
```

## espn_wch_player_records

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/records`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/records](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wch_player_records-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_records-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_records-example}

```python
espn_wch_player_records(athlete_id='4239')
```

## espn_wch_player_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/injuries`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/injuries](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wch_player_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the athlete. |
| `display_name` | String | Athlete's full display name as shown on ESPN. |
| `injuries` | String | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_injuries-example}

```python
espn_wch_player_injuries(athlete_id='4239')
```

## espn_wch_player_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/notes](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wch_player_notes-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_notes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_notes-example}

```python
espn_wch_player_notes(athlete_id='4239')
```

## espn_wch_player_vs_player

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/{athlete_id}/vsathlete/{opp_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/vsathlete/5](https://sports.core.api.espn.com/v2/sports/hockey/leagues/womens-college-hockey/athletes/4239/vsathlete/5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `opp_id` | `opp_id` |  | `Y` |  | opp_id path parameter. |

### Returns {#espn_wch_player_vs_player-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wch_player_vs_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wch_player_vs_player-example}

```python
espn_wch_player_vs_player(athlete_id='4239', opp_id='5')
```
