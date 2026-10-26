---
title: "CBS Sports — CBS Sports NAPI (api.cbssports.com/napi) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "CBS Sports — CBS Sports NAPI (api.cbssports.com/napi) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CBS Sports — CBS Sports NAPI (api.cbssports.com/napi) — Other

## cbs_bulk

Resolve resources in bulk to save HTTP traffic.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/bulk`

**Valid URL:** [https://api.cbssports.com/napi/resource/bulk](https://api.cbssports.com/napi/resource/bulk)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `PlayerResource` | `player_resource` |  |  | `Y` | CSV list of player IDs to retrieve. |
| `TeamResource` | `team_resource` |  |  | `Y` | CSV list of team IDs to retrieve. |
| `GameResource` | `game_resource` |  |  | `Y` | CSV list of game IDs to retrieve. |
| `VenueResource` | `venue_resource` |  |  | `Y` | CSV list of venue IDs to retrieve |
| `EventResource` | `event_resource` |  |  | `Y` | CSV list of event IDs to retrieve |
| `FeaturedGameResource` | `featured_game_resource` |  |  | `Y` | CSV list of game IDs to retrieve. |
| `GolfEventMarketsResource` | `golf_event_markets_resource` |  |  | `Y` | CSV list of golf event markets IDs to retrieve |

### Returns {#cbs_bulk-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_bulk-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_bulk-example}

```python
cbs_bulk()
```

## cbs_client_config

Get configuration for how a client should access our APIs, or any additional settings they want supplied to them.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/client/config/{client_name}`

**Valid URL:** [https://api.cbssports.com/napi/resource/client/config/cbs](https://api.cbssports.com/napi/resource/client/config/cbs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `client_name` | `client_name` |  | `Y` |  | Client name as it appears in the database |
| `resources` | `resources` |  |  | `Y` | Allowed: league, season. |
| `leagueId` | `league_id` |  |  | `Y` | View option.  Filter by leagueId. |
| `classifier` | `classifier` |  |  | `Y` | View option.  Filter by a certain classifier. |
| `keyName` | `key_name` |  |  | `Y` | View option.  Filter by a custom key name. |

### Returns {#cbs_client_config-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_client_config-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_client_config-example}

```python
cbs_client_config(client_name='cbs')
```

## cbs_coach_rankings

Get rankings resource for a coach.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/coach/rankings/{coach_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | Numerical player ID |

### Returns {#cbs_coach_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_coach_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_coach_rankings-example}

```python
cbs_coach_rankings()
```

## cbs_coach_team_associations

Get team associations for a particular coach.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/coach/teamAssociations/{coach_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | Numerical player ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: team. |

### Returns {#cbs_coach_team_associations-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_coach_team_associations-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_coach_team_associations-example}

```python
cbs_coach_team_associations()
```

## cbs_division_subdivisions

Get subdivisions for a division from Atlas.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/division/subdivisions/{division_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `division_id` | `division_id` |  | `Y` |  | Numerical division ID |
| `subDivisionId` | `sub_division_id` |  |  | `Y` | View option for rendering only a certain subdivision |
| `name` | `name` |  |  | `Y` | View option for a csv of subdivsion names to render |

### Returns {#cbs_division_subdivisions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_division_subdivisions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_division_subdivisions-example}

```python
cbs_division_subdivisions()
```

## cbs_endpoint_registry

Get the resource endpoint registry

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/endpoint/registry`

**Valid URL:** [https://api.cbssports.com/napi/resource/endpoint/registry](https://api.cbssports.com/napi/resource/endpoint/registry)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#cbs_endpoint_registry-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `key` | String | Registry key for the endpoint, which is CBS's internal resource class name such as BoxscoreResource or PlayerResource; the parser lifts it out of the payload's top-level key into a column. |
| `location` | String |  |
| `route` | String |  |
| `path` | String | OpenAPI-style request path for the endpoint with brace placeholders, e.g. /resource/game/boxscore/{gameId}. |
| `summary` | String |  |
| `notes` | String |  |
| `methods` | String | JSON-encoded list of HTTP verbs the endpoint accepts; every entry in the captured registry allows GET only. |
| `formats` | String | JSON-encoded list of response serialisations the endpoint can emit, json throughout the captured registry. |
| `parameters` | String | JSON-encoded list of parameter descriptors, each carrying name, required, dataType, paramType (path or query), an optional allowedValues enumeration and CBS's own prose description. |
| `versions_allowed` | String | JSON-encoded list of API versions the endpoint will serve, e.g. ["v1"]. |
| `versions_current` | String | API version the endpoint serves when the caller does not pin one, e.g. v1. |
| `auth_settings_require_auth` | Boolean | Whether CBS's registry marks the endpoint as requiring an authenticated client; the data-backed resources stay anonymously reachable in practice even where this is true. |
| `auth_settings_allow_only` | String | JSON-encoded list of CBS client identifiers allow-listed for the endpoint, e.g. mweb, mobile, fantasy, prism. |
| `resource_cache_ns` | String | Cache namespace CBS files the endpoint's responses under, e.g. FINALBOXSCORE or PLAYERTEAMASSOCIATION. |
| `resource_cache_cache_keys` | String | JSON-encoded list of request parameters that compose the endpoint's cache key, e.g. ["gameId"]. |
| `resource_cache_cache_buster` | Int64 | Generation counter CBS bumps to invalidate every cached response for the endpoint. |
| `expiration_message_object_key_name` | String | Payload key CBS quotes back in the endpoint's cache-expiration message, e.g. objectKey, playerId or teamId. |
| `routes` | String | JSON-encoded list of colon-style route patterns for the endpoints reachable at more than one route; only the conference, division and team resources carry it, each adding a /resource/vendor/{vendorId}/... variant. |
| `paths` | String | JSON-encoded list of brace-style request paths matching routes, present only on the endpoints that expose several routes. |
| `expiration_message` | String | Cache-expiration descriptor as CBS returns it for the four entries where the block is null rather than an object; the object form is flattened into expiration_message_object_key_name instead. |
| `is_active` | Boolean |  |
| `resource_cache_no_cache` | Boolean | Marker carried only by the endpoints CBS never caches (the bulk controller and the registry itself), whose resourceCache block holds noCache in place of a namespace. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_endpoint_registry-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_endpoint_registry-example}

```python
cbs_endpoint_registry()
```

## cbs_event

Get an event resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/{event_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: entrants, venues, leaderboard, weather, markets. |

### Returns {#cbs_event-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_event-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_event-example}

```python
cbs_event()
```

## cbs_event_entrants

Get players entered in a particular event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/entrants/{event_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns {#cbs_event_entrants-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_event_entrants-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_event_entrants-example}

```python
cbs_event_entrants()
```

## cbs_event_leaderboard

Get a leaderboard data resource for a particular event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/leaderboard/{event_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns {#cbs_event_leaderboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_event_leaderboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_event_leaderboard-example}

```python
cbs_event_leaderboard()
```

## cbs_event_seasons

Get seasons associated to a particular event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/seasons/{event_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns {#cbs_event_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_event_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_event_seasons-example}

```python
cbs_event_seasons()
```

## cbs_event_venues

Get venues associated to a particular event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/venues/{event_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns {#cbs_event_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_event_venues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_event_venues-example}

```python
cbs_event_venues()
```

## cbs_game

Get a game resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: homeTeam, awayTeam, league, lineup, odds, players, standings, conference, division, probablePlayers, player, playerTeamAssociations, injuries, transactions, depthCharts, metaData, boxscore, venue, scoringLeaders, scoringPlayerStats, scoringScoreboard, scoringScores, scoringYtdPlayerStats, scoringYtdTeamStats, scoringRosters, scoringPlays, scoringTeamStats, scoringBoxscores, gameOdds, gameOutcomes, ticket, scoringDrives, scoringWinProb, gameHqOdds, weather, featured, gameProps, bettingSplits, gameRTWP. |

### Returns {#cbs_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_game-example}

```python
cbs_game()
```

## cbs_golf_event_markets

Get markets for an event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/golf/event/markets/{event_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns {#cbs_golf_event_markets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_golf_event_markets-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_golf_event_markets-example}

```python
cbs_golf_event_markets()
```

## cbs_golf_player_markets

Get markets for a golfer.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/golf/player/markets/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/golf/player/markets/1751796](https://api.cbssports.com/napi/resource/golf/player/markets/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `eventId` | `event_id` |  |  | `Y` | View option.  Filter by eventId. |

### Returns {#cbs_golf_player_markets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_golf_player_markets-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_golf_player_markets-example}

```python
cbs_golf_player_markets(player_id=1751796)
```

## cbs_golfer_results

Get golfer tournament results resource for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/golfer/results/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/golfer/results/1751796](https://api.cbssports.com/napi/resource/golfer/results/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonType. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |

### Returns {#cbs_golfer_results-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_golfer_results-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_golfer_results-example}

```python
cbs_golfer_results(player_id=1751796)
```

## cbs_league

Get a league resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/league/{league_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/league/59](https://api.cbssports.com/napi/resource/league/59)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | Numerical league ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: teams, players, standings, conference, division, polls, teamSeasons. |

### Returns {#cbs_league-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | Int64 | Sleeper league id, as a string. |
| `league_abbr` | String | Short CBS code for the league, e.g. NFL, NHL, NCAAB, EPL. |
| `league_name` | String |  |
| `sport_id` | Int64 |  |
| `league_type` | String | Single-character CBS classification code for the league; all seventeen captured leagues carry M and CBS does not publish the rest of the code set. |
| `teams` | String |  |
| `color_primary` | String | Primary brand colour of the league as six hex digits, inconsistently prefixed with a hash (#003369 for the NFL, 002D72 for MLB); null for the leagues CBS carries no palette for. |
| `color_secondary` | String | Secondary brand colour of the league as six hex digits, with the same inconsistent hash prefix as color_primary; null where CBS carries no palette. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_league-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_league-example}

```python
cbs_league(league_id=59)
```

## cbs_league_teams

Get team resources on a league with optional vendor overlay

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/league/teams/{league_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/league/teams/59](https://api.cbssports.com/napi/resource/league/teams/59)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | Numerical league Id - gets team from team table not teams for season |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: players, standings, conference, division, playerTeamAssociations, injuries, transactions, depthCharts, polls, teamSeasons, sportsLineStandings. |

### Returns {#cbs_league_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_league_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_league_teams-example}

```python
cbs_league_teams(league_id=59)
```

## cbs_odds

Get an odds resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/odds/{game_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns {#cbs_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_odds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_odds-example}

```python
cbs_odds()
```

## cbs_recruit_rankings

Get rankings resource for a recruit.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/recruit/rankings/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/recruit/rankings/1751796](https://api.cbssports.com/napi/resource/recruit/rankings/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns {#cbs_recruit_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_recruit_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_recruit_rankings-example}

```python
cbs_recruit_rankings(player_id=1751796)
```

## cbs_season

Get a season resource from Atlas.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/season/{season_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/season/59](https://api.cbssports.com/napi/resource/season/59)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | Numerical season ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: sport, league, teams. |

### Returns {#cbs_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_season-example}

```python
cbs_season(season_id=59)
```

## cbs_season_teams

Get team resources associated to a season

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/season/teams/{season_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/season/teams/59](https://api.cbssports.com/napi/resource/season/teams/59)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | Optional seasonYear for leagues that change teams each year. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: players, standings, conference, division, playerTeamAssociations, injuries, transactions, depthCharts, polls, teamSeasons, sportsLineStandings, league. |

### Returns {#cbs_season_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 |  |
| `stub_hub_team_id` | Int64 | StubHub performer id for the team, which is the key behind ticket_url; entirely null for the ten soccer leagues and populated only for MLB, MLS, NBA, NCAAB, NCAAF, NFL and NHL, so the port pins it back to Int64 after pandas widens the nullable column to float. |
| `location` | String |  |
| `nick_name` | String |  |
| `medium_name` | String | Medium-length display name for the team, sitting between short_name and the full location plus nickname (Arizona for the Cardinals, Duke University, Werder Bremen). |
| `short_name` | String |  |
| `abbrev` | String |  |
| `status` | String |  |
| `home_venue_id` | Int64 |  |
| `conference_id` | Int64 |  |
| `league_id` | Int64 |  |
| `division_id` | Int64 |  |
| `ticket_url` | String | StubHub ticket-purchase URL for the team, either a bare /performer/{id} link or a slugged team-tickets link; an empty string for the leagues where CBS carries no StubHub performer. |
| `color_hex_dex` | String | Six-digit team colour drawn from CBS's own colour index, with no leading hash and an empty string where unset; it can differ slightly from color_primary_hex (96223E against 97233f for the Arizona Cardinals). |
| `color_primary_hex` | String | Team's primary colour as six hex digits with no leading hash, e.g. 97233f. |
| `color_secondary_hex` | String | Team's secondary colour as six hex digits with no leading hash, e.g. 000000. |
| `players` | String |  |
| `league` | String |  |
| `standings` | String | Nested standings sub-resource for the team, JSON-encoded when present; null unless the request asked for it through the endpoint's resources parameter, which defaults to none. |
| `conference` | String |  |
| `division` | String |  |
| `team_seasons` | String | Nested list of the team's season records, JSON-encoded when present; null unless requested through the resources parameter. |
| `polls` | String | Nested poll-ranking sub-resource for the team, JSON-encoded when present; null unless requested through the resources parameter. |
| `home_venue` | String | Nested venue record for the team's home site, JSON-encoded when present; null unless requested through the resources parameter, with home_venue_id always carrying the id. |
| `sports_line_standings` | String | Nested SportsLine standings sub-resource for the team, JSON-encoded when present; null unless requested through the resources parameter. |
| `team_stats` | String | Nested team-statistics sub-resource, JSON-encoded when present; null unless requested through the resources parameter. |
| `team_rankings` | String | Nested team-rankings sub-resource, JSON-encoded when present; null unless requested through the resources parameter. |
| `sports_line_rankings` | String | Nested SportsLine rankings sub-resource, JSON-encoded when present; null unless requested through the resources parameter. |
| `meta_tsa_overlay` | Boolean | Flag on the record's meta block marking the team as carrying CBS's TSA overlay; true for every team across the captured leagues. |
| `meta_season_id` | Int64 | Season identifier CBS attaches to each team record's meta block, a small league-scoped number (2 for MLB, 18 for the NFL, 35 for the Premier League) that is constant across every team in one response and lives in a different id space from the season_id on standings rows. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_season_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_season_teams-example}

```python
cbs_season_teams(season_id=59)
```

## cbs_sport

Get a sport resource from Atlas.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/sport/{sport_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/sport/1](https://api.cbssports.com/napi/resource/sport/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  | `Y` |  | Numerical sport ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: leagues. |

### Returns {#cbs_sport-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_sport-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_sport-example}

```python
cbs_sport(sport_id=1)
```

## cbs_sport_leagues

Get league resources for a sport.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/sport/leagues/{sport_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/sport/leagues/1](https://api.cbssports.com/napi/resource/sport/leagues/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  | `Y` |  | Numerical league ID |

### Returns {#cbs_sport_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_sport_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_sport_leagues-example}

```python
cbs_sport_leagues(sport_id=1)
```

## cbs_venue

Get a venue resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/venue/{venue_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | Numerical venue ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: metaData. |

### Returns {#cbs_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_venue-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_venue-example}

```python
cbs_venue()
```

## cbs_venue_metadata

Get a venues metadata resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/venue/metadata/{venue_id}`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | Numerical venue ID |

### Returns {#cbs_venue_metadata-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#cbs_venue_metadata-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The id is unknown -- NAPI answers it with HTTP 200 plus a `warnings` entry whose `code` is 404 (or with an HTTP 404). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, or an empty or unreadable 200 body). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#cbs_venue_metadata-example}

```python
cbs_venue_metadata()
```
