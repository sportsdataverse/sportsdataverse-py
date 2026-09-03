---
title: CBS — CBS Sports NAPI (api.cbssports.com/napi)
sidebar_label: CBS Sports NAPI (api.cbssports.com/napi)
description: "CBS — CBS Sports NAPI (api.cbssports.com/napi) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
---
# CBS — CBS Sports NAPI (api.cbssports.com/napi)

`sportsdataverse.cbs` — 82 endpoints.

## `cbs_bulk`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_bulk()
```

_Last validated n/a._

## `cbs_client_config`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_client_config(client_name='cbs')
```

_Last validated n/a._

## `cbs_coach_rankings`

Get rankings resource for a coach.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/coach/rankings/{coach_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/coach/rankings](https://api.cbssports.com/napi/resource/coach/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | Numerical player ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_coach_rankings()
```

_Last validated n/a._

## `cbs_coach_team_associations`

Get team associations for a particular coach.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/coach/teamAssociations/{coach_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/coach/teamAssociations](https://api.cbssports.com/napi/resource/coach/teamAssociations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | Numerical player ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: team. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_coach_team_associations()
```

_Last validated n/a._

## `cbs_division_subdivisions`

Get subdivisions for a division from Atlas.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/division/subdivisions/{division_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/division/subdivisions](https://api.cbssports.com/napi/resource/division/subdivisions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `division_id` | `division_id` |  | `Y` |  | Numerical division ID |
| `subDivisionId` | `sub_division_id` |  |  | `Y` | View option for rendering only a certain subdivision |
| `name` | `name` |  |  | `Y` | View option for a csv of subdivsion names to render |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_division_subdivisions()
```

_Last validated n/a._

## `cbs_endpoint_registry`

Get the resource endpoint registry

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/endpoint/registry`

**Valid URL:** [https://api.cbssports.com/napi/resource/endpoint/registry](https://api.cbssports.com/napi/resource/endpoint/registry)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | character | Registry key for the endpoint, which is CBS's internal resource class name such as BoxscoreResource or PlayerResource; the parser lifts it out of the payload's top-level key into a column. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `route` | character | A string indicating the route the primary receiver on a play took. Has the following possible values: "CORNER", "DEEP OUT", "GO", "HITCH/CURL", "IN/DIG", "POST", "QUICK OUT", "SCREEN", "SHALLOW CROSS/DRAG", "SLANT", "SWING", "TEXAS/ANGLE", "WHEEL". |
| `path` | character | OpenAPI-style request path for the endpoint with brace placeholders, e.g. /resource/game/boxscore/{gameId}. |
| `summary` | character | Record summary string (e.g. "25-15-10"). |
| `notes` | character | Free-form notes attached to the record. |
| `methods` | character | JSON-encoded list of HTTP verbs the endpoint accepts; every entry in the captured registry allows GET only. |
| `formats` | character | JSON-encoded list of response serialisations the endpoint can emit, json throughout the captured registry. |
| `parameters` | character | JSON-encoded list of parameter descriptors, each carrying name, required, dataType, paramType (path or query), an optional allowedValues enumeration and CBS's own prose description. |
| `versions_allowed` | character | JSON-encoded list of API versions the endpoint will serve, e.g. ["v1"]. |
| `versions_current` | character | API version the endpoint serves when the caller does not pin one, e.g. v1. |
| `auth_settings_require_auth` | logical | Whether CBS's registry marks the endpoint as requiring an authenticated client; the data-backed resources stay anonymously reachable in practice even where this is true. |
| `auth_settings_allow_only` | character | JSON-encoded list of CBS client identifiers allow-listed for the endpoint, e.g. mweb, mobile, fantasy, prism. |
| `resource_cache_ns` | character | Cache namespace CBS files the endpoint's responses under, e.g. FINALBOXSCORE or PLAYERTEAMASSOCIATION. |
| `resource_cache_cache_keys` | character | JSON-encoded list of request parameters that compose the endpoint's cache key, e.g. ["gameId"]. |
| `resource_cache_cache_buster` | integer | Generation counter CBS bumps to invalidate every cached response for the endpoint. |
| `expiration_message_object_key_name` | character | Payload key CBS quotes back in the endpoint's cache-expiration message, e.g. objectKey, playerId or teamId. |
| `routes` | character | JSON-encoded list of colon-style route patterns for the endpoints reachable at more than one route; only the conference, division and team resources carry it, each adding a /resource/vendor/{vendorId}/... variant. |
| `paths` | character | JSON-encoded list of brace-style request paths matching routes, present only on the endpoints that expose several routes. |
| `expiration_message` | character | Cache-expiration descriptor as CBS returns it for the four entries where the block is null rather than an object; the object form is flattened into expiration_message_object_key_name instead. |
| `is_active` | character | Whether the team was active in this season. |
| `resource_cache_no_cache` | character | Marker carried only by the endpoints CBS never caches (the bulk controller and the registry itself), whose resourceCache block holds noCache in place of a namespace. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_endpoint_registry()
```

_Last validated n/a._

## `cbs_event`

Get an event resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/{event_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/event](https://api.cbssports.com/napi/resource/event)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: entrants, venues, leaderboard, weather, markets. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_event()
```

_Last validated n/a._

## `cbs_event_entrants`

Get players entered in a particular event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/entrants/{event_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/event/entrants](https://api.cbssports.com/napi/resource/event/entrants)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_event_entrants()
```

_Last validated n/a._

## `cbs_event_leaderboard`

Get a leaderboard data resource for a particular event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/leaderboard/{event_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/event/leaderboard](https://api.cbssports.com/napi/resource/event/leaderboard)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_event_leaderboard()
```

_Last validated n/a._

## `cbs_event_seasons`

Get seasons associated to a particular event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/seasons/{event_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/event/seasons](https://api.cbssports.com/napi/resource/event/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_event_seasons()
```

_Last validated n/a._

## `cbs_event_venues`

Get venues associated to a particular event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/event/venues/{event_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/event/venues](https://api.cbssports.com/napi/resource/event/venues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_event_venues()
```

_Last validated n/a._

## `cbs_game`

Get a game resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game](https://api.cbssports.com/napi/resource/game)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: homeTeam, awayTeam, league, lineup, odds, players, standings, conference, division, probablePlayers, player, playerTeamAssociations, injuries, transactions, depthCharts, metaData, boxscore, venue, scoringLeaders, scoringPlayerStats, scoringScoreboard, scoringScores, scoringYtdPlayerStats, scoringYtdTeamStats, scoringRosters, scoringPlays, scoringTeamStats, scoringBoxscores, gameOdds, gameOutcomes, ticket, scoringDrives, scoringWinProb, gameHqOdds, weather, featured, gameProps, bettingSplits, gameRTWP. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game()
```

_Last validated n/a._

## `cbs_game_betting_splits`

Get a BettingSplits resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/bettingSplits/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/bettingSplits](https://api.cbssports.com/napi/resource/game/bettingSplits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_betting_splits()
```

_Last validated n/a._

## `cbs_game_boxscore`

Get boxscore resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/boxscore/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/boxscore](https://api.cbssports.com/napi/resource/game/boxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_boxscore()
```

_Last validated n/a._

## `cbs_game_content_preview`

Get content for game preview

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/content/preview/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/content/preview](https://api.cbssports.com/napi/resource/game/content/preview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_content_preview()
```

_Last validated n/a._

## `cbs_game_content_recap`

Get content for game recap

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/content/recap/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/content/recap](https://api.cbssports.com/napi/resource/game/content/recap)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_content_recap()
```

_Last validated n/a._

## `cbs_game_content_story`

Get content for game story

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/content/story/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/content/story](https://api.cbssports.com/napi/resource/game/content/story)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `gameIdsStoryTags` | `game_ids_story_tags` |  |  | `Y` | The tags used to retrieve stories |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_content_story()
```

_Last validated n/a._

## `cbs_game_featured`

Get a FeaturedGame resource.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/featured/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/featured](https://api.cbssports.com/napi/resource/game/featured)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_featured()
```

_Last validated n/a._

## `cbs_game_lineup`

Get a lineup resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/lineup/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/lineup](https://api.cbssports.com/napi/resource/game/lineup)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: playerTeamAssociations, injuries, metaData, playerStats. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_lineup()
```

_Last validated n/a._

## `cbs_game_odds`

Get an odds resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/odds/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/odds](https://api.cbssports.com/napi/resource/game/odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `marketIds` | `market_ids` |  |  | `Y` | This value can be used to specify markets, using the marketIds. |
| `bookIds` | `book_ids` |  |  | `Y` | This value can be used to specify books, using the bookIds. |
| `state` | `state` |  |  | `Y` | This value can be used to specify a state. |
| `model` | `model` |  |  | `Y` | This value can be used set the model to be used |
| `showHiddenOdds` | `show_hidden_odds` |  |  | `Y` | If set to 1, show the odds that has been hidden within the market and/or consensus nodes |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_odds()
```

_Last validated n/a._

## `cbs_game_odds_hq`

Get an HQ odds resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/odds/hq/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/odds/hq](https://api.cbssports.com/napi/resource/game/odds/hq)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_odds_hq()
```

_Last validated n/a._

## `cbs_game_outcomes`

Get an odds outcome for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/outcomes/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/outcomes](https://api.cbssports.com/napi/resource/game/outcomes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_outcomes()
```

_Last validated n/a._

## `cbs_game_probable_players`

Get a list of players who are probably playing in a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/probablePlayers/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/probablePlayers](https://api.cbssports.com/napi/resource/game/probablePlayers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: player, playerTeamAssociations, injuries, transactions, depthCharts, metaData. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_probable_players()
```

_Last validated n/a._

## `cbs_game_props`

Get game props for a game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/props/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/props](https://api.cbssports.com/napi/resource/game/props)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |
| `marketIds` | `market_ids` |  |  | `Y` | This value can be used to specify markets, using the marketIds. |
| `bookIds` | `book_ids` |  |  | `Y` | This value can be used to specify books, using the bookIds. |
| `propBetTypes` | `prop_bet_types` |  |  | `Y` | This value can be used to specify prop bet types, using the propBetTypes. Allowed: player, game, team. |
| `state` | `state` |  |  | `Y` | This value can be used to specify a state. |
| `includeInactiveMarkets` | `include_inactive_markets` |  |  | `Y` | This value can be used to filter out inactive markets. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_props()
```

_Last validated n/a._

## `cbs_game_rtwp`

Get a rtwp resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/rtwp/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/rtwp](https://api.cbssports.com/napi/resource/game/rtwp)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_rtwp()
```

_Last validated n/a._

## `cbs_game_ruwt_highlights`

Get the RUWT highlights resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/ruwtHighlights/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/ruwtHighlights](https://api.cbssports.com/napi/resource/game/ruwtHighlights)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_ruwt_highlights()
```

_Last validated n/a._

## `cbs_game_scoring_boxscores`

Get an scoring box scores resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/boxscores/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/boxscores](https://api.cbssports.com/napi/resource/game/scoring/boxscores)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_boxscores()
```

_Last validated n/a._

## `cbs_game_scoring_drives`

Get a drives resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/drives/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/drives](https://api.cbssports.com/napi/resource/game/scoring/drives)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_drives()
```

_Last validated n/a._

## `cbs_game_scoring_leaders`

Get an scoring leaders resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/leaders/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/leaders](https://api.cbssports.com/napi/resource/game/scoring/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_leaders()
```

_Last validated n/a._

## `cbs_game_scoring_player_stats`

Get an scoring player stats resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/playerStats/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/playerStats](https://api.cbssports.com/napi/resource/game/scoring/playerStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_player_stats()
```

_Last validated n/a._

## `cbs_game_scoring_plays`

Get an scoring plays resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/plays/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/plays](https://api.cbssports.com/napi/resource/game/scoring/plays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_plays()
```

_Last validated n/a._

## `cbs_game_scoring_rosters`

Get an scoring rosters resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/rosters/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/rosters](https://api.cbssports.com/napi/resource/game/scoring/rosters)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_rosters()
```

_Last validated n/a._

## `cbs_game_scoring_scoreboard`

Get an scoring scoreboard resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/scoreboard/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/scoreboard](https://api.cbssports.com/napi/resource/game/scoring/scoreboard)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_scoreboard()
```

_Last validated n/a._

## `cbs_game_scoring_scores`

Get an scoring scores resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/scores/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/scores](https://api.cbssports.com/napi/resource/game/scoring/scores)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_scores()
```

_Last validated n/a._

## `cbs_game_scoring_team_stats`

Get an scoring team stats resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/teamStats/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/teamStats](https://api.cbssports.com/napi/resource/game/scoring/teamStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_team_stats()
```

_Last validated n/a._

## `cbs_game_scoring_winprob`

Get an scoring winprob resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/winprob/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/winprob](https://api.cbssports.com/napi/resource/game/scoring/winprob)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_winprob()
```

_Last validated n/a._

## `cbs_game_scoring_ytd_player_stats`

Get an scoring YTD player stats resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/ytdPlayerStats/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/ytdPlayerStats](https://api.cbssports.com/napi/resource/game/scoring/ytdPlayerStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_ytd_player_stats()
```

_Last validated n/a._

## `cbs_game_scoring_ytd_team_stats`

Get an scoring YTD team stats resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/scoring/ytdTeamStats/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/scoring/ytdTeamStats](https://api.cbssports.com/napi/resource/game/scoring/ytdTeamStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_scoring_ytd_team_stats()
```

_Last validated n/a._

## `cbs_game_ticket`

Get a ticket resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/ticket/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/ticket](https://api.cbssports.com/napi/resource/game/ticket)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_ticket()
```

_Last validated n/a._

## `cbs_game_weather`

Get a Weather resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/game/weather/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/game/weather](https://api.cbssports.com/napi/resource/game/weather)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_game_weather()
```

_Last validated n/a._

## `cbs_golf_event_markets`

Get markets for an event.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/golf/event/markets/{event_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/golf/event/markets](https://api.cbssports.com/napi/resource/golf/event/markets)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | Numerical event ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_golf_event_markets()
```

_Last validated n/a._

## `cbs_golf_player_markets`

Get markets for a golfer.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/golf/player/markets/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/golf/player/markets/1751796](https://api.cbssports.com/napi/resource/golf/player/markets/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `eventId` | `event_id` |  |  | `Y` | View option.  Filter by eventId. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_golf_player_markets(player_id=1751796)
```

_Last validated n/a._

## `cbs_golfer_results`

Get golfer tournament results resource for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/golfer/results/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/golfer/results/1751796](https://api.cbssports.com/napi/resource/golfer/results/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonType. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_golfer_results(player_id=1751796)
```

_Last validated n/a._

## `cbs_league`

Get a league resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/league/{league_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/league/59](https://api.cbssports.com/napi/resource/league/59)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | Numerical league ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: teams, players, standings, conference, division, polls, teamSeasons. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_id` | integer | League identifier ('10' = WNBA). |
| `league_abbr` | character | Short CBS code for the league, e.g. NFL, NHL, NCAAB, EPL. |
| `league_name` | character | League name. |
| `sport_id` | integer | Sport MLBAM ID. |
| `league_type` | character | Single-character CBS classification code for the league; all seventeen captured leagues carry M and CBS does not publish the rest of the code set. |
| `teams` | character | Nested list of member-team membership spans. |
| `color_primary` | character | Primary brand colour of the league as six hex digits, inconsistently prefixed with a hash (#003369 for the NFL, 002D72 for MLB); null for the leagues CBS carries no palette for. |
| `color_secondary` | character | Secondary brand colour of the league as six hex digits, with the same inconsistent hash prefix as color_primary; null where CBS carries no palette. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_league(league_id=59)
```

_Last validated n/a._

## `cbs_league_teams`

Get team resources on a league with optional vendor overlay

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/league/teams/{league_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/league/teams/59](https://api.cbssports.com/napi/resource/league/teams/59)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | Numerical league Id - gets team from team table not teams for season |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: players, standings, conference, division, playerTeamAssociations, injuries, transactions, depthCharts, polls, teamSeasons, sportsLineStandings. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_league_teams(league_id=59)
```

_Last validated n/a._

## `cbs_odds`

Get an odds resource for a particular game.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/odds/{game_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/odds](https://api.cbssports.com/napi/resource/odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_id` | `game_id` |  | `Y` |  | Numerical game ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_odds()
```

_Last validated n/a._

## `cbs_player`

Get a player resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/1751796](https://api.cbssports.com/napi/resource/player/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional for any date field.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `year` | `year` |  |  | `Y` | Optional year in YYYY format (for Transactions only) |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: playerTeamAssociations, injuries, transactions, depthCharts, metaData, playerStats, standings, rankings, playerOutlook, draftInfo, combineData, positionRankings, gameStats, encyclopedia, golferResults, playerGolfMetadata, playerFutures, golferMarkets, recruitTeamAssociations, coachTeamAssociations, recruitRankings, coachRankings. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_combine_data`

Get draft related info for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/combineData/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/combineData/1751796](https://api.cbssports.com/napi/resource/player/combineData/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_combine_data(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_depth_charts`

Get depth charts for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/depthCharts/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/depthCharts/1751796](https://api.cbssports.com/napi/resource/player/depthCharts/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `position` | `position` |  |  | `Y` | A csv of positions to filter with |
| `pitchPos` | `pitch_pos` |  |  | `Y` | A csv of pitch positions to filter with (baseball only) |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_depth_charts(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_draft_info`

Get draft info for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/draftInfo/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/draftInfo/1751796](https://api.cbssports.com/napi/resource/player/draftInfo/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option, filter by seasonYear |
| `seasonType` | `season_type` |  |  | `Y` | View option, filter by seasonType Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option, filter by seasonId |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_draft_info(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_encyclopedia`

Get encyclopedia resource for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/encyclopedia/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/encyclopedia/1751796](https://api.cbssports.com/napi/resource/player/encyclopedia/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonType. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_encyclopedia(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_futures`

Get futures for a player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/futures/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/futures/1751796](https://api.cbssports.com/napi/resource/player/futures/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_futures(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_game_stats`

Get game stats for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/gameStats/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/gameStats/1751796](https://api.cbssports.com/napi/resource/player/gameStats/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `gameId` | `game_id` |  |  | `Y` | View option, filter by gameId |
| `seasonYear` | `season_year` |  |  | `Y` | Season Year in YYYY format |
| `seasonType` | `season_type` |  |  | `Y` | Csv list of pre, regular, or post Allowed: pre, regular, post. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_game_stats(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_hockey_meta`

Get the hockey meta data resource for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/hockey/meta/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/hockey/meta/1751796](https://api.cbssports.com/napi/resource/player/hockey/meta/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_hockey_meta(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_injuries`

Get injuries for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/injuries/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/injuries/1751796](https://api.cbssports.com/napi/resource/player/injuries/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_injuries(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_meta_baseball`

Get meta data associated to a particular baseball player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/meta/baseball/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/meta/baseball/1751796](https://api.cbssports.com/napi/resource/player/meta/baseball/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_meta_baseball(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_meta_golf`

Get a metadata resource for a particular golf player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/meta/golf/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/meta/golf/1751796](https://api.cbssports.com/napi/resource/player/meta/golf/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_meta_golf(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_outlook`

Get outlook for a player (context is now)

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/outlook/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/outlook/1751796](https://api.cbssports.com/napi/resource/player/outlook/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional format for dateCreated field. Available options here: http://momentjs.com/docs/#/displaying/format/ |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_outlook(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_position_rankings`

Get position rankings for a player

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/positionRankings/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/positionRankings/1751796](https://api.cbssports.com/napi/resource/player/positionRankings/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `position` | `position` |  |  | `Y` | Filter by position |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_position_rankings(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_rankings`

Get all rankings for a player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/rankings/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/rankings/1751796](https://api.cbssports.com/napi/resource/player/rankings/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `isCurrent` | `is_current` |  |  | `Y` | View option.  Only show stats for seasons where isCurrent is true. Allowed: 1. |
| `categories` | `categories` |  |  | `Y` | View option.  Only return the specified rankings categories. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_rankings(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_recruit_associations`

Get recruit associations for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/recruitAssociations/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/recruitAssociations/1751796](https://api.cbssports.com/napi/resource/player/recruitAssociations/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: team. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_recruit_associations(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_standings`

Get standings for a particular player

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/standings/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/standings/1751796](https://api.cbssports.com/napi/resource/player/standings/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `isCurrent` | `is_current` |  |  | `Y` | View option.  Only show standings for seasons where isCurrent is true. Allowed: 1. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: league. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_standings(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_stats`

Get all statistics for a player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/stats/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/stats/1751796](https://api.cbssports.com/napi/resource/player/stats/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `isCurrent` | `is_current` |  |  | `Y` | View option.  Only show stats for seasons where isCurrent is true. Allowed: 1. |
| `teamId` | `team_id` |  |  | `Y` | View option.  Filter by teamId. |
| `teamAbbr` | `team_abbr` |  |  | `Y` | View option.  Filter by a specific team abbreviation. |
| `isTotal` | `is_total` |  |  | `Y` | View option.  Filter only the isTotal record for players who played for multiple teams. Allowed: 1. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_stats(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_team_associations`

Get team associations for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/teamAssociations/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/teamAssociations/1751796](https://api.cbssports.com/napi/resource/player/teamAssociations/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `assocType` | `assoc_type` |  |  | `Y` | Filter associations by assoc type Allowed: C, H, S, F. |
| `rosterStatus` | `roster_status` |  |  | `Y` | Filter associations by roster status Allowed: ACT, NWT, MIN, MNR, RET, DEV, CUT, DIS, DL, IR, UFA, UDF, EXE, TRA, SUS, PUP, FA, RFA, KIA, INA. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: team. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_team_associations(player_id=1751796)
```

_Last validated n/a._

## `cbs_player_transactions`

Get transactions resource for a particular player.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/player/transactions/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/player/transactions/1751796](https://api.cbssports.com/napi/resource/player/transactions/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: targetTeam, currentTeam, fromTeam. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_player_transactions(player_id=1751796)
```

_Last validated n/a._

## `cbs_recruit_rankings`

Get rankings resource for a recruit.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/recruit/rankings/{player_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/recruit/rankings/1751796](https://api.cbssports.com/napi/resource/recruit/rankings/1751796)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `player_id` | `player_id` |  | `Y` |  | Numerical player ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_recruit_rankings(player_id=1751796)
```

_Last validated n/a._

## `cbs_season`

Get a season resource from Atlas.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/season/{season_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/season/59](https://api.cbssports.com/napi/resource/season/59)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | Numerical season ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: sport, league, teams. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_season(season_id=59)
```

_Last validated n/a._

## `cbs_season_teams`

Get team resources associated to a season

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/season/teams/{season_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/season/teams/59](https://api.cbssports.com/napi/resource/season/teams/59)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season_id` | `season_id` |  | `Y` |  | Optional seasonYear for leagues that change teams each year. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: players, standings, conference, division, playerTeamAssociations, injuries, transactions, depthCharts, polls, teamSeasons, sportsLineStandings, league. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `stub_hub_team_id` | integer | StubHub performer id for the team, which is the key behind ticket_url; entirely null for the ten soccer leagues and populated only for MLB, MLS, NBA, NCAAB, NCAAF, NFL and NHL, so the port pins it back to Int64 after pandas widens the nullable column to float. |
| `location` | character | Either Home if the home team is playing in their home stadium, or Neutral if the game is being played at a neutral location. This still shows as Home for games between the Giants and Jets even though they share the same home stadium. |
| `nick_name` | character | Player nickname. |
| `medium_name` | character | Medium-length display name for the team, sitting between short_name and the full location plus nickname (Arizona for the Cardinals, Duke University, Werder Bremen). |
| `short_name` | character | Short display name. |
| `abbrev` | character | Team abbreviation. |
| `status` | character | Status label. |
| `home_venue_id` | integer | Unique identifier for home venue. |
| `conference_id` | integer | Conference identifier. |
| `league_id` | integer | League identifier ('10' = WNBA). |
| `division_id` | integer | Division MLBAM ID. |
| `ticket_url` | character | StubHub ticket-purchase URL for the team, either a bare /performer/{id} link or a slugged team-tickets link; an empty string for the leagues where CBS carries no StubHub performer. |
| `color_hex_dex` | character | Six-digit team colour drawn from CBS's own colour index, with no leading hash and an empty string where unset; it can differ slightly from color_primary_hex (96223E against 97233f for the Arizona Cardinals). |
| `color_primary_hex` | character | Team's primary colour as six hex digits with no leading hash, e.g. 97233f. |
| `color_secondary_hex` | character | Team's secondary colour as six hex digits with no leading hash, e.g. 000000. |
| `players` | character | Nested list of per-player box scores. |
| `league` | character | League slug. |
| `standings` | character | Nested standings sub-resource for the team, JSON-encoded when present; null unless the request asked for it through the endpoint's resources parameter, which defaults to none. |
| `conference` | character | Conference name. |
| `division` | character | Team division. |
| `team_seasons` | character | Nested list of the team's season records, JSON-encoded when present; null unless requested through the resources parameter. |
| `polls` | character | Nested poll-ranking sub-resource for the team, JSON-encoded when present; null unless requested through the resources parameter. |
| `home_venue` | character | Nested venue record for the team's home site, JSON-encoded when present; null unless requested through the resources parameter, with home_venue_id always carrying the id. |
| `sports_line_standings` | character | Nested SportsLine standings sub-resource for the team, JSON-encoded when present; null unless requested through the resources parameter. |
| `team_stats` | character | Nested team-statistics sub-resource, JSON-encoded when present; null unless requested through the resources parameter. |
| `team_rankings` | character | Nested team-rankings sub-resource, JSON-encoded when present; null unless requested through the resources parameter. |
| `sports_line_rankings` | character | Nested SportsLine rankings sub-resource, JSON-encoded when present; null unless requested through the resources parameter. |
| `meta_tsa_overlay` | logical | Flag on the record's meta block marking the team as carrying CBS's TSA overlay; true for every team across the captured leagues. |
| `meta_season_id` | integer | Season identifier CBS attaches to each team record's meta block, a small league-scoped number (2 for MLB, 18 for the NFL, 35 for the Premier League) that is constant across every team in one response and lives in a different id space from the season_id on standings rows. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_season_teams(season_id=59)
```

_Last validated n/a._

## `cbs_sport`

Get a sport resource from Atlas.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/sport/{sport_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/sport/1](https://api.cbssports.com/napi/resource/sport/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  | `Y` |  | Numerical sport ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: leagues. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_sport(sport_id=1)
```

_Last validated n/a._

## `cbs_sport_leagues`

Get league resources for a sport.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/sport/leagues/{sport_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/sport/leagues/1](https://api.cbssports.com/napi/resource/sport/leagues/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  | `Y` |  | Numerical league ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_sport_leagues(sport_id=1)
```

_Last validated n/a._

## `cbs_team_futures`

Get futures for a team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/futures/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/futures/404](https://api.cbssports.com/napi/resource/team/futures/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_futures(team_id=404)
```

_Last validated n/a._

## `cbs_team_metadata`

Get a team metadata resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/metadata/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/metadata/404](https://api.cbssports.com/napi/resource/team/metadata/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: team. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_metadata(team_id=404)
```

_Last validated n/a._

## `cbs_team_players`

Get player resources on a team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/players/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/players/404](https://api.cbssports.com/napi/resource/team/players/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: playerTeamAssociations, injuries, transactions, depthCharts. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `full_first_name` | character | Player's full given name including any middle names CBS records (Kieran James Ricardo); null throughout the US leagues and populated mainly in the soccer leagues. |
| `last_name` | character | Player's last name. |
| `full_last_name` | character | Player's full family name as CBS records it, which can be longer than the display last_name; null throughout the US leagues and populated mainly in soccer. |
| `nick_name` | character | Player nickname. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | integer | Player weight in pounds. |
| `experience` | integer | Years of professional experience. |
| `school` | character | Team name. |
| `home_town` | character | Home town of the player. |
| `debut` | character | Date of the player's debut for the team as CBS records it; null for every player in the captured leagues, so the shipped format is unverified. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `birth_country` | character | Player birth country. |
| `birth_country_code` | character | Lowercase three-letter code for the player's country of birth, e.g. eng, wal, fra; populated in the soccer leagues and null in the US ones. |
| `nationality_country` | character | Country the player represents internationally, spelled out (England, Wales, France); can differ from birth_country for dual-eligible players. |
| `nationality_country_code` | character | Lowercase three-letter code matching nationality_country, e.g. eng. |
| `locked` | integer | Flag CBS sets on a player record it treats as locked (0 or 1); 0 for nearly every player in the captures. |
| `player_team_associations` | character | Nested list of the player's team associations, JSON-encoded when present; null unless the request named it in the endpoint's resources parameter, which defaults to none. |
| `injuries` | character | Nested injury records for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `transactions` | character | Nested transaction records for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `depth_charts` | character | Nested depth-chart entries for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `position_rankings` | character | Nested positional-ranking entries for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `player_stats` | character | Nested statistical lines for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `standings` | character | Nested standings sub-resource attached to the player's team, JSON-encoded when present; null unless requested through the resources parameter. |
| `rankings` | character | Nested ranking entries for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `player_outlook` | character | Nested fantasy-outlook copy for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `meta_data` | character | Nested metadata block for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `draft_info` | character | Draft information. |
| `game_stats` | character | Nested per-game statistical lines for the player, JSON-encoded when present; null unless requested through the resources parameter. |
| `combine_data` | character | Nested scouting-combine measurements for the player, JSON-encoded when present; null unless requested through the resources parameter. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_players(team_id=404)
```

_Last validated n/a._

## `cbs_team_polls`

Retrieve team rankings data from our various polls.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/polls/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/polls/404](https://api.cbssports.com/napi/resource/team/polls/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `polls` | `polls` |  |  | `Y` | View option.  Filter by a certain poll name. Allowed: coaches, ap, fcscoachespoll, statstsnfcspoll, rpi, playoffselectioncommitteepoll, net. |
| `seasonId` | `season_id` |  |  | `Y` | View option. Filter by seasonId. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_polls(team_id=404)
```

_Last validated n/a._

## `cbs_team_rankings`

Get rankings for a team (by season)

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/rankings/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/rankings/404](https://api.cbssports.com/napi/resource/team/rankings/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team id |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_rankings(team_id=404)
```

_Last validated n/a._

## `cbs_team_rankings_sportsline`

Get sportsline rankings for a team

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/rankings/sportsline/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/rankings/sportsline/404](https://api.cbssports.com/napi/resource/team/rankings/sportsline/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team id |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_rankings_sportsline(team_id=404)
```

_Last validated n/a._

## `cbs_team_seasons`

Get a list of Season resources associated to a Team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/seasons/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/seasons/404](https://api.cbssports.com/napi/resource/team/seasons/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve.  Defaults to none. Allowed: league. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_seasons(team_id=404)
```

_Last validated n/a._

## `cbs_team_standings`

Get standings for a particular team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/standings/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/standings/404](https://api.cbssports.com/napi/resource/team/standings/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `year` | `year` |  |  | `Y` | Optional year in YYYY format |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. v3 only! Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. v3 only! |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `streak` | character | Current streak (e.g. 'W3' for three-game win streak). |
| `win_loss_record` | character | JSON-encoded array of the team's split records, one object per split carrying wins, losses, ties (plus shootout and overtime splits in the NHL) with a name and a type such as home, away, division, conference or last5. |
| `season_id` | integer | Unique season identifier. |
| `wins_number` | integer | Wins the team recorded over the season and season type of this row. |
| `goals_for_goals` | integer | Goals the team scored, on the soccer and hockey standings shapes. |
| `last_results_r2` | character | Second of the five form-guide slots CBS publishes for a soccer team, reported as Win, Loss or Draw. |
| `last_results_r3` | character | Third of the five form-guide slots CBS publishes for a soccer team, reported as Win, Loss or Draw. |
| `last_results_r4` | character | Fourth of the five form-guide slots CBS publishes for a soccer team, reported as Win, Loss or Draw. |
| `last_results_r5` | character | Fifth of the five form-guide slots CBS publishes for a soccer team, reported as Win, Loss or Draw. |
| `last_results_r1` | character | One of the team's five most recent results in the soccer standings form guide, reported as Win, Loss or Draw; CBS labels the five slots r1 through r5 without documenting which end is the most recent match. |
| `winning_percentage_percentage` | character | Winning percentage as CBS formats it, a leading-dot three-decimal string such as .706. |
| `goals_against_goals` | integer | Goals conceded by the team, on the soccer and hockey standings shapes. |
| `losses_number` | integer | Losses the team recorded over the season and season type of this row. |
| `points_penalty_points` | integer | Table points deducted from the team as a sanction, on the soccer standings shape; 0 for teams with no deduction. |
| `points_points_per_game` | character | Table points per game played, on the soccer standings shape, e.g. 2.33. |
| `points_points` | integer | League table points the team has earned, on the soccer standings shape (three per win, one per draw). |
| `ties_number` | character | Ties (draws) the team recorded; 0 in the leagues that no longer play to a tie, and typed as text because CBS emits an empty string where the concept does not apply. |
| `games_played_games` | integer | Games the team has played in the season and season type of this row. |
| `place_previous` | integer | Position the team held in the previously published version of the same table, so movement can be shown; equal to place_place when the team did not move. |
| `place_season_end_id` | character | Numeric code paired with place_season_end (7 for Europa League, 4 for Champions League, 9 for Relegation); an empty string when no end-of-season label applies. |
| `place_season_end` | character | End-of-season outcome CBS attaches to the team's finishing position on the soccer standings shape, e.g. Champions League, Europa League Qualifying, Relegation Playoffs; an empty string when the finish carries no label. |
| `place_place` | integer | Position the team occupies in the standings table it is ranked within, 1 being top. |
| `clinch_status_id` | integer | Numeric code for the qualification or clinch status CBS assigns the team on the soccer standings shape, paired with clinch_status_status. |
| `clinch_status_status` | character | Qualification or clinch status spelled out on the soccer standings shape, e.g. Advanced to Knockout Stage or Europa League. |
| `team_info_display_name` | character | Display name CBS uses for the team on the soccer standings shape, e.g. Arsenal. |
| `team_info_name` | character | Nickname portion of the team's name on the soccer standings shape; an empty string for the soccer clubs, which carry their identity in team_info_display_name. |
| `team_info_alias` | character | Short alias CBS uses for the team on the soccer standings shape, e.g. ARS. |
| `team_info_location` | character | Location portion of the team's name on the soccer standings shape, which for many clubs repeats the club name rather than a city. |
| `team_info_id` | integer | CBS team identifier carried on the soccer standings shape's team-info block; league-scoped rather than global, and paired with team_info_global_id. |
| `team_info_global_id` | integer | CBS global team identifier on the soccer standings shape, stable across leagues and seasons where team_info_id is not. |
| `season_season_id` | integer | CBS season identifier for the standings row, an eight-digit id on modern rows (29444245 for the 2025 NFL regular season) and a small legacy number on the oldest ones. |
| `season_sport_id` | integer | CBS sport identifier for the season (1 football, 2 baseball, 3 basketball, 4 hockey, 5 soccer). |
| `season_league_id` | integer | CBS league identifier for the season, e.g. 59 for the NFL, 60 for the NHL, 52 for MLB. |
| `season_league` | character | League record nested inside the season block; CBS returns it as null on every captured standings payload. |
| `season_teams` | character | Team list nested inside the season block; CBS returns it as an empty array on standings payloads, JSON-encoded to []. |
| `season_season_year` | integer | Calendar year CBS keys the season by, matching the year key the standings block was nested under. |
| `season_is_current` | integer | Flag marking the season as the one currently under way (1 current, 0 historical). |
| `season_season_type` | character | Portion of the season the row covers, one of pre, regular or post. |
| `season_season_type_desc` | character | Human-readable label for season_season_type, e.g. Regular season. |
| `season_season_start_date` | character | First day of the season formatted MM-DD-YYYY HH:MM:SS with a UTC offset, e.g. 09-04-2025 00:00:00 -0400. |
| `season_season_end_date` | character | Last day of the season in the same MM-DD-YYYY HH:MM:SS plus UTC-offset format, e.g. 01-04-2026 23:59:59 -0500. |
| `goal_differential_differential` | integer | Goals scored minus goals conceded, on the soccer standings shape. |
| `clinched_division_clinched_id` | integer | Numeric mirror of clinched_division_clinched (1 clinched, 0 not). |
| `clinched_division_clinched` | character | Whether the team has clinched its division. |
| `team_code_id` | integer | League-scoped team code on the US-league standings shape, a small per-league sequence (3 for the Angels, 34 for the Texans) rather than the global id. |
| `team_code_global_id` | integer | CBS global team identifier on the US-league standings shape, e.g. 227 for the Angels and 325 for the Texans. |
| `team_city_city` | character | City CBS attributes the team to on the US-league standings shape, e.g. Houston. |
| `clinched_playoff_spot_clinched_id` | integer | Numeric mirror of clinched_playoff_spot_clinched (1 clinched, 0 not). |
| `clinched_playoff_spot_clinched` | character | Whether the team has clinched a playoff berth, on the MLB standings shape. |
| `games_back_number` | character | Games behind the leader of the team's standings group; typed as text because CBS emits a dash on rows where it does not compute one. |
| `division_rank_tied` | character | Whether the team's division rank is shared with another team. |
| `division_rank_rank` | integer | Team's rank within its division, 1 being best. |
| `division_rank_tied_id` | integer | Numeric mirror of division_rank_tied (1 tied, 0 not). |
| `streak_kind` | character | Direction of the team's current run when CBS returns a single streak object, e.g. winning or losing (the NHL returns an array instead, kept in the streak column). |
| `streak_games` | integer | Length in games of the current run described by streak_kind. |
| `wild_card_rank_tied` | character | Whether the team's wild-card rank is shared with another team. |
| `wild_card_rank_rank` | integer | Team's rank in the wild-card race, on the MLB and NHL standings shapes. |
| `wild_card_rank_tied_id` | integer | Numeric mirror of wild_card_rank_tied (1 tied, 0 not). |
| `today_games_included_through` | character | Whether the standings figures already account for games played today. |
| `today_games_included_through_id` | integer | Numeric mirror of today_games_included_through (1 included, 0 not). |
| `eliminated_from_playoffs_eliminated` | character | Whether the team has been eliminated from playoff contention. |
| `wc_games_back_number` | character | Games behind the last wild-card position, on the MLB standings shape. |
| `team_name_name` | character | Nickname CBS uses for the team on the US-league standings shape, e.g. Texans or Angels. |
| `team_name_alias` | character | Short alias for the team on the US-league standings shape, e.g. Hou or LAA. |
| `elimination_number_number` | character | Elimination number for the team, the combined team losses and rival wins that would end its contention; 0 once the outcome is settled, and typed as text because CBS emits an empty string where it does not compute one. |
| `wc_elimination_number_number` | integer | Elimination number for the team's wild-card contention specifically, on the MLB standings shape. |
| `runs_allowed` | integer | Runs the team conceded, on the MLB standings shape. |
| `runs_scored` | integer | Runs the team scored, on the MLB standings shape. |
| `league_rank_tied` | character | Whether the team's league rank is shared with another team. |
| `league_rank_rank` | integer | Team's rank across the whole league, 1 being best. |
| `league_rank_tied_id` | integer | Numeric mirror of league_rank_tied (1 tied, 0 not). |
| `place_conference_rank` | integer | Rank the team holds within its conference, carried on the place block of the MLS standings shape. |
| `place_division_rank` | character | Rank the team holds within its division, carried on the place block of the MLS standings shape; an empty string for leagues or seasons without divisions. |
| `conference_conference_id` | integer | CBS conference identifier for the team's conference, paired with the conference name and abbreviation on the same block. |
| `conference_name` | character | Full conference name. |
| `conference_abbreviation` | character | Conference abbreviation. |
| `basketball_nba_playoffs_indicator` | character | JSON-encoded array of the NBA clinching markers CBS attaches to the team, each an object with a type such as clinched-playoffs, division-first or conference-first. |
| `points_for_per_game_points` | numeric | Points the team scored per game, on the NBA standings shape, e.g. 120.5. |
| `magic_number_number` | character | Magic number CBS publishes for the team's clinching scenario; 0 or negative once the scenario no longer applies to a clinched team, and an empty string on rows where CBS computes none (preseason blocks). |
| `conference_games_back_games` | integer | Games behind the conference leader, on the NBA standings shape. |
| `points_against_per_game_points` | numeric | Points the team conceded per game, on the NBA standings shape, e.g. 107.6. |
| `conference_seed_seed` | character | Team's current seeding within its conference bracket. |
| `conference_eos_seed_seed` | integer | Team's end-of-season conference seed, CBS's settled bracket position once the regular season is complete. |
| `points_for` | character | Goals/points scored. |
| `points_against` | character | Points allowed. |
| `won_conference_tournament_won` | logical | Whether the team won its conference tournament, on the NCAA standings shape. |
| `rpi_rank` | integer | Team's national rank by the rpi_rpi rating, 1 being best. |
| `rpi_rpi` | character | Ratings Percentage Index for the team on the NCAA standings shape, a leading-dot four-decimal string such as .4021. |
| `sequence_sequence` | integer | Sort position CBS assigns the team within its standings group; it tracks place_place closely but can sit one higher where teams are tied. |
| `clinched_conference_clinched` | logical | Whether the team has clinched its conference. |
| `college_code_id` | integer | CBS college identifier for the school on the NCAA standings shape, e.g. 2120 for USC Upstate. |
| `ineligible_ineligible` | logical | Whether the team is ineligible for postseason play, on the NCAA standings shape. |
| `sos_sos` | character | Strength-of-schedule rating on the NCAA standings shape, a leading-dot four-decimal string such as .4528. |
| `sos_rank` | integer | Team's rank by the sos_sos rating, 1 being the toughest schedule. |
| `college_name_name` | character | School name CBS uses on the NCAA standings shape, e.g. USC Upstate. |
| `ranking_ranking` | integer | Poll ranking CBS carries for the team on the NCAA standings shape; 0 across the captured rows, which is CBS's stand-in for unranked. |
| `net_ranking_rank` | integer | Team's NET ranking on the NCAA basketball standings shape, 1 being best. |
| `conference_games_back_number` | integer | Games behind the conference leader, on the NCAA standings shape, where CBS names the same quantity number rather than games. |
| `ranking_playoff_ranking` | integer | Playoff-committee ranking CBS carries for the team on the NCAA football standings shape; 0 across the captured rows, which is CBS's stand-in for unranked. |
| `clinched_playoffs_clinched` | logical | Whether the team has clinched a playoff berth, on the NFL standings shape. |
| `strength_of_schedule_rank` | character | Team's rank by strength of schedule on the NFL standings shape; typed as text because CBS emits a dash on rows where it publishes none. |
| `points_for_number` | integer | Points the team scored, on the NFL standings shape. |
| `points_against_number` | integer | Points the team conceded, on the NFL standings shape. |
| `clinched_home_field_clinched` | logical | Whether the team has clinched home-field advantage through the playoffs, on the NFL standings shape. |
| `clinched_first_round_bye_clinched` | character | Whether the team has clinched a first-round playoff bye, on the NFL standings shape. |
| `content` | character | Raw markup fragment CBS emits on some NFL standings blocks; the captured rows carry only the string /> and it holds no standings meaning. |
| `clinched_playoffs_date_date` | integer | Day of the month on which the team clinched a playoff berth. |
| `clinched_playoffs_date_month` | integer | Month of the date on which the team clinched a playoff berth, 1 through 12. |
| `clinched_playoffs_date_year` | integer | Year of the date on which the team clinched a playoff berth, on the NFL standings shape. |
| `clinched_playoffs_date_day` | integer | Day-of-week component CBS emits alongside the clinch date; the one captured NFL row pairs 6 with Saturday 12-27-2025, an ISO Monday-is-1 index. |
| `overtime_losses_number` | integer | Losses the team took in overtime, on the NHL standings shape. |
| `regulation_plus_overtime_wins_number` | integer | Wins the team earned in regulation or overtime, excluding shootout wins, on the NHL standings shape. |
| `shootout_losses_number` | integer | Losses the team took in a shootout, on the NHL standings shape. |
| `overtime_wins_number` | integer | Wins the team earned in overtime, on the NHL standings shape. |
| `team_points_number` | integer | Standings points the team has accumulated on the NHL shape (two per win, one per overtime or shootout loss). |
| `shootout_wins_number` | integer | Wins the team earned in a shootout, on the NHL standings shape. |
| `team_city_alternate` | character | Alternate city spelling CBS carries for the team on the NHL standings shape, which usually repeats team_city_city. |
| `hockey_nhl_conference_ranking_ranking` | character | Team's ranking within its NHL conference; typed as text because CBS emits both integers and zero-padded strings such as 05. |
| `hockey_nhl_playoffs_indicator_type` | character | Single NHL clinching marker for rows where CBS sends one object rather than an array, e.g. clinched-playoffs. |
| `regulation_wins_number` | integer | Wins the team earned in regulation time, on the NHL standings shape. |
| `hockey_nhl_playoffs_indicator` | character | JSON-encoded array of the NHL clinching markers CBS attaches to the team, each an object with a type such as clinched-playoffs, division-first, conference-first or presidents' trophy; rows where CBS sends a single object instead land in hockey_nhl_playoffs_indicator_type. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_standings(team_id=404)
```

_Last validated n/a._

## `cbs_team_standings_sportsline`

Get SportsLine standings for a particular team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/standings/sportsline/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/standings/sportsline/404](https://api.cbssports.com/napi/resource/team/standings/sportsline/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `dateFormat` | `date_format` |  |  | `Y` | Optional.  Options here: http://momentjs.com/docs/#/displaying/format/ |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_standings_sportsline(team_id=404)
```

_Last validated n/a._

## `cbs_team_stats`

Get all statistics for a team.

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/team/stats/{team_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/team/stats/404](https://api.cbssports.com/napi/resource/team/stats/404)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | Numerical team ID |
| `seasonYear` | `season_year` |  |  | `Y` | View option.  Filter by seasonYear. |
| `seasonType` | `season_type` |  |  | `Y` | View option.  Filter by seasonType. Allowed: regular, pre, post. |
| `seasonId` | `season_id` |  |  | `Y` | View option.  Filter by seasonId. |
| `isCurrent` | `is_current` |  |  | `Y` | View option.  Only show stats for seasons where isCurrent is true. Allowed: 1. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_team_stats(team_id=404)
```

_Last validated n/a._

## `cbs_venue`

Get a venue resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/venue/{venue_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/venue](https://api.cbssports.com/napi/resource/venue)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | Numerical venue ID |
| `resources` | `resources` |  |  | `Y` | Specify specific sub-resources to resolve. Defaults to none. Allowed: metaData. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_venue()
```

_Last validated n/a._

## `cbs_venue_metadata`

Get a venues metadata resource

**Endpoint URL:** `GET https://api.cbssports.com/napi/resource/venue/metadata/{venue_id}`

**Valid URL:** [https://api.cbssports.com/napi/resource/venue/metadata](https://api.cbssports.com/napi/resource/venue/metadata)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | Numerical venue ID |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cbs_napi`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cbs_venue_metadata()
```

_Last validated n/a._
