---
title: CFB — College Football Data API (api.collegefootballdata.com, free API key)
sidebar_label: College Football Data API (api.collegefootballdata.com, free API key)
description: "CFB — College Football Data API (api.collegefootballdata.com, free API key) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 13
---
# CFB — College Football Data API (api.collegefootballdata.com, free API key)

`sportsdataverse.cfb` — 58 endpoints.

## `cfbd_calendar`

GET /calendar — Retrieves calendar information

**Endpoint URL:** `GET https://api.collegefootballdata.com/calendar`

**Valid URL:** [https://api.collegefootballdata.com/calendar](https://api.collegefootballdata.com/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_calendar()
```

_Last validated n/a._

## `cfbd_coaches`

GET /coaches — Retrieves historical head coach information and records

**Endpoint URL:** `GET https://api.collegefootballdata.com/coaches`

**Valid URL:** [https://api.collegefootballdata.com/coaches](https://api.collegefootballdata.com/coaches)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `firstName` | `first_name` |  |  | `Y` | Optional first name filter |
| `lastName` | `last_name` |  |  | `Y` | Optional last name filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `year` | `year` |  |  | `Y` | Optional year filter |
| `minYear` | `min_year` |  |  | `Y` | Optional start year range filter |
| `maxYear` | `max_year` |  |  | `Y` | Optional end year range filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_coaches()
```

_Last validated n/a._

## `cfbd_conferences`

GET /conferences — Retrieves list of conferences

**Endpoint URL:** `GET https://api.collegefootballdata.com/conferences`

**Valid URL:** [https://api.collegefootballdata.com/conferences](https://api.collegefootballdata.com/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_conferences()
```

_Last validated n/a._

## `cfbd_draft_picks`

GET /draft/picks — Retrieve historical NFL draft data

**Endpoint URL:** `GET https://api.collegefootballdata.com/draft/picks`

**Valid URL:** [https://api.collegefootballdata.com/draft/picks](https://api.collegefootballdata.com/draft/picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `team` | `team` |  |  | `Y` | Optional NFL team filter |
| `school` | `school` |  |  | `Y` | Optional college team filter |
| `conference` | `conference` |  |  | `Y` | Optional college conference filter |
| `position` | `position` |  |  | `Y` | Optional position classification filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_draft_picks()
```

_Last validated n/a._

## `cfbd_draft_positions`

GET /draft/positions — Retrieves list of player position categories for the NFL Draft

**Endpoint URL:** `GET https://api.collegefootballdata.com/draft/positions`

**Valid URL:** [https://api.collegefootballdata.com/draft/positions](https://api.collegefootballdata.com/draft/positions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_draft_positions()
```

_Last validated n/a._

## `cfbd_draft_teams`

GET /draft/teams — Retrieves list of NFL teams

**Endpoint URL:** `GET https://api.collegefootballdata.com/draft/teams`

**Valid URL:** [https://api.collegefootballdata.com/draft/teams](https://api.collegefootballdata.com/draft/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_draft_teams()
```

_Last validated n/a._

## `cfbd_drives`

GET /drives — Retrieves historical drive data

**Endpoint URL:** `GET https://api.collegefootballdata.com/drives`

**Valid URL:** [https://api.collegefootballdata.com/drives](https://api.collegefootballdata.com/drives)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `week` | `week` |  |  | `Y` | Optional week filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `offense` | `offense` |  |  | `Y` | Optional offensive team filter |
| `defense` | `defense` |  |  | `Y` | Optional defensive team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |
| `offenseConference` | `offense_conference` |  |  | `Y` | Optional offensive team conference filter |
| `defenseConference` | `defense_conference` |  |  | `Y` | Optional defensive team conference filter |
| `classification` | `classification` |  |  | `Y` | Optional division classification filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_drives()
```

_Last validated n/a._

## `cfbd_game_box_advanced`

GET /game/box/advanced — Retrieves an advanced box score for a game

**Endpoint URL:** `GET https://api.collegefootballdata.com/game/box/advanced`

**Valid URL:** [https://api.collegefootballdata.com/game/box/advanced](https://api.collegefootballdata.com/game/box/advanced)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Required game id filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_game_box_advanced()
```

_Last validated n/a._

## `cfbd_games`

GET /games — Retrieves historical game data

**Endpoint URL:** `GET https://api.collegefootballdata.com/games`

**Valid URL:** [https://api.collegefootballdata.com/games](https://api.collegefootballdata.com/games)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Required year filter (except when id is specified) |
| `week` | `week` |  |  | `Y` | Optional week filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `classification` | `classification` |  |  | `Y` | Optional division classification filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `home` | `home` |  |  | `Y` | Optional home team filter |
| `away` | `away` |  |  | `Y` | Optional away team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |
| `id` | `id` |  |  | `Y` | Game id filter to retrieve a single game |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_games()
```

_Last validated n/a._

## `cfbd_games_media`

GET /games/media — Retrieves media information for games

**Endpoint URL:** `GET https://api.collegefootballdata.com/games/media`

**Valid URL:** [https://api.collegefootballdata.com/games/media](https://api.collegefootballdata.com/games/media)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `week` | `week` |  |  | `Y` | Optional week filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |
| `mediaType` | `media_type` |  |  | `Y` | Optional media type filter |
| `classification` | `classification` |  |  | `Y` | Optional division classification filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_games_media()
```

_Last validated n/a._

## `cfbd_games_players`

GET /games/players — Retrieves player box score statistics

**Endpoint URL:** `GET https://api.collegefootballdata.com/games/players`

**Valid URL:** [https://api.collegefootballdata.com/games/players](https://api.collegefootballdata.com/games/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Required year filter (along with one of week, team, or conference), unless id is specified |
| `week` | `week` |  |  | `Y` | Optional week filter, required if team and conference not specified |
| `team` | `team` |  |  | `Y` | Optional team filter, required if week and conference not specified |
| `conference` | `conference` |  |  | `Y` | Optional conference filter, required if week and team not specified |
| `classification` | `classification` |  |  | `Y` | Optional division classification filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `category` | `category` |  |  | `Y` | Optional player statistical category filter |
| `id` | `id` |  |  | `Y` | Optional id filter to retrieve a single game |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_games_players()
```

_Last validated n/a._

## `cfbd_games_teams`

GET /games/teams — Retrieves team box score statistics

**Endpoint URL:** `GET https://api.collegefootballdata.com/games/teams`

**Valid URL:** [https://api.collegefootballdata.com/games/teams](https://api.collegefootballdata.com/games/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Required year filter (along with one of week, team, or conference), unless id is specified |
| `week` | `week` |  |  | `Y` | Optional week filter, required if team and conference not specified |
| `team` | `team` |  |  | `Y` | Optional team filter, required if week and conference not specified |
| `conference` | `conference` |  |  | `Y` | Optional conference filter, required if week and team not specified |
| `classification` | `classification` |  |  | `Y` | Optional division classification filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `id` | `id` |  |  | `Y` | Optional id filter to retrieve a single game |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_games_teams()
```

_Last validated n/a._

## `cfbd_games_weather`

GET /games/weather — Retrieve historical and future weather data (Patreon only)

**Endpoint URL:** `GET https://api.collegefootballdata.com/games/weather`

**Valid URL:** [https://api.collegefootballdata.com/games/weather](https://api.collegefootballdata.com/games/weather)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if game id not specified |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `week` | `week` |  |  | `Y` | Optional week filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |
| `classification` | `classification` |  |  | `Y` | Optional division classification filter |
| `gameId` | `game_id` |  |  | `Y` | Filter for retrieving a single game |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_games_weather()
```

_Last validated n/a._

## `cfbd_info`

GET /info — Retrieves information about the user, including their Patreon level and remaining API calls.

**Endpoint URL:** `GET https://api.collegefootballdata.com/info`

**Valid URL:** [https://api.collegefootballdata.com/info](https://api.collegefootballdata.com/info)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_info()
```

_Last validated n/a._

## `cfbd_lines`

GET /lines — Retrieves historical betting data

**Endpoint URL:** `GET https://api.collegefootballdata.com/lines`

**Valid URL:** [https://api.collegefootballdata.com/lines](https://api.collegefootballdata.com/lines)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  |  | `Y` | Optional gameId filter |
| `year` | `year` |  |  | `Y` | Year filter, required if game id not specified |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `week` | `week` |  |  | `Y` | Optional week filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `home` | `home` |  |  | `Y` | Optional home team filter |
| `away` | `away` |  |  | `Y` | Optional away team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |
| `provider` | `provider` |  |  | `Y` | Optional provider name filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_lines()
```

_Last validated n/a._

## `cfbd_live_plays`

GET /live/plays — Queries live play-by-play data and advanced stats

**Endpoint URL:** `GET https://api.collegefootballdata.com/live/plays`

**Valid URL:** [https://api.collegefootballdata.com/live/plays](https://api.collegefootballdata.com/live/plays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  | `Y` |  | Game Id filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_live_plays()
```

_Last validated n/a._

## `cfbd_metrics_fg_ep`

GET /metrics/fg/ep — Queries field goal expected points values

**Endpoint URL:** `GET https://api.collegefootballdata.com/metrics/fg/ep`

**Valid URL:** [https://api.collegefootballdata.com/metrics/fg/ep](https://api.collegefootballdata.com/metrics/fg/ep)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_metrics_fg_ep()
```

_Last validated n/a._

## `cfbd_metrics_wp`

GET /metrics/wp — Query play win probabilities by game

**Endpoint URL:** `GET https://api.collegefootballdata.com/metrics/wp`

**Valid URL:** [https://api.collegefootballdata.com/metrics/wp](https://api.collegefootballdata.com/metrics/wp)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  | `Y` |  | Required game ID filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_metrics_wp()
```

_Last validated n/a._

## `cfbd_metrics_wp_pregame`

GET /metrics/wp/pregame — Queries pregame win probabilities

**Endpoint URL:** `GET https://api.collegefootballdata.com/metrics/wp/pregame`

**Valid URL:** [https://api.collegefootballdata.com/metrics/wp/pregame](https://api.collegefootballdata.com/metrics/wp/pregame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `week` | `week` |  |  | `Y` | Optional week filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `team` | `team` |  |  | `Y` | Optional team filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_metrics_wp_pregame()
```

_Last validated n/a._

## `cfbd_player_portal`

GET /player/portal — Retrieves transfer portal data for a given year

**Endpoint URL:** `GET https://api.collegefootballdata.com/player/portal`

**Valid URL:** [https://api.collegefootballdata.com/player/portal](https://api.collegefootballdata.com/player/portal)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_player_portal()
```

_Last validated n/a._

## `cfbd_player_returning`

GET /player/returning — Retrieves returning production data. Either a year or team filter must be specified.

**Endpoint URL:** `GET https://api.collegefootballdata.com/player/returning`

**Valid URL:** [https://api.collegefootballdata.com/player/returning](https://api.collegefootballdata.com/player/returning)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if team not specified |
| `team` | `team` |  |  | `Y` | Team filter, required if year not specified |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_player_returning()
```

_Last validated n/a._

## `cfbd_player_search`

GET /player/search — Search for players (lists top 100 results)

**Endpoint URL:** `GET https://api.collegefootballdata.com/player/search`

**Valid URL:** [https://api.collegefootballdata.com/player/search](https://api.collegefootballdata.com/player/search)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `searchTerm` | `search_term` |  | `Y` |  | Search term for matching player name |
| `year` | `year` |  |  | `Y` | Optional year filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `position` | `position` |  |  | `Y` | Optional position abbreviation filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_player_search()
```

_Last validated n/a._

## `cfbd_player_usage`

GET /player/usage — Retrieves player usage data for a given season

**Endpoint URL:** `GET https://api.collegefootballdata.com/player/usage`

**Valid URL:** [https://api.collegefootballdata.com/player/usage](https://api.collegefootballdata.com/player/usage)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |
| `conference` | `conference` |  |  | `Y` | Optional conference abbreviation filter |
| `position` | `position` |  |  | `Y` | Optional position abbreivation filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `playerId` | `player_id` |  |  | `Y` | Optional player id filter |
| `excludeGarbageTime` | `exclude_garbage_time` |  |  | `Y` | Optional exclude garbage time flag, defaults to false |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_player_usage()
```

_Last validated n/a._

## `cfbd_plays`

GET /plays — Retrieves historical play data

**Endpoint URL:** `GET https://api.collegefootballdata.com/plays`

**Valid URL:** [https://api.collegefootballdata.com/plays](https://api.collegefootballdata.com/plays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |
| `week` | `week` |  | `Y` |  | Required week filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `offense` | `offense` |  |  | `Y` | Optional offensive team filter |
| `defense` | `defense` |  |  | `Y` | Optional defensive team filter |
| `offenseConference` | `offense_conference` |  |  | `Y` | Optional offensive conference filter |
| `defenseConference` | `defense_conference` |  |  | `Y` | Optional defensive conference filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |
| `playType` | `play_type` |  |  | `Y` | Optoinal play type abbreviation filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `classification` | `classification` |  |  | `Y` | Optional division classification filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_plays()
```

_Last validated n/a._

## `cfbd_plays_stats`

GET /plays/stats — Retrieve player-play associations (limit 2000)

**Endpoint URL:** `GET https://api.collegefootballdata.com/plays/stats`

**Valid URL:** [https://api.collegefootballdata.com/plays/stats](https://api.collegefootballdata.com/plays/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `week` | `week` |  |  | `Y` | Optional week filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `gameId` | `game_id` |  |  | `Y` | Optional gameId filter |
| `athleteId` | `athlete_id` |  |  | `Y` | Optional athleteId filter |
| `statTypeId` | `stat_type_id` |  |  | `Y` | Optional statTypeId filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_plays_stats()
```

_Last validated n/a._

## `cfbd_plays_stats_types`

GET /plays/stats/types — Retrieves available play stat types

**Endpoint URL:** `GET https://api.collegefootballdata.com/plays/stats/types`

**Valid URL:** [https://api.collegefootballdata.com/plays/stats/types](https://api.collegefootballdata.com/plays/stats/types)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_plays_stats_types()
```

_Last validated n/a._

## `cfbd_plays_types`

GET /plays/types — Retrieves available play types

**Endpoint URL:** `GET https://api.collegefootballdata.com/plays/types`

**Valid URL:** [https://api.collegefootballdata.com/plays/types](https://api.collegefootballdata.com/plays/types)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_plays_types()
```

_Last validated n/a._

## `cfbd_ppa_games`

GET /ppa/games — Retrieves historical team PPA metrics by game

**Endpoint URL:** `GET https://api.collegefootballdata.com/ppa/games`

**Valid URL:** [https://api.collegefootballdata.com/ppa/games](https://api.collegefootballdata.com/ppa/games)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |
| `week` | `week` |  |  | `Y` | Optional week filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference abbreviation filter |
| `excludeGarbageTime` | `exclude_garbage_time` |  |  | `Y` | Optional flag to exclude garbage time plays |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ppa_games()
```

_Last validated n/a._

## `cfbd_ppa_players_games`

GET /ppa/players/games — Queries player PPA statistics by game

**Endpoint URL:** `GET https://api.collegefootballdata.com/ppa/players/games`

**Valid URL:** [https://api.collegefootballdata.com/ppa/players/games](https://api.collegefootballdata.com/ppa/players/games)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |
| `week` | `week` |  |  | `Y` | Week filter, required if team not specified |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `team` | `team` |  |  | `Y` | Team filter, required if week not specified |
| `position` | `position` |  |  | `Y` | Optional player position abbreviation filter |
| `playerId` | `player_id` |  |  | `Y` | Optional player ID filter |
| `threshold` | `threshold` |  |  | `Y` | Threshold value for minimum number of plays |
| `excludeGarbageTime` | `exclude_garbage_time` |  |  | `Y` | Optional flag to exclude garbage time plays |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ppa_players_games()
```

_Last validated n/a._

## `cfbd_ppa_players_season`

GET /ppa/players/season — Queries player PPA statistics by season

**Endpoint URL:** `GET https://api.collegefootballdata.com/ppa/players/season`

**Valid URL:** [https://api.collegefootballdata.com/ppa/players/season](https://api.collegefootballdata.com/ppa/players/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if playerId not specified |
| `conference` | `conference` |  |  | `Y` | Optional conference abbreviation filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `position` | `position` |  |  | `Y` | Optional position abbreviation filter |
| `playerId` | `player_id` |  |  | `Y` | Player ID filter, required if year not specified |
| `threshold` | `threshold` |  |  | `Y` | Threshold value for minimum number of plays |
| `excludeGarbageTime` | `exclude_garbage_time` |  |  | `Y` | Optional flag to exclude garbage time plays |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ppa_players_season()
```

_Last validated n/a._

## `cfbd_ppa_predicted`

GET /ppa/predicted — Query Predicted Points values by down and distance

**Endpoint URL:** `GET https://api.collegefootballdata.com/ppa/predicted`

**Valid URL:** [https://api.collegefootballdata.com/ppa/predicted](https://api.collegefootballdata.com/ppa/predicted)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `down` | `down` |  | `Y` |  | Down value |
| `distance` | `distance` |  | `Y` |  | Distance value |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ppa_predicted()
```

_Last validated n/a._

## `cfbd_ppa_teams`

GET /ppa/teams — Retrieves historical team PPA metrics by season

**Endpoint URL:** `GET https://api.collegefootballdata.com/ppa/teams`

**Valid URL:** [https://api.collegefootballdata.com/ppa/teams](https://api.collegefootballdata.com/ppa/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if team not specified |
| `team` | `team` |  |  | `Y` | Team filter, required if year not specified |
| `conference` | `conference` |  |  | `Y` | Conference abbreviation filter |
| `excludeGarbageTime` | `exclude_garbage_time` |  |  | `Y` | Exclude garbage time plays |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ppa_teams()
```

_Last validated n/a._

## `cfbd_rankings`

GET /rankings — Retrieves historical poll data

**Endpoint URL:** `GET https://api.collegefootballdata.com/rankings`

**Valid URL:** [https://api.collegefootballdata.com/rankings](https://api.collegefootballdata.com/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `week` | `week` |  |  | `Y` | Optional week filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_rankings()
```

_Last validated n/a._

## `cfbd_ratings_elo`

GET /ratings/elo — Retrieves historical Elo ratings

**Endpoint URL:** `GET https://api.collegefootballdata.com/ratings/elo`

**Valid URL:** [https://api.collegefootballdata.com/ratings/elo](https://api.collegefootballdata.com/ratings/elo)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `week` | `week` |  |  | `Y` | Optional week filter, defaults to last available week in the season |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ratings_elo()
```

_Last validated n/a._

## `cfbd_ratings_fpi`

GET /ratings/fpi — Retrieves historical Football Power Index (FPI) ratings

**Endpoint URL:** `GET https://api.collegefootballdata.com/ratings/fpi`

**Valid URL:** [https://api.collegefootballdata.com/ratings/fpi](https://api.collegefootballdata.com/ratings/fpi)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year filter, required if team not specified |
| `team` | `team` |  |  | `Y` | team filter, required if year not specified |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ratings_fpi()
```

_Last validated n/a._

## `cfbd_ratings_sp`

GET /ratings/sp — Retrieves SP+ ratings for a given year or school

**Endpoint URL:** `GET https://api.collegefootballdata.com/ratings/sp`

**Valid URL:** [https://api.collegefootballdata.com/ratings/sp](https://api.collegefootballdata.com/ratings/sp)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if team not specified |
| `team` | `team` |  |  | `Y` | Team filter, required if year not specified |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ratings_sp()
```

_Last validated n/a._

## `cfbd_ratings_sp_conferences`

GET /ratings/sp/conferences — Retrieves aggregated historical conference SP+ data

**Endpoint URL:** `GET https://api.collegefootballdata.com/ratings/sp/conferences`

**Valid URL:** [https://api.collegefootballdata.com/ratings/sp/conferences](https://api.collegefootballdata.com/ratings/sp/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ratings_sp_conferences()
```

_Last validated n/a._

## `cfbd_ratings_srs`

GET /ratings/srs — Retrieves historical SRS for a year or team

**Endpoint URL:** `GET https://api.collegefootballdata.com/ratings/srs`

**Valid URL:** [https://api.collegefootballdata.com/ratings/srs](https://api.collegefootballdata.com/ratings/srs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if team not specified |
| `team` | `team` |  |  | `Y` | Team filter, required if year not specified |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_ratings_srs()
```

_Last validated n/a._

## `cfbd_records`

GET /records — Retrieves historical team records

**Endpoint URL:** `GET https://api.collegefootballdata.com/records`

**Valid URL:** [https://api.collegefootballdata.com/records](https://api.collegefootballdata.com/records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if team not specified |
| `team` | `team` |  |  | `Y` | Team filter, required if year not specified |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_records()
```

_Last validated n/a._

## `cfbd_recruiting_groups`

GET /recruiting/groups — Retrieves aggregated recruiting statistics by team and position grouping

**Endpoint URL:** `GET https://api.collegefootballdata.com/recruiting/groups`

**Valid URL:** [https://api.collegefootballdata.com/recruiting/groups](https://api.collegefootballdata.com/recruiting/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  |  | `Y` | Optional team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |
| `recruitType` | `recruit_type` |  |  | `Y` | Optional recruit type filter, defaults to HighSchool |
| `startYear` | `start_year` |  |  | `Y` | Optional start year range, defaults to 2000 |
| `endYear` | `end_year` |  |  | `Y` | Optional end year range, defaults to current year |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_recruiting_groups()
```

_Last validated n/a._

## `cfbd_recruiting_players`

GET /recruiting/players — Retrieves player recruiting rankings

**Endpoint URL:** `GET https://api.collegefootballdata.com/recruiting/players`

**Valid URL:** [https://api.collegefootballdata.com/recruiting/players](https://api.collegefootballdata.com/recruiting/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required when no team specified |
| `team` | `team` |  |  | `Y` | Team filter, required when no team specified |
| `position` | `position` |  |  | `Y` | Optional position categorization filter |
| `state` | `state` |  |  | `Y` | Optional state/province filter |
| `classification` | `classification` |  |  | `Y` | Optional recruit type classification filter, defaults to HighSchool |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_recruiting_players()
```

_Last validated n/a._

## `cfbd_recruiting_teams`

GET /recruiting/teams — Retrieves team recruiting rankings

**Endpoint URL:** `GET https://api.collegefootballdata.com/recruiting/teams`

**Valid URL:** [https://api.collegefootballdata.com/recruiting/teams](https://api.collegefootballdata.com/recruiting/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `team` | `team` |  |  | `Y` | Optional team filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_recruiting_teams()
```

_Last validated n/a._

## `cfbd_roster`

GET /roster — Retrieves historical roster data

**Endpoint URL:** `GET https://api.collegefootballdata.com/roster`

**Valid URL:** [https://api.collegefootballdata.com/roster](https://api.collegefootballdata.com/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team` | `team` |  |  | `Y` | Optional team filter |
| `year` | `year` |  |  | `Y` | Optional year filter, defaults to 2023 |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_roster()
```

_Last validated n/a._

## `cfbd_scoreboard`

GET /scoreboard — Retrieves live scoreboard data

**Endpoint URL:** `GET https://api.collegefootballdata.com/scoreboard`

**Valid URL:** [https://api.collegefootballdata.com/scoreboard](https://api.collegefootballdata.com/scoreboard)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `classification` | `classification` |  |  | `Y` | Optional division classification filter, defaults to fbs |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_scoreboard()
```

_Last validated n/a._

## `cfbd_stats_categories`

GET /stats/categories — Gets team statistical categories

**Endpoint URL:** `GET https://api.collegefootballdata.com/stats/categories`

**Valid URL:** [https://api.collegefootballdata.com/stats/categories](https://api.collegefootballdata.com/stats/categories)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_stats_categories()
```

_Last validated n/a._

## `cfbd_stats_game_advanced`

GET /stats/game/advanced — Retrieves advanced statistics aggregated by game

**Endpoint URL:** `GET https://api.collegefootballdata.com/stats/game/advanced`

**Valid URL:** [https://api.collegefootballdata.com/stats/game/advanced](https://api.collegefootballdata.com/stats/game/advanced)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if team not specified |
| `team` | `team` |  |  | `Y` | Team filter, required if year not specified |
| `week` | `week` |  |  | `Y` | Optional week filter |
| `opponent` | `opponent` |  |  | `Y` | Optional opponent filter |
| `excludeGarbageTime` | `exclude_garbage_time` |  |  | `Y` | Garbage time exclusion filter, defaults to false |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_stats_game_advanced()
```

_Last validated n/a._

## `cfbd_stats_player_season`

GET /stats/player/season — Retrieves aggregated player statistics for a given season

**Endpoint URL:** `GET https://api.collegefootballdata.com/stats/player/season`

**Valid URL:** [https://api.collegefootballdata.com/stats/player/season](https://api.collegefootballdata.com/stats/player/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Required year filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `startWeek` | `start_week` |  |  | `Y` | Optional starting week range |
| `endWeek` | `end_week` |  |  | `Y` | Optional ending week range |
| `seasonType` | `season_type` |  |  | `Y` | Optional season type filter |
| `category` | `category` |  |  | `Y` | Optional category filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_stats_player_season()
```

_Last validated n/a._

## `cfbd_stats_season`

GET /stats/season — Retrieves aggregated team season statistics

**Endpoint URL:** `GET https://api.collegefootballdata.com/stats/season`

**Valid URL:** [https://api.collegefootballdata.com/stats/season](https://api.collegefootballdata.com/stats/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if team not specified |
| `team` | `team` |  |  | `Y` | Team filter, required if year not specified |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |
| `startWeek` | `start_week` |  |  | `Y` | Optional week start range filter |
| `endWeek` | `end_week` |  |  | `Y` | Optional week end range filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_stats_season()
```

_Last validated n/a._

## `cfbd_stats_season_advanced`

GET /stats/season/advanced — Retrieves advanced season statistics for teams

**Endpoint URL:** `GET https://api.collegefootballdata.com/stats/season/advanced`

**Valid URL:** [https://api.collegefootballdata.com/stats/season/advanced](https://api.collegefootballdata.com/stats/season/advanced)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year filter, required if team not specified |
| `team` | `team` |  |  | `Y` | Team filter, required if year not specified |
| `excludeGarbageTime` | `exclude_garbage_time` |  |  | `Y` | Garbage time exclusion filter, defaults to false |
| `startWeek` | `start_week` |  |  | `Y` | Optional start week range filter |
| `endWeek` | `end_week` |  |  | `Y` | Optional end week range filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_stats_season_advanced()
```

_Last validated n/a._

## `cfbd_talent`

GET /talent — Retrieve 247 Team Talent Composite for a given year

**Endpoint URL:** `GET https://api.collegefootballdata.com/talent`

**Valid URL:** [https://api.collegefootballdata.com/talent](https://api.collegefootballdata.com/talent)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | Year filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_talent()
```

_Last validated n/a._

## `cfbd_teams`

GET /teams — Retrieves team information

**Endpoint URL:** `GET https://api.collegefootballdata.com/teams`

**Valid URL:** [https://api.collegefootballdata.com/teams](https://api.collegefootballdata.com/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `conference` | `conference` |  |  | `Y` | Optional conference abbreviation filter |
| `year` | `year` |  |  | `Y` | Optional year filter to get historical conference affiliations |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_teams()
```

_Last validated n/a._

## `cfbd_teams_fbs`

GET /teams/fbs — Retrieves information on teams playing in the highest division of CFB

**Endpoint URL:** `GET https://api.collegefootballdata.com/teams/fbs`

**Valid URL:** [https://api.collegefootballdata.com/teams/fbs](https://api.collegefootballdata.com/teams/fbs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Year or season |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_teams_fbs()
```

_Last validated n/a._

## `cfbd_teams_matchup`

GET /teams/matchup — Retrieves historical matchup details for two given teams

**Endpoint URL:** `GET https://api.collegefootballdata.com/teams/matchup`

**Valid URL:** [https://api.collegefootballdata.com/teams/matchup](https://api.collegefootballdata.com/teams/matchup)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team1` | `team1` |  | `Y` |  | First team to compare |
| `team2` | `team2` |  | `Y` |  | Second team to compare |
| `minYear` | `min_year` |  |  | `Y` | Optional starting year |
| `maxYear` | `max_year` |  |  | `Y` | Optional ending year |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_teams_matchup()
```

_Last validated n/a._

## `cfbd_venues`

GET /venues — Retrieve list of venues

**Endpoint URL:** `GET https://api.collegefootballdata.com/venues`

**Valid URL:** [https://api.collegefootballdata.com/venues](https://api.collegefootballdata.com/venues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_venues()
```

_Last validated n/a._

## `cfbd_wepa_players_kicking`

GET /wepa/players/kicking — Retrieve Points Added Above Replacement (PAAR) ratings for kickers

**Endpoint URL:** `GET https://api.collegefootballdata.com/wepa/players/kicking`

**Valid URL:** [https://api.collegefootballdata.com/wepa/players/kicking](https://api.collegefootballdata.com/wepa/players/kicking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference abbreviation filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_wepa_players_kicking()
```

_Last validated n/a._

## `cfbd_wepa_players_passing`

GET /wepa/players/passing — Retrieve opponent-adjusted player passing statistics

**Endpoint URL:** `GET https://api.collegefootballdata.com/wepa/players/passing`

**Valid URL:** [https://api.collegefootballdata.com/wepa/players/passing](https://api.collegefootballdata.com/wepa/players/passing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference abbreviation filter |
| `position` | `position` |  |  | `Y` | Optional position abbreviation filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_wepa_players_passing()
```

_Last validated n/a._

## `cfbd_wepa_players_rushing`

GET /wepa/players/rushing — Retrieve opponent-adjusted player rushing statistics

**Endpoint URL:** `GET https://api.collegefootballdata.com/wepa/players/rushing`

**Valid URL:** [https://api.collegefootballdata.com/wepa/players/rushing](https://api.collegefootballdata.com/wepa/players/rushing)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference abbreviation filter |
| `position` | `position` |  |  | `Y` | Optional position abbreviation filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_wepa_players_rushing()
```

_Last validated n/a._

## `cfbd_wepa_team_season`

GET /wepa/team/season — Retrieve opponent-adjusted team season statistics

**Endpoint URL:** `GET https://api.collegefootballdata.com/wepa/team/season`

**Valid URL:** [https://api.collegefootballdata.com/wepa/team/season](https://api.collegefootballdata.com/wepa/team/season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | Optional year filter |
| `team` | `team` |  |  | `Y` | Optional team filter |
| `conference` | `conference` |  |  | `Y` | Optional conference filter |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_cfbd_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
cfbd_wepa_team_season()
```

_Last validated n/a._
