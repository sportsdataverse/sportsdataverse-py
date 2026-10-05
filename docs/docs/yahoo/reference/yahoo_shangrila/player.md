---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Player"
sidebar_label: "Player"
sidebar_position: 7
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Player

## yahoo_player_basic

Yahoo shangrila persisted query `playerBasic` -> tables: players, leagues

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerBasic`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerBasic](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerBasic)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |

### Returns {#yahoo_player_basic-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**players**

| col_name | type | description |
|---|---|---|
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_lang` | character | Language/locale tag attached to the entity's Yahoo alias (e.g., "en-US"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | character | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `display_name` | character | Display name. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_id` | character | Unique player identifier. |
| `positions` | character | Positions. |
| `team_display_name` | character | Full team display name. |
| `team_team_id` | character | Unique identifier for team team. |
| `uniform_number` | character | Jersey number the player wears for the team. |
| `injury` | character | Injury (body part / description). |

**leagues**

| col_name | type | description |
|---|---|---|
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_player_basic-example}

```python
yahoo_player_basic()
```

_Last validated n/a._

## yahoo_player_career_stats

Yahoo shangrila persisted query `playerCareerStats` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerCareerStats`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerCareerStats](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerCareerStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `footballStatIds` | `football_stat_ids` |  |  | `Y` | footballStatIds query parameter. |
| `basketballStatIds` | `basketball_stat_ids` |  |  | `Y` | basketballStatIds query parameter. |
| `baseballStatIds` | `baseball_stat_ids` |  |  | `Y` | baseballStatIds query parameter. |
| `hockeyStatIds` | `hockey_stat_ids` |  |  | `Y` | hockeyStatIds query parameter. |
| `soccerStatIds` | `soccer_stat_ids` |  |  | `Y` | soccerStatIds query parameter. |

### Returns {#yahoo_player_career_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `positions` | character | Positions. |
| `stats_by_season` | character | JSON-encoded per-season statistical lines for the player. |
| `total_stats` | character | JSON-encoded career-total statistical line summing the player's seasons. |
| `career_stats` | character | JSON-encoded career statistical totals for the player across every season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_player_career_stats-example}

```python
yahoo_player_career_stats()
```

_Last validated n/a._

## yahoo_player_game_log

Yahoo shangrila persisted query `playerGameLog` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerGameLog`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerGameLog](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerGameLog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `seasons` | `seasons` |  |  | `Y` | seasons query parameter. |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `footballStatIds` | `football_stat_ids` |  |  | `Y` | footballStatIds query parameter. |
| `basketballStatIds` | `basketball_stat_ids` |  |  | `Y` | basketballStatIds query parameter. |
| `baseballStatIds` | `baseball_stat_ids` |  |  | `Y` | baseballStatIds query parameter. |
| `hockeyStatIds` | `hockey_stat_ids` |  |  | `Y` | hockeyStatIds query parameter. |
| `soccerStatIds` | `soccer_stat_ids` |  |  | `Y` | soccerStatIds query parameter. |

### Returns {#yahoo_player_game_log-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `positions` | character | Positions. |
| `team_id` | character | Unique team identifier. |
| `player_game_stats` | character | JSON-encoded per-game statistical lines for the player across the requested game log. |
| `player_season_stats` | character | JSON-encoded season statistical totals for the player. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_player_game_log-example}

```python
yahoo_player_game_log()
```

_Last validated n/a._

## yahoo_player_props

Yahoo shangrila persisted query `playerProps` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerProps`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerProps](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerProps)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |

### Returns {#yahoo_player_props-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `games` | character | Games played. |
| `player_id` | character | Unique player identifier. |
| `display_name` | character | Display name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_player_props-example}

```python
yahoo_player_props()
```

_Last validated n/a._

## yahoo_player_search

Yahoo shangrila persisted query `playerSearch` -> one row per `leagues.players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSearch`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSearch](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSearch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `name` | `name` |  |  | `Y` | name query parameter. |
| `onActiveRosterOnly` | `on_active_roster_only` |  |  | `Y` | onActiveRosterOnly query parameter. |
| `nflPositionId` | `nfl_position_id` |  |  | `Y` | nflPositionId query parameter. |
| `nbaPositionId` | `nba_position_id` |  |  | `Y` | nbaPositionId query parameter. |
| `mlbPositionId` | `mlb_position_id` |  |  | `Y` | mlbPositionId query parameter. |
| `nhlPositionId` | `nhl_position_id` |  |  | `Y` | nhlPositionId query parameter. |

### Returns {#yahoo_player_search-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `active` | character | TRUE if the row represents an active record (player / team / season). |
| `alias` | character | JSON-encoded Yahoo alias object for the entity, carrying the site URL, path and subpage routing used to build links to its page. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `display_name` | character | Display name. |
| `suggested_headshot` | character | JSON-encoded image node for the headshot Yahoo recommends for this player. |
| `team` | character | Team-side label or team identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_player_search-example}

```python
yahoo_player_search()
```

_Last validated n/a._

## yahoo_player_season_stats

Yahoo shangrila persisted query `playerSeasonStats` -> one row per `players` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSeasonStats`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSeasonStats](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/playerSeasonStats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `seasons` | `seasons` |  |  | `Y` | seasons query parameter. |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `footballStatIds` | `football_stat_ids` |  |  | `Y` | footballStatIds query parameter. |
| `footballCutTypeGroups` | `football_cut_type_groups` |  |  | `Y` | footballCutTypeGroups query parameter. |
| `basketballStatIds` | `basketball_stat_ids` |  |  | `Y` | basketballStatIds query parameter. |
| `basketballCutTypeGroups` | `basketball_cut_type_groups` |  |  | `Y` | basketballCutTypeGroups query parameter. |
| `baseballStatIds` | `baseball_stat_ids` |  |  | `Y` | baseballStatIds query parameter. |
| `baseballCutTypeGroups` | `baseball_cut_type_groups` |  |  | `Y` | baseballCutTypeGroups query parameter. |
| `hockeyStatIds` | `hockey_stat_ids` |  |  | `Y` | hockeyStatIds query parameter. |
| `hockeyCutTypeGroups` | `hockey_cut_type_groups` |  |  | `Y` | hockeyCutTypeGroups query parameter. |
| `groupBySeasonPhase` | `group_by_season_phase` |  |  | `Y` | groupBySeasonPhase query parameter. |
| `usePlayerUniqueId` | `use_player_unique_id` |  |  | `Y` | usePlayerUniqueId query parameter. |

### Returns {#yahoo_player_season_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `positions` | character | Positions. |
| `team_id` | character | Unique team identifier. |
| `player_season_stats` | character | JSON-encoded season statistical totals for the player. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_player_season_stats-example}

```python
yahoo_player_season_stats()
```

_Last validated n/a._
