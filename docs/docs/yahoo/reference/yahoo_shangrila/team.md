---
title: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Team"
sidebar_label: "Team"
sidebar_position: 8
description: "YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — Team

## yahoo_team_injuries

Yahoo shangrila persisted query `teamInjuries` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamInjuries`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamInjuries](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamInjuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns {#yahoo_team_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character |  |
| `nickname` | character |  |
| `full_name` | character |  |
| `location` | character |  |
| `display_name` | character |  |
| `primary_color` | character |  |
| `abbreviation` | character |  |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `players` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_team_injuries-example}

```python
yahoo_team_injuries()
```

_Last validated n/a._

## yahoo_team_playoff_series

Yahoo shangrila persisted query `teamPlayoffSeries` -> one row per `teams.playoffSeries` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamPlayoffSeries`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamPlayoffSeries](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamPlayoffSeries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#yahoo_team_playoff_series-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_team_playoff_series-example}

```python
yahoo_team_playoff_series()
```

_Last validated n/a._

## yahoo_team_roster

Yahoo shangrila persisted query `teamRoster` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamRoster`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamRoster](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamRoster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `playerImageHeight` | `player_image_height` |  |  | `Y` | playerImageHeight query parameter. |
| `playerImageWidth` | `player_image_width` |  |  | `Y` | playerImageWidth query parameter. |

### Returns {#yahoo_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_current_season` | character | Yahoo league-season identifier for the league's season currently in progress. |
| `roster` | character | JSON-encoded roster of the players on the team for the requested season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_team_roster-example}

```python
yahoo_team_roster()
```

_Last validated n/a._

## yahoo_team_schedule_by_season

Yahoo shangrila persisted query `teamScheduleBySeason` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamScheduleBySeason`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamScheduleBySeason](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamScheduleBySeason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns {#yahoo_team_schedule_by_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character |  |
| `display_name` | character |  |
| `primary_color` | character |  |
| `secondary_color` | character |  |
| `gametime_ticket_url` | character | Gametime affiliate ticket-purchase URL for the event or team. |
| `bye_weeks` | character | JSON-encoded list of the week numbers in which the team has no scheduled game. |
| `games` | character |  |
| `leagues` | character | JSON-encoded list of the league nodes the team's schedule spans. |
| `full_name` | character |  |
| `abbreviation` | character |  |
| `nickname` | character |  |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_standings_team_record` | character | Formatted overall record for the team (e.g., "8-2"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_team_schedule_by_season-example}

```python
yahoo_team_schedule_by_season()
```

_Last validated n/a._

## yahoo_team_search

Yahoo shangrila persisted query `teamSearch` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamSearch`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamSearch](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamSearch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `name` | `name` |  |  | `Y` | name query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns {#yahoo_team_search-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character |  |
| `display_name` | character |  |
| `full_name` | character |  |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `primary_color` | character |  |
| `secondary_color` | character |  |
| `abbreviation` | character |  |
| `league_short_name` | character |  |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_name` | character |  |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_team_search-example}

```python
yahoo_team_search()
```

_Last validated n/a._

## yahoo_team_stats_leaders_v2

Yahoo shangrila persisted query `teamStatsLeadersV2` -> tables: leagues, teams

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamStatsLeadersV2`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamStatsLeadersV2](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamStatsLeadersV2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `baseballCutType` | `baseball_cut_type` |  |  | `Y` | baseballCutType query parameter. |
| `qualified` | `qualified` |  |  | `Y` | qualified query parameter. |
| `includeTeamStats` | `include_team_stats` |  |  | `Y` | includeTeamStats query parameter. |
| `includePlayerStats` | `include_player_stats` |  |  | `Y` | includePlayerStats query parameter. |
| `isBaseball` | `is_baseball` |  |  | `Y` | isBaseball query parameter. |

### Returns {#yahoo_team_stats_leaders_v2-returns}

**`return_parsed=True`** (default) — A dict of polars/pandas DataFrames keyed by the payload's `data` collections (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**teams**

| col_name | type | description |
|---|---|---|
| `display_name` | character |  |
| `full_name` | character |  |
| `nickname` | character |  |
| `team_football` | character | JSON-encoded team-level football leader board for the team. |
| `individual_football` | character | JSON-encoded player-level football leader board for the team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_team_stats_leaders_v2-example}

```python
yahoo_team_stats_leaders_v2()
```

_Last validated n/a._

## yahoo_team_transactions

Yahoo shangrila persisted query `teamTransactions` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamTransactions`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamTransactions](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamTransactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |

### Returns {#yahoo_team_transactions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character |  |
| `nickname` | character |  |
| `full_name` | character |  |
| `location` | character |  |
| `display_name` | character |  |
| `primary_color` | character |  |
| `abbreviation` | character |  |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_domain` | character | Host the entity's Yahoo alias resolves against (e.g., "sports.yahoo.com"). |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `transactions` | character | JSON-encoded list of the team's roster transactions over the requested window. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_team_transactions-example}

```python
yahoo_team_transactions()
```

_Last validated n/a._

## yahoo_teams_basic

Yahoo shangrila persisted query `teamsBasic` -> one row per `teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamsBasic`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamsBasic](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/teamsBasic)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamIds` | `team_ids` |  |  | `Y` | teamIds query parameter. |
| `imageHeight` | `image_height` |  |  | `Y` | imageHeight query parameter. |
| `imageWidth` | `image_width` |  |  | `Y` | imageWidth query parameter. |

### Returns {#yahoo_teams_basic-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | character |  |
| `team_logo_url` | character | Absolute URL of the team's standard logo image on Yahoo's image CDN. |
| `team_logo_white_url` | character | Absolute URL of the team's white knockout logo, the variant used on dark backgrounds. |
| `display_name` | character |  |
| `full_name` | character |  |
| `nickname` | character |  |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `primary_color` | character |  |
| `secondary_color` | character |  |
| `abbreviation` | character |  |
| `league_display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `league_name` | character |  |
| `league_short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_teams_basic-example}

```python
yahoo_teams_basic()
```

_Last validated n/a._
