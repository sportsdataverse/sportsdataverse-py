# YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — League

> YAHOO — Yahoo Sports Shangrila (graphite-secure.sports.yahoo.com) — League — function reference in sdv-py, the SportsDataverse Python package.

## yahoo_league_conferences

Yahoo shangrila persisted query `leagueConferences` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueConferences`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueConferences](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueConferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `divisionIds` | `division_ids` |  |  | `Y` | divisionIds query parameter. |

### Returns {#yahoo_league_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `short_name` | character |  |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_conferences-example}

```python
yahoo_league_conferences()
```

_Last validated n/a._

## yahoo_league_filters_data

Yahoo shangrila persisted query `leagueFiltersData` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFiltersData`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFiltersData](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFiltersData)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `viewType` | `view_type` |  |  | `Y` | viewType query parameter. |
| `includePosAndSplitsData` | `include_pos_and_splits_data` |  |  | `Y` | includePosAndSplitsData query parameter. |

### Returns {#yahoo_league_filters_data-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character |  |
| `short_name` | character |  |
| `full_name` | character |  |
| `name` | character |  |
| `current_league_day` | character | Calendar date the league's live scoreboard is anchored on, in YYYY-MM-DD form. |
| `teams` | character |  |
| `current_week` | integer | Week number within the league's current season phase, counting from 1. |
| `current_season_phase` | character | Phase of the season currently in effect (e.g., "season.phase.season", "season.phase.offseason"). |
| `current_game_season_phase` | character | Season phase of the games the league feed is currently serving. |
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |
| `current_league_season` | character | Yahoo league-season identifier for the season currently in progress. |
| `league_seasons` | character | JSON-encoded list of the seasons for which Yahoo carries data for this league. |
| `league_weeks` | character | JSON-encoded list of the league's week nodes for the season. |
| `current_season_league_weeks` | character | JSON-encoded list of the week nodes making up the current league season. |
| `divisions` | character | JSON-encoded list of the league's division nodes, each carrying its member conferences and teams. |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_filters_data-example}

```python
yahoo_league_filters_data()
```

_Last validated n/a._

## yahoo_league_future_odds

Yahoo shangrila persisted query `leagueFutureOdds` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFutureOdds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFutureOdds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueFutureOdds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `betCategories` | `bet_categories` |  |  | `Y` | betCategories query parameter. |

### Returns {#yahoo_league_future_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `bets` | character | JSON-encoded list of the betting markets offered on the event (spread, moneyline and total). |
| `league` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_future_odds-example}

```python
yahoo_league_future_odds()
```

_Last validated n/a._

## yahoo_league_game_ids

Yahoo shangrila persisted query `leagueGameIds` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `gameStatusOrder` | `game_status_order` |  |  | `Y` | gameStatusOrder query parameter. |
| `startTimeOrder` | `start_time_order` |  |  | `Y` | startTimeOrder query parameter. |
| `dateFlipOffset` | `date_flip_offset` |  |  | `Y` | dateFlipOffset query parameter. |
| `seasonPhase` | `season_phase` |  |  | `Y` | seasonPhase query parameter. |
| `conferenceIds` | `conference_ids` |  |  | `Y` | conferenceIds query parameter. |
| `top25` | `top25` |  |  | `Y` | top25 query parameter. |
| `gameDayQueryType` | `game_day_query_type` |  |  | `Y` | gameDayQueryType query parameter. |

### Returns {#yahoo_league_game_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_navigation_links` | character | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `current_week` | integer | Week number within the league's current season phase, counting from 1. |
| `games` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_game_ids-example}

```python
yahoo_league_game_ids()
```

_Last validated n/a._

## yahoo_league_game_ids_by_date

Yahoo shangrila persisted query `leagueGameIdsByDate` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIdsByDate`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIdsByDate](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGameIdsByDate)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `startRange` | `start_range` |  |  | `Y` | startRange query parameter. |
| `endRange` | `end_range` |  |  | `Y` | endRange query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonPhases` | `season_phases` |  |  | `Y` | seasonPhases query parameter. |
| `conferenceIds` | `conference_ids` |  |  | `Y` | conferenceIds query parameter. |
| `divisionIds` | `division_ids` |  |  | `Y` | divisionIds query parameter. |
| `top25` | `top25` |  |  | `Y` | top25 query parameter. |
| `tournamentIds` | `tournament_ids` |  |  | `Y` | tournamentIds query parameter. |
| `isTennis` | `is_tennis` |  |  | `Y` | isTennis query parameter. |

### Returns {#yahoo_league_game_ids_by_date-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `full_name` | character |  |
| `name` | character |  |
| `current_week` | integer | Week number within the league's current season phase, counting from 1. |
| `current_game_season_phase` | character | Season phase of the games the league feed is currently serving. |
| `current_league_season` | character | Yahoo league-season identifier for the season currently in progress. |
| `games` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_game_ids_by_date-example}

```python
yahoo_league_game_ids_by_date()
```

_Last validated n/a._

## yahoo_league_games_by_round

Yahoo shangrila persisted query `leagueGamesByRound` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGamesByRound`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGamesByRound](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueGamesByRound)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `tournamentRoundIds` | `tournament_round_ids` |  |  | `Y` | tournamentRoundIds query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#yahoo_league_games_by_round-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_games_by_round-example}

```python
yahoo_league_games_by_round()
```

_Last validated n/a._

## yahoo_league_info

Yahoo shangrila persisted query `leagueInfo` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInfo`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInfo](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInfo)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns {#yahoo_league_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `name` | character |  |
| `full_name` | character |  |
| `short_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_info-example}

```python
yahoo_league_info()
```

_Last validated n/a._

## yahoo_league_injuries

Yahoo shangrila persisted query `leagueInjuries` -> one row per `leagues.teams` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInjuries`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInjuries](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueInjuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |

### Returns {#yahoo_league_injuries-returns}

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
| `alias` | character | JSON-encoded Yahoo alias object for the entity, carrying the site URL, path and subpage routing used to build links to its page. |
| `team_logo_white` | character | JSON-encoded image node for the team's white knockout logo. |
| `team_logo` | character |  |
| `players` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_injuries-example}

```python
yahoo_league_injuries()
```

_Last validated n/a._

## yahoo_league_names

Yahoo shangrila persisted query `leagueNames` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueNames`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueNames](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueNames)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |

### Returns {#yahoo_league_names-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | integer |  |
| `short_name` | character |  |
| `full_name` | character |  |
| `name` | character |  |
| `display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `display_abbr` | character | Compact league abbreviation used in dense UI (e.g., "NCAAF"). |
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |
| `league_seasons` | character | JSON-encoded list of the seasons for which Yahoo carries data for this league. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_path` | character | Site-relative path portion of the entity's Yahoo alias (e.g., "/ncaaf/teams/tcu/"). |
| `alias_subpages` | character | JSON-encoded list of subpage aliases (roster, schedule, stats) available beneath the entity's Yahoo page. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_names-example}

```python
yahoo_league_names()
```

_Last validated n/a._

## yahoo_league_prop_odds

Yahoo shangrila persisted query `leaguePropOdds` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguePropOdds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguePropOdds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguePropOdds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `count` | `count` |  |  | `Y` | count query parameter. |
| `league` | `league` |  |  | `Y` | league query parameter. |

### Returns {#yahoo_league_prop_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_prop_odds-example}

```python
yahoo_league_prop_odds()
```

_Last validated n/a._

## yahoo_league_standings

Yahoo shangrila persisted query `leagueStandings` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStandings`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStandings](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStandings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonPhase` | `season_phase` |  |  | `Y` | seasonPhase query parameter. |

### Returns {#yahoo_league_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sport_name` | character |  |
| `current_season_phase` | character | Phase of the season currently in effect (e.g., "season.phase.season", "season.phase.offseason"). |
| `current_league_season` | character | Yahoo league-season identifier for the season currently in progress. |
| `divisions` | character | JSON-encoded list of the league's division nodes, each carrying its member conferences and teams. |
| `teams` | character |  |
| `conferences` | character | JSON-encoded list of the league's conference nodes, each carrying an id, a name and its member teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_standings-example}

```python
yahoo_league_standings()
```

_Last validated n/a._

## yahoo_league_stats_by_team

Yahoo shangrila persisted query `leagueStatsByTeam` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsByTeam`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsByTeam](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsByTeam)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `leagueStructureId` | `league_structure_id` |  |  | `Y` | leagueStructureId query parameter. |
| `baseballCutType` | `baseball_cut_type` |  |  | `Y` | baseballCutType query parameter. |
| `basketballCutType` | `basketball_cut_type` |  |  | `Y` | basketballCutType query parameter. |
| `footballCutType` | `football_cut_type` |  |  | `Y` | footballCutType query parameter. |
| `hockeyCutType` | `hockey_cut_type` |  |  | `Y` | hockeyCutType query parameter. |

### Returns {#yahoo_league_stats_by_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character |  |
| `short_name` | character |  |
| `full_name` | character |  |
| `name` | character |  |
| `football_stats` | character | JSON-encoded football statistics block returned by the league stats query. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_stats_by_team-example}

```python
yahoo_league_stats_by_team()
```

_Last validated n/a._

## yahoo_league_stats_individual

Yahoo shangrila persisted query `leagueStatsIndividual` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsIndividual`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsIndividual](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsIndividual)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `qualified` | `qualified` |  |  | `Y` | qualified query parameter. |
| `leagueStructureId` | `league_structure_id` |  |  | `Y` | leagueStructureId query parameter. |
| `baseballCutType` | `baseball_cut_type` |  |  | `Y` | baseballCutType query parameter. |
| `baseballPosition` | `baseball_position` |  |  | `Y` | baseballPosition query parameter. |
| `basketballCutType` | `basketball_cut_type` |  |  | `Y` | basketballCutType query parameter. |
| `basketballPosition` | `basketball_position` |  |  | `Y` | basketballPosition query parameter. |
| `footballCutType` | `football_cut_type` |  |  | `Y` | footballCutType query parameter. |
| `hockeyCutType` | `hockey_cut_type` |  |  | `Y` | hockeyCutType query parameter. |
| `hockeyPosition` | `hockey_position` |  |  | `Y` | hockeyPosition query parameter. |
| `golfSortStat` | `golf_sort_stat` |  |  | `Y` | golfSortStat query parameter. |
| `golfStatIds` | `golf_stat_ids` |  |  | `Y` | golfStatIds query parameter. |
| `motorsportsSortStat` | `motorsports_sort_stat` |  |  | `Y` | motorsportsSortStat query parameter. |
| `motorsportsStatIds` | `motorsports_stat_ids` |  |  | `Y` | motorsportsStatIds query parameter. |

### Returns {#yahoo_league_stats_individual-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character |  |
| `short_name` | character |  |
| `full_name` | character |  |
| `name` | character |  |
| `football_stats` | character | JSON-encoded football statistics block returned by the league stats query. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_stats_individual-example}

```python
yahoo_league_stats_individual()
```

_Last validated n/a._

## yahoo_league_stats_overview

Yahoo shangrila persisted query `leagueStatsOverview` (response body not captured; shape unknown)

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsOverview`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsOverview](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsOverview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `weekSeasonPhase` | `week_season_phase` |  |  | `Y` | weekSeasonPhase query parameter. |
| `seasonPhase` | `season_phase` |  |  | `Y` | seasonPhase query parameter. |
| `leagueStructureId` | `league_structure_id` |  |  | `Y` | leagueStructureId query parameter. |
| `golfSortStat` | `golf_sort_stat` |  |  | `Y` | golfSortStat query parameter. |
| `golfStatIds` | `golf_stat_ids` |  |  | `Y` | golfStatIds query parameter. |
| `motorsportsSortStat` | `motorsports_sort_stat` |  |  | `Y` | motorsportsSortStat query parameter. |
| `motorsportsStatIds` | `motorsports_stat_ids` |  |  | `Y` | motorsportsStatIds query parameter. |

### Returns {#yahoo_league_stats_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_yahoo_shangrila`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_stats_overview-example}

```python
yahoo_league_stats_overview()
```

_Last validated n/a._

## yahoo_league_stats_weekly

Yahoo shangrila persisted query `leagueStatsWeekly` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsWeekly`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsWeekly](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsWeekly)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |
| `count` | `count` |  |  | `Y` | count query parameter. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonPhase` | `season_phase` |  |  | `Y` | seasonPhase query parameter. |

### Returns {#yahoo_league_stats_weekly-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sport_sport_id` | character | Yahoo identifier of the sport the league belongs to. |
| `sport_name` | character |  |
| `short_name` | character |  |
| `full_name` | character |  |
| `name` | character |  |
| `football_stats` | character | JSON-encoded football statistics block returned by the league stats query. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_stats_weekly-example}

```python
yahoo_league_stats_weekly()
```

_Last validated n/a._

## yahoo_league_team_ids

Yahoo shangrila persisted query `leagueTeamIds` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeamIds`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeamIds](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeamIds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `divisionIds` | `division_ids` |  |  | `Y` | divisionIds query parameter. |
| `getTeamsByDivision` | `get_teams_by_division` |  |  | `Y` | getTeamsByDivision query parameter. |

### Returns {#yahoo_league_team_ids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `short_name` | character |  |
| `teams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_team_ids-example}

```python
yahoo_league_team_ids()
```

_Last validated n/a._

## yahoo_league_teams

Yahoo shangrila persisted query `leagueTeams` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeams`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeams](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueTeams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | league query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `divisionIds` | `division_ids` |  |  | `Y` | divisionIds query parameter. |
| `getTeamsByDivision` | `get_teams_by_division` |  |  | `Y` | getTeamsByDivision query parameter. |

### Returns {#yahoo_league_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `short_name` | character |  |
| `teams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_league_teams-example}

```python
yahoo_league_teams()
```

_Last validated n/a._

## yahoo_leagues_season_states

Yahoo shangrila persisted query `leaguesSeasonStates` -> one row per `leagues` entry

**Endpoint URL:** `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguesSeasonStates`

**Valid URL:** [https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguesSeasonStates](https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leaguesSeasonStates)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagues` | `leagues` |  |  | `Y` | leagues query parameter. |

### Returns {#yahoo_leagues_season_states-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | character |  |
| `short_name` | character |  |
| `full_name` | character |  |
| `display_short` | character | Short league label used in navigation and compact UI (e.g., "NCAA FB"). |
| `current_season_phase` | character | Phase of the season currently in effect (e.g., "season.phase.season", "season.phase.offseason"). |
| `current_week` | integer | Week number within the league's current season phase, counting from 1. |
| `current_season` | integer | Season the league is currently playing, as the four-digit starting year. |
| `stats_season` | character | Season the returned statistics cover, as a four-digit year. |
| `sport_name` | character |  |
| `league_weeks` | character | JSON-encoded list of the league's week nodes for the season. |
| `alias_url` | character | Absolute sports.yahoo.com URL of the entity's page (e.g., "https://sports.yahoo.com/ncaaf/players/464024/"). |
| `alias_navigation_links` | character | JSON-encoded map of navigation links (scores, standings, teams) hanging off the entity's Yahoo alias. |
| `league_seasons` | character | JSON-encoded list of the seasons for which Yahoo carries data for this league. |
| `bye_weeks` | character | JSON-encoded list of the week numbers in which the team has no scheduled game. |
| `divisions` | character | JSON-encoded list of the league's division nodes, each carrying its member conferences and teams. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#yahoo_leagues_season_states-example}

```python
yahoo_leagues_season_states()
```

_Last validated n/a._
