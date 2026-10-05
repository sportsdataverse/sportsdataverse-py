---
title: "MLB — MLB Stats API — Other (2)"
sidebar_label: "Other (2)"
sidebar_position: 9
description: "MLB — MLB Stats API — Other (2) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — Other (2)

## mlb_umpires

GET /api/v1/jobs/umpires — current umpire crew assignments.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/umpires`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/umpires](https://statsapi.mlb.com/api/v1/jobs/umpires)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#mlb_umpires-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_umpires-example}

```python
mlb_umpires()
```

_Last validated n/a._

## mlb_conferences

View all PCL conferences.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/conferences`

**Valid URL:** [https://statsapi.mlb.com/api/v1/conferences](https://statsapi.mlb.com/api/v1/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `conferenceId` | `conference_id` |  |  | `Y` | conferenceId query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `abbreviation` | character | Short abbreviation. |
| `has_wildcard` | logical | Whether the season has a wild card round. |
| `name_short` | character | Short name of player (First Initial, Last Name) |
| `league_id` | integer | League MLBAM ID. |
| `league_link` | character | API link to the league. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_conferences-example}

```python
mlb_conferences()
```

_Last validated n/a._

## mlb_conference

View PCL conferences by conferenceId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/conferences/{conference_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/conferences/301](https://statsapi.mlb.com/api/v1/conferences/301)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `conference_id` | `conference_id` |  | `Y` |  | conference_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_conference-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `name` | character | Display name. |
| `link` | character | API link to the game feed. |
| `abbreviation` | character | Short abbreviation. |
| `has_wildcard` | logical | Whether the season has a wild card round. |
| `name_short` | character | Short name of player (First Initial, Last Name) |
| `league_id` | integer | League MLBAM ID. |
| `league_link` | character | API link to the league. |
| `sport_id` | integer | Sport MLBAM ID. |
| `sport_link` | character | API link to the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_conference-example}

```python
mlb_conference(conference_id=301)
```

_Last validated n/a._

## mlb_analytics_games

View timestamps of most recent data corrections made to games.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/analytics/game`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/analytics/game](https://statsapi.mlb.com/api/v1/game/analytics/game)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameModeId` | `game_mode_id` |  |  | `Y` | gameModeId query parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `sortBy` | `sort_by` |  |  | `Y` | sortBy query parameter. |
| `isNonStatcast` | `is_non_statcast` |  |  | `Y` | isNonStatcast query parameter. |
| `offset` | `offset` |  |  | `Y` | offset query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_analytics_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_analytics_games-example}

```python
mlb_analytics_games()
```

_Last validated n/a._

## mlb_analytics_guids

View timestamps of most recent data corrections made to GUIDs.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/game/analytics/guids`

**Valid URL:** [https://statsapi.mlb.com/api/v1/game/analytics/guids](https://statsapi.mlb.com/api/v1/game/analytics/guids)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameModeId` | `game_mode_id` |  |  | `Y` | gameModeId query parameter. |
| `timecode` | `timecode` |  |  | `Y` | timecode query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `sortBy` | `sort_by` |  |  | `Y` | sortBy query parameter. |
| `isNonStatcast` | `is_non_statcast` |  |  | `Y` | isNonStatcast query parameter. |
| `offset` | `offset` |  |  | `Y` | offset query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_analytics_guids-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_analytics_guids-example}

```python
mlb_analytics_guids()
```

_Last validated n/a._

## mlb_high_low

View high/low stats by player or team.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/highLow/{org_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/highLow/player?statGroup=hitting&sortStat=homeRuns&season=2023](https://statsapi.mlb.com/api/v1/highLow/player?statGroup=hitting&sortStat=homeRuns&season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_type` | `org_type` |  | `Y` |  | org_type path parameter. |
| `statGroup` | `stat_group` |  |  | `Y` | statGroup query parameter. |
| `sortStat` | `sort_stat` |  |  | `Y` | sortStat query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |
| `sportIds` | `sport_ids` |  |  | `Y` | sportIds query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_high_low-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `total_splits` | integer | Total number of splits in the leaderboard. |
| `exemptions` | character | Serialized list of exemption codes or player IDs excluded from the high/low statistical split calculation. |
| `splits` | character | Splits. |
| `splits_tied_with_offset` | character | Players tied at the offset boundary. |
| `splits_tied_with_limit` | character | Players tied at the limit boundary. |
| `season` | character | Season year. |
| `combined_stats` | logical | Whether the stat combines multiple split sources. |
| `group_display_name` | character | Stat group display name. |
| `game_type_id` | character | Game type code (e.g., R for regular season). |
| `game_type_description` | character | Game type description. |
| `sort_stat_name` | character | Snake-case name of the sorted statistic (e.g. 'at_bats'). |
| `sort_stat_lookup_param` | character | API lookup parameter for the sorted statistic (e.g. 'atBats'). |
| `sort_stat_is_counting` | logical | Whether the sorted statistic is a counting stat. |
| `sort_stat_label` | character | Human-readable label of the sorted statistic (e.g. 'At bats'). |
| `sort_stat_stat_groups` | character | Serialized list of statistical group identifiers (e.g., hitting, pitching) used to filter this high/low query. |
| `sort_stat_org_types` | character | Serialized list of organization types (e.g., MLB, MiLB) in scope for this high/low stat sort. |
| `sort_stat_high_low_types` | character | Serialized list of high/low result type codes (e.g., high, low) applicable to this stat leader query. |
| `sort_stat_streak_levels` | character | Serialized list of streak level codes defining the streak lengths tracked in this high/low query. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_high_low-example}

```python
mlb_high_low(org_type='player', stat_group='hitting', sort_stat='homeRuns', season='2023')
```

_Last validated n/a._

## mlb_free_agents

View biographical information and stats for Free Agents.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/people/freeAgents`

**Valid URL:** [https://statsapi.mlb.com/api/v1/people/freeAgents?season=2023](https://statsapi.mlb.com/api/v1/people/freeAgents?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `order` | `order` |  |  | `Y` | order query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_free_agents-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `notes` | character | Notes. |
| `date_declared` | character | Date the player declared free agency (YYYY-MM-DD). |
| `player_id` | integer | stats.ncaa.org player identifier. |
| `player_full_name` | character | Player full name. |
| `player_link` | character | API relative link to the player. |
| `original_team_id` | double | Team id the player left. |
| `original_team_name` | character | Name of the team the player left. |
| `original_team_link` | character | API relative link to the original team. |
| `new_team_link` | character | API relative link to the new team. |
| `position_code` | character | Numeric scorekeeping position code. |
| `position_name` | character | Position name. |
| `position_type` | character | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | character | Position abbreviation. |
| `date_signed` | character | Date the player signed a new contract (YYYY-MM-DD). |
| `new_team_id` | double | Team id the player signed with. |
| `new_team_name` | character | Name of the team the player signed with. |
| `sort_order` | double | Display sort order for the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_free_agents-example}

```python
mlb_free_agents(season='2023')
```

_Last validated n/a._

## mlb_jobs

View directory by jobType.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs?jobType=UMPR](https://statsapi.mlb.com/api/v1/jobs?jobType=UMPR)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `jobType` | `job_type` |  |  | `Y` | jobType query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_jobs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_jobs-example}

```python
mlb_jobs(job_type='UMPR')
```

_Last validated n/a._

## mlb_datacasters

View datacasters directory.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/datacasters`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/datacasters](https://statsapi.mlb.com/api/v1/jobs/datacasters)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_datacasters-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_datacasters-example}

```python
mlb_datacasters()
```

_Last validated n/a._

## mlb_official_scorers

View official scorer directory.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/officialScorers`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/officialScorers](https://statsapi.mlb.com/api/v1/jobs/officialScorers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `date` | `date` |  |  | `Y` | date query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_official_scorers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_number` | character | Jersey number worn (often blank for non-uniformed roles). |
| `job` | character | Job title (e.g. 'Umpire'). |
| `job_id` | character | Job code identifier. |
| `title` | character | Specific role title for the assignment. |
| `person_id` | integer | MLB player ID. |
| `person_full_name` | character | Player full name. |
| `person_link` | character | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_official_scorers-example}

```python
mlb_official_scorers()
```

_Last validated n/a._

## mlb_umpire_games

Get umpires and associated game for umpireId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/jobs/umpires/games/{umpire_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/jobs/umpires/games/596809?season=2023](https://statsapi.mlb.com/api/v1/jobs/umpires/games/596809?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `umpire_id` | `umpire_id` |  | `Y` |  | umpire_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_umpire_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_umpire_games-example}

```python
mlb_umpire_games(umpire_id=596809, season='2023')
```

_Last validated n/a._

## mlb_seasons_all

View information for all seasons based on id.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/seasons/all`

**Valid URL:** [https://statsapi.mlb.com/api/v1/seasons/all?sportId=1](https://statsapi.mlb.com/api/v1/seasons/all?sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `divisionId` | `division_id` |  |  | `Y` | divisionId query parameter. |
| `leagueId` | `league_id` |  |  | `Y` | leagueId query parameter. |
| `withGameTypeDates` | `with_game_type_dates` |  |  | `Y` | withGameTypeDates query parameter. |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_seasons_all-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | stats.ncaa.org season identifier. |
| `has_wildcard` | logical | Whether the season has a wild card round. |
| `pre_season_start_date` | character | Pre-season start date. |
| `season_start_date` | character | Season start date. |
| `regular_season_start_date` | character | Regular season start date. |
| `regular_season_end_date` | character | Regular season end date. |
| `season_end_date` | character | Season end date. |
| `offseason_start_date` | character | Off-season start date. |
| `off_season_end_date` | character | Off-season end date. |
| `season_level_gameday_type` | character | Season-level Gameday data feed type. |
| `game_level_gameday_type` | character | Game-level Gameday data feed type. |
| `qualifier_plate_appearances` | double | Plate appearances per team game to qualify. |
| `qualifier_outs_pitched` | double | Outs pitched per team game to qualify. |
| `post_season_start_date` | character | Post-season start date. |
| `post_season_end_date` | character | Post-season end date. |
| `last_date1st_half` | character | Last date of the first half. |
| `all_star_date` | character | All-Star Game date. |
| `first_date2nd_half` | character | First date of the second half. |
| `pre_season_end_date` | character | Pre-season end date. |
| `spring_start_date` | character | Spring training start date. |
| `spring_end_date` | character | Spring training end date. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_seasons_all-example}

```python
mlb_seasons_all(sport_id=1)
```

_Last validated n/a._

## mlb_sport

View information for any given sportId.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/sports/{sport_id}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/sports/1](https://statsapi.mlb.com/api/v1/sports/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_id` | `sport_id` |  | `Y` |  | sport_id path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_sport-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `code` | character | Fielder detail type code. |
| `link` | character | API link to the game feed. |
| `name` | character | Display name. |
| `abbreviation` | character | Short abbreviation. |
| `sort_order` | integer | Display sort order for the sport. |
| `active_status` | logical | Whether the sport/level is active. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_sport-example}

```python
mlb_sport(sport_id=1)
```

_Last validated n/a._

## mlb_stats_metrics

View Statcast stats.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/stats/metrics`

**Valid URL:** [https://statsapi.mlb.com/api/v1/stats/metrics](https://statsapi.mlb.com/api/v1/stats/metrics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `stats` | `stats` |  |  | `Y` | stats query parameter. |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `gameType` | `game_type` |  |  | `Y` | gameType query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `startDate` | `start_date` |  |  | `Y` | startDate query parameter. |
| `endDate` | `end_date` |  |  | `Y` | endDate query parameter. |
| `venueId` | `venue_id` |  |  | `Y` | venueId query parameter. |
| `minOccurrences` | `min_occurrences` |  |  | `Y` | minOccurrences query parameter. |
| `percentile` | `percentile` |  |  | `Y` | percentile query parameter. |
| `personId` | `person_id` |  |  | `Y` | personId query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `offset` | `offset` |  |  | `Y` | offset query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_stats_metrics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_stats_metrics-example}

```python
mlb_stats_metrics()
```

_Last validated n/a._
