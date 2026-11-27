# MLB — MLB Stats API — Other: meta–stats_metrics

> MLB — MLB Stats API — Other: meta–stats_metrics — function reference in sdv-py, the SportsDataverse Python package.

## mlb_meta

GET /api/v1/{metaType} — enum lookup (the API's self-describing surface).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/{meta_type}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/leagueLeaderTypes](https://statsapi.mlb.com/api/v1/leagueLeaderTypes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `meta_type` | `meta_type` |  | `Y` |  | meta_type path parameter. |

### Returns {#mlb_meta-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_meta-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_meta-example}

```python
mlb_meta(meta_type='leagueLeaderTypes')
```

## mlb_awards

GET /api/v1/awards — list award IDs (call with no params to enumerate).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/awards`

**Valid URL:** [https://statsapi.mlb.com/api/v1/awards](https://statsapi.mlb.com/api/v1/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |

### Returns {#mlb_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Id. |
| `name` | String | Display name. |
| `description` | String | Long-form description text. |
| `sort_order` | Float64 | Display sort order for the sport. |
| `active` | Boolean | Whether the player is currently active. |
| `sport_id` | Float64 | Sport MLBAM ID. |
| `sport_link` | String | API link to the sport. |
| `league_id` | Float64 | League MLBAM ID. |
| `league_link` | String | API link to the league. |
| `notes` | String | Notes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_awards-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_awards-example}

```python
mlb_awards()
```

## mlb_award_recipients

GET /api/v1/awards/{awardId}/recipients — historical winners of one award.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/awards/{award_id}/recipients`

**Valid URL:** [https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients?sportId=1](https://statsapi.mlb.com/api/v1/awards/MLBHOF/recipients?sportId=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `sportId` | `sport_id` |  |  | `Y` | sportId query parameter. |
| `hydrate` | `hydrate` |  |  | `Y` | hydrate query parameter. |

### Returns {#mlb_award_recipients-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Id. |
| `name` | String | Display name. |
| `date` | String | Date in YYYY-MM-DD format. |
| `season` | String | Season year. |
| `team_id` | Int64 | Unique ESPN team identifier. |
| `team_link` | String | API link to the team. |
| `player_id` | Int64 | stats.ncaa.org player identifier. |
| `player_link` | String | API relative link to the player. |
| `player_primary_position_code` | String | Recipient primary fielding position code. |
| `player_primary_position_name` | String | Recipient primary fielding position name. |
| `player_primary_position_type` | String | Participant primary position type (e.g. 'Hitter'). |
| `player_primary_position_abbreviation` | String | Participant primary position abbreviation (e.g. 'DH'). |
| `player_name_first_last` | String | Participant name in first-last order. |
| `votes` | Float64 | Number of votes received. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_award_recipients-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_award_recipients-example}

```python
mlb_award_recipients(award_id='MLBHOF')
```

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
| `jersey_number` | String | Jersey number worn (often blank for non-uniformed roles). |
| `job` | String | Job title (e.g. 'Umpire'). |
| `job_id` | String | Job code identifier. |
| `title` | String | Specific role title for the assignment. |
| `person_id` | Int64 | MLB player ID. |
| `person_full_name` | String | Player full name. |
| `person_link` | String | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_umpires-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_umpires-example}

```python
mlb_umpires()
```

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
| `id` | Int64 | Id. |
| `name` | String | Display name. |
| `link` | String | API link to the game feed. |
| `abbreviation` | String | Short abbreviation. |
| `has_wildcard` | Boolean | Whether the season has a wild card round. |
| `name_short` | String |  |
| `league_id` | Int64 | League MLBAM ID. |
| `league_link` | String | API link to the league. |
| `sport_id` | Int64 | Sport MLBAM ID. |
| `sport_link` | String | API link to the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_conferences-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_conferences-example}

```python
mlb_conferences()
```

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
| `id` | Int64 | Id. |
| `name` | String | Display name. |
| `link` | String | API link to the game feed. |
| `abbreviation` | String | Short abbreviation. |
| `has_wildcard` | Boolean | Whether the season has a wild card round. |
| `name_short` | String |  |
| `league_id` | Int64 | League MLBAM ID. |
| `league_link` | String | API link to the league. |
| `sport_id` | Int64 | Sport MLBAM ID. |
| `sport_link` | String | API link to the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_conference-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_conference-example}

```python
mlb_conference(conference_id=301)
```

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

**`return_parsed=True`** (default) — the output of `parse_mlb_api_list`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 404 through the residential proxy with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_analytics_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_analytics_games-example}

```python
mlb_analytics_games()
```

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

**`return_parsed=True`** (default) — the output of `parse_mlb_api_list`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 401 'Please Login' through the residential proxy; the route needs an MLB login session the package does not hold.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_analytics_guids-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_analytics_guids-example}

```python
mlb_analytics_guids()
```

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
| `total_splits` | Int64 | Total number of splits in the leaderboard. |
| `exemptions` | String | Serialized list of exemption codes or player IDs excluded from the high/low statistical split calculation. |
| `splits` | String | Splits. |
| `splits_tied_with_offset` | String | Players tied at the offset boundary. |
| `splits_tied_with_limit` | String | Players tied at the limit boundary. |
| `season` | String | Season year. |
| `combined_stats` | Boolean | Whether the stat combines multiple split sources. |
| `group_display_name` | String | Stat group display name. |
| `game_type_id` | String | Game type code (e.g., R for regular season). |
| `game_type_description` | String | Game type description. |
| `sort_stat_name` | String | Snake-case name of the sorted statistic (e.g. 'at_bats'). |
| `sort_stat_lookup_param` | String | API lookup parameter for the sorted statistic (e.g. 'atBats'). |
| `sort_stat_is_counting` | Boolean | Whether the sorted statistic is a counting stat. |
| `sort_stat_label` | String | Human-readable label of the sorted statistic (e.g. 'At bats'). |
| `sort_stat_stat_groups` | String | Serialized list of statistical group identifiers (e.g., hitting, pitching) used to filter this high/low query. |
| `sort_stat_org_types` | String | Serialized list of organization types (e.g., MLB, MiLB) in scope for this high/low stat sort. |
| `sort_stat_high_low_types` | String | Serialized list of high/low result type codes (e.g., high, low) applicable to this stat leader query. |
| `sort_stat_streak_levels` | String | Serialized list of streak level codes defining the streak lengths tracked in this high/low query. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_high_low-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_high_low-example}

```python
mlb_high_low(org_type='player', stat_group='hitting', sort_stat='homeRuns', season='2023')
```

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
| `notes` | String | Notes. |
| `date_declared` | String | Date the player declared free agency (YYYY-MM-DD). |
| `player_id` | Int64 | stats.ncaa.org player identifier. |
| `player_full_name` | String | Player full name. |
| `player_link` | String | API relative link to the player. |
| `original_team_id` | Float64 | Team id the player left. |
| `original_team_name` | String | Name of the team the player left. |
| `original_team_link` | String | API relative link to the original team. |
| `new_team_link` | String | API relative link to the new team. |
| `position_code` | String | Numeric scorekeeping position code. |
| `position_name` | String | Position name. |
| `position_type` | String | Position category (e.g. 'Pitcher', 'Infielder'). |
| `position_abbreviation` | String | Position abbreviation. |
| `date_signed` | String | Date the player signed a new contract (YYYY-MM-DD). |
| `new_team_id` | Float64 | Team id the player signed with. |
| `new_team_name` | String | Name of the team the player signed with. |
| `sort_order` | Float64 | Display sort order for the sport. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_free_agents-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_free_agents-example}

```python
mlb_free_agents(season='2023')
```

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
| `jersey_number` | String | Jersey number worn (often blank for non-uniformed roles). |
| `job` | String | Job title (e.g. 'Umpire'). |
| `job_id` | String | Job code identifier. |
| `title` | String | Specific role title for the assignment. |
| `person_id` | Int64 | MLB player ID. |
| `person_full_name` | String | Player full name. |
| `person_link` | String | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_jobs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_jobs-example}

```python
mlb_jobs(job_type='UMPR')
```

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
| `jersey_number` | String | Jersey number worn (often blank for non-uniformed roles). |
| `job` | String | Job title (e.g. 'Umpire'). |
| `job_id` | String | Job code identifier. |
| `title` | String | Specific role title for the assignment. |
| `person_id` | Int64 | MLB player ID. |
| `person_full_name` | String | Player full name. |
| `person_link` | String | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_datacasters-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_datacasters-example}

```python
mlb_datacasters()
```

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
| `jersey_number` | String | Jersey number worn (often blank for non-uniformed roles). |
| `job` | String | Job title (e.g. 'Umpire'). |
| `job_id` | String | Job code identifier. |
| `title` | String | Specific role title for the assignment. |
| `person_id` | Int64 | MLB player ID. |
| `person_full_name` | String | Player full name. |
| `person_link` | String | API relative link to the person. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_official_scorers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_official_scorers-example}

```python
mlb_official_scorers()
```

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

**`return_parsed=True`** (default) — the output of `parse_mlb_api_list`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 401 'Please Login' through the residential proxy; the route needs an MLB login session the package does not hold.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_umpire_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_umpire_games-example}

```python
mlb_umpire_games(umpire_id=596809, season='2023')
```

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
| `season_id` | String | stats.ncaa.org season identifier. |
| `has_wildcard` | Boolean | Whether the season has a wild card round. |
| `pre_season_start_date` | String | Pre-season start date. |
| `season_start_date` | String | Season start date. |
| `regular_season_start_date` | String | Regular season start date. |
| `regular_season_end_date` | String | Regular season end date. |
| `season_end_date` | String | Season end date. |
| `offseason_start_date` | String | Off-season start date. |
| `off_season_end_date` | String | Off-season end date. |
| `season_level_gameday_type` | String | Season-level Gameday data feed type. |
| `game_level_gameday_type` | String | Game-level Gameday data feed type. |
| `qualifier_plate_appearances` | Float64 | Plate appearances per team game to qualify. |
| `qualifier_outs_pitched` | Float64 | Outs pitched per team game to qualify. |
| `post_season_start_date` | String | Post-season start date. |
| `post_season_end_date` | String | Post-season end date. |
| `last_date1st_half` | String | Last date of the first half. |
| `all_star_date` | String | All-Star Game date. |
| `first_date2nd_half` | String | First date of the second half. |
| `pre_season_end_date` | String | Pre-season end date. |
| `spring_start_date` | String | Spring training start date. |
| `spring_end_date` | String | Spring training end date. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_seasons_all-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_seasons_all-example}

```python
mlb_seasons_all(sport_id=1)
```

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
| `id` | Int64 | Id. |
| `code` | String | Fielder detail type code. |
| `link` | String | API link to the game feed. |
| `name` | String | Display name. |
| `abbreviation` | String | Short abbreviation. |
| `sort_order` | Int64 | Display sort order for the sport. |
| `active_status` | Boolean | Whether the sport/level is active. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_sport-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_sport-example}

```python
mlb_sport(sport_id=1)
```

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

**`return_parsed=True`** (default) — the output of `parse_mlb_api_list`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: statsapi.mlb.com answers 406 to the datacenter IP and 401 'Please Login' through the residential proxy; the route needs an MLB login session the package does not hold.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_stats_metrics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_stats_metrics-example}

```python
mlb_stats_metrics()
```
