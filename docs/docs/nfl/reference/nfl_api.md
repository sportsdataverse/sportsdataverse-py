---
title: NFL — NFL.com API
sidebar_label: NFL.com API
sidebar_position: 10
---
# NFL — NFL.com API

`sportsdataverse.nfl` — 11 endpoints.

## `nfl_standings`

GET /football/v2/standings — one row per team standing across the returned week(s).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/standings`

**Valid URL:** [https://api.nfl.com/football/v2/standings?season=2024&seasonType=REG&week=18](https://api.nfl.com/football/v2/standings?season=2024&seasonType=REG&week=18)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season (football). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

GET /football/v2/standings — one row per team standing across the returned week(s).

### Example

```python
nfl_standings(season=2024, season_type='REG', week=18)
```

_Last validated n/a._

## `nfl_rosters`

GET /football/v2/rosters — one row per team roster for the season.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/rosters`

**Valid URL:** [https://api.nfl.com/football/v2/rosters?season=2024](https://api.nfl.com/football/v2/rosters?season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

GET /football/v2/rosters — one row per team roster for the season.

### Example

```python
nfl_rosters(season=2024)
```

_Last validated n/a._

## `nfl_teams_history`

GET /football/v2/teams/history — one row per team for a season.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/teams/history`

**Valid URL:** [https://api.nfl.com/football/v2/teams/history?season=2024](https://api.nfl.com/football/v2/teams/history?season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

GET /football/v2/teams/history — one row per team for a season.

### Example

```python
nfl_teams_history(season=2024)
```

_Last validated n/a._

## `nfl_team`

GET /football/v2/teams/{team_id} — single-team detail (one row).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/teams/{team_id}`

**Valid URL:** [https://api.nfl.com/football/v2/teams/10403800-517c-7b8c-65a3-c61b95d86123](https://api.nfl.com/football/v2/teams/10403800-517c-7b8c-65a3-c61b95d86123)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

GET /football/v2/teams/{team_id} — single-team detail (one row).

### Example

```python
nfl_team(team_id='10403800-517c-7b8c-65a3-c61b95d86123')
```

_Last validated n/a._

## `nfl_weeks`

GET /football/v2/weeks/season/{season}/seasonType/{season_type} — week calendar (one row per week).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/weeks/season/{season}/seasonType/{season_type}`

**Valid URL:** [https://api.nfl.com/football/v2/weeks/season/2024/seasonType/REG](https://api.nfl.com/football/v2/weeks/season/2024/seasonType/REG)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | season path parameter. |
| `season_type` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |

### Returns

GET /football/v2/weeks/season/{season}/seasonType/{season_type} — week calendar (one row per week).

### Example

```python
nfl_weeks(season=2024, season_type='REG')
```

_Last validated n/a._

## `nfl_weeks_by_date`

GET /football/v2/weeks/date/{YYYY-MM-DD} — the week containing a date (one row).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/weeks/date/{date}`

**Valid URL:** [https://api.nfl.com/football/v2/weeks/date/2024-09-08](https://api.nfl.com/football/v2/weeks/date/2024-09-08)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  | `Y` |  | date path parameter. |

### Returns

GET /football/v2/weeks/date/{YYYY-MM-DD} — the week containing a date (one row).

### Example

```python
nfl_weeks_by_date(date='2024-09-08')
```

_Last validated n/a._

## `nfl_combine_profiles`

GET /football/v2/combine/profiles — one row per combine prospect.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/combine/profiles`

**Valid URL:** [https://api.nfl.com/football/v2/combine/profiles?year=2024](https://api.nfl.com/football/v2/combine/profiles?year=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

GET /football/v2/combine/profiles — one row per combine prospect.

### Example

```python
nfl_combine_profiles(year=2024)
```

_Last validated n/a._

## `nfl_draft_picks`

GET /football/v2/draft/picks/report — one row per draft pick.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/draft/picks/report`

**Valid URL:** [https://api.nfl.com/football/v2/draft/picks/report?year=2024](https://api.nfl.com/football/v2/draft/picks/report?year=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

GET /football/v2/draft/picks/report — one row per draft pick.

### Example

```python
nfl_draft_picks(year=2024)
```

_Last validated n/a._

## `nfl_injuries`

GET /football/v2/injuries — one row per injured player.

**Endpoint URL:** `GET https://api.nfl.com/football/v2/injuries`

**Valid URL:** [https://api.nfl.com/football/v2/injuries?season=2024&seasonType=REG&week=1](https://api.nfl.com/football/v2/injuries?season=2024&seasonType=REG&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season (football). |

### Returns

GET /football/v2/injuries — one row per injured player.

### Example

```python
nfl_injuries(season=2024, season_type='REG', week=1)
```

_Last validated n/a._

## `nfl_game_summaries`

GET /football/v2/stats/live/game-summaries — one row per game (live state).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/stats/live/game-summaries`

**Valid URL:** [https://api.nfl.com/football/v2/stats/live/game-summaries?season=2024&seasonType=REG&week=1](https://api.nfl.com/football/v2/stats/live/game-summaries?season=2024&seasonType=REG&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasonType` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season (football). |

### Returns

GET /football/v2/stats/live/game-summaries — one row per game (live state).

### Example

```python
nfl_game_summaries(season=2024, season_type='REG', week=1)
```

_Last validated n/a._

## `nfl_weekly_game_details`

GET /football/v2/experience/weekly-game-details — one row per game (bare list).

**Endpoint URL:** `GET https://api.nfl.com/football/v2/experience/weekly-game-details`

**Valid URL:** [https://api.nfl.com/football/v2/experience/weekly-game-details?season=2024&type=REG&week=1](https://api.nfl.com/football/v2/experience/weekly-game-details?season=2024&type=REG&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `type` | `season_type` |  |  | `Y` | Season type code (string): PRE, REG, or POST (sent as the `type` query param) -- not ESPN's numeric 1/2/3. |
| `week` | `week` |  |  | `Y` | Week number within the season (football). |
| `includeDriveChart` | `include_drive_chart` |  |  | `Y` | includeDriveChart query parameter. |
| `includeReplays` | `include_replays` |  |  | `Y` | includeReplays query parameter. |
| `includeStandings` | `include_standings` |  |  | `Y` | includeStandings query parameter. |
| `includeTaggedVideos` | `include_tagged_videos` |  |  | `Y` | includeTaggedVideos query parameter. |

### Returns

GET /football/v2/experience/weekly-game-details — one row per game (bare list).

### Example

```python
nfl_weekly_game_details(season=2024, season_type='REG', week=1)
```

_Last validated n/a._
