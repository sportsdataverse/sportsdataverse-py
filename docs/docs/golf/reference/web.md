---
title: Golf — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "Golf — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# Golf — ESPN web API (v3)

`sportsdataverse.golf` — 2 endpoints.

## espn_golf_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/golf/pga/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/golf/pga/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/golf/pga/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_golf_player_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | Int64 |  |
| `athlete_display_name` | String |  |
| `athlete_short_name` | String |  |
| `athlete_position` | String |  |
| `athlete_jersey` | String |  |
| `athlete_team_id` | Int64 |  |
| `athlete_team_abbreviation` | String |  |
| `split_name` | String |  |
| `split_category` | String |  |
| `games_played` | String |  |
| `avg_minutes` | String |  |
| `field_goal_pct` | String |  |
| `three_point_pct` | String |  |
| `free_throw_pct` | String |  |
| `avg_rebounds` | String |  |
| `avg_assists` | String |  |
| `avg_blocks` | String |  |
| `avg_steals` | String |  |
| `avg_fouls` | String |  |
| `avg_turnovers` | String |  |
| `avg_points` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_player_overview-example}

```python
espn_golf_player_overview(league='pga', athlete_id='4239')
```

## espn_golf_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/golf/pga/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/golf/pga/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/golf/pga/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | Leaderboard population: the athletes ranked in that category, whose values it fills (other categories come back null). Football takes `<group>:<category>` (`offense:passing`, `offense:rushing`, `offense:receiving`, `defense:defensive`, `scoring:scoring`); a bare `passing` is rejected with HTTP 400 (ValueError). Basketball takes the bare name (`offensive`). |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | Sort key `<category>.<statName>:<asc\|desc>`, the names in the payload's categories (`passing.passingYards:desc`, `offensive.avgPoints:desc`). A stat without its category is rejected with HTTP 400 (ValueError). |

### Returns {#espn_golf_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `sort_order` | Int64 | Position of the athlete in ESPN's leaderboard order (the requested category or sort), counted across pages: (page - 1) * limit + position on the page. A position, not a rank: tied athletes still count up; ESPN's per-stat ranks stay in the raw payload (return_parsed=False). |
| `athlete_id` | Int64 | ESPN id of the athlete, the key the athlete endpoints (athlete_overview, athlete_stats) take. |
| `athlete_uid` | String | ESPN composite athlete uid (`s:<sport>~l:<league>~a:<athlete id>`), kept as text. |
| `athlete_display_name` | String | Athlete's full display name. |
| `athlete_short_name` | String | Athlete's abbreviated name (e.g. 'B. Nix'). |
| `athlete_first_name` | String | Athlete's first name. |
| `athlete_last_name` | String | Athlete's last name. |
| `athlete_position` | String | Athlete's position abbreviation (e.g. QB, G, C). |
| `athlete_status` | String | Athlete's ESPN status name (e.g. Active, Inactive) as ESPN reports it at request time, not for the requested season. |
| `team_id` | Int64 | ESPN id of the team the athlete played for in the requested season. |
| `team_abbreviation` | String | Abbreviation of the athlete's team in the requested season (ESPN teamShortName, e.g. ORE). |
| `team_name` | String | Nickname of the athlete's team in the requested season (ESPN teamName, e.g. Ducks). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_leaders-example}

```python
espn_golf_leaders(league='pga')
```
