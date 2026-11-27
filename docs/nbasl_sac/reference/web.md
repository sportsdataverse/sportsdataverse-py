# NBA Summer League (Sacramento) — ESPN web API (v3)

> NBA Summer League (Sacramento) — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.basketball.nbasl_sac` — 5 endpoints.

## espn_nbasl_sac_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nbasl_sac_player_overview-returns}

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

### Raises {#espn_nbasl_sac_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nbasl_sac_player_overview-example}

```python
espn_nbasl_sac_player_overview(athlete_id='4239')
```

## espn_nbasl_sac_player_stats

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/4239/stats](https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/4239/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nbasl_sac_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | String | ESPN stat category the row belongs to (e.g., basketball "averages" / "totals" / "miscellaneous", NFL "passing", MLB "career-batting"). The stat columns after `position` are named from each category's own `names` list, so they differ by sport, and the other categories' columns are null on a row. |
| `split_name` | String | Season label of the row (e.g., "2010-11") on the Web v3 payload, or "Career" on the row built from the category's career totals; the split's name on payloads that ship named splits. |
| `split_category` | String | Category of a named split; null on the Web v3 per-season rows. |
| `split_value` | String | Value of a named split; null on the Web v3 per-season rows. |
| `season` | Int64 | Season end year of the row (e.g., 2011 for 2010-11); null on the "Career" row. |
| `team_id` | Int64 | ESPN team id (Int64) the athlete played that season for; a traded player has one row per team. Null on the "Career" row. |
| `position` | String | Position abbreviation the athlete was listed at for that season and team (e.g., "G"); null on the "Career" row. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nbasl_sac_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nbasl_sac_player_stats-example}

```python
espn_nbasl_sac_player_stats(athlete_id='4239')
```

## espn_nbasl_sac_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nbasl_sac_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | Int64 | ESPN season type of the row (2 regular season, 3 postseason). |
| `season_type_name` | String | Season type's name as ESPN ships it (e.g., "2019-20 Regular Season"). |
| `category` | String | Split the row was listed under; on the Web v3 payload a month (e.g., "january"). |
| `event_id` | Int64 | ESPN event (game) id (Int64), the key the game endpoints take. |
| `event_date` | String | Scheduled game start, ISO 8601 UTC as ESPN ships it. |
| `home_away` | String | The athlete's side, "home" or "away", from which side the opponent was on; null when ESPN names neither. |
| `score` | String | Final score as ESPN prints it, winner first (e.g., "123-115"). |
| `opponent_id` | Int64 | ESPN team id (Int64) of the opponent. |
| `opponent_abbreviation` | String | Opponent's abbreviation (e.g., "BOS"). |
| `opponent_display_name` | String | Opponent's full name (e.g., "Boston Celtics"). |
| `game_result` | String | The athlete's team result, "W" or "L" (or "T"). |
| `game_processed` | String | Whether ESPN has finished processing the game's stats. |
| `minutes` | String | Minutes played, a whole-number string as ESPN ships it (e.g., "21"). The stat columns from here on are named from the payload's own stat `names`, so they differ by sport, and every value is a string. |
| `field_goals_made_field_goals_attempted` | String | Field goals made-attempted, a "made-attempted" string (e.g., "1-4"). |
| `field_goal_pct` | String | Field-goal percentage on a 0-100 scale, as a string (e.g., "25.0"). |
| `three_point_field_goals_made_three_point_field_goals_attempted` | String | Three-pointers made-attempted, a "made-attempted" string (e.g., "0-2"). |
| `three_point_pct` | String | Three-point percentage on a 0-100 scale, as a string. |
| `free_throws_made_free_throws_attempted` | String | Free throws made-attempted, a "made-attempted" string. |
| `free_throw_pct` | String | Free-throw percentage on a 0-100 scale, as a string. |
| `total_rebounds` | String | Total rebounds in the game. |
| `assists` | String | Assists in the game. |
| `blocks` | String | Blocked shots in the game. |
| `steals` | String | Steals in the game. |
| `fouls` | String | Personal fouls in the game. |
| `turnovers` | String | Turnovers in the game. |
| `points` | String | Points the athlete scored in the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nbasl_sac_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nbasl_sac_player_gamelog-example}

```python
espn_nbasl_sac_player_gamelog(athlete_id='4239')
```

## espn_nbasl_sac_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nbasl_sac_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | String |  |
| `display_name` | String |  |
| `display_names` | String |  |
| `filters` | String |  |
| `labels` | String |  |
| `names` | String |  |
| `split_categories` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nbasl_sac_player_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nbasl_sac_player_splits-example}

```python
espn_nbasl_sac_player_splits(athlete_id='4239')
```

## espn_nbasl_sac_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba-summer-sacramento/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | Leaderboard population: the athletes ranked in that category, whose values it fills (other categories come back null). Football takes `<group>:<category>` (`offense:passing`, `offense:rushing`, `offense:receiving`, `defense:defensive`, `scoring:scoring`); a bare `passing` is rejected with HTTP 400 (ValueError). Basketball takes the bare name (`offensive`). |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | Sort key `<category>.<statName>:<asc\|desc>`, the names in the payload's categories (`passing.passingYards:desc`, `offensive.avgPoints:desc`). A stat without its category is rejected with HTTP 400 (ValueError). |

### Returns {#espn_nbasl_sac_leaders-returns}

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

### Raises {#espn_nbasl_sac_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nbasl_sac_leaders-example}

```python
espn_nbasl_sac_leaders()
```
