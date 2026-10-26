---
title: OLYMPICS_MBK — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "OLYMPICS_MBK — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# OLYMPICS_MBK — ESPN web API (v3)

`sportsdataverse.olympics_mbk` — 5 endpoints.

## espn_olympics_mbk_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_olympics_mbk_player_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | integer |  |
| `athlete_display_name` | character |  |
| `athlete_short_name` | character |  |
| `athlete_position` | character |  |
| `athlete_jersey` | character |  |
| `athlete_team_id` | integer |  |
| `athlete_team_abbreviation` | character |  |
| `split_name` | character |  |
| `split_category` | character |  |
| `games_played` | character |  |
| `avg_minutes` | character |  |
| `field_goal_pct` | character |  |
| `three_point_pct` | character |  |
| `free_throw_pct` | character |  |
| `avg_rebounds` | character |  |
| `avg_assists` | character |  |
| `avg_blocks` | character |  |
| `avg_steals` | character |  |
| `avg_fouls` | character |  |
| `avg_turnovers` | character |  |
| `avg_points` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_olympics_mbk_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_olympics_mbk_player_overview-example}

```python
espn_olympics_mbk_player_overview(athlete_id='4239')
```

## espn_olympics_mbk_player_stats

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/4239/stats](https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/4239/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_olympics_mbk_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `category` | character | ESPN stat category the row belongs to (e.g., basketball "averages" / "totals" / "miscellaneous", NFL "passing", MLB "career-batting"). The stat columns after `position` are named from each category's own `names` list, so they differ by sport, and the other categories' columns are null on a row. |
| `split_name` | character | Season label of the row (e.g., "2010-11") on the Web v3 payload, or "Career" on the row built from the category's career totals; the split's name on payloads that ship named splits. |
| `split_category` | character | Category of a named split; null on the Web v3 per-season rows. |
| `split_value` | character | Value of a named split; null on the Web v3 per-season rows. |
| `season` | integer | Season end year of the row (e.g., 2011 for 2010-11); null on the "Career" row. |
| `team_id` | integer | ESPN team id (Int64) the athlete played that season for; a traded player has one row per team. Null on the "Career" row. |
| `position` | character | Position abbreviation the athlete was listed at for that season and team (e.g., "G"); null on the "Career" row. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_olympics_mbk_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_olympics_mbk_player_stats-example}

```python
espn_olympics_mbk_player_stats(athlete_id='4239')
```

## espn_olympics_mbk_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_olympics_mbk_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | integer |  |
| `season_type_name` | character |  |
| `category` | character |  |
| `event_id` | integer |  |
| `event_date` | character |  |
| `home_away` | character |  |
| `score` | character |  |
| `opponent_id` | integer |  |
| `opponent_abbreviation` | character |  |
| `opponent_display_name` | character |  |
| `game_result` | character |  |
| `game_processed` | character |  |
| `stat_0` | character |  |
| `stat_1` | character |  |
| `stat_2` | character |  |
| `stat_3` | character |  |
| `stat_4` | character |  |
| `stat_5` | character |  |
| `stat_6` | character |  |
| `stat_7` | character |  |
| `stat_8` | character |  |
| `stat_9` | character |  |
| `stat_10` | character |  |
| `stat_11` | character |  |
| `stat_12` | character |  |
| `stat_13` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_olympics_mbk_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_olympics_mbk_player_gamelog-example}

```python
espn_olympics_mbk_player_gamelog(athlete_id='4239')
```

## espn_olympics_mbk_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_olympics_mbk_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | character |  |
| `display_name` | character |  |
| `display_names` | character |  |
| `filters` | character |  |
| `labels` | character |  |
| `names` | character |  |
| `split_categories` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_olympics_mbk_player_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_olympics_mbk_player_splits-example}

```python
espn_olympics_mbk_player_splits(athlete_id='4239')
```

## espn_olympics_mbk_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-olympics-basketball/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | Leaderboard population, which also fills that category's per-stat ranks. Football takes '<group>:<category>' ('offense:passing', 'offense:rushing', 'offense:receiving', 'defense:defensive', 'scoring:scoring'); a bare 'passing' is rejected with HTTP 400 (ValueError). |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | Sort key '<category>.<statName>:<asc\|desc>', the names in the payload's categories ('passing.passingYards:desc', 'offensive.avgPoints:desc'). A stat without its category is rejected with HTTP 400 (ValueError). |

### Returns {#espn_olympics_mbk_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Place of the athlete in ESPN's leaderboard order (the requested category or sort), counted across pages: (page - 1) * limit + position on the page. |
| `athlete_id` | integer | ESPN id of the athlete, the key the athlete endpoints (athlete_overview, athlete_stats) take. |
| `athlete_uid` | character | ESPN composite athlete uid (s:<sport>~l:<league>~a:<athlete id>), kept as text. |
| `athlete_display_name` | character | Athlete's full display name. |
| `athlete_short_name` | character | Athlete's abbreviated name (e.g. 'B. Nix'). |
| `athlete_first_name` | character | Athlete's first name. |
| `athlete_last_name` | character | Athlete's last name. |
| `athlete_position` | character | Athlete's position abbreviation (e.g. QB, G, C). |
| `athlete_status` | character | Athlete's ESPN status name (e.g. Active, Inactive) as ESPN reports it at request time, not for the requested season. |
| `team_id` | integer | ESPN id of the team the athlete played for in the requested season. |
| `team_abbreviation` | character | Abbreviation of the athlete's team in the requested season (ESPN teamShortName, e.g. ORE). |
| `team_name` | character | Nickname of the athlete's team in the requested season (ESPN teamName, e.g. Ducks). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_olympics_mbk_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_olympics_mbk_leaders-example}

```python
espn_olympics_mbk_leaders()
```
