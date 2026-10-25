---
title: NFL — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "NFL — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# NFL — ESPN web API (v3)

`sportsdataverse.nfl` — 5 endpoints.

## espn_nfl_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nfl_player_overview-returns}

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

### Raises {#espn_nfl_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_player_overview-example}

```python
espn_nfl_player_overview(athlete_id='4239')
```

## espn_nfl_player_stats_v3

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/3139477/stats](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/3139477/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nfl_player_stats_v3-returns}

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
| `games_played` | character | Games played (GP), as ESPN ships it (a string). |
| `completions` | character | Pass completions (CMP). |
| `passing_attempts` | character | Pass attempts (ATT). |
| `completion_pct` | character | Completion percentage (CMP%). |
| `passing_yards` | character | Passing yards (YDS). |
| `yards_per_pass_attempt` | character | Passing yards per attempt (AVG). |
| `passing_touchdowns` | character | Passing touchdowns (TD). |
| `interceptions` | character | Passes thrown that were intercepted (INT). |
| `long_passing` | character | Longest completed pass in yards (LNG). |
| `sacks` | character | Times sacked (SACK) on the passing line; a defender's sacks on the defensive line. |
| `qb_rating` | character | NFL passer rating (RTG). |
| `adj_qbr` | character | ESPN's adjusted Total QBR (QBR). |
| `rushing_attempts` | character | Rushing attempts (CAR). |
| `rushing_yards` | character | Rushing yards (YDS). |
| `yards_per_rush_attempt` | character | Rushing yards per attempt (AVG). |
| `rushing_touchdowns` | character | Rushing touchdowns (TD). |
| `long_rushing` | character | Longest run in yards (LNG). |
| `rushing_first_downs` | character | First downs gained by rushing (FD). |
| `rushing_fumbles` | character | Fumbles on rushing plays (FUM). |
| `rushing_fumbles_lost` | character | Fumbles on rushing plays that the opponent recovered (LST). |
| `receptions` | character | Receptions (REC). |
| `receiving_targets` | character | Times targeted by a pass (TGTS). |
| `receiving_yards` | character | Receiving yards (YDS). |
| `yards_per_reception` | character | Receiving yards per reception (AVG). |
| `receiving_touchdowns` | character | Receiving touchdowns (TD). |
| `long_reception` | character | Longest reception in yards (LNG). |
| `receiving_first_downs` | character | First downs gained by reception (FD). |
| `receiving_fumbles` | character | Fumbles after a reception (FUM). |
| `receiving_fumbles_lost` | character | Fumbles after a reception that the opponent recovered (LST). |
| `total_tackles` | character | Total tackles (TOT), solo plus assisted. |
| `solo_tackles` | character | Unassisted tackles (SOLO). |
| `assist_tackles` | character | Assisted tackles (AST). |
| `fumbles_forced` | character | Forced fumbles (FF). |
| `fumbles_recovered` | character | Fumbles recovered (FR). |
| `fumbles_recovered_yards` | character | Return yards after fumble recoveries (YDS). |
| `interception_yards` | character | Return yards after interceptions (YDS). |
| `avg_interception_yards` | character | Average return yards per interception (AVG). |
| `interception_touchdowns` | character | Interceptions returned for a touchdown (TD). |
| `long_interception` | character | Longest interception return in yards (LNG). |
| `passes_defended` | character | Passes defended (PD). |
| `stuffs` | character | Runners stopped at or behind the line of scrimmage (STF). |
| `stuff_yards` | character | Yards lost on stuffs (STFYDS). |
| `kicks_blocked` | character | Kicks blocked (KB). |
| `return_touchdowns` | character | Kick and punt return touchdowns (RET). |
| `total_touchdowns` | character | Touchdowns of every kind (TD). |
| `total_two_point_convs` | character | Successful two-point conversions (2PT). |
| `kick_extra_points` | character | Kicked extra points made (PAT). |
| `field_goals` | character | Field goals made (FG). |
| `total_points` | character | Points scored (PTS). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_player_stats_v3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_player_stats_v3-example}

```python
espn_nfl_player_stats_v3(athlete_id='3139477')
```

## espn_nfl_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nfl_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | integer |  |
| `season_type_name` | character |  |
| `category` | character | Broader category of player positions |
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

### Raises {#espn_nfl_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_player_gamelog-example}

```python
espn_nfl_player_gamelog(athlete_id='4239')
```

## espn_nfl_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nfl_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | character |  |
| `display_name` | character | Full name of player |
| `display_names` | character |  |
| `filters` | character |  |
| `labels` | character |  |
| `names` | character |  |
| `split_categories` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_player_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_player_splits-example}

```python
espn_nfl_player_splits(athlete_id='4239')
```

## espn_nfl_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | category query parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | sort query parameter. |

### Returns {#espn_nfl_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athletes` | character | Athletes. |
| `glossary` | character | Glossary. |
| `categories` | character | Categories. |
| `pagination_count` | integer | Pagination count. |
| `pagination_limit` | integer | Pagination limit. |
| `pagination_page` | integer | Pagination page. |
| `pagination_pages` | integer | Pagination pages. |
| `pagination_first` | character | Pagination first. |
| `pagination_next` | character | Pagination next. |
| `pagination_last` | character | Pagination last. |
| `league_id` | integer | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_slug` | character | League slug. |
| `league_short_name` | character | League short name. |
| `current_season_year` | integer | Current season year. |
| `current_season_display_name` | character | Current season display name. |
| `current_season_start_date` | character | Current season start date. |
| `current_season_end_date` | character | Current season end date. |
| `current_season_type_id` | integer | Current season type id. |
| `current_season_type_type` | integer | Current season type type. |
| `current_season_type_name` | character | Current season type name. |
| `current_season_type_start_date` | character | Current season type start date. |
| `current_season_type_end_date` | character | Current season type end date. |
| `current_season_type_week_number` | integer | Ordinal week number within the current season type. |
| `current_season_type_week_start_date` | character | ISO 8601 start date of the current week within the active season type. |
| `current_season_type_week_end_date` | character | ISO 8601 end date of the current week within the active season type (e.g., regular season, playoffs). |
| `current_season_type_week_text` | character | Human-readable label for the current week within the active season type (e.g., 'Week 14'). |
| `requested_season_year` | integer | Requested season year. |
| `requested_season_display_name` | character | Requested season display name. |
| `requested_season_start_date` | character | Requested season start date. |
| `requested_season_end_date` | character | Requested season end date. |
| `requested_season_type_id` | integer | Requested season type id. |
| `requested_season_type_type` | integer | Requested season type type. |
| `requested_season_type_name` | character | Requested season type name. |
| `requested_season_type_start_date` | character | Requested season type start date. |
| `requested_season_type_end_date` | character | Requested season type end date. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_leaders-example}

```python
espn_nfl_leaders()
```
