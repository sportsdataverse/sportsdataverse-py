# NHL — NHL Records API — Goalie

> NHL — NHL Records API — Goalie — function reference in sdv-py, the SportsDataverse Python package.

## nhl_records_goalie_career_stats

Goaltender career statistics (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/goalie-career-stats](https://records.nhl.com/site/api/goalie-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_career_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_player` | Boolean | Indicator of whether the player is active. |
| `first_name` | String | Player first name. |
| `first_season_for_game_type` | Int64 | First season ID for the game type. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_seven_games_played` | String | Game seven games played. |
| `game_seven_losses` | String | Game seven losses. |
| `game_seven_wins` | String | Game seven wins. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games_played` | Int64 | Games played. |
| `goals_against` | Int64 | Goals against. |
| `goals_against_average` | Float64 | Goals against average. |
| `last_name` | String | Player last name. |
| `last_season_for_game_type` | Int64 | Last season ID for the game type. |
| `losses` | Int64 | Losses. |
| `overtime_games_played` | Int64 | Overtime games played. |
| `overtime_goals_against` | Int64 | Overtime goals against. |
| `overtime_goals_against_average` | String | Overtime goals against average. |
| `overtime_losses` | String | Total overtime losses. |
| `overtime_save_pctg` | Float64 | Overtime save percentage. |
| `overtime_shots_against` | Int64 | Overtime shots against. |
| `overtime_ties` | Int64 | Overtime ties. |
| `overtime_time_on_ice` | Float64 | Overtime time on ice (seconds). |
| `overtime_wins` | Int64 | Overtime wins. |
| `player_id` | Int64 | Unique player identifier. |
| `position_code` | String | Player position code. |
| `save_pctg` | Float64 | Save percentage. |
| `saves` | Int64 | Saves made. |
| `seasons_played` | Int64 | Number of seasons played. |
| `shots_against` | Int64 | Shots faced. |
| `shutouts` | Int64 | Shutouts recorded. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `team_names` | String | Team names. |
| `ties` | Int64 | Total ties. |
| `time_on_ice` | Int64 | Time on ice in seconds. |
| `time_on_ice_min_sec` | String | Total time on ice (MM:SS). |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_goalie_career_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_goalie_career_stats-example}

```python
nhl_records_goalie_career_stats()
```

## nhl_records_goalie_career_stats_with_playoffs

Goaltender career stats inclusive of regular season and playoffs.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs`

**Valid URL:** [https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs](https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_career_stats_with_playoffs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_player` | Int64 | Indicator of whether the player is active. |
| `first_name` | String | Player first name. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `games_played` | Int64 | Games played. |
| `goals_against` | Int64 | Goals against. |
| `goals_against_average` | Float64 | Goals against average. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `overtime_losses` | String | Total overtime losses. |
| `player_id` | Int64 | Unique player identifier. |
| `position_code` | String | Player position code. |
| `save_pctg` | Float64 | Save percentage. |
| `saves` | Int64 | Saves made. |
| `shots_against` | Int64 | Shots faced. |
| `shutouts` | Int64 | Shutouts recorded. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `team_names` | String | Team names. |
| `ties` | Int64 | Total ties. |
| `time_on_ice` | Int64 | Time on ice in seconds. |
| `time_on_ice_min_sec` | String | Total time on ice (MM:SS). |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_goalie_career_stats_with_playoffs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_goalie_career_stats_with_playoffs-example}

```python
nhl_records_goalie_career_stats_with_playoffs()
```

## nhl_records_goalie_season_stats

Goaltender single-season statistics.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-season-stats`

**Valid URL:** [https://records.nhl.com/site/api/goalie-season-stats](https://records.nhl.com/site/api/goalie-season-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_season_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_player` | Boolean | Indicator of whether the player is active. |
| `first_name` | String | Player first name. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_seven_games_played` | String | Game seven games played. |
| `game_seven_losses` | String | Game seven losses. |
| `game_seven_wins` | String | Game seven wins. |
| `game_type` | Int64 | Game type the row belongs to. |
| `games_played` | Int64 | Games played. |
| `games_started` | Int64 | Games started (goalies). |
| `goals_against` | Int64 | Goals against. |
| `goals_against_average` | Float64 | Goals against average. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `number_of_games_in_season` | Int64 | Number of games in the season. |
| `overtime_games_played` | Int64 | Overtime games played. |
| `overtime_goals_against` | Int64 | Overtime goals against. |
| `overtime_losses` | String | Total overtime losses. |
| `overtime_ties` | Int64 | Overtime ties. |
| `overtime_wins` | Int64 | Overtime wins. |
| `player_id` | Int64 | Unique player identifier. |
| `position_code` | String | Player position code. |
| `rookie_flag` | Boolean | Indicator of whether the player was a rookie. |
| `save_pctg` | Float64 | Save percentage. |
| `saves` | Int64 | Saves made. |
| `season_id` | Int64 | Season identifier. |
| `shots_against` | Int64 | Shots faced. |
| `shutouts` | Int64 | Shutouts recorded. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `team_names` | String | Team names. |
| `ties` | Int64 | Total ties. |
| `time_on_ice` | Int64 | Time on ice in seconds. |
| `time_on_ice_min_sec` | String | Total time on ice (MM:SS). |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_goalie_season_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_goalie_season_stats-example}

```python
nhl_records_goalie_season_stats()
```

## nhl_records_goalie_win_streak

Goaltenders with the longest consecutive-win streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-win-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-win-streak](https://records.nhl.com/site/api/goalie-win-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_win_streak-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_player` | Boolean | Indicator of whether the player is active. |
| `active_streak` | Boolean | Indicator of whether the streak is active. |
| `end_date` | String | Season end date. |
| `first_name` | String | Player first name. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `last_name` | String | Player last name. |
| `player_id` | Int64 | Unique player identifier. |
| `rookie` | Boolean | Whether the player is a rookie. |
| `season_id` | Int64 | Season identifier. |
| `start_date` | String | Season start date. |
| `team_abbrev` | String | Team abbreviation. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `win_streak` | Int64 | Number of consecutive wins recorded by the goalie in this streak. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_goalie_win_streak-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_goalie_win_streak-example}

```python
nhl_records_goalie_win_streak()
```

## nhl_records_goalie_shutout_streak

Goaltenders with the longest consecutive-shutout streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-shutout-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-shutout-streak](https://records.nhl.com/site/api/goalie-shutout-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_shutout_streak-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_player` | Boolean | Indicator of whether the player is active. |
| `active_streak` | Boolean | Indicator of whether the streak is active. |
| `duration_min_sec` | String | Streak duration (MM:SS). |
| `duration_seconds` | Int64 | Streak duration in seconds. |
| `end_date` | String | Season end date. |
| `first_name` | String | Player first name. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `last_name` | String | Player last name. |
| `player_id` | Int64 | Unique player identifier. |
| `saves` | String | Saves made. |
| `season_id` | Int64 | Season identifier. |
| `start_date` | String | Season start date. |
| `team_abbrev` | String | Team abbreviation. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_goalie_shutout_streak-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_goalie_shutout_streak-example}

```python
nhl_records_goalie_shutout_streak()
```

## nhl_records_goalie_win_plateaus

Goaltenders who reached each win plateau (100, 200, 300 …).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-win-plateaus`

**Valid URL:** [https://records.nhl.com/site/api/goalie-win-plateaus](https://records.nhl.com/site/api/goalie-win-plateaus)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_win_plateaus-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_player` | Boolean | Indicator of whether the player is active. |
| `first_name` | String | Player first name. |
| `forty_win_seasons` | Int64 | Number of seasons in which the goalie recorded 40 or more wins, a rare single-season achievement. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `last_name` | String | Player last name. |
| `player_id` | Int64 | Unique player identifier. |
| `seasons_played` | Int64 | Number of seasons played. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `team_names` | String | Team names. |
| `thirty_win_seasons` | Int64 | Number of seasons in which the goalie recorded 30 or more wins. |
| `twenty_win_seasons` | Int64 | Number of seasons in which the goalie recorded 20 or more wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_goalie_win_plateaus-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_goalie_win_plateaus-example}

```python
nhl_records_goalie_win_plateaus()
```

## nhl_records_goalie_playoff_streak

Goaltender consecutive playoff-win streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-playoff-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-playoff-streak](https://records.nhl.com/site/api/goalie-playoff-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_playoff_streak-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_player` | Boolean | Indicator of whether the player is active. |
| `active_streak` | Boolean | Indicator of whether the streak is active. |
| `consecutive_playoff_seasons` | Int64 | Number of consecutive playoff seasons in which the goalie appeared for the franchise during this streak. |
| `end_season` | Int64 | Last season (in YYYYYYYY format) of the goalie's consecutive playoff appearance streak. |
| `first_name` | String | Player first name. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `last_name` | String | Player last name. |
| `player_id` | Int64 | Unique player identifier. |
| `playoff_seasons` | Int64 | Number of playoff seasons. |
| `stanley_cup_wins` | Int64 | Number of Stanley Cup championships. |
| `start_season` | Int64 | First season (in YYYYYYYY format) of the goalie's consecutive playoff appearance streak. |
| `team_abbrevs` | String | Team abbreviation(s). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_goalie_playoff_streak-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_goalie_playoff_streak-example}

```python
nhl_records_goalie_playoff_streak()
```

## nhl_records_goalie_undefeated_streak

Goaltender longest undefeated streaks (wins + ties).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-undefeated-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-undefeated-streak](https://records.nhl.com/site/api/goalie-undefeated-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_goalie_undefeated_streak-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_player` | Boolean | Indicator of whether the player is active. |
| `active_streak` | Boolean | Indicator of whether the streak is active. |
| `end_date` | String | Season end date. |
| `first_name` | String | Player first name. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `last_name` | String | Player last name. |
| `player_id` | Int64 | Unique player identifier. |
| `season_id` | Int64 | Season identifier. |
| `start_date` | String | Season start date. |
| `team_abbrev` | String | Team abbreviation. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `undefeated_streak` | Int64 | Number of consecutive games without a regulation loss (wins plus overtime or shootout losses) in the goalie's record streak. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_goalie_undefeated_streak-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_goalie_undefeated_streak-example}

```python
nhl_records_goalie_undefeated_streak()
```
