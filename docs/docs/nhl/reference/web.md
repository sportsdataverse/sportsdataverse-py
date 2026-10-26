---
title: NHL — ESPN web API (v3)
sidebar_label: ESPN web API (v3)
description: "NHL — ESPN web API (v3) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 21
toc_max_heading_level: 2
---
# NHL — ESPN web API (v3)

`sportsdataverse.nhl` — 5 endpoints.

## espn_nhl_player_overview

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/{athlete_id}/overview`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/4239/overview](https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/4239/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nhl_player_overview-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | integer | ESPN athlete identifier (echoed from arg). |
| `athlete_display_name` | character | Player display name. |
| `athlete_short_name` | character | Player short name. |
| `athlete_position` | character | Position abbreviation. |
| `athlete_jersey` | character | Jersey number. |
| `athlete_team_id` | integer | ESPN team identifier. |
| `athlete_team_abbreviation` | character | Team abbreviation. |
| `split_name` | character | Split name (e.g. "All Splits"). |
| `split_category` | character | Split category name (e.g. split, location). |
| `games_played` | character | Games played. |
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

### Raises {#espn_nhl_player_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_player_overview-example}

```python
espn_nhl_player_overview(athlete_id='4239')
```

## espn_nhl_player_stats_v3

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/{athlete_id}/stats`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/4239/stats](https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/4239/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nhl_player_stats_v3-returns}

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

### Raises {#espn_nhl_player_stats_v3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_player_stats_v3-example}

```python
espn_nhl_player_stats_v3(athlete_id='4239')
```

## espn_nhl_player_gamelog

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/{athlete_id}/gamelog`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/4239/gamelog](https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/4239/gamelog)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nhl_player_gamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_type_id` | integer | Season type id (1=pre, 2=reg, 3=post, 4=off). |
| `season_type_name` | character |  |
| `category` | character | Stat leader category. |
| `event_id` | integer | ESPN event id (echoed from arg). |
| `event_date` | character |  |
| `home_away` | character | Home or away indicator. |
| `score` | character | Final score string. |
| `opponent_id` | integer | ESPN opponent team identifier. |
| `opponent_abbreviation` | character |  |
| `opponent_display_name` | character |  |
| `game_result` | character | Game result for the player's team (W/L/OT). |
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

### Raises {#espn_nhl_player_gamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_player_gamelog-example}

```python
espn_nhl_player_gamelog(athlete_id='4239')
```

## espn_nhl_player_splits

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/{athlete_id}/splits`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/4239/splits](https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/athletes/4239/splits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nhl_player_splits-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `descriptions` | character |  |
| `display_name` | character | Player display name. |
| `display_names` | character |  |
| `filters` | character |  |
| `labels` | character |  |
| `names` | character |  |
| `split_categories` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_player_splits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_player_splits-example}

```python
espn_nhl_player_splits(athlete_id='4239')
```

## espn_nhl_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/statistics/byathlete`

**Valid URL:** [https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/statistics/byathlete?limit=50&page=1](https://site.web.api.espn.com/apis/common/v3/sports/hockey/nhl/statistics/byathlete?limit=50&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `category` | `category` |  |  | `Y` | Leaderboard population, which also fills that category's per-stat ranks. Football takes '<group>:<category>' ('offense:passing', 'offense:rushing', 'offense:receiving', 'defense:defensive', 'scoring:scoring'); a bare 'passing' is rejected with HTTP 400 (ValueError). |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `sort` | `sort` |  |  | `Y` | Sort key '<category>.<statName>:<asc\|desc>', the names in the payload's categories ('passing.passingYards:desc', 'offensive.avgPoints:desc'). A stat without its category is rejected with HTTP 400 (ValueError). |

### Returns {#espn_nhl_leaders-returns}

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
| `general_games` | double | Games Played (ESPN 'general' category): Total games played. |
| `general_plus_minus` | double | Plus/Minus Rating (ESPN 'general' category). |
| `general_time_on_ice_per_game` | double | Time On Ice Per Game (ESPN 'general' category): Total time on ice per game. |
| `general_shifts` | double | Shifts (ESPN 'general' category): Total shifts. |
| `general_production` | double | Production (ESPN 'general' category): Total production. |
| `general_wins` | double | Wins (ESPN 'general' category). |
| `general_losses` | double | Losses (ESPN 'general' category). |
| `general_time_on_ice` | double | Time On Ice (ESPN 'general' category): Total time on ice. |
| `offensive_goals` | double | Goals (ESPN 'offensive' category): Total goals scored. |
| `offensive_assists` | double | Assists (ESPN 'Assists', offensive category): passes that lead directly to a score in basketball; goal assists in hockey. |
| `offensive_points` | double | Points (ESPN 'Points', offensive category): points scored in basketball; goals plus assists in hockey. |
| `offensive_power_play_goals` | double | Power Play Goals (ESPN 'offensive' category): Total power play goals. |
| `offensive_power_play_assists` | double | Power Play Assists (ESPN 'offensive' category): Total power play assists. |
| `offensive_shots_total` | double | Shots (ESPN 'offensive' category): Total shots. |
| `offensive_shooting_pct` | double | Shooting Percentage (ESPN 'offensive' category): Percentage of shots scored. |
| `offensive_game_winning_goals` | double | Game-Winning Goals (ESPN 'offensive' category): Total game-winning goals. |
| `offensive_faceoffs_won` | double | Faceoffs Won (ESPN 'offensive' category): Total faceoffs won. |
| `offensive_faceoffs_lost` | double | Faceoffs Lost (ESPN 'offensive' category): Total faceoffs lost. |
| `offensive_faceoff_percent` | double | Faceoff Win Percent (ESPN 'offensive' category): Percentage of faceoffs won. |
| `offensive_shootout_attempts` | double | Shootout Attempts (ESPN 'offensive' category): Total shootout attempts. |
| `offensive_shootout_goals` | double | Shootout Goals (ESPN 'offensive' category): Total shootout goals. |
| `offensive_shootout_shot_pct` | double | Shootout Shot Percentage (ESPN 'offensive' category): Percentage of shootout goals made. |
| `defensive_overtime_losses` | double | Overtime Losses (ESPN 'defensive' category): Total games lost in overtime. |
| `defensive_avg_goals_against` | double | Goals Against Average (ESPN 'defensive' category). |
| `defensive_shots_against` | double | Shots Against (ESPN 'defensive' category): Total shots against. |
| `defensive_goals_against` | double | Goals Against (ESPN 'defensive' category): Total goals against. |
| `defensive_saves` | double | Saves (ESPN 'defensive' category): Total saves. |
| `defensive_save_pct` | double | Save Percentage (ESPN 'defensive' category): Percentage of shots saved. |
| `defensive_shootout_shots_against` | double | Shootout Shots Against (ESPN 'defensive' category): Total shootout shots against. |
| `defensive_shootout_saves` | double | Shootout Saves (ESPN 'defensive' category): Total shootout saves. |
| `defensive_shootout_save_pct` | double | Shootout Save Percentage (ESPN 'defensive' category): Percentage of shootout shots saved. |
| `defensive_shutouts` | double | Shutouts (ESPN 'defensive' category): Total shutouts. |
| `penalties_penalty_minutes` | double | Penalty Minutes (ESPN 'penalties' category): Total penalty minutes. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nhl_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nhl_leaders-example}

```python
espn_nhl_leaders()
```
