# SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Player

> SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Player — function reference in sdv-py, the SportsDataverse Python package.

## sihf_player_stats

Skater season totals: games, goals, assists, points, penalty minutes, plus-minus.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=player&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=player&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase (player / goalie tables), every game (results), the regular-season table (standings). |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"). |
| `team_id` | Int64 | SIHF team id (Int64 join key). |
| `team_name` | String | Team display name as SIHF publishes it. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"). |
| `position` | String | Position, German whatever the language (e.g., "Stürmer"). |
| `games_played` | Int64 | Games the player played in the selection. |
| `goals` | Int64 | Goals scored by the player. |
| `assists` | Int64 | Assists (primary + secondary). |
| `points` | Int64 | Points (goals + assists). |
| `points_per_game` | Float64 | Points per game. |
| `pim_total` | Int64 | Penalty minutes. |
| `plus_minus` | Int64 | Plus-minus: even-strength and shorthanded goals for, minus against, while on ice. |
| `row_type` | String | "data" for a regular row; "average" or "total" for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_player_stats-example}

```python
sihf_player_stats(season=2026, league_id=1)
```

## sihf_player_goals_assists

Skater goals and assists by situation: even strength, power play, short-handed, game-winning and overtime goals, hat tricks.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=playerGoalAssist&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=playerGoalAssist&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_player_goals_assists-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | String | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | Int64 | Games played in the table's scope. |
| `goals` | Int64 | Goals scored (total). |
| `assists` | Int64 | Assists credited. |
| `assists1` | Int64 | Primary assists. |
| `assists2` | Int64 | Secondary assists. |
| `game_winning_goals` | Int64 | Game-winning goals. |
| `eq_goals` | Int64 | Even-strength goals. |
| `eq_assists` | Int64 | Even-strength assists. |
| `pp_goals` | Int64 | Power-play goals. |
| `pp_assists` | Int64 | Power-play assists. |
| `pk_goals` | Int64 | Short-handed goals. |
| `pk_assists` | Int64 | Short-handed assists. |
| `overtime_goals` | Int64 | Overtime goals. |
| `hattricks` | Int64 | Hat tricks (games with three or more goals). |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_player_goals_assists-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_player_goals_assists-example}

```python
sihf_player_goals_assists(season=2026, league_id=1)
```

## sihf_player_shots

Skater shots: on goal (total, slot, shooting percentage), missed, post and blocked, with per-game rates.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=playerShotDetail&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=playerShotDetail&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_player_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | String | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | Int64 | Games played in the table's scope. |
| `sog_sog` | Int64 | Shots on goal (saved or scored). |
| `sog_sog_percent` | Float64 | Shooting percentage (goals / shots on goal). |
| `sog_sog_quotient` | Float64 | Shots on goal per game. |
| `sog_sog_slot` | Int64 | Slot shots on goal. |
| `sog_sog_slot_per_game` | Float64 | Slot shots on goal per game. |
| `sog_missed` | Int64 | Missed shots, total. |
| `sog_missed_per_game` | Float64 | Missed shots per game. |
| `sog_post` | Int64 | Post/crossbar shots, total. |
| `sog_post_per_game` | Float64 | Post/crossbar shots per game. |
| `blockings` | Int64 | Number of shots blocked. |
| `blockings_per_game` | Float64 | Blocked shots per game. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_player_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_player_shots-example}

```python
sihf_player_shots(season=2026, league_id=1)
```

## sihf_player_penalties

Skater penalties: penalty minutes, 2-, 5- and 10-minute penalties, game misconducts and match penalties.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=playerFoul&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=playerFoul&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_player_penalties-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | String | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | Int64 | Games played in the table's scope. |
| `pim_total` | Int64 | Penalty minutes, total. |
| `pim2` | Int64 | Number of 2-minute minor penalties. |
| `pim5` | Int64 | Number of 5-minute major penalties. |
| `pim10` | Int64 | Number of 10-minute misconducts. |
| `game_misconducts` | Int64 | Number of game misconducts. |
| `match_penalties` | Int64 | Number of match penalties. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_player_penalties-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_player_penalties-example}

```python
sihf_player_penalties(season=2026, league_id=1)
```

## sihf_player_shootouts

Skater shootouts: attempts, goals, misses, percentage and shootout-winning goals.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=playerShootout&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=playerShootout&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_player_shootouts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | String | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | Int64 | Games played in the table's scope. |
| `pen_shots` | Int64 | Shootout/penalty shots taken. |
| `pen_shot_goals` | Int64 | Shootout/penalty goals. |
| `pen_shots_missed` | Int64 | Shootout/penalty shots missed. |
| `pen_shots_percent` | Float64 | Shootout/penalty conversion percentage. |
| `pen_shot_winning_goals` | Int64 | Deciding shootout goals. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_player_shootouts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_player_shootouts-example}

```python
sihf_player_shootouts(season=2026, league_id=1)
```

## sihf_player_faceoffs

Skater faceoffs: taken, won, lost, per game, and win percentage overall and by zone.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=playerFaceoff&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=playerFaceoff&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_player_faceoffs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | String | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | Int64 | Games played in the table's scope. |
| `faceoffs` | Int64 | Faceoffs taken, total. |
| `faceoffs_per_game` | Float64 | Faceoffs taken per game. |
| `faceoffs_percent` | Float64 | Faceoff win percentage; '-' with no faceoffs. |
| `faceoffs_won` | Int64 | Number of faceoffs won. |
| `faceoffs_lost` | Int64 | Number of faceoffs lost. |
| `faceoffs_won_per_game` | Float64 | Faceoffs won per game. |
| `faceoffs_lost_per_game` | Float64 | Faceoffs lost per game. |
| `faceoffs_offensive_percent` | Float64 | Faceoff win percentage, offensive zone. |
| `faceoffs_neutral_percent` | Float64 | Faceoff win percentage, neutral zone. |
| `faceoffs_defensive_percent` | Float64 | Faceoff win percentage, defensive zone. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_player_faceoffs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_player_faceoffs-example}

```python
sihf_player_faceoffs(season=2026, league_id=1)
```

## sihf_player_faceoffs_by_zone

Skater faceoffs by zone (offensive, neutral, defensive): taken, won, lost and win percentage.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=playerFaceoffZone&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=playerFaceoffZone&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_player_faceoffs_by_zone-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | String | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | Int64 | Games played in the table's scope. |
| `faceoffs_offensive` | Int64 | Faceoffs taken, offensive zone. |
| `faceoffs_offensive_percent` | Float64 | Faceoff win percentage, offensive zone. |
| `faceoffs_won_offensive` | Int64 | Faceoffs won, offensive zone. |
| `faceoffs_lost_offensive` | Int64 | Faceoffs lost, offensive zone. |
| `faceoffs_neutral` | Int64 | Faceoffs taken, neutral zone. |
| `faceoffs_neutral_percent` | Float64 | Faceoff win percentage, neutral zone. |
| `faceoffs_won_neutral` | Int64 | Faceoffs won, neutral zone. |
| `faceoffs_lost_neutral` | Int64 | Faceoffs lost, neutral zone. |
| `faceoffs_defensive` | Int64 | Faceoffs taken, defensive zone. |
| `faceoffs_defensive_percent` | Float64 | Faceoff win percentage, defensive zone. |
| `faceoffs_won_defensive` | Int64 | Faceoffs won, defensive zone. |
| `faceoffs_lost_defensive` | Int64 | Faceoffs lost, defensive zone. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_player_faceoffs_by_zone-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_player_faceoffs_by_zone-example}

```python
sihf_player_faceoffs_by_zone(season=2026, league_id=1)
```

## sihf_player_faceoffs_by_zone_per_game

Skater faceoffs by zone (offensive, neutral, defensive) per game: taken, won and lost.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=playerFaceoffZoneGame&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=playerFaceoffZoneGame&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_player_faceoffs_by_zone_per_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | String | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | Int64 | Games played in the table's scope. |
| `faceoffs_offensive_per_game` | Float64 | Faceoffs taken per game, offensive zone. |
| `faceoffs_won_offensive_per_game` | Float64 | Faceoffs won per game, offensive zone. |
| `faceoffs_lost_offensive_per_game` | Float64 | Faceoffs lost per game, offensive zone. |
| `faceoffs_neutral_per_game` | Float64 | Faceoffs taken per game, neutral zone. |
| `faceoffs_won_neutral_per_game` | Float64 | Faceoffs won per game, neutral zone. |
| `faceoffs_lost_neutral_per_game` | Float64 | Faceoffs lost per game, neutral zone. |
| `faceoffs_defensive_per_game` | Float64 | Faceoffs taken per game, defensive zone. |
| `faceoffs_won_defensive_per_game` | Float64 | Faceoffs won per game, defensive zone. |
| `faceoffs_lost_defensive_per_game` | Float64 | Faceoffs lost per game, defensive zone. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_player_faceoffs_by_zone_per_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_player_faceoffs_by_zone_per_game-example}

```python
sihf_player_faceoffs_by_zone_per_game(season=2026, league_id=1)
```

## sihf_player_time_on_ice

Skater ice time by situation (even strength, power play, penalty kill), totals and per game, plus shifts.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=playerTimeOnIce&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=playerTimeOnIce&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_player_time_on_ice-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | String | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | Int64 | Games played in the table's scope. |
| `time_on_ice_eq` | String | Even-strength time on ice, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_eq_per_game` | String | Even-strength time on ice per game, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_pp` | String | Power-play time on ice, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_pp_per_game` | String | Power-play time on ice per game, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_pk` | String | Short-handed time on ice, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_pk_per_game` | String | Short-handed time on ice per game, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice` | String | Time on ice, total, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_per_game` | String | Time on ice per game, mm:ss string (minutes may exceed 59), not seconds. |
| `shifts` | Int64 | Number of shifts on ice. |
| `time_on_ice_per_shift` | String | Average time on ice per shift, mm:ss string (minutes may exceed 59), not seconds; '-' with no shifts. |
| `shifts_per_game` | Float64 | Shifts per game. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_player_time_on_ice-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_player_time_on_ice-example}

```python
sihf_player_time_on_ice(season=2026, league_id=1)
```
