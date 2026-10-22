# SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Player

> SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Player — function reference in sdv-py, the SportsDataverse Python package.

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"). |
| `team_id` | integer | SIHF team id (Int64 join key). |
| `team_name` | character | Team display name as SIHF publishes it. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"). |
| `position` | character | Position, German whatever the language (e.g., "Stürmer"). |
| `games_played` | integer | Games the player played in the selection. |
| `goals` | integer | Goals scored by the player. |
| `assists` | integer | Assists (primary + secondary). |
| `points` | integer | Points (goals + assists). |
| `points_per_game` | double | Points per game. |
| `pim_total` | integer | Penalty minutes. |
| `plus_minus` | integer | Plus-minus: even-strength and shorthanded goals for, minus against, while on ice. |
| `row_type` | character | "data" for a regular row; "average" or "total" for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | character | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | integer | Games played in the table's scope. |
| `goals` | integer | Goals scored (total). |
| `assists` | integer | Assists credited. |
| `assists1` | integer | Primary assists. |
| `assists2` | integer | Secondary assists. |
| `game_winning_goals` | integer | Game-winning goals. |
| `eq_goals` | integer | Even-strength goals. |
| `eq_assists` | integer | Even-strength assists. |
| `pp_goals` | integer | Power-play goals. |
| `pp_assists` | integer | Power-play assists. |
| `pk_goals` | integer | Short-handed goals. |
| `pk_assists` | integer | Short-handed assists. |
| `overtime_goals` | integer | Overtime goals. |
| `hattricks` | integer | Hat tricks (games with three or more goals). |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | character | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | integer | Games played in the table's scope. |
| `sog_sog` | integer | Shots on goal (saved or scored). |
| `sog_sog_percent` | double | Shooting percentage (goals / shots on goal). |
| `sog_sog_quotient` | double | Shots on goal per game. |
| `sog_sog_slot` | integer | Slot shots on goal. |
| `sog_sog_slot_per_game` | double | Slot shots on goal per game. |
| `sog_missed` | integer | Missed shots, total. |
| `sog_missed_per_game` | double | Missed shots per game. |
| `sog_post` | integer | Post/crossbar shots, total. |
| `sog_post_per_game` | double | Post/crossbar shots per game. |
| `blockings` | integer | Number of shots blocked. |
| `blockings_per_game` | double | Blocked shots per game. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | character | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | integer | Games played in the table's scope. |
| `pim_total` | integer | Penalty minutes, total. |
| `pim2` | integer | Number of 2-minute minor penalties. |
| `pim5` | integer | Number of 5-minute major penalties. |
| `pim10` | integer | Number of 10-minute misconducts. |
| `game_misconducts` | integer | Number of game misconducts. |
| `match_penalties` | integer | Number of match penalties. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | character | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | integer | Games played in the table's scope. |
| `pen_shots` | integer | Shootout/penalty shots taken. |
| `pen_shot_goals` | integer | Shootout/penalty goals. |
| `pen_shots_missed` | integer | Shootout/penalty shots missed. |
| `pen_shots_percent` | double | Shootout/penalty conversion percentage. |
| `pen_shot_winning_goals` | integer | Deciding shootout goals. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | character | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | integer | Games played in the table's scope. |
| `faceoffs` | integer | Faceoffs taken, total. |
| `faceoffs_per_game` | double | Faceoffs taken per game. |
| `faceoffs_percent` | double | Faceoff win percentage; '-' with no faceoffs. |
| `faceoffs_won` | integer | Number of faceoffs won. |
| `faceoffs_lost` | integer | Number of faceoffs lost. |
| `faceoffs_won_per_game` | double | Faceoffs won per game. |
| `faceoffs_lost_per_game` | double | Faceoffs lost per game. |
| `faceoffs_offensive_percent` | double | Faceoff win percentage, offensive zone. |
| `faceoffs_neutral_percent` | double | Faceoff win percentage, neutral zone. |
| `faceoffs_defensive_percent` | double | Faceoff win percentage, defensive zone. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | character | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | integer | Games played in the table's scope. |
| `faceoffs_offensive` | integer | Faceoffs taken, offensive zone. |
| `faceoffs_offensive_percent` | double | Faceoff win percentage, offensive zone. |
| `faceoffs_won_offensive` | integer | Faceoffs won, offensive zone. |
| `faceoffs_lost_offensive` | integer | Faceoffs lost, offensive zone. |
| `faceoffs_neutral` | integer | Faceoffs taken, neutral zone. |
| `faceoffs_neutral_percent` | double | Faceoff win percentage, neutral zone. |
| `faceoffs_won_neutral` | integer | Faceoffs won, neutral zone. |
| `faceoffs_lost_neutral` | integer | Faceoffs lost, neutral zone. |
| `faceoffs_defensive` | integer | Faceoffs taken, defensive zone. |
| `faceoffs_defensive_percent` | double | Faceoff win percentage, defensive zone. |
| `faceoffs_won_defensive` | integer | Faceoffs won, defensive zone. |
| `faceoffs_lost_defensive` | integer | Faceoffs lost, defensive zone. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | character | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | integer | Games played in the table's scope. |
| `faceoffs_offensive_per_game` | double | Faceoffs taken per game, offensive zone. |
| `faceoffs_won_offensive_per_game` | double | Faceoffs won per game, offensive zone. |
| `faceoffs_lost_offensive_per_game` | double | Faceoffs lost per game, offensive zone. |
| `faceoffs_neutral_per_game` | double | Faceoffs taken per game, neutral zone. |
| `faceoffs_won_neutral_per_game` | double | Faceoffs won per game, neutral zone. |
| `faceoffs_lost_neutral_per_game` | double | Faceoffs lost per game, neutral zone. |
| `faceoffs_defensive_per_game` | double | Faceoffs taken per game, defensive zone. |
| `faceoffs_won_defensive_per_game` | double | Faceoffs won per game, defensive zone. |
| `faceoffs_lost_defensive_per_game` | double | Faceoffs lost per game, defensive zone. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"); season tables carry no player id. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `position` | character | Position, German whatever the language (e.g., "Stürmer"); null when SIHF prints "-". |
| `games_played` | integer | Games played in the table's scope. |
| `time_on_ice_eq` | character | Even-strength time on ice, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_eq_per_game` | character | Even-strength time on ice per game, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_pp` | character | Power-play time on ice, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_pp_per_game` | character | Power-play time on ice per game, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_pk` | character | Short-handed time on ice, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_pk_per_game` | character | Short-handed time on ice per game, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice` | character | Time on ice, total, mm:ss string (minutes may exceed 59), not seconds. |
| `time_on_ice_per_game` | character | Time on ice per game, mm:ss string (minutes may exceed 59), not seconds. |
| `shifts` | integer | Number of shifts on ice. |
| `time_on_ice_per_shift` | character | Average time on ice per shift, mm:ss string (minutes may exceed 59), not seconds; '-' with no shifts. |
| `shifts_per_game` | double | Shifts per game. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._
