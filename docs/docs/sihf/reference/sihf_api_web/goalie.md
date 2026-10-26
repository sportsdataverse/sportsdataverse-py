---
title: "SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Goalie"
sidebar_label: "Goalie"
sidebar_position: 2
description: "SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Goalie — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Goalie

## sihf_goalie_stats

Goaltender season totals: games, starts, goals against, GAA, shots, saves, save percentage, ice time.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=goalkeeper&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=goalkeeper&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase (player / goalie tables), every game (results), the regular-season table (standings). |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_goalie_stats-returns}

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
| `games_played` | Int64 | Games the goaltender dressed for. |
| `first_keeper` | Int64 | Games listed as the starting goaltender. |
| `goals_against` | Int64 | Goals allowed by the goaltender. |
| `goals_against_average` | Float64 | Goals against per 60 minutes. |
| `shots_against` | Int64 | Shots on goal faced. |
| `saves` | Int64 | Shots on goal saved by the goaltender. |
| `saves_percentage` | Float64 | Save percentage on a 0-100 scale (91.25 = .9125). |
| `seconds_played` | String | Ice time as "mm:ss" (a string, despite the upstream name). Minutes are not capped at 59: an overtime game reads "65:00", a season total "3615:02". |
| `penalty_in_minutes` | Int64 | Penalty minutes. |
| `goals` | Int64 | Goals scored by the goaltender (almost always 0). |
| `assists` | Int64 | Assists by the goaltender. |
| `row_type` | String | "data" for a regular row; "average" or "total" for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_goalie_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_goalie_stats-example}

```python
sihf_goalie_stats(season=2026, league_id=1)
```

## sihf_goalie_shots

Goaltender shots against: on goal (total, slot), missed, post, with per-game rates and the slot save percentage.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=goalkeeperShotDetail&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=goalkeeperShotDetail&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_goalie_shots-returns}

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
| `games_played` | Int64 | Games played in the table's scope. |
| `soga_sog` | Int64 | Shots on goal against, total. |
| `soga_sog_per_game` | Float64 | Shots on goal against per game. |
| `soga_sog_slot` | Int64 | Slot shots on goal against. |
| `soga_sog_slot_per_game` | Float64 | Slot shots on goal against per game. |
| `soga_missed` | Int64 | Missed shots against. |
| `soga_missed_per_game` | Float64 | Missed shots against per game. |
| `soga_post` | Int64 | Post/crossbar shots against. |
| `soga_post_per_game` | Float64 | Post/crossbar shots against per game. |
| `saves_slot_percentage` | Float64 | Save percentage on slot shots. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_goalie_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_goalie_shots-example}

```python
sihf_goalie_shots(season=2026, league_id=1)
```

## sihf_goalie_wins

Goaltender decisions: starts, wins and losses in regulation, overtime and shootout, and shutouts.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=goalkeeperWin&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=goalkeeperWin&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_goalie_wins-returns}

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
| `games_played` | Int64 | Games played in the table's scope. |
| `first_keeper` | Int64 | Games started as goalkeeper (GPI). |
| `wins` | Int64 | Wins, all decision types. |
| `wins_intime` | Int64 | Wins in regulation. |
| `wins_overtime` | Int64 | Games won in overtime. |
| `wins_penalty` | Int64 | Games won in a shootout. |
| `losses` | Int64 | Losses, all decision types. |
| `losses_intime` | Int64 | Losses in regulation. |
| `losses_overtime` | Int64 | Overtime losses. |
| `losses_penalty` | Int64 | Shootout losses. |
| `shutout` | Int64 | Shutouts (games with no goal against). |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_goalie_wins-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_goalie_wins-example}

```python
sihf_goalie_wins(season=2026, league_id=1)
```

## sihf_goalie_shootouts

Goaltender shootouts: attempts faced, goals against, saves and save percentage.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=goalkeeperShootout&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=goalkeeperShootout&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_goalie_shootouts-returns}

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
| `games_played` | Int64 | Games played in the table's scope. |
| `pen_shots_against` | Int64 | Shootout/penalty shots faced. |
| `pen_shot_goals_against` | Int64 | Shootout/penalty goals allowed. |
| `pen_shot_saves` | Int64 | Shootout/penalty shots saved. |
| `pen_shot_saves_percent` | Float64 | Shootout/penalty save percentage. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_goalie_shootouts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_goalie_shootouts-example}

```python
sihf_goalie_shootouts(season=2026, league_id=1)
```
