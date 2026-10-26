---
title: "SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Team"
sidebar_label: "Team"
sidebar_position: 4
description: "SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Team

## sihf_team_goals

Team goals for and against per season: totals and per game, even strength, power play and short-handed.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamGoal&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamGoal&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_goals-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | Int64 | Games played in the table's scope. |
| `goals_for` | Int64 | Goals scored (goals for). |
| `goals_for_quotient` | Float64 | Goals for per game. |
| `goals_against` | Int64 | Goals conceded (goals against). |
| `goals_against_quotient` | Float64 | Goals against per game. |
| `even_goals_for` | Int64 | Even-strength goals for. |
| `even_goals_for_quotient` | Float64 | Even-strength goals for per game. |
| `even_goals_against` | Int64 | Even-strength goals against. |
| `even_goals_against_quotient` | Float64 | Even-strength goals against per game. |
| `pp_goals_for` | Int64 | Power-play goals for. |
| `pp_goals_for_quotient` | Float64 | Power-play goals for per game. |
| `pp_goals_against` | Int64 | Goals against while on the power play. |
| `pp_goals_against_quotient` | Float64 | Goals against while on the power play, per game. |
| `sh_goals` | Int64 | Short-handed goals for. |
| `sh_goals_quotient` | Float64 | Short-handed goals for per game. |
| `pk_goals_against` | Int64 | Goals against while short-handed. |
| `pk_goals_against_quotient` | Float64 | Goals against while short-handed, per game. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_team_goals-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_goals-example}

```python
sihf_team_goals(season=2026, league_id=1)
```

## sihf_team_goals_by_position

Team goals by scorer: forwards, defensemen and import players (totals and shares), plus shootout-winning goals for and against.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamGoalPosNation&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamGoalPosNation&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_goals_by_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | Int64 | Games played in the table's scope. |
| `goals_for_fw_df` | Int64 | Goals by forwards and defencemen. |
| `goals_for_fw` | Int64 | Goals by forwards. |
| `goals_for_fw_percent` | Float64 | Share of goals by forwards, percent. |
| `goals_for_df` | Int64 | Goals by defencemen. |
| `goals_for_df_percent` | Float64 | Share of goals by defencemen, percent. |
| `goals_for_im` | Int64 | Goals by import (foreign-licence) players. |
| `goals_for_im_percent` | Float64 | Share of goals by import players, percent. |
| `sow_goals_for` | Int64 | Deciding shootout goals scored. |
| `sow_goals_against` | Int64 | Deciding shootout goals conceded. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_team_goals_by_position-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_goals_by_position-example}

```python
sihf_team_goals_by_position(season=2026, league_id=1)
```

## sihf_team_shots

Team shots per season: on goal (total, slot, shooting percentage), missed, post, blocked, and shots against, with per-game rates.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamShotDetail&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamShotDetail&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_shots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
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
| `soga_sog` | Int64 | Shots on goal against, total. |
| `soga_sog_per_game` | Float64 | Shots on goal against per game. |
| `soga_sog_slot` | Int64 | Slot shots on goal against. |
| `soga_sog_slot_per_game` | Float64 | Slot shots on goal against per game. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_team_shots-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_shots-example}

```python
sihf_team_shots(season=2026, league_id=1)
```

## sihf_team_power_play

Team power play per season: opportunities, goals, conversion percentage, power-play time, and short-handed goals against.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamPp&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamPp&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_power_play-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | Int64 | Games played in the table's scope. |
| `pp_opportunities` | Int64 | Power-play opportunities. |
| `pp_opportunities_quotient` | Float64 | Power-play opportunities per game. |
| `pp_goals_for` | Int64 | Power-play goals for. |
| `pp_goals_for_quotient` | Float64 | Power-play goals for per game. |
| `pp_percent` | Float64 | Power-play percentage. |
| `power_play_time` | String | Time on the power play, mm:ss string (minutes may exceed 59), not seconds. |
| `power_play_time_quotient` | String | Time on the power play per game, mm:ss string (minutes may exceed 59), not seconds. |
| `pp_goals_against` | Int64 | Goals against while on the power play. |
| `pp_goals_against_quotient` | Float64 | Goals against while on the power play, per game. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_team_power_play-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_power_play-example}

```python
sihf_team_power_play(season=2026, league_id=1)
```

## sihf_team_penalty_kill

Team penalty kill per season: times short-handed, goals against, kill percentage, penalty-kill time, and short-handed goals for.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamPk&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamPk&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_penalty_kill-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | Int64 | Games played in the table's scope. |
| `pk_situations` | Int64 | Short-handed situations. |
| `pk_situations_quotient` | Float64 | Short-handed situations per game. |
| `pk_goals_against` | Int64 | Goals against while short-handed. |
| `pk_goals_against_quotient` | Float64 | Goals against while short-handed, per game. |
| `pk_percent` | Float64 | Penalty-kill percentage. |
| `penalty_kill_time` | String | Time short-handed, mm:ss string (minutes may exceed 59), not seconds. |
| `penalty_kill_time_quotient` | String | Time short-handed per game, mm:ss string (minutes may exceed 59), not seconds. |
| `sh_goals` | Int64 | Short-handed goals for. |
| `sh_goals_quotient` | Float64 | Short-handed goals for per game. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_team_penalty_kill-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_penalty_kill-example}

```python
sihf_team_penalty_kill(season=2026, league_id=1)
```

## sihf_team_penalties

Team penalties per season: 2-, 5- and 10-minute penalties, game misconducts, match penalties and penalty minutes.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamFoul&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamFoul&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_penalties-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | Int64 | Games played in the table's scope. |
| `pim2` | Int64 | Number of 2-minute minor penalties. |
| `pim5` | Int64 | Number of 5-minute major penalties. |
| `pim10` | Int64 | Number of 10-minute misconducts. |
| `game_misconducts` | Int64 | Number of game misconducts. |
| `match_penalties` | Int64 | Number of match penalties. |
| `pim_total` | Int64 | Penalty minutes, total. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_team_penalties-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_penalties-example}

```python
sihf_team_penalties(season=2026, league_id=1)
```

## sihf_team_shootouts

Team shootouts per season: shootout wins and losses, attempts, goals and percentage, and attempts, goals against and saves.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamShootout&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamShootout&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_shootouts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | Int64 | Games played in the table's scope. |
| `games_won_penalty` | Int64 | Games won in a shootout. |
| `games_lost_penalty` | Int64 | Shootout losses. |
| `pen_shots` | Int64 | Shootout/penalty shots taken. |
| `pen_shot_goals` | Int64 | Shootout/penalty goals. |
| `pen_shots_missed` | Int64 | Shootout/penalty shots missed. |
| `pen_shots_percent` | Float64 | Shootout/penalty conversion percentage. |
| `pen_shots_against` | Int64 | Shootout/penalty shots faced. |
| `pen_shot_goals_against` | Int64 | Shootout/penalty goals allowed. |
| `pen_shot_saves` | Int64 | Shootout/penalty shots saved. |
| `pen_shot_saves_percent` | Float64 | Shootout/penalty save percentage. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_team_shootouts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_shootouts-example}

```python
sihf_team_shootouts(season=2026, league_id=1)
```

## sihf_team_faceoffs

Team faceoffs per season: taken, won, lost, per game, and win percentage overall and by zone.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamFaceoff&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamFaceoff&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_faceoffs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
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

### Raises {#sihf_team_faceoffs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_faceoffs-example}

```python
sihf_team_faceoffs(season=2026, league_id=1)
```

## sihf_team_faceoffs_by_zone

Team faceoffs by zone (offensive, neutral, defensive) per season: taken, won, lost and win percentage.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamFaceoffZone&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamFaceoffZone&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_faceoffs_by_zone-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
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

### Raises {#sihf_team_faceoffs_by_zone-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_faceoffs_by_zone-example}

```python
sihf_team_faceoffs_by_zone(season=2026, league_id=1)
```

## sihf_team_faceoffs_by_zone_per_game

Team faceoffs by zone (offensive, neutral, defensive) per game: taken, won and lost.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamFaceoffZoneGame&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamFaceoffZoneGame&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_faceoffs_by_zone_per_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
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

### Raises {#sihf_team_faceoffs_by_zone_per_game-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_faceoffs_by_zone_per_game-example}

```python
sihf_team_faceoffs_by_zone_per_game(season=2026, league_id=1)
```

## sihf_team_attendance

Team home attendance per season: home games, total and average spectators, average arena capacity and utilisation.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/table`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/table?alias=teamSpectator&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026](https://data.sihf.ch/statistic/api/cms/table?alias=teamSpectator&searchQuery=1%2F%2F1&language=en&filterBy=Season&filterQuery=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Season` | `season` |  | `Y` |  | Required. Season by its END year (2026 = 2025-26). A season SIHF does not have raises NoDataError (the API itself silently answers another one). |
| `League` | `league_id` |  |  | `Y` | SIHF league id: 1 = National League, 2 = Swiss League, 100 = MyHockey League, 3 = 1. Liga, 43 = SWHL B, 113 = U21-Elit (the body's League filter lists them). An id SIHF does not have for the season raises NoDataError. |
| `Phase` | `phase_id` |  |  | `Y` | SIHF phase id (regular season, a playoff round, ...; the body's Phase filter lists them). None = no phase filter: the season summary over every phase. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_team_attendance-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | Int64 | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | Int64 | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | String | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | String | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `home_games` | Int64 | Number of home games. |
| `spectators` | Int64 | Total home attendance (SIHF prints a ’ thousands separator; dropped here). |
| `stadium_capacity_quotient` | Float64 | Average arena capacity. |
| `spectators_quotient` | Float64 | Average home attendance per game. |
| `stadium_capacity_percent` | Float64 | Average arena utilisation, percent. |
| `row_type` | String | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_team_attendance-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_team_attendance-example}

```python
sihf_team_attendance(season=2026, league_id=1)
```
