---
title: "SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Team"
sidebar_label: "Team"
sidebar_position: 4
description: "SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Team

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | integer | Games played in the table's scope. |
| `goals_for` | integer | Goals scored (goals for). |
| `goals_for_quotient` | double | Goals for per game. |
| `goals_against` | integer | Goals conceded (goals against). |
| `goals_against_quotient` | double | Goals against per game. |
| `even_goals_for` | integer | Even-strength goals for. |
| `even_goals_for_quotient` | double | Even-strength goals for per game. |
| `even_goals_against` | integer | Even-strength goals against. |
| `even_goals_against_quotient` | double | Even-strength goals against per game. |
| `pp_goals_for` | integer | Power-play goals for. |
| `pp_goals_for_quotient` | double | Power-play goals for per game. |
| `pp_goals_against` | integer | Goals against while on the power play. |
| `pp_goals_against_quotient` | double | Goals against while on the power play, per game. |
| `sh_goals` | integer | Short-handed goals for. |
| `sh_goals_quotient` | double | Short-handed goals for per game. |
| `pk_goals_against` | integer | Goals against while short-handed. |
| `pk_goals_against_quotient` | double | Goals against while short-handed, per game. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | integer | Games played in the table's scope. |
| `goals_for_fw_df` | integer | Goals by forwards and defencemen. |
| `goals_for_fw` | integer | Goals by forwards. |
| `goals_for_fw_percent` | double | Share of goals by forwards, percent. |
| `goals_for_df` | integer | Goals by defencemen. |
| `goals_for_df_percent` | double | Share of goals by defencemen, percent. |
| `goals_for_im` | integer | Goals by import (foreign-licence) players. |
| `goals_for_im_percent` | double | Share of goals by import players, percent. |
| `sow_goals_for` | integer | Deciding shootout goals scored. |
| `sow_goals_against` | integer | Deciding shootout goals conceded. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
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
| `soga_sog` | integer | Shots on goal against, total. |
| `soga_sog_per_game` | double | Shots on goal against per game. |
| `soga_sog_slot` | integer | Slot shots on goal against. |
| `soga_sog_slot_per_game` | double | Slot shots on goal against per game. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | integer | Games played in the table's scope. |
| `pp_opportunities` | integer | Power-play opportunities. |
| `pp_opportunities_quotient` | double | Power-play opportunities per game. |
| `pp_goals_for` | integer | Power-play goals for. |
| `pp_goals_for_quotient` | double | Power-play goals for per game. |
| `pp_percent` | double | Power-play percentage. |
| `power_play_time` | character | Time on the power play, mm:ss string (minutes may exceed 59), not seconds. |
| `power_play_time_quotient` | character | Time on the power play per game, mm:ss string (minutes may exceed 59), not seconds. |
| `pp_goals_against` | integer | Goals against while on the power play. |
| `pp_goals_against_quotient` | double | Goals against while on the power play, per game. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | integer | Games played in the table's scope. |
| `pk_situations` | integer | Short-handed situations. |
| `pk_situations_quotient` | double | Short-handed situations per game. |
| `pk_goals_against` | integer | Goals against while short-handed. |
| `pk_goals_against_quotient` | double | Goals against while short-handed, per game. |
| `pk_percent` | double | Penalty-kill percentage. |
| `penalty_kill_time` | character | Time short-handed, mm:ss string (minutes may exceed 59), not seconds. |
| `penalty_kill_time_quotient` | character | Time short-handed per game, mm:ss string (minutes may exceed 59), not seconds. |
| `sh_goals` | integer | Short-handed goals for. |
| `sh_goals_quotient` | double | Short-handed goals for per game. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | integer | Games played in the table's scope. |
| `pim2` | integer | Number of 2-minute minor penalties. |
| `pim5` | integer | Number of 5-minute major penalties. |
| `pim10` | integer | Number of 10-minute misconducts. |
| `game_misconducts` | integer | Number of game misconducts. |
| `match_penalties` | integer | Number of match penalties. |
| `pim_total` | integer | Penalty minutes, total. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `games_played` | integer | Games played in the table's scope. |
| `games_won_penalty` | integer | Games won in a shootout. |
| `games_lost_penalty` | integer | Shootout losses. |
| `pen_shots` | integer | Shootout/penalty shots taken. |
| `pen_shot_goals` | integer | Shootout/penalty goals. |
| `pen_shots_missed` | integer | Shootout/penalty shots missed. |
| `pen_shots_percent` | double | Shootout/penalty conversion percentage. |
| `pen_shots_against` | integer | Shootout/penalty shots faced. |
| `pen_shot_goals_against` | integer | Shootout/penalty goals allowed. |
| `pen_shot_saves` | integer | Shootout/penalty shots saved. |
| `pen_shot_saves_percent` | double | Shootout/penalty save percentage. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
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

_Last validated n/a._

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
| `season` | integer | Season by its END year (2026 = 2025-26), from the body's echo of the Season filter. |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...); null unless a phase filter is passed. |
| `rank` | integer | Rank in the table; null on a summary row, and on a goaltender below the qualification line. |
| `team_id` | integer | SIHF team id (Int64 join key); null on a summary row (the pseudo-team 100000). |
| `team_name` | character | Team display name as SIHF publishes it; "Durchschnitt" on the league-average row, "Total" on the attendance total row. |
| `team_acronym` | character | Team abbreviation as SIHF publishes it (e.g., "HCD"); null on a summary row. |
| `home_games` | integer | Number of home games. |
| `spectators` | integer | Total home attendance (SIHF prints a ’ thousands separator; dropped here). |
| `stadium_capacity_quotient` | double | Average arena capacity. |
| `spectators_quotient` | double | Average home attendance per game. |
| `stadium_capacity_percent` | double | Average arena utilisation, percent. |
| `row_type` | character | "data" for a regular row; "average" (league average) or "total" (attendance total) for a summary row SIHF mixes into the table. |

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

_Last validated n/a._
