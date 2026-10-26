---
title: "SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SIHF (Swiss hockey) — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Game

## sihf_game_info

Header of one game: league, phase, teams, final and per-period score and shots, venue, attendance, officials.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/gameoverview`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en](https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `searchQuery` | `game_id` |  | `Y` |  | Required. SIHF game id (game_id in sihf_results), e.g. 20261105000325. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_game_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `season` | Int64 | Season by its END year (2026 = 2025-26). |
| `round` | Int64 | Round number within the season. |
| `league_id` | Int64 | SIHF league id of the game (1 = National League). |
| `league_name` | String | League name (e.g., "National League"). |
| `region_id` | Int64 | SIHF region id of the game (1 = national). |
| `region_name` | String | Region name (e.g., "National"). |
| `group_id` | Int64 | SIHF group id (0 = no group). |
| `group_name` | String | Name of the league group the game belongs to ('Keine' = none). |
| `tournament_id` | Int64 | SIHF tournament id (1 = championship). |
| `tournament_name` | String | Tournament name, German (e.g., 'Meisterschaft' = championship). |
| `qualification_id` | Int64 | SIHF id of the stage of the season (1 = regular season). |
| `qualification_name` | String | Stage of the season (e.g., "Regular Season", "Playoff Final"). |
| `phase_id` | Int64 | SIHF phase id (regular season, a playoff round, ...). |
| `phase_name` | String | Phase name (e.g., "NL Playoff Final"). |
| `start_date_time` | String | Scheduled start, ISO-8601 with UTC offset, as a string. |
| `end_date_time` | String | End of the game, ISO-8601 with UTC offset, as a string. |
| `update_date_time` | String | When SIHF last updated the record, ISO-8601 with UTC offset, as a string. |
| `status_id` | Int64 | SIHF game-status id (12 = finished). |
| `status_name` | String | Game-status name; German whatever the language (e.g., "Ende"). |
| `status_percent` | Int64 | Game progress, 0-100 (100 once the game is over). |
| `status_canceled` | Boolean | Whether the game was cancelled. |
| `home_team_id` | Int64 | SIHF team id of the home team. |
| `home_team_name` | String | Home team display name. |
| `home_team_acronym` | String | Home team abbreviation. |
| `away_team_id` | Int64 | SIHF team id of the away team. |
| `away_team_name` | String | Away team display name. |
| `away_team_acronym` | String | Away team abbreviation. |
| `home_score` | Int64 | Final home score. |
| `away_score` | Int64 | Final away score. |
| `home_score_by_period` | Int64 | Home goals per period, aligned with period_indicators. |
| `away_score_by_period` | Int64 | Away goals per period, aligned with period_indicators. |
| `home_shots_by_period` | Int64 | Home shots on goal per period, aligned with period_indicators. |
| `away_shots_by_period` | Int64 | Away shots on goal per period, aligned with period_indicators. |
| `period_indicators` | String | Period labels: "1", "2", "3", then "OT1", ... |
| `venue_name` | String | Name of the arena the game was played in. |
| `venue_city` | String | City of the arena with its canton code (e.g., 'Davos-Platz GR'). |
| `spectators` | Int64 | Attendance as SIHF reports it. |
| `referees` | String | Referee names, comma-joined. |
| `linesmen` | String | Linesman names, comma-joined. |
| `broadcasts` | String | Broadcasters, comma-joined (e.g., "MYSPORTS,TV24"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_game_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_game_info-example}

```python
sihf_game_info(game_id=20261105000325)
```

## sihf_game_player_stats

Skater box score of one game, both sides, with ice time and faceoffs.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/gameoverview`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en](https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `searchQuery` | `game_id` |  | `Y` |  | Required. SIHF game id (game_id in sihf_results), e.g. 20261105000325. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_game_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `team_side` | String | Side of the game the row belongs to: "home" or "away". |
| `team_id` | Int64 | SIHF team id of the row's side. |
| `team_name` | String | Team name of the row's side. |
| `player_number` | Int64 | Jersey number worn in this game. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"). |
| `player_position` | String | Position, German whatever the language (e.g., "Verteidiger"). |
| `goals` | Int64 | Goals scored by the player. |
| `assists` | Int64 | Assists (primary + secondary). |
| `assists1` | Int64 | Primary assists. |
| `assists2` | Int64 | Secondary assists. |
| `points` | Int64 | Points (goals + assists). |
| `pim_total` | Int64 | Penalty minutes. |
| `plus_minus` | Int64 | Plus-minus: even-strength and shorthanded goals for, minus against, while on ice. |
| `shots_on_goal` | Int64 | Shots on goal taken by the player. |
| `sog_missed` | Int64 | Shots that missed the net. |
| `sog_post` | Int64 | Shots off the post or crossbar. |
| `blockings` | Int64 | Opponent shots blocked by the player. |
| `faceoffs_won` | Int64 | Faceoffs won by the player. |
| `faceoffs_lost` | Int64 | Faceoffs lost by the player. |
| `faceoffs_percent` | Float64 | Faceoff win percentage, 0-100; null with no faceoffs. |
| `time_on_ice` | String | Ice time, "mm:ss". |
| `time_on_ice_pp` | String | Power-play ice time, "mm:ss". |
| `time_on_ice_pk` | String | Penalty-kill ice time, "mm:ss". |
| `row_type` | String | "data" per player, "total" for the side's total row. |
| `player_id` | Int64 | SIHF licence number of the player (Int64 join key), resolved from the game's roster. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_game_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_game_player_stats-example}

```python
sihf_game_player_stats(game_id=20261105000325)
```

## sihf_game_goalie_stats

Goaltender box score of one game, both sides.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/gameoverview`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en](https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `searchQuery` | `game_id` |  | `Y` |  | Required. SIHF game id (game_id in sihf_results), e.g. 20261105000325. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_game_goalie_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `team_side` | String | Side of the game the row belongs to: "home" or "away". |
| `team_id` | Int64 | SIHF team id of the row's side. |
| `team_name` | String | Team name of the row's side. |
| `player_number` | Int64 | Jersey number worn in this game. |
| `player` | String | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"). |
| `goals_against` | Int64 | Goals allowed by the goaltender. |
| `saves` | Int64 | Shots on goal saved by the goaltender. |
| `shots_against` | Int64 | Shots on goal faced. |
| `saves_percentage` | Float64 | Save percentage on a 0-100 scale (91.25 = .9125). |
| `seconds_played` | String | Ice time as "mm:ss" (a string, despite the upstream name). Minutes are not capped at 59: an overtime game reads "65:00", a season total "3615:02". |
| `goals_against_average` | Float64 | Goals against per 60 minutes in this game. |
| `penalty_in_minutes` | Int64 | Penalty minutes. |
| `row_type` | String | "data" for every row: a game goalie box has no summary rows. |
| `player_id` | Int64 | SIHF licence number of the player (Int64 join key), resolved from the game's roster. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_game_goalie_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_game_goalie_stats-example}

```python
sihf_game_goalie_stats(game_id=20261105000325)
```

## sihf_game_team_stats

Team stats of one game: shots by period, faceoffs by zone, penalty minutes, power play and penalty kill.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/gameoverview`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en](https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `searchQuery` | `game_id` |  | `Y` |  | Required. SIHF game id (game_id in sihf_results), e.g. 20261105000325. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_game_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `statistic` | String | Statistic label as SIHF prints it (e.g., "SOG Total", "FO% 1P", "PPT"). |
| `home_value` | String | Home team's value, as a string (counts, percentages and mm:ss times share the column). |
| `away_value` | String | Away team's value, as a string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_game_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_game_team_stats-example}

```python
sihf_game_team_stats(game_id=20261105000325)
```

## sihf_game_lineups

Lineups of one game by unit and slot, plus the absentees.

**Endpoint URL:** `GET https://data.sihf.ch/statistic/api/cms/gameoverview`

**Valid URL:** [https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en](https://data.sihf.ch/statistic/api/cms/gameoverview?alias=gameDetail&searchQuery=20261105000325&language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `searchQuery` | `game_id` |  | `Y` |  | Required. SIHF game id (game_id in sihf_results), e.g. 20261105000325. |
| `language` | `language` |  |  | `Y` | Response language. Only part of the body is translated: status, position and penalty names stay German. |

### Returns {#sihf_game_lineups-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `team_side` | String | Side of the game the row belongs to: "home" or "away". |
| `team_id` | Int64 | SIHF team id (Int64 join key). |
| `unit` | String | Lineup unit: "goalkeeper", "defense_left" / "_right", "forward_left" / "_center" / "_right", "other"; null for an absentee. |
| `slot` | Int64 | 1-based position in SIHF's listing of the unit. |
| `player_id` | Int64 | SIHF licence number of the player (Int64 join key), resolved from the game's roster. |
| `player_name` | String | Player name from the game's roster, last name first. |
| `player_jersey` | Int64 | Jersey number from the game's roster. |
| `is_captain` | Boolean | Whether the lineup tags the player as captain. |
| `is_first_goalkeeper` | Boolean | Whether the lineup tags the player as the starting goaltender. |
| `absence_code` | String | Absence code for an absentee ("SU", "IN", "AB"). |
| `absence_reason` | String | Absence reason, German (e.g., "Überzählig", "Verletzt", "Abwesend"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#sihf_game_lineups-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. SIHF answered 404 (unknown game), or it ignored a requested season / league / phase and served another one. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#sihf_game_lineups-example}

```python
sihf_game_lineups(game_id=20261105000325)
```
