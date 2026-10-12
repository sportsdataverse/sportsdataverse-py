---
title: "SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# SIHF — SIHF statistics API (data.sihf.ch; National League, Swiss League and every SIHF league) — Game

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
| `game_id` | integer | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `season` | integer | Season by its END year (2026 = 2025-26). |
| `round` | integer | Round number within the season. |
| `league_id` | integer | SIHF league id of the game (1 = National League). |
| `league_name` | character | League name (e.g., "National League"). |
| `region_id` | integer | SIHF region id of the game (1 = national). |
| `region_name` | character | Region name (e.g., "National"). |
| `group_id` | integer | SIHF group id (0 = no group). |
| `group_name` | character | Name of the league group the game belongs to ('Keine' = none). |
| `tournament_id` | integer | SIHF tournament id (1 = championship). |
| `tournament_name` | character | Tournament name, German (e.g., 'Meisterschaft' = championship). |
| `qualification_id` | integer | SIHF id of the stage of the season (1 = regular season). |
| `qualification_name` | character | Stage of the season (e.g., "Regular Season", "Playoff Final"). |
| `phase_id` | integer | SIHF phase id (regular season, a playoff round, ...). |
| `phase_name` | character | Phase name (e.g., "NL Playoff Final"). |
| `start_date_time` | character | Scheduled start, ISO-8601 with UTC offset, as a string. |
| `end_date_time` | character | End of the game, ISO-8601 with UTC offset, as a string. |
| `update_date_time` | character | When SIHF last updated the record, ISO-8601 with UTC offset, as a string. |
| `status_id` | integer | SIHF game-status id (12 = finished). |
| `status_name` | character | Game-status name; German whatever the language (e.g., "Ende"). |
| `status_percent` | integer | Game progress, 0-100 (100 once the game is over). |
| `status_canceled` | logical | Whether the game was cancelled. |
| `home_team_id` | integer | SIHF team id of the home team. |
| `home_team_name` | character | Home team display name. |
| `home_team_acronym` | character | Home team abbreviation. |
| `away_team_id` | integer | SIHF team id of the away team. |
| `away_team_name` | character | Away team display name. |
| `away_team_acronym` | character | Away team abbreviation. |
| `home_score` | integer | Final home score. |
| `away_score` | integer | Final away score. |
| `home_score_by_period` | integer | Home goals per period, aligned with period_indicators. |
| `away_score_by_period` | integer | Away goals per period, aligned with period_indicators. |
| `home_shots_by_period` | integer | Home shots on goal per period, aligned with period_indicators. |
| `away_shots_by_period` | integer | Away shots on goal per period, aligned with period_indicators. |
| `period_indicators` | character | Period labels: "1", "2", "3", then "OT1", ... |
| `venue_name` | character | Name of the arena the game was played in. |
| `venue_city` | character | City of the arena with its canton code (e.g., 'Davos-Platz GR'). |
| `spectators` | integer | Attendance as SIHF reports it. |
| `referees` | character | Referee names, comma-joined. |
| `linesmen` | character | Linesman names, comma-joined. |
| `broadcasts` | character | Broadcasters, comma-joined (e.g., "MYSPORTS,TV24"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sihf_game_info-example}

```python
sihf_game_info(game_id=20261105000325)
```

_Last validated n/a._

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
| `game_id` | integer | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `team_side` | character | Side of the game the row belongs to: "home" or "away". |
| `team_id` | integer | SIHF team id of the row's side. |
| `team_name` | character | Team name of the row's side. |
| `player_number` | integer | Jersey number worn in this game. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"). |
| `player_position` | character | Position, German whatever the language (e.g., "Verteidiger"). |
| `goals` | integer | Goals scored by the player. |
| `assists` | integer | Assists (primary + secondary). |
| `assists1` | integer | Primary assists. |
| `assists2` | integer | Secondary assists. |
| `points` | integer | Points (goals + assists). |
| `pim_total` | integer | Penalty minutes. |
| `plus_minus` | integer | Plus-minus: even-strength and shorthanded goals for, minus against, while on ice. |
| `shots_on_goal` | integer | Shots on goal taken by the player. |
| `sog_missed` | integer | Shots that missed the net. |
| `sog_post` | integer | Shots off the post or crossbar. |
| `blockings` | integer | Opponent shots blocked by the player. |
| `faceoffs_won` | integer | Faceoffs won by the player. |
| `faceoffs_lost` | integer | Faceoffs lost by the player. |
| `faceoffs_percent` | double | Faceoff win percentage, 0-100; null with no faceoffs. |
| `time_on_ice` | character | Ice time, "mm:ss". |
| `time_on_ice_pp` | character | Power-play ice time, "mm:ss". |
| `time_on_ice_pk` | character | Penalty-kill ice time, "mm:ss". |
| `row_type` | character | "data" per player, "total" for the side's total row. |
| `player_id` | integer | SIHF licence number of the player (Int64 join key), resolved from the game's roster. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sihf_game_player_stats-example}

```python
sihf_game_player_stats(game_id=20261105000325)
```

_Last validated n/a._

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
| `game_id` | integer | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `team_side` | character | Side of the game the row belongs to: "home" or "away". |
| `team_id` | integer | SIHF team id of the row's side. |
| `team_name` | character | Team name of the row's side. |
| `player_number` | integer | Jersey number worn in this game. |
| `player` | character | Player name as SIHF prints it, last name first (e.g., "Dahlbeck Klas"). |
| `goals_against` | integer | Goals allowed by the goaltender. |
| `saves` | integer | Shots on goal saved by the goaltender. |
| `shots_against` | integer | Shots on goal faced. |
| `saves_percentage` | double | Save percentage on a 0-100 scale (91.25 = .9125). |
| `seconds_played` | character | Ice time as "mm:ss" (a string, despite the upstream name). Minutes are not capped at 59: an overtime game reads "65:00", a season total "3615:02". |
| `goals_against_average` | double | Goals against per 60 minutes in this game. |
| `penalty_in_minutes` | integer | Penalty minutes. |
| `row_type` | character | "data" for every row: a game goalie box has no summary rows. |
| `player_id` | integer | SIHF licence number of the player (Int64 join key), resolved from the game's roster. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sihf_game_goalie_stats-example}

```python
sihf_game_goalie_stats(game_id=20261105000325)
```

_Last validated n/a._

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
| `game_id` | integer | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `statistic` | character | Statistic label as SIHF prints it (e.g., "SOG Total", "FO% 1P", "PPT"). |
| `home_value` | character | Home team's value, as a string (counts, percentages and mm:ss times share the column). |
| `away_value` | character | Away team's value, as a string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sihf_game_team_stats-example}

```python
sihf_game_team_stats(game_id=20261105000325)
```

_Last validated n/a._

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
| `game_id` | integer | SIHF game id (Int64 join key; the details link of sihf_results, the searchQuery of the game wrappers). |
| `team_side` | character | Side of the game the row belongs to: "home" or "away". |
| `team_id` | integer | SIHF team id (Int64 join key). |
| `unit` | character | Lineup unit: "goalkeeper", "defense_left" / "_right", "forward_left" / "_center" / "_right", "other"; null for an absentee. |
| `slot` | integer | 1-based position in SIHF's listing of the unit. |
| `player_id` | integer | SIHF licence number of the player (Int64 join key), resolved from the game's roster. |
| `player_name` | character | Player name from the game's roster, last name first. |
| `player_jersey` | integer | Jersey number from the game's roster. |
| `is_captain` | logical | Whether the lineup tags the player as captain. |
| `is_first_goalkeeper` | logical | Whether the lineup tags the player as the starting goaltender. |
| `absence_code` | character | Absence code for an absentee ("SU", "IN", "AB"). |
| `absence_reason` | character | Absence reason, German (e.g., "Überzählig", "Verletzt", "Abwesend"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#sihf_game_lineups-example}

```python
sihf_game_lineups(game_id=20261105000325)
```

_Last validated n/a._
