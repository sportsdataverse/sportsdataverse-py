---
title: "NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoretraditionalv2–boxscoreusagev3"
sidebar_label: "Box scores: boxscoretraditionalv2–boxscoreusagev3"
sidebar_position: 2
description: "NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoretraditionalv2–boxscoreusagev3 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoretraditionalv2–boxscoreusagev3

## nba_stats_boxscoretraditionalv2

Box score traditional v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoretraditionalv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoretraditionalv2?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoretraditionalv2?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoretraditionalv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`, `TeamStarterBenchStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `start_position` | String | Position the player started the game at (F, C, or G); empty for reserves. |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | String | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Rebounds per game. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `to` | Int64 | To. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `min` | String | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Rebounds per game. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `to` | Int64 | To. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |

**TeamStarterBenchStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `starters_bench` | String |  |
| `min` | String | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Rebounds per game. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `to` | Int64 | To. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_boxscoretraditionalv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscoretraditionalv2-example}

```python
nba_stats_boxscoretraditionalv2()
```

## nba_stats_boxscoretraditionalv3

Box score traditional v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoretraditionalv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoretraditionalv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoretraditionalv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoretraditionalv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `person_id` | Int64 | Player identifier from the league's stats API. |
| `first_name` | String | Player's first name. |
| `family_name` | String | Player's family (last) name. |
| `name_i` | String | Abbreviated player name (first initial and last name). |
| `player_slug` | String | URL-friendly slug for the player's name. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | String | Player's jersey number. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `field_goals_made` | Int64 | Field goals made recorded in the game. |
| `field_goals_attempted` | Int64 | Field goal attempts recorded in the game. |
| `field_goals_percentage` | Float64 | Field goal percentage for the game, as a decimal. |
| `three_pointers_made` | Int64 | Three-pointers made recorded in the game. |
| `three_pointers_attempted` | Int64 | Three-point attempts recorded in the game. |
| `three_pointers_percentage` | Float64 | Three-point percentage for the game, as a decimal. |
| `free_throws_made` | Int64 | Free throws made recorded in the game. |
| `free_throws_attempted` | Int64 | Free throw attempts recorded in the game. |
| `free_throws_percentage` | Float64 | Free throw percentage for the game, as a decimal. |
| `rebounds_offensive` | Int64 | Offensive rebounds recorded in the game. |
| `rebounds_defensive` | Int64 | Defensive rebounds recorded in the game. |
| `rebounds_total` | Int64 | Total rebounds recorded in the game. |
| `assists` | Int64 | Total assists. |
| `steals` | Int64 | Total steals. |
| `blocks` | Int64 | Total blocks. |
| `turnovers` | Int64 | Total turnovers. |
| `fouls_personal` | Int64 | Personal fouls recorded in the game. |
| `points` | Int64 | Points scored. |
| `plus_minus_points` | Float64 | Team point differential while the player was on the floor (plus-minus). |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `field_goals_made` | Int64 | Field goals made recorded in the game. |
| `field_goals_attempted` | Int64 | Field goal attempts recorded in the game. |
| `field_goals_percentage` | Float64 | Field goal percentage for the game, as a decimal. |
| `three_pointers_made` | Int64 | Three-pointers made recorded in the game. |
| `three_pointers_attempted` | Int64 | Three-point attempts recorded in the game. |
| `three_pointers_percentage` | Float64 | Three-point percentage for the game, as a decimal. |
| `free_throws_made` | Int64 | Free throws made recorded in the game. |
| `free_throws_attempted` | Int64 | Free throw attempts recorded in the game. |
| `free_throws_percentage` | Float64 | Free throw percentage for the game, as a decimal. |
| `rebounds_offensive` | Int64 | Offensive rebounds recorded in the game. |
| `rebounds_defensive` | Int64 | Defensive rebounds recorded in the game. |
| `rebounds_total` | Int64 | Total rebounds recorded in the game. |
| `assists` | Int64 | Total assists. |
| `steals` | Int64 | Total steals. |
| `blocks` | Int64 | Total blocks. |
| `turnovers` | Int64 | Total turnovers. |
| `fouls_personal` | Int64 | Personal fouls recorded in the game. |
| `points` | Int64 | Points scored. |
| `plus_minus_points` | Float64 | Team point differential while the player was on the floor (plus-minus). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_boxscoretraditionalv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscoretraditionalv3-example}

```python
nba_stats_boxscoretraditionalv3()
```

## nba_stats_boxscoreusagev3

Box score usage v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreusagev3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreusagev3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoreusagev3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoreusagev3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `person_id` | Int64 | Player identifier from the league's stats API. |
| `first_name` | String | Player's first name. |
| `family_name` | String | Player's family (last) name. |
| `name_i` | String | Abbreviated player name (first initial and last name). |
| `player_slug` | String | URL-friendly slug for the player's name. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | String | Player's jersey number. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `usage_percentage` | Float64 | Percentage of team plays used by the player while on the floor, as a decimal. |
| `percentage_field_goals_made` | Float64 | Share of the team's field goals made accounted for by the player while on the floor, as a decimal. |
| `percentage_field_goals_attempted` | Float64 | Share of the team's field goal attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_three_pointers_made` | Float64 | Share of the team's three-pointers made accounted for by the player while on the floor, as a decimal. |
| `percentage_three_pointers_attempted` | Float64 | Share of the team's three-point attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_free_throws_made` | Float64 | Share of the team's free throws made accounted for by the player while on the floor, as a decimal. |
| `percentage_free_throws_attempted` | Float64 | Share of the team's free throw attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_offensive` | Float64 | Share of the team's offensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_defensive` | Float64 | Share of the team's defensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_total` | Float64 | Share of the team's total rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_assists` | Float64 | Share of the team's assists accounted for by the player while on the floor, as a decimal. |
| `percentage_turnovers` | Float64 | Share of the team's turnovers accounted for by the player while on the floor, as a decimal. |
| `percentage_steals` | Float64 | Share of the team's steals accounted for by the player while on the floor, as a decimal. |
| `percentage_blocks` | Float64 | Share of the team's blocked shots accounted for by the player while on the floor, as a decimal. |
| `percentage_blocks_allowed` | Float64 | Share of the team's shot attempts blocked by opponents accounted for by the player while on the floor, as a decimal. |
| `percentage_personal_fouls` | Float64 | Share of the team's personal fouls accounted for by the player while on the floor, as a decimal. |
| `percentage_personal_fouls_drawn` | Float64 | Share of the team's personal fouls drawn accounted for by the player while on the floor, as a decimal. |
| `percentage_points` | Float64 | Share of the team's points accounted for by the player while on the floor, as a decimal. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `usage_percentage` | Float64 | Percentage of team plays used by the player while on the floor, as a decimal. |
| `percentage_field_goals_made` | Float64 | Share of the team's field goals made accounted for by the player while on the floor, as a decimal. |
| `percentage_field_goals_attempted` | Float64 | Share of the team's field goal attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_three_pointers_made` | Float64 | Share of the team's three-pointers made accounted for by the player while on the floor, as a decimal. |
| `percentage_three_pointers_attempted` | Float64 | Share of the team's three-point attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_free_throws_made` | Float64 | Share of the team's free throws made accounted for by the player while on the floor, as a decimal. |
| `percentage_free_throws_attempted` | Float64 | Share of the team's free throw attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_offensive` | Float64 | Share of the team's offensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_defensive` | Float64 | Share of the team's defensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_total` | Float64 | Share of the team's total rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_assists` | Float64 | Share of the team's assists accounted for by the player while on the floor, as a decimal. |
| `percentage_turnovers` | Float64 | Share of the team's turnovers accounted for by the player while on the floor, as a decimal. |
| `percentage_steals` | Float64 | Share of the team's steals accounted for by the player while on the floor, as a decimal. |
| `percentage_blocks` | Float64 | Share of the team's blocked shots accounted for by the player while on the floor, as a decimal. |
| `percentage_blocks_allowed` | Float64 | Share of the team's shot attempts blocked by opponents accounted for by the player while on the floor, as a decimal. |
| `percentage_personal_fouls` | Float64 | Share of the team's personal fouls accounted for by the player while on the floor, as a decimal. |
| `percentage_personal_fouls_drawn` | Float64 | Share of the team's personal fouls drawn accounted for by the player while on the floor, as a decimal. |
| `percentage_points` | Float64 | Share of the team's points accounted for by the player while on the floor, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_boxscoreusagev3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscoreusagev3-example}

```python
nba_stats_boxscoreusagev3()
```
