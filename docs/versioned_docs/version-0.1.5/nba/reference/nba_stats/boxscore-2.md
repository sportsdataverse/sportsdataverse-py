---
title: "NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoretraditionalv3–boxscoreusagev3"
sidebar_label: "Box scores: boxscoretraditionalv3–boxscoreusagev3"
sidebar_position: 2
description: "NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoretraditionalv3–boxscoreusagev3 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoretraditionalv3–boxscoreusagev3

## nba_stats_boxscoretraditionalv3

GET /stats/boxscoretraditionalv3

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
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | integer | Unique identifier for the away team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `person_id` | integer | Player identifier from the league's stats API. |
| `first_name` | character | Player's first name. |
| `family_name` | character | Player's family (last) name. |
| `name_i` | character | Abbreviated player name (first initial and last name). |
| `player_slug` | character | URL-friendly slug for the player's name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | character | Player's jersey number. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `field_goals_made` | integer | Field goals made recorded in the game. |
| `field_goals_attempted` | integer | Field goal attempts recorded in the game. |
| `field_goals_percentage` | numeric | Field goal percentage for the game, as a decimal. |
| `three_pointers_made` | integer | Three-pointers made recorded in the game. |
| `three_pointers_attempted` | integer | Three-point attempts recorded in the game. |
| `three_pointers_percentage` | numeric | Three-point percentage for the game, as a decimal. |
| `free_throws_made` | integer | Free throws made recorded in the game. |
| `free_throws_attempted` | integer | Free throw attempts recorded in the game. |
| `free_throws_percentage` | numeric | Free throw percentage for the game, as a decimal. |
| `rebounds_offensive` | integer | Offensive rebounds recorded in the game. |
| `rebounds_defensive` | integer | Defensive rebounds recorded in the game. |
| `rebounds_total` | integer | Total rebounds recorded in the game. |
| `assists` | integer | Total assists. |
| `steals` | integer | Total steals. |
| `blocks` | integer | Total blocks. |
| `turnovers` | integer | Total turnovers. |
| `fouls_personal` | integer | Personal fouls recorded in the game. |
| `points` | integer | Points scored. |
| `plus_minus_points` | numeric | Team point differential while the player was on the floor (plus-minus). |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | integer | Unique identifier for the away team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `field_goals_made` | integer | Field goals made recorded in the game. |
| `field_goals_attempted` | integer | Field goal attempts recorded in the game. |
| `field_goals_percentage` | numeric | Field goal percentage for the game, as a decimal. |
| `three_pointers_made` | integer | Three-pointers made recorded in the game. |
| `three_pointers_attempted` | integer | Three-point attempts recorded in the game. |
| `three_pointers_percentage` | numeric | Three-point percentage for the game, as a decimal. |
| `free_throws_made` | integer | Free throws made recorded in the game. |
| `free_throws_attempted` | integer | Free throw attempts recorded in the game. |
| `free_throws_percentage` | numeric | Free throw percentage for the game, as a decimal. |
| `rebounds_offensive` | integer | Offensive rebounds recorded in the game. |
| `rebounds_defensive` | integer | Defensive rebounds recorded in the game. |
| `rebounds_total` | integer | Total rebounds recorded in the game. |
| `assists` | integer | Total assists. |
| `steals` | integer | Total steals. |
| `blocks` | integer | Total blocks. |
| `turnovers` | integer | Total turnovers. |
| `fouls_personal` | integer | Personal fouls recorded in the game. |
| `points` | integer | Points scored. |
| `plus_minus_points` | numeric | Team point differential while the player was on the floor (plus-minus). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_boxscoretraditionalv3-example}

```python
nba_stats_boxscoretraditionalv3()
```

_Last validated n/a._

## nba_stats_boxscoreusagev3

GET /stats/boxscoreusagev3

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
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | integer | Unique identifier for the away team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `person_id` | integer | Player identifier from the league's stats API. |
| `first_name` | character | Player's first name. |
| `family_name` | character | Player's family (last) name. |
| `name_i` | character | Abbreviated player name (first initial and last name). |
| `player_slug` | character | URL-friendly slug for the player's name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | character | Player's jersey number. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `usage_percentage` | numeric | Percentage of team plays used by the player while on the floor, as a decimal. |
| `percentage_field_goals_made` | numeric | Share of the team's field goals made accounted for by the player while on the floor, as a decimal. |
| `percentage_field_goals_attempted` | numeric | Share of the team's field goal attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_three_pointers_made` | numeric | Share of the team's three-pointers made accounted for by the player while on the floor, as a decimal. |
| `percentage_three_pointers_attempted` | numeric | Share of the team's three-point attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_free_throws_made` | numeric | Share of the team's free throws made accounted for by the player while on the floor, as a decimal. |
| `percentage_free_throws_attempted` | numeric | Share of the team's free throw attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_offensive` | numeric | Share of the team's offensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_defensive` | numeric | Share of the team's defensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_total` | numeric | Share of the team's total rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_assists` | numeric | Share of the team's assists accounted for by the player while on the floor, as a decimal. |
| `percentage_turnovers` | numeric | Share of the team's turnovers accounted for by the player while on the floor, as a decimal. |
| `percentage_steals` | numeric | Share of the team's steals accounted for by the player while on the floor, as a decimal. |
| `percentage_blocks` | numeric | Share of the team's blocked shots accounted for by the player while on the floor, as a decimal. |
| `percentage_blocks_allowed` | numeric | Share of the team's shot attempts blocked by opponents accounted for by the player while on the floor, as a decimal. |
| `percentage_personal_fouls` | numeric | Share of the team's personal fouls accounted for by the player while on the floor, as a decimal. |
| `percentage_personal_fouls_drawn` | numeric | Share of the team's personal fouls drawn accounted for by the player while on the floor, as a decimal. |
| `percentage_points` | numeric | Share of the team's points accounted for by the player while on the floor, as a decimal. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | integer | Unique identifier for the away team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `usage_percentage` | numeric | Percentage of team plays used by the player while on the floor, as a decimal. |
| `percentage_field_goals_made` | numeric | Share of the team's field goals made accounted for by the player while on the floor, as a decimal. |
| `percentage_field_goals_attempted` | numeric | Share of the team's field goal attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_three_pointers_made` | numeric | Share of the team's three-pointers made accounted for by the player while on the floor, as a decimal. |
| `percentage_three_pointers_attempted` | numeric | Share of the team's three-point attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_free_throws_made` | numeric | Share of the team's free throws made accounted for by the player while on the floor, as a decimal. |
| `percentage_free_throws_attempted` | numeric | Share of the team's free throw attempts accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_offensive` | numeric | Share of the team's offensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_defensive` | numeric | Share of the team's defensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_rebounds_total` | numeric | Share of the team's total rebounds accounted for by the player while on the floor, as a decimal. |
| `percentage_assists` | numeric | Share of the team's assists accounted for by the player while on the floor, as a decimal. |
| `percentage_turnovers` | numeric | Share of the team's turnovers accounted for by the player while on the floor, as a decimal. |
| `percentage_steals` | numeric | Share of the team's steals accounted for by the player while on the floor, as a decimal. |
| `percentage_blocks` | numeric | Share of the team's blocked shots accounted for by the player while on the floor, as a decimal. |
| `percentage_blocks_allowed` | numeric | Share of the team's shot attempts blocked by opponents accounted for by the player while on the floor, as a decimal. |
| `percentage_personal_fouls` | numeric | Share of the team's personal fouls accounted for by the player while on the floor, as a decimal. |
| `percentage_personal_fouls_drawn` | numeric | Share of the team's personal fouls drawn accounted for by the player while on the floor, as a decimal. |
| `percentage_points` | numeric | Share of the team's points accounted for by the player while on the floor, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_boxscoreusagev3-example}

```python
nba_stats_boxscoreusagev3()
```

_Last validated n/a._
