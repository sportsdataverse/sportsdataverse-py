---
title: "NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoresummaryv3–boxscoreusagev3"
sidebar_label: "Box scores: boxscoresummaryv3–boxscoreusagev3"
sidebar_position: 2
description: "NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoresummaryv3–boxscoreusagev3 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoresummaryv3–boxscoreusagev3

## nbagl_stats_boxscoresummaryv3

GET /stats/boxscoresummaryv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoresummaryv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoresummaryv3?GameID=2022500137](https://stats.nba.com/stats/boxscoresummaryv3?GameID=2022500137)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoresummaryv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`, `Officials`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `game_code` | character |  |
| `game_status` | integer |  |
| `game_status_text` | character |  |
| `period` | integer |  |
| `game_clock` | character |  |
| `game_time_utc` | character |  |
| `game_et` | character |  |
| `away_team_id` | integer |  |
| `home_team_id` | integer |  |
| `duration` | character |  |
| `attendance` | integer |  |
| `sellout` | integer |  |
| `series_game_number` | character |  |
| `game_label` | character |  |
| `game_sub_label` | character |  |
| `series_text` | character |  |
| `if_necessary` | logical |  |
| `is_neutral` | logical |  |
| `video_available_flag` | integer |  |
| `pt_available` | integer |  |
| `pt_xyz_available` | integer |  |
| `wh_status` | integer |  |
| `hustle_status` | integer |  |
| `historical_status` | integer |  |
| `game_subtype` | character |  |
| `team_id` | integer |  |
| `team_name` | character |  |
| `team_city` | character |  |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `team_wins` | integer | Team's win total entering the game. |
| `team_losses` | integer | Team's loss total entering the game. |
| `score` | integer |  |
| `in_bonus` | character | Whether the team is currently in the bonus (penalty) foul situation, as reported by the stats API. |
| `timeouts_remaining` | integer | Timeouts the team has remaining. |
| `seed` | integer | Team's playoff seed, populated for postseason games. |
| `person_id` | integer |  |
| `name` | character |  |
| `name_i` | character |  |
| `first_name` | character |  |
| `family_name` | character |  |
| `jersey_num` | character |  |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `game_code` | character |  |
| `game_status` | integer |  |
| `game_status_text` | character |  |
| `period` | integer |  |
| `game_clock` | character |  |
| `game_time_utc` | character |  |
| `game_et` | character |  |
| `away_team_id` | integer |  |
| `home_team_id` | integer |  |
| `duration` | character |  |
| `attendance` | integer |  |
| `sellout` | integer |  |
| `series_game_number` | character |  |
| `game_label` | character |  |
| `game_sub_label` | character |  |
| `series_text` | character |  |
| `if_necessary` | logical |  |
| `is_neutral` | logical |  |
| `video_available_flag` | integer |  |
| `pt_available` | integer |  |
| `pt_xyz_available` | integer |  |
| `wh_status` | integer |  |
| `hustle_status` | integer |  |
| `historical_status` | integer |  |
| `game_subtype` | character |  |
| `team_id` | integer |  |
| `team_name` | character |  |
| `team_city` | character |  |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `team_wins` | integer | Team's win total entering the game. |
| `team_losses` | integer | Team's loss total entering the game. |
| `score` | integer |  |
| `in_bonus` | character | Whether the team is currently in the bonus (penalty) foul situation, as reported by the stats API. |
| `timeouts_remaining` | integer | Timeouts the team has remaining. |
| `seed` | integer | Team's playoff seed, populated for postseason games. |
| `dummy_key` | character | Placeholder key emitted by the stats API's box score summary payload; carries no data. |

**Officials**

| col_name | type | description |
|---|---|---|
| `person_id` | integer |  |
| `name` | character |  |
| `name_i` | character |  |
| `first_name` | character |  |
| `family_name` | character |  |
| `jersey_num` | character |  |
| `assignment` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoresummaryv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoresummaryv3-example}

```python
nbagl_stats_boxscoresummaryv3()
```

## nbagl_stats_boxscoretraditionalv2

GET /stats/boxscoretraditionalv2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoretraditionalv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoretraditionalv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoretraditionalv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoretraditionalv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`, `TeamStarterBenchStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `team_id` | integer |  |
| `team_abbreviation` | character |  |
| `team_city` | character |  |
| `player_id` | integer |  |
| `player_name` | character |  |
| `nickname` | character |  |
| `start_position` | character | Position the player started the game at (F, C, or G); empty for reserves. |
| `comment` | character |  |
| `min` | character |  |
| `fgm` | integer |  |
| `fga` | integer |  |
| `fg_pct` | numeric |  |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | integer |  |
| `fta` | integer |  |
| `ft_pct` | numeric |  |
| `oreb` | integer |  |
| `dreb` | integer |  |
| `reb` | integer |  |
| `ast` | integer |  |
| `stl` | integer |  |
| `blk` | integer |  |
| `to` | integer |  |
| `pf` | integer |  |
| `pts` | integer |  |
| `plus_minus` | integer |  |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `team_id` | integer |  |
| `team_name` | character |  |
| `team_abbreviation` | character |  |
| `team_city` | character |  |
| `min` | character |  |
| `fgm` | integer |  |
| `fga` | integer |  |
| `fg_pct` | numeric |  |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | integer |  |
| `fta` | integer |  |
| `ft_pct` | numeric |  |
| `oreb` | integer |  |
| `dreb` | integer |  |
| `reb` | integer |  |
| `ast` | integer |  |
| `stl` | integer |  |
| `blk` | integer |  |
| `to` | integer |  |
| `pf` | integer |  |
| `pts` | integer |  |
| `plus_minus` | integer |  |

**TeamStarterBenchStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `team_id` | integer |  |
| `team_name` | character |  |
| `team_abbreviation` | character |  |
| `team_city` | character |  |
| `starters_bench` | character |  |
| `min` | character |  |
| `fgm` | integer |  |
| `fga` | integer |  |
| `fg_pct` | numeric |  |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric |  |
| `ftm` | integer |  |
| `fta` | integer |  |
| `ft_pct` | numeric |  |
| `oreb` | integer |  |
| `dreb` | integer |  |
| `reb` | integer |  |
| `ast` | integer |  |
| `stl` | integer |  |
| `blk` | integer |  |
| `to` | integer |  |
| `pf` | integer |  |
| `pts` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoretraditionalv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoretraditionalv2-example}

```python
nbagl_stats_boxscoretraditionalv2()
```

## nbagl_stats_boxscoretraditionalv3

GET /stats/boxscoretraditionalv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoretraditionalv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoretraditionalv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoretraditionalv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoretraditionalv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | integer |  |
| `home_team_id` | integer |  |
| `team_id` | integer |  |
| `team_city` | character |  |
| `team_name` | character |  |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `person_id` | integer | Player identifier from the league's stats API. |
| `first_name` | character |  |
| `family_name` | character | Player's family (last) name. |
| `name_i` | character | Abbreviated player name (first initial and last name). |
| `player_slug` | character | URL-friendly slug for the player's name. |
| `position` | character |  |
| `comment` | character |  |
| `jersey_num` | character | Player's jersey number. |
| `minutes` | character |  |
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
| `assists` | integer |  |
| `steals` | integer |  |
| `blocks` | integer |  |
| `turnovers` | integer |  |
| `fouls_personal` | integer | Personal fouls recorded in the game. |
| `points` | integer |  |
| `plus_minus_points` | numeric | Team point differential while the player was on the floor (plus-minus). |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | integer |  |
| `home_team_id` | integer |  |
| `team_id` | integer |  |
| `team_city` | character |  |
| `team_name` | character |  |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `minutes` | character |  |
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
| `assists` | integer |  |
| `steals` | integer |  |
| `blocks` | integer |  |
| `turnovers` | integer |  |
| `fouls_personal` | integer | Personal fouls recorded in the game. |
| `points` | integer |  |
| `plus_minus_points` | numeric | Team point differential while the player was on the floor (plus-minus). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoretraditionalv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoretraditionalv3-example}

```python
nbagl_stats_boxscoretraditionalv3()
```

## nbagl_stats_boxscoreusagev2

GET /stats/boxscoreusagev2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreusagev2`

**Valid URL:** [https://stats.nba.com/stats/boxscoreusagev2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoreusagev2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoreusagev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`sqlPlayersUsage`, `sqlTeamsUsage`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**sqlPlayersUsage**

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `team_id` | integer |  |
| `team_abbreviation` | character |  |
| `team_city` | character |  |
| `player_id` | integer |  |
| `player_name` | character |  |
| `nickname` | character |  |
| `start_position` | character | Position the player started the game at ('F', 'C', or 'G'); empty for bench players. |
| `comment` | character |  |
| `min` | character |  |
| `usg_pct` | numeric | Percentage of team plays used while on the floor. |
| `pct_fgm` | numeric | Share of the team's field goals made accounted for while on the floor. |
| `pct_fga` | numeric | Share of the team's field goal attempts accounted for while on the floor. |
| `pct_fg3_m` | numeric | Share of the team's made three-pointers accounted for while on the floor. |
| `pct_fg3_a` | numeric | Share of the team's three-point attempts accounted for while on the floor. |
| `pct_ftm` | numeric | Share of the team's made free throws accounted for while on the floor. |
| `pct_fta` | numeric | Share of the team's free throw attempts accounted for while on the floor. |
| `pct_oreb` | numeric | Share of the team's offensive rebounds accounted for while on the floor. |
| `pct_dreb` | numeric | Share of the team's defensive rebounds accounted for while on the floor. |
| `pct_reb` | numeric | Share of the team's total rebounds accounted for while on the floor. |
| `pct_ast` | numeric | Share of the team's assists accounted for while on the floor. |
| `pct_tov` | numeric | Share of the team's turnovers accounted for while on the floor. |
| `pct_stl` | numeric | Share of the team's steals accounted for while on the floor. |
| `pct_blk` | numeric | Share of the team's blocks accounted for while on the floor. |
| `pct_blka` | numeric | Share of the team's blocked own attempts accounted for while on the floor. |
| `pct_pf` | numeric | Share of the team's personal fouls accounted for while on the floor. |
| `pct_pfd` | numeric | Share of the team's personal fouls drawn accounted for while on the floor. |
| `pct_pts` | numeric | Share of the team's points accounted for while on the floor. |

**sqlTeamsUsage**

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `team_id` | integer |  |
| `team_name` | character |  |
| `team_abbreviation` | character |  |
| `team_city` | character |  |
| `min` | character |  |
| `usg_pct` | numeric | Percentage of team plays used while on the floor. |
| `pct_fgm` | numeric | Share of the team's field goals made accounted for while on the floor. |
| `pct_fga` | numeric | Share of the team's field goal attempts accounted for while on the floor. |
| `pct_fg3_m` | numeric | Share of the team's made three-pointers accounted for while on the floor. |
| `pct_fg3_a` | numeric | Share of the team's three-point attempts accounted for while on the floor. |
| `pct_ftm` | numeric | Share of the team's made free throws accounted for while on the floor. |
| `pct_fta` | numeric | Share of the team's free throw attempts accounted for while on the floor. |
| `pct_oreb` | numeric | Share of the team's offensive rebounds accounted for while on the floor. |
| `pct_dreb` | numeric | Share of the team's defensive rebounds accounted for while on the floor. |
| `pct_reb` | numeric | Share of the team's total rebounds accounted for while on the floor. |
| `pct_ast` | numeric | Share of the team's assists accounted for while on the floor. |
| `pct_tov` | numeric | Share of the team's turnovers accounted for while on the floor. |
| `pct_stl` | numeric | Share of the team's steals accounted for while on the floor. |
| `pct_blk` | numeric | Share of the team's blocks accounted for while on the floor. |
| `pct_blka` | numeric | Share of the team's blocked own attempts accounted for while on the floor. |
| `pct_pf` | numeric | Share of the team's personal fouls accounted for while on the floor. |
| `pct_pfd` | numeric | Share of the team's personal fouls drawn accounted for while on the floor. |
| `pct_pts` | numeric | Share of the team's points accounted for while on the floor. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoreusagev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoreusagev2-example}

```python
nbagl_stats_boxscoreusagev2()
```

## nbagl_stats_boxscoreusagev3

GET /stats/boxscoreusagev3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreusagev3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreusagev3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoreusagev3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoreusagev3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | integer |  |
| `home_team_id` | integer |  |
| `team_id` | integer |  |
| `team_city` | character |  |
| `team_name` | character |  |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `person_id` | integer | Player identifier from the league's stats API. |
| `first_name` | character |  |
| `family_name` | character | Player's family (last) name. |
| `name_i` | character | Abbreviated player name (first initial and last name). |
| `player_slug` | character | URL-friendly slug for the player's name. |
| `position` | character |  |
| `comment` | character |  |
| `jersey_num` | character | Player's jersey number. |
| `minutes` | character |  |
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
| `away_team_id` | integer |  |
| `home_team_id` | integer |  |
| `team_id` | integer |  |
| `team_city` | character |  |
| `team_name` | character |  |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `minutes` | character |  |
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

### Raises {#nbagl_stats_boxscoreusagev3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoreusagev3-example}

```python
nbagl_stats_boxscoreusagev3()
```
