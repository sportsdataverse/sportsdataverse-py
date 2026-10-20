---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Box scores: boxscoresummaryv2–boxscoreusagev3"
sidebar_label: "Box scores: boxscoresummaryv2–boxscoreusagev3"
sidebar_position: 2
description: "WNBA — WNBA Stats API (stats.wnba.com) — Box scores: boxscoresummaryv2–boxscoreusagev3 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Box scores: boxscoresummaryv2–boxscoreusagev3

## wnba_stats_boxscoresummaryv2

GET /stats/boxscoresummaryv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoresummaryv2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoresummaryv2?GameID=1022200034](https://stats.wnba.com/stats/boxscoresummaryv2?GameID=1022200034)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#wnba_stats_boxscoresummaryv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameSummary`, `OtherStats`, `Officials`, `InactivePlayers`, `GameInfo`, `LineScore`, `LastMeeting`, `SeasonSeries`, `AvailableVideo`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GameSummary**

| col_name | type | description |
|---|---|---|
| `game_date_est` | character | Game date est. |
| `game_sequence` | integer | Game sequence. |
| `game_id` | character | Unique game identifier. |
| `game_status_id` | integer | Numeric game status identifier. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `gamecode` | character | Gamecode. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `live_period` | integer | Live period. |
| `live_pc_time` | character | Time / clock value. |
| `natl_tv_broadcaster_abbreviation` | character | Natl tv broadcaster abbreviation. |
| `live_period_time_bcast` | character | Live period time bcast. |
| `wh_status` | integer | Wh status. |

**OtherStats**

| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `pts_paint` | integer | Points scored in the paint. |
| `pts_2_nd_chance` | integer |  |
| `pts_fb` | integer |  |
| `largest_lead` | integer | Largest lead during the game. |
| `lead_changes` | integer | Lead changes. |
| `times_tied` | integer | Times tied. |
| `team_turnovers` | integer | Team turnovers (turnovers credited to the team rather than a player). |
| `total_turnovers` | integer | Total turnovers (player + team). |
| `team_rebounds` | integer | Team rebounds (rebounds credited to the team rather than a player). |
| `pts_off_to` | integer |  |

**Officials**

| col_name | type | description |
|---|---|---|
| `official_id` | integer | Unique official / referee identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `jersey_num` | character | Jersey number worn by the player. |

**InactivePlayers**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `jersey_num` | character | Jersey number worn by the player. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |

**GameInfo**

| col_name | type | description |
|---|---|---|
| `game_date` | character | Game date (YYYY-MM-DD). |
| `attendance` | integer | Reported attendance. |
| `game_time` | character | Game start time. |

**LineScore**

| col_name | type | description |
|---|---|---|
| `game_date_est` | character | Game date est. |
| `game_sequence` | integer | Game sequence. |
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city_name` | character | Team city name. |
| `team_nickname` | character | Team nickname. |
| `team_wins_losses` | character | Team wins losses. |
| `pts_qtr1` | integer | Pts qtr1. |
| `pts_qtr2` | integer | Pts qtr2. |
| `pts_qtr3` | integer | Pts qtr3. |
| `pts_qtr4` | integer | Pts qtr4. |
| `pts_ot1` | integer | Pts ot1. |
| `pts_ot2` | integer | Scoring or score-margin metric for points ot2 in the requested NBA or WNBA Stats split. |
| `pts_ot3` | integer | Scoring or score-margin metric for points ot3 in the requested NBA or WNBA Stats split. |
| `pts_ot4` | integer | Scoring or score-margin metric for points ot4 in the requested NBA or WNBA Stats split. |
| `pts_ot5` | integer | Scoring or score-margin metric for points ot5 in the requested NBA or WNBA Stats split. |
| `pts_ot6` | integer | Scoring or score-margin metric for points ot6 in the requested NBA or WNBA Stats split. |
| `pts_ot7` | integer | Scoring or score-margin metric for points ot7 in the requested NBA or WNBA Stats split. |
| `pts_ot8` | integer | Scoring or score-margin metric for points ot8 in the requested NBA or WNBA Stats split. |
| `pts_ot9` | integer | Scoring or score-margin metric for points ot9 in the requested NBA or WNBA Stats split. |
| `pts_ot10` | integer | Scoring or score-margin metric for points ot10 in the requested NBA or WNBA Stats split. |
| `pts` | integer | Points scored. |

**LastMeeting**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `last_game_id` | character |  |
| `last_game_date_est` | character |  |
| `last_game_home_team_id` | integer |  |
| `last_game_home_team_city` | character |  |
| `last_game_home_team_name` | character |  |
| `last_game_home_team_abbreviation` | character |  |
| `last_game_home_team_points` | integer |  |
| `last_game_visitor_team_id` | integer |  |
| `last_game_visitor_team_city` | character |  |
| `last_game_visitor_team_name` | character |  |
| `last_game_visitor_team_city1` | character |  |
| `last_game_visitor_team_points` | integer |  |

**SeasonSeries**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `game_date_est` | character | Game date est. |
| `home_team_wins` | integer | Home team's team wins. |
| `home_team_losses` | integer | Home team's team losses. |
| `series_leader` | character |  |

**AvailableVideo**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `video_available_flag` | integer | Video available flag. |
| `pt_available` | integer | Pt available. |
| `pt_xyz_available` | integer | Pt xyz available. |
| `wh_status` | integer | Wh status. |
| `hustle_status` | integer | Hustle status. |
| `historical_status` | integer | Historical status. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_boxscoresummaryv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#wnba_stats_boxscoresummaryv2-example}

```python
wnba_stats_boxscoresummaryv2()
```

_Last validated n/a._

## wnba_stats_boxscoresummaryv3

GET /stats/boxscoresummaryv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoresummaryv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscoresummaryv3?GameID=1022200034](https://stats.wnba.com/stats/boxscoresummaryv3?GameID=1022200034)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#wnba_stats_boxscoresummaryv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`, `Officials`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `game_code` | character | ESPN game code (numeric identifier). |
| `game_status` | integer | Game status label. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `game_clock` | character | Game clock. |
| `game_time_utc` | character | Game start time in UTC (ISO 8601 timestamp). |
| `game_et` | character | Game et. |
| `away_team_id` | integer | Unique identifier for the away team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `duration` | character | Duration. |
| `attendance` | integer | Reported attendance. |
| `sellout` | integer | Sellout. |
| `series_game_number` | character | Series game number. |
| `game_label` | character |  |
| `game_sub_label` | character |  |
| `series_text` | character | Series text. |
| `if_necessary` | logical | If necessary. |
| `is_neutral` | logical |  |
| `video_available_flag` | integer | Video available flag. |
| `pt_available` | integer | Pt available. |
| `pt_xyz_available` | integer | Pt xyz available. |
| `wh_status` | integer | Wh status. |
| `hustle_status` | integer | Hustle status. |
| `historical_status` | integer | Historical status. |
| `game_subtype` | character | Game subtype. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `team_wins` | integer | Team's win total entering the game. |
| `team_losses` | integer | Team's loss total entering the game. |
| `score` | integer | Final score. |
| `in_bonus` | character | Whether the team is currently in the bonus (penalty) foul situation, as reported by the stats API. |
| `timeouts_remaining` | integer | Timeouts the team has remaining. |
| `seed` | integer | Team's playoff seed, populated for postseason games. |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `name` | character | Display name. |
| `name_i` | character | Initialed name (e.g. 'A. Wilson'). |
| `first_name` | character | Player's first name. |
| `family_name` | character | Player's family / last name. |
| `jersey_num` | character | Jersey number worn by the player. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique 10-character game identifier from the league's stats API. |
| `game_code` | character | ESPN game code (numeric identifier). |
| `game_status` | integer | Game status label. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `game_clock` | character | Game clock. |
| `game_time_utc` | character | Game start time in UTC (ISO 8601 timestamp). |
| `game_et` | character | Game et. |
| `away_team_id` | integer | Unique identifier for the away team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `duration` | character | Duration. |
| `attendance` | integer | Reported attendance. |
| `sellout` | integer | Sellout. |
| `series_game_number` | character | Series game number. |
| `game_label` | character |  |
| `game_sub_label` | character |  |
| `series_text` | character | Series text. |
| `if_necessary` | logical | If necessary. |
| `is_neutral` | logical |  |
| `video_available_flag` | integer | Video available flag. |
| `pt_available` | integer | Pt available. |
| `pt_xyz_available` | integer | Pt xyz available. |
| `wh_status` | integer | Wh status. |
| `hustle_status` | integer | Hustle status. |
| `historical_status` | integer | Historical status. |
| `game_subtype` | character | Game subtype. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_tricode` | character | Three-letter team abbreviation. |
| `team_slug` | character | URL-friendly slug for the team name. |
| `team_wins` | integer | Team's win total entering the game. |
| `team_losses` | integer | Team's loss total entering the game. |
| `score` | integer | Final score. |
| `in_bonus` | character | Whether the team is currently in the bonus (penalty) foul situation, as reported by the stats API. |
| `timeouts_remaining` | integer | Timeouts the team has remaining. |
| `seed` | integer | Team's playoff seed, populated for postseason games. |
| `dummy_key` | character | Placeholder key emitted by the stats API's box score summary payload; carries no data. |

**Officials**

| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `name` | character | Display name. |
| `name_i` | character | Initialed name (e.g. 'A. Wilson'). |
| `first_name` | character | Player's first name. |
| `family_name` | character | Player's family / last name. |
| `jersey_num` | character | Jersey number worn by the player. |
| `assignment` | character | Assignment. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_boxscoresummaryv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#wnba_stats_boxscoresummaryv3-example}

```python
wnba_stats_boxscoresummaryv3()
```

_Last validated n/a._

## wnba_stats_boxscoretraditionalv2

GET /stats/boxscoretraditionalv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoretraditionalv2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoretraditionalv2?EndPeriod=14&EndRange=0&GameID=1022200034&RangeType=0&StartPeriod=0&StartRange=0](https://stats.wnba.com/stats/boxscoretraditionalv2?EndPeriod=14&EndRange=0&GameID=1022200034&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#wnba_stats_boxscoretraditionalv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`, `TeamStarterBenchStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `start_position` | character | Position the player started the game at (F, C, or G); empty for reserves. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | character | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Total rebounds. |
| `ast` | integer | Assists. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `to` | integer | To. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `min` | character | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Total rebounds. |
| `ast` | integer | Assists. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `to` | integer | To. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |

**TeamStarterBenchStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `starters_bench` | character |  |
| `min` | character | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Total rebounds. |
| `ast` | integer | Assists. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `to` | integer | To. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_boxscoretraditionalv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#wnba_stats_boxscoretraditionalv2-example}

```python
wnba_stats_boxscoretraditionalv2()
```

_Last validated n/a._

## wnba_stats_boxscoretraditionalv3

GET /stats/boxscoretraditionalv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoretraditionalv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscoretraditionalv3?EndPeriod=14&EndRange=0&GameID=1022200034&RangeType=0&StartPeriod=0&StartRange=0](https://stats.wnba.com/stats/boxscoretraditionalv3?EndPeriod=14&EndRange=0&GameID=1022200034&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#wnba_stats_boxscoretraditionalv3-returns}

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

### Raises {#wnba_stats_boxscoretraditionalv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#wnba_stats_boxscoretraditionalv3-example}

```python
wnba_stats_boxscoretraditionalv3()
```

_Last validated n/a._

## wnba_stats_boxscoreusagev2

GET /stats/boxscoreusagev2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoreusagev2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoreusagev2?EndPeriod=14&EndRange=0&GameID=1022200034&RangeType=0&StartPeriod=0&StartRange=0](https://stats.wnba.com/stats/boxscoreusagev2?EndPeriod=14&EndRange=0&GameID=1022200034&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#wnba_stats_boxscoreusagev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`sqlPlayersUsage`, `sqlTeamsUsage`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**sqlPlayersUsage**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `start_position` | character | Position the player started the game at ('F', 'C', or 'G'); empty for bench players. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | character | Minutes played. |
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
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `min` | character | Minutes played. |
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

### Raises {#wnba_stats_boxscoreusagev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#wnba_stats_boxscoreusagev2-example}

```python
wnba_stats_boxscoreusagev2()
```

_Last validated n/a._

## wnba_stats_boxscoreusagev3

GET /stats/boxscoreusagev3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoreusagev3`

**Valid URL:** [https://stats.wnba.com/stats/boxscoreusagev3?EndPeriod=14&EndRange=0&GameID=1022200034&RangeType=0&StartPeriod=0&StartRange=0](https://stats.wnba.com/stats/boxscoreusagev3?EndPeriod=14&EndRange=0&GameID=1022200034&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#wnba_stats_boxscoreusagev3-returns}

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

### Raises {#wnba_stats_boxscoreusagev3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#wnba_stats_boxscoreusagev3-example}

```python
wnba_stats_boxscoreusagev3()
```

_Last validated n/a._
