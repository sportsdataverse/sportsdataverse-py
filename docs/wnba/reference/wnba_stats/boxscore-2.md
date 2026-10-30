# WNBA — WNBA Stats API (stats.wnba.com) — Box scores: boxscoresummaryv2–boxscoreusagev3

> WNBA — WNBA Stats API (stats.wnba.com) — Box scores: boxscoresummaryv2–boxscoreusagev3 — function reference in sdv-py, the SportsDataverse Python package.

## wnba_stats_boxscoresummaryv2

Box score summary v2.

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
| `game_date_est` | String | Game date est. |
| `game_sequence` | Int64 | Game sequence. |
| `game_id` | String | Unique game identifier. |
| `game_status_id` | Int64 | Numeric game status identifier. |
| `game_status_text` | String | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `gamecode` | String | Gamecode. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `visitor_team_id` | Int64 | Unique identifier for visitor team. |
| `season` | String | Season identifier (4-digit year or 'YYYY-YY' string). |
| `live_period` | Int64 | Live period. |
| `live_pc_time` | String | Time / clock value. |
| `natl_tv_broadcaster_abbreviation` | String | Natl tv broadcaster abbreviation. |
| `live_period_time_bcast` | String | Live period time bcast. |
| `wh_status` | Int64 | Wh status. |

**OtherStats**

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `pts_paint` | Int64 | Points scored in the paint. |
| `pts_2_nd_chance` | Int64 |  |
| `pts_fb` | Int64 |  |
| `largest_lead` | Int64 | Largest lead during the game. |
| `lead_changes` | Int64 | Lead changes. |
| `times_tied` | Int64 | Times tied. |
| `team_turnovers` | Int64 | Team turnovers (turnovers credited to the team rather than a player). |
| `total_turnovers` | Int64 | Total turnovers (player + team). |
| `team_rebounds` | Int64 | Team rebounds (rebounds credited to the team rather than a player). |
| `pts_off_to` | Int64 |  |

**Officials**

| col_name | type | description |
|---|---|---|
| `official_id` | Int64 | Unique official / referee identifier. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_num` | String | Jersey number worn by the player. |

**InactivePlayers**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_num` | String | Jersey number worn by the player. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |

**GameInfo**

| col_name | type | description |
|---|---|---|
| `game_date` | String | Game date (YYYY-MM-DD). |
| `attendance` | Int64 | Reported attendance. |
| `game_time` | String | Game start time. |

**LineScore**

| col_name | type | description |
|---|---|---|
| `game_date_est` | String | Game date est. |
| `game_sequence` | Int64 | Game sequence. |
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city_name` | String | Team city name. |
| `team_nickname` | String | Team nickname. |
| `team_wins_losses` | String | Team wins losses. |
| `pts_qtr1` | Int64 | Pts qtr1. |
| `pts_qtr2` | Int64 | Pts qtr2. |
| `pts_qtr3` | Int64 | Pts qtr3. |
| `pts_qtr4` | Int64 | Pts qtr4. |
| `pts_ot1` | Int64 | Pts ot1. |
| `pts_ot2` | Int64 | Scoring or score-margin metric for points ot2 in the requested NBA or WNBA Stats split. |
| `pts_ot3` | Int64 | Scoring or score-margin metric for points ot3 in the requested NBA or WNBA Stats split. |
| `pts_ot4` | Int64 | Scoring or score-margin metric for points ot4 in the requested NBA or WNBA Stats split. |
| `pts_ot5` | Int64 | Scoring or score-margin metric for points ot5 in the requested NBA or WNBA Stats split. |
| `pts_ot6` | Int64 | Scoring or score-margin metric for points ot6 in the requested NBA or WNBA Stats split. |
| `pts_ot7` | Int64 | Scoring or score-margin metric for points ot7 in the requested NBA or WNBA Stats split. |
| `pts_ot8` | Int64 | Scoring or score-margin metric for points ot8 in the requested NBA or WNBA Stats split. |
| `pts_ot9` | Int64 | Scoring or score-margin metric for points ot9 in the requested NBA or WNBA Stats split. |
| `pts_ot10` | Int64 | Scoring or score-margin metric for points ot10 in the requested NBA or WNBA Stats split. |
| `pts` | Int64 | Points scored. |

**LastMeeting**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `last_game_id` | String |  |
| `last_game_date_est` | String |  |
| `last_game_home_team_id` | Int64 |  |
| `last_game_home_team_city` | String |  |
| `last_game_home_team_name` | String |  |
| `last_game_home_team_abbreviation` | String |  |
| `last_game_home_team_points` | Int64 |  |
| `last_game_visitor_team_id` | Int64 |  |
| `last_game_visitor_team_city` | String |  |
| `last_game_visitor_team_name` | String |  |
| `last_game_visitor_team_city1` | String |  |
| `last_game_visitor_team_points` | Int64 |  |

**SeasonSeries**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `visitor_team_id` | Int64 | Unique identifier for visitor team. |
| `game_date_est` | String | Game date est. |
| `home_team_wins` | Int64 | Home team's team wins. |
| `home_team_losses` | Int64 | Home team's team losses. |
| `series_leader` | String |  |

**AvailableVideo**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `video_available_flag` | Int64 | Video available flag. |
| `pt_available` | Int64 | Pt available. |
| `pt_xyz_available` | Int64 | Pt xyz available. |
| `wh_status` | Int64 | Wh status. |
| `hustle_status` | Int64 | Hustle status. |
| `historical_status` | Int64 | Historical status. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_boxscoresummaryv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_boxscoresummaryv2-example}

```python
wnba_stats_boxscoresummaryv2()
```

## wnba_stats_boxscoresummaryv3

Box score summary v3.

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
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `game_code` | String | ESPN game code (numeric identifier). |
| `game_status` | Int64 | Game status label. |
| `game_status_text` | String | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `game_clock` | String | Game clock. |
| `game_time_utc` | String | Game start time in UTC (ISO 8601 timestamp). |
| `game_et` | String | Game et. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `duration` | String | Duration. |
| `attendance` | Int64 | Reported attendance. |
| `sellout` | Int64 | Sellout. |
| `series_game_number` | String | Series game number. |
| `game_label` | String |  |
| `game_sub_label` | String |  |
| `series_text` | String | Series text. |
| `if_necessary` | Boolean | If necessary. |
| `is_neutral` | Boolean |  |
| `video_available_flag` | Int64 | Video available flag. |
| `pt_available` | Int64 | Pt available. |
| `pt_xyz_available` | Int64 | Pt xyz available. |
| `wh_status` | Int64 | Wh status. |
| `hustle_status` | Int64 | Hustle status. |
| `historical_status` | Int64 | Historical status. |
| `game_subtype` | String | Game subtype. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `team_wins` | Int64 | Team's win total entering the game. |
| `team_losses` | Int64 | Team's loss total entering the game. |
| `score` | Int64 | Final score. |
| `in_bonus` | String | Whether the team is currently in the bonus (penalty) foul situation, as reported by the stats API. |
| `timeouts_remaining` | Int64 | Timeouts the team has remaining. |
| `seed` | Int64 | Team's playoff seed, populated for postseason games. |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `name` | String | Display name. |
| `name_i` | String | Initialed name (e.g. 'A. Wilson'). |
| `first_name` | String | Player's first name. |
| `family_name` | String | Player's family / last name. |
| `jersey_num` | String | Jersey number worn by the player. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `game_code` | String | ESPN game code (numeric identifier). |
| `game_status` | Int64 | Game status label. |
| `game_status_text` | String | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `game_clock` | String | Game clock. |
| `game_time_utc` | String | Game start time in UTC (ISO 8601 timestamp). |
| `game_et` | String | Game et. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `duration` | String | Duration. |
| `attendance` | Int64 | Reported attendance. |
| `sellout` | Int64 | Sellout. |
| `series_game_number` | String | Series game number. |
| `game_label` | String |  |
| `game_sub_label` | String |  |
| `series_text` | String | Series text. |
| `if_necessary` | Boolean | If necessary. |
| `is_neutral` | Boolean |  |
| `video_available_flag` | Int64 | Video available flag. |
| `pt_available` | Int64 | Pt available. |
| `pt_xyz_available` | Int64 | Pt xyz available. |
| `wh_status` | Int64 | Wh status. |
| `hustle_status` | Int64 | Hustle status. |
| `historical_status` | Int64 | Historical status. |
| `game_subtype` | String | Game subtype. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `team_wins` | Int64 | Team's win total entering the game. |
| `team_losses` | Int64 | Team's loss total entering the game. |
| `score` | Int64 | Final score. |
| `in_bonus` | String | Whether the team is currently in the bonus (penalty) foul situation, as reported by the stats API. |
| `timeouts_remaining` | Int64 | Timeouts the team has remaining. |
| `seed` | Int64 | Team's playoff seed, populated for postseason games. |
| `dummy_key` | String | Placeholder key emitted by the stats API's box score summary payload; carries no data. |

**Officials**

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `name` | String | Display name. |
| `name_i` | String | Initialed name (e.g. 'A. Wilson'). |
| `first_name` | String | Player's first name. |
| `family_name` | String | Player's family / last name. |
| `jersey_num` | String | Jersey number worn by the player. |
| `assignment` | String | Assignment. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_boxscoresummaryv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_boxscoresummaryv3-example}

```python
wnba_stats_boxscoresummaryv3()
```

## wnba_stats_boxscoretraditionalv2

Box score traditional v2.

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
| `reb` | Int64 | Total rebounds. |
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
| `reb` | Int64 | Total rebounds. |
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
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `to` | Int64 | To. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_boxscoretraditionalv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_boxscoretraditionalv2-example}

```python
wnba_stats_boxscoretraditionalv2()
```

## wnba_stats_boxscoretraditionalv3

Box score traditional v3.

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

### Raises {#wnba_stats_boxscoretraditionalv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_boxscoretraditionalv3-example}

```python
wnba_stats_boxscoretraditionalv3()
```

## wnba_stats_boxscoreusagev2

Box score usage v2.

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
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `start_position` | String | Position the player started the game at ('F', 'C', or 'G'); empty for bench players. |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | String | Minutes played. |
| `usg_pct` | Float64 | Percentage of team plays used while on the floor. |
| `pct_fgm` | Float64 | Share of the team's field goals made accounted for while on the floor. |
| `pct_fga` | Float64 | Share of the team's field goal attempts accounted for while on the floor. |
| `pct_fg3_m` | Float64 | Share of the team's made three-pointers accounted for while on the floor. |
| `pct_fg3_a` | Float64 | Share of the team's three-point attempts accounted for while on the floor. |
| `pct_ftm` | Float64 | Share of the team's made free throws accounted for while on the floor. |
| `pct_fta` | Float64 | Share of the team's free throw attempts accounted for while on the floor. |
| `pct_oreb` | Float64 | Share of the team's offensive rebounds accounted for while on the floor. |
| `pct_dreb` | Float64 | Share of the team's defensive rebounds accounted for while on the floor. |
| `pct_reb` | Float64 | Share of the team's total rebounds accounted for while on the floor. |
| `pct_ast` | Float64 | Share of the team's assists accounted for while on the floor. |
| `pct_tov` | Float64 | Share of the team's turnovers accounted for while on the floor. |
| `pct_stl` | Float64 | Share of the team's steals accounted for while on the floor. |
| `pct_blk` | Float64 | Share of the team's blocks accounted for while on the floor. |
| `pct_blka` | Float64 | Share of the team's blocked own attempts accounted for while on the floor. |
| `pct_pf` | Float64 | Share of the team's personal fouls accounted for while on the floor. |
| `pct_pfd` | Float64 | Share of the team's personal fouls drawn accounted for while on the floor. |
| `pct_pts` | Float64 | Share of the team's points accounted for while on the floor. |

**sqlTeamsUsage**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `min` | String | Minutes played. |
| `usg_pct` | Float64 | Percentage of team plays used while on the floor. |
| `pct_fgm` | Float64 | Share of the team's field goals made accounted for while on the floor. |
| `pct_fga` | Float64 | Share of the team's field goal attempts accounted for while on the floor. |
| `pct_fg3_m` | Float64 | Share of the team's made three-pointers accounted for while on the floor. |
| `pct_fg3_a` | Float64 | Share of the team's three-point attempts accounted for while on the floor. |
| `pct_ftm` | Float64 | Share of the team's made free throws accounted for while on the floor. |
| `pct_fta` | Float64 | Share of the team's free throw attempts accounted for while on the floor. |
| `pct_oreb` | Float64 | Share of the team's offensive rebounds accounted for while on the floor. |
| `pct_dreb` | Float64 | Share of the team's defensive rebounds accounted for while on the floor. |
| `pct_reb` | Float64 | Share of the team's total rebounds accounted for while on the floor. |
| `pct_ast` | Float64 | Share of the team's assists accounted for while on the floor. |
| `pct_tov` | Float64 | Share of the team's turnovers accounted for while on the floor. |
| `pct_stl` | Float64 | Share of the team's steals accounted for while on the floor. |
| `pct_blk` | Float64 | Share of the team's blocks accounted for while on the floor. |
| `pct_blka` | Float64 | Share of the team's blocked own attempts accounted for while on the floor. |
| `pct_pf` | Float64 | Share of the team's personal fouls accounted for while on the floor. |
| `pct_pfd` | Float64 | Share of the team's personal fouls drawn accounted for while on the floor. |
| `pct_pts` | Float64 | Share of the team's points accounted for while on the floor. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_boxscoreusagev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_boxscoreusagev2-example}

```python
wnba_stats_boxscoreusagev2()
```

## wnba_stats_boxscoreusagev3

Box score usage v3.

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

### Raises {#wnba_stats_boxscoreusagev3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_boxscoreusagev3-example}

```python
wnba_stats_boxscoreusagev3()
```
