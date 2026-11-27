# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoretraditionalv2–boxscoreusagev3

> NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoretraditionalv2–boxscoreusagev3 — function reference in sdv-py, the SportsDataverse Python package.

## nbagl_stats_boxscoretraditionalv2

Box score traditional v2.

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
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `nickname` | String |  |
| `start_position` | String | Position the player started the game at (F, C, or G); empty for reserves. |
| `comment` | String |  |
| `min` | String |  |
| `fgm` | Int64 |  |
| `fga` | Int64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Int64 |  |
| `fta` | Int64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Int64 |  |
| `dreb` | Int64 |  |
| `reb` | Int64 |  |
| `ast` | Int64 |  |
| `stl` | Int64 |  |
| `blk` | Int64 |  |
| `to` | Int64 |  |
| `pf` | Int64 |  |
| `pts` | Int64 |  |
| `plus_minus` | Int64 |  |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `min` | String |  |
| `fgm` | Int64 |  |
| `fga` | Int64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Int64 |  |
| `fta` | Int64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Int64 |  |
| `dreb` | Int64 |  |
| `reb` | Int64 |  |
| `ast` | Int64 |  |
| `stl` | Int64 |  |
| `blk` | Int64 |  |
| `to` | Int64 |  |
| `pf` | Int64 |  |
| `pts` | Int64 |  |
| `plus_minus` | Int64 |  |

**TeamStarterBenchStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `starters_bench` | String |  |
| `min` | String |  |
| `fgm` | Int64 |  |
| `fga` | Int64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Int64 |  |
| `fta` | Int64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Int64 |  |
| `dreb` | Int64 |  |
| `reb` | Int64 |  |
| `ast` | Int64 |  |
| `stl` | Int64 |  |
| `blk` | Int64 |  |
| `to` | Int64 |  |
| `pf` | Int64 |  |
| `pts` | Int64 |  |

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

Box score traditional v3.

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
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | Int64 |  |
| `home_team_id` | Int64 |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `person_id` | Int64 | Player identifier from the league's stats API. |
| `first_name` | String |  |
| `family_name` | String | Player's family (last) name. |
| `name_i` | String | Abbreviated player name (first initial and last name). |
| `player_slug` | String | URL-friendly slug for the player's name. |
| `position` | String |  |
| `comment` | String |  |
| `jersey_num` | String | Player's jersey number. |
| `minutes` | String |  |
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
| `assists` | Int64 |  |
| `steals` | Int64 |  |
| `blocks` | Int64 |  |
| `turnovers` | Int64 |  |
| `fouls_personal` | Int64 | Personal fouls recorded in the game. |
| `points` | Int64 |  |
| `plus_minus_points` | Float64 | Team point differential while the player was on the floor (plus-minus). |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | Int64 |  |
| `home_team_id` | Int64 |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `minutes` | String |  |
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
| `assists` | Int64 |  |
| `steals` | Int64 |  |
| `blocks` | Int64 |  |
| `turnovers` | Int64 |  |
| `fouls_personal` | Int64 | Personal fouls recorded in the game. |
| `points` | Int64 |  |
| `plus_minus_points` | Float64 | Team point differential while the player was on the floor (plus-minus). |

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

Box score usage v2.

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
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `nickname` | String |  |
| `start_position` | String | Position the player started the game at ('F', 'C', or 'G'); empty for bench players. |
| `comment` | String |  |
| `min` | String |  |
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
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `min` | String |  |
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

Box score usage v3.

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
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `away_team_id` | Int64 |  |
| `home_team_id` | Int64 |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `person_id` | Int64 | Player identifier from the league's stats API. |
| `first_name` | String |  |
| `family_name` | String | Player's family (last) name. |
| `name_i` | String | Abbreviated player name (first initial and last name). |
| `player_slug` | String | URL-friendly slug for the player's name. |
| `position` | String |  |
| `comment` | String |  |
| `jersey_num` | String | Player's jersey number. |
| `minutes` | String |  |
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
| `away_team_id` | Int64 |  |
| `home_team_id` | Int64 |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `minutes` | String |  |
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
