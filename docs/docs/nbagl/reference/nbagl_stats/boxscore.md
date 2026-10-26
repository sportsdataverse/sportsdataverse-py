---
title: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoreadvancedv2–boxscoresummaryv3"
sidebar_label: "Box scores: boxscoreadvancedv2–boxscoresummaryv3"
sidebar_position: 1
description: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoreadvancedv2–boxscoresummaryv3 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoreadvancedv2–boxscoresummaryv3

## nbagl_stats_boxscoreadvancedv2

Box score advanced v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreadvancedv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoreadvancedv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoreadvancedv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoreadvancedv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

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
| `start_position` | String | Position the player started the game at ('F', 'C', or 'G'); empty for bench players. |
| `comment` | String |  |
| `min` | String |  |
| `e_off_rating` | Float64 | Estimated offensive rating: points produced per 100 possessions using the stats API's estimated-possession formula. |
| `off_rating` | Float64 | Points scored per 100 possessions while on the floor. |
| `e_def_rating` | Float64 | Estimated defensive rating: points allowed per 100 possessions using the estimated-possession formula. |
| `def_rating` | Float64 | Points allowed per 100 possessions while on the floor. |
| `e_net_rating` | Float64 | Estimated net rating: estimated offensive rating minus estimated defensive rating. |
| `net_rating` | Float64 |  |
| `ast_pct` | Float64 |  |
| `ast_tov` | Float64 | Ratio of assists to turnovers. |
| `ast_ratio` | Float64 | Assists per 100 possessions used. |
| `oreb_pct` | Float64 | Percentage of available offensive rebounds grabbed while on the floor. |
| `dreb_pct` | Float64 | Percentage of available defensive rebounds grabbed while on the floor. |
| `reb_pct` | Float64 | Percentage of all available rebounds grabbed while on the floor. |
| `tm_tov_pct` | Float64 | Turnovers committed per 100 possessions. |
| `efg_pct` | Float64 | Effective field goal percentage: (FGM + 0.5 * FG3M) / FGA. |
| `ts_pct` | Float64 |  |
| `usg_pct` | Float64 | Percentage of team plays used while on the floor. |
| `e_usg_pct` | Float64 | Estimated usage percentage using the stats API's estimated-possession formula. |
| `e_pace` | Float64 | Estimated pace: team possessions per regulation game, from the estimated-possession formula. |
| `pace` | Float64 |  |
| `pace_per40` | Float64 |  |
| `poss` | Int64 |  |
| `pie` | Float64 |  |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `min` | String |  |
| `e_off_rating` | Float64 | Estimated offensive rating: points produced per 100 possessions using the stats API's estimated-possession formula. |
| `off_rating` | Float64 | Points scored per 100 possessions while on the floor. |
| `e_def_rating` | Float64 | Estimated defensive rating: points allowed per 100 possessions using the estimated-possession formula. |
| `def_rating` | Float64 | Points allowed per 100 possessions while on the floor. |
| `e_net_rating` | Float64 | Estimated net rating: estimated offensive rating minus estimated defensive rating. |
| `net_rating` | Float64 |  |
| `ast_pct` | Float64 |  |
| `ast_tov` | Float64 | Ratio of assists to turnovers. |
| `ast_ratio` | Float64 | Assists per 100 possessions used. |
| `oreb_pct` | Float64 | Percentage of available offensive rebounds grabbed while on the floor. |
| `dreb_pct` | Float64 | Percentage of available defensive rebounds grabbed while on the floor. |
| `reb_pct` | Float64 | Percentage of all available rebounds grabbed while on the floor. |
| `e_tm_tov_pct` | Float64 |  |
| `tm_tov_pct` | Float64 | Turnovers committed per 100 possessions. |
| `efg_pct` | Float64 | Effective field goal percentage: (FGM + 0.5 * FG3M) / FGA. |
| `ts_pct` | Float64 |  |
| `usg_pct` | Float64 | Percentage of team plays used while on the floor. |
| `e_usg_pct` | Float64 | Estimated usage percentage using the stats API's estimated-possession formula. |
| `e_pace` | Float64 | Estimated pace: team possessions per regulation game, from the estimated-possession formula. |
| `pace` | Float64 |  |
| `pace_per40` | Float64 |  |
| `poss` | Int64 |  |
| `pie` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoreadvancedv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoreadvancedv2-example}

```python
nbagl_stats_boxscoreadvancedv2()
```

## nbagl_stats_boxscoreadvancedv3

Box score advanced v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreadvancedv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreadvancedv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoreadvancedv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoreadvancedv3-returns}

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
| `estimated_offensive_rating` | Float64 | Estimated offensive rating from the stats API's estimated-metrics family. |
| `offensive_rating` | Float64 | Points scored per 100 possessions while on the floor (offensive rating). |
| `estimated_defensive_rating` | Float64 | Estimated defensive rating from the stats API's estimated-metrics family. |
| `defensive_rating` | Float64 | Points allowed per 100 possessions while on the floor (defensive rating). |
| `estimated_net_rating` | Float64 | Estimated net rating (estimated offensive minus defensive rating) from the stats API's estimated-metrics family. |
| `net_rating` | Float64 | Offensive rating minus defensive rating while on the floor (net rating). |
| `assist_percentage` | Float64 | Percentage of teammate field goals assisted while on the floor, as a decimal. |
| `assist_to_turnover` | Float64 | Ratio of assists to turnovers. |
| `assist_ratio` | Float64 | Assists per 100 possessions used (assist ratio). |
| `offensive_rebound_percentage` | Float64 | Percentage of available offensive rebounds secured while on the floor, as a decimal. |
| `defensive_rebound_percentage` | Float64 | Percentage of available defensive rebounds secured while on the floor, as a decimal. |
| `rebound_percentage` | Float64 | Percentage of all available rebounds secured while on the floor, as a decimal. |
| `turnover_ratio` | Float64 | Turnovers per 100 possessions used (turnover ratio). |
| `effective_field_goal_percentage` | Float64 | Effective field goal percentage (weights made threes at 1.5), as a decimal. |
| `true_shooting_percentage` | Float64 | True shooting percentage (accounts for threes and free throws), as a decimal. |
| `usage_percentage` | Float64 | Percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_usage_percentage` | Float64 | Estimated percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_pace` | Float64 | Estimated pace (possessions per 48 minutes) from the stats API's estimated-metrics family. |
| `pace` | Float64 |  |
| `pace_per40` | Float64 | Pace normalized to possessions per 40 minutes. |
| `possessions` | Float64 |  |
| `pie` | Float64 |  |

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
| `estimated_offensive_rating` | Float64 | Estimated offensive rating from the stats API's estimated-metrics family. |
| `offensive_rating` | Float64 | Points scored per 100 possessions while on the floor (offensive rating). |
| `estimated_defensive_rating` | Float64 | Estimated defensive rating from the stats API's estimated-metrics family. |
| `defensive_rating` | Float64 | Points allowed per 100 possessions while on the floor (defensive rating). |
| `estimated_net_rating` | Float64 | Estimated net rating (estimated offensive minus defensive rating) from the stats API's estimated-metrics family. |
| `net_rating` | Float64 | Offensive rating minus defensive rating while on the floor (net rating). |
| `assist_percentage` | Float64 | Percentage of teammate field goals assisted while on the floor, as a decimal. |
| `assist_to_turnover` | Float64 | Ratio of assists to turnovers. |
| `assist_ratio` | Float64 | Assists per 100 possessions used (assist ratio). |
| `offensive_rebound_percentage` | Float64 | Percentage of available offensive rebounds secured while on the floor, as a decimal. |
| `defensive_rebound_percentage` | Float64 | Percentage of available defensive rebounds secured while on the floor, as a decimal. |
| `rebound_percentage` | Float64 | Percentage of all available rebounds secured while on the floor, as a decimal. |
| `estimated_team_turnover_percentage` | Float64 |  |
| `turnover_ratio` | Float64 | Turnovers per 100 possessions used (turnover ratio). |
| `effective_field_goal_percentage` | Float64 | Effective field goal percentage (weights made threes at 1.5), as a decimal. |
| `true_shooting_percentage` | Float64 | True shooting percentage (accounts for threes and free throws), as a decimal. |
| `usage_percentage` | Float64 | Percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_usage_percentage` | Float64 | Estimated percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_pace` | Float64 | Estimated pace (possessions per 48 minutes) from the stats API's estimated-metrics family. |
| `pace` | Float64 |  |
| `pace_per40` | Float64 | Pace normalized to possessions per 40 minutes. |
| `possessions` | Float64 |  |
| `pie` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoreadvancedv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoreadvancedv3-example}

```python
nbagl_stats_boxscoreadvancedv3()
```

## nbagl_stats_boxscorefourfactorsv2

Box score four factors v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorefourfactorsv2`

**Valid URL:** [https://stats.nba.com/stats/boxscorefourfactorsv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscorefourfactorsv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscorefourfactorsv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`sqlPlayersFourFactors`, `sqlTeamsFourFactors`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**sqlPlayersFourFactors**

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
| `efg_pct` | Float64 | Effective field goal percentage: (FGM + 0.5 * FG3M) / FGA. |
| `fta_rate` | Float64 | Free throw attempt rate: free throw attempts per field goal attempt. |
| `tm_tov_pct` | Float64 | Turnovers committed per 100 possessions. |
| `oreb_pct` | Float64 | Percentage of available offensive rebounds grabbed while on the floor. |
| `opp_efg_pct` | Float64 | Opponent effective field goal percentage while on the floor. |
| `opp_fta_rate` | Float64 | Opponent free throw attempts per field goal attempt while on the floor. |
| `opp_tov_pct` | Float64 | Opponent turnovers per 100 possessions while on the floor. |
| `opp_oreb_pct` | Float64 | Percentage of available offensive rebounds grabbed by the opponent while on the floor. |

**sqlTeamsFourFactors**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `min` | String |  |
| `efg_pct` | Float64 | Effective field goal percentage: (FGM + 0.5 * FG3M) / FGA. |
| `fta_rate` | Float64 | Free throw attempt rate: free throw attempts per field goal attempt. |
| `tm_tov_pct` | Float64 | Turnovers committed per 100 possessions. |
| `oreb_pct` | Float64 | Percentage of available offensive rebounds grabbed while on the floor. |
| `opp_efg_pct` | Float64 | Opponent effective field goal percentage while on the floor. |
| `opp_fta_rate` | Float64 | Opponent free throw attempts per field goal attempt while on the floor. |
| `opp_tov_pct` | Float64 | Opponent turnovers per 100 possessions while on the floor. |
| `opp_oreb_pct` | Float64 | Percentage of available offensive rebounds grabbed by the opponent while on the floor. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscorefourfactorsv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscorefourfactorsv2-example}

```python
nbagl_stats_boxscorefourfactorsv2()
```

## nbagl_stats_boxscorefourfactorsv3

Box score four factors v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorefourfactorsv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorefourfactorsv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscorefourfactorsv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscorefourfactorsv3-returns}

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
| `effective_field_goal_percentage` | Float64 | Effective field goal percentage four-factor, as a decimal. |
| `free_throw_attempt_rate` | Float64 | Free throw attempts per field goal attempt (free throw rate four-factor). |
| `team_turnover_percentage` | Float64 | Turnovers committed per 100 possessions (turnover four-factor). |
| `offensive_rebound_percentage` | Float64 | Offensive rebound percentage four-factor, as a decimal. |
| `opp_effective_field_goal_percentage` | Float64 | Opponent's effective field goal percentage while on the floor, as a decimal. |
| `opp_free_throw_attempt_rate` | Float64 | Opponent's free throw attempt rate while on the floor. |
| `opp_team_turnover_percentage` | Float64 | Opponent turnovers forced per 100 possessions while on the floor. |
| `opp_offensive_rebound_percentage` | Float64 | Opponent's offensive rebound percentage while on the floor, as a decimal. |

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
| `effective_field_goal_percentage` | Float64 | Effective field goal percentage four-factor, as a decimal. |
| `free_throw_attempt_rate` | Float64 | Free throw attempts per field goal attempt (free throw rate four-factor). |
| `team_turnover_percentage` | Float64 | Turnovers committed per 100 possessions (turnover four-factor). |
| `offensive_rebound_percentage` | Float64 | Offensive rebound percentage four-factor, as a decimal. |
| `opp_effective_field_goal_percentage` | Float64 | Opponent's effective field goal percentage while on the floor, as a decimal. |
| `opp_free_throw_attempt_rate` | Float64 | Opponent's free throw attempt rate while on the floor. |
| `opp_team_turnover_percentage` | Float64 | Opponent turnovers forced per 100 possessions while on the floor. |
| `opp_offensive_rebound_percentage` | Float64 | Opponent's offensive rebound percentage while on the floor, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscorefourfactorsv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscorefourfactorsv3-example}

```python
nbagl_stats_boxscorefourfactorsv3()
```

## nbagl_stats_boxscorehustlev2

Box score hustle v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorehustlev2`

**Valid URL:** [https://stats.nba.com/stats/boxscorehustlev2?GameID=2022500137](https://stats.nba.com/stats/boxscorehustlev2?GameID=2022500137)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscorehustlev2-returns}

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
| `points` | Int64 |  |
| `contested_shots` | Int64 | Opponent shot attempts contested in the game. |
| `contested_shots2pt` | Int64 | Opponent two-point attempts contested. |
| `contested_shots3pt` | Int64 | Opponent three-point attempts contested. |
| `deflections` | Int64 |  |
| `charges_drawn` | Int64 | Offensive charges drawn. |
| `screen_assists` | Int64 | Screens that led directly to a teammate's made field goal (screen assists). |
| `screen_assist_points` | Int64 | Points teammates scored directly off the player's screen assists. |
| `loose_balls_recovered_offensive` | Int64 | Loose balls recovered while on offense. |
| `loose_balls_recovered_defensive` | Int64 | Loose balls recovered while on defense. |
| `loose_balls_recovered_total` | Int64 | Total loose balls recovered in the game. |
| `offensive_box_outs` | Int64 | Box-outs recorded on the offensive glass. |
| `defensive_box_outs` | Int64 | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebounds` | Int64 | Team rebounds secured following the player's box-outs. |
| `box_out_player_rebounds` | Int64 | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | Int64 | Total box-outs recorded in the game (hustle stats tracking). |

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
| `points` | Int64 |  |
| `contested_shots` | Int64 | Opponent shot attempts contested in the game. |
| `contested_shots2pt` | Int64 | Opponent two-point attempts contested. |
| `contested_shots3pt` | Int64 | Opponent three-point attempts contested. |
| `deflections` | Int64 |  |
| `charges_drawn` | Int64 | Offensive charges drawn. |
| `screen_assists` | Int64 | Screens that led directly to a teammate's made field goal (screen assists). |
| `screen_assist_points` | Int64 | Points teammates scored directly off the player's screen assists. |
| `loose_balls_recovered_offensive` | Int64 | Loose balls recovered while on offense. |
| `loose_balls_recovered_defensive` | Int64 | Loose balls recovered while on defense. |
| `loose_balls_recovered_total` | Int64 | Total loose balls recovered in the game. |
| `offensive_box_outs` | Int64 | Box-outs recorded on the offensive glass. |
| `defensive_box_outs` | Int64 | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebounds` | Int64 | Team rebounds secured following the player's box-outs. |
| `box_out_player_rebounds` | Int64 | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | Int64 | Total box-outs recorded in the game (hustle stats tracking). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscorehustlev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscorehustlev2-example}

```python
nbagl_stats_boxscorehustlev2()
```

## nbagl_stats_boxscorematchupsv3

Box score matchups v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorematchupsv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorematchupsv3?GameID=2022500137](https://stats.nba.com/stats/boxscorematchupsv3?GameID=2022500137)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscorematchupsv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | Int64 |  |
| `home_team_id` | Int64 |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_tricode` | String | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscorematchupsv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscorematchupsv3-example}

```python
nbagl_stats_boxscorematchupsv3()
```

## nbagl_stats_boxscoremiscv2

Box score misc v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoremiscv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoremiscv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoremiscv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoremiscv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`sqlPlayersMisc`, `sqlTeamsMisc`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**sqlPlayersMisc**

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
| `pts_off_tov` | Int64 | Points scored following opponent turnovers. |
| `pts_2_nd_chance` | Int64 | Second-chance points scored after offensive rebounds. |
| `pts_fb` | Int64 | Fast-break points scored. |
| `pts_paint` | Int64 | Points scored in the paint. |
| `opp_pts_off_tov` | Float64 | Opponent points scored off turnovers while on the floor. |
| `opp_pts_2_nd_chance` | Float64 | Opponent second-chance points scored while on the floor. |
| `opp_pts_fb` | Float64 | Opponent fast-break points scored while on the floor. |
| `opp_pts_paint` | Float64 | Opponent points in the paint scored while on the floor. |
| `blk` | Int64 |  |
| `blka` | Int64 | Number of own field goal attempts that were blocked by opponents. |
| `pf` | Int64 |  |
| `pfd` | Int64 | Personal fouls drawn. |

**sqlTeamsMisc**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `min` | String |  |
| `pts_off_tov` | Float64 | Points scored following opponent turnovers. |
| `pts_2_nd_chance` | Float64 | Second-chance points scored after offensive rebounds. |
| `pts_fb` | Float64 | Fast-break points scored. |
| `pts_paint` | Float64 | Points scored in the paint. |
| `opp_pts_off_tov` | Float64 | Opponent points scored off turnovers while on the floor. |
| `opp_pts_2_nd_chance` | Float64 | Opponent second-chance points scored while on the floor. |
| `opp_pts_fb` | Float64 | Opponent fast-break points scored while on the floor. |
| `opp_pts_paint` | Float64 | Opponent points in the paint scored while on the floor. |
| `blk` | Int64 |  |
| `blka` | Int64 | Number of own field goal attempts that were blocked by opponents. |
| `pf` | Int64 |  |
| `pfd` | Int64 | Personal fouls drawn. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoremiscv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoremiscv2-example}

```python
nbagl_stats_boxscoremiscv2()
```

## nbagl_stats_boxscoremiscv3

Box score misc v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoremiscv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoremiscv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoremiscv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoremiscv3-returns}

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
| `points_off_turnovers` | Int64 | Points scored off opponent turnovers. |
| `points_second_chance` | Int64 | Second-chance points scored after offensive rebounds. |
| `points_fast_break` | Int64 | Fast-break points scored. |
| `points_paint` | Int64 | Points scored in the paint. |
| `opp_points_off_turnovers` | Int64 | Opponent points off turnovers scored while on the floor. |
| `opp_points_second_chance` | Int64 | Opponent second-chance points scored while on the floor. |
| `opp_points_fast_break` | Int64 | Opponent fast-break points scored while on the floor. |
| `opp_points_paint` | Int64 | Opponent points in the paint scored while on the floor. |
| `blocks` | Int64 |  |
| `blocks_against` | Int64 | Player's shot attempts that were blocked by opponents. |
| `fouls_personal` | Int64 | Personal fouls committed. |
| `fouls_drawn` | Int64 | Personal fouls drawn. |

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
| `points_off_turnovers` | Int64 | Points scored off opponent turnovers. |
| `points_second_chance` | Int64 | Second-chance points scored after offensive rebounds. |
| `points_fast_break` | Int64 | Fast-break points scored. |
| `points_paint` | Int64 | Points scored in the paint. |
| `opp_points_off_turnovers` | Int64 | Opponent points off turnovers scored while on the floor. |
| `opp_points_second_chance` | Int64 | Opponent second-chance points scored while on the floor. |
| `opp_points_fast_break` | Int64 | Opponent fast-break points scored while on the floor. |
| `opp_points_paint` | Int64 | Opponent points in the paint scored while on the floor. |
| `blocks` | Int64 |  |
| `blocks_against` | Int64 | Player's shot attempts that were blocked by opponents. |
| `fouls_personal` | Int64 | Personal fouls committed. |
| `fouls_drawn` | Int64 | Personal fouls drawn. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoremiscv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoremiscv3-example}

```python
nbagl_stats_boxscoremiscv3()
```

## nbagl_stats_boxscoreplayertrackv3

Box score player track v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreplayertrackv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreplayertrackv3?GameID=2022500137](https://stats.nba.com/stats/boxscoreplayertrackv3?GameID=2022500137)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoreplayertrackv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | Int64 |  |
| `home_team_id` | Int64 |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_tricode` | String | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `person_id` | Int64 | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `first_name` | String |  |
| `family_name` | String | Display name for familyname associated with this NBA or WNBA Stats row. |
| `name_i` | String | NBA or WNBA Stats value for namei in the boxscoreplayertrackv3 result set. |
| `player_slug` | String | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | String |  |
| `comment` | String |  |
| `jersey_num` | String | NBA or WNBA Stats value for jerseynum in the boxscoreplayertrackv3 result set. |
| `minutes` | String |  |
| `speed` | Float64 |  |
| `distance` | Float64 |  |
| `rebound_chances_offensive` | Int64 | Rebounding metric for reboundchancesoffensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_defensive` | Int64 | Rebounding metric for reboundchancesdefensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_total` | Int64 | Rebounding metric for reboundchancestotal in the requested NBA or WNBA Stats split. |
| `touches` | Int64 |  |
| `secondary_assists` | Int64 | Passing or assist metric for secondaryassists in the requested NBA or WNBA Stats split. |
| `free_throw_assists` | Int64 | Shooting metric for freethrowassists in the requested NBA or WNBA Stats split. |
| `passes` | Int64 |  |
| `assists` | Int64 |  |
| `contested_field_goals_made` | Int64 | Shooting metric for contestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `contested_field_goals_attempted` | Int64 | Shooting metric for contestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `contested_field_goal_percentage` | Float64 | Percentage or rate for contestedfieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_made` | Int64 | Shooting metric for uncontestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_attempted` | Int64 | Shooting metric for uncontestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_percentage` | Float64 | Percentage or rate for uncontestedfieldgoalspercentage in the requested NBA or WNBA Stats split. |
| `field_goal_percentage` | Float64 | Percentage or rate for fieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goals_made` | Int64 | Shooting metric for defendedatrimfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goals_attempted` | Int64 | Shooting metric for defendedatrimfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goal_percentage` | Float64 | Percentage or rate for defendedatrimfieldgoalpercentage in the requested NBA or WNBA Stats split. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | Int64 |  |
| `home_team_id` | Int64 |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_tricode` | String | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `minutes` | String |  |
| `distance` | Float64 |  |
| `rebound_chances_offensive` | Int64 | Rebounding metric for reboundchancesoffensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_defensive` | Int64 | Rebounding metric for reboundchancesdefensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_total` | Int64 | Rebounding metric for reboundchancestotal in the requested NBA or WNBA Stats split. |
| `touches` | Int64 |  |
| `secondary_assists` | Int64 | Passing or assist metric for secondaryassists in the requested NBA or WNBA Stats split. |
| `free_throw_assists` | Int64 | Shooting metric for freethrowassists in the requested NBA or WNBA Stats split. |
| `passes` | Int64 |  |
| `assists` | Int64 |  |
| `contested_field_goals_made` | Int64 | Shooting metric for contestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `contested_field_goals_attempted` | Int64 | Shooting metric for contestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `contested_field_goal_percentage` | Float64 | Percentage or rate for contestedfieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_made` | Int64 | Shooting metric for uncontestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_attempted` | Int64 | Shooting metric for uncontestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_percentage` | Float64 | Percentage or rate for uncontestedfieldgoalspercentage in the requested NBA or WNBA Stats split. |
| `field_goal_percentage` | Float64 | Percentage or rate for fieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goals_made` | Int64 | Shooting metric for defendedatrimfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goals_attempted` | Int64 | Shooting metric for defendedatrimfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goal_percentage` | Float64 | Percentage or rate for defendedatrimfieldgoalpercentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoreplayertrackv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoreplayertrackv3-example}

```python
nbagl_stats_boxscoreplayertrackv3()
```

## nbagl_stats_boxscorescoringv2

Box score scoring v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorescoringv2`

**Valid URL:** [https://stats.nba.com/stats/boxscorescoringv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscorescoringv2?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscorescoringv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`sqlPlayersScoring`, `sqlTeamsScoring`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**sqlPlayersScoring**

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
| `pct_fga_2_pt` | Float64 | Share of field goal attempts taken as two-pointers. |
| `pct_fga_3_pt` | Float64 | Share of field goal attempts taken as three-pointers. |
| `pct_pts_2_pt` | Float64 | Share of points scored on two-point field goals. |
| `pct_pts_2_pt_mr` | Float64 | Share of points scored on mid-range two-point field goals. |
| `pct_pts_3_pt` | Float64 | Share of points scored on three-point field goals. |
| `pct_pts_fb` | Float64 | Share of points scored on the fast break. |
| `pct_pts_ft` | Float64 | Share of points scored on free throws. |
| `pct_pts_off_tov` | Float64 | Share of points scored off opponent turnovers. |
| `pct_pts_paint` | Float64 | Share of points scored in the paint. |
| `pct_ast_2_pm` | Float64 | Share of made two-point field goals that were assisted. |
| `pct_uast_2_pm` | Float64 | Share of made two-point field goals that were unassisted. |
| `pct_ast_3_pm` | Float64 | Share of made three-point field goals that were assisted. |
| `pct_uast_3_pm` | Float64 | Share of made three-point field goals that were unassisted. |
| `pct_ast_fgm` | Float64 | Share of all made field goals that were assisted. |
| `pct_uast_fgm` | Float64 | Share of all made field goals that were unassisted. |

**sqlTeamsScoring**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `min` | String |  |
| `pct_fga_2_pt` | Float64 | Share of field goal attempts taken as two-pointers. |
| `pct_fga_3_pt` | Float64 | Share of field goal attempts taken as three-pointers. |
| `pct_pts_2_pt` | Float64 | Share of points scored on two-point field goals. |
| `pct_pts_2_pt_mr` | Float64 | Share of points scored on mid-range two-point field goals. |
| `pct_pts_3_pt` | Float64 | Share of points scored on three-point field goals. |
| `pct_pts_fb` | Float64 | Share of points scored on the fast break. |
| `pct_pts_ft` | Float64 | Share of points scored on free throws. |
| `pct_pts_off_tov` | Float64 | Share of points scored off opponent turnovers. |
| `pct_pts_paint` | Float64 | Share of points scored in the paint. |
| `pct_ast_2_pm` | Float64 | Share of made two-point field goals that were assisted. |
| `pct_uast_2_pm` | Float64 | Share of made two-point field goals that were unassisted. |
| `pct_ast_3_pm` | Float64 | Share of made three-point field goals that were assisted. |
| `pct_uast_3_pm` | Float64 | Share of made three-point field goals that were unassisted. |
| `pct_ast_fgm` | Float64 | Share of all made field goals that were assisted. |
| `pct_uast_fgm` | Float64 | Share of all made field goals that were unassisted. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscorescoringv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscorescoringv2-example}

```python
nbagl_stats_boxscorescoringv2()
```

## nbagl_stats_boxscorescoringv3

Box score scoring v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorescoringv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorescoringv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscorescoringv3?EndPeriod=14&EndRange=0&GameID=2022500137&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscorescoringv3-returns}

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
| `percentage_field_goals_attempted2pt` | Float64 | Share of field goal attempts taken as two-pointers, as a decimal. |
| `percentage_field_goals_attempted3pt` | Float64 | Share of field goal attempts taken as three-pointers, as a decimal. |
| `percentage_points2pt` | Float64 | Share of points scored on two-pointers, as a decimal. |
| `percentage_points_midrange2pt` | Float64 | Share of points scored on mid-range two-pointers, as a decimal. |
| `percentage_points3pt` | Float64 | Share of points scored on three-pointers, as a decimal. |
| `percentage_points_fast_break` | Float64 | Share of points scored on fast breaks, as a decimal. |
| `percentage_points_free_throw` | Float64 | Share of points scored at the free throw line, as a decimal. |
| `percentage_points_off_turnovers` | Float64 | Share of points scored off opponent turnovers, as a decimal. |
| `percentage_points_paint` | Float64 | Share of points scored in the paint, as a decimal. |
| `percentage_assisted2pt` | Float64 | Percentage of made two-pointers that were assisted, as a decimal. |
| `percentage_unassisted2pt` | Float64 | Percentage of made two-pointers that were unassisted, as a decimal. |
| `percentage_assisted3pt` | Float64 | Percentage of made three-pointers that were assisted, as a decimal. |
| `percentage_unassisted3pt` | Float64 | Percentage of made three-pointers that were unassisted, as a decimal. |
| `percentage_assisted_fgm` | Float64 | Percentage of made field goals that were assisted, as a decimal. |
| `percentage_unassisted_fgm` | Float64 | Percentage of made field goals that were unassisted, as a decimal. |

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
| `percentage_field_goals_attempted2pt` | Float64 | Share of field goal attempts taken as two-pointers, as a decimal. |
| `percentage_field_goals_attempted3pt` | Float64 | Share of field goal attempts taken as three-pointers, as a decimal. |
| `percentage_points2pt` | Float64 | Share of points scored on two-pointers, as a decimal. |
| `percentage_points_midrange2pt` | Float64 | Share of points scored on mid-range two-pointers, as a decimal. |
| `percentage_points3pt` | Float64 | Share of points scored on three-pointers, as a decimal. |
| `percentage_points_fast_break` | Float64 | Share of points scored on fast breaks, as a decimal. |
| `percentage_points_free_throw` | Float64 | Share of points scored at the free throw line, as a decimal. |
| `percentage_points_off_turnovers` | Float64 | Share of points scored off opponent turnovers, as a decimal. |
| `percentage_points_paint` | Float64 | Share of points scored in the paint, as a decimal. |
| `percentage_assisted2pt` | Float64 | Percentage of made two-pointers that were assisted, as a decimal. |
| `percentage_unassisted2pt` | Float64 | Percentage of made two-pointers that were unassisted, as a decimal. |
| `percentage_assisted3pt` | Float64 | Percentage of made three-pointers that were assisted, as a decimal. |
| `percentage_unassisted3pt` | Float64 | Percentage of made three-pointers that were unassisted, as a decimal. |
| `percentage_assisted_fgm` | Float64 | Percentage of made field goals that were assisted, as a decimal. |
| `percentage_unassisted_fgm` | Float64 | Percentage of made field goals that were unassisted, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscorescoringv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscorescoringv3-example}

```python
nbagl_stats_boxscorescoringv3()
```

## nbagl_stats_boxscoresummaryv2

Box score summary v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoresummaryv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoresummaryv2?GameID=2022500137](https://stats.nba.com/stats/boxscoresummaryv2?GameID=2022500137)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_boxscoresummaryv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameSummary`, `OtherStats`, `Officials`, `InactivePlayers`, `GameInfo`, `LineScore`, `LastMeeting`, `SeasonSeries`, `AvailableVideo`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GameSummary**

| col_name | type | description |
|---|---|---|
| `game_date_est` | String |  |
| `game_sequence` | Int64 |  |
| `game_id` | String |  |
| `game_status_id` | Int64 |  |
| `game_status_text` | String |  |
| `gamecode` | String |  |
| `home_team_id` | Int64 |  |
| `visitor_team_id` | Int64 |  |
| `season` | String |  |
| `live_period` | Int64 |  |
| `live_pc_time` | String |  |
| `natl_tv_broadcaster_abbreviation` | String |  |
| `live_period_time_bcast` | String |  |
| `wh_status` | Int64 |  |

**OtherStats**

| col_name | type | description |
|---|---|---|
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `pts_paint` | Int64 | Points scored in the paint. |
| `pts_2_nd_chance` | Int64 |  |
| `pts_fb` | Int64 |  |
| `largest_lead` | Int64 |  |
| `lead_changes` | Int64 |  |
| `times_tied` | Int64 |  |
| `team_turnovers` | Int64 |  |
| `total_turnovers` | Int64 |  |
| `team_rebounds` | Int64 | Team rebounds (rebounds credited to the team rather than a player). |
| `pts_off_to` | Int64 |  |

**Officials**

| col_name | type | description |
|---|---|---|
| `official_id` | Int64 |  |
| `first_name` | String |  |
| `last_name` | String |  |
| `jersey_num` | String |  |

**InactivePlayers**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `first_name` | String |  |
| `last_name` | String |  |
| `jersey_num` | String |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |

**GameInfo**

| col_name | type | description |
|---|---|---|
| `game_date` | String |  |
| `attendance` | Int64 |  |
| `game_time` | String |  |

**LineScore**

| col_name | type | description |
|---|---|---|
| `game_date_est` | String |  |
| `game_sequence` | Int64 |  |
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_city_name` | String |  |
| `team_nickname` | String |  |
| `team_wins_losses` | String |  |
| `pts_qtr1` | Int64 |  |
| `pts_qtr2` | Int64 |  |
| `pts_qtr3` | Int64 |  |
| `pts_qtr4` | Int64 |  |
| `pts_ot1` | Int64 |  |
| `pts_ot2` | Int64 | Scoring or score-margin metric for points ot2 in the requested NBA or WNBA Stats split. |
| `pts_ot3` | Int64 | Scoring or score-margin metric for points ot3 in the requested NBA or WNBA Stats split. |
| `pts_ot4` | Int64 | Scoring or score-margin metric for points ot4 in the requested NBA or WNBA Stats split. |
| `pts_ot5` | Int64 | Scoring or score-margin metric for points ot5 in the requested NBA or WNBA Stats split. |
| `pts_ot6` | Int64 | Scoring or score-margin metric for points ot6 in the requested NBA or WNBA Stats split. |
| `pts_ot7` | Int64 | Scoring or score-margin metric for points ot7 in the requested NBA or WNBA Stats split. |
| `pts_ot8` | Int64 | Scoring or score-margin metric for points ot8 in the requested NBA or WNBA Stats split. |
| `pts_ot9` | Int64 | Scoring or score-margin metric for points ot9 in the requested NBA or WNBA Stats split. |
| `pts_ot10` | Int64 | Scoring or score-margin metric for points ot10 in the requested NBA or WNBA Stats split. |
| `pts` | Int64 |  |

**LastMeeting**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
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
| `game_id` | String |  |
| `home_team_id` | Int64 |  |
| `visitor_team_id` | Int64 |  |
| `game_date_est` | String |  |
| `home_team_wins` | Int64 |  |
| `home_team_losses` | Int64 |  |
| `series_leader` | String |  |

**AvailableVideo**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `video_available_flag` | Int64 |  |
| `pt_available` | Int64 |  |
| `pt_xyz_available` | Int64 |  |
| `wh_status` | Int64 |  |
| `hustle_status` | Int64 |  |
| `historical_status` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_boxscoresummaryv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_boxscoresummaryv2-example}

```python
nbagl_stats_boxscoresummaryv2()
```

## nbagl_stats_boxscoresummaryv3

Box score summary v3.

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
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `game_code` | String |  |
| `game_status` | Int64 |  |
| `game_status_text` | String |  |
| `period` | Int64 |  |
| `game_clock` | String |  |
| `game_time_utc` | String |  |
| `game_et` | String |  |
| `away_team_id` | Int64 |  |
| `home_team_id` | Int64 |  |
| `duration` | String |  |
| `attendance` | Int64 |  |
| `sellout` | Int64 |  |
| `series_game_number` | String |  |
| `game_label` | String |  |
| `game_sub_label` | String |  |
| `series_text` | String |  |
| `if_necessary` | Boolean |  |
| `is_neutral` | Boolean |  |
| `video_available_flag` | Int64 |  |
| `pt_available` | Int64 |  |
| `pt_xyz_available` | Int64 |  |
| `wh_status` | Int64 |  |
| `hustle_status` | Int64 |  |
| `historical_status` | Int64 |  |
| `game_subtype` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_city` | String |  |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `team_wins` | Int64 | Team's win total entering the game. |
| `team_losses` | Int64 | Team's loss total entering the game. |
| `score` | Int64 |  |
| `in_bonus` | String | Whether the team is currently in the bonus (penalty) foul situation, as reported by the stats API. |
| `timeouts_remaining` | Int64 | Timeouts the team has remaining. |
| `seed` | Int64 | Team's playoff seed, populated for postseason games. |
| `person_id` | Int64 |  |
| `name` | String |  |
| `name_i` | String |  |
| `first_name` | String |  |
| `family_name` | String |  |
| `jersey_num` | String |  |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique 10-character game identifier from the league's stats API. |
| `game_code` | String |  |
| `game_status` | Int64 |  |
| `game_status_text` | String |  |
| `period` | Int64 |  |
| `game_clock` | String |  |
| `game_time_utc` | String |  |
| `game_et` | String |  |
| `away_team_id` | Int64 |  |
| `home_team_id` | Int64 |  |
| `duration` | String |  |
| `attendance` | Int64 |  |
| `sellout` | Int64 |  |
| `series_game_number` | String |  |
| `game_label` | String |  |
| `game_sub_label` | String |  |
| `series_text` | String |  |
| `if_necessary` | Boolean |  |
| `is_neutral` | Boolean |  |
| `video_available_flag` | Int64 |  |
| `pt_available` | Int64 |  |
| `pt_xyz_available` | Int64 |  |
| `wh_status` | Int64 |  |
| `hustle_status` | Int64 |  |
| `historical_status` | Int64 |  |
| `game_subtype` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_city` | String |  |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `team_wins` | Int64 | Team's win total entering the game. |
| `team_losses` | Int64 | Team's loss total entering the game. |
| `score` | Int64 |  |
| `in_bonus` | String | Whether the team is currently in the bonus (penalty) foul situation, as reported by the stats API. |
| `timeouts_remaining` | Int64 | Timeouts the team has remaining. |
| `seed` | Int64 | Team's playoff seed, populated for postseason games. |
| `dummy_key` | String | Placeholder key emitted by the stats API's box score summary payload; carries no data. |

**Officials**

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 |  |
| `name` | String |  |
| `name_i` | String |  |
| `first_name` | String |  |
| `family_name` | String |  |
| `jersey_num` | String |  |
| `assignment` | String |  |

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
