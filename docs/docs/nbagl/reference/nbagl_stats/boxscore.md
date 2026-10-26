---
title: "NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoreadvancedv2–boxscorescoringv3"
sidebar_label: "Box scores: boxscoreadvancedv2–boxscorescoringv3"
sidebar_position: 1
description: "NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoreadvancedv2–boxscorescoringv3 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBAGL — G League Stats API (stats.nba.com, LeagueID=20) — Box scores: boxscoreadvancedv2–boxscorescoringv3

## nbagl_stats_boxscoreadvancedv2

GET /stats/boxscoreadvancedv2

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
| `e_off_rating` | numeric | Estimated offensive rating: points produced per 100 possessions using the stats API's estimated-possession formula. |
| `off_rating` | numeric | Points scored per 100 possessions while on the floor. |
| `e_def_rating` | numeric | Estimated defensive rating: points allowed per 100 possessions using the estimated-possession formula. |
| `def_rating` | numeric | Points allowed per 100 possessions while on the floor. |
| `e_net_rating` | numeric | Estimated net rating: estimated offensive rating minus estimated defensive rating. |
| `net_rating` | numeric | Net rating (off rating - def rating). |
| `ast_pct` | numeric | Assist percentage. |
| `ast_tov` | numeric | Ratio of assists to turnovers. |
| `ast_ratio` | numeric | Assists per 100 possessions used. |
| `oreb_pct` | numeric | Percentage of available offensive rebounds grabbed while on the floor. |
| `dreb_pct` | numeric | Percentage of available defensive rebounds grabbed while on the floor. |
| `reb_pct` | numeric | Percentage of all available rebounds grabbed while on the floor. |
| `tm_tov_pct` | numeric | Turnovers committed per 100 possessions. |
| `efg_pct` | numeric | Effective field goal percentage: (FGM + 0.5 * FG3M) / FGA. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `usg_pct` | numeric | Percentage of team plays used while on the floor. |
| `e_usg_pct` | numeric | Estimated usage percentage using the stats API's estimated-possession formula. |
| `e_pace` | numeric | Estimated pace: team possessions per regulation game, from the estimated-possession formula. |
| `pace` | numeric | Possessions per 48 minutes. |
| `pace_per40` | numeric | Pace per40. |
| `poss` | integer | Poss. |
| `pie` | numeric | Player Impact Estimate (0-1). |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `min` | character | Minutes played. |
| `e_off_rating` | numeric | Estimated offensive rating: points produced per 100 possessions using the stats API's estimated-possession formula. |
| `off_rating` | numeric | Points scored per 100 possessions while on the floor. |
| `e_def_rating` | numeric | Estimated defensive rating: points allowed per 100 possessions using the estimated-possession formula. |
| `def_rating` | numeric | Points allowed per 100 possessions while on the floor. |
| `e_net_rating` | numeric | Estimated net rating: estimated offensive rating minus estimated defensive rating. |
| `net_rating` | numeric | Net rating (off rating - def rating). |
| `ast_pct` | numeric | Assist percentage. |
| `ast_tov` | numeric | Ratio of assists to turnovers. |
| `ast_ratio` | numeric | Assists per 100 possessions used. |
| `oreb_pct` | numeric | Percentage of available offensive rebounds grabbed while on the floor. |
| `dreb_pct` | numeric | Percentage of available defensive rebounds grabbed while on the floor. |
| `reb_pct` | numeric | Percentage of all available rebounds grabbed while on the floor. |
| `e_tm_tov_pct` | numeric |  |
| `tm_tov_pct` | numeric | Turnovers committed per 100 possessions. |
| `efg_pct` | numeric | Effective field goal percentage: (FGM + 0.5 * FG3M) / FGA. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `usg_pct` | numeric | Percentage of team plays used while on the floor. |
| `e_usg_pct` | numeric | Estimated usage percentage using the stats API's estimated-possession formula. |
| `e_pace` | numeric | Estimated pace: team possessions per regulation game, from the estimated-possession formula. |
| `pace` | numeric | Possessions per 48 minutes. |
| `pace_per40` | numeric | Pace per40. |
| `poss` | integer | Poss. |
| `pie` | numeric | Player Impact Estimate (0-1). |

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

GET /stats/boxscoreadvancedv3

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
| `estimated_offensive_rating` | numeric | Estimated offensive rating from the stats API's estimated-metrics family. |
| `offensive_rating` | numeric | Points scored per 100 possessions while on the floor (offensive rating). |
| `estimated_defensive_rating` | numeric | Estimated defensive rating from the stats API's estimated-metrics family. |
| `defensive_rating` | numeric | Points allowed per 100 possessions while on the floor (defensive rating). |
| `estimated_net_rating` | numeric | Estimated net rating (estimated offensive minus defensive rating) from the stats API's estimated-metrics family. |
| `net_rating` | numeric | Offensive rating minus defensive rating while on the floor (net rating). |
| `assist_percentage` | numeric | Percentage of teammate field goals assisted while on the floor, as a decimal. |
| `assist_to_turnover` | numeric | Ratio of assists to turnovers. |
| `assist_ratio` | numeric | Assists per 100 possessions used (assist ratio). |
| `offensive_rebound_percentage` | numeric | Percentage of available offensive rebounds secured while on the floor, as a decimal. |
| `defensive_rebound_percentage` | numeric | Percentage of available defensive rebounds secured while on the floor, as a decimal. |
| `rebound_percentage` | numeric | Percentage of all available rebounds secured while on the floor, as a decimal. |
| `turnover_ratio` | numeric | Turnovers per 100 possessions used (turnover ratio). |
| `effective_field_goal_percentage` | numeric | Effective field goal percentage (weights made threes at 1.5), as a decimal. |
| `true_shooting_percentage` | numeric | True shooting percentage (accounts for threes and free throws), as a decimal. |
| `usage_percentage` | numeric | Percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_usage_percentage` | numeric | Estimated percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_pace` | numeric | Estimated pace (possessions per 48 minutes) from the stats API's estimated-metrics family. |
| `pace` | numeric | Possessions per 48 minutes. |
| `pace_per40` | numeric | Pace normalized to possessions per 40 minutes. |
| `possessions` | numeric | Possessions used. |
| `pie` | numeric | Player Impact Estimate (0-1). |

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
| `estimated_offensive_rating` | numeric | Estimated offensive rating from the stats API's estimated-metrics family. |
| `offensive_rating` | numeric | Points scored per 100 possessions while on the floor (offensive rating). |
| `estimated_defensive_rating` | numeric | Estimated defensive rating from the stats API's estimated-metrics family. |
| `defensive_rating` | numeric | Points allowed per 100 possessions while on the floor (defensive rating). |
| `estimated_net_rating` | numeric | Estimated net rating (estimated offensive minus defensive rating) from the stats API's estimated-metrics family. |
| `net_rating` | numeric | Offensive rating minus defensive rating while on the floor (net rating). |
| `assist_percentage` | numeric | Percentage of teammate field goals assisted while on the floor, as a decimal. |
| `assist_to_turnover` | numeric | Ratio of assists to turnovers. |
| `assist_ratio` | numeric | Assists per 100 possessions used (assist ratio). |
| `offensive_rebound_percentage` | numeric | Percentage of available offensive rebounds secured while on the floor, as a decimal. |
| `defensive_rebound_percentage` | numeric | Percentage of available defensive rebounds secured while on the floor, as a decimal. |
| `rebound_percentage` | numeric | Percentage of all available rebounds secured while on the floor, as a decimal. |
| `estimated_team_turnover_percentage` | numeric | Estimated team turnover percentage (0-1). |
| `turnover_ratio` | numeric | Turnovers per 100 possessions used (turnover ratio). |
| `effective_field_goal_percentage` | numeric | Effective field goal percentage (weights made threes at 1.5), as a decimal. |
| `true_shooting_percentage` | numeric | True shooting percentage (accounts for threes and free throws), as a decimal. |
| `usage_percentage` | numeric | Percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_usage_percentage` | numeric | Estimated percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_pace` | numeric | Estimated pace (possessions per 48 minutes) from the stats API's estimated-metrics family. |
| `pace` | numeric | Possessions per 48 minutes. |
| `pace_per40` | numeric | Pace normalized to possessions per 40 minutes. |
| `possessions` | numeric | Possessions used. |
| `pie` | numeric | Player Impact Estimate (0-1). |

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

GET /stats/boxscorefourfactorsv2

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
| `efg_pct` | numeric | Effective field goal percentage: (FGM + 0.5 * FG3M) / FGA. |
| `fta_rate` | numeric | Free throw attempt rate: free throw attempts per field goal attempt. |
| `tm_tov_pct` | numeric | Turnovers committed per 100 possessions. |
| `oreb_pct` | numeric | Percentage of available offensive rebounds grabbed while on the floor. |
| `opp_efg_pct` | numeric | Opponent effective field goal percentage while on the floor. |
| `opp_fta_rate` | numeric | Opponent free throw attempts per field goal attempt while on the floor. |
| `opp_tov_pct` | numeric | Opponent turnovers per 100 possessions while on the floor. |
| `opp_oreb_pct` | numeric | Percentage of available offensive rebounds grabbed by the opponent while on the floor. |

**sqlTeamsFourFactors**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `min` | character | Minutes played. |
| `efg_pct` | numeric | Effective field goal percentage: (FGM + 0.5 * FG3M) / FGA. |
| `fta_rate` | numeric | Free throw attempt rate: free throw attempts per field goal attempt. |
| `tm_tov_pct` | numeric | Turnovers committed per 100 possessions. |
| `oreb_pct` | numeric | Percentage of available offensive rebounds grabbed while on the floor. |
| `opp_efg_pct` | numeric | Opponent effective field goal percentage while on the floor. |
| `opp_fta_rate` | numeric | Opponent free throw attempts per field goal attempt while on the floor. |
| `opp_tov_pct` | numeric | Opponent turnovers per 100 possessions while on the floor. |
| `opp_oreb_pct` | numeric | Percentage of available offensive rebounds grabbed by the opponent while on the floor. |

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

GET /stats/boxscorefourfactorsv3

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
| `effective_field_goal_percentage` | numeric | Effective field goal percentage four-factor, as a decimal. |
| `free_throw_attempt_rate` | numeric | Free throw attempts per field goal attempt (free throw rate four-factor). |
| `team_turnover_percentage` | numeric | Turnovers committed per 100 possessions (turnover four-factor). |
| `offensive_rebound_percentage` | numeric | Offensive rebound percentage four-factor, as a decimal. |
| `opp_effective_field_goal_percentage` | numeric | Opponent's effective field goal percentage while on the floor, as a decimal. |
| `opp_free_throw_attempt_rate` | numeric | Opponent's free throw attempt rate while on the floor. |
| `opp_team_turnover_percentage` | numeric | Opponent turnovers forced per 100 possessions while on the floor. |
| `opp_offensive_rebound_percentage` | numeric | Opponent's offensive rebound percentage while on the floor, as a decimal. |

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
| `effective_field_goal_percentage` | numeric | Effective field goal percentage four-factor, as a decimal. |
| `free_throw_attempt_rate` | numeric | Free throw attempts per field goal attempt (free throw rate four-factor). |
| `team_turnover_percentage` | numeric | Turnovers committed per 100 possessions (turnover four-factor). |
| `offensive_rebound_percentage` | numeric | Offensive rebound percentage four-factor, as a decimal. |
| `opp_effective_field_goal_percentage` | numeric | Opponent's effective field goal percentage while on the floor, as a decimal. |
| `opp_free_throw_attempt_rate` | numeric | Opponent's free throw attempt rate while on the floor. |
| `opp_team_turnover_percentage` | numeric | Opponent turnovers forced per 100 possessions while on the floor. |
| `opp_offensive_rebound_percentage` | numeric | Opponent's offensive rebound percentage while on the floor, as a decimal. |

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

GET /stats/boxscorehustlev2

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
| `points` | integer | Points scored. |
| `contested_shots` | integer | Opponent shot attempts contested in the game. |
| `contested_shots2pt` | integer | Opponent two-point attempts contested. |
| `contested_shots3pt` | integer | Opponent three-point attempts contested. |
| `deflections` | integer | Defensive deflections. |
| `charges_drawn` | integer | Offensive charges drawn. |
| `screen_assists` | integer | Screens that led directly to a teammate's made field goal (screen assists). |
| `screen_assist_points` | integer | Points teammates scored directly off the player's screen assists. |
| `loose_balls_recovered_offensive` | integer | Loose balls recovered while on offense. |
| `loose_balls_recovered_defensive` | integer | Loose balls recovered while on defense. |
| `loose_balls_recovered_total` | integer | Total loose balls recovered in the game. |
| `offensive_box_outs` | integer | Box-outs recorded on the offensive glass. |
| `defensive_box_outs` | integer | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebounds` | integer | Team rebounds secured following the player's box-outs. |
| `box_out_player_rebounds` | integer | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | integer | Total box-outs recorded in the game (hustle stats tracking). |

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
| `points` | integer | Points scored. |
| `contested_shots` | integer | Opponent shot attempts contested in the game. |
| `contested_shots2pt` | integer | Opponent two-point attempts contested. |
| `contested_shots3pt` | integer | Opponent three-point attempts contested. |
| `deflections` | integer | Defensive deflections. |
| `charges_drawn` | integer | Offensive charges drawn. |
| `screen_assists` | integer | Screens that led directly to a teammate's made field goal (screen assists). |
| `screen_assist_points` | integer | Points teammates scored directly off the player's screen assists. |
| `loose_balls_recovered_offensive` | integer | Loose balls recovered while on offense. |
| `loose_balls_recovered_defensive` | integer | Loose balls recovered while on defense. |
| `loose_balls_recovered_total` | integer | Total loose balls recovered in the game. |
| `offensive_box_outs` | integer | Box-outs recorded on the offensive glass. |
| `defensive_box_outs` | integer | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebounds` | integer | Team rebounds secured following the player's box-outs. |
| `box_out_player_rebounds` | integer | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | integer | Total box-outs recorded in the game (hustle stats tracking). |

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

GET /stats/boxscorematchupsv3

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
| `game_id` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | integer | Unique identifier for the away team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |

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

GET /stats/boxscoremiscv2

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
| `pts_off_tov` | integer | Points scored following opponent turnovers. |
| `pts_2_nd_chance` | integer | Second-chance points scored after offensive rebounds. |
| `pts_fb` | integer | Fast-break points scored. |
| `pts_paint` | integer | Points scored in the paint. |
| `opp_pts_off_tov` | numeric | Opponent points scored off turnovers while on the floor. |
| `opp_pts_2_nd_chance` | numeric | Opponent second-chance points scored while on the floor. |
| `opp_pts_fb` | numeric | Opponent fast-break points scored while on the floor. |
| `opp_pts_paint` | numeric | Opponent points in the paint scored while on the floor. |
| `blk` | integer | Blocks. |
| `blka` | integer | Number of own field goal attempts that were blocked by opponents. |
| `pf` | integer | Personal fouls. |
| `pfd` | integer | Personal fouls drawn. |

**sqlTeamsMisc**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `min` | character | Minutes played. |
| `pts_off_tov` | numeric | Points scored following opponent turnovers. |
| `pts_2_nd_chance` | numeric | Second-chance points scored after offensive rebounds. |
| `pts_fb` | numeric | Fast-break points scored. |
| `pts_paint` | numeric | Points scored in the paint. |
| `opp_pts_off_tov` | numeric | Opponent points scored off turnovers while on the floor. |
| `opp_pts_2_nd_chance` | numeric | Opponent second-chance points scored while on the floor. |
| `opp_pts_fb` | numeric | Opponent fast-break points scored while on the floor. |
| `opp_pts_paint` | numeric | Opponent points in the paint scored while on the floor. |
| `blk` | integer | Blocks. |
| `blka` | integer | Number of own field goal attempts that were blocked by opponents. |
| `pf` | integer | Personal fouls. |
| `pfd` | integer | Personal fouls drawn. |

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

GET /stats/boxscoremiscv3

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
| `points_off_turnovers` | integer | Points scored off opponent turnovers. |
| `points_second_chance` | integer | Second-chance points scored after offensive rebounds. |
| `points_fast_break` | integer | Fast-break points scored. |
| `points_paint` | integer | Points scored in the paint. |
| `opp_points_off_turnovers` | integer | Opponent points off turnovers scored while on the floor. |
| `opp_points_second_chance` | integer | Opponent second-chance points scored while on the floor. |
| `opp_points_fast_break` | integer | Opponent fast-break points scored while on the floor. |
| `opp_points_paint` | integer | Opponent points in the paint scored while on the floor. |
| `blocks` | integer | Total blocks. |
| `blocks_against` | integer | Player's shot attempts that were blocked by opponents. |
| `fouls_personal` | integer | Personal fouls committed. |
| `fouls_drawn` | integer | Personal fouls drawn. |

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
| `points_off_turnovers` | integer | Points scored off opponent turnovers. |
| `points_second_chance` | integer | Second-chance points scored after offensive rebounds. |
| `points_fast_break` | integer | Fast-break points scored. |
| `points_paint` | integer | Points scored in the paint. |
| `opp_points_off_turnovers` | integer | Opponent points off turnovers scored while on the floor. |
| `opp_points_second_chance` | integer | Opponent second-chance points scored while on the floor. |
| `opp_points_fast_break` | integer | Opponent fast-break points scored while on the floor. |
| `opp_points_paint` | integer | Opponent points in the paint scored while on the floor. |
| `blocks` | integer | Total blocks. |
| `blocks_against` | integer | Player's shot attempts that were blocked by opponents. |
| `fouls_personal` | integer | Personal fouls committed. |
| `fouls_drawn` | integer | Personal fouls drawn. |

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

GET /stats/boxscoreplayertrackv3

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
| `game_id` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | integer | Unique identifier for the away team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `person_id` | integer | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `first_name` | character | Player's first name. |
| `family_name` | character | Display name for familyname associated with this NBA or WNBA Stats row. |
| `name_i` | character | NBA or WNBA Stats value for namei in the boxscoreplayertrackv3 result set. |
| `player_slug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | character | NBA or WNBA Stats value for jerseynum in the boxscoreplayertrackv3 result set. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `speed` | numeric | Speed. |
| `distance` | numeric | Distance value (in feet for shot data; otherwise context-dependent). |
| `rebound_chances_offensive` | integer | Rebounding metric for reboundchancesoffensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_defensive` | integer | Rebounding metric for reboundchancesdefensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_total` | integer | Rebounding metric for reboundchancestotal in the requested NBA or WNBA Stats split. |
| `touches` | integer | Touches. |
| `secondary_assists` | integer | Passing or assist metric for secondaryassists in the requested NBA or WNBA Stats split. |
| `free_throw_assists` | integer | Shooting metric for freethrowassists in the requested NBA or WNBA Stats split. |
| `passes` | integer | Passes. |
| `assists` | integer | Total assists. |
| `contested_field_goals_made` | integer | Shooting metric for contestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `contested_field_goals_attempted` | integer | Shooting metric for contestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `contested_field_goal_percentage` | numeric | Percentage or rate for contestedfieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_made` | integer | Shooting metric for uncontestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_attempted` | integer | Shooting metric for uncontestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_percentage` | numeric | Percentage or rate for uncontestedfieldgoalspercentage in the requested NBA or WNBA Stats split. |
| `field_goal_percentage` | numeric | Percentage or rate for fieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goals_made` | integer | Shooting metric for defendedatrimfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goals_attempted` | integer | Shooting metric for defendedatrimfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goal_percentage` | numeric | Percentage or rate for defendedatrimfieldgoalpercentage in the requested NBA or WNBA Stats split. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | integer | Unique identifier for the away team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `distance` | numeric | Distance value (in feet for shot data; otherwise context-dependent). |
| `rebound_chances_offensive` | integer | Rebounding metric for reboundchancesoffensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_defensive` | integer | Rebounding metric for reboundchancesdefensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_total` | integer | Rebounding metric for reboundchancestotal in the requested NBA or WNBA Stats split. |
| `touches` | integer | Touches. |
| `secondary_assists` | integer | Passing or assist metric for secondaryassists in the requested NBA or WNBA Stats split. |
| `free_throw_assists` | integer | Shooting metric for freethrowassists in the requested NBA or WNBA Stats split. |
| `passes` | integer | Passes. |
| `assists` | integer | Total assists. |
| `contested_field_goals_made` | integer | Shooting metric for contestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `contested_field_goals_attempted` | integer | Shooting metric for contestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `contested_field_goal_percentage` | numeric | Percentage or rate for contestedfieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_made` | integer | Shooting metric for uncontestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_attempted` | integer | Shooting metric for uncontestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `uncontested_field_goals_percentage` | numeric | Percentage or rate for uncontestedfieldgoalspercentage in the requested NBA or WNBA Stats split. |
| `field_goal_percentage` | numeric | Percentage or rate for fieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goals_made` | integer | Shooting metric for defendedatrimfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goals_attempted` | integer | Shooting metric for defendedatrimfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `defended_at_rim_field_goal_percentage` | numeric | Percentage or rate for defendedatrimfieldgoalpercentage in the requested NBA or WNBA Stats split. |

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

GET /stats/boxscorescoringv2

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
| `pct_fga_2_pt` | numeric | Share of field goal attempts taken as two-pointers. |
| `pct_fga_3_pt` | numeric | Share of field goal attempts taken as three-pointers. |
| `pct_pts_2_pt` | numeric | Share of points scored on two-point field goals. |
| `pct_pts_2_pt_mr` | numeric | Share of points scored on mid-range two-point field goals. |
| `pct_pts_3_pt` | numeric | Share of points scored on three-point field goals. |
| `pct_pts_fb` | numeric | Share of points scored on the fast break. |
| `pct_pts_ft` | numeric | Share of points scored on free throws. |
| `pct_pts_off_tov` | numeric | Share of points scored off opponent turnovers. |
| `pct_pts_paint` | numeric | Share of points scored in the paint. |
| `pct_ast_2_pm` | numeric | Share of made two-point field goals that were assisted. |
| `pct_uast_2_pm` | numeric | Share of made two-point field goals that were unassisted. |
| `pct_ast_3_pm` | numeric | Share of made three-point field goals that were assisted. |
| `pct_uast_3_pm` | numeric | Share of made three-point field goals that were unassisted. |
| `pct_ast_fgm` | numeric | Share of all made field goals that were assisted. |
| `pct_uast_fgm` | numeric | Share of all made field goals that were unassisted. |

**sqlTeamsScoring**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `min` | character | Minutes played. |
| `pct_fga_2_pt` | numeric | Share of field goal attempts taken as two-pointers. |
| `pct_fga_3_pt` | numeric | Share of field goal attempts taken as three-pointers. |
| `pct_pts_2_pt` | numeric | Share of points scored on two-point field goals. |
| `pct_pts_2_pt_mr` | numeric | Share of points scored on mid-range two-point field goals. |
| `pct_pts_3_pt` | numeric | Share of points scored on three-point field goals. |
| `pct_pts_fb` | numeric | Share of points scored on the fast break. |
| `pct_pts_ft` | numeric | Share of points scored on free throws. |
| `pct_pts_off_tov` | numeric | Share of points scored off opponent turnovers. |
| `pct_pts_paint` | numeric | Share of points scored in the paint. |
| `pct_ast_2_pm` | numeric | Share of made two-point field goals that were assisted. |
| `pct_uast_2_pm` | numeric | Share of made two-point field goals that were unassisted. |
| `pct_ast_3_pm` | numeric | Share of made three-point field goals that were assisted. |
| `pct_uast_3_pm` | numeric | Share of made three-point field goals that were unassisted. |
| `pct_ast_fgm` | numeric | Share of all made field goals that were assisted. |
| `pct_uast_fgm` | numeric | Share of all made field goals that were unassisted. |

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

GET /stats/boxscorescoringv3

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
| `percentage_field_goals_attempted2pt` | numeric | Share of field goal attempts taken as two-pointers, as a decimal. |
| `percentage_field_goals_attempted3pt` | numeric | Share of field goal attempts taken as three-pointers, as a decimal. |
| `percentage_points2pt` | numeric | Share of points scored on two-pointers, as a decimal. |
| `percentage_points_midrange2pt` | numeric | Share of points scored on mid-range two-pointers, as a decimal. |
| `percentage_points3pt` | numeric | Share of points scored on three-pointers, as a decimal. |
| `percentage_points_fast_break` | numeric | Share of points scored on fast breaks, as a decimal. |
| `percentage_points_free_throw` | numeric | Share of points scored at the free throw line, as a decimal. |
| `percentage_points_off_turnovers` | numeric | Share of points scored off opponent turnovers, as a decimal. |
| `percentage_points_paint` | numeric | Share of points scored in the paint, as a decimal. |
| `percentage_assisted2pt` | numeric | Percentage of made two-pointers that were assisted, as a decimal. |
| `percentage_unassisted2pt` | numeric | Percentage of made two-pointers that were unassisted, as a decimal. |
| `percentage_assisted3pt` | numeric | Percentage of made three-pointers that were assisted, as a decimal. |
| `percentage_unassisted3pt` | numeric | Percentage of made three-pointers that were unassisted, as a decimal. |
| `percentage_assisted_fgm` | numeric | Percentage of made field goals that were assisted, as a decimal. |
| `percentage_unassisted_fgm` | numeric | Percentage of made field goals that were unassisted, as a decimal. |

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
| `percentage_field_goals_attempted2pt` | numeric | Share of field goal attempts taken as two-pointers, as a decimal. |
| `percentage_field_goals_attempted3pt` | numeric | Share of field goal attempts taken as three-pointers, as a decimal. |
| `percentage_points2pt` | numeric | Share of points scored on two-pointers, as a decimal. |
| `percentage_points_midrange2pt` | numeric | Share of points scored on mid-range two-pointers, as a decimal. |
| `percentage_points3pt` | numeric | Share of points scored on three-pointers, as a decimal. |
| `percentage_points_fast_break` | numeric | Share of points scored on fast breaks, as a decimal. |
| `percentage_points_free_throw` | numeric | Share of points scored at the free throw line, as a decimal. |
| `percentage_points_off_turnovers` | numeric | Share of points scored off opponent turnovers, as a decimal. |
| `percentage_points_paint` | numeric | Share of points scored in the paint, as a decimal. |
| `percentage_assisted2pt` | numeric | Percentage of made two-pointers that were assisted, as a decimal. |
| `percentage_unassisted2pt` | numeric | Percentage of made two-pointers that were unassisted, as a decimal. |
| `percentage_assisted3pt` | numeric | Percentage of made three-pointers that were assisted, as a decimal. |
| `percentage_unassisted3pt` | numeric | Percentage of made three-pointers that were unassisted, as a decimal. |
| `percentage_assisted_fgm` | numeric | Percentage of made field goals that were assisted, as a decimal. |
| `percentage_unassisted_fgm` | numeric | Percentage of made field goals that were unassisted, as a decimal. |

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
