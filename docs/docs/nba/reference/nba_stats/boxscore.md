---
title: "NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoreadvancedv3–boxscoresummaryv3"
sidebar_label: "Box scores: boxscoreadvancedv3–boxscoresummaryv3"
sidebar_position: 1
description: "NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoreadvancedv3–boxscoresummaryv3 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoreadvancedv3–boxscoresummaryv3

## nba_stats_boxscoreadvancedv3

Box score advanced v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreadvancedv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreadvancedv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoreadvancedv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoreadvancedv3-returns}

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
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace normalized to possessions per 40 minutes. |
| `possessions` | Float64 | Possessions used. |
| `pie` | Float64 | Player Impact Estimate (0-1). |

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
| `estimated_team_turnover_percentage` | Float64 | Estimated team turnover percentage (0-1). |
| `turnover_ratio` | Float64 | Turnovers per 100 possessions used (turnover ratio). |
| `effective_field_goal_percentage` | Float64 | Effective field goal percentage (weights made threes at 1.5), as a decimal. |
| `true_shooting_percentage` | Float64 | True shooting percentage (accounts for threes and free throws), as a decimal. |
| `usage_percentage` | Float64 | Percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_usage_percentage` | Float64 | Estimated percentage of team plays used by the player while on the floor, as a decimal. |
| `estimated_pace` | Float64 | Estimated pace (possessions per 48 minutes) from the stats API's estimated-metrics family. |
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace normalized to possessions per 40 minutes. |
| `possessions` | Float64 | Possessions used. |
| `pie` | Float64 | Player Impact Estimate (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_boxscoreadvancedv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscoreadvancedv3-example}

```python
nba_stats_boxscoreadvancedv3()
```

## nba_stats_boxscoredefensivev2

Box score defensive v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoredefensivev2`

**Valid URL:** [https://stats.nba.com/stats/boxscoredefensivev2?GameID=0022200021](https://stats.nba.com/stats/boxscoredefensivev2?GameID=0022200021)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoredefensivev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `person_id` | Int64 | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `first_name` | String | Player's first name. |
| `family_name` | String | Display name for familyname associated with this NBA or WNBA Stats row. |
| `name_i` | String | NBA or WNBA Stats value for namei in the boxscoredefensivev2 result set. |
| `player_slug` | String | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | String | NBA or WNBA Stats value for jerseynum in the boxscoredefensivev2 result set. |
| `matchup_minutes` | String | NBA or WNBA Stats value for matchupminutes in the boxscoredefensivev2 result set. |
| `partial_possessions` | Float64 | Estimated partial possessions credited to the stint or rotation interval. |
| `switches_on` | Int64 | NBA or WNBA Stats value for switcheson in the boxscoredefensivev2 result set. |
| `player_points` | Int64 | Scoring or score-margin metric for playerpoints in the requested NBA or WNBA Stats split. |
| `defensive_rebounds` | Int64 | Rebounding metric for defensiverebounds in the requested NBA or WNBA Stats split. |
| `matchup_assists` | Int64 | Passing or assist metric for matchupassists in the requested NBA or WNBA Stats split. |
| `matchup_turnovers` | Int64 | Turnover or loose-ball metric for matchupturnovers in the requested NBA or WNBA Stats split. |
| `steals` | Int64 | Total steals. |
| `blocks` | Int64 | Total blocks. |
| `matchup_field_goals_made` | Int64 | Shooting metric for matchupfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `matchup_field_goals_attempted` | Int64 | Shooting metric for matchupfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `matchup_field_goal_percentage` | Float64 | Percentage or rate for matchupfieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `matchup_three_pointers_made` | Int64 | Shooting metric for matchupthreepointersmade in the requested NBA or WNBA Stats split. |
| `matchup_three_pointers_attempted` | Int64 | Shooting metric for matchupthreepointersattempted in the requested NBA or WNBA Stats split. |
| `matchup_three_pointer_percentage` | Float64 | Percentage or rate for matchupthreepointerpercentage in the requested NBA or WNBA Stats split. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_boxscoredefensivev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscoredefensivev2-example}

```python
nba_stats_boxscoredefensivev2()
```

## nba_stats_boxscorefourfactorsv3

Box score four factors v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorefourfactorsv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorefourfactorsv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscorefourfactorsv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscorefourfactorsv3-returns}

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
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `effective_field_goal_percentage` | Float64 | Effective field goal percentage four-factor, as a decimal. |
| `free_throw_attempt_rate` | Float64 | Free throw attempts per field goal attempt (free throw rate four-factor). |
| `team_turnover_percentage` | Float64 | Turnovers committed per 100 possessions (turnover four-factor). |
| `offensive_rebound_percentage` | Float64 | Offensive rebound percentage four-factor, as a decimal. |
| `opp_effective_field_goal_percentage` | Float64 | Opponent's effective field goal percentage while on the floor, as a decimal. |
| `opp_free_throw_attempt_rate` | Float64 | Opponent's free throw attempt rate while on the floor. |
| `opp_team_turnover_percentage` | Float64 | Opponent turnovers forced per 100 possessions while on the floor. |
| `opp_offensive_rebound_percentage` | Float64 | Opponent's offensive rebound percentage while on the floor, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_boxscorefourfactorsv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscorefourfactorsv3-example}

```python
nba_stats_boxscorefourfactorsv3()
```

## nba_stats_boxscorehustlev2

Box score hustle v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorehustlev2`

**Valid URL:** [https://stats.nba.com/stats/boxscorehustlev2?GameID=0022200021](https://stats.nba.com/stats/boxscorehustlev2?GameID=0022200021)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscorehustlev2-returns}

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
| `points` | Int64 | Points scored. |
| `contested_shots` | Int64 | Opponent shot attempts contested in the game. |
| `contested_shots2pt` | Int64 | Opponent two-point attempts contested. |
| `contested_shots3pt` | Int64 | Opponent three-point attempts contested. |
| `deflections` | Int64 | Defensive deflections. |
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
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `points` | Int64 | Points scored. |
| `contested_shots` | Int64 | Opponent shot attempts contested in the game. |
| `contested_shots2pt` | Int64 | Opponent two-point attempts contested. |
| `contested_shots3pt` | Int64 | Opponent three-point attempts contested. |
| `deflections` | Int64 | Defensive deflections. |
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

### Raises {#nba_stats_boxscorehustlev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscorehustlev2-example}

```python
nba_stats_boxscorehustlev2()
```

## nba_stats_boxscorematchupsv3

Box score matchups v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorematchupsv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorematchupsv3?GameID=0022200021](https://stats.nba.com/stats/boxscorematchupsv3?GameID=0022200021)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscorematchupsv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `person_id` | Int64 | NBA or WNBA Stats player identifier associated with the matchup boxscore row. |
| `first_name` | String | Player's first name. |
| `family_name` | String | Player's family name in the NBA or WNBA Stats matchup boxscore row. |
| `name_i` | String | Abbreviated player display name used in the NBA or WNBA Stats matchup boxscore row. |
| `player_slug` | String | URL slug for the player on NBA or WNBA Stats pages. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | String | Player jersey number shown in the NBA or WNBA Stats matchup boxscore row. |
| `matchups` | String |  |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_boxscorematchupsv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscorematchupsv3-example}

```python
nba_stats_boxscorematchupsv3()
```

## nba_stats_boxscoremiscv3

Box score misc v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoremiscv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoremiscv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscoremiscv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoremiscv3-returns}

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
| `points_off_turnovers` | Int64 | Points scored off opponent turnovers. |
| `points_second_chance` | Int64 | Second-chance points scored after offensive rebounds. |
| `points_fast_break` | Int64 | Fast-break points scored. |
| `points_paint` | Int64 | Points scored in the paint. |
| `opp_points_off_turnovers` | Int64 | Opponent points off turnovers scored while on the floor. |
| `opp_points_second_chance` | Int64 | Opponent second-chance points scored while on the floor. |
| `opp_points_fast_break` | Int64 | Opponent fast-break points scored while on the floor. |
| `opp_points_paint` | Int64 | Opponent points in the paint scored while on the floor. |
| `blocks` | Int64 | Total blocks. |
| `blocks_against` | Int64 | Player's shot attempts that were blocked by opponents. |
| `fouls_personal` | Int64 | Personal fouls committed. |
| `fouls_drawn` | Int64 | Personal fouls drawn. |

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
| `points_off_turnovers` | Int64 | Points scored off opponent turnovers. |
| `points_second_chance` | Int64 | Second-chance points scored after offensive rebounds. |
| `points_fast_break` | Int64 | Fast-break points scored. |
| `points_paint` | Int64 | Points scored in the paint. |
| `opp_points_off_turnovers` | Int64 | Opponent points off turnovers scored while on the floor. |
| `opp_points_second_chance` | Int64 | Opponent second-chance points scored while on the floor. |
| `opp_points_fast_break` | Int64 | Opponent fast-break points scored while on the floor. |
| `opp_points_paint` | Int64 | Opponent points in the paint scored while on the floor. |
| `blocks` | Int64 | Total blocks. |
| `blocks_against` | Int64 | Player's shot attempts that were blocked by opponents. |
| `fouls_personal` | Int64 | Personal fouls committed. |
| `fouls_drawn` | Int64 | Personal fouls drawn. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_boxscoremiscv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscoremiscv3-example}

```python
nba_stats_boxscoremiscv3()
```

## nba_stats_boxscoreplayertrackv3

Box score player track v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreplayertrackv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreplayertrackv3?GameID=0022200021](https://stats.nba.com/stats/boxscoreplayertrackv3?GameID=0022200021)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoreplayertrackv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `person_id` | Int64 | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `first_name` | String | Player's first name. |
| `family_name` | String | Display name for familyname associated with this NBA or WNBA Stats row. |
| `name_i` | String | NBA or WNBA Stats value for namei in the boxscoreplayertrackv3 result set. |
| `player_slug` | String | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | String | NBA or WNBA Stats value for jerseynum in the boxscoreplayertrackv3 result set. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `speed` | Float64 | Speed. |
| `distance` | Float64 | Distance value (in feet for shot data; otherwise context-dependent). |
| `rebound_chances_offensive` | Int64 | Rebounding metric for reboundchancesoffensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_defensive` | Int64 | Rebounding metric for reboundchancesdefensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_total` | Int64 | Rebounding metric for reboundchancestotal in the requested NBA or WNBA Stats split. |
| `touches` | Int64 | Touches. |
| `secondary_assists` | Int64 | Passing or assist metric for secondaryassists in the requested NBA or WNBA Stats split. |
| `free_throw_assists` | Int64 | Shooting metric for freethrowassists in the requested NBA or WNBA Stats split. |
| `passes` | Int64 | Passes. |
| `assists` | Int64 | Total assists. |
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
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `distance` | Float64 | Distance value (in feet for shot data; otherwise context-dependent). |
| `rebound_chances_offensive` | Int64 | Rebounding metric for reboundchancesoffensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_defensive` | Int64 | Rebounding metric for reboundchancesdefensive in the requested NBA or WNBA Stats split. |
| `rebound_chances_total` | Int64 | Rebounding metric for reboundchancestotal in the requested NBA or WNBA Stats split. |
| `touches` | Int64 | Touches. |
| `secondary_assists` | Int64 | Passing or assist metric for secondaryassists in the requested NBA or WNBA Stats split. |
| `free_throw_assists` | Int64 | Shooting metric for freethrowassists in the requested NBA or WNBA Stats split. |
| `passes` | Int64 | Passes. |
| `assists` | Int64 | Total assists. |
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

### Raises {#nba_stats_boxscoreplayertrackv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscoreplayertrackv3-example}

```python
nba_stats_boxscoreplayertrackv3()
```

## nba_stats_boxscorescoringv3

Box score scoring v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorescoringv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorescoringv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0](https://stats.nba.com/stats/boxscorescoringv3?EndPeriod=14&EndRange=0&GameID=0022200021&RangeType=0&StartPeriod=0&StartRange=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscorescoringv3-returns}

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
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team abbreviation. |
| `team_slug` | String | URL-friendly slug for the team name. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
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

### Raises {#nba_stats_boxscorescoringv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscorescoringv3-example}

```python
nba_stats_boxscorescoringv3()
```

## nba_stats_boxscoresummaryv2

Box score summary v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoresummaryv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoresummaryv2?GameID=0022200021](https://stats.nba.com/stats/boxscoresummaryv2?GameID=0022200021)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoresummaryv2-returns}

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
| `season` | String | Season year. |
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

### Raises {#nba_stats_boxscoresummaryv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscoresummaryv2-example}

```python
nba_stats_boxscoresummaryv2()
```

## nba_stats_boxscoresummaryv3

Box score summary v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoresummaryv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoresummaryv3?GameID=0022200021](https://stats.nba.com/stats/boxscoresummaryv3?GameID=0022200021)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoresummaryv3-returns}

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

### Raises {#nba_stats_boxscoresummaryv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_boxscoresummaryv3-example}

```python
nba_stats_boxscoresummaryv3()
```
