---
title: "NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoreadvancedv3–boxscoretraditionalv3"
sidebar_label: "Box scores: boxscoreadvancedv3–boxscoretraditionalv3"
sidebar_position: 1
description: "NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoreadvancedv3–boxscoretraditionalv3 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Box scores: boxscoreadvancedv3–boxscoretraditionalv3

## nba_stats_boxscoreadvancedv3

GET /stats/boxscoreadvancedv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreadvancedv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreadvancedv3](https://stats.nba.com/stats/boxscoreadvancedv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoreadvancedv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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

### Example {#nba_stats_boxscoreadvancedv3-example}

```python
nba_stats_boxscoreadvancedv3()
```

_Last validated n/a._

## nba_stats_boxscoredefensivev2

GET /stats/boxscoredefensivev2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoredefensivev2`

**Valid URL:** [https://stats.nba.com/stats/boxscoredefensivev2](https://stats.nba.com/stats/boxscoredefensivev2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoredefensivev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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
| `name_i` | character | NBA or WNBA Stats value for namei in the boxscoredefensivev2 result set. |
| `player_slug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | character | NBA or WNBA Stats value for jerseynum in the boxscoredefensivev2 result set. |
| `matchup_minutes` | character | NBA or WNBA Stats value for matchupminutes in the boxscoredefensivev2 result set. |
| `partial_possessions` | numeric | Estimated partial possessions credited to the stint or rotation interval. |
| `switches_on` | integer | NBA or WNBA Stats value for switcheson in the boxscoredefensivev2 result set. |
| `player_points` | integer | Scoring or score-margin metric for playerpoints in the requested NBA or WNBA Stats split. |
| `defensive_rebounds` | integer | Rebounding metric for defensiverebounds in the requested NBA or WNBA Stats split. |
| `matchup_assists` | integer | Passing or assist metric for matchupassists in the requested NBA or WNBA Stats split. |
| `matchup_turnovers` | integer | Turnover or loose-ball metric for matchupturnovers in the requested NBA or WNBA Stats split. |
| `steals` | integer | Total steals. |
| `blocks` | integer | Total blocks. |
| `matchup_field_goals_made` | integer | Shooting metric for matchupfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `matchup_field_goals_attempted` | integer | Shooting metric for matchupfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `matchup_field_goal_percentage` | numeric | Percentage or rate for matchupfieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `matchup_three_pointers_made` | integer | Shooting metric for matchupthreepointersmade in the requested NBA or WNBA Stats split. |
| `matchup_three_pointers_attempted` | integer | Shooting metric for matchupthreepointersattempted in the requested NBA or WNBA Stats split. |
| `matchup_three_pointer_percentage` | numeric | Percentage or rate for matchupthreepointerpercentage in the requested NBA or WNBA Stats split. |

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_boxscoredefensivev2-example}

```python
nba_stats_boxscoredefensivev2()
```

_Last validated n/a._

## nba_stats_boxscorefourfactorsv3

GET /stats/boxscorefourfactorsv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorefourfactorsv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorefourfactorsv3](https://stats.nba.com/stats/boxscorefourfactorsv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscorefourfactorsv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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

### Example {#nba_stats_boxscorefourfactorsv3-example}

```python
nba_stats_boxscorefourfactorsv3()
```

_Last validated n/a._

## nba_stats_boxscorehustlev2

GET /stats/boxscorehustlev2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorehustlev2`

**Valid URL:** [https://stats.nba.com/stats/boxscorehustlev2](https://stats.nba.com/stats/boxscorehustlev2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscorehustlev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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

### Example {#nba_stats_boxscorehustlev2-example}

```python
nba_stats_boxscorehustlev2()
```

_Last validated n/a._

## nba_stats_boxscorematchupsv3

GET /stats/boxscorematchupsv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorematchupsv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorematchupsv3](https://stats.nba.com/stats/boxscorematchupsv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscorematchupsv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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
| `person_id` | integer | NBA or WNBA Stats player identifier associated with the matchup boxscore row. |
| `first_name` | character | Player's first name. |
| `family_name` | character | Player's family name in the NBA or WNBA Stats matchup boxscore row. |
| `name_i` | character | Abbreviated player display name used in the NBA or WNBA Stats matchup boxscore row. |
| `player_slug` | character | URL slug for the player on NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | character | Player jersey number shown in the NBA or WNBA Stats matchup boxscore row. |
| `matchups` | character |  |

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

### Example {#nba_stats_boxscorematchupsv3-example}

```python
nba_stats_boxscorematchupsv3()
```

_Last validated n/a._

## nba_stats_boxscoremiscv3

GET /stats/boxscoremiscv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoremiscv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoremiscv3](https://stats.nba.com/stats/boxscoremiscv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoremiscv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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

### Example {#nba_stats_boxscoremiscv3-example}

```python
nba_stats_boxscoremiscv3()
```

_Last validated n/a._

## nba_stats_boxscoreplayertrackv3

GET /stats/boxscoreplayertrackv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreplayertrackv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreplayertrackv3](https://stats.nba.com/stats/boxscoreplayertrackv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoreplayertrackv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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

### Example {#nba_stats_boxscoreplayertrackv3-example}

```python
nba_stats_boxscoreplayertrackv3()
```

_Last validated n/a._

## nba_stats_boxscorescoringv3

GET /stats/boxscorescoringv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorescoringv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorescoringv3](https://stats.nba.com/stats/boxscorescoringv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscorescoringv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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

### Example {#nba_stats_boxscorescoringv3-example}

```python
nba_stats_boxscorescoringv3()
```

_Last validated n/a._

## nba_stats_boxscoresummaryv2

GET /stats/boxscoresummaryv2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoresummaryv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoresummaryv2](https://stats.nba.com/stats/boxscoresummaryv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoresummaryv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameSummary`, `OtherStats`, `Officials`, `InactivePlayers`, `GameInfo`, `LineScore`, `LastMeeting`, `SeasonSeries`, `AvailableVideo`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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
| `season` | character | Season year. |
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

### Example {#nba_stats_boxscoresummaryv2-example}

```python
nba_stats_boxscoresummaryv2()
```

_Last validated n/a._

## nba_stats_boxscoresummaryv3

GET /stats/boxscoresummaryv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoresummaryv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoresummaryv3](https://stats.nba.com/stats/boxscoresummaryv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoresummaryv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`, `Officials`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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

### Example {#nba_stats_boxscoresummaryv3-example}

```python
nba_stats_boxscoresummaryv3()
```

_Last validated n/a._

## nba_stats_boxscoretraditionalv2

GET /stats/boxscoretraditionalv2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoretraditionalv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoretraditionalv2](https://stats.nba.com/stats/boxscoretraditionalv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoretraditionalv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`, `TeamStarterBenchStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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
| `reb` | integer | Rebounds per game. |
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
| `reb` | integer | Rebounds per game. |
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
| `reb` | integer | Rebounds per game. |
| `ast` | integer | Assists. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `to` | integer | To. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_boxscoretraditionalv2-example}

```python
nba_stats_boxscoretraditionalv2()
```

_Last validated n/a._

## nba_stats_boxscoretraditionalv3

GET /stats/boxscoretraditionalv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoretraditionalv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoretraditionalv3](https://stats.nba.com/stats/boxscoretraditionalv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns {#nba_stats_boxscoretraditionalv3-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayerStats`, `TeamStats`) (representative columns below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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
