---
title: WNBA — WNBA Stats API (stats.wnba.com)
sidebar_label: WNBA Stats API (stats.wnba.com)
description: "WNBA — WNBA Stats API (stats.wnba.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
---
# WNBA — WNBA Stats API (stats.wnba.com)

`sportsdataverse.wnba` — 111 endpoints.

## `wnba_stats_alltimeleadersgrids`

GET /stats/alltimeleadersgrids

**Endpoint URL:** `GET https://stats.wnba.com/stats/alltimeleadersgrids`

**Valid URL:** [https://stats.wnba.com/stats/alltimeleadersgrids?LeagueID=10](https://stats.wnba.com/stats/alltimeleadersgrids?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `TopX` | `topx` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `tov` | numeric | Turnovers. |
| `tov_rank` | integer | All-time league rank of the player's career turnover total on the leaders grid. |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_alltimeleadersgrids(league_id='10')
```

_Last validated n/a._

## `wnba_stats_assistleaders`

GET /stats/assistleaders

**Endpoint URL:** `GET https://stats.wnba.com/stats/assistleaders`

**Valid URL:** [https://stats.wnba.com/stats/assistleaders?LeagueID=10](https://stats.wnba.com/stats/assistleaders?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `ast` | numeric | Assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_assistleaders(league_id='10')
```

_Last validated n/a._

## `wnba_stats_assisttracker`

GET /stats/assisttracker

**Endpoint URL:** `GET https://stats.wnba.com/stats/assisttracker`

**Valid URL:** [https://stats.wnba.com/stats/assisttracker?LeagueID=10](https://stats.wnba.com/stats/assisttracker?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | numeric | Total assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_assisttracker(league_id='10')
```

_Last validated n/a._

## `wnba_stats_boxscoreadvancedv2`

GET /stats/boxscoreadvancedv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoreadvancedv2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoreadvancedv2](https://stats.wnba.com/stats/boxscoreadvancedv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoreadvancedv2()
```

_Last validated n/a._

## `wnba_stats_boxscoreadvancedv3`

GET /stats/boxscoreadvancedv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoreadvancedv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscoreadvancedv3](https://stats.wnba.com/stats/boxscoreadvancedv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `pie` | numeric | Player Impact Estimate (0-1). |
| `assistpercentage` | numeric | Percentage of teammate field goals assisted while on the floor, as a decimal. |
| `assistratio` | numeric | Assists per 100 possessions used (assist ratio). |
| `assisttoturnover` | numeric | Ratio of assists to turnovers. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `defensiverating` | numeric | Points allowed per 100 possessions while on the floor (defensive rating). |
| `defensivereboundpercentage` | numeric | Percentage of available defensive rebounds secured while on the floor, as a decimal. |
| `effectivefieldgoalpercentage` | numeric | Effective field goal percentage (weights made threes at 1.5), as a decimal. |
| `estimateddefensiverating` | numeric | Estimated defensive rating from the stats API's estimated-metrics family. |
| `estimatednetrating` | numeric | Estimated net rating (estimated offensive minus defensive rating) from the stats API's estimated-metrics family. |
| `estimatedoffensiverating` | numeric | Estimated offensive rating from the stats API's estimated-metrics family. |
| `estimatedpace` | numeric | Estimated pace (possessions per 48 minutes) from the stats API's estimated-metrics family. |
| `estimatedusagepercentage` | numeric | Estimated percentage of team plays used by the player while on the floor, as a decimal. |
| `familyname` | character | Player's family (last) name. |
| `firstname` | character | Firstname. |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `jerseynum` | character | Player's jersey number. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `namei` | character | Abbreviated player name (first initial and last name). |
| `netrating` | numeric | Offensive rating minus defensive rating while on the floor (net rating). |
| `offensiverating` | numeric | Points scored per 100 possessions while on the floor (offensive rating). |
| `offensivereboundpercentage` | numeric | Percentage of available offensive rebounds secured while on the floor, as a decimal. |
| `pace` | numeric | Possessions per 48 minutes. |
| `paceper40` | numeric | Pace normalized to possessions per 40 minutes. |
| `personid` | integer | Player identifier from the league's stats API. |
| `playerslug` | character | URL-friendly slug for the player's name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `possessions` | numeric | Possessions used. |
| `reboundpercentage` | numeric | Percentage of all available rebounds secured while on the floor, as a decimal. |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL-friendly slug for the team name. |
| `teamtricode` | character | Three-letter team abbreviation. |
| `trueshootingpercentage` | numeric | True shooting percentage (accounts for threes and free throws), as a decimal. |
| `turnoverratio` | numeric | Turnovers per 100 possessions used (turnover ratio). |
| `usagepercentage` | numeric | Percentage of team plays used by the player while on the floor, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoreadvancedv3()
```

_Last validated n/a._

## `wnba_stats_boxscoredefensivev2`

GET /stats/boxscoredefensivev2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoredefensivev2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoredefensivev2](https://stats.wnba.com/stats/boxscoredefensivev2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `blocks` | integer | Total blocks. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `defensiverebounds` | integer | Rebounding metric for defensiverebounds in the requested NBA or WNBA Stats split. |
| `familyname` | character | Display name for familyname associated with this NBA or WNBA Stats row. |
| `firstname` | character | Firstname. |
| `gameid` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `jerseynum` | character | NBA or WNBA Stats value for jerseynum in the boxscoredefensivev2 result set. |
| `matchupassists` | integer | Passing or assist metric for matchupassists in the requested NBA or WNBA Stats split. |
| `matchupfieldgoalpercentage` | numeric | Percentage or rate for matchupfieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `matchupfieldgoalsattempted` | integer | Shooting metric for matchupfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `matchupfieldgoalsmade` | integer | Shooting metric for matchupfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `matchupminutes` | character | NBA or WNBA Stats value for matchupminutes in the boxscoredefensivev2 result set. |
| `matchupthreepointerpercentage` | numeric | Percentage or rate for matchupthreepointerpercentage in the requested NBA or WNBA Stats split. |
| `matchupthreepointersattempted` | integer | Shooting metric for matchupthreepointersattempted in the requested NBA or WNBA Stats split. |
| `matchupthreepointersmade` | integer | Shooting metric for matchupthreepointersmade in the requested NBA or WNBA Stats split. |
| `matchupturnovers` | integer | Turnover or loose-ball metric for matchupturnovers in the requested NBA or WNBA Stats split. |
| `namei` | character | NBA or WNBA Stats value for namei in the boxscoredefensivev2 result set. |
| `partialpossessions` | numeric | Estimated partial possessions credited to the stint or rotation interval. |
| `personid` | integer | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `playerpoints` | integer | Scoring or score-margin metric for playerpoints in the requested NBA or WNBA Stats split. |
| `playerslug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `steals` | integer | Total steals. |
| `switcheson` | integer | NBA or WNBA Stats value for switcheson in the boxscoredefensivev2 result set. |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `teamtricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoredefensivev2()
```

_Last validated n/a._

## `wnba_stats_boxscorefourfactorsv2`

GET /stats/boxscorefourfactorsv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscorefourfactorsv2`

**Valid URL:** [https://stats.wnba.com/stats/boxscorefourfactorsv2](https://stats.wnba.com/stats/boxscorefourfactorsv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscorefourfactorsv2()
```

_Last validated n/a._

## `wnba_stats_boxscorefourfactorsv3`

GET /stats/boxscorefourfactorsv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscorefourfactorsv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscorefourfactorsv3](https://stats.wnba.com/stats/boxscorefourfactorsv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `effectivefieldgoalpercentage` | numeric | Effective field goal percentage four-factor, as a decimal. |
| `familyname` | character | Player's family (last) name. |
| `firstname` | character | Firstname. |
| `freethrowattemptrate` | numeric | Free throw attempts per field goal attempt (free throw rate four-factor). |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `jerseynum` | character | Player's jersey number. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `namei` | character | Abbreviated player name (first initial and last name). |
| `offensivereboundpercentage` | numeric | Offensive rebound percentage four-factor, as a decimal. |
| `oppeffectivefieldgoalpercentage` | numeric | Opponent's effective field goal percentage while on the floor, as a decimal. |
| `oppfreethrowattemptrate` | numeric | Opponent's free throw attempt rate while on the floor. |
| `oppoffensivereboundpercentage` | numeric | Opponent's offensive rebound percentage while on the floor, as a decimal. |
| `oppteamturnoverpercentage` | numeric | Opponent turnovers forced per 100 possessions while on the floor. |
| `personid` | integer | Player identifier from the league's stats API. |
| `playerslug` | character | URL-friendly slug for the player's name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL-friendly slug for the team name. |
| `teamtricode` | character | Three-letter team abbreviation. |
| `teamturnoverpercentage` | numeric | Turnovers committed per 100 possessions (turnover four-factor). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscorefourfactorsv3()
```

_Last validated n/a._

## `wnba_stats_boxscorehustlev2`

GET /stats/boxscorehustlev2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscorehustlev2`

**Valid URL:** [https://stats.wnba.com/stats/boxscorehustlev2](https://stats.wnba.com/stats/boxscorehustlev2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `gameid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `boxoutplayerrebounds` | integer | Rebounds the player secured directly off their own box-outs. |
| `boxoutplayerteamrebounds` | integer | Team rebounds secured following the player's box-outs. |
| `boxouts` | integer | Total box-outs recorded in the game (hustle stats tracking). |
| `chargesdrawn` | integer | Offensive charges drawn. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `contestedshots` | integer | Opponent shot attempts contested in the game. |
| `contestedshots2pt` | integer | Opponent two-point attempts contested. |
| `contestedshots3pt` | integer | Opponent three-point attempts contested. |
| `defensiveboxouts` | integer | Box-outs recorded on the defensive glass. |
| `deflections` | integer | Defensive deflections. |
| `familyname` | character | Player's family (last) name. |
| `firstname` | character | Firstname. |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `jerseynum` | character | Player's jersey number. |
| `looseballsrecovereddefensive` | integer | Loose balls recovered while on defense. |
| `looseballsrecoveredoffensive` | integer | Loose balls recovered while on offense. |
| `looseballsrecoveredtotal` | integer | Total loose balls recovered in the game. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `namei` | character | Abbreviated player name (first initial and last name). |
| `offensiveboxouts` | integer | Box-outs recorded on the offensive glass. |
| `personid` | integer | Player identifier from the league's stats API. |
| `playerslug` | character | URL-friendly slug for the player's name. |
| `points` | integer | Points scored. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `screenassistpoints` | integer | Points teammates scored directly off the player's screen assists. |
| `screenassists` | integer | Screens that led directly to a teammate's made field goal (screen assists). |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL-friendly slug for the team name. |
| `teamtricode` | character | Three-letter team abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscorehustlev2()
```

_Last validated n/a._

## `wnba_stats_boxscorematchupsv3`

GET /stats/boxscorematchupsv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscorematchupsv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscorematchupsv3](https://stats.wnba.com/stats/boxscorematchupsv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `familyname` | character | Player's family name in the NBA or WNBA Stats matchup boxscore row. |
| `firstname` | character | Firstname. |
| `gameid` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `jerseynum` | character | Player jersey number shown in the NBA or WNBA Stats matchup boxscore row. |
| `namei` | character | Abbreviated player display name used in the NBA or WNBA Stats matchup boxscore row. |
| `personid` | integer | NBA or WNBA Stats player identifier associated with the matchup boxscore row. |
| `playerslug` | character | URL slug for the player on NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `teamtricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscorematchupsv3()
```

_Last validated n/a._

## `wnba_stats_boxscoremiscv2`

GET /stats/boxscoremiscv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoremiscv2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoremiscv2](https://stats.wnba.com/stats/boxscoremiscv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `pts_2nd_chance` | integer | Second-chance points scored after offensive rebounds. |
| `pts_fb` | integer | Fast-break points scored. |
| `pts_paint` | integer | Points scored in the paint. |
| `opp_pts_off_tov` | numeric | Opponent points scored off turnovers while on the floor. |
| `opp_pts_2nd_chance` | numeric | Opponent second-chance points scored while on the floor. |
| `opp_pts_fb` | numeric | Opponent fast-break points scored while on the floor. |
| `opp_pts_paint` | numeric | Opponent points in the paint scored while on the floor. |
| `blk` | integer | Blocks. |
| `blka` | integer | Number of own field goal attempts that were blocked by opponents. |
| `pf` | integer | Personal fouls. |
| `pfd` | integer | Personal fouls drawn. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoremiscv2()
```

_Last validated n/a._

## `wnba_stats_boxscoremiscv3`

GET /stats/boxscoremiscv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoremiscv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscoremiscv3](https://stats.wnba.com/stats/boxscoremiscv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `blocks` | integer | Total blocks. |
| `blocksagainst` | integer | Player's shot attempts that were blocked by opponents. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `familyname` | character | Player's family (last) name. |
| `firstname` | character | Firstname. |
| `foulsdrawn` | integer | Personal fouls drawn. |
| `foulspersonal` | integer | Personal fouls committed. |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `jerseynum` | character | Player's jersey number. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `namei` | character | Abbreviated player name (first initial and last name). |
| `opppointsfastbreak` | integer | Opponent fast-break points scored while on the floor. |
| `opppointsoffturnovers` | integer | Opponent points off turnovers scored while on the floor. |
| `opppointspaint` | integer | Opponent points in the paint scored while on the floor. |
| `opppointssecondchance` | integer | Opponent second-chance points scored while on the floor. |
| `personid` | integer | Player identifier from the league's stats API. |
| `playerslug` | character | URL-friendly slug for the player's name. |
| `pointsfastbreak` | integer | Fast-break points scored. |
| `pointsoffturnovers` | integer | Points scored off opponent turnovers. |
| `pointspaint` | integer | Points scored in the paint. |
| `pointssecondchance` | integer | Second-chance points scored after offensive rebounds. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL-friendly slug for the team name. |
| `teamtricode` | character | Three-letter team abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoremiscv3()
```

_Last validated n/a._

## `wnba_stats_boxscoreplayertrackv3`

GET /stats/boxscoreplayertrackv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoreplayertrackv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscoreplayertrackv3](https://stats.wnba.com/stats/boxscoreplayertrackv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | integer | Total assists. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `contestedfieldgoalpercentage` | numeric | Percentage or rate for contestedfieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `contestedfieldgoalsattempted` | integer | Shooting metric for contestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `contestedfieldgoalsmade` | integer | Shooting metric for contestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `defendedatrimfieldgoalpercentage` | numeric | Percentage or rate for defendedatrimfieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `defendedatrimfieldgoalsattempted` | integer | Shooting metric for defendedatrimfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `defendedatrimfieldgoalsmade` | integer | Shooting metric for defendedatrimfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `distance` | numeric | Distance value (in feet for shot data; otherwise context-dependent). |
| `familyname` | character | Display name for familyname associated with this NBA or WNBA Stats row. |
| `fieldgoalpercentage` | numeric | Percentage or rate for fieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `firstname` | character | Firstname. |
| `freethrowassists` | integer | Shooting metric for freethrowassists in the requested NBA or WNBA Stats split. |
| `gameid` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `jerseynum` | character | NBA or WNBA Stats value for jerseynum in the boxscoreplayertrackv3 result set. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `namei` | character | NBA or WNBA Stats value for namei in the boxscoreplayertrackv3 result set. |
| `passes` | integer | Passes. |
| `personid` | integer | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `playerslug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `reboundchancesdefensive` | integer | Rebounding metric for reboundchancesdefensive in the requested NBA or WNBA Stats split. |
| `reboundchancesoffensive` | integer | Rebounding metric for reboundchancesoffensive in the requested NBA or WNBA Stats split. |
| `reboundchancestotal` | integer | Rebounding metric for reboundchancestotal in the requested NBA or WNBA Stats split. |
| `secondaryassists` | integer | Passing or assist metric for secondaryassists in the requested NBA or WNBA Stats split. |
| `speed` | numeric | Speed. |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `teamtricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `touches` | integer | Touches. |
| `uncontestedfieldgoalsattempted` | integer | Shooting metric for uncontestedfieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `uncontestedfieldgoalsmade` | integer | Shooting metric for uncontestedfieldgoalsmade in the requested NBA or WNBA Stats split. |
| `uncontestedfieldgoalspercentage` | numeric | Percentage or rate for uncontestedfieldgoalspercentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoreplayertrackv3()
```

_Last validated n/a._

## `wnba_stats_boxscorescoringv2`

GET /stats/boxscorescoringv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscorescoringv2`

**Valid URL:** [https://stats.wnba.com/stats/boxscorescoringv2](https://stats.wnba.com/stats/boxscorescoringv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `pct_fga_2pt` | numeric | Share of field goal attempts taken as two-pointers. |
| `pct_fga_3pt` | numeric | Share of field goal attempts taken as three-pointers. |
| `pct_pts_2pt` | numeric | Share of points scored on two-point field goals. |
| `pct_pts_2pt_mr` | numeric | Share of points scored on mid-range two-point field goals. |
| `pct_pts_3pt` | numeric | Share of points scored on three-point field goals. |
| `pct_pts_fb` | numeric | Share of points scored on the fast break. |
| `pct_pts_ft` | numeric | Share of points scored on free throws. |
| `pct_pts_off_tov` | numeric | Share of points scored off opponent turnovers. |
| `pct_pts_paint` | numeric | Share of points scored in the paint. |
| `pct_ast_2pm` | numeric | Share of made two-point field goals that were assisted. |
| `pct_uast_2pm` | numeric | Share of made two-point field goals that were unassisted. |
| `pct_ast_3pm` | numeric | Share of made three-point field goals that were assisted. |
| `pct_uast_3pm` | numeric | Share of made three-point field goals that were unassisted. |
| `pct_ast_fgm` | numeric | Share of all made field goals that were assisted. |
| `pct_uast_fgm` | numeric | Share of all made field goals that were unassisted. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscorescoringv2()
```

_Last validated n/a._

## `wnba_stats_boxscorescoringv3`

GET /stats/boxscorescoringv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscorescoringv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscorescoringv3](https://stats.wnba.com/stats/boxscorescoringv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `familyname` | character | Player's family (last) name. |
| `firstname` | character | Firstname. |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `jerseynum` | character | Player's jersey number. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `namei` | character | Abbreviated player name (first initial and last name). |
| `percentageassisted2pt` | numeric | Percentage of made two-pointers that were assisted, as a decimal. |
| `percentageassisted3pt` | numeric | Percentage of made three-pointers that were assisted, as a decimal. |
| `percentageassistedfgm` | numeric | Percentage of made field goals that were assisted, as a decimal. |
| `percentagefieldgoalsattempted2pt` | numeric | Share of field goal attempts taken as two-pointers, as a decimal. |
| `percentagefieldgoalsattempted3pt` | numeric | Share of field goal attempts taken as three-pointers, as a decimal. |
| `percentagepoints2pt` | numeric | Share of points scored on two-pointers, as a decimal. |
| `percentagepoints3pt` | numeric | Share of points scored on three-pointers, as a decimal. |
| `percentagepointsfastbreak` | numeric | Share of points scored on fast breaks, as a decimal. |
| `percentagepointsfreethrow` | numeric | Share of points scored at the free throw line, as a decimal. |
| `percentagepointsmidrange2pt` | numeric | Share of points scored on mid-range two-pointers, as a decimal. |
| `percentagepointsoffturnovers` | numeric | Share of points scored off opponent turnovers, as a decimal. |
| `percentagepointspaint` | numeric | Share of points scored in the paint, as a decimal. |
| `percentageunassisted2pt` | numeric | Percentage of made two-pointers that were unassisted, as a decimal. |
| `percentageunassisted3pt` | numeric | Percentage of made three-pointers that were unassisted, as a decimal. |
| `percentageunassistedfgm` | numeric | Percentage of made field goals that were unassisted, as a decimal. |
| `personid` | integer | Player identifier from the league's stats API. |
| `playerslug` | character | URL-friendly slug for the player's name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL-friendly slug for the team name. |
| `teamtricode` | character | Three-letter team abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscorescoringv3()
```

_Last validated n/a._

## `wnba_stats_boxscoresummaryv2`

GET /stats/boxscoresummaryv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoresummaryv2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoresummaryv2](https://stats.wnba.com/stats/boxscoresummaryv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoresummaryv2()
```

_Last validated n/a._

## `wnba_stats_boxscoresummaryv3`

GET /stats/boxscoresummaryv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoresummaryv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscoresummaryv3](https://stats.wnba.com/stats/boxscoresummaryv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `gameid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `dummykey` | character | Placeholder key emitted by the stats API's box score summary payload; carries no data. |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `inbonus` | character | Whether the team is currently in the bonus (penalty) foul situation, as reported by the stats API. |
| `score` | integer | Final score. |
| `seed` | integer | Team's playoff seed, populated for postseason games. |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamlosses` | integer | Team's loss total entering the game. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL-friendly slug for the team name. |
| `teamtricode` | character | Three-letter team abbreviation. |
| `teamwins` | integer | Team's win total entering the game. |
| `timeoutsremaining` | integer | Timeouts the team has remaining. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoresummaryv3()
```

_Last validated n/a._

## `wnba_stats_boxscoretraditionalv2`

GET /stats/boxscoretraditionalv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoretraditionalv2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoretraditionalv2](https://stats.wnba.com/stats/boxscoretraditionalv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `fg3m` | integer | Three-point field goals made. |
| `fg3a` | integer | Three-point field goal attempts. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoretraditionalv2()
```

_Last validated n/a._

## `wnba_stats_boxscoretraditionalv3`

GET /stats/boxscoretraditionalv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoretraditionalv3`

**Valid URL:** [https://stats.wnba.com/stats/boxscoretraditionalv3](https://stats.wnba.com/stats/boxscoretraditionalv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | integer | Total assists. |
| `bench_assists` | integer | Total assists by the team's bench players. |
| `bench_blocks` | integer | Total blocked shots by the team's bench players. |
| `bench_fieldgoalsattempted` | integer | Total field goal attempts by the team's bench players. |
| `bench_fieldgoalsmade` | integer | Total field goals made by the team's bench players. |
| `bench_fieldgoalspercentage` | numeric | Combined field goal percentage of the team's bench players. |
| `bench_foulspersonal` | integer | Total personal fouls by the team's bench players. |
| `bench_freethrowsattempted` | integer | Total free throw attempts by the team's bench players. |
| `bench_freethrowsmade` | integer | Total free throws made by the team's bench players. |
| `bench_freethrowspercentage` | numeric | Combined free throw percentage of the team's bench players. |
| `bench_minutes` | character | Total minutes played by the team's bench players. |
| `bench_points` | integer | Points scored by the bench. |
| `bench_reboundsdefensive` | integer | Total defensive rebounds by the team's bench players. |
| `bench_reboundsoffensive` | integer | Total offensive rebounds by the team's bench players. |
| `bench_reboundstotal` | integer | Total rebounds by the team's bench players. |
| `bench_steals` | integer | Total steals by the team's bench players. |
| `bench_threepointersattempted` | integer | Total three-point attempts by the team's bench players. |
| `bench_threepointersmade` | integer | Total three-pointers made by the team's bench players. |
| `bench_threepointerspercentage` | numeric | Combined three-point percentage of the team's bench players. |
| `bench_turnovers` | integer | Total turnovers by the team's bench players. |
| `blocks` | integer | Total blocks. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `familyname` | character | Player's family (last) name. |
| `fieldgoalsattempted` | integer | Field goal attempts recorded in the game. |
| `fieldgoalsmade` | integer | Field goals made recorded in the game. |
| `fieldgoalspercentage` | numeric | Field goal percentage for the game, as a decimal. |
| `firstname` | character | Firstname. |
| `foulspersonal` | integer | Personal fouls recorded in the game. |
| `freethrowsattempted` | integer | Free throw attempts recorded in the game. |
| `freethrowsmade` | integer | Free throws made recorded in the game. |
| `freethrowspercentage` | numeric | Free throw percentage for the game, as a decimal. |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `jerseynum` | character | Player's jersey number. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `namei` | character | Abbreviated player name (first initial and last name). |
| `personid` | integer | Player identifier from the league's stats API. |
| `playerslug` | character | URL-friendly slug for the player's name. |
| `plusminuspoints` | numeric | Team point differential while the player was on the floor (plus-minus). |
| `points` | integer | Points scored. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `reboundsdefensive` | integer | Defensive rebounds recorded in the game. |
| `reboundsoffensive` | integer | Offensive rebounds recorded in the game. |
| `reboundstotal` | integer | Total rebounds recorded in the game. |
| `starters_assists` | integer | Total assists by the team's starters. |
| `starters_blocks` | integer | Total blocked shots by the team's starters. |
| `starters_fieldgoalsattempted` | integer | Total field goal attempts by the team's starters. |
| `starters_fieldgoalsmade` | integer | Total field goals made by the team's starters. |
| `starters_fieldgoalspercentage` | numeric | Combined field goal percentage of the team's starters. |
| `starters_foulspersonal` | integer | Total personal fouls by the team's starters. |
| `starters_freethrowsattempted` | integer | Total free throw attempts by the team's starters. |
| `starters_freethrowsmade` | integer | Total free throws made by the team's starters. |
| `starters_freethrowspercentage` | numeric | Combined free throw percentage of the team's starters. |
| `starters_minutes` | character | Total minutes played by the team's starters. |
| `starters_points` | integer | Total points by the team's starters. |
| `starters_reboundsdefensive` | integer | Total defensive rebounds by the team's starters. |
| `starters_reboundsoffensive` | integer | Total offensive rebounds by the team's starters. |
| `starters_reboundstotal` | integer | Total rebounds by the team's starters. |
| `starters_steals` | integer | Total steals by the team's starters. |
| `starters_threepointersattempted` | integer | Total three-point attempts by the team's starters. |
| `starters_threepointersmade` | integer | Total three-pointers made by the team's starters. |
| `starters_threepointerspercentage` | numeric | Combined three-point percentage of the team's starters. |
| `starters_turnovers` | integer | Total turnovers by the team's starters. |
| `steals` | integer | Total steals. |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL-friendly slug for the team name. |
| `teamtricode` | character | Three-letter team abbreviation. |
| `threepointersattempted` | integer | Three-point attempts recorded in the game. |
| `threepointersmade` | integer | Three-pointers made recorded in the game. |
| `threepointerspercentage` | numeric | Three-point percentage for the game, as a decimal. |
| `turnovers` | integer | Total turnovers. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoretraditionalv3()
```

_Last validated n/a._

## `wnba_stats_boxscoreusagev2`

GET /stats/boxscoreusagev2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoreusagev2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoreusagev2](https://stats.wnba.com/stats/boxscoreusagev2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `pct_fg3m` | numeric | Share of the team's made three-pointers accounted for while on the floor. |
| `pct_fg3a` | numeric | Share of the team's three-point attempts accounted for while on the floor. |
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

### Example

```python
wnba_stats_boxscoreusagev2()
```

_Last validated n/a._

## `wnba_stats_boxscoreusagev3`

GET /stats/boxscoreusagev3

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoreusagev3`

**Valid URL:** [https://stats.wnba.com/stats/boxscoreusagev3](https://stats.wnba.com/stats/boxscoreusagev3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `EndRange` | `end_range` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `RangeType` | `range_type` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |
| `StartRange` | `start_range` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `familyname` | character | Player's family (last) name. |
| `firstname` | character | Firstname. |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `jerseynum` | character | Player's jersey number. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `namei` | character | Abbreviated player name (first initial and last name). |
| `percentageassists` | numeric | Share of the team's assists accounted for by the player while on the floor, as a decimal. |
| `percentageblocks` | numeric | Share of the team's blocked shots accounted for by the player while on the floor, as a decimal. |
| `percentageblocksallowed` | numeric | Share of the team's shot attempts blocked by opponents accounted for by the player while on the floor, as a decimal. |
| `percentagefieldgoalsattempted` | numeric | Share of the team's field goal attempts accounted for by the player while on the floor, as a decimal. |
| `percentagefieldgoalsmade` | numeric | Share of the team's field goals made accounted for by the player while on the floor, as a decimal. |
| `percentagefreethrowsattempted` | numeric | Share of the team's free throw attempts accounted for by the player while on the floor, as a decimal. |
| `percentagefreethrowsmade` | numeric | Share of the team's free throws made accounted for by the player while on the floor, as a decimal. |
| `percentagepersonalfouls` | numeric | Share of the team's personal fouls accounted for by the player while on the floor, as a decimal. |
| `percentagepersonalfoulsdrawn` | numeric | Share of the team's personal fouls drawn accounted for by the player while on the floor, as a decimal. |
| `percentagepoints` | numeric | Share of the team's points accounted for by the player while on the floor, as a decimal. |
| `percentagereboundsdefensive` | numeric | Share of the team's defensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentagereboundsoffensive` | numeric | Share of the team's offensive rebounds accounted for by the player while on the floor, as a decimal. |
| `percentagereboundstotal` | numeric | Share of the team's total rebounds accounted for by the player while on the floor, as a decimal. |
| `percentagesteals` | numeric | Share of the team's steals accounted for by the player while on the floor, as a decimal. |
| `percentagethreepointersattempted` | numeric | Share of the team's three-point attempts accounted for by the player while on the floor, as a decimal. |
| `percentagethreepointersmade` | numeric | Share of the team's three-pointers made accounted for by the player while on the floor, as a decimal. |
| `percentageturnovers` | numeric | Share of the team's turnovers accounted for by the player while on the floor, as a decimal. |
| `personid` | integer | Player identifier from the league's stats API. |
| `playerslug` | character | URL-friendly slug for the player's name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `teamcity` | character | Teamcity. |
| `teamid` | integer | Teamid. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL-friendly slug for the team name. |
| `teamtricode` | character | Three-letter team abbreviation. |
| `usagepercentage` | numeric | Percentage of team plays used by the player while on the floor, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoreusagev3()
```

_Last validated n/a._

## `wnba_stats_commonallplayers`

GET /stats/commonallplayers

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonallplayers`

**Valid URL:** [https://stats.wnba.com/stats/commonallplayers?LeagueID=10](https://stats.wnba.com/stats/commonallplayers?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `IsOnlyCurrentSeason` | `is_only_current_season` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `display_last_comma_first` | character | Player name formatted as "Last, First". |
| `display_first_last` | character | Player name formatted as "First Last". |
| `rosterstatus` | integer | Roster status flag (1 = currently on a roster, 0 = not). |
| `from_year` | character | First season. |
| `to_year` | character | Most recent season. |
| `playercode` | character | URL-style player code slug used by the league's legacy stats pages. |
| `player_slug` | character | URL-safe player identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_code` | character | Internal team code. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `games_played_flag` | character | Y/N flag for whether the player has appeared in a league game. |
| `otherleague_experience_ch` | character | Code for the player's experience in another league (e.g. G League), as reported by the stats API. |
| `is_nba_assigned` | integer | Flag indicating whether the player is currently on an NBA roster assignment (two-way and G League assignment tracking). |
| `nba_assigned_team_id` | integer | Team identifier of the NBA team the player is assigned to, when on assignment. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_commonallplayers(league_id='10')
```

_Last validated n/a._

## `wnba_stats_commonplayerinfo`

GET /stats/commonplayerinfo

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonplayerinfo`

**Valid URL:** [https://stats.wnba.com/stats/commonplayerinfo?LeagueID=10](https://stats.wnba.com/stats/commonplayerinfo?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `display_first_last` | character | NBA or WNBA Stats value for display first last in the commonplayerinfo result set. |
| `display_last_comma_first` | character | NBA or WNBA Stats value for display last comma first in the commonplayerinfo result set. |
| `display_fi_last` | character | NBA or WNBA Stats value for display fi last in the commonplayerinfo result set. |
| `player_slug` | character | URL-safe player identifier. |
| `birthdate` | character | Date of birth. |
| `school` | character | Player's school / college (when distinct from 'college'). |
| `country` | character | Country (full name or code). |
| `last_affiliation` | character | NBA or WNBA Stats value for last affiliation in the commonplayerinfo result set. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `season_exp` | integer | NBA or WNBA Stats value for season exp in the commonplayerinfo result set. |
| `jersey` | character | Jersey number worn by the player. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `rosterstatus` | character | NBA or WNBA Stats value for rosterstatus in the commonplayerinfo result set. |
| `games_played_current_season_flag` | character | Flag indicating games played current season flag for the requested NBA or WNBA Stats context. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_code` | character | Internal team code. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `playercode` | character | NBA or WNBA Stats value for playercode in the commonplayerinfo result set. |
| `from_year` | integer | First season. |
| `to_year` | integer | Most recent season. |
| `dleague_flag` | character | Flag indicating dleague flag for the requested NBA or WNBA Stats context. |
| `nba_flag` | character | Flag indicating NBA flag for the requested NBA or WNBA Stats context. |
| `games_played_flag` | character | Flag indicating games played flag for the requested NBA or WNBA Stats context. |
| `draft_year` | character | Draft year (4-digit). |
| `draft_round` | character | Round of the draft selection. |
| `draft_number` | character | The number pick that was used to select a given player. |
| `greatest_75_flag` | character | Flag indicating greatest 75 flag for the requested NBA or WNBA Stats context. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_commonplayerinfo(league_id='10')
```

_Last validated n/a._

## `wnba_stats_commonplayoffseries`

GET /stats/commonplayoffseries

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonplayoffseries`

**Valid URL:** [https://stats.wnba.com/stats/commonplayoffseries?LeagueID=10](https://stats.wnba.com/stats/commonplayoffseries?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeriesID` | `series_id_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `series_id` | character | Series identifier (e.g. 'W_1'). |
| `game_num` | integer | NBA or WNBA Stats value for game number in the commonplayoffseries result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_commonplayoffseries(league_id='10')
```

_Last validated n/a._

## `wnba_stats_commonteamroster`

GET /stats/commonteamroster

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonteamroster`

**Valid URL:** [https://stats.wnba.com/stats/commonteamroster?LeagueID=10](https://stats.wnba.com/stats/commonteamroster?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `teamid` | integer | Teamid. |
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `leagueid` | character | League identifier from the stats API ("00" = NBA, "10" = WNBA, "20" = G League). |
| `player` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `player_slug` | character | URL-safe player identifier. |
| `num` | character | Inning number. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `age` | numeric | Player age (in years). |
| `exp` | character | Exp. |
| `school` | character | Player's school / college (when distinct from 'college'). |
| `player_id` | integer | Unique player identifier. |
| `how_acquired` | character | How the team acquired the player (e.g. draft, trade, free agency). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_commonteamroster(league_id='10')
```

_Last validated n/a._

## `wnba_stats_commonteamyears`

GET /stats/commonteamyears

**Endpoint URL:** `GET https://stats.wnba.com/stats/commonteamyears`

**Valid URL:** [https://stats.wnba.com/stats/commonteamyears?LeagueID=10](https://stats.wnba.com/stats/commonteamyears?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `min_year` | character | Minimum year queried (echoes `min_year`). |
| `max_year` | character | Maximum year queried (echoes `max_year`). |
| `abbreviation` | character | Short abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_commonteamyears(league_id='10')
```

_Last validated n/a._

## `wnba_stats_cumestatsplayer`

GET /stats/cumestatsplayer

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsplayer`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsplayer?LeagueID=10](https://stats.wnba.com/stats/cumestatsplayer?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameIDs` | `game_ids` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `display_fi_last` | character | Abbreviated player name (first initial and last name). |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `jersey_num` | character | Jersey number worn by the player. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `actual_minutes` | integer | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | integer | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | integer | Field goals made over the aggregated games. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3` | integer | Three-point field goals made over the aggregated games. |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft` | integer | Free throws made over the aggregated games. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `off_reb` | integer | Offensive rebounds over the aggregated games. |
| `def_reb` | integer | Defensive rebounds over the aggregated games. |
| `tot_reb` | integer | Total rebounds over the aggregated games. |
| `ast` | integer | Assists. |
| `pf` | integer | Personal fouls. |
| `dq` | integer | Disqualifications (fouled out) over the aggregated games. |
| `stl` | integer | Steals. |
| `turnovers` | integer | Total turnovers. |
| `blk` | integer | Blocks. |
| `pts` | integer | Points scored. |
| `max_actual_minutes` | integer | Most whole minutes played in any single aggregated game. |
| `max_actual_seconds` | integer | Seconds component paired with the single-game maximum minutes. |
| `max_reb` | integer | Most rebounds recorded in any single aggregated game. |
| `max_ast` | integer | Most assists recorded in any single aggregated game. |
| `max_stl` | integer | Most steals recorded in any single aggregated game. |
| `max_turnovers` | integer | Most turnovers recorded in any single aggregated game. |
| `max_blk` | integer | Most blocked shots recorded in any single aggregated game. |
| `max_pts` | integer | Most points recorded in any single aggregated game. |
| `avg_actual_minutes` | integer | Average whole minutes played per aggregated game. |
| `avg_actual_seconds` | numeric | Average seconds component of playing time per aggregated game. |
| `avg_tot_reb` | numeric | Average total rebounds per game over the aggregated games. |
| `avg_ast` | numeric | Average assists per game over the aggregated games. |
| `avg_stl` | numeric | Average steals per game over the aggregated games. |
| `avg_turnovers` | numeric | Average turnovers per game over the aggregated games. |
| `avg_blk` | numeric | Average blocked shots per game over the aggregated games. |
| `avg_pts` | numeric | Average points per game over the aggregated games. |
| `per_min_tot_reb` | numeric | Total rebounds per minute played over the aggregated games. |
| `per_min_ast` | numeric | Assists per minute played over the aggregated games. |
| `per_min_stl` | numeric | Steals per minute played over the aggregated games. |
| `per_min_turnovers` | numeric | Turnovers per minute played over the aggregated games. |
| `per_min_blk` | numeric | Blocked shots per minute played over the aggregated games. |
| `per_min_pts` | numeric | Points per minute played over the aggregated games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_cumestatsplayer(league_id='10')
```

_Last validated n/a._

## `wnba_stats_cumestatsplayergames`

GET /stats/cumestatsplayergames

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsplayergames`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsplayergames?LeagueID=10](https://stats.wnba.com/stats/cumestatsplayergames?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `matchup` | character | Matchup. |
| `game_id` | character | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_cumestatsplayergames(league_id='10')
```

_Last validated n/a._

## `wnba_stats_cumestatsteam`

GET /stats/cumestatsteam

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsteam`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsteam?LeagueID=10](https://stats.wnba.com/stats/cumestatsteam?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameIDs` | `game_ids` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `jersey_num` | character | Jersey number worn by the player. |
| `player` | character | Player name. |
| `person_id` | character | Unique player identifier (V3 endpoints). |
| `team_id` | character | Unique team identifier. |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
| `actual_minutes` | character | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | character | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | character | Field goals made over the aggregated games. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |
| `fg3` | character | Three-point field goals made over the aggregated games. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | character | Three-point field goal percentage (0-1). |
| `ft` | character | Free throws made over the aggregated games. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | character | Free throw percentage (0-1). |
| `off_reb` | character | Offensive rebounds over the aggregated games. |
| `def_reb` | character | Defensive rebounds over the aggregated games. |
| `tot_reb` | character | Total rebounds over the aggregated games. |
| `ast` | character | Assists. |
| `pf` | character | Personal fouls. |
| `dq` | character | Disqualifications (fouled out) over the aggregated games. |
| `stl` | character | Steals. |
| `turnovers` | character | Total turnovers. |
| `blk` | character | Blocks. |
| `pts` | character | Points scored. |
| `max_actual_minutes` | character | Most whole minutes played in any single aggregated game. |
| `max_actual_seconds` | character | Seconds component paired with the single-game maximum minutes. |
| `max_reb` | character | Most rebounds recorded in any single aggregated game. |
| `max_ast` | character | Most assists recorded in any single aggregated game. |
| `max_stl` | character | Most steals recorded in any single aggregated game. |
| `max_turnovers` | character | Most turnovers recorded in any single aggregated game. |
| `max_blkp` | character | Most blocked shots recorded in any single aggregated game. |
| `max_pts` | character | Most points recorded in any single aggregated game. |
| `avg_actual_minutes` | character | Average whole minutes played per aggregated game. |
| `avg_actual_seconds` | character | Average seconds component of playing time per aggregated game. |
| `avg_reb` | character | Average rebounds per game over the aggregated games. |
| `avg_ast` | character | Average assists per game over the aggregated games. |
| `avg_stl` | character | Average steals per game over the aggregated games. |
| `avg_turnovers` | character | Average turnovers per game over the aggregated games. |
| `avg_blkp` | character | Average blocked shots per game over the aggregated games. |
| `avg_pts` | character | Average points per game over the aggregated games. |
| `per_min_reb` | character | Rebounds per minute played over the aggregated games. |
| `per_min_ast` | character | Assists per minute played over the aggregated games. |
| `per_min_stl` | character | Steals per minute played over the aggregated games. |
| `per_min_turnovers` | character | Turnovers per minute played over the aggregated games. |
| `per_min_blk` | character | Blocked shots per minute played over the aggregated games. |
| `per_min_pts` | character | Points per minute played over the aggregated games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_cumestatsteam(league_id='10')
```

_Last validated n/a._

## `wnba_stats_cumestatsteamgames`

GET /stats/cumestatsteamgames

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsteamgames`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsteamgames?LeagueID=10](https://stats.wnba.com/stats/cumestatsteamgames?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonID` | `season_id_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `matchup` | character | Matchup. |
| `game_id` | character | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_cumestatsteamgames(league_id='10')
```

_Last validated n/a._

## `wnba_stats_draftcombinestats`

GET /stats/draftcombinestats

**Endpoint URL:** `GET https://stats.wnba.com/stats/draftcombinestats`

**Valid URL:** [https://stats.wnba.com/stats/draftcombinestats?LeagueID=10](https://stats.wnba.com/stats/draftcombinestats?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_all_time` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height_wo_shoes` | numeric | Height measured without shoes, in inches. |
| `height_wo_shoes_ft_in` | character | Height without shoes formatted as feet and inches. |
| `height_w_shoes` | character | Height measured with shoes, in inches. |
| `height_w_shoes_ft_in` | character | Height with shoes formatted as feet and inches. |
| `weight` | character | Player weight in pounds. |
| `wingspan` | numeric | Wingspan measured at the combine, in inches. |
| `wingspan_ft_in` | character | Wingspan formatted as feet and inches. |
| `standing_reach` | numeric | Standing reach measured at the combine, in inches. |
| `standing_reach_ft_in` | character | Standing reach formatted as feet and inches. |
| `body_fat_pct` | character | Body fat percentage measured at the combine. |
| `hand_length` | numeric | Hand length measured at the combine, in inches. |
| `hand_width` | numeric | Hand width measured at the combine, in inches. |
| `standing_vertical_leap` | numeric | Standing (no-step) vertical leap, in inches. |
| `max_vertical_leap` | numeric | Maximum (running) vertical leap, in inches. |
| `lane_agility_time` | numeric | Lane agility drill time, in seconds. |
| `modified_lane_agility_time` | numeric | Modified (shuttle) lane agility drill time, in seconds. |
| `three_quarter_sprint` | numeric | Three-quarter-court sprint time, in seconds. |
| `bench_press` | character | Repetitions of 185 pounds completed on the bench press. |
| `spot_fifteen_corner_left` | character | Made-attempted result (e.g. "3-5") from the 15-foot left corner spot-up shooting station at the combine. |
| `spot_fifteen_break_left` | character | Made-attempted result (e.g. "3-5") from the 15-foot left wing (break) spot-up shooting station at the combine. |
| `spot_fifteen_top_key` | character | Made-attempted result (e.g. "3-5") from the 15-foot top of the key spot-up shooting station at the combine. |
| `spot_fifteen_break_right` | character | Made-attempted result (e.g. "3-5") from the 15-foot right wing (break) spot-up shooting station at the combine. |
| `spot_fifteen_corner_right` | character | Made-attempted result (e.g. "3-5") from the 15-foot right corner spot-up shooting station at the combine. |
| `spot_college_corner_left` | character | Made-attempted result (e.g. "3-5") from the college three-point left corner spot-up shooting station at the combine. |
| `spot_college_break_left` | character | Made-attempted result (e.g. "3-5") from the college three-point left wing (break) spot-up shooting station at the combine. |
| `spot_college_top_key` | character | Made-attempted result (e.g. "3-5") from the college three-point top of the key spot-up shooting station at the combine. |
| `spot_college_break_right` | character | Made-attempted result (e.g. "3-5") from the college three-point right wing (break) spot-up shooting station at the combine. |
| `spot_college_corner_right` | character | Made-attempted result (e.g. "3-5") from the college three-point right corner spot-up shooting station at the combine. |
| `spot_nba_corner_left` | character | Made-attempted result (e.g. "3-5") from the NBA three-point left corner spot-up shooting station at the combine. |
| `spot_nba_break_left` | character | Made-attempted result (e.g. "3-5") from the NBA three-point left wing (break) spot-up shooting station at the combine. |
| `spot_nba_top_key` | character | Made-attempted result (e.g. "3-5") from the NBA three-point top of the key spot-up shooting station at the combine. |
| `spot_nba_break_right` | character | Made-attempted result (e.g. "3-5") from the NBA three-point right wing (break) spot-up shooting station at the combine. |
| `spot_nba_corner_right` | character | Made-attempted result (e.g. "3-5") from the NBA three-point right corner spot-up shooting station at the combine. |
| `off_drib_fifteen_break_left` | character | Made-attempted result from the 15-foot left wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_fifteen_top_key` | character | Made-attempted result from the 15-foot top of the key off-the-dribble shooting station at the combine. |
| `off_drib_fifteen_break_right` | character | Made-attempted result from the 15-foot right wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_college_break_left` | character | Made-attempted result from the college three-point left wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_college_top_key` | character | Made-attempted result from the college three-point top of the key off-the-dribble shooting station at the combine. |
| `off_drib_college_break_right` | character | Made-attempted result from the college three-point right wing (break) off-the-dribble shooting station at the combine. |
| `on_move_fifteen` | character | Made-attempted result from the 15-foot shooting-on-the-move station at the combine. |
| `on_move_college` | character | Made-attempted result from the college three-point shooting-on-the-move station at the combine. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_draftcombinestats(league_id='10')
```

_Last validated n/a._

## `wnba_stats_drafthistory`

GET /stats/drafthistory

**Endpoint URL:** `GET https://stats.wnba.com/stats/drafthistory`

**Valid URL:** [https://stats.wnba.com/stats/drafthistory?LeagueID=10&Season=2024](https://stats.wnba.com/stats/drafthistory?LeagueID=10&Season=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `OverallPick` | `overall_pick_nullable` |  |  | `Y` |  |
| `RoundNum` | `round_num_nullable` |  |  | `Y` |  |
| `RoundPick` | `round_pick_nullable` |  |  | `Y` |  |
| `Season` | `season_year_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TopX` | `topx_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_name` | character | Player name. |
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `round_number` | integer | Numeric round. |
| `round_pick` | integer | Round pick. |
| `overall_pick` | integer | Overall pick. |
| `draft_type` | character | NBA or WNBA Stats value for draft type in the drafthistory result set. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `organization` | character | Organization. |
| `organization_type` | character | Organization type. |
| `player_profile_flag` | integer | Player profile flag. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_drafthistory(league_id='10', season_year_nullable='2024')
```

_Last validated n/a._

## `wnba_stats_fantasywidget`

GET /stats/fantasywidget

**Endpoint URL:** `GET https://stats.wnba.com/stats/fantasywidget`

**Valid URL:** [https://stats.wnba.com/stats/fantasywidget?LeagueID=10](https://stats.wnba.com/stats/fantasywidget?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ActivePlayers` | `active_players` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TodaysOpponent` | `todays_opponent` |  |  | `Y` |  |
| `TodaysPlayers` | `todays_players` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `min` | numeric | Minutes played. |
| `fan_duel_pts` | numeric | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `blk` | numeric | Blocks. |
| `stl` | numeric | Steals. |
| `tov` | numeric | Turnovers. |
| `fg3m` | numeric | Three-point field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_fantasywidget(league_id='10')
```

_Last validated n/a._

## `wnba_stats_franchisehistory`

GET /stats/franchisehistory

**Endpoint URL:** `GET https://stats.wnba.com/stats/franchisehistory`

**Valid URL:** [https://stats.wnba.com/stats/franchisehistory?LeagueID=10](https://stats.wnba.com/stats/franchisehistory?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `start_year` | character | Span starting year. |
| `end_year` | character | Span ending year. |
| `years` | integer | Years. |
| `games` | integer | Games played. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `win_pct` | numeric | Win percentage (0-1 decimal). |
| `po_appearances` | integer | NBA or WNBA Stats value for playoff appearances in the franchisehistory result set. |
| `div_titles` | integer | NBA or WNBA Stats value for div titles in the franchisehistory result set. |
| `conf_titles` | integer | NBA or WNBA Stats value for conf titles in the franchisehistory result set. |
| `league_titles` | integer | NBA or WNBA Stats value for league titles in the franchisehistory result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_franchisehistory(league_id='10')
```

_Last validated n/a._

## `wnba_stats_franchiseleaders`

GET /stats/franchiseleaders

**Endpoint URL:** `GET https://stats.wnba.com/stats/franchiseleaders`

**Valid URL:** [https://stats.wnba.com/stats/franchiseleaders?LeagueID=10](https://stats.wnba.com/stats/franchiseleaders?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `pts` | integer | Points scored. |
| `pts_person_id` | integer | Stats API identifier for points person identifier associated with this NBA or WNBA Stats row. |
| `pts_player` | character | Scoring or score-margin metric for points player in the requested NBA or WNBA Stats split. |
| `ast` | integer | Assists. |
| `ast_person_id` | integer | Stats API identifier for assists person identifier associated with this NBA or WNBA Stats row. |
| `ast_player` | character | NBA or WNBA Stats value for assists player in the franchiseleaders result set. |
| `reb` | integer | Total rebounds. |
| `reb_person_id` | integer | Stats API identifier for rebounds person identifier associated with this NBA or WNBA Stats row. |
| `reb_player` | character | Rebounding metric for rebounds player in the requested NBA or WNBA Stats split. |
| `blk` | integer | Blocks. |
| `blk_person_id` | integer | Stats API identifier for blocks person identifier associated with this NBA or WNBA Stats row. |
| `blk_player` | character | NBA or WNBA Stats value for blocks player in the franchiseleaders result set. |
| `stl` | integer | Steals. |
| `stl_person_id` | integer | Stats API identifier for steals person identifier associated with this NBA or WNBA Stats row. |
| `stl_player` | character | NBA or WNBA Stats value for steals player in the franchiseleaders result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_franchiseleaders(league_id='10')
```

_Last validated n/a._

## `wnba_stats_franchiseleaderswrank`

GET /stats/franchiseleaderswrank

**Endpoint URL:** `GET https://stats.wnba.com/stats/franchiseleaderswrank`

**Valid URL:** [https://stats.wnba.com/stats/franchiseleaderswrank?LeagueID=10](https://stats.wnba.com/stats/franchiseleaderswrank?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player` | character | Player name. |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `active_with_team` | integer | Flag indicating whether the franchise leader is still active with the team. |
| `gp` | integer | Games played. |
| `minutes` | numeric | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `pf` | numeric | Personal fouls. |
| `stl` | numeric | Steals. |
| `tov` | numeric | Turnovers. |
| `blk` | numeric | Blocks. |
| `pts` | numeric | Points scored. |
| `f_rank_gp` | integer | Franchise all-time rank of the player's career games played. |
| `f_rank_minutes` | integer | Franchise all-time rank of the player's career minutes played. |
| `f_rank_fgm` | integer | Franchise all-time rank of the player's career field goals made. |
| `f_rank_fga` | integer | Franchise all-time rank of the player's career field goals attempted. |
| `f_rank_fg_pct` | integer | Franchise all-time rank of the player's career field goal percentage. |
| `f_rank_fg3m` | integer | Franchise all-time rank of the player's career three-point field goals made. |
| `f_rank_fg3a` | integer | Franchise all-time rank of the player's career three-point field goals attempted. |
| `f_rank_fg3_pct` | integer | Franchise all-time rank of the player's career three-point field goal percentage. |
| `f_rank_ftm` | integer | Franchise all-time rank of the player's career free throws made. |
| `f_rank_fta` | integer | Franchise all-time rank of the player's career free throws attempted. |
| `f_rank_ft_pct` | integer | Franchise all-time rank of the player's career free throw percentage. |
| `f_rank_oreb` | integer | Franchise all-time rank of the player's career offensive rebounds. |
| `f_rank_dreb` | integer | Franchise all-time rank of the player's career defensive rebounds. |
| `f_rank_reb` | integer | Franchise all-time rank of the player's career total rebounds. |
| `f_rank_ast` | integer | Franchise all-time rank of the player's career assists. |
| `f_rank_pf` | integer | Franchise all-time rank of the player's career personal fouls committed. |
| `f_rank_stl` | integer | Franchise all-time rank of the player's career steals. |
| `f_rank_tov` | integer | Franchise all-time rank of the player's career turnovers. |
| `f_rank_blk` | integer | Franchise all-time rank of the player's career blocked shots. |
| `f_rank_pts` | integer | Franchise all-time rank of the player's career points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_franchiseleaderswrank(league_id='10')
```

_Last validated n/a._

## `wnba_stats_franchiseplayers`

GET /stats/franchiseplayers

**Endpoint URL:** `GET https://stats.wnba.com/stats/franchiseplayers`

**Valid URL:** [https://stats.wnba.com/stats/franchiseplayers?LeagueID=10](https://stats.wnba.com/stats/franchiseplayers?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player` | character | Player name. |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `active_with_team` | integer | Flag indicating whether the player is still active with the franchise. |
| `gp` | integer | Games played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `pf` | numeric | Personal fouls. |
| `stl` | numeric | Steals. |
| `tov` | numeric | Turnovers. |
| `blk` | numeric | Blocks. |
| `pts` | numeric | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_franchiseplayers(league_id='10')
```

_Last validated n/a._

## `wnba_stats_gamerotation`

GET /stats/gamerotation

**Endpoint URL:** `GET https://stats.wnba.com/stats/gamerotation`

**Valid URL:** [https://stats.wnba.com/stats/gamerotation?LeagueID=10](https://stats.wnba.com/stats/gamerotation?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_first` | character | NBA or WNBA Stats value for player first in the gamerotation result set. |
| `player_last` | character | NBA or WNBA Stats value for player last in the gamerotation result set. |
| `in_time_real` | numeric | Real-time clock value when the player entered the game rotation stint. |
| `out_time_real` | numeric | Real-time clock value when the player exited the game rotation stint. |
| `player_pts` | integer | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `pt_diff` | numeric | NBA or WNBA Stats value for pt diff in the gamerotation result set. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_gamerotation(league_id='10')
```

_Last validated n/a._

## `wnba_stats_homepageleaders`

GET /stats/homepageleaders

**Endpoint URL:** `GET https://stats.wnba.com/stats/homepageleaders`

**Valid URL:** [https://stats.wnba.com/stats/homepageleaders?LeagueID=10](https://stats.wnba.com/stats/homepageleaders?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `StatCategory` | `stat_category` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | character | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `pts` | character | Points scored. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `efg_pct` | numeric | Effective field goal percentage, as a decimal. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `pts_per48` | character | Points scored per 48 minutes played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_homepageleaders(league_id='10')
```

_Last validated n/a._

## `wnba_stats_homepagev2`

GET /stats/homepagev2

**Endpoint URL:** `GET https://stats.wnba.com/stats/homepagev2`

**Valid URL:** [https://stats.wnba.com/stats/homepagev2?LeagueID=10](https://stats.wnba.com/stats/homepagev2?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `StatType` | `stat_type` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `blk` | numeric | Blocks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_homepagev2(league_id='10')
```

_Last validated n/a._

## `wnba_stats_hustlestatsboxscore`

GET /stats/hustlestatsboxscore

**Endpoint URL:** `GET https://stats.wnba.com/stats/hustlestatsboxscore`

**Valid URL:** [https://stats.wnba.com/stats/hustlestatsboxscore](https://stats.wnba.com/stats/hustlestatsboxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | character | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | character | Unique player identifier. |
| `player_name` | character | Player name. |
| `start_position` | character | Position the player started the game at (F, C, or G); empty for reserves. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `pts` | integer | Points scored. |
| `contested_shots` | numeric | Defensively contested shots. |
| `contested_shots_2pt` | numeric | Opponent two-point attempts contested. |
| `contested_shots_3pt` | numeric | Opponent three-point attempts contested. |
| `deflections` | numeric | Defensive deflections. |
| `charges_drawn` | numeric | Charges drawn. |
| `screen_assists` | numeric | Screen assists (resulting in a basket). |
| `screen_ast_pts` | numeric | Points teammates scored directly off the row's screen assists. |
| `off_loose_balls_recovered` | numeric | Loose balls recovered while on offense. |
| `def_loose_balls_recovered` | numeric | Loose balls recovered while on defense. |
| `loose_balls_recovered` | numeric | Total loose balls recovered. |
| `off_boxouts` | numeric | Box-outs recorded on the offensive glass. |
| `def_boxouts` | numeric | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebs` | numeric | Team rebounds secured following the row's box-outs. |
| `box_out_player_rebs` | numeric | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | numeric | Box-outs executed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_hustlestatsboxscore()
```

_Last validated n/a._

## `wnba_stats_infographicfanduelplayer`

GET /stats/infographicfanduelplayer

**Endpoint URL:** `GET https://stats.wnba.com/stats/infographicfanduelplayer`

**Valid URL:** [https://stats.wnba.com/stats/infographicfanduelplayer](https://stats.wnba.com/stats/infographicfanduelplayer)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `location` | character | Filter results by game location. |
| `fan_duel_pts` | numeric | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |
| `min` | numeric | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | integer | Three-point field goals made. |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Total rebounds. |
| `ast` | integer | Assists. |
| `tov` | integer | Turnovers. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `blka` | integer | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | integer | Personal fouls. |
| `pfd` | integer | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_infographicfanduelplayer()
```

_Last validated n/a._

## `wnba_stats_leaderstiles`

GET /stats/leaderstiles

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaderstiles`

**Valid URL:** [https://stats.wnba.com/stats/leaderstiles?LeagueID=10](https://stats.wnba.com/stats/leaderstiles?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `Stat` | `stat` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pts` | numeric | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaderstiles(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguedashlineups`

GET /stats/leaguedashlineups

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguedashlineups`

**Valid URL:** [https://stats.wnba.com/stats/leaguedashlineups?LeagueID=10](https://stats.wnba.com/stats/leaguedashlineups?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GroupQuantity` | `group_quantity` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_id` | character | ESPN group id. |
| `group_name` | character | Group name (conference / division). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `sum_time_played` | integer | Time value for sum time played in the NBA or WNBA Stats result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashlineups(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguedashplayerbiostats`

GET /stats/leaguedashplayerbiostats

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguedashplayerbiostats`

**Valid URL:** [https://stats.wnba.com/stats/leaguedashplayerbiostats?LeagueID=10](https://stats.wnba.com/stats/leaguedashplayerbiostats?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `age` | numeric | Player age (in years). |
| `player_height` | character | Participant height (e.g. "6' 5\""). |
| `player_height_inches` | integer | NBA or WNBA Stats value for player height inches in the leaguedashplayerbiostats result set. |
| `player_weight` | character | Participant weight in pounds. |
| `college` | character | College or school attended. |
| `country` | character | Country (full name or code). |
| `draft_year` | character | Draft year (4-digit). |
| `draft_round` | character | Round of the draft selection. |
| `draft_number` | character | The number pick that was used to select a given player. |
| `gp` | integer | Games played. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `net_rating` | numeric | Net rating (off rating - def rating). |
| `oreb_pct` | numeric | Percentage or rate for offensive rebounds percentage in the requested NBA or WNBA Stats split. |
| `dreb_pct` | numeric | Percentage or rate for defensive rebounds percentage in the requested NBA or WNBA Stats split. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `ast_pct` | numeric | Assist percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashplayerbiostats(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguedashplayerclutch`

GET /stats/leaguedashplayerclutch

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguedashplayerclutch`

**Valid URL:** [https://stats.wnba.com/stats/leaguedashplayerclutch?LeagueID=10](https://stats.wnba.com/stats/leaguedashplayerclutch?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `AheadBehind` | `ahead_behind` |  |  | `Y` |  |
| `ClutchTime` | `clutch_time` |  |  | `Y` |  |
| `College` | `college_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `PointDiff` | `point_diff` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `age` | numeric | Player age (in years). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |
| `dd2` | integer | Double-doubles recorded over the split. |
| `td3` | integer | Triple-doubles recorded over the split. |
| `wnba_fantasy_pts` | numeric | Fantasy points under the WNBA's fantasy scoring formula. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |
| `nba_fantasy_pts_rank` | integer | League rank of the row's NBA fantasy points (league scoring formula) for the season and split. |
| `dd2_rank` | integer | League rank of the row's double-doubles for the season and split. |
| `td3_rank` | integer | League rank of the row's triple-doubles for the season and split. |
| `wnba_fantasy_pts_rank` | integer | League rank of the row's WNBA fantasy points (league scoring formula) for the season and split. |
| `team_count` | integer | Number of distinct teams aggregated into the split row. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashplayerclutch(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguedashplayershotlocations`

GET /stats/leaguedashplayershotlocations

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguedashplayershotlocations`

**Valid URL:** [https://stats.wnba.com/stats/leaguedashplayershotlocations?LeagueID=10](https://stats.wnba.com/stats/leaguedashplayershotlocations?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `DistanceRange` | `distance_range` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_simple` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `age` | numeric | Player age (in years). |
| `nickname` | character | Team or athlete nickname. |
| `less_than_5_ft_fgm` | numeric | Field goals made from less than 5 feet. |
| `less_than_5_ft_fga` | numeric | Field goals attempted from less than 5 feet. |
| `less_than_5_ft_fg_pct` | numeric | Field goal percentage on shots from less than 5 feet, as a decimal. |
| `5-9_ft_fgm` | numeric | Field goals made from 5-9 feet. |
| `5-9_ft_fga` | numeric | Field goals attempted from 5-9 feet. |
| `5-9_ft_fg_pct` | numeric | Field goal percentage on shots from 5-9-f feet, as a decimal. |
| `10-14_ft_fgm` | numeric | Field goals made from 10-14 feet. |
| `10-14_ft_fga` | numeric | Field goals attempted from 10-14 feet. |
| `10-14_ft_fg_pct` | numeric | Field goal percentage on shots from 10-14-f feet, as a decimal. |
| `15-19_ft_fgm` | numeric | Field goals made from 15-19 feet. |
| `15-19_ft_fga` | numeric | Field goals attempted from 15-19 feet. |
| `15-19_ft_fg_pct` | numeric | Field goal percentage on shots from 15-19-f feet, as a decimal. |
| `20-24_ft_fgm` | numeric | Field goals made from 20-24 feet. |
| `20-24_ft_fga` | numeric | Field goals attempted from 20-24 feet. |
| `20-24_ft_fg_pct` | numeric | Field goal percentage on shots from 20-24-f feet, as a decimal. |
| `25-29_ft_fgm` | numeric | Field goals made from 25-29 feet. |
| `25-29_ft_fga` | numeric | Field goals attempted from 25-29 feet. |
| `25-29_ft_fg_pct` | numeric | Field goal percentage on shots from 25-29-f feet, as a decimal. |
| `30-34_ft_fgm` | numeric | Field goals made from 30-34 feet. |
| `30-34_ft_fga` | numeric | Field goals attempted from 30-34 feet. |
| `30-34_ft_fg_pct` | numeric | Field goal percentage on shots from 30-34-f feet, as a decimal. |
| `35-39_ft_fgm` | numeric | Field goals made from 35-39 feet. |
| `35-39_ft_fga` | numeric | Field goals attempted from 35-39 feet. |
| `35-39_ft_fg_pct` | numeric | Field goal percentage on shots from 35-39-f feet, as a decimal. |
| `40+_ft_fgm` | numeric | Field goals made from 40 feet and beyond, per the stats API's shot-location distance bands. |
| `40+_ft_fga` | numeric | Field goals attempted from 40 feet and beyond, per the stats API's shot-location distance bands. |
| `40+_ft_fg_pct` | numeric | Field-goal percentage on attempts from 40 feet and beyond, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashplayershotlocations(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguedashplayerstats`

GET /stats/leaguedashplayerstats

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguedashplayerstats`

**Valid URL:** [https://stats.wnba.com/stats/leaguedashplayerstats?LeagueID=10](https://stats.wnba.com/stats/leaguedashplayerstats?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TwoWay` | `two_way_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `age` | numeric | Player age (in years). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked shot attempts against the player — shots the player attempted that were blocked by opponents. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn — fouls committed by opponents against this player. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | NBA fantasy points accrued under the standard NBA fantasy scoring formula. |
| `dd2` | integer | Number of double-doubles recorded over the span. |
| `td3` | integer | Number of triple-doubles recorded over the span. |
| `wnba_fantasy_pts` | numeric | WNBA fantasy points accrued under the standard WNBA fantasy scoring formula. |
| `gp_rank` | integer | Player's league rank for games played among qualified players (1 = most games). |
| `w_rank` | integer | Player's league rank for wins (1 = most wins while active). |
| `l_rank` | integer | Player's league rank for losses (1 = most losses while active). |
| `w_pct_rank` | integer | Player's league rank for win percentage (1 = highest win pct). |
| `min_rank` | integer | Player's league rank for minutes played (1 = most minutes). |
| `fgm_rank` | integer | Player's league rank for field goals made (1 = most made). |
| `fga_rank` | integer | Player's league rank for field goal attempts (1 = most attempts). |
| `fg_pct_rank` | integer | Player's league rank for field goal percentage (1 = highest pct). |
| `fg3m_rank` | integer | Player's league rank for three-point field goals made (1 = most made). |
| `fg3a_rank` | integer | Player's league rank for three-point field goal attempts (1 = most attempts). |
| `fg3_pct_rank` | integer | Player's league rank for three-point percentage (1 = highest pct). |
| `ftm_rank` | integer | Player's league rank for free throws made (1 = most made). |
| `fta_rank` | integer | Player's league rank for free throw attempts (1 = most attempts). |
| `ft_pct_rank` | integer | Player's league rank for free throw percentage (1 = highest pct). |
| `oreb_rank` | integer | Player's league rank for offensive rebounds (1 = most offensive rebounds). |
| `dreb_rank` | integer | Player's league rank for defensive rebounds (1 = most defensive rebounds). |
| `reb_rank` | integer | Player's league rank for total rebounds (1 = most rebounds). |
| `ast_rank` | integer | Player's league rank for assists (1 = most assists). |
| `tov_rank` | integer | Player's league rank for turnovers — note: lower turnovers is typically better. |
| `stl_rank` | integer | Player's league rank for steals (1 = most steals). |
| `blk_rank` | integer | Player's league rank for blocked shots (1 = most blocks). |
| `blka_rank` | integer | Player's league rank for shots blocked by opponents (1 = most blocked). |
| `pf_rank` | integer | Player's league rank for personal fouls committed (1 = most fouls). |
| `pfd_rank` | integer | Player's league rank for personal fouls drawn from opponents (1 = most drawn). |
| `pts_rank` | integer | Player's league rank for points scored (1 = league leader). |
| `plus_minus_rank` | integer | Player's league rank for plus/minus rating (1 = best differential). |
| `nba_fantasy_pts_rank` | integer | Player's league rank for NBA fantasy points scored (1 = most fantasy points). |
| `dd2_rank` | integer | Player's league rank for double-doubles recorded (1 = most double-doubles). |
| `td3_rank` | integer | Player's league rank for triple-doubles recorded (1 = most triple-doubles). |
| `wnba_fantasy_pts_rank` | integer | Player's league rank for WNBA fantasy points scored (1 = most fantasy points). |
| `team_count` | integer | Number of teams the player appeared for within the span. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashplayerstats(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguedashptdefend`

GET /stats/leaguedashptdefend

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguedashptdefend`

**Valid URL:** [https://stats.wnba.com/stats/leaguedashptdefend?LeagueID=10](https://stats.wnba.com/stats/leaguedashptdefend?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `DefenseCategory` | `defense_category` |  |  | `Y` |  |
| `Division` | `division_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `close_def_person_id` | integer | Stats API identifier for close defensive person identifier associated with this NBA or WNBA Stats row. |
| `player_name` | character | Player name. |
| `player_last_team_id` | integer | Stats API identifier for player last team identifier associated with this NBA or WNBA Stats row. |
| `player_last_team_abbreviation` | character | NBA or WNBA Stats value for player last team abbreviation in the leaguedashptdefend result set. |
| `player_position` | character | Position of the player accordinng to NGS |
| `age` | numeric | Player age (in years). |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `freq` | numeric | NBA or WNBA Stats value for freq in the leaguedashptdefend result set. |
| `d_fgm` | numeric | Shooting metric for d fgm in the requested NBA or WNBA Stats split. |
| `d_fga` | numeric | Shooting metric for d fga in the requested NBA or WNBA Stats split. |
| `d_fg_pct` | numeric | Percentage or rate for d field goals percentage in the requested NBA or WNBA Stats split. |
| `normal_fg_pct` | numeric | Percentage or rate for normal field goals percentage in the requested NBA or WNBA Stats split. |
| `pct_plusminus` | numeric | Percentage share of plusminus for the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashptdefend(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguedashteamclutch`

GET /stats/leaguedashteamclutch

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguedashteamclutch`

**Valid URL:** [https://stats.wnba.com/stats/leaguedashteamclutch?LeagueID=10](https://stats.wnba.com/stats/leaguedashteamclutch?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `AheadBehind` | `ahead_behind` |  |  | `Y` |  |
| `ClutchTime` | `clutch_time` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `PointDiff` | `point_diff` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashteamclutch(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguedashteamshotlocations`

GET /stats/leaguedashteamshotlocations

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguedashteamshotlocations`

**Valid URL:** [https://stats.wnba.com/stats/leaguedashteamshotlocations?LeagueID=10](https://stats.wnba.com/stats/leaguedashteamshotlocations?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `DistanceRange` | `distance_range` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_simple` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `less_than_5_ft_fgm` | numeric | Field goals made from less than 5 feet. |
| `less_than_5_ft_fga` | numeric | Field goals attempted from less than 5 feet. |
| `less_than_5_ft_fg_pct` | numeric | Field goal percentage on shots from less than 5 feet, as a decimal. |
| `5-9_ft_fgm` | numeric | Field goals made from 5-9 feet. |
| `5-9_ft_fga` | numeric | Field goals attempted from 5-9 feet. |
| `5-9_ft_fg_pct` | numeric | Field goal percentage on shots from 5-9-f feet, as a decimal. |
| `10-14_ft_fgm` | numeric | Field goals made from 10-14 feet. |
| `10-14_ft_fga` | numeric | Field goals attempted from 10-14 feet. |
| `10-14_ft_fg_pct` | numeric | Field goal percentage on shots from 10-14-f feet, as a decimal. |
| `15-19_ft_fgm` | numeric | Field goals made from 15-19 feet. |
| `15-19_ft_fga` | numeric | Field goals attempted from 15-19 feet. |
| `15-19_ft_fg_pct` | numeric | Field goal percentage on shots from 15-19-f feet, as a decimal. |
| `20-24_ft_fgm` | numeric | Field goals made from 20-24 feet. |
| `20-24_ft_fga` | numeric | Field goals attempted from 20-24 feet. |
| `20-24_ft_fg_pct` | numeric | Field goal percentage on shots from 20-24-f feet, as a decimal. |
| `25-29_ft_fgm` | numeric | Field goals made from 25-29 feet. |
| `25-29_ft_fga` | numeric | Field goals attempted from 25-29 feet. |
| `25-29_ft_fg_pct` | numeric | Field goal percentage on shots from 25-29-f feet, as a decimal. |
| `30-34_ft_fgm` | numeric | Field goals made from 30-34 feet. |
| `30-34_ft_fga` | numeric | Field goals attempted from 30-34 feet. |
| `30-34_ft_fg_pct` | numeric | Field goal percentage on shots from 30-34-f feet, as a decimal. |
| `35-39_ft_fgm` | numeric | Field goals made from 35-39 feet. |
| `35-39_ft_fga` | numeric | Field goals attempted from 35-39 feet. |
| `35-39_ft_fg_pct` | numeric | Field goal percentage on shots from 35-39-f feet, as a decimal. |
| `40+_ft_fgm` | numeric | Team field goals made from 40 feet and beyond, per the stats API's shot-location distance bands. |
| `40+_ft_fga` | numeric | Team field goals attempted from 40 feet and beyond, per the stats API's shot-location distance bands. |
| `40+_ft_fg_pct` | numeric | Team field-goal percentage on attempts from 40 feet and beyond, as a decimal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashteamshotlocations(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguedashteamstats`

GET /stats/leaguedashteamstats

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguedashteamstats`

**Valid URL:** [https://stats.wnba.com/stats/leaguedashteamstats?LeagueID=10](https://stats.wnba.com/stats/leaguedashteamstats?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TwoWay` | `two_way_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashteamstats(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguegamefinder`

GET /stats/leaguegamefinder

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguegamefinder`

**Valid URL:** [https://stats.wnba.com/stats/leaguegamefinder?LeagueID=10](https://stats.wnba.com/stats/leaguegamefinder?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftNumber` | `draft_number_nullable` |  |  | `Y` |  |
| `DraftRound` | `draft_round_nullable` |  |  | `Y` |  |
| `DraftTeamID` | `draft_team_id_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `EqAST` | `eq_ast_nullable` |  |  | `Y` |  |
| `EqBLK` | `eq_blk_nullable` |  |  | `Y` |  |
| `EqDD` | `eq_dd_nullable` |  |  | `Y` |  |
| `EqDREB` | `eq_dreb_nullable` |  |  | `Y` |  |
| `EqFG3A` | `eq_fg3a_nullable` |  |  | `Y` |  |
| `EqFG3M` | `eq_fg3m_nullable` |  |  | `Y` |  |
| `EqFG3_PCT` | `eq_fg3_pct_nullable` |  |  | `Y` |  |
| `EqFGA` | `eq_fga_nullable` |  |  | `Y` |  |
| `EqFGM` | `eq_fgm_nullable` |  |  | `Y` |  |
| `EqFG_PCT` | `eq_fg_pct_nullable` |  |  | `Y` |  |
| `EqFTA` | `eq_fta_nullable` |  |  | `Y` |  |
| `EqFTM` | `eq_ftm_nullable` |  |  | `Y` |  |
| `EqFT_PCT` | `eq_ft_pct_nullable` |  |  | `Y` |  |
| `EqMINUTES` | `eq_minutes_nullable` |  |  | `Y` |  |
| `EqOREB` | `eq_oreb_nullable` |  |  | `Y` |  |
| `EqPF` | `eq_pf_nullable` |  |  | `Y` |  |
| `EqPTS` | `eq_pts_nullable` |  |  | `Y` |  |
| `EqREB` | `eq_reb_nullable` |  |  | `Y` |  |
| `EqSTL` | `eq_stl_nullable` |  |  | `Y` |  |
| `EqTD` | `eq_td_nullable` |  |  | `Y` |  |
| `EqTOV` | `eq_tov_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GtAST` | `gt_ast_nullable` |  |  | `Y` |  |
| `GtBLK` | `gt_blk_nullable` |  |  | `Y` |  |
| `GtDD` | `gt_dd_nullable` |  |  | `Y` |  |
| `GtDREB` | `gt_dreb_nullable` |  |  | `Y` |  |
| `GtFG3A` | `gt_fg3a_nullable` |  |  | `Y` |  |
| `GtFG3M` | `gt_fg3m_nullable` |  |  | `Y` |  |
| `GtFG3_PCT` | `gt_fg3_pct_nullable` |  |  | `Y` |  |
| `GtFGA` | `gt_fga_nullable` |  |  | `Y` |  |
| `GtFGM` | `gt_fgm_nullable` |  |  | `Y` |  |
| `GtFG_PCT` | `gt_fg_pct_nullable` |  |  | `Y` |  |
| `GtFTA` | `gt_fta_nullable` |  |  | `Y` |  |
| `GtFTM` | `gt_ftm_nullable` |  |  | `Y` |  |
| `GtFT_PCT` | `gt_ft_pct_nullable` |  |  | `Y` |  |
| `GtMINUTES` | `gt_minutes_nullable` |  |  | `Y` |  |
| `GtOREB` | `gt_oreb_nullable` |  |  | `Y` |  |
| `GtPF` | `gt_pf_nullable` |  |  | `Y` |  |
| `GtPTS` | `gt_pts_nullable` |  |  | `Y` |  |
| `GtREB` | `gt_reb_nullable` |  |  | `Y` |  |
| `GtSTL` | `gt_stl_nullable` |  |  | `Y` |  |
| `GtTD` | `gt_td_nullable` |  |  | `Y` |  |
| `GtTOV` | `gt_tov_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `LtAST` | `lt_ast_nullable` |  |  | `Y` |  |
| `LtBLK` | `lt_blk_nullable` |  |  | `Y` |  |
| `LtDD` | `lt_dd_nullable` |  |  | `Y` |  |
| `LtDREB` | `lt_dreb_nullable` |  |  | `Y` |  |
| `LtFG3A` | `lt_fg3a_nullable` |  |  | `Y` |  |
| `LtFG3M` | `lt_fg3m_nullable` |  |  | `Y` |  |
| `LtFG3_PCT` | `lt_fg3_pct_nullable` |  |  | `Y` |  |
| `LtFGA` | `lt_fga_nullable` |  |  | `Y` |  |
| `LtFGM` | `lt_fgm_nullable` |  |  | `Y` |  |
| `LtFG_PCT` | `lt_fg_pct_nullable` |  |  | `Y` |  |
| `LtFTA` | `lt_fta_nullable` |  |  | `Y` |  |
| `LtFTM` | `lt_ftm_nullable` |  |  | `Y` |  |
| `LtFT_PCT` | `lt_ft_pct_nullable` |  |  | `Y` |  |
| `LtMINUTES` | `lt_minutes_nullable` |  |  | `Y` |  |
| `LtOREB` | `lt_oreb_nullable` |  |  | `Y` |  |
| `LtPF` | `lt_pf_nullable` |  |  | `Y` |  |
| `LtPTS` | `lt_pts_nullable` |  |  | `Y` |  |
| `LtREB` | `lt_reb_nullable` |  |  | `Y` |  |
| `LtSTL` | `lt_stl_nullable` |  |  | `Y` |  |
| `LtTD` | `lt_td_nullable` |  |  | `Y` |  |
| `LtTOV` | `lt_tov_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team_abbreviation` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |
| `YearsExperience` | `years_experience_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `min` | integer | Minutes played. |
| `pts` | integer | Points scored. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | integer | Three-point field goals made. |
| `fg3a` | integer | Three-point field goal attempts. |
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
| `tov` | integer | Turnovers. |
| `pf` | integer | Personal fouls. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguegamefinder(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguegamelog`

GET /stats/leaguegamelog

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguegamelog`

**Valid URL:** [https://stats.wnba.com/stats/leaguegamelog?LeagueID=10](https://stats.wnba.com/stats/leaguegamelog?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Counter` | `counter` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Direction` | `direction` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team_abbreviation` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `Sorter` | `sorter` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `min` | integer | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | integer | Three-point field goals made. |
| `fg3a` | integer | Three-point field goal attempts. |
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
| `tov` | integer | Turnovers. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |
| `video_available` | integer | Video available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguegamelog(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leagueleaders`

GET /stats/leagueleaders

**Endpoint URL:** `GET https://stats.wnba.com/stats/leagueleaders`

**Valid URL:** [https://stats.wnba.com/stats/leagueleaders?LeagueID=10](https://stats.wnba.com/stats/leagueleaders?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ActiveFlag` | `active_flag_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode48` |  |  | `Y` |  |
| `Scope` | `scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `StatCategory` | `stat_category_abbreviation` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |
| `gp` | integer | Games played. |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pts` | numeric | Points scored. |
| `eff` | numeric | Eff. |
| `nickname` | character | Team or athlete nickname. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leagueleaders(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguelineupviz`

GET /stats/leaguelineupviz

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguelineupviz`

**Valid URL:** [https://stats.wnba.com/stats/leaguelineupviz?LeagueID=10](https://stats.wnba.com/stats/leaguelineupviz?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GroupQuantity` | `group_quantity` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `MinutesMin` | `minutes_min` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_id` | character | ESPN group id. |
| `group_name` | character | Group name (conference / division). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `min` | numeric | Minutes played. |
| `off_rating` | numeric | Points scored per 100 possessions with the lineup on the floor (offensive rating). |
| `def_rating` | numeric | Points allowed per 100 possessions with the lineup on the floor (defensive rating). |
| `net_rating` | numeric | Net rating (off rating - def rating). |
| `pace` | numeric | Possessions per 48 minutes. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `fta_rate` | numeric | Free throw attempts per field goal attempt for the lineup. |
| `tm_ast_pct` | numeric | Percentage of the lineup's made field goals that were assisted, as a decimal. |
| `pct_fga_2pt` | numeric | Share of field goal attempts taken as two-pointers, as a decimal. |
| `pct_fga_3pt` | numeric | Share of field goal attempts taken as three-pointers, as a decimal. |
| `pct_pts_2pt_mr` | numeric | Share of points scored on mid-range two-pointers, as a decimal. |
| `pct_pts_fb` | numeric | Share of points scored on fast breaks, as a decimal. |
| `pct_pts_ft` | numeric | Share of points scored at the free throw line, as a decimal. |
| `pct_pts_paint` | numeric | Share of points scored in the paint, as a decimal. |
| `pct_ast_fgm` | numeric | Percentage of made field goals that were assisted, as a decimal. |
| `pct_uast_fgm` | numeric | Percentage of made field goals that were unassisted, as a decimal. |
| `opp_fg3_pct` | numeric | Opponent three-point percentage against the lineup, as a decimal. |
| `opp_efg_pct` | numeric | Opponent effective field goal percentage against the lineup, as a decimal. |
| `opp_fta_rate` | numeric | Opponent free throw attempt rate against the lineup. |
| `opp_tov_pct` | numeric | Opponent turnover percentage forced by the lineup. |
| `sum_tm_min` | numeric | Total team minutes summed across the lineup's stints on the floor. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguelineupviz(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leagueplayerondetails`

GET /stats/leagueplayerondetails

**Endpoint URL:** `GET https://stats.wnba.com/stats/leagueplayerondetails`

**Valid URL:** [https://stats.wnba.com/stats/leagueplayerondetails?LeagueID=10](https://stats.wnba.com/stats/leagueplayerondetails?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `vs_player_id` | integer | Stats API identifier for vs player identifier associated with this NBA or WNBA Stats row. |
| `vs_player_name` | character | Display name for vs player name associated with this NBA or WNBA Stats row. |
| `court_status` | character | Indicates whether the compared player was on court or off court for the split row. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leagueplayerondetails(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leagueseasonmatchups`

GET /stats/leagueseasonmatchups

**Endpoint URL:** `GET https://stats.wnba.com/stats/leagueseasonmatchups`

**Valid URL:** [https://stats.wnba.com/stats/leagueseasonmatchups?LeagueID=10](https://stats.wnba.com/stats/leagueseasonmatchups?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DefPlayerID` | `def_player_id_nullable` |  |  | `Y` |  |
| `DefTeamID` | `def_team_id_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `OffPlayerID` | `off_player_id_nullable` |  |  | `Y` |  |
| `OffTeamID` | `off_team_id_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `off_player_id` | integer | Stats API identifier for offensive player identifier associated with this NBA or WNBA Stats row. |
| `off_player_name` | character | Display name for offensive player name associated with this NBA or WNBA Stats row. |
| `def_player_id` | integer | Stats API identifier for defensive player identifier associated with this NBA or WNBA Stats row. |
| `def_player_name` | character | Display name for defensive player name associated with this NBA or WNBA Stats row. |
| `gp` | integer | Games played. |
| `matchup_min` | numeric | NBA or WNBA Stats value for matchup minutes in the leagueseasonmatchups result set. |
| `partial_poss` | numeric | Estimated partial possessions credited to the stint or rotation interval. |
| `player_pts` | numeric | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `team_pts` | numeric | Scoring or score-margin metric for team points in the requested NBA or WNBA Stats split. |
| `matchup_ast` | numeric | NBA or WNBA Stats value for matchup assists in the leagueseasonmatchups result set. |
| `matchup_tov` | numeric | Turnover or loose-ball metric for matchup turnovers in the requested NBA or WNBA Stats split. |
| `matchup_blk` | numeric | NBA or WNBA Stats value for matchup blocks in the leagueseasonmatchups result set. |
| `matchup_fgm` | numeric | Shooting metric for matchup fgm in the requested NBA or WNBA Stats split. |
| `matchup_fga` | numeric | Shooting metric for matchup fga in the requested NBA or WNBA Stats split. |
| `matchup_fg_pct` | numeric | Percentage or rate for matchup field goals percentage in the requested NBA or WNBA Stats split. |
| `matchup_fg3m` | numeric | Shooting metric for matchup fg3m in the requested NBA or WNBA Stats split. |
| `matchup_fg3a` | numeric | Shooting metric for matchup fg3a in the requested NBA or WNBA Stats split. |
| `matchup_fg3_pct` | numeric | Percentage or rate for matchup three-point field goals percentage in the requested NBA or WNBA Stats split. |
| `help_blk` | integer | NBA or WNBA Stats value for help blocks in the leagueseasonmatchups result set. |
| `help_fgm` | integer | Shooting metric for help fgm in the requested NBA or WNBA Stats split. |
| `help_fga` | integer | Shooting metric for help fga in the requested NBA or WNBA Stats split. |
| `help_fg_perc` | integer | Shooting metric for help field goals perc in the requested NBA or WNBA Stats split. |
| `matchup_ftm` | numeric | NBA or WNBA Stats value for matchup ftm in the leagueseasonmatchups result set. |
| `matchup_fta` | numeric | NBA or WNBA Stats value for matchup fta in the leagueseasonmatchups result set. |
| `sfl` | numeric | NBA or WNBA Stats value for sfl in the leagueseasonmatchups result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leagueseasonmatchups(league_id='10')
```

_Last validated n/a._

## `wnba_stats_leaguestandingsv3`

GET /stats/leaguestandingsv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaguestandingsv3`

**Valid URL:** [https://stats.wnba.com/stats/leaguestandingsv3?LeagueID=10](https://stats.wnba.com/stats/leaguestandingsv3?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `SeasonYear` | `season_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `leagueid` | character | League identifier used in compact NBA Stats schedule and scoreboard result sets. |
| `seasonid` | character | Stats API identifier for seasonid associated with this NBA or WNBA Stats row. |
| `teamid` | integer | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `conference` | character | Filter players or teams by conference. |
| `conferencerecord` | character | NBA or WNBA Stats value for conferencerecord in the leaguestandingsv3 result set. |
| `playoffrank` | integer | NBA or WNBA Stats value for playoffrank in the leaguestandingsv3 result set. |
| `clinchindicator` | character | NBA or WNBA Stats value for clinchindicator in the leaguestandingsv3 result set. |
| `division` | character | Team division. |
| `divisionrecord` | character | NBA or WNBA Stats value for divisionrecord in the leaguestandingsv3 result set. |
| `divisionrank` | integer | NBA or WNBA Stats value for divisionrank in the leaguestandingsv3 result set. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `winpct` | numeric | Winning percentage for the team or split represented by this row. |
| `leaguerank` | integer | NBA or WNBA Stats value for leaguerank in the leaguestandingsv3 result set. |
| `record` | character | Record string (e.g. '12-4'). |
| `home` | character | Home. |
| `road` | character | Road. |
| `l10` | character | L10. |
| `last10home` | character | NBA or WNBA Stats value for last10home in the leaguestandingsv3 result set. |
| `last10road` | character | NBA or WNBA Stats value for last10road in the leaguestandingsv3 result set. |
| `ot` | character | Ot. |
| `threeptsorless` | character | Scoring or score-margin metric for threeptsorless in the requested NBA or WNBA Stats split. |
| `tenptsormore` | character | Scoring or score-margin metric for tenptsormore in the requested NBA or WNBA Stats split. |
| `longhomestreak` | integer | NBA or WNBA Stats value for longhomestreak in the leaguestandingsv3 result set. |
| `strlonghomestreak` | character | NBA or WNBA Stats value for strlonghomestreak in the leaguestandingsv3 result set. |
| `longroadstreak` | integer | NBA or WNBA Stats value for longroadstreak in the leaguestandingsv3 result set. |
| `strlongroadstreak` | character | NBA or WNBA Stats value for strlongroadstreak in the leaguestandingsv3 result set. |
| `longwinstreak` | integer | NBA or WNBA Stats value for longwinstreak in the leaguestandingsv3 result set. |
| `longlossstreak` | integer | NBA or WNBA Stats value for longlossstreak in the leaguestandingsv3 result set. |
| `currenthomestreak` | integer | NBA or WNBA Stats value for currenthomestreak in the leaguestandingsv3 result set. |
| `strcurrenthomestreak` | character | NBA or WNBA Stats value for strcurrenthomestreak in the leaguestandingsv3 result set. |
| `currentroadstreak` | integer | NBA or WNBA Stats value for currentroadstreak in the leaguestandingsv3 result set. |
| `strcurrentroadstreak` | character | NBA or WNBA Stats value for strcurrentroadstreak in the leaguestandingsv3 result set. |
| `currentstreak` | integer | NBA or WNBA Stats value for currentstreak in the leaguestandingsv3 result set. |
| `strcurrentstreak` | character | Strcurrentstreak. |
| `conferencegamesback` | numeric | NBA or WNBA Stats value for conferencegamesback in the leaguestandingsv3 result set. |
| `divisiongamesback` | numeric | NBA or WNBA Stats value for divisiongamesback in the leaguestandingsv3 result set. |
| `clinchedconferencetitle` | integer | Flag indicating clinchedconferencetitle for the requested NBA or WNBA Stats context. |
| `clincheddivisiontitle` | integer | Flag indicating clincheddivisiontitle for the requested NBA or WNBA Stats context. |
| `clinchedplayoffbirth` | integer | Flag indicating clinchedplayoffbirth for the requested NBA or WNBA Stats context. |
| `clinchedplayin` | integer | Flag indicating clinchedplayin for the requested NBA or WNBA Stats context. |
| `eliminatedconference` | integer | Flag indicating eliminatedconference for the requested NBA or WNBA Stats context. |
| `eliminateddivision` | integer | Flag indicating eliminateddivision for the requested NBA or WNBA Stats context. |
| `aheadathalf` | character | NBA or WNBA Stats value for aheadathalf in the leaguestandingsv3 result set. |
| `behindathalf` | character | NBA or WNBA Stats value for behindathalf in the leaguestandingsv3 result set. |
| `tiedathalf` | character | NBA or WNBA Stats value for tiedathalf in the leaguestandingsv3 result set. |
| `aheadatthird` | character | NBA or WNBA Stats value for aheadatthird in the leaguestandingsv3 result set. |
| `behindatthird` | character | NBA or WNBA Stats value for behindatthird in the leaguestandingsv3 result set. |
| `tiedatthird` | character | NBA or WNBA Stats value for tiedatthird in the leaguestandingsv3 result set. |
| `score100pts` | character | Scoring or score-margin metric for score100pts in the requested NBA or WNBA Stats split. |
| `oppscore100pts` | character | Scoring or score-margin metric for oppscore100pts in the requested NBA or WNBA Stats split. |
| `oppover500` | character | NBA or WNBA Stats value for oppover500 in the leaguestandingsv3 result set. |
| `leadinfgpct` | character | Shooting metric for leadinfgpct in the requested NBA or WNBA Stats split. |
| `leadinreb` | character | Rebounding metric for leadinreb in the requested NBA or WNBA Stats split. |
| `fewerturnovers` | character | Turnover or loose-ball metric for fewerturnovers in the requested NBA or WNBA Stats split. |
| `pointspg` | numeric | Scoring or score-margin metric for pointspg in the requested NBA or WNBA Stats split. |
| `opppointspg` | numeric | Scoring or score-margin metric for opppointspg in the requested NBA or WNBA Stats split. |
| `diffpointspg` | numeric | Scoring or score-margin metric for diffpointspg in the requested NBA or WNBA Stats split. |
| `vseast` | character | NBA or WNBA Stats value for vseast in the leaguestandingsv3 result set. |
| `vsatlantic` | character | NBA or WNBA Stats value for vsatlantic in the leaguestandingsv3 result set. |
| `vscentral` | character | NBA or WNBA Stats value for vscentral in the leaguestandingsv3 result set. |
| `vssoutheast` | character | NBA or WNBA Stats value for vssoutheast in the leaguestandingsv3 result set. |
| `vswest` | character | NBA or WNBA Stats value for vswest in the leaguestandingsv3 result set. |
| `vsnorthwest` | character | NBA or WNBA Stats value for vsnorthwest in the leaguestandingsv3 result set. |
| `vspacific` | character | NBA or WNBA Stats value for vspacific in the leaguestandingsv3 result set. |
| `vssouthwest` | character | NBA or WNBA Stats value for vssouthwest in the leaguestandingsv3 result set. |
| `jan` | character | Value for January in the endpoint's monthly NBA or WNBA Stats split. |
| `feb` | character | Value for February in the endpoint's monthly NBA or WNBA Stats split. |
| `mar` | character | Value for March in the endpoint's monthly NBA or WNBA Stats split. |
| `apr` | character | Value for April in the endpoint's monthly NBA or WNBA Stats split. |
| `may` | character | Value for May in the endpoint's monthly NBA or WNBA Stats split. |
| `jun` | character | Value for June in the endpoint's monthly NBA or WNBA Stats split. |
| `jul` | character | Value for July in the endpoint's monthly NBA or WNBA Stats split. |
| `aug` | character | Value for August in the endpoint's monthly NBA or WNBA Stats split. |
| `sep` | character | Value for September in the endpoint's monthly NBA or WNBA Stats split. |
| `oct` | character | Value for October in the endpoint's monthly NBA or WNBA Stats split. |
| `nov` | character | Value for November in the endpoint's monthly NBA or WNBA Stats split. |
| `dec` | character | Value for December in the endpoint's monthly NBA or WNBA Stats split. |
| `score_80_plus` | character | Scoring or score-margin metric for score 80 plus in the requested NBA or WNBA Stats split. |
| `opp_score_80_plus` | character | Opponent score 80 plus for the requested NBA or WNBA team, player, lineup, or game split. |
| `score_below_80` | character | Scoring or score-margin metric for score below 80 in the requested NBA or WNBA Stats split. |
| `opp_score_below_80` | character | Opponent score below 80 for the requested NBA or WNBA team, player, lineup, or game split. |
| `totalpoints` | integer | Scoring or score-margin metric for totalpoints in the requested NBA or WNBA Stats split. |
| `opptotalpoints` | integer | Scoring or score-margin metric for opptotalpoints in the requested NBA or WNBA Stats split. |
| `difftotalpoints` | integer | Scoring or score-margin metric for difftotalpoints in the requested NBA or WNBA Stats split. |
| `leaguegamesback` | numeric | NBA or WNBA Stats value for leaguegamesback in the leaguestandingsv3 result set. |
| `playoffseeding` | integer | NBA or WNBA Stats value for playoffseeding in the leaguestandingsv3 result set. |
| `clinchedpostseason` | integer | Flag indicating clinchedpostseason for the requested NBA or WNBA Stats context. |
| `neutral` | character | Neutral. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguestandingsv3(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playbyplayv2`

GET /stats/playbyplayv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/playbyplayv2`

**Valid URL:** [https://stats.wnba.com/stats/playbyplayv2](https://stats.wnba.com/stats/playbyplayv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `eventnum` | integer | Sequential event number within the game's play-by-play feed. |
| `eventmsgtype` | integer | Numeric event type code (1 = made shot, 2 = missed shot, 3 = free throw, 4 = rebound, 5 = turnover, 6 = foul, ...). |
| `eventmsgactiontype` | integer | Numeric sub-type code refining eventmsgtype (e.g. the specific shot, foul, or turnover variety). |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `wctimestring` | character | Wall-clock time of day when the event occurred. |
| `pctimestring` | character | Game clock remaining in the period when the event occurred (MM:SS). |
| `homedescription` | character | Text description of the event from the home team's perspective; empty when not a home-team action. |
| `neutraldescription` | character | Neutral text description of the event (e.g. period start/end); empty for team actions. |
| `visitordescription` | character | Text description of the event from the visiting team's perspective; empty when not a visitor action. |
| `score` | character | Final score. |
| `scoremargin` | character | Score margin after the event ('TIE' when tied); empty on non-scoring events. |
| `person1type` | integer | Person1type. |
| `player1_id` | integer | V2 PBP primary player ID (e.g. shooter / fouler). |
| `player1_name` | character | V2 PBP primary player name. |
| `player1_team_id` | integer | Team ID of player1. |
| `player1_team_city` | character | Player1 team city. |
| `player1_team_nickname` | character | Player1 team nickname. |
| `player1_team_abbreviation` | character | Player1 team abbreviation. |
| `person2type` | integer | Person2type. |
| `player2_id` | integer | V2 PBP secondary player ID (e.g. assister / fouled-by). |
| `player2_name` | character | V2 PBP secondary player name. |
| `player2_team_id` | integer | Team ID of player2. |
| `player2_team_city` | character | Player2 team city. |
| `player2_team_nickname` | character | Player2 team nickname. |
| `player2_team_abbreviation` | character | Player2 team abbreviation. |
| `person3type` | integer | Person3type. |
| `player3_id` | integer | V2 PBP tertiary player ID (e.g. blocker). |
| `player3_name` | character | V2 PBP tertiary player name. |
| `player3_team_id` | integer | Team ID of player3. |
| `player3_team_city` | character | Player3 team city. |
| `player3_team_nickname` | character | Player3 team nickname. |
| `player3_team_abbreviation` | character | Player3 team abbreviation. |
| `video_available_flag` | integer | Video available flag. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playbyplayv2()
```

_Last validated n/a._

## `wnba_stats_playbyplayv3`

GET /stats/playbyplayv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/playbyplayv3`

**Valid URL:** [https://stats.wnba.com/stats/playbyplayv3](https://stats.wnba.com/stats/playbyplayv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `actionid` | integer | NBA or WNBA Stats action identifier for the play event. |
| `actionnumber` | integer | Sequential action number for the play within the game feed. |
| `actiontype` | character | Normalized play action type reported by NBA or WNBA Stats. |
| `clock` | character | Game clock value. |
| `description` | character | Long-form description text. |
| `gameid` | character | Unique NBA or WNBA Stats game identifier for the play event. |
| `isfieldgoal` | integer | Flag indicating whether the play event is a field-goal attempt. |
| `location` | character | Filter results by game location. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `personid` | integer | NBA or WNBA Stats player identifier associated with the play, when present. |
| `playername` | character | Full display name for the player associated with the play event. |
| `playernamei` | character | Abbreviated player display name used by the play feed. |
| `pointstotal` | integer | Running points total credited to the player after the play, when reported. |
| `scoreaway` | character | Away team's score after the play, when reported by the feed. |
| `scorehome` | character | Home team's score after the play, when reported by the feed. |
| `shotdistance` | integer | Shot distance in feet for shot attempts, when available. |
| `shotresult` | character | Result of the shot attempt, such as made or missed. |
| `shotvalue` | integer | Point value of the shot attempt, usually two or three points. |
| `subtype` | character | Secondary play subtype reported by NBA or WNBA Stats. |
| `teamid` | integer | Teamid. |
| `teamtricode` | character | Three-letter code for the team associated with the play event. |
| `videoavailable` | integer | Flag indicating whether video is available for the play or game row. |
| `xlegacy` | integer | Legacy NBA Stats x-coordinate for shot-location play events. |
| `ylegacy` | integer | Legacy NBA Stats y-coordinate for shot-location play events. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playbyplayv3()
```

_Last validated n/a._

## `wnba_stats_playerawards`

GET /stats/playerawards

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerawards`

**Valid URL:** [https://stats.wnba.com/stats/playerawards](https://stats.wnba.com/stats/playerawards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `team` | character | Team-side label or team identifier. |
| `description` | character | Long-form description text. |
| `all_nba_team_number` | character | NBA or WNBA Stats value for all NBA team number in the playerawards result set. |
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `month` | character | NBA or WNBA Stats value for month in the playerawards result set. |
| `week` | character | Week number. |
| `conference` | character | Filter players or teams by conference. |
| `type` | character | Record type / category. |
| `subtype1` | character | NBA or WNBA Stats value for subtype1 in the playerawards result set. |
| `subtype2` | character | NBA or WNBA Stats value for subtype2 in the playerawards result set. |
| `subtype3` | character | NBA or WNBA Stats value for subtype3 in the playerawards result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerawards()
```

_Last validated n/a._

## `wnba_stats_playercareerbycollegerollup`

GET /stats/playercareerbycollegerollup

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercareerbycollegerollup`

**Valid URL:** [https://stats.wnba.com/stats/playercareerbycollegerollup?LeagueID=10](https://stats.wnba.com/stats/playercareerbycollegerollup?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `region` | character | Region label. |
| `seed` | character | Region seed slot of the college in the stats API's career-by-college rollup grid. |
| `college` | character | College or school attended. |
| `players` | character | Nested list of per-player box scores. |
| `gp` | integer | Games played. |
| `min` | integer | Minutes played. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | character | Free throws made. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | character | Offensive rebounds. |
| `dreb` | character | Defensive rebounds. |
| `reb` | character | Total rebounds. |
| `ast` | character | Assists. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `tov` | character | Turnovers. |
| `pf` | character | Personal fouls. |
| `pts` | character | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playercareerbycollegerollup(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playercareerstats`

GET /stats/playercareerstats

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercareerstats`

**Valid URL:** [https://stats.wnba.com/stats/playercareerstats?LeagueID=10](https://stats.wnba.com/stats/playercareerstats?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode36` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playercareerstats(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playercompare`

GET /stats/playercompare

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercompare`

**Valid URL:** [https://stats.wnba.com/stats/playercompare?LeagueID=10](https://stats.wnba.com/stats/playercompare?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerIDList` | `player_id_list` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerIDList` | `vs_player_id_list` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `description` | character | Long-form description text. |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playercompare(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerdashboardbyclutch`

GET /stats/playerdashboardbyclutch

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerdashboardbyclutch`

**Valid URL:** [https://stats.wnba.com/stats/playerdashboardbyclutch?LeagueID=10](https://stats.wnba.com/stats/playerdashboardbyclutch?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | integer | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | integer | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | numeric | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | integer | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | integer | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | integer | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | integer | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `team_count` | integer | NBA or WNBA Stats value for team count in the playerdashboardbyclutch result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashboardbyclutch(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerdashboardbygamesplits`

GET /stats/playerdashboardbygamesplits

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerdashboardbygamesplits`

**Valid URL:** [https://stats.wnba.com/stats/playerdashboardbygamesplits?LeagueID=10](https://stats.wnba.com/stats/playerdashboardbygamesplits?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | integer | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | integer | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | numeric | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | integer | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | integer | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | integer | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | integer | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `team_count` | integer | NBA or WNBA Stats value for team count in the playerdashboardbygamesplits result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashboardbygamesplits(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerdashboardbygeneralsplits`

GET /stats/playerdashboardbygeneralsplits

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerdashboardbygeneralsplits`

**Valid URL:** [https://stats.wnba.com/stats/playerdashboardbygeneralsplits?LeagueID=10](https://stats.wnba.com/stats/playerdashboardbygeneralsplits?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | integer | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | integer | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | numeric | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | integer | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | integer | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | integer | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | integer | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `team_count` | integer | NBA or WNBA Stats value for team count in the playerdashboardbygeneralsplits result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashboardbygeneralsplits(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerdashboardbylastngames`

GET /stats/playerdashboardbylastngames

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerdashboardbylastngames`

**Valid URL:** [https://stats.wnba.com/stats/playerdashboardbylastngames?LeagueID=10](https://stats.wnba.com/stats/playerdashboardbylastngames?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | integer | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | integer | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | numeric | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | integer | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | integer | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | integer | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | integer | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `team_count` | integer | NBA or WNBA Stats value for team count in the playerdashboardbylastngames result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashboardbylastngames(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerdashboardbyopponent`

GET /stats/playerdashboardbyopponent

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerdashboardbyopponent`

**Valid URL:** [https://stats.wnba.com/stats/playerdashboardbyopponent?LeagueID=10](https://stats.wnba.com/stats/playerdashboardbyopponent?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from` |  |  | `Y` |  |
| `DateTo` | `date_to` |  |  | `Y` |  |
| `GameSegment` | `game_segment` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome` |  |  | `Y` |  |
| `PORound` | `po_round` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `ShotClockRange` | `shot_clock_range` |  |  | `Y` |  |
| `VsConference` | `vs_conference` |  |  | `Y` |  |
| `VsDivision` | `vs_division` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |
| `dd2` | integer | Double-doubles recorded over the split. |
| `td3` | integer | Triple-doubles recorded over the split. |
| `wnba_fantasy_pts` | numeric | Fantasy points under the WNBA's fantasy scoring formula. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |
| `nba_fantasy_pts_rank` | integer | League rank of the row's NBA fantasy points (league scoring formula) for the season and split. |
| `dd2_rank` | integer | League rank of the row's double-doubles for the season and split. |
| `td3_rank` | integer | League rank of the row's triple-doubles for the season and split. |
| `wnba_fantasy_pts_rank` | integer | League rank of the row's WNBA fantasy points (league scoring formula) for the season and split. |
| `team_count` | integer | Number of distinct teams aggregated into the split row. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashboardbyopponent(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerdashboardbyshootingsplits`

GET /stats/playerdashboardbyshootingsplits

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerdashboardbyshootingsplits`

**Valid URL:** [https://stats.wnba.com/stats/playerdashboardbyshootingsplits?LeagueID=10](https://stats.wnba.com/stats/playerdashboardbyshootingsplits?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | integer | Three-point field goals made. |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `blka` | integer | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pct_ast_2pm` | numeric | Percentage share of assists 2pm for the requested NBA or WNBA Stats split. |
| `pct_uast_2pm` | numeric | Percentage share of uast 2pm for the requested NBA or WNBA Stats split. |
| `pct_ast_3pm` | numeric | Percentage share of assists 3pm for the requested NBA or WNBA Stats split. |
| `pct_uast_3pm` | numeric | Percentage share of uast 3pm for the requested NBA or WNBA Stats split. |
| `pct_ast_fgm` | numeric | Percentage share of assists fgm for the requested NBA or WNBA Stats split. |
| `pct_uast_fgm` | numeric | Percentage share of uast fgm for the requested NBA or WNBA Stats split. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `efg_pct_rank` | integer | Rank for effective field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_ast_2pm_rank` | integer | Rank for percentage assists 2pm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_uast_2pm_rank` | integer | Rank for percentage uast 2pm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_ast_3pm_rank` | integer | Rank for percentage assists 3pm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_uast_3pm_rank` | integer | Rank for percentage uast 3pm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_ast_fgm_rank` | integer | Rank for percentage assists fgm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_uast_fgm_rank` | integer | Rank for percentage uast fgm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashboardbyshootingsplits(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerdashboardbyteamperformance`

GET /stats/playerdashboardbyteamperformance

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerdashboardbyteamperformance`

**Valid URL:** [https://stats.wnba.com/stats/playerdashboardbyteamperformance?LeagueID=10](https://stats.wnba.com/stats/playerdashboardbyteamperformance?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value_order` | integer | Sort order assigned to the grouping value in NBA or WNBA Stats dashboards. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `group_value_2` | character | Secondary grouping value for dashboards that return paired split dimensions. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | integer | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | integer | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | numeric | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | integer | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | integer | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | integer | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | integer | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `team_count` | integer | NBA or WNBA Stats value for team count in the playerdashboardbyteamperformance result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashboardbyteamperformance(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerdashboardbyyearoveryear`

GET /stats/playerdashboardbyyearoveryear

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerdashboardbyyearoveryear`

**Valid URL:** [https://stats.wnba.com/stats/playerdashboardbyyearoveryear?LeagueID=10](https://stats.wnba.com/stats/playerdashboardbyyearoveryear?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `max_game_date` | character | Date or timestamp for maximum game date in the NBA or WNBA Stats result set. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | integer | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | integer | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | numeric | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | integer | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | integer | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | integer | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | integer | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `team_count` | integer | NBA or WNBA Stats value for team count in the playerdashboardbyyearoveryear result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashboardbyyearoveryear(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerdashptshotdefend`

GET /stats/playerdashptshotdefend

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerdashptshotdefend`

**Valid URL:** [https://stats.wnba.com/stats/playerdashptshotdefend?LeagueID=10](https://stats.wnba.com/stats/playerdashptshotdefend?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `matchupid` | integer | Stats API identifier for matchupid associated with this NBA or WNBA Stats row. |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `defense_category` | character | NBA or WNBA Stats value for defense category in the playerdashptshotdefend result set. |
| `freq` | numeric | NBA or WNBA Stats value for freq in the playerdashptshotdefend result set. |
| `d_fgm` | numeric | Shooting metric for d fgm in the requested NBA or WNBA Stats split. |
| `d_fga` | numeric | Shooting metric for d fga in the requested NBA or WNBA Stats split. |
| `d_fg_pct` | numeric | Percentage or rate for d field goals percentage in the requested NBA or WNBA Stats split. |
| `normal_fg_pct` | numeric | Percentage or rate for normal field goals percentage in the requested NBA or WNBA Stats split. |
| `pct_plusminus` | numeric | Percentage share of plusminus for the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashptshotdefend(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerestimatedmetrics`

GET /stats/playerestimatedmetrics

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerestimatedmetrics`

**Valid URL:** [https://stats.wnba.com/stats/playerestimatedmetrics?LeagueID=10](https://stats.wnba.com/stats/playerestimatedmetrics?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `e_off_rating` | numeric | Estimated offensive rating for the requested NBA or WNBA Stats split. |
| `e_def_rating` | numeric | Estimated defensive rating for the requested NBA or WNBA Stats split. |
| `e_net_rating` | numeric | Estimated net rating for the requested NBA or WNBA Stats split. |
| `e_ast_ratio` | numeric | Estimated assist ratio for the requested NBA or WNBA Stats split. |
| `e_oreb_pct` | numeric | Estimated offensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_dreb_pct` | numeric | Estimated defensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_reb_pct` | numeric | Estimated rebound percentage for the requested NBA or WNBA Stats split. |
| `e_tov_pct` | numeric | Estimated turnovers percentage for the requested NBA or WNBA Stats split. |
| `e_usg_pct` | numeric | Estimated usage percentage for the requested NBA or WNBA Stats split. |
| `e_pace` | numeric | Estimated pace for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_off_rating_rank` | integer | Rank for e offensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_def_rating_rank` | integer | Rank for e defensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_net_rating_rank` | integer | Rank for e net rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_ast_ratio_rank` | integer | Rank for e assists ratio within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_oreb_pct_rank` | integer | Rank for e offensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_dreb_pct_rank` | integer | Rank for e defensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_reb_pct_rank` | integer | Rank for e rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_tov_pct_rank` | integer | Rank for e turnovers percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_usg_pct_rank` | integer | Rank for e usage percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_pace_rank` | integer | Rank for e pace within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerestimatedmetrics(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerfantasyprofile`

GET /stats/playerfantasyprofile

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerfantasyprofile`

**Valid URL:** [https://stats.wnba.com/stats/playerfantasyprofile?LeagueID=10](https://stats.wnba.com/stats/playerfantasyprofile?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `dd2` | integer | Double-doubles recorded over the split. |
| `td3` | integer | Triple-doubles recorded over the split. |
| `fan_duel_pts` | numeric | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerfantasyprofile(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerfantasyprofilebargraph`

GET /stats/playerfantasyprofilebargraph

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerfantasyprofilebargraph`

**Valid URL:** [https://stats.wnba.com/stats/playerfantasyprofilebargraph?LeagueID=10](https://stats.wnba.com/stats/playerfantasyprofilebargraph?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `fan_duel_pts` | numeric | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `fg3m` | numeric | Three-point field goals made. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerfantasyprofilebargraph(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playergamelog`

GET /stats/playergamelog

**Endpoint URL:** `GET https://stats.wnba.com/stats/playergamelog`

**Valid URL:** [https://stats.wnba.com/stats/playergamelog?LeagueID=10](https://stats.wnba.com/stats/playergamelog?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `player_id` | integer | Unique player identifier. |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `min` | integer | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | integer | Three-point field goals made. |
| `fg3a` | integer | Three-point field goal attempts. |
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
| `tov` | integer | Turnovers. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |
| `video_available` | integer | Video available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playergamelog(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playergamelogs`

GET /stats/playergamelogs

**Endpoint URL:** `GET https://stats.wnba.com/stats/playergamelogs`

**Valid URL:** [https://stats.wnba.com/stats/playergamelogs?LeagueID=10](https://stats.wnba.com/stats/playergamelogs?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_player_game_logs_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OppTeamID` | `oppteamid` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | integer | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | integer | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | numeric | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | integer | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | integer | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | integer | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | integer | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `available_flag` | integer | Flag indicating whether the requested NBA or WNBA Stats video or data asset is available. |
| `min_sec` | character | Minutes and seconds played, formatted as a game-clock duration string. |
| `team_count` | integer | NBA or WNBA Stats value for team count in the playergamelogs result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playergamelogs(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playergamestreakfinder`

GET /stats/playergamestreakfinder

**Endpoint URL:** `GET https://stats.wnba.com/stats/playergamestreakfinder`

**Valid URL:** [https://stats.wnba.com/stats/playergamestreakfinder?LeagueID=10](https://stats.wnba.com/stats/playergamestreakfinder?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ActiveStreaksOnly` | `active_streaks_only_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftNumber` | `draft_number_nullable` |  |  | `Y` |  |
| `DraftRound` | `draft_round_nullable` |  |  | `Y` |  |
| `DraftTeamID` | `draft_team_id_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `EqAST` | `eq_ast_nullable` |  |  | `Y` |  |
| `EqBLK` | `eq_blk_nullable` |  |  | `Y` |  |
| `EqDD` | `eq_dd_nullable` |  |  | `Y` |  |
| `EqDREB` | `eq_dreb_nullable` |  |  | `Y` |  |
| `EqFG3A` | `eq_fg3a_nullable` |  |  | `Y` |  |
| `EqFG3M` | `eq_fg3m_nullable` |  |  | `Y` |  |
| `EqFG3_PCT` | `eq_fg3_pct_nullable` |  |  | `Y` |  |
| `EqFGA` | `eq_fga_nullable` |  |  | `Y` |  |
| `EqFGM` | `eq_fgm_nullable` |  |  | `Y` |  |
| `EqFG_PCT` | `eq_fg_pct_nullable` |  |  | `Y` |  |
| `EqFTA` | `eq_fta_nullable` |  |  | `Y` |  |
| `EqFTM` | `eq_ftm_nullable` |  |  | `Y` |  |
| `EqFT_PCT` | `eq_ft_pct_nullable` |  |  | `Y` |  |
| `EqMINUTES` | `eq_minutes_nullable` |  |  | `Y` |  |
| `EqOREB` | `eq_oreb_nullable` |  |  | `Y` |  |
| `EqPF` | `eq_pf_nullable` |  |  | `Y` |  |
| `EqPTS` | `eq_pts_nullable` |  |  | `Y` |  |
| `EqREB` | `eq_reb_nullable` |  |  | `Y` |  |
| `EqSTL` | `eq_stl_nullable` |  |  | `Y` |  |
| `EqTD` | `eq_td_nullable` |  |  | `Y` |  |
| `EqTOV` | `eq_tov_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GtAST` | `gt_ast_nullable` |  |  | `Y` |  |
| `GtBLK` | `gt_blk_nullable` |  |  | `Y` |  |
| `GtDD` | `gt_dd_nullable` |  |  | `Y` |  |
| `GtDREB` | `gt_dreb_nullable` |  |  | `Y` |  |
| `GtFG3A` | `gt_fg3a_nullable` |  |  | `Y` |  |
| `GtFG3M` | `gt_fg3m_nullable` |  |  | `Y` |  |
| `GtFG3_PCT` | `gt_fg3_pct_nullable` |  |  | `Y` |  |
| `GtFGA` | `gt_fga_nullable` |  |  | `Y` |  |
| `GtFGM` | `gt_fgm_nullable` |  |  | `Y` |  |
| `GtFG_PCT` | `gt_fg_pct_nullable` |  |  | `Y` |  |
| `GtFTA` | `gt_fta_nullable` |  |  | `Y` |  |
| `GtFTM` | `gt_ftm_nullable` |  |  | `Y` |  |
| `GtFT_PCT` | `gt_ft_pct_nullable` |  |  | `Y` |  |
| `GtMINUTES` | `gt_minutes_nullable` |  |  | `Y` |  |
| `GtOREB` | `gt_oreb_nullable` |  |  | `Y` |  |
| `GtPF` | `gt_pf_nullable` |  |  | `Y` |  |
| `GtPTS` | `gt_pts_nullable` |  |  | `Y` |  |
| `GtREB` | `gt_reb_nullable` |  |  | `Y` |  |
| `GtSTL` | `gt_stl_nullable` |  |  | `Y` |  |
| `GtTD` | `gt_td_nullable` |  |  | `Y` |  |
| `GtTOV` | `gt_tov_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `LtAST` | `lt_ast_nullable` |  |  | `Y` |  |
| `LtBLK` | `lt_blk_nullable` |  |  | `Y` |  |
| `LtDD` | `lt_dd_nullable` |  |  | `Y` |  |
| `LtDREB` | `lt_dreb_nullable` |  |  | `Y` |  |
| `LtFG3A` | `lt_fg3a_nullable` |  |  | `Y` |  |
| `LtFG3M` | `lt_fg3m_nullable` |  |  | `Y` |  |
| `LtFG3_PCT` | `lt_fg3_pct_nullable` |  |  | `Y` |  |
| `LtFGA` | `lt_fga_nullable` |  |  | `Y` |  |
| `LtFGM` | `lt_fgm_nullable` |  |  | `Y` |  |
| `LtFG_PCT` | `lt_fg_pct_nullable` |  |  | `Y` |  |
| `LtFTA` | `lt_fta_nullable` |  |  | `Y` |  |
| `LtFTM` | `lt_ftm_nullable` |  |  | `Y` |  |
| `LtFT_PCT` | `lt_ft_pct_nullable` |  |  | `Y` |  |
| `LtMINUTES` | `lt_minutes_nullable` |  |  | `Y` |  |
| `LtOREB` | `lt_oreb_nullable` |  |  | `Y` |  |
| `LtPF` | `lt_pf_nullable` |  |  | `Y` |  |
| `LtPTS` | `lt_pts_nullable` |  |  | `Y` |  |
| `LtREB` | `lt_reb_nullable` |  |  | `Y` |  |
| `LtSTL` | `lt_stl_nullable` |  |  | `Y` |  |
| `LtTD` | `lt_td_nullable` |  |  | `Y` |  |
| `LtTOV` | `lt_tov_nullable` |  |  | `Y` |  |
| `MinGames` | `min_games_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |
| `YearsExperience` | `years_experience_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `player_id` | integer | Unique player identifier. |
| `gamestreak` | integer | NBA or WNBA Stats value for gamestreak in the playergamestreakfinder result set. |
| `startdate` | character | Date or timestamp for startdate in the NBA or WNBA Stats result set. |
| `enddate` | character | Date or timestamp for enddate in the NBA or WNBA Stats result set. |
| `activestreak` | integer | NBA or WNBA Stats value for activestreak in the playergamestreakfinder result set. |
| `numseasons` | integer | NBA or WNBA Stats value for numseasons in the playergamestreakfinder result set. |
| `lastseason` | character | NBA or WNBA Stats value for lastseason in the playergamestreakfinder result set. |
| `firstseason` | character | NBA or WNBA Stats value for firstseason in the playergamestreakfinder result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playergamestreakfinder(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerindex`

GET /stats/playerindex

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerindex`

**Valid URL:** [https://stats.wnba.com/stats/playerindex?LeagueID=10](https://stats.wnba.com/stats/playerindex?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Active` | `active_nullable` |  |  | `Y` |  |
| `AllStar` | `allstar_nullable` |  |  | `Y` |  |
| `College` | `college_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftRound` | `draft_round_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `Historical` | `historical_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_last_name` | character | Participant last name. |
| `player_first_name` | character | Participant first name. |
| `player_slug` | character | URL-safe player identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `is_defunct` | integer | Flag indicating is defunct for the requested NBA or WNBA Stats context. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `jersey_number` | character | Jersey number. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `college` | character | College or school attended. |
| `country` | character | Country (full name or code). |
| `draft_year` | integer | Draft year (4-digit). |
| `draft_round` | integer | Round of the draft selection. |
| `draft_number` | integer | The number pick that was used to select a given player. |
| `roster_status` | numeric | Payroll table the row came from: Active, IL, or Retained Salary. |
| `from_year` | character | First season. |
| `to_year` | character | Most recent season. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `stats_timeframe` | character | Time value for stats timeframe in the NBA or WNBA Stats result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerindex(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playernextngames`

GET /stats/playernextngames

**Endpoint URL:** `GET https://stats.wnba.com/stats/playernextngames`

**Valid URL:** [https://stats.wnba.com/stats/playernextngames?LeagueID=10](https://stats.wnba.com/stats/playernextngames?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `NumberOfGames` | `number_of_games` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season_all` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `home_team_name` | character | Home team name. |
| `visitor_team_name` | character | Full name of the visiting team in the upcoming game. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `visitor_team_abbreviation` | character | Abbreviation of the visiting team in the upcoming game. |
| `home_team_nickname` | character | Home team nickname label; `team_detail = TRUE` only. |
| `visitor_team_nickname` | character | Nickname of the visiting team in the upcoming game. |
| `game_time` | character | Game start time. |
| `home_wl` | character | Home team's win-loss record entering the upcoming game. |
| `visitor_wl` | character | Visiting team's win-loss record entering the upcoming game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playernextngames(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playerprofilev2`

GET /stats/playerprofilev2

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerprofilev2`

**Valid URL:** [https://stats.wnba.com/stats/playerprofilev2?LeagueID=10](https://stats.wnba.com/stats/playerprofilev2?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode36` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerprofilev2(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playervsplayer`

GET /stats/playervsplayer

**Endpoint URL:** `GET https://stats.wnba.com/stats/playervsplayer`

**Valid URL:** [https://stats.wnba.com/stats/playervsplayer?LeagueID=10](https://stats.wnba.com/stats/playervsplayer?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerID` | `vs_player_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playervsplayer(league_id='10')
```

_Last validated n/a._

## `wnba_stats_scheduleleaguev2`

GET /stats/scheduleleaguev2

**Endpoint URL:** `GET https://stats.wnba.com/stats/scheduleleaguev2`

**Valid URL:** [https://stats.wnba.com/stats/scheduleleaguev2?LeagueID=10](https://stats.wnba.com/stats/scheduleleaguev2?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `arena_city` | character | City hosting the game's arena. |
| `arena_name` | character | Name of the arena hosting the game. |
| `arena_state` | character | State or province of the game's arena (blank for international sites). |
| `away_team_city` | character | City of the away team. |
| `away_team_id` | integer | stats.nba.com / stats.wnba.com team id of the away team. |
| `away_team_losses` | integer | Away team's losses entering the game. |
| `away_team_name` | character | Away team nickname (e.g. Fever). |
| `away_team_score` | integer | Away team's final score, 0 before the game is played. |
| `away_team_seed` | integer | Away team's playoff seed, 0 outside the postseason. |
| `away_team_slug` | character | URL slug of the away team. |
| `away_team_time` | character | Scheduled tip-off in the away team's local time zone. |
| `away_team_tricode` | character | Three-letter abbreviation of the away team. |
| `away_team_wins` | integer | Away team's wins entering the game. |
| `branch_link` | character | Deep link for the game, blank when not published. |
| `day` | character | Three-letter day of week of the game date. |
| `game_code` | character | Provider game code, `YYYYMMDD/AWYHOM`. |
| `game_date` | character | Game date as served by the schedule feed (`MM/DD/YYYY HH:MM:SS`). |
| `game_date_est` | character | Game date at midnight Eastern, ISO-8601. |
| `game_date_time_est` | character | Scheduled tip-off in Eastern time, ISO-8601. |
| `game_date_time_utc` | character | Scheduled tip-off in UTC, ISO-8601. The timestamp to reduce to a calendar date. |
| `game_date_utc` | character | Game date at midnight UTC, ISO-8601. |
| `game_id` | character | Unique stats.nba.com / stats.wnba.com game id; its 3rd character encodes the season type. |
| `game_label` | character | Human-readable round or event label (e.g. Preseason, Conf. Finals). |
| `game_sequence` | integer | Ordinal of the game within its date. |
| `game_status` | integer | Game status code: 1 scheduled, 2 in progress, 3 final. |
| `game_status_text` | character | Human-readable game status (e.g. Final, 7:00 pm ET). |
| `game_sub_label` | character | Secondary event label (e.g. NBA Abu Dhabi Game). |
| `game_subtype` | character | Game subtype tag (e.g. Global Games), blank for standard games. |
| `game_time_est` | character | Scheduled tip-off time of day, Eastern. |
| `game_time_utc` | character | Scheduled tip-off time of day, UTC. |
| `home_team_city` | character | City of the home team. |
| `home_team_id` | integer | stats.nba.com / stats.wnba.com team id of the home team. |
| `home_team_losses` | integer | Home team's losses entering the game. |
| `home_team_name` | character | Home team nickname (e.g. Liberty). |
| `home_team_score` | integer | Home team's final score, 0 before the game is played. |
| `home_team_seed` | integer | Home team's playoff seed, 0 outside the postseason. |
| `home_team_slug` | character | URL slug of the home team. |
| `home_team_time` | character | Scheduled tip-off in the home team's local time zone. |
| `home_team_tricode` | character | Three-letter abbreviation of the home team. |
| `home_team_wins` | integer | Home team's wins entering the game. |
| `if_necessary` | character | Whether the game is a conditional series game that may not be played. |
| `is_neutral` | logical | Whether the game is played at a neutral site. |
| `league_id` | character | League id of the schedule: '00' NBA, '10' WNBA, '20' G-League. |
| `month_num` | integer | Calendar month number of the game date. |
| `postponed_status` | character | Postponement status; 'N' when the game is on as scheduled. |
| `season` | character | Season the schedule covers, as published by the feed ('2025-26' for the NBA, '2026' for the WNBA). |
| `season_type_description` | character | Season type label derived from season_type_id: Pre-Season, Regular Season, All-Star, Playoffs, Play-In Game. |
| `season_type_id` | character | Season type digit, the 3rd character of game_id. |
| `series_game_number` | character | Game number within a playoff series, blank outside a series. |
| `series_text` | character | Series context line (e.g. series tied 1-1), blank when not applicable. |
| `week_name` | character | Name of the schedule week, blank outside the regular season. |
| `week_number` | integer | Schedule week number, 0 outside the regular season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_scheduleleaguev2(league_id='10')
```

_Last validated n/a._

## `wnba_stats_scheduleleaguev2int`

GET /stats/scheduleleaguev2int

**Endpoint URL:** `GET https://stats.wnba.com/stats/scheduleleaguev2int`

**Valid URL:** [https://stats.wnba.com/stats/scheduleleaguev2int?LeagueID=10](https://stats.wnba.com/stats/scheduleleaguev2int?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `arena_city` | character | City hosting the game's arena. |
| `arena_name` | character | Name of the arena hosting the game. |
| `arena_state` | character | State or province of the game's arena (blank for international sites). |
| `away_team_city` | character | City of the away team. |
| `away_team_id` | integer | stats.nba.com / stats.wnba.com team id of the away team. |
| `away_team_losses` | integer | Away team's losses entering the game. |
| `away_team_name` | character | Away team nickname (e.g. Fever). |
| `away_team_score` | integer | Away team's final score, 0 before the game is played. |
| `away_team_seed` | integer | Away team's playoff seed, 0 outside the postseason. |
| `away_team_slug` | character | URL slug of the away team. |
| `away_team_time` | character | Scheduled tip-off in the away team's local time zone. |
| `away_team_tricode` | character | Three-letter abbreviation of the away team. |
| `away_team_wins` | integer | Away team's wins entering the game. |
| `branch_link` | character | Deep link for the game, blank when not published. |
| `day` | character | Three-letter day of week of the game date. |
| `game_code` | character | Provider game code, `YYYYMMDD/AWYHOM`. |
| `game_date` | character | Game date as served by the schedule feed (`MM/DD/YYYY HH:MM:SS`). |
| `game_date_est` | character | Game date at midnight Eastern, ISO-8601. |
| `game_date_time_est` | character | Scheduled tip-off in Eastern time, ISO-8601. |
| `game_date_time_utc` | character | Scheduled tip-off in UTC, ISO-8601. The timestamp to reduce to a calendar date. |
| `game_date_utc` | character | Game date at midnight UTC, ISO-8601. |
| `game_id` | character | Unique stats.nba.com / stats.wnba.com game id; its 3rd character encodes the season type. |
| `game_label` | character | Human-readable round or event label (e.g. Preseason, Conf. Finals). |
| `game_sequence` | integer | Ordinal of the game within its date. |
| `game_status` | integer | Game status code: 1 scheduled, 2 in progress, 3 final. |
| `game_status_text` | character | Human-readable game status (e.g. Final, 7:00 pm ET). |
| `game_sub_label` | character | Secondary event label (e.g. NBA Abu Dhabi Game). |
| `game_subtype` | character | Game subtype tag (e.g. Global Games), blank for standard games. |
| `game_time_est` | character | Scheduled tip-off time of day, Eastern. |
| `game_time_utc` | character | Scheduled tip-off time of day, UTC. |
| `home_team_city` | character | City of the home team. |
| `home_team_id` | integer | stats.nba.com / stats.wnba.com team id of the home team. |
| `home_team_losses` | integer | Home team's losses entering the game. |
| `home_team_name` | character | Home team nickname (e.g. Liberty). |
| `home_team_score` | integer | Home team's final score, 0 before the game is played. |
| `home_team_seed` | integer | Home team's playoff seed, 0 outside the postseason. |
| `home_team_slug` | character | URL slug of the home team. |
| `home_team_time` | character | Scheduled tip-off in the home team's local time zone. |
| `home_team_tricode` | character | Three-letter abbreviation of the home team. |
| `home_team_wins` | integer | Home team's wins entering the game. |
| `if_necessary` | character | Whether the game is a conditional series game that may not be played. |
| `is_neutral` | logical | Whether the game is played at a neutral site. |
| `league_id` | character | League id of the schedule: '00' NBA, '10' WNBA, '20' G-League. |
| `month_num` | integer | Calendar month number of the game date. |
| `postponed_status` | character | Postponement status; 'N' when the game is on as scheduled. |
| `season` | character | Season the schedule covers, as published by the feed ('2025-26' for the NBA, '2026' for the WNBA). |
| `season_type_description` | character | Season type label derived from season_type_id: Pre-Season, Regular Season, All-Star, Playoffs, Play-In Game. |
| `season_type_id` | character | Season type digit, the 3rd character of game_id. |
| `series_game_number` | character | Game number within a playoff series, blank outside a series. |
| `series_text` | character | Series context line (e.g. series tied 1-1), blank when not applicable. |
| `week_name` | character | Name of the schedule week, blank outside the regular season. |
| `week_number` | integer | Schedule week number, 0 outside the regular season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_scheduleleaguev2int(league_id='10')
```

_Last validated n/a._

## `wnba_stats_scoreboardv2`

GET /stats/scoreboardv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/scoreboardv2`

**Valid URL:** [https://stats.wnba.com/stats/scoreboardv2?LeagueID=10](https://stats.wnba.com/stats/scoreboardv2?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DayOffset` | `day_offset` |  |  | `Y` |  |
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_date_est` | character | Game date est. |
| `game_sequence` | character | Game sequence. |
| `game_id` | integer | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city_name` | character | Team city name. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_wins_losses` | character | Team wins losses. |
| `pts_qtr1` | character | Pts qtr1. |
| `pts_qtr2` | character | Pts qtr2. |
| `pts_qtr3` | character | Pts qtr3. |
| `pts_qtr4` | character | Pts qtr4. |
| `pts_ot1` | character | Pts ot1. |
| `pts_ot2` | character | Points scored by the team in overtime period 2. |
| `pts_ot3` | character | Points scored by the team in overtime period 3. |
| `pts_ot4` | character | Points scored by the team in overtime period 4. |
| `pts_ot5` | character | Points scored by the team in overtime period 5. |
| `pts_ot6` | character | Points scored by the team in overtime period 6. |
| `pts_ot7` | character | Points scored by the team in overtime period 7. |
| `pts_ot8` | character | Points scored by the team in overtime period 8. |
| `pts_ot9` | character | Points scored by the team in overtime period 9. |
| `pts_ot10` | character | Points scored by the team in overtime period 10. |
| `pts` | character | Points scored. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ast` | character | Assists. |
| `reb` | character | Total rebounds. |
| `tov` | character | Turnovers. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_scoreboardv2(league_id='10')
```

_Last validated n/a._

## `wnba_stats_scoreboardv3`

GET /stats/scoreboardv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/scoreboardv3`

**Valid URL:** [https://stats.wnba.com/stats/scoreboardv3?LeagueID=10](https://stats.wnba.com/stats/scoreboardv3?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `awayteam_inbonus` | character | Whether the away team is currently in the bonus (penalty) foul situation. |
| `awayteam_losses` | integer | Away team's loss total entering the game. |
| `awayteam_score` | integer | Current or final points scored by the away team. |
| `awayteam_seed` | integer | Playoff seed of the away team, populated for postseason games. |
| `awayteam_teamcity` | character | City name of the away team. |
| `awayteam_teamid` | integer | Team identifier of the away team from the league's stats API. |
| `awayteam_teamname` | character | Nickname of the away team. |
| `awayteam_teamslug` | character | URL-friendly slug for the away team's name. |
| `awayteam_teamtricode` | character | Three-letter abbreviation of the away team. |
| `awayteam_timeoutsremaining` | integer | Timeouts the away team has remaining. |
| `awayteam_wins` | integer | Away team's win total entering the game. |
| `gameclock` | character | Current game clock display for a live game. |
| `gamecode` | character | Gamecode. |
| `gamedate` | character | Game date as parsed from the source feed. |
| `gameet` | character | Scheduled game start time in US Eastern time. |
| `gameid` | character | Unique 10-character game identifier from the league's stats API. |
| `gamelabel` | character | Display label for the game (e.g. a playoff series or event name). |
| `gameleaders_awayleaders_assists` | integer | Assist total of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_jerseynum` | character | Jersey number of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_name` | character | Name of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_personid` | integer | Stats API player id of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_playerslug` | character | URL name slug of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_points` | integer | Point total of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_position` | character | Position of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_rebounds` | integer | Rebound total of the away team's in-game statistical leader. |
| `gameleaders_awayleaders_teamtricode` | character | Team tricode of the away team's in-game statistical leader. |
| `gameleaders_homeleaders_assists` | integer | Assist total of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_jerseynum` | character | Jersey number of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_name` | character | Name of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_personid` | integer | Stats API player id of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_playerslug` | character | URL name slug of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_points` | integer | Point total of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_position` | character | Position of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_rebounds` | integer | Rebound total of the home team's in-game statistical leader. |
| `gameleaders_homeleaders_teamtricode` | character | Team tricode of the home team's in-game statistical leader. |
| `gamestatus` | integer | Numeric game status code (1 = scheduled, 2 = in progress, 3 = final). |
| `gamestatustext` | character | Human-readable game status (e.g. "Final", "7:00 pm ET"). |
| `gamesublabel` | character | Secondary display label for the game (e.g. game number within a series). |
| `gamesubtype` | character | Subtype code for the game as reported by the stats API (e.g. in-season tournament flags). |
| `gametimeutc` | character | Scheduled game start time in UTC. |
| `hometeam_inbonus` | character | Whether the home team is currently in the bonus (penalty) foul situation. |
| `hometeam_losses` | integer | Home team's loss total entering the game. |
| `hometeam_score` | integer | Current or final points scored by the home team. |
| `hometeam_seed` | integer | Playoff seed of the home team, populated for postseason games. |
| `hometeam_teamcity` | character | City name of the home team. |
| `hometeam_teamid` | integer | Team identifier of the home team from the league's stats API. |
| `hometeam_teamname` | character | Nickname of the home team. |
| `hometeam_teamslug` | character | URL-friendly slug for the home team's name. |
| `hometeam_teamtricode` | character | Three-letter abbreviation of the home team. |
| `hometeam_timeoutsremaining` | integer | Timeouts the home team has remaining. |
| `hometeam_wins` | integer | Home team's win total entering the game. |
| `ifnecessary` | logical | Whether the game is an if-necessary playoff series game. |
| `isneutral` | logical | Whether the game is played at a neutral site. |
| `leagueid` | character | League identifier from the stats API ("00" = NBA, "10" = WNBA). |
| `leaguename` | character | Display name of the league. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `porounddesc` | character | Playoff round description (e.g. Conference Finals). |
| `regulationperiods` | integer | Number of regulation periods for the game (4). |
| `seriesconference` | character | Conference of the playoff series the game belongs to. |
| `seriesgamenumber` | character | Game number within the playoff series. |
| `seriestext` | character | Display text summarizing the series state (e.g. "BOS leads 2-1"). |
| `teamleaders_awayleaders_assists` | numeric | Assist total of the away team's season statistical leader. |
| `teamleaders_awayleaders_jerseynum` | character | Jersey number of the away team's season statistical leader. |
| `teamleaders_awayleaders_name` | character | Name of the away team's season statistical leader. |
| `teamleaders_awayleaders_personid` | integer | Stats API player id of the away team's season statistical leader. |
| `teamleaders_awayleaders_playerslug` | character | URL name slug of the away team's season statistical leader. |
| `teamleaders_awayleaders_points` | numeric | Point total of the away team's season statistical leader. |
| `teamleaders_awayleaders_position` | character | Position of the away team's season statistical leader. |
| `teamleaders_awayleaders_rebounds` | numeric | Rebound total of the away team's season statistical leader. |
| `teamleaders_awayleaders_teamtricode` | character | Team tricode of the away team's season statistical leader. |
| `teamleaders_homeleaders_assists` | numeric | Assist total of the home team's season statistical leader. |
| `teamleaders_homeleaders_jerseynum` | character | Jersey number of the home team's season statistical leader. |
| `teamleaders_homeleaders_name` | character | Name of the home team's season statistical leader. |
| `teamleaders_homeleaders_personid` | integer | Stats API player id of the home team's season statistical leader. |
| `teamleaders_homeleaders_playerslug` | character | URL name slug of the home team's season statistical leader. |
| `teamleaders_homeleaders_points` | numeric | Point total of the home team's season statistical leader. |
| `teamleaders_homeleaders_position` | character | Position of the home team's season statistical leader. |
| `teamleaders_homeleaders_rebounds` | numeric | Rebound total of the home team's season statistical leader. |
| `teamleaders_homeleaders_teamtricode` | character | Team tricode of the home team's season statistical leader. |
| `teamleaders_seasonleadersflag` | integer | Flag indicating the team-leaders block carries season-long leaders rather than in-game leaders. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_scoreboardv3(league_id='10')
```

_Last validated n/a._

## `wnba_stats_shotchartdetail`

GET /stats/shotchartdetail

**Endpoint URL:** `GET https://stats.wnba.com/stats/shotchartdetail`

**Valid URL:** [https://stats.wnba.com/stats/shotchartdetail?LeagueID=10](https://stats.wnba.com/stats/shotchartdetail?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `AheadBehind` | `ahead_behind_nullable` |  |  | `Y` |  |
| `ClutchTime` | `clutch_time_nullable` |  |  | `Y` |  |
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ContextMeasure` | `context_measure_simple` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `EndPeriod` | `end_period_nullable` |  |  | `Y` |  |
| `EndRange` | `end_range_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_nullable` |  |  | `Y` |  |
| `PointDiff` | `point_diff_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `RangeType` | `range_type_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `StartPeriod` | `start_period_nullable` |  |  | `Y` |  |
| `StartRange` | `start_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grid_type` | character | Shot chart grid type label returned by the stats API (e.g. "Shot Chart Detail"). |
| `game_id` | character | Unique game identifier. |
| `game_event_id` | character | Unique identifier for game event. |
| `player_id` | character | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `minutes_remaining` | character | Minutes remaining. |
| `seconds_remaining` | character | Seconds remaining in the period. |
| `event_type` | character | Event / play type code (V2 PBP). |
| `action_type` | character | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `shot_type` | character | Shot type label (e.g. 'Jump Shot', 'Layup'). |
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `shot_distance` | character | Shot distance from the basket, in feet. |
| `loc_x` | character | X coordinate on the court (units of inches; 0 = basket center). |
| `loc_y` | character | Y coordinate on the court (units of inches; baseline at 0). |
| `shot_attempted_flag` | character | 1 if a shot was attempted on this event. |
| `shot_made_flag` | character | 1 if the shot was made; 0 if missed. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `htm` | character | Home team abbreviation for the game. |
| `vtm` | character | Visiting team abbreviation for the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_shotchartdetail(league_id='10')
```

_Last validated n/a._

## `wnba_stats_shotchartleaguewide`

GET /stats/shotchartleaguewide

**Endpoint URL:** `GET https://stats.wnba.com/stats/shotchartleaguewide`

**Valid URL:** [https://stats.wnba.com/stats/shotchartleaguewide?LeagueID=10](https://stats.wnba.com/stats/shotchartleaguewide?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grid_type` | character | NBA or WNBA Stats value for grid type in the shotchartleaguewide result set. |
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `fga` | integer | Field goal attempts. |
| `fgm` | integer | Field goals made. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_shotchartleaguewide(league_id='10')
```

_Last validated n/a._

## `wnba_stats_shotchartlineupdetail`

GET /stats/shotchartlineupdetail

**Endpoint URL:** `GET https://stats.wnba.com/stats/shotchartlineupdetail`

**Valid URL:** [https://stats.wnba.com/stats/shotchartlineupdetail?LeagueID=10](https://stats.wnba.com/stats/shotchartlineupdetail?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ContextMeasure` | `context_measure_detailed` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GROUP_ID` | `group_id` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grid_type` | character | Shot chart grid type label returned by the stats API (e.g. "Shot Chart Detail"). |
| `game_id` | character | Unique game identifier. |
| `game_event_id` | character | Unique identifier for game event. |
| `group_id` | character | ESPN group id. |
| `group_name` | character | Group name (conference / division). |
| `player_id` | character | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `minutes_remaining` | character | Minutes remaining. |
| `seconds_remaining` | character | Seconds remaining in the period. |
| `event_type` | character | Event / play type code (V2 PBP). |
| `action_type` | character | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `shot_type` | character | Shot type label (e.g. 'Jump Shot', 'Layup'). |
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `shot_distance` | character | Shot distance from the basket, in feet. |
| `loc_x` | character | X coordinate on the court (units of inches; 0 = basket center). |
| `loc_y` | character | Y coordinate on the court (units of inches; baseline at 0). |
| `shot_attempted_flag` | character | 1 if a shot was attempted on this event. |
| `shot_made_flag` | character | 1 if the shot was made; 0 if missed. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `htm` | character | Home team abbreviation for the game. |
| `vtm` | character | Visiting team abbreviation for the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_shotchartlineupdetail(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashboardbyclutch`

GET /stats/teamdashboardbyclutch

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbyclutch`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbyclutch?LeagueID=10](https://stats.wnba.com/stats/teamdashboardbyclutch?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from` |  |  | `Y` |  |
| `DateTo` | `date_to` |  |  | `Y` |  |
| `GameSegment` | `game_segment` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome` |  |  | `Y` |  |
| `PORound` | `po_round` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `ShotClockRange` | `shot_clock_range` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference` |  |  | `Y` |  |
| `VsDivision` | `vs_division` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbyclutch(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashboardbygamesplits`

GET /stats/teamdashboardbygamesplits

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbygamesplits`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbygamesplits?LeagueID=10](https://stats.wnba.com/stats/teamdashboardbygamesplits?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from` |  |  | `Y` |  |
| `DateTo` | `date_to` |  |  | `Y` |  |
| `GameSegment` | `game_segment` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome` |  |  | `Y` |  |
| `PORound` | `po_round` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `ShotClockRange` | `shot_clock_range` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference` |  |  | `Y` |  |
| `VsDivision` | `vs_division` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbygamesplits(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashboardbygeneralsplits`

GET /stats/teamdashboardbygeneralsplits

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbygeneralsplits`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbygeneralsplits?LeagueID=10](https://stats.wnba.com/stats/teamdashboardbygeneralsplits?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `game_result` | character | Game result for the player's team (`W`/`L`). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbygeneralsplits(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashboardbylastngames`

GET /stats/teamdashboardbylastngames

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbylastngames`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbylastngames?LeagueID=10](https://stats.wnba.com/stats/teamdashboardbylastngames?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from` |  |  | `Y` |  |
| `DateTo` | `date_to` |  |  | `Y` |  |
| `GameSegment` | `game_segment` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome` |  |  | `Y` |  |
| `PORound` | `po_round` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `ShotClockRange` | `shot_clock_range` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference` |  |  | `Y` |  |
| `VsDivision` | `vs_division` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbylastngames(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashboardbyopponent`

GET /stats/teamdashboardbyopponent

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbyopponent`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbyopponent?LeagueID=10](https://stats.wnba.com/stats/teamdashboardbyopponent?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from` |  |  | `Y` |  |
| `DateTo` | `date_to` |  |  | `Y` |  |
| `GameSegment` | `game_segment` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome` |  |  | `Y` |  |
| `PORound` | `po_round` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `ShotClockRange` | `shot_clock_range` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference` |  |  | `Y` |  |
| `VsDivision` | `vs_division` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbyopponent(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashboardbyshootingsplits`

GET /stats/teamdashboardbyshootingsplits

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbyshootingsplits`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbyshootingsplits?LeagueID=10](https://stats.wnba.com/stats/teamdashboardbyshootingsplits?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | integer | Three-point field goals made. |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `blka` | integer | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pct_ast_2pm` | numeric | Percentage share of assists 2pm for the requested NBA or WNBA Stats split. |
| `pct_uast_2pm` | numeric | Percentage share of uast 2pm for the requested NBA or WNBA Stats split. |
| `pct_ast_3pm` | numeric | Percentage share of assists 3pm for the requested NBA or WNBA Stats split. |
| `pct_uast_3pm` | numeric | Percentage share of uast 3pm for the requested NBA or WNBA Stats split. |
| `pct_ast_fgm` | numeric | Percentage share of assists fgm for the requested NBA or WNBA Stats split. |
| `pct_uast_fgm` | numeric | Percentage share of uast fgm for the requested NBA or WNBA Stats split. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `efg_pct_rank` | integer | Rank for effective field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_ast_2pm_rank` | integer | Rank for percentage assists 2pm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_uast_2pm_rank` | integer | Rank for percentage uast 2pm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_ast_3pm_rank` | integer | Rank for percentage assists 3pm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_uast_3pm_rank` | integer | Rank for percentage uast 3pm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_ast_fgm_rank` | integer | Rank for percentage assists fgm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pct_uast_fgm_rank` | integer | Rank for percentage uast fgm within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbyshootingsplits(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashboardbyteamperformance`

GET /stats/teamdashboardbyteamperformance

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbyteamperformance`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbyteamperformance?LeagueID=10](https://stats.wnba.com/stats/teamdashboardbyteamperformance?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from` |  |  | `Y` |  |
| `DateTo` | `date_to` |  |  | `Y` |  |
| `GameSegment` | `game_segment` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome` |  |  | `Y` |  |
| `PORound` | `po_round` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `ShotClockRange` | `shot_clock_range` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference` |  |  | `Y` |  |
| `VsDivision` | `vs_division` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value_order` | integer | Sort order of the split value within its group. |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `group_value_2` | character | Secondary split value for the row when the group uses two dimensions. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbyteamperformance(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashboardbyyearoveryear`

GET /stats/teamdashboardbyyearoveryear

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbyyearoveryear`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbyyearoveryear?LeagueID=10](https://stats.wnba.com/stats/teamdashboardbyyearoveryear?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from` |  |  | `Y` |  |
| `DateTo` | `date_to` |  |  | `Y` |  |
| `GameSegment` | `game_segment` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome` |  |  | `Y` |  |
| `PORound` | `po_round` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `ShotClockRange` | `shot_clock_range` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference` |  |  | `Y` |  |
| `VsDivision` | `vs_division` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbyyearoveryear(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashlineups`

GET /stats/teamdashlineups

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashlineups`

**Valid URL:** [https://stats.wnba.com/stats/teamdashlineups?LeagueID=10](https://stats.wnba.com/stats/teamdashlineups?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GroupQuantity` | `group_quantity` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashlineups(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdetails`

GET /stats/teamdetails

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdetails`

**Valid URL:** [https://stats.wnba.com/stats/teamdetails](https://stats.wnba.com/stats/teamdetails)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `abbreviation` | character | Short abbreviation. |
| `nickname` | character | Team or athlete nickname. |
| `yearfounded` | integer | Year the franchise was founded. |
| `city` | character | Venue city. |
| `arena` | character | Arena. |
| `arenacapacity` | character | Seating capacity of the team's home arena. |
| `owner` | character | Name of the team's owner or ownership group. |
| `generalmanager` | character | Name of the team's general manager. |
| `headcoach` | character | Name of the team's head coach. |
| `dleagueaffiliation` | character | Name of the team's G League (formerly D-League) affiliate. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdetails()
```

_Last validated n/a._

## `wnba_stats_teamestimatedmetrics`

GET /stats/teamestimatedmetrics

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamestimatedmetrics`

**Valid URL:** [https://stats.wnba.com/stats/teamestimatedmetrics?LeagueID=10](https://stats.wnba.com/stats/teamestimatedmetrics?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `e_off_rating` | numeric | Estimated offensive rating for the requested NBA or WNBA Stats split. |
| `e_def_rating` | numeric | Estimated defensive rating for the requested NBA or WNBA Stats split. |
| `e_net_rating` | numeric | Estimated net rating for the requested NBA or WNBA Stats split. |
| `e_pace` | numeric | Estimated pace for the requested NBA or WNBA Stats split. |
| `e_ast_ratio` | numeric | Estimated assist ratio for the requested NBA or WNBA Stats split. |
| `e_oreb_pct` | numeric | Estimated offensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_dreb_pct` | numeric | Estimated defensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_reb_pct` | numeric | Estimated rebound percentage for the requested NBA or WNBA Stats split. |
| `e_tm_tov_pct` | numeric | Estimated team turnover percentage for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_off_rating_rank` | integer | Rank for e offensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_def_rating_rank` | integer | Rank for e defensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_net_rating_rank` | integer | Rank for e net rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_ast_ratio_rank` | integer | Rank for e assists ratio within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_oreb_pct_rank` | integer | Rank for e offensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_dreb_pct_rank` | integer | Rank for e defensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_reb_pct_rank` | integer | Rank for e rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_tm_tov_pct_rank` | integer | Rank for e team turnovers percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_pace_rank` | integer | Rank for e pace within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamestimatedmetrics(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamgamelog`

GET /stats/teamgamelog

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamgamelog`

**Valid URL:** [https://stats.wnba.com/stats/teamgamelog?LeagueID=10](https://stats.wnba.com/stats/teamgamelog?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | integer | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | integer | Three-point field goals made. |
| `fg3a` | integer | Three-point field goal attempts. |
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
| `tov` | integer | Turnovers. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamgamelog(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamgamelogs`

GET /stats/teamgamelogs

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamgamelogs`

**Valid URL:** [https://stats.wnba.com/stats/teamgamelogs?LeagueID=10](https://stats.wnba.com/stats/teamgamelogs?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_player_game_logs_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OppTeamID` | `opp_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `available_flag` | integer | Flag indicating whether the requested NBA or WNBA Stats video or data asset is available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamgamelogs(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teaminfocommon`

GET /stats/teaminfocommon

**Endpoint URL:** `GET https://stats.wnba.com/stats/teaminfocommon`

**Valid URL:** [https://stats.wnba.com/stats/teaminfocommon?LeagueID=10](https://stats.wnba.com/stats/teaminfocommon?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_conference` | character | Conference the team belongs to. |
| `team_division` | character | Division the team belongs to. |
| `team_code` | character | Internal team code. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `pct` | numeric | Win percentage. |
| `conf_rank` | integer | Team's current rank within its conference. |
| `div_rank` | integer | Team's current rank within its division. |
| `min_year` | character | Minimum year queried (echoes `min_year`). |
| `max_year` | character | Maximum year queried (echoes `max_year`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teaminfocommon(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamplayerdashboard`

GET /stats/teamplayerdashboard

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamplayerdashboard`

**Valid URL:** [https://stats.wnba.com/stats/teamplayerdashboard?LeagueID=10](https://stats.wnba.com/stats/teamplayerdashboard?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | integer | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | integer | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | numeric | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | integer | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | integer | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | integer | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | integer | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `team_count` | integer | NBA or WNBA Stats value for team count in the teamplayerdashboard result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamplayerdashboard(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamplayeronoffdetails`

GET /stats/teamplayeronoffdetails

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamplayeronoffdetails`

**Valid URL:** [https://stats.wnba.com/stats/teamplayeronoffdetails?LeagueID=10](https://stats.wnba.com/stats/teamplayeronoffdetails?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `team_id` | character | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `vs_player_id` | character | Stats API identifier for vs player identifier associated with this NBA or WNBA Stats row. |
| `vs_player_name` | character | Display name for vs player name associated with this NBA or WNBA Stats row. |
| `court_status` | character | Indicates whether the compared player was on court or off court for the split row. |
| `gp` | character | Games played. |
| `w` | character | Wins. |
| `l` | character | Losses. |
| `w_pct` | character | Wins percentage (0-1 decimal). |
| `min` | character | Minutes played. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | character | Three-point field goal percentage (0-1). |
| `ftm` | character | Free throws made. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | character | Free throw percentage (0-1). |
| `oreb` | character | Offensive rebounds. |
| `dreb` | character | Defensive rebounds. |
| `reb` | character | Total rebounds. |
| `ast` | character | Assists. |
| `tov` | character | Turnovers. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `blka` | character | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | character | Personal fouls. |
| `pfd` | character | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | character | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | character | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | character | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | character | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | character | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | character | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | character | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | character | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | character | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | character | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | character | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | character | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | character | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | character | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | character | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | character | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | character | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | character | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | character | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | character | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | character | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | character | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | character | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | character | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | character | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamplayeronoffdetails(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamplayeronoffsummary`

GET /stats/teamplayeronoffsummary

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamplayeronoffsummary`

**Valid URL:** [https://stats.wnba.com/stats/teamplayeronoffsummary?LeagueID=10](https://stats.wnba.com/stats/teamplayeronoffsummary?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamplayeronoffsummary(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamvsplayer`

GET /stats/teamvsplayer

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamvsplayer`

**Valid URL:** [https://stats.wnba.com/stats/teamvsplayer?LeagueID=10](https://stats.wnba.com/stats/teamvsplayer?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerID` | `vs_player_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `player_id` | integer | Unique player identifier. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |
| `dd2` | integer | Double-doubles recorded over the split. |
| `td3` | integer | Triple-doubles recorded over the split. |
| `wnba_fantasy_pts` | numeric | Fantasy points under the WNBA's fantasy scoring formula. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |
| `nba_fantasy_pts_rank` | integer | League rank of the row's NBA fantasy points (league scoring formula) for the season and split. |
| `dd2_rank` | integer | League rank of the row's double-doubles for the season and split. |
| `td3_rank` | integer | League rank of the row's triple-doubles for the season and split. |
| `wnba_fantasy_pts_rank` | integer | League rank of the row's WNBA fantasy points (league scoring formula) for the season and split. |
| `team_count` | integer | Number of distinct teams aggregated into the split row. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamvsplayer(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamyearbyyearstats`

GET /stats/teamyearbyyearstats

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamyearbyyearstats`

**Valid URL:** [https://stats.wnba.com/stats/teamyearbyyearstats?LeagueID=10](https://stats.wnba.com/stats/teamyearbyyearstats?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `year` | character | 4-digit year. |
| `gp` | integer | Games played. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `win_pct` | numeric | Win percentage (0-1 decimal). |
| `conf_rank` | integer | Team's final rank within its conference for the season. |
| `div_rank` | integer | Team's final rank within its division for the season. |
| `po_wins` | integer | Playoff wins recorded by the team that season. |
| `po_losses` | integer | Playoff losses recorded by the team that season. |
| `conf_count` | integer | Number of teams in the team's conference that season. |
| `div_count` | integer | Number of teams in the team's division that season. |
| `nba_finals_appearance` | character | Whether the team reached the league finals that season (e.g. "FINALS APPEARANCE" or "N/A"). |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `pf` | numeric | Personal fouls. |
| `stl` | numeric | Steals. |
| `tov` | numeric | Turnovers. |
| `blk` | numeric | Blocks. |
| `pts` | numeric | Points scored. |
| `pts_rank` | integer | League rank of the team's points scored for the season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamyearbyyearstats(league_id='10')
```

_Last validated n/a._

## `wnba_stats_videostatus`

GET /stats/videostatus

**Endpoint URL:** `GET https://stats.wnba.com/stats/videostatus`

**Valid URL:** [https://stats.wnba.com/stats/videostatus?LeagueID=10](https://stats.wnba.com/stats/videostatus?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `visitor_team_city` | character | City name of the visiting team. |
| `visitor_team_name` | character | Nickname of the visiting team. |
| `visitor_team_abbreviation` | character | Abbreviation of the visiting team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `home_team_city` | character | Home team city / location. |
| `home_team_name` | character | Home team name. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `game_status` | character | Game status label. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `is_available` | character | Flag indicating whether game video is available in the league's stats video system. |
| `pt_xyz_available` | character | Pt xyz available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_videostatus(league_id='10')
```

_Last validated n/a._
