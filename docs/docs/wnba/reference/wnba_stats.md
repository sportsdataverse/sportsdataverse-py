---
title: WNBA — WNBA Stats API (stats.wnba.com)
sidebar_label: WNBA Stats API (stats.wnba.com)
sidebar_position: 10
---
# WNBA — WNBA Stats API (stats.wnba.com)

`sportsdataverse.wnba` — 106 endpoints.

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
| `tov` | character | Turnovers. |
| `tov_rank` | character | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

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
| `game_id` | integer | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `start_position` | character | Starting lineup position code for the player in this game or roster row. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `e_off_rating` | character | Estimated offensive rating for the requested NBA or WNBA Stats split. |
| `off_rating` | character | Offensive rating for the requested NBA or WNBA Stats split. |
| `e_def_rating` | character | Estimated defensive rating for the requested NBA or WNBA Stats split. |
| `def_rating` | character | Defensive rating for the requested NBA or WNBA Stats split. |
| `e_net_rating` | character | Estimated net rating for the requested NBA or WNBA Stats split. |
| `net_rating` | character | Net rating (off rating - def rating). |
| `ast_pct` | numeric | Assist percentage. |
| `ast_tov` | character | Assist-to-turnover ratio for the requested NBA or WNBA Stats split. |
| `ast_ratio` | character | Assist ratio for the requested NBA or WNBA Stats split. |
| `oreb_pct` | numeric | Percentage or rate for offensive rebounds percentage in the requested NBA or WNBA Stats split. |
| `dreb_pct` | numeric | Percentage or rate for defensive rebounds percentage in the requested NBA or WNBA Stats split. |
| `reb_pct` | numeric | Percentage or rate for rebounds percentage in the requested NBA or WNBA Stats split. |
| `tm_tov_pct` | numeric | Percentage or rate for team turnovers percentage in the requested NBA or WNBA Stats split. |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |
| `e_usg_pct` | numeric | Estimated usage percentage for the requested NBA or WNBA Stats split. |
| `e_pace` | character | Estimated pace for the requested NBA or WNBA Stats split. |
| `pace` | character | Possessions per 48 minutes. |
| `pace_per40` | character | Pace per40. |
| `poss` | character | Poss. |
| `pie` | character | Player Impact Estimate (0-1). |

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
| `gameid` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `teamid` | character | FanGraphs team ID. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `personid` | character | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `firstname` | character | Firstname. |
| `familyname` | character | Display name for familyname associated with this NBA or WNBA Stats row. |
| `namei` | character | NBA or WNBA Stats value for namei in the boxscoreadvancedv3 result set. |
| `playerslug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character | NBA or WNBA Stats value for jerseynum in the boxscoreadvancedv3 result set. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `estimatedoffensiverating` | character | Advanced team or player metric for estimatedoffensiverating in the requested NBA or WNBA Stats split. |
| `offensiverating` | character | Advanced team or player metric for offensiverating in the requested NBA or WNBA Stats split. |
| `estimateddefensiverating` | character | Advanced team or player metric for estimateddefensiverating in the requested NBA or WNBA Stats split. |
| `defensiverating` | character | Advanced team or player metric for defensiverating in the requested NBA or WNBA Stats split. |
| `estimatednetrating` | character | Advanced team or player metric for estimatednetrating in the requested NBA or WNBA Stats split. |
| `netrating` | character | Advanced team or player metric for netrating in the requested NBA or WNBA Stats split. |
| `assistpercentage` | numeric | Percentage or rate for assistpercentage in the requested NBA or WNBA Stats split. |
| `assisttoturnover` | character | Passing or assist metric for assisttoturnover in the requested NBA or WNBA Stats split. |
| `assistratio` | character | Passing or assist metric for assistratio in the requested NBA or WNBA Stats split. |
| `offensivereboundpercentage` | numeric | Percentage or rate for offensivereboundpercentage in the requested NBA or WNBA Stats split. |
| `defensivereboundpercentage` | numeric | Percentage or rate for defensivereboundpercentage in the requested NBA or WNBA Stats split. |
| `reboundpercentage` | numeric | Percentage or rate for reboundpercentage in the requested NBA or WNBA Stats split. |
| `turnoverratio` | character | Turnover or loose-ball metric for turnoverratio in the requested NBA or WNBA Stats split. |
| `effectivefieldgoalpercentage` | numeric | Percentage or rate for effectivefieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `trueshootingpercentage` | numeric | Percentage or rate for trueshootingpercentage in the requested NBA or WNBA Stats split. |
| `usagepercentage` | numeric | Percentage or rate for usagepercentage in the requested NBA or WNBA Stats split. |
| `estimatedusagepercentage` | numeric | Percentage or rate for estimatedusagepercentage in the requested NBA or WNBA Stats split. |
| `estimatedpace` | character | Advanced team or player metric for estimatedpace in the requested NBA or WNBA Stats split. |
| `pace` | character | Possessions per 48 minutes. |
| `paceper40` | character | Advanced team or player metric for paceper40 in the requested NBA or WNBA Stats split. |
| `possessions` | character | Possessions used. |
| `pie` | character | Player Impact Estimate (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoreadvancedv3()
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
| `game_id` | integer | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `start_position` | character | Starting lineup position code for the player in this game or roster row. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `fta_rate` | character | NBA or WNBA Stats value for fta rate in the boxscorefourfactorsv2 result set. |
| `tm_tov_pct` | numeric | Percentage or rate for team turnovers percentage in the requested NBA or WNBA Stats split. |
| `oreb_pct` | numeric | Percentage or rate for offensive rebounds percentage in the requested NBA or WNBA Stats split. |
| `opp_efg_pct` | numeric | Opponent efg percentage for the requested NBA or WNBA team, player, lineup, or game split. |
| `opp_fta_rate` | character | Opponent fta rate for the requested NBA or WNBA team, player, lineup, or game split. |
| `opp_tov_pct` | numeric | Opponent turnovers percentage for the requested NBA or WNBA team, player, lineup, or game split. |
| `opp_oreb_pct` | numeric | Opponent offensive rebounds percentage for the requested NBA or WNBA team, player, lineup, or game split. |

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
| `gameid` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `teamid` | character | FanGraphs team ID. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `personid` | character | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `firstname` | character | Firstname. |
| `familyname` | character | Display name for familyname associated with this NBA or WNBA Stats row. |
| `namei` | character | NBA or WNBA Stats value for namei in the boxscorefourfactorsv3 result set. |
| `playerslug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character | NBA or WNBA Stats value for jerseynum in the boxscorefourfactorsv3 result set. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `effectivefieldgoalpercentage` | numeric | Percentage or rate for effectivefieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `freethrowattemptrate` | character | Shooting metric for freethrowattemptrate in the requested NBA or WNBA Stats split. |
| `teamturnoverpercentage` | numeric | Percentage or rate for teamturnoverpercentage in the requested NBA or WNBA Stats split. |
| `offensivereboundpercentage` | numeric | Percentage or rate for offensivereboundpercentage in the requested NBA or WNBA Stats split. |
| `oppeffectivefieldgoalpercentage` | numeric | Percentage or rate for oppeffectivefieldgoalpercentage in the requested NBA or WNBA Stats split. |
| `oppfreethrowattemptrate` | character | Shooting metric for oppfreethrowattemptrate in the requested NBA or WNBA Stats split. |
| `oppteamturnoverpercentage` | numeric | Percentage or rate for oppteamturnoverpercentage in the requested NBA or WNBA Stats split. |
| `oppoffensivereboundpercentage` | numeric | Percentage or rate for oppoffensivereboundpercentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscorefourfactorsv3()
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
| `game_id` | integer | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `start_position` | character | Starting lineup position code for the player in this game or roster row. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `pts_off_tov` | character | Turnover or loose-ball metric for points offensive turnovers in the requested NBA or WNBA Stats split. |
| `pts_2nd_chance` | character | Scoring or score-margin metric for points 2nd chance in the requested NBA or WNBA Stats split. |
| `pts_fb` | character | Scoring or score-margin metric for points fb in the requested NBA or WNBA Stats split. |
| `pts_paint` | character | Scoring or score-margin metric for points paint in the requested NBA or WNBA Stats split. |
| `opp_pts_off_tov` | character | Opponent points offensive turnovers for the requested NBA or WNBA team, player, lineup, or game split. |
| `opp_pts_2nd_chance` | character | Opponent points 2nd chance for the requested NBA or WNBA team, player, lineup, or game split. |
| `opp_pts_fb` | character | Opponent points fb for the requested NBA or WNBA team, player, lineup, or game split. |
| `opp_pts_paint` | character | Opponent points paint for the requested NBA or WNBA team, player, lineup, or game split. |
| `blk` | character | Blocks. |
| `blka` | character | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | character | Personal fouls. |
| `pfd` | character | Personal fouls drawn for the requested NBA or WNBA Stats split. |

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
| `gameid` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `teamid` | character | FanGraphs team ID. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `personid` | character | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `firstname` | character | Firstname. |
| `familyname` | character | Display name for familyname associated with this NBA or WNBA Stats row. |
| `namei` | character | NBA or WNBA Stats value for namei in the boxscoremiscv3 result set. |
| `playerslug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character | NBA or WNBA Stats value for jerseynum in the boxscoremiscv3 result set. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `pointsoffturnovers` | character | Turnover or loose-ball metric for pointsoffturnovers in the requested NBA or WNBA Stats split. |
| `pointssecondchance` | character | Scoring or score-margin metric for pointssecondchance in the requested NBA or WNBA Stats split. |
| `pointsfastbreak` | character | Scoring or score-margin metric for pointsfastbreak in the requested NBA or WNBA Stats split. |
| `pointspaint` | character | Scoring or score-margin metric for pointspaint in the requested NBA or WNBA Stats split. |
| `opppointsoffturnovers` | character | Turnover or loose-ball metric for opppointsoffturnovers in the requested NBA or WNBA Stats split. |
| `opppointssecondchance` | character | Scoring or score-margin metric for opppointssecondchance in the requested NBA or WNBA Stats split. |
| `opppointsfastbreak` | character | Scoring or score-margin metric for opppointsfastbreak in the requested NBA or WNBA Stats split. |
| `opppointspaint` | character | Scoring or score-margin metric for opppointspaint in the requested NBA or WNBA Stats split. |
| `blocks` | character | Total blocks. |
| `blocksagainst` | character | NBA or WNBA Stats value for blocksagainst in the boxscoremiscv3 result set. |
| `foulspersonal` | character | NBA or WNBA Stats value for foulspersonal in the boxscoremiscv3 result set. |
| `foulsdrawn` | character | NBA or WNBA Stats value for foulsdrawn in the boxscoremiscv3 result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoremiscv3()
```

_Last validated n/a._

## `wnba_stats_boxscoreplayertrackv2`

GET /stats/boxscoreplayertrackv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/boxscoreplayertrackv2`

**Valid URL:** [https://stats.wnba.com/stats/boxscoreplayertrackv2](https://stats.wnba.com/stats/boxscoreplayertrackv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_boxscoreplayertrackv2()
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
| `game_id` | integer | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `start_position` | character | Starting lineup position code for the player in this game or roster row. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `pct_fga_2pt` | numeric | Percentage share of fga 2pt for the requested NBA or WNBA Stats split. |
| `pct_fga_3pt` | numeric | Percentage share of fga 3pt for the requested NBA or WNBA Stats split. |
| `pct_pts_2pt` | numeric | Percentage share of points 2pt for the requested NBA or WNBA Stats split. |
| `pct_pts_2pt_mr` | numeric | Percentage share of points 2pt mr for the requested NBA or WNBA Stats split. |
| `pct_pts_3pt` | numeric | Percentage share of points 3pt for the requested NBA or WNBA Stats split. |
| `pct_pts_fb` | numeric | Percentage share of points fb for the requested NBA or WNBA Stats split. |
| `pct_pts_ft` | numeric | Percentage share of points free throws for the requested NBA or WNBA Stats split. |
| `pct_pts_off_tov` | numeric | Percentage share of points offensive turnovers for the requested NBA or WNBA Stats split. |
| `pct_pts_paint` | numeric | Percentage share of points paint for the requested NBA or WNBA Stats split. |
| `pct_ast_2pm` | numeric | Percentage share of assists 2pm for the requested NBA or WNBA Stats split. |
| `pct_uast_2pm` | numeric | Percentage share of uast 2pm for the requested NBA or WNBA Stats split. |
| `pct_ast_3pm` | numeric | Percentage share of assists 3pm for the requested NBA or WNBA Stats split. |
| `pct_uast_3pm` | numeric | Percentage share of uast 3pm for the requested NBA or WNBA Stats split. |
| `pct_ast_fgm` | numeric | Percentage share of assists fgm for the requested NBA or WNBA Stats split. |
| `pct_uast_fgm` | numeric | Percentage share of uast fgm for the requested NBA or WNBA Stats split. |

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
| `gameid` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `teamid` | character | FanGraphs team ID. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `personid` | character | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `firstname` | character | Firstname. |
| `familyname` | character | Display name for familyname associated with this NBA or WNBA Stats row. |
| `namei` | character | NBA or WNBA Stats value for namei in the boxscorescoringv3 result set. |
| `playerslug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character | NBA or WNBA Stats value for jerseynum in the boxscorescoringv3 result set. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `percentagefieldgoalsattempted2pt` | character | Percentage share or rate for fieldgoalsattempted2pt in the requested NBA or WNBA Stats split. |
| `percentagefieldgoalsattempted3pt` | character | Percentage share or rate for fieldgoalsattempted3pt in the requested NBA or WNBA Stats split. |
| `percentagepoints2pt` | character | Percentage share or rate for points2pt in the requested NBA or WNBA Stats split. |
| `percentagepointsmidrange2pt` | character | Percentage share or rate for pointsmidrange2pt in the requested NBA or WNBA Stats split. |
| `percentagepoints3pt` | character | Percentage share or rate for points3pt in the requested NBA or WNBA Stats split. |
| `percentagepointsfastbreak` | character | Percentage share or rate for pointsfastbreak in the requested NBA or WNBA Stats split. |
| `percentagepointsfreethrow` | character | Percentage share or rate for pointsfreethrow in the requested NBA or WNBA Stats split. |
| `percentagepointsoffturnovers` | character | Percentage share or rate for pointsoffturnovers in the requested NBA or WNBA Stats split. |
| `percentagepointspaint` | character | Percentage share or rate for pointspaint in the requested NBA or WNBA Stats split. |
| `percentageassisted2pt` | character | Percentage share or rate for assisted2pt in the requested NBA or WNBA Stats split. |
| `percentageunassisted2pt` | character | Percentage share or rate for unassisted2pt in the requested NBA or WNBA Stats split. |
| `percentageassisted3pt` | character | Percentage share or rate for assisted3pt in the requested NBA or WNBA Stats split. |
| `percentageunassisted3pt` | character | Percentage share or rate for unassisted3pt in the requested NBA or WNBA Stats split. |
| `percentageassistedfgm` | character | Percentage share or rate for assistedfgm in the requested NBA or WNBA Stats split. |
| `percentageunassistedfgm` | character | Percentage share or rate for unassistedfgm in the requested NBA or WNBA Stats split. |

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
| `game_id` | integer | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `start_position` | character | Starting lineup position code for the player in this game or roster row. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
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
| `to` | character | Final season played in NFL |
| `pf` | character | Personal fouls. |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |

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
| `gameid` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `teamid` | character | FanGraphs team ID. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `personid` | character | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `firstname` | character | Firstname. |
| `familyname` | character | Display name for familyname associated with this NBA or WNBA Stats row. |
| `namei` | character | NBA or WNBA Stats value for namei in the boxscoretraditionalv3 result set. |
| `playerslug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character | NBA or WNBA Stats value for jerseynum in the boxscoretraditionalv3 result set. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `fieldgoalsmade` | character | Shooting metric for fieldgoalsmade in the requested NBA or WNBA Stats split. |
| `fieldgoalsattempted` | character | Shooting metric for fieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `fieldgoalspercentage` | numeric | Percentage or rate for fieldgoalspercentage in the requested NBA or WNBA Stats split. |
| `threepointersmade` | character | Shooting metric for threepointersmade in the requested NBA or WNBA Stats split. |
| `threepointersattempted` | character | Shooting metric for threepointersattempted in the requested NBA or WNBA Stats split. |
| `threepointerspercentage` | numeric | Percentage or rate for threepointerspercentage in the requested NBA or WNBA Stats split. |
| `freethrowsmade` | character | Shooting metric for freethrowsmade in the requested NBA or WNBA Stats split. |
| `freethrowsattempted` | character | Shooting metric for freethrowsattempted in the requested NBA or WNBA Stats split. |
| `freethrowspercentage` | numeric | Percentage or rate for freethrowspercentage in the requested NBA or WNBA Stats split. |
| `reboundsoffensive` | character | Rebounding metric for reboundsoffensive in the requested NBA or WNBA Stats split. |
| `reboundsdefensive` | character | Rebounding metric for reboundsdefensive in the requested NBA or WNBA Stats split. |
| `reboundstotal` | character | Rebounding metric for reboundstotal in the requested NBA or WNBA Stats split. |
| `assists` | character | Total assists. |
| `steals` | character | Total steals. |
| `blocks` | character | Total blocks. |
| `turnovers` | character | Total turnovers. |
| `foulspersonal` | character | NBA or WNBA Stats value for foulspersonal in the boxscoretraditionalv3 result set. |
| `points` | character | Points scored. |
| `plusminuspoints` | character | Scoring or score-margin metric for plusminuspoints in the requested NBA or WNBA Stats split. |

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
| `game_id` | integer | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `start_position` | character | Starting lineup position code for the player in this game or roster row. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |
| `pct_fgm` | numeric | Percentage share of fgm for the requested NBA or WNBA Stats split. |
| `pct_fga` | numeric | Percentage share of fga for the requested NBA or WNBA Stats split. |
| `pct_fg3m` | numeric | Percentage share of fg3m for the requested NBA or WNBA Stats split. |
| `pct_fg3a` | numeric | Percentage share of fg3a for the requested NBA or WNBA Stats split. |
| `pct_ftm` | numeric | Percentage share of ftm for the requested NBA or WNBA Stats split. |
| `pct_fta` | numeric | Percentage share of fta for the requested NBA or WNBA Stats split. |
| `pct_oreb` | numeric | Percentage share of offensive rebounds for the requested NBA or WNBA Stats split. |
| `pct_dreb` | numeric | Percentage share of defensive rebounds for the requested NBA or WNBA Stats split. |
| `pct_reb` | numeric | Percentage share of rebounds for the requested NBA or WNBA Stats split. |
| `pct_ast` | numeric | Percentage share of assists for the requested NBA or WNBA Stats split. |
| `pct_tov` | numeric | Percentage share of turnovers for the requested NBA or WNBA Stats split. |
| `pct_stl` | numeric | Percentage share of steals for the requested NBA or WNBA Stats split. |
| `pct_blk` | numeric | Percentage share of blocks for the requested NBA or WNBA Stats split. |
| `pct_blka` | numeric | Percentage share of blocked attempts for the requested NBA or WNBA Stats split. |
| `pct_pf` | numeric | Percentage share of personal fouls for the requested NBA or WNBA Stats split. |
| `pct_pfd` | numeric | Percentage share of personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pct_pts` | numeric | Percentage share of points for the requested NBA or WNBA Stats split. |

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
| `gameid` | character | Unique stats.nba.com game identifier in endpoints that use compact schedule field names. |
| `teamid` | character | FanGraphs team ID. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character | Three-letter team code used by NBA or WNBA Stats schedule and scoreboard feeds. |
| `teamslug` | character | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `personid` | character | Stats API identifier for personid associated with this NBA or WNBA Stats row. |
| `firstname` | character | Firstname. |
| `familyname` | character | Display name for familyname associated with this NBA or WNBA Stats row. |
| `namei` | character | NBA or WNBA Stats value for namei in the boxscoreusagev3 result set. |
| `playerslug` | character | URL slug for playerslug used by NBA or WNBA Stats pages. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character | NBA or WNBA Stats value for jerseynum in the boxscoreusagev3 result set. |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `usagepercentage` | numeric | Percentage or rate for usagepercentage in the requested NBA or WNBA Stats split. |
| `percentagefieldgoalsmade` | character | Percentage share or rate for fieldgoalsmade in the requested NBA or WNBA Stats split. |
| `percentagefieldgoalsattempted` | character | Percentage share or rate for fieldgoalsattempted in the requested NBA or WNBA Stats split. |
| `percentagethreepointersmade` | character | Percentage share or rate for threepointersmade in the requested NBA or WNBA Stats split. |
| `percentagethreepointersattempted` | character | Percentage share or rate for threepointersattempted in the requested NBA or WNBA Stats split. |
| `percentagefreethrowsmade` | character | Percentage share or rate for freethrowsmade in the requested NBA or WNBA Stats split. |
| `percentagefreethrowsattempted` | character | Percentage share or rate for freethrowsattempted in the requested NBA or WNBA Stats split. |
| `percentagereboundsoffensive` | character | Percentage share or rate for reboundsoffensive in the requested NBA or WNBA Stats split. |
| `percentagereboundsdefensive` | character | Percentage share or rate for reboundsdefensive in the requested NBA or WNBA Stats split. |
| `percentagereboundstotal` | character | Percentage share or rate for reboundstotal in the requested NBA or WNBA Stats split. |
| `percentageassists` | character | Percentage share or rate for assists in the requested NBA or WNBA Stats split. |
| `percentageturnovers` | character | Percentage share or rate for turnovers in the requested NBA or WNBA Stats split. |
| `percentagesteals` | character | Percentage share or rate for steals in the requested NBA or WNBA Stats split. |
| `percentageblocks` | character | Percentage share or rate for blocks in the requested NBA or WNBA Stats split. |
| `percentageblocksallowed` | character | Percentage share or rate for blocksallowed in the requested NBA or WNBA Stats split. |
| `percentagepersonalfouls` | character | Percentage share or rate for personalfouls in the requested NBA or WNBA Stats split. |
| `percentagepersonalfoulsdrawn` | character | Percentage share or rate for personalfoulsdrawn in the requested NBA or WNBA Stats split. |
| `percentagepoints` | character | Percentage share or rate for points in the requested NBA or WNBA Stats split. |

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
| `display_last_comma_first` | character | Player name formatted as 'Last, First' for alphabetical directory sorting. |
| `display_first_last` | character | Player name formatted as 'First Last' for display in player-facing contexts. |
| `rosterstatus` | character | Whether the player is currently on an active roster (1 = active, 0 = inactive or retired). |
| `from_year` | character | First season. |
| `to_year` | character | Most recent season. |
| `playercode` | character | Slug-style identifier for the player used in stats.nba.com profile URLs. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_code` | character | Internal team code. |
| `games_played_flag` | character | Flag indicating whether the player has appeared in at least one game ('Y' or 'N'). |
| `otherleague_experience_ch` | character | Flag for whether the player has experience in a non-NBA/WNBA league prior to entering the NBA or WNBA. |

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
| `display_fi_last` | character | NBA or WNBA Stats value for display fi last in the cumestatsplayer result set. |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `jersey_num` | character | Jersey number worn by the player. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `actual_minutes` | character | NBA or WNBA Stats value for actual minutes in the cumestatsplayer result set. |
| `actual_seconds` | character | NBA or WNBA Stats value for actual seconds in the cumestatsplayer result set. |
| `fg` | character | Shooting metric for field goals in the requested NBA or WNBA Stats split. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3` | character | Shooting metric for three-point field goals in the requested NBA or WNBA Stats split. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft` | character | NBA or WNBA Stats value for free throws in the cumestatsplayer result set. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `off_reb` | character | Rebounding metric for offensive rebounds in the requested NBA or WNBA Stats split. |
| `def_reb` | character | Rebounding metric for defensive rebounds in the requested NBA or WNBA Stats split. |
| `tot_reb` | character | Rebounding metric for tot rebounds in the requested NBA or WNBA Stats split. |
| `ast` | character | Assists. |
| `pf` | character | Personal fouls. |
| `dq` | character | NBA or WNBA Stats value for dq in the cumestatsplayer result set. |
| `stl` | character | Steals. |
| `turnovers` | character | Total turnovers. |
| `blk` | character | Blocks. |
| `pts` | character | Points scored. |
| `max_actual_minutes` | character | NBA or WNBA Stats value for maximum actual minutes in the cumestatsplayer result set. |
| `max_actual_seconds` | character | NBA or WNBA Stats value for maximum actual seconds in the cumestatsplayer result set. |
| `max_reb` | character | Rebounding metric for maximum rebounds in the requested NBA or WNBA Stats split. |
| `max_ast` | character | NBA or WNBA Stats value for maximum assists in the cumestatsplayer result set. |
| `max_stl` | character | NBA or WNBA Stats value for maximum steals in the cumestatsplayer result set. |
| `max_turnovers` | character | Turnover or loose-ball metric for maximum turnovers in the requested NBA or WNBA Stats split. |
| `max_blk` | character | NBA or WNBA Stats value for maximum blocks in the cumestatsplayer result set. |
| `max_pts` | character | Scoring or score-margin metric for maximum points in the requested NBA or WNBA Stats split. |
| `avg_actual_minutes` | character | NBA or WNBA Stats value for average actual minutes in the cumestatsplayer result set. |
| `avg_actual_seconds` | character | NBA or WNBA Stats value for average actual seconds in the cumestatsplayer result set. |
| `avg_tot_reb` | character | Rebounding metric for average tot rebounds in the requested NBA or WNBA Stats split. |
| `avg_ast` | character | NBA or WNBA Stats value for average assists in the cumestatsplayer result set. |
| `avg_stl` | character | NBA or WNBA Stats value for average steals in the cumestatsplayer result set. |
| `avg_turnovers` | character | Turnover or loose-ball metric for average turnovers in the requested NBA or WNBA Stats split. |
| `avg_blk` | character | NBA or WNBA Stats value for average blocks in the cumestatsplayer result set. |
| `avg_pts` | character | Scoring or score-margin metric for average points in the requested NBA or WNBA Stats split. |
| `per_min_tot_reb` | character | Rebounding metric for per minutes tot rebounds in the requested NBA or WNBA Stats split. |
| `per_min_ast` | character | NBA or WNBA Stats value for per minutes assists in the cumestatsplayer result set. |
| `per_min_stl` | character | NBA or WNBA Stats value for per minutes steals in the cumestatsplayer result set. |
| `per_min_turnovers` | character | Turnover or loose-ball metric for per minutes turnovers in the requested NBA or WNBA Stats split. |
| `per_min_blk` | character | NBA or WNBA Stats value for per minutes blocks in the cumestatsplayer result set. |
| `per_min_pts` | character | Scoring or score-margin metric for per minutes points in the requested NBA or WNBA Stats split. |

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
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `actual_minutes` | character | NBA or WNBA Stats value for actual minutes in the cumestatsteam result set. |
| `actual_seconds` | character | NBA or WNBA Stats value for actual seconds in the cumestatsteam result set. |
| `fg` | character | Shooting metric for field goals in the requested NBA or WNBA Stats split. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3` | character | Shooting metric for three-point field goals in the requested NBA or WNBA Stats split. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft` | character | NBA or WNBA Stats value for free throws in the cumestatsteam result set. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `off_reb` | character | Rebounding metric for offensive rebounds in the requested NBA or WNBA Stats split. |
| `def_reb` | character | Rebounding metric for defensive rebounds in the requested NBA or WNBA Stats split. |
| `tot_reb` | character | Rebounding metric for tot rebounds in the requested NBA or WNBA Stats split. |
| `ast` | character | Assists. |
| `pf` | character | Personal fouls. |
| `dq` | character | NBA or WNBA Stats value for dq in the cumestatsteam result set. |
| `stl` | character | Steals. |
| `turnovers` | character | Total turnovers. |
| `blk` | character | Blocks. |
| `pts` | character | Points scored. |
| `max_actual_minutes` | character | NBA or WNBA Stats value for maximum actual minutes in the cumestatsteam result set. |
| `max_actual_seconds` | character | NBA or WNBA Stats value for maximum actual seconds in the cumestatsteam result set. |
| `max_reb` | character | Rebounding metric for maximum rebounds in the requested NBA or WNBA Stats split. |
| `max_ast` | character | NBA or WNBA Stats value for maximum assists in the cumestatsteam result set. |
| `max_stl` | character | NBA or WNBA Stats value for maximum steals in the cumestatsteam result set. |
| `max_turnovers` | character | Turnover or loose-ball metric for maximum turnovers in the requested NBA or WNBA Stats split. |
| `max_blkp` | character | NBA or WNBA Stats value for maximum blocks percentage in the cumestatsteam result set. |
| `max_pts` | character | Scoring or score-margin metric for maximum points in the requested NBA or WNBA Stats split. |
| `avg_actual_minutes` | character | NBA or WNBA Stats value for average actual minutes in the cumestatsteam result set. |
| `avg_actual_seconds` | character | NBA or WNBA Stats value for average actual seconds in the cumestatsteam result set. |
| `avg_reb` | character | Rebounding metric for average rebounds in the requested NBA or WNBA Stats split. |
| `avg_ast` | character | NBA or WNBA Stats value for average assists in the cumestatsteam result set. |
| `avg_stl` | character | NBA or WNBA Stats value for average steals in the cumestatsteam result set. |
| `avg_turnovers` | character | Turnover or loose-ball metric for average turnovers in the requested NBA or WNBA Stats split. |
| `avg_blkp` | character | NBA or WNBA Stats value for average blocks percentage in the cumestatsteam result set. |
| `avg_pts` | character | Scoring or score-margin metric for average points in the requested NBA or WNBA Stats split. |
| `per_min_reb` | character | Rebounding metric for per minutes rebounds in the requested NBA or WNBA Stats split. |
| `per_min_ast` | character | NBA or WNBA Stats value for per minutes assists in the cumestatsteam result set. |
| `per_min_stl` | character | NBA or WNBA Stats value for per minutes steals in the cumestatsteam result set. |
| `per_min_turnovers` | character | Turnover or loose-ball metric for per minutes turnovers in the requested NBA or WNBA Stats split. |
| `per_min_blk` | character | NBA or WNBA Stats value for per minutes blocks in the cumestatsteam result set. |
| `per_min_pts` | character | Scoring or score-margin metric for per minutes points in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_cumestatsteam(league_id='10')
```

_Last validated n/a._

## `wnba_stats_defensehub`

GET /stats/defensehub

**Endpoint URL:** `GET https://stats.wnba.com/stats/defensehub`

**Valid URL:** [https://stats.wnba.com/stats/defensehub?LeagueID=10](https://stats.wnba.com/stats/defensehub?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rank` | character | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `def_rim_pct` | numeric | Percentage or rate for defensive rim percentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_defensehub(league_id='10')
```

_Last validated n/a._

## `wnba_stats_draftcombinedrillresults`

GET /stats/draftcombinedrillresults

**Endpoint URL:** `GET https://stats.wnba.com/stats/draftcombinedrillresults`

**Valid URL:** [https://stats.wnba.com/stats/draftcombinedrillresults?LeagueID=10](https://stats.wnba.com/stats/draftcombinedrillresults?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer | Stats API identifier for temp player identifier associated with this NBA or WNBA Stats row. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `standing_vertical_leap` | character | NBA or WNBA Stats value for standing vertical leap in the draftcombinedrillresults result set. |
| `max_vertical_leap` | character | NBA or WNBA Stats value for maximum vertical leap in the draftcombinedrillresults result set. |
| `lane_agility_time` | character | Time value for lane agility time in the NBA or WNBA Stats result set. |
| `modified_lane_agility_time` | character | Time value for modified lane agility time in the NBA or WNBA Stats result set. |
| `three_quarter_sprint` | character | NBA or WNBA Stats value for three quarter sprint in the draftcombinedrillresults result set. |
| `bench_press` | character | NBA or WNBA Stats value for bench press in the draftcombinedrillresults result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_draftcombinedrillresults(league_id='10')
```

_Last validated n/a._

## `wnba_stats_draftcombinenonstationaryshooting`

GET /stats/draftcombinenonstationaryshooting

**Endpoint URL:** `GET https://stats.wnba.com/stats/draftcombinenonstationaryshooting`

**Valid URL:** [https://stats.wnba.com/stats/draftcombinenonstationaryshooting?LeagueID=10](https://stats.wnba.com/stats/draftcombinenonstationaryshooting?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer | Stats API identifier for temp player identifier associated with this NBA or WNBA Stats row. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `off_drib_fifteen_break_left_made` | character | NBA or WNBA Stats value for offensive drib fifteen break left made in the draftcombinenonstationaryshooting result set. |
| `off_drib_fifteen_break_left_attempt` | character | NBA or WNBA Stats value for offensive drib fifteen break left attempt in the draftcombinenonstationaryshooting result set. |
| `off_drib_fifteen_break_left_pct` | numeric | Percentage or rate for offensive drib fifteen break left percentage in the requested NBA or WNBA Stats split. |
| `off_drib_fifteen_top_key_made` | character | NBA or WNBA Stats value for offensive drib fifteen top key made in the draftcombinenonstationaryshooting result set. |
| `off_drib_fifteen_top_key_attempt` | character | NBA or WNBA Stats value for offensive drib fifteen top key attempt in the draftcombinenonstationaryshooting result set. |
| `off_drib_fifteen_top_key_pct` | numeric | Percentage or rate for offensive drib fifteen top key percentage in the requested NBA or WNBA Stats split. |
| `off_drib_fifteen_break_right_made` | character | NBA or WNBA Stats value for offensive drib fifteen break right made in the draftcombinenonstationaryshooting result set. |
| `off_drib_fifteen_break_right_attempt` | character | NBA or WNBA Stats value for offensive drib fifteen break right attempt in the draftcombinenonstationaryshooting result set. |
| `off_drib_fifteen_break_right_pct` | numeric | Percentage or rate for offensive drib fifteen break right percentage in the requested NBA or WNBA Stats split. |
| `off_drib_college_break_left_made` | character | NBA or WNBA Stats value for offensive drib college break left made in the draftcombinenonstationaryshooting result set. |
| `off_drib_college_break_left_attempt` | character | NBA or WNBA Stats value for offensive drib college break left attempt in the draftcombinenonstationaryshooting result set. |
| `off_drib_college_break_left_pct` | numeric | Percentage or rate for offensive drib college break left percentage in the requested NBA or WNBA Stats split. |
| `off_drib_college_top_key_made` | character | NBA or WNBA Stats value for offensive drib college top key made in the draftcombinenonstationaryshooting result set. |
| `off_drib_college_top_key_attempt` | character | NBA or WNBA Stats value for offensive drib college top key attempt in the draftcombinenonstationaryshooting result set. |
| `off_drib_college_top_key_pct` | numeric | Percentage or rate for offensive drib college top key percentage in the requested NBA or WNBA Stats split. |
| `off_drib_college_break_right_made` | character | NBA or WNBA Stats value for offensive drib college break right made in the draftcombinenonstationaryshooting result set. |
| `off_drib_college_break_right_attempt` | character | NBA or WNBA Stats value for offensive drib college break right attempt in the draftcombinenonstationaryshooting result set. |
| `off_drib_college_break_right_pct` | numeric | Percentage or rate for offensive drib college break right percentage in the requested NBA or WNBA Stats split. |
| `on_move_fifteen_made` | character | NBA or WNBA Stats value for on move fifteen made in the draftcombinenonstationaryshooting result set. |
| `on_move_fifteen_attempt` | character | NBA or WNBA Stats value for on move fifteen attempt in the draftcombinenonstationaryshooting result set. |
| `on_move_fifteen_pct` | numeric | Percentage or rate for on move fifteen percentage in the requested NBA or WNBA Stats split. |
| `on_move_college_made` | character | NBA or WNBA Stats value for on move college made in the draftcombinenonstationaryshooting result set. |
| `on_move_college_attempt` | character | NBA or WNBA Stats value for on move college attempt in the draftcombinenonstationaryshooting result set. |
| `on_move_college_pct` | numeric | Percentage or rate for on move college percentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_draftcombinenonstationaryshooting(league_id='10')
```

_Last validated n/a._

## `wnba_stats_draftcombineplayeranthro`

GET /stats/draftcombineplayeranthro

**Endpoint URL:** `GET https://stats.wnba.com/stats/draftcombineplayeranthro`

**Valid URL:** [https://stats.wnba.com/stats/draftcombineplayeranthro?LeagueID=10](https://stats.wnba.com/stats/draftcombineplayeranthro?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer | Stats API identifier for temp player identifier associated with this NBA or WNBA Stats row. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height_wo_shoes` | character | NBA or WNBA Stats value for height wo shoes in the draftcombineplayeranthro result set. |
| `height_wo_shoes_ft_in` | character | NBA or WNBA Stats value for height wo shoes free throws in in the draftcombineplayeranthro result set. |
| `height_w_shoes` | character | NBA or WNBA Stats value for height w shoes in the draftcombineplayeranthro result set. |
| `height_w_shoes_ft_in` | character | NBA or WNBA Stats value for height w shoes free throws in in the draftcombineplayeranthro result set. |
| `weight` | character | Player weight in pounds. |
| `wingspan` | character | NBA or WNBA Stats value for wingspan in the draftcombineplayeranthro result set. |
| `wingspan_ft_in` | character | NBA or WNBA Stats value for wingspan free throws in in the draftcombineplayeranthro result set. |
| `standing_reach` | character | NBA or WNBA Stats value for standing reach in the draftcombineplayeranthro result set. |
| `standing_reach_ft_in` | character | NBA or WNBA Stats value for standing reach free throws in in the draftcombineplayeranthro result set. |
| `body_fat_pct` | numeric | Percentage or rate for body fat percentage in the requested NBA or WNBA Stats split. |
| `hand_length` | character | NBA or WNBA Stats value for hand length in the draftcombineplayeranthro result set. |
| `hand_width` | character | NBA or WNBA Stats value for hand width in the draftcombineplayeranthro result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_draftcombineplayeranthro(league_id='10')
```

_Last validated n/a._

## `wnba_stats_draftcombinespotshooting`

GET /stats/draftcombinespotshooting

**Endpoint URL:** `GET https://stats.wnba.com/stats/draftcombinespotshooting`

**Valid URL:** [https://stats.wnba.com/stats/draftcombinespotshooting?LeagueID=10](https://stats.wnba.com/stats/draftcombinespotshooting?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer | Stats API identifier for temp player identifier associated with this NBA or WNBA Stats row. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `fifteen_corner_left_made` | character | NBA or WNBA Stats value for fifteen corner left made in the draftcombinespotshooting result set. |
| `fifteen_corner_left_attempt` | character | NBA or WNBA Stats value for fifteen corner left attempt in the draftcombinespotshooting result set. |
| `fifteen_corner_left_pct` | numeric | Percentage or rate for fifteen corner left percentage in the requested NBA or WNBA Stats split. |
| `fifteen_break_left_made` | character | NBA or WNBA Stats value for fifteen break left made in the draftcombinespotshooting result set. |
| `fifteen_break_left_attempt` | character | NBA or WNBA Stats value for fifteen break left attempt in the draftcombinespotshooting result set. |
| `fifteen_break_left_pct` | numeric | Percentage or rate for fifteen break left percentage in the requested NBA or WNBA Stats split. |
| `fifteen_top_key_made` | character | NBA or WNBA Stats value for fifteen top key made in the draftcombinespotshooting result set. |
| `fifteen_top_key_attempt` | character | NBA or WNBA Stats value for fifteen top key attempt in the draftcombinespotshooting result set. |
| `fifteen_top_key_pct` | numeric | Percentage or rate for fifteen top key percentage in the requested NBA or WNBA Stats split. |
| `fifteen_break_right_made` | character | NBA or WNBA Stats value for fifteen break right made in the draftcombinespotshooting result set. |
| `fifteen_break_right_attempt` | character | NBA or WNBA Stats value for fifteen break right attempt in the draftcombinespotshooting result set. |
| `fifteen_break_right_pct` | numeric | Percentage or rate for fifteen break right percentage in the requested NBA or WNBA Stats split. |
| `fifteen_corner_right_made` | character | NBA or WNBA Stats value for fifteen corner right made in the draftcombinespotshooting result set. |
| `fifteen_corner_right_attempt` | character | NBA or WNBA Stats value for fifteen corner right attempt in the draftcombinespotshooting result set. |
| `fifteen_corner_right_pct` | numeric | Percentage or rate for fifteen corner right percentage in the requested NBA or WNBA Stats split. |
| `college_corner_left_made` | character | NBA or WNBA Stats value for college corner left made in the draftcombinespotshooting result set. |
| `college_corner_left_attempt` | character | NBA or WNBA Stats value for college corner left attempt in the draftcombinespotshooting result set. |
| `college_corner_left_pct` | numeric | Percentage or rate for college corner left percentage in the requested NBA or WNBA Stats split. |
| `college_break_left_made` | character | NBA or WNBA Stats value for college break left made in the draftcombinespotshooting result set. |
| `college_break_left_attempt` | character | NBA or WNBA Stats value for college break left attempt in the draftcombinespotshooting result set. |
| `college_break_left_pct` | numeric | Percentage or rate for college break left percentage in the requested NBA or WNBA Stats split. |
| `college_top_key_made` | character | NBA or WNBA Stats value for college top key made in the draftcombinespotshooting result set. |
| `college_top_key_attempt` | character | NBA or WNBA Stats value for college top key attempt in the draftcombinespotshooting result set. |
| `college_top_key_pct` | numeric | Percentage or rate for college top key percentage in the requested NBA or WNBA Stats split. |
| `college_break_right_made` | character | NBA or WNBA Stats value for college break right made in the draftcombinespotshooting result set. |
| `college_break_right_attempt` | character | NBA or WNBA Stats value for college break right attempt in the draftcombinespotshooting result set. |
| `college_break_right_pct` | numeric | Percentage or rate for college break right percentage in the requested NBA or WNBA Stats split. |
| `college_corner_right_made` | character | NBA or WNBA Stats value for college corner right made in the draftcombinespotshooting result set. |
| `college_corner_right_attempt` | character | NBA or WNBA Stats value for college corner right attempt in the draftcombinespotshooting result set. |
| `college_corner_right_pct` | numeric | Percentage or rate for college corner right percentage in the requested NBA or WNBA Stats split. |
| `nba_corner_left_made` | character | NBA or WNBA Stats value for NBA corner left made in the draftcombinespotshooting result set. |
| `nba_corner_left_attempt` | character | NBA or WNBA Stats value for NBA corner left attempt in the draftcombinespotshooting result set. |
| `nba_corner_left_pct` | numeric | Percentage or rate for NBA corner left percentage in the requested NBA or WNBA Stats split. |
| `nba_break_left_made` | character | NBA or WNBA Stats value for NBA break left made in the draftcombinespotshooting result set. |
| `nba_break_left_attempt` | character | NBA or WNBA Stats value for NBA break left attempt in the draftcombinespotshooting result set. |
| `nba_break_left_pct` | numeric | Percentage or rate for NBA break left percentage in the requested NBA or WNBA Stats split. |
| `nba_top_key_made` | character | NBA or WNBA Stats value for NBA top key made in the draftcombinespotshooting result set. |
| `nba_top_key_attempt` | character | NBA or WNBA Stats value for NBA top key attempt in the draftcombinespotshooting result set. |
| `nba_top_key_pct` | numeric | Percentage or rate for NBA top key percentage in the requested NBA or WNBA Stats split. |
| `nba_break_right_made` | character | NBA or WNBA Stats value for NBA break right made in the draftcombinespotshooting result set. |
| `nba_break_right_attempt` | character | NBA or WNBA Stats value for NBA break right attempt in the draftcombinespotshooting result set. |
| `nba_break_right_pct` | numeric | Percentage or rate for NBA break right percentage in the requested NBA or WNBA Stats split. |
| `nba_corner_right_made` | character | NBA or WNBA Stats value for NBA corner right made in the draftcombinespotshooting result set. |
| `nba_corner_right_attempt` | character | NBA or WNBA Stats value for NBA corner right attempt in the draftcombinespotshooting result set. |
| `nba_corner_right_pct` | numeric | Percentage or rate for NBA corner right percentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_draftcombinespotshooting(league_id='10')
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
| `height_wo_shoes` | character | NBA or WNBA Stats value for height wo shoes in the draftcombinestats result set. |
| `height_wo_shoes_ft_in` | character | NBA or WNBA Stats value for height wo shoes free throws in in the draftcombinestats result set. |
| `height_w_shoes` | character | NBA or WNBA Stats value for height w shoes in the draftcombinestats result set. |
| `height_w_shoes_ft_in` | character | NBA or WNBA Stats value for height w shoes free throws in in the draftcombinestats result set. |
| `weight` | character | Player weight in pounds. |
| `wingspan` | character | NBA or WNBA Stats value for wingspan in the draftcombinestats result set. |
| `wingspan_ft_in` | character | NBA or WNBA Stats value for wingspan free throws in in the draftcombinestats result set. |
| `standing_reach` | character | NBA or WNBA Stats value for standing reach in the draftcombinestats result set. |
| `standing_reach_ft_in` | character | NBA or WNBA Stats value for standing reach free throws in in the draftcombinestats result set. |
| `body_fat_pct` | numeric | Percentage or rate for body fat percentage in the requested NBA or WNBA Stats split. |
| `hand_length` | character | NBA or WNBA Stats value for hand length in the draftcombinestats result set. |
| `hand_width` | character | NBA or WNBA Stats value for hand width in the draftcombinestats result set. |
| `standing_vertical_leap` | character | NBA or WNBA Stats value for standing vertical leap in the draftcombinestats result set. |
| `max_vertical_leap` | character | NBA or WNBA Stats value for maximum vertical leap in the draftcombinestats result set. |
| `lane_agility_time` | character | Time value for lane agility time in the NBA or WNBA Stats result set. |
| `modified_lane_agility_time` | character | Time value for modified lane agility time in the NBA or WNBA Stats result set. |
| `three_quarter_sprint` | character | NBA or WNBA Stats value for three quarter sprint in the draftcombinestats result set. |
| `bench_press` | character | NBA or WNBA Stats value for bench press in the draftcombinestats result set. |
| `spot_fifteen_corner_left` | character | NBA or WNBA Stats value for spot fifteen corner left in the draftcombinestats result set. |
| `spot_fifteen_break_left` | character | NBA or WNBA Stats value for spot fifteen break left in the draftcombinestats result set. |
| `spot_fifteen_top_key` | character | NBA or WNBA Stats value for spot fifteen top key in the draftcombinestats result set. |
| `spot_fifteen_break_right` | character | NBA or WNBA Stats value for spot fifteen break right in the draftcombinestats result set. |
| `spot_fifteen_corner_right` | character | NBA or WNBA Stats value for spot fifteen corner right in the draftcombinestats result set. |
| `spot_college_corner_left` | character | NBA or WNBA Stats value for spot college corner left in the draftcombinestats result set. |
| `spot_college_break_left` | character | NBA or WNBA Stats value for spot college break left in the draftcombinestats result set. |
| `spot_college_top_key` | character | NBA or WNBA Stats value for spot college top key in the draftcombinestats result set. |
| `spot_college_break_right` | character | NBA or WNBA Stats value for spot college break right in the draftcombinestats result set. |
| `spot_college_corner_right` | character | NBA or WNBA Stats value for spot college corner right in the draftcombinestats result set. |
| `spot_nba_corner_left` | character | NBA or WNBA Stats value for spot NBA corner left in the draftcombinestats result set. |
| `spot_nba_break_left` | character | NBA or WNBA Stats value for spot NBA break left in the draftcombinestats result set. |
| `spot_nba_top_key` | character | NBA or WNBA Stats value for spot NBA top key in the draftcombinestats result set. |
| `spot_nba_break_right` | character | NBA or WNBA Stats value for spot NBA break right in the draftcombinestats result set. |
| `spot_nba_corner_right` | character | NBA or WNBA Stats value for spot NBA corner right in the draftcombinestats result set. |
| `off_drib_fifteen_break_left` | character | NBA or WNBA Stats value for offensive drib fifteen break left in the draftcombinestats result set. |
| `off_drib_fifteen_top_key` | character | NBA or WNBA Stats value for offensive drib fifteen top key in the draftcombinestats result set. |
| `off_drib_fifteen_break_right` | character | NBA or WNBA Stats value for offensive drib fifteen break right in the draftcombinestats result set. |
| `off_drib_college_break_left` | character | NBA or WNBA Stats value for offensive drib college break left in the draftcombinestats result set. |
| `off_drib_college_top_key` | character | NBA or WNBA Stats value for offensive drib college top key in the draftcombinestats result set. |
| `off_drib_college_break_right` | character | NBA or WNBA Stats value for offensive drib college break right in the draftcombinestats result set. |
| `on_move_fifteen` | character | NBA or WNBA Stats value for on move fifteen in the draftcombinestats result set. |
| `on_move_college` | character | NBA or WNBA Stats value for on move college in the draftcombinestats result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_draftcombinestats(league_id='10')
```

_Last validated n/a._

## `wnba_stats_drafthistory`

GET /stats/drafthistory

**Endpoint URL:** `GET https://stats.wnba.com/stats/drafthistory`

**Valid URL:** [https://stats.wnba.com/stats/drafthistory?LeagueID=10](https://stats.wnba.com/stats/drafthistory?LeagueID=10)

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
wnba_stats_drafthistory(league_id='10')
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
| `min` | integer | Minutes played. |
| `fan_duel_pts` | character | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | character | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `pts` | character | Points scored. |
| `reb` | character | Total rebounds. |
| `ast` | character | Assists. |
| `blk` | character | Blocks. |
| `stl` | character | Steals. |
| `tov` | character | Turnovers. |
| `fg3m` | character | Three-point field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fta` | character | Free throw attempts. |
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
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `pts_per48` | character | Scoring or score-margin metric for points per48 in the requested NBA or WNBA Stats split. |

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
| `rank` | character | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `blk` | character | Blocks. |

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
| `start_position` | character | Starting lineup position code for the player in this game or roster row. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `pts` | integer | Points scored. |
| `contested_shots` | numeric | Defensively contested shots. |
| `contested_shots_2pt` | numeric | Shooting metric for contested shots 2pt in the requested NBA or WNBA Stats split. |
| `contested_shots_3pt` | numeric | Shooting metric for contested shots 3pt in the requested NBA or WNBA Stats split. |
| `deflections` | numeric | Defensive deflections. |
| `charges_drawn` | numeric | Charges drawn. |
| `screen_assists` | numeric | Screen assists (resulting in a basket). |
| `screen_ast_pts` | numeric | Scoring or score-margin metric for screen assists points in the requested NBA or WNBA Stats split. |
| `off_loose_balls_recovered` | numeric | Turnover or loose-ball metric for offensive loose balls recovered in the requested NBA or WNBA Stats split. |
| `def_loose_balls_recovered` | numeric | Turnover or loose-ball metric for defensive loose balls recovered in the requested NBA or WNBA Stats split. |
| `loose_balls_recovered` | numeric | Turnover or loose-ball metric for loose balls recovered in the requested NBA or WNBA Stats split. |
| `off_boxouts` | numeric | Rebounding metric for offensive boxouts in the requested NBA or WNBA Stats split. |
| `def_boxouts` | numeric | Rebounding metric for defensive boxouts in the requested NBA or WNBA Stats split. |
| `box_out_player_team_rebs` | numeric | Rebounding metric for box out player team rebs in the requested NBA or WNBA Stats split. |
| `box_out_player_rebs` | numeric | Rebounding metric for box out player rebs in the requested NBA or WNBA Stats split. |
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
| `rank` | character | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pts` | character | Points scored. |

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
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `age` | numeric | Player age (in years). |
| `gp` | integer | Games played. |
| `w` | character | Wins. |
| `l` | character | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
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
| `tov` | character | Turnovers. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `blka` | character | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | character | Personal fouls. |
| `pfd` | character | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | character | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | character | Triple-doubles for the requested NBA or WNBA Stats split. |
| `gp_rank` | character | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | character | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | character | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | numeric | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | character | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | character | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | character | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | numeric | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | character | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | character | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | numeric | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | character | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | character | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | numeric | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
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
| `nba_fantasy_pts_rank` | character | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | character | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | character | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `cfid` | character | NBA Stats custom-filter identifier attached to the row or request context. |
| `cfparams` | character | NBA Stats custom-filter parameter string attached to the row or request context. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashplayerclutch(league_id='10')
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
| `g` | integer | Goals (skaters). |
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
| `w` | character | Wins. |
| `l` | character | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
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
| `w_pct_rank` | numeric | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | character | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | character | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | character | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | numeric | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | character | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | character | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | numeric | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | character | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | character | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | numeric | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
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
| `cfid` | character | NBA Stats custom-filter identifier attached to the row or request context. |
| `cfparams` | character | NBA Stats custom-filter parameter string attached to the row or request context. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_leaguedashteamclutch(league_id='10')
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
| `season_id` | integer | Unique season identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | integer | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `min` | integer | Minutes played. |
| `pts` | character | Points scored. |
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
| `plus_minus` | character | Plus/minus point differential while on court. |

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
| `season_id` | integer | Unique season identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | integer | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
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
| `plus_minus` | character | Plus/minus point differential while on court. |
| `video_available` | character | Video available. |

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
| `rank` | character | Whether to include statistical ranks in the returned table. |
| `player` | character | Player name. |
| `team` | character | Team-side label or team identifier. |
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
| `eff` | character | Eff. |
| `ast_tov` | character | Assist-to-turnover ratio for the requested NBA or WNBA Stats split. |
| `stl_tov` | character | Turnover or loose-ball metric for steals turnovers in the requested NBA or WNBA Stats split. |

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
| `group_id` | integer | ESPN group id. |
| `group_name` | character | Group name (conference / division). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `min` | integer | Minutes played. |
| `off_rating` | character | Offensive rating for the requested NBA or WNBA Stats split. |
| `def_rating` | character | Defensive rating for the requested NBA or WNBA Stats split. |
| `net_rating` | character | Net rating (off rating - def rating). |
| `pace` | character | Possessions per 48 minutes. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `fta_rate` | character | NBA or WNBA Stats value for fta rate in the leaguelineupviz result set. |
| `tm_ast_pct` | numeric | Percentage or rate for team assists percentage in the requested NBA or WNBA Stats split. |
| `pct_fga_2pt` | numeric | Percentage share of fga 2pt for the requested NBA or WNBA Stats split. |
| `pct_fga_3pt` | numeric | Percentage share of fga 3pt for the requested NBA or WNBA Stats split. |
| `pct_pts_2pt_mr` | numeric | Percentage share of points 2pt mr for the requested NBA or WNBA Stats split. |
| `pct_pts_fb` | numeric | Percentage share of points fb for the requested NBA or WNBA Stats split. |
| `pct_pts_ft` | numeric | Percentage share of points free throws for the requested NBA or WNBA Stats split. |
| `pct_pts_paint` | numeric | Percentage share of points paint for the requested NBA or WNBA Stats split. |
| `pct_ast_fgm` | numeric | Percentage share of assists fgm for the requested NBA or WNBA Stats split. |
| `pct_uast_fgm` | numeric | Percentage share of uast fgm for the requested NBA or WNBA Stats split. |
| `opp_fg3_pct` | numeric | Opponent three-point field goals percentage for the requested NBA or WNBA team, player, lineup, or game split. |
| `opp_efg_pct` | numeric | Opponent efg percentage for the requested NBA or WNBA team, player, lineup, or game split. |
| `opp_fta_rate` | character | Opponent fta rate for the requested NBA or WNBA team, player, lineup, or game split. |
| `opp_tov_pct` | numeric | Opponent turnovers percentage for the requested NBA or WNBA team, player, lineup, or game split. |

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
| `teamid` | integer | FanGraphs team ID. |
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
| `ot` | character | Overtime results. |
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

## `wnba_stats_playbyplay`

GET /stats/playbyplay

**Endpoint URL:** `GET https://stats.wnba.com/stats/playbyplay`

**Valid URL:** [https://stats.wnba.com/stats/playbyplay](https://stats.wnba.com/stats/playbyplay)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `eventnum` | character | NBA or WNBA Stats value for eventnum in the playbyplay result set. |
| `eventmsgtype` | character | NBA or WNBA Stats value for eventmsgtype in the playbyplay result set. |
| `eventmsgactiontype` | character | NBA or WNBA Stats value for eventmsgactiontype in the playbyplay result set. |
| `period` | character | Period of the game (1-4 quarters; 5+ for OT). |
| `wctimestring` | character | Time value for wall-clock time in the NBA or WNBA Stats result set. |
| `pctimestring` | numeric | Time value for period clock time in the NBA or WNBA Stats result set. |
| `homedescription` | character | Play-by-play text description from the home-team perspective. |
| `neutraldescription` | character | Neutral play-by-play text description for the event. |
| `visitordescription` | character | Play-by-play text description from the visiting-team perspective. |
| `score` | character | Final score. |
| `scoremargin` | character | Scoring or score-margin metric for scoremargin in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playbyplay()
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
| `eventnum` | integer | NBA or WNBA Stats value for eventnum in the playbyplayv2 result set. |
| `eventmsgtype` | integer | NBA or WNBA Stats value for eventmsgtype in the playbyplayv2 result set. |
| `eventmsgactiontype` | integer | NBA or WNBA Stats value for eventmsgactiontype in the playbyplayv2 result set. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `wctimestring` | character | Time value for wall-clock time in the NBA or WNBA Stats result set. |
| `pctimestring` | character | Time value for period clock time in the NBA or WNBA Stats result set. |
| `homedescription` | character | Play-by-play text description from the home-team perspective. |
| `neutraldescription` | character | Neutral play-by-play text description for the event. |
| `visitordescription` | character | Play-by-play text description from the visiting-team perspective. |
| `score` | character | Final score. |
| `scoremargin` | character | Scoring or score-margin metric for scoremargin in the requested NBA or WNBA Stats split. |
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

## `wnba_stats_playercareerbycollege`

GET /stats/playercareerbycollege

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercareerbycollege`

**Valid URL:** [https://stats.wnba.com/stats/playercareerbycollege?LeagueID=10](https://stats.wnba.com/stats/playercareerbycollege?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `college` | character | College or school attended. |
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
wnba_stats_playercareerbycollege(league_id='10')
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
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `description` | character | Long-form description text. |
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
| `tov` | character | Turnovers. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `blka` | character | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | character | Personal fouls. |
| `pfd` | character | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |

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

**Valid URL:** [https://stats.wnba.com/stats/playerdashboardbyopponent](https://stats.wnba.com/stats/playerdashboardbyopponent)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerdashboardbyopponent()
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
| `g` | integer | Goals (skaters). |
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

**Valid URL:** [https://stats.wnba.com/stats/playerfantasyprofile](https://stats.wnba.com/stats/playerfantasyprofile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playerfantasyprofile()
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
| `game_id` | integer | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `home_team_name` | character | Home team name. |
| `visitor_team_name` | character | Team name for the visiting team in this NBA or WNBA Stats row. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `visitor_team_abbreviation` | character | Team abbreviation for the visiting team in this NBA or WNBA Stats row. |
| `home_team_nickname` | character | Home team nickname label; `team_detail = TRUE` only. |
| `visitor_team_nickname` | character | Team nickname for the visiting team in this NBA or WNBA Stats row. |
| `game_time` | character | Scheduled start time of the game. |
| `home_wl` | character | Win-loss result for the home team in this NBA or WNBA Stats row. |
| `visitor_wl` | character | Win-loss result for the visiting team in this NBA or WNBA Stats row. |

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
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API identifier for vs player identifier associated with this NBA or WNBA Stats row. |
| `vs_player_name` | character | Display name for vs player name associated with this NBA or WNBA Stats row. |
| `court_status` | character | Indicates whether the compared player was on court or off court for the split row. |
| `gp` | integer | Games played. |
| `w` | character | Wins. |
| `l` | character | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
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
| `tov` | character | Turnovers. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `blka` | character | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | character | Personal fouls. |
| `pfd` | character | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `cfid` | character | NBA Stats custom-filter identifier attached to the row or request context. |
| `cfparams` | character | NBA Stats custom-filter parameter string attached to the row or request context. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playervsplayer(league_id='10')
```

_Last validated n/a._

## `wnba_stats_playoffpicture`

GET /stats/playoffpicture

**Endpoint URL:** `GET https://stats.wnba.com/stats/playoffpicture`

**Valid URL:** [https://stats.wnba.com/stats/playoffpicture?LeagueID=10](https://stats.wnba.com/stats/playoffpicture?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonID` | `season_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `conference` | character | Filter players or teams by conference. |
| `rank` | character | Whether to include statistical ranks in the returned table. |
| `team` | character | Team-side label or team identifier. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_id` | integer | Unique team identifier. |
| `wins` | character | Total wins. |
| `losses` | character | Total losses. |
| `pct` | numeric | Win percentage. |
| `div` | character | NBA or WNBA Stats value for div in the playoffpicture result set. |
| `conf` | character | character. |
| `home` | character | Home. |
| `away` | character | Away team shots in the period. |
| `gb` | character | Average exit velocity on ground balls (mph). |
| `gr_over_500` | character | NBA or WNBA Stats value for gr over 500 in the playoffpicture result set. |
| `gr_over_500_home` | character | NBA or WNBA Stats value for gr over 500 home in the playoffpicture result set. |
| `gr_over_500_away` | character | NBA or WNBA Stats value for gr over 500 away in the playoffpicture result set. |
| `gr_under_500` | character | NBA or WNBA Stats value for gr under 500 in the playoffpicture result set. |
| `gr_under_500_home` | character | NBA or WNBA Stats value for gr under 500 home in the playoffpicture result set. |
| `gr_under_500_away` | character | NBA or WNBA Stats value for gr under 500 away in the playoffpicture result set. |
| `ranking_criteria` | character | NBA or WNBA Stats value for ranking criteria in the playoffpicture result set. |
| `clinched_playoffs` | character | Flag indicating clinched playoffs for the requested NBA or WNBA Stats context. |
| `clinched_conference` | character | Flag indicating clinched conference for the requested NBA or WNBA Stats context. |
| `clinched_division` | character | Flag indicating clinched division for the requested NBA or WNBA Stats context. |
| `clinched_play_in` | character | Flag indicating clinched play in for the requested NBA or WNBA Stats context. |
| `eliminated_playoffs` | character | Flag indicating eliminated playoffs for the requested NBA or WNBA Stats context. |
| `sosa_remaining` | character | NBA or WNBA Stats value for sosa remaining in the playoffpicture result set. |
| `returntoplay_east_pi_flag` | character | Flag indicating returntoplay east pi flag for the requested NBA or WNBA Stats context. |
| `returntoplay_already_eliminated` | character | NBA or WNBA Stats value for returntoplay already eliminated in the playoffpicture result set. |
| `seeding_game_1_outcome` | character | Outcome for seeding game 1 in the playoff or return-to-play picture. |
| `seeding_game_2_outcome` | character | Outcome for seeding game 2 in the playoff or return-to-play picture. |
| `seeding_game_3_outcome` | character | Outcome for seeding game 3 in the playoff or return-to-play picture. |
| `seeding_game_4_outcome` | character | Outcome for seeding game 4 in the playoff or return-to-play picture. |
| `seeding_game_5_outcome` | character | Outcome for seeding game 5 in the playoff or return-to-play picture. |
| `seeding_game_6_outcome` | character | Outcome for seeding game 6 in the playoff or return-to-play picture. |
| `seeding_game_7_outcome` | character | Outcome for seeding game 7 in the playoff or return-to-play picture. |
| `seeding_game_8_outcome` | character | Outcome for seeding game 8 in the playoff or return-to-play picture. |
| `seeding_game_1_id` | integer | Identifier for seeding game 1 in the playoff or return-to-play picture. |
| `seeding_game_2_id` | integer | Identifier for seeding game 2 in the playoff or return-to-play picture. |
| `seeding_game_3_id` | integer | Identifier for seeding game 3 in the playoff or return-to-play picture. |
| `seeding_game_4_id` | integer | Identifier for seeding game 4 in the playoff or return-to-play picture. |
| `seeding_game_5_id` | integer | Identifier for seeding game 5 in the playoff or return-to-play picture. |
| `seeding_game_6_id` | integer | Identifier for seeding game 6 in the playoff or return-to-play picture. |
| `seeding_game_7_id` | integer | Identifier for seeding game 7 in the playoff or return-to-play picture. |
| `seeding_game_8_id` | integer | Identifier for seeding game 8 in the playoff or return-to-play picture. |
| `seeding_game_1_opponent` | character | Opponent for seeding game 1 in the playoff or return-to-play picture. |
| `seeding_game_2_opponent` | character | Opponent for seeding game 2 in the playoff or return-to-play picture. |
| `seeding_game_3_opponent` | character | Opponent for seeding game 3 in the playoff or return-to-play picture. |
| `seeding_game_4_opponent` | character | Opponent for seeding game 4 in the playoff or return-to-play picture. |
| `seeding_game_5_opponent` | character | Opponent for seeding game 5 in the playoff or return-to-play picture. |
| `seeding_game_6_opponent` | character | Opponent for seeding game 6 in the playoff or return-to-play picture. |
| `seeding_game_7_opponent` | character | Opponent for seeding game 7 in the playoff or return-to-play picture. |
| `seeding_game_8_opponent` | character | Opponent for seeding game 8 in the playoff or return-to-play picture. |
| `seeding_game_1_label` | character | Label for seeding game 1 in the playoff or return-to-play picture. |
| `seeding_game_2_label` | character | Label for seeding game 2 in the playoff or return-to-play picture. |
| `seeding_game_3_label` | character | Label for seeding game 3 in the playoff or return-to-play picture. |
| `seeding_game_4_label` | character | Label for seeding game 4 in the playoff or return-to-play picture. |
| `seeding_game_5_label` | character | Label for seeding game 5 in the playoff or return-to-play picture. |
| `seeding_game_6_label` | character | Label for seeding game 6 in the playoff or return-to-play picture. |
| `seeding_game_7_label` | character | Label for seeding game 7 in the playoff or return-to-play picture. |
| `seeding_game_8_label` | character | Label for seeding game 8 in the playoff or return-to-play picture. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_playoffpicture(league_id='10')
```

_Last validated n/a._

## `wnba_stats_scoreboard`

GET /stats/scoreboard

**Endpoint URL:** `GET https://stats.wnba.com/stats/scoreboard`

**Valid URL:** [https://stats.wnba.com/stats/scoreboard](https://stats.wnba.com/stats/scoreboard)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_scoreboard()
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
| `pts_ot2` | character | Scoring or score-margin metric for points ot2 in the requested NBA or WNBA Stats split. |
| `pts_ot3` | character | Scoring or score-margin metric for points ot3 in the requested NBA or WNBA Stats split. |
| `pts_ot4` | character | Scoring or score-margin metric for points ot4 in the requested NBA or WNBA Stats split. |
| `pts_ot5` | character | Scoring or score-margin metric for points ot5 in the requested NBA or WNBA Stats split. |
| `pts_ot6` | character | Scoring or score-margin metric for points ot6 in the requested NBA or WNBA Stats split. |
| `pts_ot7` | character | Scoring or score-margin metric for points ot7 in the requested NBA or WNBA Stats split. |
| `pts_ot8` | character | Scoring or score-margin metric for points ot8 in the requested NBA or WNBA Stats split. |
| `pts_ot9` | character | Scoring or score-margin metric for points ot9 in the requested NBA or WNBA Stats split. |
| `pts_ot10` | character | Scoring or score-margin metric for points ot10 in the requested NBA or WNBA Stats split. |
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

**Valid URL:** [https://stats.wnba.com/stats/scoreboardv3](https://stats.wnba.com/stats/scoreboardv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_scoreboardv3()
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
| `grid_type` | character | NBA or WNBA Stats value for grid type in the shotchartdetail result set. |
| `game_id` | integer | Unique game identifier. |
| `game_event_id` | integer | Unique identifier for game event. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
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
| `htm` | character | NBA or WNBA Stats value for htm in the shotchartdetail result set. |
| `vtm` | character | NBA or WNBA Stats value for vtm in the shotchartdetail result set. |

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
| `grid_type` | character | NBA or WNBA Stats value for grid type in the shotchartlineupdetail result set. |
| `game_id` | integer | Unique game identifier. |
| `game_event_id` | integer | Unique identifier for game event. |
| `group_id` | integer | ESPN group id. |
| `group_name` | character | Group name (conference / division). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
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
| `htm` | character | NBA or WNBA Stats value for htm in the shotchartlineupdetail result set. |
| `vtm` | character | NBA or WNBA Stats value for vtm in the shotchartlineupdetail result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_shotchartlineupdetail(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamdashboardbyclutch`

GET /stats/teamdashboardbyclutch

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbyclutch`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbyclutch](https://stats.wnba.com/stats/teamdashboardbyclutch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbyclutch()
```

_Last validated n/a._

## `wnba_stats_teamdashboardbygamesplits`

GET /stats/teamdashboardbygamesplits

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbygamesplits`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbygamesplits](https://stats.wnba.com/stats/teamdashboardbygamesplits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbygamesplits()
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

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbylastngames](https://stats.wnba.com/stats/teamdashboardbylastngames)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbylastngames()
```

_Last validated n/a._

## `wnba_stats_teamdashboardbyopponent`

GET /stats/teamdashboardbyopponent

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbyopponent`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbyopponent](https://stats.wnba.com/stats/teamdashboardbyopponent)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbyopponent()
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

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbyteamperformance](https://stats.wnba.com/stats/teamdashboardbyteamperformance)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbyteamperformance()
```

_Last validated n/a._

## `wnba_stats_teamdashboardbyyearoveryear`

GET /stats/teamdashboardbyyearoveryear

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdashboardbyyearoveryear`

**Valid URL:** [https://stats.wnba.com/stats/teamdashboardbyyearoveryear](https://stats.wnba.com/stats/teamdashboardbyyearoveryear)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamdashboardbyyearoveryear()
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
| `yearfounded` | character | NBA or WNBA Stats value for yearfounded in the teamdetails result set. |
| `city` | character | Venue city. |
| `arena` | character | Arena. |
| `arenacapacity` | character | NBA or WNBA Stats value for arenacapacity in the teamdetails result set. |
| `owner` | character | NBA or WNBA Stats value for owner in the teamdetails result set. |
| `generalmanager` | character | NBA or WNBA Stats value for generalmanager in the teamdetails result set. |
| `headcoach` | character | NBA or WNBA Stats value for headcoach in the teamdetails result set. |
| `dleagueaffiliation` | character | NBA or WNBA Stats value for dleagueaffiliation in the teamdetails result set. |

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

## `wnba_stats_teamgamestreakfinder`

GET /stats/teamgamestreakfinder

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamgamestreakfinder`

**Valid URL:** [https://stats.wnba.com/stats/teamgamestreakfinder?LeagueID=10](https://stats.wnba.com/stats/teamgamestreakfinder?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ActiveStreaksOnly` | `active_streaks_only_nullable` |  |  | `Y` |  |
| `ActiveTeamsOnly` | `active_teams_only_nullable` |  |  | `Y` |  |
| `BtrOPPAST` | `btr_opp_ast_nullable` |  |  | `Y` |  |
| `BtrOPPBLK` | `btr_opp_blk_nullable` |  |  | `Y` |  |
| `BtrOPPDREB` | `btr_opp_dreb_nullable` |  |  | `Y` |  |
| `BtrOPPFG3A` | `btr_opp_fg3a_nullable` |  |  | `Y` |  |
| `BtrOPPFG3M` | `btr_opp_fg3m_nullable` |  |  | `Y` |  |
| `BtrOPPFG3PCT` | `btr_opp_fg3_pct_nullable` |  |  | `Y` |  |
| `BtrOPPFGA` | `btr_opp_fga_nullable` |  |  | `Y` |  |
| `BtrOPPFGM` | `btr_opp_fgm_nullable` |  |  | `Y` |  |
| `BtrOPPFG_PCT` | `btr_opp_fg_pct_nullable` |  |  | `Y` |  |
| `BtrOPPFTA` | `btr_opp_fta_nullable` |  |  | `Y` |  |
| `BtrOPPFTM` | `btr_opp_ftm_nullable` |  |  | `Y` |  |
| `BtrOPPFT_PCT` | `btr_opp_ft_pct_nullable` |  |  | `Y` |  |
| `BtrOPPOREB` | `btr_opp_oreb_nullable` |  |  | `Y` |  |
| `BtrOPPPF` | `btr_opp_pf_nullable` |  |  | `Y` |  |
| `BtrOPPPTS` | `btr_opp_pts_nullable` |  |  | `Y` |  |
| `BtrOPPPTS2NDCHANCE` | `btr_opp_pts2nd_chance_nullable` |  |  | `Y` |  |
| `BtrOPPPTSFB` | `btr_opp_pts_fb_nullable` |  |  | `Y` |  |
| `BtrOPPPTSOFFTOV` | `btr_opp_pts_off_tov_nullable` |  |  | `Y` |  |
| `BtrOPPPTSPAINT` | `btr_opp_pts_paint_nullable` |  |  | `Y` |  |
| `BtrOPPREB` | `btr_opp_reb_nullable` |  |  | `Y` |  |
| `BtrOPPSTL` | `btr_opp_stl_nullable` |  |  | `Y` |  |
| `BtrOPPTOV` | `btr_opp_tov_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
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
| `EqOPPPTS2NDCHANCE` | `eq_opp_pts2nd_chance_nullable` |  |  | `Y` |  |
| `EqOPPPTSFB` | `eq_opp_pts_fb_nullable` |  |  | `Y` |  |
| `EqOPPPTSOFFTOV` | `eq_opp_pts_off_tov_nullable` |  |  | `Y` |  |
| `EqOPPPTSPAINT` | `eq_opp_pts_paint_nullable` |  |  | `Y` |  |
| `EqOREB` | `eq_oreb_nullable` |  |  | `Y` |  |
| `EqPF` | `eq_pf_nullable` |  |  | `Y` |  |
| `EqPTS` | `eq_pts_nullable` |  |  | `Y` |  |
| `EqPTS2NDCHANCE` | `eq_pts2nd_chance_nullable` |  |  | `Y` |  |
| `EqPTSFB` | `eq_pts_fb_nullable` |  |  | `Y` |  |
| `EqPTSOFFTOV` | `eq_pts_off_tov_nullable` |  |  | `Y` |  |
| `EqPTSPAINT` | `eq_pts_paint_nullable` |  |  | `Y` |  |
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
| `GtOPPAST` | `gt_opp_ast_nullable` |  |  | `Y` |  |
| `GtOPPBLK` | `gt_opp_blk_nullable` |  |  | `Y` |  |
| `GtOPPDREB` | `gt_opp_dreb_nullable` |  |  | `Y` |  |
| `GtOPPFG3A` | `gt_opp_fg3a_nullable` |  |  | `Y` |  |
| `GtOPPFG3M` | `gt_opp_fg3m_nullable` |  |  | `Y` |  |
| `GtOPPFG3PCT` | `gt_opp_fg3_pct_nullable` |  |  | `Y` |  |
| `GtOPPFGA` | `gt_opp_fga_nullable` |  |  | `Y` |  |
| `GtOPPFGM` | `gt_opp_fgm_nullable` |  |  | `Y` |  |
| `GtOPPFG_PCT` | `gt_opp_fg_pct_nullable` |  |  | `Y` |  |
| `GtOPPFTA` | `gt_opp_fta_nullable` |  |  | `Y` |  |
| `GtOPPFTM` | `gt_opp_ftm_nullable` |  |  | `Y` |  |
| `GtOPPFT_PCT` | `gt_opp_ft_pct_nullable` |  |  | `Y` |  |
| `GtOPPOREB` | `gt_opp_oreb_nullable` |  |  | `Y` |  |
| `GtOPPPF` | `gt_opp_pf_nullable` |  |  | `Y` |  |
| `GtOPPPTS` | `gt_opp_pts_nullable` |  |  | `Y` |  |
| `GtOPPPTS2NDCHANCE` | `gt_opp_pts2nd_chance_nullable` |  |  | `Y` |  |
| `GtOPPPTSFB` | `gt_opp_pts_fb_nullable` |  |  | `Y` |  |
| `GtOPPPTSOFFTOV` | `gt_opp_pts_off_tov_nullable` |  |  | `Y` |  |
| `GtOPPPTSPAINT` | `gt_opp_pts_paint_nullable` |  |  | `Y` |  |
| `GtOPPREB` | `gt_opp_reb_nullable` |  |  | `Y` |  |
| `GtOPPSTL` | `gt_opp_stl_nullable` |  |  | `Y` |  |
| `GtOPPTOV` | `gt_opp_tov_nullable` |  |  | `Y` |  |
| `GtOREB` | `gt_oreb_nullable` |  |  | `Y` |  |
| `GtPF` | `gt_pf_nullable` |  |  | `Y` |  |
| `GtPTS` | `gt_pts_nullable` |  |  | `Y` |  |
| `GtPTS2NDCHANCE` | `gt_pts2nd_chance_nullable` |  |  | `Y` |  |
| `GtPTSFB` | `gt_pts_fb_nullable` |  |  | `Y` |  |
| `GtPTSOFFTOV` | `gt_pts_off_tov_nullable` |  |  | `Y` |  |
| `GtPTSPAINT` | `gt_pts_paint_nullable` |  |  | `Y` |  |
| `GtREB` | `gt_reb_nullable` |  |  | `Y` |  |
| `GtSTL` | `gt_stl_nullable` |  |  | `Y` |  |
| `GtTD` | `gt_td_nullable` |  |  | `Y` |  |
| `GtTOV` | `gt_tov_nullable` |  |  | `Y` |  |
| `LStreak` | `lstreak_nullable` |  |  | `Y` |  |
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
| `LtOPPAST` | `lt_opp_ast_nullable` |  |  | `Y` |  |
| `LtOPPBLK` | `lt_opp_blk_nullable` |  |  | `Y` |  |
| `LtOPPDREB` | `lt_opp_dreb_nullable` |  |  | `Y` |  |
| `LtOPPFG3A` | `lt_opp_fg3a_nullable` |  |  | `Y` |  |
| `LtOPPFG3M` | `lt_opp_fg3m_nullable` |  |  | `Y` |  |
| `LtOPPFG3PCT` | `lt_opp_fg3_pct_nullable` |  |  | `Y` |  |
| `LtOPPFGA` | `lt_opp_fga_nullable` |  |  | `Y` |  |
| `LtOPPFGM` | `lt_opp_fgm_nullable` |  |  | `Y` |  |
| `LtOPPFG_PCT` | `lt_opp_fg_pct_nullable` |  |  | `Y` |  |
| `LtOPPFTA` | `lt_opp_fta_nullable` |  |  | `Y` |  |
| `LtOPPFTM` | `lt_opp_ftm_nullable` |  |  | `Y` |  |
| `LtOPPFT_PCT` | `lt_opp_ft_pct_nullable` |  |  | `Y` |  |
| `LtOPPOREB` | `lt_opp_oreb_nullable` |  |  | `Y` |  |
| `LtOPPPF` | `lt_opp_pf_nullable` |  |  | `Y` |  |
| `LtOPPPTS` | `lt_opp_pts_nullable` |  |  | `Y` |  |
| `LtOPPPTS2NDCHANCE` | `lt_opp_pts2nd_chance_nullable` |  |  | `Y` |  |
| `LtOPPPTSFB` | `lt_opp_pts_fb_nullable` |  |  | `Y` |  |
| `LtOPPPTSOFFTOV` | `lt_opp_pts_off_tov_nullable` |  |  | `Y` |  |
| `LtOPPPTSPAINT` | `lt_opp_pts_paint_nullable` |  |  | `Y` |  |
| `LtOPPREB` | `lt_opp_reb_nullable` |  |  | `Y` |  |
| `LtOPPSTL` | `lt_opp_stl_nullable` |  |  | `Y` |  |
| `LtOPPTOV` | `lt_opp_tov_nullable` |  |  | `Y` |  |
| `LtOREB` | `lt_oreb_nullable` |  |  | `Y` |  |
| `LtPF` | `lt_pf_nullable` |  |  | `Y` |  |
| `LtPTS` | `lt_pts_nullable` |  |  | `Y` |  |
| `LtPTS2NDCHANCE` | `lt_pts2nd_chance_nullable` |  |  | `Y` |  |
| `LtPTSFB` | `lt_pts_fb_nullable` |  |  | `Y` |  |
| `LtPTSOFFTOV` | `lt_pts_off_tov_nullable` |  |  | `Y` |  |
| `LtPTSPAINT` | `lt_pts_paint_nullable` |  |  | `Y` |  |
| `LtREB` | `lt_reb_nullable` |  |  | `Y` |  |
| `LtSTL` | `lt_stl_nullable` |  |  | `Y` |  |
| `LtTD` | `lt_td_nullable` |  |  | `Y` |  |
| `LtTOV` | `lt_tov_nullable` |  |  | `Y` |  |
| `MinGames` | `min_games_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |
| `WStreak` | `wstreak_nullable` |  |  | `Y` |  |
| `WrsOPPAST` | `wrs_opp_ast_nullable` |  |  | `Y` |  |
| `WrsOPPBLK` | `wrs_opp_blk_nullable` |  |  | `Y` |  |
| `WrsOPPDREB` | `wrs_opp_dreb_nullable` |  |  | `Y` |  |
| `WrsOPPFG3A` | `wrs_opp_fg3a_nullable` |  |  | `Y` |  |
| `WrsOPPFG3M` | `wrs_opp_fg3m_nullable` |  |  | `Y` |  |
| `WrsOPPFG3PCT` | `wrs_opp_fg3_pct_nullable` |  |  | `Y` |  |
| `WrsOPPFGA` | `wrs_opp_fga_nullable` |  |  | `Y` |  |
| `WrsOPPFGM` | `wrs_opp_fgm_nullable` |  |  | `Y` |  |
| `WrsOPPFG_PCT` | `wrs_opp_fg_pct_nullable` |  |  | `Y` |  |
| `WrsOPPFTA` | `wrs_opp_fta_nullable` |  |  | `Y` |  |
| `WrsOPPFTM` | `wrs_opp_ftm_nullable` |  |  | `Y` |  |
| `WrsOPPFT_PCT` | `wrs_opp_ft_pct_nullable` |  |  | `Y` |  |
| `WrsOPPOREB` | `wrs_opp_oreb_nullable` |  |  | `Y` |  |
| `WrsOPPPF` | `wrs_opp_pf_nullable` |  |  | `Y` |  |
| `WrsOPPPTS` | `wrs_opp_pts_nullable` |  |  | `Y` |  |
| `WrsOPPPTS2NDCHANCE` | `wrs_opp_pts2nd_chance_nullable` |  |  | `Y` |  |
| `WrsOPPPTSFB` | `wrs_opp_pts_fb_nullable` |  |  | `Y` |  |
| `WrsOPPPTSOFFTOV` | `wrs_opp_pts_off_tov_nullable` |  |  | `Y` |  |
| `WrsOPPPTSPAINT` | `wrs_opp_pts_paint_nullable` |  |  | `Y` |  |
| `WrsOPPREB` | `wrs_opp_reb_nullable` |  |  | `Y` |  |
| `WrsOPPSTL` | `wrs_opp_stl_nullable` |  |  | `Y` |  |
| `WrsOPPTOV` | `wrs_opp_tov_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | integer | Unique team identifier. |
| `gamestreak` | character | NBA or WNBA Stats value for gamestreak in the teamgamestreakfinder result set. |
| `startdate` | character | Date or timestamp for startdate in the NBA or WNBA Stats result set. |
| `enddate` | character | Date or timestamp for enddate in the NBA or WNBA Stats result set. |
| `activestreak` | character | NBA or WNBA Stats value for activestreak in the teamgamestreakfinder result set. |
| `numseasons` | character | NBA or WNBA Stats value for numseasons in the teamgamestreakfinder result set. |
| `lastseason` | character | NBA or WNBA Stats value for lastseason in the teamgamestreakfinder result set. |
| `firstseason` | character | NBA or WNBA Stats value for firstseason in the teamgamestreakfinder result set. |
| `abbreviation` | character | Short abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamgamestreakfinder(league_id='10')
```

_Last validated n/a._

## `wnba_stats_teamhistoricalleaders`

GET /stats/teamhistoricalleaders

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamhistoricalleaders`

**Valid URL:** [https://stats.wnba.com/stats/teamhistoricalleaders?LeagueID=10](https://stats.wnba.com/stats/teamhistoricalleaders?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonID` | `season_id` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `pts` | character | Points scored. |
| `pts_person_id` | integer | Stats API identifier for points person identifier associated with this NBA or WNBA Stats row. |
| `pts_player` | character | Scoring or score-margin metric for points player in the requested NBA or WNBA Stats split. |
| `ast` | character | Assists. |
| `ast_person_id` | integer | Stats API identifier for assists person identifier associated with this NBA or WNBA Stats row. |
| `ast_player` | character | NBA or WNBA Stats value for assists player in the teamhistoricalleaders result set. |
| `reb` | character | Total rebounds. |
| `reb_person_id` | integer | Stats API identifier for rebounds person identifier associated with this NBA or WNBA Stats row. |
| `reb_player` | character | Rebounding metric for rebounds player in the requested NBA or WNBA Stats split. |
| `blk` | character | Blocks. |
| `blk_person_id` | integer | Stats API identifier for blocks person identifier associated with this NBA or WNBA Stats row. |
| `blk_player` | character | NBA or WNBA Stats value for blocks player in the teamhistoricalleaders result set. |
| `stl` | character | Steals. |
| `stl_person_id` | integer | Stats API identifier for steals person identifier associated with this NBA or WNBA Stats row. |
| `stl_player` | character | NBA or WNBA Stats value for steals player in the teamhistoricalleaders result set. |
| `season_year` | character | Season year string ('YYYY-YY' format). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamhistoricalleaders(league_id='10')
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
| `team_conference` | character | NBA or WNBA Stats value for team conference in the teaminfocommon result set. |
| `team_division` | character | NBA or WNBA Stats value for team division in the teaminfocommon result set. |
| `team_code` | character | Internal team code. |
| `w` | character | Wins. |
| `l` | character | Losses. |
| `pct` | numeric | Win percentage. |
| `conf_rank` | character | Rank for conf within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `div_rank` | character | Rank for div within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
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
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_value` | character | Specific grouping value for this dashboard or split row. |
| `player_id` | integer | Unique player identifier. |
| `gp` | integer | Games played. |
| `w` | character | Wins. |
| `l` | character | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
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
| `tov` | character | Turnovers. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `blka` | character | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | character | Personal fouls. |
| `pfd` | character | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | character | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | character | Triple-doubles for the requested NBA or WNBA Stats split. |
| `gp_rank` | character | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | character | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | character | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | numeric | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | character | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | character | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | character | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | numeric | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | character | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | character | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | numeric | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | character | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | character | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | numeric | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
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
| `nba_fantasy_pts_rank` | character | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | character | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | character | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `cfid` | character | NBA Stats custom-filter identifier attached to the row or request context. |
| `cfparams` | character | NBA Stats custom-filter parameter string attached to the row or request context. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_teamvsplayer(league_id='10')
```

_Last validated n/a._

## `wnba_stats_videoevents`

GET /stats/videoevents

**Endpoint URL:** `GET https://stats.wnba.com/stats/videoevents`

**Valid URL:** [https://stats.wnba.com/stats/videoevents](https://stats.wnba.com/stats/videoevents)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameEventID` | `game_event_id` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_wnba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_videoevents()
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
| `visitor_team_city` | character | Team city for the visiting team in this NBA or WNBA Stats row. |
| `visitor_team_name` | character | Team name for the visiting team in this NBA or WNBA Stats row. |
| `visitor_team_abbreviation` | character | Team abbreviation for the visiting team in this NBA or WNBA Stats row. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `home_team_city` | character | Home team city / location. |
| `home_team_name` | character | Home team name. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `game_status` | character | Game status label. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `is_available` | character | Flag indicating is available for the requested NBA or WNBA Stats context. |
| `pt_xyz_available` | character | Pt xyz available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
wnba_stats_videostatus(league_id='10')
```

_Last validated n/a._
