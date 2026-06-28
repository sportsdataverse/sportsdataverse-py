---
title: NBA — NBA Stats API (stats.nba.com)
sidebar_label: NBA Stats API (stats.nba.com)
sidebar_position: 10
---
# NBA — NBA Stats API (stats.nba.com)

`sportsdataverse.nba` — 151 endpoints.

## `nba_stats_alltimeleadersgrids`

GET /stats/alltimeleadersgrids

**Endpoint URL:** `GET https://stats.nba.com/stats/alltimeleadersgrids`

**Valid URL:** [https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=00](https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=00)

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
| `ast` | character | Assists. |
| `ast_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_alltimeleadersgrids(league_id='00')
```

_Last validated n/a._

## `nba_stats_assistleaders`

GET /stats/assistleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/assistleaders`

**Valid URL:** [https://stats.nba.com/stats/assistleaders?LeagueID=00](https://stats.nba.com/stats/assistleaders?LeagueID=00)

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
| `rank` | character | Rank. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `ast` | character | Assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_assistleaders(league_id='00')
```

_Last validated n/a._

## `nba_stats_assisttracker`

GET /stats/assisttracker

**Endpoint URL:** `GET https://stats.nba.com/stats/assisttracker`

**Valid URL:** [https://stats.nba.com/stats/assisttracker?LeagueID=00](https://stats.nba.com/stats/assisttracker?LeagueID=00)

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
| `assists` | character | Total assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_assisttracker(league_id='00')
```

_Last validated n/a._

## `nba_stats_boxscoreadvancedv2`

GET /stats/boxscoreadvancedv2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreadvancedv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoreadvancedv2](https://stats.nba.com/stats/boxscoreadvancedv2)

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
| `start_position` | character |  |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `e_off_rating` | character |  |
| `off_rating` | character |  |
| `e_def_rating` | character |  |
| `def_rating` | character |  |
| `e_net_rating` | character |  |
| `net_rating` | character | Net rating (off rating - def rating). |
| `ast_pct` | numeric |  |
| `ast_tov` | character |  |
| `ast_ratio` | character |  |
| `oreb_pct` | numeric |  |
| `dreb_pct` | numeric |  |
| `reb_pct` | numeric |  |
| `tm_tov_pct` | numeric |  |
| `efg_pct` | numeric |  |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `usg_pct` | numeric |  |
| `e_usg_pct` | numeric |  |
| `e_pace` | character |  |
| `pace` | character | Possessions per 48 minutes. |
| `pace_per40` | character | Pace per40. |
| `poss` | character | Poss. |
| `pie` | character | Player Impact Estimate (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoreadvancedv2()
```

_Last validated n/a._

## `nba_stats_boxscoreadvancedv3`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personid` | character |  |
| `firstname` | character | Firstname. |
| `familyname` | character |  |
| `namei` | character |  |
| `playerslug` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character |  |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `estimatedoffensiverating` | character |  |
| `offensiverating` | character |  |
| `estimateddefensiverating` | character |  |
| `defensiverating` | character |  |
| `estimatednetrating` | character |  |
| `netrating` | character |  |
| `assistpercentage` | numeric |  |
| `assisttoturnover` | character |  |
| `assistratio` | character |  |
| `offensivereboundpercentage` | numeric |  |
| `defensivereboundpercentage` | numeric |  |
| `reboundpercentage` | numeric |  |
| `turnoverratio` | character |  |
| `effectivefieldgoalpercentage` | numeric |  |
| `trueshootingpercentage` | numeric |  |
| `usagepercentage` | numeric |  |
| `estimatedusagepercentage` | numeric |  |
| `estimatedpace` | character |  |
| `pace` | character | Possessions per 48 minutes. |
| `paceper40` | character |  |
| `possessions` | character | Possessions used. |
| `pie` | character | Player Impact Estimate (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoreadvancedv3()
```

_Last validated n/a._

## `nba_stats_boxscoredefensive`

GET /stats/boxscoredefensive

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoredefensive`

**Valid URL:** [https://stats.nba.com/stats/boxscoredefensive](https://stats.nba.com/stats/boxscoredefensive)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoredefensive()
```

_Last validated n/a._

## `nba_stats_boxscoredefensivev2`

GET /stats/boxscoredefensivev2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoredefensivev2`

**Valid URL:** [https://stats.nba.com/stats/boxscoredefensivev2](https://stats.nba.com/stats/boxscoredefensivev2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personid` | character |  |
| `firstname` | character | Firstname. |
| `familyname` | character |  |
| `namei` | character |  |
| `playerslug` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character |  |
| `matchupminutes` | character |  |
| `partialpossessions` | character |  |
| `switcheson` | character |  |
| `playerpoints` | character |  |
| `defensiverebounds` | character |  |
| `matchupassists` | character |  |
| `matchupturnovers` | character |  |
| `steals` | character | Total steals. |
| `blocks` | character | Total blocks. |
| `matchupfieldgoalsmade` | character |  |
| `matchupfieldgoalsattempted` | character |  |
| `matchupfieldgoalpercentage` | numeric |  |
| `matchupthreepointersmade` | character |  |
| `matchupthreepointersattempted` | character |  |
| `matchupthreepointerpercentage` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoredefensivev2()
```

_Last validated n/a._

## `nba_stats_boxscorefourfactorsv2`

GET /stats/boxscorefourfactorsv2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorefourfactorsv2`

**Valid URL:** [https://stats.nba.com/stats/boxscorefourfactorsv2](https://stats.nba.com/stats/boxscorefourfactorsv2)

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
| `start_position` | character |  |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `efg_pct` | numeric |  |
| `fta_rate` | character |  |
| `tm_tov_pct` | numeric |  |
| `oreb_pct` | numeric |  |
| `opp_efg_pct` | numeric |  |
| `opp_fta_rate` | character |  |
| `opp_tov_pct` | numeric |  |
| `opp_oreb_pct` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscorefourfactorsv2()
```

_Last validated n/a._

## `nba_stats_boxscorefourfactorsv3`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personid` | character |  |
| `firstname` | character | Firstname. |
| `familyname` | character |  |
| `namei` | character |  |
| `playerslug` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character |  |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `effectivefieldgoalpercentage` | numeric |  |
| `freethrowattemptrate` | character |  |
| `teamturnoverpercentage` | numeric |  |
| `offensivereboundpercentage` | numeric |  |
| `oppeffectivefieldgoalpercentage` | numeric |  |
| `oppfreethrowattemptrate` | character |  |
| `oppteamturnoverpercentage` | numeric |  |
| `oppoffensivereboundpercentage` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscorefourfactorsv3()
```

_Last validated n/a._

## `nba_stats_boxscorehustlev2`

GET /stats/boxscorehustlev2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorehustlev2`

**Valid URL:** [https://stats.nba.com/stats/boxscorehustlev2](https://stats.nba.com/stats/boxscorehustlev2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `gameid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personid` | character |  |
| `firstname` | character | Firstname. |
| `familyname` | character |  |
| `namei` | character |  |
| `playerslug` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character |  |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `points` | character | Points scored. |
| `contestedshots` | character |  |
| `contestedshots2pt` | character |  |
| `contestedshots3pt` | character |  |
| `deflections` | character | Defensive deflections. |
| `chargesdrawn` | character |  |
| `screenassists` | character |  |
| `screenassistpoints` | character |  |
| `looseballsrecoveredoffensive` | character |  |
| `looseballsrecovereddefensive` | character |  |
| `looseballsrecoveredtotal` | character |  |
| `offensiveboxouts` | character |  |
| `defensiveboxouts` | character |  |
| `boxoutplayerteamrebounds` | character |  |
| `boxoutplayerrebounds` | character |  |
| `boxouts` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscorehustlev2()
```

_Last validated n/a._

## `nba_stats_boxscorematchups`

GET /stats/boxscorematchups

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorematchups`

**Valid URL:** [https://stats.nba.com/stats/boxscorematchups](https://stats.nba.com/stats/boxscorematchups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscorematchups()
```

_Last validated n/a._

## `nba_stats_boxscorematchupsv3`

GET /stats/boxscorematchupsv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorematchupsv3`

**Valid URL:** [https://stats.nba.com/stats/boxscorematchupsv3](https://stats.nba.com/stats/boxscorematchupsv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personidoff` | character |  |
| `firstnameoff` | character |  |
| `familynameoff` | character |  |
| `nameioff` | character |  |
| `playerslugoff` | character |  |
| `jerseynumoff` | character |  |
| `personiddef` | character |  |
| `firstnamedef` | character |  |
| `familynamedef` | character |  |
| `nameidef` | character |  |
| `playerslugdef` | character |  |
| `positiondef` | character |  |
| `commentdef` | character |  |
| `jerseynumdef` | character |  |
| `matchupminutes` | character |  |
| `matchupminutessort` | character |  |
| `partialpossessions` | character |  |
| `percentagedefendertotaltime` | character |  |
| `percentageoffensivetotaltime` | character |  |
| `percentagetotaltimebothon` | character |  |
| `switcheson` | character |  |
| `playerpoints` | character |  |
| `teampoints` | character |  |
| `matchupassists` | character |  |
| `matchuppotentialassists` | character |  |
| `matchupturnovers` | character |  |
| `matchupblocks` | character |  |
| `matchupfieldgoalsmade` | character |  |
| `matchupfieldgoalsattempted` | character |  |
| `matchupfieldgoalspercentage` | numeric |  |
| `matchupthreepointersmade` | character |  |
| `matchupthreepointersattempted` | character |  |
| `matchupthreepointerspercentage` | numeric |  |
| `helpblocks` | character |  |
| `helpfieldgoalsmade` | character |  |
| `helpfieldgoalsattempted` | character |  |
| `helpfieldgoalspercentage` | numeric |  |
| `matchupfreethrowsmade` | character |  |
| `matchupfreethrowsattempted` | character |  |
| `shootingfouls` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscorematchupsv3()
```

_Last validated n/a._

## `nba_stats_boxscoremiscv2`

GET /stats/boxscoremiscv2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoremiscv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoremiscv2](https://stats.nba.com/stats/boxscoremiscv2)

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
| `start_position` | character |  |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `pts_off_tov` | character |  |
| `pts_2nd_chance` | character |  |
| `pts_fb` | character |  |
| `pts_paint` | character |  |
| `opp_pts_off_tov` | character |  |
| `opp_pts_2nd_chance` | character |  |
| `opp_pts_fb` | character |  |
| `opp_pts_paint` | character |  |
| `blk` | character | Blocks. |
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoremiscv2()
```

_Last validated n/a._

## `nba_stats_boxscoremiscv3`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personid` | character |  |
| `firstname` | character | Firstname. |
| `familyname` | character |  |
| `namei` | character |  |
| `playerslug` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character |  |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `pointsoffturnovers` | character |  |
| `pointssecondchance` | character |  |
| `pointsfastbreak` | character |  |
| `pointspaint` | character |  |
| `opppointsoffturnovers` | character |  |
| `opppointssecondchance` | character |  |
| `opppointsfastbreak` | character |  |
| `opppointspaint` | character |  |
| `blocks` | character | Total blocks. |
| `blocksagainst` | character |  |
| `foulspersonal` | character |  |
| `foulsdrawn` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoremiscv3()
```

_Last validated n/a._

## `nba_stats_boxscoreplayertrackv2`

GET /stats/boxscoreplayertrackv2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreplayertrackv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoreplayertrackv2](https://stats.nba.com/stats/boxscoreplayertrackv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoreplayertrackv2()
```

_Last validated n/a._

## `nba_stats_boxscoreplayertrackv3`

GET /stats/boxscoreplayertrackv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreplayertrackv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreplayertrackv3](https://stats.nba.com/stats/boxscoreplayertrackv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personid` | character |  |
| `firstname` | character | Firstname. |
| `familyname` | character |  |
| `namei` | character |  |
| `playerslug` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character |  |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `speed` | character | Speed. |
| `distance` | character | Distance value (in feet for shot data; otherwise context-dependent). |
| `reboundchancesoffensive` | character |  |
| `reboundchancesdefensive` | character |  |
| `reboundchancestotal` | character |  |
| `touches` | character | Touches. |
| `secondaryassists` | character |  |
| `freethrowassists` | character |  |
| `passes` | character | Passes. |
| `assists` | character | Total assists. |
| `contestedfieldgoalsmade` | character |  |
| `contestedfieldgoalsattempted` | character |  |
| `contestedfieldgoalpercentage` | numeric |  |
| `uncontestedfieldgoalsmade` | character |  |
| `uncontestedfieldgoalsattempted` | character |  |
| `uncontestedfieldgoalspercentage` | numeric |  |
| `fieldgoalpercentage` | numeric |  |
| `defendedatrimfieldgoalsmade` | character |  |
| `defendedatrimfieldgoalsattempted` | character |  |
| `defendedatrimfieldgoalpercentage` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoreplayertrackv3()
```

_Last validated n/a._

## `nba_stats_boxscorescoringv2`

GET /stats/boxscorescoringv2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscorescoringv2`

**Valid URL:** [https://stats.nba.com/stats/boxscorescoringv2](https://stats.nba.com/stats/boxscorescoringv2)

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
| `start_position` | character |  |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `pct_fga_2pt` | numeric |  |
| `pct_fga_3pt` | numeric |  |
| `pct_pts_2pt` | numeric |  |
| `pct_pts_2pt_mr` | numeric |  |
| `pct_pts_3pt` | numeric |  |
| `pct_pts_fb` | numeric |  |
| `pct_pts_ft` | numeric |  |
| `pct_pts_off_tov` | numeric |  |
| `pct_pts_paint` | numeric |  |
| `pct_ast_2pm` | numeric |  |
| `pct_uast_2pm` | numeric |  |
| `pct_ast_3pm` | numeric |  |
| `pct_uast_3pm` | numeric |  |
| `pct_ast_fgm` | numeric |  |
| `pct_uast_fgm` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscorescoringv2()
```

_Last validated n/a._

## `nba_stats_boxscorescoringv3`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personid` | character |  |
| `firstname` | character | Firstname. |
| `familyname` | character |  |
| `namei` | character |  |
| `playerslug` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character |  |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `percentagefieldgoalsattempted2pt` | character |  |
| `percentagefieldgoalsattempted3pt` | character |  |
| `percentagepoints2pt` | character |  |
| `percentagepointsmidrange2pt` | character |  |
| `percentagepoints3pt` | character |  |
| `percentagepointsfastbreak` | character |  |
| `percentagepointsfreethrow` | character |  |
| `percentagepointsoffturnovers` | character |  |
| `percentagepointspaint` | character |  |
| `percentageassisted2pt` | character |  |
| `percentageunassisted2pt` | character |  |
| `percentageassisted3pt` | character |  |
| `percentageunassisted3pt` | character |  |
| `percentageassistedfgm` | character |  |
| `percentageunassistedfgm` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscorescoringv3()
```

_Last validated n/a._

## `nba_stats_boxscoresimilarityscore`

GET /stats/boxscoresimilarityscore

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoresimilarityscore`

**Valid URL:** [https://stats.nba.com/stats/boxscoresimilarityscore](https://stats.nba.com/stats/boxscoresimilarityscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoresimilarityscore()
```

_Last validated n/a._

## `nba_stats_boxscoresummaryv2`

GET /stats/boxscoresummaryv2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoresummaryv2`

**Valid URL:** [https://stats.nba.com/stats/boxscoresummaryv2](https://stats.nba.com/stats/boxscoresummaryv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `video_available_flag` | character | Video available flag. |
| `pt_available` | character | Pt available. |
| `pt_xyz_available` | character | Pt xyz available. |
| `wh_status` | character | Wh status. |
| `hustle_status` | character | Hustle status. |
| `historical_status` | character | Historical status. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoresummaryv2()
```

_Last validated n/a._

## `nba_stats_boxscoresummaryv3`

GET /stats/boxscoresummaryv3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoresummaryv3`

**Valid URL:** [https://stats.nba.com/stats/boxscoresummaryv3](https://stats.nba.com/stats/boxscoresummaryv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `gameid` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `arenaid` | character |  |
| `arenaname` | character |  |
| `arenacity` | character |  |
| `arenastate` | character |  |
| `arenacountry` | character |  |
| `arenatimezone` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoresummaryv3()
```

_Last validated n/a._

## `nba_stats_boxscoretraditionalv2`

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
| `start_position` | character |  |
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
| `to` | character | To. |
| `pf` | character | Personal fouls. |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoretraditionalv2()
```

_Last validated n/a._

## `nba_stats_boxscoretraditionalv3`

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

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personid` | character |  |
| `firstname` | character | Firstname. |
| `familyname` | character |  |
| `namei` | character |  |
| `playerslug` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character |  |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `fieldgoalsmade` | character |  |
| `fieldgoalsattempted` | character |  |
| `fieldgoalspercentage` | numeric |  |
| `threepointersmade` | character |  |
| `threepointersattempted` | character |  |
| `threepointerspercentage` | numeric |  |
| `freethrowsmade` | character |  |
| `freethrowsattempted` | character |  |
| `freethrowspercentage` | numeric |  |
| `reboundsoffensive` | character |  |
| `reboundsdefensive` | character |  |
| `reboundstotal` | character |  |
| `assists` | character | Total assists. |
| `steals` | character | Total steals. |
| `blocks` | character | Total blocks. |
| `turnovers` | character | Total turnovers. |
| `foulspersonal` | character |  |
| `points` | character | Points scored. |
| `plusminuspoints` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoretraditionalv3()
```

_Last validated n/a._

## `nba_stats_boxscoreusagev2`

GET /stats/boxscoreusagev2

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreusagev2`

**Valid URL:** [https://stats.nba.com/stats/boxscoreusagev2](https://stats.nba.com/stats/boxscoreusagev2)

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
| `start_position` | character |  |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `min` | integer | Minutes played. |
| `usg_pct` | numeric |  |
| `pct_fgm` | numeric |  |
| `pct_fga` | numeric |  |
| `pct_fg3m` | numeric |  |
| `pct_fg3a` | numeric |  |
| `pct_ftm` | numeric |  |
| `pct_fta` | numeric |  |
| `pct_oreb` | numeric |  |
| `pct_dreb` | numeric |  |
| `pct_reb` | numeric |  |
| `pct_ast` | numeric |  |
| `pct_tov` | numeric |  |
| `pct_stl` | numeric |  |
| `pct_blk` | numeric |  |
| `pct_blka` | numeric |  |
| `pct_pf` | numeric |  |
| `pct_pfd` | numeric |  |
| `pct_pts` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoreusagev2()
```

_Last validated n/a._

## `nba_stats_boxscoreusagev3`

GET /stats/boxscoreusagev3

**Endpoint URL:** `GET https://stats.nba.com/stats/boxscoreusagev3`

**Valid URL:** [https://stats.nba.com/stats/boxscoreusagev3](https://stats.nba.com/stats/boxscoreusagev3)

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
| `gameid` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamtricode` | character |  |
| `teamslug` | character |  |
| `personid` | character |  |
| `firstname` | character | Firstname. |
| `familyname` | character |  |
| `namei` | character |  |
| `playerslug` | character |  |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jerseynum` | character |  |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `usagepercentage` | numeric |  |
| `percentagefieldgoalsmade` | character |  |
| `percentagefieldgoalsattempted` | character |  |
| `percentagethreepointersmade` | character |  |
| `percentagethreepointersattempted` | character |  |
| `percentagefreethrowsmade` | character |  |
| `percentagefreethrowsattempted` | character |  |
| `percentagereboundsoffensive` | character |  |
| `percentagereboundsdefensive` | character |  |
| `percentagereboundstotal` | character |  |
| `percentageassists` | character |  |
| `percentageturnovers` | character |  |
| `percentagesteals` | character |  |
| `percentageblocks` | character |  |
| `percentageblocksallowed` | character |  |
| `percentagepersonalfouls` | character |  |
| `percentagepersonalfoulsdrawn` | character |  |
| `percentagepoints` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_boxscoreusagev3()
```

_Last validated n/a._

## `nba_stats_commonallplayers`

GET /stats/commonallplayers

**Endpoint URL:** `GET https://stats.nba.com/stats/commonallplayers`

**Valid URL:** [https://stats.nba.com/stats/commonallplayers?LeagueID=00](https://stats.nba.com/stats/commonallplayers?LeagueID=00)

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
| `display_last_comma_first` | character |  |
| `display_first_last` | character |  |
| `rosterstatus` | character |  |
| `from_year` | character |  |
| `to_year` | character |  |
| `playercode` | character |  |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_code` | character | Internal team code. |
| `games_played_flag` | character |  |
| `otherleague_experience_ch` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_commonallplayers(league_id='00')
```

_Last validated n/a._

## `nba_stats_commonplayerinfo`

GET /stats/commonplayerinfo

**Endpoint URL:** `GET https://stats.nba.com/stats/commonplayerinfo`

**Valid URL:** [https://stats.nba.com/stats/commonplayerinfo?LeagueID=00](https://stats.nba.com/stats/commonplayerinfo?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_commonplayerinfo(league_id='00')
```

_Last validated n/a._

## `nba_stats_commonplayoffseries`

GET /stats/commonplayoffseries

**Endpoint URL:** `GET https://stats.nba.com/stats/commonplayoffseries`

**Valid URL:** [https://stats.nba.com/stats/commonplayoffseries?LeagueID=00](https://stats.nba.com/stats/commonplayoffseries?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeriesID` | `series_id_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `series_id` | integer | Series identifier (e.g. 'W_1'). |
| `game_num` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_commonplayoffseries(league_id='00')
```

_Last validated n/a._

## `nba_stats_commonteamroster`

GET /stats/commonteamroster

**Endpoint URL:** `GET https://stats.nba.com/stats/commonteamroster`

**Valid URL:** [https://stats.nba.com/stats/commonteamroster?LeagueID=00](https://stats.nba.com/stats/commonteamroster?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `season` | character | Season year. |
| `coach_id` | integer | ESPN coach id. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `coach_name` | character |  |
| `is_assistant` | character |  |
| `coach_type` | character |  |
| `sort_sequence` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_commonteamroster(league_id='00')
```

_Last validated n/a._

## `nba_stats_commonteamyears`

GET /stats/commonteamyears

**Endpoint URL:** `GET https://stats.nba.com/stats/commonteamyears`

**Valid URL:** [https://stats.nba.com/stats/commonteamyears?LeagueID=00](https://stats.nba.com/stats/commonteamyears?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `min_year` | character |  |
| `max_year` | character |  |
| `abbreviation` | character | Short abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_commonteamyears(league_id='00')
```

_Last validated n/a._

## `nba_stats_cumestatsplayer`

GET /stats/cumestatsplayer

**Endpoint URL:** `GET https://stats.nba.com/stats/cumestatsplayer`

**Valid URL:** [https://stats.nba.com/stats/cumestatsplayer?LeagueID=00](https://stats.nba.com/stats/cumestatsplayer?LeagueID=00)

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
| `date_est` | character |  |
| `visitor_team` | character |  |
| `home_team` | character | Home team's team. |
| `gp` | integer | Games played. |
| `gs` | integer |  |
| `actual_minutes` | character |  |
| `actual_seconds` | character |  |
| `fg` | character |  |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3` | character |  |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft` | character |  |
| `fta` | character | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `off_reb` | character |  |
| `def_reb` | character |  |
| `tot_reb` | character |  |
| `avg_tot_reb` | character |  |
| `ast` | character | Assists. |
| `pf` | character | Personal fouls. |
| `dq` | character |  |
| `stl` | character | Steals. |
| `turnovers` | character | Total turnovers. |
| `blk` | character | Blocks. |
| `pts` | character | Points scored. |
| `avg_pts` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_cumestatsplayer(league_id='00')
```

_Last validated n/a._

## `nba_stats_cumestatsplayergames`

GET /stats/cumestatsplayergames

**Endpoint URL:** `GET https://stats.nba.com/stats/cumestatsplayergames`

**Valid URL:** [https://stats.nba.com/stats/cumestatsplayergames?LeagueID=00](https://stats.nba.com/stats/cumestatsplayergames?LeagueID=00)

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
| `game_id` | integer | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_cumestatsplayergames(league_id='00')
```

_Last validated n/a._

## `nba_stats_cumestatsteam`

GET /stats/cumestatsteam

**Endpoint URL:** `GET https://stats.nba.com/stats/cumestatsteam`

**Valid URL:** [https://stats.nba.com/stats/cumestatsteam?LeagueID=00](https://stats.nba.com/stats/cumestatsteam?LeagueID=00)

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
| `player` | character | Player. |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer |  |
| `actual_minutes` | character |  |
| `actual_seconds` | character |  |
| `fg` | character |  |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3` | character |  |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft` | character |  |
| `fta` | character | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `off_reb` | character |  |
| `def_reb` | character |  |
| `tot_reb` | character |  |
| `ast` | character | Assists. |
| `pf` | character | Personal fouls. |
| `dq` | character |  |
| `stl` | character | Steals. |
| `turnovers` | character | Total turnovers. |
| `blk` | character | Blocks. |
| `pts` | character | Points scored. |
| `max_actual_minutes` | character |  |
| `max_actual_seconds` | character |  |
| `max_reb` | character |  |
| `max_ast` | character |  |
| `max_stl` | character |  |
| `max_turnovers` | character |  |
| `max_blkp` | character |  |
| `max_pts` | character |  |
| `avg_actual_minutes` | character |  |
| `avg_actual_seconds` | character |  |
| `avg_reb` | character |  |
| `avg_ast` | character |  |
| `avg_stl` | character |  |
| `avg_turnovers` | character |  |
| `avg_blkp` | character |  |
| `avg_pts` | character |  |
| `per_min_reb` | character |  |
| `per_min_ast` | character |  |
| `per_min_stl` | character |  |
| `per_min_turnovers` | character |  |
| `per_min_blk` | character |  |
| `per_min_pts` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_cumestatsteam(league_id='00')
```

_Last validated n/a._

## `nba_stats_cumestatsteamgames`

GET /stats/cumestatsteamgames

**Endpoint URL:** `GET https://stats.nba.com/stats/cumestatsteamgames`

**Valid URL:** [https://stats.nba.com/stats/cumestatsteamgames?LeagueID=00](https://stats.nba.com/stats/cumestatsteamgames?LeagueID=00)

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
| `game_id` | integer | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_cumestatsteamgames(league_id='00')
```

_Last validated n/a._

## `nba_stats_defensehub`

GET /stats/defensehub

**Endpoint URL:** `GET https://stats.nba.com/stats/defensehub`

**Valid URL:** [https://stats.nba.com/stats/defensehub?LeagueID=00](https://stats.nba.com/stats/defensehub?LeagueID=00)

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
| `rank` | character | Rank. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `dreb` | character | Defensive rebounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_defensehub(league_id='00')
```

_Last validated n/a._

## `nba_stats_draftboard`

GET /stats/draftboard

**Endpoint URL:** `GET https://stats.nba.com/stats/draftboard`

**Valid URL:** [https://stats.nba.com/stats/draftboard?LeagueID=00](https://stats.nba.com/stats/draftboard?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `OverallPick` | `overall_pick_nullable` |  |  | `Y` |  |
| `RoundNum` | `round_num_nullable` |  |  | `Y` |  |
| `RoundPick` | `round_pick_nullable` |  |  | `Y` |  |
| `Season` | `season_year` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TopX` | `topx_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_name` | character | Player name. |
| `season` | character | Season year. |
| `round_number` | character | Draft round number. |
| `round_pick` | character | Round pick. |
| `overall_pick` | character | Overall pick number in the draft. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `organization` | character | Organization. |
| `organization_type` | character | Organization type. |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `birthdate` | character | Date of birth. |
| `age` | numeric | Player age (in years). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_draftboard(league_id='00')
```

_Last validated n/a._

## `nba_stats_draftcombinedrillresults`

GET /stats/draftcombinedrillresults

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombinedrillresults`

**Valid URL:** [https://stats.nba.com/stats/draftcombinedrillresults?LeagueID=00](https://stats.nba.com/stats/draftcombinedrillresults?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer |  |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `standing_vertical_leap` | character |  |
| `max_vertical_leap` | character |  |
| `lane_agility_time` | character |  |
| `modified_lane_agility_time` | character |  |
| `three_quarter_sprint` | character |  |
| `bench_press` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_draftcombinedrillresults(league_id='00')
```

_Last validated n/a._

## `nba_stats_draftcombinenonstationaryshooting`

GET /stats/draftcombinenonstationaryshooting

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombinenonstationaryshooting`

**Valid URL:** [https://stats.nba.com/stats/draftcombinenonstationaryshooting?LeagueID=00](https://stats.nba.com/stats/draftcombinenonstationaryshooting?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer |  |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `off_drib_fifteen_break_left_made` | character |  |
| `off_drib_fifteen_break_left_attempt` | character |  |
| `off_drib_fifteen_break_left_pct` | numeric |  |
| `off_drib_fifteen_top_key_made` | character |  |
| `off_drib_fifteen_top_key_attempt` | character |  |
| `off_drib_fifteen_top_key_pct` | numeric |  |
| `off_drib_fifteen_break_right_made` | character |  |
| `off_drib_fifteen_break_right_attempt` | character |  |
| `off_drib_fifteen_break_right_pct` | numeric |  |
| `off_drib_college_break_left_made` | character |  |
| `off_drib_college_break_left_attempt` | character |  |
| `off_drib_college_break_left_pct` | numeric |  |
| `off_drib_college_top_key_made` | character |  |
| `off_drib_college_top_key_attempt` | character |  |
| `off_drib_college_top_key_pct` | numeric |  |
| `off_drib_college_break_right_made` | character |  |
| `off_drib_college_break_right_attempt` | character |  |
| `off_drib_college_break_right_pct` | numeric |  |
| `on_move_fifteen_made` | character |  |
| `on_move_fifteen_attempt` | character |  |
| `on_move_fifteen_pct` | numeric |  |
| `on_move_college_made` | character |  |
| `on_move_college_attempt` | character |  |
| `on_move_college_pct` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_draftcombinenonstationaryshooting(league_id='00')
```

_Last validated n/a._

## `nba_stats_draftcombineplayeranthro`

GET /stats/draftcombineplayeranthro

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombineplayeranthro`

**Valid URL:** [https://stats.nba.com/stats/draftcombineplayeranthro?LeagueID=00](https://stats.nba.com/stats/draftcombineplayeranthro?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer |  |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height_wo_shoes` | character |  |
| `height_wo_shoes_ft_in` | character |  |
| `height_w_shoes` | character |  |
| `height_w_shoes_ft_in` | character |  |
| `weight` | character | Player weight in pounds. |
| `wingspan` | character |  |
| `wingspan_ft_in` | character |  |
| `standing_reach` | character |  |
| `standing_reach_ft_in` | character |  |
| `body_fat_pct` | numeric |  |
| `hand_length` | character |  |
| `hand_width` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_draftcombineplayeranthro(league_id='00')
```

_Last validated n/a._

## `nba_stats_draftcombinespotshooting`

GET /stats/draftcombinespotshooting

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombinespotshooting`

**Valid URL:** [https://stats.nba.com/stats/draftcombinespotshooting?LeagueID=00](https://stats.nba.com/stats/draftcombinespotshooting?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_year` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `temp_player_id` | integer |  |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `fifteen_corner_left_made` | character |  |
| `fifteen_corner_left_attempt` | character |  |
| `fifteen_corner_left_pct` | numeric |  |
| `fifteen_break_left_made` | character |  |
| `fifteen_break_left_attempt` | character |  |
| `fifteen_break_left_pct` | numeric |  |
| `fifteen_top_key_made` | character |  |
| `fifteen_top_key_attempt` | character |  |
| `fifteen_top_key_pct` | numeric |  |
| `fifteen_break_right_made` | character |  |
| `fifteen_break_right_attempt` | character |  |
| `fifteen_break_right_pct` | numeric |  |
| `fifteen_corner_right_made` | character |  |
| `fifteen_corner_right_attempt` | character |  |
| `fifteen_corner_right_pct` | numeric |  |
| `college_corner_left_made` | character |  |
| `college_corner_left_attempt` | character |  |
| `college_corner_left_pct` | numeric |  |
| `college_break_left_made` | character |  |
| `college_break_left_attempt` | character |  |
| `college_break_left_pct` | numeric |  |
| `college_top_key_made` | character |  |
| `college_top_key_attempt` | character |  |
| `college_top_key_pct` | numeric |  |
| `college_break_right_made` | character |  |
| `college_break_right_attempt` | character |  |
| `college_break_right_pct` | numeric |  |
| `college_corner_right_made` | character |  |
| `college_corner_right_attempt` | character |  |
| `college_corner_right_pct` | numeric |  |
| `nba_corner_left_made` | character |  |
| `nba_corner_left_attempt` | character |  |
| `nba_corner_left_pct` | numeric |  |
| `nba_break_left_made` | character |  |
| `nba_break_left_attempt` | character |  |
| `nba_break_left_pct` | numeric |  |
| `nba_top_key_made` | character |  |
| `nba_top_key_attempt` | character |  |
| `nba_top_key_pct` | numeric |  |
| `nba_break_right_made` | character |  |
| `nba_break_right_attempt` | character |  |
| `nba_break_right_pct` | numeric |  |
| `nba_corner_right_made` | character |  |
| `nba_corner_right_attempt` | character |  |
| `nba_corner_right_pct` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_draftcombinespotshooting(league_id='00')
```

_Last validated n/a._

## `nba_stats_draftcombinestats`

GET /stats/draftcombinestats

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombinestats`

**Valid URL:** [https://stats.nba.com/stats/draftcombinestats?LeagueID=00](https://stats.nba.com/stats/draftcombinestats?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_all_time` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season` | character | Season year. |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height_wo_shoes` | character |  |
| `height_wo_shoes_ft_in` | character |  |
| `height_w_shoes` | character |  |
| `height_w_shoes_ft_in` | character |  |
| `weight` | character | Player weight in pounds. |
| `wingspan` | character |  |
| `wingspan_ft_in` | character |  |
| `standing_reach` | character |  |
| `standing_reach_ft_in` | character |  |
| `body_fat_pct` | numeric |  |
| `hand_length` | character |  |
| `hand_width` | character |  |
| `standing_vertical_leap` | character |  |
| `max_vertical_leap` | character |  |
| `lane_agility_time` | character |  |
| `modified_lane_agility_time` | character |  |
| `three_quarter_sprint` | character |  |
| `bench_press` | character |  |
| `spot_fifteen_corner_left` | character |  |
| `spot_fifteen_break_left` | character |  |
| `spot_fifteen_top_key` | character |  |
| `spot_fifteen_break_right` | character |  |
| `spot_fifteen_corner_right` | character |  |
| `spot_college_corner_left` | character |  |
| `spot_college_break_left` | character |  |
| `spot_college_top_key` | character |  |
| `spot_college_break_right` | character |  |
| `spot_college_corner_right` | character |  |
| `spot_nba_corner_left` | character |  |
| `spot_nba_break_left` | character |  |
| `spot_nba_top_key` | character |  |
| `spot_nba_break_right` | character |  |
| `spot_nba_corner_right` | character |  |
| `off_drib_fifteen_break_left` | character |  |
| `off_drib_fifteen_top_key` | character |  |
| `off_drib_fifteen_break_right` | character |  |
| `off_drib_college_break_left` | character |  |
| `off_drib_college_top_key` | character |  |
| `off_drib_college_break_right` | character |  |
| `on_move_fifteen` | character |  |
| `on_move_college` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_draftcombinestats(league_id='00')
```

_Last validated n/a._

## `nba_stats_dunkscoreleaders`

GET /stats/dunkscoreleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/dunkscoreleaders`

**Valid URL:** [https://stats.nba.com/stats/dunkscoreleaders](https://stats.nba.com/stats/dunkscoreleaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_dunkscoreleaders()
```

_Last validated n/a._

## `nba_stats_fantasywidget`

GET /stats/fantasywidget

**Endpoint URL:** `GET https://stats.nba.com/stats/fantasywidget`

**Valid URL:** [https://stats.nba.com/stats/fantasywidget?LeagueID=00](https://stats.nba.com/stats/fantasywidget?LeagueID=00)

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
| `fan_duel_pts` | character |  |
| `nba_fantasy_pts` | character |  |
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
nba_stats_fantasywidget(league_id='00')
```

_Last validated n/a._

## `nba_stats_franchisehistory`

GET /stats/franchisehistory

**Endpoint URL:** `GET https://stats.nba.com/stats/franchisehistory`

**Valid URL:** [https://stats.nba.com/stats/franchisehistory?LeagueID=00](https://stats.nba.com/stats/franchisehistory?LeagueID=00)

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
| `games` | integer | Number of games included in the ATS summary. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `win_pct` | numeric | Win percentage (0-1 decimal). |
| `po_appearances` | integer |  |
| `div_titles` | integer |  |
| `conf_titles` | integer |  |
| `league_titles` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_franchisehistory(league_id='00')
```

_Last validated n/a._

## `nba_stats_franchiseleaders`

GET /stats/franchiseleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/franchiseleaders`

**Valid URL:** [https://stats.nba.com/stats/franchiseleaders?LeagueID=00](https://stats.nba.com/stats/franchiseleaders?LeagueID=00)

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
| `pts_person_id` | integer |  |
| `pts_player` | character |  |
| `ast` | integer | Assists. |
| `ast_person_id` | integer |  |
| `ast_player` | character |  |
| `reb` | integer | Total rebounds. |
| `reb_person_id` | integer |  |
| `reb_player` | character |  |
| `blk` | integer | Blocks. |
| `blk_person_id` | integer |  |
| `blk_player` | character |  |
| `stl` | integer | Steals. |
| `stl_person_id` | integer |  |
| `stl_player` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_franchiseleaders(league_id='00')
```

_Last validated n/a._

## `nba_stats_franchiseleaderswrank`

GET /stats/franchiseleaderswrank

**Endpoint URL:** `GET https://stats.nba.com/stats/franchiseleaderswrank`

**Valid URL:** [https://stats.nba.com/stats/franchiseleaderswrank](https://stats.nba.com/stats/franchiseleaderswrank)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_franchiseleaderswrank()
```

_Last validated n/a._

## `nba_stats_franchiseplayers`

GET /stats/franchiseplayers

**Endpoint URL:** `GET https://stats.nba.com/stats/franchiseplayers`

**Valid URL:** [https://stats.nba.com/stats/franchiseplayers?LeagueID=00](https://stats.nba.com/stats/franchiseplayers?LeagueID=00)

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
| `league_id` | integer | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player` | character | Player. |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `active_with_team` | character |  |
| `gp` | integer | Games played. |
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
| `pf` | character | Personal fouls. |
| `stl` | character | Steals. |
| `tov` | character | Turnovers. |
| `blk` | character | Blocks. |
| `pts` | character | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_franchiseplayers(league_id='00')
```

_Last validated n/a._

## `nba_stats_gamerotation`

GET /stats/gamerotation

**Endpoint URL:** `GET https://stats.nba.com/stats/gamerotation`

**Valid URL:** [https://stats.nba.com/stats/gamerotation?LeagueID=00](https://stats.nba.com/stats/gamerotation?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_first` | character |  |
| `player_last` | character |  |
| `in_time_real` | character |  |
| `out_time_real` | character |  |
| `player_pts` | character |  |
| `pt_diff` | character |  |
| `usg_pct` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_gamerotation(league_id='00')
```

_Last validated n/a._

## `nba_stats_glalumboxscoresimilarityscore`

GET /stats/glalumboxscoresimilarityscore

**Endpoint URL:** `GET https://stats.nba.com/stats/glalumboxscoresimilarityscore`

**Valid URL:** [https://stats.nba.com/stats/glalumboxscoresimilarityscore](https://stats.nba.com/stats/glalumboxscoresimilarityscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Person1Id` | `person1_id` |  |  | `Y` |  |
| `Person1LeagueId` | `person1_league_id` |  |  | `Y` |  |
| `Person1Season` | `person1_season_year` |  |  | `Y` |  |
| `Person1SeasonType` | `person1_season_type` |  |  | `Y` |  |
| `Person2Id` | `person2_id` |  |  | `Y` |  |
| `Person2LeagueId` | `person2_league_id` |  |  | `Y` |  |
| `Person2Season` | `person2_season_year` |  |  | `Y` |  |
| `Person2SeasonType` | `person2_season_type` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_2_id` | integer |  |
| `person_2` | character |  |
| `team_id` | integer | Unique team identifier. |
| `similarity_score` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_glalumboxscoresimilarityscore()
```

_Last validated n/a._

## `nba_stats_gravityleaders`

GET /stats/gravityleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/gravityleaders`

**Valid URL:** [https://stats.nba.com/stats/gravityleaders](https://stats.nba.com/stats/gravityleaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_gravityleaders()
```

_Last validated n/a._

## `nba_stats_homepageleaders`

GET /stats/homepageleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/homepageleaders`

**Valid URL:** [https://stats.nba.com/stats/homepageleaders?LeagueID=00](https://stats.nba.com/stats/homepageleaders?LeagueID=00)

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
| `rank` | character | Rank. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `pts` | character | Points scored. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `efg_pct` | numeric |  |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `pts_per48` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_homepageleaders(league_id='00')
```

_Last validated n/a._

## `nba_stats_homepagev2`

GET /stats/homepagev2

**Endpoint URL:** `GET https://stats.nba.com/stats/homepagev2`

**Valid URL:** [https://stats.nba.com/stats/homepagev2?LeagueID=00](https://stats.nba.com/stats/homepagev2?LeagueID=00)

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
| `rank` | character | Rank. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pts` | character | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_homepagev2(league_id='00')
```

_Last validated n/a._

## `nba_stats_hustlestatsboxscore`

GET /stats/hustlestatsboxscore

**Endpoint URL:** `GET https://stats.nba.com/stats/hustlestatsboxscore`

**Valid URL:** [https://stats.nba.com/stats/hustlestatsboxscore](https://stats.nba.com/stats/hustlestatsboxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `hustle_status` | character | Hustle status. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_hustlestatsboxscore()
```

_Last validated n/a._

## `nba_stats_infographicfanduelplayer`

GET /stats/infographicfanduelplayer

**Endpoint URL:** `GET https://stats.nba.com/stats/infographicfanduelplayer`

**Valid URL:** [https://stats.nba.com/stats/infographicfanduelplayer](https://stats.nba.com/stats/infographicfanduelplayer)

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
| `location` | character | Location. |
| `fan_duel_pts` | character |  |
| `nba_fantasy_pts` | character |  |
| `usg_pct` | numeric |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_infographicfanduelplayer()
```

_Last validated n/a._

## `nba_stats_iststandings`

GET /stats/iststandings

**Endpoint URL:** `GET https://stats.nba.com/stats/iststandings`

**Valid URL:** [https://stats.nba.com/stats/iststandings?LeagueID=00](https://stats.nba.com/stats/iststandings?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `Section` | `section` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `leagueid` | character |  |
| `seasonyear` | character |  |
| `teamid` | character | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamabbreviation` | character | Teamabbreviation. |
| `teamslug` | character |  |
| `conference` | character | Conference. |
| `istgroup` | character |  |
| `clinchindicator` | character |  |
| `clinchedistknockout` | character |  |
| `clinchedistgroup` | character |  |
| `clinchedistwildcard` | character |  |
| `istwildcardrank` | character |  |
| `istgrouprank` | character |  |
| `istknockoutrank` | character |  |
| `wins` | character | Total wins. |
| `losses` | character | Total losses. |
| `pct` | numeric | Pct. |
| `istgroupgb` | character |  |
| `istwildcardgb` | character |  |
| `diff` | character | Diff. |
| `pts` | character | Points scored. |
| `opppts` | character |  |
| `gameid1` | character |  |
| `opponentteamabbreviation1` | character |  |
| `location1` | character |  |
| `gamestatus1` | character |  |
| `gamestatustext1` | character |  |
| `outcome1` | character |  |
| `gameid2` | character |  |
| `opponentteamabbreviation2` | character |  |
| `location2` | character |  |
| `gamestatus2` | character |  |
| `gamestatustext2` | character |  |
| `outcome2` | character |  |
| `gameid3` | character |  |
| `opponentteamabbreviation3` | character |  |
| `location3` | character |  |
| `gamestatus3` | character |  |
| `gamestatustext3` | character |  |
| `outcome3` | character |  |
| `gameid4` | character |  |
| `opponentteamabbreviation4` | character |  |
| `location4` | character |  |
| `gamestatus4` | character |  |
| `gamestatustext4` | character |  |
| `outcome4` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_iststandings(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaderstiles`

GET /stats/leaderstiles

**Endpoint URL:** `GET https://stats.nba.com/stats/leaderstiles`

**Valid URL:** [https://stats.nba.com/stats/leaderstiles?LeagueID=00](https://stats.nba.com/stats/leaderstiles?LeagueID=00)

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
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `pts` | character | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaderstiles(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashlineups`

GET /stats/leaguedashlineups

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashlineups`

**Valid URL:** [https://stats.nba.com/stats/leaguedashlineups?LeagueID=00](https://stats.nba.com/stats/leaguedashlineups?LeagueID=00)

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
| `group_set` | character |  |
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
| `blka` | numeric |  |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric |  |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer |  |
| `w_rank` | integer |  |
| `l_rank` | integer |  |
| `w_pct_rank` | integer |  |
| `min_rank` | integer |  |
| `fgm_rank` | integer |  |
| `fga_rank` | integer |  |
| `fg_pct_rank` | integer |  |
| `fg3m_rank` | integer |  |
| `fg3a_rank` | integer |  |
| `fg3_pct_rank` | integer |  |
| `ftm_rank` | integer |  |
| `fta_rank` | integer |  |
| `ft_pct_rank` | integer |  |
| `oreb_rank` | integer |  |
| `dreb_rank` | integer |  |
| `reb_rank` | integer |  |
| `ast_rank` | integer |  |
| `tov_rank` | integer |  |
| `stl_rank` | integer |  |
| `blk_rank` | integer |  |
| `blka_rank` | integer |  |
| `pf_rank` | integer |  |
| `pfd_rank` | integer |  |
| `pts_rank` | integer |  |
| `plus_minus_rank` | integer |  |
| `sum_time_played` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashlineups(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashoppptshot`

GET /stats/leaguedashoppptshot

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashoppptshot`

**Valid URL:** [https://stats.nba.com/stats/leaguedashoppptshot?LeagueID=00](https://stats.nba.com/stats/leaguedashoppptshot?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `CloseDefDistRange` | `close_def_dist_range_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_nullable` |  |  | `Y` |  |
| `DribbleRange` | `dribble_range_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GeneralRange` | `general_range_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `ShotDistRange` | `shot_dist_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TouchTimeRange` | `touch_time_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `fga_frequency` | numeric |  |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric |  |
| `fg2a_frequency` | numeric |  |
| `fg2m` | numeric |  |
| `fg2a` | numeric |  |
| `fg2_pct` | numeric |  |
| `fg3a_frequency` | numeric |  |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashoppptshot(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashplayerbiostats`

GET /stats/leaguedashplayerbiostats

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashplayerbiostats`

**Valid URL:** [https://stats.nba.com/stats/leaguedashplayerbiostats?LeagueID=00](https://stats.nba.com/stats/leaguedashplayerbiostats?LeagueID=00)

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
| `player_height_inches` | integer |  |
| `player_weight` | character | Participant weight in pounds. |
| `college` | character | Official college (usually the last one attended) |
| `country` | character | Country (full name or code). |
| `draft_year` | character | Draft year (4-digit). |
| `draft_round` | character | Round of the draft selection. |
| `draft_number` | character | The number pick that was used to select a given player. |
| `gp` | integer | Games played. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `net_rating` | numeric | Net rating (off rating - def rating). |
| `oreb_pct` | numeric |  |
| `dreb_pct` | numeric |  |
| `usg_pct` | numeric |  |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `ast_pct` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashplayerbiostats(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashplayerclutch`

GET /stats/leaguedashplayerclutch

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashplayerclutch`

**Valid URL:** [https://stats.nba.com/stats/leaguedashplayerclutch?LeagueID=00](https://stats.nba.com/stats/leaguedashplayerclutch?LeagueID=00)

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
| `group_set` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `dd2` | character |  |
| `td3` | character |  |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `nba_fantasy_pts_rank` | character |  |
| `dd2_rank` | character |  |
| `td3_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashplayerclutch(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashplayerptshot`

GET /stats/leaguedashplayerptshot

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashplayerptshot`

**Valid URL:** [https://stats.nba.com/stats/leaguedashplayerptshot?LeagueID=00](https://stats.nba.com/stats/leaguedashplayerptshot?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `CloseDefDistRange` | `close_def_dist_range_nullable` |  |  | `Y` |  |
| `College` | `college_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `DribbleRange` | `dribble_range_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GeneralRange` | `general_range_nullable` |  |  | `Y` |  |
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
| `PlayerPosition` | `player_position_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `ShotDistRange` | `shot_dist_range_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TouchTimeRange` | `touch_time_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `player_last_team_id` | integer |  |
| `player_last_team_abbreviation` | character |  |
| `age` | numeric | Player age (in years). |
| `gp` | integer | Games played. |
| `g` | character | Games played. |
| `fga_frequency` | character |  |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric |  |
| `fg2a_frequency` | character |  |
| `fg2m` | character |  |
| `fg2a` | character |  |
| `fg2_pct` | numeric |  |
| `fg3a_frequency` | character |  |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashplayerptshot(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashplayerstats`

GET /stats/leaguedashplayerstats

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashplayerstats`

**Valid URL:** [https://stats.nba.com/stats/leaguedashplayerstats?LeagueID=00](https://stats.nba.com/stats/leaguedashplayerstats?LeagueID=00)

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
| `blka` | numeric |  |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric |  |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric |  |
| `dd2` | integer |  |
| `td3` | integer |  |
| `wnba_fantasy_pts` | numeric |  |
| `gp_rank` | integer |  |
| `w_rank` | integer |  |
| `l_rank` | integer |  |
| `w_pct_rank` | integer |  |
| `min_rank` | integer |  |
| `fgm_rank` | integer |  |
| `fga_rank` | integer |  |
| `fg_pct_rank` | integer |  |
| `fg3m_rank` | integer |  |
| `fg3a_rank` | integer |  |
| `fg3_pct_rank` | integer |  |
| `ftm_rank` | integer |  |
| `fta_rank` | integer |  |
| `ft_pct_rank` | integer |  |
| `oreb_rank` | integer |  |
| `dreb_rank` | integer |  |
| `reb_rank` | integer |  |
| `ast_rank` | integer |  |
| `tov_rank` | integer |  |
| `stl_rank` | integer |  |
| `blk_rank` | integer |  |
| `blka_rank` | integer |  |
| `pf_rank` | integer |  |
| `pfd_rank` | integer |  |
| `pts_rank` | integer |  |
| `plus_minus_rank` | integer |  |
| `nba_fantasy_pts_rank` | integer |  |
| `dd2_rank` | integer |  |
| `td3_rank` | integer |  |
| `wnba_fantasy_pts_rank` | integer |  |
| `team_count` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashplayerstats(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashptdefend`

GET /stats/leaguedashptdefend

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashptdefend`

**Valid URL:** [https://stats.nba.com/stats/leaguedashptdefend?LeagueID=00](https://stats.nba.com/stats/leaguedashptdefend?LeagueID=00)

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
| `close_def_person_id` | integer |  |
| `player_name` | character | Player name. |
| `player_last_team_id` | integer |  |
| `player_last_team_abbreviation` | character |  |
| `player_position` | character | Position of the player accordinng to NGS |
| `age` | numeric | Player age (in years). |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `freq` | numeric |  |
| `d_fgm` | numeric |  |
| `d_fga` | numeric |  |
| `d_fg_pct` | numeric |  |
| `normal_fg_pct` | numeric |  |
| `pct_plusminus` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashptdefend(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashptstats`

GET /stats/leaguedashptstats

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashptstats`

**Valid URL:** [https://stats.nba.com/stats/leaguedashptstats?LeagueID=00](https://stats.nba.com/stats/leaguedashptstats?LeagueID=00)

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
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `PtMeasureType` | `pt_measure_type` |  |  | `Y` |  |
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
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `gp` | integer | Games played. |
| `w` | character | Wins. |
| `l` | character | Losses. |
| `min` | integer | Minutes played. |
| `dist_feet` | character |  |
| `dist_miles` | character |  |
| `dist_miles_off` | character |  |
| `dist_miles_def` | character |  |
| `avg_speed` | character |  |
| `avg_speed_off` | character |  |
| `avg_speed_def` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashptstats(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashptteamdefend`

GET /stats/leaguedashptteamdefend

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashptteamdefend`

**Valid URL:** [https://stats.nba.com/stats/leaguedashptteamdefend?LeagueID=00](https://stats.nba.com/stats/leaguedashptteamdefend?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `DefenseCategory` | `defense_category` |  |  | `Y` |  |
| `Division` | `division_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `freq` | numeric |  |
| `d_fgm` | numeric |  |
| `d_fga` | numeric |  |
| `d_fg_pct` | numeric |  |
| `normal_fg_pct` | numeric |  |
| `pct_plusminus` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashptteamdefend(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashteamclutch`

GET /stats/leaguedashteamclutch

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashteamclutch`

**Valid URL:** [https://stats.nba.com/stats/leaguedashteamclutch?LeagueID=00](https://stats.nba.com/stats/leaguedashteamclutch?LeagueID=00)

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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashteamclutch(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashteamptshot`

GET /stats/leaguedashteamptshot

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashteamptshot`

**Valid URL:** [https://stats.nba.com/stats/leaguedashteamptshot?LeagueID=00](https://stats.nba.com/stats/leaguedashteamptshot?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `CloseDefDistRange` | `close_def_dist_range_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_nullable` |  |  | `Y` |  |
| `DribbleRange` | `dribble_range_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GeneralRange` | `general_range_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `ShotDistRange` | `shot_dist_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TouchTimeRange` | `touch_time_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `g` | character | Games played. |
| `fga_frequency` | character |  |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric |  |
| `fg2a_frequency` | character |  |
| `fg2m` | character |  |
| `fg2a` | character |  |
| `fg2_pct` | numeric |  |
| `fg3a_frequency` | character |  |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashteamptshot(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguedashteamstats`

GET /stats/leaguedashteamstats

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashteamstats`

**Valid URL:** [https://stats.nba.com/stats/leaguedashteamstats?LeagueID=00](https://stats.nba.com/stats/leaguedashteamstats?LeagueID=00)

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
| `blka` | numeric |  |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric |  |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer |  |
| `w_rank` | integer |  |
| `l_rank` | integer |  |
| `w_pct_rank` | integer |  |
| `min_rank` | integer |  |
| `fgm_rank` | integer |  |
| `fga_rank` | integer |  |
| `fg_pct_rank` | integer |  |
| `fg3m_rank` | integer |  |
| `fg3a_rank` | integer |  |
| `fg3_pct_rank` | integer |  |
| `ftm_rank` | integer |  |
| `fta_rank` | integer |  |
| `ft_pct_rank` | integer |  |
| `oreb_rank` | integer |  |
| `dreb_rank` | integer |  |
| `reb_rank` | integer |  |
| `ast_rank` | integer |  |
| `tov_rank` | integer |  |
| `stl_rank` | integer |  |
| `blk_rank` | integer |  |
| `blka_rank` | integer |  |
| `pf_rank` | integer |  |
| `pfd_rank` | integer |  |
| `pts_rank` | integer |  |
| `plus_minus_rank` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguedashteamstats(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguegamefinder`

GET /stats/leaguegamefinder

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguegamefinder`

**Valid URL:** [https://stats.nba.com/stats/leaguegamefinder?LeagueID=00](https://stats.nba.com/stats/leaguegamefinder?LeagueID=00)

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
nba_stats_leaguegamefinder(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguegamelog`

GET /stats/leaguegamelog

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguegamelog`

**Valid URL:** [https://stats.nba.com/stats/leaguegamelog?LeagueID=00](https://stats.nba.com/stats/leaguegamelog?LeagueID=00)

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
nba_stats_leaguegamelog(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguehustlestatsplayer`

GET /stats/leaguehustlestatsplayer

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguehustlestatsplayer`

**Valid URL:** [https://stats.nba.com/stats/leaguehustlestatsplayer?LeagueID=00](https://stats.nba.com/stats/leaguehustlestatsplayer?LeagueID=00)

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
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_time` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
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
| `g` | integer | Games played. |
| `min` | numeric | Minutes played. |
| `contested_shots` | numeric | Defensively contested shots. |
| `contested_shots_2pt` | numeric |  |
| `contested_shots_3pt` | numeric |  |
| `deflections` | numeric | Defensive deflections. |
| `charges_drawn` | numeric | Charges drawn. |
| `screen_assists` | numeric | Screen assists (resulting in a basket). |
| `screen_ast_pts` | numeric |  |
| `off_loose_balls_recovered` | numeric |  |
| `def_loose_balls_recovered` | numeric |  |
| `loose_balls_recovered` | numeric |  |
| `pct_loose_balls_recovered_off` | numeric |  |
| `pct_loose_balls_recovered_def` | numeric |  |
| `off_boxouts` | numeric |  |
| `def_boxouts` | numeric |  |
| `box_outs` | numeric | Box-outs executed. |
| `box_out_player_team_rebs` | numeric |  |
| `box_out_player_rebs` | numeric |  |
| `pct_box_outs_off` | numeric |  |
| `pct_box_outs_def` | numeric |  |
| `pct_box_outs_team_reb` | numeric |  |
| `pct_box_outs_reb` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguehustlestatsplayer(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguehustlestatsplayerleaders`

GET /stats/leaguehustlestatsplayerleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguehustlestatsplayerleaders`

**Valid URL:** [https://stats.nba.com/stats/leaguehustlestatsplayerleaders](https://stats.nba.com/stats/leaguehustlestatsplayerleaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguehustlestatsplayerleaders()
```

_Last validated n/a._

## `nba_stats_leaguehustlestatsteam`

GET /stats/leaguehustlestatsteam

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguehustlestatsteam`

**Valid URL:** [https://stats.nba.com/stats/leaguehustlestatsteam?LeagueID=00](https://stats.nba.com/stats/leaguehustlestatsteam?LeagueID=00)

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
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_time` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `min` | numeric | Minutes played. |
| `contested_shots` | numeric | Defensively contested shots. |
| `contested_shots_2pt` | numeric |  |
| `contested_shots_3pt` | numeric |  |
| `deflections` | numeric | Defensive deflections. |
| `charges_drawn` | numeric | Charges drawn. |
| `screen_assists` | numeric | Screen assists (resulting in a basket). |
| `screen_ast_pts` | numeric |  |
| `off_loose_balls_recovered` | numeric |  |
| `def_loose_balls_recovered` | numeric |  |
| `loose_balls_recovered` | numeric |  |
| `pct_loose_balls_recovered_off` | numeric |  |
| `pct_loose_balls_recovered_def` | numeric |  |
| `off_boxouts` | numeric |  |
| `def_boxouts` | numeric |  |
| `box_outs` | numeric | Box-outs executed. |
| `pct_box_outs_off` | numeric |  |
| `pct_box_outs_def` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguehustlestatsteam(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguehustlestatsteamleaders`

GET /stats/leaguehustlestatsteamleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguehustlestatsteamleaders`

**Valid URL:** [https://stats.nba.com/stats/leaguehustlestatsteamleaders](https://stats.nba.com/stats/leaguehustlestatsteamleaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguehustlestatsteamleaders()
```

_Last validated n/a._

## `nba_stats_leagueleaders`

GET /stats/leagueleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/leagueleaders`

**Valid URL:** [https://stats.nba.com/stats/leagueleaders?LeagueID=00](https://stats.nba.com/stats/leagueleaders?LeagueID=00)

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
| `rank` | character | Rank. |
| `player` | character | Player. |
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
| `ast_tov` | character |  |
| `stl_tov` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leagueleaders(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguelineupviz`

GET /stats/leaguelineupviz

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguelineupviz`

**Valid URL:** [https://stats.nba.com/stats/leaguelineupviz?LeagueID=00](https://stats.nba.com/stats/leaguelineupviz?LeagueID=00)

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
| `off_rating` | character |  |
| `def_rating` | character |  |
| `net_rating` | character | Net rating (off rating - def rating). |
| `pace` | character | Possessions per 48 minutes. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `fta_rate` | character |  |
| `tm_ast_pct` | numeric |  |
| `pct_fga_2pt` | numeric |  |
| `pct_fga_3pt` | numeric |  |
| `pct_pts_2pt_mr` | numeric |  |
| `pct_pts_fb` | numeric |  |
| `pct_pts_ft` | numeric |  |
| `pct_pts_paint` | numeric |  |
| `pct_ast_fgm` | numeric |  |
| `pct_uast_fgm` | numeric |  |
| `opp_fg3_pct` | numeric |  |
| `opp_efg_pct` | numeric |  |
| `opp_fta_rate` | character |  |
| `opp_tov_pct` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguelineupviz(league_id='00')
```

_Last validated n/a._

## `nba_stats_leagueplayerondetails`

GET /stats/leagueplayerondetails

**Endpoint URL:** `GET https://stats.nba.com/stats/leagueplayerondetails`

**Valid URL:** [https://stats.nba.com/stats/leagueplayerondetails?LeagueID=00](https://stats.nba.com/stats/leagueplayerondetails?LeagueID=00)

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
| `group_set` | character |  |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `vs_player_id` | integer |  |
| `vs_player_name` | character |  |
| `court_status` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leagueplayerondetails(league_id='00')
```

_Last validated n/a._

## `nba_stats_leagueseasonmatchups`

GET /stats/leagueseasonmatchups

**Endpoint URL:** `GET https://stats.nba.com/stats/leagueseasonmatchups`

**Valid URL:** [https://stats.nba.com/stats/leagueseasonmatchups?LeagueID=00](https://stats.nba.com/stats/leagueseasonmatchups?LeagueID=00)

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
| `season_id` | integer | Unique season identifier. |
| `off_player_id` | integer |  |
| `off_player_name` | character |  |
| `def_player_id` | integer |  |
| `def_player_name` | character |  |
| `gp` | integer | Games played. |
| `matchup_min` | character |  |
| `partial_poss` | character |  |
| `player_pts` | character |  |
| `team_pts` | character |  |
| `matchup_ast` | character |  |
| `matchup_tov` | character |  |
| `matchup_blk` | character |  |
| `matchup_fgm` | character |  |
| `matchup_fga` | character |  |
| `matchup_fg_pct` | numeric |  |
| `matchup_fg3m` | character |  |
| `matchup_fg3a` | character |  |
| `matchup_fg3_pct` | numeric |  |
| `help_blk` | character |  |
| `help_fgm` | character |  |
| `help_fga` | character |  |
| `help_fg_perc` | character |  |
| `matchup_ftm` | character |  |
| `matchup_fta` | character |  |
| `sfl` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leagueseasonmatchups(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguestandings`

GET /stats/leaguestandings

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguestandings`

**Valid URL:** [https://stats.nba.com/stats/leaguestandings?LeagueID=00](https://stats.nba.com/stats/leaguestandings?LeagueID=00)

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
| `leagueid` | character |  |
| `seasonid` | character |  |
| `teamid` | integer | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `conference` | character | Conference. |
| `conferencerecord` | character |  |
| `playoffrank` | integer |  |
| `clinchindicator` | character |  |
| `division` | character | Team division. |
| `divisionrecord` | character |  |
| `divisionrank` | integer |  |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `winpct` | numeric |  |
| `leaguerank` | integer |  |
| `record` | character | Team win-loss record for the season. |
| `home` | character | Home. |
| `road` | character | Road. |
| `l10` | character | L10. |
| `last10home` | character |  |
| `last10road` | character |  |
| `ot` | character | Ot. |
| `threeptsorless` | character |  |
| `tenptsormore` | character |  |
| `longhomestreak` | integer |  |
| `strlonghomestreak` | character |  |
| `longroadstreak` | integer |  |
| `strlongroadstreak` | character |  |
| `longwinstreak` | integer |  |
| `longlossstreak` | integer |  |
| `currenthomestreak` | integer |  |
| `strcurrenthomestreak` | character |  |
| `currentroadstreak` | integer |  |
| `strcurrentroadstreak` | character |  |
| `currentstreak` | integer |  |
| `strcurrentstreak` | character | Strcurrentstreak. |
| `conferencegamesback` | numeric |  |
| `divisiongamesback` | numeric |  |
| `clinchedconferencetitle` | integer |  |
| `clincheddivisiontitle` | integer |  |
| `clinchedplayoffbirth` | integer |  |
| `eliminatedconference` | integer |  |
| `eliminateddivision` | integer |  |
| `aheadathalf` | character |  |
| `behindathalf` | character |  |
| `tiedathalf` | character |  |
| `aheadatthird` | character |  |
| `behindatthird` | character |  |
| `tiedatthird` | character |  |
| `score100pts` | character |  |
| `oppscore100pts` | character |  |
| `oppover500` | character |  |
| `leadinfgpct` | character |  |
| `leadinreb` | character |  |
| `fewerturnovers` | character |  |
| `pointspg` | numeric |  |
| `opppointspg` | numeric |  |
| `diffpointspg` | numeric |  |
| `vseast` | character |  |
| `vsatlantic` | character |  |
| `vscentral` | character |  |
| `vssoutheast` | character |  |
| `vswest` | character |  |
| `vsnorthwest` | character |  |
| `vspacific` | character |  |
| `vssouthwest` | character |  |
| `jan` | character |  |
| `feb` | character |  |
| `mar` | character |  |
| `apr` | character |  |
| `may` | character |  |
| `jun` | character |  |
| `jul` | character |  |
| `aug` | character |  |
| `sep` | character |  |
| `oct` | character |  |
| `nov` | character |  |
| `dec` | character |  |
| `preas` | character |  |
| `postas` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguestandings(league_id='00')
```

_Last validated n/a._

## `nba_stats_leaguestandingsv3`

GET /stats/leaguestandingsv3

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguestandingsv3`

**Valid URL:** [https://stats.nba.com/stats/leaguestandingsv3?LeagueID=00](https://stats.nba.com/stats/leaguestandingsv3?LeagueID=00)

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
| `leagueid` | character |  |
| `seasonid` | character |  |
| `teamid` | integer | Teamid. |
| `teamcity` | character | Teamcity. |
| `teamname` | character | Teamname. |
| `teamslug` | character |  |
| `conference` | character | Conference. |
| `conferencerecord` | character |  |
| `playoffrank` | integer |  |
| `clinchindicator` | character |  |
| `division` | character | Team division. |
| `divisionrecord` | character |  |
| `divisionrank` | integer |  |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `winpct` | numeric |  |
| `leaguerank` | integer |  |
| `record` | character | Team win-loss record for the season. |
| `home` | character | Home. |
| `road` | character | Road. |
| `l10` | character | L10. |
| `last10home` | character |  |
| `last10road` | character |  |
| `ot` | character | Ot. |
| `threeptsorless` | character |  |
| `tenptsormore` | character |  |
| `longhomestreak` | integer |  |
| `strlonghomestreak` | character |  |
| `longroadstreak` | integer |  |
| `strlongroadstreak` | character |  |
| `longwinstreak` | integer |  |
| `longlossstreak` | integer |  |
| `currenthomestreak` | integer |  |
| `strcurrenthomestreak` | character |  |
| `currentroadstreak` | integer |  |
| `strcurrentroadstreak` | character |  |
| `currentstreak` | integer |  |
| `strcurrentstreak` | character | Strcurrentstreak. |
| `conferencegamesback` | numeric |  |
| `divisiongamesback` | numeric |  |
| `clinchedconferencetitle` | integer |  |
| `clincheddivisiontitle` | integer |  |
| `clinchedplayoffbirth` | integer |  |
| `clinchedplayin` | integer |  |
| `eliminatedconference` | integer |  |
| `eliminateddivision` | integer |  |
| `aheadathalf` | character |  |
| `behindathalf` | character |  |
| `tiedathalf` | character |  |
| `aheadatthird` | character |  |
| `behindatthird` | character |  |
| `tiedatthird` | character |  |
| `score100pts` | character |  |
| `oppscore100pts` | character |  |
| `oppover500` | character |  |
| `leadinfgpct` | character |  |
| `leadinreb` | character |  |
| `fewerturnovers` | character |  |
| `pointspg` | numeric |  |
| `opppointspg` | numeric |  |
| `diffpointspg` | numeric |  |
| `vseast` | character |  |
| `vsatlantic` | character |  |
| `vscentral` | character |  |
| `vssoutheast` | character |  |
| `vswest` | character |  |
| `vsnorthwest` | character |  |
| `vspacific` | character |  |
| `vssouthwest` | character |  |
| `jan` | character |  |
| `feb` | character |  |
| `mar` | character |  |
| `apr` | character |  |
| `may` | character |  |
| `jun` | character |  |
| `jul` | character |  |
| `aug` | character |  |
| `sep` | character |  |
| `oct` | character |  |
| `nov` | character |  |
| `dec` | character |  |
| `score_80_plus` | character |  |
| `opp_score_80_plus` | character |  |
| `score_below_80` | character |  |
| `opp_score_below_80` | character |  |
| `totalpoints` | integer |  |
| `opptotalpoints` | integer |  |
| `difftotalpoints` | integer |  |
| `leaguegamesback` | numeric |  |
| `playoffseeding` | integer |  |
| `clinchedpostseason` | integer |  |
| `neutral` | character | Neutral. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_leaguestandingsv3(league_id='00')
```

_Last validated n/a._

## `nba_stats_matchupsrollup`

GET /stats/matchupsrollup

**Endpoint URL:** `GET https://stats.nba.com/stats/matchupsrollup`

**Valid URL:** [https://stats.nba.com/stats/matchupsrollup?LeagueID=00](https://stats.nba.com/stats/matchupsrollup?LeagueID=00)

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
| `season_id` | integer | Unique season identifier. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `percent_of_time` | character |  |
| `def_player_id` | integer |  |
| `def_player_name` | character |  |
| `gp` | integer | Games played. |
| `matchup_min` | character |  |
| `partial_poss` | character |  |
| `player_pts` | character |  |
| `team_pts` | character |  |
| `matchup_ast` | character |  |
| `matchup_tov` | character |  |
| `matchup_blk` | character |  |
| `matchup_fgm` | character |  |
| `matchup_fga` | character |  |
| `matchup_fg_pct` | numeric |  |
| `matchup_fg3m` | character |  |
| `matchup_fg3a` | character |  |
| `matchup_fg3_pct` | numeric |  |
| `matchup_ftm` | character |  |
| `matchup_fta` | character |  |
| `sfl` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_matchupsrollup(league_id='00')
```

_Last validated n/a._

## `nba_stats_playbyplay`

GET /stats/playbyplay

**Endpoint URL:** `GET https://stats.nba.com/stats/playbyplay`

**Valid URL:** [https://stats.nba.com/stats/playbyplay](https://stats.nba.com/stats/playbyplay)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `video_available_flag` | character | Video available flag. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playbyplay()
```

_Last validated n/a._

## `nba_stats_playbyplayv2`

GET /stats/playbyplayv2

**Endpoint URL:** `GET https://stats.nba.com/stats/playbyplayv2`

**Valid URL:** [https://stats.nba.com/stats/playbyplayv2](https://stats.nba.com/stats/playbyplayv2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `video_available_flag` | character | Video available flag. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playbyplayv2()
```

_Last validated n/a._

## `nba_stats_playbyplayv3`

GET /stats/playbyplayv3

**Endpoint URL:** `GET https://stats.nba.com/stats/playbyplayv3`

**Valid URL:** [https://stats.nba.com/stats/playbyplayv3](https://stats.nba.com/stats/playbyplayv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `videoavailable` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playbyplayv3()
```

_Last validated n/a._

## `nba_stats_playerawards`

GET /stats/playerawards

**Endpoint URL:** `GET https://stats.nba.com/stats/playerawards`

**Valid URL:** [https://stats.nba.com/stats/playerawards](https://stats.nba.com/stats/playerawards)

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
| `all_nba_team_number` | character |  |
| `season` | character | Season year. |
| `month` | character |  |
| `week` | character | Week number. |
| `conference` | character | Conference. |
| `type` | character | Record type / category. |
| `subtype1` | character |  |
| `subtype2` | character |  |
| `subtype3` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerawards()
```

_Last validated n/a._

## `nba_stats_playercareerbycollege`

GET /stats/playercareerbycollege

**Endpoint URL:** `GET https://stats.nba.com/stats/playercareerbycollege`

**Valid URL:** [https://stats.nba.com/stats/playercareerbycollege?LeagueID=00](https://stats.nba.com/stats/playercareerbycollege?LeagueID=00)

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
| `college` | character | Official college (usually the last one attended) |
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
nba_stats_playercareerbycollege(league_id='00')
```

_Last validated n/a._

## `nba_stats_playercareerbycollegerollup`

GET /stats/playercareerbycollegerollup

**Endpoint URL:** `GET https://stats.nba.com/stats/playercareerbycollegerollup`

**Valid URL:** [https://stats.nba.com/stats/playercareerbycollegerollup?LeagueID=00](https://stats.nba.com/stats/playercareerbycollegerollup?LeagueID=00)

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
| `seed` | character |  |
| `college` | character | Official college (usually the last one attended) |
| `players` | character |  |
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
nba_stats_playercareerbycollegerollup(league_id='00')
```

_Last validated n/a._

## `nba_stats_playercareerstats`

GET /stats/playercareerstats

**Endpoint URL:** `GET https://stats.nba.com/stats/playercareerstats`

**Valid URL:** [https://stats.nba.com/stats/playercareerstats?LeagueID=00](https://stats.nba.com/stats/playercareerstats?LeagueID=00)

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
| `game_date` | character | Game date (YYYY-MM-DD). |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_name` | character |  |
| `vs_team_abbreviation` | character |  |
| `stat` | character | Stat. |
| `stats_value` | integer |  |
| `stat_order` | integer |  |
| `date_est` | character |  |
| `game_id` | character | Unique game identifier. |
| `stat_value` | integer | Numeric stat value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playercareerstats(league_id='00')
```

_Last validated n/a._

## `nba_stats_playercompare`

GET /stats/playercompare

**Endpoint URL:** `GET https://stats.nba.com/stats/playercompare`

**Valid URL:** [https://stats.nba.com/stats/playercompare?LeagueID=00](https://stats.nba.com/stats/playercompare?LeagueID=00)

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
| `group_set` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playercompare(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashboardbyclutch`

GET /stats/playerdashboardbyclutch

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashboardbyclutch`

**Valid URL:** [https://stats.nba.com/stats/playerdashboardbyclutch?LeagueID=00](https://stats.nba.com/stats/playerdashboardbyclutch?LeagueID=00)

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
| `group_set` | character |  |
| `group_value` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `dd2` | character |  |
| `td3` | character |  |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `nba_fantasy_pts_rank` | character |  |
| `dd2_rank` | character |  |
| `td3_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashboardbyclutch(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashboardbygamesplits`

GET /stats/playerdashboardbygamesplits

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashboardbygamesplits`

**Valid URL:** [https://stats.nba.com/stats/playerdashboardbygamesplits?LeagueID=00](https://stats.nba.com/stats/playerdashboardbygamesplits?LeagueID=00)

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
| `group_set` | character |  |
| `group_value` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `dd2` | character |  |
| `td3` | character |  |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `nba_fantasy_pts_rank` | character |  |
| `dd2_rank` | character |  |
| `td3_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashboardbygamesplits(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashboardbygeneralsplits`

GET /stats/playerdashboardbygeneralsplits

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashboardbygeneralsplits`

**Valid URL:** [https://stats.nba.com/stats/playerdashboardbygeneralsplits?LeagueID=00](https://stats.nba.com/stats/playerdashboardbygeneralsplits?LeagueID=00)

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
| `group_set` | character |  |
| `group_value` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `dd2` | character |  |
| `td3` | character |  |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `nba_fantasy_pts_rank` | character |  |
| `dd2_rank` | character |  |
| `td3_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashboardbygeneralsplits(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashboardbylastngames`

GET /stats/playerdashboardbylastngames

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashboardbylastngames`

**Valid URL:** [https://stats.nba.com/stats/playerdashboardbylastngames?LeagueID=00](https://stats.nba.com/stats/playerdashboardbylastngames?LeagueID=00)

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
| `group_set` | character |  |
| `group_value` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `dd2` | character |  |
| `td3` | character |  |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `nba_fantasy_pts_rank` | character |  |
| `dd2_rank` | character |  |
| `td3_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashboardbylastngames(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashboardbyopponent`

GET /stats/playerdashboardbyopponent

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashboardbyopponent`

**Valid URL:** [https://stats.nba.com/stats/playerdashboardbyopponent](https://stats.nba.com/stats/playerdashboardbyopponent)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashboardbyopponent()
```

_Last validated n/a._

## `nba_stats_playerdashboardbyshootingsplits`

GET /stats/playerdashboardbyshootingsplits

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashboardbyshootingsplits`

**Valid URL:** [https://stats.nba.com/stats/playerdashboardbyshootingsplits?LeagueID=00](https://stats.nba.com/stats/playerdashboardbyshootingsplits?LeagueID=00)

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
| `group_set` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `efg_pct` | numeric |  |
| `blka` | character |  |
| `pct_ast_2pm` | numeric |  |
| `pct_uast_2pm` | numeric |  |
| `pct_ast_3pm` | numeric |  |
| `pct_uast_3pm` | numeric |  |
| `pct_ast_fgm` | numeric |  |
| `pct_uast_fgm` | numeric |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `efg_pct_rank` | numeric |  |
| `blka_rank` | character |  |
| `pct_ast_2pm_rank` | numeric |  |
| `pct_uast_2pm_rank` | numeric |  |
| `pct_ast_3pm_rank` | numeric |  |
| `pct_uast_3pm_rank` | numeric |  |
| `pct_ast_fgm_rank` | numeric |  |
| `pct_uast_fgm_rank` | numeric |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashboardbyshootingsplits(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashboardbyteamperformance`

GET /stats/playerdashboardbyteamperformance

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashboardbyteamperformance`

**Valid URL:** [https://stats.nba.com/stats/playerdashboardbyteamperformance?LeagueID=00](https://stats.nba.com/stats/playerdashboardbyteamperformance?LeagueID=00)

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
| `group_set` | character |  |
| `group_value` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `dd2` | character |  |
| `td3` | character |  |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `nba_fantasy_pts_rank` | character |  |
| `dd2_rank` | character |  |
| `td3_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashboardbyteamperformance(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashboardbyyearoveryear`

GET /stats/playerdashboardbyyearoveryear

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashboardbyyearoveryear`

**Valid URL:** [https://stats.nba.com/stats/playerdashboardbyyearoveryear?LeagueID=00](https://stats.nba.com/stats/playerdashboardbyyearoveryear?LeagueID=00)

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
| `group_set` | character |  |
| `group_value` | character |  |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `max_game_date` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `dd2` | character |  |
| `td3` | character |  |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `nba_fantasy_pts_rank` | character |  |
| `dd2_rank` | character |  |
| `td3_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashboardbyyearoveryear(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashptpass`

GET /stats/playerdashptpass

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashptpass`

**Valid URL:** [https://stats.nba.com/stats/playerdashptpass?LeagueID=00](https://stats.nba.com/stats/playerdashptpass?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
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
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character |  |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `pass_type` | character |  |
| `g` | character | Games played. |
| `pass_to` | character |  |
| `pass_teammate_player_id` | integer |  |
| `frequency` | character |  |
| `pass` | character | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `ast` | character | Assists. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg2m` | character |  |
| `fg2a` | character |  |
| `fg2_pct` | numeric |  |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashptpass(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashptreb`

GET /stats/playerdashptreb

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashptreb`

**Valid URL:** [https://stats.nba.com/stats/playerdashptreb?LeagueID=00](https://stats.nba.com/stats/playerdashptreb?LeagueID=00)

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
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character |  |
| `sort_order` | character | Display sort order for the sport. |
| `g` | character | Games played. |
| `reb_num_contesting_range` | character |  |
| `reb_frequency` | character |  |
| `oreb` | character | Offensive rebounds. |
| `dreb` | character | Defensive rebounds. |
| `reb` | character | Total rebounds. |
| `c_oreb` | character |  |
| `c_dreb` | character |  |
| `c_reb` | character |  |
| `c_reb_pct` | numeric |  |
| `uc_oreb` | character |  |
| `uc_dreb` | character |  |
| `uc_reb` | character |  |
| `uc_reb_pct` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashptreb(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashptshotdefend`

GET /stats/playerdashptshotdefend

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashptshotdefend`

**Valid URL:** [https://stats.nba.com/stats/playerdashptshotdefend?LeagueID=00](https://stats.nba.com/stats/playerdashptshotdefend?LeagueID=00)

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
| `close_def_person_id` | integer |  |
| `gp` | integer | Games played. |
| `g` | character | Games played. |
| `defense_category` | character |  |
| `freq` | character |  |
| `d_fgm` | character |  |
| `d_fga` | character |  |
| `d_fg_pct` | numeric |  |
| `normal_fg_pct` | numeric |  |
| `pct_plusminus` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashptshotdefend(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerdashptshots`

GET /stats/playerdashptshots

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashptshots`

**Valid URL:** [https://stats.nba.com/stats/playerdashptshots?LeagueID=00](https://stats.nba.com/stats/playerdashptshots?LeagueID=00)

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
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character |  |
| `sort_order` | character | Display sort order for the sport. |
| `gp` | integer | Games played. |
| `g` | character | Games played. |
| `close_def_dist_range` | character |  |
| `fga_frequency` | character |  |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric |  |
| `fg2a_frequency` | character |  |
| `fg2m` | character |  |
| `fg2a` | character |  |
| `fg2_pct` | numeric |  |
| `fg3a_frequency` | character |  |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerdashptshots(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerestimatedmetrics`

GET /stats/playerestimatedmetrics

**Endpoint URL:** `GET https://stats.nba.com/stats/playerestimatedmetrics`

**Valid URL:** [https://stats.nba.com/stats/playerestimatedmetrics?LeagueID=00](https://stats.nba.com/stats/playerestimatedmetrics?LeagueID=00)

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
| `e_off_rating` | numeric |  |
| `e_def_rating` | numeric |  |
| `e_net_rating` | numeric |  |
| `e_ast_ratio` | numeric |  |
| `e_oreb_pct` | numeric |  |
| `e_dreb_pct` | numeric |  |
| `e_reb_pct` | numeric |  |
| `e_tov_pct` | numeric |  |
| `e_usg_pct` | numeric |  |
| `e_pace` | numeric |  |
| `gp_rank` | integer |  |
| `w_rank` | integer |  |
| `l_rank` | integer |  |
| `w_pct_rank` | integer |  |
| `min_rank` | integer |  |
| `e_off_rating_rank` | integer |  |
| `e_def_rating_rank` | integer |  |
| `e_net_rating_rank` | integer |  |
| `e_ast_ratio_rank` | integer |  |
| `e_oreb_pct_rank` | integer |  |
| `e_dreb_pct_rank` | integer |  |
| `e_reb_pct_rank` | integer |  |
| `e_tov_pct_rank` | integer |  |
| `e_usg_pct_rank` | integer |  |
| `e_pace_rank` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerestimatedmetrics(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerfantasyprofile`

GET /stats/playerfantasyprofile

**Endpoint URL:** `GET https://stats.nba.com/stats/playerfantasyprofile`

**Valid URL:** [https://stats.nba.com/stats/playerfantasyprofile](https://stats.nba.com/stats/playerfantasyprofile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerfantasyprofile()
```

_Last validated n/a._

## `nba_stats_playerfantasyprofilebargraph`

GET /stats/playerfantasyprofilebargraph

**Endpoint URL:** `GET https://stats.nba.com/stats/playerfantasyprofilebargraph`

**Valid URL:** [https://stats.nba.com/stats/playerfantasyprofilebargraph?LeagueID=00](https://stats.nba.com/stats/playerfantasyprofilebargraph?LeagueID=00)

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
| `fan_duel_pts` | character |  |
| `nba_fantasy_pts` | character |  |
| `pts` | character | Points scored. |
| `reb` | character | Total rebounds. |
| `ast` | character | Assists. |
| `fg3m` | character | Three-point field goals made. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `tov` | character | Turnovers. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerfantasyprofilebargraph(league_id='00')
```

_Last validated n/a._

## `nba_stats_playergamelog`

GET /stats/playergamelog

**Endpoint URL:** `GET https://stats.nba.com/stats/playergamelog`

**Valid URL:** [https://stats.nba.com/stats/playergamelog?LeagueID=00](https://stats.nba.com/stats/playergamelog?LeagueID=00)

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
| `season_id` | integer | Unique season identifier. |
| `player_id` | integer | Unique player identifier. |
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
nba_stats_playergamelog(league_id='00')
```

_Last validated n/a._

## `nba_stats_playergamelogs`

GET /stats/playergamelogs

**Endpoint URL:** `GET https://stats.nba.com/stats/playergamelogs`

**Valid URL:** [https://stats.nba.com/stats/playergamelogs?LeagueID=00](https://stats.nba.com/stats/playergamelogs?LeagueID=00)

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
| `tov` | character | Turnovers. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `dd2` | character |  |
| `td3` | character |  |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `nba_fantasy_pts_rank` | character |  |
| `dd2_rank` | character |  |
| `td3_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playergamelogs(league_id='00')
```

_Last validated n/a._

## `nba_stats_playergamestreakfinder`

GET /stats/playergamestreakfinder

**Endpoint URL:** `GET https://stats.nba.com/stats/playergamestreakfinder`

**Valid URL:** [https://stats.nba.com/stats/playergamestreakfinder?LeagueID=00](https://stats.nba.com/stats/playergamestreakfinder?LeagueID=00)

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
| `player_name_last_first` | character |  |
| `player_id` | integer | Unique player identifier. |
| `gamestreak` | character |  |
| `startdate` | character |  |
| `enddate` | character |  |
| `activestreak` | character |  |
| `numseasons` | character |  |
| `lastseason` | character |  |
| `firstseason` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playergamestreakfinder(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerindex`

GET /stats/playerindex

**Endpoint URL:** `GET https://stats.nba.com/stats/playerindex`

**Valid URL:** [https://stats.nba.com/stats/playerindex?LeagueID=00](https://stats.nba.com/stats/playerindex?LeagueID=00)

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
| `is_defunct` | integer |  |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `college` | character | Official college (usually the last one attended) |
| `country` | character | Country (full name or code). |
| `draft_year` | integer | Draft year (4-digit). |
| `draft_round` | integer | Round of the draft selection. |
| `draft_number` | integer | The number pick that was used to select a given player. |
| `roster_status` | numeric | Payroll table the row came from: Active, IL, or Retained Salary. |
| `from_year` | character |  |
| `to_year` | character |  |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `stats_timeframe` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerindex(league_id='00')
```

_Last validated n/a._

## `nba_stats_playernextngames`

GET /stats/playernextngames

**Endpoint URL:** `GET https://stats.nba.com/stats/playernextngames`

**Valid URL:** [https://stats.nba.com/stats/playernextngames?LeagueID=00](https://stats.nba.com/stats/playernextngames?LeagueID=00)

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
| `visitor_team_name` | character |  |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `visitor_team_abbreviation` | character |  |
| `home_team_nickname` | character | Home team nickname label; `team_detail = TRUE` only. |
| `visitor_team_nickname` | character |  |
| `game_time` | character | Game start time. |
| `home_wl` | character |  |
| `visitor_wl` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playernextngames(league_id='00')
```

_Last validated n/a._

## `nba_stats_playerprofilev2`

GET /stats/playerprofilev2

**Endpoint URL:** `GET https://stats.nba.com/stats/playerprofilev2`

**Valid URL:** [https://stats.nba.com/stats/playerprofilev2?LeagueID=00](https://stats.nba.com/stats/playerprofilev2?LeagueID=00)

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
| `game_date` | character | Game date (YYYY-MM-DD). |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_name` | character |  |
| `vs_team_abbreviation` | character |  |
| `stat` | character | Stat. |
| `stats_value` | character |  |
| `stat_order` | character |  |
| `date_est` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playerprofilev2(league_id='00')
```

_Last validated n/a._

## `nba_stats_playervsplayer`

GET /stats/playervsplayer

**Endpoint URL:** `GET https://stats.nba.com/stats/playervsplayer`

**Valid URL:** [https://stats.nba.com/stats/playervsplayer?LeagueID=00](https://stats.nba.com/stats/playervsplayer?LeagueID=00)

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
| `group_set` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer |  |
| `vs_player_name` | character |  |
| `court_status` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playervsplayer(league_id='00')
```

_Last validated n/a._

## `nba_stats_playoffpicture`

GET /stats/playoffpicture

**Endpoint URL:** `GET https://stats.nba.com/stats/playoffpicture`

**Valid URL:** [https://stats.nba.com/stats/playoffpicture?LeagueID=00](https://stats.nba.com/stats/playoffpicture?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonID` | `season_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `conference` | character | Conference. |
| `high_seed_rank` | character |  |
| `high_seed_team` | character |  |
| `high_seed_team_id` | integer |  |
| `low_seed_rank` | character |  |
| `low_seed_team` | character |  |
| `low_seed_team_id` | integer |  |
| `high_seed_series_w` | character |  |
| `high_seed_series_l` | character |  |
| `high_seed_series_remaining_g` | character |  |
| `high_seed_series_remaining_home_g` | character |  |
| `high_seed_series_remaining_away_g` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_playoffpicture(league_id='00')
```

_Last validated n/a._

## `nba_stats_scheduleleaguev2`

GET /stats/scheduleleaguev2

**Endpoint URL:** `GET https://stats.nba.com/stats/scheduleleaguev2`

**Valid URL:** [https://stats.nba.com/stats/scheduleleaguev2?LeagueID=00](https://stats.nba.com/stats/scheduleleaguev2?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `leagueid` | character |  |
| `seasonyear` | character |  |
| `gamedate` | character | Game date as parsed from the source feed. |
| `gameid` | character |  |
| `gamecode` | character | Gamecode. |
| `gamestatus` | character |  |
| `gamestatustext` | character |  |
| `gamesequence` | character |  |
| `gamedateest` | character |  |
| `gametimeest` | character |  |
| `gamedatetimeest` | character |  |
| `gamedateutc` | character |  |
| `gametimeutc` | character |  |
| `gamedatetimeutc` | character |  |
| `awayteamtime` | character |  |
| `hometeamtime` | character |  |
| `day` | character | Day number within the month. |
| `monthnum` | character |  |
| `weeknumber` | character |  |
| `weekname` | character |  |
| `ifnecessary` | character |  |
| `seriesgamenumber` | character |  |
| `gamelabel` | character |  |
| `gamesublabel` | character |  |
| `seriestext` | character |  |
| `arenaname` | character |  |
| `arenastate` | character |  |
| `arenacity` | character |  |
| `postponedstatus` | character |  |
| `branchlink` | character |  |
| `gamesubtype` | character |  |
| `isneutral` | character |  |
| `hometeam_teamid` | character |  |
| `hometeam_teamname` | character |  |
| `hometeam_teamcity` | character |  |
| `hometeam_teamtricode` | character |  |
| `hometeam_teamslug` | character |  |
| `hometeam_wins` | character |  |
| `hometeam_losses` | character |  |
| `hometeam_score` | character |  |
| `hometeam_seed` | character |  |
| `awayteam_teamid` | character |  |
| `awayteam_teamname` | character |  |
| `awayteam_teamcity` | character |  |
| `awayteam_teamtricode` | character |  |
| `awayteam_teamslug` | character |  |
| `awayteam_wins` | character |  |
| `awayteam_losses` | character |  |
| `awayteam_score` | character |  |
| `awayteam_seed` | character |  |
| `pointsleaders_personid` | character |  |
| `pointsleaders_firstname` | character |  |
| `pointsleaders_lastname` | character |  |
| `pointsleaders_teamid` | character |  |
| `pointsleaders_teamcity` | character |  |
| `pointsleaders_teamname` | character |  |
| `pointsleaders_teamtricode` | character |  |
| `pointsleaders_points` | character |  |
| `nationalbroadcasters_broadcasterscope` | character |  |
| `nationalbroadcasters_broadcastermedia` | character |  |
| `nationalbroadcasters_broadcasterid` | character |  |
| `nationalbroadcasters_broadcasterdisplay` | character |  |
| `nationalbroadcasters_broadcasterabbreviation` | character |  |
| `nationalbroadcasters_tapedelaycomments` | character |  |
| `nationalbroadcasters_broadcastervideolink` | character |  |
| `nationalbroadcasters_broadcasterdescription` | character |  |
| `nationalbroadcasters_broadcasterteamid` | character |  |
| `nationalradiobroadcasters_broadcasterscope` | character |  |
| `nationalradiobroadcasters_broadcastermedia` | character |  |
| `nationalradiobroadcasters_broadcasterid` | character |  |
| `nationalradiobroadcasters_broadcasterdisplay` | character |  |
| `nationalradiobroadcasters_broadcasterabbreviation` | character |  |
| `nationalradiobroadcasters_tapedelaycomments` | character |  |
| `nationalradiobroadcasters_broadcastervideolink` | character |  |
| `nationalradiobroadcasters_broadcasterdescription` | character |  |
| `nationalradiobroadcasters_broadcasterteamid` | character |  |
| `nationalottbroadcasters_broadcasterscope` | character |  |
| `nationalottbroadcasters_broadcastermedia` | character |  |
| `nationalottbroadcasters_broadcasterid` | character |  |
| `nationalottbroadcasters_broadcasterdisplay` | character |  |
| `nationalottbroadcasters_broadcasterabbreviation` | character |  |
| `nationalottbroadcasters_tapedelaycomments` | character |  |
| `nationalottbroadcasters_broadcastervideolink` | character |  |
| `nationalottbroadcasters_broadcasterdescription` | character |  |
| `nationalottbroadcasters_broadcasterteamid` | character |  |
| `hometvbroadcasters_broadcasterscope` | character |  |
| `hometvbroadcasters_broadcastermedia` | character |  |
| `hometvbroadcasters_broadcasterid` | character |  |
| `hometvbroadcasters_broadcasterdisplay` | character |  |
| `hometvbroadcasters_broadcasterabbreviation` | character |  |
| `hometvbroadcasters_tapedelaycomments` | character |  |
| `hometvbroadcasters_broadcastervideolink` | character |  |
| `hometvbroadcasters_broadcasterdescription` | character |  |
| `hometvbroadcasters_broadcasterteamid` | character |  |
| `homeradiobroadcasters_broadcasterscope` | character |  |
| `homeradiobroadcasters_broadcastermedia` | character |  |
| `homeradiobroadcasters_broadcasterid` | character |  |
| `homeradiobroadcasters_broadcasterdisplay` | character |  |
| `homeradiobroadcasters_broadcasterabbreviation` | character |  |
| `homeradiobroadcasters_tapedelaycomments` | character |  |
| `homeradiobroadcasters_broadcastervideolink` | character |  |
| `homeradiobroadcasters_broadcasterdescription` | character |  |
| `homeradiobroadcasters_broadcasterteamid` | character |  |
| `homeottbroadcasters_broadcasterscope` | character |  |
| `homeottbroadcasters_broadcastermedia` | character |  |
| `homeottbroadcasters_broadcasterid` | character |  |
| `homeottbroadcasters_broadcasterdisplay` | character |  |
| `homeottbroadcasters_broadcasterabbreviation` | character |  |
| `homeottbroadcasters_tapedelaycomments` | character |  |
| `homeottbroadcasters_broadcastervideolink` | character |  |
| `homeottbroadcasters_broadcasterdescription` | character |  |
| `homeottbroadcasters_broadcasterteamid` | character |  |
| `awaytvbroadcasters_broadcasterscope` | character |  |
| `awaytvbroadcasters_broadcastermedia` | character |  |
| `awaytvbroadcasters_broadcasterid` | character |  |
| `awaytvbroadcasters_broadcasterdisplay` | character |  |
| `awaytvbroadcasters_broadcasterabbreviation` | character |  |
| `awaytvbroadcasters_tapedelaycomments` | character |  |
| `awaytvbroadcasters_broadcastervideolink` | character |  |
| `awaytvbroadcasters_broadcasterdescription` | character |  |
| `awaytvbroadcasters_broadcasterteamid` | character |  |
| `awayradiobroadcasters_broadcasterscope` | character |  |
| `awayradiobroadcasters_broadcastermedia` | character |  |
| `awayradiobroadcasters_broadcasterid` | character |  |
| `awayradiobroadcasters_broadcasterdisplay` | character |  |
| `awayradiobroadcasters_broadcasterabbreviation` | character |  |
| `awayradiobroadcasters_tapedelaycomments` | character |  |
| `awayradiobroadcasters_broadcastervideolink` | character |  |
| `awayradiobroadcasters_broadcasterdescription` | character |  |
| `awayradiobroadcasters_broadcasterteamid` | character |  |
| `awayottbroadcasters_broadcasterscope` | character |  |
| `awayottbroadcasters_broadcastermedia` | character |  |
| `awayottbroadcasters_broadcasterid` | character |  |
| `awayottbroadcasters_broadcasterdisplay` | character |  |
| `awayottbroadcasters_broadcasterabbreviation` | character |  |
| `awayottbroadcasters_tapedelaycomments` | character |  |
| `awayottbroadcasters_broadcastervideolink` | character |  |
| `awayottbroadcasters_broadcasterdescription` | character |  |
| `awayottbroadcasters_broadcasterteamid` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_scheduleleaguev2(league_id='00')
```

_Last validated n/a._

## `nba_stats_scheduleleaguev2int`

GET /stats/scheduleleaguev2int

**Endpoint URL:** `GET https://stats.nba.com/stats/scheduleleaguev2int`

**Valid URL:** [https://stats.nba.com/stats/scheduleleaguev2int?LeagueID=00](https://stats.nba.com/stats/scheduleleaguev2int?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `leagueid` | character |  |
| `seasonyear` | character |  |
| `broadcasterabbreviation` | character |  |
| `broadcasterdisplay` | character |  |
| `broadcasterid` | character |  |
| `regionid` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_scheduleleaguev2int(league_id='00')
```

_Last validated n/a._

## `nba_stats_scoreboard`

GET /stats/scoreboard

**Endpoint URL:** `GET https://stats.nba.com/stats/scoreboard`

**Valid URL:** [https://stats.nba.com/stats/scoreboard](https://stats.nba.com/stats/scoreboard)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_scoreboard()
```

_Last validated n/a._

## `nba_stats_scoreboardv2`

GET /stats/scoreboardv2

**Endpoint URL:** `GET https://stats.nba.com/stats/scoreboardv2`

**Valid URL:** [https://stats.nba.com/stats/scoreboardv2?LeagueID=00](https://stats.nba.com/stats/scoreboardv2?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DayOffset` | `day_offset` |  |  | `Y` |  |
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `pt_available` | character | Pt available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_scoreboardv2(league_id='00')
```

_Last validated n/a._

## `nba_stats_scoreboardv3`

GET /stats/scoreboardv3

**Endpoint URL:** `GET https://stats.nba.com/stats/scoreboardv3`

**Valid URL:** [https://stats.nba.com/stats/scoreboardv3](https://stats.nba.com/stats/scoreboardv3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_scoreboardv3()
```

_Last validated n/a._

## `nba_stats_shotchartdetail`

GET /stats/shotchartdetail

**Endpoint URL:** `GET https://stats.nba.com/stats/shotchartdetail`

**Valid URL:** [https://stats.nba.com/stats/shotchartdetail?LeagueID=00](https://stats.nba.com/stats/shotchartdetail?LeagueID=00)

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
| `grid_type` | character |  |
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `fga` | character | Field goal attempts. |
| `fgm` | character | Field goals made. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_shotchartdetail(league_id='00')
```

_Last validated n/a._

## `nba_stats_shotchartleaguewide`

GET /stats/shotchartleaguewide

**Endpoint URL:** `GET https://stats.nba.com/stats/shotchartleaguewide`

**Valid URL:** [https://stats.nba.com/stats/shotchartleaguewide?LeagueID=00](https://stats.nba.com/stats/shotchartleaguewide?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grid_type` | character |  |
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `fga` | character | Field goal attempts. |
| `fgm` | character | Field goals made. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_shotchartleaguewide(league_id='00')
```

_Last validated n/a._

## `nba_stats_shotchartlineupdetail`

GET /stats/shotchartlineupdetail

**Endpoint URL:** `GET https://stats.nba.com/stats/shotchartlineupdetail`

**Valid URL:** [https://stats.nba.com/stats/shotchartlineupdetail?LeagueID=00](https://stats.nba.com/stats/shotchartlineupdetail?LeagueID=00)

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
| `grid_type` | character |  |
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
| `shot_type` | character | Type of shot taken (e.g. wrist, snap, backhand). |
| `shot_zone_basic` | character | Shot zone (e.g. 'Restricted Area', 'Mid-Range', 'Above the Break 3'). |
| `shot_zone_area` | character | Shot zone area ('Left Side', 'Right Side', 'Center'). |
| `shot_zone_range` | character | Shot zone range ('Less Than 8 ft.', '8-16 ft.', '16-24 ft.', etc.). |
| `shot_distance` | character | Shot distance from the basket, in feet. |
| `loc_x` | character | X coordinate on the court (units of inches; 0 = basket center). |
| `loc_y` | character | Y coordinate on the court (units of inches; baseline at 0). |
| `shot_attempted_flag` | character | 1 if a shot was attempted on this event. |
| `shot_made_flag` | character | 1 if the shot was made; 0 if missed. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `htm` | character |  |
| `vtm` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_shotchartlineupdetail(league_id='00')
```

_Last validated n/a._

## `nba_stats_synergyplaytypes`

GET /stats/synergyplaytypes

**Endpoint URL:** `GET https://stats.nba.com/stats/synergyplaytypes`

**Valid URL:** [https://stats.nba.com/stats/synergyplaytypes?LeagueID=00](https://stats.nba.com/stats/synergyplaytypes?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayType` | `play_type_nullable` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team_abbreviation` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `SeasonYear` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `TypeGrouping` | `type_grouping_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | integer | Unique season identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `play_type` | character | String indicating the type of play: pass (includes sacks), run (includes scrambles), punt, field_goal, kickoff, extra_point, qb_kneel, qb_spike, no_play (timeouts and penalties), and missing for rows indicating end of play. |
| `type_grouping` | character |  |
| `percentile` | character |  |
| `gp` | integer | Games played. |
| `poss_pct` | numeric | Poss percentage (0-1 decimal). |
| `ppp` | character |  |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `ft_poss_pct` | numeric |  |
| `tov_poss_pct` | numeric |  |
| `sf_poss_pct` | numeric |  |
| `plusone_poss_pct` | numeric |  |
| `score_poss_pct` | numeric |  |
| `efg_pct` | numeric |  |
| `poss` | character | Poss. |
| `pts` | character | Points scored. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fgmx` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_synergyplaytypes(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamandplayersvsplayers`

GET /stats/teamandplayersvsplayers

**Endpoint URL:** `GET https://stats.nba.com/stats/teamandplayersvsplayers`

**Valid URL:** [https://stats.nba.com/stats/teamandplayersvsplayers?LeagueID=00](https://stats.nba.com/stats/teamandplayersvsplayers?LeagueID=00)

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
| `PlayerID1` | `player_id1` |  |  | `Y` |  |
| `PlayerID2` | `player_id2` |  |  | `Y` |  |
| `PlayerID3` | `player_id3` |  |  | `Y` |  |
| `PlayerID4` | `player_id4` |  |  | `Y` |  |
| `PlayerID5` | `player_id5` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerID1` | `vs_player_id1` |  |  | `Y` |  |
| `VsPlayerID2` | `vs_player_id2` |  |  | `Y` |  |
| `VsPlayerID3` | `vs_player_id3` |  |  | `Y` |  |
| `VsPlayerID4` | `vs_player_id4` |  |  | `Y` |  |
| `VsPlayerID5` | `vs_player_id5` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character |  |
| `title_description` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamandplayersvsplayers(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamdashboardbyclutch`

GET /stats/teamdashboardbyclutch

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbyclutch`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbyclutch](https://stats.nba.com/stats/teamdashboardbyclutch)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashboardbyclutch()
```

_Last validated n/a._

## `nba_stats_teamdashboardbygamesplits`

GET /stats/teamdashboardbygamesplits

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbygamesplits`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbygamesplits](https://stats.nba.com/stats/teamdashboardbygamesplits)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashboardbygamesplits()
```

_Last validated n/a._

## `nba_stats_teamdashboardbygeneralsplits`

GET /stats/teamdashboardbygeneralsplits

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbygeneralsplits`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbygeneralsplits?LeagueID=00](https://stats.nba.com/stats/teamdashboardbygeneralsplits?LeagueID=00)

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
| `group_set` | character |  |
| `group_value` | character |  |
| `team_days_rest_range` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashboardbygeneralsplits(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamdashboardbylastngames`

GET /stats/teamdashboardbylastngames

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbylastngames`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbylastngames](https://stats.nba.com/stats/teamdashboardbylastngames)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashboardbylastngames()
```

_Last validated n/a._

## `nba_stats_teamdashboardbyopponent`

GET /stats/teamdashboardbyopponent

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbyopponent`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbyopponent](https://stats.nba.com/stats/teamdashboardbyopponent)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashboardbyopponent()
```

_Last validated n/a._

## `nba_stats_teamdashboardbyshootingsplits`

GET /stats/teamdashboardbyshootingsplits

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbyshootingsplits`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbyshootingsplits?LeagueID=00](https://stats.nba.com/stats/teamdashboardbyshootingsplits?LeagueID=00)

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
| `group_set` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `efg_pct` | numeric |  |
| `blka` | character |  |
| `pct_ast_2pm` | numeric |  |
| `pct_uast_2pm` | numeric |  |
| `pct_ast_3pm` | numeric |  |
| `pct_uast_3pm` | numeric |  |
| `pct_ast_fgm` | numeric |  |
| `pct_uast_fgm` | numeric |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `efg_pct_rank` | numeric |  |
| `blka_rank` | character |  |
| `pct_ast_2pm_rank` | numeric |  |
| `pct_uast_2pm_rank` | numeric |  |
| `pct_ast_3pm_rank` | numeric |  |
| `pct_uast_3pm_rank` | numeric |  |
| `pct_ast_fgm_rank` | numeric |  |
| `pct_uast_fgm_rank` | numeric |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashboardbyshootingsplits(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamdashboardbyteamperformance`

GET /stats/teamdashboardbyteamperformance

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbyteamperformance`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbyteamperformance](https://stats.nba.com/stats/teamdashboardbyteamperformance)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashboardbyteamperformance()
```

_Last validated n/a._

## `nba_stats_teamdashboardbyyearoveryear`

GET /stats/teamdashboardbyyearoveryear

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbyyearoveryear`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbyyearoveryear](https://stats.nba.com/stats/teamdashboardbyyearoveryear)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashboardbyyearoveryear()
```

_Last validated n/a._

## `nba_stats_teamdashlineups`

GET /stats/teamdashlineups

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashlineups`

**Valid URL:** [https://stats.nba.com/stats/teamdashlineups?LeagueID=00](https://stats.nba.com/stats/teamdashlineups?LeagueID=00)

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
| `group_set` | character |  |
| `group_id` | integer | ESPN group id. |
| `group_name` | character | Group name (conference / division). |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashlineups(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamdashptpass`

GET /stats/teamdashptpass

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashptpass`

**Valid URL:** [https://stats.nba.com/stats/teamdashptpass?LeagueID=00](https://stats.nba.com/stats/teamdashptpass?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pass_type` | character |  |
| `g` | character | Games played. |
| `pass_from` | character |  |
| `pass_teammate_player_id` | integer |  |
| `frequency` | character |  |
| `pass` | character | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `ast` | character | Assists. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg2m` | character |  |
| `fg2a` | character |  |
| `fg2_pct` | numeric |  |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashptpass(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamdashptreb`

GET /stats/teamdashptreb

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashptreb`

**Valid URL:** [https://stats.nba.com/stats/teamdashptreb?LeagueID=00](https://stats.nba.com/stats/teamdashptreb?LeagueID=00)

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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `sort_order` | character | Display sort order for the sport. |
| `g` | character | Games played. |
| `reb_num_contesting_range` | character |  |
| `reb_frequency` | character |  |
| `oreb` | character | Offensive rebounds. |
| `dreb` | character | Defensive rebounds. |
| `reb` | character | Total rebounds. |
| `c_oreb` | character |  |
| `c_dreb` | character |  |
| `c_reb` | character |  |
| `c_reb_pct` | numeric |  |
| `uc_oreb` | character |  |
| `uc_dreb` | character |  |
| `uc_reb` | character |  |
| `uc_reb_pct` | numeric |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashptreb(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamdashptshots`

GET /stats/teamdashptshots

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashptshots`

**Valid URL:** [https://stats.nba.com/stats/teamdashptshots?LeagueID=00](https://stats.nba.com/stats/teamdashptshots?LeagueID=00)

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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `sort_order` | character | Display sort order for the sport. |
| `g` | character | Games played. |
| `close_def_dist_range` | character |  |
| `fga_frequency` | character |  |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric |  |
| `fg2a_frequency` | character |  |
| `fg2m` | character |  |
| `fg2a` | character |  |
| `fg2_pct` | numeric |  |
| `fg3a_frequency` | character |  |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdashptshots(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamdetails`

GET /stats/teamdetails

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdetails`

**Valid URL:** [https://stats.nba.com/stats/teamdetails](https://stats.nba.com/stats/teamdetails)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `yearawarded` | character |  |
| `oppositeteam` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamdetails()
```

_Last validated n/a._

## `nba_stats_teamestimatedmetrics`

GET /stats/teamestimatedmetrics

**Endpoint URL:** `GET https://stats.nba.com/stats/teamestimatedmetrics`

**Valid URL:** [https://stats.nba.com/stats/teamestimatedmetrics?LeagueID=00](https://stats.nba.com/stats/teamestimatedmetrics?LeagueID=00)

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
| `e_off_rating` | numeric |  |
| `e_def_rating` | numeric |  |
| `e_net_rating` | numeric |  |
| `e_pace` | numeric |  |
| `e_ast_ratio` | numeric |  |
| `e_oreb_pct` | numeric |  |
| `e_dreb_pct` | numeric |  |
| `e_reb_pct` | numeric |  |
| `e_tm_tov_pct` | numeric |  |
| `gp_rank` | integer |  |
| `w_rank` | integer |  |
| `l_rank` | integer |  |
| `w_pct_rank` | integer |  |
| `min_rank` | integer |  |
| `e_off_rating_rank` | integer |  |
| `e_def_rating_rank` | integer |  |
| `e_net_rating_rank` | integer |  |
| `e_ast_ratio_rank` | integer |  |
| `e_oreb_pct_rank` | integer |  |
| `e_dreb_pct_rank` | integer |  |
| `e_reb_pct_rank` | integer |  |
| `e_tm_tov_pct_rank` | integer |  |
| `e_pace_rank` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamestimatedmetrics(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamgamelog`

GET /stats/teamgamelog

**Endpoint URL:** `GET https://stats.nba.com/stats/teamgamelog`

**Valid URL:** [https://stats.nba.com/stats/teamgamelog?LeagueID=00](https://stats.nba.com/stats/teamgamelog?LeagueID=00)

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
| `game_id` | integer | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
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
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `tov` | character | Turnovers. |
| `pf` | character | Personal fouls. |
| `pts` | character | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamgamelog(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamgamelogs`

GET /stats/teamgamelogs

**Endpoint URL:** `GET https://stats.nba.com/stats/teamgamelogs`

**Valid URL:** [https://stats.nba.com/stats/teamgamelogs?LeagueID=00](https://stats.nba.com/stats/teamgamelogs?LeagueID=00)

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
| `tov` | character | Turnovers. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamgamelogs(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamgamestreakfinder`

GET /stats/teamgamestreakfinder

**Endpoint URL:** `GET https://stats.nba.com/stats/teamgamestreakfinder`

**Valid URL:** [https://stats.nba.com/stats/teamgamestreakfinder?LeagueID=00](https://stats.nba.com/stats/teamgamestreakfinder?LeagueID=00)

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
| `gamestreak` | character |  |
| `startdate` | character |  |
| `enddate` | character |  |
| `activestreak` | character |  |
| `numseasons` | character |  |
| `lastseason` | character |  |
| `firstseason` | character |  |
| `abbreviation` | character | Short abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamgamestreakfinder(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamhistoricalleaders`

GET /stats/teamhistoricalleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/teamhistoricalleaders`

**Valid URL:** [https://stats.nba.com/stats/teamhistoricalleaders?LeagueID=00](https://stats.nba.com/stats/teamhistoricalleaders?LeagueID=00)

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
| `pts_person_id` | integer |  |
| `pts_player` | character |  |
| `ast` | character | Assists. |
| `ast_person_id` | integer |  |
| `ast_player` | character |  |
| `reb` | character | Total rebounds. |
| `reb_person_id` | integer |  |
| `reb_player` | character |  |
| `blk` | character | Blocks. |
| `blk_person_id` | integer |  |
| `blk_player` | character |  |
| `stl` | character | Steals. |
| `stl_person_id` | integer |  |
| `stl_player` | character |  |
| `season_year` | character | Season year string ('YYYY-YY' format). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamhistoricalleaders(league_id='00')
```

_Last validated n/a._

## `nba_stats_teaminfocommon`

GET /stats/teaminfocommon

**Endpoint URL:** `GET https://stats.nba.com/stats/teaminfocommon`

**Valid URL:** [https://stats.nba.com/stats/teaminfocommon?LeagueID=00](https://stats.nba.com/stats/teaminfocommon?LeagueID=00)

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
| `season_id` | integer | Unique season identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teaminfocommon(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamplayerdashboard`

GET /stats/teamplayerdashboard

**Endpoint URL:** `GET https://stats.nba.com/stats/teamplayerdashboard`

**Valid URL:** [https://stats.nba.com/stats/teamplayerdashboard?LeagueID=00](https://stats.nba.com/stats/teamplayerdashboard?LeagueID=00)

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
| `group_set` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | character |  |
| `dd2` | character |  |
| `td3` | character |  |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `nba_fantasy_pts_rank` | character |  |
| `dd2_rank` | character |  |
| `td3_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamplayerdashboard(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamplayeronoffdetails`

GET /stats/teamplayeronoffdetails

**Endpoint URL:** `GET https://stats.nba.com/stats/teamplayeronoffdetails`

**Valid URL:** [https://stats.nba.com/stats/teamplayeronoffdetails?LeagueID=00](https://stats.nba.com/stats/teamplayeronoffdetails?LeagueID=00)

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
| `group_set` | character |  |
| `group_value` | character |  |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamplayeronoffdetails(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamplayeronoffsummary`

GET /stats/teamplayeronoffsummary

**Endpoint URL:** `GET https://stats.nba.com/stats/teamplayeronoffsummary`

**Valid URL:** [https://stats.nba.com/stats/teamplayeronoffsummary?LeagueID=00](https://stats.nba.com/stats/teamplayeronoffsummary?LeagueID=00)

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
| `group_set` | character |  |
| `group_value` | character |  |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamplayeronoffsummary(league_id='00')
```

_Last validated n/a._

## `nba_stats_teamvsplayer`

GET /stats/teamvsplayer

**Endpoint URL:** `GET https://stats.nba.com/stats/teamvsplayer`

**Valid URL:** [https://stats.nba.com/stats/teamvsplayer?LeagueID=00](https://stats.nba.com/stats/teamvsplayer?LeagueID=00)

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
| `group_set` | character |  |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `vs_player_id` | integer |  |
| `vs_player_name` | character |  |
| `court_status` | character |  |
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
| `blka` | character |  |
| `pf` | character | Personal fouls. |
| `pfd` | character |  |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character |  |
| `w_rank` | character |  |
| `l_rank` | character |  |
| `w_pct_rank` | numeric |  |
| `min_rank` | character |  |
| `fgm_rank` | character |  |
| `fga_rank` | character |  |
| `fg_pct_rank` | numeric |  |
| `fg3m_rank` | character |  |
| `fg3a_rank` | character |  |
| `fg3_pct_rank` | numeric |  |
| `ftm_rank` | character |  |
| `fta_rank` | character |  |
| `ft_pct_rank` | numeric |  |
| `oreb_rank` | character |  |
| `dreb_rank` | character |  |
| `reb_rank` | character |  |
| `ast_rank` | character |  |
| `tov_rank` | character |  |
| `stl_rank` | character |  |
| `blk_rank` | character |  |
| `blka_rank` | character |  |
| `pf_rank` | character |  |
| `pfd_rank` | character |  |
| `pts_rank` | character |  |
| `plus_minus_rank` | character |  |
| `cfid` | character |  |
| `cfparams` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_teamvsplayer(league_id='00')
```

_Last validated n/a._

## `nba_stats_videodetails`

GET /stats/videodetails

**Endpoint URL:** `GET https://stats.nba.com/stats/videodetails`

**Valid URL:** [https://stats.nba.com/stats/videodetails?LeagueID=00](https://stats.nba.com/stats/videodetails?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ContextMeasure` | `context_measure_detailed` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `StartRange` | `start_range_nullable` |  |  | `Y` |  |
| `StartPeriod` | `start_period_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `RangeType` | `range_type_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `PointDiff` | `point_diff_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `EndRange` | `end_range_nullable` |  |  | `Y` |  |
| `EndPeriod` | `end_period_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ClutchTime` | `clutch_time_nullable` |  |  | `Y` |  |
| `AheadBehind` | `ahead_behind_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_videodetails(league_id='00')
```

_Last validated n/a._

## `nba_stats_videodetailsasset`

GET /stats/videodetailsasset

**Endpoint URL:** `GET https://stats.nba.com/stats/videodetailsasset`

**Valid URL:** [https://stats.nba.com/stats/videodetailsasset?LeagueID=00](https://stats.nba.com/stats/videodetailsasset?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ContextMeasure` | `context_measure_detailed` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `StartRange` | `start_range_nullable` |  |  | `Y` |  |
| `StartPeriod` | `start_period_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `RangeType` | `range_type_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `PointDiff` | `point_diff_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `EndRange` | `end_range_nullable` |  |  | `Y` |  |
| `EndPeriod` | `end_period_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ClutchTime` | `clutch_time_nullable` |  |  | `Y` |  |
| `AheadBehind` | `ahead_behind_nullable` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_videodetailsasset(league_id='00')
```

_Last validated n/a._

## `nba_stats_videoevents`

GET /stats/videoevents

**Endpoint URL:** `GET https://stats.nba.com/stats/videoevents`

**Valid URL:** [https://stats.nba.com/stats/videoevents](https://stats.nba.com/stats/videoevents)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameEventID` | `game_event_id` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_videoevents()
```

_Last validated n/a._

## `nba_stats_videoeventsasset`

GET /stats/videoeventsasset

**Endpoint URL:** `GET https://stats.nba.com/stats/videoeventsasset`

**Valid URL:** [https://stats.nba.com/stats/videoeventsasset](https://stats.nba.com/stats/videoeventsasset)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nba_stats_result_sets`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_videoeventsasset()
```

_Last validated n/a._

## `nba_stats_videostatus`

GET /stats/videostatus

**Endpoint URL:** `GET https://stats.nba.com/stats/videostatus`

**Valid URL:** [https://stats.nba.com/stats/videostatus?LeagueID=00](https://stats.nba.com/stats/videostatus?LeagueID=00)

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
| `visitor_team_city` | character |  |
| `visitor_team_name` | character |  |
| `visitor_team_abbreviation` | character |  |
| `home_team_id` | integer | Unique identifier for the home team. |
| `home_team_city` | character | Home team city / location. |
| `home_team_name` | character | Home team name. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `game_status` | character | Game status label. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `is_available` | character |  |
| `pt_xyz_available` | character | Pt xyz available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_videostatus(league_id='00')
```

_Last validated n/a._

## `nba_stats_winprobabilitypbp`

GET /stats/winprobabilitypbp

**Endpoint URL:** `GET https://stats.nba.com/stats/winprobabilitypbp`

**Valid URL:** [https://stats.nba.com/stats/winprobabilitypbp](https://stats.nba.com/stats/winprobabilitypbp)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |
| `RunType` | `run_type` |  |  | `Y` |  |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `home_team_id` | integer | Unique identifier for the home team. |
| `home_team_abr` | character |  |
| `home_team_pts` | character |  |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `visitor_team_abr` | character |  |
| `visitor_team_pts` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nba_stats_winprobabilitypbp()
```

_Last validated n/a._
