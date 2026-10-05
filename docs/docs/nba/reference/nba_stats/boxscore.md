---
title: "NBA — NBA Stats API (stats.nba.com) — Box scores"
sidebar_label: "Box scores"
sidebar_position: 1
description: "NBA — NBA Stats API (stats.nba.com) — Box scores — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Box scores

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
| `reb` | integer | Rebounds per game. |
| `ast` | integer | Assists. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `to` | integer | To. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |

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

### Example {#nba_stats_boxscoretraditionalv3-example}

```python
nba_stats_boxscoretraditionalv3()
```

_Last validated n/a._

## nba_stats_boxscoreusagev3

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

### Returns {#nba_stats_boxscoreusagev3-returns}

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

### Example {#nba_stats_boxscoreusagev3-example}

```python
nba_stats_boxscoreusagev3()
```

_Last validated n/a._
