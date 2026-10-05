---
title: "NBA — NBA Stats API (stats.nba.com) — Player dashboards: playerdashboardbyyearoveryear–playerdashptshots"
sidebar_label: "Player dashboards: playerdashboardbyyearoveryear–playerdashptshots"
sidebar_position: 17
description: "NBA — NBA Stats API (stats.nba.com) — Player dashboards: playerdashboardbyyearoveryear–playerdashptshots — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Player dashboards: playerdashboardbyyearoveryear–playerdashptshots

## nba_stats_playerdashboardbyyearoveryear

GET /stats/playerdashboardbyyearoveryear

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashboardbyyearoveryear`

**Valid URL:** [https://stats.nba.com/stats/playerdashboardbyyearoveryear?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=2544&PlusMinus=N&Rank=N&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision=](https://stats.nba.com/stats/playerdashboardbyyearoveryear?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=2544&PlusMinus=N&Rank=N&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision=)

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

### Returns {#nba_stats_playerdashboardbyyearoveryear-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`OverallPlayerDashboard`, `ByYearPlayerDashboard`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**OverallPlayerDashboard**

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
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Rebounds per game. |
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
| `fg3_m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
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

**ByYearPlayerDashboard**

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
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Rebounds per game. |
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
| `fg3_m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playerdashboardbyyearoveryear-example}

```python
nba_stats_playerdashboardbyyearoveryear(league_id='00')
```

_Last validated n/a._

## nba_stats_playerdashptpass

GET /stats/playerdashptpass

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashptpass`

**Valid URL:** [https://stats.nba.com/stats/playerdashptpass?DateFrom=&DateTo=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&PlayerID=2544&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=](https://stats.nba.com/stats/playerdashptpass?DateFrom=&DateTo=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&PlayerID=2544&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=)

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
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November it tips off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, playoff series from May. stats.nba.com answers a request without a season with an empty HTTP 500. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_playerdashptpass-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PassesMade`, `PassesReceived`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PassesMade**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `pass_type` | character | Passing or assist metric for pass type in the requested NBA or WNBA Stats split. |
| `g` | integer | Games played. |
| `pass_to` | character |  |
| `pass_teammate_player_id` | integer | Stats API identifier for pass teammate player identifier associated with this NBA or WNBA Stats row. |
| `frequency` | numeric | NBA or WNBA Stats value for frequency in the playerdashptpass result set. |
| `pass` | numeric | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `ast` | numeric | Assists. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg2_m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2_a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**PassesReceived**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `pass_type` | character | Passing or assist metric for pass type in the requested NBA or WNBA Stats split. |
| `g` | integer | Games played. |
| `pass_from` | character | Passing or assist metric for pass from in the requested NBA or WNBA Stats split. |
| `pass_teammate_player_id` | integer | Stats API identifier for pass teammate player identifier associated with this NBA or WNBA Stats row. |
| `frequency` | numeric | NBA or WNBA Stats value for frequency in the playerdashptpass result set. |
| `pass` | numeric | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `ast` | numeric | Assists. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg2_m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2_a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playerdashptpass-example}

```python
nba_stats_playerdashptpass(league_id='00', season='2024-25')
```

_Last validated n/a._

## nba_stats_playerdashptreb

GET /stats/playerdashptreb

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashptreb`

**Valid URL:** [https://stats.nba.com/stats/playerdashptreb?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&PlayerID=2544&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=](https://stats.nba.com/stats/playerdashptreb?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&PlayerID=2544&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=)

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

### Returns {#nba_stats_playerdashptreb-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`OverallRebounding`, `ShotTypeRebounding`, `NumContestedRebounding`, `ShotDistanceRebounding`, `RebDistanceRebounding`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**OverallRebounding**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `g` | integer | Games played. |
| `overall` | character | Overall pick number. |
| `reb_frequency` | numeric | Rebounding metric for rebounds frequency in the requested NBA or WNBA Stats split. |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Rebounds per game. |
| `c_oreb` | numeric | Rebounding metric for c offensive rebounds in the requested NBA or WNBA Stats split. |
| `c_dreb` | numeric | Rebounding metric for c defensive rebounds in the requested NBA or WNBA Stats split. |
| `c_reb` | numeric | Rebounding metric for c rebounds in the requested NBA or WNBA Stats split. |
| `c_reb_pct` | numeric | Percentage or rate for c rebounds percentage in the requested NBA or WNBA Stats split. |
| `uc_oreb` | numeric | Rebounding metric for uc offensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_dreb` | numeric | Rebounding metric for uc defensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb` | numeric | Rebounding metric for uc rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb_pct` | numeric | Percentage or rate for uc rebounds percentage in the requested NBA or WNBA Stats split. |

**ShotTypeRebounding**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `g` | integer | Games played. |
| `shot_type_range` | character | Shooting metric for shot type range in the requested NBA or WNBA Stats split. |
| `reb_frequency` | numeric | Rebounding metric for rebounds frequency in the requested NBA or WNBA Stats split. |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Rebounds per game. |
| `c_oreb` | numeric | Rebounding metric for c offensive rebounds in the requested NBA or WNBA Stats split. |
| `c_dreb` | numeric | Rebounding metric for c defensive rebounds in the requested NBA or WNBA Stats split. |
| `c_reb` | numeric | Rebounding metric for c rebounds in the requested NBA or WNBA Stats split. |
| `c_reb_pct` | numeric | Percentage or rate for c rebounds percentage in the requested NBA or WNBA Stats split. |
| `uc_oreb` | numeric | Rebounding metric for uc offensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_dreb` | numeric | Rebounding metric for uc defensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb` | numeric | Rebounding metric for uc rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb_pct` | numeric | Percentage or rate for uc rebounds percentage in the requested NBA or WNBA Stats split. |

**NumContestedRebounding**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `g` | integer | Games played. |
| `reb_num_contesting_range` | character |  |
| `reb_frequency` | numeric | Rebounding metric for rebounds frequency in the requested NBA or WNBA Stats split. |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Rebounds per game. |
| `c_oreb` | numeric | Rebounding metric for c offensive rebounds in the requested NBA or WNBA Stats split. |
| `c_dreb` | numeric | Rebounding metric for c defensive rebounds in the requested NBA or WNBA Stats split. |
| `c_reb` | numeric | Rebounding metric for c rebounds in the requested NBA or WNBA Stats split. |
| `c_reb_pct` | numeric | Percentage or rate for c rebounds percentage in the requested NBA or WNBA Stats split. |
| `uc_oreb` | numeric | Rebounding metric for uc offensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_dreb` | numeric | Rebounding metric for uc defensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb` | numeric | Rebounding metric for uc rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb_pct` | numeric | Percentage or rate for uc rebounds percentage in the requested NBA or WNBA Stats split. |

**ShotDistanceRebounding**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `g` | integer | Games played. |
| `shot_dist_range` | character |  |
| `reb_frequency` | numeric | Rebounding metric for rebounds frequency in the requested NBA or WNBA Stats split. |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Rebounds per game. |
| `c_oreb` | numeric | Rebounding metric for c offensive rebounds in the requested NBA or WNBA Stats split. |
| `c_dreb` | numeric | Rebounding metric for c defensive rebounds in the requested NBA or WNBA Stats split. |
| `c_reb` | numeric | Rebounding metric for c rebounds in the requested NBA or WNBA Stats split. |
| `c_reb_pct` | numeric | Percentage or rate for c rebounds percentage in the requested NBA or WNBA Stats split. |
| `uc_oreb` | numeric | Rebounding metric for uc offensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_dreb` | numeric | Rebounding metric for uc defensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb` | numeric | Rebounding metric for uc rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb_pct` | numeric | Percentage or rate for uc rebounds percentage in the requested NBA or WNBA Stats split. |

**RebDistanceRebounding**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `g` | integer | Games played. |
| `reb_dist_range` | character |  |
| `reb_frequency` | numeric | Rebounding metric for rebounds frequency in the requested NBA or WNBA Stats split. |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Rebounds per game. |
| `c_oreb` | numeric | Rebounding metric for c offensive rebounds in the requested NBA or WNBA Stats split. |
| `c_dreb` | numeric | Rebounding metric for c defensive rebounds in the requested NBA or WNBA Stats split. |
| `c_reb` | numeric | Rebounding metric for c rebounds in the requested NBA or WNBA Stats split. |
| `c_reb_pct` | numeric | Percentage or rate for c rebounds percentage in the requested NBA or WNBA Stats split. |
| `uc_oreb` | numeric | Rebounding metric for uc offensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_dreb` | numeric | Rebounding metric for uc defensive rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb` | numeric | Rebounding metric for uc rebounds in the requested NBA or WNBA Stats split. |
| `uc_reb_pct` | numeric | Percentage or rate for uc rebounds percentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playerdashptreb-example}

```python
nba_stats_playerdashptreb(league_id='00')
```

_Last validated n/a._

## nba_stats_playerdashptshotdefend

GET /stats/playerdashptshotdefend

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashptshotdefend`

**Valid URL:** [https://stats.nba.com/stats/playerdashptshotdefend?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&PlayerID=2544&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=](https://stats.nba.com/stats/playerdashptshotdefend?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&PlayerID=2544&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=)

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
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November it tips off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, playoff series from May. stats.nba.com answers a request without a season with an empty HTTP 500. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_playerdashptshotdefend-returns}

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

### Example {#nba_stats_playerdashptshotdefend-example}

```python
nba_stats_playerdashptshotdefend(league_id='00', season='2024-25')
```

_Last validated n/a._

## nba_stats_playerdashptshots

GET /stats/playerdashptshots

**Endpoint URL:** `GET https://stats.nba.com/stats/playerdashptshots`

**Valid URL:** [https://stats.nba.com/stats/playerdashptshots?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&PlayerID=2544&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=](https://stats.nba.com/stats/playerdashptshots?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&PlayerID=2544&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=)

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

### Returns {#nba_stats_playerdashptshots-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `GeneralShooting`, `ShotClockShooting`, `DribbleShooting`, `ClosestDefenderShooting`, `ClosestDefender10ftPlusShooting`, `TouchTimeShooting`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**Overall**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `shot_type` | character | Shot type label (e.g. 'Jump Shot', 'Layup'). |
| `fga_frequency` | numeric | Shooting metric for fga frequency in the requested NBA or WNBA Stats split. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `fg2_a_frequency` | numeric | Shooting metric for fg2a frequency in the requested NBA or WNBA Stats split. |
| `fg2_m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2_a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3_a_frequency` | numeric | Shooting metric for fg3a frequency in the requested NBA or WNBA Stats split. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**GeneralShooting**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `shot_type` | character | Shot type label (e.g. 'Jump Shot', 'Layup'). |
| `fga_frequency` | numeric | Shooting metric for fga frequency in the requested NBA or WNBA Stats split. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `fg2_a_frequency` | numeric | Shooting metric for fg2a frequency in the requested NBA or WNBA Stats split. |
| `fg2_m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2_a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3_a_frequency` | numeric | Shooting metric for fg3a frequency in the requested NBA or WNBA Stats split. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**ShotClockShooting**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `shot_clock_range` | character |  |
| `fga_frequency` | numeric | Shooting metric for fga frequency in the requested NBA or WNBA Stats split. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `fg2_a_frequency` | numeric | Shooting metric for fg2a frequency in the requested NBA or WNBA Stats split. |
| `fg2_m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2_a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3_a_frequency` | numeric | Shooting metric for fg3a frequency in the requested NBA or WNBA Stats split. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**DribbleShooting**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `dribble_range` | character |  |
| `fga_frequency` | numeric | Shooting metric for fga frequency in the requested NBA or WNBA Stats split. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `fg2_a_frequency` | numeric | Shooting metric for fg2a frequency in the requested NBA or WNBA Stats split. |
| `fg2_m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2_a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3_a_frequency` | numeric | Shooting metric for fg3a frequency in the requested NBA or WNBA Stats split. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**ClosestDefenderShooting**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `close_def_dist_range` | character |  |
| `fga_frequency` | numeric | Shooting metric for fga frequency in the requested NBA or WNBA Stats split. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `fg2_a_frequency` | numeric | Shooting metric for fg2a frequency in the requested NBA or WNBA Stats split. |
| `fg2_m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2_a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3_a_frequency` | numeric | Shooting metric for fg3a frequency in the requested NBA or WNBA Stats split. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**ClosestDefender10ftPlusShooting**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `close_def_dist_range` | character |  |
| `fga_frequency` | numeric | Shooting metric for fga frequency in the requested NBA or WNBA Stats split. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `fg2_a_frequency` | numeric | Shooting metric for fg2a frequency in the requested NBA or WNBA Stats split. |
| `fg2_m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2_a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3_a_frequency` | numeric | Shooting metric for fg3a frequency in the requested NBA or WNBA Stats split. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**TouchTimeShooting**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `sort_order` | integer | Display sort order for the sport. |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `touch_time_range` | character | Time value for touch time range in the NBA or WNBA Stats result set. |
| `fga_frequency` | numeric | Shooting metric for fga frequency in the requested NBA or WNBA Stats split. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `efg_pct` | numeric | Percentage or rate for efg percentage in the requested NBA or WNBA Stats split. |
| `fg2_a_frequency` | numeric | Shooting metric for fg2a frequency in the requested NBA or WNBA Stats split. |
| `fg2_m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2_a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3_a_frequency` | numeric | Shooting metric for fg3a frequency in the requested NBA or WNBA Stats split. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playerdashptshots-example}

```python
nba_stats_playerdashptshots(league_id='00')
```

_Last validated n/a._
