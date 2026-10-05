---
title: "NBA — NBA Stats API (stats.nba.com) — Team dashboards: teamdashlineups–teamdashptshots"
sidebar_label: "Team dashboards: teamdashlineups–teamdashptshots"
sidebar_position: 27
description: "NBA — NBA Stats API (stats.nba.com) — Team dashboards: teamdashlineups–teamdashptshots — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Team dashboards: teamdashlineups–teamdashptshots

## nba_stats_teamdashlineups

GET /stats/teamdashlineups

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashlineups`

**Valid URL:** [https://stats.nba.com/stats/teamdashlineups?DateFrom=&DateTo=&GameID=&GameSegment=&GroupQuantity=5&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=1611661328&VsConference=&VsDivision=](https://stats.nba.com/stats/teamdashlineups?DateFrom=&DateTo=&GameID=&GameSegment=&GroupQuantity=5&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=1611661328&VsConference=&VsDivision=)

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

### Returns {#nba_stats_teamdashlineups-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `Lineups`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**Overall**

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

**Lineups**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the grouping family used for this dashboard or split row. |
| `group_id` | character | ESPN group id. |
| `group_name` | character | Group name (conference / division). |
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
| `sum_time_played` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_teamdashlineups-example}

```python
nba_stats_teamdashlineups(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashptpass

GET /stats/teamdashptpass

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashptpass`

**Valid URL:** [https://stats.nba.com/stats/teamdashptpass?DateFrom=&DateTo=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&SeasonSegment=&SeasonType=Regular+Season&TeamID=1610612749&VsConference=&VsDivision=](https://stats.nba.com/stats/teamdashptpass?DateFrom=&DateTo=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&SeasonSegment=&SeasonType=Regular+Season&TeamID=1610612749&VsConference=&VsDivision=)

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

### Returns {#nba_stats_teamdashptpass-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PassesMade`, `PassesReceived`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PassesMade**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pass_type` | character | Passing or assist metric for pass type in the requested NBA or WNBA Stats split. |
| `g` | integer | Games played. |
| `pass_from` | character |  |
| `pass_teammate_player_id` | integer | Stats API identifier for pass teammate player identifier associated with this NBA or WNBA Stats row. |
| `frequency` | numeric | NBA or WNBA Stats value for frequency in the teamdashptpass result set. |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pass_type` | character | Passing or assist metric for pass type in the requested NBA or WNBA Stats split. |
| `g` | integer | Games played. |
| `pass_to` | character | Passing or assist metric for pass to in the requested NBA or WNBA Stats split. |
| `pass_teammate_player_id` | integer | Stats API identifier for pass teammate player identifier associated with this NBA or WNBA Stats row. |
| `frequency` | numeric | NBA or WNBA Stats value for frequency in the teamdashptpass result set. |
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

### Example {#nba_stats_teamdashptpass-example}

```python
nba_stats_teamdashptpass(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashptreb

GET /stats/teamdashptreb

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashptreb`

**Valid URL:** [https://stats.nba.com/stats/teamdashptreb?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&SeasonSegment=&SeasonType=Regular+Season&TeamID=1610612749&VsConference=&VsDivision=](https://stats.nba.com/stats/teamdashptreb?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&SeasonSegment=&SeasonType=Regular+Season&TeamID=1610612749&VsConference=&VsDivision=)

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

### Returns {#nba_stats_teamdashptreb-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`OverallRebounding`, `ShotTypeRebounding`, `NumContestedRebounding`, `ShotDistanceRebounding`, `RebDistanceRebounding`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**OverallRebounding**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
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

### Example {#nba_stats_teamdashptreb-example}

```python
nba_stats_teamdashptreb(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashptshots

GET /stats/teamdashptshots

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashptshots`

**Valid URL:** [https://stats.nba.com/stats/teamdashptshots?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&SeasonSegment=&SeasonType=Regular+Season&TeamID=1610612749&VsConference=&VsDivision=](https://stats.nba.com/stats/teamdashptshots?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&PerMode=Totals&Period=0&SeasonSegment=&SeasonType=Regular+Season&TeamID=1610612749&VsConference=&VsDivision=)

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

### Returns {#nba_stats_teamdashptshots-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GeneralShooting`, `ShotClockShooting`, `DribbleShooting`, `ClosestDefenderShooting`, `ClosestDefender10ftPlusShooting`, `TouchTimeShooting`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GeneralShooting**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `sort_order` | integer | Display sort order for the sport. |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `sort_order` | integer | Display sort order for the sport. |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `sort_order` | integer | Display sort order for the sport. |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `sort_order` | integer | Display sort order for the sport. |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `sort_order` | integer | Display sort order for the sport. |
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
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `sort_order` | integer | Display sort order for the sport. |
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

### Example {#nba_stats_teamdashptshots-example}

```python
nba_stats_teamdashptshots(league_id='00')
```

_Last validated n/a._
