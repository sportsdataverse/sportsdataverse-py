---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Team dashboards: teamdashboardbyyearoveryear–teamdashlineups"
sidebar_label: "Team dashboards: teamdashboardbyyearoveryear–teamdashlineups"
sidebar_position: 24
description: "WNBA — WNBA Stats API (stats.wnba.com) — Team dashboards: teamdashboardbyyearoveryear–teamdashlineups — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Team dashboards: teamdashboardbyyearoveryear–teamdashlineups

## wnba_stats_teamdashboardbyyearoveryear

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

### Returns {#wnba_stats_teamdashboardbyyearoveryear-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`OverallTeamDashboard`, `ByYearTeamDashboard`) (one table per key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**OverallTeamDashboard**

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
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
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
| `fg3_m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
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

**ByYearTeamDashboard**

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
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
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
| `fg3_m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
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

### Example {#wnba_stats_teamdashboardbyyearoveryear-example}

```python
wnba_stats_teamdashboardbyyearoveryear(league_id='10')
```

_Last validated n/a._

## wnba_stats_teamdashlineups

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
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2025``. Defaults to the current WNBA season at call time (``2026`` from May 2026); stats.wnba.com answers a request without a season with an empty HTTP 500. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_teamdashlineups-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `Lineups`) (one table per key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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

### Example {#wnba_stats_teamdashlineups-example}

```python
wnba_stats_teamdashlineups(league_id='10')
```

_Last validated n/a._
