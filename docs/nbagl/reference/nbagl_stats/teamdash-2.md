# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Team dashboards: teamdashboardbygeneralsplits–teamdashboardbylastngames

> NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Team dashboards: teamdashboardbygeneralsplits–teamdashboardbylastngames — function reference in sdv-py, the SportsDataverse Python package.

## nbagl_stats_teamdashboardbygeneralsplits

Team dashboard by general splits.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbygeneralsplits`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbygeneralsplits?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=1612709914&VsConference=&VsDivision=](https://stats.nba.com/stats/teamdashboardbygeneralsplits?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=1612709914&VsConference=&VsDivision=)

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
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamdashboardbygeneralsplits-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`OverallTeamDashboard`, `LocationTeamDashboard`, `WinsLossesTeamDashboard`, `MonthTeamDashboard`, `PrePostAllStarTeamDashboard`, `DaysRestTeamDashboard`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**OverallTeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `group_value` | String | Specific grouping value for this dashboard or split row. |
| `season_year` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**LocationTeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `group_value` | String | Specific grouping value for this dashboard or split row. |
| `team_game_location` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**WinsLossesTeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `group_value` | String | Specific grouping value for this dashboard or split row. |
| `game_result` | String | Split label of the win/loss rows: 'Wins' or 'Losses'. |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**MonthTeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `group_value` | String | Specific grouping value for this dashboard or split row. |
| `season_month_name` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**PrePostAllStarTeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `group_value` | String | Specific grouping value for this dashboard or split row. |
| `season_segment` | String | Season segment filter ('Pre All-Star' / 'Post All-Star'). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**DaysRestTeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `group_value` | String | Specific grouping value for this dashboard or split row. |
| `team_days_rest_range` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamdashboardbygeneralsplits-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamdashboardbygeneralsplits-example}

```python
nbagl_stats_teamdashboardbygeneralsplits(league_id='20', season='2024-25')
```

## nbagl_stats_teamdashboardbylastngames

Team dashboard by last n games.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbylastngames`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbylastngames?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=1612709914&VsConference=&VsDivision=](https://stats.nba.com/stats/teamdashboardbylastngames?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=1612709914&VsConference=&VsDivision=)

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
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `ShotClockRange` | `shot_clock_range` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference` |  |  | `Y` |  |
| `VsDivision` | `vs_division` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamdashboardbylastngames-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`OverallTeamDashboard`, `Last5TeamDashboard`, `Last10TeamDashboard`, `Last15TeamDashboard`, `Last20TeamDashboard`, `GameNumberTeamDashboard`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**OverallTeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | Int64 | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | Int64 | League rank of the row's free throws made for the season and split. |
| `fta_rank` | Int64 | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | Int64 | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | Int64 | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | Int64 | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | Int64 | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | Int64 | League rank of the row's assists for the season and split. |
| `tov_rank` | Int64 | League rank of the row's turnovers for the season and split. |
| `stl_rank` | Int64 | League rank of the row's steals for the season and split. |
| `blk_rank` | Int64 | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | Int64 | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | Int64 | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | Int64 | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | Int64 | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | Int64 | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**Last5TeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | Int64 | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | Int64 | League rank of the row's free throws made for the season and split. |
| `fta_rank` | Int64 | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | Int64 | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | Int64 | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | Int64 | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | Int64 | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | Int64 | League rank of the row's assists for the season and split. |
| `tov_rank` | Int64 | League rank of the row's turnovers for the season and split. |
| `stl_rank` | Int64 | League rank of the row's steals for the season and split. |
| `blk_rank` | Int64 | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | Int64 | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | Int64 | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | Int64 | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | Int64 | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | Int64 | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**Last10TeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | Int64 | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | Int64 | League rank of the row's free throws made for the season and split. |
| `fta_rank` | Int64 | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | Int64 | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | Int64 | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | Int64 | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | Int64 | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | Int64 | League rank of the row's assists for the season and split. |
| `tov_rank` | Int64 | League rank of the row's turnovers for the season and split. |
| `stl_rank` | Int64 | League rank of the row's steals for the season and split. |
| `blk_rank` | Int64 | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | Int64 | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | Int64 | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | Int64 | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | Int64 | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | Int64 | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**Last15TeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | Int64 | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | Int64 | League rank of the row's free throws made for the season and split. |
| `fta_rank` | Int64 | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | Int64 | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | Int64 | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | Int64 | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | Int64 | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | Int64 | League rank of the row's assists for the season and split. |
| `tov_rank` | Int64 | League rank of the row's turnovers for the season and split. |
| `stl_rank` | Int64 | League rank of the row's steals for the season and split. |
| `blk_rank` | Int64 | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | Int64 | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | Int64 | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | Int64 | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | Int64 | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | Int64 | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**Last20TeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | Int64 | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | Int64 | League rank of the row's free throws made for the season and split. |
| `fta_rank` | Int64 | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | Int64 | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | Int64 | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | Int64 | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | Int64 | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | Int64 | League rank of the row's assists for the season and split. |
| `tov_rank` | Int64 | League rank of the row's turnovers for the season and split. |
| `stl_rank` | Int64 | League rank of the row's steals for the season and split. |
| `blk_rank` | Int64 | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | Int64 | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | Int64 | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | Int64 | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | Int64 | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | Int64 | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**GameNumberTeamDashboard**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | Int64 | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | Int64 | League rank of the row's free throws made for the season and split. |
| `fta_rank` | Int64 | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | Int64 | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | Int64 | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | Int64 | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | Int64 | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | Int64 | League rank of the row's assists for the season and split. |
| `tov_rank` | Int64 | League rank of the row's turnovers for the season and split. |
| `stl_rank` | Int64 | League rank of the row's steals for the season and split. |
| `blk_rank` | Int64 | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | Int64 | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | Int64 | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | Int64 | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | Int64 | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | Int64 | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamdashboardbylastngames-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamdashboardbylastngames-example}

```python
nbagl_stats_teamdashboardbylastngames(league_id='20', season='2024-25')
```
