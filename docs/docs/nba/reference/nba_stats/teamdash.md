---
title: "NBA — NBA Stats API (stats.nba.com) — Team dashboards"
sidebar_label: "Team dashboards"
sidebar_position: 16
description: "NBA — NBA Stats API (stats.nba.com) — Team dashboards — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Team dashboards

## nba_stats_teamdashboardbyclutch

GET /stats/teamdashboardbyclutch

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbyclutch`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbyclutch?LeagueID=00](https://stats.nba.com/stats/teamdashboardbyclutch?LeagueID=00)

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

### Returns {#nba_stats_teamdashboardbyclutch-returns}

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
| `reb` | numeric | Rebounds per game. |
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

### Example {#nba_stats_teamdashboardbyclutch-example}

```python
nba_stats_teamdashboardbyclutch(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashboardbygamesplits

GET /stats/teamdashboardbygamesplits

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbygamesplits`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbygamesplits?LeagueID=00](https://stats.nba.com/stats/teamdashboardbygamesplits?LeagueID=00)

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

### Returns {#nba_stats_teamdashboardbygamesplits-returns}

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
| `reb` | numeric | Rebounds per game. |
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

### Example {#nba_stats_teamdashboardbygamesplits-example}

```python
nba_stats_teamdashboardbygamesplits(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashboardbygeneralsplits

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

### Returns {#nba_stats_teamdashboardbygeneralsplits-returns}

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

### Example {#nba_stats_teamdashboardbygeneralsplits-example}

```python
nba_stats_teamdashboardbygeneralsplits(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashboardbylastngames

GET /stats/teamdashboardbylastngames

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbylastngames`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbylastngames?LeagueID=00](https://stats.nba.com/stats/teamdashboardbylastngames?LeagueID=00)

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

### Returns {#nba_stats_teamdashboardbylastngames-returns}

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
| `reb` | numeric | Rebounds per game. |
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

### Example {#nba_stats_teamdashboardbylastngames-example}

```python
nba_stats_teamdashboardbylastngames(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashboardbyopponent

GET /stats/teamdashboardbyopponent

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbyopponent`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbyopponent?LeagueID=00](https://stats.nba.com/stats/teamdashboardbyopponent?LeagueID=00)

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

### Returns {#nba_stats_teamdashboardbyopponent-returns}

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
| `reb` | numeric | Rebounds per game. |
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

### Example {#nba_stats_teamdashboardbyopponent-example}

```python
nba_stats_teamdashboardbyopponent(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashboardbyshootingsplits

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

### Returns {#nba_stats_teamdashboardbyshootingsplits-returns}

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

### Example {#nba_stats_teamdashboardbyshootingsplits-example}

```python
nba_stats_teamdashboardbyshootingsplits(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashboardbyteamperformance

GET /stats/teamdashboardbyteamperformance

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbyteamperformance`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbyteamperformance?LeagueID=00](https://stats.nba.com/stats/teamdashboardbyteamperformance?LeagueID=00)

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

### Returns {#nba_stats_teamdashboardbyteamperformance-returns}

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
| `reb` | numeric | Rebounds per game. |
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

### Example {#nba_stats_teamdashboardbyteamperformance-example}

```python
nba_stats_teamdashboardbyteamperformance(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashboardbyyearoveryear

GET /stats/teamdashboardbyyearoveryear

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdashboardbyyearoveryear`

**Valid URL:** [https://stats.nba.com/stats/teamdashboardbyyearoveryear?LeagueID=00](https://stats.nba.com/stats/teamdashboardbyyearoveryear?LeagueID=00)

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

### Returns {#nba_stats_teamdashboardbyyearoveryear-returns}

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
| `reb` | numeric | Rebounds per game. |
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

### Example {#nba_stats_teamdashboardbyyearoveryear-example}

```python
nba_stats_teamdashboardbyyearoveryear(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashlineups

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

### Returns {#nba_stats_teamdashlineups-returns}

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

### Example {#nba_stats_teamdashlineups-example}

```python
nba_stats_teamdashlineups(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashptpass

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

### Returns {#nba_stats_teamdashptpass-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `fg2m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
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

### Returns {#nba_stats_teamdashptreb-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_teamdashptreb-example}

```python
nba_stats_teamdashptreb(league_id='00')
```

_Last validated n/a._

## nba_stats_teamdashptshots

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

### Returns {#nba_stats_teamdashptshots-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `fg2a_frequency` | numeric | Shooting metric for fg2a frequency in the requested NBA or WNBA Stats split. |
| `fg2m` | numeric | Shooting metric for fg2m in the requested NBA or WNBA Stats split. |
| `fg2a` | numeric | Shooting metric for fg2a in the requested NBA or WNBA Stats split. |
| `fg2_pct` | numeric | Percentage or rate for two-point field goals percentage in the requested NBA or WNBA Stats split. |
| `fg3a_frequency` | numeric | Shooting metric for fg3a frequency in the requested NBA or WNBA Stats split. |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_teamdashptshots-example}

```python
nba_stats_teamdashptshots(league_id='00')
```

_Last validated n/a._
