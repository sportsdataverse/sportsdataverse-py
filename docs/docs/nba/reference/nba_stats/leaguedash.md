---
title: "NBA — NBA Stats API (stats.nba.com) — League dashboards"
sidebar_label: "League dashboards"
sidebar_position: 7
description: "NBA — NBA Stats API (stats.nba.com) — League dashboards — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — League dashboards

## nba_stats_leaguedashlineups

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

### Returns {#nba_stats_leaguedashlineups-returns}

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
| `sum_time_played` | integer | Time value for sum time played in the NBA or WNBA Stats result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_leaguedashlineups-example}

```python
nba_stats_leaguedashlineups(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashoppptshot

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

### Returns {#nba_stats_leaguedashoppptshot-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
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

### Example {#nba_stats_leaguedashoppptshot-example}

```python
nba_stats_leaguedashoppptshot(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashplayerbiostats

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

### Returns {#nba_stats_leaguedashplayerbiostats-returns}

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
| `college` | character | College. |
| `country` | character | Venue country. |
| `draft_year` | character | Draft year (4-digit). |
| `draft_round` | character | Round of the draft selection. |
| `draft_number` | character | The number pick that was used to select a given player. |
| `gp` | integer | Games played. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Rebounds per game. |
| `ast` | numeric | Assists. |
| `net_rating` | numeric | Net rating (off rating - def rating). |
| `oreb_pct` | numeric | Percentage or rate for offensive rebounds percentage in the requested NBA or WNBA Stats split. |
| `dreb_pct` | numeric | Percentage or rate for defensive rebounds percentage in the requested NBA or WNBA Stats split. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `ast_pct` | numeric | Assist percentage. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_leaguedashplayerbiostats-example}

```python
nba_stats_leaguedashplayerbiostats(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashplayerclutch

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

### Returns {#nba_stats_leaguedashplayerclutch-returns}

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

### Example {#nba_stats_leaguedashplayerclutch-example}

```python
nba_stats_leaguedashplayerclutch(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashplayerptshot

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

### Returns {#nba_stats_leaguedashplayerptshot-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `player_last_team_id` | integer | Stats API identifier for player last team identifier associated with this NBA or WNBA Stats row. |
| `player_last_team_abbreviation` | character | NBA or WNBA Stats value for player last team abbreviation in the leaguedashplayerptshot result set. |
| `age` | numeric | Player age (in years). |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
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

### Example {#nba_stats_leaguedashplayerptshot-example}

```python
nba_stats_leaguedashplayerptshot(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashplayershotlocations

GET /stats/leaguedashplayershotlocations

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashplayershotlocations`

**Valid URL:** [https://stats.nba.com/stats/leaguedashplayershotlocations?LeagueID=00](https://stats.nba.com/stats/leaguedashplayershotlocations?LeagueID=00)

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

### Returns {#nba_stats_leaguedashplayershotlocations-returns}

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

### Example {#nba_stats_leaguedashplayershotlocations-example}

```python
nba_stats_leaguedashplayershotlocations(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashplayerstats

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

### Returns {#nba_stats_leaguedashplayerstats-returns}

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
| `reb` | numeric | Rebounds per game. |
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

### Example {#nba_stats_leaguedashplayerstats-example}

```python
nba_stats_leaguedashplayerstats(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashptdefend

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

### Returns {#nba_stats_leaguedashptdefend-returns}

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

### Example {#nba_stats_leaguedashptdefend-example}

```python
nba_stats_leaguedashptdefend(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashptstats

GET /stats/leaguedashptstats

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashptstats`

**Valid URL:** [https://stats.nba.com/stats/leaguedashptstats?LeagueID=00](https://stats.nba.com/stats/leaguedashptstats?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` | College query parameter. |
| `Conference` | `conference_nullable` |  |  | `Y` | Conference query parameter. |
| `Country` | `country_nullable` |  |  | `Y` | Country query parameter. |
| `DateFrom` | `date_from_nullable` |  |  | `Y` | DateFrom query parameter. |
| `DateTo` | `date_to_nullable` |  |  | `Y` | DateTo query parameter. |
| `Division` | `division_simple_nullable` |  |  | `Y` | Division query parameter. |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` | DraftPick query parameter. |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` | DraftYear query parameter. |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` | GameScope query parameter. |
| `Height` | `height_nullable` |  |  | `Y` | Height query parameter. |
| `LastNGames` | `last_n_games` |  |  | `Y` | LastNGames query parameter. |
| `LeagueID` | `league_id` |  |  | `Y` | LeagueID query parameter. |
| `Location` | `location_nullable` |  |  | `Y` | Location query parameter. |
| `Month` | `month` |  |  | `Y` | Month query parameter. |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` | OpponentTeamID query parameter. |
| `Outcome` | `outcome_nullable` |  |  | `Y` | Outcome query parameter. |
| `PORound` | `po_round_nullable` |  |  | `Y` | PORound query parameter. |
| `PerMode` | `per_mode_simple` |  |  | `Y` | PerMode query parameter. |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` | PlayerExperience query parameter. |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` | PlayerOrTeam query parameter. |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` | PlayerPosition query parameter. |
| `PtMeasureType` | `pt_measure_type` |  |  | `Y` | PtMeasureType query parameter. |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` | SeasonSegment query parameter. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | SeasonType query parameter. |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` | StarterBench query parameter. |
| `TeamID` | `team_id_nullable` |  |  | `Y` | TeamID query parameter. |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` | VsConference query parameter. |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` | VsDivision query parameter. |
| `Weight` | `weight_nullable` |  |  | `Y` | Weight query parameter. |

### Returns {#nba_stats_leaguedashptstats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `min` | numeric | Minutes played. |
| `dist_feet` | numeric | NBA or WNBA Stats value for dist feet in the leaguedashptstats result set. |
| `dist_miles` | numeric | NBA or WNBA Stats value for dist miles in the leaguedashptstats result set. |
| `dist_miles_off` | numeric | NBA or WNBA Stats value for dist miles offensive in the leaguedashptstats result set. |
| `dist_miles_def` | numeric | NBA or WNBA Stats value for dist miles defensive in the leaguedashptstats result set. |
| `avg_speed` | numeric | NBA or WNBA Stats value for average speed in the leaguedashptstats result set. |
| `avg_speed_off` | numeric | NBA or WNBA Stats value for average speed offensive in the leaguedashptstats result set. |
| `avg_speed_def` | numeric | NBA or WNBA Stats value for average speed defensive in the leaguedashptstats result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_leaguedashptstats-example}

```python
nba_stats_leaguedashptstats(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashptteamdefend

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

### Returns {#nba_stats_leaguedashptteamdefend-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
| `freq` | numeric | NBA or WNBA Stats value for freq in the leaguedashptteamdefend result set. |
| `d_fgm` | numeric | Shooting metric for d fgm in the requested NBA or WNBA Stats split. |
| `d_fga` | numeric | Shooting metric for d fga in the requested NBA or WNBA Stats split. |
| `d_fg_pct` | numeric | Percentage or rate for d field goals percentage in the requested NBA or WNBA Stats split. |
| `normal_fg_pct` | numeric | Percentage or rate for normal field goals percentage in the requested NBA or WNBA Stats split. |
| `pct_plusminus` | numeric | Percentage share of plusminus for the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_leaguedashptteamdefend-example}

```python
nba_stats_leaguedashptteamdefend(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashteamclutch

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

### Returns {#nba_stats_leaguedashteamclutch-returns}

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

### Example {#nba_stats_leaguedashteamclutch-example}

```python
nba_stats_leaguedashteamclutch(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashteamptshot

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

### Returns {#nba_stats_leaguedashteamptshot-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `g` | integer | Games played. |
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

### Example {#nba_stats_leaguedashteamptshot-example}

```python
nba_stats_leaguedashteamptshot(league_id='00')
```

_Last validated n/a._

## nba_stats_leaguedashteamshotlocations

GET /stats/leaguedashteamshotlocations

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashteamshotlocations`

**Valid URL:** [https://stats.nba.com/stats/leaguedashteamshotlocations?LeagueID=00](https://stats.nba.com/stats/leaguedashteamshotlocations?LeagueID=00)

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

### Returns {#nba_stats_leaguedashteamshotlocations-returns}

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

### Example {#nba_stats_leaguedashteamshotlocations-example}

```python
nba_stats_leaguedashteamshotlocations(league_id='00')
```

_Last validated n/a._
