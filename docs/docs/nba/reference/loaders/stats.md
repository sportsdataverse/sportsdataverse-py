---
title: "NBA dataset loaders — Stats"
sidebar_label: "Stats"
sidebar_position: 2
description: "NBA dataset loaders — Stats — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA dataset loaders — Stats

## load_nba_stats_schedules

Release: [nba_stats_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_schedules/nba_schedule_{season + 1}.parquet`
### Returns {#load_nba_stats_schedules-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `season` | Int64 | Season year. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `home_team_abbreviation` | String | Home team abbreviation; `team_detail = TRUE` only. |
| `home_team_name` | String | Home team name. |
| `home_pts` | Int64 | Final points scored by the home team. |
| `home_wl` | String | Home team's result for the game (W or L). |
| `away_team_id` | Int64 | Unique identifier for the away team. |
| `away_team_abbreviation` | String | Away team abbreviation; `team_detail = TRUE` only. |
| `away_team_name` | String | Away team name. |
| `away_pts` | Int64 | Final points scored by the away team. |
| `away_wl` | String | Away team's result for the game (W or L). |

```python
load_nba_stats_schedules(seasons=2025)
```

## load_nba_stats_coaches

Release: [nba_stats_coaches](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_coaches) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_coaches/coaches_{season + 1}.parquet`
### Returns {#load_nba_stats_coaches-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `season` | Int32 | Season year. |
| `coach_id` | Int64 | ESPN coach id. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `coach_name` | String | Coach's full name. |
| `is_assistant` | Int64 | Numeric flag from the NBA Stats API distinguishing assistants from head coaches. |
| `coach_type` | String | Coach role description (e.g. "Head Coach", "Assistant Coach"). |
| `sort_sequence` | Int64 | Sort order of the coach within the team's staff listing. |
| `sub_sort_sequence` | Int64 | Secondary sort order within the coach type. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

```python
load_nba_stats_coaches(seasons=2025)
```

## load_nba_stats_game_rosters

Release: [nba_stats_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_game_rosters/game_rosters_{season + 1}.parquet`
### Returns {#load_nba_stats_game_rosters-returns}

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_num` | String | Jersey number worn by the player. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `season` | Int32 | Season year. |
| `game_id` | String | Unique game identifier. |
| `season_type_id` | String | Season-type digit: the 3rd character of game_id (and the leading digit of season_id). 1 = preseason, 2 = regular season, 3 = All-Star, 4 = playoffs, 5 = play-in, 6 = NBA Cup final, 9 = international. |

```python
load_nba_stats_game_rosters(seasons=2025)
```

## load_nba_stats_lineups

Release: [nba_stats_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_lineups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_lineups/lineups_{season + 1}.parquet`
### Returns {#load_nba_stats_lineups-returns}

| col_name | type | description |
|---|---|---|
| `group_set` | String | Lineup grouping label from the NBA Stats API (e.g. "Lineups"). |
| `group_id` | String | ESPN group id. |
| `group_name` | String | Group name (conference / division). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `e_off_rating` | Float64 | Estimated offensive rating (NBA Stats estimated-metrics family) over the split. |
| `off_rating` | Float64 | Offensive rating (points scored per 100 possessions) over the split. |
| `e_def_rating` | Float64 | Estimated defensive rating (NBA Stats estimated-metrics family) over the split. |
| `def_rating` | Float64 | Defensive rating (points allowed per 100 possessions) over the split. |
| `e_net_rating` | Float64 | Estimated net rating (NBA Stats estimated-metrics family) over the split. |
| `net_rating` | Float64 | Net rating (off rating - def rating). |
| `ast_pct` | Float64 | Assist percentage. |
| `ast_to` | Float64 | Assist-to-turnover ratio over the split. |
| `ast_ratio` | Float64 | Assist ratio (assists per 100 possessions used) over the split. |
| `oreb_pct` | Float64 | Offensive rebound percentage over the split, as a decimal. |
| `dreb_pct` | Float64 | Defensive rebound percentage over the split, as a decimal. |
| `reb_pct` | Float64 | Total rebound percentage over the split, as a decimal. |
| `tm_tov_pct` | Float64 | Team turnover percentage (turnovers per 100 possessions) over the split, as a decimal. |
| `efg_pct` | Float64 | Effective field goal percentage over the split, as a decimal. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `e_pace` | Float64 | Estimated pace (NBA Stats estimated-metrics family) over the split. |
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace per40. |
| `poss` | Int64 | Poss. |
| `pie` | Float64 | Player Impact Estimate (0-1). |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `off_rating_rank` | Int64 | League rank of the row's offensive rating (points scored per 100 possessions) for the season and split. |
| `def_rating_rank` | Int64 | League rank of the row's defensive rating (points allowed per 100 possessions) for the season and split. |
| `net_rating_rank` | Int64 | League rank of the row's net rating (offensive minus defensive rating) for the season and split. |
| `ast_pct_rank` | Int64 | League rank of the row's assist percentage (share of teammate field goals assisted while on the floor) for the season and split. |
| `ast_to_rank` | Int64 | League rank of the row's assist-to-turnover ratio for the season and split. |
| `ast_ratio_rank` | Int64 | League rank of the row's assist ratio (assists per 100 possessions used) for the season and split. |
| `oreb_pct_rank` | Int64 | League rank of the row's offensive rebound percentage for the season and split. |
| `dreb_pct_rank` | Int64 | League rank of the row's defensive rebound percentage for the season and split. |
| `reb_pct_rank` | Int64 | League rank of the row's total rebound percentage for the season and split. |
| `tm_tov_pct_rank` | Int64 | League rank of the row's team turnover percentage (turnovers per 100 possessions) for the season and split. |
| `efg_pct_rank` | Int64 | League rank of the row's effective field goal percentage for the season and split. |
| `ts_pct_rank` | Int64 | League rank of the row's true shooting percentage for the season and split. |
| `pace_rank` | Int64 | League rank of the row's pace (possessions per 48 minutes) for the season and split. |
| `pie_rank` | Int64 | League rank of the row's Player Impact Estimate (PIE, the NBA Stats catch-all impact metric) for the season and split. |
| `sum_time_played` | Int64 | Total time the five-man lineup was on the floor across the split. |
| `season` | Int32 | Season year. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `measure_type` | String | NBA Stats measure type the row was pulled from (e.g. Base, Advanced, Misc, Scoring, Opponent, Usage, Defense). |
| `per_mode` | String | NBA Stats per-mode of the row values (e.g. Totals, PerGame, Per100Possessions). |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3m` | Float64 | Three-point field goals made. |
| `fg3a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Rebounds per game. |
| `ast` | Float64 | Assists. |
| `tov` | Float64 | Turnovers. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 | Personal fouls. |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 | Points scored. |
| `plus_minus` | Float64 | Plus/minus point differential while on court. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `fg3m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
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
| `pts_off_tov` | Float64 | Points scored off opponent turnovers over the split. |
| `pts_2nd_chance` | Float64 | Second-chance points over the split. |
| `pts_fb` | Float64 | Fast-break points over the split. |
| `pts_paint` | Float64 | Points in the paint over the split. |
| `opp_pts_off_tov` | Float64 | Opponent points scored off opponent turnovers allowed over the split. |
| `opp_pts_2nd_chance` | Float64 | Opponent second-chance points allowed over the split. |
| `opp_pts_fb` | Float64 | Opponent fast-break points allowed over the split. |
| `opp_pts_paint` | Float64 | Opponent points in the paint allowed over the split. |
| `pts_off_tov_rank` | Int64 | League rank of the row's points scored off opponent turnovers for the season and split. |
| `pts_2nd_chance_rank` | Int64 | League rank of the row's second-chance points for the season and split. |
| `pts_fb_rank` | Int64 | League rank of the row's fast-break points for the season and split. |
| `pts_paint_rank` | Int64 | League rank of the row's points in the paint for the season and split. |
| `opp_pts_off_tov_rank` | Int64 | League rank of the row's opponent points scored off opponent turnovers for the season and split. |
| `opp_pts_2nd_chance_rank` | Int64 | League rank of the row's opponent second-chance points for the season and split. |
| `opp_pts_fb_rank` | Int64 | League rank of the row's opponent fast-break points for the season and split. |
| `opp_pts_paint_rank` | Int64 | League rank of the row's opponent points in the paint for the season and split. |
| `opp_fgm` | Float64 | Opponent field goals made allowed over the split. |
| `opp_fga` | Float64 | Opponent field goals attempted allowed over the split. |
| `opp_fg_pct` | Float64 | Opponent field goal percentage allowed over the split. |
| `opp_fg3m` | Float64 | Opponent three-point field goals made allowed over the split. |
| `opp_fg3a` | Float64 | Opponent three-point field goals attempted allowed over the split. |
| `opp_fg3_pct` | Float64 | Opponent three-point field goal percentage allowed over the split. |
| `opp_ftm` | Float64 | Opponent free throws made allowed over the split. |
| `opp_fta` | Float64 | Opponent free throws attempted allowed over the split. |
| `opp_ft_pct` | Float64 | Opponent free throw percentage allowed over the split. |
| `opp_oreb` | Float64 | Opponent offensive rebounds allowed over the split. |
| `opp_dreb` | Float64 | Opponent defensive rebounds allowed over the split. |
| `opp_reb` | Float64 | Opponent total rebounds allowed over the split. |
| `opp_ast` | Float64 | Opponent assists allowed over the split. |
| `opp_tov` | Float64 | Opponent turnovers allowed over the split. |
| `opp_stl` | Float64 | Opponent steals allowed over the split. |
| `opp_blk` | Float64 | Opponent blocked shots allowed over the split. |
| `opp_blka` | Float64 | Opponent shot attempts blocked by opponents (blocks against) allowed over the split. |
| `opp_pf` | Float64 | Opponent personal fouls committed allowed over the split. |
| `opp_pfd` | Float64 | Opponent personal fouls drawn allowed over the split. |
| `opp_pts` | Float64 | Opponent points. |
| `opp_fgm_rank` | Int64 | League rank of the row's opponent field goals made for the season and split. |
| `opp_fga_rank` | Int64 | League rank of the row's opponent field goals attempted for the season and split. |
| `opp_fg_pct_rank` | Int64 | League rank of the row's opponent field goal percentage for the season and split. |
| `opp_fg3m_rank` | Int64 | League rank of the row's opponent three-point field goals made for the season and split. |
| `opp_fg3a_rank` | Int64 | League rank of the row's opponent three-point field goals attempted for the season and split. |
| `opp_fg3_pct_rank` | Int64 | League rank of the row's opponent three-point field goal percentage for the season and split. |
| `opp_ftm_rank` | Int64 | League rank of the row's opponent free throws made for the season and split. |
| `opp_fta_rank` | Int64 | League rank of the row's opponent free throws attempted for the season and split. |
| `opp_ft_pct_rank` | Int64 | League rank of the row's opponent free throw percentage for the season and split. |
| `opp_oreb_rank` | Int64 | League rank of the row's opponent offensive rebounds for the season and split. |
| `opp_dreb_rank` | Int64 | League rank of the row's opponent defensive rebounds for the season and split. |
| `opp_reb_rank` | Int64 | League rank of the row's opponent total rebounds for the season and split. |
| `opp_ast_rank` | Int64 | League rank of the row's opponent assists for the season and split. |
| `opp_tov_rank` | Int64 | League rank of the row's opponent turnovers for the season and split. |
| `opp_stl_rank` | Int64 | League rank of the row's opponent steals for the season and split. |
| `opp_blk_rank` | Int64 | League rank of the row's opponent blocked shots for the season and split. |
| `opp_blka_rank` | Int64 | League rank of the row's opponent shot attempts blocked by opponents (blocks against) for the season and split. |
| `opp_pf_rank` | Int64 | League rank of the row's opponent personal fouls committed for the season and split. |
| `opp_pfd_rank` | Int64 | League rank of the row's opponent personal fouls drawn for the season and split. |
| `opp_pts_rank` | Int64 | League rank of the row's opponent points scored for the season and split. |
| `pct_fga_2pt` | Float64 | Share of field goal attempts taken as two-pointers, as a decimal. |
| `pct_fga_3pt` | Float64 | Share of field goal attempts taken as three-pointers, as a decimal. |
| `pct_pts_2pt` | Float64 | Share of points scored on two-point field goals, as a decimal. |
| `pct_pts_2pt_mr` | Float64 | Share of points scored on mid-range two-pointers, as a decimal. |
| `pct_pts_3pt` | Float64 | Share of points scored on three-pointers, as a decimal. |
| `pct_pts_fb` | Float64 | Share of points scored on fast breaks, as a decimal. |
| `pct_pts_ft` | Float64 | Share of points scored at the free throw line, as a decimal. |
| `pct_pts_off_tov` | Float64 | Share of points scored off opponent turnovers, as a decimal. |
| `pct_pts_paint` | Float64 | Share of points scored in the paint, as a decimal. |
| `pct_ast_2pm` | Float64 | Percentage of made two-pointers that were assisted, as a decimal. |
| `pct_uast_2pm` | Float64 | Percentage of made two-pointers that were unassisted, as a decimal. |
| `pct_ast_3pm` | Float64 | Percentage of made three-pointers that were assisted, as a decimal. |
| `pct_uast_3pm` | Float64 | Percentage of made three-pointers that were unassisted, as a decimal. |
| `pct_ast_fgm` | Float64 | Percentage of made field goals that were assisted, as a decimal. |
| `pct_uast_fgm` | Float64 | Percentage of made field goals that were unassisted, as a decimal. |
| `pct_fga_2pt_rank` | Int64 | League rank of the row's share of field goal attempts taken as two-pointers for the season and split. |
| `pct_fga_3pt_rank` | Int64 | League rank of the row's share of field goal attempts taken as three-pointers for the season and split. |
| `pct_pts_2pt_rank` | Int64 | League rank of the row's share of points scored on two-point field goals for the season and split. |
| `pct_pts_2pt_mr_rank` | Int64 | League rank of the row's share of points scored on mid-range two-pointers for the season and split. |
| `pct_pts_3pt_rank` | Int64 | League rank of the row's share of points scored on three-pointers for the season and split. |
| `pct_pts_fb_rank` | Int64 | League rank of the row's share of points scored on fast breaks for the season and split. |
| `pct_pts_ft_rank` | Int64 | League rank of the row's share of points scored at the free throw line for the season and split. |
| `pct_pts_off_tov_rank` | Int64 | League rank of the row's share of points scored off opponent turnovers for the season and split. |
| `pct_pts_paint_rank` | Int64 | League rank of the row's share of points scored in the paint for the season and split. |
| `pct_ast_2pm_rank` | Int64 | League rank of the row's percentage of made two-pointers that were assisted for the season and split. |
| `pct_uast_2pm_rank` | Int64 | League rank of the row's percentage of made two-pointers that were unassisted for the season and split. |
| `pct_ast_3pm_rank` | Int64 | League rank of the row's percentage of made three-pointers that were assisted for the season and split. |
| `pct_uast_3pm_rank` | Int64 | League rank of the row's percentage of made three-pointers that were unassisted for the season and split. |
| `pct_ast_fgm_rank` | Int64 | League rank of the row's percentage of made field goals that were assisted for the season and split. |
| `pct_uast_fgm_rank` | Int64 | League rank of the row's percentage of made field goals that were unassisted for the season and split. |

```python
load_nba_stats_lineups(seasons=2025)
```

## load_nba_stats_lineups_v3

Release: [nba_stats_game_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_game_lineups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_game_lineups/nba_lineups_{season + 1}.parquet`
### Returns {#load_nba_stats_lineups_v3-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `home_player_1` | Int64 | NBA Stats player id of the home on-court player 1 of 5 for the row. |
| `home_player_2` | Int64 | NBA Stats player id of the home on-court player 2 of 5 for the row. |
| `home_player_3` | Int64 | NBA Stats player id of the home on-court player 3 of 5 for the row. |
| `home_player_4` | Int64 | NBA Stats player id of the home on-court player 4 of 5 for the row. |
| `home_player_5` | Int64 | NBA Stats player id of the home on-court player 5 of 5 for the row. |
| `away_player_1` | Int64 | NBA Stats player id of the away on-court player 1 of 5 for the row. |
| `away_player_2` | Int64 | NBA Stats player id of the away on-court player 2 of 5 for the row. |
| `away_player_3` | Int64 | NBA Stats player id of the away on-court player 3 of 5 for the row. |
| `away_player_4` | Int64 | NBA Stats player id of the away on-court player 4 of 5 for the row. |
| `away_player_5` | Int64 | NBA Stats player id of the away on-court player 5 of 5 for the row. |
| `season` | Int64 | Season year. |

```python
load_nba_stats_lineups_v3(seasons=2025)
```

## load_nba_stats_officials

Release: [nba_stats_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_officials) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_officials/officials_{season + 1}.parquet`
### Returns {#load_nba_stats_officials-returns}

| col_name | type | description |
|---|---|---|
| `official_id` | Int64 | Unique official / referee identifier. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey_num` | String | Jersey number worn by the player. |
| `season` | Int32 | Season year. |
| `game_id` | String | Unique game identifier. |
| `season_type_id` | String | Season-type digit: the 3rd character of game_id (and the leading digit of season_id). 1 = preseason, 2 = regular season, 3 = All-Star, 4 = playoffs, 5 = play-in, 6 = NBA Cup final, 9 = international. |

```python
load_nba_stats_officials(seasons=2025)
```

## load_nba_stats_pbp

Release: [nba_stats_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_pbp/nba_play_by_play_{season + 1}.parquet`
### Returns {#load_nba_stats_pbp-returns}

| col_name | type | description |
|---|---|---|
| `order_index` | Int64 | Stable within-game ordering index for events after pbpstats-style reordering of the raw feed. |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `clock` | String | Game clock value. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | Int64 | Unique team identifier. |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `player_name_i` | String | Player name i. |
| `x_legacy` | Int64 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int64 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `shot_distance` | Int64 | Shot distance from the basket, in feet. |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `is_field_goal` | Int64 | 1 if the action was a field goal; 0 otherwise. |
| `score_home` | String | Score home. |
| `score_away` | String | Score away. |
| `points_total` | Int64 | Running total of points scored. |
| `location` | String | Location. |
| `description` | String | Long-form description text. |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `video_available` | Int64 | Video available. |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `action_id` | Int64 | Unique action identifier within a game (V3 PBP). |
| `game_id` | String | Unique game identifier. |
| `seconds_remaining` | Float64 | Seconds remaining in the period. |
| `event_type` | String | Event / play type code (V2 PBP). |
| `is_made_shot` | Boolean | Whether the event is a made field goal. |
| `is_missed_shot` | Boolean | Whether the event is a missed field goal. |
| `is_free_throw` | Boolean | Whether the event is a free throw attempt. |
| `is_rebound` | Boolean | Whether the event is a rebound. |
| `is_turnover` | Boolean | `TRUE` if the play was a turnover. |
| `is_foul` | Boolean | Whether the event is a foul. |
| `is_substitution` | Boolean | Whether the event is a substitution. |
| `is_jump_ball` | Boolean | Whether the event is a jump ball. |
| `is_timeout` | Boolean | Whether the event is a timeout. |
| `is_period` | Boolean | Whether the event is a period start or end marker. |
| `possession_number` | Int64 | Possession number. |
| `off_player_1` | Int64 | NBA Stats player id of offensive on-court player 1 of 5 during the event. |
| `off_player_2` | Int64 | NBA Stats player id of offensive on-court player 2 of 5 during the event. |
| `off_player_3` | Int64 | NBA Stats player id of offensive on-court player 3 of 5 during the event. |
| `off_player_4` | Int64 | NBA Stats player id of offensive on-court player 4 of 5 during the event. |
| `off_player_5` | Int64 | NBA Stats player id of offensive on-court player 5 of 5 during the event. |
| `def_player_1` | Int64 | NBA Stats player id of defensive on-court player 1 of 5 during the event. |
| `def_player_2` | Int64 | NBA Stats player id of defensive on-court player 2 of 5 during the event. |
| `def_player_3` | Int64 | NBA Stats player id of defensive on-court player 3 of 5 during the event. |
| `def_player_4` | Int64 | NBA Stats player id of defensive on-court player 4 of 5 during the event. |
| `def_player_5` | Int64 | NBA Stats player id of defensive on-court player 5 of 5 during the event. |
| `season` | Int64 | Season year. |

```python
load_nba_stats_pbp(seasons=2025)
```

## load_nba_stats_possessions

Release: [nba_stats_possessions](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_possessions) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_possessions/nba_possessions_{season + 1}.parquet`
### Returns {#load_nba_stats_possessions-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 | NBA Stats team id of the defending team for the possession. |
| `start_order_index` | Int64 | order_index of the play-by-play event that starts the possession. |
| `end_order_index` | Int64 | order_index of the play-by-play event that ends the possession. |
| `start_seconds_remaining` | Float64 | Seconds remaining in the period when the possession started. |
| `end_seconds_remaining` | Float64 | Seconds remaining in the period when the possession ended. |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean | Whether the row is a second-chance continuation following an offensive rebound. |
| `number_in_period` | Int64 | Sequential possession number for the offense within the period. |
| `possession_start_type` | String | How the possession began (e.g. off a made shot, defensive rebound, turnover, or period start). |
| `count_as_possession` | Boolean | Whether the row counts as a true possession for per-possession rate stats. |
| `fg2a` | Int64 | Two-point field goal attempts during the possession. |
| `fg2m` | Int64 | Two-point field goals made during the possession. |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `off_player_1` | Int64 | NBA Stats player id of offensive on-court player 1 of 5 for the possession. |
| `off_player_2` | Int64 | NBA Stats player id of offensive on-court player 2 of 5 for the possession. |
| `off_player_3` | Int64 | NBA Stats player id of offensive on-court player 3 of 5 for the possession. |
| `off_player_4` | Int64 | NBA Stats player id of offensive on-court player 4 of 5 for the possession. |
| `off_player_5` | Int64 | NBA Stats player id of offensive on-court player 5 of 5 for the possession. |
| `def_player_1` | Int64 | NBA Stats player id of defensive on-court player 1 of 5 for the possession. |
| `def_player_2` | Int64 | NBA Stats player id of defensive on-court player 2 of 5 for the possession. |
| `def_player_3` | Int64 | NBA Stats player id of defensive on-court player 3 of 5 for the possession. |
| `def_player_4` | Int64 | NBA Stats player id of defensive on-court player 4 of 5 for the possession. |
| `def_player_5` | Int64 | NBA Stats player id of defensive on-court player 5 of 5 for the possession. |
| `lineup_source` | String | Provenance of the on-court lineup identification for the row (how the five-man units were resolved). |
| `season` | Int64 | Season year. |

```python
load_nba_stats_possessions(seasons=2025)
```

## load_nba_stats_game_lineups

Release: [nba_stats_game_lineups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_game_lineups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_game_lineups/nba_lineups_{season + 1}.parquet`
### Returns {#load_nba_stats_game_lineups-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `home_player_1` | Int64 | NBA Stats player id of the home on-court player 1 of 5 for the row. |
| `home_player_2` | Int64 | NBA Stats player id of the home on-court player 2 of 5 for the row. |
| `home_player_3` | Int64 | NBA Stats player id of the home on-court player 3 of 5 for the row. |
| `home_player_4` | Int64 | NBA Stats player id of the home on-court player 4 of 5 for the row. |
| `home_player_5` | Int64 | NBA Stats player id of the home on-court player 5 of 5 for the row. |
| `away_player_1` | Int64 | NBA Stats player id of the away on-court player 1 of 5 for the row. |
| `away_player_2` | Int64 | NBA Stats player id of the away on-court player 2 of 5 for the row. |
| `away_player_3` | Int64 | NBA Stats player id of the away on-court player 3 of 5 for the row. |
| `away_player_4` | Int64 | NBA Stats player id of the away on-court player 4 of 5 for the row. |
| `away_player_5` | Int64 | NBA Stats player id of the away on-court player 5 of 5 for the row. |
| `season` | Int64 | Season year. |

```python
load_nba_stats_game_lineups(seasons=2025)
```

## load_nba_stats_game_matchups

Release: [nba_stats_game_matchups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_game_matchups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_game_matchups/game_matchups_{season + 1}.parquet`
### Returns {#load_nba_stats_game_matchups-returns}

| col_name | type | description |
|---|---|---|
| `off_team_id` | Int64 | Team id of the offensive player. Taken from the payload's team block. |
| `off_team_city` | String | City/market of the offensive player's team ("Indiana"). |
| `off_team_name` | String | Nickname of the offensive player's team ("Pacers") -- pair with `off_team_city` for the full club name. |
| `off_team_tricode` | String | Three-letter abbreviation of the offensive player's team ("IND"). |
| `off_team_slug` | String | URL slug of the offensive player's team ("pacers"). |
| `def_team_id` | Int64 | Team id of the defender, read from the game envelope's homeTeamId/awayTeamId rather than the nested team object, which is 0 on uncovered captures. |
| `side` | String | Which side of the game the row's team was on: "home" or "away". In `game_matchups`, where a row carries two teams, it is the OFFENSIVE player's team (`off_team_id`) -- the defender is always the other side. |
| `off_person_id` | Int64 | stats.nba.com person id of the offensive player -- the one being guarded. |
| `off_first_name` | String | First name of the offensive player. |
| `off_family_name` | String | Family name of the offensive player. |
| `off_name_i` | String | Offensive player's abbreviated display name ("B. Mathurin"). |
| `off_player_slug` | String | URL slug of the offensive player ("bennedict-mathurin"). |
| `off_position` | String | Starting position of the offensive player as the payload reports it; empty for players who did not start. |
| `off_comment` | String | Availability note on the offensive player (DNP reason); empty when they played. |
| `off_jersey_num` | String | Jersey number of the offensive player, as a string (it can carry a leading zero, e.g. "00"). |
| `def_person_id` | Int64 | stats.nba.com person id of the defender guarding the offensive player. |
| `def_first_name` | String | First name of the defender. |
| `def_family_name` | String | Family name of the defender. |
| `def_name_i` | String | Defender's abbreviated display name ("J. Allen"). |
| `def_player_slug` | String | URL slug of the defender ("jarrett-allen"). |
| `def_jersey_num` | String | Jersey number of the defender, as a string (it can carry a leading zero). |
| `matchup_minutes` | String | Time the pair were matched up, as the payload's MM:SS string; use `matchup_minutes_sort` for arithmetic. |
| `matchup_minutes_sort` | Float64 | The same matchup time in seconds, as a float -- the sortable/summable form. |
| `partial_possessions` | Float64 | Possessions credited to the matchup. Fractional because a possession is split across every defender who guarded the ball-handler during it, which is why matchup counting stats do not sum exactly to a player's game totals. |
| `percentage_defender_total_time` | Float64 | Share of the defender's floor time spent guarding this offensive player. |
| `percentage_offensive_total_time` | Float64 | Share of the offensive player's floor time spent guarded by this defender. |
| `percentage_total_time_both_on` | Float64 | Share of the time both players were on the floor together that they were matched up. |
| `switches_on` | Int64 | Times the defense switched this defender onto the offensive player. |
| `player_points` | Int64 | Points the offensive player scored while guarded by this defender. |
| `team_points` | Int64 | Points the offensive player's team scored while this matchup was on. |
| `matchup_assists` | Int64 | Assists by the offensive player while guarded by this defender. |
| `matchup_potential_assists` | Int64 | Passes by the offensive player that would have been assists had the shot fallen, while guarded by this defender. |
| `matchup_turnovers` | Int64 | Turnovers by the offensive player while guarded by this defender. |
| `matchup_blocks` | Int64 | Shots by the offensive player blocked by this defender. |
| `matchup_field_goals_made` | Int64 | Field goals made by the offensive player against this defender. |
| `matchup_field_goals_attempted` | Int64 | Field goals attempted by the offensive player against this defender. |
| `matchup_field_goals_percentage` | Float64 | Field-goal percentage of the offensive player against this defender. |
| `matchup_three_pointers_made` | Int64 | Three-pointers made by the offensive player against this defender. |
| `matchup_three_pointers_attempted` | Int64 | Three-pointers attempted by the offensive player against this defender. |
| `matchup_three_pointers_percentage` | Float64 | Three-point percentage of the offensive player against this defender. |
| `help_blocks` | Int64 | Blocks by this defender on the offensive player when helping off another assignment rather than as the primary defender. |
| `help_field_goals_made` | Int64 | Field goals the offensive player made against this defender in help defense. |
| `help_field_goals_attempted` | Int64 | Field goals the offensive player attempted against this defender in help defense. |
| `help_field_goals_percentage` | Float64 | Field-goal percentage allowed by this defender in help defense. |
| `matchup_free_throws_made` | Int64 | Free throws made by the offensive player on trips drawn against this defender. |
| `matchup_free_throws_attempted` | Int64 | Free throws attempted by the offensive player on trips drawn against this defender. |
| `shooting_fouls` | Int64 | Shooting fouls committed by this defender on the offensive player. |
| `game_id` | String | Unique game identifier. |
| `season` | Int32 | Season year. |
| `season_type_id` | String | Season-type digit: the 3rd character of game_id (and the leading digit of season_id). 1 = preseason, 2 = regular season, 3 = All-Star, 4 = playoffs, 5 = play-in, 6 = NBA Cup final, 9 = international. |

```python
load_nba_stats_game_matchups(seasons=2025)
```

## load_nba_stats_pbp_v3

Release: [nba_stats_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_pbp/nba_play_by_play_{season + 1}.parquet`
### Returns {#load_nba_stats_pbp_v3-returns}

| col_name | type | description |
|---|---|---|
| `order_index` | Int64 | Stable within-game ordering index for events after pbpstats-style reordering of the raw feed. |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `clock` | String | Game clock value. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | Int64 | Unique team identifier. |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `player_name_i` | String | Player name i. |
| `x_legacy` | Int64 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int64 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `shot_distance` | Int64 | Shot distance from the basket, in feet. |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `is_field_goal` | Int64 | 1 if the action was a field goal; 0 otherwise. |
| `score_home` | String | Score home. |
| `score_away` | String | Score away. |
| `points_total` | Int64 | Running total of points scored. |
| `location` | String | Location. |
| `description` | String | Long-form description text. |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `video_available` | Int64 | Video available. |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `action_id` | Int64 | Unique action identifier within a game (V3 PBP). |
| `game_id` | String | Unique game identifier. |
| `seconds_remaining` | Float64 | Seconds remaining in the period. |
| `event_type` | String | Event / play type code (V2 PBP). |
| `is_made_shot` | Boolean | Whether the event is a made field goal. |
| `is_missed_shot` | Boolean | Whether the event is a missed field goal. |
| `is_free_throw` | Boolean | Whether the event is a free throw attempt. |
| `is_rebound` | Boolean | Whether the event is a rebound. |
| `is_turnover` | Boolean | `TRUE` if the play was a turnover. |
| `is_foul` | Boolean | Whether the event is a foul. |
| `is_substitution` | Boolean | Whether the event is a substitution. |
| `is_jump_ball` | Boolean | Whether the event is a jump ball. |
| `is_timeout` | Boolean | Whether the event is a timeout. |
| `is_period` | Boolean | Whether the event is a period start or end marker. |
| `possession_number` | Int64 | Possession number. |
| `off_player_1` | Int64 | NBA Stats player id of offensive on-court player 1 of 5 during the event. |
| `off_player_2` | Int64 | NBA Stats player id of offensive on-court player 2 of 5 during the event. |
| `off_player_3` | Int64 | NBA Stats player id of offensive on-court player 3 of 5 during the event. |
| `off_player_4` | Int64 | NBA Stats player id of offensive on-court player 4 of 5 during the event. |
| `off_player_5` | Int64 | NBA Stats player id of offensive on-court player 5 of 5 during the event. |
| `def_player_1` | Int64 | NBA Stats player id of defensive on-court player 1 of 5 during the event. |
| `def_player_2` | Int64 | NBA Stats player id of defensive on-court player 2 of 5 during the event. |
| `def_player_3` | Int64 | NBA Stats player id of defensive on-court player 3 of 5 during the event. |
| `def_player_4` | Int64 | NBA Stats player id of defensive on-court player 4 of 5 during the event. |
| `def_player_5` | Int64 | NBA Stats player id of defensive on-court player 5 of 5 during the event. |
| `season` | Int64 | Season year. |

```python
load_nba_stats_pbp_v3(seasons=2025)
```

## load_nba_stats_player_boxscores

Release: [nba_stats_player_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_player_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_player_boxscores/player_boxscores_{season + 1}.parquet`
### Returns {#load_nba_stats_player_boxscores-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | City/market of the team ("Indiana"); pair with `team_name` for the full club name. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `team_slug` | String | URL slug of the team ("pacers"). |
| `side` | String | Side label (e.g. 'home', 'away', or 'overUnder'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `first_name` | String | Player's first name. |
| `family_name` | String | Player's family / last name. |
| `name_i` | String | Initialed name (e.g. 'A. Wilson'). |
| `player_slug` | String | URL-safe player identifier. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `field_goals_made` | Int64 | Field goals made (2-pt + 3-pt). |
| `field_goals_attempted` | Int64 | Field goal attempts (2-pt + 3-pt). |
| `field_goals_percentage` | Float64 | Field goal percentage (0-1 decimal). |
| `three_pointers_made` | Int64 | Three-point field goals made. |
| `three_pointers_attempted` | Int64 | Three-point field goal attempts. |
| `three_pointers_percentage` | Float64 | Three-point field goal percentage (0-1 decimal). |
| `free_throws_made` | Int64 | Free throws made. |
| `free_throws_attempted` | Int64 | Free throw attempts. |
| `free_throws_percentage` | Float64 | Free throw percentage (0-1 decimal). |
| `rebounds_offensive` | Int64 | Offensive rebounds. |
| `rebounds_defensive` | Int64 | Defensive rebounds. |
| `rebounds_total` | Int64 | Total rebounds. |
| `assists` | Int64 | Total assists. |
| `steals` | Int64 | Total steals. |
| `blocks` | Int64 | Total blocks. |
| `turnovers` | Int64 | Total turnovers. |
| `fouls_personal` | Int64 | Personal fouls. |
| `points` | Int64 | Points scored. |
| `plus_minus_points` | Float64 | Plus/minus point differential while on court. |
| `game_id` | String | Unique game identifier. |
| `season` | Int32 | Season year. |
| `season_type_id` | String | Season-type digit: the 3rd character of game_id (and the leading digit of season_id). 1 = preseason, 2 = regular season, 3 = All-Star, 4 = playoffs, 5 = play-in, 6 = NBA Cup final, 9 = international. |

```python
load_nba_stats_player_boxscores(seasons=2025)
```

## load_nba_stats_player_game_logs

Release: [nba_stats_player_game_logs](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_player_game_logs) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_player_game_logs/player_game_logs_{season + 1}.parquet`
### Returns {#load_nba_stats_player_game_logs-returns}

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `wl` | String | Wl. |
| `min` | Int64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3m` | Int64 | Three-point field goals made. |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Rebounds per game. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |
| `video_available` | Int64 | Video available. |
| `season` | Int32 | Season year. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

```python
load_nba_stats_player_game_logs(seasons=2025)
```

## load_nba_stats_player_season_stats

Release: [nba_stats_player_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_player_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_player_season_stats/player_season_stats_{season + 1}.parquet`
### Returns {#load_nba_stats_player_season_stats-returns}

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `age` | Float64 | Player age (in years). |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `e_off_rating` | Float64 | Estimated offensive rating (NBA Stats estimated-metrics family) over the split. |
| `off_rating` | Float64 | Offensive rating (points scored per 100 possessions) over the split. |
| `sp_work_off_rating` | Float64 | Offensive rating carried in the stats API's SP_WORK column set (mirrors off_rating) over the split. |
| `e_def_rating` | Float64 | Estimated defensive rating (NBA Stats estimated-metrics family) over the split. |
| `def_rating` | Float64 | Defensive rating (points allowed per 100 possessions) over the split. |
| `sp_work_def_rating` | Float64 | Defensive rating carried in the stats API's SP_WORK column set (mirrors def_rating) over the split. |
| `e_net_rating` | Float64 | Estimated net rating (NBA Stats estimated-metrics family) over the split. |
| `net_rating` | Float64 | Net rating (off rating - def rating). |
| `sp_work_net_rating` | Float64 | Net rating carried in the stats API's SP_WORK column set (mirrors net_rating) over the split. |
| `ast_pct` | Float64 | Assist percentage. |
| `ast_to` | Float64 | Assist-to-turnover ratio over the split. |
| `ast_ratio` | Float64 | Assist ratio (assists per 100 possessions used) over the split. |
| `oreb_pct` | Float64 | Offensive rebound percentage over the split, as a decimal. |
| `dreb_pct` | Float64 | Defensive rebound percentage over the split, as a decimal. |
| `reb_pct` | Float64 | Total rebound percentage over the split, as a decimal. |
| `tm_tov_pct` | Float64 | Team turnover percentage (turnovers per 100 possessions) over the split, as a decimal. |
| `e_tov_pct` | Float64 | Estimated turnover percentage (NBA Stats estimated-metrics family) over the split, as a decimal. |
| `efg_pct` | Float64 | Effective field goal percentage over the split, as a decimal. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `usg_pct` | Float64 | Usage percentage (share of team plays used while on the floor) over the split, as a decimal. |
| `e_usg_pct` | Float64 | Estimated usage percentage (NBA Stats estimated-metrics family) over the split, as a decimal. |
| `e_pace` | Float64 | Estimated pace (NBA Stats estimated-metrics family) over the split. |
| `pace` | Float64 | Possessions per 48 minutes. |
| `pace_per40` | Float64 | Pace per40. |
| `sp_work_pace` | Float64 | Pace carried in the stats API's SP_WORK column set (mirrors pace) over the split. |
| `pie` | Float64 | Player Impact Estimate (0-1). |
| `poss` | Int64 | Poss. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fgm_pg` | Float64 | Field goals made per game over the split. |
| `fga_pg` | Float64 | Field goals attempted per game over the split. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `e_off_rating_rank` | Int64 | League rank of the row's estimated offensive rating (NBA Stats estimated-metrics family) for the season and split. |
| `off_rating_rank` | Int64 | League rank of the row's offensive rating (points scored per 100 possessions) for the season and split. |
| `sp_work_off_rating_rank` | Int64 | League rank of the row's offensive rating carried in the stats API's SP_WORK column set (mirrors off_rating) for the season and split. |
| `e_def_rating_rank` | Int64 | League rank of the row's estimated defensive rating (NBA Stats estimated-metrics family) for the season and split. |
| `def_rating_rank` | Int64 | League rank of the row's defensive rating (points allowed per 100 possessions) for the season and split. |
| `sp_work_def_rating_rank` | Int64 | League rank of the row's defensive rating carried in the stats API's SP_WORK column set (mirrors def_rating) for the season and split. |
| `e_net_rating_rank` | Int64 | League rank of the row's estimated net rating (NBA Stats estimated-metrics family) for the season and split. |
| `net_rating_rank` | Int64 | League rank of the row's net rating (offensive minus defensive rating) for the season and split. |
| `sp_work_net_rating_rank` | Int64 | League rank of the row's net rating carried in the stats API's SP_WORK column set (mirrors net_rating) for the season and split. |
| `ast_pct_rank` | Int64 | League rank of the row's assist percentage (share of teammate field goals assisted while on the floor) for the season and split. |
| `ast_to_rank` | Int64 | League rank of the row's assist-to-turnover ratio for the season and split. |
| `ast_ratio_rank` | Int64 | League rank of the row's assist ratio (assists per 100 possessions used) for the season and split. |
| `oreb_pct_rank` | Int64 | League rank of the row's offensive rebound percentage for the season and split. |
| `dreb_pct_rank` | Int64 | League rank of the row's defensive rebound percentage for the season and split. |
| `reb_pct_rank` | Int64 | League rank of the row's total rebound percentage for the season and split. |
| `tm_tov_pct_rank` | Int64 | League rank of the row's team turnover percentage (turnovers per 100 possessions) for the season and split. |
| `e_tov_pct_rank` | Int64 | League rank of the row's estimated turnover percentage (NBA Stats estimated-metrics family) for the season and split. |
| `efg_pct_rank` | Int64 | League rank of the row's effective field goal percentage for the season and split. |
| `ts_pct_rank` | Int64 | League rank of the row's true shooting percentage for the season and split. |
| `usg_pct_rank` | Int64 | League rank of the row's usage percentage (share of team plays used while on the floor) for the season and split. |
| `e_usg_pct_rank` | Int64 | League rank of the row's estimated usage percentage (NBA Stats estimated-metrics family) for the season and split. |
| `e_pace_rank` | Int64 | League rank of the row's estimated pace (NBA Stats estimated-metrics family) for the season and split. |
| `pace_rank` | Int64 | League rank of the row's pace (possessions per 48 minutes) for the season and split. |
| `sp_work_pace_rank` | Int64 | League rank of the row's pace carried in the stats API's SP_WORK column set (mirrors pace) for the season and split. |
| `pie_rank` | Int64 | League rank of the row's Player Impact Estimate (PIE, the NBA Stats catch-all impact metric) for the season and split. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fgm_pg_rank` | Int64 | League rank of the row's field goals made per game for the season and split. |
| `fga_pg_rank` | Int64 | League rank of the row's field goals attempted per game for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `team_count` | Int64 | Number of distinct teams aggregated into the split row. |
| `season` | Int32 | Season year. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `measure_type` | String | NBA Stats measure type the row was pulled from (e.g. Base, Advanced, Misc, Scoring, Opponent, Usage, Defense). |
| `per_mode` | String | NBA Stats per-mode of the row values (e.g. Totals, PerGame, Per100Possessions). |
| `fg3m` | Float64 | Three-point field goals made. |
| `fg3a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Rebounds per game. |
| `ast` | Float64 | Assists. |
| `tov` | Float64 | Turnovers. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 | Personal fouls. |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 | Points scored. |
| `plus_minus` | Float64 | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `wnba_fantasy_pts` | Float64 | Fantasy points under the WNBA's fantasy scoring formula. |
| `fg3m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
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
| `nba_fantasy_pts_rank` | Int64 | League rank of the row's NBA fantasy points (league scoring formula) for the season and split. |
| `dd2_rank` | Int64 | League rank of the row's double-doubles for the season and split. |
| `td3_rank` | Int64 | League rank of the row's triple-doubles for the season and split. |
| `wnba_fantasy_pts_rank` | Int64 | League rank of the row's WNBA fantasy points (league scoring formula) for the season and split. |
| `pct_dreb` | Float64 | Share of the team's defensive rebounds accounted for by the player while on the floor, as a decimal. |
| `pct_stl` | Float64 | Share of the team's steals accounted for by the player while on the floor, as a decimal. |
| `pct_blk` | Float64 | Share of the team's blocked shots accounted for by the player while on the floor, as a decimal. |
| `opp_pts_off_tov` | Float64 | Opponent points scored off opponent turnovers allowed over the split. |
| `opp_pts_2nd_chance` | Float64 | Opponent second-chance points allowed over the split. |
| `opp_pts_fb` | Float64 | Opponent fast-break points allowed over the split. |
| `opp_pts_paint` | Float64 | Opponent points in the paint allowed over the split. |
| `def_ws` | Float64 | Defensive win shares credited to the player (NBA Stats defense dashboard metric). |
| `def_ws_raw` | Float64 | Unscaled (raw) defensive win shares value carried alongside def_ws by the NBA Stats API. |
| `pct_dreb_rank` | Int64 | League rank of the row's share of the team's defensive rebounds accounted for by the player while on the floor for the season and split. |
| `pct_stl_rank` | Int64 | League rank of the row's share of the team's steals accounted for by the player while on the floor for the season and split. |
| `pct_blk_rank` | Int64 | League rank of the row's share of the team's blocked shots accounted for by the player while on the floor for the season and split. |
| `opp_pts_off_tov_rank` | Int64 | League rank of the row's opponent points scored off opponent turnovers for the season and split. |
| `opp_pts_2nd_chance_rank` | Int64 | League rank of the row's opponent second-chance points for the season and split. |
| `opp_pts_fb_rank` | Int64 | League rank of the row's opponent fast-break points for the season and split. |
| `opp_pts_paint_rank` | Int64 | League rank of the row's opponent points in the paint for the season and split. |
| `def_ws_rank` | Int64 | League rank of the row's defensive win shares (NBA Stats defense dashboard metric) for the season and split. |
| `pts_off_tov` | Float64 | Points scored off opponent turnovers over the split. |
| `pts_2nd_chance` | Float64 | Second-chance points over the split. |
| `pts_fb` | Float64 | Fast-break points over the split. |
| `pts_paint` | Float64 | Points in the paint over the split. |
| `pts_off_tov_rank` | Int64 | League rank of the row's points scored off opponent turnovers for the season and split. |
| `pts_2nd_chance_rank` | Int64 | League rank of the row's second-chance points for the season and split. |
| `pts_fb_rank` | Int64 | League rank of the row's fast-break points for the season and split. |
| `pts_paint_rank` | Int64 | League rank of the row's points in the paint for the season and split. |
| `pct_fga_2pt` | Float64 | Share of field goal attempts taken as two-pointers, as a decimal. |
| `pct_fga_3pt` | Float64 | Share of field goal attempts taken as three-pointers, as a decimal. |
| `pct_pts_2pt` | Float64 | Share of points scored on two-point field goals, as a decimal. |
| `pct_pts_2pt_mr` | Float64 | Share of points scored on mid-range two-pointers, as a decimal. |
| `pct_pts_3pt` | Float64 | Share of points scored on three-pointers, as a decimal. |
| `pct_pts_fb` | Float64 | Share of points scored on fast breaks, as a decimal. |
| `pct_pts_ft` | Float64 | Share of points scored at the free throw line, as a decimal. |
| `pct_pts_off_tov` | Float64 | Share of points scored off opponent turnovers, as a decimal. |
| `pct_pts_paint` | Float64 | Share of points scored in the paint, as a decimal. |
| `pct_ast_2pm` | Float64 | Percentage of made two-pointers that were assisted, as a decimal. |
| `pct_uast_2pm` | Float64 | Percentage of made two-pointers that were unassisted, as a decimal. |
| `pct_ast_3pm` | Float64 | Percentage of made three-pointers that were assisted, as a decimal. |
| `pct_uast_3pm` | Float64 | Percentage of made three-pointers that were unassisted, as a decimal. |
| `pct_ast_fgm` | Float64 | Percentage of made field goals that were assisted, as a decimal. |
| `pct_uast_fgm` | Float64 | Percentage of made field goals that were unassisted, as a decimal. |
| `pct_fga_2pt_rank` | Int64 | League rank of the row's share of field goal attempts taken as two-pointers for the season and split. |
| `pct_fga_3pt_rank` | Int64 | League rank of the row's share of field goal attempts taken as three-pointers for the season and split. |
| `pct_pts_2pt_rank` | Int64 | League rank of the row's share of points scored on two-point field goals for the season and split. |
| `pct_pts_2pt_mr_rank` | Int64 | League rank of the row's share of points scored on mid-range two-pointers for the season and split. |
| `pct_pts_3pt_rank` | Int64 | League rank of the row's share of points scored on three-pointers for the season and split. |
| `pct_pts_fb_rank` | Int64 | League rank of the row's share of points scored on fast breaks for the season and split. |
| `pct_pts_ft_rank` | Int64 | League rank of the row's share of points scored at the free throw line for the season and split. |
| `pct_pts_off_tov_rank` | Int64 | League rank of the row's share of points scored off opponent turnovers for the season and split. |
| `pct_pts_paint_rank` | Int64 | League rank of the row's share of points scored in the paint for the season and split. |
| `pct_ast_2pm_rank` | Int64 | League rank of the row's percentage of made two-pointers that were assisted for the season and split. |
| `pct_uast_2pm_rank` | Int64 | League rank of the row's percentage of made two-pointers that were unassisted for the season and split. |
| `pct_ast_3pm_rank` | Int64 | League rank of the row's percentage of made three-pointers that were assisted for the season and split. |
| `pct_uast_3pm_rank` | Int64 | League rank of the row's percentage of made three-pointers that were unassisted for the season and split. |
| `pct_ast_fgm_rank` | Int64 | League rank of the row's percentage of made field goals that were assisted for the season and split. |
| `pct_uast_fgm_rank` | Int64 | League rank of the row's percentage of made field goals that were unassisted for the season and split. |
| `pct_fgm` | Float64 | Share of the team's field goals made accounted for by the player while on the floor, as a decimal. |
| `pct_fga` | Float64 | Share of the team's field goals attempted accounted for by the player while on the floor, as a decimal. |
| `pct_fg3m` | Float64 | Share of the team's three-point field goals made accounted for by the player while on the floor, as a decimal. |
| `pct_fg3a` | Float64 | Share of the team's three-point field goals attempted accounted for by the player while on the floor, as a decimal. |
| `pct_ftm` | Float64 | Share of the team's free throws made accounted for by the player while on the floor, as a decimal. |
| `pct_fta` | Float64 | Share of the team's free throws attempted accounted for by the player while on the floor, as a decimal. |
| `pct_oreb` | Float64 | Share of the team's offensive rebounds accounted for by the player while on the floor, as a decimal. |
| `pct_reb` | Float64 | Share of the team's total rebounds accounted for by the player while on the floor, as a decimal. |
| `pct_ast` | Float64 | Share of the team's assists accounted for by the player while on the floor, as a decimal. |
| `pct_tov` | Float64 | Share of the team's turnovers accounted for by the player while on the floor, as a decimal. |
| `pct_blka` | Float64 | Share of the team's shot attempts blocked by opponents (blocks against) accounted for by the player while on the floor, as a decimal. |
| `pct_pf` | Float64 | Share of the team's personal fouls committed accounted for by the player while on the floor, as a decimal. |
| `pct_pfd` | Float64 | Share of the team's personal fouls drawn accounted for by the player while on the floor, as a decimal. |
| `pct_pts` | Float64 | Share of the team's points scored accounted for by the player while on the floor, as a decimal. |
| `pct_fgm_rank` | Int64 | League rank of the row's share of the team's field goals made accounted for by the player while on the floor for the season and split. |
| `pct_fga_rank` | Int64 | League rank of the row's share of the team's field goals attempted accounted for by the player while on the floor for the season and split. |
| `pct_fg3m_rank` | Int64 | League rank of the row's share of the team's three-point field goals made accounted for by the player while on the floor for the season and split. |
| `pct_fg3a_rank` | Int64 | League rank of the row's share of the team's three-point field goals attempted accounted for by the player while on the floor for the season and split. |
| `pct_ftm_rank` | Int64 | League rank of the row's share of the team's free throws made accounted for by the player while on the floor for the season and split. |
| `pct_fta_rank` | Int64 | League rank of the row's share of the team's free throws attempted accounted for by the player while on the floor for the season and split. |
| `pct_oreb_rank` | Int64 | League rank of the row's share of the team's offensive rebounds accounted for by the player while on the floor for the season and split. |
| `pct_reb_rank` | Int64 | League rank of the row's share of the team's total rebounds accounted for by the player while on the floor for the season and split. |
| `pct_ast_rank` | Int64 | League rank of the row's share of the team's assists accounted for by the player while on the floor for the season and split. |
| `pct_tov_rank` | Int64 | League rank of the row's share of the team's turnovers accounted for by the player while on the floor for the season and split. |
| `pct_blka_rank` | Int64 | League rank of the row's share of the team's shot attempts blocked by opponents (blocks against) accounted for by the player while on the floor for the season and split. |
| `pct_pf_rank` | Int64 | League rank of the row's share of the team's personal fouls committed accounted for by the player while on the floor for the season and split. |
| `pct_pfd_rank` | Int64 | League rank of the row's share of the team's personal fouls drawn accounted for by the player while on the floor for the season and split. |
| `pct_pts_rank` | Int64 | League rank of the row's share of the team's points scored accounted for by the player while on the floor for the season and split. |

```python
load_nba_stats_player_season_stats(seasons=2025)
```
