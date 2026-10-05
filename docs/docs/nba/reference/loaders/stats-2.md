---
title: "NBA dataset loaders — Stats (2)"
sidebar_label: "Stats (2)"
sidebar_position: 3
description: "NBA dataset loaders — Stats (2) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA dataset loaders — Stats (2)

## load_nba_stats_possessions_v3

Release: [nba_stats_possessions](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_possessions) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_possessions/nba_possessions_{season + 1}.parquet`
### Returns {#load_nba_stats_possessions_v3-returns}

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
load_nba_stats_possessions_v3(seasons=2025)
```

## load_nba_stats_rosters

Release: [nba_stats_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_rosters/rosters_{season + 1}.parquet`
### Returns {#load_nba_stats_rosters-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `season` | Int32 | Season year. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `player` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `player_slug` | String | URL-safe player identifier. |
| `num` | String | Jersey number worn by the player. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | String | Player weight in pounds. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `age` | Float64 | Player age (in years). |
| `exp` | String | Years of NBA playing experience entering the season ('R' = rookie). |
| `school` | String | Player school / pre-draft team. |
| `player_id` | Int64 | Unique player identifier. |
| `how_acquired` | String | How the team acquired the player (e.g. draft, trade, free agency). |
| `supplemental_status` | Int64 | Numeric supplemental roster-status code from the stats.nba.com roster feed. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

```python
load_nba_stats_rosters(seasons=2025)
```

## load_nba_stats_shots

Release: [nba_stats_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_shots) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_shots/shots_{season + 1}.parquet`
### Returns {#load_nba_stats_shots-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `season` | Int32 | Season year. |
| `season_type_id` | String | Season-type digit: the 3rd character of game_id (and the leading digit of season_id). 1 = preseason, 2 = regular season, 3 = All-Star, 4 = playoffs, 5 = play-in, 6 = NBA Cup final, 9 = international. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `clock` | String | Game clock value. |
| `team_id` | Int64 | Unique team identifier. |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `shot_distance` | Int64 | Shot distance from the basket, in feet. |
| `x_legacy` | Int64 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int64 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `description` | String | Long-form description text. |
| `score_home` | String | Score home. |
| `score_away` | String | Score away. |

```python
load_nba_stats_shots(seasons=2025)
```

## load_nba_stats_standings

Release: [nba_stats_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_standings) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_standings/standings_{season + 1}.parquet`
### Returns {#load_nba_stats_standings-returns}

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier ('10' = WNBA). |
| `season_id` | String | Unique season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `conference` | String | Conference name. |
| `conference_record` | String | Conference win-loss record. |
| `playoff_rank` | Int64 | League/season rank for playoff. |
| `clinch_indicator` | String | Playoff clinch indicator (e.g. 'x' clinched playoff, 'e' eliminated). |
| `division` | String | Team division. |
| `division_record` | String | Win-loss record against division opponents. |
| `division_rank` | Int64 | Team's rank within its division. |
| `wins` | Int64 | Total wins. |
| `losses` | Int64 | Total losses. |
| `win_pct` | Float64 | Win percentage (0-1 decimal). |
| `league_rank` | Int64 | Team's rank in the overall league standings. |
| `record` | String | Overall win-loss record. |
| `home` | String | Home. |
| `road` | String | Road. |
| `l10` | String | Last-ten record. |
| `last10_home` | String | Win-loss record over the team's last 10 home games. |
| `last10_road` | String | Win-loss record over the team's last 10 road games. |
| `ot` | String | Ot. |
| `three_pts_or_less` | String | Win-loss record in games decided by three points or fewer. |
| `ten_pts_or_more` | String | Win-loss record in games decided by ten points or more. |
| `long_home_streak` | Int64 | Longest home streak of the season (positive counts wins, negative losses). |
| `str_long_home_streak` | String | Longest home streak of the season as display text (e.g. "W 5"). |
| `long_road_streak` | Int64 | Longest road streak of the season (positive counts wins, negative losses). |
| `str_long_road_streak` | String | Longest road streak of the season as display text (e.g. "W 5"). |
| `long_win_streak` | Int64 | Longest winning streak of the season, in games. |
| `long_loss_streak` | Int64 | Longest losing streak of the season, in games. |
| `current_home_streak` | Int64 | Current home streak (positive counts wins, negative losses). |
| `str_current_home_streak` | String | Current home streak as display text (e.g. "L 2"). |
| `current_road_streak` | Int64 | Current road streak (positive counts wins, negative losses). |
| `str_current_road_streak` | String | Current road streak as display text (e.g. "W 3"). |
| `current_streak` | Int64 | Current overall streak (positive counts wins, negative losses). |
| `str_current_streak` | String | Current overall streak as display text (e.g. "W 4"). |
| `conference_games_back` | Float64 | Games behind the conference leader. |
| `division_games_back` | Float64 | Games behind the division leader. |
| `clinched_conference_title` | Int64 | Flag (1/0) for whether the team has clinched the conference title. |
| `clinched_division_title` | Int64 | Flag (1/0) for whether the team has clinched its division. |
| `clinched_playoff_birth` | Int64 | Flag (1/0) for whether the team has clinched a playoff berth. |
| `clinched_play_in` | Int64 | Flag (1/0) for whether the team has clinched a play-in tournament spot. |
| `eliminated_conference` | Int64 | Flag (1/0) for whether the team is eliminated from conference contention. |
| `eliminated_division` | Int64 | Flag (1/0) for whether the team is eliminated from division contention. |
| `ahead_at_half` | String | Win-loss record when leading at halftime. |
| `behind_at_half` | String | Win-loss record when trailing at halftime. |
| `tied_at_half` | String | Win-loss record when tied at halftime. |
| `ahead_at_third` | String | Win-loss record when leading after three quarters. |
| `behind_at_third` | String | Win-loss record when trailing after three quarters. |
| `tied_at_third` | String | Win-loss record when tied after three quarters. |
| `score100_pts` | String | Win-loss record when scoring 100 or more points. |
| `opp_score100_pts` | String | Win-loss record when the opponent scores 100 or more points. |
| `opp_over500` | String | Win-loss record against teams with winning (over .500) records. |
| `lead_in_fgpct` | String | Win-loss record when posting the higher field goal percentage. |
| `lead_in_reb` | String | Win-loss record when out-rebounding the opponent. |
| `fewer_turnovers` | String | Win-loss record when committing fewer turnovers than the opponent. |
| `points_pg` | Float64 | Points pg. |
| `opp_points_pg` | Float64 | Opponent points pg. |
| `diff_points_pg` | Float64 | Diff points pg. |
| `vs_east` | String | Win-loss record against Eastern Conference opponents. |
| `vs_atlantic` | String | Win-loss record against Atlantic Division opponents. |
| `vs_central` | String | Win-loss record against Central Division opponents. |
| `vs_southeast` | String | Win-loss record against Southeast Division opponents. |
| `vs_west` | String | Win-loss record against Western Conference opponents. |
| `vs_northwest` | String | Win-loss record against Northwest Division opponents. |
| `vs_pacific` | String | Win-loss record against Pacific Division opponents. |
| `vs_southwest` | String | Win-loss record against Southwest Division opponents. |
| `jan` | String | Win-loss record in games played in January. |
| `feb` | String | Win-loss record in games played in February. |
| `mar` | String | Win-loss record in games played in March. |
| `apr` | String | Win-loss record in games played in April. |
| `may` | Null | Win-loss record in games played in May. |
| `jun` | Null | Win-loss record in games played in June. |
| `jul` | Null | Win-loss record in games played in July. |
| `aug` | Null | Win-loss record in games played in August. |
| `sep` | Null | Win-loss record in games played in September. |
| `oct` | String | Win-loss record in games played in October. |
| `nov` | String | Win-loss record in games played in November. |
| `dec` | String | Win-loss record in games played in December. |
| `score_80_plus` | String | Win-loss record when scoring 80 or more points. |
| `opp_score_80_plus` | String | Win-loss record when the opponent scores 80 or more points. |
| `score_below_80` | String | Win-loss record when scoring fewer than 80 points. |
| `opp_score_below_80` | String | Win-loss record when holding the opponent below 80 points. |
| `total_points` | Int64 | Total points scored by the team over the season to date. |
| `opp_total_points` | Int64 | Total points allowed by the team over the season to date. |
| `diff_total_points` | Int64 | Season point differential (points scored minus points allowed). |
| `league_games_back` | Float64 | Games behind the overall league leader. |
| `playoff_seeding` | Int64 | Team's current playoff seed. |
| `clinched_post_season` | Int64 | Flag (1/0) for whether the team has clinched any postseason berth. |
| `neutral` | String | Neutral. |
| `season` | Int32 | Season year. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |

```python
load_nba_stats_standings(seasons=2025)
```

## load_nba_stats_team_boxscores

Release: [nba_stats_team_boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_team_boxscores) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_team_boxscores/team_boxscores_{season + 1}.parquet`
### Returns {#load_nba_stats_team_boxscores-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | City/market of the team ("Indiana"); pair with `team_name` for the full club name. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `team_slug` | String | URL slug of the team ("pacers"). |
| `side` | String | Side label (e.g. 'home', 'away', or 'overUnder'). |
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
load_nba_stats_team_boxscores(seasons=2025)
```

## load_nba_stats_team_season_stats

Release: [nba_stats_team_season_stats](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/nba_stats_team_season_stats) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nba_stats_team_season_stats/team_season_stats_{season + 1}.parquet`
### Returns {#load_nba_stats_team_season_stats-returns}

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
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
| `opp_pts_off_tov` | Float64 | Opponent points scored off opponent turnovers allowed over the split. |
| `opp_pts_2nd_chance` | Float64 | Opponent second-chance points allowed over the split. |
| `opp_pts_fb` | Float64 | Opponent fast-break points allowed over the split. |
| `opp_pts_paint` | Float64 | Opponent points in the paint allowed over the split. |
| `opp_pts_off_tov_rank` | Int64 | League rank of the row's opponent points scored off opponent turnovers for the season and split. |
| `opp_pts_2nd_chance_rank` | Int64 | League rank of the row's opponent second-chance points for the season and split. |
| `opp_pts_fb_rank` | Int64 | League rank of the row's opponent fast-break points for the season and split. |
| `opp_pts_paint_rank` | Int64 | League rank of the row's opponent points in the paint for the season and split. |
| `pts_off_tov` | Float64 | Points scored off opponent turnovers over the split. |
| `pts_2nd_chance` | Float64 | Second-chance points over the split. |
| `pts_fb` | Float64 | Fast-break points over the split. |
| `pts_paint` | Float64 | Points in the paint over the split. |
| `pts_off_tov_rank` | Int64 | League rank of the row's points scored off opponent turnovers for the season and split. |
| `pts_2nd_chance_rank` | Int64 | League rank of the row's second-chance points for the season and split. |
| `pts_fb_rank` | Int64 | League rank of the row's fast-break points for the season and split. |
| `pts_paint_rank` | Int64 | League rank of the row's points in the paint for the season and split. |
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
load_nba_stats_team_season_stats(seasons=2025)
```
