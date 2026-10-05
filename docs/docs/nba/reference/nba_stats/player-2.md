---
title: "NBA — NBA Stats API (stats.nba.com) — Player: playerprofilev2–playervsplayer"
sidebar_label: "Player: playerprofilev2–playervsplayer"
sidebar_position: 11
description: "NBA — NBA Stats API (stats.nba.com) — Player: playerprofilev2–playervsplayer — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Player: playerprofilev2–playervsplayer

## nba_stats_playerprofilev2

GET /stats/playerprofilev2

**Endpoint URL:** `GET https://stats.nba.com/stats/playerprofilev2`

**Valid URL:** [https://stats.nba.com/stats/playerprofilev2?LeagueID=00](https://stats.nba.com/stats/playerprofilev2?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode36` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#nba_stats_playerprofilev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`SeasonTotalsRegularSeason`, `CareerTotalsRegularSeason`, `SeasonTotalsPostSeason`, `CareerTotalsPostSeason`, `SeasonTotalsAllStarSeason`, `CareerTotalsAllStarSeason`, `SeasonTotalsCollegeSeason`, `CareerTotalsCollegeSeason`, `SeasonTotalsPreseason`, `CareerTotalsPreseason`, `SeasonRankingsRegularSeason`, `SeasonRankingsPostSeason`, `SeasonHighs`, `CareerHighs`, `NextGame`) (one table per key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**SeasonTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**CareerTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**SeasonTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**CareerTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**SeasonTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**CareerTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**SeasonTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `organization_id` | character |  |
| `school_name` | character | School name. |
| `player_age` | character | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
| `min` | character | Minutes played. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |
| `fg3_m` | character | Three-point field goals made. |
| `fg3_a` | character | Three-point field goal attempts. |
| `fg3_pct` | character | Three-point field goal percentage (0-1). |
| `ftm` | character | Free throws made. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | character | Free throw percentage (0-1). |
| `oreb` | character | Offensive rebounds. |
| `dreb` | character | Defensive rebounds. |
| `reb` | character | Rebounds per game. |
| `ast` | character | Assists. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `tov` | character | Turnovers. |
| `pf` | character | Personal fouls. |
| `pts` | character | Points scored. |

**CareerTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `organization_id` | character |  |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
| `min` | character | Minutes played. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |
| `fg3_m` | character | Three-point field goals made. |
| `fg3_a` | character | Three-point field goal attempts. |
| `fg3_pct` | character | Three-point field goal percentage (0-1). |
| `ftm` | character | Free throws made. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | character | Free throw percentage (0-1). |
| `oreb` | character | Offensive rebounds. |
| `dreb` | character | Defensive rebounds. |
| `reb` | character | Rebounds per game. |
| `ast` | character | Assists. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `tov` | character | Turnovers. |
| `pf` | character | Personal fouls. |
| `pts` | character | Points scored. |

**SeasonTotalsPreseason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**CareerTotalsPreseason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**SeasonRankingsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | character | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
| `rank_pg_min` | integer |  |
| `rank_pg_fgm` | integer |  |
| `rank_pg_fga` | integer |  |
| `rank_fg_pct` | integer |  |
| `rank_pg_fg3_m` | integer |  |
| `rank_pg_fg3_a` | integer |  |
| `rank_fg3_pct` | integer |  |
| `rank_pg_ftm` | integer |  |
| `rank_pg_fta` | integer |  |
| `rank_ft_pct` | integer |  |
| `rank_pg_oreb` | integer |  |
| `rank_pg_dreb` | integer |  |
| `rank_pg_reb` | integer |  |
| `rank_pg_ast` | integer |  |
| `rank_pg_stl` | integer |  |
| `rank_pg_blk` | integer |  |
| `rank_pg_tov` | integer |  |
| `rank_pg_pts` | integer |  |
| `rank_pg_eff` | integer |  |

**SeasonRankingsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | character | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
| `rank_pg_min` | integer |  |
| `rank_pg_fgm` | integer |  |
| `rank_pg_fga` | integer |  |
| `rank_fg_pct` | integer |  |
| `rank_pg_fg3_m` | integer |  |
| `rank_pg_fg3_a` | integer |  |
| `rank_fg3_pct` | integer |  |
| `rank_pg_ftm` | integer |  |
| `rank_pg_fta` | integer |  |
| `rank_ft_pct` | integer |  |
| `rank_pg_oreb` | integer |  |
| `rank_pg_dreb` | integer |  |
| `rank_pg_reb` | integer |  |
| `rank_pg_ast` | integer |  |
| `rank_pg_stl` | integer |  |
| `rank_pg_blk` | integer |  |
| `rank_pg_tov` | integer |  |
| `rank_pg_pts` | integer |  |
| `rank_pg_eff` | integer |  |

**SeasonHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_name` | character |  |
| `vs_team_abbreviation` | character |  |
| `stat` | character | Stat. |
| `stat_value` | integer | Stat value. |
| `stat_order` | integer |  |
| `date_est` | character |  |

**CareerHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_name` | character |  |
| `vs_team_abbreviation` | character |  |
| `stat` | character | Stat. |
| `stat_value` | integer | Stat value. |
| `stat_order` | integer |  |
| `date_est` | character |  |

**NextGame**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `game_time` | character | Game start time. |
| `location` | character | Location. |
| `player_team_id` | integer | Unique team identifier of the primary player. |
| `player_team_city` | character |  |
| `player_team_nickname` | character |  |
| `player_team_abbreviation` | character |  |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_nickname` | character |  |
| `vs_team_abbreviation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playerprofilev2-example}

```python
nba_stats_playerprofilev2(league_id='00')
```

_Last validated n/a._

## nba_stats_playervsplayer

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
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2025-26``. Defaults to the current season at call time, as hoopR does (``2026-27`` from October 2026); stats.nba.com answers a request without a season with an empty HTTP 500. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerID` | `vs_player_id` |  |  | `Y` |  |

### Returns {#nba_stats_playervsplayer-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `OnOffCourt`, `ShotDistanceOverall`, `ShotDistanceOnCourt`, `ShotDistanceOffCourt`, `ShotAreaOverall`, `ShotAreaOnCourt`, `ShotAreaOffCourt`, `PlayerInfo`, `VsPlayerInfo`) (one table per key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**Overall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
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
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |

**OnOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
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
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |

**ShotDistanceOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotDistanceOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotDistanceOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotAreaOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotAreaOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotAreaOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**PlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `display_first_last` | character |  |
| `display_last_comma_first` | character |  |
| `display_fi_last` | character |  |
| `birthdate` | character | Date of birth. |
| `school` | character | Player school / pre-draft team. |
| `country` | character | Venue country. |
| `last_affiliation` | character |  |

**VsPlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `display_first_last` | character |  |
| `display_last_comma_first` | character |  |
| `display_fi_last` | character |  |
| `birthdate` | character | Date of birth. |
| `school` | character | Player school / pre-draft team. |
| `country` | character | Venue country. |
| `last_affiliation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playervsplayer-example}

```python
nba_stats_playervsplayer(league_id='00')
```

_Last validated n/a._
