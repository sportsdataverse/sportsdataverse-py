---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Player"
sidebar_label: "Player"
sidebar_position: 7
description: "WNBA — WNBA Stats API (stats.wnba.com) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Player

## wnba_stats_playerawards

GET /stats/playerawards

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerawards`

**Valid URL:** [https://stats.wnba.com/stats/playerawards](https://stats.wnba.com/stats/playerawards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_playerawards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `team` | character | Team-side label or team identifier. |
| `description` | character | Long-form description text. |
| `all_nba_team_number` | character | NBA or WNBA Stats value for all NBA team number in the playerawards result set. |
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `month` | character | NBA or WNBA Stats value for month in the playerawards result set. |
| `week` | character | Week number. |
| `conference` | character | Filter players or teams by conference. |
| `type` | character | Record type / category. |
| `subtype1` | character | NBA or WNBA Stats value for subtype1 in the playerawards result set. |
| `subtype2` | character | NBA or WNBA Stats value for subtype2 in the playerawards result set. |
| `subtype3` | character | NBA or WNBA Stats value for subtype3 in the playerawards result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playerawards-example}

```python
wnba_stats_playerawards()
```

_Last validated n/a._

## wnba_stats_playercareerbycollegerollup

GET /stats/playercareerbycollegerollup

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercareerbycollegerollup`

**Valid URL:** [https://stats.wnba.com/stats/playercareerbycollegerollup?LeagueID=10](https://stats.wnba.com/stats/playercareerbycollegerollup?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |

### Returns {#wnba_stats_playercareerbycollegerollup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `region` | character | Region label. |
| `seed` | character | Region seed slot of the college in the stats API's career-by-college rollup grid. |
| `college` | character | College or school attended. |
| `players` | character | Nested list of per-player box scores. |
| `gp` | integer | Games played. |
| `min` | integer | Minutes played. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | character | Three-point field goals made. |
| `fg3a` | character | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | character | Free throws made. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | character | Offensive rebounds. |
| `dreb` | character | Defensive rebounds. |
| `reb` | character | Total rebounds. |
| `ast` | character | Assists. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `tov` | character | Turnovers. |
| `pf` | character | Personal fouls. |
| `pts` | character | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playercareerbycollegerollup-example}

```python
wnba_stats_playercareerbycollegerollup(league_id='10')
```

_Last validated n/a._

## wnba_stats_playercareerstats

GET /stats/playercareerstats

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercareerstats`

**Valid URL:** [https://stats.wnba.com/stats/playercareerstats?LeagueID=10](https://stats.wnba.com/stats/playercareerstats?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode36` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_playercareerstats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playercareerstats-example}

```python
wnba_stats_playercareerstats(league_id='10')
```

_Last validated n/a._

## wnba_stats_playercompare

GET /stats/playercompare

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercompare`

**Valid URL:** [https://stats.wnba.com/stats/playercompare?LeagueID=10](https://stats.wnba.com/stats/playercompare?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
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
| `PlayerIDList` | `player_id_list` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerIDList` | `vs_player_id_list` |  |  | `Y` |  |

### Returns {#wnba_stats_playercompare-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `description` | character | Long-form description text. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playercompare-example}

```python
wnba_stats_playercompare(league_id='10')
```

_Last validated n/a._

## wnba_stats_playerestimatedmetrics

GET /stats/playerestimatedmetrics

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerestimatedmetrics`

**Valid URL:** [https://stats.wnba.com/stats/playerestimatedmetrics?LeagueID=10](https://stats.wnba.com/stats/playerestimatedmetrics?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |

### Returns {#wnba_stats_playerestimatedmetrics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `e_off_rating` | numeric | Estimated offensive rating for the requested NBA or WNBA Stats split. |
| `e_def_rating` | numeric | Estimated defensive rating for the requested NBA or WNBA Stats split. |
| `e_net_rating` | numeric | Estimated net rating for the requested NBA or WNBA Stats split. |
| `e_ast_ratio` | numeric | Estimated assist ratio for the requested NBA or WNBA Stats split. |
| `e_oreb_pct` | numeric | Estimated offensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_dreb_pct` | numeric | Estimated defensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_reb_pct` | numeric | Estimated rebound percentage for the requested NBA or WNBA Stats split. |
| `e_tov_pct` | numeric | Estimated turnovers percentage for the requested NBA or WNBA Stats split. |
| `e_usg_pct` | numeric | Estimated usage percentage for the requested NBA or WNBA Stats split. |
| `e_pace` | numeric | Estimated pace for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_off_rating_rank` | integer | Rank for e offensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_def_rating_rank` | integer | Rank for e defensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_net_rating_rank` | integer | Rank for e net rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_ast_ratio_rank` | integer | Rank for e assists ratio within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_oreb_pct_rank` | integer | Rank for e offensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_dreb_pct_rank` | integer | Rank for e defensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_reb_pct_rank` | integer | Rank for e rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_tov_pct_rank` | integer | Rank for e turnovers percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_usg_pct_rank` | integer | Rank for e usage percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_pace_rank` | integer | Rank for e pace within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playerestimatedmetrics-example}

```python
wnba_stats_playerestimatedmetrics(league_id='10')
```

_Last validated n/a._

## wnba_stats_playerfantasyprofile

GET /stats/playerfantasyprofile

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerfantasyprofile`

**Valid URL:** [https://stats.wnba.com/stats/playerfantasyprofile?LeagueID=10](https://stats.wnba.com/stats/playerfantasyprofile?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |

### Returns {#wnba_stats_playerfantasyprofile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `season_year` | character | Season year string ('YYYY-YY' format). |
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
| `dd2` | integer | Double-doubles recorded over the split. |
| `td3` | integer | Triple-doubles recorded over the split. |
| `fan_duel_pts` | numeric | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playerfantasyprofile-example}

```python
wnba_stats_playerfantasyprofile(league_id='10')
```

_Last validated n/a._

## wnba_stats_playerfantasyprofilebargraph

GET /stats/playerfantasyprofilebargraph

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerfantasyprofilebargraph`

**Valid URL:** [https://stats.wnba.com/stats/playerfantasyprofilebargraph?LeagueID=10](https://stats.wnba.com/stats/playerfantasyprofilebargraph?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_playerfantasyprofilebargraph-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `fan_duel_pts` | numeric | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `fg3m` | numeric | Three-point field goals made. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playerfantasyprofilebargraph-example}

```python
wnba_stats_playerfantasyprofilebargraph(league_id='10')
```

_Last validated n/a._

## wnba_stats_playergamelog

GET /stats/playergamelog

**Endpoint URL:** `GET https://stats.wnba.com/stats/playergamelog`

**Valid URL:** [https://stats.wnba.com/stats/playergamelog?LeagueID=10](https://stats.wnba.com/stats/playergamelog?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |

### Returns {#wnba_stats_playergamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `player_id` | integer | Unique player identifier. |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `min` | integer | Minutes played. |
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
| `reb` | integer | Total rebounds. |
| `ast` | integer | Assists. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `tov` | integer | Turnovers. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |
| `video_available` | integer | Video available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playergamelog-example}

```python
wnba_stats_playergamelog(league_id='10')
```

_Last validated n/a._

## wnba_stats_playergamelogs

GET /stats/playergamelogs

**Endpoint URL:** `GET https://stats.wnba.com/stats/playergamelogs`

**Valid URL:** [https://stats.wnba.com/stats/playergamelogs?LeagueID=10](https://stats.wnba.com/stats/playergamelogs?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_player_game_logs_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OppTeamID` | `oppteamid` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_playergamelogs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `nickname` | character | Team or athlete nickname. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
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
| `nba_fantasy_pts_rank` | integer | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | integer | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | integer | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | integer | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `available_flag` | integer | Flag indicating whether the requested NBA or WNBA Stats video or data asset is available. |
| `min_sec` | character | Minutes and seconds played, formatted as a game-clock duration string. |
| `team_count` | integer | NBA or WNBA Stats value for team count in the playergamelogs result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playergamelogs-example}

```python
wnba_stats_playergamelogs(league_id='10')
```

_Last validated n/a._

## wnba_stats_playergamestreakfinder

GET /stats/playergamestreakfinder

**Endpoint URL:** `GET https://stats.wnba.com/stats/playergamestreakfinder`

**Valid URL:** [https://stats.wnba.com/stats/playergamestreakfinder?LeagueID=10](https://stats.wnba.com/stats/playergamestreakfinder?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ActiveStreaksOnly` | `active_streaks_only_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftNumber` | `draft_number_nullable` |  |  | `Y` |  |
| `DraftRound` | `draft_round_nullable` |  |  | `Y` |  |
| `DraftTeamID` | `draft_team_id_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `EqAST` | `eq_ast_nullable` |  |  | `Y` |  |
| `EqBLK` | `eq_blk_nullable` |  |  | `Y` |  |
| `EqDD` | `eq_dd_nullable` |  |  | `Y` |  |
| `EqDREB` | `eq_dreb_nullable` |  |  | `Y` |  |
| `EqFG3A` | `eq_fg3a_nullable` |  |  | `Y` |  |
| `EqFG3M` | `eq_fg3m_nullable` |  |  | `Y` |  |
| `EqFG3_PCT` | `eq_fg3_pct_nullable` |  |  | `Y` |  |
| `EqFGA` | `eq_fga_nullable` |  |  | `Y` |  |
| `EqFGM` | `eq_fgm_nullable` |  |  | `Y` |  |
| `EqFG_PCT` | `eq_fg_pct_nullable` |  |  | `Y` |  |
| `EqFTA` | `eq_fta_nullable` |  |  | `Y` |  |
| `EqFTM` | `eq_ftm_nullable` |  |  | `Y` |  |
| `EqFT_PCT` | `eq_ft_pct_nullable` |  |  | `Y` |  |
| `EqMINUTES` | `eq_minutes_nullable` |  |  | `Y` |  |
| `EqOREB` | `eq_oreb_nullable` |  |  | `Y` |  |
| `EqPF` | `eq_pf_nullable` |  |  | `Y` |  |
| `EqPTS` | `eq_pts_nullable` |  |  | `Y` |  |
| `EqREB` | `eq_reb_nullable` |  |  | `Y` |  |
| `EqSTL` | `eq_stl_nullable` |  |  | `Y` |  |
| `EqTD` | `eq_td_nullable` |  |  | `Y` |  |
| `EqTOV` | `eq_tov_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GtAST` | `gt_ast_nullable` |  |  | `Y` |  |
| `GtBLK` | `gt_blk_nullable` |  |  | `Y` |  |
| `GtDD` | `gt_dd_nullable` |  |  | `Y` |  |
| `GtDREB` | `gt_dreb_nullable` |  |  | `Y` |  |
| `GtFG3A` | `gt_fg3a_nullable` |  |  | `Y` |  |
| `GtFG3M` | `gt_fg3m_nullable` |  |  | `Y` |  |
| `GtFG3_PCT` | `gt_fg3_pct_nullable` |  |  | `Y` |  |
| `GtFGA` | `gt_fga_nullable` |  |  | `Y` |  |
| `GtFGM` | `gt_fgm_nullable` |  |  | `Y` |  |
| `GtFG_PCT` | `gt_fg_pct_nullable` |  |  | `Y` |  |
| `GtFTA` | `gt_fta_nullable` |  |  | `Y` |  |
| `GtFTM` | `gt_ftm_nullable` |  |  | `Y` |  |
| `GtFT_PCT` | `gt_ft_pct_nullable` |  |  | `Y` |  |
| `GtMINUTES` | `gt_minutes_nullable` |  |  | `Y` |  |
| `GtOREB` | `gt_oreb_nullable` |  |  | `Y` |  |
| `GtPF` | `gt_pf_nullable` |  |  | `Y` |  |
| `GtPTS` | `gt_pts_nullable` |  |  | `Y` |  |
| `GtREB` | `gt_reb_nullable` |  |  | `Y` |  |
| `GtSTL` | `gt_stl_nullable` |  |  | `Y` |  |
| `GtTD` | `gt_td_nullable` |  |  | `Y` |  |
| `GtTOV` | `gt_tov_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `LtAST` | `lt_ast_nullable` |  |  | `Y` |  |
| `LtBLK` | `lt_blk_nullable` |  |  | `Y` |  |
| `LtDD` | `lt_dd_nullable` |  |  | `Y` |  |
| `LtDREB` | `lt_dreb_nullable` |  |  | `Y` |  |
| `LtFG3A` | `lt_fg3a_nullable` |  |  | `Y` |  |
| `LtFG3M` | `lt_fg3m_nullable` |  |  | `Y` |  |
| `LtFG3_PCT` | `lt_fg3_pct_nullable` |  |  | `Y` |  |
| `LtFGA` | `lt_fga_nullable` |  |  | `Y` |  |
| `LtFGM` | `lt_fgm_nullable` |  |  | `Y` |  |
| `LtFG_PCT` | `lt_fg_pct_nullable` |  |  | `Y` |  |
| `LtFTA` | `lt_fta_nullable` |  |  | `Y` |  |
| `LtFTM` | `lt_ftm_nullable` |  |  | `Y` |  |
| `LtFT_PCT` | `lt_ft_pct_nullable` |  |  | `Y` |  |
| `LtMINUTES` | `lt_minutes_nullable` |  |  | `Y` |  |
| `LtOREB` | `lt_oreb_nullable` |  |  | `Y` |  |
| `LtPF` | `lt_pf_nullable` |  |  | `Y` |  |
| `LtPTS` | `lt_pts_nullable` |  |  | `Y` |  |
| `LtREB` | `lt_reb_nullable` |  |  | `Y` |  |
| `LtSTL` | `lt_stl_nullable` |  |  | `Y` |  |
| `LtTD` | `lt_td_nullable` |  |  | `Y` |  |
| `LtTOV` | `lt_tov_nullable` |  |  | `Y` |  |
| `MinGames` | `min_games_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |
| `YearsExperience` | `years_experience_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_playergamestreakfinder-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `player_id` | integer | Unique player identifier. |
| `gamestreak` | integer | NBA or WNBA Stats value for gamestreak in the playergamestreakfinder result set. |
| `startdate` | character | Date or timestamp for startdate in the NBA or WNBA Stats result set. |
| `enddate` | character | Date or timestamp for enddate in the NBA or WNBA Stats result set. |
| `activestreak` | integer | NBA or WNBA Stats value for activestreak in the playergamestreakfinder result set. |
| `numseasons` | integer | NBA or WNBA Stats value for numseasons in the playergamestreakfinder result set. |
| `lastseason` | character | NBA or WNBA Stats value for lastseason in the playergamestreakfinder result set. |
| `firstseason` | character | NBA or WNBA Stats value for firstseason in the playergamestreakfinder result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playergamestreakfinder-example}

```python
wnba_stats_playergamestreakfinder(league_id='10')
```

_Last validated n/a._

## wnba_stats_playerindex

GET /stats/playerindex

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerindex`

**Valid URL:** [https://stats.wnba.com/stats/playerindex?LeagueID=10](https://stats.wnba.com/stats/playerindex?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Active` | `active_nullable` |  |  | `Y` |  |
| `AllStar` | `allstar_nullable` |  |  | `Y` |  |
| `College` | `college_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftRound` | `draft_round_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `Historical` | `historical_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_playerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_last_name` | character | Participant last name. |
| `player_first_name` | character | Participant first name. |
| `player_slug` | character | URL-safe player identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `is_defunct` | integer | Flag indicating is defunct for the requested NBA or WNBA Stats context. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `jersey_number` | character | Jersey number. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `college` | character | College or school attended. |
| `country` | character | Country (full name or code). |
| `draft_year` | integer | Draft year (4-digit). |
| `draft_round` | integer | Round of the draft selection. |
| `draft_number` | integer | The number pick that was used to select a given player. |
| `roster_status` | numeric | Payroll table the row came from: Active, IL, or Retained Salary. |
| `from_year` | character | First season. |
| `to_year` | character | Most recent season. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `stats_timeframe` | character | Time value for stats timeframe in the NBA or WNBA Stats result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playerindex-example}

```python
wnba_stats_playerindex(league_id='10')
```

_Last validated n/a._

## wnba_stats_playernextngames

GET /stats/playernextngames

**Endpoint URL:** `GET https://stats.wnba.com/stats/playernextngames`

**Valid URL:** [https://stats.wnba.com/stats/playernextngames?LeagueID=10](https://stats.wnba.com/stats/playernextngames?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `NumberOfGames` | `number_of_games` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season_all` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |

### Returns {#wnba_stats_playernextngames-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `home_team_name` | character | Home team name. |
| `visitor_team_name` | character | Full name of the visiting team in the upcoming game. |
| `home_team_abbreviation` | character | Home team abbreviation; `team_detail = TRUE` only. |
| `visitor_team_abbreviation` | character | Abbreviation of the visiting team in the upcoming game. |
| `home_team_nickname` | character | Home team nickname label; `team_detail = TRUE` only. |
| `visitor_team_nickname` | character | Nickname of the visiting team in the upcoming game. |
| `game_time` | character | Game start time. |
| `home_wl` | character | Home team's win-loss record entering the upcoming game. |
| `visitor_wl` | character | Visiting team's win-loss record entering the upcoming game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playernextngames-example}

```python
wnba_stats_playernextngames(league_id='10')
```

_Last validated n/a._

## wnba_stats_playerprofilev2

GET /stats/playerprofilev2

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerprofilev2`

**Valid URL:** [https://stats.wnba.com/stats/playerprofilev2?LeagueID=10](https://stats.wnba.com/stats/playerprofilev2?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode36` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_playerprofilev2-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playerprofilev2-example}

```python
wnba_stats_playerprofilev2(league_id='10')
```

_Last validated n/a._

## wnba_stats_playervsplayer

GET /stats/playervsplayer

**Endpoint URL:** `GET https://stats.wnba.com/stats/playervsplayer`

**Valid URL:** [https://stats.wnba.com/stats/playervsplayer?LeagueID=10](https://stats.wnba.com/stats/playervsplayer?LeagueID=10)

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
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerID` | `vs_player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_playervsplayer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
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
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playervsplayer-example}

```python
wnba_stats_playervsplayer(league_id='10')
```

_Last validated n/a._
