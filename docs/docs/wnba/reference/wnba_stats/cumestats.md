---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Cumulative stats"
sidebar_label: "Cumulative stats"
sidebar_position: 4
description: "WNBA — WNBA Stats API (stats.wnba.com) — Cumulative stats — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Cumulative stats

## wnba_stats_cumestatsplayer

GET /stats/cumestatsplayer

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsplayer`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsplayer?GameIDs=1022200018&LeagueID=10&PlayerID=204319&Season=2021-22&SeasonType=Regular+Season](https://stats.wnba.com/stats/cumestatsplayer?GameIDs=1022200018&LeagueID=10&PlayerID=204319&Season=2021-22&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameIDs` | `game_ids` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |

### Returns {#wnba_stats_cumestatsplayer-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameByGameStats`, `TotalPlayerStats`) (one table per key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**GameByGameStats**

| col_name | type | description |
|---|---|---|
| `date_est` | character |  |
| `visitor_team` | character |  |
| `home_team` | character | Home team name. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `actual_minutes` | integer | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | integer | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | integer | Field goals made over the aggregated games. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3` | integer | Three-point field goals made over the aggregated games. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft` | integer | Free throws made over the aggregated games. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `off_reb` | integer | Offensive rebounds over the aggregated games. |
| `def_reb` | integer | Defensive rebounds over the aggregated games. |
| `tot_reb` | integer | Total rebounds over the aggregated games. |
| `avg_tot_reb` | numeric | Average total rebounds per game over the aggregated games. |
| `ast` | integer | Assists. |
| `pf` | integer | Personal fouls. |
| `dq` | integer | Disqualifications (fouled out) over the aggregated games. |
| `stl` | integer | Steals. |
| `turnovers` | integer | Total turnovers. |
| `blk` | integer | Blocks. |
| `pts` | integer | Points scored. |
| `avg_pts` | numeric | Average points per game over the aggregated games. |

**TotalPlayerStats**

| col_name | type | description |
|---|---|---|
| `display_fi_last` | character | Abbreviated player name (first initial and last name). |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `jersey_num` | character | Jersey number worn by the player. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `actual_minutes` | integer | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | integer | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | integer | Field goals made over the aggregated games. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3` | integer | Three-point field goals made over the aggregated games. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft` | integer | Free throws made over the aggregated games. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `off_reb` | integer | Offensive rebounds over the aggregated games. |
| `def_reb` | integer | Defensive rebounds over the aggregated games. |
| `tot_reb` | integer | Total rebounds over the aggregated games. |
| `ast` | integer | Assists. |
| `pf` | integer | Personal fouls. |
| `dq` | integer | Disqualifications (fouled out) over the aggregated games. |
| `stl` | integer | Steals. |
| `turnovers` | integer | Total turnovers. |
| `blk` | integer | Blocks. |
| `pts` | integer | Points scored. |
| `max_actual_minutes` | integer | Most whole minutes played in any single aggregated game. |
| `max_actual_seconds` | integer | Seconds component paired with the single-game maximum minutes. |
| `max_reb` | integer | Most rebounds recorded in any single aggregated game. |
| `max_ast` | integer | Most assists recorded in any single aggregated game. |
| `max_stl` | integer | Most steals recorded in any single aggregated game. |
| `max_turnovers` | integer | Most turnovers recorded in any single aggregated game. |
| `max_blk` | integer | Most blocked shots recorded in any single aggregated game. |
| `max_pts` | integer | Most points recorded in any single aggregated game. |
| `avg_actual_minutes` | integer | Average whole minutes played per aggregated game. |
| `avg_actual_seconds` | numeric | Average seconds component of playing time per aggregated game. |
| `avg_tot_reb` | numeric | Average total rebounds per game over the aggregated games. |
| `avg_ast` | numeric | Average assists per game over the aggregated games. |
| `avg_stl` | numeric | Average steals per game over the aggregated games. |
| `avg_turnovers` | numeric | Average turnovers per game over the aggregated games. |
| `avg_blk` | numeric | Average blocked shots per game over the aggregated games. |
| `avg_pts` | numeric | Average points per game over the aggregated games. |
| `per_min_tot_reb` | numeric | Total rebounds per minute played over the aggregated games. |
| `per_min_ast` | numeric | Assists per minute played over the aggregated games. |
| `per_min_stl` | numeric | Steals per minute played over the aggregated games. |
| `per_min_turnovers` | numeric | Turnovers per minute played over the aggregated games. |
| `per_min_blk` | numeric | Blocked shots per minute played over the aggregated games. |
| `per_min_pts` | numeric | Points per minute played over the aggregated games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_cumestatsplayer-example}

```python
wnba_stats_cumestatsplayer(league_id='10')
```

_Last validated n/a._

## wnba_stats_cumestatsplayergames

GET /stats/cumestatsplayergames

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsplayergames`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsplayergames?LeagueID=10&Location=&Outcome=&PlayerID=204319&Season=2021-22&SeasonType=Regular+Season&VsConference=&VsDivision=&VsTeamID=0](https://stats.wnba.com/stats/cumestatsplayergames?LeagueID=10&Location=&Outcome=&PlayerID=204319&Season=2021-22&SeasonType=Regular+Season&VsConference=&VsDivision=&VsTeamID=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_cumestatsplayergames-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `matchup` | character | Matchup. |
| `game_id` | character | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_cumestatsplayergames-example}

```python
wnba_stats_cumestatsplayergames(league_id='10')
```

_Last validated n/a._

## wnba_stats_cumestatsteam

GET /stats/cumestatsteam

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsteam`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsteam?GameIDs=1022200018&LeagueID=10&Season=2021-22&SeasonType=Regular+Season&TeamID=1611661317](https://stats.wnba.com/stats/cumestatsteam?GameIDs=1022200018&LeagueID=10&Season=2021-22&SeasonType=Regular+Season&TeamID=1611661317)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameIDs` | `game_ids` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_cumestatsteam-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameByGameStats`, `TotalTeamStats`) (one table per key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
**GameByGameStats**

| col_name | type | description |
|---|---|---|
| `jersey_num` | character | Jersey number worn by the player. |
| `player` | character | Player name. |
| `person_id` | character | Unique player identifier (V3 endpoints). |
| `team_id` | character | Unique team identifier. |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
| `actual_minutes` | character | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | character | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | character | Field goals made over the aggregated games. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |
| `fg3` | character | Three-point field goals made over the aggregated games. |
| `fg3_a` | character | Three-point field goal attempts. |
| `fg3_pct` | character | Three-point field goal percentage (0-1). |
| `ft` | character | Free throws made over the aggregated games. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | character | Free throw percentage (0-1). |
| `off_reb` | character | Offensive rebounds over the aggregated games. |
| `def_reb` | character | Defensive rebounds over the aggregated games. |
| `tot_reb` | character | Total rebounds over the aggregated games. |
| `ast` | character | Assists. |
| `pf` | character | Personal fouls. |
| `dq` | character | Disqualifications (fouled out) over the aggregated games. |
| `stl` | character | Steals. |
| `turnovers` | character | Total turnovers. |
| `blk` | character | Blocks. |
| `pts` | character | Points scored. |
| `max_actual_minutes` | character | Most whole minutes played in any single aggregated game. |
| `max_actual_seconds` | character | Seconds component paired with the single-game maximum minutes. |
| `max_reb` | character | Most rebounds recorded in any single aggregated game. |
| `max_ast` | character | Most assists recorded in any single aggregated game. |
| `max_stl` | character | Most steals recorded in any single aggregated game. |
| `max_turnovers` | character | Most turnovers recorded in any single aggregated game. |
| `max_blkp` | character | Most blocked shots recorded in any single aggregated game. |
| `max_pts` | character | Most points recorded in any single aggregated game. |
| `avg_actual_minutes` | character | Average whole minutes played per aggregated game. |
| `avg_actual_seconds` | character | Average seconds component of playing time per aggregated game. |
| `avg_reb` | character | Average rebounds per game over the aggregated games. |
| `avg_ast` | character | Average assists per game over the aggregated games. |
| `avg_stl` | character | Average steals per game over the aggregated games. |
| `avg_turnovers` | character | Average turnovers per game over the aggregated games. |
| `avg_blkp` | character | Average blocked shots per game over the aggregated games. |
| `avg_pts` | character | Average points per game over the aggregated games. |
| `per_min_reb` | character | Rebounds per minute played over the aggregated games. |
| `per_min_ast` | character | Assists per minute played over the aggregated games. |
| `per_min_stl` | character | Steals per minute played over the aggregated games. |
| `per_min_turnovers` | character | Turnovers per minute played over the aggregated games. |
| `per_min_blk` | character | Blocked shots per minute played over the aggregated games. |
| `per_min_pts` | character | Points per minute played over the aggregated games. |

**TotalTeamStats**

| col_name | type | description |
|---|---|---|
| `city` | character | Venue city. |
| `nickname` | character | Team or athlete nickname. |
| `team_id` | integer | Unique team identifier. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_home` | integer |  |
| `l_home` | integer |  |
| `w_road` | integer |  |
| `l_road` | integer |  |
| `team_turnovers` | integer | Team turnovers (turnovers credited to the team rather than a player). |
| `team_rebounds` | integer | Team rebounds (rebounds credited to the team rather than a player). |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
| `actual_minutes` | integer | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | integer | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | integer | Field goals made over the aggregated games. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3` | integer | Three-point field goals made over the aggregated games. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft` | integer | Free throws made over the aggregated games. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `off_reb` | integer | Offensive rebounds over the aggregated games. |
| `def_reb` | integer | Defensive rebounds over the aggregated games. |
| `tot_reb` | integer | Total rebounds over the aggregated games. |
| `ast` | integer | Assists. |
| `pf` | integer | Personal fouls. |
| `stl` | integer | Steals. |
| `total_turnovers` | integer | Total turnovers (player + team). |
| `blk` | integer | Blocks. |
| `pts` | integer | Points scored. |
| `avg_reb` | numeric | Average rebounds per game over the aggregated games. |
| `avg_pts` | numeric | Average points per game over the aggregated games. |
| `dq` | integer | Disqualifications (fouled out) over the aggregated games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_cumestatsteam-example}

```python
wnba_stats_cumestatsteam(league_id='10')
```

_Last validated n/a._

## wnba_stats_cumestatsteamgames

GET /stats/cumestatsteamgames

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsteamgames`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsteamgames?LeagueID=10&Location=&Outcome=&Season=2021-22&SeasonID=&SeasonType=Regular+Season&TeamID=1611661317&VsConference=&VsDivision=&VsTeamID=0](https://stats.wnba.com/stats/cumestatsteamgames?LeagueID=10&Location=&Outcome=&Season=2021-22&SeasonID=&SeasonType=Regular+Season&TeamID=1611661317&VsConference=&VsDivision=&VsTeamID=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonID` | `season_id_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_cumestatsteamgames-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `matchup` | character | Matchup. |
| `game_id` | character | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_cumestatsteamgames-example}

```python
wnba_stats_cumestatsteamgames(league_id='10')
```

_Last validated n/a._
