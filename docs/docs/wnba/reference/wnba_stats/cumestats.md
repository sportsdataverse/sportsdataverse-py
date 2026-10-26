---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Cumulative stats"
sidebar_label: "Cumulative stats"
sidebar_position: 4
description: "WNBA — WNBA Stats API (stats.wnba.com) — Cumulative stats — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Cumulative stats

## wnba_stats_cumestatsplayer

Cume stats player.

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsplayer`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsplayer?GameIDs=1022200018&LeagueID=10&PlayerID=204319&Season=2021-22&SeasonType=Regular+Season](https://stats.wnba.com/stats/cumestatsplayer?GameIDs=1022200018&LeagueID=10&PlayerID=204319&Season=2021-22&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameIDs` | `game_ids` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |

### Returns {#wnba_stats_cumestatsplayer-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameByGameStats`, `TotalPlayerStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GameByGameStats**

| col_name | type | description |
|---|---|---|
| `date_est` | String |  |
| `visitor_team` | String |  |
| `home_team` | String | Home team name. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `actual_minutes` | Int64 | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | Int64 | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | Int64 | Field goals made over the aggregated games. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3` | Int64 | Three-point field goals made over the aggregated games. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ft` | Int64 | Free throws made over the aggregated games. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `off_reb` | Int64 | Offensive rebounds over the aggregated games. |
| `def_reb` | Int64 | Defensive rebounds over the aggregated games. |
| `tot_reb` | Int64 | Total rebounds over the aggregated games. |
| `avg_tot_reb` | Float64 | Average total rebounds per game over the aggregated games. |
| `ast` | Int64 | Assists. |
| `pf` | Int64 | Personal fouls. |
| `dq` | Int64 | Disqualifications (fouled out) over the aggregated games. |
| `stl` | Int64 | Steals. |
| `turnovers` | Int64 | Total turnovers. |
| `blk` | Int64 | Blocks. |
| `pts` | Int64 | Points scored. |
| `avg_pts` | Float64 | Average points per game over the aggregated games. |

**TotalPlayerStats**

| col_name | type | description |
|---|---|---|
| `display_fi_last` | String | Abbreviated player name (first initial and last name). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `jersey_num` | String | Jersey number worn by the player. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `actual_minutes` | Int64 | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | Int64 | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | Int64 | Field goals made over the aggregated games. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3` | Int64 | Three-point field goals made over the aggregated games. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ft` | Int64 | Free throws made over the aggregated games. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `off_reb` | Int64 | Offensive rebounds over the aggregated games. |
| `def_reb` | Int64 | Defensive rebounds over the aggregated games. |
| `tot_reb` | Int64 | Total rebounds over the aggregated games. |
| `ast` | Int64 | Assists. |
| `pf` | Int64 | Personal fouls. |
| `dq` | Int64 | Disqualifications (fouled out) over the aggregated games. |
| `stl` | Int64 | Steals. |
| `turnovers` | Int64 | Total turnovers. |
| `blk` | Int64 | Blocks. |
| `pts` | Int64 | Points scored. |
| `max_actual_minutes` | Int64 | Most whole minutes played in any single aggregated game. |
| `max_actual_seconds` | Int64 | Seconds component paired with the single-game maximum minutes. |
| `max_reb` | Int64 | Most rebounds recorded in any single aggregated game. |
| `max_ast` | Int64 | Most assists recorded in any single aggregated game. |
| `max_stl` | Int64 | Most steals recorded in any single aggregated game. |
| `max_turnovers` | Int64 | Most turnovers recorded in any single aggregated game. |
| `max_blk` | Int64 | Most blocked shots recorded in any single aggregated game. |
| `max_pts` | Int64 | Most points recorded in any single aggregated game. |
| `avg_actual_minutes` | Int64 | Average whole minutes played per aggregated game. |
| `avg_actual_seconds` | Float64 | Average seconds component of playing time per aggregated game. |
| `avg_tot_reb` | Float64 | Average total rebounds per game over the aggregated games. |
| `avg_ast` | Float64 | Average assists per game over the aggregated games. |
| `avg_stl` | Float64 | Average steals per game over the aggregated games. |
| `avg_turnovers` | Float64 | Average turnovers per game over the aggregated games. |
| `avg_blk` | Float64 | Average blocked shots per game over the aggregated games. |
| `avg_pts` | Float64 | Average points per game over the aggregated games. |
| `per_min_tot_reb` | Float64 | Total rebounds per minute played over the aggregated games. |
| `per_min_ast` | Float64 | Assists per minute played over the aggregated games. |
| `per_min_stl` | Float64 | Steals per minute played over the aggregated games. |
| `per_min_turnovers` | Float64 | Turnovers per minute played over the aggregated games. |
| `per_min_blk` | Float64 | Blocked shots per minute played over the aggregated games. |
| `per_min_pts` | Float64 | Points per minute played over the aggregated games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_cumestatsplayer-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_cumestatsplayer-example}

```python
wnba_stats_cumestatsplayer(league_id='10')
```

## wnba_stats_cumestatsplayergames

Cume stats player games.

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsplayergames`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsplayergames?LeagueID=10&Location=&Outcome=&PlayerID=204319&Season=2021-22&SeasonType=Regular+Season&VsConference=&VsDivision=&VsTeamID=0](https://stats.wnba.com/stats/cumestatsplayergames?LeagueID=10&Location=&Outcome=&PlayerID=204319&Season=2021-22&SeasonType=Regular+Season&VsConference=&VsDivision=&VsTeamID=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_cumestatsplayergames-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `matchup` | String | Matchup. |
| `game_id` | String | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_cumestatsplayergames-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_cumestatsplayergames-example}

```python
wnba_stats_cumestatsplayergames(league_id='10')
```

## wnba_stats_cumestatsteam

Cume stats team.

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsteam`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsteam?GameIDs=1022200018&LeagueID=10&Season=2021-22&SeasonType=Regular+Season&TeamID=1611661317](https://stats.wnba.com/stats/cumestatsteam?GameIDs=1022200018&LeagueID=10&Season=2021-22&SeasonType=Regular+Season&TeamID=1611661317)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameIDs` | `game_ids` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_cumestatsteam-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameByGameStats`, `TotalTeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GameByGameStats**

| col_name | type | description |
|---|---|---|
| `jersey_num` | String | Jersey number worn by the player. |
| `player` | String | Player name. |
| `person_id` | String | Unique player identifier (V3 endpoints). |
| `team_id` | String | Unique team identifier. |
| `gp` | String | Games played. |
| `gs` | String | Games started. |
| `actual_minutes` | String | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | String | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | String | Field goals made over the aggregated games. |
| `fga` | String | Field goal attempts. |
| `fg_pct` | String | Field goal percentage (0-1). |
| `fg3` | String | Three-point field goals made over the aggregated games. |
| `fg3_a` | String | Three-point field goal attempts. |
| `fg3_pct` | String | Three-point field goal percentage (0-1). |
| `ft` | String | Free throws made over the aggregated games. |
| `fta` | String | Free throw attempts. |
| `ft_pct` | String | Free throw percentage (0-1). |
| `off_reb` | String | Offensive rebounds over the aggregated games. |
| `def_reb` | String | Defensive rebounds over the aggregated games. |
| `tot_reb` | String | Total rebounds over the aggregated games. |
| `ast` | String | Assists. |
| `pf` | String | Personal fouls. |
| `dq` | String | Disqualifications (fouled out) over the aggregated games. |
| `stl` | String | Steals. |
| `turnovers` | String | Total turnovers. |
| `blk` | String | Blocks. |
| `pts` | String | Points scored. |
| `max_actual_minutes` | String | Most whole minutes played in any single aggregated game. |
| `max_actual_seconds` | String | Seconds component paired with the single-game maximum minutes. |
| `max_reb` | String | Most rebounds recorded in any single aggregated game. |
| `max_ast` | String | Most assists recorded in any single aggregated game. |
| `max_stl` | String | Most steals recorded in any single aggregated game. |
| `max_turnovers` | String | Most turnovers recorded in any single aggregated game. |
| `max_blkp` | String | Most blocked shots recorded in any single aggregated game. |
| `max_pts` | String | Most points recorded in any single aggregated game. |
| `avg_actual_minutes` | String | Average whole minutes played per aggregated game. |
| `avg_actual_seconds` | String | Average seconds component of playing time per aggregated game. |
| `avg_reb` | String | Average rebounds per game over the aggregated games. |
| `avg_ast` | String | Average assists per game over the aggregated games. |
| `avg_stl` | String | Average steals per game over the aggregated games. |
| `avg_turnovers` | String | Average turnovers per game over the aggregated games. |
| `avg_blkp` | String | Average blocked shots per game over the aggregated games. |
| `avg_pts` | String | Average points per game over the aggregated games. |
| `per_min_reb` | String | Rebounds per minute played over the aggregated games. |
| `per_min_ast` | String | Assists per minute played over the aggregated games. |
| `per_min_stl` | String | Steals per minute played over the aggregated games. |
| `per_min_turnovers` | String | Turnovers per minute played over the aggregated games. |
| `per_min_blk` | String | Blocked shots per minute played over the aggregated games. |
| `per_min_pts` | String | Points per minute played over the aggregated games. |

**TotalTeamStats**

| col_name | type | description |
|---|---|---|
| `city` | String | Venue city. |
| `nickname` | String | Team or athlete nickname. |
| `team_id` | Int64 | Unique team identifier. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_home` | Int64 |  |
| `l_home` | Int64 |  |
| `w_road` | Int64 |  |
| `l_road` | Int64 |  |
| `team_turnovers` | Int64 | Team turnovers (turnovers credited to the team rather than a player). |
| `team_rebounds` | Int64 | Team rebounds (rebounds credited to the team rather than a player). |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `actual_minutes` | Int64 | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | Int64 | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | Int64 | Field goals made over the aggregated games. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3` | Int64 | Three-point field goals made over the aggregated games. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ft` | Int64 | Free throws made over the aggregated games. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `off_reb` | Int64 | Offensive rebounds over the aggregated games. |
| `def_reb` | Int64 | Defensive rebounds over the aggregated games. |
| `tot_reb` | Int64 | Total rebounds over the aggregated games. |
| `ast` | Int64 | Assists. |
| `pf` | Int64 | Personal fouls. |
| `stl` | Int64 | Steals. |
| `total_turnovers` | Int64 | Total turnovers (player + team). |
| `blk` | Int64 | Blocks. |
| `pts` | Int64 | Points scored. |
| `avg_reb` | Float64 | Average rebounds per game over the aggregated games. |
| `avg_pts` | Float64 | Average points per game over the aggregated games. |
| `dq` | Int64 | Disqualifications (fouled out) over the aggregated games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_cumestatsteam-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_cumestatsteam-example}

```python
wnba_stats_cumestatsteam(league_id='10')
```

## wnba_stats_cumestatsteamgames

Cume stats team games.

**Endpoint URL:** `GET https://stats.wnba.com/stats/cumestatsteamgames`

**Valid URL:** [https://stats.wnba.com/stats/cumestatsteamgames?LeagueID=10&Location=&Outcome=&Season=2021-22&SeasonID=&SeasonType=Regular+Season&TeamID=1611661317&VsConference=&VsDivision=&VsTeamID=0](https://stats.wnba.com/stats/cumestatsteamgames?LeagueID=10&Location=&Outcome=&Season=2021-22&SeasonID=&SeasonType=Regular+Season&TeamID=1611661317&VsConference=&VsDivision=&VsTeamID=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonID` | `season_id_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_cumestatsteamgames-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `matchup` | String | Matchup. |
| `game_id` | String | Unique game identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_cumestatsteamgames-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_cumestatsteamgames-example}

```python
wnba_stats_cumestatsteamgames(league_id='10')
```
